import LazyLevel from "@peerbit/lazy-level";
import { AbstractLevel } from "abstract-level";
import { Level } from "level";
import { MemoryLevel } from "memory-level";
import { multiaddr, Multiaddr, isMultiaddr } from "@multiformats/multiaddr";
import type { Libp2p } from "libp2p";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Libp2pKeychain,
} from "@peerbit/crypto";
import {
	Program,
	Address,
	ProgramInitializationOptions,
	ProgramClient,
} from "@peerbit/program";
import { logger as loggerFn } from "@peerbit/logger";
import { DirectSub } from "@peerbit/pubsub";
import sodium from "libsodium-wrappers";
import path from "path-browserify";
import { waitFor } from "@peerbit/time";
import "@libp2p/peer-id";
import { Cache } from "@peerbit/cache";

import {
	createLibp2pExtended,
	Libp2pExtended,
	Libp2pCreateOptions as ClientCreateOptions,
} from "./libp2p.js";
import { DirectBlock } from "@peerbit/blocks";
import { LevelDatastore } from "datastore-level";
import { BinaryWriter } from "@dao-xyz/borsh";
import PQueue from "p-queue";

export const logger = loggerFn({ module: "client" });

export type OptionalCreateOptions = {
	limitSigning?: boolean;
	minReplicas?: number;
	refreshIntreval?: number;
	libp2pExternal?: boolean;
};
export type CreateOptions = {
	directory?: string;
	cache: LazyLevel;
	identity: Ed25519Keypair;
	keychain: Libp2pKeychain;
} & OptionalCreateOptions;

export type CreateInstanceOptions = {
	libp2p?: Libp2pExtended | ClientCreateOptions;
	directory?: string;
	cache?: LazyLevel;
} & OptionalCreateOptions;
type ProgramMergeStrategy = "replace" | "reject";
export type OpenOptions<Args> = {
	timeout?: number;
	existing?: ProgramMergeStrategy;
	/* 
	reset?: boolean; */
} & ProgramInitializationOptions<Args>;

const isLibp2pInstance = (libp2p: Libp2pExtended | ClientCreateOptions) =>
	!!(libp2p as Libp2p).getMultiaddrs;

const createLevel = (path?: string): AbstractLevel<any, string, Uint8Array> => {
	return path
		? new Level(path, { valueEncoding: "view" })
		: new MemoryLevel({ valueEncoding: "view" });
};

const createCache = async (
	directory: string | undefined,
	options?: { reset?: boolean }
) => {
	const cache = await new LazyLevel(createLevel(directory));

	// "Wake up" the caches if they need it
	if (cache) await cache.open();
	if (options?.reset) {
		await cache["_store"].clear();
	}

	return cache;
};

export class Peerbit implements ProgramClient {
	_libp2p: Libp2pExtended;

	directory?: string;

	/// program address => Program metadata
	programs: Map<string, Program>;
	limitSigning: boolean;

	private _cache: LazyLevel;
	private _libp2pExternal?: boolean = false;

	// Libp2p peerid in Identity form
	private _identity: Ed25519Keypair;

	private _keychain: Libp2pKeychain; // Keychain + Caching + X25519 keys

	private _openQueue: PQueue;
	constructor(libp2p: Libp2pExtended, options: CreateOptions) {
		if (libp2p == null) {
			throw new Error("Libp2p required");
		}
		this._libp2p = libp2p;
		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					this.libp2p.peerId.type
			);
		}

		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error("Only Ed25519 peerIds are supported");
		}

		this._identity = options.identity;
		this._keychain = options.keychain;

		this.directory = options.directory;
		this.programs = new Map();
		this.limitSigning = options.limitSigning || false;
		this._cache = options.cache;
		this._libp2pExternal = options.libp2pExternal;
		this._openQueue = new PQueue({ concurrency: 1 });
	}

	static async create(options: CreateInstanceOptions = {}): Promise<Peerbit> {
		await sodium.ready; // Some of the modules depends on sodium to be readyy

		let libp2pExtended: Libp2pExtended = options.libp2p as Libp2pExtended;
		const blocksDirectory =
			options.directory != null
				? path.join(options.directory, "/blocks").toString()
				: undefined;
		let libp2pExternal = false;

		const datastore =
			options.directory != null
				? new LevelDatastore(path.join(options.directory, "/libp2p").toString())
				: undefined;
		if (datastore) {
			await datastore.open();
		}

		if (!libp2pExtended) {
			libp2pExtended = await createLibp2pExtended({
				services: {
					blocks: (c) => new DirectBlock(c, { directory: blocksDirectory }),
					pubsub: (c) => new DirectSub(c),
				},
				// If directory is passed, we store keys within that directory, else we will use memory datastore (which is the default behaviour)
				datastore,
			});
		} else {
			if (isLibp2pInstance(libp2pExtended)) {
				libp2pExternal = true; // libp2p was created outside
			} else {
				const extendedOptions = libp2pExtended as any as ClientCreateOptions;
				libp2pExtended = await createLibp2pExtended({
					...extendedOptions,
					services: {
						blocks: (c) => new DirectBlock(c, { directory: blocksDirectory }),
						pubsub: (c) => new DirectSub(c),
						...extendedOptions?.services,
					},
					datastore,
				});
			}
		}
		if (datastore) {
			const stopFn = libp2pExtended.stop.bind(libp2pExtended);
			libp2pExtended.stop = async () => {
				await stopFn();
				await datastore?.close();
			};
		}

		if (!libp2pExtended.isStarted()) {
			await libp2pExtended.start();
		}

		if (libp2pExtended.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					libp2pExtended.peerId.type
			);
		}

		const directory = options.directory;
		const cache =
			options.cache ||
			(await createCache(
				directory ? path.join(directory, "/cache") : undefined
			));

		const identity = Ed25519Keypair.fromPeerId(libp2pExtended.peerId);
		const keychain = new Libp2pKeychain(libp2pExtended.keychain, {
			cache: new Cache({ max: 1000 }),
		});

		try {
			const writer = new BinaryWriter();
			writer.string("identity");
			await keychain.import(identity, writer.finalize());
		} catch (error: any) {
			if (error.code == "ERR_KEY_ALREADY_EXISTS") {
				// Do nothing
			} else {
				throw error;
			}
		}

		const peer = new Peerbit(libp2pExtended, {
			directory,
			cache,
			libp2pExternal,
			limitSigning: options.limitSigning,
			minReplicas: options.minReplicas,
			refreshIntreval: options.refreshIntreval,
			identity,
			keychain,
		});
		return peer;
	}
	get libp2p(): Libp2pExtended {
		return this._libp2p;
	}

	get identity(): Ed25519Keypair {
		return this._identity;
	}

	get peerId() {
		return this.libp2p.peerId;
	}

	get services() {
		return this.libp2p.services;
	}

	get keychain(): Libp2pKeychain {
		return this._keychain;
	}

	getMultiaddrs(): Multiaddr[] {
		return this.libp2p.getMultiaddrs();
	}
	/**
	 * Dial a peer with an Ed25519 peerId
	 */
	async dial(
		address: string | Multiaddr | Multiaddr[] | ProgramClient
	): Promise<boolean> {
		const maddress =
			typeof address == "string"
				? multiaddr(address)
				: isMultiaddr(address) || Array.isArray(address)
				? address
				: address.getMultiaddrs();
		const connection = await this.libp2p.dial(maddress);
		const publicKey = Ed25519PublicKey.fromPeerId(connection.remotePeer);

		// TODO, do this as a promise instead using the onPeerConnected vents in pubsub and blocks
		return (
			(await waitFor(
				() =>
					this.libp2p.services.pubsub.peers.has(publicKey.hashcode()) &&
					this.libp2p.services.blocks.peers.has(publicKey.hashcode())
			)) || false
		);
	}

	async start() {
		if (!this.libp2p.isStarted()) {
			this._libp2pExternal = false; // this means we will also close libp2p client on close
			return this.libp2p.start();
		}
	}
	async stop() {
		this._openQueue.clear();
		await this._openQueue.onIdle();

		// Close all open databases
		await Promise.all(
			[...this.programs.values()].map((program) => program.close())
		);

		await this._cache.close();

		// Close libp2p (after above)
		if (!this._libp2pExternal) {
			// only close it if we created it
			await this.libp2p.stop();
		}

		// Remove all databases from the state
		this.programs = new Map();
	}

	private _onProgamClose(program: Program) {
		this.programs.delete(program.address!.toString());
	}

	private async _onProgramOpen(
		program: Program,
		mergeSrategy?: ProgramMergeStrategy
	) {
		const programAddress = program.address?.toString();
		if (!programAddress) {
			throw new Error("Missing program address");
		}
		if (this.programs.has(programAddress)) {
			// second condition only makes this throw error if we are to add a new instance with the same address
			await this.checkProcessExisting(programAddress, program, mergeSrategy);
			this.programs.set(programAddress, program);
		} else {
			this.programs.set(programAddress, program);
		}
	}

	private async checkProcessExisting(
		address: Address,
		toOpen: Program,
		mergeSrategy: ProgramMergeStrategy = "reject"
	) {
		if (mergeSrategy === "reject") {
			throw new Error(`Program at ${address} is already open`);
		} else if (mergeSrategy === "replace") {
			const prev = this.programs.get(address);
			if (prev && prev !== toOpen) {
				await prev.close(); // clouse previous
			}
		}
	}
	/**
	 * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
	 * and to replicate heads (and updates) which is requested by another peer
	 * @param store
	 * @param options
	 * @returns
	 */

	async open<S extends Program<Args>, Args = any>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<Args> = {}
	): Promise<S> {
		const fn = async (): Promise<S> => {
			if (!this.libp2p.isStarted()) {
				throw new Error("Can not open a store while disconnected");
			}

			// TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?

			let program = storeOrAddress as S;
			if (typeof storeOrAddress === "string") {
				try {
					if (this.programs?.has(storeOrAddress.toString())) {
						await this.checkProcessExisting(
							storeOrAddress.toString(),
							program,
							options?.existing
						);
					} else {
						program = (await Program.load(
							storeOrAddress,
							this._libp2p.services.blocks,
							options
						)) as S; // TODO fix typings
						if (program instanceof Program === false) {
							throw new Error(
								`Failed to open program because program is of type ${program?.constructor.name} and not ${Program.name}`
							);
						}
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " + storeOrAddress.toString()
					);
					throw error;
				}
			} else if (!program.closed) {
				const existing = this.programs.get(program.address);
				if (existing === program) {
					return program;
				} else if (existing) {
					await this.checkProcessExisting(
						program.address,
						program,
						options?.existing
					);
				}
			}

			logger.debug(`Open database '${program.constructor.name}`);
			await program.beforeOpen(this, {
				onBeforeOpen: (p) => {
					if (p instanceof Program && p.parents.length === 1 && !p.parents[0]) {
						return this._onProgramOpen(p, options?.existing);
					}
				},
				onClose: (p) => {
					if (p instanceof Program) {
						return this._onProgamClose(p);
					}
				},
				onDrop: (p) => {
					if (p instanceof Program) {
						return this._onProgamClose(p);
					}
				},
				...options,
				// If the program opens more programs
				// reset: options.reset,
			});

			await program.open(options.args);
			await program.afterOpen();

			return program as S;
		};
		return this._openQueue.add(fn) as any as S; // TODO p-queue seem to return void type ;
	}

	get memory() {
		return this._cache;
	}
}
