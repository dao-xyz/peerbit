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

import {
	createLibp2pExtended,
	Libp2pExtended,
	Libp2pCreateOptions as ClientCreateOptions,
} from "./libp2p.js";
import { DirectBlock } from "@peerbit/blocks";
import { LevelDatastore } from "datastore-level";
import { BinaryWriter } from "@dao-xyz/borsh";

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
export type OpenOptions<Args> = {
	timeout?: number;
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
		const keychain = new Libp2pKeychain(libp2pExtended.keychain);

		// Import identity into keychain

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
		// Close a direct connection and remove it from internal state

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

	private _onProgramOpen(program: Program) {
		const programAddress = program.address?.toString();
		if (!programAddress) {
			throw new Error("Missing program address");
		}
		if (this.programs.has(programAddress)) {
			// second condition only makes this throw error if we are to add a new instance with the same address
			throw new Error(`Program at ${programAddress} is already open`);
		}

		this.programs.set(programAddress, program);
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
		if (!this.libp2p.isStarted()) {
			throw new Error("Can not open a store while disconnected");
		}

		// TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?

		let program = storeOrAddress as S;
		if (typeof storeOrAddress === "string") {
			try {
				if (this.programs?.has(storeOrAddress.toString())) {
					throw new Error(
						`Program at ${storeOrAddress.toString()} is already open`
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
				throw new Error(
					`Program at ${program.address} is already open, but is another instance`
				);
			}
		}

		logger.debug(`Open database '${program.constructor.name}`);
		await program.beforeOpen(this, {
			onOpen: (p) => {
				if (p instanceof Program && p.parents.length === 1 && !p.parents[0]) {
					return this._onProgramOpen(p);
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
	}

	get memory() {
		return this._cache;
	}
}
