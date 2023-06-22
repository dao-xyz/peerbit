import LazyLevel from "@dao-xyz/lazy-level";
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
import { Program, Address } from "@peerbit/program";
import PQueue from "p-queue";
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
import { Peerbit as IPeerbit } from "@peerbit/interface";
import { BinaryWriter } from "@dao-xyz/borsh";
export const logger = loggerFn({ module: "peer" });

interface ProgramWithMetadata {
	program: Program;
	openCounter: number;
}

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
export type OpenOptions = {
	timeout?: number;
	reset?: boolean;
};

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

const createSubCache = async (
	from: LazyLevel,
	name: string,
	options?: { reset?: boolean }
) => {
	const cache = await new LazyLevel(from["_store"].sublevel(name));

	// "Wake up" the caches if they need it
	if (cache) await cache.open();
	if (options?.reset) {
		await cache["_store"].clear();
	}

	return cache;
};

export class Peerbit implements IPeerbit {
	_libp2p: Libp2pExtended;

	directory?: string;

	/// program address => Program metadata
	programs: Map<string, ProgramWithMetadata>;
	limitSigning: boolean;

	private _openProgramQueue: PQueue;
	private _disconnected = false;
	private _disconnecting = false;
	private _refreshInterval: any;
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
		this._openProgramQueue = new PQueue({ concurrency: 1 });
	}

	static async create(options: CreateInstanceOptions = {}) {
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
		const writer = new BinaryWriter();
		writer.string("identity");
		keychain.import(identity, writer.finalize());

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

	get disconnected() {
		return this._disconnected;
	}

	get disconnecting() {
		return this._disconnecting;
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
		address: string | Multiaddr | Multiaddr[] | IPeerbit
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

	async stop() {
		this._disconnecting = true;
		// Close a direct connection and remove it from internal state

		this._refreshInterval && clearInterval(this._refreshInterval);

		// Close all open databases
		await Promise.all(
			[...this.programs.values()].map((program) => program.program.close())
		);

		await this._cache.close();

		// Close libp2p (after above)
		if (!this._libp2pExternal) {
			// only close it if we created it
			await this.libp2p.stop();
		}

		// Remove all databases from the state
		this.programs = new Map();
		this._disconnecting = false;
		this._disconnected = true;
	}

	private async _onProgamClose(program: Program, programCache: LazyLevel) {
		await programCache.close();
		const programAddress = program.address?.toString();
		if (programAddress) {
			this.programs.delete(programAddress);
		}
	}

	private _addProgram(program: Program) {
		const programAddress = program.address?.toString();
		if (!programAddress) {
			throw new Error("Missing program address");
		}
		const existingProgramAndStores = this.programs.get(programAddress);
		if (
			!!existingProgramAndStores &&
			existingProgramAndStores.program !== program
		) {
			// second condition only makes this throw error if we are to add a new instance with the same address
			throw new Error(`Program at ${programAddress} is already created`);
		}

		this.programs.set(programAddress, {
			program,
			openCounter: 1,
		});
		return program;
	}

	/**
	 * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
	 * and to replicate heads (and updates) which is requested by another peer
	 * @param store
	 * @param options
	 * @returns
	 */

	async open<S extends Program>(
		storeOrAddress: /* string | Address |  */ S | Address | string,
		options: OpenOptions = {}
	): Promise<S> {
		if (this._disconnected || this._disconnecting) {
			throw new Error("Can not open a store while disconnected");
		}

		const fn = async (): Promise<Program> => {
			// TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?

			let program = storeOrAddress as S;
			let existing = false;
			if (typeof storeOrAddress === "string") {
				try {
					const fromExisting = this.programs?.get(storeOrAddress.toString())
						?.program as S;
					if (fromExisting) {
						program = fromExisting;
						existing = true;
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
			}

			if (!program.address && !existing) {
				await program.save(this._libp2p.services.blocks);
			}

			const programAddress = program.address!.toString()!;
			if (programAddress) {
				const existingProgram = this.programs?.get(programAddress);
				if (existingProgram) {
					existingProgram.openCounter += 1;
					return existingProgram.program;
				}
			}

			logger.debug(`Open database '${program.constructor.name}`);

			let programCache: LazyLevel | undefined = undefined;

			await program.open(this, {
				onClose: async () => {
					return this._onProgamClose(program, programCache!);
				},
				onDrop: () => this._onProgamClose(program, programCache!),

				// If the program opens more programs
				open: (program) => this.open(program, options),
				onSave: async (address) => {
					programCache = await createSubCache(this._cache, address.toString(), {
						reset: options.reset,
					});
				},
			});
			return this._addProgram(program);
		};
		return this._openProgramQueue.add(fn) as Promise<S>;
	}

	get memory() {
		return this._cache;
	}
}
