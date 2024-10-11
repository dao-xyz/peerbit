import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import "@libp2p/peer-id";
import {
	type Multiaddr,
	isMultiaddr,
	multiaddr,
} from "@multiformats/multiaddr";
import { type AnyStore, createStore } from "@peerbit/any-store";
import { DirectBlock } from "@peerbit/blocks";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Secp256k1Keypair,
	getKeypairFromPrivateKey,
} from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { create as createSQLiteIndexer } from "@peerbit/indexer-sqlite3";
import { DefaultKeychain } from "@peerbit/keychain";
import { logger as loggerFn } from "@peerbit/logger";
import {
	type Address,
	type ExtractArgs,
	type OpenOptions,
	type Program,
	type ProgramClient,
	ProgramHandler,
} from "@peerbit/program";
import { DirectSub } from "@peerbit/pubsub";
import { waitFor } from "@peerbit/time";
import { LevelDatastore } from "datastore-level";
import type { Libp2p } from "libp2p";
import sodium from "libsodium-wrappers";
import path from "path-browserify";
import { concat } from "uint8arrays";
import { resolveBootstrapAddresses } from "./bootstrap.js";
import {
	type Libp2pCreateOptions as ClientCreateOptions,
	type Libp2pExtended,
	type PartialLibp2pCreateOptions,
	createLibp2pExtended,
} from "./libp2p.js";

export const logger = loggerFn({ module: "client" });

export type OptionalCreateOptions = {
	libp2pExternal?: boolean;
};
export type CreateOptions = {
	directory?: string;
	storage: AnyStore;
	indexer: Indices;
	identity: Ed25519Keypair;
} & OptionalCreateOptions;
type Libp2pOptions = { libp2p?: Libp2pExtended | PartialLibp2pCreateOptions };
type SimpleLibp2pOptions = { relay?: boolean };
export type CreateInstanceOptions = (SimpleLibp2pOptions | Libp2pOptions) & {
	directory?: string;
	indexer?: (directory?: string) => Promise<Indices> | Indices;
} & OptionalCreateOptions;

const isLibp2pInstance = (libp2p: Libp2pExtended | ClientCreateOptions) =>
	!!(libp2p as Libp2p).getMultiaddrs;

const createCache = async (
	directory: string | undefined,
	options?: { reset?: boolean },
) => {
	const cache = createStore(directory);

	// "Wake up" the caches if they need it
	if (cache) await cache.open();
	if (options?.reset) {
		await cache.clear();
	}
	return cache;
};

const SELF_IDENTITY_KEY_ID = new TextEncoder().encode("__self__");

export class Peerbit implements ProgramClient {
	_libp2p: Libp2pExtended;

	directory?: string;

	private _storage: AnyStore;
	private _indexer: Indices;
	private _libp2pExternal?: boolean = false;

	// Libp2p peerid in Identity form
	private _identity: Ed25519Keypair;
	private _handler: ProgramHandler;

	constructor(libp2p: Libp2pExtended, options: CreateOptions) {
		if (libp2p == null) {
			throw new Error("Libp2p required");
		}
		this._libp2p = libp2p;
		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					this.libp2p.peerId.type,
			);
		}

		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error("Only Ed25519 peerIds are supported");
		}

		this._identity = options.identity;
		this.directory = options.directory;
		this._storage = options.storage;
		this._libp2pExternal = options.libp2pExternal;
		this._indexer = options.indexer;
	}

	static async create(options: CreateInstanceOptions = {}): Promise<Peerbit> {
		await sodium.ready; // Some of the modules depends on sodium to be readyy

		let libp2pExtended: Libp2pExtended | undefined = (options as Libp2pOptions)
			.libp2p as Libp2pExtended;

		const asRelay = (options as SimpleLibp2pOptions).relay;

		const directory = options.directory;
		const hasDir = directory != null;

		const storage = await createCache(
			directory != null ? path.join(directory, "/cache") : undefined,
		);

		const indexerFn = options.indexer || createSQLiteIndexer;
		const indexer =
			directory != null
				? await indexerFn(path.join(directory, "/index"))
				: await indexerFn();

		const blocksDirectory = hasDir
			? path.join(directory, "/blocks").toString()
			: undefined;

		const keychainDirectory = hasDir
			? path.join(directory, "/keychain").toString()
			: undefined;

		const datastore = hasDir
			? new LevelDatastore(path.join(directory, "/libp2p").toString())
			: undefined;

		if (datastore) {
			await datastore.open();
		}

		const libp2pExternal = libp2pExtended && isLibp2pInstance(libp2pExtended);
		if (!libp2pExternal) {
			const extendedOptions: ClientCreateOptions | undefined =
				libp2pExtended as any as ClientCreateOptions;
			const store = createStore(keychainDirectory);
			await store.open();

			const keychain = new DefaultKeychain({
				store,
			});
			let privateKey = extendedOptions?.privateKey;
			if (!privateKey) {
				const exported = await keychain.exportById(
					SELF_IDENTITY_KEY_ID,
					Ed25519Keypair,
				);
				privateKey = exported
					? privateKeyFromRaw(
							concat([
								exported.privateKey.privateKey,
								exported.publicKey.publicKey,
							]),
						)
					: undefined;
			}

			libp2pExtended = await createLibp2pExtended({
				...extendedOptions,
				privateKey,
				services: {
					keychain: (c: any) => keychain,
					blocks: (c: any) =>
						new DirectBlock(c, {
							canRelayMessage: asRelay,
							directory: blocksDirectory,
						}),
					pubsub: (c: any) => new DirectSub(c, { canRelayMessage: asRelay }),
					...extendedOptions?.services,
				} as any, // TODO types are funky
				datastore,
			});
		}
		if (datastore) {
			const stopFn = libp2pExtended.stop.bind(libp2pExtended);
			libp2pExtended.stop = async () => {
				await stopFn();
				await datastore?.close();
			};
		}

		if (
			libp2pExtended.status === "stopped" ||
			libp2pExtended.status === "stopping"
		) {
			await libp2pExtended.start();
		}

		if (libp2pExtended.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					libp2pExtended.peerId.type,
			);
		}

		const identity = getKeypairFromPrivateKey(
			(libp2pExtended as any)["components"].privateKey, // TODO can we export privateKey in a better way?
		);

		if (identity instanceof Secp256k1Keypair) {
			throw new Error("Only Ed25519 keypairs are supported");
		}

		try {
			await libp2pExtended.services.keychain.import({
				keypair: identity,
				id: SELF_IDENTITY_KEY_ID,
			});
		} catch (error: any) {
			if (error.code === "ERR_KEY_ALREADY_EXISTS") {
				// Do nothing
			} else {
				throw error;
			}
		}

		const peer = new Peerbit(libp2pExtended, {
			directory,
			storage,
			libp2pExternal,
			identity,
			indexer,
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

	get handler(): ProgramHandler {
		return this._handler;
	}

	getMultiaddrs(): Multiaddr[] {
		return this.libp2p.getMultiaddrs();
	}
	/**
	 * Dial a peer with an Ed25519 peerId
	 */
	async dial(
		address: string | Multiaddr | Multiaddr[] | ProgramClient,
	): Promise<boolean> {
		const maddress =
			typeof address === "string"
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
					this.libp2p.services.blocks.peers.has(publicKey.hashcode()),
			)) || false
		);
	}

	async start() {
		await this._storage.open();
		await this.indexer.start();

		if (this.libp2p.status === "stopped" || this.libp2p.status === "stopping") {
			this._libp2pExternal = false; // this means we will also close libp2p client on close
			return this.libp2p.start();
		}
	}
	async stop() {
		await this._handler?.stop();
		await this._storage.close();
		await this.indexer.stop();

		// Close libp2p (after above)
		if (!this._libp2pExternal) {
			// only close it if we created it
			await this.libp2p.stop();
		}
	}

	async bootstrap() {
		const addresses = await resolveBootstrapAddresses();
		if (addresses.length === 0) {
			throw new Error("Failed to find any addresses to dial");
		}
		const settled = await Promise.allSettled(
			addresses.map((x) => this.dial(x)),
		);
		let once = false;
		for (const [i, result] of settled.entries()) {
			if (result.status === "fulfilled") {
				once = true;
			} else {
				logger.warn(
					"Failed to dial bootstrap address(s): " +
						JSON.stringify(addresses[i]) +
						". Reason: " +
						result.reason,
				);
			}
		}
		if (!once) {
			throw new Error("Failed to succefully dial any bootstrap node");
		}
	}

	/**
	 * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
	 * and to replicate heads (and updates) which is requested by another peer
	 * @param store
	 * @param options
	 * @returns
	 */

	async open<S extends Program<ExtractArgs<S>>>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<S> = {},
	): Promise<S> {
		return (
			this._handler || (this._handler = new ProgramHandler({ client: this }))
		).open(storeOrAddress, options);
	}

	get storage() {
		return this._storage;
	}

	get indexer() {
		return this._indexer;
	}
}
