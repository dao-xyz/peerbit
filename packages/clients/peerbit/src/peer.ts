import { LevelDatastore } from "@dao-xyz/datastore-level";
import { keys } from "@libp2p/crypto";
import type { PeerId } from "@libp2p/interface";
import type { KeychainComponents } from "@libp2p/keychain";
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
	PublicSignKey,
	Secp256k1Keypair,
	getPublicKeyFromPeerId,
	getKeypairFromPrivateKey,
} from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { DefaultCryptoKeychain, keychain } from "@peerbit/keychain";
import { logger as loggerFn } from "@peerbit/logger";
import {
	type Address,
	type ExtractArgs,
	type OpenOptions,
	type Program,
	type ProgramClient,
	ProgramHandler,
} from "@peerbit/program";
import {
	FanoutChannel,
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
	type FanoutTreeChannelOptions,
	type FanoutTreeJoinOptions,
} from "@peerbit/pubsub";
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

export const logger = loggerFn("peerbit:client");

export type OptionalCreateOptions = {
	libp2pExternal?: boolean;
};
export type CreateOptions = {
	directory?: string;
	storage: AnyStore;
	indexer: Indices;
	identity: Ed25519Keypair;
} & OptionalCreateOptions;
type Libp2pOptions = {
	libp2p?: Libp2pExtended | (PartialLibp2pCreateOptions & { peerId?: never });
};
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

const SELF_IDENTITY_KEY_ID = new Uint8Array([
	95, 95, 115, 101, 108, 102, 95, 95,
]); // new TextEncoder().encode("__self__");

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

		const asRelay = (options as SimpleLibp2pOptions).relay ?? true;

		const directory = options.directory;
		const hasDir = directory != null;

		const storage = await createCache(
			directory != null ? path.join(directory, "/cache") : undefined,
		);
		const indexerFn =
			options.indexer ||
			(async (directory?: string) => {
				// Lazy-import to avoid loading sqlite-wasm in browser-like runtimes/tests
				const { create } = await import("@peerbit/indexer-sqlite3");
				return create(directory);
			});
		const indexer =
			directory != null
				? await indexerFn(path.join(directory, "/index"))
				: await indexerFn();

		await indexer.start();

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

		let stopLibp2pOnClose = false;
		const libp2pExternal = libp2pExtended && isLibp2pInstance(libp2pExtended);
		if (!libp2pExternal) {
			const extendedOptions: ClientCreateOptions | undefined =
				libp2pExtended as any as ClientCreateOptions;
			if (extendedOptions && "peerId" in extendedOptions) {
				throw new Error(
					"Invalid libp2p option 'peerId'. libp2p derives the peer id from 'privateKey', so pass 'privateKey' to control identity.",
				);
			}
			const store = createStore(keychainDirectory);
			await store.open();

			const cryptoKeychain = new DefaultCryptoKeychain({
				store,
			});
			let privateKey = extendedOptions?.privateKey;
			if (!privateKey) {
				const exported = await cryptoKeychain.exportById(
					SELF_IDENTITY_KEY_ID,
					Ed25519Keypair,
				);
				privateKey = exported
					? keys.privateKeyFromRaw(
							concat([
								exported.privateKey.privateKey,
								exported.publicKey.publicKey,
							]),
						)
					: undefined;
			}

			const topicRootControlPlane = new TopicRootControlPlane();

			// Used for wiring blocks <-> fanout provider discovery without introducing hard
			// dependencies between services. (Both services are optional/overridable.)
			let fanoutService: FanoutTree | undefined;
			const blockProviderNamespace = (cid: string) => `cid:${cid}`;

			const services: any = {
				keychain: (components: KeychainComponents) =>
					keychain({ libp2p: {}, crypto: cryptoKeychain })(components),
				fanout: (c: any) =>
					(fanoutService = new FanoutTree(c, {
						connectionManager: false,
						topicRootControlPlane,
					})),
				blocks: (c: any) => {
					let blocksService: DirectBlock | undefined;

					const fallbackConnectedPeers = () => {
						const out: string[] = [];
						const push = (hash?: string) => {
							if (!hash) return;
							if (hash === blocksService?.publicKeyHash) return;
							// Small bounded list; avoid Set allocations on hot paths.
							if (out.includes(hash)) return;
							out.push(hash);
						};

						// Prefer peers we've already negotiated `/peerbit/direct-block` streams with.
						for (const h of blocksService?.peers.keys() ?? []) {
							push(h);
							if (out.length >= 32) return out;
						}

						// Fall back to currently connected libp2p peers.
						for (const conn of c.connectionManager.getConnections()) {
							try {
								push(getPublicKeyFromPeerId(conn.remotePeer).hashcode());
							} catch {
								// ignore unexpected key types
							}
							if (out.length >= 32) break;
						}

						return out;
					};

					const resolveProviders = async (
						cid: string,
						options?: { signal?: AbortSignal },
					) => {
						// 1) tracker-backed provider directory (best-effort, bounded)
						try {
							const providers = await fanoutService?.queryProviders(
								blockProviderNamespace(cid),
								{
									want: 8,
									timeoutMs: 2_000,
									queryTimeoutMs: 500,
									bootstrapMaxPeers: 2,
									signal: options?.signal,
								},
							);
							if (providers && providers.length > 0) return providers;
						} catch {
							// ignore discovery failures
						}

						// 2) fallback to currently connected peers (keeps local/small nets working without trackers)
						return fallbackConnectedPeers();
					};

					blocksService = new DirectBlock(c, {
						canRelayMessage: asRelay,
						directory: blocksDirectory,
						resolveProviders,
						onPut: async (cid) => {
							// Best-effort directory announce for "get without remote.from" workflows.
							try {
								await fanoutService?.announceProvider(blockProviderNamespace(cid), {
									ttlMs: 120_000,
									bootstrapMaxPeers: 2,
								});
							} catch {
								// ignore announce failures
							}
						},
					});
					return blocksService;
				},
				pubsub: (c: any) =>
					new TopicControlPlane(c, {
						canRelayMessage: asRelay,
						topicRootControlPlane,
					}),
				...extendedOptions?.services,
			};

			if (!asRelay) {
				services.relay = null;
			}

			libp2pExtended = await createLibp2pExtended({
				...extendedOptions,
				privateKey,
				services,
				datastore,
				start: true,
			});
			stopLibp2pOnClose = true; // we created it, so we will stop it
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
			stopLibp2pOnClose = true;
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
			libp2pExternal: !stopLibp2pOnClose,
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
		try {
			await this.libp2p.services.pubsub.waitFor(publicKey.hashcode(), {
				target: "neighbor",
			});
		} catch (error) {
			throw new Error(`Failed to dial peer. Not available on Pubsub`);
		}

		try {
			await this.libp2p.services.blocks.waitFor(publicKey.hashcode(), {
				target: "neighbor",
			});
		} catch (error) {
			throw new Error(`Failed to dial peer. Not available on Blocks`);
		}
		return true;
	}

	async hangUp(address: PeerId | PublicSignKey | string | Multiaddr) {
		await this.libp2p.hangUp(
			address instanceof PublicSignKey
				? address.toPeerId()
				: typeof address === "string"
					? multiaddr(address)
					: address,
		);
		// TODO wait for pubsub and blocks to disconnect?
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

	async bootstrap(addresses?: string[] | Multiaddr[]) {
		const _addresses = addresses ?? (await resolveBootstrapAddresses());
		if (_addresses.length === 0) {
			throw new Error("Failed to find any addresses to dial");
		}
		// Keep fanout bootstrap config aligned with peer bootstrap config so fanout
		// channels can join via the same rendezvous nodes.
		try {
			this.libp2p.services.fanout.setBootstraps(_addresses);
		} catch {
			// ignore if fanout service is not present/overridden
		}
		const settled = await Promise.allSettled(
			_addresses.map((x) => this.dial(x)),
		);
		let once = false;
		for (const [i, result] of settled.entries()) {
			if (result.status === "fulfilled") {
				once = true;
			} else {
				logger.error(
					"Failed to dial bootstrap address(s): " +
						JSON.stringify(_addresses[i]) +
						". Reason: " +
						result.reason,
				);
			}
		}
		if (!once) {
			throw new Error("Failed to succefully dial any bootstrap node");
		}

		// Seed deterministic topic-root candidates for the topic-root resolver.
		// We use all currently connected peers after bootstrap dialing.
		const servicesAny: any = this.libp2p.services as any;
		const fanoutPlane = servicesAny?.fanout?.topicRootControlPlane;
		const pubsubPlane = servicesAny?.pubsub?.topicRootControlPlane;
		const planes = [...new Set([fanoutPlane, pubsubPlane].filter(Boolean))];
		if (planes.length > 0) {
			const candidates = new Set<string>();
			for (const connection of this.libp2p.getConnections()) {
				try {
					candidates.add(getPublicKeyFromPeerId(connection.remotePeer).hashcode());
				} catch {
					// ignore peers without a resolvable public key
				}
			}
			if (candidates.size > 0) {
				const list = [...candidates];
				for (const plane of planes) {
					plane.setTopicRootCandidates(list);
				}
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

	async open<S extends Program<ExtractArgs<S>>>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<S> = {},
	): Promise<S> {
		return (
			this._handler || (this._handler = new ProgramHandler({ client: this }))
		).open(storeOrAddress, options);
	}

	public fanoutChannel(topic: string, root: string = this.services.fanout.publicKeyHash) {
		if (root === this.services.fanout.publicKeyHash) {
			return FanoutChannel.fromSelf(this.services.fanout, topic);
		}
		return new FanoutChannel(this.services.fanout, { topic, root });
	}

	public async fanoutResolveRoot(topic: string): Promise<string> {
		const servicesAny: any = this.services as any;
		const topicRootControlPlane =
			servicesAny?.fanout?.topicRootControlPlane ||
			servicesAny?.pubsub?.topicRootControlPlane;
		const resolved = await topicRootControlPlane?.resolveTopicRoot?.(topic);
		if (!resolved) {
			throw new Error(
				`Failed to resolve fanout root for topic ${topic}. Configure topic-root candidates/resolver (Peerbit.bootstrap() does this automatically) or pass root explicitly.`,
			);
		}
		return resolved;
	}

	public fanoutOpenAsRoot(
		topic: string,
		options: Omit<FanoutTreeChannelOptions, "role">,
	) {
		return this.fanoutChannel(topic).openAsRoot(options);
	}

	public fanoutJoin(
		topic: string,
		root: string,
		options: Omit<FanoutTreeChannelOptions, "role">,
		joinOpts?: FanoutTreeJoinOptions,
	) {
		return this.fanoutChannel(topic, root).join(options, joinOpts);
	}

	public async fanoutJoinAuto(
		topic: string,
		options: Omit<FanoutTreeChannelOptions, "role">,
		joinOpts?: FanoutTreeJoinOptions,
	) {
		const root = await this.fanoutResolveRoot(topic);
		return this.fanoutJoin(topic, root, options, joinOpts);
	}

	get storage() {
		return this._storage;
	}

	get indexer() {
		return this._indexer;
	}
}
