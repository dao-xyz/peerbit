import { field, variant } from "@dao-xyz/borsh";
import { TypedEventEmitter } from "@libp2p/interface";
import {
	type GetOptions,
	type Blocks as IBlocks,
	checkDecodeBlock,
	cidifyString,
	codecCodes,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import { PublicSignKey } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import { type PublishOptions, dontThrowIfDeliveryError } from "@peerbit/stream";
import {
	type PeerRefs,
	SilentDelivery,
	type WaitForAnyOpts,
	type WaitForPeersFn,
	type WaitForPresentOpts,
} from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { CID } from "multiformats";
import { type Block } from "multiformats/block";
import PQueue from "p-queue";
import { AnyBlockStore } from "./any-blockstore.js";
import type { BlockStore } from "./interface.js";

export const logger = loggerFn("peerbit:transport:blocks");
const warn = logger.newScope("warn");

export class BlockMessage {}

@variant(0)
export class BlockRequest extends BlockMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}

@variant(1)
export class BlockResponse extends BlockMessage {
	@field({ type: "string" })
	cid: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(cid: string, bytes: Uint8Array) {
		super();
		this.cid = cid;
		this.bytes = bytes;
	}
}

export class RemoteBlocks implements IBlocks {
	localStore: BlockStore;

	private _responseHandler?: (data: BlockMessage, from?: string) => any;
	private _resolvers: Map<string, (data: Uint8Array) => Promise<void>>;
	private _blockCache?: Cache<Uint8Array>;
	private _providerCache?: Cache<string[]>;
	private readonly publicKeyHash: string;
	private readonly maxProviderHintsPerCid: number;
	private readonly maxRequeryOnReachable: number;

	private _loadFetchQueue: PQueue;
	private _readFromPeersPromises: Map<
		string,
		Promise<Block<any, any, any, 1> | undefined> | undefined
	>;
	_open = false;
	private _events: TypedEventEmitter<{
		"peer:reachable": CustomEvent<PublicSignKey>;
		"providers:hints": CustomEvent<{ cid: string }>;
	}> = new TypedEventEmitter();
	private closeController: AbortController = new AbortController();

	constructor(
		readonly options: {
			local: AnyBlockStore;
			localTimeout?: number;
			messageProcessingConcurrency?: number;
			publicKey: PublicSignKey;
			eagerBlocks?: boolean | { cacheSize?: number };
			/**
			 * Optional provider resolver used when `remote: true` is used without `remote.from`.
			 *
			 * This is intentionally best-effort and must be bounded; returning large lists is
			 * counterproductive at scale.
			 */
			resolveProviders?: (
				cid: string,
				options?: { signal?: AbortSignal },
			) => Promise<string[] | undefined> | string[] | undefined;
			/**
			 * Optional hook called after a block is stored locally (best-effort).
			 *
			 * Intended for wiring in discovery/provider announcements without coupling
			 * this transport to a specific directory implementation.
			 */
			onPut?: (cid: string) => Promise<void> | void;
			/**
			 * Cache of learned/suggested providers per CID to reduce repeated lookups and avoid
			 * expensive "search" behaviors.
			 */
			providerCache?:
				| boolean
				| {
						/** Max distinct CIDs kept in memory. */
						maxEntries?: number;
						/** Entry TTL in milliseconds. */
						ttlMs?: number;
						/** Max provider hashes stored per CID. */
						maxProvidersPerCid?: number;
				  };
			/**
			 * When a request is in-flight and new peers become reachable, re-issue the request
			 * a limited number of times (helps "get before connect" workflows).
			 */
			requeryOnReachable?: number;
			publish: (
				data: BlockRequest | BlockResponse,
				options: PublishOptions,
			) => Promise<Uint8Array | undefined | void>;
			waitFor: WaitForPeersFn;
		},
	) {
		const localTimeout = options?.localTimeout || 1000;
		this.publicKeyHash = options.publicKey.hashcode();
		this._loadFetchQueue = new PQueue({
			concurrency: options?.messageProcessingConcurrency || 10,
		});
		this.localStore = options?.local;
		this._resolvers = new Map();
		this._readFromPeersPromises = new Map();
		this._blockCache = options?.eagerBlocks
			? new Cache<Uint8Array>({
					max:
						typeof options.eagerBlocks === "boolean"
							? 1e3
							: (options.eagerBlocks.cacheSize ?? 1e3),
					ttl: 1e4,
				})
			: undefined;
		type ProviderCacheOptions = {
			maxEntries?: number;
			ttlMs?: number;
			maxProvidersPerCid?: number;
		};
		const providerCache: ProviderCacheOptions | undefined =
			options.providerCache === false
				? undefined
				: typeof options.providerCache === "object"
					? options.providerCache
					: {};
		this._providerCache = providerCache
			? new Cache<string[]>({
					max: providerCache.maxEntries ?? 2048,
					ttl: providerCache.ttlMs ?? 10 * 60 * 1000,
				})
			: undefined;
		this.maxProviderHintsPerCid = providerCache?.maxProvidersPerCid ?? 8;
		this.maxRequeryOnReachable = options.requeryOnReachable ?? 4;

		this._responseHandler = async (message: BlockMessage, from?: string) => {
			try {
				if (message instanceof BlockRequest && this.localStore) {
					this._loadFetchQueue
						.add(() => this.handleFetchRequest(message, localTimeout, from))
						.catch((e) => {
							try {
								dontThrowIfDeliveryError(e);
							} catch (error) {
								logger.error("Got error for libp2p block transport: ", error);
							}
						});
				} else if (message instanceof BlockResponse) {
					// TODO make sure we are not storing too much bytes in ram (like filter large blocks)
					if (from) {
						this.rememberProvider(message.cid, from);
					}
					let resolver = this._resolvers.get(message.cid);
					if (!resolver) {
						if (options.eagerBlocks) {
							// wait for the resolve to exist
							this._blockCache!.add(message.cid, message.bytes);
						}
					} else {
						await resolver(message.bytes);
					}
				}
			} catch (error) {
				logger.error("Got error for libp2p block transport: ", error);
				// timeout o r invalid cid
			}
		};
	}

	private normalizeProviderHints(
		providers: string[] | undefined,
		limit = this.maxProviderHintsPerCid || 8,
	): string[] {
		if (!providers || providers.length === 0) return [];
		const out: string[] = [];
		for (const p of providers) {
			if (!p) continue;
			if (p === this.publicKeyHash) continue;
			// Small bounded list; avoid Set allocations on hot paths.
			if (out.includes(p)) continue;
			out.push(p);
			if (out.length >= limit) break;
		}
		return out;
	}

	private rememberProvider(cidString: string, providerHash: string) {
		if (!this._providerCache) return;
		if (!providerHash || providerHash === this.publicKeyHash) return;
		const current = this._providerCache.get(cidString) ?? [];
		const next: string[] = [providerHash];
		for (const p of current) {
			if (p === providerHash) continue;
			if (!p || p === this.publicKeyHash) continue;
			next.push(p);
			if (next.length >= this.maxProviderHintsPerCid) break;
		}
		this._providerCache.add(cidString, next);
	}

	private rememberProviderHints(cidString: string, providers: string[]) {
		if (!this._providerCache) return;
		const normalized = this.normalizeProviderHints(providers);
		if (normalized.length === 0) return;
		this._providerCache.add(cidString, normalized);
	}

	private async resolveRemoteProviders(
		cidString: string,
		options?: { signal?: AbortSignal },
	): Promise<string[]> {
		// Priority:
		// 1. cached providers (from previous reads)
		// 2. resolveProviders hook (e.g. program-level replicators, DHT, tracker)
		const cached = this.normalizeProviderHints(
			this._providerCache?.get(cidString) ?? undefined,
		);
		if (cached.length > 0) return cached;
		if (!this.options.resolveProviders) return [];
		try {
			const resolved = await this.options.resolveProviders(cidString, options);
			const normalized = this.normalizeProviderHints(resolved);
			if (normalized.length > 0) {
				this.rememberProviderHints(cidString, normalized);
			}
			return normalized;
		} catch {
			return [];
		}
	}

	async put(
		bytes: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): Promise<string> {
		if (!this.localStore) {
			throw new Error("Local store not set");
		}
		const cid = await this.localStore!.put(bytes);
		try {
			await this.options.onPut?.(cid);
		} catch {
			// ignore best-effort hooks
		}
		return cid;
	}

	async has(cid: string) {
		return this.localStore.has(cid);
	}

	async get(
		cid: string,
		options?: GetOptions | undefined,
	): Promise<Uint8Array | undefined> {
		let value = this.localStore
			? await this.localStore.get(cid, options)
			: undefined;

		if (!value) {
			// try to get it remotelly
			const remoteOptions = options?.remote === true ? {} : options?.remote;
			if (remoteOptions) {
				const cidObject = cidifyString(cid);
				value = await this._readFromPeers(cid, cidObject, remoteOptions);
				if (remoteOptions?.replicate && value) {
					await this.put(value);
				}
			}
		}

		return value;
	}

	hintProviders(cid: string, providers: string[]) {
		const cidString = stringifyCid(cid);
		this.rememberProviderHints(cidString, providers);
		this._events.dispatchEvent(
			new CustomEvent("providers:hints", { detail: { cid: cidString } }),
		);
	}

	async rm(cid: string) {
		await this.localStore?.rm(cid);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this.localStore.iterator()) {
			yield [key, value];
		}
	}

	async start(): Promise<void> {
		this._events = new TypedEventEmitter();
		this.closeController = new AbortController();
		await this.localStore?.start();
		this._open = true;
	}

	onMessage(data: BlockMessage, from?: string) {
		return this._responseHandler!(data, from);
	}
	onReachable(publicKey: PublicSignKey) {
		this._events.dispatchEvent(
			new CustomEvent("peer:reachable", { detail: publicKey }),
		);
	}

	private async handleFetchRequest(
		request: BlockRequest,
		localTimeout: number,
		from?: string,
	) {
		if (!from) {
			warn("No from in handleFetchRequest");
			return;
		}
		const cid = stringifyCid(request.cid);
		let bytes = await this.localStore.get(cid, {
			remote: {
				timeout: localTimeout,
			},
		});

		if (!bytes) {
			// Best-effort relay/proxy: if we don't have the block locally, try to fetch it
			// from other reachable peers and then respond to the requester.
			//
			// This keeps multi-hop topologies working (e.g. A <-> relay <-> B) without
			// requiring the requester to know an explicit `remote.from` provider set.
			try {
				const cidObject = cidifyString(cid);
				const proxyTimeoutMs = Math.max(1_000, Math.floor(localTimeout) || 0);
				const controller = new AbortController();
				const timer = setTimeout(() => controller.abort(), proxyTimeoutMs);
				try {
					const candidates = await this.resolveRemoteProviders(cid, {
						signal: controller.signal,
					});
					// Never bounce the request back to the requester; keep the fanout bounded.
					const providers = candidates
						.filter((p) => p && p !== from)
						.slice(0, this.maxProviderHintsPerCid);
					if (providers.length > 0) {
						bytes = await this._readFromPeers(cid, cidObject, {
							signal: controller.signal,
							timeout: proxyTimeoutMs,
							from: providers,
						});
					}
				} finally {
					clearTimeout(timer);
				}
			} catch {
				// ignore proxy failures
			}
		}

		if (!bytes) return;
		await this.options
			.publish(new BlockResponse(cid, bytes), { to: [from] })
			.catch(dontThrowIfDeliveryError);
	}

	private async _readFromPeers(
		cidString: string,
		cidObject: CID,
		options: {
			signal?: AbortSignal;
			timeout?: number;
			hasher?: any;
			from?: string[];
		} = {},
	): Promise<Uint8Array | undefined> {
		const codec = (codecCodes as any)[cidObject.code];

		const tryDecode = async (bytes: Uint8Array) => {
			const value = await checkDecodeBlock(cidObject, bytes, {
				codec,
				hasher: options?.hasher,
			});

			return value;
		};
		const cachedValue = this.options.eagerBlocks
			? this._blockCache?.get(cidString)
			: undefined;
		if (cachedValue) {
			this._blockCache!.del(cidString);
			try {
				const result = await tryDecode(cachedValue);
				return result.bytes;
			} catch (error) {
				// ignore
			}
		}

		const explicitFrom = this.normalizeProviderHints(options.from);
		let providers =
			explicitFrom.length > 0
				? explicitFrom
				: await this.resolveRemoteProviders(cidString, { signal: options.signal });
		const canResolveLater =
			explicitFrom.length === 0 && typeof this.options.resolveProviders === "function";
		if (providers.length === 0 && !canResolveLater) {
			// Without an explicit provider set (or a resolver), we intentionally do not
			// fall back to network-wide flooding. Scalable deployments must provide a
			// discovery mechanism (program-level hints, DHT/tracker, etc).
			return undefined;
		}
		if (explicitFrom.length > 0 && providers.length > 0) {
			this.rememberProviderHints(cidString, providers);
		}

		let promise = this._readFromPeersPromises.get(cidString);
		if (!promise) {
			promise = new Promise<Block<any, any, any, 1> | undefined>(
				(resolve, reject) => {
					let timeoutCallback: ReturnType<typeof setTimeout> | undefined;
					const abortHandler = () => {
						cleanup();
						reject(new AbortError());
					};

					const cleanup = () => {
						if (timeoutCallback) clearTimeout(timeoutCallback);
						this._resolvers.delete(cidString);
						this.closeController.signal.removeEventListener(
							"abort",
							abortHandler,
						);
						options?.signal?.removeEventListener("abort", abortHandler);
					};

					timeoutCallback = setTimeout(() => {
						cleanup();
						resolve(undefined);
					}, options.timeout || 30 * 1000);

					this.closeController.signal.addEventListener("abort", abortHandler);
					options?.signal?.addEventListener("abort", abortHandler);

					this._resolvers.set(cidString, async (bytes: Uint8Array) => {
						const value = await tryDecode(bytes);
						cleanup();
						resolve(value);
					});
				},
			);

			this._readFromPeersPromises.set(cidString, promise);

			let requeryCount = 0;
			const tryPublishRequest = async () => {
				if (requeryCount >= this.maxRequeryOnReachable) return;
				if (providers.length === 0) {
					providers = await this.resolveRemoteProviders(cidString, {
						signal: options.signal,
					});
				}
				if (providers.length === 0) return;
				try {
					await this.options.publish(new BlockRequest(cidString), {
						mode: new SilentDelivery({ to: providers, redundancy: 1 }),
					});
					requeryCount += 1;
				} catch (e) {
					dontThrowIfDeliveryError(e);
				}
			};

			const publishOnNewPeers = () => {
				// Re-issue when reachability changes to handle "get before connect".
				// Bounded to avoid accidental amplification at large scale.
				if (requeryCount >= this.maxRequeryOnReachable) return;
				tryPublishRequest().catch(dontThrowIfDeliveryError);
			};

			const publishOnProviderHints = (ev: CustomEvent<{ cid: string }>) => {
				if (requeryCount >= this.maxRequeryOnReachable) return;
				if (!ev?.detail?.cid) return;
				if (ev.detail.cid !== cidString) return;
				tryPublishRequest().catch(dontThrowIfDeliveryError);
			};

			this._events.addEventListener("peer:reachable", publishOnNewPeers);
			this._events.addEventListener("providers:hints", publishOnProviderHints);
			try {
				await tryPublishRequest();
				const result = await promise;
				return result?.bytes;
			} finally {
				this._readFromPeersPromises.delete(cidString);
				this._events.removeEventListener("peer:reachable", publishOnNewPeers);
				this._events.removeEventListener("providers:hints", publishOnProviderHints);
			}
		} else {
			const result = await promise;
			return result?.bytes;
		}
	}

	async stop(): Promise<void> {
		// Dont listen for more incoming messages

		// Wait for processing request
		this.closeController.abort();
		this._loadFetchQueue.clear();
		await this._loadFetchQueue.onIdle(); // wait for pending
		await this.localStore?.stop();
		this._readFromPeersPromises.clear();
		this._resolvers.clear();
		this._blockCache?.clear();
		this._providerCache?.clear();
		this._open = false;
		// we dont cleanup subscription because we dont know if someone else is sbuscribing also
	}

	waitFor(
		peer: PeerRefs,
		options?: WaitForPresentOpts | WaitForAnyOpts,
	): Promise<string[]> {
		return this.options.waitFor(peer, options);
	}

	async size() {
		return this.localStore.size();
	}

	get status() {
		if (this._open) {
			return this.localStore?.status();
		} else {
			return "closed";
		}
	}

	persisted(): boolean | Promise<boolean> {
		return this.localStore?.persisted() || false;
	}
}
