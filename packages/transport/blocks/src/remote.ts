import { field, variant } from "@dao-xyz/borsh";
import { TypedEventEmitter } from "@libp2p/interface";
import {
	type GetOptions,
	type Blocks as IBlocks,
	cidifyString,
	codecCodes,
	codecMap,
	defaultHasher,
	stringifyCid,
	verifyBlockBytes,
} from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import { PublicSignKey } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import {
	type PublishOptions,
	type RustBlockExchange,
	type RustBlockProviderCache,
	dontThrowIfDeliveryError,
} from "@peerbit/stream";
import {
	BACKGROUND_MESSAGE_PRIORITY,
	FOREGROUND_READ_MESSAGE_PRIORITY,
	type PeerRefs,
	type RequestTransportContext,
	SilentDelivery,
	type WaitForAnyOpts,
	type WaitForPeersFn,
	type WaitForPresentOpts,
} from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { CID } from "multiformats";
import { type Block } from "multiformats/block";
import PQueue from "p-queue";
import {
	BoundedEagerBlockCache,
	type EagerBlockCache,
	type EagerBlocksSetting,
	MAX_EAGER_BLOCK_CID_LENGTH,
	type NormalizedEagerBlocksOptions,
	normalizeEagerBlocksOptions,
} from "./eager-cache.js";
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

type BlockMessageContext = {
	from?: string;
	transport?: RequestTransportContext;
};

type InFlightRead = {
	promise: Promise<Block<any, any, any, 1> | undefined>;
	addProviders: (providers: string[]) => void;
	promotePriority: (priority: number | undefined) => void;
};

type RemoteReadOptions = Exclude<GetOptions["remote"], boolean | undefined> & {
	hasher?: any;
};

export type EagerBlockCacheTelemetry = {
	entries: number;
	bytes: number;
	peakEntries: number;
	peakBytes: number;
	evictions: number;
	expirations: number;
	pendingEntries: number;
	pendingBytes: number;
	peakPendingEntries: number;
	peakPendingBytes: number;
	admitted: number;
	hits: number;
	rejectedCid: number;
	rejectedCodec: number;
	rejectedSize: number;
	rejectedPending: number;
	rejectedIntegrity: number;
	rejectedLifecycle: number;
	limits: NormalizedEagerBlocksOptions;
};

type EagerAdmissionCounters = Omit<
	EagerBlockCacheTelemetry,
	| "entries"
	| "bytes"
	| "peakEntries"
	| "peakBytes"
	| "evictions"
	| "expirations"
	| "limits"
>;

export class RemoteBlocks implements IBlocks {
	localStore: BlockStore;

	// Assigned in the constructor only when the local store supports it:
	// callers feature-detect this method (the log's columnar raw-receive
	// fast path), so it must not exist without a delegation target.
	putKnownManyColumns?: (cids: string[], bytes: Uint8Array[]) => string[];
	// Native commit callbacks are synchronous, so a write-through local store
	// exposes an explicit post-commit durability barrier for the lower log.
	waitForDurableWrites?: () => Promise<void> | void;
	// Durable native stores expose a synchronous poison guard so lower-log
	// mutation entry points can reject before changing graph/index state.
	throwIfDurableWritesFailed?: () => void;
	rollbackFailedNativeCommits?: (
		cids: string[],
		restoreNativeCids?: string[],
		ownershipToken?: unknown,
	) => Promise<void>;
	acknowledgeNativeCommitOwnership?: (ownershipToken: unknown) => void;
	// Native trim uses the same feature-detection pattern to mirror a hot-store
	// deletion into a write-through store without re-entering generic rmMany.
	rmManyAfterNativeDelete?: (
		cids: string[],
		cleanupToken?: unknown,
	) => Promise<number | void> | number | void;

	private _responseHandler?: (
		data: BlockMessage,
		context?: BlockMessageContext,
	) => any;
	private _resolvers: Map<
		string,
		(data: Uint8Array, providerHash?: string) => Promise<void>
	>;
	private _blockCache?: EagerBlockCache;
	private _eagerBlocksOptions?: NormalizedEagerBlocksOptions;
	private _eagerValidationQueue?: PQueue;
	private _eagerAdmission: EagerAdmissionCounters = {
		pendingEntries: 0,
		pendingBytes: 0,
		peakPendingEntries: 0,
		peakPendingBytes: 0,
		admitted: 0,
		hits: 0,
		rejectedCid: 0,
		rejectedCodec: 0,
		rejectedSize: 0,
		rejectedPending: 0,
		rejectedIntegrity: 0,
		rejectedLifecycle: 0,
	};
	private _providerCache?: Cache<string[]>;
	private _rustProviderCache?: RustBlockProviderCache;
	private readonly rustExchange?: RustBlockExchange;
	private readonly publicKeyHash: string;
	private readonly maxProviderHintsPerCid: number;
	private readonly maxRequeryOnReachable: number;

	private _loadFetchQueue: PQueue;
	private _backgroundLoadFetchQueue: PQueue;
	private _readFromPeersPromises: Map<string, InFlightRead>;
	private _deferredStoredNotificationCids?: Set<string>;
	private _deferredStoredNotificationTimer?: ReturnType<typeof setTimeout>;
	_open = false;
	private _events: TypedEventEmitter<{
		"peer:reachable": CustomEvent<PublicSignKey>;
		"providers:hints": CustomEvent<{ cid: string }>;
	}> = new TypedEventEmitter();
	private closeController: AbortController = new AbortController();

	constructor(
		readonly options: {
			local: BlockStore;
			localTimeout?: number;
			messageProcessingConcurrency?: number;
			publicKey: PublicSignKey;
			eagerBlocks?: EagerBlocksSetting;
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
			 * Optional push-based provider watcher used to wake pending remote reads as soon as
			 * provider availability changes.
			 *
			 * This complements `resolveProviders` with an event-first path. Implementations may
			 * still rely on best-effort discovery internally, but should invoke `onProviders`
			 * promptly when new candidates become known.
			 */
			watchProviders?: (
				cid: string,
				options: {
					signal?: AbortSignal;
					onProviders: (providers: string[]) => void;
				},
			) => void | { close: () => void } | (() => void);
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
			/**
			 * Native block-exchange components (rust-core mode). When set, the
			 * provider caches/decisions and eager-block bookkeeping run in the
			 * native core; `publishRaw` additionally enables serving natively
			 * stored blocks as wasm-serialized payloads that never surface the
			 * block bytes to JS.
			 */
			rust?: {
				exchange: RustBlockExchange;
				publishRaw?: (
					payload: Uint8Array,
					options: PublishOptions,
				) => Promise<Uint8Array | undefined | void>;
			};
		},
	) {
		const localTimeout = options?.localTimeout || 1000;
		this.publicKeyHash = options.publicKey.hashcode();
		this.rustExchange = options.rust?.exchange;
		const messageProcessingConcurrency =
			options?.messageProcessingConcurrency || 10;
		this._loadFetchQueue = new PQueue({
			concurrency: messageProcessingConcurrency,
		});
		// A provider handler includes the response publication. When every handler
		// is publishing bulk/background blocks under backpressure, a single priority
		// queue cannot preempt that already-active work. Admit at most N-1 background
		// handlers so foreground reads can use the reserved slot whenever the
		// configured concurrency is at least two. Foreground-only traffic can still
		// use the full configured concurrency.
		this._backgroundLoadFetchQueue = new PQueue({
			concurrency: Math.max(1, messageProcessingConcurrency - 1),
		});
		this.localStore = options?.local;
		const localPutKnownManyColumns = (
			this.localStore as {
				putKnownManyColumns?: (cids: string[], bytes: Uint8Array[]) => string[];
			}
		)?.putKnownManyColumns;
		if (typeof localPutKnownManyColumns === "function") {
			this.putKnownManyColumns = (cids, bytes) => {
				const stored = localPutKnownManyColumns.call(
					this.localStore,
					cids,
					bytes,
				);
				const durableBarrier = this.waitForDurableWrites?.();
				if (durableBarrier && typeof durableBarrier.then === "function") {
					void Promise.resolve(durableBarrier).then(
						() => this.notifyPuts(stored),
						(): void => undefined,
					);
				} else {
					void this.notifyPuts(stored);
				}
				return stored;
			};
		}
		const localWaitForDurableWrites = (
			this.localStore as {
				waitForDurableWrites?: () => Promise<void> | void;
			}
		).waitForDurableWrites;
		if (typeof localWaitForDurableWrites === "function") {
			this.waitForDurableWrites = () =>
				localWaitForDurableWrites.call(this.localStore);
		}
		const localThrowIfDurableWritesFailed = (
			this.localStore as {
				throwIfDurableWritesFailed?: () => void;
			}
		).throwIfDurableWritesFailed;
		if (typeof localThrowIfDurableWritesFailed === "function") {
			this.throwIfDurableWritesFailed = () =>
				localThrowIfDurableWritesFailed.call(this.localStore);
		}
		const localRollbackFailedNativeCommits = (
			this.localStore as {
				rollbackFailedNativeCommits?: (
					cids: string[],
					restoreNativeCids?: string[],
					ownershipToken?: unknown,
				) => Promise<void>;
			}
		).rollbackFailedNativeCommits;
		if (typeof localRollbackFailedNativeCommits === "function") {
			this.rollbackFailedNativeCommits = (
				cids,
				restoreNativeCids,
				ownershipToken,
			) =>
				localRollbackFailedNativeCommits.call(
					this.localStore,
					cids,
					restoreNativeCids,
					ownershipToken,
				);
		}
		const localAcknowledgeNativeCommitOwnership = (
			this.localStore as {
				acknowledgeNativeCommitOwnership?: (ownershipToken: unknown) => void;
			}
		).acknowledgeNativeCommitOwnership;
		if (typeof localAcknowledgeNativeCommitOwnership === "function") {
			this.acknowledgeNativeCommitOwnership = (ownershipToken) =>
				localAcknowledgeNativeCommitOwnership.call(
					this.localStore,
					ownershipToken,
				);
		}
		const localRmManyAfterNativeDelete = (
			this.localStore as {
				rmManyAfterNativeDelete?: (
					cids: string[],
					cleanupToken?: unknown,
				) => Promise<number | void> | number | void;
			}
		).rmManyAfterNativeDelete;
		if (typeof localRmManyAfterNativeDelete === "function") {
			this.rmManyAfterNativeDelete = (cids, cleanupToken) =>
				localRmManyAfterNativeDelete.call(this.localStore, cids, cleanupToken);
		}
		this._resolvers = new Map();
		this._readFromPeersPromises = new Map();
		if (options?.eagerBlocks) {
			this._eagerBlocksOptions = normalizeEagerBlocksOptions(
				options.eagerBlocks,
			);
			this._eagerValidationQueue = new PQueue({
				concurrency: this._eagerBlocksOptions.validationConcurrency,
			});
			this._blockCache = this.rustExchange
				? this.rustExchange.createEagerCache({
						maxEntries: this._eagerBlocksOptions.maxEntries,
						maxBytes: this._eagerBlocksOptions.maxBytes,
						ttlMs: this._eagerBlocksOptions.ttlMs,
					})
				: new BoundedEagerBlockCache({
						maxEntries: this._eagerBlocksOptions.maxEntries,
						maxBytes: this._eagerBlocksOptions.maxBytes,
						ttlMs: this._eagerBlocksOptions.ttlMs,
					});
		}
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
		if (providerCache) {
			if (this.rustExchange) {
				this._rustProviderCache = this.rustExchange.createProviderCache({
					me: this.publicKeyHash,
					maxEntries: providerCache.maxEntries ?? 2048,
					ttlMs: providerCache.ttlMs ?? 10 * 60 * 1000,
					maxProvidersPerCid: providerCache.maxProvidersPerCid ?? 8,
				});
			} else {
				this._providerCache = new Cache<string[]>({
					max: providerCache.maxEntries ?? 2048,
					ttl: providerCache.ttlMs ?? 10 * 60 * 1000,
				});
			}
		}
		this.maxProviderHintsPerCid = providerCache?.maxProvidersPerCid ?? 8;
		this.maxRequeryOnReachable = options.requeryOnReachable ?? 4;

		this._responseHandler = async (
			message: BlockMessage,
			context?: BlockMessageContext,
		) => {
			try {
				if (message instanceof BlockRequest && this.localStore) {
					const priority =
						context?.transport?.requestPriority ?? BACKGROUND_MESSAGE_PRIORITY;
					const queueSignal = this.closeController.signal;
					const run = () =>
						this._loadFetchQueue.add(
							() =>
								queueSignal.aborted
									? undefined
									: this.handleFetchRequest(message, localTimeout, context),
							{
								priority,
							},
						);
					const scheduled =
						priority >= FOREGROUND_READ_MESSAGE_PRIORITY
							? run()
							: this._backgroundLoadFetchQueue.add(
									() => (queueSignal.aborted ? undefined : run()),
									{ priority },
								);
					scheduled.catch((e) => {
						if (queueSignal.aborted) return;
						try {
							dontThrowIfDeliveryError(e);
						} catch (error) {
							logger.error("Got error for libp2p block transport: ", error);
						}
					});
				} else if (message instanceof BlockResponse) {
					const resolver = this._resolvers.get(message.cid);
					if (!resolver) {
						this.queueEagerBlock(message.cid, message.bytes, context?.from);
					} else {
						await resolver(message.bytes, context?.from);
					}
				}
			} catch (error) {
				logger.error("Got error for libp2p block transport: ", error);
				// timeout o r invalid cid
			}
		};
	}

	getNativeLogBlockStoreHandle(): unknown {
		return this.localStore.getNativeLogBlockStoreHandle?.();
	}

	/** Snapshot of bounded eager-response admission and retention state. */
	getEagerBlockCacheTelemetry(): EagerBlockCacheTelemetry | undefined {
		if (!this._blockCache || !this._eagerBlocksOptions) return undefined;
		return {
			...this._blockCache.stats(),
			...this._eagerAdmission,
			limits: { ...this._eagerBlocksOptions },
		};
	}

	/** Primarily useful for diagnostics and deterministic tests. */
	waitForEagerBlockValidation(): Promise<void> {
		return this._eagerValidationQueue?.onIdle() ?? Promise.resolve();
	}

	private queueEagerBlock(
		cidString: string,
		incomingBytes: Uint8Array,
		providerHash?: string,
	): void {
		const limits = this._eagerBlocksOptions;
		const queue = this._eagerValidationQueue;
		const cache = this._blockCache;
		if (!limits || !queue || !cache) return;

		const generationSignal = this.closeController.signal;
		if (generationSignal.aborted) {
			this._eagerAdmission.rejectedLifecycle += 1;
			return;
		}
		if (!cidString || cidString.length > MAX_EAGER_BLOCK_CID_LENGTH) {
			this._eagerAdmission.rejectedCid += 1;
			return;
		}

		let cidObject: CID;
		let canonicalCid: string;
		try {
			cidObject = cidifyString(cidString);
			canonicalCid = stringifyCid(cidObject);
			if (
				cidObject.multihash.code !== defaultHasher.code ||
				canonicalCid.length > MAX_EAGER_BLOCK_CID_LENGTH
			) {
				throw new Error("unsupported eager block cid");
			}
		} catch {
			this._eagerAdmission.rejectedCid += 1;
			return;
		}
		// Logical DAG-CBOR decoding can materialize even hash-valid attacker-
		// controlled object graphs and expand a small wire payload by orders of
		// magnitude. Eager admission therefore verifies only raw bytes; all other
		// codecs remain available through the requested-response path.
		if (cidObject.code !== codecMap.raw.code) {
			this._eagerAdmission.rejectedCodec += 1;
			return;
		}
		const codec = codecMap.raw;

		const byteLength = incomingBytes.byteLength;
		if (
			byteLength > limits.maxBlockBytes ||
			byteLength > limits.maxBytes ||
			byteLength > limits.maxPendingBytes
		) {
			this._eagerAdmission.rejectedSize += 1;
			return;
		}
		if (
			this._eagerAdmission.pendingEntries >= limits.maxPendingEntries ||
			this._eagerAdmission.pendingBytes + byteLength > limits.maxPendingBytes
		) {
			this._eagerAdmission.rejectedPending += 1;
			return;
		}

		// The decoder commonly returns a subarray into the complete network frame.
		// Copy exactly the block bytes before queuing so no larger backing buffer or
		// response object remains retained while integrity validation is pending.
		const bytes = new Uint8Array(byteLength);
		bytes.set(incomingBytes);
		this._eagerAdmission.pendingEntries += 1;
		this._eagerAdmission.pendingBytes += byteLength;
		this._eagerAdmission.peakPendingEntries = Math.max(
			this._eagerAdmission.peakPendingEntries,
			this._eagerAdmission.pendingEntries,
		);
		this._eagerAdmission.peakPendingBytes = Math.max(
			this._eagerAdmission.peakPendingBytes,
			this._eagerAdmission.pendingBytes,
		);

		let released = false;
		const releaseReservation = () => {
			if (released) return;
			released = true;
			this._eagerAdmission.pendingEntries -= 1;
			this._eagerAdmission.pendingBytes -= byteLength;
		};
		const validateAndAdmit = async () => {
			try {
				if (generationSignal.aborted) {
					this._eagerAdmission.rejectedLifecycle += 1;
					return;
				}
				try {
					await this.validateEagerBlock(cidObject, bytes, codec);
				} catch {
					this._eagerAdmission.rejectedIntegrity += 1;
					return;
				}
				if (generationSignal.aborted) {
					this._eagerAdmission.rejectedLifecycle += 1;
					return;
				}
				const resolver =
					this._resolvers.get(canonicalCid) ?? this._resolvers.get(cidString);
				if (resolver) {
					try {
						// A read can install its resolver while eager validation is in
						// progress. Let that resolver enforce its own (possibly custom)
						// hasher contract and learn the provider only if it succeeds.
						await resolver(bytes, providerHash);
					} catch {
						// Match an invalid active response: drop it and leave the read open.
					}
					return;
				}
				if (!cache.add(canonicalCid, bytes)) {
					this._eagerAdmission.rejectedSize += 1;
					return;
				}
				this._eagerAdmission.admitted += 1;
				if (providerHash) {
					// An unsolicited response is only a provider signal after both its
					// CID and payload have passed the integrity gate.
					this.rememberProvider(canonicalCid, providerHash);
				}
			} finally {
				releaseReservation();
			}
		};
		try {
			void queue.add(validateAndAdmit).catch(() => {
				releaseReservation();
				if (!generationSignal.aborted) {
					this._eagerAdmission.rejectedLifecycle += 1;
				}
			});
		} catch {
			releaseReservation();
			this._eagerAdmission.rejectedLifecycle += 1;
		}
	}

	private validateEagerBlock(
		cid: CID,
		bytes: Uint8Array,
		codec: (typeof codecCodes)[keyof typeof codecCodes],
	) {
		return verifyBlockBytes(cid, bytes, { codec });
	}

	private normalizeProviderHints(
		providers: string[] | undefined,
		limit = this.maxProviderHintsPerCid || 8,
	): string[] {
		if (!providers || providers.length === 0) return [];
		if (this.rustExchange) {
			return this.rustExchange.normalizeProviderHints(
				providers,
				this.publicKeyHash,
				limit,
			);
		}
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
		if (this._rustProviderCache) {
			this._rustProviderCache.rememberProvider(cidString, providerHash);
			return;
		}
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
		if (this._rustProviderCache) {
			this._rustProviderCache.rememberHints(cidString, providers);
			return;
		}
		if (!this._providerCache) return;
		const normalized = this.normalizeProviderHints(providers);
		if (normalized.length === 0) return;
		this._providerCache.add(cidString, normalized);
	}

	private getCachedProviders(cidString: string): string[] | undefined {
		return this._rustProviderCache
			? this._rustProviderCache.get(cidString)
			: (this._providerCache?.get(cidString) ?? undefined);
	}

	private pickRequestBatch(providers: string[], attempt: number): string[] {
		if (this.rustExchange) {
			return this.rustExchange.pickRequestBatch(
				providers,
				this.publicKeyHash,
				attempt,
			);
		}
		if (providers.length <= 1) {
			return providers;
		}
		const batchSize = Math.min(2, providers.length);
		const start = (attempt * batchSize) % providers.length;
		const batch: string[] = [];
		for (let i = 0; i < batchSize; i++) {
			batch.push(providers[(start + i) % providers.length]);
		}
		return this.normalizeProviderHints(batch, batchSize);
	}

	private async resolveRemoteProviders(
		cidString: string,
		options?: { signal?: AbortSignal; refresh?: boolean },
	): Promise<string[]> {
		// Priority:
		// 1. cached providers (from previous reads)
		// 2. resolveProviders hook (e.g. program-level replicators, DHT, tracker)
		const cached = this.normalizeProviderHints(
			this.getCachedProviders(cidString),
		);
		if (!this.options.resolveProviders) return cached;
		if (cached.length > 0 && !options?.refresh) return cached;
		try {
			const resolved = await this.options.resolveProviders(cidString, options);
			const normalized = this.normalizeProviderHints([
				...cached,
				...(resolved ?? []),
			]);
			if (normalized.length > 0) {
				this.rememberProviderHints(cidString, normalized);
			}
			return normalized;
		} catch {
			return cached;
		}
	}

	async put(
		bytes: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): Promise<string> {
		if (!this.localStore) {
			throw new Error("Local store not set");
		}
		const cid = await this.localStore!.put(bytes);
		await this.notifyPut(cid);
		return cid;
	}

	async putMany(
		blocks: Array<
			Uint8Array | { block: Block<any, any, any, any>; cid: string }
		>,
	): Promise<string[]> {
		if (!this.localStore) {
			throw new Error("Local store not set");
		}
		const cids = await this.localStore.putMany(blocks);
		await this.notifyPuts(cids);
		return cids;
	}

	async putKnown(cid: string, bytes: Uint8Array): Promise<string> {
		if (!this.localStore) {
			throw new Error("Local store not set");
		}
		const storedCid = await this.localStore.putKnown(cid, bytes);
		await this.notifyPut(storedCid);
		return storedCid;
	}

	async putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): Promise<string[]> {
		if (!this.localStore) {
			throw new Error("Local store not set");
		}
		const cids = await this.localStore.putKnownMany(blocks);
		await this.notifyPuts(cids);
		return cids;
	}

	hasNotifyStoredHook(): boolean {
		return !!this.options.onPut;
	}

	notifyStored(cid: string): Promise<void> | void {
		return this.notifyPut(cid);
	}

	notifyStoredMany(cids: string[]): Promise<void> | void {
		return this.notifyPuts(cids);
	}

	notifyStoredDeferred(cid: string): void {
		this.notifyStoredManyDeferred([cid]);
	}

	notifyStoredManyDeferred(cids: string[]): void {
		if (!this.options.onPut || cids.length === 0) {
			return;
		}
		let pending = this._deferredStoredNotificationCids;
		if (!pending) {
			pending = new Set();
			this._deferredStoredNotificationCids = pending;
		}
		for (const cid of cids) {
			if (cid) {
				pending.add(cid);
			}
		}
		if (pending.size === 0 || this._deferredStoredNotificationTimer) {
			return;
		}
		this._deferredStoredNotificationTimer = setTimeout(() => {
			this._deferredStoredNotificationTimer = undefined;
			const flushed = this.flushDeferredStoredNotifications();
			if (flushed && typeof flushed.catch === "function") {
				flushed.catch((): void => undefined);
			}
		}, 0);
	}

	private flushDeferredStoredNotifications(): Promise<void> | void {
		const pending = this._deferredStoredNotificationCids;
		if (!pending || pending.size === 0) {
			return;
		}
		this._deferredStoredNotificationCids = undefined;
		const waits: Promise<void>[] = [];
		for (const cid of pending) {
			try {
				const result = this.notifyStored(cid);
				if (result && typeof result.then === "function") {
					waits.push(result);
				}
			} catch {
				// ignore best-effort hooks
			}
		}
		if (waits.length > 0) {
			return Promise.all(waits).then((): void => undefined);
		}
	}

	private notifyPut(cid: string): Promise<void> | void {
		const onPut = this.options.onPut;
		if (!onPut) {
			return;
		}
		try {
			const result = onPut(cid);
			if (result && typeof result.then === "function") {
				return result.catch((): void => undefined);
			}
		} catch {
			// ignore best-effort hooks
		}
	}

	private notifyPuts(cids: string[]): Promise<void> | void {
		const onPut = this.options.onPut;
		if (!onPut || cids.length === 0) {
			return;
		}
		if (cids.length === 1) {
			return this.notifyPut(cids[0]!);
		}
		return Promise.all(
			cids.map(async (cid) => {
				try {
					await onPut(cid);
				} catch {
					// ignore best-effort hooks
				}
			}),
		).then((): void => undefined);
	}

	async has(cid: string) {
		return this.localStore.has(cid);
	}

	async hasMany(cids: string[]): Promise<boolean[]> {
		return this.localStore.hasMany(cids);
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
					// _readFromPeers verifies the response bytes against cid before it
					// resolves, so avoid hashing the full block a second time here.
					await this.putKnown(cid, value);
				}
			}
		}

		return value;
	}

	async getMany(
		cids: string[],
		options?: GetOptions | undefined,
	): Promise<Array<Uint8Array | undefined>> {
		if (!options?.remote) {
			return this.localStore.getMany(cids, options);
		}
		return Promise.all(cids.map((cid) => this.get(cid, options)));
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

	async rmMany(cids: string[]) {
		await this.localStore?.rmMany(cids);
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

	onMessage(data: BlockMessage, context?: BlockMessageContext) {
		return this._responseHandler!(data, context);
	}
	onReachable(publicKey: PublicSignKey) {
		this._events.dispatchEvent(
			new CustomEvent("peer:reachable", { detail: publicKey }),
		);
	}

	private async handleFetchRequest(
		request: BlockRequest,
		localTimeout: number,
		context?: BlockMessageContext,
	) {
		const from = context?.from;
		if (!from) {
			warn("No from in handleFetchRequest");
			return;
		}
		const cid = stringifyCid(request.cid);
		const publishRaw = this.options.rust?.publishRaw;
		if (publishRaw) {
			// Native-store-served response: the borsh BlockResponse payload is
			// serialized inside wasm straight from the native log block store,
			// so the block bytes never materialize as a JS value.
			const payload = this.localStore.getBlockResponsePayload?.(cid);
			if (payload) {
				const responsePublishOptions = context?.transport
					? context.transport.withResponseOptions({ to: [from] })
					: { to: [from] };
				await publishRaw(payload, responsePublishOptions).catch(
					dontThrowIfDeliveryError,
				);
				return;
			}
		}
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
				const inheritedTimeoutMs = context?.transport
					? Math.floor(context.transport.remainingTime())
					: undefined;
				if (inheritedTimeoutMs != null && inheritedTimeoutMs <= 0) {
					return;
				}
				const proxyTimeoutMs =
					inheritedTimeoutMs != null
						? Math.max(1, Math.min(60_000, inheritedTimeoutMs))
						: Math.max(10_000, Math.floor(localTimeout) || 0);
				const controller = new AbortController();
				const abortOnClose = () => controller.abort();
				this.closeController.signal.addEventListener("abort", abortOnClose, {
					once: true,
				});
				if (this.closeController.signal.aborted) {
					abortOnClose();
				}
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
							priority: context?.transport?.requestPriority,
						});
					}
				} finally {
					clearTimeout(timer);
					this.closeController.signal.removeEventListener(
						"abort",
						abortOnClose,
					);
				}
			} catch {
				// ignore proxy failures
			}
		}

		if (!bytes) return;
		const responsePublishOptions = context?.transport
			? context.transport.withResponseOptions({ to: [from] })
			: { to: [from] };
		await this.options
			.publish(new BlockResponse(cid, bytes), responsePublishOptions)
			.catch(dontThrowIfDeliveryError);
	}

	private async _readFromPeers(
		cidString: string,
		cidObject: CID,
		options: RemoteReadOptions = {},
	): Promise<Uint8Array | undefined> {
		// Capture the controller for this open/close generation. start() replaces the
		// field, so cleanup must remove listeners from the signal it registered on.
		const closeSignal = this.closeController.signal;
		const readWasAborted = () =>
			closeSignal.aborted || options.signal?.aborted === true;
		const throwIfReadWasAborted = () => {
			if (readWasAborted()) {
				throw new AbortError();
			}
		};
		throwIfReadWasAborted();

		const codec = codecCodes[cidObject.code as keyof typeof codecCodes];

		const tryVerify = async (bytes: Uint8Array) => {
			const verifiedCid = await verifyBlockBytes(cidObject, bytes, {
				codec,
				hasher: options?.hasher,
			});
			// This block-shaped value is internal bookkeeping only. RemoteBlocks moves
			// opaque bytes and must not invoke the logical codec at this boundary.
			return {
				bytes,
				cid: verifiedCid,
				value: bytes,
			} as Block<Uint8Array, any, any, 1>;
		};
		const eagerCacheKey = stringifyCid(cidObject);
		const consumeCachedValue = (): Uint8Array | undefined => {
			if (options.hasher && options.hasher !== defaultHasher) {
				// Eager admission proves the built-in SHA-256 contract only. Keep this
				// lookup synchronous and leave custom hashers on the requested-response
				// path; asynchronously revalidating cached entries would reopen a race
				// before the read resolver is installed.
				return undefined;
			}
			const cachedValue = this._blockCache?.get(eagerCacheKey);
			if (cachedValue === undefined) return undefined;
			this._blockCache!.del(eagerCacheKey);
			this._eagerAdmission.hits += 1;
			// Eager entries are inserted only after verifyBlockBytes succeeds. The
			// one-shot cache hit therefore does not hash the full block a second time.
			return cachedValue;
		};
		let cachedResult = consumeCachedValue();
		if (cachedResult) return cachedResult;

		const explicitFrom = this.normalizeProviderHints(options.from);
		let providers =
			explicitFrom.length > 0
				? explicitFrom
				: await this.resolveRemoteProviders(cidString, {
						signal: options.signal,
					});
		// A resolver may observe abort and still complete normally. Do not create a
		// timeout-backed read from the candidates it returns after shutdown.
		throwIfReadWasAborted();
		// Provider resolution is asynchronous. An eager validation can complete and
		// cache the response during that await, so consume it before either returning
		// for lack of providers or installing a resolver that would otherwise wait.
		cachedResult = consumeCachedValue();
		if (cachedResult) return cachedResult;
		const canResolveLater = typeof this.options.resolveProviders === "function";
		if (providers.length === 0 && !canResolveLater) {
			// Without an explicit provider set (or a resolver), we intentionally do not
			// fall back to network-wide flooding. Scalable deployments must provide a
			// discovery mechanism (program-level hints, DHT/tracker, etc).
			return undefined;
		}
		if (explicitFrom.length > 0 && providers.length > 0) {
			this.rememberProviderHints(cidString, providers);
		}

		let inFlight = this._readFromPeersPromises.get(cidString);
		if (!inFlight) {
			let requestPriority = options.priority ?? BACKGROUND_MESSAGE_PRIORITY;
			let publishAdditionalProviders: (providers: string[]) => void = () => {};
			const promise = new Promise<Block<any, any, any, 1> | undefined>(
				(resolve, reject) => {
					let timeoutCallback: ReturnType<typeof setTimeout> | undefined;
					let resolver:
						| ((bytes: Uint8Array, providerHash?: string) => Promise<void>)
						| undefined;
					let settled = false;
					const abortHandler = () => {
						cleanup();
						if (settled) return;
						settled = true;
						reject(new AbortError());
					};

					const cleanup = () => {
						if (timeoutCallback) clearTimeout(timeoutCallback);
						if (resolver && this._resolvers.get(cidString) === resolver) {
							this._resolvers.delete(cidString);
						}
						closeSignal.removeEventListener("abort", abortHandler);
						options?.signal?.removeEventListener("abort", abortHandler);
					};

					// Register first, then re-check. This closes the check/listener window and
					// makes repeated stop() calls harmless through the settled guard.
					closeSignal.addEventListener("abort", abortHandler, { once: true });
					options?.signal?.addEventListener("abort", abortHandler, {
						once: true,
					});
					if (readWasAborted()) {
						abortHandler();
						return;
					}

					timeoutCallback = setTimeout(
						() => {
							cleanup();
							if (settled) return;
							settled = true;
							resolve(undefined);
						},
						options.timeout || 30 * 1000,
					);

					resolver = async (bytes: Uint8Array, providerHash?: string) => {
						const value = await tryVerify(bytes);
						if (settled) return;
						if (providerHash) {
							// A response is only evidence that its sender can provide this CID
							// after the payload has passed the requested CID's integrity check.
							this.rememberProvider(cidString, providerHash);
						}
						settled = true;
						cleanup();
						resolve(value);
					};
					this._resolvers.set(cidString, resolver);
				},
			);

			let requeryCount = 0;
			const maxRequests = Math.max(1, this.maxRequeryOnReachable);
			const requestRetryIntervalMs = Math.max(
				1_000,
				Math.min(
					5_000,
					Math.floor((options.timeout ?? 30_000) / Math.max(2, maxRequests)),
				),
			);
			const providerDiscoveryRetryIntervalMs = Math.max(
				250,
				Math.min(1_000, Math.floor(requestRetryIntervalMs / 2)),
			);
			let retryTimeout: ReturnType<typeof setTimeout> | undefined;
			let stopWatchingProviders: void | (() => void) | { close: () => void };
			const refreshProviders = async (force = false) => {
				if (!canResolveLater) return;
				if (!force && explicitFrom.length > 0) return;
				const resolved = await this.resolveRemoteProviders(cidString, {
					signal: options.signal,
					refresh: force,
				});
				if (resolved.length > 0) {
					providers = this.normalizeProviderHints([...providers, ...resolved]);
				}
			};
			const tryPublishRequest = async (properties?: {
				refreshProviders?: boolean;
				force?: boolean;
				providers?: string[];
			}) => {
				if (requeryCount >= maxRequests && !properties?.force) return;
				if (providers.length === 0 || properties?.refreshProviders) {
					await refreshProviders(properties?.refreshProviders === true);
				}
				if (providers.length === 0) return;
				try {
					const expiresAt = Date.now() + (options.timeout ?? 30_000);
					const requestProviders =
						properties?.providers && properties.providers.length > 0
							? this.normalizeProviderHints(properties.providers)
							: this.pickRequestBatch(providers, requeryCount);
					if (requestProviders.length === 0) return;
					await this.options.publish(new BlockRequest(cidString), {
						priority: requestPriority,
						responsePriority: requestPriority,
						expiresAt,
						mode: new SilentDelivery({
							to: requestProviders,
							redundancy: requestProviders.length,
						}),
					});
					requeryCount += 1;
				} catch (e) {
					dontThrowIfDeliveryError(e);
				}
			};
			publishAdditionalProviders = (nextProviders: string[]) => {
				if (!this._resolvers.has(cidString)) return;
				const requestProviders = this.normalizeProviderHints(nextProviders);
				if (requestProviders.length === 0) return;
				const merged = this.normalizeProviderHints([
					...requestProviders,
					...providers,
				]);
				if (merged.length === 0) return;
				let changed = merged.length !== providers.length;
				if (!changed) {
					for (let i = 0; i < merged.length; i++) {
						if (merged[i] !== providers[i]) {
							changed = true;
							break;
						}
					}
				}
				if (!changed) return;
				providers = merged;
				tryPublishRequest({ force: true, providers: requestProviders }).catch(
					dontThrowIfDeliveryError,
				);
			};
			inFlight = {
				promise,
				addProviders: (nextProviders) =>
					publishAdditionalProviders(nextProviders),
				promotePriority: (nextPriority) => {
					if (!this._resolvers.has(cidString)) return;
					// A background replication read may win CID coalescing before a
					// foreground waiter arrives. Reissue only on a strict priority upgrade.
					const promotedPriority = nextPriority ?? BACKGROUND_MESSAGE_PRIORITY;
					if (
						!Number.isFinite(promotedPriority) ||
						promotedPriority <= requestPriority
					) {
						return;
					}
					requestPriority = promotedPriority;
					tryPublishRequest({ force: true }).catch(dontThrowIfDeliveryError);
				},
			};
			this._readFromPeersPromises.set(cidString, inFlight);

			const scheduleRetry = () => {
				if (retryTimeout) {
					clearTimeout(retryTimeout);
				}
				if (requeryCount >= maxRequests) return;
				const retryIntervalMs =
					providers.length > 0
						? requestRetryIntervalMs
						: providerDiscoveryRetryIntervalMs;
				retryTimeout = setTimeout(() => {
					if (!this._resolvers.has(cidString)) return;
					tryPublishRequest({ refreshProviders: true })
						.catch(dontThrowIfDeliveryError)
						.finally(scheduleRetry);
				}, retryIntervalMs);
			};

			const publishOnNewPeers = () => {
				// Re-issue when reachability changes to handle "get before connect".
				// Bounded to avoid accidental amplification at large scale.
				if (requeryCount >= maxRequests) return;
				tryPublishRequest({ refreshProviders: true }).catch(
					dontThrowIfDeliveryError,
				);
			};

			const publishOnProviderHints = (ev: CustomEvent<{ cid: string }>) => {
				if (!ev?.detail?.cid) return;
				if (ev.detail.cid !== cidString) return;
				tryPublishRequest({ refreshProviders: true, force: true }).catch(
					dontThrowIfDeliveryError,
				);
			};

			if (
				canResolveLater &&
				explicitFrom.length === 0 &&
				this.options.watchProviders
			) {
				stopWatchingProviders = this.options.watchProviders(cidString, {
					signal: options.signal,
					onProviders: (nextProviders) => {
						if (!this._resolvers.has(cidString)) return;
						const normalized = this.normalizeProviderHints(nextProviders);
						if (normalized.length === 0) return;
						this.rememberProviderHints(cidString, normalized);
						providers = this.normalizeProviderHints([
							...providers,
							...normalized,
						]);
						tryPublishRequest({ force: true, providers: normalized }).catch(
							dontThrowIfDeliveryError,
						);
					},
				});
			}

			this._events.addEventListener("peer:reachable", publishOnNewPeers);
			this._events.addEventListener("providers:hints", publishOnProviderHints);
			try {
				await tryPublishRequest();
				scheduleRetry();
				const result = await promise;
				return result?.bytes;
			} finally {
				if (retryTimeout) {
					clearTimeout(retryTimeout);
				}
				this._readFromPeersPromises.delete(cidString);
				this._events.removeEventListener("peer:reachable", publishOnNewPeers);
				this._events.removeEventListener(
					"providers:hints",
					publishOnProviderHints,
				);
				if (typeof stopWatchingProviders === "function") {
					stopWatchingProviders();
				} else if (stopWatchingProviders) {
					stopWatchingProviders?.close();
				}
			}
		} else {
			inFlight.promotePriority(options.priority);
			if (providers.length > 0) {
				inFlight.addProviders(providers);
			}
			return this.waitForInFlightRead(inFlight, options);
		}
	}

	private waitForInFlightRead(
		inFlight: InFlightRead,
		options: RemoteReadOptions,
	): Promise<Uint8Array | undefined> {
		if (options.timeout == null && options.signal == null) {
			return inFlight.promise.then((result) => result?.bytes);
		}

		return new Promise((resolve, reject) => {
			let settled = false;
			let timeout: ReturnType<typeof setTimeout> | undefined;

			const cleanup = () => {
				if (timeout) clearTimeout(timeout);
				options.signal?.removeEventListener("abort", abort);
			};
			const finish = (callback: () => void) => {
				if (settled) return;
				settled = true;
				cleanup();
				callback();
			};
			const abort = () => {
				finish(() => reject(new AbortError()));
			};

			if (options.signal?.aborted) {
				abort();
				return;
			}

			if (options.timeout != null) {
				timeout = setTimeout(
					() => {
						finish(() => resolve(undefined));
					},
					Math.max(0, options.timeout),
				);
			}
			options.signal?.addEventListener("abort", abort, { once: true });

			inFlight.promise.then(
				(result) => finish(() => resolve(result?.bytes)),
				(error) => finish(() => reject(error)),
			);
		});
	}

	async stop(): Promise<void> {
		let firstError: unknown;
		const capture = async (operation: () => Promise<unknown> | unknown) => {
			try {
				await operation();
			} catch (error) {
				firstError ??= error;
			}
		};

		// Stop accepting work first, then always release every independent
		// queue/store/cache resource even when an earlier drain fails.
		await capture(() => this.closeController.abort());
		if (this._deferredStoredNotificationTimer) {
			clearTimeout(this._deferredStoredNotificationTimer);
			this._deferredStoredNotificationTimer = undefined;
		}
		await capture(() => this.flushDeferredStoredNotifications());
		// Queued wrappers observe the aborted generation and drain without starting
		// new work. Avoid PQueue.clear(): it would strand promises already returned
		// by the nested background-admission queue.
		await capture(() => this._backgroundLoadFetchQueue.onIdle());
		await capture(() => this._loadFetchQueue.onIdle());
		await capture(() => this._eagerValidationQueue?.onIdle());
		await capture(() => this.localStore?.stop());
		this._readFromPeersPromises.clear();
		this._resolvers.clear();
		this._blockCache?.clear();
		this._providerCache?.clear();
		this._rustProviderCache?.clear();
		this._open = false;
		// we dont cleanup subscription because we dont know if someone else is sbuscribing also
		if (firstError !== undefined) {
			throw firstError;
		}
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
