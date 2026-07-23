import { field, variant, vec } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import {
	Compare,
	type Index,
	IntegerCompare,
	Or,
} from "@peerbit/indexer-interface";
import { Entry, Log } from "@peerbit/log";
import type { RPC, RequestContext } from "@peerbit/rpc";
import {
	CONVERGENCE_MESSAGE_PRIORITY,
	SilentDelivery,
} from "@peerbit/stream-interface";
import {
	EntryWithRefs,
	createExchangeHeadsMessages,
	createRawExchangeHeadsMessages,
} from "../exchange-heads.js";
import { TransportMessage } from "../message.js";
import type { EntryReplicated } from "../ranges.js";
import type {
	HashSymbolResolver,
	HashSymbolHashListResolver,
	RawExchangeHeadsSender,
	RepairSession,
	RepairSessionMode,
	RepairSessionResult,
	SyncEntryCoordinates,
	SyncOptions,
	SyncableKey,
	Syncronizer,
} from "./index.js";
import { emitSyncProfileDuration, syncProfileStart } from "./profile.js";

@variant([0, 1])
export class RequestMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 2])
export class ResponseMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 5])
export class RequestMaybeSyncCoordinate extends TransportMessage {
	@field({ type: vec("u64") })
	hashNumbers: bigint[];

	constructor(props: { hashNumbers: bigint[] }) {
		super();
		this.hashNumbers = props.hashNumbers;
	}
}

@variant([0, 6])
export class ConfirmEntriesMessage extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

export const SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY = 1;

@variant([0, 8])
export class ResponseMaybeSyncCapabilities extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	@field({ type: "u32" })
	capabilities: number;

	constructor(props: { hashes: string[]; capabilities?: number }) {
		super();
		this.hashes = props.hashes;
		this.capabilities =
			props.capabilities ?? SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY;
	}
}

@variant([0, 9])
export class RequestMaybeSyncCoordinateCapabilities extends TransportMessage {
	@field({ type: vec("u64") })
	hashNumbers: bigint[];

	@field({ type: "u32" })
	capabilities: number;

	constructor(props: { hashNumbers: bigint[]; capabilities?: number }) {
		super();
		this.hashNumbers = props.hashNumbers;
		this.capabilities =
			props.capabilities ?? SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY;
	}
}

const canReceiveRawExchangeHeads = (
	message:
		| ResponseMaybeSync
		| ResponseMaybeSyncCapabilities
		| RequestMaybeSyncCoordinate
		| RequestMaybeSyncCoordinateCapabilities,
) =>
	(message instanceof ResponseMaybeSyncCapabilities ||
		message instanceof RequestMaybeSyncCoordinateCapabilities) &&
	(message.capabilities & SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY) !== 0;

type KnownSyncKeys = {
	keys: Set<SyncableKey>;
	checkedCoordinates: boolean;
	checkedHashes: boolean;
};

const getHashesFromSymbols = async (
	symbols: bigint[],
	entryIndex: Index<EntryReplicated<any>, any>,
	coordinateToHash: Cache<string>,
	resolveHashesForSymbols?: HashSymbolResolver,
	resolveHashListForSymbols?: HashSymbolHashListResolver,
): Promise<Set<string> | string[]> => {
	let queries: IntegerCompare[] = [];
	let batchSize = 128; // TODO arg
	let results = new Set<string>();
	let missingSymbols: bigint[] = [];
	const addMissingUnlessCached = (symbol: bigint) => {
		const fromCache = coordinateToHash.get(symbol);
		if (fromCache) {
			results.add(fromCache);
			return;
		}
		missingSymbols.push(symbol);
	};
	const handleBatch = async (end = false) => {
		if (queries.length >= batchSize || (end && queries.length > 0)) {
			const entries = await entryIndex
				.iterate(
					{ query: queries.length > 1 ? new Or(queries) : queries },
					{ shape: { hash: true, hashNumber: true } },
				)
				.all();
			queries = [];

			for (const entry of entries) {
				results.add(entry.value.hash);
				coordinateToHash.add(entry.value.hashNumber, entry.value.hash);
			}
		}
	};

	if (resolveHashListForSymbols) {
		const resolvedHashes = await resolveHashListForSymbols(symbols);
		if (resolvedHashes) {
			const resolvedHashList = Array.isArray(resolvedHashes)
				? resolvedHashes
				: [...resolvedHashes];
			let mergedHashes: Set<string> | undefined;
			for (const symbol of symbols) {
				const fromCache = coordinateToHash.get(symbol);
				if (fromCache) {
					mergedHashes ??= new Set(resolvedHashList);
					mergedHashes.add(fromCache);
				}
			}
			return mergedHashes ?? resolvedHashList;
		}
	}

	if (resolveHashesForSymbols) {
		const resolved = await resolveHashesForSymbols(symbols);
		if (resolved) {
			for (const symbol of symbols) {
				const hashes = resolved.get(symbol);
				if (!hashes) {
					addMissingUnlessCached(symbol);
					continue;
				}
				let singleHash: string | undefined;
				let count = 0;
				for (const hash of hashes) {
					results.add(hash);
					singleHash = hash;
					count += 1;
				}
				if (count === 0) {
					addMissingUnlessCached(symbol);
				} else if (count === 1) {
					coordinateToHash.add(symbol, singleHash!);
				}
			}
		} else {
			for (const symbol of symbols) {
				addMissingUnlessCached(symbol);
			}
		}
	} else {
		for (const symbol of symbols) {
			addMissingUnlessCached(symbol);
		}
	}

	for (const symbol of missingSymbols) {
		const matchQuery = new IntegerCompare({
			key: "hashNumber",
			compare: Compare.Equal,
			value: symbol,
		});

		queries.push(matchQuery);
		await handleBatch();
	}
	await handleBatch(true);

	return results;
};

const hashLookupResultSize = (hashes: Set<string> | string[]) =>
	Array.isArray(hashes) ? hashes.length : hashes.size;

const DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS = 30_000;
const DEFAULT_CONVERGENT_RETRY_INTERVALS_MS = [0, 1_000, 3_000, 7_000];
const DEFAULT_BEST_EFFORT_RETRY_INTERVALS_MS = [0];
const SESSION_POLL_INTERVAL_MS = 100;
const DEFAULT_MAX_HASHES_PER_MESSAGE = 1_024;
const DEFAULT_MAX_COORDINATES_PER_MESSAGE = 1_024;
const DEFAULT_MAX_CONVERGENT_TRACKED_HASHES = 4_096;
// Keep convergence sync above the default/background lane. Dropping it to the
// background priority lets repair traffic starve behind foreground work.
export const SYNC_MESSAGE_PRIORITY = CONVERGENCE_MESSAGE_PRIORITY;
// Retry missing entry requests when the first response was lost (for example, due to
// pubsub stream warmup). Keep it coarse-grained so we do not hammer the network under
// large historical backfills.
const SIMPLE_SYNC_RETRY_AFTER_MS = 10_000;
const EXCHANGE_HEAD_RESPONSE_DEDUPE_TTL_MS = SIMPLE_SYNC_RETRY_AFTER_MS - 1_000;
const RECENT_KNOWN_EXCHANGE_HEAD_SUPPRESSION_MS = 30_000;
const PENDING_MAYBE_SYNC_RESPONSE_TTL_MS = 30_000;
// Bound retained request/response associations globally. Ten thousand hashes
// covers several full default-size request batches while keeping adversarial or
// abandoned requests to a small, predictable amount of heap.
const MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES = 10_000;

type PendingMaybeSyncResponse = {
	hashes: Set<string>;
	target: string;
	targetLifecycle: SyncDispatchTargetLifecycle;
	expiresAt: number;
};

type PendingMaybeSyncResponseAuthorization = {
	batch: PendingMaybeSyncResponse;
	hash: string;
};

type PendingMaybeSyncResponseReservation = {
	release: () => void;
	newlyAuthorizedByTarget: Map<string, string[]>;
	retained: () => boolean;
	signal: AbortSignal;
};

export type AuthorizedMaybeSyncResponseLease = {
	hashes: string[];
	signal: AbortSignal;
	release: () => void;
};

type SyncDispatchLifecycle = {
	ownershipLifecycleController: AbortController;
	callerSignal?: AbortSignal;
	controller: AbortController;
	targets: Map<string, SyncDispatchTargetLifecycle>;
	onOwnerOrCallerAbort: () => void;
	dispatchFinished: boolean;
	disposed: boolean;
	abortAllOnTargetDisconnect: boolean;
};

type SyncDispatchTargetEpoch = {
	id: number;
};

type SyncDispatchTargetLifecycle = {
	lifecycle: SyncDispatchLifecycle;
	target: string;
	epoch: SyncDispatchTargetEpoch;
	controller: AbortController;
	batches: Set<PendingMaybeSyncResponse>;
	responseLeases: number;
	activeWaiters: number;
};

const createDeferred = <T>() => {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
};

type RepairSessionTargetState = {
	unresolved: Set<string>;
	requestedCount: number;
	requestedTotalCount: number;
	attempts: number;
	targetEpoch: SyncDispatchTargetEpoch;
};

type RepairSessionState = {
	id: string;
	mode: RepairSessionMode;
	startedAt: number;
	timeoutMs: number;
	retryIntervalsMs: number[];
	targets: Map<string, RepairSessionTargetState>;
	truncated: boolean;
	deferred: ReturnType<typeof createDeferred<RepairSessionResult[]>>;
	cancelled: boolean;
	timer?: ReturnType<typeof setTimeout>;
};

export class SimpleSyncronizer<R extends "u32" | "u64">
	implements Syncronizer<R>
{
	// map of hash to public keys that we can ask for entries
	syncInFlightQueue: Map<SyncableKey, PublicSignKey[]>;
	syncInFlightQueueInverted: Map<string, Set<SyncableKey>>;

	// map of hash to public keys that we have asked for entries
	syncInFlight!: Map<string, Map<SyncableKey, { timestamp: number }>>;

	rpc: RPC<TransportMessage, TransportMessage>;
	log: Log<any>;
	entryIndex: Index<EntryReplicated<R>, any>;
	coordinateToHash: Cache<string>;
	private resolveHashesForSymbols?: HashSymbolResolver;
	private resolveHashListForSymbols?: HashSymbolHashListResolver;
	private syncOptions?: SyncOptions<R>;
	private isEntryRecentlyKnownByPeer?: (
		hash: string,
		peer: string,
		maxAgeMs: number,
	) => boolean;
	private sendRawExchangeHeads?: RawExchangeHeadsSender;
	private recentlySentExchangeHeads: Map<string, Map<string, number>>;
	private pendingMaybeSyncResponses: Map<
		string,
		Map<string, PendingMaybeSyncResponseAuthorization>
	>;
	private pendingMaybeSyncResponseCount: number;
	private pendingMaybeSyncResponseWaiters: Set<() => void>;
	private pendingMaybeSyncResponseBatches: Set<PendingMaybeSyncResponse>;
	private pendingMaybeSyncResponseExpiryTimer?: ReturnType<typeof setTimeout>;
	private syncDispatchLifecycleController: AbortController;
	private syncDispatchTargetEpochCounter: number;
	private syncDispatchTargetEpochs: Map<string, SyncDispatchTargetEpoch>;
	private syncDispatchTargets: Map<string, Set<SyncDispatchTargetLifecycle>>;
	private repairSessionCounter: number;
	private repairSessions: Map<string, RepairSessionState>;

	// Syncing and dedeplucation work
	syncMoreInterval?: ReturnType<typeof setTimeout>;

	closed!: boolean;

	constructor(properties: {
		rpc: RPC<TransportMessage, TransportMessage>;
		entryIndex: Index<EntryReplicated<R>, any>;
		log: Log<any>;
		coordinateToHash: Cache<string>;
		resolveHashesForSymbols?: HashSymbolResolver;
		resolveHashListForSymbols?: HashSymbolHashListResolver;
		sync?: SyncOptions<R>;
		isEntryRecentlyKnownByPeer?: (
			hash: string,
			peer: string,
			maxAgeMs: number,
		) => boolean;
		sendRawExchangeHeads?: RawExchangeHeadsSender;
	}) {
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlight = new Map();
		this.rpc = properties.rpc;
		this.log = properties.log;
		this.entryIndex = properties.entryIndex;
		this.coordinateToHash = properties.coordinateToHash;
		this.resolveHashesForSymbols = properties.resolveHashesForSymbols;
		this.resolveHashListForSymbols = properties.resolveHashListForSymbols;
		this.syncOptions = properties.sync;
		this.isEntryRecentlyKnownByPeer = properties.isEntryRecentlyKnownByPeer;
		this.sendRawExchangeHeads = properties.sendRawExchangeHeads;
		this.recentlySentExchangeHeads = new Map();
		this.pendingMaybeSyncResponses = new Map();
		this.pendingMaybeSyncResponseCount = 0;
		this.pendingMaybeSyncResponseWaiters = new Set();
		this.pendingMaybeSyncResponseBatches = new Set();
		this.syncDispatchLifecycleController = new AbortController();
		this.syncDispatchTargetEpochCounter = 0;
		this.syncDispatchTargetEpochs = new Map();
		this.syncDispatchTargets = new Map();
		this.repairSessionCounter = 0;
		this.repairSessions = new Map();
	}

	private getPrioritizedHashes(
		entries: Map<string, SyncEntryCoordinates<R>>,
	): string[] {
		const priorityFn = this.syncOptions?.priority;
		if (!priorityFn) {
			return [...entries.keys()];
		}

		let index = 0;
		const scored: { hash: string; index: number; priority: number }[] = [];
		for (const [hash, entry] of entries) {
			const priorityValue = priorityFn(entry as EntryReplicated<R>);
			scored.push({
				hash,
				index,
				priority: Number.isFinite(priorityValue) ? priorityValue : 0,
			});
			index += 1;
		}
		scored.sort((a, b) => b.priority - a.priority || a.index - b.index);
		return scored.map((x) => x.hash);
	}

	private normalizeRetryIntervals(
		mode: RepairSessionMode,
		retryIntervalsMs?: number[],
	): number[] {
		const defaults =
			mode === "convergent"
				? DEFAULT_CONVERGENT_RETRY_INTERVALS_MS
				: DEFAULT_BEST_EFFORT_RETRY_INTERVALS_MS;
		if (!retryIntervalsMs || retryIntervalsMs.length === 0) {
			return [...defaults];
		}

		return [...retryIntervalsMs]
			.map((x) => Math.max(0, Math.floor(x)))
			.filter((x, i, arr) => arr.indexOf(x) === i)
			.sort((a, b) => a - b);
	}

	private get maxHashesPerMessage() {
		const value = this.syncOptions?.maxSimpleHashesPerMessage;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
			: DEFAULT_MAX_HASHES_PER_MESSAGE;
	}

	private get maxCoordinatesPerMessage() {
		const value = this.syncOptions?.maxSimpleCoordinatesPerMessage;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
			: DEFAULT_MAX_COORDINATES_PER_MESSAGE;
	}

	private get maxConvergentTrackedHashes() {
		const value = this.syncOptions?.maxConvergentTrackedHashes;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
			: DEFAULT_MAX_CONVERGENT_TRACKED_HASHES;
	}

	private chunk<T>(values: T[], size: number): T[][] {
		if (values.length === 0) {
			return [];
		}
		const out: T[][] = [];
		for (let i = 0; i < values.length; i += size) {
			out.push(values.slice(i, i + size));
		}
		return out;
	}

	private filterRecentlySentExchangeHeads(
		hashes: Iterable<string>,
		peer: PublicSignKey,
	): string[] {
		const peerHash = peer.hashcode();
		const now = Date.now();
		let recentlySent = this.recentlySentExchangeHeads.get(peerHash);
		if (!recentlySent) {
			recentlySent = new Map();
			this.recentlySentExchangeHeads.set(peerHash, recentlySent);
		}
		for (const [hash, timestamp] of recentlySent) {
			if (now - timestamp > EXCHANGE_HEAD_RESPONSE_DEDUPE_TTL_MS) {
				recentlySent.delete(hash);
			}
		}
		const out: string[] = [];
		const seen = new Set<string>();
		for (const hash of hashes) {
			if (seen.has(hash)) {
				continue;
			}
			seen.add(hash);
			if (recentlySent.has(hash)) {
				continue;
			}
			if (
				this.isEntryRecentlyKnownByPeer?.(
					hash,
					peerHash,
					RECENT_KNOWN_EXCHANGE_HEAD_SUPPRESSION_MS,
				)
			) {
				continue;
			}
			recentlySent.set(hash, now);
			out.push(hash);
		}
		return out;
	}

	private forgetRecentlySentExchangeHeads(
		hashes: Iterable<string>,
		peer: PublicSignKey,
	): void {
		const recentlySent = this.recentlySentExchangeHeads.get(peer.hashcode());
		if (!recentlySent) {
			return;
		}
		for (const hash of hashes) {
			recentlySent.delete(hash);
		}
		if (recentlySent.size === 0) {
			this.recentlySentExchangeHeads.delete(peer.hashcode());
		}
	}

	private getOrCreateSyncDispatchTargetEpoch(
		target: string,
	): SyncDispatchTargetEpoch {
		let epoch = this.syncDispatchTargetEpochs.get(target);
		if (!epoch) {
			epoch = { id: ++this.syncDispatchTargetEpochCounter };
			this.syncDispatchTargetEpochs.set(target, epoch);
		}
		return epoch;
	}

	private captureSyncDispatchLifecycle(
		targets: string[],
		callerSignal?: AbortSignal,
		options?: {
			abortAllOnTargetDisconnect?: boolean;
			ownershipLifecycleController?: AbortController;
			targetEpochs?: Map<string, SyncDispatchTargetEpoch>;
			createTargetEpochs?: boolean;
		},
	): SyncDispatchLifecycle {
		const ownershipLifecycleController =
			options?.ownershipLifecycleController ??
			this.syncDispatchLifecycleController;
		const lifecycle = {
			ownershipLifecycleController,
			callerSignal,
			controller: new AbortController(),
			targets: new Map<string, SyncDispatchTargetLifecycle>(),
			onOwnerOrCallerAbort: () => {
				const reason =
					callerSignal?.aborted === true
						? callerSignal.reason
						: ownershipLifecycleController.signal.reason;
				this.abortSyncDispatchLifecycle(lifecycle, reason);
			},
			dispatchFinished: false,
			disposed: false,
			abortAllOnTargetDisconnect: options?.abortAllOnTargetDisconnect === true,
		} satisfies SyncDispatchLifecycle;

		for (const target of [...new Set(targets)]) {
			const expectedEpoch = options?.targetEpochs?.get(target);
			const currentEpoch = this.syncDispatchTargetEpochs.get(target);
			const epoch =
				expectedEpoch ??
				currentEpoch ??
				(options?.createTargetEpochs === false
					? undefined
					: this.getOrCreateSyncDispatchTargetEpoch(target));
			if (!epoch) {
				continue;
			}
			const targetLifecycle: SyncDispatchTargetLifecycle = {
				lifecycle,
				target,
				epoch,
				controller: new AbortController(),
				batches: new Set(),
				responseLeases: 0,
				activeWaiters: 0,
			};
			lifecycle.targets.set(target, targetLifecycle);
			let activeForTarget = this.syncDispatchTargets.get(target);
			if (!activeForTarget) {
				activeForTarget = new Set();
				this.syncDispatchTargets.set(target, activeForTarget);
			}
			activeForTarget.add(targetLifecycle);
			if (this.syncDispatchTargetEpochs.get(target) !== epoch) {
				this.abortSyncDispatchTarget(
					targetLifecycle,
					new Error("sync target lifecycle is stale"),
				);
			}
		}

		ownershipLifecycleController.signal.addEventListener(
			"abort",
			lifecycle.onOwnerOrCallerAbort,
			{ once: true },
		);
		if (callerSignal && callerSignal !== ownershipLifecycleController.signal) {
			callerSignal.addEventListener("abort", lifecycle.onOwnerOrCallerAbort, {
				once: true,
			});
		}
		if (
			this.closed === true ||
			ownershipLifecycleController !== this.syncDispatchLifecycleController ||
			ownershipLifecycleController.signal.aborted ||
			callerSignal?.aborted
		) {
			lifecycle.onOwnerOrCallerAbort();
		}
		return lifecycle;
	}

	private abortSyncDispatchTarget(
		targetLifecycle: SyncDispatchTargetLifecycle,
		reason?: unknown,
	): void {
		if (!targetLifecycle.controller.signal.aborted) {
			targetLifecycle.controller.abort(reason);
		}
		for (const batch of [...targetLifecycle.batches]) {
			this.removePendingMaybeSyncResponseBatch(batch);
		}
		this.notifyPendingMaybeSyncResponseWaiters();
		this.maybeDisposeSyncDispatchLifecycle(targetLifecycle.lifecycle);
	}

	private abortSyncDispatchLifecycle(
		lifecycle: SyncDispatchLifecycle,
		reason?: unknown,
	): void {
		if (!lifecycle.controller.signal.aborted) {
			lifecycle.controller.abort(reason);
		}
		for (const targetLifecycle of lifecycle.targets.values()) {
			this.abortSyncDispatchTarget(targetLifecycle, reason);
		}
		this.notifyPendingMaybeSyncResponseWaiters();
		this.maybeDisposeSyncDispatchLifecycle(lifecycle);
	}

	private finishSyncDispatchLifecycle(lifecycle: SyncDispatchLifecycle): void {
		lifecycle.dispatchFinished = true;
		this.maybeDisposeSyncDispatchLifecycle(lifecycle);
	}

	private maybeDisposeSyncDispatchLifecycle(
		lifecycle: SyncDispatchLifecycle,
	): void {
		if (
			lifecycle.disposed ||
			!lifecycle.dispatchFinished ||
			[...lifecycle.targets.values()].some(
				(target) =>
					target.batches.size > 0 ||
					target.responseLeases > 0 ||
					target.activeWaiters > 0,
			)
		) {
			return;
		}
		lifecycle.disposed = true;
		lifecycle.ownershipLifecycleController.signal.removeEventListener(
			"abort",
			lifecycle.onOwnerOrCallerAbort,
		);
		if (
			lifecycle.callerSignal &&
			lifecycle.callerSignal !== lifecycle.ownershipLifecycleController.signal
		) {
			lifecycle.callerSignal.removeEventListener(
				"abort",
				lifecycle.onOwnerOrCallerAbort,
			);
		}
		for (const targetLifecycle of lifecycle.targets.values()) {
			const activeForTarget = this.syncDispatchTargets.get(
				targetLifecycle.target,
			);
			activeForTarget?.delete(targetLifecycle);
			if (activeForTarget?.size === 0) {
				this.syncDispatchTargets.delete(targetLifecycle.target);
			}
		}
	}

	private isSyncDispatchLifecycleActive(
		lifecycle: SyncDispatchLifecycle,
		target?: string,
	): boolean {
		if (
			this.closed === true ||
			lifecycle.disposed ||
			lifecycle.ownershipLifecycleController !==
				this.syncDispatchLifecycleController ||
			lifecycle.ownershipLifecycleController.signal.aborted ||
			lifecycle.callerSignal?.aborted ||
			lifecycle.controller.signal.aborted
		) {
			return false;
		}
		if (target === undefined) {
			return true;
		}
		const targetLifecycle = lifecycle.targets.get(target);
		return (
			targetLifecycle !== undefined &&
			!targetLifecycle.controller.signal.aborted &&
			this.syncDispatchTargetEpochs.get(target) === targetLifecycle.epoch
		);
	}

	private getSyncDispatchSignal(
		lifecycle: SyncDispatchLifecycle,
		target: string,
	): AbortSignal {
		return (
			lifecycle.targets.get(target)?.controller.signal ??
			lifecycle.controller.signal
		);
	}

	private notifyPendingMaybeSyncResponseWaiters(): void {
		const waiters = [...this.pendingMaybeSyncResponseWaiters];
		this.pendingMaybeSyncResponseWaiters.clear();
		for (const wake of waiters) {
			wake();
		}
	}

	private schedulePendingMaybeSyncResponseExpiry(): void {
		if (
			this.pendingMaybeSyncResponseExpiryTimer ||
			this.pendingMaybeSyncResponseBatches.size === 0
		) {
			return;
		}
		let earliest = Number.POSITIVE_INFINITY;
		for (const batch of this.pendingMaybeSyncResponseBatches) {
			earliest = Math.min(earliest, batch.expiresAt);
		}
		this.pendingMaybeSyncResponseExpiryTimer = setTimeout(
			() => {
				this.pendingMaybeSyncResponseExpiryTimer = undefined;
				const now = Date.now();
				for (const batch of [...this.pendingMaybeSyncResponseBatches]) {
					if (batch.expiresAt <= now) {
						this.removePendingMaybeSyncResponseBatch(batch);
					}
				}
				this.schedulePendingMaybeSyncResponseExpiry();
			},
			Math.max(0, earliest - Date.now()),
		);
		this.pendingMaybeSyncResponseExpiryTimer.unref?.();
	}

	private removePendingMaybeSyncResponseBatch(
		batch: PendingMaybeSyncResponse,
	): void {
		const pendingForTarget = this.pendingMaybeSyncResponses.get(batch.target);
		let removed = 0;
		if (pendingForTarget) {
			for (const hash of batch.hashes) {
				if (pendingForTarget.get(hash)?.batch !== batch) {
					continue;
				}
				pendingForTarget.delete(hash);
				removed += 1;
			}
			if (pendingForTarget.size === 0) {
				this.pendingMaybeSyncResponses.delete(batch.target);
			}
		}
		this.pendingMaybeSyncResponseCount -= removed;
		this.pendingMaybeSyncResponseBatches.delete(batch);
		batch.targetLifecycle.batches.delete(batch);
		batch.hashes.clear();
		if (
			this.pendingMaybeSyncResponseBatches.size === 0 &&
			this.pendingMaybeSyncResponseExpiryTimer
		) {
			clearTimeout(this.pendingMaybeSyncResponseExpiryTimer);
			this.pendingMaybeSyncResponseExpiryTimer = undefined;
		}
		this.notifyPendingMaybeSyncResponseWaiters();
		this.maybeDisposeSyncDispatchLifecycle(batch.targetLifecycle.lifecycle);
	}

	private clearPendingMaybeSyncResponses(target?: string): void {
		const batches =
			target === undefined
				? [...this.pendingMaybeSyncResponseBatches]
				: [
						...new Set(
							[
								...(this.pendingMaybeSyncResponses.get(target)?.values() ?? []),
							].map((authorization) => authorization.batch),
						),
					];
		for (const batch of batches) {
			this.removePendingMaybeSyncResponseBatch(batch);
		}
		this.notifyPendingMaybeSyncResponseWaiters();
	}

	private tryReservePendingMaybeSyncResponse(properties: {
		hashes: Iterable<string>;
		targets: string[];
		lifecycle: SyncDispatchLifecycle;
	}): PendingMaybeSyncResponseReservation | undefined {
		const hashes = [...new Set(properties.hashes)];
		const targets = [...new Set(properties.targets)];
		if (
			!this.isSyncDispatchLifecycleActive(properties.lifecycle) ||
			targets.some(
				(target) =>
					!this.isSyncDispatchLifecycleActive(properties.lifecycle, target),
			)
		) {
			return undefined;
		}

		const hashesToAddByTarget = new Map<string, string[]>();
		let required = 0;
		for (const target of targets) {
			const targetLifecycle = properties.lifecycle.targets.get(target)!;
			const hashesToAdd: string[] = [];
			for (const hash of hashes) {
				let existing = this.pendingMaybeSyncResponses.get(target)?.get(hash);
				if (
					existing &&
					!this.isSyncDispatchLifecycleActive(
						existing.batch.targetLifecycle.lifecycle,
						target,
					)
				) {
					this.removePendingMaybeSyncResponseBatch(existing.batch);
					existing = undefined;
				}
				if (existing) {
					const existingTarget = existing.batch.targetLifecycle;
					if (
						existingTarget.epoch === targetLifecycle.epoch &&
						existingTarget.lifecycle.ownershipLifecycleController ===
							properties.lifecycle.ownershipLifecycleController &&
						existingTarget.lifecycle.callerSignal ===
							properties.lifecycle.callerSignal
					) {
						continue;
					}
					return undefined;
				}
				hashesToAdd.push(hash);
			}
			if (hashesToAdd.length > 0) {
				hashesToAddByTarget.set(target, hashesToAdd);
				required += hashesToAdd.length;
			}
		}

		if (
			required >
			MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES -
				this.pendingMaybeSyncResponseCount
		) {
			return undefined;
		}

		const addedBatches: PendingMaybeSyncResponse[] = [];
		for (const [target, hashesToAdd] of hashesToAddByTarget) {
			const targetLifecycle = properties.lifecycle.targets.get(target)!;
			const batch: PendingMaybeSyncResponse = {
				hashes: new Set(hashesToAdd),
				target,
				targetLifecycle,
				expiresAt: Date.now() + PENDING_MAYBE_SYNC_RESPONSE_TTL_MS,
			};
			let pendingForTarget = this.pendingMaybeSyncResponses.get(target);
			if (!pendingForTarget) {
				pendingForTarget = new Map();
				this.pendingMaybeSyncResponses.set(target, pendingForTarget);
			}
			for (const hash of hashesToAdd) {
				pendingForTarget.set(hash, { batch, hash });
			}
			this.pendingMaybeSyncResponseCount += hashesToAdd.length;
			this.pendingMaybeSyncResponseBatches.add(batch);
			targetLifecycle.batches.add(batch);
			addedBatches.push(batch);
		}
		this.schedulePendingMaybeSyncResponseExpiry();

		let released = false;
		const release = () => {
			if (released) {
				return;
			}
			released = true;
			for (const batch of addedBatches) {
				this.removePendingMaybeSyncResponseBatch(batch);
			}
		};
		const retained = () =>
			this.isSyncDispatchLifecycleActive(properties.lifecycle) &&
			addedBatches.every(
				(batch) =>
					this.pendingMaybeSyncResponseBatches.has(batch) &&
					batch.hashes.size > 0,
			);
		if (!this.isSyncDispatchLifecycleActive(properties.lifecycle)) {
			release();
			return undefined;
		}
		return {
			release,
			newlyAuthorizedByTarget: hashesToAddByTarget,
			retained,
			signal: properties.lifecycle.controller.signal,
		};
	}

	private waitForPendingMaybeSyncResponseChange(
		lifecycle: SyncDispatchLifecycle,
		targets: string[],
	): Promise<void> {
		if (
			!this.isSyncDispatchLifecycleActive(lifecycle) ||
			targets.some(
				(target) => !this.isSyncDispatchLifecycleActive(lifecycle, target),
			)
		) {
			return Promise.resolve();
		}
		const targetLifecycles = targets
			.map((target) => lifecycle.targets.get(target))
			.filter(
				(target): target is SyncDispatchTargetLifecycle => target !== undefined,
			);
		for (const targetLifecycle of targetLifecycles) {
			targetLifecycle.activeWaiters += 1;
		}
		return new Promise<void>((resolve) => {
			let settled = false;
			const wake = () => {
				if (settled) {
					return;
				}
				settled = true;
				this.pendingMaybeSyncResponseWaiters.delete(wake);
				for (const targetLifecycle of targetLifecycles) {
					targetLifecycle.activeWaiters -= 1;
				}
				resolve();
				this.maybeDisposeSyncDispatchLifecycle(lifecycle);
			};
			this.pendingMaybeSyncResponseWaiters.add(wake);
			if (
				!this.isSyncDispatchLifecycleActive(lifecycle) ||
				targets.some(
					(target) => !this.isSyncDispatchLifecycleActive(lifecycle, target),
				)
			) {
				wake();
			}
		});
	}

	private async reservePendingMaybeSyncResponse(properties: {
		hashes: Iterable<string>;
		targets: string[];
		lifecycle: SyncDispatchLifecycle;
	}): Promise<PendingMaybeSyncResponseReservation | undefined> {
		while (
			this.isSyncDispatchLifecycleActive(properties.lifecycle) &&
			properties.targets.every((target) =>
				this.isSyncDispatchLifecycleActive(properties.lifecycle, target),
			)
		) {
			const reservation = this.tryReservePendingMaybeSyncResponse(properties);
			if (reservation) {
				return reservation;
			}
			await this.waitForPendingMaybeSyncResponseChange(
				properties.lifecycle,
				properties.targets,
			);
		}
		return undefined;
	}

	expectMaybeSyncResponse(properties: {
		hashes: Iterable<string>;
		targets: string[];
		signal?: AbortSignal;
	}): PendingMaybeSyncResponseReservation | undefined {
		const targets = [...new Set(properties.targets)];
		const lifecycle = this.captureSyncDispatchLifecycle(
			targets,
			properties.signal,
			{ abortAllOnTargetDisconnect: true },
		);
		const reservation = this.tryReservePendingMaybeSyncResponse({
			hashes: properties.hashes,
			targets,
			lifecycle,
		});
		let retainedReservation: PendingMaybeSyncResponseReservation | undefined;
		if (reservation) {
			const leasedTargets = [...lifecycle.targets.values()];
			for (const targetLifecycle of leasedTargets) {
				targetLifecycle.responseLeases += 1;
			}
			let released = false;
			retainedReservation = {
				...reservation,
				release: () => {
					if (released) {
						return;
					}
					released = true;
					reservation.release();
					for (const targetLifecycle of leasedTargets) {
						targetLifecycle.responseLeases -= 1;
					}
					this.maybeDisposeSyncDispatchLifecycle(lifecycle);
				},
			};
		}
		this.finishSyncDispatchLifecycle(lifecycle);
		return retainedReservation;
	}

	consumeAuthorizedMaybeSyncResponse(
		hashes: Iterable<string>,
		from: PublicSignKey,
	): AuthorizedMaybeSyncResponseLease[] {
		const fromHash = from.hashcode();
		const pendingForTarget = this.pendingMaybeSyncResponses.get(fromHash);
		if (!pendingForTarget) {
			return [];
		}
		const acceptedByLifecycle = new Map<
			SyncDispatchTargetLifecycle,
			string[]
		>();
		const seen = new Set<string>();
		for (const hash of hashes) {
			if (seen.has(hash)) {
				continue;
			}
			seen.add(hash);
			const authorization = pendingForTarget.get(hash);
			if (!authorization) {
				continue;
			}
			const batch = authorization.batch;
			const targetLifecycle = batch.targetLifecycle;
			if (
				!this.isSyncDispatchLifecycleActive(targetLifecycle.lifecycle, fromHash)
			) {
				this.removePendingMaybeSyncResponseBatch(batch);
				continue;
			}
			if (pendingForTarget.get(hash)?.batch !== batch) {
				continue;
			}
			let accepted = acceptedByLifecycle.get(targetLifecycle);
			if (!accepted) {
				accepted = [];
				acceptedByLifecycle.set(targetLifecycle, accepted);
				targetLifecycle.responseLeases += 1;
			}
			pendingForTarget.delete(hash);
			batch.hashes.delete(hash);
			this.pendingMaybeSyncResponseCount -= 1;
			accepted.push(hash);
			if (batch.hashes.size === 0) {
				this.removePendingMaybeSyncResponseBatch(batch);
			}
		}
		if (pendingForTarget.size === 0) {
			this.pendingMaybeSyncResponses.delete(fromHash);
		}
		this.notifyPendingMaybeSyncResponseWaiters();
		return [...acceptedByLifecycle].map(([targetLifecycle, acceptedHashes]) => {
			let released = false;
			return {
				hashes: acceptedHashes,
				signal: targetLifecycle.controller.signal,
				release: () => {
					if (released) {
						return;
					}
					released = true;
					targetLifecycle.responseLeases -= 1;
					this.maybeDisposeSyncDispatchLifecycle(targetLifecycle.lifecycle);
				},
			};
		});
	}

	private isRepairSessionComplete(session: RepairSessionState): boolean {
		for (const state of session.targets.values()) {
			if (state.unresolved.size > 0) {
				return false;
			}
		}
		return true;
	}

	private buildRepairSessionResult(
		session: RepairSessionState,
		completed: boolean,
	): RepairSessionResult[] {
		const durationMs = Date.now() - session.startedAt;
		const out: RepairSessionResult[] = [];
		for (const [target, state] of session.targets) {
			const unresolved = [...state.unresolved];
			out.push({
				target,
				requested: state.requestedCount,
				resolved: state.requestedCount - unresolved.length,
				unresolved,
				attempts: state.attempts,
				durationMs,
				completed,
				requestedTotal: state.requestedTotalCount,
				truncated: session.truncated,
			});
		}
		return out;
	}

	private finalizeRepairSession(sessionId: string, completed: boolean): void {
		const session = this.repairSessions.get(sessionId);
		if (!session) {
			return;
		}
		this.repairSessions.delete(sessionId);
		session.cancelled = true;
		if (session.timer) {
			clearTimeout(session.timer);
		}
		session.deferred.resolve(this.buildRepairSessionResult(session, completed));
	}

	private async refreshRepairSessionState(sessionId: string): Promise<void> {
		const session = this.repairSessions.get(sessionId);
		if (!session) {
			return;
		}
		for (const state of session.targets.values()) {
			const resolved =
				typeof this.log.hasMany === "function"
					? await this.log.hasMany(state.unresolved)
					: await this.getExistingRepairHashes(state.unresolved);
			for (const hash of resolved) {
				state.unresolved.delete(hash);
			}
		}
	}

	private async getExistingRepairHashes(
		hashes: Iterable<string>,
	): Promise<Set<string>> {
		const resolved = new Set<string>();
		for (const hash of hashes) {
			if (await this.log.has(hash)) {
				resolved.add(hash);
			}
		}
		return resolved;
	}

	private markRepairSessionResolvedHashes(hashes: string[]): void {
		if (hashes.length === 0 || this.repairSessions.size === 0) {
			return;
		}
		for (const [sessionId, session] of this.repairSessions) {
			for (const state of session.targets.values()) {
				for (const hash of hashes) {
					state.unresolved.delete(hash);
				}
			}
			if (this.isRepairSessionComplete(session)) {
				this.finalizeRepairSession(sessionId, true);
			}
		}
	}

	private markRepairSessionResolvedHash(hash: string): void {
		if (this.repairSessions.size === 0) {
			return;
		}
		for (const [sessionId, session] of this.repairSessions) {
			for (const state of session.targets.values()) {
				state.unresolved.delete(hash);
			}
			if (this.isRepairSessionComplete(session)) {
				this.finalizeRepairSession(sessionId, true);
			}
		}
	}

	private async runRepairSession(sessionId: string): Promise<void> {
		const session = this.repairSessions.get(sessionId);
		if (!session) {
			return;
		}

		let previousDelay = 0;
		for (const delayMs of session.retryIntervalsMs) {
			if (!this.repairSessions.has(sessionId) || this.closed) {
				return;
			}

			const waitMs = Math.max(0, delayMs - previousDelay);
			previousDelay = delayMs;
			if (waitMs > 0) {
				await new Promise<void>((resolve) => {
					const timer = setTimeout(resolve, waitMs);
					timer.unref?.();
				});
			}
			if (!this.repairSessions.has(sessionId) || this.closed) {
				return;
			}

			await this.refreshRepairSessionState(sessionId);
			const current = this.repairSessions.get(sessionId);
			if (!current) {
				return;
			}
			if (this.isRepairSessionComplete(current)) {
				this.finalizeRepairSession(sessionId, true);
				return;
			}

			for (const [target, state] of current.targets) {
				if (state.unresolved.size === 0) {
					continue;
				}
				state.attempts += 1;
				try {
					await this.requestSync([...state.unresolved], [target], {
						targetEpochs: new Map([[target, state.targetEpoch]]),
						createTargetEpochs: false,
					});
				} catch {
					// Best-effort: keep unresolved and let retries/timeout determine outcome.
				}
			}

			await this.refreshRepairSessionState(sessionId);
			const afterSend = this.repairSessions.get(sessionId);
			if (!afterSend) {
				return;
			}
			if (this.isRepairSessionComplete(afterSend)) {
				this.finalizeRepairSession(sessionId, true);
				return;
			}

			if (afterSend.mode === "best-effort") {
				this.finalizeRepairSession(sessionId, false);
				return;
			}
		}

		for (;;) {
			if (!this.repairSessions.has(sessionId) || this.closed) {
				return;
			}
			await this.refreshRepairSessionState(sessionId);
			const current = this.repairSessions.get(sessionId);
			if (!current) {
				return;
			}
			if (this.isRepairSessionComplete(current)) {
				this.finalizeRepairSession(sessionId, true);
				return;
			}
			await new Promise<void>((resolve) => {
				const timer = setTimeout(resolve, SESSION_POLL_INTERVAL_MS);
				timer.unref?.();
			});
		}
	}

	private getQueuedSyncKey(key: SyncableKey): SyncableKey | undefined {
		if (this.syncInFlightQueue.has(key)) {
			return key;
		}
		if (typeof key === "string") {
			for (const queuedKey of this.syncInFlightQueue.keys()) {
				if (
					typeof queuedKey === "bigint" &&
					this.coordinateToHash.get(queuedKey) === key
				) {
					return queuedKey;
				}
			}
			return undefined;
		}
		const hash = this.coordinateToHash.get(key);
		return hash && this.syncInFlightQueue.has(hash) ? hash : undefined;
	}

	startRepairSession(properties: {
		entries: Map<string, SyncEntryCoordinates<R>>;
		targets: string[];
		mode?: RepairSessionMode;
		timeoutMs?: number;
		retryIntervalsMs?: number[];
	}): RepairSession {
		const mode = properties.mode ?? "best-effort";
		const startedAt = Date.now();
		const timeoutMs = Math.max(
			1,
			Math.floor(
				properties.timeoutMs ??
					(mode === "convergent"
						? DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS
						: DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS),
			),
		);
		const retryIntervalsMs = this.normalizeRetryIntervals(
			mode,
			properties.retryIntervalsMs,
		);
		const allHashes = this.getPrioritizedHashes(properties.entries);
		const trackedHashes =
			mode === "convergent" &&
			allHashes.length > this.maxConvergentTrackedHashes
				? allHashes.slice(0, this.maxConvergentTrackedHashes)
				: allHashes;
		const truncated = trackedHashes.length < allHashes.length;
		const targets = [...new Set(properties.targets)];
		const id = `repair-${++this.repairSessionCounter}`;
		const deferred = createDeferred<RepairSessionResult[]>();

		const targetStates = new Map<string, RepairSessionTargetState>();
		for (const target of targets) {
			targetStates.set(target, {
				unresolved: new Set(trackedHashes),
				requestedCount: trackedHashes.length,
				requestedTotalCount: allHashes.length,
				attempts: 0,
				targetEpoch: this.getOrCreateSyncDispatchTargetEpoch(target),
			});
		}

		const session: RepairSessionState = {
			id,
			mode,
			startedAt,
			timeoutMs,
			retryIntervalsMs,
			targets: targetStates,
			truncated,
			deferred,
			cancelled: false,
		};

		if (allHashes.length === 0 || targets.length === 0) {
			deferred.resolve(this.buildRepairSessionResult(session, true));
			return {
				id,
				done: deferred.promise,
				cancel: () => {
					// no-op
				},
			};
		}

		// For capped convergent sessions, still dispatch the full set once so large
		// repairs are not limited to tracked hashes.
		if (mode === "convergent" && truncated) {
			void this.onMaybeMissingEntries({
				entries: properties.entries,
				targets,
			}).catch(() => {
				// Best-effort: retries on tracked hashes continue via runRepairSession.
			});
		}

		session.timer = setTimeout(() => {
			this.finalizeRepairSession(id, false);
		}, timeoutMs);
		session.timer.unref?.();

		this.repairSessions.set(id, session);
		void this.runRepairSession(id).catch(() => {
			this.finalizeRepairSession(id, false);
		});

		return {
			id,
			done: deferred.promise,
			cancel: () => {
				this.finalizeRepairSession(id, false);
			},
		};
	}

	async onMaybeMissingEntries(properties: {
		entries: Map<string, SyncEntryCoordinates<R>>;
		targets: string[];
		signal?: AbortSignal;
	}): Promise<void> {
		if (properties.signal?.aborted) {
			return;
		}
		await this.onMaybeMissingHashes({
			hashes: this.getPrioritizedHashes(properties.entries),
			targets: properties.targets,
			signal: properties.signal,
		});
	}

	async onMaybeMissingHashes(properties: {
		hashes: Iterable<string>;
		targets: string[];
		signal?: AbortSignal;
	}): Promise<void> {
		const targets = [...new Set(properties.targets)];
		const lifecycle = this.captureSyncDispatchLifecycle(
			targets,
			properties.signal,
		);
		if (!this.isSyncDispatchLifecycleActive(lifecycle)) {
			this.finishSyncDispatchLifecycle(lifecycle);
			return;
		}
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		const hashes = [...new Set(properties.hashes)];
		const targetsPerAuthorizationWindow = Math.max(
			1,
			Math.min(targets.length, MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES),
		);
		const chunks = this.chunk(
			hashes,
			Math.max(
				1,
				Math.min(
					this.maxHashesPerMessage,
					Math.floor(
						MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES /
							targetsPerAuthorizationWindow,
					),
				),
			),
		);
		let messages = 0;
		try {
			for (const chunk of chunks) {
				if (!this.isSyncDispatchLifecycleActive(lifecycle)) {
					break;
				}
				for (const target of targets) {
					if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
						continue;
					}
					const reservation = await this.reservePendingMaybeSyncResponse({
						hashes: chunk,
						targets: [target],
						lifecycle,
					});
					if (
						!reservation ||
						!this.isSyncDispatchLifecycleActive(lifecycle, target)
					) {
						continue;
					}
					const hashesToSend =
						reservation.newlyAuthorizedByTarget.get(target) ?? [];
					if (hashesToSend.length === 0) {
						continue;
					}
					if (!reservation.retained()) {
						reservation.release();
						continue;
					}
					try {
						await this.rpc.send(
							new RequestMaybeSync({ hashes: hashesToSend }),
							{
								priority: SYNC_MESSAGE_PRIORITY,
								mode: new SilentDelivery({
									to: [target],
									redundancy: 1,
								}),
								signal: this.getSyncDispatchSignal(lifecycle, target),
							},
						);
						messages += 1;
					} catch (error) {
						reservation.release();
						if (
							!this.isSyncDispatchLifecycleActive(lifecycle) ||
							!this.isSyncDispatchLifecycleActive(lifecycle, target)
						) {
							continue;
						}
						throw error;
					}
					if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
						break;
					}
				}
				if (!this.isSyncDispatchLifecycleActive(lifecycle)) {
					break;
				}
			}
		} finally {
			this.finishSyncDispatchLifecycle(lifecycle);
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.onMaybeMissingEntries",
					entries: hashes.length,
					messages,
					targets: targets.length,
					details: !this.isSyncDispatchLifecycleActive(lifecycle)
						? { cancelled: true }
						: undefined,
				});
			}
		}
	}

	/**
	 * Ship exchange heads to one peer: the fused native raw path when the
	 * peer advertised raw capability and the shared log provided a fused
	 * sender, otherwise the TS message path (raw or plain by capability).
	 * Returns the number of messages sent and whether the fused path ran.
	 */
	private async shipExchangeHeads(
		hashes: string[],
		to: PublicSignKey,
		canReceiveRaw: boolean,
		signal?: AbortSignal,
	): Promise<{ messages: number; fused: boolean }> {
		if (signal?.aborted) {
			return { messages: 0, fused: false };
		}
		if (canReceiveRaw && this.sendRawExchangeHeads) {
			let sentMessages: number | undefined;
			try {
				sentMessages = await this.sendRawExchangeHeads(
					hashes,
					[to.hashcode()],
					{ signal },
				);
			} catch (error) {
				if (signal?.aborted) {
					return { messages: 0, fused: true };
				}
				throw error;
			}
			if (sentMessages !== undefined) {
				return { messages: sentMessages, fused: true };
			}
		}
		let messages = 0;
		const messageGenerator = canReceiveRaw
			? createRawExchangeHeadsMessages(
					this.log,
					hashes,
					this.syncOptions?.profile,
				)
			: createExchangeHeadsMessages(this.log, hashes);
		for await (const message of messageGenerator) {
			if (signal?.aborted) {
				break;
			}
			try {
				await this.rpc.send(message, {
					mode: new SilentDelivery({ to: [to], redundancy: 1 }),
					signal,
				});
				messages += 1;
			} catch (error) {
				if (signal?.aborted) {
					break;
				}
				throw error;
			}
		}
		return { messages, fused: false };
	}

	/**
	 * Ships hashes that the caller has already authorized for this exact sender.
	 *
	 * This deliberately does not consult or extend Simple's bounded pending-response
	 * window. Rateless sync owns a separate, target-scoped authorization lifecycle
	 * and uses this helper only after intersecting the response with that process's
	 * exact advertised hash set.
	 */
	async shipAuthorizedMaybeSyncResponse(properties: {
		hashes: string[];
		from: PublicSignKey;
		response: ResponseMaybeSync | ResponseMaybeSyncCapabilities;
		signal: AbortSignal;
	}): Promise<{ messages: number; fused: boolean; entries: number }> {
		const hashes = this.filterRecentlySentExchangeHeads(
			properties.hashes,
			properties.from,
		);
		try {
			const shipped = await this.shipExchangeHeads(
				hashes,
				properties.from,
				canReceiveRawExchangeHeads(properties.response),
				properties.signal,
			);
			return {
				...shipped,
				entries: hashes.length,
			};
		} catch (error) {
			this.forgetRecentlySentExchangeHeads(hashes, properties.from);
			throw error;
		} finally {
			if (properties.signal.aborted) {
				this.forgetRecentlySentExchangeHeads(hashes, properties.from);
			}
		}
	}

	async shipAuthorizedMaybeSyncResponseLeases(properties: {
		leases: AuthorizedMaybeSyncResponseLease[];
		from: PublicSignKey;
		response: ResponseMaybeSync | ResponseMaybeSyncCapabilities;
		source?: string;
	}): Promise<{ messages: number; fused: boolean; entries: number }> {
		if (properties.leases.length === 0) {
			return { messages: 0, fused: false, entries: 0 };
		}
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		let messages = 0;
		let fused = false;
		let entries = 0;
		let firstError: unknown;
		for (const lease of properties.leases) {
			try {
				const shipped = await this.shipAuthorizedMaybeSyncResponse({
					hashes: lease.hashes,
					from: properties.from,
					response: properties.response,
					signal: lease.signal,
				});
				messages += shipped.messages;
				fused ||= shipped.fused;
				entries += shipped.entries;
			} catch (error) {
				firstError ??= error;
			} finally {
				lease.release();
			}
		}
		if (profile) {
			emitSyncProfileDuration(profile, startedAt, {
				name: "simple.exchangeHeads",
				entries,
				messages,
				targets: 1,
				details: {
					source: properties.source ?? "responseMaybeSync",
					fused,
				},
			});
		}
		if (firstError !== undefined) {
			throw firstError;
		}
		return { messages, fused, entries };
	}

	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		const from = context.from!;
		if (msg instanceof RequestMaybeSync) {
			await this.queueSync(msg.hashes, from);
			return true;
		} else if (
			msg instanceof ResponseMaybeSync ||
			msg instanceof ResponseMaybeSyncCapabilities
		) {
			// TODO perhaps send less messages to more receivers for performance reasons?
			// TODO wait for previous send to target before trying to send more?

			const pending = this.consumeAuthorizedMaybeSyncResponse(msg.hashes, from);
			if (pending.length === 0) {
				return true;
			}
			await this.shipAuthorizedMaybeSyncResponseLeases({
				leases: pending,
				from,
				response: msg,
			});
			return true;
		} else if (
			msg instanceof RequestMaybeSyncCoordinate ||
			msg instanceof RequestMaybeSyncCoordinateCapabilities
		) {
			const target = from.hashcode();
			const lifecycle = this.captureSyncDispatchLifecycle([target]);
			try {
				if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
					return true;
				}
				const profile = this.syncOptions?.profile;
				const lookupStartedAt = syncProfileStart(profile);
				const hashes = await getHashesFromSymbols(
					msg.hashNumbers,
					this.entryIndex,
					this.coordinateToHash,
					this.resolveHashesForSymbols,
					this.resolveHashListForSymbols,
				);
				if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
					return true;
				}
				if (profile) {
					emitSyncProfileDuration(profile, lookupStartedAt, {
						name: "simple.coordinateLookup",
						entries: hashLookupResultSize(hashes),
						symbols: msg.hashNumbers.length,
					});
				}

				const exchangeStartedAt = syncProfileStart(profile);
				const hashesToSend = this.filterRecentlySentExchangeHeads(hashes, from);
				let messages = 0;
				let fused = false;
				try {
					// dont set priority 1 here because this will block other messages that should higher priority
					({ messages, fused } = await this.shipExchangeHeads(
						hashesToSend,
						context.from!,
						canReceiveRawExchangeHeads(msg),
						this.getSyncDispatchSignal(lifecycle, target),
					));
				} finally {
					if (profile) {
						emitSyncProfileDuration(profile, exchangeStartedAt, {
							name: "simple.exchangeHeads",
							entries: hashesToSend.length,
							messages,
							targets: 1,
							details: {
								source: "requestMaybeSyncCoordinate",
								fused,
							},
						});
					}
				}

				return true;
			} finally {
				this.finishSyncDispatchLifecycle(lifecycle);
			}
		} else {
			return false; // no message was consumed
		}
	}

	onReceivedEntries(properties: {
		entries: EntryWithRefs<any>[];
		from: PublicSignKey;
	}): Promise<void> | void {
		return this.onReceivedEntryHashes({
			hashes: properties.entries.map((entry) => entry.entry.hash),
			from: properties.from,
		});
	}

	onReceivedEntryHashes(properties: {
		hashes: string[];
		from: PublicSignKey;
	}): Promise<void> | void {
		this.clearSyncInFlightForPeerHashes(
			properties.from.hashcode(),
			properties.hashes,
		);
		this.markRepairSessionResolvedHashes(properties.hashes);
	}

	async queueSync(
		keys: SyncableKey[],
		from: PublicSignKey,
		options?: { skipCheck?: boolean },
	) {
		const fromHash = from.hashcode();
		const targetEpoch = this.getOrCreateSyncDispatchTargetEpoch(fromHash);
		const ownershipLifecycleController = this.syncDispatchLifecycleController;
		const isCapturedLifecycleActive = () =>
			this.closed !== true &&
			this.syncDispatchLifecycleController === ownershipLifecycleController &&
			!ownershipLifecycleController.signal.aborted &&
			this.syncDispatchTargetEpochs.get(fromHash) === targetEpoch;
		const requestHashes: SyncableKey[] = [];
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		const resolveKnownStartedAt = syncProfileStart(profile);
		const knownKeys =
			options?.skipCheck === true
				? undefined
				: await this.resolveKnownSyncKeys(keys);
		if (!isCapturedLifecycleActive()) {
			return;
		}
		if (profile) {
			emitSyncProfileDuration(profile, resolveKnownStartedAt, {
				name: "simple.queueSync.resolveKnown",
				entries: keys.length,
				count: knownKeys?.keys.size ?? 0,
				details: {
					checkedCoordinates: knownKeys?.checkedCoordinates === true,
					checkedHashes: knownKeys?.checkedHashes === true,
					skipCheck: options?.skipCheck === true,
				},
			});
		}
		let queuedHashAliases: Map<string, SyncableKey> | undefined;
		const getQueuedSyncKeyForBatch = (key: SyncableKey) => {
			if (this.syncInFlightQueue.has(key)) {
				return key;
			}
			if (typeof key === "string") {
				if (!queuedHashAliases) {
					queuedHashAliases = new Map();
					for (const queuedKey of this.syncInFlightQueue.keys()) {
						if (typeof queuedKey !== "bigint") {
							continue;
						}
						const hash = this.coordinateToHash.get(queuedKey);
						if (hash) {
							queuedHashAliases.set(hash, queuedKey);
						}
					}
				}
				return queuedHashAliases.get(key);
			}
			const hash = this.coordinateToHash.get(key);
			return hash && this.syncInFlightQueue.has(hash) ? hash : undefined;
		};

		try {
			const loopStartedAt = syncProfileStart(profile);
			for (const key of keys) {
				if (!isCapturedLifecycleActive()) {
					return;
				}
				const coordinateOrHash = getQueuedSyncKeyForBatch(key) ?? key;
				const inFlight = this.syncInFlightQueue.get(coordinateOrHash);
				if (inFlight) {
					if (!inFlight.find((x) => x.hashcode() === fromHash)) {
						inFlight.push(from);
						let inverted = this.syncInFlightQueueInverted.get(fromHash);
						if (!inverted) {
							inverted = new Set();
							this.syncInFlightQueueInverted.set(fromHash, inverted);
						}
						inverted.add(coordinateOrHash);
					}
				} else {
					const has =
						options?.skipCheck !== true &&
						(await this.checkHasCoordinateOrHash(coordinateOrHash, knownKeys));
					if (!isCapturedLifecycleActive()) {
						return;
					}
					if (has) {
						continue;
					}
					// Track the initial sender so we can retry if the first request is lost.
					this.syncInFlightQueue.set(coordinateOrHash, [from]);
					let inverted = this.syncInFlightQueueInverted.get(fromHash);
					if (!inverted) {
						inverted = new Set();
						this.syncInFlightQueueInverted.set(fromHash, inverted);
					}
					inverted.add(coordinateOrHash);
					requestHashes.push(coordinateOrHash); // request immediately (first time we have seen this hash)
					if (
						queuedHashAliases &&
						typeof coordinateOrHash === "bigint"
					) {
						const hash = this.coordinateToHash.get(coordinateOrHash);
						if (hash) {
							queuedHashAliases.set(hash, coordinateOrHash);
						}
					}
				}
			}
			if (profile) {
				emitSyncProfileDuration(profile, loopStartedAt, {
					name: "simple.queueSync.plan",
					entries: keys.length,
					count: requestHashes.length,
					targets: 1,
				});
			}

			requestHashes.length > 0 &&
				(await this.requestSync(requestHashes, [fromHash], {
					ownershipLifecycleController,
					targetEpochs: new Map([[fromHash, targetEpoch]]),
					createTargetEpochs: false,
				}));
		} finally {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.queueSync",
					entries: keys.length,
					targets: 1,
					details: {
						requested: requestHashes.length,
						skipCheck: options?.skipCheck === true,
					},
				});
			}
		}
	}

	private async requestSync(
		hashes: SyncableKey[],
		to: Set<string> | string[],
		options?: {
			ownershipLifecycleController?: AbortController;
			targetEpochs?: Map<string, SyncDispatchTargetEpoch>;
			createTargetEpochs?: boolean;
		},
	) {
		const targets = [...new Set(to)];
		if (hashes.length === 0 || targets.length === 0) {
			return;
		}
		const lifecycle = this.captureSyncDispatchLifecycle(targets, undefined, {
			ownershipLifecycleController: options?.ownershipLifecycleController,
			targetEpochs: options?.targetEpochs,
			createTargetEpochs: options?.createTargetEpochs,
		});
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		let coordinateMessages = 0;
		let stringMessages = 0;
		let coordinateHashCount = 0;
		let stringHashCount = 0;

		try {
			const now = +new Date();
			for (const node of targets) {
				if (!this.isSyncDispatchLifecycleActive(lifecycle, node)) {
					continue;
				}
				let map = this.syncInFlight.get(node);
				if (!map) {
					map = new Map();
					this.syncInFlight.set(node, map);
				}
				for (const hash of hashes) {
					map.set(hash, { timestamp: now });
				}
			}

			const coordinateHashes: bigint[] = [];
			const stringHashes: string[] = [];
			for (const hash of hashes) {
				if (typeof hash === "bigint") {
					coordinateHashes.push(hash);
				} else {
					stringHashes.push(hash);
				}
			}
			coordinateHashCount = coordinateHashes.length;
			stringHashCount = stringHashes.length;

			if (coordinateHashes.length > 0) {
				const chunks = this.chunk(
					coordinateHashes,
					this.maxCoordinatesPerMessage,
				);
				for (const target of targets) {
					for (const chunk of chunks) {
						if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
							break;
						}
						try {
							await this.rpc.send(
								this.syncOptions?.rawExchangeHeads
									? new RequestMaybeSyncCoordinateCapabilities({
											hashNumbers: chunk,
										})
									: new RequestMaybeSyncCoordinate({
											hashNumbers: chunk,
										}),
								{
									mode: new SilentDelivery({
										to: [target],
										redundancy: 1,
									}),
									priority: SYNC_MESSAGE_PRIORITY,
									signal: this.getSyncDispatchSignal(lifecycle, target),
								},
							);
							coordinateMessages += 1;
						} catch (error) {
							if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
								break;
							}
							throw error;
						}
					}
				}
			}
			if (stringHashes.length > 0) {
				const chunks = this.chunk(stringHashes, this.maxHashesPerMessage);
				for (const target of targets) {
					for (const chunk of chunks) {
						if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
							break;
						}
						try {
							await this.rpc.send(
								this.syncOptions?.rawExchangeHeads
									? new ResponseMaybeSyncCapabilities({
											hashes: chunk,
										})
									: new ResponseMaybeSync({ hashes: chunk }),
								{
									mode: new SilentDelivery({
										to: [target],
										redundancy: 1,
									}),
									priority: SYNC_MESSAGE_PRIORITY,
									signal: this.getSyncDispatchSignal(lifecycle, target),
								},
							);
							stringMessages += 1;
						} catch (error) {
							if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
								break;
							}
							throw error;
						}
					}
				}
			}
		} finally {
			this.finishSyncDispatchLifecycle(lifecycle);
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.requestSync",
					entries: hashes.length,
					messages: coordinateMessages + stringMessages,
					targets: targets.length,
					details: {
						coordinateHashes: coordinateHashCount,
						stringHashes: stringHashCount,
					},
				});
			}
		}
	}
	private async resolveKnownSyncKeys(
		keys: SyncableKey[],
	): Promise<KnownSyncKeys | undefined> {
		const hashes: string[] = [];
		const coordinates: bigint[] = [];
		for (const key of keys) {
			if (typeof key === "bigint") {
				coordinates.push(key);
			} else {
				hashes.push(key);
			}
		}
		const known: KnownSyncKeys = {
			keys: new Set(),
			checkedCoordinates: false,
			checkedHashes: false,
		};
		if (hashes.length > 0) {
			for (const hash of await this.log.hasMany(hashes)) {
				known.keys.add(hash);
			}
			known.checkedHashes = true;
		}
		if (coordinates.length > 0 && this.resolveHashesForSymbols) {
			const resolved = await this.resolveHashesForSymbols(coordinates);
			if (resolved) {
				for (const coordinate of coordinates) {
					const hashes = resolved.get(coordinate);
					if (!hashes) {
						continue;
					}
					for (const _hash of hashes) {
						known.keys.add(coordinate);
						break;
					}
				}
				known.checkedCoordinates = true;
			}
		}
		return known.checkedCoordinates || known.checkedHashes ? known : undefined;
	}

	private async checkHasCoordinateOrHash(
		key: string | bigint,
		knownKeys?: KnownSyncKeys,
	) {
		if (knownKeys) {
			if (knownKeys.keys.has(key)) {
				return true;
			}
			if (typeof key === "bigint" && knownKeys.checkedCoordinates) {
				return false;
			}
			if (typeof key === "string" && knownKeys.checkedHashes) {
				return false;
			}
		}
		return typeof key === "bigint"
			? (await this.entryIndex.count({ query: { hashNumber: key } })) > 0
			: this.log.has(key);
	}
	async open() {
		this.syncDispatchLifecycleController.abort();
		this.clearPendingMaybeSyncResponses();
		this.syncDispatchTargetEpochs.clear();
		this.syncDispatchLifecycleController = new AbortController();
		const openLifecycleController = this.syncDispatchLifecycleController;
		this.closed = false;
		const isOpenLifecycleActive = () =>
			this.closed !== true &&
			this.syncDispatchLifecycleController === openLifecycleController &&
			!openLifecycleController.signal.aborted;
		let requestSyncLoop!: () => Promise<void>;
		const scheduleRequestSyncLoop = () => {
			if (!isOpenLifecycleActive()) {
				return;
			}
			this.syncMoreInterval = setTimeout(() => {
				void requestSyncLoop().catch(() => {
					// The loop is best-effort. Observe all failures so a transport or
					// storage rejection cannot become an unhandled process rejection.
				});
			}, 3e3);
		};
		requestSyncLoop = async () => {
			/**
			 * This method fetches entries that we potentially want.
			 * In a case in which we become replicator of a segment,
			 * multiple remote peers might want to send us entries
			 * This method makes sure that we only request on entry from the remotes at a time
			 * so we don't get flooded with the same entry
			 */

			if (!isOpenLifecycleActive()) {
				return;
			}
			try {
				const requestHashes: SyncableKey[] = [];
				const from: Set<string> = new Set();
				const now = Date.now();
				for (const [key] of this.syncInFlightQueue) {
					if (!isOpenLifecycleActive()) {
						return;
					}

					const has = await this.checkHasCoordinateOrHash(key);
					if (!isOpenLifecycleActive()) {
						return;
					}
					const value = this.syncInFlightQueue.get(key);
					if (!value) {
						continue;
					}

					if (!has) {
						if (value.length === 0) {
							this.clearSyncProcessKey(key);
							continue;
						}

						const candidate = value[0]!;
						const publicKeyHash = candidate.hashcode();
						const inflightTimestamp = this.syncInFlight
							.get(publicKeyHash)
							?.get(key)?.timestamp;
						if (
							inflightTimestamp == null ||
							now - inflightTimestamp >= SIMPLE_SYNC_RETRY_AFTER_MS
						) {
							requestHashes.push(key);
							from.add(publicKeyHash);
							if (value.length > 1) {
								value.push(value.shift()!);
							}
						}
					} else {
						this.clearSyncProcessKey(key);
					}
				}

				if (!isOpenLifecycleActive()) {
					return;
				}
				const nowMin10s = +new Date() - 2e4;
				for (const [key, map] of this.syncInFlight) {
					for (const [hash, { timestamp }] of map) {
						if (timestamp < nowMin10s) {
							map.delete(hash);
						}
					}
					if (map.size === 0) {
						this.syncInFlight.delete(key);
					}
				}
				if (!isOpenLifecycleActive()) {
					return;
				}
				const targetEpochs = new Map<string, SyncDispatchTargetEpoch>();
				for (const target of from) {
					const epoch = this.syncDispatchTargetEpochs.get(target);
					if (epoch) {
						targetEpochs.set(target, epoch);
					}
				}
				await this.requestSync(requestHashes, from, {
					ownershipLifecycleController: openLifecycleController,
					targetEpochs,
					createTargetEpochs: false,
				});
				if (!isOpenLifecycleActive()) {
					return;
				}
			} catch {
				if (!isOpenLifecycleActive()) {
					return;
				}
				// Retry on the next interval; the rejection is deliberately observed.
			}
			scheduleRequestSyncLoop();
		};

		void requestSyncLoop().catch(() => {
			// Defensive observation for failures outside the loop's best-effort body.
		});
	}

	async close() {
		this.closed = true;
		this.syncDispatchLifecycleController.abort();
		this.syncDispatchTargetEpochs.clear();
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		this.recentlySentExchangeHeads.clear();
		this.clearPendingMaybeSyncResponses();
		for (const sessionId of [...this.repairSessions.keys()]) {
			this.finalizeRepairSession(sessionId, false);
		}
		clearTimeout(this.syncMoreInterval);
	}
	onEntryAdded(entry: Entry<any>): void {
		this.onEntryAddedHash(entry.hash);
	}

	onEntryAddedHashes(hashes: string[]): void {
		if (hashes.length === 0 || !this.hasEntryAddedState()) {
			return;
		}
		this.clearSyncProcesses(hashes);
		this.markRepairSessionResolvedHashes(hashes);
	}

	onEntryAddedHash(hash: string): void {
		if (!this.hasEntryAddedState()) {
			return;
		}
		this.clearSyncProcess(hash);
		this.markRepairSessionResolvedHash(hash);
	}

	onEntryRemoved(hash: string): void {
		if (!this.hasSyncProcessState()) {
			return;
		}
		return this.clearSyncProcess(hash);
	}

	onEntryRemovedHashes(hashes: string[]): void {
		if (hashes.length === 0 || !this.hasSyncProcessState()) {
			return;
		}
		return this.clearSyncProcesses(hashes);
	}

	private hasEntryAddedState(): boolean {
		return this.hasSyncProcessState() || this.repairSessions.size > 0;
	}

	private hasSyncProcessState(): boolean {
		return (
			this.syncInFlightQueue.size > 0 ||
			this.syncInFlightQueueInverted.size > 0 ||
			this.syncInFlight.size > 0
		);
	}

	private clearSyncProcessKey(key: SyncableKey) {
		const inflight = this.syncInFlightQueue.get(key);
		if (inflight) {
			for (const peer of inflight) {
				const map = this.syncInFlightQueueInverted.get(peer.hashcode());
				if (map) {
					map.delete(key);
					if (map.size === 0) {
						this.syncInFlightQueueInverted.delete(peer.hashcode());
					}
				}
			}

			this.syncInFlightQueue.delete(key);
		}

		this.clearSyncInFlightKey(key);
	}

	private clearSyncInFlightKey(key: SyncableKey) {
		for (const [peer, map] of this.syncInFlight) {
			map.delete(key);
			if (map.size === 0) {
				this.syncInFlight.delete(peer);
			}
		}
	}

	private forEachKnownAlias(
		hash: string,
		callback: (key: SyncableKey) => void,
	): void {
		callback(hash);
		if (this.syncInFlightQueue.size === 0 && this.syncInFlight.size === 0) {
			return;
		}
		for (const key of this.syncInFlightQueue.keys()) {
			if (typeof key === "bigint" && this.coordinateToHash.get(key) === hash) {
				callback(key);
			}
		}
		for (const map of this.syncInFlight.values()) {
			for (const key of map.keys()) {
				if (
					typeof key === "bigint" &&
					this.coordinateToHash.get(key) === hash
				) {
					callback(key);
				}
			}
		}
	}

	private clearSyncInFlightForPeer(publicKeyHash: string, hash: string) {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map) {
			return;
		}
		this.forEachKnownAlias(hash, (key) => map.delete(key));
		if (map.size === 0) {
			this.syncInFlight.delete(publicKeyHash);
		}
	}

	private clearSyncInFlightForPeerHashes(
		publicKeyHash: string,
		hashes: string[],
	) {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map || hashes.length === 0) {
			return;
		}
		const keys = new Set<SyncableKey>(hashes);
		const hashSet = new Set(hashes);
		for (const key of map.keys()) {
			if (typeof key !== "bigint") {
				continue;
			}
			const hash = this.coordinateToHash.get(key);
			if (hash != null && hashSet.has(hash)) {
				keys.add(key);
			}
		}
		for (const key of keys) {
			map.delete(key);
		}
		if (map.size === 0) {
			this.syncInFlight.delete(publicKeyHash);
		}
	}

	private clearSyncProcess(hash: string) {
		this.forEachKnownAlias(hash, (key) => this.clearSyncProcessKey(key));
	}

	private clearSyncProcesses(hashes: string[]) {
		if (hashes.length === 0) {
			return;
		}
		const keys = new Set<SyncableKey>(hashes);
		const hashSet = new Set(hashes);
		const maybeAddAlias = (key: SyncableKey) => {
			if (typeof key !== "bigint") {
				return;
			}
			const hash = this.coordinateToHash.get(key);
			if (hash != null && hashSet.has(hash)) {
				keys.add(key);
			}
		};
		for (const key of this.syncInFlightQueue.keys()) {
			maybeAddAlias(key);
		}
		for (const map of this.syncInFlight.values()) {
			for (const key of map.keys()) {
				maybeAddAlias(key);
			}
		}
		for (const key of keys) {
			this.clearSyncProcessKey(key);
		}
	}

	onPeerDisconnected(key: PublicSignKey | string): Promise<void> | void {
		const publicKeyHash = typeof key === "string" ? key : key.hashcode();
		return this.clearSyncProcessPublicKeyHash(publicKeyHash);
	}
	private clearSyncProcessPublicKeyHash(publicKeyHash: string) {
		this.syncDispatchTargetEpochs.delete(publicKeyHash);
		for (const targetLifecycle of [
			...(this.syncDispatchTargets.get(publicKeyHash) ?? []),
		]) {
			if (targetLifecycle.lifecycle.abortAllOnTargetDisconnect) {
				this.abortSyncDispatchLifecycle(
					targetLifecycle.lifecycle,
					new Error("sync target disconnected"),
				);
			} else {
				this.abortSyncDispatchTarget(
					targetLifecycle,
					new Error("sync target disconnected"),
				);
			}
		}
		this.syncInFlight.delete(publicKeyHash);
		this.recentlySentExchangeHeads.delete(publicKeyHash);
		this.clearPendingMaybeSyncResponses(publicKeyHash);
		const map = this.syncInFlightQueueInverted.get(publicKeyHash);
		if (map) {
			for (const hash of map) {
				const arr = this.syncInFlightQueue.get(hash);
				if (arr) {
					const filtered = arr.filter((x) => x.hashcode() !== publicKeyHash);
					if (filtered.length > 0) {
						this.syncInFlightQueue.set(hash, filtered);
					} else {
						this.syncInFlightQueue.delete(hash);
					}
				}
			}
			this.syncInFlightQueueInverted.delete(publicKeyHash);
		}
	}

	get pending() {
		return this.syncInFlightQueue.size;
	}
}
