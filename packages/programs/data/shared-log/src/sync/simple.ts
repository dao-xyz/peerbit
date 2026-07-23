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
	HashSymbolHashListResolver,
	HashSymbolResolver,
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
	maxHashes = 10_000,
): Promise<Set<string> | string[]> => {
	let queries: IntegerCompare[] = [];
	let batchSize = 128; // TODO arg
	let results = new Set<string>();
	let missingSymbols: bigint[] = [];
	const addHash = (hash: string) => {
		if (results.has(hash)) {
			return true;
		}
		if (results.size >= maxHashes) {
			return false;
		}
		results.add(hash);
		return true;
	};
	const addMissingUnlessCached = (symbol: bigint) => {
		const fromCache = coordinateToHash.get(symbol);
		if (fromCache) {
			addHash(fromCache);
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
				if (!addHash(entry.value.hash)) {
					break;
				}
				coordinateToHash.add(entry.value.hashNumber, entry.value.hash);
				if (results.size >= maxHashes) {
					break;
				}
			}
		}
	};

	if (resolveHashListForSymbols) {
		const resolvedHashes = await resolveHashListForSymbols(symbols);
		if (resolvedHashes) {
			const resolvedHashList: string[] = [];
			const iterator = resolvedHashes[Symbol.iterator]();
			let exhausted = false;
			try {
				while (resolvedHashList.length < maxHashes) {
					const next = iterator.next();
					if (next.done) {
						exhausted = true;
						break;
					}
					resolvedHashList.push(next.value);
				}
			} finally {
				if (!exhausted) {
					iterator.return?.();
				}
			}
			let mergedHashes: Set<string> | undefined;
			for (const symbol of symbols) {
				const fromCache = coordinateToHash.get(symbol);
				if (fromCache) {
					mergedHashes ??= new Set(resolvedHashList);
					if (mergedHashes.size < maxHashes) {
						mergedHashes.add(fromCache);
					}
				}
			}
			return mergedHashes ?? resolvedHashList;
		}
	}

	if (resolveHashesForSymbols) {
		const resolved = await resolveHashesForSymbols(symbols);
		if (resolved) {
			let resolvedItemCount = 0;
			for (const symbol of symbols) {
				if (resolvedItemCount >= maxHashes) {
					break;
				}
				const hashes = resolved.get(symbol);
				if (!hashes) {
					addMissingUnlessCached(symbol);
					continue;
				}
				let singleHash: string | undefined;
				let count = 0;
				let truncated = false;
				const iterator = hashes[Symbol.iterator]();
				let exhausted = false;
				try {
					while (resolvedItemCount < maxHashes) {
						const next = iterator.next();
						if (next.done) {
							exhausted = true;
							break;
						}
						resolvedItemCount += 1;
						if (!addHash(next.value)) {
							truncated = true;
							break;
						}
						singleHash = next.value;
						count += 1;
						if (results.size >= maxHashes) {
							truncated = true;
							break;
						}
					}
				} finally {
					if (!exhausted) {
						truncated = true;
						iterator.return?.();
					}
				}
				if (count === 0) {
					addMissingUnlessCached(symbol);
				} else if (count === 1 && !truncated) {
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
		if (results.size >= maxHashes) {
			break;
		}
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
export const MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS = 1_024;
export const MAX_SIMPLE_COORDINATE_RESPONSE_HASHES = 10_000;
export const MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER = 4;
export const MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_GLOBAL = 32;
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
// An incoming maybe-sync claim keeps one retry candidate in both
// syncInFlightQueue and syncInFlightQueueInverted. Bound associations rather
// than only unique keys: otherwise many peers can grow the claimant array for
// the same keys without changing syncInFlightQueue.size.
// The per-peer allowance matches one full 10,000-hash response-authorization
// window; the global allowance lets four peers make full bounded progress.
export const MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER = 10_000;
export const MAX_PENDING_SIMPLE_SYNC_KEYS_GLOBAL = 40_000;
// Storage presence resolution is not universally abortable. Keep the number of
// live resolver calls small even when requests use only one key each.
export const MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER = 4;
export const MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL = 32;
// Retry scanning can touch persistent indexes. Bound each pass so a full
// adversarial queue cannot force 40,000 lookups in one event-loop turn.
export const MAX_SIMPLE_SYNC_RETRY_KEYS_PER_TICK = 4_096;
// Late coordinate-to-hash cache fills are discovered incrementally. Keep this
// independent of retained queue size so an empty or repeated request cannot
// force an O(global pending keys) reverse-alias rebuild.
const MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE = 128;
const QUEUED_SYNC_ALIAS_REFRESH_PENDING = Symbol(
	"queued-sync-alias-refresh-pending",
);
// This is an absolute first-seen lifetime. Repeated claims and additional peers
// deliberately do not slide the deadline.
export const PENDING_SIMPLE_SYNC_KEY_TTL_MS = 60_000;
// Bound retained request/response associations globally. Ten thousand hashes
// covers several full default-size request batches while keeping adversarial or
// abandoned requests to a small, predictable amount of heap.
const MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES = 10_000;
export const MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER = 4;
export const MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_GLOBAL = 32;
const MAX_PENDING_MAYBE_SYNC_RESPONSE_WAITER_BYPASSES = 32;
const MAX_PENDING_MAYBE_SYNC_RESPONSE_WAITERS = 10_000;

type PendingSyncAdmissionReservation = {
	peer: string;
	remaining: number;
	active: boolean;
	released: boolean;
	expiresAt: number;
	identities: Set<SyncableKey>;
	retainedSettled: number;
};

type PendingSyncExpiryNode =
	| {
			kind: "key";
			key: SyncableKey;
			expiresAt: number;
			heapIndex: number;
	  }
	| {
			kind: "admission";
			reservation: PendingSyncAdmissionReservation;
			expiresAt: number;
			heapIndex: number;
	  };

type PendingMaybeSyncResponse = {
	hashes: Set<string>;
	target: string;
	targetLifecycle: SyncDispatchTargetLifecycle;
	expiresAt: number;
	heapIndex: number;
};

type PendingMaybeSyncResponseAuthorizationEvent =
	| "delivered"
	| "fulfilled"
	| "released";

type PendingMaybeSyncResponseAuthorization = {
	batch: PendingMaybeSyncResponse;
	hash: string;
	waiters: Set<(event: PendingMaybeSyncResponseAuthorizationEvent) => void>;
	requestDelivered?: boolean;
	deliveryInFlight?: boolean;
	settled?: "fulfilled" | "released";
	active?: boolean;
};

type PendingMaybeSyncResponseReservation = {
	release: () => void;
	beginDelivery: () => void;
	finishDelivery: () => void;
	markDelivered: () => void;
	newlyAuthorizedByTarget: Map<string, string[]>;
	retained: () => boolean;
	signal: AbortSignal;
};

type PendingMaybeSyncResponseReservationAttempt =
	| {
			kind: "reserved";
			reservation: PendingMaybeSyncResponseReservation;
			conflicts: PendingMaybeSyncResponseAuthorization[];
	  }
	| {
			kind: "capacity";
			required: number;
	  }
	| {
			kind: "inactive";
	  };

type PendingMaybeSyncResponseWaiter = {
	required: number;
	associations: number;
	order: number;
	bypasses: number;
	heapIndex: number;
	fitHeapIndex: number;
	wake: () => void;
};

export type AuthorizedMaybeSyncResponseLease = {
	hashes: string[];
	signal: AbortSignal;
	release: (options?: { fulfilled?: boolean }) => void;
};

type SyncDispatchLifecycle = {
	ownershipLifecycleController: AbortController;
	callerSignal?: AbortSignal;
	controller: AbortController;
	targets: Map<string, SyncDispatchTargetLifecycle>;
	retainedWork: number;
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
	private syncInFlightQueueExpiresAt: Map<SyncableKey, number>;
	private syncInFlightQueueExpiryTimer?: ReturnType<typeof setTimeout>;
	private pendingSyncExpiryHeap: PendingSyncExpiryNode[];
	private pendingSyncKeyExpiryNodes: Map<SyncableKey, PendingSyncExpiryNode>;
	private pendingSyncAdmissionExpiryNodes: Map<
		PendingSyncAdmissionReservation,
		PendingSyncExpiryNode
	>;
	private syncInFlightRetryIterator?: IterableIterator<
		[SyncableKey, PublicSignKey[]]
	>;
	private syncInFlightRetryRemaining = 0;
	private syncInFlightQueueClaimants: Map<SyncableKey, Set<string>>;
	private syncInFlightQueueClaimantIndexes: Map<
		SyncableKey,
		Map<string, number>
	>;
	private syncInFlightQueueRoundRobinCursor: Map<SyncableKey, number>;
	private syncInFlightQueuedCoordinates: Set<bigint>;
	private syncInFlightQueuedHashByCoordinate: Map<bigint, string>;
	private syncInFlightQueuedCoordinatesByHash: Map<string, Set<bigint>>;
	private syncInFlightQueuedCoordinateRefreshIterator?: IterableIterator<bigint>;
	private pendingSyncClaimCount: number;
	private pendingSyncAdmissionCount: number;
	private pendingSyncActiveAdmissionReservations: number;
	private pendingSyncAdmissionCountByPeer: Map<string, number>;
	private pendingSyncAdmissionIdentitiesByPeer: Map<string, Set<SyncableKey>>;
	private pendingSyncAdmissionReservations: Set<PendingSyncAdmissionReservation>;
	private pendingSyncAdmissionReservationsByPeer: Map<
		string,
		Set<PendingSyncAdmissionReservation>
	>;
	private pendingSyncAdmissionReservationsByIdentity: Map<
		SyncableKey,
		Set<PendingSyncAdmissionReservation>
	>;
	private pendingCoordinateLookupCount: number;
	private pendingCoordinateLookupCountByPeer: Map<string, number>;
	private pendingCoordinateResponseCount: number;
	private pendingCoordinateResponseCountByPeer: Map<string, number>;

	// map of hash to public keys that we have asked for entries
	syncInFlight!: Map<string, Map<SyncableKey, { timestamp: number }>>;
	private syncInFlightTargetsByKey: Map<SyncableKey, Set<string>>;

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
	private pendingMaybeSyncResponseWaiters: Set<PendingMaybeSyncResponseWaiter>;
	private pendingMaybeSyncResponseWaiterHeap: PendingMaybeSyncResponseWaiter[];
	private pendingMaybeSyncResponseWaiterFitHeap: PendingMaybeSyncResponseWaiter[];
	private pendingMaybeSyncResponseWaiterOrder: number;
	private pendingMaybeSyncResponseWaiterAssociationCount: number;
	private pendingMaybeSyncResponseWakeScheduled: boolean;
	private pendingMaybeSyncResponseConflictWaiterCount: number;
	private pendingMaybeSyncResponseBatches: Set<PendingMaybeSyncResponse>;
	private pendingMaybeSyncResponseExpiryTimer?: ReturnType<typeof setTimeout>;
	private pendingMaybeSyncResponseExpiryHeap: PendingMaybeSyncResponse[];
	private activeMaybeSyncResponseCount: number;
	private activeMaybeSyncResponseCountByPeer: Map<string, number>;
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
		this.syncInFlightQueueExpiresAt = new Map();
		this.pendingSyncExpiryHeap = [];
		this.pendingSyncKeyExpiryNodes = new Map();
		this.pendingSyncAdmissionExpiryNodes = new Map();
		this.syncInFlightQueueClaimants = new Map();
		this.syncInFlightQueueClaimantIndexes = new Map();
		this.syncInFlightQueueRoundRobinCursor = new Map();
		this.syncInFlightQueuedCoordinates = new Set();
		this.syncInFlightQueuedHashByCoordinate = new Map();
		this.syncInFlightQueuedCoordinatesByHash = new Map();
		this.pendingSyncClaimCount = 0;
		this.pendingSyncAdmissionCount = 0;
		this.pendingSyncActiveAdmissionReservations = 0;
		this.pendingSyncAdmissionCountByPeer = new Map();
		this.pendingSyncAdmissionIdentitiesByPeer = new Map();
		this.pendingSyncAdmissionReservations = new Set();
		this.pendingSyncAdmissionReservationsByPeer = new Map();
		this.pendingSyncAdmissionReservationsByIdentity = new Map();
		this.pendingCoordinateLookupCount = 0;
		this.pendingCoordinateLookupCountByPeer = new Map();
		this.pendingCoordinateResponseCount = 0;
		this.pendingCoordinateResponseCountByPeer = new Map();
		this.syncInFlight = new Map();
		this.syncInFlightTargetsByKey = new Map();
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
		this.pendingMaybeSyncResponseWaiterHeap = [];
		this.pendingMaybeSyncResponseWaiterFitHeap = [];
		this.pendingMaybeSyncResponseWaiterOrder = 0;
		this.pendingMaybeSyncResponseWaiterAssociationCount = 0;
		this.pendingMaybeSyncResponseWakeScheduled = false;
		this.pendingMaybeSyncResponseConflictWaiterCount = 0;
		this.pendingMaybeSyncResponseBatches = new Set();
		this.pendingMaybeSyncResponseExpiryHeap = [];
		this.activeMaybeSyncResponseCount = 0;
		this.activeMaybeSyncResponseCountByPeer = new Map();
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
			? Math.max(1, Math.floor(value))
			: DEFAULT_MAX_HASHES_PER_MESSAGE;
	}

	private get maxCoordinatesPerMessage() {
		const value = this.syncOptions?.maxSimpleCoordinatesPerMessage;
		return value && Number.isFinite(value) && value > 0
			? Math.min(
					MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS,
					Math.max(1, Math.floor(value)),
				)
			: DEFAULT_MAX_COORDINATES_PER_MESSAGE;
	}

	private get maxConvergentTrackedHashes() {
		const value = this.syncOptions?.maxConvergentTrackedHashes;
		return value && Number.isFinite(value) && value > 0
			? Math.max(1, Math.floor(value))
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
			retainedWork: 0,
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
			lifecycle.retainedWork > 0
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

	private pendingMaybeSyncResponseWaiterBefore(
		left: PendingMaybeSyncResponseWaiter,
		right: PendingMaybeSyncResponseWaiter,
	): boolean {
		return left.order < right.order;
	}

	private swapPendingMaybeSyncResponseWaiters(
		left: number,
		right: number,
	): void {
		const leftWaiter = this.pendingMaybeSyncResponseWaiterHeap[left]!;
		const rightWaiter = this.pendingMaybeSyncResponseWaiterHeap[right]!;
		this.pendingMaybeSyncResponseWaiterHeap[left] = rightWaiter;
		this.pendingMaybeSyncResponseWaiterHeap[right] = leftWaiter;
		rightWaiter.heapIndex = left;
		leftWaiter.heapIndex = right;
	}

	private pushPendingMaybeSyncResponseWaiter(
		waiter: PendingMaybeSyncResponseWaiter,
	): void {
		waiter.heapIndex = this.pendingMaybeSyncResponseWaiterHeap.length;
		this.pendingMaybeSyncResponseWaiterHeap.push(waiter);
		let index = waiter.heapIndex;
		while (index > 0) {
			const parent = Math.floor((index - 1) / 2);
			if (
				this.pendingMaybeSyncResponseWaiterBefore(
					this.pendingMaybeSyncResponseWaiterHeap[parent]!,
					this.pendingMaybeSyncResponseWaiterHeap[index]!,
				)
			) {
				break;
			}
			this.swapPendingMaybeSyncResponseWaiters(parent, index);
			index = parent;
		}
		this.pushPendingMaybeSyncResponseFitWaiter(waiter);
	}

	private removePendingMaybeSyncResponseWaiter(
		waiter: PendingMaybeSyncResponseWaiter,
	): void {
		this.removePendingMaybeSyncResponseFitWaiter(waiter);
		const index = waiter.heapIndex;
		if (
			index < 0 ||
			index >= this.pendingMaybeSyncResponseWaiterHeap.length ||
			this.pendingMaybeSyncResponseWaiterHeap[index] !== waiter
		) {
			return;
		}
		const last = this.pendingMaybeSyncResponseWaiterHeap.pop()!;
		waiter.heapIndex = -1;
		if (index >= this.pendingMaybeSyncResponseWaiterHeap.length) {
			return;
		}
		this.pendingMaybeSyncResponseWaiterHeap[index] = last;
		last.heapIndex = index;
		let current = index;
		while (current > 0) {
			const parent = Math.floor((current - 1) / 2);
			if (
				this.pendingMaybeSyncResponseWaiterBefore(
					this.pendingMaybeSyncResponseWaiterHeap[parent]!,
					this.pendingMaybeSyncResponseWaiterHeap[current]!,
				)
			) {
				break;
			}
			this.swapPendingMaybeSyncResponseWaiters(parent, current);
			current = parent;
		}
		for (;;) {
			const left = current * 2 + 1;
			const right = left + 1;
			let smallest = current;
			if (
				left < this.pendingMaybeSyncResponseWaiterHeap.length &&
				this.pendingMaybeSyncResponseWaiterBefore(
					this.pendingMaybeSyncResponseWaiterHeap[left]!,
					this.pendingMaybeSyncResponseWaiterHeap[smallest]!,
				)
			) {
				smallest = left;
			}
			if (
				right < this.pendingMaybeSyncResponseWaiterHeap.length &&
				this.pendingMaybeSyncResponseWaiterBefore(
					this.pendingMaybeSyncResponseWaiterHeap[right]!,
					this.pendingMaybeSyncResponseWaiterHeap[smallest]!,
				)
			) {
				smallest = right;
			}
			if (smallest === current) {
				break;
			}
			this.swapPendingMaybeSyncResponseWaiters(current, smallest);
			current = smallest;
		}
	}

	private pendingMaybeSyncResponseFitWaiterBefore(
		left: PendingMaybeSyncResponseWaiter,
		right: PendingMaybeSyncResponseWaiter,
	): boolean {
		return (
			left.required < right.required ||
			(left.required === right.required && left.order < right.order)
		);
	}

	private swapPendingMaybeSyncResponseFitWaiters(
		left: number,
		right: number,
	): void {
		const leftWaiter = this.pendingMaybeSyncResponseWaiterFitHeap[left]!;
		const rightWaiter = this.pendingMaybeSyncResponseWaiterFitHeap[right]!;
		this.pendingMaybeSyncResponseWaiterFitHeap[left] = rightWaiter;
		this.pendingMaybeSyncResponseWaiterFitHeap[right] = leftWaiter;
		rightWaiter.fitHeapIndex = left;
		leftWaiter.fitHeapIndex = right;
	}

	private pushPendingMaybeSyncResponseFitWaiter(
		waiter: PendingMaybeSyncResponseWaiter,
	): void {
		waiter.fitHeapIndex = this.pendingMaybeSyncResponseWaiterFitHeap.length;
		this.pendingMaybeSyncResponseWaiterFitHeap.push(waiter);
		let index = waiter.fitHeapIndex;
		while (index > 0) {
			const parent = Math.floor((index - 1) / 2);
			if (
				this.pendingMaybeSyncResponseFitWaiterBefore(
					this.pendingMaybeSyncResponseWaiterFitHeap[parent]!,
					this.pendingMaybeSyncResponseWaiterFitHeap[index]!,
				)
			) {
				break;
			}
			this.swapPendingMaybeSyncResponseFitWaiters(parent, index);
			index = parent;
		}
	}

	private removePendingMaybeSyncResponseFitWaiter(
		waiter: PendingMaybeSyncResponseWaiter,
	): void {
		const index = waiter.fitHeapIndex;
		if (
			index < 0 ||
			index >= this.pendingMaybeSyncResponseWaiterFitHeap.length ||
			this.pendingMaybeSyncResponseWaiterFitHeap[index] !== waiter
		) {
			return;
		}
		const last = this.pendingMaybeSyncResponseWaiterFitHeap.pop()!;
		waiter.fitHeapIndex = -1;
		if (index >= this.pendingMaybeSyncResponseWaiterFitHeap.length) {
			return;
		}
		this.pendingMaybeSyncResponseWaiterFitHeap[index] = last;
		last.fitHeapIndex = index;
		let current = index;
		while (current > 0) {
			const parent = Math.floor((current - 1) / 2);
			if (
				this.pendingMaybeSyncResponseFitWaiterBefore(
					this.pendingMaybeSyncResponseWaiterFitHeap[parent]!,
					this.pendingMaybeSyncResponseWaiterFitHeap[current]!,
				)
			) {
				break;
			}
			this.swapPendingMaybeSyncResponseFitWaiters(parent, current);
			current = parent;
		}
		for (;;) {
			const left = current * 2 + 1;
			const right = left + 1;
			let smallest = current;
			if (
				left < this.pendingMaybeSyncResponseWaiterFitHeap.length &&
				this.pendingMaybeSyncResponseFitWaiterBefore(
					this.pendingMaybeSyncResponseWaiterFitHeap[left]!,
					this.pendingMaybeSyncResponseWaiterFitHeap[smallest]!,
				)
			) {
				smallest = left;
			}
			if (
				right < this.pendingMaybeSyncResponseWaiterFitHeap.length &&
				this.pendingMaybeSyncResponseFitWaiterBefore(
					this.pendingMaybeSyncResponseWaiterFitHeap[right]!,
					this.pendingMaybeSyncResponseWaiterFitHeap[smallest]!,
				)
			) {
				smallest = right;
			}
			if (smallest === current) {
				break;
			}
			this.swapPendingMaybeSyncResponseFitWaiters(current, smallest);
			current = smallest;
		}
	}

	private notifyPendingMaybeSyncResponseWaiter(): void {
		const waiter = this.pendingMaybeSyncResponseWaiterHeap[0];
		const available =
			MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES -
			this.pendingMaybeSyncResponseCount;
		if (!waiter) {
			return;
		}
		if (waiter.required <= available) {
			waiter.wake();
			return;
		}
		if (waiter.bypasses >= MAX_PENDING_MAYBE_SYNC_RESPONSE_WAITER_BYPASSES) {
			// Reserve newly freed capacity for the oldest large request after a
			// bounded number of smaller requests have bypassed it.
			return;
		}
		const candidate = this.pendingMaybeSyncResponseWaiterFitHeap[0];
		if (candidate && candidate.required <= available) {
			waiter.bypasses += 1;
			candidate.wake();
		}
	}

	private schedulePendingMaybeSyncResponseWaiter(): void {
		if (this.pendingMaybeSyncResponseWakeScheduled) {
			return;
		}
		this.pendingMaybeSyncResponseWakeScheduled = true;
		queueMicrotask(() => {
			this.pendingMaybeSyncResponseWakeScheduled = false;
			this.notifyPendingMaybeSyncResponseWaiter();
		});
	}

	private swapPendingMaybeSyncResponseExpiry(
		left: number,
		right: number,
	): void {
		const leftBatch = this.pendingMaybeSyncResponseExpiryHeap[left]!;
		const rightBatch = this.pendingMaybeSyncResponseExpiryHeap[right]!;
		this.pendingMaybeSyncResponseExpiryHeap[left] = rightBatch;
		this.pendingMaybeSyncResponseExpiryHeap[right] = leftBatch;
		rightBatch.heapIndex = left;
		leftBatch.heapIndex = right;
	}

	private pushPendingMaybeSyncResponseExpiry(
		batch: PendingMaybeSyncResponse,
	): void {
		batch.heapIndex = this.pendingMaybeSyncResponseExpiryHeap.length;
		this.pendingMaybeSyncResponseExpiryHeap.push(batch);
		let index = batch.heapIndex;
		while (index > 0) {
			const parent = Math.floor((index - 1) / 2);
			if (
				this.pendingMaybeSyncResponseExpiryHeap[parent]!.expiresAt <=
				this.pendingMaybeSyncResponseExpiryHeap[index]!.expiresAt
			) {
				break;
			}
			this.swapPendingMaybeSyncResponseExpiry(parent, index);
			index = parent;
		}
	}

	private removePendingMaybeSyncResponseExpiry(
		batch: PendingMaybeSyncResponse,
	): void {
		const index = batch.heapIndex;
		if (
			index < 0 ||
			index >= this.pendingMaybeSyncResponseExpiryHeap.length ||
			this.pendingMaybeSyncResponseExpiryHeap[index] !== batch
		) {
			return;
		}
		const last = this.pendingMaybeSyncResponseExpiryHeap.pop()!;
		batch.heapIndex = -1;
		if (index >= this.pendingMaybeSyncResponseExpiryHeap.length) {
			return;
		}
		this.pendingMaybeSyncResponseExpiryHeap[index] = last;
		last.heapIndex = index;
		let current = index;
		while (current > 0) {
			const parent = Math.floor((current - 1) / 2);
			if (
				this.pendingMaybeSyncResponseExpiryHeap[parent]!.expiresAt <=
				this.pendingMaybeSyncResponseExpiryHeap[current]!.expiresAt
			) {
				break;
			}
			this.swapPendingMaybeSyncResponseExpiry(parent, current);
			current = parent;
		}
		for (;;) {
			const left = current * 2 + 1;
			const right = left + 1;
			let smallest = current;
			if (
				left < this.pendingMaybeSyncResponseExpiryHeap.length &&
				this.pendingMaybeSyncResponseExpiryHeap[left]!.expiresAt <
					this.pendingMaybeSyncResponseExpiryHeap[smallest]!.expiresAt
			) {
				smallest = left;
			}
			if (
				right < this.pendingMaybeSyncResponseExpiryHeap.length &&
				this.pendingMaybeSyncResponseExpiryHeap[right]!.expiresAt <
					this.pendingMaybeSyncResponseExpiryHeap[smallest]!.expiresAt
			) {
				smallest = right;
			}
			if (smallest === current) {
				break;
			}
			this.swapPendingMaybeSyncResponseExpiry(current, smallest);
			current = smallest;
		}
	}

	private schedulePendingMaybeSyncResponseExpiry(): void {
		if (
			this.pendingMaybeSyncResponseExpiryTimer ||
			this.pendingMaybeSyncResponseExpiryHeap.length === 0
		) {
			return;
		}
		const earliest = this.pendingMaybeSyncResponseExpiryHeap[0]!.expiresAt;
		this.pendingMaybeSyncResponseExpiryTimer = setTimeout(
			() => {
				this.pendingMaybeSyncResponseExpiryTimer = undefined;
				this.expirePendingMaybeSyncResponses();
				this.schedulePendingMaybeSyncResponseExpiry();
			},
			Math.max(0, earliest - Date.now()),
		);
		this.pendingMaybeSyncResponseExpiryTimer.unref?.();
	}

	private expirePendingMaybeSyncResponses(now = Date.now()): void {
		for (;;) {
			const batch = this.pendingMaybeSyncResponseExpiryHeap[0];
			if (!batch || batch.expiresAt > now) {
				break;
			}
			this.removePendingMaybeSyncResponseBatch(batch);
		}
	}

	private settlePendingMaybeSyncResponseAuthorization(
		authorization: PendingMaybeSyncResponseAuthorization,
		fulfilled: boolean,
	): void {
		if (authorization.settled) {
			return;
		}
		authorization.settled = fulfilled ? "fulfilled" : "released";
		const waiters = [...authorization.waiters];
		authorization.waiters.clear();
		for (const waiter of waiters) {
			waiter(fulfilled ? "fulfilled" : "released");
		}
	}

	private doesPendingMaybeSyncResponseScopeMatch(
		authorization: PendingMaybeSyncResponseAuthorization,
		lifecycle: SyncDispatchLifecycle,
		target: string,
	): boolean {
		const owner = authorization.batch.targetLifecycle;
		return (
			owner.epoch === lifecycle.targets.get(target)?.epoch &&
			owner.lifecycle.ownershipLifecycleController ===
				lifecycle.ownershipLifecycleController &&
			owner.lifecycle.callerSignal === lifecycle.callerSignal
		);
	}

	private waitForPendingMaybeSyncResponseConflicts(
		conflicts: PendingMaybeSyncResponseAuthorization[],
		lifecycle: SyncDispatchLifecycle,
		target: string,
		retainedAssociations: number,
	): Promise<string[]> {
		const unique = [...new Set(conflicts)];
		if (
			this.pendingMaybeSyncResponseWaiterAssociationCount +
				retainedAssociations >
			MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES
		) {
			return Promise.resolve([]);
		}
		const available =
			MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES -
			this.pendingMaybeSyncResponseConflictWaiterCount;
		const admitted = unique.slice(0, Math.max(0, available));
		if (admitted.length === 0) {
			return Promise.resolve([]);
		}
		this.pendingMaybeSyncResponseConflictWaiterCount += admitted.length;
		this.pendingMaybeSyncResponseWaiterAssociationCount += retainedAssociations;
		const targetSignal =
			lifecycle.targets.get(target)?.controller.signal ??
			lifecycle.controller.signal;
		return new Promise<string[]>((resolve) => {
			const retry = new Set<string>();
			const callbacks = new Map<
				PendingMaybeSyncResponseAuthorization,
				(event: PendingMaybeSyncResponseAuthorizationEvent) => void
			>();
			let remaining = admitted.length;
			let groupSettled = false;
			const finishGroup = () => {
				if (groupSettled || remaining !== 0) {
					return;
				}
				groupSettled = true;
				lifecycle.controller.signal.removeEventListener("abort", abort);
				if (targetSignal !== lifecycle.controller.signal) {
					targetSignal.removeEventListener("abort", abort);
				}
				this.pendingMaybeSyncResponseWaiterAssociationCount -=
					retainedAssociations;
				resolve([...retry]);
			};
			const finishOne = (
				authorization: PendingMaybeSyncResponseAuthorization,
				event: PendingMaybeSyncResponseAuthorizationEvent,
			) => {
				if (
					event === "delivered" &&
					(authorization.active === true ||
						!this.doesPendingMaybeSyncResponseScopeMatch(
							authorization,
							lifecycle,
							target,
						))
				) {
					return;
				}
				const callback = callbacks.get(authorization);
				if (!callback) {
					return;
				}
				callbacks.delete(authorization);
				authorization.waiters.delete(callback);
				this.pendingMaybeSyncResponseConflictWaiterCount -= 1;
				if (
					event === "released" &&
					this.isSyncDispatchLifecycleActive(lifecycle, target)
				) {
					retry.add(authorization.hash);
				}
				remaining -= 1;
				finishGroup();
			};
			const abort = () => {
				for (const authorization of [...callbacks.keys()]) {
					finishOne(authorization, "fulfilled");
				}
			};
			lifecycle.controller.signal.addEventListener("abort", abort, {
				once: true,
			});
			if (targetSignal !== lifecycle.controller.signal) {
				targetSignal.addEventListener("abort", abort, { once: true });
			}
			for (const authorization of admitted) {
				const callback = (event: PendingMaybeSyncResponseAuthorizationEvent) =>
					finishOne(authorization, event);
				callbacks.set(authorization, callback);
				authorization.waiters.add(callback);
				if (authorization.settled) {
					callback(authorization.settled);
				} else if (
					authorization.requestDelivered === true &&
					authorization.active !== true
				) {
					callback("delivered");
				}
			}
			if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
				abort();
			}
		});
	}

	private removePendingMaybeSyncResponseBatch(
		batch: PendingMaybeSyncResponse,
	): void {
		const pendingForTarget = this.pendingMaybeSyncResponses.get(batch.target);
		let removed = 0;
		if (pendingForTarget) {
			for (const hash of batch.hashes) {
				const authorization = pendingForTarget.get(hash);
				if (authorization?.batch !== batch) {
					continue;
				}
				this.settlePendingMaybeSyncResponseAuthorization(authorization, false);
				pendingForTarget.delete(hash);
				if (authorization.deliveryInFlight !== true) {
					removed += 1;
				}
			}
			if (pendingForTarget.size === 0) {
				this.pendingMaybeSyncResponses.delete(batch.target);
			}
		}
		this.pendingMaybeSyncResponseCount -= removed;
		this.pendingMaybeSyncResponseBatches.delete(batch);
		this.removePendingMaybeSyncResponseExpiry(batch);
		if (batch.targetLifecycle.batches.delete(batch)) {
			batch.targetLifecycle.lifecycle.retainedWork -= 1;
		}
		batch.hashes.clear();
		if (
			this.pendingMaybeSyncResponseExpiryHeap.length === 0 &&
			this.pendingMaybeSyncResponseExpiryTimer
		) {
			clearTimeout(this.pendingMaybeSyncResponseExpiryTimer);
			this.pendingMaybeSyncResponseExpiryTimer = undefined;
		}
		if (removed > 0) {
			this.schedulePendingMaybeSyncResponseWaiter();
		}
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
	}

	private tryReservePendingMaybeSyncResponse(properties: {
		hashes: Iterable<string>;
		targets: string[];
		lifecycle: SyncDispatchLifecycle;
	}): PendingMaybeSyncResponseReservationAttempt {
		// Timers are only a cleanup aid. Enforce absolute deadlines at the
		// admission boundary as well, including for unrelated fresh hashes.
		this.expirePendingMaybeSyncResponses();
		const hashes = [...new Set(properties.hashes)];
		const targets = [...new Set(properties.targets)];
		if (
			!this.isSyncDispatchLifecycleActive(properties.lifecycle) ||
			targets.some(
				(target) =>
					!this.isSyncDispatchLifecycleActive(properties.lifecycle, target),
			)
		) {
			return { kind: "inactive" };
		}

		const hashesToAddByTarget = new Map<string, string[]>();
		const conflicts: PendingMaybeSyncResponseAuthorization[] = [];
		let required = 0;
		for (const target of targets) {
			const hashesToAdd: string[] = [];
			for (const hash of hashes) {
				let existing = this.pendingMaybeSyncResponses.get(target)?.get(hash);
				if (
					existing &&
					existing.active !== true &&
					(existing.batch.expiresAt <= Date.now() ||
						!this.isSyncDispatchLifecycleActive(
							existing.batch.targetLifecycle.lifecycle,
							target,
						))
				) {
					this.removePendingMaybeSyncResponseBatch(existing.batch);
					existing = undefined;
				}
				if (existing) {
					const existingTarget = existing.batch.targetLifecycle;
					if (
						existingTarget.lifecycle === properties.lifecycle ||
						(existing.active !== true &&
							existing.requestDelivered === true &&
							this.doesPendingMaybeSyncResponseScopeMatch(
								existing,
								properties.lifecycle,
								target,
							))
					) {
						continue;
					}
					// Another live caller already owns the authorization for this
					// exact target/hash. Send unrelated hashes now, then wait for this
					// authorization to be fulfilled or released before deciding
					// whether this caller must retry it.
					conflicts.push(existing);
					continue;
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
			return { kind: "capacity", required };
		}

		const addedBatches: PendingMaybeSyncResponse[] = [];
		const addedAuthorizations: PendingMaybeSyncResponseAuthorization[] = [];
		for (const [target, hashesToAdd] of hashesToAddByTarget) {
			const targetLifecycle = properties.lifecycle.targets.get(target)!;
			const batch: PendingMaybeSyncResponse = {
				hashes: new Set(hashesToAdd),
				target,
				targetLifecycle,
				// Start the response deadline only after rpc.send succeeds. Keeping
				// pre-delivery work charged prevents a slow/non-abortable transport
				// from rolling over an unbounded number of sends.
				expiresAt: Infinity,
				heapIndex: -1,
			};
			let pendingForTarget = this.pendingMaybeSyncResponses.get(target);
			if (!pendingForTarget) {
				pendingForTarget = new Map();
				this.pendingMaybeSyncResponses.set(target, pendingForTarget);
			}
			for (const hash of hashesToAdd) {
				const authorization: PendingMaybeSyncResponseAuthorization = {
					batch,
					hash,
					waiters: new Set(),
				};
				pendingForTarget.set(hash, authorization);
				addedAuthorizations.push(authorization);
			}
			this.pendingMaybeSyncResponseCount += hashesToAdd.length;
			this.pendingMaybeSyncResponseBatches.add(batch);
			targetLifecycle.batches.add(batch);
			targetLifecycle.lifecycle.retainedWork += 1;
			addedBatches.push(batch);
		}
		this.schedulePendingMaybeSyncResponseExpiry();
		if (this.pendingMaybeSyncResponseWaiters.size > 0) {
			this.schedulePendingMaybeSyncResponseWaiter();
		}

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
					batch.expiresAt > Date.now() &&
					batch.hashes.size > 0,
			);
		if (!this.isSyncDispatchLifecycleActive(properties.lifecycle)) {
			release();
			return { kind: "inactive" };
		}
		return {
			kind: "reserved",
			reservation: {
				release,
				beginDelivery: () => {
					for (const authorization of addedAuthorizations) {
						if (!authorization.settled) {
							authorization.deliveryInFlight = true;
						}
					}
				},
				finishDelivery: () => {
					let releasedCount = 0;
					for (const authorization of addedAuthorizations) {
						if (authorization.deliveryInFlight !== true) {
							continue;
						}
						authorization.deliveryInFlight = false;
						if (
							authorization.settled ||
							this.pendingMaybeSyncResponses
								.get(authorization.batch.target)
								?.get(authorization.hash) !== authorization
						) {
							releasedCount += 1;
						}
					}
					if (releasedCount > 0) {
						this.pendingMaybeSyncResponseCount -= releasedCount;
						this.schedulePendingMaybeSyncResponseWaiter();
					}
				},
				markDelivered: () => {
					const delivered: PendingMaybeSyncResponseAuthorization[] = [];
					const deliveredBatches = new Set<PendingMaybeSyncResponse>();
					for (const authorization of addedAuthorizations) {
						if (
							authorization.settled ||
							authorization.active === true ||
							this.pendingMaybeSyncResponses
								.get(authorization.batch.target)
								?.get(authorization.hash) !== authorization ||
							!this.pendingMaybeSyncResponseBatches.has(authorization.batch)
						) {
							continue;
						}
						authorization.requestDelivered = true;
						delivered.push(authorization);
						deliveredBatches.add(authorization.batch);
					}
					const expiresAt = Date.now() + PENDING_MAYBE_SYNC_RESPONSE_TTL_MS;
					for (const batch of deliveredBatches) {
						if (batch.heapIndex >= 0) {
							this.removePendingMaybeSyncResponseExpiry(batch);
						}
						batch.expiresAt = expiresAt;
						this.pushPendingMaybeSyncResponseExpiry(batch);
					}
					this.schedulePendingMaybeSyncResponseExpiry();
					for (const authorization of delivered) {
						if (authorization.settled || authorization.active === true) {
							continue;
						}
						for (const waiter of [...authorization.waiters]) {
							waiter("delivered");
						}
					}
				},
				newlyAuthorizedByTarget: hashesToAddByTarget,
				retained,
				signal: properties.lifecycle.controller.signal,
			},
			conflicts,
		};
	}

	private waitForPendingMaybeSyncResponseChange(
		lifecycle: SyncDispatchLifecycle,
		targets: string[],
		required: number,
		associations: number,
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
			targetLifecycle.lifecycle.retainedWork += 1;
		}
		return new Promise<void>((resolve) => {
			let settled = false;
			let waiter!: PendingMaybeSyncResponseWaiter;
			const abortSignals = [
				lifecycle.controller.signal,
				...targetLifecycles.map(
					(targetLifecycle) => targetLifecycle.controller.signal,
				),
			];
			const wake = () => {
				if (settled) {
					return;
				}
				const advanceWaiters =
					!this.isSyncDispatchLifecycleActive(lifecycle) ||
					targets.some(
						(target) => !this.isSyncDispatchLifecycleActive(lifecycle, target),
					);
				settled = true;
				this.pendingMaybeSyncResponseWaiters.delete(waiter);
				this.removePendingMaybeSyncResponseWaiter(waiter);
				this.pendingMaybeSyncResponseWaiterAssociationCount -=
					waiter.associations;
				for (const signal of abortSignals) {
					signal.removeEventListener("abort", wake);
				}
				for (const targetLifecycle of targetLifecycles) {
					targetLifecycle.activeWaiters -= 1;
					targetLifecycle.lifecycle.retainedWork -= 1;
				}
				resolve();
				this.maybeDisposeSyncDispatchLifecycle(lifecycle);
				if (advanceWaiters) {
					this.schedulePendingMaybeSyncResponseWaiter();
				}
			};
			waiter = {
				required,
				associations,
				order: ++this.pendingMaybeSyncResponseWaiterOrder,
				bypasses: 0,
				heapIndex: -1,
				fitHeapIndex: -1,
				wake,
			};
			this.pendingMaybeSyncResponseWaiters.add(waiter);
			this.pendingMaybeSyncResponseWaiterAssociationCount += associations;
			this.pushPendingMaybeSyncResponseWaiter(waiter);
			// This exact request does not fit, but an older/stale requirement may
			// have changed and another smaller waiter can still use the capacity.
			this.schedulePendingMaybeSyncResponseWaiter();
			for (const signal of abortSignals) {
				signal.addEventListener("abort", wake, { once: true });
			}
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
	}): Promise<
		| {
				reservation: PendingMaybeSyncResponseReservation;
				conflicts: PendingMaybeSyncResponseAuthorization[];
		  }
		| undefined
	> {
		const hashes = [...new Set(properties.hashes)];
		const targets = [...new Set(properties.targets)];
		const associations = Math.max(
			1,
			Math.min(MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES, hashes.length) *
				Math.min(MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES, targets.length),
		);
		while (
			this.isSyncDispatchLifecycleActive(properties.lifecycle) &&
			targets.every((target) =>
				this.isSyncDispatchLifecycleActive(properties.lifecycle, target),
			)
		) {
			const attempt = this.tryReservePendingMaybeSyncResponse({
				hashes,
				targets,
				lifecycle: properties.lifecycle,
			});
			if (attempt.kind === "reserved") {
				return {
					reservation: attempt.reservation,
					conflicts: attempt.conflicts,
				};
			}
			if (attempt.kind !== "capacity") {
				this.schedulePendingMaybeSyncResponseWaiter();
				return undefined;
			}
			if (
				this.pendingMaybeSyncResponseWaiters.size >=
					MAX_PENDING_MAYBE_SYNC_RESPONSE_WAITERS ||
				this.pendingMaybeSyncResponseWaiterAssociationCount + associations >
					MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES
			) {
				return undefined;
			}
			await this.waitForPendingMaybeSyncResponseChange(
				properties.lifecycle,
				targets,
				attempt.required,
				associations,
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
		const attempt = this.tryReservePendingMaybeSyncResponse({
			hashes: properties.hashes,
			targets,
			lifecycle,
		});
		const reservation =
			attempt.kind === "reserved" ? attempt.reservation : undefined;
		let retainedReservation: PendingMaybeSyncResponseReservation | undefined;
		if (reservation) {
			// This synchronous helper represents an already-issued request.
			reservation.markDelivered();
			const leasedTargets = [...lifecycle.targets.values()];
			for (const targetLifecycle of leasedTargets) {
				targetLifecycle.responseLeases += 1;
				targetLifecycle.lifecycle.retainedWork += 1;
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
						targetLifecycle.lifecycle.retainedWork -= 1;
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
		if (
			this.activeMaybeSyncResponseCount >=
				MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_GLOBAL ||
			(this.activeMaybeSyncResponseCountByPeer.get(fromHash) ?? 0) >=
				MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER
		) {
			return [];
		}
		const acceptedByLifecycle = new Map<
			SyncDispatchTargetLifecycle,
			{
				hashes: string[];
				authorizations: PendingMaybeSyncResponseAuthorization[];
			}
		>();
		const seen = new Set<string>();
		let inspected = 0;
		const iterator = hashes[Symbol.iterator]();
		let exhausted = false;
		try {
			while (
				inspected < MAX_PENDING_MAYBE_SYNC_RESPONSE_HASHES &&
				pendingForTarget.size > 0
			) {
				const next = iterator.next();
				if (next.done) {
					exhausted = true;
					break;
				}
				inspected += 1;
				const hash = next.value;
				if (seen.has(hash)) {
					continue;
				}
				seen.add(hash);
				const authorization = pendingForTarget.get(hash);
				if (!authorization || authorization.active === true) {
					continue;
				}
				const batch = authorization.batch;
				const targetLifecycle = batch.targetLifecycle;
				if (
					batch.expiresAt <= Date.now() ||
					!this.isSyncDispatchLifecycleActive(
						targetLifecycle.lifecycle,
						fromHash,
					)
				) {
					this.removePendingMaybeSyncResponseBatch(batch);
					continue;
				}
				if (pendingForTarget.get(hash)?.batch !== batch) {
					continue;
				}
				let accepted = acceptedByLifecycle.get(targetLifecycle);
				if (!accepted) {
					accepted = { hashes: [], authorizations: [] };
					acceptedByLifecycle.set(targetLifecycle, accepted);
					targetLifecycle.responseLeases += 1;
					targetLifecycle.lifecycle.retainedWork += 1;
				}
				authorization.active = true;
				batch.hashes.delete(hash);
				accepted.hashes.push(hash);
				accepted.authorizations.push(authorization);
				if (batch.hashes.size === 0) {
					this.removePendingMaybeSyncResponseBatch(batch);
				}
			}
		} finally {
			if (!exhausted) {
				iterator.return?.();
			}
		}
		if (pendingForTarget.size === 0) {
			this.pendingMaybeSyncResponses.delete(fromHash);
		}
		if (acceptedByLifecycle.size === 0) {
			return [];
		}
		this.activeMaybeSyncResponseCount += 1;
		this.activeMaybeSyncResponseCountByPeer.set(
			fromHash,
			(this.activeMaybeSyncResponseCountByPeer.get(fromHash) ?? 0) + 1,
		);
		let remainingLeases = acceptedByLifecycle.size;
		return [...acceptedByLifecycle].map(
			([targetLifecycle, { hashes: acceptedHashes, authorizations }]) => {
				let released = false;
				return {
					hashes: acceptedHashes,
					signal: targetLifecycle.controller.signal,
					release: (options?: { fulfilled?: boolean }) => {
						if (released) {
							return;
						}
						released = true;
						const pendingForTarget =
							this.pendingMaybeSyncResponses.get(fromHash);
						for (const authorization of authorizations) {
							this.settlePendingMaybeSyncResponseAuthorization(
								authorization,
								options?.fulfilled === true,
							);
							if (pendingForTarget?.get(authorization.hash) === authorization) {
								pendingForTarget.delete(authorization.hash);
							}
						}
						if (pendingForTarget?.size === 0) {
							this.pendingMaybeSyncResponses.delete(fromHash);
						}
						this.pendingMaybeSyncResponseCount -= authorizations.filter(
							(authorization) => authorization.deliveryInFlight !== true,
						).length;
						targetLifecycle.responseLeases -= 1;
						targetLifecycle.lifecycle.retainedWork -= 1;
						remainingLeases -= 1;
						if (remainingLeases === 0) {
							this.activeMaybeSyncResponseCount -= 1;
							const activeForPeer =
								(this.activeMaybeSyncResponseCountByPeer.get(fromHash) ?? 1) -
								1;
							if (activeForPeer === 0) {
								this.activeMaybeSyncResponseCountByPeer.delete(fromHash);
							} else {
								this.activeMaybeSyncResponseCountByPeer.set(
									fromHash,
									activeForPeer,
								);
							}
						}
						this.schedulePendingMaybeSyncResponseWaiter();
						this.maybeDisposeSyncDispatchLifecycle(targetLifecycle.lifecycle);
					},
				};
			},
		);
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
					let hashesToAuthorize = chunk;
					while (
						hashesToAuthorize.length > 0 &&
						this.isSyncDispatchLifecycleActive(lifecycle, target)
					) {
						const reserved = await this.reservePendingMaybeSyncResponse({
							hashes: hashesToAuthorize,
							targets: [target],
							lifecycle,
						});
						if (
							!reserved ||
							!this.isSyncDispatchLifecycleActive(lifecycle, target)
						) {
							break;
						}
						const { reservation, conflicts } = reserved;
						const hashesToSend =
							reservation.newlyAuthorizedByTarget.get(target) ?? [];
						if (!reservation.retained()) {
							reservation.release();
							break;
						}
						if (hashesToSend.length > 0) {
							reservation.beginDelivery();
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
								reservation.markDelivered();
								messages += 1;
							} catch (error) {
								reservation.release();
								if (
									!this.isSyncDispatchLifecycleActive(lifecycle) ||
									!this.isSyncDispatchLifecycleActive(lifecycle, target)
								) {
									break;
								}
								throw error;
							} finally {
								reservation.finishDelivery();
							}
						}
						hashesToAuthorize =
							await this.waitForPendingMaybeSyncResponseConflicts(
								conflicts,
								lifecycle,
								target,
								hashesToAuthorize.length,
							);
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
			let fulfilled = false;
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
				fulfilled = !lease.signal.aborted;
			} catch (error) {
				firstError ??= error;
			} finally {
				lease.release({ fulfilled });
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
			if (
				msg.hashNumbers.length === 0 ||
				msg.hashNumbers.length > MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS
			) {
				return true;
			}
			const target = from.hashcode();
			const releaseLookup = this.tryAcquireCoordinateLookup(target);
			if (!releaseLookup) {
				return true;
			}
			const lifecycle = this.captureSyncDispatchLifecycle([target]);
			let lookupReleased = false;
			const finishLookup = () => {
				if (lookupReleased) {
					return;
				}
				lookupReleased = true;
				releaseLookup();
			};
			try {
				if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
					return true;
				}
				const symbols = [...new Set(msg.hashNumbers)];
				const profile = this.syncOptions?.profile;
				const lookupStartedAt = syncProfileStart(profile);
				const hashes = await getHashesFromSymbols(
					symbols,
					this.entryIndex,
					this.coordinateToHash,
					this.resolveHashesForSymbols,
					this.resolveHashListForSymbols,
					MAX_SIMPLE_COORDINATE_RESPONSE_HASHES,
				);
				finishLookup();
				if (!this.isSyncDispatchLifecycleActive(lifecycle, target)) {
					return true;
				}
				if (profile) {
					emitSyncProfileDuration(profile, lookupStartedAt, {
						name: "simple.coordinateLookup",
						entries: hashLookupResultSize(hashes),
						symbols: symbols.length,
					});
				}

				const releaseResponse = this.tryAcquireCoordinateResponse(target);
				if (!releaseResponse) {
					return true;
				}
				const exchangeStartedAt = syncProfileStart(profile);
				let hashesToSend: string[] = [];
				let messages = 0;
				let fused = false;
				try {
					hashesToSend = this.filterRecentlySentExchangeHeads(hashes, from);
					// dont set priority 1 here because this will block other messages that should higher priority
					({ messages, fused } = await this.shipExchangeHeads(
						hashesToSend,
						context.from!,
						canReceiveRawExchangeHeads(msg),
						this.getSyncDispatchSignal(lifecycle, target),
					));
				} finally {
					releaseResponse();
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
				finishLookup();
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

	private getPendingSyncKeyIdentity(key: SyncableKey): SyncableKey {
		if (typeof key === "string") {
			return key;
		}
		const hash = this.coordinateToHash.get(key);
		return hash ?? key;
	}

	private removeQueuedSyncCoordinateAlias(key: bigint): void {
		this.syncInFlightQueuedCoordinates.delete(key);
		if (this.syncInFlightQueuedCoordinates.size === 0) {
			this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
		}
		const previousHash = this.syncInFlightQueuedHashByCoordinate.get(key);
		this.syncInFlightQueuedHashByCoordinate.delete(key);
		if (previousHash != null) {
			const coordinates =
				this.syncInFlightQueuedCoordinatesByHash.get(previousHash);
			coordinates?.delete(key);
			if (coordinates?.size === 0) {
				this.syncInFlightQueuedCoordinatesByHash.delete(previousHash);
			}
		}
	}

	private refreshQueuedSyncCoordinateAlias(key: bigint): void {
		if (!this.syncInFlightQueue.has(key)) {
			this.removeQueuedSyncCoordinateAlias(key);
			return;
		}
		this.syncInFlightQueuedCoordinates.add(key);
		const hash = this.coordinateToHash.get(key) ?? undefined;
		const previousHash = this.syncInFlightQueuedHashByCoordinate.get(key);
		if (previousHash !== hash) {
			if (previousHash != null) {
				const coordinates =
					this.syncInFlightQueuedCoordinatesByHash.get(previousHash);
				coordinates?.delete(key);
				if (coordinates?.size === 0) {
					this.syncInFlightQueuedCoordinatesByHash.delete(previousHash);
				}
			}
			if (hash == null) {
				this.syncInFlightQueuedHashByCoordinate.delete(key);
			} else {
				this.syncInFlightQueuedHashByCoordinate.set(key, hash);
			}
		}
		if (hash != null) {
			let coordinates = this.syncInFlightQueuedCoordinatesByHash.get(hash);
			if (!coordinates) {
				coordinates = new Set();
				this.syncInFlightQueuedCoordinatesByHash.set(hash, coordinates);
			}
			coordinates.add(key);
			this.reconcileQueuedSyncCoordinateAlias(key, hash);
		}
	}

	private reconcileQueuedSyncCoordinateAlias(
		coordinate: bigint,
		hash: string,
	): void {
		const now = Date.now();
		const coordinateExpiresAt = this.syncInFlightQueueExpiresAt.get(coordinate);
		if (coordinateExpiresAt != null && coordinateExpiresAt <= now) {
			this.clearSyncProcessKey(coordinate);
			return;
		}
		const hashExpiresAt = this.syncInFlightQueueExpiresAt.get(hash);
		if (hashExpiresAt != null && hashExpiresAt <= now) {
			this.clearSyncProcessKey(hash);
			return;
		}
		const hashClaimants = this.syncInFlightQueue.get(hash);
		if (!hashClaimants || !this.syncInFlightQueue.has(coordinate)) {
			return;
		}
		const expiresAt = Math.min(
			this.syncInFlightQueueExpiresAt.get(coordinate) ?? Infinity,
			this.syncInFlightQueueExpiresAt.get(hash) ?? Infinity,
		);
		for (const claimant of [...hashClaimants]) {
			this.addPendingSyncClaim(coordinate, claimant, expiresAt);
		}
		if (Number.isFinite(expiresAt)) {
			this.movePendingSyncKeyExpiryEarlier(coordinate, expiresAt);
		}
		for (const target of [...(this.syncInFlightTargetsByKey.get(hash) ?? [])]) {
			const state = this.syncInFlight.get(target)?.get(hash);
			if (state) {
				this.setSyncInFlightTargetKey(target, coordinate, state.timestamp);
			}
		}
		this.clearSyncProcessKey(hash);
	}

	private refreshQueuedSyncCoordinateAliases(): void {
		if (
			this.syncInFlightQueuedCoordinates.size === 0 &&
			this.syncInFlightQueueClaimants.size < this.syncInFlightQueue.size
		) {
			// Defensive compatibility for callers/tests that seed the public queue
			// directly. Keep hydration bounded; internal writes register coordinates
			// when the key is first admitted.
			let inspected = 0;
			for (const key of this.syncInFlightQueue.keys()) {
				if (typeof key === "bigint") {
					this.syncInFlightQueuedCoordinates.add(key);
				}
				inspected += 1;
				if (inspected >= MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE) {
					break;
				}
			}
		}
		if (this.syncInFlightQueuedCoordinates.size === 0) {
			this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
			return;
		}
		this.syncInFlightQueuedCoordinateRefreshIterator ??=
			this.syncInFlightQueuedCoordinates.values();
		for (
			let refreshed = 0;
			refreshed < MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE;
			refreshed += 1
		) {
			const next = this.syncInFlightQueuedCoordinateRefreshIterator.next();
			if (next.done) {
				this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
				break;
			}
			this.refreshQueuedSyncCoordinateAlias(next.value);
		}
	}

	private getQueuedSyncKeyForAdmission(
		key: SyncableKey,
	): SyncableKey | typeof QUEUED_SYNC_ALIAS_REFRESH_PENDING | undefined {
		const getValidQueuedKey = (
			candidate: SyncableKey,
		): SyncableKey | undefined => {
			if (!this.syncInFlightQueue.has(candidate)) {
				return undefined;
			}
			const expiresAt = this.syncInFlightQueueExpiresAt.get(candidate);
			if (expiresAt != null && expiresAt <= Date.now()) {
				this.clearSyncProcessKey(candidate);
				return undefined;
			}
			return candidate;
		};
		if (getValidQueuedKey(key) != null) {
			if (typeof key === "bigint") {
				this.refreshQueuedSyncCoordinateAlias(key);
				return getValidQueuedKey(key);
			}
			return key;
		}
		if (typeof key === "string") {
			const aliases = this.syncInFlightQueuedCoordinatesByHash.get(key);
			if (aliases) {
				let inspected = 0;
				for (const alias of aliases) {
					if (inspected >= MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE) {
						return QUEUED_SYNC_ALIAS_REFRESH_PENDING;
					}
					inspected += 1;
					this.refreshQueuedSyncCoordinateAlias(alias);
					if (
						this.syncInFlightQueuedCoordinatesByHash.get(key)?.has(alias) ===
						true
					) {
						const validAlias = getValidQueuedKey(alias);
						if (validAlias != null) {
							return validAlias;
						}
					}
				}
				if (
					(this.syncInFlightQueuedCoordinatesByHash.get(key)?.size ?? 0) > 0
				) {
					return QUEUED_SYNC_ALIAS_REFRESH_PENDING;
				}
			}
			return undefined;
		}
		const hash = this.coordinateToHash.get(key);
		return hash != null ? getValidQueuedKey(hash) : undefined;
	}

	private addPendingSyncClaim(
		key: SyncableKey,
		from: PublicSignKey,
		expiresAt?: number,
	): boolean {
		const fromHash = from.hashcode();
		let peers = this.syncInFlightQueue.get(key);
		let claimants = this.syncInFlightQueueClaimants.get(key);
		let claimantIndexes = this.syncInFlightQueueClaimantIndexes.get(key);
		if (!peers) {
			peers = [];
			this.syncInFlightQueue.set(key, peers);
			claimants = new Set();
			this.syncInFlightQueueClaimants.set(key, claimants);
			claimantIndexes = new Map();
			this.syncInFlightQueueClaimantIndexes.set(key, claimantIndexes);
			const deadline = expiresAt ?? Date.now() + PENDING_SIMPLE_SYNC_KEY_TTL_MS;
			this.syncInFlightQueueExpiresAt.set(key, deadline);
			const expiryNode: PendingSyncExpiryNode = {
				kind: "key",
				key,
				expiresAt: deadline,
				heapIndex: -1,
			};
			this.pendingSyncKeyExpiryNodes.set(key, expiryNode);
			this.pushPendingSyncExpiry(expiryNode);
			if (typeof key === "bigint") {
				this.refreshQueuedSyncCoordinateAlias(key);
			}
		} else if (!claimants) {
			// Defensive compatibility for callers/tests that seed the public queue
			// maps directly. Internally every retained key gets this set at creation.
			claimants = new Set(peers.map((peer) => peer.hashcode()));
			this.syncInFlightQueueClaimants.set(key, claimants);
			this.pendingSyncClaimCount += claimants.size;
		}
		if (!claimantIndexes) {
			claimantIndexes = new Map();
			for (let index = 0; index < peers.length; index += 1) {
				claimantIndexes.set(peers[index]!.hashcode(), index);
			}
			this.syncInFlightQueueClaimantIndexes.set(key, claimantIndexes);
		}
		if (claimants.has(fromHash)) {
			return false;
		}

		claimantIndexes.set(fromHash, peers.length);
		peers.push(from);
		claimants.add(fromHash);
		let inverted = this.syncInFlightQueueInverted.get(fromHash);
		if (!inverted) {
			inverted = new Set();
			this.syncInFlightQueueInverted.set(fromHash, inverted);
		}
		inverted.add(key);
		this.pendingSyncClaimCount += 1;
		this.schedulePendingSyncKeyExpiry();
		return true;
	}

	private hasPendingSyncClaim(key: SyncableKey, peer: string): boolean {
		const claimants = this.syncInFlightQueueClaimants.get(key);
		if (claimants) {
			return claimants.has(peer);
		}
		const peers = this.syncInFlightQueue.get(key);
		if (!peers) {
			return false;
		}
		const hydrated = new Set(peers.map((candidate) => candidate.hashcode()));
		this.syncInFlightQueueClaimants.set(key, hydrated);
		const indexes = new Map<string, number>();
		for (let index = 0; index < peers.length; index += 1) {
			indexes.set(peers[index]!.hashcode(), index);
		}
		this.syncInFlightQueueClaimantIndexes.set(key, indexes);
		this.pendingSyncClaimCount += hydrated.size;
		return hydrated.has(peer);
	}

	private filterDispatchablePendingSyncClaims(
		keys: SyncableKey[],
		peer: string,
		epoch: SyncDispatchTargetEpoch,
	): SyncableKey[] {
		if (this.syncDispatchTargetEpochs.get(peer) !== epoch) {
			return [];
		}
		const now = Date.now();
		const dispatchable: SyncableKey[] = [];
		for (const key of keys) {
			const expiresAt = this.syncInFlightQueueExpiresAt.get(key);
			if (expiresAt != null && expiresAt <= now) {
				this.clearSyncProcessKey(key);
				continue;
			}
			if (
				this.syncInFlightQueue.has(key) &&
				this.hasPendingSyncClaim(key, peer)
			) {
				dispatchable.push(key);
			}
		}
		return dispatchable;
	}

	private canStartPendingSyncLookup(peer: string): boolean {
		return (
			this.pendingSyncAdmissionReservations.size +
				this.pendingCoordinateLookupCount <
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL &&
			(this.pendingSyncAdmissionReservationsByPeer.get(peer)?.size ?? 0) +
				(this.pendingCoordinateLookupCountByPeer.get(peer) ?? 0) <
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER
		);
	}

	private tryAcquireCoordinateLookup(peer: string): (() => void) | undefined {
		if (!this.canStartPendingSyncLookup(peer)) {
			return undefined;
		}
		this.pendingCoordinateLookupCount += 1;
		this.pendingCoordinateLookupCountByPeer.set(
			peer,
			(this.pendingCoordinateLookupCountByPeer.get(peer) ?? 0) + 1,
		);
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			this.pendingCoordinateLookupCount -= 1;
			const remaining =
				(this.pendingCoordinateLookupCountByPeer.get(peer) ?? 1) - 1;
			if (remaining === 0) {
				this.pendingCoordinateLookupCountByPeer.delete(peer);
			} else {
				this.pendingCoordinateLookupCountByPeer.set(peer, remaining);
			}
		};
	}

	private tryAcquireCoordinateResponse(peer: string): (() => void) | undefined {
		if (
			this.pendingCoordinateResponseCount >=
				MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_GLOBAL ||
			(this.pendingCoordinateResponseCountByPeer.get(peer) ?? 0) >=
				MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER
		) {
			return undefined;
		}
		this.pendingCoordinateResponseCount += 1;
		this.pendingCoordinateResponseCountByPeer.set(
			peer,
			(this.pendingCoordinateResponseCountByPeer.get(peer) ?? 0) + 1,
		);
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			this.pendingCoordinateResponseCount -= 1;
			const remaining =
				(this.pendingCoordinateResponseCountByPeer.get(peer) ?? 1) - 1;
			if (remaining === 0) {
				this.pendingCoordinateResponseCountByPeer.delete(peer);
			} else {
				this.pendingCoordinateResponseCountByPeer.set(peer, remaining);
			}
		};
	}

	private reservePendingSyncAdmission(
		peer: string,
		identities: SyncableKey[],
	): PendingSyncAdmissionReservation | undefined {
		const count = identities.length;
		if (count <= 0) {
			return undefined;
		}
		const reservation: PendingSyncAdmissionReservation = {
			peer,
			remaining: count,
			active: true,
			released: false,
			expiresAt: Date.now() + PENDING_SIMPLE_SYNC_KEY_TTL_MS,
			identities: new Set(identities),
			retainedSettled: 0,
		};
		this.pendingSyncAdmissionReservations.add(reservation);
		let peerReservations =
			this.pendingSyncAdmissionReservationsByPeer.get(peer);
		if (!peerReservations) {
			peerReservations = new Set();
			this.pendingSyncAdmissionReservationsByPeer.set(peer, peerReservations);
		}
		peerReservations.add(reservation);
		this.pendingSyncActiveAdmissionReservations += 1;
		this.pendingSyncAdmissionCount += count;
		this.pendingSyncAdmissionCountByPeer.set(
			peer,
			(this.pendingSyncAdmissionCountByPeer.get(peer) ?? 0) + count,
		);
		let reservedIdentities =
			this.pendingSyncAdmissionIdentitiesByPeer.get(peer);
		if (!reservedIdentities) {
			reservedIdentities = new Set();
			this.pendingSyncAdmissionIdentitiesByPeer.set(peer, reservedIdentities);
		}
		for (const identity of identities) {
			reservedIdentities.add(identity);
			let reservationsForIdentity =
				this.pendingSyncAdmissionReservationsByIdentity.get(identity);
			if (!reservationsForIdentity) {
				reservationsForIdentity = new Set();
				this.pendingSyncAdmissionReservationsByIdentity.set(
					identity,
					reservationsForIdentity,
				);
			}
			reservationsForIdentity.add(reservation);
		}
		const expiryNode: PendingSyncExpiryNode = {
			kind: "admission",
			reservation,
			expiresAt: reservation.expiresAt,
			heapIndex: -1,
		};
		this.pendingSyncAdmissionExpiryNodes.set(reservation, expiryNode);
		this.pushPendingSyncExpiry(expiryNode);
		this.schedulePendingSyncKeyExpiry();
		return reservation;
	}

	private removePendingSyncAdmissionIdentity(
		reservation: PendingSyncAdmissionReservation,
		identity: SyncableKey,
		options?: { retainQuota?: boolean },
	): boolean {
		if (!reservation.identities.delete(identity)) {
			return false;
		}
		if (options?.retainQuota === true) {
			reservation.retainedSettled += 1;
		} else {
			reservation.remaining -= 1;
			this.pendingSyncAdmissionCount -= 1;
			const peerCount =
				(this.pendingSyncAdmissionCountByPeer.get(reservation.peer) ?? 0) - 1;
			if (peerCount === 0) {
				this.pendingSyncAdmissionCountByPeer.delete(reservation.peer);
			} else {
				this.pendingSyncAdmissionCountByPeer.set(reservation.peer, peerCount);
			}
		}
		const reservedIdentities = this.pendingSyncAdmissionIdentitiesByPeer.get(
			reservation.peer,
		);
		reservedIdentities?.delete(identity);
		if (reservedIdentities?.size === 0) {
			this.pendingSyncAdmissionIdentitiesByPeer.delete(reservation.peer);
		}
		const reservationsForIdentity =
			this.pendingSyncAdmissionReservationsByIdentity.get(identity);
		reservationsForIdentity?.delete(reservation);
		if (reservationsForIdentity?.size === 0) {
			this.pendingSyncAdmissionReservationsByIdentity.delete(identity);
		}
		return true;
	}

	private clearPendingSyncAdmissionIdentity(identity: SyncableKey): void {
		const reservations =
			this.pendingSyncAdmissionReservationsByIdentity.get(identity);
		if (!reservations) {
			return;
		}
		for (const reservation of [...reservations]) {
			this.removePendingSyncAdmissionIdentity(reservation, identity, {
				retainQuota: true,
			});
		}
	}

	private consumePendingSyncAdmission(
		reservation: PendingSyncAdmissionReservation,
		identity: SyncableKey,
	): "consumed" | "settled" | "invalid" {
		if (!reservation.active || reservation.expiresAt <= Date.now()) {
			if (reservation.expiresAt <= Date.now()) {
				this.invalidatePendingSyncAdmission(reservation);
			}
			return "invalid";
		}
		if (!reservation.identities.has(identity)) {
			return "settled";
		}
		this.removePendingSyncAdmissionIdentity(reservation, identity);
		if (reservation.remaining === 0) {
			this.removePendingSyncAdmissionExpiry(reservation);
			reservation.active = false;
			reservation.released = true;
			this.pendingSyncActiveAdmissionReservations -= 1;
			this.pendingSyncAdmissionReservations.delete(reservation);
			const peerReservations = this.pendingSyncAdmissionReservationsByPeer.get(
				reservation.peer,
			);
			peerReservations?.delete(reservation);
			if (peerReservations?.size === 0) {
				this.pendingSyncAdmissionReservationsByPeer.delete(reservation.peer);
			}
			this.clearPendingSyncExpiryTimerIfIdle();
		}
		return "consumed";
	}

	private transferPendingSyncAdmissionIdentity(
		peer: string,
		identity: SyncableKey,
	): number | undefined {
		const reservations =
			this.pendingSyncAdmissionReservationsByIdentity.get(identity);
		if (!reservations) {
			return undefined;
		}
		for (const reservation of reservations) {
			if (
				reservation.peer !== peer ||
				reservation.released ||
				reservation.expiresAt <= Date.now() ||
				!this.removePendingSyncAdmissionIdentity(reservation, identity, {
					retainQuota: true,
				})
			) {
				continue;
			}
			// The original resolver may be non-abortable and still retains its input
			// arrays. Keep that reservation charged until its queueSync finally
			// settles; the queued claim is counted separately and can disappear
			// without returning the resolver's quota early.
			return reservation.expiresAt;
		}
		return undefined;
	}

	private invalidatePendingSyncAdmission(
		reservation?: PendingSyncAdmissionReservation,
	): void {
		if (!reservation || reservation.released || !reservation.active) {
			return;
		}
		// Expiry/disconnect invalidates late lookup results, but it must not return
		// the quota slot while the underlying storage/index work is still alive.
		// Those lookups are not generally abortable; only queueSync's finally block
		// may release their active-work accounting.
		this.removePendingSyncAdmissionExpiry(reservation);
		reservation.active = false;
		this.pendingSyncActiveAdmissionReservations -= 1;
		this.clearPendingSyncExpiryTimerIfIdle();
	}

	private releasePendingSyncAdmission(
		reservation?: PendingSyncAdmissionReservation,
	): void {
		if (!reservation || reservation.released) {
			return;
		}
		this.removePendingSyncAdmissionExpiry(reservation);
		for (const identity of [...reservation.identities]) {
			this.removePendingSyncAdmissionIdentity(reservation, identity);
		}
		if (reservation.retainedSettled > 0) {
			const retainedSettled = reservation.retainedSettled;
			reservation.retainedSettled = 0;
			reservation.remaining -= retainedSettled;
			this.pendingSyncAdmissionCount -= retainedSettled;
			const peerCount =
				(this.pendingSyncAdmissionCountByPeer.get(reservation.peer) ?? 0) -
				retainedSettled;
			if (peerCount === 0) {
				this.pendingSyncAdmissionCountByPeer.delete(reservation.peer);
			} else {
				this.pendingSyncAdmissionCountByPeer.set(reservation.peer, peerCount);
			}
		}
		if (reservation.active) {
			this.pendingSyncActiveAdmissionReservations -= 1;
		}
		reservation.active = false;
		reservation.released = true;
		this.pendingSyncAdmissionReservations.delete(reservation);
		const peerReservations = this.pendingSyncAdmissionReservationsByPeer.get(
			reservation.peer,
		);
		peerReservations?.delete(reservation);
		if (peerReservations?.size === 0) {
			this.pendingSyncAdmissionReservationsByPeer.delete(reservation.peer);
		}
		this.clearPendingSyncExpiryTimerIfIdle();
	}

	private clearPendingSyncAdmissions(peer?: string): void {
		const reservations =
			peer == null
				? this.pendingSyncAdmissionReservations
				: this.pendingSyncAdmissionReservationsByPeer.get(peer);
		if (!reservations) {
			return;
		}
		for (const reservation of [...reservations]) {
			this.invalidatePendingSyncAdmission(reservation);
		}
	}

	private swapPendingSyncExpiry(left: number, right: number): void {
		const leftNode = this.pendingSyncExpiryHeap[left]!;
		const rightNode = this.pendingSyncExpiryHeap[right]!;
		this.pendingSyncExpiryHeap[left] = rightNode;
		this.pendingSyncExpiryHeap[right] = leftNode;
		rightNode.heapIndex = left;
		leftNode.heapIndex = right;
	}

	private pushPendingSyncExpiry(node: PendingSyncExpiryNode): void {
		node.heapIndex = this.pendingSyncExpiryHeap.length;
		this.pendingSyncExpiryHeap.push(node);
		let index = node.heapIndex;
		while (index > 0) {
			const parent = Math.floor((index - 1) / 2);
			if (
				this.pendingSyncExpiryHeap[parent]!.expiresAt <=
				this.pendingSyncExpiryHeap[index]!.expiresAt
			) {
				break;
			}
			this.swapPendingSyncExpiry(parent, index);
			index = parent;
		}
	}

	private removePendingSyncExpiry(node: PendingSyncExpiryNode): void {
		const index = node.heapIndex;
		if (
			index < 0 ||
			index >= this.pendingSyncExpiryHeap.length ||
			this.pendingSyncExpiryHeap[index] !== node
		) {
			return;
		}
		const last = this.pendingSyncExpiryHeap.pop()!;
		node.heapIndex = -1;
		if (index >= this.pendingSyncExpiryHeap.length) {
			return;
		}
		this.pendingSyncExpiryHeap[index] = last;
		last.heapIndex = index;

		let current = index;
		while (current > 0) {
			const parent = Math.floor((current - 1) / 2);
			if (
				this.pendingSyncExpiryHeap[parent]!.expiresAt <=
				this.pendingSyncExpiryHeap[current]!.expiresAt
			) {
				break;
			}
			this.swapPendingSyncExpiry(parent, current);
			current = parent;
		}
		for (;;) {
			const left = current * 2 + 1;
			const right = left + 1;
			let smallest = current;
			if (
				left < this.pendingSyncExpiryHeap.length &&
				this.pendingSyncExpiryHeap[left]!.expiresAt <
					this.pendingSyncExpiryHeap[smallest]!.expiresAt
			) {
				smallest = left;
			}
			if (
				right < this.pendingSyncExpiryHeap.length &&
				this.pendingSyncExpiryHeap[right]!.expiresAt <
					this.pendingSyncExpiryHeap[smallest]!.expiresAt
			) {
				smallest = right;
			}
			if (smallest === current) {
				break;
			}
			this.swapPendingSyncExpiry(current, smallest);
			current = smallest;
		}
	}

	private removePendingSyncKeyExpiry(key: SyncableKey): void {
		const node = this.pendingSyncKeyExpiryNodes.get(key);
		if (!node) {
			return;
		}
		this.pendingSyncKeyExpiryNodes.delete(key);
		this.removePendingSyncExpiry(node);
	}

	private movePendingSyncKeyExpiryEarlier(
		key: SyncableKey,
		expiresAt: number,
	): void {
		const current = this.syncInFlightQueueExpiresAt.get(key);
		if (current == null || current <= expiresAt) {
			return;
		}
		this.removePendingSyncKeyExpiry(key);
		this.syncInFlightQueueExpiresAt.set(key, expiresAt);
		const node: PendingSyncExpiryNode = {
			kind: "key",
			key,
			expiresAt,
			heapIndex: -1,
		};
		this.pendingSyncKeyExpiryNodes.set(key, node);
		this.pushPendingSyncExpiry(node);
		this.schedulePendingSyncKeyExpiry();
	}

	private removePendingSyncAdmissionExpiry(
		reservation: PendingSyncAdmissionReservation,
	): void {
		const node = this.pendingSyncAdmissionExpiryNodes.get(reservation);
		if (!node) {
			return;
		}
		this.pendingSyncAdmissionExpiryNodes.delete(reservation);
		this.removePendingSyncExpiry(node);
	}

	private expirePendingSyncKeys(now = Date.now()): void {
		for (;;) {
			const node = this.pendingSyncExpiryHeap[0];
			if (!node || node.expiresAt > now) {
				break;
			}
			this.removePendingSyncExpiry(node);
			if (node.kind === "key") {
				if (this.pendingSyncKeyExpiryNodes.get(node.key) !== node) {
					continue;
				}
				this.pendingSyncKeyExpiryNodes.delete(node.key);
				this.clearSyncProcessKey(node.key);
			} else {
				if (
					this.pendingSyncAdmissionExpiryNodes.get(node.reservation) !== node
				) {
					continue;
				}
				this.pendingSyncAdmissionExpiryNodes.delete(node.reservation);
				this.invalidatePendingSyncAdmission(node.reservation);
			}
		}
	}

	private clearPendingSyncExpiryTimerIfIdle(): void {
		if (
			this.pendingSyncExpiryHeap.length === 0 &&
			this.syncInFlightQueueExpiryTimer != null
		) {
			clearTimeout(this.syncInFlightQueueExpiryTimer);
			this.syncInFlightQueueExpiryTimer = undefined;
		}
	}

	private schedulePendingSyncKeyExpiry(): void {
		if (
			this.syncInFlightQueueExpiryTimer != null ||
			this.pendingSyncExpiryHeap.length === 0
		) {
			return;
		}
		const expiresAt = this.pendingSyncExpiryHeap[0]!.expiresAt;
		this.syncInFlightQueueExpiryTimer = setTimeout(
			() => {
				this.syncInFlightQueueExpiryTimer = undefined;
				this.expirePendingSyncKeys();
				this.schedulePendingSyncKeyExpiry();
			},
			Math.max(0, expiresAt - Date.now()),
		);
		this.syncInFlightQueueExpiryTimer.unref?.();
	}

	async queueSync(
		keys: SyncableKey[],
		from: PublicSignKey,
		options?: { skipCheck?: boolean },
	) {
		if (this.closed === true || keys.length === 0) {
			return;
		}
		// A delayed timer must not let expired claims or admission reservations
		// keep the exact per-peer/global quota closed to fresh work.
		this.expirePendingSyncKeys();
		this.clearPendingSyncExpiryTimerIfIdle();
		const fromHash = from.hashcode();
		const canStartLookup =
			options?.skipCheck === true || this.canStartPendingSyncLookup(fromHash);
		const peerClaimCount =
			this.syncInFlightQueueInverted.get(fromHash)?.size ?? 0;
		let availableClaims = Math.max(
			0,
			Math.min(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER -
					peerClaimCount -
					(this.pendingSyncAdmissionCountByPeer.get(fromHash) ?? 0),
				MAX_PENDING_SIMPLE_SYNC_KEYS_GLOBAL -
					this.pendingSyncClaimCount -
					this.pendingSyncAdmissionCount,
			),
		);
		const targetEpoch = this.getOrCreateSyncDispatchTargetEpoch(fromHash);
		const ownershipLifecycleController = this.syncDispatchLifecycleController;
		const isCapturedLifecycleActive = () =>
			this.closed !== true &&
			this.syncDispatchLifecycleController === ownershipLifecycleController &&
			!ownershipLifecycleController.signal.aborted &&
			this.syncDispatchTargetEpochs.get(fromHash) === targetEpoch;
		const requestHashes: SyncableKey[] = [];
		const existingRequestHashes: SyncableKey[] = [];
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		if (!isCapturedLifecycleActive()) {
			return;
		}
		if (availableClaims === 0) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		const keysToCheck: SyncableKey[] = [];
		const identitiesToCheck: SyncableKey[] = [];
		const seen = new Set<SyncableKey>();
		const pendingAdmissionIdentities =
			this.pendingSyncAdmissionIdentitiesByPeer.get(fromHash);
		// Default senders use 1,024-key messages. Capping examined input at the
		// entire per-peer allowance prevents a duplicate-filled oversized vector
		// from turning admission itself into unbounded work.
		const inspectionLimit = canStartLookup
			? MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER
			: Math.min(DEFAULT_MAX_HASHES_PER_MESSAGE, keys.length);
		for (
			let index = 0;
			index < keys.length && index < inspectionLimit;
			index += 1
		) {
			const key = keys[index]!;
			const queuedKeyResult = this.getQueuedSyncKeyForAdmission(key);
			if (queuedKeyResult === QUEUED_SYNC_ALIAS_REFRESH_PENDING) {
				continue;
			}
			const queuedKey = queuedKeyResult;
			const coordinateOrHash = queuedKey ?? key;
			const identity = this.getPendingSyncKeyIdentity(coordinateOrHash);
			if (seen.has(identity)) {
				continue;
			}
			seen.add(identity);

			if (queuedKey != null) {
				let transferredDeadline: number | undefined;
				if (
					availableClaims > 0 &&
					pendingAdmissionIdentities?.has(identity) &&
					!this.hasPendingSyncClaim(queuedKey, fromHash)
				) {
					transferredDeadline = this.transferPendingSyncAdmissionIdentity(
						fromHash,
						identity,
					);
					if (transferredDeadline != null) {
						this.movePendingSyncKeyExpiryEarlier(
							queuedKey,
							transferredDeadline,
						);
					}
				}
				if (availableClaims === 0) {
					continue;
				}
				if (this.addPendingSyncClaim(queuedKey, from)) {
					availableClaims -= 1;
					existingRequestHashes.push(queuedKey);
				}
				continue;
			}

			if (
				pendingAdmissionIdentities?.has(identity) ||
				!canStartLookup ||
				availableClaims === 0
			) {
				continue;
			}
			keysToCheck.push(key);
			identitiesToCheck.push(identity);
			availableClaims -= 1;
		}
		const admission = this.reservePendingSyncAdmission(
			fromHash,
			identitiesToCheck,
		);
		const dispatchableExistingRequestHashes =
			this.filterDispatchablePendingSyncClaims(
				existingRequestHashes,
				fromHash,
				targetEpoch,
			);
		const existingRequest =
			dispatchableExistingRequestHashes.length > 0
				? this.requestSync(dispatchableExistingRequestHashes, [fromHash], {
						ownershipLifecycleController,
						targetEpochs: new Map([[fromHash, targetEpoch]]),
						createTargetEpochs: false,
					})
				: undefined;
		// Existing queued claims must not wait behind a potentially blocked storage
		// lookup for unrelated new keys. Observe this eagerly-started request even
		// when a later lifecycle check returns before joining it.
		void existingRequest?.catch(() => {});
		const resolveKnownStartedAt = syncProfileStart(profile);
		try {
			const knownKeys =
				options?.skipCheck === true || keysToCheck.length === 0
					? undefined
					: await this.resolveKnownSyncKeys(keysToCheck);
			if (!isCapturedLifecycleActive()) {
				return;
			}
			if (profile) {
				emitSyncProfileDuration(profile, resolveKnownStartedAt, {
					name: "simple.queueSync.resolveKnown",
					entries: keysToCheck.length,
					count: knownKeys?.keys.size ?? 0,
					details: {
						checkedCoordinates: knownKeys?.checkedCoordinates === true,
						checkedHashes: knownKeys?.checkedHashes === true,
						skipCheck: options?.skipCheck === true,
					},
				});
			}

			if (keysToCheck.length > 0) {
				// A resolver/index lookup may have populated coordinateToHash while
				// it yielded. Refresh another fixed-size slice, never the full queue.
				this.refreshQueuedSyncCoordinateAliases();
			}
			const loopStartedAt = syncProfileStart(profile);
			for (let index = 0; index < keysToCheck.length; index += 1) {
				const key = keysToCheck[index]!;
				const identity = identitiesToCheck[index]!;
				if (!isCapturedLifecycleActive()) {
					return;
				}
				const queuedKeyResult = this.getQueuedSyncKeyForAdmission(key);
				if (queuedKeyResult === QUEUED_SYNC_ALIAS_REFRESH_PENDING) {
					const consumption = this.consumePendingSyncAdmission(
						admission!,
						identity,
					);
					if (consumption === "invalid") {
						return;
					}
					continue;
				}
				const coordinateOrHash = queuedKeyResult ?? key;
				const inFlight = this.syncInFlightQueue.get(coordinateOrHash);
				if (inFlight) {
					if (!this.hasPendingSyncClaim(coordinateOrHash, fromHash)) {
						const consumption = this.consumePendingSyncAdmission(
							admission!,
							identity,
						);
						if (consumption === "invalid") {
							return;
						}
						if (consumption === "settled") {
							continue;
						}
						this.movePendingSyncKeyExpiryEarlier(
							coordinateOrHash,
							admission!.expiresAt,
						);
						const added = this.addPendingSyncClaim(
							coordinateOrHash,
							from,
							admission!.expiresAt,
						);
						if (added) {
							requestHashes.push(coordinateOrHash);
						}
					}
				} else {
					const has =
						options?.skipCheck !== true &&
						(await this.checkHasCoordinateOrHash(coordinateOrHash, knownKeys));
					if (!isCapturedLifecycleActive()) {
						return;
					}
					if (has) {
						this.clearPendingSyncAdmissionIdentity(identity);
						continue;
					}
					const consumption = this.consumePendingSyncAdmission(
						admission!,
						identity,
					);
					if (consumption === "invalid") {
						return;
					}
					if (consumption === "settled") {
						continue;
					}
					// Track the initial sender so we can retry if the first request is lost.
					this.addPendingSyncClaim(
						coordinateOrHash,
						from,
						admission!.expiresAt,
					);
					requestHashes.push(coordinateOrHash); // request immediately (first time we have seen this hash)
				}
			}
			if (profile) {
				emitSyncProfileDuration(profile, loopStartedAt, {
					name: "simple.queueSync.plan",
					entries: keysToCheck.length,
					count: requestHashes.length,
					targets: 1,
				});
			}

			// Persistent admission work is complete. Do not let an unrelated
			// blocked transport send retain unused quota.
			this.releasePendingSyncAdmission(admission);
			const dispatchableRequestHashes =
				this.filterDispatchablePendingSyncClaims(
					requestHashes,
					fromHash,
					targetEpoch,
				);
			dispatchableRequestHashes.length > 0 &&
				(await this.requestSync(dispatchableRequestHashes, [fromHash], {
					ownershipLifecycleController,
					targetEpochs: new Map([[fromHash, targetEpoch]]),
					createTargetEpochs: false,
				}));
			await existingRequest;
		} finally {
			this.releasePendingSyncAdmission(admission);
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.queueSync",
					entries: keys.length,
					targets: 1,
					details: {
						admitted: keysToCheck.length,
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
				for (const hash of hashes) {
					this.setSyncInFlightTargetKey(node, hash, now);
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
		if (typeof key === "bigint") {
			const mappedHash = this.coordinateToHash.get(key);
			if (mappedHash != null && (await this.log.has(mappedHash))) {
				return true;
			}
		}
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
		this.syncInFlightRetryIterator = undefined;
		this.syncInFlightRetryRemaining = 0;
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
				const requestHashesByEpoch = new Map<
					SyncDispatchTargetEpoch,
					{ target: string; hashes: SyncableKey[] }
				>();
				const now = Date.now();
				if (!this.syncInFlightRetryIterator) {
					this.syncInFlightRetryIterator = this.syncInFlightQueue.entries();
					this.syncInFlightRetryRemaining = this.syncInFlightQueue.size;
				}
				for (
					let inspected = 0;
					inspected < MAX_SIMPLE_SYNC_RETRY_KEYS_PER_TICK &&
					this.syncInFlightRetryRemaining > 0;
					inspected += 1
				) {
					const next = this.syncInFlightRetryIterator.next();
					if (next.done) {
						this.syncInFlightRetryIterator = undefined;
						this.syncInFlightRetryRemaining = 0;
						break;
					}
					this.syncInFlightRetryRemaining -= 1;
					const [key] = next.value;
					if (!isOpenLifecycleActive()) {
						return;
					}
					const expiresAt = this.syncInFlightQueueExpiresAt.get(key);
					if (expiresAt != null && expiresAt <= Date.now()) {
						this.clearSyncProcessKey(key);
						continue;
					}

					const has = await this.checkHasCoordinateOrHash(key);
					if (!isOpenLifecycleActive()) {
						return;
					}
					const value = this.syncInFlightQueue.get(key);
					if (!value) {
						continue;
					}
					const currentExpiresAt = this.syncInFlightQueueExpiresAt.get(key);
					if (currentExpiresAt != null && currentExpiresAt <= Date.now()) {
						this.clearSyncProcessKey(key);
						continue;
					}

					if (!has) {
						if (value.length === 0) {
							this.clearSyncProcessKey(key);
							continue;
						}

						const cursor =
							(this.syncInFlightQueueRoundRobinCursor.get(key) ?? 0) %
							value.length;
						const candidate = value[cursor]!;
						const publicKeyHash = candidate.hashcode();
						const inflightTimestamp = this.syncInFlight
							.get(publicKeyHash)
							?.get(key)?.timestamp;
						if (
							inflightTimestamp == null ||
							now - inflightTimestamp >= SIMPLE_SYNC_RETRY_AFTER_MS
						) {
							const epoch = this.syncDispatchTargetEpochs.get(publicKeyHash);
							if (!epoch) {
								continue;
							}
							let request = requestHashesByEpoch.get(epoch);
							if (!request) {
								request = { target: publicKeyHash, hashes: [] };
								requestHashesByEpoch.set(epoch, request);
							}
							request.hashes.push(key);
							if (value.length > 1) {
								this.syncInFlightQueueRoundRobinCursor.set(
									key,
									(cursor + 1) % value.length,
								);
							}
						}
					} else {
						this.clearSyncProcessKey(key);
					}
				}
				if (this.syncInFlightRetryRemaining === 0) {
					this.syncInFlightRetryIterator = undefined;
				}

				if (!isOpenLifecycleActive()) {
					return;
				}
				const nowMin10s = +new Date() - 2e4;
				for (const [target, map] of this.syncInFlight) {
					for (const [key, { timestamp }] of map) {
						if (timestamp < nowMin10s) {
							this.removeSyncInFlightTargetKey(target, key);
						}
					}
				}
				if (!isOpenLifecycleActive()) {
					return;
				}
				for (const [epoch, request] of requestHashesByEpoch) {
					if (!isOpenLifecycleActive()) {
						return;
					}
					const requestHashes = this.filterDispatchablePendingSyncClaims(
						request.hashes,
						request.target,
						epoch,
					);
					if (requestHashes.length === 0) {
						continue;
					}
					try {
						await this.requestSync(requestHashes, [request.target], {
							ownershipLifecycleController: openLifecycleController,
							targetEpochs: new Map([[request.target, epoch]]),
							createTargetEpochs: false,
						});
					} catch {
						if (!isOpenLifecycleActive()) {
							return;
						}
						// A failed target must not prevent bounded retries for unrelated
						// peers in this pass.
					}
				}
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
		this.clearPendingSyncAdmissions();
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlightRetryIterator = undefined;
		this.syncInFlightRetryRemaining = 0;
		this.syncInFlightQueueExpiresAt.clear();
		this.pendingSyncExpiryHeap.length = 0;
		this.pendingSyncKeyExpiryNodes.clear();
		this.pendingSyncAdmissionExpiryNodes.clear();
		this.syncInFlightQueueClaimants.clear();
		this.syncInFlightQueueClaimantIndexes.clear();
		this.syncInFlightQueueRoundRobinCursor.clear();
		this.syncInFlightQueuedCoordinates.clear();
		this.syncInFlightQueuedHashByCoordinate.clear();
		this.syncInFlightQueuedCoordinatesByHash.clear();
		this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
		this.pendingSyncClaimCount = 0;
		if (this.syncInFlightQueueExpiryTimer != null) {
			clearTimeout(this.syncInFlightQueueExpiryTimer);
			this.syncInFlightQueueExpiryTimer = undefined;
		}
		this.syncInFlight.clear();
		this.syncInFlightTargetsByKey.clear();
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
		for (const hash of hashes) {
			this.clearPendingSyncAdmissionIdentity(hash);
		}
		this.clearSyncProcesses(hashes);
		this.markRepairSessionResolvedHashes(hashes);
	}

	onEntryAddedHash(hash: string): void {
		if (!this.hasEntryAddedState()) {
			return;
		}
		this.clearPendingSyncAdmissionIdentity(hash);
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
			this.pendingSyncAdmissionCount > 0 ||
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

			const trackedClaimants = this.syncInFlightQueueClaimants.get(key);
			this.pendingSyncClaimCount = Math.max(
				0,
				this.pendingSyncClaimCount -
					(trackedClaimants?.size ?? inflight.length),
			);
			this.syncInFlightQueue.delete(key);
		}
		this.syncInFlightQueueClaimants.delete(key);
		this.syncInFlightQueueClaimantIndexes.delete(key);
		this.syncInFlightQueueRoundRobinCursor.delete(key);
		if (typeof key === "bigint") {
			this.removeQueuedSyncCoordinateAlias(key);
		}
		this.removePendingSyncKeyExpiry(key);
		this.syncInFlightQueueExpiresAt.delete(key);
		this.clearPendingSyncExpiryTimerIfIdle();

		this.clearSyncInFlightKey(key);
	}

	private removePendingSyncClaim(key: SyncableKey, peer: string): void {
		const inflight = this.syncInFlightQueue.get(key);
		if (!inflight) {
			return;
		}
		let claimants = this.syncInFlightQueueClaimants.get(key);
		let claimantIndexes = this.syncInFlightQueueClaimantIndexes.get(key);
		if (!claimants || !claimantIndexes) {
			claimants = new Set();
			claimantIndexes = new Map();
			for (let index = 0; index < inflight.length; index += 1) {
				const claimant = inflight[index]!.hashcode();
				claimants.add(claimant);
				claimantIndexes.set(claimant, index);
			}
			if (!this.syncInFlightQueueClaimants.has(key)) {
				this.pendingSyncClaimCount += claimants.size;
			}
			this.syncInFlightQueueClaimants.set(key, claimants);
			this.syncInFlightQueueClaimantIndexes.set(key, claimantIndexes);
		}
		const index = claimantIndexes.get(peer);
		if (index == null) {
			return;
		}

		const lastIndex = inflight.length - 1;
		if (index !== lastIndex) {
			const lastClaimant = inflight[lastIndex]!;
			const lastClaimantHash = lastClaimant.hashcode();
			inflight[index] = lastClaimant;
			claimantIndexes.set(lastClaimantHash, index);
		}
		inflight.pop();
		claimantIndexes.delete(peer);
		claimants.delete(peer);
		this.pendingSyncClaimCount = Math.max(0, this.pendingSyncClaimCount - 1);
		const inverted = this.syncInFlightQueueInverted.get(peer);
		inverted?.delete(key);
		if (inverted?.size === 0) {
			this.syncInFlightQueueInverted.delete(peer);
		}
		if (inflight.length > 0) {
			const cursor = this.syncInFlightQueueRoundRobinCursor.get(key) ?? 0;
			this.syncInFlightQueueRoundRobinCursor.set(
				key,
				cursor === lastIndex
					? index % inflight.length
					: cursor % inflight.length,
			);
			return;
		}

		this.syncInFlightQueue.delete(key);
		this.syncInFlightQueueClaimants.delete(key);
		this.syncInFlightQueueClaimantIndexes.delete(key);
		this.syncInFlightQueueRoundRobinCursor.delete(key);
		if (typeof key === "bigint") {
			this.removeQueuedSyncCoordinateAlias(key);
		}
		this.removePendingSyncKeyExpiry(key);
		this.syncInFlightQueueExpiresAt.delete(key);
		this.clearSyncInFlightKey(key);
		this.clearPendingSyncExpiryTimerIfIdle();
	}

	private removeSyncInFlightTargetKey(peer: string, key: SyncableKey): void {
		const map = this.syncInFlight.get(peer);
		if (!map?.delete(key)) {
			return;
		}
		if (map.size === 0) {
			this.syncInFlight.delete(peer);
		}
		const targets = this.syncInFlightTargetsByKey.get(key);
		targets?.delete(peer);
		if (targets?.size === 0) {
			this.syncInFlightTargetsByKey.delete(key);
		}
	}

	private setSyncInFlightTargetKey(
		peer: string,
		key: SyncableKey,
		timestamp: number,
	): void {
		let map = this.syncInFlight.get(peer);
		if (!map) {
			map = new Map();
			this.syncInFlight.set(peer, map);
		}
		const existing = map.get(key);
		if (!existing || existing.timestamp < timestamp) {
			map.set(key, { timestamp });
		}
		let targets = this.syncInFlightTargetsByKey.get(key);
		if (!targets) {
			targets = new Set();
			this.syncInFlightTargetsByKey.set(key, targets);
		}
		targets.add(peer);
	}

	private clearSyncInFlightTarget(peer: string): void {
		const map = this.syncInFlight.get(peer);
		if (!map) {
			return;
		}
		for (const key of map.keys()) {
			const targets = this.syncInFlightTargetsByKey.get(key);
			targets?.delete(peer);
			if (targets?.size === 0) {
				this.syncInFlightTargetsByKey.delete(key);
			}
		}
		this.syncInFlight.delete(peer);
	}

	private clearSyncInFlightKey(key: SyncableKey) {
		const targets = this.syncInFlightTargetsByKey.get(key);
		if (!targets) {
			// Defensive compatibility for tests or integrations that seed the public
			// syncInFlight map directly. Internal writes always populate the index.
			for (const [peer, map] of this.syncInFlight) {
				if (map.has(key)) {
					this.removeSyncInFlightTargetKey(peer, key);
				}
			}
			return;
		}
		for (const peer of [...targets]) {
			this.removeSyncInFlightTargetKey(peer, key);
		}
	}

	private forEachKnownAlias(
		hash: string,
		callback: (key: SyncableKey) => void,
	): void {
		callback(hash);
		for (const coordinate of [
			...(this.syncInFlightQueuedCoordinatesByHash.get(hash) ?? []),
		]) {
			callback(coordinate);
		}
	}

	private clearSyncInFlightForPeer(publicKeyHash: string, hash: string) {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		this.forEachKnownAlias(hash, (key) =>
			this.removeSyncInFlightTargetKey(publicKeyHash, key),
		);
	}

	private clearSyncInFlightForPeerHashes(
		publicKeyHash: string,
		hashes: string[],
	) {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map || hashes.length === 0) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		for (const hash of hashes) {
			this.forEachKnownAlias(hash, (key) =>
				this.removeSyncInFlightTargetKey(publicKeyHash, key),
			);
		}
	}

	private clearSyncProcess(hash: string) {
		this.refreshQueuedSyncCoordinateAliases();
		this.forEachKnownAlias(hash, (key) => this.clearSyncProcessKey(key));
	}

	private clearSyncProcesses(hashes: string[]) {
		if (hashes.length === 0) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		const keys = new Set<SyncableKey>();
		for (const hash of hashes) {
			this.forEachKnownAlias(hash, (key) => keys.add(key));
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
		this.clearPendingSyncAdmissions(publicKeyHash);
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
		this.clearSyncInFlightTarget(publicKeyHash);
		this.recentlySentExchangeHeads.delete(publicKeyHash);
		this.clearPendingMaybeSyncResponses(publicKeyHash);
		const map = this.syncInFlightQueueInverted.get(publicKeyHash);
		if (map) {
			for (const hash of [...map]) {
				this.removePendingSyncClaim(hash, publicKeyHash);
			}
			this.syncInFlightQueueInverted.delete(publicKeyHash);
		}
	}

	get pending() {
		return this.syncInFlightQueue.size;
	}
}
