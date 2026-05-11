import { BorshError, deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { AnyBlockStore, RemoteBlocks } from "@peerbit/blocks";
import { cidifyString } from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import {
	AccessError,
	Ed25519PublicKey,
	PublicSignKey,
	Secp256k1PublicKey,
	getPublicKeyFromPeerId,
	sha256Base64Sync,
	sha256Sync,
} from "@peerbit/crypto";
import {
	And,
	ByteMatchQuery,
	type DeleteOptions,
	type IdKey,
	type Ideable,
	type Index,
	NotStartedError as IndexNotStartedError,
	Or,
	Sort,
	StringMatch,
	toId,
} from "@peerbit/indexer-interface";
import {
	type AppendDeliveryPlan,
	type NativeReplicationRange,
	type SharedLogNativeState,
	type SharedLogRangePlanner,
	createRangePlanner,
	createSharedLogState,
} from "@peerbit/shared-log-rust";
import {
	type AppendOptions,
	type Change,
	Entry,
	EntryType,
	Log,
	type LogEvents,
	type LogProperties,
	Meta,
	ShallowEntry,
	type ShallowOrFullEntry,
} from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { ClosedError, Program, type ProgramEvents } from "@peerbit/program";
import {
	FanoutChannel,
	type FanoutProviderHandle,
	type FanoutTree,
	type FanoutTreeChannelOptions,
	type FanoutTreeDataEvent,
	type FanoutTreeUnicastEvent,
	type FanoutTreeJoinOptions,
	waitForSubscribers,
} from "@peerbit/pubsub";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { RPC, type RequestContext } from "@peerbit/rpc";
import {
	ACK_CONTROL_PRIORITY,
	AcknowledgeDelivery,
	AnyWhere,
	createRequestTransportContext,
	DataMessage,
	MessageHeader,
	NotStartedError,
	type RouteHint,
	SilentDelivery,
} from "@peerbit/stream-interface";
import {
	AbortError,
	TimeoutError,
	debounceAccumulator,
	debounceFixedInterval,
	waitFor,
} from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";
import PQueue from "p-queue";
import { concat, fromString } from "uint8arrays";
import { BlocksMessage } from "./blocks.js";
import { type CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
import {
	type DebouncedAccumulatorMap,
	debouncedAccumulatorMap,
} from "./debounce.js";
import { NoPeersError } from "./errors.js";

type SharedLogServicesWithFanout = {
	fanout?: FanoutTree;
};

const getSharedLogFanoutService = (
	services: unknown,
): FanoutTree | undefined =>
	(services as SharedLogServicesWithFanout).fanout;
import {
	EXCHANGE_HEADS_REPAIR_HINT,
	EntryWithRefs,
	ExchangeHeadsMessage,
	RequestIPrune,
	ResponseIPrune,
	createExchangeHeadsMessages,
} from "./exchange-heads.js";
import { FanoutEnvelope } from "./fanout-envelope.js";
import {
	MAX_U32,
	MAX_U64,
	type NumberFromType,
	type Numbers,
	bytesToNumber,
	createNumbers,
	denormalizer,
} from "./integers.js";
import { TransportMessage } from "./message.js";
import { PIDReplicationController } from "./pid.js";
import {
	type EntryReplicated,
	EntryReplicatedU32,
	EntryReplicatedU64,
	type ReplicationChange,
	type ReplicationChanges,
	ReplicationIntent,
	type ReplicationRangeIndexable,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
	ReplicationRangeMessage,
	appromixateCoverage,
	calculateCoverage,
	countCoveringRangesSameOwner,
	createAssignedRangesQuery,
	debounceAggregationChanges,
	getAllMergeCandiates,
	getCoverSet,
	getSamples,
	isMatured,
	isReplicationRangeMessage,
	mergeRanges,
	minimumWidthToCover,
	shouldAssigneToRangeBoundary as shouldAssignToRangeBoundary,
	toRebalance,
} from "./ranges.js";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "./replication-domain-hash.js";
import {
	type ReplicationDomainTime,
	createReplicationDomainTime,
} from "./replication-domain-time.js";
import {
	type CoverRange,
	type ExtractDomainArgs,
	type ReplicationDomain,
	type ReplicationDomainConstructor,
} from "./replication-domain.js";
import {
	AbsoluteReplicas,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	MinReplicas,
	ReplicationPingMessage,
	ReplicationError,
	type ReplicationLimits,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
	StoppedReplicating,
	decodeReplicas,
	encodeReplicas,
	maxReplicas,
} from "./replication.js";
import { Observer, Replicator } from "./role.js";
import type {
	SyncOptions,
	SynchronizerConstructor,
	Syncronizer,
} from "./sync/index.js";
import { RatelessIBLTSynchronizer } from "./sync/rateless-iblt.js";
import { ConfirmEntriesMessage, SimpleSyncronizer } from "./sync/simple.js";
import { groupByGid } from "./utils.js";

const toLocalPublicSignKey = (
	key: PublicSignKey | string,
): PublicSignKey | undefined => {
	if (typeof key === "string") {
		return undefined;
	}
	if (key instanceof PublicSignKey) {
		return key;
	}

	try {
		return deserialize(serialize(key), PublicSignKey);
	} catch {
		const publicKey = (key as { publicKey?: unknown }).publicKey;
		const publicKeyBytes =
			publicKey instanceof Uint8Array
				? publicKey
				: ArrayBuffer.isView(publicKey)
					? new Uint8Array(
							publicKey.buffer,
							publicKey.byteOffset,
							publicKey.byteLength,
						)
					: undefined;

		if (publicKeyBytes?.byteLength === 32) {
			return new Ed25519PublicKey({
				publicKey: new Uint8Array(publicKeyBytes),
			});
		}
		if (publicKeyBytes?.byteLength === 33) {
			return new Secp256k1PublicKey({
				publicKey: new Uint8Array(publicKeyBytes),
			});
		}

		return undefined;
	}
};

export {
	type ReplicationDomain,
	type ReplicationDomainHash,
	type ReplicationDomainTime,
	createReplicationDomainHash,
	createReplicationDomainTime,
};
export { type CPUUsage, CPUUsageIntervalLag };
export * from "./replication.js";
export type {
	LogLike,
	LogResultsIterator,
	SharedLogLike,
	SharedLogReplicationIndexLike,
} from "./like.js";
export {
	type ReplicationRangeIndexable,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
	type EntryReplicated,
	EntryReplicatedU32,
	EntryReplicatedU64,
	type CoverRange,
	NoPeersError,
};
export { MAX_U32, MAX_U64, type NumberFromType };
export const logger = loggerFn("peerbit:shared-log");
const warn = logger.newScope("warn");

type CheckedPruneLeaderMap = Map<string, { intersecting: boolean }>;
type CheckedPruneEntry<T, R extends "u32" | "u64"> =
	| Entry<T>
	| ShallowEntry
	| EntryReplicated<R>;

type LeaderMap = Map<string, { intersecting: boolean }>;

type LeaderSelectionOptions<R extends "u32" | "u64"> = {
	roleAge?: number;
	candidates?: Iterable<string>;
	onLeader?: (key: string) => void;
	persist?:
		| {
				prev?: EntryReplicated<R>;
		  }
		| false;
};

type WaitForReplicatorsOptions<R extends "u32" | "u64"> =
	LeaderSelectionOptions<R> & {
		timeout?: number;
	};

type WaitForReplicator = { key: string; replicator: boolean };

type EntryLeaderPlan<R extends "u32" | "u64"> = {
	coordinates: NumberFromType<R>[];
	leaders: LeaderMap;
	isLeader: boolean;
	assignedToRangeBoundary?: boolean;
};

type NativeAppendEntryPlan<R extends "u32" | "u64"> = EntryLeaderPlan<R> & {
	delivery?: AppendDeliveryPlan;
};

type EntryLeaderBatchItem<R extends "u32" | "u64"> = {
	entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
	replicas: number;
	options?: LeaderSelectionOptions<R>;
};

const getLatestEntry = (
	entries: (ShallowOrFullEntry<any> | EntryWithRefs<any>)[],
) => {
	let latest: ShallowOrFullEntry<any> | undefined = undefined;
	for (const element of entries) {
		let entry = element instanceof EntryWithRefs ? element.entry : element;
		if (
			!latest ||
			entry.meta.clock.timestamp.compare(latest.meta.clock.timestamp) > 0
		) {
			latest = entry;
		}
	}
	return latest;
};

const hashToSeed32 = (str: string) => {
	// FNV-1a 32-bit, fast and deterministic.
	let hash = 0x811c9dc5;
	for (let i = 0; i < str.length; i++) {
		hash ^= str.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193);
	}
	return hash >>> 0;
};

const pickDeterministicSubset = (peers: string[], seed: number, max: number) => {
	if (peers.length <= max) return peers;

	const subset: string[] = [];
	const used = new Set<string>();
	let x = seed || 1;
	while (subset.length < max) {
		// xorshift32
		x ^= x << 13;
		x ^= x >>> 17;
		x ^= x << 5;
		const peer = peers[(x >>> 0) % peers.length];
		if (!used.has(peer)) {
			used.add(peer);
			subset.push(peer);
		}
	}
	return subset;
};

export type ReplicationLimitsOptions =
	| Partial<ReplicationLimits>
	| { min?: number; max?: number };

export type DynamicReplicationOptions<R extends "u32" | "u64"> = {
	limits?: {
		interval?: number;
		storage?: number;
		cpu?: number | { max: number; monitor?: CPUUsage };
	};
} & (
	| { offset: number; normalized?: true | undefined }
	| { offset: NumberFromType<R>; normalized: false }
	| { offset?: undefined; normalized?: undefined }
);

export type FixedReplicationOptions = {
	id?: Uint8Array;
	normalized?: boolean;
	factor: number | bigint | "all" | "right";
	strict?: boolean; // if true, only this range will be replicated
	offset?: number | bigint;
};

type NewReplicationOptions<R extends "u32" | "u64" = any> =
	| DynamicReplicationOptions<R>
	| FixedReplicationOptions
	| FixedReplicationOptions[]
	| number
	| boolean;

type ExistingReplicationOptions<R extends "u32" | "u64" = any> = {
	type: "resume";
	default: NewReplicationOptions<R>;
};
export type ReplicationOptions<R extends "u32" | "u64" = any> =
	| NewReplicationOptions<R>
	| ExistingReplicationOptions<R>;

export { BlocksMessage };

const isAdaptiveReplicatorOption = (
	options: ReplicationOptions<any>,
): options is DynamicReplicationOptions<any> => {
	if (typeof options === "number") {
		return false;
	}
	if (typeof options === "boolean") {
		return false;
	}
	if ((options as FixedReplicationOptions).factor != null) {
		return false;
	}
	if (Array.isArray(options)) {
		return false;
	}
	return true;
};

const isUnreplicationOptions = (options?: ReplicationOptions<any>): boolean =>
	options === false ||
	options === 0 ||
	((options as FixedReplicationOptions)?.offset === undefined &&
		(options as FixedReplicationOptions)?.factor === 0);

const isReplicationOptionsDependentOnPreviousState = async (
	options: ReplicationOptions<any> | undefined,
	index: Index<ReplicationRangeIndexable<any>>,
	me: PublicSignKey,
): Promise<boolean> => {
	if (options === true) {
		return true;
	}

	if ((options as ExistingReplicationOptions<any>)?.type === "resume") {
		// check if there is actually previous replication info
		let countSegments = await index.count({
			query: new StringMatch({
				key: "hash",
				value: me.hashcode(),
			}),
		});
		return countSegments > 0;
	}

	if (options == null) {
		// when not providing options, we assume previous behaviour
		return true;
	}

	// if empty object but with no keys
	if (typeof options === "object" && Object.keys(options).length === 0) {
		return true;
	}

	return false;
};

const isNotStartedError = (e: Error) => {
	if (e instanceof AbortError) {
		return true;
	}
	if (e instanceof NotStartedError) {
		return true;
	}
	if (e instanceof IndexNotStartedError) {
		return true;
	}
	if (e instanceof ClosedError) {
		return true;
	}
	return false;
};

interface IndexableDomain<R extends "u32" | "u64"> {
	numbers: Numbers<R>;
	constructorEntry: new (properties: {
		coordinates: NumberFromType<R>[];
		hash: string;
		meta: Meta;
		metaBytes?: Uint8Array;
		assignedToRangeBoundary: boolean;
		hashNumber: NumberFromType<R>;
	}) => EntryReplicated<R>;
	constructorRange: new (
		properties: {
			id?: Uint8Array;
			offset: NumberFromType<R>;
			width: NumberFromType<R>;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		} & ({ publicKeyHash: string } | { publicKey: PublicSignKey }),
	) => ReplicationRangeIndexable<R>;
}

type PutAndDeleteIndex<T extends Record<string, any>> = Index<T> & {
	putAndDelete?: (
		value: T,
		deleteOptions: DeleteOptions,
	) => Promise<unknown> | unknown;
	putAndDeleteIds?: (
		value: T,
		deleteIds: Array<IdKey | Ideable>,
		id?: IdKey,
	) => Promise<unknown> | unknown;
	delIds?: (deleteIds: Array<IdKey | Ideable>) => Promise<unknown> | unknown;
	putSharedLogCoordinateAndDeleteIds?: (
		value: T,
		fields: {
			hash: string;
			hashNumber: NumberFromType<any>;
			gid: string;
			coordinates: NumberFromType<any>[];
			wallTime: bigint;
			assignedToRangeBoundary: boolean;
			metaBytes: Uint8Array;
		},
		deleteIds?: Array<IdKey | Ideable>,
		id?: IdKey,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinatesAndDeleteIdsBatch?: (
		values: Array<{
			value: T;
			fields: {
				hash: string;
				hashNumber: NumberFromType<any>;
				gid: string;
				coordinates: NumberFromType<any>[];
				wallTime: bigint;
				assignedToRangeBoundary: boolean;
				metaBytes: Uint8Array;
			};
			deleteIds?: Array<IdKey | Ideable>;
			id?: IdKey;
		}>,
	) => Promise<unknown> | unknown;
};

type EntryWithMetaBytes = {
	getMetaBytes?: () => Uint8Array | undefined;
	getHashDigestBytes?: () => Uint8Array | undefined;
};

const createIndexableDomainFromResolution = <R extends "u32" | "u64">(
	resolution: R,
): IndexableDomain<R> => {
	const denormalizerFn = denormalizer<R>(resolution);
	const byteToNumberFn = bytesToNumber<R>(resolution);
	if (resolution === "u32") {
		return {
			constructorEntry: EntryReplicatedU32,
			constructorRange: ReplicationRangeIndexableU32,
			denormalize: denormalizerFn,
			bytesToNumber: byteToNumberFn,
			numbers: createNumbers(resolution),
		} as any as IndexableDomain<R>;
	} else if (resolution === "u64") {
		return {
			constructorEntry: EntryReplicatedU64,
			constructorRange: ReplicationRangeIndexableU64,
			denormalize: denormalizerFn,
			bytesToNumber: byteToNumberFn,
			numbers: createNumbers(resolution),
		} as any as IndexableDomain<R>;
	}
	throw new Error("Unsupported resolution");
};

export type SharedLogOptions<
	T,
	D extends ReplicationDomain<any, T, R>,
	R extends "u32" | "u64" = D extends ReplicationDomain<any, T, infer I>
		? I
		: "u32",
> = {
	appendDurability?: LogProperties<T>["appendDurability"];
	nativeGraph?: LogProperties<T>["nativeGraph"];
	nativeRangePlanner?: false | { optional?: boolean };
	replicate?: ReplicationOptions<R>;
	replicas?: ReplicationLimitsOptions;
	respondToIHaveTimeout?: number;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
	keep?: (
		entry: ShallowOrFullEntry<T> | EntryReplicated<R>,
	) => Promise<boolean> | boolean;
	sync?: SyncOptions<R>;
	syncronizer?: SynchronizerConstructor<R>;
	timeUntilRoleMaturity?: number;
	waitForReplicatorTimeout?: number;
	waitForReplicatorRequestIntervalMs?: number;
	waitForReplicatorRequestMaxAttempts?: number;
	waitForPruneDelay?: number;
	distributionDebounceTime?: number;
	strictFullReplicaFallback?: boolean;
	compatibility?: number;
	domain?: ReplicationDomainConstructor<D>;
	eagerBlocks?: boolean | { cacheSize?: number };
	fanout?: SharedLogFanoutOptions;
};

export const DEFAULT_MIN_REPLICAS = 2;
export const WAIT_FOR_REPLICATOR_TIMEOUT = 20000;
export const WAIT_FOR_ROLE_MATURITY = 5000;
export const WAIT_FOR_REPLICATOR_REQUEST_INTERVAL = 1000;
export const WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS = 3;
// TODO(prune): Investigate if/when a non-zero prune delay is required for correctness
// (e.g. responsibility/replication-info message reordering in multi-peer scenarios).
// Prefer making pruning robust without timing-based heuristics.
export const WAIT_FOR_PRUNE_DELAY = 0;
const PRUNE_DEBOUNCE_INTERVAL = 500;
const CHECKED_PRUNE_RESEND_INTERVAL_MIN_MS = 250;
const CHECKED_PRUNE_RESEND_INTERVAL_MAX_MS = 5_000;
const CHECKED_PRUNE_RETRY_MAX_ATTEMPTS = 3;
const CHECKED_PRUNE_RETRY_MAX_DELAY_MS = 30_000;

// DONT SET THIS ANY LOWER, because it will make the pid controller unstable as the system responses are not fast enough to updates from the pid controller
const RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL = 1000;
const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE = 0.01;
const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_CPU_LIMIT = 0.005;
const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_MEMORY_LIMIT = 0.001;
const RECALCULATE_PARTICIPATION_RELATIVE_DENOMINATOR_FLOOR = 1e-3;
const TOPIC_SUBSCRIBERS_CACHE_TTL_MS = 250;
const ADAPTIVE_REBALANCE_IDLE_INTERVAL_MULTIPLIER = 5;
const ADAPTIVE_REBALANCE_MIN_IDLE_AFTER_LOCAL_APPEND_MS = 10_000;

const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;
const RECENT_REPAIR_DISPATCH_TTL_MS = 5_000;
const REPAIR_SWEEP_ENTRY_BATCH_SIZE = 1_000;
const REPAIR_SWEEP_TARGET_BUFFER_SIZE = 1024;
// In sparse topologies (browser/relay), peers can learn about replicators via broadcast
// replication announcements without having a direct connection that emits unsubscribe
// on abrupt churn. Probe conservatively so a single missed ACK does not evict a
// healthy replicator, and rely on replication-info refresh to recover membership.
const REPLICATOR_LIVENESS_SWEEP_INTERVAL_MS = 2_000;
const REPLICATOR_LIVENESS_IDLE_THRESHOLD_MS = 8_000;
const REPLICATOR_LIVENESS_PROBE_FAILURES_TO_EVICT = 2;
// Churn/join repair can race with pruning and transient missed sync requests under
// heavy event-loop load. Keep retries alive with a longer tail so reassigned
// entries are retried after short bursts and slower recovery windows.
const CHURN_REPAIR_RETRY_SCHEDULE_MS = [
	0, 1_000, 3_000, 7_000, 15_000, 30_000, 45_000,
];
const JOIN_WARMUP_RETRY_SCHEDULE_MS = [
	0,
	1_000,
	3_000,
	7_000,
	15_000,
	30_000,
	60_000,
];
const JOIN_AUTHORITATIVE_RETRY_SCHEDULE_MS = [
	0,
	1_000,
	3_000,
	7_000,
	15_000,
	30_000,
	60_000,
];
const APPEND_BACKFILL_RETRY_SCHEDULE_MS = [0, 1_000, 3_000, 7_000];
const JOIN_AUTHORITATIVE_REPAIR_DELAY_MS = 2_000;
const JOIN_AUTHORITATIVE_REPAIR_SWEEP_DELAYS_MS = [
	JOIN_AUTHORITATIVE_REPAIR_DELAY_MS,
	7_000,
	15_000,
	30_000,
];
const APPEND_BACKFILL_DELAY_MS = 500;
const ASSUME_SYNCED_REPAIR_SUPPRESSION_MS = 5_000;
const REPAIR_CONFIRMATION_HASH_BATCH_SIZE = 1_024;

type RepairDispatchMode =
	| "join-warmup"
	| "join-authoritative"
	| "append-backfill"
	| "churn";
type RepairTransportMode = "rateless" | "simple";
type RepairMetricBucket = {
	dispatches: number;
	entries: number;
	ratelessFirstPasses: number;
	simpleFallbackPasses: number;
};
type RepairMetrics = Record<RepairDispatchMode, RepairMetricBucket>;

const REPAIR_DISPATCH_MODES: RepairDispatchMode[] = [
	"join-warmup",
	"join-authoritative",
	"append-backfill",
	"churn",
];

const createRepairMetricBucket = (): RepairMetricBucket => ({
	dispatches: 0,
	entries: 0,
	ratelessFirstPasses: 0,
	simpleFallbackPasses: 0,
});

const createRepairMetrics = (): RepairMetrics => ({
	"join-warmup": createRepairMetricBucket(),
	"join-authoritative": createRepairMetricBucket(),
	"append-backfill": createRepairMetricBucket(),
	churn: createRepairMetricBucket(),
});

const createRepairPendingPeersByMode = () =>
	new Map<RepairDispatchMode, Set<string>>(
		REPAIR_DISPATCH_MODES.map((mode) => [mode, new Set<string>()]),
	);

const cloneRepairPendingPeersByMode = (
	pending: Map<RepairDispatchMode, Set<string>>,
) =>
	new Map<RepairDispatchMode, Set<string>>(
		REPAIR_DISPATCH_MODES.map((mode) => [mode, new Set(pending.get(mode) ?? [])]),
	);

const createRepairFrontierByMode = () =>
	new Map<
		RepairDispatchMode,
		Map<string, Map<string, EntryReplicated<any>>>
	>(REPAIR_DISPATCH_MODES.map((mode) => [mode, new Map()]));

const createRepairActiveTargetsByMode = () =>
	new Map<RepairDispatchMode, Set<string>>(
		REPAIR_DISPATCH_MODES.map((mode) => [mode, new Set()]),
	);

const getRepairRetrySchedule = (mode: RepairDispatchMode) => {
	switch (mode) {
		case "join-warmup":
			return JOIN_WARMUP_RETRY_SCHEDULE_MS;
		case "join-authoritative":
			return JOIN_AUTHORITATIVE_RETRY_SCHEDULE_MS;
		case "append-backfill":
			return APPEND_BACKFILL_RETRY_SCHEDULE_MS;
		case "churn":
			return CHURN_REPAIR_RETRY_SCHEDULE_MS;
	}
};

const resolveRepairRetrySchedule = (
	mode: RepairDispatchMode,
	override?: number[],
	trackedFrontier = false,
) => {
	const fallback = getRepairRetrySchedule(mode);
	if (!override || override.length === 0) {
		return fallback;
	}
	if (
		trackedFrontier &&
		override.length === 1 &&
		override[0] === 0 &&
		fallback.length > 1
	) {
		// A tracked frontier with only an immediate retry would otherwise stay on
		// attempt 0 forever, which means rateless-only retries and no sparse-tail
		// simple fallback. Keep the immediate seed, then continue with the normal
		// tracked repair schedule.
		return [0, ...fallback.slice(1)];
	}
	return override;
};

const getRepairTransportForAttempt = (
	mode: RepairDispatchMode,
	attemptIndex: number,
): RepairTransportMode => {
	if (mode === "churn") {
		return "simple";
	}
	return attemptIndex === 0 ? "rateless" : "simple";
};

const toPositiveInteger = (
	value: number | undefined,
	fallback: number,
	label: string,
) => {
	if (value == null) {
		return fallback;
	}
	if (!Number.isFinite(value) || value <= 0) {
		throw new Error(`${label} must be a positive number`);
	}
	return Math.max(1, Math.floor(value));
};

const DEFAULT_SHARED_LOG_FANOUT_CHANNEL_OPTIONS: Omit<
	FanoutTreeChannelOptions,
	"role"
> = {
	msgRate: 30,
	msgSize: 1024,
	uploadLimitBps: 5_000_000,
	maxChildren: 24,
	repair: true,
};

const getIdForDynamicRange = (publicKey: PublicSignKey) => {
	return sha256Sync(
		concat([publicKey.bytes, new TextEncoder().encode("dynamic")]),
	);
};

const checkMinReplicasLimit = (minReplicas: number) => {
	if (minReplicas > 100) {
		throw new Error(
			"Higher replication degree than 100 is not recommended for performance reasons",
		);
	}
};

export type Args<
	T,
	D extends ReplicationDomain<any, T, R>,
	R extends "u32" | "u64" = D extends ReplicationDomain<any, T, infer I>
		? I
		: "u32",
> = LogProperties<T> & LogEvents<T> & SharedLogOptions<T, D, R>;

export type DeliveryReliability = "ack" | "best-effort";

export type DeliveryOptions = {
	reliability?: DeliveryReliability;
	minAcks?: number;
	requireRecipients?: boolean;
	timeout?: number;
	signal?: AbortSignal;
};

export type SharedLogFanoutOptions = {
	root?: string;
	channel?: Partial<Omit<FanoutTreeChannelOptions, "role">>;
	join?: FanoutTreeJoinOptions;
};

type SharedAppendBaseOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
	replicate?: boolean;
};

export type SharedAppendOptions<T> =
	| (SharedAppendBaseOptions<T> & {
			target?: "replicators" | "none";
			delivery?: false | true | DeliveryOptions;
	  })
	| (SharedAppendBaseOptions<T> & {
			// target=all uses the fanout data plane and intentionally does not expose
			// per-recipient settle semantics from RPC delivery options.
			target: "all";
			delivery?: false | undefined;
	  });

export type ReplicatorJoinEvent = { publicKey: PublicSignKey };
export type ReplicatorLeaveEvent = { publicKey: PublicSignKey };
export type ReplicationChangeEvent = { publicKey: PublicSignKey };
export type ReplicatorMatureEvent = { publicKey: PublicSignKey };

export interface SharedLogEvents extends ProgramEvents {
	"replicator:join": CustomEvent<ReplicatorJoinEvent>;
	"replicator:leave": CustomEvent<ReplicatorLeaveEvent>;
	"replication:change": CustomEvent<ReplicationChangeEvent>;
	"replicator:mature": CustomEvent<ReplicatorMatureEvent>;
}

@variant("shared_log")
export class SharedLog<
	T,
	D extends ReplicationDomain<any, T, R> = any,
	R extends "u32" | "u64" = D extends ReplicationDomain<any, T, infer I>
		? I
		: "u32",
> extends Program<Args<T, D, R>, SharedLogEvents> {
	@field({ type: Log })
	log: Log<T>;

	@field({ type: RPC })
	rpc: RPC<TransportMessage, TransportMessage>;

	// options
	private _isReplicating!: boolean;
	private _isAdaptiveReplicating!: boolean;

	private _replicationRangeIndex!: Index<ReplicationRangeIndexable<R>>;
	private _entryCoordinatesIndex!: Index<EntryReplicated<R>>;
	private _nativeRangePlanner?: SharedLogRangePlanner;
	private _nativeSharedLogState?: SharedLogNativeState;
	private _residentEntryCoordinatesByHash?: Map<string, EntryReplicated<R>>;
	private coordinateToHash!: Cache<string>;
	private recentlyRebalanced!: Cache<string>;

	uniqueReplicators!: Set<string>;
	private _replicatorJoinEmitted!: Set<string>;
	private _replicatorsReconciled!: boolean;

	/* private _totalParticipation!: number; */

	// gid -> coordinate -> publicKeyHash list (of owners)
	_gidPeersHistory!: Map<string, Set<string>>;

	private _onSubscriptionFn!: (arg: any) => any;
	private _onUnsubscriptionFn!: (arg: any) => any;
	private _onFanoutDataFn?: (arg: any) => void;
	private _onFanoutUnicastFn?: (arg: any) => void;
	private _fanoutChannel?: FanoutChannel;
	private _providerHandle?: FanoutProviderHandle;

	private _isTrustedReplicator?: (
		publicKey: PublicSignKey,
	) => Promise<boolean> | boolean;

	private _logProperties?: LogProperties<T> &
		LogEvents<T> &
		SharedLogOptions<T, D, R>;
	private _closeController!: AbortController;
	private _respondToIHaveTimeout!: any;
	private _pendingDeletes!: Map<
		string,
		{
			promise: DeferredPromise<void>;
			clear: () => void;
			resolve: (publicKeyHash: string) => Promise<void> | void;
			reject(reason: any): Promise<void> | void;
		}
	>;

	private _pendingIHave!: Map<
		string,
		{
			resetTimeout: () => void;
			requesting: Set<string>;
			clear: () => void;
			callback: (entry: Entry<T>) => void;
		}
	>;

	// public key hash to range id to range
	pendingMaturity!: Map<
		string,
		Map<
			string,
			{
				range: ReplicationChange;
				timeout: ReturnType<typeof setTimeout>;
			}
		>
	>; // map of peerId to timeout

	private latestReplicationInfoMessage!: Map<string, bigint>;
	// Peers that have unsubscribed from this log's topic. We ignore replication-info
	// messages from them until we see a new subscription, to avoid re-introducing
	// stale membership state during close/unsubscribe races.
	private _replicationInfoBlockedPeers!: Set<string>;
	private _replicationInfoRequestByPeer!: Map<
		string,
		{ attempts: number; timer?: ReturnType<typeof setTimeout> }
	>;
	private _replicationInfoApplyQueueByPeer!: Map<string, Promise<void>>;
	private _replicatorLivenessSweepRunning!: boolean;
	private _replicatorLivenessTimer?: ReturnType<typeof setInterval>;
	private _replicatorLivenessTargets!: string[];
	private _replicatorLivenessTargetsSize!: number;
	private _replicatorLivenessCursor!: number;
	private _replicatorLivenessFailures!: Map<string, number>;
	private _replicatorLastActivityAt!: Map<string, number>;

	private remoteBlocks!: RemoteBlocks;

	private openTime!: number;
	private oldestOpenTime!: number;

	private keep?: (
		entry: ShallowOrFullEntry<T> | EntryReplicated<R>,
	) => Promise<boolean> | boolean;

	// A fn that we can call many times that recalculates the participation role
	private rebalanceParticipationDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;

	// A fn for debouncing the calls for pruning
	pruneDebouncedFn!: DebouncedAccumulatorMap<{
		entry: CheckedPruneEntry<T, R>;
		leaders: CheckedPruneLeaderMap;
	}>;
	private responseToPruneDebouncedFn!: ReturnType<
		typeof debounceAccumulator<
			string,
			{
				hashes: string[];
				peers: string[] | Set<string>;
			},
			Map<string, Set<string>>
		>
	>;

	private _requestIPruneSent!: Map<string, Set<string>>; // tracks entry hash to peer hash for requesting I prune messages
	private _requestIPruneResponseReplicatorSet!: Map<string, Set<string>>; // tracks entry hash to peer hash
	private _checkedPruneRetries!: Map<
		string,
		{ attempts: number; timer?: ReturnType<typeof setTimeout> }
	>;

	private replicationChangeDebounceFn!: ReturnType<
		typeof debounceAggregationChanges<ReplicationRangeIndexable<R>>
	>;
	private _repairRetryTimers!: Set<ReturnType<typeof setTimeout>>;
	private _recentRepairDispatch!: Map<string, Map<string, number>>;
	private _repairSweepRunning!: boolean;
	private _repairSweepPendingModes!: Set<RepairDispatchMode>;
	private _repairSweepPendingPeersByMode!: Map<RepairDispatchMode, Set<string>>;
	private _repairFrontierByMode!: Map<
		RepairDispatchMode,
		Map<string, Map<string, EntryReplicated<R>>>
	>;
	private _repairFrontierActiveTargetsByMode!: Map<RepairDispatchMode, Set<string>>;
	private _repairSweepOptimisticGidPeersPending!: Map<string, Map<string, number>>;
	private _entryKnownPeers!: Map<string, Set<string>>;
	private _joinAuthoritativeRepairTimersByDelay!: Map<
		number,
		ReturnType<typeof setTimeout>
	>;
	private _joinAuthoritativeRepairPeersByDelay!: Map<number, Set<string>>;
	private _assumeSyncedRepairSuppressedUntil!: number;
	private _appendBackfillTimer?: ReturnType<typeof setTimeout>;
	private _appendBackfillPendingByTarget!: Map<string, Map<string, EntryReplicated<R>>>;
	private _repairMetrics!: RepairMetrics;
	private _topicSubscribersCache!: Map<
		string,
		{ expiresAt: number; keys: PublicSignKey[] }
	>;

	// regular distribution checks
	private distributeQueue?: PQueue;

	syncronizer!: Syncronizer<R>;

	replicas!: ReplicationLimits;

	private cpuUsage?: CPUUsage;
	private _lastLocalAppendAt!: number;
	private adaptiveRebalanceIdleMs!: number;

	timeUntilRoleMaturity!: number;
	waitForReplicatorTimeout!: number;
	waitForReplicatorRequestIntervalMs!: number;
	waitForReplicatorRequestMaxAttempts?: number;
	waitForPruneDelay!: number;
	distributionDebounceTime!: number;
	repairSweepTargetBufferSize!: number;

	replicationController!: PIDReplicationController;
	history!: { usedMemory: number; factor: number }[];
	domain!: D;
	indexableDomain!: IndexableDomain<R>;
	interval: any;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.log = new Log(properties);
		this.rpc = new RPC();
	}

	get compatibility(): number | undefined {
		return this._logProperties?.compatibility;
	}

	get isAdaptiveReplicating() {
		return this._isAdaptiveReplicating;
	}

	private get v8Behaviour() {
		return (this.compatibility ?? Number.MAX_VALUE) < 9;
	}

	private getFanoutChannelOptions(
		options?: SharedLogFanoutOptions,
	): Omit<FanoutTreeChannelOptions, "role"> {
		return {
			...DEFAULT_SHARED_LOG_FANOUT_CHANNEL_OPTIONS,
			...(options?.channel ?? {}),
		};
	}

	private async _openFanoutChannel(options?: SharedLogFanoutOptions) {
		this._closeFanoutChannel();
		if (!options) {
			return;
		}

		const fanoutService = getSharedLogFanoutService(this.node.services);
		if (!fanoutService) {
			throw new Error(
				`Fanout is configured for shared-log topic ${this.topic}, but no fanout service is available on this client`,
			);
		}

		const resolvedRoot =
			options.root ??
			(await fanoutService?.topicRootControlPlane?.resolveTopicRoot?.(
				this.topic,
			));
		if (!resolvedRoot) {
			throw new Error(
				`Fanout is configured for shared-log topic ${this.topic}, but no fanout root was provided and none could be resolved`,
			);
		}

		const channel = new FanoutChannel(fanoutService, {
			topic: this.topic,
			root: resolvedRoot,
		});
		this._fanoutChannel = channel;

		this._onFanoutDataFn =
			this._onFanoutDataFn ||
			((evt: any) => {
				const detail = (evt as CustomEvent<FanoutTreeDataEvent>)?.detail;
				if (!detail) {
					return;
				}
				void this._onFanoutData(detail).catch((error) => logger.error(error));
			});
		channel.addEventListener("data", this._onFanoutDataFn);

		this._onFanoutUnicastFn =
			this._onFanoutUnicastFn ||
			((evt: any) => {
				const detail = (evt as CustomEvent<FanoutTreeUnicastEvent>)?.detail;
				if (!detail) {
					return;
				}
				void this._onFanoutUnicast(detail).catch((error) => logger.error(error));
			});
		channel.addEventListener("unicast", this._onFanoutUnicastFn);

		try {
			const channelOptions = this.getFanoutChannelOptions(options);
			if (resolvedRoot === fanoutService.publicKeyHash) {
				await channel.openAsRoot(channelOptions);
				return;
			}
			await channel.join(channelOptions, options.join);
		} catch (error) {
			this._closeFanoutChannel();
			throw error;
		}
	}

	private _closeFanoutChannel() {
		if (this._fanoutChannel) {
			if (this._onFanoutDataFn) {
				this._fanoutChannel.removeEventListener("data", this._onFanoutDataFn);
			}
			if (this._onFanoutUnicastFn) {
				this._fanoutChannel.removeEventListener(
					"unicast",
					this._onFanoutUnicastFn,
				);
			}
			this._fanoutChannel.close();
		}
		this._fanoutChannel = undefined;
	}

	private async _onFanoutData(detail: FanoutTreeDataEvent) {
		let envelope: FanoutEnvelope;
		try {
			envelope = deserialize(detail.payload, FanoutEnvelope);
		} catch (error) {
			if (error instanceof BorshError) {
				return;
			}
			throw error;
		}

		let message: TransportMessage;
		try {
			message = deserialize(envelope.payload, TransportMessage);
		} catch (error) {
			if (error instanceof BorshError) {
				return;
			}
			throw error;
		}

		if (!(message instanceof ExchangeHeadsMessage)) {
			return;
		}

		const from =
			(await this._resolvePublicKeyFromHash(envelope.from)) ??
			({ hashcode: () => envelope.from } as PublicSignKey);

		const contextMessage = new DataMessage({
			header: new MessageHeader({
				session: 0,
				mode: new AnyWhere(),
				priority: 0,
			}),
		});
		contextMessage.header.timestamp = envelope.timestamp;

		await this.onMessage(message, {
			from,
			message: contextMessage,
			transport: createRequestTransportContext(contextMessage),
		});
	}

	private async _onFanoutUnicast(detail: FanoutTreeUnicastEvent) {
		let message: TransportMessage;
		try {
			message = deserialize(detail.payload, TransportMessage);
		} catch (error) {
			if (error instanceof BorshError) {
				return;
			}
			throw error;
		}

		const fromHash = detail.origin || detail.from;
		const from =
			(await this._resolvePublicKeyFromHash(fromHash)) ??
			({ hashcode: () => fromHash } as PublicSignKey);

		const contextMessage = new DataMessage({
			header: new MessageHeader({
				session: 0,
				mode: new AnyWhere(),
				priority: 0,
			}),
		});
		contextMessage.header.timestamp = detail.timestamp;

		await this.onMessage(message, {
			from,
			message: contextMessage,
			transport: createRequestTransportContext(contextMessage),
		});
	}

	private async _publishExchangeHeadsViaFanout(
		message: ExchangeHeadsMessage<any>,
	): Promise<void> {
		if (!this._fanoutChannel) {
			throw new Error(
				`No fanout channel configured for shared-log topic ${this.topic}`,
			);
		}
		const envelope = new FanoutEnvelope({
			from: this.node.identity.publicKey.hashcode(),
			timestamp: BigInt(Date.now()),
			payload: serialize(message),
		});
		await this._fanoutChannel.publish(serialize(envelope));
	}

	private _parseDeliveryOptions(
		deliveryArg: false | true | DeliveryOptions | undefined,
	): {
		delivery?: DeliveryOptions;
		reliability: DeliveryReliability;
		requireRecipients: boolean;
		minAcks?: number;
		wrap?: (promise: Promise<void>) => Promise<void>;
	} {
		const delivery: DeliveryOptions | undefined =
			deliveryArg === undefined || deliveryArg === false
				? undefined
				: deliveryArg === true
					? { reliability: "ack" }
					: deliveryArg;
		if (!delivery) {
			return {
				delivery: undefined,
				reliability: "best-effort",
				requireRecipients: false,
				minAcks: undefined,
				wrap: undefined,
			};
		}

		const reliability: DeliveryReliability = delivery.reliability ?? "ack";
		const deliveryTimeout = delivery.timeout;
		const deliverySignal = delivery.signal;
		const requireRecipients = delivery.requireRecipients === true;
		const minAcks =
			delivery.minAcks != null && Number.isFinite(delivery.minAcks)
				? Math.max(0, Math.floor(delivery.minAcks))
				: undefined;

		const wrap =
			deliveryTimeout == null && deliverySignal == null
				? undefined
				: (promise: Promise<void>) =>
						new Promise<void>((resolve, reject) => {
							let settled = false;
							let timer: ReturnType<typeof setTimeout> | undefined = undefined;
							const onAbort = () => {
								if (settled) {
									return;
								}
								settled = true;
								promise.catch(() => {});
								cleanup();
								reject(new AbortError());
							};

							const cleanup = () => {
								if (timer != null) {
									clearTimeout(timer);
									timer = undefined;
								}
								deliverySignal?.removeEventListener("abort", onAbort);
							};

							if (deliverySignal) {
								if (deliverySignal.aborted) {
									onAbort();
									return;
								}
								deliverySignal.addEventListener("abort", onAbort);
							}

							if (deliveryTimeout != null) {
								timer = setTimeout(() => {
									if (settled) {
										return;
									}
									settled = true;
									promise.catch(() => {});
									cleanup();
									reject(new TimeoutError(`Timeout waiting for delivery`));
								}, deliveryTimeout);
							}

							promise
								.then(() => {
									if (settled) {
										return;
									}
									settled = true;
									cleanup();
									resolve();
								})
								.catch((error) => {
									if (settled) {
										return;
									}
									settled = true;
									cleanup();
									reject(error);
								});
						});

		return {
			delivery,
			reliability,
			requireRecipients,
			minAcks,
			wrap,
		};
	}

	private async _getSortedRouteHints(
		targetHash: string,
	): Promise<RouteHint[]> {
		const pubsub: any = this.node.services.pubsub as any;
		const maybeHints = await pubsub?.getUnifiedRouteHints?.(this.topic, targetHash);
		const hints: RouteHint[] = Array.isArray(maybeHints) ? maybeHints : [];
		const now = Date.now();
		return hints
			.filter((hint) => hint.expiresAt == null || hint.expiresAt > now)
			.sort((a, b) => {
				const rankA = a.kind === "directstream-ack" ? 0 : 1;
				const rankB = b.kind === "directstream-ack" ? 0 : 1;
				if (rankA !== rankB) {
					return rankA - rankB;
				}

				const costA =
					a.kind === "directstream-ack"
						? a.distance
						: Math.max(0, (a.route?.length ?? 1) - 1);
				const costB =
					b.kind === "directstream-ack"
						? b.distance
						: Math.max(0, (b.route?.length ?? 1) - 1);
				if (costA !== costB) {
					return costA - costB;
				}

				return (b.updatedAt ?? 0) - (a.updatedAt ?? 0);
			});
	}

	private async _sendAckWithUnifiedHints(properties: {
		peer: string;
		message: ExchangeHeadsMessage<any>;
		payload: Uint8Array;
		fanoutUnicastOptions?: { timeoutMs?: number; signal?: AbortSignal };
	}): Promise<void> {
		const { peer, message, payload, fanoutUnicastOptions } = properties;
		const hints = await this._getSortedRouteHints(peer);
		const hasDirectHint = hints.some((hint) => hint.kind === "directstream-ack");
		const fanoutHint = hints.find(
			(hint): hint is Extract<RouteHint, { kind: "fanout-token" }> =>
				hint.kind === "fanout-token",
		);

		if (hasDirectHint) {
			try {
				await this.rpc.send(message, {
					mode: new AcknowledgeDelivery({
						redundancy: 1,
						to: [peer],
					}),
				});
				return;
			} catch {
				// Fall back to fanout token/direct fanout unicast below.
			}
		}

		if (fanoutHint && this._fanoutChannel) {
			try {
				await this._fanoutChannel.unicastAck(
					fanoutHint.route,
					payload,
					fanoutUnicastOptions,
				);
				return;
			} catch {
				// Fall back below.
			}
		}

		if (this._fanoutChannel) {
			try {
				await this._fanoutChannel.unicastToAck(
					peer,
					payload,
					fanoutUnicastOptions,
				);
				return;
			} catch {
				// Fall back below.
			}
		}

		await this.rpc.send(message, {
			mode: new AcknowledgeDelivery({
				redundancy: 1,
				to: [peer],
			}),
		});
	}

		private async _appendDeliverToReplicators(
			entry: Entry<T>,
			coordinates: NumberFromType<R>[],
			minReplicasValue: number,
			leaders: Map<string, any>,
			selfHash: string,
			isLeader: boolean,
			deliveryArg: false | true | DeliveryOptions | undefined,
			nativeDeliveryPlan?: AppendDeliveryPlan,
		) {
			const { delivery, reliability, requireRecipients, minAcks, wrap } =
				this._parseDeliveryOptions(deliveryArg);
			const pending: Promise<void>[] = [];
			const track = (promise: Promise<void>) => {
				pending.push(wrap ? wrap(promise) : promise);
			};
			const fanoutUnicastOptions =
				delivery?.timeout != null || delivery?.signal != null
					? { timeoutMs: delivery.timeout, signal: delivery.signal }
					: undefined;

			if (!nativeDeliveryPlan) {
				const fullReplicaDeliveryCandidates =
					await this.getFullReplicaRepairCandidates(undefined, {
						includeSubscribers: false,
					});
				if (minReplicasValue >= Math.max(1, fullReplicaDeliveryCandidates.size)) {
					for (const peer of fullReplicaDeliveryCandidates) {
						if (!leaders.has(peer)) {
							leaders.set(peer, { intersecting: true });
						}
					}
				}
			}

			const entryReplicatedForRepair = this.createEntryReplicatedForRepair({
				entry,
				coordinates,
				leaders: leaders as Map<string, { intersecting: boolean }>,
				replicas: minReplicasValue,
			});
			for await (const message of createExchangeHeadsMessages(this.log, [entry])) {
				const leaderCountBeforeReferenceMerge = leaders.size;
				await this._mergeLeadersFromGidReferences(message, minReplicasValue, leaders);
				const canUseNativeDeliveryPlan =
					!!nativeDeliveryPlan &&
					nativeDeliveryPlan.hasRemoteRecipients &&
					leaders.size === leaderCountBeforeReferenceMerge;
				if (canUseNativeDeliveryPlan) {
					if (!delivery) {
						for (const peer of nativeDeliveryPlan.repairTargets) {
							this.queueAppendBackfill(peer, entryReplicatedForRepair);
						}
						this.rpc
							.send(message, {
								mode: nativeDeliveryPlan.defaultSendSilent
									? new SilentDelivery({
											redundancy: 1,
											to: nativeDeliveryPlan.sendTo,
										})
									: new AcknowledgeDelivery({
											redundancy: 1,
											to: nativeDeliveryPlan.sendTo,
										}),
							})
							.catch((error) => logger.error(error));
						continue;
					}

					if (requireRecipients && nativeDeliveryPlan.noPeerError) {
						throw new NoPeersError(this.rpc.topic);
					}

					if (nativeDeliveryPlan.ackTo.length > 0) {
						const payload = serialize(message);
						for (const peer of nativeDeliveryPlan.ackTo) {
							track(
								(async () => {
									await this._sendAckWithUnifiedHints({
										peer,
										message,
										payload,
										fanoutUnicastOptions,
									});
								})(),
							);
						}
					}

					if (nativeDeliveryPlan.silentTo.length > 0) {
						this.rpc
							.send(message, {
								mode: new SilentDelivery({
									redundancy: 1,
									to: nativeDeliveryPlan.silentTo,
								}),
							})
							.catch((error) => logger.error(error));
					}
					for (const peer of nativeDeliveryPlan.repairTargets) {
						this.queueAppendBackfill(peer, entryReplicatedForRepair);
					}
					continue;
				}

				const authoritativeRecipients = new Set(leaders.keys());
				const leadersForDelivery = delivery
					? new Set(authoritativeRecipients)
					: undefined;

				// Outbound append delivery only tells us who we intend to send to, not who has
				// actually stored the entry. Keep this recipient set local so later repair
				// sweeps can still backfill peers that missed the initial delivery.
				const set = new Set(leaders.keys());
				let hasRemotePeers = set.has(selfHash) ? set.size > 1 : set.size > 0;
				const allowSubscriberFallback =
					this.syncronizer instanceof SimpleSyncronizer ||
					(this.compatibility ?? Number.MAX_VALUE) < 10;
				if (!hasRemotePeers && allowSubscriberFallback) {
					try {
						const subscribers = await this._getTopicSubscribers(this.topic);
						if (subscribers && subscribers.length > 0) {
							for (const subscriber of subscribers) {
								const hash = subscriber.hashcode();
								if (hash === selfHash) {
									continue;
								}
								set.add(hash);
								leadersForDelivery?.add(hash);
							}
							hasRemotePeers = set.has(selfHash) ? set.size > 1 : set.size > 0;
						}
					} catch {
						// Best-effort only; keep discovered recipients as-is.
					}
				}
				if (!hasRemotePeers) {
					if (requireRecipients) {
						throw new NoPeersError(this.rpc.topic);
					}
				continue;
			}

				if (!delivery) {
					for (const peer of authoritativeRecipients) {
						if (peer === selfHash) {
							continue;
						}
						// Default live append delivery is still optimistic. If one remote misses
						// the initial heads exchange and the caller did not opt into explicit
						// delivery acks, we still need a targeted backfill source of truth for the
						// authoritative recipients or one entry can get stuck at 2/3 replicas
						// forever. Best-effort fallback subscribers are not repair-worthy.
						this.queueAppendBackfill(peer, entryReplicatedForRepair);
					}
					this.rpc
						.send(message, {
							mode: isLeader
								? new SilentDelivery({ redundancy: 1, to: set })
								: new AcknowledgeDelivery({ redundancy: 1, to: set }),
						})
						.catch((error) => logger.error(error));
					continue;
				}

				const orderedRemoteRecipients: string[] = [];
				for (const peer of leadersForDelivery!) {
					if (peer === selfHash) {
						continue;
					}
					orderedRemoteRecipients.push(peer);
				}
				for (const peer of set) {
					if (peer === selfHash) {
						continue;
					}
					if (leadersForDelivery!.has(peer)) {
						continue;
					}
					orderedRemoteRecipients.push(peer);
				}

				const ackTo: string[] = [];
				let silentTo: string[] | undefined;
				const repairTargets = new Set<string>();
				// Default delivery semantics: require enough remote ACKs to reach the requested
				// replication degree (local append counts as 1).
				const defaultMinAcks = Math.max(0, minReplicasValue - 1);
				const ackLimitRaw =
					reliability === "ack" ? (minAcks ?? defaultMinAcks) : 0;
				const ackLimit = Math.max(
					0,
					Math.min(Math.floor(ackLimitRaw), orderedRemoteRecipients.length),
				);

				for (const peer of orderedRemoteRecipients) {
					if (authoritativeRecipients.has(peer)) {
						repairTargets.add(peer);
					}
					if (ackTo.length < ackLimit) {
						ackTo.push(peer);
					} else {
						silentTo ||= [];
						silentTo.push(peer);
					}
				}

				if (requireRecipients && orderedRemoteRecipients.length === 0) {
					throw new NoPeersError(this.rpc.topic);
				}
				if (requireRecipients && ackTo.length + (silentTo?.length || 0) === 0) {
					throw new NoPeersError(this.rpc.topic);
				}

			if (ackTo.length > 0) {
				const payload = serialize(message);
				for (const peer of ackTo) {
					track(
						(async () => {
							await this._sendAckWithUnifiedHints({
								peer,
								message,
								payload,
								fanoutUnicastOptions,
							});
						})(),
					);
				}
			}

			if (silentTo?.length) {
				this.rpc
					.send(message, {
						mode: new SilentDelivery({ redundancy: 1, to: silentTo }),
					})
					.catch((error) => logger.error(error));
			}
				for (const peer of repairTargets) {
					// Direct append delivery is intentionally optimistic. Queue one delayed,
					// batched maybe-sync pass for the intended recipients so stable 3-peer
					// append workloads do not depend on perfect first-try delivery ordering.
					this.queueAppendBackfill(peer, entryReplicatedForRepair);
				}
		}

		if (pending.length > 0) {
			await Promise.all(pending);
		}
	}

	private async _mergeLeadersFromGidReferences(
		message: ExchangeHeadsMessage<any>,
		minReplicasValue: number,
		leaders: Map<string, any>,
	) {
		const gidReferences = message.heads[0]?.gidRefrences;
		if (!gidReferences || gidReferences.length === 0) {
			return;
		}

		for (const gidReference of gidReferences) {
			const entryFromGid = this.log.entryIndex.getHeads(gidReference, false);
			for (const gidEntry of await entryFromGid.all()) {
				let coordinates = await this.getCoordinates(gidEntry);
				let found: Map<string, { intersecting: boolean }>;
				if (coordinates == null) {
					found = await this.findLeadersFromEntry(gidEntry, minReplicasValue);
				} else {
					found = await this._findLeaders(coordinates);
				}

				for (const [key, value] of found) {
					leaders.set(key, value);
				}
			}
		}
	}

	private async _appendDeliverToAllFanout(entry: Entry<T>) {
		for await (const message of createExchangeHeadsMessages(this.log, [entry])) {
			await this._publishExchangeHeadsViaFanout(message);
		}
	}

	private async _resolvePublicKeyFromHash(
		hash: string,
	): Promise<PublicSignKey | undefined> {
		const fanoutService = getSharedLogFanoutService(this.node.services);
		return (
			fanoutService?.getPublicKey?.(hash) ??
			this.node.services.pubsub.getPublicKey(hash)
		);
	}

	private async _getTopicSubscribers(
		topic: string,
	): Promise<PublicSignKey[] | undefined> {
		const cached = this._topicSubscribersCache.get(topic);
		if (cached && cached.expiresAt > Date.now()) {
			return cached.keys.slice();
		}

		const maxPeers = 64;
		const cache = (keys: PublicSignKey[]) => {
			this._topicSubscribersCache.set(topic, {
				expiresAt: Date.now() + TOPIC_SUBSCRIBERS_CACHE_TTL_MS,
				keys,
			});
			return keys.slice();
		};

		const selfHash = this.node.identity.publicKey.hashcode();
		const hashes = new Set<string>();
		const keysByHash = new Map<string, PublicSignKey>();
		const addHash = (hash: string | undefined) => {
			if (!hash || hash === selfHash || keysByHash.has(hash)) {
				return;
			}
			hashes.add(hash);
		};
		const addKey = (key: PublicSignKey | undefined) => {
			if (!key) {
				return;
			}
			const hash = key.hashcode();
			if (hash === selfHash) {
				return;
			}
			hashes.delete(hash);
			keysByHash.set(hash, key);
		};

		// Fanout is a useful hint, but it can lag direct pubsub connectivity. Keep
		// collecting other local views instead of treating an empty fanout snapshot as
		// authoritative absence.
		if (this._fanoutChannel && (topic === this.topic || topic === this.rpc.topic)) {
			for (const hash of this._fanoutChannel.getPeerHashes({
				includeSelf: false,
			})) {
				addHash(hash);
				if (hashes.size + keysByHash.size >= maxPeers) break;
			}
		}

		// Already-connected peer streams are cheap and are the strongest local signal
		// when fanout/provider membership is stale.
		const peerMap: Map<string, { publicKey?: PublicSignKey }> | undefined = (this.node
			.services.pubsub as any)?.peers;
		if (peerMap?.entries) {
			for (const [hash, peer] of peerMap.entries()) {
				addKey(peer?.publicKey);
				addHash(hash);
				if (hashes.size + keysByHash.size >= maxPeers) break;
			}
		}

		// Libp2p connections cover bootstrap/direct peers even before a higher-level
		// topic subscriber snapshot has converged.
		if (hashes.size + keysByHash.size < maxPeers) {
			const connectionManager = (this.node.services.pubsub as any)?.components
				?.connectionManager;
			const connections = connectionManager?.getConnections?.() ?? [];
			for (const conn of connections) {
				const peerId = conn?.remotePeer;
				if (!peerId) continue;
				try {
					addKey(getPublicKeyFromPeerId(peerId));
					if (hashes.size + keysByHash.size >= maxPeers) break;
				} catch {
					// Best-effort only.
				}
			}
		}

		// Best-effort provider discovery (bounded). This requires bootstrap trackers.
		if (hashes.size + keysByHash.size < maxPeers) {
			try {
				const fanoutService = getSharedLogFanoutService(this.node.services);
				if (fanoutService?.queryProviders) {
					const ns = `shared-log|${this.topic}`;
					const seed = hashToSeed32(topic);
					const providers: string[] = await fanoutService.queryProviders(ns, {
						want: maxPeers - keysByHash.size - hashes.size,
						seed,
					});
					for (const hash of providers ?? []) {
						addHash(hash);
						if (hashes.size + keysByHash.size >= maxPeers) break;
					}
				}
			} catch {
				// Best-effort only.
			}
		}

		if (hashes.size === 0 && keysByHash.size === 0) return cache([]);

		const unresolvedHashes = [...hashes].slice(
			0,
			Math.max(0, maxPeers - keysByHash.size),
		);
		const keys = await Promise.all(
			unresolvedHashes.map((hash) => this._resolvePublicKeyFromHash(hash)),
		);
		for (const key of keys) {
			addKey(key);
		}
		return cache([...keysByHash.values()].slice(0, maxPeers));
	}

	private invalidateTopicSubscribersCache(...topics: (string | undefined)[]) {
		for (const topic of topics) {
			if (!topic) continue;
			this._topicSubscribersCache.delete(topic);
		}
	}

	private invalidateSharedLogTopicSubscribersCache() {
		this.invalidateTopicSubscribersCache(this.topic, this.rpc.topic);
	}

	// @deprecated
	private async getRole() {
		const segments = await this.getMyReplicationSegments();
		if (segments.length > 1) {
			throw new Error(
				"More than one replication segment found. Can only use one segment for compatbility with v8",
			);
		}

		if (segments.length > 0) {
			const segment = segments[0].toReplicationRange();
			return new Replicator({
				// TODO types
				factor: (segment.factor as number) / MAX_U32,
				offset: (segment.offset as number) / MAX_U32,
			});
		}

		// TODO this is not accurate but might be good enough
		return new Observer();
	}

	async isReplicating() {
		if (!this._isReplicating) {
			return false;
		}
		return (await this.countReplicationSegments()) > 0;
	}

	private setupRebalanceDebounceFunction(
		interval = RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL,
	) {
		this.rebalanceParticipationDebounced = undefined;

		this.rebalanceParticipationDebounced = debounceFixedInterval(
			() => this.rebalanceParticipation(),
			/* Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.getSize() || 0) *
					REBALANCE_DEBOUNCE_INTERVAL
				)
			) */
			interval, // TODO make this dynamic on the number of replicators
		);
	}

	private markLocalAppendActivity(timestamp = Date.now()) {
		this._lastLocalAppendAt = Math.max(this._lastLocalAppendAt ?? 0, timestamp);
	}

	private shouldDelayAdaptiveRebalance(now = Date.now()) {
		return (
			this._isAdaptiveReplicating &&
			this._lastLocalAppendAt > 0 &&
			now - this._lastLocalAppendAt < this.adaptiveRebalanceIdleMs
		);
	}

	private shouldDeferHeadCoordinatePersistence(
		options?: SharedAppendOptions<T>,
	) {
		return (
			!this._isReplicating &&
			options?.replicate === false &&
			options?.target === "none"
		);
	}

	private async deleteCoordinatesForHashes(hashes: Iterable<string>) {
		const values = [...new Set([...hashes].filter(Boolean))];
		if (values.length === 0) {
			return;
		}
		this._nativeSharedLogState?.deleteEntryCoordinatesBatch(values);
		if (this._residentEntryCoordinatesByHash) {
			for (const hash of values) {
				this._residentEntryCoordinatesByHash.delete(hash);
			}
		}
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIds) {
			await coordinateIndex.delIds(values);
			return;
		}
		await this.entryCoordinatesIndex.del({
			query:
				values.length === 1
					? { hash: values[0] }
					: new Or(
							values.map((hash) => new StringMatch({ key: "hash", value: hash })),
						),
		});
	}

	private async ensureCurrentHeadCoordinatesIndexed() {
		const heads = await this.log.getHeads(true).all();
		const headsByHash = new Map(heads.map((head) => [head.hash, head]));
		const nativeHashes = this._nativeSharedLogState?.getEntryCoordinateHashes();
		const indexedHashes = nativeHashes
			? new Set(nativeHashes)
			: new Set(
					(
						await this.entryCoordinatesIndex
							.iterate({}, { shape: { hash: true } })
							.all()
					).map((entry) => entry.value.hash),
				);
		const staleHashes = [...indexedHashes].filter((hash) => !headsByHash.has(hash));

		if (staleHashes.length > 0) {
			await this.deleteCoordinatesForHashes(staleHashes);
		}

		const missingHeads: EntryLeaderBatchItem<R>[] = [];
		for (const head of heads) {
			if (indexedHashes.has(head.hash)) {
				continue;
			}
			missingHeads.push({
				entry: head,
				replicas: decodeReplicas(head).getValue(this),
				options: { persist: {} },
			});
		}

		if (missingHeads.length > 0) {
			await this.planEntryLeaderBatch(missingHeads);
		}
	}

	private async _replicate(
		options?: ReplicationOptions<R>,
		{
			reset,
			checkDuplicates,
			announce,
			mergeSegments,
			rebalance,
		}: {
			reset?: boolean;
			checkDuplicates?: boolean;
			mergeSegments?: boolean;
			rebalance?: boolean;
			announce?: (
				msg: AddedReplicationSegmentMessage | AllReplicatingSegmentsMessage,
			) => void;
		} = {},
	): Promise<ReplicationRangeIndexable<R>[]> {
		let offsetWasProvided = false;
		if (isUnreplicationOptions(options)) {
			await this.unreplicate();
			return [];
		}
		if ((options as ExistingReplicationOptions).type === "resume") {
			options = (options as ExistingReplicationOptions)
				.default as ReplicationOptions<R>;
		}

		let rangesToReplicate: ReplicationRangeIndexable<R>[] = [];
		let rangesToUnreplicate: ReplicationRangeIndexable<R>[] = [];

		if (options == null) {
			options = {};
		} else if (options === true) {
			options = {};
		}

		this._isReplicating = true;

		if (isAdaptiveReplicatorOption(options!)) {
			this._isAdaptiveReplicating = true;
			this.setupDebouncedRebalancing(options);

			// initial role in a dynamic setup
			const maybeRange = await this.getDynamicRange();
			if (!maybeRange) {
				// not allowed
				return [];
			}
			rangesToReplicate = [maybeRange];

			offsetWasProvided = true;
		} else if (isReplicationRangeMessage(options)) {
			rangesToReplicate = [
				options.toReplicationRangeIndexable(this.node.identity.publicKey),
			];

			offsetWasProvided = true;
		} else {
			let rangeArgs: FixedReplicationOptions[];
			if (typeof options === "number") {
				rangeArgs = [
					{
						factor: options,
					} as FixedReplicationOptions,
				];
			} else {
				const fixed = options as
					| FixedReplicationOptions
					| FixedReplicationOptions[];
				rangeArgs = Array.isArray(fixed)
					? fixed
					: [{ ...(fixed as FixedReplicationOptions) }];
			}

			if (rangeArgs.length === 0) {
				// nothing to do
				return [];
			}

			for (const rangeArg of rangeArgs) {
				let timestamp: bigint | undefined = undefined;
				if (rangeArg.id != null) {
					// fetch the previous timestamp if it exists
					const indexed = await this.replicationIndex.get(toId(rangeArg.id), {
						shape: { id: true, timestamp: true },
					});
					if (indexed) {
						timestamp = indexed.value.timestamp;
					}
				}
				const normalized = rangeArg.normalized ?? true;
				offsetWasProvided = rangeArg.offset != null;
				const offset =
					rangeArg.offset != null
						? normalized
							? this.indexableDomain.numbers.denormalize(
									rangeArg.offset as number,
								)
							: rangeArg.offset
						: this.indexableDomain.numbers.random();
				let factor = rangeArg.factor;
				let fullWidth = this.indexableDomain.numbers.maxValue;

				let factorDenormalized = !normalized
					? factor
					: this.indexableDomain.numbers.denormalize(factor as number);
				rangesToReplicate.push(
					new this.indexableDomain.constructorRange({
						id: rangeArg.id,
						// @ts-ignore
						offset: offset,
						// @ts-ignore
						width: (factor === "all"
							? fullWidth
							: factor === "right"
								? // @ts-ignore
									fullWidth - offset
								: factorDenormalized) as NumberFromType<R>,
						publicKeyHash: this.node.identity.publicKey.hashcode(),
						mode: rangeArg.strict
							? ReplicationIntent.Strict
							: ReplicationIntent.NonStrict, // automatic means that this range might be reused later for dynamic replication behaviour
						timestamp: timestamp ?? BigInt(+new Date()),
					}),
				);
			}

			if (mergeSegments) {
				let range =
					rangesToReplicate.length > 1
						? mergeRanges(rangesToReplicate, this.indexableDomain.numbers)
						: rangesToReplicate[0];

				// also merge segments that are already in the index
				if (this.domain.canMerge) {
					const mergeRangesThatAlreadyExist = await getAllMergeCandiates(
						this.replicationIndex,
						range,
						this.indexableDomain.numbers,
					);
					const mergeableFiltered: ReplicationRangeIndexable<R>[] = [];
					const toKeep: Set<string> = new Set();

					for (const [_key, mergeCandidate] of mergeRangesThatAlreadyExist) {
						if (this.domain.canMerge(mergeCandidate, range)) {
							mergeableFiltered.push(mergeCandidate);
						} else {
							toKeep.add(mergeCandidate.idString);
						}
					}

					mergeableFiltered.push(range); // * we push this last, because mergeRanges will reuse ids of the first elements
					if (mergeableFiltered.length > 1) {
						// ** this is important here as we want to reuse ids of what we already persist, not the new ranges, so we dont get a delet add op, but just a update op
						range = mergeRanges(
							mergeableFiltered,
							this.indexableDomain.numbers,
						);
					}
					for (const [_key, mergeCandidate] of mergeRangesThatAlreadyExist) {
						if (
							mergeCandidate.idString !== range.idString &&
							!toKeep.has(mergeCandidate.idString)
						) {
							rangesToUnreplicate.push(mergeCandidate);
						}
					}
				}
				rangesToReplicate = [range];
			}
		}

		for (const range of rangesToReplicate) {
			this.oldestOpenTime = Math.min(
				Number(range.timestamp),
				this.oldestOpenTime,
			);
		}

		let resetRanges = reset;
		if (!resetRanges && !offsetWasProvided) {
			resetRanges = true;
			// because if we do something like replicate ({ factor: 0.5 }) it means that we want to replicate 50%
			// but ({ replicate: 0.5, offset: 0.5 }) means that we want to add a range
			// TODO make behaviour more clear
		}
		if (rangesToUnreplicate.length > 0) {
			await this.removeReplicationRanges(
				rangesToUnreplicate,
				this.node.identity.publicKey,
			);
		}

		await this.startAnnounceReplicating(rangesToReplicate, {
			reset: resetRanges ?? false,
			checkDuplicates,
			announce,
			rebalance,
		});

		if (rangesToUnreplicate.length > 0) {
			await this.rpc.send(
				new StoppedReplicating({
					segmentIds: rangesToUnreplicate.map((x) => x.id),
				}),
				{
					priority: 1,
				},
			);
		}

		return rangesToReplicate;
	}

	setupDebouncedRebalancing(options?: DynamicReplicationOptions<R>) {
		this.cpuUsage?.stop?.();

		this.replicationController = new PIDReplicationController(
			this.node.identity.publicKey.hashcode(),
			{
				storage:
					options?.limits?.storage != null
						? { max: options?.limits?.storage }
						: undefined,
				cpu:
					options?.limits?.cpu != null
						? {
								max:
									typeof options?.limits?.cpu === "object"
										? options.limits.cpu.max
										: options?.limits?.cpu,
							}
						: undefined,
			},
		);

		this.cpuUsage =
			options?.limits?.cpu && typeof options?.limits?.cpu === "object"
				? options?.limits?.cpu?.monitor || new CPUUsageIntervalLag()
				: new CPUUsageIntervalLag();
		this.cpuUsage?.start?.();
		this.setupRebalanceDebounceFunction(options?.limits?.interval);
	}

	async replicate(
		rangeOrEntry?: ReplicationOptions<R> | Entry<T> | Entry<T>[],
		options?: {
			reset?: boolean;
			checkDuplicates?: boolean;
			rebalance?: boolean;
			mergeSegments?: boolean;
			announce?: (
				msg: AllReplicatingSegmentsMessage | AddedReplicationSegmentMessage,
			) => void;
		},
	) {
		const entryRangeId = (entry: Entry<T>) =>
			sha256Sync(
				concat([
					this.log.id,
					fromString(entry.hash),
					fromString(this.node.identity.publicKey.hashcode()),
				]),
			);
		let range:
			| ReplicationRangeMessage<any>[]
			| ReplicationOptions<R>
			| undefined = undefined;

		if (rangeOrEntry instanceof ReplicationRangeMessage) {
			range = rangeOrEntry;
		} else if (rangeOrEntry instanceof Entry) {
			range = {
				id: entryRangeId(rangeOrEntry),
				factor: 1,
				offset: await this.domain.fromEntry(rangeOrEntry),
				normalized: false,
			};
		} else if (Array.isArray(rangeOrEntry)) {
			let ranges: (ReplicationRangeMessage<any> | FixedReplicationOptions)[] =
				[];
			for (const entry of rangeOrEntry) {
				if (entry instanceof Entry) {
					ranges.push({
						id: entryRangeId(entry),
						factor: 1,
						offset: await this.domain.fromEntry(entry),
						normalized: false,
						strict: true,
					});
				} else {
					ranges.push(entry);
				}
			}
			range = ranges;
		} else if (
			rangeOrEntry &&
			(rangeOrEntry as ExistingReplicationOptions<R>).type === "resume"
		) {
			range = (rangeOrEntry as ExistingReplicationOptions<R>).default;
		} else {
			range = rangeOrEntry ?? true;
		}

		return this._replicate(range, options);
	}

	async unreplicate(rangeOrEntry?: Entry<T> | { id: Uint8Array }[]) {
		let segmentIds: Uint8Array[];
		if (rangeOrEntry instanceof Entry) {
			let range: FixedReplicationOptions = {
				factor: 1,
				offset: await this.domain.fromEntry(rangeOrEntry),
			};
			const indexed = this.replicationIndex.iterate({
				query: {
					width: 1,
					start1: range.offset /* ,
					hash: this.node.identity.publicKey.hashcode(), */,
				},
			});
			segmentIds = (await indexed.all()).map((x) => x.id.key as Uint8Array);
			if (segmentIds.length === 0) {
				warn("No segment found to unreplicate");
				return;
			}
		} else if (Array.isArray(rangeOrEntry)) {
			segmentIds = rangeOrEntry.map((x) => x.id);
			if (segmentIds.length === 0) {
				warn("No segment found to unreplicate");
				return;
			}
		} else {
			this._isReplicating = false;
			this._isAdaptiveReplicating = false;
			await this.removeReplicator(this.node.identity.publicKey);
			return;
		}

		if (this._isAdaptiveReplicating) {
			// we can not unreplicate individual ranges when dynamically replicating (yet)
			// TODO support this by never deleting the range with the segment id that is generated by the dynamic replication method
			throw new Error("Unsupported when adaptive replicating");
		}

		const rangesToRemove = await this.resolveReplicationRangesFromIdsAndKey(
			segmentIds,
			this.node.identity.publicKey,
		);
		await this.removeReplicationRanges(
			rangesToRemove,
			this.node.identity.publicKey,
		);
		await this.rpc.send(new StoppedReplicating({ segmentIds }), {
			priority: 1,
		});
	}

	private async removeReplicator(
		key: PublicSignKey | string,
		options?: { noEvent?: boolean },
	) {
		const keyHash = typeof key === "string" ? key : key.hashcode();
		const deleted = await this.replicationIndex
			.iterate({
				query: { hash: keyHash },
			})
			.all();

			this.uniqueReplicators.delete(keyHash);
			this._replicatorJoinEmitted.delete(keyHash);
			await this.replicationIndex.del({ query: { hash: keyHash } });
			for (const result of deleted) {
				this.deleteNativeReplicationRange(result.value);
			}

		await this.updateOldestTimestampFromIndex();

		const isMe = this.node.identity.publicKey.hashcode() === keyHash;
		if (isMe) {
			// announce that we are no longer replicating

			await this.rpc.send(new AllReplicatingSegmentsMessage({ segments: [] }), {
				priority: 1,
			});
		}

		if (options?.noEvent !== true) {
			const publicKey = toLocalPublicSignKey(key);
			if (publicKey) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicationChangeEvent>("replication:change", {
						detail: { publicKey },
					}),
				);
			} else {
				throw new Error("Key was not a PublicSignKey");
			}
		}

			const timestamp = BigInt(+new Date());
			for (const x of deleted) {
				this.replicationChangeDebounceFn.add({
					range: x.value,
					type: "removed",
					timestamp,
				});
			}

		const pendingMaturity = this.pendingMaturity.get(keyHash);
		if (pendingMaturity) {
			for (const [_k, v] of pendingMaturity) {
				clearTimeout(v.timeout);
			}
			this.pendingMaturity.delete(keyHash);
		}

		// Keep local sync/prune state consistent even when a peer disappears
		// through replication-info updates without a topic unsubscribe event.
		this.removePeerFromGidPeerHistory(keyHash);
		this.removeRepairFrontierTarget(keyHash);
		this._recentRepairDispatch.delete(keyHash);
		if (!isMe) {
			this.syncronizer.onPeerDisconnected(keyHash);
		}

		if (!isMe) {
			this.rebalanceParticipationDebounced?.call();
		}
	}

	private async updateOldestTimestampFromIndex() {
		const iterator = await this.replicationIndex.iterate(
			{
				sort: [new Sort({ key: "timestamp", direction: "asc" })],
			},
			{ reference: true },
		);
		const oldestTimestampFromDB = (await iterator.next(1))[0]?.value.timestamp;
		await iterator.close();

		this.oldestOpenTime =
			oldestTimestampFromDB != null
				? Number(oldestTimestampFromDB)
				: +new Date();
	}

	private async resolveReplicationRangesFromIdsAndKey(
		ids: Uint8Array[],
		from: PublicSignKey,
	) {
		let idMatcher = new Or(
			ids.map((x) => new ByteMatchQuery({ key: "id", value: x })),
		);

		// make sure we are not removing something that is owned by the replicator
		let identityMatcher = new StringMatch({
			key: "hash",
			value: from.hashcode(),
		});

		let query = new And([idMatcher, identityMatcher]);
		return (await this.replicationIndex.iterate({ query }).all()).map(
			(x) => x.value,
		);
	}
	private async removeReplicationRanges(
		ranges: ReplicationRangeIndexable<R>[],
		from: PublicSignKey,
	) {
		if (ranges.length === 0) {
			return;
		}
		const pendingMaturity = this.pendingMaturity.get(from.hashcode());
		if (pendingMaturity) {
			for (const id of ranges) {
				const info = pendingMaturity.get(id.toString());
				if (info) {
					clearTimeout(info.timeout);
					pendingMaturity.delete(id.toString());
				}
			}
			if (pendingMaturity.size === 0) {
				this.pendingMaturity.delete(from.hashcode());
			}
		}

		await this.replicationIndex.del({
			query: new Or(
				ranges.map((x) => new ByteMatchQuery({ key: "id", value: x.id })),
			),
		});
		for (const range of ranges) {
			this.deleteNativeReplicationRange(range);
		}

		const otherSegmentsIterator = this.replicationIndex.iterate(
			{ query: { hash: from.hashcode() } },
			{ shape: { id: true } },
		);
			if ((await otherSegmentsIterator.next(1)).length === 0) {
				this.uniqueReplicators.delete(from.hashcode());
				this._replicatorJoinEmitted.delete(from.hashcode());
			}
		await otherSegmentsIterator.close();

		await this.updateOldestTimestampFromIndex();

		this.events.dispatchEvent(
			new CustomEvent<ReplicationChangeEvent>("replication:change", {
				detail: { publicKey: from },
			}),
		);

		if (!from.equals(this.node.identity.publicKey)) {
			this.rebalanceParticipationDebounced?.call();
		}
	}

	private async addReplicationRange(
		ranges: ReplicationRangeIndexable<any>[],
		from: PublicSignKey,
		{
			reset,
			checkDuplicates,
			timestamp: ts,
			rebalance,
		}: {
			reset?: boolean;
			rebalance?: boolean;
			checkDuplicates?: boolean;
			timestamp?: number;
		} = {},
	) {
		if (this._isTrustedReplicator && !(await this._isTrustedReplicator(from))) {
			return undefined;
		}
		let isNewReplicator = false;
		let timestamp = BigInt(ts ?? +new Date());
		rebalance = rebalance == null ? true : rebalance;

		let diffs: ReplicationChanges<ReplicationRangeIndexable<R>>;
		let deleted: ReplicationRangeIndexable<R>[] | undefined = undefined;
		let isStoppedReplicating = false;
		if (reset) {
			deleted = (
				await this.replicationIndex
					.iterate({
						query: { hash: from.hashcode() },
					})
					.all()
			).map((x) => x.value);

			let prevCount = deleted.length;

			const existingById = new Map(deleted.map((x) => [x.idString, x]));
			const hasSameRanges =
				deleted.length === ranges.length &&
				ranges.every((range) => {
					const existing = existingById.get(range.idString);
					return existing != null && existing.equalRange(range);
				});

			// Avoid churn on repeated full-state announcements that don't change any
			// replication ranges. This prevents unnecessary `replication:change`
			// events and rebalancing cascades.
			if (hasSameRanges) {
				diffs = [];
			} else {
				await this.replicationIndex.del({ query: { hash: from.hashcode() } });

				diffs = [
					...deleted.map((x) => {
						return { range: x, type: "removed" as const, timestamp };
					}),
					...ranges.map((x) => {
						return { range: x, type: "added" as const, timestamp };
					}),
				];
			}

			isNewReplicator = prevCount === 0 && ranges.length > 0;
			isStoppedReplicating = prevCount > 0 && ranges.length === 0;
		} else {
			let batchSize = 100;
			let existing: ReplicationRangeIndexable<R>[] = [];
			for (let i = 0; i < ranges.length; i += batchSize) {
				const results = await this.replicationIndex
					.iterate(
						{
							query: (ranges.length <= batchSize
								? ranges
								: ranges.slice(i, i + batchSize)
							).map((x) => new ByteMatchQuery({ key: "id", value: x.id })),
						},
						{ reference: true },
					)
					.all();
				for (const result of results) {
					existing.push(result.value);
				}
			}

			let prevCountForOwner: number | undefined = undefined;
			if (existing.length === 0) {
				prevCountForOwner = await this.replicationIndex.count({
					query: new StringMatch({ key: "hash", value: from.hashcode() }),
				});
				isNewReplicator = prevCountForOwner === 0;
			} else {
				isNewReplicator = false;
			}

			if (
				checkDuplicates &&
				(existing.length > 0 || (prevCountForOwner ?? 0) > 0)
			) {
				let deduplicated: ReplicationRangeIndexable<any>[] = [];

				// TODO also deduplicate/de-overlap among the ranges that ought to be inserted?
				for (const range of ranges) {
					if (
						!(await countCoveringRangesSameOwner(this.replicationIndex, range))
					) {
						deduplicated.push(range);
					}
				}
				ranges = deduplicated;
			}
			let existingMap = new Map<string, ReplicationRangeIndexable<any>>();
			for (const result of existing) {
				existingMap.set(result.idString, result);
			}

			let changes: ReplicationChanges<ReplicationRangeIndexable<R>> = ranges
				.map((x) => {
					const prev = existingMap.get(x.idString);
					if (prev) {
						if (prev.equalRange(x)) {
							return [];
						}
						return [
							{
								range: prev,
								timestamp: x.timestamp - 1n,
								prev,
								type: "replaced" as const,
							},
							{
								range: x,
								timestamp: x.timestamp,
								type: "added" as const,
							},
						];
					} else {
						return {
							range: x,
							timestamp: x.timestamp,
							type: "added" as const,
						};
					}
				})
				.flat() as ReplicationChanges<ReplicationRangeIndexable<R>>;
			diffs = changes;
		}

			const fromHash = from.hashcode();
			// Track replicator membership transitions synchronously so join/leave events are
			// idempotent even if we process concurrent reset messages/unsubscribes.
			const stoppedTransition =
				ranges.length === 0 ? this.uniqueReplicators.delete(fromHash) : false;
			if (ranges.length === 0) {
				this._replicatorJoinEmitted.delete(fromHash);
			} else {
				this.uniqueReplicators.add(fromHash);
			}

		let now = +new Date();
		let minRoleAge = await this.getDefaultMinRoleAge();
		let isAllMature = true;

		for (const diff of diffs) {
			if (diff.type === "added") {
				/* if (this.closed) {
					return;
				} */
				await this.replicationIndex.put(diff.range);
				this.putNativeReplicationRange(diff.range);

				if (!reset) {
					this.oldestOpenTime = Math.min(
						Number(diff.range.timestamp),
						this.oldestOpenTime,
					);
				}

				const isMature = isMatured(diff.range, now, minRoleAge);

				if (
					!isMature /* && diff.range.hash !== this.node.identity.publicKey.hashcode() */
				) {
					// second condition is to avoid the case where we are adding a range that we own
					isAllMature = false;
					let pendingRanges = this.pendingMaturity.get(diff.range.hash);
					if (!pendingRanges) {
						pendingRanges = new Map();
						this.pendingMaturity.set(diff.range.hash, pendingRanges);
					}

					let waitForMaturityTime = Math.max(
						minRoleAge - (now - Number(diff.range.timestamp)),
						0,
					);

					const setupTimeout = () =>
						setTimeout(async () => {
							this.events.dispatchEvent(
								new CustomEvent<ReplicationChangeEvent>("replicator:mature", {
									detail: { publicKey: from },
								}),
							);

								if (rebalance && diff.range.mode !== ReplicationIntent.Strict) {
									// TODO this statement (might) cause issues with triggering pruning if the segment is strict and maturity timings will affect the outcome of rebalancing
									this.replicationChangeDebounceFn.add({
										...diff,
										matured: true,
									}); // we need to call this here because the outcom of findLeaders will be different when some ranges become mature, i.e. some of data we own might be prunable!
								}
							pendingRanges.delete(diff.range.idString);
							if (pendingRanges.size === 0) {
								this.pendingMaturity.delete(diff.range.hash);
							}
						}, waitForMaturityTime);

					let prevPendingMaturity = pendingRanges.get(diff.range.idString);
					if (prevPendingMaturity) {
						// only reset the timer if the new range is older than the previous one, this means that waitForMaturityTime less than the previous one
						clearTimeout(prevPendingMaturity.timeout);
						prevPendingMaturity.timeout = setupTimeout();
					} else {
						pendingRanges.set(diff.range.idString, {
							range: diff,
							timeout: setupTimeout(),
						});
					}
				}
			} else if (diff.type === "removed") {
				this.deleteNativeReplicationRange(diff.range);
				const pendingFromPeer = this.pendingMaturity.get(diff.range.hash);
				if (pendingFromPeer) {
					const prev = pendingFromPeer.get(diff.range.idString);
					if (prev) {
						clearTimeout(prev.timeout);
						pendingFromPeer.delete(diff.range.idString);
					}
					if (pendingFromPeer.size === 0) {
						this.pendingMaturity.delete(diff.range.hash);
					}
				}
			}
			// else replaced, do nothing
		}

		if (diffs.length > 0) {
			if (reset) {
				await this.updateOldestTimestampFromIndex();
			}

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChangeEvent>("replication:change", {
					detail: { publicKey: from },
				}),
			);

				if (isNewReplicator) {
					if (!this._replicatorJoinEmitted.has(fromHash)) {
						this._replicatorJoinEmitted.add(fromHash);
						this.events.dispatchEvent(
							new CustomEvent<ReplicatorJoinEvent>("replicator:join", {
								detail: { publicKey: from },
							}),
						);
					}

					if (isAllMature) {
						this.events.dispatchEvent(
							new CustomEvent<ReplicatorMatureEvent>("replicator:mature", {
								detail: { publicKey: from },
						}),
					);
				}
			}

			if (isStoppedReplicating && stoppedTransition) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
						detail: { publicKey: from },
					}),
				);
			}

				if (rebalance) {
					for (const diff of diffs) {
						this.replicationChangeDebounceFn.add(diff);
					}
				}

			if (!from.equals(this.node.identity.publicKey)) {
				this.rebalanceParticipationDebounced?.call();
			}
		}
		return diffs;
	}

	async startAnnounceReplicating(
		range: ReplicationRangeIndexable<R>[],
		options: {
			reset?: boolean;
			checkDuplicates?: boolean;
			rebalance?: boolean;
			announce?: (
				msg: AllReplicatingSegmentsMessage | AddedReplicationSegmentMessage,
			) => void;
		} = {},
	) {
		await this.ensureCurrentHeadCoordinatesIndexed();

		const change = await this.addReplicationRange(
			range,
			this.node.identity.publicKey,
			options,
		);

		if (!change) {
			warn("Not allowed to replicate by canReplicate");
		}

		if (change) {
			let addedOrReplaced = change.filter((x) => x.type !== "removed");
			if (addedOrReplaced.length > 0) {
				// Provider discovery keep-alive (best-effort). This enables bounded targeted fetches
				// without relying on any global subscriber list.
				try {
					const fanoutService = getSharedLogFanoutService(this.node.services);
					if (fanoutService?.provide && !this._providerHandle) {
						this._providerHandle = fanoutService.provide(`shared-log|${this.topic}`, {
							ttlMs: 120_000,
							announceIntervalMs: 60_000,
						});
					}
				} catch {
					// Best-effort only.
				}

				let message:
					| AllReplicatingSegmentsMessage
					| AddedReplicationSegmentMessage
					| undefined = undefined;
				if (options.reset) {
					message = new AllReplicatingSegmentsMessage({
						segments: addedOrReplaced.map((x) => x.range.toReplicationRange()),
					});
				} else {
					message = new AddedReplicationSegmentMessage({
						segments: addedOrReplaced.map((x) => x.range.toReplicationRange()),
					});
				}
				if (options.announce) {
					return options.announce(message);
				} else {
					await this.rpc.send(message, {
						priority: 1,
					});
				}
			}
		}
	}

	private removePeerFromGidPeerHistory(publicKeyHash: string, gid?: string) {
		this._nativeSharedLogState?.removeGidPeer(publicKeyHash, gid);
		if (gid) {
			const gidMap = this._gidPeersHistory.get(gid);
			if (gidMap) {
				gidMap.delete(publicKeyHash);

				if (gidMap.size === 0) {
					this._gidPeersHistory.delete(gid);
				}
			}
		} else {
			for (const key of this._gidPeersHistory.keys()) {
				this.removePeerFromGidPeerHistory(publicKeyHash, key);
			}
			this.removePeerFromEntryKnownPeers(publicKeyHash);
		}
	}

	private deleteGidPeerHistory(gid: string) {
		this._nativeSharedLogState?.deleteGidPeers(gid);
		this._gidPeersHistory.delete(gid);
	}

	addPeersToGidPeerHistory(
		gid: string,
		publicKeys: Iterable<string>,
		reset?: boolean,
	) {
		const publicKeyArray = [...publicKeys];
		this._nativeSharedLogState?.addGidPeers(gid, publicKeyArray, reset === true);
		let set = this._gidPeersHistory.get(gid);
		if (!set) {
			set = new Set();
			this._gidPeersHistory.set(gid, set);
		} else {
			if (reset) {
				set.clear();
			}
		}

		for (const key of publicKeyArray) {
			set.add(key);
		}
		return set;
	}

	private markEntriesKnownByPeer(hashes: Iterable<string>, peer: string) {
		const hashArray = [...hashes];
		this._nativeSharedLogState?.markEntriesKnownByPeer(hashArray, peer);
		for (const hash of hashArray) {
			let peers = this._entryKnownPeers.get(hash);
			if (!peers) {
				peers = new Set();
				this._entryKnownPeers.set(hash, peers);
			}
			peers.add(peer);
		}
	}

	private removeEntriesKnownByPeer(hashes: Iterable<string>, peer: string) {
		const hashArray = [...hashes];
		this._nativeSharedLogState?.removeEntriesKnownByPeer(hashArray, peer);
		for (const hash of hashArray) {
			const peers = this._entryKnownPeers.get(hash);
			if (!peers) {
				continue;
			}
			peers.delete(peer);
			if (peers.size === 0) {
				this._entryKnownPeers.delete(hash);
			}
		}
	}

	private removePeerFromEntryKnownPeers(peer: string) {
		this._nativeSharedLogState?.removePeerFromEntryKnownPeers(peer);
		for (const [hash, peers] of this._entryKnownPeers) {
			peers.delete(peer);
			if (peers.size === 0) {
				this._entryKnownPeers.delete(hash);
			}
		}
	}

	private isEntryKnownByPeer(hash: string, peer: string) {
		return this._entryKnownPeers.get(hash)?.has(peer) === true;
	}

	private markRepairSweepOptimisticPeer(gid: string, peer: string) {
		let peers = this._repairSweepOptimisticGidPeersPending.get(gid);
		if (!peers) {
			peers = new Map();
			this._repairSweepOptimisticGidPeersPending.set(gid, peers);
		}
		peers.set(peer, (peers.get(peer) || 0) + 1);
	}

	private hasPendingRepairSweepOptimisticPeer(gid: string, peer: string) {
		return (this._repairSweepOptimisticGidPeersPending.get(gid)?.get(peer) || 0) > 0;
	}

	private createEntryReplicatedForRepair(properties: {
		entry: Entry<T>;
		coordinates: NumberFromType<R>[];
		leaders: Map<string, { intersecting: boolean }>;
		replicas: number;
	}) {
		const assignedToRangeBoundary = shouldAssignToRangeBoundary(
			properties.leaders,
			properties.replicas,
		);
		const hashNumber = this.getEntryHashNumber(properties.entry);
		return new this.indexableDomain.constructorEntry({
			assignedToRangeBoundary,
			coordinates: properties.coordinates,
			meta: properties.entry.meta,
			metaBytes: (properties.entry as EntryWithMetaBytes).getMetaBytes?.(),
			hash: properties.entry.hash,
			hashNumber,
		});
	}

	private isAssumeSyncedRepairSuppressed() {
		return this._assumeSyncedRepairSuppressedUntil > Date.now();
	}

	private isFrontierTrackedRepairMode(mode: RepairDispatchMode) {
		return mode !== "join-warmup";
	}

	private async sleepTracked(delayMs: number) {
		if (delayMs <= 0) {
			return;
		}
		await new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				this._repairRetryTimers.delete(timer);
				resolve();
			}, delayMs);
			timer.unref?.();
			this._repairRetryTimers.add(timer);
		});
	}

	private queueRepairFrontierEntries(
		mode: RepairDispatchMode,
		target: string,
		entries: Map<string, EntryReplicated<R>>,
	) {
		let targets = this._repairFrontierByMode.get(mode);
		if (!targets) {
			targets = new Map();
			this._repairFrontierByMode.set(mode, targets);
		}
		let pending = targets.get(target);
		if (!pending) {
			pending = new Map();
			targets.set(target, pending);
		}
		for (const [hash, entry] of entries) {
			pending.set(hash, entry);
		}
	}

	private clearRepairFrontierHashes(target: string, hashes: Iterable<string>) {
		const hashList = [...hashes];
		if (hashList.length === 0) {
			return;
		}
		for (const mode of REPAIR_DISPATCH_MODES) {
			const pending = this._repairFrontierByMode.get(mode)?.get(target);
			if (!pending) {
				continue;
			}
			for (const hash of hashList) {
				pending.delete(hash);
			}
			if (pending.size === 0) {
				this._repairFrontierByMode.get(mode)?.delete(target);
			}
		}
	}

	private async getFullReplicaRepairCandidates(
		extraPeers?: Iterable<string>,
		options?: { includeSubscribers?: boolean },
	) {
		const candidates = new Set<string>([
			this.node.identity.publicKey.hashcode(),
		]);
		try {
			for (const peer of await this.getReplicators()) {
				candidates.add(peer);
			}
		} catch {
			for (const peer of this.uniqueReplicators) {
				candidates.add(peer);
			}
		}
		for (const peer of extraPeers ?? []) {
			candidates.add(peer);
		}
		if (options?.includeSubscribers !== false) {
			try {
				for (const subscriber of (await this._getTopicSubscribers(this.topic)) ?? []) {
					candidates.add(subscriber.hashcode());
				}
			} catch {
				// Best-effort only; explicit repair peers still keep the path safe.
			}
		}
		return candidates;
	}

	private removeRepairFrontierTarget(target: string) {
		for (const mode of REPAIR_DISPATCH_MODES) {
			this._repairFrontierByMode.get(mode)?.delete(target);
			this._repairFrontierActiveTargetsByMode.get(mode)?.delete(target);
		}
	}

	private async sendRepairConfirmation(
		target: PublicSignKey,
		hashes: Iterable<string>,
	) {
		const uniqueHashes = [...new Set(hashes)];
		for (let i = 0; i < uniqueHashes.length; i += REPAIR_CONFIRMATION_HASH_BATCH_SIZE) {
			const chunk = uniqueHashes.slice(
				i,
				i + REPAIR_CONFIRMATION_HASH_BATCH_SIZE,
			);
			await this.rpc.send(new ConfirmEntriesMessage({ hashes: chunk }), {
				priority: 1,
				mode: new SilentDelivery({ to: [target], redundancy: 1 }),
			});
		}
	}

	private async pushRepairEntries(
		target: string,
		entries: Map<string, EntryReplicated<R>>,
	) {
		for await (const message of createExchangeHeadsMessages(
			this.log,
			[...entries.keys()],
		)) {
			message.reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
			await this.rpc.send(message, {
				priority: 1,
				mode: new SilentDelivery({ to: [target], redundancy: 1 }),
			});
		}
	}

	private async sendRepairEntriesWithTransport(
		target: string,
		entries: Map<string, EntryReplicated<R>>,
		transport: RepairTransportMode,
		options?: { bypassKnownPeers?: boolean },
	) {
		const unknownEntries = new Map<string, EntryReplicated<R>>();
		const knownHashes: string[] = [];
		for (const [hash, entry] of entries) {
			if (options?.bypassKnownPeers || !this.isEntryKnownByPeer(hash, target)) {
				unknownEntries.set(hash, entry);
			} else {
				knownHashes.push(hash);
			}
		}
		this.clearRepairFrontierHashes(target, knownHashes);
		if (unknownEntries.size === 0) {
			return;
		}
		if (transport === "simple") {
			// Fallback repair should not depend on the target completing the
			// RequestMaybeSync -> ResponseMaybeSync round trip.
			await this.pushRepairEntries(target, unknownEntries);
			return;
		}

		await this.syncronizer.onMaybeMissingEntries({
			entries: unknownEntries,
			targets: [target],
		});
	}

	private async sendMaybeMissingEntriesNow(
		target: string,
		entries: Map<string, EntryReplicated<R>>,
		options: {
			mode: RepairDispatchMode;
			transport: RepairTransportMode;
			bypassRecentDedupe?: boolean;
		},
	) {
		if (entries.size === 0) {
			return;
		}

		const now = Date.now();
		let recentlyDispatchedByHash = this._recentRepairDispatch.get(target);
		if (!recentlyDispatchedByHash) {
			recentlyDispatchedByHash = new Map();
			this._recentRepairDispatch.set(target, recentlyDispatchedByHash);
		}
		for (const [hash, ts] of recentlyDispatchedByHash) {
			if (now - ts > RECENT_REPAIR_DISPATCH_TTL_MS) {
				recentlyDispatchedByHash.delete(hash);
			}
		}

		const filteredEntries =
			options.bypassRecentDedupe === true
				? new Map(entries)
				: new Map<string, EntryReplicated<any>>();
		if (options.bypassRecentDedupe !== true) {
			for (const [hash, entry] of entries) {
				const prev = recentlyDispatchedByHash.get(hash);
				if (prev != null && now - prev <= RECENT_REPAIR_DISPATCH_TTL_MS) {
					continue;
				}
				recentlyDispatchedByHash.set(hash, now);
				filteredEntries.set(hash, entry);
			}
		} else {
			for (const hash of entries.keys()) {
				recentlyDispatchedByHash.set(hash, now);
			}
		}
		if (filteredEntries.size === 0) {
			return;
		}

		const bucket = this._repairMetrics[options.mode];
		bucket.dispatches += 1;
		bucket.entries += filteredEntries.size;
		if (options.transport === "simple") {
			bucket.simpleFallbackPasses += 1;
		} else {
			bucket.ratelessFirstPasses += 1;
		}

		await Promise.resolve(
			this.sendRepairEntriesWithTransport(
				target,
				filteredEntries,
				options.transport,
				{ bypassKnownPeers: options.mode === "churn" },
			),
		).catch((error: any) => logger.error(error));
	}

	private ensureRepairFrontierRunner(
		mode: RepairDispatchMode,
		target: string,
		retryScheduleMs?: number[],
	) {
		const activeTargets = this._repairFrontierActiveTargetsByMode.get(mode);
		if (!activeTargets || activeTargets.has(target) || this.closed) {
			return;
		}
		activeTargets.add(target);
		const retrySchedule = resolveRepairRetrySchedule(
			mode,
			retryScheduleMs,
			this.isFrontierTrackedRepairMode(mode),
		);
		const steadyStateDelay =
			retrySchedule.length > 1
				? Math.max(1, retrySchedule[retrySchedule.length - 1] - retrySchedule[retrySchedule.length - 2])
				: Math.max(retrySchedule[0] || 1_000, 1_000);

		void (async () => {
			let attemptIndex = 0;
			try {
				for (;;) {
					if (this.closed) {
						return;
					}
					const pending = this._repairFrontierByMode.get(mode)?.get(target);
					if (!pending || pending.size === 0) {
						return;
					}

					if (
						(mode === "join-warmup" || mode === "join-authoritative") &&
						this.isAssumeSyncedRepairSuppressed()
					) {
						await this.sleepTracked(
							Math.max(250, this._assumeSyncedRepairSuppressedUntil - Date.now()),
						);
						continue;
					}

					await this.sendMaybeMissingEntriesNow(target, pending, {
						mode,
						transport: getRepairTransportForAttempt(mode, attemptIndex),
						bypassRecentDedupe: true,
					});

					const remaining = this._repairFrontierByMode.get(mode)?.get(target);
					if (!remaining || remaining.size === 0) {
						return;
					}

					const waitMs =
						attemptIndex + 1 < retrySchedule.length
							? Math.max(0, retrySchedule[attemptIndex + 1] - retrySchedule[attemptIndex])
							: steadyStateDelay;
					attemptIndex = Math.min(attemptIndex + 1, retrySchedule.length - 1);
					await this.sleepTracked(waitMs);
				}
			} finally {
				activeTargets.delete(target);
				if (
					!this.closed &&
					(this._repairFrontierByMode.get(mode)?.get(target)?.size || 0) > 0
				) {
					this.ensureRepairFrontierRunner(mode, target, retryScheduleMs);
				}
			}
		})().catch((error: any) => {
			activeTargets.delete(target);
			logger.error(error);
		});
	}

	private flushAppendBackfill() {
		if (this._appendBackfillPendingByTarget.size === 0) {
			return;
		}
		const pending = this._appendBackfillPendingByTarget;
		this._appendBackfillPendingByTarget = new Map();
		for (const [target, entries] of pending) {
			this.dispatchMaybeMissingEntries(target, entries, {
				mode: "append-backfill",
			});
		}
	}

	private queueAppendBackfill(target: string, entry: EntryReplicated<R>) {
		let entries = this._appendBackfillPendingByTarget.get(target);
		if (!entries) {
			entries = new Map();
			this._appendBackfillPendingByTarget.set(target, entries);
		}
		entries.set(entry.hash, entry);
		if (entries.size >= this.repairSweepTargetBufferSize) {
			this.flushAppendBackfill();
			return;
		}
		if (this._appendBackfillTimer || this.closed) {
			return;
		}
		const timer = setTimeout(() => {
			this._repairRetryTimers.delete(timer);
			if (this._appendBackfillTimer === timer) {
				this._appendBackfillTimer = undefined;
			}
			if (this.closed) {
				return;
			}
			this.flushAppendBackfill();
		}, APPEND_BACKFILL_DELAY_MS);
		timer.unref?.();
		this._repairRetryTimers.add(timer);
		this._appendBackfillTimer = timer;
	}

	private dispatchMaybeMissingEntries(
		target: string,
		entries: Map<string, EntryReplicated<R>>,
		options: {
			mode: RepairDispatchMode;
			bypassRecentDedupe?: boolean;
			retryScheduleMs?: number[];
		},
	) {
		if (entries.size === 0) {
			return;
		}

		if (this.isFrontierTrackedRepairMode(options.mode)) {
			this.queueRepairFrontierEntries(options.mode, target, entries);
			this.ensureRepairFrontierRunner(
				options.mode,
				target,
				options.retryScheduleMs,
			);
			return;
		}

		const now = Date.now();
		let recentlyDispatchedByHash = this._recentRepairDispatch.get(target);
		if (!recentlyDispatchedByHash) {
			recentlyDispatchedByHash = new Map();
			this._recentRepairDispatch.set(target, recentlyDispatchedByHash);
		}
		for (const [hash, ts] of recentlyDispatchedByHash) {
			if (now - ts > RECENT_REPAIR_DISPATCH_TTL_MS) {
				recentlyDispatchedByHash.delete(hash);
			}
		}

		const filteredEntries =
			options.bypassRecentDedupe === true
				? new Map(entries)
				: new Map<string, EntryReplicated<any>>();
		if (options.bypassRecentDedupe !== true) {
			for (const [hash, entry] of entries) {
				const prev = recentlyDispatchedByHash.get(hash);
				if (prev != null && now - prev <= RECENT_REPAIR_DISPATCH_TTL_MS) {
					continue;
				}
				recentlyDispatchedByHash.set(hash, now);
				filteredEntries.set(hash, entry);
			}
		} else {
			for (const hash of entries.keys()) {
				recentlyDispatchedByHash.set(hash, now);
			}
		}
		if (filteredEntries.size === 0) {
			return;
		}

		if (
			(options.mode === "join-warmup" ||
				options.mode === "join-authoritative") &&
			this.isAssumeSyncedRepairSuppressed()
		) {
			return;
		}

		const retrySchedule = resolveRepairRetrySchedule(
			options.mode,
			options.retryScheduleMs,
			this.isFrontierTrackedRepairMode(options.mode),
		);
		const bucket = this._repairMetrics[options.mode];
		bucket.dispatches += 1;
		bucket.entries += filteredEntries.size;

		const run = (transport: RepairTransportMode) => {
			if (transport === "simple") {
				bucket.simpleFallbackPasses += 1;
			} else {
				bucket.ratelessFirstPasses += 1;
			}

			return Promise.resolve(
				this.sendRepairEntriesWithTransport(
					target,
					filteredEntries,
					transport,
					{ bypassKnownPeers: options.mode === "churn" },
				),
			).catch((error: any) => logger.error(error));
		};

		retrySchedule.forEach((delayMs, index) => {
			const transport = getRepairTransportForAttempt(options.mode, index);
			if (delayMs === 0) {
				void run(transport);
				return;
			}
			const timer = setTimeout(() => {
				this._repairRetryTimers.delete(timer);
				if (this.closed) {
					return;
				}
				void run(transport);
			}, delayMs);
			timer.unref?.();
			this._repairRetryTimers.add(timer);
		});
	}

	private scheduleRepairSweep(options: {
		mode: RepairDispatchMode;
		peers?: Iterable<string>;
	}) {
		this._repairSweepPendingModes.add(options.mode);
		const pendingPeers = this._repairSweepPendingPeersByMode.get(options.mode);
		if (pendingPeers) {
			for (const peer of options.peers ?? []) {
				pendingPeers.add(peer);
			}
		}
		if (!this._repairSweepRunning && !this.closed) {
			this._repairSweepRunning = true;
			void this.runRepairSweep();
		}
	}

	private scheduleJoinAuthoritativeRepair(peers: Set<string>) {
		if (this.closed || peers.size === 0) {
			return;
		}

		for (const delayMs of JOIN_AUTHORITATIVE_REPAIR_SWEEP_DELAYS_MS) {
			let pendingPeers = this._joinAuthoritativeRepairPeersByDelay.get(delayMs);
			if (!pendingPeers) {
				pendingPeers = new Set();
				this._joinAuthoritativeRepairPeersByDelay.set(delayMs, pendingPeers);
			}
			for (const peer of peers) {
				pendingPeers.add(peer);
			}

			if (this._joinAuthoritativeRepairTimersByDelay.has(delayMs)) {
				continue;
			}

			const timer = setTimeout(() => {
				this._repairRetryTimers.delete(timer);
				this._joinAuthoritativeRepairTimersByDelay.delete(delayMs);
				if (this.closed) {
					return;
				}

				const peersForSweep = new Set(
					this._joinAuthoritativeRepairPeersByDelay.get(delayMs) ?? [],
				);
				this._joinAuthoritativeRepairPeersByDelay.delete(delayMs);
				if (peersForSweep.size === 0) {
					return;
				}

				// A joiner's leader view can still be partial on the first delayed pass
				// under pubsub jitter. Bounded per-peer rescans widen the authoritative
				// frontier without adding per-append sweeps.
				this.scheduleRepairSweep({
					mode: "join-authoritative",
					peers: peersForSweep,
				});
			}, delayMs);
			timer.unref?.();
			this._repairRetryTimers.add(timer);
			this._joinAuthoritativeRepairTimersByDelay.set(delayMs, timer);
		}
	}

	private async runRepairSweep() {
		try {
			while (!this.closed) {
				const pendingModes = new Set(this._repairSweepPendingModes);
				const pendingPeersByMode = cloneRepairPendingPeersByMode(
					this._repairSweepPendingPeersByMode,
				);
				this._repairSweepPendingModes.clear();
				for (const peers of this._repairSweepPendingPeersByMode.values()) {
					peers.clear();
				}

				if (pendingModes.size === 0) {
					return;
				}

				const optimisticGidPeersByMode = new Map<
					RepairDispatchMode,
					Map<string, Set<string>>
				>();
				const optimisticGidPeersConsumedByMode = new Map<
					RepairDispatchMode,
					Map<string, Map<string, number>>
				>();
				for (const mode of pendingModes) {
					const modePeers = pendingPeersByMode.get(mode);
					if (!modePeers || modePeers.size === 0) {
						continue;
					}
					const optimisticGidPeers = new Map<string, Set<string>>();
					const optimisticGidPeersConsumed = new Map<string, Map<string, number>>();
					for (const [gid, peerCounts] of this._repairSweepOptimisticGidPeersPending) {
						let matchedPeers: Set<string> | undefined;
						let matchedCounts: Map<string, number> | undefined;
						for (const [peer, count] of peerCounts) {
							if (!modePeers.has(peer)) {
								continue;
							}
							matchedPeers ||= new Set();
							matchedCounts ||= new Map();
							matchedPeers.add(peer);
							matchedCounts.set(peer, count);
						}
						if (matchedPeers && matchedCounts) {
							optimisticGidPeers.set(gid, matchedPeers);
							optimisticGidPeersConsumed.set(gid, matchedCounts);
						}
					}
					if (optimisticGidPeers.size > 0) {
						optimisticGidPeersByMode.set(mode, optimisticGidPeers);
						optimisticGidPeersConsumedByMode.set(mode, optimisticGidPeersConsumed);
					}
				}

				const pendingByMode = new Map<
					RepairDispatchMode,
					Map<string, Map<string, EntryReplicated<any>>>
				>(REPAIR_DISPATCH_MODES.map((mode) => [mode, new Map()]));
				const pendingRepairPeers = new Set<string>();
				for (const peers of pendingPeersByMode.values()) {
					for (const peer of peers) {
						pendingRepairPeers.add(peer);
					}
				}
				const fullReplicaRepairCandidates =
					await this.getFullReplicaRepairCandidates(pendingRepairPeers, {
						includeSubscribers: false,
					});
				const fullReplicaRepairCandidateCount = Math.max(
					1,
					fullReplicaRepairCandidates.size,
				);
				const nextFrontierByMode = new Map<
					RepairDispatchMode,
					Map<string, Map<string, EntryReplicated<any>>>
				>([
					["join-authoritative", new Map()],
					["churn", new Map()],
				]);
				const flushTarget = (mode: RepairDispatchMode, target: string) => {
					const targets = pendingByMode.get(mode);
					const entries = targets?.get(target);
					if (!entries || entries.size === 0) {
						return;
					}
					this.dispatchMaybeMissingEntries(target, entries, {
						bypassRecentDedupe: true,
						mode,
					});
					targets?.delete(target);
				};
				const queueEntryForTarget = (
					mode: RepairDispatchMode,
					target: string,
					entry: EntryReplicated<any>,
				) => {
					const sweepTargets = nextFrontierByMode.get(mode);
					if (sweepTargets) {
						let sweepSet = sweepTargets.get(target);
						if (!sweepSet) {
							sweepSet = new Map();
							sweepTargets.set(target, sweepSet);
						}
						sweepSet.set(entry.hash, entry);
					}
					const targets = pendingByMode.get(mode)!;
					let set = targets.get(target);
					if (!set) {
						set = new Map();
						targets.set(target, set);
					}
					if (set.has(entry.hash)) {
						return;
					}
					set.set(entry.hash, entry);
					if (set.size >= this.repairSweepTargetBufferSize) {
						flushTarget(mode, target);
					}
				};

				const residentEntriesByHash = this._residentEntryCoordinatesByHash;
				if (this._nativeSharedLogState && residentEntriesByHash) {
					const repairDispatchPlan = await this.planResidentRepairDispatchBatch({
						pendingModes,
						pendingPeersByMode,
						optimisticGidPeersByMode,
						fullReplicaRepairCandidates,
						fullReplicaRepairCandidateCount,
						selfHash: this.node.identity.publicKey.hashcode(),
					});
					for (const [mode, targets] of repairDispatchPlan) {
						for (const [target, hashes] of targets) {
							for (const hash of hashes) {
								const entry = residentEntriesByHash.get(hash);
								if (entry) {
									queueEntryForTarget(mode, target, entry);
								}
							}
						}
					}
				} else {
					const iterator = this.entryCoordinatesIndex.iterate({});
					try {
						while (!this.closed && !iterator.done()) {
							const entries = await iterator.next(REPAIR_SWEEP_ENTRY_BATCH_SIZE);
							const entryReplicatedBatch = entries.map((entry) => entry.value);
							const requestedReplicasBatch = entryReplicatedBatch.map((entry) =>
								decodeReplicas(entry).getValue(this),
							);
							const repairDispatchPlan = await this.planRepairDispatchBatch({
								entries: entryReplicatedBatch,
								requestedReplicasBatch,
								pendingModes,
								pendingPeersByMode,
								optimisticGidPeersByMode,
								fullReplicaRepairCandidates,
								fullReplicaRepairCandidateCount,
								selfHash: this.node.identity.publicKey.hashcode(),
							});
							const entriesByHash = new Map(
								entryReplicatedBatch.map((entry) => [entry.hash, entry]),
							);
							for (const [mode, targets] of repairDispatchPlan) {
								for (const [target, hashes] of targets) {
									for (const hash of hashes) {
										const entry = entriesByHash.get(hash);
										if (entry) {
											queueEntryForTarget(mode, target, entry);
										}
									}
								}
							}
						}
					} finally {
						await iterator.close();
					}
				}

				for (const [, optimisticGidPeersConsumed] of optimisticGidPeersConsumedByMode) {
					for (const [gid, peerCounts] of optimisticGidPeersConsumed) {
						const pendingPeerCounts =
							this._repairSweepOptimisticGidPeersPending.get(gid);
						if (!pendingPeerCounts) {
							continue;
						}
						for (const [peer, count] of peerCounts) {
							const current = pendingPeerCounts.get(peer) || 0;
							const next = current - count;
							if (next > 0) {
								pendingPeerCounts.set(peer, next);
							} else {
								pendingPeerCounts.delete(peer);
							}
						}
						if (pendingPeerCounts.size === 0) {
							this._repairSweepOptimisticGidPeersPending.delete(gid);
						}
					}
				}

				for (const mode of pendingModes) {
					if (mode !== "join-authoritative" && mode !== "churn") {
						continue;
					}
					const nextTargets = nextFrontierByMode.get(mode) ?? new Map();
					const frontierTargets = this._repairFrontierByMode.get(mode);
					for (const target of pendingPeersByMode.get(mode) ?? []) {
						const replacement = nextTargets.get(target);
						// These repairs are receipt-driven: a later sweep can have a narrower
						// transient leader view, but it must not forget unconfirmed hashes
						// that were already queued for this target.
						if (replacement && replacement.size > 0) {
							const existing = frontierTargets?.get(target);
							if (existing && existing.size > 0) {
								for (const [hash, entry] of replacement) {
									existing.set(hash, entry);
								}
							} else {
								frontierTargets?.set(target, replacement);
							}
						}
					}
				}

				for (const [mode, targets] of pendingByMode) {
					for (const target of [...targets.keys()]) {
						flushTarget(mode, target);
					}
				}
			}
		} catch (error: any) {
			if (!isNotStartedError(error)) {
				logger.error(`Repair sweep failed: ${error?.message ?? error}`);
			}
		} finally {
			this._repairSweepRunning = false;
			if (!this.closed && this._repairSweepPendingModes.size > 0) {
				this._repairSweepRunning = true;
				void this.runRepairSweep();
			}
		}
	}

	private async pruneDebouncedFnAddIfNotKeeping(args: {
		key: string;
		value: {
			entry: CheckedPruneEntry<T, R>;
			leaders: CheckedPruneLeaderMap;
		};
	}): Promise<boolean> {
		if (this.keep && (await this.keep(args.value.entry))) {
			return false;
		}
		void this.pruneDebouncedFn.add(args);
		return true;
	}

	private async cancelCheckedPruneForLocalLeader(hash: string) {
		this.pruneDebouncedFn.delete(hash);
		this.clearCheckedPruneRetry(hash);
		this.removePruneRequestSent(hash);
		this._requestIPruneResponseReplicatorSet.delete(hash);
		await this._pendingDeletes
			.get(hash)
			?.reject(new Error("Failed to delete, is leader again"));
	}

	private hasActiveCheckedPruneWork(hash: string) {
		return (
			this._pendingDeletes.has(hash) ||
			this._requestIPruneSent.has(hash) ||
			this._requestIPruneResponseReplicatorSet.has(hash) ||
			this._checkedPruneRetries.has(hash)
		);
	}

	private async resolveCheckedPruneLeaders(args: {
		hash: string;
		entry: CheckedPruneEntry<T, R>;
		leaders: CheckedPruneLeaderMap;
		selfReplicating?: boolean;
	}): Promise<{
		leaders: CheckedPruneLeaderMap;
		localLeader: boolean;
	}> {
		const selfHash = this.node.identity.publicKey.hashcode();
		if (args.leaders.has(selfHash)) {
			if (args.selfReplicating === false) {
				return { leaders: args.leaders, localLeader: false };
			}
			if (args.selfReplicating == null && !(await this.isReplicating())) {
				return { leaders: args.leaders, localLeader: false };
			}
			return { leaders: args.leaders, localLeader: true };
		}

		if (!this.hasActiveCheckedPruneWork(args.hash)) {
			return { leaders: args.leaders, localLeader: false };
		}

		if (args.selfReplicating === false) {
			return { leaders: args.leaders, localLeader: false };
		}
		if (args.selfReplicating == null && !(await this.isReplicating())) {
			return { leaders: args.leaders, localLeader: false };
		}

		try {
			const currentLeaders = await this.findLeadersFromEntry(
				args.entry,
				decodeReplicas(args.entry).getValue(this),
			);
			if (currentLeaders.size > 0) {
				return {
					leaders: currentLeaders,
					localLeader: currentLeaders.has(selfHash),
				};
			}
		} catch {
			// Best-effort only. If the fresh check fails, keep the original prune
			// decision instead of hiding a legitimately prunable entry.
		}

		return { leaders: args.leaders, localLeader: false };
	}

	private async pruneJoinedEntriesNoLongerLed(entries: Entry<T>[]) {
		const selfHash = this.node.identity.publicKey.hashcode();
		for (const entry of entries) {
			if (this.closed) {
				continue;
			}

			const leaders = await this.findLeadersFromEntry(
				entry,
				decodeReplicas(entry).getValue(this),
				{ roleAge: 0 },
			);

			if (leaders.has(selfHash)) {
				await this.cancelCheckedPruneForLocalLeader(entry.hash);
				continue;
			}

			if (this._pendingDeletes.has(entry.hash)) {
				continue;
			}

			if (leaders.size === 0) {
				continue;
			}

			await this.pruneDebouncedFnAddIfNotKeeping({
				key: entry.hash,
				value: { entry, leaders },
			});
			this.responseToPruneDebouncedFn.delete(entry.hash);
		}
	}

	private async pruneIndexedEntriesNoLongerLed() {
		const selfHash = this.node.identity.publicKey.hashcode();
		const iterator = this.entryCoordinatesIndex.iterate({});
		let enqueuedPrune = false;
		try {
			while (!this.closed && !iterator.done()) {
				const entries = await iterator.next(REPAIR_SWEEP_ENTRY_BATCH_SIZE);
				for (const entry of entries) {
					const entryReplicated = entry.value;
					if (this.closed) {
						continue;
					}

					const leaders = await this.findLeaders(
						entryReplicated.coordinates,
						entryReplicated,
						{ roleAge: 0 },
					);

					if (leaders.has(selfHash)) {
						await this.cancelCheckedPruneForLocalLeader(entryReplicated.hash);
						continue;
					}

					if (this._pendingDeletes.has(entryReplicated.hash)) {
						continue;
					}

					if (leaders.size === 0) {
						continue;
					}

					enqueuedPrune =
						(await this.pruneDebouncedFnAddIfNotKeeping({
							key: entryReplicated.hash,
							value: { entry: entryReplicated, leaders },
						})) || enqueuedPrune;
					this.responseToPruneDebouncedFn.delete(entryReplicated.hash);
				}
			}
		} finally {
			await iterator.close();
		}
		if (enqueuedPrune && !this.closed) {
			await this.pruneDebouncedFn.flush();
		}
	}

	private clearCheckedPruneRetry(hash: string) {
		const state = this._checkedPruneRetries.get(hash);
		if (state?.timer) {
			clearTimeout(state.timer);
		}
		this._checkedPruneRetries.delete(hash);
	}

	private scheduleCheckedPruneRetry(args: {
		entry: CheckedPruneEntry<T, R>;
		leaders: CheckedPruneLeaderMap | Set<string>;
	}) {
		if (this.closed) return;
		if (this._pendingDeletes.has(args.entry.hash)) return;

		const hash = args.entry.hash;
		const state =
			this._checkedPruneRetries.get(hash) ?? { attempts: 0 };

		if (state.timer) return;
		if (state.attempts >= CHECKED_PRUNE_RETRY_MAX_ATTEMPTS) {
			// Avoid unbounded background retries; a new replication-change event can
			// always re-enqueue pruning with fresh leader info.
			return;
		}

		const attempt = state.attempts + 1;
		const jitterMs = Math.floor(Math.random() * 250);
		const delayMs = Math.min(
			CHECKED_PRUNE_RETRY_MAX_DELAY_MS,
			1_000 * 2 ** (attempt - 1) + jitterMs,
		);

		state.attempts = attempt;
		state.timer = setTimeout(async () => {
			const st = this._checkedPruneRetries.get(hash);
			if (st) st.timer = undefined;
			if (this.closed) return;
			if (this._pendingDeletes.has(hash)) return;

			let leadersMap: CheckedPruneLeaderMap | undefined;
			try {
				const replicas = decodeReplicas(args.entry).getValue(this);
				leadersMap = await this.findLeadersFromEntry(args.entry, replicas, {
					roleAge: 0,
				});
			} catch {
				// Best-effort only.
			}

			if (!leadersMap || leadersMap.size === 0) {
				if (args.leaders instanceof Map) {
					leadersMap = args.leaders;
				} else {
					leadersMap = new Map<string, { intersecting: boolean }>();
					for (const k of args.leaders) {
						leadersMap.set(k, { intersecting: true });
					}
				}
			}

			try {
				const leadersForRetry =
					leadersMap ?? new Map<string, { intersecting: boolean }>();
				await this.pruneDebouncedFnAddIfNotKeeping({
					key: hash,
					value: { entry: args.entry, leaders: leadersForRetry },
				});
			} catch {
				// Best-effort only; pruning will be re-attempted on future changes.
			}
		}, delayMs);
		state.timer.unref?.();
		this._checkedPruneRetries.set(hash, state);
	}

	async append(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		const result = await this.log.append(data, appendOptions);
		await this.processLocalAppend(result.entry, result.removed, options, {
			minReplicasValue,
		});
		return result;
	}

	/** Trusted local append path for callers that already validated the entry. */
	async appendLocallyValidated(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		if (options?.canAppend || options?.onChange) {
			throw new Error(
				"appendLocallyValidated does not accept canAppend or onChange hooks",
			);
		}
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		appendOptions.__peerbitCanAppendAlreadyValidated = true;
		appendOptions.onChange = (change) => this.onChange(change);
		const result = await this.log.append(data, appendOptions);
		await this.processLocalAppend(result.entry, result.removed, options, {
			minReplicasValue,
		});
		return result;
	}

	/** Trusted local append path that lets the shared log own change application. */
	async appendLocallyPrepared(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
		},
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		if (options?.canAppend || options?.onChange) {
			throw new Error(
				"appendLocallyPrepared does not accept canAppend or onChange hooks",
			);
		}
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		appendOptions.__peerbitCanAppendAlreadyValidated = true;
		const result = await this.log.appendLocallyPrepared(data, appendOptions, {
			skipMissingNextJoin: properties?.skipMissingNextJoin,
			resolveTrimmedEntries: properties?.resolveTrimmedEntries,
		});
		await this.onChange(result.change);
		await this.processLocalAppend(result.entry, result.removed, options, {
			minReplicasValue,
		});
		return { entry: result.entry, removed: result.removed };
	}

	async appendLocallyPreparedManyIndependent(
		data: T[],
		options?: SharedAppendOptions<T> | undefined,
		properties?: {
			resolveTrimmedEntries?: boolean;
		},
	): Promise<
		| {
				entries: Entry<T>[];
				removed: ShallowOrFullEntry<T>[];
		  }
		| undefined
	> {
		if (data.length === 0) {
			return { entries: [], removed: [] };
		}
		if (options?.canAppend || options?.onChange) {
			throw new Error(
				"appendLocallyPreparedManyIndependent does not accept canAppend or onChange hooks",
			);
		}
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		appendOptions.__peerbitCanAppendAlreadyValidated = true;
		const result = await this.log.appendLocallyPreparedManyIndependent(
			data,
			appendOptions,
			{
				resolveTrimmedEntries: properties?.resolveTrimmedEntries,
			},
		);
		if (!result) {
			return undefined;
		}

		await this.onChange(result.change);
		const deferHeadCoordinatePersistence =
			this.shouldDeferHeadCoordinatePersistence(options);

		if (deferHeadCoordinatePersistence) {
			await this.deleteCoordinatesForHashes([
				...result.entries.flatMap((entry) => entry.meta.next),
				...result.removed.map((entry) => entry.hash),
			]);
			return { entries: result.entries, removed: result.removed };
		}

		const nativeAppendPlans =
			options?.replicate === true
				? undefined
				: options?.target === "none"
					? await this.planNativeLocalAppendEntries(
							result.entries,
							minReplicasValue,
						)
					: await this.planNativeAppendEntries(
							result.entries,
							minReplicasValue,
							options?.delivery,
							options,
						);
		if (
			nativeAppendPlans &&
			result.removed.length === 0 &&
			(await this.processLocalAppendManyNativePlanned(
				result.entries,
				options,
				{
					nativeAppendPlans,
				},
			))
		) {
			return { entries: result.entries, removed: result.removed };
		}
		for (let i = 0; i < result.entries.length; i++) {
			await this.processLocalAppend(
				result.entries[i]!,
				i === result.entries.length - 1 ? result.removed : [],
				options,
				{
					minReplicasValue,
					deferHeadCoordinatePersistence: false,
					nativeAppendPlan: nativeAppendPlans?.[i],
				},
			);
		}

		return { entries: result.entries, removed: result.removed };
	}

	async appendMany(
		data: T[],
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entries: Entry<T>[];
		removed: ShallowOrFullEntry<T>[];
	}> {
		if (data.length === 0) {
			return { entries: [], removed: [] };
		}
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		const result = await this.log.appendMany(data, appendOptions);
		const deferHeadCoordinatePersistence =
			this.shouldDeferHeadCoordinatePersistence(options);

		if (deferHeadCoordinatePersistence) {
			await this.deleteCoordinatesForHashes([
				...result.entries.flatMap((entry) => entry.meta.next),
				...result.removed.map((entry) => entry.hash),
			]);
			return result;
		}

		if (this.canCoalesceLocalAppendMany(result.entries, options)) {
			await this.processLocalAppendManyCoalesced(result, options, {
				minReplicasValue,
			});
			return result;
		}

		const nativeAppendPlans =
			options?.replicate === true
				? undefined
				: options?.target === "none"
					? await this.planNativeLocalAppendEntries(
							result.entries,
							minReplicasValue,
						)
					: await this.planNativeAppendEntries(
							result.entries,
							minReplicasValue,
							options?.delivery,
							options,
						);
		for (let i = 0; i < result.entries.length; i++) {
			const entry = result.entries[i]!;
			await this.processLocalAppend(entry, [], options, {
				minReplicasValue,
				deferHeadCoordinatePersistence: false,
				nativeAppendPlan: nativeAppendPlans?.[i],
			});
		}
		return result;
	}

	private canCoalesceLocalAppendMany(
		entries: Entry<T>[],
		options?: SharedAppendOptions<T>,
	): boolean {
		if (
			entries.length <= 1 ||
			options?.target === "all" ||
			options?.target === "none" ||
			options?.replicate === true ||
			(options?.delivery !== undefined && options.delivery !== false)
		) {
			return false;
		}

		for (let i = 1; i < entries.length; i++) {
			const previous = entries[i - 1]!;
			const entry = entries[i]!;
			if (
				entry.meta.next.length !== 1 ||
				entry.meta.next[0] !== previous.hash ||
				entry.meta.gid !== previous.meta.gid
			) {
				return false;
			}
		}
		return true;
	}

	private async processLocalAppendManyCoalesced(
		result: {
			entries: Entry<T>[];
			removed: ShallowOrFullEntry<T>[];
		},
		options: SharedAppendOptions<T> | undefined,
		properties: {
			minReplicasValue: number;
		},
	): Promise<void> {
		const head = result.entries[result.entries.length - 1]!;
		await this.deleteCoordinatesForHashes([
			...result.entries[0]!.meta.next,
			...result.entries.slice(0, -1).map((entry) => entry.hash),
			...result.removed.map((entry) => entry.hash),
		]);
		await this.processLocalAppend(head, result.removed, options, {
			minReplicasValue: properties.minReplicasValue,
			deferHeadCoordinatePersistence: false,
		});
	}

	private async processLocalAppendManyNativePlanned(
		entries: Entry<T>[],
		options: SharedAppendOptions<T> | undefined,
		properties: {
			nativeAppendPlans: NativeAppendEntryPlan<R>[];
		},
	): Promise<boolean> {
		if (
			entries.length === 0 ||
			options?.target !== "none" ||
			options?.replicate === true ||
			properties.nativeAppendPlans.length !== entries.length
		) {
			return false;
		}

		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		await this.persistCoordinatesBatch(
			entries.map((entry, index) => {
				const plan = properties.nativeAppendPlans[index]!;
				return {
					leaders: plan.leaders,
					coordinates: plan.coordinates,
					replicas: plan.coordinates.length,
					entry,
					assignedToRangeBoundary: plan.assignedToRangeBoundary,
					commitNative: false,
				};
			}),
		);

		if (!delayAdaptiveRebalance) {
			for (let i = 0; i < entries.length; i++) {
				const plan = properties.nativeAppendPlans[i]!;
				if (!plan.isLeader) {
					this.pruneDebouncedFnAddIfNotKeeping({
						key: entries[i]!.hash,
						value: { entry: entries[i]!, leaders: plan.leaders },
					});
				}
			}
			this.rebalanceParticipationDebounced?.call();
		}
		return true;
	}

	private createLogAppendOptions(options?: SharedAppendOptions<T>): {
		appendOptions: AppendOptions<T>;
		minReplicasValue: number;
	} {
		const appendOptions: AppendOptions<T> = { ...options };
		const minReplicas = this.getClampedReplicas(
			options?.replicas
				? typeof options.replicas === "number"
					? new AbsoluteReplicas(options.replicas)
					: options.replicas
				: undefined,
		);
		const minReplicasData = encodeReplicas(minReplicas);
		const minReplicasValue = minReplicas.getValue(this);
		checkMinReplicasLimit(minReplicasValue);

		if (!appendOptions.meta) {
			appendOptions.meta = {
				data: minReplicasData,
			};
		} else {
			appendOptions.meta.data = minReplicasData;
		}
		if (options?.canAppend) {
			appendOptions.canAppend = async (entry) => {
				if (!(await this.canAppend(entry))) {
					return false;
				}
				return options.canAppend!(entry);
			};
		}

		if (options?.onChange) {
			appendOptions.onChange = async (change) => {
				await this.onChange(change);
				return options.onChange!(change);
			};
		}

		return { appendOptions, minReplicasValue };
	}

	private async planNativeAppendEntry(
		entry: Entry<T>,
		replicas: number,
		deliveryArg: false | true | DeliveryOptions | undefined,
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		if (!this._nativeSharedLogState || !this.canPlanNativeHashGid(entry)) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext();
		const fullReplicaDeliveryCandidates =
			await this.getFullReplicaRepairCandidates(undefined, {
				includeSubscribers: false,
			});
		const { delivery, reliability, requireRecipients, minAcks } =
			this._parseDeliveryOptions(deliveryArg);
		const plan = this._nativeSharedLogState.planAppendForGid(
			{
				entryHash: entry.hash,
				gid: entry.meta.gid,
				hashNumber: this.getEntryHashNumber(entry),
				nextHashes: entry.meta.next,
				replicas,
				fullReplicaCandidates: fullReplicaDeliveryCandidates,
				selfHash: context.selfHash,
				deliveryEnabled: !!delivery,
				reliabilityAck: reliability === "ack",
				minAcks,
				requireRecipients,
			},
			this.createNativeLeaderOptions(context),
		);
		return {
			coordinates: plan.coordinates as NumberFromType<R>[],
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
			delivery: plan.delivery,
		};
	}

	private async planNativeLocalAppendEntry(
		entry: Entry<T>,
		replicas: number,
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		if (!this._nativeSharedLogState || !this.canPlanNativeHashGid(entry)) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext();
		const plan = this._nativeSharedLogState.planLocalAppendForGid(
			{
				entryHash: entry.hash,
				gid: entry.meta.gid,
				hashNumber: this.getEntryHashNumber(entry),
				nextHashes: entry.meta.next,
				replicas,
				selfHash: context.selfHash,
			},
			this.createNativeLeaderOptions(context),
		);
		return {
			coordinates: plan.coordinates as NumberFromType<R>[],
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
		};
	}

	private async planNativeLocalAppendEntries(
		entries: Entry<T>[],
		replicas: number,
	): Promise<NativeAppendEntryPlan<R>[] | undefined> {
		if (
			!this._nativeSharedLogState ||
			entries.length === 0 ||
			!entries.every((entry) => this.canPlanNativeHashGid(entry))
		) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext();
		const plans = this._nativeSharedLogState.planAppendForGidsBatch(
			{
				entries: entries.map((entry) => ({
					entryHash: entry.hash,
					gid: entry.meta.gid,
					hashNumber: this.getEntryHashNumber(entry),
					nextHashes: entry.meta.next,
					replicas,
				})),
				fullReplicaCandidates: [],
				selfHash: context.selfHash,
				deliveryEnabled: false,
				reliabilityAck: false,
				requireRecipients: false,
			},
			this.createNativeLeaderOptions(context),
		);
		return plans.map((plan) => ({
			coordinates: plan.coordinates as NumberFromType<R>[],
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
		}));
	}

	private async planNativeAppendEntries(
		entries: Entry<T>[],
		replicas: number,
		deliveryArg: false | true | DeliveryOptions | undefined,
		options: SharedAppendOptions<T> | undefined,
	): Promise<NativeAppendEntryPlan<R>[] | undefined> {
		const target = options?.target;
		if (
			target === "all" ||
			target === "none" ||
			!this._nativeSharedLogState ||
			entries.length === 0 ||
			!entries.every((entry) => this.canPlanNativeHashGid(entry))
		) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext();
		const fullReplicaDeliveryCandidates =
			await this.getFullReplicaRepairCandidates(undefined, {
				includeSubscribers: false,
			});
		const { delivery, reliability, requireRecipients, minAcks } =
			this._parseDeliveryOptions(deliveryArg);
		const plans = this._nativeSharedLogState.planAppendForGidsBatch(
			{
				entries: entries.map((entry) => ({
					entryHash: entry.hash,
					gid: entry.meta.gid,
					hashNumber: this.getEntryHashNumber(entry),
					nextHashes: entry.meta.next,
					replicas,
				})),
				fullReplicaCandidates: fullReplicaDeliveryCandidates,
				selfHash: context.selfHash,
				deliveryEnabled: !!delivery,
				reliabilityAck: reliability === "ack",
				minAcks,
				requireRecipients,
			},
			this.createNativeLeaderOptions(context),
		);
		return plans.map((plan) => ({
			coordinates: plan.coordinates as NumberFromType<R>[],
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
			delivery: plan.delivery,
		}));
	}

	private async processLocalAppend(
		entry: Entry<T>,
		removed: ShallowOrFullEntry<T>[],
		options: SharedAppendOptions<T> | undefined,
		properties: {
			minReplicasValue: number;
			deferHeadCoordinatePersistence?: boolean;
			nativeAppendPlan?: NativeAppendEntryPlan<R>;
		},
	) {
		const deferHeadCoordinatePersistence =
			properties.deferHeadCoordinatePersistence ??
			(entry.meta.type !== EntryType.CUT &&
				this.shouldDeferHeadCoordinatePersistence(options));

		if (options?.replicate) {
			await this.replicate(entry, { checkDuplicates: true });
		}

		if (deferHeadCoordinatePersistence) {
			await this.deleteCoordinatesForHashes([
				...entry.meta.next,
				...removed.map((entry) => entry.hash),
			]);
			return;
		}

		const selfHash = this.node.identity.publicKey.hashcode();
		const target = options?.target;
		const deliveryArg = options?.delivery;
		let nativeAppendPlan = properties.nativeAppendPlan;
		if (!nativeAppendPlan && target !== "all") {
			nativeAppendPlan =
				target === "none"
					? await this.planNativeLocalAppendEntry(
							entry,
							properties.minReplicasValue,
						)
					: await this.planNativeAppendEntry(
						entry,
						properties.minReplicasValue,
						deliveryArg,
					);
		}
		let coordinates: NumberFromType<R>[];
		let leaders: LeaderMap;
		let isLeader: boolean;
		let nativeDeliveryPlan: AppendDeliveryPlan | undefined;
		if (nativeAppendPlan) {
			coordinates = nativeAppendPlan.coordinates;
			leaders = nativeAppendPlan.leaders;
			isLeader = nativeAppendPlan.isLeader;
			nativeDeliveryPlan = nativeAppendPlan.delivery;
			await this.persistCoordinate({
				leaders,
				coordinates,
				replicas: coordinates.length,
				entry,
				assignedToRangeBoundary: nativeAppendPlan.assignedToRangeBoundary,
				commitNative: false,
			});
		} else {
			({ coordinates, leaders, isLeader } = await this.planEntryLeaders(
				entry,
				properties.minReplicasValue,
				{
					persist: {},
				},
			));
		}

		if (options?.target !== "none") {
			const hasDelivery = !(deliveryArg === undefined || deliveryArg === false);

			if (target === "all" && hasDelivery) {
				throw new Error(
					`delivery options are not supported with target="all"; fanout broadcast is fire-and-forward`,
				);
			}
			if (target === "all" && !this._fanoutChannel) {
				throw new Error(
					`No fanout channel configured for shared-log topic ${this.topic}`,
				);
			}

			if (target === "all") {
				await this._appendDeliverToAllFanout(entry);
			} else {
				await this._appendDeliverToReplicators(
					entry,
					coordinates,
					properties.minReplicasValue,
					leaders,
					selfHash,
					isLeader,
					deliveryArg,
					nativeDeliveryPlan,
				);
			}
		}

		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		if (!isLeader && !delayAdaptiveRebalance) {
			this.pruneDebouncedFnAddIfNotKeeping({
				key: entry.hash,
				value: { entry, leaders },
			});
		}
		if (!delayAdaptiveRebalance) {
			this.rebalanceParticipationDebounced?.call();
		}
	}

	async open(options?: Args<T, D, R>): Promise<void> {
		this.replicas = {
			min:
				options?.replicas?.min != null
					? typeof options?.replicas?.min === "number"
						? new AbsoluteReplicas(options?.replicas?.min)
						: options?.replicas?.min
					: new AbsoluteReplicas(DEFAULT_MIN_REPLICAS),
			max: options?.replicas?.max
				? typeof options?.replicas?.max === "number"
					? new AbsoluteReplicas(options?.replicas?.max)
					: options.replicas.max
				: undefined,
		};
		this._logProperties = options;

		// TODO types
		this.domain = options?.domain
			? (options.domain(this) as D)
			: (createReplicationDomainHash(
					options?.compatibility && options?.compatibility < 10 ? "u32" : "u64",
				)(this) as D);
		this.indexableDomain = createIndexableDomainFromResolution(
			this.domain.resolution,
		);
		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 2e4;
		this._pendingDeletes = new Map();
		this._pendingIHave = new Map();
		this.latestReplicationInfoMessage = new Map();
		this._replicationInfoBlockedPeers = new Set();
		this._replicationInfoRequestByPeer = new Map();
		this._replicationInfoApplyQueueByPeer = new Map();
		this._repairRetryTimers = new Set();
		this._recentRepairDispatch = new Map();
		this._repairSweepRunning = false;
		this._repairSweepPendingModes = new Set();
		this._repairSweepPendingPeersByMode = createRepairPendingPeersByMode();
		this._repairFrontierByMode = createRepairFrontierByMode() as Map<
			RepairDispatchMode,
			Map<string, Map<string, EntryReplicated<R>>>
		>;
		this._repairFrontierActiveTargetsByMode = createRepairActiveTargetsByMode();
		this._repairSweepOptimisticGidPeersPending = new Map();
		this._entryKnownPeers = new Map();
		this._joinAuthoritativeRepairTimersByDelay = new Map();
		this._joinAuthoritativeRepairPeersByDelay = new Map();
		this._assumeSyncedRepairSuppressedUntil = 0;
		this._appendBackfillTimer = undefined;
		this._appendBackfillPendingByTarget = new Map();
		this._repairMetrics = createRepairMetrics();
		this._topicSubscribersCache = new Map();
		this.coordinateToHash = new Cache<string>({ max: 1e6, ttl: 1e4 });
		this.recentlyRebalanced = new Cache<string>({ max: 1e4, ttl: 1e5 });

		this.uniqueReplicators = new Set();
		this._replicatorJoinEmitted = new Set();
		this._replicatorsReconciled = false;
		this._replicatorLivenessSweepRunning = false;
		this._replicatorLivenessTimer = undefined;
		this._replicatorLivenessTargets = [];
		this._replicatorLivenessTargetsSize = 0;
		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures = new Map();
		this._replicatorLastActivityAt = new Map();
		this._lastLocalAppendAt = 0;
		const adaptiveReplicateOptions =
			options?.replicate && isAdaptiveReplicatorOption(options.replicate)
				? options.replicate
				: undefined;
		this.adaptiveRebalanceIdleMs = Math.max(
			ADAPTIVE_REBALANCE_MIN_IDLE_AFTER_LOCAL_APPEND_MS,
			(adaptiveReplicateOptions?.limits?.interval ??
				RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL) *
				ADAPTIVE_REBALANCE_IDLE_INTERVAL_MULTIPLIER,
		);

		this.openTime = +new Date();
		this.oldestOpenTime = this.openTime;
		this.distributionDebounceTime =
			options?.distributionDebounceTime || DEFAULT_DISTRIBUTION_DEBOUNCE_TIME; // expect > 0
		this.repairSweepTargetBufferSize = toPositiveInteger(
			options?.sync?.repairSweepTargetBufferSize,
			REPAIR_SWEEP_TARGET_BUFFER_SIZE,
			"sync.repairSweepTargetBufferSize",
		);

		this.timeUntilRoleMaturity =
			options?.timeUntilRoleMaturity ?? WAIT_FOR_ROLE_MATURITY;
		this.waitForReplicatorTimeout =
			options?.waitForReplicatorTimeout ?? WAIT_FOR_REPLICATOR_TIMEOUT;
		this.waitForReplicatorRequestIntervalMs =
			options?.waitForReplicatorRequestIntervalMs ??
			WAIT_FOR_REPLICATOR_REQUEST_INTERVAL;
		this.waitForReplicatorRequestMaxAttempts =
			options?.waitForReplicatorRequestMaxAttempts;
		this.waitForPruneDelay = options?.waitForPruneDelay ?? WAIT_FOR_PRUNE_DELAY;

		if (this.waitForReplicatorTimeout < this.timeUntilRoleMaturity) {
			this.waitForReplicatorTimeout = this.timeUntilRoleMaturity; // does not makes sense to expect a replicator to mature faster than it is reachable
		}

		if (this.waitForReplicatorRequestIntervalMs <= 0) {
			throw new Error(
				"waitForReplicatorRequestIntervalMs must be a positive number",
			);
		}
		if (
			this.waitForReplicatorRequestMaxAttempts != null &&
			this.waitForReplicatorRequestMaxAttempts <= 0
		) {
			throw new Error(
				"waitForReplicatorRequestMaxAttempts must be a positive number",
			);
		}

		this._closeController = new AbortController();
		this._closeController.signal.addEventListener("abort", () => {
			for (const [_peer, state] of this._replicationInfoRequestByPeer) {
				if (state.timer) clearTimeout(state.timer);
			}
			this._replicationInfoRequestByPeer.clear();
		});

		this._isTrustedReplicator = options?.canReplicate;
		this.keep = options?.keep;
		this.pendingMaturity = new Map();

		const id = sha256Base64Sync(this.log.id);
		const [storage, logScope] = await Promise.all([
			this.node.storage.sublevel(id),
			this.node.indexer.scope(id),
		]);

		const localBlocks = await new AnyBlockStore(await storage.sublevel("blocks"));
		const fanoutService = getSharedLogFanoutService(this.node.services);
		const blockProviderNamespace = (cid: string) => `cid:${cid}`;
		this.remoteBlocks = new RemoteBlocks({
			local: localBlocks,
			publish: (message, options) => this.rpc.send(new BlocksMessage(message), options),
			waitFor: this.rpc.waitFor.bind(this.rpc),
			publicKey: this.node.identity.publicKey,
			eagerBlocks: options?.eagerBlocks ?? true,
			resolveProviders: async (cid, opts) => {
				// 1) tracker-backed provider directory (best-effort, bounded)
				try {
					const providers = await fanoutService?.queryProviders(
						blockProviderNamespace(cid),
						{
							want: 8,
							timeoutMs: 2_000,
							queryTimeoutMs: 500,
							bootstrapMaxPeers: 2,
							signal: opts?.signal,
						},
					);
					if (providers && providers.length > 0) return providers;
				} catch {
					// ignore discovery failures
				}

				// 2) reuse the same per-hash / replicator / subscriber fallback used by
				// entry loads so block retries can widen beyond stale explicit hints.
				return (
					(await this.resolveCandidatePeersForHash(cid, {
						signal: opts?.signal,
						maxPeers: 8,
					})) ?? []
				);
			},
			watchProviders: fanoutService
				? (cid, opts) =>
						fanoutService.watchProviders(blockProviderNamespace(cid), {
							signal: opts.signal,
							want: 8,
							ttlMs: 10_000,
							renewIntervalMs: 5_000,
							bootstrapMaxPeers: 2,
							onProviders: (providers) =>
								opts.onProviders(providers.map((provider) => provider.hash)),
						})
				: undefined,
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

		const remoteBlocksStartPromise = this.remoteBlocks.start();
		const [replicationIndex, logIndex] = await Promise.all([
			logScope.scope("replication"),
			logScope.scope("log"),
		]);
		this._replicationRangeIndex = await replicationIndex.init({
			schema: this.indexableDomain.constructorRange,
		});
		this._entryCoordinatesIndex = await replicationIndex.init({
			schema: this.indexableDomain.constructorEntry,
		});
		await this.openNativeRangePlanner(options?.nativeRangePlanner);

		await remoteBlocksStartPromise;
		const hasIndexedReplicationInfo =
			(await this.replicationIndex.count({
				query: [
					new StringMatch({
						key: "hash",
						value: this.node.identity.publicKey.hashcode(),
					}),
				],
			})) > 0;

		this._gidPeersHistory = new Map();
		this._requestIPruneSent = new Map();
		this._requestIPruneResponseReplicatorSet = new Map();
		this._checkedPruneRetries = new Map();

		this.replicationChangeDebounceFn = debounceAggregationChanges<
			ReplicationRangeIndexable<R>
		>(
			(change) =>
				this.onReplicationChange(change).then(() =>
					this.rebalanceParticipationDebounced?.call(),
				),
			this.distributionDebounceTime,
		);

		this.pruneDebouncedFn = debouncedAccumulatorMap(
			async (map) => {
				const current = new Map<
					string,
					{
						entry: CheckedPruneEntry<T, R>;
						leaders: CheckedPruneLeaderMap;
					}
				>();
				const selfReplicating = await this.isReplicating();
				for (const [hash, value] of map) {
					const checkedPruneLeaders = await this.resolveCheckedPruneLeaders({
						hash,
						entry: value.entry,
						leaders: value.leaders,
						selfReplicating,
					});
					if (checkedPruneLeaders.localLeader) {
						await this.cancelCheckedPruneForLocalLeader(hash);
						continue;
					}
					current.set(hash, {
						...value,
						leaders: checkedPruneLeaders.leaders,
					});
				}
				if (current.size > 0) {
					this.prune(current);
				}
			},
			PRUNE_DEBOUNCE_INTERVAL, // TODO make this dynamic on the number of replicators
			(into, from) => {
				for (const [k, v] of from.leaders) {
					if (!into.leaders.has(k)) {
						into.leaders.set(k, v);
					}
				}
			},
		);

		this.responseToPruneDebouncedFn = debounceAccumulator<
			string,
			{
				hashes: string[];
				peers: string[] | Set<string>;
			},
			Map<string, Set<string>>
		>(
			(result) => {
				let allRequestingPeers = new Set<string>();
				let hashes: string[] = [];
				for (const [hash, requestingPeers] of result) {
					for (const peer of requestingPeers) {
						allRequestingPeers.add(peer);
					}
					hashes.push(hash);
				}

				hashes.length > 0 &&
					this.rpc.send(new ResponseIPrune({ hashes }), {
						mode: new SilentDelivery({
							to: allRequestingPeers,
							redundancy: 1,
						}),
						priority: 1,
					});
			},
			() => {
				let accumulator = new Map<string, Set<string>>();
				return {
					add: (props: { hashes: string[]; peers: string[] | Set<string> }) => {
						for (const hash of props.hashes) {
							let prev = accumulator.get(hash);
							if (!prev) {
								prev = new Set<string>();
								accumulator.set(hash, prev);
							}
							for (const peer of props.peers) {
								prev.add(peer);
							}
						}
					},
					delete: (hash: string) => {
						accumulator.delete(hash);
					},
					finalize: () => {
						return undefined as any;
					},
					size: () => accumulator.size,
					clear: () => accumulator.clear(),
					value: accumulator,
					has: (hash: string) => accumulator.has(hash),
				};
			},
			PRUNE_DEBOUNCE_INTERVAL, // TODO make this dynamic on the number of replicators
		);

		await this.log.open(this.remoteBlocks, this.node.identity, {
			keychain: this.node.services.keychain,
			resolveRemotePeers: (hash, options) =>
				this.resolveCandidatePeersForHash(hash, {
					signal: options?.signal,
					maxPeers: 8,
				}),
			...this._logProperties,
			nativeGraph: this._logProperties?.nativeGraph ?? { optional: true },
			onChange: async (change) => {
				await this.onChange(change);
				return this._logProperties?.onChange?.(change);
			},
			canAppend: async (entry) => {
				if (!(await this.canAppend(entry))) {
					return false;
				}
				return this._logProperties?.canAppend?.(entry) ?? true;
			},
			trim: this._logProperties?.trim && {
				...this._logProperties?.trim,
			},
			indexer: logIndex,
		});
		const resolveHashesForSymbols = (symbols: bigint[]) =>
			this._nativeSharedLogState?.getEntryHashesForHashNumbers(symbols);
		const resolveHashNumbersInRange = (range: {
			start1: bigint | number;
			end1: bigint | number;
			start2: bigint | number;
			end2: bigint | number;
		}) => this._nativeSharedLogState?.getEntryHashNumbersInRange(range);

		if (options?.syncronizer) {
			this.syncronizer = new options.syncronizer({
				numbers: this.indexableDomain.numbers,
				entryIndex: this.entryCoordinatesIndex,
				log: this.log,
				rangeIndex: this._replicationRangeIndex,
				rpc: this.rpc,
				coordinateToHash: this.coordinateToHash,
				resolveHashesForSymbols,
				resolveHashNumbersInRange,
				sync: options?.sync,
			});
		} else {
			if (
				this._logProperties?.compatibility &&
				this._logProperties.compatibility < 10
			) {
				this.syncronizer = new SimpleSyncronizer({
					log: this.log,
					rpc: this.rpc,
					entryIndex: this.entryCoordinatesIndex,
					coordinateToHash: this.coordinateToHash,
					resolveHashesForSymbols,
					sync: options?.sync,
				});
			} else {
				if (this.domain.resolution === "u32") {
					warn(
						"u32 resolution is not recommended for RatelessIBLTSynchronizer",
					);
				}

				this.syncronizer = new RatelessIBLTSynchronizer<R>({
					numbers: this.indexableDomain.numbers,
					entryIndex: this.entryCoordinatesIndex,
					log: this.log,
					rangeIndex: this._replicationRangeIndex,
					rpc: this.rpc,
					coordinateToHash: this.coordinateToHash,
					resolveHashesForSymbols,
					resolveHashNumbersInRange,
					sync: options?.sync,
				}) as Syncronizer<R>;
			}
		}

		// Open for communcation
		this._onSubscriptionFn =
			this._onSubscriptionFn || this._onSubscription.bind(this);
		this._onUnsubscriptionFn =
			this._onUnsubscriptionFn || this._onUnsubscription.bind(this);
		await Promise.all([
			this.rpc.open({
				queryType: TransportMessage,
				responseType: TransportMessage,
				responseHandler: (query, context) => this.onMessage(query, context),
				topic: this.topic,
			}),
			this.node.services.pubsub.addEventListener(
				"subscribe",
				this._onSubscriptionFn,
			),
			this.node.services.pubsub.addEventListener(
				"unsubscribe",
				this._onUnsubscriptionFn,
			),
		]);

		const fanoutOpenPromise = this._openFanoutChannel(options?.fanout);
		// Mark previously-owned replication ranges as "new" only when they already exist.
		// Fresh opens have nothing to touch here, so skip the extra scan/write entirely.
		const updateOwnedReplicationPromise = hasIndexedReplicationInfo
			? this.updateTimestampOfOwnedReplicationRanges()
			: Promise.resolve();
		await Promise.all([fanoutOpenPromise, updateOwnedReplicationPromise]);

		// if we had a previous session with replication info, and new replication info dictates that we unreplicate
		// we should do that. Otherwise if options is a unreplication we dont need to do anything because
		// we are already unreplicated (as we are just opening)

		const isUnreplicationOptionsDefined = isUnreplicationOptions(
			options?.replicate,
		);

		const canResumeReplication =
			hasIndexedReplicationInfo &&
			(await isReplicationOptionsDependentOnPreviousState(
				options?.replicate,
				this.replicationIndex,
				this.node.identity.publicKey,
			));

		if (hasIndexedReplicationInfo && isUnreplicationOptionsDefined) {
			await this.replicate(options?.replicate, { checkDuplicates: true });
		} else if (canResumeReplication) {
			// dont do anthing since we are alread replicating stuff
		} else {
			await this.replicate(options?.replicate, {
				checkDuplicates: true,
				reset: true,
			});
		}
		await this.syncronizer.open();

		this.interval = setInterval(() => {
			this.rebalanceParticipationDebounced?.call();
		}, RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL);
	}

	private toNativeReplicationRange(
		range: ReplicationRangeIndexable<R>,
	): NativeReplicationRange {
		return {
			id: range.idString,
			hash: range.hash,
			timestamp: range.timestamp,
			start1: range.start1,
			end1: range.end1,
			start2: range.start2,
			end2: range.end2,
			width: range.width,
			mode: range.mode,
		};
	}

	private putNativeReplicationRange(range: ReplicationRangeIndexable<R>): void {
		const nativeRange = this.toNativeReplicationRange(range);
		this._nativeRangePlanner?.put(nativeRange);
		this._nativeSharedLogState?.put(nativeRange);
	}

	private deleteNativeReplicationRange(range: ReplicationRangeIndexable<R>): void {
		this._nativeRangePlanner?.delete(range.idString);
		this._nativeSharedLogState?.delete(range.idString);
	}

	private async hydrateNativeRangePlanner(
		planner: Pick<SharedLogRangePlanner, "clear" | "put">,
	): Promise<void> {
		planner.clear();
		const iterator = this.replicationIndex.iterate();
		try {
			for (;;) {
				const batch = await iterator.next(256);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					planner.put(this.toNativeReplicationRange(result.value));
				}
			}
		} finally {
			await iterator.close();
		}
	}

	private async hydrateNativeSharedLogState(
		state: SharedLogNativeState,
	): Promise<void> {
		state.clearEntryCoordinates();
		this._residentEntryCoordinatesByHash = new Map();
		const iterator = this.entryCoordinatesIndex.iterate({});
		try {
			for (;;) {
				const batch = await iterator.next(256);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					const requestedReplicas = decodeReplicas(result.value).getValue(this);
					state.putEntryCoordinates(
						result.value.hash,
						result.value.gid,
						result.value.coordinates,
						result.value.assignedToRangeBoundary,
						requestedReplicas,
						result.value.hashNumber,
					);
					this._residentEntryCoordinatesByHash.set(
						result.value.hash,
						result.value,
					);
				}
			}
		} finally {
			await iterator.close();
		}
	}

	private async openNativeRangePlanner(
		options: SharedLogOptions<T, D, R>["nativeRangePlanner"],
	): Promise<void> {
		this._nativeRangePlanner = undefined;
		this._nativeSharedLogState = undefined;
		this._residentEntryCoordinatesByHash = undefined;
		if (options === false) {
			return;
		}

		try {
			const [planner, state] = await Promise.all([
				createRangePlanner(this.domain.resolution),
				createSharedLogState(this.domain.resolution),
			]);
			await Promise.all([
				this.hydrateNativeRangePlanner(planner),
				this.hydrateNativeRangePlanner(state),
			]);
			await this.hydrateNativeSharedLogState(state);
			this._nativeRangePlanner = planner;
			this._nativeSharedLogState = state;
		} catch (error) {
			this._residentEntryCoordinatesByHash = undefined;
			if (options?.optional === false) {
				throw error;
			}
			warn(
				`Native range planner unavailable, falling back to TypeScript getSamples: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
		}
	}

	private async updateTimestampOfOwnedReplicationRanges(
		timestamp: number = +new Date(),
	) {
		const all = await this.replicationIndex
			.iterate({
				query: { hash: this.node.identity.publicKey.hashcode() },
			})
			.all();
		let bnTimestamp = BigInt(timestamp);
		for (const x of all) {
			x.value.timestamp = bnTimestamp;
			await this.replicationIndex.put(x.value);
			this.putNativeReplicationRange(x.value);
		}

		if (all.length > 0) {
			// emit mature event
			const maturityTimeout = setTimeout(
				() => {
					this.events.dispatchEvent(
						new CustomEvent<ReplicationChangeEvent>("replicator:mature", {
							detail: { publicKey: this.node.identity.publicKey },
						}),
					);
				},
				await this.getDefaultMinRoleAge(),
			);
			this._closeController.signal.addEventListener("abort", () => {
				clearTimeout(maturityTimeout);
			});
		}
	}

	async afterOpen(): Promise<void> {
		await super.afterOpen();
		const existingSubscribersPromise = this._getTopicSubscribers(this.topic);

		// We do this here, because these calls requires this.closed == false
		void this.pruneOfflineReplicators()
			.then(() => {
				this._replicatorsReconciled = true;
			})
			.catch((error) => {
				if (isNotStartedError(error as Error)) {
					return;
				}
				logger.error(error);
			});

		this.startReplicatorLivenessSweep();

		await this.rebalanceParticipation();

		// Take into account existing subscription
		(await existingSubscribersPromise)?.forEach((v) => {
			if (v.equals(this.node.identity.publicKey)) {
				return;
			}
			if (this.closed) {
				return;
			}
			this.handleSubscriptionChange(v, [this.topic], true);
		});
	}

	async reset() {
		await this.log.load({ reset: true });
	}

	async pruneOfflineReplicators() {
		// Go through all segments and wait for replicators to become reachable;
		// otherwise prune them away from the local membership view.
		try {
			const promises: Promise<any>[] = [];
			const iterator = this.replicationIndex.iterate();
			const checkedIsAlive = new Set<string>();

			while (!iterator.done()) {
				for (const segment of await iterator.next(1000)) {
					if (
						checkedIsAlive.has(segment.value.hash) ||
						this.node.identity.publicKey.hashcode() === segment.value.hash
					) {
						this.uniqueReplicators.add(this.node.identity.publicKey.hashcode());
						continue;
					}

					checkedIsAlive.add(segment.value.hash);

					promises.push(
						waitForSubscribers(this.node, segment.value.hash, this.rpc.topic, {
							timeout: this.waitForReplicatorTimeout,
							signal: this._closeController.signal,
						})
							.then(async () => {
								const key = await this._resolvePublicKeyFromHash(
									segment.value.hash,
								);
								if (!key) {
									throw new Error(
										"Failed to resolve public key from hash: " +
											segment.value.hash,
									);
								}

								const keyHash = key.hashcode();
								this.uniqueReplicators.add(keyHash);

								if (!this._replicatorJoinEmitted.has(keyHash)) {
									this._replicatorJoinEmitted.add(keyHash);
									this.events.dispatchEvent(
										new CustomEvent<ReplicatorJoinEvent>("replicator:join", {
											detail: { publicKey: key },
										}),
									);
									this.events.dispatchEvent(
										new CustomEvent<ReplicationChangeEvent>("replication:change", {
											detail: { publicKey: key },
										}),
									);
								}
							})
							.catch(async (error) => {
								if (isNotStartedError(error as Error)) {
									return;
								}

								return this.removeReplicator(segment.value.hash, {
									noEvent: true,
								});
							}),
					);
				}
			}

			return Promise.all(promises);
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return;
			}
			throw error;
		}
	}

	private startReplicatorLivenessSweep() {
		if (this._replicatorLivenessTimer) {
			return;
		}
		this._replicatorLivenessTimer = setInterval(() => {
			void this.runReplicatorLivenessSweep();
		}, REPLICATOR_LIVENESS_SWEEP_INTERVAL_MS);
		this._replicatorLivenessTimer.unref?.();
	}

	private stopReplicatorLivenessSweep() {
		if (this._replicatorLivenessTimer) {
			clearInterval(this._replicatorLivenessTimer);
			this._replicatorLivenessTimer = undefined;
		}
		this._replicatorLivenessSweepRunning = false;
		this._replicatorLivenessTargets = [];
		this._replicatorLivenessTargetsSize = 0;
		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures.clear();
		this._replicatorLastActivityAt.clear();
	}

	private rebuildReplicatorLivenessTargets() {
		const selfHash = this.node.identity.publicKey.hashcode();
		this._replicatorLivenessTargets = [...this.uniqueReplicators].filter(
			(hash) => hash !== selfHash,
		);
		this._replicatorLivenessTargetsSize = this.uniqueReplicators.size;
		if (this._replicatorLivenessCursor >= this._replicatorLivenessTargets.length) {
			this._replicatorLivenessCursor = 0;
		}
	}

	private getReplicatorLivenessTargets() {
		const selfHash = this.node.identity.publicKey.hashcode();
		const expected =
			this.uniqueReplicators.size - (this.uniqueReplicators.has(selfHash) ? 1 : 0);

		if (this._replicatorLivenessTargets.length > 0) {
			// Keep the cursor stable, but purge stale hashes (membership can change while
			// the total size stays constant).
			this._replicatorLivenessTargets = this._replicatorLivenessTargets.filter(
				(hash) => hash !== selfHash && this.uniqueReplicators.has(hash),
			);
		}

		if (
			this._replicatorLivenessTargetsSize !== this.uniqueReplicators.size ||
			this._replicatorLivenessTargets.length !== expected
		) {
			this.rebuildReplicatorLivenessTargets();
		}

		return this._replicatorLivenessTargets;
	}

	private cleanupPeerDisconnectTracking(peerHash: string) {
		this.cancelReplicationInfoRequests(peerHash);
		this._replicatorLivenessFailures.delete(peerHash);
		this._replicatorLastActivityAt.delete(peerHash);

		for (const [hash, peers] of this._requestIPruneSent) {
			peers.delete(peerHash);
			if (peers.size === 0) {
				this._requestIPruneSent.delete(hash);
			}
		}

		for (const [hash, peers] of this._requestIPruneResponseReplicatorSet) {
			peers.delete(peerHash);
			if (peers.size === 0) {
				this._requestIPruneResponseReplicatorSet.delete(hash);
			}
		}
	}

	private markReplicatorActivity(peerHash: string, now = Date.now()) {
		this._replicatorLastActivityAt.set(peerHash, now);
	}

	private hasRecentReplicatorActivity(peerHash: string, now = Date.now()) {
		const lastActivityAt = this._replicatorLastActivityAt.get(peerHash);
		if (
			lastActivityAt != null &&
			now - lastActivityAt < REPLICATOR_LIVENESS_IDLE_THRESHOLD_MS
		) {
			this._replicatorLivenessFailures.delete(peerHash);
			return true;
		}
		return false;
	}

	private async evictReplicatorFromLiveness(
		peerHash: string,
		publicKey: PublicSignKey,
	) {
		const wasReplicator = this.uniqueReplicators.has(peerHash);
		const watermark = BigInt(+new Date());
		const previousWatermark = this.latestReplicationInfoMessage.get(peerHash);
		if (!previousWatermark || previousWatermark < watermark) {
			this.latestReplicationInfoMessage.set(peerHash, watermark);
		}

		try {
			await this.removeReplicator(publicKey, { noEvent: true });
		} catch (error) {
			if (!isNotStartedError(error as Error)) {
				throw error;
			}
		}

		this.cleanupPeerDisconnectTracking(peerHash);

		if (wasReplicator) {
			this.events.dispatchEvent(
				new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
					detail: { publicKey },
				}),
			);
		}

		if (!this._replicationInfoBlockedPeers.has(peerHash)) {
			this.scheduleReplicationInfoRequests(publicKey);
		}
		this._replicatorLivenessTargetsSize = -1;
	}

	private async resolveCandidatePeersForHash(
		hash: string,
		options?: { signal?: AbortSignal; maxPeers?: number },
	): Promise<string[] | undefined> {
		if (options?.signal?.aborted) return undefined;

		const maxPeers = options?.maxPeers ?? 8;
		const self = this.node.identity.publicKey.hashcode();
		const seed = hashToSeed32(hash);

		const hinted = this._requestIPruneResponseReplicatorSet.get(hash);
		if (hinted && hinted.size > 0) {
			const peers = [...hinted].filter((p) => p !== self);
			return peers.length > 0
				? pickDeterministicSubset(peers, seed, maxPeers)
				: undefined;
		}

		const contacted = this._requestIPruneSent.get(hash);
		if (contacted && contacted.size > 0) {
			const peers = [...contacted].filter((p) => p !== self);
			return peers.length > 0
				? pickDeterministicSubset(peers, seed, maxPeers)
				: undefined;
		}

		let candidates: string[] | undefined;
		const replicatorCandidates = [...this.uniqueReplicators].filter((p) => p !== self);
		if (replicatorCandidates.length > 0) {
			candidates = replicatorCandidates;
		} else {
			try {
				const subscribers = await this._getTopicSubscribers(this.topic);
				const subscriberCandidates =
					subscribers?.map((k) => k.hashcode()).filter((p) => p !== self) ?? [];
				candidates =
					subscriberCandidates.length > 0 ? subscriberCandidates : undefined;
			} catch {
				// Best-effort only.
			}

			if (!candidates || candidates.length === 0) {
				const peerMap = (this.node.services.pubsub as any)?.peers;
				if (peerMap?.keys) {
					candidates = [...peerMap.keys()];
				}
			}

			if (!candidates || candidates.length === 0) {
				const connectionManager = (this.node.services.pubsub as any)?.components
					?.connectionManager;
				const connections = connectionManager?.getConnections?.() ?? [];
				const connectionHashes: string[] = [];
				for (const conn of connections) {
					const peerId = conn?.remotePeer;
					if (!peerId) continue;
					try {
						connectionHashes.push(getPublicKeyFromPeerId(peerId).hashcode());
					} catch {
						// Best-effort only.
					}
				}
				if (connectionHashes.length > 0) {
					candidates = connectionHashes;
				}
			}
		}

		if (!candidates || candidates.length === 0) return undefined;
		const peers = candidates.filter((p) => p !== self);
		if (peers.length === 0) return undefined;
		return pickDeterministicSubset(peers, seed, maxPeers);
	}

	private async runReplicatorLivenessSweep() {
		if (this.closed || this._closeController.signal.aborted) {
			return;
		}
		if (this._replicatorLivenessSweepRunning) {
			return;
		}

		const targets = this.getReplicatorLivenessTargets();
		if (targets.length === 0) {
			return;
		}

		this._replicatorLivenessSweepRunning = true;
		try {
			if (this._replicatorLivenessCursor >= targets.length) {
				this._replicatorLivenessCursor = 0;
			}
			const peerHash = targets[this._replicatorLivenessCursor]!;
			this._replicatorLivenessCursor =
				(this._replicatorLivenessCursor + 1) % targets.length;
			await this.probeReplicatorLiveness(peerHash);
		} catch (error) {
			if (!isNotStartedError(error as Error)) {
				logger.error((error as any)?.toString?.() ?? String(error));
			}
		} finally {
			this._replicatorLivenessSweepRunning = false;
		}
	}

	private async probeReplicatorLiveness(peerHash: string) {
		if (this.closed || this._closeController.signal.aborted) {
			return;
		}
		if (!this.uniqueReplicators.has(peerHash)) {
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}

		const publicKey = await this._resolvePublicKeyFromHash(peerHash);
		if (!publicKey) {
			try {
				await this.removeReplicator(peerHash, { noEvent: true });
			} catch (error) {
				if (!isNotStartedError(error as Error)) {
					throw error;
				}
			}
			this.cleanupPeerDisconnectTracking(peerHash);
			this._replicatorLivenessTargetsSize = -1;
			return;
		}

		try {
			// Explicit ping (ACKed) instead of RequestReplicationInfoMessage to avoid
			// triggering large segment snapshots just to prove liveness.
			await this.rpc.send(new ReplicationPingMessage(), {
				mode: new AcknowledgeDelivery({ redundancy: 1, to: [publicKey] }),
				priority: ACK_CONTROL_PRIORITY,
				responsePriority: ACK_CONTROL_PRIORITY,
			});
			this.markReplicatorActivity(peerHash);
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return;
			}
		}

		// Relay-backed prod paths can keep a peer subscribed/reachable even if an
		// ACKed liveness ping gets delayed or dropped under load. Treat observed
		// topic presence as a positive liveness signal before evicting the peer.
		if (await this.confirmReplicatorSubscriberPresence(peerHash)) {
			this.markReplicatorActivity(peerHash);
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}

		const failures = (this._replicatorLivenessFailures.get(peerHash) ?? 0) + 1;
		this._replicatorLivenessFailures.set(peerHash, failures);
		this.scheduleReplicationInfoRequests(publicKey);

		if (failures < REPLICATOR_LIVENESS_PROBE_FAILURES_TO_EVICT) {
			return;
		}
		if (!this.uniqueReplicators.has(peerHash)) {
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}

		await this.evictReplicatorFromLiveness(peerHash, publicKey);
	}

	private async confirmReplicatorSubscriberPresence(peerHash: string) {
		try {
			const subscribers = await this._getTopicSubscribers(this.rpc.topic);
			if (subscribers?.some((subscriber) => subscriber.hashcode() === peerHash)) {
				return true;
			}
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return false;
			}
		}

		try {
			await waitForSubscribers(this.node, peerHash, this.rpc.topic, {
				signal: this._closeController.signal,
				timeout: Math.max(
					1_000,
					Math.min(5_000, Math.floor(this.waitForReplicatorTimeout / 4)),
				),
			});
			return true;
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return false;
			}
			return false;
		}
	}

	async getMemoryUsage() {
		return this.log.blocks.size();
		/* ((await this.log.entryIndex?.getMemoryUsage()) || 0) */ // + (await this.log.blocks.size())
	}

	private clampReplicas(value: number) {
		const lower = this.replicas.min?.getValue(this) || 1;
		const higher = this.replicas.max?.getValue(this) ?? Number.MAX_SAFE_INTEGER;
		return Math.max(Math.min(higher, value), lower);
	}

	private async getMaxReplicasFromHeads(gid: string) {
		const nativeMax = await this.log.entryIndex.getMaxHeadDataU32(gid);
		if (nativeMax != null) {
			return this.clampReplicas(nativeMax);
		}
		const headsWithGid = (await this.log.entryIndex
			.getHeads(gid, {
				type: "shape",
				shape: { meta: { data: true } },
			})
			.all()) as { meta: { data?: Uint8Array } }[];
		if (headsWithGid.length === 0) {
			return undefined;
		}
		return maxReplicas(this, headsWithGid.values());
	}

	private async getMaxReplicasFromHeadsBatch(gids: Iterable<string>) {
		const uniqueGids = [...new Set([...gids].filter(Boolean))];
		const out = new Map<string, number | undefined>();
		if (uniqueGids.length === 0) {
			return out;
		}

		const nativeMaxes =
			await this.log.entryIndex.getMaxHeadDataU32Batch(uniqueGids);
		if (nativeMaxes != null) {
			for (let i = 0; i < uniqueGids.length; i++) {
				const gid = uniqueGids[i]!;
				const nativeMax = nativeMaxes[i];
				out.set(
					gid,
					nativeMax == null ? undefined : this.clampReplicas(nativeMax),
				);
			}
			return out;
		}

		await Promise.all(
			uniqueGids.map(async (gid) => {
				out.set(gid, await this.getMaxReplicasFromHeads(gid));
			}),
		);
		return out;
	}

	private async hasHeadForGid(gid: string) {
		const nativeHasHead = await this.log.entryIndex.hasHead(gid);
		if (nativeHasHead != null) {
			return nativeHasHead;
		}
		const heads = await this.log.entryIndex
			.getHeads(gid, {
				type: "shape",
				shape: { hash: true },
			})
			.all();
		return heads.length > 0;
	}

	private async hasAnyHeadForGids(gids: string[]) {
		const uniqueGids = [...new Set(gids.filter(Boolean))];
		if (uniqueGids.length === 0) {
			return false;
		}
		const nativeHasHead = await this.log.entryIndex.hasAnyHead(uniqueGids);
		if (nativeHasHead != null) {
			return nativeHasHead;
		}
		for (const gid of uniqueGids) {
			if (await this.hasHeadForGid(gid)) {
				return true;
			}
		}
		return false;
	}

	private async hasAnyHeadForGidSets(gidSets: string[][]) {
		const nativeHasHeads = await this.log.entryIndex.hasAnyHeadBatch(gidSets);
		if (nativeHasHeads != null) {
			return nativeHasHeads;
		}
		const out: boolean[] = [];
		for (const gids of gidSets) {
			out.push(await this.hasAnyHeadForGids(gids));
		}
		return out;
	}

	get topic() {
		return this.log.idString;
	}

	async onChange(change: Change<T>) {
		for (const added of change.added) {
			this.onEntryAdded(added.entry);
		}
		for (const removed of change.removed) {
			await this.deleteCoordinates({ hash: removed.hash });
			this.onEntryRemoved(removed.hash);
		}
	}

	async canAppend(entry: Entry<T>) {
		try {
			if (!entry.meta.data) {
				warn("Received entry without meta data, skipping");
				return false;
			}
			const replicas = decodeReplicas(entry).getValue(this);
			if (Number.isFinite(replicas) === false) {
				return false;
			}

			checkMinReplicasLimit(replicas);

			// Don't verify entries that we have created (TODO should we? perf impact?)
			if (!entry.createdLocally && !(await entry.verifySignatures())) {
				return false;
			}
			return true;
		} catch (error) {
			if (error instanceof BorshError || error instanceof ReplicationError) {
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}
	}

	async getCover(
		properties:
			| { args?: ExtractDomainArgs<D> }
			| { range: CoverRange<NumberFromType<R>> },
		options?: {
			reachableOnly?: boolean;
			roleAge?: number;
			eager?:
				| {
						unmaturedFetchCoverSize?: number;
				  }
				| boolean;
			signal?: AbortSignal;
		},
	) {
		// Check if aborted before starting
		if (options?.signal?.aborted) {
			return [];
		}

		// Return empty array if closed/closing to avoid NotStartedError/ClosedError
		// This can happen during component unmount while remote queries are in flight
		if (this.closed || !this._replicationRangeIndex) {
			return [];
		}

		try {
			let roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge());
			let eager = options?.eager ?? false;
			let range: CoverRange<NumberFromType<R>>;
			if (properties && "range" in properties) {
				range = properties.range;
			} else {
				range = await this.domain.fromArgs(properties.args);
			}

			// Check abort signal after async operations
			if (options?.signal?.aborted) {
				return [];
			}

			const width =
				range.length ??
				(await minimumWidthToCover<R>(
					this.replicas.min.getValue(this),
					this.indexableDomain.numbers,
				));

			// Check abort signal before expensive getCoverSet
			if (options?.signal?.aborted) {
				return [];
			}

			const set = await getCoverSet<R>({
				peers: this.replicationIndex,
				start: range.offset,
				widthToCoverScaled: width,
				roleAge,
				eager,
				numbers: this.indexableDomain.numbers,
			});

				// Check abort signal before building result
				if (options?.signal?.aborted) {
					return [];
				}

				// add all in flight
				for (const [key, _] of this.syncronizer.syncInFlight) {
					set.add(key);
				}

				const selfHash = this.node.identity.publicKey.hashcode();

				if (options?.reachableOnly) {
					const directPeers: Map<string, unknown> | undefined = (this.node.services
						.pubsub as any)?.peers;

					// Prefer the live pubsub subscriber set when filtering reachability. In some
					// flows peers can be reachable/active even before (or without) subscriber
					// state converging, so also consider direct pubsub peers.
					const subscribers =
						(await this._getTopicSubscribers(this.topic)) ?? undefined;
					const subscriberHashcodes = subscribers
						? new Set(subscribers.map((key) => key.hashcode()))
					: undefined;

				// If reachability is requested but we have no basis for filtering yet
				// (subscriber snapshot hasn't converged), return the full cover set.
				// Otherwise, only keep peers we can currently reach.
				const canFilter =
					directPeers != null ||
					(subscriberHashcodes && subscriberHashcodes.size > 0);
				if (!canFilter) {
					return [...set];
				}

				const reachable: string[] = [];
				for (const peer of set) {
					if (peer === selfHash) {
						reachable.push(peer);
						continue;
					}
					if (
						(subscriberHashcodes && subscriberHashcodes.has(peer)) ||
						(directPeers && directPeers.has(peer))
					) {
						reachable.push(peer);
					}
				}
				return reachable;
				}

				return [...set];
			} catch (error) {
			// Handle race conditions where the index gets closed during the operation
			if (isNotStartedError(error as Error)) {
				return [];
			}
			throw error;
		}
	}

	private async _close() {
		await this.syncronizer.close();

		for (const [_key, peerMap] of this.pendingMaturity) {
			for (const [_key2, info] of peerMap) {
				clearTimeout(info.timeout);
			}
		}

		this.pendingMaturity.clear();

		this.distributeQueue?.clear();
		this._closeFanoutChannel();
		try {
			this._providerHandle?.close();
		} catch {
			// ignore
		}
		this._providerHandle = undefined;
		this.coordinateToHash.clear();
		this.recentlyRebalanced.clear();
		this.uniqueReplicators.clear();
		this._topicSubscribersCache.clear();
		this._closeController.abort();

		clearInterval(this.interval);
		this.stopReplicatorLivenessSweep();

		this.node.services.pubsub.removeEventListener(
			"subscribe",
			this._onSubscriptionFn,
		);

		this.node.services.pubsub.removeEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn,
		);
		for (const timer of this._repairRetryTimers) {
			clearTimeout(timer);
		}
		this._repairRetryTimers.clear();
		this._recentRepairDispatch.clear();
		this._repairSweepRunning = false;
		this._repairSweepPendingModes.clear();
		for (const peers of this._repairSweepPendingPeersByMode.values()) {
			peers.clear();
		}
		this._repairSweepOptimisticGidPeersPending.clear();
		this._entryKnownPeers.clear();
		this._nativeSharedLogState?.clearEntryKnownPeers();
		for (const timer of this._joinAuthoritativeRepairTimersByDelay.values()) {
			clearTimeout(timer);
		}
		this._joinAuthoritativeRepairTimersByDelay.clear();
		this._joinAuthoritativeRepairPeersByDelay.clear();
		for (const targets of this._repairFrontierByMode.values()) {
			targets.clear();
		}
		for (const targets of this._repairFrontierActiveTargetsByMode.values()) {
			targets.clear();
		}
		if (this._appendBackfillTimer) {
			clearTimeout(this._appendBackfillTimer);
			this._appendBackfillTimer = undefined;
		}
		this._appendBackfillPendingByTarget.clear();

		for (const [_k, v] of this._pendingDeletes) {
			v.clear();
			v.promise.resolve(); // TODO or reject?
		}
		for (const [_k, v] of this._pendingIHave) {
			v.clear();
		}
		for (const [_k, v] of this._checkedPruneRetries) {
			if (v.timer) clearTimeout(v.timer);
		}

		await this.remoteBlocks.stop();
		this._pendingDeletes.clear();
		this._pendingIHave.clear();
		this._checkedPruneRetries.clear();
		this.latestReplicationInfoMessage.clear();
		this._gidPeersHistory.clear();
		this._nativeSharedLogState?.clearGidPeers();
		this._requestIPruneSent.clear();
		this._requestIPruneResponseReplicatorSet.clear();
		// Cancel any pending debounced timers so they can't fire after we've torn down
		// indexes/RPC state.
		this.rebalanceParticipationDebounced?.close();
		this.replicationChangeDebounceFn?.close?.();
		this.pruneDebouncedFn?.close?.();
		this.responseToPruneDebouncedFn?.close?.();
		this.pruneDebouncedFn = undefined as any;
		this.rebalanceParticipationDebounced = undefined;
		this._replicationRangeIndex.stop();
		this._entryCoordinatesIndex.stop();
		this._replicationRangeIndex = undefined as any;
		this._entryCoordinatesIndex = undefined as any;
		this._nativeRangePlanner = undefined;
		this._nativeSharedLogState = undefined;
		this._residentEntryCoordinatesByHash = undefined;

		this.cpuUsage?.stop?.();
		/* this._totalParticipation = 0; */
	}
			async close(from?: Program): Promise<boolean> {
				// Best-effort: announce that we are going offline before tearing down
				// RPC/subscription state.
			//
				// Important: do not delete our local replication ranges here. Keeping them
				// allows `replicate: { type: "resume" }` to restore the previous role on
				// restart. Explicit `unreplicate()` still clears local state.
				try {
					if (!this.closed) {
						// Prevent any late debounced timers (rebalance/prune) from publishing
						// replication info after we announce "segments: []". These races can leave
						// stale segments on remotes after rapid open/close cycles.
						this._isReplicating = false;
						this._isAdaptiveReplicating = false;
						this.rebalanceParticipationDebounced?.close();
						this.replicationChangeDebounceFn?.close?.();
						this.pruneDebouncedFn?.close?.();
						this.responseToPruneDebouncedFn?.close?.();

						// Ensure the "I'm leaving" replication reset is actually published before
						// the RPC child program closes and unsubscribes from its topic. If we fire
						// and forget here, the publish can race with `super.close()` and get dropped,
						// leaving stale replication segments on remotes (flaky join/leave tests).
						// Also ensure close is bounded even when shard overlays are mid-reconcile.
						const abort = new AbortController();
						const abortTimer = setTimeout(() => {
							try {
								abort.abort(
									new TimeoutError(
										"shared-log close replication reset timed out",
									),
								);
							} catch {
								abort.abort();
							}
						}, 2_000);
						try {
							await this.rpc
								.send(new AllReplicatingSegmentsMessage({ segments: [] }), {
									priority: 1,
									signal: abort.signal,
								})
								.catch(() => {});
						} finally {
							clearTimeout(abortTimer);
						}
					}
				} catch {
					// ignore: close should be resilient even if we were never fully started
				}
		const superClosed = await super.close(from);
		if (!superClosed) {
			return superClosed;
		}
		await this._close();
		await this.log.close();
		return true;
	}

		async drop(from?: Program): Promise<boolean> {
			// Best-effort: announce that we are going offline before tearing down
			// RPC/subscription state (same reasoning as in `close()`).
			try {
				if (!this.closed) {
					this._isReplicating = false;
					this._isAdaptiveReplicating = false;
					this.rebalanceParticipationDebounced?.close();
					this.replicationChangeDebounceFn?.close?.();
					this.pruneDebouncedFn?.close?.();
					this.responseToPruneDebouncedFn?.close?.();

					const abort = new AbortController();
					const abortTimer = setTimeout(() => {
						try {
							abort.abort(
								new TimeoutError(
									"shared-log drop replication reset timed out",
								),
							);
						} catch {
							abort.abort();
						}
					}, 2_000);
					try {
						await this.rpc
							.send(new AllReplicatingSegmentsMessage({ segments: [] }), {
								priority: 1,
								signal: abort.signal,
							})
							.catch(() => {});
					} finally {
						clearTimeout(abortTimer);
					}
				}
			} catch {
				// ignore: drop should be resilient even if we were never fully started
			}

			const superDropped = await super.drop(from);
			if (!superDropped) {
				return superDropped;
			}
			await this._entryCoordinatesIndex.drop();
		await this._replicationRangeIndex.drop();
		await this.log.drop();
		await this._close();
		return true;
	}

	async recover(): Promise<void> {
		return this.log.recover();
	}

	// Callback for receiving a message from the network
	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<void> {
		try {
			if (!context.from) {
				throw new Error("Missing from in update role message");
			}
			if (!context.from.equals(this.node.identity.publicKey)) {
				this.markReplicatorActivity(context.from.hashcode());
			}

			if (msg instanceof ResponseRoleMessage) {
				msg = msg.toReplicationInfoMessage(); // migration
			}

			if (msg instanceof ExchangeHeadsMessage) {
				/**
				 * I have received heads from someone else.
				 * I can use them to load associated logs and join/sync them with the data stores I own
				 */

				const { heads } = msg;
				const isRepairHint =
					(msg.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) !== 0;

				logger.trace(
					`${this.node.identity.publicKey.hashcode()}: Recieved heads: ${
						heads.length === 1 ? heads[0].entry.hash : "#" + heads.length
					}, logId: ${this.log.idString}`,
				);

				if (heads) {
					const filteredHeads: EntryWithRefs<any>[] = [];
					const confirmedHashes = new Set<string>();
					const existingHashes = await this.log.hasMany(
						heads.map((head) => head.entry.hash),
					);
					for (const head of heads) {
						if (!existingHashes.has(head.entry.hash)) {
							head.entry.init({
								// we need to init because we perhaps need to decrypt gid
								keychain: this.log.keychain,
								encoding: this.log.encoding,
							});
							filteredHeads.push(head);
						} else {
							confirmedHashes.add(head.entry.hash);
						}
					}
					const fromIsSelf = context.from.equals(this.node.identity.publicKey);
					if (!fromIsSelf) {
						this.markEntriesKnownByPeer(
							heads.map((head) => head.entry.hash),
							context.from.hashcode(),
						);
					}

					if (filteredHeads.length === 0) {
						if (confirmedHashes.size > 0 && !fromIsSelf) {
							await this.sendRepairConfirmation(context.from!, confirmedHashes);
						}
						return;
					}
					const groupedByGid = await groupByGid(filteredHeads);
					const maxReplicasFromHeadsByGid =
						await this.getMaxReplicasFromHeadsBatch(groupedByGid.keys());
					const promises: Promise<void>[] = [];

					for (const [gid, entries] of groupedByGid) {
						const fn = async () => {
							/// we clear sync in flight here because we want to join before that, so that entries are totally accounted for
							await this.syncronizer.onReceivedEntries({
								entries,
								from: context.from!,
							});

							const latestEntry = getLatestEntry(entries)!;

							const maxReplicasFromHead =
								maxReplicasFromHeadsByGid.get(gid) ??
								this.replicas.min.getValue(this);

							const maxReplicasFromNewEntries = maxReplicas(
								this,
								entries.map((x) => x.entry),
							);

							const maxMaxReplicas = Math.max(
								maxReplicasFromHead,
								maxReplicasFromNewEntries,
							);

							const isReplicating = this._isReplicating;

							let isLeader = false;
							let fromIsLeader = false;
							let leaders: LeaderMap | false;
							if (isReplicating) {
								leaders = await this._waitForEntryReplicators(
									latestEntry,
									maxMaxReplicas,
									[
										{
											key: this.node.identity.publicKey.hashcode(),
											replicator: true,
										},
									],
									{
										// we do this here so that we quickly assume leader role (and also so that 'from' is also assumed to be leader)
										// TODO potential side effects?
										roleAge: 0,
										timeout: 2e4,
										onLeader: (key) => {
											isLeader =
												isLeader ||
												this.node.identity.publicKey.hashcode() === key;
											fromIsLeader =
												fromIsLeader || context.from!.hashcode() === key;
										},
									},
								);
							} else {
								const plan = await this.planEntryLeaders(latestEntry, maxMaxReplicas, {
									onLeader: (key) => {
										fromIsLeader =
											fromIsLeader || context.from!.hashcode() === key;
										isLeader =
											isLeader ||
											this.node.identity.publicKey.hashcode() === key;
									},
								});
								leaders = plan.leaders;
							}

							if (this.closed) {
								return;
							}

							let maybeDelete: EntryWithRefs<any>[][] | undefined;
							let toMerge: Entry<any>[] = [];
							let toPersist: Entry<any>[] = [];
							let toDelete: Entry<any>[] | undefined;
							// Targeted repair is sent only to peers the sender currently believes
							// should store the entry. Accept it while local membership catches up;
							// the normal checked-prune path below can still remove it if this peer
							// truly no longer owns the entry.
							const acceptsTargetedRepair = isRepairHint && fromIsLeader;
							const keepAsLeader = isLeader || acceptsTargetedRepair;
							const gidReferenceHeads = keepAsLeader
								? undefined
								: await this.hasAnyHeadForGidSets(
										entries.map((entry) => entry.gidRefrences),
									);
							if (keepAsLeader) {
								for (const entry of entries) {
									this.pruneDebouncedFn.delete(entry.entry.hash);
									this.removePruneRequestSent(entry.entry.hash);
									this._requestIPruneResponseReplicatorSet.delete(
										entry.entry.hash,
									);

									if (fromIsLeader) {
										this.addPeersToGidPeerHistory(gid, [
											context.from!.hashcode(),
										]);
									}
								}

								if (maxReplicasFromNewEntries < maxReplicasFromHead) {
									(maybeDelete || (maybeDelete = [])).push(entries);
								}
							}

							outer: for (let i = 0; i < entries.length; i++) {
								const entry = entries[i]!;
								if (keepAsLeader || (await this.keep?.(entry.entry))) {
									toMerge.push(entry.entry);
									toPersist.push(entry.entry);
								} else {
									if (gidReferenceHeads?.[i]) {
										toMerge.push(entry.entry);
										(toDelete || (toDelete = [])).push(entry.entry);
										continue outer;
									}
								}

								logger.trace(
									`${this.node.identity.publicKey.hashcode()}: Dropping heads with gid: ${
										entry.entry.meta.gid
									}. Because not leader`,
								);
							}

							if (this.closed) {
								return;
							}

							if (toMerge.length > 0) {
								this.markEntriesKnownByPeer(
									toMerge.map((entry) => entry.hash),
									context.from!.hashcode(),
								);
								await this.log.join(toMerge);
								// Network joins bypass SharedLog.join(), but churn repair scans
								// the coordinate index to redistribute entries after membership changes.
								for (const entry of toPersist) {
									const replicas = decodeReplicas(entry).getValue(this);
									await this.planEntryLeaders(entry, replicas, {
										roleAge: 0,
										persist: {},
									});
								}
								for (const merged of toMerge) {
									confirmedHashes.add(merged.hash);
								}
								await this.pruneJoinedEntriesNoLongerLed(toMerge);

								toDelete?.map((x) =>
									// TODO types
									this.pruneDebouncedFnAddIfNotKeeping({
										key: x.hash,
										value: { entry: x, leaders: leaders as Map<string, any> },
									}),
								);
								this.rebalanceParticipationDebounced?.call();
							}

							if (maybeDelete) {
								for (const entries of maybeDelete as EntryWithRefs<any>[][]) {
									const minReplicas = await this.getMaxReplicasFromHeads(
										entries[0].entry.meta.gid,
									);
									if (minReplicas != null) {
										const isLeader = await this.isLeader({
											entry: entries[0].entry,
											replicas: minReplicas,
										});

										if (!isLeader) {
											for (const x of entries) {
												this.pruneDebouncedFnAddIfNotKeeping({
													key: x.entry.hash,
													// TODO types
													value: {
														entry: x.entry,
														leaders: leaders as Map<string, any>,
													},
												});
											}
										}
									}
								}
							}
						};
						promises.push(fn()); // we do this concurrently since waitForIsLeader might be a blocking operation for some entries
					}
					await Promise.all(promises);
					if (confirmedHashes.size > 0 && !context.from.equals(this.node.identity.publicKey)) {
						this.markEntriesKnownByPeer(confirmedHashes, context.from.hashcode());
						await this.sendRepairConfirmation(context.from!, confirmedHashes);
					}
				}
			} else if (msg instanceof RequestIPrune) {
				const hasAndIsLeader: string[] = [];
				const from = context.from.hashcode();
				const nativeEntryMetadata = this._nativeRangePlanner
					? this.log.entryIndex.getNativeEntryMetadataBatch(msg.hashes)
					: undefined;
				const presentBlocks = await this.log.blocks.hasMany?.(msg.hashes);

				for (let i = 0; i < msg.hashes.length; i++) {
					const hash = msg.hashes[i]!;
					this.removePruneRequestSent(hash, from);
					this.removeEntriesKnownByPeer([hash], from);

					// if we expect the remote to be owner of this entry because we are to prune ourselves, then we need to remove the remote
					// this is due to that the remote has previously indicated to be a replicator to help us prune but now has changed their mind
					const outGoingPrunes =
						this._requestIPruneResponseReplicatorSet.get(hash);
					if (outGoingPrunes) {
						outGoingPrunes.delete(from);
					}

					const nativeEntry = nativeEntryMetadata?.[i];
					const indexedEntry = nativeEntry
						? undefined
						: await this.log.entryIndex.getShallow(hash);
					let isLeader = false;

					if (
						(nativeEntry || indexedEntry) &&
						!this._pendingDeletes.has(hash) &&
						(presentBlocks
							? presentBlocks[i] === true
							: await this.log.blocks.has(hash))
					) {
						const gid = nativeEntry?.gid ?? indexedEntry!.value.meta.gid;
						const replicas = decodeReplicas({
							meta: {
								data: nativeEntry?.data ?? indexedEntry!.value.meta.data,
							},
						}).getValue(this);

						this.removePeerFromGidPeerHistory(
							context.from!.hashcode(),
							gid,
						);

						const waitFor: WaitForReplicator[] = [
							{
								key: this.node.identity.publicKey.hashcode(),
								replicator: true,
							},
						];
						const waitOptions: WaitForReplicatorsOptions<R> = {
							onLeader: (key) => {
								isLeader =
									isLeader || key === this.node.identity.publicKey.hashcode();
							},
						};

						if (nativeEntry) {
							await this._waitForGidReplicators(
								gid,
								replicas,
								waitFor,
								waitOptions,
							);
						} else {
							await this._waitForEntryReplicators(
								indexedEntry!.value,
								replicas,
								waitFor,
								waitOptions,
							);
						}
					}

					if (isLeader) {
						hasAndIsLeader.push(hash);
						hasAndIsLeader.length > 0 &&
							this.responseToPruneDebouncedFn.add({
								hashes: hasAndIsLeader,
								peers: [context.from!.hashcode()],
							});
					} else {
						const prevPendingIHave = this._pendingIHave.get(hash);
						if (prevPendingIHave) {
							prevPendingIHave.requesting.add(context.from.hashcode());
							prevPendingIHave.resetTimeout();
						} else {
							const requesting = new Set([context.from.hashcode()]);

							let timeout = setTimeout(() => {
								this._pendingIHave.delete(hash);
							}, this._respondToIHaveTimeout);

							const pendingIHave = {
								requesting,
								resetTimeout: () => {
									clearTimeout(timeout);
									timeout = setTimeout(() => {
										this._pendingIHave.delete(hash);
									}, this._respondToIHaveTimeout);
								},
								clear: () => {
									clearTimeout(timeout);
								},
								callback: async (entry: Entry<T>) => {
									this.removePeerFromGidPeerHistory(
										context.from!.hashcode(),
										entry.meta.gid,
									);
									this.removePruneRequestSent(entry.hash, from);
									let isLeader = false;
									await this._waitForEntryReplicators(
										entry,
										decodeReplicas(entry).getValue(this),
										[
											{
												key: this.node.identity.publicKey.hashcode(),
												replicator: true,
											},
										],
										{
											onLeader: (key) => {
												isLeader =
													isLeader ||
													key === this.node.identity.publicKey.hashcode();
											},
										},
									);
									if (isLeader) {
										this.responseToPruneDebouncedFn.add({
											hashes: [entry.hash],
											peers: requesting,
										});
										this._pendingIHave.delete(hash);
									}
								},
							};

							this._pendingIHave.set(hash, pendingIHave);
						}
					}
				}
			} else if (msg instanceof ResponseIPrune) {
				for (const hash of msg.hashes) {
					this._pendingDeletes.get(hash)?.resolve(context.from.hashcode());
				}
			} else if (msg instanceof ConfirmEntriesMessage) {
				this.markEntriesKnownByPeer(msg.hashes, context.from.hashcode());
				this.clearRepairFrontierHashes(context.from.hashcode(), msg.hashes);
				return;
			} else if (await this.syncronizer.onMessage(msg, context)) {
				return; // the syncronizer has handled the message
			} else if (msg instanceof BlocksMessage) {
				await this.remoteBlocks.onMessage(
					msg.message,
					{
						from: context.from!.hashcode(),
						transport: createRequestTransportContext(context.message),
					},
				);
			} else if (msg instanceof ReplicationPingMessage) {
				// No-op: used as an ACKed unicast liveness probe.
			} else if (msg instanceof RequestReplicationInfoMessage) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				const segments = (await this.getMyReplicationSegments()).map((x) =>
					x.toReplicationRange(),
				);

				this.rpc
					.send(new AllReplicatingSegmentsMessage({ segments }), {
						mode: new AcknowledgeDelivery({ to: [context.from], redundancy: 1 }),
					})
					.catch((e) => logger.error(e.toString()));

				// for backwards compatibility (v8) remove this when we are sure that all nodes are v9+
				if (this.v8Behaviour) {
					const role = this.getRole();
					if (role instanceof Replicator) {
						const fixedSettings = !this._isAdaptiveReplicating;
						if (fixedSettings) {
							await this.rpc.send(
								new ResponseRoleMessage({
									role,
								}),
								{
									mode: new SilentDelivery({
										to: [context.from],
										redundancy: 1,
									}),
								},
							);
						}
					}
				}
			} else if (
				msg instanceof AllReplicatingSegmentsMessage ||
				msg instanceof AddedReplicationSegmentMessage
			) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				const replicationInfoMessage = msg as
					| AllReplicatingSegmentsMessage
					| AddedReplicationSegmentMessage;

				// Process replication updates even if the sender isn't yet considered "ready" by
				// `Program.waitFor()`. Dropping these messages can lead to missing replicator info
				// (and downstream `waitForReplicator()` timeouts) under timing-sensitive joins.
				const from = context.from!;
				const fromHash = from.hashcode();
				if (this._replicationInfoBlockedPeers.has(fromHash)) {
					return;
				}
				const messageTimestamp = context.message.header.timestamp;
				await this.withReplicationInfoApplyQueue(fromHash, async () => {
					try {
						// The peer may have unsubscribed after this message was queued.
						if (this._replicationInfoBlockedPeers.has(fromHash)) {
							return;
						}

						// Process in-order to avoid races where repeated reset messages arrive
						// concurrently and trigger spurious "added" diffs / rebalancing.
						const prev = this.latestReplicationInfoMessage.get(fromHash);
						if (prev && prev > messageTimestamp) {
							return;
						}

						this.latestReplicationInfoMessage.set(fromHash, messageTimestamp);
						this._replicatorLivenessFailures.delete(fromHash);

						if (this.closed) {
							return;
						}

						const reset = msg instanceof AllReplicatingSegmentsMessage;
						await this.addReplicationRange(
							replicationInfoMessage.segments.map((x) =>
								x.toReplicationRangeIndexable(from),
							),
							from,
							{
								reset,
								checkDuplicates: true,
								timestamp: Number(messageTimestamp),
							},
						);

						// If the peer reports any replication segments, stop re-requesting.
						// (Empty reports can be transient during startup.)
						if (replicationInfoMessage.segments.length > 0) {
							this.cancelReplicationInfoRequests(fromHash);
						}
					} catch (e) {
						if (isNotStartedError(e as Error)) {
							return;
						}
						logger.error(
							`Failed to apply replication settings from '${fromHash}': ${
								(e as any)?.message ?? e
							}`,
						);
					}
				});
			} else if (msg instanceof StoppedReplicating) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}
				const fromHash = context.from.hashcode();
				if (this._replicationInfoBlockedPeers.has(fromHash)) {
					return;
				}
				this._replicatorLivenessFailures.delete(fromHash);

				const rangesToRemove = await this.resolveReplicationRangesFromIdsAndKey(
					msg.segmentIds,
					context.from,
				);

				await this.removeReplicationRanges(rangesToRemove, context.from);
				const timestamp = BigInt(+new Date());
				for (const range of rangesToRemove) {
						this.replicationChangeDebounceFn.add({
							range,
							type: "removed",
							timestamp,
						});
					}
			} else {
				throw new Error("Unexpected message");
			}
		} catch (e: any) {
			if (
				e instanceof AbortError ||
				e instanceof NotStartedError ||
				e instanceof IndexNotStartedError
			) {
				return;
			}

			if (e instanceof BorshError) {
				logger.trace(
					`${this.node.identity.publicKey.hashcode()}: Failed to handle message on topic: ${JSON.stringify(
						this.log.idString,
					)}: Got message for a different namespace`,
				);
				return;
			}

			if (e instanceof AccessError) {
				logger.trace(
					`${this.node.identity.publicKey.hashcode()}: Failed to handle message for log: ${JSON.stringify(
						this.log.idString,
					)}: Do not have permissions`,
				);
				return;
			}
			logger.error(e);
		}
	}

	async calculateTotalParticipation(options?: { sum?: boolean }) {
		if (options?.sum) {
			const ranges = await this.replicationIndex.iterate().all();
			let sum = 0;
			for (const range of ranges) {
				sum += range.value.widthNormalized;
			}
			return sum;
		}
		return appromixateCoverage({
			peers: this._replicationRangeIndex,
			numbers: this.indexableDomain.numbers,
			samples: 25,
		});
	}

	async calculateCoverage(properties?: {
		/** Optional: start of the content range (inclusive) */
		start?: NumberFromType<R>;
		/** Optional: end of the content range (exclusive) */
		end?: NumberFromType<R>;

		/** Optional: roleAge (in ms) */
		roleAge?: number;
	}) {
		return calculateCoverage({
			numbers: this.indexableDomain.numbers,
			peers: this.replicationIndex,
			end: properties?.end,
			start: properties?.start,
			roleAge: properties?.roleAge,
		});
	}

	async countReplicationSegments() {
		const count = await this.replicationIndex.count({
			query: new StringMatch({
				key: "hash",
				value: this.node.identity.publicKey.hashcode(),
			}),
		});
		return count;
	}

	async getAllReplicationSegments() {
		const ranges = await this.replicationIndex.iterate().all();
		return ranges.map((x) => x.value);
	}

	async getMyReplicationSegments() {
		const ranges = await this.replicationIndex
			.iterate({
				query: new StringMatch({
					key: "hash",
					value: this.node.identity.publicKey.hashcode(),
				}),
			})
			.all();
		return ranges.map((x) => x.value);
	}

	async calculateMyTotalParticipation() {
		// sum all of my replicator rects
		return (await this.getMyReplicationSegments()).reduce(
			(acc, { widthNormalized }) => acc + widthNormalized,
			0,
		);
	}

	async countAssignedHeads(options?: { strict: boolean }): Promise<number> {
		const myRanges = await this.getMyReplicationSegments();
		if (this._nativeSharedLogState) {
			const includeAssignedToRangeBoundary =
				options?.strict !== true &&
				(myRanges.length === 0 ||
					myRanges.some((range) => range.mode === ReplicationIntent.NonStrict));
			return this._nativeSharedLogState.countEntryCoordinatesInRanges(myRanges, {
				includeAssignedToRangeBoundary,
			});
		}
		const query = createAssignedRangesQuery(
			myRanges.map((x) => {
				return { range: x };
			}),
			{ strict: options?.strict },
		);
		const count = await this.entryCoordinatesIndex.count({
			query,
		});
		return count;
	}

	async countHeads(_properties: { approximate: true }): Promise<number> {
		let isReplicating = await this.isReplicating();
		if (!isReplicating) {
			throw new Error("Not implemented for non-replicators");
		}
		const myTotalParticipation = await this.calculateMyTotalParticipation();
		let minReplicasValue = this.replicas.min.getValue(this);
		const ownedHeadCount = await this.countAssignedHeads({ strict: true });

		// this scale factor arise from that we distribute the content 'minReplicasValue' on the domain axis (i.e. we shard the content)
		// but if we replicate more than 1/replicasValue space we will encounter the same head multiple times
		const scaleFactor = Math.max(
			1,
			1 / (minReplicasValue * myTotalParticipation),
		);
		return Math.round(ownedHeadCount * scaleFactor);
	}

	get replicationIndex(): Index<ReplicationRangeIndexable<R>> {
		if (!this._replicationRangeIndex) {
			throw new ClosedError();
		}
		return this._replicationRangeIndex;
	}

	get entryCoordinatesIndex(): Index<EntryReplicated<R>> {
		if (!this._entryCoordinatesIndex) {
			throw new ClosedError();
		}
		return this._entryCoordinatesIndex;
	}

	/**
	 * TODO improve efficiency
	 */
	async getReplicators() {
		let set = new Set<string>();
		const results = await this.replicationIndex
			.iterate({}, { reference: true, shape: { hash: true } })
			.all();
		results.forEach((result) => {
			set.add(result.value.hash);
		});

		return set;
	}

	async join(
		entries: (string | Entry<T> | ShallowEntry)[],
		options?: {
			verifySignatures?: boolean;
			timeout?: number;
			replicate?:
				| boolean
				| {
						mergeSegments?: boolean;
						assumeSynced?: boolean;
				  };
		},
	): Promise<void> {
		let entriesToReplicate: Entry<T>[] = [];
		const localHashes =
			options?.replicate && this.log.length > 0
				? await this.log.entryIndex.hasMany(
						entries.map((element) =>
							typeof element === "string" ? element : element.hash,
						),
				  )
				: new Set<string>();
		if (options?.replicate && this.log.length > 0) {
			// TODO this block should perhaps be called from a callback on the this.log.join method on all the ignored element because already joined, like "onAlreadyJoined"

			// check which entrise we already have but not are replicating, and replicate them
			// we can not just do the 'join' call because it will ignore the already joined entries
			for (const element of entries) {
				if (typeof element === "string") {
					if (localHashes.has(element)) {
						const entry = await this.log.get(element);
						if (entry) {
							entriesToReplicate.push(entry);
						}
					}
				} else if (element instanceof Entry) {
					if (localHashes.has(element.hash)) {
						entriesToReplicate.push(element);
					}
				} else {
					if (localHashes.has(element.hash)) {
						const entry = await this.log.get(element.hash);
						if (entry) {
							entriesToReplicate.push(entry);
						}
					}
				}
			}
		}

		const onChangeForReplication = options?.replicate
			? async (change: Change<T>) => {
					if (change.added) {
						for (const entry of change.added) {
							if (entry.head) {
								entriesToReplicate.push(entry.entry);
							}
						}
					}
				}
			: undefined;

		let assumeSynced =
			options?.replicate &&
			typeof options.replicate !== "boolean" &&
			options.replicate.assumeSynced;
		const seedAssumeSyncedPeerHistory = async (entry: Entry<T>) => {
			if (!assumeSynced) {
				return;
			}

			const minReplicas = decodeReplicas(entry).getValue(this);
			const { leaders } = await this.planEntryLeaders(entry, minReplicas, {
				roleAge: 0,
				persist: false,
			});

			this.addPeersToGidPeerHistory(entry.meta.gid, leaders.keys());
		};
		const persistCoordinate = async (entry: Entry<T>) => {
			const minReplicas = decodeReplicas(entry).getValue(this);
			const { leaders } = await this.planEntryLeaders(entry, minReplicas, {
				persist: {},
			});

			if (assumeSynced) {
				// make sure we dont start to initate syncing process outwards for this entry
				this.addPeersToGidPeerHistory(entry.meta.gid, leaders.keys());
			}
		};
		let entriesToPersist: Entry<T>[] = [];
		let joinOptions = {
			...options,
			onChange: async (change: Change<T>) => {
				await onChangeForReplication?.(change);
				for (const entry of change.added) {
					if (!entry.head) {
						continue;
					}

					if (!options?.replicate) {
						// we persist coordinates for all added entries here

						await persistCoordinate(entry.entry);
					} else {
						// else we persist after replication range update has been done so that
						// the indexed info becomes up to date
						entriesToPersist.push(entry.entry);
					}
				}
			},
		};

		await this.log.join(entries, joinOptions);

		if (options?.replicate) {
			let messageToSend: AddedReplicationSegmentMessage | undefined = undefined;

			if (assumeSynced) {
				// `assumeSynced` is an explicit contract that this join should trust the
				// supplied history and avoid initiating outbound repair while the local
				// replication ranges settle.
				this._assumeSyncedRepairSuppressedUntil =
					Date.now() + ASSUME_SYNCED_REPAIR_SUPPRESSION_MS;
				for (const entry of entriesToReplicate) {
					await seedAssumeSyncedPeerHistory(entry);
				}
			}

			await this.replicate(entriesToReplicate, {
				rebalance: assumeSynced ? false : true,
				checkDuplicates: assumeSynced ? false : true,
				mergeSegments:
					typeof options.replicate !== "boolean" && options.replicate
						? options.replicate.mergeSegments
						: false,

				// we override the announce step here to make sure we announce all new replication info
				// in one large message instead
				announce: (msg) => {
					if (msg instanceof AllReplicatingSegmentsMessage) {
						throw new Error("Unexpected");
					}

					if (messageToSend) {
						// merge segments to make it into one messages
						for (const segment of msg.segments) {
							messageToSend.segments.push(segment);
						}
					} else {
						messageToSend = msg;
					}
				},
			});

			// it is importat that we call persistCoordinate after this.replicate(entries) as else there might be a prune job deleting the entry before replication duties has been assigned to self
			for (const entry of entriesToPersist) {
				await persistCoordinate(entry);
			}

			if (messageToSend) {
				await this.rpc.send(messageToSend, {
					priority: 1,
				});
			}
		}
	}

		async waitForReplicator(
			key: PublicSignKey,
			options?: {
				signal?: AbortSignal;
			eager?: boolean;
			roleAge?: number;
			timeout?: number;
		},
	) {
		const deferred = pDefer<void>();
		const timeoutMs = options?.timeout ?? this.waitForReplicatorTimeout;
		const resolvedRoleAge = options?.eager
			? undefined
			: (options?.roleAge ?? (await this.getDefaultMinRoleAge()));

			let settled = false;
			let timer: ReturnType<typeof setTimeout> | undefined;
			let requestTimer: ReturnType<typeof setTimeout> | undefined;

		const clear = () => {
			this.events.removeEventListener("replicator:mature", check);
			this.events.removeEventListener("replication:change", check);
			options?.signal?.removeEventListener("abort", onAbort);
			if (timer != null) {
				clearTimeout(timer);
				timer = undefined;
			}
			if (requestTimer != null) {
				clearTimeout(requestTimer);
				requestTimer = undefined;
			}
		};

			const resolve = async () => {
				if (settled) {
					return;
				}
				settled = true;
				clear();
				// `waitForReplicator()` is typically used as a precondition before join/replicate
				// flows. A replicator can become mature and enqueue a debounced rebalance
				// (`replicationChangeDebounceFn`) slightly later. Kick the flush, but do not
				// make membership waits depend on all rebalance work finishing; callers that
				// need settled distribution already wait for that explicitly.
				this.replicationChangeDebounceFn?.flush?.().catch((error: any) => {
					if (!isNotStartedError(error)) {
						logger.error(error?.toString?.() ?? String(error));
					}
				});
				deferred.resolve();
			};

		const reject = (error: Error) => {
			if (settled) {
				return;
			}
			settled = true;
			clear();
			deferred.reject(error);
		};

		const onAbort = () => reject(new AbortError());
		if (options?.signal) {
			options.signal.addEventListener("abort", onAbort);
		}

		timer = setTimeout(() => {
			reject(
				new TimeoutError(`Timeout waiting for replicator ${key.hashcode()}`),
			);
		}, timeoutMs);

		let requestAttempts = 0;
		const requestIntervalMs = this.waitForReplicatorRequestIntervalMs;
		const maxRequestAttempts =
			this.waitForReplicatorRequestMaxAttempts ??
			Math.max(
				WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS,
				Math.ceil(timeoutMs / requestIntervalMs),
			);

		const requestReplicationInfo = () => {
			if (settled || this.closed) {
				return;
			}

			if (requestAttempts >= maxRequestAttempts) {
				return;
			}

			requestAttempts++;

			this.rpc
				.send(new RequestReplicationInfoMessage(), {
					mode: new AcknowledgeDelivery({ redundancy: 1, to: [key] }),
				})
				.catch((e) => {
					// Best-effort: missing peers / unopened RPC should not fail the wait logic.
					if (isNotStartedError(e as Error)) {
						return;
					}
					logger.error(e?.toString?.() ?? String(e));
				});

			if (requestAttempts < maxRequestAttempts) {
				requestTimer = setTimeout(requestReplicationInfo, requestIntervalMs);
			}
		};

			const check = async () => {
				const iterator = this.replicationIndex?.iterate(
					{ query: new StringMatch({ key: "hash", value: key.hashcode() }) },
					{ reference: true },
				);
				try {
					const rects = await iterator?.next(1);
					const rect = rects?.[0]?.value;
					if (!rect) {
						return;
					}
					if (!options?.eager && resolvedRoleAge != null) {
						if (!isMatured(rect, +new Date(), resolvedRoleAge)) {
							return;
						}
					}
					await resolve();
				} catch (error) {
					reject(error instanceof Error ? error : new Error(String(error)));
				} finally {
					await iterator?.close();
				}
			};

		requestReplicationInfo();
		check();
		this.events.addEventListener("replicator:mature", check);
		this.events.addEventListener("replication:change", check);

		return deferred.promise.finally(clear);
	}

	async waitForReplicators(options?: {
		timeout?: number;
		roleAge?: number;
		signal?: AbortSignal;
		coverageThreshold?: number;
		waitForNewPeers?: boolean;
	}) {
		let coverageThreshold = options?.coverageThreshold ?? 1;
		let deferred = pDefer<void>();
		let settled = false;

		const roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge());
		const providedCustomRoleAge = options?.roleAge != null;

		const resolve = () => {
			if (settled) return;
			settled = true;
			deferred.resolve();
		};

		const reject = (error: unknown) => {
			if (settled) return;
			settled = true;
			deferred.reject(error);
		};

		let checkInFlight: Promise<void> | undefined;
		const checkCoverage = async () => {
			const coverage = await this.calculateCoverage({
				roleAge,
			});

			if (coverage >= coverageThreshold) {
				resolve();
				return true;
			}
			return false;
		};

		const scheduleCheckCoverage = () => {
			if (settled || checkInFlight) {
				return;
			}

			checkInFlight = checkCoverage()
				.then(() => {})
				.catch(reject)
				.finally(() => {
					checkInFlight = undefined;
				});
		};
		const onReplicatorMature = () => {
			scheduleCheckCoverage();
		};
		const onReplicationChange = () => {
			scheduleCheckCoverage();
		};
		this.events.addEventListener("replicator:mature", onReplicatorMature);
		this.events.addEventListener("replication:change", onReplicationChange);
		await checkCoverage().catch(reject);

		let intervalMs = providedCustomRoleAge ? 100 : 250;
		let interval =
			roleAge > 0
				? setInterval(() => {
						scheduleCheckCoverage();
					}, intervalMs)
				: undefined;

		let timeout = options?.timeout ?? this.waitForReplicatorTimeout;
		const timer = setTimeout(() => {
			clear();
			reject(new TimeoutError(`Timeout waiting for mature replicators`));
		}, timeout);

		const abortListener = () => {
			clear();
			reject(new AbortError());
		};

		if (options?.signal) {
			options.signal.addEventListener("abort", abortListener);
		}
		const clear = () => {
			interval && clearInterval(interval);
			this.events.removeEventListener("replicator:mature", onReplicatorMature);
			this.events.removeEventListener(
				"replication:change",
				onReplicationChange,
			);
			clearTimeout(timer);
			if (options?.signal) {
				options.signal.removeEventListener("abort", abortListener);
			}
		};

		return deferred.promise.finally(() => {
			return clear();
		});
	}

	private async _waitForReplicators(
		cursors: NumberFromType<R>[],
		entry: Entry<T> | EntryReplicated<R> | ShallowEntry,
		waitFor: WaitForReplicator[],
		options: WaitForReplicatorsOptions<R> = {
			timeout: this.waitForReplicatorTimeout,
		},
	): Promise<LeaderMap | false> {
		return this.waitForLeaderSelection(waitFor, options, (checkOptions) =>
			this.findLeaders(cursors, entry, checkOptions),
		);
	}

	private async _waitForEntryReplicators(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
		replicas: number,
		waitFor: WaitForReplicator[],
		options: WaitForReplicatorsOptions<R> = {
			timeout: this.waitForReplicatorTimeout,
		},
	): Promise<LeaderMap | false> {
		if (this.canPlanNativeHashGid(entry) && this._nativeRangePlanner) {
			return this.waitForLeaderSelection(waitFor, options, async (checkOptions) => {
				const plan = await this.planEntryLeaders(entry, replicas, checkOptions);
				return plan.leaders;
			});
		}

		return this._waitForReplicators(
			await this.createCoordinates(entry, replicas),
			entry,
			waitFor,
			options,
		);
	}

	private async _waitForGidReplicators(
		gid: string,
		replicas: number,
		waitFor: WaitForReplicator[],
		options: WaitForReplicatorsOptions<R> = {
			timeout: this.waitForReplicatorTimeout,
		},
	): Promise<LeaderMap | false> {
		if (!this._nativeRangePlanner) {
			return false;
		}
		return this.waitForLeaderSelection(waitFor, options, async (checkOptions) => {
			const plan =
				(await this._findEntryAssignmentPlanFromHashGid(
					gid,
					replicas,
					checkOptions,
				)) ??
				(await this._findLeaderPlanFromHashGid(gid, replicas, checkOptions));
			if (!plan) {
				return new Map();
			}
			for (const key of plan.leaders.keys()) {
				checkOptions.onLeader?.(key);
			}
			return plan.leaders;
		});
	}

	private async waitForLeaderSelection(
		waitFor: WaitForReplicator[],
		options: WaitForReplicatorsOptions<R>,
		checkLeaders: (
			options: WaitForReplicatorsOptions<R>,
		) => Promise<LeaderMap>,
	): Promise<LeaderMap | false> {
		const timeout = options.timeout ?? this.waitForReplicatorTimeout;

		return new Promise((resolve, reject) => {
			let settled = false;
			const removeListeners = () => {
				this.events.removeEventListener("replication:change", roleListener);
				this.events.removeEventListener("replicator:mature", roleListener); // TODO replication:change event  ?
				this._closeController.signal.removeEventListener(
					"abort",
					abortListener,
				);
			};
			const settleResolve = (value: LeaderMap | false) => {
				if (settled) return;
				settled = true;
				removeListeners();
				clearTimeout(timer);
				resolve(value);
			};
			const settleReject = (error: unknown) => {
				if (settled) return;
				settled = true;
				removeListeners();
				clearTimeout(timer);
				reject(error);
			};
			const abortListener = () => {
				settleResolve(false);
			};

			const timer = setTimeout(async () => {
				settleResolve(false);
			}, timeout);

			const check = async () => {
				let leaderKeys = new Set<string>();
				const leaders = await checkLeaders({
					...options,
					onLeader: (key) => {
						options?.onLeader && options.onLeader(key);
						leaderKeys.add(key);
					},
				});

				for (const waitForKey of waitFor) {
					if (waitForKey.replicator && !leaderKeys!.has(waitForKey.key)) {
						return;
					}

					if (!waitForKey.replicator && leaderKeys!.has(waitForKey.key)) {
						return;
					}
				}
				options?.onLeader && leaderKeys.forEach(options.onLeader);

				settleResolve(leaders);
			};
			const runCheck = () => {
				void check().catch((error) => {
					settleReject(error);
				});
			};

			const roleListener = () => {
				runCheck();
			};

			this.events.addEventListener("replication:change", roleListener); // TODO replication:change event  ?
			this.events.addEventListener("replicator:mature", roleListener); // TODO replication:change event  ?
			this._closeController.signal.addEventListener("abort", abortListener);
			runCheck();
		});
	}

	async createCoordinates(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R> | NumberFromType<R>,
		minReplicas: number,
	) {
		if (
			typeof entry !== "number" &&
			typeof entry !== "bigint" &&
			this.canPlanNativeHashGid(entry)
		) {
			const nativeCoordinates = this._nativeRangePlanner?.getGidCoordinates(
				entry.meta.gid,
				minReplicas,
			) as NumberFromType<R>[] | undefined;
			if (nativeCoordinates) {
				return nativeCoordinates;
			}
		}

		const cursor =
			typeof entry === "number" || typeof entry === "bigint"
				? entry
				: await this.domain.fromEntry(entry);
		const nativeGrid = this._nativeRangePlanner?.getGrid(
			cursor,
			minReplicas,
		) as NumberFromType<R>[] | undefined;
		return nativeGrid ?? this.indexableDomain.numbers.getGrid(cursor, minReplicas);
	}

	private async getCoordinates(entry: { hash: string }) {
		const nativeCoordinates = this._nativeSharedLogState?.getEntryCoordinates(
			entry.hash,
		);
		if (nativeCoordinates) {
			return nativeCoordinates as NumberFromType<R>[];
		}
		const result = await this.entryCoordinatesIndex
			.iterate({ query: { hash: entry.hash } })
			.all();
		return result[0].value.coordinates;
	}

	private createCoordinatePersistenceEntry(properties: {
		coordinates: NumberFromType<R>[];
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		leaders:
			| Map<
					string,
					{
						intersecting: boolean;
					}
			  >
			| false;
		replicas: number;
		prev?: EntryReplicated<R>;
		assignedToRangeBoundary?: boolean;
	}): { coordinateEntry: EntryReplicated<R>; assignedToRangeBoundary: boolean } | false {
		const assignedToRangeBoundary =
			properties.assignedToRangeBoundary ??
			shouldAssignToRangeBoundary(properties.leaders, properties.replicas);

		if (
			properties.prev &&
			properties.prev.assignedToRangeBoundary === assignedToRangeBoundary
		) {
			return false;
		}

		const metaBytes = (properties.entry as EntryWithMetaBytes).getMetaBytes?.();
		const coordinateEntry = new this.indexableDomain.constructorEntry({
			assignedToRangeBoundary,
			coordinates: properties.coordinates,
			meta: properties.entry.meta,
			metaBytes,
			hash: properties.entry.hash,
			hashNumber: this.getEntryHashNumber(properties.entry),
		});
		return { coordinateEntry, assignedToRangeBoundary };
	}

	private async persistCoordinate(properties: {
		coordinates: NumberFromType<R>[];
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		leaders:
			| Map<
					string,
					{
						intersecting: boolean;
					}
			  >
			| false;
		replicas: number;
		prev?: EntryReplicated<R>;
		assignedToRangeBoundary?: boolean;
		commitNative?: boolean;
	}) {
		const prepared = this.createCoordinatePersistenceEntry(properties);
		if (!prepared) {
			return false;
		}
		const { coordinateEntry, assignedToRangeBoundary } = prepared;
		const nextHashes = properties.entry.meta.next;
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		let deleteNextOptions: DeleteOptions | undefined;
		if (coordinateIndex.putSharedLogCoordinateAndDeleteIds) {
			await coordinateIndex.putSharedLogCoordinateAndDeleteIds(
				coordinateEntry,
				{
					hash: coordinateEntry.hash,
					hashNumber: coordinateEntry.hashNumber,
					gid: coordinateEntry.gid,
					coordinates: coordinateEntry.coordinates,
					wallTime: coordinateEntry.wallTime,
					assignedToRangeBoundary: coordinateEntry.assignedToRangeBoundary,
					metaBytes: coordinateEntry.getMetaBytes(),
				},
				nextHashes,
			);
		} else if (nextHashes.length > 0 && coordinateIndex.putAndDeleteIds) {
			await coordinateIndex.putAndDeleteIds(coordinateEntry, nextHashes);
		} else {
			deleteNextOptions =
				nextHashes.length === 0
					? undefined
					: nextHashes.length === 1
						? { query: { hash: nextHashes[0] } }
						: {
								query: new Or(
									nextHashes.map(
										(x) => new StringMatch({ key: "hash", value: x }),
									),
								),
							};
			if (deleteNextOptions && coordinateIndex.putAndDelete) {
				await coordinateIndex.putAndDelete(coordinateEntry, deleteNextOptions);
			} else {
				await this.entryCoordinatesIndex.put(coordinateEntry);
			}
		}
		if (properties.commitNative !== false) {
			this._nativeSharedLogState?.commitEntryCoordinates(
				properties.entry.hash,
				coordinateEntry.gid,
				properties.coordinates,
				properties.entry.meta.next,
				assignedToRangeBoundary,
				properties.replicas,
				coordinateEntry.hashNumber,
			);
		}
		if (this._residentEntryCoordinatesByHash) {
			this._residentEntryCoordinatesByHash.set(
				properties.entry.hash,
				coordinateEntry,
			);
			for (const nextHash of nextHashes) {
				this._residentEntryCoordinatesByHash.delete(nextHash);
			}
		}

		for (const coordinate of properties.coordinates) {
			this.coordinateToHash.add(coordinate, properties.entry.hash);
		}

		if (deleteNextOptions && !coordinateIndex.putAndDelete) {
			await this.entryCoordinatesIndex.del(
				deleteNextOptions,
			);
		}
		return true;
	}

	private async persistCoordinatesBatch(
		items: Array<{
			coordinates: NumberFromType<R>[];
			entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
			leaders:
				| Map<
						string,
						{
							intersecting: boolean;
						}
				  >
				| false;
			replicas: number;
			prev?: EntryReplicated<R>;
			assignedToRangeBoundary?: boolean;
			commitNative?: boolean;
		}>,
	): Promise<boolean[]> {
		if (items.length === 0) {
			return [];
		}

		const prepared = items.map((item) => ({
			item,
			prepared: this.createCoordinatePersistenceEntry(item),
		}));
		const changed = prepared.filter(
			(
				entry,
			): entry is {
				item: (typeof items)[number];
				prepared: {
					coordinateEntry: EntryReplicated<R>;
					assignedToRangeBoundary: boolean;
				};
			} => entry.prepared !== false,
		);
		if (changed.length === 0) {
			return items.map(() => false);
		}

		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		const canUseGenericPutBatch =
			typeof coordinateIndex.putBatch === "function" &&
			changed.every(({ item }) => item.entry.meta.next.length === 0);

		if (coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch(
				changed.map(({ item, prepared }) => ({
					value: prepared.coordinateEntry,
					fields: {
						hash: prepared.coordinateEntry.hash,
						hashNumber: prepared.coordinateEntry.hashNumber,
						gid: prepared.coordinateEntry.gid,
						coordinates: prepared.coordinateEntry.coordinates,
						wallTime: prepared.coordinateEntry.wallTime,
						assignedToRangeBoundary:
							prepared.coordinateEntry.assignedToRangeBoundary,
						metaBytes: prepared.coordinateEntry.getMetaBytes(),
					},
					deleteIds: item.entry.meta.next,
				})),
			);
		} else if (canUseGenericPutBatch) {
			await coordinateIndex.putBatch!(
				changed.map(({ prepared }) => prepared.coordinateEntry),
			);
		} else {
			const results: boolean[] = [];
			for (const item of items) {
				results.push(await this.persistCoordinate(item));
			}
			return results;
		}

		for (const { item, prepared } of changed) {
			if (item.commitNative !== false) {
				this._nativeSharedLogState?.commitEntryCoordinates(
					item.entry.hash,
					prepared.coordinateEntry.gid,
					item.coordinates,
					item.entry.meta.next,
					prepared.assignedToRangeBoundary,
					item.replicas,
					prepared.coordinateEntry.hashNumber,
				);
			}
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(
					item.entry.hash,
					prepared.coordinateEntry,
				);
				for (const nextHash of item.entry.meta.next) {
					this._residentEntryCoordinatesByHash.delete(nextHash);
				}
			}
			for (const coordinate of item.coordinates) {
				this.coordinateToHash.add(coordinate, item.entry.hash);
			}
		}

		const changedHashes = new Set(
			changed.map(({ prepared }) => prepared.coordinateEntry.hash),
		);
		return items.map((item) => changedHashes.has(item.entry.hash));
	}

	private async deleteCoordinates(properties: { hash: string }) {
		this._nativeSharedLogState?.deleteEntryCoordinates(properties.hash);
		this._residentEntryCoordinatesByHash?.delete(properties.hash);
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIds) {
			await coordinateIndex.delIds([properties.hash]);
		} else {
			await this.entryCoordinatesIndex.del({ query: properties });
		}
	}

	async getDefaultMinRoleAge(): Promise<number> {
		if (this._isReplicating === false) {
			return 0;
		}

		// Explicitly disable maturity gating (used by many tests).
		if (this.timeUntilRoleMaturity <= 0) {
			return 0;
		}

		// If we're alone (or pubsub isn't ready), a fixed maturity time is sufficient.
		// When there are multiple replicators we want a stable threshold that doesn't
		// depend on "now" (otherwise it can drift and turn into a flake).
		let subscribers = 1;
		if (!this.rpc.closed) {
			try {
				subscribers = (await this._getTopicSubscribers(this.rpc.topic))?.length ?? 1;
			} catch {
				// Best-effort only; fall back to 1.
			}
		}

		if (subscribers <= 1) {
			return this.timeUntilRoleMaturity;
		}

		// Use replication range timestamps to compute a stable "age gap" between the
		// newest and oldest known roles. This keeps the oldest role mature while
		// preventing newer roles from being treated as mature purely because time
		// passes between test steps / network events.
		let newestOpenTime = this.openTime;
		try {
			const newestIterator = await this.replicationIndex.iterate(
				{
					sort: [new Sort({ key: "timestamp", direction: "desc" })],
				},
				{ shape: { timestamp: true }, reference: true },
			);
			const newestTimestampFromDB = (await newestIterator.next(1))[0]?.value
				.timestamp;
			await newestIterator.close();
			if (newestTimestampFromDB != null) {
				newestOpenTime = Number(newestTimestampFromDB);
			}
		} catch {
			// Best-effort only; fall back to local open time.
		}

		const ageGapToOldest = newestOpenTime - this.oldestOpenTime;
		const roleAge = Math.max(this.timeUntilRoleMaturity, ageGapToOldest);
		return roleAge < 0 ? 0 : roleAge;
	}

	async findLeaders(
		cursors: NumberFromType<R>[],
		entry: Entry<T> | EntryReplicated<R> | ShallowEntry,
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
			onLeader?: (key: string) => void;
			// persist even if not leader
			persist?:
				| {
						prev?: EntryReplicated<R>;
				  }
				| false;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
		// we consume a list of coordinates in this method since if we are leader of one coordinate we want to persist all of them
		const set = await this._findLeaders(cursors, options);
		await this.applyLeaderSelection(cursors, entry, set, options);
		return set;
	}

	private canPlanNativeEntryLeaderBatch(
		items: EntryLeaderBatchItem<R>[],
	): boolean {
		if (!this._nativeRangePlanner || items.length === 0) {
			return false;
		}

		const first = items[0]!;
		const firstRoleAge = first.options?.roleAge;
		for (const item of items) {
			if (
				!this.canPlanNativeHashGid(item.entry) ||
				item.options?.candidates ||
				item.options?.onLeader ||
				item.options?.roleAge !== firstRoleAge
			) {
				return false;
			}
		}
		return true;
	}

	private canPlanNativeHashGid(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
	): entry is ShallowOrFullEntry<any> | EntryReplicated<R> {
		return this.domain.type === "hash" && typeof entry.meta.gid === "string";
	}

	private getEntryHashNumber(
		entry: Entry<T> | ShallowOrFullEntry<any> | EntryReplicated<R>,
	): NumberFromType<R> {
		if ("hashNumber" in entry && entry.hashNumber != null) {
			return entry.hashNumber as NumberFromType<R>;
		}
		return this.indexableDomain.numbers.bytesToNumber(
			(entry as EntryWithMetaBytes).getHashDigestBytes?.() ??
				cidifyString(entry.hash).multihash.digest,
		);
	}

	private async applyLeaderSelection(
		cursors: NumberFromType<R>[],
		entry: Entry<T> | EntryReplicated<R> | ShallowEntry,
		leaders: LeaderMap,
		options?: {
			onLeader?: (key: string) => void;
			persist?:
				| {
						prev?: EntryReplicated<R>;
				  }
				| false;
		},
		assignedToRangeBoundary?: boolean,
	): Promise<boolean> {
		const selfHash = this.node.identity.publicKey.hashcode();
		const isLeader = leaders.has(selfHash);
		let shouldPersistLocalLeader = false;
		for (const key of leaders.keys()) {
			if (options?.onLeader) {
				options.onLeader(key);
				shouldPersistLocalLeader = shouldPersistLocalLeader || key === selfHash;
			}
		}

		if (
			options?.persist !== false &&
			(shouldPersistLocalLeader || options?.persist)
		) {
			!this.closed &&
				(await this.persistCoordinate({
					leaders,
					coordinates: cursors,
					replicas: cursors.length,
					entry,
					prev: options?.persist?.prev,
					assignedToRangeBoundary,
				}));
		}

		return isLeader;
	}

	private async planEntryLeaders(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
		replicas: number,
		options?: LeaderSelectionOptions<R>,
	): Promise<EntryLeaderPlan<R>> {
		let coordinates: NumberFromType<R>[];
		let leaders: LeaderMap;
		let assignedToRangeBoundary: boolean | undefined;

		if (this.canPlanNativeHashGid(entry)) {
			const plan =
				(await this._findEntryAssignmentPlanFromHashGid(
					entry.meta.gid,
					replicas,
					options,
				)) ??
				(await this._findLeaderPlanFromHashGid(
					entry.meta.gid,
					replicas,
					options,
				));
			if (plan) {
				coordinates = plan.coordinates as NumberFromType<R>[];
				leaders = plan.leaders;
				assignedToRangeBoundary =
					"assignedToRangeBoundary" in plan
						? (plan.assignedToRangeBoundary as boolean)
						: undefined;
				const isLeader = await this.applyLeaderSelection(
					coordinates,
					entry,
					leaders,
					options,
					assignedToRangeBoundary,
				);
				return {
					coordinates,
					leaders,
					isLeader,
					assignedToRangeBoundary,
				};
			}
		}

		coordinates = await this.createCoordinates(entry, replicas);
		leaders = await this._findLeaders(coordinates, options);
		const isLeader = await this.applyLeaderSelection(
			coordinates,
			entry,
			leaders,
			options,
		);
		return { coordinates, leaders, isLeader };
	}

	private async planEntryLeaderBatch(
		items: Iterable<EntryLeaderBatchItem<R>>,
	): Promise<EntryLeaderPlan<R>[]> {
		const itemArray = [...items];
		const firstItem = itemArray[0];
		if (!firstItem) {
			return [];
		}

		if (this.canPlanNativeEntryLeaderBatch(itemArray)) {
			const context = await this.createLeaderSelectionContext(firstItem.options);
			const nativePlans = this._nativeRangePlanner!.planLeadersForGidsBatch(
				itemArray.map((item) => ({
					gid: item.entry.meta.gid as string,
					replicas: item.replicas,
				})),
				this.createNativeLeaderOptions(context, firstItem.options),
			);
			const plans: EntryLeaderPlan<R>[] = [];
			for (let i = 0; i < itemArray.length; i++) {
				const item = itemArray[i]!;
				const nativePlan = nativePlans[i]!;
				const coordinates = nativePlan.coordinates as NumberFromType<R>[];
				const leaders = nativePlan.leaders;
				const isLeader = await this.applyLeaderSelection(
					coordinates,
					item.entry,
					leaders,
					item.options,
				);
				plans.push({ coordinates, leaders, isLeader });
			}
			return plans;
		}

		const plans: EntryLeaderPlan<R>[] = [];
		for (const item of itemArray) {
			plans.push(
				await this.planEntryLeaders(item.entry, item.replicas, item.options),
			);
		}
		return plans;
	}

	async isLeader(
		properties: {
			entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
			replicas: number;
		},
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
			onLeader?: (key: string) => void;
			// persist even if not leader
			persist?:
				| {
						prev?: EntryReplicated<R>;
				  }
				| false;
		},
	): Promise<boolean> {
		const plan = await this.planEntryLeaders(
			properties.entry,
			properties.replicas,
			options,
		);
		return plan.isLeader;
	}

	private async createLeaderSelectionContext(options?: {
		roleAge?: number;
		candidates?: Iterable<string>;
	}): Promise<{
		roleAge: number;
		selfHash: string;
		selfReplicating: boolean;
		peerFilter: Set<string> | undefined;
	}> {
		const roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge()); // TODO -500 as is added so that i f someone else is just as new as us, then we treat them as mature as us. without -500 we might be slower syncing if two nodes starts almost at the same time
		const selfHash = this.node.identity.publicKey.hashcode();

		// Prefer `uniqueReplicators` (replicator cache) as soon as it has any data.
		// If it is still warming up (for example, only contains self), supplement with
		// current subscribers until we have enough candidates for this decision.
		let peerFilter: Set<string> | undefined = undefined;
		let selfReplicating = false;
		if (options?.candidates) {
			peerFilter = new Set(options.candidates);
		} else {
			selfReplicating = await this.isReplicating();
			if (this.uniqueReplicators.size > 0) {
				peerFilter = new Set(this.uniqueReplicators);
				if (selfReplicating) {
					peerFilter.add(selfHash);
				} else {
					peerFilter.delete(selfHash);
				}

				try {
					const subscribers = await this._getTopicSubscribers(this.topic);
					if (subscribers && subscribers.length > 0) {
						for (const subscriber of subscribers) {
							peerFilter.add(subscriber.hashcode());
						}
						if (selfReplicating) {
							peerFilter.add(selfHash);
						} else {
							peerFilter.delete(selfHash);
						}
					}
				} catch {
					// Best-effort only; keep current peerFilter.
				}
			} else {
				try {
					const subscribers =
						(await this._getTopicSubscribers(this.topic)) ?? undefined;
					if (subscribers && subscribers.length > 0) {
						peerFilter = new Set(subscribers.map((key) => key.hashcode()));
						if (selfReplicating) {
							peerFilter.add(selfHash);
						} else {
							peerFilter.delete(selfHash);
						}
					}
				} catch {
					// Best-effort only; if pubsub isn't ready, do a full scan.
				}
			}
		}

		return {
			roleAge,
			selfHash,
			selfReplicating,
			peerFilter,
		};
	}

	private createNativeLeaderOptions(
		context: {
			roleAge: number;
			selfHash: string;
			selfReplicating: boolean;
			peerFilter: Set<string> | undefined;
		},
		options?: {
			candidates?: Iterable<string>;
		},
	) {
		return {
			roleAge: context.roleAge,
			now: Date.now(),
			peerFilter: context.peerFilter,
			expandPeerFilter: !options?.candidates,
			selfHash: context.selfHash,
			selfReplicating: context.selfReplicating,
			fullReplicaFallback: !options?.candidates,
			includeStrictFullReplica:
				this._logProperties?.strictFullReplicaFallback !== false,
		};
	}

	private async _findLeaders(
		cursors: NumberFromType<R>[],
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
		const context = await this.createLeaderSelectionContext(options);
		let peerFilter = context.peerFilter;

		if (this._nativeRangePlanner) {
			return this._nativeRangePlanner.findLeaders(cursors, cursors.length, {
				...this.createNativeLeaderOptions(context, options),
			});
		}

		if (!options?.candidates) {
			// Reachability snapshots can briefly under-report peers. Do not let that
			// turn a known mature indexed range into a false self-only full replica.
			peerFilter = await this.includeIndexedLeaderCandidatesWhenUnderfilled(
				peerFilter,
				context.roleAge,
				cursors.length,
				context.selfReplicating,
			);
		}

		if (!options?.candidates) {
			const fullReplicaLeaders = await this.findFullReplicaLeaders(
				cursors.length,
				context.roleAge,
				peerFilter,
			);
			if (fullReplicaLeaders) {
				return fullReplicaLeaders;
			}
		}

		return getSamples<R>(
			cursors,
			this.replicationIndex,
			context.roleAge,
			this.indexableDomain.numbers,
			{
				peerFilter,
				uniqueReplicators: peerFilter,
			},
		);
	}

	private async includeIndexedLeaderCandidatesWhenUnderfilled(
		peerFilter: Set<string> | undefined,
		roleAge: number,
		replicas: number,
		selfReplicating: boolean,
	): Promise<Set<string> | undefined> {
		if (!peerFilter || peerFilter.size > replicas) {
			return peerFilter;
		}

		const selfHash = this.node.identity.publicKey.hashcode();
		const now = Date.now();
		const iterator = this.replicationIndex.iterate(
			{},
			{ shape: { hash: true, timestamp: true }, reference: true },
		);

		try {
			for (;;) {
				const batch = await iterator.next(64);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					const range = result.value;
					if (range.hash === selfHash && !selfReplicating) {
						continue;
					}
					if (!isMatured(range, now, roleAge)) {
						continue;
					}
					peerFilter.add(range.hash);
				}
			}
		} finally {
			await iterator.close();
		}

		return peerFilter;
	}

	private async findFullReplicaLeaders(
		replicas: number,
		roleAge: number,
		peerFilter?: Set<string>,
	): Promise<Map<string, { intersecting: boolean }> | undefined> {
		const now = Date.now();
		const leaders = new Map<string, { intersecting: boolean }>();
		const includeStrict =
			this._logProperties?.strictFullReplicaFallback !== false;
		const iterator = this.replicationIndex.iterate(
			{},
			{ shape: { hash: true, timestamp: true, mode: true } },
		);

		try {
			for (;;) {
				const batch = await iterator.next(64);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					const range = result.value;
					if (peerFilter && !peerFilter.has(range.hash)) {
						continue;
					}
					if (!isMatured(range, now, roleAge)) {
						continue;
					}
					if (range.mode === ReplicationIntent.Strict && !includeStrict) {
						continue;
					}
					leaders.set(range.hash, { intersecting: true });
					if (leaders.size > replicas) {
						return undefined;
					}
				}
			}
		} finally {
			await iterator.close();
		}

		return leaders.size > 0 ? leaders : undefined;
	}

	private async findEntryReplicatedLeaderBatch(
		entries: EntryReplicated<R>[],
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
	): Promise<LeaderMap[]> {
		if (entries.length === 0) {
			return [];
		}

		if (this._nativeRangePlanner) {
			const context = await this.createLeaderSelectionContext(options);
			return this._nativeRangePlanner.findLeadersBatch(
				entries.map((entry) => ({
					cursors: entry.coordinates,
					replicas: entry.coordinates.length,
				})),
				this.createNativeLeaderOptions(context, options),
			);
		}

		const leaders: LeaderMap[] = [];
		for (const entry of entries) {
			leaders.push(await this._findLeaders(entry.coordinates, options));
		}
		return leaders;
	}

	private async planResidentRepairDispatchBatch(properties: {
		pendingModes: Set<RepairDispatchMode>;
		pendingPeersByMode: Map<RepairDispatchMode, Set<string>>;
		optimisticGidPeersByMode: Map<RepairDispatchMode, Map<string, Set<string>>>;
		fullReplicaRepairCandidates: Set<string>;
		fullReplicaRepairCandidateCount: number;
		selfHash: string;
	}): Promise<Map<RepairDispatchMode, Map<string, string[]>>> {
		const pendingPeersByMode = new Map<string, Iterable<string>>();
		const optimisticPeersByMode = new Map<
			string,
			Map<string, Iterable<string>>
		>();
		for (const mode of properties.pendingModes) {
			pendingPeersByMode.set(
				mode,
				properties.pendingPeersByMode.get(mode) ?? [],
			);
			const optimisticByGid = properties.optimisticGidPeersByMode.get(mode);
			if (optimisticByGid) {
				const optimisticEntries = new Map<string, Iterable<string>>();
				for (const [gid, peers] of optimisticByGid) {
					optimisticEntries.set(gid, peers);
				}
				optimisticPeersByMode.set(mode, optimisticEntries);
			}
		}

		const context = await this.createLeaderSelectionContext({ roleAge: 0 });
		const nativePlan =
			this._nativeSharedLogState!.planRepairDispatchForResidentEntries(
				{
					pendingModes: properties.pendingModes,
					pendingPeersByMode,
					optimisticPeersByMode,
					fullReplicaRepairCandidates: properties.fullReplicaRepairCandidates,
					fullReplicaRepairCandidateCount:
						properties.fullReplicaRepairCandidateCount,
					selfHash: properties.selfHash,
				},
				this.createNativeLeaderOptions(context),
			);

		const plan = new Map<RepairDispatchMode, Map<string, string[]>>();
		for (const [mode, targets] of nativePlan) {
			plan.set(mode as RepairDispatchMode, targets);
		}
		return plan;
	}

	private async planRepairDispatchBatch(properties: {
		entries: EntryReplicated<R>[];
		requestedReplicasBatch: number[];
		pendingModes: Set<RepairDispatchMode>;
		pendingPeersByMode: Map<RepairDispatchMode, Set<string>>;
		optimisticGidPeersByMode: Map<RepairDispatchMode, Map<string, Set<string>>>;
		fullReplicaRepairCandidates: Set<string>;
		fullReplicaRepairCandidateCount: number;
		selfHash: string;
	}): Promise<Map<RepairDispatchMode, Map<string, string[]>>> {
		const add = (
			plan: Map<RepairDispatchMode, Map<string, string[]>>,
			mode: RepairDispatchMode,
			target: string,
			hash: string,
		) => {
			let targets = plan.get(mode);
			if (!targets) {
				targets = new Map();
				plan.set(mode, targets);
			}
			let hashes = targets.get(target);
			if (!hashes) {
				hashes = [];
				targets.set(target, hashes);
			}
			if (!hashes.includes(hash)) {
				hashes.push(hash);
			}
		};

		if (this._nativeSharedLogState) {
			const pendingPeersByMode = new Map<string, Iterable<string>>();
			const optimisticPeersByMode = new Map<
				string,
				Map<string, Iterable<string>>
			>();
			for (const mode of properties.pendingModes) {
				pendingPeersByMode.set(
					mode,
					properties.pendingPeersByMode.get(mode) ?? [],
				);
				const optimisticByGid = properties.optimisticGidPeersByMode.get(mode);
				if (optimisticByGid) {
					const optimisticEntries = new Map<string, Iterable<string>>();
					for (const [gid, peers] of optimisticByGid) {
						optimisticEntries.set(gid, peers);
					}
					optimisticPeersByMode.set(mode, optimisticEntries);
				}
			}

			const context = await this.createLeaderSelectionContext({ roleAge: 0 });
			const nativePlan = this._nativeSharedLogState.planRepairDispatchForEntries(
				{
					entries: properties.entries.map((entry, i) => ({
						hash: entry.hash,
						gid: entry.gid,
						requestedReplicas: properties.requestedReplicasBatch[i]!,
						coordinates: entry.coordinates,
					})),
					pendingModes: properties.pendingModes,
					pendingPeersByMode,
					optimisticPeersByMode,
					fullReplicaRepairCandidates: properties.fullReplicaRepairCandidates,
					fullReplicaRepairCandidateCount:
						properties.fullReplicaRepairCandidateCount,
					selfHash: properties.selfHash,
				},
				this.createNativeLeaderOptions(context),
			);

			const plan = new Map<RepairDispatchMode, Map<string, string[]>>();
			for (const [mode, targets] of nativePlan) {
				plan.set(mode as RepairDispatchMode, targets);
			}
			return plan;
		}

		const currentPeersBatch = await this.findEntryReplicatedLeaderBatch(
			properties.entries,
			{ roleAge: 0 },
		);
		const plan = new Map<RepairDispatchMode, Map<string, string[]>>();
		for (let i = 0; i < properties.entries.length; i++) {
			const entry = properties.entries[i]!;
			const currentPeers = currentPeersBatch[i]!;
			const requestedReplicas = properties.requestedReplicasBatch[i]!;
			const knownPeers = this._gidPeersHistory.get(entry.gid);

			if (properties.pendingModes.has("churn")) {
				for (const [currentPeer] of currentPeers) {
					if (currentPeer !== properties.selfHash) {
						add(plan, "churn", currentPeer, entry.hash);
					}
				}
			}

			for (const mode of properties.pendingModes) {
				const modePeers = properties.pendingPeersByMode.get(mode);
				if (!modePeers || modePeers.size === 0) {
					continue;
				}
				const optimisticPeers = properties.optimisticGidPeersByMode
					.get(mode)
					?.get(entry.gid);
				for (const peer of modePeers) {
					if (this.isEntryKnownByPeer(entry.hash, peer)) {
						continue;
					}
					const wasOptimisticallyAssigned =
						optimisticPeers?.has(peer) === true;
					const isCoveredByFullReplicaRepair =
						mode === "join-authoritative" &&
						properties.fullReplicaRepairCandidates.has(peer) &&
						requestedReplicas >= properties.fullReplicaRepairCandidateCount;
					const shouldQueue =
						mode === "join-authoritative"
							? currentPeers.has(peer) || isCoveredByFullReplicaRepair
							: wasOptimisticallyAssigned ||
							  (currentPeers.has(peer) && !knownPeers?.has(peer));
					if (shouldQueue) {
						add(plan, mode, peer, entry.hash);
					}
				}
			}
		}
		return plan;
	}

	private async _findLeadersFromHashGid(
		gid: string,
		replicas: number,
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
	): Promise<Map<string, { intersecting: boolean }> | undefined> {
		if (!this._nativeSharedLogState && !this._nativeRangePlanner) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(options);
		if (this._nativeSharedLogState) {
			return this._nativeSharedLogState.planLeadersForGid(
				gid,
				replicas,
				this.createNativeLeaderOptions(context, options),
			).leaders;
		}

		if (!this._nativeRangePlanner) {
			return undefined;
		}

		return this._nativeRangePlanner.findLeadersForGid(
			gid,
			replicas,
			this.createNativeLeaderOptions(context, options),
		);
	}

	private async _findLeaderPlanFromHashGid(
		gid: string,
		replicas: number,
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
	): Promise<
		| {
				coordinates: Array<number | bigint>;
				leaders: Map<string, { intersecting: boolean }>;
		  }
		| undefined
	> {
		const planner = this._nativeSharedLogState ?? this._nativeRangePlanner;
		if (!planner) {
			return undefined;
		}
		const context = await this.createLeaderSelectionContext(options);
		return planner.planLeadersForGid(
			gid,
			replicas,
			this.createNativeLeaderOptions(context, options),
		);
	}

	private async _findEntryAssignmentPlanFromHashGid(
		gid: string,
		replicas: number,
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
	): Promise<
		| {
				coordinates: Array<number | bigint>;
				leaders: Map<string, { intersecting: boolean }>;
				assignedToRangeBoundary: boolean;
		  }
		| undefined
	> {
		if (!this._nativeSharedLogState) {
			return undefined;
		}
		const context = await this.createLeaderSelectionContext(options);
		return this._nativeSharedLogState.planEntryAssignmentForGid(
			gid,
			replicas,
			this.createNativeLeaderOptions(context, options),
		);
	}

	async findLeadersFromEntry(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
		replicas: number,
		options?: {
			roleAge?: number;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
		if (this.canPlanNativeHashGid(entry)) {
			const nativeResult = await this._findLeadersFromHashGid(
				entry.meta.gid,
				replicas,
				options,
			);
			if (nativeResult) {
				return nativeResult;
			}
		}

		const coordinates = await this.createCoordinates(entry, replicas);
		const result = await this._findLeaders(coordinates, options);
		return result;
	}

	async isReplicator(
		entry: Entry<any>,
		options?: {
			candidates?: string[];
			roleAge?: number;
		},
	) {
		return this.isLeader(
			{
				entry,
				replicas: maxReplicas(this, [entry]),
			},
			options,
		);
	}

	private withReplicationInfoApplyQueue(
		peerHash: string,
		fn: () => Promise<void>,
	): Promise<void> {
		const prev = this._replicationInfoApplyQueueByPeer.get(peerHash);
		const next = (prev ?? Promise.resolve())
			.catch(() => {
				// Avoid stuck queues if a previous apply failed.
			})
			.then(fn);
		this._replicationInfoApplyQueueByPeer.set(peerHash, next);
		return next.finally(() => {
			if (this._replicationInfoApplyQueueByPeer.get(peerHash) === next) {
				this._replicationInfoApplyQueueByPeer.delete(peerHash);
			}
		});
	}

	private cancelReplicationInfoRequests(peerHash: string) {
		const state = this._replicationInfoRequestByPeer.get(peerHash);
		if (!state) return;
		if (state.timer) {
			clearTimeout(state.timer);
		}
		this._replicationInfoRequestByPeer.delete(peerHash);
	}

	private scheduleReplicationInfoRequests(peer: PublicSignKey) {
		const peerHash = peer.hashcode();
		if (this._replicationInfoRequestByPeer.has(peerHash)) {
			return;
		}

		const state: { attempts: number; timer?: ReturnType<typeof setTimeout> } = {
			attempts: 0,
		};
		this._replicationInfoRequestByPeer.set(peerHash, state);

		const intervalMs = Math.max(50, this.waitForReplicatorRequestIntervalMs);
		const maxAttempts =
			this.waitForReplicatorRequestMaxAttempts ??
			Math.max(
				WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS,
				Math.ceil(this.waitForReplicatorTimeout / intervalMs),
			);

		const tick = () => {
			if (this.closed || this._closeController.signal.aborted) {
				this.cancelReplicationInfoRequests(peerHash);
				return;
			}

			state.attempts++;

			this.rpc
				.send(new RequestReplicationInfoMessage(), {
					mode: new AcknowledgeDelivery({ redundancy: 1, to: [peer] }),
				})
				.catch((e) => {
					// Best-effort: missing peers / unopened RPC should not fail join flows.
					if (isNotStartedError(e as Error)) {
						return;
					}
					logger.error(e?.toString?.() ?? String(e));
				});

			if (state.attempts >= maxAttempts) {
				this.cancelReplicationInfoRequests(peerHash);
				return;
			}

			state.timer = setTimeout(tick, intervalMs);
			state.timer.unref?.();
		};

		tick();
	}

	async handleSubscriptionChange(
		publicKey: PublicSignKey,
		topics: string[],
		subscribed: boolean,
	) {
		if (!topics.includes(this.topic)) {
			return;
		}

		const peerHash = publicKey.hashcode();
		if (!subscribed) {
			this._replicationInfoBlockedPeers.add(peerHash);

			const now = BigInt(+new Date());
			const previous = this.latestReplicationInfoMessage.get(peerHash);
			if (!previous || previous < now) {
				this.latestReplicationInfoMessage.set(peerHash, now);
			}

			const wasReplicator = this.uniqueReplicators.has(peerHash);
			try {
				// Unsubscribe can race with the peer's final replication reset message.
				// Proactively evict its ranges so leader selection doesn't keep stale owners.
				await this.removeReplicator(publicKey, { noEvent: true });
			} catch (error) {
				if (!isNotStartedError(error as Error)) {
					throw error;
				}
			}

			this._replicatorJoinEmitted.delete(peerHash);
			this.cleanupPeerDisconnectTracking(peerHash);

			if (wasReplicator) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
						detail: { publicKey },
					}),
				);
			}
			return;
		}

		this._replicationInfoBlockedPeers.delete(peerHash);
		this._replicatorLivenessFailures.delete(peerHash);
		this.markReplicatorActivity(peerHash);

		const replicationSegments = await this.getMyReplicationSegments();
		if (replicationSegments.length > 0) {
			this.rpc
				.send(
					new AllReplicatingSegmentsMessage({
						segments: replicationSegments.map((x) => x.toReplicationRange()),
					}),
					{
						mode: new AcknowledgeDelivery({ redundancy: 1, to: [publicKey] }),
					},
				)
				.catch((e) => logger.error(e.toString()));

			if (this.v8Behaviour) {
				// for backwards compatibility
				this.rpc
					.send(new ResponseRoleMessage({ role: await this.getRole() }), {
						mode: new AcknowledgeDelivery({ redundancy: 1, to: [publicKey] }),
					})
					.catch((e) => logger.error(e.toString()));
			}
		}

		// Request the remote peer's replication info. This makes joins resilient to
		// timing-sensitive delivery/order issues where we may miss their initial
		// replication announcement.
		this.scheduleReplicationInfoRequests(publicKey);
	}

	private getClampedReplicas(customValue?: MinReplicas) {
		if (!customValue) {
			return this.replicas.min;
		}
		const min = customValue.getValue(this);
		const maxValue = Math.max(this.replicas.min.getValue(this), min);

		if (this.replicas.max) {
			return new AbsoluteReplicas(
				Math.min(maxValue, this.replicas.max.getValue(this)),
			);
		}
		return new AbsoluteReplicas(maxValue);
	}

	private removePruneRequestSent(hash: string, to?: string) {
		if (!to) {
			this._requestIPruneSent.delete(hash);
		} else {
			let set = this._requestIPruneSent.get(hash);
			if (set) {
				set.delete(to);
				if (set.size === 0) {
					this._requestIPruneSent.delete(hash);
				}
			}
		}
	}

	prune(
		entries: Map<
			string,
			{
				entry: CheckedPruneEntry<T, R>;
				leaders: CheckedPruneLeaderMap | Set<string>;
			}
		>,
			options?: { timeout?: number; unchecked?: boolean },
			): Promise<any>[] {
		if (options?.unchecked) {
			return [...entries.values()].map((x) => {
				this.deleteGidPeerHistory(x.entry.meta.gid);
				this.removePruneRequestSent(x.entry.hash);
				this._requestIPruneResponseReplicatorSet.delete(x.entry.hash);
				return this.log.remove(x.entry, {
					recursively: true,
				});
			});
		}

		if (this.closed) {
			return [];
		}

		// ask network if they have they entry,
		// so I can delete it

		// There is a few reasons why we might end up here

		// - Two logs merge, and we should not anymore keep the joined log replicated (because we are not responsible for the resulting gid)
		// - An entry is joined, where min replicas is lower than before (for all heads for this particular gid) and therefore we are not replicating anymore for this particular gid
		// - Peers join and leave, which means we might not be a replicator anymore

			const promises: Promise<any>[] = [];

			let peerToEntries: Map<string, string[]> = new Map();
			let cleanupTimer: ReturnType<typeof setTimeout>[] = [];
			const explicitTimeout = options?.timeout != null;

			for (const { entry, leaders } of entries.values()) {
				for (const leader of leaders.keys()) {
					let set = peerToEntries.get(leader);
					if (!set) {
						set = [];
						peerToEntries.set(leader, set);
					}

					set.push(entry.hash);
				}

				const pendingPrev = this._pendingDeletes.get(entry.hash);
				if (pendingPrev) {
					// If a background prune is already in-flight, an explicit prune request should
					// still respect the caller's timeout. Otherwise, tests (and user calls) can
					// block on the longer "checked prune" timeout derived from
					// `_respondToIHaveTimeout + waitForReplicatorTimeout`, which is intentionally
					// large for resiliency.
					if (explicitTimeout) {
						const timeoutMs = Math.max(0, Math.floor(options?.timeout ?? 0));
						promises.push(
							new Promise((resolve, reject) => {
								// Mirror the checked-prune error prefix so existing callers/tests can
								// match on the message substring.
								const timer = setTimeout(() => {
									reject(
										new Error(
											`Timeout for checked pruning after ${timeoutMs}ms (pending=true closed=${this.closed})`,
										),
									);
								}, timeoutMs);
								timer.unref?.();
								pendingPrev.promise.promise
									.then(resolve, reject)
									.finally(() => clearTimeout(timer));
							}),
						);
					} else {
						promises.push(pendingPrev.promise.promise);
					}
					continue;
				}

				const minReplicas = decodeReplicas(entry);
				const deferredPromise: DeferredPromise<void> = pDefer();

			const clear = () => {
				const pending = this._pendingDeletes.get(entry.hash);
				if (pending?.promise === deferredPromise) {
					this._pendingDeletes.delete(entry.hash);
				}
				clearTimeout(timeout);
			};

					const resolve = () => {
						clear();
						this.clearCheckedPruneRetry(entry.hash);
						cleanupTimer.push(
							setTimeout(async () => {
								this.deleteGidPeerHistory(entry.meta.gid);
								this.removePruneRequestSent(entry.hash);
							this._requestIPruneResponseReplicatorSet.delete(entry.hash);

						if (
							await this.isLeader({
								entry,
								replicas: minReplicas.getValue(this),
							})
						) {
							deferredPromise.reject(
								new Error("Failed to delete, is leader again"),
							);
							return;
						}

						return this.log
							.remove(entry, {
								recursively: true,
							})
							.then(() => {
								deferredPromise.resolve();
							})
								.catch((e) => {
									deferredPromise.reject(e);
								})
								.finally(async () => {
									this.deleteGidPeerHistory(entry.meta.gid);
									this.removePruneRequestSent(entry.hash);
									this._requestIPruneResponseReplicatorSet.delete(entry.hash);
								// TODO in the case we become leader again here we need to re-add the entry

								if (
									await this.isLeader({
										entry,
										replicas: minReplicas.getValue(this),
									})
								) {
									logger.error("Unexpected: Is leader after delete");
								}
							});
					}, this.waitForPruneDelay),
				);
			};

				const reject = (e: any) => {
					clear();
					const isCheckedPruneTimeout =
						e instanceof Error &&
						typeof e.message === "string" &&
						e.message.startsWith("Timeout for checked pruning");
					if (explicitTimeout || !isCheckedPruneTimeout) {
						this.clearCheckedPruneRetry(entry.hash);
					}
					this.removePruneRequestSent(entry.hash);
					this._requestIPruneResponseReplicatorSet.delete(entry.hash);
					deferredPromise.reject(e);
				};

			// Checked prune requests can legitimately take longer than a fixed 10s:
			// - The remote may not have the entry yet and will wait up to `_respondToIHaveTimeout`
			// - Leadership/replicator information may take up to `waitForReplicatorTimeout` to settle
			// If we time out too early we can end up with permanently prunable heads that never
			// get retried (a common CI flake in "prune before join" tests).
			const checkedPruneTimeoutMs =
				options?.timeout ??
				Math.max(
					10_000,
					Number(this._respondToIHaveTimeout ?? 0) +
						this.waitForReplicatorTimeout +
						PRUNE_DEBOUNCE_INTERVAL * 2,
				);

				const timeout = setTimeout(() => {
					// For internal/background prune flows (no explicit timeout), retry a few times
					// to avoid "permanently prunable" entries when `_pendingIHave` expires under
					// heavy load.
					if (!explicitTimeout) {
						this.scheduleCheckedPruneRetry({ entry, leaders });
					}
					reject(
						new Error(
							`Timeout for checked pruning after ${checkedPruneTimeoutMs}ms (closed=${this.closed})`,
						),
					);
				}, checkedPruneTimeoutMs);
				timeout.unref?.();

			this._pendingDeletes.set(entry.hash, {
				promise: deferredPromise,
				clear,
				reject,
				resolve: async (publicKeyHash: string) => {
					const minReplicasObj = this.getClampedReplicas(minReplicas);
					const minReplicasValue = minReplicasObj.getValue(this);

					// TODO is this check necessary
					if (
						!(await this._waitForEntryReplicators(
							entry,
							minReplicasValue,
							[
								{ key: publicKeyHash, replicator: true },
								{
									key: this.node.identity.publicKey.hashcode(),
									replicator: false,
								},
							],
							{
								persist: false,
							},
						))
					) {
						return;
					}

					let existCounter = this._requestIPruneResponseReplicatorSet.get(
						entry.hash,
					);
						if (!existCounter) {
							existCounter = new Set();
							this._requestIPruneResponseReplicatorSet.set(
								entry.hash,
								existCounter,
							);
						}
						existCounter.add(publicKeyHash);
						// Seed provider hints so future remote reads can avoid extra round-trips.
						this.remoteBlocks.hintProviders(entry.hash, [publicKeyHash]);

						if (minReplicasValue <= existCounter.size) {
							resolve();
						}
					},
				});

			promises.push(deferredPromise.promise);
		}

		const emitMessages = async (entries: string[], to: string) => {
			const filteredSet: string[] = [];
			for (const entry of entries) {
				let set = this._requestIPruneSent.get(entry);
				if (!set) {
					set = new Set();
					this._requestIPruneSent.set(entry, set);
				}
				/* TODO why can we not have this statement? 
				if (set.has(to)) {
					continue;
				} */
				set.add(to);
				filteredSet.push(entry);
			}
			if (filteredSet.length > 0) {
				return this.rpc.send(
					new RequestIPrune({
						hashes: filteredSet,
					}),
					{
						mode: new SilentDelivery({
							to: [to], // TODO group by peers?
							redundancy: 1,
						}),
						priority: 1,
					},
				);
			}
		};

			for (const [k, v] of peerToEntries) {
				emitMessages(v, k);
			}

			// Keep remote `_pendingIHave` alive in the common "leader doesn't have entry yet"
			// case. This is intentionally disabled when an explicit timeout is provided to
			// preserve unit tests that assert remote `_pendingIHave` clears promptly.
			if (!explicitTimeout && peerToEntries.size > 0) {
				const respondToIHaveTimeout = Number(this._respondToIHaveTimeout ?? 0);
				const resendIntervalMs = Math.min(
					CHECKED_PRUNE_RESEND_INTERVAL_MAX_MS,
					Math.max(
						CHECKED_PRUNE_RESEND_INTERVAL_MIN_MS,
						Math.floor(respondToIHaveTimeout / 2) || 1_000,
					),
				);
				let inFlight = false;
				const timer = setInterval(() => {
					if (inFlight) return;
					if (this.closed) return;

					const pendingByPeer: [string, string[]][] = [];
					for (const [peer, hashes] of peerToEntries) {
						const pending = hashes.filter((h) => this._pendingDeletes.has(h));
						if (pending.length > 0) {
							pendingByPeer.push([peer, pending]);
						}
					}
					if (pendingByPeer.length === 0) {
						clearInterval(timer);
						return;
					}

					inFlight = true;
					Promise.allSettled(
						pendingByPeer.map(([peer, hashes]) =>
							emitMessages(hashes, peer).catch(() => {}),
						),
					).finally(() => {
						inFlight = false;
					});
				}, resendIntervalMs);
				timer.unref?.();
				cleanupTimer.push(timer as any);
			}

			let cleanup = () => {
				for (const timer of cleanupTimer) {
					clearTimeout(timer);
				}
				this._closeController.signal.removeEventListener("abort", cleanup);
			};

		Promise.allSettled(promises).finally(cleanup);
		this._closeController.signal.addEventListener("abort", cleanup);
		return promises;
	}

	/**
	 * For debugging
	 */
	async getPrunable(roleAge?: number) {
		const heads = await this.log.getHeads(true).all();
		let prunable: Entry<any>[] = [];
		for (const head of heads) {
			const isLeader = await this.isLeader(
				{ entry: head, replicas: maxReplicas(this, [head]) },
				{ roleAge },
			);
			if (!isLeader) {
				prunable.push(head);
			}
		}
		return prunable;
	}

	async getNonPrunable(roleAge?: number) {
		const heads = await this.log.getHeads(true).all();
		let nonPrunable: Entry<any>[] = [];
		for (const head of heads) {
			const isLeader = await this.isLeader(
				{ entry: head, replicas: maxReplicas(this, [head]) },
				{ roleAge },
			);
			if (isLeader) {
				nonPrunable.push(head);
			}
		}
		return nonPrunable;
	}

	async rebalanceAll(options?: { clearCache?: boolean }) {
		if (options?.clearCache) {
			this._gidPeersHistory.clear();
			this._nativeSharedLogState?.clearGidPeers();
		}

		const timestamp = BigInt(+new Date());
		return this.onReplicationChange(
			(await this.getAllReplicationSegments()).map((x) => {
				return { range: x, type: "added", timestamp };
			}),
		);
	}

	async waitForPruned(options?: {
		timeout?: number;
		signal?: AbortSignal;
		delayInterval?: number;
		timeoutMessage?: string;
	}) {
		await waitFor(() => this._pendingDeletes.size === 0, options);
	}

	async onReplicationChange(
		changeOrChanges:
			| ReplicationChanges<ReplicationRangeIndexable<R>>
			| ReplicationChanges<ReplicationRangeIndexable<R>>[],
	) {
		/**
		 * TODO use information of new joined/leaving peer to create a subset of heads
		 * that we potentially need to share with other peers
		 */

		if (this.closed) {
			return;
		}

		await this.log.trim();

		const batchedChanges = Array.isArray(changeOrChanges[0])
			? (changeOrChanges as ReplicationChanges<ReplicationRangeIndexable<R>>[])
			: [changeOrChanges as ReplicationChanges<ReplicationRangeIndexable<R>>];
		const changes = batchedChanges.flat();
		const selfHash = this.node.identity.publicKey.hashcode();
		// On removed ranges (peer leaves / shrink), gid-level history can hide
		// per-entry gaps. Force a fresh delivery pass for reassigned entries.
		const forceFreshDelivery = changes.some(
			(change) => change.type === "removed" && change.range.hash !== selfHash,
		);
		const gidPeersHistorySnapshot = new Map<string, Set<string> | undefined>();
		const dedupeCutoff = Date.now() - RECENT_REPAIR_DISPATCH_TTL_MS;
		for (const [target, hashes] of this._recentRepairDispatch) {
			for (const [hash, ts] of hashes) {
				if (ts <= dedupeCutoff) {
					hashes.delete(hash);
				}
			}
			if (hashes.size === 0) {
				this._recentRepairDispatch.delete(target);
			}
		}

			const changed = false;
			const addedPeers = new Set<string>();
			const authoritativeRepairPeers = new Set<string>();
			const warmupPeers = new Set<string>();
			const churnRepairPeers = new Set<string>();
			const hasSelfWarmupChange = changes.some(
				(change) =>
					change.range.hash === selfHash &&
					(change.type === "added" || change.type === "replaced"),
			);
			const hasSelfRangeRemoval = changes.some(
				(change) =>
					change.range.hash === selfHash &&
					(change.type === "removed" || change.type === "replaced"),
			);
			for (const change of changes) {
				if (
					change.range.hash !== selfHash &&
					(change.type === "removed" || change.type === "replaced")
				) {
					this.removePeerFromEntryKnownPeers(change.range.hash);
				}
				if (change.type === "added" || change.type === "replaced") {
					const hash = change.range.hash;
					if (hash !== selfHash) {
						// Existing peers can widen/shift ranges after the initial join. If we
						// only rescan on first-seen "added", late authoritative range updates can
						// leave historical backfill permanently partial under load.
						authoritativeRepairPeers.add(hash);
						// Range updates can reassign entries to an existing peer shortly after it
						// already received a subset. Avoid suppressing legitimate follow-up repair.
						this._recentRepairDispatch.delete(hash);
					}
				}
				if (change.type === "added") {
					const hash = change.range.hash;
					if (hash !== selfHash) {
						addedPeers.add(hash);
						warmupPeers.add(hash);
					}
				}
			}
		const hasAdaptiveStorageLimit =
			this._isAdaptiveReplicating &&
			this.replicationController?.maxMemoryLimit != null;
		const useJoinWarmupFastPath =
			!forceFreshDelivery &&
			warmupPeers.size > 0 &&
			!hasSelfWarmupChange &&
			!hasAdaptiveStorageLimit;
		const immediateRebalanceChanges = useJoinWarmupFastPath
			? changes.filter(
					(change) =>
						!(
							change.range.hash === selfHash &&
							(change.type === "added" || change.type === "replaced")
						),
				)
			: changes;

		try {
			const uncheckedDeliver: Map<
				string,
				Map<string, EntryReplicated<any>>
			> = new Map();
				const flushUncheckedDeliverTarget = (target: string) => {
					const entries = uncheckedDeliver.get(target);
					if (!entries || entries.size === 0) {
						return;
					}
					const isWarmupTarget = warmupPeers.has(target);
					const mode: RepairDispatchMode = forceFreshDelivery
						? "churn"
						: isWarmupTarget
							? "join-warmup"
							: "join-authoritative";
					this.dispatchMaybeMissingEntries(target, entries, {
						bypassRecentDedupe: isWarmupTarget || forceFreshDelivery,
						mode,
						retryScheduleMs:
							mode === "join-warmup"
								? JOIN_WARMUP_RETRY_SCHEDULE_MS
								: mode === "join-authoritative"
									? [0]
									: undefined,
					});
					uncheckedDeliver.delete(target);
				};
			const queueUncheckedDeliver = (
				target: string,
				entry: EntryReplicated<any>,
			) => {
				churnRepairPeers.add(target);
				let set = uncheckedDeliver.get(target);
				if (!set) {
					set = new Map();
					uncheckedDeliver.set(target, set);
				}
				if (set.has(entry.hash)) {
					return;
				}
				set.set(entry.hash, entry);
				if (set.size >= this.repairSweepTargetBufferSize) {
					flushUncheckedDeliverTarget(target);
				}
			};

				if (immediateRebalanceChanges.length > 0) {
					for await (const entryReplicated of toRebalance<R>(
						immediateRebalanceChanges,
						this.entryCoordinatesIndex,
						this.recentlyRebalanced,
						{
							forceFresh: forceFreshDelivery || useJoinWarmupFastPath,
						},
					)) {
						if (this.closed) {
							break;
						}

						if (useJoinWarmupFastPath) {
							let oldPeersSet: Set<string> | undefined;
							const gid = entryReplicated.gid;
							oldPeersSet = gidPeersHistorySnapshot.get(gid);
							if (!gidPeersHistorySnapshot.has(gid)) {
								const existing = this._gidPeersHistory.get(gid);
								oldPeersSet = existing ? new Set(existing) : undefined;
								gidPeersHistorySnapshot.set(gid, oldPeersSet);
							}

							for (const target of warmupPeers) {
								queueUncheckedDeliver(target, entryReplicated);
							}

							const candidatePeers = new Set<string>([selfHash]);
							for (const target of warmupPeers) {
								candidatePeers.add(target);
							}
							if (oldPeersSet) {
								for (const oldPeer of oldPeersSet) {
									candidatePeers.add(oldPeer);
								}
							}

							const currentPeers = await this.findLeaders(
								entryReplicated.coordinates,
								entryReplicated,
								{
									roleAge: 0,
									candidates: candidatePeers,
									persist: false,
								},
							);

							if (oldPeersSet) {
								for (const oldPeer of oldPeersSet) {
									if (!currentPeers.has(oldPeer)) {
										this.removePruneRequestSent(entryReplicated.hash);
									}
								}
							}

							for (const [peer] of currentPeers) {
								if (warmupPeers.has(peer)) {
									this.markRepairSweepOptimisticPeer(entryReplicated.gid, peer);
								}
							}

							const authoritativePeers = [...currentPeers.keys()].filter(
								(peer) =>
									!warmupPeers.has(peer) &&
									!this.hasPendingRepairSweepOptimisticPeer(entryReplicated.gid, peer),
							);
							this.addPeersToGidPeerHistory(
								entryReplicated.gid,
								authoritativePeers,
								true,
							);

							if (!currentPeers.has(selfHash)) {
								this.pruneDebouncedFnAddIfNotKeeping({
									key: entryReplicated.hash,
									value: { entry: entryReplicated, leaders: currentPeers },
								});

								this.responseToPruneDebouncedFn.delete(entryReplicated.hash);
							} else {
								await this.cancelCheckedPruneForLocalLeader(entryReplicated.hash);
							}
							continue;
						}

					let oldPeersSet: Set<string> | undefined;
					const gid = entryReplicated.gid;
					oldPeersSet = gidPeersHistorySnapshot.get(gid);
					if (!gidPeersHistorySnapshot.has(gid)) {
						const existing = this._gidPeersHistory.get(gid);
						oldPeersSet = existing ? new Set(existing) : undefined;
						gidPeersHistorySnapshot.set(gid, oldPeersSet);
					}

					let isLeader = false;
					const currentPeers = await this.findLeaders(
						entryReplicated.coordinates,
						entryReplicated,
						{
							// We do this to make sure new replicators get data even though
							// they are not mature so they can figure out if they want to
							// replicate more or less.
							roleAge: 0,
						},
					);

					for (const [currentPeer] of currentPeers) {
						if (currentPeer === this.node.identity.publicKey.hashcode()) {
							isLeader = true;
							continue;
						}

						if (!oldPeersSet?.has(currentPeer)) {
							queueUncheckedDeliver(currentPeer, entryReplicated);
						}
					}

						if (oldPeersSet) {
							for (const oldPeer of oldPeersSet) {
								if (!currentPeers.has(oldPeer)) {
									this.removePruneRequestSent(entryReplicated.hash);
								}
							}
						}

						for (const [peer] of currentPeers) {
							if (addedPeers.has(peer)) {
								this.markRepairSweepOptimisticPeer(entryReplicated.gid, peer);
							}
						}

						const authoritativePeers = [...currentPeers.keys()].filter(
							(peer) =>
								!addedPeers.has(peer) &&
								!this.hasPendingRepairSweepOptimisticPeer(entryReplicated.gid, peer),
						);
						this.addPeersToGidPeerHistory(
							entryReplicated.gid,
							authoritativePeers,
							true,
						);

					if (!isLeader) {
						this.pruneDebouncedFnAddIfNotKeeping({
							key: entryReplicated.hash,
							value: { entry: entryReplicated, leaders: currentPeers },
						});

						this.responseToPruneDebouncedFn.delete(entryReplicated.hash); // don't allow others to prune because of expecting me to replicating this entry
					} else {
						await this.cancelCheckedPruneForLocalLeader(entryReplicated.hash);
					}
				}
				}

				if (forceFreshDelivery) {
					// Pure leave/shrink churn can have zero `addedPeers`, but the peers that
					// received redistributed entries still need a follow-up repair pass if the
					// immediate maybe-sync misses one entry.
					this.scheduleRepairSweep({
						mode: "churn",
						peers: churnRepairPeers,
					});
				} else if (useJoinWarmupFastPath) {
					// Pure join warmup uses the cheap immediate maybe-missing dispatch above,
					// then defers the authoritative sweep so it does not compete with the
					// write burst itself.
					const peers = new Set(addedPeers);
					const timer = setTimeout(() => {
						this._repairRetryTimers.delete(timer);
						if (this.closed) {
							return;
						}
						this.scheduleRepairSweep({
							mode: "join-warmup",
							peers,
						});
					}, 250);
					timer.unref?.();
					this._repairRetryTimers.add(timer);
				} else if (authoritativeRepairPeers.size > 0) {
					this.scheduleRepairSweep({
						mode: "join-authoritative",
						peers: authoritativeRepairPeers,
					});
				}

				if (!forceFreshDelivery && authoritativeRepairPeers.size > 0) {
					this.scheduleJoinAuthoritativeRepair(authoritativeRepairPeers);
				}

			for (const target of [...uncheckedDeliver.keys()]) {
				flushUncheckedDeliverTarget(target);
			}

			if (this._isAdaptiveReplicating && hasSelfRangeRemoval) {
				// Adaptive shrink/replacement can make already-indexed local heads
				// prunable even when the incremental rebalance scan missed them under
				// churn or timing pressure. Re-scan after repair dispatches are flushed
				// so checked prune work is enqueued before callers wait for idle.
				await this.pruneIndexedEntriesNoLongerLed();
			}

			return changed;
		} catch (error: any) {
			if (isNotStartedError(error)) {
				return false; // we are not started yet, so no changes
			}

			logger.error(error.toString());
			throw error;
		}
	}

	async _onUnsubscription(evt: CustomEvent<UnsubcriptionEvent>) {
		logger.trace(
			`Peer disconnected '${evt.detail.from.hashcode()}' from '${JSON.stringify(
				evt.detail.topics.map((x) => x),
			)} '`,
		);
		if (!evt.detail.topics.includes(this.topic)) {
			return;
		}

		const fromHash = evt.detail.from.hashcode();
		this._replicationInfoBlockedPeers.add(fromHash);
		this._recentRepairDispatch.delete(fromHash);

		// Keep a per-peer timestamp watermark when we observe an unsubscribe. This
		// prevents late/out-of-order replication-info messages from re-introducing
		// stale segments for a peer that has already left the topic.
		const now = BigInt(+new Date());
		const prev = this.latestReplicationInfoMessage.get(fromHash);
		if (!prev || prev < now) {
			this.latestReplicationInfoMessage.set(fromHash, now);
		}
		this.invalidateSharedLogTopicSubscribersCache();

		return this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.topics,
			false,
		);
	}

	async _onSubscription(evt: CustomEvent<SubscriptionEvent>) {
		logger.trace(
			`New peer '${evt.detail.from.hashcode()}' connected to '${JSON.stringify(
				evt.detail.topics.map((x) => x),
			)}'`,
		);
		if (!evt.detail.topics.includes(this.topic)) {
			return;
		}

		this.remoteBlocks.onReachable(evt.detail.from);
		this._replicationInfoBlockedPeers.delete(evt.detail.from.hashcode());
		this.invalidateSharedLogTopicSubscribersCache();

		await this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.topics,
			true,
		);
	}

	async rebalanceParticipation() {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu

		const isClosedStoreRace = (error: any) => {
			const message =
				typeof error?.message === "string" ? error.message : String(error);
			return (
				this.closed ||
				message.includes("Iterator is not open") ||
				message.includes("cannot read after close()") ||
				message.includes("Database is not open")
			);
		};

		const fn = async () => {
			if (this.closed) {
				return false;
			}

			// The role is fixed (no changes depending on memory usage or peer count etc)
			if (!this._isReplicating) {
				return false;
			}

			if (this._isAdaptiveReplicating) {
				if (this.shouldDelayAdaptiveRebalance()) {
					this.rebalanceParticipationDebounced?.call();
					return false;
				}

				const peers = this.replicationIndex;
				const usedMemory = await this.getMemoryUsage();
				let dynamicRange = await this.getDynamicRange();

				if (!dynamicRange) {
					return; // not allowed to replicate
				}

				if (
					this.replicationController.maxMemoryLimit != null &&
					usedMemory > this.replicationController.maxMemoryLimit
				) {
					await this.pruneIndexedEntriesNoLongerLed();
				}

				const peersSize = (await peers.getSize()) || 1;
				const totalParticipation = await this.calculateTotalParticipation();

				const newFactor = this.replicationController.step({
					memoryUsage: usedMemory,
					currentFactor: dynamicRange.widthNormalized,
					totalFactor: totalParticipation, // TODO use this._totalParticipation when flakiness is fixed
					peerCount: peersSize,
					cpuUsage: this.cpuUsage?.value(),
				});

				const absoluteDifference = Math.abs(
					dynamicRange.widthNormalized - newFactor,
				);
				const relativeDifference =
					absoluteDifference /
					Math.max(
						dynamicRange.widthNormalized,
						RECALCULATE_PARTICIPATION_RELATIVE_DENOMINATOR_FLOOR,
					);

				let minRelativeChange = RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE;
				if (this.replicationController.maxMemoryLimit != null) {
					minRelativeChange =
						RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_MEMORY_LIMIT;
				} else if (this.replicationController.maxCPUUsage != null) {
					minRelativeChange =
						RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_CPU_LIMIT;
				}

				if (relativeDifference > minRelativeChange) {
					// TODO can not reuse old range, since it will (potentially) affect the index because of sideeffects
					dynamicRange = new this.indexableDomain.constructorRange({
						offset: dynamicRange.start1,
						width: this.indexableDomain.numbers.denormalize(newFactor),
						publicKeyHash: dynamicRange.hash,
						id: dynamicRange.id,
						mode: dynamicRange.mode,
						timestamp: dynamicRange.timestamp,
					});

					const canReplicate =
						!this._isTrustedReplicator ||
						(await this._isTrustedReplicator(this.node.identity.publicKey));
					if (!canReplicate) {
						return false;
					}

					await this.startAnnounceReplicating([dynamicRange], {
						checkDuplicates: false,
						reset: false,
					});

					/* await this._updateRole(newRole, onRoleChange); */
					this.rebalanceParticipationDebounced?.call();

					return true;
				} else {
					this.rebalanceParticipationDebounced?.call();
				}
				return false;
			}
			return false;
		};

		const resp = await fn().catch((error: any) => {
			if (isNotStartedError(error) || isClosedStoreRace(error)) {
				return false;
			}
			throw error;
		});

		return resp;
	}

	private getDynamicRangeOffset(): NumberFromType<R> {
		const options = this._logProperties
			?.replicate as DynamicReplicationOptions<R>;
		if (options?.offset != null) {
			const normalized = options.normalized ?? true;
			return (
				normalized
					? this.indexableDomain.numbers.denormalize(Number(options.offset))
					: options.offset
			) as NumberFromType<R>;
		}

		return this.indexableDomain.numbers.bytesToNumber(
			this.node.identity.publicKey.bytes,
		);
	}
	async getDynamicRange() {
		let dynamicRangeId = getIdForDynamicRange(this.node.identity.publicKey);
		let range = (
			await this.replicationIndex
				.iterate({
					query: [
						new ByteMatchQuery({
							key: "id",
							value: dynamicRangeId,
						}),
					],
				})
				.all()
		)?.[0]?.value;
		if (!range) {
			range = new this.indexableDomain.constructorRange({
				offset: this.getDynamicRangeOffset(),
				width: this.indexableDomain.numbers.zero,
				publicKeyHash: this.node.identity.publicKey.hashcode(),
				mode: ReplicationIntent.NonStrict,
				timestamp: BigInt(+new Date()),
				id: dynamicRangeId,
			});
			const added = await this.addReplicationRange(
				[range],
				this.node.identity.publicKey,
				{ reset: false, checkDuplicates: false },
			);
			if (!added) {
				warn("Not allowed to replicate by canReplicate");
				return;
			}
		}
		return range;
	}

	private async onEntryAdded(entry: Entry<any>) {
		const ih = this._pendingIHave.get(entry.hash);

		if (ih) {
			ih.clear();
			ih.callback(entry);
		}

		this.syncronizer.onEntryAdded(entry);
	}

	onEntryRemoved(hash: string) {
		this.syncronizer.onEntryRemoved(hash);
	}
}
