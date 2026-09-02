import {
	BorshError,
	deserialize,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import type { AnyStore } from "@peerbit/any-store";
import {
	AnyBlockStore,
	type EagerBlocksSetting,
	RemoteBlocks,
} from "@peerbit/blocks";
import { cidifyString } from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import {
	AccessError,
	Ed25519Keypair,
	Ed25519PublicKey,
	PublicSignKey,
	Secp256k1PublicKey,
	getPublicKeyFromPeerId,
	randomBytes,
	sha256Base64Sync,
	sha256Sync,
	toHexString,
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
	type AppendOptions,
	type Change,
	type Ed25519VerifyBatchInput,
	Entry,
	type EntryIndexHashMutationLockOwner,
	EntryType,
	LamportClock,
	Log,
	type LogEvents,
	type LogProperties,
	Meta,
	type PreparedAppendFacts,
	type PreparedAppendJoinFacts,
	ShallowEntry,
	ShallowMeta,
	type ShallowOrFullEntry,
	Timestamp,
	verifyEd25519Batch,
	verifyEntryV0Ed25519BatchFromEntries,
} from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import type {
	NativeBackboneAppendResult,
	NativeBackboneCoordinateFields,
	NativeBackboneRawReceiveGroupAssignmentPlan,
	NativeBackboneRawReceiveGroupIndexPlan,
	NativeBackboneRawReceiveGroupLeaderPlan,
	NativeBackboneRawReceiveGroupPlan,
	NativeBackboneRawReceiveSelectionPlan,
	NativePeerbitBackbone,
	NativeBackboneCoordinatePersistenceConfig as RuntimeNativeBackboneCoordinatePersistenceConfig,
} from "@peerbit/native-backbone";
import {
	ClosedError,
	Program,
	type ProgramEvents,
	TerminalOperationNotStartedError,
} from "@peerbit/program";
import {
	FanoutChannel,
	type FanoutProviderHandle,
	type FanoutTree,
	type FanoutTreeChannelOptions,
	type FanoutTreeDataEvent,
	type FanoutTreeJoinOptions,
	type FanoutTreeUnicastEvent,
	waitForSubscribers,
} from "@peerbit/pubsub";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { RPC, type RequestContext } from "@peerbit/rpc";
import type {
	AppendDeliveryPlan,
	NativeAppendCoordinatePlan,
	NativeReplicationRange,
	SharedLogNativeState,
	SharedLogRangePlanner,
} from "@peerbit/shared-log-rust";
import {
	AcknowledgeDelivery,
	AnyWhere,
	BACKGROUND_MESSAGE_PRIORITY,
	CONVERGENCE_MESSAGE_PRIORITY,
	DataMessage,
	MessageHeader,
	NotStartedError,
	type RouteHint,
	SilentDelivery,
	createRequestTransportContext,
} from "@peerbit/stream-interface";
import {
	AbortError,
	TimeoutError,
	debounceFixedInterval,
	delay,
	waitFor,
} from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";
import PQueue from "p-queue";
import { concat, fromString } from "uint8arrays";
import { BlocksMessage } from "./blocks.js";
import {
	CheckedPruneCoordinator,
	type CheckedPruneEntry,
	type CheckedPruneLeaderMap,
	type CheckedPrunePendingDelete,
	type CheckedPruneRestartCandidate,
	type CheckedPruneRetryIdentity,
	type CheckedPruneRetryState,
} from "./checked-prune.js";
import {
	CoordinatePersistenceCoordinator,
	type MaybePromise,
	combineCoordinateDeleteHashes,
	isPromiseLike,
	mapMaybePromise,
	normalizedHashValues,
} from "./coordinate-persistence.js";
import { type CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
import {
	type DebouncedAccumulatorMap,
	debouncedAccumulatorMap,
} from "./debounce.js";
import {
	CompatibilityModeRetiredError,
	NativeDurableCommitError,
	NoPeersError,
	PersistedDeliveryError,
	isNotStartedError,
} from "./errors.js";
import {
	EXCHANGE_HEADS_REPAIR_HINT,
	EntryWithRefs,
	ExchangeHeadsMessage,
	MAX_RAW_EXCHANGE_MESSAGE_SIZE,
	RawEntryWithRefs,
	type RawExchangeHeadSendPlan,
	RawExchangeHeadsMessage,
	type RawReceiveHashSelection,
	RequestIPrune,
	RequestIPruneV2,
	ResponseIPrune,
	ResponseIPruneV2,
	SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
	SYNC_CAPABILITY_RAW_EXCHANGE_HEADS,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND,
	StashBackedRawExchangeHeadsMessage,
	SyncCapabilitiesMessage,
	collectRawExchangeHeadSendPlan,
	createExchangeHeadsMessages,
	createRawExchangeHeadsMessages,
	getExchangeHeadHash,
	getPreparedRawExchangeGid,
	getPreparedRawExchangeHashNumber,
	getPreparedRawExchangeHeadAppendFacts,
	getPreparedRawExchangeHeadGid,
	getPreparedRawExchangeHeadRequestedReplicas,
	getPreparedRawExchangeHeadShallowEntry,
	getPreparedRawExchangeHeadSignatureVerified,
	getPreparedRawExchangeNext,
	getPreparedRawExchangeRequestedReplicas,
	getPreparedRawExchangeTimestamp,
	getRawExchangeHeadByteLength,
	getRawExchangeHeadStashIndexes,
	initExchangeHeadEntry,
	isPreparedRawEntryWithRefs,
	isStashBackedRawExchangeHeadsMessage,
	materializeVerifiedRawExchangeHeadsMessage,
} from "./exchange-heads.js";
import { FanoutEnvelope } from "./fanout-envelope.js";
import { InstanceLifecycle } from "./instance-lifecycle.js";
import {
	MAX_U32,
	MAX_U64,
	type NumberFromType,
	type Numbers,
	createNumbers,
} from "./integers.js";
import { JoinWarmupCoordinator, type WarmupSession } from "./join-warmup.js";
import { LeaderPlanCache } from "./leader-plan-cache.js";
import { TransportMessage } from "./message.js";
import { NativeBackboneWriteThroughBlockStore } from "./native-write-through-block-store.js";
import { type PeerSession, PeerSessionRegistry } from "./peer-session.js";
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
	isEntryReplicated,
	isMatured,
	isReplicationRangeMessage,
	mergeRanges,
	minimumWidthToCover,
	shouldAssigneToRangeBoundary as shouldAssignToRangeBoundary,
	toRebalance,
} from "./ranges.js";
import {
	type ReceiveSignatureVerificationFact,
	receiveSignatureVerificationResult as getReceiveSignatureVerificationResult,
	withReceiveSignatureVerificationFacts as withSignatureVerificationFacts,
} from "./receive-signature-verification.js";
import { ReplicationAnnouncementCoordinator } from "./replication-announcement.js";
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
import type {
	AddedReplicationInfoMutation,
	FullReplicationInfoMutation,
} from "./replication-info-mutation.js";
import { ReplicationInfoV2ReceiveCoordinator } from "./replication-info-v2-receive.js";
import {
	type ReplicationInfoV2ConfirmationOptions,
	ReplicationInfoV2SendCoordinator,
} from "./replication-info-v2-send.js";
import {
	type ReplicationStatus,
	classifyReplicationStatus,
} from "./replication-status.js";
import {
	AbsoluteReplicas,
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	FullReplicationInfoV2Message,
	MinReplicas,
	ReplicationError,
	ReplicationInfoV2AppliedMessage,
	type ReplicationInfoV2Message,
	type ReplicationLimits,
	ReplicationPingMessage,
	RequestReplicationInfoMessage,
	RequestReplicationInfoV2AppliedMessage,
	RequestReplicationInfoV2Message,
	ResponseRoleMessage,
	StoppedReplicating,
	StoppedReplicationInfoV2Message,
	decodeReplicas,
	encodeReplicas,
	isReplicationInfoV2Message,
	maxReplicas,
} from "./replication.js";
import { ReplicatorLivenessMonitor } from "./replicator-liveness.js";
import { createSyncronizer } from "./sync/factory.js";
import type {
	SharedLogNativeWireSync,
	SyncEntryCoordinates,
	SyncOptions,
	SyncProfileFn,
	SynchronizerConstructor,
	Syncronizer,
} from "./sync/index.js";
import {
	emitSyncProfileDuration,
	emitSyncProfileEvent,
	syncProfileStart,
} from "./sync/profile.js";
import {
	ConfirmEntriesMessage,
	RECENT_KNOWN_EXCHANGE_HEAD_SUPPRESSION_MS,
	RequestPersistedEntriesV1,
	SYNC_MESSAGE_PRIORITY,
	SimpleSyncronizer,
} from "./sync/simple.js";
import { groupByGid, tryGroupByGidSync } from "./utils.js";

type SharedLogServicesWithFanout = {
	fanout?: FanoutTree;
};

const getSharedLogFanoutService = (services: unknown): FanoutTree | undefined =>
	(services as SharedLogServicesWithFanout).fanout;

type PendingIHave<T> = {
	resetTimeout: () => void;
	requesting: Map<string, Uint8Array>;
	clear: () => void;
	callback: (entry: Entry<T>) => MaybePromise<void>;
	expiresAt?: number;
};

type PeerReceiveLeaseBucket = {
	active: number;
	drain?: DeferredPromise<void>;
};

type PeerReceiveLeaseState = {
	current: PeerReceiveLeaseBucket;
	activeBuckets: Set<PeerReceiveLeaseBucket>;
};

/**
 * A one-shot handle over an acquired peer receive lease. The replication-info
 * arms of `onMessage` release the lease BEFORE joining their apply lane and
 * the shared finally releases it on every other path; only the first release
 * has any effect (pinned by the stage-4.5 lease one-shot test).
 */
type PeerReceiveLease = {
	release: () => void;
};

const createOneShotPeerReceiveLease = (
	releaseFn: () => void,
): PeerReceiveLease => {
	let released = false;
	return {
		release: () => {
			if (released) {
				return;
			}
			released = true;
			releaseFn();
		},
	};
};

/**
 * Receive-scope captures shared with the extracted control-plane handlers of
 * `onMessage` (stage 4.5). Constructed only after the Raw/ExchangeHeads fast
 * path has returned or fallen through, so the heads path allocates nothing
 * for it. Every field is the `onMessage` prelude capture of the same
 * `receive*` name; the handlers re-alias them locally so their bodies stay
 * byte-identical with the pre-extraction branches.
 */
type ReceiveLaneContext = {
	fromHash: string;
	session: PeerSession | null;
	lifecycleController: AbortController | undefined;
	ownershipLifecycleController: AbortController;
	receiveEpoch: object | null;
	syncProfile: SyncProfileFn | undefined;
	lease: PeerReceiveLease;
};

/** `onMessage`'s prelude throws when `from` is missing; handlers run after. */
type ReceiveRequestContext = RequestContext & { from: PublicSignKey };

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
export * from "./replication-status.js";
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
	CompatibilityModeRetiredError,
	NativeDurableCommitError,
	NoPeersError,
	PersistedDeliveryError,
};
export { MAX_U32, MAX_U64, type NumberFromType };
export type {
	SharedLogNativeWireSync,
	SyncOptions,
	SyncProfileEvent,
	SyncProfileFn,
} from "./sync/index.js";
export {
	ExchangeHeadsMessage,
	RawExchangeHeadsMessage,
	StashBackedRawExchangeHeadsMessage,
};
export const logger = loggerFn("peerbit:shared-log");
const warn = logger.newScope("warn");
const traceLogger = logger.trace as typeof logger.trace & { enabled?: boolean };

const emitAdvisorySyncProfileDuration = (
	profile: SyncProfileFn | undefined,
	startedAt: number,
	event: Parameters<typeof emitSyncProfileDuration>[2],
): void => {
	try {
		emitSyncProfileDuration(profile, startedAt, event);
	} catch {
		// Diagnostics must not change open or provider-resolution correctness.
	}
};

const canUseOptionalNativeModuleImports = (): boolean => {
	const scope = globalThis as {
		ServiceWorkerGlobalScope?: unknown;
		clients?: unknown;
		registration?: unknown;
		skipWaiting?: unknown;
	};
	const serviceWorkerGlobalScope = scope.ServiceWorkerGlobalScope;
	return !(
		(typeof serviceWorkerGlobalScope === "function" &&
			globalThis instanceof serviceWorkerGlobalScope) ||
		(!!scope.clients &&
			!!scope.registration &&
			typeof scope.skipWaiting === "function")
	);
};

/**
 * Build the per-program coordinate persistence directory under a node's
 * durable storage root: `<nodeDirectory>/coordinates/<fsSafeLogId>`. Uses
 * forward-slash joining (accepted by Node's `fs` on every platform) so
 * shared-log does not need to statically import `node:path`, which is not
 * available in browser bundles. Trailing separators on the root are trimmed
 * to avoid doubled slashes.
 */
const joinNativeCoordinateDirectory = (
	nodeDirectory: string,
	fsSafeLogId: string,
): string =>
	`${nodeDirectory.replace(/[/\\]+$/, "")}/coordinates/${fsSafeLogId}`;

type DurableBlockSublevelStore = {
	readonly supportsCrashSafeJournalCheckpoint?: boolean;
	sublevel(
		name: string,
		options?: {
			compactOnClose?: boolean;
			compactOnCloseMinJournalBytes?: number;
			compactMaxJournalBytes?: number;
			durability?: "normal" | "strict";
		},
	): MaybePromise<AnyStore>;
};

const defaultNativeEntryBlockCompactMaxJournalBytes = 64 * 1024 * 1024;

const createNativeDurableBlockStore = async (
	storage: DurableBlockSublevelStore,
): Promise<AnyBlockStore> =>
	new AnyBlockStore(
		await storage.sublevel("blocks", {
			// Strict mirrors remain WAL-backed. On POSIX Node, checkpoint only the
			// historical suffix through the Rust store's fsync + atomic-rename path;
			// browsers/custom backends and Windows retain the prior unbounded WAL until
			// they expose an equally strong directory durability barrier.
			compactOnClose: false,
			...(storage.supportsCrashSafeJournalCheckpoint === true
				? {
						compactMaxJournalBytes:
							defaultNativeEntryBlockCompactMaxJournalBytes,
					}
				: {}),
			// A native append is acknowledged only after this mirror resolves. The
			// Rust store's normal immutable fast path may resolve before its WAL write;
			// strict mode waits for the journal write and sync, closing the SIGKILL gap.
			durability: "strict",
		}),
	);

const createDefaultDurableBlockStore = async (
	storage: DurableBlockSublevelStore,
): Promise<AnyBlockStore> =>
	new AnyBlockStore(
		await storage.sublevel("blocks", {
			// State this default explicitly so a cached child created by the native
			// path cannot silently carry its deferred-close policy into this path.
			compactOnClose: true,
		}),
	);

type LeaderMap = Map<string, { intersecting: boolean }>;

type LeaderSelectionOptions<R extends "u32" | "u64"> = {
	roleAge?: number;
	candidates?: Iterable<string>;
	freshLeaderPlan?: boolean;
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

type ReceiveLeaderObservation = {
	isLeader: boolean;
	fromIsLeader: boolean;
};

// Keep long-lived role callbacks outside the receive activation so event-target
// wrappers cannot retain decoded entries through the callback's closure context.
const createReceiveLeaderObserver =
	(observation: ReceiveLeaderObservation, localKey: string, fromKey: string) =>
	(key: string) => {
		observation.isLeader ||= localKey === key;
		observation.fromIsLeader ||= fromKey === key;
	};

type PendingMaturityRecord<R extends "u32" | "u64"> = {
	range: ReplicationChange<ReplicationRangeIndexable<R>>;
	timeout: ReturnType<typeof setTimeout>;
	expiresAt: number;
	from: PublicSignKey;
	rebalance: boolean;
	ownershipLifecycleController: AbortController;
};

type ReplicationRangeDeletionOutcome<R extends "u32" | "u64"> = {
	removed: ReplicationRangeIndexable<R>[];
	retained: ReplicationRangeIndexable<R>[];
	ownerHasRanges: boolean;
	rollback: () => Promise<void>;
	error?: unknown;
};

export type EntryLeaderPlan<R extends "u32" | "u64"> = {
	coordinates: NumberFromType<R>[];
	coordinateStrings?: string[];
	leaders: LeaderMap;
	isLeader: boolean;
	assignedToRangeBoundary?: boolean;
};

export type ReusableReceiveCoordinatePlan<R extends "u32" | "u64"> = {
	plan: EntryLeaderPlan<R>;
	replicas: number;
	prepared: PreparedCoordinatePersistence<R>;
};

export type DecodedReplicaCountMap = ReadonlyMap<string, number>;
export type SharedLogCoordinateNativeFields<R extends "u32" | "u64"> = {
	hash: string;
	hashNumber: NumberFromType<R>;
	hashNumberString?: string;
	gid: string;
	coordinates: NumberFromType<R>[];
	coordinateStrings?: string[];
	wallTime: bigint;
	wallTimeString?: string;
	assignedToRangeBoundary: boolean;
	metaBytes: Uint8Array;
};

export type PreparedCoordinatePersistence<R extends "u32" | "u64"> = {
	coordinateEntry?: EntryReplicated<R>;
	assignedToRangeBoundary: boolean;
	fields: SharedLogCoordinateNativeFields<R>;
};

export type ResidentCoordinateEntry<R extends "u32" | "u64"> =
	| EntryReplicated<R>
	| SharedLogCoordinateNativeFields<R>;

export type CoordinatePersistBatchItem<R extends "u32" | "u64"> = {
	coordinates: NumberFromType<R>[];
	entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
	leaders: LeaderMap | false;
	replicas: number;
	prev?: EntryReplicated<R>;
	assignedToRangeBoundary?: boolean;
	commitNative?: boolean;
	commitNativeBackbone?: boolean;
	hashNumber?: NumberFromType<R>;
	prepared?: PreparedCoordinatePersistence<R>;
};

export type NativeBackboneReceiveCoordinateRow<R extends "u32" | "u64"> = {
	item: CoordinatePersistBatchItem<R>;
	prepared: PreparedCoordinatePersistence<R>;
	fields: SharedLogCoordinateNativeFields<R>;
	deleteHashes: string[];
};

export type NativeBackboneReceiveCoordinateBatch<R extends "u32" | "u64"> = {
	rows: NativeBackboneReceiveCoordinateRow<R>[];
	rollbackCoordinateEntries?: NativeBackboneCoordinateRollback<R>;
};

export type NativeBackboneCoordinateRollback<R extends "u32" | "u64"> = {
	hashes: Set<string>;
	entries: Map<string, ResidentCoordinateEntry<R>>;
	generations: Map<string, number>;
	/**
	 * Set by `settleResidentCoordinateSnapshot` once the token's holds on the
	 * mutation-generation map have been released. Load-bearing: it makes the
	 * settle idempotent, so a second settle of this token cannot consume a
	 * different token's hold on a shared hash.
	 */
	settled?: boolean;
};

export type RepairDispatchEntry<R extends "u32" | "u64"> =
	ResidentCoordinateEntry<R>;

type PreparedLocalAppendCommit<R extends "u32" | "u64"> = {
	hash: string;
	gid: string;
	next: string[];
	wallTime: bigint;
	logical: number;
	clockId?: Uint8Array;
	type?: EntryType;
	metaData?: Uint8Array;
	payloadSize: number;
	entrySize?: number;
	metaBytes?: Uint8Array;
	storageBytes?: Uint8Array;
	hashNumber?: NumberFromType<R>;
	coordinateFields?: SharedLogCoordinateNativeFields<R>;
	nativeBackboneDocumentIndexCommitted?: boolean;
	nativeBackboneDocumentIndexTrimmedHeadsProcessed?: boolean;
	nativeBackboneDocumentDeleteCommitted?: boolean;
	documentPreviousContext?: {
		created: bigint;
		modified: bigint;
		head: string;
		gid: string;
		size: number;
	};
};

type PersistedDeliveryPlanningEntry<T, R extends "u32" | "u64"> =
	| ShallowOrFullEntry<T>
	| EntryReplicated<R>;

type PersistedDeliveryPlanningRecord<T, R extends "u32" | "u64"> = Readonly<{
	canonicalHash: string;
	createDefaultPlanningSource: () => PersistedDeliveryPlanningEntry<T, R>;
	createFullPlanningSource?: () => Entry<T>;
}>;

type PersistedAppendBackfillSource<T, R extends "u32" | "u64"> = {
	entry: Entry<T>;
	coordinates: NumberFromType<R>[];
	assignmentExtraLeaders: LeaderMap;
	deliveryExtraTargets: Set<string>;
	extrasOwnershipRevision?: number;
};

type NativeBackboneSimpleDocumentProjectionPlan = {
	documentVariantType?: "u8" | "string";
	documentVariantValue?: string;
	documentFieldNames: string[];
	documentFieldTypes: string[];
	outputVariantType?: "u8" | "string";
	outputVariantValue?: string;
	outputFieldTypes: string[];
	sourceKinds: string[];
	sourceValues: string[];
};

type NativeBackboneDocumentIndexCommitInput = {
	key: string;
	valuePrefixBytes?: Uint8Array;
	usePlainPutPayload?: boolean;
	projection?: {
		encodedDocument: Uint8Array;
		plan: NativeBackboneSimpleDocumentProjectionPlan;
		signer?: Uint8Array;
	};
	existingCreated?: bigint;
	byteElementIndexLimit?: number;
	deleteTrimmedHeads?: boolean;
	useLatestContext?: boolean;
	requiredPreviousSignerPublicKey?: Uint8Array;
};

type NativeBackboneDocumentRollback = {
	key: string;
	value?: Uint8Array;
	byteElementIndexLimit: number;
};

type NativeBackboneDocumentIndexAppendFacts = {
	wallTime: bigint | number | string;
	gid: string;
	payloadSize: number;
};

type NativeBackboneDocumentIndexPreparer = (
	facts: NativeBackboneDocumentIndexAppendFacts,
) => NativeBackboneDocumentIndexCommitInput | undefined;

type NativeBackboneDocumentCommitOptions = {
	nativeBackboneDocumentIndex?: NativeBackboneDocumentIndexCommitInput;
	prepareNativeBackboneDocumentIndex?: NativeBackboneDocumentIndexPreparer;
	useNativeExistingDocumentContext?: boolean;
	nativeBackboneDocumentDeleteKey?: string;
};

type NativeBackboneCoordinatePersistenceFiles = {
	snapshot?: string;
	journal?: string;
	documentSnapshot?: string;
	documentJournal?: string;
	documentSignerSnapshot?: string;
	documentSignerJournal?: string;
};

type NativeBackboneCoordinatePersistenceOptions =
	NativeBackboneCoordinatePersistenceFiles & {
		flushOnAppend?: boolean;
		flushMaxPendingBytes?: number;
		flushIntervalMs?: number;
		compactMaxJournalBytes?: number;
		compactMaxJournalRecords?: number;
	};

export type NativeBackboneCoordinatePersistenceStore = {
	read(name: string): Promise<Uint8Array | undefined>;
	write(name: string, bytes: Uint8Array): Promise<void>;
	append(name: string, bytes: Uint8Array): Promise<void>;
	remove?(name: string): Promise<void>;
	durableBarrier?(name?: string): Promise<void>;
	supportsRemoval?: boolean;
	flush?(name?: string): Promise<void>;
	close?(options?: { flush?: boolean }): Promise<void>;
};

export type NativeBackboneCoordinatePersistenceAdapter = {
	/** Explicit capability required by durable strict-native operation intents. */
	intentStore?: NativeBackboneCoordinatePersistenceStore;
	flushOnAppend?: boolean;
	flushMaxPendingBytes?: number;
	flushIntervalMs?: number;
	compactMaxJournalBytes?: number;
	compactMaxJournalRecords?: number;
	crashSafeCompaction?: boolean;
	durableBarrier?: boolean;
	supportsDrop?: boolean;
	dropIsTerminal?: boolean;
	hydrate(backbone: unknown): Promise<number>;
	flushJournal(backbone: unknown): Promise<number>;
	flushJournalOnAppend?(backbone: unknown): number | Promise<number>;
	compact?(backbone: unknown): Promise<void>;
	drop?(additionalFiles?: readonly string[]): Promise<void>;
	/**
	 * Resume a failed tombstoned erase. `true` is terminal only when this adapter
	 * initiated the drop; recovery of a prior generation returns active. `false`
	 * restores explicit-drop admission, as must corrupt-marker rejection.
	 */
	resumeDrop?(): Promise<boolean>;
	close?(): Promise<void>;
};

type NativeBackboneCoordinatePersistenceConfig =
	| NativeBackboneCoordinatePersistenceAdapter
	| (NativeBackboneCoordinatePersistenceOptions & {
			store: NativeBackboneCoordinatePersistenceStore;
			buffered?: boolean | { maxBufferedBytes?: number };
	  });

type NativeStrictDurableTransactionIntent = {
	version: 1;
	lowerMarkerCommitted?: boolean;
	appendHashes: string[];
	trimHashes: string[];
	coordinateDeleteHashes?: string[];
	lowerIndexRows: Array<{
		hash: string;
		before?: number[];
		after?: number[];
	}>;
	coordinates: Array<{
		hash: string;
		value?: {
			hashNumber: string;
			gid: string;
			coordinates: string[];
			wallTime: string;
			assignedToRangeBoundary: boolean;
			metaBytes: number[];
		};
	}>;
	documents: Array<{
		key: string;
		value?: number[];
		byteElementIndexLimit: number;
	}>;
};

type NativeStrictDurableTransactionJournalBody = {
	format: "peerbit-native-strict-durable-transaction";
	version: 1;
	sequence: number;
	state: "intent" | "cleared";
	intent: NativeStrictDurableTransactionIntent | null;
};

type NativeStrictDurableTransactionJournalRecord =
	NativeStrictDurableTransactionJournalBody & {
		checksum: string;
	};

type NativeStrictDurableTransactionJournalState = {
	sequence: number;
	slot: 0 | 1;
	intent?: NativeStrictDurableTransactionIntent;
	/** No journal file exists yet; materialize a cleared frame before first use. */
	implicit?: boolean;
};

type NativeStrictDurableTransactionHandle = {
	intent: NativeStrictDurableTransactionIntent;
	release: () => void;
	released: boolean;
	lowerHashMutationLockOwner?: EntryIndexHashMutationLockOwner;
};

const NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILE =
	"strict-durable-transaction-intent.json";
const NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_BACKUP_FILE =
	"strict-durable-transaction-intent.backup.json";
const NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILES = [
	NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILE,
	NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_BACKUP_FILE,
] as const;
const NATIVE_STRICT_DURABLE_TRANSACTION_JOURNAL_FORMAT =
	"peerbit-native-strict-durable-transaction" as const;

const nativeStrictDurableTransactionJournalBody = (
	sequence: number,
	intent: NativeStrictDurableTransactionIntent | undefined,
): NativeStrictDurableTransactionJournalBody => ({
	format: NATIVE_STRICT_DURABLE_TRANSACTION_JOURNAL_FORMAT,
	version: 1,
	sequence,
	state: intent ? "intent" : "cleared",
	intent: intent ?? null,
});

const nativeStrictDurableTransactionJournalBodyBytes = (
	body: NativeStrictDurableTransactionJournalBody,
) => new TextEncoder().encode(JSON.stringify(body));

const nativeStrictDurableTransactionJournalRecordBytes = (
	sequence: number,
	intent: NativeStrictDurableTransactionIntent | undefined,
) => {
	const body = nativeStrictDurableTransactionJournalBody(sequence, intent);
	const record: NativeStrictDurableTransactionJournalRecord = {
		...body,
		checksum: toHexString(
			sha256Sync(nativeStrictDurableTransactionJournalBodyBytes(body)),
		),
	};
	return new TextEncoder().encode(JSON.stringify(record));
};

type TrustedLocalCommitEvidence = {
	committedHashes: Set<string>;
};

type PreparedPayloadCommitOnlyProperties =
	NativeBackboneDocumentCommitOptions & {
		skipMissingNextJoin?: boolean;
		resolveTrimmedEntries?: boolean;
		localCommitEvidence?: TrustedLocalCommitEvidence;
	};

type PreparedPayloadsManyIndependentProperties<T> = {
	resolveTrimmedEntries?: boolean;
	payloadDatas?: Uint8Array[];
	nexts?: ShallowOrFullEntry<T>[][];
	nativeBackboneDocumentIndexes?: NativeBackboneDocumentIndexCommitInput[];
	retainMaterializationBytes?: boolean;
	localCommitEvidence?: TrustedLocalCommitEvidence;
};

type PreparedPayloadCommitOnlyResult<T, R extends "u32" | "u64"> = {
	entry: Entry<T>;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
	removedGids?: string[];
	appendCommit: PreparedLocalAppendCommit<R>;
};

type NativeAppendEntryPlan<R extends "u32" | "u64"> = {
	coordinates: NumberFromType<R>[];
	leaders?: LeaderMap;
	isLeader: boolean;
	assignedToRangeBoundary?: boolean;
	hashNumber: NumberFromType<R>;
	preparedCoordinate: PreparedCoordinatePersistence<R>;
	delivery?: AppendDeliveryPlan;
	committedNativeCoordinateState?: boolean;
	committedNativeBackboneCoordinateState?: boolean;
	committedNativeCoordinateDeletes?: boolean;
};

type NativeFullReplicaCandidateSource = {
	fullReplicaCandidatesFor?: (
		minReplicas: number,
		selfHash: string,
	) => string[];
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
		let entry =
			element instanceof EntryWithRefs ||
			isPreparedRawEntryWithRefs(element as any)
				? (getPreparedRawExchangeHeadShallowEntry(
						element as EntryWithRefs<any>,
					) ?? (element as EntryWithRefs<any>).entry)
				: element;
		if (!latest || compareEntryTimestamp(entry, latest) > 0) {
			latest = entry;
		}
	}
	return latest;
};

const getEntryTimestampParts = (entry: ShallowOrFullEntry<any>) => {
	if (entry instanceof Entry) {
		const rawTimestamp = getPreparedRawExchangeTimestamp(entry);
		if (rawTimestamp) {
			return rawTimestamp;
		}
	}
	return {
		wallTime: entry.meta.clock.timestamp.wallTime,
		logical: entry.meta.clock.timestamp.logical,
	};
};

const compareEntryTimestamp = (
	a: ShallowOrFullEntry<any>,
	b: ShallowOrFullEntry<any>,
) => {
	const aTimestamp = getEntryTimestampParts(a);
	const bTimestamp = getEntryTimestampParts(b);
	if (aTimestamp.wallTime > bTimestamp.wallTime) {
		return 1;
	}
	if (aTimestamp.wallTime < bTimestamp.wallTime) {
		return -1;
	}
	if (aTimestamp.logical > bTimestamp.logical) {
		return 1;
	}
	if (aTimestamp.logical < bTimestamp.logical) {
		return -1;
	}
	return 0;
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

const pickDeterministicSubset = (
	peers: string[],
	seed: number,
	max: number,
) => {
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
		/**
		 * Soft byte objective for the adaptive PID controller. This is not a hard
		 * quota: coverage pressure, discrete entries, and delayed pruning can make
		 * actual local storage exceed this value.
		 */
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

/** Opt-in proof that a local replication-role update applied remotely. */
export type ReplicationConfirmationOptions =
	ReplicationInfoV2ConfirmationOptions;

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

export interface IndexableDomain<R extends "u32" | "u64"> {
	numbers: Numbers<R>;
	constructorEntry: new (properties: {
		coordinates: NumberFromType<R>[];
		hash: string;
		meta?: Meta | ShallowMeta;
		metaBytes?: Uint8Array;
		gid?: string;
		wallTime?: bigint;
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

export type PutAndDeleteIndex<T extends Record<string, any>> = Index<T> & {
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
	delIdsNoReturn?: (
		deleteIds: Array<IdKey | Ideable>,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateAndDeleteIds?: (
		value: T,
		fields: SharedLogCoordinateNativeFields<any>,
		deleteIds?: Array<IdKey | Ideable>,
		id?: IdKey,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsAndDeleteIds?: (
		fields: SharedLogCoordinateNativeFields<any>,
		deleteIds?: Array<IdKey | Ideable>,
		id?: IdKey,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsAndDeleteHashes?: (
		fields: SharedLogCoordinateNativeFields<any>,
		deleteHashes?: string[],
		id?: IdKey,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsAndDeleteHashesNoReturn?: (
		fields: SharedLogCoordinateNativeFields<any>,
		deleteHashes?: string[],
		id?: IdKey,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn?: (
		fields: SharedLogCoordinateNativeFields<any>,
		deleteHashes?: string[],
		id?: IdKey,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinatesAndDeleteIdsBatch?: (
		values: Array<{
			value: T;
			fields: SharedLogCoordinateNativeFields<any>;
			deleteIds?: Array<IdKey | Ideable>;
			id?: IdKey;
		}>,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsAndDeleteIdsBatch?: (
		values: Array<{
			fields: SharedLogCoordinateNativeFields<any>;
			deleteIds?: Array<IdKey | Ideable>;
			id?: IdKey;
		}>,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsAndDeleteHashesBatch?: (
		values: Array<{
			fields: SharedLogCoordinateNativeFields<any>;
			deleteHashes?: string[];
			id?: IdKey;
		}>,
	) => Promise<unknown> | unknown;
	putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn?: (
		values: Array<{
			fields: SharedLogCoordinateNativeFields<any>;
			deleteHashes?: string[];
			id?: IdKey;
		}>,
	) => Promise<unknown> | unknown;
};

export type EntryWithMetaBytes = {
	getMetaBytes?: () => Uint8Array | undefined;
	getHashDigestBytes?: () => Uint8Array | undefined;
};

const createIndexableDomainFromResolution = <R extends "u32" | "u64">(
	resolution: R,
): IndexableDomain<R> => {
	if (resolution === "u32") {
		return {
			constructorEntry: EntryReplicatedU32,
			constructorRange: ReplicationRangeIndexableU32,
			numbers: createNumbers(resolution),
		} as any as IndexableDomain<R>;
	} else if (resolution === "u64") {
		return {
			constructorEntry: EntryReplicatedU64,
			constructorRange: ReplicationRangeIndexableU64,
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
	nativeBackbone?:
		| false
		| {
				optional?: boolean;
				heads?: boolean;
				documentIndex?: boolean;
				coordinatePersistence?: NativeBackboneCoordinatePersistenceConfig;
		  };
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
	domain?: ReplicationDomainConstructor<D>;
	eagerBlocks?: EagerBlocksSetting;
	fanout?: SharedLogFanoutOptions;
};

/**
 * Runtime defaults a client can advertise for shared-log programs opened on
 * it. The historical name is retained because the peerbit native network
 * preset introduced this hook. Defaults fill in open options the caller left
 * undefined; explicit per-open options (including `false`) always win.
 * Without the property on the client, behavior is unchanged.
 */
export type SharedLogNativeDefaults = {
	nativeBackbone?: SharedLogOptions<any, any, any>["nativeBackbone"];
	nativeGraph?: LogProperties<any>["nativeGraph"];
	sync?: Pick<SyncOptions<any>, "rawExchangeHeads" | "nativeWireSync">;
	/**
	 * Per-channel defaults applied only when the caller opts into SharedLog
	 * fanout. Explicit per-open channel options take precedence.
	 */
	fanout?: Pick<SharedLogFanoutOptions, "channel">;
};

type NodeWithSharedLogNativeDefaults = {
	sharedLogNativeDefaults?: SharedLogNativeDefaults;
};

type SharedLogFanoutChannelOptions = NonNullable<
	SharedLogFanoutOptions["channel"]
>;

const mergeDefinedFanoutChannelOptions = (
	...sources: Array<SharedLogFanoutChannelOptions | undefined>
): SharedLogFanoutChannelOptions | undefined => {
	let merged: Record<string, unknown> | undefined;
	for (const source of sources) {
		if (!source) {
			continue;
		}
		for (const [key, value] of Object.entries(source)) {
			if (value === undefined) {
				continue;
			}
			merged ??= {};
			merged[key] = value;
		}
	}
	return merged as SharedLogFanoutChannelOptions | undefined;
};

const applySharedLogNativeDefaults = <
	O extends {
		nativeBackbone?: SharedLogOptions<any, any, any>["nativeBackbone"];
		nativeGraph?: LogProperties<any>["nativeGraph"];
		sync?: SyncOptions<any>;
		fanout?: SharedLogFanoutOptions;
	},
>(
	options: O | undefined,
	defaults: SharedLogNativeDefaults | undefined,
): O | undefined => {
	if (!defaults) {
		return options;
	}
	const sync =
		defaults.sync || options?.sync
			? {
					...options?.sync,
					rawExchangeHeads:
						options?.sync?.rawExchangeHeads ?? defaults.sync?.rawExchangeHeads,
					nativeWireSync:
						options?.sync?.nativeWireSync ?? defaults.sync?.nativeWireSync,
				}
			: undefined;
	const fanout = options?.fanout
		? {
				...options.fanout,
				channel: mergeDefinedFanoutChannelOptions(
					defaults.fanout?.channel,
					options.fanout.channel,
				),
			}
		: undefined;
	return {
		...options,
		nativeBackbone: options?.nativeBackbone ?? defaults.nativeBackbone,
		nativeGraph: options?.nativeGraph ?? defaults.nativeGraph,
		sync,
		fanout,
	} as O;
};

export const DEFAULT_MIN_REPLICAS = 2;
export const WAIT_FOR_REPLICATOR_TIMEOUT = 20000;
export const WAIT_FOR_ROLE_MATURITY = 5000;
export const WAIT_FOR_REPLICATOR_REQUEST_INTERVAL = 1000;
export const WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS = 3;
// The V2 recovery scheduler is deliberately persistent (a subscribed peer is
// re-solicited for as long as its topic session stays open), but consecutive
// fruitless park/unpark cycles double the wait before the next unpark so a
// silent-but-subscribed peer converges to one bounded request cycle per cap
// window instead of one per base interval. Any applied V2 progress resets it.
export const REPLICATION_INFO_V2_RECOVERY_MAX_UNPARK_DELAY = 300_000;
const REPLICATION_INFO_V2_RECOVERY_MAX_UNPARK_EXPONENT = 20;
// TODO(prune): Investigate if/when a non-zero prune delay is required for correctness
// (e.g. responsibility/replication-info message reordering in multi-peer scenarios).
// Prefer making pruning robust without timing-based heuristics.
export const WAIT_FOR_PRUNE_DELAY = 0;
const PRUNE_DEBOUNCE_INTERVAL = 500;
const CHECKED_PRUNE_RESEND_INTERVAL_MIN_MS = 250;
const CHECKED_PRUNE_RESEND_INTERVAL_MAX_MS = 5_000;
const CHECKED_PRUNE_BACKGROUND_TIMEOUT_MIN_MS = 120_000;
const CHECKED_PRUNE_RETRY_MAX_ATTEMPTS = 3;
const CHECKED_PRUNE_RETRY_MAX_DELAY_MS = 30_000;
const CHECKED_PRUNE_AUDIT_INTERVAL_MS = 30_000;
const CHECKED_PRUNE_AUDIT_BATCH_SIZE = 128;

// DONT SET THIS ANY LOWER, because it will make the pid controller unstable as the system responses are not fast enough to updates from the pid controller
const RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL = 1000;
// Index backends flatten logical queries before execution and have practical
// expression limits well below a large local range set. Keep exact range
// lookups/deletes bounded while the mutation lane preserves operation ordering.
const REPLICATION_RANGE_ID_QUERY_BATCH_SIZE = 100;
// A normal peer owns far fewer ranges. This intentionally generous ceiling keeps
// decoded, untrusted announcements from forcing unbounded conversion/query work
// without changing the wire schema or constraining ordinary replication plans.
const MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT = 4096;
const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE = 0.01;
const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_CPU_LIMIT = 0.005;
const RECALCULATE_PARTICIPATION_MIN_RELATIVE_CHANGE_WITH_MEMORY_LIMIT = 0.001;
const RECALCULATE_PARTICIPATION_RELATIVE_DENOMINATOR_FLOOR = 1e-3;
const TOPIC_SUBSCRIBERS_CACHE_TTL_MS = 250;
const LOCAL_REACHABLE_PEERS_CACHE_TTL_MS = 250;
const LOCAL_REACHABLE_PEERS_MAX = 64;
const LEADER_SELECTION_CONTEXT_CACHE_TTL_MS = 50;
// Leader-plan results are invalidated structurally (replication:change,
// replicator:mature, subscriber changes). The TTL bounds wall-clock maturity
// flips that have no timer at all — a range re-crossing maturity after the
// dynamic default roleAge regrew is only visible via expiry — so it is
// load-bearing for that edge and must stay at the same order as the
// leader-selection context TTL.
const LEADER_PLAN_CACHE_TTL_MS = 50;
const LEADER_PLAN_CACHE_MAX = 10_000;
const ADAPTIVE_REBALANCE_IDLE_INTERVAL_MULTIPLIER = 5;
const ADAPTIVE_REBALANCE_MIN_IDLE_AFTER_LOCAL_APPEND_MS = 10_000;

// Live raw gossip micro-batching. Appended entries destined for the same
// raw-capable recipient set coalesce until the end of the current event-loop
// turn (queueMicrotask would only merge same-tick appends; a macrotask also
// merges the awaited-put pattern where each append resolves through
// microtasks) — so a lone put still flushes within one loop turn (sub-ms on
// an idle loop) while a put burst ships as one multi-entry raw frame,
// amortizing the receiver's per-message fixed costs. The entry/byte caps
// bound the worst-case receiver stall per frame and keep frames within the
// raw exchange message size the receive path is tuned for.
const LIVE_RAW_GOSSIP_MAX_ENTRIES = 256;
const LIVE_RAW_GOSSIP_MAX_BYTES = 128 * 1024;
const GID_PEER_HISTORY_CLEANUP_BATCH_SIZE = 256;
const GID_PEER_HISTORY_CLEANUP_PENDING_CAPACITY = 4096;

type LiveRawGossipBatch = {
	to: string[];
	hashes: string[];
	gidRefrences: string[][];
	bytes: number;
};

const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;
const RECENT_REPAIR_DISPATCH_TTL_MS = 5_000;
const REPAIR_SWEEP_ENTRY_BATCH_SIZE = 1_000;
const REPAIR_SWEEP_TARGET_BUFFER_SIZE = 1024;
const NATIVE_ED25519_VERIFY_BATCH_MIN_ENTRIES = 16;
const hasPreverifiedSignature = (entry: Entry<any>) =>
	(entry as { __peerbitSignatureVerified?: unknown })
		.__peerbitSignatureVerified === true;

// Churn/join repair can race with pruning and transient missed sync requests under
// heavy event-loop load. Keep retries alive with a longer tail so reassigned
// entries are retried after short bursts and slower recovery windows.
const CHURN_REPAIR_RETRY_SCHEDULE_MS = [
	0, 1_000, 3_000, 7_000, 15_000, 30_000, 45_000,
];
// Preserve the bounded retry window for transient local misses, but serialize
// delayed warmup sends per target so fixed snapshots cannot overlap and amplify
// large transfers. Every queued pass re-checks current peer knowledge on entry.
const JOIN_WARMUP_RETRY_SCHEDULE_MS = [
	0, 1_000, 3_000, 7_000, 15_000, 30_000, 60_000,
];
const JOIN_AUTHORITATIVE_RETRY_SCHEDULE_MS = [
	0, 1_000, 3_000, 7_000, 15_000, 30_000, 60_000,
];
const APPEND_BACKFILL_RETRY_SCHEDULE_MS = [0, 1_000, 3_000, 7_000];
const RECENT_KNOWN_REPAIR_SUPPRESSION_MS = 30_000;
// `_entryKnownPeerObservedAt` is read ONLY through isEntryRecentlyKnownByPeer,
// which treats an over-age row and an absent row identically (both false). So
// rows older than the longest horizon any caller asks about are dead weight,
// and dropping them is behaviour-identical rather than merely safe. Derived
// from the horizons themselves -- never hardcode it -- so a future caller with
// a longer window cannot silently outlive the retention that serves it.
const ENTRY_KNOWN_PEER_OBSERVED_AT_RETENTION_MS = Math.max(
	RECENT_KNOWN_REPAIR_SUPPRESSION_MS,
	RECENT_KNOWN_EXCHANGE_HEAD_SUPPRESSION_MS,
);
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

type RepairSweepOptimisticPeerState = {
	count: number;
	session: WarmupSession;
};

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
		REPAIR_DISPATCH_MODES.map((mode) => [
			mode,
			new Set(pending.get(mode) ?? []),
		]),
	);

const createRepairFrontierByMode = () =>
	new Map<
		RepairDispatchMode,
		Map<string, Map<string, RepairDispatchEntry<any>>>
	>(REPAIR_DISPATCH_MODES.map((mode) => [mode, new Map()]));

const createRepairActiveTargetsByMode = () =>
	new Map<RepairDispatchMode, Map<string, object>>(
		REPAIR_DISPATCH_MODES.map((mode) => [mode, new Map()]),
	);

const createRepairFrontierBypassKnownPeersByMode = () =>
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

/**
 * `persisted` waits, after the local commit, for `minAcks` distinct remote
 * leaders that advertised crash-safe receipt support. Each receipt proves that
 * the exact block, lower-log row, and replication-coordinate row crossed that
 * peer's storage barriers at the receipt instant. It is cooperative-peer
 * evidence, not a Byzantine proof or a promise that the peer will retain the
 * entry forever. Only current capable leaders count.
 */
export type DeliveryReliability = "ack" | "best-effort" | "persisted";

export type DeliveryOptions = {
	reliability?: DeliveryReliability;
	/**
	 * Required for persisted delivery; counts distinct current remote leaders.
	 * This does not increase the entry's replication degree, so the configured
	 * replication must make at least this many capable remote leaders eligible.
	 */
	minAcks?: number;
	requireRecipients?: boolean;
	/**
	 * Transport priority for directed RPC delivery. Fanout unicast already uses
	 * its control lane, so this only changes the direct/fallback RPC path.
	 */
	priority?: number;
	/**
	 * Overall delivery deadline in milliseconds. For persisted delivery it
	 * starts after the local append has returned and includes leader planning,
	 * transfer, receipt requests, and final ownership/session validation. The
	 * omitted persisted default is 10 seconds plus one admission-attempt budget
	 * per transfer chunk and the minimum receipt sender-pacing time implied by
	 * the batch size. An explicit timeout remains exact.
	 */
	timeout?: number;
	signal?: AbortSignal;
};

export type PersistedReceiptPeerReadinessPendingReason =
	| "closed"
	| "no-current-session"
	| "session-opening"
	| "capability-pending"
	| "replication-state-pending"
	| "replication-confirmation-pending"
	| "not-replicating"
	| "not-entry-leader"
	| "ownership-changing";

export type PersistedReceiptPeerReadinessUnsupportedReason =
	| "persisted-receipts-unsupported"
	| "replication-confirmation-unsupported";

/**
 * Detached view of one public key's current persisted-receipt generation.
 * `generation` is opaque: callers may compare it for equality, but must not
 * interpret its contents or use it as a future-session capability. Equal
 * generations mean the connection/receive/capability binding is unchanged;
 * leadership and outbound confirmation can still change within a generation.
 */
export type PersistedReceiptPeerReadiness =
	| Readonly<{
			status: "ready";
			generation: string;
	  }>
	| Readonly<{
			status: "pending";
			reason: PersistedReceiptPeerReadinessPendingReason;
			generation?: string;
	  }>
	| Readonly<{
			status: "unsupported";
			reason: PersistedReceiptPeerReadinessUnsupportedReason;
			generation: string;
	  }>;

export type PersistedReceiptPeerReady = Extract<
	PersistedReceiptPeerReadiness,
	{ status: "ready" }
>;

export type PersistedReceiptPeerReadinessOptions<
	T,
	R extends "u32" | "u64",
> = Readonly<{
	/** Require this peer to be a freshly planned leader for every entry. */
	entries?: readonly (ShallowOrFullEntry<T> | EntryReplicated<R>)[];
	/**
	 * Total leader-plan replica degree used for `entries`; defaults to this log's
	 * configured minimum. This is not the persisted delivery `minAcks` count.
	 */
	replicas?: number;
}>;

export type WaitForPersistedReceiptPeerReadinessOptions<
	T,
	R extends "u32" | "u64",
> = PersistedReceiptPeerReadinessOptions<T, R> &
	Readonly<{
		timeout?: number;
		signal?: AbortSignal;
	}>;

type PersistedDeliveryOptions = Readonly<{
	reliability: "persisted";
	minAcks: number;
	requireRecipients?: boolean;
	priority?: number;
	timeout?: number;
	signal?: AbortSignal;
}>;

type PersistedAppendInvocation<T> = {
	options: SharedAppendOptions<T>;
	delivery: PersistedDeliveryOptions;
};

type CrashSafeStorageBarrier = {
	readonly crashSafe: true;
	barrier(): MaybePromise<void>;
};

type PersistedReceiptStorage = {
	block: CrashSafeStorageBarrier;
	lower: CrashSafeStorageBarrier;
	coordinate: CrashSafeStorageBarrier;
};

type PersistedDeliveryDeadline = {
	deadline: number;
	signal: AbortSignal;
	dispose(): void;
};

const MAX_PERSISTED_RECEIPT_HASHES = 1_024;
const MAX_PERSISTED_RECEIPT_HASH_BYTES = 128 * 1_024;
const PERSISTED_RECEIPT_CHUNK_SIZE = 512;
const PERSISTED_TRANSFER_CHUNK_SIZE = 256;
const DEFAULT_PERSISTED_RECEIPT_TIMEOUT_MS = 10_000;
const MAX_PERSISTED_DELIVERY_TIMEOUT_MS = 2_147_483_647;
const PERSISTED_RECEIPT_RETRY_MS = 50;
const MAX_PERSISTED_RECEIPT_ATTEMPT_MS = 2_000;
const MAX_PERSISTED_RECEIPT_REQUESTS_GLOBAL = 8;
const MAX_PERSISTED_RECEIPT_REQUESTS_PER_PEER = 2;
const MAX_PERSISTED_RECEIPT_READINESS_WAITERS = 1_024;
const PERSISTED_RECEIPT_INGRESS_PEER_REQUEST_CAPACITY = 16;
const PERSISTED_RECEIPT_INGRESS_PEER_HASH_CAPACITY = 8_192;
const PERSISTED_RECEIPT_INGRESS_PEER_REQUESTS_PER_SECOND = 8;
const PERSISTED_RECEIPT_INGRESS_PEER_HASHES_PER_SECOND = 4_096;
const PERSISTED_RECEIPT_INGRESS_NODE_REQUEST_CAPACITY = 32;
const PERSISTED_RECEIPT_INGRESS_NODE_HASH_CAPACITY = 16_384;
const PERSISTED_RECEIPT_INGRESS_NODE_REQUESTS_PER_SECOND = 16;
const PERSISTED_RECEIPT_INGRESS_NODE_HASHES_PER_SECOND = 8_192;
const MAX_PERSISTED_RECEIPT_INGRESS_PEER_SESSIONS = 256;

type PersistedReceiptIngressBucket = {
	requestTokens: number;
	hashTokens: number;
	refilledAt: number;
};

type PersistedReceiptNodeIngressBudget = PersistedReceiptIngressBucket & {
	peerSessions: Map<string, PersistedReceiptIngressBucket>;
};

type PersistedReceiptNodeEgressBudget = {
	peerSessions: Map<string, PersistedReceiptIngressBucket>;
};

const persistedReceiptIngressBudgets = new WeakMap<
	object,
	PersistedReceiptNodeIngressBudget
>();

// Sender pacing mirrors the receiver's per-peer/session allowance. Keeping it
// on the Peerbit node (rather than one SharedLog) prevents independent programs
// from silently overrunning the same remote receiver together.
const persistedReceiptEgressBudgets = new WeakMap<
	object,
	PersistedReceiptNodeEgressBudget
>();

const refillPersistedReceiptIngressBucket = (
	bucket: PersistedReceiptIngressBucket,
	now: number,
	requestCapacity: number,
	hashCapacity: number,
	requestsPerSecond: number,
	hashesPerSecond: number,
) => {
	// Never move the refill watermark backwards. Date.now() can jump after a
	// clock correction; accepting that earlier timestamp would grant the same
	// elapsed interval again on the next request.
	if (now <= bucket.refilledAt) {
		return;
	}
	const elapsedSeconds = Math.max(0, now - bucket.refilledAt) / 1_000;
	if (elapsedSeconds > 0) {
		bucket.requestTokens = Math.min(
			requestCapacity,
			bucket.requestTokens + elapsedSeconds * requestsPerSecond,
		);
		bucket.hashTokens = Math.min(
			hashCapacity,
			bucket.hashTokens + elapsedSeconds * hashesPerSecond,
		);
	}
	bucket.refilledAt = now;
};

const persistedReceiptPacingFloorMs = (hashCount: number): number => {
	const boundedHashCount = Math.max(0, Math.floor(hashCount));
	const requestCount = Math.ceil(
		boundedHashCount / PERSISTED_RECEIPT_CHUNK_SIZE,
	);
	return Math.max(
		0,
		((requestCount - PERSISTED_RECEIPT_INGRESS_PEER_REQUEST_CAPACITY) /
			PERSISTED_RECEIPT_INGRESS_PEER_REQUESTS_PER_SECOND) *
			1_000,
		((boundedHashCount - PERSISTED_RECEIPT_INGRESS_PEER_HASH_CAPACITY) /
			PERSISTED_RECEIPT_INGRESS_PEER_HASHES_PER_SECOND) *
			1_000,
	);
};

const persistedTransferAdmissionBudgetMs = (hashCount: number): number =>
	Math.ceil(
		Math.max(0, Math.floor(hashCount)) / PERSISTED_TRANSFER_CHUNK_SIZE,
	) * MAX_PERSISTED_RECEIPT_ATTEMPT_MS;

export type SharedLogFanoutOptions = {
	root?: string;
	channel?: Partial<Omit<FanoutTreeChannelOptions, "role">>;
	join?: FanoutTreeJoinOptions;
};

type SharedAppendBaseOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
	replicate?: boolean;
};

type TrustedLogAppendOptions<T> = AppendOptions<T> & {
	__peerbitCanAppendAlreadyValidated?: boolean;
	__peerbitOnLocalCommit?: (
		hashes: readonly string[],
		entries?: readonly Entry<T>[],
	) => void;
};

const attachTrustedLocalCommitEvidence = <T>(
	options: AppendOptions<T>,
	evidence: TrustedLocalCommitEvidence | undefined,
): void => {
	if (!evidence) return;
	(options as TrustedLogAppendOptions<T>).__peerbitOnLocalCommit = (hashes) => {
		for (const hash of hashes) {
			evidence.committedHashes.add(hash);
		}
	};
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

type DocumentSharedAppendOptions<T> = SharedAppendOptions<T> & {
	unique?: boolean;
	checkRemote?: boolean;
};

type TrustedLowerLogAppendHashesSink = (
	hashes: string[],
) => void | Promise<void>;

type TrustedLowerLogJoinOptions<T> = Parameters<Log<T>["join"]>[1] & {
	__peerbitBatchIndependent?: boolean;
	__peerbitEntriesAlreadyMissing?: boolean;
	__peerbitCanAppendAlreadyValidated?: boolean;
	__peerbitOnAppendHashes?: TrustedLowerLogAppendHashesSink;
	__peerbitDeferIndexWrite?: boolean;
	__peerbitProfile?: SyncProfileFn;
};

type TrustedLowerLogPreparedJoinCommitInput = {
	entries: PreparedAppendJoinFacts[];
	hashes: string[];
	headFlags: boolean[];
	headFlagsBytes: Uint8Array;
	trustedMissing: boolean;
	validatePlan?: boolean;
};

type TrustedLowerLogPreparedJoinCommittedInput = {
	entries: PreparedAppendJoinFacts[];
	hashes: string[];
	headFlags: boolean[];
	nativePreparedCommitted: boolean;
};

type TrustedLowerLogPreparedJoinOptions = {
	__peerbitEntriesAlreadyMissing?: boolean;
	__peerbitCanAppendAlreadyValidated?: boolean;
	__peerbitOnAppendHashes?: TrustedLowerLogAppendHashesSink;
	__peerbitDeferIndexWrite?: boolean;
	__peerbitProfile?: SyncProfileFn;
	__peerbitNativePreparedJoinCommit?: (
		input: TrustedLowerLogPreparedJoinCommitInput,
	) => Promise<boolean> | boolean;
	__peerbitNativePreparedJoinCommitValidatesPlan?: boolean;
	__peerbitOnPreparedJoinCommitted?: (
		input: TrustedLowerLogPreparedJoinCommittedInput,
	) => Promise<void> | void;
};

type TrustedLowerLogNativeCommitInput = {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	wallTime: bigint;
	logical: number;
	gid: string;
	type: EntryType;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
	next?: string[];
	resolveTrimmedEntries?: boolean;
	trimLengthTo?: number;
};

type TrustedLowerLogNativePreparedCommit = {
	bytes?: Uint8Array;
	getBytes?: (hash: string) => Uint8Array | undefined;
	cid?: string;
	hash?: string;
	gid?: string;
	next?: string[];
	byteLength: number;
	metaBytes?: Uint8Array;
	hashDigestBytes?: Uint8Array;
	trimmedEntries?: unknown[];
	trimmedEntryHashes?: string[];
	trimmedEntryGids?: string[];
	nativeBlocksDeleted?: boolean;
	nativeDeleteCleanupToken?: unknown;
	nativeCommitOwnershipToken?: unknown;
	nativeIndexMutationLockOwner?: EntryIndexHashMutationLockOwner;
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: PreparedLocalAppendCommit<"u32">["documentPreviousContext"];
};

type TrustedLowerLogPreparedAppendResult<T> = {
	entry: Entry<T>;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
	removedGids?: string[];
	change: Change<T>;
	appendFacts: PreparedAppendFacts;
};

type TrustedLowerLogCommitOnlyAppendResult<T> = {
	entry: Entry<T>;
	materializeEntry: () => Entry<T>;
	shallowEntry: ShallowEntry;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
	removedGids?: string[];
	appendFacts: PreparedAppendFacts;
	documentTrimmedHeadsProcessed?: boolean;
	documentPreviousContext?: PreparedLocalAppendCommit<"u32">["documentPreviousContext"];
	nativeCommittedAppendFinalizer?: TrustedLowerLogNativeCommitFinalizer;
};

type TrustedLowerLogCommitOnlyAppendBatchResult<T> = {
	entries: Entry<T>[];
	materializeEntries: Array<() => Entry<T>>;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
	removedGids?: string[];
	appendFacts: PreparedAppendFacts[];
	documentTrimmedHeadsProcessed?: boolean[];
	nativeCommittedAppendFinalizer?: TrustedLowerLogNativeCommitFinalizer;
};

type TrustedLowerLogNativeCommitFinalizer = {
	acknowledge(onLowerMarkerDurable?: () => Promise<void>): Promise<void>;
	retainForRecovery(): void;
	rollback(): Promise<void>;
};

type TrustedLowerLog<T> = {
	snapshotAppendEncryptionForTrustedCaller(
		encryption: NonNullable<AppendOptions<T>["encryption"]>,
	): NonNullable<AppendOptions<T>["encryption"]>;
	appendLocallyPrepared(
		data: T,
		options?: TrustedLogAppendOptions<T>,
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
	): Promise<TrustedLowerLogPreparedAppendResult<T>>;
	appendLocallyPreparedCommitOnly(
		data: T,
		options?: TrustedLogAppendOptions<T>,
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
		},
	): MaybePromise<TrustedLowerLogCommitOnlyAppendResult<T> | undefined>;
	appendLocallyPreparedNativeNoNextCommitOnly(
		data: T,
		options: TrustedLogAppendOptions<T> | undefined,
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: TrustedLowerLogNativeCommitInput,
		) => MaybePromise<TrustedLowerLogNativePreparedCommit | undefined>,
	): MaybePromise<TrustedLowerLogCommitOnlyAppendResult<T> | undefined>;
	appendLocallyPreparedNativeKnownNoNextCommitOnly(
		data: T,
		options: TrustedLogAppendOptions<T> | undefined,
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: TrustedLowerLogNativeCommitInput,
		) => MaybePromise<TrustedLowerLogNativePreparedCommit | undefined>,
	): MaybePromise<TrustedLowerLogCommitOnlyAppendResult<T> | undefined>;
	appendLocallyPreparedNativeCommitOnly(
		data: T,
		options: TrustedLogAppendOptions<T> | undefined,
		properties: {
			payloadData?: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			knownNoNext?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			input: TrustedLowerLogNativeCommitInput,
		) => MaybePromise<TrustedLowerLogNativePreparedCommit | undefined>,
		knownNoNext?: boolean,
	): MaybePromise<TrustedLowerLogCommitOnlyAppendResult<T> | undefined>;
	appendLocallyPreparedManyIndependent(
		data: T[],
		options?: TrustedLogAppendOptions<T>,
		properties?: {
			resolveTrimmedEntries?: boolean;
			payloadDatas?: Uint8Array[];
			nexts?: ShallowOrFullEntry<T>[][];
		},
	): Promise<
		| {
				entries: Entry<T>[];
				removed: ShallowOrFullEntry<T>[];
				change: Change<T>;
				appendFacts: PreparedAppendFacts[];
		  }
		| undefined
	>;
	appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch(
		data: T[],
		options: TrustedLogAppendOptions<T> | undefined,
		properties: {
			payloadDatas: Uint8Array[];
			resolveTrimmedEntries?: boolean;
			allowPreparedNexts?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		},
		prepare: (
			inputs: TrustedLowerLogNativeCommitInput[],
		) => MaybePromise<
			Array<TrustedLowerLogNativePreparedCommit | undefined> | undefined
		>,
	): MaybePromise<TrustedLowerLogCommitOnlyAppendBatchResult<T> | undefined>;
	join(
		entriesOrLog: Parameters<Log<T>["join"]>[0],
		options?: TrustedLowerLogJoinOptions<T>,
	): Promise<void>;
	joinPreparedAppendFactsBatch(
		entries: PreparedAppendJoinFacts[],
		options?: TrustedLowerLogPreparedJoinOptions,
	): Promise<boolean>;
};

const asTrustedLowerLog = <T>(log: Log<T>): TrustedLowerLog<T> =>
	log as unknown as TrustedLowerLog<T>;

export type ReplicatorJoinEvent = { publicKey: PublicSignKey };
export type ReplicatorLeaveEvent = { publicKey: PublicSignKey };
export type ReplicationChangeEvent = { publicKey: PublicSignKey };
export type ReplicatorMatureEvent = { publicKey: PublicSignKey };
export type ReplicationStatusEvent = ReplicationStatus;
/** `peerHash` is the result of `PublicSignKey.hashcode()`. */
export type PersistedReceiptPeerReadinessEvent = Readonly<{
	peerHash: string;
}>;

class ReplicationStatusSnapshotChangedError extends Error {
	constructor() {
		super("Replication status changed while it was being measured");
		this.name = "ReplicationStatusSnapshotChangedError";
	}
}

type LeaderSelectionContext = {
	roleAge: number;
	selfHash: string;
	selfReplicating: boolean;
	peerFilter: Set<string> | undefined;
	peerFilterArray: string[] | undefined;
};

export interface SharedLogEvents extends ProgramEvents {
	"replicator:join": CustomEvent<ReplicatorJoinEvent>;
	"replicator:leave": CustomEvent<ReplicatorLeaveEvent>;
	"replication:change": CustomEvent<ReplicationChangeEvent>;
	"replicator:mature": CustomEvent<ReplicatorMatureEvent>;
	"replication:status": CustomEvent<ReplicationStatusEvent>;
	/**
	 * Non-exhaustive wake hint that a peer may now produce a new readiness
	 * snapshot. Consumers must re-read the snapshot; this event is deliberately
	 * not a durable transition log and a `ready` result remains advisory.
	 */
	"persisted-receipt:readiness": CustomEvent<PersistedReceiptPeerReadinessEvent>;
}

export type SharedLogRuntimeSnapshot = Readonly<{
	nativeGraph: Readonly<{
		active: boolean;
		useHeads: boolean;
	}>;
}>;

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
	private _replicationStatus?: ReplicationStatus;
	private _replicationStatusReadTail!: Promise<void>;
	private _replicationStatusRefreshScheduled!: boolean;
	private _replicationStatusRefreshDirty!: boolean;

	private _replicationRangeIndex!: Index<ReplicationRangeIndexable<R>>;
	private _entryCoordinatesIndex!: Index<EntryReplicated<R>>;
	private _nativeRangePlanner?: SharedLogRangePlanner;
	private _nativeSharedLogState?: SharedLogNativeState;
	private _nativeBackbone?: NativePeerbitBackbone;
	private _nativeDurableCommitFailure?: NativeDurableCommitError;
	private _nativeDurableRecoveryReadyForReopen = false;
	private _nativeDurableRecoveryCids = new Set<string>();
	private _wireSyncSession?: SharedLogNativeWireSync;
	private _nativeBackboneCoordinatePersistenceStore?: NativeBackboneCoordinatePersistenceStore;
	private _nativeBackboneDropStarted = false;
	private _nativeStrictDurableTransactionTail?: Promise<void>;
	private _nativeStrictDurableTransactions?: Set<NativeStrictDurableTransactionHandle>;
	private _nativeStrictDurableTransactionJournalState?: NativeStrictDurableTransactionJournalState;
	private _nativeStrictDurableDocumentRecoveryDeferred = false;
	private _nativeStrictDurableTransactionsClosing = false;
	private _nativeStrictDurableTransactionFailure?: unknown;
	private _defaultAppendReplicaMetadataCache?: {
		source: MinReplicas;
		value: number;
		bytes: Uint8Array;
	};
	// Stage 4.5: the four coordinate state fields
	// (_residentEntryCoordinatesByHash, _nativeBackboneCoordinatePersistence,
	// _nativeCoordinateMutationGenerations,
	// _nativeBackboneCoordinateJournalLastFlushMs) are physically owned by the
	// CoordinatePersistenceCoordinator under their historical names (the
	// _nativeCoordinateMutationGenerations ratchet entry moved file-to-file —
	// see src/coordinate-persistence.ts). Constructed by
	// ensureNativeDurabilityRuntimeState (constructor + every clone-hydration
	// site where the legacy `??=` defaults ran); all internal readers use
	// direct property hops (`this._coordinates.<field>`), never the remaining
	// compatibility state accessors below.
	// Typed `<any>` deliberately: the coordinator's deps closures mention R in
	// invariant positions, and an R-typed field here would make SharedLog
	// instantiations invariant in R (breaking the long-standing
	// `EventStore<string, any>` assignability tests rely on). The remaining
	// compatibility state accessors below re-state the precise R types at their
	// boundaries, so host-side inference is unchanged.
	private _coordinates!: CoordinatePersistenceCoordinator<any>;
	// Test-visible compat accessors under the historical field names. The
	// getters mirror the legacy never-opened-clone reads (`undefined` before
	// the coordinator exists); the setters hydrate the coordinator exactly
	// like the legacy field writes did.
	get _residentEntryCoordinatesByHash():
		| Map<string, ResidentCoordinateEntry<R>>
		| undefined {
		return this._coordinates?._residentEntryCoordinatesByHash;
	}

	set _residentEntryCoordinatesByHash(
		value: Map<string, ResidentCoordinateEntry<R>> | undefined,
	) {
		(this._coordinates ??=
			this.createCoordinatePersistenceCoordinator())._residentEntryCoordinatesByHash =
			value;
	}

	get _nativeBackboneCoordinatePersistence():
		| NativeBackboneCoordinatePersistenceAdapter
		| undefined {
		return this._coordinates?._nativeBackboneCoordinatePersistence;
	}

	set _nativeBackboneCoordinatePersistence(
		value: NativeBackboneCoordinatePersistenceAdapter | undefined,
	) {
		(this._coordinates ??=
			this.createCoordinatePersistenceCoordinator())._nativeBackboneCoordinatePersistence =
			value;
	}

	get _nativeBackboneCoordinateJournalLastFlushMs(): number {
		return this._coordinates
			?._nativeBackboneCoordinateJournalLastFlushMs as number;
	}

	set _nativeBackboneCoordinateJournalLastFlushMs(value: number) {
		(this._coordinates ??=
			this.createCoordinatePersistenceCoordinator())._nativeBackboneCoordinateJournalLastFlushMs =
			value;
	}

	private coordinateToHash!: Cache<string>;
	private recentlyRebalanced!: Cache<string>;

	uniqueReplicators!: Set<string>;
	private _replicatorJoinEmitted!: Set<string>;

	/* private _totalParticipation!: number; */

	// gid -> set of publicKeyHashes known to hold that gid's entries.
	//
	// This is a suppression memo, not a source of truth. A present row lets the
	// rebalance and repair paths skip re-sending an entry to a peer that already
	// has it. Every read is `?.has(peer)` guarded, and a MISSING row always
	// means "assume nothing is known", which produces strictly MORE work --
	// redundant unchecked delivery in the rebalance loop, redundant queueing in
	// the repair planner -- and never a wrong prune, a wrong quorum, or data
	// loss. Losing a row costs bandwidth; keeping a stale row costs a little
	// memory. That asymmetry is what the rest of this note turns on.
	//
	// GROWTH SHAPE. Rows are released by `deleteGidPeerHistory` on the two prune
	// paths, by `removePeerFromGidPeerHistory` once a gid's last peer drops (the
	// routine disconnect outcome), by `rebalanceAll({ clearCache: true })`, and
	// wholesale on close/reset. Trim now carries compact distinct removed-gid
	// facts beside its hash-only result. After the lower mutation commits, a
	// coalesced cleanup checks authoritative local-head liveness and drops a row
	// only when no sibling remains. A gid names a graph, not an entry: an
	// entry with `meta.next` inherits `min(next.meta.gid)` (see
	// packages/log/src/entry-v0.ts), so document updates fold into the gid of
	// the first put and the row count tracks distinct chain roots -- distinct
	// document ids -- rather than entry count. Insert-only workloads mint a
	// fresh gid per append and so do grow one row per entry. Merge-shadowed gids
	// are a separate, smaller source that still needs a fast-path-compatible
	// post-commit metadata seam. Registering the lower log's `onGidRemoved` hook
	// here would disable native prepared, commit-only, and batch append paths, so
	// this bounded slice deliberately addresses trim only.
	//
	// WHY TRIM DOES NOT SIMPLY CALL `deleteGidPeerHistory` AS WELL. Both prune
	// callers delete a whole row from a single entry's gid, and under the
	// default hash domain that is correct by construction: the coordinate is a
	// pure function of the gid (replication-domain-hash.ts sha256s
	// `entry.meta.gid`), so identical gid => identical coordinates => identical
	// leader set => every local sibling of that gid is prune-eligible in the
	// same batch. The gid really is finished locally. Trim offers no such
	// guarantee. It walks oldest-first against a length/bytelength/age bound and
	// stops the instant the bound is met (packages/log/src/trim.ts); its only
	// use of gid is memoizing the caller's `canTrim` verdict, never grouping
	// deletes. So trim routinely removes the OLDEST entry of a gid while newer
	// siblings -- same gid, same coordinates, still local, still replicated --
	// remain. Copying the prune call onto trim would therefore delete a LIVE row
	// on the common path, paying for the freed memory in repeated re-delivery of
	// entries that are still here. That trade is not worth it.
	//
	// Cleanup work is coalesced, checked in bounded batches, and capped at 4096
	// pending gids. Saturation forgets the overflowing suppression row
	// immediately. That bounded fallback can cause redundant delivery for a live
	// sibling, but missing history is deliberately correctness-safe as described
	// above; it cannot affect ownership, quorum, or retained data.
	//
	// Existing, deliberate imprecision: under the time domain the coordinate is
	// `meta.clock.timestamp.wallTime` (replication-domain-time.ts) and is
	// gid-independent, so siblings of one gid can carry different leader sets
	// and prune's whole-row delete is already over-eager there. The cost is the
	// same bounded extra traffic, never a wrong prune.
	_gidPeersHistory!: Map<string, Set<string>>;
	private _gidPeerHistoryCleanupState!: {
		history: Map<string, Set<string>>;
		tail: Promise<void>;
		pending: Set<string>;
		draining: boolean;
		highWater: number;
		wake?: () => void;
	};

	private _onSubscriptionFn!: (arg: any) => any;
	private _onUnsubscriptionFn!: (arg: any) => any;
	private _subscriptionChangeCallbacks?: Set<Promise<void>>;
	private _acceptSubscriptionChangeCallbacks = false;
	private _activeReceiveHandlersByPeer!: Map<string, PeerReceiveLeaseState>;
	private _receiveHandlerDrainByPeer!: Map<string, Set<Promise<void>>>;
	// The per-peer receive cleanup-gate refcounts now live on the
	// PeerSessionRegistry (_receiveCleanupGateByPeer moved file-to-file; see
	// src/peer-session.ts): acquired via acquireReceiveCleanupGate, read via
	// isReceiveCleanupGateOpen, cleared at _close via
	// clearCleanupGatesForClose.
	// One-shot capability adverts staged while a peer's opening barrier is
	// running (window truth = the session's openingBarrierActive sub-state;
	// the legacy _subscriptionOpeningEpochByPeer map is gone). `epoch` is the
	// PeerSession the advert was staged under; promote/delete key off it.
	private _openingSyncCapabilitiesByPeer!: Map<
		string,
		{
			epoch: object;
			capabilities: number;
			transportSession?: bigint;
			timestamp?: bigint;
		}
	>;
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
	// A successful native signature batch is only an authorization input for the
	// receive operation that requested it. Facts retain the exact verified bytes
	// and are leased around the lower-log join; byte comparison prevents an
	// earlier application callback from mutating a later entry underneath the
	// batch result. The array form also makes overlapping leases for the same
	// object safe without making verification sticky across retries.
	private _receiveSignatureVerificationFacts = new WeakMap<
		Entry<T>,
		ReceiveSignatureVerificationFact<T>[]
	>();
	private _closeController!: AbortController;
	private _respondToIHaveTimeout!: any;
	private _checkedPrune!: CheckedPruneCoordinator<T, R>;
	private _admittedPruneRemoves!: Set<Promise<unknown>>;
	private _pruneRemovesClosing = false;
	private _pendingIHaveCallbacks!: Set<Promise<void>>;
	private _pendingIHaveExpiryTimer?: ReturnType<typeof setTimeout>;
	private _pendingIHaveExpiryDeadline = Number.POSITIVE_INFINITY;

	private _pendingIHave!: Map<string, PendingIHave<T>>;

	// public key hash to range id to range
	pendingMaturity!: Map<string, Map<string, PendingMaturityRecord<R>>>; // map of peerId to timeout

	// The legacy replication-info ordering watermark (fence B8) is deleted:
	// its FENCING role was subsumed by the per-peer receive epoch, blocked set
	// and session identity, and its intra-epoch ORDERING role existed only for
	// the legacy apply lanes. The V2 lane orders by sender-authoritative
	// sequence numbers, and legacy frames are dropped unconditionally at the
	// B1 gate before any side effect.
	// The replication-info blocked set (fence B5) lives on the peer-session
	// registry: unsubscribed peers whose replication-info is ignored until a
	// reconnect barrier commits. See PeerSessionRegistry._replicationInfoBlockedPeers.
	// V2 recovery scheduler state, one row per open peer session (B12: the
	// legacy request scheduler that shared this map is deleted).
	private _replicationInfoRequestByPeer!: Map<
		string,
		{
			// Consecutive fruitless unparks — the escalation exponent for the
			// next unpark delay, reset on any applied V2 progress.
			attempts: number;
			timer?: ReturnType<typeof setTimeout>;
			peerSession: PeerSession;
			// When the current park was first observed.
			parkedSinceMs?: number;
		}
	>;
	private _replicationInfoApplyQueueByPeer!: Map<string, Promise<void>>;
	// One in-flight targeted subscriber-snapshot request per session-less peer.
	// A capability burst from a peer whose Subscribe has not been observed must
	// coalesce into a single pubsub.requestSubscribers call (mirrors the
	// waitForReplicator in-flight coalescing); a later burst may request again.
	private _subscriberSnapshotRequestsByPeer!: Map<string, Promise<void>>;
	// Range ids are global primary keys while receive lanes are per peer. Keep
	// reads and writes that decide one mutation in a single global lane.
	private _replicationRangeMutationTail: Promise<void> = Promise.resolve();
	private _replicationRangeMutationsClosing = false;
	// Log.remove awaits program onChange callbacks before its physical delete.
	// The counter tracking when checked prune holds the ownership lane across
	// that lower-log removal lives on the CheckedPruneCoordinator
	// (blockLocalRangeMutation / isBlockingLocalRangeMutation) so the callback
	// wrapper can identify its direct invocation.
	// Reject public local role/terminal operations invoked directly by that
	// program callback rather than letting it await the lane that is awaiting the
	// callback. Keep the guard for the callback's full async lifetime: a callback
	// that yields before re-entering is still part of the same deadlock cycle.
	// Remote/internal mutations bypass these public-operation admission checks.
	private _checkedPruneRemovalCallbackInvocationDepth = 0;
	// The local replication role sub-generation lives on the per-open
	// InstanceLifecycle (roleGeneration): explicit role changes invalidate
	// adaptive planners admitted under the previous role, because the ownership
	// lifecycle alone is insufficient — unreplicate() deliberately keeps the
	// store open. Bumped via lifecycle.bumpRoleGeneration(), checked via
	// lifecycle.isRoleCurrent(); the stage-2 accessor shim is gone.
	// If durable post-state cannot be reconciled to every native/runtime mirror,
	// reject later writers and planners until reopen rehydrates those mirrors.
	private _replicationRangeMutationFailure?: unknown;
	// design-note: not a new fence — this is the per-open identity object the
	// fence ratchet is migrating TOWARD (the session/lifecycle refactor). As
	// of stage 4 it physically owns the ownership/membership controllers
	// alongside the role sub-generation and receive counters;
	// the poison latch, terminal fences, and coordinator/debouncer identities
	// stay wrapped via late-bound readers until their own drain stages.
	private _instanceLifecycle?: InstanceLifecycle;
	// The local receive generations that fence replication-info handlers
	// across liveness evictions now live on the PeerSessionRegistry
	// (_replicationInfoReceiveEpochByPeer moved file-to-file; see
	// src/peer-session.ts): advanced through the registry and cleared at _close.
	// Receive-side ownership plans may span lower-log joins that invoke user code.
	// Increment synchronously with leader-cache invalidation so the handler can
	// detect whether its pre-join plan needs one fresh post-persist audit.
	// Count ownership-changing range mutations from queue admission through
	// settlement, including mutations already pending when a receive starts.
	// Subscription callbacks can overlap because removing a replicator mutates the
	// replication index asynchronously. Keep that lifecycle separate from message
	// timestamps so a reconnect can synchronously revoke an older unsubscribe.
	// The epoch tokens are PeerSession objects (src/peer-session.ts); the map
	// itself lives on the registry. Stage-2 of the fence refactor.
	private _peerSessions!: PeerSessionRegistry;
	// A superseded removal may be the queue item that actually observed an active
	// replicator. Carry that leave obligation to the transition that ultimately
	// wins, while a winning reconnect clears it without emitting a stale leave.
	private _pendingReplicatorLeaveByPeer!: Set<string>;
	private _liveness!: ReplicatorLivenessMonitor;

	private remoteBlocks!: RemoteBlocks;

	// Stage 3 (seam 8): the ownership-fence helper family below delegates to
	// InstanceLifecycle, whose folds are documented byte-identical in error
	// type, message, and `cause` (src/instance-lifecycle.ts). The helpers
	// stay instance methods — extracted-module deps closures and test spies
	// depend on them. The `lifecycle === undefined` fallbacks preserve the
	// legacy semantics verbatim for borsh-deserialized never-opened clones,
	// which skip constructors (no lifecycle, no controllers) — including the
	// legacy TypeError a capture attempt raises on such a clone.
	private throwIfReplicationOwnershipPoisoned(): void {
		const lifecycle = this._instanceLifecycle;
		if (lifecycle === undefined) {
			if (this._replicationRangeMutationFailure !== undefined) {
				throw new Error(
					"Replication ownership recovery is required before further planning",
					{ cause: this._replicationRangeMutationFailure },
				);
			}
			return;
		}
		lifecycle.throwIfPoisoned();
	}

	// THE single rotation point (stage 4): lifecycle + both controllers
	// replace together by construction — a fresh InstanceLifecycle IS the
	// rotation of the ownership controller it owns. The outgoing lifecycle's
	// ownership controller MUST be aborted here (a re-abort no-op on every
	// terminal path into open(), and the load-bearing abort for a direct
	// mid-open rotation): the stale-captured-lifecycle predicates rely on
	// non-current ⇒ aborted (see instance-lifecycle.ts). The abort runs
	// BEFORE the fresh lifecycle is installed, preserving the legacy
	// ordering: a synchronous abort listener observes pre-rotation state.
	private startRepairLifecycle(): AbortController {
		this._instanceLifecycle?.abortOwnership();
		this._instanceLifecycle = this.createInstanceLifecycle();
		this.clearCheckedPruneAuditTimer();
		return this._instanceLifecycle.ownershipLifecycleController;
	}

	private stopRepairLifecycle(): void {
		this._instanceLifecycle?.abortOwnership();
	}

	private isRepairLifecycleActive(controller: AbortController): boolean {
		const lifecycle = this._instanceLifecycle;
		if (lifecycle === undefined) {
			return (
				controller ===
					(this._instanceLifecycle
						?.ownershipLifecycleController as AbortController) &&
				!controller.signal.aborted &&
				this._replicationRangeMutationFailure === undefined &&
				!this.closed
			);
		}
		return lifecycle.isActiveFor(controller);
	}

	private captureReplicationOwnershipLifecycle(): AbortController {
		const controller = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController;
		this.throwIfReplicationOwnershipLifecycleInactive(controller);
		return controller;
	}

	private throwIfReplicationOwnershipLifecycleInactive(
		controller: AbortController,
	): void {
		const lifecycle = this._instanceLifecycle;
		if (lifecycle === undefined) {
			this.throwIfReplicationOwnershipPoisoned();
			if (!this.isRepairLifecycleActive(controller)) {
				throw new TerminalOperationNotStartedError(
					"Replication ownership lifecycle is no longer active",
				);
			}
			return;
		}
		// `lifecycle` is the current instance fetched synchronously above, so
		// throwIfInactive's isCurrent() term is trivially true and this reduces
		// to the legacy poison-then-active sequence with identical errors.
		lifecycle.throwIfInactive(controller);
	}

	private poisonReplicationOwnership(failure: unknown): unknown {
		this._replicationRangeMutationFailure ??= failure;
		this._instanceLifecycle?.markPoisoned(failure);
		this.stopRepairLifecycle();
		// Pending aggregate changes belong to the poisoned ownership generation.
		// Closing also resolves ignored `add()` promises, while the guarded
		// callback below observes any already-running rejection.
		this.replicationChangeDebounceFn?.close?.();
		this.pruneDebouncedFn?.close?.();
		this.rebalanceParticipationDebounced?.close();
		this.clearCheckedPruneAuditTimer();
		for (const hash of this._checkedPrune?.retries.keys() ?? []) {
			this._checkedPrune.clearRetry(hash);
		}
		this.joinWarmup.cancelAllJoinWarmupTargets();
		for (const timer of this._repairRetryTimers) {
			clearTimeout(timer);
		}
		this._repairRetryTimers.clear();
		for (const timer of this._joinAuthoritativeRepairTimersByDelay.values()) {
			clearTimeout(timer);
		}
		this._joinAuthoritativeRepairTimersByDelay.clear();
		this._joinAuthoritativeRepairPeersByDelay.clear();
		this._repairSweepPendingModes.clear();
		for (const peers of this._repairSweepPendingPeersByMode.values()) {
			peers.clear();
		}
		this.joinWarmup.clearRepairSweepWarmupSessions();
		this._repairSweepOptimisticGidPeersPending.clear();
		this._repairSweepOptimisticGidsByPeer.clear();
		for (const targets of this._repairFrontierByMode.values()) {
			targets.clear();
		}
		for (const targets of this._repairFrontierActiveTargetsByMode.values()) {
			targets.clear();
		}
		for (const targets of this._repairFrontierBypassKnownPeersByMode.values()) {
			targets.clear();
		}
		if (this._appendBackfillTimer) {
			clearTimeout(this._appendBackfillTimer);
			this._appendBackfillTimer = undefined;
		}
		this._appendBackfillPendingByTarget.clear();
		for (const pendingRanges of this.pendingMaturity?.values() ?? []) {
			for (const pending of pendingRanges.values()) {
				clearTimeout(pending.timeout);
			}
			pendingRanges.clear();
		}
		this.pendingMaturity?.clear();
		return this._replicationRangeMutationFailure;
	}

	private throwIfNativeDurableCommitFailed(): void {
		this.throwIfReplicationOwnershipPoisoned();
		if (this._nativeStrictDurableTransactionFailure !== undefined) {
			throw new Error(
				"Native durable transaction recovery is required before another mutation",
				{ cause: this._nativeStrictDurableTransactionFailure },
			);
		}
		const wrapperFailure = (
			this.remoteBlocks?.localStore as unknown as {
				getNativeDurableCommitFailure?: () =>
					| NativeDurableCommitError
					| undefined;
			}
		)?.getNativeDurableCommitFailure?.();
		if (wrapperFailure) {
			this._nativeDurableCommitFailure ??= wrapperFailure;
		}
		if (this._nativeDurableCommitFailure) {
			throw this._nativeDurableCommitFailure;
		}
	}

	private poisonNativeStrictDurableTransaction(cause: unknown): void {
		this._nativeStrictDurableTransactionFailure ??= cause;
		this.log?.entryIndex?.poisonNativeDurableTransactionMutations(
			this._nativeStrictDurableTransactionFailure,
		);
	}

	private clearNativeStrictDurableTransactionFailure(): void {
		this._nativeStrictDurableTransactionFailure = undefined;
		this.log?.entryIndex?.clearNativeDurableTransactionMutationFailure();
	}

	private failNativeDurableCommit(
		cause: unknown,
		options?: {
			committedCids?: Iterable<string>;
			failedCids?: Iterable<string>;
		},
	): never {
		this.ensureNativeDurabilityRuntimeState();
		for (const cid of options?.committedCids ?? []) {
			this._nativeDurableRecoveryCids.add(cid);
		}
		if (cause instanceof NativeDurableCommitError) {
			cause.addCommitContext(options, { preferIncomingOrder: true });
		}
		this._nativeDurableCommitFailure ??=
			cause instanceof NativeDurableCommitError
				? cause
				: new NativeDurableCommitError(cause, options);
		this._nativeDurableCommitFailure.addCommitContext(options, {
			preferIncomingOrder: true,
		});
		throw this._nativeDurableCommitFailure;
	}

	private snapshotNativeBackboneDocument(
		input: NativeBackboneDocumentIndexCommitInput | undefined,
	): NativeBackboneDocumentRollback | undefined {
		const backbone = this._nativeBackbone;
		if (!backbone || !input) return undefined;
		const value = backbone.documentValueBytes(input.key);
		return {
			key: input.key,
			value: value ? new Uint8Array(value) : undefined,
			byteElementIndexLimit: input.byteElementIndexLimit ?? 0,
		};
	}

	private restoreNativeBackboneDocument(
		rollback: NativeBackboneDocumentRollback,
	): void {
		const backbone = this._nativeBackbone;
		if (!backbone) return;
		backbone.deleteDocument(rollback.key);
		if (rollback.value) {
			// documentValueBytes returns the complete stored encoding, so it can be
			// restored as one prefix with an empty suffix.
			backbone.putDocumentEncodedPartsStored(
				rollback.key,
				rollback.value,
				new Uint8Array(),
				rollback.byteElementIndexLimit,
			);
		}
	}

	private parseNativeStrictDurableTransactionJournalRecord(
		bytes: Uint8Array | undefined,
		slot: 0 | 1,
	): NativeStrictDurableTransactionJournalState | undefined {
		if (bytes === undefined) {
			return undefined;
		}
		// The pre-journal implementation represented a cleared intent as an empty
		// primary file. Treat it as generation zero so the first framed update is
		// written to the other slot and can never destroy the only valid state.
		if (bytes.byteLength === 0) {
			return { sequence: 0, slot };
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(new TextDecoder().decode(bytes));
		} catch (error) {
			throw new Error("Invalid native durable transaction journal JSON", {
				cause: error,
			});
		}
		if (!parsed || typeof parsed !== "object") {
			throw new Error("Invalid native durable transaction journal record");
		}
		const candidate =
			parsed as Partial<NativeStrictDurableTransactionJournalRecord>;
		if (candidate.format === NATIVE_STRICT_DURABLE_TRANSACTION_JOURNAL_FORMAT) {
			if (
				candidate.version !== 1 ||
				!Number.isSafeInteger(candidate.sequence) ||
				(candidate.sequence ?? -1) < 1 ||
				(candidate.state !== "intent" && candidate.state !== "cleared") ||
				typeof candidate.checksum !== "string"
			) {
				throw new Error("Invalid native durable transaction journal frame");
			}
			const intent = candidate.intent ?? null;
			if (
				(candidate.state === "intent" && intent?.version !== 1) ||
				(candidate.state === "cleared" && intent !== null)
			) {
				throw new Error("Invalid native durable transaction journal state");
			}
			const body = nativeStrictDurableTransactionJournalBody(
				candidate.sequence!,
				intent ?? undefined,
			);
			const checksum = toHexString(
				sha256Sync(nativeStrictDurableTransactionJournalBodyBytes(body)),
			);
			if (checksum !== candidate.checksum) {
				throw new Error("Native durable transaction journal checksum mismatch");
			}
			return {
				sequence: candidate.sequence!,
				slot,
				intent: intent ?? undefined,
			};
		}
		// Backward compatibility with the original single raw-JSON intent. A
		// framed generation always sorts after this synthetic generation zero.
		const legacy = parsed as Partial<NativeStrictDurableTransactionIntent>;
		if (legacy.version !== 1) {
			throw new Error("Unsupported native durable transaction recovery intent");
		}
		return {
			sequence: 0,
			slot,
			intent: legacy as NativeStrictDurableTransactionIntent,
		};
	}

	private async loadNativeStrictDurableTransactionJournalState(): Promise<NativeStrictDurableTransactionJournalState> {
		if (this._nativeStrictDurableTransactionJournalState) {
			return this._nativeStrictDurableTransactionJournalState;
		}
		const store = this._nativeBackboneCoordinatePersistenceStore;
		if (!store) {
			return (this._nativeStrictDurableTransactionJournalState = {
				sequence: 0,
				slot: 0,
			});
		}
		const bytes = await Promise.all(
			NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILES.map((name) =>
				store.read(name),
			),
		);
		const valid: NativeStrictDurableTransactionJournalState[] = [];
		const errors: unknown[] = [];
		for (let index = 0; index < bytes.length; index++) {
			try {
				const state = this.parseNativeStrictDurableTransactionJournalRecord(
					bytes[index],
					index as 0 | 1,
				);
				if (state) valid.push(state);
			} catch (error) {
				errors.push(error);
			}
		}
		if (valid.length === 0) {
			if (errors.length > 0) {
				throw new AggregateError(
					errors,
					"No valid native durable transaction journal generation remains",
				);
			}
			// A completely new store has an implicit cleared generation. Before the
			// first intent is written we materialize this baseline in one slot, so a
			// corrupt sole slot can never be confused with a safe first-write tear (or
			// with a torn legacy single-file intent).
			return (this._nativeStrictDurableTransactionJournalState = {
				sequence: 0,
				slot: 0,
				implicit: true,
			});
		}
		valid.sort(
			(left, right) => left.sequence - right.sequence || left.slot - right.slot,
		);
		return (this._nativeStrictDurableTransactionJournalState = valid.at(-1)!);
	}

	private async writeNativeStrictDurableTransactionIntent(
		intent: NativeStrictDurableTransactionIntent | undefined,
	) {
		const store = this._nativeBackboneCoordinatePersistenceStore;
		if (!store) {
			return;
		}
		let previous = await this.loadNativeStrictDurableTransactionJournalState();
		if (previous.implicit) {
			const baselineSequence = previous.sequence + 1;
			const baselineSlot = (previous.slot === 0 ? 1 : 0) as 0 | 1;
			const baselineFile =
				NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILES[baselineSlot];
			await store.write(
				baselineFile,
				nativeStrictDurableTransactionJournalRecordBytes(
					baselineSequence,
					undefined,
				),
			);
			await this.barrierNativeStrictDurableStore(store, baselineFile);
			previous = {
				sequence: baselineSequence,
				slot: baselineSlot,
			};
			this._nativeStrictDurableTransactionJournalState = previous;
		}
		const sequence = previous.sequence + 1;
		const slot = (previous.slot === 0 ? 1 : 0) as 0 | 1;
		const bytes = nativeStrictDurableTransactionJournalRecordBytes(
			sequence,
			intent,
		);
		const file = NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILES[slot];
		// Alternate slots. If this write is interrupted or torn, the previous
		// checksummed generation remains untouched and recovery ignores the invalid
		// newer slot. A durable shared-log generation requires an explicit physical
		// barrier before this generation can become recovery-authoritative.
		await store.write(file, bytes);
		await this.barrierNativeStrictDurableStore(store, file);
		this._nativeStrictDurableTransactionJournalState = {
			sequence,
			slot,
			intent,
		};
	}

	private async barrierNativeStrictDurableStore(
		store: NativeBackboneCoordinatePersistenceStore,
		file: string,
	): Promise<void> {
		if (this.node.directory != null) {
			if (typeof store.durableBarrier !== "function") {
				throw new Error(
					"Durable native coordinate persistence does not expose a physical durability barrier",
				);
			}
			await store.durableBarrier(file);
			return;
		}
		// Memory-only operation has no durable lower marker. Preserve compatibility
		// for transient adapters while still using a real barrier when they expose it.
		if (store.durableBarrier) {
			await store.durableBarrier(file);
		} else {
			await store.flush?.(file);
		}
	}

	private async beginNativeStrictDurableTransaction(
		documents: NativeBackboneDocumentRollback[],
	): Promise<NativeStrictDurableTransactionHandle> {
		this.throwIfNativeDurableCommitFailed();
		if (this._nativeStrictDurableTransactionsClosing) {
			throw new Error("Shared log is closing");
		}
		const previous =
			this._nativeStrictDurableTransactionTail ?? Promise.resolve();
		let release!: () => void;
		const held = new Promise<void>((resolve) => {
			release = resolve;
		});
		this._nativeStrictDurableTransactionTail = previous.then(() => held);
		await previous;
		try {
			this.throwIfNativeDurableCommitFailed();
			if (this._nativeStrictDurableTransactionsClosing) {
				throw new Error("Shared log is closing");
			}
		} catch (error) {
			release();
			throw error;
		}
		const handle: NativeStrictDurableTransactionHandle = {
			intent: {
				version: 1,
				lowerMarkerCommitted: false,
				appendHashes: [],
				trimHashes: [],
				coordinateDeleteHashes: [],
				lowerIndexRows: [],
				coordinates: [],
				documents: documents.map((document) => ({
					key: document.key,
					value: document.value ? [...document.value] : undefined,
					byteElementIndexLimit: document.byteElementIndexLimit,
				})),
			},
			release,
			released: false,
		};
		(this._nativeStrictDurableTransactions ??= new Set()).add(handle);
		try {
			await this.writeNativeStrictDurableTransactionIntent(handle.intent);
			return handle;
		} catch (error) {
			handle.released = true;
			this._nativeStrictDurableTransactions.delete(handle);
			handle.release();
			throw error;
		}
	}

	private async setNativeStrictDurableTransactionOperation(
		handle: NativeStrictDurableTransactionHandle | undefined,
		appendHashes: string[],
		trimHashes: string[],
		coordinateRollback?: NativeBackboneCoordinateRollback<R>,
		coordinateDeleteHashes: string[] = [],
	) {
		if (!handle) {
			return;
		}
		handle.intent.appendHashes = [...new Set(appendHashes.filter(Boolean))];
		handle.intent.trimHashes = [...new Set(trimHashes.filter(Boolean))];
		handle.intent.coordinateDeleteHashes = [
			...new Set(coordinateDeleteHashes.filter(Boolean)),
		];
		const lowerHashes = [
			...new Set([
				...handle.intent.appendHashes,
				...handle.intent.trimHashes,
				...(coordinateRollback?.hashes ?? []),
				...handle.intent.coordinateDeleteHashes,
			]),
		];
		if (handle.lowerHashMutationLockOwner) {
			throw new Error("Native durable transaction operation is already locked");
		}
		// This lease is shared with the lower native transaction and every ordinary
		// EntryIndex mutation. Acquire it before reading any before-image, then hold
		// it through marker acknowledgement or exact compensation.
		handle.lowerHashMutationLockOwner =
			await this.log.entryIndex.acquireHashMutationLocks(lowerHashes);
		handle.intent.lowerIndexRows = await Promise.all(
			lowerHashes.map(async (hash) => {
				// EntryIndex publication may still live only in its pending generation.
				// Snapshot the exact logical row that the marker phase will consume,
				// rather than only the durable index, so a pre-marker crash can restore a
				// pending external-next head instead of treating it as previously absent.
				const previous = (await this.log.entryIndex.getShallow(hash))?.value;
				return {
					hash,
					before: previous ? [...serialize(previous)] : undefined,
				};
			}),
		);
		handle.intent.coordinates = coordinateRollback
			? [...coordinateRollback.hashes].map((hash) => {
					const previous = coordinateRollback.entries.get(hash);
					if (!previous) {
						return { hash };
					}
					const materialized =
						this._coordinates.materializeResidentCoordinateEntry(previous);
					return {
						hash,
						value: {
							hashNumber: materialized.hashNumber.toString(),
							gid: materialized.gid,
							coordinates: materialized.coordinates.map((value) =>
								value.toString(),
							),
							wallTime: materialized.wallTime.toString(),
							assignedToRangeBoundary: materialized.assignedToRangeBoundary,
							metaBytes: [...materialized.getMetaBytes()],
						},
					};
				})
			: [];
		await this.writeNativeStrictDurableTransactionIntent(handle.intent);
	}

	private async setNativeStrictDurableTransactionExpectedRows(
		handle: NativeStrictDurableTransactionHandle | undefined,
		rows: ShallowEntry[],
	) {
		if (!handle) {
			return;
		}
		if (!handle.lowerHashMutationLockOwner) {
			throw new Error(
				"Native durable transaction has no lower hash lock owner",
			);
		}
		this.log.entryIndex.assertHashMutationLocks(
			handle.lowerHashMutationLockOwner,
			rows.flatMap((row) => [row.hash, ...row.meta.next]),
		);
		const rowsByHash = new Map(
			handle.intent.lowerIndexRows.map((row) => [row.hash, row]),
		);
		for (const row of rows) {
			const intentRow = rowsByHash.get(row.hash);
			if (intentRow) {
				intentRow.after = [...serialize(row)];
			}
		}
		for (const nextHash of new Set(rows.flatMap((row) => row.meta.next))) {
			const existingIntentRow = rowsByHash.get(nextHash);
			if (existingIntentRow) {
				if (!existingIntentRow.after && existingIntentRow.before) {
					const after = deserialize(
						Uint8Array.from(existingIntentRow.before),
						ShallowEntry,
					);
					after.head = false;
					existingIntentRow.after = [...serialize(after)];
				}
				continue;
			}
			const previous = (await this.log.entryIndex.getShallow(nextHash))?.value;
			if (!previous) {
				continue;
			}
			const after = deserialize(serialize(previous), ShallowEntry);
			after.head = false;
			const intentRow = {
				hash: nextHash,
				before: [...serialize(previous)],
				after: [...serialize(after)],
			};
			handle.intent.lowerIndexRows.push(intentRow);
			rowsByHash.set(nextHash, intentRow);
		}
		handle.intent.coordinateDeleteHashes = [
			...new Set([
				...(handle.intent.coordinateDeleteHashes ?? []),
				...handle.intent.trimHashes,
				...rows.flatMap((row) => row.meta.next),
			]),
		];
		await this.writeNativeStrictDurableTransactionIntent(handle.intent);
	}

	private async markNativeStrictDurableTransactionLowerMarker(
		handle: NativeStrictDurableTransactionHandle | undefined,
	) {
		if (!handle || handle.released) {
			return;
		}
		handle.intent.lowerMarkerCommitted = true;
		await this.writeNativeStrictDurableTransactionIntent(handle.intent);
	}

	private async markNativeStrictDurableTransactionRollback(
		handle: NativeStrictDurableTransactionHandle | undefined,
	) {
		if (!handle || handle.released) {
			return;
		}
		const previousMarker = handle.intent.lowerMarkerCommitted;
		handle.intent.lowerMarkerCommitted = false;
		try {
			await this.writeNativeStrictDurableTransactionIntent(handle.intent);
		} catch (error) {
			// The last valid generation may still contain a true marker. Preserve
			// that in-memory knowledge and keep the handle held until the caller has
			// retained the lower finalizer. Releasing first would let concurrent close
			// compensate lower facts while recovery still sees a committed marker.
			handle.intent.lowerMarkerCommitted = previousMarker;
			this.poisonNativeStrictDurableTransaction(error);
			throw error;
		}
	}

	private async completeNativeStrictDurableTrimCleanup(
		intent: NativeStrictDurableTransactionIntent,
		committed = intent.lowerMarkerCommitted === true,
		reconstructMissing = false,
	) {
		if (!committed || intent.trimHashes.length === 0) {
			return;
		}
		const localStore = this.remoteBlocks?.localStore as unknown as {
			completeCommittedNativeDeleteCleanup?: (
				cids: string[],
				options?: { reconstructMissing?: boolean },
			) => Promise<void>;
		};
		if (
			typeof localStore?.completeCommittedNativeDeleteCleanup === "function"
		) {
			await localStore.completeCommittedNativeDeleteCleanup(intent.trimHashes, {
				reconstructMissing,
			});
		}
	}

	private async completeNativeStrictDurableCoordinateCleanup(
		intent: NativeStrictDurableTransactionIntent,
		committed = intent.lowerMarkerCommitted === true,
	) {
		const hashes = intent.coordinateDeleteHashes ?? [];
		if (!committed || hashes.length === 0) {
			return;
		}
		await this._coordinates.deleteCoordinatesForHashes(hashes);
		const flushed = this._coordinates.flushNativeBackboneCoordinateJournal();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
	}

	private async completeNativeStrictDurableTransaction(
		handle: NativeStrictDurableTransactionHandle | undefined,
	) {
		if (!handle || handle.released) {
			return;
		}
		try {
			await this.completeNativeStrictDurableCoordinateCleanup(handle.intent);
			await this.completeNativeStrictDurableTrimCleanup(handle.intent);
			await this.writeNativeStrictDurableTransactionIntent(undefined);
			this.clearNativeStrictDurableTransactionFailure();
		} catch (error) {
			// The lower marker may already be acknowledged. Retain the intent and
			// reject every later mutation until reopen can finish recovery; allowing a
			// new transaction to overwrite this generation would make rollback/GC debt
			// ambiguous and can erase acknowledged data.
			this.poisonNativeStrictDurableTransaction(error);
			throw error;
		} finally {
			if (handle.lowerHashMutationLockOwner) {
				this.log.entryIndex.releaseHashMutationLocks(
					handle.lowerHashMutationLockOwner,
				);
				handle.lowerHashMutationLockOwner = undefined;
			}
			handle.released = true;
			this._nativeStrictDurableTransactions?.delete(handle);
			handle.release();
		}
	}

	private async finishCommittedNativeStrictDurableTransaction<TValue>(
		handle: NativeStrictDurableTransactionHandle | undefined,
		finish: () => MaybePromise<TValue>,
		shouldWarnOnRetirementFailure: () => boolean = () => true,
	): Promise<TValue> {
		let finishResult: TValue | undefined;
		let finishError: unknown;
		let finishFailed = false;
		try {
			finishResult = await finish();
		} catch (error) {
			finishError = error;
			finishFailed = true;
		}
		try {
			await this.completeNativeStrictDurableTransaction(handle);
		} catch (error) {
			// The acknowledged lower commit is already final. Completion poisons and
			// releases the in-memory transaction before throwing, so preserve an
			// earlier post-commit failure while leaving recovery to retire the intent.
			if (!finishFailed && !shouldWarnOnRetirementFailure()) {
				throw error;
			}
			warn(`Failed to retire committed native intent: ${String(error)}`);
		}
		if (finishFailed) {
			throw finishError;
		}
		return finishResult as TValue;
	}

	private releaseNativeStrictDurableTransaction(
		handle: NativeStrictDurableTransactionHandle | undefined,
		cause: unknown = new Error(
			"Native durable transaction intent was retained for recovery",
		),
	) {
		if (!handle || handle.released) {
			return;
		}
		this.poisonNativeStrictDurableTransaction(cause);
		if (handle.lowerHashMutationLockOwner) {
			this.log.entryIndex.releaseHashMutationLocks(
				handle.lowerHashMutationLockOwner,
			);
			handle.lowerHashMutationLockOwner = undefined;
		}
		handle.released = true;
		this._nativeStrictDurableTransactions?.delete(handle);
		handle.release();
	}

	private retainNativeStrictDurableTransactionAfterMarkerFailure(
		handle: NativeStrictDurableTransactionHandle | undefined,
		finalizer: TrustedLowerLogNativeCommitFinalizer | undefined,
		cause: unknown,
	): unknown[] {
		const failures: unknown[] = [cause];
		try {
			finalizer?.retainForRecovery();
		} catch (error) {
			failures.push(error);
		} finally {
			// retainForRecovery finalizes its lower transaction even when one of its
			// internal cleanup steps reports an error. Only release the strict handle
			// after that synchronous state transition has been attempted.
			this.releaseNativeStrictDurableTransaction(handle, cause);
		}
		return failures;
	}

	private async settleNativeStrictDurableTransactionsForClose(): Promise<void> {
		while ((this._nativeStrictDurableTransactions?.size ?? 0) > 0) {
			const tail = this._nativeStrictDurableTransactionTail;
			if (!tail) {
				throw new Error(
					"Native strict durable transaction has no settlement tail",
				);
			}
			// A close racing an acknowledged lower marker must not release the strict
			// handle and let Log.close() compensate while the on-disk intent still says
			// committed. Wait until the owner either retires the intent or deliberately
			// retains it for recovery before closing the lower log or persistence stores.
			await tail;
		}
	}

	private async recoverNativeStrictDurableTransactionIntent(
		documentIndexReady = false,
	): Promise<boolean> {
		const store = this._nativeBackboneCoordinatePersistenceStore;
		if (!store || !this._nativeBackbone) {
			this._nativeStrictDurableDocumentRecoveryDeferred = false;
			return true;
		}
		const journalState =
			await this.loadNativeStrictDurableTransactionJournalState();
		const intent = journalState.intent;
		if (!intent) {
			this._nativeStrictDurableDocumentRecoveryDeferred = false;
			this.clearNativeStrictDurableTransactionFailure();
			return true;
		}
		if (intent.version !== 1) {
			throw new Error("Unsupported native durable transaction recovery intent");
		}
		intent.trimHashes ??= [];
		intent.coordinateDeleteHashes ??= [];
		intent.lowerIndexRows ??= [];
		intent.coordinates ??= [];
		const bytesEqual = (
			left: Uint8Array | undefined,
			right: number[] | undefined,
		) => {
			if (!left || !right) {
				return left === undefined && right === undefined;
			}
			if (left.byteLength !== right.length) {
				return false;
			}
			for (let index = 0; index < left.byteLength; index++) {
				if (left[index] !== right[index]) {
					return false;
				}
			}
			return true;
		};
		const immutableRowEquals = (
			current: Uint8Array | undefined,
			expected: number[] | undefined,
		) => {
			if (!current || !expected) {
				return current === undefined && expected === undefined;
			}
			const currentRow = deserialize(current, ShallowEntry);
			const expectedRow = deserialize(Uint8Array.from(expected), ShallowEntry);
			// `head` is a mutable graph projection. Hash, payload size, and metadata
			// are content-addressed append identity and are safe marker evidence even
			// when a later acknowledged entry has demoted this row.
			currentRow.head = false;
			expectedRow.head = false;
			return bytesEqual(serialize(currentRow), [...serialize(expectedRow)]);
		};
		const currentLowerRows = new Map<string, Uint8Array | undefined>();
		for (const row of intent.lowerIndexRows) {
			const current = (
				await this.log.entryIndex.properties.index.get(toId(row.hash))
			)?.value;
			currentLowerRows.set(row.hash, current ? serialize(current) : undefined);
		}
		const trimHashes = new Set(intent.trimHashes);
		const retainedMarkerRows = intent.lowerIndexRows.filter(
			(row) =>
				intent.appendHashes.includes(row.hash) &&
				!trimHashes.has(row.hash) &&
				row.after !== undefined,
		);
		// Only a row known absent in the before-image is an unambiguous lower commit
		// marker. An existing content-addressed row can equal the after-image once
		// mutable `head` is ignored even before this transaction mutated anything.
		const expectedMarkerRows = retainedMarkerRows.filter(
			(row) => row.before === undefined,
		);
		let lowerMarkerCommitted =
			intent.lowerMarkerCommitted === true ||
			(expectedMarkerRows.length > 0 &&
				expectedMarkerRows.every((row) =>
					immutableRowEquals(currentLowerRows.get(row.hash), row.after),
				));
		if (
			!lowerMarkerCommitted &&
			!documentIndexReady &&
			intent.documents.some((document) => document.value !== undefined)
		) {
			// SharedLog opens before Documents can attach its schema-aware native
			// index. Restoring an encoded before-image requires that schema. Keep the
			// intent authoritative and mutations poisoned until Documents has attached
			// the index and explicitly resumes recovery.
			this._nativeStrictDurableDocumentRecoveryDeferred = true;
			this.poisonNativeStrictDurableTransaction(
				new Error(
					"Native strict durable document recovery is waiting for its document index",
				),
			);
			return false;
		}

		const lowerIndex = this.log.entryIndex.properties
			.index as PutAndDeleteIndex<ShallowEntry>;
		const deleteLowerIndexHash = async (hash: string) => {
			if (lowerIndex.delIds) {
				await lowerIndex.delIds([hash]);
			} else if (lowerIndex.delIdsNoReturn) {
				await lowerIndex.delIdsNoReturn([hash]);
			} else {
				await lowerIndex.del({ query: { hash } });
			}
		};
		let lowerIndexChanged = false;
		if (lowerMarkerCommitted) {
			for (const row of intent.lowerIndexRows) {
				if (trimHashes.has(row.hash) || !row.after) {
					continue;
				}
				const current = currentLowerRows.get(row.hash);
				if (immutableRowEquals(current, row.after)) {
					// Preserve the current mutable head projection. It may include a later
					// acknowledged Y -> X demotion that must survive recovery.
					continue;
				}
				if (!intent.appendHashes.includes(row.hash) || current !== undefined) {
					// External-next rows are not resurrected over a later delete, and a
					// conflicting present content-addressed row is never overwritten.
					continue;
				}
				await lowerIndex.put(
					deserialize(Uint8Array.from(row.after), ShallowEntry),
				);
				lowerIndexChanged = true;
			}
			for (const hash of intent.trimHashes) {
				const current = await lowerIndex.get(toId(hash));
				const intentRow = intent.lowerIndexRows.find(
					(row) => row.hash === hash,
				);
				if (
					current &&
					intentRow?.before &&
					bytesEqual(serialize(current.value), intentRow.before)
				) {
					await deleteLowerIndexHash(hash);
					lowerIndexChanged = true;
				}
			}
		} else {
			for (const row of intent.lowerIndexRows) {
				const current = currentLowerRows.get(row.hash);
				if (bytesEqual(current, row.before)) {
					continue;
				}
				// Exact after-image CAS: a later mutation (including only a `head`
				// change) owns the row and must not be erased or overwritten by recovery.
				if (!bytesEqual(current, row.after)) {
					continue;
				}
				if (row.before) {
					await lowerIndex.put(
						deserialize(Uint8Array.from(row.before), ShallowEntry),
					);
				} else {
					await deleteLowerIndexHash(row.hash);
				}
				lowerIndexChanged = true;
			}
		}
		if (lowerIndexChanged) {
			await this.log.entryIndex.init();
		}

		if (!lowerMarkerCommitted) {
			if (intent.coordinates.length > 0) {
				const mutationGenerations =
					(this._coordinates._nativeCoordinateMutationGenerations ??=
						new Map());
				const rollback: NativeBackboneCoordinateRollback<R> = {
					hashes: new Set(),
					entries: new Map(),
					generations: new Map(),
				};
				for (const coordinate of intent.coordinates) {
					rollback.hashes.add(coordinate.hash);
					// Same hold-counted row shape the coordinator's own
					// snapshot writes: one hold per hash, released by the
					// settle after the replay consumes the token below.
					// RELIES ON `intent.coordinates` HOLDING UNIQUE HASHES —
					// it is built from the token's `hashes` Set (see
					// setNativeStrictDurableTransactionOperation). Holds are
					// taken per element here but released per unique hash by
					// the settle, so a duplicate would take two and release
					// one and retain that row forever. That is the safe
					// direction (a retained row, never a fail-open rollback),
					// but keep the source a Set.
					const row = mutationGenerations.get(coordinate.hash);
					const generation = (row?.generation ?? 0) + 1;
					mutationGenerations.set(coordinate.hash, {
						generation,
						holds: (row?.holds ?? 0) + 1,
					});
					rollback.generations.set(coordinate.hash, generation);
					if (coordinate.value) {
						const number = (value: string) =>
							(this.domain.resolution === "u32"
								? Number(value)
								: BigInt(value)) as NumberFromType<R>;
						rollback.entries.set(
							coordinate.hash,
							new this.indexableDomain.constructorEntry({
								hash: coordinate.hash,
								hashNumber: number(coordinate.value.hashNumber),
								gid: coordinate.value.gid,
								coordinates: coordinate.value.coordinates.map(number),
								wallTime: BigInt(coordinate.value.wallTime),
								assignedToRangeBoundary:
									coordinate.value.assignedToRangeBoundary,
								metaBytes: Uint8Array.from(coordinate.value.metaBytes),
							}),
						);
					}
				}
				await this._coordinates.rollbackNativeBackboneCoordinateAppendDurably(
					"",
					rollback,
				);
				// The replay fabricated these generations itself one turn
				// earlier and has now consumed them, so the rows are dead.
				this._coordinates.settleResidentCoordinateSnapshot(rollback);
			}
			for (const document of intent.documents) {
				this.restoreNativeBackboneDocument({
					key: document.key,
					value: document.value ? Uint8Array.from(document.value) : undefined,
					byteElementIndexLimit: document.byteElementIndexLimit,
				});
			}
			const flushed = this._coordinates.flushNativeBackboneCoordinateJournal();
			if (isPromiseLike(flushed)) {
				await flushed;
			}
		}
		if (lowerMarkerCommitted) {
			await this.completeNativeStrictDurableCoordinateCleanup(intent, true);
			await this.completeNativeStrictDurableTrimCleanup(intent, true, true);
		}
		await this.writeNativeStrictDurableTransactionIntent(undefined);
		this._nativeStrictDurableDocumentRecoveryDeferred = false;
		this.clearNativeStrictDurableTransactionFailure();
		return true;
	}

	/** @internal Complete a deferred rollback after Documents attaches its schema. */
	async finishNativeStrictDurableDocumentRecovery(): Promise<void> {
		if (!this._nativeStrictDurableDocumentRecoveryDeferred) {
			return;
		}
		const completed =
			await this.recoverNativeStrictDurableTransactionIntent(true);
		if (!completed) {
			throw new Error(
				"Native strict durable document recovery did not complete",
			);
		}
		await this.reconcileNativeCoordinatesWithLowerCommitMarkers();
	}

	private async rollbackFailedNativeBackboneTransaction(properties: {
		committedHashes: string[];
		trimmedEntries?: Parameters<NativePeerbitBackbone["graph"]["putBatch"]>[0];
		coordinateEntries?: NativeBackboneCoordinateRollback<R>;
		documents?: NativeBackboneDocumentRollback[];
		unmirroredBlockCompensation?: boolean;
		skipBlockCompensation?: boolean;
		restoreGraphFromIndex?: boolean;
		durableWrapper?: {
			rollbackUnmirroredNativeCommits?: (
				cids: string[],
				restoreNativeCids?: string[],
			) => Promise<void>;
			rollbackFailedNativeCommits?: (
				cids: string[],
				restoreNativeCids?: string[],
			) => Promise<void>;
		};
	}): Promise<void> {
		const backbone = this._nativeBackbone;
		if (!backbone) return;
		for (
			let index = properties.committedHashes.length - 1;
			index >= 0;
			index--
		) {
			const hash = properties.committedHashes[index]!;
			backbone.graph.delete(hash);
			this._coordinates.rollbackNativeBackboneCoordinateAppend(
				hash,
				properties.coordinateEntries,
			);
		}
		if (properties.restoreGraphFromIndex) {
			await this.log.entryIndex.restoreNativeGraphFromIndex();
		} else {
			if (properties.trimmedEntries?.length) {
				backbone.graph.putBatch(properties.trimmedEntries);
			}
		}
		for (const document of properties.documents ?? []) {
			this.restoreNativeBackboneDocument(document);
		}
		const flushed = this._coordinates.flushNativeBackboneCoordinateJournal();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
		if (properties.skipBlockCompensation) {
			return;
		}
		let compensated = false;
		try {
			if (
				properties.unmirroredBlockCompensation &&
				properties.durableWrapper?.rollbackUnmirroredNativeCommits
			) {
				await properties.durableWrapper.rollbackUnmirroredNativeCommits(
					properties.committedHashes,
					properties.trimmedEntries?.map((entry) => entry.hash),
				);
			} else if (properties.durableWrapper?.rollbackFailedNativeCommits) {
				await properties.durableWrapper.rollbackFailedNativeCommits(
					properties.committedHashes,
					properties.trimmedEntries?.map((entry) => entry.hash),
				);
			} else {
				await backbone.blocks.rmMany(properties.committedHashes);
			}
			compensated = true;
		} finally {
			this._nativeDurableRecoveryReadyForReopen = compensated;
		}
	}

	private openTime!: number;
	private oldestOpenTime!: number;

	private keep?: (
		entry: ShallowOrFullEntry<T> | EntryReplicated<R>,
	) => Promise<boolean> | boolean;

	// A fn that we can call many times that recalculates the participation role
	private rebalanceParticipationDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;
	private _announcements!: ReplicationAnnouncementCoordinator;
	private _v2Receive!: ReplicationInfoV2ReceiveCoordinator;
	private _v2Send!: ReplicationInfoV2SendCoordinator<R>;

	// A fn for debouncing the calls for pruning
	pruneDebouncedFn!: DebouncedAccumulatorMap<{
		entry: CheckedPruneEntry<T, R>;
		leaders: CheckedPruneLeaderMap;
		workToken?: object;
	}>;
	private _checkedPruneAuditTimer?: ReturnType<typeof setTimeout>;

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
		Map<string, Map<string, RepairDispatchEntry<R>>>
	>;
	private _repairFrontierActiveTargetsByMode!: Map<
		RepairDispatchMode,
		Map<string, object>
	>;
	private _repairFrontierBypassKnownPeersByMode!: Map<
		RepairDispatchMode,
		Set<string>
	>;
	private joinWarmup!: JoinWarmupCoordinator<RepairDispatchEntry<R>>;
	private _repairSweepOptimisticGidPeersPending!: Map<
		string,
		Map<string, RepairSweepOptimisticPeerState>
	>;
	private _repairSweepOptimisticGidsByPeer!: Map<string, Set<string>>;
	private _entryKnownPeers!: Map<string, Set<string>>;
	private _entryKnownPeerObservedAt!: Map<string, Map<string, number>>;
	private _entryKnownPeerObservedAtSweptAt = 0;
	private _joinAuthoritativeRepairTimersByDelay!: Map<
		number,
		ReturnType<typeof setTimeout>
	>;
	private _joinAuthoritativeRepairPeersByDelay!: Map<number, Set<string>>;
	private _assumeSyncedRepairSuppressedUntil!: number;
	private _appendBackfillTimer?: ReturnType<typeof setTimeout>;
	private _appendBackfillPendingByTarget!: Map<
		string,
		Map<string, EntryReplicated<R>>
	>;
	private _repairMetrics!: RepairMetrics;
	private _topicSubscribersCache!: Map<
		string,
		{ expiresAt: number; keys: PublicSignKey[] }
	>;
	private _localReachablePeerHashesCache!: Map<
		string,
		{
			expiresAt: number;
			hashes?: string[];
			inFlight?: Promise<string[]>;
		}
	>;
	private _leaderSelectionContextCache?: {
		expiresAt: number;
		context: LeaderSelectionContext;
	};
	private _leaderPlanCache!: LeaderPlanCache;
	// Sync capability bits advertised by peers (SyncCapabilitiesMessage), keyed
	// by public key hash. Entries are dropped on unsubscribe/disconnect.
	private _peerSyncCapabilities!: Map<string, number>;
	// Signed transport session carried by the capability envelope. Kept in a
	// parallel map so existing capability-number consumers remain unchanged.
	private _peerSyncCapabilitySessions!: Map<string, bigint>;
	private _peerSyncCapabilityTimestamps!: Map<string, bigint>;
	// design-note: these fields cache a stable, public diagnostics token for the
	// composite of PeerSession identity, receive epoch, and signed capability
	// session. They are not consulted to admit or fence asynchronous work. A
	// separate opaque token is necessary because exposing any of those internal
	// identities would leak protocol/session values, while PeerSession alone does
	// not change when receive or capability state is replaced.
	private _persistedReceiptReadinessGenerations!: WeakMap<
		PeerSession,
		{
			receiveEpoch: object | null;
			capabilitySession?: bigint;
			generation: string;
		}
	>;
	private _persistedReceiptReadinessGenerationPrefix!: string;
	private _persistedReceiptReadinessGenerationCounter!: number;
	private _persistedReceiptReadinessWaiters!: Set<object>;
	private _persistedReceiptStorage?: PersistedReceiptStorage;
	private _persistedReceiptRequestsInFlight!: Map<string, number>;
	private _persistedReceiptRequestsInFlightTotal!: number;
	// Pending live raw exchange-head gossip, coalesced per recipient set and
	// flushed at the end of the current event-loop turn (or when a batch cap
	// is hit). Only used when every recipient advertised raw capability.
	private _liveRawGossipBatches!: Map<string, LiveRawGossipBatch>;
	private _liveRawGossipFlushScheduled!: boolean;

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

	private createJoinWarmupCoordinator(): JoinWarmupCoordinator<
		RepairDispatchEntry<R>
	> {
		return new JoinWarmupCoordinator<RepairDispatchEntry<R>>({
			isLifecycleActive: (controller) =>
				this.isRepairLifecycleActive(controller),
			getCurrentLifecycleController: () =>
				this._instanceLifecycle
					?.ownershipLifecycleController as AbortController,
			getRepairRetryTimers: () => this._repairRetryTimers,
			isClosed: () => this.closed,
			onTargetCancelled: (target) => {
				const pendingWarmupPeers =
					this._repairSweepPendingPeersByMode.get("join-warmup");
				pendingWarmupPeers?.delete(target);
				if (pendingWarmupPeers?.size === 0) {
					this._repairSweepPendingModes.delete("join-warmup");
				}
				this.clearRepairSweepOptimisticPeer(target);
			},
			bumpSimpleFallbackPasses: () => {
				this._repairMetrics["join-warmup"].simpleFallbackPasses += 1;
			},
			sendEntriesSimple: (target, entries, options) =>
				this.sendRepairEntriesWithTransport(target, entries, "simple", options),
			logError: (error) => logger.error(error),
		});
	}

	private createReplicationAnnouncementCoordinator(): ReplicationAnnouncementCoordinator {
		return new ReplicationAnnouncementCoordinator({
			enqueueReplicationInfoV2: (message) => this._v2Send.enqueue(message),
			captureReplicationOwnershipLifecycle: () =>
				this.captureReplicationOwnershipLifecycle(),
			throwIfReplicationOwnershipLifecycleInactive: (controller) =>
				this.throwIfReplicationOwnershipLifecycleInactive(controller),
		});
	}

	private createReplicationInfoV2SendCoordinator(): ReplicationInfoV2SendCoordinator<R> {
		return new ReplicationInfoV2SendCoordinator<R>({
			getRpc: () => this.rpc,
			getSelfKey: () => this.node.identity.publicKey,
			getSenderTransportSession: () => this.ownTransportSession(),
			getMyReplicationSegments: () => this.getMyReplicationSegments(),
			validatePersistedReplicationRangeSnapshot: (ranges) =>
				this.validatePersistedReplicationRangeSnapshot(ranges),
			isClosed: () => this.closed,
			isPeerSessionCurrent: (peerHash, peerSession) =>
				this._peerSessions.isCurrent(peerHash, peerSession),
			isPeerSessionOpen: (peerHash, peerSession) =>
				this._peerSessions.isCurrent(peerHash, peerSession) &&
				(peerSession as PeerSession).phase === "open" &&
				!this._peerSessions.isReplicationInfoBlocked(peerHash) &&
				this._peerSessions.isReceiveCleanupGateOpen(peerHash),
			captureReplicationOwnershipLifecycle: () =>
				this.captureReplicationOwnershipLifecycle(),
			isReplicationOwnershipLifecycleActive: (controller) =>
				this.isRepairLifecycleActive(controller),
			supportsApplicationConfirmation: (peerHash, receiverTransportSession) =>
				this._peerSyncCapabilitySessions.get(peerHash) ===
					receiverTransportSession &&
				((this._peerSyncCapabilities.get(peerHash) ?? 0) &
					SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM) !==
					0,
		});
	}

	private resolvePersistedReceiptStorage():
		| PersistedReceiptStorage
		| undefined {
		if (this.log.appendDurability !== "strict") {
			return undefined;
		}
		const block = this.remoteBlocks.crashSafeDurability;
		const lower = this.log.entryIndex.properties.index.crashSafeDurability;
		const coordinate = this.entryCoordinatesIndex.crashSafeDurability;
		if (!block || !lower || !coordinate) {
			return undefined;
		}
		return { block, lower, coordinate };
	}

	private ownTransportSession(): bigint {
		return BigInt(
			(this.node.services.pubsub as unknown as { session: number | bigint })
				.session,
		);
	}

	private replicationInfoV2ReceiveCapabilities(): number {
		return (
			SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
			SYNC_CAPABILITY_REPLICATION_INFO_V2_SEND |
			SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY |
			SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM |
			(this._persistedReceiptStorage
				? SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS
				: 0) |
			(this._logProperties?.sync?.rawExchangeHeads === true
				? SYNC_CAPABILITY_RAW_EXCHANGE_HEADS
				: 0)
		);
	}

	private async advertiseReplicationInfoV2ReceiveCapability(properties: {
		target: PublicSignKey;
		peerSession: PeerSession;
		receiveEpoch: object | null;
		signal: AbortSignal;
	}): Promise<
		{ receiverTransportSession: bigint; requestNotBeforeMs: number } | undefined
	> {
		const peerHash = properties.target.hashcode();
		const receiverTransportSession = this.ownTransportSession();
		await this.rpc.send(
			new SyncCapabilitiesMessage({
				capabilities: this.replicationInfoV2ReceiveCapabilities(),
			}),
			{
				mode: new AcknowledgeDelivery({
					redundancy: 1,
					to: [properties.target],
				}),
				priority: CONVERGENCE_MESSAGE_PRIORITY,
				signal: properties.signal,
			},
		);
		if (
			properties.signal.aborted ||
			this.closed ||
			!this._peerSessions.isCurrent(peerHash, properties.peerSession) ||
			properties.peerSession.phase !== "open" ||
			!properties.peerSession.isActive() ||
			!this._peerSessions.isReceiveEpochCurrent(
				peerHash,
				properties.receiveEpoch,
			) ||
			this._peerSessions.isReplicationInfoBlocked(peerHash) ||
			!this._peerSessions.isReceiveCleanupGateOpen(peerHash) ||
			this.ownTransportSession() !== receiverTransportSession
		) {
			return undefined;
		}
		return {
			receiverTransportSession,
			requestNotBeforeMs: Date.now(),
		};
	}

	private createReplicationInfoV2ReceiveCoordinator(): ReplicationInfoV2ReceiveCoordinator {
		return new ReplicationInfoV2ReceiveCoordinator({
			getSelfKey: () => this.node.identity.publicKey,
			getReceiverTransportSession: () => this.ownTransportSession(),
			isClosed: () => this.closed,
			isPeerSessionCurrent: (peerHash, peerSession) =>
				this._peerSessions.isCurrent(peerHash, peerSession) &&
				(peerSession as PeerSession).phase === "open" &&
				(peerSession as PeerSession).isActive(),
			isReceiveEpochCurrent: (peerHash, receiveEpoch) =>
				this._peerSessions.isReceiveEpochCurrent(peerHash, receiveEpoch),
			isPeerStateCurrent: (peerHash, peerSession, receiveEpoch) =>
				this._peerSessions.isCurrent(peerHash, peerSession) &&
				(peerSession as PeerSession).phase === "open" &&
				(peerSession as PeerSession).isActive() &&
				this._peerSessions.isReceiveEpochCurrent(peerHash, receiveEpoch) &&
				!this._peerSessions.isReplicationInfoBlocked(peerHash) &&
				this._peerSessions.isReceiveCleanupGateOpen(peerHash),
			isSenderTransportSessionCurrent: (peerHash, senderTransportSession) =>
				this._peerSyncCapabilitySessions.get(peerHash) ===
				senderTransportSession,
			sendRequest: async (request, target, signal) => {
				await this.rpc.send(request, {
					mode: new AcknowledgeDelivery({ redundancy: 1, to: [target] }),
					priority: CONVERGENCE_MESSAGE_PRIORITY,
					signal,
				});
			},
			refreshLocalCapability: (properties) =>
				this.advertiseReplicationInfoV2ReceiveCapability({
					target: properties.target,
					peerSession: properties.peerSession as PeerSession,
					receiveEpoch: properties.receiveEpoch,
					signal: properties.signal,
				}),
			onRequestError: (error) => {
				if (
					isNotStartedError(error as Error) ||
					(this.closed && error instanceof AbortError)
				) {
					return;
				}
				logger.trace(
					`Replication-info V2 recovery request failed: ${
						(error as Error)?.message ?? String(error)
					}`,
				);
			},
			onLocalCapabilityError: (error) =>
				this.handleReplicationLifecycleSendError(error),
		});
	}

	private createReplicatorLivenessMonitor(): ReplicatorLivenessMonitor {
		return new ReplicatorLivenessMonitor({
			// Sweep-driven probes and activity marks dispatch through the owner so
			// monitor stubs/spies keep intercepting them.
			probeReplicatorLiveness: (peerHash: string) =>
				this._liveness.probeReplicatorLiveness(peerHash),
			markReplicatorActivity: (peerHash: string, now?: number) =>
				this._liveness.markReplicatorActivity(peerHash, now),
			isClosed: () => this.closed,
			getCloseSignal: () => this._closeController.signal,
			getReplicationLifecycleController: () =>
				this._instanceLifecycle?.membershipLifecycleController,
			isReplicationLifecycleActive: (controller) =>
				this.isReplicationLifecycleActive(controller),
			getSelfHash: () => this.node.identity.publicKey.hashcode(),
			getUniqueReplicators: () => this.uniqueReplicators,
			getPeerSession: (peerHash) => this._peerSessions.current(peerHash),
			isPeerSessionCurrent: (peerHash, session) =>
				this._peerSessions.isCurrent(peerHash, session),
			resolvePublicKeyFromHash: (hash) => this._resolvePublicKeyFromHash(hash),
			removeReplicator: (key, options) => this.removeReplicator(key, options),
			getRpc: () => this.rpc,
			getPendingReplicatorLeaveByPeer: () => this._pendingReplicatorLeaveByPeer,
			dispatchReplicatorLeave: (publicKey) => {
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
						detail: { publicKey },
					}),
				);
			},
			isBlockedPeer: (hash) =>
				this._peerSessions.isReplicationInfoBlocked(hash),
			scheduleReplicationInfoRequests: (peer, replicationLifecycleController) =>
				this.scheduleReplicationInfoRequests(
					peer,
					replicationLifecycleController,
				),
			getTopicSubscribers: (topic) => this._getTopicSubscribers(topic),
			confirmReplicatorSubscriberPresence: (peerHash) =>
				this._liveness.confirmReplicatorSubscriberPresence(peerHash),
			getNode: () => this.node,
			getWaitForReplicatorTimeout: () => this.waitForReplicatorTimeout,
		});
	}

	private createPeerSessionRegistry(): PeerSessionRegistry {
		return new PeerSessionRegistry({
			// Delegators, not bound refs — instance spies must keep observing.
			isReplicationLifecycleActive: (controller) =>
				this.isReplicationLifecycleActive(controller),
			getReplicationLifecycleController: () =>
				this._instanceLifecycle?.membershipLifecycleController,
		});
	}

	private createInstanceLifecycle(): InstanceLifecycle {
		return new InstanceLifecycle({
			getCurrentLifecycle: () => this._instanceLifecycle,
			getCloseController: () => this._closeController,
			getPoisonFailure: () => this._replicationRangeMutationFailure,
			isHostClosed: () => this.closed,
			isHostTerminating: () => this.isTerminating(),
			getCheckedPruneCoordinator: () => this._checkedPrune,
			areRangeMutationsClosing: () => this._replicationRangeMutationsClosing,
			arePruneRemovesClosing: () => this._pruneRemovesClosing,
			getPruneDebouncer: () => this.pruneDebouncedFn,
			getReplicationChangeDebouncer: () => this.replicationChangeDebounceFn,
			getRebalanceDebouncer: () => this.rebalanceParticipationDebounced,
		});
	}

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.ensureNativeDurabilityRuntimeState();
		this.log = new Log(properties);
		this.rpc = new RPC();
		this._checkedPrune = new CheckedPruneCoordinator<T, R>();
		this._checkedPruneAuditTimer = undefined;
		this._admittedPruneRemoves = new Set();
		this._pendingIHave = new Map();
		this._pendingIHaveCallbacks = new Set();
		this._replicationInfoRequestByPeer = new Map();
		this._subscriberSnapshotRequestsByPeer = new Map();
		this._replicationInfoApplyQueueByPeer = new Map();
		// The registry constructor runs resetForOpen(), which creates the
		// replication-info blocked set (fence B5) alongside the session maps —
		// the legacy inline `new Set()` that sat above moved there.
		this._peerSessions = this.createPeerSessionRegistry();
		this._pendingReplicatorLeaveByPeer = new Set();
		this._activeReceiveHandlersByPeer = new Map();
		this._receiveHandlerDrainByPeer = new Map();
		this._openingSyncCapabilitiesByPeer = new Map();
		this._gidPeersHistory = new Map();
		this.resetGidPeerHistoryCleanupState();
		this._repairRetryTimers = new Set();
		this._recentRepairDispatch = new Map();
		this._repairSweepRunning = false;
		this._repairSweepPendingModes = new Set();
		this._repairSweepPendingPeersByMode = createRepairPendingPeersByMode();
		this._repairFrontierByMode = createRepairFrontierByMode() as Map<
			RepairDispatchMode,
			Map<string, Map<string, RepairDispatchEntry<R>>>
		>;
		this._repairFrontierActiveTargetsByMode = createRepairActiveTargetsByMode();
		this._repairFrontierBypassKnownPeersByMode =
			createRepairFrontierBypassKnownPeersByMode();
		this.joinWarmup = this.createJoinWarmupCoordinator();
		this._repairSweepOptimisticGidPeersPending = new Map();
		this._repairSweepOptimisticGidsByPeer = new Map();
		this._entryKnownPeers = new Map();
		this._entryKnownPeerObservedAt = new Map();
		this._entryKnownPeerObservedAtSweptAt = 0;
		this._joinAuthoritativeRepairTimersByDelay = new Map();
		this._joinAuthoritativeRepairPeersByDelay = new Map();
		this._appendBackfillPendingByTarget = new Map();
		this._topicSubscribersCache = new Map();
		this._localReachablePeerHashesCache = new Map();
		this._peerSyncCapabilities = new Map();
		this._peerSyncCapabilitySessions = new Map();
		this._peerSyncCapabilityTimestamps = new Map();
		this._persistedReceiptReadinessGenerations = new WeakMap();
		this._persistedReceiptReadinessGenerationPrefix = toHexString(
			randomBytes(8),
		);
		this._persistedReceiptReadinessGenerationCounter = 0;
		this._persistedReceiptReadinessWaiters = new Set();
		this._persistedReceiptStorage = undefined;
		this._persistedReceiptRequestsInFlight = new Map();
		this._persistedReceiptRequestsInFlightTotal = 0;
		this._liveRawGossipBatches = new Map();
		this._liveRawGossipFlushScheduled = false;
		this.coordinateToHash = new Cache<string>({ max: 1e6, ttl: 1e4 });
		this.recentlyRebalanced = new Cache<string>({ max: 1e4, ttl: 1e5 });
		this.uniqueReplicators = new Set();
		this._replicatorJoinEmitted = new Set();
		this._liveness = this.createReplicatorLivenessMonitor();
		this._v2Receive = this.createReplicationInfoV2ReceiveCoordinator();
		this._v2Send = this.createReplicationInfoV2SendCoordinator();
		this._announcements = this.createReplicationAnnouncementCoordinator();
		this.pendingMaturity = new Map();
		this._closeController = new AbortController();
		this._instanceLifecycle = this.createInstanceLifecycle();
	}

	// Every dep is a late-bound closure into the live host: the native
	// runtimes, the coordinate index, remoteBlocks, coordinateToHash,
	// timeUntilRoleMaturity, and the durability/drop flags are all
	// re-assigned across open/close/drop cycles, so nothing is captured by
	// value; the ownership-lifecycle helpers stay host methods (sinon spies
	// and the InstanceLifecycle fold depend on them).
	private createCoordinatePersistenceCoordinator(): CoordinatePersistenceCoordinator<R> {
		return new CoordinatePersistenceCoordinator<R>({
			// Owner-routed for stub visibility (see the deps doc comment).
			canUseNativeBackboneResidentCoordinateState: () =>
				this._coordinates.canUseNativeBackboneResidentCoordinateState(),
			host: () => this,
			nativeBackbone: () => this._nativeBackbone,
			nativeRangePlanner: () => this._nativeRangePlanner,
			nativeSharedLogState: () => this._nativeSharedLogState,
			entryCoordinatesIndex: () => this.entryCoordinatesIndex,
			log: () => this.log,
			remoteBlocks: () => this.remoteBlocks,
			domain: () => this.domain,
			indexableDomain: () => this.indexableDomain,
			coordinateToHash: () => this.coordinateToHash,
			timeUntilRoleMaturity: () => this.timeUntilRoleMaturity,
			getEntryGid: (entry) => this.getEntryGid(entry),
			getEntryNext: (entry) => this.getEntryNext(entry),
			getEntryHashNumber: (entry) => this.getEntryHashNumber(entry),
			canPlanNativeHashGid: (entry) => this.canPlanNativeHashGid(entry),
			hasCustomFindLeaders: () => this.hasCustomFindLeaders(),
			captureReplicationOwnershipLifecycle: () =>
				this.captureReplicationOwnershipLifecycle(),
			throwIfReplicationOwnershipLifecycleInactive: (controller) =>
				this.throwIfReplicationOwnershipLifecycleInactive(controller),
			throwIfReplicationOwnershipPoisoned: () =>
				this.throwIfReplicationOwnershipPoisoned(),
			isDropStarted: () => this._nativeBackboneDropStarted,
			getDurableCommitFailure: () => this._nativeDurableCommitFailure,
			isDurableRecoveryReadyForReopen: () =>
				this._nativeDurableRecoveryReadyForReopen,
			setDurableRecoveryReadyForReopen: (value) => {
				this._nativeDurableRecoveryReadyForReopen = value;
			},
		});
	}

	private ensureNativeDurabilityRuntimeState(): void {
		// Program clones are borsh-created without running class field initializers.
		// Keep recovery state from an existing generation, while supplying fresh
		// defaults only when the runtime-only fields are absent.
		this._nativeDurableRecoveryReadyForReopen ??= false;
		this._nativeDurableRecoveryCids ??= new Set();
		this._nativeBackboneDropStarted ??= false;
		// Constructing the coordinator supplies the same fresh defaults the
		// legacy field initializers did (`journal last-flush ??= 0` included):
		// coordinator instances are always `new`ed, never borsh-cloned.
		this._coordinates ??= this.createCoordinatePersistenceCoordinator();
		this._nativeStrictDurableDocumentRecoveryDeferred ??= false;
		this._nativeStrictDurableTransactionsClosing ??= false;
	}

	get isAdaptiveReplicating() {
		return this._isAdaptiveReplicating;
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
				void this._onFanoutUnicast(detail).catch((error) =>
					logger.error(error),
				);
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

	private ensureLogProviderHandle(fanoutService: FanoutTree): void {
		if (this._providerHandle || this._closeController.signal.aborted) return;
		this._providerHandle = fanoutService.provide(`shared-log|${this.topic}`, {
			ttlMs: 120_000,
			announceIntervalMs: 60_000,
		});
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

		if (
			!(
				message instanceof ExchangeHeadsMessage ||
				message instanceof RawExchangeHeadsMessage
			)
		) {
			return;
		}

		const from =
			(await this._resolvePublicKeyFromHash(envelope.from)) ??
			({ hashcode: () => envelope.from } as PublicSignKey);

		const contextMessage = new DataMessage({
			header: new MessageHeader({
				session: 0,
				mode: new AnyWhere(),
				priority: BACKGROUND_MESSAGE_PRIORITY,
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
				priority: BACKGROUND_MESSAGE_PRIORITY,
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
		const requireRecipients =
			reliability === "persisted" || delivery.requireRecipients === true;
		const minAcks =
			delivery.minAcks != null && Number.isFinite(delivery.minAcks)
				? Math.max(0, Math.floor(delivery.minAcks))
				: undefined;
		if (reliability === "persisted") {
			if (
				delivery.minAcks == null ||
				!Number.isSafeInteger(delivery.minAcks) ||
				delivery.minAcks <= 0
			) {
				throw new Error(
					'persisted delivery requires a positive explicit "minAcks"',
				);
			}
		}

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

	private validatePersistedReceiptRequestShape(
		request: RequestPersistedEntriesV1,
	): void {
		if (
			request.hashes.length === 0 ||
			request.hashes.length > MAX_PERSISTED_RECEIPT_HASHES
		) {
			throw new Error(
				`Persisted receipt requests require 1-${MAX_PERSISTED_RECEIPT_HASHES} hashes`,
			);
		}
		let bytes = 0;
		const encoder = new TextEncoder();
		for (const hash of request.hashes) {
			if (hash.length === 0 || hash.length > MAX_PERSISTED_RECEIPT_HASH_BYTES) {
				throw new Error("Invalid persisted receipt hash batch");
			}
			bytes += encoder.encode(hash).byteLength;
			if (bytes > MAX_PERSISTED_RECEIPT_HASH_BYTES) {
				throw new Error("Invalid persisted receipt hash batch");
			}
		}
	}

	private hasValidPersistedReceiptHashes(
		request: RequestPersistedEntriesV1,
	): boolean {
		const seen = new Set<string>();
		try {
			for (const hash of request.hashes) {
				if (seen.has(hash)) return false;
				cidifyString(hash);
				seen.add(hash);
			}
		} catch {
			return false;
		}
		return true;
	}

	private snapshotDeliveryOptions(
		deliveryArgument: DeliveryOptions,
		reliability: DeliveryReliability | undefined,
	): Readonly<DeliveryOptions> {
		// Keep the selected AbortSignal object live: aborting it remains effective,
		// while reassigning any caller field cannot change this invocation.
		return Object.freeze({
			reliability,
			minAcks: deliveryArgument.minAcks,
			requireRecipients: deliveryArgument.requireRecipients,
			priority: deliveryArgument.priority,
			timeout: deliveryArgument.timeout,
			signal: deliveryArgument.signal,
		});
	}

	private snapshotAppendTrim(
		trim: NonNullable<AppendOptions<T>["trim"]>,
	): NonNullable<AppendOptions<T>["trim"]> {
		const type = trim.type;
		const filterArgument = trim.filter;
		const filter = filterArgument
			? Object.freeze({
					canTrim: filterArgument.canTrim,
					cacheId: filterArgument.cacheId,
				})
			: undefined;
		if (type === "time") {
			return Object.freeze({ type, maxAge: trim.maxAge, filter });
		}
		if (type === "length" || type === "bytelength") {
			return Object.freeze({
				type,
				to: trim.to,
				from: trim.from,
				filter,
			});
		}
		throw new Error("Unsupported append trim type");
	}

	private snapshotAppendEncryption(
		encryption: NonNullable<AppendOptions<T>["encryption"]>,
	): NonNullable<AppendOptions<T>["encryption"]> {
		return asTrustedLowerLog(this.log).snapshotAppendEncryptionForTrustedCaller(
			encryption,
		);
	}

	private captureFullAppendNextStorageReader(
		next: Entry<any> | ShallowEntry,
	): (() => Uint8Array) | undefined {
		if (next instanceof Entry) {
			const getStorageBytes = next.getStorageBytes;
			return () => Reflect.apply(getStorageBytes, next, []);
		}
		const candidate = next as Partial<Entry<any>>;
		const init = candidate.init;
		const getNext = candidate.getNext;
		const getClock = candidate.getClock;
		const getStorageBytes = candidate.getStorageBytes;
		const verifySignatures = candidate.verifySignatures;
		if (
			typeof init !== "function" ||
			typeof getNext !== "function" ||
			typeof getClock !== "function" ||
			typeof getStorageBytes !== "function" ||
			typeof verifySignatures !== "function"
		) {
			return;
		}
		return () => Reflect.apply(getStorageBytes, next, []);
	}

	private snapshotFullAppendNext(
		next: Entry<any>,
		getStorageBytes: () => Uint8Array,
		canonicalHash: string,
	): Entry<any> | undefined {
		let entrySize: number | undefined;
		try {
			entrySize = next.size;
		} catch {
			// A valid but not-yet-sized entry still has exact owned storage bytes below.
		}
		const createdLocally = next.createdLocally;
		let sourceBytes = Entry.getPreparedStorageBytes(next);
		if (!sourceBytes) {
			try {
				sourceBytes = getStorageBytes();
			} catch {
				// Native commit-only entries intentionally keep payload/signature bytes
				// outside the JS Entry. They are valid sortable parents but cannot be
				// imported into another log as full blocks, so snapshot them below as
				// shallow canonical references instead.
				return;
			}
		}
		const bytes = Uint8Array.from(sourceBytes);
		const captured = deserialize(bytes, Entry) as Entry<any>;
		const decodedHash = captured.hash;
		if (decodedHash && decodedHash !== canonicalHash) {
			throw new Error(
				`Explicit append next bytes did not match captured hash ${canonicalHash}`,
			);
		}
		captured.hash = "";
		Entry.prepareMultihashBytes(captured, bytes, canonicalHash);
		captured.hash = canonicalHash;
		captured.size = entrySize ?? bytes.byteLength;
		captured.createdLocally = createdLocally;
		captured.init({
			encoding: this.log.encoding,
			keychain: this.log.keychain,
		});
		return captured;
	}

	private snapshotAppendNext(
		next: Entry<any> | ShallowEntry,
	): Entry<any> | ShallowEntry {
		const hash = next.hash;
		if (!hash) {
			throw new Error("Explicit append next requires a canonical hash");
		}
		const getStorageBytes = this.captureFullAppendNextStorageReader(next);
		if (getStorageBytes) {
			const captured = this.snapshotFullAppendNext(
				next as Entry<any>,
				getStorageBytes,
				hash,
			);
			if (captured) return captured;
		}
		const meta = next.meta;
		const gid = meta.gid;
		const nextHashes = [...meta.next];
		const type = meta.type;
		const sourceData = meta.data;
		const data = sourceData && Uint8Array.from(sourceData);
		const clock = meta.clock;
		const clockId = Uint8Array.from(clock.id);
		const sourceTimestamp = clock.timestamp;
		const timestamp = new Timestamp({
			wallTime: sourceTimestamp.wallTime,
			logical: sourceTimestamp.logical,
		});
		const candidate = next as ShallowEntry & { payloadByteLength?: number };
		let payloadSize = 0;
		try {
			const shallowPayloadSize = candidate.payloadSize;
			if (typeof shallowPayloadSize === "number") {
				payloadSize = shallowPayloadSize;
			} else {
				const fullPayloadSize = candidate.payloadByteLength;
				if (typeof fullPayloadSize === "number") payloadSize = fullPayloadSize;
			}
		} catch {
			// Hollow native parents need only their canonical sorting/link facts.
		}
		const candidateHead = candidate.head;
		const head = typeof candidateHead === "boolean" ? candidateHead : true;
		const captured = new ShallowEntry({
			hash,
			payloadSize,
			head,
			meta: new ShallowMeta({
				gid,
				next: nextHashes,
				type,
				data,
				clock: new LamportClock({ id: clockId, timestamp }),
			}),
		});
		Object.freeze(captured.meta.next);
		Object.freeze(captured.meta.clock.timestamp);
		Object.freeze(captured.meta.clock);
		Object.freeze(captured.meta);
		return Object.freeze(captured);
	}

	private snapshotSupportedAppendOptions(
		options: DocumentSharedAppendOptions<T>,
		delivery: SharedAppendOptions<T>["delivery"],
		includeDocumentShape: boolean,
		validateCapturedOptions?: (options: DocumentSharedAppendOptions<T>) => void,
	): DocumentSharedAppendOptions<T> {
		// Copy the finite public option surface, not arbitrary enumerable caller
		// properties. This pins every value that the async append/document paths
		// reread while avoiding surprising evaluation of unrelated custom getters.
		const captured = Object.create(null) as DocumentSharedAppendOptions<T>;
		const durability = options.durability;
		const deferIndexWrite = options.deferIndexWrite;
		const meta = options.meta;
		const identity = options.identity;
		const signers = options.signers;
		const trim = options.trim;
		const encryption = options.encryption;
		const onChange = options.onChange;
		const canAppend = options.canAppend;
		const replicas = options.replicas;
		const replicate = options.replicate;
		const target = options.target;
		const unique = includeDocumentShape ? options.unique : undefined;
		const checkRemote = includeDocumentShape ? options.checkRemote : undefined;

		if (durability !== undefined) captured.durability = durability;
		if (deferIndexWrite !== undefined) {
			captured.deferIndexWrite = deferIndexWrite;
		}
		if (meta !== undefined) {
			const capturedMetaInput = Object.create(null) as NonNullable<
				SharedAppendOptions<T>["meta"]
			>;
			const type = meta.type;
			const gidSeed = meta.gidSeed;
			const hasData = "data" in meta;
			const data = meta.data;
			const timestamp = meta.timestamp;
			const next = meta.next;
			if (type !== undefined) capturedMetaInput.type = type;
			if (gidSeed !== undefined) capturedMetaInput.gidSeed = gidSeed;
			if (hasData) capturedMetaInput.data = data;
			if (timestamp !== undefined) capturedMetaInput.timestamp = timestamp;
			if (next !== undefined) capturedMetaInput.next = next;
			captured.meta = capturedMetaInput;
		}
		if (identity !== undefined) captured.identity = identity;
		if (signers !== undefined) captured.signers = signers;
		if (trim !== undefined) captured.trim = trim;
		if (encryption !== undefined) captured.encryption = encryption;
		if (onChange !== undefined) captured.onChange = onChange;
		if (canAppend !== undefined) captured.canAppend = canAppend;
		if (replicas !== undefined) captured.replicas = replicas;
		if (replicate !== undefined) captured.replicate = replicate;
		if (target !== undefined) captured.target = target;
		if (delivery !== undefined) captured.delivery = delivery;
		if (includeDocumentShape) {
			if (unique !== undefined) captured.unique = unique;
			if (checkRemote !== undefined) captured.checkRemote = checkRemote;
		}
		// Strict-native Documents validates the already-captured finite surface
		// before any accepted nested value is cloned or normalized. This keeps its
		// mode error stable even for malformed unsupported option values, without
		// rereading caller-owned top-level fields.
		validateCapturedOptions?.(captured);
		if (meta !== undefined) {
			const capturedMetaInput = captured.meta!;
			const capturedMeta = Object.create(null) as NonNullable<
				SharedAppendOptions<T>["meta"]
			>;
			if (capturedMetaInput.type !== undefined) {
				capturedMeta.type = capturedMetaInput.type;
			}
			if (capturedMetaInput.gidSeed !== undefined) {
				capturedMeta.gidSeed = Uint8Array.from(capturedMetaInput.gidSeed);
			}
			if ("data" in capturedMetaInput) {
				capturedMeta.data =
					capturedMetaInput.data && Uint8Array.from(capturedMetaInput.data);
			}
			if (capturedMetaInput.timestamp !== undefined) {
				capturedMeta.timestamp = capturedMetaInput.timestamp.clone();
			}
			if (capturedMetaInput.next !== undefined) {
				capturedMeta.next = Object.freeze(
					capturedMetaInput.next.map((entry) => this.snapshotAppendNext(entry)),
				) as unknown as Entry<any>[] | ShallowEntry[];
			}
			captured.meta = capturedMeta;
		}
		if (signers !== undefined) captured.signers = [...signers];
		if (trim !== undefined) captured.trim = this.snapshotAppendTrim(trim);
		if (encryption !== undefined) {
			captured.encryption = this.snapshotAppendEncryption(encryption);
		}
		if (replicas !== undefined) {
			captured.replicas =
				typeof replicas === "number"
					? replicas
					: new AbsoluteReplicas(replicas.getValue(this));
		}
		return captured;
	}

	private validatePersistedAppendInvocation(
		capturedOptions: SharedAppendOptions<T>,
		delivery: PersistedDeliveryOptions,
	): void {
		this._parseDeliveryOptions(delivery);
		const target = capturedOptions.target;
		if (target !== undefined && target !== "replicators") {
			throw new Error(
				'persisted delivery requires target="replicators" (or an omitted target)',
			);
		}
		if (
			delivery.timeout != null &&
			(!Number.isFinite(delivery.timeout) ||
				delivery.timeout <= 0 ||
				delivery.timeout > MAX_PERSISTED_DELIVERY_TIMEOUT_MS)
		) {
			throw new Error(
				`persisted delivery timeout must be a positive number no greater than ${MAX_PERSISTED_DELIVERY_TIMEOUT_MS}`,
			);
		}
		if (delivery.signal?.aborted) {
			throw delivery.signal.reason ?? new AbortError();
		}
	}

	private capturePersistedAppendInvocation(
		options?: SharedAppendOptions<T>,
	): PersistedAppendInvocation<T> | undefined {
		const deliveryArgument = options?.delivery;
		if (typeof deliveryArgument !== "object" || deliveryArgument === null) {
			return undefined;
		}
		const reliability = deliveryArgument.reliability;
		if (reliability !== "persisted") {
			return undefined;
		}
		const delivery = this.snapshotDeliveryOptions(
			deliveryArgument,
			reliability,
		) as PersistedDeliveryOptions;
		const capturedOptions = this.snapshotSupportedAppendOptions(
			options!,
			delivery,
			false,
		);
		this.validatePersistedAppendInvocation(capturedOptions, delivery);
		return { options: capturedOptions, delivery };
	}

	private snapshotDocumentAppendOptions(
		options?: DocumentSharedAppendOptions<T>,
		validateCapturedOptions?: (options: DocumentSharedAppendOptions<T>) => void,
	): DocumentSharedAppendOptions<T> | undefined {
		if (!options) return;
		const deliveryArgument = options.delivery;
		let delivery: SharedAppendOptions<T>["delivery"] = deliveryArgument;
		if (typeof deliveryArgument === "object" && deliveryArgument !== null) {
			const reliability = deliveryArgument.reliability;
			delivery = this.snapshotDeliveryOptions(deliveryArgument, reliability);
		}
		const capturedOptions = this.snapshotSupportedAppendOptions(
			options,
			delivery,
			true,
			validateCapturedOptions,
		);
		if (
			typeof delivery === "object" &&
			delivery !== null &&
			delivery.reliability === "persisted"
		) {
			this.validatePersistedAppendInvocation(
				capturedOptions,
				delivery as PersistedDeliveryOptions,
			);
		}
		return capturedOptions;
	}

	private async _getSortedRouteHints(targetHash: string): Promise<RouteHint[]> {
		const pubsub: any = this.node.services.pubsub as any;
		const maybeHints = await pubsub?.getUnifiedRouteHints?.(
			this.topic,
			targetHash,
		);
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
		priority?: number;
		fanoutUnicastOptions?: { timeoutMs?: number; signal?: AbortSignal };
	}): Promise<void> {
		const { peer, message, payload, priority, fanoutUnicastOptions } =
			properties;
		const hints = await this._getSortedRouteHints(peer);
		const hasDirectHint = hints.some(
			(hint) => hint.kind === "directstream-ack",
		);
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
					priority,
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
			priority,
		});
	}

	/** Live append gossip that stayed on the plain TS path (countable in tests). */
	private emitPlainLiveSendProfile(message: ExchangeHeadsMessage<any>): void {
		const profile = this._logProperties?.sync?.profile;
		if (profile) {
			emitSyncProfileEvent(profile, {
				name: "sharedLog.liveSend.plain",
				component: "shared-log",
				entries: message.heads.length,
				messages: 1,
			});
		}
	}

	private peerSupportsRawExchangeHeads(peerHash: string): boolean {
		return (
			((this._peerSyncCapabilities.get(peerHash) ?? 0) &
				SYNC_CAPABILITY_RAW_EXCHANGE_HEADS) !==
			0
		);
	}

	private observePeerSyncCapabilities(properties: {
		peerHash: string;
		capabilities: number;
		transportSession?: bigint;
		timestamp?: bigint;
		openingSession?: PeerSession;
	}): boolean {
		const {
			peerHash,
			capabilities,
			transportSession,
			timestamp,
			openingSession,
		} = properties;
		if (openingSession) {
			const previous = this._openingSyncCapabilitiesByPeer.get(peerHash);
			if (
				transportSession !== undefined &&
				timestamp !== undefined &&
				previous?.epoch === openingSession &&
				previous.timestamp !== undefined &&
				previous.transportSession !== transportSession &&
				timestamp <= previous.timestamp
			) {
				return false;
			}
			if (
				transportSession !== undefined &&
				timestamp !== undefined &&
				previous?.epoch === openingSession &&
				previous.transportSession === transportSession
			) {
				if (
					previous.timestamp !== undefined &&
					timestamp < previous.timestamp
				) {
					return false;
				}
				const nextCapabilities = previous.capabilities | capabilities;
				const nextTimestamp =
					previous.timestamp === undefined || timestamp > previous.timestamp
						? timestamp
						: previous.timestamp;
				this._openingSyncCapabilitiesByPeer.set(peerHash, {
					epoch: openingSession,
					capabilities: nextCapabilities,
					transportSession,
					timestamp: nextTimestamp,
				});
				if (
					previous.capabilities !== nextCapabilities ||
					previous.timestamp === undefined
				) {
					this.dispatchPersistedReceiptReadinessChange(peerHash);
				}
				return true;
			}
			this._openingSyncCapabilitiesByPeer.set(peerHash, {
				epoch: openingSession,
				capabilities,
				transportSession,
				timestamp,
			});
			this.dispatchPersistedReceiptReadinessChange(peerHash);
			return true;
		}

		if (transportSession === undefined || timestamp === undefined) {
			// Test/in-process synthetic contexts predate signed envelope captures.
			// They may exercise capability-number behavior, but can never authorize V2.
			const readinessChanged =
				this._peerSyncCapabilities.get(peerHash) !== capabilities ||
				this._peerSyncCapabilitySessions.has(peerHash) ||
				this._peerSyncCapabilityTimestamps.has(peerHash);
			this._peerSyncCapabilities.set(peerHash, capabilities);
			this._peerSyncCapabilitySessions.delete(peerHash);
			this._peerSyncCapabilityTimestamps.delete(peerHash);
			this._v2Send.advancePeerCapability(peerHash);
			this._v2Receive.revokePeerCapability(peerHash);
			if (readinessChanged) {
				this.dispatchPersistedReceiptReadinessChange(peerHash);
			}
			return true;
		}

		const previousSession = this._peerSyncCapabilitySessions.get(peerHash);
		const previousTimestamp = this._peerSyncCapabilityTimestamps.get(peerHash);
		const sameTransportSession = previousSession === transportSession;
		if (
			previousTimestamp !== undefined &&
			((sameTransportSession && timestamp < previousTimestamp) ||
				(!sameTransportSession && timestamp <= previousTimestamp))
		) {
			return false;
		}
		const previousCapabilities = this._peerSyncCapabilities.get(peerHash) ?? 0;
		const nextCapabilities = sameTransportSession
			? previousCapabilities | capabilities
			: capabilities;
		const senderGrantCapabilityMask =
			SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
			SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY;
		const generationAdvanced =
			!sameTransportSession ||
			(previousCapabilities & senderGrantCapabilityMask) !==
				(nextCapabilities & senderGrantCapabilityMask);
		const readinessChanged =
			!sameTransportSession ||
			previousTimestamp === undefined ||
			previousCapabilities !== nextCapabilities;
		this._peerSyncCapabilities.set(peerHash, nextCapabilities);
		this._peerSyncCapabilitySessions.set(peerHash, transportSession);
		this._peerSyncCapabilityTimestamps.set(
			peerHash,
			previousTimestamp === undefined ||
				!sameTransportSession ||
				timestamp > previousTimestamp
				? timestamp
				: previousTimestamp,
		);
		if (generationAdvanced) {
			this._v2Send.advancePeerCapability(peerHash);
			// A fresh signed capability generation is V2 progress from the peer:
			// recovery re-solicitation may restart from the base interval.
			this.resetReplicationInfoV2RecoveryEscalation(peerHash);
		}
		if (readinessChanged) {
			this.dispatchPersistedReceiptReadinessChange(peerHash);
		}
		return true;
	}

	private promoteReplicationInfoV2ReceiveCapability(
		target: PublicSignKey,
		peerSession: PeerSession,
	): boolean {
		const peerHash = target.hashcode();
		const senderTransportSession =
			this._peerSyncCapabilitySessions.get(peerHash);
		const capabilityTimestamp =
			this._peerSyncCapabilityTimestamps.get(peerHash);
		if (
			senderTransportSession === undefined ||
			capabilityTimestamp === undefined
		) {
			return false;
		}
		return this._v2Receive.observeCapability({
			peerHash,
			target,
			peerSession,
			receiveEpoch: this._peerSessions.receiveEpoch(peerHash),
			capabilities: this._peerSyncCapabilities.get(peerHash) ?? 0,
			senderTransportSession,
			capabilityTimestamp,
		});
	}

	/**
	 * Coalesced targeted subscriber-snapshot request for the
	 * capability-before-Subscribe recovery path. The observed-capability gate
	 * is sender-paced (any advancing timestamp passes), so a burst of frames
	 * from one session-less peer must not fan out into one GetSubscribers
	 * unicast per frame. One request per peer is in flight at a time; once it
	 * settles, a genuinely new session-less capability may request again.
	 */
	private requestSubscriberSnapshotForCapability(target: PublicSignKey): void {
		const peerHash = target.hashcode();
		if (this._subscriberSnapshotRequestsByPeer.has(peerHash)) {
			return;
		}
		const request = Promise.resolve()
			.then(() =>
				this.node.services.pubsub.requestSubscribers(this.topic, target),
			)
			.catch((error) => {
				if (!isNotStartedError(error as Error)) {
					logger.error(error?.toString?.() ?? String(error));
				}
			})
			.finally(() => {
				if (this._subscriberSnapshotRequestsByPeer.get(peerHash) === request) {
					this._subscriberSnapshotRequestsByPeer.delete(peerHash);
				}
			});
		this._subscriberSnapshotRequestsByPeer.set(peerHash, request);
	}

	/**
	 * Live append gossip may use the raw exchange-heads path only when we
	 * opted into raw sync and every remote recipient advertised raw capability
	 * (via {@link SyncCapabilitiesMessage}). Peers that never advertised —
	 * older versions or raw sync disabled — keep receiving the unchanged plain
	 * `ExchangeHeadsMessage` path.
	 */
	private canUseLiveRawGossip(
		to: Iterable<string>,
		selfHash: string,
	): string[] | undefined {
		if (this._logProperties?.sync?.rawExchangeHeads !== true) {
			return undefined;
		}
		const remote: string[] = [];
		for (const peer of to) {
			if (peer === selfHash) {
				continue;
			}
			if (!this.peerSupportsRawExchangeHeads(peer)) {
				return undefined;
			}
			remote.push(peer);
		}
		return remote.length > 0 ? remote : undefined;
	}

	private queueLiveRawGossip(
		hash: string,
		gidRefrences: string[],
		byteLength: number,
		to: string[],
	): void {
		const key = to.length === 1 ? to[0]! : [...to].sort().join("\n");
		let batch = this._liveRawGossipBatches.get(key);
		if (!batch) {
			batch = { to, hashes: [], gidRefrences: [], bytes: 0 };
			this._liveRawGossipBatches.set(key, batch);
		}
		batch.hashes.push(hash);
		batch.gidRefrences.push(gidRefrences);
		batch.bytes += byteLength;
		if (
			batch.hashes.length >= LIVE_RAW_GOSSIP_MAX_ENTRIES ||
			batch.bytes >= LIVE_RAW_GOSSIP_MAX_BYTES
		) {
			this._liveRawGossipBatches.delete(key);
			void this.sendLiveRawGossipBatch(batch);
			return;
		}
		this.scheduleLiveRawGossipFlush();
	}

	private scheduleLiveRawGossipFlush(): void {
		if (this._liveRawGossipFlushScheduled) {
			return;
		}
		this._liveRawGossipFlushScheduled = true;
		const flush = () => {
			this._liveRawGossipFlushScheduled = false;
			this.flushLiveRawGossip();
		};
		// End-of-turn flush: setImmediate on node fires after the current
		// turn's microtasks (so awaited sequential appends coalesce) but
		// before the next turn's timers/IO (so a lone put is not delayed).
		if (typeof setImmediate === "function") {
			setImmediate(flush);
		} else {
			setTimeout(flush, 0);
		}
	}

	private flushLiveRawGossip(): void {
		if (this._liveRawGossipBatches.size === 0) {
			return;
		}
		const batches = [...this._liveRawGossipBatches.values()];
		this._liveRawGossipBatches.clear();
		for (const batch of batches) {
			void this.sendLiveRawGossipBatch(batch);
		}
	}

	private async sendLiveRawGossipBatch(
		batch: LiveRawGossipBatch,
	): Promise<void> {
		try {
			const sentMessages = await this.sendFusedRawExchangeHeadsPlan(
				{ hashes: batch.hashes, gidRefrences: batch.gidRefrences },
				batch.to,
			);
			if (sentMessages !== undefined) {
				return;
			}
			// TS fallback (no native payload encoder or blocks not natively
			// stored): still one batched raw message per size cap.
			for await (const message of createRawExchangeHeadsMessages(
				this.log,
				batch.hashes,
				this._logProperties?.sync?.profile,
			)) {
				await this.rpc.send(message, {
					mode: new SilentDelivery({ redundancy: 1, to: batch.to }),
				});
			}
		} catch (error: any) {
			if (this.closed) {
				return;
			}
			logger.error(error);
		}
	}

	/**
	 * Fused raw exchange-heads send: the full sync payload — PubSubData →
	 * RequestV0 → RawExchangeHeadsMessage including the entry block bytes — is
	 * serialized inside the native-backbone wasm module straight from the
	 * native block store and published pre-encoded, so entry block bytes never
	 * materialize as JS values on the send path. Returns the number of
	 * messages sent, or `undefined` when this path is unavailable (no native
	 * encoder, blocks not natively stored, or no pre-encoded publish support)
	 * so callers fall back to the TS message path.
	 */
	private async sendFusedRawExchangeHeadsPlan(
		plan: RawExchangeHeadSendPlan,
		to: string[] | Set<string>,
		options?: {
			acknowledge?: boolean;
			priority?: number;
			reserved?: Uint8Array;
			signal?: AbortSignal;
		},
	): Promise<number | undefined> {
		const backbone = this._nativeBackbone;
		if (!backbone?.encodeRawExchangeSyncPayload) {
			return undefined;
		}
		const pubsub = this.node.services.pubsub as unknown as {
			publishPreEncodedData?: (
				payload: Uint8Array,
				properties: { topics: string[] },
				options: {
					mode: SilentDelivery | AcknowledgeDelivery;
					priority?: number;
					signal?: AbortSignal;
				},
			) => Promise<Uint8Array | undefined>;
		};
		if (typeof pubsub.publishPreEncodedData !== "function") {
			return undefined;
		}
		if (plan.hashes.length === 0) {
			return 0;
		}
		const byteLengths = backbone.syncSendBlockByteLengths?.(plan.hashes);
		if (!byteLengths) {
			return undefined;
		}

		const topic = this.rpc.topic;
		const payloads: {
			payload: Uint8Array;
			entries: number;
			bytes: number;
		}[] = [];
		const encodeChunk = (
			from: number,
			until: number,
			bytes: number,
		): boolean => {
			const payload = backbone.encodeRawExchangeSyncPayload!({
				topic,
				hashes:
					from === 0 && until === plan.hashes.length
						? plan.hashes
						: plan.hashes.slice(from, until),
				gidRefrences:
					from === 0 && until === plan.gidRefrences.length
						? plan.gidRefrences
						: plan.gidRefrences.slice(from, until),
				reserved: options?.reserved,
			});
			if (!payload) {
				return false;
			}
			payloads.push({ payload, entries: until - from, bytes });
			return true;
		};
		// Same greedy chunking rule as `createRawExchangeHeadsMessages`: close
		// a message after the head that pushes it over the size cap.
		let chunkStart = 0;
		let size = 0;
		let totalBytes = 0;
		for (let i = 0; i < plan.hashes.length; i++) {
			const length = byteLengths[i];
			if (length === undefined) {
				return undefined;
			}
			size += length;
			totalBytes += length;
			if (size > MAX_RAW_EXCHANGE_MESSAGE_SIZE) {
				if (!encodeChunk(chunkStart, i + 1, size)) {
					return undefined;
				}
				chunkStart = i + 1;
				size = 0;
			}
		}
		if (chunkStart < plan.hashes.length) {
			if (!encodeChunk(chunkStart, plan.hashes.length, size)) {
				return undefined;
			}
		}
		// Every payload is encoded before anything is published, so a caller
		// falling back on `undefined` never double-sends part of a plan.
		const profile = this._logProperties?.sync?.profile;
		let attemptedMessages = 0;
		let sentMessages = 0;
		let sentEntries = 0;
		let sentBytes = 0;
		try {
			for (const item of payloads) {
				if (options?.signal?.aborted) {
					break;
				}
				attemptedMessages += 1;
				await pubsub.publishPreEncodedData(
					item.payload,
					{ topics: [topic] },
					{
						mode: options?.acknowledge
							? new AcknowledgeDelivery({ redundancy: 1, to: [...to] })
							: new SilentDelivery({ redundancy: 1, to: [...to] }),
						priority: options?.priority,
						signal: options?.signal,
					},
				);
				sentMessages += 1;
				sentEntries += item.entries;
				sentBytes += item.bytes;
			}
		} finally {
			if (profile) {
				emitSyncProfileEvent(profile, {
					name: "sharedLog.rawSend.fused",
					component: "shared-log",
					entries: sentEntries,
					bytes: sentBytes,
					messages: sentMessages,
					details: {
						attemptedMessages,
						cancelled: options?.signal?.aborted || undefined,
						plannedBytes: totalBytes,
						plannedEntries: plan.hashes.length,
						plannedMessages: payloads.length,
					},
				});
			}
		}
		return sentMessages;
	}

	/**
	 * `RawExchangeHeadsSender` seam handed to the synchronizer for bulk sync
	 * responses: resolves the head/reference plan like the TS raw path and
	 * ships it fused when possible.
	 */
	private async trySendFusedRawExchangeHeads(
		hashes: string[],
		to: string[],
		options?: {
			acknowledge?: boolean;
			priority?: number;
			reserved?: Uint8Array;
			signal?: AbortSignal;
		},
	): Promise<number | undefined> {
		if (!this._nativeBackbone?.encodeRawExchangeSyncPayload) {
			return undefined;
		}
		const plan = collectRawExchangeHeadSendPlan(this.log, hashes);
		if (!plan) {
			return undefined;
		}
		if (plan.hashes.length === 0) {
			return 0;
		}
		return this.sendFusedRawExchangeHeadsPlan(plan, to, options);
	}

	private persistedReceiptReadinessGeneration(
		peerSession: PeerSession,
		receiveEpoch: object | null,
		capabilitySession: bigint | undefined,
	): string {
		const current = this._persistedReceiptReadinessGenerations.get(peerSession);
		if (
			current?.receiveEpoch === receiveEpoch &&
			current.capabilitySession === capabilitySession
		) {
			return current.generation;
		}
		const generation = `${this._persistedReceiptReadinessGenerationPrefix}:${(++this
			._persistedReceiptReadinessGenerationCounter).toString(36)}`;
		this._persistedReceiptReadinessGenerations.set(peerSession, {
			receiveEpoch,
			capabilitySession,
			generation,
		});
		return generation;
	}

	private pendingPersistedReceiptReadiness(
		reason: PersistedReceiptPeerReadinessPendingReason,
		generation?: string,
	): PersistedReceiptPeerReadiness {
		return Object.freeze({
			status: "pending" as const,
			reason,
			...(generation === undefined ? {} : { generation }),
		});
	}

	private unsupportedPersistedReceiptReadiness(
		reason: PersistedReceiptPeerReadinessUnsupportedReason,
		generation: string,
	): PersistedReceiptPeerReadiness {
		return Object.freeze({
			status: "unsupported" as const,
			reason,
			generation,
		});
	}

	private dispatchPersistedReceiptReadinessChange(peerHash: string): void {
		this.events.dispatchEvent(
			new CustomEvent<PersistedReceiptPeerReadinessEvent>(
				"persisted-receipt:readiness",
				{ detail: Object.freeze({ peerHash }) },
			),
		);
	}

	private persistedReceiptReadinessCandidate(peerHash: string):
		| {
				capabilitySession: bigint;
				peerSession: PeerSession;
				receiveEpoch: object | null;
				generation: string;
		  }
		| PersistedReceiptPeerReadiness {
		if (this.closed) {
			return this.pendingPersistedReceiptReadiness("closed");
		}
		const peerSession = this._peerSessions.current(peerHash);
		if (!peerSession) {
			return this.pendingPersistedReceiptReadiness("no-current-session");
		}
		const receiveEpoch = this._peerSessions.receiveEpoch(peerHash);
		const capabilitySession = this._peerSyncCapabilitySessions.get(peerHash);
		const generation = this.persistedReceiptReadinessGeneration(
			peerSession,
			receiveEpoch,
			capabilitySession,
		);
		if (
			peerSession.phase !== "open" ||
			!peerSession.isActive() ||
			this._peerSessions.isReplicationInfoBlocked(peerHash) ||
			!this._peerSessions.isReceiveCleanupGateOpen(peerHash)
		) {
			return this.pendingPersistedReceiptReadiness(
				"session-opening",
				generation,
			);
		}
		if (
			capabilitySession === undefined ||
			!this._peerSyncCapabilityTimestamps.has(peerHash)
		) {
			return this.pendingPersistedReceiptReadiness(
				"capability-pending",
				generation,
			);
		}
		const capabilities = this._peerSyncCapabilities.get(peerHash) ?? 0;
		if ((capabilities & SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS) === 0) {
			return this.unsupportedPersistedReceiptReadiness(
				"persisted-receipts-unsupported",
				generation,
			);
		}
		if ((capabilities & SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM) === 0) {
			return this.unsupportedPersistedReceiptReadiness(
				"replication-confirmation-unsupported",
				generation,
			);
		}
		if (
			!this._v2Receive.isCurrentActive({
				peerHash,
				peerSession,
				receiveEpoch,
				senderTransportSession: capabilitySession,
			})
		) {
			return this.pendingPersistedReceiptReadiness(
				"replication-state-pending",
				generation,
			);
		}
		if (!this.uniqueReplicators.has(peerHash)) {
			return this.pendingPersistedReceiptReadiness(
				"not-replicating",
				generation,
			);
		}
		return {
			capabilitySession,
			peerSession,
			receiveEpoch,
			generation,
		};
	}

	private persistedReceiptPeerSession(
		peerHash: string,
	): { capabilitySession: bigint; peerSession: PeerSession } | undefined {
		// This is a hot receipt/transfer-loop predicate. Keep it allocation-light,
		// while mirroring every exact-session gate in
		// persistedReceiptReadinessCandidate (which additionally creates public
		// reason/generation snapshots).
		const capabilitySession = this._peerSyncCapabilitySessions.get(peerHash);
		const peerSession = this._peerSessions.current(peerHash);
		const receiveEpoch = this._peerSessions.receiveEpoch(peerHash);
		const requiredCapabilities =
			SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS |
			SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM;
		if (
			this.closed ||
			capabilitySession == null ||
			!peerSession ||
			peerSession.phase !== "open" ||
			!peerSession.isActive() ||
			this._peerSessions.isReplicationInfoBlocked(peerHash) ||
			!this._peerSessions.isReceiveCleanupGateOpen(peerHash) ||
			!this.uniqueReplicators.has(peerHash) ||
			!this._peerSyncCapabilityTimestamps.has(peerHash) ||
			((this._peerSyncCapabilities.get(peerHash) ?? 0) &
				requiredCapabilities) !==
				requiredCapabilities ||
			!this._v2Receive.isCurrentActive({
				peerHash,
				peerSession,
				receiveEpoch,
				senderTransportSession: capabilitySession,
			})
		) {
			return undefined;
		}
		return { capabilitySession, peerSession };
	}

	private async waitPersistedReceiptRetry(
		signal: AbortSignal,
		ms: number,
	): Promise<void> {
		try {
			await delay(ms, { signal });
		} catch (error) {
			throw signal.aborted ? (signal.reason ?? error) : error;
		}
	}

	private reservePersistedReceiptEgress(
		peer: string,
		capabilitySession: bigint,
		hashCount: number,
		now = Date.now(),
	): number {
		let nodeBudget = persistedReceiptEgressBudgets.get(this.node);
		if (!nodeBudget) {
			nodeBudget = { peerSessions: new Map() };
			persistedReceiptEgressBudgets.set(this.node, nodeBudget);
		}
		const peerSessionKey = `${peer}\0${capabilitySession}`;
		let bucket = nodeBudget.peerSessions.get(peerSessionKey);
		if (!bucket) {
			while (
				nodeBudget.peerSessions.size >=
				MAX_PERSISTED_RECEIPT_INGRESS_PEER_SESSIONS
			) {
				const oldest = nodeBudget.peerSessions.keys().next().value;
				if (oldest === undefined) break;
				nodeBudget.peerSessions.delete(oldest);
			}
			bucket = {
				requestTokens: PERSISTED_RECEIPT_INGRESS_PEER_REQUEST_CAPACITY,
				hashTokens: PERSISTED_RECEIPT_INGRESS_PEER_HASH_CAPACITY,
				refilledAt: now,
			};
			nodeBudget.peerSessions.set(peerSessionKey, bucket);
		} else {
			nodeBudget.peerSessions.delete(peerSessionKey);
			nodeBudget.peerSessions.set(peerSessionKey, bucket);
		}
		refillPersistedReceiptIngressBucket(
			bucket,
			now,
			PERSISTED_RECEIPT_INGRESS_PEER_REQUEST_CAPACITY,
			PERSISTED_RECEIPT_INGRESS_PEER_HASH_CAPACITY,
			PERSISTED_RECEIPT_INGRESS_PEER_REQUESTS_PER_SECOND,
			PERSISTED_RECEIPT_INGRESS_PEER_HASHES_PER_SECOND,
		);
		if (bucket.requestTokens >= 1 && bucket.hashTokens >= hashCount) {
			bucket.requestTokens -= 1;
			bucket.hashTokens -= hashCount;
			return 0;
		}
		return Math.max(
			1,
			Math.ceil(
				Math.max(
					((1 - bucket.requestTokens) /
						PERSISTED_RECEIPT_INGRESS_PEER_REQUESTS_PER_SECOND) *
						1_000,
					((hashCount - bucket.hashTokens) /
						PERSISTED_RECEIPT_INGRESS_PEER_HASHES_PER_SECOND) *
						1_000,
				),
			),
		);
	}

	private async waitForPersistedReceiptEgressAdmission(
		peer: string,
		capabilitySession: bigint,
		hashCount: number,
		signal: AbortSignal,
	): Promise<void> {
		while (true) {
			if (signal.aborted) {
				throw signal.reason ?? new AbortError();
			}
			const waitMs = this.reservePersistedReceiptEgress(
				peer,
				capabilitySession,
				hashCount,
			);
			if (waitMs === 0) return;
			await this.waitPersistedReceiptRetry(signal, waitMs);
		}
	}

	private persistedDeliveryTimeoutMs(
		delivery: PersistedDeliveryOptions,
		hashCount: number,
	): number {
		return (
			delivery.timeout ??
			Math.min(
				MAX_PERSISTED_DELIVERY_TIMEOUT_MS,
				DEFAULT_PERSISTED_RECEIPT_TIMEOUT_MS +
					persistedTransferAdmissionBudgetMs(hashCount) +
					Math.ceil(persistedReceiptPacingFloorMs(hashCount)),
			)
		);
	}

	private async waitForPersistedTransferAdmission(
		peer: string,
		hashes: readonly string[],
		captured: { capabilitySession: bigint; peerSession: PeerSession },
		signal: AbortSignal,
		isStillCurrent: () => boolean,
	): Promise<boolean> {
		const expiresAt = Date.now() + MAX_PERSISTED_RECEIPT_ATTEMPT_MS;
		while (!signal.aborted && isStillCurrent()) {
			const current = this.persistedReceiptPeerSession(peer);
			if (
				!current ||
				current.capabilitySession !== captured.capabilitySession ||
				current.peerSession !== captured.peerSession
			) {
				return false;
			}
			if (hashes.every((hash) => this.isEntryKnownByPeer(hash, peer))) {
				return true;
			}
			const remaining = expiresAt - Date.now();
			if (remaining <= 0) return false;
			await this.waitPersistedReceiptRetry(
				signal,
				Math.min(PERSISTED_RECEIPT_RETRY_MS, remaining),
			);
		}
		if (signal.aborted) throw signal.reason ?? new AbortError();
		return false;
	}

	private createPersistedDeliveryDeadline(
		delivery: PersistedDeliveryOptions,
		ownershipLifecycleController: AbortController,
		hashCount = 1,
	): PersistedDeliveryDeadline {
		const timeoutMs = this.persistedDeliveryTimeoutMs(delivery, hashCount);
		if (
			!Number.isFinite(timeoutMs) ||
			timeoutMs <= 0 ||
			timeoutMs > MAX_PERSISTED_DELIVERY_TIMEOUT_MS
		) {
			throw new Error(
				`persisted delivery timeout must be a positive number no greater than ${MAX_PERSISTED_DELIVERY_TIMEOUT_MS}`,
			);
		}
		const minAcks = Math.floor(delivery.minAcks!);
		const deadlineController = new AbortController();
		const timeout = setTimeout(
			() =>
				deadlineController.abort(
					new TimeoutError(
						`Timed out waiting for ${minAcks} persisted remote replicas.`,
					),
				),
			timeoutMs,
		);
		timeout.unref?.();
		return {
			deadline: Date.now() + timeoutMs,
			signal: AbortSignal.any(
				[
					delivery.signal,
					this._closeController.signal,
					ownershipLifecycleController.signal,
					deadlineController.signal,
				].filter((value): value is AbortSignal => !!value),
			),
			dispose: () => clearTimeout(timeout),
		};
	}

	private async planPersistedDeliveryLeaders(
		records: PersistedDeliveryPlanningRecord<T, R>[],
		replicas: number,
		ownershipLifecycleController: AbortController,
	): Promise<LeaderMap[]> {
		if (
			this.findLeadersFromEntry !== SharedLog.prototype.findLeadersFromEntry
		) {
			const leaders: LeaderMap[] = [];
			for (const record of records) {
				const entry = record.createFullPlanningSource?.();
				if (!entry) {
					throw new Error(
						`Persisted delivery requires canonical entry bytes for custom leader planning of ${record.canonicalHash}`,
					);
				}
				leaders.push(
					await this.findLeadersFromEntry(
						entry,
						replicas,
						{ freshLeaderPlan: true },
						ownershipLifecycleController,
					),
				);
			}
			return leaders;
		}
		const items: EntryLeaderBatchItem<R>[] = records.map((record) => ({
			entry: record.createDefaultPlanningSource(),
			replicas,
			options: { freshLeaderPlan: true, persist: false },
		}));
		const nativeRoutingPlanner =
			this._nativeRangePlanner ?? this._nativeBackbone;
		if (this.canPlanNativeEntryLeaderBatch(items) && nativeRoutingPlanner) {
			const options = items[0]!.options!;
			const context = await this.createLeaderSelectionContext(
				options,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeOptions = this.createNativeLeaderOptions(context, options);
			const fullReplicaLeaders =
				nativeRoutingPlanner.getRoutingFullReplicaLeaders?.(
					replicas,
					nativeOptions,
				);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (fullReplicaLeaders) {
				// Receipt settlement treats leader maps as read-only. Reusing the one
				// gid-independent routing result avoids one Map allocation per entry.
				return new Array<LeaderMap>(items.length).fill(fullReplicaLeaders);
			}
			if (nativeRoutingPlanner.planLeaderSamplesForGidsBatch) {
				const nativeLeaders =
					nativeRoutingPlanner.planLeaderSamplesForGidsBatch(
						items.map((item) => ({
							gid: this.getEntryGid(item.entry),
							replicas: item.replicas,
						})),
						nativeOptions,
					);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				if (nativeLeaders?.length === items.length) {
					return nativeLeaders;
				}
			}
		}
		return (
			await this.planEntryLeaderBatch(items, ownershipLifecycleController)
		).map((plan) => plan.leaders);
	}

	private async settlePersistedDelivery(
		input: PersistedDeliveryPlanningRecord<T, R>[],
		replicas: number,
		delivery: PersistedDeliveryOptions,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
		persistedDeadline?: PersistedDeliveryDeadline,
		transferOnFirstRound = false,
		onFreshLeaderPlan?: (
			leadersByEntry: readonly LeaderMap[],
			ownershipRevision: number,
		) => void,
	): Promise<void> {
		const minAcks = Math.floor(delivery.minAcks!);
		const records = new Map(
			input.map((record) => [record.canonicalHash, record]),
		);
		if (records.size === 0) return;

		const committedHashes = [...records.keys()];
		const ownedDeadline = !persistedDeadline;
		const deadline =
			persistedDeadline ??
			this.createPersistedDeliveryDeadline(
				delivery,
				ownershipLifecycleController,
				records.size,
			);
		const signal = deadline.signal;
		let maxAttemptMs = MAX_PERSISTED_RECEIPT_ATTEMPT_MS;
		let initialTransferPending = transferOnFirstRound;
		let needsInitialLeaderCheck = true;
		const carriedAcknowledgements = new Map<
			string,
			Map<string, { capabilitySession: bigint; peerSession: PeerSession }>
		>(committedHashes.map((hash) => [hash, new Map()]));
		const repairsByPeer = new Map<
			string,
			{
				capabilitySession: bigint;
				peerSession: PeerSession;
				hashes: Set<string>;
			}
		>();
		let acknowledgementOwnershipRevision: number | undefined;
		const purgePeerDeliveryState = (peer: string) => {
			for (const acknowledgements of carriedAcknowledgements.values()) {
				acknowledgements.delete(peer);
			}
			repairsByPeer.delete(peer);
		};
		try {
			while (true) {
				if (signal.aborted) {
					throw signal.reason ?? new AbortError();
				}
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				const ownershipRevision =
					this._instanceLifecycle?._receiveOwnershipRevision ?? 0;
				const isRoundOwnershipCurrent = () =>
					this.isReceiveOwnershipSnapshotStable(ownershipRevision);
				if (acknowledgementOwnershipRevision !== ownershipRevision) {
					for (const acknowledgements of carriedAcknowledgements.values()) {
						acknowledgements.clear();
					}
					repairsByPeer.clear();
					acknowledgementOwnershipRevision = ownershipRevision;
				} else {
					for (const acknowledgements of carriedAcknowledgements.values()) {
						for (const [peer, captured] of acknowledgements) {
							const current = this.persistedReceiptPeerSession(peer);
							if (
								!current ||
								current.capabilitySession !== captured.capabilitySession ||
								current.peerSession !== captured.peerSession
							) {
								purgePeerDeliveryState(peer);
							}
						}
					}
				}
				if (!isRoundOwnershipCurrent()) {
					await this.waitPersistedReceiptRetry(
						signal,
						PERSISTED_RECEIPT_RETRY_MS,
					);
					continue;
				}

				// Carry receipts only across retries in the exact same ownership and
				// transport epoch. A revision/session change purges them before they can
				// survive an away-and-back leader transition or combine with a later peer.
				const hashesByPeer = new Map<string, string[]>();
				const entryArray = [...records.values()];
				const leadersByEntry = await this.planPersistedDeliveryLeaders(
					entryArray,
					replicas,
					ownershipLifecycleController,
				);
				if (!isRoundOwnershipCurrent()) continue;
				onFreshLeaderPlan?.(leadersByEntry, ownershipRevision);
				if (!isRoundOwnershipCurrent()) continue;
				const selfHash = this.node.identity.publicKey.hashcode();
				if (needsInitialLeaderCheck) {
					needsInitialLeaderCheck = false;
					if (
						leadersByEntry.some(
							(leaders) =>
								leaders.size === 0 ||
								(leaders.size === 1 && leaders.has(selfHash)),
						)
					) {
						throw new NoPeersError(this.rpc.topic);
					}
				}
				for (let index = 0; index < entryArray.length; index++) {
					const hash = entryArray[index]!.canonicalHash;
					const leaders = leadersByEntry[index]!;
					if (signal.aborted) {
						throw signal.reason ?? new AbortError();
					}
					if (!isRoundOwnershipCurrent()) break;
					const acknowledgements = carriedAcknowledgements.get(hash)!;
					for (const [peer, captured] of acknowledgements) {
						const current = this.persistedReceiptPeerSession(peer);
						if (
							!leaders.has(peer) ||
							!current ||
							current.capabilitySession !== captured.capabilitySession ||
							current.peerSession !== captured.peerSession
						) {
							acknowledgements.delete(peer);
						}
					}
					if (acknowledgements.size >= minAcks) continue;
					for (const peer of leaders.keys()) {
						if (peer === selfHash) continue;
						const current = this.persistedReceiptPeerSession(peer);
						if (!current) continue;
						if (acknowledgements.has(peer)) continue;
						const hashes = hashesByPeer.get(peer) ?? [];
						hashes.push(hash);
						hashesByPeer.set(peer, hashes);
					}
				}
				if (!isRoundOwnershipCurrent()) continue;

				const operationQueue = new PQueue({
					concurrency: MAX_PERSISTED_RECEIPT_REQUESTS_GLOBAL,
				});
				const candidateWaves = Math.max(
					1,
					Math.ceil(hashesByPeer.size / MAX_PERSISTED_RECEIPT_REQUESTS_GLOBAL),
				);
				const roundController = new AbortController();
				const roundSignal = AbortSignal.any([signal, roundController.signal]);
				const getAttemptTimeout = () =>
					Math.max(
						1,
						Math.min(
							maxAttemptMs,
							Math.floor((deadline.deadline - Date.now()) / candidateWaves),
						),
					);
				const requests = new Set<Promise<void>>();
				const transferAllOnRound = initialTransferPending;
				try {
					for (const [peer, hashes] of hashesByPeer) {
						let request!: Promise<void>;
						request = (async () => {
							const captured = this.persistedReceiptPeerSession(peer);
							if (!captured) return;
							const previousRepair = repairsByPeer.get(peer);
							if (
								previousRepair &&
								(previousRepair.capabilitySession !==
									captured.capabilitySession ||
									previousRepair.peerSession !== captured.peerSession)
							) {
								repairsByPeer.delete(peer);
							}
							const ensureRepairState = () => {
								let state = repairsByPeer.get(peer);
								if (!state) {
									state = {
										capabilitySession: captured.capabilitySession,
										peerSession: captured.peerSession,
										hashes: new Set<string>(),
									};
									repairsByPeer.set(peer, state);
								}
								return state;
							};
							const isPeerRoundCurrent = () => {
								if (roundSignal.aborted || !isRoundOwnershipCurrent()) {
									return false;
								}
								const current = this.persistedReceiptPeerSession(peer);
								return (
									!!current &&
									current.capabilitySession === captured.capabilitySession &&
									current.peerSession === captured.peerSession
								);
							};
							try {
								await this._v2Send.confirmLatestForPeer(
									{
										peerHash: peer,
										peerSession: captured.peerSession,
										receiverTransportSession: captured.capabilitySession,
									},
									{
										timeout: getAttemptTimeout(),
										signal: roundSignal,
									},
								);
							} catch {
								// The exact receiver generation did not prove that it applied
								// our latest role state. Replan instead of transferring to a
								// peer that can still make a stale admission decision.
								if (transferAllOnRound && isPeerRoundCurrent()) {
									const state = ensureRepairState();
									for (const hash of hashes) state.hashes.add(hash);
								}
								return;
							}
							if (!isPeerRoundCurrent()) {
								purgePeerDeliveryState(peer);
								return;
							}
							const repairs = repairsByPeer.get(peer)?.hashes;
							const transferHashes = transferAllOnRound
								? hashes
								: repairs
									? hashes.filter((hash) => repairs.has(hash))
									: [];
							let attemptedHashCount = 0;
							if (transferHashes.length > 0) {
								try {
									await this.pushEntryHashes(peer, transferHashes, {
										acknowledge: false,
										chunkTimeout: getAttemptTimeout,
										chunkSize: PERSISTED_TRANSFER_CHUNK_SIZE,
										isStillCurrent: isPeerRoundCurrent,
										onChunkAttempted: (chunk) => {
											attemptedHashCount += chunk.length;
											if (!transferAllOnRound) {
												const state = repairsByPeer.get(peer);
												for (const hash of chunk) state?.hashes.delete(hash);
											}
										},
										onChunkSent: async (chunk) => {
											return this.waitForPersistedTransferAdmission(
												peer,
												chunk,
												captured,
												roundSignal,
												isPeerRoundCurrent,
											);
										},
										operationQueue,
										priority: delivery.priority,
										repairHint: !transferAllOnRound,
										signal: roundSignal,
									});
								} catch {
									// A send can fail after remote admission. The attempted prefix
									// includes that uncertain chunk for authoritative receipt probing.
								}
								if (
									transferAllOnRound &&
									attemptedHashCount < transferHashes.length
								) {
									const state = ensureRepairState();
									for (const hash of transferHashes.slice(attemptedHashCount)) {
										state.hashes.add(hash);
									}
								}
							}
							const receiptHashes = transferAllOnRound
								? hashes.slice(0, attemptedHashCount)
								: hashes;
							if (!isPeerRoundCurrent()) {
								purgePeerDeliveryState(peer);
								return;
							}
							// Keep receipt chunks sequential per peer. The shared operation
							// queue bounds only active sends/requests; admission waits never
							// occupy its slots and cannot starve a later healthy candidate.
							for (
								let offset = 0;
								offset < receiptHashes.length;
								offset += PERSISTED_RECEIPT_CHUNK_SIZE
							) {
								const requestedHashes = receiptHashes.slice(
									offset,
									offset + PERSISTED_RECEIPT_CHUNK_SIZE,
								);
								if (roundSignal.aborted || !isPeerRoundCurrent()) {
									if (!isRoundOwnershipCurrent()) roundController.abort();
									break;
								}
								let responses;
								try {
									await this.waitForPersistedReceiptEgressAdmission(
										peer,
										captured.capabilitySession,
										requestedHashes.length,
										roundSignal,
									);
									if (!isPeerRoundCurrent()) break;
									const attemptTimeout = getAttemptTimeout();
									responses =
										(await operationQueue.add(async () => {
											if (!isPeerRoundCurrent()) return [];
											return this.rpc.request(
												new RequestPersistedEntriesV1({
													expectedReceiverSession: captured.capabilitySession,
													hashes: requestedHashes,
												}),
												{
													mode: new SilentDelivery({
														to: [peer],
														redundancy: 1,
													}),
													amount: 1,
													priority: delivery.priority,
													timeout: attemptTimeout,
													signal: roundSignal,
												},
											);
										})) ?? [];
								} catch {
									if (roundSignal.aborted) break;
									// A peer can disconnect or miss this retry while the overall
									// quorum deadline remains active. Replan on the next round.
									break;
								}
								if (!isRoundOwnershipCurrent()) {
									roundController.abort();
									break;
								}
								const current = this.persistedReceiptPeerSession(peer);
								if (
									!current ||
									current.capabilitySession !== captured.capabilitySession ||
									current.peerSession !== captured.peerSession
								) {
									purgePeerDeliveryState(peer);
									break;
								}
								const requested = new Set(requestedHashes);
								const confirmed = new Set<string>();
								let receivedValidConfirmation = false;
								for (const result of responses) {
									if (
										!(result.response instanceof ConfirmEntriesMessage) ||
										result.from?.hashcode() !== peer ||
										result.message.header.session !== captured.capabilitySession
									) {
										continue;
									}
									const unique = new Set(result.response.hashes);
									if (
										unique.size !== result.response.hashes.length ||
										[...unique].some((hash) => !requested.has(hash))
									) {
										continue;
									}
									receivedValidConfirmation = true;
									for (const hash of unique) {
										confirmed.add(hash);
										carriedAcknowledgements.get(hash)?.set(peer, captured);
									}
								}
								if (receivedValidConfirmation) {
									const state = ensureRepairState();
									for (const hash of requested) {
										if (confirmed.has(hash)) {
											state.hashes.delete(hash);
										} else {
											state.hashes.add(hash);
										}
									}
								}
							}
						})()
							.then(() => undefined)
							.catch(() => undefined)
							.finally(() => requests.delete(request));
						requests.add(request);
					}

					const roundComplete = async (): Promise<boolean> => {
						if (signal.aborted) {
							throw signal.reason ?? new AbortError();
						}
						if (!isRoundOwnershipCurrent()) return false;
						for (const acknowledgements of carriedAcknowledgements.values()) {
							if (acknowledgements.size < minAcks) return false;
						}
						const validatedLeaders = await this.planPersistedDeliveryLeaders(
							entryArray,
							replicas,
							ownershipLifecycleController,
						);
						if (!isRoundOwnershipCurrent()) return false;
						onFreshLeaderPlan?.(validatedLeaders, ownershipRevision);
						if (!isRoundOwnershipCurrent()) return false;
						for (let index = 0; index < entryArray.length; index++) {
							if (signal.aborted || !isRoundOwnershipCurrent()) {
								if (signal.aborted) {
									throw signal.reason ?? new AbortError();
								}
								return false;
							}
							const hash = entryArray[index]!.canonicalHash;
							const leaders = validatedLeaders[index]!;
							let valid = 0;
							const acknowledgements = carriedAcknowledgements.get(hash)!;
							for (const [peer, captured] of acknowledgements) {
								const current = this.persistedReceiptPeerSession(peer);
								if (
									!current ||
									current.capabilitySession !== captured.capabilitySession ||
									current.peerSession !== captured.peerSession
								) {
									purgePeerDeliveryState(peer);
									continue;
								}
								if (!leaders.has(peer)) {
									acknowledgements.delete(peer);
									continue;
								}
								if (++valid >= minAcks) {
									break;
								}
							}
							if (valid < minAcks) return false;
						}
						this.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
						if (signal.aborted) {
							throw signal.reason ?? new AbortError();
						}
						return isRoundOwnershipCurrent();
					};

					while (requests.size > 0) {
						await Promise.race(requests);
						if (await roundComplete()) {
							return;
						}
					}
					if (await roundComplete()) {
						return;
					}
				} finally {
					roundController.abort();
					void Promise.allSettled([...requests]);
				}
				if (isRoundOwnershipCurrent()) {
					initialTransferPending = false;
				}
				// Keep early retries fair and responsive, then let a caller's longer
				// overall deadline accommodate a genuinely slow durability barrier.
				maxAttemptMs = Math.min(
					MAX_PERSISTED_DELIVERY_TIMEOUT_MS,
					maxAttemptMs * 2,
				);
				await this.waitPersistedReceiptRetry(
					signal,
					Math.max(
						0,
						Math.min(
							PERSISTED_RECEIPT_RETRY_MS,
							deadline.deadline - Date.now(),
						),
					),
				);
			}
		} catch (error) {
			if (error instanceof PersistedDeliveryError) {
				throw error;
			}
			throw new PersistedDeliveryError(error, committedHashes);
		} finally {
			if (ownedDeadline) deadline.dispose();
		}
	}

	private async collectDeferredAppendBackfillExtras(
		entry: Entry<T>,
		replicas: number,
		baseLeaders: LeaderMap,
		nativeDeliveryPlan: AppendDeliveryPlan | undefined,
		ownershipLifecycleController: AbortController,
	): Promise<
		Omit<PersistedAppendBackfillSource<T, R>, "entry" | "coordinates">
	> {
		const ownershipRevision =
			this._instanceLifecycle?._receiveOwnershipRevision ?? 0;
		const ownershipWasStable =
			this.isReceiveOwnershipSnapshotStable(ownershipRevision);
		const assignmentExtraLeaders: LeaderMap = new Map();
		const deliveryExtraTargets = new Set<string>();
		if (nativeDeliveryPlan) {
			for (const peer of nativeDeliveryPlan.repairTargets) {
				// The sampled entry leaders are replaced by settlement's fresh plan.
				// Retain only native repair additions that were outside that base map.
				if (!baseLeaders.has(peer)) deliveryExtraTargets.add(peer);
			}
		} else {
			const selfHash = this.node.identity.publicKey.hashcode();
			const fullReplicaDeliveryCandidates =
				await this.getNativeFullReplicaDeliveryCandidates(replicas, selfHash);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (replicas >= Math.max(1, fullReplicaDeliveryCandidates.size)) {
				for (const peer of fullReplicaDeliveryCandidates) {
					if (!baseLeaders.has(peer)) {
						assignmentExtraLeaders.set(peer, { intersecting: true });
					}
				}
			}
		}

		const referenceLeaders: LeaderMap = new Map();
		for await (const message of createExchangeHeadsMessages(this.log, [
			entry,
		])) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			await this._mergeLeadersFromGidReferences(
				message,
				replicas,
				referenceLeaders,
				ownershipLifecycleController,
				{ freshLeaderPlan: true },
			);
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		for (const peer of referenceLeaders.keys()) {
			deliveryExtraTargets.add(peer);
		}
		return {
			assignmentExtraLeaders,
			deliveryExtraTargets,
			extrasOwnershipRevision:
				ownershipWasStable &&
				this.isReceiveOwnershipSnapshotStable(ownershipRevision)
					? ownershipRevision
					: undefined,
		};
	}

	private async _appendDeliverToReplicators(
		entry: Entry<T>,
		coordinates: NumberFromType<R>[],
		minReplicasValue: number,
		leaders: LeaderMap,
		selfHash: string,
		isLeader: boolean,
		deliveryArg: false | true | DeliveryOptions | undefined,
		nativeDeliveryPlan?: AppendDeliveryPlan,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		const throwIfInactive = () =>
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		throwIfInactive();
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
		if (
			nativeDeliveryPlan &&
			!nativeDeliveryPlan.hasRemoteRecipients &&
			!delivery &&
			!requireRecipients &&
			(leaders.size === 0 || (leaders.size === 1 && leaders.has(selfHash)))
		) {
			const allowSubscriberFallback =
				this.syncronizer instanceof SimpleSyncronizer;
			if (!allowSubscriberFallback) {
				return;
			}
			try {
				const subscribers = await this._getTopicSubscribers(this.topic);
				throwIfInactive();
				const hasRemoteSubscriber = subscribers?.some(
					(subscriber) => subscriber.hashcode() !== selfHash,
				);
				if (!hasRemoteSubscriber) {
					return;
				}
			} catch {
				throwIfInactive();
				return;
			}
		}

		if (!nativeDeliveryPlan) {
			const fullReplicaDeliveryCandidates =
				await this.getFullReplicaRepairCandidates(undefined, {
					includeSubscribers: false,
				});
			throwIfInactive();
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
		for await (const message of createExchangeHeadsMessages(this.log, [
			entry,
		])) {
			throwIfInactive();
			const leaderCountBeforeReferenceMerge = leaders.size;
			await this._mergeLeadersFromGidReferences(
				message,
				minReplicasValue,
				leaders,
				ownershipLifecycleController,
			);
			throwIfInactive();
			const canUseNativeDeliveryPlan =
				!!nativeDeliveryPlan &&
				nativeDeliveryPlan.hasRemoteRecipients &&
				leaders.size === leaderCountBeforeReferenceMerge;
			if (canUseNativeDeliveryPlan) {
				if (!delivery) {
					for (const peer of nativeDeliveryPlan.repairTargets) {
						throwIfInactive();
						this.queueAppendBackfill(
							peer,
							entryReplicatedForRepair,
							ownershipLifecycleController,
						);
					}
					if (nativeDeliveryPlan.defaultSendSilent) {
						const rawTargets = this.canUseLiveRawGossip(
							nativeDeliveryPlan.sendTo,
							selfHash,
						);
						if (rawTargets) {
							throwIfInactive();
							this.queueLiveRawGossip(
								entry.hash,
								message.heads[0]?.gidRefrences ?? [],
								entry.size ?? 0,
								rawTargets,
							);
							continue;
						}
					}
					throwIfInactive();
					this.emitPlainLiveSendProfile(message);
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
								throwIfInactive();
								await this._sendAckWithUnifiedHints({
									peer,
									message,
									payload,
									priority: delivery.priority,
									fanoutUnicastOptions,
								});
								throwIfInactive();
							})(),
						);
					}
				}

				if (nativeDeliveryPlan.silentTo.length > 0) {
					throwIfInactive();
					this.rpc
						.send(message, {
							mode: new SilentDelivery({
								redundancy: 1,
								to: nativeDeliveryPlan.silentTo,
							}),
							priority: delivery.priority,
						})
						.catch((error) => logger.error(error));
				}
				for (const peer of nativeDeliveryPlan.repairTargets) {
					throwIfInactive();
					this.queueAppendBackfill(
						peer,
						entryReplicatedForRepair,
						ownershipLifecycleController,
					);
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
				this.syncronizer instanceof SimpleSyncronizer;
			if (!hasRemotePeers && allowSubscriberFallback) {
				try {
					const subscribers = await this._getTopicSubscribers(this.topic);
					throwIfInactive();
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
					throwIfInactive();
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
					throwIfInactive();
					if (peer === selfHash) {
						continue;
					}
					// Default live append delivery is still optimistic. If one remote misses
					// the initial heads exchange and the caller did not opt into explicit
					// delivery acks, we still need a targeted backfill source of truth for the
					// authoritative recipients or one entry can get stuck at 2/3 replicas
					// forever. Best-effort fallback subscribers are not repair-worthy.
					this.queueAppendBackfill(
						peer,
						entryReplicatedForRepair,
						ownershipLifecycleController,
					);
				}
				if (isLeader) {
					const rawTargets = this.canUseLiveRawGossip(set, selfHash);
					if (rawTargets) {
						throwIfInactive();
						this.queueLiveRawGossip(
							entry.hash,
							message.heads[0]?.gidRefrences ?? [],
							entry.size ?? 0,
							rawTargets,
						);
						continue;
					}
				}
				throwIfInactive();
				this.emitPlainLiveSendProfile(message);
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
							throwIfInactive();
							await this._sendAckWithUnifiedHints({
								peer,
								message,
								payload,
								priority: delivery.priority,
								fanoutUnicastOptions,
							});
							throwIfInactive();
						})(),
					);
				}
			}

			if (silentTo?.length) {
				throwIfInactive();
				this.rpc
					.send(message, {
						mode: new SilentDelivery({ redundancy: 1, to: silentTo }),
						priority: delivery.priority,
					})
					.catch((error) => logger.error(error));
			}
			for (const peer of repairTargets) {
				throwIfInactive();
				// Direct append delivery is intentionally optimistic. Queue one delayed,
				// batched maybe-sync pass for the intended recipients so stable 3-peer
				// append workloads do not depend on perfect first-try delivery ordering.
				this.queueAppendBackfill(
					peer,
					entryReplicatedForRepair,
					ownershipLifecycleController,
				);
			}
		}

		if (pending.length > 0) {
			await Promise.all(pending);
			throwIfInactive();
		}
	}

	private async _mergeLeadersFromGidReferences(
		message: ExchangeHeadsMessage<any>,
		minReplicasValue: number,
		leaders: LeaderMap,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
		options?: { freshLeaderPlan?: boolean },
	) {
		const throwIfInactive = () =>
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		throwIfInactive();
		const gidReferences = message.heads[0]?.gidRefrences;
		if (!gidReferences || gidReferences.length === 0) {
			return;
		}

		for (const gidReference of gidReferences) {
			throwIfInactive();
			const entryFromGid = this.log.entryIndex.getHeads(gidReference, false);
			for (const gidEntry of await entryFromGid.all()) {
				throwIfInactive();
				let coordinates = (await this._coordinates.getCoordinates(
					gidEntry,
				)) as NumberFromType<R>[];
				throwIfInactive();
				let found: Map<string, { intersecting: boolean }>;
				if (coordinates == null) {
					found = await this.findLeadersFromEntry(
						gidEntry,
						minReplicasValue,
						options?.freshLeaderPlan ? { freshLeaderPlan: true } : undefined,
						ownershipLifecycleController,
					);
				} else {
					found = await this._findLeaders(
						coordinates,
						options?.freshLeaderPlan ? { freshLeaderPlan: true } : undefined,
						ownershipLifecycleController,
					);
				}
				throwIfInactive();

				for (const [key, value] of found) {
					leaders.set(key, value);
				}
			}
		}
	}

	private async _appendDeliverToAllFanout(
		entry: Entry<T>,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		const throwIfInactive = () =>
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		throwIfInactive();
		for await (const message of createExchangeHeadsMessages(this.log, [
			entry,
		])) {
			throwIfInactive();
			await this._publishExchangeHeadsViaFanout(message);
			throwIfInactive();
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

	private async _getLocalReachablePeerHashes(topic: string): Promise<string[]> {
		const cache = (this._localReachablePeerHashesCache ??= new Map());
		const cached = cache.get(topic);
		if (cached?.hashes && cached.expiresAt > Date.now()) {
			return cached.hashes.slice();
		}
		if (cached?.inFlight) {
			return (await cached.inFlight).slice();
		}

		const entry: {
			expiresAt: number;
			hashes?: string[];
			inFlight?: Promise<string[]>;
		} = { expiresAt: 0 };
		const inFlight = (async () => {
			const selfHash = this.node.identity.publicKey.hashcode();
			const hashes: string[] = [];
			const seen = new Set<string>();
			const addHash = (hash: unknown) => {
				if (
					typeof hash !== "string" ||
					hash.length === 0 ||
					hash === selfHash ||
					seen.has(hash) ||
					hashes.length >= LOCAL_REACHABLE_PEERS_MAX
				) {
					return;
				}
				seen.add(hash);
				hashes.push(hash);
			};

			const pubsub = this.node.services.pubsub as any;
			const peerMap = pubsub?.peers;
			if (peerMap?.entries) {
				for (const [hash, peer] of peerMap.entries()) {
					addHash(peer?.publicKey?.hashcode?.());
					addHash(hash);
					if (hashes.length >= LOCAL_REACHABLE_PEERS_MAX) break;
				}
			}

			if (hashes.length < LOCAL_REACHABLE_PEERS_MAX) {
				const connectionManager = pubsub?.components?.connectionManager;
				for (const conn of connectionManager?.getConnections?.() ?? []) {
					const peerId = conn?.remotePeer;
					if (!peerId) continue;
					try {
						addHash(getPublicKeyFromPeerId(peerId).hashcode());
					} catch {
						// Best-effort only.
					}
					if (hashes.length >= LOCAL_REACHABLE_PEERS_MAX) break;
				}
			}

			if (hashes.length < LOCAL_REACHABLE_PEERS_MAX) {
				try {
					for (const subscriber of (await pubsub.getSubscribers(topic)) ?? []) {
						addHash(subscriber?.hashcode?.());
						if (hashes.length >= LOCAL_REACHABLE_PEERS_MAX) break;
					}
				} catch {
					// Local reachability is best-effort.
				}
			}

			return hashes;
		})();
		entry.inFlight = inFlight;
		cache.set(topic, entry);

		try {
			const hashes = await inFlight;
			if (cache.get(topic) === entry) {
				entry.hashes = hashes;
				entry.expiresAt = Date.now() + LOCAL_REACHABLE_PEERS_CACHE_TTL_MS;
				entry.inFlight = undefined;
			}
			return hashes.slice();
		} catch (error) {
			if (cache.get(topic) === entry) cache.delete(topic);
			throw error;
		}
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
		if (
			this._fanoutChannel &&
			(topic === this.topic || topic === this.rpc.topic)
		) {
			for (const hash of this._fanoutChannel.getPeerHashes({
				includeSelf: false,
			})) {
				addHash(hash);
				if (hashes.size + keysByHash.size >= maxPeers) break;
			}
		}

		// Already-connected peer streams are cheap and are the strongest local signal
		// when fanout/provider membership is stale.
		const peerMap: Map<string, { publicKey?: PublicSignKey }> | undefined = (
			this.node.services.pubsub as any
		)?.peers;
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
			this._localReachablePeerHashesCache?.delete(topic);
		}
		this.invalidateLeaderSelectionContextCache();
	}

	private invalidateSharedLogTopicSubscribersCache() {
		this.invalidateTopicSubscribersCache(this.topic, this.rpc.topic);
	}

	private invalidateLeaderSelectionContextCache() {
		if (this._instanceLifecycle) {
			this._instanceLifecycle._receiveOwnershipRevision++;
		}
		this._leaderSelectionContextCache = undefined;
		this._leaderPlanCache?.invalidate();
	}

	/**
	 * Cache key for a leader-plan lookup, or undefined when the call is
	 * uncacheable: explicit freshness demands, per-call candidate filters,
	 * roleAge outside the two hot buckets (dynamic default and 0), or an
	 * in-flight replication-range mutation window (a plan observed
	 * mid-transition must not outlive the transition).
	 */
	private leaderPlanCacheBucket(options?: {
		roleAge?: number;
		candidates?: Iterable<string>;
		freshLeaderPlan?: boolean;
	}): string | undefined {
		if (options?.freshLeaderPlan || options?.candidates != null) {
			return undefined;
		}
		if (
			(this._instanceLifecycle?._receiveOwnershipMutationAdmissions ?? 0) !== 0
		) {
			return undefined;
		}
		if (options?.roleAge == null) {
			return "d";
		}
		if (options.roleAge === 0) {
			return "0";
		}
		return undefined;
	}

	private isReceiveOwnershipSnapshotStable(revision: number): boolean {
		return (
			(this._instanceLifecycle?._receiveOwnershipMutationAdmissions ?? 0) ===
				0 &&
			(this._instanceLifecycle?._receiveOwnershipRevision ?? 0) === revision
		);
	}

	private canCacheLeaderSelectionContext(options?: {
		roleAge?: number;
		candidates?: Iterable<string>;
		freshLeaderPlan?: boolean;
	}) {
		return (
			options?.roleAge == null &&
			options?.candidates == null &&
			options?.freshLeaderPlan !== true
		);
	}

	private cloneLeaderSelectionContext(
		context: LeaderSelectionContext,
	): LeaderSelectionContext {
		return {
			...context,
			peerFilter: context.peerFilter ? new Set(context.peerFilter) : undefined,
			peerFilterArray: context.peerFilterArray
				? [...context.peerFilterArray]
				: undefined,
		};
	}

	private getCachedLeaderSelectionContext(options?: {
		roleAge?: number;
		candidates?: Iterable<string>;
		freshLeaderPlan?: boolean;
	}): LeaderSelectionContext | undefined {
		if (!this.canCacheLeaderSelectionContext(options)) {
			return;
		}
		const cached = this._leaderSelectionContextCache;
		if (!cached || cached.expiresAt <= Date.now()) {
			return;
		}
		return this.cloneLeaderSelectionContext(cached.context);
	}

	private setCachedLeaderSelectionContext(
		options:
			| {
					roleAge?: number;
					candidates?: Iterable<string>;
					freshLeaderPlan?: boolean;
			  }
			| undefined,
		context: LeaderSelectionContext,
	) {
		if (!this.canCacheLeaderSelectionContext(options)) {
			return;
		}
		this._leaderSelectionContextCache = {
			expiresAt: Date.now() + LEADER_SELECTION_CONTEXT_CACHE_TTL_MS,
			context: this.cloneLeaderSelectionContext(context),
		};
	}

	private isTerminating() {
		return (
			this.acceptsParentAttachments === false ||
			this.closed ||
			this._closeController?.signal.aborted === true
		);
	}

	private isReplicationLifecycleActive(
		controller: AbortController | undefined,
	) {
		return (
			controller != null &&
			controller === this._instanceLifecycle?.membershipLifecycleController &&
			!controller.signal.aborted &&
			!this.isTerminating()
		);
	}

	private resetSubscriptionChangeCallbackTracking() {
		this._subscriptionChangeCallbacks = new Set();
		this._acceptSubscriptionChangeCallbacks = true;
		this._instanceLifecycle!.beginMembership();
	}

	private runSubscriptionChangeCallback(
		callback: () => Promise<void>,
	): Promise<void> | undefined {
		if (!this._acceptSubscriptionChangeCallbacks || this.isTerminating()) {
			return;
		}

		const running = (async () => callback())();
		const observed = running.catch((error) => {
			if (!(this.isTerminating() && isNotStartedError(error as Error))) {
				logger.error(error?.toString?.() ?? String(error));
			}
		});
		const callbacks = (this._subscriptionChangeCallbacks ??= new Set());
		callbacks.add(observed);
		void observed.finally(() => callbacks.delete(observed));
		return observed;
	}

	private stopSubscriptionChangeCallbackAdmission() {
		this._acceptSubscriptionChangeCallbacks = false;
		this._instanceLifecycle?.abortMembership(
			new AbortError("SharedLog is terminating"),
		);
		if (this._onSubscriptionFn) {
			this.node.services.pubsub.removeEventListener(
				"subscribe",
				this._onSubscriptionFn,
			);
		}
		if (this._onUnsubscriptionFn) {
			this.node.services.pubsub.removeEventListener(
				"unsubscribe",
				this._onUnsubscriptionFn,
			);
		}
	}

	private async drainSubscriptionChangeCallbacks() {
		const callbacks = this._subscriptionChangeCallbacks;
		while (callbacks && callbacks.size > 0) {
			await Promise.all([...callbacks]);
		}
	}

	private acquirePeerReceiveLease(
		peerHash: string,
		replicationLifecycleController: AbortController | undefined,
		subscriptionEpoch: object | null,
		options?: {
			allowReplicationInfoBlocked?: boolean;
			allowCleanupGate?: boolean;
		},
	): (() => void) | undefined {
		if (
			!this._peerSessions.isReceiveAdmissionOpen(
				peerHash,
				subscriptionEpoch,
				replicationLifecycleController,
				options,
			)
		) {
			return;
		}

		let state = this._activeReceiveHandlersByPeer.get(peerHash);
		if (!state) {
			const current: PeerReceiveLeaseBucket = { active: 0 };
			state = { current, activeBuckets: new Set() };
			this._activeReceiveHandlersByPeer.set(peerHash, state);
		}
		const bucket = state.current;
		bucket.active += 1;
		state.activeBuckets.add(bucket);
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			bucket.active -= 1;
			if (bucket.active > 0) {
				return;
			}
			state.activeBuckets.delete(bucket);
			bucket.drain?.resolve();
			if (
				state.activeBuckets.size === 0 &&
				state.current.active === 0 &&
				this._activeReceiveHandlersByPeer.get(peerHash) === state
			) {
				this._activeReceiveHandlersByPeer.delete(peerHash);
			}
		};
	}

	private async drainPeerReceiveHandlers(peerHash: string): Promise<void> {
		const state = this._activeReceiveHandlersByPeer.get(peerHash);
		if (!state || state.activeBuckets.size === 0) {
			return;
		}

		// Rotate before awaiting so a reconnect/opening generation can keep receiving
		// sync traffic without joining the drain for the previous subscription. Cleanup
		// callers gate admission first; terminal callers also repeat until empty.
		const buckets = [...state.activeBuckets];
		state.current = { active: 0 };
		const drain = Promise.all(
			buckets.map((bucket) => {
				bucket.drain ??= pDefer<void>();
				return bucket.drain.promise;
			}),
		).then(() => undefined);
		let drains = this._receiveHandlerDrainByPeer.get(peerHash);
		if (!drains) {
			drains = new Set();
			this._receiveHandlerDrainByPeer.set(peerHash, drains);
		}
		drains.add(drain);
		try {
			await drain;
		} finally {
			drains.delete(drain);
			if (drains.size === 0) {
				this._receiveHandlerDrainByPeer.delete(peerHash);
			}
		}
	}

	private async drainReceiveHandlers(): Promise<void> {
		for (;;) {
			const peers = [...this._activeReceiveHandlersByPeer.keys()];
			if (peers.length === 0) {
				return;
			}
			await Promise.all(
				peers.map((peerHash) => this.drainPeerReceiveHandlers(peerHash)),
			);
		}
	}

	private runPendingIHaveCallback(
		pending: PendingIHave<T>,
		entry: Entry<T>,
	): void {
		const replicationLifecycleController =
			this._instanceLifecycle?.membershipLifecycleController;
		if (!this.isReplicationLifecycleActive(replicationLifecycleController)) {
			if (this._pendingIHave.get(entry.hash) === pending) {
				pending.clear();
				this._pendingIHave.delete(entry.hash);
			}
			return;
		}

		// Register before invoking the callback so a synchronous terminal reentry
		// cannot make close/drop miss work that has already been admitted.
		const completion = pDefer<void>();
		const observed = completion.promise.catch((error) => {
			if (!(this.isTerminating() && isNotStartedError(error as Error))) {
				logger.error(error?.toString?.() ?? String(error));
			}
		});
		this._pendingIHaveCallbacks.add(observed);
		void observed.finally(() => {
			this._pendingIHaveCallbacks.delete(observed);
			if (this._pendingIHave.get(entry.hash) === pending) {
				pending.clear();
				this._pendingIHave.delete(entry.hash);
			}
		});

		try {
			Promise.resolve(pending.callback(entry)).then(
				() => completion.resolve(),
				(error) => completion.reject(error),
			);
		} catch (error) {
			completion.reject(error);
		}
	}

	private async drainPendingIHaveCallbacks(): Promise<void> {
		while (this._pendingIHaveCallbacks.size > 0) {
			await Promise.all([...this._pendingIHaveCallbacks]);
		}
	}

	private handleReplicationLifecycleSendError(
		error: unknown,
		controller = this._instanceLifecycle?.membershipLifecycleController,
	) {
		if (
			(controller?.signal.aborted ||
				!this.isReplicationLifecycleActive(controller)) &&
			(error instanceof AbortError || isNotStartedError(error as Error))
		) {
			return;
		}
		logger.error((error as any)?.toString?.() ?? String(error));
	}

	async isReplicating() {
		if (!this._isReplicating) {
			return false;
		}
		return (await this.countReplicationSegments()) > 0;
	}

	private knownSelfReplicating(selfHash: string): boolean | undefined {
		if (!this._isReplicating) {
			return false;
		}
		if (this.uniqueReplicators.has(selfHash)) {
			return true;
		}
		return undefined;
	}

	private setupRebalanceDebounceFunction(
		interval = RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL,
	) {
		this.rebalanceParticipationDebounced?.close();
		this.rebalanceParticipationDebounced = undefined;

		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		let rebalanceParticipationDebounced!: ReturnType<
			typeof debounceFixedInterval
		>;
		rebalanceParticipationDebounced = debounceFixedInterval(
			() =>
				this.rebalanceParticipation(
					ownershipLifecycleController,
					rebalanceParticipationDebounced,
				),
			/* Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.getSize() || 0) *
					REBALANCE_DEBOUNCE_INTERVAL
				)
			) */
			interval, // TODO make this dynamic on the number of replicators
			{
				onError: (error) => this.onRebalanceParticipationError(error),
			},
		);
		this.rebalanceParticipationDebounced = rebalanceParticipationDebounced;
	}

	private onRebalanceParticipationError(error: Error): void {
		if (this.closed || isNotStartedError(error)) {
			return;
		}

		// Debounced invocations run from an un-awaited timer. Throwing here would
		// create an unhandled rejection (and a browser pageerror), so surface
		// unexpected failures through the logger instead.
		logger.error(error);
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

	private async ensureCurrentHeadCoordinatesIndexed(
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const heads = await this.log.getHeads(true).all();
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const headsByHash = new Map(heads.map((head) => [head.hash, head]));
		const nativeCoordinateState =
			this._nativeBackbone ?? this._nativeSharedLogState;
		const nativeHashes = nativeCoordinateState?.getEntryCoordinateHashes();
		const indexedHashes = nativeHashes
			? new Set(nativeHashes)
			: new Set(
					(
						await this.entryCoordinatesIndex
							.iterate({}, { shape: { hash: true } })
							.all()
					).map((entry) => entry.value.hash),
				);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const staleHashes = [...indexedHashes].filter(
			(hash) => !headsByHash.has(hash),
		);

		if (staleHashes.length > 0) {
			await this._coordinates.deleteCoordinatesForHashes(
				staleHashes,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
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
			await this.planEntryLeaderBatch(
				missingHeads,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
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
				msg: AddedReplicationInfoMutation | FullReplicationInfoMutation,
			) => void;
		} = {},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<ReplicationRangeIndexable<R>[]> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		let offsetWasProvided = false;
		if (isUnreplicationOptions(options)) {
			await this._unreplicate(
				undefined,
				replicationOwnershipLifecycleController,
			);
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
		// Flipping replication mode changes getDefaultMinRoleAge and the
		// self-replicating context input even when no range is admitted.
		this.invalidateLeaderSelectionContextCache();

		if (isAdaptiveReplicatorOption(options!)) {
			this._isAdaptiveReplicating = true;
			this.setupDebouncedRebalancing(options);

			// initial role in a dynamic setup
			const maybeRange = await this.getDynamicRange();
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
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
					this.throwIfReplicationOwnershipLifecycleInactive(
						replicationOwnershipLifecycleController,
					);
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
					this.throwIfReplicationOwnershipLifecycleInactive(
						replicationOwnershipLifecycleController,
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
		const confirmedPreliminaryRemovals: ReplicationRangeIndexable<R>[] = [];
		try {
			if (rangesToUnreplicate.length > 0) {
				await this.removeReplicationRanges(
					rangesToUnreplicate,
					this.node.identity.publicKey,
					{
						onRemoved: (removed) => {
							confirmedPreliminaryRemovals.push(...removed);
						},
					},
					replicationOwnershipLifecycleController,
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				if (confirmedPreliminaryRemovals.length > 0 && rebalance !== false) {
					const timestamp = BigInt(Date.now());
					for (const range of confirmedPreliminaryRemovals) {
						this.replicationChangeDebounceFn.add({
							range,
							type: "removed",
							timestamp,
						});
					}
				}
			}

			await this.startAnnounceReplicating(
				rangesToReplicate,
				{
					reset: resetRanges ?? false,
					checkDuplicates,
					announce,
					rebalance,
				},
				replicationOwnershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);

			if (confirmedPreliminaryRemovals.length > 0) {
				await this._announcements.sendReplicationAnnouncement(
					{
						stopped: {
							segmentIds: confirmedPreliminaryRemovals.map((x) => x.id),
						},
					},
					replicationOwnershipLifecycleController,
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
			}
		} catch (operationError) {
			if (
				confirmedPreliminaryRemovals.length === 0 ||
				!this.isRepairLifecycleActive(replicationOwnershipLifecycleController)
			) {
				throw operationError;
			}

			let announcementError: unknown;
			try {
				const segments = (await this.getMyReplicationSegments()).map((range) =>
					range.toReplicationRange(),
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				this.validatePersistedReplicationRangeSnapshot(segments);
				await this._announcements.sendReplicationAnnouncement(
					{ full: { segments } },
					replicationOwnershipLifecycleController,
				);
			} catch (error) {
				announcementError = error;
			}
			if (announcementError !== undefined) {
				throw new AggregateError(
					[operationError, announcementError],
					"Replication ranges changed durably but their corrective announcement failed",
				);
			}
			throw operationError;
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
			/** Wait for explicit remote durable application; omitted is unchanged. */
			confirm?: boolean | ReplicationConfirmationOptions;
			announce?: (
				msg: FullReplicationInfoMutation | AddedReplicationInfoMutation,
			) => void;
		},
	) {
		const wasAdaptiveReplicating = this._isAdaptiveReplicating;
		const previousStorageObjective = wasAdaptiveReplicating
			? this.replicationController?.maxMemoryLimit
			: undefined;
		try {
			const ranges = await this.applyReplicationRole(rangeOrEntry, options);
			if (options?.confirm) {
				await this._v2Send.confirmLatest(
					typeof options.confirm === "object" ? options.confirm : undefined,
				);
			}
			return ranges;
		} finally {
			const storageObjective = this._isAdaptiveReplicating
				? this.replicationController?.maxMemoryLimit
				: undefined;
			if (
				this._isAdaptiveReplicating !== wasAdaptiveReplicating ||
				storageObjective !== previousStorageObjective
			) {
				this.scheduleReplicationStatusRefresh();
			}
		}
	}

	private async applyReplicationRole(
		rangeOrEntry?: ReplicationOptions<R> | Entry<T> | Entry<T>[],
		options?: {
			reset?: boolean;
			checkDuplicates?: boolean;
			rebalance?: boolean;
			mergeSegments?: boolean;
			announce?: (
				msg: FullReplicationInfoMutation | AddedReplicationInfoMutation,
			) => void;
		},
	) {
		this.throwIfCheckedPruneRemoveBlocksLocalOperation(
			"replication range mutation",
		);
		const replicationOwnershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const requestedRole =
			rangeOrEntry &&
			(rangeOrEntry as ExistingReplicationOptions<R>).type === "resume"
				? (rangeOrEntry as ExistingReplicationOptions<R>).default
				: (rangeOrEntry ?? true);
		const requestsAdaptiveRole =
			requestedRole === true ||
			(!(requestedRole instanceof Entry) &&
				!(requestedRole instanceof ReplicationRangeMessage) &&
				isAdaptiveReplicatorOption(requestedRole as ReplicationOptions<R>));
		const requestedFixedRoleIsEmpty =
			Array.isArray(requestedRole) && requestedRole.length === 0;
		const finalRequestedRange = Array.isArray(requestedRole)
			? requestedRole[requestedRole.length - 1]
			: requestedRole;
		const fixedRoleOffsetWasProvided =
			finalRequestedRange instanceof Entry ||
			finalRequestedRange instanceof ReplicationRangeMessage ||
			(typeof finalRequestedRange === "object" &&
				finalRequestedRange !== null &&
				(finalRequestedRange as FixedReplicationOptions).offset != null);
		if (
			!isUnreplicationOptions(requestedRole as ReplicationOptions<R>) &&
			!requestsAdaptiveRole &&
			!requestedFixedRoleIsEmpty &&
			(options?.reset === true || !fixedRoleOffsetWasProvided)
		) {
			// A fixed replacement is a local role transition, not an additive
			// per-entry replication request. Match _replicate's implicit reset for
			// fixed options without an offset as well as an explicit reset.
			// Invalidate already-admitted adaptive planners synchronously, before
			// entry-coordinate resolution or the ownership lane can yield, and
			// prevent a new adaptive planner from starting while the fixed
			// replacement is being committed.
			this._instanceLifecycle?.bumpRoleGeneration();
			this._isAdaptiveReplicating = false;
		}
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
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
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
					this.throwIfReplicationOwnershipLifecycleInactive(
						replicationOwnershipLifecycleController,
					);
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

		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		return this._replicate(
			range,
			options,
			replicationOwnershipLifecycleController,
		);
	}

	async unreplicate(
		rangeOrEntry?: Entry<T> | { id: Uint8Array }[],
		options?: { confirm?: boolean | ReplicationConfirmationOptions },
	) {
		const wasAdaptiveReplicating = this._isAdaptiveReplicating;
		const previousStorageObjective = wasAdaptiveReplicating
			? this.replicationController?.maxMemoryLimit
			: undefined;
		this.throwIfCheckedPruneRemoveBlocksLocalOperation(
			"replication range mutation",
		);
		const replicationOwnershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		try {
			const result = await this._unreplicate(
				rangeOrEntry,
				replicationOwnershipLifecycleController,
			);
			if (options?.confirm) {
				await this._v2Send.confirmLatest(
					typeof options.confirm === "object" ? options.confirm : undefined,
				);
			}
			return result;
		} finally {
			const storageObjective = this._isAdaptiveReplicating
				? this.replicationController?.maxMemoryLimit
				: undefined;
			if (
				this._isAdaptiveReplicating !== wasAdaptiveReplicating ||
				storageObjective !== previousStorageObjective
			) {
				this.scheduleReplicationStatusRefresh();
			}
		}
	}

	private async _unreplicate(
		rangeOrEntry: Entry<T> | { id: Uint8Array }[] | undefined,
		replicationOwnershipLifecycleController: AbortController,
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		let segmentIds: Uint8Array[];
		if (rangeOrEntry instanceof Entry) {
			let range: FixedReplicationOptions = {
				factor: 1,
				offset: await this.domain.fromEntry(rangeOrEntry),
			};
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			const indexed = this.replicationIndex.iterate({
				query: {
					width: 1,
					start1: range.offset /* ,
					hash: this.node.identity.publicKey.hashcode(), */,
				},
			});
			segmentIds = (await indexed.all()).map((x) => x.id.key as Uint8Array);
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
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
			// Invalidate already-admitted adaptive planners synchronously, before
			// this operation waits for the ownership lane. Otherwise a planner
			// that passed its preliminary role checks can enqueue a stale dynamic
			// range after this full unreplication removes the previous role.
			this._instanceLifecycle?.bumpRoleGeneration();
			this._isReplicating = false;
			this._isAdaptiveReplicating = false;
			await this.removeReplicator(this.node.identity.publicKey, {
				replicationOwnershipLifecycleController,
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			try {
				await this.replicationChangeDebounceFn.flush?.();
			} catch (error: any) {
				if (!isNotStartedError(error)) {
					throw error;
				}
			}
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			await this.pruneIndexedEntriesNoLongerLed({
				useDefaultRoleAge: true,
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			await this.pruneCurrentHeadsNoLongerLed({
				useDefaultRoleAge: true,
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
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
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		if (rangesToRemove.length === 0) {
			return;
		}
		const removedSegmentIds: Uint8Array[] = [];
		let mutationError: unknown;
		try {
			await this.removeReplicationRanges(
				rangesToRemove,
				this.node.identity.publicKey,
				{
					onRemoved: (ranges) => {
						removedSegmentIds.push(...ranges.map((range) => range.id));
					},
				},
				replicationOwnershipLifecycleController,
			);
		} catch (error) {
			mutationError = error;
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		if (removedSegmentIds.length === 0) {
			if (mutationError !== undefined) {
				throw mutationError;
			}
			return;
		}
		try {
			await this._announcements.sendReplicationAnnouncement(
				{ stopped: { segmentIds: removedSegmentIds } },
				replicationOwnershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
		} catch (announcementError) {
			if (mutationError !== undefined) {
				throw new AggregateError(
					[mutationError, announcementError],
					"Replication ranges were removed but their announcement failed",
				);
			}
			throw announcementError;
		}
		if (mutationError !== undefined) {
			throw mutationError;
		}
	}

	private async removeReplicator(
		key: PublicSignKey | string,
		options?: {
			cleanupIfSubscriptionSuperseded?: boolean;
			expectedWarmupSession?: WarmupSession | null;
			noEvent?: boolean;
			onRemoved?: (state: { wasReplicator: boolean }) => void;
			replicationLifecycleController?: AbortController;
			replicationOwnershipLifecycleController?: AbortController;
			shouldRemove?: () => boolean;
			subscriptionEpoch?: object | null;
		},
	): Promise<boolean> {
		const replicationOwnershipLifecycleController =
			options?.replicationOwnershipLifecycleController ??
			this.captureReplicationOwnershipLifecycle();
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		const keyHash = typeof key === "string" ? key : key.hashcode();
		const expectedWarmupSession =
			options?.expectedWarmupSession !== undefined
				? options.expectedWarmupSession
				: (this.joinWarmup._warmupSessionsByTarget.get(keyHash) ?? null);
		// Stage 3 (seam 1): the three ownership predicates delegate onto the
		// session registry / instance lifecycle. `undefined` keeps meaning
		// "removal not scoped to that identity" — the guards stay in the
		// caller expressions exactly as before.
		const ownsSubscriptionEpoch = () =>
			options?.subscriptionEpoch === undefined ||
			this._peerSessions.isCurrent(keyHash, options.subscriptionEpoch);
		const ownsReplicationLifecycle = () =>
			options?.replicationLifecycleController === undefined ||
			this._instanceLifecycle!.isMembershipActiveFor(
				options.replicationLifecycleController,
			);
		const ownsReplicationOwnershipLifecycle = () =>
			this._instanceLifecycle!.isActiveFor(
				replicationOwnershipLifecycleController,
			);
		const cancelExpectedJoinWarmupTarget = () => {
			if (
				expectedWarmupSession !== null &&
				this.joinWarmup._warmupSessionsByTarget.get(keyHash) ===
					expectedWarmupSession
			) {
				this.joinWarmup.cancelJoinWarmupTarget(keyHash);
			}
		};
		const isMe = this.node.identity.publicKey.hashcode() === keyHash;
		// The registry acquire captures the gate-map instance: close/reopen
		// replaces the map, and the release in this call's finally must drain
		// the exact map it incremented — a late release against a fresh open's
		// map would corrupt that open's refcounts. (Every path to the acquire
		// runs behind an ownership-lifecycle throw, so the map current at
		// acquisition is the map that was current at this call's entry.)
		let releaseReceiveCleanupGate: (() => void) | undefined;
		const checkedPruneCoordinator = this._checkedPrune;
		const isSpeculativePeerRemoval =
			!isMe && options?.shouldRemove !== undefined;
		const releaseCheckedPrunePeerRemovalFence = isSpeculativePeerRemoval
			? checkedPruneCoordinator.fencePeerRemoval(keyHash)
			: undefined;
		const blockPeerReceiveAdmission = () => {
			if (!releaseReceiveCleanupGate) {
				releaseReceiveCleanupGate =
					this._peerSessions.acquireReceiveCleanupGate(keyHash);
				this.dispatchPersistedReceiptReadinessChange(keyHash);
			}
		};
		if (!isMe && !isSpeculativePeerRemoval) {
			// Revoke this peer's receipts synchronously, before this removal can
			// wait behind either the per-peer apply queue or the ownership lane.
			// Keep receive admission open until removal is actually admitted so
			// fresh authenticated activity can still cancel a liveness eviction.
			this.cleanupCheckedPrunePeer(
				keyHash,
				replicationOwnershipLifecycleController,
				checkedPruneCoordinator,
			);
		}
		const blockAndDrainPeerReceives = async () => {
			blockPeerReceiveAdmission();
			await this.drainPeerReceiveHandlers(keyHash);
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
		};
		const cleanupDisconnectedPeer = async () => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			await blockAndDrainPeerReceives();
			this.removePeerFromGidPeerHistory(keyHash);
			this.cleanupPeerDisconnectTracking(
				keyHash,
				replicationOwnershipLifecycleController,
			);
			this.removeRepairFrontierTarget(keyHash, {
				expectedWarmupSession,
			});
			this._recentRepairDispatch.delete(keyHash);
			if (!isMe) {
				await this.syncronizer.onPeerDisconnected(keyHash);
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
			}
		};
		let removed = false;
		let removalCallCompleted = false;
		let replicationInfoRecoveryEpochAdvanced = false;

		// Replication-info updates already serialize per peer. Put the hash-wide
		// removal on the same queue so a newer reset cannot be deleted underneath
		// itself by an older unsubscribe callback.
		try {
			await this.withReplicationInfoApplyQueue(keyHash, async () => {
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				if (!ownsReplicationLifecycle()) {
					return;
				}
				if (!ownsSubscriptionEpoch()) {
					// A reconnect may supersede an unsubscribe before its destructive
					// removal starts. Still retire the old connection's sync/request state
					// in lane order so the reconnect barrier cannot inherit stale caches.
					if (options?.cleanupIfSubscriptionSuperseded) {
						await cleanupDisconnectedPeer();
					}
					return;
				}
				if (options?.shouldRemove && !options.shouldRemove()) {
					return;
				}
				// Stop and drain admitted receives before taking the global range lane.
				// User receive hooks may re-enter replicate()/unreplicate(); holding the
				// range lane while waiting for such a hook would deadlock on our own tail.
				await blockAndDrainPeerReceives();
				if (
					!ownsReplicationOwnershipLifecycle() ||
					!ownsReplicationLifecycle() ||
					!ownsSubscriptionEpoch() ||
					(options?.shouldRemove && !options.shouldRemove())
				) {
					if (
						!ownsSubscriptionEpoch() &&
						options?.cleanupIfSubscriptionSuperseded
					) {
						await cleanupDisconnectedPeer();
					}
					return;
				}
				let wasReplicator = false;
				let deleted: ReplicationRangeIndexable<R>[] = [];
				let ownerHasRanges = false;
				let mutationError: unknown;
				const mutationCommitted = await this.withReceiveOwnershipMutationQueue(
					async () => {
						if (
							!ownsReplicationOwnershipLifecycle() ||
							!ownsReplicationLifecycle() ||
							!ownsSubscriptionEpoch() ||
							(options?.shouldRemove && !options.shouldRemove())
						) {
							return false;
						}
						wasReplicator = this.uniqueReplicators.has(keyHash);
						deleted = (
							await this.replicationIndex
								.iterate({
									query: { hash: keyHash },
								})
								.all()
						).map((result) => result.value);
						this.throwIfReplicationOwnershipLifecycleInactive(
							replicationOwnershipLifecycleController,
						);
						// Liveness evidence can arrive while the scan is pending. Admission is
						// already blocked and older receives are drained; revalidate immediately
						// before destructive mutation.
						if (
							!ownsReplicationOwnershipLifecycle() ||
							!ownsReplicationLifecycle() ||
							!ownsSubscriptionEpoch() ||
							(options?.shouldRemove && !options.shouldRemove())
						) {
							return false;
						}
						cancelExpectedJoinWarmupTarget();
						if (isSpeculativePeerRemoval) {
							// The final liveness check committed this removal. Convert its
							// temporary quorum fence into permanent generation revocation
							// before mutating ownership.
							this.cleanupCheckedPrunePeer(
								keyHash,
								replicationOwnershipLifecycleController,
								checkedPruneCoordinator,
							);
						}
						if (!isMe) {
							// This is the last synchronous current-generation boundary
							// before destructive work. Fence parked receives now because
							// the coherent delete can durably mutate and then fail or poison.
							this.advanceReplicationInfoRecoveryEpoch(keyHash);
							replicationInfoRecoveryEpochAdvanced = true;
						}
						const deletion = await this.deleteReplicationRangesCoherently(
							deleted,
							keyHash,
						);
						deleted = deletion.removed;
						ownerHasRanges = deletion.ownerHasRanges;
						mutationError = deletion.error;
						return true;
					},
					replicationOwnershipLifecycleController,
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				if (!mutationCommitted) {
					if (
						!ownsSubscriptionEpoch() &&
						options?.cleanupIfSubscriptionSuperseded
					) {
						await cleanupDisconnectedPeer();
					}
					return;
				}
				if (deleted.length > 0 || (wasReplicator && !ownerHasRanges)) {
					// Some committed removals intentionally suppress replication:change,
					// and liveness cleanup can retire membership without a leave event.
					// Refresh from the committed post-state instead of relying on either
					// optional notification path.
					this.scheduleReplicationStatusRefresh();
				}
				if (options?.noEvent !== true && deleted.length > 0) {
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
						range: x,
						type: "removed",
						timestamp,
					});
				}

				const pendingMaturity = this.pendingMaturity.get(keyHash);
				if (!ownerHasRanges && pendingMaturity) {
					for (const [_k, v] of pendingMaturity) {
						clearTimeout(v.timeout);
					}
					pendingMaturity.clear();
					this.pendingMaturity.delete(keyHash);
				}

				// Keep local sync/prune state consistent even when a peer disappears
				// through replication-info updates without a topic unsubscribe event.
				await cleanupDisconnectedPeer();

				if (!isMe) {
					this.rebalanceParticipationDebounced?.call();
				}
				removed = true;
				this._peerSessions?.noteReplicatorRemoved(
					keyHash,
					options?.subscriptionEpoch,
				);
				if (!ownerHasRanges) {
					options?.onRemoved?.({ wasReplicator });
				}
				let announcementError: unknown;
				if (isMe && !ownerHasRanges) {
					try {
						await this._announcements.sendReplicationAnnouncement(
							{ full: { segments: [] } },
							replicationOwnershipLifecycleController,
						);
					} catch (error) {
						announcementError = error;
					}
				}
				if (mutationError !== undefined && announcementError !== undefined) {
					throw new AggregateError(
						[mutationError, announcementError],
						"Replication ranges were removed but their announcement failed",
					);
				}
				if (mutationError !== undefined) {
					throw mutationError;
				}
				if (announcementError !== undefined) {
					throw announcementError;
				}
			});
			removalCallCompleted = true;
		} finally {
			if (releaseReceiveCleanupGate) {
				releaseReceiveCleanupGate();
				this.dispatchPersistedReceiptReadinessChange(keyHash);
			}
			if (
				replicationInfoRecoveryEpochAdvanced &&
				ownsReplicationOwnershipLifecycle() &&
				ownsReplicationLifecycle() &&
				ownsSubscriptionEpoch()
			) {
				// The first recovery arm ran while receive admission was fenced.
				// Re-arm after releasing the cleanup gate so a still-open peer can
				// deliver the authoritative Full that completes recovery.
				const peerSession = this._peerSessions.current(keyHash);
				if (peerSession?.phase === "open") {
					const receiveEpoch = this._peerSessions.receiveEpoch(keyHash);
					this._v2Receive.advanceRecovery({
						peerHash: keyHash,
						peerSession,
						receiveEpoch,
					});
					this._v2Receive.reAdvertiseLocalCapabilityForRecovery({
						peerHash: keyHash,
						peerSession,
						receiveEpoch,
					});
				}
			}
			if (isSpeculativePeerRemoval) {
				const cancelledByFreshActivity =
					removalCallCompleted &&
					!removed &&
					ownsReplicationOwnershipLifecycle() &&
					ownsReplicationLifecycle() &&
					ownsSubscriptionEpoch() &&
					options.shouldRemove?.() === false;
				if (!cancelledByFreshActivity) {
					// Only current-epoch authenticated liveness evidence may preserve
					// this generation. Errors and lifecycle/epoch changes fail closed.
					this.cleanupCheckedPrunePeer(
						keyHash,
						replicationOwnershipLifecycleController,
						checkedPruneCoordinator,
					);
				}
			}
			releaseCheckedPrunePeerRemovalFence?.();
		}
		return removed;
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
		const uniqueIds = [
			...new Map(ids.map((id) => [toHexString(id), id])).values(),
		];
		const resolvedById = new Map<string, ReplicationRangeIndexable<R>>();
		const ownerHash = from.hashcode();
		for (
			let i = 0;
			i < uniqueIds.length;
			i += REPLICATION_RANGE_ID_QUERY_BATCH_SIZE
		) {
			const query = new And([
				new StringMatch({ key: "hash", value: ownerHash }),
				new Or(
					uniqueIds
						.slice(i, i + REPLICATION_RANGE_ID_QUERY_BATCH_SIZE)
						.map((id) => new ByteMatchQuery({ key: "id", value: id })),
				),
			]);
			for (const result of await this.replicationIndex
				.iterate({ query })
				.all()) {
				resolvedById.set(result.value.idString, result.value);
			}
		}
		return [...resolvedById.values()];
	}
	private removeReplicationRanges(
		ranges: ReplicationRangeIndexable<R>[],
		from: PublicSignKey,
		options?: {
			onRemoved?: (ranges: ReplicationRangeIndexable<R>[]) => void;
			shouldRemove?: () => boolean;
			onDurableRemoveCommitted?: () => boolean | void;
		},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<boolean> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		return this.withReceiveOwnershipMutationQueue(
			() => this.removeReplicationRangesUnlocked(ranges, from, options),
			replicationOwnershipLifecycleController,
		);
	}

	private async removeReplicationRangesUnlocked(
		ranges: ReplicationRangeIndexable<R>[],
		from: PublicSignKey,
		options?: {
			onRemoved?: (ranges: ReplicationRangeIndexable<R>[]) => void;
			shouldRemove?: () => boolean;
			onDurableRemoveCommitted?: () => boolean | void;
		},
	): Promise<boolean> {
		if (ranges.length === 0) {
			return false;
		}
		if (options?.shouldRemove && !options.shouldRemove()) {
			return false;
		}
		const expectedRangeById = new Map(
			ranges.map((range) => [range.idString, range]),
		);
		const expectedRanges = [...expectedRangeById.values()];
		const refreshedRanges: ReplicationRangeIndexable<R>[] = [];
		const ownerHash = from.hashcode();
		for (
			let i = 0;
			i < expectedRanges.length;
			i += REPLICATION_RANGE_ID_QUERY_BATCH_SIZE
		) {
			const results = await this.replicationIndex
				.iterate({
					query: new And([
						new StringMatch({ key: "hash", value: ownerHash }),
						new Or(
							expectedRanges
								.slice(i, i + REPLICATION_RANGE_ID_QUERY_BATCH_SIZE)
								.map(
									(range) =>
										new ByteMatchQuery({
											key: "id",
											value: range.id,
										}),
								),
						),
					]),
				})
				.all();
			for (const result of results) {
				const expected = expectedRangeById.get(result.value.idString);
				if (expected?.rangeHash === result.value.rangeHash) {
					refreshedRanges.push(result.value);
				}
			}
		}
		ranges = refreshedRanges;
		if (
			ranges.length === 0 ||
			(options?.shouldRemove && !options.shouldRemove())
		) {
			return false;
		}
		const deletion = await this.deleteReplicationRangesCoherently(
			ranges,
			ownerHash,
		);
		ranges = deletion.removed;
		if (
			(options?.shouldRemove && !options.shouldRemove()) ||
			options?.onDurableRemoveCommitted?.() === false
		) {
			await deletion.rollback();
			return false;
		}
		options?.onRemoved?.(ranges);

		if (ranges.length > 0) {
			this.events.dispatchEvent(
				new CustomEvent<ReplicationChangeEvent>("replication:change", {
					detail: { publicKey: from },
				}),
			);
		}

		if (ranges.length > 0 && !from.equals(this.node.identity.publicKey)) {
			this.rebalanceParticipationDebounced?.call();
		}
		if (deletion.error !== undefined) {
			// The caller will observe the failure and cannot publish its normal
			// negative work. Queue only rows proven absent by the durable probe.
			const timestamp = BigInt(Date.now());
			for (const range of ranges) {
				this.replicationChangeDebounceFn.add({
					range,
					type: "removed",
					timestamp,
				});
			}
			throw deletion.error;
		}
		return ranges.length > 0;
	}

	private validateReplicationRangeAnnouncement(
		ranges: readonly { mode: unknown }[],
	): void {
		if (ranges.length > MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT) {
			throw new Error(
				`Replication range announcement exceeds the ${MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT}-range limit`,
			);
		}
		for (let index = 0; index < ranges.length; index++) {
			const mode = ranges[index].mode;
			if (
				mode !== ReplicationIntent.Strict &&
				mode !== ReplicationIntent.NonStrict
			) {
				throw new Error(
					`Invalid replication range mode at index ${index}: ${String(mode)}`,
				);
			}
		}
	}

	private validatePersistedReplicationRangeSnapshot(
		ranges: readonly { mode: unknown }[],
	): void {
		try {
			this.validateReplicationRangeAnnouncement(ranges);
		} catch (cause) {
			const failure = new Error(
				"Persisted replication ownership is invalid and cannot be announced",
				{ cause },
			);
			this.poisonReplicationOwnership(failure);
			throw failure;
		}
	}

	private validateStoppedReplicationAnnouncement(
		segmentIds: readonly Uint8Array[],
	): void {
		if (segmentIds.length > MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT) {
			throw new Error(
				`Stopped-replication announcement exceeds the ${MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT}-segment limit`,
			);
		}
	}

	private async addReplicationRange(
		ranges: ReplicationRangeIndexable<any>[],
		from: PublicSignKey,
		options: {
			reset?: boolean;
			rebalance?: boolean;
			checkDuplicates?: boolean;
			timestamp?: number;
			allowOrderedReplacementPairs?: boolean;
			onConfirmedDurableStateChanged?: () => void;
			onDurableApplyCommitted?: () => boolean | void;
			shouldApply?: () => boolean;
		} = {},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.validateReplicationRangeAnnouncement(ranges);
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		// Authorization can be asynchronous or re-entrant. Never invoke it while
		// holding the global mutation lane.
		if (this._isTrustedReplicator && !(await this._isTrustedReplicator(from))) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			return undefined;
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		return this.withReceiveOwnershipMutationQueue(
			() =>
				this.addReplicationRangeUnlocked(
					ranges,
					from,
					options,
					replicationOwnershipLifecycleController,
				),
			replicationOwnershipLifecycleController,
		);
	}

	private async addReplicationRangeUnlocked(
		ranges: ReplicationRangeIndexable<any>[],
		from: PublicSignKey,
		{
			reset,
			checkDuplicates,
			timestamp: ts,
			rebalance,
			allowOrderedReplacementPairs,
			onConfirmedDurableStateChanged,
			onDurableApplyCommitted,
			shouldApply,
		}: {
			reset?: boolean;
			rebalance?: boolean;
			checkDuplicates?: boolean;
			timestamp?: number;
			allowOrderedReplacementPairs?: boolean;
			onConfirmedDurableStateChanged?: () => void;
			onDurableApplyCommitted?: () => boolean | void;
			shouldApply?: () => boolean;
		} = {},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.validateReplicationRangeAnnouncement(ranges);
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		// This predicate is intentionally checked inside the global ownership lane.
		// An adaptive update delayed by authorization must not commit after a newer
		// full unreplication already completed in that lane.
		if (shouldApply && !shouldApply()) {
			return [];
		}
		const applySuperseded = Symbol("replication-range-apply-superseded");
		const throwIfApplySuperseded = () => {
			if (shouldApply && !shouldApply()) {
				throw applySuperseded;
			}
		};
		const fromHash = from.hashcode();
		const incomingRangesById = new Map<
			string,
			ReplicationRangeIndexable<any>
		>();
		const incomingRangeCountsById = new Map<string, number>();
		for (const range of ranges) {
			if (range.hash !== fromHash) {
				throw new Error(
					`Replication range owner mismatch for id ${range.idString}: expected ${fromHash}, received ${range.hash}`,
				);
			}
			const count = (incomingRangeCountsById.get(range.idString) ?? 0) + 1;
			incomingRangeCountsById.set(range.idString, count);
			if (
				count > 1 &&
				(!allowOrderedReplacementPairs || reset === true || count > 2)
			) {
				throw new Error(
					`Duplicate replication range id in announcement: ${range.idString}`,
				);
			}
			// Rolling-upgrade relaxation on the live V2 Added path: a peer may
			// still express a non-reset replacement as the retired geometry
			// followed by the current geometry under one id. The sender is already
			// authorized to replace that id with the final item, so collapsing an
			// EXACT two-item incremental pair to its last item accepts the older
			// wire shape without broadening authority. Deliberately narrow — reset
			// announcements and any run longer than two still fail as duplicates.
			incomingRangesById.set(range.idString, range);
		}
		const incomingRanges = [...incomingRangesById.values()];
		for (
			let i = 0;
			i < incomingRanges.length;
			i += REPLICATION_RANGE_ID_QUERY_BATCH_SIZE
		) {
			const existing = await this.replicationIndex
				.iterate(
					{
						query: new Or(
							incomingRanges
								.slice(i, i + REPLICATION_RANGE_ID_QUERY_BATCH_SIZE)
								.map(
									(range) =>
										new ByteMatchQuery({
											key: "id",
											value: range.id,
										}),
								),
						),
					},
					{ reference: true },
				)
				.all();
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			const conflicting = existing.find(
				(result) => result.value.hash !== fromHash,
			)?.value;
			if (conflicting) {
				throw new Error(
					`Replication range id is already owned by another replicator: ${conflicting.idString}`,
				);
			}
		}
		ranges = incomingRanges;
		// Preserve what the peer announced before duplicate filtering can empty the
		// working array. A repeated authoritative/non-empty announcement is still
		// proof of live membership after this process reopens.
		const announcedReplication = ranges.length > 0;
		let isNewReplicator = false;
		let timestamp = BigInt(ts ?? +new Date());
		rebalance = rebalance == null ? true : rebalance;
		// Complete every fallible policy lookup before a reset crosses its
		// destructive boundary. Later failures are handled by the positive-write
		// rollback path and publish confirmed negative state.
		const now = +new Date();
		const minRoleAge =
			ranges.length > 0 ? await this.getDefaultMinRoleAge() : 0;
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);

		let diffs: ReplicationChanges<ReplicationRangeIndexable<R>>;
		let deleted: ReplicationRangeIndexable<R>[] | undefined = undefined;
		let previousRangesById = new Map<string, ReplicationRangeIndexable<R>>();
		let isStoppedReplicating = false;
		let wasReplicatorBeforeDestructiveReset = false;
		let resetFailureLeaveEmitted = false;
		let resetDeletionRollback: (() => Promise<void>) | undefined;
		const publishConfirmedResetStop = (ownerHasRanges: boolean) => {
			if (ownerHasRanges || resetFailureLeaveEmitted) {
				return;
			}
			const stoppedTransition =
				wasReplicatorBeforeDestructiveReset ||
				this.uniqueReplicators.has(fromHash);
			this.uniqueReplicators.delete(fromHash);
			this._replicatorJoinEmitted.delete(fromHash);
			if (stoppedTransition) {
				resetFailureLeaveEmitted = true;
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
						detail: { publicKey: from },
					}),
				);
			}
		};
		if (reset) {
			deleted = (
				await this.replicationIndex
					.iterate({
						query: { hash: from.hashcode() },
					})
					.all()
			).map((x) => x.value);
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);

			let prevCount = deleted.length;

			const existingById = new Map(deleted.map((x) => [x.idString, x]));
			const hasSameRanges =
				deleted.length === ranges.length &&
				ranges.every((range) => {
					const existing = existingById.get(range.idString);
					return (
						existing != null &&
						existing.equalRange(range) &&
						existing.mode === range.mode
					);
				});

			// Avoid churn on repeated full-state announcements that don't change any
			// replication ranges. This prevents unnecessary `replication:change`
			// events and rebalancing cascades.
			if (hasSameRanges) {
				diffs = [];
			} else {
				wasReplicatorBeforeDestructiveReset =
					this.uniqueReplicators.has(fromHash);
				throwIfApplySuperseded();
				const deletion = await this.deleteReplicationRangesCoherently(
					deleted,
					fromHash,
					{ preserveOwnerMembership: ranges.length > 0 },
				);
				deleted = deletion.removed;
				resetDeletionRollback = deletion.rollback;
				if (shouldApply && !shouldApply()) {
					await deletion.rollback();
					return [];
				}

				diffs = [
					...deleted.map((x) => {
						return { range: x, type: "removed" as const, timestamp };
					}),
					...(deletion.error === undefined
						? ranges.map((x) => {
								return { range: x, type: "added" as const, timestamp };
							})
						: []),
				];
				if (deletion.error !== undefined) {
					if (diffs.length > 0) {
						this.events.dispatchEvent(
							new CustomEvent<ReplicationChangeEvent>("replication:change", {
								detail: { publicKey: from },
							}),
						);
						if (rebalance) {
							for (const diff of diffs) {
								this.replicationChangeDebounceFn.add(diff);
							}
						}
						if (!from.equals(this.node.identity.publicKey)) {
							this.rebalanceParticipationDebounced?.call();
						}
						if (
							from.equals(this.node.identity.publicKey) &&
							this._replicationRangeMutationFailure === undefined
						) {
							onConfirmedDurableStateChanged?.();
						}
					}
					publishConfirmedResetStop(deletion.ownerHasRanges);
					throw deletion.error;
				}
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
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				for (const result of results) {
					existing.push(result.value);
				}
			}

			const prevCountForOwner = await this.replicationIndex.count({
				query: new StringMatch({ key: "hash", value: fromHash }),
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
			isNewReplicator = prevCountForOwner === 0;

			if (checkDuplicates && prevCountForOwner > 0) {
				let deduplicated: ReplicationRangeIndexable<any>[] = [];

				// TODO also deduplicate/de-overlap among the ranges that ought to be inserted?
				for (const range of ranges) {
					const hasCoveringRange = await countCoveringRangesSameOwner(
						this.replicationIndex,
						range,
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						replicationOwnershipLifecycleController,
					);
					if (!hasCoveringRange) {
						deduplicated.push(range);
					}
				}
				ranges = deduplicated;
			}
			let existingMap = new Map<string, ReplicationRangeIndexable<R>>();
			for (const result of existing) {
				existingMap.set(result.idString, result);
			}
			const projectedCount =
				prevCountForOwner +
				ranges.reduce(
					(count, range) => count + (existingMap.has(range.idString) ? 0 : 1),
					0,
				);
			if (projectedCount > MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT) {
				throw new Error(
					`Replication range ownership exceeds the ${MAX_REPLICATION_RANGES_PER_ANNOUNCEMENT}-range limit`,
				);
			}
			previousRangesById = existingMap;

			let changes: ReplicationChanges<ReplicationRangeIndexable<R>> = ranges
				.map((x) => {
					const prev = existingMap.get(x.idString);
					if (prev) {
						if (prev.equalRange(x) && prev.mode === x.mode) {
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

		let isAllMature = true;

		const appliedPositiveRanges: ReplicationChange<
			ReplicationRangeIndexable<R>
		>[] = [];
		const rollbackAppliedPositiveRanges = async () => {
			for (const applied of [...appliedPositiveRanges].reverse()) {
				const range = applied.range;
				const current = (
					await this.replicationIndex
						.iterate({
							query: new And([
								new StringMatch({ key: "hash", value: range.hash }),
								new ByteMatchQuery({ key: "id", value: range.id }),
							]),
						})
						.all()
				)[0]?.value;
				if (!current || current.rangeHash !== range.rangeHash) {
					continue;
				}
				const previous = reset
					? undefined
					: previousRangesById.get(range.idString);
				if (previous) {
					await this.replicationIndex.put(previous);
					this.putNativeReplicationRange(previous);
				} else {
					await this.replicationIndex.del({
						query: new And([
							new StringMatch({ key: "hash", value: range.hash }),
							new ByteMatchQuery({ key: "id", value: range.id }),
						]),
					});
					this.deleteNativeReplicationRange(range);
				}
			}
			appliedPositiveRanges.length = 0;
			await this.updateOldestTimestampFromIndex();
		};
		const poisonFromPositiveRollback = (
			rollbackError: unknown,
			primaryError?: unknown,
		) => {
			const errors =
				primaryError === undefined || primaryError === rollbackError
					? [rollbackError]
					: [primaryError, rollbackError];
			const failure = new AggregateError(
				errors,
				"Replication-range positive mutation rollback failed",
			);
			this.poisonReplicationOwnership(failure);
			return failure;
		};

		try {
			throwIfApplySuperseded();
			for (const diff of diffs) {
				if (diff.type !== "added") {
					continue;
				}
				throwIfApplySuperseded();
				appliedPositiveRanges.push(diff);
				await this.replicationIndex.put(diff.range);
				this.putNativeReplicationRange(diff.range);
				throwIfApplySuperseded();
			}
			if (reset && diffs.length > 0) {
				await this.updateOldestTimestampFromIndex();
				throwIfApplySuperseded();
			}
			if (onDurableApplyCommitted?.() === false) {
				throw applySuperseded;
			}
		} catch (error) {
			const superseded = error === applySuperseded;
			let outcomeError = error;
			if (appliedPositiveRanges.length > 0) {
				try {
					await rollbackAppliedPositiveRanges();
				} catch (rollbackError) {
					outcomeError = poisonFromPositiveRollback(rollbackError, error);
				}
			}
			if (superseded) {
				if (resetDeletionRollback) {
					try {
						await resetDeletionRollback();
					} catch (rollbackError) {
						outcomeError = rollbackError;
					}
				}
				if (outcomeError === applySuperseded) {
					return [];
				}
				throw outcomeError;
			}
			if (reset) {
				const negativeDiffs = diffs.filter((diff) => diff.type !== "added");
				if (negativeDiffs.length > 0) {
					this.events.dispatchEvent(
						new CustomEvent<ReplicationChangeEvent>("replication:change", {
							detail: { publicKey: from },
						}),
					);
					if (rebalance) {
						for (const diff of negativeDiffs) {
							this.replicationChangeDebounceFn.add(diff);
						}
					}
					if (
						from.equals(this.node.identity.publicKey) &&
						this._replicationRangeMutationFailure === undefined
					) {
						onConfirmedDurableStateChanged?.();
					}
				}
				if (!from.equals(this.node.identity.publicKey)) {
					this.rebalanceParticipationDebounced?.call();
				}
				try {
					const ownerHasRanges =
						(await this.replicationIndex.count({
							query: { hash: fromHash },
						})) > 0;
					publishConfirmedResetStop(ownerHasRanges);
				} catch (membershipProbeError) {
					const failure = new AggregateError(
						outcomeError === membershipProbeError
							? [membershipProbeError]
							: [outcomeError, membershipProbeError],
						"Could not determine replication membership after failed reset rollback",
					);
					this.poisonReplicationOwnership(failure);
					outcomeError = failure;
				}
			}
			throw outcomeError;
		}
		if (diffs.length > 0) {
			// From this point onward the durable/native range mutation has
			// committed and its rollback window has closed. If any later local
			// bookkeeping fails, the caller must publish an authoritative snapshot
			// instead of leaving peers on the pre-mutation geometry.
			onConfirmedDurableStateChanged?.();
		}

		const clearPendingMaturityForRange = (
			range: ReplicationRangeIndexable<R>,
		) => {
			const pendingFromPeer = this.pendingMaturity.get(range.hash);
			const pending = pendingFromPeer?.get(range.idString);
			if (!pending || !pendingFromPeer) {
				return;
			}
			clearTimeout(pending.timeout);
			pendingFromPeer.delete(range.idString);
			if (pendingFromPeer.size === 0) {
				this.pendingMaturity.delete(range.hash);
			}
		};
		for (const diff of diffs) {
			if (diff.type !== "added") {
				clearPendingMaturityForRange(diff.range);
			}
		}
		for (const applied of appliedPositiveRanges) {
			const range = applied.range;
			if (!reset) {
				this.oldestOpenTime = Math.min(
					Number(range.timestamp),
					this.oldestOpenTime,
				);
			}
			if (!isMatured(range, now, minRoleAge)) {
				isAllMature = false;
				this.schedulePendingMaturity(
					applied,
					from,
					{
						rebalance,
						waitMs: Math.max(minRoleAge - (now - Number(range.timestamp)), 0),
					},
					replicationOwnershipLifecycleController,
				);
			}
		}

		// Membership becomes visible only after every awaited positive mutation has
		// completed. A non-reset duplicate remains positive liveness evidence.
		const announcedStopped = reset === true && !announcedReplication;
		const stoppedTransition = announcedStopped
			? wasReplicatorBeforeDestructiveReset ||
				this.uniqueReplicators.delete(fromHash)
			: false;
		if (announcedStopped) {
			this._replicatorJoinEmitted.delete(fromHash);
		} else if (announcedReplication) {
			this.uniqueReplicators.add(fromHash);
		}

		if (diffs.length > 0) {
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
		if (
			announcedReplication &&
			!from.equals(this.node.identity.publicKey) &&
			!this._replicatorJoinEmitted.has(fromHash)
		) {
			this._replicatorJoinEmitted.add(fromHash);
			this.events.dispatchEvent(
				new CustomEvent<ReplicatorJoinEvent>("replicator:join", {
					detail: { publicKey: from },
				}),
			);
		}
		return diffs;
	}

	async startAnnounceReplicating(
		range: ReplicationRangeIndexable<R>[],
		options: {
			reset?: boolean;
			checkDuplicates?: boolean;
			rebalance?: boolean;
			shouldApply?: () => boolean;
			announce?: (
				msg: FullReplicationInfoMutation | AddedReplicationInfoMutation,
			) => void;
		} = {},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		await this.ensureCurrentHeadCoordinatesIndexed(
			replicationOwnershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);

		let confirmedDurableStateChanged = false;
		let change: ReplicationChanges<ReplicationRangeIndexable<R>> | undefined;
		try {
			change = await this.addReplicationRange(
				range,
				this.node.identity.publicKey,
				{
					...options,
					onConfirmedDurableStateChanged: () => {
						confirmedDurableStateChanged = true;
					},
				},
				replicationOwnershipLifecycleController,
			);
		} catch (mutationError) {
			if (
				!confirmedDurableStateChanged ||
				!this.isRepairLifecycleActive(replicationOwnershipLifecycleController)
			) {
				throw mutationError;
			}

			let announcementError: unknown;
			try {
				const segments = (await this.getMyReplicationSegments()).map((range) =>
					range.toReplicationRange(),
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				this.validatePersistedReplicationRangeSnapshot(segments);
				await this._announcements.sendReplicationAnnouncement(
					{ full: { segments } },
					replicationOwnershipLifecycleController,
				);
			} catch (error) {
				announcementError = error;
			}
			if (announcementError !== undefined) {
				throw new AggregateError(
					[mutationError, announcementError],
					"Replication state changed durably but its corrective announcement failed",
				);
			}
			throw mutationError;
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);

		if (!change) {
			warn("Not allowed to replicate by canReplicate");
		}

		if (change) {
			if (options.shouldApply && !options.shouldApply()) {
				return;
			}
			// Local replacements are represented as a negative `replaced` fact for
			// the retired geometry followed by an `added` fact for the durable
			// replacement. Only the positive/current fact belongs on the wire:
			// announcing both would send the same range id twice, which receivers
			// correctly reject as an ambiguous ownership announcement.
			const added = change.filter((x) => x.type === "added");
			if (added.length > 0) {
				// Provider discovery keep-alive (best-effort). This enables bounded targeted fetches
				// without relying on any global subscriber list.
				try {
					const fanoutService = getSharedLogFanoutService(this.node.services);
					if (fanoutService?.provide && !this._providerHandle) {
						this.ensureLogProviderHandle(fanoutService);
					}
				} catch {
					// Best-effort only.
				}

				let message:
					| FullReplicationInfoMutation
					| AddedReplicationInfoMutation
					| undefined = undefined;
				if (options.reset) {
					message = {
						full: { segments: added.map((x) => x.range.toReplicationRange()) },
					};
				} else {
					message = {
						added: { segments: added.map((x) => x.range.toReplicationRange()) },
					};
				}
				if (options.announce) {
					return options.announce(message);
				} else {
					await this._announcements.sendReplicationAnnouncement(
						message,
						replicationOwnershipLifecycleController,
						{ shouldSend: options.shouldApply },
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						replicationOwnershipLifecycleController,
					);
				}
			}
		}
	}

	private removePeerFromGidPeerHistory(publicKeyHash: string, gid?: string) {
		this._nativeSharedLogState?.removeGidPeer(publicKeyHash, gid);
		this._nativeBackbone?.removeGidPeer(publicKeyHash, gid);
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

	private removePeerFromGidPeerHistoryBatch(
		publicKeyHash: string,
		gids: Iterable<string>,
		options?: { skipNativeBackbone?: boolean },
	) {
		const gidArray = Array.isArray(gids) ? gids : [...gids];
		if (gidArray.length === 0) {
			return;
		}
		const nativeSharedLogState = this._nativeSharedLogState as
			| (typeof this._nativeSharedLogState & {
					removeGidPeers?: (peer: string, gids: Iterable<string>) => void;
			  })
			| undefined;
		const nativeBackbone = this._nativeBackbone as
			| (typeof this._nativeBackbone & {
					removeGidPeers?: (peer: string, gids: Iterable<string>) => void;
			  })
			| undefined;
		if (nativeSharedLogState?.removeGidPeers) {
			nativeSharedLogState.removeGidPeers(publicKeyHash, gidArray);
		} else if (this._nativeSharedLogState) {
			for (const gid of gidArray) {
				this._nativeSharedLogState.removeGidPeer(publicKeyHash, gid);
			}
		}
		if (options?.skipNativeBackbone !== true) {
			if (nativeBackbone?.removeGidPeers) {
				nativeBackbone.removeGidPeers(publicKeyHash, gidArray);
			} else if (this._nativeBackbone) {
				for (const gid of gidArray) {
					this._nativeBackbone.removeGidPeer(publicKeyHash, gid);
				}
			}
		}
		if (this._gidPeersHistory.size === 0) {
			return;
		}
		for (const gid of gidArray) {
			const gidMap = this._gidPeersHistory.get(gid);
			if (!gidMap) {
				continue;
			}
			gidMap.delete(publicKeyHash);
			if (gidMap.size === 0) {
				this._gidPeersHistory.delete(gid);
			}
		}
	}

	private deleteGidPeerHistory(gid: string) {
		this._nativeSharedLogState?.deleteGidPeers(gid);
		this._nativeBackbone?.deleteGidPeers(gid);
		this._gidPeersHistory.delete(gid);
	}

	addPeersToGidPeerHistory(
		gid: string,
		publicKeys: Iterable<string>,
		reset?: boolean,
	) {
		const publicKeyArray = [...publicKeys];
		this._nativeSharedLogState?.addGidPeers(
			gid,
			publicKeyArray,
			reset === true,
		);
		this._nativeBackbone?.addGidPeers(gid, publicKeyArray, reset === true);
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
		const hashArray = Array.isArray(hashes) ? hashes : [...hashes];
		this._nativeSharedLogState?.markEntriesKnownByPeer(hashArray, peer);
		this._nativeBackbone?.markEntriesKnownByPeer(hashArray, peer);
		const now = Date.now();
		// Growth is driven by writes, so the sweep rides the write path rather
		// than a timer or the rebalance pass: cost stays proportional to the
		// traffic that creates rows. Rate-limited to one pass per retention
		// window, over a map that after the first pass holds one window of marks.
		if (
			now - this._entryKnownPeerObservedAtSweptAt >=
			ENTRY_KNOWN_PEER_OBSERVED_AT_RETENTION_MS
		) {
			this.sweepEntryKnownPeerObservedAt(now);
		}
		for (const hash of hashArray) {
			let peers = this._entryKnownPeers.get(hash);
			if (!peers) {
				peers = new Set();
				this._entryKnownPeers.set(hash, peers);
			}
			peers.add(peer);

			let observedAt = this._entryKnownPeerObservedAt.get(hash);
			if (!observedAt) {
				observedAt = new Map();
				this._entryKnownPeerObservedAt.set(hash, observedAt);
			}
			observedAt.set(peer, now);
		}
	}

	private removeEntriesKnownByPeer(hashes: Iterable<string>, peer: string) {
		const hashArray = Array.isArray(hashes) ? hashes : [...hashes];
		this._nativeSharedLogState?.removeEntriesKnownByPeer(hashArray, peer);
		this._nativeBackbone?.removeEntriesKnownByPeer(hashArray, peer);
		for (const hash of hashArray) {
			const peers = this._entryKnownPeers.get(hash);
			if (peers) {
				peers.delete(peer);
				if (peers.size === 0) {
					this._entryKnownPeers.delete(hash);
				}
			}
			const observedAt = this._entryKnownPeerObservedAt.get(hash);
			if (observedAt) {
				observedAt.delete(peer);
				if (observedAt.size === 0) {
					this._entryKnownPeerObservedAt.delete(hash);
				}
			}
		}
	}

	private removePeerFromEntryKnownPeers(peer: string) {
		this._nativeSharedLogState?.removePeerFromEntryKnownPeers(peer);
		this._nativeBackbone?.removePeerFromEntryKnownPeers(peer);
		for (const [hash, peers] of this._entryKnownPeers) {
			peers.delete(peer);
			if (peers.size === 0) {
				this._entryKnownPeers.delete(hash);
			}
		}
		for (const [hash, observedAt] of this._entryKnownPeerObservedAt) {
			observedAt.delete(peer);
			if (observedAt.size === 0) {
				this._entryKnownPeerObservedAt.delete(hash);
			}
		}
	}

	private isEntryKnownByPeer(hash: string, peer: string) {
		return this._entryKnownPeers.get(hash)?.has(peer) === true;
	}

	private isEntryRecentlyKnownByPeer(
		hash: string,
		peer: string,
		maxAgeMs: number,
	) {
		const observedAt = this._entryKnownPeerObservedAt.get(hash)?.get(peer);
		return observedAt != null && Date.now() - observedAt <= maxAgeMs;
	}

	/** Drop recency marks no reader can still act on.
	 *
	 * Touches ONLY `_entryKnownPeerObservedAt`. `_entryKnownPeers` carries
	 * membership, not recency, and its rows stay until the peer dimension
	 * clears them; the native mirrors have no recency dimension at all
	 * (mark/remove/removePeer only), so this must not call into them or the
	 * two sides would disagree.
	 */
	private sweepEntryKnownPeerObservedAt(now: number) {
		for (const [hash, observedAt] of this._entryKnownPeerObservedAt) {
			for (const [peer, timestamp] of observedAt) {
				if (now - timestamp > ENTRY_KNOWN_PEER_OBSERVED_AT_RETENTION_MS) {
					observedAt.delete(peer);
				}
			}
			if (observedAt.size === 0) {
				this._entryKnownPeerObservedAt.delete(hash);
			}
		}
		this._entryKnownPeerObservedAtSweptAt = now;
	}

	private markRepairSweepOptimisticPeer(
		gid: string,
		peer: string,
		session: WarmupSession,
	) {
		let peers = this._repairSweepOptimisticGidPeersPending.get(gid);
		if (!peers) {
			peers = new Map();
			this._repairSweepOptimisticGidPeersPending.set(gid, peers);
		}
		const current = peers.get(peer);
		peers.set(peer, {
			count: current?.session === session ? current.count + 1 : 1,
			session,
		});
		let gids = this._repairSweepOptimisticGidsByPeer.get(peer);
		if (!gids) {
			gids = new Set();
			this._repairSweepOptimisticGidsByPeer.set(peer, gids);
		}
		gids.add(gid);
	}

	private hasPendingRepairSweepOptimisticPeer(gid: string, peer: string) {
		return (
			(this._repairSweepOptimisticGidPeersPending.get(gid)?.get(peer)?.count ||
				0) > 0
		);
	}

	private clearRepairSweepOptimisticPeer(peer: string) {
		for (const gid of this._repairSweepOptimisticGidsByPeer.get(peer) ?? []) {
			const peers = this._repairSweepOptimisticGidPeersPending.get(gid);
			if (!peers) {
				continue;
			}
			peers.delete(peer);
			if (peers.size === 0) {
				this._repairSweepOptimisticGidPeersPending.delete(gid);
			}
		}
		this._repairSweepOptimisticGidsByPeer.delete(peer);
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

	private usesBroadRepairCandidatePlanning(mode: RepairDispatchMode) {
		// Candidate planning may ignore stale gid-peer history, but final sends
		// still suppress hashes that the target has already confirmed.
		return mode === "join-authoritative" || mode === "churn";
	}

	private shouldBypassKnownPeerHints(
		mode: RepairDispatchMode,
		bypassKnownPeerHints?: boolean,
	) {
		return mode === "churn" || bypassKnownPeerHints === true;
	}

	private async sleepTracked(
		delayMs: number,
		repairLifecycleController: AbortController,
	) {
		if (delayMs <= 0) {
			return this.isRepairLifecycleActive(repairLifecycleController);
		}
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return false;
		}
		await new Promise<void>((resolve) => {
			let settled = false;
			const settle = () => {
				if (settled) {
					return;
				}
				settled = true;
				clearTimeout(timer);
				this._repairRetryTimers.delete(timer);
				repairLifecycleController.signal.removeEventListener("abort", settle);
				resolve();
			};
			const timer = setTimeout(settle, delayMs);
			timer.unref?.();
			this._repairRetryTimers.add(timer);
			repairLifecycleController.signal.addEventListener("abort", settle, {
				once: true,
			});
			if (repairLifecycleController.signal.aborted) {
				settle();
			}
		});
		return this.isRepairLifecycleActive(repairLifecycleController);
	}

	private queueRepairFrontierEntries(
		mode: RepairDispatchMode,
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		options?: { bypassKnownPeerHints?: boolean },
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	): boolean {
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return false;
		}
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
		if (options?.bypassKnownPeerHints === true) {
			this._repairFrontierBypassKnownPeersByMode.get(mode)?.add(target);
		}
		return true;
	}

	private clearRepairFrontierHashes(
		target: string,
		hashes: Iterable<string>,
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return;
		}
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
				this._repairFrontierBypassKnownPeersByMode.get(mode)?.delete(target);
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
				for (const subscriber of (await this._getTopicSubscribers(
					this.topic,
				)) ?? []) {
					candidates.add(subscriber.hashcode());
				}
			} catch {
				// Best-effort only; explicit repair peers still keep the path safe.
			}
		}
		return candidates;
	}

	private async getNativeFullReplicaDeliveryCandidates(
		minReplicas: number,
		selfHash: string,
	): Promise<Set<string>> {
		const source = (this._nativeBackbone ?? this._nativeSharedLogState) as
			| NativeFullReplicaCandidateSource
			| undefined;
		if (typeof source?.fullReplicaCandidatesFor === "function") {
			return new Set(source.fullReplicaCandidatesFor(minReplicas, selfHash));
		}
		return this.getFullReplicaRepairCandidates(undefined, {
			includeSubscribers: false,
		});
	}

	private removeRepairFrontierTarget(
		target: string,
		options?: { expectedWarmupSession?: WarmupSession | null },
	) {
		if (
			options?.expectedWarmupSession === undefined ||
			(options.expectedWarmupSession !== null &&
				this.joinWarmup._warmupSessionsByTarget.get(target) ===
					options.expectedWarmupSession)
		) {
			this.joinWarmup.cancelJoinWarmupTarget(target);
		}
		for (const mode of REPAIR_DISPATCH_MODES) {
			this._repairFrontierByMode.get(mode)?.delete(target);
			this._repairFrontierActiveTargetsByMode.get(mode)?.delete(target);
			this._repairFrontierBypassKnownPeersByMode.get(mode)?.delete(target);
		}
	}

	private async sendRepairConfirmation(
		target: PublicSignKey,
		hashes: Iterable<string>,
	) {
		const uniqueHashes = [...new Set(hashes)];
		for (
			let i = 0;
			i < uniqueHashes.length;
			i += REPAIR_CONFIRMATION_HASH_BATCH_SIZE
		) {
			const chunk = uniqueHashes.slice(
				i,
				i + REPAIR_CONFIRMATION_HASH_BATCH_SIZE,
			);
			await this.rpc.send(new ConfirmEntriesMessage({ hashes: chunk }), {
				priority: CONVERGENCE_MESSAGE_PRIORITY,
				mode: new SilentDelivery({ to: [target], redundancy: 1 }),
			});
		}
	}

	private async pushEntryHashChunk(
		target: string,
		chunk: string[],
		options: {
			acknowledge?: boolean;
			priority?: number;
			repairHint?: boolean;
			signal?: AbortSignal;
		},
		isStillCurrent: () => boolean,
	): Promise<boolean> {
		if (!isStillCurrent()) return false;
		const useRaw =
			this._logProperties?.sync?.rawExchangeHeads === true &&
			this.peerSupportsRawExchangeHeads(target);
		if (useRaw) {
			const reserved = options.repairHint ? new Uint8Array(4) : undefined;
			if (reserved) reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
			const sentMessages = await this.trySendFusedRawExchangeHeads(
				chunk,
				[target],
				{
					acknowledge: options.acknowledge,
					priority: options.priority,
					reserved,
					signal: options.signal,
				},
			);
			if (!isStillCurrent()) return false;
			if (sentMessages === undefined) {
				for await (const message of createRawExchangeHeadsMessages(
					this.log,
					chunk,
					this._logProperties?.sync?.profile,
				)) {
					if (!isStillCurrent()) return false;
					if (options.repairHint) {
						message.reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
					}
					await this.rpc.send(message, {
						priority: options.priority,
						mode: options.acknowledge
							? new AcknowledgeDelivery({ to: [target], redundancy: 1 })
							: new SilentDelivery({ to: [target], redundancy: 1 }),
						signal: options.signal,
					});
				}
			}
		} else {
			for await (const message of createExchangeHeadsMessages(
				this.log,
				chunk,
			)) {
				if (!isStillCurrent()) return false;
				if (options.repairHint) {
					message.reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
				}
				await this.rpc.send(message, {
					priority: options.priority,
					mode: options.acknowledge
						? new AcknowledgeDelivery({ to: [target], redundancy: 1 })
						: new SilentDelivery({ to: [target], redundancy: 1 }),
					signal: options.signal,
				});
			}
		}
		return isStillCurrent();
	}

	private async pushEntryHashes(
		target: string,
		hashes: string[],
		options: {
			acknowledge?: boolean;
			chunkTimeout?: () => number;
			chunkSize?: number;
			isStillCurrent?: () => boolean;
			onChunkAttempted?: (hashes: readonly string[]) => void;
			onChunkSent?: (hashes: readonly string[]) => Promise<boolean>;
			operationQueue?: PQueue;
			priority?: number;
			repairHint?: boolean;
			signal?: AbortSignal;
		},
	) {
		const isStillCurrent = options.isStillCurrent ?? (() => true);
		if (!isStillCurrent()) return;
		const chunkSize = Math.max(1, options.chunkSize ?? hashes.length);
		for (let offset = 0; offset < hashes.length; offset += chunkSize) {
			const chunk = hashes.slice(offset, offset + chunkSize);
			const pushChunk = () => {
				options.onChunkAttempted?.(chunk);
				const chunkTimeout = options.chunkTimeout?.();
				const chunkSignal =
					chunkTimeout == null
						? options.signal
						: AbortSignal.any([
								...(options.signal ? [options.signal] : []),
								AbortSignal.timeout(Math.max(1, chunkTimeout)),
							]);
				return this.pushEntryHashChunk(
					target,
					chunk,
					{ ...options, signal: chunkSignal },
					isStillCurrent,
				);
			};
			const pushed = options.operationQueue
				? await options.operationQueue.add(pushChunk)
				: await pushChunk();
			if (pushed !== true) return;
			if (options.onChunkSent && !(await options.onChunkSent(chunk))) return;
		}
	}

	private async pushRepairEntries(
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		isStillCurrent: () => boolean = () => true,
		signal?: AbortSignal,
	) {
		return this.pushEntryHashes(target, [...entries.keys()], {
			isStillCurrent,
			priority: SYNC_MESSAGE_PRIORITY,
			repairHint: true,
			signal,
		});
	}

	private async sendRepairEntriesWithTransport(
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		transport: RepairTransportMode,
		options?: {
			bypassKnownPeers?: boolean;
			bypassRecentKnownPeers?: boolean;
			isStillCurrent?: () => boolean;
			signal?: AbortSignal;
		},
	) {
		const isStillCurrent = options?.isStillCurrent ?? (() => true);
		if (!isStillCurrent()) {
			return;
		}
		const unknownEntries = new Map<string, RepairDispatchEntry<R>>();
		const knownHashes: string[] = [];
		for (const [hash, entry] of entries) {
			if (
				(options?.bypassRecentKnownPeers ||
					!this.isEntryRecentlyKnownByPeer(
						hash,
						target,
						RECENT_KNOWN_REPAIR_SUPPRESSION_MS,
					)) &&
				(options?.bypassKnownPeers || !this.isEntryKnownByPeer(hash, target))
			) {
				unknownEntries.set(hash, entry);
			} else {
				knownHashes.push(hash);
			}
		}
		if (!isStillCurrent()) {
			return;
		}
		this.clearRepairFrontierHashes(target, knownHashes);
		if (unknownEntries.size === 0) {
			return;
		}
		if (transport === "simple") {
			// Fallback repair should not depend on the target completing the
			// RequestMaybeSync -> ResponseMaybeSync round trip.
			await this.pushRepairEntries(
				target,
				unknownEntries,
				isStillCurrent,
				options?.signal,
			);
			return;
		}

		const syncEntries = this._logProperties?.sync?.priority
			? (this._coordinates.materializeRepairDispatchEntries(
					unknownEntries,
				) as unknown as Map<string, SyncEntryCoordinates<R>>)
			: (unknownEntries as Map<string, SyncEntryCoordinates<R>>);
		if (!isStillCurrent()) {
			return;
		}
		await this.syncronizer.onMaybeMissingEntries({
			entries: syncEntries,
			targets: [target],
			signal: options?.signal,
		});
	}

	private async sendMaybeMissingEntriesNow(
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		options: {
			mode: RepairDispatchMode;
			transport: RepairTransportMode;
			bypassRecentDedupe?: boolean;
			bypassKnownPeerHints?: boolean;
		},
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		if (
			entries.size === 0 ||
			!this.isRepairLifecycleActive(repairLifecycleController)
		) {
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
				: new Map<string, RepairDispatchEntry<any>>();
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
		const bypassKnownPeerHints = this.shouldBypassKnownPeerHints(
			options.mode,
			options.bypassKnownPeerHints,
		);
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return;
		}

		await Promise.resolve(
			this.sendRepairEntriesWithTransport(
				target,
				filteredEntries,
				options.transport,
				{
					bypassKnownPeers: bypassKnownPeerHints,
					bypassRecentKnownPeers: bypassKnownPeerHints,
					isStillCurrent: () =>
						this.isRepairLifecycleActive(repairLifecycleController),
					signal: repairLifecycleController.signal,
				},
			),
		).catch((error: any) => logger.error(error));
	}

	private ensureRepairFrontierRunner(
		mode: RepairDispatchMode,
		target: string,
		retryScheduleMs?: number[],
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		const activeTargets = this._repairFrontierActiveTargetsByMode.get(mode);
		if (
			!activeTargets ||
			activeTargets.has(target) ||
			!this.isRepairLifecycleActive(repairLifecycleController)
		) {
			return;
		}
		const runnerToken = {};
		activeTargets.set(target, runnerToken);
		const isCurrentRunner = () =>
			activeTargets.get(target) === runnerToken &&
			this.isRepairLifecycleActive(repairLifecycleController);
		const retrySchedule = resolveRepairRetrySchedule(
			mode,
			retryScheduleMs,
			this.isFrontierTrackedRepairMode(mode),
		);
		const steadyStateDelay =
			retrySchedule.length > 1
				? Math.max(
						1,
						retrySchedule[retrySchedule.length - 1] -
							retrySchedule[retrySchedule.length - 2],
					)
				: Math.max(retrySchedule[0] || 1_000, 1_000);

		void (async () => {
			let attemptIndex = 0;
			try {
				for (;;) {
					if (!isCurrentRunner()) {
						return;
					}
					const pending = this._repairFrontierByMode.get(mode)?.get(target);
					if (!pending || pending.size === 0) {
						if (!isCurrentRunner()) {
							return;
						}
						this._repairFrontierBypassKnownPeersByMode
							.get(mode)
							?.delete(target);
						return;
					}

					if (
						(mode === "join-warmup" || mode === "join-authoritative") &&
						this.isAssumeSyncedRepairSuppressed()
					) {
						if (
							!(await this.sleepTracked(
								Math.max(
									250,
									this._assumeSyncedRepairSuppressedUntil - Date.now(),
								),
								repairLifecycleController,
							))
						) {
							return;
						}
						continue;
					}

					if (!isCurrentRunner()) {
						return;
					}
					await this.sendMaybeMissingEntriesNow(
						target,
						pending,
						{
							mode,
							transport: getRepairTransportForAttempt(mode, attemptIndex),
							bypassRecentDedupe: true,
							bypassKnownPeerHints:
								this._repairFrontierBypassKnownPeersByMode
									.get(mode)
									?.has(target) === true,
						},
						repairLifecycleController,
					);
					if (!isCurrentRunner()) {
						return;
					}

					const remaining = this._repairFrontierByMode.get(mode)?.get(target);
					if (!remaining || remaining.size === 0) {
						return;
					}

					const waitMs =
						attemptIndex + 1 < retrySchedule.length
							? Math.max(
									0,
									retrySchedule[attemptIndex + 1] - retrySchedule[attemptIndex],
								)
							: steadyStateDelay;
					attemptIndex = Math.min(attemptIndex + 1, retrySchedule.length - 1);
					if (!(await this.sleepTracked(waitMs, repairLifecycleController))) {
						return;
					}
				}
			} finally {
				if (activeTargets.get(target) === runnerToken) {
					activeTargets.delete(target);
					if (
						this.isRepairLifecycleActive(repairLifecycleController) &&
						(this._repairFrontierByMode.get(mode)?.get(target)?.size || 0) > 0
					) {
						this.ensureRepairFrontierRunner(
							mode,
							target,
							retryScheduleMs,
							repairLifecycleController,
						);
					}
				}
			}
		})().catch((error: any) => {
			if (activeTargets.get(target) === runnerToken) {
				activeTargets.delete(target);
			}
			if (this.isRepairLifecycleActive(repairLifecycleController)) {
				logger.error(error);
			}
		});
	}

	private flushAppendBackfill(repairLifecycleController: AbortController) {
		if (
			!this.isRepairLifecycleActive(repairLifecycleController) ||
			this._appendBackfillPendingByTarget.size === 0
		) {
			return;
		}
		const pending = this._appendBackfillPendingByTarget;
		this._appendBackfillPendingByTarget = new Map();
		for (const [target, entries] of pending) {
			this.dispatchMaybeMissingEntries(
				target,
				entries,
				{
					mode: "append-backfill",
				},
				repairLifecycleController,
			);
		}
	}

	private queueAppendBackfill(
		target: string,
		entry: EntryReplicated<R>,
		repairLifecycleController: AbortController,
	) {
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return;
		}
		let entries = this._appendBackfillPendingByTarget.get(target);
		if (!entries) {
			entries = new Map();
			this._appendBackfillPendingByTarget.set(target, entries);
		}
		entries.set(entry.hash, entry);
		if (entries.size >= this.repairSweepTargetBufferSize) {
			this.flushAppendBackfill(repairLifecycleController);
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
			if (!this.isRepairLifecycleActive(repairLifecycleController)) {
				return;
			}
			this.flushAppendBackfill(repairLifecycleController);
		}, APPEND_BACKFILL_DELAY_MS);
		timer.unref?.();
		this._repairRetryTimers.add(timer);
		this._appendBackfillTimer = timer;
	}

	private queuePersistedAppendBackfill(
		source: PersistedAppendBackfillSource<T, R>,
		leaders: LeaderMap,
		replicas: number,
		repairLifecycleController: AbortController,
		ownershipRevision: number,
	): void {
		try {
			if (
				!this.isRepairLifecycleActive(repairLifecycleController) ||
				!this.isReceiveOwnershipSnapshotStable(ownershipRevision)
			) {
				return;
			}
			const assignmentLeaders = new Map(leaders);
			const deliveryTargets = new Set(leaders.keys());
			if (source.extrasOwnershipRevision === ownershipRevision) {
				for (const [peer, sample] of source.assignmentExtraLeaders) {
					assignmentLeaders.set(peer, sample);
				}
				for (const peer of source.deliveryExtraTargets) {
					deliveryTargets.add(peer);
				}
			}
			const repairEntry = this.createEntryReplicatedForRepair({
				entry: source.entry,
				coordinates: source.coordinates,
				leaders: assignmentLeaders,
				replicas,
			});
			const selfHash = this.node.identity.publicKey.hashcode();
			for (const peer of assignmentLeaders.keys()) {
				deliveryTargets.add(peer);
			}
			for (const peer of deliveryTargets) {
				if (peer === selfHash) continue;
				if (!this.isReceiveOwnershipSnapshotStable(ownershipRevision)) return;
				try {
					this.queueAppendBackfill(
						peer,
						repairEntry,
						repairLifecycleController,
					);
				} catch (error) {
					if (this.isRepairLifecycleActive(repairLifecycleController)) {
						logger.error(error);
					}
				}
			}
		} catch (error) {
			if (this.isRepairLifecycleActive(repairLifecycleController)) {
				logger.error(error);
			}
		}
	}

	private dispatchMaybeMissingEntries(
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		options: {
			mode: RepairDispatchMode;
			bypassRecentDedupe?: boolean;
			bypassKnownPeerHints?: boolean;
			retryScheduleMs?: number[];
		},
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		if (
			entries.size === 0 ||
			!this.isRepairLifecycleActive(repairLifecycleController)
		) {
			return;
		}

		if (this.isFrontierTrackedRepairMode(options.mode)) {
			if (
				!this.queueRepairFrontierEntries(
					options.mode,
					target,
					entries,
					{
						bypassKnownPeerHints: this.shouldBypassKnownPeerHints(
							options.mode,
							options.bypassKnownPeerHints,
						),
					},
					repairLifecycleController,
				)
			) {
				return;
			}
			this.ensureRepairFrontierRunner(
				options.mode,
				target,
				options.retryScheduleMs,
				repairLifecycleController,
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
				: new Map<string, RepairDispatchEntry<any>>();
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
		const warmupSession =
			options.mode === "join-warmup"
				? this.joinWarmup.ensureWarmupSession(target)
				: undefined;
		const bypassKnownPeerHints = this.shouldBypassKnownPeerHints(
			options.mode,
			options.bypassKnownPeerHints,
		);

		const run = (transport: RepairTransportMode) => {
			if (!this.isRepairLifecycleActive(repairLifecycleController)) {
				return;
			}
			if (
				transport === "simple" &&
				options.mode === "join-warmup" &&
				warmupSession
			) {
				this.joinWarmup.queueJoinWarmupSend(
					target,
					warmupSession,
					filteredEntries,
					bypassKnownPeerHints,
					repairLifecycleController,
				);
				return;
			}
			if (transport === "rateless") {
				bucket.ratelessFirstPasses += 1;
			} else {
				bucket.simpleFallbackPasses += 1;
			}
			return Promise.resolve(
				this.sendRepairEntriesWithTransport(
					target,
					filteredEntries,
					transport,
					{
						bypassKnownPeers: bypassKnownPeerHints,
						bypassRecentKnownPeers: bypassKnownPeerHints,
						isStillCurrent: () =>
							this.isRepairLifecycleActive(repairLifecycleController),
						signal: repairLifecycleController.signal,
					},
				),
			).catch((error: any) => logger.error(error));
		};

		const delayedJoinWarmupRetries: number[] = [];
		retrySchedule.forEach((delayMs, index) => {
			const transport = getRepairTransportForAttempt(options.mode, index);
			if (delayMs === 0) {
				void run(transport);
				return;
			}
			if (
				options.mode === "join-warmup" &&
				warmupSession &&
				transport === "simple"
			) {
				delayedJoinWarmupRetries.push(delayMs);
				return;
			}
			const timer = setTimeout(() => {
				if (
					repairLifecycleController ===
					(this._instanceLifecycle
						?.ownershipLifecycleController as AbortController)
				) {
					this._repairRetryTimers.delete(timer);
				}
				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				void run(transport);
			}, delayMs);
			timer.unref?.();
			this._repairRetryTimers.add(timer);
		});
		if (warmupSession && delayedJoinWarmupRetries.length > 0) {
			this.joinWarmup.scheduleJoinWarmupRetries(
				target,
				warmupSession,
				delayedJoinWarmupRetries,
				filteredEntries,
				bypassKnownPeerHints,
				repairLifecycleController,
			);
		}
	}

	private scheduleRepairSweep(
		options: {
			mode: RepairDispatchMode;
			peers?: Iterable<string>;
			warmupSessions?: ReadonlyMap<string, WarmupSession>;
		},
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return;
		}
		const pendingPeers = this._repairSweepPendingPeersByMode.get(options.mode);
		if (pendingPeers) {
			for (const peer of options.peers ?? []) {
				if (options.mode === "join-warmup") {
					const session =
						options.warmupSessions?.get(peer) ??
						this.joinWarmup.ensureWarmupSession(peer);
					if (this.joinWarmup._warmupSessionsByTarget.get(peer) !== session) {
						continue;
					}
					this.joinWarmup._repairSweepWarmupSessionByTarget.set(peer, session);
				}
				pendingPeers.add(peer);
			}
		}
		if (!pendingPeers || pendingPeers.size === 0) {
			return;
		}
		this._repairSweepPendingModes.add(options.mode);
		if (!this._repairSweepRunning && !this.closed) {
			this._repairSweepRunning = true;
			void this.runRepairSweep(repairLifecycleController);
		}
	}

	private scheduleJoinAuthoritativeRepair(
		peers: Set<string>,
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		if (
			!this.isRepairLifecycleActive(repairLifecycleController) ||
			peers.size === 0
		) {
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
				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				this._repairRetryTimers.delete(timer);
				this._joinAuthoritativeRepairTimersByDelay.delete(delayMs);

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

	private async runRepairSweep(
		repairLifecycleController: AbortController = this._instanceLifecycle
			?.ownershipLifecycleController as AbortController,
	) {
		try {
			while (this.isRepairLifecycleActive(repairLifecycleController)) {
				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				const pendingModes = new Set(this._repairSweepPendingModes);
				const pendingPeersByMode = cloneRepairPendingPeersByMode(
					this._repairSweepPendingPeersByMode,
				);
				const pendingWarmupSessions = new Map(
					this.joinWarmup._repairSweepWarmupSessionByTarget,
				);
				this._repairSweepPendingModes.clear();
				for (const peers of this._repairSweepPendingPeersByMode.values()) {
					peers.clear();
				}
				this.joinWarmup._repairSweepWarmupSessionByTarget.clear();
				const pendingJoinWarmupPeers = pendingPeersByMode.get("join-warmup");
				const pruneStaleJoinWarmupPeers = () => {
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return false;
					}
					for (const peer of [...(pendingJoinWarmupPeers ?? [])]) {
						if (
							this.joinWarmup._warmupSessionsByTarget.get(peer) !==
							pendingWarmupSessions.get(peer)
						) {
							pendingJoinWarmupPeers?.delete(peer);
						}
					}
					if (pendingJoinWarmupPeers?.size === 0) {
						pendingModes.delete("join-warmup");
					}
					return pendingModes.size > 0;
				};
				pruneStaleJoinWarmupPeers();

				if (pendingModes.size === 0) {
					return;
				}

				const optimisticGidPeersByMode = new Map<
					RepairDispatchMode,
					Map<string, Set<string>>
				>();
				const optimisticGidPeersConsumedByMode = new Map<
					RepairDispatchMode,
					Map<string, Map<string, RepairSweepOptimisticPeerState>>
				>();
				for (const mode of pendingModes) {
					const modePeers = pendingPeersByMode.get(mode);
					if (!modePeers || modePeers.size === 0) {
						continue;
					}
					const optimisticGidPeers = new Map<string, Set<string>>();
					const optimisticGidPeersConsumed = new Map<
						string,
						Map<string, RepairSweepOptimisticPeerState>
					>();
					for (const [gid, peerCounts] of this
						._repairSweepOptimisticGidPeersPending) {
						let matchedPeers: Set<string> | undefined;
						let matchedCounts:
							| Map<string, RepairSweepOptimisticPeerState>
							| undefined;
						for (const [peer, state] of peerCounts) {
							if (!modePeers.has(peer)) {
								continue;
							}
							matchedPeers ||= new Set();
							matchedCounts ||= new Map();
							matchedPeers.add(peer);
							matchedCounts.set(peer, { ...state });
						}
						if (matchedPeers && matchedCounts) {
							optimisticGidPeers.set(gid, matchedPeers);
							optimisticGidPeersConsumed.set(gid, matchedCounts);
						}
					}
					if (optimisticGidPeers.size > 0) {
						optimisticGidPeersByMode.set(mode, optimisticGidPeers);
						optimisticGidPeersConsumedByMode.set(
							mode,
							optimisticGidPeersConsumed,
						);
					}
				}

				const pendingByMode = new Map<
					RepairDispatchMode,
					Map<string, Map<string, RepairDispatchEntry<any>>>
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
				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				pruneStaleJoinWarmupPeers();
				const fullReplicaRepairCandidateCount = Math.max(
					1,
					fullReplicaRepairCandidates.size,
				);
				const nextFrontierByMode = new Map<
					RepairDispatchMode,
					Map<string, Map<string, RepairDispatchEntry<any>>>
				>([
					["join-authoritative", new Map()],
					["churn", new Map()],
				]);
				const flushTarget = (mode: RepairDispatchMode, target: string) => {
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return;
					}
					const targets = pendingByMode.get(mode);
					const entries = targets?.get(target);
					if (!entries || entries.size === 0) {
						return;
					}
					if (
						mode === "join-warmup" &&
						this.joinWarmup._warmupSessionsByTarget.get(target) !==
							pendingWarmupSessions.get(target)
					) {
						targets?.delete(target);
						pendingJoinWarmupPeers?.delete(target);
						if (pendingJoinWarmupPeers?.size === 0) {
							pendingModes.delete("join-warmup");
						}
						return;
					}
					this.dispatchMaybeMissingEntries(
						target,
						entries,
						{
							bypassRecentDedupe: true,
							bypassKnownPeerHints:
								mode === "churn" ||
								this._repairFrontierBypassKnownPeersByMode
									.get(mode)
									?.has(target) === true,
							mode,
						},
						repairLifecycleController,
					);
					targets?.delete(target);
				};
				const queueEntryForTarget = (
					mode: RepairDispatchMode,
					target: string,
					entry: RepairDispatchEntry<any>,
				) => {
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return;
					}
					if (
						mode === "join-warmup" &&
						this.joinWarmup._warmupSessionsByTarget.get(target) !==
							pendingWarmupSessions.get(target)
					) {
						pendingJoinWarmupPeers?.delete(target);
						if (pendingJoinWarmupPeers?.size === 0) {
							pendingModes.delete("join-warmup");
						}
						return;
					}
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

				const residentEntriesByHash =
					this._coordinates._residentEntryCoordinatesByHash;
				if (
					(this._nativeBackbone ?? this._nativeSharedLogState) &&
					residentEntriesByHash &&
					!this.hasCustomFindLeaders()
				) {
					const repairDispatchPlan = pruneStaleJoinWarmupPeers()
						? await this.planResidentRepairDispatchBatch(
								{
									pendingModes,
									pendingPeersByMode,
									optimisticGidPeersByMode,
									fullReplicaRepairCandidates,
									fullReplicaRepairCandidateCount,
									selfHash: this.node.identity.publicKey.hashcode(),
								},
								repairLifecycleController,
							)
						: new Map();
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return;
					}
					pruneStaleJoinWarmupPeers();
					for (const [mode, targets] of repairDispatchPlan) {
						for (const [target, hashes] of targets) {
							for (const hash of hashes) {
								const residentEntry = residentEntriesByHash.get(hash);
								if (residentEntry) {
									queueEntryForTarget(mode, target, residentEntry);
								}
							}
						}
					}
				} else if (pruneStaleJoinWarmupPeers()) {
					const iterator = this.entryCoordinatesIndex.iterate({});
					try {
						while (
							this.isRepairLifecycleActive(repairLifecycleController) &&
							!iterator.done() &&
							pruneStaleJoinWarmupPeers()
						) {
							const entries = await iterator.next(
								REPAIR_SWEEP_ENTRY_BATCH_SIZE,
							);
							if (!this.isRepairLifecycleActive(repairLifecycleController)) {
								return;
							}
							if (!pruneStaleJoinWarmupPeers()) {
								break;
							}
							const entryReplicatedBatch = entries.map((entry) => entry.value);
							const requestedReplicasBatch = entryReplicatedBatch.map((entry) =>
								decodeReplicas(entry).getValue(this),
							);
							const repairDispatchPlan = await this.planRepairDispatchBatch(
								{
									entries: entryReplicatedBatch,
									requestedReplicasBatch,
									pendingModes,
									pendingPeersByMode,
									optimisticGidPeersByMode,
									fullReplicaRepairCandidates,
									fullReplicaRepairCandidateCount,
									selfHash: this.node.identity.publicKey.hashcode(),
								},
								repairLifecycleController,
							);
							if (!this.isRepairLifecycleActive(repairLifecycleController)) {
								return;
							}
							if (!pruneStaleJoinWarmupPeers()) {
								break;
							}
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

				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				for (const [
					,
					optimisticGidPeersConsumed,
				] of optimisticGidPeersConsumedByMode) {
					for (const [gid, peerCounts] of optimisticGidPeersConsumed) {
						const pendingPeerCounts =
							this._repairSweepOptimisticGidPeersPending.get(gid);
						if (!pendingPeerCounts) {
							continue;
						}
						for (const [peer, consumed] of peerCounts) {
							const current = pendingPeerCounts.get(peer);
							if (!current || current.session !== consumed.session) {
								continue;
							}
							const next = current.count - consumed.count;
							if (next > 0) {
								pendingPeerCounts.set(peer, {
									count: next,
									session: current.session,
								});
							} else {
								pendingPeerCounts.delete(peer);
								const gids = this._repairSweepOptimisticGidsByPeer.get(peer);
								gids?.delete(gid);
								if (gids?.size === 0) {
									this._repairSweepOptimisticGidsByPeer.delete(peer);
								}
							}
						}
						if (pendingPeerCounts.size === 0) {
							this._repairSweepOptimisticGidPeersPending.delete(gid);
						}
					}
				}

				for (const mode of pendingModes) {
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return;
					}
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
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return;
					}
					for (const target of [...targets.keys()]) {
						flushTarget(mode, target);
					}
				}
			}
		} catch (error: any) {
			if (
				this.isRepairLifecycleActive(repairLifecycleController) &&
				!isNotStartedError(error)
			) {
				logger.error(`Repair sweep failed: ${error?.message ?? error}`);
			}
		} finally {
			if (
				repairLifecycleController ===
				(this._instanceLifecycle
					?.ownershipLifecycleController as AbortController)
			) {
				this._repairSweepRunning = false;
				if (
					this.isRepairLifecycleActive(repairLifecycleController) &&
					this._repairSweepPendingModes.size > 0
				) {
					this._repairSweepRunning = true;
					void this.runRepairSweep(repairLifecycleController);
				}
			}
		}
	}

	private async pruneDebouncedFnAddIfNotKeeping(
		args: {
			key: string;
			value: {
				entry: CheckedPruneEntry<T, R>;
				leaders: CheckedPruneLeaderMap;
				workToken?: object;
			};
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
		additionalCurrentCheck?: () => boolean,
	): Promise<boolean> {
		if (this.closed || !this.pruneDebouncedFn) {
			return false;
		}
		const checkedPruneCoordinator = this._checkedPrune;
		const pruneDebouncedFn = this.pruneDebouncedFn;
		const lifecycle = this._instanceLifecycle!;
		const isCurrent = () =>
			// closeController omitted: this seat never compared it.
			lifecycle.isCheckedPruneCurrent(
				checkedPruneCoordinator,
				undefined,
				ownershipLifecycleController,
			) &&
			lifecycle.isPruneDebouncerCurrent(pruneDebouncedFn) &&
			(additionalCurrentCheck?.() ?? true);
		if (!isCurrent()) {
			return false;
		}
		if (this.keep) {
			const keepResult = this.keep(args.value.entry);
			if (isPromiseLike(keepResult) ? await keepResult : keepResult) {
				return false;
			}
			if (!isCurrent()) {
				return false;
			}
		}
		const workToken = checkedPruneCoordinator.trackCandidate(
			args.key,
			args.value.entry,
			args.value.leaders,
		);
		args.value.workToken = workToken;
		void pruneDebouncedFn.add(args).catch((error) => {
			if (
				checkedPruneCoordinator.isCandidateTokenCurrent(args.key, workToken)
			) {
				checkedPruneCoordinator.invalidateCandidateToken(args.key);
			}
			if (isCurrent() && !isNotStartedError(error as Error)) {
				logger.error(error);
				try {
					this.scheduleCheckedPruneRetry(
						args.value,
						ownershipLifecycleController,
					);
				} catch {
					checkedPruneCoordinator.clearRetry(args.key);
				}
			}
		});
		return true;
	}

	private deleteQueuedCheckedPrune(
		hash: string,
		checkedPruneCoordinator = this._checkedPrune,
	) {
		this.pruneDebouncedFn.delete(hash);
		checkedPruneCoordinator.invalidateCandidateToken(hash);
	}

	private async cancelCheckedPruneForLocalLeader(
		hash: string,
		options?: { preserveRetry?: boolean },
	) {
		this.deleteQueuedCheckedPrune(hash);
		const pendingDelete = this._checkedPrune.getPendingDelete(hash);
		if (pendingDelete) {
			this._checkedPrune.markCancelled(hash, pendingDelete, {
				preserveRetry: options?.preserveRetry,
			});
		} else {
			this._checkedPrune.markCancelled(hash, {
				preserveRetry: options?.preserveRetry,
			});
		}
		await pendingDelete?.reject(new Error("Failed to delete, is leader again"));
	}

	private rearmCheckedPruneAfterTemporaryReceive(hash: string) {
		const candidate = this._checkedPrune.getRestartCandidate(hash);
		void this.cancelCheckedPruneForLocalLeader(hash, {
			preserveRetry: true,
		}).catch(() => {});
		if (!candidate) {
			// New receive work is reconsidered only after the lower-log join proves
			// the hash was admitted. pruneJoinedEntriesNoLongerLed owns that path.
			return;
		}
		try {
			this.scheduleCheckedPruneRetry(candidate);
		} catch {
			this._checkedPrune.clearRetry(hash);
		}
	}

	private async revalidateCheckedPruneGrantLocalLeaders(
		hashes: string[],
		prunePeer: string,
		ownershipLifecycleController: AbortController,
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const localLeaderHashes = new Set<string>();
		const unresolvedHashes = new Set(hashes);
		const nativePlanner = this._nativeBackbone ?? this._nativeRangePlanner;
		const nativeEntryMetadata =
			this._coordinates.getNativeLogEntryMetadataBatch(hashes);

		if (nativePlanner && !this.hasCustomFindLeaders() && nativeEntryMetadata) {
			const nativeItems: Array<{
				hash: string;
				gid: string;
				replicas: number;
			}> = [];
			for (let index = 0; index < hashes.length; index++) {
				const entry = nativeEntryMetadata[index];
				if (!entry) {
					continue;
				}
				this.removePeerFromGidPeerHistory(prunePeer, entry.gid);
				nativeItems.push({
					hash: hashes[index]!,
					gid: entry.gid,
					replicas:
						entry.replicas ??
						decodeReplicas({
							meta: { data: entry.data },
						}).getValue(this),
				});
			}
			if (nativeItems.length > 0) {
				const context = await this.createLeaderSelectionContext(
					undefined,
					ownershipLifecycleController,
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				const plans = nativePlanner.planLeadersForGidsBatch(
					nativeItems.map(({ gid, replicas }) => ({ gid, replicas })),
					this.createNativeLeaderOptions(context),
				);
				for (let index = 0; index < nativeItems.length; index++) {
					const item = nativeItems[index]!;
					unresolvedHashes.delete(item.hash);
					if (plans[index]?.leaders.has(context.selfHash)) {
						localLeaderHashes.add(item.hash);
					}
				}
			}
		}

		for (const hash of unresolvedHashes) {
			const indexedEntry = await this.log.entryIndex.getShallow(hash);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (!indexedEntry) {
				continue;
			}
			this.removePeerFromGidPeerHistory(prunePeer, indexedEntry.value.meta.gid);
			const plan = await this.planEntryLeaders(
				indexedEntry.value,
				decodeReplicas(indexedEntry.value).getValue(this),
				{ persist: false },
				ownershipLifecycleController,
			);
			if (plan.isLeader) {
				localLeaderHashes.add(hash);
			}
		}
		return localLeaderHashes;
	}

	private async revalidateCheckedPruneOwnership(args: {
		hash: string;
		entry: CheckedPruneEntry<T, R>;
		leaders: CheckedPruneLeaderMap;
		selfReplicating?: boolean;
		requireFreshLeaderDecision?: boolean;
		ownershipLifecycleController?: AbortController;
		checkedPruneCoordinator?: CheckedPruneCoordinator<T, R>;
	}): Promise<{
		leaders: CheckedPruneLeaderMap;
		localLeader: boolean;
	}> {
		const checkedPruneCoordinator =
			args.checkedPruneCoordinator ?? this._checkedPrune;
		const throwIfInactive = () => {
			if (args.ownershipLifecycleController) {
				// Stage 3 (seam 7): lifecycle.throwIfInactive + an EXPLICIT
				// coordinator compare. Deliberately NOT isCheckedPruneCurrent /
				// throwIfCheckedPruneInactive with a closeController term: the
				// legacy predicate here never compared _closeController, and
				// folding it in would strengthen this fence. The lifecycle is
				// re-fetched per invocation, matching the legacy re-read of
				// this._checkedPrune against the older captured coordinator
				// (mixed-window semantics are intentional).
				this._instanceLifecycle!.throwIfInactive(
					args.ownershipLifecycleController,
				);
				if (this._checkedPrune !== checkedPruneCoordinator) {
					throw new TerminalOperationNotStartedError(
						"Checked prune lifecycle is no longer active",
					);
				}
				return;
			}
			this.throwIfReplicationOwnershipPoisoned();
		};
		throwIfInactive();
		const selfHash = this.node.identity.publicKey.hashcode();
		if (!args.requireFreshLeaderDecision && args.leaders.has(selfHash)) {
			if (args.selfReplicating === false) {
				return { leaders: args.leaders, localLeader: false };
			}
			if (args.selfReplicating == null) {
				throwIfInactive();
				const selfReplicating = await this.isReplicating();
				throwIfInactive();
				if (!selfReplicating) {
					return { leaders: args.leaders, localLeader: false };
				}
			}
			throwIfInactive();
			return { leaders: args.leaders, localLeader: true };
		}

		throwIfInactive();
		if (!checkedPruneCoordinator.hasActiveWork(args.hash)) {
			if (args.requireFreshLeaderDecision) {
				throw new TerminalOperationNotStartedError(
					"Checked prune work is no longer active at the delete boundary",
				);
			}
			return { leaders: args.leaders, localLeader: false };
		}

		if (args.selfReplicating === false) {
			return { leaders: args.leaders, localLeader: false };
		}
		if (args.selfReplicating == null) {
			throwIfInactive();
			const selfReplicating = await this.isReplicating();
			throwIfInactive();
			if (!selfReplicating) {
				return { leaders: args.leaders, localLeader: false };
			}
		}

		try {
			throwIfInactive();
			// Ownership decisions here feed prune/delete outcomes; never serve
			// them from the leader-plan cache.
			const currentLeaders = await this.findLeadersFromEntry(
				args.entry,
				decodeReplicas(args.entry).getValue(this),
				{ freshLeaderPlan: true },
			);
			throwIfInactive();
			if (currentLeaders.size > 0) {
				return {
					leaders: currentLeaders,
					localLeader: currentLeaders.has(selfHash),
				};
			}
			if (args.requireFreshLeaderDecision) {
				throw new Error(
					"Could not establish current leaders at the checked-prune delete boundary",
				);
			}
		} catch (error) {
			throwIfInactive();
			if (args.requireFreshLeaderDecision) {
				throw error;
			}
			// Best-effort only. If the fresh check fails, keep the original prune
			// decision instead of hiding a legitimately prunable entry.
		}

		throwIfInactive();
		return { leaders: args.leaders, localLeader: false };
	}

	private async prunePromotedCheckedPruneParents(
		parentHashes: Iterable<string>,
		ownershipLifecycleController: AbortController,
	) {
		const parents: ShallowOrFullEntry<T>[] = [];
		for (const hash of new Set(parentHashes)) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const indexed = await this.log.entryIndex.getShallow(hash);
			if (indexed?.value.head === true) {
				parents.push(indexed.value);
			}
		}
		await this.pruneJoinedEntriesNoLongerLed(
			parents,
			undefined,
			ownershipLifecycleController,
		);
	}

	private async pruneJoinedEntriesNoLongerLed(
		entries: ShallowOrFullEntry<T>[],
		options?: {
			decodedReplicaCounts?: DecodedReplicaCountMap;
			freshReceiveOwnerAudit?: boolean;
			preserveExistingPruneOnLocalResult?: boolean;
			reusableLeaderPlans?: ReadonlyMap<
				string,
				Pick<ReusableReceiveCoordinatePlan<R>, "plan" | "replicas">
			>;
			profile?: SyncProfileFn;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		if (
			entries.length === 0 ||
			!this.isRepairLifecycleActive(ownershipLifecycleController)
		) {
			return;
		}
		const selfHash = this.node.identity.publicKey.hashcode();
		const freshReceiveRoleAge = options?.freshReceiveOwnerAudit
			? await this.getDefaultMinRoleAge()
			: undefined;
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const plans = new Array<EntryLeaderPlan<R> | undefined>(entries.length);
		const leaderItems: Array<{
			entry: ShallowOrFullEntry<T>;
			replicas: number;
			options: LeaderSelectionOptions<R>;
		}> = [];
		const leaderItemIndexes: number[] = [];
		let reusableLeaderPlanHits = 0;
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			const replicas =
				options?.decodedReplicaCounts?.get(entry.hash) ??
				decodeReplicas(entry).getValue(this);
			const reusablePlan = options?.freshReceiveOwnerAudit
				? undefined
				: options?.reusableLeaderPlans?.get(entry.hash);
			if (reusablePlan && reusablePlan.replicas === replicas) {
				plans[i] = reusablePlan.plan;
				reusableLeaderPlanHits++;
				continue;
			}
			leaderItems.push({
				entry,
				replicas,
				options: options?.freshReceiveOwnerAudit
					? { roleAge: freshReceiveRoleAge!, persist: false }
					: { roleAge: 0, persist: false },
			});
			leaderItemIndexes.push(i);
		}
		const nativeBatch = this.canPlanNativeEntryLeaderBatch(leaderItems);
		const planStartedAt = syncProfileStart(options?.profile);
		let leaderMapsOnly = false;
		let nativeLeaderMaps:
			| Array<Map<string, { intersecting: boolean }>>
			| undefined;
		const nativeBackboneLeaderMaps = this._nativeBackbone as
			| (NativePeerbitBackbone & {
					planLeaderSamplesForGidsBatch?: (
						items: Iterable<{ gid: string; replicas: number }>,
						options?: unknown,
					) => Array<Map<string, { intersecting: boolean }>> | undefined;
			  })
			| undefined;
		if (
			nativeBatch &&
			nativeBackboneLeaderMaps?.planLeaderSamplesForGidsBatch
		) {
			const firstOptions = leaderItems[0]?.options;
			const context = await this.createLeaderSelectionContext(
				{
					roleAge: firstOptions?.roleAge,
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			nativeLeaderMaps = nativeBackboneLeaderMaps.planLeaderSamplesForGidsBatch(
				leaderItems.map((item) => ({
					gid: this.getEntryGid(item.entry),
					replicas: item.replicas,
				})),
				this.createNativeLeaderOptions(context),
			);
		}
		if (nativeLeaderMaps && nativeLeaderMaps.length === leaderItems.length) {
			leaderMapsOnly = true;
			for (let i = 0; i < nativeLeaderMaps.length; i++) {
				const leaders = nativeLeaderMaps[i]!;
				plans[leaderItemIndexes[i]!] = {
					coordinates: [],
					leaders,
					isLeader: leaders.has(selfHash),
				};
			}
		} else if (leaderItems.length > 0) {
			const missingPlans = await this.planEntryLeaderBatch(
				leaderItems,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			for (let i = 0; i < missingPlans.length; i++) {
				plans[leaderItemIndexes[i]!] = missingPlans[i];
			}
		}
		emitSyncProfileDuration(options?.profile, planStartedAt, {
			name: "sharedLog.receive.checkedPrune.plan",
			component: "shared-log",
			entries: entries.length,
			count: leaderItems.length,
			messages: 1,
			details: { nativeBatch, leaderMapsOnly, reusableLeaderPlanHits },
		});

		const loopStartedAt = syncProfileStart(options?.profile);
		let enqueuedPrune = 0;
		let cancelledLocalLeader = 0;
		let localLeaderResults = 0;
		for (let i = 0; i < entries.length; i++) {
			if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
				continue;
			}
			const entry = entries[i]!;
			const leaders = plans[i]?.leaders ?? new Map();

			if (leaders.has(selfHash)) {
				if (!options?.preserveExistingPruneOnLocalResult) {
					await this.cancelCheckedPruneForLocalLeader(entry.hash);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					cancelledLocalLeader++;
				}
				localLeaderResults++;
				continue;
			}

			if (this._checkedPrune.hasPendingDelete(entry.hash)) {
				continue;
			}

			if (leaders.size === 0) {
				this.scheduleCheckedPruneRetry(
					{ entry, leaders },
					ownershipLifecycleController,
				);
				continue;
			}

			await this.pruneDebouncedFnAddIfNotKeeping(
				{
					key: entry.hash,
					value: { entry, leaders },
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			enqueuedPrune++;
		}
		emitSyncProfileDuration(options?.profile, loopStartedAt, {
			name: "sharedLog.receive.checkedPrune.loop",
			component: "shared-log",
			entries: entries.length,
			count: enqueuedPrune,
			messages: 1,
			details: { cancelledLocalLeader, localLeaderResults },
		});
	}

	private async pruneIndexedEntriesNoLongerLed(
		options?: {
			useDefaultRoleAge?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const selfHash = this.node.identity.publicKey.hashcode();
		const iterator = this.entryCoordinatesIndex.iterate({});
		let enqueuedPrune = false;
		try {
			while (
				this.isRepairLifecycleActive(ownershipLifecycleController) &&
				!iterator.done()
			) {
				const entries = await iterator.next(REPAIR_SWEEP_ENTRY_BATCH_SIZE);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				for (const entry of entries) {
					const entryReplicated = entry.value;
					if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
						continue;
					}

					const leaders = await this.findLeaders(
						entryReplicated.coordinates,
						entryReplicated,
						options?.useDefaultRoleAge ? undefined : { roleAge: 0 },
						ownershipLifecycleController,
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);

					if (leaders.has(selfHash)) {
						await this.cancelCheckedPruneForLocalLeader(entryReplicated.hash);
						this.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
						continue;
					}

					if (this._checkedPrune.hasPendingDelete(entryReplicated.hash)) {
						continue;
					}

					if (leaders.size === 0) {
						this.scheduleCheckedPruneRetry(
							{ entry: entryReplicated, leaders },
							ownershipLifecycleController,
						);
						continue;
					}

					enqueuedPrune =
						(await this.pruneDebouncedFnAddIfNotKeeping(
							{
								key: entryReplicated.hash,
								value: { entry: entryReplicated, leaders },
							},
							ownershipLifecycleController,
						)) || enqueuedPrune;
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			}
		} finally {
			await iterator.close();
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (enqueuedPrune) {
			await this.pruneDebouncedFn.flush();
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
	}

	private async pruneCurrentHeadsNoLongerLed(
		options?: {
			useDefaultRoleAge?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const selfHash = this.node.identity.publicKey.hashcode();
		const nativeHeads = this.log.entryIndex.getHeadsForAppend();
		const heads: ShallowOrFullEntry<T>[] = nativeHeads
			? await this.pruneHeadEntriesFromNativeHeadFacts(
					nativeHeads,
					ownershipLifecycleController,
				)
			: await this.log.getHeads(true).all();
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		let enqueuedPrune = false;

		for (const head of heads) {
			if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
				break;
			}

			const leaders = await this.findLeadersFromEntry(
				head,
				maxReplicas(this, [head]),
				options?.useDefaultRoleAge ? undefined : { roleAge: 0 },
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);

			if (leaders.has(selfHash)) {
				await this.cancelCheckedPruneForLocalLeader(head.hash);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				continue;
			}

			if (this._checkedPrune.hasPendingDelete(head.hash)) {
				continue;
			}

			if (leaders.size === 0) {
				this.scheduleCheckedPruneRetry(
					{ entry: head, leaders },
					ownershipLifecycleController,
				);
				continue;
			}

			enqueuedPrune =
				(await this.pruneDebouncedFnAddIfNotKeeping(
					{
						key: head.hash,
						value: { entry: head, leaders },
					},
					ownershipLifecycleController,
				)) || enqueuedPrune;
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}

		if (enqueuedPrune) {
			await this.pruneDebouncedFn.flush();
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
	}

	private async pruneHeadEntriesFromNativeHeadFacts(
		heads: Array<{
			hash: string;
			meta: { gid: string; clock: { timestamp: Timestamp } };
		}>,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<ShallowEntry[]> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (heads.length === 0) {
			return [];
		}
		const headDataRows = (await this.log.entryIndex
			.getHeads(undefined, {
				type: "shape",
				shape: { hash: true, meta: { data: true } },
			})
			.all()) as Array<{ hash: string; meta: { data?: Uint8Array } }>;
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const dataByHash = new Map(
			headDataRows
				.filter((entry) => entry.meta.data)
				.map((entry) => [entry.hash, entry.meta.data!]),
		);
		const prunableHeads: ShallowEntry[] = [];
		for (const head of heads) {
			const data = dataByHash.get(head.hash);
			if (!data) {
				continue;
			}
			prunableHeads.push(
				new ShallowEntry({
					hash: head.hash,
					head: true,
					payloadSize: 0,
					meta: new ShallowMeta({
						gid: head.meta.gid,
						clock: new LamportClock({
							id: this.node.identity.publicKey.bytes,
							timestamp: new Timestamp({
								wallTime: head.meta.clock.timestamp.wallTime,
								logical: head.meta.clock.timestamp.logical,
							}),
						}),
						data,
						next: [],
						type: EntryType.APPEND,
					}),
				}),
			);
		}
		return prunableHeads;
	}

	private checkedPruneLeadersToMap(
		leaders: CheckedPruneLeaderMap | Set<string>,
	): CheckedPruneLeaderMap {
		if (leaders instanceof Map) {
			return new Map(leaders);
		}
		const leadersMap: CheckedPruneLeaderMap = new Map();
		for (const leader of leaders) {
			leadersMap.set(leader, { intersecting: true });
		}
		return leadersMap;
	}

	private clearCheckedPruneAuditTimer() {
		if (this._checkedPruneAuditTimer) {
			clearTimeout(this._checkedPruneAuditTimer);
			this._checkedPruneAuditTimer = undefined;
		}
	}

	private scheduleCheckedPruneAudit(
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		const checkedPruneCoordinator = this._checkedPrune;
		const lifecycle = this._instanceLifecycle!;
		const isCurrent = () =>
			// closeController omitted: this seat never compared it.
			lifecycle.isCheckedPruneCurrent(
				checkedPruneCoordinator,
				undefined,
				ownershipLifecycleController,
			);
		if (!isCurrent() || this._checkedPruneAuditTimer) {
			return;
		}

		const timer = setTimeout(() => {
			if (this._checkedPruneAuditTimer !== timer) {
				return;
			}
			this._checkedPruneAuditTimer = undefined;
			if (!isCurrent()) {
				return;
			}

			const eligible = [...checkedPruneCoordinator.retries].filter(
				([hash, state]) =>
					!state.timer &&
					state.attempts >= CHECKED_PRUNE_RETRY_MAX_ATTEMPTS &&
					!checkedPruneCoordinator.hasPendingDelete(hash) &&
					!checkedPruneCoordinator.hasCandidate(hash) &&
					!checkedPruneCoordinator.hasRestartReservation(hash),
			);
			const batch = eligible.slice(0, CHECKED_PRUNE_AUDIT_BATCH_SIZE);
			for (const [hash, state] of batch) {
				// Rotate serviced hashes behind the unserviced cohort. Otherwise an
				// unreachable first batch could monopolize every audit interval.
				checkedPruneCoordinator.retries.delete(hash);

				// The fast budget is intentionally not reset by remote traffic.
				// A low-rate, coalesced audit grants one final-budget attempt at a
				// time so a quiet topology can still converge without one timer per
				// retained hash.
				state.attempts = CHECKED_PRUNE_RETRY_MAX_ATTEMPTS - 1;
				checkedPruneCoordinator.setRetry(hash, state);
				this.scheduleCheckedPruneRetry(
					{ entry: state.entry, leaders: state.leaders },
					ownershipLifecycleController,
				);
			}

			if (eligible.length > batch.length) {
				this.scheduleCheckedPruneAudit(ownershipLifecycleController);
			}
		}, CHECKED_PRUNE_AUDIT_INTERVAL_MS);
		this._checkedPruneAuditTimer = timer;
		timer.unref?.();
	}

	private scheduleCheckedPruneRetry(
		args: {
			entry: CheckedPruneEntry<T, R>;
			leaders: CheckedPruneLeaderMap | Set<string>;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		const checkedPruneCoordinator = this._checkedPrune;
		const lifecycle = this._instanceLifecycle!;
		const isCurrent = () =>
			// closeController omitted: this seat never compared it.
			lifecycle.isCheckedPruneCurrent(
				checkedPruneCoordinator,
				undefined,
				ownershipLifecycleController,
			);
		if (!isCurrent()) return;
		if (checkedPruneCoordinator.hasPendingDelete(args.entry.hash)) return;

		const hash = args.entry.hash;
		const state =
			checkedPruneCoordinator.getRetry(hash) ??
			({
				attempts: 0,
				generation: 0,
				entry: args.entry,
				leaders: args.leaders,
			} satisfies CheckedPruneRetryState<T, R>);
		state.generation++;
		state.entry = args.entry;
		state.leaders = args.leaders;

		if (state.timer) return;
		if (state.attempts >= CHECKED_PRUNE_RETRY_MAX_ATTEMPTS) {
			// Fast per-hash retries are bounded. Exhausted obligations move to one
			// coalesced low-rate audit instead of being forgotten while the topology
			// is otherwise quiet.
			checkedPruneCoordinator.setRetry(hash, state);
			this.scheduleCheckedPruneAudit(ownershipLifecycleController);
			return;
		}

		const attempt = state.attempts + 1;
		const jitterMs = Math.floor(Math.random() * 250);
		const delayMs = Math.min(
			CHECKED_PRUNE_RETRY_MAX_DELAY_MS,
			1_000 * 2 ** (attempt - 1) + jitterMs,
		);

		state.attempts = attempt;
		const timer = setTimeout(() => {
			const generation = state.generation;
			const run = async () => {
				const st = checkedPruneCoordinator.getRetry(hash);
				if (st !== state || state.timer !== timer || !isCurrent()) {
					return;
				}
				state.timer = undefined;
				const isExactAttempt = () =>
					isCurrent() &&
					checkedPruneCoordinator.getRetry(hash) === state &&
					state.generation === generation &&
					state.timer === undefined;
				if (checkedPruneCoordinator.hasPendingDelete(hash)) return;
				const retryEntry = state.entry;
				const retryLeaders = state.leaders;

				let leadersMap: CheckedPruneLeaderMap | undefined;
				try {
					const replicas = decodeReplicas(retryEntry).getValue(this);
					leadersMap = await this.findLeadersFromEntry(
						retryEntry,
						replicas,
						{ roleAge: 0 },
						ownershipLifecycleController,
					);
				} catch {
					if (!isExactAttempt()) {
						return;
					}
					// A current-generation planning failure is best-effort; fall back
					// to the last confirmed leader set below.
				}
				if (!isExactAttempt()) return;

				if (!leadersMap || leadersMap.size === 0) {
					leadersMap = this.checkedPruneLeadersToMap(retryLeaders);
				}
				if (leadersMap.size === 0) {
					this.scheduleCheckedPruneRetry(
						{ entry: retryEntry, leaders: retryLeaders },
						ownershipLifecycleController,
					);
					return;
				}

				const leadersForRetry = leadersMap;
				const enqueued = await this.pruneDebouncedFnAddIfNotKeeping(
					{
						key: hash,
						value: { entry: retryEntry, leaders: leadersForRetry },
					},
					ownershipLifecycleController,
					() =>
						isExactAttempt() && !checkedPruneCoordinator.hasPendingDelete(hash),
				);
				if (
					!enqueued &&
					isExactAttempt() &&
					!checkedPruneCoordinator.hasPendingDelete(hash)
				) {
					// A keep policy is terminal for this background prune. Do not
					// retain a retry record with neither a timer nor pending work.
					checkedPruneCoordinator.clearRetry(hash, {
						state,
						generation,
					});
				}
			};
			void run().catch((error) => {
				if (isCurrent() && !isNotStartedError(error as Error)) {
					logger.error(error);
					if (
						checkedPruneCoordinator.getRetry(hash) === state &&
						state.generation === generation &&
						!state.timer &&
						!checkedPruneCoordinator.hasPendingDelete(hash)
					) {
						try {
							this.scheduleCheckedPruneRetry(
								{ entry: state.entry, leaders: state.leaders },
								ownershipLifecycleController,
							);
						} catch {
							checkedPruneCoordinator.clearRetry(hash, {
								state,
								generation,
							});
						}
					}
				}
			});
		}, delayMs);
		state.timer = timer;
		timer.unref?.();
		checkedPruneCoordinator.setRetry(hash, state);
	}

	async append(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		this.throwIfNativeDurableCommitFailed();
		const persistedInvocation = this.capturePersistedAppendInvocation(options);
		options = persistedInvocation?.options ?? options;
		const persistedDelivery = persistedInvocation?.delivery;
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } = this.createLogAppendOptions(
			options,
			ownershipLifecycleController,
		);
		let committedHashes: readonly string[] | undefined;
		let persistedAppendCommit: PreparedLocalAppendCommit<R> | undefined;
		let persistedPlanningRecord:
			| PersistedDeliveryPlanningRecord<T, R>
			| undefined;
		let persistedBackfillSource:
			| PersistedAppendBackfillSource<T, R>
			| undefined;
		let persistedBackfillLeaders: LeaderMap | undefined;
		let persistedBackfillOwnershipRevision: number | undefined;
		let localAppendProcessed = false;
		if (persistedDelivery) {
			(appendOptions as TrustedLogAppendOptions<T>).__peerbitOnLocalCommit = (
				hashes,
				entries,
			) => {
				// The lower log reports the exact hashes immediately after their
				// irreversible local mutation and before trim/change callbacks. Own the
				// canonical hash first, then detach all planning facts and storage bytes
				// synchronously so callback mutation cannot retarget the later quorum.
				committedHashes = Object.freeze([...hashes]);
				if (hashes.length !== 1 || entries?.length !== 1) {
					throw new Error(
						"Persisted delivery requires exact lower-log entry commit evidence",
					);
				}
				persistedAppendCommit = this.capturePersistedLocalAppendCommit(
					hashes[0]!,
					entries[0]!,
				);
				persistedPlanningRecord = this.createPersistedDeliveryPlanningRecord(
					persistedAppendCommit,
				);
			};
		}
		let persistedDeadline: PersistedDeliveryDeadline | undefined;
		const throwIfDeliveryAborted = () => {
			if (persistedDeadline?.signal.aborted) {
				throw persistedDeadline.signal.reason ?? new AbortError();
			}
		};
		try {
			const result = await this.log.append(data, appendOptions);
			if (persistedDelivery && (!committedHashes || !persistedPlanningRecord)) {
				throw new Error(
					"Lower log did not provide persisted-delivery commit evidence",
				);
			}
			persistedDeadline = persistedDelivery
				? this.createPersistedDeliveryDeadline(
						persistedDelivery,
						ownershipLifecycleController,
						1,
					)
				: undefined;
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			throwIfDeliveryAborted();
			const processingEntry =
				persistedPlanningRecord?.createFullPlanningSource?.() ?? result.entry;
			await this.processLocalAppend(processingEntry, result.removed, options, {
				minReplicasValue,
				appendFacts: persistedAppendCommit,
				// Persisted settlement must confirm each exact receiver generation before
				// using its transfer as receipt evidence. Keep the optimistic append path
				// out of that ordering decision.
				captureDeferredBackfillSource: persistedDelivery
					? (source) => {
							persistedBackfillSource = source;
						}
					: undefined,
				ownershipLifecycleController,
			});
			localAppendProcessed = true;
			throwIfDeliveryAborted();
			if (persistedDelivery && persistedDeadline) {
				await this.settlePersistedDelivery(
					[persistedPlanningRecord!],
					minReplicasValue,
					persistedDelivery,
					ownershipLifecycleController,
					persistedDeadline,
					true,
					(leadersByEntry, ownershipRevision) => {
						const leaders = leadersByEntry[0];
						persistedBackfillLeaders = leaders ? new Map(leaders) : undefined;
						persistedBackfillOwnershipRevision = ownershipRevision;
					},
				);
			}
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return result;
		} catch (error) {
			if (persistedDelivery && committedHashes) {
				throw new PersistedDeliveryError(error, committedHashes);
			}
			throw error;
		} finally {
			if (
				persistedBackfillSource &&
				persistedBackfillLeaders &&
				persistedBackfillOwnershipRevision !== undefined &&
				localAppendProcessed
			) {
				// A persisted quorum changes the return condition, not the configured
				// replication degree. Reuse settlement's latest fresh leader plan rather
				// than carrying an optimistic target or extending the receipt deadline
				// with another plan. Fence best-effort repair to this append's ownership
				// generation, and never let it mask the primary result.
				try {
					this.queuePersistedAppendBackfill(
						persistedBackfillSource,
						persistedBackfillLeaders,
						minReplicasValue,
						ownershipLifecycleController,
						persistedBackfillOwnershipRevision,
					);
				} catch {}
			}
			persistedDeadline?.dispose();
		}
	}

	private rejectPersistedDeliveryOnTrustedLocalAppend(
		options?: SharedAppendOptions<T>,
	): void {
		if (this.capturePersistedAppendInvocation(options)) {
			throw new Error(
				"trusted local append paths require delivery=false; call deliverPersistedEntries after the local commit",
			);
		}
	}

	// Trusted local append path for callers that already validated the entry.
	private async appendLocallyValidated(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		this.throwIfNativeDurableCommitFailed();
		this.rejectPersistedDeliveryOnTrustedLocalAppend(options);
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		if (options?.canAppend || options?.onChange) {
			throw new Error(
				"appendLocallyValidated does not accept canAppend or onChange hooks",
			);
		}
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } = this.createLogAppendOptions(
			options,
			ownershipLifecycleController,
		);
		appendOptions.__peerbitCanAppendAlreadyValidated = true;
		appendOptions.onChange = (change) =>
			this.onChange(change, ownershipLifecycleController);
		const result = await this.log.append(data, appendOptions);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		await this.processLocalAppend(result.entry, result.removed, options, {
			minReplicasValue,
			ownershipLifecycleController,
		});
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return result;
	}

	// Trusted local append path that lets the shared log own change application.
	private async appendLocallyPrepared(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
		properties?: {
			skipMissingNextJoin?: boolean;
			resolveTrimmedEntries?: boolean;
			payloadData?: Uint8Array;
			localCommitEvidence?: TrustedLocalCommitEvidence;
		},
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
		removedHashes?: string[];
		removedGids?: string[];
		appendCommit: PreparedLocalAppendCommit<R>;
	}> {
		this.throwIfNativeDurableCommitFailed();
		this.rejectPersistedDeliveryOnTrustedLocalAppend(options);
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
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
		attachTrustedLocalCommitEvidence(
			appendOptions,
			properties?.localCommitEvidence,
		);
		const result = await asTrustedLowerLog(this.log).appendLocallyPrepared(
			data,
			appendOptions,
			{
				skipMissingNextJoin: properties?.skipMissingNextJoin,
				resolveTrimmedEntries: properties?.resolveTrimmedEntries,
				payloadData: properties?.payloadData,
			},
		);
		if (properties?.localCommitEvidence) {
			properties.localCommitEvidence.committedHashes.add(
				result.appendFacts.hash,
			);
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePreparedCommit =
			await this.processNativePreparedTargetNoneAppend(result, options, {
				minReplicasValue,
				ownershipLifecycleController,
			});
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (nativePreparedCommit) {
			return {
				entry: result.entry,
				removed: result.removed,
				removedHashes: result.removedHashes,
				removedGids: result.removedGids,
				appendCommit: nativePreparedCommit,
			};
		}
		let nativeAppendPlan: NativeAppendEntryPlan<R> | undefined;
		let deferredCoordinateDeleteHashes: string[] | undefined;
		if (this.canCoalescePreparedAppendCoordinateDeletes(result, options)) {
			deferredCoordinateDeleteHashes =
				this.applyChangeWithDeferredCoordinateDeletes(result.change, {
					ownershipLifecycleController,
				});
			nativeAppendPlan = await this.planNativeLocalAppendFacts(
				result.appendFacts,
				minReplicasValue,
				undefined,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (!nativeAppendPlan) {
				if (deferredCoordinateDeleteHashes) {
					await this._coordinates.deleteCoordinatesForHashes(
						deferredCoordinateDeleteHashes,
						ownershipLifecycleController,
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
				deferredCoordinateDeleteHashes = undefined;
			}
		} else {
			const changeResult = this.applyChange(result.change, {
				ownershipLifecycleController,
			});
			if (isPromiseLike(changeResult)) {
				await changeResult;
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
		}
		try {
			nativeAppendPlan =
				(await this.processLocalAppend(result.entry, result.removed, options, {
					minReplicasValue,
					appendFacts: result.appendFacts,
					nativeAppendPlan,
					extraCoordinateDeleteHashes: deferredCoordinateDeleteHashes,
					ownershipLifecycleController,
				})) ?? nativeAppendPlan;
		} catch (error) {
			if (deferredCoordinateDeleteHashes) {
				await this._coordinates.deleteCoordinatesForHashes(
					deferredCoordinateDeleteHashes,
					ownershipLifecycleController,
				);
			}
			throw error;
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return {
			entry: result.entry,
			removed: result.removed,
			removedHashes: result.removedHashes,
			removedGids: result.removedGids,
			appendCommit: this.createPreparedLocalAppendCommitFromFacts(
				result.appendFacts,
				nativeAppendPlan,
			),
		};
	}

	private async processNativePreparedTargetNoneAppend(
		result: {
			entry?: Entry<T>;
			materializeEntry?: () => Entry<T>;
			removed: ShallowOrFullEntry<T>[];
			removedHashes?: string[];
			removedGids?: string[];
			change?: Change<T>;
			appendFacts: PreparedAppendFacts;
		},
		options: SharedAppendOptions<T> | undefined,
		properties: {
			minReplicasValue: number;
			ownershipLifecycleController?: AbortController;
		},
	): Promise<PreparedLocalAppendCommit<R> | undefined> {
		const ownershipLifecycleController =
			properties.ownershipLifecycleController ??
			this.captureReplicationOwnershipLifecycle();
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (
			options?.target !== "none" ||
			options?.replicate === true ||
			this.shouldDeferHeadCoordinatePersistence(options) ||
			(!this._nativeSharedLogState && !this._nativeBackbone) ||
			!this.canPlanNativeAppendFacts(result.appendFacts)
		) {
			return undefined;
		}

		const plannedCoordinateDeleteHashes =
			result.change?.removed.map((entry) => entry.hash) ??
			result.removedHashes ??
			result.removed.map((entry) => entry.hash);
		const nativeAppendPlan = await this.planNativeLocalAppendFacts(
			result.appendFacts,
			properties.minReplicasValue,
			{
				deleteHashes:
					plannedCoordinateDeleteHashes.length > 0
						? plannedCoordinateDeleteHashes
						: undefined,
			},
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (!nativeAppendPlan) {
			return undefined;
		}

		let deferredCoordinateDeleteHashes: string[] | undefined;
		try {
			deferredCoordinateDeleteHashes = result.change
				? this.applyChangeWithDeferredCoordinateDeletes(result.change, {
						forgetNativeCoordinates:
							!nativeAppendPlan.committedNativeCoordinateDeletes,
						ownershipLifecycleController,
					})
				: this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
						result.appendFacts,
						result.removed,
						() => this.materializePreparedAppendResultEntry(result),
						{
							forgetNativeCoordinates:
								!nativeAppendPlan.committedNativeCoordinateDeletes,
							removedHashes: result.removedHashes,
							removedGids: result.removedGids,
							ownershipLifecycleController,
						},
					);
			await this._coordinates.persistPreparedCoordinate(
				{
					prepared: nativeAppendPlan.preparedCoordinate,
					hash: result.appendFacts.hash,
					nextHashes: result.appendFacts.next,
					deleteHashes: deferredCoordinateDeleteHashes,
					coordinates: nativeAppendPlan.coordinates,
					replicas: nativeAppendPlan.coordinates.length,
					commitNative:
						nativeAppendPlan.committedNativeCoordinateState !== true,
					commitNativeBackbone:
						nativeAppendPlan.committedNativeBackboneCoordinateState !== true,
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		} catch (error) {
			if (deferredCoordinateDeleteHashes) {
				await this._coordinates.deleteCoordinatesForHashes(
					deferredCoordinateDeleteHashes,
					ownershipLifecycleController,
				);
			}
			throw error;
		}

		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		if (!nativeAppendPlan.isLeader && !delayAdaptiveRebalance) {
			let leaders = nativeAppendPlan.leaders;
			let pruneEntry: EntryReplicated<R> | undefined;
			if (!leaders) {
				pruneEntry = this._coordinates.materializePreparedCoordinateEntry(
					nativeAppendPlan.preparedCoordinate,
				);
				leaders = (
					await this.planEntryLeaders(
						pruneEntry,
						properties.minReplicasValue,
						{
							persist: false,
						},
						ownershipLifecycleController,
					)
				).leaders;
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
			pruneEntry ??= this._coordinates.materializePreparedCoordinateEntry(
				nativeAppendPlan.preparedCoordinate,
			);
			await this.pruneDebouncedFnAddIfNotKeeping(
				{
					key: pruneEntry.hash,
					value: { entry: pruneEntry, leaders },
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		if (!delayAdaptiveRebalance) {
			this.rebalanceParticipationDebounced?.call();
		}
		return this.createPreparedLocalAppendCommitFromFacts(
			result.appendFacts,
			nativeAppendPlan,
		);
	}

	private async appendLocallyPreparedPayload(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<T> | undefined,
		properties?: PreparedPayloadCommitOnlyProperties,
	) {
		return this.appendLocallyPrepared(undefined as T, options, {
			skipMissingNextJoin: properties?.skipMissingNextJoin,
			resolveTrimmedEntries: properties?.resolveTrimmedEntries,
			payloadData,
			...(properties?.localCommitEvidence
				? { localCommitEvidence: properties.localCommitEvidence }
				: undefined),
		});
	}

	// Trusted local payload append path that keeps the public Entry lazy.
	private appendLocallyPreparedPayloadCommitOnly(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<T> | undefined,
		properties?: PreparedPayloadCommitOnlyProperties,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfNativeDurableCommitFailed();
		this.rejectPersistedDeliveryOnTrustedLocalAppend(options);
		if (options?.canAppend || options?.onChange) {
			throw new Error(
				"appendLocallyPreparedPayloadCommitOnly does not accept canAppend or onChange hooks",
			);
		}
		if (
			options?.target !== "none" ||
			options?.replicate === true ||
			(!this.shouldDeferHeadCoordinatePersistence(options) &&
				!this._nativeSharedLogState &&
				!this._coordinates.canUseNativeBackboneResidentCoordinateState())
		) {
			return undefined;
		}
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		appendOptions.__peerbitCanAppendAlreadyValidated = true;
		attachTrustedLocalCommitEvidence(
			appendOptions,
			properties?.localCommitEvidence,
		);
		const deferHeadCoordinatePersistence =
			this.shouldDeferHeadCoordinatePersistence(options);
		const nativeBackboneResult =
			this.appendLocallyPreparedPayloadNativeBackboneCommitOnly(
				payloadData,
				appendOptions,
				options,
				properties,
				minReplicasValue,
				deferHeadCoordinatePersistence,
				ownershipLifecycleController,
			);
		if (nativeBackboneResult) {
			return mapMaybePromise(nativeBackboneResult, (result) => {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				if (result) {
					return result;
				}
				return this.appendLocallyPreparedPayloadCommitOnlyFallback(
					payloadData,
					appendOptions,
					options,
					properties,
					minReplicasValue,
					deferHeadCoordinatePersistence,
					ownershipLifecycleController,
				);
			});
		}
		return this.appendLocallyPreparedPayloadCommitOnlyFallback(
			payloadData,
			appendOptions,
			options,
			properties,
			minReplicasValue,
			deferHeadCoordinatePersistence,
			ownershipLifecycleController,
		);
	}

	// Strict native document path. Never falls back to compatibility append.
	private appendStrictNativeDocumentPayloadCommitOnly(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<T> | undefined,
		properties?: PreparedPayloadCommitOnlyProperties,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfNativeDurableCommitFailed();
		this.rejectPersistedDeliveryOnTrustedLocalAppend(options);
		if (options?.canAppend || options?.onChange) {
			throw new Error(
				"appendStrictNativeDocumentPayloadCommitOnly does not accept canAppend or onChange hooks",
			);
		}
		if (
			options?.target !== "none" ||
			options?.replicate === true ||
			(!this.shouldDeferHeadCoordinatePersistence(options) &&
				!this._nativeSharedLogState &&
				!this._coordinates.canUseNativeBackboneResidentCoordinateState())
		) {
			return undefined;
		}
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } =
			this.createLogAppendOptions(options);
		appendOptions.__peerbitCanAppendAlreadyValidated = true;
		attachTrustedLocalCommitEvidence(
			appendOptions,
			properties?.localCommitEvidence,
		);
		const result = this.appendLocallyPreparedPayloadNativeBackboneCommitOnly(
			payloadData,
			appendOptions,
			options,
			properties,
			minReplicasValue,
			this.shouldDeferHeadCoordinatePersistence(options),
			ownershipLifecycleController,
		);
		return mapMaybePromise(result, (commitOnly) => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return commitOnly ?? undefined;
		});
	}

	private appendLocallyPreparedPayloadCommitOnlyFallback(
		payloadData: Uint8Array,
		appendOptions: AppendOptions<T>,
		options: SharedAppendOptions<T> | undefined,
		properties: PreparedPayloadCommitOnlyProperties | undefined,
		minReplicasValue: number,
		deferHeadCoordinatePersistence: boolean,
		ownershipLifecycleController: AbortController,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const resultMaybe = asTrustedLowerLog(
			this.log,
		).appendLocallyPreparedCommitOnly(undefined as T, appendOptions, {
			skipMissingNextJoin: properties?.skipMissingNextJoin,
			resolveTrimmedEntries: properties?.resolveTrimmedEntries,
			payloadData,
			includeMaterializationBytes: false,
			includeAppendFactsBytes: !deferHeadCoordinatePersistence,
		});
		return mapMaybePromise(resultMaybe, (result) => {
			if (result && properties?.localCommitEvidence) {
				properties.localCommitEvidence.committedHashes.add(
					result.appendFacts.hash,
				);
			}
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return this.finishPreparedPayloadCommitOnlyAppend(
				result,
				options,
				minReplicasValue,
				ownershipLifecycleController,
			);
		});
	}

	private appendLocallyPreparedPayloadNativeBackboneCommitOnly(
		payloadData: Uint8Array,
		appendOptions: AppendOptions<T>,
		options: SharedAppendOptions<T> | undefined,
		properties: PreparedPayloadCommitOnlyProperties | undefined,
		minReplicasValue: number,
		deferHeadCoordinatePersistence: boolean,
		ownershipLifecycleController: AbortController,
	):
		| MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined>
		| undefined {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (
			!this._nativeBackbone ||
			options?.target !== "none" ||
			options?.replicate === true
		) {
			return undefined;
		}
		if (!deferHeadCoordinatePersistence) {
			return this.appendLocallyPreparedPayloadNativeBackboneStorageTransaction(
				payloadData,
				appendOptions,
				properties,
				minReplicasValue,
				options?.replicate === false,
				ownershipLifecycleController,
			);
		}
		const hasDocumentIndexCommit =
			!!properties?.nativeBackboneDocumentIndex ||
			!!properties?.prepareNativeBackboneDocumentIndex ||
			!!properties?.nativeBackboneDocumentDeleteKey;
		if (
			options?.replicate === false &&
			hasDocumentIndexCommit &&
			this._coordinates.canUseBackboneOnlyCoordinatePersistence()
		) {
			return this.appendLocallyPreparedPayloadNativeBackboneStorageTransaction(
				payloadData,
				appendOptions,
				properties,
				minReplicasValue,
				true,
				ownershipLifecycleController,
			);
		}
		if (
			options?.replicate === false &&
			hasDocumentIndexCommit &&
			((appendOptions.meta?.next?.length ?? 0) > 0 ||
				(properties?.useNativeExistingDocumentContext === true &&
					properties?.resolveTrimmedEntries !== false))
		) {
			return this.appendLocallyPreparedPayloadNativeBackboneStorageTransaction(
				payloadData,
				appendOptions,
				properties,
				minReplicasValue,
				true,
				ownershipLifecycleController,
			);
		}
		if (options?.replicate !== false) {
			return undefined;
		}
		const backbone = this._nativeBackbone;
		// When the durable write-through wrapper is active the log's block store
		// (this.remoteBlocks.localStore) is the wrapper, NOT the raw wasm block map
		// (backbone.blocks), so this comparison is false. In that case the block
		// must be mirrored to durable directly; see the guarded handling in the
		// prepare callback below.
		const durableWrapperActive =
			this.remoteBlocks?.localStore !== backbone.blocks;
		// The write-through wrapper instance, captured only when active, so the
		// commit-only block can be mirrored to durable WITHOUT routing it through
		// the log's finishBlocks/putKnown* (which would disturb the strict-native
		// resident-coordinate append path). `mirrorToDurable` writes to the durable
		// side only; the lower-log result is held behind that durability barrier.
		const durableWrapper = durableWrapperActive
			? (this.remoteBlocks?.localStore as unknown as {
					beginNativeDeleteCleanup?: (cids: string[]) => number | undefined;
					cancelNativeDeleteCleanup?: (cleanupToken: unknown) => void;
					mirrorToDurable?: (
						cid: string,
						bytes: Uint8Array,
						options?: { nativeTrimmed?: boolean },
					) => Promise<unknown>;
					rollbackFailedNativeCommits?: (
						cids: string[],
						restoreNativeCids?: string[],
					) => Promise<void>;
				})
			: undefined;
		let nativeBackboneDocumentIndexCommitted = false;
		let nativeDeleteCleanupToken: unknown;
		let nativeDocumentRollback: NativeBackboneDocumentRollback | undefined;
		let nativeStrictTransaction:
			| NativeStrictDurableTransactionHandle
			| undefined;
		let lowerPublicationRollback:
			| {
					committedHashes: string[];
					trimmedEntries?: Parameters<
						NativePeerbitBackbone["graph"]["putBatch"]
					>[0];
					coordinateEntries?: NativeBackboneCoordinateRollback<R>;
					documents?: NativeBackboneDocumentRollback[];
					durableWrapper?: {
						rollbackUnmirroredNativeCommits?: (
							cids: string[],
							restoreNativeCids?: string[],
						) => Promise<void>;
						rollbackFailedNativeCommits?: (
							cids: string[],
							restoreNativeCids?: string[],
						) => Promise<void>;
					};
					lowerPublicationStarted: boolean;
			  }
			| undefined;
		const nativeCommitProperties = {
			payloadData,
			resolveTrimmedEntries: properties?.resolveTrimmedEntries,
		} as {
			payloadData: Uint8Array;
			resolveTrimmedEntries?: boolean;
			skipMissingNextJoin?: boolean;
			retainMaterializationBytes?: boolean;
			deferNativeTransactionAcknowledgement?: boolean;
		};
		nativeCommitProperties.skipMissingNextJoin =
			properties?.skipMissingNextJoin;
		nativeCommitProperties.retainMaterializationBytes =
			this._logProperties?.trim != null;
		nativeCommitProperties.deferNativeTransactionAcknowledgement = true;
		const rollbackLowerPublication = async (error: unknown): Promise<never> => {
			durableWrapper?.cancelNativeDeleteCleanup?.(nativeDeleteCleanupToken);
			const rollbackFailures: unknown[] = [];
			if (
				lowerPublicationRollback &&
				!(error instanceof NativeDurableCommitError)
			) {
				try {
					const lowerPublicationStarted =
						lowerPublicationRollback.lowerPublicationStarted;
					await this.rollbackFailedNativeBackboneTransaction({
						committedHashes: lowerPublicationRollback.committedHashes,
						trimmedEntries: lowerPublicationRollback.trimmedEntries,
						coordinateEntries: lowerPublicationRollback.coordinateEntries,
						documents: lowerPublicationRollback.documents,
						durableWrapper: lowerPublicationStarted
							? undefined
							: lowerPublicationRollback.durableWrapper,
						skipBlockCompensation: lowerPublicationStarted,
						unmirroredBlockCompensation: !lowerPublicationStarted,
						restoreGraphFromIndex: true,
					});
				} catch (rollbackError) {
					rollbackFailures.push(rollbackError);
				}
			} else if (
				nativeDocumentRollback &&
				!(error instanceof NativeDurableCommitError)
			) {
				try {
					this.restoreNativeBackboneDocument(nativeDocumentRollback);
					const flushed =
						this._coordinates.flushNativeBackboneCoordinateJournal();
					if (isPromiseLike(flushed)) {
						await flushed;
					}
				} catch (rollbackError) {
					rollbackFailures.push(rollbackError);
				}
			}
			if (rollbackFailures.length > 0) {
				this.releaseNativeStrictDurableTransaction(nativeStrictTransaction);
				throw new AggregateError(
					[error, ...rollbackFailures],
					"Lower-log publication and native compensation both failed",
				);
			}
			await this.completeNativeStrictDurableTransaction(
				nativeStrictTransaction,
			);
			throw error;
		};
		let result: MaybePromise<
			TrustedLowerLogCommitOnlyAppendResult<T> | undefined
		>;
		try {
			result = asTrustedLowerLog(
				this.log,
			).appendLocallyPreparedNativeNoNextCommitOnly(
				undefined as T,
				appendOptions,
				nativeCommitProperties,
				async (input) => {
					const next =
						"next" in input && Array.isArray(input.next) ? input.next : [];
					const nativeBackboneDocumentIndex =
						properties?.nativeBackboneDocumentIndex ??
						properties?.prepareNativeBackboneDocumentIndex?.({
							wallTime: input.wallTime,
							gid: input.gid,
							payloadSize: input.payloadData.byteLength,
						});
					const nativeBackboneDocumentIndexForAppend =
						nativeBackboneDocumentIndex &&
						input.trimLengthTo == null &&
						nativeBackboneDocumentIndex.deleteTrimmedHeads === true
							? {
									...nativeBackboneDocumentIndex,
									deleteTrimmedHeads: false,
								}
							: nativeBackboneDocumentIndex;
					if (nativeBackboneDocumentIndex) {
						nativeBackboneDocumentIndexCommitted = true;
					}
					const useLatestDocumentContext =
						properties?.useNativeExistingDocumentContext === true;
					nativeDocumentRollback = this.snapshotNativeBackboneDocument(
						nativeBackboneDocumentIndexForAppend,
					);
					nativeStrictTransaction =
						await this.beginNativeStrictDurableTransaction(
							nativeDocumentRollback ? [nativeDocumentRollback] : [],
						);
					const prepared = backbone.graph.prepareEntryV0PlainEntryCommit(
						{
							...input,
							next,
							includeMaterializationBytes: false,
							includeAppendFactsBytes: true,
							trimLengthTo: input.trimLengthTo,
							...(nativeBackboneDocumentIndexForAppend
								? {
										documentIndex: {
											...nativeBackboneDocumentIndexForAppend,
											...(useLatestDocumentContext
												? { useLatestContext: true }
												: {}),
										},
									}
								: {}),
						},
						backbone.blocks,
					);
					if (prepared) {
						const preparedHash = prepared.cid ?? prepared.hash;
						const preparedNext = prepared.next ?? next;
						const nativeTrimmedHashes =
							prepared.trimmedEntryHashes ??
							(
								prepared.trimmedEntries as Array<{ hash?: string }> | undefined
							)?.flatMap((entry) => (entry.hash ? [entry.hash] : [])) ??
							[];
						const coordinateRollback =
							this._coordinates.snapshotResidentCoordinateEntries([
								...(preparedHash ? [preparedHash] : []),
								...preparedNext,
								...nativeTrimmedHashes,
							]);
						lowerPublicationRollback = {
							committedHashes: preparedHash ? [preparedHash] : [],
							trimmedEntries: prepared.trimmedEntries,
							coordinateEntries: coordinateRollback,
							documents: nativeDocumentRollback
								? [nativeDocumentRollback]
								: undefined,
							durableWrapper,
							lowerPublicationStarted: false,
						};
						await this.setNativeStrictDurableTransactionOperation(
							nativeStrictTransaction,
							preparedHash ? [preparedHash] : [],
							nativeTrimmedHashes,
							coordinateRollback,
							combineCoordinateDeleteHashes(preparedNext, nativeTrimmedHashes),
						);
						if (prepared.bytes) {
							lowerPublicationRollback.lowerPublicationStarted = true;
							return {
								...prepared,
								nativeIndexMutationLockOwner:
									nativeStrictTransaction?.lowerHashMutationLockOwner,
							};
						}
						const rollbackCommitted = async (
							cause: unknown,
							committedCids: string[],
						): Promise<never> => {
							durableWrapper?.cancelNativeDeleteCleanup?.(
								nativeDeleteCleanupToken,
							);
							let compensated = false;
							try {
								await this.rollbackFailedNativeBackboneTransaction({
									committedHashes: committedCids,
									trimmedEntries: prepared.trimmedEntries,
									coordinateEntries: coordinateRollback,
									documents: nativeDocumentRollback
										? [nativeDocumentRollback]
										: undefined,
									durableWrapper,
								});
								compensated = true;
							} catch {
								// Keep recovery marked incomplete; close will discard pending native
								// journals. Reopen preserves uncertain content-addressed bytes and
								// recovers liveness from the authoritative lower-log facts.
							}
							if (compensated) {
								await this.completeNativeStrictDurableTransaction(
									nativeStrictTransaction,
								);
							} else {
								this.releaseNativeStrictDurableTransaction(
									nativeStrictTransaction,
								);
							}
							return this.failNativeDurableCommit(cause, {
								committedCids,
								failedCids: committedCids,
							});
						};
						if (
							durableWrapper &&
							nativeTrimmedHashes.length > 0 &&
							!durableWrapper.beginNativeDeleteCleanup
						) {
							return rollbackCommitted(
								new Error(
									"Native durable block wrapper cannot preannounce trim cleanup",
								),
								preparedHash ? [preparedHash] : [],
							);
						}
						nativeDeleteCleanupToken =
							durableWrapper?.beginNativeDeleteCleanup?.(nativeTrimmedHashes);
						const preparedResult = {
							...prepared,
							nativeIndexMutationLockOwner:
								nativeStrictTransaction?.lowerHashMutationLockOwner,
							getBytes: (hash: string) => backbone.blocks.get(hash),
							nativeBlocksDeleted: true,
							nativeDeleteCleanupToken,
						};
						if (durableWrapper) {
							// The durable write-through wrapper is active, so the block store
							// the log writes through (this.remoteBlocks.localStore) is the
							// wrapper, NOT the raw wasm block map. prepareEntryV0PlainEntryCommit
							// committed the block into the wasm map ONLY and returned no raw
							// bytes, so on its own the block would never reach durable (log's
							// finishBlocks only calls putKnown* when prepared.bytes is set) and
							// a non-replicating native node would lose it on restart.
							//
							// Mirror the just-committed block (read back from the wasm store)
							// straight to the DURABLE side of the wrapper. Crucially we do NOT
							// attach prepared.bytes: doing so would make the log's finishBlocks
							// call putKnown*, which changes the commit-only append path and
							// breaks the strict-native resident-coordinate optimization (the
							// reopen tests assert the append stays native and resolves no entry
							// block). Instead the prepared result is returned exactly as the
							// memory-only branch below (getBytes only, no bytes), so the log's
							// finishBlocks path is UNCHANGED. Returning the mirror promise from
							// this prepare callback holds lower-log index/head/trim publication
							// until durable succeeds.
							if (!preparedHash) {
								durableWrapper.cancelNativeDeleteCleanup?.(
									nativeDeleteCleanupToken,
								);
								return rollbackCommitted(
									new Error("Native commit returned no entry CID to mirror"),
									[],
								);
							}
							if (!durableWrapper.mirrorToDurable) {
								durableWrapper.cancelNativeDeleteCleanup?.(
									nativeDeleteCleanupToken,
								);
								return rollbackCommitted(
									new Error(
										"Native durable block wrapper has no mirror method",
									),
									[preparedHash],
								);
							}
							const committedBytes = backbone.blocks.get(preparedHash);
							if (!committedBytes) {
								durableWrapper.cancelNativeDeleteCleanup?.(
									nativeDeleteCleanupToken,
								);
								return rollbackCommitted(
									new Error(
										`Native committed block ${preparedHash} is missing from the hot store`,
									),
									[preparedHash],
								);
							}
							return durableWrapper
								.mirrorToDurable(preparedHash, committedBytes, {
									nativeTrimmed: nativeTrimmedHashes.includes(preparedHash),
								})
								.then(
									(nativeCommitOwnershipToken) => {
										lowerPublicationRollback!.lowerPublicationStarted = true;
										return {
											...preparedResult,
											nativeCommitOwnershipToken,
										};
									},
									(error) => {
										durableWrapper.cancelNativeDeleteCleanup?.(
											nativeDeleteCleanupToken,
										);
										return rollbackCommitted(error, [preparedHash]);
									},
								);
						}
						lowerPublicationRollback.lowerPublicationStarted = true;
						return preparedResult;
					}
					await this.completeNativeStrictDurableTransaction(
						nativeStrictTransaction,
					);
					return prepared;
				},
			);
			if (isPromiseLike(result)) {
				result = result.catch(rollbackLowerPublication);
			}
		} catch (error) {
			return rollbackLowerPublication(error);
		}
		if (!result) {
			// Abandon arm: the token was minted but nothing downstream can
			// roll it back from here.
			this._coordinates.settleResidentCoordinateSnapshot(
				lowerPublicationRollback?.coordinateEntries,
			);
			return this.completeNativeStrictDurableTransaction(
				nativeStrictTransaction,
			).then(() => undefined);
		}
		return mapMaybePromise(result, async (prepared) => {
			if (!prepared) {
				// Abandon arm: same shape as the `!result` arm above.
				this._coordinates.settleResidentCoordinateSnapshot(
					lowerPublicationRollback?.coordinateEntries,
				);
				await this.completeNativeStrictDurableTransaction(
					nativeStrictTransaction,
				);
				return undefined;
			}
			const rollback = async (error: unknown): Promise<never> => {
				const rollbackFailures: unknown[] = [];
				try {
					await this.markNativeStrictDurableTransactionRollback(
						nativeStrictTransaction,
					);
				} catch (rollbackError) {
					const retentionFailures =
						this.retainNativeStrictDurableTransactionAfterMarkerFailure(
							nativeStrictTransaction,
							prepared.nativeCommittedAppendFinalizer,
							rollbackError,
						);
					throw new AggregateError(
						[error, ...retentionFailures],
						"Native rollback marker could not be persisted; recovery is required",
					);
				}
				try {
					await prepared.nativeCommittedAppendFinalizer?.rollback();
				} catch (rollbackError) {
					rollbackFailures.push(rollbackError);
				}
				try {
					await this._coordinates.rollbackNativeBackboneCoordinateAppendDurably(
						prepared.appendFacts.hash,
						lowerPublicationRollback?.coordinateEntries,
					);
					// Terminal: this is the last rollback consumer for the
					// token. A throw above leaves the row, which is safe.
					this._coordinates.settleResidentCoordinateSnapshot(
						lowerPublicationRollback?.coordinateEntries,
					);
					for (const document of lowerPublicationRollback?.documents ?? []) {
						this.restoreNativeBackboneDocument(document);
					}
					const flushed =
						this._coordinates.flushNativeBackboneCoordinateJournal();
					if (isPromiseLike(flushed)) {
						await flushed;
					}
				} catch (rollbackError) {
					rollbackFailures.push(rollbackError);
				}
				if (rollbackFailures.length === 0) {
					try {
						await this.completeNativeStrictDurableTransaction(
							nativeStrictTransaction,
						);
					} catch (rollbackError) {
						rollbackFailures.push(rollbackError);
					}
				} else {
					this.releaseNativeStrictDurableTransaction(nativeStrictTransaction);
				}
				if (rollbackFailures.length > 0) {
					throw new AggregateError(
						[error, ...rollbackFailures],
						"Shared-log append and compensation both failed",
					);
				}
				throw error;
			};
			let finishResult: PreparedPayloadCommitOnlyResult<T, R> | undefined;
			try {
				await this.setNativeStrictDurableTransactionExpectedRows(
					nativeStrictTransaction,
					[prepared.shallowEntry],
				);
				const finish = (): PreparedPayloadCommitOnlyResult<T, R> => {
					const appendCommit = this.createPreparedLocalAppendCommitFromFacts(
						prepared.appendFacts,
					);
					if (nativeBackboneDocumentIndexCommitted) {
						appendCommit.nativeBackboneDocumentIndexCommitted = true;
						appendCommit.nativeBackboneDocumentIndexTrimmedHeadsProcessed =
							prepared.documentTrimmedHeadsProcessed;
					}
					return {
						get entry() {
							return prepared.entry;
						},
						removed: prepared.removed,
						removedHashes: prepared.removedHashes,
						removedGids: prepared.removedGids,
						appendCommit,
					};
				};
				if (!prepared.nativeCommittedAppendFinalizer) {
					throw new Error("Missing deferred native append finalizer");
				}
				// Strict success cannot honor batching thresholds: native
				// coordinate/document/signer facts must be physically durable before
				// the lower commit marker is acknowledged and its intent is retired.
				await this._coordinates.flushNativeBackboneCoordinateJournal();
				await prepared.nativeCommittedAppendFinalizer.acknowledge(() =>
					this.markNativeStrictDurableTransactionLowerMarker(
						nativeStrictTransaction,
					),
				);
				finishResult = finish();
			} catch (error) {
				return rollback(error);
			}
			// Success seam. The last await inside the protected try is the
			// finalizer acknowledge; `finish()` is synchronous, so no async
			// boundary separates the catch above from this statement and
			// `rollback` can no longer fire. Nothing downstream rolls back
			// (the retire below only warns), so the token is terminal here.
			if (properties?.localCommitEvidence) {
				properties.localCommitEvidence.committedHashes.add(
					prepared.appendFacts.hash,
				);
			}
			await this.finishCommittedNativeStrictDurableTransaction(
				nativeStrictTransaction,
				() => {
					this._coordinates.settleResidentCoordinateSnapshot(
						lowerPublicationRollback?.coordinateEntries,
					);
					this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
						prepared.appendFacts,
						prepared.removed,
						prepared.materializeEntry,
						{
							removedHashes: prepared.removedHashes,
							removedGids: prepared.removedGids,
						},
					);
				},
			);
			return finishResult;
		});
	}

	private appendLocallyPreparedPayloadNativeBackboneStorageTransaction(
		payloadData: Uint8Array,
		appendOptions: AppendOptions<T>,
		properties: PreparedPayloadCommitOnlyProperties | undefined,
		minReplicasValue: number,
		runtimeOnlyCoordinates: boolean,
		ownershipLifecycleController: AbortController,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const backbone = this._nativeBackbone;
		if (
			!backbone ||
			!this._coordinates.canUseNativeBackboneResidentCoordinateState()
		) {
			return undefined;
		}
		if (
			properties?.nativeBackboneDocumentDeleteKey &&
			!this._coordinates.canUseBackboneOnlyCoordinatePersistence()
		) {
			return undefined;
		}
		return mapMaybePromise(
			this.createLeaderSelectionContext(
				undefined,
				ownershipLifecycleController,
			),
			(context) => {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				const nativeLeaderOptions = this.createNativeLeaderOptions(context);
				let backboneAppend:
					| ReturnType<
							NativePeerbitBackbone["preparePlainStorageAppendTransaction"]
					  >
					| ReturnType<
							NativePeerbitBackbone["preparePlainCommittedStorageAppendTransaction"]
					  >
					| ReturnType<
							NativePeerbitBackbone["preparePlainCommittedNoNextStorageAppendTransaction"]
					  >
					| undefined;
				// The write-through durable wrapper, when active, is captured here so a
				// just-committed block can be mirrored to durable without disturbing the
				// strict-native commit path. When the wrapper is active the log's block
				// store is the wrapper (not the raw wasm map), so `localStore ===
				// backbone.blocks` is false — but the store is still native-backed, so we
				// must commit blocks in the backbone (the committed native prepare variant
				// is the one that emits the document-signer journal record; the
				// block-deferring variant does not, which would otherwise leave
				// document-signers.wal unwritten and break same-signer facts after
				// reopen). The block is then mirrored to durable out-of-band below.
				const durableWrapperActive =
					this.remoteBlocks?.localStore !== backbone.blocks;
				const durableWrapper = durableWrapperActive
					? (this.remoteBlocks?.localStore as unknown as {
							beginNativeDeleteCleanup?: (cids: string[]) => number | undefined;
							cancelNativeDeleteCleanup?: (cleanupToken: unknown) => void;
							mirrorToDurable?: (
								cid: string,
								bytes: Uint8Array,
								options?: { nativeTrimmed?: boolean },
							) => Promise<unknown>;
							rollbackFailedNativeCommits?: (
								cids: string[],
								restoreNativeCids?: string[],
							) => Promise<void>;
						})
					: undefined;
				const commitBlocksInBackbone =
					this.remoteBlocks?.localStore === backbone.blocks ||
					durableWrapperActive;
				let nativeBackboneDocumentIndexCommitted = false;
				let nativeBackboneDocumentDeleteCommitted = false;
				let nativeDocumentRollback: NativeBackboneDocumentRollback | undefined;
				let nativeCoordinateRollback:
					| NativeBackboneCoordinateRollback<R>
					| undefined;
				let nativeDeleteCleanupToken: unknown;
				let nativeStrictTransaction:
					| NativeStrictDurableTransactionHandle
					| undefined;
				const prepareBackboneAppend = async (input: {
					wallTime: bigint;
					logical: number;
					gid: string;
					next?: string[];
					type: number;
					metaData?: Uint8Array;
					payloadData: Uint8Array;
					trimLengthTo?: number;
				}) => {
					const next = input.next ?? [];
					const appendInput = {
						wallTime: input.wallTime,
						logical: input.logical,
						gid: input.gid,
						next,
						type: input.type,
						metaData: input.metaData,
						payloadData: input.payloadData,
						replicas: minReplicasValue,
						roleAgeMs: nativeLeaderOptions.roleAge,
						now: nativeLeaderOptions.now,
						selfHash: nativeLeaderOptions.selfHash,
						selfReplicating: nativeLeaderOptions.selfReplicating,
						trimLengthTo: input.trimLengthTo,
						resolveTrimmedEntries: properties?.resolveTrimmedEntries,
					};
					const nativeBackboneDocumentIndex =
						properties?.nativeBackboneDocumentIndex ??
						properties?.prepareNativeBackboneDocumentIndex?.({
							wallTime: input.wallTime,
							gid: input.gid,
							payloadSize: input.payloadData.byteLength,
						});
					const nativeBackboneDocumentIndexForAppend =
						nativeBackboneDocumentIndex &&
						input.trimLengthTo == null &&
						nativeBackboneDocumentIndex.deleteTrimmedHeads === true
							? {
									...nativeBackboneDocumentIndex,
									deleteTrimmedHeads: false,
								}
							: nativeBackboneDocumentIndex;
					const nativeBackboneDocumentDeleteKey =
						properties?.nativeBackboneDocumentDeleteKey;
					if (
						nativeBackboneDocumentDeleteKey &&
						nativeBackboneDocumentIndexForAppend
					) {
						throw new Error(
							"Native backbone append cannot both put and delete a document index row",
						);
					}
					const appendInputWithDocumentIndex =
						nativeBackboneDocumentIndexForAppend
							? {
									...appendInput,
									documentIndex: {
										...nativeBackboneDocumentIndexForAppend,
										useLatestContext:
											properties?.useNativeExistingDocumentContext === true,
									},
								}
							: nativeBackboneDocumentDeleteKey
								? {
										...appendInput,
										documentDeleteKey: nativeBackboneDocumentDeleteKey,
									}
								: appendInput;
					nativeDocumentRollback = this.snapshotNativeBackboneDocument(
						nativeBackboneDocumentIndexForAppend ??
							(nativeBackboneDocumentDeleteKey
								? { key: nativeBackboneDocumentDeleteKey }
								: undefined),
					);
					nativeStrictTransaction =
						await this.beginNativeStrictDurableTransaction(
							nativeDocumentRollback ? [nativeDocumentRollback] : [],
						);
					this.throwIfReplicationOwnershipPoisoned();
					if (next.length === 0) {
						if (commitBlocksInBackbone) {
							if (
								nativeBackboneDocumentIndex &&
								properties?.useNativeExistingDocumentContext === true
							) {
								backboneAppend =
									backbone.preparePlainCommittedStorageAppendTransaction(
										appendInputWithDocumentIndex,
									);
							} else if (
								nativeBackboneDocumentIndex &&
								properties?.resolveTrimmedEntries === false
							) {
								backboneAppend =
									backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactTransaction(
										{
											...appendInput,
											documentIndex: nativeBackboneDocumentIndexForAppend,
										},
									);
							} else {
								backboneAppend =
									backbone.preparePlainCommittedNoNextStorageAppendTransaction(
										appendInputWithDocumentIndex,
									);
							}
						} else {
							backboneAppend =
								backbone.preparePlainNoNextStorageAppendTransaction(
									appendInputWithDocumentIndex,
								);
						}
					} else {
						backboneAppend = commitBlocksInBackbone
							? backbone.preparePlainCommittedStorageAppendTransaction(
									appendInputWithDocumentIndex,
								)
							: backbone.preparePlainStorageAppendTransaction(
									appendInputWithDocumentIndex,
								);
					}
					if (nativeBackboneDocumentIndex) {
						nativeBackboneDocumentIndexCommitted = true;
					}
					nativeBackboneDocumentDeleteCommitted =
						!!nativeBackboneDocumentDeleteKey;
					const useTrimmedHashesOnly =
						properties?.resolveTrimmedEntries === false;
					const nativeTrimmedHashes =
						backboneAppend.trimmedHashes ??
						backboneAppend.trimmed.map((entry) => entry.hash);
					const nativeTrimmedGids =
						backboneAppend.trimmedGids ??
						(backboneAppend.trimmed.length > 0
							? backboneAppend.trimmed.map((entry) => entry.gid)
							: undefined);
					const committedHash =
						backboneAppend.entry.cid ?? backboneAppend.entry.hash;
					const committedNext = backboneAppend.entry.next ?? next;
					nativeCoordinateRollback =
						this._coordinates.snapshotResidentCoordinateEntries([
							...(committedHash ? [committedHash] : []),
							...committedNext,
							...nativeTrimmedHashes,
						]);
					await this.setNativeStrictDurableTransactionOperation(
						nativeStrictTransaction,
						committedHash ? [committedHash] : [],
						nativeTrimmedHashes,
						nativeCoordinateRollback,
						combineCoordinateDeleteHashes(committedNext, nativeTrimmedHashes),
					);
					const rollbackCommitted = async (
						cause: unknown,
						committedCids: string[],
					): Promise<never> => {
						durableWrapper?.cancelNativeDeleteCleanup?.(
							nativeDeleteCleanupToken,
						);
						let compensated = false;
						try {
							await this.rollbackFailedNativeBackboneTransaction({
								committedHashes: committedCids,
								trimmedEntries: backboneAppend?.trimmed,
								coordinateEntries: nativeCoordinateRollback,
								documents: nativeDocumentRollback
									? [nativeDocumentRollback]
									: undefined,
								durableWrapper,
							});
							compensated = true;
						} catch {
							// close/reopen completes recovery if durable compensation failed
						}
						if (compensated) {
							await this.completeNativeStrictDurableTransaction(
								nativeStrictTransaction,
							);
						} else {
							this.releaseNativeStrictDurableTransaction(
								nativeStrictTransaction,
							);
						}
						return this.failNativeDurableCommit(cause, {
							committedCids,
							failedCids: committedCids,
						});
					};
					if (
						durableWrapper &&
						commitBlocksInBackbone &&
						nativeTrimmedHashes.length > 0 &&
						!durableWrapper.beginNativeDeleteCleanup
					) {
						return rollbackCommitted(
							new Error(
								"Native durable block wrapper cannot preannounce trim cleanup",
							),
							committedHash ? [committedHash] : [],
						);
					}
					nativeDeleteCleanupToken = commitBlocksInBackbone
						? durableWrapper?.beginNativeDeleteCleanup?.(nativeTrimmedHashes)
						: undefined;
					const preparedResult = {
						...backboneAppend.entry,
						nativeIndexMutationLockOwner:
							nativeStrictTransaction?.lowerHashMutationLockOwner,
						gid: backboneAppend.coordinate.gid,
						getBytes: commitBlocksInBackbone
							? (hash: string) => backbone.blocks.get(hash)
							: undefined,
						trimmedEntries: useTrimmedHashesOnly
							? undefined
							: backboneAppend.trimmed,
						trimmedEntryHashes: useTrimmedHashesOnly
							? backboneAppend.trimmedHashes
							: undefined,
						trimmedEntryGids: useTrimmedHashesOnly
							? nativeTrimmedGids
							: undefined,
						nativeBlocksDeleted: commitBlocksInBackbone,
						nativeDeleteCleanupToken,
						documentPreviousContext: backboneAppend.documentPreviousContext,
					};
					if (durableWrapper?.mirrorToDurable) {
						// The block was committed into the wasm map (commitBlocksInBackbone is
						// true) but not into durable, because the log's finishBlocks path is
						// left UNCHANGED for strict-native mode (getBytes only, no bytes, so
						// no putKnown* through the wrapper). Returning the promise here prevents
						// lower-log index/head/trim publication until the mirror settles.
						if (!committedHash) {
							durableWrapper.cancelNativeDeleteCleanup?.(
								nativeDeleteCleanupToken,
							);
							return rollbackCommitted(
								new Error("Native commit returned no entry CID to mirror"),
								[],
							);
						}
						const committedBytes =
							backboneAppend.entry.bytes ?? backbone.blocks.get(committedHash);
						if (!committedBytes) {
							durableWrapper.cancelNativeDeleteCleanup?.(
								nativeDeleteCleanupToken,
							);
							return rollbackCommitted(
								new Error(
									`Native committed block ${committedHash} is missing from the hot store`,
								),
								[committedHash],
							);
						}
						return durableWrapper
							.mirrorToDurable(committedHash, committedBytes, {
								nativeTrimmed: nativeTrimmedHashes.includes(committedHash),
							})
							.then(
								(nativeCommitOwnershipToken) => ({
									...preparedResult,
									nativeCommitOwnershipToken,
								}),
								(error) => {
									durableWrapper.cancelNativeDeleteCleanup?.(
										nativeDeleteCleanupToken,
									);
									return rollbackCommitted(error, [committedHash]);
								},
							);
					}
					if (durableWrapper) {
						durableWrapper.cancelNativeDeleteCleanup?.(
							nativeDeleteCleanupToken,
						);
						return rollbackCommitted(
							new Error("Native durable block wrapper has no mirror method"),
							committedHash ? [committedHash] : [],
						);
					}
					return preparedResult;
				};
				const hasKnownNoNext =
					appendOptions.meta?.next != null &&
					appendOptions.meta.next.length === 0;
				const appendGenericNativeCommit = () =>
					asTrustedLowerLog(this.log).appendLocallyPreparedNativeCommitOnly(
						undefined as T,
						appendOptions,
						{
							payloadData,
							resolveTrimmedEntries: properties?.resolveTrimmedEntries,
							skipMissingNextJoin: properties?.skipMissingNextJoin,
							retainMaterializationBytes: this._logProperties?.trim != null,
							deferNativeTransactionAcknowledgement: true,
						},
						prepareBackboneAppend,
					);
				const rollbackLowerPublication = async (
					error: unknown,
				): Promise<never> => {
					durableWrapper?.cancelNativeDeleteCleanup?.(nativeDeleteCleanupToken);
					let compensated = !backboneAppend;
					if (backboneAppend && !(error instanceof NativeDurableCommitError)) {
						const committedHash =
							backboneAppend.entry.cid ?? backboneAppend.entry.hash;
						try {
							await this.rollbackFailedNativeBackboneTransaction({
								committedHashes: committedHash ? [committedHash] : [],
								coordinateEntries: nativeCoordinateRollback,
								documents: nativeDocumentRollback
									? [nativeDocumentRollback]
									: undefined,
								skipBlockCompensation: true,
								restoreGraphFromIndex: true,
							});
							compensated = true;
						} catch {
							// Lower-log compensation already handled durable/native blocks.
							// Preserve the index publication error for this caller.
						}
					}
					if (compensated) {
						await this.completeNativeStrictDurableTransaction(
							nativeStrictTransaction,
						);
					} else {
						this.releaseNativeStrictDurableTransaction(nativeStrictTransaction);
					}
					throw error;
				};
				let result: MaybePromise<
					TrustedLowerLogCommitOnlyAppendResult<T> | undefined
				>;
				try {
					const directNoNextResult = hasKnownNoNext
						? asTrustedLowerLog(
								this.log,
							).appendLocallyPreparedNativeKnownNoNextCommitOnly(
								undefined as T,
								appendOptions,
								{
									payloadData,
									resolveTrimmedEntries: properties?.resolveTrimmedEntries,
									retainMaterializationBytes: this._logProperties?.trim != null,
									deferNativeTransactionAcknowledgement: true,
								},
								prepareBackboneAppend,
							)
						: undefined;
					result =
						directNoNextResult === undefined
							? appendGenericNativeCommit()
							: directNoNextResult;
					if (isPromiseLike(result)) {
						result = result.catch(rollbackLowerPublication);
					}
				} catch (error) {
					return rollbackLowerPublication(error);
				}
				return mapMaybePromise(result, async (prepared) => {
					if (!prepared || !backboneAppend) {
						// Abandon arm: the token was minted inside
						// `prepareBackboneAppend` and nothing downstream of
						// this return can roll it back.
						this._coordinates.settleResidentCoordinateSnapshot(
							nativeCoordinateRollback,
						);
						await this.completeNativeStrictDurableTransaction(
							nativeStrictTransaction,
						);
						return undefined;
					}
					const coordinateFields =
						this.createCoordinateFieldsFromNativePlanFacts({
							appendFacts: prepared.appendFacts,
							plan: backboneAppend.coordinate,
						});
					if (!coordinateFields) {
						throw new Error(
							"Native backbone append transaction returned mismatched coordinate facts",
						);
					}
					let preparedCoordinate: PreparedCoordinatePersistence<R> | undefined;
					const getPreparedCoordinate = (): PreparedCoordinatePersistence<R> =>
						(preparedCoordinate ??= {
							assignedToRangeBoundary: coordinateFields.assignedToRangeBoundary,
							fields: coordinateFields,
						});
					const plannedCoordinateDeleteHashes = combineCoordinateDeleteHashes(
						prepared.appendFacts.next,
						prepared.removedHashes ??
							prepared.removed.map((entry) => entry.hash),
					);
					const rollbackCoordinateEntries =
						nativeCoordinateRollback ??
						this._coordinates.snapshotResidentCoordinateEntries([
							prepared.appendFacts.hash,
							...plannedCoordinateDeleteHashes,
						]);
					const finish = (): PreparedPayloadCommitOnlyResult<T, R> => {
						const appendCommit = this.createPreparedLocalAppendCommitFromFacts(
							prepared.appendFacts,
							{
								hashNumber: backboneAppend!.coordinate
									.hashNumber as NumberFromType<R>,
								coordinateFields,
							},
						);
						if (nativeBackboneDocumentIndexCommitted) {
							appendCommit.nativeBackboneDocumentIndexCommitted = true;
							appendCommit.nativeBackboneDocumentIndexTrimmedHeadsProcessed =
								backboneAppend!.documentTrimmedHeadsProcessed;
						}
						if (nativeBackboneDocumentDeleteCommitted) {
							appendCommit.nativeBackboneDocumentDeleteCommitted = true;
							appendCommit.nativeBackboneDocumentIndexCommitted = true;
						}
						appendCommit.documentPreviousContext =
							prepared.documentPreviousContext;
						return {
							get entry() {
								return prepared.entry;
							},
							removed: prepared.removed,
							removedHashes: prepared.removedHashes,
							removedGids: prepared.removedGids,
							appendCommit,
						};
					};
					const coordinateIndex = this
						.entryCoordinatesIndex as PutAndDeleteIndex<EntryReplicated<R>>;
					const rollback = async (error: unknown): Promise<never> => {
						const rollbackFailures: unknown[] = [];
						try {
							await this.markNativeStrictDurableTransactionRollback(
								nativeStrictTransaction,
							);
						} catch (rollbackError) {
							const retentionFailures =
								this.retainNativeStrictDurableTransactionAfterMarkerFailure(
									nativeStrictTransaction,
									prepared.nativeCommittedAppendFinalizer,
									rollbackError,
								);
							throw new AggregateError(
								[error, ...retentionFailures],
								"Native rollback marker could not be persisted; recovery is required",
							);
						}
						try {
							await prepared.nativeCommittedAppendFinalizer?.rollback();
						} catch (rollbackError) {
							rollbackFailures.push(rollbackError);
						}
						try {
							await this._coordinates.rollbackNativeBackboneCoordinateAppendDurably(
								prepared.appendFacts.hash,
								rollbackCoordinateEntries,
							);
							// Terminal: last rollback consumer for the token.
							this._coordinates.settleResidentCoordinateSnapshot(
								rollbackCoordinateEntries,
							);
						} catch (rollbackError) {
							rollbackFailures.push(rollbackError);
						}
						if (nativeDocumentRollback) {
							try {
								this.restoreNativeBackboneDocument(nativeDocumentRollback);
								const flushed =
									this._coordinates.flushNativeBackboneCoordinateJournal();
								if (isPromiseLike(flushed)) {
									await flushed;
								}
							} catch (rollbackError) {
								rollbackFailures.push(rollbackError);
							}
						}
						if (rollbackFailures.length > 0) {
							this.releaseNativeStrictDurableTransaction(
								nativeStrictTransaction,
							);
							throw new AggregateError(
								[error, ...rollbackFailures],
								"Shared-log append and compensation both failed",
							);
						}
						await this.completeNativeStrictDurableTransaction(
							nativeStrictTransaction,
						);
						throw error;
					};
					try {
						await this.setNativeStrictDurableTransactionExpectedRows(
							nativeStrictTransaction,
							[prepared.shallowEntry],
						);
						const hasNativeCoordinatePut =
							this._coordinates.canUseBackboneOnlyCoordinatePersistence() ||
							coordinateIndex.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn ||
							coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn;
						const persisted = hasNativeCoordinatePut
							? this._coordinates.persistBackboneCoordinateFieldsNativeTransaction(
									{
										coordinateIndex,
										fields: coordinateFields,
										hash: prepared.appendFacts.hash,
										deleteHashes: [],
										coordinates: backboneAppend.coordinate
											.coordinates as NumberFromType<R>[],
										skipGenericTransientCoordinateIndex: runtimeOnlyCoordinates,
									},
								)
							: this._coordinates.persistPreparedCoordinate({
									prepared: getPreparedCoordinate(),
									hash: prepared.appendFacts.hash,
									nextHashes: [],
									deleteHashes: [],
									coordinates: backboneAppend.coordinate
										.coordinates as NumberFromType<R>[],
									replicas: backboneAppend.coordinate.coordinates.length,
									commitNative: true,
									commitNativeBackbone: false,
								});
						if (isPromiseLike(persisted)) {
							await persisted;
						}
						if (!prepared.nativeCommittedAppendFinalizer) {
							throw new Error("Missing deferred native append finalizer");
						}
						await this._coordinates.flushNativeBackboneCoordinateJournal();
						await prepared.nativeCommittedAppendFinalizer.acknowledge(() =>
							this.markNativeStrictDurableTransactionLowerMarker(
								nativeStrictTransaction,
							),
						);
					} catch (error) {
						return rollback(error);
					}
					// Success seam: the finalizer acknowledge above is the last
					// await inside the protected try, so `rollback` can no
					// longer fire and the retire below only warns.
					if (properties?.localCommitEvidence) {
						properties.localCommitEvidence.committedHashes.add(
							prepared.appendFacts.hash,
						);
					}
					await this.finishCommittedNativeStrictDurableTransaction(
						nativeStrictTransaction,
						() => {
							this._coordinates.settleResidentCoordinateSnapshot(
								rollbackCoordinateEntries,
							);
							this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
								prepared.appendFacts,
								prepared.removed,
								prepared.materializeEntry,
								{
									forgetNativeCoordinates: false,
									removedHashes: prepared.removedHashes,
									removedGids: prepared.removedGids,
								},
							);
						},
					);
					if (
						commitBlocksInBackbone &&
						!runtimeOnlyCoordinates &&
						this.remoteBlocks.hasNotifyStoredHook()
					) {
						this.remoteBlocks.notifyStoredDeferred(prepared.appendFacts.hash);
					}
					const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
					if (!backboneAppend.isLeader && !delayAdaptiveRebalance) {
						const leaders = backboneAppend.leaders;
						if (leaders) {
							const pruneEntry =
								this._coordinates.materializePreparedCoordinateEntry(
									getPreparedCoordinate(),
								);
							this.pruneDebouncedFnAddIfNotKeeping({
								key: pruneEntry.hash,
								value: { entry: pruneEntry, leaders },
							});
						}
					}
					if (!delayAdaptiveRebalance) {
						this.rebalanceParticipationDebounced?.call();
					}
					return finish();
				});
			},
		);
	}

	private finishPreparedPayloadCommitOnlyAppend(
		result:
			| {
					entry: Entry<T>;
					materializeEntry: () => Entry<T>;
					removed: ShallowOrFullEntry<T>[];
					removedHashes?: string[];
					removedGids?: string[];
					appendFacts: PreparedAppendFacts;
			  }
			| undefined,
		options: SharedAppendOptions<T> | undefined,
		minReplicasValue: number,
		ownershipLifecycleController: AbortController,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (!result) {
			return undefined;
		}

		if (this.shouldDeferHeadCoordinatePersistence(options)) {
			const deferredCoordinateDeleteHashes =
				this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
					result.appendFacts,
					result.removed,
					result.materializeEntry,
					{
						removedHashes: result.removedHashes,
						removedGids: result.removedGids,
						ownershipLifecycleController,
					},
				);
			const deleteHashes =
				deferredCoordinateDeleteHashes &&
				deferredCoordinateDeleteHashes.length > 0
					? [
							...new Set([
								...result.appendFacts.next,
								...deferredCoordinateDeleteHashes,
							]),
						]
					: result.appendFacts.next;
			if (deleteHashes.length > 0) {
				return mapMaybePromise(
					this._coordinates.deleteCoordinatesForHashes(
						deleteHashes,
						ownershipLifecycleController,
					),
					() => {
						this.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
						return {
							get entry() {
								return result.entry;
							},
							removed: result.removed,
							removedHashes: result.removedHashes,
							removedGids: result.removedGids,
							appendCommit: this.createPreparedLocalAppendCommitFromFacts(
								result.appendFacts,
							),
						};
					},
				);
			}
			return {
				get entry() {
					return result.entry;
				},
				removed: result.removed,
				removedHashes: result.removedHashes,
				removedGids: result.removedGids,
				appendCommit: this.createPreparedLocalAppendCommitFromFacts(
					result.appendFacts,
				),
			};
		}

		const nativeTransaction = this.finishPreparedPayloadNativeAppendTransaction(
			result,
			options,
			minReplicasValue,
			ownershipLifecycleController,
		);
		if (nativeTransaction) {
			return nativeTransaction;
		}

		return this.finishPreparedPayloadCommitOnlyAppendAsync(
			result,
			options,
			minReplicasValue,
			ownershipLifecycleController,
		);
	}

	private finishPreparedPayloadNativeAppendTransaction(
		result: {
			entry: Entry<T>;
			materializeEntry: () => Entry<T>;
			removed: ShallowOrFullEntry<T>[];
			removedHashes?: string[];
			removedGids?: string[];
			appendFacts: PreparedAppendFacts;
		},
		options: SharedAppendOptions<T> | undefined,
		minReplicasValue: number,
		ownershipLifecycleController: AbortController,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const coordinateIndex = this.getNativeTransactionCoordinateIndex(
			result,
			options,
		);
		if (!coordinateIndex) {
			return undefined;
		}
		return this.finishPreparedPayloadNativeAppendTransactionAsync(
			result,
			minReplicasValue,
			coordinateIndex,
			ownershipLifecycleController,
		);
	}

	private getNativeTransactionCoordinateIndex(
		result: { appendFacts: PreparedAppendFacts },
		options: SharedAppendOptions<T> | undefined,
	): PutAndDeleteIndex<EntryReplicated<R>> | undefined {
		if (
			options?.target !== "none" ||
			options?.replicate === true ||
			this.shouldDeferHeadCoordinatePersistence(options) ||
			(!this._nativeSharedLogState &&
				!this._coordinates.canUseNativeBackboneResidentCoordinateState()) ||
			!this.canPlanNativeAppendFacts(result.appendFacts)
		) {
			return undefined;
		}
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		return coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn
			? coordinateIndex
			: undefined;
	}

	private async finishPreparedPayloadNativeAppendTransactionAsync(
		result: {
			entry: Entry<T>;
			materializeEntry: () => Entry<T>;
			removed: ShallowOrFullEntry<T>[];
			removedHashes?: string[];
			removedGids?: string[];
			appendFacts: PreparedAppendFacts;
		},
		minReplicasValue: number,
		coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>,
		ownershipLifecycleController: AbortController,
	): Promise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		const nativePreparedCommit =
			await this.processNativePreparedTargetNoneAppendTransaction(result, {
				minReplicasValue,
				coordinateIndex,
				ownershipLifecycleController,
			});
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (!nativePreparedCommit) {
			return undefined;
		}
		const sharedLog = this;
		return {
			get entry() {
				return sharedLog.materializePreparedAppendResultEntry(result);
			},
			removed: result.removed,
			removedHashes: result.removedHashes,
			removedGids: result.removedGids,
			appendCommit: nativePreparedCommit,
		};
	}

	private async processNativePreparedTargetNoneAppendTransaction(
		result: {
			entry?: Entry<T>;
			materializeEntry?: () => Entry<T>;
			removed: ShallowOrFullEntry<T>[];
			removedHashes?: string[];
			removedGids?: string[];
			change?: Change<T>;
			appendFacts: PreparedAppendFacts;
		},
		properties: {
			minReplicasValue: number;
			coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>;
			ownershipLifecycleController: AbortController;
		},
	): Promise<PreparedLocalAppendCommit<R> | undefined> {
		const ownershipLifecycleController =
			properties.ownershipLifecycleController;
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const plannedCoordinateDeleteHashes =
			result.change?.removed.map((entry) => entry.hash) ??
			result.removed.map((entry) => entry.hash);
		const nativeAppendPlan = await this.planNativeLocalAppendFacts(
			result.appendFacts,
			properties.minReplicasValue,
			{
				deleteHashes:
					plannedCoordinateDeleteHashes.length > 0
						? plannedCoordinateDeleteHashes
						: undefined,
			},
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (!nativeAppendPlan) {
			return undefined;
		}

		let deferredCoordinateDeleteHashes: string[] | undefined;
		try {
			deferredCoordinateDeleteHashes = result.change
				? this.applyChangeWithDeferredCoordinateDeletes(result.change, {
						forgetNativeCoordinates:
							!nativeAppendPlan.committedNativeCoordinateDeletes,
						ownershipLifecycleController,
					})
				: this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
						result.appendFacts,
						result.removed,
						() => this.materializePreparedAppendResultEntry(result),
						{
							forgetNativeCoordinates:
								!nativeAppendPlan.committedNativeCoordinateDeletes,
							removedHashes: result.removedHashes,
							removedGids: result.removedGids,
							ownershipLifecycleController,
						},
					);
			await this._coordinates.persistPreparedCoordinateNativeTransaction(
				{
					coordinateIndex: properties.coordinateIndex,
					prepared: nativeAppendPlan.preparedCoordinate,
					hash: result.appendFacts.hash,
					nextHashes: result.appendFacts.next,
					deleteHashes: deferredCoordinateDeleteHashes,
					coordinates: nativeAppendPlan.coordinates,
					commitNative:
						nativeAppendPlan.committedNativeCoordinateState !== true,
					commitNativeBackbone:
						nativeAppendPlan.committedNativeBackboneCoordinateState !== true,
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		} catch (error) {
			if (deferredCoordinateDeleteHashes) {
				await this._coordinates.deleteCoordinatesForHashes(
					deferredCoordinateDeleteHashes,
					ownershipLifecycleController,
				);
			}
			throw error;
		}

		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		if (!nativeAppendPlan.isLeader && !delayAdaptiveRebalance) {
			let leaders = nativeAppendPlan.leaders;
			let pruneEntry: EntryReplicated<R> | undefined;
			if (!leaders) {
				pruneEntry = this._coordinates.materializePreparedCoordinateEntry(
					nativeAppendPlan.preparedCoordinate,
				);
				leaders = (
					await this.planEntryLeaders(
						pruneEntry,
						properties.minReplicasValue,
						{
							persist: false,
						},
						ownershipLifecycleController,
					)
				).leaders;
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
			pruneEntry ??= this._coordinates.materializePreparedCoordinateEntry(
				nativeAppendPlan.preparedCoordinate,
			);
			await this.pruneDebouncedFnAddIfNotKeeping(
				{
					key: pruneEntry.hash,
					value: { entry: pruneEntry, leaders },
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		if (!delayAdaptiveRebalance) {
			this.rebalanceParticipationDebounced?.call();
		}
		return this.createPreparedLocalAppendCommitFromFacts(
			result.appendFacts,
			nativeAppendPlan,
		);
	}

	private async finishPreparedPayloadCommitOnlyAppendAsync(
		result: {
			entry: Entry<T>;
			materializeEntry: () => Entry<T>;
			removed: ShallowOrFullEntry<T>[];
			removedHashes?: string[];
			removedGids?: string[];
			appendFacts: PreparedAppendFacts;
		},
		options: SharedAppendOptions<T> | undefined,
		minReplicasValue: number,
		ownershipLifecycleController: AbortController,
	): Promise<PreparedPayloadCommitOnlyResult<T, R>> {
		const nativePreparedCommit =
			await this.processNativePreparedTargetNoneAppend(result, options, {
				minReplicasValue,
				ownershipLifecycleController,
			});
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (nativePreparedCommit) {
			return {
				get entry() {
					return result.entry;
				},
				removed: result.removed,
				removedHashes: result.removedHashes,
				removedGids: result.removedGids,
				appendCommit: nativePreparedCommit,
			};
		}

		let nativeAppendPlan: NativeAppendEntryPlan<R> | undefined;
		let deferredCoordinateDeleteHashes: string[] | undefined;
		if (this.canCoalescePreparedAppendCoordinateDeletes(result, options)) {
			deferredCoordinateDeleteHashes =
				this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
					result.appendFacts,
					result.removed,
					result.materializeEntry,
					{
						removedHashes: result.removedHashes,
						removedGids: result.removedGids,
						ownershipLifecycleController,
					},
				);
			nativeAppendPlan = await this.planNativeLocalAppendFacts(
				result.appendFacts,
				minReplicasValue,
				undefined,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (!nativeAppendPlan) {
				if (deferredCoordinateDeleteHashes) {
					await this._coordinates.deleteCoordinatesForHashes(
						deferredCoordinateDeleteHashes,
						ownershipLifecycleController,
					);
				}
				deferredCoordinateDeleteHashes = undefined;
			}
		} else {
			this.onEntryAddedHash(result.appendFacts.hash, result.materializeEntry);
			if (result.removed.length > 0) {
				await this.applyRemovedChange(
					result.removed,
					ownershipLifecycleController,
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
		}
		const entry = result.entry;
		try {
			nativeAppendPlan =
				(await this.processLocalAppend(entry, result.removed, options, {
					minReplicasValue,
					appendFacts: result.appendFacts,
					nativeAppendPlan,
					extraCoordinateDeleteHashes: deferredCoordinateDeleteHashes,
					ownershipLifecycleController,
				})) ?? nativeAppendPlan;
		} catch (error) {
			if (deferredCoordinateDeleteHashes) {
				await this._coordinates.deleteCoordinatesForHashes(
					deferredCoordinateDeleteHashes,
					ownershipLifecycleController,
				);
			}
			throw error;
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return {
			get entry() {
				return result.entry;
			},
			removed: result.removed,
			removedHashes: result.removedHashes,
			removedGids: result.removedGids,
			appendCommit: this.createPreparedLocalAppendCommitFromFacts(
				result.appendFacts,
				nativeAppendPlan,
			),
		};
	}

	private canCoalescePreparedAppendCoordinateDeletes(
		result: { removed: ShallowOrFullEntry<T>[] },
		options?: SharedAppendOptions<T>,
	): boolean {
		return (
			result.removed.length > 0 &&
			options?.target === "none" &&
			options?.replicate !== true &&
			!this.shouldDeferHeadCoordinatePersistence(options) &&
			(!!this._nativeSharedLogState || !!this._nativeBackbone)
		);
	}

	private async appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch(
		data: T[],
		appendOptions: AppendOptions<T>,
		options: SharedAppendOptions<T> | undefined,
		properties: PreparedPayloadsManyIndependentProperties<T> | undefined,
		minReplicasValue: number,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<
		| {
				entries: Entry<T>[];
				materializeEntries?: Array<() => Entry<T>>;
				removed: ShallowOrFullEntry<T>[];
				appendCommits: PreparedLocalAppendCommit<R>[];
		  }
		| undefined
	> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const backbone = this._nativeBackbone;
		const payloadDatas = properties?.payloadDatas;
		const documentIndexes = properties?.nativeBackboneDocumentIndexes;
		if (
			!backbone ||
			!payloadDatas ||
			!documentIndexes ||
			payloadDatas.length !== data.length ||
			documentIndexes.length !== data.length ||
			options?.target !== "none" ||
			options?.replicate === true ||
			(options?.delivery !== undefined && options.delivery !== false) ||
			!this._coordinates.canUseNativeBackboneResidentCoordinateState() ||
			properties?.nexts?.some((nexts) => nexts.length > 0)
		) {
			return undefined;
		}
		// When the durable write-through wrapper is active the log's block store is
		// the wrapper, not the raw wasm map, so `localStore === backbone.blocks` is
		// false. This batch path always commits blocks in the backbone (the
		// committed native batch prepare variants), so it is safe with the wrapper:
		// the blocks land in wasm and are mirrored to durable per-entry below. This
		// preserves the resident-coordinate fast batch path (and its meta.next
		// linking) after a reopen instead of bailing to a slow generic path.
		const durableWrapperActive =
			this.remoteBlocks?.localStore !== backbone.blocks;
		const durableWrapper = durableWrapperActive
			? (this.remoteBlocks?.localStore as unknown as {
					beginNativeDeleteCleanup?: (cids: string[]) => number | undefined;
					cancelNativeDeleteCleanup?: (cleanupToken: unknown) => void;
					mirrorToDurable?: (
						cid: string,
						bytes: Uint8Array,
						options?: { nativeTrimmed?: boolean },
					) => Promise<unknown>;
					mirrorManyToDurable?: (
						blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
						options?: { nativeTrimmedCids?: ReadonlySet<string> },
					) => Promise<unknown>;
					rollbackFailedNativeCommits?: (
						cids: string[],
						restoreNativeCids?: string[],
					) => Promise<void>;
				})
			: undefined;
		const usesLatestDocumentContext = documentIndexes.every(
			(index) => index.useLatestContext === true,
		);
		if (
			!usesLatestDocumentContext &&
			documentIndexes.some((index) => index.useLatestContext === true)
		) {
			return undefined;
		}
		if (
			documentIndexes.some(
				(index) => !index.valuePrefixBytes && !index.projection,
			)
		) {
			return undefined;
		}
		const firstIndex = documentIndexes[0];
		if (!firstIndex) {
			return undefined;
		}
		const byteElementIndexLimit = firstIndex.byteElementIndexLimit ?? 0;
		const deleteTrimmedHeads = firstIndex.deleteTrimmedHeads === true;
		if (
			documentIndexes.some(
				(index) =>
					(index.byteElementIndexLimit ?? 0) !== byteElementIndexLimit ||
					(index.deleteTrimmedHeads === true) !== deleteTrimmedHeads,
			)
		) {
			return undefined;
		}
		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativeLeaderOptions = this.createNativeLeaderOptions(context);
		let backboneAppends: NativeBackboneAppendResult[] | undefined;
		let batchDocumentRollbacks: NativeBackboneDocumentRollback[] = [];
		let batchCoordinateRollback:
			| NativeBackboneCoordinateRollback<R>
			| undefined;
		let nativeDeleteCleanupToken: unknown;
		let nativeStrictTransaction:
			| NativeStrictDurableTransactionHandle
			| undefined;
		let appended: TrustedLowerLogCommitOnlyAppendBatchResult<T> | undefined;
		try {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			appended = await asTrustedLowerLog(
				this.log,
			).appendLocallyPreparedNativeKnownNoNextCommitOnlyBatch(
				data,
				appendOptions,
				{
					payloadDatas,
					resolveTrimmedEntries: properties?.resolveTrimmedEntries,
					allowPreparedNexts: usesLatestDocumentContext,
					retainMaterializationBytes:
						properties?.retainMaterializationBytes === true ||
						this._logProperties?.trim != null,
					deferNativeTransactionAcknowledgement: true,
				},
				async (inputs) => {
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					batchDocumentRollbacks = documentIndexes
						.map((index) => this.snapshotNativeBackboneDocument(index))
						.filter(
							(value): value is NativeBackboneDocumentRollback => !!value,
						);
					nativeStrictTransaction =
						await this.beginNativeStrictDurableTransaction(
							batchDocumentRollbacks,
						);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					const documentDeleteTrimmedHeadsForAppend =
						deleteTrimmedHeads && inputs[0]?.trimLengthTo != null;
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					backboneAppends = usesLatestDocumentContext
						? backbone.preparePlainCommittedStorageAppendDocumentIndexLatestBatchTransaction(
								{
									entries: inputs.map((input, index) => ({
										wallTime: input.wallTime,
										logical: input.logical,
										gid: input.gid,
										type: input.type,
										metaData: input.metaData,
										payloadData: input.payloadData,
										documentIndex: documentIndexes[index]!,
									})),
									replicas: minReplicasValue,
									roleAgeMs: nativeLeaderOptions.roleAge,
									now: nativeLeaderOptions.now,
									selfHash: nativeLeaderOptions.selfHash,
									selfReplicating: nativeLeaderOptions.selfReplicating,
									resolveTrimmedEntries: properties?.resolveTrimmedEntries,
									documentByteElementIndexLimit: byteElementIndexLimit,
									documentDeleteTrimmedHeads:
										documentDeleteTrimmedHeadsForAppend,
									trimLengthTo: inputs[0]?.trimLengthTo,
								},
							)
						: backbone.preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction(
								{
									entries: inputs.map((input, index) => ({
										wallTime: input.wallTime,
										logical: input.logical,
										gid: input.gid,
										type: input.type,
										metaData: input.metaData,
										payloadData: input.payloadData,
										documentIndex: documentIndexes[index]!,
									})),
									replicas: minReplicasValue,
									roleAgeMs: nativeLeaderOptions.roleAge,
									now: nativeLeaderOptions.now,
									selfHash: nativeLeaderOptions.selfHash,
									selfReplicating: nativeLeaderOptions.selfReplicating,
									documentByteElementIndexLimit: byteElementIndexLimit,
									documentDeleteTrimmedHeads:
										documentDeleteTrimmedHeadsForAppend,
									trimLengthTo: inputs[0]?.trimLengthTo,
								},
							);
					if (!backboneAppends) {
						await this.completeNativeStrictDurableTransaction(
							nativeStrictTransaction,
						);
						this.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
						return undefined;
					}
					const committedAppends = backboneAppends;
					const committedCids = committedAppends
						.map((append) => append.entry.cid ?? append.entry.hash)
						.filter((cid): cid is string => !!cid);
					const nativeTrimmedHashSet = new Set(
						committedAppends.flatMap(
							(append) =>
								append.trimmedHashes ??
								append.trimmed.map((entry) => entry.hash),
						),
					);
					const nativeTrimmedHashes = [...nativeTrimmedHashSet];
					batchCoordinateRollback =
						this._coordinates.snapshotResidentCoordinateEntries(
							committedAppends.flatMap((append) => [
								...((append.entry.cid ?? append.entry.hash)
									? [append.entry.cid ?? append.entry.hash!]
									: []),
								...append.entry.next,
								...(append.trimmedHashes ??
									append.trimmed.map((entry) => entry.hash)),
							]),
						);
					await this.setNativeStrictDurableTransactionOperation(
						nativeStrictTransaction,
						committedCids,
						nativeTrimmedHashes,
						batchCoordinateRollback,
						committedAppends.flatMap((append) => [
							...append.entry.next,
							...(append.trimmedHashes ??
								append.trimmed.map((entry) => entry.hash)),
						]),
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					const rollbackCommitted = async (cause: unknown): Promise<never> => {
						durableWrapper?.cancelNativeDeleteCleanup?.(
							nativeDeleteCleanupToken,
						);
						let compensated = false;
						try {
							await this.rollbackFailedNativeBackboneTransaction({
								committedHashes: committedCids,
								trimmedEntries: committedAppends.flatMap(
									(append) => append.trimmed,
								),
								coordinateEntries: batchCoordinateRollback,
								documents: batchDocumentRollbacks,
								durableWrapper,
							});
							compensated = true;
						} catch {
							// close/reopen completes recovery if durable compensation failed
						}
						if (compensated) {
							await this.completeNativeStrictDurableTransaction(
								nativeStrictTransaction,
							);
						} else {
							this.releaseNativeStrictDurableTransaction(
								nativeStrictTransaction,
							);
						}
						return this.failNativeDurableCommit(cause, {
							committedCids,
							failedCids: committedCids,
						});
					};
					if (
						durableWrapper &&
						nativeTrimmedHashes.length > 0 &&
						!durableWrapper.beginNativeDeleteCleanup
					) {
						return rollbackCommitted(
							new Error(
								"Native durable block wrapper cannot preannounce trim cleanup",
							),
						);
					}
					nativeDeleteCleanupToken =
						durableWrapper?.beginNativeDeleteCleanup?.(nativeTrimmedHashes);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					const preparedRows = committedAppends.map((append) => ({
						cid: append.entry.hash,
						hash: append.entry.hash,
						gid: append.coordinate.gid,
						next: append.entry.next,
						bytes: append.entry.bytes,
						byteLength: append.entry.byteLength,
						metaBytes: append.entry.metaBytes,
						hashDigestBytes: append.entry.hashDigestBytes,
						getBytes: (hash: string) => backbone.blocks.get(hash),
						nativeIndexMutationLockOwner:
							nativeStrictTransaction?.lowerHashMutationLockOwner,
						trimmedEntryHashes: append.trimmedHashes,
						trimmedEntryGids:
							append.trimmedGids ??
							(append.trimmed.length > 0
								? append.trimmed.map((entry) => entry.gid)
								: undefined),
						nativeBlocksDeleted: true,
						nativeDeleteCleanupToken,
						documentTrimmedHeadsProcessed: append.documentTrimmedHeadsProcessed,
						documentPreviousContext: append.documentPreviousContext,
					}));
					if (!durableWrapper) {
						return preparedRows;
					}
					if (!durableWrapper.mirrorManyToDurable) {
						durableWrapper.cancelNativeDeleteCleanup?.(
							nativeDeleteCleanupToken,
						);
						return rollbackCommitted(
							new Error(
								"Native durable block wrapper has no batch mirror method",
							),
						);
					}
					const durableMirrorBlocks: Array<
						readonly [cid: string, bytes: Uint8Array]
					> = [];
					const missingCommittedCids: string[] = [];
					let missingCommittedHash = false;
					for (const backboneAppend of committedAppends) {
						const committedHash =
							backboneAppend.entry.cid ?? backboneAppend.entry.hash;
						if (!committedHash) {
							missingCommittedHash = true;
							continue;
						}
						// Earlier rows can be trimmed by later rows in this one native batch.
						// The native result retains their bytes even though the final hot map
						// no longer does; mirror those bytes, then let the explicit trim cleanup
						// remove the durable copy.
						const committedBytes =
							backboneAppend.entry.bytes ?? backbone.blocks.get(committedHash);
						if (!committedBytes) {
							missingCommittedCids.push(committedHash);
							continue;
						}
						durableMirrorBlocks.push([committedHash, committedBytes]);
					}
					// One strict putKnownMany WAL mutation gives the whole native batch one
					// durability barrier instead of issuing and fsyncing one record per row.
					const durableMirror =
						durableMirrorBlocks.length > 0
							? durableWrapper.mirrorManyToDurable(durableMirrorBlocks, {
									nativeTrimmedCids: nativeTrimmedHashSet,
								})
							: Promise.resolve();
					return Promise.allSettled([durableMirror]).then(async (settled) => {
						this.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
						const rejected =
							settled[0]?.status === "rejected"
								? (settled[0] as PromiseRejectedResult).reason
								: undefined;
						if (
							missingCommittedHash ||
							missingCommittedCids.length > 0 ||
							rejected !== undefined
						) {
							durableWrapper.cancelNativeDeleteCleanup?.(
								nativeDeleteCleanupToken,
							);
							const cause =
								rejected === undefined
									? new Error(
											missingCommittedHash
												? "Native batch commit returned an entry with no CID to mirror"
												: `Native committed blocks are missing from the hot store: ${missingCommittedCids.join(", ")}`,
										)
									: rejected;
							const rejectedCids =
								cause instanceof NativeDurableCommitError
									? cause.failedCids.filter((cid) =>
											committedCids.includes(cid),
										)
									: durableMirrorBlocks.map(([cid]) => cid);
							if (cause instanceof NativeDurableCommitError) {
								cause.addCommitContext({
									committedCids,
									failedCids: [...missingCommittedCids, ...rejectedCids],
								});
							}
							return rollbackCommitted(cause);
						}
						const nativeCommitOwnershipToken =
							settled[0]?.status === "fulfilled" ? settled[0].value : undefined;
						return preparedRows.map((row) => ({
							...row,
							nativeCommitOwnershipToken,
						}));
					});
				},
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		} catch (error) {
			durableWrapper?.cancelNativeDeleteCleanup?.(nativeDeleteCleanupToken);
			let compensated = !backboneAppends;
			if (backboneAppends && !(error instanceof NativeDurableCommitError)) {
				try {
					await this.rollbackFailedNativeBackboneTransaction({
						committedHashes: backboneAppends
							.map((append) => append.entry.cid ?? append.entry.hash)
							.filter((hash): hash is string => !!hash),
						coordinateEntries: batchCoordinateRollback,
						documents: batchDocumentRollbacks,
						skipBlockCompensation: true,
						restoreGraphFromIndex: true,
					});
					compensated = true;
				} catch {
					// Preserve the lower index publication failure.
				}
			}
			if (!(error instanceof NativeDurableCommitError)) {
				if (compensated) {
					await this.completeNativeStrictDurableTransaction(
						nativeStrictTransaction,
					);
				} else {
					this.releaseNativeStrictDurableTransaction(nativeStrictTransaction);
				}
			}
			throw error;
		}
		if (!appended || !backboneAppends) {
			// Abandon arm: no consumer downstream of this return.
			this._coordinates.settleResidentCoordinateSnapshot(
				batchCoordinateRollback,
			);
			await this.completeNativeStrictDurableTransaction(
				nativeStrictTransaction,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return undefined;
		}
		const runtimeOnlyCoordinates = options?.replicate === false;
		const rollbackBatch = async (error: unknown): Promise<never> => {
			const rollbackFailures: unknown[] = [];
			try {
				await this.markNativeStrictDurableTransactionRollback(
					nativeStrictTransaction,
				);
			} catch (rollbackError) {
				const retentionFailures =
					this.retainNativeStrictDurableTransactionAfterMarkerFailure(
						nativeStrictTransaction,
						appended.nativeCommittedAppendFinalizer,
						rollbackError,
					);
				throw new AggregateError(
					[error, ...retentionFailures],
					"Native rollback marker could not be persisted; recovery is required",
				);
			}
			try {
				await appended.nativeCommittedAppendFinalizer?.rollback();
			} catch (rollbackError) {
				rollbackFailures.push(rollbackError);
			}
			try {
				await this._coordinates.rollbackNativeBackboneCoordinateAppendDurably(
					appended.appendFacts[0]?.hash ?? "",
					batchCoordinateRollback,
				);
				// Terminal: last rollback consumer for the batch token.
				this._coordinates.settleResidentCoordinateSnapshot(
					batchCoordinateRollback,
				);
			} catch (rollbackError) {
				rollbackFailures.push(rollbackError);
			}
			try {
				for (const document of batchDocumentRollbacks) {
					this.restoreNativeBackboneDocument(document);
				}
				const flushed =
					this._coordinates.flushNativeBackboneCoordinateJournal();
				if (isPromiseLike(flushed)) {
					await flushed;
				}
			} catch (rollbackError) {
				rollbackFailures.push(rollbackError);
			}
			if (rollbackFailures.length > 0) {
				this.releaseNativeStrictDurableTransaction(nativeStrictTransaction);
				throw new AggregateError(
					[error, ...rollbackFailures],
					"Shared-log append batch and compensation both failed",
				);
			}
			await this.completeNativeStrictDurableTransaction(
				nativeStrictTransaction,
			);
			throw error;
		};
		const coordinateRows: Array<{
			facts: PreparedAppendFacts;
			backboneAppend: NativeBackboneAppendResult;
			coordinateFields: SharedLogCoordinateNativeFields<R>;
			plannedCoordinateDeleteHashes: string[];
		}> = [];
		try {
			const batchExternalNextHashes = new Set(
				appended.appendFacts.flatMap((facts) => facts.next),
			);
			await this.setNativeStrictDurableTransactionExpectedRows(
				nativeStrictTransaction,
				appended.appendFacts.map(
					(facts) =>
						new ShallowEntry({
							hash: facts.hash,
							payloadSize: facts.payloadSize,
							head: !batchExternalNextHashes.has(facts.hash),
							meta: new ShallowMeta({
								gid: facts.gid,
								clock: new LamportClock({
									id: facts.clockId ?? this.node.identity.publicKey.bytes,
									timestamp: new Timestamp({
										wallTime: facts.wallTime,
										logical: facts.logical,
									}),
								}),
								data: facts.metaData,
								next: facts.next,
								type: facts.type ?? EntryType.APPEND,
							}),
						}),
				),
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			for (let i = 0; i < appended.appendFacts.length; i++) {
				const facts = appended.appendFacts[i]!;
				const backboneAppend = backboneAppends[i]!;
				const coordinateFields = this.createCoordinateFieldsFromNativePlanFacts(
					{
						appendFacts: facts,
						plan: backboneAppend.coordinate,
					},
				);
				if (!coordinateFields) {
					throw new Error(
						"Native backbone batch append transaction returned mismatched coordinate facts",
					);
				}
				const plannedCoordinateDeleteHashes = combineCoordinateDeleteHashes(
					facts.next,
					backboneAppend.trimmedHashes ?? [],
				);
				coordinateRows.push({
					facts,
					backboneAppend,
					coordinateFields,
					plannedCoordinateDeleteHashes,
				});
				const persisted =
					this._coordinates.persistBackboneCoordinateFieldsNativeTransaction(
						{
							coordinateIndex: this.entryCoordinatesIndex as PutAndDeleteIndex<
								EntryReplicated<R>
							>,
							fields: coordinateFields,
							hash: facts.hash,
							deleteHashes: [],
							coordinates: backboneAppend.coordinate
								.coordinates as NumberFromType<R>[],
							skipGenericTransientCoordinateIndex: runtimeOnlyCoordinates,
						},
						ownershipLifecycleController,
					);
				if (isPromiseLike(persisted)) {
					await persisted;
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			}
			if (!appended.nativeCommittedAppendFinalizer) {
				throw new Error("Missing deferred native append batch finalizer");
			}
			await this._coordinates.flushNativeBackboneCoordinateJournal();
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			await appended.nativeCommittedAppendFinalizer.acknowledge(() => {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				return this.markNativeStrictDurableTransactionLowerMarker(
					nativeStrictTransaction,
				);
			});
		} catch (error) {
			return rollbackBatch(error);
		}
		// Success seam: `rollbackBatch` has exactly one call site (the catch
		// above), and everything from here on escapes without any rollback.
		if (properties?.localCommitEvidence) {
			for (const facts of appended.appendFacts) {
				properties.localCommitEvidence.committedHashes.add(facts.hash);
			}
		}
		const appendCommits =
			await this.finishCommittedNativeStrictDurableTransaction(
				nativeStrictTransaction,
				() => {
					this._coordinates.settleResidentCoordinateSnapshot(
						batchCoordinateRollback,
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					const commits: PreparedLocalAppendCommit<R>[] = [];
					for (let i = 0; i < coordinateRows.length; i++) {
						const {
							facts,
							backboneAppend,
							coordinateFields,
							plannedCoordinateDeleteHashes,
						} = coordinateRows[i]!;
						this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
							facts,
							[],
							appended.materializeEntries[i]!,
							{
								forgetNativeCoordinates: false,
								removedHashes: plannedCoordinateDeleteHashes,
								removedGids:
									backboneAppend.trimmedGids ??
									(backboneAppend.trimmed.length > 0
										? backboneAppend.trimmed.map((entry) => entry.gid)
										: undefined),
							},
						);
						if (
							!runtimeOnlyCoordinates &&
							this.remoteBlocks.hasNotifyStoredHook()
						) {
							this.remoteBlocks.notifyStoredDeferred(facts.hash);
						}
						const appendCommit = this.createPreparedLocalAppendCommitFromFacts(
							facts,
							{
								hashNumber: backboneAppend.coordinate
									.hashNumber as NumberFromType<R>,
								coordinateFields,
							},
						);
						appendCommit.nativeBackboneDocumentIndexCommitted = true;
						appendCommit.nativeBackboneDocumentIndexTrimmedHeadsProcessed =
							appended.documentTrimmedHeadsProcessed?.[i];
						appendCommit.documentPreviousContext =
							backboneAppend.documentPreviousContext;
						commits.push(appendCommit);
					}
					return commits;
				},
				() => this.isRepairLifecycleActive(ownershipLifecycleController),
			);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		if (!delayAdaptiveRebalance) {
			this.rebalanceParticipationDebounced?.call();
		}
		return {
			get entries() {
				return appended.entries;
			},
			materializeEntries: appended.materializeEntries,
			removed: appended.removed,
			appendCommits,
		};
	}

	private async appendLocallyPreparedManyIndependent(
		data: T[],
		options?: SharedAppendOptions<T> | undefined,
		properties?: PreparedPayloadsManyIndependentProperties<T>,
	): Promise<
		| {
				entries: Entry<T>[];
				materializeEntries?: Array<() => Entry<T>>;
				removed: ShallowOrFullEntry<T>[];
				appendCommits: PreparedLocalAppendCommit<R>[];
		  }
		| undefined
	> {
		this.throwIfNativeDurableCommitFailed();
		this.rejectPersistedDeliveryOnTrustedLocalAppend(options);
		if (data.length === 0) {
			return { entries: [], removed: [], appendCommits: [] };
		}
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
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
		attachTrustedLocalCommitEvidence(
			appendOptions,
			properties?.localCommitEvidence,
		);
		const nativeBackboneBatch =
			await this.appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch(
				data,
				appendOptions,
				options,
				properties,
				minReplicasValue,
				ownershipLifecycleController,
			);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (nativeBackboneBatch) {
			return nativeBackboneBatch;
		}
		const result = await asTrustedLowerLog(
			this.log,
		).appendLocallyPreparedManyIndependent(data, appendOptions, {
			resolveTrimmedEntries: properties?.resolveTrimmedEntries,
			payloadDatas: properties?.payloadDatas,
			nexts: properties?.nexts,
		});
		if (result && properties?.localCommitEvidence) {
			for (const facts of result.appendFacts) {
				properties.localCommitEvidence.committedHashes.add(facts.hash);
			}
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (!result) {
			return undefined;
		}

		const changeResult = this.applyChange(result.change, {
			ownershipLifecycleController,
		});
		if (isPromiseLike(changeResult)) {
			await changeResult;
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		const deferHeadCoordinatePersistence =
			this.shouldDeferHeadCoordinatePersistence(options);

		if (deferHeadCoordinatePersistence) {
			await this._coordinates.deleteCoordinatesForHashes(
				[
					...result.entries.flatMap((entry) => entry.meta.next),
					...result.removed.map((entry) => entry.hash),
				],
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return {
				entries: result.entries,
				removed: result.removed,
				appendCommits: this.createPreparedLocalAppendCommitsFromFacts(
					result.appendFacts,
					result.entries,
				),
			};
		}

		let nativeAppendPlans =
			options?.replicate === true
				? undefined
				: options?.target === "none"
					? await this.planNativeLocalAppendEntries(
							result.entries,
							minReplicasValue,
							ownershipLifecycleController,
						)
					: await this.planNativeAppendEntries(
							result.entries,
							minReplicasValue,
							options?.delivery,
							options,
							ownershipLifecycleController,
						);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (
			nativeAppendPlans &&
			(await this.processLocalAppendManyNativePlanned(result.entries, options, {
				nativeAppendPlans,
				ownershipLifecycleController,
			}))
		) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return {
				entries: result.entries,
				removed: result.removed,
				appendCommits: this.createPreparedLocalAppendCommitsFromFacts(
					result.appendFacts,
					result.entries,
					nativeAppendPlans,
				),
			};
		}
		for (let i = 0; i < result.entries.length; i++) {
			const processedPlan = await this.processLocalAppend(
				result.entries[i]!,
				i === result.entries.length - 1 ? result.removed : [],
				options,
				{
					minReplicasValue,
					deferHeadCoordinatePersistence: false,
					nativeAppendPlan: nativeAppendPlans?.[i],
					ownershipLifecycleController,
				},
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (processedPlan) {
				nativeAppendPlans ??= [];
				nativeAppendPlans[i] = processedPlan;
			}
		}

		return {
			entries: result.entries,
			removed: result.removed,
			appendCommits: this.createPreparedLocalAppendCommitsFromFacts(
				result.appendFacts,
				result.entries,
				nativeAppendPlans,
			),
		};
	}

	private async appendLocallyPreparedPayloadsManyIndependent(
		payloadDatas: Uint8Array[],
		options?: SharedAppendOptions<T> | undefined,
		properties?: Omit<
			PreparedPayloadsManyIndependentProperties<T>,
			"payloadDatas"
		>,
	) {
		return this.appendLocallyPreparedManyIndependent(
			new Array(payloadDatas.length) as T[],
			options,
			{
				resolveTrimmedEntries: properties?.resolveTrimmedEntries,
				payloadDatas,
				nexts: properties?.nexts,
				nativeBackboneDocumentIndexes:
					properties?.nativeBackboneDocumentIndexes,
				retainMaterializationBytes: properties?.retainMaterializationBytes,
				...(properties?.localCommitEvidence
					? { localCommitEvidence: properties.localCommitEvidence }
					: undefined),
			},
		);
	}

	async appendMany(
		data: T[],
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entries: Entry<T>[];
		removed: ShallowOrFullEntry<T>[];
	}> {
		this.throwIfNativeDurableCommitFailed();
		const persistedDelivery = this.capturePersistedAppendInvocation(options);
		if (data.length === 0) {
			return { entries: [], removed: [] };
		}
		if (persistedDelivery) {
			throw new Error(
				"persisted delivery is not supported for chained appendMany; use independent document puts",
			);
		}
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } = this.createLogAppendOptions(
			options,
			ownershipLifecycleController,
		);
		const result = await this.log.appendMany(data, appendOptions);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const deferHeadCoordinatePersistence =
			this.shouldDeferHeadCoordinatePersistence(options);

		if (deferHeadCoordinatePersistence) {
			await this._coordinates.deleteCoordinatesForHashes(
				[
					...result.entries.flatMap((entry) => entry.meta.next),
					...result.removed.map((entry) => entry.hash),
				],
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return result;
		}

		if (this.canCoalesceLocalAppendMany(result.entries, options)) {
			await this.processLocalAppendManyCoalesced(result, options, {
				minReplicasValue,
				ownershipLifecycleController,
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return result;
		}

		const nativeAppendPlans =
			options?.replicate === true
				? undefined
				: options?.target === "none"
					? await this.planNativeLocalAppendEntries(
							result.entries,
							minReplicasValue,
							ownershipLifecycleController,
						)
					: await this.planNativeAppendEntries(
							result.entries,
							minReplicasValue,
							options?.delivery,
							options,
							ownershipLifecycleController,
						);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		for (let i = 0; i < result.entries.length; i++) {
			const entry = result.entries[i]!;
			await this.processLocalAppend(entry, [], options, {
				minReplicasValue,
				deferHeadCoordinatePersistence: false,
				nativeAppendPlan: nativeAppendPlans?.[i],
				ownershipLifecycleController,
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		return result;
	}

	/**
	 * Deliver entries that were already committed locally, then wait for the
	 * requested persisted remote quorum. This is the post-commit seam used by
	 * higher-level transactional/batched writers so a receipt timeout never
	 * rolls back or hides their successful local commit.
	 */
	private async deliverPersistedPlanningEntries(
		resolveRecords: () => PersistedDeliveryPlanningRecord<T, R>[],
		committedHashesInput: readonly string[],
		options: SharedAppendOptions<T>,
	): Promise<void> {
		const committedHashes = Object.freeze([...committedHashesInput]);
		let persistedDeadline: PersistedDeliveryDeadline | undefined;
		try {
			const persistedInvocation =
				this.capturePersistedAppendInvocation(options);
			if (!persistedInvocation) {
				throw new Error(
					'deliverPersistedEntries requires reliability="persisted"',
				);
			}
			options = persistedInvocation.options;
			const delivery = persistedInvocation.delivery;
			const ownershipLifecycleController =
				this.captureReplicationOwnershipLifecycle();
			const deadline = this.createPersistedDeliveryDeadline(
				delivery,
				ownershipLifecycleController,
				committedHashes.length,
			);
			persistedDeadline = deadline;
			const throwIfDeliveryAborted = () => {
				if (deadline.signal.aborted) {
					throw deadline.signal.reason ?? new AbortError();
				}
				if (Date.now() >= deadline.deadline) {
					throw new TimeoutError(
						`Timed out waiting for ${Math.floor(delivery.minAcks!)} persisted remote replicas.`,
					);
				}
			};
			const records = resolveRecords();
			throwIfDeliveryAborted();
			if (records.length !== committedHashes.length) {
				throw new Error("Persisted delivery planning evidence count mismatch");
			}
			for (let index = 0; index < records.length; index++) {
				if (records[index]!.canonicalHash !== committedHashes[index]) {
					throw new Error(
						`Persisted delivery planning evidence did not match committed hash ${committedHashes[index]}`,
					);
				}
			}
			if (records.length === 0) return;
			const { minReplicasValue } = this.createLogAppendOptions(
				options,
				ownershipLifecycleController,
			);
			throwIfDeliveryAborted();
			await this.settlePersistedDelivery(
				records,
				minReplicasValue,
				delivery,
				ownershipLifecycleController,
				deadline,
				true,
			);
		} catch (error) {
			throw new PersistedDeliveryError(error, committedHashes);
		} finally {
			persistedDeadline?.dispose();
		}
	}

	private createPersistedDeliveryPlanningRecords(
		appendCommits: PreparedLocalAppendCommit<R>[],
		materializeEntries: () => Entry<T>[],
	): PersistedDeliveryPlanningRecord<T, R>[] {
		let materializedEntries: Entry<T>[] | undefined;
		const requiresFullEntries =
			this.findLeadersFromEntry !== SharedLog.prototype.findLeadersFromEntry;
		const getCommittedEntry = (
			appendCommit: PreparedLocalAppendCommit<R>,
			index: number,
		) => {
			materializedEntries ??= materializeEntries();
			const entry = materializedEntries[index];
			const materializedHash = entry?.hash;
			if (!entry || materializedHash !== appendCommit.hash) {
				throw new Error(
					`Persisted delivery materializer did not return committed entry ${appendCommit.hash}`,
				);
			}
			return entry;
		};
		return appendCommits.map((appendCommit, index) => {
			let capturedCommit = this.snapshotPreparedLocalAppendCommit(appendCommit);
			if (requiresFullEntries && !capturedCommit.storageBytes) {
				capturedCommit = this.capturePersistedLocalAppendCommit(
					capturedCommit.hash,
					getCommittedEntry(capturedCommit, index),
				);
			}
			return this.createPersistedDeliveryPlanningRecord(capturedCommit);
		});
	}

	private deliverPersistedAppendCommits(
		appendCommits: PreparedLocalAppendCommit<R>[],
		materializeEntries: () => Entry<T>[],
		options: SharedAppendOptions<T>,
	): Promise<void> {
		return this.deliverPersistedPlanningEntries(
			() =>
				this.createPersistedDeliveryPlanningRecords(
					appendCommits,
					materializeEntries,
				),
			appendCommits.map((appendCommit) => appendCommit.hash),
			options,
		);
	}

	async deliverPersistedEntries(
		entries: Entry<T>[],
		options: SharedAppendOptions<T>,
	): Promise<void> {
		const records = entries.map((entry) =>
			this.snapshotPersistedDeliveryPlanningEntry(entry),
		);
		return this.deliverPersistedPlanningEntries(
			() => records,
			records.map((record) => record.canonicalHash),
			options,
		);
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
			ownershipLifecycleController: AbortController;
		},
	): Promise<void> {
		const ownershipLifecycleController =
			properties.ownershipLifecycleController;
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const head = result.entries[result.entries.length - 1]!;
		await this._coordinates.deleteCoordinatesForHashes(
			[
				...result.entries[0]!.meta.next,
				...result.entries.slice(0, -1).map((entry) => entry.hash),
				...result.removed.map((entry) => entry.hash),
			],
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		await this.processLocalAppend(head, result.removed, options, {
			minReplicasValue: properties.minReplicasValue,
			deferHeadCoordinatePersistence: false,
			ownershipLifecycleController,
		});
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
	}

	private async processLocalAppendManyNativePlanned(
		entries: Entry<T>[],
		options: SharedAppendOptions<T> | undefined,
		properties: {
			nativeAppendPlans: NativeAppendEntryPlan<R>[];
			ownershipLifecycleController: AbortController;
		},
	): Promise<boolean> {
		const ownershipLifecycleController =
			properties.ownershipLifecycleController;
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (
			entries.length === 0 ||
			options?.target !== "none" ||
			options?.replicate === true ||
			properties.nativeAppendPlans.length !== entries.length
		) {
			return false;
		}

		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		await this._coordinates.persistCoordinatesBatch(
			entries.map((entry, index) => {
				const plan = properties.nativeAppendPlans[index]!;
				return {
					leaders: plan.leaders!,
					coordinates: plan.coordinates,
					replicas: plan.coordinates.length,
					entry,
					assignedToRangeBoundary: plan.assignedToRangeBoundary,
					commitNative: plan.committedNativeCoordinateState !== true,
					commitNativeBackbone:
						plan.committedNativeBackboneCoordinateState !== true,
					hashNumber: plan.hashNumber,
					prepared: plan.preparedCoordinate,
				};
			}),
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);

		if (!delayAdaptiveRebalance) {
			for (let i = 0; i < entries.length; i++) {
				const plan = properties.nativeAppendPlans[i]!;
				if (!plan.isLeader) {
					await this.pruneDebouncedFnAddIfNotKeeping(
						{
							key: entries[i]!.hash,
							value: { entry: entries[i]!, leaders: plan.leaders! },
						},
						ownershipLifecycleController,
					);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			}
			this.rebalanceParticipationDebounced?.call();
		}
		return true;
	}

	private createLogAppendOptions(
		options?: SharedAppendOptions<T>,
		ownershipLifecycleController?: AbortController,
	): {
		appendOptions: TrustedLogAppendOptions<T>;
		minReplicasValue: number;
	} {
		const appendOptions: TrustedLogAppendOptions<T> = { ...options };
		const { minReplicasData, minReplicasValue } =
			this.createAppendReplicaMetadata(options?.replicas);

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

		if (ownershipLifecycleController) {
			appendOptions.onChange = async (change) => {
				await this.onChange(change, ownershipLifecycleController);
				if (options?.onChange) {
					return options.onChange(change);
				}
				return this._logProperties?.onChange?.(change);
			};
		} else if (options?.onChange) {
			appendOptions.onChange = async (change) => {
				await this.onChange(change);
				return options.onChange!(change);
			};
		}

		return { appendOptions, minReplicasValue };
	}

	private createAppendReplicaMetadata(
		replicas: SharedAppendOptions<T>["replicas"] | undefined,
	): { minReplicasData: Uint8Array; minReplicasValue: number } {
		const customValue = replicas
			? typeof replicas === "number"
				? new AbsoluteReplicas(replicas)
				: replicas
			: undefined;
		const minReplicas = this.getClampedReplicas(customValue);
		const minReplicasValue = minReplicas.getValue(this);
		checkMinReplicasLimit(minReplicasValue);
		if (!customValue) {
			const cache = this._defaultAppendReplicaMetadataCache;
			if (cache?.source === minReplicas && cache.value === minReplicasValue) {
				return {
					minReplicasData: cache.bytes,
					minReplicasValue,
				};
			}
			const minReplicasData = encodeReplicas(minReplicas);
			this._defaultAppendReplicaMetadataCache = {
				source: minReplicas,
				value: minReplicasValue,
				bytes: minReplicasData,
			};
			return { minReplicasData, minReplicasValue };
		}
		return {
			minReplicasData: encodeReplicas(minReplicas),
			minReplicasValue,
		};
	}

	private canPlanNativeAppendFacts(appendFacts: PreparedAppendFacts): boolean {
		return this.domain.type === "hash" && typeof appendFacts.gid === "string";
	}

	private getAppendFactsHashNumber(
		appendFacts: PreparedAppendFacts,
	): NumberFromType<R> {
		return this.indexableDomain.numbers.bytesToNumber(
			appendFacts.hashDigestBytes ??
				cidifyString(appendFacts.hash).multihash.digest,
		);
	}

	private createCoordinateFieldsFromNativePlanFacts(properties: {
		appendFacts: PreparedAppendFacts;
		plan: NativeAppendCoordinatePlan;
		prev?: EntryReplicated<R>;
	}): SharedLogCoordinateNativeFields<R> | false {
		if (
			properties.plan.hash !== properties.appendFacts.hash ||
			properties.plan.gid !== properties.appendFacts.gid
		) {
			return false;
		}

		const assignedToRangeBoundary = properties.plan.assignedToRangeBoundary;
		if (
			properties.prev &&
			properties.prev.assignedToRangeBoundary === assignedToRangeBoundary
		) {
			return false;
		}

		const coordinates = properties.plan.coordinates as NumberFromType<R>[];
		const hashNumber = properties.plan.hashNumber as NumberFromType<R>;
		const wallTime = properties.appendFacts.wallTime;
		const metaBytes = properties.appendFacts.metaBytes;
		if (!metaBytes) {
			return false;
		}
		return {
			hash: properties.plan.hash,
			hashNumber,
			hashNumberString: properties.plan.hashNumberString,
			gid: properties.plan.gid,
			coordinates,
			coordinateStrings: properties.plan.coordinateStrings,
			wallTime,
			wallTimeString: wallTime.toString(),
			assignedToRangeBoundary,
			metaBytes,
		};
	}

	private createCoordinatePersistenceEntryFromNativePlanFacts(properties: {
		appendFacts: PreparedAppendFacts;
		plan: NativeAppendCoordinatePlan;
		prev?: EntryReplicated<R>;
	}): PreparedCoordinatePersistence<R> | false {
		const fields = this.createCoordinateFieldsFromNativePlanFacts(properties);
		if (!fields) {
			return false;
		}
		return {
			assignedToRangeBoundary: fields.assignedToRangeBoundary,
			fields,
		};
	}

	private async planNativeAppendFacts(
		appendFacts: PreparedAppendFacts,
		replicas: number,
		deliveryArg: false | true | DeliveryOptions | undefined,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePlanner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (!nativePlanner || !this.canPlanNativeAppendFacts(appendFacts)) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const fullReplicaDeliveryCandidates =
			await this.getNativeFullReplicaDeliveryCandidates(
				replicas,
				context.selfHash,
			);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { delivery, reliability, requireRecipients, minAcks } =
			this._parseDeliveryOptions(deliveryArg);
		const hashNumber = this.getAppendFactsHashNumber(appendFacts);
		const plan = nativePlanner.planAppendForGid(
			{
				entryHash: appendFacts.hash,
				gid: appendFacts.gid,
				hashNumber,
				nextHashes: appendFacts.next,
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
		const coordinates = plan.coordinate.coordinates as NumberFromType<R>[];
		const hashNumberFromPlan = plan.coordinate.hashNumber as NumberFromType<R>;
		const preparedCoordinate =
			this.createCoordinatePersistenceEntryFromNativePlanFacts({
				appendFacts,
				plan: plan.coordinate,
			});
		if (!preparedCoordinate) {
			return undefined;
		}
		return {
			coordinates,
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
			hashNumber: hashNumberFromPlan,
			preparedCoordinate,
			delivery: plan.delivery,
			committedNativeCoordinateState:
				nativePlanner === this._nativeSharedLogState,
			committedNativeBackboneCoordinateState:
				nativePlanner === this._nativeBackbone,
		};
	}

	private async planNativeLocalAppendFacts(
		appendFacts: PreparedAppendFacts,
		replicas: number,
		options?: { deleteHashes?: string[] },
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePlanner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (!nativePlanner || !this.canPlanNativeAppendFacts(appendFacts)) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const hashNumber = this.getAppendFactsHashNumber(appendFacts);
		const nativeLeaderOptions = this.createNativeLeaderOptions(context);
		const plan =
			options?.deleteHashes && options.deleteHashes.length > 0
				? nativePlanner.commitLocalAppendForGidCompact(
						{
							entryHash: appendFacts.hash,
							gid: appendFacts.gid,
							hashNumber,
							nextHashes: appendFacts.next,
							deleteHashes: options.deleteHashes,
							replicas,
							selfHash: context.selfHash,
						},
						nativeLeaderOptions,
					)
				: nativePlanner.planLocalAppendForGidCompact(
						{
							entryHash: appendFacts.hash,
							gid: appendFacts.gid,
							hashNumber,
							nextHashes: appendFacts.next,
							replicas,
							selfHash: context.selfHash,
						},
						nativeLeaderOptions,
					);
		const coordinates = plan.coordinate.coordinates as NumberFromType<R>[];
		const hashNumberFromPlan = plan.coordinate.hashNumber as NumberFromType<R>;
		const preparedCoordinate =
			this.createCoordinatePersistenceEntryFromNativePlanFacts({
				appendFacts,
				plan: plan.coordinate,
			});
		if (!preparedCoordinate) {
			return undefined;
		}
		return {
			coordinates,
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
			hashNumber: hashNumberFromPlan,
			preparedCoordinate,
			committedNativeCoordinateState:
				nativePlanner === this._nativeSharedLogState,
			committedNativeBackboneCoordinateState:
				nativePlanner === this._nativeBackbone,
			committedNativeCoordinateDeletes:
				!!options?.deleteHashes && options.deleteHashes.length > 0,
		};
	}

	private async planNativeAppendEntry(
		entry: Entry<T>,
		replicas: number,
		deliveryArg: false | true | DeliveryOptions | undefined,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePlanner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (!nativePlanner || !this.canPlanNativeHashGid(entry)) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const fullReplicaDeliveryCandidates =
			await this.getNativeFullReplicaDeliveryCandidates(
				replicas,
				context.selfHash,
			);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { delivery, reliability, requireRecipients, minAcks } =
			this._parseDeliveryOptions(deliveryArg);
		const hashNumber = this.getEntryHashNumber(entry);
		const plan = nativePlanner.planAppendForGid(
			{
				entryHash: entry.hash,
				gid: entry.meta.gid,
				hashNumber,
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
		const coordinates = plan.coordinate.coordinates as NumberFromType<R>[];
		const hashNumberFromPlan = plan.coordinate.hashNumber as NumberFromType<R>;
		const preparedCoordinate =
			this._coordinates.createCoordinatePersistenceEntryFromNativePlan({
				entry,
				plan: plan.coordinate,
			});
		if (!preparedCoordinate) {
			return undefined;
		}
		return {
			coordinates,
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
			hashNumber: hashNumberFromPlan,
			preparedCoordinate,
			delivery: plan.delivery,
			committedNativeCoordinateState:
				nativePlanner === this._nativeSharedLogState,
			committedNativeBackboneCoordinateState:
				nativePlanner === this._nativeBackbone,
		};
	}

	private async planNativeLocalAppendEntry(
		entry: Entry<T>,
		replicas: number,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePlanner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (!nativePlanner || !this.canPlanNativeHashGid(entry)) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const hashNumber = this.getEntryHashNumber(entry);
		const plan = nativePlanner.planLocalAppendForGidCompact(
			{
				entryHash: entry.hash,
				gid: entry.meta.gid,
				hashNumber,
				nextHashes: entry.meta.next,
				replicas,
				selfHash: context.selfHash,
			},
			this.createNativeLeaderOptions(context),
		);
		const coordinates = plan.coordinate.coordinates as NumberFromType<R>[];
		const hashNumberFromPlan = plan.coordinate.hashNumber as NumberFromType<R>;
		const preparedCoordinate =
			this._coordinates.createCoordinatePersistenceEntryFromNativePlan({
				entry,
				plan: plan.coordinate,
			});
		if (!preparedCoordinate) {
			return undefined;
		}
		return {
			coordinates,
			leaders: plan.leaders,
			isLeader: plan.isLeader,
			assignedToRangeBoundary: plan.assignedToRangeBoundary,
			hashNumber: hashNumberFromPlan,
			preparedCoordinate,
			committedNativeCoordinateState:
				nativePlanner === this._nativeSharedLogState,
			committedNativeBackboneCoordinateState:
				nativePlanner === this._nativeBackbone,
		};
	}

	private async planNativeLocalAppendEntries(
		entries: Entry<T>[],
		replicas: number,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<NativeAppendEntryPlan<R>[] | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePlanner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (
			!nativePlanner ||
			entries.length === 0 ||
			!entries.every((entry) => this.canPlanNativeHashGid(entry))
		) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const entriesWithHashNumbers = entries.map((entry) => ({
			entry,
			hashNumber: this.getEntryHashNumber(entry),
		}));
		const plans = nativePlanner.planAppendForGidsBatch(
			{
				entries: entriesWithHashNumbers.map(({ entry, hashNumber }) => ({
					entryHash: entry.hash,
					gid: entry.meta.gid,
					hashNumber,
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
		const out: NativeAppendEntryPlan<R>[] = [];
		for (let index = 0; index < plans.length; index++) {
			const plan = plans[index]!;
			const { entry } = entriesWithHashNumbers[index]!;
			const coordinates = plan.coordinate.coordinates as NumberFromType<R>[];
			const hashNumberFromPlan = plan.coordinate
				.hashNumber as NumberFromType<R>;
			const preparedCoordinate =
				this._coordinates.createCoordinatePersistenceEntryFromNativePlan({
					entry,
					plan: plan.coordinate,
				});
			if (!preparedCoordinate) {
				return undefined;
			}
			out.push({
				coordinates,
				leaders: plan.leaders,
				isLeader: plan.isLeader,
				assignedToRangeBoundary: plan.assignedToRangeBoundary,
				hashNumber: hashNumberFromPlan,
				preparedCoordinate,
				committedNativeCoordinateState:
					nativePlanner === this._nativeSharedLogState,
				committedNativeBackboneCoordinateState:
					nativePlanner === this._nativeBackbone,
			});
		}
		return out;
	}

	private async planNativeAppendEntries(
		entries: Entry<T>[],
		replicas: number,
		deliveryArg: false | true | DeliveryOptions | undefined,
		options: SharedAppendOptions<T> | undefined,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<NativeAppendEntryPlan<R>[] | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const target = options?.target;
		const nativePlanner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (
			target === "all" ||
			target === "none" ||
			!nativePlanner ||
			entries.length === 0 ||
			!entries.every((entry) => this.canPlanNativeHashGid(entry))
		) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const fullReplicaDeliveryCandidates =
			await this.getNativeFullReplicaDeliveryCandidates(
				replicas,
				context.selfHash,
			);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { delivery, reliability, requireRecipients, minAcks } =
			this._parseDeliveryOptions(deliveryArg);
		const entriesWithHashNumbers = entries.map((entry) => ({
			entry,
			hashNumber: this.getEntryHashNumber(entry),
		}));
		const plans = nativePlanner.planAppendForGidsBatch(
			{
				entries: entriesWithHashNumbers.map(({ entry, hashNumber }) => ({
					entryHash: entry.hash,
					gid: entry.meta.gid,
					hashNumber,
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
		const out: NativeAppendEntryPlan<R>[] = [];
		for (let index = 0; index < plans.length; index++) {
			const plan = plans[index]!;
			const { entry } = entriesWithHashNumbers[index]!;
			const coordinates = plan.coordinate.coordinates as NumberFromType<R>[];
			const hashNumberFromPlan = plan.coordinate
				.hashNumber as NumberFromType<R>;
			const preparedCoordinate =
				this._coordinates.createCoordinatePersistenceEntryFromNativePlan({
					entry,
					plan: plan.coordinate,
				});
			if (!preparedCoordinate) {
				return undefined;
			}
			out.push({
				coordinates,
				leaders: plan.leaders,
				isLeader: plan.isLeader,
				assignedToRangeBoundary: plan.assignedToRangeBoundary,
				hashNumber: hashNumberFromPlan,
				preparedCoordinate,
				delivery: plan.delivery,
				committedNativeCoordinateState:
					nativePlanner === this._nativeSharedLogState,
				committedNativeBackboneCoordinateState:
					nativePlanner === this._nativeBackbone,
			});
		}
		return out;
	}

	private async processLocalAppend(
		entry: Entry<T>,
		removed: ShallowOrFullEntry<T>[],
		options: SharedAppendOptions<T> | undefined,
		properties: {
			minReplicasValue: number;
			appendFacts?: PreparedAppendFacts;
			deferHeadCoordinatePersistence?: boolean;
			captureDeferredBackfillSource?: (
				source: PersistedAppendBackfillSource<T, R>,
			) => void;
			nativeAppendPlan?: NativeAppendEntryPlan<R>;
			extraCoordinateDeleteHashes?: string[];
			ownershipLifecycleController?: AbortController;
		},
	): Promise<NativeAppendEntryPlan<R> | undefined> {
		const ownershipLifecycleController =
			properties.ownershipLifecycleController ??
			this.captureReplicationOwnershipLifecycle();
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const deferHeadCoordinatePersistence =
			properties.deferHeadCoordinatePersistence ??
			(entry.meta.type !== EntryType.CUT &&
				this.shouldDeferHeadCoordinatePersistence(options));

		if (options?.replicate) {
			await this.replicate(entry, { checkDuplicates: true });
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}

		if (deferHeadCoordinatePersistence) {
			await this._coordinates.deleteCoordinatesForHashes(
				[
					...(properties.appendFacts?.next ?? entry.meta.next),
					...removed.map((entry) => entry.hash),
				],
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return;
		}

		const selfHash = this.node.identity.publicKey.hashcode();
		const target = options?.target;
		const deliveryArg = options?.delivery;
		let nativeAppendPlan = properties.nativeAppendPlan;
		if (!nativeAppendPlan && target !== "all") {
			nativeAppendPlan =
				target === "none"
					? properties.appendFacts
						? await this.planNativeLocalAppendFacts(
								properties.appendFacts,
								properties.minReplicasValue,
								undefined,
								ownershipLifecycleController,
							)
						: await this.planNativeLocalAppendEntry(
								entry,
								properties.minReplicasValue,
								ownershipLifecycleController,
							)
					: properties.appendFacts
						? await this.planNativeAppendFacts(
								properties.appendFacts,
								properties.minReplicasValue,
								deliveryArg,
								ownershipLifecycleController,
							)
						: await this.planNativeAppendEntry(
								entry,
								properties.minReplicasValue,
								deliveryArg,
								ownershipLifecycleController,
							);
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		let coordinates: NumberFromType<R>[];
		let leaders: LeaderMap | undefined;
		let isLeader: boolean;
		let nativeDeliveryPlan: AppendDeliveryPlan | undefined;
		if (nativeAppendPlan) {
			coordinates = nativeAppendPlan.coordinates;
			leaders = nativeAppendPlan.leaders;
			isLeader = nativeAppendPlan.isLeader;
			nativeDeliveryPlan = nativeAppendPlan.delivery;
			if (!isLeader && !leaders) {
				leaders = (
					await this.planEntryLeaders(
						entry,
						properties.minReplicasValue,
						{
							persist: false,
						},
						ownershipLifecycleController,
					)
				).leaders;
			}
			if (properties.appendFacts) {
				await this._coordinates.persistPreparedCoordinate(
					{
						prepared: nativeAppendPlan.preparedCoordinate,
						hash: properties.appendFacts.hash,
						nextHashes: properties.appendFacts.next,
						deleteHashes: properties.extraCoordinateDeleteHashes,
						coordinates,
						replicas: coordinates.length,
						commitNative:
							nativeAppendPlan.committedNativeCoordinateState !== true,
						commitNativeBackbone:
							nativeAppendPlan.committedNativeBackboneCoordinateState !== true,
					},
					ownershipLifecycleController,
				);
			} else {
				await this._coordinates.persistCoordinate(
					{
						leaders: leaders ?? false,
						coordinates,
						replicas: coordinates.length,
						entry,
						assignedToRangeBoundary: nativeAppendPlan.assignedToRangeBoundary,
						commitNative:
							nativeAppendPlan.committedNativeCoordinateState !== true,
						commitNativeBackbone:
							nativeAppendPlan.committedNativeBackboneCoordinateState !== true,
						deleteHashes: properties.extraCoordinateDeleteHashes,
						hashNumber: nativeAppendPlan.hashNumber,
						prepared: nativeAppendPlan.preparedCoordinate,
					},
					ownershipLifecycleController,
				);
			}
		} else {
			({ coordinates, leaders, isLeader } = await this.planEntryLeaders(
				entry,
				properties.minReplicasValue,
				{
					persist: {},
				},
				ownershipLifecycleController,
			));
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);

		if (properties.captureDeferredBackfillSource) {
			let extras: Omit<
				PersistedAppendBackfillSource<T, R>,
				"entry" | "coordinates"
			> = {
				assignmentExtraLeaders: new Map(),
				deliveryExtraTargets: new Set(),
			};
			try {
				extras = await this.collectDeferredAppendBackfillExtras(
					entry,
					properties.minReplicasValue,
					leaders!,
					nativeDeliveryPlan,
					ownershipLifecycleController,
				);
			} catch (error) {
				if (this.isRepairLifecycleActive(ownershipLifecycleController)) {
					logger.error(error);
				}
			}
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			properties.captureDeferredBackfillSource({
				entry,
				coordinates: [...coordinates],
				...extras,
			});
		}

		if (
			options?.target !== "none" &&
			!properties.captureDeferredBackfillSource
		) {
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
				await this._appendDeliverToAllFanout(
					entry,
					ownershipLifecycleController,
				);
			} else {
				await this._appendDeliverToReplicators(
					entry,
					coordinates,
					properties.minReplicasValue,
					leaders!,
					selfHash,
					isLeader,
					deliveryArg,
					nativeDeliveryPlan,
					ownershipLifecycleController,
				);
			}
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}

		const delayAdaptiveRebalance = this.shouldDelayAdaptiveRebalance();
		if (!isLeader && !delayAdaptiveRebalance) {
			await this.pruneDebouncedFnAddIfNotKeeping(
				{
					key: entry.hash,
					value: { entry, leaders: leaders! },
				},
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		// Keep the debounced rebalance loop alive even when the current write
		// burst delays the actual rebalance; the loop will wake after the idle
		// window and re-check participation/memory.
		this.rebalanceParticipationDebounced?.call();

		return nativeAppendPlan;
	}

	private createPreparedLocalAppendCommit(
		entry: Entry<T>,
		nativeAppendPlan?: NativeAppendEntryPlan<R>,
	): PreparedLocalAppendCommit<R> {
		return {
			hash: entry.hash,
			gid: entry.meta.gid,
			next: entry.meta.next,
			wallTime: entry.meta.clock.timestamp.wallTime,
			logical: entry.meta.clock.timestamp.logical,
			clockId: entry.meta.clock.id,
			type: entry.meta.type,
			metaData: entry.meta.data,
			payloadSize: entry.payload.byteLength,
			entrySize: entry.size,
			metaBytes: (entry as EntryWithMetaBytes).getMetaBytes?.(),
			hashNumber: nativeAppendPlan?.hashNumber,
			coordinateFields: nativeAppendPlan?.preparedCoordinate.fields,
		};
	}

	private createPreparedLocalAppendCommitFromFacts(
		appendFacts: PreparedAppendFacts,
		nativeAppendPlan?: {
			hashNumber?: NumberFromType<R>;
			preparedCoordinate?: PreparedCoordinatePersistence<R>;
			coordinateFields?: SharedLogCoordinateNativeFields<R>;
		},
	): PreparedLocalAppendCommit<R> {
		return {
			hash: appendFacts.hash,
			gid: appendFacts.gid,
			next: appendFacts.next,
			wallTime: appendFacts.wallTime,
			logical: appendFacts.logical,
			clockId: appendFacts.clockId,
			type: appendFacts.type,
			metaData: appendFacts.metaData,
			payloadSize: appendFacts.payloadSize,
			metaBytes: appendFacts.metaBytes,
			hashNumber: nativeAppendPlan?.hashNumber,
			coordinateFields:
				nativeAppendPlan?.coordinateFields ??
				nativeAppendPlan?.preparedCoordinate?.fields,
		};
	}

	private snapshotPreparedLocalAppendCommit(
		appendCommit: PreparedLocalAppendCommit<R>,
	): PreparedLocalAppendCommit<R> {
		const coordinateFields = appendCommit.coordinateFields;
		return Object.freeze({
			...appendCommit,
			next: [...appendCommit.next],
			clockId: appendCommit.clockId && Uint8Array.from(appendCommit.clockId),
			metaData: appendCommit.metaData && Uint8Array.from(appendCommit.metaData),
			metaBytes:
				appendCommit.metaBytes && Uint8Array.from(appendCommit.metaBytes),
			storageBytes:
				appendCommit.storageBytes && Uint8Array.from(appendCommit.storageBytes),
			coordinateFields: coordinateFields
				? {
						...coordinateFields,
						coordinates: [...coordinateFields.coordinates],
						coordinateStrings: coordinateFields.coordinateStrings && [
							...coordinateFields.coordinateStrings,
						],
						metaBytes: Uint8Array.from(coordinateFields.metaBytes),
					}
				: undefined,
			documentPreviousContext: appendCommit.documentPreviousContext
				? { ...appendCommit.documentPreviousContext }
				: undefined,
		});
	}

	private capturePersistedLocalAppendCommit(
		canonicalHash: string,
		entry: Entry<T>,
	): PreparedLocalAppendCommit<R> {
		const sourceHash = entry.hash;
		if (sourceHash !== canonicalHash) {
			throw new Error(
				`Lower-log commit evidence entry did not match committed hash ${canonicalHash}`,
			);
		}
		const storageBytes =
			Entry.getPreparedStorageBytes(entry) ?? entry.getStorageBytes();
		return this.snapshotPreparedLocalAppendCommit({
			...this.createPreparedLocalAppendCommit(entry),
			hash: canonicalHash,
			storageBytes,
		});
	}

	private createPersistedDeliveryPlanningRecord(
		appendCommitInput: PreparedLocalAppendCommit<R>,
	): PersistedDeliveryPlanningRecord<T, R> {
		// Callers hand this helper an invocation-owned snapshot. Do not duplicate
		// full entry bytes here; a fresh copy is made only when a custom planner is
		// actually invoked (and again for each replan so planner mutation cannot
		// persist between rounds).
		const appendCommit = appendCommitInput;
		const canonicalHash = appendCommit.hash;
		const coordinateFields = appendCommit.coordinateFields;
		const createDefaultPlanningSource = coordinateFields
			? () =>
					this._coordinates.materializeResidentCoordinateEntry({
						...coordinateFields,
						coordinates: [...coordinateFields.coordinates],
						coordinateStrings: coordinateFields.coordinateStrings && [
							...coordinateFields.coordinateStrings,
						],
						metaBytes: Uint8Array.from(coordinateFields.metaBytes),
					})
			: () =>
					new ShallowEntry({
						hash: canonicalHash,
						head: true,
						payloadSize: appendCommit.payloadSize,
						meta: new ShallowMeta({
							gid: appendCommit.gid,
							next: [...appendCommit.next],
							type: appendCommit.type ?? EntryType.APPEND,
							data:
								appendCommit.metaData && Uint8Array.from(appendCommit.metaData),
							clock: new LamportClock({
								id: Uint8Array.from(appendCommit.clockId ?? new Uint8Array()),
								timestamp: new Timestamp({
									wallTime: appendCommit.wallTime,
									logical: appendCommit.logical,
								}),
							}),
						}),
					});
		const storageBytes = appendCommit.storageBytes;
		const createFullPlanningSource = storageBytes
			? () => {
					const bytes = Uint8Array.from(storageBytes);
					const entry = deserialize(bytes, Entry) as Entry<T>;
					const decodedHash = entry.hash;
					if (decodedHash && decodedHash !== canonicalHash) {
						throw new Error(
							`Persisted delivery planning bytes did not match committed hash ${canonicalHash}`,
						);
					}
					if (!decodedHash) {
						Entry.prepareMultihashBytes(entry, bytes, canonicalHash);
						entry.hash = canonicalHash;
					}
					entry.size = appendCommit.entrySize ?? bytes.byteLength;
					entry.createdLocally = true;
					entry.init({
						encoding: this.log.encoding,
						keychain: this.log.keychain,
					});
					return entry;
				}
			: undefined;
		return Object.freeze({
			canonicalHash,
			createDefaultPlanningSource,
			createFullPlanningSource,
		});
	}

	private snapshotPersistedDeliveryPlanningEntry(
		value: Entry<T>,
	): PersistedDeliveryPlanningRecord<T, R> {
		const canonicalHash = value.hash;
		return this.createPersistedDeliveryPlanningRecord(
			this.capturePersistedLocalAppendCommit(canonicalHash, value),
		);
	}

	private createPreparedLocalAppendCommits(
		entries: Entry<T>[],
		nativeAppendPlans?: Array<NativeAppendEntryPlan<R> | undefined>,
	): PreparedLocalAppendCommit<R>[] {
		return entries.map((entry, index) =>
			this.createPreparedLocalAppendCommit(entry, nativeAppendPlans?.[index]),
		);
	}

	private createPreparedLocalAppendCommitsFromFacts(
		appendFacts: PreparedAppendFacts[] | undefined,
		entries: Entry<T>[],
		nativeAppendPlans?: Array<NativeAppendEntryPlan<R> | undefined>,
	): PreparedLocalAppendCommit<R>[] {
		if (appendFacts && appendFacts.length === entries.length) {
			return appendFacts.map((facts, index) =>
				this.createPreparedLocalAppendCommitFromFacts(
					facts,
					nativeAppendPlans?.[index],
				),
			);
		}
		return this.createPreparedLocalAppendCommits(entries, nativeAppendPlans);
	}

	async open(options?: Args<T, D, R>): Promise<void> {
		// B12: replication-info network compatibility modes are retired. Read the
		// RAW argument value (the option no longer exists on the type) so untyped
		// JS callers cannot smuggle a value past the removed field, and reject
		// ANY defined value — including 10, which previously behaved like the
		// default — BEFORE any open-time side effect (rpc.open, index/native
		// setup, domain resolution, synchronizer creation, subscription setup).
		// An explicitly-present `undefined` stays accepted.
		const rawCompatibility = (options as any)?.compatibility;
		if (rawCompatibility !== undefined) {
			throw new CompatibilityModeRetiredError(rawCompatibility);
		}
		this.ensureNativeDurabilityRuntimeState();
		this._nativeStrictDurableTransactionsClosing = false;
		this._replicationRangeMutationsClosing = false;
		// The legacy `_checkedPruneRemoveBlocksLocalRangeMutationAdmission = 0`
		// reset that sat here comes free with the fresh CheckedPruneCoordinator
		// created below: the counter physically lives on it now, and nothing
		// between this point and that creation invokes invokeProgramOnChange
		// (its only reader) or any log mutation.
		this._checkedPruneRemovalCallbackInvocationDepth = 0;
		// The legacy `_receiveOwnershipRevision = 0` and
		// `_receiveOwnershipMutationAdmissions = 0` resets that sat here come
		// free with the fresh InstanceLifecycle created below: the counters
		// physically live on it now, and no consumer runs between this point
		// and that creation (straight-line assignments only).
		this._pruneRemovesClosing = false;
		this._replicationRangeMutationFailure = undefined;
		// One InstanceLifecycle per open(), created inside
		// startRepairLifecycle() (stage 4): the fresh object physically owns
		// the ownership controller, so lifecycle and controller rotate
		// together by construction and no statement in this reset block can
		// observe the previous lifecycle's roleGeneration. Late-bound readers
		// make it insensitive to the resets that follow (membership controller
		// at resetSubscriptionChangeCallbackTracking below, _checkedPrune and
		// _closeController and the debouncers in the setup blocks further
		// down). The fresh object is also the per-open reset for the role
		// sub-generation and the receive-ownership counters: roleGeneration,
		// _receiveOwnershipRevision and _receiveOwnershipMutationAdmissions
		// all start at 0 on the incoming lifecycle.
		this.startRepairLifecycle();
		this.resetReplicationStatusLifecycle();
		// Guard: between startRepairLifecycle() and
		// resetSubscriptionChangeCallbackTracking() the fresh lifecycle's
		// membership controller is undefined (legacy exposed the previous
		// open's aborted controller in this window). Straight-line statements
		// only; do not read the membership lifecycle controller here.
		this._replicationRangeMutationTail = Promise.resolve();
		this.resetSubscriptionChangeCallbackTracking();
		const recoveringNativeDurableFailure =
			this._nativeDurableCommitFailure !== undefined;
		options = applySharedLogNativeDefaults(
			options,
			(this.node as unknown as NodeWithSharedLogNativeDefaults)
				.sharedLogNativeDefaults,
		);
		const openProfile = options?.sync?.profile;
		const openStartedAt = syncProfileStart(openProfile);
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
		this._receiveSignatureVerificationFacts = new WeakMap();

		this.domain = options?.domain
			? (options.domain(this) as unknown as D)
			: (createReplicationDomainHash("u64")(this) as unknown as D);
		this.indexableDomain = createIndexableDomainFromResolution(
			this.domain.resolution,
		);
		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 2e4;
		this._checkedPrune = new CheckedPruneCoordinator<T, R>();
		this._checkedPruneAuditTimer = undefined;
		this._admittedPruneRemoves = new Set();
		this._pendingIHave = new Map();
		this._pendingIHaveCallbacks = new Set();
		this._replicationInfoRequestByPeer = new Map();
		this._subscriberSnapshotRequestsByPeer = new Map();
		// Terminal close/drop drains the previous lifecycle before another open can
		// install fresh lanes and opaque per-subscription ownership tokens.
		this._replicationInfoApplyQueueByPeer = new Map();
		// Deserialized instances never ran the constructor; create the session
		// registry lazily on first open. Reopens keep the SAME registry so stale
		// continuations observe resetForOpen()'s map swap via property lookup.
		// resetForOpen also replaces the per-peer receive-epoch and receive
		// cleanup-gate maps and the replication-info blocked set (fence B5),
		// matching the legacy open()-time
		// `_replicationInfoReceiveEpochByPeer = new Map()`,
		// `_receiveCleanupGateByPeer = new Map()` and
		// `_replicationInfoBlockedPeers = new Set()`.
		this._peerSessions ??= this.createPeerSessionRegistry();
		this._peerSessions.resetForOpen();
		this._pendingReplicatorLeaveByPeer = new Set();
		this._activeReceiveHandlersByPeer = new Map();
		this._receiveHandlerDrainByPeer = new Map();
		this._openingSyncCapabilitiesByPeer = new Map();
		this._repairRetryTimers = new Set();
		this._recentRepairDispatch = new Map();
		this._repairSweepRunning = false;
		this._repairSweepPendingModes = new Set();
		this._repairSweepPendingPeersByMode = createRepairPendingPeersByMode();
		this._repairFrontierByMode = createRepairFrontierByMode() as Map<
			RepairDispatchMode,
			Map<string, Map<string, RepairDispatchEntry<R>>>
		>;
		this._repairFrontierActiveTargetsByMode = createRepairActiveTargetsByMode();
		this._repairFrontierBypassKnownPeersByMode =
			createRepairFrontierBypassKnownPeersByMode();
		// Deserialized instances never ran the constructor; create the coordinator
		// lazily on first open. Reopens keep the SAME coordinator instance so a
		// still-running drain from a previous lifecycle observes reset()'s map
		// swaps via property lookup.
		this.joinWarmup ??= this.createJoinWarmupCoordinator();
		this.joinWarmup.reset();
		this._repairSweepOptimisticGidPeersPending = new Map();
		this._repairSweepOptimisticGidsByPeer = new Map();
		this._entryKnownPeers = new Map();
		this._entryKnownPeerObservedAt = new Map();
		this._entryKnownPeerObservedAtSweptAt = 0;
		this._joinAuthoritativeRepairTimersByDelay = new Map();
		this._joinAuthoritativeRepairPeersByDelay = new Map();
		this._assumeSyncedRepairSuppressedUntil = 0;
		this._appendBackfillTimer = undefined;
		this._appendBackfillPendingByTarget = new Map();
		this._repairMetrics = createRepairMetrics();
		this._topicSubscribersCache = new Map();
		this._localReachablePeerHashesCache = new Map();
		this._leaderSelectionContextCache = undefined;
		this._leaderPlanCache = new LeaderPlanCache({
			max: LEADER_PLAN_CACHE_MAX,
			ttl: LEADER_PLAN_CACHE_TTL_MS,
		});
		this._peerSyncCapabilities = new Map();
		this._peerSyncCapabilitySessions = new Map();
		this._peerSyncCapabilityTimestamps = new Map();
		this._persistedReceiptReadinessGenerations = new WeakMap();
		this._persistedReceiptReadinessGenerationPrefix = toHexString(
			randomBytes(8),
		);
		this._persistedReceiptReadinessGenerationCounter = 0;
		this._persistedReceiptReadinessWaiters = new Set();
		this._persistedReceiptStorage = undefined;
		this._persistedReceiptRequestsInFlight = new Map();
		this._persistedReceiptRequestsInFlightTotal = 0;
		this._liveRawGossipBatches = new Map();
		this._liveRawGossipFlushScheduled = false;
		this.coordinateToHash = new Cache<string>({ max: 1e6, ttl: 1e4 });
		this.recentlyRebalanced = new Cache<string>({ max: 1e4, ttl: 1e5 });

		this.uniqueReplicators = new Set();
		this._replicatorJoinEmitted = new Set();
		// Deserialized instances never ran the constructor; create the monitor and
		// coordinator lazily on first open. Reopens keep the SAME instances so
		// stale async continuations observe resets via property lookup.
		this._liveness ??= this.createReplicatorLivenessMonitor();
		this._liveness.resetForOpen();
		this._lastLocalAppendAt = 0;
		this._announcements ??= this.createReplicationAnnouncementCoordinator();
		this._v2Receive ??= this.createReplicationInfoV2ReceiveCoordinator();
		this._v2Receive.resetForOpen();
		this._v2Send ??= this.createReplicationInfoV2SendCoordinator();
		this._v2Send.resetForOpen();
		const adaptiveReplicateOptions =
			options?.replicate && isAdaptiveReplicatorOption(options.replicate)
				? options.replicate
				: undefined;
		const adaptiveRebalanceInterval =
			adaptiveReplicateOptions?.limits?.interval ??
			RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL;
		const hasAdaptiveResourceLimits =
			adaptiveReplicateOptions?.limits?.storage != null ||
			adaptiveReplicateOptions?.limits?.cpu != null;
		this.adaptiveRebalanceIdleMs = hasAdaptiveResourceLimits
			? Math.max(
					ADAPTIVE_REBALANCE_MIN_IDLE_AFTER_LOCAL_APPEND_MS,
					adaptiveRebalanceInterval *
						ADAPTIVE_REBALANCE_IDLE_INTERVAL_MULTIPLIER,
				)
			: adaptiveRebalanceInterval;

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
		const invalidateLeaderSelectionContext = () =>
			this.invalidateLeaderSelectionContextCache();
		const onReplicationStatusInputChange = () =>
			this.scheduleReplicationStatusRefresh();
		this.events.addEventListener(
			"replication:change",
			invalidateLeaderSelectionContext,
		);
		this.events.addEventListener(
			"replication:change",
			onReplicationStatusInputChange,
		);
		this.events.addEventListener(
			"replicator:join",
			onReplicationStatusInputChange,
		);
		this.events.addEventListener(
			"replicator:leave",
			onReplicationStatusInputChange,
		);
		this.events.addEventListener(
			"replicator:mature",
			invalidateLeaderSelectionContext,
		);
		this._closeController.signal.addEventListener("abort", () => {
			this.events.removeEventListener(
				"replication:change",
				invalidateLeaderSelectionContext,
			);
			this.events.removeEventListener(
				"replication:change",
				onReplicationStatusInputChange,
			);
			this.events.removeEventListener(
				"replicator:join",
				onReplicationStatusInputChange,
			);
			this.events.removeEventListener(
				"replicator:leave",
				onReplicationStatusInputChange,
			);
			this.events.removeEventListener(
				"replicator:mature",
				invalidateLeaderSelectionContext,
			);
			this.invalidateLeaderSelectionContextCache();
		});

		this._isTrustedReplicator = options?.canReplicate;
		this.keep = options?.keep;
		this.pendingMaturity = new Map();

		const localStateStartedAt = syncProfileStart(openProfile);
		const id = sha256Base64Sync(this.log.id);
		const [storage, logScope] = await Promise.all([
			this.node.storage.sublevel(id),
			this.node.indexer.scope(id),
		]);

		const fanoutService = getSharedLogFanoutService(this.node.services);
		const blockProviderNamespace = (cid: string) => `cid:${cid}`;
		const logProviderNamespace = `shared-log|${this.topic}`;
		const announceBlockProvider = async (cid: string): Promise<void> => {
			try {
				await fanoutService?.announceProvider(blockProviderNamespace(cid), {
					ttlMs: 120_000,
					bootstrapMaxPeers: 2,
				});
			} catch {
				// Provider publication is best-effort.
			}
		};
		const announceBlockProviders = async (cids: string[]): Promise<void> => {
			const batchedAnnounce = fanoutService?.announceProviders;
			if (typeof batchedAnnounce === "function") {
				const namespaces = function* () {
					for (const cid of cids) yield blockProviderNamespace(cid);
				};
				await batchedAnnounce.call(fanoutService, namespaces(), {
					ttlMs: 120_000,
					bootstrapMaxPeers: 2,
				});
				return;
			}

			// Tolerate a skewed runtime that supplied an older FanoutTree service.
			let nextIndex = 0;
			const worker = async () => {
				for (;;) {
					const index = nextIndex++;
					if (index >= cids.length) return;
					await announceBlockProvider(cids[index]!);
				}
			};
			await Promise.all(
				Array.from({ length: Math.min(8, cids.length) }, () => worker()),
			);
		};
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
		emitAdvisorySyncProfileDuration(openProfile, localStateStartedAt, {
			name: "sharedLog.open.localState",
			component: "shared-log",
		});
		const blockStoreStartedAt = syncProfileStart(openProfile);
		const deferStandaloneNativeRangePlanner =
			!!options?.nativeBackbone && options.nativeRangePlanner == null;
		await this.openNativeRangePlanner(
			deferStandaloneNativeRangePlanner ? false : options?.nativeRangePlanner,
		);

		this._nativeBackbone = await this.openNativeBackbone(
			options?.nativeBackbone,
		);
		if (this._nativeBackbone) {
			await this.hydrateNativeBackboneSharedLog(this._nativeBackbone);
		} else if (deferStandaloneNativeRangePlanner) {
			await this.openNativeRangePlanner(options?.nativeRangePlanner);
		}
		// Receive fusion: register this program's RPC topic so the native wire
		// decoder stashes raw exchange-head payloads addressed to it. Only
		// useful together with the native backbone (the stashed prepare runs in
		// the same wasm module); without it the regular decode path is used.
		this._wireSyncSession = undefined;
		const wireSyncSession = options?.sync?.nativeWireSync;
		if (wireSyncSession && this._nativeBackbone) {
			this._wireSyncSession = wireSyncSession;
			wireSyncSession.registerTopic(this.topic);
		}
		// Block store selection:
		// - No native backbone: durable per-program cache (unchanged default).
		// - Native backbone WITHOUT a durable directory (memory-only node): the
		//   wasm-memory native store only (unchanged prior behavior).
		// - Native backbone WITH a durable directory: a write-through wrapper that
		//   mirrors the native wasm store to the SAME durable `blocks` sublevel the
		//   default path uses, and rehydrates the wasm map from disk on open. This
		//   is what makes native entry blocks survive a restart so heads reload.
		let localBlocks: NonNullable<RemoteBlocks["localStore"]>;
		if (this._nativeBackbone) {
			if (this.node.directory != null) {
				const durable = await createNativeDurableBlockStore(
					storage as unknown as DurableBlockSublevelStore,
				);
				localBlocks = new NativeBackboneWriteThroughBlockStore(
					this._nativeBackbone.blocks,
					durable,
				) as unknown as NonNullable<RemoteBlocks["localStore"]>;
			} else {
				localBlocks = this._nativeBackbone.blocks;
			}
		} else {
			localBlocks = await createDefaultDurableBlockStore(
				storage as unknown as DurableBlockSublevelStore,
			);
		}
		emitAdvisorySyncProfileDuration(openProfile, blockStoreStartedAt, {
			name: "sharedLog.open.blockStore",
			component: "shared-log",
			details: {
				nativeBackbone: this._nativeBackbone != null,
				directoryConfigured: this.node.directory != null,
			},
		});
		this.remoteBlocks = new RemoteBlocks({
			local: localBlocks,
			publish: (message, options) =>
				this.rpc.send(new BlocksMessage(message), options),
			waitFor: this.rpc.waitFor.bind(this.rpc),
			publicKey: this.node.identity.publicKey,
			// Unsolicited block retention is opt-in. Explicit `true` retains the
			// compatible eager path with bounded validation and storage budgets.
			eagerBlocks: options?.eagerBlocks ?? false,
			resolveProviders: async (cid, opts) => {
				const profile = this._logProperties?.sync?.profile;
				const maxPeers = 8;
				const excluded = new Set((opts?.exclude ?? []).slice(0, maxPeers));
				const lookupPeers = opts?.refresh
					? Math.min(maxPeers * 2, maxPeers + excluded.size)
					: maxPeers;
				const resolutionStartedAt = syncProfileStart(profile);
				const localCandidates =
					(await this.resolveCandidatePeersForHash(cid, {
						signal: opts?.signal,
						maxPeers: lookupPeers,
					})) ?? [];
				const emitResolution = profile
					? (
							status: "aborted" | "local" | "directory",
							targets: number,
							directoryCandidates = 0,
							reachableCandidates = 0,
						) =>
							emitAdvisorySyncProfileDuration(profile, resolutionStartedAt, {
								name: "sharedLog.blocks.resolveProviders",
								component: "shared-log",
								count: targets,
								targets,
								details: {
									status,
									refresh: opts?.refresh === true,
									excluded: excluded.size,
									lookupPeers,
									localCandidates: localCandidates.length,
									directoryCandidates,
									reachableCandidates,
								},
							})
					: undefined;
				if (opts?.signal?.aborted) {
					emitResolution?.("aborted", 0);
					return [];
				}
				const locallyReachable = new Set(
					await this._getLocalReachablePeerHashes(this.topic),
				);
				if (opts?.signal?.aborted) {
					emitResolution?.("aborted", 0, 0, locallyReachable.size);
					return [];
				}
				const confirmed = this._checkedPrune.getConfirmedReplicators(cid);
				const contacted = this._checkedPrune.getContactedReplicators(cid);
				const hasProviderEvidence = (peer: string) =>
					confirmed?.has(peer) === true ||
					contacted?.has(peer) ||
					this.uniqueReplicators.has(peer);
				const hasLiveCandidate = localCandidates.some(
					(peer) => locallyReachable.has(peer) && hasProviderEvidence(peer),
				);

				// Only reachability corroborated by provider/replicator evidence may
				// bypass the initial CID lookup. Arbitrary bootstrap connections are
				// useful fallbacks, but are not evidence that they hold this block.
				if (hasLiveCandidate && !opts?.refresh) {
					const selected = localCandidates.slice(0, maxPeers);
					emitResolution?.("local", selected.length, 0, locallyReachable.size);
					return selected;
				}

				let directoryProviders: string[] = [];
				try {
					const query = (namespace: string) =>
						fanoutService?.queryProviders(namespace, {
							want: lookupPeers,
							timeoutMs: 2_000,
							queryTimeoutMs: 500,
							bootstrapMaxPeers: 2,
							signal: opts?.signal,
						}) ?? Promise.resolve([]);
					const results = await Promise.allSettled([
						query(blockProviderNamespace(cid)),
						query(logProviderNamespace),
					]);
					for (const result of results) {
						if (result.status === "fulfilled") {
							directoryProviders.push(...result.value.slice(0, lookupPeers));
						}
					}
				} catch {
					// Ignore discovery failures; local evidence remains usable.
				}
				if (opts?.signal?.aborted) {
					emitResolution?.(
						"aborted",
						0,
						directoryProviders.length,
						locallyReachable.size,
					);
					return [];
				}

				const selected: string[] = [];
				const selectedSet = new Set<string>();
				const add = (peer: string | undefined) => {
					if (!peer || selectedSet.has(peer) || selected.length >= maxPeers) {
						return;
					}
					selectedSet.add(peer);
					selected.push(peer);
				};
				const append = (
					providers: readonly string[],
					includeExcluded: boolean,
					predicate?: (provider: string) => boolean,
				) => {
					for (const provider of providers) {
						if (selected.length >= maxPeers) return;
						if (
							excluded.has(provider) === includeExcluded &&
							(!predicate || predicate(provider))
						) {
							add(provider);
						}
					}
				};
				const appendInterleaved = (includeExcluded: boolean) => {
					for (
						let index = 0;
						selected.length < maxPeers &&
						(index < localCandidates.length ||
							index < directoryProviders.length);
						index++
					) {
						const local = localCandidates[index];
						if (local && excluded.has(local) === includeExcluded) add(local);
						const directory = directoryProviders[index];
						if (directory && excluded.has(directory) === includeExcluded) {
							add(directory);
						}
					}
				};
				if (opts?.refresh) {
					// Retry results are wider than the regular eight-peer window. Prefer
					// untried reachable holders, then the remaining fresh directory
					// evidence, without discarding attempted peers as bounded transient-
					// failure fallbacks.
					append(directoryProviders, false, (peer) =>
						locallyReachable.has(peer),
					);
					append(
						localCandidates,
						false,
						(peer) => locallyReachable.has(peer) && hasProviderEvidence(peer),
					);
					append(directoryProviders, false);
					append(localCandidates, false);
				} else {
					appendInterleaved(false);
				}
				appendInterleaved(true);
				emitResolution?.(
					"directory",
					selected.length,
					directoryProviders.length,
					locallyReachable.size,
				);
				return selected;
			},
			watchProviders: fanoutService
				? (cid, opts) => {
						const watch = (namespace: string) =>
							fanoutService.watchProviders(namespace, {
								signal: opts.signal,
								want: 8,
								ttlMs: 10_000,
								renewIntervalMs: 5_000,
								bootstrapMaxPeers: 2,
								onProviders: (providers) =>
									opts.onProviders(providers.map((provider) => provider.hash)),
							});
						const cidWatch = watch(blockProviderNamespace(cid));
						try {
							const logWatch = watch(logProviderNamespace);
							return {
								close: () => {
									cidWatch.close();
									logWatch.close();
								},
							};
						} catch (error) {
							cidWatch.close();
							throw error;
						}
					}
				: undefined,
			onPut: fanoutService ? announceBlockProvider : undefined,
			onPutMany: fanoutService
				? (cids) => {
						// A renewable log-wide provider lease makes every CID in a
						// stored batch discoverable to current readers. Retain the
						// per-CID directory publications for released readers that only
						// know the legacy namespace; the bounded workers avoid creating
						// one in-flight promise per block.
						try {
							this.ensureLogProviderHandle(fanoutService);
						} catch {
							// ignore announce failures
						}
						return announceBlockProviders(cids);
					}
				: undefined,
		});

		const remoteBlocksStartedAt = syncProfileStart(openProfile);
		const remoteBlocksStartPromise = this.remoteBlocks.start().then(() => {
			emitAdvisorySyncProfileDuration(openProfile, remoteBlocksStartedAt, {
				name: "sharedLog.open.remoteBlocks",
				component: "shared-log",
			});
		});
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
		this.resetGidPeerHistoryCleanupState();
		const replicationChangeOwnershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		let replicationChangeDebounceFn!: typeof this.replicationChangeDebounceFn;
		replicationChangeDebounceFn = debounceAggregationChanges<
			ReplicationRangeIndexable<R>
		>(async (change) => {
			if (
				this.replicationChangeDebounceFn !== replicationChangeDebounceFn ||
				!this.isRepairLifecycleActive(
					replicationChangeOwnershipLifecycleController,
				)
			) {
				return;
			}
			try {
				await this.onReplicationChange(change);
				if (
					this.replicationChangeDebounceFn === replicationChangeDebounceFn &&
					this.isRepairLifecycleActive(
						replicationChangeOwnershipLifecycleController,
					)
				) {
					this.rebalanceParticipationDebounced?.call();
				}
			} catch (error: any) {
				if (
					this.replicationChangeDebounceFn === replicationChangeDebounceFn &&
					this.isRepairLifecycleActive(
						replicationChangeOwnershipLifecycleController,
					) &&
					!isNotStartedError(error)
				) {
					logger.error(error?.toString?.() ?? String(error));
				}
			}
		}, this.distributionDebounceTime);
		this.replicationChangeDebounceFn = replicationChangeDebounceFn;

		const pruneOwnershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const checkedPruneCoordinator = this._checkedPrune;
		const pruneLifecycle = this._instanceLifecycle!;
		let pruneDebouncedFn!: typeof this.pruneDebouncedFn;
		const isPruneDebounceCurrent = () =>
			// closeController omitted: this seat never compared it.
			pruneLifecycle.isCheckedPruneCurrent(
				checkedPruneCoordinator,
				undefined,
				pruneOwnershipLifecycleController,
			) && pruneLifecycle.isPruneDebouncerCurrent(pruneDebouncedFn);
		pruneDebouncedFn = debouncedAccumulatorMap(
			async (map) => {
				if (!isPruneDebounceCurrent()) {
					return;
				}
				try {
					const current = new Map<
						string,
						{
							entry: CheckedPruneEntry<T, R>;
							leaders: CheckedPruneLeaderMap;
							workToken?: object;
						}
					>();
					const isCandidateCurrent = (
						hash: string,
						value: { workToken?: object },
					) =>
						value.workToken != null &&
						checkedPruneCoordinator.isCandidateTokenCurrent(
							hash,
							value.workToken,
						);
					const selfReplicating = await this.isReplicating();
					if (!isPruneDebounceCurrent()) {
						return;
					}
					for (const [hash, value] of map) {
						if (!isCandidateCurrent(hash, value)) {
							continue;
						}
						const checkedPruneLeaders =
							await this.revalidateCheckedPruneOwnership({
								hash,
								entry: value.entry,
								leaders: value.leaders,
								selfReplicating,
								ownershipLifecycleController: pruneOwnershipLifecycleController,
								checkedPruneCoordinator,
							});
						if (!isPruneDebounceCurrent()) {
							return;
						}
						if (!isCandidateCurrent(hash, value)) {
							continue;
						}
						if (checkedPruneLeaders.localLeader) {
							const retryIdentity =
								checkedPruneCoordinator.getRetryIdentity(hash);
							const retry = retryIdentity?.state;
							await this.cancelCheckedPruneForLocalLeader(hash, {
								preserveRetry: retry != null,
							});
							if (!isPruneDebounceCurrent()) {
								return;
							}
							if (retry) {
								// A delayed confirmation of local ownership is terminal.
								// Future ownership mutations will enqueue fresh work.
								checkedPruneCoordinator.clearRetry(hash, retryIdentity);
							} else {
								// A first positive view can be a transient membership
								// snapshot. Confirm it once after the normal retry delay
								// before forgetting the prune obligation.
								this.scheduleCheckedPruneRetry(
									{
										entry: value.entry,
										leaders: checkedPruneLeaders.leaders,
									},
									pruneOwnershipLifecycleController,
								);
							}
							continue;
						}
						current.set(hash, {
							...value,
							leaders: checkedPruneLeaders.leaders,
						});
					}
					if (current.size > 0 && isPruneDebounceCurrent()) {
						for (const [hash, value] of current) {
							if (!isCandidateCurrent(hash, value)) {
								current.delete(hash);
							}
						}
						this.prune(current, undefined, pruneOwnershipLifecycleController);
					}
				} catch (error) {
					if (isPruneDebounceCurrent() && !isNotStartedError(error as Error)) {
						logger.error(error);
						for (const [hash, value] of map) {
							if (
								!isPruneDebounceCurrent() ||
								value.workToken == null ||
								!checkedPruneCoordinator.isCandidateTokenCurrent(
									hash,
									value.workToken,
								) ||
								checkedPruneCoordinator.hasPendingDelete(hash)
							) {
								continue;
							}
							try {
								this.scheduleCheckedPruneRetry(
									value,
									pruneOwnershipLifecycleController,
								);
							} catch {
								if (
									value.workToken != null &&
									checkedPruneCoordinator.isCandidateTokenCurrent(
										hash,
										value.workToken,
									)
								) {
									checkedPruneCoordinator.invalidateCandidateToken(hash);
								}
								checkedPruneCoordinator.clearRetry(hash);
							}
						}
					}
				}
			},
			PRUNE_DEBOUNCE_INTERVAL,
			(into, from) => {
				into.workToken = from.workToken;
				for (const [k, v] of from.leaders) {
					if (!into.leaders.has(k)) {
						into.leaders.set(k, v);
					}
				}
			},
		);
		this.pruneDebouncedFn = pruneDebouncedFn;

		await remoteBlocksStartPromise;
		// Failed native prepares can leave content-addressed bytes behind. Recovery
		// deliberately preserves them: the reopened lower log is the liveness
		// authority, while these unreachable bytes are safer than deleting a CID that
		// may also belong to an acknowledged, restored, or concurrent operation.
		const useNativeBackboneBlocks =
			this._nativeBackbone && this._logProperties?.replicate === false;
		const nativeBackboneGraph = this._nativeBackbone
			? useNativeBackboneBlocks
				? this._nativeBackbone.graph
				: this._nativeBackbone.storageBackedGraph
			: undefined;
		// The log always opens on RemoteBlocks, whose local layer is the native
		// block store when the backbone is active (see localBlocks above). Opening
		// it on the raw native store instead would drop the remote-fetch options
		// joins rely on: a replicate:false observer syncing a head whose parents
		// are not local would fail block resolution, and Log.join treats that as
		// recoverable and skips the entry without persisting anything.
		const lowerLogStartedAt = syncProfileStart(openProfile);
		await this.log.open(this.remoteBlocks, this.node.identity, {
			keychain: this.node.services.keychain,
			resolveRemotePeers: (hash, options) =>
				this.resolveCandidatePeersForHash(hash, {
					signal: options?.signal,
					maxPeers: 8,
				}),
			...this._logProperties,
			nativeGraph: nativeBackboneGraph
				? {
						graph: nativeBackboneGraph,
						heads: this._logProperties?.nativeBackbone
							? this._logProperties.nativeBackbone.heads
							: undefined,
					}
				: (this._logProperties?.nativeGraph ?? { optional: true }),
			onChange: async (change) => {
				await this.onChange(change);
				return this.invokeProgramOnChange(change);
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
		emitAdvisorySyncProfileDuration(openProfile, lowerLogStartedAt, {
			name: "sharedLog.open.lowerLog",
			component: "shared-log",
		});
		this._persistedReceiptStorage = this.resolvePersistedReceiptStorage();
		try {
			const recovered =
				await this.recoverNativeStrictDurableTransactionIntent();
			if (recovered) {
				await this.reconcileNativeCoordinatesWithLowerCommitMarkers();
			}
		} catch (error) {
			this.poisonNativeStrictDurableTransaction(error);
			throw error;
		}
		// A fresh wrapper alone is not proof of recovery. Clear the cached poison
		// only after the failed native transaction was compensated (or its pending
		// native journals were deliberately discarded during close) and the lower log
		// reopened successfully. Unreferenced content-addressed bytes are preserved;
		// the reopened lower-log facts, not block presence, determine liveness.
		if (
			localBlocks instanceof NativeBackboneWriteThroughBlockStore &&
			!localBlocks.getNativeDurableCommitFailure() &&
			(!recoveringNativeDurableFailure ||
				this._nativeDurableRecoveryReadyForReopen)
		) {
			this._nativeDurableCommitFailure = undefined;
			this._nativeDurableRecoveryReadyForReopen = false;
			this._nativeDurableRecoveryCids.clear();
		}
		this.syncronizer = createSyncronizer<R>({
			numbers: this.indexableDomain.numbers,
			entryIndex: this.entryCoordinatesIndex,
			rangeIndex: this._replicationRangeIndex,
			log: this.log,
			rpc: this.rpc,
			coordinateToHash: this.coordinateToHash,
			getNativeState: () => this._nativeBackbone ?? this._nativeSharedLogState,
			isEntryRecentlyKnownByPeer: (hash, peer, maxAgeMs) =>
				this.isEntryRecentlyKnownByPeer(hash, peer, maxAgeMs),
			peerSupportsRawExchangeHeads: (peer) =>
				this.peerSupportsRawExchangeHeads(peer),
			sendRawExchangeHeads: (
				hashes: string[],
				to: string[],
				sendOptions?: { priority?: number; signal?: AbortSignal },
			) => this.trySendFusedRawExchangeHeads(hashes, to, sendOptions),
			warn,
			resolution: this.domain.resolution,
			sync: options?.sync,
			syncronizer: options?.syncronizer,
		});

		// Open for communcation
		this._onSubscriptionFn =
			this._onSubscriptionFn ||
			((event) => {
				void this.runSubscriptionChangeCallback(() =>
					this._onSubscription(event),
				);
			});
		this._onUnsubscriptionFn =
			this._onUnsubscriptionFn ||
			((event) => {
				void this.runSubscriptionChangeCallback(() =>
					this._onUnsubscription(event),
				);
			});
		const communicationStartedAt = syncProfileStart(openProfile);
		await Promise.all([
			this.rpc.open({
				queryType: TransportMessage,
				responseType: TransportMessage,
				responseHandler: (query, context) => this.onMessage(query, context),
				resolveRequest: (message) =>
					this.resolveStashedRawExchangeHeadsMessage(message),
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
		emitAdvisorySyncProfileDuration(openProfile, communicationStartedAt, {
			name: "sharedLog.open.rpcSubscriptions",
			component: "shared-log",
		});

		const providerChannelStartedAt = syncProfileStart(openProfile);
		const fanoutOpenPromise = this._openFanoutChannel(options?.fanout);
		// Mark previously-owned replication ranges as "new" only when they already exist.
		// Fresh opens have nothing to touch here, so skip the extra scan/write entirely.
		const updateOwnedReplicationPromise = hasIndexedReplicationInfo
			? this.updateTimestampOfOwnedReplicationRanges()
			: Promise.resolve();
		await Promise.all([fanoutOpenPromise, updateOwnedReplicationPromise]);
		emitAdvisorySyncProfileDuration(openProfile, providerChannelStartedAt, {
			name: "sharedLog.open.providerAndOwnership",
			component: "shared-log",
			details: { indexedReplicationInfo: hasIndexedReplicationInfo },
		});

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

		const replicationStartedAt = syncProfileStart(openProfile);
		let replicationAction: "replace" | "resume" | "reset";
		if (hasIndexedReplicationInfo && isUnreplicationOptionsDefined) {
			replicationAction = "replace";
			await this.replicate(options?.replicate, { checkDuplicates: true });
		} else if (canResumeReplication) {
			replicationAction = "resume";
			// dont do anthing since we are alread replicating stuff
		} else {
			replicationAction = "reset";
			await this.replicate(options?.replicate, {
				checkDuplicates: true,
				reset: true,
			});
		}
		emitAdvisorySyncProfileDuration(openProfile, replicationStartedAt, {
			name: "sharedLog.open.replication",
			component: "shared-log",
			details: {
				hadIndexedState: hasIndexedReplicationInfo,
				action: replicationAction,
			},
		});
		const synchronizerStartedAt = syncProfileStart(openProfile);
		await this.syncronizer.open();
		emitAdvisorySyncProfileDuration(openProfile, synchronizerStartedAt, {
			name: "sharedLog.open.synchronizer",
			component: "shared-log",
		});

		this.interval = setInterval(() => {
			void this.rebalanceParticipationDebounced?.call();
		}, RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL);

		this._instanceLifecycle!.markOpenComplete();
		this.scheduleReplicationStatusRefresh();
		emitAdvisorySyncProfileDuration(openProfile, openStartedAt, {
			name: "sharedLog.open.total",
			component: "shared-log",
		});
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
		const errors: unknown[] = [];
		for (const operation of [
			() => this._nativeRangePlanner?.put(nativeRange),
			() => this._nativeSharedLogState?.put(nativeRange),
			() => this._nativeBackbone?.putRange(nativeRange),
		]) {
			try {
				operation();
			} catch (error) {
				errors.push(error);
			}
		}
		if (errors.length === 1) {
			throw errors[0];
		}
		if (errors.length > 1) {
			throw new AggregateError(
				errors,
				"Failed to publish a replication range to every native mirror",
			);
		}
	}

	private deleteNativeReplicationRange(
		range: ReplicationRangeIndexable<R>,
	): void {
		const errors: unknown[] = [];
		for (const operation of [
			() => this._nativeRangePlanner?.delete(range.idString),
			() => this._nativeSharedLogState?.delete(range.idString),
			() => this._nativeBackbone?.deleteRange(range.idString),
		]) {
			try {
				operation();
			} catch (error) {
				errors.push(error);
			}
		}
		if (errors.length === 1) {
			throw errors[0];
		}
		if (errors.length > 1) {
			throw new AggregateError(
				errors,
				"Failed to remove a replication range from every native mirror",
			);
		}
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
		this._coordinates._residentEntryCoordinatesByHash = new Map();
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
					this._coordinates._residentEntryCoordinatesByHash.set(
						result.value.hash,
						result.value,
					);
				}
			}
		} finally {
			await iterator.close();
		}
	}

	private async hydrateNativeBackboneSharedLog(
		backbone: NativePeerbitBackbone,
	): Promise<void> {
		backbone.clearSharedLog();
		const rangeIterator = this.replicationIndex.iterate();
		try {
			for (;;) {
				const batch = await rangeIterator.next(256);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					backbone.putRange(this.toNativeReplicationRange(result.value));
				}
			}
		} finally {
			await rangeIterator.close();
		}
		if (this._coordinates._nativeBackboneCoordinatePersistence) {
			// A previous explicit drop may have been interrupted after its durable
			// tombstone was written. Complete that erase before the adapter can expose
			// any stale coordinate or document state to this backbone.
			await this._coordinates._nativeBackboneCoordinatePersistence.resumeDrop?.();
			await this._coordinates._nativeBackboneCoordinatePersistence.hydrate(
				backbone,
			);
			this._coordinates._nativeBackboneCoordinateJournalLastFlushMs =
				Date.now();
			this.hydrateNativeCoordinateStateFromBackbone(backbone);
			return;
		}
		this._coordinates._residentEntryCoordinatesByHash ??= new Map();
		const iterator = this.entryCoordinatesIndex.iterate({});
		try {
			for (;;) {
				const batch = await iterator.next(256);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					const requestedReplicas = decodeReplicas(result.value).getValue(this);
					backbone.putEntryCoordinates(
						result.value.hash,
						result.value.gid,
						result.value.coordinates,
						result.value.assignedToRangeBoundary,
						requestedReplicas,
						result.value.hashNumber,
					);
					this._coordinates._residentEntryCoordinatesByHash.set(
						result.value.hash,
						result.value,
					);
					for (const value of result.value.coordinates) {
						this.coordinateToHash.add(value, result.value.hash);
					}
				}
			}
		} finally {
			await iterator.close();
		}
	}

	private async reconcileNativeCoordinatesWithLowerCommitMarkers() {
		if (!this._nativeBackbone) {
			return;
		}
		const hashes = new Set(
			this._coordinates._residentEntryCoordinatesByHash?.keys() ?? [],
		);
		const iterator = this.entryCoordinatesIndex.iterate({});
		try {
			for (;;) {
				const batch = await iterator.next(256);
				if (batch.length === 0) {
					break;
				}
				for (const result of batch) {
					hashes.add(result.value.hash);
				}
			}
		} finally {
			await iterator.close();
		}
		if (hashes.size === 0) {
			return;
		}
		const committed = await this.log.entryIndex.hasMany(hashes);
		const orphaned = [...hashes].filter((hash) => !committed.has(hash));
		if (orphaned.length === 0) {
			return;
		}
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIdsNoReturn) {
			await coordinateIndex.delIdsNoReturn(orphaned);
		} else if (coordinateIndex.delIds) {
			await coordinateIndex.delIds(orphaned);
		} else {
			await coordinateIndex.del({
				query:
					orphaned.length === 1
						? { hash: orphaned[0]! }
						: new Or(
								orphaned.map(
									(hash) => new StringMatch({ key: "hash", value: hash }),
								),
							),
			});
		}
		for (const hash of orphaned) {
			this._nativeBackbone.deleteEntryCoordinates(hash);
			this._nativeSharedLogState?.deleteEntryCoordinates(hash);
			this._coordinates._residentEntryCoordinatesByHash?.delete(hash);
		}
		const flushed = this._coordinates.flushNativeBackboneCoordinateJournal();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
	}

	private hydrateNativeCoordinateStateFromBackbone(
		backbone: NativePeerbitBackbone,
	): void {
		const fields = backbone.getEntryCoordinateFields();
		this._nativeSharedLogState?.clearEntryCoordinates();
		this._coordinates._residentEntryCoordinatesByHash = new Map();
		for (const coordinate of fields) {
			const sharedFields =
				this.nativeBackboneCoordinateFieldsToSharedLogFields(coordinate);
			this._nativeSharedLogState?.putEntryCoordinates(
				sharedFields.hash,
				sharedFields.gid,
				sharedFields.coordinates,
				sharedFields.assignedToRangeBoundary,
				coordinate.requestedReplicas,
				sharedFields.hashNumber,
			);
			this._coordinates._residentEntryCoordinatesByHash.set(
				sharedFields.hash,
				sharedFields,
			);
			for (const value of sharedFields.coordinates) {
				this.coordinateToHash.add(value, sharedFields.hash);
			}
		}
	}

	private nativeBackboneCoordinateFieldsToSharedLogFields(
		coordinate: NativeBackboneCoordinateFields,
	): SharedLogCoordinateNativeFields<R> {
		const hashNumber =
			this.domain.resolution === "u32"
				? Number(coordinate.hashNumberString)
				: BigInt(coordinate.hashNumberString);
		const coordinates =
			this.domain.resolution === "u32"
				? coordinate.coordinateStrings.map((value) => Number(value))
				: coordinate.coordinateStrings.map((value) => BigInt(value));
		return {
			hash: coordinate.hash,
			hashNumber: hashNumber as NumberFromType<R>,
			hashNumberString: coordinate.hashNumberString,
			gid: coordinate.gid,
			coordinates: coordinates as NumberFromType<R>[],
			coordinateStrings: coordinate.coordinateStrings,
			wallTime: coordinate.wallTime,
			wallTimeString: coordinate.wallTimeString,
			assignedToRangeBoundary: coordinate.assignedToRangeBoundary,
			metaBytes: coordinate.metaBytes,
		};
	}

	private async openNativeRangePlanner(
		options: SharedLogOptions<T, D, R>["nativeRangePlanner"],
	): Promise<void> {
		this._nativeRangePlanner = undefined;
		this._nativeSharedLogState = undefined;
		this._nativeBackbone = undefined;
		this._coordinates._nativeBackboneCoordinatePersistence = undefined;
		this._coordinates._nativeBackboneCoordinateJournalLastFlushMs = 0;
		this._coordinates._residentEntryCoordinatesByHash = undefined;
		if (options === false) {
			return;
		}
		if (!canUseOptionalNativeModuleImports()) {
			if (options?.optional === false) {
				throw new Error(
					"Native range planner is unavailable in service worker contexts",
				);
			}
			return;
		}

		try {
			const { createRangePlanner, createSharedLogState } = await import(
				/* @vite-ignore */ "@peerbit/shared-log-rust"
			);
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
			this._coordinates._residentEntryCoordinatesByHash = undefined;
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

	private async openNativeBackbone(
		options: SharedLogOptions<T, D, R>["nativeBackbone"],
	): Promise<NativePeerbitBackbone | undefined> {
		this._coordinates._nativeBackboneCoordinatePersistence = undefined;
		this._nativeBackboneCoordinatePersistenceStore = undefined;
		this._nativeBackboneDropStarted = false;
		this._coordinates._nativeBackboneCoordinateJournalLastFlushMs = 0;
		this._nativeStrictDurableTransactionJournalState = undefined;
		if (!options) {
			return undefined;
		}
		if (!canUseOptionalNativeModuleImports()) {
			const error = new Error(
				"Native backbone is unavailable in service worker contexts",
			);
			if (options.optional === false) {
				throw error;
			}
			warn(error.message);
			return undefined;
		}
		if (!(this.node.identity instanceof Ed25519Keypair)) {
			const error = new Error(
				"nativeBackbone requires an Ed25519 node identity",
			);
			if (options.optional === false) {
				throw error;
			}
			warn(error.message);
			return undefined;
		}
		try {
			const nativeBackboneModule = await import(
				/* @vite-ignore */ "@peerbit/native-backbone"
			);
			const {
				createNativeBackboneCoordinatePersistence,
				createNativePeerbitBackbone,
			} = nativeBackboneModule;
			const backbone = await createNativePeerbitBackbone({
				resolution: this.domain.resolution,
				clockId: this.node.identity.publicKey.bytes,
				privateKey: this.node.identity.privateKey.privateKey,
				publicKey: this.node.identity.publicKey.publicKey,
			});
			// Backward compatible: an explicitly supplied coordinate persistence
			// config always wins and is used unchanged. Otherwise, when the node
			// runs on durable on-disk storage, auto-derive a per-program store so
			// replication coordinates survive a clean stop -> restart without a
			// peer to re-derive from. Memory-only nodes (no directory) keep the
			// previous in-memory behavior.
			if (options.coordinatePersistence) {
				if ("store" in options.coordinatePersistence) {
					this._nativeBackboneCoordinatePersistenceStore =
						options.coordinatePersistence.store;
				} else if (options.coordinatePersistence.intentStore) {
					this._nativeBackboneCoordinatePersistenceStore =
						options.coordinatePersistence.intentStore;
				} else if (this.node.directory != null) {
					throw new Error(
						"Durable nativeBackbone.coordinatePersistence adapters must expose intentStore",
					);
				}
				this._coordinates._nativeBackboneCoordinatePersistence =
					createNativeBackboneCoordinatePersistence(
						options.coordinatePersistence as RuntimeNativeBackboneCoordinatePersistenceConfig,
					);
			} else {
				this._coordinates._nativeBackboneCoordinatePersistence =
					await this.createAutoDerivedCoordinatePersistence(
						nativeBackboneModule,
					);
			}
			if (
				this.node.directory != null &&
				this._coordinates._nativeBackboneCoordinatePersistence
			) {
				if (
					this._coordinates._nativeBackboneCoordinatePersistence
						.durableBarrier !== true ||
					typeof this._nativeBackboneCoordinatePersistenceStore
						?.durableBarrier !== "function"
				) {
					throw new Error(
						"Durable nativeBackbone coordinate persistence requires an explicit physical durability barrier",
					);
				}
			}
			if (
				this._coordinates._nativeBackboneCoordinatePersistence &&
				(this._coordinates._nativeBackboneCoordinatePersistence
					.compactMaxJournalBytes != null ||
					this._coordinates._nativeBackboneCoordinatePersistence
						.compactMaxJournalRecords != null) &&
				this._coordinates._nativeBackboneCoordinatePersistence
					.crashSafeCompaction !== true
			) {
				// Durable custom adapters must explicitly advertise an atomic generation
				// protocol before SharedLog permits automatic WAL compaction.
				throw new Error(
					"Durable native coordinate persistence compaction thresholds require crashSafeCompaction",
				);
			}
			return backbone;
		} catch (error) {
			if (options.optional === false) {
				throw error;
			}
			warn(
				`Native backbone unavailable, falling back to regular log storage: ${
					error instanceof Error ? error.message : String(error)
				}`,
			);
			return undefined;
		}
	}

	/**
	 * When the node has a durable on-disk storage directory, build a
	 * per-program coordinate persistence store rooted under it so replication
	 * coordinates auto-persist and survive a clean stop -> restart. Returns
	 * `undefined` for memory-only nodes (no directory), preserving the prior
	 * in-memory behavior.
	 *
	 * Namespacing: `<nodeDirectory>/coordinates/<fsSafe(log.id)>`. The log id
	 * is the same identity used for `storage.sublevel`/`indexer.scope`
	 * (see the `sha256Base64Sync(this.log.id)` above), but that base64 form is
	 * not filesystem-path-safe, so the directory segment uses the hex encoding
	 * of `this.log.id` (only `[0-9a-f]`, no `/`, `+`, or padding).
	 *
	 * Node vs OPFS is chosen with the same signal native-backbone uses to load
	 * its wasm (`globalThis.process?.versions?.node`): Node gets the on-disk
	 * store, browsers get the OPFS store.
	 */
	private async createAutoDerivedCoordinatePersistence(
		nativeBackboneModule: typeof import("@peerbit/native-backbone"),
	): Promise<NativeBackboneCoordinatePersistenceAdapter | undefined> {
		const directory = this.node.directory;
		if (directory == null) {
			// Memory-only node: keep prior in-memory behavior.
			return undefined;
		}
		const {
			createNativeBackboneCoordinatePersistence,
			NativeBackboneNodeCoordinatePersistenceStore,
			NativeBackboneOPFSCoordinatePersistenceStore,
		} = nativeBackboneModule;
		const namespace = toHexString(this.log.id);
		const isNode = !!(
			globalThis as { process?: { versions?: { node?: string } } }
		).process?.versions?.node;
		let store: NativeBackboneCoordinatePersistenceStore;
		if (isNode) {
			const coordinateDirectory = joinNativeCoordinateDirectory(
				directory,
				namespace,
			);
			store = new NativeBackboneNodeCoordinatePersistenceStore(
				coordinateDirectory,
			);
		} else {
			// OPFS stores address by directory parts relative to the OPFS root,
			// not by an absolute filesystem path, so the node `directory` only
			// gates activation; the per-program namespace segments keep programs
			// isolated within the browser's origin-private file system.
			store = await NativeBackboneOPFSCoordinatePersistenceStore.create({
				directory: ["coordinates", namespace],
			});
		}
		this._nativeBackboneCoordinatePersistenceStore = store;
		return createNativeBackboneCoordinatePersistence({
			store,
			buffered: true,
			flushOnAppend: true,
		});
	}

	private async updateTimestampOfOwnedReplicationRanges(
		timestamp: number = +new Date(),
	) {
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		return this.withReceiveOwnershipMutationQueue(async () => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const all = await this.replicationIndex
				.iterate({
					query: { hash: this.node.identity.publicKey.hashcode() },
				})
				.all();
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			this.validatePersistedReplicationRangeSnapshot(
				all.map((result) => result.value),
			);
			const minRoleAge = all.length > 0 ? await this.getDefaultMinRoleAge() : 0;
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);

			const previousRanges = all.map((result) => result.value);
			const bnTimestamp = BigInt(timestamp);
			const updatedRanges = previousRanges.map(
				(range) =>
					Object.assign(Object.create(Object.getPrototypeOf(range)), range, {
						timestamp: bnTimestamp,
					}) as ReplicationRangeIndexable<R>,
			);
			let crossedWriteBoundary = false;
			try {
				for (const range of updatedRanges) {
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					// `put` may commit and then throw, so crossing the call boundary is
					// already an ambiguous durable outcome.
					crossedWriteBoundary = true;
					await this.replicationIndex.put(range);
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					this.putNativeReplicationRange(range);
				}
				if (updatedRanges.length > 0) {
					await this.updateOldestTimestampFromIndex();
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			} catch (primaryError) {
				if (!crossedWriteBoundary) {
					throw primaryError;
				}
				const recoveryErrors: unknown[] = [primaryError];
				let durableRangesById:
					| Map<string, ReplicationRangeIndexable<R>>
					| undefined;
				try {
					const durableRanges =
						await this.resolveReplicationRangesFromIdsAndKey(
							updatedRanges.map((range) => range.id),
							this.node.identity.publicKey,
						);
					durableRangesById = new Map(
						durableRanges.map((range) => [range.idString, range]),
					);
				} catch (probeError) {
					recoveryErrors.push(probeError);
				}

				if (durableRangesById) {
					for (const previousRange of previousRanges) {
						const durableRange = durableRangesById.get(previousRange.idString);
						try {
							if (durableRange) {
								this.putNativeReplicationRange(durableRange);
							} else {
								this.deleteNativeReplicationRange(previousRange);
							}
						} catch (reconcileError) {
							recoveryErrors.push(reconcileError);
						}
					}
					try {
						await this.updateOldestTimestampFromIndex();
					} catch (oldestTimestampError) {
						recoveryErrors.push(oldestTimestampError);
					}
				}

				const failure = new AggregateError(
					recoveryErrors,
					"Failed to update owned replication-range timestamps coherently",
				);
				this.poisonReplicationOwnership(failure);
				throw failure;
			}

			if (updatedRanges.length === 0) {
				return;
			}
			const repairTimers = this._repairRetryTimers;
			let maturityTimeout: ReturnType<typeof setTimeout>;
			const cancelMaturity = () => {
				clearTimeout(maturityTimeout);
				repairTimers.delete(maturityTimeout);
				ownershipLifecycleController.signal.removeEventListener(
					"abort",
					cancelMaturity,
				);
			};
			maturityTimeout = setTimeout(() => {
				repairTimers.delete(maturityTimeout);
				ownershipLifecycleController.signal.removeEventListener(
					"abort",
					cancelMaturity,
				);
				if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
					return;
				}
				this.events.dispatchEvent(
					new CustomEvent<ReplicationChangeEvent>("replicator:mature", {
						detail: { publicKey: this.node.identity.publicKey },
					}),
				);
			}, minRoleAge);
			maturityTimeout.unref?.();
			repairTimers.add(maturityTimeout);
			ownershipLifecycleController.signal.addEventListener(
				"abort",
				cancelMaturity,
				{ once: true },
			);
		}, ownershipLifecycleController);
	}

	async afterOpen(): Promise<void> {
		await super.afterOpen();
		// Start the broader discovery eagerly, in parallel with rebalance, for its
		// routing/cache side effects. It also contains connected/provider/fanout
		// candidates that have not subscribed to this log, so only the authoritative
		// pubsub snapshot below may create subscription fallback sessions.
		const subscriberDiscoveryPromise = this._getTopicSubscribers(this.topic);
		const existingSubscribersPromise = this.node.services.pubsub.getSubscribers(
			this.topic,
		);
		// We do this here, because these calls requires this.closed == false
		void this.pruneOfflineReplicators().catch((error) => {
			if (isNotStartedError(error as Error)) {
				return;
			}
			logger.error(error);
		});

		this._liveness.startReplicatorLivenessSweep();

		await this.rebalanceParticipation();
		await subscriberDiscoveryPromise;

		// Take into account existing subscription
		(await existingSubscribersPromise)?.forEach((v) => {
			if (v.equals(this.node.identity.publicKey)) {
				return;
			}
			if (this.closed) {
				return;
			}
			// The live subscribe event and this after-open snapshot can report the
			// same initial transport generation. The live callback rotates its
			// PeerSession synchronously, so any current session here proves the
			// fallback is stale/duplicate. Rotating again would erase the signed
			// capability binding that the first callback just established.
			if (this._peerSessions.current(v.hashcode()) !== null) {
				return;
			}
			void this.runSubscriptionChangeCallback(() =>
				this.handleSubscriptionChange(v, [this.topic], true),
			);
		});
	}

	async reset() {
		await this.log.load({ reset: true });
	}

	async pruneOfflineReplicators() {
		// Go through all segments and wait for replicators to become reachable;
		// otherwise prune them away from the local membership view.
		const replicationLifecycleController =
			this._instanceLifecycle?.membershipLifecycleController;
		try {
			if (
				!replicationLifecycleController ||
				!this.isReplicationLifecycleActive(replicationLifecycleController)
			) {
				return;
			}
			const promises: Promise<any>[] = [];
			const iterator = this.replicationIndex.iterate();
			const checkedIsAlive = new Set<string>();
			const selfHash = this.node.identity.publicKey.hashcode();

			while (!iterator.done()) {
				const segments = await iterator.next(1000);
				if (
					!this.isReplicationLifecycleActive(replicationLifecycleController)
				) {
					return;
				}
				for (const segment of segments) {
					if (
						checkedIsAlive.has(segment.value.hash) ||
						selfHash === segment.value.hash
					) {
						if (!this.uniqueReplicators.has(selfHash)) {
							this.uniqueReplicators.add(selfHash);
							this.invalidateLeaderSelectionContextCache();
							this.scheduleReplicationStatusRefresh();
						}
						continue;
					}

					checkedIsAlive.add(segment.value.hash);
					const peerHash = segment.value.hash;
					const subscriptionEpoch = this._peerSessions.current(peerHash);

					promises.push(
						waitForSubscribers(this.node, peerHash, this.rpc.topic, {
							timeout: this.waitForReplicatorTimeout,
							signal: this._closeController.signal,
						})
							.then(async () => {
								if (
									!this.isReplicationLifecycleActive(
										replicationLifecycleController,
									)
								) {
									return;
								}
								const key = await this._resolvePublicKeyFromHash(peerHash);
								if (!key) {
									throw new Error(
										"Failed to resolve public key from hash: " + peerHash,
									);
								}

								const keyHash = key.hashcode();
								if (keyHash !== peerHash) {
									return;
								}
								return this.withReplicationInfoApplyQueue(keyHash, async () => {
									// A successful reachability check may legitimately span a
									// subscribe event during startup. The current lane's blocked
									// state plus an extant index row are the authoritative guard;
									// only the destructive catch path remains tied to the old token.
									if (
										!this.isReplicationLifecycleActive(
											replicationLifecycleController,
										) ||
										this.closed ||
										this._peerSessions.isReplicationInfoBlocked(keyHash)
									) {
										return;
									}
									const hasReplicationRange =
										(await this.replicationIndex.count({
											query: { hash: keyHash },
										})) > 0;
									if (
										!hasReplicationRange ||
										!this.isReplicationLifecycleActive(
											replicationLifecycleController,
										) ||
										this._peerSessions.isReplicationInfoBlocked(keyHash)
									) {
										return;
									}
									if (!this.uniqueReplicators.has(keyHash)) {
										this.uniqueReplicators.add(keyHash);
										this.invalidateLeaderSelectionContextCache();
									}

									if (!this._replicatorJoinEmitted.has(keyHash)) {
										this._replicatorJoinEmitted.add(keyHash);
										this.events.dispatchEvent(
											new CustomEvent<ReplicatorJoinEvent>("replicator:join", {
												detail: { publicKey: key },
											}),
										);
										this.events.dispatchEvent(
											new CustomEvent<ReplicationChangeEvent>(
												"replication:change",
												{ detail: { publicKey: key } },
											),
										);
									}
								});
							})
							.catch(async (error) => {
								if (
									isNotStartedError(error as Error) ||
									!this.isReplicationLifecycleActive(
										replicationLifecycleController,
									)
								) {
									return;
								}

								return this.removeReplicator(peerHash, {
									noEvent: true,
									replicationLifecycleController,
									subscriptionEpoch,
								});
							}),
					);
				}
			}

			return Promise.all(promises);
		} catch (error) {
			if (
				isNotStartedError(error as Error) ||
				!this.isReplicationLifecycleActive(replicationLifecycleController)
			) {
				return;
			}
			throw error;
		}
	}

	private cleanupCheckedPrunePeer(
		peerHash: string,
		ownershipLifecycleController: AbortController,
		checkedPruneCoordinator = this._checkedPrune,
	) {
		const lifecycle = this._instanceLifecycle!;
		const invalidated = checkedPruneCoordinator.cleanupPeer(peerHash);
		for (const generation of invalidated) {
			const canRetry =
				generation.pending.retryOnInvalidation === true &&
				// closeController omitted: this seat never compared it. The
				// coordinator here is a parameter — the compare stays
				// param-vs-current exactly as before.
				lifecycle.isCheckedPruneCurrent(
					checkedPruneCoordinator,
					undefined,
					ownershipLifecycleController,
				);
			try {
				void generation.pending.reject(
					new Error(
						`Checked prune generation invalidated by peer cleanup: ${peerHash}`,
					),
					{ preserveRetry: canRetry },
				);
			} catch {
				// State was already invalidated above; retry admission is independent
				// from a synchronous observer error.
			}
			if (
				canRetry &&
				lifecycle.isCheckedPruneCurrent(
					checkedPruneCoordinator,
					undefined,
					ownershipLifecycleController,
				) &&
				!checkedPruneCoordinator.hasPendingDelete(generation.hash)
			) {
				try {
					this.scheduleCheckedPruneRetry(
						{
							entry: generation.entry,
							leaders: generation.leaders,
						},
						ownershipLifecycleController,
					);
				} catch {
					// Peer cleanup is the safety boundary. A best-effort background
					// retry must not abort the ownership removal that invalidated it.
					checkedPruneCoordinator.clearRetry(generation.hash);
				}
			}
		}
	}

	private cleanupPeerDisconnectTracking(
		peerHash: string,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		const peerSession = this._peerSessions.current(peerHash);
		const preserveV2Session =
			peerSession?.phase === "open" && peerSession.isActive();
		if (!preserveV2Session) {
			this.cancelReplicationInfoRequests(peerHash);
		}
		this._liveness._replicatorLivenessFailures.delete(peerHash);
		this._liveness._replicatorLastActivityAt.delete(peerHash);
		if (!preserveV2Session) {
			this._peerSyncCapabilities.delete(peerHash);
			this._peerSyncCapabilitySessions.delete(peerHash);
			this._peerSyncCapabilityTimestamps.delete(peerHash);
			this._v2Receive.clearPeer(peerHash);
			this._v2Send.clearPeer(peerHash);
		}
		this.cleanupPendingIHavePeer(peerHash);
		this.cleanupCheckedPrunePeer(
			peerHash,
			ownershipLifecycleController,
			this._checkedPrune,
		);
		this.dispatchPersistedReceiptReadinessChange(peerHash);
	}

	private cleanupPendingIHavePeer(peerHash: string) {
		for (const [hash, pending] of this._pendingIHave) {
			pending.requesting.delete(peerHash);
			if (pending.requesting.size === 0) {
				pending.clear();
				this._pendingIHave.delete(hash);
			}
		}
	}

	private advanceReplicationInfoRecoveryEpoch(peerHash: string) {
		// Handlers admitted before a successful peer removal must not restore state
		// when they eventually reach the apply lane.
		const receiveEpoch = this._peerSessions.advanceReceiveEpoch(peerHash);
		const peerSession = this._peerSessions.current(peerHash);
		if (peerSession?.phase === "open") {
			this._v2Receive.advanceRecovery({
				peerHash,
				peerSession,
				receiveEpoch,
			});
		}
		this.dispatchPersistedReceiptReadinessChange(peerHash);
	}

	private async resolveCandidatePeersForHash(
		hash: string,
		options?: { signal?: AbortSignal; maxPeers?: number },
	): Promise<string[] | undefined> {
		if (options?.signal?.aborted) return undefined;

		const maxPeers = options?.maxPeers ?? 8;
		if (maxPeers <= 0) return undefined;
		const self = this.node.identity.publicKey.hashcode();
		const seed = hashToSeed32(hash);

		// Replication/provider knowledge deliberately outlives a transport session.
		// That is useful for repair, but it must not fill the bounded fetch target set
		// with departed peers while a reachable holder is available. The local
		// snapshot performs no tracker/provider discovery or key resolution.
		const reachable = await this._getLocalReachablePeerHashes(this.topic);
		if (options?.signal?.aborted) return undefined;
		const reachableSet = new Set(reachable);
		const confirmed = this._checkedPrune.getConfirmedReplicators(hash);
		const contacted = this._checkedPrune.getContactedReplicators(hash);
		const evidenceTiers: readonly Iterable<string>[] = [
			confirmed ?? [],
			contacted ?? [],
			this.uniqueReplicators,
		];

		// Peers supported by both current transport state and provider history are
		// the strongest signal. Afterwards alternate live-only and historical-only
		// evidence so either source can widen a saturated bounded set. Derive the
		// live intersection from the bounded reachability snapshot; never clone or
		// scan an unbounded historical replicator set per missing block.
		const liveEvidenceTiers: string[][] = [[], [], []];
		for (const peer of reachable) {
			if (confirmed?.has(peer)) liveEvidenceTiers[0].push(peer);
			else if (contacted?.has(peer)) liveEvidenceTiers[1].push(peer);
			else if (this.uniqueReplicators.has(peer))
				liveEvidenceTiers[2].push(peer);
		}
		const liveKnown: string[] = [];
		for (let tier = 0; tier < liveEvidenceTiers.length; tier++) {
			liveKnown.push(
				...pickDeterministicSubset(
					liveEvidenceTiers[tier],
					(seed ^ Math.imul(tier + 1, 0x9e3779b1)) >>> 0,
					maxPeers - liveKnown.length,
				),
			);
			if (liveKnown.length >= maxPeers) break;
		}
		const liveKnownSet = new Set(liveKnown);
		const liveOnly = pickDeterministicSubset(
			reachable.filter((peer) => !liveKnownSet.has(peer)),
			(seed ^ 0x85ebca6b) >>> 0,
			maxPeers,
		);
		const historicalOnly: string[] = [];
		const historicalSeen = new Set<string>();
		for (let tier = 0; tier < evidenceTiers.length; tier++) {
			const available: string[] = [];
			let inspected = 0;
			for (const peer of evidenceTiers[tier]) {
				if (inspected++ >= LOCAL_REACHABLE_PEERS_MAX) break;
				if (
					!peer ||
					peer === self ||
					reachableSet.has(peer) ||
					historicalSeen.has(peer)
				) {
					continue;
				}
				historicalSeen.add(peer);
				available.push(peer);
			}
			historicalOnly.push(
				...pickDeterministicSubset(
					available,
					(seed ^ Math.imul(tier + 5, 0x9e3779b1)) >>> 0,
					maxPeers - historicalOnly.length,
				),
			);
			if (historicalOnly.length >= maxPeers) break;
		}
		const selected = liveKnown.slice(0, maxPeers);
		let liveIndex = 0;
		let historicalIndex = 0;
		while (
			selected.length < maxPeers &&
			(liveIndex < liveOnly.length || historicalIndex < historicalOnly.length)
		) {
			if (liveIndex < liveOnly.length) {
				selected.push(liveOnly[liveIndex++]);
			}
			if (
				selected.length < maxPeers &&
				historicalIndex < historicalOnly.length
			) {
				selected.push(historicalOnly[historicalIndex++]);
			}
		}

		return selected.length > 0 ? selected : undefined;
	}

	async getMemoryUsage() {
		return this.log.blocks.size();
		/* ((await this.log.entryIndex?.getMemoryUsage()) || 0) */ // + (await this.log.blocks.size())
	}

	private resetReplicationStatusLifecycle(): void {
		this._replicationStatus = undefined;
		this._replicationStatusReadTail = Promise.resolve();
		this._replicationStatusRefreshScheduled = false;
		this._replicationStatusRefreshDirty = false;
	}

	private isReplicationStatusLifecycleCurrent(
		lifecycle: InstanceLifecycle | undefined,
	): lifecycle is InstanceLifecycle {
		return (
			lifecycle != null &&
			lifecycle === this._instanceLifecycle &&
			lifecycle.phase() === "active"
		);
	}

	private async measureReplicationStatus(
		lifecycle: InstanceLifecycle,
	): Promise<ReplicationStatus> {
		if (!this.isReplicationStatusLifecycleCurrent(lifecycle)) {
			throw new ClosedError();
		}

		const capturedRoleGeneration = lifecycle.roleGeneration;
		const capturedOwnershipRevision = lifecycle._receiveOwnershipRevision;
		if (lifecycle._receiveOwnershipMutationAdmissions !== 0) {
			throw new ReplicationStatusSnapshotChangedError();
		}

		// Capture every synchronous policy/membership input before yielding. The
		// generation and ownership checks below reject the whole measurement if a
		// same-open role or range mutation races either asynchronous metric read.
		const defaultReplicaTarget = this.replicas.min.getValue(this);
		const isAdaptiveReplicating = this._isAdaptiveReplicating;
		const storageObjectiveBytes = isAdaptiveReplicating
			? this.replicationController?.maxMemoryLimit
			: undefined;
		const activeReplicators = this.uniqueReplicators.size;
		const [storageUsedBytes, rangeCoverage] = await Promise.all([
			this.getMemoryUsage(),
			this.calculateCoverage(),
		]);
		if (!this.isReplicationStatusLifecycleCurrent(lifecycle)) {
			throw new ClosedError();
		}
		const currentStorageObjectiveBytes = this._isAdaptiveReplicating
			? this.replicationController?.maxMemoryLimit
			: undefined;
		if (
			!lifecycle.isRoleCurrent(capturedRoleGeneration) ||
			!this.isReceiveOwnershipSnapshotStable(capturedOwnershipRevision) ||
			this.replicas.min.getValue(this) !== defaultReplicaTarget ||
			this._isAdaptiveReplicating !== isAdaptiveReplicating ||
			currentStorageObjectiveBytes !== storageObjectiveBytes ||
			this.uniqueReplicators.size !== activeReplicators
		) {
			throw new ReplicationStatusSnapshotChangedError();
		}
		const status = classifyReplicationStatus({
			storageUsedBytes,
			storageObjectiveBytes,
			rangeCoverage,
			defaultReplicaTarget,
			activeReplicators,
		});
		const previous = this._replicationStatus;
		this._replicationStatus = status;
		const transitioned =
			previous == null ||
			previous.reasons.length !== status.reasons.length ||
			previous.reasons.some(
				(reason, index) => reason !== status.reasons[index],
			);
		if (transitioned) {
			this.events.dispatchEvent(
				new CustomEvent<ReplicationStatusEvent>("replication:status", {
					detail: status,
				}),
			);
		}
		return status;
	}

	/**
	 * Measure local replication health. The snapshot and event are advisory:
	 * they are not persisted, sent to peers, or consumed by replication logic.
	 * Event emission is deduplicated by the ordered reason set, so metric drift
	 * within the same state does not create event traffic. Explicit measurements
	 * work without listeners; automatic refreshes run only while the status event
	 * has at least one listener.
	 */
	getReplicationStatus(): Promise<ReplicationStatus> {
		const lifecycle = this._instanceLifecycle;
		if (!this.isReplicationStatusLifecycleCurrent(lifecycle)) {
			return Promise.reject(new ClosedError());
		}
		const previous = this._replicationStatusReadTail ?? Promise.resolve();
		const operation = previous
			.catch(() => {})
			.then(async () => {
				let staleError: ReplicationStatusSnapshotChangedError | undefined;
				for (let attempt = 0; attempt < 2; attempt++) {
					try {
						return await this.measureReplicationStatus(lifecycle);
					} catch (error) {
						if (!(error instanceof ReplicationStatusSnapshotChangedError)) {
							throw error;
						}
						staleError = error;
						if (attempt === 0) {
							const mutationTail = this._replicationRangeMutationTail;
							await mutationTail.catch(() => {});
						}
					}
				}
				throw staleError!;
			});
		this._replicationStatusReadTail = operation.then(
			() => {},
			() => {},
		);
		return operation;
	}

	private scheduleReplicationStatusRefresh(): void {
		if (this.events.listenerCount("replication:status") === 0) {
			return;
		}
		const lifecycle = this._instanceLifecycle;
		if (!this.isReplicationStatusLifecycleCurrent(lifecycle)) {
			return;
		}
		if (this._replicationStatusRefreshScheduled) {
			this._replicationStatusRefreshDirty = true;
			return;
		}
		this._replicationStatusRefreshScheduled = true;
		queueMicrotask(() => {
			if (lifecycle !== this._instanceLifecycle) {
				return;
			}
			if (this.events.listenerCount("replication:status") === 0) {
				this._replicationStatusRefreshScheduled = false;
				this._replicationStatusRefreshDirty = false;
				return;
			}
			// Calls coalesced before the microtask starts are reflected by this scan.
			// Calls arriving while it is in flight set the bit again and receive one
			// follow-up scan for the latest state.
			this._replicationStatusRefreshDirty = false;
			if (!this.isReplicationStatusLifecycleCurrent(lifecycle)) {
				this._replicationStatusRefreshScheduled = false;
				return;
			}
			void this.getReplicationStatus()
				.catch((error) => {
					if (error instanceof ReplicationStatusSnapshotChangedError) {
						this._replicationStatusRefreshDirty = true;
						return;
					}
					if (
						this.isReplicationStatusLifecycleCurrent(lifecycle) &&
						!(error instanceof ClosedError) &&
						!isNotStartedError(error as Error)
					) {
						logger.error(error);
					}
				})
				.then(() => {
					if (lifecycle !== this._instanceLifecycle) {
						return;
					}
					this._replicationStatusRefreshScheduled = false;
					if (!this.isReplicationStatusLifecycleCurrent(lifecycle)) {
						this._replicationStatusRefreshDirty = false;
						return;
					}
					if (this._replicationStatusRefreshDirty) {
						this._replicationStatusRefreshDirty = false;
						this.scheduleReplicationStatusRefresh();
					}
				});
		});
	}

	private scheduleReplicationStatusRefreshForStorage(
		storageUsedBytes: number,
	): void {
		const objective = this.replicationController?.maxMemoryLimit;
		if (!this._isAdaptiveReplicating || objective == null) {
			return;
		}
		const exceeded = storageUsedBytes > objective;
		const wasExceeded = this._replicationStatus?.reasons.includes(
			"storage-objective-exceeded",
		);
		if (wasExceeded == null || wasExceeded !== exceeded) {
			this.scheduleReplicationStatusRefresh();
		}
	}

	/** Return a detached snapshot of effective shared-log runtime settings. */
	getRuntimeSnapshot(): SharedLogRuntimeSnapshot {
		const nativeGraph = this.log.entryIndex.properties.nativeGraph;
		const active = nativeGraph?.graph != null;
		return Object.freeze({
			nativeGraph: Object.freeze({
				active,
				useHeads: active && nativeGraph?.useHeads === true,
			}),
		});
	}

	/**
	 * Return a detached snapshot of the optional eager-response cache.
	 * Undefined means eager response retention is disabled for this log.
	 */
	getEagerBlockCacheTelemetry() {
		return this.remoteBlocks?.getEagerBlockCacheTelemetry();
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

	async onChange(
		change: Change<T>,
		ownershipLifecycleController?: AbortController,
	): Promise<void> {
		if (ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		const result = this.applyChange(change, {
			ownershipLifecycleController,
		});
		if (isPromiseLike(result)) {
			await result;
		}
		if (ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
	}

	private applyChange(
		change: Change<T>,
		options?: {
			deferCoordinateIndexDeletes?: boolean;
			ownershipLifecycleController?: AbortController;
		},
	): MaybePromise<string[] | undefined> {
		if (options?.ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				options.ownershipLifecycleController,
			);
		}
		if (options?.deferCoordinateIndexDeletes) {
			return this.applyChangeWithDeferredCoordinateDeletes(change, {
				ownershipLifecycleController: options.ownershipLifecycleController,
			});
		}
		for (const added of change.added) {
			this.onEntryAdded(added.entry);
		}
		if (change.removed.length === 0) {
			return undefined;
		}
		return this.applyRemovedChange(
			change.removed,
			options?.ownershipLifecycleController,
		);
	}

	private applyChangeWithDeferredCoordinateDeletes(
		change: Change<T>,
		options?: {
			forgetNativeCoordinates?: boolean;
			ownershipLifecycleController?: AbortController;
		},
	): string[] | undefined {
		if (options?.ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				options.ownershipLifecycleController,
			);
		}
		for (const added of change.added) {
			this.onEntryAdded(added.entry);
		}
		if (change.removed.length === 0) {
			return undefined;
		}
		const deferredCoordinateDeleteHashes = change.removed.map(
			(removed) => removed.hash,
		);
		this.onEntryRemovedHashes(deferredCoordinateDeleteHashes);
		this.scheduleDeadGidPeerHistoryReclaim(
			change.removed.map((removed) => removed.meta.gid),
		);
		if (options?.forgetNativeCoordinates === false) {
			this._coordinates.forgetResidentCoordinateStateForHashes(
				deferredCoordinateDeleteHashes,
			);
		} else {
			this._coordinates.forgetCoordinateStateForHashes(
				deferredCoordinateDeleteHashes,
			);
		}
		return deferredCoordinateDeleteHashes;
	}

	private materializePreparedAppendResultEntry(result: {
		entry?: Entry<T>;
		materializeEntry?: () => Entry<T>;
	}): Entry<T> {
		const entry = result.entry ?? result.materializeEntry?.();
		if (!entry) {
			throw new Error("Missing prepared append entry materializer");
		}
		return entry;
	}

	private applyPreparedAppendFactsWithDeferredCoordinateDeletes(
		appendFacts: PreparedAppendFacts,
		removed: ShallowOrFullEntry<T>[],
		materializeEntry: () => Entry<T>,
		options?: {
			forgetNativeCoordinates?: boolean;
			removedHashes?: string[];
			removedGids?: string[];
			ownershipLifecycleController?: AbortController;
		},
	): string[] | undefined {
		if (options?.ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				options.ownershipLifecycleController,
			);
		}
		this.onEntryAddedHash(appendFacts.hash, materializeEntry);
		const removedHashes = options?.removedHashes;
		if (
			removed.length === 0 &&
			(!removedHashes || removedHashes.length === 0)
		) {
			return undefined;
		}
		const deferredCoordinateDeleteHashes = removedHashes
			? normalizedHashValues(removedHashes)
			: removed.map((entry) => entry.hash);
		this.onEntryRemovedHashes(deferredCoordinateDeleteHashes);
		this.scheduleDeadGidPeerHistoryReclaim(
			removed.length > 0
				? removed.map((entry) => entry.meta.gid)
				: options?.removedGids,
		);
		if (options?.forgetNativeCoordinates === false) {
			this._coordinates.forgetResidentCoordinateStateForHashes(
				deferredCoordinateDeleteHashes,
			);
		} else {
			this._coordinates.forgetCoordinateStateForHashes(
				deferredCoordinateDeleteHashes,
			);
		}
		return deferredCoordinateDeleteHashes;
	}

	private async applyRemovedChange(
		removedEntries: ShallowOrFullEntry<T>[],
		ownershipLifecycleController?: AbortController,
	): Promise<undefined> {
		for (const removed of removedEntries) {
			if (ownershipLifecycleController) {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
			await this._coordinates.deleteCoordinates(
				{ hash: removed.hash },
				ownershipLifecycleController,
			);
			if (ownershipLifecycleController) {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
			this.onEntryRemoved(removed.hash);
		}
		this.scheduleDeadGidPeerHistoryReclaim(
			removedEntries.map((removed) => removed.meta.gid),
		);
		return undefined;
	}

	private async reclaimDeadGidPeerHistory(
		gids: Iterable<string> | undefined,
		history = this._gidPeersHistory,
	): Promise<void> {
		if (!gids) return;
		const candidates = [...new Set(gids)].filter(
			(gid) => history === this._gidPeersHistory && !!gid && history.has(gid),
		);
		if (candidates.length === 0) return;
		const hasHeads = await this.hasAnyHeadForGidSets(
			candidates.map((gid) => [gid]),
		);
		if (history !== this._gidPeersHistory) return;
		for (let index = 0; index < candidates.length; index++) {
			const gid = candidates[index]!;
			if (!hasHeads[index] && history.has(gid)) {
				this.deleteGidPeerHistory(gid);
			}
		}
	}

	private resetGidPeerHistoryCleanupState(): void {
		this._gidPeerHistoryCleanupState = {
			history: this._gidPeersHistory,
			tail: Promise.resolve(),
			pending: new Set(),
			draining: false,
			highWater: 0,
		};
	}

	private scheduleDeadGidPeerHistoryReclaim(
		gids: Iterable<string> | undefined,
	): void {
		if (!gids) return;
		const state = this._gidPeerHistoryCleanupState;
		const history = state.history;
		for (const gid of gids) {
			if (!gid || !history.has(gid)) continue;
			if (state.pending.has(gid)) continue;
			if (state.pending.size >= GID_PEER_HISTORY_CLEANUP_PENDING_CAPACITY) {
				// History is only a suppression memo. Under sustained synchronous
				// production, forgetting an overflow row is the bounded, safe fallback:
				// it can cause redundant delivery but cannot affect ownership or data.
				this.deleteGidPeerHistory(gid);
				continue;
			}
			state.pending.add(gid);
		}
		state.highWater = Math.max(state.highWater, state.pending.size);
		if (state.pending.size === 0) return;
		// A macrotask gate lets sequential awaited appends coalesce; a Promise
		// microtask runs before the caller can enqueue the next trimmed gid. The
		// fixed early-wake threshold also amortizes latest-only (retain=1) logs,
		// while the separate pending cap remains the absolute memory bound.
		const wakeThreshold = GID_PEER_HISTORY_CLEANUP_BATCH_SIZE;
		if (state.draining) {
			if (state.pending.size >= wakeThreshold) state.wake?.();
			return;
		}
		state.draining = true;
		let wake!: () => void;
		const batchReady = new Promise<void>((resolve) => {
			let settled = false;
			let cancel = () => {};
			wake = () => {
				if (settled) return;
				settled = true;
				cancel();
				if (state.wake === wake) state.wake = undefined;
				resolve();
			};
			state.wake = wake;
			if (typeof setImmediate === "function") {
				const handle = setImmediate(wake);
				cancel = () => clearImmediate(handle);
			} else {
				const handle = setTimeout(wake, 0);
				cancel = () => clearTimeout(handle);
			}
		});
		if (state.pending.size >= wakeThreshold) wake();
		const drain = state.tail
			.then(async () => {
				await batchReady;
				while (history === this._gidPeersHistory) {
					const batch = [...state.pending].slice(
						0,
						GID_PEER_HISTORY_CLEANUP_BATCH_SIZE,
					);
					if (batch.length === 0) break;
					for (const gid of batch) state.pending.delete(gid);
					try {
						await this.reclaimDeadGidPeerHistory(batch, history);
					} catch (error) {
						warn(`Failed to reclaim gid peer history: ${String(error)}`);
						if (history === this._gidPeersHistory) {
							for (const gid of batch) this.deleteGidPeerHistory(gid);
						}
					}
				}
			})
			.catch((error) => {
				warn(`Failed to reclaim gid peer history: ${String(error)}`);
			});
		state.tail = drain.finally(() => {
			state.draining = false;
			if (history !== this._gidPeersHistory) {
				state.pending.clear();
			}
			if (
				history === this._gidPeersHistory &&
				state.pending.size > 0 &&
				state === this._gidPeerHistoryCleanupState
			) {
				this.scheduleDeadGidPeerHistoryReclaim([]);
			}
		});
	}

	private receiveSignatureVerificationResult(
		entry: Entry<T>,
	): boolean | undefined {
		return getReceiveSignatureVerificationResult(
			this._receiveSignatureVerificationFacts,
			entry,
		);
	}

	private async preverifyReceiveSignaturesBatch(
		entries: Entry<T>[],
		profile?: SyncProfileFn,
	): Promise<ReceiveSignatureVerificationFact<T>[] | undefined> {
		const candidates = entries.filter(
			(entry) => !entry.createdLocally && !hasPreverifiedSignature(entry),
		);
		if (candidates.length < NATIVE_ED25519_VERIFY_BATCH_MIN_ENTRIES) {
			return undefined;
		}

		let storageBytes: Uint8Array[];
		let signableInputs: Ed25519VerifyBatchInput[] | undefined;
		try {
			// Capture the callback-visible storage state and native signable inputs
			// before the first await so the result is bound to one entry state.
			storageBytes = candidates.map((entry) => entry.getStorageBytes().slice());
			signableInputs =
				this.prepareReceiveNativeEd25519VerificationBatch(candidates);
		} catch {
			return undefined;
		}
		if (!signableInputs) {
			return undefined;
		}

		const verifyStartedAt = syncProfileStart(profile);
		let verified: boolean[] | undefined;
		try {
			verified = await verifyEd25519Batch(signableInputs);
		} catch {
			verified = undefined;
		}
		if (!verified || verified.length !== candidates.length) {
			return undefined;
		}
		if (profile) {
			emitSyncProfileDuration(profile, verifyStartedAt, {
				name: "sharedLog.canAppendBatch.verifySignatures",
				component: "shared-log",
				entries: candidates.length,
				messages: 1,
				details: {
					native: true,
					mode: "signable",
					programCanAppendDeferred: true,
				},
			});
		}
		return candidates.map((entry, index) => ({
			entry,
			storageBytes: storageBytes[index]!,
			verified: verified[index]!,
		}));
	}

	private prepareReceiveNativeEd25519VerificationBatch(
		entries: Entry<T>[],
	): Ed25519VerifyBatchInput[] | undefined {
		const inputs = this.prepareNativeEd25519VerificationBatch(entries);
		return inputs?.map((input) => ({
			signature: input.signature.slice(),
			publicKey: input.publicKey.slice(),
			message: input.message.slice(),
		}));
	}

	private async withReceiveSignatureVerificationFacts<R>(
		facts: ReceiveSignatureVerificationFact<T>[] | undefined,
		operation: () => Promise<R>,
	): Promise<R> {
		return withSignatureVerificationFacts(
			this._receiveSignatureVerificationFacts,
			facts,
			operation,
		);
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

			const receiveSignatureVerification =
				this.receiveSignatureVerificationResult(entry);
			if (receiveSignatureVerification === false) {
				return false;
			}

			// Locally-created entries were signed before append. Raw prepared entries
			// can carry a durable verifier fact from their decoder; plain receive facts
			// are scoped to this lower-log join and checked against the exact bytes.
			if (
				!entry.createdLocally &&
				!hasPreverifiedSignature(entry) &&
				receiveSignatureVerification !== true &&
				!(await entry.verifySignatures())
			) {
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

	private prepareNativeEd25519VerificationBatch(
		entries: Entry<T>[],
	): Ed25519VerifyBatchInput[] | undefined {
		const inputs: Ed25519VerifyBatchInput[] = [];
		for (const entry of entries) {
			let signatures;
			try {
				signatures = entry.signatures;
			} catch {
				return undefined;
			}
			if (signatures.length !== 1) {
				return undefined;
			}
			const signature = signatures[0]!;
			if (
				!(signature.publicKey instanceof Ed25519PublicKey) ||
				signature.prehash !== 0
			) {
				return undefined;
			}
			try {
				inputs.push({
					signature: signature.signature,
					publicKey: signature.publicKey.publicKey,
					message: entry.getSignableBytes(),
				});
			} catch {
				return undefined;
			}
		}
		return inputs;
	}

	private async canAppendBatch(
		entries: Entry<T>[],
		profile?: SyncProfileFn,
		options?: { decodedReplicaCounts?: DecodedReplicaCountMap },
	) {
		try {
			const signaturesToVerify: Entry<T>[] = [];
			const checkStartedAt = syncProfileStart(profile);
			let replicaCacheHits = 0;
			let predecodedReplicaHits = 0;
			for (const entry of entries) {
				if (!entry.meta.data) {
					warn("Received entry without meta data, skipping");
					return false;
				}
				let replicas: number;
				if (options?.decodedReplicaCounts?.has(entry.hash)) {
					replicas = options.decodedReplicaCounts.get(entry.hash)!;
					replicaCacheHits++;
				} else {
					const predecodedReplicas =
						getPreparedRawExchangeRequestedReplicas(entry);
					if (predecodedReplicas != null) {
						replicas = predecodedReplicas;
						predecodedReplicaHits++;
					} else {
						replicas = decodeReplicas(entry).getValue(this);
					}
				}
				if (Number.isFinite(replicas) === false) {
					return false;
				}

				checkMinReplicasLimit(replicas);

				if (!entry.createdLocally && !hasPreverifiedSignature(entry)) {
					signaturesToVerify.push(entry);
				}
			}
			if (profile) {
				emitSyncProfileDuration(profile, checkStartedAt, {
					name: "sharedLog.canAppendBatch.metadata",
					component: "shared-log",
					entries: entries.length,
					count: signaturesToVerify.length,
					messages: 1,
					details: { replicaCacheHits, predecodedReplicaHits },
				});
			}
			if (signaturesToVerify.length === 0) {
				return true;
			}
			const verifyStartedAt = syncProfileStart(profile);
			let native = false;
			let nativeMode: "entry-v0" | "signable" | undefined;
			let verified: boolean[] | undefined;
			if (
				signaturesToVerify.length >= NATIVE_ED25519_VERIFY_BATCH_MIN_ENTRIES
			) {
				try {
					verified =
						await verifyEntryV0Ed25519BatchFromEntries(signaturesToVerify);
					native = !!verified;
					nativeMode = verified ? "entry-v0" : undefined;
				} catch {
					verified = undefined;
				}
				if (!verified) {
					const nativeInputs =
						this.prepareNativeEd25519VerificationBatch(signaturesToVerify);
					if (nativeInputs) {
						try {
							verified = await verifyEd25519Batch(nativeInputs);
							native = !!verified;
							nativeMode = verified ? "signable" : undefined;
						} catch {
							verified = undefined;
						}
					}
				}
			}
			verified ??= await Promise.all(
				signaturesToVerify.map((entry) => entry.verifySignatures()),
			);
			if (profile) {
				emitSyncProfileDuration(profile, verifyStartedAt, {
					name: "sharedLog.canAppendBatch.verifySignatures",
					component: "shared-log",
					entries: signaturesToVerify.length,
					messages: 1,
					details: { native, mode: nativeMode },
				});
			}
			return verified.every(Boolean);
		} catch (error) {
			if (error instanceof BorshError || error instanceof ReplicationError) {
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}
	}

	private validatePreparedRawReceiveMetadataWithNativeBackbone(
		entries: Entry<T>[],
		profile?: SyncProfileFn,
		options?: { decodedReplicaCounts?: DecodedReplicaCountMap },
	): { signatureHashes: string[] } | false | undefined {
		if (!this._nativeBackbone?.graph.verifyPreparedRawReceiveEntries) {
			return undefined;
		}
		try {
			const signatureHashes: string[] = [];
			const checkStartedAt = syncProfileStart(profile);
			let replicaCacheHits = 0;
			let predecodedReplicaHits = 0;
			for (const entry of entries) {
				if (!entry.meta.data) {
					warn("Received entry without meta data, skipping");
					return false;
				}
				let replicas: number;
				if (options?.decodedReplicaCounts?.has(entry.hash)) {
					replicas = options.decodedReplicaCounts.get(entry.hash)!;
					replicaCacheHits++;
				} else {
					const predecodedReplicas =
						getPreparedRawExchangeRequestedReplicas(entry);
					if (predecodedReplicas != null) {
						replicas = predecodedReplicas;
						predecodedReplicaHits++;
					} else {
						replicas = decodeReplicas(entry).getValue(this);
					}
				}
				if (Number.isFinite(replicas) === false) {
					return false;
				}

				checkMinReplicasLimit(replicas);

				if (!entry.createdLocally && !hasPreverifiedSignature(entry)) {
					signatureHashes.push(entry.hash);
				}
			}

			if (profile) {
				emitSyncProfileDuration(profile, checkStartedAt, {
					name: "sharedLog.canAppendBatch.metadata",
					component: "shared-log",
					entries: entries.length,
					count: signatureHashes.length,
					messages: 1,
					details: { replicaCacheHits, predecodedReplicaHits },
				});
			}
			return { signatureHashes };
		} catch (error) {
			if (error instanceof BorshError || error instanceof ReplicationError) {
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			return undefined;
		}
	}

	private validatePreparedRawReceiveHeadsMetadataWithNativeBackbone(
		heads: EntryWithRefs<T>[],
		profile?: SyncProfileFn,
		options?: { decodedReplicaCounts?: DecodedReplicaCountMap },
	): { signatureHashes: string[] } | false | undefined {
		if (!this._nativeBackbone?.graph.verifyPreparedRawReceiveEntries) {
			return undefined;
		}
		try {
			const signatureHashes: string[] = [];
			const checkStartedAt = syncProfileStart(profile);
			let replicaCacheHits = 0;
			let predecodedReplicaHits = 0;
			for (const head of heads) {
				const hash = getExchangeHeadHash(head);
				const shallow = getPreparedRawExchangeHeadShallowEntry(head);
				const metaData = shallow?.meta.data ?? head.entry.meta.data;
				if (!metaData) {
					warn("Received entry without meta data, skipping");
					return false;
				}
				let replicas: number;
				if (options?.decodedReplicaCounts?.has(hash)) {
					replicas = options.decodedReplicaCounts.get(hash)!;
					replicaCacheHits++;
				} else {
					const predecodedReplicas =
						getPreparedRawExchangeHeadRequestedReplicas(head);
					if (predecodedReplicas != null) {
						replicas = predecodedReplicas;
						predecodedReplicaHits++;
					} else {
						replicas = decodeReplicas({ meta: { data: metaData } }).getValue(
							this,
						);
					}
				}
				if (Number.isFinite(replicas) === false) {
					return false;
				}

				checkMinReplicasLimit(replicas);

				const preparedSignatureVerified =
					getPreparedRawExchangeHeadSignatureVerified(head);
				if (preparedSignatureVerified === true) {
					continue;
				}
				if (preparedSignatureVerified === false) {
					signatureHashes.push(hash);
					continue;
				}
				const entry = head.entry;
				if (!entry.createdLocally && !hasPreverifiedSignature(entry)) {
					signatureHashes.push(hash);
				}
			}

			if (profile) {
				emitSyncProfileDuration(profile, checkStartedAt, {
					name: "sharedLog.canAppendBatch.metadata",
					component: "shared-log",
					entries: heads.length,
					count: signatureHashes.length,
					messages: 1,
					details: { replicaCacheHits, predecodedReplicaHits },
				});
			}
			return { signatureHashes };
		} catch (error) {
			if (error instanceof BorshError || error instanceof ReplicationError) {
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			return undefined;
		}
	}

	private canAppendPreparedRawReceiveBatchWithNativeBackbone(
		entries: Entry<T>[],
		profile?: SyncProfileFn,
		options?: { decodedReplicaCounts?: DecodedReplicaCountMap },
	): boolean | undefined {
		const verifier =
			this._nativeBackbone?.graph.verifyPreparedRawReceiveEntries;
		const validated = this.validatePreparedRawReceiveMetadataWithNativeBackbone(
			entries,
			profile,
			options,
		);
		if (!verifier || !validated) {
			return validated === false ? false : undefined;
		}
		if (validated.signatureHashes.length === 0) {
			return true;
		}
		try {
			const verifyStartedAt = syncProfileStart(profile);
			const verified = verifier.call(
				this._nativeBackbone!.graph,
				validated.signatureHashes,
			);
			if (!verified || verified.length !== validated.signatureHashes.length) {
				return undefined;
			}
			if (profile) {
				emitSyncProfileDuration(profile, verifyStartedAt, {
					name: "sharedLog.canAppendBatch.verifySignatures",
					component: "shared-log",
					entries: validated.signatureHashes.length,
					messages: 1,
					details: { native: true, mode: "backbone-prepared" },
				});
			}
			return verified.every(Boolean);
		} catch (error) {
			if (error instanceof BorshError || error instanceof ReplicationError) {
				warn("Received payload that could not be decoded, skipping");
				return false;
			}
			return undefined;
		}
	}

	private async canSkipLowerLogCanAppendForNetworkJoin(
		entries: Entry<T>[],
		profile?: SyncProfileFn,
		options?: { decodedReplicaCounts?: DecodedReplicaCountMap },
	): Promise<boolean> {
		if (entries.length === 0 || this._logProperties?.canAppend) {
			return false;
		}
		const nativeBackboneValidated =
			this.canAppendPreparedRawReceiveBatchWithNativeBackbone(
				entries,
				profile,
				options,
			);
		if (nativeBackboneValidated !== undefined) {
			return nativeBackboneValidated;
		}
		return this.canAppendBatch(entries, profile, options);
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
				const directPeers: Map<string, unknown> | undefined = (
					this.node.services.pubsub as any
				)?.peers;

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

	private async _close(options?: { preserveDropRetryResources?: boolean }) {
		this.stopRepairLifecycle();
		this._instanceLifecycle?.beginTerminal("internal-close");
		this.resetReplicationStatusLifecycle();
		const preserveDropRetryResources =
			options?.preserveDropRetryResources === true;
		let firstError: unknown;
		const capture = async (operation: () => Promise<unknown> | unknown) => {
			try {
				await operation();
			} catch (error) {
				firstError ??= error;
			}
		};
		const captureSync = (operation: () => unknown) => {
			try {
				operation();
			} catch (error) {
				firstError ??= error;
			}
		};
		captureSync(() => {
			if (this._wireSyncSession) {
				this._wireSyncSession.unregisterTopic(this.topic);
				this._wireSyncSession = undefined;
			}
		});
		await capture(() =>
			this._coordinates.closeNativeBackboneCoordinatePersistence(),
		);
		await capture(() => this.syncronizer?.close());

		captureSync(() => {
			for (const [_key, peerMap] of this.pendingMaturity ?? []) {
				for (const [_key2, info] of peerMap) clearTimeout(info.timeout);
				peerMap.clear();
			}
			this.pendingMaturity?.clear();
			this.distributeQueue?.clear();
		});
		captureSync(() => this._closeFanoutChannel());
		captureSync(() => this._providerHandle?.close());
		this._providerHandle = undefined;
		captureSync(() => {
			this.coordinateToHash?.clear();
			this.recentlyRebalanced?.clear();
			this.uniqueReplicators?.clear();
			this._topicSubscribersCache?.clear();
			this._localReachablePeerHashesCache?.clear();
			this._closeController.abort();
			clearInterval(this.interval);
			this._liveness?.stopReplicatorLivenessSweep();
		});
		captureSync(() =>
			this.node.services.pubsub.removeEventListener(
				"subscribe",
				this._onSubscriptionFn,
			),
		);
		captureSync(() =>
			this.node.services.pubsub.removeEventListener(
				"unsubscribe",
				this._onUnsubscriptionFn,
			),
		);
		captureSync(() => {
			this.joinWarmup.cancelAllJoinWarmupTargets();
			this.clearCheckedPruneAuditTimer();
			for (const timer of this._repairRetryTimers ?? []) clearTimeout(timer);
			this._repairRetryTimers?.clear();
			this._recentRepairDispatch?.clear();
			this._repairSweepRunning = false;
			this._repairSweepPendingModes?.clear();
			for (const peers of this._repairSweepPendingPeersByMode?.values() ?? [])
				peers.clear();
			this.joinWarmup?.clearRepairSweepWarmupSessions();
			this._repairSweepOptimisticGidPeersPending?.clear();
			this._repairSweepOptimisticGidsByPeer?.clear();
			this._entryKnownPeers?.clear();
			this._entryKnownPeerObservedAt?.clear();
			this._nativeSharedLogState?.clearEntryKnownPeers();
			this._nativeBackbone?.clearEntryKnownPeers();
			for (const timer of this._joinAuthoritativeRepairTimersByDelay?.values() ??
				[])
				clearTimeout(timer);
			this._joinAuthoritativeRepairTimersByDelay?.clear();
			this._joinAuthoritativeRepairPeersByDelay?.clear();
			for (const targets of this._repairFrontierByMode?.values() ?? [])
				targets.clear();
			for (const targets of this._repairFrontierActiveTargetsByMode?.values() ??
				[])
				targets.clear();
			for (const targets of this._repairFrontierBypassKnownPeersByMode?.values() ??
				[])
				targets.clear();
			if (this._appendBackfillTimer) {
				clearTimeout(this._appendBackfillTimer);
				this._appendBackfillTimer = undefined;
			}
			this._appendBackfillPendingByTarget?.clear();
			for (const [_key, value] of this._pendingIHave ?? []) value.clear();
			if (this._pendingIHaveExpiryTimer) {
				clearTimeout(this._pendingIHaveExpiryTimer);
				this._pendingIHaveExpiryTimer = undefined;
				this._pendingIHaveExpiryDeadline = Number.POSITIVE_INFINITY;
			}
		});
		captureSync(() => this._checkedPrune.close());

		if (!preserveDropRetryResources) {
			await capture(() => this.remoteBlocks?.stop?.());
		}
		captureSync(() => {
			this._pendingIHave?.clear();
			this._pendingIHaveCallbacks?.clear();
			this._peerSessions?.clearReceiveEpochsForClose();
			this._peerSessions?.clearCleanupGatesForClose();
			this._activeReceiveHandlersByPeer?.clear();
			this._receiveHandlerDrainByPeer?.clear();
			this._openingSyncCapabilitiesByPeer?.clear();
			this._gidPeersHistory?.clear();
			this._peerSyncCapabilities?.clear();
			this._peerSyncCapabilitySessions?.clear();
			this._peerSyncCapabilityTimestamps?.clear();
			this._persistedReceiptReadinessGenerations = new WeakMap();
			this._persistedReceiptStorage = undefined;
			this._persistedReceiptRequestsInFlight?.clear();
			this._persistedReceiptRequestsInFlightTotal = 0;
			this._v2Receive?.clearForClose();
			this._v2Send?.clearForClose();
			this._liveRawGossipBatches?.clear();
			this._nativeSharedLogState?.clearGidPeers();
			this._nativeBackbone?.clearGidPeers();
			this._replicationRangeMutationTail = Promise.resolve();
		});
		// Cancel every debounce independently so one faulty close hook cannot keep
		// the remaining timers or indexes alive.
		captureSync(() => this.rebalanceParticipationDebounced?.close());
		captureSync(() => this.replicationChangeDebounceFn?.close?.());
		captureSync(() => this.pruneDebouncedFn?.close?.());
		this.pruneDebouncedFn = undefined as any;
		this.rebalanceParticipationDebounced = undefined;
		if (!preserveDropRetryResources) {
			const stopIndex = async (
				index: Index<any> | undefined,
				forget: () => void,
			) => {
				if (!index) {
					return;
				}
				try {
					await index.stop?.();
					forget();
				} catch (error) {
					firstError ??= error;
				}
			};
			await stopIndex(this._replicationRangeIndex, () => {
				this._replicationRangeIndex = undefined as any;
			});
			await stopIndex(this._entryCoordinatesIndex, () => {
				this._entryCoordinatesIndex = undefined as any;
			});
		}
		this._nativeRangePlanner = undefined;
		this._nativeSharedLogState = undefined;
		this._coordinates._residentEntryCoordinatesByHash = undefined;
		captureSync(() => this.cpuUsage?.stop?.());

		if (firstError !== undefined) {
			throw firstError;
		}
	}

	private classifyTerminalOwnership(
		from?: Program,
	): "terminal" | "nonterminal" {
		if (this.closed) {
			return "terminal";
		}
		const parentIndex =
			this.parents?.findIndex((parent) => parent === from) ?? -1;
		if (from && parentIndex === -1) {
			throw new TerminalOperationNotStartedError(
				"Could not find from in parents",
			);
		}
		return parentIndex !== -1 && (this.parents?.length ?? 0) > 1
			? "nonterminal"
			: "terminal";
	}

	async close(from?: Program): Promise<boolean> {
		if (this.classifyTerminalOwnership(from) === "nonterminal") {
			return super.close(from);
		}
		this.throwIfCheckedPruneRemoveBlocksLocalOperation("close");
		// Match Program.end()'s synchronous terminal admission fence before any
		// SharedLog-specific await or observable teardown can admit a new owner.
		this.preventParentAttachments();
		this.stopRepairLifecycle();
		this._instanceLifecycle?.beginTerminal("close");
		this.resetReplicationStatusLifecycle();
		this._v2Receive?.clearForClose();
		const replicationRangeTerminalFence =
			this.acquireReplicationRangeMutationTerminalFence();
		const pruneRemoveTerminalFence = this.acquirePruneRemoveTerminalFence();
		try {
			this.stopSubscriptionChangeCallbackAdmission();
			this.joinWarmup.cancelAllJoinWarmupTargets();
			await this.drainSubscriptionChangeCallbacks();
			// An already-admitted subscription callback can create a fresh warmup
			// generation while the first cancellation is draining.
			this.joinWarmup.cancelAllJoinWarmupTargets();
			await this.drainReceiveHandlers();
			await this.drainReplicationInfoApplyQueues();
			await replicationRangeTerminalFence.drained;
			await pruneRemoveTerminalFence.drained;
			await this.drainPendingIHaveCallbacks();
			this.ensureNativeDurabilityRuntimeState();
		} catch (error) {
			// The terminal preamble has already disabled parent attachments and the
			// network lifecycle. Keep mutation admission fenced for an exact retry.
			throw error;
		}
		// Best-effort: announce that we are going offline before tearing down
		// RPC/subscription state.
		//
		// Important: do not delete our local replication ranges here. Keeping them
		// allows `replicate: { type: "resume" }` to restore the previous role on
		// restart. Explicit `unreplicate()` still clears local state.
		try {
			if (!this.closed) {
				// Ship any coalesced live gossip before the RPC child program
				// closes; entries appended right before close should still be
				// offered to their replicators (best effort, like the inline
				// sends they replaced).
				this.flushLiveRawGossip();
				// Prevent any late debounced timers (rebalance/prune) from publishing
				// replication info after we announce "segments: []". These races can leave
				// stale segments on remotes after rapid open/close cycles.
				this._isReplicating = false;
				this._isAdaptiveReplicating = false;
				this.rebalanceParticipationDebounced?.close();
				this.replicationChangeDebounceFn?.close?.();
				this.pruneDebouncedFn?.close?.();

				// Ensure the "I'm leaving" replication reset is actually published before
				// the RPC child program closes and unsubscribes from its topic. If we fire
				// and forget here, the publish can race with `super.close()` and get dropped,
				// leaving stale replication segments on remotes (flaky join/leave tests).
				// Also ensure close is bounded even when shard overlays are mid-reconcile.
				const abort = new AbortController();
				const abortTimer = setTimeout(() => {
					try {
						abort.abort(
							new TimeoutError("shared-log close replication reset timed out"),
						);
					} catch {
						abort.abort();
					}
				}, 2_000);
				try {
					await this._v2Send.sendTerminalReset(abort.signal);
				} finally {
					clearTimeout(abortTimer);
				}
			}
		} catch {
			// ignore: close should be resilient even if we were never fully started
		}
		let firstError: unknown;
		let superClosed = false;
		try {
			superClosed = await super.close(from);
		} catch (error) {
			if (!this.closed || this.pendingTerminalOperation !== "close") {
				// Child/base admission failed before Program committed this terminal
				// transition (including a cleanly closed instance). Lower resources are
				// still live data or belong to a completed generation, so do not mutate
				// them while merely propagating the base error.
				throw error;
			}
			firstError = error;
		}
		if (!superClosed && firstError === undefined) {
			return false;
		}
		this._nativeStrictDurableTransactionsClosing = true;
		let strictTransactionsSettled = false;
		try {
			await this.settleNativeStrictDurableTransactionsForClose();
			strictTransactionsSettled = true;
		} catch (error) {
			firstError ??= error;
		}
		if (!strictTransactionsSettled) {
			throw firstError;
		}
		try {
			await this.log.close();
		} catch (error) {
			firstError ??= error;
		}
		try {
			await this._close();
		} catch (error) {
			firstError ??= error;
		}
		if (firstError !== undefined) {
			throw firstError;
		}
		return true;
	}

	async drop(from?: Program): Promise<boolean> {
		if (this.classifyTerminalOwnership(from) === "nonterminal") {
			return super.drop(from);
		}
		this.throwIfCheckedPruneRemoveBlocksLocalOperation("drop");
		this.ensureNativeDurabilityRuntimeState();
		const nativePersistence =
			this._coordinates._nativeBackboneCoordinatePersistence;
		if (
			nativePersistence &&
			(typeof nativePersistence.drop !== "function" ||
				typeof nativePersistence.resumeDrop !== "function" ||
				nativePersistence.supportsDrop !== true ||
				nativePersistence.dropIsTerminal !== true)
		) {
			// Reject before `super.drop()` can drop child programs or any lower index.
			throw new TerminalOperationNotStartedError(
				"NativeBackbone coordinate persistence adapters must expose a terminal underlying drop capability and resumeDrop before SharedLog.drop() can erase their namespace",
			);
		}
		// Adapter capability validation above is explicitly unstarted. Establish
		// the terminal fence only after that precondition succeeds.
		this.preventParentAttachments();
		this.stopRepairLifecycle();
		this._instanceLifecycle?.beginTerminal("drop");
		this.resetReplicationStatusLifecycle();
		this._v2Receive?.clearForClose();
		const replicationRangeTerminalFence =
			this.acquireReplicationRangeMutationTerminalFence();
		const pruneRemoveTerminalFence = this.acquirePruneRemoveTerminalFence();
		try {
			this.stopSubscriptionChangeCallbackAdmission();
			this.joinWarmup.cancelAllJoinWarmupTargets();
			await this.drainSubscriptionChangeCallbacks();
			// An already-admitted subscription callback can create a fresh warmup
			// generation while the first cancellation is draining.
			this.joinWarmup.cancelAllJoinWarmupTargets();
			await this.drainReceiveHandlers();
			await this.drainReplicationInfoApplyQueues();
			await replicationRangeTerminalFence.drained;
			await pruneRemoveTerminalFence.drained;
			await this.drainPendingIHaveCallbacks();
		} catch (error) {
			// The terminal preamble is not safely reversible. Preserve the fence until
			// a retry finishes cleanup.
			throw error;
		}
		// Best-effort: announce that we are going offline before tearing down
		// RPC/subscription state (same reasoning as in `close()`).
		try {
			if (!this.closed) {
				this.flushLiveRawGossip();
				this._isReplicating = false;
				this._isAdaptiveReplicating = false;
				this.rebalanceParticipationDebounced?.close();
				this.replicationChangeDebounceFn?.close?.();
				this.pruneDebouncedFn?.close?.();

				const abort = new AbortController();
				const abortTimer = setTimeout(() => {
					try {
						abort.abort(
							new TimeoutError("shared-log drop replication reset timed out"),
						);
					} catch {
						abort.abort();
					}
				}, 2_000);
				try {
					await this._v2Send.sendTerminalReset(abort.signal);
				} finally {
					clearTimeout(abortTimer);
				}
			}
		} catch {
			// ignore: drop should be resilient even if we were never fully started
		}

		let firstError: unknown;
		let superDropped = false;
		try {
			superDropped = await super.drop(from);
		} catch (error) {
			if (!this.closed || this.pendingTerminalOperation !== "drop") {
				// A fresh drop on a cleanly closed Program is an API rejection, not
				// permission to erase the already-closed lower log. Likewise, a child
				// failure before the base drop commits must leave all lower data intact.
				throw error;
			}
			firstError = error;
		}
		if (!superDropped && firstError === undefined) {
			return false;
		}
		this._nativeStrictDurableTransactionsClosing = true;
		const capture = async (operation: () => Promise<unknown> | unknown) => {
			try {
				await operation();
			} catch (error) {
				firstError ??= error;
			}
		};
		let strictTransactionsSettled = false;
		try {
			await this.settleNativeStrictDurableTransactionsForClose();
			strictTransactionsSettled = true;
		} catch (error) {
			firstError ??= error;
		}
		if (!strictTransactionsSettled) {
			throw firstError;
		}
		if (nativePersistence) {
			try {
				const additionalFiles = this._nativeBackboneCoordinatePersistenceStore
					? NATIVE_STRICT_DURABLE_TRANSACTION_INTENT_FILES
					: [];
				if (this._nativeBackboneDropStarted) {
					let resumed: boolean;
					try {
						resumed = await nativePersistence.resumeDrop!();
					} catch (resumeError) {
						try {
							// A corrupt or partial tombstone deliberately restores the
							// adapter to active so explicit drop can overwrite it. For
							// transient read/remove failures the adapter stays dropping,
							// this fallback rejects, and the original recovery error wins.
							await nativePersistence.drop!(additionalFiles);
							resumed = true;
						} catch (restartError) {
							throw new AggregateError(
								[resumeError, restartError],
								"Failed to resume or restart native backbone drop",
							);
						}
					}
					if (!resumed) {
						await nativePersistence.drop!(additionalFiles);
					}
				} else {
					this._nativeBackboneDropStarted = true;
					await nativePersistence.drop!(additionalFiles);
				}
			} catch (error) {
				firstError ??= error;
				// Quiesce the failed generation without closing the lower block/index
				// handles: exact drop retry still owns their destructive cleanup.
				await capture(() => this.log.close());
				await capture(() => this._close({ preserveDropRetryResources: true }));
				throw firstError;
			}
			// These in-memory states only stop being recovery-authoritative after all
			// six persistence files and both alternating intent slots are durably gone.
			this._nativeStrictDurableTransactionJournalState = undefined;
			this._nativeStrictDurableDocumentRecoveryDeferred = false;
			this._nativeStrictDurableTransactionTail = undefined;
			this._nativeStrictDurableTransactions?.clear();
			this.clearNativeStrictDurableTransactionFailure();
			this._nativeDurableCommitFailure = undefined;
			this._nativeDurableRecoveryReadyForReopen = false;
			this._nativeDurableRecoveryCids.clear();
		}
		let destructiveCleanupFailed = false;
		const dropIndex = async (
			index: Index<any> | undefined,
			forget: () => void,
		) => {
			if (!index) {
				return;
			}
			try {
				await index.drop();
				forget();
			} catch (error) {
				firstError ??= error;
				destructiveCleanupFailed = true;
			}
		};
		await dropIndex(this._entryCoordinatesIndex, () => {
			this._entryCoordinatesIndex = undefined as any;
		});
		await dropIndex(this._replicationRangeIndex, () => {
			this._replicationRangeIndex = undefined as any;
		});
		try {
			await this.log.drop();
		} catch (error) {
			firstError ??= error;
			destructiveCleanupFailed = true;
		}
		if (destructiveCleanupFailed) {
			// Exact drop retry still owns every failed destructive handle. Quiesce
			// the rest of the generation without turning an erase failure into a
			// successful close or forgetting the only object that can retry it.
			await capture(() => this._close({ preserveDropRetryResources: true }));
			throw firstError;
		}
		await capture(() => this._close());
		if (firstError !== undefined) {
			throw firstError;
		}
		return true;
	}

	async recover(): Promise<void> {
		return this.log.recover();
	}

	/**
	 * Receive-fusion resolver passed to the RPC controller: when the native
	 * wire decoder stashed this message's raw exchange-head payload (keyed by
	 * the DataMessage id), build the message from stash metadata instead of
	 * borsh-decoding the entries in JS. The block bytes stay in wasm memory
	 * for the stashed prepare pipeline.
	 */
	private resolveStashedRawExchangeHeadsMessage(
		message: DataMessage,
	): StashBackedRawExchangeHeadsMessage | undefined {
		const session = this._wireSyncSession;
		const backbone = this._nativeBackbone;
		if (!session || !backbone) {
			return undefined;
		}
		const meta = session.stashedMeta(message.header.id);
		if (!meta) {
			return undefined;
		}
		const syncProfile = this._logProperties?.sync?.profile;
		if (syncProfile) {
			emitSyncProfileEvent(syncProfile, {
				name: "sharedLog.rawReceive.wireStashResolve",
				component: "shared-log",
				entries: meta.hashes.length,
				bytes: meta.payloadLength,
				messages: 1,
			});
		}
		return new StashBackedRawExchangeHeadsMessage({
			messageId: message.header.id,
			hashes: meta.hashes,
			gidRefrences: meta.gidRefrences,
			byteLengths: meta.byteLengths,
			reserved: meta.reserved,
			stash: session,
			resolveReleasedBlock: (hash) => backbone.rawReceiveBlockBytes(hash),
		});
	}

	/**
	 * Normalize a raw exchange-heads receive into the regular exchange message
	 * consumed by the rest of the receive path. An undefined result means the
	 * message was fully handled (all heads were already present or the native
	 * receive plan dropped every head).
	 *
	 * This helper intentionally runs inside `onMessage`'s shared try/finally
	 * envelope so receive errors keep their existing classification and a
	 * wire-backed message keeps its single outer stash-release boundary.
	 */
	private async materializeRawReceiveMessage(
		msg: RawExchangeHeadsMessage,
		properties: {
			from: PublicSignKey;
			stashBackedRawMessage?: StashBackedRawExchangeHeadsMessage;
			syncProfile?: SyncProfileFn;
			receiveOwnershipRevision: number;
		},
	): Promise<
		| {
				message: ExchangeHeadsMessage<any>;
				preparedSelection: NativeBackboneRawReceiveSelectionPlan | undefined;
		  }
		| undefined
	> {
		const {
			from: rawFrom,
			stashBackedRawMessage,
			syncProfile,
			receiveOwnershipRevision,
		} = properties;
		const fromIsSelf = rawFrom.equals(this.node.identity.publicKey);
		if (syncProfile && !stashBackedRawMessage) {
			// Per-message JS-side entry decode: the heads were
			// borsh-decoded in TS (regular RPC path) instead of being
			// resolved from the native wire stash. Zero on the fused
			// hot path.
			emitSyncProfileEvent(syncProfile, {
				name: "sharedLog.rawReceive.jsEntryDecode",
				component: "shared-log",
				entries: msg.heads.length,
				messages: 1,
			});
		}
		const rawExistingStartedAt = syncProfileStart(syncProfile);
		const rawExistingHashes = await this.log.hasMany(
			msg.heads.map((head) => head.hash),
		);
		if (syncProfile) {
			emitSyncProfileDuration(syncProfile, rawExistingStartedAt, {
				name: "sharedLog.rawReceive.existingHeads",
				component: "shared-log",
				entries: msg.heads.length,
				messages: 1,
			});
		}
		const rawMissingHeads = [];
		const rawConfirmedHashes = new Set<string>();
		let rawMissingBytes = 0;
		for (const head of msg.heads) {
			if (rawExistingHashes.has(head.hash)) {
				rawConfirmedHashes.add(head.hash);
			} else {
				rawMissingHeads.push(head);
				rawMissingBytes += getRawExchangeHeadByteLength(head);
			}
		}
		if (rawConfirmedHashes.size > 0 && !fromIsSelf) {
			const rawConfirmStartedAt = syncProfileStart(syncProfile);
			this.markEntriesKnownByPeer(rawConfirmedHashes, rawFrom.hashcode());
			await this.sendRepairConfirmation(rawFrom, rawConfirmedHashes);
			if (syncProfile) {
				emitSyncProfileDuration(syncProfile, rawConfirmStartedAt, {
					name: "sharedLog.rawReceive.confirmExisting",
					component: "shared-log",
					entries: rawConfirmedHashes.size,
					messages: 1,
				});
			}
		}
		if (rawMissingHeads.length === 0) {
			return undefined;
		}
		const rawIsRepairHint =
			(msg.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) !== 0;
		const rawPrepareVerifySetting =
			this._logProperties?.sync?.rawExchangeHeadsVerifySignaturesDuringPrepare;
		// A program-level canAppend hook must observe every entry before
		// it commits, so the native join commit (which validates and
		// commits entirely in wasm) is not used for programs that
		// register one; those joins run through the lower-log batch
		// join where the hook fires per entry.
		const programCanAppend = !!this._logProperties?.canAppend;
		const canVerifyPreparedRawReceiveOnCommit =
			!programCanAppend &&
			!!this._nativeBackbone?.graph.commitVerifiedPreparedRawReceiveJoinBatch;
		const canDeferRawReceiveVerificationUntilNativeSelection =
			!rawIsRepairHint &&
			!!this._nativeBackbone?.verifyPreparedRawReceiveEntries &&
			!this._isReplicating &&
			!this.keep &&
			!this.closed &&
			!!this.syncronizer.onReceivedEntryHashes &&
			rawMissingHeads.every((head) => head.gidRefrences.length === 0);
		const verifyNativeBackboneSignaturesDuringPrepare =
			rawPrepareVerifySetting === true ||
			(rawPrepareVerifySetting !== false &&
				(canDeferRawReceiveVerificationUntilNativeSelection ||
					(this._isReplicating &&
						!rawIsRepairHint &&
						!canVerifyPreparedRawReceiveOnCommit)));
		const deferNativeBackboneSignatureVerificationUntilSelection =
			verifyNativeBackboneSignaturesDuringPrepare &&
			canDeferRawReceiveVerificationUntilNativeSelection;
		const deferNativeBackboneSignatureVerificationUntilCommit =
			deferNativeBackboneSignatureVerificationUntilSelection &&
			!programCanAppend &&
			!!this._nativeBackbone?.graph.commitVerifiedPreparedRawReceiveJoinBatch;
		let rawPreparedReceiveSelectionValue:
			| NativeBackboneRawReceiveSelectionPlan
			| undefined;
		let rawPreparedReceiveSelection:
			| Promise<NativeBackboneRawReceiveSelectionPlan | undefined>
			| undefined;
		const getRawPreparedReceiveSelection = async (
			heads: RawEntryWithRefs[],
			hashes: string[],
		) => {
			if (rawPreparedReceiveSelectionValue) {
				return rawPreparedReceiveSelectionValue;
			}
			rawPreparedReceiveSelection ??=
				this.planNativePreparedRawReceiveSelection({
					heads,
					hashes,
					from: rawFrom,
				});
			rawPreparedReceiveSelectionValue = await rawPreparedReceiveSelection;
			return rawPreparedReceiveSelectionValue;
		};
		// Receive fusion: when this message was resolved from the wire
		// stash, the prepared receive reads entry block bytes straight
		// out of wasm memory (indexed into the stashed frame) instead
		// of copying a JS blocks array across the boundary.
		const rawStashIndexes = stashBackedRawMessage
			? getRawExchangeHeadStashIndexes(rawMissingHeads)
			: undefined;
		const prepareNativeBackboneExpectedColumns =
			stashBackedRawMessage && rawStashIndexes
				? ({
						hashes,
						verifySignatures,
					}: {
						hashes: string[];
						verifySignatures: boolean;
					}) => {
						const backbone = this._nativeBackbone;
						const wireSession = this._wireSyncSession;
						if (!backbone || !wireSession) {
							return undefined;
						}
						try {
							return backbone.prepareStashedRawReceiveExpectedColumnsBatch(
								wireSession,
								stashBackedRawMessage.messageId,
								rawStashIndexes,
								hashes,
								{ verifySignatures },
							);
						} catch {
							return undefined;
						}
					}
				: undefined;
		const prepareNativeBackboneExpectedColumnsAndSelection = rawIsRepairHint
			? undefined
			: async ({
					blocks,
					hashes,
					verifySignatures,
				}: {
					blocks: () => Uint8Array[];
					hashes: string[];
					verifySignatures: boolean;
				}) => {
					if (
						verifySignatures ||
						!canDeferRawReceiveVerificationUntilNativeSelection
					) {
						return undefined;
					}
					try {
						const replicaOptions = {
							minReplicas: this.replicas.min?.getValue(this) || 1,
							maxReplicas: this.replicas.max?.getValue(this),
						};
						const leaderSelectionContext =
							await this.createLeaderSelectionContext();
						const prepareOptions = {
							verifySignatures: false as const,
							...replicaOptions,
							leaderOptions: this.createNativeLeaderOptions(
								leaderSelectionContext,
							),
							fromHash: rawFrom.hashcode(),
						};
						let prepared:
							| ReturnType<
									NativePeerbitBackbone["prepareRawReceiveExpectedColumnsAndSelectionBatch"]
							  >
							| undefined;
						const wireSession = this._wireSyncSession;
						if (
							stashBackedRawMessage &&
							rawStashIndexes &&
							wireSession &&
							this._nativeBackbone
						) {
							prepared =
								this._nativeBackbone.prepareStashedRawReceiveExpectedColumnsAndSelectionBatch(
									wireSession,
									stashBackedRawMessage.messageId,
									rawStashIndexes,
									hashes,
									prepareOptions,
								);
						}
						if (
							!prepared &&
							this._nativeBackbone
								?.prepareRawReceiveExpectedColumnsAndSelectionBatch
						) {
							prepared =
								this._nativeBackbone.prepareRawReceiveExpectedColumnsAndSelectionBatch(
									blocks(),
									hashes,
									prepareOptions,
								);
						}
						if (!prepared) {
							return undefined;
						}
						rawPreparedReceiveSelectionValue = prepared.selection;
						rawPreparedReceiveSelection = Promise.resolve(
							rawPreparedReceiveSelectionValue,
						);
						return { columns: prepared.columns };
					} catch {
						this.throwIfReplicationOwnershipPoisoned();
						return undefined;
					}
				};
		const rawMaterializeStartedAt = syncProfileStart(syncProfile);
		const materializedRawMessage =
			await materializeVerifiedRawExchangeHeadsMessage(
				new RawExchangeHeadsMessage({
					heads: rawMissingHeads,
					reserved: msg.reserved,
				}),
				this.log,
				syncProfile,
				{
					nativeBackbone: this._nativeBackbone,
					verifyNativeBackboneSignaturesDuringPrepare:
						verifyNativeBackboneSignaturesDuringPrepare,
					deferNativeBackboneSignatureVerificationUntilSelection:
						deferNativeBackboneSignatureVerificationUntilSelection,
					deferNativeBackboneSignatureVerificationUntilCommit:
						deferNativeBackboneSignatureVerificationUntilCommit,
					prepareNativeBackboneExpectedColumnsAndSelection:
						prepareNativeBackboneExpectedColumnsAndSelection,
					prepareNativeBackboneExpectedColumns:
						prepareNativeBackboneExpectedColumns,
					tryPreparedRawReceiveFastDrop: rawIsRepairHint
						? undefined
						: async ({ heads, hashes }) =>
								this.tryFastDropPreparedRawReceive({
									heads,
									hashes,
									from: rawFrom,
									fromIsSelf,
									syncProfile,
									selection: await getRawPreparedReceiveSelection(
										heads,
										hashes,
									),
									receiveOwnershipRevision,
								}),
					selectPreparedRawReceiveHashes: rawIsRepairHint
						? undefined
						: async ({ heads, hashes }) =>
								this.selectNativePreparedRawReceiveHashes({
									heads,
									hashes,
									from: rawFrom,
									fromIsSelf,
									syncProfile,
									selection: await getRawPreparedReceiveSelection(
										heads,
										hashes,
									),
									receiveOwnershipRevision,
								}),
				},
			);
		if (materializedRawMessage === undefined) {
			if (syncProfile) {
				emitSyncProfileDuration(syncProfile, rawMaterializeStartedAt, {
					name: "sharedLog.rawReceive.materialize",
					component: "shared-log",
					entries: rawMissingHeads.length,
					bytes: rawMissingBytes,
					messages: 1,
					details: { nativeFastDropEarly: true },
				});
			}
			return undefined;
		}
		if (syncProfile) {
			emitSyncProfileDuration(syncProfile, rawMaterializeStartedAt, {
				name: "sharedLog.rawReceive.materialize",
				component: "shared-log",
				entries: rawMissingHeads.length,
				bytes: rawMissingBytes,
				messages: 1,
			});
		}
		return {
			message: materializedRawMessage,
			preparedSelection: rawPreparedReceiveSelectionValue,
		};
	}

	// Callback for receiving a message from the network
	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<void>;
	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<any> {
		const stashBackedRawMessage = isStashBackedRawExchangeHeadsMessage(msg)
			? msg
			: undefined;
		let peerReceiveLease: PeerReceiveLease | undefined;
		try {
			this.throwIfNativeDurableCommitFailed();
			if (!context.from) {
				throw new Error("Missing from in update role message");
			}
			if (
				msg instanceof RequestReplicationInfoMessage ||
				msg instanceof ResponseRoleMessage ||
				msg instanceof AllReplicatingSegmentsMessage ||
				msg instanceof AddedReplicationSegmentMessage ||
				msg instanceof StoppedReplicating
			) {
				// These variants remain registered decode tombstones, but logs fail
				// closed unconditionally before leases, synchronizer work, liveness,
				// watermarks or mutations (B12: the compatibility opens that once
				// admitted them reject at open()).
				return;
			}
			// Snapshot receive ownership before any async handler gets a chance to
			// yield. Replication-info messages reach their branch only after the
			// synchronizer declines them, and a U/S transition can happen meanwhile.
			const receiveFromHash = context.from.hashcode();
			const receiveReplicationLifecycleController =
				this._instanceLifecycle?.membershipLifecycleController;
			// ONE session capture replaces the per-peer subscription-epoch and
			// opening-window captures: the PeerSession IS the epoch token, and
			// the opening-barrier window is its sub-state. `null` = the peer
			// never subscribed (a valid current value, preserved exactly).
			const receiveSession = this._peerSessions.current(receiveFromHash);
			const isOpeningSubscriptionReceive =
				receiveSession?.openingBarrierActive === true;
			const isOpeningCapabilityAdvertisement =
				msg instanceof SyncCapabilitiesMessage && isOpeningSubscriptionReceive;
			const releasePeerReceiveLease = this.acquirePeerReceiveLease(
				receiveFromHash,
				receiveReplicationLifecycleController,
				receiveSession,
				{
					// The replication-info fence existed before receive leases and is
					// intentionally narrower than the subscription itself. Keep admitting
					// sync negotiation/data while the new subscription drains the previous
					// apply generation; replication-info branches recheck the fence below.
					allowReplicationInfoBlocked: isOpeningSubscriptionReceive,
					allowCleanupGate: isOpeningCapabilityAdvertisement,
				},
			);
			if (!releasePeerReceiveLease) {
				return;
			}
			peerReceiveLease = createOneShotPeerReceiveLease(releasePeerReceiveLease);
			const receiveOwnershipRevision =
				this._instanceLifecycle?._receiveOwnershipRevision ?? 0;
			const receiveOwnershipLifecycleController =
				this.captureReplicationOwnershipLifecycle();
			const receiveReplicationInfoReceiveEpoch =
				this._peerSessions.receiveEpoch(receiveFromHash);
			if (
				msg instanceof FullReplicationInfoV2Message ||
				msg instanceof AddedReplicationInfoV2Message
			) {
				// Bound decoded untrusted vectors before per-peer/global mutation
				// queues, trusted-replicator authorization, or liveness side effects.
				this.validateReplicationRangeAnnouncement(msg.segments);
			} else if (msg instanceof StoppedReplicationInfoV2Message) {
				// Bound the raw decoded vector before deduplication can hide the
				// allocation cost, and before liveness or apply-queue side effects.
				this.validateStoppedReplicationAnnouncement(msg.segmentIds);
			}
			if (
				!context.from.equals(this.node.identity.publicKey) &&
				!(msg instanceof RequestReplicationInfoV2Message) &&
				!(msg instanceof RequestPersistedEntriesV1) &&
				!isReplicationInfoV2Message(msg)
			) {
				this._liveness.markReplicatorActivity(receiveFromHash);
			}

			const syncProfile = this._logProperties?.sync?.profile;
			let rawMaterializedKnownMissing = false;
			let rawPreparedReceiveSelectionValue:
				| NativeBackboneRawReceiveSelectionPlan
				| undefined;
			if (msg instanceof RawExchangeHeadsMessage) {
				const materializedRawReceive = await this.materializeRawReceiveMessage(
					msg,
					{
						from: context.from,
						stashBackedRawMessage,
						syncProfile,
						receiveOwnershipRevision,
					},
				);
				if (materializedRawReceive === undefined) {
					return;
				}
				msg = materializedRawReceive.message;
				rawPreparedReceiveSelectionValue =
					materializedRawReceive.preparedSelection;
				rawMaterializedKnownMissing = true;
			}
			if (msg instanceof ExchangeHeadsMessage) {
				/**
				 * I have received heads from someone else.
				 * I can use them to load associated logs and join/sync them with the data stores I own
				 */

				if (syncProfile && !rawMaterializedKnownMissing) {
					// Entries arrived as fully TS-decoded `Entry` objects (the
					// non-raw exchange path). Zero on the fused hot path.
					emitSyncProfileEvent(syncProfile, {
						name: "sharedLog.rawReceive.jsEntryDecode",
						component: "shared-log",
						entries: msg.heads.length,
						messages: 1,
					});
				}

				const { heads } = msg;
				const headHashes =
					msg.preparedHashes && msg.preparedHashes.length === heads.length
						? msg.preparedHashes
						: heads.map(getExchangeHeadHash);
				const isRepairHint =
					(msg.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) !== 0;

				logger.trace(
					`${this.node.identity.publicKey.hashcode()}: Recieved heads: ${
						heads.length === 1 ? headHashes[0] : "#" + heads.length
					}, logId: ${this.log.idString}`,
				);

				if (heads) {
					let filteredHeads: EntryWithRefs<any>[];
					let filteredHeadHashes: string[];
					const confirmedHashes = new Set<string>();
					const existingStartedAt = syncProfileStart(syncProfile);
					const existingHashes = rawMaterializedKnownMissing
						? undefined
						: await this.log.hasMany(headHashes);
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, existingStartedAt, {
							name: "sharedLog.receive.existingHeads",
							component: "shared-log",
							entries: heads.length,
							messages: 1,
							details: { rawMaterializedKnownMissing },
						});
					}
					if (rawMaterializedKnownMissing) {
						filteredHeads = heads;
						filteredHeadHashes = headHashes;
					} else {
						filteredHeads = [];
						filteredHeadHashes = [];
						for (let headIndex = 0; headIndex < heads.length; headIndex++) {
							const head = heads[headIndex]!;
							const headHash = headHashes[headIndex]!;
							if (!existingHashes!.has(headHash)) {
								initExchangeHeadEntry(head, {
									// we need to init because we perhaps need to decrypt gid
									keychain: this.log.keychain,
									encoding: this.log.encoding,
								});
								filteredHeads.push(head);
								filteredHeadHashes.push(headHash);
							} else {
								confirmedHashes.add(headHash);
							}
						}
					}
					const fromIsSelf = context.from.equals(this.node.identity.publicKey);
					const contextFromHash = context.from.hashcode();
					if (!fromIsSelf) {
						this.markEntriesKnownByPeer(headHashes, contextFromHash);
					}

					if (filteredHeads.length === 0) {
						if (confirmedHashes.size > 0 && !fromIsSelf) {
							await this.sendRepairConfirmation(context.from!, confirmedHashes);
						}
						return;
					}
					const receivePlanStartedAt = syncProfileStart(syncProfile);
					const receiveReplicaCounts = new Map<string, number>();
					let receivePredecodedReplicaHits = 0;
					const decodeReceiveReplicaCount = (entry: {
						hash: string;
						meta: { data?: Uint8Array };
					}) => {
						const cached = receiveReplicaCounts.get(entry.hash);
						if (cached !== undefined) {
							return cached;
						}
						const predecodedReplicas =
							entry instanceof Entry
								? getPreparedRawExchangeRequestedReplicas(entry)
								: undefined;
						const replicas =
							predecodedReplicas ?? decodeReplicas(entry).getValue(this);
						if (predecodedReplicas != null) {
							receivePredecodedReplicaHits++;
						}
						receiveReplicaCounts.set(entry.hash, replicas);
						return replicas;
					};
					const decodeReceiveHeadReplicaCount = (head: EntryWithRefs<any>) => {
						const hash = getExchangeHeadHash(head);
						const cached = receiveReplicaCounts.get(hash);
						if (cached !== undefined) {
							return cached;
						}
						const predecodedReplicas =
							getPreparedRawExchangeHeadRequestedReplicas(head);
						if (predecodedReplicas != null) {
							receivePredecodedReplicaHits++;
							receiveReplicaCounts.set(hash, predecodedReplicas);
							return predecodedReplicas;
						}
						const shallow = getPreparedRawExchangeHeadShallowEntry(head);
						if (shallow) {
							return decodeReceiveReplicaCount(shallow);
						}
						return decodeReceiveReplicaCount(head.entry);
					};
					const getReceiveHeadShallowOrEntry = (
						head: EntryWithRefs<any>,
					): ShallowOrFullEntry<any> =>
						getPreparedRawExchangeHeadShallowEntry(head) ?? head.entry;
					const receiveKeepDecisions = new Map<string, MaybePromise<boolean>>();
					const getReceiveKeepDecision = (head: EntryWithRefs<any>) => {
						const hash = getExchangeHeadHash(head);
						if (receiveKeepDecisions.has(hash)) {
							return receiveKeepDecisions.get(hash)!;
						}
						const result = this.keep!(getReceiveHeadShallowOrEntry(head));
						receiveKeepDecisions.set(hash, result);
						return result;
					};
					type ReceivedGidJoinPlan = {
						toMerge: EntryWithRefs<any>[];
						toPersist: ShallowOrFullEntry<any>[];
						toDelete?: ShallowOrFullEntry<any>[];
						maybeDelete?: EntryWithRefs<any>[][];
						leaders: LeaderMap | false;
					};
					type ReceivedGidInput = {
						gid: string;
						entries: EntryWithRefs<any>[];
						latestEntry: ShallowOrFullEntry<any>;
						maxReplicasFromHead: number;
						maxReplicasFromNewEntries: number;
						maxMaxReplicas: number;
						leaderPlan?: EntryLeaderPlan<R>;
						isLeader?: boolean;
						fromIsLeader?: boolean;
						leaders?: LeaderMap | false;
					};
					const isReplicating = this._isReplicating;
					const receiveGroups: ReceivedGidInput[] = [];
					let nativeRawGroupPlans:
						| NativeBackboneRawReceiveGroupPlan[]
						| undefined;
					let nativeRawGroupAssignmentPlans:
						| NativeBackboneRawReceiveGroupAssignmentPlan[]
						| undefined;
					let nativeRawGroupIndexPlans:
						| NativeBackboneRawReceiveGroupIndexPlan[]
						| undefined;
					let nativeRawGroupLeaderPlans:
						| NativeBackboneRawReceiveGroupLeaderPlan[]
						| undefined;
					let usedNativeRawGroupLeaderPlansFromSelection = false;
					if (rawMaterializedKnownMissing && this._nativeBackbone) {
						const replicaOptions = {
							minReplicas: this.replicas.min?.getValue(this) || 1,
							maxReplicas: this.replicas.max?.getValue(this),
						};
						if (
							!isReplicating &&
							rawPreparedReceiveSelectionValue?.retainedGroupLeaderPlans &&
							rawPreparedReceiveSelectionValue.retainedHashes.length ===
								filteredHeadHashes.length &&
							rawPreparedReceiveSelectionValue.retainedHashes.every(
								(hash, index) => hash === filteredHeadHashes[index],
							)
						) {
							nativeRawGroupLeaderPlans =
								rawPreparedReceiveSelectionValue.retainedGroupLeaderPlans;
							usedNativeRawGroupLeaderPlansFromSelection = true;
						}
						if (nativeRawGroupLeaderPlans === undefined) {
							try {
								const leaderOptions = isReplicating
									? ({ roleAge: 0 } as const)
									: undefined;
								const leaderContext =
									await this.createLeaderSelectionContext(leaderOptions);
								const nativeLeaderOptions =
									this.createNativeLeaderOptions(leaderContext);
								if (!this.keep && !traceLogger.enabled && !this.closed) {
									nativeRawGroupAssignmentPlans =
										this._nativeBackbone.planPreparedRawReceiveGroupAssignments?.(
											filteredHeadHashes,
											replicaOptions,
											nativeLeaderOptions,
											contextFromHash,
										);
									if (
										nativeRawGroupAssignmentPlans &&
										!nativeRawGroupAssignmentPlans.every((plan) => {
											const keepAsLeader =
												plan.isLeader || (isRepairHint && plan.fromIsLeader);
											const canKeepWithoutWait = isReplicating
												? plan.isLeader
												: keepAsLeader;
											return (
												canKeepWithoutWait &&
												plan.maxReplicasFromNewEntries >=
													plan.maxReplicasFromHead
											);
										})
									) {
										nativeRawGroupAssignmentPlans = undefined;
									}
								}
								if (!nativeRawGroupAssignmentPlans) {
									nativeRawGroupLeaderPlans =
										this._nativeBackbone.planPreparedRawReceiveGroupLeaders?.(
											filteredHeadHashes,
											replicaOptions,
											nativeLeaderOptions,
										);
								}
							} catch {
								this.throwIfReplicationOwnershipPoisoned();
								nativeRawGroupAssignmentPlans = undefined;
								nativeRawGroupLeaderPlans = undefined;
							}
						}
						if (
							nativeRawGroupLeaderPlans === undefined &&
							nativeRawGroupAssignmentPlans === undefined
						) {
							nativeRawGroupIndexPlans =
								this._nativeBackbone.planPreparedRawReceiveGroupIndexes?.(
									filteredHeadHashes,
									replicaOptions,
								);
						}
						nativeRawGroupPlans =
							nativeRawGroupLeaderPlans === undefined &&
							nativeRawGroupAssignmentPlans === undefined &&
							nativeRawGroupIndexPlans === undefined
								? this._nativeBackbone.planPreparedRawReceiveGroups(
										filteredHeadHashes,
										replicaOptions,
									)
								: undefined;
					}
					let usedNativeRawGroups = false;
					let usedNativeRawGroupAssignmentPlans = false;
					let usedNativeRawGroupIndexes = false;
					let usedNativeRawGroupLeaderPlans = false;
					if (nativeRawGroupAssignmentPlans) {
						let canUseNativeRawGroups = true;
						for (const plan of nativeRawGroupAssignmentPlans) {
							if (plan.indexes.length !== plan.requestedReplicas.length) {
								canUseNativeRawGroups = false;
								break;
							}
							const entries: EntryWithRefs<any>[] = [];
							for (let i = 0; i < plan.indexes.length; i++) {
								const entryIndex = plan.indexes[i]!;
								const entry = filteredHeads[entryIndex];
								const hash = filteredHeadHashes[entryIndex];
								if (!entry || !hash) {
									canUseNativeRawGroups = false;
									break;
								}
								entries.push(entry);
								receiveReplicaCounts.set(hash, plan.requestedReplicas[i]!);
							}
							if (!canUseNativeRawGroups) {
								break;
							}
							const latestHead = filteredHeads[plan.latestIndex];
							if (!latestHead) {
								canUseNativeRawGroups = false;
								break;
							}
							receivePredecodedReplicaHits += plan.indexes.length;
							receiveGroups.push({
								gid: plan.gid,
								entries,
								latestEntry: getReceiveHeadShallowOrEntry(latestHead),
								maxReplicasFromHead: plan.maxReplicasFromHead,
								maxReplicasFromNewEntries: plan.maxReplicasFromNewEntries,
								maxMaxReplicas: plan.maxMaxReplicas,
								leaderPlan: {
									coordinates: plan.coordinates as NumberFromType<R>[],
									coordinateStrings: plan.coordinateStrings,
									leaders: new Map(),
									isLeader: plan.isLeader,
									assignedToRangeBoundary: plan.assignedToRangeBoundary,
								},
								leaders: false,
								isLeader: plan.isLeader,
								fromIsLeader: plan.fromIsLeader,
							});
						}
						if (canUseNativeRawGroups) {
							usedNativeRawGroups = true;
							usedNativeRawGroupIndexes = true;
							usedNativeRawGroupAssignmentPlans = true;
						} else {
							receiveGroups.length = 0;
							receiveReplicaCounts.clear();
							receivePredecodedReplicaHits = 0;
						}
					} else if (nativeRawGroupLeaderPlans) {
						let canUseNativeRawGroups = true;
						for (const plan of nativeRawGroupLeaderPlans) {
							if (plan.indexes.length !== plan.requestedReplicas.length) {
								canUseNativeRawGroups = false;
								break;
							}
							const entries: EntryWithRefs<any>[] = [];
							for (let i = 0; i < plan.indexes.length; i++) {
								const entryIndex = plan.indexes[i]!;
								const entry = filteredHeads[entryIndex];
								const hash = filteredHeadHashes[entryIndex];
								if (!entry || !hash) {
									canUseNativeRawGroups = false;
									break;
								}
								entries.push(entry);
								receiveReplicaCounts.set(hash, plan.requestedReplicas[i]!);
							}
							if (!canUseNativeRawGroups) {
								break;
							}
							const latestHead = filteredHeads[plan.latestIndex];
							if (!latestHead) {
								canUseNativeRawGroups = false;
								break;
							}
							receivePredecodedReplicaHits += plan.indexes.length;
							receiveGroups.push({
								gid: plan.gid,
								entries,
								latestEntry: getReceiveHeadShallowOrEntry(latestHead),
								maxReplicasFromHead: plan.maxReplicasFromHead,
								maxReplicasFromNewEntries: plan.maxReplicasFromNewEntries,
								maxMaxReplicas: plan.maxMaxReplicas,
								leaderPlan: {
									coordinates: Array.from(
										plan.coordinates as Iterable<NumberFromType<R>>,
									),
									coordinateStrings: plan.coordinateStrings,
									leaders: plan.leaders,
									isLeader: plan.leaders.has(
										this.node.identity.publicKey.hashcode(),
									),
								},
								leaders: plan.leaders,
								isLeader: plan.leaders.has(
									this.node.identity.publicKey.hashcode(),
								),
								fromIsLeader: plan.leaders.has(contextFromHash),
							});
						}
						if (canUseNativeRawGroups) {
							usedNativeRawGroups = true;
							usedNativeRawGroupIndexes = true;
							usedNativeRawGroupLeaderPlans = true;
						} else {
							receiveGroups.length = 0;
							receiveReplicaCounts.clear();
							receivePredecodedReplicaHits = 0;
						}
					} else if (nativeRawGroupIndexPlans) {
						let canUseNativeRawGroups = true;
						for (const plan of nativeRawGroupIndexPlans) {
							if (plan.indexes.length !== plan.requestedReplicas.length) {
								canUseNativeRawGroups = false;
								break;
							}
							const entries: EntryWithRefs<any>[] = [];
							for (let i = 0; i < plan.indexes.length; i++) {
								const entryIndex = plan.indexes[i]!;
								const entry = filteredHeads[entryIndex];
								const hash = filteredHeadHashes[entryIndex];
								if (!entry || !hash) {
									canUseNativeRawGroups = false;
									break;
								}
								entries.push(entry);
								receiveReplicaCounts.set(hash, plan.requestedReplicas[i]!);
							}
							if (!canUseNativeRawGroups) {
								break;
							}
							const latestHead = filteredHeads[plan.latestIndex];
							if (!latestHead) {
								canUseNativeRawGroups = false;
								break;
							}
							receivePredecodedReplicaHits += plan.indexes.length;
							receiveGroups.push({
								gid: plan.gid,
								entries,
								latestEntry: getReceiveHeadShallowOrEntry(latestHead),
								maxReplicasFromHead: plan.maxReplicasFromHead,
								maxReplicasFromNewEntries: plan.maxReplicasFromNewEntries,
								maxMaxReplicas: plan.maxMaxReplicas,
							});
						}
						if (canUseNativeRawGroups) {
							usedNativeRawGroups = true;
							usedNativeRawGroupIndexes = true;
						} else {
							receiveGroups.length = 0;
							receiveReplicaCounts.clear();
							receivePredecodedReplicaHits = 0;
						}
					} else if (nativeRawGroupPlans) {
						const headByHash = new Map(
							filteredHeads.map((head, index) => [
								filteredHeadHashes[index]!,
								head,
							]),
						);
						let canUseNativeRawGroups = true;
						for (const plan of nativeRawGroupPlans) {
							if (plan.hashes.length !== plan.requestedReplicas.length) {
								canUseNativeRawGroups = false;
								break;
							}
							const entries: EntryWithRefs<any>[] = [];
							for (const hash of plan.hashes) {
								const entry = headByHash.get(hash);
								if (!entry) {
									canUseNativeRawGroups = false;
									break;
								}
								entries.push(entry);
							}
							if (!canUseNativeRawGroups) {
								break;
							}
							const latestHead = headByHash.get(plan.latestHash);
							if (!latestHead) {
								canUseNativeRawGroups = false;
								break;
							}
							for (let i = 0; i < plan.hashes.length; i++) {
								receiveReplicaCounts.set(
									plan.hashes[i]!,
									plan.requestedReplicas[i]!,
								);
							}
							receivePredecodedReplicaHits += plan.hashes.length;
							receiveGroups.push({
								gid: plan.gid,
								entries,
								latestEntry: getReceiveHeadShallowOrEntry(latestHead),
								maxReplicasFromHead: plan.maxReplicasFromHead,
								maxReplicasFromNewEntries: plan.maxReplicasFromNewEntries,
								maxMaxReplicas: plan.maxMaxReplicas,
							});
						}
						if (canUseNativeRawGroups) {
							usedNativeRawGroups = true;
						} else {
							receiveGroups.length = 0;
							receiveReplicaCounts.clear();
							receivePredecodedReplicaHits = 0;
						}
					}
					if (!usedNativeRawGroups) {
						const groupedByGid =
							tryGroupByGidSync(filteredHeads) ??
							(await groupByGid(filteredHeads));
						const maxReplicasFromHeadsByGid =
							await this.getMaxReplicasFromHeadsBatch(groupedByGid.keys());
						for (const [gid, entries] of groupedByGid) {
							const latestEntry = getLatestEntry(entries)!;
							const maxReplicasFromHead =
								maxReplicasFromHeadsByGid.get(gid) ??
								this.replicas.min.getValue(this);
							let maxRequestedReplicasFromNewEntries = 0;
							for (const entry of entries) {
								maxRequestedReplicasFromNewEntries = Math.max(
									decodeReceiveHeadReplicaCount(entry),
									maxRequestedReplicasFromNewEntries,
								);
							}
							const lower = this.replicas.min?.getValue(this) || 1;
							const higher =
								this.replicas.max?.getValue(this) ?? Number.MAX_SAFE_INTEGER;
							const maxReplicasFromNewEntries = Math.max(
								Math.min(higher, maxRequestedReplicasFromNewEntries),
								lower,
							);
							receiveGroups.push({
								gid,
								entries,
								latestEntry,
								maxReplicasFromHead,
								maxReplicasFromNewEntries,
								maxMaxReplicas: Math.max(
									maxReplicasFromHead,
									maxReplicasFromNewEntries,
								),
							});
						}
					}
					let usedNativeReceiveGroupLeaderPlans = false;
					if (!isReplicating) {
						let leaderPlans =
							usedNativeRawGroupLeaderPlans || usedNativeRawGroupAssignmentPlans
								? receiveGroups.map((group) => group.leaderPlan!)
								: usedNativeRawGroups && this._nativeBackbone
									? await this.planNativeBackboneReceiveGroupLeaders(
											receiveGroups,
										)
									: undefined;
						usedNativeReceiveGroupLeaderPlans = leaderPlans !== undefined;
						leaderPlans ??= await this.planEntryLeaderBatch(
							receiveGroups.map((group) => ({
								entry: group.latestEntry,
								replicas: group.maxMaxReplicas,
							})),
						);
						for (let i = 0; i < receiveGroups.length; i++) {
							const group = receiveGroups[i]!;
							const leaderPlan = leaderPlans[i];
							group.leaderPlan = leaderPlan;
							if (!leaderPlan) {
								continue;
							}
							group.leaders = leaderPlan.leaders;
							group.isLeader = leaderPlan.isLeader;
							group.fromIsLeader = leaderPlan.leaders.has(contextFromHash);
						}
					}
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, receivePlanStartedAt, {
							name: "sharedLog.receive.plan",
							component: "shared-log",
							entries: filteredHeads.length,
							count: receiveGroups.length,
							messages: 1,
							details: {
								replicating: isReplicating,
								predecodedReplicaHits: receivePredecodedReplicaHits,
								nativeRawGroups: usedNativeRawGroups,
								nativeRawGroupIndexes: usedNativeRawGroupIndexes,
								nativeRawGroupLeaderPlans: usedNativeRawGroupLeaderPlans,
								nativeRawGroupAssignmentPlans:
									usedNativeRawGroupAssignmentPlans,
								nativeRawGroupLeaderPlansFromSelection:
									usedNativeRawGroupLeaderPlansFromSelection,
								nativeReceiveGroupLeaderPlans:
									usedNativeReceiveGroupLeaderPlans,
							},
						});
					}
					let immediateReplicatingLeaderPlans: EntryLeaderPlan<R>[] | undefined;
					let immediateReplicatingLeaderPlanHits = 0;
					let usedNativeImmediateReceiveGroupLeaderPlans = false;
					if (isReplicating && receiveGroups.length > 0) {
						const immediateLeaderStartedAt = syncProfileStart(syncProfile);
						const immediateLeaderItems = receiveGroups.map((group) => ({
							entry: group.latestEntry,
							replicas: group.maxMaxReplicas,
							options: { roleAge: 0, persist: false as const },
						}));
						if (
							usedNativeRawGroupLeaderPlans ||
							usedNativeRawGroupAssignmentPlans
						) {
							immediateReplicatingLeaderPlans = receiveGroups.map(
								(group) => group.leaderPlan!,
							);
							usedNativeImmediateReceiveGroupLeaderPlans = true;
						} else if (usedNativeRawGroups && this._nativeBackbone) {
							immediateReplicatingLeaderPlans =
								await this.planNativeBackboneReceiveGroupLeaders(
									receiveGroups,
									{ roleAge: 0 },
								);
							usedNativeImmediateReceiveGroupLeaderPlans =
								immediateReplicatingLeaderPlans !== undefined;
						}
						if (
							!immediateReplicatingLeaderPlans &&
							this.canPlanNativeEntryLeaderBatch(immediateLeaderItems)
						) {
							immediateReplicatingLeaderPlans =
								await this.planEntryLeaderBatch(immediateLeaderItems);
						}
						if (immediateReplicatingLeaderPlans) {
							for (let i = 0; i < immediateReplicatingLeaderPlans.length; i++) {
								const plan = immediateReplicatingLeaderPlans[i];
								if (!plan?.isLeader) {
									continue;
								}
								const group = receiveGroups[i]!;
								group.leaderPlan = plan;
								group.leaders = plan.leaders;
								group.isLeader = true;
								group.fromIsLeader = plan.leaders.has(contextFromHash);
							}
						}
						if (syncProfile) {
							emitSyncProfileDuration(syncProfile, immediateLeaderStartedAt, {
								name: "sharedLog.receive.immediateLeaderPlan",
								component: "shared-log",
								entries: filteredHeads.length,
								count: immediateReplicatingLeaderPlans?.length ?? 0,
								messages: 1,
								details: {
									nativeBatch: immediateReplicatingLeaderPlans !== undefined,
									nativeReceiveGroupLeaderPlans:
										usedNativeImmediateReceiveGroupLeaderPlans,
								},
							});
						}
					}

					const notifyStartedAt = syncProfileStart(syncProfile);
					if (this.syncronizer.onReceivedEntryHashes) {
						await this.syncronizer.onReceivedEntryHashes({
							hashes: filteredHeadHashes,
							from: context.from!,
						});
					} else {
						await this.syncronizer.onReceivedEntries({
							entries: filteredHeads.map((head) =>
								isPreparedRawEntryWithRefs(head)
									? new EntryWithRefs({
											entry: head.entry,
											gidRefrences: head.gidRefrences,
										})
									: head,
							),
							from: context.from!,
						});
					}
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, notifyStartedAt, {
							name: "sharedLog.receive.notifySynchronizer",
							component: "shared-log",
							entries: filteredHeads.length,
							messages: 1,
							details: {
								hashOnly: !!this.syncronizer.onReceivedEntryHashes,
							},
						});
					}
					const canFastDropNativeRawReceive =
						rawMaterializedKnownMissing &&
						usedNativeRawGroups &&
						!isReplicating &&
						!this.keep &&
						!isRepairHint &&
						this.isReceiveOwnershipSnapshotStable(receiveOwnershipRevision) &&
						receiveGroups.length > 0 &&
						receiveGroups.every(
							(group) =>
								group.isLeader === false &&
								group.fromIsLeader === false &&
								group.entries.every((entry) => entry.gidRefrences.length === 0),
						);
					if (canFastDropNativeRawReceive) {
						const joinPlanStartedAt = syncProfileStart(syncProfile);
						if (syncProfile) {
							emitSyncProfileDuration(syncProfile, joinPlanStartedAt, {
								name: "sharedLog.receive.joinPlan",
								component: "shared-log",
								entries: filteredHeads.length,
								count: 0,
								messages: 1,
								details: { nativeFastDrop: true },
							});
						}
						this._nativeBackbone?.clearPreparedRawReceiveEntries(
							filteredHeadHashes,
						);
						if (
							confirmedHashes.size > 0 &&
							!context.from.equals(this.node.identity.publicKey)
						) {
							const confirmStartedAt = syncProfileStart(syncProfile);
							await this.sendRepairConfirmation(context.from!, confirmedHashes);
							if (syncProfile) {
								emitSyncProfileDuration(syncProfile, confirmStartedAt, {
									name: "sharedLog.receive.confirmJoined",
									component: "shared-log",
									entries: confirmedHashes.size,
									messages: 1,
								});
							}
						}
						return;
					}
					const joinPlanStartedAt = syncProfileStart(syncProfile);
					let usedNativeSynchronousJoinPlan = false;
					let usedNativeAllKeptJoinPlan = false;
					let nativeAllKeptJoinHashes: string[] | undefined;
					let joinPlans: ReceivedGidJoinPlan[];
					const canUseNativeSynchronousJoinPlanBase =
						(usedNativeRawGroupLeaderPlans ||
							usedNativeRawGroupAssignmentPlans) &&
						!traceLogger.enabled &&
						!this.closed &&
						(!isReplicating ||
							receiveGroups.every((group) => group.isLeader === true)) &&
						receiveGroups.every(
							(group) =>
								group.leaders !== undefined &&
								group.entries.every((entry) => entry.gidRefrences.length === 0),
						);
					let canUseAllKeptNativeJoinPlan = canUseNativeSynchronousJoinPlanBase;
					if (canUseAllKeptNativeJoinPlan) {
						for (const group of receiveGroups) {
							const fromIsLeader = group.fromIsLeader ?? false;
							const keepAsLeader =
								group.isLeader === true || (isRepairHint && fromIsLeader);
							if (group.maxReplicasFromNewEntries < group.maxReplicasFromHead) {
								canUseAllKeptNativeJoinPlan = false;
								break;
							}
							if (keepAsLeader) {
								continue;
							}
							if (!this.keep) {
								canUseAllKeptNativeJoinPlan = false;
								break;
							}
							for (const entry of group.entries) {
								const keepResult = getReceiveKeepDecision(entry);
								if (isPromiseLike(keepResult) || !keepResult) {
									canUseAllKeptNativeJoinPlan = false;
									break;
								}
							}
							if (!canUseAllKeptNativeJoinPlan) {
								break;
							}
						}
					}
					const canUseNativeSynchronousJoinPlan =
						canUseNativeSynchronousJoinPlanBase &&
						(!this.keep || canUseAllKeptNativeJoinPlan);
					if (canUseNativeSynchronousJoinPlan) {
						usedNativeSynchronousJoinPlan = true;
						const contextFromHashes = [contextFromHash];
						if (canUseAllKeptNativeJoinPlan) {
							usedNativeAllKeptJoinPlan = true;
							const toMerge: EntryWithRefs<any>[] = [];
							const toPersist: ShallowOrFullEntry<any>[] = [];
							const cleanupHashes: string[] = [];
							for (const group of receiveGroups) {
								if (isReplicating && group.isLeader === true) {
									immediateReplicatingLeaderPlanHits++;
								}
								const temporaryRepairKeep =
									group.isLeader !== true &&
									isRepairHint &&
									(group.fromIsLeader ?? false);
								if (group.fromIsLeader) {
									this.addPeersToGidPeerHistory(group.gid, contextFromHashes);
								}
								for (const entry of group.entries) {
									const hash = getExchangeHeadHash(entry);
									cleanupHashes.push(hash);
									if (temporaryRepairKeep) {
										this.rearmCheckedPruneAfterTemporaryReceive(hash);
									} else {
										void this.cancelCheckedPruneForLocalLeader(hash).catch(
											() => {},
										);
									}
									toMerge.push(entry);
									toPersist.push(getReceiveHeadShallowOrEntry(entry));
								}
							}
							this._checkedPrune.removeRequestsSent(cleanupHashes);
							this._checkedPrune.clearConfirmedReplicatorsBatch(cleanupHashes);
							nativeAllKeptJoinHashes = cleanupHashes;
							joinPlans = [
								{
									toMerge,
									toPersist,
									leaders: false,
								},
							];
						} else {
							joinPlans = [];
							for (const group of receiveGroups) {
								const leaders = group.leaders!;
								const fromIsLeader = group.fromIsLeader ?? false;
								const keepAsLeader =
									group.isLeader === true || (isRepairHint && fromIsLeader);
								const temporaryRepairKeep =
									group.isLeader !== true && isRepairHint && fromIsLeader;
								let maybeDelete: EntryWithRefs<any>[][] | undefined;
								const toMerge: EntryWithRefs<any>[] = [];
								const toPersist: ShallowOrFullEntry<any>[] = [];
								if (isReplicating && group.isLeader === true) {
									immediateReplicatingLeaderPlanHits++;
								}
								if (keepAsLeader) {
									for (const entry of group.entries) {
										const hash = getExchangeHeadHash(entry);
										if (temporaryRepairKeep) {
											this.rearmCheckedPruneAfterTemporaryReceive(hash);
										} else {
											void this.cancelCheckedPruneForLocalLeader(hash).catch(
												() => {},
											);
										}
										this._checkedPrune.removeRequestSent(hash);
										this._checkedPrune.clearConfirmedReplicators(hash);
										toMerge.push(entry);
										toPersist.push(getReceiveHeadShallowOrEntry(entry));
									}
									if (fromIsLeader) {
										this.addPeersToGidPeerHistory(group.gid, [contextFromHash]);
									}
									if (
										group.maxReplicasFromNewEntries < group.maxReplicasFromHead
									) {
										(maybeDelete || (maybeDelete = [])).push(group.entries);
									}
								}
								joinPlans.push({
									toMerge,
									toPersist,
									maybeDelete,
									leaders,
								});
							}
						}
					} else {
						const promises: Promise<ReceivedGidJoinPlan | undefined>[] = [];

						for (
							let groupIndex = 0;
							groupIndex < receiveGroups.length;
							groupIndex++
						) {
							const {
								gid,
								entries,
								latestEntry,
								maxReplicasFromHead,
								maxReplicasFromNewEntries,
								maxMaxReplicas,
								leaderPlan,
								isLeader: plannedIsLeader,
								fromIsLeader: plannedFromIsLeader,
								leaders: plannedLeaders,
							} = receiveGroups[groupIndex]!;
							const fn = async () => {
								let isLeader = false;
								let fromIsLeader = false;
								let leaders: LeaderMap | false;
								if (isReplicating) {
									const immediatePlan =
										immediateReplicatingLeaderPlans?.[groupIndex];
									if (immediatePlan?.isLeader) {
										immediateReplicatingLeaderPlanHits++;
										leaders = immediatePlan.leaders;
										isLeader = true;
										fromIsLeader = leaders.has(contextFromHash);
									} else {
										const leaderObservation: ReceiveLeaderObservation = {
											isLeader: false,
											fromIsLeader: false,
										};
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
												// Let raw receive confirm immediate leadership against the current replicator set.
												roleAge: 0,
												timeout: 2e4,
												onLeader: createReceiveLeaderObserver(
													leaderObservation,
													this.node.identity.publicKey.hashcode(),
													contextFromHash,
												),
											},
										);
										isLeader = leaderObservation.isLeader;
										fromIsLeader = leaderObservation.fromIsLeader;
									}
								} else {
									if (plannedLeaders) {
										leaders = plannedLeaders;
										isLeader = plannedIsLeader ?? false;
										fromIsLeader = plannedFromIsLeader ?? false;
									} else {
										const plan =
											leaderPlan ??
											(await this.planEntryLeaders(
												latestEntry,
												maxMaxReplicas,
											));
										leaders = plan.leaders;
										isLeader = plan.isLeader;
										fromIsLeader = leaders.has(contextFromHash);
									}
								}

								if (this.closed) {
									return;
								}

								let maybeDelete: EntryWithRefs<any>[][] | undefined;
								let toMerge: EntryWithRefs<any>[] = [];
								let toPersist: ShallowOrFullEntry<any>[] = [];
								let toDelete: ShallowOrFullEntry<any>[] | undefined;
								// Targeted repair is sent only to peers the sender currently believes
								// should store the entry. Accept it while local membership catches up;
								// the normal checked-prune path below can still remove it if this peer
								// truly no longer owns the entry.
								const acceptsTargetedRepair = isRepairHint && fromIsLeader;
								const keepAsLeader = isLeader || acceptsTargetedRepair;
								let gidReferenceHeads: boolean[] | undefined;
								const getGidReferenceHeads = async () => {
									gidReferenceHeads ??= await this.hasAnyHeadForGidSets(
										entries.map((entry) => entry.gidRefrences),
									);
									return gidReferenceHeads;
								};
								if (keepAsLeader) {
									for (const entry of entries) {
										const hash = getExchangeHeadHash(entry);
										if (acceptsTargetedRepair && !isLeader) {
											this.rearmCheckedPruneAfterTemporaryReceive(hash);
										} else {
											void this.cancelCheckedPruneForLocalLeader(hash).catch(
												() => {},
											);
										}
										this._checkedPrune.removeRequestSent(hash);
										this._checkedPrune.clearConfirmedReplicators(hash);
									}
									if (fromIsLeader) {
										this.addPeersToGidPeerHistory(gid, [contextFromHash]);
									}

									if (maxReplicasFromNewEntries < maxReplicasFromHead) {
										(maybeDelete || (maybeDelete = [])).push(entries);
									}
								}

								outer: for (let i = 0; i < entries.length; i++) {
									const entry = entries[i]!;
									let shouldKeep = keepAsLeader;
									if (!shouldKeep && this.keep) {
										const keepResult = getReceiveKeepDecision(entry);
										shouldKeep = isPromiseLike(keepResult)
											? await keepResult
											: keepResult;
									}
									if (shouldKeep) {
										if (!keepAsLeader) {
											void this.cancelCheckedPruneForLocalLeader(
												getExchangeHeadHash(entry),
											).catch(() => {});
										}
										toMerge.push(entry);
										toPersist.push(getReceiveHeadShallowOrEntry(entry));
									} else if (entry.gidRefrences.length > 0) {
										const referenceHeads = await getGidReferenceHeads();
										if (referenceHeads[i]) {
											toMerge.push(entry);
											(toDelete || (toDelete = [])).push(
												getReceiveHeadShallowOrEntry(entry),
											);
											continue outer;
										}
									}

									if (traceLogger.enabled) {
										const droppedGid =
											getPreparedRawExchangeHeadGid(entry) ??
											this.getEntryGid(entry.entry);
										traceLogger(
											`${this.node.identity.publicKey.hashcode()}: Dropping heads with gid: ${droppedGid}. Because not leader`,
										);
									}
								}

								if (this.closed) {
									return;
								}

								return { toMerge, toPersist, toDelete, maybeDelete, leaders };
							};
							promises.push(fn()); // we do this concurrently since waitForIsLeader might be a blocking operation for some entries
						}
						joinPlans = (await Promise.all(promises)).filter(
							(plan): plan is ReceivedGidJoinPlan => !!plan,
						);
					}
					const reusableCoordinatePlans =
						this._coordinates.createReusableReceiveCoordinatePlans(
							receiveGroups,
							{
								decodedReplicaCounts: receiveReplicaCounts,
								allowRoleAgeZeroPlans:
									immediateReplicatingLeaderPlans !== undefined,
							},
						) as Map<string, ReusableReceiveCoordinatePlan<R>>;
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, joinPlanStartedAt, {
							name: "sharedLog.receive.joinPlan",
							component: "shared-log",
							entries: filteredHeads.length,
							count: joinPlans.length,
							messages: 1,
							details: {
								immediateReplicatingLeaderPlanHits,
								immediateReplicatingLeaderPlans:
									immediateReplicatingLeaderPlans?.length ?? 0,
								nativeSynchronousJoinPlan: usedNativeSynchronousJoinPlan,
								nativeAllKeptJoinPlan: usedNativeAllKeptJoinPlan,
							},
						});
					}
					const allToMerge = usedNativeAllKeptJoinPlan
						? joinPlans[0]!.toMerge
						: joinPlans.flatMap((plan) => plan.toMerge);
					const allToMergeHashes =
						nativeAllKeptJoinHashes ??
						allToMerge.map((entry) => getExchangeHeadHash(entry));
					const allToMergeShallowEntries = usedNativeAllKeptJoinPlan
						? joinPlans[0]!.toPersist
						: allToMerge.map((entry) => getReceiveHeadShallowOrEntry(entry));
					let allToMergeMaterializedEntries: Entry<any>[] | undefined;
					const materializeAllToMergeEntries = () => {
						allToMergeMaterializedEntries ??= allToMerge.map(
							(entry) => entry.entry,
						);
						return allToMergeMaterializedEntries;
					};
					let admittedMergeHashes: ReadonlySet<string> = new Set();
					let nativePreparedCommittedHashes: Set<string> | undefined;
					if (allToMerge.length > 0) {
						const validateStartedAt = syncProfileStart(syncProfile);
						// Program-level hooks must observe the joined entries:
						// a canAppend hook disables the native-validated commit
						// (the lower-log join runs the hook per entry instead),
						// and an onChange consumer disables the hash-only sink
						// so the join dispatches the change event with lazy
						// entry views.
						const programCanAppend = !!this._logProperties?.canAppend;
						const programOnChange = !!this._logProperties?.onChange;
						const receiveSignatureVerificationFacts = programCanAppend
							? await this.preverifyReceiveSignaturesBatch(
									materializeAllToMergeEntries(),
									syncProfile,
								)
							: undefined;
						const nativeBackboneCommitValidation = programCanAppend
							? undefined
							: this.validatePreparedRawReceiveHeadsMetadataWithNativeBackbone(
									allToMerge,
									syncProfile,
									{ decodedReplicaCounts: receiveReplicaCounts },
								);
						let canAppendAlreadyValidated = false;
						let fallbackCanAppendAlreadyValidated = false;
						let nativeCommitVerifyHashes: string[] | undefined;
						let nativeCommitVerifyAllHashes = false;
						let nativeCommitCanValidateAppend = false;
						if (nativeBackboneCommitValidation === false) {
							canAppendAlreadyValidated = false;
						} else if (nativeBackboneCommitValidation) {
							nativeCommitCanValidateAppend = true;
							nativeCommitVerifyHashes =
								nativeBackboneCommitValidation.signatureHashes;
							nativeCommitVerifyAllHashes =
								nativeCommitVerifyHashes.length === allToMerge.length;
						} else {
							canAppendAlreadyValidated =
								await this.canSkipLowerLogCanAppendForNetworkJoin(
									materializeAllToMergeEntries(),
									syncProfile,
									{ decodedReplicaCounts: receiveReplicaCounts },
								);
							fallbackCanAppendAlreadyValidated = canAppendAlreadyValidated;
						}
						if (syncProfile) {
							emitSyncProfileDuration(syncProfile, validateStartedAt, {
								name: "sharedLog.receive.validateCanAppend",
								component: "shared-log",
								entries: allToMerge.length,
								messages: 1,
								cacheHit:
									canAppendAlreadyValidated || nativeCommitCanValidateAppend,
								details: {
									nativeCommitCanValidateAppend,
									nativeCommitVerifyHashes:
										nativeCommitVerifyHashes?.length ?? 0,
									nativeCommitVerifyAllHashes,
								},
							});
						}
						const lowerLogJoinStartedAt = syncProfileStart(syncProfile);
						const hashOnlyEntryAdded =
							!programOnChange &&
							!!this.syncronizer.onEntryAddedHash &&
							this._pendingIHave.size === 0;
						const batchHashOnlyEntryAdded =
							!programOnChange &&
							!!this.syncronizer.onEntryAddedHashes &&
							this._pendingIHave.size === 0;
						let mergeEntryByHash: Map<string, EntryWithRefs<any>> | undefined;
						const materializeMergedEntry = (hash: string) => {
							mergeEntryByHash ??= new Map(
								allToMerge.map((entry) => [getExchangeHeadHash(entry), entry]),
							);
							const entryRef = mergeEntryByHash.get(hash);
							if (!entryRef) {
								throw new Error("Missing merged entry for appended hash");
							}
							return entryRef.entry;
						};
						const onAppendHashes = (hashes: string[]) => {
							if (batchHashOnlyEntryAdded) {
								let hashesWithoutWaiters: string[] | undefined;
								for (const hash of hashes) {
									if (this._pendingIHave.has(hash)) {
										this.onEntryAddedHash(hash, () =>
											materializeMergedEntry(hash),
										);
										continue;
									}
									(hashesWithoutWaiters ??= []).push(hash);
								}
								if (hashesWithoutWaiters) {
									this.syncronizer.onEntryAddedHashes?.(hashesWithoutWaiters);
								}
								return;
							}
							for (const hash of hashes) {
								if (hashOnlyEntryAdded && !this._pendingIHave.has(hash)) {
									this.onEntryAddedHash(hash);
									continue;
								}
								this.onEntryAddedHash(hash, () => materializeMergedEntry(hash));
							}
						};
						const preparedAppendFacts: PreparedAppendJoinFacts[] = [];
						let canUsePreparedAppendFacts =
							canAppendAlreadyValidated || nativeCommitCanValidateAppend;
						if (canUsePreparedAppendFacts) {
							for (const entry of allToMerge) {
								const prepared = getPreparedRawExchangeHeadAppendFacts(entry);
								if (!prepared) {
									canUsePreparedAppendFacts = false;
									preparedAppendFacts.length = 0;
									break;
								}
								preparedAppendFacts.push(prepared);
							}
						}
						// Network joins bypass SharedLog.join(), but churn repair scans
						// the coordinate index to redistribute entries after membership changes.
						const entriesToPersist = usedNativeAllKeptJoinPlan
							? allToMergeShallowEntries
							: joinPlans.flatMap((plan) => plan.toPersist);
						let coordinatePersistFallbackEntries: ShallowOrFullEntry<any>[] =
							[];
						let reusableCoordinatePersistItems: CoordinatePersistBatchItem<R>[] =
							[];
						for (const entry of entriesToPersist) {
							const reusablePlan = reusableCoordinatePlans.get(entry.hash);
							if (!reusablePlan) {
								coordinatePersistFallbackEntries.push(entry);
								continue;
							}
							reusableCoordinatePersistItems.push({
								coordinates: reusablePlan.plan.coordinates,
								entry,
								leaders: reusablePlan.plan.leaders,
								replicas: reusablePlan.replicas,
								assignedToRangeBoundary:
									reusablePlan.plan.assignedToRangeBoundary,
								prepared: reusablePlan.prepared,
							});
						}
						let nativePreparedCoordinateBatch:
							| NativeBackboneReceiveCoordinateBatch<R>
							| undefined;
						let nativePreparedCoordinatesFinished = false;
						let nativeBackboneOnlyPersistedHashes: Set<string> | undefined;
						const nativeReceiveCoordinateBatch = canUsePreparedAppendFacts
							? this._coordinates.createBackboneOnlyReceiveCoordinateBatch(
									reusableCoordinatePersistItems,
								)
							: undefined;
						try {
							const nativePreparedJoinCommit = canUsePreparedAppendFacts
								? this._coordinates.createNativeBackbonePreparedJoinCommit(
										nativeReceiveCoordinateBatch,
										(batch) => {
											nativePreparedCoordinateBatch = batch;
										},
										nativeCommitVerifyHashes,
										nativeCommitVerifyAllHashes,
										syncProfile,
										(committedHashes) => {
											nativePreparedCommittedHashes = new Set(committedHashes);
										},
									)
								: undefined;
							const finishNativePreparedCoordinates = async (properties: {
								nativePreparedCommitted: boolean;
							}) => {
								if (
									!properties.nativePreparedCommitted ||
									!nativePreparedCoordinateBatch
								) {
									return;
								}
								try {
									nativeBackboneOnlyPersistedHashes =
										await this._coordinates.finishBackboneOnlyReceiveCoordinateBatch(
											nativePreparedCoordinateBatch,
											syncProfile,
										);
									nativePreparedCoordinatesFinished = true;
								} catch (error) {
									this._coordinates.rollbackBackboneOnlyReceiveCoordinateBatch(
										nativePreparedCoordinateBatch,
									);
									throw error;
								}
							};
							const preparedAppendCanValidateAppend =
								canAppendAlreadyValidated ||
								(nativeCommitCanValidateAppend && !!nativePreparedJoinCommit);
							if (!preparedAppendCanValidateAppend) {
								canUsePreparedAppendFacts = false;
							}
							const nativePreparedJoinCommitValidatesPlan =
								!!nativePreparedJoinCommit &&
								(nativeCommitVerifyHashes && nativeCommitVerifyHashes.length > 0
									? nativeCommitVerifyAllHashes
										? !!this._nativeBackbone?.graph
												.commitVerifiedAllPreparedRawReceiveJoinBatch ||
											!!this._nativeBackbone?.graph
												.commitVerifiedPreparedRawReceiveJoinBatch
										: !!this._nativeBackbone?.graph
												.commitVerifiedPreparedRawReceiveJoinBatch
									: !!this._nativeBackbone?.graph
											.commitPreparedRawReceiveJoinBatch);
							const trustedLowerLog = this.log as unknown as TrustedLowerLog<T>;
							// With a program-level onChange consumer the hash-only
							// sink is not used: the lower-log join dispatches the
							// change event (lazy entry views over the prepared raw
							// facts) so per-entry consumers observe every commit.
							const joinOnAppendHashes = programOnChange
								? undefined
								: onAppendHashes;
							let joinedPreparedFacts = false;
							const joinLowerLog = async () => {
								joinedPreparedFacts =
									canUsePreparedAppendFacts &&
									(await trustedLowerLog.joinPreparedAppendFactsBatch(
										preparedAppendFacts,
										{
											__peerbitEntriesAlreadyMissing: true,
											__peerbitCanAppendAlreadyValidated: true,
											__peerbitDeferIndexWrite: true,
											__peerbitOnAppendHashes: joinOnAppendHashes,
											__peerbitProfile: syncProfile,
											__peerbitNativePreparedJoinCommit:
												nativePreparedJoinCommit,
											__peerbitNativePreparedJoinCommitValidatesPlan:
												nativePreparedJoinCommitValidatesPlan,
											__peerbitOnPreparedJoinCommitted: nativePreparedJoinCommit
												? finishNativePreparedCoordinates
												: undefined,
										},
									));
								if (!joinedPreparedFacts) {
									await trustedLowerLog.join(materializeAllToMergeEntries(), {
										__peerbitBatchIndependent: true,
										__peerbitEntriesAlreadyMissing: true,
										__peerbitCanAppendAlreadyValidated:
											fallbackCanAppendAlreadyValidated,
										__peerbitDeferIndexWrite: true,
										__peerbitOnAppendHashes: joinOnAppendHashes,
										__peerbitProfile: syncProfile,
									});
								}
							};
							await this.withReceiveSignatureVerificationFacts(
								receiveSignatureVerificationFacts,
								joinLowerLog,
							);
							// A recursive lower-log join can resolve successfully while declining
							// an individual top-level entry (for example, when one of its parents
							// is temporarily unavailable). The public Log.join() API intentionally
							// does not expose that per-entry result, so make local index presence the
							// authority before publishing any SharedLog-side effects. A successful
							// prepared-facts batch is atomic and already proves every input hash.
							const admittedHashes = joinedPreparedFacts
								? new Set(allToMergeHashes)
								: await this.log.hasMany(allToMergeHashes);
							admittedMergeHashes = admittedHashes;
							const admittedShallowEntries =
								admittedHashes.size === allToMergeShallowEntries.length
									? allToMergeShallowEntries
									: allToMergeShallowEntries.filter((entry) =>
											admittedHashes.has(entry.hash),
										);
							if (!joinedPreparedFacts) {
								reusableCoordinatePersistItems =
									reusableCoordinatePersistItems.filter((item) =>
										admittedHashes.has(item.entry.hash),
									);
								coordinatePersistFallbackEntries =
									coordinatePersistFallbackEntries.filter((entry) =>
										admittedHashes.has(entry.hash),
									);
							}
							const reusableCoordinatePersistItemCount =
								reusableCoordinatePersistItems.length;
							if (syncProfile) {
								emitSyncProfileDuration(syncProfile, lowerLogJoinStartedAt, {
									name: "sharedLog.receive.lowerLogJoin",
									component: "shared-log",
									entries: allToMerge.length,
									messages: 1,
									details: {
										hashOnlyEntryAdded,
										batchHashOnlyEntryAdded,
										programOnChange,
										joinedPreparedFacts,
										admittedEntries: admittedHashes.size,
										nativePreparedCoordinatesFinished,
									},
								});
							}
							const coordinatePersistStartedAt = syncProfileStart(syncProfile);
							if (nativePreparedCoordinatesFinished) {
								// The lower-log prepared receive transaction already finished
								// the native coordinate mirror/journal after entry-index commit.
							} else if (nativePreparedCoordinateBatch) {
								try {
									nativeBackboneOnlyPersistedHashes =
										await this._coordinates.finishBackboneOnlyReceiveCoordinateBatch(
											nativePreparedCoordinateBatch,
											syncProfile,
										);
								} catch (error) {
									this._coordinates.rollbackBackboneOnlyReceiveCoordinateBatch(
										nativePreparedCoordinateBatch,
									);
									throw error;
								}
							} else {
								nativeBackboneOnlyPersistedHashes =
									await this._coordinates.persistBackboneOnlyReceiveCoordinateBatch(
										reusableCoordinatePersistItems,
									);
							}
							if (
								nativeBackboneOnlyPersistedHashes &&
								nativeBackboneOnlyPersistedHashes.size > 0
							) {
								for (
									let i = reusableCoordinatePersistItems.length - 1;
									i >= 0;
									i--
								) {
									if (
										nativeBackboneOnlyPersistedHashes.has(
											reusableCoordinatePersistItems[i]!.entry.hash,
										)
									) {
										reusableCoordinatePersistItems.splice(i, 1);
									}
								}
							}
							if (reusableCoordinatePersistItems.length > 0) {
								await this._coordinates.persistCoordinatesBatch(
									reusableCoordinatePersistItems,
								);
							}
							if (coordinatePersistFallbackEntries.length > 0) {
								await this.planEntryLeaderBatch(
									coordinatePersistFallbackEntries.map((entry) => ({
										entry,
										replicas:
											receiveReplicaCounts.get(entry.hash) ??
											decodeReplicas(entry).getValue(this),
										options: { roleAge: 0, persist: {} },
									})),
								);
							}
							if (syncProfile) {
								emitSyncProfileDuration(
									syncProfile,
									coordinatePersistStartedAt,
									{
										name: "sharedLog.receive.coordinatePersist",
										component: "shared-log",
										entries: entriesToPersist.length,
										messages: 1,
										details: {
											reusedLeaderPlans: reusableCoordinatePersistItemCount,
											nativeBackboneOnly:
												nativeBackboneOnlyPersistedHashes?.size ?? 0,
										},
									},
								);
							}
							for (const hash of admittedHashes) {
								confirmedHashes.add(hash);
							}
							const checkedPruneStartedAt = syncProfileStart(syncProfile);
							const ownershipChangedDuringReceive =
								!this.isReceiveOwnershipSnapshotStable(
									receiveOwnershipRevision,
								);
							if (ownershipChangedDuringReceive) {
								const freshAuditRevision =
									this._instanceLifecycle?._receiveOwnershipRevision ?? 0;
								const armFreshAuditRetry = () => {
									for (const entry of admittedShallowEntries) {
										this.scheduleCheckedPruneRetry(
											{ entry, leaders: new Map() },
											receiveOwnershipLifecycleController,
										);
									}
								};
								try {
									await this.pruneJoinedEntriesNoLongerLed(
										admittedShallowEntries,
										{
											decodedReplicaCounts: receiveReplicaCounts,
											freshReceiveOwnerAudit: true,
											preserveExistingPruneOnLocalResult: true,
											profile: syncProfile,
										},
										receiveOwnershipLifecycleController,
									);
									this.throwIfReplicationOwnershipLifecycleInactive(
										receiveOwnershipLifecycleController,
									);
									if (
										!this.isReceiveOwnershipSnapshotStable(freshAuditRevision)
									) {
										armFreshAuditRetry();
									}
								} catch {
									// The lower-log and coordinate commits are already durable. A
									// sender retry will filter these hashes as present, so retain a
									// bounded local obligation instead of failing the admitted receive.
									this.throwIfReplicationOwnershipLifecycleInactive(
										receiveOwnershipLifecycleController,
									);
									armFreshAuditRetry();
								}
							} else {
								await this.pruneJoinedEntriesNoLongerLed(
									admittedShallowEntries,
									{
										decodedReplicaCounts: receiveReplicaCounts,
										preserveExistingPruneOnLocalResult: true,
										reusableLeaderPlans: reusableCoordinatePlans,
										profile: syncProfile,
									},
									receiveOwnershipLifecycleController,
								);
							}
							if (syncProfile) {
								emitSyncProfileDuration(syncProfile, checkedPruneStartedAt, {
									name: "sharedLog.receive.checkedPrune",
									component: "shared-log",
									entries: allToMerge.length,
									messages: 1,
								});
							}

							for (const plan of joinPlans) {
								plan.toDelete
									?.filter((entry) => admittedMergeHashes.has(entry.hash))
									.map((entry) =>
										this.pruneDebouncedFnAddIfNotKeeping({
											key: entry.hash,
											value: {
												entry,
												leaders: plan.leaders as Map<string, any>,
											},
										}),
									);
							}
							this.rebalanceParticipationDebounced?.call();
						} finally {
							// Settle seam for the receive token. Every consumer that can
							// roll it back runs inline before control leaves this block: the
							// prepared-join callback resolves during the join await, and the
							// late finish/rollback arm runs above. This `finally` is what
							// closes the abandon arms (no prepared-join commit, a declined
							// native commit, a downgrade to the plain join) without having
							// to enumerate them.
							this._coordinates.settleResidentCoordinateSnapshot(
								nativeReceiveCoordinateBatch?.rollbackCoordinateEntries,
							);
						}
					}

					for (const plan of joinPlans) {
						if (!plan.maybeDelete) {
							continue;
						}
						for (const entries of plan.maybeDelete) {
							const admittedEntries = entries.filter((entry) =>
								admittedMergeHashes.has(getExchangeHeadHash(entry)),
							);
							if (admittedEntries.length === 0) {
								continue;
							}
							const minReplicas = await this.getMaxReplicasFromHeads(
								this.getEntryGid(admittedEntries[0].entry),
							);
							if (minReplicas != null) {
								const isLeader = await this.isLeader({
									entry: admittedEntries[0].entry,
									replicas: minReplicas,
								});

								if (!isLeader) {
									for (const x of admittedEntries) {
										this.pruneDebouncedFnAddIfNotKeeping({
											key: x.entry.hash,
											value: {
												entry: x.entry,
												leaders: plan.leaders as Map<string, any>,
											},
										});
									}
								}
							}
						}
					}
					const clearPreparedStartedAt = syncProfileStart(syncProfile);
					const hashesToClear = nativePreparedCommittedHashes
						? filteredHeadHashes.filter(
								(hash) => !nativePreparedCommittedHashes!.has(hash),
							)
						: filteredHeadHashes;
					if (hashesToClear.length > 0) {
						this._nativeBackbone?.clearPreparedRawReceiveEntries(hashesToClear);
					}
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, clearPreparedStartedAt, {
							name: "sharedLog.receive.clearPreparedRaw",
							component: "shared-log",
							entries: hashesToClear.length,
							messages: 1,
							details: {
								nativeCommitted: nativePreparedCommittedHashes?.size ?? 0,
							},
						});
					}
					if (
						confirmedHashes.size > 0 &&
						!context.from.equals(this.node.identity.publicKey)
					) {
						const confirmStartedAt = syncProfileStart(syncProfile);
						await this.sendRepairConfirmation(context.from!, confirmedHashes);
						if (syncProfile) {
							emitSyncProfileDuration(syncProfile, confirmStartedAt, {
								name: "sharedLog.receive.confirmJoined",
								component: "shared-log",
								entries: confirmedHashes.size,
								messages: 1,
							});
						}
					}
				}
			} else {
				// Control-plane dispatch (stage 4.5). The lane context is built
				// only after the Raw/ExchangeHeads fast path has returned or
				// fallen through, so the heads path allocates nothing for it.
				// Branch order, branch bodies and the shared catch/finally
				// envelope are unchanged: the five extracted handlers hold the
				// former branch bodies verbatim behind alias preambles.
				const lane: ReceiveLaneContext = {
					fromHash: receiveFromHash,
					session: receiveSession,
					lifecycleController: receiveReplicationLifecycleController,
					ownershipLifecycleController: receiveOwnershipLifecycleController,
					receiveEpoch: receiveReplicationInfoReceiveEpoch,
					syncProfile,
					lease: peerReceiveLease,
				};
				// The prelude already threw when `context.from` was missing.
				const laneRequestContext = context as ReceiveRequestContext;
				if (msg instanceof RequestPersistedEntriesV1) {
					return await this.handleRequestPersistedEntriesV1(
						msg,
						laneRequestContext,
						lane,
					);
				} else if (msg instanceof RequestIPruneV2) {
					await this.handleRequestIPruneV2(msg, laneRequestContext, lane);
				} else if (msg instanceof ResponseIPruneV2) {
					await this.handleResponseIPruneV2(msg, laneRequestContext, lane);
				} else if (
					msg instanceof RequestIPrune ||
					msg instanceof ResponseIPrune
				) {
					// Legacy checked-prune messages are signed but uncorrelated. They cannot
					// authorize deletion for a particular active request generation. Mixed
					// versions intentionally retain extra copies until peers upgrade; bounded
					// background auditing preserves convergence without weakening this gate.
					return;
				} else if (msg instanceof ConfirmEntriesMessage) {
					this.markEntriesKnownByPeer(msg.hashes, context.from.hashcode());
					this.clearRepairFrontierHashes(context.from.hashcode(), msg.hashes);
					return;
				} else if (msg instanceof RequestReplicationInfoV2AppliedMessage) {
					if (
						receiveSession?.phase === "open" &&
						this._peerSyncCapabilitySessions.get(receiveFromHash) ===
							context.message.header.session &&
						((this._peerSyncCapabilities.get(receiveFromHash) ?? 0) &
							SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM) !==
							0
					) {
						const applied = this._v2Receive.confirmApplied(msg, {
							from: context.from,
							peerSession: receiveSession,
							receiveEpoch: receiveReplicationInfoReceiveEpoch,
							senderTransportSession: context.message.header.session,
						});
						if (applied) {
							await this.rpc.send(applied, {
								mode: new SilentDelivery({
									to: [context.from],
									redundancy: 1,
								}),
								priority: CONVERGENCE_MESSAGE_PRIORITY,
								signal: AbortSignal.any([
									this._closeController.signal,
									lane.ownershipLifecycleController.signal,
								]),
							});
						}
					}
					return;
				} else if (msg instanceof ReplicationInfoV2AppliedMessage) {
					if (
						this._v2Send.acceptApplied(msg, {
							from: context.from,
							receiverTransportSession: context.message.header.session,
						})
					) {
						this.dispatchPersistedReceiptReadinessChange(receiveFromHash);
					}
					return;
				} else if (isReplicationInfoV2Message(msg)) {
					await this.handleReplicationInfoV2Announcement(
						msg,
						laneRequestContext,
						lane,
					);
				} else if (msg instanceof SyncCapabilitiesMessage) {
					if (!context.from.equals(this.node.identity.publicKey)) {
						const capabilityTransportSession = context.message?.header?.session;
						const capabilityTimestamp = context.message?.header?.timestamp;
						// No await separates this from the capture above, so the captured
						// window state is exact: the legacy re-read of the opening map here
						// could never observe a different value.
						if (
							this._peerSessions.isReplicationInfoBlocked(receiveFromHash) &&
							isOpeningSubscriptionReceive
						) {
							// A prior unsubscribe cleanup may still be ahead of this reconnect
							// barrier. Stage the new generation's one-shot advertisement so that
							// cleanup cannot erase it before the opening transition commits.
							this.observePeerSyncCapabilities({
								peerHash: receiveFromHash,
								capabilities: msg.capabilities,
								transportSession: capabilityTransportSession,
								timestamp: capabilityTimestamp,
								openingSession: receiveSession!,
							});
						} else {
							const observed = this.observePeerSyncCapabilities({
								peerHash: receiveFromHash,
								capabilities: msg.capabilities,
								transportSession: capabilityTransportSession,
								timestamp: capabilityTimestamp,
							});
							if (observed && receiveSession?.phase === "open") {
								this.promoteReplicationInfoV2ReceiveCapability(
									context.from,
									receiveSession,
								);
							} else if (observed && receiveSession === null) {
								// A capability can arrive before the sender's topic Subscribe after
								// reconnect. Ask that authenticated peer for its authoritative
								// subscriber snapshot; the resulting Subscribe creates the real
								// PeerSession and completes the symmetric capability handshake.
								// Never synthesize membership from capability traffic alone.
								this.requestSubscriberSnapshotForCapability(context.from);
							}
						}
					}
					return;
				} else if (msg instanceof RequestReplicationInfoV2Message) {
					const requiredCapabilities =
						SYNC_CAPABILITY_REPLICATION_INFO_V2_DECODE |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_APPLY;
					if (
						receiveSession &&
						((this._peerSyncCapabilities.get(receiveFromHash) ?? 0) &
							requiredCapabilities) ===
							requiredCapabilities &&
						this._peerSyncCapabilitySessions.get(receiveFromHash) ===
							context.message.header.session &&
						this._peerSyncCapabilityTimestamps.has(receiveFromHash) &&
						context.message.header.timestamp >
							this._peerSyncCapabilityTimestamps.get(receiveFromHash)! &&
						this._v2Send.acceptRequest(msg, {
							from: context.from,
							peerSession: receiveSession,
							receiverTransportSession: context.message.header.session,
							capabilityTimestamp:
								this._peerSyncCapabilityTimestamps.get(receiveFromHash)!,
							requestTimestamp: context.message.header.timestamp,
						})
					) {
						this._liveness.markReplicatorActivity(receiveFromHash);
					}
					return;
				} else if (await this.syncronizer.onMessage(msg, context)) {
					return; // the syncronizer has handled the message
				} else if (msg instanceof BlocksMessage) {
					await this.remoteBlocks.onMessage(msg.message, {
						from: context.from!.hashcode(),
						transport: createRequestTransportContext(context.message),
					});
				} else if (msg instanceof ReplicationPingMessage) {
					// No-op: used as an ACKed unicast liveness probe.
				} else {
					throw new Error("Unexpected message");
				}
			}
		} catch (e: any) {
			if (e instanceof NativeDurableCommitError) {
				throw e;
			}
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
		} finally {
			try {
				if (stashBackedRawMessage && stashBackedRawMessage.release()) {
					const syncProfile = this._logProperties?.sync?.profile;
					if (syncProfile) {
						emitSyncProfileEvent(syncProfile, {
							name: "sharedLog.rawReceive.wireStashRelease",
							component: "shared-log",
							entries: stashBackedRawMessage.heads.length,
							messages: 1,
							details: {
								bytesMaterialized: stashBackedRawMessage.bytesMaterializedCount,
							},
						});
					}
				}
			} finally {
				// Every return and every locally swallowed receive error passes this
				// boundary. Release a native wire stash exactly once first, then surface
				// any durable mutation poison that arose while handling the message.
				peerReceiveLease?.release();
				this.throwIfNativeDurableCommitFailed();
			}
		}
	}

	// -----------------------------------------------------------------
	// Stage-4.5 control-plane receive handlers. Each holds the former
	// `onMessage` branch body VERBATIM behind a small alias preamble that
	// maps the receive-lane context back onto the prelude capture names;
	// the preamble is the only glue. Throws intentionally traverse
	// `onMessage`'s shared catch/finally envelope (error classification,
	// wire-stash release, lease release, durable-poison recheck).
	// -----------------------------------------------------------------

	private isPersistedReceiptRequestSessionCurrent(
		request: RequestPersistedEntriesV1,
		context: ReceiveRequestContext,
		lane: ReceiveLaneContext,
	): boolean {
		const session = lane.session;
		return (
			!!this._persistedReceiptStorage &&
			!context.from.equals(this.node.identity.publicKey) &&
			request.expectedReceiverSession === this.ownTransportSession() &&
			session !== null &&
			session.phase === "open" &&
			this._peerSessions.isCurrent(lane.fromHash, session) &&
			!this._peerSessions.isReplicationInfoBlocked(lane.fromHash) &&
			this._peerSessions.isReceiveCleanupGateOpen(lane.fromHash) &&
			this._peerSyncCapabilitySessions.get(lane.fromHash) ===
				context.message.header.session &&
			this._peerSyncCapabilityTimestamps.has(lane.fromHash) &&
			this._v2Receive.isCurrentActive({
				peerHash: lane.fromHash,
				peerSession: session,
				receiveEpoch: lane.receiveEpoch,
				senderTransportSession: context.message.header.session,
			}) &&
			this.isRepairLifecycleActive(lane.ownershipLifecycleController)
		);
	}

	private admitPersistedReceiptIngress(
		peer: string,
		transportSession: bigint,
		hashCount: number,
		now = Date.now(),
	): boolean {
		// Charge malformed empty/oversized vectors too. The request was already
		// decoded by the transport, so letting an invalid shape bypass this bucket
		// would leave an authenticated peer with an unmetered validation/logging
		// path. Clamp only the accounting cost; shape validation still rejects the
		// request below.
		const hashCost = Math.max(
			1,
			Math.min(MAX_PERSISTED_RECEIPT_HASHES, hashCount),
		);
		let nodeBudget = persistedReceiptIngressBudgets.get(this.node);
		if (!nodeBudget) {
			nodeBudget = {
				requestTokens: PERSISTED_RECEIPT_INGRESS_NODE_REQUEST_CAPACITY,
				hashTokens: PERSISTED_RECEIPT_INGRESS_NODE_HASH_CAPACITY,
				refilledAt: now,
				peerSessions: new Map(),
			};
			persistedReceiptIngressBudgets.set(this.node, nodeBudget);
		}
		refillPersistedReceiptIngressBucket(
			nodeBudget,
			now,
			PERSISTED_RECEIPT_INGRESS_NODE_REQUEST_CAPACITY,
			PERSISTED_RECEIPT_INGRESS_NODE_HASH_CAPACITY,
			PERSISTED_RECEIPT_INGRESS_NODE_REQUESTS_PER_SECOND,
			PERSISTED_RECEIPT_INGRESS_NODE_HASHES_PER_SECOND,
		);

		const peerSessionKey = `${peer}\0${transportSession}`;
		let peerBudget = nodeBudget.peerSessions.get(peerSessionKey);
		if (!peerBudget) {
			while (
				nodeBudget.peerSessions.size >=
				MAX_PERSISTED_RECEIPT_INGRESS_PEER_SESSIONS
			) {
				const oldest = nodeBudget.peerSessions.keys().next().value;
				if (oldest === undefined) break;
				nodeBudget.peerSessions.delete(oldest);
			}
			peerBudget = {
				requestTokens: PERSISTED_RECEIPT_INGRESS_PEER_REQUEST_CAPACITY,
				hashTokens: PERSISTED_RECEIPT_INGRESS_PEER_HASH_CAPACITY,
				refilledAt: now,
			};
			nodeBudget.peerSessions.set(peerSessionKey, peerBudget);
		} else {
			// Refresh insertion order so bounded eviction prefers inactive sessions.
			nodeBudget.peerSessions.delete(peerSessionKey);
			nodeBudget.peerSessions.set(peerSessionKey, peerBudget);
		}
		refillPersistedReceiptIngressBucket(
			peerBudget,
			now,
			PERSISTED_RECEIPT_INGRESS_PEER_REQUEST_CAPACITY,
			PERSISTED_RECEIPT_INGRESS_PEER_HASH_CAPACITY,
			PERSISTED_RECEIPT_INGRESS_PEER_REQUESTS_PER_SECOND,
			PERSISTED_RECEIPT_INGRESS_PEER_HASHES_PER_SECOND,
		);

		if (
			nodeBudget.requestTokens < 1 ||
			nodeBudget.hashTokens < hashCost ||
			peerBudget.requestTokens < 1 ||
			peerBudget.hashTokens < hashCost
		) {
			return false;
		}
		nodeBudget.requestTokens -= 1;
		nodeBudget.hashTokens -= hashCost;
		peerBudget.requestTokens -= 1;
		peerBudget.hashTokens -= hashCost;
		return true;
	}

	private async handleRequestPersistedEntriesV1(
		request: RequestPersistedEntriesV1,
		context: ReceiveRequestContext,
		lane: ReceiveLaneContext,
	): Promise<ConfirmEntriesMessage | undefined> {
		if (!this.isPersistedReceiptRequestSessionCurrent(request, context, lane)) {
			return undefined;
		}
		if (
			!this.admitPersistedReceiptIngress(
				lane.fromHash,
				context.message.header.session,
				request.hashes.length,
			)
		) {
			return undefined;
		}
		// Charge the request before shape validation and attacker-controlled CID
		// parsing. Invalid shapes are rejected quietly here so they cannot turn the
		// outer receive error logger into a post-budget work amplifier.
		try {
			this.validatePersistedReceiptRequestShape(request);
		} catch {
			return undefined;
		}
		if (!this.hasValidPersistedReceiptHashes(request)) {
			return undefined;
		}
		const peerInFlight =
			this._persistedReceiptRequestsInFlight.get(lane.fromHash) ?? 0;
		if (
			peerInFlight >= MAX_PERSISTED_RECEIPT_REQUESTS_PER_PEER ||
			this._persistedReceiptRequestsInFlightTotal >=
				MAX_PERSISTED_RECEIPT_REQUESTS_GLOBAL
		) {
			return undefined;
		}
		this._persistedReceiptRequestsInFlight.set(lane.fromHash, peerInFlight + 1);
		this._persistedReceiptRequestsInFlightTotal++;

		try {
			// A positive receipt requires the block itself. Reject entirely absent
			// batches before the serialized mutation lane and, critically, before a
			// request can force unrelated pending index/coordinate journals durable.
			// A concurrent put can yield a harmless false negative: the sender retries.
			const blockPresence = await this.remoteBlocks.localStore.hasMany(
				request.hashes,
			);
			const presentBlockHashes = request.hashes.filter(
				(_hash, index) => blockPresence[index] === true,
			);
			if (
				!this.isPersistedReceiptRequestSessionCurrent(request, context, lane)
			) {
				return undefined;
			}
			if (presentBlockHashes.length === 0) {
				return new ConfirmEntriesMessage({ hashes: [] });
			}
			this._liveness.markReplicatorActivity(lane.fromHash);
			return await this.withReplicationRangeMutationQueue(async () => {
				if (
					!this.isPersistedReceiptRequestSessionCurrent(request, context, lane)
				) {
					return undefined;
				}
				this.throwIfNativeDurableCommitFailed();
				const storage = this.resolvePersistedReceiptStorage();
				if (!storage) {
					return undefined;
				}

				await this.log.entryIndex.flushPendingWrites(presentBlockHashes);
				await this._coordinates.flushNativeBackboneCoordinateJournal();
				const candidates = new Map(
					await Promise.all(
						presentBlockHashes.map(async (hash) => {
							const [blockPresent, lowerRow, coordinate] = await Promise.all([
								this.remoteBlocks.localStore.has(hash),
								this.log.entryIndex.properties.index.get(toId(hash)),
								this._coordinates.getAuthoritativeCoordinateEntryForReceipt(
									hash,
								),
							]);
							return [hash, { blockPresent, lowerRow, coordinate }] as const;
						}),
					),
				);
				const presentHashes = presentBlockHashes.filter((hash) => {
					const candidate = candidates.get(hash)!;
					return (
						candidate.blockPresent &&
						candidate.lowerRow != null &&
						candidate.coordinate != null
					);
				});
				if (presentHashes.length === 0) {
					return new ConfirmEntriesMessage({ hashes: [] });
				}
				await Promise.all(
					[...new Set([storage.block, storage.lower, storage.coordinate])].map(
						(store) => store.barrier(),
					),
				);
				this.throwIfNativeDurableCommitFailed();
				if (
					!this.isPersistedReceiptRequestSessionCurrent(request, context, lane)
				) {
					return undefined;
				}

				const ownershipRevision =
					this._instanceLifecycle?._receiveOwnershipRevision ?? 0;
				if (!this.isReceiveOwnershipSnapshotStable(ownershipRevision)) {
					return new ConfirmEntriesMessage({ hashes: [] });
				}

				const selfHash = this.node.identity.publicKey.hashcode();
				const confirmed: string[] = [];
				for (const hash of presentHashes) {
					const candidate = candidates.get(hash)!;
					if (
						!candidate.blockPresent ||
						!candidate.lowerRow ||
						!candidate.coordinate ||
						this._checkedPrune.hasActiveWork(hash) ||
						!this.isPersistedReceiptRequestSessionCurrent(
							request,
							context,
							lane,
						) ||
						!this.isReceiveOwnershipSnapshotStable(ownershipRevision)
					) {
						continue;
					}

					const [blockPresent, lowerRow, coordinate] = await Promise.all([
						this.remoteBlocks.localStore.has(hash),
						this.log.entryIndex.properties.index.get(toId(hash)),
						this._coordinates.getAuthoritativeCoordinateEntryForReceipt(hash),
					]);
					if (!blockPresent || !lowerRow || !coordinate) {
						continue;
					}

					const replicas = decodeReplicas(coordinate).getValue(this);
					const leaders = await this.findLeadersFromEntry(
						coordinate,
						replicas,
						{ freshLeaderPlan: true },
						lane.ownershipLifecycleController,
					);
					if (!leaders.has(selfHash)) {
						continue;
					}
					if (
						!this._checkedPrune.hasActiveWork(hash) &&
						this.isReceiveOwnershipSnapshotStable(ownershipRevision) &&
						this.isPersistedReceiptRequestSessionCurrent(request, context, lane)
					) {
						confirmed.push(hash);
					}
				}

				if (
					confirmed.length === 0 ||
					!this.isReceiveOwnershipSnapshotStable(ownershipRevision) ||
					!this.isPersistedReceiptRequestSessionCurrent(request, context, lane)
				) {
					return new ConfirmEntriesMessage({ hashes: [] });
				}
				const finalRows = await Promise.all(
					confirmed.map(async (hash) => {
						const [block, lower, coordinate] = await Promise.all([
							this.remoteBlocks.localStore.has(hash),
							this.log.entryIndex.properties.index.get(toId(hash)),
							this._coordinates.getAuthoritativeCoordinateEntryForReceipt(hash),
						]);
						return { hash, block, lower, coordinate };
					}),
				);
				if (
					!this.isReceiveOwnershipSnapshotStable(ownershipRevision) ||
					!this.isPersistedReceiptRequestSessionCurrent(request, context, lane)
				) {
					return new ConfirmEntriesMessage({ hashes: [] });
				}
				return new ConfirmEntriesMessage({
					hashes: finalRows
						.filter(
							(row) =>
								row.block &&
								row.lower &&
								row.coordinate &&
								!this._checkedPrune.hasActiveWork(row.hash),
						)
						.map((row) => row.hash),
				});
			}, lane.ownershipLifecycleController);
		} finally {
			const remaining =
				(this._persistedReceiptRequestsInFlight.get(lane.fromHash) ?? 1) - 1;
			if (remaining <= 0) {
				this._persistedReceiptRequestsInFlight.delete(lane.fromHash);
			} else {
				this._persistedReceiptRequestsInFlight.set(lane.fromHash, remaining);
			}
			this._persistedReceiptRequestsInFlightTotal = Math.max(
				0,
				this._persistedReceiptRequestsInFlightTotal - 1,
			);
		}
	}

	private async handleRequestIPruneV2(
		msg: RequestIPruneV2,
		context: ReceiveRequestContext,
		lane: ReceiveLaneContext,
	): Promise<void> {
		const syncProfile = lane.syncProfile;
		const receiveReplicationLifecycleController = lane.lifecycleController;
		const requestPruneStartedAt = syncProfileStart(syncProfile);
		const from = context.from.hashcode();
		const requestsByHash = new Map<string, Uint8Array>();
		for (const request of msg.requests) {
			if (requestsByHash.has(request.hash)) {
				return;
			}
			requestsByHash.set(request.hash, request.requestId);
		}
		const hashes = [...requestsByHash.keys()];
		if (hashes.length === 0) {
			return;
		}

		const coordinatorCleanupStartedAt = syncProfileStart(syncProfile);
		this.removeEntriesKnownByPeer(hashes, from);
		// A prune request means the sender is preparing to stop retaining these
		// hashes. Any receipt it gave our current generation is therefore stale,
		// even when we cannot grant its reciprocal request.
		this._checkedPrune.removeRequestsSent(hashes, from);
		if (syncProfile) {
			emitSyncProfileDuration(syncProfile, coordinatorCleanupStartedAt, {
				name: "sharedLog.receive.requestPrune.coordinatorCleanup",
				component: "shared-log",
				entries: hashes.length,
				messages: 1,
				details: { hashes: hashes.length },
			});
		}

		const admission = await this.admitAndSendCheckedPruneGrants(
			from,
			msg.requests,
		);
		if (
			!this.isReplicationLifecycleActive(receiveReplicationLifecycleController)
		) {
			return;
		}

		let pendingIHaveCreated = 0;
		let pendingIHaveExtended = 0;
		for (const request of admission.missing) {
			const previous = this._pendingIHave.get(request.hash);
			if (previous) {
				pendingIHaveExtended += 1;
				previous.requesting.set(from, request.requestId);
				previous.resetTimeout();
				continue;
			}

			pendingIHaveCreated += 1;
			const requesting = new Map([[from, request.requestId]]);
			let pendingIHave!: PendingIHave<T>;
			pendingIHave = {
				requesting,
				resetTimeout: () => this.resetPendingIHaveTimeout(pendingIHave),
				clear: () => this.clearPendingIHaveTimeout(pendingIHave),
				callback: async (entry: Entry<T>) => {
					if (
						!this.isReplicationLifecycleActive(
							receiveReplicationLifecycleController,
						) ||
						requesting.size === 0
					) {
						return;
					}
					for (const requester of requesting.keys()) {
						this.removePeerFromGidPeerHistory(requester, entry.meta.gid);
					}
					await Promise.all(
						[...requesting].map(([requester, requestId]) =>
							this.admitAndSendCheckedPruneGrants(requester, [
								{ hash: entry.hash, requestId },
							]),
						),
					);
				},
			};
			this._pendingIHave.set(request.hash, pendingIHave);
			this.resetPendingIHaveTimeout(pendingIHave);
		}

		// Close the arrival race between the in-lane presence check and
		// installing pending-IHave. If admission completed in that window,
		// run the same callback that onEntryAdded would have run.
		for (const request of admission.missing) {
			const pendingIHave = this._pendingIHave.get(request.hash);
			if (!pendingIHave) {
				continue;
			}
			let entry: Entry<T> | undefined;
			try {
				if (await this.log.blocks.has(request.hash)) {
					entry = (await this.log.get(request.hash)) as Entry<T> | undefined;
				}
			} catch {
				// The normal entry-admission hook will retry this path.
			}
			if (entry && this._pendingIHave.get(request.hash) === pendingIHave) {
				pendingIHave.clear();
				this.runPendingIHaveCallback(pendingIHave, entry);
			}
		}

		if (syncProfile) {
			emitSyncProfileDuration(syncProfile, requestPruneStartedAt, {
				name: "sharedLog.receive.requestPrune.total",
				component: "shared-log",
				entries: hashes.length,
				messages: 1,
				details: {
					presentEntries: hashes.length - admission.missing.length,
					leaderResponses: admission.admitted.length,
					pendingIHaveCreated,
					pendingIHaveExtended,
				},
			});
		}
	}

	private async handleResponseIPruneV2(
		msg: ResponseIPruneV2,
		context: ReceiveRequestContext,
		_lane: ReceiveLaneContext,
	): Promise<void> {
		const responseHashes = new Set<string>();
		for (const request of msg.requests) {
			if (responseHashes.has(request.hash)) {
				return;
			}
			responseHashes.add(request.hash);
		}
		const responseTasks: Promise<void>[] = [];
		for (const request of msg.requests) {
			const pendingDelete = this._checkedPrune.getPendingDelete(request.hash);
			if (pendingDelete) {
				responseTasks.push(
					Promise.resolve(
						pendingDelete.resolve(context.from.hashcode(), request.requestId),
					),
				);
			}
		}
		const results = await Promise.allSettled(responseTasks);
		for (const result of results) {
			if (result.status === "rejected") {
				logger.error(result.reason?.toString?.() ?? String(result.reason));
			}
		}
	}

	private async handleReplicationInfoV2Announcement(
		msg: ReplicationInfoV2Message,
		context: ReceiveRequestContext,
		lane: ReceiveLaneContext,
	): Promise<void> {
		const from = context.from;
		const fromHash = lane.fromHash;
		const receiveSession = lane.session;
		if (
			from.equals(this.node.identity.publicKey) ||
			receiveSession === null ||
			receiveSession.phase !== "open"
		) {
			return;
		}

		// Authenticate and reserve before the per-peer lane. One reservation per
		// peer bounds decoded-frame retention while another mutation is parked.
		const receiveState = this._v2Receive._receiveStates.get(fromHash);
		if (
			!receiveState ||
			receiveState.peerSession !== receiveSession ||
			receiveState.senderTransportSession !== context.message.header.session
		) {
			return;
		}
		const admission = this._v2Receive.reserve(msg, {
			from,
			peerSession: receiveSession,
			receiveEpoch: lane.receiveEpoch,
			senderTransportSession: context.message.header.session,
			transportTimestamp: context.message.header.timestamp,
		});
		if (!admission) {
			return;
		}

		lane.lease.release();
		try {
			await this.withReplicationInfoApplyQueue(fromHash, async () => {
				const hostGate = () =>
					this._instanceLifecycle!.isMembershipActiveFor(
						lane.lifecycleController,
					) &&
					this.isRepairLifecycleActive(lane.ownershipLifecycleController) &&
					this._peerSessions.isCurrent(fromHash, receiveSession) &&
					receiveSession.phase === "open" &&
					this._peerSessions.isReceiveEpochCurrent(
						fromHash,
						lane.receiveEpoch,
					) &&
					!this._peerSessions.isReplicationInfoBlocked(fromHash) &&
					this._peerSessions.isReceiveCleanupGateOpen(fromHash);
				if (!hostGate()) {
					return;
				}
				const exactGate = () =>
					hostGate() && this._v2Receive.isAdmissionCurrent(admission);
				let durableCommitted = false;

				try {
					if (
						msg instanceof FullReplicationInfoV2Message ||
						msg instanceof AddedReplicationInfoV2Message
					) {
						let mutationGateChecked = false;
						let mutationGateAdmitted = false;
						const result = await this.addReplicationRange(
							msg.segments.map((segment) =>
								segment.toReplicationRangeIndexable(from),
							),
							from,
							{
								reset: msg instanceof FullReplicationInfoV2Message,
								checkDuplicates: true,
								timestamp: Number(context.message.header.timestamp),
								allowOrderedReplacementPairs:
									msg instanceof AddedReplicationInfoV2Message,
								shouldApply: () => {
									mutationGateChecked = true;
									mutationGateAdmitted = exactGate();
									return mutationGateAdmitted;
								},
								onDurableApplyCommitted: () => {
									if (!exactGate() || !this._v2Receive.commit(admission)) {
										return false;
									}
									durableCommitted = true;
									return true;
								},
							},
							lane.ownershipLifecycleController,
						);
						if (
							result === undefined ||
							!mutationGateChecked ||
							!mutationGateAdmitted ||
							!durableCommitted ||
							!exactGate()
						) {
							return;
						}
					} else {
						if (!exactGate()) {
							return;
						}
						const rangesToRemove =
							await this.resolveReplicationRangesFromIdsAndKey(
								msg.segmentIds,
								from,
							);
						if (!exactGate()) {
							return;
						}
						let mutationGateAdmitted = true;
						const removedRanges: ReplicationRangeIndexable<R>[] = [];
						const removed = await this.removeReplicationRanges(
							rangesToRemove,
							from,
							{
								shouldRemove: () => {
									mutationGateAdmitted = exactGate();
									return mutationGateAdmitted;
								},
								onDurableRemoveCommitted: () => {
									if (!exactGate() || !this._v2Receive.commit(admission)) {
										return false;
									}
									durableCommitted = true;
									return true;
								},
								onRemoved: (ranges) => removedRanges.push(...ranges),
							},
							lane.ownershipLifecycleController,
						);
						if (!durableCommitted) {
							if (
								!mutationGateAdmitted ||
								!exactGate() ||
								removed ||
								!this._v2Receive.commit(admission)
							) {
								return;
							}
							durableCommitted = true;
						}
						if (
							this._instanceLifecycle!.isMembershipActiveFor(
								lane.lifecycleController,
							) &&
							this.isRepairLifecycleActive(lane.ownershipLifecycleController)
						) {
							const timestamp = BigInt(Date.now());
							for (const range of removedRanges) {
								this.replicationChangeDebounceFn.add({
									range,
									type: "removed",
									timestamp,
								});
							}
						}
					}
				} catch (error) {
					this._v2Receive.requireFullAfterFailure(admission);
					throw error;
				}

				if (!durableCommitted) {
					if (!exactGate() || !this._v2Receive.commit(admission)) {
						return;
					}
				}
				if (!hostGate()) {
					return;
				}
				this._liveness.markReplicatorActivity(fromHash);
				// A committed V2 announcement is applied progress: the peer answers,
				// so recovery re-solicitation may restart from the base interval.
				this.resetReplicationInfoV2RecoveryEscalation(fromHash);
				this.dispatchPersistedReceiptReadinessChange(fromHash);
			});
		} finally {
			this._v2Receive.release(admission);
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
		const nativeCoordinateState =
			this._nativeBackbone ?? this._nativeSharedLogState;
		if (nativeCoordinateState && !this.hasCustomFindLeaders()) {
			const includeAssignedToRangeBoundary =
				options?.strict !== true &&
				(myRanges.length === 0 ||
					myRanges.some((range) => range.mode === ReplicationIntent.NonStrict));
			return nativeCoordinateState.countEntryCoordinatesInRanges(myRanges, {
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

	/** Return known replicator hashes from the replication index. */
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
		this.throwIfNativeDurableCommitFailed();
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const throwIfInactive = () =>
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		throwIfInactive();
		let entriesToReplicate: Entry<T>[] = [];
		const localHashes =
			options?.replicate && this.log.length > 0
				? await this.log.entryIndex.hasMany(
						entries.map((element) =>
							typeof element === "string" ? element : element.hash,
						),
					)
				: new Set<string>();
		throwIfInactive();
		if (options?.replicate && this.log.length > 0) {
			// Replicate entries that are already joined locally; join ignores them.
			for (const element of entries) {
				throwIfInactive();
				if (typeof element === "string") {
					if (localHashes.has(element)) {
						const entry = await this.log.get(element);
						throwIfInactive();
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
						throwIfInactive();
						if (entry) {
							entriesToReplicate.push(entry);
						}
					}
				}
			}
		}

		const onChangeForReplication = options?.replicate
			? async (change: Change<T>) => {
					throwIfInactive();
					if (change.added) {
						for (const entry of change.added) {
							throwIfInactive();
							if (entry.head) {
								entriesToReplicate.push(entry.entry);
							}
						}
					}
					throwIfInactive();
				}
			: undefined;

		let assumeSynced =
			options?.replicate &&
			typeof options.replicate !== "boolean" &&
			options.replicate.assumeSynced;
		const seedAssumeSyncedPeerHistory = async (entry: Entry<T>) => {
			throwIfInactive();
			if (!assumeSynced) {
				return;
			}

			const minReplicas = decodeReplicas(entry).getValue(this);
			const { leaders } = await this.planEntryLeaders(
				entry,
				minReplicas,
				{
					roleAge: 0,
					persist: false,
				},
				ownershipLifecycleController,
			);

			throwIfInactive();
			this.addPeersToGidPeerHistory(entry.meta.gid, leaders.keys());
		};
		const persistCoordinate = async (entry: Entry<T>) => {
			throwIfInactive();
			const minReplicas = decodeReplicas(entry).getValue(this);
			const { leaders } = await this.planEntryLeaders(
				entry,
				minReplicas,
				{
					persist: {},
				},
				ownershipLifecycleController,
			);

			throwIfInactive();
			if (assumeSynced) {
				// make sure we dont start to initate syncing process outwards for this entry
				this.addPeersToGidPeerHistory(entry.meta.gid, leaders.keys());
			}
		};
		let entriesToPersist: Entry<T>[] = [];
		let joinOptions = {
			...options,
			onChange: async (change: Change<T>) => {
				throwIfInactive();
				await onChangeForReplication?.(change);
				throwIfInactive();
				for (const entry of change.added) {
					throwIfInactive();
					if (!entry.head) {
						continue;
					}

					if (!options?.replicate) {
						// we persist coordinates for all added entries here

						await persistCoordinate(entry.entry);
						throwIfInactive();
					} else {
						// else we persist after replication range update has been done so that
						// the indexed info becomes up to date
						entriesToPersist.push(entry.entry);
					}
				}
			},
		};

		throwIfInactive();
		await this.log.join(entries, joinOptions);
		throwIfInactive();

		if (options?.replicate) {
			let messageToSend: AddedReplicationInfoMutation | undefined = undefined;

			if (assumeSynced) {
				throwIfInactive();
				// `assumeSynced` is an explicit contract that this join should trust the
				// supplied history and avoid initiating outbound repair while the local
				// replication ranges settle.
				this._assumeSyncedRepairSuppressedUntil =
					Date.now() + ASSUME_SYNCED_REPAIR_SUPPRESSION_MS;
				for (const entry of entriesToReplicate) {
					await seedAssumeSyncedPeerHistory(entry);
					throwIfInactive();
				}
			}

			throwIfInactive();
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
					throwIfInactive();
					if (!("added" in msg)) {
						throw new Error("Unexpected");
					}

					if (messageToSend) {
						// merge segments to make it into one messages
						for (const segment of msg.added.segments) {
							messageToSend.added.segments.push(segment);
						}
					} else {
						messageToSend = msg;
					}
				},
			});
			throwIfInactive();

			// it is importat that we call persistCoordinate after this.replicate(entries) as else there might be a prune job deleting the entry before replication duties has been assigned to self
			for (const entry of entriesToPersist) {
				await persistCoordinate(entry);
				throwIfInactive();
			}

			if (messageToSend) {
				await this._announcements.sendReplicationAnnouncement(
					messageToSend,
					ownershipLifecycleController,
				);
				throwIfInactive();
			}
		}
		throwIfInactive();
	}

	private nudgePersistedReceiptPeerReadiness(publicKey: PublicSignKey): void {
		if (this.closed) return;
		const peerHash = publicKey.hashcode();
		const peerSession = this._peerSessions.current(peerHash);
		if (
			!peerSession ||
			peerSession.phase === "departing" ||
			(peerSession.phase === "opening" &&
				!peerSession.openingBarrierActive)
		) {
			// A barrier rejection deliberately leaves the current session in its
			// fail-closed opening phase after the barrier window has settled. Ask the
			// authenticated peer for a fresh subscriber snapshot so the replacement
			// session can recover; never rotate a barrier that is still in flight.
			this.requestSubscriberSnapshotForCapability(publicKey);
			return;
		}
		if (peerSession.phase !== "open" || !peerSession.isActive()) {
			return;
		}
		const receiveEpoch = this._peerSessions.receiveEpoch(peerHash);
		this.promoteReplicationInfoV2ReceiveCapability(publicKey, peerSession);
		this._v2Receive.reAdvertiseLocalCapabilityForRecovery({
			peerHash,
			peerSession,
			receiveEpoch,
		});
		this._v2Receive.ensureRequestProgress({
			peerHash,
			peerSession,
			receiveEpoch,
		});
		this.scheduleReplicationInfoV2Recovery(publicKey);
	}

	/**
	 * Inspect whether one public key's exact current connection generation can
	 * supply persisted-receipt evidence. The returned object is frozen and never
	 * exposes the internal PeerSession token. When `entries` are supplied, the
	 * peer must also be present in a fresh leader plan for every entry.
	 *
	 * This is advisory preflight state. Persisted delivery repeats every
	 * generation, leadership, ownership and storage check at receipt time; a
	 * `ready` snapshot is never itself authority to dispose a source copy.
	 */
	async getPersistedReceiptPeerReadiness(
		key: PublicSignKey,
		options: PersistedReceiptPeerReadinessOptions<T, R> = {},
	): Promise<PersistedReceiptPeerReadiness> {
		return this.inspectPersistedReceiptPeerReadiness(key, options);
	}

	private async inspectPersistedReceiptPeerReadiness(
		key: PublicSignKey,
		options: PersistedReceiptPeerReadinessOptions<T, R>,
		assertContinue?: () => void,
	): Promise<PersistedReceiptPeerReadiness> {
		// Capture and validate caller-owned planning input before consulting live
		// peer state. Invalid options must not appear to work merely because the
		// peer is currently absent, then fail later when the same session connects.
		const entries = options.entries ? [...options.entries] : [];
		const replicas =
			options.replicas ??
			(entries.length > 0 ? this.replicas.min.getValue(this) : undefined);
		if (replicas !== undefined) {
			if (!Number.isSafeInteger(replicas) || replicas <= 0) {
				throw new RangeError(
					"Persisted-receipt readiness replicas must be a positive integer",
				);
			}
			checkMinReplicasLimit(replicas);
		}

		const peerHash = key.hashcode();
		const captured = this.persistedReceiptReadinessCandidate(peerHash);
		if ("status" in captured) {
			return captured;
		}
		assertContinue?.();

		if (entries.length > 0) {
			const ownershipLifecycleController =
				this.captureReplicationOwnershipLifecycle();
			const ownershipRevision =
				this._instanceLifecycle?._receiveOwnershipRevision ?? 0;
			if (!this.isReceiveOwnershipSnapshotStable(ownershipRevision)) {
				return this.pendingPersistedReceiptReadiness(
					"ownership-changing",
					captured.generation,
				);
			}
			for (const entry of entries) {
				assertContinue?.();
				const leaders = await this.findLeadersFromEntry(
					entry,
					replicas!,
					{ freshLeaderPlan: true },
					ownershipLifecycleController,
				);
				assertContinue?.();
				if (!this.isReceiveOwnershipSnapshotStable(ownershipRevision)) {
					return this.pendingPersistedReceiptReadiness(
						"ownership-changing",
						captured.generation,
					);
				}
				const current = this.persistedReceiptReadinessCandidate(peerHash);
				if ("status" in current) {
					return current;
				}
				if (
					current.peerSession !== captured.peerSession ||
					current.receiveEpoch !== captured.receiveEpoch ||
					current.capabilitySession !== captured.capabilitySession
				) {
					return this.pendingPersistedReceiptReadiness(
						"replication-state-pending",
						current.generation,
					);
				}
				if (!leaders.has(peerHash)) {
					return this.pendingPersistedReceiptReadiness(
						"not-entry-leader",
						captured.generation,
					);
				}
			}
		}

		assertContinue?.();
		const current = this.persistedReceiptReadinessCandidate(peerHash);
		if ("status" in current) {
			return current;
		}
		if (
			current.peerSession !== captured.peerSession ||
			current.receiveEpoch !== captured.receiveEpoch ||
			current.capabilitySession !== captured.capabilitySession
		) {
			return this.pendingPersistedReceiptReadiness(
				"replication-state-pending",
				current.generation,
			);
		}
		if (
			!this._v2Send.isLatestConfirmedForPeer({
				peerHash,
				peerSession: captured.peerSession,
				receiverTransportSession: captured.capabilitySession,
			})
		) {
			return this.pendingPersistedReceiptReadiness(
				"replication-confirmation-pending",
				captured.generation,
			);
		}
		return Object.freeze({
			status: "ready" as const,
			generation: captured.generation,
		});
	}

	/**
	 * Wait for a public key's current (or replacement) connection generation to
	 * become persisted-receipt ready. Transition listeners are installed before
	 * the first asynchronous inspection, and a bounded recovery tick repairs
	 * missed subscriber/capability wakes without retaining stale PeerSessions.
	 * This waiter is advisory only; the following persisted delivery remains the
	 * operation that proves the requested remote durability quorum.
	 */
	async waitForPersistedReceiptPeerReadiness(
		key: PublicSignKey,
		options: WaitForPersistedReceiptPeerReadinessOptions<T, R> = {},
	): Promise<PersistedReceiptPeerReady> {
		if (this.closed) {
			throw new ClosedError();
		}
		const timeoutMs = options.timeout ?? this.waitForReplicatorTimeout;
		if (
			!Number.isSafeInteger(timeoutMs) ||
			timeoutMs <= 0 ||
			timeoutMs > MAX_PERSISTED_DELIVERY_TIMEOUT_MS
		) {
			throw new RangeError(
				`Persisted-receipt readiness timeout must be an integer from 1 to ${MAX_PERSISTED_DELIVERY_TIMEOUT_MS} milliseconds`,
			);
		}
		if (options.signal?.aborted) {
			throw options.signal.reason instanceof Error
				? options.signal.reason
				: new AbortError("Persisted-receipt readiness wait aborted");
		}

		// Capture caller-owned inputs before reserving a waiter slot. A throwing
		// iterator/key implementation must not strand capacity permanently.
		const entries = options.entries ? [...options.entries] : undefined;
		const inspectOptions: PersistedReceiptPeerReadinessOptions<T, R> = {
			...(entries ? { entries } : {}),
			...(options.replicas === undefined ? {} : { replicas: options.replicas }),
		};
		const peerHash = key.hashcode();
		const waiterSet = this._persistedReceiptReadinessWaiters;
		if (waiterSet.size >= MAX_PERSISTED_RECEIPT_READINESS_WAITERS) {
			throw new RangeError(
				`Too many pending persisted-receipt readiness waits (maximum ${MAX_PERSISTED_RECEIPT_READINESS_WAITERS})`,
			);
		}
		const waiterToken = {};
		waiterSet.add(waiterToken);
		const deadline = Date.now() + timeoutMs;
		const closeSignal = this._closeController.signal;
		const operationController = new AbortController();
		const operationSignal = AbortSignal.any(
			[options.signal, closeSignal, operationController.signal].filter(
				(value): value is AbortSignal => value !== undefined,
			),
		);
		const deferred = pDefer<PersistedReceiptPeerReady>();
		let settled = false;
		let checkScheduled = false;
		let checkInFlight = false;
		let rerun = false;
		let recoveryTimer: ReturnType<typeof setTimeout> | undefined;
		let confirmationController: AbortController | undefined;
		let lastSnapshot: PersistedReceiptPeerReadiness | undefined;
		const createTimeoutError = () => {
			const suffix = lastSnapshot
				? ` (last status: ${lastSnapshot.status}${
						"reason" in lastSnapshot ? `/${lastSnapshot.reason}` : ""
					})`
				: "";
			return new TimeoutError(
				`Timeout waiting for persisted-receipt readiness from ${peerHash}${suffix}`,
			);
		};

		const cleanup = () => {
			waiterSet.delete(waiterToken);
			this.events.removeEventListener(
				"persisted-receipt:readiness",
				onReadinessChange,
			);
			this.events.removeEventListener("replication:change", onRoleChange);
			this.events.removeEventListener("replicator:mature", onRoleChange);
			options.signal?.removeEventListener("abort", onCallerAbort);
			closeSignal.removeEventListener("abort", onClose);
			if (recoveryTimer) {
				clearTimeout(recoveryTimer);
				recoveryTimer = undefined;
			}
			confirmationController?.abort(
				new AbortError("Persisted-receipt readiness generation changed"),
			);
			confirmationController = undefined;
			operationController.abort(
				new AbortError("Persisted-receipt readiness wait settled"),
			);
		};
		const resolve = (snapshot: PersistedReceiptPeerReady) => {
			if (settled) return;
			settled = true;
			cleanup();
			deferred.resolve(snapshot);
		};
		const reject = (error: unknown) => {
			if (settled) return;
			settled = true;
			cleanup();
			deferred.reject(
				error instanceof Error ? error : new Error(String(error)),
			);
		};
		const onCallerAbort = () =>
			reject(
				options.signal?.reason instanceof Error
					? options.signal.reason
					: new AbortError("Persisted-receipt readiness wait aborted"),
			);
		const onClose = () => reject(new ClosedError());
		const continueWait = () => {
			if (settled) return false;
			if (closeSignal.aborted) {
				onClose();
				return false;
			}
			if (options.signal?.aborted) {
				onCallerAbort();
				return false;
			}
			if (Date.now() >= deadline) {
				reject(createTimeoutError());
				return false;
			}
			return true;
		};
		const assertInspectionCurrent = () => {
			if (!continueWait()) {
				throw new AbortError("Persisted-receipt readiness wait settled");
			}
		};
		const armRecoveryTick = () => {
			if (settled || recoveryTimer) return;
			const delayMs = Math.max(
				50,
				Math.min(1_000, this.waitForReplicatorRequestIntervalMs),
			);
			recoveryTimer = setTimeout(() => {
				recoveryTimer = undefined;
				if (!continueWait()) return;
				this.nudgePersistedReceiptPeerReadiness(key);
				scheduleCheck();
			}, delayMs);
			recoveryTimer.unref?.();
		};
		const runCheck = async () => {
			checkScheduled = false;
			if (!continueWait()) return;
			if (checkInFlight) {
				rerun = true;
				return;
			}
			checkInFlight = true;
			try {
				let snapshot = await this.inspectPersistedReceiptPeerReadiness(
					key,
					inspectOptions,
					assertInspectionCurrent,
				);
				lastSnapshot = snapshot;
				if (!continueWait()) return;
				if (rerun) return;
				if (snapshot.status === "ready") {
					// A wake observed while the asynchronous inspection was running may
					// already have invalidated this snapshot. Drain that coalesced wake
					// before publishing readiness.
					resolve(snapshot);
					return;
				}
				if (
					snapshot.status === "pending" &&
					snapshot.reason === "replication-confirmation-pending"
				) {
					const target = this.persistedReceiptPeerSession(peerHash);
					if (target) {
						const currentConfirmationController = new AbortController();
						confirmationController = currentConfirmationController;
						try {
							await this._v2Send.confirmLatestForPeer(
								{
									peerHash,
									peerSession: target.peerSession,
									receiverTransportSession: target.capabilitySession,
								},
								{
									timeout: Math.max(1, deadline - Date.now()),
									signal: AbortSignal.any([
										operationSignal,
										currentConfirmationController.signal,
									]),
								},
							);
						} catch (error) {
							if (!continueWait()) return;
							if (!(error instanceof AbortError)) {
								throw error;
							}
							rerun = true;
						} finally {
							if (confirmationController === currentConfirmationController) {
								confirmationController = undefined;
							}
						}
						if (!continueWait()) return;
						snapshot = await this.inspectPersistedReceiptPeerReadiness(
							key,
							inspectOptions,
							assertInspectionCurrent,
						);
						lastSnapshot = snapshot;
						if (!continueWait()) return;
						if (rerun) return;
						if (snapshot.status === "ready") {
							resolve(snapshot);
							return;
						}
					}
				}
				if (!continueWait()) return;
				this.nudgePersistedReceiptPeerReadiness(key);
			} catch (error) {
				if (!settled) reject(error);
			} finally {
				checkInFlight = false;
				if (!settled && rerun) {
					rerun = false;
					scheduleCheck();
				} else {
					armRecoveryTick();
				}
			}
		};
		const scheduleCheck = (interruptConfirmation = false) => {
			if (settled) return;
			if (recoveryTimer) {
				clearTimeout(recoveryTimer);
				recoveryTimer = undefined;
			}
			if (checkInFlight) {
				rerun = true;
				if (interruptConfirmation) {
					confirmationController?.abort(
						new AbortError(
							"Persisted-receipt readiness changed during confirmation",
						),
					);
				}
				return;
			}
			if (checkScheduled) return;
			checkScheduled = true;
			void Promise.resolve().then(runCheck);
		};
		const onReadinessChange = (
			event: CustomEvent<PersistedReceiptPeerReadinessEvent>,
		) => {
			if (event.detail.peerHash === peerHash) scheduleCheck(true);
		};
		const onRoleChange = (event: CustomEvent<ReplicationChangeEvent>) => {
			if (
				(entries?.length ?? 0) > 0 ||
				event.detail.publicKey.hashcode() === peerHash
			) {
				scheduleCheck(true);
			}
		};

		// Register wake sources before the first state inspection. EventTarget does
		// not replay a transition that fired between an async check and registration.
		this.events.addEventListener(
			"persisted-receipt:readiness",
			onReadinessChange,
		);
		this.events.addEventListener("replication:change", onRoleChange);
		this.events.addEventListener("replicator:mature", onRoleChange);
		options.signal?.addEventListener("abort", onCallerAbort, { once: true });
		closeSignal.addEventListener("abort", onClose, { once: true });
		if (options.signal?.aborted) {
			onCallerAbort();
		} else if (closeSignal.aborted) {
			onClose();
		} else {
			scheduleCheck();
		}

		const timeout = setTimeout(() => reject(createTimeoutError()), timeoutMs);
		timeout.unref?.();
		return deferred.promise.finally(() => clearTimeout(timeout));
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
		if (options?.signal?.aborted) {
			throw new AbortError();
		}
		const deferred = pDefer<void>();
		const timeoutMs = options?.timeout ?? this.waitForReplicatorTimeout;
		const resolvedRoleAge = options?.eager
			? undefined
			: (options?.roleAge ?? (await this.getDefaultMinRoleAge()));
		if (options?.signal?.aborted) {
			throw new AbortError();
		}

		let settled = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		let requestTimer: ReturnType<typeof setTimeout> | undefined;
		let checkInFlight = false;
		let checkAgain = false;

		const clear = () => {
			checkAgain = false;
			this.events.removeEventListener("replicator:mature", runCheck);
			this.events.removeEventListener("replication:change", runCheck);
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
		let subscriberSnapshotInFlight: Promise<void> | undefined;
		const requestIntervalMs = this.waitForReplicatorRequestIntervalMs;
		const maxRequestAttempts =
			this.waitForReplicatorRequestMaxAttempts ??
			Math.max(
				WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS,
				Math.ceil(timeoutMs / requestIntervalMs),
			);
		const requestSubscriberSnapshot = () => {
			if (subscriberSnapshotInFlight) {
				return;
			}
			subscriberSnapshotInFlight = Promise.resolve()
				.then(async () => {
					if (settled || this.closed) {
						return;
					}
					await this.node.services.pubsub.requestSubscribers(this.topic, key);
				})
				.catch((error) => {
					if (!isNotStartedError(error as Error)) {
						logger.error(error?.toString?.() ?? String(error));
					}
				})
				.finally(() => {
					subscriberSnapshotInFlight = undefined;
				});
		};

		const requestReplicationInfo = () => {
			if (settled || this.closed) {
				return;
			}

			if (requestAttempts >= maxRequestAttempts) {
				return;
			}

			requestAttempts++;

			const peerHash = key.hashcode();
			const peerSession = this._peerSessions.current(peerHash);
			if (peerSession?.phase === "open") {
				this._v2Receive.ensureRequestProgress({
					peerHash,
					peerSession,
					receiveEpoch: this._peerSessions.receiveEpoch(peerHash),
				});
			} else if (peerSession === null || peerSession.phase === "departing") {
				// A peer can be known to routing before its SharedLog topic
				// subscription has been observed. Legacy requests used to bootstrap
				// that case directly; V2 needs an authoritative Subscribe snapshot
				// before it can create a fenced PeerSession and request a Full.
				requestSubscriberSnapshot();
			}

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
		const runCheck = () => {
			if (settled) return;
			if (checkInFlight) {
				checkAgain = true;
				return;
			}
			// Reserve synchronously before `check()` can dispatch/re-enter from an
			// index implementation's first `next()` call.
			checkInFlight = true;
			void check()
				.catch((error) =>
					reject(error instanceof Error ? error : new Error(String(error))),
				)
				.finally(() => {
					checkInFlight = false;
					if (!settled && checkAgain) {
						checkAgain = false;
						runCheck();
					}
				});
		};

		// Register before the first asynchronous index read. EventTarget does not
		// replay a maturity/change event that fires while that read is in flight.
		this.events.addEventListener("replicator:mature", runCheck);
		this.events.addEventListener("replication:change", runCheck);
		requestReplicationInfo();
		runCheck();

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
		// Leader rechecks can outlive the receive call while replication roles
		// settle. Keep the metadata they need, not the decoded entry's serialized
		// backing buffer (which can be as large as the transferred block).
		const leaderSelectionEntry =
			entry instanceof Entry
				? deserialize(
						Uint8Array.from(serialize(entry.toShallow(true))),
						ShallowEntry,
					)
				: entry instanceof ShallowEntry
					? deserialize(Uint8Array.from(serialize(entry)), ShallowEntry)
					: entry;
		if (
			this.canPlanNativeHashGid(leaderSelectionEntry) &&
			(this._nativeBackbone ??
				this._nativeSharedLogState ??
				this._nativeRangePlanner)
		) {
			return this.waitForLeaderSelection(
				waitFor,
				options,
				async (checkOptions) => {
					const plan = await this.planEntryLeaders(
						leaderSelectionEntry,
						replicas,
						checkOptions,
					);
					return plan.leaders;
				},
			);
		}

		return this._waitForReplicators(
			await this.createCoordinates(leaderSelectionEntry, replicas),
			leaderSelectionEntry,
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
		if (
			!this._nativeBackbone &&
			!this._nativeSharedLogState &&
			!this._nativeRangePlanner
		) {
			return false;
		}
		return this.waitForLeaderSelection(
			waitFor,
			options,
			async (checkOptions) => {
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
			},
		);
	}

	private async waitForLeaderSelection(
		waitFor: WaitForReplicator[],
		options: WaitForReplicatorsOptions<R>,
		checkLeaders: (options: WaitForReplicatorsOptions<R>) => Promise<LeaderMap>,
	): Promise<LeaderMap | false> {
		const timeout = options.timeout ?? this.waitForReplicatorTimeout;
		const closeSignal = this._closeController.signal;
		const replicationLifecycleSignal =
			this._instanceLifecycle?.membershipLifecycleController?.signal;

		return new Promise((resolve, reject) => {
			let settled = false;
			const checks = new Set<Promise<void>>();
			const removeListeners = () => {
				this.events.removeEventListener("replication:change", roleListener);
				this.events.removeEventListener("replicator:mature", roleListener);
				closeSignal.removeEventListener("abort", abortListener);
				replicationLifecycleSignal?.removeEventListener("abort", abortListener);
			};
			const settleResolve = (value: LeaderMap | false) => {
				if (settled) return;
				settled = true;
				removeListeners();
				clearTimeout(timer);
				// Leader planning may persist coordinates. Keep the caller (and any
				// receive lease it owns) alive until checks admitted before this
				// timeout/abort have finished their local side effects.
				void Promise.allSettled([...checks]).then(() => resolve(value));
			};
			const settleReject = (error: unknown) => {
				if (settled) return;
				settled = true;
				removeListeners();
				clearTimeout(timer);
				void Promise.allSettled([...checks]).then(() => reject(error));
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
				if (settled) return;
				let running!: Promise<void>;
				running = check()
					.catch((error) => {
						settleReject(error);
					})
					.finally(() => checks.delete(running));
				checks.add(running);
			};

			const roleListener = () => {
				runCheck();
			};

			this.events.addEventListener("replication:change", roleListener);
			this.events.addEventListener("replicator:mature", roleListener);
			closeSignal.addEventListener("abort", abortListener);
			replicationLifecycleSignal?.addEventListener("abort", abortListener);
			// AbortSignal does not replay an abort event to listeners added after it
			// fired. Recheck after registration so work started concurrently with the
			// terminal fence cannot wait for the full leader-selection timeout.
			if (closeSignal.aborted || replicationLifecycleSignal?.aborted) {
				abortListener();
				return;
			}
			runCheck();
		});
	}

	// Public compatibility entry point; internal persistence calls use the coordinator directly.
	async createCoordinates(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R> | NumberFromType<R>,
		minReplicas: number,
	): Promise<NumberFromType<R>[]> {
		return this._coordinates.createCoordinates(entry, minReplicas) as Promise<
			NumberFromType<R>[]
		>;
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
				subscribers =
					(await this._getTopicSubscribers(this.rpc.topic))?.length ?? 1;
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
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<string, { intersecting: boolean }>> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		// we consume a list of coordinates in this method since if we are leader of one coordinate we want to persist all of them
		const set = await this._findLeaders(
			cursors,
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		await this.applyLeaderSelection(
			cursors,
			entry,
			set,
			options,
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return set;
	}

	private canPlanNativeEntryLeaderBatch(
		items: EntryLeaderBatchItem<R>[],
	): boolean {
		const nativePlanner = this._nativeBackbone ?? this._nativeRangePlanner;
		if (!nativePlanner || items.length === 0) {
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
		return (
			this.domain.type === "hash" && typeof this.getEntryGid(entry) === "string"
		);
	}

	private getEntryGid(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
	): string {
		if (entry instanceof Entry) {
			const rawGid = getPreparedRawExchangeGid(entry);
			if (rawGid) {
				return rawGid;
			}
		}
		return isEntryReplicated(entry) ? entry.gid : entry.meta.gid;
	}

	private getEntryNext(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
	): string[] {
		if (entry instanceof Entry) {
			const rawNext = getPreparedRawExchangeNext(entry);
			if (rawNext) {
				return rawNext;
			}
		}
		return entry.meta.next;
	}

	private getEntryHashNumber(
		entry: Entry<T> | ShallowOrFullEntry<any> | EntryReplicated<R>,
	): NumberFromType<R> {
		if ("hashNumber" in entry && entry.hashNumber != null) {
			return entry.hashNumber as NumberFromType<R>;
		}
		if (entry instanceof Entry) {
			const rawHashNumber = getPreparedRawExchangeHashNumber(entry);
			if (rawHashNumber != null) {
				if (typeof rawHashNumber === "bigint") {
					return (
						this.domain.resolution === "u32"
							? Number(rawHashNumber)
							: rawHashNumber
					) as NumberFromType<R>;
				}
				return (
					this.domain.resolution === "u32"
						? Number(rawHashNumber)
						: BigInt(rawHashNumber)
				) as NumberFromType<R>;
			}
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
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<boolean> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
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
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			!this.closed &&
				(await this._coordinates.persistCoordinate(
					{
						leaders,
						coordinates: cursors,
						replicas: cursors.length,
						entry,
						prev: options?.persist?.prev,
						assignedToRangeBoundary,
					},
					ownershipLifecycleController,
				));
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}

		return isLeader;
	}

	private async planEntryLeaders(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
		replicas: number,
		options?: LeaderSelectionOptions<R>,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<EntryLeaderPlan<R>> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		let coordinates: NumberFromType<R>[];
		let leaders: LeaderMap;
		let assignedToRangeBoundary: boolean | undefined;

		if (this.canPlanNativeHashGid(entry)) {
			const gid = this.getEntryGid(entry);
			const plan =
				(await this._findEntryAssignmentPlanFromHashGid(
					gid,
					replicas,
					options,
					ownershipLifecycleController,
				)) ??
				(await this._findLeaderPlanFromHashGid(
					gid,
					replicas,
					options,
					ownershipLifecycleController,
				));
			if (plan) {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
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
					ownershipLifecycleController,
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
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		leaders = await this._findLeaders(
			coordinates,
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const isLeader = await this.applyLeaderSelection(
			coordinates,
			entry,
			leaders,
			options,
			undefined,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return { coordinates, leaders, isLeader };
	}

	private async planEntryLeaderBatch(
		items: Iterable<EntryLeaderBatchItem<R>>,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<EntryLeaderPlan<R>[]> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const itemArray = [...items];
		const firstItem = itemArray[0];
		if (!firstItem) {
			return [];
		}

		if (this.canPlanNativeEntryLeaderBatch(itemArray)) {
			const context = await this.createLeaderSelectionContext(
				firstItem.options,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeReceivePlanner =
				this._nativeBackbone ?? this._nativeSharedLogState;
			const canUseNativeReceiveCoordinateBatch =
				!!nativeReceivePlanner &&
				itemArray.every((item) => {
					const persist = item.options?.persist;
					return !!persist && !persist.prev;
				});
			if (canUseNativeReceiveCoordinateBatch) {
				const nativePlans =
					nativeReceivePlanner.planReceiveCoordinatesForGidsBatch(
						{
							entries: itemArray.map((item) => ({
								entryHash: item.entry.hash,
								gid: this.getEntryGid(item.entry),
								hashNumber: this.getEntryHashNumber(item.entry),
								nextHashes: this.getEntryNext(item.entry),
								replicas: item.replicas,
							})),
							selfHash: context.selfHash,
						},
						this.createNativeLeaderOptions(context, firstItem.options),
					);
				const plans: EntryLeaderPlan<R>[] = [];
				const persistItems: Parameters<
					typeof this._coordinates.persistCoordinatesBatch
				>[0] = [];
				for (let i = 0; i < itemArray.length; i++) {
					const item = itemArray[i]!;
					const nativePlan = nativePlans[i]!;
					const coordinates = Array.from(
						nativePlan.coordinate.coordinates as Iterable<NumberFromType<R>>,
					);
					const leaders = nativePlan.leaders ?? new Map();
					const assignedToRangeBoundary =
						nativePlan.coordinate.assignedToRangeBoundary;
					plans.push({
						coordinates,
						leaders,
						isLeader: nativePlan.isLeader,
						assignedToRangeBoundary,
					});
					if (!this.closed) {
						const prepared =
							this._coordinates.createCoordinatePersistenceEntryFromNativePlan({
								entry: item.entry,
								plan: nativePlan.coordinate,
							});
						persistItems.push({
							coordinates,
							entry: item.entry,
							leaders,
							replicas: coordinates.length,
							assignedToRangeBoundary,
							prepared: prepared || undefined,
							commitNative:
								nativeReceivePlanner === this._nativeSharedLogState
									? false
									: undefined,
							commitNativeBackbone:
								nativeReceivePlanner === this._nativeBackbone
									? false
									: undefined,
						});
					}
				}
				if (!this.closed && persistItems.length > 0) {
					await this._coordinates.persistCoordinatesBatch(
						persistItems,
						ownershipLifecycleController,
					);
				}
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				return plans;
			}

			const nativePlanner = this._nativeBackbone ?? this._nativeRangePlanner;
			const nativePlans = nativePlanner!.planLeadersForGidsBatch(
				itemArray.map((item) => ({
					gid: this.getEntryGid(item.entry),
					replicas: item.replicas,
				})),
				this.createNativeLeaderOptions(context, firstItem.options),
			);
			const selfHash = this.node.identity.publicKey.hashcode();
			const plans: EntryLeaderPlan<R>[] = [];
			const persistItems: Parameters<
				typeof this._coordinates.persistCoordinatesBatch
			>[0] = [];
			for (let i = 0; i < itemArray.length; i++) {
				const item = itemArray[i]!;
				const nativePlan = nativePlans[i]!;
				const coordinates = Array.from(
					nativePlan.coordinates as Iterable<NumberFromType<R>>,
				);
				const leaders = nativePlan.leaders;
				const assignedToRangeBoundary =
					"assignedToRangeBoundary" in nativePlan
						? (nativePlan.assignedToRangeBoundary as boolean)
						: undefined;
				const isLeader = leaders.has(selfHash);
				const coordinateStrings =
					"coordinateStrings" in nativePlan
						? (nativePlan.coordinateStrings as string[])
						: undefined;
				const plan: EntryLeaderPlan<R> = {
					coordinates,
					leaders,
					isLeader,
					assignedToRangeBoundary,
				};
				if (coordinateStrings) {
					plan.coordinateStrings = coordinateStrings;
				}
				plans.push(plan);
				if (!this.closed && item.options?.persist) {
					persistItems.push({
						coordinates,
						entry: item.entry,
						leaders,
						replicas: coordinates.length,
						prev: item.options.persist.prev,
						assignedToRangeBoundary,
					});
				}
			}
			if (!this.closed && persistItems.length > 0) {
				await this._coordinates.persistCoordinatesBatch(
					persistItems,
					ownershipLifecycleController,
				);
			}
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return plans;
		}

		const plans: EntryLeaderPlan<R>[] = [];
		for (const item of itemArray) {
			plans.push(
				await this.planEntryLeaders(
					item.entry,
					item.replicas,
					item.options,
					ownershipLifecycleController,
				),
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		return plans;
	}

	private async planNativeBackboneReceiveGroupLeaders(
		groups: Iterable<{ gid: string; maxMaxReplicas: number }>,
		options?: { roleAge?: number; candidates?: Iterable<string> },
	): Promise<EntryLeaderPlan<R>[] | undefined> {
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const backbone = this._nativeBackbone;
		if (!backbone) {
			return undefined;
		}
		const groupArray = [...groups];
		if (groupArray.length === 0) {
			return [];
		}
		try {
			const context = await this.createLeaderSelectionContext(
				options,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativePlans = backbone.planLeadersForGidsBatch(
				groupArray.map((group) => ({
					gid: group.gid,
					replicas: group.maxMaxReplicas,
				})),
				this.createNativeLeaderOptions(context, options),
			);
			if (nativePlans.length !== groupArray.length) {
				return undefined;
			}
			return nativePlans.map((nativePlan) => {
				const leaders = nativePlan.leaders;
				return {
					coordinates: Array.from(
						nativePlan.coordinates as Iterable<NumberFromType<R>>,
					),
					coordinateStrings: nativePlan.coordinateStrings,
					leaders,
					isLeader: leaders.has(context.selfHash),
					assignedToRangeBoundary:
						"assignedToRangeBoundary" in nativePlan
							? (nativePlan.assignedToRangeBoundary as boolean)
							: undefined,
				};
			});
		} catch {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return undefined;
		}
	}

	private async planNativePreparedRawReceiveSelection(properties: {
		heads: RawEntryWithRefs[];
		hashes: string[];
		from: PublicSignKey;
	}): Promise<NativeBackboneRawReceiveSelectionPlan | undefined> {
		const backbone = this._nativeBackbone;
		if (
			!backbone ||
			this._isReplicating ||
			this.keep ||
			this.closed ||
			!this.syncronizer.onReceivedEntryHashes ||
			properties.heads.length === 0 ||
			properties.heads.some((head) => head.gidRefrences.length > 0)
		) {
			return undefined;
		}

		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const fromHash = properties.from.hashcode();
		try {
			const replicaOptions = {
				minReplicas: this.replicas.min?.getValue(this) || 1,
				maxReplicas: this.replicas.max?.getValue(this),
			};
			const leaderSelectionContext = await this.createLeaderSelectionContext(
				undefined,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (backbone.planPreparedRawReceiveSelection) {
				const nativeSelection = backbone.planPreparedRawReceiveSelection(
					properties.hashes,
					replicaOptions,
					this.createNativeLeaderOptions(leaderSelectionContext),
					fromHash,
				);
				return nativeSelection;
			}
			const nativeFastDropPlan = backbone.planPreparedRawReceiveFastDrop?.(
				properties.hashes,
				replicaOptions,
				this.createNativeLeaderOptions(leaderSelectionContext),
				fromHash,
			);
			if (
				nativeFastDropPlan &&
				nativeFastDropPlan.plannedHashCount === properties.hashes.length &&
				nativeFastDropPlan.groupCount > 0 &&
				nativeFastDropPlan.canDrop
			) {
				return {
					retainedHashes: [],
					droppedHashes: properties.hashes,
					groupCount: nativeFastDropPlan.groupCount,
					plannedHashCount: nativeFastDropPlan.plannedHashCount,
					usedNativeFastDropPlan: true,
					usedLeaderSamplePlans: true,
				};
			}
			const nativeSelection = backbone.selectPreparedRawReceiveHashes?.(
				properties.hashes,
				replicaOptions,
				this.createNativeLeaderOptions(leaderSelectionContext),
				fromHash,
			);
			if (nativeSelection) {
				return nativeSelection;
			}

			const nativeGroups = backbone.planPreparedRawReceiveGroups(
				properties.hashes,
				replicaOptions,
			);
			if (!nativeGroups || nativeGroups.length === 0) {
				return undefined;
			}
			let plannedHashCount = 0;
			for (const group of nativeGroups) {
				if (group.hashes.length !== group.requestedReplicas.length) {
					return undefined;
				}
				plannedHashCount += group.hashes.length;
			}
			if (plannedHashCount !== properties.hashes.length) {
				return undefined;
			}
			const leaderInputs = nativeGroups.map((group) => ({
				gid: group.gid,
				replicas: group.maxMaxReplicas,
			}));
			let usedLeaderSamplePlans = false;
			let leaderSamples = backbone.planLeaderSamplesForGidsBatch?.(
				leaderInputs,
				this.createNativeLeaderOptions(leaderSelectionContext),
			);
			let leaderPlans: EntryLeaderPlan<R>[] | undefined;
			if (leaderSamples?.length === nativeGroups.length) {
				usedLeaderSamplePlans = true;
			} else {
				leaderSamples = undefined;
				leaderPlans = backbone
					.planLeadersForGidsBatch(
						leaderInputs,
						this.createNativeLeaderOptions(leaderSelectionContext),
					)
					.map((nativePlan) => ({
						coordinates: Array.from(
							nativePlan.coordinates as Iterable<NumberFromType<R>>,
						),
						coordinateStrings: nativePlan.coordinateStrings,
						leaders: nativePlan.leaders,
						isLeader: nativePlan.leaders.has(leaderSelectionContext.selfHash),
						assignedToRangeBoundary:
							"assignedToRangeBoundary" in nativePlan
								? (nativePlan.assignedToRangeBoundary as boolean)
								: undefined,
					}));
				if (leaderPlans.length !== nativeGroups.length) {
					return undefined;
				}
			}

			const retainedHashes: string[] = [];
			const droppedHashes: string[] = [];
			if (leaderSamples) {
				for (let i = 0; i < nativeGroups.length; i++) {
					const group = nativeGroups[i]!;
					const leaders = leaderSamples[i]!;
					const shouldRetain = leaders.has(leaderSelectionContext!.selfHash);
					(shouldRetain ? retainedHashes : droppedHashes).push(...group.hashes);
				}
			} else {
				for (let i = 0; i < nativeGroups.length; i++) {
					const group = nativeGroups[i]!;
					const leaderPlan = leaderPlans?.[i];
					if (!leaderPlan) {
						return undefined;
					}
					const shouldRetain = leaderPlan.isLeader;
					(shouldRetain ? retainedHashes : droppedHashes).push(...group.hashes);
				}
			}
			if (droppedHashes.length === 0) {
				return undefined;
			}
			return {
				retainedHashes,
				droppedHashes,
				groupCount: nativeGroups.length,
				plannedHashCount,
				usedNativeFastDropPlan: false,
				usedLeaderSamplePlans,
			};
		} catch {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return undefined;
		}
	}

	private async tryFastDropPreparedRawReceive(properties: {
		heads: RawEntryWithRefs[];
		hashes: string[];
		from: PublicSignKey;
		fromIsSelf: boolean;
		syncProfile?: SyncProfileFn;
		selection?: NativeBackboneRawReceiveSelectionPlan;
		receiveOwnershipRevision: number;
	}): Promise<boolean> {
		const backbone = this._nativeBackbone;
		const ownershipRevision = properties.receiveOwnershipRevision;
		if (!backbone || !this.syncronizer.onReceivedEntryHashes) {
			return false;
		}
		const receivePlanStartedAt = syncProfileStart(properties.syncProfile);
		const selection =
			properties.selection ??
			(await this.planNativePreparedRawReceiveSelection(properties));
		if (
			!selection ||
			selection.retainedHashes.length > 0 ||
			!this.isReceiveOwnershipSnapshotStable(ownershipRevision)
		) {
			return false;
		}

		if (properties.syncProfile) {
			emitSyncProfileDuration(properties.syncProfile, receivePlanStartedAt, {
				name: "sharedLog.receive.plan",
				component: "shared-log",
				entries: properties.hashes.length,
				count: selection.groupCount,
				messages: 1,
				details: {
					replicating: false,
					predecodedReplicaHits: selection.plannedHashCount,
					nativeRawGroups: true,
					nativeReceiveGroupLeaderPlans: true,
					nativeReceiveGroupLeaderSamples: selection.usedLeaderSamplePlans,
					nativePreparedFastDropPlan: selection.usedNativeFastDropPlan,
					nativeFastDropEarly: true,
				},
			});
		}

		if (!properties.fromIsSelf) {
			this.markEntriesKnownByPeer(
				selection.droppedHashes,
				properties.from.hashcode(),
			);
		}

		const notifyStartedAt = syncProfileStart(properties.syncProfile);
		await this.syncronizer.onReceivedEntryHashes({
			hashes: selection.droppedHashes,
			from: properties.from,
		});
		if (!this.isReceiveOwnershipSnapshotStable(ownershipRevision)) {
			return false;
		}
		if (properties.syncProfile) {
			emitSyncProfileDuration(properties.syncProfile, notifyStartedAt, {
				name: "sharedLog.receive.notifySynchronizer",
				component: "shared-log",
				entries: selection.droppedHashes.length,
				messages: 1,
				details: {
					hashOnly: true,
					nativeFastDropEarly: true,
				},
			});
		}

		const joinPlanStartedAt = syncProfileStart(properties.syncProfile);
		if (properties.syncProfile) {
			emitSyncProfileDuration(properties.syncProfile, joinPlanStartedAt, {
				name: "sharedLog.receive.joinPlan",
				component: "shared-log",
				entries: properties.hashes.length,
				count: 0,
				messages: 1,
				details: {
					nativeFastDrop: true,
					nativeFastDropEarly: true,
				},
			});
		}
		backbone.clearPreparedRawReceiveEntries?.(selection.droppedHashes);
		return true;
	}

	private async selectNativePreparedRawReceiveHashes(properties: {
		heads: RawEntryWithRefs[];
		hashes: string[];
		from: PublicSignKey;
		fromIsSelf: boolean;
		syncProfile?: SyncProfileFn;
		selection?: NativeBackboneRawReceiveSelectionPlan;
		receiveOwnershipRevision: number;
	}): Promise<RawReceiveHashSelection | undefined> {
		const ownershipRevision = properties.receiveOwnershipRevision;
		if (!this.syncronizer.onReceivedEntryHashes) {
			return undefined;
		}
		const receivePlanStartedAt = syncProfileStart(properties.syncProfile);
		const selection =
			properties.selection ??
			(await this.planNativePreparedRawReceiveSelection(properties));
		if (
			!selection ||
			!this.isReceiveOwnershipSnapshotStable(ownershipRevision)
		) {
			return undefined;
		}
		if (selection.droppedHashes.length === 0) {
			return undefined;
		}

		if (!properties.fromIsSelf) {
			this.markEntriesKnownByPeer(
				selection.droppedHashes,
				properties.from.hashcode(),
			);
		}
		const notifyStartedAt = syncProfileStart(properties.syncProfile);
		await this.syncronizer.onReceivedEntryHashes({
			hashes: selection.droppedHashes,
			from: properties.from,
		});
		if (!this.isReceiveOwnershipSnapshotStable(ownershipRevision)) {
			return undefined;
		}
		emitSyncProfileDuration(properties.syncProfile, notifyStartedAt, {
			name: "sharedLog.receive.notifySynchronizer",
			component: "shared-log",
			entries: selection.droppedHashes.length,
			messages: 1,
			details: {
				hashOnly: true,
				nativeSelectDropped: true,
			},
		});
		emitSyncProfileDuration(properties.syncProfile, receivePlanStartedAt, {
			name: "sharedLog.rawReceive.nativeSelect",
			component: "shared-log",
			entries: properties.hashes.length,
			count: selection.retainedHashes.length,
			messages: 1,
			details: {
				dropped: selection.droppedHashes.length,
				groups: selection.groupCount,
				predecodedReplicaHits: selection.plannedHashCount,
			},
		});
		return selection.retainedIndexes
			? {
					hashes: selection.retainedHashes,
					indexes: selection.retainedIndexes,
					droppedIndexes: selection.droppedIndexes,
				}
			: selection.retainedHashes;
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

	private async createLeaderSelectionContext(
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
			freshLeaderPlan?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<LeaderSelectionContext> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const cached = this.getCachedLeaderSelectionContext(options);
		if (cached) {
			return cached;
		}
		const selfHash = this.node.identity.publicKey.hashcode();
		const roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge());
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);

		// Prefer `uniqueReplicators` (replicator cache) as soon as it has any data.
		// If it is still warming up (for example, only contains self), supplement with
		// current subscribers until we have enough candidates for this decision.
		let peerFilter: Set<string> | undefined = undefined;
		let selfReplicating = false;
		if (options?.candidates) {
			peerFilter = new Set(options.candidates);
		} else {
			selfReplicating =
				this.knownSelfReplicating(selfHash) ?? (await this.isReplicating());
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
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
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
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
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
		}

		const context = {
			roleAge,
			selfHash,
			selfReplicating,
			peerFilter,
			peerFilterArray: peerFilter ? [...peerFilter] : undefined,
		};
		// Every lookup above can yield while a mutation discovers an incoherent
		// ownership mirror. Do not cache or publish that stale context.
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		this.setCachedLeaderSelectionContext(options, context);
		return context;
	}

	private createNativeLeaderOptions(
		context: {
			roleAge: number;
			selfHash: string;
			selfReplicating: boolean;
			peerFilter: Set<string> | undefined;
			peerFilterArray?: string[] | undefined;
		},
		options?: {
			candidates?: Iterable<string>;
		},
	) {
		return {
			roleAge: context.roleAge,
			now: Date.now(),
			peerFilter: context.peerFilterArray ?? context.peerFilter,
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
			freshLeaderPlan?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<string, { intersecting: boolean }>> {
		const cacheBucket = this.leaderPlanCacheBucket(options);
		if (cacheBucket == null) {
			return this._findLeadersUncached(
				cursors,
				options,
				ownershipLifecycleController,
			);
		}
		const cacheKey = `c:${cursors.join(",")}|${cacheBucket}`;
		const cached = this._leaderPlanCache.get(cacheKey);
		if (cached) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return cached;
		}
		const capturedVersion = this._leaderPlanCache.capture();
		const leaders = await this._findLeadersUncached(
			cursors,
			options,
			ownershipLifecycleController,
		);
		if (
			(this._instanceLifecycle?._receiveOwnershipMutationAdmissions ?? 0) === 0
		) {
			this._leaderPlanCache.put(cacheKey, leaders, capturedVersion);
		}
		return leaders;
	}

	private async _findLeadersUncached(
		cursors: NumberFromType<R>[],
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
			freshLeaderPlan?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<string, { intersecting: boolean }>> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const context = await this.createLeaderSelectionContext(
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		let peerFilter = context.peerFilter;

		const nativePlanner = this._nativeBackbone ?? this._nativeRangePlanner;
		if (nativePlanner) {
			const leaders = nativePlanner.findLeaders(cursors, cursors.length, {
				...this.createNativeLeaderOptions(context, options),
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return leaders;
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
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}

		if (!options?.candidates) {
			const fullReplicaLeaders = await this.findFullReplicaLeaders(
				cursors.length,
				context.roleAge,
				peerFilter,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (fullReplicaLeaders) {
				return fullReplicaLeaders;
			}
		}

		const leaders = await getSamples<R>(
			cursors,
			this.replicationIndex,
			context.roleAge,
			this.indexableDomain.numbers,
			{
				peerFilter,
				uniqueReplicators: peerFilter,
			},
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return leaders;
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
		// Strict-only peers are not global fallbacks, but may still own an entry's
		// coordinates. Remember them so a partial fallback cannot bypass sampling.
		const excludedStrictPeers = new Set<string>();
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
					if (range.mode === ReplicationIntent.Strict && !includeStrict) {
						excludedStrictPeers.add(range.hash);
						continue;
					}
					if (!isMatured(range, now, roleAge)) {
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

		for (const hash of excludedStrictPeers) {
			if (!leaders.has(hash)) {
				return undefined;
			}
		}

		return leaders.size > 0 ? leaders : undefined;
	}

	private async findEntryReplicatedLeaderBatch(
		entries: EntryReplicated<R>[],
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<LeaderMap[]> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (entries.length === 0) {
			return [];
		}

		const nativePlanner = this._nativeBackbone ?? this._nativeRangePlanner;
		if (nativePlanner && !this.hasCustomFindLeaders()) {
			const context = await this.createLeaderSelectionContext(
				options,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const leaders = nativePlanner.findLeadersBatch(
				entries.map((entry) => ({
					cursors: entry.coordinates,
					replicas: entry.coordinates.length,
				})),
				this.createNativeLeaderOptions(context, options),
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return leaders;
		}

		const leaders: LeaderMap[] = [];
		for (const entry of entries) {
			leaders.push(
				await this.findLeaders(
					entry.coordinates,
					entry,
					options,
					ownershipLifecycleController,
				),
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		return leaders;
	}

	private hasCustomFindLeaders(): boolean {
		return this.findLeaders !== SharedLog.prototype.findLeaders;
	}

	private async planResidentRepairDispatchBatch(
		properties: {
			pendingModes: Set<RepairDispatchMode>;
			pendingPeersByMode: Map<RepairDispatchMode, Set<string>>;
			optimisticGidPeersByMode: Map<
				RepairDispatchMode,
				Map<string, Set<string>>
			>;
			fullReplicaRepairCandidates: Set<string>;
			fullReplicaRepairCandidateCount: number;
			selfHash: string;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<RepairDispatchMode, Map<string, string[]>>> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativeRepairPlanner =
			this._nativeBackbone ?? this._nativeSharedLogState;
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

		const context = await this.createLeaderSelectionContext(
			{ roleAge: 0 },
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const nativePlan =
			nativeRepairPlanner!.planRepairDispatchForResidentEntries(
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

	private async planRepairDispatchBatch(
		properties: {
			entries: EntryReplicated<R>[];
			requestedReplicasBatch: number[];
			pendingModes: Set<RepairDispatchMode>;
			pendingPeersByMode: Map<RepairDispatchMode, Set<string>>;
			optimisticGidPeersByMode: Map<
				RepairDispatchMode,
				Map<string, Set<string>>
			>;
			fullReplicaRepairCandidates: Set<string>;
			fullReplicaRepairCandidateCount: number;
			selfHash: string;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<RepairDispatchMode, Map<string, string[]>>> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
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

		const nativeRepairPlanner =
			this._nativeBackbone ?? this._nativeSharedLogState;
		if (nativeRepairPlanner && !this.hasCustomFindLeaders()) {
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

			const context = await this.createLeaderSelectionContext(
				{ roleAge: 0 },
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativePlan = nativeRepairPlanner.planRepairDispatchForEntries(
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
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
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
				const broadRepairCandidatePlanning =
					this.usesBroadRepairCandidatePlanning(mode);
				for (const peer of modePeers) {
					if (
						!broadRepairCandidatePlanning &&
						this.isEntryKnownByPeer(entry.hash, peer)
					) {
						continue;
					}
					const wasOptimisticallyAssigned = optimisticPeers?.has(peer) === true;
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
			freshLeaderPlan?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<string, { intersecting: boolean }> | undefined> {
		const cacheBucket = this.leaderPlanCacheBucket(options);
		if (cacheBucket == null) {
			return this._findLeadersFromHashGidUncached(
				gid,
				replicas,
				options,
				ownershipLifecycleController,
			);
		}
		const cacheKey = `g:${gid}:${replicas}|${cacheBucket}`;
		const cached = this._leaderPlanCache.get(cacheKey);
		if (cached) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			return cached;
		}
		const capturedVersion = this._leaderPlanCache.capture();
		const leaders = await this._findLeadersFromHashGidUncached(
			gid,
			replicas,
			options,
			ownershipLifecycleController,
		);
		if (
			leaders &&
			(this._instanceLifecycle?._receiveOwnershipMutationAdmissions ?? 0) === 0
		) {
			this._leaderPlanCache.put(cacheKey, leaders, capturedVersion);
		}
		return leaders;
	}

	private async _findLeadersFromHashGidUncached(
		gid: string,
		replicas: number,
		options?: {
			roleAge?: number;
			candidates?: Iterable<string>;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<string, { intersecting: boolean }> | undefined> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (
			!this._nativeBackbone &&
			!this._nativeSharedLogState &&
			!this._nativeRangePlanner
		) {
			return undefined;
		}

		const context = await this.createLeaderSelectionContext(
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (this._nativeBackbone) {
			return this._nativeBackbone.planLeadersForGid(
				gid,
				replicas,
				this.createNativeLeaderOptions(context, options),
			).leaders;
		}
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
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<
		| {
				coordinates: Array<number | bigint>;
				leaders: Map<string, { intersecting: boolean }>;
		  }
		| undefined
	> {
		const planner =
			this._nativeBackbone ??
			this._nativeSharedLogState ??
			this._nativeRangePlanner;
		if (!planner) {
			return undefined;
		}
		const context = await this.createLeaderSelectionContext(
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
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
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<
		| {
				coordinates: Array<number | bigint>;
				leaders: Map<string, { intersecting: boolean }>;
				assignedToRangeBoundary: boolean;
		  }
		| undefined
	> {
		const planner = this._nativeBackbone ?? this._nativeSharedLogState;
		if (!planner) {
			return undefined;
		}
		const context = await this.createLeaderSelectionContext(
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		return planner.planEntryAssignmentForGid(
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
			freshLeaderPlan?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<Map<string, { intersecting: boolean }>> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (this.canPlanNativeHashGid(entry)) {
			const nativeResult = await this._findLeadersFromHashGid(
				entry.meta.gid,
				replicas,
				options,
				ownershipLifecycleController,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (nativeResult) {
				return nativeResult;
			}
		}

		const coordinates = await this.createCoordinates(entry, replicas);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const result = await this._findLeaders(
			coordinates,
			options,
			ownershipLifecycleController,
		);
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
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

	private throwIfCheckedPruneRemoveBlocksLocalOperation(
		operation: "replication range mutation" | "close" | "drop",
	): void {
		if (this._checkedPruneRemovalCallbackInvocationDepth > 0) {
			throw new TerminalOperationNotStartedError(
				`${operation} cannot start during a checked-prune removal callback`,
			);
		}
	}

	private invokeProgramOnChange(change: Change<T>) {
		const onChange = this._logProperties?.onChange;
		if (!onChange) {
			return;
		}
		if (
			change.removed.length === 0 ||
			!this._checkedPrune.isBlockingLocalRangeMutation()
		) {
			return onChange(change);
		}

		this._checkedPruneRemovalCallbackInvocationDepth++;
		let result: ReturnType<NonNullable<typeof onChange>>;
		try {
			result = onChange(change);
		} catch (error) {
			this._checkedPruneRemovalCallbackInvocationDepth--;
			throw error;
		}
		if (isPromiseLike(result)) {
			return Promise.resolve(result).finally(() => {
				this._checkedPruneRemovalCallbackInvocationDepth--;
			});
		}
		this._checkedPruneRemovalCallbackInvocationDepth--;
		return result;
	}

	private withReplicationRangeMutationQueue<T>(
		fn: () => Promise<T>,
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
		options?: { affectsReceiveOwnership?: boolean },
	): Promise<T> {
		try {
			this.throwIfReplicationOwnershipLifecycleInactive(
				replicationOwnershipLifecycleController,
			);
		} catch (error) {
			return Promise.reject(error);
		}
		if (this._replicationRangeMutationsClosing) {
			return Promise.reject(
				new TerminalOperationNotStartedError(
					"Replication range mutations are closing",
				),
			);
		}
		let releaseReceiveOwnershipMutationAdmission: (() => void) | undefined;
		if (options?.affectsReceiveOwnership) {
			if (this._instanceLifecycle) {
				this._instanceLifecycle._receiveOwnershipMutationAdmissions++;
			}
			this.invalidateLeaderSelectionContextCache();
			releaseReceiveOwnershipMutationAdmission = () => {
				if (this._instanceLifecycle) {
					this._instanceLifecycle._receiveOwnershipMutationAdmissions--;
				}
				this.invalidateLeaderSelectionContextCache();
			};
		}
		const run = (this._replicationRangeMutationTail ?? Promise.resolve())
			.catch(() => {
				// A failed predecessor must not leave the queue permanently rejected.
			})
			.then(() => {
				// A predecessor can poison ownership, or close/reopen can replace the
				// ownership generation, after this mutation was admitted. Recheck the
				// exact captured generation before a queued follower touches state.
				this.throwIfReplicationOwnershipLifecycleInactive(
					replicationOwnershipLifecycleController,
				);
				return fn();
			});
		const fencedRun = releaseReceiveOwnershipMutationAdmission
			? run.finally(releaseReceiveOwnershipMutationAdmission)
			: run;
		this._replicationRangeMutationTail = fencedRun.then(
			() => undefined,
			() => undefined,
		);
		return fencedRun;
	}

	private withReceiveOwnershipMutationQueue<T>(
		fn: () => Promise<T>,
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<T> {
		return this.withReplicationRangeMutationQueue(
			fn,
			replicationOwnershipLifecycleController,
			{ affectsReceiveOwnership: true },
		);
	}

	private acquireReplicationRangeMutationTerminalFence(): {
		drained: Promise<void>;
	} {
		this._replicationRangeMutationsClosing = true;
		return { drained: this.drainReplicationRangeMutationQueue() };
	}

	private async drainReplicationRangeMutationQueue(): Promise<void> {
		for (;;) {
			const tail = this._replicationRangeMutationTail;
			if (!tail) {
				return;
			}
			await tail.catch(() => {});
			await Promise.resolve();
			if (this._replicationRangeMutationTail === tail) {
				return;
			}
		}
	}

	private async drainReplicationInfoApplyQueues(): Promise<void> {
		for (;;) {
			const tails = [
				...(this._replicationInfoApplyQueueByPeer?.values() ?? []),
			];
			if (tails.length === 0) {
				return;
			}
			await Promise.allSettled(tails);
			// Queue cleanup runs in `finally`; give it a microtask before checking for
			// tails admitted while the previous snapshot was settling.
			await Promise.resolve();
		}
	}

	private trackAdmittedPruneRemove<T>(
		remove: () => Promise<T>,
		ownershipLifecycleController: AbortController,
	): Promise<T> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (this._pruneRemovesClosing) {
			return Promise.reject(
				new TerminalOperationNotStartedError("Prune removals are closing"),
			);
		}

		let operation: Promise<T>;
		try {
			// Invoke synchronously after admission so terminal close cannot establish
			// its fence between the lifecycle check and the lower-log mutation.
			operation = Promise.resolve(remove());
		} catch (error) {
			return Promise.reject(error);
		}
		this._admittedPruneRemoves.add(operation);
		void operation.then(
			() => {
				this._admittedPruneRemoves.delete(operation);
			},
			() => {
				this._admittedPruneRemoves.delete(operation);
			},
		);
		return operation;
	}

	private acquirePruneRemoveTerminalFence(): { drained: Promise<void> } {
		this._pruneRemovesClosing = true;
		return { drained: this.drainAdmittedPruneRemoves() };
	}

	private async drainAdmittedPruneRemoves(): Promise<void> {
		for (;;) {
			const admitted = [...(this._admittedPruneRemoves ?? [])];
			if (admitted.length === 0) {
				return;
			}
			await Promise.allSettled(admitted);
			await Promise.resolve();
		}
	}

	private schedulePendingMaturity(
		change: ReplicationChange<ReplicationRangeIndexable<R>>,
		from: PublicSignKey,
		options: { rebalance: boolean; waitMs: number },
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
			return;
		}
		let pendingRanges = this.pendingMaturity.get(change.range.hash);
		if (!pendingRanges) {
			pendingRanges = new Map();
			this.pendingMaturity.set(change.range.hash, pendingRanges);
		}
		const previous = pendingRanges.get(change.range.idString);
		if (previous) {
			clearTimeout(previous.timeout);
		}

		const pendingMaturity: PendingMaturityRecord<R> = {
			range: change,
			timeout: undefined as unknown as ReturnType<typeof setTimeout>,
			expiresAt: Date.now() + options.waitMs,
			from,
			rebalance: options.rebalance,
			ownershipLifecycleController,
		};
		const rangeHash = change.range.hash;
		const rangeIdString = change.range.idString;
		pendingMaturity.timeout = setTimeout(() => {
			if (
				!this.isRepairLifecycleActive(
					pendingMaturity.ownershipLifecycleController,
				)
			) {
				if (pendingRanges.get(rangeIdString) === pendingMaturity) {
					pendingRanges.delete(rangeIdString);
					if (
						pendingRanges.size === 0 &&
						this.pendingMaturity.get(rangeHash) === pendingRanges
					) {
						this.pendingMaturity.delete(rangeHash);
					}
				}
				return;
			}
			// Clearing or replacing the exact range invalidates this object. A timer
			// already queued by the event loop must become a no-op.
			if (pendingRanges.get(rangeIdString) !== pendingMaturity) {
				return;
			}
			pendingRanges.delete(rangeIdString);
			if (
				pendingRanges.size === 0 &&
				this.pendingMaturity.get(rangeHash) === pendingRanges
			) {
				this.pendingMaturity.delete(rangeHash);
			}

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChangeEvent>("replicator:mature", {
					detail: { publicKey: pendingMaturity.from },
				}),
			);

			if (
				this.isRepairLifecycleActive(
					pendingMaturity.ownershipLifecycleController,
				) &&
				pendingMaturity.rebalance &&
				change.range.mode !== ReplicationIntent.Strict &&
				change.type === "added"
			) {
				this.replicationChangeDebounceFn.add({
					...change,
					matured: true,
				});
			}
		}, options.waitMs);
		pendingRanges.set(rangeIdString, pendingMaturity);
	}

	/**
	 * Delete exact durable ranges, then reconcile every native/runtime mirror to
	 * the observed durable post-state. Backends may reject after committing all
	 * or part of a delete, so a blind compensating put could resurrect data.
	 *
	 * This method must run inside the global replication-range mutation lane.
	 */
	private async deleteReplicationRangesCoherently(
		ranges: ReplicationRangeIndexable<R>[],
		ownerHash: string,
		options?: { preserveOwnerMembership?: boolean },
	): Promise<ReplicationRangeDeletionOutcome<R>> {
		const wasReplicator = this.uniqueReplicators.has(ownerHash);
		const joinWasEmitted = this._replicatorJoinEmitted.has(ownerHash);
		if (ranges.length === 0) {
			const ownerHasRanges =
				(await this.replicationIndex.count({ query: { hash: ownerHash } })) > 0;
			if (!ownerHasRanges) {
				this.uniqueReplicators.delete(ownerHash);
				this._replicatorJoinEmitted.delete(ownerHash);
			}
			return {
				removed: [],
				retained: [],
				ownerHasRanges,
				rollback: async () => {
					if (wasReplicator) {
						this.uniqueReplicators.add(ownerHash);
					} else {
						this.uniqueReplicators.delete(ownerHash);
					}
					if (joinWasEmitted) {
						this._replicatorJoinEmitted.add(ownerHash);
					} else {
						this._replicatorJoinEmitted.delete(ownerHash);
					}
				},
			};
		}

		const uniqueRanges = [
			...new Map(ranges.map((range) => [range.idString, range])).values(),
		];
		type Snapshot = {
			range: ReplicationRangeIndexable<R>;
			pending?: PendingMaturityRecord<R>;
		};
		const snapshots: Snapshot[] = uniqueRanges.map((range) => {
			const pending = this.pendingMaturity.get(ownerHash)?.get(range.idString);
			return {
				range,
				pending:
					pending?.range.range.rangeHash === range.rangeHash
						? pending
						: undefined,
			};
		});

		// Suspend exact maturity timers before the durable operation becomes
		// ambiguous; confirmed survivors are restored below with their remaining age.
		for (const snapshot of snapshots) {
			if (snapshot.pending) {
				clearTimeout(snapshot.pending.timeout);
				const peerPending = this.pendingMaturity.get(ownerHash);
				if (peerPending?.get(snapshot.range.idString) === snapshot.pending) {
					peerPending.delete(snapshot.range.idString);
					if (peerPending.size === 0) {
						this.pendingMaturity.delete(ownerHash);
					}
				}
			}
		}

		let primaryError: unknown;
		const deletionErrors: unknown[] = [];
		const reconciliationErrors: unknown[] = [];
		// No backend transaction spans bounded calls. Attempt every batch and probe
		// the exact post-state even after an ambiguous failure.
		for (
			let i = 0;
			i < uniqueRanges.length;
			i += REPLICATION_RANGE_ID_QUERY_BATCH_SIZE
		) {
			try {
				await this.replicationIndex.del({
					query: new And([
						new StringMatch({ key: "hash", value: ownerHash }),
						new Or(
							uniqueRanges
								.slice(i, i + REPLICATION_RANGE_ID_QUERY_BATCH_SIZE)
								.map(
									(range) =>
										new ByteMatchQuery({
											key: "id",
											value: range.id,
										}),
								),
						),
					]),
				});
			} catch (error) {
				deletionErrors.push(error);
			}
		}
		if (deletionErrors.length === 1) {
			primaryError = deletionErrors[0];
		} else if (deletionErrors.length > 1) {
			primaryError = new AggregateError(
				deletionErrors,
				"Multiple replication-range deletion batches failed",
			);
		}

		const probeCurrentRows = async () => {
			const currentById = new Map<string, ReplicationRangeIndexable<R>>();
			for (
				let i = 0;
				i < uniqueRanges.length;
				i += REPLICATION_RANGE_ID_QUERY_BATCH_SIZE
			) {
				const current = await this.replicationIndex
					.iterate(
						{
							query: new Or(
								uniqueRanges
									.slice(i, i + REPLICATION_RANGE_ID_QUERY_BATCH_SIZE)
									.map(
										(range) =>
											new ByteMatchQuery({
												key: "id",
												value: range.id,
											}),
									),
							),
						},
						{ reference: true },
					)
					.all();
				for (const result of current) {
					currentById.set(result.value.idString, result.value);
				}
			}
			return currentById;
		};

		let currentById: Map<string, ReplicationRangeIndexable<R>>;
		try {
			currentById = await probeCurrentRows();
		} catch (error) {
			primaryError ??= error;
			try {
				currentById = await probeCurrentRows();
			} catch (retryError) {
				const failure = new AggregateError(
					primaryError === retryError
						? [retryError]
						: [primaryError, retryError],
					"Could not determine durable replication-range state after deletion",
				);
				this.poisonReplicationOwnership(failure);
				throw failure;
			}
		}

		const removed: ReplicationRangeIndexable<R>[] = [];
		const retained: ReplicationRangeIndexable<R>[] = [];
		for (const snapshot of snapshots) {
			const current = currentById.get(snapshot.range.idString);
			if (
				current?.hash === snapshot.range.hash &&
				current.rangeHash === snapshot.range.rangeHash
			) {
				retained.push(snapshot.range);
			} else {
				removed.push(snapshot.range);
			}
		}
		if (primaryError === undefined && retained.length > 0) {
			primaryError = new Error(
				"Replication-range deletion resolved without removing every selected row",
			);
		}
		const retainedIds = new Set(retained.map((range) => range.idString));

		const reconcileNative = (
			snapshot: Snapshot,
			current: ReplicationRangeIndexable<R> | undefined,
		) => {
			if (current) {
				this.putNativeReplicationRange(current);
			} else {
				this.deleteNativeReplicationRange(snapshot.range);
			}
		};
		for (const snapshot of snapshots) {
			const current = currentById.get(snapshot.range.idString);
			try {
				reconcileNative(snapshot, current);
			} catch (error) {
				primaryError ??= error;
				try {
					reconcileNative(snapshot, current);
				} catch (retryError) {
					reconciliationErrors.push(retryError);
				}
			}

			if (
				snapshot.pending &&
				retainedIds.has(snapshot.range.idString) &&
				!this.pendingMaturity.get(ownerHash)?.has(snapshot.range.idString)
			) {
				this.schedulePendingMaturity(
					snapshot.pending.range,
					snapshot.pending.from,
					{
						rebalance: snapshot.pending.rebalance,
						waitMs: Math.max(0, snapshot.pending.expiresAt - Date.now()),
					},
					snapshot.pending.ownershipLifecycleController,
				);
			}
		}

		let ownerHasRanges = wasReplicator;
		let ownerStateKnown = false;
		try {
			ownerHasRanges =
				(await this.replicationIndex.count({ query: { hash: ownerHash } })) > 0;
			ownerStateKnown = true;
		} catch (error) {
			primaryError ??= error;
			reconciliationErrors.push(error);
		}
		try {
			await this.updateOldestTimestampFromIndex();
		} catch (error) {
			primaryError ??= error;
			reconciliationErrors.push(error);
		}
		if (ownerStateKnown) {
			try {
				// Preserve membership only for a successful destructive reset that is
				// immediately installing replacement rows.
				const canPreserveOwnerMembership =
					options?.preserveOwnerMembership === true &&
					primaryError === undefined;
				if (ownerHasRanges || canPreserveOwnerMembership) {
					if (ownerHasRanges || wasReplicator) {
						this.uniqueReplicators.add(ownerHash);
					}
					if (joinWasEmitted) {
						this._replicatorJoinEmitted.add(ownerHash);
					}
				} else {
					this.uniqueReplicators.delete(ownerHash);
					this._replicatorJoinEmitted.delete(ownerHash);
				}
			} catch (error) {
				primaryError ??= error;
				reconciliationErrors.push(error);
			}
		}

		let outcomeError = primaryError;
		if (reconciliationErrors.length > 0) {
			const errors = [
				...(primaryError === undefined ? [] : [primaryError]),
				...reconciliationErrors.filter((error) => error !== primaryError),
			];
			outcomeError = new AggregateError(
				errors,
				"Replication-range deletion and post-state reconciliation failed",
			);
			this.poisonReplicationOwnership(outcomeError);
		}
		let rollbackStarted = false;
		const rollback = async () => {
			if (rollbackStarted) {
				return;
			}
			rollbackStarted = true;
			const rollbackErrors: unknown[] = [];
			const removedIds = new Set(removed.map((range) => range.idString));
			for (const snapshot of snapshots) {
				if (!removedIds.has(snapshot.range.idString)) {
					continue;
				}
				try {
					await this.replicationIndex.put(snapshot.range);
					this.putNativeReplicationRange(snapshot.range);
				} catch (error) {
					rollbackErrors.push(error);
				}
			}
			for (const snapshot of snapshots) {
				if (
					!removedIds.has(snapshot.range.idString) ||
					!snapshot.pending ||
					this.pendingMaturity.get(ownerHash)?.has(snapshot.range.idString)
				) {
					continue;
				}
				try {
					this.schedulePendingMaturity(
						snapshot.pending.range,
						snapshot.pending.from,
						{
							rebalance: snapshot.pending.rebalance,
							waitMs: Math.max(0, snapshot.pending.expiresAt - Date.now()),
						},
						snapshot.pending.ownershipLifecycleController,
					);
				} catch (error) {
					rollbackErrors.push(error);
				}
			}
			try {
				if (wasReplicator) {
					this.uniqueReplicators.add(ownerHash);
				} else {
					this.uniqueReplicators.delete(ownerHash);
				}
				if (joinWasEmitted) {
					this._replicatorJoinEmitted.add(ownerHash);
				} else {
					this._replicatorJoinEmitted.delete(ownerHash);
				}
			} catch (error) {
				rollbackErrors.push(error);
			}
			try {
				await this.updateOldestTimestampFromIndex();
			} catch (error) {
				rollbackErrors.push(error);
			}
			if (rollbackErrors.length > 0) {
				const failure = new AggregateError(
					rollbackErrors,
					"Replication-range deletion rollback failed",
				);
				this.poisonReplicationOwnership(failure);
				throw failure;
			}
		};
		return {
			removed,
			retained,
			ownerHasRanges,
			rollback,
			error: outcomeError,
		};
	}

	private cancelReplicationInfoRequests(peerHash: string) {
		const state = this._replicationInfoRequestByPeer.get(peerHash);
		if (!state) return;
		if (state.timer) {
			clearTimeout(state.timer);
		}
		this._replicationInfoRequestByPeer.delete(peerHash);
	}

	/**
	 * Applied V2 progress from a peer (a committed Full/Added/Stopped, or a
	 * rotated capability generation) proves the peer answers. Reset the
	 * recovery scheduler's unpark escalation so a later stall restarts from
	 * the base interval. Peer-session rotation resets implicitly: the recovery
	 * scheduler creates a fresh per-session state.
	 */
	private resetReplicationInfoV2RecoveryEscalation(peerHash: string) {
		const state = this._replicationInfoRequestByPeer.get(peerHash);
		if (!state) {
			return;
		}
		state.attempts = 0;
		state.parkedSinceMs = undefined;
	}

	private scheduleReplicationInfoV2Recovery(
		peer: PublicSignKey,
		replicationLifecycleController = this._instanceLifecycle
			?.membershipLifecycleController,
	) {
		if (
			!replicationLifecycleController ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}
		const peerHash = peer.hashcode();
		const peerSession = this._peerSessions.current(peerHash);
		if (!peerSession || peerSession.phase !== "open") {
			return;
		}
		const requestStates = this._replicationInfoRequestByPeer;
		const existing = requestStates.get(peerHash);
		if (existing?.peerSession === peerSession) {
			return;
		}
		if (existing) {
			if (existing.timer) {
				clearTimeout(existing.timer);
			}
			requestStates.delete(peerHash);
		}
		const state: {
			attempts: number;
			timer?: ReturnType<typeof setTimeout>;
			peerSession: PeerSession;
			parkedSinceMs?: number;
		} = {
			attempts: 0,
			peerSession,
		};
		requestStates.set(peerHash, state);
		const cancel = () => {
			if (requestStates.get(peerHash) !== state) {
				return;
			}
			if (state.timer) {
				clearTimeout(state.timer);
			}
			requestStates.delete(peerHash);
		};
		const intervalMs = Math.max(50, this.waitForReplicatorRequestIntervalMs);
		const maxUnparkDelayMs = Math.max(
			intervalMs,
			REPLICATION_INFO_V2_RECOVERY_MAX_UNPARK_DELAY,
		);
		const unparkDelayMs = () =>
			Math.min(
				maxUnparkDelayMs,
				intervalMs *
					2 **
						Math.min(
							state.attempts,
							REPLICATION_INFO_V2_RECOVERY_MAX_UNPARK_EXPONENT,
						),
			);
		const arm = (delayMs: number) => {
			state.timer = setTimeout(tick, delayMs);
			state.timer.unref?.();
		};
		const tick = () => {
			if (
				!this.isReplicationLifecycleActive(replicationLifecycleController) ||
				peerSession.phase !== "open" ||
				!this._peerSessions.isCurrent(peerHash, peerSession)
			) {
				cancel();
				return;
			}
			const receiveEpoch = this._peerSessions.receiveEpoch(peerHash);
			if (
				!this._v2Receive.isRequestParked({
					peerHash,
					peerSession,
					receiveEpoch,
				})
			) {
				// Active and timed/in-flight cycles are no-ops. An unparked,
				// non-active generation with no worker can occur when its timer fires
				// behind a temporary receive gate; repair that exact session here.
				this._v2Receive.ensureRequestProgress({
					peerHash,
					peerSession,
					receiveEpoch,
				});
				state.parkedSinceMs = undefined;
				arm(intervalMs);
				return;
			}
			const now = Date.now();
			if (state.parkedSinceMs === undefined) {
				state.parkedSinceMs = now;
			}
			const resumeAtMs = state.parkedSinceMs + unparkDelayMs();
			if (now < resumeAtMs) {
				arm(Math.max(50, resumeAtMs - now));
				return;
			}
			if (
				this._v2Receive.resumeParkedRequest({
					peerHash,
					peerSession,
					receiveEpoch,
				})
			) {
				// Fruitless until proven otherwise: escalate the next unpark wait.
				// Applied progress resets via resetReplicationInfoV2RecoveryEscalation.
				state.attempts++;
				state.parkedSinceMs = undefined;
			}
			arm(intervalMs);
		};
		tick();
	}

	/**
	 * Collapsed B12 shell: the legacy request-polling body (bounded
	 * RequestReplicationInfoMessage ticks) is deleted; V2 recovery is the
	 * only scheduler. Retained as a named seam rather than inlined at the
	 * callers because the liveness monitor wiring and several suites
	 * stub/spy it by name.
	 */
	private scheduleReplicationInfoRequests(
		peer: PublicSignKey,
		replicationLifecycleController = this._instanceLifecycle
			?.membershipLifecycleController,
	) {
		if (
			!replicationLifecycleController ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}
		this.scheduleReplicationInfoV2Recovery(
			peer,
			replicationLifecycleController,
		);
	}

	async handleSubscriptionChange(
		publicKey: PublicSignKey,
		topics: string[],
		subscribed: boolean,
		subscriptionEpoch?: PeerSession,
		subscriptionTransportSession?: bigint,
	) {
		if (!topics.includes(this.topic)) {
			return;
		}
		const replicationLifecycleController =
			this._instanceLifecycle?.membershipLifecycleController;
		if (
			!replicationLifecycleController ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}

		const peerHash = publicKey.hashcode();
		const expectedSubscriptionEpoch =
			subscriptionEpoch ??
			this._peerSessions.rotate(peerHash, subscribed ? "opening" : "departing");
		const ownsSubscriptionEpoch = () =>
			this._peerSessions.isCurrent(peerHash, expectedSubscriptionEpoch);
		if (!ownsSubscriptionEpoch()) {
			return;
		}
		this.dispatchPersistedReceiptReadinessChange(peerHash);
		// A reconnect can arrive before the previous exact-session recovery tick
		// observes its stale session. Retire that job synchronously so it cannot
		// suppress the replacement session's scheduler in the shared peer slot.
		this.cancelReplicationInfoRequests(peerHash);
		// A destination stream is scoped to exactly one topic-subscription
		// session. Abort the predecessor synchronously before either barrier can
		// yield; a late queue completion must never enter the new session.
		this._v2Receive.clearPeer(peerHash);
		this._v2Send.clearPeer(peerHash);
		const capabilityTransportSession =
			this._peerSyncCapabilitySessions.get(peerHash);
		const canInheritCapability =
			subscribed &&
			(subscriptionTransportSession !== undefined
				? capabilityTransportSession === subscriptionTransportSession
				: !expectedSubscriptionEpoch.hasPredecessor);
		if (!canInheritCapability) {
			// Departures and successor openings revoke the signed transport binding.
			// A live successor may inherit a capability that arrived just before its
			// Subscribe only when both signed frames belong to the same transport
			// generation. Snapshot fallbacks lack that proof, so only their first
			// opening may inherit pre-opening capability state.
			this._peerSyncCapabilitySessions.delete(peerHash);
			this._peerSyncCapabilityTimestamps.delete(peerHash);
		}
		if (subscribed) {
			const pendingOpeningCapabilities =
				this._openingSyncCapabilitiesByPeer.get(peerHash);
			if (
				pendingOpeningCapabilities &&
				pendingOpeningCapabilities.epoch !== expectedSubscriptionEpoch
			) {
				this._openingSyncCapabilitiesByPeer.delete(peerHash);
			}
			// Open the barrier WINDOW on the session (legacy: opening-epoch map
			// set). Same synchronous window as the ownsSubscriptionEpoch() check
			// above, so the flagged session is the current one — the invariant
			// every reader's captured-session check relies on.
			expectedSubscriptionEpoch.beginOpeningBarrier();
			// Fence new messages immediately, drain handlers admitted by the previous
			// subscription, then wait behind every queued replication mutation. A
			// reconnect must not inherit metadata or ranges from the old connection.
			try {
				this._peerSessions.blockReplicationInfo(peerHash);
				await this.drainPeerReceiveHandlers(peerHash);
				await this.withReplicationInfoApplyQueue(peerHash, async () => {});
				if (
					!this.isReplicationLifecycleActive(replicationLifecycleController) ||
					!ownsSubscriptionEpoch()
				) {
					return;
				}
				this._pendingReplicatorLeaveByPeer.delete(peerHash);
				const openingCapabilities =
					this._openingSyncCapabilitiesByPeer.get(peerHash);
				if (openingCapabilities?.epoch === expectedSubscriptionEpoch) {
					this._peerSyncCapabilities.set(
						peerHash,
						openingCapabilities.capabilities,
					);
					if (openingCapabilities.transportSession === undefined) {
						this._peerSyncCapabilitySessions.delete(peerHash);
					} else {
						this._peerSyncCapabilitySessions.set(
							peerHash,
							openingCapabilities.transportSession,
						);
					}
					if (openingCapabilities.timestamp === undefined) {
						this._peerSyncCapabilityTimestamps.delete(peerHash);
					} else {
						this._peerSyncCapabilityTimestamps.set(
							peerHash,
							openingCapabilities.timestamp,
						);
					}
					this._openingSyncCapabilitiesByPeer.delete(peerHash);
				}
				this._peerSessions.unblockReplicationInfo(peerHash);
			} finally {
				if (
					this._openingSyncCapabilitiesByPeer.get(peerHash)?.epoch ===
					expectedSubscriptionEpoch
				) {
					this._openingSyncCapabilitiesByPeer.delete(peerHash);
				}
				// Close the barrier window on every settle path, INCLUDING a
				// barrier throw (legacy: identity-guarded map delete). The legacy
				// guard only prevented deleting a newer session's entry; the
				// per-session flag cannot collide, so the clear is unconditional.
				expectedSubscriptionEpoch.finishOpeningBarrier();
			}
		}
		if (!subscribed) {
			// Legacy code also deleted the opening-epoch map entry here. The
			// per-session equivalent is a no-op: any still-open barrier window
			// belongs to an opening session this departing rotation already
			// superseded — readers key off the CURRENT session, so that flag is
			// unreachable, and its own barrier `finally` still clears it.
			this._openingSyncCapabilitiesByPeer.delete(peerHash);
			this._peerSessions.blockReplicationInfo(peerHash);
			const disconnectedWarmupSession =
				this.joinWarmup._warmupSessionsByTarget.get(peerHash) ?? null;
			this.joinWarmup.cancelJoinWarmupTarget(peerHash);

			let removed = false;
			try {
				// Unsubscribe can race with the peer's final replication reset message.
				// Proactively evict its ranges so leader selection doesn't keep stale owners.
				removed = await this.removeReplicator(publicKey, {
					cleanupIfSubscriptionSuperseded: true,
					expectedWarmupSession: disconnectedWarmupSession,
					noEvent: true,
					onRemoved: ({ wasReplicator }) => {
						if (wasReplicator) {
							this._pendingReplicatorLeaveByPeer.add(peerHash);
						}
					},
					replicationLifecycleController,
					subscriptionEpoch: expectedSubscriptionEpoch,
				});
			} catch (error) {
				if (!isNotStartedError(error as Error)) {
					throw error;
				}
			}
			if (
				!this.isReplicationLifecycleActive(replicationLifecycleController) ||
				!ownsSubscriptionEpoch() ||
				!removed
			) {
				return;
			}

			if (this._pendingReplicatorLeaveByPeer.delete(peerHash)) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
						detail: { publicKey },
					}),
				);
			}
			return;
		}

		this._peerSessions.unblockReplicationInfo(peerHash);
		this._liveness._replicatorLivenessFailures.delete(peerHash);
		this._liveness.markReplicatorActivity(peerHash);
		this._peerSessions.markOpen(peerHash, expectedSubscriptionEpoch);
		this.promoteReplicationInfoV2ReceiveCapability(
			publicKey,
			expectedSubscriptionEpoch,
		);

		// Decode, sender and authenticated apply readiness are separate bits. An
		// ACKed capability advert is the local half of receiver-led negotiation;
		// readiness is promoted through the coordinator once the ACK arrives.
		// This keeps capability ACK latency off the subscription callback's
		// critical path.
		const receiveEpoch = this._peerSessions.receiveEpoch(peerHash);
		// B12: there is no legacy startup work to order ahead of RequestV2, so
		// the two-phase legacy barrier is gone — an ACKed advert promotes
		// readiness through the coordinator as soon as it lands.
		this._v2Receive.advertiseLocalCapability({
			target: publicKey,
			peerSession: expectedSubscriptionEpoch,
			receiveEpoch,
			signal: replicationLifecycleController.signal,
		});
		this.scheduleReplicationInfoV2Recovery(
			publicKey,
			replicationLifecycleController,
		);
		this.dispatchPersistedReceiptReadinessChange(peerHash);
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

	private async sendCheckedPruneResponse(
		to: string,
		requests: Array<{ hash: string; requestId: Uint8Array }>,
		signal?: AbortSignal,
	) {
		if (requests.length === 0) {
			return;
		}
		await this.rpc.send(new ResponseIPruneV2({ requests }), {
			mode: new AcknowledgeDelivery({
				to: [to],
				redundancy: 1,
			}),
			priority: CONVERGENCE_MESSAGE_PRIORITY,
			signal,
		});
	}

	private async admitAndSendCheckedPruneGrants(
		to: string,
		requests: Array<{ hash: string; requestId: Uint8Array }>,
	) {
		const latestByHash = new Map(
			requests.map((request) => [request.hash, request]),
		);
		if (latestByHash.size === 0) {
			return { admitted: [], missing: [] };
		}
		const checkedPruneCoordinator = this._checkedPrune;
		const lifecycle = this._instanceLifecycle!;
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const admission = await this.withReplicationRangeMutationQueue(async () => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			// KEEP-OLD (stage 3): a coordinator mismatch here returns empty, it
			// does not throw — the shape stays a bare identity compare.
			if (this._checkedPrune !== checkedPruneCoordinator) {
				return {
					admitted: [] as typeof requests,
					missing: [] as typeof requests,
				};
			}
			const hashes = [...latestByHash.keys()];
			let presentHashes: string[] | undefined;
			let localLeaderHashes: Set<string> | undefined;
			if (this._nativeBackbone && !this.hasCustomFindLeaders()) {
				const context = await this.createLeaderSelectionContext(
					undefined,
					ownershipLifecycleController,
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				const allConfirmed = this._nativeBackbone.planRequestPruneAllConfirmed(
					hashes,
					to,
					{
						...this.createNativeLeaderOptions(context),
						omitPeerHistoryGids:
							this._gidPeersHistory.size === 0 &&
							this._nativeSharedLogState == null,
					},
				);
				if (allConfirmed?.peerHistoryGids.length) {
					this.removePeerFromGidPeerHistoryBatch(
						to,
						allConfirmed.peerHistoryGids,
						{ skipNativeBackbone: true },
					);
				}
				if (allConfirmed?.allConfirmed) {
					presentHashes = hashes;
					localLeaderHashes = new Set(hashes);
				}
			}
			if (!presentHashes || !localLeaderHashes) {
				const present =
					(await this.log.blocks.hasMany?.(hashes)) ??
					(await Promise.all(hashes.map((hash) => this.log.blocks.has(hash))));
				presentHashes = hashes.filter(
					(_hash, index) => present[index] === true,
				);
				localLeaderHashes = await this.revalidateCheckedPruneGrantLocalLeaders(
					presentHashes,
					to,
					ownershipLifecycleController,
				);
			}
			const presentHashSet = new Set(presentHashes);
			const missing = hashes
				.filter((hash) => !presentHashSet.has(hash))
				.map((hash) => latestByHash.get(hash)!);
			const admitted = presentHashes
				.filter((hash) => localLeaderHashes.has(hash))
				.map((hash) => latestByHash.get(hash)!);
			if (admitted.length === 0) {
				return { admitted, missing };
			}

			const barrier = pDefer<void>();
			const observedBarrier = checkedPruneCoordinator.trackGrantSend(
				admitted.map(({ hash }) => hash),
				barrier.promise,
			);
			const restartCandidates: Array<{
				candidate: CheckedPruneRestartCandidate<T, R>;
				reservation: object;
				retryIdentity?: CheckedPruneRetryIdentity<T, R>;
			}> = [];
			for (const { hash } of admitted) {
				const candidate = checkedPruneCoordinator.getRestartCandidate(hash);
				const pending = checkedPruneCoordinator.getPendingDelete(hash);
				const retryIdentity = checkedPruneCoordinator.getRetryIdentity(hash);
				const retry = retryIdentity?.state;
				const hadQueuedCandidate = this.pruneDebouncedFn.has(hash);
				const hadInFlightCandidate = checkedPruneCoordinator.hasCandidate(hash);
				const shouldRestart =
					candidate != null &&
					(hadQueuedCandidate ||
						hadInFlightCandidate ||
						retry != null ||
						pending?.retryOnInvalidation === true);
				this.deleteQueuedCheckedPrune(hash, checkedPruneCoordinator);
				if (pending) {
					try {
						void Promise.resolve(
							pending.reject(
								new Error(
									"Failed to delete, granted checked prune to another peer",
								),
								{ preserveRetry: shouldRestart },
							),
						).catch(() => {});
					} catch {
						// Cancellation is enforced below. A synchronous observer error
						// cannot revoke the admitted grant.
					}
					if (checkedPruneCoordinator.isCurrentRequest(hash, pending)) {
						checkedPruneCoordinator.markCancelled(hash, pending, {
							preserveRetry: shouldRestart,
						});
					}
				} else if (
					candidate ||
					hadQueuedCandidate ||
					hadInFlightCandidate ||
					retry
				) {
					checkedPruneCoordinator.markCancelled(hash, {
						preserveRetry: shouldRestart,
					});
				}
				if (shouldRestart) {
					restartCandidates.push({
						candidate,
						reservation: checkedPruneCoordinator.reserveRestart(hash),
						retryIdentity,
					});
				}
			}
			return {
				admitted,
				missing,
				barrier,
				observedBarrier,
				restartCandidates,
			};
		}, ownershipLifecycleController);

		if (!admission.barrier) {
			return {
				admitted: admission.admitted,
				missing: admission.missing,
			};
		}
		try {
			// KEEP-OLD (stage 3): this send guard is deliberately WEAKER than the
			// checked-prune triple — it omits the poison and `closed` terms so an
			// already-admitted grant response still goes out. Do not migrate it to
			// isCheckedPruneCurrent (that would strengthen the predicate).
			if (
				this._checkedPrune === checkedPruneCoordinator &&
				!ownershipLifecycleController.signal.aborted
			) {
				await this.sendCheckedPruneResponse(
					to,
					admission.admitted,
					ownershipLifecycleController.signal,
				);
			}
		} finally {
			admission.barrier.resolve();
			await admission.observedBarrier;
			for (const {
				candidate,
				reservation,
				retryIdentity,
			} of admission.restartCandidates) {
				if (
					// closeController omitted: this seat never compared it.
					!lifecycle.isCheckedPruneCurrent(
						checkedPruneCoordinator,
						undefined,
						ownershipLifecycleController,
					)
				) {
					checkedPruneCoordinator.cancelRestartReservation(
						candidate.hash,
						reservation,
					);
					if (retryIdentity) {
						checkedPruneCoordinator.clearRetry(candidate.hash, retryIdentity);
					}
					continue;
				}
				if (
					!checkedPruneCoordinator.consumeRestartReservation(
						candidate.hash,
						reservation,
					)
				) {
					continue;
				}
				try {
					this.scheduleCheckedPruneRetry(
						candidate,
						ownershipLifecycleController,
					);
				} catch {
					// Granting the requester is the safety boundary. Best-effort
					// liveness rearming must not turn a completed grant into failure.
					checkedPruneCoordinator.clearRetry(candidate.hash);
				}
			}
		}
		return {
			admitted: admission.admitted,
			missing: admission.missing,
		};
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
		ownershipLifecycleController?: AbortController,
	): Promise<any>[] {
		if (!options?.unchecked && this.closed) {
			return [];
		}
		ownershipLifecycleController ??=
			this.captureReplicationOwnershipLifecycle();
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (options?.unchecked) {
			return [...entries.values()].map((x) => {
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				this.deleteGidPeerHistory(x.entry.meta.gid);
				this._checkedPrune.removeRequestSent(x.entry.hash);
				this._checkedPrune.clearConfirmedReplicators(x.entry.hash);
				return this.trackAdmittedPruneRemove(
					() =>
						this.log.remove(x.entry, {
							recursively: true,
						}),
					ownershipLifecycleController,
				);
			});
		}

		const checkedPruneCoordinator = this._checkedPrune;
		const closeController = this._closeController;
		const lifecycle = this._instanceLifecycle!;
		// Stage 3 (seam 4): prune() is the one checked-prune seat that always
		// compared the co-captured _closeController identity — keep that term.
		const isCheckedPruneLifecycleCurrent = () =>
			lifecycle.isCheckedPruneCurrent(
				checkedPruneCoordinator,
				closeController,
				ownershipLifecycleController,
			);
		const throwIfCheckedPruneLifecycleInactive = () =>
			lifecycle.throwIfCheckedPruneInactive(
				checkedPruneCoordinator,
				closeController,
				ownershipLifecycleController,
			);

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

			const pendingPrev = checkedPruneCoordinator.getPendingDelete(entry.hash);
			if (pendingPrev) {
				const replaceRevokedBackgroundGeneration =
					!explicitTimeout &&
					checkedPruneCoordinator.hasRevokedLeader(
						entry.hash,
						pendingPrev,
						leaders,
					);
				if (replaceRevokedBackgroundGeneration) {
					try {
						void Promise.resolve(
							pendingPrev.reject(
								new Error(
									"Checked prune ownership now requires a peer revoked from the current generation",
								),
								{ preserveRetry: true },
							),
						).catch(() => {});
					} catch {
						// Exact cancellation below still fences the old request ID.
					}
					if (
						checkedPruneCoordinator.isCurrentRequest(entry.hash, pendingPrev)
					) {
						checkedPruneCoordinator.markCancelled(entry.hash, pendingPrev, {
							preserveRetry: true,
						});
					}
				} else {
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
			}

			const minReplicas = decodeReplicas(entry);
			const minReplicasValue =
				this.getClampedReplicas(minReplicas).getValue(this);
			const parentHashes = this.getEntryNext(entry);
			const requestId = randomBytes(32);
			const deferredPromise: DeferredPromise<void> = pDefer();
			let deleteScheduled = false;
			let pending!: CheckedPrunePendingDelete;

			const clear = () => {
				if (checkedPruneCoordinator.isCurrentRequest(entry.hash, pending)) {
					checkedPruneCoordinator.deletePendingDelete(entry.hash, pending);
				}
				clearTimeout(timeout);
			};

			const resolveDelete = () => {
				try {
					throwIfCheckedPruneLifecycleInactive();
				} catch (error) {
					reject(error);
					return;
				}
				clearTimeout(timeout);
				cleanupTimer.push(
					setTimeout(() => {
						const run = async () => {
							throwIfCheckedPruneLifecycleInactive();
							try {
								const removed = await this.withReplicationRangeMutationQueue(
									async () => {
										throwIfCheckedPruneLifecycleInactive();
										if (
											!checkedPruneCoordinator.isCurrentRequest(
												entry.hash,
												pending,
											)
										) {
											throw new TerminalOperationNotStartedError(
												"Checked prune session is no longer active at the delete boundary",
											);
										}
										// The correlated receipt is only evidence that a contacted peer
										// granted this handoff. The sole ownership decision is made here
										// in the global mutation lane, immediately before deletion.
										let finalOwnership: {
											leaders: CheckedPruneLeaderMap;
											localLeader: boolean;
										};
										finalOwnership = await this.revalidateCheckedPruneOwnership(
											{
												hash: entry.hash,
												entry,
												leaders: this.checkedPruneLeadersToMap(leaders),
												selfReplicating: true,
												requireFreshLeaderDecision: true,
												ownershipLifecycleController,
												checkedPruneCoordinator,
											},
										);
										throwIfCheckedPruneLifecycleInactive();
										if (
											!checkedPruneCoordinator.isCurrentRequest(
												entry.hash,
												pending,
											)
										) {
											throw new TerminalOperationNotStartedError(
												"Checked prune session changed before removal",
											);
										}
										if (finalOwnership.localLeader) {
											return false;
										}

										const exactConfirmations =
											checkedPruneCoordinator.getExactConfirmedReplicators(
												entry.hash,
												pending,
											);
										let finalConfirmationCount = 0;
										for (const peer of exactConfirmations) {
											if (
												finalOwnership.leaders.has(peer) &&
												!this._peerSessions.isReplicationInfoBlocked(peer) &&
												this._peerSessions.isReceiveCleanupGateOpen(peer)
											) {
												finalConfirmationCount += 1;
											}
										}
										if (finalConfirmationCount < minReplicasValue) {
											return false;
										}

										this.deleteGidPeerHistory(entry.meta.gid);
										throwIfCheckedPruneLifecycleInactive();
										if (
											!checkedPruneCoordinator.markRemoving(entry.hash, pending)
										) {
											throw new TerminalOperationNotStartedError(
												"Checked prune session changed before removal admission",
											);
										}
										checkedPruneCoordinator.removeRequestSent(entry.hash);
										checkedPruneCoordinator.clearConfirmedReplicators(
											entry.hash,
										);
										const releaseLocalRangeMutationBlock =
											checkedPruneCoordinator.blockLocalRangeMutation();
										try {
											await this.trackAdmittedPruneRemove(
												() =>
													this.log.remove(entry, {
														recursively: false,
													}),
												ownershipLifecycleController,
											);
										} finally {
											releaseLocalRangeMutationBlock();
										}
										if (
											!checkedPruneCoordinator.markDone(entry.hash, pending)
										) {
											throw new TerminalOperationNotStartedError(
												"Checked prune session changed while removal was admitted",
											);
										}
										clear();
										return true;
									},
									ownershipLifecycleController,
								);
								if (!removed) {
									clear();
									if (!explicitTimeout) {
										this.scheduleCheckedPruneRetry(
											{ entry, leaders },
											ownershipLifecycleController,
										);
									}
									deferredPromise.reject(
										new Error("Failed to delete, is leader again"),
									);
									return;
								}
							} catch (error) {
								const shouldRetry =
									!explicitTimeout &&
									isCheckedPruneLifecycleCurrent() &&
									!isNotStartedError(error as Error);
								checkedPruneCoordinator.markCancelled(entry.hash, pending, {
									preserveRetry: shouldRetry,
								});
								clear();
								if (shouldRetry) {
									this.scheduleCheckedPruneRetry(
										{ entry, leaders },
										ownershipLifecycleController,
									);
								}
								deferredPromise.reject(error);
								return;
							}

							try {
								await this.prunePromotedCheckedPruneParents(
									parentHashes,
									ownershipLifecycleController,
								);
							} catch (error) {
								if (
									isCheckedPruneLifecycleCurrent() &&
									!isNotStartedError(error as Error)
								) {
									logger.error(error);
								}
							}
							deferredPromise.resolve();
						};
						void run().catch((error) => {
							reject(error);
						});
					}, this.waitForPruneDelay),
				);
			};

			const reject = (e: any, rejectOptions?: { preserveRetry?: boolean }) => {
				const isCheckedPruneTimeout =
					e instanceof Error &&
					typeof e.message === "string" &&
					e.message.startsWith("Timeout for checked pruning");
				checkedPruneCoordinator.markCancelled(entry.hash, pending, {
					preserveRetry:
						rejectOptions?.preserveRetry ??
						(!explicitTimeout && isCheckedPruneTimeout),
				});
				clear();
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
					CHECKED_PRUNE_BACKGROUND_TIMEOUT_MIN_MS,
					Number(this._respondToIHaveTimeout ?? 0) +
						this.waitForReplicatorTimeout +
						PRUNE_DEBOUNCE_INTERVAL * 2,
				);

			const timeout = setTimeout(() => {
				// For internal/background prune flows (no explicit timeout), retry a few times
				// to avoid "permanently prunable" entries when `_pendingIHave` expires under
				// heavy load.
				const shouldRetry =
					!explicitTimeout && isCheckedPruneLifecycleCurrent();
				reject(
					new Error(
						`Timeout for checked pruning after ${checkedPruneTimeoutMs}ms (closed=${this.closed})`,
					),
				);
				// Requeue only after rejecting clears the current generation. Scheduling
				// while it is still pending is deliberately ignored by the coordinator.
				if (shouldRetry && isCheckedPruneLifecycleCurrent()) {
					this.scheduleCheckedPruneRetry(
						{ entry, leaders },
						ownershipLifecycleController,
					);
				}
			}, checkedPruneTimeoutMs);
			timeout.unref?.();

			pending = {
				requestId,
				retryOnInvalidation: !explicitTimeout,
				promise: deferredPromise,
				clear,
				reject,
				resolve: async (
					publicKeyHash: string,
					responseRequestId: Uint8Array,
				) => {
					try {
						throwIfCheckedPruneLifecycleInactive();
					} catch (error) {
						reject(error);
						return;
					}
					if (
						!checkedPruneCoordinator.isCurrentRequestId(
							entry.hash,
							pending,
							responseRequestId,
						) ||
						!checkedPruneCoordinator
							.getContactedReplicators(entry.hash)
							?.has(publicKeyHash)
					) {
						return;
					}
					const exactConfirmations =
						checkedPruneCoordinator.addConfirmedReplicator(
							entry.hash,
							publicKeyHash,
							pending,
							responseRequestId,
						);
					if (!exactConfirmations) {
						return;
					}
					// Seed provider hints so future remote reads can avoid extra round-trips.
					this.remoteBlocks.hintProviders(entry.hash, [publicKeyHash]);

					if (!deleteScheduled && minReplicasValue <= exactConfirmations.size) {
						deleteScheduled = true;
						resolveDelete();
					}
				},
			};
			checkedPruneCoordinator.setPendingDelete(
				entry.hash,
				pending,
				entry,
				leaders,
			);

			promises.push(deferredPromise.promise);
		}

		const emitMessages = async (entries: string[], to: string) => {
			throwIfCheckedPruneLifecycleInactive();
			const pendingEntries = [...new Set(entries)].flatMap((hash) => {
				const pending = checkedPruneCoordinator.getPendingDelete(hash);
				return pending ? [{ hash, pending }] : [];
			});
			while (true) {
				const grantSends = pendingEntries.flatMap(({ hash }) => {
					const send = checkedPruneCoordinator.waitForGrantSends(hash);
					return send ? [send] : [];
				});
				if (grantSends.length === 0) {
					break;
				}
				await Promise.all(grantSends);
				throwIfCheckedPruneLifecycleInactive();
			}
			const requests: Array<{ hash: string; requestId: Uint8Array }> = [];
			for (const { hash, pending } of pendingEntries) {
				if (checkedPruneCoordinator.addRequestSent(hash, to, pending)) {
					requests.push({ hash, requestId: pending.requestId });
				}
			}
			if (requests.length > 0) {
				const result = await this.rpc.send(
					new RequestIPruneV2({
						requests,
					}),
					{
						mode: new AcknowledgeDelivery({
							to: [to],
							redundancy: 1,
						}),
						priority: CONVERGENCE_MESSAGE_PRIORITY,
					},
				);
				throwIfCheckedPruneLifecycleInactive();
				return result;
			}
		};

		for (const [k, v] of peerToEntries) {
			emitMessages(v, k).catch(() => {});
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
				if (!isCheckedPruneLifecycleCurrent()) {
					clearInterval(timer);
					return;
				}

				const pendingByPeer: [string, string[]][] = [];
				for (const [peer, hashes] of peerToEntries) {
					const pending = hashes.filter((h) =>
						checkedPruneCoordinator.hasPendingDelete(h),
					);
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
					if (isCheckedPruneLifecycleCurrent()) {
						inFlight = false;
					}
				});
			}, resendIntervalMs);
			timer.unref?.();
			cleanupTimer.push(timer as any);
		}

		let cleanup = () => {
			for (const timer of cleanupTimer) {
				clearTimeout(timer);
			}
			closeController.signal.removeEventListener("abort", cleanup);
		};

		Promise.allSettled(promises).finally(cleanup);
		closeController.signal.addEventListener("abort", cleanup);
		return promises;
	}

	/**
	 * For debugging
	 */
	async getPrunable(roleAge?: number) {
		this.throwIfReplicationOwnershipPoisoned();
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
		this.throwIfReplicationOwnershipPoisoned();
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
			this._gidPeersHistory = new Map();
			this.resetGidPeerHistoryCleanupState();
			this._nativeSharedLogState?.clearGidPeers();
			this._nativeBackbone?.clearGidPeers();
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
		await waitFor(() => this._checkedPrune.pendingDeletes.size === 0, options);
	}

	async onReplicationChange(
		changeOrChanges:
			| ReplicationChanges<ReplicationRangeIndexable<R>>
			| ReplicationChanges<ReplicationRangeIndexable<R>>[],
	) {
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		const isOwnershipLifecycleCurrent = () =>
			this.isRepairLifecycleActive(ownershipLifecycleController);
		const throwIfOwnershipLifecycleInactive = () =>
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		/**
		 * TODO use information of new joined/leaving peer to create a subset of heads
		 * that we potentially need to share with other peers
		 */

		if (this.closed) {
			return;
		}
		const batchedChanges = Array.isArray(changeOrChanges[0])
			? (changeOrChanges as ReplicationChanges<ReplicationRangeIndexable<R>>[])
			: [changeOrChanges as ReplicationChanges<ReplicationRangeIndexable<R>>];
		const changes = batchedChanges.flat();
		const selfHash = this.node.identity.publicKey.hashcode();
		const warmupSessions = new Map<string, WarmupSession>();
		for (const change of changes) {
			if (change.type === "added" && change.range.hash !== selfHash) {
				warmupSessions.set(
					change.range.hash,
					this.joinWarmup.ensureWarmupSession(change.range.hash),
				);
			}
		}

		await this.log.trim();
		if (!isOwnershipLifecycleCurrent()) {
			return false;
		}

		// On removed ranges (peer leaves / shrink), gid-level history can hide
		// per-entry gaps. Force a fresh delivery pass for reassigned entries.
		const forceFreshDelivery = changes.some(
			(change) => change.type === "removed",
		);
		const gidPeersHistorySnapshot = new Map<string, Set<string> | undefined>();
		const dedupeCutoff = Date.now() - RECENT_REPAIR_DISPATCH_TTL_MS;
		for (const [target, hashes] of this._recentRepairDispatch) {
			if (!isOwnershipLifecycleCurrent()) {
				return false;
			}
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
			if (!isOwnershipLifecycleCurrent()) {
				return false;
			}
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
		const isCurrentJoinWarmupTarget = (target: string) =>
			isOwnershipLifecycleCurrent() &&
			warmupPeers.has(target) &&
			this.joinWarmup._warmupSessionsByTarget.get(target) ===
				warmupSessions.get(target);
		const areJoinWarmupGenerationsCurrent = () =>
			isOwnershipLifecycleCurrent() &&
			[...warmupPeers].every(isCurrentJoinWarmupTarget);

		try {
			const uncheckedDeliver: Map<
				string,
				Map<string, EntryReplicated<any>>
			> = new Map();
			const flushUncheckedDeliverTarget = (target: string) => {
				if (!isOwnershipLifecycleCurrent()) {
					return;
				}
				const entries = uncheckedDeliver.get(target);
				if (!entries || entries.size === 0) {
					return;
				}
				const isWarmupTarget = warmupPeers.has(target);
				if (isWarmupTarget && !isCurrentJoinWarmupTarget(target)) {
					uncheckedDeliver.delete(target);
					return;
				}
				const mode: RepairDispatchMode = forceFreshDelivery
					? "churn"
					: isWarmupTarget
						? "join-warmup"
						: "join-authoritative";
				this.dispatchMaybeMissingEntries(
					target,
					entries,
					{
						bypassRecentDedupe: isWarmupTarget || forceFreshDelivery,
						bypassKnownPeerHints:
							forceFreshDelivery ||
							(mode === "join-authoritative" && addedPeers.has(target)),
						mode,
						retryScheduleMs:
							mode === "join-warmup"
								? JOIN_WARMUP_RETRY_SCHEDULE_MS
								: mode === "join-authoritative"
									? [0]
									: undefined,
					},
					ownershipLifecycleController,
				);
				uncheckedDeliver.delete(target);
			};
			const queueUncheckedDeliver = (
				target: string,
				entry: EntryReplicated<any>,
			) => {
				if (!isOwnershipLifecycleCurrent()) {
					return;
				}
				if (warmupPeers.has(target) && !isCurrentJoinWarmupTarget(target)) {
					return;
				}
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
					if (
						!isOwnershipLifecycleCurrent() ||
						(useJoinWarmupFastPath && !areJoinWarmupGenerationsCurrent())
					) {
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
							if (isCurrentJoinWarmupTarget(target)) {
								candidatePeers.add(target);
							}
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
						if (!isOwnershipLifecycleCurrent()) {
							return false;
						}
						if (!areJoinWarmupGenerationsCurrent()) {
							continue;
						}

						if (oldPeersSet) {
							for (const oldPeer of oldPeersSet) {
								if (!currentPeers.has(oldPeer)) {
									this._checkedPrune.removeRequestSent(
										entryReplicated.hash,
										oldPeer,
									);
								}
							}
						}

						for (const [peer] of currentPeers) {
							if (isCurrentJoinWarmupTarget(peer)) {
								this.markRepairSweepOptimisticPeer(
									entryReplicated.gid,
									peer,
									warmupSessions.get(peer)!,
								);
							}
						}

						const authoritativePeers = [...currentPeers.keys()].filter(
							(peer) =>
								!isCurrentJoinWarmupTarget(peer) &&
								!this.hasPendingRepairSweepOptimisticPeer(
									entryReplicated.gid,
									peer,
								),
						);
						this.addPeersToGidPeerHistory(
							entryReplicated.gid,
							authoritativePeers,
							true,
						);

						if (!currentPeers.has(selfHash)) {
							throwIfOwnershipLifecycleInactive();
							await this.pruneDebouncedFnAddIfNotKeeping(
								{
									key: entryReplicated.hash,
									value: { entry: entryReplicated, leaders: currentPeers },
								},
								ownershipLifecycleController,
							);
							if (!isOwnershipLifecycleCurrent()) {
								return false;
							}
						} else {
							throwIfOwnershipLifecycleInactive();
							await this.cancelCheckedPruneForLocalLeader(entryReplicated.hash);
							if (!isOwnershipLifecycleCurrent()) {
								return false;
							}
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
					if (!isOwnershipLifecycleCurrent()) {
						return false;
					}

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
								this._checkedPrune.removeRequestSent(
									entryReplicated.hash,
									oldPeer,
								);
							}
						}
					}

					for (const [peer] of currentPeers) {
						if (isCurrentJoinWarmupTarget(peer)) {
							this.markRepairSweepOptimisticPeer(
								entryReplicated.gid,
								peer,
								warmupSessions.get(peer)!,
							);
						}
					}

					const authoritativePeers = [...currentPeers.keys()].filter(
						(peer) =>
							!addedPeers.has(peer) &&
							!this.hasPendingRepairSweepOptimisticPeer(
								entryReplicated.gid,
								peer,
							),
					);
					this.addPeersToGidPeerHistory(
						entryReplicated.gid,
						authoritativePeers,
						true,
					);

					if (!isLeader) {
						throwIfOwnershipLifecycleInactive();
						await this.pruneDebouncedFnAddIfNotKeeping(
							{
								key: entryReplicated.hash,
								value: { entry: entryReplicated, leaders: currentPeers },
							},
							ownershipLifecycleController,
						);
						if (!isOwnershipLifecycleCurrent()) {
							return false;
						}
					} else {
						throwIfOwnershipLifecycleInactive();
						await this.cancelCheckedPruneForLocalLeader(entryReplicated.hash);
						if (!isOwnershipLifecycleCurrent()) {
							return false;
						}
					}
				}
			}

			if (forceFreshDelivery) {
				throwIfOwnershipLifecycleInactive();
				// Pure leave/shrink churn can have zero `addedPeers`, but the peers that
				// received redistributed entries still need a follow-up repair pass if the
				// immediate maybe-sync misses one entry.
				this.scheduleRepairSweep(
					{
						mode: "churn",
						peers: churnRepairPeers,
					},
					ownershipLifecycleController,
				);
			} else if (useJoinWarmupFastPath) {
				throwIfOwnershipLifecycleInactive();
				// Pure join warmup uses the cheap immediate maybe-missing dispatch above,
				// then defers the authoritative sweep so it does not compete with the
				// write burst itself.
				const peers = new Set(addedPeers);
				const repairTimers = this._repairRetryTimers;
				const timer = setTimeout(() => {
					repairTimers.delete(timer);
					if (!isOwnershipLifecycleCurrent()) {
						return;
					}
					this.scheduleRepairSweep(
						{
							mode: "join-warmup",
							peers,
							warmupSessions,
						},
						ownershipLifecycleController,
					);
				}, 250);
				timer.unref?.();
				repairTimers.add(timer);
			} else if (authoritativeRepairPeers.size > 0) {
				throwIfOwnershipLifecycleInactive();
				this.scheduleRepairSweep(
					{
						mode: "join-authoritative",
						peers: authoritativeRepairPeers,
					},
					ownershipLifecycleController,
				);
			}

			if (!forceFreshDelivery && authoritativeRepairPeers.size > 0) {
				throwIfOwnershipLifecycleInactive();
				this.scheduleJoinAuthoritativeRepair(
					authoritativeRepairPeers,
					ownershipLifecycleController,
				);
			}

			for (const target of [...uncheckedDeliver.keys()]) {
				if (!isOwnershipLifecycleCurrent()) {
					return false;
				}
				flushUncheckedDeliverTarget(target);
			}

			const localSegmentsAfterChange =
				hasSelfRangeRemoval && !this._isAdaptiveReplicating
					? await this.getMyReplicationSegments()
					: undefined;
			if (!isOwnershipLifecycleCurrent()) {
				return false;
			}
			const hasFixedSelfRangeRemovalToZero =
				localSegmentsAfterChange != null &&
				localSegmentsAfterChange.length > 0 &&
				localSegmentsAfterChange.every(
					(segment) => segment.widthNormalized === 0,
				);
			const shouldRunLocalPruneScan =
				hasFixedSelfRangeRemovalToZero ||
				(this._isAdaptiveReplicating &&
					changes.some(
						(change) =>
							change.type === "added" ||
							change.type === "removed" ||
							change.type === "replaced",
					));

			if (shouldRunLocalPruneScan) {
				throwIfOwnershipLifecycleInactive();
				// Adaptive range changes and fixed zero-width updates can make already-indexed
				// local heads prunable even when the incremental rebalance scan misses them
				// under churn or timing pressure. Re-scan after repair dispatches are flushed
				// using the mature-role view, which matches the bounded pruning contract.
				await this.pruneIndexedEntriesNoLongerLed({
					useDefaultRoleAge: true,
				});
				if (!isOwnershipLifecycleCurrent()) {
					return false;
				}
				await this.pruneCurrentHeadsNoLongerLed({
					useDefaultRoleAge: true,
				});
				if (!isOwnershipLifecycleCurrent()) {
					return false;
				}
			}

			return changed;
		} catch (error: any) {
			if (!isOwnershipLifecycleCurrent()) {
				return false;
			}
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
		const subscriptionEpoch = this._peerSessions.rotate(fromHash, "departing");
		this._peerSessions.blockReplicationInfo(fromHash);
		this._recentRepairDispatch.delete(fromHash);
		this.invalidateSharedLogTopicSubscribersCache();

		return this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.topics,
			false,
			subscriptionEpoch,
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

		const fromHash = evt.detail.from.hashcode();
		const subscriptionEpoch = this._peerSessions.rotate(fromHash, "opening");
		this._peerSessions.blockReplicationInfo(fromHash);
		this.invalidateSharedLogTopicSubscribersCache();
		// Invalidate the local reachability snapshot before waking block reads;
		// their forced resolver runs synchronously from the reachable event.
		this.remoteBlocks.onReachable(evt.detail.from);

		await this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.topics,
			true,
			subscriptionEpoch,
			evt.detail.session,
		);
	}

	async rebalanceParticipation(
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
		rebalanceParticipationDebounced = this.rebalanceParticipationDebounced,
	) {
		// Stage 3: the lifecycle owns all three identity terms. `lifecycle` may
		// go stale later; its deps late-bind to the host, so the debouncer term
		// still reads the current host field, and the role term can disagree
		// with the current lifecycle's counter only after a rotation, where the
		// isActiveFor term is already false (the stale lifecycle's ownership
		// controller is aborted in the same synchronous block).
		const lifecycle = this._instanceLifecycle;
		const capturedRoleGeneration = lifecycle?.roleGeneration ?? 0;
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu
		const isCurrent = () =>
			lifecycle != null &&
			lifecycle.isActiveFor(ownershipLifecycleController) &&
			lifecycle.isRebalanceDebouncerCurrent(rebalanceParticipationDebounced) &&
			lifecycle.isRoleCurrent(capturedRoleGeneration);

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
			if (!isCurrent()) {
				return false;
			}

			// The role is fixed (no changes depending on memory usage or peer count etc)
			if (!this._isReplicating) {
				return false;
			}

			if (this._isAdaptiveReplicating) {
				if (this.shouldDelayAdaptiveRebalance()) {
					if (isCurrent()) {
						void rebalanceParticipationDebounced?.call();
					}
					return false;
				}

				const peers = this.replicationIndex;
				const usedMemory = await this.getMemoryUsage();
				if (!isCurrent()) return false;
				this.scheduleReplicationStatusRefreshForStorage(usedMemory);
				let dynamicRange = await this.getDynamicRange();
				if (!isCurrent()) return false;

				if (!dynamicRange) {
					return; // not allowed to replicate
				}

				if (
					this.replicationController.maxMemoryLimit != null &&
					usedMemory > this.replicationController.maxMemoryLimit
				) {
					// Memory pressure can leave prunable frontier heads even when the
					// coordinate-index scan has no pending prune candidates.
					await this.pruneIndexedEntriesNoLongerLed(
						undefined,
						ownershipLifecycleController,
					);
					if (!isCurrent()) return false;
					await this.pruneCurrentHeadsNoLongerLed(
						undefined,
						ownershipLifecycleController,
					);
					if (!isCurrent()) return false;
				}

				const peersSize = (await peers.getSize()) || 1;
				if (!isCurrent()) return false;
				const totalParticipation = await this.calculateTotalParticipation();
				if (!isCurrent()) return false;

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
					if (!isCurrent()) return false;
					if (!canReplicate) {
						return false;
					}

					await this.startAnnounceReplicating(
						[dynamicRange],
						{
							checkDuplicates: false,
							reset: false,
							shouldApply: isCurrent,
						},
						ownershipLifecycleController,
					);
					if (!isCurrent()) return false;

					/* await this._updateRole(newRole, onRoleChange); */
					if (isCurrent()) {
						void rebalanceParticipationDebounced?.call();
					}

					return true;
				} else {
					if (isCurrent()) {
						void rebalanceParticipationDebounced?.call();
					}
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

	private onEntryAdded(entry: Entry<any>) {
		const ih = this._pendingIHave.get(entry.hash);

		if (ih) {
			ih.clear();
			this.runPendingIHaveCallback(ih, entry);
		}

		this.syncronizer.onEntryAdded(entry);
	}

	private onEntryAddedHash(hash: string, materializeEntry?: () => Entry<any>) {
		const ih = this._pendingIHave.get(hash);
		if (ih) {
			if (!materializeEntry) {
				throw new Error("Missing entry materializer for pending IHave");
			}
			const entry = materializeEntry();
			ih.clear();
			this.runPendingIHaveCallback(ih, entry);
			this.syncronizer.onEntryAdded(entry);
			return;
		}
		if (this.syncronizer.onEntryAddedHash) {
			this.syncronizer.onEntryAddedHash(hash);
			return;
		}
		if (!materializeEntry) {
			throw new Error("Missing entry materializer for synchronizer update");
		}
		this.syncronizer.onEntryAdded(materializeEntry());
	}

	private resetPendingIHaveTimeout(pending: PendingIHave<T>): void {
		pending.expiresAt =
			Date.now() + Math.max(0, Number(this._respondToIHaveTimeout ?? 0));
		this.schedulePendingIHaveExpiry(pending.expiresAt);
	}

	private clearPendingIHaveTimeout(pending: PendingIHave<T>): void {
		pending.expiresAt = undefined;
	}

	private schedulePendingIHaveExpiry(deadline: number): void {
		if (deadline >= this._pendingIHaveExpiryDeadline) {
			return;
		}
		if (this._pendingIHaveExpiryTimer) {
			clearTimeout(this._pendingIHaveExpiryTimer);
		}
		this._pendingIHaveExpiryDeadline = deadline;
		this._pendingIHaveExpiryTimer = setTimeout(
			() => this.expirePendingIHaves(),
			Math.max(0, deadline - Date.now()),
		);
		this._pendingIHaveExpiryTimer.unref?.();
	}

	private expirePendingIHaves(): void {
		this._pendingIHaveExpiryTimer = undefined;
		this._pendingIHaveExpiryDeadline = Number.POSITIVE_INFINITY;
		if (this.closed) {
			return;
		}
		const now = Date.now();
		let nextDeadline = Number.POSITIVE_INFINITY;
		for (const [hash, pending] of this._pendingIHave) {
			const expiresAt = pending.expiresAt;
			if (expiresAt == null) {
				continue;
			}
			if (expiresAt <= now) {
				pending.expiresAt = undefined;
				this._pendingIHave.delete(hash);
				continue;
			}
			if (expiresAt < nextDeadline) {
				nextDeadline = expiresAt;
			}
		}
		if (nextDeadline !== Number.POSITIVE_INFINITY) {
			this.schedulePendingIHaveExpiry(nextDeadline);
		}
	}

	onEntryRemoved(hash: string) {
		this.syncronizer.onEntryRemoved(hash);
	}

	private onEntryRemovedHashes(hashes: string[]) {
		if (hashes.length === 0) {
			return;
		}
		if (this.syncronizer.onEntryRemovedHashes) {
			this.syncronizer.onEntryRemovedHashes(hashes);
			return;
		}
		for (const hash of hashes) {
			this.syncronizer.onEntryRemoved(hash);
		}
	}
}
