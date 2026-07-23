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
	NativeBackboneAppendProfile,
	NativeBackboneAppendResult,
	NativeBackboneCoordinateCommitColumns,
	NativeBackboneCoordinateFields,
	NativeBackboneLogCommitEntry,
	NativeBackboneRawReceiveGroupAssignmentPlan,
	NativeBackboneRawReceiveGroupIndexPlan,
	NativeBackboneRawReceiveGroupLeaderPlan,
	NativeBackboneRawReceiveGroupPlan,
	NativeBackboneRawReceiveSelectionPlan,
	NativeBackboneRequestPruneHintColumns,
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
	ACK_CONTROL_PRIORITY,
	AcknowledgeDelivery,
	AnyWhere,
	BACKGROUND_MESSAGE_PRIORITY,
	CONVERGENCE_MESSAGE_PRIORITY,
	DataMessage,
	DeliveryError,
	MessageHeader,
	NotStartedError,
	type RouteHint,
	SilentDelivery,
	createRequestTransportContext,
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
import {
	CheckedPruneCoordinator,
	type CheckedPruneEntry,
	type CheckedPruneLeaderMap,
	type CheckedPruneRetryState,
} from "./checked-prune.js";
import { type CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
import {
	type DebouncedAccumulatorMap,
	debouncedAccumulatorMap,
} from "./debounce.js";
import { NativeDurableCommitError, NoPeersError } from "./errors.js";
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
	ResponseIPrune,
	SYNC_CAPABILITY_RAW_EXCHANGE_HEADS,
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
import {
	MAX_U32,
	MAX_U64,
	type NumberFromType,
	type Numbers,
	createNumbers,
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
	isEntryReplicated,
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
	ReplicationError,
	type ReplicationLimits,
	ReplicationPingMessage,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
	StoppedReplicating,
	decodeReplicas,
	encodeReplicas,
	maxReplicas,
} from "./replication.js";
import { Observer, Replicator } from "./role.js";
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
import { RatelessIBLTSynchronizer } from "./sync/rateless-iblt.js";
import {
	ConfirmEntriesMessage,
	SYNC_MESSAGE_PRIORITY,
	SimpleSyncronizer,
} from "./sync/simple.js";
import { groupByGid, tryGroupByGidSync } from "./utils.js";

type SharedLogServicesWithFanout = {
	fanout?: FanoutTree;
};

const getSharedLogFanoutService = (services: unknown): FanoutTree | undefined =>
	(services as SharedLogServicesWithFanout).fanout;

type MaybePromise<T> = T | Promise<T>;

const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> =>
	!!value && typeof (value as Promise<T>).then === "function";

const mapMaybePromise = <T, R>(
	value: MaybePromise<T>,
	fn: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (isPromiseLike(value) ? value.then(fn) : fn(value));

type PendingIHave<T> = {
	resetTimeout: () => void;
	requesting: Set<string>;
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
	NativeDurableCommitError,
	NoPeersError,
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
	sublevel(
		name: string,
		options?: {
			compactOnClose?: boolean;
			compactOnCloseMinJournalBytes?: number;
			durability?: "normal" | "strict";
		},
	): MaybePromise<AnyStore>;
};

const createNativeDurableBlockStore = async (
	storage: DurableBlockSublevelStore,
): Promise<AnyBlockStore> =>
	new AnyBlockStore(
		await storage.sublevel("blocks", {
			// Strict mirrors remain WAL-backed across close. The available snapshot
			// rewrite is not a crash-atomic generation protocol, so thresholds must
			// not re-enable it behind this acknowledgement boundary.
			compactOnClose: false,
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

/** The native backbone's in-wasm-memory block store. */
type NativeBackboneBlocks = NativePeerbitBackbone["blocks"];

type NativeCommitOwnershipToken = {
	id: number;
	rows: Map<
		string,
		{
			generation: number;
			durableExistedBefore?: boolean;
			shared: boolean;
		}
	>;
};

/**
 * Write-through block store bridging the native backbone's in-wasm-memory block
 * store to a durable per-program {@link AnyBlockStore}.
 *
 * WHY: when the native backbone is active the log's entry blocks live only in
 * the native wasm block map (`NativeBackboneBlockStore.persisted() === false`).
 * On a restart that map is empty, so the native graph cannot reload heads the
 * durable heads index still lists ("Failed to load entry from head"). This
 * wrapper mirrors every write into the same durable `blocks` sublevel the
 * non-native path uses. On a native miss, reads fall through to durable storage
 * and lazily repopulate the wasm map so the native graph can walk the DAG again.
 *
 * The native store stays the authoritative hot store the native graph reads
 * from: reads hit native first and only fall back to durable (repopulating
 * native on a hit so subsequent native-graph reads succeed).
 *
 * METHOD SURFACE (see #1006): `RemoteBlocks` and the log feature-detect the
 * optional batch methods (`putMany`/`putKnown`/`putKnownMany`/
 * `putKnownManyColumns`/`rmMany`). To keep the receive-fusion / columnar fast
 * paths engaged this wrapper preserves the native store's optional write
 * methods — including `putKnownManyColumns`, which `AnyBlockStore` does not have
 * — and delegates each. It adds only local durability/trim coordination hooks
 * consumed by `RemoteBlocks` and the log; protocol/native-handle capabilities
 * such as `getBlockResponsePayload`/`getNativeLogBlockStoreHandle` stay absent,
 * so their optional-chained probes keep the existing fallback behavior.
 */
class NativeBackboneWriteThroughBlockStore {
	constructor(
		private readonly native: NativeBackboneBlocks,
		private readonly durable: AnyBlockStore,
	) {}

	// A durable mirror write that cannot be awaited at its call site (the
	// columnar putKnownManyColumns fast path must return a synchronous string[]
	// because RemoteBlocks.putKnownManyColumns treats the result as sync) is
	// tracked here instead of being silently `void`ed. Its rejection is stored
	// and re-thrown on the next awaited wrapper method (and on stop()), so a
	// failed durable write (IO/disk-full) surfaces as an error rather than
	// vanishing and leaving the block out of durable while native/log report
	// success. `.catch` also prevents unhandled-rejection noise.
	private readonly pendingDurableWrites = new Set<Promise<unknown>>();
	private nativeDurableCommitFailure?: NativeDurableCommitError;
	private stopCompleted = false;
	private readonly nativeDeleteTombstones = new Map<string, number>();
	private nativeDeleteEpoch = 0;
	private readonly nativeBlockWriteGenerations = new Map<string, number>();
	private readonly pendingNativeDeleteCleanup = new Map<string, number>();
	private readonly stagedNativeDeleteCleanups = new Map<
		number,
		Map<string, number>
	>();
	private nextNativeDeleteCleanupToken = 0;
	private nativeDeleteCleanupRunning: Promise<void> | undefined;
	private nextNativeCommitOwnershipToken = 0;
	private readonly nativeCommitOwnerships = new Map<
		number,
		NativeCommitOwnershipToken
	>();
	private readonly nativeCommitOwnershipsByCid = new Map<string, Set<number>>();

	getNativeDurableCommitFailure(): NativeDurableCommitError | undefined {
		return this.nativeDurableCommitFailure;
	}

	private recordNativeDurableCommitFailure(
		cause: unknown,
		options?: {
			committedCids?: Iterable<string>;
			failedCids?: Iterable<string>;
		},
	): NativeDurableCommitError {
		if (this.nativeDurableCommitFailure) {
			this.nativeDurableCommitFailure.addCommitContext(options);
			return this.nativeDurableCommitFailure;
		}
		this.nativeDurableCommitFailure =
			cause instanceof NativeDurableCommitError
				? cause
				: new NativeDurableCommitError(cause, options);
		if (cause instanceof NativeDurableCommitError) {
			cause.addCommitContext(options);
		}
		return this.nativeDurableCommitFailure;
	}

	private throwIfNativeDurableCommitFailed(): void {
		if (this.nativeDurableCommitFailure) {
			throw this.nativeDurableCommitFailure;
		}
	}

	throwIfDurableWritesFailed(): void {
		this.throwIfNativeDurableCommitFailed();
	}

	private async commitDurableMutation<T>(
		operation: () => MaybePromise<T>,
		committedCids: Iterable<string>,
		failedCids?: Iterable<string>,
	): Promise<T> {
		this.throwIfNativeDurableCommitFailed();
		const committedCidList = [...committedCids];
		const failedCidList = failedCids ? [...failedCids] : committedCidList;
		let result: T;
		const operationResult = Promise.resolve().then(operation);
		this.trackAwaitedDurable(operationResult);
		try {
			result = await operationResult;
		} catch (error) {
			throw this.recordNativeDurableCommitFailure(error, {
				committedCids: committedCidList,
				failedCids: failedCidList,
			});
		}
		// A different concurrent native mutation may have poisoned the wrapper
		// while this durable call was in flight. Include this operation among the
		// native-applied facts, but not among durable calls that actually failed.
		if (this.nativeDurableCommitFailure) {
			this.nativeDurableCommitFailure.addCommitContext({
				committedCids: committedCidList,
				failedCids: [],
			});
			throw this.nativeDurableCommitFailure;
		}
		return result;
	}

	private beginNativeDelete(cids: string[]): void {
		this.nativeDeleteEpoch++;
		for (const cid of cids) {
			this.nativeDeleteTombstones.set(
				cid,
				(this.nativeDeleteTombstones.get(cid) ?? 0) + 1,
			);
		}
	}

	private endNativeDelete(cids: string[]): void {
		for (const cid of cids) {
			const remaining = (this.nativeDeleteTombstones.get(cid) ?? 1) - 1;
			if (remaining <= 0) {
				this.nativeDeleteTombstones.delete(cid);
			} else {
				this.nativeDeleteTombstones.set(cid, remaining);
			}
		}
	}

	private isNativeDeletePending(cid: string): boolean {
		return this.nativeDeleteTombstones.has(cid);
	}

	// A CID can be legitimately re-added after a native trim (content addressing
	// makes the bytes identical, but its liveness is new). Cancel any queued trim
	// for that CID and advance its generation before the write is exposed. An
	// already-running cleanup uses the generation/pending map to avoid deleting
	// the new native value; synchronous columnar writes also chain their durable
	// mirror behind that cleanup below.
	private noteNativeBlockWrite(cids: string[]): Map<string, number> {
		const generations = new Map<string, number>();
		for (const cid of new Set(cids)) {
			const generation = (this.nativeBlockWriteGenerations.get(cid) ?? 0) + 1;
			this.nativeBlockWriteGenerations.set(cid, generation);
			generations.set(cid, generation);
			if (this.pendingNativeDeleteCleanup.delete(cid)) {
				this.endNativeDelete([cid]);
			}
			for (const staged of this.stagedNativeDeleteCleanups.values()) {
				if (staged.delete(cid)) {
					this.endNativeDelete([cid]);
				}
			}
		}
		return generations;
	}

	private beginNativeCommitOwnership(
		generations: Map<string, number>,
	): NativeCommitOwnershipToken | undefined {
		if (generations.size === 0) {
			return undefined;
		}
		const token: NativeCommitOwnershipToken = {
			id: ++this.nextNativeCommitOwnershipToken,
			rows: new Map(),
		};
		for (const [cid, generation] of generations) {
			const owners = this.nativeCommitOwnershipsByCid.get(cid) ?? new Set();
			const shared = owners.size > 0;
			for (const ownerId of owners) {
				const owner = this.nativeCommitOwnerships.get(ownerId);
				const row = owner?.rows.get(cid);
				if (row) row.shared = true;
			}
			owners.add(token.id);
			this.nativeCommitOwnershipsByCid.set(cid, owners);
			token.rows.set(cid, { generation, shared });
		}
		this.nativeCommitOwnerships.set(token.id, token);
		return token;
	}

	private releaseNativeCommitOwnership(token: unknown): void {
		if (
			!token ||
			typeof token !== "object" ||
			typeof (token as NativeCommitOwnershipToken).id !== "number"
		) {
			return;
		}
		const owned = this.nativeCommitOwnerships.get(
			(token as NativeCommitOwnershipToken).id,
		);
		if (owned !== token) {
			return;
		}
		this.nativeCommitOwnerships.delete(owned.id);
		for (const cid of owned.rows.keys()) {
			const owners = this.nativeCommitOwnershipsByCid.get(cid);
			owners?.delete(owned.id);
			if (owners?.size === 0) {
				this.nativeCommitOwnershipsByCid.delete(cid);
			}
		}
	}

	acknowledgeNativeCommitOwnership(token: unknown): void {
		this.releaseNativeCommitOwnership(token);
	}

	private enqueueNativeDeleteCleanup(cids: string[]): void {
		for (const cid of new Set(cids)) {
			if (!this.pendingNativeDeleteCleanup.has(cid)) {
				this.pendingNativeDeleteCleanup.set(
					cid,
					this.nativeBlockWriteGenerations.get(cid) ?? 0,
				);
				this.beginNativeDelete([cid]);
			}
		}
	}

	private releaseStagedNativeDeleteCleanup(token: number): boolean {
		const staged = this.stagedNativeDeleteCleanups.get(token);
		if (!staged) {
			return false;
		}
		this.stagedNativeDeleteCleanups.delete(token);
		for (const [cid] of staged) {
			this.endNativeDelete([cid]);
		}
		return true;
	}

	private discardStagedNativeDeleteCleanups(): void {
		for (const token of [...this.stagedNativeDeleteCleanups.keys()]) {
			this.releaseStagedNativeDeleteCleanup(token);
		}
	}

	// Native commit callbacks call this immediately after the native transaction
	// returns, before awaiting the new block's durable mirror. This stages read
	// tombstones only. Durable deletion is promoted later by the exact EntryIndex
	// consume token, so an unacknowledged/failed append cannot delete the old
	// durable head that the lower log still publishes.
	beginNativeDeleteCleanup(cids: string[]): number | undefined {
		this.throwIfNativeDurableCommitFailed();
		const uniqueCids = [...new Set(cids)];
		if (uniqueCids.length === 0) {
			return undefined;
		}
		const token = ++this.nextNativeDeleteCleanupToken;
		const staged = new Map(
			uniqueCids.map((cid) => [
				cid,
				this.nativeBlockWriteGenerations.get(cid) ?? 0,
			]),
		);
		this.stagedNativeDeleteCleanups.set(token, staged);
		this.beginNativeDelete(uniqueCids);
		return token;
	}

	cancelNativeDeleteCleanup(cleanupToken: unknown): void {
		if (typeof cleanupToken === "number") {
			this.releaseStagedNativeDeleteCleanup(cleanupToken);
		}
	}

	private async waitForNativeDeleteCleanup(): Promise<void> {
		while (this.nativeDeleteCleanupRunning) {
			await this.nativeDeleteCleanupRunning;
		}
	}

	private async waitForTrackedDurableWrites(): Promise<void> {
		while (this.pendingDurableWrites.size > 0) {
			await Promise.allSettled([...this.pendingDurableWrites]);
		}
	}

	private async retryNativeDeleteCleanup(options?: {
		allowPoisoned?: boolean;
		throwOnFailure?: boolean;
	}): Promise<void> {
		await this.waitForNativeDeleteCleanup();
		if (this.nativeDurableCommitFailure && !options?.allowPoisoned) {
			throw this.nativeDurableCommitFailure;
		}
		// A synchronous columnar write may already have scheduled its durable
		// mirror. Let it settle before deleting queued CIDs, but leave any recorded
		// error for drainDurable() to surface to its owning operation/stop.
		await this.waitForTrackedDurableWrites();
		await this.waitForNativeDeleteCleanup();
		// A tracked mirror or the cleanup we just waited for may have poisoned the
		// wrapper. Do not begin another durable mutation on the ordinary path after
		// that asynchronous boundary. stop() alone opts into one cleanup retry so a
		// transient delete failure can still release its tombstones and resources.
		if (this.nativeDurableCommitFailure && !options?.allowPoisoned) {
			throw this.nativeDurableCommitFailure;
		}
		if (this.pendingNativeDeleteCleanup.size === 0) {
			return;
		}
		const cleanupEntries = [...this.pendingNativeDeleteCleanup].filter(
			([cid, generation]) =>
				(this.nativeBlockWriteGenerations.get(cid) ?? 0) === generation,
		);
		if (cleanupEntries.length === 0) {
			return;
		}
		const cids = cleanupEntries.map(([cid]) => cid);
		let cleanupFailure: unknown;
		const running = (async () => {
			let durableRemoved = false;
			let nativeRemoved = false;
			try {
				await this.durable.rmMany(cids);
				durableRemoved = true;
			} catch (error) {
				cleanupFailure = error;
				// The new entry blocks are already durable at this point and the native
				// transaction has selected the new graph/index state. Old content-addressed
				// blocks are therefore harmless unreachable orphans. Keep this cleanup as
				// retryable debt instead of poisoning/rolling back a fully durable append;
				// a partial rmMany is safe for the same reason.
				warn(
					`Failed durable native-trim cleanup; retaining retry debt: ${String(error)}`,
				);
			} finally {
				// A read that began in the native-transaction -> cleanup-hook gap may
				// have repopulated native. Always repeat the native removal, even when
				// durable rm failed. Exclude CIDs re-added while durable IO was pending;
				// their generation change cancelled the queued delete.
				const stillDeleted = cids.filter((cid) =>
					this.pendingNativeDeleteCleanup.has(cid),
				);
				try {
					if (stillDeleted.length > 0) {
						await this.native.rmMany(stillDeleted);
					}
					nativeRemoved = true;
				} catch (error) {
					cleanupFailure ??= error;
					warn(
						`Failed to repeat native-trim hot block removal: ${String(error)}`,
					);
				}
			}
			if (durableRemoved && nativeRemoved) {
				for (const [cid, generation] of cleanupEntries) {
					if (this.pendingNativeDeleteCleanup.get(cid) === generation) {
						this.pendingNativeDeleteCleanup.delete(cid);
						this.nativeBlockWriteGenerations.delete(cid);
						this.endNativeDelete([cid]);
					}
				}
			}
		})();
		this.nativeDeleteCleanupRunning = running;
		try {
			await running;
		} finally {
			if (this.nativeDeleteCleanupRunning === running) {
				this.nativeDeleteCleanupRunning = undefined;
			}
		}
		if (options?.throwOnFailure && cleanupFailure !== undefined) {
			throw cleanupFailure;
		}
	}

	private trackDurable(result: unknown, cids: string[]): void {
		// The durable store may answer synchronously (an in-memory or already
		// resolved store); only a real pending promise needs tracking. A sync
		// success is already durable; a sync throw would have propagated already.
		if (!isPromiseLike(result)) {
			return;
		}
		const tracked = Promise.resolve(result).then(
			() => {
				this.pendingDurableWrites.delete(tracked);
			},
			(error) => {
				this.pendingDurableWrites.delete(tracked);
				this.recordNativeDurableCommitFailure(error, {
					committedCids: cids,
					failedCids: cids,
				});
			},
		);
		this.pendingDurableWrites.add(tracked);
	}

	// Awaited mirror writes surface their own rejection to the append that
	// created them. Track a non-rejecting settlement companion as well so stop()
	// and later wrapper operations cannot close/use durable storage while that
	// append barrier is still in flight.
	private trackAwaitedDurable(result: Promise<unknown>): void {
		const tracked = result.then(
			() => {
				this.pendingDurableWrites.delete(tracked);
			},
			() => {
				this.pendingDurableWrites.delete(tracked);
			},
		);
		this.pendingDurableWrites.add(tracked);
	}

	// Wait for every tracked (sync-path) durable write to settle, then surface
	// the first failure. Awaited methods call this so a prior columnar durable
	// failure propagates as back-pressure to the next caller.
	private async drainDurable(): Promise<void> {
		await this.waitForTrackedDurableWrites();
		this.throwIfNativeDurableCommitFailed();
	}

	// --- lifecycle -------------------------------------------------------
	// The native store's lifecycle hooks are no-ops; only the durable store
	// needs starting/stopping. The wasm map is NOT eagerly rehydrated from disk:
	// entry blocks are pulled back lazily on demand through the read fallback in
	// getMany()/get() (durable hit -> repopulate the wasm map), which is what the
	// log's DAG walk (EntryIndex.resolveMany -> store.getMany) exercises. Keeping
	// the wasm map cold on open is required by the strict-native resident
	// coordinate-state optimization: a reopened non-replicating native node must
	// report hasBlock(head) === false and answer a same-signer append from the
	// persisted coordinate + signer facts without resolving the entry block.
	async start(): Promise<void> {
		await this.durable.start();
		this.stopCompleted = false;
	}

	async stop(): Promise<void> {
		if (this.stopCompleted) {
			return;
		}
		// Surface any tracked (sync-path) durable write failure and ensure all
		// mirror writes have settled before the durable store is torn down. Closing
		// the durable store is unconditional: a prior columnar mirror failure must
		// not leak the store lifecycle resource.
		let firstError: unknown;
		let shutdownError: unknown;
		try {
			await this.drainDurable();
		} catch (error) {
			firstError = error;
		}
		// Tokens not consumed by EntryIndex belong to native prepares that never
		// published their lower-log trim. Release their read tombstones, but never
		// promote them to durable deletion during shutdown.
		this.discardStagedNativeDeleteCleanups();
		try {
			await this.retryNativeDeleteCleanup({ allowPoisoned: true });
		} catch (error) {
			firstError ??= error;
			shutdownError ??= error;
		}
		try {
			await this.durable.stop();
		} catch (error) {
			firstError ??= error;
			shutdownError ??= error;
		}
		if (shutdownError === undefined) {
			// A durable poison belongs to the generation being closed. Report it once
			// so the owning terminal call observes the failed append, but remember that
			// every mandatory shutdown stage completed. Conservative content-addressed
			// trim debt may remain in this retired wrapper after best-effort cleanup; a
			// fresh generation gets a new wrapper and must not be wedged by that debt.
			// The exact terminal retry may therefore finish parent bookkeeping without
			// rethrowing the same latched poison forever.
			this.stopCompleted = true;
		}
		if (firstError !== undefined) {
			throw firstError;
		}
	}

	status() {
		return this.durable.status();
	}

	waitFor(): Promise<string[]> {
		return Promise.resolve([]);
	}

	// Native commit APIs must keep their block-store callback synchronous. The
	// lower log calls this barrier after that callback reports committed blocks
	// and before publishing index/head facts.
	waitForDurableWrites(): Promise<void> {
		return this.drainDurable();
	}

	// Mirror a single already-committed block (present in the wasm map) to the
	// durable store ONLY. Used by the native commit-only append fast path: the
	// native prepare commits the entry block into the wasm map and returns no raw
	// bytes, and the strict-native resident-coordinate path deliberately does NOT
	// route the block through the log's finishBlocks/putKnown* (that would disturb
	// the commit-only append path the RCS optimization depends on). Instead the
	// caller reads the committed bytes back and calls this so the block lands in
	// durable directly. The caller awaits this method before acknowledging its
	// append, so a failed write rejects the append that produced the block.
	async mirrorToDurable(
		cid: string,
		bytes: Uint8Array,
		options?: { nativeTrimmed?: boolean },
	): Promise<unknown> {
		this.throwIfNativeDurableCommitFailed();
		// The native commit happened before this call. Mark the CID live before
		// awaiting anything so a concurrent retry of an older trim cannot remove
		// the newly committed hot block.
		let ownership: NativeCommitOwnershipToken | undefined;
		if (options?.nativeTrimmed !== true) {
			ownership = this.beginNativeCommitOwnership(
				this.noteNativeBlockWrite([cid]),
			);
			await this.waitForNativeDeleteCleanup();
		}
		try {
			await this.drainDurable();
			if (ownership) {
				const existed = await this.durable.hasMany([...ownership.rows.keys()]);
				let index = 0;
				for (const row of ownership.rows.values()) {
					row.durableExistedBefore = existed[index++] === true;
				}
			}
			const result = this.commitDurableMutation(
				() => this.durable.putKnown(cid, bytes),
				[cid],
			);
			await result;
			await this.retryNativeDeleteCleanup();
			this.throwIfNativeDurableCommitFailed();
			return ownership;
		} catch (error) {
			// The caller never receives an ownership token for an indeterminate write.
			// Release the in-memory claim and preserve any bytes the backend may have
			// applied before rejecting.
			this.releaseNativeCommitOwnership(ownership);
			throw error;
		}
	}

	async mirrorManyToDurable(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
		options?: { nativeTrimmedCids?: ReadonlySet<string> },
	): Promise<unknown> {
		this.throwIfNativeDurableCommitFailed();
		const cids = blocks.map(([cid]) => cid);
		// Rows trimmed later in the same native batch deliberately remain owned by
		// that batch's staged cleanup; only mark surviving rows live.
		const liveCids = cids.filter(
			(cid) => !options?.nativeTrimmedCids?.has(cid),
		);
		const ownership = this.beginNativeCommitOwnership(
			this.noteNativeBlockWrite(liveCids),
		);
		if (liveCids.length > 0) {
			await this.waitForNativeDeleteCleanup();
		}
		try {
			await this.drainDurable();
			if (ownership) {
				const existed = await this.durable.hasMany([...ownership.rows.keys()]);
				let index = 0;
				for (const row of ownership.rows.values()) {
					row.durableExistedBefore = existed[index++] === true;
				}
			}
			if (blocks.length > 0) {
				await this.commitDurableMutation(
					() => this.durable.putKnownMany(blocks),
					cids,
				);
			}
			await this.retryNativeDeleteCleanup();
			this.throwIfNativeDurableCommitFailed();
			return ownership;
		} catch (error) {
			this.releaseNativeCommitOwnership(ownership);
			throw error;
		}
	}

	async rollbackFailedNativeCommits(
		cids: string[],
		restoreNativeCids: string[] = [],
		ownershipToken?: unknown,
	): Promise<void> {
		// This is the sole mutation allowed after poison: it removes a native
		// transaction that the lower log never published. Do not route through the
		// guarded rmMany path, and settle the failed mirror before compensating it.
		await this.waitForTrackedDurableWrites();
		// A failed replacement never published its trim. Reassert the restored CIDs
		// as live before any compensation IO so staged/pending delete intents cannot
		// remove the last acknowledged blocks during stop or reopen.
		const ownership =
			ownershipToken && typeof ownershipToken === "object"
				? this.nativeCommitOwnerships.get(
						(ownershipToken as NativeCommitOwnershipToken).id,
					)
				: undefined;
		const verifiedOwnership =
			ownership && ownership === ownershipToken ? ownership : undefined;
		const restoreSet = new Set(restoreNativeCids);
		const safeDurableDeletes = verifiedOwnership
			? [...new Set(cids)].filter((cid) => {
					const row = verifiedOwnership.rows.get(cid);
					const owners = this.nativeCommitOwnershipsByCid.get(cid);
					return (
						!restoreSet.has(cid) &&
						row?.durableExistedBefore === false &&
						row.shared === false &&
						(this.nativeBlockWriteGenerations.get(cid) ?? 0) ===
							row.generation &&
						owners?.size === 1 &&
						owners.has(verifiedOwnership.id)
					);
				})
			: [];
		this.noteNativeBlockWrite(restoreNativeCids);
		let firstError: unknown;
		try {
			if (safeDurableDeletes.length > 0) {
				await this.durable.rmMany(safeDurableDeletes);
			}
		} catch (error) {
			firstError = error;
		}
		// A native prepare runs before ownership can observe the hot map, so it cannot
		// prove that a CID was absent there before this operation. Keep native bytes as
		// unreachable orphans rather than deleting acknowledged/shared/restored data.
		if (restoreNativeCids.length > 0) {
			try {
				const values = await this.durable.getMany(restoreNativeCids);
				const restore: Array<readonly [string, Uint8Array]> = [];
				for (let index = 0; index < restoreNativeCids.length; index++) {
					const value = values[index];
					if (value) restore.push([restoreNativeCids[index]!, value]);
				}
				if (restore.length > 0) {
					this.native.putKnownMany(restore);
				}
			} catch (error) {
				firstError ??= error;
			}
		}
		this.releaseNativeCommitOwnership(ownershipToken);
		if (firstError !== undefined) throw firstError;
	}

	/**
	 * Compensate a native prepare that failed before its durable mirror began.
	 * Durable presence proves a same-CID acknowledged owner; an active ownership
	 * token proves a concurrent mirror. Only an unowned, non-durable hot block is
	 * exclusively attributable to the failed prepare and safe to remove.
	 */
	async rollbackUnmirroredNativeCommits(
		cids: string[],
		restoreNativeCids: string[] = [],
	): Promise<void> {
		const unique = [...new Set(cids)];
		// Native prepares bypass this wrapper. Any observed wrapper generation is
		// therefore evidence of a generic/same-CID writer, not of the failed prepare.
		// Snapshot before the first await and require both absence and stability so a
		// write starting before or during durable.hasMany cannot lose its hot value to
		// a stale `false` result.
		const genericWriteGenerations = new Map(
			unique.map((cid) => [cid, this.nativeBlockWriteGenerations.get(cid)]),
		);
		await this.rollbackFailedNativeCommits(cids, restoreNativeCids);
		const durablePresence = await this.durable.hasMany(unique);
		const restore = new Set(restoreNativeCids);
		const safeNativeDeletes = unique.filter(
			(cid, index) =>
				!restore.has(cid) &&
				durablePresence[index] !== true &&
				genericWriteGenerations.get(cid) === undefined &&
				this.nativeBlockWriteGenerations.get(cid) === undefined &&
				(this.nativeCommitOwnershipsByCid.get(cid)?.size ?? 0) === 0,
		);
		if (safeNativeDeletes.length > 0) {
			await this.native.rmMany(safeNativeDeletes);
		}
	}

	// --- writes (apply to BOTH: native first for the hot path, then durable) ---
	async put(
		data: Uint8Array | { block: { bytes: Uint8Array }; cid: string },
	): Promise<string> {
		await this.drainDurable();
		const cid = await this.native.put(data as any);
		// The native store computes a raw-codec CID for a `Uint8Array`, storing
		// the bytes verbatim (raw codec is identity), and stores `block.bytes`
		// for the pre-CIDed object form. Either way the input bytes match what
		// native stored, so feed durable the known cid+bytes without recomputing.
		const value =
			data instanceof Uint8Array
				? data
				: (data as { block: { bytes: Uint8Array } }).block.bytes;
		this.noteNativeBlockWrite([cid]);
		await this.waitForNativeDeleteCleanup();
		// put() may have yielded while calculating the CID. Restore the hot value
		// after any older cleanup that was already in flight.
		this.throwIfNativeDurableCommitFailed();
		this.native.putKnown(cid, value);
		await this.commitDurableMutation(
			() => this.durable.putKnown(cid, value),
			[cid],
		);
		return cid;
	}

	async putMany(
		blocks: Array<Uint8Array | { block: { bytes: Uint8Array }; cid: string }>,
	): Promise<string[]> {
		await this.drainDurable();
		const cids = await this.native.putMany(blocks as any);
		const durableBlocks: Array<readonly [string, Uint8Array]> = cids.map(
			(cid, index) => {
				const block = blocks[index]!;
				const value =
					block instanceof Uint8Array
						? block
						: (block as { block: { bytes: Uint8Array } }).block.bytes;
				return [cid, value] as const;
			},
		);
		this.noteNativeBlockWrite(cids);
		await this.waitForNativeDeleteCleanup();
		this.throwIfNativeDurableCommitFailed();
		this.native.putKnownMany(durableBlocks);
		await this.commitDurableMutation(
			() => this.durable.putKnownMany(durableBlocks),
			cids,
		);
		return cids;
	}

	// Native put is synchronous (the authoritative hot store); the durable mirror
	// is awaited so the returned promise resolves only after BOTH native and
	// durable succeed and a durable IO/disk-full failure rejects here instead of
	// being swallowed. RemoteBlocks.putKnown and the log's putKnownEntryBytesBatch
	// both await this method, so returning a promise is compatible.
	async putKnown(cid: string, bytes: Uint8Array): Promise<string> {
		await this.drainDurable();
		await this.waitForNativeDeleteCleanup();
		this.throwIfNativeDurableCommitFailed();
		const stored = this.native.putKnown(cid, bytes);
		this.noteNativeBlockWrite([cid]);
		await this.commitDurableMutation(
			() => this.durable.putKnown(cid, bytes),
			[cid],
		);
		return stored;
	}

	async putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): Promise<string[]> {
		await this.drainDurable();
		await this.waitForNativeDeleteCleanup();
		this.throwIfNativeDurableCommitFailed();
		const cids = this.native.putKnownMany(blocks);
		this.noteNativeBlockWrite(cids);
		await this.commitDurableMutation(
			() => this.durable.putKnownMany(blocks),
			cids,
		);
		return cids;
	}

	putKnownManyColumns(cids: string[], bytes: Uint8Array[]): string[] {
		this.throwIfNativeDurableCommitFailed();
		if (cids.length !== bytes.length) {
			throw new Error("Expected equal block column lengths");
		}
		const cleanupBarrier = this.nativeDeleteCleanupRunning;
		const stored = this.native.putKnownManyColumns(cids, bytes);
		this.noteNativeBlockWrite(cids);
		// AnyBlockStore has no columnar method; mirror via putKnownMany, which
		// takes [cid, bytes] tuples and hits the same batched store path.
		// This method must return a synchronous string[] (RemoteBlocks.putKnownManyColumns
		// consumes the result synchronously), so the durable write cannot be awaited
		// inline. Track it instead of `void`ing it so a durable rejection surfaces
		// on the next awaited wrapper method / stop() rather than being swallowed.
		const durableBlocks = cids.map(
			(cid, index) => [cid, bytes[index]!] as const,
		);
		let durableResult: unknown;
		try {
			durableResult = cleanupBarrier
				? cleanupBarrier.then(() => this.durable.putKnownMany(durableBlocks))
				: this.durable.putKnownMany(durableBlocks);
		} catch (error) {
			throw this.recordNativeDurableCommitFailure(error, {
				committedCids: cids,
				failedCids: cids,
			});
		}
		this.trackDurable(durableResult, cids);
		return stored;
	}

	// --- reads (native/wasm first; on miss, durable fallback + repopulate) ---
	// RemoteBlocks.get awaits this single-get, so on a native miss consult the
	// durable store (like getMany does) and repopulate the native map, rather
	// than returning undefined and only scheduling a background repopulate. That
	// avoids a spurious miss (which would otherwise fall through to a remote
	// read) for a block that is present on disk.
	async get(cid: string, _options?: unknown): Promise<Uint8Array | undefined> {
		if (this.nativeDurableCommitFailure) {
			// After poison, durable storage is the last acknowledged authority. Do not
			// repopulate or otherwise mutate the native store until reopen.
			return this.isNativeDeletePending(cid)
				? undefined
				: this.durable.get(cid);
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		if (this.isNativeDeletePending(cid)) {
			return undefined;
		}
		const local = this.native.get(cid);
		if (local != null) {
			return deleteEpoch === this.nativeDeleteEpoch ? local : this.get(cid);
		}
		const durableValue = await this.durable.get(cid);
		if (this.nativeDurableCommitFailure) {
			return this.isNativeDeletePending(cid) ? undefined : durableValue;
		}
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.get(cid);
		}
		if (durableValue != null) {
			// Repopulate the native map so the native graph reads it next time.
			this.native.putKnownManyColumns([cid], [durableValue]);
			return durableValue;
		}
		return undefined;
	}

	async getMany(cids: string[]): Promise<Array<Uint8Array | undefined>> {
		if (this.nativeDurableCommitFailure) {
			const values = await this.durable.getMany(cids);
			for (let index = 0; index < cids.length; index++) {
				if (this.isNativeDeletePending(cids[index]!)) {
					values[index] = undefined;
				}
			}
			return values;
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		const results = await this.native.getMany(cids);
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.getMany(cids);
		}
		const missing: string[] = [];
		const missingIndexes: number[] = [];
		for (let i = 0; i < results.length; i++) {
			if (this.isNativeDeletePending(cids[i]!)) {
				results[i] = undefined;
			} else if (results[i] == null) {
				missing.push(cids[i]!);
				missingIndexes.push(i);
			}
		}
		if (missing.length === 0) {
			return results;
		}
		const durableValues = await this.durable.getMany(missing);
		if (this.nativeDurableCommitFailure) {
			const values = await this.durable.getMany(cids);
			for (let index = 0; index < cids.length; index++) {
				if (this.isNativeDeletePending(cids[index]!)) {
					values[index] = undefined;
				}
			}
			return values;
		}
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.getMany(cids);
		}
		const repopulateCids: string[] = [];
		const repopulateBytes: Uint8Array[] = [];
		for (
			let missingIndex = 0;
			missingIndex < missingIndexes.length;
			missingIndex++
		) {
			const i = missingIndexes[missingIndex]!;
			const value = durableValues[missingIndex];
			if (value != null) {
				results[i] = value;
				repopulateCids.push(cids[i]!);
				repopulateBytes.push(value);
			}
		}
		if (repopulateCids.length > 0) {
			// Repopulate the native map so the native graph sees these blocks.
			this.native.putKnownManyColumns(repopulateCids, repopulateBytes);
		}
		return results;
	}

	async has(cid: string): Promise<boolean> {
		if (this.nativeDurableCommitFailure) {
			return this.isNativeDeletePending(cid) ? false : this.durable.has(cid);
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		if (this.isNativeDeletePending(cid)) {
			return false;
		}
		if (this.native.has(cid)) {
			return deleteEpoch === this.nativeDeleteEpoch ? true : this.has(cid);
		}
		// Mirror getMany/hasMany: a block absent from the native wasm map may still
		// be present in the durable store (e.g. persisted on disk but not yet
		// repopulated into wasm). Consult durable on a native miss so presence
		// checks agree with the resolves that getMany/hasMany already durable-fall
		// back on. `Blocks.has` is declared `MaybePromise<boolean>`, so returning a
		// promise here is contract-compatible.
		const durableHas = await this.durable.has(cid);
		return deleteEpoch === this.nativeDeleteEpoch ? durableHas : this.has(cid);
	}

	async hasMany(cids: string[]): Promise<boolean[]> {
		if (this.nativeDurableCommitFailure) {
			const values = await this.durable.hasMany(cids);
			for (let index = 0; index < cids.length; index++) {
				if (this.isNativeDeletePending(cids[index]!)) {
					values[index] = false;
				}
			}
			return values;
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		const nativeHas = await this.native.hasMany(cids);
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.hasMany(cids);
		}
		const missing: string[] = [];
		const missingIndexes: number[] = [];
		for (let i = 0; i < nativeHas.length; i++) {
			if (this.isNativeDeletePending(cids[i]!)) {
				nativeHas[i] = false;
			} else if (!nativeHas[i]) {
				missing.push(cids[i]!);
				missingIndexes.push(i);
			}
		}
		if (missing.length === 0) {
			return nativeHas;
		}
		const durableHas = await this.durable.hasMany(missing);
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.hasMany(cids);
		}
		for (
			let missingIndex = 0;
			missingIndex < missingIndexes.length;
			missingIndex++
		) {
			nativeHas[missingIndexes[missingIndex]!] = durableHas[missingIndex]!;
		}
		return nativeHas;
	}

	// --- removes (apply to BOTH) ----------------------------------------
	// Native rm is synchronous; the durable rm is awaited so the returned promise
	// resolves only after both succeed and a durable failure rejects here rather
	// than being swallowed. All rm callers (RemoteBlocks.rm, the log) await it.
	async rm(cid: string): Promise<void> {
		this.throwIfNativeDurableCommitFailed();
		const writeGeneration = this.nativeBlockWriteGenerations.get(cid);
		this.beginNativeDelete([cid]);
		try {
			await this.drainDurable();
			this.native.rm(cid);
			await this.commitDurableMutation(() => this.durable.rm(cid), [cid]);
			// A durable read that began before the tombstone may have repopulated
			// native while durable rm was pending. Remove it idempotently again.
			this.native.rm(cid);
			if (
				this.nativeBlockWriteGenerations.get(cid) === writeGeneration &&
				!this.pendingNativeDeleteCleanup.has(cid)
			) {
				this.nativeBlockWriteGenerations.delete(cid);
			}
		} finally {
			this.endNativeDelete([cid]);
		}
	}

	del(cid: string): Promise<void> {
		return this.rm(cid);
	}

	async rmMany(cids: string[]): Promise<number> {
		this.throwIfNativeDurableCommitFailed();
		const writeGenerations = new Map(
			cids.map((cid) => [cid, this.nativeBlockWriteGenerations.get(cid)]),
		);
		this.beginNativeDelete(cids);
		try {
			await this.drainDurable();
			const removed = await this.native.rmMany(cids);
			await this.commitDurableMutation(() => this.durable.rmMany(cids), cids);
			await this.native.rmMany(cids);
			for (const cid of cids) {
				if (
					this.nativeBlockWriteGenerations.get(cid) ===
						writeGenerations.get(cid) &&
					!this.pendingNativeDeleteCleanup.has(cid)
				) {
					this.nativeBlockWriteGenerations.delete(cid);
				}
			}
			return removed;
		} finally {
			this.endNativeDelete(cids);
		}
	}

	// Native trim may already have removed the hot wasm blocks. Queue the durable
	// copy for cleanup, retaining read tombstones until removal succeeds; a
	// cleanup failure is retried and never fed into ordinary append rollback.
	// EntryIndex feature-detects this hook.
	async rmManyAfterNativeDelete(
		cids: string[],
		cleanupToken?: unknown,
	): Promise<void> {
		if (this.nativeDurableCommitFailure) {
			this.cancelNativeDeleteCleanup(cleanupToken);
			throw this.nativeDurableCommitFailure;
		}
		let preannounced = false;
		if (typeof cleanupToken === "number") {
			const staged = this.stagedNativeDeleteCleanups.get(cleanupToken);
			if (staged) {
				preannounced = true;
				this.stagedNativeDeleteCleanups.delete(cleanupToken);
				for (const [cid, generation] of staged) {
					if ((this.nativeBlockWriteGenerations.get(cid) ?? 0) !== generation) {
						this.endNativeDelete([cid]);
						continue;
					}
					if (this.pendingNativeDeleteCleanup.has(cid)) {
						// Another delete already owns a tombstone for this CID.
						this.endNativeDelete([cid]);
					} else {
						// Transfer the staged tombstone to the now-published cleanup.
						this.pendingNativeDeleteCleanup.set(cid, generation);
					}
				}
			}
		}
		if (!preannounced) {
			this.enqueueNativeDeleteCleanup(cids);
		}
		await this.retryNativeDeleteCleanup();
	}

	/** Finish committed trim GC before its durable recovery intent is retired. */
	async completeCommittedNativeDeleteCleanup(
		cids: string[],
		options?: { reconstructMissing?: boolean },
	): Promise<void> {
		this.throwIfNativeDurableCommitFailed();
		const uniqueCids = [...new Set(cids.filter(Boolean))];
		if (uniqueCids.length === 0) {
			return;
		}
		// Only restart recovery reconstructs missing debt. On the live path, an
		// absent row may mean the CID was legitimately re-added after its original
		// generation-owned trim completed or was cancelled; re-enqueueing it here
		// would capture the new generation and delete live content.
		if (options?.reconstructMissing) {
			this.enqueueNativeDeleteCleanup(uniqueCids);
		}
		if (!uniqueCids.some((cid) => this.pendingNativeDeleteCleanup.has(cid))) {
			return;
		}
		await this.retryNativeDeleteCleanup({ throwOnFailure: true });
		const remaining = uniqueCids.filter((cid) =>
			this.pendingNativeDeleteCleanup.has(cid),
		);
		if (remaining.length > 0) {
			throw new Error(
				`Committed native trim cleanup remains incomplete: ${remaining.join(", ")}`,
			);
		}
	}

	// --- misc ------------------------------------------------------------
	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		if (this.nativeDurableCommitFailure) {
			for await (const block of this.durable.iterator()) {
				if (!this.isNativeDeletePending(block[0])) {
					yield block;
				}
			}
			return;
		}
		yield* this.native.iterator();
	}

	async size(): Promise<number> {
		// The hot wasm map is intentionally cold after reopen, so it cannot be the
		// storage-budget authority. Settle synchronous columnar mirrors first, then
		// report durable bytes; pending trim cleanup remains conservatively counted
		// until its durable deletion succeeds.
		await this.drainDurable();
		return this.durable.size();
	}

	persisted(): boolean {
		// The blocks are now mirrored to a durable store, so report persisted so
		// callers that gate durable-only behavior on this flag behave correctly.
		return true;
	}
}

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
	error?: unknown;
};

type EntryLeaderPlan<R extends "u32" | "u64"> = {
	coordinates: NumberFromType<R>[];
	coordinateStrings?: string[];
	leaders: LeaderMap;
	isLeader: boolean;
	assignedToRangeBoundary?: boolean;
};

type ReusableReceiveCoordinatePlan<R extends "u32" | "u64"> = {
	plan: EntryLeaderPlan<R>;
	replicas: number;
	prepared: PreparedCoordinatePersistence<R>;
};

type DecodedReplicaCountMap = ReadonlyMap<string, number>;
type NativeRequestPruneLeaderHints = {
	localLeaderHashes: Set<string>;
	replicaCounts: Map<string, number>;
	replicaCountsByIndex?: ArrayLike<number | undefined>;
	peerHistoryGids: string[];
	peerHistoryRemovedHashes: Set<string>;
	peerHistoryRemovedFlags?: ArrayLike<boolean | number>;
	nativeEntries?: Map<
		string,
		{ gid: string; data?: Uint8Array; replicas?: number }
	>;
	nativeEntryMetadata?: Array<
		{ gid: string; data?: Uint8Array; replicas?: number } | undefined | null
	>;
	nativeEntryGids?: ArrayLike<string | undefined | null>;
	nativeEntryDataByIndex?: ArrayLike<Uint8Array | undefined | null>;
	presentBlockHashes?: Set<string>;
	presentBlocks?: ArrayLike<boolean | number>;
	localLeaderFlags?: ArrayLike<boolean | number>;
	nativeAllConfirmed?: boolean;
	nativeBackbonePeerHistoryCleaned?: boolean;
};

const countTruthyValues = (values?: ArrayLike<boolean | number>) => {
	if (!values) {
		return 0;
	}
	let count = 0;
	for (let i = 0; i < values.length; i++) {
		if (values[i]) {
			count += 1;
		}
	}
	return count;
};

const countPresentValues = (values?: ArrayLike<unknown>) => {
	if (!values) {
		return 0;
	}
	let count = 0;
	for (let i = 0; i < values.length; i++) {
		if (values[i] != null) {
			count += 1;
		}
	}
	return count;
};

const countPositiveValues = (values?: ArrayLike<number | undefined>) => {
	if (!values) {
		return 0;
	}
	let count = 0;
	for (let i = 0; i < values.length; i++) {
		if ((values[i] ?? 0) > 0) {
			count += 1;
		}
	}
	return count;
};

const canConfirmNativeRequestPruneBatch = (
	hints: NativeRequestPruneLeaderHints,
	hashCount: number,
) => {
	if (hashCount > 0 && hints.nativeAllConfirmed === true) {
		return true;
	}
	if (
		hashCount === 0 ||
		!hints.nativeEntryGids ||
		!hints.presentBlocks ||
		!hints.localLeaderFlags ||
		!hints.replicaCountsByIndex ||
		!hints.peerHistoryRemovedFlags ||
		hints.nativeEntryGids.length < hashCount ||
		hints.presentBlocks.length < hashCount ||
		hints.localLeaderFlags.length < hashCount ||
		hints.replicaCountsByIndex.length < hashCount ||
		hints.peerHistoryRemovedFlags.length < hashCount
	) {
		return false;
	}

	for (let i = 0; i < hashCount; i++) {
		if (
			hints.nativeEntryGids[i] == null ||
			!hints.presentBlocks[i] ||
			!hints.localLeaderFlags[i] ||
			(hints.replicaCountsByIndex[i] ?? 0) <= 0 ||
			!hints.peerHistoryRemovedFlags[i]
		) {
			return false;
		}
	}

	return true;
};

type SharedLogCoordinateNativeFields<R extends "u32" | "u64"> = {
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

type PreparedCoordinatePersistence<R extends "u32" | "u64"> = {
	coordinateEntry?: EntryReplicated<R>;
	assignedToRangeBoundary: boolean;
	fields: SharedLogCoordinateNativeFields<R>;
};

type ResidentCoordinateEntry<R extends "u32" | "u64"> =
	| EntryReplicated<R>
	| SharedLogCoordinateNativeFields<R>;

type CoordinatePersistBatchItem<R extends "u32" | "u64"> = {
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

type NativeBackboneReceiveCoordinateRow<R extends "u32" | "u64"> = {
	item: CoordinatePersistBatchItem<R>;
	prepared: PreparedCoordinatePersistence<R>;
	fields: SharedLogCoordinateNativeFields<R>;
	deleteHashes: string[];
};

type NativeBackboneReceiveCoordinateBatch<R extends "u32" | "u64"> = {
	rows: NativeBackboneReceiveCoordinateRow<R>[];
	rollbackCoordinateEntries?: NativeBackboneCoordinateRollback<R>;
};

type NativeBackboneCoordinateRollback<R extends "u32" | "u64"> = {
	hashes: Set<string>;
	entries: Map<string, ResidentCoordinateEntry<R>>;
	generations: Map<string, number>;
};

type RepairDispatchEntry<R extends "u32" | "u64"> = ResidentCoordinateEntry<R>;

type PreparedLocalAppendCommit<R extends "u32" | "u64"> = {
	hash: string;
	gid: string;
	next: string[];
	wallTime: bigint;
	logical: number;
	payloadSize: number;
	metaBytes?: Uint8Array;
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

type NativeBackboneCoordinatePersistenceStore = {
	read(name: string): Promise<Uint8Array | undefined>;
	write(name: string, bytes: Uint8Array): Promise<void>;
	append(name: string, bytes: Uint8Array): Promise<void>;
	remove?(name: string): Promise<void>;
	durableBarrier?(name?: string): Promise<void>;
	supportsRemoval?: boolean;
	flush?(name?: string): Promise<void>;
	close?(options?: { flush?: boolean }): Promise<void>;
};

type NativeBackboneCoordinatePersistenceAdapter = {
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

type PreparedPayloadCommitOnlyProperties =
	NativeBackboneDocumentCommitOptions & {
		skipMissingNextJoin?: boolean;
		resolveTrimmedEntries?: boolean;
	};

type PreparedPayloadsManyIndependentProperties<T> = {
	resolveTrimmedEntries?: boolean;
	payloadDatas?: Uint8Array[];
	nexts?: ShallowOrFullEntry<T>[][];
	nativeBackboneDocumentIndexes?: NativeBackboneDocumentIndexCommitInput[];
	retainMaterializationBytes?: boolean;
};

type PreparedPayloadCommitOnlyResult<T, R extends "u32" | "u64"> = {
	entry: Entry<T>;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
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

/**
 * Replication announcements are best-effort convergence messages. A detached
 * fanout shard can time out even though the shared log itself remains open.
 * Keep retries deliberately limited to concrete TimeoutErrors: abort/close and
 * unexpected programming/data errors must retain their existing semantics.
 *
 * Exact constructor/name checks complement `instanceof` for errors crossing
 * worker or duplicate-package boundaries in browsers.
 */
const isTransientReplicationAnnouncementError = (
	error: unknown,
	seen = new Set<unknown>(),
): boolean => {
	if (
		error != null &&
		(typeof error === "object" || typeof error === "function")
	) {
		if (seen.has(error)) {
			return false;
		}
		seen.add(error);
	}

	if (error instanceof TimeoutError) {
		return true;
	}

	const nested = (error as { errors?: unknown })?.errors;
	if (Array.isArray(nested) && nested.length > 0) {
		return nested.every((item) =>
			isTransientReplicationAnnouncementError(item, new Set(seen)),
		);
	}

	const cause = (error as { cause?: unknown })?.cause;
	if (cause != null && isTransientReplicationAnnouncementError(cause, seen)) {
		return true;
	}

	const constructorName =
		typeof (error as { constructor?: { name?: unknown } })?.constructor
			?.name === "string"
			? (error as { constructor: { name: string } }).constructor.name
			: "";
	const name =
		typeof (error as { name?: unknown })?.name === "string"
			? (error as { name: string }).name
			: "";
	return constructorName === "TimeoutError" || name === "TimeoutError";
};

/**
 * Directed transport-delivery repair is allowed to retry explicit delivery
 * failures in addition to timeouts. A DirectStream ACK confirms receipt of the
 * signed envelope, not successful application by the receiver. Keep this
 * separate from the primary fanout classifier above so replicate() rejection
 * semantics remain unchanged for programming, serialization, and lifecycle
 * errors.
 */
const isTransientReplicationAnnouncementRepairError = (
	error: unknown,
	seen = new Set<unknown>(),
): boolean => {
	if (
		error != null &&
		(typeof error === "object" || typeof error === "function")
	) {
		if (seen.has(error)) {
			return false;
		}
		seen.add(error);
	}

	if (error instanceof DeliveryError || error instanceof TimeoutError) {
		return true;
	}

	const nested = (error as { errors?: unknown })?.errors;
	if (Array.isArray(nested) && nested.length > 0) {
		return nested.every((item) =>
			isTransientReplicationAnnouncementRepairError(item, new Set(seen)),
		);
	}

	const cause = (error as { cause?: unknown })?.cause;
	if (
		cause != null &&
		isTransientReplicationAnnouncementRepairError(cause, seen)
	) {
		return true;
	}

	const constructorName =
		typeof (error as { constructor?: { name?: unknown } })?.constructor
			?.name === "string"
			? (error as { constructor: { name: string } }).constructor.name
			: "";
	const name =
		typeof (error as { name?: unknown })?.name === "string"
			? (error as { name: string }).name
			: "";
	return (
		constructorName === "DeliveryError" ||
		name === "DeliveryError" ||
		constructorName === "TimeoutError" ||
		name === "TimeoutError"
	);
};

interface IndexableDomain<R extends "u32" | "u64"> {
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

type EntryWithMetaBytes = {
	getMetaBytes?: () => Uint8Array | undefined;
	getHashDigestBytes?: () => Uint8Array | undefined;
};

const EMPTY_HASHES: string[] = [];

const normalizedHashValues = (hashes: Iterable<string>): string[] => {
	if (Array.isArray(hashes)) {
		if (hashes.length === 0) {
			return EMPTY_HASHES;
		}
		if (hashes.length === 1) {
			return hashes[0] ? hashes : EMPTY_HASHES;
		}
	}
	const values: string[] = [];
	const seen = new Set<string>();
	for (const hash of hashes) {
		if (!hash || seen.has(hash)) {
			continue;
		}
		seen.add(hash);
		values.push(hash);
	}
	return values;
};

const combineCoordinateDeleteHashes = (
	nextHashes: string[],
	deleteHashes?: string[],
): string[] => {
	if (!deleteHashes || deleteHashes.length === 0) {
		return nextHashes;
	}
	if (nextHashes.length === 0) {
		return deleteHashes;
	}
	const combined: string[] = [];
	const seen = new Set<string>();
	for (const hash of nextHashes) {
		if (!seen.has(hash)) {
			seen.add(hash);
			combined.push(hash);
		}
	}
	for (const hash of deleteHashes) {
		if (!seen.has(hash)) {
			seen.add(hash);
			combined.push(hash);
		}
	}
	return combined;
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
	compatibility?: number;
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

// DONT SET THIS ANY LOWER, because it will make the pid controller unstable as the system responses are not fast enough to updates from the pid controller
const RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL = 1000;
const REPLICATION_ANNOUNCEMENT_RETRY_INTERVAL = 1000;
const REPLICATION_ANNOUNCEMENT_REPAIR_INTERVAL = 1000;
const REPLICATION_ANNOUNCEMENT_REPAIR_MAX_ATTEMPTS = 3;
// Repair one bounded cohort per mutation generation. The subscriber snapshot
// is a best-effort cache and can contain thousands of entries, so attempting
// the whole cache after every role mutation would turn convergence repair into
// an unbounded burst of separately signed, acknowledged messages. A cursor
// retained across generations rotates best-effort coverage over later changes.
const REPLICATION_ANNOUNCEMENT_REPAIR_TARGETS_PER_GENERATION = 8;
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
const LEADER_SELECTION_CONTEXT_CACHE_TTL_MS = 50;
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

type ReplicationAnnouncementRepairTarget = {
	key: PublicSignKey;
	generation: number;
	attempts: number;
	done: boolean;
};
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
// Preserve the bounded retry window for transient local misses, but serialize
// delayed warmup sends per target so fixed snapshots cannot overlap and amplify
// large transfers. Every queued pass re-checks current peer knowledge on entry.
const JOIN_WARMUP_RETRY_SCHEDULE_MS = [
	0, 1_000, 3_000, 7_000, 15_000, 30_000, 60_000,
];
const JOIN_WARMUP_SEND_SPACING_MS = 250;
const JOIN_AUTHORITATIVE_RETRY_SCHEDULE_MS = [
	0, 1_000, 3_000, 7_000, 15_000, 30_000, 60_000,
];
const APPEND_BACKFILL_RETRY_SCHEDULE_MS = [0, 1_000, 3_000, 7_000];
const RECENT_KNOWN_REPAIR_SUPPRESSION_MS = 30_000;
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

type JoinWarmupSendState<R extends "u32" | "u64"> = {
	bypassKnownPeerHints: boolean;
	entries: Map<string, RepairDispatchEntry<R>>;
	generation: object;
	lastCompletedAt: number;
	pending: boolean;
	running: boolean;
};

type JoinWarmupRetryTimer = {
	handle: ReturnType<typeof setTimeout>;
	resolve?: () => void;
};

type JoinWarmupScheduledRetryBatch<R extends "u32" | "u64"> = {
	bypassKnownPeerHints: boolean;
	entries: Map<string, RepairDispatchEntry<R>>;
	remainingAttempts: number;
};

type JoinWarmupScheduledRetryCohort<R extends "u32" | "u64"> = {
	batches: JoinWarmupScheduledRetryBatch<R>[];
	dueAt: number;
};

type JoinWarmupScheduledRetrySlot<R extends "u32" | "u64"> = {
	cohorts: JoinWarmupScheduledRetryCohort<R>[];
	head: number;
	timer?: JoinWarmupRetryTimer;
	timerDueAt?: number;
};

type JoinWarmupScheduledRetries<R extends "u32" | "u64"> = {
	generation: object;
	slotsByDelay: Map<number, JoinWarmupScheduledRetrySlot<R>>;
};

type RepairSweepOptimisticPeerState = {
	count: number;
	generation: object;
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
	new Map<RepairDispatchMode, Set<string>>(
		REPAIR_DISPATCH_MODES.map((mode) => [mode, new Set()]),
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

export type DeliveryReliability = "ack" | "best-effort";

export type DeliveryOptions = {
	reliability?: DeliveryReliability;
	minAcks?: number;
	requireRecipients?: boolean;
	/**
	 * Transport priority for directed RPC delivery. Fanout unicast already uses
	 * its control lane, so this only changes the direct/fallback RPC path.
	 */
	priority?: number;
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

type TrustedLogAppendOptions<T> = AppendOptions<T> & {
	__peerbitCanAppendAlreadyValidated?: boolean;
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
	change: Change<T>;
	appendFacts: PreparedAppendFacts;
};

type TrustedLowerLogCommitOnlyAppendResult<T> = {
	entry: Entry<T>;
	materializeEntry: () => Entry<T>;
	shallowEntry: ShallowEntry;
	removed: ShallowOrFullEntry<T>[];
	removedHashes?: string[];
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

	private _replicationRangeIndex!: Index<ReplicationRangeIndexable<R>>;
	private _entryCoordinatesIndex!: Index<EntryReplicated<R>>;
	private _nativeRangePlanner?: SharedLogRangePlanner;
	private _nativeSharedLogState?: SharedLogNativeState;
	private _nativeBackbone?: NativePeerbitBackbone;
	private _nativeDurableCommitFailure?: NativeDurableCommitError;
	private _nativeDurableRecoveryReadyForReopen = false;
	private _nativeDurableRecoveryCids = new Set<string>();
	private _wireSyncSession?: SharedLogNativeWireSync;
	private _nativeBackboneCoordinatePersistence?: NativeBackboneCoordinatePersistenceAdapter;
	private _nativeBackboneCoordinatePersistenceStore?: NativeBackboneCoordinatePersistenceStore;
	private _nativeBackboneDropStarted = false;
	private _nativeBackboneCoordinateJournalLastFlushMs = 0;
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
	private _residentEntryCoordinatesByHash?: Map<
		string,
		ResidentCoordinateEntry<R>
	>;
	private _nativeCoordinateMutationGenerations?: Map<string, number>;
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
	private _subscriptionChangeCallbacks?: Set<Promise<void>>;
	private _acceptSubscriptionChangeCallbacks = false;
	private _replicationLifecycleController?: AbortController;
	private _activeReceiveHandlersByPeer!: Map<string, PeerReceiveLeaseState>;
	private _receiveHandlerDrainByPeer!: Map<string, Set<Promise<void>>>;
	private _receiveCleanupGateByPeer!: Map<string, number>;
	private _subscriptionOpeningEpochByPeer!: Map<string, object>;
	private _openingSyncCapabilitiesByPeer!: Map<
		string,
		{ epoch: object; capabilities: number }
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
	private _closeController!: AbortController;
	private _respondToIHaveTimeout!: any;
	private _checkedPrune!: CheckedPruneCoordinator<T, R>;
	private _admittedPruneRemoves!: Set<Promise<unknown>>;
	private _pruneRemovesClosing = false;
	private _pendingIHaveCallbacks!: Set<Promise<void>>;
	private _pendingIHaveExpiryTimer?: ReturnType<typeof setTimeout>;
	private _pendingIHaveExpiryDeadline = Number.POSITIVE_INFINITY;

	private get _pendingDeletes() {
		return this._checkedPrune.pendingDeletes;
	}

	private _pendingIHave!: Map<string, PendingIHave<T>>;

	// public key hash to range id to range
	pendingMaturity!: Map<string, Map<string, PendingMaturityRecord<R>>>; // map of peerId to timeout

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
	// Range ids are global primary keys while receive lanes are per peer. Keep
	// reads and writes that decide one mutation in a single global lane.
	private _replicationRangeMutationTail: Promise<void> = Promise.resolve();
	private _replicationRangeMutationsClosing = false;
	// Log.remove awaits program onChange callbacks before its physical delete.
	// Track when checked prune holds the ownership lane across that lower-log
	// removal so the callback wrapper can identify its direct invocation.
	private _checkedPruneRemoveBlocksLocalRangeMutationAdmission = 0;
	// Reject public local role/terminal operations invoked directly by that
	// program callback rather than letting it await the lane that is awaiting the
	// callback. Remote/internal mutations and unrelated external callers remain
	// queued/drained normally.
	private _checkedPruneRemovalCallbackInvocationDepth = 0;
	// If durable post-state cannot be reconciled to every native/runtime mirror,
	// reject later writers and planners until reopen rehydrates those mirrors.
	private _replicationRangeMutationFailure?: unknown;
	// Background repair work can outlive the await that admitted it. Replace this
	// opaque token on poison and every terminal/open boundary so an older runner
	// can neither dispatch nor mutate a freshly opened lifecycle.
	private _repairLifecycleController = new AbortController();
	// Local receive generations fence replication-info handlers that were admitted
	// before a liveness eviction but reach the per-peer apply lane after it. Unlike
	// message timestamps, these tokens never compare clocks from different peers.
	private _replicationInfoReceiveEpochByPeer!: Map<string, object>;
	// Subscription callbacks can overlap because removing a replicator mutates the
	// replication index asynchronously. Keep that lifecycle separate from message
	// timestamps so a reconnect can synchronously revoke an older unsubscribe.
	private _subscriptionEpochByPeer!: Map<string, object>;
	// A superseded removal may be the queue item that actually observed an active
	// replicator. Carry that leave obligation to the transition that ultimately
	// wins, while a winning reconnect clears it without emitting a stale leave.
	private _pendingReplicatorLeaveByPeer!: Set<string>;
	private _replicatorLivenessSweepRunning!: boolean;
	private _replicatorLivenessTimer?: ReturnType<typeof setInterval>;
	private _replicatorLivenessTargets!: string[];
	private _replicatorLivenessTargetsSize!: number;
	private _replicatorLivenessCursor!: number;
	private _replicatorLivenessFailures!: Map<string, number>;
	private _replicatorLastActivityAt!: Map<string, number>;

	private remoteBlocks!: RemoteBlocks;

	private throwIfReplicationOwnershipPoisoned(): void {
		if (this._replicationRangeMutationFailure !== undefined) {
			throw new Error(
				"Replication ownership recovery is required before further planning",
				{ cause: this._replicationRangeMutationFailure },
			);
		}
	}

	private startRepairLifecycle(): AbortController {
		this._repairLifecycleController?.abort();
		this._repairLifecycleController = new AbortController();
		return this._repairLifecycleController;
	}

	private stopRepairLifecycle(): void {
		this._repairLifecycleController?.abort();
	}

	private isRepairLifecycleActive(controller: AbortController): boolean {
		return (
			controller === this._repairLifecycleController &&
			!controller.signal.aborted &&
			this._replicationRangeMutationFailure === undefined &&
			!this.closed
		);
	}

	private captureReplicationOwnershipLifecycle(): AbortController {
		const controller = this._repairLifecycleController;
		this.throwIfReplicationOwnershipLifecycleInactive(controller);
		return controller;
	}

	private throwIfReplicationOwnershipLifecycleInactive(
		controller: AbortController,
	): void {
		this.throwIfReplicationOwnershipPoisoned();
		if (!this.isRepairLifecycleActive(controller)) {
			throw new TerminalOperationNotStartedError(
				"Replication ownership lifecycle is no longer active",
			);
		}
	}

	private poisonReplicationOwnership(failure: unknown): unknown {
		this._replicationRangeMutationFailure ??= failure;
		this.stopRepairLifecycle();
		// Pending aggregate changes belong to the poisoned ownership generation.
		// Closing also resolves ignored `add()` promises, while the guarded
		// callback below observes any already-running rejection.
		this.replicationChangeDebounceFn?.close?.();
		this.pruneDebouncedFn?.close?.();
		this.rebalanceParticipationDebounced?.close();
		for (const hash of this._checkedPrune?.retries.keys() ?? []) {
			this._checkedPrune.clearRetry(hash);
		}
		this.cancelCurrentReplicationStateAnnouncementRetry();
		this.cancelAllJoinWarmupTargets();
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
		this._repairSweepJoinWarmupGenerationByTarget.clear();
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
						this.materializeResidentCoordinateEntry(previous);
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
		await this.deleteCoordinatesForHashes(hashes);
		const flushed = this.flushNativeBackboneCoordinateJournal();
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
					(this._nativeCoordinateMutationGenerations ??= new Map());
				const rollback: NativeBackboneCoordinateRollback<R> = {
					hashes: new Set(),
					entries: new Map(),
					generations: new Map(),
				};
				for (const coordinate of intent.coordinates) {
					rollback.hashes.add(coordinate.hash);
					const generation =
						(mutationGenerations.get(coordinate.hash) ?? 0) + 1;
					mutationGenerations.set(coordinate.hash, generation);
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
				await this.rollbackNativeBackboneCoordinateAppendDurably("", rollback);
			}
			for (const document of intent.documents) {
				this.restoreNativeBackboneDocument({
					key: document.key,
					value: document.value ? Uint8Array.from(document.value) : undefined,
					byteElementIndexLimit: document.byteElementIndexLimit,
				});
			}
			const flushed = this.flushNativeBackboneCoordinateJournal();
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
			this.rollbackNativeBackboneCoordinateAppend(
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
		const flushed = this.flushNativeBackboneCoordinateJournal();
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
	private replicationAnnouncementRetryDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;
	private _replicationAnnouncementRetryPending!: boolean;
	private _replicationAnnouncementRetryGeneration!: number;
	private _replicationAnnouncementRetryController!: AbortController;
	private replicationAnnouncementRepairDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;
	private _replicationAnnouncementRepairPending!: boolean;
	private _replicationAnnouncementRepairGeneration!: number;
	private _replicationAnnouncementRepairGenerationController!: AbortController;
	private _replicationAnnouncementRepairTargets!: Map<
		string,
		ReplicationAnnouncementRepairTarget
	>;
	private _replicationAnnouncementRepairCohortSelected!: boolean;
	private _replicationAnnouncementRepairFairCursorHash!: string | undefined;
	private _replicationAnnouncementRepairMaxAttempts!: number;
	private _replicationAnnouncementRepairController!: AbortController;

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

	private get _requestIPruneSent() {
		return this._checkedPrune.requestIPruneSent;
	}
	private get _requestIPruneResponseReplicatorSet() {
		return this._checkedPrune.responseReplicatorSet;
	}
	private get _checkedPruneRetries() {
		return this._checkedPrune.retries;
	}

	private replicationChangeDebounceFn!: ReturnType<
		typeof debounceAggregationChanges<ReplicationRangeIndexable<R>>
	>;
	private _repairRetryTimers!: Set<ReturnType<typeof setTimeout>>;
	private _recentRepairDispatch!: Map<string, Map<string, number>>;
	private _repairSweepRunning!: boolean;
	private _repairSweepPendingModes!: Set<RepairDispatchMode>;
	private _repairSweepPendingPeersByMode!: Map<RepairDispatchMode, Set<string>>;
	private _repairSweepJoinWarmupGenerationByTarget!: Map<string, object>;
	private _repairFrontierByMode!: Map<
		RepairDispatchMode,
		Map<string, Map<string, RepairDispatchEntry<R>>>
	>;
	private _repairFrontierActiveTargetsByMode!: Map<
		RepairDispatchMode,
		Set<string>
	>;
	private _repairFrontierBypassKnownPeersByMode!: Map<
		RepairDispatchMode,
		Set<string>
	>;
	private _joinWarmupGenerationByTarget!: Map<string, object>;
	private _joinWarmupSendStateByTarget!: Map<
		string,
		JoinWarmupSendState<R>
	>;
	private _joinWarmupRetryTimersByTarget!: Map<
		string,
		Set<JoinWarmupRetryTimer>
	>;
	private _joinWarmupScheduledRetriesByTarget!: Map<
		string,
		JoinWarmupScheduledRetries<R>
	>;
	private _repairSweepOptimisticGidPeersPending!: Map<
		string,
		Map<string, RepairSweepOptimisticPeerState>
	>;
	private _repairSweepOptimisticGidsByPeer!: Map<string, Set<string>>;
	private _entryKnownPeers!: Map<string, Set<string>>;
	private _entryKnownPeerObservedAt!: Map<string, Map<string, number>>;
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
	private _leaderSelectionContextCache?: {
		expiresAt: number;
		context: LeaderSelectionContext;
	};
	// Sync capability bits advertised by peers (SyncCapabilitiesMessage), keyed
	// by public key hash. Entries are dropped on unsubscribe/disconnect.
	private _peerSyncCapabilities!: Map<string, number>;
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

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.ensureNativeDurabilityRuntimeState();
		this.log = new Log(properties);
		this.rpc = new RPC();
		this._checkedPrune = new CheckedPruneCoordinator<T, R>();
		this._admittedPruneRemoves = new Set();
		this._pendingIHave = new Map();
		this._pendingIHaveCallbacks = new Set();
		this.latestReplicationInfoMessage = new Map();
		this._replicationInfoBlockedPeers = new Set();
		this._replicationInfoRequestByPeer = new Map();
		this._replicationInfoApplyQueueByPeer = new Map();
		this._replicationInfoReceiveEpochByPeer = new Map();
		this._subscriptionEpochByPeer = new Map();
		this._pendingReplicatorLeaveByPeer = new Set();
		this._activeReceiveHandlersByPeer = new Map();
		this._receiveHandlerDrainByPeer = new Map();
		this._receiveCleanupGateByPeer = new Map();
		this._subscriptionOpeningEpochByPeer = new Map();
		this._openingSyncCapabilitiesByPeer = new Map();
		this._gidPeersHistory = new Map();
		this._repairRetryTimers = new Set();
		this._recentRepairDispatch = new Map();
		this._repairSweepRunning = false;
		this._repairSweepPendingModes = new Set();
		this._repairSweepPendingPeersByMode = createRepairPendingPeersByMode();
		this._repairSweepJoinWarmupGenerationByTarget = new Map();
		this._repairFrontierByMode = createRepairFrontierByMode() as Map<
			RepairDispatchMode,
			Map<string, Map<string, RepairDispatchEntry<R>>>
		>;
		this._repairFrontierActiveTargetsByMode = createRepairActiveTargetsByMode();
		this._repairFrontierBypassKnownPeersByMode =
			createRepairFrontierBypassKnownPeersByMode();
		this._joinWarmupGenerationByTarget = new Map();
		this._joinWarmupSendStateByTarget = new Map();
		this._joinWarmupRetryTimersByTarget = new Map();
		this._joinWarmupScheduledRetriesByTarget = new Map();
		this._repairSweepOptimisticGidPeersPending = new Map();
		this._repairSweepOptimisticGidsByPeer = new Map();
		this._entryKnownPeers = new Map();
		this._joinAuthoritativeRepairTimersByDelay = new Map();
		this._joinAuthoritativeRepairPeersByDelay = new Map();
		this._appendBackfillPendingByTarget = new Map();
		this._topicSubscribersCache = new Map();
		this._peerSyncCapabilities = new Map();
		this._liveRawGossipBatches = new Map();
		this._liveRawGossipFlushScheduled = false;
		this.coordinateToHash = new Cache<string>({ max: 1e6, ttl: 1e4 });
		this.recentlyRebalanced = new Cache<string>({ max: 1e4, ttl: 1e5 });
		this.uniqueReplicators = new Set();
		this._replicatorJoinEmitted = new Set();
		this._replicatorsReconciled = false;
		this._replicatorLivenessSweepRunning = false;
		this._replicatorLivenessTargets = [];
		this._replicatorLivenessTargetsSize = 0;
		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures = new Map();
		this._replicatorLastActivityAt = new Map();
		this._replicationAnnouncementRetryPending = false;
		this._replicationAnnouncementRetryGeneration = 0;
		this._replicationAnnouncementRetryController = new AbortController();
		this._replicationAnnouncementRepairPending = false;
		this._replicationAnnouncementRepairGeneration = 0;
		this._replicationAnnouncementRepairGenerationController =
			new AbortController();
		this._replicationAnnouncementRepairTargets = new Map();
		this._replicationAnnouncementRepairCohortSelected = false;
		this._replicationAnnouncementRepairFairCursorHash = undefined;
		this._replicationAnnouncementRepairMaxAttempts =
			REPLICATION_ANNOUNCEMENT_REPAIR_MAX_ATTEMPTS;
		this._replicationAnnouncementRepairController = new AbortController();
		this.pendingMaturity = new Map();
		this._closeController = new AbortController();
	}

	private ensureNativeDurabilityRuntimeState(): void {
		// Program clones are borsh-created without running class field initializers.
		// Keep recovery state from an existing generation, while supplying fresh
		// defaults only when the runtime-only fields are absent.
		this._nativeDurableRecoveryReadyForReopen ??= false;
		this._nativeDurableRecoveryCids ??= new Set();
		this._nativeBackboneDropStarted ??= false;
		this._nativeBackboneCoordinateJournalLastFlushMs ??= 0;
		this._nativeStrictDurableDocumentRecoveryDeferred ??= false;
		this._nativeStrictDurableTransactionsClosing ??= false;
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
					mode: SilentDelivery;
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
						mode: new SilentDelivery({ redundancy: 1, to: [...to] }),
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
				this.syncronizer instanceof SimpleSyncronizer ||
				(this.compatibility ?? Number.MAX_VALUE) < 10;
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
						this.queueAppendBackfill(peer, entryReplicatedForRepair);
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
					this.queueAppendBackfill(peer, entryReplicatedForRepair);
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
				this.queueAppendBackfill(peer, entryReplicatedForRepair);
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
				let coordinates = await this.getCoordinates(gidEntry);
				throwIfInactive();
				let found: Map<string, { intersecting: boolean }>;
				if (coordinates == null) {
					found = await this.findLeadersFromEntry(
						gidEntry,
						minReplicasValue,
						undefined,
						ownershipLifecycleController,
					);
				} else {
					found = await this._findLeaders(
						coordinates,
						undefined,
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
		}
		this.invalidateLeaderSelectionContextCache();
	}

	private invalidateSharedLogTopicSubscribersCache() {
		this.invalidateTopicSubscribersCache(this.topic, this.rpc.topic);
	}

	private invalidateLeaderSelectionContextCache() {
		this._leaderSelectionContextCache = undefined;
	}

	private canCacheLeaderSelectionContext(options?: {
		roleAge?: number;
		candidates?: Iterable<string>;
	}) {
		return options?.roleAge == null && options?.candidates == null;
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

	// @deprecated
	private getRoleFromReplicationSegments(
		segments: ReplicationRangeIndexable<R>[],
	) {
		if (segments.length > 1) {
			throw new Error(
				"More than one replication segment found. Can only use one segment for compatbility with v8",
			);
		}

		if (segments.length > 0) {
			const segment = segments[0].toReplicationRange();
			return new Replicator({
				factor: (segment.factor as number) / MAX_U32,
				offset: (segment.offset as number) / MAX_U32,
			});
		}

		// TODO this is not accurate but might be good enough
		return new Observer();
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
			controller === this._replicationLifecycleController &&
			!controller.signal.aborted &&
			!this.isTerminating()
		);
	}

	private resetSubscriptionChangeCallbackTracking() {
		this._subscriptionChangeCallbacks = new Set();
		this._acceptSubscriptionChangeCallbacks = true;
		this._replicationLifecycleController = new AbortController();
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
		if (!this._replicationLifecycleController?.signal.aborted) {
			this._replicationLifecycleController?.abort(
				new AbortError("SharedLog is terminating"),
			);
		}
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

	private isPeerReceiveAdmissionOpen(
		peerHash: string,
		replicationLifecycleController: AbortController | undefined,
		subscriptionEpoch: object | null,
		options?: {
			allowReplicationInfoBlocked?: boolean;
			allowCleanupGate?: boolean;
		},
	) {
		return (
			this.isReplicationLifecycleActive(replicationLifecycleController) &&
			this.isCurrentSubscriptionEpoch(peerHash, subscriptionEpoch) &&
			(options?.allowReplicationInfoBlocked === true ||
				!this._replicationInfoBlockedPeers.has(peerHash)) &&
			(options?.allowCleanupGate === true ||
				(this._receiveCleanupGateByPeer.get(peerHash) ?? 0) === 0)
		);
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
			!this.isPeerReceiveAdmissionOpen(
				peerHash,
				replicationLifecycleController,
				subscriptionEpoch,
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
			this._replicationLifecycleController;
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

	private blockPeerReceiveAdmission(peerHash: string) {
		this._receiveCleanupGateByPeer.set(
			peerHash,
			(this._receiveCleanupGateByPeer.get(peerHash) ?? 0) + 1,
		);
	}

	private unblockPeerReceiveAdmission(peerHash: string) {
		const remaining = (this._receiveCleanupGateByPeer.get(peerHash) ?? 1) - 1;
		if (remaining > 0) {
			this._receiveCleanupGateByPeer.set(peerHash, remaining);
		} else {
			this._receiveCleanupGateByPeer.delete(peerHash);
		}
	}

	private handleReplicationLifecycleSendError(
		error: unknown,
		controller = this._replicationLifecycleController,
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

	private queueCurrentReplicationStateAnnouncementRetry(
		error: unknown,
	): boolean {
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			this._replicationAnnouncementRetryController.signal.aborted ||
			!isTransientReplicationAnnouncementError(error)
		) {
			return false;
		}

		this._replicationAnnouncementRetryPending = true;
		void this.replicationAnnouncementRetryDebounced?.call();
		return true;
	}

	private onRebalanceParticipationError(error: Error): void {
		if (
			this.closed ||
			isNotStartedError(error) ||
			(isTransientReplicationAnnouncementError(error) &&
				this._replicationAnnouncementRetryPending)
		) {
			return;
		}

		// Debounced invocations run from an un-awaited timer. Throwing here would
		// create an unhandled rejection (and a browser pageerror), so surface
		// unexpected failures through the logger instead.
		logger.error(error);
	}

	private setupReplicationAnnouncementRetryFunction(
		interval = REPLICATION_ANNOUNCEMENT_RETRY_INTERVAL,
	): void {
		this.replicationAnnouncementRetryDebounced?.close();
		this._replicationAnnouncementRetryController?.abort();
		this._replicationAnnouncementRetryController = new AbortController();
		this.replicationAnnouncementRetryDebounced = debounceFixedInterval(
			() => this.retryCurrentReplicationStateAnnouncement(),
			interval,
			{
				leading: false,
				onError: (error) => {
					if (
						this.closed ||
						this._closeController.signal.aborted ||
						isNotStartedError(error)
					) {
						return;
					}
					logger.error(error);
				},
			},
		);
	}

	private setupReplicationAnnouncementRepairFunction(
		interval = REPLICATION_ANNOUNCEMENT_REPAIR_INTERVAL,
		maxAttempts = REPLICATION_ANNOUNCEMENT_REPAIR_MAX_ATTEMPTS,
	): void {
		if (!Number.isSafeInteger(maxAttempts) || maxAttempts <= 0) {
			throw new RangeError(
				"Replication announcement repair attempts must be positive",
			);
		}
		this.replicationAnnouncementRepairDebounced?.close();
		this._replicationAnnouncementRepairController?.abort();
		this._replicationAnnouncementRepairGenerationController?.abort();
		this._replicationAnnouncementRepairController = new AbortController();
		this._replicationAnnouncementRepairGenerationController =
			new AbortController();
		this._replicationAnnouncementRepairPending = false;
		this._replicationAnnouncementRepairGeneration =
			this._replicationAnnouncementRetryGeneration;
		this._replicationAnnouncementRepairTargets = new Map();
		this._replicationAnnouncementRepairCohortSelected = false;
		this._replicationAnnouncementRepairFairCursorHash = undefined;
		this._replicationAnnouncementRepairMaxAttempts = maxAttempts;
		this.replicationAnnouncementRepairDebounced = debounceFixedInterval(
			() => this.runCurrentReplicationStateAnnouncementRepair(),
			interval,
			{
				leading: false,
				// The wrapper catches worker failures while it still owns the generation
				// context. Keep this boundary visibility-only: it must never mutate a
				// possibly newer generation's pending state.
				onError: (error) => logger.error(error),
			},
		);
	}

	private cancelCurrentReplicationStateAnnouncementRepair(): void {
		this._replicationAnnouncementRepairPending = false;
		this._replicationAnnouncementRepairController?.abort();
		this._replicationAnnouncementRepairGenerationController?.abort();
		this.replicationAnnouncementRepairDebounced?.close();
		this._replicationAnnouncementRepairTargets?.clear();
	}

	private advanceCurrentReplicationStateAnnouncementRepairGeneration(): void {
		const generation = this._replicationAnnouncementRetryGeneration;
		if (generation === this._replicationAnnouncementRepairGeneration) {
			return;
		}

		// Abort acknowledged sends carrying the old full-state snapshot before the
		// primary announcement for the new mutation waits on transport. Otherwise a
		// stale batch can hold the current state behind DirectStream's seek timeout.
		this._replicationAnnouncementRepairGenerationController?.abort();
		this._replicationAnnouncementRepairGenerationController =
			new AbortController();
		this._replicationAnnouncementRepairGeneration = generation;
		this._replicationAnnouncementRepairPending = false;
		this._replicationAnnouncementRepairTargets.clear();
		this._replicationAnnouncementRepairCohortSelected = false;
	}

	private queueCurrentReplicationStateAnnouncementRepair(): void {
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			this._replicationAnnouncementRepairController.signal.aborted ||
			!this.replicationAnnouncementRepairDebounced
		) {
			return;
		}

		this.advanceCurrentReplicationStateAnnouncementRepairGeneration();
		this._replicationAnnouncementRepairPending = true;
		void this.replicationAnnouncementRepairDebounced.call();
	}

	private async runCurrentReplicationStateAnnouncementRepair(): Promise<void> {
		const generation = this._replicationAnnouncementRetryGeneration;
		const lifecycleController = this._replicationAnnouncementRepairController;
		const generationController =
			this._replicationAnnouncementRepairGenerationController;
		try {
			await this.repairCurrentReplicationStateAnnouncement({
				generation,
				lifecycleController,
				generationController,
			});
		} catch (error) {
			if (
				this.closed ||
				this._closeController.signal.aborted ||
				lifecycleController.signal.aborted ||
				generationController.signal.aborted ||
				generation !== this._replicationAnnouncementRetryGeneration ||
				generationController !==
					this._replicationAnnouncementRepairGenerationController
			) {
				return;
			}
			if (isNotStartedError(error as Error)) {
				return;
			}

			// Only the worker that still owns the current generation may conclude
			// that its repair failed. A stale worker must not clear a newer call's
			// pending flag or attribute its error to the new generation.
			this._replicationAnnouncementRepairPending = false;
			logger.error(error);
		}
	}

	private async repairCurrentReplicationStateAnnouncement(context?: {
		generation: number;
		lifecycleController: AbortController;
		generationController: AbortController;
	}): Promise<void> {
		if (!this._replicationAnnouncementRepairPending) {
			return;
		}
		const generation =
			context?.generation ?? this._replicationAnnouncementRetryGeneration;
		const lifecycleController =
			context?.lifecycleController ??
			this._replicationAnnouncementRepairController;
		const generationController =
			context?.generationController ??
			this._replicationAnnouncementRepairGenerationController;
		const segments = (await this.getMyReplicationSegments()).map((range) =>
			range.toReplicationRange(),
		);
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			lifecycleController.signal.aborted ||
			generationController.signal.aborted
		) {
			return;
		}
		if (generation !== this._replicationAnnouncementRetryGeneration) {
			this.queueCurrentReplicationStateAnnouncementRepair();
			return;
		}
		this.validatePersistedReplicationRangeSnapshot(segments);

		const subscribers =
			(await this.node.services.pubsub.getSubscribers(this.topic)) ?? [];
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			lifecycleController.signal.aborted ||
			generationController.signal.aborted
		) {
			return;
		}
		if (generation !== this._replicationAnnouncementRetryGeneration) {
			this.queueCurrentReplicationStateAnnouncementRepair();
			return;
		}

		const selfHash = this.node.identity.publicKey.hashcode();
		const currentTargets = new Map<string, PublicSignKey>();
		for (const key of subscribers) {
			const hash = key.hashcode();
			if (
				hash !== selfHash &&
				!this._replicationInfoBlockedPeers.has(hash) &&
				!currentTargets.has(hash)
			) {
				currentTargets.set(hash, key);
			}
		}

		for (const [hash, target] of this._replicationAnnouncementRepairTargets) {
			if (target.generation !== generation || !currentTargets.has(hash)) {
				this._replicationAnnouncementRepairTargets.delete(hash);
			} else {
				target.key = currentTargets.get(hash)!;
			}
		}
		if (!this._replicationAnnouncementRepairCohortSelected) {
			const candidates = [...currentTargets.entries()].sort(([left], [right]) =>
				left.localeCompare(right),
			);
			const cursorIndex = this._replicationAnnouncementRepairFairCursorHash
				? candidates.findIndex(
						([hash]) =>
							hash.localeCompare(
								this._replicationAnnouncementRepairFairCursorHash!,
							) > 0,
					)
				: 0;
			const fairStart = cursorIndex < 0 ? 0 : cursorIndex;
			const fairOrder = [
				...candidates.slice(fairStart),
				...candidates.slice(0, fairStart),
			];
			const cohort = fairOrder.slice(
				0,
				REPLICATION_ANNOUNCEMENT_REPAIR_TARGETS_PER_GENERATION,
			);
			for (const [hash, key] of cohort) {
				this._replicationAnnouncementRepairTargets.set(hash, {
					key,
					generation,
					attempts: 0,
					done: false,
				});
			}
			if (cohort.length > 0) {
				this._replicationAnnouncementRepairFairCursorHash =
					cohort[cohort.length - 1][0];
			}
			this._replicationAnnouncementRepairCohortSelected = true;
		}

		const batch = [
			...this._replicationAnnouncementRepairTargets.entries(),
		].filter(([, target]) => !target.done);
		const snapshot = new AllReplicatingSegmentsMessage({ segments });
		const results = await Promise.allSettled(
			batch.map(([, target]) =>
				this.rpc.send(snapshot, {
					mode: new AcknowledgeDelivery({
						to: [target.key],
						redundancy: 1,
					}),
					priority: CONVERGENCE_MESSAGE_PRIORITY,
					signal: generationController.signal,
				}),
			),
		);
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			lifecycleController.signal.aborted ||
			generationController.signal.aborted
		) {
			return;
		}
		if (generation !== this._replicationAnnouncementRetryGeneration) {
			this.queueCurrentReplicationStateAnnouncementRepair();
			return;
		}

		for (const [index, result] of results.entries()) {
			const [hash, attemptedTarget] = batch[index];
			const target = this._replicationAnnouncementRepairTargets.get(hash);
			if (target !== attemptedTarget || target.generation !== generation) {
				continue;
			}
			if (result.status === "fulfilled") {
				// DirectStream ACKs confirm that the signed transport envelope reached
				// the target. Applying the contained replication state remains a
				// receiver-local, best-effort operation.
				target.done = true;
				continue;
			}

			target.attempts += 1;
			if (!isTransientReplicationAnnouncementRepairError(result.reason)) {
				target.done = true;
				logger.error(result.reason);
			} else if (
				target.attempts >= this._replicationAnnouncementRepairMaxAttempts
			) {
				target.done = true;
				logger.trace(
					"Acknowledged replication announcement repair exhausted for %s",
					hash,
				);
			}
		}

		if (generation !== this._replicationAnnouncementRetryGeneration) {
			this.queueCurrentReplicationStateAnnouncementRepair();
			return;
		}
		if (
			[...this._replicationAnnouncementRepairTargets.values()].some(
				(target) => !target.done,
			)
		) {
			void this.replicationAnnouncementRepairDebounced?.call();
			return;
		}

		this._replicationAnnouncementRepairPending = false;
		this._replicationAnnouncementRepairTargets.clear();
	}

	private cancelCurrentReplicationStateAnnouncementRetry(): void {
		this._replicationAnnouncementRetryGeneration += 1;
		this._replicationAnnouncementRetryPending = false;
		this._replicationAnnouncementRetryController?.abort();
		this.replicationAnnouncementRetryDebounced?.close();
		this.cancelCurrentReplicationStateAnnouncementRepair();
	}

	private async sendReplicationAnnouncement(
		message:
			| AllReplicatingSegmentsMessage
			| AddedReplicationSegmentMessage
			| StoppedReplicating,
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<void> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		// Advance before every post-mutation send, including successful ones. An
		// authoritative retry already in flight may have captured the previous
		// local state; the generation mismatch forces one more current snapshot
		// after that stale send settles.
		this._replicationAnnouncementRetryGeneration += 1;
		this.advanceCurrentReplicationStateAnnouncementRepairGeneration();
		try {
			await this.rpc.send(message, {
				priority: CONVERGENCE_MESSAGE_PRIORITY,
				signal: ownershipLifecycleController.signal,
			});
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			this.queueCurrentReplicationStateAnnouncementRepair();
		} catch (error) {
			// An old send can reject only after poison or close has installed a new
			// ownership generation. Never enqueue its retry work into that generation.
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			// The local replication-index mutation precedes all calls to this
			// wrapper. Preserve the explicit caller's rejection, but independently
			// schedule an authoritative snapshot so peers eventually observe the
			// already-committed local state.
			this.queueCurrentReplicationStateAnnouncementRetry(error);
			throw error;
		}
	}

	private async retryCurrentReplicationStateAnnouncement(): Promise<void> {
		const generation = this._replicationAnnouncementRetryGeneration;
		const controller = this._replicationAnnouncementRetryController;
		try {
			const segments = (await this.getMyReplicationSegments()).map((range) =>
				range.toReplicationRange(),
			);
			if (
				this.closed ||
				this._closeController.signal.aborted ||
				controller.signal.aborted
			) {
				return;
			}
			if (generation !== this._replicationAnnouncementRetryGeneration) {
				void this.replicationAnnouncementRetryDebounced?.call();
				return;
			}
			this.validatePersistedReplicationRangeSnapshot(segments);

			await this.rpc.send(new AllReplicatingSegmentsMessage({ segments }), {
				priority: CONVERGENCE_MESSAGE_PRIORITY,
				signal: controller.signal,
			});
			this.queueCurrentReplicationStateAnnouncementRepair();
		} catch (error) {
			if (
				this.closed ||
				this._closeController.signal.aborted ||
				controller.signal.aborted
			) {
				return;
			}
			if (this.queueCurrentReplicationStateAnnouncementRetry(error)) {
				return;
			}
			if (generation === this._replicationAnnouncementRetryGeneration) {
				this._replicationAnnouncementRetryPending = false;
			} else {
				void this.replicationAnnouncementRetryDebounced?.call();
			}
			throw error;
		}
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			controller.signal.aborted
		) {
			return;
		}

		// A newer mutation announcement may have started while this snapshot was
		// in flight. In that case keep the repair pending so the newer current
		// state is also announced in full, regardless of whether its incremental
		// send succeeded or failed.
		if (generation === this._replicationAnnouncementRetryGeneration) {
			this._replicationAnnouncementRetryPending = false;
			if (
				!this.closed &&
				!this._closeController.signal.aborted &&
				!controller.signal.aborted &&
				this._isAdaptiveReplicating
			) {
				void this.rebalanceParticipationDebounced?.call();
			}
		} else {
			void this.replicationAnnouncementRetryDebounced?.call();
		}
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

	private deleteCoordinatesForHashes(
		hashes: Iterable<string>,
		ownershipLifecycleController?: AbortController,
	): MaybePromise<void> {
		if (ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetCoordinateStateForHashValues(values);
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIdsNoReturn) {
			return mapMaybePromise(coordinateIndex.delIdsNoReturn(values), () => {
				if (ownershipLifecycleController) {
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			});
		}
		if (coordinateIndex.delIds) {
			return mapMaybePromise(coordinateIndex.delIds(values), () => {
				if (ownershipLifecycleController) {
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			});
		}
		return mapMaybePromise(
			this.entryCoordinatesIndex.del({
				query:
					values.length === 1
						? { hash: values[0] }
						: new Or(
								values.map(
									(hash) => new StringMatch({ key: "hash", value: hash }),
								),
							),
			}),
			() => {
				if (ownershipLifecycleController) {
					this.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			},
		);
	}

	private forgetCoordinateStateForHashes(hashes: Iterable<string>) {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetCoordinateStateForHashValues(values);
	}

	private forgetCoordinateStateForHashValues(values: string[]) {
		this._nativeSharedLogState?.deleteEntryCoordinatesBatch(values);
		this._nativeBackbone?.deleteEntryCoordinatesBatch(values);
		this.forgetResidentCoordinateStateForHashValues(values);
	}

	private forgetResidentCoordinateStateForHashes(hashes: Iterable<string>) {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetResidentCoordinateStateForHashValues(values);
	}

	private forgetResidentCoordinateStateForHashValues(values: string[]) {
		if (this._residentEntryCoordinatesByHash) {
			for (const hash of values) {
				this._residentEntryCoordinatesByHash.delete(hash);
			}
		}
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
			await this.deleteCoordinatesForHashes(
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
				msg: AddedReplicationSegmentMessage | AllReplicatingSegmentsMessage,
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
				await this.sendReplicationAnnouncement(
					new StoppedReplicating({
						segmentIds: confirmedPreliminaryRemovals.map((x) => x.id),
					}),
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
				await this.sendReplicationAnnouncement(
					new AllReplicatingSegmentsMessage({ segments }),
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
			announce?: (
				msg: AllReplicatingSegmentsMessage | AddedReplicationSegmentMessage,
			) => void;
		},
	) {
		this.throwIfCheckedPruneRemoveBlocksLocalOperation(
			"replication range mutation",
		);
		const replicationOwnershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
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

	async unreplicate(rangeOrEntry?: Entry<T> | { id: Uint8Array }[]) {
		this.throwIfCheckedPruneRemoveBlocksLocalOperation(
			"replication range mutation",
		);
		const replicationOwnershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		return this._unreplicate(
			rangeOrEntry,
			replicationOwnershipLifecycleController,
		);
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
			await this.sendReplicationAnnouncement(
				new StoppedReplicating({ segmentIds: removedSegmentIds }),
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
			expectedJoinWarmupGeneration?: object | null;
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
		const expectedJoinWarmupGeneration =
			options?.expectedJoinWarmupGeneration !== undefined
				? options.expectedJoinWarmupGeneration
				: (this._joinWarmupGenerationByTarget.get(keyHash) ?? null);
		const ownsSubscriptionEpoch = () =>
			options?.subscriptionEpoch === undefined ||
			this.isCurrentSubscriptionEpoch(keyHash, options.subscriptionEpoch);
		const ownsReplicationLifecycle = () =>
			options?.replicationLifecycleController === undefined ||
			this.isReplicationLifecycleActive(options.replicationLifecycleController);
		const ownsReplicationOwnershipLifecycle = () =>
			this.isRepairLifecycleActive(replicationOwnershipLifecycleController);
		const cancelExpectedJoinWarmupTarget = () => {
			if (
				expectedJoinWarmupGeneration !== null &&
				this._joinWarmupGenerationByTarget.get(keyHash) ===
					expectedJoinWarmupGeneration
			) {
				this.cancelJoinWarmupTarget(keyHash);
			}
		};
		const isMe = this.node.identity.publicKey.hashcode() === keyHash;
		let receiveAdmissionBlocked = false;
		const receiveCleanupGateByPeer = this._receiveCleanupGateByPeer;
		const blockAndDrainPeerReceives = async () => {
			if (!receiveAdmissionBlocked) {
				receiveCleanupGateByPeer.set(
					keyHash,
					(receiveCleanupGateByPeer.get(keyHash) ?? 0) + 1,
				);
				receiveAdmissionBlocked = true;
			}
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
			this.cleanupPeerDisconnectTracking(keyHash);
			this.removeRepairFrontierTarget(keyHash, {
				expectedJoinWarmupGeneration,
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

		// Replication-info updates already serialize per peer. Put the hash-wide
		// removal on the same queue so a newer reset cannot be deleted underneath
		// itself by an older unsubscribe callback.
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
			const mutationCommitted = await this.withReplicationRangeMutationQueue(
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
				// Replication-info handlers release their receive lease before joining
				// this lane. Fence every handler queued behind this successful removal,
				// regardless of whether it came from liveness, startup pruning, or an
				// unsubscribe transition.
				this.advanceReplicationInfoRecoveryEpoch(keyHash);
				this.rebalanceParticipationDebounced?.call();
			}
			removed = true;
			if (!ownerHasRanges) {
				options?.onRemoved?.({ wasReplicator });
			}
			let announcementError: unknown;
			if (isMe && !ownerHasRanges) {
				try {
					await this.sendReplicationAnnouncement(
						new AllReplicatingSegmentsMessage({ segments: [] }),
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
		}).finally(() => {
			if (receiveAdmissionBlocked) {
				const remaining = (receiveCleanupGateByPeer.get(keyHash) ?? 1) - 1;
				if (remaining > 0) {
					receiveCleanupGateByPeer.set(keyHash, remaining);
				} else {
					receiveCleanupGateByPeer.delete(keyHash);
				}
			}
		});
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
		},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<boolean> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
		return this.withReplicationRangeMutationQueue(
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
			allowLegacyOrderedReplacementPairs?: boolean;
			onConfirmedDurableStateChanged?: () => void;
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
		return this.withReplicationRangeMutationQueue(
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
			allowLegacyOrderedReplacementPairs,
			onConfirmedDurableStateChanged,
		}: {
			reset?: boolean;
			rebalance?: boolean;
			checkDuplicates?: boolean;
			timestamp?: number;
			allowLegacyOrderedReplacementPairs?: boolean;
			onConfirmedDurableStateChanged?: () => void;
		} = {},
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.validateReplicationRangeAnnouncement(ranges);
		this.throwIfReplicationOwnershipLifecycleInactive(
			replicationOwnershipLifecycleController,
		);
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
				(!allowLegacyOrderedReplacementPairs || reset === true || count > 2)
			) {
				throw new Error(
					`Duplicate replication range id in announcement: ${range.idString}`,
				);
			}
			// Released peers represented a non-reset replacement as the retired
			// geometry followed by the current geometry under the same id. The
			// sender is already authorized to replace that id with the final item,
			// so collapsing an exact two-item incremental pair to its last item
			// preserves rolling-upgrade compatibility without broadening authority.
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
				const deletion = await this.deleteReplicationRangesCoherently(
					deleted,
					fromHash,
					{ preserveOwnerMembership: ranges.length > 0 },
				);
				deleted = deletion.removed;

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
			for (const diff of diffs) {
				if (diff.type !== "added") {
					continue;
				}
				appliedPositiveRanges.push(diff);
				await this.replicationIndex.put(diff.range);
				this.putNativeReplicationRange(diff.range);
			}
			if (reset && diffs.length > 0) {
				await this.updateOldestTimestampFromIndex();
			}
		} catch (error) {
			let outcomeError = error;
			if (appliedPositiveRanges.length > 0) {
				try {
					await rollbackAppliedPositiveRanges();
				} catch (rollbackError) {
					outcomeError = poisonFromPositiveRollback(rollbackError, error);
				}
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
			announce?: (
				msg: AllReplicatingSegmentsMessage | AddedReplicationSegmentMessage,
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
				await this.sendReplicationAnnouncement(
					new AllReplicatingSegmentsMessage({ segments }),
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
						this._providerHandle = fanoutService.provide(
							`shared-log|${this.topic}`,
							{
								ttlMs: 120_000,
								announceIntervalMs: 60_000,
							},
						);
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
						segments: added.map((x) => x.range.toReplicationRange()),
					});
				} else {
					message = new AddedReplicationSegmentMessage({
						segments: added.map((x) => x.range.toReplicationRange()),
					});
				}
				if (options.announce) {
					return options.announce(message);
				} else {
					await this.sendReplicationAnnouncement(
						message,
						replicationOwnershipLifecycleController,
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

	private markRepairSweepOptimisticPeer(
		gid: string,
		peer: string,
		generation: object,
	) {
		let peers = this._repairSweepOptimisticGidPeersPending.get(gid);
		if (!peers) {
			peers = new Map();
			this._repairSweepOptimisticGidPeersPending.set(gid, peers);
		}
		const current = peers.get(peer);
		peers.set(peer, {
			count: current?.generation === generation ? current.count + 1 : 1,
			generation,
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
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
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
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
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

	private getJoinWarmupGeneration(target: string) {
		let generation = this._joinWarmupGenerationByTarget.get(target);
		if (!generation) {
			generation = {};
			this._joinWarmupGenerationByTarget.set(target, generation);
		}
		return generation;
	}

	private trackJoinWarmupTimer(
		target: string,
		timer: JoinWarmupRetryTimer,
	) {
		let timers = this._joinWarmupRetryTimersByTarget.get(target);
		if (!timers) {
			timers = new Set();
			this._joinWarmupRetryTimersByTarget.set(target, timers);
		}
		timers.add(timer);
		this._repairRetryTimers.add(timer.handle);
	}

	private untrackJoinWarmupTimer(
		target: string,
		timer: JoinWarmupRetryTimer,
	) {
		this._repairRetryTimers.delete(timer.handle);
		const timers = this._joinWarmupRetryTimersByTarget.get(target);
		if (!timers) {
			return;
		}
		timers.delete(timer);
		if (timers.size === 0) {
			this._joinWarmupRetryTimersByTarget.delete(target);
		}
	}

	private cancelJoinWarmupTimers(target: string) {
		const timers = this._joinWarmupRetryTimersByTarget.get(target);
		if (!timers) {
			return;
		}
		for (const timer of [...timers]) {
			clearTimeout(timer.handle);
			timer.resolve?.();
			this.untrackJoinWarmupTimer(target, timer);
		}
	}

	private cancelJoinWarmupTarget(target: string) {
		this._joinWarmupGenerationByTarget.delete(target);
		const pendingWarmupPeers =
			this._repairSweepPendingPeersByMode.get("join-warmup");
		pendingWarmupPeers?.delete(target);
		if (pendingWarmupPeers?.size === 0) {
			this._repairSweepPendingModes.delete("join-warmup");
		}
		this._repairSweepJoinWarmupGenerationByTarget.delete(target);
		this.clearRepairSweepOptimisticPeer(target);
		this.cancelJoinWarmupTimers(target);
		this._joinWarmupScheduledRetriesByTarget.delete(target);
		const state = this._joinWarmupSendStateByTarget.get(target);
		if (!state) {
			return;
		}
		state.bypassKnownPeerHints = false;
		state.entries.clear();
		state.pending = false;
		if (!state.running) {
			this._joinWarmupSendStateByTarget.delete(target);
		}
	}

	private cancelAllJoinWarmupTargets() {
		const targets = new Set([
			...this._joinWarmupGenerationByTarget.keys(),
			...this._joinWarmupRetryTimersByTarget.keys(),
			...this._joinWarmupScheduledRetriesByTarget.keys(),
			...this._joinWarmupSendStateByTarget.keys(),
		]);
		for (const target of targets) {
			this.cancelJoinWarmupTarget(target);
		}
	}

	private async sleepJoinWarmupTracked(
		target: string,
		delayMs: number,
		repairLifecycleController: AbortController,
	) {
		if (delayMs <= 0) {
			return;
		}
		await new Promise<void>((resolve) => {
			let settled = false;
			let trackedTimer!: JoinWarmupRetryTimer;
			const settle = () => {
				if (settled) {
					return;
				}
				settled = true;
				if (repairLifecycleController === this._repairLifecycleController) {
					this.untrackJoinWarmupTimer(target, trackedTimer);
				}
				resolve();
			};
			const handle = setTimeout(settle, delayMs);
			handle.unref?.();
			trackedTimer = { handle, resolve: settle };
			this.trackJoinWarmupTimer(target, trackedTimer);
		});
	}

	private scheduleJoinWarmupRetries(
		target: string,
		generation: object,
		delaysMs: Iterable<number>,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		bypassKnownPeerHints: boolean,
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
	) {
		if (
			!this.isRepairLifecycleActive(repairLifecycleController) ||
			this._joinWarmupGenerationByTarget.get(target) !== generation
		) {
			return;
		}
		let scheduled = this._joinWarmupScheduledRetriesByTarget.get(target);
		if (scheduled?.generation !== generation) {
			this.cancelJoinWarmupTimers(target);
			this._joinWarmupScheduledRetriesByTarget.delete(target);
			scheduled = undefined;
		}
		if (!scheduled) {
			scheduled = {
				generation,
				slotsByDelay: new Map(),
			};
			this._joinWarmupScheduledRetriesByTarget.set(target, scheduled);
		}
		const delays = [...new Set(delaysMs)];
		const batch: JoinWarmupScheduledRetryBatch<R> = {
			bypassKnownPeerHints,
			entries: new Map(entries),
			remainingAttempts: delays.length,
		};
		const now = Date.now();
		for (const delayMs of delays) {
			let slot = scheduled.slotsByDelay.get(delayMs);
			if (!slot) {
				slot = { cohorts: [], head: 0 };
				scheduled.slotsByDelay.set(delayMs, slot);
			}
			const tail = slot.cohorts.at(-1);
			const dueAt = Math.max(tail?.dueAt ?? 0, now + delayMs);
			if (tail?.dueAt === dueAt) {
				tail.batches.push(batch);
			} else {
				slot.cohorts.push({
					batches: [batch],
					dueAt,
				});
			}
			this.armJoinWarmupRetrySlot(
				target,
				scheduled,
				delayMs,
				slot,
				repairLifecycleController,
			);
		}
	}

	private armJoinWarmupRetrySlot(
		target: string,
		scheduled: JoinWarmupScheduledRetries<R>,
		delayMs: number,
		slot: JoinWarmupScheduledRetrySlot<R>,
		repairLifecycleController: AbortController,
	) {
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return;
		}
		const nextDueAt = slot.cohorts[slot.head]?.dueAt;
		if (nextDueAt == null) {
			return;
		}
		if (slot.timer && slot.timerDueAt === nextDueAt) {
			return;
		}
		if (slot.timer) {
			clearTimeout(slot.timer.handle);
			this.untrackJoinWarmupTimer(target, slot.timer);
		}
		let trackedTimer!: JoinWarmupRetryTimer;
		const handle = setTimeout(
			() => {
				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				this.untrackJoinWarmupTimer(target, trackedTimer);
				if (slot.timer !== trackedTimer) {
					return;
				}
				slot.timer = undefined;
				slot.timerDueAt = undefined;
				const current = this._joinWarmupScheduledRetriesByTarget.get(target);
				if (
					current !== scheduled ||
					current.slotsByDelay.get(delayMs) !== slot
				) {
					return;
				}

				const dueEntries = new Map<string, RepairDispatchEntry<R>>();
				let bypassKnownPeerHints = false;
				const now = Date.now();
				while (
					slot.head < slot.cohorts.length &&
					slot.cohorts[slot.head].dueAt <= now
				) {
					const cohort = slot.cohorts[slot.head++];
					for (const batch of cohort.batches) {
						for (const [hash, entry] of batch.entries) {
							dueEntries.set(hash, entry);
						}
						bypassKnownPeerHints ||=
							batch.bypassKnownPeerHints;
						batch.remainingAttempts -= 1;
						if (batch.remainingAttempts === 0) {
							batch.entries.clear();
						}
					}
					cohort.batches.length = 0;
				}
				if (
					dueEntries.size > 0 &&
					!this.closed &&
					this._joinWarmupGenerationByTarget.get(target) ===
						scheduled.generation
				) {
					this.queueJoinWarmupSend(
						target,
						scheduled.generation,
						dueEntries,
						bypassKnownPeerHints,
						repairLifecycleController,
					);
				}
				if (slot.head === slot.cohorts.length) {
					current.slotsByDelay.delete(delayMs);
					if (current.slotsByDelay.size === 0) {
						this._joinWarmupScheduledRetriesByTarget.delete(target);
					}
					return;
				}
				if (slot.head >= 1_024 && slot.head * 2 >= slot.cohorts.length) {
					slot.cohorts = slot.cohorts.slice(slot.head);
					slot.head = 0;
				}
				this.armJoinWarmupRetrySlot(
					target,
					current,
					delayMs,
					slot,
					repairLifecycleController,
				);
			},
			Math.max(0, nextDueAt - Date.now()),
		);
		handle.unref?.();
		trackedTimer = { handle };
		slot.timer = trackedTimer;
		slot.timerDueAt = nextDueAt;
		this.trackJoinWarmupTimer(target, trackedTimer);
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

	private removeRepairFrontierTarget(
		target: string,
		options?: { expectedJoinWarmupGeneration?: object | null },
	) {
		if (
			options?.expectedJoinWarmupGeneration === undefined ||
			(options.expectedJoinWarmupGeneration !== null &&
				this._joinWarmupGenerationByTarget.get(target) ===
					options.expectedJoinWarmupGeneration)
		) {
			this.cancelJoinWarmupTarget(target);
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

	private async pushRepairEntries(
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		isStillCurrent: () => boolean = () => true,
		signal?: AbortSignal,
	) {
		if (!isStillCurrent()) {
			return;
		}
		const hashes = [...entries.keys()];
		if (
			this._logProperties?.sync?.rawExchangeHeads === true &&
			this.peerSupportsRawExchangeHeads(target)
		) {
			const reserved = new Uint8Array(4);
			reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
			const sentMessages = await this.trySendFusedRawExchangeHeads(
				hashes,
				[target],
				{ priority: SYNC_MESSAGE_PRIORITY, reserved, signal },
			);
			if (!isStillCurrent()) {
				return;
			}
			if (sentMessages !== undefined) {
				return;
			}
			for await (const message of createRawExchangeHeadsMessages(
				this.log,
				hashes,
				this._logProperties?.sync?.profile,
			)) {
				if (!isStillCurrent()) {
					return;
				}
				message.reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
				await this.rpc.send(message, {
					priority: SYNC_MESSAGE_PRIORITY,
					mode: new SilentDelivery({ to: [target], redundancy: 1 }),
					signal,
				});
			}
			return;
		}
		for await (const message of createExchangeHeadsMessages(this.log, hashes)) {
			if (!isStillCurrent()) {
				return;
			}
			message.reserved[0] |= EXCHANGE_HEADS_REPAIR_HINT;
			await this.rpc.send(message, {
				priority: SYNC_MESSAGE_PRIORITY,
				mode: new SilentDelivery({ to: [target], redundancy: 1 }),
				signal,
			});
		}
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
			? this.materializeRepairDispatchEntries(unknownEntries)
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

	private queueJoinWarmupSend(
		target: string,
		generation: object,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		bypassKnownPeerHints: boolean,
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
	) {
		if (
			!this.isRepairLifecycleActive(repairLifecycleController) ||
			this._joinWarmupGenerationByTarget.get(target) !== generation
		) {
			return;
		}
		let state = this._joinWarmupSendStateByTarget.get(target);
		if (!state) {
			state = {
				bypassKnownPeerHints: false,
				entries: new Map(),
				generation,
				lastCompletedAt: Number.NEGATIVE_INFINITY,
				pending: false,
				running: false,
			};
			this._joinWarmupSendStateByTarget.set(target, state);
		} else if (state.generation !== generation) {
			state.bypassKnownPeerHints = false;
			state.entries.clear();
			state.pending = false;
		}
		for (const [hash, entry] of entries) {
			state.entries.set(hash, entry);
		}
		state.bypassKnownPeerHints ||= bypassKnownPeerHints;
		state.generation = generation;
		state.pending = true;
		if (state.running) {
			return;
		}
		void this.drainJoinWarmupSends(
			target,
			state,
			repairLifecycleController,
		).catch((error: any) => {
			if (this.isRepairLifecycleActive(repairLifecycleController)) {
				logger.error(error);
			}
		});
	}

	private async drainJoinWarmupSends(
		target: string,
		state: JoinWarmupSendState<R>,
		repairLifecycleController: AbortController,
	) {
		if (
			state.running ||
			!this.isRepairLifecycleActive(repairLifecycleController)
		) {
			return;
		}
		state.running = true;
		try {
			while (state.pending) {
				if (!this.isRepairLifecycleActive(repairLifecycleController)) {
					return;
				}
				state.pending = false;
				const generation = state.generation;
				const entries = new Map(state.entries);
				state.entries.clear();
				const bypassKnownPeerHints = state.bypassKnownPeerHints;
				state.bypassKnownPeerHints = false;
				const spacingMs = Math.max(
					0,
					state.lastCompletedAt + JOIN_WARMUP_SEND_SPACING_MS - Date.now(),
				);
				await this.sleepJoinWarmupTracked(
					target,
					spacingMs,
					repairLifecycleController,
				);
				if (
					!this.isRepairLifecycleActive(repairLifecycleController) ||
					state.generation !== generation ||
					this._joinWarmupGenerationByTarget.get(target) !== generation
				) {
					continue;
				}
				if (entries.size === 0) {
					continue;
				}
				this._repairMetrics["join-warmup"].simpleFallbackPasses += 1;
				try {
					await this.sendRepairEntriesWithTransport(target, entries, "simple", {
						bypassKnownPeers: bypassKnownPeerHints,
						bypassRecentKnownPeers: bypassKnownPeerHints,
						isStillCurrent: () =>
							this.isRepairLifecycleActive(repairLifecycleController),
						signal: repairLifecycleController.signal,
					});
				} catch (error: any) {
					if (this.isRepairLifecycleActive(repairLifecycleController)) {
						logger.error(error);
					}
				} finally {
					if (this.isRepairLifecycleActive(repairLifecycleController)) {
						state.lastCompletedAt = Date.now();
					}
				}
			}
		} finally {
			state.running = false;
			if (this._joinWarmupSendStateByTarget.get(target) === state) {
				const currentRepairLifecycleController =
					this._repairLifecycleController;
				if (
					state.pending &&
					this.isRepairLifecycleActive(currentRepairLifecycleController)
				) {
					void this.drainJoinWarmupSends(
						target,
						state,
						currentRepairLifecycleController,
					).catch((error: any) => logger.error(error));
				} else if (!this._joinWarmupGenerationByTarget.has(target)) {
					this._joinWarmupSendStateByTarget.delete(target);
				}
			}
		}
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
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
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
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
	) {
		const activeTargets = this._repairFrontierActiveTargetsByMode.get(mode);
		if (
			!activeTargets ||
			activeTargets.has(target) ||
			!this.isRepairLifecycleActive(repairLifecycleController)
		) {
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
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return;
					}
					const pending = this._repairFrontierByMode.get(mode)?.get(target);
					if (!pending || pending.size === 0) {
						if (!this.isRepairLifecycleActive(repairLifecycleController)) {
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

					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
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
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
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
		})().catch((error: any) => {
			activeTargets.delete(target);
			if (this.isRepairLifecycleActive(repairLifecycleController)) {
				logger.error(error);
			}
		});
	}

	private flushAppendBackfill(
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
	) {
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

	private queueAppendBackfill(target: string, entry: EntryReplicated<R>) {
		const repairLifecycleController = this._repairLifecycleController;
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

	private dispatchMaybeMissingEntries(
		target: string,
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
		options: {
			mode: RepairDispatchMode;
			bypassRecentDedupe?: boolean;
			bypassKnownPeerHints?: boolean;
			retryScheduleMs?: number[];
		},
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
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
		const joinWarmupGeneration =
			options.mode === "join-warmup"
				? this.getJoinWarmupGeneration(target)
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
				joinWarmupGeneration
			) {
				this.queueJoinWarmupSend(
					target,
					joinWarmupGeneration,
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
				joinWarmupGeneration &&
				transport === "simple"
			) {
				delayedJoinWarmupRetries.push(delayMs);
				return;
			}
			const timer = setTimeout(() => {
				if (repairLifecycleController === this._repairLifecycleController) {
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
		if (joinWarmupGeneration && delayedJoinWarmupRetries.length > 0) {
			this.scheduleJoinWarmupRetries(
				target,
				joinWarmupGeneration,
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
			joinWarmupGenerations?: ReadonlyMap<string, object>;
		},
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
	) {
		if (!this.isRepairLifecycleActive(repairLifecycleController)) {
			return;
		}
		const pendingPeers = this._repairSweepPendingPeersByMode.get(options.mode);
		if (pendingPeers) {
			for (const peer of options.peers ?? []) {
				if (options.mode === "join-warmup") {
					const generation =
						options.joinWarmupGenerations?.get(peer) ??
						this.getJoinWarmupGeneration(peer);
					if (
						this._joinWarmupGenerationByTarget.get(peer) !== generation
					) {
						continue;
					}
					this._repairSweepJoinWarmupGenerationByTarget.set(
						peer,
						generation,
					);
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
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
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
		repairLifecycleController: AbortController = this
			._repairLifecycleController,
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
				const pendingJoinWarmupGenerations = new Map(
					this._repairSweepJoinWarmupGenerationByTarget,
				);
				this._repairSweepPendingModes.clear();
				for (const peers of this._repairSweepPendingPeersByMode.values()) {
					peers.clear();
				}
				this._repairSweepJoinWarmupGenerationByTarget.clear();
				const pendingJoinWarmupPeers = pendingPeersByMode.get("join-warmup");
				const pruneStaleJoinWarmupPeers = () => {
					if (!this.isRepairLifecycleActive(repairLifecycleController)) {
						return false;
					}
					for (const peer of [...(pendingJoinWarmupPeers ?? [])]) {
						if (
							this._joinWarmupGenerationByTarget.get(peer) !==
							pendingJoinWarmupGenerations.get(peer)
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
						this._joinWarmupGenerationByTarget.get(target) !==
							pendingJoinWarmupGenerations.get(target)
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
						this._joinWarmupGenerationByTarget.get(target) !==
							pendingJoinWarmupGenerations.get(target)
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

				const residentEntriesByHash = this._residentEntryCoordinatesByHash;
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
							if (
								!current ||
								current.generation !== consumed.generation
							) {
								continue;
							}
							const next = current.count - consumed.count;
							if (next > 0) {
								pendingPeerCounts.set(peer, {
									count: next,
									generation: current.generation,
								});
							} else {
								pendingPeerCounts.delete(peer);
								const gids =
									this._repairSweepOptimisticGidsByPeer.get(peer);
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
			if (repairLifecycleController !== this._repairLifecycleController) {
				return;
			}
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

	private async pruneDebouncedFnAddIfNotKeeping(
		args: {
			key: string;
			value: {
				entry: CheckedPruneEntry<T, R>;
				leaders: CheckedPruneLeaderMap;
			};
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<boolean> {
		if (this.closed || !this.pruneDebouncedFn) {
			return false;
		}
		const checkedPruneCoordinator = this._checkedPrune;
		const pruneDebouncedFn = this.pruneDebouncedFn;
		const isCurrent = () =>
			this.isRepairLifecycleActive(ownershipLifecycleController) &&
			this._checkedPrune === checkedPruneCoordinator &&
			this.pruneDebouncedFn === pruneDebouncedFn;
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
		checkedPruneCoordinator.trackCandidate(
			args.key,
			args.value.entry,
			args.value.leaders,
		);
		void pruneDebouncedFn.add(args).catch((error) => {
			if (isCurrent() && !isNotStartedError(error as Error)) {
				logger.error(error);
			}
		});
		return true;
	}

	private async cancelCheckedPruneForLocalLeader(
		hash: string,
		options?: { preserveRetry?: boolean },
	) {
		this.pruneDebouncedFn.delete(hash);
		const pendingDelete = this._checkedPrune.getPendingDelete(hash);
		this._checkedPrune.markCancelled(hash, {
			preserveRetry: options?.preserveRetry,
		});
		await pendingDelete?.reject(new Error("Failed to delete, is leader again"));
	}

	private hasActiveCheckedPruneWork(hash: string) {
		return this._checkedPrune.hasActiveWork(hash);
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
				this.throwIfReplicationOwnershipLifecycleInactive(
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
		if (args.leaders.has(selfHash)) {
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
			const currentLeaders = await this.findLeadersFromEntry(
				args.entry,
				decodeReplicas(args.entry).getValue(this),
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

	private async pruneJoinedEntriesNoLongerLed(
		entries: ShallowOrFullEntry<T>[],
		options?: {
			decodedReplicaCounts?: DecodedReplicaCountMap;
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
		const plans = new Array<EntryLeaderPlan<R> | undefined>(entries.length);
		const leaderItems: Array<{
			entry: ShallowOrFullEntry<T>;
			replicas: number;
			options: { roleAge: number; persist: false };
		}> = [];
		const leaderItemIndexes: number[] = [];
		let reusableLeaderPlanHits = 0;
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			const replicas =
				options?.decodedReplicaCounts?.get(entry.hash) ??
				decodeReplicas(entry).getValue(this);
			const reusablePlan = options?.reusableLeaderPlans?.get(entry.hash);
			if (reusablePlan && reusablePlan.replicas === replicas) {
				plans[i] = reusablePlan.plan;
				reusableLeaderPlanHits++;
				continue;
			}
			leaderItems.push({
				entry,
				replicas,
				options: { roleAge: 0, persist: false },
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
		for (let i = 0; i < entries.length; i++) {
			if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
				continue;
			}
			const entry = entries[i]!;
			const leaders = plans[i]?.leaders ?? new Map();

			if (leaders.has(selfHash)) {
				await this.cancelCheckedPruneForLocalLeader(entry.hash);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				cancelledLocalLeader++;
				continue;
			}

			if (this._checkedPrune.hasPendingDelete(entry.hash)) {
				continue;
			}

			if (leaders.size === 0) {
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
			this.responseToPruneDebouncedFn.delete(entry.hash);
		}
		emitSyncProfileDuration(options?.profile, loopStartedAt, {
			name: "sharedLog.receive.checkedPrune.loop",
			component: "shared-log",
			entries: entries.length,
			count: enqueuedPrune,
			messages: 1,
			details: { cancelledLocalLeader },
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
					this.responseToPruneDebouncedFn.delete(entryReplicated.hash);
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
			this.responseToPruneDebouncedFn.delete(head.hash);
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

	private clearCheckedPruneRetry(hash: string) {
		this._checkedPrune.clearRetry(hash);
	}

	private scheduleCheckedPruneRetry(
		args: {
			entry: CheckedPruneEntry<T, R>;
			leaders: CheckedPruneLeaderMap | Set<string>;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		const checkedPruneCoordinator = this._checkedPrune;
		const isCurrent = () =>
			this.isRepairLifecycleActive(ownershipLifecycleController) &&
			this._checkedPrune === checkedPruneCoordinator;
		if (!isCurrent()) return;
		if (checkedPruneCoordinator.hasPendingDelete(args.entry.hash)) return;

		const hash = args.entry.hash;
		const state =
			checkedPruneCoordinator.getRetry(hash) ??
			({
				attempts: 0,
				entry: args.entry,
				leaders: args.leaders,
			} satisfies CheckedPruneRetryState<T, R>);
		state.entry = args.entry;
		state.leaders = args.leaders;

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
		state.timer = setTimeout(() => {
			const run = async () => {
				const st = checkedPruneCoordinator.getRetry(hash);
				if (st) st.timer = undefined;
				if (!isCurrent()) return;
				if (checkedPruneCoordinator.hasPendingDelete(hash)) return;
				const retryEntry = st?.entry ?? args.entry;
				const retryLeaders = st?.leaders ?? args.leaders;

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
					if (!isCurrent()) {
						return;
					}
					// A current-generation planning failure is best-effort; fall back
					// to the last confirmed leader set below.
				}
				if (!isCurrent()) return;

				if (!leadersMap || leadersMap.size === 0) {
					leadersMap = this.checkedPruneLeadersToMap(retryLeaders);
				}

				const leadersForRetry =
					leadersMap ?? new Map<string, { intersecting: boolean }>();
				await this.pruneDebouncedFnAddIfNotKeeping(
					{
						key: hash,
						value: { entry: retryEntry, leaders: leadersForRetry },
					},
					ownershipLifecycleController,
				);
			};
			void run().catch((error) => {
				if (isCurrent() && !isNotStartedError(error as Error)) {
					logger.error(error);
				}
			});
		}, delayMs);
		state.timer.unref?.();
		checkedPruneCoordinator.setRetry(hash, state);
	}

	private async recoverCheckedPruneFromLateResponses(
		hashes: string[],
		publicKeyHash: string,
	) {
		if (this.closed) return;
		const selfHash = this.node.identity.publicKey.hashcode();
		const toPrune = new Map<
			string,
			{
				entry: CheckedPruneEntry<T, R>;
				leaders: CheckedPruneLeaderMap;
			}
		>();
		const responseStillApplies: string[] = [];

		for (const hash of hashes) {
			if (this.closed) {
				break;
			}
			if (this._checkedPrune.hasPendingDelete(hash)) {
				continue;
			}
			const retry = this._checkedPrune.clearRetryTimer(hash);
			if (!retry) {
				continue;
			}

			const entry = retry.entry;
			let leaders = this.checkedPruneLeadersToMap(retry.leaders);
			try {
				const currentLeaders = await this.findLeadersFromEntry(
					entry,
					decodeReplicas(entry).getValue(this),
					{ roleAge: 0 },
				);
				if (currentLeaders.size > 0) {
					leaders = currentLeaders;
				}
			} catch {
				// Best-effort only; the stored retry leaders came from a previous
				// checked-prune decision for this exact entry.
			}

			if (leaders.has(selfHash)) {
				await this.cancelCheckedPruneForLocalLeader(hash);
				continue;
			}
			if (leaders.size === 0) {
				continue;
			}

			toPrune.set(hash, { entry, leaders });
			if (leaders.has(publicKeyHash)) {
				responseStillApplies.push(hash);
			}
		}

		if (toPrune.size === 0) {
			return;
		}

		const pruneTasks = this.prune(toPrune);
		const confirmationTasks: Promise<void>[] = [];
		for (const hash of responseStillApplies) {
			const pendingDelete = this._checkedPrune.getPendingDelete(hash);
			if (pendingDelete) {
				confirmationTasks.push(
					Promise.resolve(pendingDelete.resolve(publicKeyHash)),
				);
			}
		}
		// The restarted prune session owns its own bounded timeout and revalidates
		// before deletion. Only the peer-derived confirmations must remain inside
		// this receive lease; waiting for the whole prune session could stall close
		// or disconnect until its background timeout.
		void Promise.allSettled(pruneTasks);
		await Promise.allSettled(confirmationTasks);
	}

	async append(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		this.throwIfNativeDurableCommitFailed();
		const ownershipLifecycleController =
			this.captureReplicationOwnershipLifecycle();
		if (this._isAdaptiveReplicating) {
			this.markLocalAppendActivity();
		}

		const { appendOptions, minReplicasValue } = this.createLogAppendOptions(
			options,
			ownershipLifecycleController,
		);
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

	// Trusted local append path for callers that already validated the entry.
	private async appendLocallyValidated(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		this.throwIfNativeDurableCommitFailed();
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
		},
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
		appendCommit: PreparedLocalAppendCommit<R>;
	}> {
		this.throwIfNativeDurableCommitFailed();
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
		const result = await asTrustedLowerLog(this.log).appendLocallyPrepared(
			data,
			appendOptions,
			{
				skipMissingNextJoin: properties?.skipMissingNextJoin,
				resolveTrimmedEntries: properties?.resolveTrimmedEntries,
				payloadData: properties?.payloadData,
			},
		);
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
					await this.deleteCoordinatesForHashes(
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
				await this.deleteCoordinatesForHashes(
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
							ownershipLifecycleController,
						},
					);
			await this.persistPreparedCoordinate(
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
				await this.deleteCoordinatesForHashes(
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
				pruneEntry = this.materializePreparedCoordinateEntry(
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
			pruneEntry ??= this.materializePreparedCoordinateEntry(
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
		});
	}

	// Trusted local payload append path that keeps the public Entry lazy.
	private appendLocallyPreparedPayloadCommitOnly(
		payloadData: Uint8Array,
		options?: SharedAppendOptions<T> | undefined,
		properties?: PreparedPayloadCommitOnlyProperties,
	): MaybePromise<PreparedPayloadCommitOnlyResult<T, R> | undefined> {
		this.throwIfNativeDurableCommitFailed();
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
				!this.canUseNativeBackboneResidentCoordinateState())
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
				!this.canUseNativeBackboneResidentCoordinateState())
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
			this.canUseBackboneOnlyCoordinatePersistence()
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
					const flushed = this.flushNativeBackboneCoordinateJournal();
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
						const coordinateRollback = this.snapshotResidentCoordinateEntries([
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
			return this.completeNativeStrictDurableTransaction(
				nativeStrictTransaction,
			).then(() => undefined);
		}
		return mapMaybePromise(result, async (prepared) => {
			if (!prepared) {
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
					await this.rollbackNativeBackboneCoordinateAppendDurably(
						prepared.appendFacts.hash,
						lowerPublicationRollback?.coordinateEntries,
					);
					for (const document of lowerPublicationRollback?.documents ?? []) {
						this.restoreNativeBackboneDocument(document);
					}
					const flushed = this.flushNativeBackboneCoordinateJournal();
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
						appendCommit,
					};
				};
				if (!prepared.nativeCommittedAppendFinalizer) {
					throw new Error("Missing deferred native append finalizer");
				}
				// Strict success cannot honor batching thresholds: native
				// coordinate/document/signer facts must be physically durable before
				// the lower commit marker is acknowledged and its intent is retired.
				await this.flushNativeBackboneCoordinateJournal();
				await prepared.nativeCommittedAppendFinalizer.acknowledge(() =>
					this.markNativeStrictDurableTransactionLowerMarker(
						nativeStrictTransaction,
					),
				);
				finishResult = finish();
			} catch (error) {
				return rollback(error);
			}
			this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
				prepared.appendFacts,
				prepared.removed,
				prepared.materializeEntry,
				{ removedHashes: prepared.removedHashes },
			);
			try {
				await this.completeNativeStrictDurableTransaction(
					nativeStrictTransaction,
				);
			} catch (error) {
				warn(`Failed to retire committed native intent: ${String(error)}`);
			}
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
		if (!backbone || !this.canUseNativeBackboneResidentCoordinateState()) {
			return undefined;
		}
		if (
			properties?.nativeBackboneDocumentDeleteKey &&
			!this.canUseBackboneOnlyCoordinatePersistence()
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
					const committedHash =
						backboneAppend.entry.cid ?? backboneAppend.entry.hash;
					const committedNext = backboneAppend.entry.next ?? next;
					nativeCoordinateRollback = this.snapshotResidentCoordinateEntries([
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
						this.snapshotResidentCoordinateEntries([
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
							await this.rollbackNativeBackboneCoordinateAppendDurably(
								prepared.appendFacts.hash,
								rollbackCoordinateEntries,
							);
						} catch (rollbackError) {
							rollbackFailures.push(rollbackError);
						}
						if (nativeDocumentRollback) {
							try {
								this.restoreNativeBackboneDocument(nativeDocumentRollback);
								const flushed = this.flushNativeBackboneCoordinateJournal();
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
							this.canUseBackboneOnlyCoordinatePersistence() ||
							coordinateIndex.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn ||
							coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn;
						const persisted = hasNativeCoordinatePut
							? this.persistBackboneCoordinateFieldsNativeTransaction({
									coordinateIndex,
									fields: coordinateFields,
									hash: prepared.appendFacts.hash,
									deleteHashes: [],
									coordinates: backboneAppend.coordinate
										.coordinates as NumberFromType<R>[],
									skipGenericTransientCoordinateIndex: runtimeOnlyCoordinates,
								})
							: this.persistPreparedCoordinate({
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
						await this.flushNativeBackboneCoordinateJournal();
						await prepared.nativeCommittedAppendFinalizer.acknowledge(() =>
							this.markNativeStrictDurableTransactionLowerMarker(
								nativeStrictTransaction,
							),
						);
					} catch (error) {
						return rollback(error);
					}
					this.applyPreparedAppendFactsWithDeferredCoordinateDeletes(
						prepared.appendFacts,
						prepared.removed,
						prepared.materializeEntry,
						{
							forgetNativeCoordinates: false,
							removedHashes: prepared.removedHashes,
						},
					);
					try {
						await this.completeNativeStrictDurableTransaction(
							nativeStrictTransaction,
						);
					} catch (error) {
						warn(`Failed to retire committed native intent: ${String(error)}`);
					}
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
							const pruneEntry = this.materializePreparedCoordinateEntry(
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
					this.deleteCoordinatesForHashes(
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
				!this.canUseNativeBackboneResidentCoordinateState()) ||
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
			appendCommit: nativePreparedCommit,
		};
	}

	private async processNativePreparedTargetNoneAppendTransaction(
		result: {
			entry?: Entry<T>;
			materializeEntry?: () => Entry<T>;
			removed: ShallowOrFullEntry<T>[];
			removedHashes?: string[];
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
							ownershipLifecycleController,
						},
					);
			await this.persistPreparedCoordinateNativeTransaction(
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
				await this.deleteCoordinatesForHashes(
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
				pruneEntry = this.materializePreparedCoordinateEntry(
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
			pruneEntry ??= this.materializePreparedCoordinateEntry(
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
					await this.deleteCoordinatesForHashes(
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
				await this.deleteCoordinatesForHashes(
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
			!this.canUseNativeBackboneResidentCoordinateState() ||
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
					batchCoordinateRollback = this.snapshotResidentCoordinateEntries(
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
				await this.rollbackNativeBackboneCoordinateAppendDurably(
					appended.appendFacts[0]?.hash ?? "",
					batchCoordinateRollback,
				);
			} catch (rollbackError) {
				rollbackFailures.push(rollbackError);
			}
			try {
				for (const document of batchDocumentRollbacks) {
					this.restoreNativeBackboneDocument(document);
				}
				const flushed = this.flushNativeBackboneCoordinateJournal();
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
				const persisted = this.persistBackboneCoordinateFieldsNativeTransaction(
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
			await this.flushNativeBackboneCoordinateJournal();
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
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		} catch (error) {
			return rollbackBatch(error);
		}

		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const appendCommits: PreparedLocalAppendCommit<R>[] = [];
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
				},
			);
			if (!runtimeOnlyCoordinates && this.remoteBlocks.hasNotifyStoredHook()) {
				this.remoteBlocks.notifyStoredDeferred(facts.hash);
			}
			const appendCommit = this.createPreparedLocalAppendCommitFromFacts(
				facts,
				{
					hashNumber: backboneAppend.coordinate.hashNumber as NumberFromType<R>,
					coordinateFields,
				},
			);
			appendCommit.nativeBackboneDocumentIndexCommitted = true;
			appendCommit.nativeBackboneDocumentIndexTrimmedHeadsProcessed =
				appended.documentTrimmedHeadsProcessed?.[i];
			appendCommit.documentPreviousContext =
				backboneAppend.documentPreviousContext;
			appendCommits.push(appendCommit);
		}
		try {
			await this.completeNativeStrictDurableTransaction(
				nativeStrictTransaction,
			);
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		} catch (error) {
			if (!this.isRepairLifecycleActive(ownershipLifecycleController)) {
				throw error;
			}
			warn(`Failed to retire committed native intent: ${String(error)}`);
		}
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
			await this.deleteCoordinatesForHashes(
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
		if (data.length === 0) {
			return { entries: [], removed: [] };
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
			await this.deleteCoordinatesForHashes(
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
		await this.deleteCoordinatesForHashes(
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
		await this.persistCoordinatesBatch(
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
			await this.getFullReplicaRepairCandidates(undefined, {
				includeSubscribers: false,
			});
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
			await this.getFullReplicaRepairCandidates(undefined, {
				includeSubscribers: false,
			});
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
			this.createCoordinatePersistenceEntryFromNativePlan({
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
			this.createCoordinatePersistenceEntryFromNativePlan({
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
				this.createCoordinatePersistenceEntryFromNativePlan({
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
			await this.getFullReplicaRepairCandidates(undefined, {
				includeSubscribers: false,
			});
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
				this.createCoordinatePersistenceEntryFromNativePlan({
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
			await this.deleteCoordinatesForHashes(
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
				await this.persistPreparedCoordinate(
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
				await this.persistCoordinate(
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
			payloadSize: entry.payload.byteLength,
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
			payloadSize: appendFacts.payloadSize,
			metaBytes: appendFacts.metaBytes,
			hashNumber: nativeAppendPlan?.hashNumber,
			coordinateFields:
				nativeAppendPlan?.coordinateFields ??
				nativeAppendPlan?.preparedCoordinate?.fields,
		};
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
		this.ensureNativeDurabilityRuntimeState();
		this._nativeStrictDurableTransactionsClosing = false;
		this._replicationRangeMutationsClosing = false;
		this._checkedPruneRemoveBlocksLocalRangeMutationAdmission = 0;
		this._checkedPruneRemovalCallbackInvocationDepth = 0;
		this._pruneRemovesClosing = false;
		this._replicationRangeMutationFailure = undefined;
		this.startRepairLifecycle();
		this._replicationRangeMutationTail = Promise.resolve();
		this.resetSubscriptionChangeCallbackTracking();
		const recoveringNativeDurableFailure =
			this._nativeDurableCommitFailure !== undefined;
		options = applySharedLogNativeDefaults(
			options,
			(this.node as unknown as NodeWithSharedLogNativeDefaults)
				.sharedLogNativeDefaults,
		);
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

		this.domain = options?.domain
			? (options.domain(this) as unknown as D)
			: (createReplicationDomainHash(
					options?.compatibility && options?.compatibility < 10 ? "u32" : "u64",
				)(this) as unknown as D);
		this.indexableDomain = createIndexableDomainFromResolution(
			this.domain.resolution,
		);
		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 2e4;
		this._checkedPrune = new CheckedPruneCoordinator<T, R>();
		this._admittedPruneRemoves = new Set();
		this._pendingIHave = new Map();
		this._pendingIHaveCallbacks = new Set();
		this.latestReplicationInfoMessage = new Map();
		this._replicationInfoBlockedPeers = new Set();
		this._replicationInfoRequestByPeer = new Map();
		// Terminal close/drop drains the previous lifecycle before another open can
		// install fresh lanes and opaque per-subscription ownership tokens.
		this._replicationInfoApplyQueueByPeer = new Map();
		this._replicationInfoReceiveEpochByPeer = new Map();
		this._subscriptionEpochByPeer = new Map();
		this._pendingReplicatorLeaveByPeer = new Set();
		this._activeReceiveHandlersByPeer = new Map();
		this._receiveHandlerDrainByPeer = new Map();
		this._receiveCleanupGateByPeer = new Map();
		this._subscriptionOpeningEpochByPeer = new Map();
		this._openingSyncCapabilitiesByPeer = new Map();
		this._repairRetryTimers = new Set();
		this._recentRepairDispatch = new Map();
		this._repairSweepRunning = false;
		this._repairSweepPendingModes = new Set();
		this._repairSweepPendingPeersByMode = createRepairPendingPeersByMode();
		this._repairSweepJoinWarmupGenerationByTarget = new Map();
		this._repairFrontierByMode = createRepairFrontierByMode() as Map<
			RepairDispatchMode,
			Map<string, Map<string, RepairDispatchEntry<R>>>
		>;
		this._repairFrontierActiveTargetsByMode = createRepairActiveTargetsByMode();
		this._repairFrontierBypassKnownPeersByMode =
			createRepairFrontierBypassKnownPeersByMode();
		this._joinWarmupGenerationByTarget = new Map();
		this._joinWarmupSendStateByTarget = new Map();
		this._joinWarmupRetryTimersByTarget = new Map();
		this._joinWarmupScheduledRetriesByTarget = new Map();
		this._repairSweepOptimisticGidPeersPending = new Map();
		this._repairSweepOptimisticGidsByPeer = new Map();
		this._entryKnownPeers = new Map();
		this._entryKnownPeerObservedAt = new Map();
		this._joinAuthoritativeRepairTimersByDelay = new Map();
		this._joinAuthoritativeRepairPeersByDelay = new Map();
		this._assumeSyncedRepairSuppressedUntil = 0;
		this._appendBackfillTimer = undefined;
		this._appendBackfillPendingByTarget = new Map();
		this._repairMetrics = createRepairMetrics();
		this._topicSubscribersCache = new Map();
		this._leaderSelectionContextCache = undefined;
		this._peerSyncCapabilities = new Map();
		this._liveRawGossipBatches = new Map();
		this._liveRawGossipFlushScheduled = false;
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
		this._replicationAnnouncementRetryPending = false;
		this._replicationAnnouncementRetryGeneration = 0;
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
		this.setupReplicationAnnouncementRetryFunction();
		this.setupReplicationAnnouncementRepairFunction();
		this._closeController.signal.addEventListener("abort", () => {
			for (const [_peer, state] of this._replicationInfoRequestByPeer) {
				if (state.timer) clearTimeout(state.timer);
			}
			this._replicationInfoRequestByPeer.clear();
		});
		const invalidateLeaderSelectionContext = () =>
			this.invalidateLeaderSelectionContextCache();
		this.events.addEventListener(
			"replication:change",
			invalidateLeaderSelectionContext,
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
				"replicator:mature",
				invalidateLeaderSelectionContext,
			);
			this.invalidateLeaderSelectionContextCache();
		});

		this._isTrustedReplicator = options?.canReplicate;
		this.keep = options?.keep;
		this.pendingMaturity = new Map();

		const id = sha256Base64Sync(this.log.id);
		const [storage, logScope] = await Promise.all([
			this.node.storage.sublevel(id),
			this.node.indexer.scope(id),
		]);

		const fanoutService = getSharedLogFanoutService(this.node.services);
		const blockProviderNamespace = (cid: string) => `cid:${cid}`;
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
			onPut: fanoutService
				? async (cid) => {
						// Best-effort directory announce for "get without remote.from" workflows.
						try {
							await fanoutService.announceProvider(
								blockProviderNamespace(cid),
								{
									ttlMs: 120_000,
									bootstrapMaxPeers: 2,
								},
							);
						} catch {
							// ignore announce failures
						}
					}
				: undefined,
		});

		const remoteBlocksStartPromise = this.remoteBlocks.start();
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
		let pruneDebouncedFn!: typeof this.pruneDebouncedFn;
		const isPruneDebounceCurrent = () =>
			this.isRepairLifecycleActive(pruneOwnershipLifecycleController) &&
			this._checkedPrune === checkedPruneCoordinator &&
			this.pruneDebouncedFn === pruneDebouncedFn;
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
						}
					>();
					const selfReplicating = await this.isReplicating();
					if (!isPruneDebounceCurrent()) {
						return;
					}
					for (const [hash, value] of map) {
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
						if (checkedPruneLeaders.localLeader) {
							const preserveRetry = checkedPruneCoordinator.hasRetry(hash);
							await this.cancelCheckedPruneForLocalLeader(hash, {
								preserveRetry,
							});
							if (!isPruneDebounceCurrent()) {
								return;
							}
							if (preserveRetry) {
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
						this.prune(current, undefined, pruneOwnershipLifecycleController);
					}
				} catch (error) {
					if (isPruneDebounceCurrent() && !isNotStartedError(error as Error)) {
						logger.error(error);
					}
				}
			},
			PRUNE_DEBOUNCE_INTERVAL,
			(into, from) => {
				for (const [k, v] of from.leaders) {
					if (!into.leaders.has(k)) {
						into.leaders.set(k, v);
					}
				}
			},
		);
		this.pruneDebouncedFn = pruneDebouncedFn;

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

				if (hashes.length > 0 && allRequestingPeers.size > 0) {
					this.rpc
						.send(new ResponseIPrune({ hashes }), {
							mode: new AcknowledgeDelivery({
								to: allRequestingPeers,
								redundancy: 1,
							}),
							priority: CONVERGENCE_MESSAGE_PRIORITY,
						})
						.catch(() => {});
				}
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
					size: () => accumulator.size,
					clear: () => accumulator.clear(),
					value: accumulator,
					has: (hash: string) => accumulator.has(hash),
				};
			},
			PRUNE_DEBOUNCE_INTERVAL,
		);

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
		const resolveHashesForSymbols = (
			symbols: readonly bigint[] | BigUint64Array,
		) => {
			const nativeState = this._nativeBackbone ?? this._nativeSharedLogState;
			if (!nativeState) {
				return undefined;
			}
			if (
				typeof BigUint64Array !== "undefined" &&
				typeof nativeState.getEntryHashesForHashNumbersU64 === "function"
			) {
				return nativeState.getEntryHashesForHashNumbersU64(
					symbols instanceof BigUint64Array
						? symbols
						: BigUint64Array.from(symbols),
				);
			}
			return nativeState.getEntryHashesForHashNumbers(symbols);
		};
		const resolveHashListForSymbols = (
			symbols: readonly bigint[] | BigUint64Array,
		) => {
			const nativeState = this._nativeBackbone ?? this._nativeSharedLogState;
			if (
				!nativeState ||
				typeof BigUint64Array === "undefined" ||
				typeof nativeState.getEntryHashListForHashNumbersU64 !== "function"
			) {
				return undefined;
			}
			return nativeState.getEntryHashListForHashNumbersU64(
				symbols instanceof BigUint64Array
					? symbols
					: BigUint64Array.from(symbols),
			);
		};
		const resolveHashNumbersInRange = (range: {
			start1: bigint | number;
			end1: bigint | number;
			start2: bigint | number;
			end2: bigint | number;
		}) => {
			const nativeState = this._nativeBackbone ?? this._nativeSharedLogState;
			return (
				nativeState?.getEntryHashNumbersInRangeU64?.(range) ??
				nativeState?.getEntryHashNumbersInRange(range)
			);
		};

		const sendRawExchangeHeads = (
			hashes: string[],
			to: string[],
			sendOptions?: { priority?: number; signal?: AbortSignal },
		) => this.trySendFusedRawExchangeHeads(hashes, to, sendOptions);
		if (options?.syncronizer) {
			this.syncronizer = new options.syncronizer({
				numbers: this.indexableDomain.numbers,
				entryIndex: this.entryCoordinatesIndex,
				log: this.log,
				rangeIndex: this._replicationRangeIndex,
				rpc: this.rpc,
				coordinateToHash: this.coordinateToHash,
				resolveHashesForSymbols,
				resolveHashListForSymbols,
				resolveHashNumbersInRange,
				sync: options?.sync,
				isEntryRecentlyKnownByPeer: (hash, peer, maxAgeMs) =>
					this.isEntryRecentlyKnownByPeer(hash, peer, maxAgeMs),
				sendRawExchangeHeads,
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
					resolveHashListForSymbols,
					sync: options?.sync,
					isEntryRecentlyKnownByPeer: (hash, peer, maxAgeMs) =>
						this.isEntryRecentlyKnownByPeer(hash, peer, maxAgeMs),
					sendRawExchangeHeads,
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
					resolveHashListForSymbols,
					resolveHashNumbersInRange,
					sync: options?.sync,
					isEntryRecentlyKnownByPeer: (hash, peer, maxAgeMs) =>
						this.isEntryRecentlyKnownByPeer(hash, peer, maxAgeMs),
					sendRawExchangeHeads,
				}) as Syncronizer<R>;
			}
		}

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
			void this.rebalanceParticipationDebounced?.call();
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
		if (this._nativeBackboneCoordinatePersistence) {
			// A previous explicit drop may have been interrupted after its durable
			// tombstone was written. Complete that erase before the adapter can expose
			// any stale coordinate or document state to this backbone.
			await this._nativeBackboneCoordinatePersistence.resumeDrop?.();
			await this._nativeBackboneCoordinatePersistence.hydrate(backbone);
			this._nativeBackboneCoordinateJournalLastFlushMs = Date.now();
			this.hydrateNativeCoordinateStateFromBackbone(backbone);
			return;
		}
		this._residentEntryCoordinatesByHash ??= new Map();
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
					this._residentEntryCoordinatesByHash.set(
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
		const hashes = new Set(this._residentEntryCoordinatesByHash?.keys() ?? []);
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
			this._residentEntryCoordinatesByHash?.delete(hash);
		}
		const flushed = this.flushNativeBackboneCoordinateJournal();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
	}

	private hydrateNativeCoordinateStateFromBackbone(
		backbone: NativePeerbitBackbone,
	): void {
		const fields = backbone.getEntryCoordinateFields();
		this._nativeSharedLogState?.clearEntryCoordinates();
		this._residentEntryCoordinatesByHash = new Map();
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
			this._residentEntryCoordinatesByHash.set(sharedFields.hash, sharedFields);
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
		this._nativeBackboneCoordinatePersistence = undefined;
		this._nativeBackboneCoordinateJournalLastFlushMs = 0;
		this._residentEntryCoordinatesByHash = undefined;
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

	private async openNativeBackbone(
		options: SharedLogOptions<T, D, R>["nativeBackbone"],
	): Promise<NativePeerbitBackbone | undefined> {
		this._nativeBackboneCoordinatePersistence = undefined;
		this._nativeBackboneCoordinatePersistenceStore = undefined;
		this._nativeBackboneDropStarted = false;
		this._nativeBackboneCoordinateJournalLastFlushMs = 0;
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
				this._nativeBackboneCoordinatePersistence =
					createNativeBackboneCoordinatePersistence(
						options.coordinatePersistence as RuntimeNativeBackboneCoordinatePersistenceConfig,
					);
			} else {
				this._nativeBackboneCoordinatePersistence =
					await this.createAutoDerivedCoordinatePersistence(
						nativeBackboneModule,
					);
			}
			if (
				this.node.directory != null &&
				this._nativeBackboneCoordinatePersistence
			) {
				if (
					this._nativeBackboneCoordinatePersistence.durableBarrier !== true ||
					typeof this._nativeBackboneCoordinatePersistenceStore
						?.durableBarrier !== "function"
				) {
					throw new Error(
						"Durable nativeBackbone coordinate persistence requires an explicit physical durability barrier",
					);
				}
			}
			if (
				this._nativeBackboneCoordinatePersistence &&
				(this._nativeBackboneCoordinatePersistence.compactMaxJournalBytes !=
					null ||
					this._nativeBackboneCoordinatePersistence.compactMaxJournalRecords !=
						null) &&
				this._nativeBackboneCoordinatePersistence.crashSafeCompaction !== true
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
		return this.withReplicationRangeMutationQueue(async () => {
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
		const existingSubscribersPromise = this._getTopicSubscribers(this.topic);
		const replicationLifecycleController =
			this._replicationLifecycleController;

		// We do this here, because these calls requires this.closed == false
		void this.pruneOfflineReplicators()
			.then(() => {
				if (
					this.isReplicationLifecycleActive(replicationLifecycleController)
				) {
					this._replicatorsReconciled = true;
				}
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
			this._replicationLifecycleController;
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

			while (!iterator.done()) {
				const segments = await iterator.next(1000);
				if (!this.isReplicationLifecycleActive(replicationLifecycleController)) {
					return;
				}
				for (const segment of segments) {
					if (
						checkedIsAlive.has(segment.value.hash) ||
						this.node.identity.publicKey.hashcode() === segment.value.hash
					) {
						this.uniqueReplicators.add(this.node.identity.publicKey.hashcode());
						continue;
					}

					checkedIsAlive.add(segment.value.hash);
					const peerHash = segment.value.hash;
					const subscriptionEpoch = this.getSubscriptionEpoch(peerHash);

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
										"Failed to resolve public key from hash: " +
											peerHash,
									);
								}

								const keyHash = key.hashcode();
								if (keyHash !== peerHash) {
									return;
								}
								return this.withReplicationInfoApplyQueue(
									keyHash,
									async () => {
										// A successful reachability check may legitimately span a
										// subscribe event during startup. The current lane's blocked
										// state plus an extant index row are the authoritative guard;
										// only the destructive catch path remains tied to the old token.
										if (
											!this.isReplicationLifecycleActive(
												replicationLifecycleController,
											) ||
											this.closed ||
											this._replicationInfoBlockedPeers.has(keyHash)
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
											this._replicationInfoBlockedPeers.has(keyHash)
										) {
											return;
										}
										this.uniqueReplicators.add(keyHash);

										if (!this._replicatorJoinEmitted.has(keyHash)) {
											this._replicatorJoinEmitted.add(keyHash);
											this.events.dispatchEvent(
												new CustomEvent<ReplicatorJoinEvent>(
													"replicator:join",
													{ detail: { publicKey: key } },
												),
											);
											this.events.dispatchEvent(
												new CustomEvent<ReplicationChangeEvent>(
													"replication:change",
													{ detail: { publicKey: key } },
												),
											);
										}
									},
								);
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
				!this.isReplicationLifecycleActive(
					replicationLifecycleController,
				)
			) {
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
		if (
			this._replicatorLivenessCursor >= this._replicatorLivenessTargets.length
		) {
			this._replicatorLivenessCursor = 0;
		}
	}

	private getReplicatorLivenessTargets() {
		const selfHash = this.node.identity.publicKey.hashcode();
		const expected =
			this.uniqueReplicators.size -
			(this.uniqueReplicators.has(selfHash) ? 1 : 0);

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
		this._peerSyncCapabilities.delete(peerHash);
		this.cleanupPendingIHavePeer(peerHash);
		this._checkedPrune.cleanupPeer(peerHash);
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

	private markReplicatorActivity(peerHash: string, now = Date.now()) {
		this._replicatorLastActivityAt.set(peerHash, now);
		// Any recent authenticated activity is positive liveness evidence. Reset the
		// consecutive miss streak immediately, including while an eviction is
		// waiting in the per-peer mutation lane.
		if (Date.now() - now < REPLICATOR_LIVENESS_IDLE_THRESHOLD_MS) {
			this._replicatorLivenessFailures.delete(peerHash);
		}
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

	private advanceReplicationInfoRecoveryEpoch(peerHash: string) {
		// Handlers admitted before a successful peer removal must not restore state
		// when they eventually reach the apply lane. Reset the sender's
		// ordering watermark with the local epoch so a later arrival can be
		// accepted without comparing its clock to this receiver's clock.
		this.advanceReplicationInfoReceiveEpoch(peerHash);
		this.latestReplicationInfoMessage.delete(peerHash);
	}

	private async evictReplicatorFromLiveness(
		peerHash: string,
		publicKey: PublicSignKey,
		replicationLifecycleController: AbortController,
		subscriptionEpoch: object | null,
		observedActivityAt: number | undefined,
	) {
		try {
			await this.removeReplicator(publicKey, {
				noEvent: true,
				replicationLifecycleController,
				shouldRemove: () =>
					this._replicatorLastActivityAt.get(peerHash) === observedActivityAt,
				subscriptionEpoch,
				onRemoved: ({ wasReplicator }) => {
					if (wasReplicator) {
						this._pendingReplicatorLeaveByPeer.add(peerHash);
					}
					// A newer subscription/lifecycle may have started while the admitted
					// removal was completing. Its reconnect barrier owns all later effects.
					if (
						!this.isReplicationLifecycleActive(
							replicationLifecycleController,
						) ||
						!this.isCurrentSubscriptionEpoch(peerHash, subscriptionEpoch)
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

					if (!this._replicationInfoBlockedPeers.has(peerHash)) {
						this.scheduleReplicationInfoRequests(
							publicKey,
							replicationLifecycleController,
						);
					}
					this._replicatorLivenessTargetsSize = -1;
				},
			});
		} catch (error) {
			if (!isNotStartedError(error as Error)) {
				throw error;
			}
		}
	}

	private async resolveCandidatePeersForHash(
		hash: string,
		options?: { signal?: AbortSignal; maxPeers?: number },
	): Promise<string[] | undefined> {
		if (options?.signal?.aborted) return undefined;

		const maxPeers = options?.maxPeers ?? 8;
		const self = this.node.identity.publicKey.hashcode();
		const seed = hashToSeed32(hash);

		const hinted = this._checkedPrune.getConfirmedReplicators(hash);
		if (hinted && hinted.size > 0) {
			const peers = [...hinted].filter((p) => p !== self);
			return peers.length > 0
				? pickDeterministicSubset(peers, seed, maxPeers)
				: undefined;
		}

		const contacted = this._checkedPrune.getContactedReplicators(hash);
		if (contacted && contacted.size > 0) {
			const peers = [...contacted].filter((p) => p !== self);
			return peers.length > 0
				? pickDeterministicSubset(peers, seed, maxPeers)
				: undefined;
		}

		let candidates: string[] | undefined;
		const replicatorCandidates = [...this.uniqueReplicators].filter(
			(p) => p !== self,
		);
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
		const replicationLifecycleController =
			this._replicationLifecycleController;
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
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
			if (
				this._replicationLifecycleController ===
				replicationLifecycleController
			) {
				this._replicatorLivenessSweepRunning = false;
			}
		}
	}

	private async probeReplicatorLiveness(peerHash: string) {
		const replicationLifecycleController =
			this._replicationLifecycleController;
		if (
			this.closed ||
			this._closeController.signal.aborted ||
			!replicationLifecycleController ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}
		const subscriptionEpoch = this.getSubscriptionEpoch(peerHash);
		const ownsProbe = () =>
			this.isReplicationLifecycleActive(replicationLifecycleController) &&
			this.isCurrentSubscriptionEpoch(peerHash, subscriptionEpoch);
		if (!this.uniqueReplicators.has(peerHash)) {
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}
		const observedActivityAt = this._replicatorLastActivityAt.get(peerHash);

		const publicKey = await this._resolvePublicKeyFromHash(peerHash);
		if (!ownsProbe()) {
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}
		if (!publicKey) {
			try {
				await this.removeReplicator(peerHash, {
					noEvent: true,
					replicationLifecycleController,
					shouldRemove: () =>
						this._replicatorLastActivityAt.get(peerHash) ===
						observedActivityAt,
					subscriptionEpoch,
					onRemoved: () => {
						if (!ownsProbe()) {
							return;
						}
						this._replicatorLivenessTargetsSize = -1;
					},
				});
			} catch (error) {
				if (!isNotStartedError(error as Error)) {
					throw error;
				}
			}
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
			if (!ownsProbe()) {
				return;
			}
			this.markReplicatorActivity(peerHash);
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return;
			}
		}
		if (!ownsProbe()) {
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}

		// Relay-backed prod paths can keep a peer subscribed/reachable even if an
		// ACKed liveness ping gets delayed or dropped under load. Treat observed
		// topic presence as a positive liveness signal before evicting the peer.
		if (await this.confirmReplicatorSubscriberPresence(peerHash)) {
			if (!ownsProbe()) {
				return;
			}
			this.markReplicatorActivity(peerHash);
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}
		if (!ownsProbe()) {
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}

		const failures = (this._replicatorLivenessFailures.get(peerHash) ?? 0) + 1;
		this._replicatorLivenessFailures.set(peerHash, failures);
		this.scheduleReplicationInfoRequests(
			publicKey,
			replicationLifecycleController,
		);

		if (failures < REPLICATOR_LIVENESS_PROBE_FAILURES_TO_EVICT) {
			return;
		}
		if (!ownsProbe() || !this.uniqueReplicators.has(peerHash)) {
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}

		await this.evictReplicatorFromLiveness(
			peerHash,
			publicKey,
			replicationLifecycleController,
			subscriptionEpoch,
			observedActivityAt,
		);
	}

	private async confirmReplicatorSubscriberPresence(peerHash: string) {
		try {
			const subscribers = await this._getTopicSubscribers(this.rpc.topic);
			if (
				subscribers?.some((subscriber) => subscriber.hashcode() === peerHash)
			) {
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
		if (options?.forgetNativeCoordinates === false) {
			this.forgetResidentCoordinateStateForHashes(
				deferredCoordinateDeleteHashes,
			);
		} else {
			this.forgetCoordinateStateForHashes(deferredCoordinateDeleteHashes);
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
		if (options?.forgetNativeCoordinates === false) {
			this.forgetResidentCoordinateStateForHashes(
				deferredCoordinateDeleteHashes,
			);
		} else {
			this.forgetCoordinateStateForHashes(deferredCoordinateDeleteHashes);
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
			await this.deleteCoordinates(
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
		return undefined;
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

			// Locally-created entries were signed before append.
			if (
				!entry.createdLocally &&
				!hasPreverifiedSignature(entry) &&
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
		captureSync(() => this.cancelCurrentReplicationStateAnnouncementRetry());
		this.replicationAnnouncementRetryDebounced = undefined;
		captureSync(() => {
			if (this._wireSyncSession) {
				this._wireSyncSession.unregisterTopic(this.topic);
				this._wireSyncSession = undefined;
			}
		});
		await capture(() => this.closeNativeBackboneCoordinatePersistence());
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
			this._closeController.abort();
			clearInterval(this.interval);
			this.stopReplicatorLivenessSweep();
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
			this.cancelAllJoinWarmupTargets();
			for (const timer of this._repairRetryTimers ?? []) clearTimeout(timer);
			this._repairRetryTimers?.clear();
			this._recentRepairDispatch?.clear();
			this._repairSweepRunning = false;
			this._repairSweepPendingModes?.clear();
			for (const peers of this._repairSweepPendingPeersByMode?.values() ?? [])
				peers.clear();
			this._repairSweepJoinWarmupGenerationByTarget?.clear();
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
			this.latestReplicationInfoMessage?.clear();
			this._replicationInfoReceiveEpochByPeer?.clear();
			this._activeReceiveHandlersByPeer?.clear();
			this._receiveHandlerDrainByPeer?.clear();
			this._receiveCleanupGateByPeer?.clear();
			this._subscriptionOpeningEpochByPeer?.clear();
			this._openingSyncCapabilitiesByPeer?.clear();
			this._gidPeersHistory?.clear();
			this._peerSyncCapabilities?.clear();
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
		captureSync(() => this.responseToPruneDebouncedFn?.close?.());
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
		this._residentEntryCoordinatesByHash = undefined;
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
		const replicationRangeTerminalFence =
			this.acquireReplicationRangeMutationTerminalFence();
		const pruneRemoveTerminalFence = this.acquirePruneRemoveTerminalFence();
		try {
			this.stopSubscriptionChangeCallbackAdmission();
			this.cancelAllJoinWarmupTargets();
			await this.drainSubscriptionChangeCallbacks();
			// An already-admitted subscription callback can create a fresh warmup
			// generation while the first cancellation is draining.
			this.cancelAllJoinWarmupTargets();
			await this.drainReceiveHandlers();
			await this.drainReplicationInfoApplyQueues();
			await replicationRangeTerminalFence.drained;
			await pruneRemoveTerminalFence.drained;
			await this.drainPendingIHaveCallbacks();
			this.ensureNativeDurabilityRuntimeState();
			this.cancelCurrentReplicationStateAnnouncementRetry();
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
							new TimeoutError("shared-log close replication reset timed out"),
						);
					} catch {
						abort.abort();
					}
				}, 2_000);
				try {
					await this.rpc
						.send(new AllReplicatingSegmentsMessage({ segments: [] }), {
							priority: CONVERGENCE_MESSAGE_PRIORITY,
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
		const nativePersistence = this._nativeBackboneCoordinatePersistence;
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
		const replicationRangeTerminalFence =
			this.acquireReplicationRangeMutationTerminalFence();
		const pruneRemoveTerminalFence = this.acquirePruneRemoveTerminalFence();
		try {
			this.stopSubscriptionChangeCallbackAdmission();
			this.cancelAllJoinWarmupTargets();
			await this.drainSubscriptionChangeCallbacks();
			// An already-admitted subscription callback can create a fresh warmup
			// generation while the first cancellation is draining.
			this.cancelAllJoinWarmupTargets();
			await this.drainReceiveHandlers();
			await this.drainReplicationInfoApplyQueues();
			await replicationRangeTerminalFence.drained;
			await pruneRemoveTerminalFence.drained;
			await this.drainPendingIHaveCallbacks();
			this.cancelCurrentReplicationStateAnnouncementRetry();
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
				this.responseToPruneDebouncedFn?.close?.();

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
					await this.rpc
						.send(new AllReplicatingSegmentsMessage({ segments: [] }), {
							priority: CONVERGENCE_MESSAGE_PRIORITY,
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

	// Callback for receiving a message from the network
	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<void> {
		const stashBackedRawMessage = isStashBackedRawExchangeHeadsMessage(msg)
			? msg
			: undefined;
		let releasePeerReceiveLease: (() => void) | undefined;
		try {
			this.throwIfNativeDurableCommitFailed();
			if (!context.from) {
				throw new Error("Missing from in update role message");
			}
			// Snapshot receive ownership before any async handler gets a chance to
			// yield. Replication-info messages reach their branch only after the
			// synchronizer declines them, and a U/S transition can happen meanwhile.
			const receiveFromHash = context.from.hashcode();
			const receiveReplicationLifecycleController =
				this._replicationLifecycleController;
			const receiveSubscriptionEpoch =
				this.getSubscriptionEpoch(receiveFromHash);
			const isOpeningSubscriptionReceive =
				this._subscriptionOpeningEpochByPeer.get(receiveFromHash) ===
				receiveSubscriptionEpoch;
			const isOpeningCapabilityAdvertisement =
				msg instanceof SyncCapabilitiesMessage && isOpeningSubscriptionReceive;
			releasePeerReceiveLease = this.acquirePeerReceiveLease(
				receiveFromHash,
				receiveReplicationLifecycleController,
				receiveSubscriptionEpoch,
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
			const receiveReplicationInfoReceiveEpoch =
				this.getReplicationInfoReceiveEpoch(receiveFromHash);
			if (msg instanceof ResponseRoleMessage) {
				msg = msg.toReplicationInfoMessage(); // migration
			}
			if (
				msg instanceof AllReplicatingSegmentsMessage ||
				msg instanceof AddedReplicationSegmentMessage
			) {
				// Bound decoded untrusted vectors before per-peer/global mutation
				// queues, trusted-replicator authorization, or liveness side effects.
				this.validateReplicationRangeAnnouncement(msg.segments);
			} else if (msg instanceof StoppedReplicating) {
				// Bound the raw decoded vector before deduplication can hide the
				// allocation cost, and before liveness or apply-queue side effects.
				this.validateStoppedReplicationAnnouncement(msg.segmentIds);
			}
			if (!context.from.equals(this.node.identity.publicKey)) {
				this.markReplicatorActivity(receiveFromHash);
			}

			const syncProfile = this._logProperties?.sync?.profile;
			let rawMaterializedKnownMissing = false;
			let rawPreparedReceiveSelectionValue:
				| NativeBackboneRawReceiveSelectionPlan
				| undefined;
			if (msg instanceof RawExchangeHeadsMessage) {
				const rawFrom = context.from!;
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
					return;
				}
				const rawIsRepairHint =
					(msg.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) !== 0;
				const rawPrepareVerifySetting =
					this._logProperties?.sync
						?.rawExchangeHeadsVerifySignaturesDuringPrepare;
				// A program-level canAppend hook must observe every entry before
				// it commits, so the native join commit (which validates and
				// commits entirely in wasm) is not used for programs that
				// register one; those joins run through the lower-log batch
				// join where the hook fires per entry.
				const programCanAppend = !!this._logProperties?.canAppend;
				const canVerifyPreparedRawReceiveOnCommit =
					!programCanAppend &&
					!!this._nativeBackbone?.graph
						.commitVerifiedPreparedRawReceiveJoinBatch;
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
					!!this._nativeBackbone?.graph
						.commitVerifiedPreparedRawReceiveJoinBatch;
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
					return;
				}
				msg = materializedRawMessage;
				rawMaterializedKnownMissing = true;
				if (syncProfile) {
					emitSyncProfileDuration(syncProfile, rawMaterializeStartedAt, {
						name: "sharedLog.rawReceive.materialize",
						component: "shared-log",
						entries: rawMissingHeads.length,
						bytes: rawMissingBytes,
						messages: 1,
					});
				}
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
								if (group.fromIsLeader) {
									this.addPeersToGidPeerHistory(group.gid, contextFromHashes);
								}
								for (const entry of group.entries) {
									const hash = getExchangeHeadHash(entry);
									cleanupHashes.push(hash);
									this.pruneDebouncedFn.delete(hash);
									toMerge.push(entry);
									toPersist.push(getReceiveHeadShallowOrEntry(entry));
								}
							}
							this.removePruneRequestsSent(cleanupHashes);
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
								let maybeDelete: EntryWithRefs<any>[][] | undefined;
								const toMerge: EntryWithRefs<any>[] = [];
								const toPersist: ShallowOrFullEntry<any>[] = [];
								if (isReplicating && group.isLeader === true) {
									immediateReplicatingLeaderPlanHits++;
								}
								if (keepAsLeader) {
									for (const entry of group.entries) {
										const hash = getExchangeHeadHash(entry);
										this.pruneDebouncedFn.delete(hash);
										this.removePruneRequestSent(hash);
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
												onLeader: (key) => {
													isLeader =
														isLeader ||
														this.node.identity.publicKey.hashcode() === key;
													fromIsLeader =
														fromIsLeader || contextFromHash === key;
												},
											},
										);
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
										this.pruneDebouncedFn.delete(hash);
										this.removePruneRequestSent(hash);
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
						this.createReusableReceiveCoordinatePlans(receiveGroups, {
							decodedReplicaCounts: receiveReplicaCounts,
							allowRoleAgeZeroPlans:
								immediateReplicatingLeaderPlans !== undefined,
						});
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
							? this.createBackboneOnlyReceiveCoordinateBatch(
									reusableCoordinatePersistItems,
								)
							: undefined;
						const nativePreparedJoinCommit = canUsePreparedAppendFacts
							? this.createNativeBackbonePreparedJoinCommit(
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
									await this.finishBackboneOnlyReceiveCoordinateBatch(
										nativePreparedCoordinateBatch,
										syncProfile,
									);
								nativePreparedCoordinatesFinished = true;
							} catch (error) {
								this.rollbackBackboneOnlyReceiveCoordinateBatch(
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
						const joinedPreparedFacts =
							canUsePreparedAppendFacts &&
							(await trustedLowerLog.joinPreparedAppendFactsBatch(
								preparedAppendFacts,
								{
									__peerbitEntriesAlreadyMissing: true,
									__peerbitCanAppendAlreadyValidated: true,
									__peerbitDeferIndexWrite: true,
									__peerbitOnAppendHashes: joinOnAppendHashes,
									__peerbitProfile: syncProfile,
									__peerbitNativePreparedJoinCommit: nativePreparedJoinCommit,
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
									await this.finishBackboneOnlyReceiveCoordinateBatch(
										nativePreparedCoordinateBatch,
										syncProfile,
									);
							} catch (error) {
								this.rollbackBackboneOnlyReceiveCoordinateBatch(
									nativePreparedCoordinateBatch,
								);
								throw error;
							}
						} else {
							nativeBackboneOnlyPersistedHashes =
								await this.persistBackboneOnlyReceiveCoordinateBatch(
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
							await this.persistCoordinatesBatch(
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
							emitSyncProfileDuration(syncProfile, coordinatePersistStartedAt, {
								name: "sharedLog.receive.coordinatePersist",
								component: "shared-log",
								entries: entriesToPersist.length,
								messages: 1,
								details: {
									reusedLeaderPlans: reusableCoordinatePersistItemCount,
									nativeBackboneOnly:
										nativeBackboneOnlyPersistedHashes?.size ?? 0,
								},
							});
						}
						for (const hash of admittedHashes) {
							confirmedHashes.add(hash);
						}
						const checkedPruneStartedAt = syncProfileStart(syncProfile);
						await this.pruneJoinedEntriesNoLongerLed(admittedShallowEntries, {
							decodedReplicaCounts: receiveReplicaCounts,
							reusableLeaderPlans: reusableCoordinatePlans,
							profile: syncProfile,
						});
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
			} else if (msg instanceof RequestIPrune) {
				const requestPruneStartedAt = syncProfileStart(syncProfile);
				const from = context.from.hashcode();
				const coordinatorCleanupStartedAt = syncProfileStart(syncProfile);
				this.removeEntriesKnownByPeer(msg.hashes, from);
				this.removePruneRequestsSent(msg.hashes, from);
				this._checkedPrune.removeConfirmedReplicators(msg.hashes, from);
				if (syncProfile) {
					emitSyncProfileDuration(syncProfile, coordinatorCleanupStartedAt, {
						name: "sharedLog.receive.requestPrune.coordinatorCleanup",
						component: "shared-log",
						entries: msg.hashes.length,
						messages: 1,
						details: { hashes: msg.hashes.length },
					});
				}
				let nativeEntryMetadata:
					| Array<
							| { gid: string; data?: Uint8Array; replicas?: number }
							| undefined
							| null
					  >
					| undefined;
				let presentBlocks: boolean[] | undefined;
				const nativeBackbonePlanStartedAt = syncProfileStart(syncProfile);
				let nativeLeaderHints =
					await this.planCurrentNativeBackboneRequestPruneLeaderHints(
						msg.hashes,
						from,
					);
				if (nativeLeaderHints) {
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, nativeBackbonePlanStartedAt, {
							name: "sharedLog.receive.requestPrune.nativeBackbonePlan",
							component: "shared-log",
							entries: msg.hashes.length,
							messages: 1,
							details: {
								nativeEntries:
									(nativeLeaderHints.nativeAllConfirmed
										? msg.hashes.length
										: undefined) ??
									nativeLeaderHints.nativeEntries?.size ??
									countPresentValues(
										nativeLeaderHints.nativeEntryGids ??
											nativeLeaderHints.nativeEntryMetadata,
									) ??
									0,
								presentBlocks:
									(nativeLeaderHints.nativeAllConfirmed
										? msg.hashes.length
										: undefined) ??
									nativeLeaderHints.presentBlockHashes?.size ??
									countTruthyValues(nativeLeaderHints.presentBlocks) ??
									0,
								localLeaders: nativeLeaderHints.nativeAllConfirmed
									? msg.hashes.length
									: nativeLeaderHints.localLeaderHashes.size ||
										countTruthyValues(nativeLeaderHints.localLeaderFlags) ||
										0,
								plannedEntries: nativeLeaderHints.nativeAllConfirmed
									? msg.hashes.length
									: nativeLeaderHints.replicaCounts.size ||
										countPositiveValues(
											nativeLeaderHints.replicaCountsByIndex,
										) ||
										0,
								peerHistoryGids: nativeLeaderHints.peerHistoryGids.length,
							},
						});
					}
				} else {
					const metadataStartedAt = syncProfileStart(syncProfile);
					nativeEntryMetadata = this.getNativeLogEntryMetadataBatch(msg.hashes);
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, metadataStartedAt, {
							name: "sharedLog.receive.requestPrune.nativeMetadata",
							component: "shared-log",
							entries: msg.hashes.length,
							messages: 1,
							details: {
								nativeEntries:
									nativeEntryMetadata?.reduce(
										(sum, entry) => sum + (entry ? 1 : 0),
										0,
									) ?? 0,
							},
						});
					}
					const blockHasManyStartedAt = syncProfileStart(syncProfile);
					presentBlocks = await this.log.blocks.hasMany?.(msg.hashes);
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, blockHasManyStartedAt, {
							name: "sharedLog.receive.requestPrune.blockHasMany",
							component: "shared-log",
							entries: msg.hashes.length,
							messages: 1,
							details: {
								batched: presentBlocks != null,
								presentBlocks:
									presentBlocks?.reduce(
										(sum, present) => sum + (present ? 1 : 0),
										0,
									) ?? 0,
							},
						});
					}
					const nativeLeaderPlanStartedAt = syncProfileStart(syncProfile);
					nativeLeaderHints =
						await this.planCurrentNativeRequestPruneLeaderHints({
							hashes: msg.hashes,
							nativeEntryMetadata,
							presentBlocks,
						});
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, nativeLeaderPlanStartedAt, {
							name: "sharedLog.receive.requestPrune.nativeLeaderPlan",
							component: "shared-log",
							entries: msg.hashes.length,
							messages: 1,
							details: {
								localLeaders: nativeLeaderHints.localLeaderHashes.size,
								plannedEntries: nativeLeaderHints.replicaCounts.size,
								peerHistoryGids: nativeLeaderHints.peerHistoryGids.length,
							},
						});
					}
				}
				const gidCleanupStartedAt = syncProfileStart(syncProfile);
				this.removePeerFromGidPeerHistoryBatch(
					from,
					nativeLeaderHints.peerHistoryGids,
					{
						skipNativeBackbone:
							nativeLeaderHints.nativeBackbonePeerHistoryCleaned === true,
					},
				);
				if (syncProfile) {
					emitSyncProfileDuration(syncProfile, gidCleanupStartedAt, {
						name: "sharedLog.receive.requestPrune.gidCleanup",
						component: "shared-log",
						entries: nativeLeaderHints.peerHistoryGids.length,
						messages: 1,
					});
				}

				const requestPruneLoopStartedAt = syncProfileStart(syncProfile);
				if (
					canConfirmNativeRequestPruneBatch(
						nativeLeaderHints,
						msg.hashes.length,
					)
				) {
					this.responseToPruneDebouncedFn.add({
						hashes: msg.hashes,
						peers: [from],
					});
					if (syncProfile) {
						emitSyncProfileDuration(syncProfile, requestPruneLoopStartedAt, {
							name: "sharedLog.receive.requestPrune.loop",
							component: "shared-log",
							entries: msg.hashes.length,
							messages: 1,
							details: {
								presentEntries: msg.hashes.length,
								indexedFallbackLookups: 0,
								fallbackBlockChecks: 0,
								pendingDeleteEntries: 0,
								leaderResponses: msg.hashes.length,
								leaderResponseBatches: 1,
								pendingIHaveCreated: 0,
								pendingIHaveExtended: 0,
								skippedIndexedLookupsForMissingBlocks: 0,
								nativeBatchConfirmed: true,
							},
						});
						emitSyncProfileDuration(syncProfile, requestPruneStartedAt, {
							name: "sharedLog.receive.requestPrune.total",
							component: "shared-log",
							entries: msg.hashes.length,
							messages: 1,
						});
					}
					return;
				}
				let presentEntries = 0;
				let indexedFallbackLookups = 0;
				let fallbackBlockChecks = 0;
				let skippedIndexedLookupsForMissingBlocks = 0;
				let pendingDeleteEntries = 0;
				let leaderResponses = 0;
				let pendingIHaveCreated = 0;
				let pendingIHaveExtended = 0;
				const leaderResponseHashes: string[] = [];
				for (let i = 0; i < msg.hashes.length; i++) {
					if (
						!this.isReplicationLifecycleActive(
							receiveReplicationLifecycleController,
						)
					) {
						return;
					}
					const hash = msg.hashes[i]!;

					const nativeEntryGid = nativeLeaderHints.nativeEntryGids?.[i];
					const nativeEntryData = nativeLeaderHints.nativeEntryDataByIndex?.[i];
					const nativeEntry =
						nativeLeaderHints.nativeEntryMetadata?.[i] ??
						nativeLeaderHints.nativeEntries?.get(hash) ??
						nativeEntryMetadata?.[i];
					const hasNativeEntry = nativeEntryGid != null || nativeEntry != null;
					let indexedEntry:
						| Awaited<ReturnType<typeof this.log.entryIndex.getShallow>>
						| undefined;
					let isLeader = false;

					const hasPresentBlock = nativeLeaderHints.presentBlockHashes
						? nativeLeaderHints.presentBlockHashes.has(hash)
						: nativeLeaderHints.presentBlocks
							? !!nativeLeaderHints.presentBlocks[i]
							: presentBlocks
								? presentBlocks[i] === true
								: await this.log.blocks.has(hash);
					if (
						!presentBlocks &&
						!nativeLeaderHints.presentBlockHashes &&
						!nativeLeaderHints.presentBlocks
					) {
						fallbackBlockChecks += 1;
					}
					if (!hasNativeEntry && hasPresentBlock) {
						indexedEntry = await this.log.entryIndex.getShallow(hash);
						indexedFallbackLookups += 1;
					} else if (!hasNativeEntry) {
						skippedIndexedLookupsForMissingBlocks += 1;
					}
					if ((hasNativeEntry || indexedEntry) && hasPresentBlock) {
						presentEntries += 1;
						const pendingDelete = this._checkedPrune.getPendingDelete(hash);
						if (pendingDelete) {
							pendingDeleteEntries += 1;
							const pendingEntry =
								indexedEntry?.value ??
								(await this.log.entryIndex.getShallow(hash))?.value;
							if (pendingEntry) {
								const ownership = await this.revalidateCheckedPruneOwnership({
									hash,
									entry: pendingEntry,
									leaders: new Map(),
								});
								if (ownership.localLeader) {
									await this.cancelCheckedPruneForLocalLeader(hash);
									isLeader = true;
								}
							}
						} else {
							const gid =
								nativeEntryGid ??
								nativeEntry?.gid ??
								indexedEntry!.value.meta.gid;
							const replicaCountByIndex =
								nativeLeaderHints.replicaCountsByIndex?.[i];
							const replicas =
								replicaCountByIndex != null && replicaCountByIndex > 0
									? replicaCountByIndex
									: (nativeLeaderHints.replicaCounts.get(hash) ??
										decodeReplicas({
											meta: {
												data:
													nativeEntryData ??
													nativeEntry?.data ??
													indexedEntry!.value.meta.data,
											},
										}).getValue(this));

							if (
								!nativeLeaderHints.peerHistoryRemovedFlags?.[i] &&
								!nativeLeaderHints.peerHistoryRemovedHashes.has(hash)
							) {
								this.removePeerFromGidPeerHistory(from, gid);
							}

							if (
								!!nativeLeaderHints.localLeaderFlags?.[i] ||
								nativeLeaderHints.localLeaderHashes.has(hash)
							) {
								isLeader = true;
							} else {
								const selfHash = this.node.identity.publicKey.hashcode();
								const waitFor: WaitForReplicator[] = [
									{
										key: selfHash,
										replicator: true,
									},
								];
								const waitOptions: WaitForReplicatorsOptions<R> = {
									onLeader: (key) => {
										isLeader = isLeader || key === selfHash;
									},
								};
								if (hasNativeEntry) {
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
						}
					}

					if (isLeader) {
						leaderResponses += 1;
						leaderResponseHashes.push(hash);
					} else {
						const prevPendingIHave = this._pendingIHave.get(hash);
						if (prevPendingIHave) {
							pendingIHaveExtended += 1;
							prevPendingIHave.requesting.add(from);
							prevPendingIHave.resetTimeout();
						} else {
							pendingIHaveCreated += 1;
							const requesting = new Set([from]);
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
									for (const requester of requesting) {
										this.removePeerFromGidPeerHistory(
											requester,
											entry.meta.gid,
										);
										this.removePruneRequestSent(entry.hash, requester);
									}
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
									if (
										!this.isReplicationLifecycleActive(
											receiveReplicationLifecycleController,
										) ||
										requesting.size === 0
									) {
										return;
									}
									if (isLeader) {
										this.responseToPruneDebouncedFn.add({
											hashes: [entry.hash],
											peers: new Set(requesting),
										});
									}
								},
							};

							this._pendingIHave.set(hash, pendingIHave);
							this.resetPendingIHaveTimeout(pendingIHave);
						}
					}
				}
				if (leaderResponseHashes.length > 0) {
					this.responseToPruneDebouncedFn.add({
						hashes: leaderResponseHashes,
						peers: [from],
					});
				}
				if (syncProfile) {
					emitSyncProfileDuration(syncProfile, requestPruneLoopStartedAt, {
						name: "sharedLog.receive.requestPrune.loop",
						component: "shared-log",
						entries: msg.hashes.length,
						messages: 1,
						details: {
							presentEntries,
							indexedFallbackLookups,
							fallbackBlockChecks,
							pendingDeleteEntries,
							leaderResponses,
							leaderResponseBatches: leaderResponseHashes.length > 0 ? 1 : 0,
							pendingIHaveCreated,
							pendingIHaveExtended,
							skippedIndexedLookupsForMissingBlocks,
						},
					});
					emitSyncProfileDuration(syncProfile, requestPruneStartedAt, {
						name: "sharedLog.receive.requestPrune.total",
						component: "shared-log",
						entries: msg.hashes.length,
						messages: 1,
					});
				}
			} else if (msg instanceof ResponseIPrune) {
				const lateResponses: string[] = [];
				const responseTasks: Promise<void>[] = [];
				for (const hash of msg.hashes) {
					const pendingDelete = this._checkedPrune.getPendingDelete(hash);
					if (pendingDelete) {
						responseTasks.push(
							Promise.resolve(
								pendingDelete.resolve(context.from.hashcode()),
							),
						);
					} else {
						lateResponses.push(hash);
					}
				}
				if (lateResponses.length > 0) {
					responseTasks.push(
						this.recoverCheckedPruneFromLateResponses(
							lateResponses,
							context.from.hashcode(),
						),
					);
				}
				const results = await Promise.allSettled(responseTasks);
				for (const result of results) {
					if (result.status === "rejected") {
						logger.error(result.reason?.toString?.() ?? String(result.reason));
					}
				}
			} else if (msg instanceof ConfirmEntriesMessage) {
				this.markEntriesKnownByPeer(msg.hashes, context.from.hashcode());
				this.clearRepairFrontierHashes(context.from.hashcode(), msg.hashes);
				return;
			} else if (msg instanceof SyncCapabilitiesMessage) {
				if (!context.from.equals(this.node.identity.publicKey)) {
					const openingEpoch =
						this._subscriptionOpeningEpochByPeer.get(receiveFromHash);
					if (
						this._replicationInfoBlockedPeers.has(receiveFromHash) &&
						openingEpoch === receiveSubscriptionEpoch
					) {
						// A prior unsubscribe cleanup may still be ahead of this reconnect
						// barrier. Stage the new generation's one-shot advertisement so that
						// cleanup cannot erase it before the opening transition commits.
						this._openingSyncCapabilitiesByPeer.set(receiveFromHash, {
							epoch: openingEpoch,
							capabilities: msg.capabilities,
						});
					} else {
						this._peerSyncCapabilities.set(
							receiveFromHash,
							msg.capabilities,
						);
					}
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
			} else if (msg instanceof RequestReplicationInfoMessage) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}
				const replicationLifecycleController =
					receiveReplicationLifecycleController;
				if (
					!replicationLifecycleController ||
					!this.isPeerReceiveAdmissionOpen(
						receiveFromHash,
						replicationLifecycleController,
						receiveSubscriptionEpoch,
					)
				) {
					return;
				}

				let replicationSegments: ReplicationRangeIndexable<R>[];
				try {
					replicationSegments = await this.getMyReplicationSegments();
				} catch (error) {
					if (
						!this.isPeerReceiveAdmissionOpen(
							receiveFromHash,
							replicationLifecycleController,
							receiveSubscriptionEpoch,
						) &&
						isNotStartedError(error as Error)
					) {
						return;
					}
					throw error;
				}
				if (
					!this.isPeerReceiveAdmissionOpen(
						receiveFromHash,
						replicationLifecycleController,
						receiveSubscriptionEpoch,
					)
				) {
					return;
				}
				const segments = replicationSegments.map((x) => x.toReplicationRange());
				this.validatePersistedReplicationRangeSnapshot(segments);

				await this.rpc
					.send(new AllReplicatingSegmentsMessage({ segments }), {
						mode: new AcknowledgeDelivery({
							to: [context.from],
							redundancy: 1,
						}),
						signal: replicationLifecycleController.signal,
					})
					.catch((error) =>
						this.handleReplicationLifecycleSendError(
							error,
							replicationLifecycleController,
						),
					);
				if (
					!this.isPeerReceiveAdmissionOpen(
						receiveFromHash,
						replicationLifecycleController,
						receiveSubscriptionEpoch,
					)
				) {
					return;
				}

				// for backwards compatibility (v8) remove this when we are sure that all nodes are v9+
				if (this.v8Behaviour) {
					const role = this.getRoleFromReplicationSegments(replicationSegments);
					if (role instanceof Replicator) {
						const fixedSettings = !this._isAdaptiveReplicating;
						if (fixedSettings) {
							await this.rpc
								.send(
									new ResponseRoleMessage({
										role,
									}),
									{
										mode: new SilentDelivery({
											to: [context.from],
											redundancy: 1,
										}),
										signal: replicationLifecycleController.signal,
									},
								)
								.catch((error) =>
									this.handleReplicationLifecycleSendError(
										error,
										replicationLifecycleController,
									),
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
				if (
					!this.isReplicationLifecycleActive(
						receiveReplicationLifecycleController,
					) ||
					!this.isCurrentReplicationInfoReceiveEpoch(
						receiveFromHash,
						receiveReplicationInfoReceiveEpoch,
					) ||
					this._replicationInfoBlockedPeers.has(fromHash)
				) {
					return;
				}
				const messageTimestamp = context.message.header.timestamp;
				releasePeerReceiveLease?.();
				releasePeerReceiveLease = undefined;
				await this.withReplicationInfoApplyQueue(fromHash, async () => {
					try {
						// The peer may have unsubscribed after this message was queued.
						if (
							!this.isReplicationLifecycleActive(
								receiveReplicationLifecycleController,
							) ||
							!this.isCurrentSubscriptionEpoch(
								fromHash,
								receiveSubscriptionEpoch,
							) ||
							!this.isCurrentReplicationInfoReceiveEpoch(
								fromHash,
								receiveReplicationInfoReceiveEpoch,
							) ||
							this._replicationInfoBlockedPeers.has(fromHash)
						) {
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
								allowLegacyOrderedReplacementPairs:
									msg instanceof AddedReplicationSegmentMessage,
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
				const from = context.from!;
				const segmentIds = msg.segmentIds;
				if (from.equals(this.node.identity.publicKey)) {
					return;
				}
				const fromHash = from.hashcode();
				if (
					!this.isReplicationLifecycleActive(
						receiveReplicationLifecycleController,
					) ||
					!this.isCurrentReplicationInfoReceiveEpoch(
						receiveFromHash,
						receiveReplicationInfoReceiveEpoch,
					) ||
					this._replicationInfoBlockedPeers.has(fromHash)
				) {
					return;
				}
				const messageTimestamp = context.message.header.timestamp;
				releasePeerReceiveLease?.();
				releasePeerReceiveLease = undefined;
				await this.withReplicationInfoApplyQueue(fromHash, async () => {
					if (
						!this.isReplicationLifecycleActive(
							receiveReplicationLifecycleController,
						) ||
						!this.isCurrentSubscriptionEpoch(
							fromHash,
							receiveSubscriptionEpoch,
						) ||
						!this.isCurrentReplicationInfoReceiveEpoch(
							fromHash,
							receiveReplicationInfoReceiveEpoch,
						) ||
						this._replicationInfoBlockedPeers.has(fromHash)
					) {
						return;
					}

					const previousTimestamp =
						this.latestReplicationInfoMessage.get(fromHash);
					if (previousTimestamp && previousTimestamp > messageTimestamp) {
						return;
					}
					this.latestReplicationInfoMessage.set(fromHash, messageTimestamp);
					this._replicatorLivenessFailures.delete(fromHash);
					if (this.closed) {
						return;
					}

					const rangesToRemove =
						await this.resolveReplicationRangesFromIdsAndKey(
							segmentIds,
							from,
						);

					await this.removeReplicationRanges(rangesToRemove, from);
					const timestamp = BigInt(+new Date());
					for (const range of rangesToRemove) {
						this.replicationChangeDebounceFn.add({
							range,
							type: "removed",
							timestamp,
						});
					}
				});
			} else {
				throw new Error("Unexpected message");
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
				releasePeerReceiveLease?.();
				releasePeerReceiveLease = undefined;
				this.throwIfNativeDurableCommitFailed();
			}
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
			let messageToSend: AddedReplicationSegmentMessage | undefined = undefined;

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
			throwIfInactive();

			// it is importat that we call persistCoordinate after this.replicate(entries) as else there might be a prune job deleting the entry before replication duties has been assigned to self
			for (const entry of entriesToPersist) {
				await persistCoordinate(entry);
				throwIfInactive();
			}

			if (messageToSend) {
				await this.sendReplicationAnnouncement(
					messageToSend,
					ownershipLifecycleController,
				);
				throwIfInactive();
			}
		}
		throwIfInactive();
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
		if (
			this.canPlanNativeHashGid(entry) &&
			(this._nativeBackbone ??
				this._nativeSharedLogState ??
				this._nativeRangePlanner)
		) {
			return this.waitForLeaderSelection(
				waitFor,
				options,
				async (checkOptions) => {
					const plan = await this.planEntryLeaders(
						entry,
						replicas,
						checkOptions,
					);
					return plan.leaders;
				},
			);
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
			this._replicationLifecycleController?.signal;

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

	async createCoordinates(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R> | NumberFromType<R>,
		minReplicas: number,
	) {
		if (
			typeof entry !== "number" &&
			typeof entry !== "bigint" &&
			this.canPlanNativeHashGid(entry)
		) {
			const nativeCoordinates = (
				this._nativeBackbone ?? this._nativeRangePlanner
			)?.getGidCoordinates(entry.meta.gid, minReplicas) as
				| NumberFromType<R>[]
				| undefined;
			if (nativeCoordinates) {
				return nativeCoordinates;
			}
		}

		const cursor =
			typeof entry === "number" || typeof entry === "bigint"
				? entry
				: await this.domain.fromEntry(entry);
		const nativeGrid = (
			this._nativeBackbone ?? this._nativeRangePlanner
		)?.getGrid(cursor, minReplicas) as NumberFromType<R>[] | undefined;
		return (
			nativeGrid ?? this.indexableDomain.numbers.getGrid(cursor, minReplicas)
		);
	}

	private async getCoordinates(entry: { hash: string }) {
		const nativeCoordinates = (
			this._nativeBackbone ?? this._nativeSharedLogState
		)?.getEntryCoordinates(entry.hash);
		if (nativeCoordinates) {
			return nativeCoordinates as NumberFromType<R>[];
		}
		const result = await this.entryCoordinatesIndex
			.iterate({ query: { hash: entry.hash } })
			.all();
		return result[0].value.coordinates;
	}

	private getNativeLogEntryMetadataBatch(hashes: Iterable<string>) {
		const normalized = [...hashes];
		if (normalized.length === 0) {
			return [];
		}
		const backboneMetadata =
			this._nativeBackbone?.graph.entryMetadataHintsBatch(normalized) ??
			this._nativeBackbone?.graph.entryMetadataBatch(normalized);
		if (backboneMetadata?.every((entry) => entry != null)) {
			return backboneMetadata;
		}
		const indexMetadata =
			this.log.entryIndex.getNativeEntryMetadataHintsBatch(normalized) ??
			this.log.entryIndex.getNativeEntryMetadataBatch(normalized);
		if (!backboneMetadata) {
			return indexMetadata;
		}
		if (!indexMetadata) {
			return backboneMetadata;
		}
		return backboneMetadata.map(
			(entry, index) => entry ?? indexMetadata[index],
		);
	}

	private async planCurrentNativeRequestPruneLeaderHints(properties: {
		hashes: string[];
		nativeEntryMetadata?: Array<
			{ gid: string; data?: Uint8Array; replicas?: number } | undefined | null
		>;
		presentBlocks?: boolean[] | undefined;
	}): Promise<NativeRequestPruneLeaderHints> {
		const empty = (): NativeRequestPruneLeaderHints => ({
			localLeaderHashes: new Set(),
			replicaCounts: new Map(),
			peerHistoryGids: [],
			peerHistoryRemovedHashes: new Set(),
		});
		const planner = this._nativeBackbone ?? this._nativeRangePlanner;
		if (
			!planner ||
			!properties.nativeEntryMetadata ||
			!properties.presentBlocks
		) {
			return empty();
		}

		const hashes: string[] = [];
		const entries: Array<{ gid: string; replicas: number }> = [];
		const replicaCounts = new Map<string, number>();
		const peerHistoryGids: string[] = [];
		const peerHistoryRemovedHashes = new Set<string>();
		for (let i = 0; i < properties.hashes.length; i++) {
			const hash = properties.hashes[i]!;
			const nativeEntry = properties.nativeEntryMetadata[i];
			if (
				!nativeEntry ||
				properties.presentBlocks[i] !== true ||
				this._checkedPrune.getPendingDelete(hash)
			) {
				continue;
			}
			const replicas =
				nativeEntry.replicas ??
				decodeReplicas({
					meta: {
						data: nativeEntry.data,
					},
				}).getValue(this);
			hashes.push(hash);
			replicaCounts.set(hash, replicas);
			peerHistoryGids.push(nativeEntry.gid);
			peerHistoryRemovedHashes.add(hash);
			entries.push({
				gid: nativeEntry.gid,
				replicas,
			});
		}
		if (entries.length === 0) {
			return {
				localLeaderHashes: new Set(),
				replicaCounts,
				peerHistoryGids,
				peerHistoryRemovedHashes,
			};
		}

		const context = await this.createLeaderSelectionContext();
		const nativeLeaderOptions = this.createNativeLeaderOptions(context);
		let localLeaderHashes = (
			planner as {
				planLocalLeaderHashesForGidsBatch?: (
					items: Iterable<{ hash: string; gid: string; replicas: number }>,
					options: typeof nativeLeaderOptions,
				) => Set<string> | undefined;
			}
		).planLocalLeaderHashesForGidsBatch?.(
			entries.map((entry, index) => ({
				hash: hashes[index]!,
				...entry,
			})),
			nativeLeaderOptions,
		);
		if (!localLeaderHashes) {
			const nativePlans = planner.planLeadersForGidsBatch(
				entries,
				nativeLeaderOptions,
			);
			localLeaderHashes = new Set<string>();
			for (let i = 0; i < nativePlans.length; i++) {
				if (nativePlans[i]?.leaders.has(context.selfHash)) {
					localLeaderHashes.add(hashes[i]!);
				}
			}
		}
		return {
			localLeaderHashes,
			replicaCounts,
			peerHistoryGids,
			peerHistoryRemovedHashes,
		};
	}

	private async planCurrentNativeBackboneRequestPruneLeaderHints(
		hashes: string[],
		from: string,
	): Promise<NativeRequestPruneLeaderHints | undefined> {
		if (!this._nativeBackbone) {
			return undefined;
		}
		const context = await this.createLeaderSelectionContext();
		const skipHashes =
			this._checkedPrune.pendingDeletes.size > 0
				? [...this._checkedPrune.pendingDeletes.keys()]
				: [];
		const nativeBackboneHintArrays = this
			._nativeBackbone as NativePeerbitBackbone & {
			planRequestPruneAllConfirmed?: (
				hashes: Iterable<string>,
				prunePeer: string,
				options?: { omitPeerHistoryGids?: boolean },
			) => { allConfirmed: boolean; peerHistoryGids: string[] } | undefined;
			planRequestPruneLeaderHintColumns?: (
				hashes: Iterable<string>,
				skipHashes: Iterable<string>,
				options?: unknown,
			) => NativeBackboneRequestPruneHintColumns | undefined;
		};
		if (skipHashes.length === 0) {
			const allConfirmed =
				nativeBackboneHintArrays.planRequestPruneAllConfirmed?.(hashes, from, {
					...this.createNativeLeaderOptions(context),
					omitPeerHistoryGids:
						this._gidPeersHistory.size === 0 &&
						this._nativeSharedLogState == null,
				});
			if (allConfirmed?.allConfirmed) {
				return {
					localLeaderHashes: new Set(),
					replicaCounts: new Map(),
					peerHistoryGids: allConfirmed.peerHistoryGids,
					peerHistoryRemovedHashes: new Set(),
					nativeAllConfirmed: true,
					nativeBackbonePeerHistoryCleaned: true,
				};
			}
		}
		const hintColumns =
			nativeBackboneHintArrays.planRequestPruneLeaderHintColumns?.(
				hashes,
				skipHashes,
				this.createNativeLeaderOptions(context),
			);
		if (hintColumns) {
			return {
				localLeaderHashes: new Set(),
				replicaCounts: new Map(),
				replicaCountsByIndex: hintColumns.replicaCounts,
				peerHistoryGids: hintColumns.peerHistoryGids,
				peerHistoryRemovedHashes: new Set(),
				peerHistoryRemovedFlags: hintColumns.peerHistoryRemovedFlags,
				nativeEntryGids: hintColumns.gids,
				nativeEntryDataByIndex: hintColumns.data,
				presentBlocks: hintColumns.presentBlockFlags,
				localLeaderFlags: hintColumns.localLeaderFlags,
			};
		}
		const hints = this._nativeBackbone.planRequestPruneLeaderHints(
			hashes,
			skipHashes,
			this.createNativeLeaderOptions(context),
		);
		if (!hints) {
			return undefined;
		}
		return {
			localLeaderHashes: hints.localLeaderHashes,
			replicaCounts: hints.replicaCounts,
			peerHistoryGids: hints.peerHistoryGids,
			peerHistoryRemovedHashes: hints.peerHistoryRemovedHashes,
			nativeEntries: hints.entries,
			presentBlockHashes: hints.presentBlockHashes,
		};
	}

	private createReusableReceiveCoordinatePlans(
		receiveGroups: Array<{
			latestEntry: ShallowOrFullEntry<any>;
			maxMaxReplicas: number;
			leaderPlan?: EntryLeaderPlan<R>;
		}>,
		options?: {
			decodedReplicaCounts?: DecodedReplicaCountMap;
			allowRoleAgeZeroPlans?: boolean;
		},
	): Map<string, ReusableReceiveCoordinatePlan<R>> {
		const reusablePlans = new Map<string, ReusableReceiveCoordinatePlan<R>>();
		if (this.timeUntilRoleMaturity > 0 && !options?.allowRoleAgeZeroPlans) {
			return reusablePlans;
		}

		for (const group of receiveGroups) {
			const plan = group.leaderPlan;
			if (!plan) {
				continue;
			}
			const replicas =
				options?.decodedReplicaCounts?.get(group.latestEntry.hash) ??
				decodeReplicas(group.latestEntry).getValue(this);
			if (replicas !== group.maxMaxReplicas) {
				continue;
			}
			const prepared = this.createCoordinatePersistenceEntryFromLeaderPlan({
				entry: group.latestEntry,
				plan,
				replicas,
			});
			if (!prepared) {
				continue;
			}
			reusablePlans.set(group.latestEntry.hash, {
				plan,
				replicas,
				prepared,
			});
		}
		return reusablePlans;
	}

	private createBackboneOnlyReceiveCoordinateBatch(
		items: CoordinatePersistBatchItem<R>[],
	): NativeBackboneReceiveCoordinateBatch<R> | undefined {
		if (
			!this._nativeBackbone ||
			items.length === 0 ||
			!this.canUseBackboneOnlyCoordinatePersistence()
		) {
			return undefined;
		}

		const rows = items
			.filter((item) => item.prepared)
			.map((item) => {
				const prepared = item.prepared!;
				const deleteHashes = this.getEntryNext(item.entry);
				return {
					item,
					prepared,
					fields: prepared.fields,
					deleteHashes,
				};
			});
		if (rows.length === 0) {
			return undefined;
		}

		return {
			rows,
			rollbackCoordinateEntries: this.snapshotResidentCoordinateEntries(
				rows.flatMap((row) => [row.item.entry.hash, ...row.deleteHashes]),
			),
		};
	}

	private nativeBackboneReceiveCoordinateRowsToColumns(
		rows: NativeBackboneReceiveCoordinateRow<R>[],
	): NativeBackboneCoordinateCommitColumns {
		const hashes = new Array<string>(rows.length);
		const gids = new Array<string>(rows.length);
		const hashNumberValues = new BigUint64Array(rows.length);
		const coordinateCounts = new Uint32Array(rows.length);
		const coordinateValues = new BigUint64Array(
			rows.reduce((sum, row) => sum + row.fields.coordinates.length, 0),
		);
		const nextHashBatches = new Array<string[]>(rows.length);
		const assignedToRangeBoundaries = new Uint8Array(rows.length);
		const requestedReplicaValues = new Uint32Array(rows.length);
		let coordinateOffset = 0;
		for (let i = 0; i < rows.length; i++) {
			const { item, prepared, fields, deleteHashes } = rows[i]!;
			hashes[i] = item.entry.hash;
			gids[i] = fields.gid;
			hashNumberValues[i] =
				typeof fields.hashNumber === "bigint"
					? fields.hashNumber
					: BigInt(fields.hashNumberString ?? fields.hashNumber);
			coordinateCounts[i] = fields.coordinates.length;
			for (const coordinate of fields.coordinates) {
				coordinateValues[coordinateOffset++] =
					typeof coordinate === "bigint" ? coordinate : BigInt(coordinate);
			}
			nextHashBatches[i] = deleteHashes;
			assignedToRangeBoundaries[i] =
				prepared.assignedToRangeBoundary === true ? 1 : 0;
			requestedReplicaValues[i] = item.replicas;
		}
		return {
			hashes,
			gids,
			hashNumberValues,
			coordinateCounts,
			coordinateValues,
			nextHashBatches,
			assignedToRangeBoundaries,
			requestedReplicaValues,
		};
	}

	private async finishBackboneOnlyReceiveCoordinateBatch(
		batch: NativeBackboneReceiveCoordinateBatch<R>,
		profile?: SyncProfileFn,
	): Promise<Set<string>> {
		const mirrorStartedAt = syncProfileStart(profile);
		const persistedHashes = new Set<string>();
		const coordinateToHashRows: [NumberFromType<R>, string][] = [];
		let deleteCount = 0;
		for (const { item, prepared, fields, deleteHashes } of batch.rows) {
			persistedHashes.add(item.entry.hash);
			this._residentEntryCoordinatesByHash?.set(
				item.entry.hash,
				prepared.coordinateEntry ?? fields,
			);
			for (const deletedHash of deleteHashes) {
				this._residentEntryCoordinatesByHash?.delete(deletedHash);
				deleteCount++;
			}
			for (const coordinate of item.coordinates) {
				coordinateToHashRows.push([coordinate, item.entry.hash]);
			}
		}
		this.coordinateToHash.addMany(coordinateToHashRows);
		emitSyncProfileDuration(profile, mirrorStartedAt, {
			name: "sharedLog.receive.coordinateResidentMirror",
			component: "shared-log",
			entries: batch.rows.length,
			count: coordinateToHashRows.length,
			messages: 1,
			details: { deletes: deleteCount },
		});

		const flushStartedAt = syncProfileStart(profile);
		const flushed = this.flushNativeBackboneCoordinateJournalOnAppend();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
		emitSyncProfileDuration(profile, flushStartedAt, {
			name: "sharedLog.receive.coordinateJournalFlush",
			component: "shared-log",
			entries: batch.rows.length,
			messages: 1,
		});
		return persistedHashes;
	}

	private rollbackBackboneOnlyReceiveCoordinateBatch(
		batch: NativeBackboneReceiveCoordinateBatch<R>,
	): void {
		for (const { item } of batch.rows) {
			this.rollbackNativeBackboneCoordinateAppend(
				item.entry.hash,
				batch.rollbackCoordinateEntries,
			);
		}
	}

	private async persistBackboneOnlyReceiveCoordinateBatch(
		items: CoordinatePersistBatchItem<R>[],
	): Promise<Set<string> | undefined> {
		const backbone = this._nativeBackbone;
		const batch = this.createBackboneOnlyReceiveCoordinateBatch(items);
		if (!backbone || !batch) {
			return undefined;
		}
		try {
			backbone.commitEntryCoordinatesColumnsBatch(
				this.nativeBackboneReceiveCoordinateRowsToColumns(batch.rows),
			);
			return await this.finishBackboneOnlyReceiveCoordinateBatch(batch);
		} catch (error) {
			this.rollbackBackboneOnlyReceiveCoordinateBatch(batch);
			throw error;
		}
	}

	private emitNativeBackboneRawCommitProfile(
		profile: SyncProfileFn | undefined,
		nativeProfile: NativeBackboneAppendProfile | undefined,
		entries: number,
		verifyCount: number,
	): void {
		if (!profile || !nativeProfile) {
			return;
		}
		const events: Array<[name: string, durationMs: number, count?: number]> = [
			[
				"sharedLog.receive.nativeRawCommit.pendingCheck",
				nativeProfile.nativeBackboneRawReceivePendingCheckMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.verify",
				nativeProfile.nativeBackboneRawReceiveVerifyMs,
				verifyCount,
			],
			[
				"sharedLog.receive.nativeRawCommit.verifyStatus",
				nativeProfile.nativeBackboneRawReceiveVerifyStatusMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.joinPlan",
				nativeProfile.nativeBackboneRawReceiveJoinPlanMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.removePending",
				nativeProfile.nativeBackboneRawReceiveRemoveMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.blockPut",
				nativeProfile.nativeBackboneRawReceiveBlockPutMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.graphPut",
				nativeProfile.nativeBackboneRawReceiveGraphPutMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.coordinateCommit",
				nativeProfile.nativeBackboneRawReceiveCoordinateCommitMs,
			],
		];
		for (const [name, durationMs, count] of events) {
			if (durationMs > 0) {
				emitSyncProfileEvent(profile, {
					name,
					component: "shared-log",
					durationMs,
					entries,
					count,
					messages: 1,
				});
			}
		}
	}

	private createNativeBackbonePreparedJoinCommit(
		coordinateBatch?: NativeBackboneReceiveCoordinateBatch<R>,
		onCoordinatesCommitted?: (
			batch: NativeBackboneReceiveCoordinateBatch<R>,
		) => void,
		verifyHashes?: string[],
		verifyAllHashes = false,
		profile?: SyncProfileFn,
		onPreparedEntriesCommitted?: (hashes: string[]) => void,
	):
		| ((input: {
				entries: PreparedAppendJoinFacts[];
				hashes: string[];
				headFlags: boolean[];
				headFlagsBytes: Uint8Array;
				trustedMissing: boolean;
				validatePlan?: boolean;
		  }) => boolean)
		| undefined {
		const backbone = this._nativeBackbone;
		if (
			!backbone ||
			this.remoteBlocks?.localStore !== backbone.blocks ||
			(verifyHashes &&
				verifyHashes.length > 0 &&
				!backbone.graph.commitVerifiedPreparedRawReceiveJoinBatch)
		) {
			return undefined;
		}
		return ({
			entries,
			hashes,
			headFlags,
			headFlagsBytes,
			trustedMissing,
			validatePlan,
		}) => {
			this.throwIfReplicationOwnershipPoisoned();
			if (!trustedMissing || entries.length === 0) {
				return false;
			}
			const coordinateColumns =
				coordinateBatch && coordinateBatch.rows.length > 0
					? this.nativeBackboneReceiveCoordinateRowsToColumns(
							coordinateBatch.rows,
						)
					: undefined;
			if (validatePlan) {
				const verifiedCommitStartedAt = syncProfileStart(profile);
				const profileNativeBackbone =
					!!profile &&
					!!backbone.resetAppendProfile &&
					!!backbone.setAppendProfileEnabled &&
					!!backbone.appendProfile;
				if (profileNativeBackbone) {
					backbone.resetAppendProfile();
					backbone.setAppendProfileEnabled(true);
				}
				let committed: boolean | undefined;
				try {
					if (verifyHashes && verifyHashes.length > 0) {
						if (verifyAllHashes) {
							committed =
								backbone.graph.commitVerifiedAllPreparedRawReceiveJoinBatch?.(
									hashes,
									headFlagsBytes,
									coordinateColumns,
								);
						}
						committed ??=
							backbone.graph.commitVerifiedPreparedRawReceiveJoinBatch?.(
								hashes,
								headFlagsBytes,
								verifyHashes,
								coordinateColumns,
							);
					} else {
						committed = backbone.graph.commitPreparedRawReceiveJoinBatch?.(
							hashes,
							headFlagsBytes,
							coordinateColumns,
						);
					}
				} finally {
					if (profileNativeBackbone) {
						backbone.setAppendProfileEnabled(false);
						this.emitNativeBackboneRawCommitProfile(
							profile,
							backbone.appendProfile(),
							entries.length,
							verifyHashes?.length ?? 0,
						);
					}
				}
				if (verifyHashes && verifyHashes.length > 0 && profile) {
					emitSyncProfileDuration(profile, verifiedCommitStartedAt, {
						name: "sharedLog.receive.nativeVerifiedCommit",
						component: "shared-log",
						entries: entries.length,
						count: verifyHashes.length,
						messages: 1,
					});
				}
				if (committed === true) {
					onPreparedEntriesCommitted?.(hashes);
					if (coordinateBatch) {
						onCoordinatesCommitted?.(coordinateBatch);
					}
					return true;
				}
				if (committed === false) {
					backbone.graph.clearPreparedRawReceiveEntries?.(hashes);
					return false;
				}
			}
			if (
				backbone.graph.commitPreparedRawReceiveBatch(
					hashes,
					headFlagsBytes,
					coordinateColumns,
				)
			) {
				onPreparedEntriesCommitted?.(hashes);
				if (coordinateBatch) {
					onCoordinatesCommitted?.(coordinateBatch);
				}
				return true;
			}
			const commitEntries = new Array<NativeBackboneLogCommitEntry>(
				entries.length,
			);
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i]!;
				if (
					!entry.bytes ||
					!entry.nativeEntry ||
					entry.meta.type !== EntryType.APPEND
				) {
					return false;
				}
				commitEntries[i] = {
					...entry.nativeEntry,
					head: headFlags[i] ?? true,
					bytes: entry.bytes,
				};
			}
			if (coordinateBatch && coordinateBatch.rows.length > 0) {
				backbone.graph.commitBlocksGraphAndCoordinatesBatch(
					commitEntries,
					coordinateColumns!,
				);
				onCoordinatesCommitted?.(coordinateBatch);
			} else {
				backbone.graph.commitBlocksAndGraphBatch(commitEntries);
			}
			onPreparedEntriesCommitted?.(hashes);
			return true;
		};
	}

	private createCoordinatePersistenceEntryFromLeaderPlan(properties: {
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		plan: EntryLeaderPlan<R>;
		replicas: number;
	}): PreparedCoordinatePersistence<R> | false {
		const assignedToRangeBoundary =
			properties.plan.assignedToRangeBoundary ??
			shouldAssignToRangeBoundary(properties.plan.leaders, properties.replicas);
		const hashNumber = this.getEntryHashNumber(properties.entry);
		const metaBytes = (properties.entry as EntryWithMetaBytes).getMetaBytes?.();
		if (metaBytes) {
			const rawTimestamp =
				properties.entry instanceof Entry
					? getPreparedRawExchangeTimestamp(properties.entry)
					: undefined;
			const wallTime =
				rawTimestamp?.wallTime ??
				properties.entry.meta.clock.timestamp.wallTime;
			return {
				assignedToRangeBoundary,
				fields: {
					hash: properties.entry.hash,
					hashNumber,
					hashNumberString: hashNumber.toString(),
					gid: this.getEntryGid(properties.entry),
					coordinates: properties.plan.coordinates,
					coordinateStrings:
						properties.plan.coordinateStrings ??
						properties.plan.coordinates.map((coordinate) =>
							coordinate.toString(),
						),
					wallTime,
					wallTimeString: wallTime.toString(),
					assignedToRangeBoundary,
					metaBytes,
				},
			};
		}
		return this.createCoordinatePersistenceEntry({
			coordinates: properties.plan.coordinates,
			entry: properties.entry,
			leaders: properties.plan.leaders,
			replicas: properties.replicas,
			assignedToRangeBoundary,
			hashNumber,
		});
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
		hashNumber?: NumberFromType<R>;
	}): PreparedCoordinatePersistence<R> | false {
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
			hashNumber:
				properties.hashNumber ?? this.getEntryHashNumber(properties.entry),
		});
		return {
			coordinateEntry,
			assignedToRangeBoundary,
			fields: {
				hash: coordinateEntry.hash,
				hashNumber: coordinateEntry.hashNumber,
				gid: coordinateEntry.gid,
				coordinates: coordinateEntry.coordinates,
				wallTime: coordinateEntry.wallTime,
				assignedToRangeBoundary: coordinateEntry.assignedToRangeBoundary,
				metaBytes: coordinateEntry.getMetaBytes(),
			},
		};
	}

	private createCoordinatePersistenceEntryFromNativePlan(properties: {
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		plan: NativeAppendCoordinatePlan;
		prev?: EntryReplicated<R>;
	}): PreparedCoordinatePersistence<R> | false {
		if (
			properties.plan.hash !== properties.entry.hash ||
			properties.plan.gid !== this.getEntryGid(properties.entry)
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
		const metaBytes = (properties.entry as EntryWithMetaBytes).getMetaBytes?.();
		if (metaBytes) {
			const rawTimestamp =
				properties.entry instanceof Entry
					? getPreparedRawExchangeTimestamp(properties.entry)
					: undefined;
			const wallTime =
				rawTimestamp?.wallTime ??
				properties.entry.meta.clock.timestamp.wallTime;
			return {
				assignedToRangeBoundary,
				fields: {
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
				},
			};
		}
		const entryMeta = properties.entry.meta;
		const coordinateEntry = new this.indexableDomain.constructorEntry({
			assignedToRangeBoundary,
			coordinates,
			meta: entryMeta,
			hash: properties.plan.hash,
			hashNumber,
		});
		return {
			coordinateEntry,
			assignedToRangeBoundary,
			fields: {
				hash: properties.plan.hash,
				hashNumber,
				hashNumberString: properties.plan.hashNumberString,
				gid: properties.plan.gid,
				coordinates,
				coordinateStrings: properties.plan.coordinateStrings,
				wallTime: coordinateEntry.wallTime,
				wallTimeString: coordinateEntry.wallTime.toString(),
				assignedToRangeBoundary,
				metaBytes: coordinateEntry.getMetaBytes(),
			},
		};
	}

	private createCoordinateEntryFromNativeFields(
		fields: SharedLogCoordinateNativeFields<R>,
	): EntryReplicated<R> {
		return new this.indexableDomain.constructorEntry({
			assignedToRangeBoundary: fields.assignedToRangeBoundary,
			coordinates: fields.coordinates,
			metaBytes: fields.metaBytes,
			gid: fields.gid,
			wallTime: fields.wallTime,
			hash: fields.hash,
			hashNumber: fields.hashNumber,
		});
	}

	private materializePreparedCoordinateEntry(
		prepared: PreparedCoordinatePersistence<R>,
	): EntryReplicated<R> {
		return (prepared.coordinateEntry ??=
			this.createCoordinateEntryFromNativeFields(prepared.fields));
	}

	private materializeResidentCoordinateEntry(
		entry: ResidentCoordinateEntry<R>,
	): EntryReplicated<R> {
		return isEntryReplicated(entry)
			? entry
			: this.createCoordinateEntryFromNativeFields(entry);
	}

	private materializeRepairDispatchEntries(
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
	): Map<string, EntryReplicated<R>> {
		const materialized = new Map<string, EntryReplicated<R>>();
		for (const [hash, entry] of entries) {
			materialized.set(hash, this.materializeResidentCoordinateEntry(entry));
		}
		return materialized;
	}

	private snapshotResidentCoordinateEntries(
		hashes: Iterable<string>,
	): NativeBackboneCoordinateRollback<R> | undefined {
		const uniqueHashes = new Set([...hashes].filter(Boolean));
		if (uniqueHashes.size === 0) {
			return undefined;
		}
		const entries = new Map<string, ResidentCoordinateEntry<R>>();
		const generations = new Map<string, number>();
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of uniqueHashes) {
			const generation = (mutationGenerations.get(hash) ?? 0) + 1;
			mutationGenerations.set(hash, generation);
			generations.set(hash, generation);
			const entry = this._residentEntryCoordinatesByHash?.get(hash);
			if (entry) {
				entries.set(hash, entry);
			}
		}
		return { hashes: uniqueHashes, entries, generations };
	}

	private rollbackNativeBackboneCoordinateAppend(
		appendHash: string,
		rollback?: NativeBackboneCoordinateRollback<R>,
	): void {
		const backbone = this._nativeBackbone;
		if (!backbone) {
			return;
		}
		const hashes = rollback?.hashes ?? new Set([appendHash]);
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of hashes) {
			const expectedGeneration = rollback?.generations.get(hash);
			if (
				expectedGeneration !== undefined &&
				mutationGenerations.get(hash) !== expectedGeneration
			) {
				continue;
			}
			backbone.deleteEntryCoordinates(hash);
			this._nativeSharedLogState?.deleteEntryCoordinates(hash);
			this._residentEntryCoordinatesByHash?.delete(hash);
			const entry = rollback?.entries.get(hash);
			if (!entry) {
				continue;
			}
			const fields = isEntryReplicated(entry)
				? {
						hash: entry.hash,
						gid: entry.gid,
						coordinates: entry.coordinates,
						assignedToRangeBoundary: entry.assignedToRangeBoundary,
						hashNumber: entry.hashNumber,
					}
				: entry;
			const requestedReplicas = isEntryReplicated(entry)
				? decodeReplicas(entry).getValue(this)
				: fields.coordinates.length;
			backbone.putEntryCoordinates(
				fields.hash,
				fields.gid,
				fields.coordinates,
				fields.assignedToRangeBoundary,
				requestedReplicas,
				fields.hashNumber,
			);
			this._nativeSharedLogState?.putEntryCoordinates(
				fields.hash,
				fields.gid,
				fields.coordinates,
				fields.assignedToRangeBoundary,
				requestedReplicas,
				fields.hashNumber,
			);
			this._residentEntryCoordinatesByHash?.set(hash, entry);
		}
	}

	private async rollbackNativeBackboneCoordinateAppendDurably(
		appendHash: string,
		rollback?: NativeBackboneCoordinateRollback<R>,
	): Promise<void> {
		this.rollbackNativeBackboneCoordinateAppend(appendHash, rollback);
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		const hashes = rollback?.hashes ?? new Set([appendHash]);
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of hashes) {
			const expectedGeneration = rollback?.generations.get(hash);
			if (
				expectedGeneration !== undefined &&
				mutationGenerations.get(hash) !== expectedGeneration
			) {
				continue;
			}
			const previous = rollback?.entries.get(hash);
			if (previous) {
				await coordinateIndex.put(
					this.materializeResidentCoordinateEntry(previous),
				);
			} else if (coordinateIndex.delIds) {
				await coordinateIndex.delIds([hash]);
			} else if (coordinateIndex.delIdsNoReturn) {
				await coordinateIndex.delIdsNoReturn([hash]);
			} else {
				await coordinateIndex.del({ query: { hash } });
			}
		}
		const flushed = this.flushNativeBackboneCoordinateJournal();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
	}

	private persistPreparedCoordinate(
		properties: {
			prepared: PreparedCoordinatePersistence<R>;
			hash: string;
			nextHashes: string[];
			coordinates: NumberFromType<R>[];
			replicas: number;
			commitNative?: boolean;
			commitNativeBackbone?: boolean;
			deleteHashes?: string[];
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): MaybePromise<boolean> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { assignedToRangeBoundary, fields } = properties.prepared;
		const deleteHashes = combineCoordinateDeleteHashes(
			properties.nextHashes,
			properties.deleteHashes,
		);
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		let deleteNextOptions: DeleteOptions | undefined;
		let putResult: MaybePromise<unknown>;
		if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn) {
			putResult =
				coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn(
					fields,
					deleteHashes,
				);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashes) {
			putResult = coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashes(
				fields,
				deleteHashes,
			);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds) {
			putResult = coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds(
				fields,
				deleteHashes,
				toId(fields.hash),
			);
		} else if (coordinateIndex.putSharedLogCoordinateAndDeleteIds) {
			const coordinateEntry = this.materializePreparedCoordinateEntry(
				properties.prepared,
			);
			putResult = coordinateIndex.putSharedLogCoordinateAndDeleteIds(
				coordinateEntry,
				fields,
				deleteHashes,
				toId(fields.hash),
			);
		} else if (deleteHashes.length > 0 && coordinateIndex.putAndDeleteIds) {
			const coordinateEntry = this.materializePreparedCoordinateEntry(
				properties.prepared,
			);
			putResult = coordinateIndex.putAndDeleteIds(
				coordinateEntry,
				deleteHashes,
			);
		} else {
			const coordinateEntry = this.materializePreparedCoordinateEntry(
				properties.prepared,
			);
			deleteNextOptions =
				deleteHashes.length === 0
					? undefined
					: deleteHashes.length === 1
						? { query: { hash: deleteHashes[0] } }
						: {
								query: new Or(
									deleteHashes.map(
										(x) => new StringMatch({ key: "hash", value: x }),
									),
								),
							};
			if (deleteNextOptions && coordinateIndex.putAndDelete) {
				putResult = coordinateIndex.putAndDelete(
					coordinateEntry,
					deleteNextOptions,
				);
			} else {
				putResult = this.entryCoordinatesIndex.put(coordinateEntry);
			}
		}

		const finish = (): MaybePromise<boolean> => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeDeleteHashes = combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			);
			if (properties.commitNative !== false) {
				this._nativeSharedLogState?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					assignedToRangeBoundary,
					properties.replicas,
					fields.hashNumber,
				);
			}
			if (properties.commitNativeBackbone !== false) {
				this._nativeBackbone?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					assignedToRangeBoundary,
					properties.replicas,
					fields.hashNumber,
				);
			}
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(
					properties.hash,
					properties.prepared.coordinateEntry ?? fields,
				);
				for (const nextHash of nativeDeleteHashes) {
					this._residentEntryCoordinatesByHash.delete(nextHash);
				}
			}

			for (const coordinate of properties.coordinates) {
				this.coordinateToHash.add(coordinate, properties.hash);
			}

			if (deleteNextOptions && !coordinateIndex.putAndDelete) {
				return mapMaybePromise(
					this.entryCoordinatesIndex.del(deleteNextOptions),
					() => true,
				);
			}
			return true;
		};
		return mapMaybePromise(putResult, finish);
	}

	private persistPreparedCoordinateNativeTransaction(
		properties: {
			coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>;
			prepared: PreparedCoordinatePersistence<R>;
			hash: string;
			nextHashes: string[];
			coordinates: NumberFromType<R>[];
			deleteHashes?: string[];
			commitNative?: boolean;
			commitNativeBackbone?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): MaybePromise<boolean> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { fields } = properties.prepared;
		const putNative =
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn ??
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn;
		if (!putNative) {
			return false;
		}
		const putResult = putNative.call(
			properties.coordinateIndex,
			fields,
			combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			),
		);
		const finish = () => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeDeleteHashes = combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			);
			if (properties.commitNative !== false) {
				this._nativeSharedLogState?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					properties.prepared.assignedToRangeBoundary,
					properties.coordinates.length,
					fields.hashNumber,
				);
			}
			if (properties.commitNativeBackbone !== false) {
				this._nativeBackbone?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					properties.prepared.assignedToRangeBoundary,
					properties.coordinates.length,
					fields.hashNumber,
				);
			}
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(
					properties.hash,
					properties.prepared.coordinateEntry ?? fields,
				);
				for (const nextHash of nativeDeleteHashes) {
					this._residentEntryCoordinatesByHash.delete(nextHash);
				}
			}
			for (const coordinate of properties.coordinates) {
				this.coordinateToHash.add(coordinate, properties.hash);
			}
			return true;
		};
		return mapMaybePromise(putResult, finish);
	}

	private persistBackboneCoordinateFieldsNativeTransaction(
		properties: {
			coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>;
			fields: SharedLogCoordinateNativeFields<R>;
			hash: string;
			coordinates: NumberFromType<R>[];
			deleteHashes: string[];
			skipGenericTransientCoordinateIndex?: boolean;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): MaybePromise<boolean> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { fields } = properties;
		const useBackboneOnlyCoordinatePersistence =
			this.canUseBackboneOnlyCoordinatePersistence();
		const finish = (): MaybePromise<boolean> => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			this._nativeSharedLogState?.commitEntryCoordinates(
				properties.hash,
				fields.gid,
				properties.coordinates,
				properties.deleteHashes,
				fields.assignedToRangeBoundary,
				properties.coordinates.length,
				fields.hashNumber,
			);
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(properties.hash, fields);
				for (const deletedHash of properties.deleteHashes) {
					this._residentEntryCoordinatesByHash.delete(deletedHash);
				}
			}
			for (const coordinate of properties.coordinates) {
				this.coordinateToHash.add(coordinate, properties.hash);
			}
			if (this._nativeBackboneCoordinatePersistence) {
				const flushed = this.flushNativeBackboneCoordinateJournalOnAppend();
				if (isPromiseLike(flushed)) {
					return mapMaybePromise(flushed, () => true);
				}
			}
			return true;
		};
		if (
			(properties.skipGenericTransientCoordinateIndex &&
				this.canUseRuntimeOnlyNativeBackboneCoordinates(
					properties.coordinateIndex,
				)) ||
			useBackboneOnlyCoordinatePersistence
		) {
			return finish();
		}

		const putNative =
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn ??
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn;
		if (!putNative) {
			return false;
		}
		const putResult = putNative.call(
			properties.coordinateIndex,
			fields,
			properties.deleteHashes,
		);
		return mapMaybePromise(putResult, finish);
	}

	private flushNativeBackboneCoordinateJournal(): MaybePromise<void> {
		const backbone = this._nativeBackbone;
		const persistence = this._nativeBackboneCoordinatePersistence;
		if (!backbone || !persistence || this._nativeBackboneDropStarted) {
			return undefined;
		}
		if (
			backbone.coordinatePendingJournalLength === 0 &&
			backbone.documentPendingJournalLength === 0 &&
			backbone.documentSignerPendingJournalLength === 0
		) {
			return undefined;
		}
		return mapMaybePromise(persistence.flushJournal(backbone), () => {
			this._nativeBackboneCoordinateJournalLastFlushMs = Date.now();
			return undefined;
		});
	}

	private flushNativeBackboneCoordinateJournalOnAppend(): MaybePromise<void> {
		const backbone = this._nativeBackbone;
		const persistence = this._nativeBackboneCoordinatePersistence;
		if (!backbone || !persistence || this._nativeBackboneDropStarted) {
			return undefined;
		}
		if (persistence.flushJournalOnAppend) {
			const flushed = persistence.flushJournalOnAppend(backbone);
			if (!isPromiseLike(flushed)) {
				return undefined;
			}
			return mapMaybePromise(flushed, () => {
				return undefined;
			});
		}
		if (!this.shouldFlushNativeBackboneCoordinateJournalOnAppend()) {
			return undefined;
		}
		return this.flushNativeBackboneCoordinateJournal();
	}

	private shouldFlushNativeBackboneCoordinateJournalOnAppend(): boolean {
		const persistence = this._nativeBackboneCoordinatePersistence;
		if (!persistence || persistence.flushOnAppend !== false) {
			return true;
		}
		const backbone = this._nativeBackbone;
		if (!backbone || backbone.coordinatePendingJournalLength === 0) {
			return false;
		}
		if (
			persistence.flushMaxPendingBytes != null &&
			backbone.coordinatePendingJournalByteLength >=
				persistence.flushMaxPendingBytes
		) {
			return true;
		}
		return (
			persistence.flushIntervalMs != null &&
			Date.now() - this._nativeBackboneCoordinateJournalLastFlushMs >=
				persistence.flushIntervalMs
		);
	}

	private async closeNativeBackboneCoordinatePersistence(): Promise<void> {
		const persistence = this._nativeBackboneCoordinatePersistence;
		if (!persistence) {
			return;
		}
		if (this._nativeBackboneDropStarted) {
			// `drop()` owns the durable namespace lifecycle. Never flush the live wasm
			// journals or invoke an ordinary custom close after its tombstone/erase has
			// started: a close implementation that rewrites cached state could resurrect
			// files after a successful terminal drop.
			return;
		}
		if (
			this._nativeDurableCommitFailure &&
			!this._nativeDurableRecoveryReadyForReopen
		) {
			// The failed native transaction was never published by the lower log.
			// Its coordinate/document/signer records are still only in the wasm
			// pending journals. Closing without flushing discards that generation;
			// the next backbone hydrates the last acknowledged checkpoint.
			await persistence.close?.();
			this._nativeDurableRecoveryReadyForReopen = true;
			return;
		}
		await this.flushNativeBackboneCoordinateJournal();
		await persistence.close?.();
	}

	private canUseBackboneOnlyCoordinatePersistence(): boolean {
		return (
			!!this._nativeBackboneCoordinatePersistence &&
			this.canUseNativeBackboneResidentCoordinateState()
		);
	}

	private canUseNativeBackboneResidentCoordinateState(): boolean {
		return (
			!!this._nativeBackbone &&
			!!this._residentEntryCoordinatesByHash &&
			!this.hasCustomFindLeaders()
		);
	}

	private canUseRuntimeOnlyNativeBackboneCoordinates(
		coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>,
	): boolean {
		if (
			!this.canUseNativeBackboneResidentCoordinateState() ||
			Object.prototype.hasOwnProperty.call(
				coordinateIndex,
				"putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn",
			) ||
			Object.prototype.hasOwnProperty.call(
				coordinateIndex,
				"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
			)
		) {
			return false;
		}
		const persisted = (
			coordinateIndex as PutAndDeleteIndex<EntryReplicated<R>> & {
				persisted?: () => MaybePromise<boolean>;
			}
		).persisted?.();
		return persisted === false;
	}

	private async persistCoordinate(
		properties: {
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
			commitNativeBackbone?: boolean;
			deleteHashes?: string[];
			hashNumber?: NumberFromType<R>;
			nextHashes?: string[];
			prepared?: PreparedCoordinatePersistence<R>;
		},
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	) {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const prepared =
			properties.prepared ?? this.createCoordinatePersistenceEntry(properties);
		if (!prepared) {
			return false;
		}
		return this.persistPreparedCoordinate(
			{
				prepared,
				hash: properties.entry.hash,
				nextHashes: properties.nextHashes ?? properties.entry.meta.next,
				coordinates: properties.coordinates,
				replicas: properties.replicas,
				commitNative: properties.commitNative,
				commitNativeBackbone: properties.commitNativeBackbone,
				deleteHashes: properties.deleteHashes,
			},
			ownershipLifecycleController,
		);
	}

	private async persistCoordinatesBatch(
		items: CoordinatePersistBatchItem<R>[],
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
	): Promise<boolean[]> {
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (items.length === 0) {
			return [];
		}

		const prepared = items.map((item) => ({
			item,
			prepared: item.prepared ?? this.createCoordinatePersistenceEntry(item),
		}));
		const changed = prepared.filter(
			(
				entry,
			): entry is {
				item: (typeof items)[number];
				prepared: PreparedCoordinatePersistence<R>;
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

		if (
			coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn
		) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn(
				changed.map(({ item, prepared }) => ({
					fields: prepared.fields,
					deleteHashes: item.entry.meta.next,
				})),
			);
		} else if (
			coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch
		) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch(
				changed.map(({ item, prepared }) => ({
					fields: prepared.fields,
					deleteHashes: item.entry.meta.next,
				})),
			);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch(
				changed.map(({ item, prepared }) => ({
					fields: prepared.fields,
					deleteIds: item.entry.meta.next,
					id: toId(prepared.fields.hash),
				})),
			);
		} else if (coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch(
				changed.map(({ item, prepared }) => ({
					value: this.materializePreparedCoordinateEntry(prepared),
					fields: prepared.fields,
					deleteIds: item.entry.meta.next,
					id: toId(prepared.fields.hash),
				})),
			);
		} else if (canUseGenericPutBatch) {
			await coordinateIndex.putBatch!(
				changed.map(({ prepared }) =>
					this.materializePreparedCoordinateEntry(prepared),
				),
			);
		} else {
			const results: boolean[] = [];
			for (const item of items) {
				results.push(
					await this.persistCoordinate(item, ownershipLifecycleController),
				);
				this.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
			return results;
		}
		this.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);

		const nativeCoordinateCommits = changed.filter(
			({ item }) => item.commitNative !== false,
		);
		if (nativeCoordinateCommits.length > 0 && this._nativeSharedLogState) {
			if (this._nativeSharedLogState.commitEntryCoordinatesBatch) {
				this._nativeSharedLogState.commitEntryCoordinatesBatch(
					nativeCoordinateCommits.map(({ item, prepared }) => ({
						hash: item.entry.hash,
						gid: prepared.fields.gid,
						coordinates: item.coordinates,
						nextHashes: item.entry.meta.next,
						assignedToRangeBoundary: prepared.assignedToRangeBoundary,
						requestedReplicas: item.replicas,
						hashNumber: prepared.fields.hashNumber,
					})),
				);
			} else {
				for (const { item, prepared } of nativeCoordinateCommits) {
					this._nativeSharedLogState.commitEntryCoordinates(
						item.entry.hash,
						prepared.fields.gid,
						item.coordinates,
						item.entry.meta.next,
						prepared.assignedToRangeBoundary,
						item.replicas,
						prepared.fields.hashNumber,
					);
				}
			}
		}

		const nativeBackboneCoordinateCommits = changed.filter(
			({ item }) => item.commitNativeBackbone !== false,
		);
		if (nativeBackboneCoordinateCommits.length > 0 && this._nativeBackbone) {
			if (this._nativeBackbone.commitEntryCoordinatesBatch) {
				this._nativeBackbone.commitEntryCoordinatesBatch(
					nativeBackboneCoordinateCommits.map(({ item, prepared }) => ({
						hash: item.entry.hash,
						gid: prepared.fields.gid,
						coordinates: item.coordinates,
						nextHashes: item.entry.meta.next,
						assignedToRangeBoundary: prepared.assignedToRangeBoundary,
						requestedReplicas: item.replicas,
						hashNumber: prepared.fields.hashNumber,
					})),
				);
			} else {
				for (const { item, prepared } of nativeBackboneCoordinateCommits) {
					this._nativeBackbone.commitEntryCoordinates(
						item.entry.hash,
						prepared.fields.gid,
						item.coordinates,
						item.entry.meta.next,
						prepared.assignedToRangeBoundary,
						item.replicas,
						prepared.fields.hashNumber,
					);
				}
			}
		}

		for (const { item, prepared } of changed) {
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(
					item.entry.hash,
					prepared.coordinateEntry ?? prepared.fields,
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
			changed.map(({ prepared }) => prepared.fields.hash),
		);
		return items.map((item) => changedHashes.has(item.entry.hash));
	}

	private async deleteCoordinates(
		properties: { hash: string },
		ownershipLifecycleController?: AbortController,
	) {
		if (ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		this._nativeSharedLogState?.deleteEntryCoordinates(properties.hash);
		this._nativeBackbone?.deleteEntryCoordinates(properties.hash);
		this._residentEntryCoordinatesByHash?.delete(properties.hash);
		const coordinateIndex = this.entryCoordinatesIndex as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIds) {
			await coordinateIndex.delIds([properties.hash]);
		} else {
			await this.entryCoordinatesIndex.del({ query: properties });
		}
		if (ownershipLifecycleController) {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
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
				(await this.persistCoordinate(
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
				const persistItems: Parameters<typeof this.persistCoordinatesBatch>[0] =
					[];
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
							this.createCoordinatePersistenceEntryFromNativePlan({
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
					await this.persistCoordinatesBatch(
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
			const persistItems: Parameters<typeof this.persistCoordinatesBatch>[0] =
				[];
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
				await this.persistCoordinatesBatch(
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
	}): Promise<boolean> {
		const backbone = this._nativeBackbone;
		if (!backbone || !this.syncronizer.onReceivedEntryHashes) {
			return false;
		}
		const receivePlanStartedAt = syncProfileStart(properties.syncProfile);
		const selection =
			properties.selection ??
			(await this.planNativePreparedRawReceiveSelection(properties));
		if (!selection || selection.retainedHashes.length > 0) {
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
	}): Promise<RawReceiveHashSelection | undefined> {
		if (!this.syncronizer.onReceivedEntryHashes) {
			return undefined;
		}
		const receivePlanStartedAt = syncProfileStart(properties.syncProfile);
		const selection =
			properties.selection ??
			(await this.planNativePreparedRawReceiveSelection(properties));
		if (!selection) {
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
			this._checkedPruneRemoveBlocksLocalRangeMutationAdmission === 0
		) {
			return onChange(change);
		}

		this._checkedPruneRemovalCallbackInvocationDepth++;
		try {
			// Deliberately return the callback promise without awaiting it. The
			// reentrancy guard covers the callback's immediate invocation, while an
			// independent operation started during a later async suspension must be
			// allowed to queue/drain behind the admitted lower-log removal.
			return onChange(change);
		} finally {
			this._checkedPruneRemovalCallbackInvocationDepth--;
		}
	}

	private withReplicationRangeMutationQueue<T>(
		fn: () => Promise<T>,
		replicationOwnershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
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
		this._replicationRangeMutationTail = run.then(
			() => undefined,
			() => undefined,
		);
		return run;
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
			const tails = [...(this._replicationInfoApplyQueueByPeer?.values() ?? [])];
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
			};
		}

		const uniqueRanges = [
			...new Map(ranges.map((range) => [range.idString, range])).values(),
		];
		const wasReplicator = this.uniqueReplicators.has(ownerHash);
		const joinWasEmitted = this._replicatorJoinEmitted.has(ownerHash);
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
		return {
			removed,
			retained,
			ownerHasRanges,
			error: outcomeError,
		};
	}

	private advanceReplicationInfoReceiveEpoch(peerHash: string): object {
		const next = {};
		this._replicationInfoReceiveEpochByPeer.set(peerHash, next);
		return next;
	}

	private getReplicationInfoReceiveEpoch(peerHash: string): object | null {
		return this._replicationInfoReceiveEpochByPeer.get(peerHash) ?? null;
	}

	private isCurrentReplicationInfoReceiveEpoch(
		peerHash: string,
		epoch: object | null,
	): boolean {
		return this.getReplicationInfoReceiveEpoch(peerHash) === epoch;
	}

	private advanceSubscriptionEpoch(peerHash: string): object {
		const next = {};
		this._subscriptionEpochByPeer.set(peerHash, next);
		return next;
	}

	private getSubscriptionEpoch(peerHash: string): object | null {
		return this._subscriptionEpochByPeer.get(peerHash) ?? null;
	}

	private isCurrentSubscriptionEpoch(
		peerHash: string,
		epoch: object | null,
	): boolean {
		return this.getSubscriptionEpoch(peerHash) === epoch;
	}

	private cancelReplicationInfoRequests(peerHash: string) {
		const state = this._replicationInfoRequestByPeer.get(peerHash);
		if (!state) return;
		if (state.timer) {
			clearTimeout(state.timer);
		}
		this._replicationInfoRequestByPeer.delete(peerHash);
	}

	private scheduleReplicationInfoRequests(
		peer: PublicSignKey,
		replicationLifecycleController = this._replicationLifecycleController,
	) {
		if (
			!replicationLifecycleController ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}
		const peerHash = peer.hashcode();
		const requestStates = this._replicationInfoRequestByPeer;
		if (requestStates.has(peerHash)) {
			return;
		}

		const state: { attempts: number; timer?: ReturnType<typeof setTimeout> } = {
			attempts: 0,
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
		const maxAttempts =
			this.waitForReplicatorRequestMaxAttempts ??
			Math.max(
				WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS,
				Math.ceil(this.waitForReplicatorTimeout / intervalMs),
			);

		const tick = () => {
			if (!this.isReplicationLifecycleActive(replicationLifecycleController)) {
				cancel();
				return;
			}

			state.attempts++;

			this.rpc
				.send(new RequestReplicationInfoMessage(), {
					mode: new AcknowledgeDelivery({ redundancy: 1, to: [peer] }),
					signal: replicationLifecycleController.signal,
				})
				.catch((e) => {
					// Best-effort: missing peers / unopened RPC should not fail join flows.
					if (
						isNotStartedError(e as Error) ||
						(replicationLifecycleController.signal.aborted &&
							e instanceof AbortError)
					) {
						return;
					}
					logger.error(e?.toString?.() ?? String(e));
				});

			if (state.attempts >= maxAttempts) {
				cancel();
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
		subscriptionEpoch?: object,
	) {
		if (!topics.includes(this.topic)) {
			return;
		}
		const replicationLifecycleController = this._replicationLifecycleController;
		if (
			!replicationLifecycleController ||
			!this.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}

		const peerHash = publicKey.hashcode();
		const expectedSubscriptionEpoch =
			subscriptionEpoch ?? this.advanceSubscriptionEpoch(peerHash);
		const ownsSubscriptionEpoch = () =>
			this.isCurrentSubscriptionEpoch(peerHash, expectedSubscriptionEpoch);
		if (!ownsSubscriptionEpoch()) {
			return;
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
			this._subscriptionOpeningEpochByPeer.set(
				peerHash,
				expectedSubscriptionEpoch,
			);
			// Fence new messages immediately, drain handlers admitted by the previous
			// subscription, then wait behind every queued replication mutation. A
			// reconnect must not inherit metadata or ranges from the old connection.
			try {
				this._replicationInfoBlockedPeers.add(peerHash);
				await this.drainPeerReceiveHandlers(peerHash);
				await this.withReplicationInfoApplyQueue(peerHash, async () => {});
				if (
					!this.isReplicationLifecycleActive(replicationLifecycleController) ||
					!ownsSubscriptionEpoch()
				) {
					return;
				}
				// The timestamp watermark belongs to the previous subscription epoch.
				// Sender clocks are not synchronized, so carrying a local unsubscribe
				// timestamp forward could reject every valid announcement after reconnect.
				this.latestReplicationInfoMessage.delete(peerHash);
				this._pendingReplicatorLeaveByPeer.delete(peerHash);
				const openingCapabilities =
					this._openingSyncCapabilitiesByPeer.get(peerHash);
				if (openingCapabilities?.epoch === expectedSubscriptionEpoch) {
					this._peerSyncCapabilities.set(
						peerHash,
						openingCapabilities.capabilities,
					);
					this._openingSyncCapabilitiesByPeer.delete(peerHash);
				}
				this._replicationInfoBlockedPeers.delete(peerHash);
			} finally {
				if (
					this._openingSyncCapabilitiesByPeer.get(peerHash)?.epoch ===
					expectedSubscriptionEpoch
				) {
					this._openingSyncCapabilitiesByPeer.delete(peerHash);
				}
				if (
					this._subscriptionOpeningEpochByPeer.get(peerHash) ===
					expectedSubscriptionEpoch
				) {
					this._subscriptionOpeningEpochByPeer.delete(peerHash);
				}
			}
		}
		if (!subscribed) {
			this._subscriptionOpeningEpochByPeer.delete(peerHash);
			this._openingSyncCapabilitiesByPeer.delete(peerHash);
			this._replicationInfoBlockedPeers.add(peerHash);
			const disconnectedJoinWarmupGeneration =
				this._joinWarmupGenerationByTarget.get(peerHash) ?? null;
			this.cancelJoinWarmupTarget(peerHash);

			const now = BigInt(+new Date());
			const previous = this.latestReplicationInfoMessage.get(peerHash);
			if (!previous || previous < now) {
				this.latestReplicationInfoMessage.set(peerHash, now);
			}

			let removed = false;
			try {
				// Unsubscribe can race with the peer's final replication reset message.
				// Proactively evict its ranges so leader selection doesn't keep stale owners.
				removed = await this.removeReplicator(publicKey, {
					cleanupIfSubscriptionSuperseded: true,
					expectedJoinWarmupGeneration:
						disconnectedJoinWarmupGeneration,
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

		this._replicationInfoBlockedPeers.delete(peerHash);
		this._replicatorLivenessFailures.delete(peerHash);
		this.markReplicatorActivity(peerHash);

		if (this._logProperties?.sync?.rawExchangeHeads === true) {
			// One-shot capability advertisement so live append gossip can pick
			// the raw exchange-heads path for this peer without a per-request
			// round trip. Peers that do not know the message drop it.
			this.rpc
				.send(
					new SyncCapabilitiesMessage({
						capabilities: SYNC_CAPABILITY_RAW_EXCHANGE_HEADS,
					}),
					{
						mode: new SilentDelivery({ redundancy: 1, to: [publicKey] }),
						signal: replicationLifecycleController.signal,
					},
				)
				.catch((error) =>
					this.handleReplicationLifecycleSendError(
						error,
						replicationLifecycleController,
					),
				);
		}

		let replicationSegments: ReplicationRangeIndexable<R>[];
		try {
			replicationSegments = await this.getMyReplicationSegments();
		} catch (error) {
			if (
				!this.isReplicationLifecycleActive(replicationLifecycleController) &&
				isNotStartedError(error as Error)
			) {
				return;
			}
			throw error;
		}
		if (
			!this.isReplicationLifecycleActive(replicationLifecycleController) ||
			!ownsSubscriptionEpoch()
		) {
			return;
		}
		if (replicationSegments.length > 0) {
			const segments = replicationSegments.map((x) => x.toReplicationRange());
			this.validatePersistedReplicationRangeSnapshot(segments);
			await this.rpc
				.send(
					new AllReplicatingSegmentsMessage({
						segments,
					}),
					{
						mode: new AcknowledgeDelivery({ redundancy: 1, to: [publicKey] }),
						signal: replicationLifecycleController.signal,
					},
				)
				.catch((error) =>
					this.handleReplicationLifecycleSendError(
						error,
						replicationLifecycleController,
					),
				);
			if (
				!this.isReplicationLifecycleActive(replicationLifecycleController) ||
				!ownsSubscriptionEpoch()
			) {
				return;
			}

			if (this.v8Behaviour) {
				// for backwards compatibility
				await this.rpc
					.send(
						new ResponseRoleMessage({
							role: this.getRoleFromReplicationSegments(replicationSegments),
						}),
						{
							mode: new AcknowledgeDelivery({
								redundancy: 1,
								to: [publicKey],
							}),
							signal: replicationLifecycleController.signal,
						},
					)
					.catch((error) =>
						this.handleReplicationLifecycleSendError(
							error,
							replicationLifecycleController,
						),
					);
			}
		}

		// Request the remote peer's replication info. This makes joins resilient to
		// timing-sensitive delivery/order issues where we may miss their initial
		// replication announcement.
		if (
			this.isReplicationLifecycleActive(replicationLifecycleController) &&
			ownsSubscriptionEpoch()
		) {
			this.scheduleReplicationInfoRequests(
				publicKey,
				replicationLifecycleController,
			);
		}
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
		this._checkedPrune.removeRequestSent(hash, to);
	}

	private removePruneRequestsSent(hashes: Iterable<string>, to?: string) {
		this._checkedPrune.removeRequestsSent(hashes, to);
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
				this.removePruneRequestSent(x.entry.hash);
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
		const isCheckedPruneLifecycleCurrent = () =>
			this.isRepairLifecycleActive(ownershipLifecycleController) &&
			this._checkedPrune === checkedPruneCoordinator &&
			this._closeController === closeController;
		const throwIfCheckedPruneLifecycleInactive = () => {
			this.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			if (
				this._checkedPrune !== checkedPruneCoordinator ||
				this._closeController !== closeController
			) {
				throw new TerminalOperationNotStartedError(
					"Checked prune lifecycle is no longer active",
				);
			}
		};

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
			let finalOwnershipRevalidationFailed = false;

			const clear = () => {
				const pending = checkedPruneCoordinator.getPendingDelete(entry.hash);
				if (pending?.promise === deferredPromise) {
					checkedPruneCoordinator.deletePendingDelete(entry.hash, pending);
				}
				clearTimeout(timeout);
			};

			const resolve = () => {
				try {
					throwIfCheckedPruneLifecycleInactive();
				} catch (error) {
					reject(error);
					return;
				}
				clearTimeout(timeout);
				checkedPruneCoordinator.clearRetry(entry.hash);
				cleanupTimer.push(
					setTimeout(() => {
						const run = async () => {
							throwIfCheckedPruneLifecycleInactive();
							const ownership = await this.revalidateCheckedPruneOwnership({
								hash: entry.hash,
								entry,
								leaders: this.checkedPruneLeadersToMap(leaders),
								selfReplicating: true,
								ownershipLifecycleController,
								checkedPruneCoordinator,
							});
							throwIfCheckedPruneLifecycleInactive();
							if (ownership.localLeader) {
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

							try {
								const removed = await this.withReplicationRangeMutationQueue(
									async () => {
										throwIfCheckedPruneLifecycleInactive();
										// The network confirmation and preliminary planner check
										// deliberately happen outside the global ownership lane. Once
										// admitted here, re-read leadership and keep the lower-log
										// delete in the same lane so a durable range mutation cannot
										// commit between the decision and the destructive operation.
										let finalOwnership: {
											leaders: CheckedPruneLeaderMap;
											localLeader: boolean;
										};
										try {
											finalOwnership =
												await this.revalidateCheckedPruneOwnership({
													hash: entry.hash,
													entry,
													leaders: this.checkedPruneLeadersToMap(leaders),
													selfReplicating: true,
													requireFreshLeaderDecision: true,
													ownershipLifecycleController,
													checkedPruneCoordinator,
												});
										} catch (error) {
											finalOwnershipRevalidationFailed = true;
											throw error;
										}
										throwIfCheckedPruneLifecycleInactive();
										if (finalOwnership.localLeader) {
											return false;
										}

										this.deleteGidPeerHistory(entry.meta.gid);
										checkedPruneCoordinator.removeRequestSent(entry.hash);
										checkedPruneCoordinator.clearConfirmedReplicators(
											entry.hash,
										);
										throwIfCheckedPruneLifecycleInactive();
										checkedPruneCoordinator.markRemoving(entry.hash);
										this._checkedPruneRemoveBlocksLocalRangeMutationAdmission++;
										try {
											await this.trackAdmittedPruneRemove(
												() =>
													this.log.remove(entry, {
														recursively: true,
													}),
												ownershipLifecycleController,
											);
										} finally {
											this
												._checkedPruneRemoveBlocksLocalRangeMutationAdmission--;
										}
										clear();
										checkedPruneCoordinator.markDone(entry.hash);
										deferredPromise.resolve();
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
								clear();
								checkedPruneCoordinator.markCancelled(entry.hash, {
									preserveRetry:
										!explicitTimeout && finalOwnershipRevalidationFailed,
								});
								if (!explicitTimeout && finalOwnershipRevalidationFailed) {
									this.scheduleCheckedPruneRetry(
										{ entry, leaders },
										ownershipLifecycleController,
									);
								}
								deferredPromise.reject(error);
								return;
							}

							// The delete has already settled. Diagnostics are best-effort:
							// poison/close/reopen must not turn an ignored timer callback into
							// an unhandled rejection or mutate the next lifecycle.
							if (!isCheckedPruneLifecycleCurrent()) {
								return;
							}
							try {
								this.deleteGidPeerHistory(entry.meta.gid);
								checkedPruneCoordinator.removeRequestSent(entry.hash);
								checkedPruneCoordinator.clearConfirmedReplicators(entry.hash);
								const postRemoveOwnership =
									await this.revalidateCheckedPruneOwnership({
										hash: entry.hash,
										entry,
										leaders: this.checkedPruneLeadersToMap(leaders),
										selfReplicating: true,
										ownershipLifecycleController,
										checkedPruneCoordinator,
									});
								if (
									isCheckedPruneLifecycleCurrent() &&
									postRemoveOwnership.localLeader
								) {
									logger.error("Unexpected: Is leader after delete");
								}
							} catch (error) {
								if (
									isCheckedPruneLifecycleCurrent() &&
									!isNotStartedError(error as Error)
								) {
									logger.error(error);
								}
							}
						};
						void run().catch((error) => {
							reject(error);
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
				checkedPruneCoordinator.markCancelled(entry.hash, {
					preserveRetry: !explicitTimeout && isCheckedPruneTimeout,
				});
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
				if (!explicitTimeout && isCheckedPruneLifecycleCurrent()) {
					this.scheduleCheckedPruneRetry(
						{ entry, leaders },
						ownershipLifecycleController,
					);
				}
				reject(
					new Error(
						`Timeout for checked pruning after ${checkedPruneTimeoutMs}ms (closed=${this.closed})`,
					),
				);
			}, checkedPruneTimeoutMs);
			timeout.unref?.();

			checkedPruneCoordinator.setPendingDelete(
				entry.hash,
				{
					promise: deferredPromise,
					clear,
					reject,
					resolve: async (publicKeyHash: string) => {
						try {
							throwIfCheckedPruneLifecycleInactive();
						} catch (error) {
							reject(error);
							return;
						}
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
						try {
							throwIfCheckedPruneLifecycleInactive();
						} catch (error) {
							reject(error);
							return;
						}

						const existCounter = checkedPruneCoordinator.addConfirmedReplicator(
							entry.hash,
							publicKeyHash,
						);
						// Seed provider hints so future remote reads can avoid extra round-trips.
						this.remoteBlocks.hintProviders(entry.hash, [publicKeyHash]);

						if (minReplicasValue <= existCounter.size) {
							resolve();
						}
					},
				},
				entry,
				leaders,
			);

			promises.push(deferredPromise.promise);
		}

		const emitMessages = async (entries: string[], to: string) => {
			throwIfCheckedPruneLifecycleInactive();
			const filteredSet: string[] = [];
			for (const entry of entries) {
				/* TODO why can we not have this statement? 
				if (set.has(to)) {
					continue;
				} */
				checkedPruneCoordinator.addRequestSent(entry, to);
				filteredSet.push(entry);
			}
			if (filteredSet.length > 0) {
				const result = await this.rpc.send(
					new RequestIPrune({
						hashes: filteredSet,
					}),
					{
						mode: new AcknowledgeDelivery({
							to: [to], // TODO group by peers?
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
			this._gidPeersHistory.clear();
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
		await waitFor(() => this._pendingDeletes.size === 0, options);
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
		const joinWarmupGenerations = new Map<string, object>();
		for (const change of changes) {
			if (change.type === "added" && change.range.hash !== selfHash) {
				joinWarmupGenerations.set(
					change.range.hash,
					this.getJoinWarmupGeneration(change.range.hash),
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
			this._joinWarmupGenerationByTarget.get(target) ===
				joinWarmupGenerations.get(target);
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
									this.removePruneRequestSent(entryReplicated.hash);
								}
							}
						}

						for (const [peer] of currentPeers) {
							if (isCurrentJoinWarmupTarget(peer)) {
								this.markRepairSweepOptimisticPeer(
									entryReplicated.gid,
									peer,
									joinWarmupGenerations.get(peer)!,
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

							this.responseToPruneDebouncedFn.delete(entryReplicated.hash);
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
								this.removePruneRequestSent(entryReplicated.hash);
							}
						}
					}

					for (const [peer] of currentPeers) {
						if (isCurrentJoinWarmupTarget(peer)) {
							this.markRepairSweepOptimisticPeer(
								entryReplicated.gid,
								peer,
								joinWarmupGenerations.get(peer)!,
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

						this.responseToPruneDebouncedFn.delete(entryReplicated.hash); // don't allow others to prune because of expecting me to replicating this entry
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
							joinWarmupGenerations,
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
		const subscriptionEpoch = this.advanceSubscriptionEpoch(fromHash);
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
		const subscriptionEpoch = this.advanceSubscriptionEpoch(fromHash);
		this.remoteBlocks.onReachable(evt.detail.from);
		this._replicationInfoBlockedPeers.add(fromHash);
		this.invalidateSharedLogTopicSubscribersCache();

		await this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.topics,
			true,
			subscriptionEpoch,
		);
	}

	async rebalanceParticipation(
		ownershipLifecycleController = this.captureReplicationOwnershipLifecycle(),
		rebalanceParticipationDebounced = this.rebalanceParticipationDebounced,
	) {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu
		const isCurrent = () =>
			this.isRepairLifecycleActive(ownershipLifecycleController) &&
			this.rebalanceParticipationDebounced === rebalanceParticipationDebounced;

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

					try {
						await this.startAnnounceReplicating(
							[dynamicRange],
							{
								checkDuplicates: false,
								reset: false,
							},
							ownershipLifecycleController,
						);
						if (!isCurrent()) return false;
					} catch (error) {
						if (
							isTransientReplicationAnnouncementError(error) &&
							this._replicationAnnouncementRetryPending
						) {
							return false;
						}
						throw error;
					}

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
