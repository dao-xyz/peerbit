import { BorshError, deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { AnyBlockStore, RemoteBlocks } from "@peerbit/blocks";
import { cidifyString } from "@peerbit/blocks-interface";
import { Cache } from "@peerbit/cache";
import {
	AccessError,
	PublicSignKey,
	getPublicKeyFromPeerId,
	sha256Base64Sync,
	sha256Sync,
} from "@peerbit/crypto";
import {
	And,
	ByteMatchQuery,
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
	Entry,
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
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	MessageHeader,
	NotStartedError,
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
import { concat } from "uint8arrays";
import { BlocksMessage } from "./blocks.js";
import { type CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
import {
	type DebouncedAccumulatorMap,
	debouncedAccumulatorMap,
} from "./debounce.js";
import { NoPeersError } from "./errors.js";
import {
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
import { SimpleSyncronizer } from "./sync/simple.js";
import { groupByGid } from "./utils.js";

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

const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;

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

export type DeliveryOptions = {
	settle?: true | { min: number };
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
		entry: Entry<T> | ShallowEntry | EntryReplicated<R>;
		leaders: Map<string, any>;
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

	// regular distribution checks
	private distributeQueue?: PQueue;

	syncronizer!: Syncronizer<R>;

	replicas!: ReplicationLimits;

	private cpuUsage?: CPUUsage;

	timeUntilRoleMaturity!: number;
	waitForReplicatorTimeout!: number;
	waitForReplicatorRequestIntervalMs!: number;
	waitForReplicatorRequestMaxAttempts?: number;
	waitForPruneDelay!: number;
	distributionDebounceTime!: number;

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

		const fanoutService = (this.node.services as any).fanout;
		if (!fanoutService) {
			throw new Error(
				`Fanout is configured for shared-log topic ${this.topic}, but no fanout service is available on this client`,
			);
		}

		const resolvedRoot =
			options.root ??
			(await (fanoutService as any)?.topicRootControlPlane?.resolveTopicRoot?.(
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
		requireRecipients: boolean;
		settleMin?: number;
		wrap?: (promise: Promise<void>) => Promise<void>;
	} {
		const delivery: DeliveryOptions | undefined =
			deliveryArg === undefined || deliveryArg === false
				? undefined
				: deliveryArg === true
					? {}
					: deliveryArg;
		if (!delivery) {
			return {
				delivery: undefined,
				requireRecipients: false,
				settleMin: undefined,
				wrap: undefined,
			};
		}

		const deliverySettle = delivery.settle ?? true;
		const deliveryTimeout = delivery.timeout;
		const deliverySignal = delivery.signal;
		const requireRecipients = delivery.requireRecipients === true;
		const settleMin =
			typeof deliverySettle === "object" && Number.isFinite(deliverySettle.min)
				? Math.max(0, Math.floor(deliverySettle.min))
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
			requireRecipients,
			settleMin,
			wrap,
		};
	}

		private async _appendDeliverToReplicators(
			entry: Entry<T>,
			minReplicasValue: number,
			leaders: Map<string, any>,
			selfHash: string,
			isLeader: boolean,
			deliveryArg: false | true | DeliveryOptions | undefined,
		) {
			const { delivery, requireRecipients, settleMin, wrap } =
				this._parseDeliveryOptions(deliveryArg);
			const pending: Promise<void>[] = [];
			const track = (promise: Promise<void>) => {
				pending.push(wrap ? wrap(promise) : promise);
			};
			const fanoutUnicastOptions =
				delivery?.timeout != null || delivery?.signal != null
					? { timeoutMs: delivery.timeout, signal: delivery.signal }
					: undefined;

			for await (const message of createExchangeHeadsMessages(this.log, [entry])) {
				await this._mergeLeadersFromGidReferences(message, minReplicasValue, leaders);
				const leadersForDelivery = delivery ? new Set(leaders.keys()) : undefined;

				const set = this.addPeersToGidPeerHistory(entry.meta.gid, leaders.keys());
				const hasRemotePeers = set.has(selfHash) ? set.size > 1 : set.size > 0;
				if (!hasRemotePeers) {
					if (requireRecipients) {
					throw new NoPeersError(this.rpc.topic);
				}
				continue;
			}

				if (!delivery) {
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
				// Default delivery semantics: require enough remote ACKs to reach the requested
				// replication degree (local append counts as 1).
				const ackLimit =
					settleMin == null ? Math.max(0, minReplicasValue - 1) : settleMin;

				for (const peer of orderedRemoteRecipients) {
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
							// Unified decision point:
							// - If we can prove a cheap direct path (connected or routed), use it.
							// - Otherwise, fall back to the fanout unicast ACK path (bounded overlay routing).
							// - If that fails, fall back to pubsub/RPC routing which may flood to discover routes.
							const pubsub: any = this.node.services.pubsub as any;
							const canDirectFast =
								Boolean(pubsub?.peers?.get?.(peer)?.isWritable) ||
								Boolean(
									pubsub?.routes?.isReachable?.(
										pubsub?.publicKeyHash,
										peer,
										0,
									),
								);

							if (canDirectFast) {
								await this.rpc.send(message, {
									mode: new AcknowledgeDelivery({
										redundancy: 1,
										to: [peer],
									}),
								});
								return;
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
									// fall back below
								}
							}
							await this.rpc.send(message, {
								mode: new AcknowledgeDelivery({
									redundancy: 1,
									to: [peer],
								}),
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
				if (coordinates == null) {
					coordinates = await this.createCoordinates(gidEntry, minReplicasValue);
				}

				const found = await this._findLeaders(coordinates);
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
		const fanoutService = (this.node.services as any).fanout;
		return (
			fanoutService?.getPublicKey?.(hash) ??
			this.node.services.pubsub.getPublicKey(hash)
		);
	}

	private async _getTopicSubscribers(
		topic: string,
	): Promise<PublicSignKey[] | undefined> {
		const maxPeers = 64;

		// Prefer the bounded peer set we already know from the fanout overlay.
		if (this._fanoutChannel && (topic === this.topic || topic === this.rpc.topic)) {
			const hashes = this._fanoutChannel
				.getPeerHashes({ includeSelf: false })
				.slice(0, maxPeers);
			if (hashes.length === 0) return [];

			const keys = await Promise.all(
				hashes.map((hash) => this._resolvePublicKeyFromHash(hash)),
			);
			const uniqueKeys: PublicSignKey[] = [];
			const seen = new Set<string>();
			const selfHash = this.node.identity.publicKey.hashcode();
			for (const key of keys) {
				if (!key) continue;
				const hash = key.hashcode();
				if (hash === selfHash) continue;
				if (seen.has(hash)) continue;
				seen.add(hash);
				uniqueKeys.push(key);
			}
			return uniqueKeys;
		}

		const selfHash = this.node.identity.publicKey.hashcode();
		const hashes: string[] = [];

		// Best-effort provider discovery (bounded). This requires bootstrap trackers.
		try {
			const fanoutService = (this.node.services as any).fanout;
			if (fanoutService?.queryProviders) {
				const ns = `shared-log|${this.topic}`;
				const seed = hashToSeed32(topic);
				const providers: string[] = await fanoutService.queryProviders(ns, {
					want: maxPeers,
					seed,
				});
				for (const h of providers ?? []) {
					if (!h || h === selfHash) continue;
					hashes.push(h);
					if (hashes.length >= maxPeers) break;
				}
			}
		} catch {
			// Best-effort only.
		}

		// Next, use already-connected peer streams (bounded and cheap).
		const peerMap: Map<string, unknown> | undefined = (this.node.services.pubsub as any)
			?.peers;
		if (peerMap?.keys) {
			for (const h of peerMap.keys()) {
				if (!h || h === selfHash) continue;
				hashes.push(h);
				if (hashes.length >= maxPeers) break;
			}
		}

		// Finally, fall back to libp2p connections (e.g. bootstrap peers) without requiring
		// any global topic membership view.
		if (hashes.length < maxPeers) {
			const connectionManager = (this.node.services.pubsub as any)?.components
				?.connectionManager;
			const connections = connectionManager?.getConnections?.() ?? [];
			for (const conn of connections) {
				const peerId = conn?.remotePeer;
				if (!peerId) continue;
				try {
					const h = getPublicKeyFromPeerId(peerId).hashcode();
					if (!h || h === selfHash) continue;
					hashes.push(h);
					if (hashes.length >= maxPeers) break;
				} catch {
					// Best-effort only.
				}
			}
		}

		if (hashes.length === 0) return [];

		const uniqueHashes: string[] = [];
		const seen = new Set<string>();
		for (const h of hashes) {
			if (seen.has(h)) continue;
			seen.add(h);
			uniqueHashes.push(h);
			if (uniqueHashes.length >= maxPeers) break;
		}

		const keys = await Promise.all(
			uniqueHashes.map((hash) => this._resolvePublicKeyFromHash(hash)),
		);
		const uniqueKeys: PublicSignKey[] = [];
		for (const key of keys) {
			if (!key) continue;
			const hash = key.hashcode();
			if (hash === selfHash) continue;
			uniqueKeys.push(key);
		}
		return uniqueKeys;
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
		let range:
			| ReplicationRangeMessage<any>[]
			| ReplicationOptions<R>
			| undefined = undefined;

		if (rangeOrEntry instanceof ReplicationRangeMessage) {
			range = rangeOrEntry;
		} else if (rangeOrEntry instanceof Entry) {
			range = {
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

		await this.updateOldestTimestampFromIndex();

		const isMe = this.node.identity.publicKey.hashcode() === keyHash;
		if (isMe) {
			// announce that we are no longer replicating

			await this.rpc.send(new AllReplicatingSegmentsMessage({ segments: [] }), {
				priority: 1,
			});
		}

		if (options?.noEvent !== true) {
			if (key instanceof PublicSignKey) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicationChangeEvent>("replication:change", {
						detail: { publicKey: key },
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
					const fanoutService = (this.node.services as any).fanout;
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
		}
	}

	addPeersToGidPeerHistory(
		gid: string,
		publicKeys: Iterable<string>,
		reset?: boolean,
	) {
		let set = this._gidPeersHistory.get(gid);
		if (!set) {
			set = new Set();
			this._gidPeersHistory.set(gid, set);
		} else {
			if (reset) {
				set.clear();
			}
		}

		for (const key of publicKeys) {
			set.add(key);
		}
		return set;
	}

	private async pruneDebouncedFnAddIfNotKeeping(args: {
		key: string;
		value: {
			entry: Entry<T> | ShallowEntry | EntryReplicated<R>;
			leaders: Map<string, any>;
		};
	}) {
		if (!this.keep || !(await this.keep(args.value.entry))) {
			return this.pruneDebouncedFn.add(args);
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
		entry: EntryReplicated<R> | ShallowOrFullEntry<any>;
		leaders: Map<string, unknown> | Set<string>;
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

			let leadersMap: Map<string, any> | undefined;
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
						leadersMap = args.leaders as any;
					} else {
						leadersMap = new Map<string, any>();
						for (const k of args.leaders) {
							leadersMap.set(k, { intersecting: true });
						}
					}
				}

				try {
					const leadersForRetry = leadersMap ?? new Map<string, any>();
					await this.pruneDebouncedFnAddIfNotKeeping({
						key: hash,
						// TODO types
						value: { entry: args.entry as any, leaders: leadersForRetry },
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

		const result = await this.log.append(data, appendOptions);

		if (options?.replicate) {
			await this.replicate(result.entry, { checkDuplicates: true });
		}

		const coordinates = await this.createCoordinates(
			result.entry,
			minReplicasValue,
		);

		const selfHash = this.node.identity.publicKey.hashcode();
		let isLeader = false;
		let leaders = await this.findLeaders(coordinates, result.entry, {
			persist: {},
			onLeader: (key) => {
				isLeader = isLeader || selfHash === key;
			},
		});

		if (options?.target !== "none") {
			const target = options?.target;
			const deliveryArg = options?.delivery;
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
				await this._appendDeliverToAllFanout(result.entry);
			} else {
				await this._appendDeliverToReplicators(
					result.entry,
					minReplicasValue,
					leaders,
					selfHash,
					isLeader,
					deliveryArg,
				);
			}
		}

		if (!isLeader) {
			this.pruneDebouncedFnAddIfNotKeeping({
				key: result.entry.hash,
				value: { entry: result.entry, leaders },
			});
		}
		this.rebalanceParticipationDebounced?.call();

		return result;
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
			this.coordinateToHash = new Cache<string>({ max: 1e6, ttl: 1e4 });
			this.recentlyRebalanced = new Cache<string>({ max: 1e4, ttl: 1e5 });

			this.uniqueReplicators = new Set();
			this._replicatorJoinEmitted = new Set();
			this._replicatorsReconciled = false;

		this.openTime = +new Date();
		this.oldestOpenTime = this.openTime;
		this.distributionDebounceTime =
			options?.distributionDebounceTime || DEFAULT_DISTRIBUTION_DEBOUNCE_TIME; // expect > 0

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
		const storage = await this.node.storage.sublevel(id);

		const localBlocks = await new AnyBlockStore(await storage.sublevel("blocks"));
		const fanoutService = (this.node.services as any).fanout as FanoutTree | undefined;
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

				// 2) fallback to currently connected RPC peers
				const self = this.node.identity.publicKey.hashcode();
				const out: string[] = [];
				const peers = (this.rpc as any)?.peers;
				for (const h of peers?.keys?.() ?? []) {
					if (h === self) continue;
					if (out.includes(h)) continue;
					out.push(h);
					if (out.length >= 32) break;
				}
				return out;
			},
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

		await this.remoteBlocks.start();

		const logScope = await this.node.indexer.scope(id);
		const replicationIndex = await logScope.scope("replication");
		this._replicationRangeIndex = await replicationIndex.init({
			schema: this.indexableDomain.constructorRange,
		});

		this._entryCoordinatesIndex = await replicationIndex.init({
			schema: this.indexableDomain.constructorEntry,
		});

		const logIndex = await logScope.scope("log");

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
			(map) => {
				this.prune(map);
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
			resolveRemotePeers: async (hash, options) => {
				if (options?.signal?.aborted) return undefined;

				const maxPeers = 8;
				const self = this.node.identity.publicKey.hashcode();
				const seed = hashToSeed32(hash);

				// Best hint: peers that have recently confirmed having this entry hash.
				const hinted = this._requestIPruneResponseReplicatorSet.get(hash);
				if (hinted && hinted.size > 0) {
					const peers = [...hinted].filter((p) => p !== self);
					return peers.length > 0
						? pickDeterministicSubset(peers, seed, maxPeers)
						: undefined;
				}

				// Next: peers we already contacted about this hash (may still have it).
				const contacted = this._requestIPruneSent.get(hash);
				if (contacted && contacted.size > 0) {
					const peers = [...contacted].filter((p) => p !== self);
					return peers.length > 0
						? pickDeterministicSubset(peers, seed, maxPeers)
						: undefined;
				}

					let candidates: string[] | undefined;

					// Prefer the replicator cache; fall back to subscribers if we have no other signal.
					const replicatorCandidates = [...this.uniqueReplicators].filter(
						(p) => p !== self,
					);
					if (replicatorCandidates.length > 0) {
						candidates = replicatorCandidates;
					} else {
						try {
							const subscribers = await this._getTopicSubscribers(this.topic);
							const subscriberCandidates =
								subscribers?.map((k) => k.hashcode()).filter((p) => p !== self) ??
								[];
							candidates =
								subscriberCandidates.length > 0 ? subscriberCandidates : undefined;
						} catch {
							// Best-effort only.
						}

						if (!candidates || candidates.length === 0) {
							// Last resort: peers we are already directly connected to. This avoids
							// depending on global membership knowledge in early-join scenarios.
							const peerMap = (this.node.services.pubsub as any)?.peers;
							if (peerMap?.keys) {
								candidates = [...peerMap.keys()];
							}
						}

						if (!candidates || candidates.length === 0) {
							// Even if the pubsub stream has no established peer streams yet, we may
							// still have a libp2p connection to one or more peers (e.g. bootstrap).
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
				},
			...this._logProperties,
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
		if (options?.syncronizer) {
			this.syncronizer = new options.syncronizer({
				numbers: this.indexableDomain.numbers,
				entryIndex: this.entryCoordinatesIndex,
				log: this.log,
				rangeIndex: this._replicationRangeIndex,
				rpc: this.rpc,
				coordinateToHash: this.coordinateToHash,
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
					sync: options?.sync,
				}) as Syncronizer<R>;
			}
		}

		// Open for communcation
		await this.rpc.open({
			queryType: TransportMessage,
			responseType: TransportMessage,
			responseHandler: (query, context) => this.onMessage(query, context),
			topic: this.topic,
		});

		this._onSubscriptionFn =
			this._onSubscriptionFn || this._onSubscription.bind(this);
		await this.node.services.pubsub.addEventListener(
			"subscribe",
			this._onSubscriptionFn,
		);

		this._onUnsubscriptionFn =
			this._onUnsubscriptionFn || this._onUnsubscription.bind(this);
		await this.node.services.pubsub.addEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn,
		);

		await this.rpc.subscribe();
		await this._openFanoutChannel(options?.fanout);

		// mark all our replicaiton ranges as "new", this would allow other peers to understand that we recently reopend our database and might need some sync and warmup
		await this.updateTimestampOfOwnedReplicationRanges(); // TODO do we need to do this before subscribing?

		// if we had a previous session with replication info, and new replication info dictates that we unreplicate
		// we should do that. Otherwise if options is a unreplication we dont need to do anything because
		// we are already unreplicated (as we are just opening)

		let isUnreplicationOptionsDefined = isUnreplicationOptions(
			options?.replicate,
		);

		const canResumeReplication =
			(await isReplicationOptionsDependentOnPreviousState(
				options?.replicate,
				this.replicationIndex,
				this.node.identity.publicKey,
			)) && hasIndexedReplicationInfo;

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

		await this.rebalanceParticipation();

		// Take into account existing subscription
		(await this._getTopicSubscribers(this.topic))?.forEach((v) => {
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
		// go through all segments and for waitForAll replicators to become reachable if not prune them away

		try {
			const promises: Promise<any>[] = [];
			const iterator = this.replicationIndex.iterate();
			let checkedIsAlive = new Set<string>();

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
								// is reachable, announce change events
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
											new CustomEvent<ReplicationChangeEvent>(
												"replication:change",
												{
													detail: { publicKey: key },
												},
											),
										);
									}
								})
							.catch(async (e) => {
								if (isNotStartedError(e)) {
									return; // TODO test this path
								}

								// not reachable
								return this.removeReplicator(segment.value.hash, {
									noEvent: true,
								}); // done announce since replicator was never reachable
							}),
					);
				}
			}
			const results = await Promise.all(promises);
			return results;
		} catch (error: any) {
			if (isNotStartedError(error)) {
				return;
			}
			throw error;
		}
	}

	async getMemoryUsage() {
		return this.log.blocks.size();
		/* ((await this.log.entryIndex?.getMemoryUsage()) || 0) */ // + (await this.log.blocks.size())
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
		this._closeController.abort();

		clearInterval(this.interval);

		this.node.services.pubsub.removeEventListener(
			"subscribe",
			this._onSubscriptionFn,
		);

		this.node.services.pubsub.removeEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn,
		);

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

			if (msg instanceof ResponseRoleMessage) {
				msg = msg.toReplicationInfoMessage(); // migration
			}

			if (msg instanceof ExchangeHeadsMessage) {
				/**
				 * I have received heads from someone else.
				 * I can use them to load associated logs and join/sync them with the data stores I own
				 */

				const { heads } = msg;

				logger.trace(
					`${this.node.identity.publicKey.hashcode()}: Recieved heads: ${
						heads.length === 1 ? heads[0].entry.hash : "#" + heads.length
					}, logId: ${this.log.idString}`,
				);

				if (heads) {
					const filteredHeads: EntryWithRefs<any>[] = [];
					for (const head of heads) {
						if (!(await this.log.has(head.entry.hash))) {
							head.entry.init({
								// we need to init because we perhaps need to decrypt gid
								keychain: this.log.keychain,
								encoding: this.log.encoding,
							});
							filteredHeads.push(head);
						}
					}

					if (filteredHeads.length === 0) {
						return;
					}

					const groupedByGid = await groupByGid(filteredHeads);
					const promises: Promise<void>[] = [];

					for (const [gid, entries] of groupedByGid) {
						const fn = async () => {
							/// we clear sync in flight here because we want to join before that, so that entries are totally accounted for
							await this.syncronizer.onReceivedEntries({
								entries,
								from: context.from!,
							});

							const headsWithGid = await this.log.entryIndex
								.getHeads(gid)
								.all();

							const latestEntry = getLatestEntry(entries)!;

							const maxReplicasFromHead =
								headsWithGid && headsWithGid.length > 0
									? maxReplicas(this, [...headsWithGid.values()])
									: this.replicas.min.getValue(this);

							const maxReplicasFromNewEntries = maxReplicas(
								this,
								entries.map((x) => x.entry),
							);

							const maxMaxReplicas = Math.max(
								maxReplicasFromHead,
								maxReplicasFromNewEntries,
							);

							const cursor = await this.createCoordinates(
								latestEntry,
								maxMaxReplicas,
							);

							const isReplicating = this._isReplicating;

							let isLeader = false;
							let fromIsLeader = false;
							let leaders: Map<string, { intersecting: boolean }> | false;
							if (isReplicating) {
								leaders = await this._waitForReplicators(
									cursor,
									latestEntry,
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
								leaders = await this.findLeaders(cursor, latestEntry, {
									onLeader: (key) => {
										fromIsLeader =
											fromIsLeader || context.from!.hashcode() === key;
										isLeader =
											isLeader ||
											this.node.identity.publicKey.hashcode() === key;
									},
								});
							}

							if (this.closed) {
								return;
							}

							let maybeDelete: EntryWithRefs<any>[][] | undefined;
							let toMerge: Entry<any>[] = [];
							let toDelete: Entry<any>[] | undefined;
							if (isLeader) {
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

							outer: for (const entry of entries) {
								if (isLeader || (await this.keep?.(entry.entry))) {
									toMerge.push(entry.entry);
								} else {
									for (const ref of entry.gidRefrences) {
										const map = await this.log.entryIndex.getHeads(ref).all();
										if (map && map.length > 0) {
											toMerge.push(entry.entry);
											(toDelete || (toDelete = [])).push(entry.entry);
											continue outer;
										}
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
								await this.log.join(toMerge);

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
									const headsWithGid = await this.log.entryIndex
										.getHeads(entries[0].entry.meta.gid)
										.all();
									if (headsWithGid && headsWithGid.length > 0) {
										const minReplicas = maxReplicas(
											this,
											headsWithGid.values(),
										);

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
				}
			} else if (msg instanceof RequestIPrune) {
				const hasAndIsLeader: string[] = [];
				const from = context.from.hashcode();

				for (const hash of msg.hashes) {
					this.removePruneRequestSent(hash, from);

					// if we expect the remote to be owner of this entry because we are to prune ourselves, then we need to remove the remote
					// this is due to that the remote has previously indicated to be a replicator to help us prune but now has changed their mind
					const outGoingPrunes =
						this._requestIPruneResponseReplicatorSet.get(hash);
					if (outGoingPrunes) {
						outGoingPrunes.delete(from);
					}

					const indexedEntry = await this.log.entryIndex.getShallow(hash);
					let isLeader = false;

					if (indexedEntry) {
						this.removePeerFromGidPeerHistory(
							context.from!.hashcode(),
							indexedEntry!.value.meta.gid,
						);

						await this._waitForReplicators(
							await this.createCoordinates(
								indexedEntry.value,
								decodeReplicas(indexedEntry.value).getValue(this),
							),
							indexedEntry.value,
							[
								{
									key: this.node.identity.publicKey.hashcode(),
									replicator: true,
								},
							],
							{
								onLeader: (key) => {
									isLeader =
										isLeader || key === this.node.identity.publicKey.hashcode();
								},
							},
						);
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
									await this._waitForReplicators(
										await this.createCoordinates(
											entry,
											decodeReplicas(entry).getValue(this),
										),
										entry,
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
			} else if (await this.syncronizer.onMessage(msg, context)) {
				return; // the syncronizer has handled the message
			} else if (msg instanceof BlocksMessage) {
				await this.remoteBlocks.onMessage(
					msg.message,
					context.from!.hashcode(),
				);
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
		if (options?.replicate && this.log.length > 0) {
			// TODO this block should perhaps be called from a callback on the this.log.join method on all the ignored element because already joined, like "onAlreadyJoined"

			// check which entrise we already have but not are replicating, and replicate them
			// we can not just do the 'join' call because it will ignore the already joined entries
			for (const element of entries) {
				if (typeof element === "string") {
					if (await this.log.has(element)) {
						const entry = await this.log.get(element);
						if (entry) {
							entriesToReplicate.push(entry);
						}
					}
				} else if (element instanceof Entry) {
					if (await this.log.has(element.hash)) {
						entriesToReplicate.push(element);
					}
				} else {
					if (await this.log.has(element.hash)) {
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
		const persistCoordinate = async (entry: Entry<T>) => {
			const minReplicas = decodeReplicas(entry).getValue(this);
			const leaders = await this.findLeaders(
				await this.createCoordinates(entry, minReplicas),
				entry,
				{ persist: {} },
			);

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

			await this.replicate(entriesToReplicate, {
				rebalance: assumeSynced ? false : true,
				checkDuplicates: true,
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
				// (`replicationChangeDebounceFn`) slightly later. Flush here so callers don't
				// observe a "late" rebalance after the wait resolves.
				await this.replicationChangeDebounceFn?.flush?.();
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
		waitFor: { key: string; replicator: boolean }[],
		options: {
			timeout?: number;
			roleAge?: number;
			onLeader?: (key: string) => void;
			// persist even if not leader
			persist?:
				| {
						prev?: EntryReplicated<R>;
				  }
				| false;
		} = { timeout: this.waitForReplicatorTimeout },
	): Promise<Map<string, { intersecting: boolean }> | false> {
		const timeout = options.timeout ?? this.waitForReplicatorTimeout;

		return new Promise((resolve, reject) => {
			const removeListeners = () => {
				this.events.removeEventListener("replication:change", roleListener);
				this.events.removeEventListener("replicator:mature", roleListener); // TODO replication:change event  ?
				this._closeController.signal.removeEventListener(
					"abort",
					abortListener,
				);
			};
			const abortListener = () => {
				removeListeners();
				clearTimeout(timer);
				resolve(false);
			};

			const timer = setTimeout(async () => {
				removeListeners();
				resolve(false);
			}, timeout);

			const check = async () => {
				let leaderKeys = new Set<string>();
				const leaders = await this.findLeaders(cursors, entry, {
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

				removeListeners();
				clearTimeout(timer);
				resolve(leaders);
			};

			const roleListener = () => {
				check();
			};

			this.events.addEventListener("replication:change", roleListener); // TODO replication:change event  ?
			this.events.addEventListener("replicator:mature", roleListener); // TODO replication:change event  ?
			this._closeController.signal.addEventListener("abort", abortListener);
			check();
		});
	}

	async createCoordinates(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R> | NumberFromType<R>,
		minReplicas: number,
	) {
		const cursor =
			typeof entry === "number" || typeof entry === "bigint"
				? entry
				: await this.domain.fromEntry(entry);
		const out = this.indexableDomain.numbers.getGrid(cursor, minReplicas);
		return out;
	}

	private async getCoordinates(entry: { hash: string }) {
		const result = await this.entryCoordinatesIndex
			.iterate({ query: { hash: entry.hash } })
			.all();
		return result[0].value.coordinates;
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
	}) {
		let assignedToRangeBoundary = shouldAssignToRangeBoundary(
			properties.leaders,
			properties.replicas,
		);

		if (
			properties.prev &&
			properties.prev.assignedToRangeBoundary === assignedToRangeBoundary
		) {
			return; // no change
		}

		const cidObject = cidifyString(properties.entry.hash);
		const hashNumber = this.indexableDomain.numbers.bytesToNumber(
			cidObject.multihash.digest,
		);

		await this.entryCoordinatesIndex.put(
			new this.indexableDomain.constructorEntry({
				assignedToRangeBoundary,
				coordinates: properties.coordinates,
				meta: properties.entry.meta,
				hash: properties.entry.hash,
				hashNumber,
			}),
		);

		for (const coordinate of properties.coordinates) {
			this.coordinateToHash.add(coordinate, properties.entry.hash);
		}

		if (properties.entry.meta.next.length > 0) {
			await this.entryCoordinatesIndex.del({
				query: new Or(
					properties.entry.meta.next.map(
						(x) => new StringMatch({ key: "hash", value: x }),
					),
				),
			});
		}
	}

	private async deleteCoordinates(properties: { hash: string }) {
		await this.entryCoordinatesIndex.del({ query: properties });
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
		let isLeader = false;

		const set = await this._findLeaders(cursors, options);
		for (const key of set.keys()) {
			if (options?.onLeader) {
				options.onLeader(key);
				isLeader = isLeader || key === this.node.identity.publicKey.hashcode();
			}
		}

		if (options?.persist !== false) {
			if (isLeader || options?.persist) {
				!this.closed &&
					(await this.persistCoordinate({
						leaders: set,
						coordinates: cursors,
						replicas: cursors.length,
						entry,
						prev: options?.persist?.prev,
					}));
			}
		}

		return set;
	}

	async isLeader(
		properties: {
			entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
			replicas: number;
		},
		options?: {
			roleAge?: number;
			onLeader?: (key: string) => void;
			// persist even if not leader
			persist?:
				| {
						prev?: EntryReplicated<R>;
				  }
				| false;
		},
	): Promise<boolean> {
		let cursors: NumberFromType<R>[] = await this.createCoordinates(
			properties.entry,
			properties.replicas,
		);

		const leaders = await this.findLeaders(cursors, properties.entry, options);
		if (leaders.has(this.node.identity.publicKey.hashcode())) {
			return true;
		}
		return false;
	}

	private async _findLeaders(
		cursors: NumberFromType<R>[],
		options?: {
			roleAge?: number;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
		const roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge()); // TODO -500 as is added so that i f someone else is just as new as us, then we treat them as mature as us. without -500 we might be slower syncing if two nodes starts almost at the same time
		const selfHash = this.node.identity.publicKey.hashcode();

		// Prefer `uniqueReplicators` (replicator cache) as soon as it has any data.
		// Falling back to live pubsub subscribers can include non-replicators and can
		// break delivery/availability when writers are not directly connected.
		let peerFilter: Set<string> | undefined = undefined;
		const selfReplicating = await this.isReplicating();
		if (this.uniqueReplicators.size > 0) {
			peerFilter = new Set(this.uniqueReplicators);
			if (selfReplicating) {
				peerFilter.add(selfHash);
			} else {
				peerFilter.delete(selfHash);
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
		return getSamples<R>(
			cursors,
			this.replicationIndex,
			roleAge,
			this.indexableDomain.numbers,
			{
				peerFilter,
				uniqueReplicators: peerFilter,
			},
		);
	}

	async findLeadersFromEntry(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
		replicas: number,
		options?: {
			roleAge?: number;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
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
		const maxAttempts = Math.min(
			5,
			this.waitForReplicatorRequestMaxAttempts ??
				WAIT_FOR_REPLICATOR_REQUEST_MIN_ATTEMPTS,
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
			if (subscribed) {
				this._replicationInfoBlockedPeers.delete(peerHash);
			} else {
				this._replicationInfoBlockedPeers.add(peerHash);
			}

			if (!subscribed) {
				// Emit replicator:leave at most once per (join -> leave) transition, even if we
				// concurrently process unsubscribe + replication reset messages for the same peer.
				const stoppedTransition = this.uniqueReplicators.delete(peerHash);
				this._replicatorJoinEmitted.delete(peerHash);

				this.cancelReplicationInfoRequests(peerHash);
				this.removePeerFromGidPeerHistory(peerHash);

				for (const [k, v] of this._requestIPruneSent) {
					v.delete(peerHash);
					if (v.size === 0) {
						this._requestIPruneSent.delete(k);
					}
				}

				for (const [k, v] of this._requestIPruneResponseReplicatorSet) {
					v.delete(peerHash);
					if (v.size === 0) {
						this._requestIPruneResponseReplicatorSet.delete(k);
					}
				}

				this.syncronizer.onPeerDisconnected(publicKey);

				stoppedTransition &&
					this.events.dispatchEvent(
						new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
							detail: { publicKey },
						}),
					);
			}

			if (subscribed) {
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
		} else {
			await this.removeReplicator(publicKey);
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
				entry: EntryReplicated<R> | ShallowOrFullEntry<any>;
				leaders: Map<string, unknown> | Set<string>;
			}
		>,
			options?: { timeout?: number; unchecked?: boolean },
		): Promise<any>[] {
		if (options?.unchecked) {
			return [...entries.values()].map((x) => {
				this._gidPeersHistory.delete(x.entry.meta.gid);
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
							this._gidPeersHistory.delete(entry.meta.gid);
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
								this._gidPeersHistory.delete(entry.meta.gid);
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

			let cursor: NumberFromType<R>[] | undefined = undefined;

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
						!(await this._waitForReplicators(
							cursor ??
								(cursor = await this.createCoordinates(
									entry,
									minReplicasValue,
								)),
							entry,
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

		const changed = false;

		try {
			const uncheckedDeliver: Map<
				string,
				Map<string, EntryReplicated<any>>
			> = new Map();

			for await (const entryReplicated of toRebalance<R>(
				changeOrChanges,
				this.entryCoordinatesIndex,
				this.recentlyRebalanced,
			)) {
				if (this.closed) {
					break;
				}

				let oldPeersSet = this._gidPeersHistory.get(entryReplicated.gid);
				let isLeader = false;

				let currentPeers = await this.findLeaders(
					entryReplicated.coordinates,
					entryReplicated,
					{
						// we do this to make sure new replicators get data even though they are not mature so they can figure out if they want to replicate more or less
						// TODO make this smarter because if a new replicator is not mature and want to replicate too much data the syncing overhead can be bad
						roleAge: 0,
					},
				);

				for (const [currentPeer] of currentPeers) {
					if (currentPeer === this.node.identity.publicKey.hashcode()) {
						isLeader = true;
						continue;
					}

					if (!oldPeersSet?.has(currentPeer)) {
						let set = uncheckedDeliver.get(currentPeer);
						if (!set) {
							set = new Map();
							uncheckedDeliver.set(currentPeer, set);
						}

						if (!set.has(entryReplicated.hash)) {
							set.set(entryReplicated.hash, entryReplicated);
						}
					}
				}

				if (oldPeersSet) {
					for (const oldPeer of oldPeersSet) {
						if (!currentPeers.has(oldPeer)) {
							this.removePruneRequestSent(entryReplicated.hash);
						}
					}
				}

				this.addPeersToGidPeerHistory(
					entryReplicated.gid,
					currentPeers.keys(),
					true,
				);

				if (!isLeader) {
					this.pruneDebouncedFnAddIfNotKeeping({
						key: entryReplicated.hash,
						value: { entry: entryReplicated, leaders: currentPeers },
					});

					this.responseToPruneDebouncedFn.delete(entryReplicated.hash); // don't allow others to prune because of expecting me to replicating this entry
				} else {
					this.pruneDebouncedFn.delete(entryReplicated.hash);
					await this._pendingDeletes
						.get(entryReplicated.hash)
						?.reject(new Error("Failed to delete, is leader again"));
					this.removePruneRequestSent(entryReplicated.hash);
				}
			}
			for (const [target, entries] of uncheckedDeliver) {
				this.syncronizer.onMaybeMissingEntries({
					entries,
					targets: [target],
				});
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

			// Keep a per-peer timestamp watermark when we observe an unsubscribe. This
			// prevents late/out-of-order replication-info messages from re-introducing
			// stale segments for a peer that has already left the topic.
			const now = BigInt(+new Date());
			const prev = this.latestReplicationInfoMessage.get(fromHash);
			if (!prev || prev < now) {
				this.latestReplicationInfoMessage.set(fromHash, now);
			}

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

			return this.handleSubscriptionChange(
				evt.detail.from,
				evt.detail.topics,
			true,
		);
	}

	async rebalanceParticipation() {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu

		const fn = async () => {
			if (this.closed) {
				return false;
			}

			// The role is fixed (no changes depending on memory usage or peer count etc)
			if (!this._isReplicating) {
				return false;
			}

			if (this._isAdaptiveReplicating) {
				const peers = this.replicationIndex;
				const usedMemory = await this.getMemoryUsage();
				let dynamicRange = await this.getDynamicRange();

				if (!dynamicRange) {
					return; // not allowed to replicate
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

		const resp = await fn();

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
