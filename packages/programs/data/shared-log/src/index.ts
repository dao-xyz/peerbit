import { BorshError, field, variant } from "@dao-xyz/borsh";
import { AnyBlockStore, RemoteBlocks } from "@peerbit/blocks";
import {
	AccessError,
	PublicSignKey,
	sha256Base64Sync,
	sha256Sync,
} from "@peerbit/crypto";
import {
	And,
	ByteMatchQuery,
	type Index,
	Or,
	Sort,
	StringMatch,
} from "@peerbit/indexer-interface";
import {
	type AppendOptions,
	type Change,
	Entry,
	Log,
	type LogEvents,
	type LogProperties,
	ShallowEntry,
	type ShallowOrFullEntry,
} from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { ClosedError, Program, type ProgramEvents } from "@peerbit/program";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { RPC, type RequestContext } from "@peerbit/rpc";
import {
	AcknowledgeDelivery,
	DeliveryMode,
	NotStartedError,
	SilentDelivery,
} from "@peerbit/stream-interface";
import {
	AbortError,
	/* delay, */
	waitFor,
} from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";
import PQueue from "p-queue";
import { concat } from "uint8arrays";
import { BlocksMessage } from "./blocks.js";
import { type CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
import {
	type DebouncedAccumulatorMap,
	debounceAcculmulator,
	debounceFixedInterval,
	debouncedAccumulatorMap,
} from "./debounce.js";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RequestIPrune,
	RequestMaybeSync,
	ResponseIPrune,
	ResponseMaybeSync,
	createExchangeHeadsMessages,
} from "./exchange-heads.js";
import { TransportMessage } from "./message.js";
import { PIDReplicationController } from "./pid.js";
import {
	EntryReplicated,
	ReplicationIntent,
	ReplicationRange,
	ReplicationRangeIndexable,
	getCoverSet,
	getEvenlySpacedU32,
	getSamples,
	hasCoveringRange,
	isMatured,
	minimumWidthToCover,
	shouldAssigneToRangeBoundary,
	toRebalance,
} from "./ranges.js";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
	hashToU32,
} from "./replication-domain-hash.js";
import {
	type ReplicationDomainTime,
	createReplicationDomainTime,
} from "./replication-domain-time.js";
import {
	type ExtractDomainArgs,
	type ReplicationChange,
	type ReplicationChanges,
	type ReplicationDomain,
	debounceAggregationChanges,
	mergeReplicationChanges,
	type u32,
} from "./replication-domain.js";
import {
	AbsoluteReplicas,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	ReplicationError,
	type ReplicationLimits,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
	StoppedReplicating,
	decodeReplicas,
	encodeReplicas,
	maxReplicas,
} from "./replication.js";
import { MAX_U32, Observer, Replicator, scaleToU32 } from "./role.js";
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
export { EntryReplicated, ReplicationRangeIndexable };
export const logger = loggerFn({ module: "shared-log" });

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

export type ReplicationLimitsOptions =
	| Partial<ReplicationLimits>
	| { min?: number; max?: number };

export type DynamicReplicationOptions = {
	limits?: {
		interval?: number;
		storage?: number;
		cpu?: number | { max: number; monitor?: CPUUsage };
	};
};

export type FixedReplicationOptions = {
	id?: Uint8Array;
	normalized?: boolean;
	factor: number | "all" | "right";
	strict?: boolean; // if true, only this range will be replicated
	offset?: number;
};

export type ReplicationOptions =
	| DynamicReplicationOptions
	| FixedReplicationOptions
	| FixedReplicationOptions[]
	| number
	| boolean;

const isAdaptiveReplicatorOption = (
	options: ReplicationOptions,
): options is DynamicReplicationOptions => {
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

const isUnreplicationOptions = (options?: ReplicationOptions): boolean =>
	options === false ||
	options === 0 ||
	((options as FixedReplicationOptions)?.offset === undefined &&
		(options as FixedReplicationOptions)?.factor === 0);

const isReplicationOptionsDependentOnPreviousState = (
	options?: ReplicationOptions,
): boolean => {
	if (options === true) {
		return true;
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

export type SharedLogOptions<T, D extends ReplicationDomain<any, T>> = {
	replicate?: ReplicationOptions;
	replicas?: ReplicationLimitsOptions;
	respondToIHaveTimeout?: number;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
	sync?: (entry: ShallowOrFullEntry<T> | EntryReplicated) => boolean;
	timeUntilRoleMaturity?: number;
	waitForReplicatorTimeout?: number;
	distributionDebounceTime?: number;
	compatibility?: number;
	domain?: D;
};

export const DEFAULT_MIN_REPLICAS = 2;
export const WAIT_FOR_REPLICATOR_TIMEOUT = 9000;
export const WAIT_FOR_ROLE_MATURITY = 5000;
const PRUNE_DEBOUNCE_INTERVAL = 500;

// DONT SET THIS ANY LOWER, because it will make the pid controller unstable as the system responses are not fast enough to updates from the pid controller
const RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL = 1000;

const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;

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
	D extends ReplicationDomain<any, T> = ReplicationDomainHash,
> = LogProperties<T> & LogEvents<T> & SharedLogOptions<T, D>;

export type SharedAppendOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
	replicate?: boolean;
	target?: "all" | "replicators" | "none";
};

type ReplicatorJoinEvent = { publicKey: PublicSignKey };
type ReplicatorLeaveEvent = { publicKey: PublicSignKey };
type ReplicationChangeEvent = { publicKey: PublicSignKey };
type ReplicatorMatureEvent = { publicKey: PublicSignKey };

export interface SharedLogEvents extends ProgramEvents {
	"replicator:join": CustomEvent<ReplicatorJoinEvent>;
	"replicator:leave": CustomEvent<ReplicatorLeaveEvent>;
	"replication:change": CustomEvent<ReplicationChangeEvent>;
	"replicator:mature": CustomEvent<ReplicatorMatureEvent>;
}

@variant("shared_log")
export class SharedLog<
	T = Uint8Array,
	D extends ReplicationDomain<any, T> = ReplicationDomainHash,
> extends Program<Args<T, D>, SharedLogEvents> {
	@field({ type: Log })
	log: Log<T>;

	@field({ type: RPC })
	rpc: RPC<TransportMessage, TransportMessage>;

	// options
	private _isReplicating!: boolean;
	private _isAdaptiveReplicating!: boolean;

	private _replicationRangeIndex!: Index<ReplicationRangeIndexable>;
	private _entryCoordinatesIndex!: Index<EntryReplicated>;

	/* private _totalParticipation!: number; */
	private _gidPeersHistory!: Map<string, Set<string>>;

	private _onSubscriptionFn!: (arg: any) => any;
	private _onUnsubscriptionFn!: (arg: any) => any;

	private _isTrustedReplicator?: (
		publicKey: PublicSignKey,
	) => Promise<boolean> | boolean;

	private _logProperties?: LogProperties<T> &
		LogEvents<T> &
		SharedLogOptions<T, D>;
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

	private pendingMaturity!: Map<
		string,
		{
			timestamp: bigint;
			ranges: Map<string, ReplicationChange>;
			timeout: ReturnType<typeof setTimeout>;
		}
	>; // map of peerId to timeout

	private latestReplicationInfoMessage!: Map<string, bigint>;

	private remoteBlocks!: RemoteBlocks;

	private openTime!: number;
	private oldestOpenTime!: number;

	private sync?: (entry: ShallowOrFullEntry<T> | EntryReplicated) => boolean;

	// A fn that we can call many times that recalculates the participation role
	private rebalanceParticipationDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;

	// A fn for debouncing the calls for pruning
	pruneDebouncedFn!: DebouncedAccumulatorMap<
		Entry<T> | ShallowEntry | EntryReplicated
	>;
	private responseToPruneDebouncedFn!: ReturnType<
		typeof debounceAcculmulator<
			string,
			{
				hashes: string[];
				peers: string[] | Set<string>;
			},
			Map<string, Set<string>>
		>
	>;
	private replicationChangeDebounceFn!: ReturnType<
		typeof debounceAggregationChanges
	>;

	// regular distribution checks
	private distributeQueue?: PQueue;

	// Syncing and dedeplucation work
	private syncMoreInterval?: ReturnType<typeof setTimeout>;

	// map of hash to public keys that we can ask for entries
	private syncInFlightQueue!: Map<string, PublicSignKey[]>;
	private syncInFlightQueueInverted!: Map<string, Set<string>>;

	// map of hash to public keys that we have asked for entries
	syncInFlight!: Map<string, Map<string, { timestamp: number }>>;

	replicas!: ReplicationLimits;

	private cpuUsage?: CPUUsage;

	timeUntilRoleMaturity!: number;
	waitForReplicatorTimeout!: number;
	distributionDebounceTime!: number;

	replicationController!: PIDReplicationController;
	history!: { usedMemory: number; factor: number }[];
	domain!: D;
	interval: any;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.log = new Log(properties);
		this.rpc = new RPC();
	}

	get compatibility(): number | undefined {
		return this._logProperties?.compatibility;
	}

	private get v8Behaviour() {
		return (this.compatibility ?? Number.MAX_VALUE) < 9;
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
				factor: segment.factor / MAX_U32,
				offset: segment.offset / MAX_U32,
			});
		}

		// TODO this is not accurate but might be good enough
		return new Observer();
	}

	async isReplicating() {
		if (!this._isReplicating) {
			return false;
		}

		/* 
		if (isAdaptiveReplicatorOption(this._replicationSettings)) {
			return true;
		}

		if ((this.replicationSettings as FixedReplicationOptions).factor !== 0) {
			return true;
		} */

		return (await this.countReplicationSegments()) > 0;
	}

	/* get totalParticipation(): number {
		return this._totalParticipation;
	} */

	async calculateTotalParticipation() {
		const sum = await this.replicationIndex.sum({ key: "width" });
		return Number(sum) / MAX_U32;
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

	private setupRebalanceDebounceFunction(
		interval = RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL,
	) {
		this.rebalanceParticipationDebounced = undefined;

		// make the rebalancing to respect warmup time
		let intervalTime = interval * 2;
		let timeout = setTimeout(() => {
			intervalTime = interval;
		}, this.timeUntilRoleMaturity);
		this._closeController.signal.addEventListener("abort", () => {
			clearTimeout(timeout);
		});

		this.rebalanceParticipationDebounced = debounceFixedInterval(
			() => this.rebalanceParticipation(),
			/* Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.getSize() || 0) *
					REBALANCE_DEBOUNCE_INTERVAL
				)
			) */
			() => intervalTime, // TODO make this dynamic on the number of replicators
		);
	}

	private async _replicate(
		options?: ReplicationOptions,
		{
			reset,
			checkDuplicates,
			announce,
		}: {
			reset?: boolean;
			checkDuplicates?: boolean;
			announce?: (
				msg: AddedReplicationSegmentMessage | AllReplicatingSegmentsMessage,
			) => void;
		} = {},
	) {
		let offsetWasProvided = false;
		if (isUnreplicationOptions(options)) {
			await this.unreplicate();
		} else {
			let ranges: ReplicationRangeIndexable[] = [];

			if (options == null) {
				options = {};
			} else if (options === true) {
				options = {};
			}

			this._isReplicating = true;
			this._isAdaptiveReplicating = false;

			if (isAdaptiveReplicatorOption(options!)) {
				this._isAdaptiveReplicating = true;
				this.setupDebouncedRebalancing(options);

				// initial role in a dynamic setup
				const maybeRange = await this.getDynamicRange();
				if (!maybeRange) {
					// not allowed
					return;
				}
				ranges = [maybeRange];

				offsetWasProvided = true;
			} else if (options instanceof ReplicationRange) {
				ranges = [
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
					rangeArgs = (
						Array.isArray(options) ? options : [{ ...options }]
					) as FixedReplicationOptions[];
				}

				if (rangeArgs.length === 0) {
					// nothing to do
					return;
				}

				for (const rangeArg of rangeArgs) {
					const normalized = rangeArg.normalized ?? true;
					offsetWasProvided = rangeArg.offset != null;
					const offset =
						rangeArg.offset ??
						(normalized ? Math.random() : scaleToU32(Math.random()));
					let factor = rangeArg.factor;
					let width = normalized ? 1 : scaleToU32(1);
					ranges.push(
						new ReplicationRangeIndexable({
							id: rangeArg.id,
							normalized,
							offset: offset,
							length:
								typeof factor === "number"
									? factor
									: factor === "all"
										? width
										: width - offset,
							publicKeyHash: this.node.identity.publicKey.hashcode(),
							mode: rangeArg.strict
								? ReplicationIntent.Strict
								: ReplicationIntent.NonStrict, // automatic means that this range might be reused later for dynamic replication behaviour
							timestamp: BigInt(+new Date()),
						}),
					);
				}
			}

			for (const range of ranges) {
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
			await this.startAnnounceReplicating(ranges, {
				reset: resetRanges ?? false,
				checkDuplicates,
				announce,
			});

			return ranges;
		}
	}

	setupDebouncedRebalancing(options?: DynamicReplicationOptions) {
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
		rangeOrEntry?: ReplicationOptions | Entry<T> | Entry<T>[],
		options?: {
			reset?: boolean;
			checkDuplicates?: boolean;
			announce?: (
				msg: AllReplicatingSegmentsMessage | AddedReplicationSegmentMessage,
			) => void;
		},
	) {
		let range: ReplicationRange[] | ReplicationOptions | undefined = undefined;

		if (rangeOrEntry instanceof ReplicationRange) {
			range = rangeOrEntry;
		} else if (rangeOrEntry instanceof Entry) {
			range = {
				factor: 1,
				offset: await this.domain.fromEntry(rangeOrEntry),
				normalized: false,
			};
		} else if (Array.isArray(rangeOrEntry)) {
			let ranges: (ReplicationRange | FixedReplicationOptions)[] = [];
			for (const entry of rangeOrEntry) {
				if (entry instanceof Entry) {
					ranges.push({
						factor: 1,
						offset: await this.domain.fromEntry(entry),
						normalized: false,
					});
				} else {
					ranges.push(entry);
				}
			}
			range = ranges;
		} else {
			range = rangeOrEntry ?? true;
		}

		return this._replicate(range, options);
	}

	async unreplicate(rangeOrEntry?: Entry<T> | ReplicationRange) {
		let range: FixedReplicationOptions;
		if (rangeOrEntry instanceof Entry) {
			range = {
				factor: 1,
				offset: await this.domain.fromEntry(rangeOrEntry),
			};
		} else if (rangeOrEntry instanceof ReplicationRange) {
			range = rangeOrEntry;
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

		const indexed = this.replicationIndex.iterate({
			query: {
				width: 1,
				start1: range.offset,
			},
		});

		const segmentIds = (await indexed.all()).map((x) => x.id.key as Uint8Array);
		await this.removeReplicationRange(segmentIds, this.node.identity.publicKey);
		await this.rpc.send(new StoppedReplicating({ segmentIds }), {
			priority: 1,
		});
	}

	private async removeReplicator(
		key: PublicSignKey | string,
		options?: { noEvent?: boolean },
	) {
		const fn = async () => {
			const keyHash = typeof key === "string" ? key : key.hashcode();
			const deleted = await this.replicationIndex
				.iterate({
					query: { hash: keyHash },
				})
				.all();

			await this.replicationIndex.del({ query: { hash: keyHash } });

			await this.updateOldestTimestampFromIndex();

			const isMe = this.node.identity.publicKey.hashcode() === keyHash;
			if (isMe) {
				// announce that we are no longer replicating

				await this.rpc.send(
					new AllReplicatingSegmentsMessage({ segments: [] }),
					{ priority: 1 },
				);
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

			deleted.forEach((x) => {
				return this.replicationChangeDebounceFn.add({
					range: x.value,
					type: "removed",
				});
			});

			const pendingMaturity = this.pendingMaturity.get(keyHash);
			if (pendingMaturity) {
				clearTimeout(pendingMaturity.timeout);
				this.pendingMaturity.delete(keyHash);
			}

			if (!isMe) {
				this.rebalanceParticipationDebounced?.();
			}
		};

		return fn();
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

	private async removeReplicationRange(ids: Uint8Array[], from: PublicSignKey) {
		const fn = async () => {
			let idMatcher = new Or(
				ids.map((x) => new ByteMatchQuery({ key: "id", value: x })),
			);

			// make sure we are not removing something that is owned by the replicator
			let identityMatcher = new StringMatch({
				key: "hash",
				value: from.hashcode(),
			});

			let query = new And([idMatcher, identityMatcher]);

			const pendingMaturity = this.pendingMaturity.get(from.hashcode());
			if (pendingMaturity) {
				for (const id of ids) {
					pendingMaturity.ranges.delete(id.toString());
				}
				if (pendingMaturity.ranges.size === 0) {
					clearTimeout(pendingMaturity.timeout);
					this.pendingMaturity.delete(from.hashcode());
				}
			}

			await this.replicationIndex.del({ query });

			await this.updateOldestTimestampFromIndex();

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChangeEvent>("replication:change", {
					detail: { publicKey: from },
				}),
			);

			if (!from.equals(this.node.identity.publicKey)) {
				this.rebalanceParticipationDebounced?.();
			}
		};

		return fn();
	}

	private async addReplicationRange(
		ranges: ReplicationRangeIndexable[],
		from: PublicSignKey,
		{
			reset,
			checkDuplicates,
		}: { reset?: boolean; checkDuplicates?: boolean } = {},
	) {
		const fn = async () => {
			if (
				this._isTrustedReplicator &&
				!(await this._isTrustedReplicator(from))
			) {
				return undefined;
			}

			let isNewReplicator = false;

			let diffs: ReplicationChanges;
			let deleted: ReplicationRangeIndexable[] | undefined = undefined;
			if (reset) {
				deleted = (
					await this.replicationIndex
						.iterate({
							query: { hash: from.hashcode() },
						})
						.all()
				).map((x) => x.value);

				let prevCount = deleted.length;

				await this.replicationIndex.del({ query: { hash: from.hashcode() } });

				diffs = [
					...deleted.map((x) => {
						return { range: x, type: "removed" as const };
					}),
					...ranges.map((x) => {
						return { range: x, type: "added" as const };
					}),
				];

				isNewReplicator = prevCount === 0 && ranges.length > 0;
			} else {
				let existing = await this.replicationIndex
					.iterate(
						{
							query: ranges.map(
								(x) => new ByteMatchQuery({ key: "id", value: x.id }),
							),
						},
						{ reference: true },
					)
					.all();
				if (existing.length === 0) {
					let prevCount = await this.replicationIndex.count({
						query: new StringMatch({ key: "hash", value: from.hashcode() }),
					});
					isNewReplicator = prevCount === 0;
				} else {
					isNewReplicator = false;
				}

				if (checkDuplicates) {
					let deduplicated: ReplicationRangeIndexable[] = [];

					// TODO also deduplicate/de-overlap among the ranges that ought to be inserted?
					for (const range of ranges) {
						if (!(await hasCoveringRange(this.replicationIndex, range))) {
							deduplicated.push(range);
						}
					}
					ranges = deduplicated;
				}
				let existingMap = new Map<string, ReplicationRangeIndexable>();
				for (const result of existing) {
					existingMap.set(result.value.idString, result.value);
				}

				let changes: ReplicationChanges = ranges
					.map((x) => {
						const prev = existingMap.get(x.idString);
						if (prev) {
							if (prev.equalRange(x)) {
								return undefined;
							}
							return { range: x, prev, type: "updated" };
						} else {
							return { range: x, type: "added" };
						}
					})
					.filter((x) => x != null) as ReplicationChanges;
				diffs = changes;
			}

			let now = +new Date();
			let minRoleAge = await this.getDefaultMinRoleAge();
			let isAllMature = true;

			for (const diff of diffs) {
				if (diff.type === "added" || diff.type === "updated") {
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
						let prevPendingMaturity = this.pendingMaturity.get(diff.range.hash);
						let map: Map<string, ReplicationChange>;
						let waitForMaturityTime = Math.max(
							minRoleAge - (now - Number(diff.range.timestamp)),
							0,
						);

						if (prevPendingMaturity) {
							map = prevPendingMaturity.ranges;
							if (prevPendingMaturity.timestamp < diff.range.timestamp) {
								// something has changed so we need to reset the timeout
								clearTimeout(prevPendingMaturity.timeout);
								prevPendingMaturity.timestamp = diff.range.timestamp;
								prevPendingMaturity.timeout = setTimeout(() => {
									this.events.dispatchEvent(
										new CustomEvent<ReplicationChangeEvent>(
											"replicator:mature",
											{
												detail: { publicKey: from },
											},
										),
									);
									for (const value of map.values()) {
										this.replicationChangeDebounceFn.add(value); // we need to call this here because the outcom of findLeaders will be different when some ranges become mature, i.e. some of data we own might be prunable!
									}
								}, waitForMaturityTime);
							}
						} else {
							map = new Map();
							this.pendingMaturity.set(diff.range.hash, {
								timestamp: diff.range.timestamp,
								ranges: map,
								timeout: setTimeout(() => {
									this.events.dispatchEvent(
										new CustomEvent<ReplicationChangeEvent>(
											"replicator:mature",
											{
												detail: { publicKey: from },
											},
										),
									);
									for (const value of map.values()) {
										this.replicationChangeDebounceFn.add(value); // we need to call this here because the outcom of findLeaders will be different when some ranges become mature, i.e. some of data we own might be prunable!
									}
								}, waitForMaturityTime),
							});
						}

						map.set(diff.range.idString, diff);
					}
				} else {
					const prev = this.pendingMaturity.get(diff.range.hash);
					if (prev) {
						prev.ranges.delete(diff.range.idString);
					}
				}
			}

			if (reset) {
				await this.updateOldestTimestampFromIndex();
			}

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChangeEvent>("replication:change", {
					detail: { publicKey: from },
				}),
			);

			if (isNewReplicator) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorJoinEvent>("replicator:join", {
						detail: { publicKey: from },
					}),
				);

				if (isAllMature) {
					this.events.dispatchEvent(
						new CustomEvent<ReplicatorMatureEvent>("replicator:mature", {
							detail: { publicKey: from },
						}),
					);
				}
			}

			diffs.length > 0 &&
				diffs.map((x) => this.replicationChangeDebounceFn.add(x));

			if (!from.equals(this.node.identity.publicKey)) {
				this.rebalanceParticipationDebounced?.();
			}

			return diffs;
		};

		// we sequialize this because we are going to queries to check wether to add or not
		// if two processes do the same this both process might add a range while only one in practice should
		return fn();
	}

	async startAnnounceReplicating(
		range: ReplicationRangeIndexable[],
		options: {
			reset?: boolean;
			checkDuplicates?: boolean;
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
			logger.warn("Not allowed to replicate by canReplicate");
		}

		let message: AllReplicatingSegmentsMessage | AddedReplicationSegmentMessage;

		if (change) {
			if (options.reset) {
				message = new AllReplicatingSegmentsMessage({
					segments: range.map((x) => x.toReplicationRange()),
				});
			} else {
				message = new AddedReplicationSegmentMessage({
					segments: range.map((x) => x.toReplicationRange()),
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

	async append(
		data: T,
		options?: SharedAppendOptions<T> | undefined,
	): Promise<{
		entry: Entry<T>;
		removed: ShallowOrFullEntry<T>[];
	}> {
		const appendOptions: AppendOptions<T> = { ...options };
		const minReplicas = options?.replicas
			? typeof options.replicas === "number"
				? new AbsoluteReplicas(options.replicas)
				: options.replicas
			: this.replicas.min;
		const minReplicasValue = minReplicas.getValue(this);
		const minReplicasData = encodeReplicas(minReplicas);

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
		let mode: DeliveryMode | undefined = undefined;

		if (options?.replicate) {
			await this.replicate(result.entry, { checkDuplicates: true });
		}

		let { leaders, isLeader } = await this.findLeadersPersist(
			{
				entry: result.entry,
				minReplicas: minReplicas.getValue(this),
			},
			result.entry,
			{ persist: {} },
		);

		// --------------

		if (options?.target !== "none") {
			for await (const message of createExchangeHeadsMessages(this.log, [
				result.entry,
			])) {
				if (options?.target === "replicators" || !options?.target) {
					if (message.heads[0].gidRefrences.length > 0) {
						const newAndOldLeaders = new Map(leaders);
						for (const ref of message.heads[0].gidRefrences) {
							const entryFromGid = this.log.entryIndex.getHeads(ref, false);
							for (const entry of await entryFromGid.all()) {
								let coordinate = await this.getCoordinates(entry);
								if (coordinate == null) {
									coordinate = await this.createCoordinates(
										entry,
										minReplicasValue,
									);
									// TODO are we every to come here?
								}
								for (const [hash, features] of await this.findLeaders(
									coordinate,
								)) {
									newAndOldLeaders.set(hash, features);
								}
							}
						}
						leaders = newAndOldLeaders;
					}

					let set = this._gidPeersHistory.get(result.entry.meta.gid);
					if (!set) {
						set = new Set(leaders.keys());
						this._gidPeersHistory.set(result.entry.meta.gid, set);
					} else {
						for (const [receiver, _features] of leaders) {
							set.add(receiver);
						}
					}

					mode = isLeader
						? new SilentDelivery({ redundancy: 1, to: leaders.keys() })
						: new AcknowledgeDelivery({ redundancy: 1, to: leaders.keys() });
				}

				// TODO add options for waiting ?
				this.rpc.send(message, {
					mode,
				});
			}
		}

		if (!isLeader) {
			this.pruneDebouncedFn.add({
				key: result.entry.hash,
				value: result.entry,
			});
		}
		this.rebalanceParticipationDebounced?.();

		return result;
	}

	async open(options?: Args<T, D>): Promise<void> {
		this.replicas = {
			min: options?.replicas?.min
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
		this.domain = options?.domain ?? (createReplicationDomainHash() as D);
		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 2e4;
		this._pendingDeletes = new Map();
		this._pendingIHave = new Map();
		this.latestReplicationInfoMessage = new Map();
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlight = new Map();
		this.openTime = +new Date();
		this.oldestOpenTime = this.openTime;
		this.distributionDebounceTime =
			options?.distributionDebounceTime || DEFAULT_DISTRIBUTION_DEBOUNCE_TIME; // expect > 0
		this.timeUntilRoleMaturity =
			options?.timeUntilRoleMaturity ?? WAIT_FOR_ROLE_MATURITY;
		this.waitForReplicatorTimeout =
			options?.waitForReplicatorTimeout || WAIT_FOR_REPLICATOR_TIMEOUT;
		this._closeController = new AbortController();
		this._isTrustedReplicator = options?.canReplicate;
		this.sync = options?.sync;
		this._logProperties = options;
		this.pendingMaturity = new Map();

		const id = sha256Base64Sync(this.log.id);
		const storage = await this.node.storage.sublevel(id);

		const localBlocks = await new AnyBlockStore(
			await storage.sublevel("blocks"),
		);
		this.remoteBlocks = new RemoteBlocks({
			local: localBlocks,
			publish: (message, options) =>
				this.rpc.send(new BlocksMessage(message), {
					mode: options?.to
						? new SilentDelivery({ to: options.to, redundancy: 1 })
						: undefined,
				}),
			waitFor: this.rpc.waitFor.bind(this.rpc),
		});

		await this.remoteBlocks.start();

		/* this._totalParticipation = 0; */
		const logScope = await this.node.indexer.scope(id);
		const replicationIndex = await logScope.scope("replication");
		this._replicationRangeIndex = await replicationIndex.init({
			schema: ReplicationRangeIndexable,
		});

		this._entryCoordinatesIndex = await replicationIndex.init({
			schema: EntryReplicated,
		});

		const logIndex = await logScope.scope("log");

		await this.node.indexer.start(); // TODO why do we need to start the indexer here?

		const hasIndexedReplicationInfo =
			(await this.replicationIndex.count({
				query: [
					new StringMatch({
						key: "hash",
						value: this.node.identity.publicKey.hashcode(),
					}),
				],
			})) > 0;

		/* this._totalParticipation = await this.calculateTotalParticipation(); */

		this._gidPeersHistory = new Map();

		this.replicationChangeDebounceFn = debounceAggregationChanges(
			(change) =>
				this.onReplicationChange(change).then(() =>
					this.rebalanceParticipationDebounced?.(),
				),
			this.distributionDebounceTime,
		);

		this.pruneDebouncedFn = debouncedAccumulatorMap(
			(map) => {
				this.prune(map);
			},
			PRUNE_DEBOUNCE_INTERVAL, // TODO make this dynamic on the number of replicators
		);

		this.responseToPruneDebouncedFn = debounceAcculmulator<
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
				};
			},
			PRUNE_DEBOUNCE_INTERVAL, // TODO make this dynamic on the number of replicators
		);

		await this.log.open(this.remoteBlocks, this.node.identity, {
			keychain: this.node.services.keychain,
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

		// Open for communcation
		await this.rpc.open({
			queryType: TransportMessage,
			responseType: TransportMessage,
			responseHandler: (query, context) => this._onMessage(query, context),
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

		const requestSync = async () => {
			/**
			 * This method fetches entries that we potentially want.
			 * In a case in which we become replicator of a segment,
			 * multiple remote peers might want to send us entries
			 * This method makes sure that we only request on entry from the remotes at a time
			 * so we don't get flooded with the same entry
			 */
			const requestHashes: string[] = [];
			const from: Set<string> = new Set();
			for (const [key, value] of this.syncInFlightQueue) {
				if (!(await this.log.has(key))) {
					// TODO test that this if statement actually does anymeaningfull
					if (value.length > 0) {
						requestHashes.push(key);
						const publicKeyHash = value.shift()!.hashcode();
						from.add(publicKeyHash);
						const invertedSet =
							this.syncInFlightQueueInverted.get(publicKeyHash);
						if (invertedSet) {
							if (invertedSet.delete(key)) {
								if (invertedSet.size === 0) {
									this.syncInFlightQueueInverted.delete(publicKeyHash);
								}
							}
						}
					}
					if (value.length === 0) {
						this.syncInFlightQueue.delete(key); // no-one more to ask for this entry
					}
				} else {
					this.syncInFlightQueue.delete(key);
				}
			}

			const nowMin10s = +new Date() - 1e4;
			for (const [key, map] of this.syncInFlight) {
				// cleanup "old" missing syncs
				for (const [hash, { timestamp }] of map) {
					if (timestamp < nowMin10s) {
						map.delete(hash);
					}
				}
				if (map.size === 0) {
					this.syncInFlight.delete(key);
				}
			}
			this.requestSync(requestHashes, from).finally(() => {
				if (this.closed) {
					return;
				}
				this.syncMoreInterval = setTimeout(requestSync, 3e3);
			});
		};

		// if we had a previous session with replication info, and new replication info dictates that we unreplicate
		// we should do that. Otherwise if options is a unreplication we dont need to do anything because
		// we are already unreplicated (as we are just opening)

		let isUnreplicationOptionsDefined = isUnreplicationOptions(
			options?.replicate,
		);
		if (hasIndexedReplicationInfo && isUnreplicationOptionsDefined) {
			await this.replicate(options?.replicate, { checkDuplicates: true });
		} else if (
			isReplicationOptionsDependentOnPreviousState(options?.replicate) &&
			hasIndexedReplicationInfo
		) {
			// dont do anthing since we are alread replicating stuff
		} else {
			await this.replicate(options?.replicate, {
				checkDuplicates: true,
				reset: true,
			});
		}

		requestSync();

		this.interval = setInterval(() => {
			this.rebalanceParticipationDebounced?.();
		}, RECALCULATE_PARTICIPATION_DEBOUNCE_INTERVAL);
	}

	async afterOpen(): Promise<void> {
		await super.afterOpen();

		// We do this here, because these calls requires this.closed == false
		await this.pruneOfflineReplicators();

		await this.rebalanceParticipation();

		// Take into account existing subscription
		(await this.node.services.pubsub.getSubscribers(this.topic))?.forEach(
			(v, k) => {
				if (v.equals(this.node.identity.publicKey)) {
					return;
				}
				this.handleSubscriptionChange(v, [this.topic], true);
			},
		);
	}

	async reset() {
		await this.log.load({ reset: true });
	}

	async pruneOfflineReplicators() {
		// go through all segments and for waitForAll replicators to become reachable if not prune them away

		const promises: Promise<any>[] = [];
		const iterator = this.replicationIndex.iterate();
		let checked = new Set<string>();
		while (!iterator.done()) {
			for (const segment of await iterator.next(1000)) {
				if (
					checked.has(segment.value.hash) ||
					this.node.identity.publicKey.hashcode() === segment.value.hash
				) {
					continue;
				}

				checked.add(segment.value.hash);

				promises.push(
					this.waitFor(segment.value.hash, {
						timeout: this.waitForReplicatorTimeout,
						signal: this._closeController.signal,
					})
						.then(async () => {
							// is reachable, announce change events
							const key = await this.node.services.pubsub.getPublicKey(
								segment.value.hash,
							);
							if (!key) {
								throw new Error(
									"Failed to resolve public key from hash: " +
										segment.value.hash,
								);
							}
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
						})
						.catch((e) => {
							// not reachable
							return this.removeReplicator(segment.value.hash, {
								noEvent: true,
							}); // done announce since replicator was never reachable
						}),
				);
			}
		}
		return Promise.all(promises);
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
				logger.warn("Received entry without meta data, skipping");
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
				logger.warn("Received payload that could not be decoded, skipping");
				return false;
			}
			throw error;
		}
	}

	async getCover(
		args: ExtractDomainArgs<D>,
		options?: {
			roleAge?: number;
			eager?:
				| {
						unmaturedFetchCoverSize?: number;
				  }
				| boolean;
		},
	) {
		let roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge());
		let eager = options?.eager ?? false;
		const range = await this.domain.fromArgs(args, this);

		const set = await getCoverSet({
			peers: this.replicationIndex,
			start: range.offset,
			widthToCoverScaled:
				range.length ??
				(await minimumWidthToCover(this.replicas.min.getValue(this))),
			roleAge,
			eager,
			intervalWidth: MAX_U32,
		});

		// add all in flight
		for (const [key, _] of this.syncInFlight) {
			set.add(key);
		}

		return [...set];
	}

	private async _close() {
		clearTimeout(this.syncMoreInterval);

		for (const [_key, value] of this.pendingMaturity) {
			clearTimeout(value.timeout);
		}
		this.pendingMaturity.clear();

		this.distributeQueue?.clear();

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

		await this.remoteBlocks.stop();
		this._pendingDeletes.clear();
		this._pendingIHave.clear();
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		this.latestReplicationInfoMessage.clear();
		this._gidPeersHistory.clear();
		this.pruneDebouncedFn = undefined as any;
		this.rebalanceParticipationDebounced = undefined;
		this._replicationRangeIndex = undefined as any;
		this._entryCoordinatesIndex = undefined as any;

		this.cpuUsage?.stop?.();
		/* this._totalParticipation = 0; */
	}
	async close(from?: Program): Promise<boolean> {
		const superClosed = await super.close(from);
		if (!superClosed) {
			return superClosed;
		}
		await this._close();
		await this.log.close();
		return true;
	}

	async drop(from?: Program): Promise<boolean> {
		const superDropped = await super.drop(from);
		if (!superDropped) {
			return superDropped;
		}
		await this.log.drop();
		await this._close();
		return true;
	}

	async recover(): Promise<void> {
		return this.log.recover();
	}

	// Callback for receiving a message from the network
	async _onMessage(
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

				logger.debug(
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

							const isReplicating = await this.isReplicating();

							let isLeader:
								| Map<
										string,
										{
											intersecting: boolean;
										}
								  >
								| false;

							if (isReplicating) {
								isLeader = await this.waitForIsLeader(
									cursor,
									this.node.identity.publicKey.hashcode(),
								);
							} else {
								isLeader = await this.findLeaders(cursor);

								isLeader = isLeader.has(this.node.identity.publicKey.hashcode())
									? isLeader
									: false;
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
								}

								for (const entry of entries) {
									await this.persistCoordinate({
										leaders: isLeader,
										coordinates: cursor,
										entry: entry.entry,
									});
								}

								const fromIsLeader = isLeader.get(context.from!.hashcode());
								if (fromIsLeader) {
									let peerSet = this._gidPeersHistory.get(gid);
									if (!peerSet) {
										peerSet = new Set();
										this._gidPeersHistory.set(gid, peerSet);
									}
									peerSet.add(context.from!.hashcode());
								}

								if (maxReplicasFromNewEntries < maxReplicasFromHead) {
									(maybeDelete || (maybeDelete = [])).push(entries);
								}
							}

							outer: for (const entry of entries) {
								if (isLeader || this.sync?.(entry.entry)) {
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

								logger.debug(
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
									this.pruneDebouncedFn.add({ key: x.hash, value: x }),
								);
								this.rebalanceParticipationDebounced?.();
							}

							/// we clear sync in flight here because we want to join before that, so that entries are totally accounted for
							for (const entry of entries) {
								const set = this.syncInFlight.get(context.from!.hashcode());
								if (set) {
									set.delete(entry.entry.hash);
									if (set?.size === 0) {
										this.syncInFlight.delete(context.from!.hashcode());
									}
								}
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
											entries.map((x) =>
												this.pruneDebouncedFn.add({
													key: x.entry.hash,
													value: x.entry,
												}),
											);
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
				//	await delay(3000)
				for (const hash of msg.hashes) {
					const indexedEntry = await this.log.entryIndex.getShallow(hash);
					if (
						indexedEntry &&
						(
							await this.findLeadersPersist(
								{
									entry: indexedEntry.value,
									minReplicas: decodeReplicas(indexedEntry.value).getValue(
										this,
									),
								},
								indexedEntry.value,
							)
						).isLeader
					) {
						this._gidPeersHistory
							.get(indexedEntry.value.meta.gid)
							?.delete(context.from.hashcode());
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
									if (
										(
											await this.findLeadersPersist(
												{
													entry,
													minReplicas: decodeReplicas(entry).getValue(this),
												},
												entry,
											)
										).isLeader
									) {
										for (const peer of requesting) {
											this._gidPeersHistory.get(entry.meta.gid)?.delete(peer);
										}

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
			} else if (msg instanceof RequestMaybeSync) {
				const requestHashes: string[] = [];

				for (const hash of msg.hashes) {
					const inFlight = this.syncInFlightQueue.get(hash);
					if (inFlight) {
						if (
							!inFlight.find((x) => x.hashcode() === context.from!.hashcode())
						) {
							inFlight.push(context.from);
							let inverted = this.syncInFlightQueueInverted.get(
								context.from.hashcode(),
							);
							if (!inverted) {
								inverted = new Set();
								this.syncInFlightQueueInverted.set(
									context.from.hashcode(),
									inverted,
								);
							}
							inverted.add(hash);
						}
					} else if (!(await this.log.has(hash))) {
						this.syncInFlightQueue.set(hash, []);
						requestHashes.push(hash); // request immediately (first time we have seen this hash)
					}
				}
				requestHashes.length > 0 &&
					(await this.requestSync(requestHashes, [context.from.hashcode()]));
			} else if (msg instanceof ResponseMaybeSync) {
				// TODO perhaps send less messages to more receivers for performance reasons?
				// TODO wait for previous send to target before trying to send more?
				for await (const message of createExchangeHeadsMessages(
					this.log,
					msg.hashes,
				)) {
					await this.rpc.send(message, {
						mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					});
				}
			} else if (msg instanceof BlocksMessage) {
				await this.remoteBlocks.onMessage(msg.message);
			} else if (msg instanceof RequestReplicationInfoMessage) {
				// TODO this message type is never used, should we remove it?

				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}
				await this.rpc.send(
					new AllReplicatingSegmentsMessage({
						segments: (await this.getMyReplicationSegments()).map((x) =>
							x.toReplicationRange(),
						),
					}),
					{
						mode: new SilentDelivery({ to: [context.from], redundancy: 1 }),
					},
				);

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

				let replicationInfoMessage = msg as
					| AllReplicatingSegmentsMessage
					| AddedReplicationSegmentMessage;

				// we have this statement because peers might have changed/announced their role,
				// but we don't know them as "subscribers" yet. i.e. they are not online

				this.waitFor(context.from, {
					signal: this._closeController.signal,
					timeout: this.waitForReplicatorTimeout,
				})
					.then(async () => {
						// do use an operation log here, because we want to make sure that we don't miss any updates
						// and do them in the right order
						const prev = this.latestReplicationInfoMessage.get(
							context.from!.hashcode(),
						);

						if (prev && prev > context.timestamp) {
							return;
						}

						this.latestReplicationInfoMessage.set(
							context.from!.hashcode(),
							context.timestamp,
						);

						let reset = msg instanceof AllReplicatingSegmentsMessage;

						await this.addReplicationRange(
							replicationInfoMessage.segments.map((x) =>
								x.toReplicationRangeIndexable(context.from!),
							),
							context.from!,
							{ reset, checkDuplicates: true },
						);

						/* await this._modifyReplicators(msg.role, context.from!); */
					})
					.catch((e) => {
						if (e instanceof AbortError) {
							return;
						}
						if (e instanceof NotStartedError) {
							return;
						}
						logger.error(
							"Failed to find peer who updated replication settings: " +
								e?.message,
						);
					});
			} else if (msg instanceof StoppedReplicating) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				await this.removeReplicationRange(msg.segmentIds, context.from);
			} else {
				throw new Error("Unexpected message");
			}
		} catch (e: any) {
			if (e instanceof AbortError) {
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

	async getMyTotalParticipation() {
		// sum all of my replicator rects
		return (await this.getMyReplicationSegments()).reduce(
			(acc, { widthNormalized }) => acc + widthNormalized,
			0,
		);
	}

	get replicationIndex(): Index<ReplicationRangeIndexable> {
		if (!this._replicationRangeIndex) {
			throw new ClosedError();
		}
		return this._replicationRangeIndex;
	}

	get entryCoordinatesIndex(): Index<EntryReplicated> {
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

	async waitForReplicator(...keys: PublicSignKey[]) {
		const check = async () => {
			for (const k of keys) {
				const rects = await this.replicationIndex
					?.iterate(
						{ query: new StringMatch({ key: "hash", value: k.hashcode() }) },
						{ reference: true },
					)
					.all();
				const rect = rects[0]?.value;
				if (
					!rect ||
					!isMatured(rect, +new Date(), await this.getDefaultMinRoleAge())
				) {
					return false;
				}
			}
			return true;
		};
		return waitFor(() => check(), {
			signal: this._closeController.signal,
		}).catch((e) => {
			if (e instanceof AbortError) {
				// ignore error
				return;
			}
			throw e;
		});
	}

	async join(
		entries: (string | Entry<T> | ShallowEntry)[],
		options?: {
			verifySignatures?: boolean;
			timeout?: number;
			replicate?: boolean;
		},
	): Promise<void> {
		let messageToSend: AddedReplicationSegmentMessage | undefined = undefined;

		if (options?.replicate) {
			// TODO this block should perhaps be called from a callback on the this.log.join method on all the ignored element because already joined, like "onAlreadyJoined"

			// check which entrise we already have but not are replicating, and replicate them
			let alreadyJoined: Entry<T>[] = [];
			for (const element of entries) {
				if (typeof element === "string") {
					const entry = await this.log.get(element);
					if (entry) {
						alreadyJoined.push(entry);
					}
				} else if (element instanceof Entry) {
					if (await this.log.has(element.hash)) {
						alreadyJoined.push(element);
					}
				} else {
					const entry = await this.log.get(element.hash);
					if (entry) {
						alreadyJoined.push(entry);
					}
				}
			}

			// assume is heads
			await this.replicate(alreadyJoined, {
				checkDuplicates: true,
				announce: (msg) => {
					messageToSend = msg;
				},
			});
		}

		let joinOptions = options?.replicate
			? {
					...options,
					onChange: async (change: Change<T>) => {
						if (change.added) {
							for (const entry of change.added) {
								if (entry.head) {
									await this.replicate(entry.entry, {
										checkDuplicates: true,

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
								}
							}
						}
					},
				}
			: options;

		await this.log.join(entries, joinOptions);

		if (messageToSend) {
			await this.rpc.send(messageToSend, {
				priority: 1,
			});
		}
	}

	private async findLeadersPersist(
		cursor:
			| number[]
			| {
					entry: ShallowOrFullEntry<any> | EntryReplicated;
					minReplicas: number;
			  },
		entry: ShallowOrFullEntry<any> | EntryReplicated,
		options?: {
			roleAge?: number;
			// persist even if not leader
			persist?: {
				prev?: EntryReplicated[];
			};
		},
	): Promise<{
		leaders: Map<string, { intersecting: boolean }>;
		isLeader: boolean;
	}> {
		const coordinates = Array.isArray(cursor)
			? cursor
			: await this.createCoordinates(cursor.entry, cursor.minReplicas);
		const minReplicas = coordinates.length;
		const leaders = await this.findLeaders(coordinates, options);
		const isLeader = leaders.has(this.node.identity.publicKey.hashcode());

		if (isLeader || options?.persist) {
			let assignToRangeBoundary: boolean | undefined = undefined;
			if (options?.persist?.prev) {
				assignToRangeBoundary = shouldAssigneToRangeBoundary(
					leaders,
					minReplicas,
				);
				const prev = options.persist.prev;
				// dont do anthing if nothing has changed
				if (prev.length > 0) {
					let allTheSame = true;

					for (const element of prev) {
						if (element.assignedToRangeBoundary !== assignToRangeBoundary) {
							allTheSame = false;
							break;
						}
					}

					if (allTheSame) {
						return { leaders, isLeader };
					}
				}
			}

			!this.closed &&
				(await this.persistCoordinate(
					{
						leaders,
						coordinates,
						entry,
					},
					{
						assignToRangeBoundary,
					},
				));
		}

		return { leaders, isLeader };
	}

	async isLeader(
		cursor:
			| number[]
			| {
					entry: ShallowOrFullEntry<any> | EntryReplicated;
					replicas: number;
			  },
		options?: {
			roleAge?: number;
		},
	): Promise<boolean> {
		const leaders = await this.findLeaders(cursor, options);
		return leaders.has(this.node.identity.publicKey.hashcode());
	}

	private async waitForIsLeader(
		cursor: number[],
		hash: string,
		options: {
			timeout: number;
		} = { timeout: this.waitForReplicatorTimeout },
	): Promise<Map<string, { intersecting: boolean }> | false> {
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

			const timer = setTimeout(() => {
				removeListeners();
				resolve(false);
			}, options.timeout);

			const check = () =>
				this.findLeaders(cursor).then((leaders) => {
					const isLeader = leaders.has(hash);
					if (isLeader) {
						removeListeners();
						clearTimeout(timer);
						resolve(leaders);
					}
				});

			const roleListener = () => {
				check();
			};

			this.events.addEventListener("replication:change", roleListener); // TODO replication:change event  ?
			this.events.addEventListener("replicator:mature", roleListener); // TODO replication:change event  ?
			this._closeController.signal.addEventListener("abort", abortListener);
			check();
		});
	}

	async findLeaders(
		cursor:
			| number[]
			| {
					entry: ShallowOrFullEntry<any> | EntryReplicated;
					replicas: number;
			  },
		options?: {
			roleAge?: number;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
		if (this.closed) {
			const map = new Map(); // Assumption: if the store is closed, always assume we have responsibility over the data
			map.set(this.node.identity.publicKey.hashcode(), { intersecting: false });
			return map;
		}

		const coordinates = Array.isArray(cursor)
			? cursor
			: await this.createCoordinates(cursor.entry, cursor.replicas);
		const leaders = await this.findLeadersFromU32(coordinates, options);

		return leaders;
	}

	private async groupByLeaders(
		cursors: (
			| number[]
			| {
					entry: ShallowOrFullEntry<any> | EntryReplicated;
					replicas: number;
			  }
		)[],
		options?: {
			roleAge?: number;
		},
	) {
		const leaders = await Promise.all(
			cursors.map((x) => this.findLeaders(x, options)),
		);
		const map = new Map<string, number[]>();
		leaders.forEach((leader, i) => {
			for (const [hash] of leader) {
				const arr = map.get(hash) ?? [];
				arr.push(i);
				map.set(hash, arr);
			}
		});

		return map;
	}

	private async createCoordinates(
		entry: ShallowOrFullEntry<any> | EntryReplicated,
		minReplicas: number,
	) {
		const cursor = await this.domain.fromEntry(entry);
		const out = getEvenlySpacedU32(cursor, minReplicas);
		return out;
	}

	private async getCoordinates(entry: { hash: string }) {
		const result = await this.entryCoordinatesIndex
			.iterate({ query: { hash: entry.hash } })
			.all();
		return result.map((x) => x.value.coordinate);
	}

	private async persistCoordinate(
		properties: {
			coordinates: number[];
			entry: ShallowOrFullEntry<any> | EntryReplicated;
			leaders:
				| Map<
						string,
						{
							intersecting: boolean;
						}
				  >
				| false;
		},
		options?: {
			assignToRangeBoundary?: boolean;
		},
	) {
		let assignedToRangeBoundary =
			options?.assignToRangeBoundary ??
			shouldAssigneToRangeBoundary(
				properties.leaders,
				properties.coordinates.length,
			);

		for (const coordinate of properties.coordinates) {
			await this.entryCoordinatesIndex.put(
				new EntryReplicated({
					assignedToRangeBoundary,
					coordinate,
					meta: properties.entry.meta,
					hash: properties.entry.hash,
				}),
			);
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

	private async deleteCoordinates(
		properties: { gid: string } | { hash: string },
	) {
		await this.entryCoordinatesIndex.del({ query: properties });
	}

	async getDefaultMinRoleAge(): Promise<number> {
		if ((await this.isReplicating()) === false) {
			return 0;
		}

		const now = +new Date();
		const replLength = await this.replicationIndex.getSize();
		const diffToOldest =
			replLength > 1 ? now - this.oldestOpenTime - 1 : Number.MAX_SAFE_INTEGER;
		return Math.min(
			this.timeUntilRoleMaturity,
			Math.max(diffToOldest, this.timeUntilRoleMaturity),
			Math.max(
				Math.round((this.timeUntilRoleMaturity * Math.log(replLength + 1)) / 3),
				this.timeUntilRoleMaturity,
			),
		); // / 3 so that if 2 replicators and timeUntilRoleMaturity = 1e4 the result will be 1
	}

	private async findLeadersFromU32(
		cursor: u32[],
		options?: {
			roleAge?: number;
		},
	): Promise<Map<string, { intersecting: boolean }>> {
		const roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge()); // TODO -500 as is added so that i f someone else is just as new as us, then we treat them as mature as us. without -500 we might be slower syncing if two nodes starts almost at the same time
		return getSamples(cursor, this.replicationIndex, roleAge);
	}

	async isReplicator(
		entry: Entry<any>,
		options?: {
			candidates?: string[];
			roleAge?: number;
		},
	) {
		return this.isLeader(
			{ entry, replicas: decodeReplicas(entry).getValue(this) },
			options,
		);
	}

	async handleSubscriptionChange(
		publicKey: PublicSignKey,
		topics: string[],
		subscribed: boolean,
	) {
		if (!topics.includes(this.topic)) {
			return;
		}

		if (!subscribed) {
			for (const [_a, b] of this._gidPeersHistory) {
				b.delete(publicKey.hashcode());
			}
			this.clearSyncProcessPublicKey(publicKey);

			(await this.replicationIndex.count({
				query: { hash: publicKey.hashcode() },
			})) > 0 &&
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
							mode: new SilentDelivery({ redundancy: 1, to: [publicKey] }),
						},
					)
					.catch((e) => logger.error(e.toString()));

				if (this.v8Behaviour) {
					// for backwards compatibility
					this.rpc
						.send(new ResponseRoleMessage({ role: await this.getRole() }), {
							mode: new SilentDelivery({ redundancy: 1, to: [publicKey] }),
						})
						.catch((e) => logger.error(e.toString()));
				}
			}
		} else {
			await this.removeReplicator(publicKey);
		}
	}

	prune(
		entries:
			| (EntryReplicated | ShallowOrFullEntry<any>)[]
			| Map<string, EntryReplicated | ShallowOrFullEntry<any>>,
		options?: { timeout?: number; unchecked?: boolean },
	): Promise<any>[] {
		if (options?.unchecked) {
			return [...entries.values()].map((x) => {
				this._gidPeersHistory.delete(x.meta.gid);
				return this.log.remove(x, {
					recursively: true,
				});
			});
		}

		// ask network if they have they entry,
		// so I can delete it

		// There is a few reasons why we might end up here

		// - Two logs merge, and we should not anymore keep the joined log replicated (because we are not responsible for the resulting gid)
		// - An entry is joined, where min replicas is lower than before (for all heads for this particular gid) and therefore we are not replicating anymore for this particular gid
		// - Peers join and leave, which means we might not be a replicator anymore

		const promises: Promise<any>[] = [];
		const filteredEntries: (EntryReplicated | ShallowOrFullEntry<any>)[] = [];
		const deleted = new Set();

		for (const entry of entries.values()) {
			const pendingPrev = this._pendingDeletes.get(entry.hash);
			if (pendingPrev) {
				promises.push(pendingPrev.promise.promise);
				continue;
			}
			filteredEntries.push(entry);

			const existCounter = new Set<string>();
			const minReplicas = decodeReplicas(entry);
			const deferredPromise: DeferredPromise<void> = pDefer();

			const clear = () => {
				//pendingPrev?.clear();
				const pending = this._pendingDeletes.get(entry.hash);
				if (pending?.promise === deferredPromise) {
					this._pendingDeletes.delete(entry.hash);
				}
				clearTimeout(timeout);
			};
			const resolve = () => {
				clear();
				deferredPromise.resolve();
			};

			const reject = (e: any) => {
				clear();
				deferredPromise.reject(e);
			};

			let cursor: number[] | undefined = undefined;

			const timeout = setTimeout(async () => {
				reject(
					new Error("Timeout for checked pruning: Closed: " + this.closed),
				);
			}, options?.timeout ?? 1e4);

			this._pendingDeletes.set(entry.hash, {
				promise: deferredPromise,
				clear: () => {
					clear();
				},
				reject,
				resolve: async (publicKeyHash: string) => {
					const minReplicasValue = minReplicas.getValue(this);
					const minMinReplicasValue = this.replicas.max
						? Math.min(minReplicasValue, this.replicas.max.getValue(this))
						: minReplicasValue;

					const leaders = await this.waitForIsLeader(
						cursor ??
							(cursor = await this.createCoordinates(
								entry,
								minMinReplicasValue,
							)),
						publicKeyHash,
					);
					if (leaders) {
						if (leaders.has(this.node.identity.publicKey.hashcode())) {
							reject(new Error("Failed to delete, is leader"));
							return;
						}

						existCounter.add(publicKeyHash);
						if (minMinReplicasValue <= existCounter.size) {
							clear();
							this._gidPeersHistory.delete(entry.meta.gid);
							this.log
								.remove(entry, {
									recursively: true,
								})
								.then(() => {
									deleted.add(entry.hash);
									return resolve();
								})
								.catch((e: any) => {
									reject(new Error("Failed to delete entry: " + e.toString()));
								});
						}
					}
				},
			});

			promises.push(deferredPromise.promise);
		}

		if (filteredEntries.length === 0) {
			return promises;
		}

		const emitMessages = (entries: string[], to: string) => {
			this.rpc.send(
				new RequestIPrune({
					hashes: entries,
				}),
				{
					mode: new SilentDelivery({
						to: [to], // TODO group by peers?
						redundancy: 1,
					}),
					priority: 1,
				},
			);
		};

		const maxReplicasValue = maxReplicas(this, filteredEntries);
		this.groupByLeaders(
			filteredEntries.map((x) => {
				return { entry: x, replicas: maxReplicasValue }; // TODO choose right maxReplicasValue, should it really be for all entries combined?
			}),
		).then((map) => {
			for (const [peer, idx] of map) {
				emitMessages(
					idx.map((i) => filteredEntries[i].hash),
					peer,
				);
			}
		});

		const onPeersChange = async (e: CustomEvent<ReplicatorJoinEvent>) => {
			if (e.detail.publicKey.equals(this.node.identity.publicKey) === false) {
				const peerEntries = (
					await this.groupByLeaders(
						filteredEntries
							.filter((x) => !deleted.has(x.hash))
							.map((x) => {
								return { entry: x, replicas: maxReplicasValue }; // TODO choose right maxReplicasValue, should it really be for all entries combined?
							}),
					)
				).get(e.detail.publicKey.hashcode());
				if (peerEntries && peerEntries.length > 0) {
					emitMessages(
						peerEntries.map((x) => filteredEntries[x].hash),
						e.detail.publicKey.hashcode(),
					);
				}
			}
		};

		// check joining peers
		this.events.addEventListener("replicator:mature", onPeersChange);
		this.events.addEventListener("replicator:join", onPeersChange);
		Promise.allSettled(promises).finally(() => {
			this.events.removeEventListener("replicator:mature", onPeersChange);
			this.events.removeEventListener("replicator:join", onPeersChange);
		});

		return promises;
	}

	/**
	 * For debugging
	 */
	async getPrunable() {
		const heads = await this.log.getHeads(true).all();
		let prunable: Entry<any>[] = [];
		for (const head of heads) {
			const isLeader = await this.isLeader({
				entry: head,
				replicas: maxReplicas(this, [head]),
			});

			if (!isLeader) {
				prunable.push(head);
			}
		}
		return prunable;
	}

	async getNonPrunable() {
		const heads = await this.log.getHeads(true).all();
		let nonPrunable: Entry<any>[] = [];
		for (const head of heads) {
			const isLeader = await this.isLeader({
				entry: head,
				replicas: maxReplicas(this, [head]),
			});

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

		this.onReplicationChange(
			(await this.getMyReplicationSegments()).map((x) => {
				return { range: x, type: "added" };
			}),
		);
	}

	async waitForPruned() {
		await waitFor(() => this._pendingDeletes.size === 0);
	}

	async onReplicationChange(
		changeOrChanges: ReplicationChanges | ReplicationChanges[],
	) {
		/**
		 * TODO use information of new joined/leaving peer to create a subset of heads
		 * that we potentially need to share with other peers
		 */

		if (this.closed) {
			return;
		}

		const change = mergeReplicationChanges(changeOrChanges);
		const changed = false;

		try {
			await this.log.trim();

			const uncheckedDeliver: Map<string, Set<string>> = new Map();

			const allEntriesToDelete: EntryReplicated[] = [];

			for await (const { gid, entries: coordinates } of toRebalance(
				change,
				this.entryCoordinatesIndex,
			)) {
				if (this.closed) {
					break;
				}
				const oldPeersSet = this._gidPeersHistory.get(gid);

				if (this.closed) {
					return;
				}

				let { isLeader, leaders: currentPeers } = await this.findLeadersPersist(
					coordinates.map((x) => x.coordinate),
					coordinates[0],
					{
						roleAge: 0,
						persist: {
							prev: coordinates,
						},
					},
				);

				if (isLeader) {
					for (const entry of coordinates) {
						this.pruneDebouncedFn.delete(entry.hash);
					}
				}

				const currentPeersSet = new Set<string>(currentPeers.keys());
				this._gidPeersHistory.set(gid, currentPeersSet);

				for (const [currentPeer] of currentPeers) {
					if (currentPeer === this.node.identity.publicKey.hashcode()) {
						continue;
					}

					if (!oldPeersSet?.has(currentPeer)) {
						let set = uncheckedDeliver.get(currentPeer);
						if (!set) {
							set = new Set();
							uncheckedDeliver.set(currentPeer, set);
						}

						for (const entry of coordinates) {
							set.add(entry.hash);
						}
					}
				}

				if (!isLeader) {
					if (currentPeers.size > 0) {
						// If we are observer, never prune locally created entries, since we dont really know who can store them
						// if we are replicator, we will always persist entries that we need to so filtering on createdLocally will not make a difference
						let entriesToDelete = coordinates;

						if (this.sync) {
							entriesToDelete = entriesToDelete.filter(
								(entry) => this.sync!(entry) === false,
							);
						}
						allEntriesToDelete.push(...entriesToDelete);
					}
				} else {
					for (const entry of coordinates) {
						await this._pendingDeletes
							.get(entry.hash)
							?.reject(
								new Error(
									"Failed to delete, is leader again. Closed: " + this.closed,
								),
							);
					}
				}
			}

			for (const [target, entries] of uncheckedDeliver) {
				this.rpc.send(new RequestMaybeSync({ hashes: [...entries] }), {
					mode: new SilentDelivery({ to: [target], redundancy: 1 }),
				});
			}

			if (allEntriesToDelete.length > 0) {
				allEntriesToDelete.map((x) =>
					this.pruneDebouncedFn.add({ key: x.hash, value: x }),
				);
			}
			return changed;
		} catch (error: any) {
			logger.error(error.toString());
			throw error;
		}
	}

	private async requestSync(hashes: string[], to: Set<string> | string[]) {
		const now = +new Date();
		for (const node of to) {
			let map = this.syncInFlight.get(node);
			if (!map) {
				map = new Map();
				this.syncInFlight.set(node, map);
			}
			for (const hash of hashes) {
				map.set(hash, { timestamp: now });
			}
		}

		await this.rpc.send(
			new ResponseMaybeSync({
				hashes: hashes,
			}),
			{
				mode: new SilentDelivery({ to, redundancy: 1 }),
			},
		);
	}

	async _onUnsubscription(evt: CustomEvent<UnsubcriptionEvent>) {
		logger.debug(
			`Peer disconnected '${evt.detail.from.hashcode()}' from '${JSON.stringify(
				evt.detail.unsubscriptions.map((x) => x),
			)} '`,
		);
		this.latestReplicationInfoMessage.delete(evt.detail.from.hashcode());

		return this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.unsubscriptions,
			false,
		);
	}

	async _onSubscription(evt: CustomEvent<SubscriptionEvent>) {
		logger.debug(
			`New peer '${evt.detail.from.hashcode()}' connected to '${JSON.stringify(
				evt.detail.subscriptions.map((x) => x),
			)}'`,
		);
		this.remoteBlocks.onReachable(evt.detail.from);

		return this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.subscriptions,
			true,
		);
	}

	async addToHistory(usedMemory: number, factor: number) {
		(this.history || (this.history = [])).push({ usedMemory, factor });

		// Keep only the last N entries in the history array (you can adjust N based on your needs)
		const maxHistoryLength = 10;
		if (this.history.length > maxHistoryLength) {
			this.history.shift();
		}
	}

	async calculateTrend() {
		// Calculate the average change in factor per unit change in memory usage
		const factorChanges = this.history.map((entry, index) => {
			if (index > 0) {
				const memoryChange =
					entry.usedMemory - this.history[index - 1].usedMemory;
				if (memoryChange !== 0) {
					const factorChange = entry.factor - this.history[index - 1].factor;
					return factorChange / memoryChange;
				}
			}
			return 0;
		});

		// Return the average factor change per unit memory change
		return (
			factorChanges.reduce((sum, change) => sum + change, 0) /
			factorChanges.length
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

				const relativeDifference =
					Math.abs(dynamicRange.widthNormalized - newFactor) /
					dynamicRange.widthNormalized;

				if (relativeDifference > 0.0001) {
					// TODO can not reuse old range, since it will (potentially) affect the index because of sideeffects
					dynamicRange = new ReplicationRangeIndexable({
						offset: hashToU32(this.node.identity.publicKey.bytes),
						length: scaleToU32(newFactor),
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
					this.rebalanceParticipationDebounced?.();

					return true;
				} else {
					this.rebalanceParticipationDebounced?.();
				}
				return false;
			}
			return false;
		};

		const resp = await fn();

		return resp;
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
			range = new ReplicationRangeIndexable({
				normalized: true,
				offset: Math.random(),
				length: 0,
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
				logger.warn("Not allowed to replicate by canReplicate");
				return;
			}
		}
		return range;
	}

	private clearSyncProcess(hash: string) {
		const inflight = this.syncInFlightQueue.get(hash);
		if (inflight) {
			for (const key of inflight) {
				const map = this.syncInFlightQueueInverted.get(key.hashcode());
				if (map) {
					map.delete(hash);
					if (map.size === 0) {
						this.syncInFlightQueueInverted.delete(key.hashcode());
					}
				}
			}

			this.syncInFlightQueue.delete(hash);
		}
	}

	private clearSyncProcessPublicKey(publicKey: PublicSignKey) {
		this.syncInFlight.delete(publicKey.hashcode());
		const map = this.syncInFlightQueueInverted.get(publicKey.hashcode());
		if (map) {
			for (const hash of map) {
				const arr = this.syncInFlightQueue.get(hash);
				if (arr) {
					const filtered = arr.filter((x) => !x.equals(publicKey));
					if (filtered.length > 0) {
						this.syncInFlightQueue.set(hash, filtered);
					} else {
						this.syncInFlightQueue.delete(hash);
					}
				}
			}
			this.syncInFlightQueueInverted.delete(publicKey.hashcode());
		}
	}

	private async onEntryAdded(entry: Entry<any>) {
		const ih = this._pendingIHave.get(entry.hash);
		if (ih) {
			ih.clear();
			ih.callback(entry);
		}

		this.clearSyncProcess(entry.hash);
	}

	onEntryRemoved(hash: string) {
		this.clearSyncProcess(hash);
	}
}
