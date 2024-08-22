import { BorshError, field, variant } from "@dao-xyz/borsh";
import { CustomEvent } from "@libp2p/interface";
import { AnyBlockStore, RemoteBlocks } from "@peerbit/blocks";
import { Cache } from "@peerbit/cache";
import {
	AccessError,
	PublicSignKey,
	sha256Base64Sync,
	sha256Sync,
} from "@peerbit/crypto";
import {
	And,
	ByteMatchQuery,
	CountRequest,
	DeleteRequest,
	type Index,
	Or,
	SearchRequest,
	Sort,
	StringMatch,
	SumRequest,
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
import { AbortError, delay, waitFor } from "@peerbit/time";
import debounce from "p-debounce";
import pDefer, { type DeferredPromise } from "p-defer";
import PQueue from "p-queue";
import { concat } from "uint8arrays";
import { BlocksMessage } from "./blocks.js";
import { type CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
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
	getCoverSet,
	getSamples,
	isMatured,
	minimumWidthToCover,
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
	type ReplicationDomain,
	type u32,
} from "./replication-domain.js";
import {
	AbsoluteReplicas,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	ReplicationError,
	ReplicationIntent,
	type ReplicationLimits,
	ReplicationRange,
	ReplicationRangeIndexable,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
	StoppedReplicating,
	decodeReplicas,
	encodeReplicas,
	maxReplicas,
} from "./replication.js";
import { MAX_U32, Observer, Replicator, scaleToU32 } from "./role.js";

export {
	type ReplicationDomain,
	type ReplicationDomainHash,
	type ReplicationDomainTime,
	createReplicationDomainHash,
	createReplicationDomainTime,
};
export { type CPUUsage, CPUUsageIntervalLag };
export * from "./replication.js";

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

const groupByGid = async <
	T extends ShallowEntry | Entry<any> | EntryWithRefs<any>,
>(
	entries: T[],
): Promise<Map<string, T[]>> => {
	const groupByGid: Map<string, T[]> = new Map();
	for (const head of entries) {
		const gid = await (head instanceof Entry
			? (await head.getMeta()).gid
			: head instanceof ShallowEntry
				? head.meta.gid
				: (await head.entry.getMeta()).gid);
		let value = groupByGid.get(gid);
		if (!value) {
			value = [];
			groupByGid.set(gid, value);
		}
		value.push(head);
	}
	return groupByGid;
};

export type ReplicationLimitsOptions =
	| Partial<ReplicationLimits>
	| { min?: number; max?: number };

export type DynamicReplicationOptions = {
	limits?: {
		storage?: number;
		cpu?: number | { max: number; monitor?: CPUUsage };
	};
};

export type FixedReplicationOptions = {
	normalized?: boolean;
	factor: number | "all" | "right";
	strict?: boolean; // if true, only this range will be replicated
	offset?: number;
};

export type ReplicationOptions =
	| DynamicReplicationOptions
	| FixedReplicationOptions
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
	return true;
};

export type SharedLogOptions<T, D extends ReplicationDomain<any, T>> = {
	replicate?: ReplicationOptions;
	replicas?: ReplicationLimitsOptions;
	respondToIHaveTimeout?: number;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
	sync?: (entry: Entry<T> | ShallowEntry) => boolean;
	timeUntilRoleMaturity?: number;
	waitForReplicatorTimeout?: number;
	distributionDebounceTime?: number;
	compatibility?: number;
	domain?: D;
};

export const DEFAULT_MIN_REPLICAS = 2;
export const WAIT_FOR_REPLICATOR_TIMEOUT = 9000;
export const WAIT_FOR_ROLE_MATURITY = 5000;
const REBALANCE_DEBOUNCE_INTERVAL = 100;
const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;

export type Args<
	T,
	D extends ReplicationDomain<any, T> = ReplicationDomainHash,
> = LogProperties<T> & LogEvents<T> & SharedLogOptions<T, D>;

export type SharedAppendOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
	replicate?: boolean;
	target?: "all" | "replicators";
};

type ReplicatorJoinEvent = { publicKey: PublicSignKey };
type ReplicatorLeaveEvent = { publicKey: PublicSignKey };
type ReplicationChange = { publicKey: PublicSignKey };

export interface SharedLogEvents extends ProgramEvents {
	"replicator:join": CustomEvent<ReplicatorJoinEvent>;
	"replicator:leave": CustomEvent<ReplicatorLeaveEvent>;
	"replication:change": CustomEvent<ReplicationChange>;
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
	private _isReplicating: boolean;
	private _isAdaptiveReplicating: boolean;

	private _replicationRangeIndex!: Index<ReplicationRangeIndexable>;
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
	private _gidParentCache!: Cache<Entry<any>[]>;
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
		{ clear: () => void; callback: (entry: Entry<T>) => void }
	>;

	private latestReplicationInfoMessage!: Map<string, bigint>;

	private remoteBlocks!: RemoteBlocks;

	private openTime!: number;
	private oldestOpenTime!: number;

	private sync?: (entry: Entry<T> | ShallowEntry) => boolean;

	// A fn that we can call many times that recalculates the participation role
	private rebalanceParticipationDebounced:
		| ReturnType<typeof debounce>
		| undefined;

	// regular distribution checks
	private distributeInterval!: ReturnType<typeof setInterval>;
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
	domain: D;

	private pq: PQueue<any>;

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
		const sum = await this.replicationIndex.sum(
			new SumRequest({ key: "width" }),
		);
		return Number(sum) / MAX_U32;
	}

	async countReplicationSegments() {
		const count = await this.replicationIndex.count(
			new CountRequest({
				query: new StringMatch({
					key: "hash",
					value: this.node.identity.publicKey.hashcode(),
				}),
			}),
		);
		return count;
	}

	private setupRebalanceDebounceFunction() {
		this.rebalanceParticipationDebounced = undefined;
		this.rebalanceParticipationDebounced = debounce(
			() => this.rebalanceParticipation(),
			/* Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.getSize() || 0) *
					REBALANCE_DEBOUNCE_INTERVAL
				)
			) */
			REBALANCE_DEBOUNCE_INTERVAL, // TODO make this dynamic on the number of replicators
		);
	}
	private async _replicate(
		options?: ReplicationOptions,
		{
			reset,
			checkDuplicates,
		}: { reset?: boolean; checkDuplicates?: boolean } = {},
	) {
		let offsetWasProvided = false;
		if (
			options === false ||
			options === 0 ||
			(options as FixedReplicationOptions)?.factor === 0
		) {
			await this.unreplicate();
		} else {
			let range: ReplicationRangeIndexable;

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
				range = maybeRange;

				offsetWasProvided = true;
			} else if (
				typeof options === "object" &&
				Object.keys(options).length === 0
			) {
				throw new Error("Unexpected");
			} else if (options instanceof ReplicationRange) {
				range = options.toReplicationRangeIndexable(
					this.node.identity.publicKey,
				);

				offsetWasProvided = true;
			} else {
				let fixedOptions: FixedReplicationOptions;
				if (typeof options === "number") {
					fixedOptions = {
						factor: options,
					} as FixedReplicationOptions;
				} else {
					fixedOptions = { ...options } as FixedReplicationOptions;
				}

				// fixed
				const normalized = fixedOptions.normalized ?? true;
				offsetWasProvided = fixedOptions.offset != null;
				const offset =
					fixedOptions.offset ??
					(normalized ? Math.random() : scaleToU32(Math.random()));
				let factor = fixedOptions.factor;
				let width = normalized ? 1 : scaleToU32(1);
				range = new ReplicationRangeIndexable({
					normalized,
					offset: offset,
					length:
						typeof factor === "number"
							? factor
							: factor === "all"
								? width
								: width - offset,
					publicKeyHash: this.node.identity.publicKey.hashcode(),
					mode: fixedOptions.strict
						? ReplicationIntent.Strict
						: ReplicationIntent.NonStrict, // automatic means that this range might be reused later for dynamic replication behaviour
					timestamp: BigInt(+new Date()),
				});
			}

			this.oldestOpenTime = Math.min(
				Number(range.timestamp),
				this.oldestOpenTime,
			);

			let resetRanges = reset;
			if (!resetRanges && !offsetWasProvided) {
				resetRanges = true;
				// because if we do something like replicate ({ factor: 0.5 }) it means that we want to replicate 50%
				// but ({ replicate: 0.5, offset: 0.5 }) means that we want to add a range
				// TODO make behaviour more clear
			}
			await this.startAnnounceReplicating([range], {
				reset: resetRanges ?? false,
				checkDuplicates,
			});
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
		this.setupRebalanceDebounceFunction();
	}

	async replicate(
		rangeOrEntry?: ReplicationRange | ReplicationOptions | Entry<T>,
		options?: { reset?: boolean; checkDuplicates?: boolean },
	) {
		let range: ReplicationRange | ReplicationOptions | undefined = undefined;

		if (rangeOrEntry instanceof ReplicationRange) {
			range = rangeOrEntry;
		} else if (rangeOrEntry instanceof Entry) {
			range = {
				factor: 1,
				offset: await this.domain.fromEntry(rangeOrEntry),
				normalized: false,
			};
		} else {
			range = rangeOrEntry ?? true;
		}

		await this._replicate(range, options);

		// assume new role
		await this.distribute();
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

		const indexed = await this.replicationIndex.query(
			new SearchRequest({
				query: {
					width: 1,
					start1: range.offset,
				},
			}),
		);

		const segmentIds = indexed.results.map((x) => x.id.key as Uint8Array);
		await this.removeReplicationRange(segmentIds, this.node.identity.publicKey);
		await this.rpc.send(new StoppedReplicating({ segmentIds }), {
			priority: 1,
		});
	}

	private async removeReplicator(key: PublicSignKey) {
		const fn = async () => {
			await this.replicationIndex.del(
				new DeleteRequest({ query: { hash: key.hashcode() } }),
			);

			await this.updateOldestTimestampFromIndex();

			if (this.node.identity.publicKey.equals(key)) {
				// announce that we are no longer replicating
				await this.rpc.send(
					new AllReplicatingSegmentsMessage({ segments: [] }),
					{ priority: 1 },
				);
			}

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChange>("replication:change", {
					detail: { publicKey: key },
				}),
			);

			if (!key.equals(this.node.identity.publicKey)) {
				this.rebalanceParticipationDebounced?.();
			}
		};

		return this.pq.add(fn);
	}

	private async updateOldestTimestampFromIndex() {
		const oldestTimestampFromDB = (
			await this.replicationIndex.query(
				new SearchRequest({
					fetch: 1,
					sort: [new Sort({ key: "timestamp", direction: "asc" })],
				}),
				{ reference: true },
			)
		).results[0]?.value.timestamp;
		this.oldestOpenTime =
			oldestTimestampFromDB != null
				? Number(oldestTimestampFromDB)
				: +new Date();
	}

	private async removeReplicationRange(id: Uint8Array[], from: PublicSignKey) {
		const fn = async () => {
			let idMatcher = new Or(
				id.map((x) => new ByteMatchQuery({ key: "id", value: x })),
			);

			// make sure we are not removing something that is owned by the replicator
			let identityMatcher = new StringMatch({
				key: "hash",
				value: from.hashcode(),
			});

			let query = new And([idMatcher, identityMatcher]);

			await this.replicationIndex.del(new DeleteRequest({ query }));

			await this.updateOldestTimestampFromIndex();

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChange>("replication:change", {
					detail: { publicKey: from },
				}),
			);

			if (!from.equals(this.node.identity.publicKey)) {
				this.rebalanceParticipationDebounced?.();
			}
		};

		return this.pq.add(fn);
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
				return false;
			}

			let prevCount = await this.replicationIndex.count(
				new CountRequest({
					query: new StringMatch({ key: "hash", value: from.hashcode() }),
				}),
			);
			const isNewReplicator = prevCount === 0;

			if (reset) {
				await this.replicationIndex.del(
					new DeleteRequest({ query: { hash: from.hashcode() } }),
				);
			} else if (checkDuplicates) {
				let deduplicated: any[] = [];
				for (const range of ranges) {
					const prev = await this.replicationIndex.query(
						new SearchRequest({
							query: {
								start1: range.start1,
								width: range.width,
								mode: range.mode,
								hash: range.hash,
							},
						}),
					);

					if (prev.results.length === 0) {
						deduplicated.push(range);
					}
				}
				ranges = deduplicated;
			}

			for (const range of ranges) {
				await this.replicationIndex.put(range);
				if (!reset) {
					this.oldestOpenTime = Math.min(
						Number(range.timestamp),
						this.oldestOpenTime,
					);
				}
			}

			if (reset) {
				await this.updateOldestTimestampFromIndex();
			}

			this.events.dispatchEvent(
				new CustomEvent<ReplicationChange>("replication:change", {
					detail: { publicKey: from },
				}),
			);

			if (isNewReplicator) {
				this.events.dispatchEvent(
					new CustomEvent<ReplicatorJoinEvent>("replicator:join", {
						detail: { publicKey: from },
					}),
				);
			}

			if (!from.equals(this.node.identity.publicKey)) {
				this.rebalanceParticipationDebounced?.();
			}
			return true;
		};
		return this.pq.add(fn);
	}

	async startAnnounceReplicating(
		range: ReplicationRangeIndexable[],
		options: { reset?: boolean; checkDuplicates?: boolean } = {},
	) {
		const added = await this.addReplicationRange(
			range,
			this.node.identity.publicKey,
			options,
		);
		if (!added) {
			logger.warn("Not allowed to replicate by canReplicate");
		}

		if (added) {
			if (options.reset) {
				await this.rpc.send(
					new AllReplicatingSegmentsMessage({
						segments: range.map((x) => x.toReplicationRange()),
					}),
					{ priority: 1 },
				);
			} else {
				await this.rpc.send(
					new AddedReplicationSegmentMessage({
						segments: range.map((x) => x.toReplicationRange()),
					}),
					{
						priority: 1,
					},
				);
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
		const minReplicasData = encodeReplicas(
			options?.replicas
				? typeof options.replicas === "number"
					? new AbsoluteReplicas(options.replicas)
					: options.replicas
				: this.replicas.min,
		);

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

		for (const message of await createExchangeHeadsMessages(
			this.log,
			[result.entry],
			this._gidParentCache,
		)) {
			if (options?.target === "replicators" || !options?.target) {
				const minReplicas = decodeReplicas(result.entry).getValue(this);

				let leaders: string[] | Set<string> = await this.findLeaders(
					result.entry,
					minReplicas,
				);

				const isLeader = leaders.includes(
					this.node.identity.publicKey.hashcode(),
				);

				if (message.heads[0].gidRefrences.length > 0) {
					const newAndOldLeaders = new Set(leaders);
					for (const ref of message.heads[0].gidRefrences) {
						const entryFromGid = this.log.entryIndex.getHeads(ref, false);
						for (const entry of await entryFromGid.all()) {
							for (const hash of await this.findLeaders(entry, minReplicas)) {
								newAndOldLeaders.add(hash);
							}
						}
					}
					leaders = newAndOldLeaders;
				}

				let set = this._gidPeersHistory.get(result.entry.meta.gid);
				if (!set) {
					set = new Set(leaders);
					this._gidPeersHistory.set(result.entry.meta.gid, set);
				} else {
					for (const receiver of leaders) {
						set.add(receiver);
					}
				}
				mode = isLeader
					? new SilentDelivery({ redundancy: 1, to: leaders })
					: new AcknowledgeDelivery({ redundancy: 1, to: leaders });
			}

			// TODO add options for waiting ?
			this.rpc.send(message, {
				mode,
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
		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 10 * 1000; // TODO make into arg
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
		this._gidParentCache = new Cache({ max: 100 }); // TODO choose a good number
		this._closeController = new AbortController();
		this._isTrustedReplicator = options?.canReplicate;
		this.sync = options?.sync;
		this._logProperties = options;
		this.pq = new PQueue({ concurrency: 1000 });

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
		const logIndex = await logScope.scope("log");
		await this.node.indexer.start(); // TODO why do we need to start the indexer here?

		/* this._totalParticipation = await this.calculateTotalParticipation(); */

		this._gidPeersHistory = new Map();

		await this.log.open(this.remoteBlocks, this.node.identity, {
			keychain: this.node.services.keychain,
			...this._logProperties,
			onChange: (change) => {
				this.onChange(change);
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
			responseHandler: this._onMessage.bind(this),
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

		// await this.log.load();

		// TODO (do better)
		// we do this distribution interval to eliminate the sideeffects arriving from updating roles and joining entries continously.
		// an alternative to this would be to call distribute/maybe prune after every join if our role has changed
		this.distributeInterval = setInterval(() => {
			this.distribute();
		}, 7.5 * 1000);

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
						from.add(value.shift()!.hashcode());
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
				this.syncMoreInterval = setTimeout(requestSync, 1e4);
			});
		};

		await this.replicate(options?.replicate, { checkDuplicates: true });
		requestSync();
	}

	async afterOpen(): Promise<void> {
		await super.afterOpen();

		// We do this here, because these calls requires this.closed == false
		/* await this._updateRole(); */
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

	async reload() {
		await this.log.load({ reset: true, reload: true });
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
			this.onEntryAdded(added);
		}
		for (const removed of change.removed) {
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

	async getCover(args: ExtractDomainArgs<D>, roleAge?: number) {
		roleAge = roleAge ?? (await this.getDefaultMinRoleAge());

		const range = await this.domain.fromArgs(args, this);

		const set = await getCoverSet(
			this.replicationIndex,
			roleAge,
			range.offset,
			range.length ??
				(await minimumWidthToCover(this.replicas.min.getValue(this))),
			MAX_U32,
		);

		// add all in flight
		for (const [key, _] of this.syncInFlight) {
			set.add(key);
		}

		return [...set];
	}

	private async _close() {
		clearTimeout(this.syncMoreInterval);
		clearInterval(this.distributeInterval);
		this.distributeQueue?.clear();

		this._closeController.abort();

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
		this._gidParentCache.clear();
		this._pendingDeletes.clear();
		this._pendingIHave.clear();
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		this.latestReplicationInfoMessage.clear();
		this._gidPeersHistory.clear();

		this._replicationRangeIndex = undefined as any;
		this.cpuUsage?.stop?.();
		/* this._totalParticipation = 0; */
		this.pq.clear();
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

					const toMerge: Entry<any>[] = [];
					let toDelete: Entry<any>[] | undefined = undefined;
					let maybeDelete: EntryWithRefs<any>[][] | undefined = undefined;

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

							const isReplicating = await this.isReplicating();

							let isLeader: string[] | false;

							if (isReplicating) {
								isLeader = await this.waitForIsLeader(
									latestEntry,
									Math.max(maxReplicasFromHead, maxReplicasFromNewEntries),
								);
							} else {
								isLeader = await this.findLeaders(
									latestEntry,
									Math.max(maxReplicasFromHead, maxReplicasFromNewEntries),
								);

								isLeader = isLeader.includes(
									this.node.identity.publicKey.hashcode(),
								)
									? isLeader
									: false;
							}

							if (isLeader) {
								if (isLeader.find((x) => x === context.from!.hashcode())) {
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
						};
						promises.push(fn());
					}
					await Promise.all(promises);

					if (this.closed) {
						return;
					}

					if (toMerge.length > 0) {
						await this.log.join(toMerge);
						toDelete &&
							Promise.all(this.prune(toDelete)).catch((e) => {
								logger.info(e.toString());
							});
						this.rebalanceParticipationDebounced?.();
					}

					/// we clear sync in flight here because we want to join before that, so that entries are totally accounted for
					for (const head of heads) {
						const set = this.syncInFlight.get(context.from.hashcode());
						if (set) {
							set.delete(head.entry.hash);
							if (set?.size === 0) {
								this.syncInFlight.delete(context.from.hashcode());
							}
						}
					}

					if (maybeDelete) {
						for (const entries of maybeDelete as EntryWithRefs<any>[][]) {
							const headsWithGid = await this.log.entryIndex
								.getHeads(entries[0].entry.meta.gid)
								.all();
							if (headsWithGid && headsWithGid.length > 0) {
								const minReplicas = maxReplicas(this, headsWithGid.values());

								const isLeader = await this.isLeader(
									entries[0].entry,
									minReplicas,
								);

								if (!isLeader) {
									Promise.all(this.prune(entries.map((x) => x.entry))).catch(
										(e) => {
											logger.info(e.toString());
										},
									);
								}
							}
						}
					}
				}
			} else if (msg instanceof RequestIPrune) {
				const hasAndIsLeader: string[] = [];
				for (const hash of msg.hashes) {
					const indexedEntry = await this.log.entryIndex.getShallow(hash);
					if (
						indexedEntry &&
						(await this.isLeader(
							indexedEntry.value,
							decodeReplicas(indexedEntry.value).getValue(this),
						))
					) {
						this._gidPeersHistory
							.get(indexedEntry.value.meta.gid)
							?.delete(context.from.hashcode());
						hasAndIsLeader.push(hash);
					} else {
						const prevPendingIHave = this._pendingIHave.get(hash);
						const pendingIHave = {
							clear: () => {
								clearTimeout(timeout);
								prevPendingIHave?.clear();
							},
							callback: async (entry: any) => {
								if (
									await this.isLeader(
										entry,
										decodeReplicas(entry).getValue(this),
									)
								) {
									this._gidPeersHistory
										.get(entry.meta.gid)
										?.delete(context.from!.hashcode());
									this.rpc.send(new ResponseIPrune({ hashes: [entry.hash] }), {
										mode: new SilentDelivery({
											to: [context.from!],
											redundancy: 1,
										}),
									});
								}

								prevPendingIHave && prevPendingIHave.callback(entry);
								this._pendingIHave.delete(entry.hash);
							},
						};
						const timeout = setTimeout(() => {
							const pendingIHaveRef = this._pendingIHave.get(hash);
							if (pendingIHave === pendingIHaveRef) {
								this._pendingIHave.delete(hash);
							}
						}, this._respondToIHaveTimeout);

						this._pendingIHave.set(hash, pendingIHave);
					}
				}

				await this.rpc.send(new ResponseIPrune({ hashes: hasAndIsLeader }), {
					mode: new SilentDelivery({ to: [context.from], redundancy: 1 }),
				});
			} else if (msg instanceof ResponseIPrune) {
				for (const hash of msg.hashes) {
					this._pendingDeletes.get(hash)?.resolve(context.from.hashcode());
				}
			} else if (msg instanceof RequestMaybeSync) {
				const requestHashes: string[] = [];
				for (const hash of msg.hashes) {
					const inFlight = this.syncInFlightQueue.get(hash);
					if (inFlight) {
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
					} else if (!(await this.log.has(hash))) {
						this.syncInFlightQueue.set(hash, []);
						requestHashes.push(hash); // request immediately (first time we have seen this hash)
					}
				}

				await this.requestSync(requestHashes, [context.from.hashcode()]);
			} else if (msg instanceof ResponseMaybeSync) {
				// TODO better choice of step size

				const entries = (
					await Promise.all(msg.hashes.map((x) => this.log.get(x)))
				).filter((x): x is Entry<any> => !!x);
				const messages = await createExchangeHeadsMessages(
					this.log,
					entries,
					this._gidParentCache,
				);

				// TODO perhaps send less messages to more receivers for performance reasons?
				// TODO wait for previous send to target before trying to send more?
				let p = Promise.resolve();
				for (const message of messages) {
					p = p.then(() =>
						this.rpc.send(message, {
							mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
						}),
					); // push in series, if one fails, then we should just stop
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
						const added = await this.addReplicationRange(
							replicationInfoMessage.segments.map((x) =>
								x.toReplicationRangeIndexable(context.from!),
							),
							context.from!,
							{ reset, checkDuplicates: true },
						);

						added && (await this.distribute());

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
		const ranges = await this.replicationIndex.query(
			new SearchRequest({
				query: [
					new StringMatch({
						key: "hash",
						value: this.node.identity.publicKey.hashcode(),
					}),
				],
				fetch: 0xffffffff,
			}),
		);
		return ranges.results.map((x) => x.value);
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

	/**
	 * TODO improve efficiency
	 */
	async getReplicators() {
		let set = new Set();
		const results = await this.replicationIndex.query(
			new SearchRequest({ fetch: 0xfffffff }),
			{ reference: true, shape: { hash: true } },
		);
		results.results.forEach((result) => {
			set.add(result.value.hash);
		});

		return set;
	}

	async waitForReplicator(...keys: PublicSignKey[]) {
		const check = async () => {
			for (const k of keys) {
				const rects = await this.replicationIndex?.query(
					new SearchRequest({
						query: [new StringMatch({ key: "hash", value: k.hashcode() })],
					}),
					{ reference: true },
				);
				const rect = await rects.results[0]?.value;

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
			replicate: boolean;
		},
	): Promise<void> {
		const joined = await this.log.join(entries, options);
		if (options?.replicate) {
			let p: Promise<void>[] = [];
			for (const entry of joined) {
				p.push(this.replicate(entry));
			}
		}
	}

	async isLeader(
		entry: ShallowOrFullEntry<any>,
		numberOfLeaders: number,
		options?: {
			candidates?: string[];
			roleAge?: number;
		},
	): Promise<boolean> {
		const isLeader = (
			await this.findLeaders(entry, numberOfLeaders, options)
		).find((l) => l === this.node.identity.publicKey.hashcode());
		return !!isLeader;
	}

	private async waitForIsLeader(
		entry: ShallowOrFullEntry<T>,
		numberOfLeaders: number,
		timeout = this.waitForReplicatorTimeout,
	): Promise<string[] | false> {
		return new Promise((resolve, reject) => {
			const removeListeners = () => {
				this.events.removeEventListener("replication:change", roleListener);
				this._closeController.signal.addEventListener("abort", abortListener);
			};
			const abortListener = () => {
				removeListeners();
				clearTimeout(timer);
				resolve(false);
			};

			const timer = setTimeout(() => {
				removeListeners();
				resolve(false);
			}, timeout);

			const check = () =>
				this.findLeaders(entry, numberOfLeaders).then((leaders) => {
					const isLeader = leaders.find(
						(l) => l === this.node.identity.publicKey.hashcode(),
					);
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
			this._closeController.signal.addEventListener("abort", abortListener);

			check();
		});
	}

	async findLeaders(
		entry: ShallowOrFullEntry<any>,
		numberOfLeaders: number,
		options?: {
			roleAge?: number;
		},
	): Promise<string[]> {
		if (this.closed) {
			return [this.node.identity.publicKey.hashcode()]; // Assumption: if the store is closed, always assume we have responsibility over the data
		}

		const cursor = await this.domain.fromEntry(entry);
		return this.findLeadersFromU32(cursor, numberOfLeaders, options);
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
			diffToOldest,
			Math.round((this.timeUntilRoleMaturity * Math.log(replLength + 1)) / 3),
		); // / 3 so that if 2 replicators and timeUntilRoleMaturity = 1e4 the result will be 1
	}

	private async findLeadersFromU32(
		cursor: u32,
		numberOfLeaders: number,
		options?: {
			roleAge?: number;
		},
	) {
		const roleAge = options?.roleAge ?? (await this.getDefaultMinRoleAge()); // TODO -500 as is added so that i f someone else is just as new as us, then we treat them as mature as us. without -500 we might be slower syncing if two nodes starts almost at the same time
		return getSamples(cursor, this.replicationIndex, numberOfLeaders, roleAge);
	}

	async isReplicator(
		entry: Entry<any>,
		options?: {
			candidates?: string[];
			roleAge?: number;
		},
	) {
		return this.isLeader(entry, decodeReplicas(entry).getValue(this), options);
	}

	async handleSubscriptionChange(
		publicKey: PublicSignKey,
		topics: string[],
		subscribed: boolean,
	) {
		for (const topic of topics) {
			if (this.log.idString !== topic) {
				continue;
			}
		}

		if (!subscribed) {
			for (const [_a, b] of this._gidPeersHistory) {
				b.delete(publicKey.hashcode());
			}
			this.syncInFlight.delete(publicKey.hashcode());
			const waitingHashes = this.syncInFlightQueueInverted.get(
				publicKey.hashcode(),
			);
			if (waitingHashes) {
				for (const hash of waitingHashes) {
					let arr = this.syncInFlightQueue.get(hash);
					if (arr) {
						arr = arr.filter((x) => !x.equals(publicKey));
					}
					if (this.syncInFlightQueue.size === 0) {
						this.syncInFlightQueue.delete(hash);
					}
				}
			}
			this.syncInFlightQueueInverted.delete(publicKey.hashcode());
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
		entries: (Entry<any> | ShallowEntry)[],
		options?: { timeout?: number; unchecked?: boolean },
	): Promise<any>[] {
		if (options?.unchecked) {
			return entries.map((x) => {
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
		const filteredEntries: (Entry<any> | ShallowEntry)[] = [];
		for (const entry of entries) {
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

			const timeout = setTimeout(
				() => {
					reject(
						new Error("Timeout for checked pruning: Closed: " + this.closed),
					);
				},
				options?.timeout ?? 10 * 1000,
			);

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

					const leaders = await this.findLeaders(entry, minMinReplicasValue);

					if (
						leaders.find((x) => x === this.node.identity.publicKey.hashcode())
					) {
						reject(new Error("Failed to delete, is leader"));
						return;
					}

					if (leaders.find((x) => x === publicKeyHash)) {
						existCounter.add(publicKeyHash);
						if (minMinReplicasValue <= existCounter.size) {
							clear();
							this._gidPeersHistory.delete(entry.meta.gid);
							this.log
								.remove(entry, {
									recursively: true,
								})
								.then(() => {
									resolve();
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

		this.rpc.send(
			new RequestIPrune({ hashes: filteredEntries.map((x) => x.hash) }),
		);

		const onNewPeer = async (e: CustomEvent<ReplicatorJoinEvent>) => {
			if (e.detail.publicKey.equals(this.node.identity.publicKey) === false) {
				await this.rpc.send(
					new RequestIPrune({ hashes: filteredEntries.map((x) => x.hash) }),
					{
						mode: new SilentDelivery({
							to: [e.detail.publicKey.hashcode()],
							redundancy: 1,
						}),
					},
				);
			}
		};

		// check joining peers
		this.events.addEventListener("replicator:join", onNewPeer);
		Promise.allSettled(promises).finally(() =>
			this.events.removeEventListener("replicator:join", onNewPeer),
		);

		return promises;
	}

	async distribute() {
		// if there is one or more items waiting for run, don't bother adding a new item just wait for the queue to empty
		if (this.distributeQueue && this.distributeQueue?.size > 0) {
			return this.distributeQueue.onEmpty();
		}
		if (this.closed) {
			return;
		}
		const queue =
			this.distributeQueue ||
			(this.distributeQueue = new PQueue({ concurrency: 1 }));
		return queue
			.add(() =>
				delay(Math.min(this.log.length, this.distributionDebounceTime), {
					signal: this._closeController.signal,
				}).then(() => this._distribute()),
			)
			.catch(() => {}); // catch ignore delay abort errror
	}

	async _distribute() {
		/**
		 * TODO use information of new joined/leaving peer to create a subset of heads
		 * that we potentially need to share with other peers
		 */

		if (this.closed) {
			return;
		}

		const changed = false;
		await this.log.trim();
		const heads = await this.log.getHeads().all();

		const groupedByGid = await groupByGid(heads);
		const uncheckedDeliver: Map<string, (Entry<any> | ShallowEntry)[]> =
			new Map();
		const allEntriesToDelete: (Entry<any> | ShallowEntry)[] = [];

		for (const [gid, entries] of groupedByGid) {
			if (this.closed) {
				break;
			}

			if (entries.length === 0) {
				continue; // TODO maybe close store?
			}

			const oldPeersSet = this._gidPeersHistory.get(gid);
			const currentPeers = await this.findLeaders(
				getLatestEntry(entries)!,
				maxReplicas(this, entries), // pick max replication policy of all entries, so all information is treated equally important as the most important
			);

			const isLeader = currentPeers.find(
				(x) => x === this.node.identity.publicKey.hashcode(),
			);
			const currentPeersSet = new Set(currentPeers);
			this._gidPeersHistory.set(gid, currentPeersSet);

			for (const currentPeer of currentPeers) {
				if (currentPeer === this.node.identity.publicKey.hashcode()) {
					continue;
				}

				if (!oldPeersSet?.has(currentPeer)) {
					let arr = uncheckedDeliver.get(currentPeer);
					if (!arr) {
						arr = [];
						uncheckedDeliver.set(currentPeer, arr);
					}

					for (const entry of entries) {
						arr.push(entry);
					}
				}
			}

			if (!isLeader) {
				if (currentPeers.length > 0) {
					// If we are observer, never prune locally created entries, since we dont really know who can store them
					// if we are replicator, we will always persist entries that we need to so filtering on createdLocally will not make a difference
					let entriesToDelete = entries;

					if (this.sync) {
						entriesToDelete = entriesToDelete.filter(
							(entry) => this.sync!(entry) === false,
						);
					}
					allEntriesToDelete.push(...entriesToDelete);
				}
			} else {
				for (const entry of entries) {
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
			this.rpc.send(
				new RequestMaybeSync({ hashes: entries.map((x) => x.hash) }),
				{
					mode: new SilentDelivery({ to: [target], redundancy: 1 }),
				},
			);
		}

		if (allEntriesToDelete.length > 0) {
			Promise.allSettled(this.prune(allEntriesToDelete)).catch((e) => {
				logger.info(e.toString());
			});
		}
		return changed;
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
			)}'`,
		);
		this.latestReplicationInfoMessage.delete(evt.detail.from.hashcode());

		this.events.dispatchEvent(
			new CustomEvent<ReplicatorLeaveEvent>("replicator:leave", {
				detail: { publicKey: evt.detail.from },
			}),
		);

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

	async rebalanceParticipation(onRoleChange = true) {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu

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
					checkDuplicates: true,
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
	}
	async getDynamicRange() {
		let dynamicRangeId = sha256Sync(
			concat([
				this.node.identity.publicKey.bytes,
				new TextEncoder().encode("dynamic"),
			]),
		);
		let range = (
			await this.replicationIndex.query(
				new SearchRequest({
					query: [
						new ByteMatchQuery({
							key: "id",
							value: dynamicRangeId,
						}),
					],
					fetch: 1,
				}),
			)
		)?.results[0]?.value;
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
	private onEntryAdded(entry: Entry<any>) {
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
