import { RequestContext, RPC } from "@peerbit/rpc";
import { TransportMessage } from "./message.js";
import {
	AppendOptions,
	Entry,
	Log,
	LogEvents,
	LogProperties,
	ShallowEntry
} from "@peerbit/log";
import { Program, ProgramEvents } from "@peerbit/program";
import { BinaryWriter, BorshError, field, variant } from "@dao-xyz/borsh";
import {
	AccessError,
	PublicSignKey,
	sha256,
	sha256Base64Sync
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RequestIPrune,
	RequestMaybeSync,
	ResponseIPrune,
	ResponseMaybeSync,
	createExchangeHeadsMessages
} from "./exchange-heads.js";
import {
	SubscriptionEvent,
	UnsubcriptionEvent
} from "@peerbit/pubsub-interface";
import { AbortError, delay, waitFor } from "@peerbit/time";
import { Observer, Replicator, Role } from "./role.js";
import {
	AbsoluteReplicas,
	ReplicationError,
	ReplicationLimits,
	ReplicatorRect,
	RequestRoleMessage,
	ResponseRoleMessage,
	decodeReplicas,
	encodeReplicas,
	hashToUniformNumber,
	maxReplicas
} from "./replication.js";
import pDefer, { DeferredPromise } from "p-defer";
import { Cache } from "@peerbit/cache";
import { CustomEvent } from "@libp2p/interface";
import yallist from "yallist";
import {
	AcknowledgeDelivery,
	DeliveryMode,
	SilentDelivery,
	NotStartedError
} from "@peerbit/stream-interface";
import { AnyBlockStore, RemoteBlocks } from "@peerbit/blocks";
import { BlocksMessage } from "./blocks.js";
import debounce from "p-debounce";
import { PIDReplicationController } from "./pid.js";
export * from "./replication.js";
import PQueue from "p-queue";
import { CPUUsage, CPUUsageIntervalLag } from "./cpu.js";
import { getCoverSet, getSamples, isMatured } from "./ranges.js";
export { type CPUUsage, CPUUsageIntervalLag };
export { Observer, Replicator, Role };

export const logger = loggerFn({ module: "shared-log" });

const groupByGid = async <T extends Entry<any> | EntryWithRefs<any>>(
	entries: T[]
): Promise<Map<string, T[]>> => {
	const groupByGid: Map<string, T[]> = new Map();
	for (const head of entries) {
		const gid = await (head instanceof Entry
			? head.getGid()
			: head.entry.getGid());
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

type StringRoleOptions = "observer" | "replicator";

export type AdaptiveReplicatorOptions = {
	type: "replicator";
	limits?: {
		storage?: number;
		cpu?: number | { max: number; monitor?: CPUUsage };
	};
};

export type FixedReplicatorOptions = {
	type: "replicator";
	factor: number;
};

export type ObserverType = {
	type: "observer";
};

export type RoleOptions =
	| StringRoleOptions
	| ObserverType
	| FixedReplicatorOptions
	| AdaptiveReplicatorOptions;

const isAdaptiveReplicatorOption = (
	options: FixedReplicatorOptions | AdaptiveReplicatorOptions
): options is AdaptiveReplicatorOptions => {
	if (
		(options as AdaptiveReplicatorOptions).limits ||
		(options as FixedReplicatorOptions).factor == null
	) {
		return true;
	}
	return false;
};

export type SharedLogOptions<T> = {
	role?: RoleOptions;
	replicas?: ReplicationLimitsOptions;
	respondToIHaveTimeout?: number;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
	sync?: (entry: Entry<T>) => boolean;
	timeUntilRoleMaturity?: number;
	waitForReplicatorTimeout?: number;
	distributionDebounceTime?: number;
};

export const DEFAULT_MIN_REPLICAS = 2;
export const WAIT_FOR_REPLICATOR_TIMEOUT = 9000;
export const WAIT_FOR_ROLE_MATURITY = 5000;
const REBALANCE_DEBOUNCE_INTERVAL = 100;
const DEFAULT_DISTRIBUTION_DEBOUNCE_TIME = 500;

export type Args<T> = LogProperties<T> & LogEvents<T> & SharedLogOptions<T>;

export type SharedAppendOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
	target?: "all" | "replicators";
};

type UpdateRoleEvent = { publicKey: PublicSignKey; role: Role };
export interface SharedLogEvents extends ProgramEvents {
	role: CustomEvent<UpdateRoleEvent>;
}

@variant("shared_log")
export class SharedLog<T = Uint8Array> extends Program<
	Args<T>,
	SharedLogEvents
> {
	@field({ type: Log })
	log: Log<T>;

	@field({ type: RPC })
	rpc: RPC<TransportMessage, TransportMessage>;

	// options
	private _role: Observer | Replicator;
	private _roleConfig: AdaptiveReplicatorOptions | Observer | Replicator;
	private _sortedPeersCache: yallist<ReplicatorRect> | undefined;
	private _totalParticipation: number;
	private _gidPeersHistory: Map<string, Set<string>>;

	private _onSubscriptionFn: (arg: any) => any;
	private _onUnsubscriptionFn: (arg: any) => any;

	private _canReplicate?: (
		publicKey: PublicSignKey,
		role: Replicator
	) => Promise<boolean> | boolean;

	private _logProperties?: LogProperties<T> & LogEvents<T>;
	private _closeController: AbortController;
	private _gidParentCache: Cache<Entry<any>[]>;
	private _respondToIHaveTimeout;
	private _pendingDeletes: Map<
		string,
		{
			promise: DeferredPromise<void>;
			clear: () => void;
			resolve: (publicKeyHash: string) => Promise<void> | void;
			reject(reason: any): Promise<void> | void;
		}
	>;

	private _pendingIHave: Map<
		string,
		{ clear: () => void; callback: (entry: Entry<T>) => void }
	>;

	private latestRoleMessages: Map<string, bigint>;

	private remoteBlocks: RemoteBlocks;

	private openTime: number;
	private oldestOpenTime: number;

	private sync?: (entry: Entry<T>) => boolean;

	// A fn that we can call many times that recalculates the participation role
	private rebalanceParticipationDebounced:
		| ReturnType<typeof debounce>
		| undefined;

	// regular distribution checks
	private distributeInterval: ReturnType<typeof setInterval>;
	private distributeQueue?: PQueue;

	// Syncing and dedeplucation work
	private syncMoreInterval?: ReturnType<typeof setTimeout>;
	private syncInFlightQueue: Map<string, PublicSignKey[]>;
	private syncInFlightQueueInverted: Map<string, Set<string>>;
	private syncInFlight: Map<string, Map<string, { timestamp: number }>>;

	replicas: ReplicationLimits;

	private cpuUsage?: CPUUsage;

	timeUntilRoleMaturity: number;
	waitForReplicatorTimeout: number;
	distributionDebounceTime: number;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.log = new Log(properties);
		this.rpc = new RPC();
	}

	/**
	 * Returns the current role
	 */
	get role(): Observer | Replicator {
		return this._role;
	}

	/**
	 * Return the
	 */
	get roleConfig(): Observer | Replicator | AdaptiveReplicatorOptions {
		return this._roleConfig;
	}

	get totalParticipation(): number {
		return this._totalParticipation;
	}

	private setupRebalanceDebounceFunction() {
		this.rebalanceParticipationDebounced = debounce(
			() => this.rebalanceParticipation(),
			Math.max(
				REBALANCE_DEBOUNCE_INTERVAL,
				Math.log(
					(this.getReplicatorsSorted()?.length || 0) *
						REBALANCE_DEBOUNCE_INTERVAL
				)
			)
		);
	}
	private setupRole(options?: RoleOptions) {
		this.rebalanceParticipationDebounced = undefined;

		const setupDebouncedRebalancing = (options?: AdaptiveReplicatorOptions) => {
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
											: options?.limits?.cpu
								}
							: undefined
				}
			);

			this.cpuUsage =
				options?.limits?.cpu && typeof options?.limits?.cpu === "object"
					? options?.limits?.cpu?.monitor || new CPUUsageIntervalLag()
					: new CPUUsageIntervalLag();
			this.cpuUsage?.start?.();

			this.setupRebalanceDebounceFunction();
		};

		if (options instanceof Observer || options instanceof Replicator) {
			throw new Error("Unsupported role option type");
		} else if (options === "observer") {
			this._roleConfig = new Observer();
		} else if (options === "replicator") {
			setupDebouncedRebalancing();
			this._roleConfig = { type: options };
		} else if (options) {
			if (options.type === "replicator") {
				if (isAdaptiveReplicatorOption(options)) {
					setupDebouncedRebalancing(options);
					this._roleConfig = options;
				} else {
					this._roleConfig = new Replicator({
						factor: options.factor,
						offset: this.getReplicationOffset()
					});
				}
			} else {
				this._roleConfig = new Observer();
			}
		} else {
			// Default option
			setupDebouncedRebalancing();
			this._roleConfig = { type: "replicator" };
		}

		// setup the initial role

		if (
			this._roleConfig instanceof Replicator ||
			this._roleConfig instanceof Observer
		) {
			this._role = this._roleConfig as Replicator | Observer;
		} else {
			// initial role in a dynamic setup

			if (this._roleConfig?.limits) {
				this._role = new Replicator({
					factor: this._role instanceof Replicator ? this._role.factor : 0,
					offset: this.getReplicationOffset()
				});
			} else {
				this._role = new Replicator({
					factor: this._role instanceof Replicator ? this._role.factor : 1,
					offset: this.getReplicationOffset()
				});
			}
		}

		return this._role;
	}

	async updateRole(role: RoleOptions, onRoleChange = true) {
		return this._updateRole(this.setupRole(role), onRoleChange);
	}

	private async _updateRole(
		role: Observer | Replicator = this._role,
		onRoleChange = true
	) {
		this._role = role;
		const { changed } = await this._modifyReplicators(
			this.role,
			this.node.identity.publicKey
		);

		await this.rpc.subscribe();
		await this.rpc.send(new ResponseRoleMessage({ role: this._role }), {
			priority: 1
		});

		if (onRoleChange && changed !== "none") {
			this.onRoleChange(this._role, this.node.identity.publicKey);
		}

		return changed;
	}

	async append(
		data: T,
		options?: SharedAppendOptions<T> | undefined
	): Promise<{
		entry: Entry<T>;
		removed: Entry<T>[];
	}> {
		const appendOptions: AppendOptions<T> = { ...options };
		const minReplicasData = encodeReplicas(
			options?.replicas
				? typeof options.replicas === "number"
					? new AbsoluteReplicas(options.replicas)
					: options.replicas
				: this.replicas.min
		);

		if (!appendOptions.meta) {
			appendOptions.meta = {
				data: minReplicasData
			};
		} else {
			appendOptions.meta.data = minReplicasData;
		}

		const result = await this.log.append(data, appendOptions);
		let mode: DeliveryMode | undefined = undefined;

		for (const message of await createExchangeHeadsMessages(
			this.log,
			[result.entry],
			this._gidParentCache
		)) {
			if (options?.target === "replicators" || !options?.target) {
				const minReplicas = decodeReplicas(result.entry).getValue(this);
				let leaders: string[] | Set<string> = await this.findLeaders(
					result.entry.meta.gid,
					minReplicas
				);
				const isLeader = leaders.includes(
					this.node.identity.publicKey.hashcode()
				);
				if (message.heads[0].gidRefrences.length > 0) {
					const newAndOldLeaders = new Set(leaders);
					for (const ref of message.heads[0].gidRefrences) {
						for (const hash of await this.findLeaders(ref, minReplicas)) {
							newAndOldLeaders.add(hash);
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
				mode
			});
		}
		this.rebalanceParticipationDebounced?.();

		return result;
	}

	async open(options?: Args<T>): Promise<void> {
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
				: undefined
		};

		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 10 * 1000; // TODO make into arg
		this._pendingDeletes = new Map();
		this._pendingIHave = new Map();
		this.latestRoleMessages = new Map();
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlight = new Map();
		this.openTime = +new Date();
		this.oldestOpenTime = this.openTime;
		this.distributionDebounceTime =
			options?.distributionDebounceTime || DEFAULT_DISTRIBUTION_DEBOUNCE_TIME;
		this.timeUntilRoleMaturity =
			options?.timeUntilRoleMaturity || WAIT_FOR_ROLE_MATURITY;
		this.waitForReplicatorTimeout =
			options?.waitForReplicatorTimeout || WAIT_FOR_REPLICATOR_TIMEOUT;
		this._gidParentCache = new Cache({ max: 1000 });
		this._closeController = new AbortController();
		this._canReplicate = options?.canReplicate;
		this.sync = options?.sync;
		this._logProperties = options;

		this.setupRole(options?.role);

		const id = sha256Base64Sync(this.log.id);
		const storage = await this.node.storage.sublevel(id);

		const localBlocks = await new AnyBlockStore(
			await storage.sublevel("blocks")
		);
		this.remoteBlocks = new RemoteBlocks({
			local: localBlocks,
			publish: (message, options) =>
				this.rpc.send(new BlocksMessage(message), {
					mode: options?.to
						? new SilentDelivery({ to: options.to, redundancy: 1 })
						: undefined
				}),
			waitFor: this.rpc.waitFor.bind(this.rpc)
		});

		await this.remoteBlocks.start();

		this._onSubscriptionFn = this._onSubscription.bind(this);
		this._totalParticipation = 0;
		this._sortedPeersCache = yallist.create();
		this._gidPeersHistory = new Map();

		const cache = await storage.sublevel("cache");

		await this.log.open(this.remoteBlocks, this.node.identity, {
			keychain: this.node.services.keychain,
			...this._logProperties,
			onChange: (change) => {
				for (const added of change.added) {
					this.onEntryAdded(added);
				}
				for (const removed of change.removed) {
					this.onEntryRemoved(removed.hash);
				}
				return this._logProperties?.onChange?.(change);
			},
			canAppend: async (entry) => {
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
				} catch (error) {
					if (
						error instanceof BorshError ||
						error instanceof ReplicationError
					) {
						logger.warn("Received payload that could not be decoded, skipping");
						return false;
					}
					throw error;
				}

				return this._logProperties?.canAppend?.(entry) ?? true;
			},
			trim: this._logProperties?.trim && {
				...this._logProperties?.trim
			},
			cache: cache
		});

		// Open for communcation
		await this.rpc.open({
			queryType: TransportMessage,
			responseType: TransportMessage,
			responseHandler: this._onMessage.bind(this),
			topic: this.topic
		});

		await this.node.services.pubsub.addEventListener(
			"subscribe",
			this._onSubscriptionFn
		);

		this._onUnsubscriptionFn = this._onUnsubscription.bind(this);
		await this.node.services.pubsub.addEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn
		);

		await this.log.load();

		// TODO (do better)
		// we do this distribution interval to eliminate the sideeffects arriving from updating roles and joining entries continously.
		// an alternative to this would be to call distribute/maybe prune after every join if our role has changed
		this.distributeInterval = setInterval(() => {
			this.distribute();
		}, 7.5 * 1000);

		const requestSync = () => {
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
				if (!this.log.has(key)) {
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
		requestSync();
	}

	async afterOpen(): Promise<void> {
		await super.afterOpen();

		// We do this here, because these calls requires this.closed == false
		await this._updateRole();
		await this.rebalanceParticipation();

		// Take into account existing subscription
		(await this.node.services.pubsub.getSubscribers(this.topic))?.forEach(
			(v, k) => {
				if (v.equals(this.node.identity.publicKey)) {
					return;
				}

				this.handleSubscriptionChange(v, [this.topic], true);
			}
		);
	}
	async getMemoryUsage() {
		return (
			((await this.log.memory?.size()) || 0) + (await this.log.blocks.size())
		);
	}

	get topic() {
		return this.log.idString;
	}

	private async _close() {
		clearTimeout(this.syncMoreInterval);
		clearInterval(this.distributeInterval);
		this.distributeQueue?.clear();

		this._closeController.abort();

		this.node.services.pubsub.removeEventListener(
			"subscribe",
			this._onSubscriptionFn
		);

		this._onUnsubscriptionFn = this._onUnsubscription.bind(this);
		this.node.services.pubsub.removeEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn
		);

		for (const [k, v] of this._pendingDeletes) {
			v.clear();
			v.promise.resolve(); // TODO or reject?
		}
		for (const [k, v] of this._pendingIHave) {
			v.clear();
		}

		await this.remoteBlocks.stop();
		this._gidParentCache.clear();
		this._pendingDeletes.clear();
		this._pendingIHave.clear();
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		this.latestRoleMessages.clear();
		this._gidPeersHistory.clear();

		this._sortedPeersCache = undefined;
		this.cpuUsage?.stop?.();
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
		context: RequestContext
	): Promise<TransportMessage | undefined> {
		try {
			if (!context.from) {
				throw new Error("Missing from in update role message");
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
					}, logId: ${this.log.idString}`
				);
				if (heads) {
					const filteredHeads: EntryWithRefs<any>[] = [];
					for (const head of heads) {
						if (!this.log.has(head.entry.hash)) {
							head.entry.init({
								// we need to init because we perhaps need to decrypt gid
								keychain: this.log.keychain,
								encoding: this.log.encoding
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
							const headsWithGid = this.log.headsIndex.gids.get(gid);

							const maxReplicasFromHead =
								headsWithGid && headsWithGid.size > 0
									? maxReplicas(this, [...headsWithGid.values()])
									: this.replicas.min.getValue(this);

							const maxReplicasFromNewEntries = maxReplicas(
								this,
								entries.map((x) => x.entry)
							);

							const leaders = await (this.role instanceof Observer
								? this.findLeaders(
										gid,
										Math.max(maxReplicasFromHead, maxReplicasFromNewEntries)
									)
								: this.waitForIsLeader(
										gid,
										Math.max(maxReplicasFromHead, maxReplicasFromNewEntries)
									));

							const isLeader = !!leaders;

							if (isLeader) {
								if (leaders.find((x) => x === context.from!.hashcode())) {
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
										const map = this.log.headsIndex.gids.get(ref);
										if (map && map.size > 0) {
											toMerge.push(entry.entry);
											(toDelete || (toDelete = [])).push(entry.entry);
											continue outer;
										}
									}
								}

								logger.debug(
									`${this.node.identity.publicKey.hashcode()}: Dropping heads with gid: ${
										entry.entry.gid
									}. Because not leader`
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
							const headsWithGid = this.log.headsIndex.gids.get(
								entries[0].entry.meta.gid
							);
							if (headsWithGid && headsWithGid.size > 0) {
								const minReplicas = maxReplicas(this, headsWithGid.values());

								const isLeader = await this.isLeader(
									entries[0].entry.meta.gid,
									minReplicas
								);

								if (!isLeader) {
									Promise.all(this.prune(entries.map((x) => x.entry))).catch(
										(e) => {
											logger.info(e.toString());
										}
									);
								}
							}
						}
					}
				}
			} else if (msg instanceof RequestIPrune) {
				const hasAndIsLeader: string[] = [];

				for (const hash of msg.hashes) {
					const indexedEntry = this.log.entryIndex.getShallow(hash);
					if (
						indexedEntry &&
						(await this.isLeader(
							indexedEntry.meta.gid,
							decodeReplicas(indexedEntry).getValue(this)
						))
					) {
						this._gidPeersHistory
							.get(indexedEntry.meta.gid)
							?.delete(context.from.hashcode());
						hasAndIsLeader.push(hash);
					} else {
						const prevPendingIHave = this._pendingIHave.get(hash);
						const pendingIHave = {
							clear: () => {
								clearTimeout(timeout);
								prevPendingIHave?.clear();
							},
							callback: async (entry) => {
								if (
									await this.isLeader(
										entry.meta.gid,
										decodeReplicas(entry).getValue(this)
									)
								) {
									this._gidPeersHistory
										.get(entry.meta.gid)
										?.delete(context.from!.hashcode());
									this.rpc.send(new ResponseIPrune({ hashes: [entry.hash] }), {
										mode: new SilentDelivery({
											to: [context.from!],
											redundancy: 1
										})
									});
								}

								prevPendingIHave && prevPendingIHave.callback(entry);
								this._pendingIHave.delete(entry.hash);
							}
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
					mode: new SilentDelivery({ to: [context.from], redundancy: 1 })
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
							context.from.hashcode()
						);
						if (!inverted) {
							inverted = new Set();
							this.syncInFlightQueueInverted.set(
								context.from.hashcode(),
								inverted
							);
						}
						inverted.add(hash);
					} else if (!this.log.has(hash)) {
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
					this._gidParentCache
				);
				// TODO perhaps send less messages to more receivers for performance reasons?
				// TODO wait for previous send to target before trying to send more?
				let p = Promise.resolve();
				for (const message of messages) {
					p = p.then(() =>
						this.rpc.send(message, {
							mode: new SilentDelivery({ to: [context.from!], redundancy: 1 })
						})
					); // push in series, if one fails, then we should just stop
				}
			} else if (msg instanceof BlocksMessage) {
				await this.remoteBlocks.onMessage(msg.message);
			} else if (msg instanceof RequestRoleMessage) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				await this.rpc.send(new ResponseRoleMessage({ role: this.role }), {
					mode: new SilentDelivery({ to: [context.from], redundancy: 1 })
				});
			} else if (msg instanceof ResponseRoleMessage) {
				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				// we have this statement because peers might have changed/announced their role,
				// but we don't know them as "subscribers" yet. i.e. they are not online
				this.waitFor(context.from, {
					signal: this._closeController.signal,
					timeout: this.waitForReplicatorTimeout
				})
					.then(async () => {
						// peer should not be online (for us)
						const prev = this.latestRoleMessages.get(context.from!.hashcode());
						if (prev && prev > context.timestamp) {
							return;
						}
						this.latestRoleMessages.set(
							context.from!.hashcode(),
							context.timestamp
						);
						await this.modifyReplicators(msg.role, context.from!);
					})
					.catch((e) => {
						if (e instanceof AbortError) {
							return;
						}
						if (e instanceof NotStartedError) {
							return;
						}
						logger.error(
							"Failed to find peer who updated their role: " + e?.message
						);
					});
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
						this.log.idString
					)}: Got message for a different namespace`
				);
				return;
			}

			if (e instanceof AccessError) {
				logger.trace(
					`${this.node.identity.publicKey.hashcode()}: Failed to handle message for log: ${JSON.stringify(
						this.log.idString
					)}: Do not have permissions`
				);
				return;
			}
			logger.error(e);
		}
	}

	getReplicatorsSorted(): yallist<ReplicatorRect> | undefined {
		return this._sortedPeersCache;
	}

	async waitForReplicator(...keys: PublicSignKey[]) {
		const check = () => {
			for (const k of keys) {
				const rect = this.getReplicatorsSorted()
					?.toArray()
					?.find((x) => x.publicKey.equals(k));
				if (
					!rect ||
					!isMatured(rect.role, +new Date(), this.getDefaultMinRoleAge())
				) {
					return false;
				}
			}
			return true;
		};
		return waitFor(() => check(), { signal: this._closeController.signal });
	}

	async isLeader(
		slot: { toString(): string },
		numberOfLeaders: number,
		options?: {
			candidates?: string[];
			roleAge?: number;
		}
	): Promise<boolean> {
		const isLeader = (
			await this.findLeaders(slot, numberOfLeaders, options)
		).find((l) => l === this.node.identity.publicKey.hashcode());
		return !!isLeader;
	}

	private getReplicationOffset() {
		return hashToUniformNumber(this.node.identity.publicKey.bytes);
	}

	private async waitForIsLeader(
		slot: { toString(): string },
		numberOfLeaders: number,
		timeout = this.waitForReplicatorTimeout
	): Promise<string[] | false> {
		return new Promise((res, rej) => {
			const removeListeners = () => {
				this.events.removeEventListener("role", roleListener);
				this._closeController.signal.addEventListener("abort", abortListener);
			};
			const abortListener = () => {
				removeListeners();
				clearTimeout(timer);
				res(false);
			};

			const timer = setTimeout(() => {
				removeListeners();
				res(false);
			}, timeout);

			const check = () =>
				this.findLeaders(slot, numberOfLeaders).then((leaders) => {
					const isLeader = leaders.find(
						(l) => l === this.node.identity.publicKey.hashcode()
					);
					if (isLeader) {
						removeListeners();
						clearTimeout(timer);
						res(leaders);
					}
				});

			const roleListener = () => {
				check();
			};
			this.events.addEventListener("role", roleListener);
			this._closeController.signal.addEventListener("abort", abortListener);

			check();
		});
	}

	async findLeaders(
		subject: { toString(): string },
		numberOfLeaders: number,
		options?: {
			roleAge?: number;
		}
	): Promise<string[]> {
		if (this.closed) {
			return [this.node.identity.publicKey.hashcode()]; // Assumption: if the store is closed, always assume we have responsibility over the data
		}

		// For a fixed set or members, the choosen leaders will always be the same (address invariant)
		// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies

		// Convert this thing we wan't to distribute to 8 bytes so we get can convert it into a u64
		// modulus into an index
		const utf8writer = new BinaryWriter();
		utf8writer.string(subject.toString());
		const seed = await sha256(utf8writer.finalize());

		// convert hash of slot to a number
		const cursor = hashToUniformNumber(seed); // bounded between 0 and 1
		return this.findLeadersFromUniformNumber(cursor, numberOfLeaders, options);
	}

	getDefaultMinRoleAge(): number {
		const now = +new Date();
		const replLength = this.getReplicatorsSorted()!.length;
		const diffToOldest =
			replLength > 1 ? now - this.oldestOpenTime - 1 : Number.MAX_SAFE_INTEGER;
		return Math.min(
			this.timeUntilRoleMaturity,
			diffToOldest,
			(this.timeUntilRoleMaturity * Math.log(replLength)) / 3
		); // / 3 so that if 2 replicators and timeUntilRoleMaturity = 1e4 the result will be 1
	}
	private findLeadersFromUniformNumber(
		cursor: number,
		numberOfLeaders: number,
		options?: {
			roleAge?: number;
		}
	) {
		const roleAge = options?.roleAge ?? this.getDefaultMinRoleAge(); // TODO -500 as is added so that i f someone else is just as new as us, then we treat them as mature as us. without -500 we might be slower syncing if two nodes starts almost at the same time

		const sampes = getSamples(
			cursor,
			this.getReplicatorsSorted()!,
			numberOfLeaders,
			roleAge,
			this.role instanceof Replicator && this.role.factor === 0
				? this.node.identity.publicKey.hashcode()
				: undefined
		);

		return sampes;
	}

	/**
	 *
	 * @returns groups where at least one in any group will have the entry you are looking for
	 */
	getReplicatorUnion(roleAge: number = this.getDefaultMinRoleAge()) {
		if (this.closed === true) {
			throw new Error("Closed");
		}

		// Total replication "width"
		const width = 1; //this.getParticipationSum(roleAge);

		// How much width you need to "query" to

		const peers = this.getReplicatorsSorted()!; // TODO types
		const minReplicas = Math.min(
			peers.length,
			this.replicas.min.getValue(this)
		);

		// If min replicas = 2
		// then we need to make sure we cover 0.5 of the total 'width' of the replication space
		// to make sure we reach sufficient amount of nodes such that at least one one has
		// the entry we are looking for
		const coveringWidth = width / minReplicas;

		const set = getCoverSet(
			coveringWidth,
			peers,
			roleAge,
			this.role instanceof Replicator ? this.node.identity.publicKey : undefined
		);

		// add all in flight
		for (const [key, _] of this.syncInFlight) {
			set.add(key);
		}
		return [...set];
	}

	async replicator(
		entry: Entry<any>,
		options?: {
			candidates?: string[];
			roleAge?: number;
		}
	) {
		return this.isLeader(
			entry.gid,
			decodeReplicas(entry).getValue(this),
			options
		);
	}

	private onRoleChange(role: Observer | Replicator, publicKey: PublicSignKey) {
		if (this.closed) {
			return;
		}

		this.distribute();

		if (role instanceof Replicator) {
			const timer = setTimeout(async () => {
				this._closeController.signal.removeEventListener("abort", listener);
				await this.rebalanceParticipationDebounced?.();
				this.distribute();
			}, this.getDefaultMinRoleAge() + 100);

			const listener = () => {
				clearTimeout(timer);
				this._closeController.signal.removeEventListener("abort", listener);
			};

			this._closeController.signal.addEventListener("abort", listener);
		}

		this.events.dispatchEvent(
			new CustomEvent<UpdateRoleEvent>("role", {
				detail: { publicKey, role }
			})
		);
	}

	private async modifyReplicators(
		role: Observer | Replicator,
		publicKey: PublicSignKey
	) {
		const update = await this._modifyReplicators(role, publicKey);
		if (update.changed !== "none") {
			if (update.changed === "added" || update.changed === "removed") {
				this.setupRebalanceDebounceFunction();
			}

			await this.rebalanceParticipationDebounced?.(); /* await this.rebalanceParticipation(false); */
			if (update.changed === "added") {
				// TODO this message can be redudant, only send this when necessary (see conditions when rebalanceParticipation sends messages)
				await this.rpc.send(new ResponseRoleMessage({ role: this._role }), {
					mode: new SilentDelivery({
						to: [publicKey.hashcode()],
						redundancy: 1
					}),
					priority: 1
				});
			}
			this.onRoleChange(role, publicKey);
			return true;
		}
		return false;
	}

	private async _modifyReplicators(
		role: Observer | Replicator,
		publicKey: PublicSignKey
	): Promise<
		| { changed: "added" | "none" }
		| { prev: Replicator; changed: "updated" | "removed" }
	> {
		// TODO can this call create race condition? _modifyReplicators might have to be queued
		// TODO should we remove replicators if they are already added?
		if (
			role instanceof Replicator &&
			this._canReplicate &&
			!(await this._canReplicate(publicKey, role))
		) {
			return { changed: "none" };
		}

		const sortedPeer = this._sortedPeersCache;
		if (!sortedPeer) {
			if (this.closed === false) {
				throw new Error("Unexpected, sortedPeersCache is undefined");
			}
			return { changed: "none" };
		}

		if (role instanceof Replicator) {
			// TODO use Set + list for fast lookup
			// check also that peer is online

			const isOnline =
				this.node.identity.publicKey.equals(publicKey) ||
				(await this.getReady()).has(publicKey.hashcode());
			if (!isOnline) {
				// TODO should we remove replicators if they are already added?
				return { changed: "none" };
			}
			this.oldestOpenTime = Math.min(
				this.oldestOpenTime,
				Number(role.timestamp)
			);

			// insert or if already there do nothing
			const rect: ReplicatorRect = {
				publicKey,
				role
			};

			let currentNode = sortedPeer.head;
			if (!currentNode) {
				sortedPeer.push(rect);
				this._totalParticipation += rect.role.factor;
				return { changed: "added" };
			} else {
				while (currentNode) {
					if (currentNode.value.publicKey.equals(publicKey)) {
						// update the value
						// rect.timestamp = currentNode.value.timestamp;
						const prev = currentNode.value;
						currentNode.value = rect;
						this._totalParticipation += rect.role.factor;
						this._totalParticipation -= prev.role.factor;
						// TODO change detection and only do change stuff if diff?
						return { prev: prev.role, changed: "updated" };
					}

					if (role.offset > currentNode.value.role.offset) {
						const next = currentNode?.next;
						if (next) {
							currentNode = next;
							continue;
						} else {
							break;
						}
					} else {
						currentNode = currentNode.prev;
						break;
					}
				}

				const prev = currentNode;
				if (!prev?.next?.value.publicKey.equals(publicKey)) {
					this._totalParticipation += rect.role.factor;
					_insertAfter(sortedPeer, prev || undefined, rect);
				} else {
					throw new Error("Unexpected");
				}
				return { changed: "added" };
			}
		} else {
			let currentNode = sortedPeer.head;
			while (currentNode) {
				if (currentNode.value.publicKey.equals(publicKey)) {
					sortedPeer.removeNode(currentNode);
					this._totalParticipation -= currentNode.value.role.factor;
					return { prev: currentNode.value.role, changed: "removed" };
				}
				currentNode = currentNode.next;
			}
			return { changed: "none" };
		}
	}

	async handleSubscriptionChange(
		publicKey: PublicSignKey,
		changes: string[],
		subscribed: boolean
	) {
		for (const topic of changes) {
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
				publicKey.hashcode()
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
			if (this.role instanceof Replicator) {
				this.rpc
					.send(new ResponseRoleMessage({ role: this._role }), {
						mode: new SilentDelivery({ redundancy: 1, to: [publicKey] })
					})
					.catch((e) => logger.error(e.toString()));
			}
		} else {
			await this.modifyReplicators(new Observer(), publicKey);
		}
	}

	prune(
		entries: Entry<any>[],
		options?: { timeout?: number; unchecked?: boolean }
	): Promise<any>[] {
		if (options?.unchecked) {
			return entries.map((x) => {
				this._gidPeersHistory.delete(x.meta.gid);
				return this.log.remove(x, {
					recursively: true
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
		const filteredEntries: Entry<any>[] = [];
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
				if (pending?.promise == deferredPromise) {
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
					reject(new Error("Timeout for checked pruning"));
				},
				options?.timeout ?? 10 * 1000
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

					const leaders = await this.findLeaders(
						entry.gid,
						minMinReplicasValue
					);

					if (
						leaders.find((x) => x === this.node.identity.publicKey.hashcode())
					) {
						reject(new Error("Failed to delete, is leader"));
						return;
					}

					if (leaders.find((x) => x === publicKeyHash)) {
						existCounter.add(publicKeyHash);
						if (minMinReplicasValue <= existCounter.size) {
							this._gidPeersHistory.delete(entry.meta.gid);
							this.log
								.remove(entry, {
									recursively: true
								})
								.then(() => {
									resolve();
								})
								.catch((e: any) => {
									reject(new Error("Failed to delete entry: " + e.toString()));
								});
						}
					}
				}
			});
			promises.push(deferredPromise.promise);
		}

		if (filteredEntries.length == 0) {
			return [];
		}

		this.rpc.send(
			new RequestIPrune({ hashes: filteredEntries.map((x) => x.hash) })
		);

		const onNewPeer = async (e: CustomEvent<UpdateRoleEvent>) => {
			if (e.detail.role instanceof Replicator) {
				await this.rpc.send(
					new RequestIPrune({ hashes: filteredEntries.map((x) => x.hash) }),
					{
						mode: new SilentDelivery({
							to: [e.detail.publicKey.hashcode()],
							redundancy: 1
						})
					}
				);
			}
		};

		// check joining peers
		this.events.addEventListener("role", onNewPeer);
		Promise.allSettled(promises).finally(() =>
			this.events.removeEventListener("role", onNewPeer)
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
					signal: this._closeController.signal
				}).then(() => this._distribute())
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
		const heads = await this.log.getHeads();
		const groupedByGid = await groupByGid(heads);
		const uncheckedDeliver: Map<string, Entry<any>[]> = new Map();
		const allEntriesToDelete: Entry<any>[] = [];

		for (const [gid, entries] of groupedByGid) {
			if (this.closed) {
				break;
			}

			if (entries.length === 0) {
				continue; // TODO maybe close store?
			}

			const oldPeersSet = this._gidPeersHistory.get(gid);
			const currentPeers = await this.findLeaders(
				gid,
				maxReplicas(this, entries) // pick max replication policy of all entries, so all information is treated equally important as the most important
			);

			const isLeader = currentPeers.find(
				(x) => x === this.node.identity.publicKey.hashcode()
			);
			const currentPeersSet = new Set(currentPeers);
			this._gidPeersHistory.set(gid, currentPeersSet);

			for (const currentPeer of currentPeers) {
				if (currentPeer == this.node.identity.publicKey.hashcode()) {
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
					let entriesToDelete =
						this._role instanceof Observer
							? entries.filter((e) => !e.createdLocally)
							: entries;

					if (this.sync) {
						entriesToDelete = entriesToDelete.filter(
							(entry) => this.sync!(entry) === false
						);
					}
					allEntriesToDelete.push(...entriesToDelete);
				}
			} else {
				for (const entry of entries) {
					this._pendingDeletes
						.get(entry.hash)
						?.reject(new Error("Failed to delete, is leader again"));
				}
			}
		}

		for (const [target, entries] of uncheckedDeliver) {
			this.rpc.send(
				new RequestMaybeSync({ hashes: entries.map((x) => x.hash) }),
				{
					mode: new SilentDelivery({ to: [target], redundancy: 1 })
				}
			);
		}

		if (allEntriesToDelete.length > 0) {
			Promise.allSettled(this.prune(allEntriesToDelete)).catch((e) => {
				logger.error(e.toString());
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
				hashes: hashes
			}),
			{
				mode: new SilentDelivery({ to, redundancy: 1 })
			}
		);
	}

	async _onUnsubscription(evt: CustomEvent<UnsubcriptionEvent>) {
		logger.debug(
			`Peer disconnected '${evt.detail.from.hashcode()}' from '${JSON.stringify(
				evt.detail.unsubscriptions.map((x) => x)
			)}'`
		);
		this.latestRoleMessages.delete(evt.detail.from.hashcode());

		this.events.dispatchEvent(
			new CustomEvent<UpdateRoleEvent>("role", {
				detail: { publicKey: evt.detail.from, role: new Observer() }
			})
		);

		return this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.unsubscriptions,
			false
		);
	}

	async _onSubscription(evt: CustomEvent<SubscriptionEvent>) {
		logger.debug(
			`New peer '${evt.detail.from.hashcode()}' connected to '${JSON.stringify(
				evt.detail.subscriptions.map((x) => x)
			)}'`
		);
		this.remoteBlocks.onReachable(evt.detail.from);

		return this.handleSubscriptionChange(
			evt.detail.from,
			evt.detail.subscriptions,
			true
		);
	}
	replicationController: PIDReplicationController;

	history: { usedMemory: number; factor: number }[];
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
		if (this._roleConfig instanceof Role) {
			return false;
		}

		// TODO second condition: what if the current role is Observer?
		if (
			this._roleConfig.type == "replicator" &&
			this._role instanceof Replicator
		) {
			const peers = this.getReplicatorsSorted();
			const usedMemory = await this.getMemoryUsage();

			const newFactor = this.replicationController.step({
				memoryUsage: usedMemory,
				currentFactor: this._role.factor,
				totalFactor: this._totalParticipation,
				peerCount: peers?.length || 1,
				cpuUsage: this.cpuUsage?.value()
			});

			const relativeDifference =
				Math.abs(this._role.factor - newFactor) / this._role.factor;

			if (relativeDifference > 0.0001) {
				const newRole = new Replicator({
					factor: newFactor,
					timestamp: this._role.timestamp,
					offset: hashToUniformNumber(this.node.identity.publicKey.bytes)
				});

				const canReplicate =
					!this._canReplicate ||
					(await this._canReplicate(this.node.identity.publicKey, newRole));
				if (!canReplicate) {
					return false;
				}

				await this._updateRole(newRole, onRoleChange);
				this.rebalanceParticipationDebounced?.();

				return true;
			} else {
				this.rebalanceParticipationDebounced?.();
			}
			return false;
		}
		return false;
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

function _insertAfter(
	self: yallist<any>,
	node: yallist.Node<ReplicatorRect> | undefined,
	value: ReplicatorRect
) {
	const inserted = !node
		? new yallist.Node(
				value,
				null as any,
				self.head as yallist.Node<ReplicatorRect> | undefined,
				self
			)
		: new yallist.Node(
				value,
				node,
				node.next as yallist.Node<ReplicatorRect> | undefined,
				self
			);

	// is tail
	if (inserted.next === null) {
		self.tail = inserted;
	}

	// is head
	if (inserted.prev === null) {
		self.head = inserted;
	}

	self.length++;
	return inserted;
}
