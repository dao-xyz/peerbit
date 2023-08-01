import { QueryContext, RPC } from "@peerbit/rpc";
import { TransportMessage } from "./message.js";
import {
	AppendOptions,
	Entry,
	Log,
	LogEvents,
	LogProperties,
} from "@peerbit/log";
import { Program } from "@peerbit/program";
import {
	BinaryReader,
	BinaryWriter,
	BorshError,
	deserialize,
	field,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	AccessError,
	getPublicKeyFromPeerId,
	sha256,
	sha256Base64Sync,
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RequestHasEntries,
	ResponseHasEntries,
	createExchangeHeadsMessage,
} from "./exchange-heads.js";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { startsWith } from "@peerbit/uint8arrays";
import { TimeoutError } from "@peerbit/time";
import { REPLICATOR_TYPE_VARIANT, Observer, Replicator, Role } from "./role.js";
import {
	AbsoluteReplicas,
	MinReplicas,
	decodeReplicas,
	encodeReplicas,
	maxReplicas,
} from "./replication.js";
import pDefer, { DeferredPromise } from "p-defer";

export * from "./replication.js";
export { Observer, Replicator, Role };

export const logger = loggerFn({ module: "peer" });

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

export type SyncFilter = (entries: Entry<any>) => Promise<boolean> | boolean;

type ReplicationLimits = { min: MinReplicas; max?: MinReplicas };
export type ReplicationLimitsOptions =
	| Partial<ReplicationLimits>
	| { min?: number; max?: number };

export interface SharedLogOptions {
	replicas?: ReplicationLimitsOptions;
	sync?: SyncFilter;
	role?: Role;
}

export const DEFAULT_MIN_REPLICAS = 2;

export type Args<T> = LogProperties<T> & LogEvents<T> & SharedLogOptions;
export type SharedAppendOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
};

@variant("shared_log")
export class SharedLog<T = Uint8Array> extends Program<Args<T>> {
	@field({ type: Log })
	log: Log<T>;

	@field({ type: RPC })
	rpc: RPC<TransportMessage, TransportMessage>;

	// options
	private _sync?: SyncFilter;
	private _role: Observer | Replicator;

	private _sortedPeersCache: { hash: string; timestamp: number }[] | undefined;
	private _lastSubscriptionMessageId: number;
	private _gidPeersHistory: Map<string, Set<string>>;

	private _onSubscriptionFn: (arg: any) => any;
	private _onUnsubscriptionFn: (arg: any) => any;

	private _logProperties?: LogProperties<T> & LogEvents<T>;

	private _loadedOnce = false;

	private _pendingDeletes: Map<
		string,
		{
			promise: DeferredPromise<void>;
			clear: () => void;
			callback: (publicKeyHash: string) => Promise<void> | void;
		}
	>;

	private __pendingIHave: Map<string, () => void>;

	replicas: ReplicationLimits;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.log = new Log(properties);
		this.rpc = new RPC();
	}

	get role(): Observer | Replicator {
		return this._role;
	}

	async updateRole(role: Observer | Replicator) {
		const wasRepicators = this._role instanceof Replicator;
		this._role = role;
		await this.initializeWithRole();
		await this.rpc.subscribe(serialize(this._role));

		if (wasRepicators) {
			await this.replicationReorganization();
		}
	}

	private async initializeWithRole() {
		try {
			this.modifySortedSubscriptionCache(
				this._role instanceof Replicator ? true : false,
				getPublicKeyFromPeerId(this.node.peerId).hashcode()
			);

			if (!this._loadedOnce) {
				await this.log.load();
				this._loadedOnce = true;
			}
		} catch (error) {
			if (error instanceof AccessError) {
				logger.error(
					"Failed to load all entries due to access error, make sure you are opening the program with approate keychain configuration"
				);
			} else {
				throw error;
			}
		}
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
				data: minReplicasData,
			};
		} else {
			appendOptions.meta.data = minReplicasData;
		}

		const result = await this.log.append(data, appendOptions);

		await this.rpc.send(
			await createExchangeHeadsMessage(this.log, [result.entry], true)
		);
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
				: undefined,
		};

		this._pendingDeletes = new Map();
		this.__pendingIHave = new Map();

		this._sync = options?.sync;
		this._logProperties = options;
		this._role = options?.role || new Replicator();

		this._lastSubscriptionMessageId = 0;
		this._onSubscriptionFn = this._onSubscription.bind(this);

		this._sortedPeersCache = [];
		this._gidPeersHistory = new Map();

		await this.node.services.pubsub.addEventListener(
			"subscribe",
			this._onSubscriptionFn
		);

		this._onUnsubscriptionFn = this._onUnsubscription.bind(this);
		await this.node.services.pubsub.addEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn
		);

		await this.log.open(this.node.services.blocks, this.node.identity, {
			keychain: this.node.keychain,

			...this._logProperties,
			onChange: (change) => {
				if (this.__pendingIHave.size > 0) {
					for (const added of change.added) {
						this.__pendingIHave.get(added.hash)?.();
					}
				}
				return this._logProperties?.onChange?.(change);
			},
			canAppend: (entry) => {
				const replicas = decodeReplicas(entry).getValue(this);
				if (Number.isFinite(replicas) === false) {
					return false;
				}
				return this._logProperties?.canAppend?.(entry) ?? true;
			},
			trim: this._logProperties?.trim && {
				...this._logProperties?.trim,
				filter: {
					canTrim: async (entry) =>
						!(await this.isLeader(
							entry.meta.gid,
							decodeReplicas(entry).getValue(this)
						)), // TODO types
					cacheId: () => this._lastSubscriptionMessageId,
				},
			},
			cache:
				this.node.memory &&
				(await this.node.memory.sublevel(sha256Base64Sync(this.log.id))),
		});

		await this.initializeWithRole();

		// Take into account existing subscription
		(await this.node.services.pubsub.getSubscribers(this.topic))?.forEach(
			(v, k) => {
				this.handleSubscriptionChange(
					k,
					[{ topic: this.topic, data: v.data }],
					true
				);
			}
		);

		// Open for communcation
		await this.rpc.open({
			queryType: TransportMessage,
			responseType: TransportMessage,
			responseHandler: this._onMessage.bind(this),
			topic: this.topic,
			subscriptionData: serialize(this.role),
		});
	}

	get topic() {
		return this.log.idString;
	}

	private async _close() {
		for (const [k, v] of this._pendingDeletes) {
			v.clear();
			v.promise.resolve(); // TODO or reject?
		}
		this._pendingDeletes = new Map();
		this.__pendingIHave = new Map();

		this._gidPeersHistory = new Map();
		this._sortedPeersCache = undefined;
		this._loadedOnce = false;

		this.node.services.pubsub.removeEventListener(
			"subscribe",
			this._onSubscriptionFn
		);

		this._onUnsubscriptionFn = this._onUnsubscription.bind(this);
		this.node.services.pubsub.removeEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn
		);
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
		await this._close();
		await this.log.drop();
		return true;
	}

	// Callback for receiving a message from the network
	async _onMessage(
		msg: TransportMessage,
		context: QueryContext
	): Promise<TransportMessage | undefined> {
		try {
			if (msg instanceof ExchangeHeadsMessage) {
				/**
				 * I have recieved heads from someone else.
				 * I can use them to load associated logs and join/sync them with the data stores I own
				 */

				const { heads } = msg;
				// replication topic === trustedNetwork address

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
								encoding: this.log.encoding,
							});
							filteredHeads.push(head);
						}
					}

					if (!this._sync) {
						const toMerge: EntryWithRefs<any>[] = [];

						let toDelete: Entry<any>[] | undefined = undefined;
						let maybeDelete: EntryWithRefs<any>[][] | undefined = undefined;

						const groupedByGid = await groupByGid(filteredHeads);

						for (const [gid, entries] of groupedByGid) {
							const headsWithGid = this.log.headsIndex.gids.get(gid);
							const maxReplicasFromHead =
								headsWithGid && headsWithGid.size > 0
									? maxReplicas(this, [...headsWithGid.values()])
									: this.replicas.min.getValue(this);

							const maxReplicasFromNewEntries = maxReplicas(this, [
								...entries.map((x) => x.entry),
							]);

							const isLeader = await this.isLeader(
								gid,
								Math.max(maxReplicasFromHead, maxReplicasFromNewEntries)
							);

							if (maxReplicasFromNewEntries < maxReplicasFromHead && isLeader) {
								(maybeDelete || (maybeDelete = [])).push(entries);
							}

							outer: for (const entry of entries) {
								if (isLeader) {
									toMerge.push(entry);
								} else {
									for (const ref of entry.references) {
										const map = this.log.headsIndex.gids.get(
											await ref.getGid()
										);
										if (map && map.size > 0) {
											toMerge.push(entry);
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
						}

						await this.log.join(toMerge);
						toDelete &&
							Promise.all(this.pruneSafely(toDelete)).catch((e) => {
								logger.error(e.toString());
							});

						if (maybeDelete) {
							for (const entries of maybeDelete) {
								const headsWithGid = this.log.headsIndex.gids.get(
									entries[0].entry.meta.gid
								);
								if (headsWithGid && headsWithGid.size > 0) {
									const minReplicas = maxReplicas(this, [
										...headsWithGid.values(),
									]);

									const isLeader = await this.isLeader(
										entries[0].entry.meta.gid,
										minReplicas
									);
									if (!isLeader) {
										Promise.all(
											this.pruneSafely(entries.map((x) => x.entry))
										).catch((e) => {
											logger.error(e.toString());
										});
									}
								}
							}
						}
					} else {
						await this.log.join(
							await Promise.all(
								filteredHeads.map((x) => this._sync!(x.entry))
							).then((filter) => filteredHeads.filter((v, ix) => filter[ix]))
						);
					}
				}
			} else if (msg instanceof RequestHasEntries) {
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
						hasAndIsLeader.push(hash);
					} else {
						const pendingIHave = this.__pendingIHave.get(hash);

						this.__pendingIHave.set(hash, () => {
							pendingIHave && pendingIHave();
							this.rpc.send(new ResponseHasEntries({ hashes: [hash] }), {
								to: [context.from!],
							});
							this.__pendingIHave.delete(hash);
						});
					}
				}
				this.rpc.send(new ResponseHasEntries({ hashes: hasAndIsLeader }), {
					to: [context.from!],
				});
			} else if (msg instanceof ResponseHasEntries) {
				for (const hash of msg.hashes) {
					this._pendingDeletes.get(hash)?.callback(context.from!.hashcode());
				}
			} else {
				throw new Error("Unexpected message");
			}
		} catch (e: any) {
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

	getReplicatorsSorted(): { hash: string; timestamp: number }[] | undefined {
		return this._sortedPeersCache;
	}

	async isLeader(
		slot: { toString(): string },
		numberOfLeaders: number
	): Promise<boolean> {
		const isLeader = (await this.findLeaders(slot, numberOfLeaders)).find(
			(l) => l === this.node.identity.publicKey.hashcode()
		);
		return !!isLeader;
	}

	async findLeaders(
		subject: { toString(): string },
		numberOfLeadersUnbounded: number
	): Promise<string[]> {
		const lower = this.replicas.min.getValue(this);
		const higher = this.replicas.max?.getValue(this) ?? Number.MAX_SAFE_INTEGER;
		let numberOfLeaders = Math.max(
			Math.min(higher, numberOfLeadersUnbounded),
			lower
		);

		// For a fixed set or members, the choosen leaders will always be the same (address invariant)
		// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies
		const peers: { hash: string; timestamp: number }[] =
			this.getReplicatorsSorted() || [];

		if (peers.length === 0) {
			return [];
		}

		numberOfLeaders = Math.min(numberOfLeaders, peers.length);

		// Convert this thing we wan't to distribute to 8 bytes so we get can convert it into a u64
		// modulus into an index
		const utf8writer = new BinaryWriter();
		utf8writer.string(subject.toString());
		const seed = await sha256(utf8writer.finalize());

		// convert hash of slot to a number
		const seedNumber = new BinaryReader(
			seed.subarray(seed.length - 8, seed.length)
		).u64();
		const startIndex = Number(seedNumber % BigInt(peers.length));

		// we only step forward 1 step (ignoring that step backward 1 could be 'closer')
		// This does not matter, we only have to make sure all nodes running the code comes to somewhat the
		// same conclusion (are running the same leader selection algorithm)
		const leaders = new Array(numberOfLeaders);
		for (let i = 0; i < numberOfLeaders; i++) {
			leaders[i] = peers[(i + startIndex) % peers.length].hash;
		}
		return leaders;
	}

	private modifySortedSubscriptionCache(subscribed: boolean, fromHash: string) {
		const sortedPeer = this._sortedPeersCache;
		if (!sortedPeer) {
			if (this.closed === false) {
				throw new Error("Unexpected, sortedPeersCache is undefined");
			}
			return false;
		}
		const code = fromHash;
		if (subscribed) {
			// TODO use Set + list for fast lookup
			if (!sortedPeer.find((x) => x.hash === code)) {
				sortedPeer.push({ hash: code, timestamp: +new Date() });
				sortedPeer.sort((a, b) => a.hash.localeCompare(b.hash));
				return true;
			} else {
				return false;
			}
		} else {
			const deleteIndex = sortedPeer.findIndex((x) => x.hash === code);
			if (deleteIndex >= 0) {
				sortedPeer.splice(deleteIndex, 1);
				return true;
			} else {
				return false;
			}
		}
	}

	async handleSubscriptionChange(
		fromHash: string,
		changes: { topic: string; data?: Uint8Array }[],
		subscribed: boolean
	) {
		// TODO why are we doing two loops?
		const prev: boolean[] = [];
		for (const subscription of changes) {
			if (this.log.idString !== subscription.topic) {
				continue;
			}

			if (
				!subscription.data ||
				!startsWith(subscription.data, REPLICATOR_TYPE_VARIANT)
			) {
				prev.push(this.modifySortedSubscriptionCache(false, fromHash));
				continue;
			} else {
				this._lastSubscriptionMessageId += 1;
				prev.push(this.modifySortedSubscriptionCache(subscribed, fromHash));
			}
		}

		for (const [i, subscription] of changes.entries()) {
			if (this.log.idString !== subscription.topic) {
				continue;
			}
			if (subscription.data) {
				try {
					const type = deserialize(subscription.data, Role);

					// Reorganize if the new subscriber is a replicator, or observers AND was replicator
					if (type instanceof Replicator || prev[i]) {
						await this.replicationReorganization();
					}
				} catch (error: any) {
					logger.warn(
						"Recieved subscription with invalid data on topic: " +
							subscription.topic +
							". Error: " +
							error?.message
					);
				}
			}
		}
	}

	pruneSafely(entries: Entry<any>[], options?: { timeout: number }) {
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

			filteredEntries.push(entry);
			const existCounter = new Set<string>();
			const minReplicas = decodeReplicas(entry);
			const deferredPromise: DeferredPromise<void> = pDefer();

			const clear = () => {
				pendingPrev?.clear();
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

			const timeout = setTimeout(() => {
				reject(new Error("Timedout"));
			}, options?.timeout ?? 10 * 1000);

			this._pendingDeletes.set(entry.hash, {
				promise: deferredPromise,
				clear: () => {
					clear();
				},
				callback: async (publicKeyHash: string) => {
					const minReplicasValue = minReplicas.getValue(this);
					const l = await this.findLeaders(entry.gid, minReplicasValue);
					if (l.find((x) => x === publicKeyHash)) {
						existCounter.add(publicKeyHash);
						if (minReplicas.getValue(this) <= existCounter.size) {
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
		if (filteredEntries.length > 0) {
			this.rpc.send(
				new RequestHasEntries({ hashes: filteredEntries.map((x) => x.hash) })
			);
		}

		return promises;
	}

	/**
	 * When a peers join the networkk and want to participate the leaders for particular log subgraphs might change, hence some might start replicating, might some stop
	 * This method will go through my owned entries, and see whether I should share them with a new leader, and/or I should stop care about specific entries
	 * @param channel
	 */
	async replicationReorganization() {
		const changed = false;
		const heads = await this.log.getHeads();
		const groupedByGid = await groupByGid(heads);
		let storeChanged = false;
		for (const [gid, entries] of groupedByGid) {
			const toSend: Map<string, Entry<any>> = new Map();
			const newPeers: string[] = [];

			if (entries.length === 0) {
				continue; // TODO maybe close store?
			}

			const oldPeersSet = this._gidPeersHistory.get(gid);
			const currentPeers = await this.findLeaders(
				gid,
				maxReplicas(this, entries) // pick max replication policy of all entries, so all information is treated equally important as the most important
			);

			for (const currentPeer of currentPeers) {
				if (
					!oldPeersSet?.has(currentPeer) &&
					currentPeer !== this.node.identity.publicKey.hashcode()
				) {
					storeChanged = true;
					// second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send
					newPeers.push(currentPeer);

					// send heads to the new peer
					// console.log('new gid for peer', newPeers.length, this.id.toString(), newPeer, gid, entries.length, newPeers)
					try {
						logger.debug(
							`${this.node.identity.publicKey.hashcode()}: Exchange heads ${
								entries.length === 1 ? entries[0].hash : "#" + entries.length
							}  on rebalance`
						);
						for (const entry of entries) {
							toSend.set(entry.hash, entry);
						}
					} catch (error) {
						if (error instanceof TimeoutError) {
							logger.error(
								"Missing channel when reorg to peer: " + currentPeer.toString()
							);
							continue;
						}
						throw error;
					}
				}
			}

			// We don't need this clause anymore because we got the trim option!
			if (
				!currentPeers.find((x) => x === this.node.identity.publicKey.hashcode())
			) {
				let entriesToDelete = entries.filter((e) => !e.createdLocally);

				if (this._sync) {
					// dont delete entries which we wish to keep
					entriesToDelete = await Promise.all(
						entriesToDelete.map((x) => this._sync!(x))
					).then((filter) => entriesToDelete.filter((v, ix) => !filter[ix]));
				}

				// delete entries since we are not suppose to replicate this anymore
				// TODO add delay? freeze time? (to ensure resiliance for bad io)
				if (entriesToDelete.length > 0) {
					Promise.all(this.pruneSafely(entriesToDelete)).catch((e) => {
						logger.error(e.toString());
					});
				}

				// TODO if length === 0 maybe close store?
			}
			this._gidPeersHistory.set(gid, new Set(currentPeers));

			if (toSend.size === 0) {
				continue;
			}
			const message = await createExchangeHeadsMessage(
				this.log,
				[...toSend.values()], // TODO send to peers directly
				true
			);

			// TODO perhaps send less messages to more recievers for performance reasons?
			await this.rpc.send(message, {
				to: newPeers,
				strict: true,
			});
		}
		if (storeChanged) {
			await this.log.trim(); // because for entries createdLocally,we can have trim options that still allow us to delete them
		}
		return storeChanged || changed;
	}

	/**
	 *
	 * @returns groups where at least one in any group will have the entry you are looking for
	 */
	getDiscoveryGroups() {
		// TODO Optimize this so we don't have to recreate the array all the time!
		const minReplicas = this.replicas.min.getValue(this);
		const replicators = this.getReplicatorsSorted();
		if (!replicators) {
			return []; // No subscribers and we are not replicating
		}
		const numberOfGroups = Math.min(
			Math.ceil(replicators!.length / minReplicas)
		);
		const groups = new Array<{ hash: string; timestamp: number }[]>(
			numberOfGroups
		);
		for (let i = 0; i < groups.length; i++) {
			groups[i] = [];
		}
		for (let i = 0; i < replicators!.length; i++) {
			groups[i % numberOfGroups].push(replicators![i]);
		}

		return groups;
	}
	async replicator(entry: Entry<any>) {
		return this.isLeader(entry.gid, decodeReplicas(entry).getValue(this));
	}

	async _onUnsubscription(evt: CustomEvent<UnsubcriptionEvent>) {
		logger.debug(
			`Peer disconnected '${evt.detail.from.hashcode()}' from '${JSON.stringify(
				evt.detail.unsubscriptions.map((x) => x.topic)
			)}'`
		);

		return this.handleSubscriptionChange(
			evt.detail.from.hashcode(),
			evt.detail.unsubscriptions,
			false
		);
	}

	async _onSubscription(evt: CustomEvent<SubscriptionEvent>) {
		logger.debug(
			`New peer '${evt.detail.from.hashcode()}' connected to '${JSON.stringify(
				evt.detail.subscriptions.map((x) => x.topic)
			)}'`
		);
		return this.handleSubscriptionChange(
			evt.detail.from.hashcode(),
			evt.detail.subscriptions,
			true
		);
	}
}
