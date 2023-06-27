import { QueryContext, RPC } from "@peerbit/rpc";
import { TransportMessage } from "./message.js";
import {
	AppendOptions,
	Entry,
	Log,
	LogEvents,
	LogProperties,
} from "@peerbit/log";
import {
	AbstractProgram,
	Address,
	ComposableProgram,
	ProgramInitializationOptions,
} from "@peerbit/program";
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
	getKeypairFromPeerId,
	getPublicKeyFromPeerId,
	sha256,
	sha256Base64Sync,
} from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import {
	AbsolutMinReplicas,
	EntryWithRefs,
	ExchangeHeadsMessage,
	MinReplicas,
	createExchangeHeadsMessage,
} from "./exchange-heads.js";
import {
	Subscription,
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { startsWith } from "@dao-xyz/uint8arrays";
import { TimeoutError } from "@peerbit/time";
import {
	REPLICATOR_TYPE_VARIANT,
	Observer,
	Replicator,
	SubscriptionType,
} from "./role.js";
import { Peerbit } from "@peerbit/interface";
export { Observer, Replicator, SubscriptionType };

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

export interface SharedLogOptions {
	minReplicas?: number;
	sync?: SyncFilter;
	role?: SubscriptionType;
}

export const DEFAULT_MIN_REPLICAS = 2;

export type Args<T> = LogProperties<T> & LogEvents<T> & SharedLogOptions;
@variant("shared_log")
export class SharedLog<T> extends ComposableProgram<Args<T>> {
	@field({ type: Log })
	log: Log<T>;

	@field({ type: RPC })
	rpc: RPC<TransportMessage, TransportMessage>;

	// options
	private _minReplicas: MinReplicas;
	private _sync?: SyncFilter;
	private _role: SubscriptionType;

	private _sortedPeersCache: string[] | undefined;
	private _lastSubscriptionMessageId: number;
	private _gidPeersHistory: Map<string, Set<string>>;

	private _onSubscriptionFn: (arg: any) => any;
	private _onUnsubscriptionFn: (arg: any) => any;
	private _logProperties?: LogProperties<T> & LogEvents<T>;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.log = new Log(properties);
		this.rpc = new RPC();
	}

	get minReplicas() {
		return this._minReplicas;
	}

	set minReplicas(minReplicas: MinReplicas) {
		this._minReplicas = minReplicas;
	}
	get role(): SubscriptionType {
		return this._role;
	}

	async append(
		data: T,
		options?: AppendOptions<T> | undefined
	): Promise<{
		entry: Entry<T>;
		removed: Entry<T>[];
	}> {
		const result = await this.log.append(data, options);
		await this.rpc.send(
			await createExchangeHeadsMessage(this.log, [result.entry], true)
		);
		return result;
	}

	async open(options?: Args<T>): Promise<void> {
		this._minReplicas = new AbsolutMinReplicas(options?.minReplicas || 2);
		this._sync = options?.sync;
		this._role = options?.role || new Replicator();
		this._logProperties = options;

		this._lastSubscriptionMessageId = 0;
		this._onSubscriptionFn = this._onSubscription.bind(this);

		this._sortedPeersCache = [];
		this._gidPeersHistory = new Map();

		this.node.services.pubsub.addEventListener(
			"subscribe",
			this._onSubscriptionFn
		);

		this._onUnsubscriptionFn = this._onUnsubscription.bind(this);
		this.node.services.pubsub.addEventListener(
			"unsubscribe",
			this._onUnsubscriptionFn
		);

		await this.log.open(this.node.services.blocks, this.node.identity, {
			keychain: this.node.keychain,

			...this._logProperties,
			trim: this._logProperties?.trim && {
				...this._logProperties?.trim,
				filter: {
					canTrim: async (gid) => !(await this.isLeader(gid)), // TODO types
					cacheId: () => this._lastSubscriptionMessageId,
				},
			},
			cache:
				this.node.memory &&
				(await this.node.memory.sublevel(sha256Base64Sync(this.log.id))),
		});

		try {
			if (this._role instanceof Replicator) {
				this.modifySortedSubscriptionCache(
					true,
					getPublicKeyFromPeerId(this.node.peerId).hashcode()
				);
				await this.log.load();
			} else {
				await this.log.load({ heads: true, reload: true });
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

		// Take into account existing subscription
		this.node.services.pubsub.getSubscribers(this.topic)?.forEach((v, k) => {
			this.handleSubscriptionChange(
				k,
				[{ topic: this.topic, data: v.data }],
				true
			);
		});

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
		this._gidPeersHistory = new Map();
		this._sortedPeersCache = undefined;

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
	async close(from?: AbstractProgram): Promise<boolean> {
		const superClosed = await super.close(from);

		if (!superClosed) {
			return superClosed;
		}

		await this._close();
		await this.log.close();
		return superClosed;
	}

	async drop(): Promise<void> {
		await this._close();
		await this.log.drop();
		return super.drop();
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

					let toMerge: EntryWithRefs<any>[];
					if (!this._sync) {
						toMerge = [];
						for (const [gid, value] of await groupByGid(filteredHeads)) {
							if (!(await this.isLeader(gid, this._minReplicas.value))) {
								logger.debug(
									`${this.node.identity.publicKey.hashcode()}: Dropping heads with gid: ${gid}. Because not leader`
								);
								continue;
							}
							for (const head of value) {
								toMerge.push(head);
							}
						}
					} else {
						toMerge = await Promise.all(
							filteredHeads.map((x) => this._sync!(x.entry))
						).then((filter) => filteredHeads.filter((v, ix) => filter[ix]));
					}

					if (toMerge.length > 0) {
						await this.log.join(toMerge);
					}
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

	getReplicatorsSorted(): string[] | undefined {
		return this._sortedPeersCache;
	}

	async isLeader(
		slot: { toString(): string },
		numberOfLeaders: number = this.minReplicas.value
	): Promise<boolean> {
		const isLeader = (await this.findLeaders(slot, numberOfLeaders)).find(
			(l) => l === this.node.identity.publicKey.hashcode()
		);
		return !!isLeader;
	}

	async findLeaders(
		subject: { toString(): string },
		numberOfLeaders: number = this.minReplicas.value
	): Promise<string[]> {
		// For a fixed set or members, the choosen leaders will always be the same (address invariant)
		// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies
		const peers: string[] = this.getReplicatorsSorted() || [];

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
			leaders[i] = peers[(i + startIndex) % peers.length];
		}
		return leaders;
	}

	private modifySortedSubscriptionCache(subscribed: boolean, fromHash: string) {
		const sortedPeer = this._sortedPeersCache;
		if (!sortedPeer) {
			if (this.closed === false) {
				throw new Error("Unexpected, sortedPeersCache is undefined");
			}
			return;
		}
		const code = fromHash;
		if (subscribed) {
			// TODO use Set + list for fast lookup
			if (!sortedPeer.find((x) => x === code)) {
				sortedPeer.push(code);
				sortedPeer.sort((a, b) => a.localeCompare(b));
			}
		} else {
			const deleteIndex = sortedPeer.findIndex((x) => x === code);
			sortedPeer.splice(deleteIndex, 1);
		}
	}

	async handleSubscriptionChange(
		fromHash: string,
		changes: { topic: string; data?: Uint8Array }[],
		subscribed: boolean
	) {
		// TODO why are we doing two loops?
		for (const subscription of changes) {
			if (this.log.idString !== subscription.topic) {
				continue;
			}

			if (
				!subscription.data ||
				!startsWith(subscription.data, REPLICATOR_TYPE_VARIANT)
			) {
				continue;
			}
			this._lastSubscriptionMessageId += 1;
			this.modifySortedSubscriptionCache(subscribed, fromHash);
		}

		for (const subscription of changes) {
			if (this.log.idString !== subscription.topic) {
				continue;
			}
			if (subscription.data) {
				try {
					const type = deserialize(subscription.data, SubscriptionType);
					if (type instanceof Replicator) {
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
			const currentPeers = await this.findLeaders(gid);
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
					await this.log.remove(entriesToDelete, {
						recursively: true,
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

	replicators() {
		// TODO Optimize this so we don't have to recreate the array all the time!
		const minReplicas = this.minReplicas.value;
		const replicators = this.getReplicatorsSorted();
		if (!replicators) {
			return []; // No subscribers and we are not replicating
		}
		const numberOfGroups = Math.min(
			Math.ceil(replicators!.length / minReplicas)
		);
		const groups = new Array<string[]>(numberOfGroups);
		for (let i = 0; i < groups.length; i++) {
			groups[i] = [];
		}
		for (let i = 0; i < replicators!.length; i++) {
			groups[i % numberOfGroups].push(replicators![i]);
		}
		return groups;
	}
	async replicator(gid) {
		return this.isLeader(gid);
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
