import { RequestContext, RPC } from "@peerbit/rpc";
import { TransportMessage } from "./message.js";
import {
	AppendOptions,
	Entry,
	Log,
	LogEvents,
	LogProperties
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
	RequestIHave,
	ResponseIHave,
	createExchangeHeadsMessage
} from "./exchange-heads.js";
import {
	SubscriptionEvent,
	UnsubcriptionEvent
} from "@peerbit/pubsub-interface";
import { AbortError, delay, TimeoutError, waitFor } from "@peerbit/time";
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
import { CustomEvent } from "@libp2p/interface/events";
import yallist from "yallist";
import { AcknowledgeDelivery, SilentDelivery } from "@peerbit/stream-interface";
import { AnyBlockStore, RemoteBlocks } from "@peerbit/blocks";
import { BlocksMessage } from "./blocks.js";

export * from "./replication.js";

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

export interface SharedLogOptions {
	replicas?: ReplicationLimitsOptions;
	role?: Role | number | { limits: { storage: number } };
	respondToIHaveTimeout?: number;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
}

export const DEFAULT_MIN_REPLICAS = 2;
export const WAIT_FOR_REPLICATOR_TIMEOUT = 9000;
export const WAIT_FOR_ROLE_MATURITY = 5000;

export type Args<T> = LogProperties<T> & LogEvents<T> & SharedLogOptions;
export type SharedAppendOptions<T> = AppendOptions<T> & {
	replicas?: AbsoluteReplicas | number;
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

	private _sortedPeersCache: yallist<ReplicatorRect> | undefined;
	private _lastSubscriptionMessageId: number;
	private _gidPeersHistory: Map<string, Set<string>>;

	private _onSubscriptionFn: (arg: any) => any;
	private _onUnsubscriptionFn: (arg: any) => any;

	private _canReplicate?: (
		publicKey: PublicSignKey,
		role: Replicator
	) => Promise<boolean> | boolean;

	private _logProperties?: LogProperties<T> & LogEvents<T>;
	private _closeController: AbortController;
	private _loadedOnce = false;
	private _gidParentCache: Cache<Entry<any>[]>;
	private _respondToIHaveTimeout;
	private _pendingDeletes: Map<
		string,
		{
			promise: DeferredPromise<void>;
			clear: () => void;
			callback: (publicKeyHash: string) => Promise<void> | void;
		}
	>;

	private _pendingIHave: Map<
		string,
		{ clear: () => void; callback: (entry: Entry<T>) => void }
	>;

	private latestRoleMessages: Map<string, bigint>;

	private fixedParticipationRate: boolean;

	private remoteBlocks: RemoteBlocks;

	replicas: ReplicationLimits;

	constructor(properties?: { id?: Uint8Array }) {
		super();
		this.log = new Log(properties);
		this.rpc = new RPC();
	}

	get role(): Observer | Replicator {
		return this._role;
	}

	async updateRole(role: Observer | Replicator, onRoleChange = true) {
		this._role = role;
		this._lastSubscriptionMessageId += 1;
		const { changed } = await this._modifyReplicators(
			this.role,
			this.node.identity.publicKey
		);
		if (!this._loadedOnce) {
			await this.log.load();
			this._loadedOnce = true;
		}
		await this.rpc.subscribe();
		await this.rpc.send(new ResponseRoleMessage(role));

		if (onRoleChange) {
			this.onRoleChange(undefined, this._role, this.node.identity.publicKey);
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
		const leaders = await this.findLeaders(
			result.entry.meta.gid,
			decodeReplicas(result.entry).getValue(this)
		);
		const isLeader = leaders.includes(this.node.identity.publicKey.hashcode());

		await this.rpc.send(
			await createExchangeHeadsMessage(
				this.log,
				[result.entry],
				this._gidParentCache
			),
			{
				to: leaders,
				strict: true,
				mode: isLeader ? new SilentDelivery(1) : new AcknowledgeDelivery(1)
			}
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
				: undefined
		};

		this._respondToIHaveTimeout = options?.respondToIHaveTimeout ?? 10 * 1000; // TODO make into arg
		this._pendingDeletes = new Map();
		this._pendingIHave = new Map();
		this.latestRoleMessages = new Map();

		this._gidParentCache = new Cache({ max: 1000 });
		this._closeController = new AbortController();

		this._canReplicate = options?.canReplicate;
		this._logProperties = options;

		let role: Role;
		if (options?.role) {
			role = options?.role;
			this.fixedParticipationRate = true;
		} else {
			role = new Replicator({ factor: 1, timestamp: BigInt(+new Date()) });
			this.fixedParticipationRate = false;
		}

		this._lastSubscriptionMessageId = 0;
		this._onSubscriptionFn = this._onSubscription.bind(this);
		this._sortedPeersCache = yallist.create();
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

		const id = sha256Base64Sync(this.log.id);
		const storage = await this.node.memory.sublevel(id);
		const localBlocks = await new AnyBlockStore(
			await storage.sublevel("blocks")
		);
		const cache = await storage.sublevel("cache");

		this.remoteBlocks = new RemoteBlocks({
			local: localBlocks,
			publish: (message, options) =>
				this.rpc.send(new BlocksMessage(message), {
					to: options?.to,
					strict: options?.to && true
				}),
			waitFor: this.rpc.waitFor.bind(this.rpc)
		});

		await this.remoteBlocks.start();

		await this.log.open(this.remoteBlocks, this.node.identity, {
			keychain: this.node.keychain,
			...this._logProperties,
			onChange: (change) => {
				if (this._pendingIHave.size > 0) {
					for (const added of change.added) {
						const ih = this._pendingIHave.get(added.hash);
						if (ih) {
							ih.clear();
							ih.callback(added);
						}
					}
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
				/* filter: {
					canTrim: async (entry) => {
						await this.warmUpPromise;
						const isLeader = (await this.isLeader(
							entry.meta.gid,
							maxReplicas(this, [entry])
						))
						if (!isLeader) {
							this.trims = (this.trims || 0) + 1;
						}
						else {
							this.ntrims = (this.ntrims || 0) + 1;
		
						}
						return !isLeader
					},
					cacheId: () => this._lastSubscriptionMessageId
				} */
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

		await this.updateRole(role);
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

	get topic() {
		return this.log.idString;
	}

	private async _close() {
		this._closeController.abort();

		for (const [k, v] of this._pendingDeletes) {
			v.clear();
			v.promise.resolve(); // TODO or reject?
		}
		for (const [k, v] of this._pendingIHave) {
			v.clear();
		}

		await this.remoteBlocks.stop();
		this._gidParentCache.clear();
		this._pendingDeletes = new Map();
		this._pendingIHave = new Map();
		this.latestRoleMessages.clear();

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

					const toMerge: EntryWithRefs<any>[] = [];
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

							const isLeader = await this.waitForIsLeader(
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
						};
						promises.push(fn());
					}
					await Promise.all(promises);

					if (toMerge.length > 0) {
						await this.log.join(toMerge);
						toDelete &&
							Promise.all(this.prune(toDelete)).catch((e) => {
								logger.error(e.toString());
							});
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
											logger.error(e.toString());
										}
									);
								}
							}
						}
					}
				}
			} else if (msg instanceof RequestIHave) {
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
									this.rpc.send(new ResponseIHave({ hashes: [entry.hash] }), {
										to: [context.from!]
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
				await this.rpc.send(new ResponseIHave({ hashes: hasAndIsLeader }), {
					to: [context.from!]
				});
			} else if (msg instanceof ResponseIHave) {
				for (const hash of msg.hashes) {
					this._pendingDeletes.get(hash)?.callback(context.from!.hashcode());
				}
			} else if (msg instanceof BlocksMessage) {
				await this.remoteBlocks.onMessage(msg.message);
			} else if (msg instanceof RequestRoleMessage) {
				if (!context.from) {
					throw new Error("Missing form in update role message");
				}

				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				await this.rpc.send(new ResponseRoleMessage({ role: this.role }), {
					to: [context.from!]
				});
			} else if (msg instanceof ResponseRoleMessage) {
				if (!context.from) {
					throw new Error("Missing form in update role message");
				}

				if (context.from.equals(this.node.identity.publicKey)) {
					return;
				}

				this.waitFor(context.from, {
					signal: this._closeController.signal,
					timeout: WAIT_FOR_REPLICATOR_TIMEOUT
				})
					.then(async () => {
						//await delay(4000 + Math.random())

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
				if (
					!this.getReplicatorsSorted()
						?.toArray()
						?.find((x) => x.publicKey.equals(k))
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

	private async waitForIsLeader(
		slot: { toString(): string },
		numberOfLeaders: number,
		timeout = WAIT_FOR_REPLICATOR_TIMEOUT
	): Promise<boolean> {
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
				this.isLeader(slot, numberOfLeaders).then((isLeader) => {
					if (isLeader) {
						removeListeners();
						clearTimeout(timer);
						res(isLeader);
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

	/* getParticipationSum(age: number) {
		let sum = 0;
		const t = +new Date;
		let currentNode = this.getReplicatorsSorted()?.head;
		while (currentNode) {

			const value = currentNode.value;
			if (t - value.timestamp > age || currentNode.value.publicKey.equals(this.node.identity.publicKey)) {
				sum += value.length;
			}
			if (sum >= 1) {
				return 1;
			}
			currentNode = currentNode.next
		}

		return sum;
	} */

	private findLeadersFromUniformNumber(
		cursor: number,
		numberOfLeaders: number,
		options?: {
			roleAge?: number;
		}
	) {
		const leaders: Set<string> = new Set();
		const width = 1; // this.getParticipationSum(roleAge);
		const peers = this.getReplicatorsSorted();
		if (!peers || peers?.length === 0) {
			return [];
		}
		numberOfLeaders = Math.min(numberOfLeaders, peers.length);

		const t = +new Date();
		const roleAge = options?.roleAge ?? WAIT_FOR_ROLE_MATURITY;

		for (let i = 0; i < numberOfLeaders; i++) {
			let matured = 0;
			const maybeIncrementMatured = (role: Replicator) => {
				if (t - Number(role.timestamp) > roleAge) {
					matured++;
				}
			};

			const x = ((cursor + i / numberOfLeaders) % 1) * width;
			let currentNode = peers.head;
			const diffs: { diff: number; rect: ReplicatorRect }[] = [];
			while (currentNode) {
				const start = currentNode.value.offset % width;
				const absDelta = Math.abs(start - x);
				const diff = Math.min(absDelta, width - absDelta);

				if (diff < currentNode.value.role.factor / 2 + 0.00001) {
					leaders.add(currentNode.value.publicKey.hashcode());
					maybeIncrementMatured(currentNode.value.role);
				} else {
					diffs.push({ diff, rect: currentNode.value });
				}

				currentNode = currentNode.next;
			}
			if (matured === 0) {
				diffs.sort((x, y) => x.diff - y.diff);
				for (const node of diffs) {
					leaders.add(node.rect.publicKey.hashcode());
					maybeIncrementMatured(node.rect.role);
					if (matured > 0) {
						break;
					}
				}
			}
		}

		return [...leaders];
	}

	/**
	 *
	 * @returns groups where at least one in any group will have the entry you are looking for
	 */
	getReplicatorUnion(roleAge: number = WAIT_FOR_ROLE_MATURITY) {
		// Total replication "width"
		const width = 1; //this.getParticipationSum(roleAge);

		// How much width you need to "query" to

		const peers = this.getReplicatorsSorted()!; // TODO types
		const minReplicas = Math.min(
			peers.length,
			this.replicas.min.getValue(this)
		);
		const coveringWidth = width / minReplicas;

		let walker = peers.head;
		if (this.role instanceof Replicator) {
			// start at our node (local first)
			while (walker) {
				if (walker.value.publicKey.equals(this.node.identity.publicKey)) {
					break;
				}
				walker = walker.next;
			}
		} else {
			const seed = Math.round(peers.length * Math.random()); // start at a random point
			for (let i = 0; i < seed - 1; i++) {
				if (walker?.next == null) {
					break;
				}
				walker = walker.next;
			}
		}

		const set: string[] = [];
		let distance = 0;
		const startNode = walker;
		if (!startNode) {
			return [];
		}

		let nextPoint = startNode.value.offset;
		const t = +new Date();
		while (walker && distance < coveringWidth) {
			const absDelta = Math.abs(walker!.value.offset - nextPoint);
			const diff = Math.min(absDelta, width - absDelta);

			if (diff < walker!.value.role.factor / 2 + 0.00001) {
				set.push(walker!.value.publicKey.hashcode());
				if (
					t - Number(walker!.value.role.timestamp) >
					roleAge /* ||
					walker!.value.publicKey.equals(this.node.identity.publicKey)) */
				) {
					nextPoint = (nextPoint + walker!.value.role.factor) % 1;
					distance += walker!.value.role.factor;
				}
			}

			walker = walker.next || peers.head;

			if (
				walker?.value.publicKey &&
				startNode?.value.publicKey.equals(walker?.value.publicKey)
			) {
				break; // TODO throw error for failing to fetch ffull width
			}
		}

		return set;
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

	private onRoleChange(
		prev: Observer | Replicator | undefined,
		role: Observer | Replicator,
		publicKey: PublicSignKey
	) {
		this._lastSubscriptionMessageId += 1;
		this.distribute();

		if (role instanceof Replicator) {
			const timer = setTimeout(() => {
				this._closeController.signal.removeEventListener("abort", listener);
				this.distribute();
			}, WAIT_FOR_ROLE_MATURITY + 2000);

			const listener = () => {
				clearTimeout(timer);
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
		const { prev, changed } = await this._modifyReplicators(role, publicKey);
		if (changed) {
			await this.rebalanceParticipation(false);
			this.onRoleChange(prev, role, publicKey);
			return true;
		}
		return false;
	}

	private async _modifyReplicators(
		role: Observer | Replicator,
		publicKey: PublicSignKey
	): Promise<{ prev?: Replicator; changed: boolean }> {
		if (
			role instanceof Replicator &&
			this._canReplicate &&
			!(await this._canReplicate(publicKey, role))
		) {
			return { changed: false };
		}

		const sortedPeer = this._sortedPeersCache;
		if (!sortedPeer) {
			if (this.closed === false) {
				throw new Error("Unexpected, sortedPeersCache is undefined");
			}
			return { changed: false };
		}

		if (role instanceof Replicator && role.factor > 0) {
			// TODO use Set + list for fast lookup
			// check also that peer is online

			const isOnline =
				this.node.identity.publicKey.equals(publicKey) ||
				(await this.waitFor(publicKey, { signal: this._closeController.signal })
					.then(() => true)
					.catch(() => false));

			if (isOnline) {
				// insert or if already there do nothing
				const code = hashToUniformNumber(publicKey.bytes);
				const rect: ReplicatorRect = {
					publicKey,
					offset: code,
					role
				};

				let currentNode = sortedPeer.head;
				if (!currentNode) {
					sortedPeer.push(rect);
					return { changed: true };
				} else {
					while (currentNode) {
						if (currentNode.value.publicKey.equals(publicKey)) {
							// update the value
							// rect.timestamp = currentNode.value.timestamp;
							const prev = currentNode.value;
							currentNode.value = rect;
							// TODO change detection and only do change stuff if diff?
							return { prev: prev.role, changed: true };
						}

						if (code > currentNode.value.offset) {
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
						_insertAfter(sortedPeer, prev || undefined, rect);
					} else {
						throw new Error("Unexpected");
					}
					return { changed: true };
				}
			} else {
				return { changed: false };
			}
		} else {
			let currentNode = sortedPeer.head;
			while (currentNode) {
				if (currentNode.value.publicKey.equals(publicKey)) {
					sortedPeer.removeNode(currentNode);
					return { prev: currentNode.value.role, changed: true };
				}
				currentNode = currentNode.next;
			}
			return { changed: false };
		}
	}

	async handleSubscriptionChange(
		publicKey: PublicSignKey,
		changes: string[],
		subscribed: boolean
	) {
		if (subscribed) {
			if (this.role instanceof Replicator) {
				for (const subscription of changes) {
					if (this.log.idString !== subscription) {
						continue;
					}
					this.rpc
						.send(new ResponseRoleMessage(this.role), {
							to: [publicKey],
							strict: true,
							mode: new AcknowledgeDelivery(1)
						})
						.catch((e) => logger.error(e.toString()));
				}
			}

			//if(evt.detail.subscriptions.map((x) => x.topic).includes())
		} else {
			for (const topic of changes) {
				if (this.log.idString !== topic) {
					continue;
				}

				await this.modifyReplicators(new Observer(), publicKey);
			}
		}
	}

	prune(
		entries: Entry<any>[],
		options?: { timeout?: number; unchecked?: boolean }
	) {
		if (options?.unchecked) {
			return entries.map((x) =>
				this.log.remove(x, {
					recursively: true
				})
			);
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
				callback: async (publicKeyHash: string) => {
					const minReplicasValue = minReplicas.getValue(this);
					const minMinReplicasValue = this.replicas.max
						? Math.min(minReplicasValue, this.replicas.max.getValue(this))
						: minReplicasValue;

					const l = await this.findLeaders(entry.gid, minMinReplicasValue);

					if (l.find((x) => x === this.node.identity.publicKey.hashcode())) {
						reject(new Error("Failed to delete, is leader"));
						return;
					}

					if (l.find((x) => x === publicKeyHash)) {
						existCounter.add(publicKeyHash);
						if (minMinReplicasValue <= existCounter.size) {
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
		if (filteredEntries.length > 0) {
			this.rpc.send(
				new RequestIHave({ hashes: filteredEntries.map((x) => x.hash) })
			);
		}
		return promises;
	}

	async distribute() {
		/**
		 * TODO use information of new joined/leaving peer to create a subset of heads
		 * that we potentially need to share with other peers
		 */
		const changed = false;
		await this.log.trim();
		const heads = await this.log.getHeads();
		const groupedByGid = await groupByGid(heads);
		const toDeliver: Map<string, Entry<any>[]> = new Map();
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
			const currentPeersSet = new Set(currentPeers);
			this._gidPeersHistory.set(gid, currentPeersSet);

			for (const currentPeer of currentPeers) {
				if (currentPeer == this.node.identity.publicKey.hashcode()) {
					continue;
				}

				if (!oldPeersSet?.has(currentPeer)) {
					// second condition means that if the new peer is us, we should not do anything, since we are expecting to receive heads, not send
					let arr = toDeliver.get(currentPeer);
					if (!arr) {
						arr = [];
						toDeliver.set(currentPeer, arr);
					}

					for (const entry of entries) {
						arr.push(entry);
					}
				}
			}

			if (
				!currentPeers.find((x) => x === this.node.identity.publicKey.hashcode())
			) {
				if (currentPeers.length > 0) {
					// If we are observer, never prune locally created entries, since we dont really know who can store them
					// if we are replicator, we will always persist entries that we need to so filtering on createdLocally will not make a difference
					const entriesToDelete =
						this._role instanceof Observer
							? entries.filter((e) => !e.createdLocally)
							: entries;
					entriesToDelete.map((x) => this._gidPeersHistory.delete(x.meta.gid));
					allEntriesToDelete.push(...entriesToDelete);
				}
			} else {
				for (const entry of entries) {
					this._pendingDeletes
						.get(entry.hash)
						?.promise.reject(
							new Error(
								"Failed to delete, is leader: " +
									this.role.constructor.name +
									". " +
									this.node.identity.publicKey.hashcode()
							)
						);
				}
			}
		}

		for (const [target, entries] of toDeliver) {
			const message = await createExchangeHeadsMessage(
				this.log,
				entries, // TODO send to peers directly
				this._gidParentCache
			);
			// TODO perhaps send less messages to more receivers for performance reasons?
			await this.rpc.send(message, {
				to: [target],
				strict: true
			});
		}

		this._lastSubscriptionMessageId += 1;

		if (allEntriesToDelete.length > 0) {
			Promise.all(this.prune(allEntriesToDelete)).catch((e) => {
				logger.error(e.toString());
			});
		}

		return changed;
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

	async rebalanceParticipation(onRoleChange = true) {
		// update more participation rate to converge to the average expected rate or bounded by
		// resources such as memory and or cpu
		if (this.fixedParticipationRate) {
			return false;
		}

		if (this._role instanceof Replicator) {
			const canReplicate =
				!this._canReplicate ||
				(await this._canReplicate(this.node.identity.publicKey, this._role));
			if (!canReplicate) {
				return false;
			}

			const peers = this.getReplicatorsSorted();
			const wantedFraction = 1 / peers!.length;
			const relativeDifference =
				Math.abs(this._role.factor - wantedFraction) / this._role.factor;

			if (relativeDifference > 0.0001) {
				await this.updateRole(
					new Replicator({ factor: wantedFraction }),
					onRoleChange
				);
				return true;
			}
		}
		return false;
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
