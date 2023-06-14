import LazyLevel from "@dao-xyz/lazy-level";
import { AbstractLevel } from "abstract-level";
import { Level } from "level";
import { MemoryLevel } from "memory-level";
import { multiaddr, Multiaddr } from "@multiformats/multiaddr";
import type { Libp2p } from "libp2p";
import {
	createExchangeHeadsMessage,
	ExchangeHeadsMessage,
	AbsolutMinReplicas,
	EntryWithRefs,
	MinReplicas,
} from "./exchange-heads.js";
import { Entry, Log, LogOptions } from "@dao-xyz/peerbit-log";
import {
	serialize,
	deserialize,
	BorshError,
	BinaryReader,
	BinaryWriter,
} from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import {
	X25519PublicKey,
	AccessError,
	DecryptedThing,
	Ed25519Keypair,
	EncryptedThing,
	MaybeEncrypted,
	PublicKeyEncryptionResolver,
	Ed25519PublicKey,
	sha256,
	Identity,
} from "@dao-xyz/peerbit-crypto";
import { FastKeychain } from "./encryption.js";
import { MaybeSigned } from "@dao-xyz/peerbit-crypto";
import { Program, Address, LogCallbackOptions } from "@dao-xyz/peerbit-program";
import PQueue from "p-queue";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import {
	DirectSub,
	PubSubData,
	Subscription,
	SubscriptionEvent,
	UnsubcriptionEvent,
	waitForSubscribers,
} from "@dao-xyz/libp2p-direct-sub";
import sodium from "libsodium-wrappers";
import path from "path-browserify";
import { TimeoutError, waitFor } from "@dao-xyz/peerbit-time";
import "@libp2p/peer-id";
import { createLibp2pExtended, Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
import {
	OBSERVER_TYPE_VARIANT,
	Replicator,
	REPLICATOR_TYPE_VARIANT,
	SubscriptionType,
} from "@dao-xyz/peerbit-program";
import { TrimToByteLengthOption } from "@dao-xyz/peerbit-log";
import { TrimToLengthOption } from "@dao-xyz/peerbit-log";
import { startsWith } from "@dao-xyz/uint8arrays";
import { CreateOptions as ClientCreateOptions } from "@dao-xyz/peerbit-libp2p";
import { DirectBlock } from "@dao-xyz/libp2p-direct-block";
import { LevelDatastore } from "datastore-level";

export const logger = loggerFn({ module: "peer" });

const MIN_REPLICAS = 2;

interface ProgramWithMetadata {
	program: Program;
	openCounter: number;
}

interface LogWithMetaata {
	open: number;
	log: Log<any>;
	sync?: SyncFilter;
	minReplicas: MinReplicas;
}
export type OptionalCreateOptions = {
	limitSigning?: boolean;
	minReplicas?: number;
	refreshIntreval?: number;
	libp2pExternal?: boolean;
};
export type CreateOptions = {
	directory?: string;
	cache: LazyLevel;
	identity: Ed25519Keypair;
	keychain: FastKeychain;
} & OptionalCreateOptions;

export type SyncFilter = (entries: Entry<any>) => Promise<boolean> | boolean;
export type CreateInstanceOptions = {
	libp2p?: Libp2pExtended | ClientCreateOptions;
	directory?: string;
	cache?: LazyLevel;
} & OptionalCreateOptions;
export type OpenOptions = {
	entryToReplicate?: Entry<any>;
	role?: SubscriptionType;
	sync?: SyncFilter;
	timeout?: number;
	minReplicas?: MinReplicas | number;
	trim?: TrimToByteLengthOption | TrimToLengthOption;
	reset?: boolean;
} & { log?: LogCallbackOptions };

const isLibp2pInstance = (libp2p: Libp2pExtended | ClientCreateOptions) =>
	!!(libp2p as Libp2p).getMultiaddrs;

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

const createLevel = (path?: string): AbstractLevel<any, string, Uint8Array> => {
	return path
		? new Level(path, { valueEncoding: "view" })
		: new MemoryLevel({ valueEncoding: "view" });
};

const createCache = async (
	directory: string | undefined,
	options?: { reset?: boolean }
) => {
	const cache = await new LazyLevel(createLevel(directory));

	// "Wake up" the caches if they need it
	if (cache) await cache.open();
	if (options?.reset) {
		await cache._store.clear();
	}

	return cache;
};

const createSubCache = async (
	from: LazyLevel,
	name: string,
	options?: { reset?: boolean }
) => {
	const cache = await new LazyLevel(from._store.sublevel(name));

	// "Wake up" the caches if they need it
	if (cache) await cache.open();
	if (options?.reset) {
		await cache._store.clear();
	}

	return cache;
};

export class Peerbit {
	_libp2p: Libp2pExtended;

	directory?: string;
	_minReplicas: number;

	/// program address => Program metadata
	programs: Map<string, ProgramWithMetadata>;
	limitSigning: boolean;
	logs: Map<string, LogWithMetaata> = new Map();

	private _sortedPeersCache: Map<string, string[]> = new Map();
	private _gidPeersHistory: Map<string, Set<string>> = new Map();
	private _openProgramQueue: PQueue;
	private _disconnected = false;
	private _disconnecting = false;
	private _refreshInterval: any;
	private _lastSubscriptionMessageId = 0;
	private _cache: LazyLevel;
	private _libp2pExternal?: boolean = false;

	// Libp2p peerid in Identity form
	private _identityHash: string;
	private _identity: Ed25519Keypair;

	private _keychain: FastKeychain; // Keychain + Caching + X25519 keys

	constructor(libp2p: Libp2pExtended, options: CreateOptions) {
		if (libp2p == null) {
			throw new Error("Libp2p required");
		}
		this._libp2p = libp2p;
		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					this.libp2p.peerId.type
			);
		}

		if (this.libp2p.peerId.type !== "Ed25519") {
			throw new Error("Only Ed25519 peerIds are supported");
		}

		this._identity = options.identity;
		this._keychain = options.keychain;

		this._identityHash = this._identity.publicKey.hashcode();
		this.directory = options.directory;
		this.programs = new Map();
		this._minReplicas = options.minReplicas || MIN_REPLICAS;
		this.limitSigning = options.limitSigning || false;
		this._cache = options.cache;
		this._libp2pExternal = options.libp2pExternal;
		this._openProgramQueue = new PQueue({ concurrency: 1 });

		this.libp2p.services.pubsub.addEventListener(
			"data",
			this._onMessage.bind(this)
		);
		this.libp2p.services.pubsub.addEventListener(
			"subscribe",
			this._onSubscription.bind(this)
		);
		this.libp2p.services.pubsub.addEventListener(
			"unsubscribe",
			this._onUnsubscription.bind(this)
		);
	}

	static async create(options: CreateInstanceOptions = {}) {
		await sodium.ready; // Some of the modules depends on sodium to be readyy

		let libp2pExtended: Libp2pExtended = options.libp2p as Libp2pExtended;
		const blocksDirectory =
			options.directory != null
				? path.join(options.directory, "/blocks").toString()
				: undefined;
		let libp2pExternal = false;

		const datastore =
			options.directory != null
				? new LevelDatastore(path.join(options.directory, "/libp2p").toString())
				: undefined;
		if (datastore) {
			await datastore.open();
		}

		if (!libp2pExtended) {
			libp2pExtended = await createLibp2pExtended({
				services: {
					blocks: (c) => new DirectBlock(c, { directory: blocksDirectory }),
					pubsub: (c) => new DirectSub(c),
				},
				// If directory is passed, we store keys within that directory, else we will use memory datastore (which is the default behaviour)
				datastore,
			});
		} else {
			if (isLibp2pInstance(libp2pExtended)) {
				libp2pExternal = true; // libp2p was created outside
			} else {
				const extendedOptions = libp2pExtended as any as ClientCreateOptions;
				libp2pExtended = await createLibp2pExtended({
					...extendedOptions,
					services: {
						blocks: (c) => new DirectBlock(c, { directory: blocksDirectory }),
						pubsub: (c) => new DirectSub(c),
						...extendedOptions?.services,
					},
					datastore,
				});
			}
		}
		if (datastore) {
			const stopFn = libp2pExtended.stop.bind(libp2pExtended);
			libp2pExtended.stop = async () => {
				await stopFn();
				await datastore?.close();
			};
		}

		if (!libp2pExtended.isStarted()) {
			await libp2pExtended.start();
		}

		if (libp2pExtended.peerId.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " +
					libp2pExtended.peerId.type
			);
		}

		const directory = options.directory;
		const cache =
			options.cache ||
			(await createCache(
				directory ? path.join(directory, "/cache") : undefined
			));

		const identity = Ed25519Keypair.fromPeerId(libp2pExtended.peerId);
		const peer = new Peerbit(libp2pExtended, {
			directory,
			cache,
			libp2pExternal,
			limitSigning: options.limitSigning,
			minReplicas: options.minReplicas,
			refreshIntreval: options.refreshIntreval,
			identity,
			keychain: await FastKeychain.create(identity, libp2pExtended.keychain),
		});
		return peer;
	}
	get libp2p(): Libp2pExtended {
		return this._libp2p;
	}

	get cache() {
		return this._cache;
	}

	get encryption(): PublicKeyEncryptionResolver {
		return this._keychain;
	}

	get disconnected() {
		return this._disconnected;
	}

	get disconnecting() {
		return this._disconnecting;
	}

	get identityHash() {
		return this._identityHash;
	}

	get identity(): Ed25519Keypair {
		return this._identity;
	}

	async importKeypair(keypair: Ed25519Keypair) {
		return this._keychain.importKeypair(keypair);
	}

	async exportKeypair<T extends Ed25519PublicKey | X25519PublicKey>(
		publicKey: T
	) {
		return this._keychain.exportKeypair<T>(publicKey);
	}

	/**
	 * Dial a peer with an Ed25519 peerId
	 */
	async dial(address: string | Multiaddr | Multiaddr[] | Peerbit) {
		const maddress =
			typeof address == "string"
				? multiaddr(address)
				: address instanceof Peerbit
				? address.libp2p.getMultiaddrs()
				: address;
		const connection = await this.libp2p.dial(maddress);
		const publicKey = Ed25519PublicKey.fromPeerId(connection.remotePeer);

		// TODO, do this as a promise instead using the onPeerConnected vents in pubsub and blocks
		return waitFor(
			() =>
				this.libp2p.services.pubsub.peers.has(publicKey.hashcode()) &&
				this.libp2p.services.blocks.peers.has(publicKey.hashcode())
		);
	}

	async stop() {
		this._disconnecting = true;
		// Close a direct connection and remove it from internal state

		this._refreshInterval && clearInterval(this._refreshInterval);

		// Close all open databases
		await Promise.all(
			[...this.programs.values()].map((program) => program.program.close())
		);

		await this._cache.close();

		// Close libp2p (after above)
		if (!this._libp2pExternal) {
			// only close it if we created it
			await this.libp2p.stop();
		}

		// Remove all databases from the state
		this.programs = new Map();
		this._disconnecting = false;
		this._disconnected = true;
	}

	// Callback for local writes to the database. We the update to pubsub.
	onWrite<T>(_program: Program, log: Log<any>, entry: Entry<T>): void {
		// TODO Should we also do gidHashhistory update here?
		createExchangeHeadsMessage(
			log,
			[entry],
			true,
			this.limitSigning ? undefined : this.identity
		).then((bytes) => {
			this.libp2p.services.pubsub.publish(bytes, { topics: [log.idString] });
		});
	}

	_maybeOpenStorePromise: Promise<boolean>;
	// Callback for receiving a message from the network
	async _onMessage(evt: CustomEvent<PubSubData>) {
		const message = evt.detail;
		/* logger.debug(
			`${this.id}: Recieved message on topics: ${
				message.topics.length > 1
					? "#" + message.topics.length
					: message.topics[0]
			} ${message.data.length}`
		); */

		if (message.topics.find((x) => this.logs.has(x)) == null) {
			return; // not for me
		}

		if (this._disconnecting) {
			logger.warn("Got message while disconnecting");
			return;
		}

		if (this._disconnected) {
			if (!areWeTestingWithJest())
				throw new Error("Got message while disconnected");
			return; // because these could just be testing sideffects
		}

		try {
			/*   const peer =
				  message.type === "signed"
					  ? (message as SignedPubSubMessage).from
					  : undefined; */
			const maybeEncryptedMessage = deserialize(
				message.data,
				MaybeEncrypted
			) as MaybeEncrypted<MaybeSigned<TransportMessage>>;
			const decrypted = await maybeEncryptedMessage.decrypt(
				this.encryption.getAnyKeypair
			);
			const signedMessage = decrypted.getValue(MaybeSigned);
			await signedMessage.verify();
			const msg = signedMessage.getValue(TransportMessage);

			if (msg instanceof ExchangeHeadsMessage) {
				/**
				 * I have recieved heads from someone else.
				 * I can use them to load associated logs and join/sync them with the data stores I own
				 */

				const { logId } = msg;
				const { heads } = msg;
				// replication topic === trustedNetwork address

				const idString = Log.createIdString(logId);
				logger.debug(
					`${this.identity.publicKey.hashcode()}: Recieved heads: ${
						heads.length === 1 ? heads[0].entry.hash : "#" + heads.length
					}, logId: ${idString}`
				);
				if (heads) {
					const logInfo = this.logs.get(idString);
					if (!logInfo) {
						logger.error(
							"Missing log info, which was expected to exist for " + idString
						);
						return;
					}

					const filteredHeads = heads
						.filter((head) => !logInfo.log.has(head.entry.hash))
						.map((head) => {
							head.entry.init({
								encryption: logInfo.log.encryption,
								encoding: logInfo.log.encoding,
							});
							return head;
						}); // we need to init because we perhaps need to decrypt gid

					let toMerge: EntryWithRefs<any>[];
					if (!logInfo.sync) {
						toMerge = [];
						for (const [gid, value] of await groupByGid(filteredHeads)) {
							if (
								!(await this.isLeader(
									logInfo.log,
									gid,
									logInfo.minReplicas.value
								))
							) {
								logger.debug(
									`${this.identity.publicKey.hashcode()}: Dropping heads with gid: ${gid}. Because not leader`
								);
								continue;
							}
							for (const head of value) {
								toMerge.push(head);
							}
						}
					} else {
						toMerge = await Promise.all(
							filteredHeads.map((x) => logInfo.sync!(x.entry))
						).then((filter) => filteredHeads.filter((v, ix) => filter[ix]));
					}

					if (toMerge.length > 0) {
						await logInfo.log.join(toMerge);

						/*  TODO does this debug affect performance?
						
						logger.debug(
							`${this.id}: Synced ${toMerge.length} heads for '${programAddressObject}/${storeIndex}':\n`,
							JSON.stringify(
								toMerge.map((e) => e.entry.hash),
								null,
								2
							)
						); */
					}
				}
			} else {
				throw new Error("Unexpected message");
			}
		} catch (e: any) {
			if (e instanceof BorshError) {
				logger.trace(
					`${this.identity.publicKey.hashcode()}: Failed to handle message on topic: ${JSON.stringify(
						message.topics
					)} ${message.data.length}: Got message for a different namespace`
				);
				return;
			}
			if (e instanceof AccessError) {
				logger.trace(
					`${this.identity.publicKey.hashcode()}: Failed to handle message on topic: ${JSON.stringify(
						message.topics
					)} ${message.data.length}: Got message I could not decrypt`
				);
				return;
			}
			logger.error(e);
		}
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

	private modifySortedSubscriptionCache(
		topic: string,
		subscribed: boolean,
		fromHash: string
	) {
		const sortedPeer = this._sortedPeersCache.get(topic);
		if (sortedPeer) {
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
		} else if (subscribed) {
			this._sortedPeersCache.set(topic, [fromHash]);
		}
	}

	async handleSubscriptionChange(
		fromHash: string,
		changes: Subscription[],
		subscribed: boolean
	) {
		for (const c of changes) {
			if (!c.data || !startsWith(c.data, REPLICATOR_TYPE_VARIANT)) {
				return;
			}
			this._lastSubscriptionMessageId += 1;

			this.modifySortedSubscriptionCache(c.topic, subscribed, fromHash);
		}

		for (const subscription of changes) {
			if (subscription.data) {
				try {
					const type = deserialize(subscription.data, SubscriptionType);
					if (type instanceof Replicator) {
						const p = this.logs.get(subscription.topic);
						if (p) {
							await this.replicationReorganization([p.log.idString]);
						}
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
	async replicationReorganization(changedLogs: Set<string> | string[]) {
		let changed = false;
		for (const logId of changedLogs) {
			const logInfo = this.logs.get(logId);
			if (!logInfo || logInfo.log.closed) {
				continue;
			}

			const heads = await logInfo.log.getHeads();
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
					logInfo.log,
					gid,
					logInfo.minReplicas.value
				);
				for (const currentPeer of currentPeers) {
					if (
						!oldPeersSet?.has(currentPeer) &&
						currentPeer !== this.identityHash
					) {
						storeChanged = true;
						// second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send
						newPeers.push(currentPeer);

						// send heads to the new peer
						// console.log('new gid for peer', newPeers.length, this.id.toString(), newPeer, gid, entries.length, newPeers)
						try {
							logger.debug(
								`${this.identity.publicKey.hashcode()}: Exchange heads ${
									entries.length === 1 ? entries[0].hash : "#" + entries.length
								}  on rebalance`
							);
							for (const entry of entries) {
								toSend.set(entry.hash, entry);
							}
						} catch (error) {
							if (error instanceof TimeoutError) {
								logger.error(
									"Missing channel when reorg to peer: " +
										currentPeer.toString()
								);
								continue;
							}
							throw error;
						}
					}
				}

				// We don't need this clause anymore because we got the trim option!
				if (!currentPeers.find((x) => x === this.identityHash)) {
					let entriesToDelete = entries.filter((e) => !e.createdLocally);

					if (logInfo.sync) {
						// dont delete entries which we wish to keep
						entriesToDelete = await Promise.all(
							entriesToDelete.map((x) => logInfo.sync!(x))
						).then((filter) => entriesToDelete.filter((v, ix) => !filter[ix]));
					}

					// delete entries since we are not suppose to replicate this anymore
					// TODO add delay? freeze time? (to ensure resiliance for bad io)
					if (entriesToDelete.length > 0) {
						await logInfo.log.remove(entriesToDelete, {
							recursively: true,
						});
					}

					// TODO if length === 0 maybe close store?
				}
				this._gidPeersHistory.set(gid, new Set(currentPeers));

				if (toSend.size === 0) {
					continue;
				}
				const bytes = await createExchangeHeadsMessage(
					logInfo.log,
					[...toSend.values()], // TODO send to peers directly
					true,
					this.limitSigning ? undefined : this.identity
				);

				// TODO perhaps send less messages to more recievers for performance reasons?
				await this._libp2p.services.pubsub.publish(bytes, {
					to: newPeers,
					strict: true,
					topics: [logInfo.log.idString],
				});
			}
			if (storeChanged) {
				await logInfo.log.trim(); // because for entries createdLocally,we can have trim options that still allow us to delete them
			}
			changed = storeChanged || changed;
		}
		return changed;
	}

	/* TODO put this on the program level
	getCanTrust(address: Address): CanTrust | undefined {
		const p = this.programs.get(address.toString())?.program;
		if (p) {
			const ct = this.programs.get(address.toString())
				?.program as any as CanTrust;
			if (ct.isTrusted !== undefined) {
				return ct;
			}
		}
		return;
	} */

	// Callback when a store was closed
	async _onClose(program: Program, log: Log<any>) {
		// TODO Can we really close a this.programs, either we close all stores in the replication topic or none
		const programAddress = program.address?.toString();
		logger.debug(`Close ${programAddress}/${log.idString}`);

		const logid = log.idString;
		const lookup = this.logs.get(logid);
		if (lookup) {
			lookup.open -= 1;
			if (lookup.open === 0) {
				this.logs.delete(logid);
				await this.unsubscribeToProgram(log as Log<any>); // TODO unsubscribe with 1 role but maybe have another role left?
			}
		}
	}

	async _onProgamClose(program: Program, programCache: LazyLevel) {
		await programCache.close();
		const programAddress = program.address?.toString();
		if (programAddress) {
			this.programs.delete(programAddress);
		}
	}

	addProgram(program: Program): ProgramWithMetadata {
		const programAddress = program.address?.toString();
		if (!programAddress) {
			throw new Error("Missing program address");
		}
		const existingProgramAndStores = this.programs.get(programAddress);
		if (
			!!existingProgramAndStores &&
			existingProgramAndStores.program !== program
		) {
			// second condition only makes this throw error if we are to add a new instance with the same address
			throw new Error(`Program at ${programAddress} is already created`);
		}
		const p = {
			program,
			openCounter: 1,
			replicators: new Set<string>(),
		};

		this.programs.set(programAddress, p);
		return p;
	}

	/* getReplicators(log: Log<any>): string[] | undefined {
		let replicators = this.libp2p.services.pubsub.getSubscribersWithData(
			log.idString,
			REPLICATOR_TYPE_VARIANT,
			{ prefix: true }
		);

		const iAmReplicating = this._logsById.get(log.idString)?.log.replication.replicating; // TODO add conditional whether this represents a network (I am not replicating if I am not trusted (pointless))

		if (iAmReplicating) {
			replicators = replicators || [];
			replicators.push(this.idKeyHash.toString());
		}
		return replicators;
	} */

	getReplicatorsSorted(log: Log<any>): string[] | undefined {
		return this._sortedPeersCache.get(log.idString);
	}

	getObservers(address: Address): string[] | undefined {
		return this.libp2p.services.pubsub.getSubscribersWithData(
			address.toString(),
			OBSERVER_TYPE_VARIANT,
			{ prefix: true }
		);
	}

	async isLeader(
		log: Log<any>,
		slot: { toString(): string },
		numberOfLeaders: number
	): Promise<boolean> {
		const isLeader = (await this.findLeaders(log, slot, numberOfLeaders)).find(
			(l) => l === this.identityHash
		);
		return !!isLeader;
	}

	async findLeaders(
		log: Log<any>,
		subject: { toString(): string },
		numberOfLeaders: number
	): Promise<string[]> {
		// For a fixed set or members, the choosen leaders will always be the same (address invariant)
		// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies
		const peers: string[] = this.getReplicatorsSorted(log) || [];

		// Assumption: Network specification is accurate
		// Replication topic is not an address we assume that the network allows all participants
		/* TODO put this on the program level
		const network = this.getCanTrust(address);
		let peers: string[];
		if (network) {
			const isTrusted = (peer: string) =>
				network
					? network.isTrusted(
						peer // TODO improve perf, caching etc?
					)
					: true;
	
			peers = await Promise.all(peersPreFilter.map(isTrusted)).then((results) =>
				peersPreFilter.filter((_v, index) => results[index])
			);
		} else {
			peers = peersPreFilter;
		} */

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

	private async subscribeToProgram(
		log: Log<any>,
		role: SubscriptionType
	): Promise<void> {
		if (this._disconnected || this._disconnecting) {
			throw new Error("Disconnected");
		}

		if (role instanceof Replicator) {
			this.modifySortedSubscriptionCache(log.idString, true, this.identityHash);
		}

		this.libp2p.services.pubsub.subscribe(log.idString, {
			data: serialize(role),
		});

		return this.libp2p.services.pubsub.requestSubscribers(log.idString); // get up to date with who are subscribing to this topic
	}

	private async unsubscribeToProgram(id: Log<any>): Promise<void> {
		if (this._disconnected) {
			throw new Error("Disconnected");
		}
		this._sortedPeersCache.delete(id.idString);
		await this.libp2p.services.pubsub.unsubscribe(id.idString);
	}

	hasSubscribedToTopic(topic: string): boolean {
		return this.programs.has(topic);
	}

	/**
	 * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
	 * and to replicate heads (and updates) which is requested by another peer
	 * @param store
	 * @param options
	 * @returns
	 */

	async open<S extends Program>(
		storeOrAddress: /* string | Address |  */ S | Address | string,
		options: OpenOptions = {}
	): Promise<S> {
		if (this._disconnected || this._disconnecting) {
			throw new Error("Can not open a store while disconnected");
		}

		const fn = async (): Promise<ProgramWithMetadata> => {
			// TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?

			if (
				typeof storeOrAddress === "string" ||
				storeOrAddress instanceof Address
			) {
				storeOrAddress =
					storeOrAddress instanceof Address
						? storeOrAddress
						: Address.parse(storeOrAddress);
			}
			let program = storeOrAddress as S;
			let existing = false;
			if (
				storeOrAddress instanceof Address ||
				typeof storeOrAddress === "string"
			) {
				try {
					const fromExisting = this.programs?.get(storeOrAddress.toString())
						?.program as S;
					if (fromExisting) {
						program = fromExisting;
						existing = true;
					} else {
						program = (await Program.load(
							this._libp2p.services.blocks,
							storeOrAddress,
							options
						)) as S; // TODO fix typings
						if (program instanceof Program === false) {
							throw new Error(
								`Failed to open program because program is of type ${program?.constructor.name} and not ${Program.name}`
							);
						}
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " + storeOrAddress.toString()
					);
					throw error;
				}
			}

			if (!program.address && !existing) {
				await program.save(this._libp2p.services.blocks);
			}

			const programAddress = program.address!.toString()!;
			if (programAddress) {
				const existingProgram = this.programs?.get(programAddress);
				if (existingProgram) {
					existingProgram.openCounter += 1;
					return existingProgram;
				}
			}

			logger.debug(`Open database '${program.constructor.name}`);

			const role = options.role || new Replicator();

			const minReplicas =
				options.minReplicas != null
					? typeof options.minReplicas === "number"
						? new AbsolutMinReplicas(options.minReplicas)
						: options.minReplicas
					: new AbsolutMinReplicas(this._minReplicas);

			let programCache: LazyLevel | undefined = undefined;
			const resolveMinReplicas = (log: Log<any>) =>
				this.logs.get(log.idString)!.minReplicas.value;
			await program.init(this.libp2p, {
				onClose: async () => {
					return this._onProgamClose(program, programCache!);
				},
				onDrop: () => this._onProgamClose(program, programCache!),
				role,

				// If the program opens more programs
				open: (program) => this.open(program, options),
				onSave: async (address) => {
					programCache = await createSubCache(this._cache, address.toString(), {
						reset: options.reset,
					});
				},
				encryption: this.encryption,
				waitFor: async (other) => {
					await Promise.all(
						program.logs.map((x) =>
							waitForSubscribers(this.libp2p, other, x.idString)
						)
					);
				},
				log: (log) => {
					const cfg: LogOptions<any> = {
						encryption: this.encryption,
						trim: options.trim && {
							...options.trim,
							filter: {
								canTrim: async (gid) =>
									!(await this.isLeader(log, gid, resolveMinReplicas(log))), // TODO types
								cacheId: () => this._lastSubscriptionMessageId,
							},
						},
						cache: async (name: string) => {
							return createSubCache(
								programCache!, // TODO types
								path.join("log", name)
							);
						},
						onClose: async () => {
							await this._onClose(program, log);
							return options.log?.onClose?.(log);
						},
						onDrop: async () => {
							await this._onClose(program, log);
							return options.log?.onClose?.(log);
						},
						replication: {
							replicators: () => {
								// TODO Optimize this so we don't have to recreate the array all the time!
								const minReplicas = resolveMinReplicas(log);
								const replicators = this.getReplicatorsSorted(log);
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
							},
							replicator: (gid) =>
								this.isLeader(log, gid, resolveMinReplicas(log)),
						},
						onOpen: async () => {
							const logid = (log as Log<any>).idString;
							const lookup = this.logs.get(logid);
							if (lookup) {
								lookup.open += 1;
							} else {
								await this.subscribeToProgram(log as Log<any>, role);
								this.logs.set(logid, {
									log,
									open: 1,
									sync: options.sync,
									minReplicas,
								});
							}
						},
						onWrite: async (entry) => {
							await this.onWrite(program, log, entry);
							return options.log?.onWrite?.(log, entry);
						},

						onChange: async (change) => {
							return options?.log?.onChange?.(log, change);
						},
					};
					return cfg;
				},
			});
			return this.addProgram(program);
		};
		const openStore = await this._openProgramQueue.add(fn);
		if (!openStore?.program.address) {
			throw new Error("Unexpected");
		}
		return openStore.program as S;
	}
}

const areWeTestingWithJest = (): boolean => {
	return process.env.JEST_WORKER_ID !== undefined;
};
