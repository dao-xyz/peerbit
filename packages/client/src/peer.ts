import LazyLevel from "@dao-xyz/lazy-level";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
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
import { Entry, Identity, Log, LogOptions } from "@dao-xyz/peerbit-log";
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
	X25519Keypair,
	Ed25519PublicKey,
	Ed25519PrivateKey,
	getKeypairFromPeerId,
	Sec256k1Keccak256Keypair,
	PreHash,
	sha256,
	PublicSignKey,
} from "@dao-xyz/peerbit-crypto";
import { encryptionWithRequestKey } from "./encryption.js";
import { MaybeSigned } from "@dao-xyz/peerbit-crypto";
import { Program, Address, LogCallbackOptions } from "@dao-xyz/peerbit-program";
import PQueue from "p-queue";
import type { Ed25519PeerId } from "@libp2p/interface-peer-id";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import {
	DirectSub,
	PubSubData,
	Subscription,
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@dao-xyz/libp2p-direct-sub";
import sodium from "libsodium-wrappers";
import path from "path-browserify";
import { TimeoutError, waitFor } from "@dao-xyz/peerbit-time";
import "@libp2p/peer-id";
import { createLibp2pExtended, Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
import {
	OBSERVER_TYPE_VARIANT,
	ReplicatorType,
	REPLICATOR_TYPE_VARIANT,
	SubscriptionType,
} from "@dao-xyz/peerbit-program";
import { TrimToByteLengthOption } from "@dao-xyz/peerbit-log";
import { TrimToLengthOption } from "@dao-xyz/peerbit-log";
import { startsWith } from "@dao-xyz/uint8arrays";
import { v4 as uuid } from "uuid";
import { CreateLibp2pExtendedOptions } from "@dao-xyz/peerbit-libp2p";
import {
	DirectBlock,
	LevelBlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
export const logger = loggerFn({ module: "peer" });

const MIN_REPLICAS = 2;

interface ProgramWithMetadata {
	program: Program;
	openCounter: number;
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
	keystore: Keystore;
	identity: Identity;
	directory?: string;
	peerId: Ed25519PeerId;
	cache: LazyLevel;
	localNetwork: boolean;
} & OptionalCreateOptions;

export type SyncFilter = (entries: Entry<any>) => Promise<boolean> | boolean;
export type CreateInstanceOptions = {
	libp2p?: Libp2pExtended | CreateLibp2pExtendedOptions;
	directory?: string;
	keystore?: Keystore;
	peerId?: Ed25519PeerId;
	identity?: Identity;
	cache?: LazyLevel;
	localNetwork?: boolean;
} & OptionalCreateOptions;
export type OpenOptions = {
	identity?: Identity;
	entryToReplicate?: Entry<any>;
	role?: SubscriptionType;
	sync?: SyncFilter;
	timeout?: number;
	minReplicas?: MinReplicas | number;
	trim?: TrimToByteLengthOption | TrimToLengthOption;
	reset?: boolean;
} & { log?: LogCallbackOptions };

const isLibp2pInstance = (
	libp2p: Libp2pExtended | CreateLibp2pExtendedOptions
) => !!(libp2p as Libp2p).getMultiaddrs;

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

	// User id
	identity: Identity;

	// Node id
	id: Ed25519PeerId;
	idKey: Ed25519Keypair;
	idKeyHash: string;
	idIdentity: Identity;

	directory?: string;
	keystore: Keystore;
	_minReplicas: number;
	/// program address => Program metadata
	programs: Map<string, ProgramWithMetadata>;
	limitSigning: boolean;
	localNetwork: boolean;

	private _sortedPeersCache: Map<string, string[]> = new Map();
	private _gidPeersHistory: Map<string, Set<string>> = new Map();
	private _openProgramQueue: PQueue;
	private _disconnected = false;
	private _disconnecting = false;
	private _encryption: PublicKeyEncryptionResolver;
	private _refreshInterval: any;
	private _lastSubscriptionMessageId = "";
	private _cache: LazyLevel;
	private _libp2pExternal?: boolean = false;

	constructor(
		libp2p: Libp2pExtended,
		identity: Identity,
		options: CreateOptions
	) {
		if (libp2p == null) {
			throw new Error("Libp2p required");
		}
		if (identity == null) {
			throw new Error("identity key required");
		}

		this._libp2p = libp2p;
		this.identity = identity;
		this.id = options.peerId;

		if (this.id.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " + this.id.type
			);
		}

		if (!this.id.privateKey) {
			throw new Error("Expecting private key to be defined");
		}
		this.idKey = new Ed25519Keypair({
			privateKey: new Ed25519PrivateKey({
				privateKey: this.id.privateKey.slice(4),
			}),
			publicKey: new Ed25519PublicKey({
				publicKey: this.id.publicKey.slice(4),
			}),
		});
		this.idKeyHash = this.idKey.publicKey.hashcode();
		this.idIdentity = {
			...this.idKey,
			sign: (data) => this.idKey.sign(data, PreHash.SHA_256),
		};

		this.directory = options.directory;
		this.programs = new Map();
		this._minReplicas = options.minReplicas || MIN_REPLICAS;
		this.limitSigning = options.limitSigning || false;
		this.localNetwork = options.localNetwork;
		this._cache = options.cache;
		this._libp2pExternal = options.libp2pExternal;
		this.keystore = options.keystore;
		this._openProgramQueue = new PQueue({ concurrency: 1 });

		this.libp2p.services.directsub.addEventListener(
			"data",
			this._onMessage.bind(this)
		);
		this.libp2p.services.directsub.addEventListener(
			"subscribe",
			this._onSubscription.bind(this)
		);
		this.libp2p.services.directsub.addEventListener(
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
		if (!libp2pExtended) {
			libp2pExtended = await createLibp2pExtended({
				services: {
					directblock: (c) =>
						new DirectBlock(c, { directory: blocksDirectory }),
					directsub: (c) => new DirectSub(c),
				},
			});
		} else {
			if (isLibp2pInstance(libp2pExtended)) {
				libp2pExternal = true; // libp2p was created outside
			} else {
				const extendedOptions = (libp2pExtended as CreateLibp2pExtendedOptions)
					.libp2p;
				libp2pExtended = await createLibp2pExtended({
					services: {
						directblock:
							extendedOptions?.services?.directblock ||
							((c) => new DirectBlock(c, { directory: blocksDirectory })),
						directsub:
							extendedOptions?.services?.directsub || ((c) => new DirectSub(c)),
					},
				});
			}
		}
		if (!libp2pExtended.isStarted()) {
			await libp2pExtended.start();
		}

		const id: Ed25519PeerId = libp2pExtended.peerId as Ed25519PeerId;
		if (id.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " + id.type
			);
		}

		const directory = options.directory;

		let keystore: Keystore;
		if (options.keystore) {
			keystore = options.keystore;
		} else {
			const keyStorePath = directory
				? path.join(directory, "/keystore")
				: undefined;
			logger.debug(
				keyStorePath
					? "Creating keystore at path: " + keyStorePath
					: "Creating an in memory keystore"
			);
			keystore = new Keystore(createLevel(keyStorePath));
		}

		let identity: Identity;
		if (options.identity) {
			identity = options.identity;
		} else {
			const keypair = getKeypairFromPeerId(id);
			if (keypair instanceof Sec256k1Keccak256Keypair) {
				identity = {
					publicKey: keypair.publicKey,
					sign: (data) => keypair.sign(data),
				};
			} else {
				identity = {
					privateKey: keypair.privateKey,
					publicKey: keypair.publicKey,
					sign: (data) => keypair.sign(data),
				};
			}
		}

		const cache =
			options.cache ||
			(await createCache(
				directory ? path.join(directory, "/cache") : undefined
			));

		const peer = new Peerbit(libp2pExtended, identity, {
			peerId: id,
			keystore,
			identity,
			directory,
			cache,
			localNetwork: options.localNetwork || false,
			libp2pExternal,
			limitSigning: options.limitSigning,
			minReplicas: options.minReplicas,
			refreshIntreval: options.refreshIntreval,
		});
		await peer.getEncryption();
		return peer;
	}
	get libp2p(): Libp2pExtended {
		return this._libp2p;
	}

	get cache() {
		return this._cache;
	}

	get encryption() {
		if (!this._encryption) {
			throw new Error("Unexpected");
		}
		return this._encryption;
	}

	get disconnected() {
		return this._disconnected;
	}

	get disconnecting() {
		return this._disconnecting;
	}

	async getEncryption(): Promise<PublicKeyEncryptionResolver> {
		this._encryption = await encryptionWithRequestKey(
			this.identity,
			this.keystore
		);
		return this._encryption;
	}

	async decryptedSignedThing(
		data: Uint8Array,
		options?: {
			signWithPeerId: boolean;
		}
	): Promise<DecryptedThing<MaybeSigned<Uint8Array>>> {
		const signedMessage = await new MaybeSigned({ data }).sign(async (data) => {
			return options?.signWithPeerId
				? this.idKey.sign(data)
				: this.identity.sign(data);
		});
		return new DecryptedThing({
			data: serialize(signedMessage),
		});
	}

	async enryptedSignedThing(
		data: Uint8Array,
		reciever: X25519PublicKey,
		options?: {
			signWithPeerId: boolean;
		}
	): Promise<EncryptedThing<MaybeSigned<Uint8Array>>> {
		const signedMessage = await new MaybeSigned({ data }).sign(async (data) => {
			return options?.signWithPeerId
				? this.idKey.sign(data)
				: this.identity.sign(data);
		});
		return new DecryptedThing<MaybeSigned<Uint8Array>>({
			data: serialize(signedMessage),
		}).encrypt(this.encryption.getEncryptionKeypair, reciever);
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
		const publicKey = Ed25519PublicKey.from(connection.remotePeer);

		// TODO, do this as a promise instead using the onPeerConnected vents in directsub and directblock
		return waitFor(
			() =>
				this.libp2p.services.directsub.peers.has(publicKey.hashcode()) &&
				this.libp2p.services.directblock.peers.has(publicKey.hashcode())
		);
	}

	async disconnect() {
		this._disconnecting = true;
		// Close a direct connection and remove it from internal state

		this._refreshInterval && clearInterval(this._refreshInterval);

		// close keystore
		await this.keystore.close();

		// Close libp2p
		if (!this._libp2pExternal) {
			// only close it if we created it
			await this.libp2p.stop();
		}

		// Close all open databases
		await Promise.all(
			[...this.programs.values()].map((program) => program.program.close())
		);

		await this._cache.close();

		// Remove all databases from the state
		this.programs = new Map();
		this._disconnecting = false;
		this._disconnected = true;
	}

	// Alias for disconnect()
	async stop() {
		await this.disconnect();
	}

	// Callback for local writes to the database. We the update to pubsub.
	onWrite<T>(
		program: Program,
		log: Log<any>,
		entry: Entry<T>,
		address?: Address | string
	): void {
		const programAddress = (
			program.address || program.parentProgram.address
		).toString();
		const writeAddress = address?.toString() || programAddress;
		const storeInfo = this.programs
			.get(programAddress)
			?.program.allLogsMap.get(log.idString);

		if (!storeInfo) {
			throw new Error("Missing store info");
		}

		// TODO Should we also do gidHashhistory update here?
		createExchangeHeadsMessage(
			log,
			program,
			[entry],
			true,
			this.limitSigning ? undefined : this.idIdentity
		).then((bytes) => {
			this.libp2p.services.directsub.publish(bytes, { topics: [writeAddress] });
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

		if (message.topics.find((x) => this.programs.has(x)) == null) {
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

				const { logId, programAddress } = msg;
				const { heads } = msg;
				// replication topic === trustedNetwork address

				const programAddressObject = Address.parse(programAddress);
				const idString = Log.createIdString(logId);
				logger.debug(
					`${this.id}: Recieved heads: ${
						heads.length === 1 ? heads[0].entry.hash : "#" + heads.length
					}, logId: ${idString}`
				);
				if (heads) {
					const programInfo = this.programs.get(programAddress)!;

					if (!programInfo) {
						return;
					}

					const logInfo = programInfo.program.allLogsMap.get(idString);
					if (!logInfo) {
						logger.error(
							"Missing store info, which was expected to exist for " +
								programAddressObject +
								", " +
								idString
						);
						return;
					}

					const filteredHeads = heads
						.filter((head) => !logInfo.has(head.entry.hash))
						.map((head) => {
							head.entry.init({
								encryption: logInfo.encryption,
								encoding: logInfo.encoding,
							});
							return head;
						}); // we need to init because we perhaps need to decrypt gid

					let toMerge: EntryWithRefs<any>[];
					if (!programInfo.sync) {
						toMerge = [];
						for (const [gid, value] of await groupByGid(filteredHeads)) {
							if (
								!(await this.isLeader(
									programAddressObject,
									gid,
									programInfo.minReplicas.value
								))
							) {
								logger.debug(
									`${this.id}: Dropping heads with gid: ${gid}. Because not leader`
								);
								continue;
							}
							value.forEach((head) => {
								toMerge.push(head);
							});
						}
					} else {
						toMerge = await Promise.all(
							filteredHeads.map((x) => programInfo.sync!(x.entry))
						).then((filter) => filteredHeads.filter((v, ix) => filter[ix]));
					}

					if (toMerge.length > 0) {
						const store = programInfo.program.allLogsMap.get(idString);
						if (!store) {
							throw new Error("Unexpected, missing store on sync");
						}

						await store.join(toMerge);

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
					`${this.id}: Failed to handle message on topic: ${JSON.stringify(
						message.topics
					)} ${message.data.length}: Got message for a different namespace`
				);
				return;
			}
			if (e instanceof AccessError) {
				logger.trace(
					`${this.id}: Failed to handle message on topic: ${JSON.stringify(
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
			evt.detail.from,
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
			evt.detail.from,
			evt.detail.subscriptions,
			true
		);
	}

	async handleSubscriptionChange(
		from: PublicSignKey,
		changes: Subscription[],
		subscribed: boolean
	) {
		changes.forEach((c) => {
			if (!c.data || !startsWith(c.data, REPLICATOR_TYPE_VARIANT)) {
				return;
			}
			this._lastSubscriptionMessageId = uuid();

			const sortedPeer = this._sortedPeersCache.get(c.topic);
			if (sortedPeer) {
				const code = from.hashcode();
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
		});

		for (const subscription of changes) {
			if (subscription.data) {
				try {
					const type = deserialize(subscription.data, SubscriptionType);
					if (type instanceof ReplicatorType) {
						const p = this.programs.get(subscription.topic);
						if (p) {
							await this.replicationReorganization([
								p.program.address.toString(),
							]);
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
	async replicationReorganization(changedProgarms: Set<string> | string[]) {
		let changed = false;
		for (const address of changedProgarms) {
			const programInfo = this.programs.get(address);
			if (programInfo) {
				for (const [_, log] of programInfo.program.allLogsMap) {
					const heads = await log.getHeads();
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
							programInfo.program.address,
							gid,
							programInfo.minReplicas.value
						);
						for (const currentPeer of currentPeers) {
							if (
								!oldPeersSet?.has(currentPeer) &&
								currentPeer !== this.idKeyHash
							) {
								storeChanged = true;
								// second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send
								newPeers.push(currentPeer);

								// send heads to the new peer
								// console.log('new gid for peer', newPeers.length, this.id.toString(), newPeer, gid, entries.length, newPeers)
								try {
									logger.debug(
										`${this.id}: Exchange heads ${
											entries.length === 1
												? entries[0].hash
												: "#" + entries.length
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
						if (!currentPeers.find((x) => x === this.idKeyHash)) {
							let entriesToDelete = entries.filter((e) => !e.createdLocally);

							if (programInfo.sync) {
								// dont delete entries which we wish to keep
								entriesToDelete = await Promise.all(
									entriesToDelete.map((x) => programInfo.sync!(x))
								).then((filter) =>
									entriesToDelete.filter((v, ix) => !filter[ix])
								);
							}

							// delete entries since we are not suppose to replicate this anymore
							// TODO add delay? freeze time? (to ensure resiliance for bad io)
							if (entriesToDelete.length > 0) {
								await log.remove(entriesToDelete, {
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
							log,
							programInfo.program,
							[...toSend.values()], // TODO send to peers directly
							true,
							this.limitSigning ? undefined : this.idIdentity
						);

						// TODO perhaps send less messages to more recievers for performance reasons?
						await this._libp2p.services.directsub.publish(bytes, {
							to: newPeers,
							strict: true,
							topics: [address],
						});
					}
					if (storeChanged) {
						await log.trim(); // because for entries createdLocally,we can have trim options that still allow us to delete them
					}
					changed = storeChanged || changed;
				}
			}
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
	}

	async _onProgamClose(program: Program, programCache: LazyLevel) {
		await programCache.close();
		const programAddress = program.address?.toString();
		if (programAddress) {
			const deleted = this.programs.delete(programAddress);
			if (deleted) {
				this.libp2p.services.directsub.unsubscribe(programAddress);
			}
		}
		this._sortedPeersCache.delete(programAddress);
	}

	addProgram(
		program: Program,
		minReplicas: MinReplicas,
		sync?: SyncFilter
	): ProgramWithMetadata {
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
			minReplicas,
			sync,
			openCounter: 1,
			replicators: new Set<string>(),
		};

		this.programs.set(programAddress, p);
		return p;
	}

	/**
	 * An intentionally imperfect leader rotation routine
	 * @param slot, some time measure
	 * @returns
	 */

	getReplicators(address: string): string[] | undefined {
		let replicators = this.libp2p.services.directsub.getSubscribersWithData(
			address,
			REPLICATOR_TYPE_VARIANT,
			{ prefix: true }
		);

		const iAmReplicating =
			this.programs.get(address.toString())?.program?.role instanceof
			ReplicatorType; // TODO add conditional whether this represents a network (I am not replicating if I am not trusted (pointless))

		if (iAmReplicating) {
			replicators = replicators || [];
			replicators.push(this.idKeyHash.toString());
		}
		return replicators;
	}

	getReplicatorsSorted(address: string): string[] | undefined {
		const replicators = this._sortedPeersCache.get(address);
		if (replicators) {
			return replicators;
		}
		throw new Error(
			"Missing sorted peer list of address: " +
				address +
				". Unexpected error. " +
				this.programs.get(address)?.program
		);
	}

	getObservers(address: Address): string[] | undefined {
		return this.libp2p.services.directsub.getSubscribersWithData(
			address.toString(),
			OBSERVER_TYPE_VARIANT,
			{ prefix: true }
		);
	}

	async isLeader(
		address: Address,
		slot: { toString(): string },
		numberOfLeaders: number
	): Promise<boolean> {
		const isLeader = (
			await this.findLeaders(address, slot, numberOfLeaders)
		).find((l) => l === this.idKeyHash);
		return !!isLeader;
	}

	async findLeaders(
		address: Address,
		subject: { toString(): string },
		numberOfLeaders: number
	): Promise<string[]> {
		// For a fixed set or members, the choosen leaders will always be the same (address invariant)
		// This allows for that same content is always chosen to be distributed to same peers, to remove unecessary copies
		const peers: string[] = this.getReplicatorsSorted(address.toString()) || [];

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
		address: string | Address,
		role: SubscriptionType
	): Promise<void> {
		if (this._disconnected || this._disconnecting) {
			throw new Error("Disconnected");
		}
		const topic = typeof address === "string" ? address : address.toString();
		this.libp2p.services.directsub.subscribe(topic, {
			data: serialize(role),
		});
		await this.libp2p.services.directsub.requestSubscribers(topic); // get up to date with who are subscribing to this topic
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

				if (storeOrAddress.path) {
					throw new Error(
						"Opening programs by subprogram addresses is currently unsupported"
					);
				}
			}
			let program = storeOrAddress as S;

			if (
				storeOrAddress instanceof Address ||
				typeof storeOrAddress === "string"
			) {
				try {
					program = (await Program.load(
						this._libp2p.services.directblock,
						storeOrAddress,
						options
					)) as S; // TODO fix typings
					if (program instanceof Program === false) {
						throw new Error(
							`Failed to open program because program is of type ${program?.constructor.name} and not ${Program.name}`
						);
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " + storeOrAddress.toString()
					);
					throw error;
				}
			}

			await program.save(this._libp2p.services.directblock);
			const programAddress = program.address!.toString()!;

			if (programAddress) {
				const existingProgram = this.programs?.get(programAddress);
				if (existingProgram) {
					existingProgram.openCounter += 1;
					return existingProgram;
				}
			}

			logger.debug(`Open database '${program.constructor.name}`);

			const encryption =
				/* 	options.log?.encryption ||  TODO  */
				await encryptionWithRequestKey(this.identity, this.keystore);
			const role = options.role || new ReplicatorType();

			const minReplicas =
				options.minReplicas != null
					? typeof options.minReplicas === "number"
						? new AbsolutMinReplicas(options.minReplicas)
						: options.minReplicas
					: new AbsolutMinReplicas(this._minReplicas);

			const resolveMinReplicas = () => {
				const value = this.programs.get(program.address.toString())?.minReplicas
					.value;
				if (value == null) {
					throw new Error("Missing minReplica value, unexpected");
				}
				return value;
			};

			let programCache: LazyLevel | undefined = undefined;
			await program.init(this.libp2p, options.identity || this.identity, {
				onClose: async () => {
					return this._onProgamClose(program, programCache!);
				},
				onDrop: () => this._onProgamClose(program, programCache!),
				role,
				replicators: () => {
					// TODO Optimize this so we don't have to recreate the array all the time!
					const minReplicas = resolveMinReplicas();
					const replicators = this.getReplicatorsSorted(
						program.address.toString()
					);
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
				// If the program opens more programs
				open: (program) => this.open(program, options),
				onSave: async (address) => {
					programCache = await createSubCache(this._cache, address.toString(), {
						reset: options.reset,
					});
				},
				replicator: (gid) =>
					this.isLeader(program.address, gid, resolveMinReplicas()),

				log: {
					encryption,
					trim: options.trim && {
						...options.trim,
						filter: {
							canTrim: async (gid) =>
								!(await this.isLeader(
									program.address,
									gid,
									resolveMinReplicas()
								)),
							cacheId: () => this._lastSubscriptionMessageId,
						},
					},
					cache: async (name: string) => {
						return createSubCache(
							programCache!, // TODO types
							path.join("log", name)
						);
					},
					onClose: async (log) => {
						//	CLOSED IS NOT CALLED ON DROP!
						await this._onClose(program, log);
						return options.log?.onClose?.(log);
					},

					onWrite: async (log, entry) => {
						await this.onWrite(program, log, entry, program.address);
						return options.log?.onWrite?.(log, entry);
					},

					onChange: async (log, change) => {
						return options?.log?.onChange?.(log, change);
					},
				},
			});

			// Address will not always exist
			const pm = await this.addProgram(program, minReplicas, options.sync);
			await this.subscribeToProgram(program.address.toString(), role);

			/// TODO encryption
			this._sortedPeersCache.set(
				program.address.toString(),
				role instanceof ReplicatorType ? [this.idKeyHash] : []
			);

			return pm;
		};
		const openStore = await this._openProgramQueue.add(fn);
		if (!openStore?.program.address) {
			throw new Error("Unexpected");
		}
		return openStore.program as S;
	}

	async getEncryptionKey(
		address: string
	): Promise<KeyWithMeta<Ed25519Keypair | X25519Keypair> | undefined> {
		// v0 take some recent
		const keys = await this.keystore.getKeys<Ed25519Keypair | X25519Keypair>(
			address
		);
		const key = keys?.[0];
		return key;
	}
}
const areWeTestingWithJest = (): boolean => {
	return process.env.JEST_WORKER_ID !== undefined;
};
