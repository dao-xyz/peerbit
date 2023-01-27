import { IStoreOptions, Store } from "@dao-xyz/peerbit-store";
import Cache from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta, StoreError } from "@dao-xyz/peerbit-keystore";
import { AbstractLevel } from "abstract-level";
import { Level } from "level";
import { MemoryLevel } from "memory-level";
import { multiaddr } from "@multiformats/multiaddr";
import {
	createExchangeHeadsMessage,
	ExchangeHeadsMessage,
	AbsolutMinReplicas,
	EntryWithRefs,
	MinReplicas,
} from "./exchange-heads.js";
import { Entry, Identity } from "@dao-xyz/peerbit-log";
import { serialize, deserialize, BorshError } from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import {
	X25519PublicKey,
	AccessError,
	DecryptedThing,
	Ed25519Keypair,
	EncryptedThing,
	MaybeEncrypted,
	PublicKeyEncryptionResolver,
	PublicSignKey,
	X25519Keypair,
	Ed25519PublicKey,
	Ed25519PrivateKey,
	getKeypairFromPeerId,
	Sec256k1Keccak256Keypair,
} from "@dao-xyz/peerbit-crypto";
import LRU from "lru-cache";
import { encryptionWithRequestKey } from "./encryption.js";
import { MaybeSigned } from "@dao-xyz/peerbit-crypto";
import {
	Program,
	Address,
	CanTrust
} from "@dao-xyz/peerbit-program";
import PQueue from "p-queue";
import type { Ed25519PeerId } from "@libp2p/interface-peer-id";
import {
	ExchangeSwarmMessage,
} from "./exchange-network.js";
import isNode from "is-node";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import {
	BlockStore,
} from "@dao-xyz/libp2p-direct-block";
import {
	PubSubData, SubscriptionEvent, UnsubcriptionEvent,
} from "@dao-xyz/libp2p-direct-sub";
import sodium from "libsodium-wrappers";
import path from "path-browserify";
import { TimeoutError } from "@dao-xyz/peerbit-time";
import "@libp2p/peer-id";
import { peerIdFromString } from "@libp2p/peer-id";
import { Libp2p } from 'libp2p';
import { createLibp2pExtended, Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
import { equals } from 'uint8arrays'
export const logger = loggerFn({ module: "peer" });

const MIN_REPLICAS = 2;

interface ProgramWithMetadata {
	program: Program;
	minReplicas: MinReplicas
}

export type StoreOperations = "write" | "all";
export type Storage = {
	createStore: (string?: string) => AbstractLevel<any, string, Uint8Array>;
};
export type OptionalCreateOptions = {
	limitSigning?: boolean;
	minReplicas?: number;
	store?: BlockStore;
	refreshIntreval?: number;
};
export type CreateOptions = {
	keystore: Keystore;
	identity: Identity;
	directory?: string;
	peerId: Ed25519PeerId;
	storage: Storage;
	cache: Cache<any>;
	localNetwork: boolean;
	browser?: boolean;
} & OptionalCreateOptions;
export type CreateInstanceOptions = {
	libp2p?: Libp2p | Libp2pExtended,
	storage?: Storage;
	directory?: string;
	keystore?: Keystore;
	peerId?: Ed25519PeerId;
	identity?: Identity;
	cache?: Cache<any>;
	localNetwork?: boolean;
	browser?: boolean;
} & OptionalCreateOptions;
export type OpenOptions = {
	identity?: Identity;
	entryToReplicate?: Entry<any>;
	replicate?: boolean;
	directory?: string;
	timeout?: number;
	minReplicas?: MinReplicas;
} & IStoreOptions<any>;


const OBSERVER_TAG_DATA = new Uint8Array([0]);
const REPLICATOR_TAG_DATA = new Uint8Array([1]);

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
	storage: Storage;
	caches: { [key: string]: { cache: Cache<any>; handlers: Set<string> } };
	keystore: Keystore;
	_minReplicas: number;
	/// program address => Program metadata
	programs: Map<string, ProgramWithMetadata>;
	limitSigning: boolean;
	localNetwork: boolean;
	browser: boolean; // is running inside of browser?
	_gidPeersHistory: Map<string, Set<string>> = new Map();


	/*  _canOpenProgram: (
		 address: string,
		 topic?: string,
		 entryTopReplicate?: Entry<any>
	 ) => Promise<boolean>; */
	_openProgramQueue: PQueue;
	_reorgQueue: PQueue;

	_disconnected = false;
	_disconnecting = false;
	_encryption: PublicKeyEncryptionResolver;
	_programsByReplicator: Map<string, Set<string>> = new Map();
	_refreshInterval: any;

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
		/* 		this._store = new Blocks(
					new LibP2PBlockStore(
						this._libp2p,
						options.store ||
						new LevelBlockStore(
							options.storage.createStore(
								options.directory &&
								path
									.join(options.directory, "/blocks")
									.toString()
							)
						)
					)
				);
				this._store.open(); */

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
			sign: (data) => this.idKey.sign(data),
		};

		this.directory = options.directory || "./peerbit/data";
		this.storage = options.storage;
		this.programs = new Map();
		this.caches = {};
		this._minReplicas = options.minReplicas || MIN_REPLICAS;
		this.limitSigning = options.limitSigning || false;
		this.browser = options.browser || !isNode;
		this.localNetwork = options.localNetwork;
		this.caches[this.directory] = {
			cache: options.cache,
			handlers: new Set(),
		};
		this.keystore = options.keystore;

		this._openProgramQueue = new PQueue({ concurrency: 1 });
		this._reorgQueue = new PQueue({ concurrency: 1 });

		const refreshInterval = options.refreshIntreval || 10000;

		const promise: Promise<boolean> | undefined = undefined;
		// 	TODO do we need this?

		/* 	this._refreshInterval = setInterval(async () => {
				if (promise) {
					return;
				}
				promise = this.replicationReorganization([...this.programs.keys()]);
				await promise;
				promise = undefined;
			}, refreshInterval); */

		this.libp2p.directsub.addEventListener('data', this._onMessage.bind(this))
		this.libp2p.directsub.addEventListener('subscribe', this._onSubscription.bind(this))
		this.libp2p.directsub.addEventListener('unsubscribe', this._onUnsubscription.bind(this))

	}

	static async create(
		options: CreateInstanceOptions = {}
	) {
		await sodium.ready;
		let libp2pExtended: Libp2pExtended = options.libp2p as Libp2pExtended;
		if (!libp2pExtended) {
			libp2pExtended = await createLibp2pExtended()
		}
		else {
			if (!!(libp2pExtended as Libp2pExtended).directblock != !!(libp2pExtended as Libp2pExtended).directsub) {
				throw new Error("Expecting libp2p argument to either be of type Libp2p or Libp2pExtended")
			}
			if (!(options.libp2p as Libp2pExtended).directblock && !(options.libp2p as Libp2pExtended).directsub) {
				libp2pExtended = await createLibp2pExtended({
					libp2p: options.libp2p,
					blocks: {
						directory: options.directory &&
							path
								.join(options.directory, "/blocks")
								.toString()
					}
				})
			}
		}

		await libp2pExtended.start();

		const id: Ed25519PeerId = libp2pExtended.peerId as Ed25519PeerId;
		if (id.type !== "Ed25519") {
			throw new Error(
				"Unsupported id type, expecting Ed25519 but got " + id.type
			);
		}

		const directory = options.directory;
		const storage = options.storage || {
			createStore: (
				path?: string
			): AbstractLevel<any, string, Uint8Array> => {
				return path
					? new Level(path, { valueEncoding: "view" })
					: new MemoryLevel({ valueEncoding: "view" });
			},
		};

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
			keystore = new Keystore(await storage.createStore(keyStorePath));
		}

		let identity: Identity;
		if (options.identity) {
			identity = options.identity;
		} else {

			/* 
						let signKey: KeyWithMeta<Ed25519Keypair>;

			const existingKey = await keystore.getKey(id.toString());
			if (existingKey) {
				if (existingKey.keypair instanceof Ed25519Keypair === false) {
					// TODO add better behaviour for this
					throw new Error(
						"Failed to create keypair from ipfs id because it already exist with a different type: " +
						existingKey.keypair.constructor.name
					);
				}
				signKey = existingKey as KeyWithMeta<Ed25519Keypair>;
			} else {
				signKey = await keystore.createEd25519Key({
					id: id.toString(),
				});
			} */
			const keypair = getKeypairFromPeerId(id);
			if (keypair instanceof Sec256k1Keccak256Keypair) {
				identity = {
					publicKey: keypair.publicKey,
					sign: (data) => keypair.sign(data),
				};
			}
			else {
				identity = {
					privateKey: keypair.privateKey,
					publicKey: keypair.publicKey,
					sign: (data) => keypair.sign(data),
				};
			}

		}

		const cache =
			options.cache ||
			new Cache(
				await storage.createStore(
					directory ? path.join(directory, "/cache") : undefined
				)
			);
		const localNetwork = options.localNetwork || false;
		const finalOptions = Object.assign({}, options, {
			peerId: id,
			keystore,
			identity,
			directory,
			storage,
			cache,
			localNetwork,
		});

		const peer = new Peerbit(libp2pExtended, identity, finalOptions);
		await peer.getEncryption();
		return peer;
	}
	get libp2p(): Libp2pExtended {
		return this._libp2p;
	}

	get cacheDir() {
		return this.directory || "./cache";
	}

	get cache() {
		return this.caches[this.cacheDir].cache;
	}

	get encryption() {
		if (!this._encryption) {
			throw new Error("Unexpected");
		}
		return this._encryption;
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
		const signedMessage = await new MaybeSigned({ data }).sign(
			async (data) => {
				return options?.signWithPeerId
					? {
						publicKey: this.idKey.publicKey,
						signature: this.idKey.sign(data),
					}
					: {
						publicKey: this.identity.publicKey,
						signature: await this.identity.sign(data),
					};
			}
		);
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
		const signedMessage = await new MaybeSigned({ data }).sign(
			async (data) => {
				return options?.signWithPeerId
					? {
						publicKey: this.idKey.publicKey,
						signature: this.idKey.sign(data),
					}
					: {
						publicKey: this.identity.publicKey,
						signature: await this.identity.sign(data),
					};
			}
		);
		return new DecryptedThing<MaybeSigned<Uint8Array>>({
			data: serialize(signedMessage),
		}).encrypt(this.encryption.getEncryptionKeypair, reciever);
	}

	async disconnect() {
		this._disconnecting = true;
		// Close a direct connection and remove it from internal state

		this._refreshInterval && clearInterval(this._refreshInterval);

		// close keystore
		await this.keystore.close();

		// Close all open databases
		await Promise.all(
			[...this.programs.values()].map((program) =>
				program.program.close()
			)
		);

		const caches = Object.keys(this.caches);
		for (const directory of caches) {
			await this.caches[directory].cache.close();
			delete this.caches[directory];
		}

		/* await this._store.close(); */

		// Remove all databases from the state
		this.programs = new Map();
		this._disconnecting = false;
		this._disconnected = true;
	}

	// Alias for disconnect()
	async stop() {
		await this.disconnect();
	}

	async _createCache(directory: string) {
		const cacheStorage = await this.storage.createStore(directory);
		return new Cache(cacheStorage);
	}

	// Callback for local writes to the database. We the update to pubsub.
	onWrite<T>(program: Program, store: Store<any>, entry: Entry<T>, address?: Address | string): void {

		const programAddress = (program.address || program.parentProgram.address).toString();
		const writeAddress = (address?.toString() || programAddress)
		const storeInfo = this.programs
			.get(programAddress)
			?.program.allStoresMap.get(store._storeIndex);
		if (!storeInfo) {
			throw new Error("Missing store info");
		}


		// TODO Should we also do gidHashhistory update here?
		createExchangeHeadsMessage(
			store,
			program,
			[entry],
			true,
			this.limitSigning ? undefined : this.idIdentity
		).then((bytes) => { this.libp2p.directsub.publish(bytes, { topics: [writeAddress] }) })
	}


	_maybeOpenStorePromise: Promise<boolean>;
	// Callback for receiving a message from the network
	async _onMessage(evt: CustomEvent<PubSubData>) {
		const message = evt.detail;
		logger.debug(
			`${this.id}: Recieved message on topics: ${message.topics.length > 1 ? '#' + message.topics.length : message.topics[0]} ${message.data.length}`
		);
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
			const sender: string | undefined =
				signedMessage.signature?.publicKey.hashcode();

			const checkTrustedSender = async (
				address: string
			): Promise<boolean> => {
				const isTrusted = false;
				if (sender) {
					// find the progrma
					const ct = this.getCanTrust(address);
					/* if (!network) {
						if (onlyNetworked) {
							return false;
						}
						return true;
					} else if (network instanceof TrustedNetwork) {
						isTrusted = !!(await network.isTrusted(sender));
					} else {
						throw new Error("Unexpected network type");
					} */
					if (!ct) {
						return false;
					}
					return !!(await ct.isTrusted(sender));
				}
				if (!isTrusted) {
					logger.info("Recieved message from untrusted peer");
					return false;
				}
				return true;
			};


			if (msg instanceof ExchangeHeadsMessage) {
				/**
				 * I have recieved heads from someone else.
				 * I can use them to load associated logs and join/sync them with the data stores I own
				 */

				const { storeIndex, programAddress } = msg;
				let { heads } = msg;
				// replication topic === trustedNetwork address

				const pstores = this.programs;
				const paddress = programAddress;

				logger.debug(
					`${this.id}: Recieved heads: ${heads.length === 1
						? heads[0].entry.hash
						: "#" + heads.length
					}, storeIndex: ${storeIndex}`
				);
				if (heads) {

					const programInfo = this.programs.get(paddress)!;

					if (!programInfo) {
						return;
					}

					const storeInfo =
						programInfo.program.allStoresMap.get(storeIndex);
					if (!storeInfo) {
						logger.error(
							"Missing store info, which was expected to exist for " +
							paddress +
							", " +
							storeIndex
						);
						return;
					}
					const toMerge: EntryWithRefs<any>[] = [];
					heads = heads
						.filter((head) => !storeInfo.oplog.has(head.entry.hash))
						.map((head) => {
							head.entry.init({
								encryption: storeInfo.oplog._encryption,
								encoding: storeInfo.oplog._encoding,
							});
							return head;
						}); // we need to init because we perhaps need to decrypt gid

					for (const [gid, value] of await groupByGid(heads)) {
						if (!(await this.isLeader(programAddress, gid, programInfo.minReplicas.value))) {
							logger.debug(
								`${this.id}: Dropping heads with gid: ${gid}. Because not leader`
							);
							continue;
						}
						value.forEach((head) => {
							toMerge.push(head);
						});
					}

					if (toMerge.length > 0) {
						const store =
							programInfo.program.allStoresMap.get(storeIndex);
						if (!store) {
							throw new Error(
								"Unexpected, missing store on sync"
							);
						}
						/*          for (const v of store.oplog.heads) {
									 if (!v.createdLocally && !(await this.findLeaders(programAddress, v.gid, programInfo.minReplicas.value)).find(x => x === this.id.toString())) {
										 const t = 123;
									 }
								 }
								 const ccc1 = [...programInfo.replicators]; */

						await store.sync(toMerge);

						/*  this._reorgQueue.add(async () => {
							 const ccx = ccc1;
							 const ccc2 = programInfo.replicators.size;
							 const l1 = store.oplog.values.length;
							 const l2 = store.oplog.values.length;
							 for (const v of store.oplog.heads) {
								 for (let i = 0; i < 100; i++) {
									 const tmg = (await this.findLeaders(programAddress, toMerge[0].entry.gid, programInfo.minReplicas.value)).find(x => x === this.id.toString());
									 if (!tmg) {
										 const t = 123;
									 }
								 }
								 if (!v.createdLocally && !(await this.findLeaders(programAddress, v.gid, programInfo.minReplicas.value)).find(x => x === this.id.toString())) {
									 const t = 123;
								 }
							 }
						 }) */
					}
				}
				logger.debug(
					`${this.id}: Synced ${heads.length} heads for '${paddress}/${storeIndex}':\n`,
					JSON.stringify(
						heads.map((e) => e.entry.hash),
						null,
						2
					)
				);
			} else if (msg instanceof ExchangeSwarmMessage) {
				let hasAll = true;
				for (const i of msg.info) {
					if (!this.libp2p.peerStore.has(peerIdFromString(i.id))) {
						hasAll = false;
						break;
					}
				}
				if (hasAll) {
					return;
				}

				await Promise.all(
					msg.info.map(async (info) => {
						if (info.id === this.id.toString()) {
							return;
						}
						const suffix = "/p2p/" + info.id;
						this._libp2p.peerStore.addressBook.set(
							info.peerId,
							info.multiaddrs
						);
						const promises = await Promise.any(
							info.multiaddrs.map((addr) =>
								this._libp2p.dial(
									// addr
									multiaddr(
										addr.toString() +
										(addr.toString().indexOf(suffix) ===
											-1
											? suffix
											: "")
									)
								)
							)
						);
						//  const promises = await this._libp2p.dial(info.peerId)

						return promises;
					})
				);
			} else {
				throw new Error("Unexpected message");
			}
		} catch (e: any) {
			if (e instanceof BorshError) {
				logger.trace(
					`${this.id}: Failed to handle message on topic: ${JSON.stringify(message.topics)} ${message.data.length}: Got message for a different namespace`
				);
				return;
			}
			if (e instanceof AccessError) {
				logger.trace(
					`${this.id}: Failed to handle message on topic: ${JSON.stringify(message.topics)} ${message.data.length}: Got message I could not decrypt`
				);
				return;
			}
			logger.error(e);
		}
	}

	async _onUnsubscription(evt: CustomEvent<UnsubcriptionEvent>) {
		logger.debug(`Peer disconnected '${evt.detail.from.hashcode()}' from '${JSON.stringify(evt.detail.unsubscriptions.map(x => x.topic))}'`);
		for (const unsubscription of evt.detail.unsubscriptions) {
			if (unsubscription.data && equals(unsubscription.data, REPLICATOR_TAG_DATA)) {
				const p = this.programs.get(unsubscription.topic);
				if (p) {
					await this.replicationReorganization([p.program.address.toString()])
				}
			}
		}
	}

	async _onSubscription(evt: CustomEvent<SubscriptionEvent>) {
		logger.debug(`New peer '${evt.detail.from.hashcode()}' connected to '${JSON.stringify(evt.detail.subscriptions.map(x => x.topic))}'`);
		for (const subscription of evt.detail.subscriptions) {
			if (subscription.data && equals(subscription.data, REPLICATOR_TAG_DATA)) {
				const p = this.programs.get(subscription.topic);
				if (p) {
					await this.replicationReorganization([p.program.address.toString()])
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
				for (const [_, store] of programInfo.program.allStoresMap) {
					const heads = store.oplog.heads;
					const groupedByGid = await groupByGid(heads);
					for (const [gid, entries] of groupedByGid) {

						const toSend: Map<string, Entry<any>> = new Map();
						const newPeers: string[] = [];

						if (entries.length === 0) {
							continue; // TODO maybe close store?
						}

						const oldPeersSet = this._gidPeersHistory.get(gid);
						const currentPeers = await this.findLeaders(
							programInfo.program.address.toString(),
							gid,
							programInfo.minReplicas.value
						);
						for (const currentPeer of currentPeers) {
							if (
								!oldPeersSet?.has(currentPeer) &&
								currentPeer !== this.idKeyHash
							) {
								// second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send
								newPeers.push(currentPeer);

								// send heads to the new peer
								// console.log('new gid for peer', newPeers.length, this.id.toString(), newPeer, gid, entries.length, newPeers)
								try {
									logger.debug(
										`${this.id}: Exchange heads ${entries.length === 1
											? entries[0].hash
											: "#" + entries.length
										}  on rebalance`
									);
									entries.forEach((entry) => {
										/*  arr.push(entry); */
										toSend.set(entry.hash, entry);
									});
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

						if (!currentPeers.find((x) => x === this.idKeyHash)) {
							const notCreatedLocally = entries.filter(
								(e) => !e.createdLocally
							);
							// delete entries since we are not suppose to replicate this anymore
							// TODO add delay? freeze time? (to ensure resiliance for bad io)
							if (notCreatedLocally.length > 0) {
								await store.removeOperation(notCreatedLocally, {
									recursively: true,
								});
							}

							// TODO if length === 0 maybe close store?
						}
						this._gidPeersHistory.set(gid, new Set(currentPeers));

						if (toSend.size === 0) {
							continue
						}
						const bytes = await createExchangeHeadsMessage(
							store,
							programInfo.program,
							[...toSend.values()], // TODO send to peers directly
							true,
							this.limitSigning ? undefined : this.idIdentity,
						);

						// TODO perhaps send less messages to more recievers for performance reasons?
						await this._libp2p.directsub.publish(bytes, { to: newPeers })
					}
					changed = true;


				}
			}
		}
		return changed;
	}

	getCanTrust(address: string): CanTrust | undefined {
		const p = this.programs.get(address)?.program;
		if (p) {
			const ct = this.programs.get(address)?.program as any as CanTrust;
			if (ct.isTrusted !== undefined) {
				return ct;
			}
		}
		return;
	}


	// Callback when a store was closed
	async _onClose(program: Program, db: Store<any>) {
		// TODO Can we really close a this.programs, either we close all stores in the replication topic or none

		const programAddress = program.address?.toString();

		logger.debug(`Close ${programAddress}/${db.id}`);

		const dir =
			db && db._options.directory ? db._options.directory : this.cacheDir;
		const cache = this.caches[dir];
		if (cache && cache.handlers.has(db.id)) {
			cache.handlers.delete(db.id);
			if (!cache.handlers.size) {
				await cache.cache.close();
			}
		}
	}
	async _onProgamClose(program: Program) {
		const programAddress = program.address?.toString();
		if (programAddress) {
			const deleted = this.programs.delete(programAddress);
			if (deleted) {
				this.libp2p.directsub.unsubscribe(programAddress);
			}
		}
	}

	_onDrop(db: Store<any>) {
		logger.info("Dropped store: " + db.id);
	}

	addProgram(
		program: Program,
		minReplicas: MinReplicas
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


	getReplicators(address: string): Set<string> | undefined {
		return this.libp2p.directsub.getSubscribersWithData(address, REPLICATOR_TAG_DATA)
	}

	getObservers(address: string): Set<string> | undefined {
		return this.libp2p.directsub.getSubscribersWithData(address, OBSERVER_TAG_DATA)
	}

	async isLeader(address: string, slot: { toString(): string }, numberOfLeaders: number): Promise<boolean> {
		const isLeader = (await this.findLeaders(
			address,
			slot,
			numberOfLeaders
		)).find(
			(l) => l === this.idKeyHash
		);
		return !!isLeader
	}

	async findLeaders(
		address: string,
		slot: { toString(): string },
		numberOfLeaders: number
	): Promise<string[]> {
		// Hash the time, and find the closest peer id to this hash

		const h = (h: string) => sodium.crypto_generichash(16, h, null, "hex");
		const slotHash = h(slot.toString());

		// Assumption: All peers wanting to replicate on topic has direct connections with me (Fully connected network)
		const allPeers: string[] = [
			...(this.getReplicators(address)?.keys() || []),
		];

		// Assumption: Network specification is accurate
		// Replication topic is not an address we assume that the network allows all participants
		const network = this.getCanTrust(address);

		let peers: string[];
		if (network) {
			const isTrusted = (peer: string) =>
				network
					? network.isTrusted(
						peer // TODO improve perf
					)
					: true;

			peers = await Promise.all(allPeers.map(isTrusted)).then((results) =>
				allPeers.filter((_v, index) => results[index])
			);
		} else {
			peers = allPeers;
		}

		const hashToPeer: Map<string, string> = new Map();
		const peerHashed: string[] = [];

		// Add self
		const iAmReplicating = this.programs.get(address)?.program?.replicate; // TODO add conditional whether this represents a network (I am not replicating if I am not trusted (pointless))

		if (peers.length === 0) {
			return iAmReplicating ? [this.idKeyHash] : peers;
		}

		if (iAmReplicating) {
			peers.push(this.idKeyHash.toString());
		}

		// Hash step
		peers.forEach((peer) => {
			const peerHash = h(peer + slotHash); // we do peer + slotHash because we want peerHashed.sort() to be different for each slot, (so that uniformly random pick leaders). You can see this as seed
			hashToPeer.set(peerHash, peer);
			peerHashed.push(peerHash);
		});

		numberOfLeaders = Math.min(numberOfLeaders, peerHashed.length);
		peerHashed.push(slotHash);

		// Choice step

		// TODO make more efficient
		peerHashed.sort((a, b) => a.localeCompare(b)); // sort is needed, since "getPeers" order is not deterministic
		const slotIndex = peerHashed.findIndex((x) => x === slotHash);
		// we only step forward 1 step (ignoring that step backward 1 could be 'closer')
		// This does not matter, we only have to make sure all nodes running the code comes to somewhat the
		// same conclusion (are running the same leader selection algorithm)
		const leaders: string[] = [];
		let offset = 0;
		for (let i = 0; i < numberOfLeaders; i++) {
			let nextIndex = (slotIndex + 1 + i + offset) % peerHashed.length;
			if (nextIndex === slotIndex) {
				offset += 1;
				nextIndex = (nextIndex + 1) % peerHashed.length;
			}
			leaders.push(hashToPeer.get(peerHashed[nextIndex]) as string);
		}
		return leaders;
	}

	private async subscribeToProgram(address: string | Address, replicate?: boolean): Promise<void> {
		if (this._disconnected || this._disconnecting) {
			throw new Error("Disconnected");
		}
		const topic = typeof address === 'string' ? address : address.toString();
		this.libp2p.directsub.subscribe(topic, { data: replicate ? REPLICATOR_TAG_DATA : OBSERVER_TAG_DATA });
		await this.libp2p.directsub.requestSubscribers(topic); // get up to date with who are subscribing to this topic

	}

	hasSubscribedToTopic(topic: string): boolean {
		return this.programs.has(topic);
	}

	async _requestCache(
		address: string,
		directory: string,
		existingCache?: Cache<any>
	) {
		const dir = directory || this.cacheDir;
		if (!this.caches[dir]) {
			const newCache = existingCache || (await this._createCache(dir));
			this.caches[dir] = { cache: newCache, handlers: new Set() };
		}
		this.caches[dir].handlers.add(address);
		const cache = this.caches[dir].cache;

		// "Wake up" the caches if they need it
		if (cache) await cache.open();

		return cache;
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
						this._libp2p.directblock,
						storeOrAddress,
						options
					)) as any as S; // TODO fix typings
					if (program instanceof Program === false) {
						throw new Error(
							`Failed to open program because program is of type ${program.constructor.name} and not ${Program.name}`
						);
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " +
						storeOrAddress.toString()
					);
					throw error;
				}
			}

			await program.save(this._libp2p.directblock);
			const programAddress = program.address!.toString()!;

			if (programAddress) {
				const existingProgram = this.programs?.get(programAddress);
				if (existingProgram) {
					return existingProgram;
				}
			}

			logger.debug("open()");

			options = Object.assign(
				{ localOnly: false, create: false },
				options
			);
			logger.debug(`Open database '${program.constructor.name}`);

			if (!options.encryption) {
				options.encryption = await encryptionWithRequestKey(
					this.identity,
					this.keystore
				);
			}
			const replicate =
				options.replicate !== undefined ? options.replicate : true;
			await program.init(
				this.libp2p,
				options.identity || this.identity,
				{
					onClose: () => this._onProgamClose(program),
					onDrop: () => this._onProgamClose(program),
					replicate,
					replicator: (address, gid) => this.isLeader(address.toString(), gid, (options.minReplicas || new AbsolutMinReplicas(this._minReplicas)).value),
					open: (program) => this.open(program, options), // If the program opens more programs
					store: {
						...options,
						cacheId: programAddress,
						resolveCache: (store) => {
							const programAddress = program.address?.toString();
							if (!programAddress) {
								throw new Error("Unexpected");
							}
							return new Cache(
								this.cache._store.sublevel(
									path.join(programAddress, "store", store.id)
								)
							);
						},
						onClose: async (store) => {
							await this._onClose(program, store);
							if (options.onClose) {
								return options.onClose(store);
							}
							return;
						},
						onDrop: async (store) => {
							await this._onDrop(store);
							if (options.onDrop) {
								return options.onDrop(store);
							}
							return;
						},
						onLoad: async (store) => {
							/*  await this._onLoad(store) */
							if (options.onLoad) {
								return options.onLoad(store);
							}
							return;
						},
						onWrite: async (store, entry) => {
							await this.onWrite(program, store, entry, program.address);
							if (options.onWrite) {
								return options.onWrite(store, entry);
							}
							return;
						},
						onReplicationComplete: async (store) => {
							if (options.onReplicationComplete) {
								options.onReplicationComplete(store);
							}
						},
						onReplicationFetch: async (store, entry) => {
							if (options.onReplicationFetch) {
								options.onReplicationFetch(store, entry);
							}
						},
						onReplicationQueued: async (store, entry) => {
							if (options.onReplicationQueued) {
								options.onReplicationQueued(store, entry);
							}
						},
						onOpen: async (store) => {
							if (options.onOpen) {
								return options.onOpen(store);
							}
							return;
						},
					},
				}
			);

			const resolveCache = async (address: Address) => {
				const cache = await this._requestCache(
					address.toString(),
					options.directory || this.cacheDir
				);
				const haveDB = await this._haveLocalData(
					cache,
					address.toString()
				);
				logger.debug(
					(haveDB ? "Found" : "Didn't find") +
					` database '${address}'`
				);
				if (options.localOnly && !haveDB) {
					logger.warn(`Database '${address}' doesn't exist!`);
					throw new Error(`Database '${address}' doesn't exist!`);
				}
				return cache;
			};
			await resolveCache(program.address!);

			const pm = await this.addProgram(
				program,
				options.minReplicas || new AbsolutMinReplicas(this._minReplicas)
			);

			await this.subscribeToProgram(program.address.toString(), replicate);
			/// TODO encryption


			return pm;
		};
		const openStore = await this._openProgramQueue.add(fn);
		if (!openStore?.program.address) {
			throw new Error("Unexpected");
		}
		return openStore.program as S;
	}

	/**
	 * Check if we have the database, or part of it, saved locally
	 * @param  {[Cache]} cache [The Cache instance containing the local data]
	 * @param  {[Address]} dbAddress [Address of the database to check]
	 * @return {[Boolean]} [Returns true if we have cached the db locally, false if not]
	 */
	async _haveLocalData(cache: Cache<any>, id: string) {
		if (!cache) {
			return false;
		}

		const addr = id;
		const data = await cache.get(path.join(addr, "_manifest"));
		return data !== undefined && data !== null;
	}


	async getEncryptionKey(
		address: string
	): Promise<KeyWithMeta<Ed25519Keypair | X25519Keypair> | undefined> {
		// v0 take some recent
		const keys = await this.keystore.getKeys<
			Ed25519Keypair | X25519Keypair
		>(address);
		const key = keys?.[0];
		return key;
	}
}
const areWeTestingWithJest = (): boolean => {
	return process.env.JEST_WORKER_ID !== undefined;
};
