import path from "path";
import { IStoreOptions, Store } from "@dao-xyz/peerbit-store";
import Cache from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta, StoreError } from "@dao-xyz/peerbit-keystore";
import { isDefined } from "./is-defined.js";
import { AbstractLevel } from "abstract-level";
import { Level } from "level";
import { MemoryLevel } from "memory-level";
import { multiaddr } from "@multiformats/multiaddr";
import {
    exchangeHeads,
    ExchangeHeadsMessage,
    AbsolutMinReplicas,
    EntryWithRefs,
    MinReplicas,
} from "./exchange-heads.js";
import { Entry, Identity } from "@dao-xyz/ipfs-log";
import { serialize, deserialize, BorshError } from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import type {
    Message as PubSubMessage,
    SignedMessage as SignedPubSubMessage,
} from "@libp2p/interface-pubsub";
import { SharedChannel, SharedIPFSChannel } from "./channel.js";
import {
    exchangeKeys,
    KeyResponseMessage,
    KeyAccessCondition,
    recieveKeys,
    requestAndWaitForKeys,
    RequestKeyMessage,
    RequestKeyCondition,
    RequestKeysByKey,
    RequestKeysByAddress,
} from "./exchange-keys.js";
import {
    X25519PublicKey,
    PeerIdAddress,
    AccessError,
    DecryptedThing,
    Ed25519Keypair,
    EncryptedThing,
    MaybeEncrypted,
    PublicKeyEncryptionResolver,
    PublicSignKey,
    X25519Keypair,
} from "@dao-xyz/peerbit-crypto";
import LRU from "lru-cache";
import { DirectChannel } from "@dao-xyz/ipfs-pubsub-direct-channel";
import { encryptionWithRequestKey } from "./encryption.js";
import { MaybeSigned } from "@dao-xyz/peerbit-crypto";
import { createHash } from "crypto";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import {
    AbstractProgram,
    CanOpenSubPrograms,
    Program,
    Address,
} from "@dao-xyz/peerbit-program";
import PQueue from "p-queue";
import { Libp2p } from "libp2p";
import { IpfsPubsubPeerMonitor } from "@dao-xyz/ipfs-pubsub-peer-monitor";
import type { PeerId } from "@libp2p/interface-peer-id";
import {
    exchangeSwarmAddresses,
    ExchangeSwarmMessage,
} from "./exchange-network.js";
import { setTimeout } from "timers";
import { getNetwork, network } from "./network.js";
import isNode from "is-node";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import {
    LibP2PBlockStore,
    LevelBlockStore,
    Blocks,
    BlockStore,
} from "@dao-xyz/peerbit-block";
export const logger = loggerFn({ module: "peer" });
const MIN_REPLICAS = 2;

interface ProgramWithMetadata {
    program: Program;
    minReplicas: MinReplicas;
}

export type StoreOperations = "write" | "all";
export type Storage = {
    createStore: (string?: string) => AbstractLevel<any, string, Uint8Array>;
};
export type OptionalCreateOptions = {
    limitSigning?: boolean;
    minReplicas?: number;
    waitForKeysTimout?: number;
    store?: BlockStore;
    canOpenProgram?(
        address: string,
        topic?: string,
        entryToReplicate?: Entry<any>
    ): Promise<boolean>;
};
export type CreateOptions = {
    keystore: Keystore;
    identity: Identity;
    directory?: string;
    peerId: PeerId;
    storage: Storage;
    cache: Cache<any>;
    localNetwork: boolean;
    browser?: boolean;
} & OptionalCreateOptions;
export type CreateInstanceOptions = {
    storage?: Storage;
    directory?: string;
    keystore?: Keystore;
    peerId?: PeerId;
    identity?: Identity;
    cache?: Cache<any>;
    localNetwork?: boolean;
    browser?: boolean;
} & OptionalCreateOptions;
export type OpenStoreOptions = {
    identity?: Identity;
    entryToReplicate?: Entry<any>;
    directory?: string;
    timeout?: number;
    minReplicas?: MinReplicas;
    verifyCanOpen?: boolean;
    topic?: string;
} & IStoreOptions<any>;

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

export const getObserverTopic = (topic: string) => {
    return topic;
};

export const getReplicationTopic = (topic: string) => {
    return topic + "!";
};
export const isReplicationTopic = (topic: string) => {
    return topic.endsWith("!");
};

export class Peerbit {
    _libp2p: Libp2p;
    _store: Blocks;
    _directConnections: Map<string, SharedChannel<DirectChannel>>;
    _topicSubscriptions: Map<string, SharedChannel<SharedIPFSChannel>>;

    identity: Identity;
    id: PeerId;
    directory?: string;
    storage: Storage;
    caches: { [key: string]: { cache: Cache<any>; handlers: Set<string> } };
    keystore: Keystore;
    _minReplicas: number;
    /// topic => program address => Program metadata
    programs: Map<string, Map<string, ProgramWithMetadata>>;
    limitSigning: boolean;
    localNetwork: boolean;
    browser: boolean; // is running inside of browser?

    _gidPeersHistory: Map<string, Set<string>> = new Map();
    _waitForKeysTimeout: number | undefined;
    _keysInflightMap: Map<string, Promise<any>> = new Map(); // TODO fix types
    _keyRequestsLRU: LRU<
        string,
        KeyWithMeta<Ed25519Keypair | X25519Keypair>[] | null
    > = new LRU({ max: 100, ttl: 10000 });

    _canOpenProgram: (
        address: string,
        topic?: string,
        entryTopReplicate?: Entry<any>
    ) => Promise<boolean>;
    _openProgramQueue: PQueue;
    _disconnected = false;
    _disconnecting = false;
    _encryption: PublicKeyEncryptionResolver;

    constructor(libp2p: Libp2p, identity: Identity, options: CreateOptions) {
        if (!isDefined(libp2p)) {
            throw new Error("Libp2p required");
        }
        if (!isDefined(identity)) {
            throw new Error("identity key required");
        }

        this._libp2p = libp2p;
        this._store = new Blocks(
            new LibP2PBlockStore(
                this._libp2p,
                options.store ||
                    new LevelBlockStore(
                        options.storage.createStore(
                            options.directory &&
                                path
                                    .join(
                                        options.directory,
                                        options.peerId.toString(),
                                        "/blocks"
                                    )
                                    .toString()
                        )
                    )
            )
        );
        this._store.open();

        this.identity = identity;
        this.id = options.peerId;

        this.directory = options.directory || "./orbitdb";
        this.storage = options.storage;
        this._directConnections = new Map();
        this.programs = new Map();
        this.caches = {};
        this._minReplicas = options.minReplicas || MIN_REPLICAS;
        this.limitSigning = options.limitSigning || false;
        this.browser = options.browser || !isNode;
        this._canOpenProgram =
            options.canOpenProgram ||
            (async (address, topic, entryToReplicate) => {
                const network = this._getNetwork(address, topic);
                if (!network) {
                    return Promise.resolve(true);
                }

                if (!entryToReplicate) {
                    return this.isTrustedByNetwork(undefined, address, topic);
                }

                for (const signature of await entryToReplicate.getSignatures()) {
                    const trusted = await this.isTrustedByNetwork(
                        signature.publicKey,
                        address,
                        topic
                    );
                    if (trusted) {
                        return true;
                    }
                }
                return false;
            });

        this.localNetwork = options.localNetwork;
        this.caches[this.directory] = {
            cache: options.cache,
            handlers: new Set(),
        };
        this.keystore = options.keystore;
        if (typeof options.waitForKeysTimout === "number") {
            this._waitForKeysTimeout = options.waitForKeysTimout;
        }
        this._openProgramQueue = new PQueue({ concurrency: 1 });

        this._topicSubscriptions = new Map();
    }

    get libp2p(): Libp2p {
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
        data: Uint8Array
    ): Promise<DecryptedThing<MaybeSigned<Uint8Array>>> {
        const signedMessage = await new MaybeSigned({ data }).sign(
            async (data) => {
                return {
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
        reciever: X25519PublicKey
    ): Promise<EncryptedThing<MaybeSigned<Uint8Array>>> {
        const signedMessage = await new MaybeSigned({ data }).sign(
            async (data) => {
                return {
                    publicKey: this.identity.publicKey,
                    signature: await this.identity.sign(data),
                };
            }
        );
        return new DecryptedThing<MaybeSigned<Uint8Array>>({
            data: serialize(signedMessage),
        }).encrypt(this.encryption.getEncryptionKeypair, reciever);
    }

    static async create(libp2p: Libp2p, options: CreateInstanceOptions = {}) {
        const id: PeerId = libp2p.peerId;
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
                ? path.join(directory, id.toString(), "/keystore")
                : undefined;
            logger.info(
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
            }

            identity = {
                ...signKey.keypair,
                sign: (data) => signKey.keypair.sign(data),
            };
        }

        const cache =
            options.cache ||
            new Cache(
                await storage.createStore(
                    directory
                        ? path.join(directory, id.toString(), "/cache")
                        : undefined
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

        const peer = new Peerbit(libp2p, identity, finalOptions);
        await peer.getEncryption();
        return peer;
    }

    async disconnect() {
        this._disconnecting = true;
        // Close a direct connection and remove it from internal state

        for (const [_topic, channel] of this._topicSubscriptions) {
            await channel.close();
        }

        const removeDirectConnect = (value: any, e: string) => {
            this._directConnections.get(e)?.close();
            this._directConnections.delete(e);
        };

        // Close all direct connections to peers
        this._directConnections.forEach(removeDirectConnect);

        // close keystore
        await this.keystore.close();

        // Close all open databases
        for (const [key, dbs] of this.programs.entries()) {
            await Promise.all(
                [...dbs.values()].map((program) => program.program.close())
            );
            this.programs.delete(key);
            // delete this.allPrograms[key];
        }

        const caches = Object.keys(this.caches);
        for (const directory of caches) {
            await this.caches[directory].cache.close();
            delete this.caches[directory];
        }

        await this._store.close();

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
    onWrite<T>(program: Program) {
        return (store: Store<any>, entry: Entry<T>, topic: string): void => {
            const programAddress =
                program.address?.toString() ||
                program.parentProgram.address!.toString();
            const storeInfo = this.programs
                .get(topic)
                ?.get(programAddress)
                ?.program.allStoresMap.get(store._storeIndex);
            if (!storeInfo) {
                throw new Error("Missing store info");
            }
            const observerTopic = getObserverTopic(topic);
            const sendAll =
                this._libp2p.pubsub.getSubscribers(observerTopic)?.length > 0
                    ? (data: Uint8Array): Promise<any> => {
                          return this.libp2p.pubsub.publish(
                              getObserverTopic(topic),
                              data
                          );
                      }
                    : undefined;
            let send = sendAll;
            if (!this.browser && store.replicate) {
                // send to peers directly
                send = async (data: Uint8Array) => {
                    const minReplicas = this.programs
                        .get(topic)
                        ?.get(programAddress)?.minReplicas.value;
                    if (typeof minReplicas !== "number") {
                        throw new Error(
                            "Min replicas info not found for: " +
                                topic +
                                "/" +
                                programAddress
                        );
                    }

                    const replicators = await this.findReplicators(
                        topic,
                        programAddress,
                        entry.gid,
                        minReplicas
                    );
                    const channels: SharedChannel<DirectChannel>[] = [];
                    for (const replicator of replicators) {
                        if (replicator === this.id.toString()) {
                            continue;
                        }
                        const channel = this._directConnections.get(replicator);
                        if (
                            !channel ||
                            this.libp2p.pubsub.getSubscribers(
                                channel.channel._id
                            ).length === 0
                        ) {
                            // we are missing a channel, send to all instead as fallback
                            return sendAll && sendAll(data);
                        } else {
                            channels.push(channel);
                        }
                    }
                    await Promise.all(
                        channels.map((channel) => channel.channel.send(data))
                    );
                    return;
                };
            }
            if (send) {
                exchangeHeads(
                    send,
                    store,
                    program,
                    [entry],
                    topic,
                    true,
                    this.limitSigning ? undefined : this.identity
                ).catch((error) => {
                    logger.error("Got error when exchanging heads: " + error);
                    throw error;
                });
            }
        };
    }

    async isTrustedByNetwork(
        identity: PublicSignKey | undefined,
        address: string,
        topic?: string
    ): Promise<boolean> {
        if (!identity) {
            return false;
        }
        const network = this._getNetwork(address, topic);
        if (!network) {
            return false;
        }
        return !!(await network.isTrusted(identity));
    }

    _maybeOpenStorePromise: Promise<boolean>;
    // Callback for receiving a message from the network
    async _onMessage(message: PubSubMessage) {
        logger.debug(
            `${this.id}: Recieved message on topic: ${message.topic} ${message.data.length}`
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
            const peer =
                message.type === "signed"
                    ? (message as SignedPubSubMessage).from
                    : undefined;
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
            const sender: PublicSignKey | undefined =
                signedMessage.signature?.publicKey;
            const checkTrustedSender = async (
                address: string,
                onlyNetworked: boolean
            ): Promise<boolean> => {
                let isTrusted = false;
                if (sender) {
                    // find the progrma
                    const network = this._getNetwork(address);
                    if (!network) {
                        if (onlyNetworked) {
                            return false;
                        }
                        return true;
                    } else if (network instanceof TrustedNetwork) {
                        isTrusted = !!(await network.isTrusted(sender));
                    } else {
                        throw new Error("Unexpected network type");
                    }
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

                const { topic, storeIndex, programAddress, heads } = msg;
                // replication topic === trustedNetwork address

                const pstores = this.programs.get(topic);
                const paddress = programAddress;

                logger.debug(
                    `${this.id}: Recieved heads: ${
                        heads.length === 1
                            ? heads[0].entry.hash
                            : "#" + heads[0].entry.hash
                    }, topic: ${topic}, storeIndex: ${storeIndex}`
                );
                if (heads) {
                    const leaderCache: Map<string, string[]> = new Map();
                    if (!pstores?.has(programAddress)) {
                        await this._maybeOpenStorePromise;
                        for (const [gid, entries] of await groupByGid(heads)) {
                            // Check if root, if so, we check if we should open the store
                            const leaders = await this.findLeaders(
                                topic,
                                programAddress,
                                gid,
                                msg.minReplicas?.value || this._minReplicas
                            ); // Todo reuse calculations
                            leaderCache.set(gid, leaders);
                            if (leaders.find((x) => x === this.id.toString())) {
                                const oneEntry = entries[0].entry;
                                try {
                                    // Assumption: All entries should suffice as references
                                    // when passing to this.open as reasons/proof of validity of opening the store

                                    await this.open(Address.parse(paddress), {
                                        topic: topic,
                                        directory: this.directory,
                                        entryToReplicate: oneEntry,
                                        verifyCanOpen: true,
                                        identity: this.identity,
                                        minReplicas: msg.minReplicas,
                                    });
                                } catch (error) {
                                    if (error instanceof AccessError) {
                                        logger.debug(
                                            `${this.id}: Failed to open store from head: ${oneEntry.hash}`
                                        );

                                        return;
                                    }
                                    throw error; // unexpected
                                }
                                break;
                            }
                        }
                    }

                    const programInfo = this.programs
                        .get(topic)!
                        .get(paddress)!;
                    const storeInfo =
                        programInfo.program.allStoresMap.get(storeIndex);
                    if (!storeInfo) {
                        throw new Error(
                            "Missing store info, which was expected to exist for " +
                                topic +
                                ", " +
                                paddress +
                                ", " +
                                storeIndex
                        );
                    }
                    const toMerge: EntryWithRefs<any>[] = [];

                    await programInfo.program.initializationPromise; // Make sure it is ready

                    heads.forEach((head) =>
                        head.entry.init({
                            encryption: storeInfo.oplog._encryption,
                            encoding: storeInfo.oplog._encoding,
                        })
                    ); // we need to init because we perhaps need to decrypt gid

                    for (const [gid, value] of await groupByGid(heads)) {
                        const leaders =
                            leaderCache.get(gid) ||
                            (await this.findLeaders(
                                topic,
                                programAddress,
                                gid,
                                programInfo.minReplicas.value
                            ));
                        const isLeader = leaders.find(
                            (l) => l === this.id.toString()
                        );
                        if (!isLeader) {
                            logger.debug(
                                `${this.id}: Dropping heads with gid: ${gid}. Because not leader`
                            );
                            continue;
                        }
                        value.forEach((head) => {
                            toMerge.push(head);
                            logger.debug(
                                `${this.id}: Leader for head: ' ${head.entry.hash}`
                            );
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
                        await store.sync(toMerge);
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
            } else if (msg instanceof KeyResponseMessage) {
                await recieveKeys(msg, (keys) => {
                    return Promise.all(
                        keys.map((key) => this.keystore.saveKey(key))
                    );
                });
            } else if (msg instanceof ExchangeSwarmMessage) {
                let hasAll = true;
                for (const i of msg.info) {
                    if (!this._directConnections.has(i.id)) {
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
            } else if (msg instanceof RequestKeyMessage) {
                /**
                 * Someone is requesting X25519 Secret keys for me so that they can open encrypted messages (and encrypt)
                 *
                 */
                if (!peer) {
                    logger.error("Execting a sigmed pubsub message");
                    return;
                }

                if (!sender) {
                    logger.info("Expecing sender when recieving key info");
                    return;
                }
                let canExchangeKey: KeyAccessCondition;
                if (msg.condition instanceof RequestKeysByAddress) {
                    if (
                        !(await checkTrustedSender(msg.condition.address, true))
                    ) {
                        return;
                    }
                    canExchangeKey = (key) =>
                        Promise.resolve(
                            key.group ===
                                (msg.condition as RequestKeysByAddress).address
                        );
                } else if (msg.condition instanceof RequestKeysByKey) {
                    canExchangeKey = (key) =>
                        checkTrustedSender(key.group, true);
                } else {
                    throw new Error("Unexpected message");
                }

                const send = (data: Uint8Array) =>
                    this._libp2p.pubsub.publish(
                        message.topic, // DirectChannel.getTopic([peer.toString()]),
                        data
                    );
                await exchangeKeys(
                    send,
                    msg,
                    canExchangeKey,
                    this.keystore,
                    this.identity,
                    this.encryption
                );
                logger.debug(`Exchanged keys`);
            } else {
                throw new Error("Unexpected message");
            }
        } catch (e: any) {
            if (e instanceof BorshError) {
                logger.trace(
                    `${this.id}: Failed to handle message on topic: ${message.topic} ${message.data.length}: Got message for a different namespace`
                );
                return;
            }
            if (e instanceof AccessError) {
                logger.trace(
                    `${this.id}: Failed to handle message on topic: ${message.topic} ${message.data.length}: Got message I could not decrypt`
                );
                return;
            }
            logger.error(e);
        }
    }

    async _onPeerConnected(topic: string, peer: string) {
        logger.debug(`New peer '${peer}' connected to '${topic}'`);
        try {
            // determine if we should open a channel (we are replicating a store on the topic + a weak check the peer is trusted)
            const programs = this.programs.get(topic);
            if (programs) {
                // Should subscription to a replication be a proof of "REPLICATING?"
                const initializeAsNonBrowser = async () => {
                    await exchangeSwarmAddresses(
                        (data) =>
                            this.libp2p.pubsub.publish(
                                getReplicationTopic(topic),
                                data
                            ),
                        this.identity,
                        peer,
                        this._libp2p.pubsub.getPeers(),
                        this._libp2p.peerStore.addressBook,
                        this._getNetwork(topic),
                        this.localNetwork
                    );
                    await this.getChannel(peer, topic); // always open a channel, and drop channels if necessary (not trusted) (TODO)
                };
                if (programs.size === 0 && !this.browser) {
                    // we are subscribed to replicationTopic, but we have not opened any store, this "means"
                    // that we are intending to replicate data for this topic
                    await initializeAsNonBrowser();
                    return;
                }

                for (const [
                    _storeAddress,
                    programAndStores,
                ] of programs.entries()) {
                    for (const [_, store] of programAndStores.program
                        .allStoresMap) {
                        if (!this.browser && store.replicate) {
                            // create a channel for sending/receiving messages

                            await initializeAsNonBrowser();
                            return;

                            // Creation of this channel here, will make sure it is created even though a head might not be exchangee
                        } else {
                            // If replicate false, we are in write mode. Means we should exchange all heads
                            // Because we dont know anything about whom are to store data, so we assume all peers might have responsibility
                            const send = (data: Uint8Array) =>
                                this._libp2p.pubsub.publish(
                                    getObserverTopic(topic), // DirectChannel.getTopic([peer.toString()]),
                                    data
                                );
                            const headsToExchange = store.oplog.heads;
                            logger.debug(
                                `${this.id}: Exchange heads ${
                                    headsToExchange.length === 1
                                        ? headsToExchange[0].hash
                                        : "#" + headsToExchange.length
                                } onPeerConnected ${topic} - ${peer}`
                            );
                            await exchangeHeads(
                                send,
                                store,
                                programAndStores.program,
                                headsToExchange,
                                topic,
                                false,
                                this.limitSigning ? undefined : this.identity
                            );
                        }
                    }
                }
            }
        } catch (error: any) {
            logger.error(
                "Unexpected error in _onPeerConnected callback: " +
                    error.toString()
            );
            throw error;
        }
    }

    /**
     * When a peers join the networkk and want to participate the leaders for particular log subgraphs might change, hence some might start replicating, might some stop
     * This method will go through my owned entries, and see whether I should share them with a new leader, and/or I should stop care about specific entries
     * @param channel
     */
    async replicationReorganization(modifiedChannel: DirectChannel) {
        const connections = this._directConnections.get(
            modifiedChannel.recieverId.toString()
        );
        if (!connections) {
            logger.error(
                "Missing direct connection to: " +
                    modifiedChannel.recieverId.toString()
            );
            return;
        }

        for (const topic of connections.dependencies) {
            const programs = this.programs.get(topic);
            if (programs) {
                for (const programInfo of programs.values()) {
                    for (const [_, store] of programInfo.program.allStoresMap) {
                        const heads = store.oplog.heads;
                        const groupedByGid = await groupByGid(heads);
                        for (const [gid, entries] of groupedByGid) {
                            if (entries.length === 0) {
                                continue; // TODO maybe close store?
                            }

                            const oldPeersSet = this._gidPeersHistory.get(gid);
                            const newPeers = await this.findReplicators(
                                topic,
                                programInfo.program.address.toString(),
                                gid,
                                programInfo.minReplicas.value
                            );

                            for (const newPeer of newPeers) {
                                if (
                                    !oldPeersSet?.has(newPeer) &&
                                    newPeer !== this.id.toString()
                                ) {
                                    // second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send

                                    // send heads to the new peer
                                    const channel =
                                        this._directConnections.get(
                                            newPeer
                                        )?.channel;
                                    if (!channel) {
                                        logger.error(
                                            "Missing channel when reorg to peer: " +
                                                newPeer.toString()
                                        );
                                        continue;
                                    }
                                    logger.debug(
                                        `${this.id}: Exchange heads ${
                                            entries.length === 1
                                                ? entries[0].hash
                                                : "#" + entries.length
                                        }  on rebalance ${topic}`
                                    );
                                    await exchangeHeads(
                                        async (message) => {
                                            await channel.send(message);
                                        },
                                        store,
                                        programInfo.program,
                                        entries,
                                        topic,
                                        true,
                                        this.limitSigning
                                            ? undefined
                                            : this.identity
                                    );
                                }
                            }

                            if (
                                !newPeers.find((x) => x === this.id.toString())
                            ) {
                                // delete entries since we are not suppose to replicate this anymore
                                // TODO add delay? freeze time? (to ensure resiliance for bad io)
                                store.oplog.removeAll(entries);

                                // TODO if length === 0 maybe close store?
                            }
                            this._gidPeersHistory.set(gid, new Set(newPeers));
                        }
                    }
                }
            }
        }
    }

    async join(program: Program) {
        const network = getNetwork(program);
        if (!network) {
            throw new Error("Program not part of anetwork");
        }
        // Will be rejected by peers if my identity is not trusted
        // (this will sign our IPFS ID with our client Ed25519 key identity, if peers do not trust our identity, we will be rejected)
        await network.add(
            new PeerIdAddress({ address: this._libp2p.peerId.toString() })
        );
    }

    async getChannel(
        peer: string,
        fromTopic: string
    ): Promise<DirectChannel | undefined> {
        // TODO what happens if disconnect and connection to direct connection is happening
        // simultaneously
        const getDirectConnection = (peer: string) =>
            this._directConnections.get(peer)?._channel;

        let channel = getDirectConnection(peer);
        if (!channel) {
            try {
                logger.debug(`Create a channel to ${peer}`);
                channel = await DirectChannel.open(
                    this.libp2p,
                    peer,
                    (message) => {
                        logger.debug(
                            `${this.id}: Recieved message from direct channel: ${message.topic}`
                        );
                        this._onMessage(message);
                    },
                    {
                        onPeerLeaveCallback: (channel) => {
                            // First modify direct connections
                            this._directConnections
                                .get(channel.recieverId.toString())
                                ?.close(channel.recieverId.toString());

                            // Then perform replication reorg
                            this.replicationReorganization(channel);
                        },
                        onNewPeerCallback: (channel) => {
                            // First modify direct connections
                            if (
                                !this._directConnections.has(
                                    channel.recieverId.toString()
                                )
                            ) {
                                this._directConnections.set(
                                    channel.recieverId.toString(),
                                    new SharedChannel(
                                        channel,
                                        new Set([fromTopic])
                                    )
                                );
                            } else {
                                this._directConnections
                                    .get(channel.recieverId.toString())
                                    ?.dependencies.add(fromTopic);
                            }

                            // Then perform replication reorg
                            this.replicationReorganization(channel);
                        },
                    }
                );
                logger.debug(`Channel created to ${peer}`);
            } catch (e: any) {
                logger.error(e);
                return undefined;
            }
        }

        // Wait for the direct channel to be fully connected
        try {
            let cancel = false;
            setTimeout(() => {
                cancel = true;
            }, 20 * 1000); // 20s timeout

            const connected = await channel.connect({
                isClosed: () =>
                    cancel || this._disconnected || this._disconnecting,
            });
            if (!connected) {
                return undefined; // failed to create channel
            }
        } catch (error) {
            if (this._disconnected || this._disconnecting) {
                return; // its ok
            }
            throw error; // unexpected
        }
        logger.debug(`Connected to ${peer}`);

        return channel;
    }

    // Callback when a store was closed
    async _onClose(program: Program, db: Store<any>, topic: string) {
        // TODO Can we really close a this.programs, either we close all stores in the replication topic or none

        const programAddress = program.address?.toString();

        logger.debug(`Close ${programAddress}/${db.id}`);

        // Unsubscribe from pubsub
        await this.unsubscribeToTopic(topic, db.id);

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
    async _onProgamClose(program: Program, topic: string) {
        const programAddress = program.address?.toString();
        if (programAddress) {
            this.programs.get(topic)?.delete(programAddress);
        }
        const otherStoresUsingSameReplicationTopic = this.programs.get(topic);
        // close all connections with this repplication topic if this is the last dependency
        const isLastStoreForReplicationTopic =
            otherStoresUsingSameReplicationTopic?.size === 0;
        if (isLastStoreForReplicationTopic) {
            for (const [key, connection] of this._directConnections) {
                await connection.close(topic);
                // Delete connection from thing

                // TODO what happens if we close a store, but not its direct connection? we should open direct connections and make it dependenct on the replciation topic
            }
        }
    }

    _onDrop(db: Store<any>) {
        logger.info("Dropped store: " + db.id);
    }

    addProgram(
        topic: string,
        program: Program,
        minReplicas: MinReplicas
    ): ProgramWithMetadata {
        if (!this.programs.has(topic)) {
            this.programs.set(topic, new Map());
        }
        if (!this.programs.has(topic)) {
            throw new Error("Unexpected behaviour");
        }

        const programAddress = program.address?.toString();
        if (!programAddress) {
            throw new Error("Missing program address");
        }
        const existingProgramAndStores = this.programs
            .get(topic)
            ?.get(programAddress);
        if (
            !!existingProgramAndStores &&
            existingProgramAndStores.program !== program
        ) {
            // second condition only makes this throw error if we are to add a new instance with the same address
            throw new Error(`Program at ${topic} is already created`);
        }
        const p = {
            program,
            minReplicas,
        };
        this.programs.get(topic)?.set(programAddress, p);
        return p;
    }

    getReplicatorsOnTopic(topic: string): string[] {
        return (
            this._topicSubscriptions.get(getReplicationTopic(topic))?.channel
                ._monitor?._peers || []
        );
    }

    /**
     * An intentionally imperfect leader rotation routine
     * @param slot, some time measure
     * @returns
     */
    isLeader(leaders: string[]): boolean {
        return !!leaders.find((id) => id === this.id.toString());
    }

    findReplicators(
        topic: string,
        address: string,
        gid: string,
        minReplicas: number
    ): Promise<string[]> {
        return this.findLeaders(topic, address, gid, minReplicas);
    }

    async findLeaders(
        topic: string,
        address: string,
        slot: { toString(): string },
        numberOfLeaders: number
    ): Promise<string[]> {
        // Hash the time, and find the closest peer id to this hash
        const h = (h: string) => createHash("sha1").update(h).digest("hex");
        const slotHash = h(slot.toString());

        // Assumption: All peers wanting to replicate on topic has direct connections with me (Fully connected network)
        const allPeers: string[] = this.getReplicatorsOnTopic(topic);

        // Assumption: Network specification is accurate
        // Replication topic is not an address we assume that the network allows all participants
        const network = this._getNetwork(address, topic);
        const isTrusted = (peer: string | PeerId) =>
            network
                ? network.isTrusted(
                      new PeerIdAddress({ address: peer.toString() })
                  )
                : true;
        const peers = await Promise.all(allPeers.map(isTrusted)).then(
            (results) => allPeers.filter((_v, index) => results[index])
        );

        const hashToPeer: Map<string, string> = new Map();
        const peerHashed: string[] = [];

        if (peers.length === 0) {
            return [this.id.toString()];
        }

        // Add self
        const iAmReplicating = this._topicSubscriptions.has(
            getReplicationTopic(topic)
        ); // TODO add conditional whether this represents a network (I am not replicating if I am not trusted (pointless))
        if (iAmReplicating) {
            peers.push(this.id.toString());
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

    async subscribeToTopic(
        topic: string,
        asReplicator: boolean
    ): Promise<void> {
        if (this._disconnected || this._disconnecting) {
            throw new Error("Disconnected");
        }

        if (!this.programs.has(topic)) {
            this.programs.set(topic, new Map());
        }

        const handleTopic = async (
            topic: string,
            onPeerConnected?: (peer: string) => void
        ) => {
            if (!this._topicSubscriptions.has(topic)) {
                const topicMonitor = new IpfsPubsubPeerMonitor(
                    this.libp2p.pubsub,
                    topic,
                    {
                        onJoin: (peer) => {
                            logger.debug(`Peer joined ${topic}:`);
                            logger.debug(peer);
                            onPeerConnected && onPeerConnected(peer);
                        },
                        onLeave: (peer) => {
                            logger.debug(`Peer ${peer} left ${topic}`);
                        },
                        onError: (e) => {
                            logger.error(e);
                        },
                    }
                );
                this._topicSubscriptions.set(
                    topic,
                    new SharedChannel(
                        await new SharedIPFSChannel(
                            this._libp2p,
                            this.id,
                            topic,
                            (message) => {
                                return this._onMessage(message);
                            },
                            topicMonitor
                        ).start()
                    )
                );
            }
        };

        // The last argument below make sure we only do (onPeerConnected once even though we might be subscribing as an observer (and potentialy replicator))
        await handleTopic(
            getObserverTopic(topic),
            asReplicator
                ? undefined
                : (peer) => this._onPeerConnected(topic, peer)
        );
        if (asReplicator) {
            await handleTopic(getReplicationTopic(topic), (peer) =>
                this._onPeerConnected(topic, peer)
            );
        }
    }

    hasSubscribedToTopic(topic: string): boolean {
        return this.programs.has(topic);
    }
    async unsubscribeToTopic(
        topic: string | TrustedNetwork,
        id: string
    ): Promise<boolean> {
        if (typeof topic !== "string") {
            if (!topic.address) {
                throw new Error(
                    "Can not get network address from topic as TrustedNetwork"
                );
            }
            topic = topic.address.toString();
        }

        const a = await this._topicSubscriptions
            .get(getReplicationTopic(topic))
            ?.close(id);
        const b = await this._topicSubscriptions
            .get(getObserverTopic(topic))
            ?.close(id);
        return !!(a || b);
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
        options: OpenStoreOptions = {}
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
                        this._store,
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

            await program.save(this._store);
            const programAddress = program.address!.toString()!;

            const definedTopic: string = (options.topic || programAddress)!;
            if (!definedTopic) {
                throw new Error("Replication topic is undefined");
            }
            if (programAddress) {
                const existingProgram = this.programs
                    .get(definedTopic)
                    ?.get(programAddress);
                if (existingProgram) {
                    return existingProgram;
                }
            }

            logger.debug("open()");

            const pstores = this.programs.get(definedTopic);
            if (
                programAddress &&
                (!pstores || !pstores.has(programAddress)) &&
                options.verifyCanOpen
            ) {
                // open store if is leader and sender is trusted
                let senderCanOpen = false;

                if (!program.owner) {
                    // can open is is trusted by netwoek?
                    senderCanOpen = await this._canOpenProgram(
                        programAddress,
                        definedTopic,
                        options.entryToReplicate
                    );
                } else if (options.entryToReplicate) {
                    const ownerAddress = Address.parse(program.owner);
                    const ownerProgramRootAddress = ownerAddress.root();
                    let ownerProgram: AbstractProgram | undefined =
                        this.programs
                            .get(definedTopic)
                            ?.get(ownerProgramRootAddress.toString())?.program;
                    if (ownerAddress.path) {
                        ownerProgram = ownerProgram?.subprogramsMap.get(
                            ownerAddress.path.index
                        );
                    }
                    if (!ownerProgram) {
                        logger.info("Failed to find owner program");
                        throw new AccessError("Failed to find owner program");
                    }
                    // TOOD make typesafe
                    const csp =
                        ownerProgram as Program as any as CanOpenSubPrograms;
                    if (!csp.canOpen) {
                        senderCanOpen = false;
                    } else {
                        senderCanOpen = await csp.canOpen(
                            program,
                            options.entryToReplicate
                        );
                    }
                }

                if (!senderCanOpen) {
                    logger.info(
                        "Failed to open program because request is not trusted"
                    );
                    throw new AccessError(
                        "Failed to open program because request is not trusted"
                    );
                }
            }

            options = Object.assign(
                { localOnly: false, create: false },
                options
            );
            logger.debug(`Open database '${program.constructor.name}`);

            if (!options.encryption) {
                options.encryption = await encryptionWithRequestKey(
                    this.identity,
                    this.keystore,
                    this._waitForKeysTimeout
                        ? (key) =>
                              this.requestAndWaitForKeys(
                                  definedTopic,
                                  programAddress,
                                  new RequestKeysByKey({
                                      key,
                                  })
                              )
                        : undefined
                );
            }
            const replicate =
                options.replicate !== undefined ? options.replicate : true;
            await program.init(
                this.libp2p,
                this._store,
                options.identity || this.identity,
                {
                    topic: definedTopic,
                    onClose: () => this._onProgamClose(program, definedTopic!),
                    onDrop: () => this._onProgamClose(program, definedTopic!),

                    store: {
                        ...options,
                        replicate,
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
                            await this._onClose(program, store, definedTopic);
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
                            await this.onWrite(program)(
                                store,
                                entry,
                                definedTopic
                            );
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
                        onReplicationProgress: async (store, entry) => {
                            if (options.onReplicationProgress) {
                                options.onReplicationProgress(store, entry);
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
                definedTopic,
                program,
                options.minReplicas || new AbsolutMinReplicas(this._minReplicas)
            );
            await this.subscribeToTopic(definedTopic, replicate);
            return pm;
        };
        const openStore = await this._openProgramQueue.add(fn);
        if (!openStore?.program.address) {
            throw new Error("Unexpected");
        }
        return openStore.program as S;
        /*  } */
    }

    _getNetwork(
        address: string | Address,
        topic?: string
    ): TrustedNetwork | undefined {
        const a = typeof address === "string" ? address : address.toString();
        if (!topic)
            for (const [k, v] of this.programs.entries()) {
                if (v.has(a)) {
                    topic = k;
                }
            }
        if (!topic) {
            return;
        }
        const parsedAddress =
            address instanceof Address ? address : Address.parse(address);
        const asPermissioned = this.programs
            .get(topic)
            ?.get(parsedAddress.root().toString())?.program;
        if (!asPermissioned) {
            return;
        }
        return getNetwork(asPermissioned);
    }

    /**
     * Check if we have the database, or part of it, saved locally
     * @param  {[Cache]} cache [The OrbitDBCache instance containing the local data]
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

    async requestAndWaitForKeys<T extends Ed25519Keypair | X25519Keypair>(
        topic: string,
        address: string,
        condition: RequestKeyCondition
    ): Promise<KeyWithMeta<T>[] | undefined> {
        if (!this._getNetwork(address)) {
            return;
        }
        const promiseKey = condition.hashcode;
        const existingPromise = this._keysInflightMap.get(promiseKey);
        if (existingPromise) {
            return existingPromise;
        }

        const lruCache = this._keyRequestsLRU.get(promiseKey);
        if (lruCache !== undefined) {
            return lruCache as KeyWithMeta<T>[];
        }

        const promise = new Promise<KeyWithMeta<T>[] | undefined>(
            (resolve, reject) => {
                const send = (message: Uint8Array) =>
                    this._libp2p.pubsub.publish(
                        getReplicationTopic(topic),
                        message
                    );
                requestAndWaitForKeys(
                    condition,
                    send,
                    this.keystore,
                    this.identity,
                    this._waitForKeysTimeout
                )
                    .then((results) => {
                        if (results && results?.length > 0) {
                            resolve(results as KeyWithMeta<T>[] | undefined); // TODO fix type safety
                        } else {
                            resolve(undefined);
                        }
                    })
                    .catch((error) => {
                        reject(error);
                    });
            }
        );
        this._keysInflightMap.set(promiseKey, promise);

        try {
            const result = await promise;
            this._keyRequestsLRU.set(promiseKey, result ? result : null);
            this._keysInflightMap.delete(promiseKey);
            return result;
        } catch (error) {
            if (error instanceof StoreError) {
                if (this._disconnected) {
                    return undefined;
                }
            }
            throw error;
        }
    }

    async getEncryptionKey(
        topic: string,
        address: string
    ): Promise<KeyWithMeta<Ed25519Keypair | X25519Keypair> | undefined> {
        // v0 take some recent
        const keys = await this.keystore.getKeys<
            Ed25519Keypair | X25519Keypair
        >(address);
        let key = keys?.[0];
        if (!key) {
            const keys = this._waitForKeysTimeout
                ? await this.requestAndWaitForKeys(
                      topic,
                      address,
                      new RequestKeysByAddress({
                          address,
                          type: "encryption",
                      })
                  )
                : undefined;
            key = keys ? keys[0] : undefined;
        }
        return key;
    }
}
const areWeTestingWithJest = (): boolean => {
    return process.env.JEST_WORKER_ID !== undefined;
};
