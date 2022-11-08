import path from 'path'
import { IStoreOptions, Store } from '@dao-xyz/peerbit-store'
import { IPFS } from 'ipfs-core-types';
import Cache from '@dao-xyz/peerbit-cache'
import { Keystore, KeyWithMeta, StoreError } from '@dao-xyz/peerbit-keystore'
import { isDefined } from './is-defined.js'
import { Level } from 'level';
import { exchangeHeads, ExchangeHeadsMessage, AbsolutMinReplicas, EntryWithRefs, MinReplicas } from './exchange-heads.js'
import { Entry, Identity } from '@dao-xyz/ipfs-log'
import { serialize, deserialize } from '@dao-xyz/borsh'
import { ProtocolMessage } from './message.js'
import type { Message as PubSubMessage, SignedMessage as SignedPubSubMessage } from '@libp2p/interface-pubsub';
import { SharedChannel, SharedIPFSChannel } from './channel.js'
import { exchangeKeys, KeyResponseMessage, KeyAccessCondition, recieveKeys, requestAndWaitForKeys, RequestKeyMessage, RequestKeyCondition, RequestKeysByKey, RequestKeysByAddress } from './exchange-keys.js'
import { AccessError, DecryptedThing, Ed25519Keypair, EncryptedThing, MaybeEncrypted, PublicKeyEncryptionResolver, SignatureWithKey, SignKey, X25519Keypair } from "@dao-xyz/peerbit-crypto"
import { X25519PublicKey, IPFSAddress } from '@dao-xyz/peerbit-crypto'
import LRU from 'lru-cache';
import { DirectChannel } from '@dao-xyz/ipfs-pubsub-direct-channel'
import { encryptionWithRequestKey } from './encryption.js'
import { MaybeSigned } from '@dao-xyz/peerbit-crypto';
import { WAIT_FOR_PEERS_TIME, PeerInfoWithMeta } from './exchange-replication.js'
import { createHash } from 'crypto'
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network';
import { multiaddr } from '@multiformats/multiaddr'
import { AbstractProgram, CanOpenSubPrograms, Program, Address } from '@dao-xyz/peerbit-program';
import PQueue from 'p-queue';
import { LRUCounter } from './lru-counter.js'
import { IpfsPubsubPeerMonitor } from '@dao-xyz/ipfs-pubsub-peer-monitor';
import type { PeerId } from '@libp2p/interface-peer-id';
import { exchangeSwarmAddresses, ExchangeSwarmMessage } from './exchange-network.js';
import { setTimeout } from 'timers';
import { logger as parentLogger } from './logger.js'
import { isVPC, VPC } from './network.js';


const logger = parentLogger.child({ module: 'peer' });


const MIN_REPLICAS = 2;

interface ProgramWithMetadata {
  program: Program;
  minReplicas: MinReplicas;
}

export type StoreOperations = 'write' | 'all'
export type Storage = { createStore: (string: string) => Level }
export type OptionalCreateOptions = { limitSigning?: boolean, minReplicas?: number, waitForKeysTimout?: number, canOpenProgram?(address: string, replicationTopic?: string, entryToReplicate?: Entry<any>): Promise<boolean> }
export type CreateOptions = { keystore: Keystore, identity: Identity, directory: string, peerId: PeerId, storage: Storage, cache: Cache<any>, localNetwork: boolean } & OptionalCreateOptions;
export type CreateInstanceOptions = { storage?: Storage, directory?: string, keystore?: Keystore, peerId?: PeerId, identity?: Identity, cache?: Cache<any>, localNetwork?: boolean } & OptionalCreateOptions;
export type OpenStoreOptions = {
  identity?: Identity,
  entryToReplicate?: Entry<any>,
  directory?: string,
  timeout?: number,
  minReplicas?: MinReplicas
  verifyCanOpen?: boolean,
  replicationTopic?: string
} & IStoreOptions<any>;

const groupByGid = <T extends (Entry<any> | EntryWithRefs<any>)>(entries: T[]) => {
  const groupByGid: Map<string, T[]> = new Map()
  for (const head of entries) {
    const gid = head instanceof Entry ? head.gid : head.entry.gid;
    let value = groupByGid.get(gid);
    if (!value) {
      value = []
      groupByGid.set(gid, value)
    }
    value.push(head);
  }
  return groupByGid;
}



export class Peerbit {

  _ipfs: IPFS;
  _directConnections: Map<string, SharedChannel<DirectChannel>>;
  _replicationTopicSubscriptions: Map<string, SharedChannel<SharedIPFSChannel>>;

  identity: Identity;
  id: PeerId;
  directory: string;
  storage: Storage;
  caches: { [key: string]: { cache: Cache<any>, handlers: Set<string> } };
  keystore: Keystore;
  _minReplicas: number;
  /// topic => program address => Program metadata
  programs: Map<string, Map<string, ProgramWithMetadata>>;
  limitSigning: boolean;
  localNetwork: boolean;

  _gidPeersHistory: Map<string, Set<string>> = new Map()
  _waitForKeysTimeout = 10000;
  _keysInflightMap: Map<string, Promise<any>> = new Map(); // TODO fix types
  _keyRequestsLRU: LRU<string, KeyWithMeta<Ed25519Keypair | X25519Keypair>[] | null> = new LRU({ max: 100, ttl: 10000 });
  _peerInfoLRU: Map<string, PeerInfoWithMeta> = new Map();// LRU = new LRU({ max: 1000, ttl:  EMIT_HEALTHCHECK_INTERVAL * 4 });
  _supportedHashesLRU: LRUCounter = new LRUCounter(new LRU({ ttl: 60000 }))
  _peerInfoResponseCounter: LRUCounter = new LRUCounter(new LRU({ ttl: 100000 }))
  _canOpenProgram: (address: string, replicationTopic?: string, entryTopReplicate?: Entry<any>) => Promise<boolean>
  _openProgramQueue: PQueue
  _disconnected: boolean = false;
  _disconnecting: boolean = false;

  constructor(ipfs: IPFS, identity: Identity, options: CreateOptions) {
    if (!isDefined(ipfs)) { throw new Error('IPFS required') }
    if (!isDefined(identity)) { throw new Error('identity key required') }

    this._ipfs = ipfs
    this.identity = identity
    this.id = options.peerId

    this.directory = options.directory || './orbitdb'
    this.storage = options.storage
    this._directConnections = new Map();
    this.programs = new Map()
    this.caches = {}
    this._minReplicas = options.minReplicas || MIN_REPLICAS;
    this.limitSigning = options.limitSigning || false;
    this._canOpenProgram = options.canOpenProgram || (async (address, replicationTopic, entryToReplicate) => !this._getNetwork(address, replicationTopic) ? Promise.resolve(true) : (this.isTrustedByNetwork(!entryToReplicate ? undefined : await entryToReplicate.getSignature().then(x => x.publicKey).catch(e => undefined), address, replicationTopic)))
    this.localNetwork = options.localNetwork;
    this.caches[this.directory] = { cache: options.cache, handlers: new Set() }
    this.keystore = options.keystore
    if (typeof options.waitForKeysTimout === 'number') {
      this._waitForKeysTimeout = options.waitForKeysTimout;
    }
    this._openProgramQueue = new PQueue({ concurrency: 1 })
    this._ipfs.pubsub.subscribe(DirectChannel.getTopic([this.id.toString()]), this._onMessage.bind(this));

    this._replicationTopicSubscriptions = new Map();
  }

  get ipfs(): IPFS {
    return this._ipfs;
  }

  get cache() { return this.caches[this.directory].cache }

  get encryption(): PublicKeyEncryptionResolver {
    return encryptionWithRequestKey(this.identity, this.keystore)
  }

  async requestAndWaitForKeys<T extends (Ed25519Keypair | X25519Keypair)>(replicationTopic: string, address: string, condition: RequestKeyCondition<T>): Promise<KeyWithMeta<T>[] | undefined> {
    if (!this._getNetwork(address)) {
      return;
    }
    const promiseKey = condition.hashcode;
    const existingPromise = this._keysInflightMap.get(promiseKey);
    if (existingPromise) {
      return existingPromise
    }

    let lruCache = this._keyRequestsLRU.get(promiseKey);
    if (lruCache !== undefined) {
      return lruCache as KeyWithMeta<T>[];
    }

    const promise = new Promise<KeyWithMeta<T>[] | undefined>((resolve, reject) => {
      const send = (message: Uint8Array) => this._ipfs.pubsub.publish(replicationTopic, message)
      requestAndWaitForKeys(condition, send, this.keystore, this.identity, this._waitForKeysTimeout).then((results) => {
        if (results && results?.length > 0) {
          resolve(results);
        }
        else {
          resolve(undefined);
        }
      }).catch((error) => {
        reject(error);
      })
    })
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
        throw error;
      }
    }
  }

  async decryptedSignedThing(data: Uint8Array): Promise<DecryptedThing<MaybeSigned<Uint8Array>>> {
    const signedMessage = await (new MaybeSigned({ data })).sign(async (data) => {
      return {
        publicKey: this.identity.publicKey,
        signature: await this.identity.sign(data)
      }
    });
    return new DecryptedThing({
      data: serialize(signedMessage)
    })
  }

  async enryptedSignedThing(data: Uint8Array, reciever: X25519PublicKey): Promise<EncryptedThing<MaybeSigned<Uint8Array>>> {
    const signedMessage = await (new MaybeSigned({ data })).sign(async (data) => {
      return {
        publicKey: this.identity.publicKey,
        signature: await this.identity.sign(data)
      }
    });
    return new DecryptedThing<MaybeSigned<Uint8Array>>({
      data: serialize(signedMessage)
    }).encrypt(this.encryption.getEncryptionKeypair, reciever)
  }

  /*   getReplicationTopicEncryption(): PublicKeyEncryptionResolver {
      return replicationTopicEncryptionWithRequestKey(this.identity, this.keystore, (key, replicationTopic) => this.requestAndWaitForKeys(replicationTopic, new RequestKeysByKey<(Ed25519Keypair | X25519Keypair)>({
        key
      })))
    } */


  async getEncryptionKey(replicationTopic: string, address: string): Promise<KeyWithMeta<Ed25519Keypair | X25519Keypair> | undefined> {
    // v0 take some recent
    const keys = (await this.keystore.getKeys<Ed25519Keypair | X25519Keypair>(address));
    let key = keys?.[0];
    if (!key) {
      const keys = this._waitForKeysTimeout ? await this.requestAndWaitForKeys(replicationTopic, address, new RequestKeysByAddress({
        address
      })) : undefined;
      key = keys ? keys[0] : undefined;
    }
    return key;
  }


  static async create(ipfs: IPFS, options: CreateInstanceOptions = {}) {
    let id: PeerId = (await ipfs.id()).id;
    const directory = options.directory || './orbitdb'

    const storage = options.storage || {
      createStore: (path): Level => {
        return new Level(path)
      }
    };

    const keystore: Keystore = options.keystore || new Keystore(await storage.createStore(path.join(directory, id.toString(), '/keystore')))
    let identity: Identity;
    if (options.identity) {
      identity = options.identity;
    }
    else {

      let signKey: KeyWithMeta<Ed25519Keypair>;

      const existingKey = (await keystore.getKey(id.toString()));
      if (existingKey) {
        if (existingKey.keypair instanceof Ed25519Keypair === false) {
          // TODO add better behaviour for this 
          throw new Error("Failed to create keypair from ipfs id because it already exist with a different type: " + existingKey.keypair.constructor.name);

        }
        signKey = existingKey as KeyWithMeta<Ed25519Keypair>;
      }
      else {
        signKey = await keystore.createEd25519Key({ id: id.toString() });
      }


      identity = {
        ...signKey.keypair,
        sign: (data) => signKey.keypair.sign(data)

      }
    }

    const cache = options.cache || new Cache(await storage.createStore(path.join(directory, id.toString(), '/cache')));
    const localNetwork = options.localNetwork || false;
    const finalOptions = Object.assign({}, options, { peerId: id, keystore, identity, directory, storage, cache, localNetwork })
    return new Peerbit(ipfs, identity, finalOptions)
  }



  async disconnect() {
    this._disconnecting = true;
    // Close a direct connection and remove it from internal state

    for (const [_topic, channel] of this._replicationTopicSubscriptions) {
      await channel.close();
    }


    await this._ipfs.pubsub.unsubscribe(DirectChannel.getTopic([this.id.toString()]));
    const removeDirectConnect = (value: any, e: string) => {
      this._directConnections.get(e)?.close()
      this._directConnections.delete(e);
    }

    // Close all direct connections to peers
    this._directConnections.forEach(removeDirectConnect);


    // Disconnect from pubsub
    /*   if (this._pubsub) {
        await this._ipfs.pubsub.disconnect()
      } */


    // close keystore
    await this.keystore.close()

    // Close all open databases
    for (const [key, dbs] of this.programs.entries()) {

      await Promise.all([...dbs.values()].map(program => program.program.close()))
      this.programs.delete(key)
      // delete this.allPrograms[key];
    }

    const caches = Object.keys(this.caches)
    for (const directory of caches) {
      await this.caches[directory].cache.close()
      delete this.caches[directory]
    }

    // Remove all databases from the state
    this.programs = new Map()
    // this.allPrograms = {}

    this._disconnecting = false;
    this._disconnected = true;

  }

  // Alias for disconnect()
  async stop() {
    await this.disconnect()
  }

  async _createCache(directory: string) {
    const cacheStorage = await this.storage.createStore(directory)
    return new Cache(cacheStorage)
  }



  // Callback for local writes to the database. We the update to pubsub.
  onWrite<T>(program: Program) {
    return (store: Store<any>, entry: Entry<T>, replicationTopic: string): void => {
      const programAddress = program.address?.toString() || program.parentProgram.address!.toString();
      const storeInfo = this.programs.get(replicationTopic)?.get(programAddress)?.program.allStoresMap.get(store._storeIndex);
      if (!storeInfo) {
        throw new Error("Missing store info")
      }
      const sendAll = (data: Uint8Array): Promise<void> => this._ipfs.pubsub.publish(replicationTopic, data);
      let send = sendAll;
      if (store.replicate) {
        // send to peers directly
        send = async (data: Uint8Array) => {
          const minReplicas = this.programs.get(replicationTopic)?.get(programAddress)?.minReplicas.value;
          if (typeof minReplicas !== 'number') {
            throw new Error("Min replicas info not found for: " + replicationTopic + '/' + programAddress);
          }

          const replicators = await this.findReplicators(replicationTopic, programAddress, store.replicate, entry.gid, minReplicas);
          const channels: SharedChannel<DirectChannel>[] = [];
          for (const replicator of replicators) {
            if (replicator === this.id.toString()) {
              continue;
            }
            let channel = this._directConnections.get(replicator);
            if (!channel) { // we are missing a channel, send to all instead as fallback
              return sendAll(data);
            }
            else {
              channels.push(channel);
            }
          }
          await Promise.all(channels.map(channel => channel.channel.send(data)));
          return;
        }
      }

      exchangeHeads(send, store, program, [entry], replicationTopic, true, this.limitSigning ? undefined : this.identity)
    }

  }

  async isTrustedByNetwork(identity: SignKey | undefined, address: string, replicationTopic?: string): Promise<boolean> {
    if (!identity) {
      return false;
    }
    let network = this._getNetwork(address, replicationTopic);
    if (!network) {
      return false;
    }
    return !!(await network.isTrusted(identity))
  }

  _maybeOpenStorePromise: Promise<boolean>;
  // Callback for receiving a message from the network
  async _onMessage(message: PubSubMessage) {
    if (this._disconnecting) {
      logger.warn("Got message while disconnecting")
      return;
    }

    if (this._disconnected) {
      throw new Error("Got message while disconnected")
    }

    try {
      const peer = message.type === 'signed' ? (message as SignedPubSubMessage).from : undefined;
      const maybeEncryptedMessage = deserialize(message.data, MaybeEncrypted) as MaybeEncrypted<MaybeSigned<ProtocolMessage>>
      const decrypted = await maybeEncryptedMessage.decrypt(this.encryption.getAnyKeypair)
      const signedMessage = decrypted.getValue(MaybeSigned);
      await signedMessage.verify();
      const msg = signedMessage.getValue(ProtocolMessage);
      const sender: SignKey | undefined = signedMessage.signature?.publicKey;
      const checkTrustedSender = async (address: string, onlyNetworked: boolean): Promise<boolean> => {
        let isTrusted = false;
        if (sender) {
          // find the progrma 
          let network = this._getNetwork(address);
          if (!network) {
            if (onlyNetworked) {
              return false;
            }
            return true;
          }
          else if (network instanceof TrustedNetwork) {
            isTrusted = !!(await network.isTrusted(sender))

          }
          else {
            throw new Error("Unexpected network type")
          }
        }
        if (!isTrusted) {
          logger.info("Recieved message from untrusted peer")
          return false
        }
        return true
      }

      if (msg instanceof ExchangeHeadsMessage) {
        /**
         * I have recieved heads from someone else. 
         * I can use them to load associated logs and join/sync them with the data stores I own
         */

        const { replicationTopic, storeIndex, programIndex, programAddress, heads } = msg
        // replication topic === trustedNetwork address

        let pstores = this.programs.get(replicationTopic)
        const isReplicating = this._replicationTopicSubscriptions.has(replicationTopic); // TODO should be TrustedNetwork has my ipfs id
        const paddress = programAddress;

        if (heads) {

          const leaderCache: Map<string, string[]> = new Map();
          if (!pstores?.has(programAddress)) {
            await this._maybeOpenStorePromise;
            for (const [gid, entries] of groupByGid(heads)) {
              // Check if root, if so, we check if we should open the store
              const leaders = await this.findLeaders(replicationTopic, programAddress, isReplicating, gid, msg.minReplicas?.value || this._minReplicas); // Todo reuse calculations
              leaderCache.set(gid, leaders);
              if (leaders.find(x => x === this.id.toString())) {
                try {
                  // Assumption: All entries should suffice as references 
                  // when passing to this.open as reasons/proof of validity of opening the store
                  const oneEntry = entries[0].entry;

                  await this.open(Address.parse(paddress), { replicationTopic, directory: this.directory, entryToReplicate: oneEntry, verifyCanOpen: true, identity: this.identity, minReplicas: msg.minReplicas })
                }
                catch (error) {
                  if (error instanceof AccessError) {
                    return
                  }
                  throw error; // unexpected
                }
                break;
              }
            }
          }


          const programInfo = this.programs.get(replicationTopic)?.get(paddress)!;
          const storeInfo = programInfo.program.allStoresMap.get(storeIndex);
          if (!storeInfo) {
            throw new Error("Missing store info, which was expected to exist for " + replicationTopic + ", " + paddress + ", " + storeIndex)
          }
          const toMerge: EntryWithRefs<any>[] = [];

          await programInfo.program.initializationPromise; // Make sure it is ready
          for (const [gid, value] of groupByGid(heads)) {
            const leaders = leaderCache.get(gid) || await this.findLeaders(replicationTopic, programAddress, isReplicating, gid, programInfo.minReplicas.value);
            const isLeader = leaders.find((l) => l === this.id.toString());
            if (!isLeader) {
              continue;
            }
            value.forEach((head) => {
              toMerge.push(head);
            })

          }
          if (toMerge.length > 0) {
            const store = programInfo.program.allStoresMap.get(storeIndex);
            if (!store) {
              throw new Error("Unexpected, missing store on sync")
            }
            await store.sync(toMerge);

          }
        }
        logger.debug(`Received ${heads.length} heads for '${paddress}/${storeIndex}':\n`, JSON.stringify(heads.map(e => e.entry.hash), null, 2))
      }

      else if (msg instanceof KeyResponseMessage) {
        await recieveKeys(msg, (keys) => {
          return Promise.all(keys.map((key) => this.keystore.saveKey(key)))
        })

      }
      else if (msg instanceof ExchangeSwarmMessage) {
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

        /*    if (!await checkTrustedSender(message.address, false)) { TODO, how to make this DDOS resistant?
             return;
           }
    */
        msg.info.forEach(async (info) => {
          if (info.id === this.id.toString()) {
            return;
          }
          const suffix = '/p2p/' + info.id;
          this._ipfs.swarm.connect(multiaddr(info.address.toString() + (info.address.indexOf(suffix) === -1 ? suffix : '')));
        })
      }
      else if (msg instanceof RequestKeyMessage) {

        if (!peer) {
          logger.error("Execting a sigmed pubsub message")
          return;
        }

        if (!sender) {
          logger.info("Expecing sender when recieving key info")
          return;
        }

        if (msg.condition instanceof RequestKeysByAddress) {
          if (!await checkTrustedSender(msg.condition.address, true)) {
            return;
          }
          const canExchangeKey: KeyAccessCondition = (key) => key.group === (msg.condition as RequestKeysByAddress<any>).address;

          /**
           * Someone is requesting X25519 Secret keys for me so that they can open encrypted messages (and encrypt)
           * 
           */

          const send = (data: Uint8Array) => this._ipfs.pubsub.publish(DirectChannel.getTopic([peer.toString()]), data);
          await exchangeKeys(send, msg, canExchangeKey, this.keystore, this.identity, this.encryption)
          logger.debug(`Exchanged keys`)
        }

      }


      else {
        throw new Error("Unexpected message")
      }
    } catch (e: any) {
      logger.error(e)
    }
  }


  async _onPeerConnected(replicationTopic: string, peer: string) {
    logger.debug(`New peer '${peer}' connected to '${replicationTopic}'`)
    try {


      // determine if we should open a channel (we are replicating a store on the topic + a weak check the peer is trusted)
      const programs = this.programs.get(replicationTopic);
      if (programs) {

        // Should subscription to a replication be a proof of "REPLICATING?"
        const initializeAsReplicator = async () => {
          await exchangeSwarmAddresses((data) => this._ipfs.pubsub.publish(replicationTopic, data), this.identity, peer, await this._ipfs.swarm.peers(), this._getNetwork(replicationTopic), this.localNetwork)
          await this.getChannel(peer, replicationTopic); // always open a channel, and drop channels if necessary (not trusted) (TODO)
        }
        if (programs.size === 0) {
          // we are subscribed to replicationTopic, but we have not opened any store, this "means" 
          // that we are intending to replicate data for this topic 
          await initializeAsReplicator();
          return;
        }

        for (const [_storeAddress, programAndStores] of programs.entries()) {
          for (const [_, store] of programAndStores.program.allStoresMap) {
            if (store.replicate) {
              // create a channel for sending/receiving messages

              await initializeAsReplicator();
              return;

              // Creation of this channel here, will make sure it is created even though a head might not be exchangee

            }
            else {
              // If replicate false, we are in write mode. Means we should exchange all heads 
              // Because we dont know anything about whom are to store data, so we assume all peers might have responsibility
              const send = (data: Uint8Array) => this._ipfs.pubsub.publish(DirectChannel.getTopic([peer.toString()]), data);
              await exchangeHeads(send, store, programAndStores.program, store.oplog.heads, replicationTopic, false, this.limitSigning ? undefined : this.identity);

            }
          }

        }
      }
    } catch (error: any) {
      logger.error("Unexpected error in _onPeerConnected callback: " + error.toString())
      throw error;
    }
  }

  /**
   * When a peers join the networkk and want to participate the leaders for particular log subgraphs might change, hence some might start replicating, might some stop
   * This method will go through my owned entries, and see whether I should share them with a new leader, and/or I should stop care about specific entries
   * @param channel
   */
  async replicationReorganization(modifiedChannel: DirectChannel) {
    const connections = this._directConnections.get(modifiedChannel.recieverId.toString());
    if (!connections) {
      logger.error("Missing direct connection to: " + modifiedChannel.recieverId.toString());
      return;
    }

    for (const replicationTopic of connections.dependencies) {
      const programs = this.programs.get(replicationTopic);
      if (programs) {
        for (const programInfo of programs.values()) {
          for (const [_, store] of programInfo.program.allStoresMap) {
            const heads = store.oplog.heads;
            const groupedByGid = groupByGid(heads);
            for (const [gid, entries] of groupedByGid) {
              if (entries.length === 0) {
                continue; // TODO maybe close store?
              }

              const oldPeersSet = this._gidPeersHistory.get(gid);
              const newPeers = await this.findReplicators(replicationTopic, programInfo.program.address.toString(), store.replicate, gid, programInfo.minReplicas.value);

              for (const newPeer of newPeers) {
                if (!oldPeersSet?.has(newPeer) && newPeer !== this.id.toString()) { // second condition means that if the new peer is us, we should not do anything, since we are expecting to recieve heads, not send

                  // send heads to the new peer
                  const channel = this._directConnections.get(newPeer)?.channel;
                  if (!channel) {

                    logger.error("Missing channel when reorg to peer: " + newPeer.toString())
                    continue
                  }

                  await exchangeHeads(async (message) => {
                    await channel.send(message);
                  }, store, programInfo.program, entries, replicationTopic, true, this.limitSigning ? undefined : this.identity)
                }
              }

              if (!newPeers.find(x => x === this.id.toString())) {
                // delete entries since we are not suppose to replicate this anymore
                // TODO add delay? freeze time? (to ensure resiliance for bad io)
                store.oplog.removeAll(entries);

                // TODO if length === 0 maybe close store? 
              }
              this._gidPeersHistory.set(gid, new Set(newPeers))

            }
          }
        }
      }

    }
  }

  async join(program: VPC) {

    // Will be rejected by peers if my identity is not trusted
    // (this will sign our IPFS ID with our client Ed25519 key identity, if peers do not trust our identity, we will be rejected)
    await program.network.add(new IPFSAddress({ address: (await this.ipfs.id()).id.toString() }))
  }

  async getChannel(peer: string, fromTopic: string): Promise<DirectChannel | undefined> {

    // TODO what happens if disconnect and connection to direct connection is happening
    // simultaneously
    const getDirectConnection = (peer: string) => this._directConnections.get(peer)?._channel

    let channel = getDirectConnection(peer)
    if (!channel) {
      try {
        logger.debug(`Create a channel to ${peer}`)
        channel = await DirectChannel.open(this._ipfs, peer, this._onMessage.bind(this), {
          onPeerLeaveCallback: (channel) => {

            // First modify direct connections
            this._directConnections.get(channel.recieverId.toString())?.close(channel.recieverId.toString())

            // Then perform replication reorg
            this.replicationReorganization(channel);
          },
          onNewPeerCallback: (channel) => {

            // First modify direct connections
            if (!this._directConnections.has(channel.recieverId.toString())) {
              this._directConnections.set(channel.recieverId.toString(), new SharedChannel(channel, new Set([fromTopic])));
            }
            else {
              this._directConnections.get(channel.recieverId.toString())?.dependencies.add(fromTopic);
            }

            // Then perform replication reorg
            this.replicationReorganization(channel);
          }
        })
        logger.debug(`Channel created to ${peer}`)
      } catch (e: any) {
        logger.error(e)
        return undefined;
      }
    }

    // Wait for the direct channel to be fully connected
    try {
      let cancel = false;
      setTimeout(() => {
        cancel = true;
      }, 20 * 1000) // 20s timeout

      const connected = await channel.connect({ isClosed: () => cancel || this._disconnected || this._disconnecting })
      if (!connected) {
        return undefined; // failed to create channel
      }
    } catch (error) {

      if (this._disconnected || this._disconnecting) {
        return // its ok
      }
      throw error; // unexpected
    }
    logger.debug(`Connected to ${peer}`)

    return channel;
  }



  // Callback when a store was closed
  async _onClose(program: Program, db: Store<any>, replicationTopic: string) { // TODO Can we really close a this.programs, either we close all stores in the replication topic or none

    const programAddress = program.address?.toString();

    logger.debug(`Close ${programAddress}/${db.id}`)

    // Unsubscribe from pubsub
    await this._replicationTopicSubscriptions.get(replicationTopic)?.close(db.id);


    const dir = db && db._options.directory ? db._options.directory : this.directory
    const cache = this.caches[dir]

    if (cache && cache.handlers.has(db.id)) {
      cache.handlers.delete(db.id)
      if (!cache.handlers.size) {
        await cache.cache.close()
      }
    }
  }
  async _onProgamClose(program: Program, replicationTopic: string) {

    const programAddress = program.address?.toString();
    if (programAddress) {
      this.programs.get(replicationTopic)?.delete(programAddress);
    }
    const otherStoresUsingSameReplicationTopic = this.programs.get(replicationTopic)
    // close all connections with this repplication topic if this is the last dependency
    const isLastStoreForReplicationTopic = otherStoresUsingSameReplicationTopic?.size === 0;
    if (isLastStoreForReplicationTopic) {

      for (const [key, connection] of this._directConnections) {
        await connection.close(replicationTopic);
        // Delete connection from thing

        // TODO what happens if we close a store, but not its direct connection? we should open direct connections and make it dependenct on the replciation topic
      }
    }
  }

  async _onDrop(db: Store<any>) { }


  async addProgram(replicationTopic: string, program: Program, minReplicas: MinReplicas): Promise<ProgramWithMetadata> {
    if (!this.programs.has(replicationTopic)) {
      this.programs.set(replicationTopic, new Map());
    }
    if (!this.programs.has(replicationTopic)) {
      throw new Error("Unexpected behaviour")
    }

    const programAddress = program.address?.toString();
    if (!programAddress) {
      throw new Error("Missing program address");
    }
    const existingProgramAndStores = this.programs.get(replicationTopic)?.get(programAddress);
    if (!!existingProgramAndStores && existingProgramAndStores.program !== program) { // second condition only makes this throw error if we are to add a new instance with the same address
      throw new Error(`Program at ${replicationTopic} is already created`)
    }
    const p = {
      program,
      minReplicas
    };
    this.programs.get(replicationTopic)?.set(programAddress, p);
    return p;

  }

  _getPeersLRU: LRU<string, Promise<PeerInfoWithMeta[]>> = new LRU({ max: 500, ttl: WAIT_FOR_PEERS_TIME })

  getPeersOnTopic(topic: string): string[] {
    const ret: string[] = [];
    for (const [k, v] of this._directConnections) {
      if (v.dependencies.has(topic)) {
        ret.push(k);
      }
    }
    return ret;
  }

  /**
  * An intentionally imperfect leader rotation routine
  * @param slot, some time measure
  * @returns 
  */
  isLeader(leaders: string[]): boolean {
    return !!(leaders.find(id => id === this.id.toString()))
  }

  findReplicators(replicationTopic: string, address: string, replicating: boolean, gid: string, minReplicas: number): Promise<string[]> {
    return this.findLeaders(replicationTopic, address, replicating, gid, minReplicas);
  }


  async findLeaders(replicationTopic: string, address: string, replicating: boolean, slot: { toString(): string }, numberOfLeaders: number/* , addPeers: string[] = [], removePeers: string[] = [] */): Promise<string[]> {
    // Hash the time, and find the closest peer id to this hash
    const h = (h: string) => createHash('sha1').update(h).digest('hex');
    const slotHash = h(slot.toString())

    // Assumption: All peers wanting to replicate on topic has direct connections with me (Fully connected network)
    const allPeers: string[] = this.getPeersOnTopic(replicationTopic);

    // Assumption: Network specification is accurate
    // Replication topic is not an address we assume that the network allows all participants
    const network = this._getNetwork(address, replicationTopic);
    const isTrusted = (peer: string | PeerId) => network ? network.isTrusted(new IPFSAddress({ address: peer.toString() })) : true
    const peers = await Promise.all(allPeers.map(isTrusted))
      .then((results) => allPeers.filter((_v, index) => results[index]))

    const hashToPeer: Map<string, string> = new Map();
    const peerHashed: string[] = [];

    if (peers.length === 0) {
      return [this.id.toString()];
    }

    // Add self
    if (replicating) {
      peers.push(this.id.toString())
    }


    // Hash step
    peers.forEach((peer) => {
      const peerHash = h(peer + slotHash); // we do peer + slotHash because we want peerHashed.sort() to be different for each slot, (so that uniformly random pick leaders). You can see this as seed
      hashToPeer.set(peerHash, peer);
      peerHashed.push(peerHash);
    })
    numberOfLeaders = Math.min(numberOfLeaders, peerHashed.length);
    peerHashed.push(slotHash);

    // Choice step

    // TODO make more efficient
    peerHashed.sort((a, b) => a.localeCompare(b)) // sort is needed, since "getPeers" order is not deterministic
    let slotIndex = peerHashed.findIndex(x => x === slotHash);
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




  async subscribeToReplicationTopic(topic: string): Promise<void> {
    if (this._disconnected || this._disconnecting) {
      throw new Error("Disconnected")
    }

    if (!this.programs.has(topic)) {
      this.programs.set(topic, new Map())
    }
    if (!this._replicationTopicSubscriptions.has(topic)) {
      const topicMonitor = new IpfsPubsubPeerMonitor(this._ipfs.pubsub, topic, {
        onJoin: (peer) => {
          logger.debug(`Peer joined ${topic}:`)
          logger.debug(peer)
          this._onPeerConnected(topic, peer);
        },
        onLeave: (peer) => {
          logger.debug(`Peer ${peer} left ${topic}`)
        },
        onError: (e) => {
          logger.error(e)
        }

      })
      this._replicationTopicSubscriptions.set(topic, new SharedChannel(await new SharedIPFSChannel(this._ipfs, this.id, topic, this._onMessage.bind(this), topicMonitor).start()));

    }


  }
  hasSubscribedToReplicationTopic(topic: string): boolean {
    return this.programs.has(topic)
  }
  unsubscribeToReplicationTopic(topic: string | TrustedNetwork, id: string = '_'): Promise<boolean> | undefined {
    if (typeof topic !== 'string') {
      if (!topic.address) {
        throw new Error("Can not get network address from topic as TrustedNetwork")
      }
      topic = topic.address.toString();
    }

    return this._replicationTopicSubscriptions.get(topic as string)?.close(id);
  }



  async _requestCache(address: string, directory: string, existingCache?: Cache<any>) {
    const dir = directory || this.directory
    if (!this.caches[dir]) {
      const newCache = existingCache || await this._createCache(dir)
      this.caches[dir] = { cache: newCache, handlers: new Set() }
    }
    this.caches[dir].handlers.add(address)
    const cache = this.caches[dir].cache

    // "Wake up" the caches if they need it
    if (cache) await cache.open()

    return cache
  }



  /**
   * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
   * and to replicate heads (and updates) which is requested by another peer
   * @param store 
   * @param options 
   * @returns 
   */

  async open<S extends Program>(storeOrAddress: /* string | Address |  */S | Address | string, options: OpenStoreOptions = {}): Promise<S> {


    if (this._disconnected || this._disconnecting) {
      throw new Error("Can not open a store while disconnected")
    }
    const fn = async (): Promise<ProgramWithMetadata> => {
      // TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?

      if (typeof storeOrAddress === 'string' || storeOrAddress instanceof Address) {
        storeOrAddress = storeOrAddress instanceof Address ? storeOrAddress : Address.parse(storeOrAddress);

        if (storeOrAddress.path) {
          throw new Error("Opening programs by subprogram addresses is currently unsupported")
        }
      }
      let program = storeOrAddress as S;

      if (storeOrAddress instanceof Address || typeof storeOrAddress === 'string') {
        try {
          program = await Program.load(this._ipfs, storeOrAddress, options) as any as S // TODO fix typings
          if (program instanceof Program === false) {
            throw new Error(`Failed to open program because program is of type ${program.constructor.name} and not ${Program.name}`);
          }
        } catch (error) {
          logger.error("Failed to load store with address: " + storeOrAddress.toString());
          throw error;
          ;
        }
      }

      await program.save(this.ipfs);
      let programAddress = program.address?.toString()!;

      let definedReplicationTopic: string = (options.replicationTopic || programAddress)!;
      if (!definedReplicationTopic) {
        throw new Error("Replication topic is undefined")
      }
      if (programAddress) {
        const existingProgram = this.programs.get(definedReplicationTopic)?.get(programAddress)
        if (existingProgram) {
          return existingProgram;
        }
      }

      try {

        logger.debug('open()')

        let pstores = this.programs.get(definedReplicationTopic);
        if (programAddress && (!pstores || !pstores.has(programAddress)) && options.verifyCanOpen) {
          // open store if is leader and sender is trusted
          let senderCanOpen: boolean = false;

          if (!program.owner) {
            // can open is is trusted by netwoek?
            senderCanOpen = await this._canOpenProgram(programAddress, definedReplicationTopic, options.entryToReplicate);
          }
          else if (options.entryToReplicate) {

            let ownerAddress = Address.parse(program.owner);
            let ownerProgramRootAddress = ownerAddress.root();
            let ownerProgram: AbstractProgram | undefined = this.programs.get(definedReplicationTopic)?.get(ownerProgramRootAddress.toString())?.program;
            if (ownerAddress.path) {
              ownerProgram = ownerProgram?.subprogramsMap.get(ownerAddress.path.index)
            }
            if (!ownerProgram) {
              throw new AccessError("Failed to find owner program")
            }
            // TOOD make typesafe
            const csp = ((ownerProgram as Program) as any as CanOpenSubPrograms)
            if (!csp.canOpen) {
              senderCanOpen = false
            }
            else {
              senderCanOpen = await csp.canOpen(program, options.entryToReplicate);
            }
          }

          if (!senderCanOpen) {
            throw new AccessError('Failed to open program because request is not trusted');
          }
        }


        options = Object.assign({ localOnly: false, create: false }, options)
        logger.debug(`Open database '${program.constructor.name}`)


        if (!options.encryption) {
          options.encryption = encryptionWithRequestKey(this.identity, this.keystore, this._waitForKeysTimeout ? (key) => this.requestAndWaitForKeys(definedReplicationTopic, programAddress, new RequestKeysByKey<(Ed25519Keypair | X25519Keypair)>({
            key
          })) : undefined)
        }

        await program.init(this._ipfs, options.identity || this.identity, {
          replicationTopic: definedReplicationTopic,
          onClose: () => this._onProgamClose(program, definedReplicationTopic!),
          onDrop: () => this._onProgamClose(program, definedReplicationTopic!),

          store: {
            replicate: true,
            ...options,
            resolveCache: (store) => {
              const programAddress = program.address?.toString();
              if (!programAddress) {
                throw new Error("Unexpected");
              }
              return new Cache(this.cache._store.sublevel(path.join(programAddress, 'store', store.id)))
            },
            onClose: async (store) => {
              await this._onClose(program, store, definedReplicationTopic)
              if (options.onClose) {
                return options.onClose(store);
              }
              return;
            },
            onDrop: async (store) => {
              await this._onDrop(store)
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
              await this.onWrite(program)(store, entry, definedReplicationTopic)
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
            }

          }
        });

        const resolveCache = async (address: Address) => {
          const cache = await this._requestCache(address.toString(), options.directory || this.directory)
          const haveDB = await this._haveLocalData(cache, address.toString())
          logger.debug((haveDB ? 'Found' : 'Didn\'t find') + ` database '${address}'`)
          if (options.localOnly && !haveDB) {
            logger.warn(`Database '${address}' doesn't exist!`)
            throw new Error(`Database '${address}' doesn't exist!`)
          }
          return cache;
        }
        await resolveCache(program.address!);

        const pm = await this.addProgram(definedReplicationTopic, program, options.minReplicas || new AbsolutMinReplicas(this._minReplicas));
        await this.subscribeToReplicationTopic(definedReplicationTopic);
        return pm
      } catch (error) {
        throw error;
      }
    }
    const openStore = await this._openProgramQueue.add(fn);
    if (!openStore?.program.address) {
      throw new Error("Unexpected")
    }
    return openStore.program as S
    /*  } */

  }

  _getNetwork(address: string | Address, replicationTopic?: string): TrustedNetwork | undefined {
    const a = typeof address === 'string' ? address : address.toString();
    if (!replicationTopic)
      for (const [k, v] of this.programs.entries()) {
        if (v.has(a)) {
          replicationTopic = k;
        }
      }
    if (!replicationTopic) {
      return;
    }
    const parsedAddress = address instanceof Address ? address : Address.parse(address);
    const asPermissioned = this.programs.get(replicationTopic)?.get(parsedAddress.root().toString())?.program
    if (!asPermissioned || !isVPC(asPermissioned)) {
      return;
    }
    return asPermissioned.network;
  }


  /**
   * Check if we have the database, or part of it, saved locally
   * @param  {[Cache]} cache [The OrbitDBCache instance containing the local data]
   * @param  {[Address]} dbAddress [Address of the database to check]
   * @return {[Boolean]} [Returns true if we have cached the db locally, false if not]
   */
  async _haveLocalData(cache: Cache<any>, id: string) {
    if (!cache) {
      return false
    }

    const addr = id;
    const data = await cache.get(path.join(addr, '_manifest'))
    return data !== undefined && data !== null
  }

}
