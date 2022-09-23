import path from 'path'
import { Address, IStoreOptions, ResourceOptions, Store, StoreLike, StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store'
import { PubSub, Subscription } from '@dao-xyz/orbit-db-pubsub'
import Logger from 'logplease'
const logger = Logger.create('orbit-db')
import { IPFS as IPFSInstance } from 'ipfs-core-types';
import Cache from '@dao-xyz/orbit-db-cache'
import { BoxKeyWithMeta, Keystore, KeyWithMeta, SignKeyWithMeta, WithType } from '@dao-xyz/orbit-db-keystore'
import { isDefined } from './is-defined'
import { Level } from 'level';
import { exchangeHeads, ExchangeHeadsMessage, RequestHeadsMessage } from './exchange-heads'
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { serialize, deserialize } from '@dao-xyz/borsh'
import { Message } from './message'
import { getOrCreateChannel } from './channel'
import { exchangeKeys, KeyResponseMessage, KeyAccessCondition, recieveKeys, requestAndWaitForKeys, RequestKeyMessage, RequestKeyCondition, RequestKeysByKey, RequestKeysByReplicationTopic } from './exchange-keys'
import { DecryptedThing, EncryptedThing, MaybeEncrypted, PublicKeyEncryption } from '@dao-xyz/encryption-utils'
import { X25519PublicKey } from 'sodium-plus'
import LRU from 'lru';
import { DirectChannel } from '@dao-xyz/ipfs-pubsub-1on1'
import { encryptionWithRequestKey, replicationTopicEncryptionWithRequestKey } from './encryption'
import { Ed25519PublicKeyData, PublicKey } from '@dao-xyz/identity';
import { MaybeSigned, SignatureWithKey } from '@dao-xyz/identity';
import { EMIT_HEALTHCHECK_INTERVAL, exchangePeerInfo, HeapSizeRequirement, ReplicatorInfo, PeerInfoWithMeta, RequestReplicatorInfo, requestPeerInfo } from './exchange-replication'
import { createHash } from 'crypto'
import isNode from 'is-node';
import { delay, waitForAsync } from '@dao-xyz/time'
let v8 = undefined;
if (isNode) {
  v8 = require('v8');
}

/* let AccessControllersModule = AccessControllers;
 */
Logger.setLogLevel('ERROR')

const defaultTimeout = 30000 // 30 seconds
const STORE_MIN_HEAP_SIZE = 50 * 1000;

const MIN_REPLICAS = 2;

export type StoreOperations = 'write' | 'all'
export type Storage = { createStore: (string) => any }
export type CreateOptions = {
  AccessControllers?: any, cache?: Cache, keystore?: Keystore, peerId?: string, offline?: boolean, directory?: string, storage?: Storage, broker?: any, minReplicas?: number, heapsizeLimitForForks?: number, waitForKeysTimout?: number, canAccessKeys?: KeyAccessCondition, isTrusted?: (key: PublicKey, replicationTopic: string) => Promise<boolean>
};
export type CreateInstanceOptions = CreateOptions & { publicKey?: PublicKey, sign?: (data: Uint8Array) => Promise<Uint8Array>, id?: string };
export class OrbitDB {

  _ipfs: IPFSInstance;

  _pubsub: PubSub;
  _directConnections: { [key: string]: { channel: DirectChannel, dependencies: Set<string> } };

  publicKey: PublicKey;
  sign: (data: Uint8Array) => Promise<Uint8Array>;
  id: string;
  directory: string;
  storage: Storage;
  caches: any;
  keystore: Keystore;
  minReplicas: number;
  heapsizeLimitForForks: number = 1000 * 1000 * 1000;
  stores: { [topic: string]: { [address: string]: StoreLike<any> } };

  _subscribeForReplication = new Set<string>();
  _waitForKeysTimeout = 10000;
  _keysInflightMap: Map<string, Promise<any>> = new Map(); // TODO fix types
  _keyRequestsLRU: LRU = new LRU({ max: 100, maxAge: 10000 });
  /*   _replicationTopicJobs: Map<string, { controller: AbortController }> = new Map(); */
  _peerInfoLRU: LRU = new LRU({ max: 1000, maxAge: EMIT_HEALTHCHECK_INTERVAL * 4 });

  //_peerInfoMap: Map<string, Map<string, Set<string>>> // peer -> store -> heads


  isTrusted: (key: PublicKey, replicationTopic: string) => Promise<boolean>
  canAccessKeys: KeyAccessCondition


  constructor(ipfs: IPFSInstance, publicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: CreateOptions = {}) {
    if (!isDefined(ipfs)) { throw new Error('IPFS required') }
    if (!isDefined(publicKey)) { throw new Error('public key required') }
    if (!isDefined(sign)) { throw new Error('sign function required') }

    this._ipfs = ipfs
    this.publicKey = publicKey
    this.sign = sign;
    this.id = options.peerId
    this._pubsub = !options.offline
      ? new (
        options.broker ? options.broker : PubSub
      )(this._ipfs, this.id)
      : null
    this.directory = options.directory || './orbitdb'
    this.storage = options.storage
    this._directConnections = {}
    this.stores = {}
    this.caches = {}
    this.minReplicas = options.minReplicas || MIN_REPLICAS;
    this.caches[this.directory] = { cache: options.cache, handlers: new Set() }
    this.keystore = options.keystore
    this.canAccessKeys = options.canAccessKeys || (() => Promise.resolve(false));
    this.isTrusted = options.isTrusted || (() => Promise.resolve(true))
    if (options.waitForKeysTimout) {
      this._waitForKeysTimeout = options.waitForKeysTimout;
    }
    this.heapsizeLimitForForks = options.heapsizeLimitForForks;
    // AccessControllers module can be passed in to enable
    // testing with orbit-db-access-controller
    /*     AccessControllersModule = options.AccessControllers || AccessControllers
     */
  }

  get cache() { return this.caches[this.directory].cache }

  get identity(): PublicKey {
    return this.publicKey;
  }
  get encryption(): PublicKeyEncryption {
    return encryptionWithRequestKey(this.publicKey, this.keystore)
  }

  async requestAndWaitForKeys<T extends KeyWithMeta>(replicationTopic: string, condition: RequestKeyCondition<T>): Promise<T[]> {
    const promiseKey = condition.hashcode;
    const existingPromise = this._keysInflightMap.get(promiseKey);
    if (existingPromise) {
      return existingPromise
    }

    let lruCache = this._keyRequestsLRU.get(promiseKey);
    if (lruCache !== undefined) {
      return lruCache;
    }

    const promise = new Promise<T[] | undefined>((resolve, reject) => {
      const send = (message: Uint8Array) => this._pubsub.publish(replicationTopic, message)
      requestAndWaitForKeys(condition, send, this.keystore, this.publicKey, this.sign, this._waitForKeysTimeout).then((results) => {
        if (results?.length > 0) {
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
    const result = await promise;
    this._keyRequestsLRU.set(promiseKey, result ? result : null);
    this._keysInflightMap.delete(promiseKey);
    return result;
  }

  async decryptedSignedThing(data: Uint8Array): Promise<DecryptedThing<MaybeSigned<Uint8Array>>> {
    const signedMessage = await (new MaybeSigned({ data })).sign(await this.getSigner());
    return new DecryptedThing({
      data: serialize(signedMessage)
    })
  }

  async enryptedSignedThing(data: Uint8Array, reciever: X25519PublicKey): Promise<EncryptedThing<MaybeSigned<Uint8Array>>> {
    const signedMessage = await (new MaybeSigned({ data })).sign(await this.getSigner());
    return new DecryptedThing<MaybeSigned<Uint8Array>>({
      data: serialize(signedMessage)
    }).encrypt(reciever)
  }

  replicationTopicEncryption(): StorePublicKeyEncryption {
    return replicationTopicEncryptionWithRequestKey(this.identity, this.keystore, (key, replicationTopic) => this.requestAndWaitForKeys<BoxKeyWithMeta>(replicationTopic, new RequestKeysByKey<BoxKeyWithMeta>({
      key: new Uint8Array(key.getBuffer()),
      type: BoxKeyWithMeta
    })))
  }


  async getEncryptionKey(replicationTopic: string): Promise<BoxKeyWithMeta | undefined> {
    // v0 take some recent
    const keys = (await this.keystore.getKeys(replicationTopic, BoxKeyWithMeta));
    let key = keys[0];
    if (!key) {
      const keys = await this.requestAndWaitForKeys(replicationTopic, new RequestKeysByReplicationTopic({
        replicationTopic,
        type: BoxKeyWithMeta
      }))
      key = keys ? keys[0] : undefined;
    }
    return key;
  }


  static async createInstance(ipfs, options: CreateInstanceOptions = {}) {
    if (!isDefined(ipfs)) { throw new Error('IPFS is a required argument. See https://github.com/orbitdb/orbit-db/blob/master/API.md#createinstance') }

    if (options.offline === undefined) {
      options.offline = false
    }

    if (options.offline && !options.id) {
      throw new Error('Offline mode requires passing an `id` in the options')
    }

    let id: string = undefined;
    if (options.id || options.offline) {

      id = options.id;
    }
    else {
      const idFromIpfs: string | { toString: () => string } = (await ipfs.id()).id;
      if (typeof idFromIpfs !== 'string') {
        id = idFromIpfs.toString(); //  ipfs 57+ seems to return an id object rather than id
      }
      else {
        id = idFromIpfs
      }
    }

    if (!options.directory) { options.directory = './orbitdb' }

    if (!options.storage) {

      // Create default `level` store
      options.storage = {
        createStore: (path): Level => {
          return new Level(path)
        }
      };
    }



    /* if (options.identity && options.identity.provider.keystore) {
      options.keystore = options.identity.provider.keystore
    } */

    if (!options.keystore) {
      const keystorePath = path.join(options.directory, id, '/keystore')
      const keyStorage = await options.storage.createStore(keystorePath)
      options.keystore = new (Keystore as any)(keyStorage) // TODO fix typings
    }
    let publicKey: PublicKey = undefined;
    let sign: (data: Uint8Array) => Promise<Uint8Array> = undefined;
    if (!!options.publicKey != !!options.sign) {
      throw new Error("Either both publicKey and sign function has to be provided, or neither")
    }
    if (options.publicKey) {
      publicKey = options.publicKey;
      sign = options.sign;
    }
    else {
      const signKey = await options.keystore.createKey(Buffer.from(id), SignKeyWithMeta);
      publicKey = new Ed25519PublicKeyData({
        publicKey: signKey.publicKey
      });
      sign = (data) => Keystore.sign(data, signKey);
    }

    /* const signKey = options.signKey || await options.keystore.createKey(Buffer.from(id), SignKeyWithMeta); */
    /* if (!options.identity) {
      options.identity = await Identities.createIdentity({
        id: new Uint8Array(Buffer.from(id)),
        keystore: options.keystore
      })
    } */


    if (!options.cache) {
      const cachePath = path.join(options.directory, id, '/cache')
      const cacheStorage = await options.storage.createStore(cachePath)
      options.cache = new Cache(cacheStorage)
    }

    const finalOptions = Object.assign({}, options, { peerId: id })
    return new OrbitDB(ipfs, publicKey, sign, finalOptions)
  }


  async disconnect() {
    // Close a direct connection and remove it from internal state
    this._subscribeForReplication.clear();

    const removeDirectConnect = e => {
      this._directConnections[e].channel.close()
      delete this._directConnections[e]
    }

    // Close all direct connections to peers
    Object.keys(this._directConnections).forEach(removeDirectConnect)

    // Disconnect from pubsub
    if (this._pubsub) {
      await this._pubsub.disconnect()
    }

    // close keystore
    await this.keystore.close()

    // Close all open databases
    for (const [key, dbs] of Object.entries(this.stores)) {
      await Promise.all(Object.values(dbs).map(db => db.close()));
      delete this.stores[key]
    }

    const caches = Object.keys(this.caches)
    for (const directory of caches) {
      await this.caches[directory].cache.close()
      delete this.caches[directory]
    }

    // Remove all databases from the state
    this.stores = {}
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
  _onWrite<T>(topic: string, address: string, _entry: Entry<T>, heads: Entry<T>[]) {
    if (!heads) {
      throw new Error("'heads' not defined")
    }
    if (this._pubsub && heads.length > 0) {
      this.decryptedSignedThing(serialize(new ExchangeHeadsMessage({
        address,
        heads,
        replicationTopic: topic
      }))).then((thing) => {
        this._pubsub.publish(topic, serialize(thing))
      })
    }
  }

  // Callback for receiving a message from the network
  async _onMessage(onMessageTopic: string, data: Uint8Array, peer: string) {
    try {

      const maybeEncryptedMessage = deserialize(data, MaybeEncrypted) as MaybeEncrypted<MaybeSigned<Message>>
      const decrypted = await maybeEncryptedMessage.init(this.encryption).decrypt()
      const signedMessage = decrypted.getValue(MaybeSigned);
      await signedMessage.verify();
      const msg = signedMessage.getValue(Message);
      const sender: PublicKey | undefined = signedMessage.signature?.publicKey;
      const checkTrustedSender = async (replicationTopic: string): Promise<boolean> => {
        let isTrusted = false;
        if (sender) {
          isTrusted = await this.isTrusted(sender, replicationTopic);
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

        const { replicationTopic, address, heads } = msg
        if (!(await checkTrustedSender(replicationTopic))) {
          return;
        }
        const stores = this.stores[replicationTopic]
        if (heads && stores) {
          let isLeaderResolver = () => this.isLeader(replicationTopic, address, Buffer.from(signedMessage.signature.signature).toString('base64'), MIN_REPLICAS)
          if (!stores[address]) {
            // open store if is leader
            const isLeader = await isLeaderResolver();
            if (isLeader) {
              // open store since it is not open
              await this.open(Address.parse(address), { replicationTopic })
              isLeaderResolver = () => Promise.resolve(true);
            }
            else {
              return; // is not leader, so we should not open the store
            }

          }
          for (const [storeAddress, store] of Object.entries(stores)) {
            if (store) {
              if (storeAddress !== address) {
                continue // this messages was intended for another store
              }
              if (heads.length > 0) {
                if (!store.replicate) {
                  // if we are only to write, then only care about others clock
                  for (const head of heads) {
                    head.init({
                      encoding: store.oplog._encoding,
                      encryption: store.oplog._encryption
                    })
                    const clock = await head.getClock();
                    store.oplog.mergeClock(clock)
                  }
                }
                else {
                  // Full sync
                  await store.sync(heads, () => isLeaderResolver());

                }
              }
              store.events.emit('peer.exchanged', peer, address, heads)
            }
          }
        }

        logger.debug(`Received ${heads.length} heads for '${address}':\n`, JSON.stringify(heads.map(e => e.hash), null, 2))
      }
      else if (msg instanceof RequestHeadsMessage) {
        /**
         * I have recieved a message urging me to share my heads
         * so that another peer can clone my log and join with theirs
         */
        const { replicationTopic, address } = msg
        if (!(await checkTrustedSender(replicationTopic))) {
          return;
        }
        const channel = await this.getChannel(peer, replicationTopic);
        await exchangeHeads(channel, replicationTopic, (address: string) => this.stores[address], await this.getSigner());
        logger.debug(`Received exchange heades request for topic: ${replicationTopic}, address: ${address}`)
      }
      else if (msg instanceof KeyResponseMessage) {
        await recieveKeys(msg, (keys) => {
          const keysToSave = keys.filter(key => key instanceof SignKeyWithMeta || key instanceof BoxKeyWithMeta);
          return Promise.all(keysToSave.map((key) => this.keystore.saveKey(key)))
        })
        /*         
        this._keysInFlightResolver?.();
         */
      }
      else if (msg instanceof RequestKeyMessage) {

        /**
         * Someone is requesting X25519 Secret keys for me so that they can open encrypted messages (and encrypt)
         * 
         */

        const channel = await this.getChannel(peer, onMessageTopic);
        const getKeysByGroup = <T extends KeyWithMeta>(group: string, type: WithType<T>) => this.keystore.getKeys(group, type);
        const getKeysByPublicKey = (key: Uint8Array) => this.keystore.getKeyById(key);

        await exchangeKeys(channel, msg, sender, this.canAccessKeys, getKeysByPublicKey, getKeysByGroup, await this.getSigner(), this.encryption)
        logger.debug(`Exchanged keys`)
      }
      else if (msg instanceof RequestReplicatorInfo) {

        if (!(await checkTrustedSender(msg.replicationTopic))) {
          return;
        }
        // if supports store, return resp
        const store = this.stores[msg.replicationTopic]?.[msg.address];
        let hasHead = msg.head ? !!store.oplog._entryIndex.get(msg.head) : true;

        // TODO do direct channel repsonse?
        if (store && hasHead) {
          await exchangePeerInfo(msg.replicationTopic, store, (topic, message) => this._pubsub.publish(topic, message), await this.getSigner())
        }
      }
      else if (msg instanceof ReplicatorInfo) {

        if (!(await checkTrustedSender(msg.replicationTopic))) {
          return;
        }
        this._peerInfoLRU.set(sender.hashCode(), {
          peerInfo: msg,
          publicKey: sender
        } as PeerInfoWithMeta)
      }

      /*  else if (msg instanceof RequestReplication) {
 
         if (!this._subscribeForReplication.has(msg.replicationTopic)) {
           return;
         }
 
         if (!(await checkTrustedSender(msg.replicationTopic))) {
           return;
         }
         for (const r of msg.resourceRequirements) {
           if (!await r.ok(this)) {
             return; // does not fulfill criteria
           }
         }
         // TODO only leader open?
         await this.open(msg.store, { replicationTopic: msg.replicationTopic });
         if (msg.heads.length > 0) {
           await msg.store.sync(msg.heads, () => this.isLeader(msg.store, Buffer.from(data).toString('base64'), XXX))
         }
       } */

      else {
        throw new Error("Unexpected message")
      }
    } catch (e) {
      logger.error(e)
    }
  }


  // Callback for when a peer connected to a database
  async _onPeerConnected(replicationTopic: string, peer: string, subscription: Subscription) {
    logger.debug(`New peer '${peer}' connected to '${replicationTopic}'`)

    // create a channel for sending/receiving messages
    const channel = await this.getChannel(peer, subscription.topicMonitor.topic)

    const getStore = (topic: string) => this.stores[topic]

    // Exchange heads
    await exchangeHeads(channel, replicationTopic, getStore, await this.getSigner())

  }

  async getSigner() {
    return async (bytes) => {
      return {
        signature: await this.sign(bytes),
        publicKey: this.publicKey
      }
    }
  }


  async getChannel(peer: string, fromTopic: string) {
    // TODO what happens if disconnect and connection to direct connection is happening
    // simultaneously
    const getDirectConnection = (peer: string) => this._directConnections[peer]?.channel
    const _onChannelCreated = (channel: DirectChannel) => {
      this._directConnections[channel.recieverId] = {
        channel,
        dependencies: new Set([fromTopic])
      }
    }

    const handleMessage = (message: { data: Uint8Array }) => {
      this._onMessage(undefined, message.data, peer)
    }
    let channel = await getOrCreateChannel(this._ipfs, peer, getDirectConnection, handleMessage, _onChannelCreated);

    this._directConnections[channel.recieverId].dependencies.add(fromTopic);

    return channel;
  }



  // Callback when a database was closed
  async _onClose(db: Store<any>) {
    const address = db.address.toString()
    logger.debug(`Close ${address}`)

    // Unsubscribe from pubsub
    let subscriptionId = undefined;
    if (this._pubsub) {
      subscriptionId = await this._pubsub.unsubscribe(db.replicationTopic, db.id)
    }

    const dir = db && db.options.directory ? db.options.directory : this.directory
    const cache = this.caches[dir]

    if (cache && cache.handlers.has(address)) {
      cache.handlers.delete(address)
      if (!cache.handlers.size) {
        await cache.cache.close()
      }
    }

    delete this.stores[db.replicationTopic][address]

    const otherStoresUsingSameReplicationTopic = this.stores[db.replicationTopic]

    // close all connections with this repplication topic if this is the last dependency
    const isLastStoreForReplicationTopic = Object.keys(otherStoresUsingSameReplicationTopic).length === 0;
    if (isLastStoreForReplicationTopic) {

      /*   const cron = this._replicationTopicJobs.get(db.replicationTopic);
        if (cron) {
          cron.controller.abort();
          this._replicationTopicJobs.delete(db.replicationTopic);
        }
   */
      for (const [key, connection] of Object.entries(this._directConnections)) {
        connection.dependencies.delete(db.replicationTopic);
        if (connection.dependencies.size === 0) {
          await connection?.channel.close();
          delete this._directConnections[key];
        }
        // Delete connection from thing
      }
    }



  }

  async _onDrop(db: Store<any>) {
    const address = db.address.toString()
    const dir = db && db.options.directory ? db.options.directory : this.directory
    await this._requestCache(address, dir, db._cache)
  }

  async _onLoad(db: Store<any>) {
    const address = db.address.toString()
    const dir = db && db.options.directory ? db.options.directory : this.directory
    await this._requestCache(address, dir, db._cache)
    /*   this.addStore(db); */
  }


  /* addStore(store: Store<any>) {
    const storeAddress = store.address.toString();
    if (!storeAddress) { throw new Error("Address undefined") }
   
    const existingStore = this.stores[storeAddress];
    if (!!existingStore && existingStore !== store) { // second condition only makes this throw error if we are to add a new instance with the same address
      throw new Error(`Store at ${storeAddress} is already created`)
    }
    this.stores[storeAddress] = store;
  }
  */
  async addStore(store: StoreLike<any>) {
    const replicationTopic = store.replicationTopic;
    if (!this.stores[replicationTopic]) {
      this.stores[replicationTopic] = {};
    }

    const storeAddress = store.address.toString();
    const existingStore = this.stores[replicationTopic][storeAddress];
    if (!!existingStore && existingStore !== store) { // second condition only makes this throw error if we are to add a new instance with the same address
      throw new Error(`Store at ${replicationTopic}/${storeAddress} is already created`)
    }
    this.stores[replicationTopic][storeAddress] = store;

    /* if (!this._replicationTopicJobs.has(replicationTopic) && store.replicate) {
      const controller = new AbortController();
      const job = await createEmitHealthCheckJob({
        stores: () => Object.keys(this.stores[replicationTopic]),
        subscribingForReplication: (topic) => this._subscribeForReplication.has(topic)
      }, replicationTopic, (r, d) => this._pubsub.publish(r, d), () => this._ipfs.isOnline(), controller, await this.getSigner(), this.encryption);
      job();
      this._replicationTopicJobs.set(replicationTopic, {
        controller
      })
    } */
  }

  async getPeers(request: RequestReplicatorInfo, options: { waitForPeersTime?: number } = {}): Promise<PeerInfoWithMeta[]> {

    await this.subscribeToReplicationTopic(request.replicationTopic);
    await requestPeerInfo(request, (topic, message) => this._pubsub.publish(topic, message), await this.getSigner())
    await delay(options?.waitForPeersTime || EMIT_HEALTHCHECK_INTERVAL * 2);
    const caches: { value: PeerInfoWithMeta }[] = Object.values(this._peerInfoLRU.cache);
    const peersSupportingAddress = caches.filter(cache => cache.value.peerInfo.store === request.address).map(x => x.value)
    return peersSupportingAddress
  }

  /**
  * An intentionally imperfect leader rotation routine
  * @param slot, some time measure
  * @returns 
  */
  async isLeader(replicationTopic: string, address: string, slot: { toString(): string }, numberOfLeaders: number, options: { waitForPeersTime?: number } = {}): Promise<boolean> {
    // Hash the time, and find the closest peer id to this hash
    const h = (h: string) => createHash('sha1').update(h).digest('hex');
    const slotHash = h(slot.toString())


    const hashToPeer: Map<string, (OrbitDB | PeerInfoWithMeta)> = new Map();
    const peers: (OrbitDB | PeerInfoWithMeta)[] = await this.getPeers(new RequestReplicatorInfo({ address, replicationTopic }), options);
    if (peers.length == 0) {
      return false;
    }

    const peerHashed: string[] = [];
    peers.push(this)
    peers.forEach((peer) => {
      const peerHash = h(peer.publicKey.hashCode());
      hashToPeer.set(peerHash, peer);
      peerHashed.push(peerHash);
    })
    numberOfLeaders = Math.min(numberOfLeaders, peerHashed.length);

    peerHashed.push(slotHash);

    // TODO make more efficient
    peerHashed.sort((a, b) => a.localeCompare(b)) // sort is needed, since "getPeers" order is not deterministic
    let slotIndex = peerHashed.findIndex(x => x === slotHash);
    // we only step forward 1 step (ignoring that step backward 1 could be 'closer')
    // This does not matter, we only have to make sure all nodes running the code comes to somewhat the 
    // same conclusion (are running the same leader selection algorithm)
    for (let i = 0; i < numberOfLeaders; i++) {
      let nextIndex = slotIndex + 1 + i;
      if (nextIndex >= peerHashed.length)
        nextIndex = 0;
      const isLeader = hashToPeer.get(peerHashed[nextIndex]).publicKey.equals(this.identity)
      if (isLeader) {
        return true;
      }
    }

    return false;
    // better alg, 
    // convert slot into hash, find most "probable peer" 
  }


  /*  _requestingReplicationPromise: Promise<void>;
   async requestReplication(store: Store<any>, options: { heads?: Entry<any>[], replicationTopic?: string, waitForPeersTime?: number } = {}) {
     const replicationTopic = options?.replicationTopic || store.replicationTopic;
     if (!replicationTopic) {
       throw new Error("Missing replication topic for replication");
     }
     await this._requestingReplicationPromise;
     if (!store.address) {
       await store.save(this._ipfs);
     }
     const currentPeersCountFn = async () => (await this.getPeers(replicationTopic, store.address, options)).length
     const currentPeersCount = await currentPeersCountFn();
     this._requestingReplicationPromise = new Promise(async (resolve, reject) => {
       const signedThing = new DecryptedThing({
         data: await serialize(await (new MaybeSigned({
           data: serialize(new RequestReplication({
             replicationTopic,
             store,
             heads: options?.heads,
             resourceRequirements: [new HeapSizeRequirement({
               heapSize: BigInt(STORE_MIN_HEAP_SIZE)
             })]
           }))
         })).sign(await this.getSigner()))
       }) /// TODO add encryption?
 
       await this._pubsub.publish(replicationTopic, serialize(signedThing));
       await waitForAsync(async () => await currentPeersCountFn() >= currentPeersCount + 1, {
         timeout: (options?.waitForPeersTime || 5000) * 2,
         delayInterval: 50
       })
       resolve();
 
     })
     await this._requestingReplicationPromise;
   } */


  subscribeToReplicationTopic(topic: string, id: string = '_'): Promise<any> {
    if (!this.stores[topic]) {
      this.stores[topic] = {};
    }
    if (!this._pubsub._subscriptions[topic]) {
      return this._pubsub.subscribe(topic, id, this._onMessage.bind(this), {
        onNewPeerCallback: this._onPeerConnected.bind(this)
      })
    }

  }
  hasSubscribedToReplicationTopic(topic: string): boolean {
    return !!this._pubsub._subscriptions[topic]
  }
  unsubscribeToReplicationTopic(topic: string, id: string = '_'): Promise<any> {
    this._subscribeForReplication.delete(topic);
    if (this._pubsub._subscriptions[topic]) {
      return this._pubsub.unsubscribe(topic, id)
    }
  }

  subscribeForReplicationStart(topic: string): Promise<any> {
    this._subscribeForReplication.add(topic);
    return this.subscribeToReplicationTopic(topic);
  }

  subscribeForReplicationStop(topic: string): Promise<any> {
    this._subscribeForReplication.delete(topic);
    return this.unsubscribeToReplicationTopic(topic);

  }


  /* Create and Open databases */

  /*
    options = {
      accessController: { write: [] } // array of keys that can write to this database
      overwrite: false, // whether we should overwrite the existing database if it exists
    }
  */


  /*  directory?: string,
   onlyHash?: boolean,
   overwrite?: boolean,
   accessController?: any,
   create?: boolean,
   type?: string,
   localOnly?: boolean,
   replicationConcurrency?: number,
   replicate?: boolean,
   replicationTopic?: string | (() => string),
 
   encoding?: IOOptions<any>;
   encryption?: (keystore: Keystore) => StorePublicKeyEncryption; */
  /* async create<S extends StoreLike<any>>(store: S, options: {
    timeout?: number,
    identity?: Identity,
    cache?: Cache,



  } & IStoreOptions<any> = {}): Promise<S> {

    logger.debug('create()')
    logger.debug(`Creating database '${store.name}' as ${store.constructor.name}`)

    // Create the database address

    // TODO prevent double save (store is also saved on init)
    const dbAddress = await store.save(this._ipfs, { pin: true });

    if (!options.cache)
      options.cache = await this._requestCache(dbAddress.toString(), options.directory)

    // Check if we have the database locally
    const haveDB = await this._haveLocalData(options.cache, dbAddress)
    if (haveDB) { throw new Error(`Database '${dbAddress}' already exists!`) }

    // Save the database locally
    await this._addManifestToCache(options.cache, dbAddress)

    logger.debug(`Created database '${dbAddress}'`)

    // Open the database
    return this.open<S>(store, options)
  } */

  async _requestCache(address: string, directory: string, existingCache?: Cache) {
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


  _openStorePromise: Promise<StoreLike<any>>

  /**
   * Default behaviour of a store is only to accept heads that are forks (new roots) with some probability
   * and to replicate heads (and updates) which is requested by another peer
   * @param store 
   * @param options 
   * @returns 
   */
  async open<S extends StoreLike<any>>(storeOrAddress: /* string | Address |  */S | Address | string, options: {
    timeout?: number,
    publicKey?: PublicKey,
    sign?: (data: Uint8Array) => Promise<Uint8Array>,
    rejectIfAlreadyOpen?: boolean,

    /* cache?: Cache,
    directory?: string,
    accessController?: any,
    onlyHash?: boolean,
    create?: boolean,
    type?: string,
    localOnly?: boolean,
    replicationConcurrency?: number,
    replicate?: boolean,
    replicationTopic?: string | (() => string),
    encryption?: (keystore: Keystore) => StorePublicKeyEncryption; */
  } & IStoreOptions<any> = {}): Promise<S> {


    // TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?
    await this._openStorePromise;

    this._openStorePromise = new Promise<S | undefined>(async (resolve, reject) => {
      let store = storeOrAddress as S;
      if (typeof storeOrAddress === 'string') {
        storeOrAddress = Address.parse(storeOrAddress);
      }
      if (storeOrAddress instanceof Address) {
        try {
          store = await Store.load(this._ipfs, storeOrAddress as any as Address) as any as S // TODO fix typings
        } catch (error) {
          logger.error("Failed to load store with address: " + storeOrAddress.toString());
          reject(error);
        }
      }

      try {
        logger.debug('open()')

        options = Object.assign({ localOnly: false, create: false }, options)
        logger.debug(`Open database '${store}'`)

        const resolveCache = async (address: Address) => {
          const cache = await this._requestCache(address.toString(), options.directory)
          const haveDB = await this._haveLocalData(cache, address)
          logger.debug((haveDB ? 'Found' : 'Didn\'t find') + ` database '${address}'`)
          if (options.localOnly && !haveDB) {
            logger.warn(`Database '${address}' doesn't exist!`)
            throw new Error(`Database '${address}' doesn't exist!`)
          }

          if (!haveDB) {
            await this._addManifestToCache(cache, address)
          }
          return cache;
        }

        if (!options.encryption) {
          options.encryption = this.replicationTopicEncryption();
        }

        // Open the the database
        await store.init(this._ipfs, options.publicKey || this.publicKey, options.sign || this.sign, {
          replicate: true, ...options, ...{
            resolveCache,
            saveAndResolveStore: async (store: StoreLike<any>) => {
              const address = await store.save(this._ipfs);
              const r = Store.getReplicationTopic(address, options);
              const a = address.toString();
              const alreadyHaveStore = this.stores[r]?.[a];
              if (options.rejectIfAlreadyOpen) {
                new Error(`Store at ${r}/${a} is already created`)
              }
              return alreadyHaveStore || store;
            }
          },
          resourceOptions: options.resourceOptions || this.heapsizeLimitForForks ? { heapSizeLimit: () => this.heapsizeLimitForForks } : undefined,
          onClose: this._onClose.bind(this),
          onDrop: this._onDrop.bind(this),
          onLoad: this._onLoad.bind(this),
          onWrite: this._onWrite.bind(this),
          onOpen: async (store) => {

            // ID of the store is the address as a string
            await this.addStore(store)

            // Subscribe to pubsub to get updates from peers,
            // this is what hooks us into the message propagation layer
            // and the p2p network
            if (this._pubsub) {
              if (!this._pubsub._subscriptions[store.replicationTopic]) {
                await this.subscribeToReplicationTopic(store.replicationTopic, store.id);
              }
              else {
                const msg = new RequestHeadsMessage({
                  address: store.address.toString(),
                  replicationTopic: store.replicationTopic
                });
                await this._pubsub.publish(store.replicationTopic, serialize(await this.decryptedSignedThing(serialize(msg))));

              }
            }
          }
        });
        resolve(store)
      } catch (error) {
        reject(error);
      }
    })
    return this._openStorePromise as Promise<S>;
    /*  } */

  }

  // Save the database locally
  async _addManifestToCache(cache, dbAddress: Address) {
    await cache.set(path.join(dbAddress.toString(), '_manifest'), dbAddress.root)
    logger.debug(`Saved manifest to IPFS as '${dbAddress.root}'`)
  }

  /**
   * Check if we have the database, or part of it, saved locally
   * @param  {[Cache]} cache [The OrbitDBCache instance containing the local data]
   * @param  {[Address]} dbAddress [Address of the database to check]
   * @return {[Boolean]} [Returns true if we have cached the db locally, false if not]
   */
  async _haveLocalData(cache, dbAddress: Address) {
    if (!cache) {
      return false
    }

    const addr = dbAddress.toString()
    const data = await cache.get(path.join(addr, '_manifest'))
    return data !== undefined && data !== null
  }

}
