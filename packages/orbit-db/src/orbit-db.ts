import path from 'path'
import { Address, IStoreOptions, Store, StoreLike, StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store'
import { PubSub, Subscription } from '@dao-xyz/orbit-db-pubsub'
import Logger from 'logplease'
const logger = Logger.create('orbit-db')
import { Identity, Identities } from '@dao-xyz/orbit-db-identity-provider'
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
import { DecryptedThing, EncryptedThing, MaybeEncrypted, MaybeSigned, PublicKeyEncryption } from '@dao-xyz/encryption-utils'
import { Ed25519PublicKey, X25519PublicKey } from 'sodium-plus'
import LRU from 'lru';
import { DirectChannel } from '@dao-xyz/ipfs-pubsub-1on1'
import { encryptionWithRequestKey, replicationTopicEncryptionWithRequestKey } from './encryption'

/* let AccessControllersModule = AccessControllers;
 */
Logger.setLogLevel('ERROR')

const defaultTimeout = 30000 // 30 seconds

export type StoreOperations = 'write' | 'all'
export type Storage = { createStore: (string) => any }
export type CreateOptions = {
  AccessControllers?: any, cache?: Cache, keystore?: Keystore, peerId?: string, offline?: boolean, directory?: string, storage?: Storage, broker?: any, waitForKeysTimout?: number, canAccessKeys?: KeyAccessCondition, isTrusted?: (key: Ed25519PublicKey, replicationTopic: string) => Promise<boolean>
};
export type CreateInstanceOptions = CreateOptions & { identity?: Identity, id?: string };
export class OrbitDB {
  _ipfs: IPFSInstance;
  identity: Identity;
  id: string;
  _pubsub: PubSub;
  _directConnections: { [key: string]: { channel: DirectChannel, dependencies: Set<string> } };
  /*   _directConnectionsByTopic: { [key: string]: { [key: string]: DirectChannel } } = {};
   */

  directory: string;
  storage: Storage;
  caches: any;
  keystore: Keystore;
  stores: { [topic: string]: { [address: string]: StoreLike<any> } };

  _waitForKeysTimeout = 10000;
  _keysInflightMap: Map<string, Promise<any>> = new Map(); // TODO fix types
  _keyRequestsLRU: LRU = new LRU({ max: 100, maxAge: 10000 });

  isTrusted: (key: Ed25519PublicKey, replicationTopic: string) => Promise<boolean>
  canAccessKeys: KeyAccessCondition

  constructor(ipfs: IPFSInstance, identity: Identity, options: CreateOptions = {}) {
    if (!isDefined(ipfs)) { throw new Error('IPFS is a required argument. See https://github.com/orbitdb/orbit-db/blob/master/API.md#createinstance') }
    if (!isDefined(identity)) { throw new Error('identity is a required argument. See https://github.com/orbitdb/orbit-db/blob/master/API.md#createinstance') }

    this._ipfs = ipfs
    this.identity = identity
    this.id = options.peerId
    this._pubsub = !options.offline
      ? new (
        options.broker ? options.broker : PubSub
      )(this._ipfs, this.id)
      : null
    this.directory = options.directory || './orbitdb'
    this.storage = options.storage
    this._directConnections = {}

    this.caches = {}
    this.caches[this.directory] = { cache: options.cache, handlers: new Set() }
    this.keystore = options.keystore
    this.canAccessKeys = options.canAccessKeys || (() => Promise.resolve(false));
    this.isTrusted = options.isTrusted || (() => Promise.resolve(true))
    this.stores = {}
    if (options.waitForKeysTimout) {
      this._waitForKeysTimeout = options.waitForKeysTimout;
    }

    // AccessControllers module can be passed in to enable
    // testing with orbit-db-access-controller
    /*     AccessControllersModule = options.AccessControllers || AccessControllers
     */
  }

  static get Pubsub() { return PubSub }
  static get Cache() { return Cache }
  static get Keystore() { return Keystore }
  static get Identities() { return Identities }
  /*   static get AccessControllers() { return AccessControllersModule }
   */

  static get Store() { return Store }

  get cache() { return this.caches[this.directory].cache }

  get encryption(): PublicKeyEncryption {
    return encryptionWithRequestKey(this.identity, this.keystore)
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
      requestAndWaitForKeys(condition, send, this.keystore, this.identity, this._waitForKeysTimeout).then((results) => {
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



    if (options.identity && options.identity.provider.keystore) {
      options.keystore = options.identity.provider.keystore
    }

    if (!options.keystore) {
      const keystorePath = path.join(options.directory, id, '/keystore')
      const keyStorage = await options.storage.createStore(keystorePath)
      options.keystore = new (Keystore as any)(keyStorage) // TODO fix typings
    }

    if (!options.identity) {
      options.identity = await Identities.createIdentity({
        id: new Uint8Array(Buffer.from(id)),
        keystore: options.keystore
      })
    }

    if (!options.cache) {
      const cachePath = path.join(options.directory, id, '/cache')
      const cacheStorage = await options.storage.createStore(cachePath)
      options.cache = new Cache(cacheStorage)
    }

    const finalOptions = Object.assign({}, options, { peerId: id })
    return new OrbitDB(ipfs, options.identity, finalOptions)
  }


  async disconnect() {
    // Close a direct connection and remove it from internal state
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

  async _createCache(path: string) {
    const cacheStorage = await this.storage.createStore(path)
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

      const maybeEncryptedMessage = deserialize(Buffer.from(data), MaybeEncrypted) as MaybeEncrypted<MaybeSigned<Message>>
      const decrypted = await maybeEncryptedMessage.init(this.encryption).decrypt()
      const signedMessage = decrypted.getValue(MaybeSigned);
      await signedMessage.verify(this.keystore.verify);
      const msg = signedMessage.getValue(Message);
      const sender: Ed25519PublicKey | undefined = signedMessage.signature?.publicKey;
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
                  await store.sync(heads)

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

    const getStore = (address: string) => this.stores[address]

    // Exchange heads
    await exchangeHeads(channel, replicationTopic, getStore, await this.getSigner())

  }

  async getSigner() {
    const senderSignerSecretKey = await this.keystore.getKeyByPath(this.identity.id, SignKeyWithMeta)
    return async (bytes) => {
      return {
        signature: await this.keystore.sign(bytes, senderSignerSecretKey),
        publicKey: senderSignerSecretKey.publicKey
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



  // Callback when database was closed
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
      if (!cache.handlers.size) await cache.cache.close()
    }

    delete this.stores[db.replicationTopic][address]

    const otherStoresUsingSameReplicaitonTopic = this.stores[db.replicationTopic]


    // close all connections with this repplication topic
    const deleteDirectConnectionsForTopic = Object.keys(otherStoresUsingSameReplicaitonTopic).length === 0;
    for (const [key, connection] of Object.entries(this._directConnections)) {
      connection.dependencies.delete(db.replicationTopic);
      if (deleteDirectConnectionsForTopic && connection.dependencies.size === 0) {
        await connection?.channel.close();
        delete this._directConnections[key];
      }
      // Delete connection from thing
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
  addStore(store: StoreLike<any>) {
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

  /*
      options = {
        localOnly: false // if set to true, throws an error if database can't be found locally
        create: false // whether to create the database


      }
   */
  async open<S extends StoreLike<any>>(store: /* string | Address |  */S, options: {
    timeout?: number,
    identity?: Identity,

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
    logger.debug('open()')

    options = Object.assign({ localOnly: false, create: false }, options)
    logger.debug(`Open database '${store}'`)

    // If address is just the name of database, check the options to crate the database
    /*  if (!store.address) {
 
       logger.warn(`Not a valid OrbitDB address '${store}', creating the database`)
       return this.create(store, options)
     }
     else { */

    // Parse the database address
    /*  const address = store.address;
     if (!address) {
       throw new Error("Missing address to open");
     } */
    // If database is already open, return early by returning the instance
    // if (this.stores[dbAddress]) {
    //   return this.stores[dbAddress]
    // }

    const resolveCache = async (address: Address) => {
      const cache = await this._requestCache(address.toString(), options.directory)

      // Check if we have the database
      const haveDB = await this._haveLocalData(cache, address)

      logger.debug((haveDB ? 'Found' : 'Didn\'t find') + ` database '${address}'`)

      // If we want to try and open the database local-only, throw an error
      // if we don't have the database locally
      if (options.localOnly && !haveDB) {
        logger.warn(`Database '${address}' doesn't exist!`)
        throw new Error(`Database '${address}' doesn't exist!`)
      }

      if (haveDB) {
        throw new Error("Cache already exist for address: " + address.toString())
      }

      // Save the database locally
      await this._addManifestToCache(cache, address)
      return cache;
    }


    logger.debug(`Loading store`)
    /*  let store = store instanceof Store ? store : undefined;
     if (!store) {
       try {
         // Get the database manifest from IPFS
         store = await Store.load(this._ipfs, address, { timeout: options.timeout || defaultTimeout }) as S
         logger.debug(`Manifest for '${address}':\n${JSON.stringify(store.name, null, 2)}`)
       } catch (e) {
         if (e.name === 'TimeoutError' && e.code === 'ERR_TIMEOUT') {
           console.error(e)
           throw new Error('ipfs unable to find and fetch store for this address.')
         } else {
           throw e
         }
       }
     } */

    /*  if (store.name !== address.path) {
       logger.warn(`Store name '${store.name}' and path name '${address.path}' do not match`)
     } */

    if (!options.encryption) {
      options.encryption = this.replicationTopicEncryption();
    }

    // Open the the database
    await await store.init(this._ipfs, options.identity || this.identity, {
      replicate: true, ...options, ...{
        resolveCache,
        onClose: this._onClose.bind(this),
        onDrop: this._onDrop.bind(this),
        onLoad: this._onLoad.bind(this),
        onWrite: this._onWrite.bind(this),
        onOpen: async (store) => {

          // ID of the store is the address as a string
          this.addStore(store)

          // Subscribe to pubsub to get updates from peers,
          // this is what hooks us into the message propagation layer
          // and the p2p network
          if (this._pubsub) {
            if (!this._pubsub._subscriptions[store.replicationTopic]) {
              await this._pubsub.subscribe(store.replicationTopic, store.id, this._onMessage.bind(this), {
                onNewPeerCallback: this._onPeerConnected.bind(this)
              })
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
      }
    });
    return store as S;
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
