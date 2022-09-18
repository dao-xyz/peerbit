import path from 'path'
import { EventEmitter } from 'events'
import mapSeries from 'p-each-series'
import { default as PQueue } from 'p-queue'
import { Log, ISortFunction, RecycleOptions, LogOptions } from '@dao-xyz/ipfs-log'
import { IOOptions, EncryptionTemplateMaybeEncrypted, Payload } from '@dao-xyz/ipfs-log-entry'
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Replicator } from './replicator'
import { ReplicationInfo } from './replication-info'
import Logger from 'logplease'
import io from '@dao-xyz/orbit-db-io'
import Cache from '@dao-xyz/orbit-db-cache';
import { variant, field, vec, option } from '@dao-xyz/borsh';
import { IPFS } from 'ipfs-core-types/src/'
import stringify from 'json-stringify-deterministic'
import { serialize, deserialize } from '@dao-xyz/borsh';
import { Snapshot } from './snapshot'
import { AccessError, PublicKeyEncryption } from '@dao-xyz/encryption-utils'
import { Address, load, save } from './io'
import { AccessController } from './access-controller'
import { v4 as uuid } from 'uuid';
import { StoreLike } from './store-like'
import { Ed25519PublicKeyData, PublicKey } from '@dao-xyz/identity'
import { Ed25519PublicKey } from 'sodium-plus';
import { joinUint8Arrays } from '@dao-xyz/io-utils';
import isNode from 'is-node';
import { NoSharding, Sharding } from './shard'

let v8 = undefined;
if (isNode) {
  v8 = require('v8');
}
export type Constructor<T> = new (...args: any[]) => T;

const logger = Logger.create('orbit-db.store', { color: Logger.Colors.Blue })
Logger.setLogLevel('ERROR')

const bigIntMax = (...args) => args.reduce((m, e) => e > m ? e : m);
const bigIntMin = (...args) => args.reduce((m, e) => e < m ? e : m);


@variant(0)
export class HeadsCache<T> {

  @field({ type: vec(Entry) })
  heads: Entry<T>[]

  constructor(opts?: {
    heads: Entry<T>[]
  }) {
    if (opts) {
      this.heads = opts.heads;
    }
  }
}

export type StorePublicKeyEncryption = (replicationTopic: string) => PublicKeyEncryption/* {
  encrypt: (bytes: Uint8Array, reciever: X25519PublicKey) => Promise<{
    data: Uint8Array
    senderPublicKey: X25519PublicKey
  }>,
  decrypt: (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey) => Promise<Uint8Array | undefined>
} */


export interface IStoreOptions<T> {
  /**
   * f set to true, will throw an error if the database can't be found locally. (Default: false)
   */
  localOnly?: boolean;

  /**
   * The directory where data will be stored (Default: uses directory option passed to OrbitDB constructor or ./orbitdb if none was provided).
   */
  directory?: string;

  /**
   * Replicate the database with peers, requires IPFS PubSub. (Default: true)
   */
  replicate?: boolean;


  replicationTopic?: string | (() => string),



  /**
   * Name to name conditioned some external property
   */
  /*   nameResolver?: (name: string) => string */

  encryption?: StorePublicKeyEncryption,
  encoding?: IOOptions<T>

  maxHistory?: number,
  fetchEntryTimeout?: number,
  referenceCount?: number,
  replicationConcurrency?: number,
  fallbackAccessController?: AccessController<T>,
  syncLocal?: boolean,
  sortFn?: ISortFunction,
  recycle?: RecycleOptions,
  typeMap?: { [key: string]: Constructor<any> }
  onUpdate?: (oplog: Log<T>, entries?: Entry<T>[]) => void,
  resourceOptions?: ResourceOptions<T>,


}

export type ResourceOptions<T> = { heapSizeLimit: () => number };


export interface IInitializationOptions<T> extends IStoreOptions<T> {

  /* encryption?: {
    encrypt: (arr: Uint8Array, keyGroup: string) => Promise<{ bytes: Uint8Array, keyId: Uint8Array }>
    decrypt: (arr: Uint8Array, keyGroup: string, keyId: Uint8Array) => Promise<Uint8Array>
  }, */

  requestNewShard: () => void,
  saveAndResolveStore: (store: StoreLike<any>) => Promise<StoreLike<any>>,
  resolveCache: (address: Address) => Promise<Cache>,
  onClose?: (store: Store<T>) => void,
  onDrop?: (store: Store<T>) => void,
  onLoad?: (store: Store<T>) => void,
  onWrite?: (topic: string, address: string, _entry: Entry<T>, heads: Entry<T>[]) => void
  onOpen?: (store: Store<any>) => Promise<void>,
}
export const JSON_ENCODER = {
  encoder: (obj) => new Uint8Array(Buffer.from(stringify(obj))),
  decoder: (obj) => JSON.parse(Buffer.from(obj).toString())
};

export const DefaultOptions: IInitializationOptions<any> = {
  onUpdate: () => { },
  maxHistory: -1,
  fetchEntryTimeout: null,
  referenceCount: 32,
  replicationConcurrency: 32,
  syncLocal: false,
  sortFn: undefined,
  typeMap: {},
  /* nameResolver: (name: string) => name, */
  encoding: JSON_ENCODER,
  onClose: undefined,
  onDrop: undefined,
  onLoad: undefined,
  resolveCache: undefined,
  resourceOptions: undefined,
  requestNewShard: undefined,
  saveAndResolveStore: async (store: Store<any>) => {
    await store.save(store._ipfs, { pin: true })
    return store;
  }
}




@variant(0)
export class Store<T> implements StoreLike<T> {

  @field({ type: 'string' })
  name: string;

  @field({ type: Sharding })
  sharding: Sharding

  @field({ type: option(AccessController) })
  accessController?: AccessController<T> | (StoreLike<any> & AccessController<T>)

  // An access controller that is note part of the store manifest, usefull for circular store -> access controller -> store structures
  fallbackAccessController?: AccessController<T> | (StoreLike<any> & AccessController<T>)

  id: string;
  options: IInitializationOptions<T>;
  publicKey: PublicKey;
  sign: (data: Uint8Array) => Promise<Uint8Array>;
  address: Address;
  dbname: string;
  events: EventEmitter;
  remoteHeadsPath: string;
  localHeadsPath: string;
  snapshotPath: string;
  queuePath: string;
  manifestPath: string;
  initialized: boolean;

  _freezed: boolean;
  _ipfs: IPFS;
  _cache: Cache;
  _oplog: Log<T>;
  _queue: PQueue<any, any>
  _replicationStatus: ReplicationInfo;
  _stats: any;
  _replicator: Replicator<T>;
  _loader: Replicator<T>;
  _key: string;

  /* 
  
  */

  constructor(properties?: { sharding?: Sharding, name?: string, accessController?: AccessController<T> | (StoreLike<any> & AccessController<T>) }) {

    if (properties) {
      this.name = properties.name || uuid();
      this.accessController = properties.accessController;
      this.sharding = properties.sharding || new NoSharding()
    }


    // Access mapping
    /*   this.access = options.accessController || {
        canAppend: async (payload: MaybeEncrypted<Payload<T>>, entryIdentity: MaybeEncrypted<IdentitySerializable>, _identityProvider: Identities) => true,
        type: undefined,
        address: undefined,
        close: undefined,
        load: undefined,
        save: undefined
      } as any as AccessController<T> */ // TODO fix types


  }

  async init(ipfs: IPFS, publicKey: PublicKey | Ed25519PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: IInitializationOptions<T>): Promise<StoreLike<T>> {

    if (this.initialized) {
      return;
    }

    // Set ipfs since we are to save the store
    this._ipfs = ipfs

    // Set the options (we will use the replicationTopic property after thiis)
    const opts = Object.assign({}, DefaultOptions)
    Object.assign(opts, options)
    this.options = opts


    const thisAlternative = await options.saveAndResolveStore(this);
    if (thisAlternative !== this) {
      return thisAlternative;
    }


    this.publicKey = publicKey instanceof Ed25519PublicKey ? new Ed25519PublicKeyData({ publicKey }) : publicKey;

    if ((this.accessController as StoreLike<any>)?.init) {
      this.accessController = (await (this.accessController as StoreLike<any>).init(ipfs, this.publicKey, sign, options)) as (StoreLike<any> & AccessController<any>);
    }

    const address = this.address; // will exist since options.saveAndResolveStore will save

    // Create IDs, names and paths
    this.id = address.toString();
    this.address = address as Address
    this.dbname = (address as Address).path || ''
    this.events = new EventEmitter()
    this.remoteHeadsPath = path.join(this.id, '_remoteHeads')
    this.localHeadsPath = path.join(this.id, '_localHeads')
    this.snapshotPath = path.join(this.id, 'snapshot')
    this.queuePath = path.join(this.id, 'queue')
    this.manifestPath = path.join(this.id, '_manifest')
    this.sign = sign;
    this.fallbackAccessController = options.fallbackAccessController;
    this.sharding.init(options.requestNewShard);



    // External dependencies
    this._cache = await options.resolveCache(this.address);

    // Create the operations log
    this._oplog = new Log<T>(this._ipfs, publicKey, sign, this.logOptions)

    // _addOperation and log-joins queue. Adding ops and joins to the queue
    // makes sure they get processed sequentially to avoid race conditions
    // between writes and joins (coming from Replicator)
    this._queue = new PQueue({ concurrency: 1 })

    // Replication progress info
    this._replicationStatus = new ReplicationInfo()


    // Statistics
    this._stats = {
      snapshot: {
        bytesLoaded: -1
      },
      syncRequestsReceieved: 0
    }


    try {
      const onReplicationQueued = async (entry: Entry<T>) => {
        // Update the latest entry state (latest is the entry with largest clock time)
        await entry.getClock();
        this._recalculateReplicationMax(entry.clock.time)
        this.events.emit('replicate', this.address.toString(), entry)
      }

      const onReplicationProgress = async (entry: Entry<T>) => {
        const previousProgress = this.replicationStatus.progress
        const previousMax = this.replicationStatus.max
        this._recalculateReplicationStatus((await entry.clock).time)
        if (this._oplog.length + 1 > this.replicationStatus.progress ||
          this.replicationStatus.progress > previousProgress ||
          this.replicationStatus.max > previousMax) {
          this.events.emit('replicate.progress', this.address.toString(), entry.hash, entry, this.replicationStatus.progress, this.replicationStatus.max)
        }
      }

      const onReplicationComplete = async (logs) => {
        const updateState = async () => {
          try {
            if (this._oplog && logs.length > 0) {
              try {
                for (const log of logs) {
                  await this._oplog.join(log)
                }
              } catch (error) {
                if (error instanceof AccessError) {
                  logger.info(error.message);
                  return;
                }
              }

              // only store heads that has been verified and merges
              const heads = this._oplog.heads
              await this._cache.setBinary(this.remoteHeadsPath, new HeadsCache({ heads }))
              logger.debug(`Saved heads ${heads.length} [${heads.map(e => e.hash).join(', ')}]`)

              // update the store's index after joining the logs
              // and persisting the latest heads
              await this._updateIndex()

              if (this._oplog.length > this.replicationStatus.progress) {
                this._recalculateReplicationStatus(this._oplog.length)
              }

              this.events.emit('replicated', this.address.toString(), logs.length, this)
            }
          } catch (e) {
            throw e;
          }
        }
        await this._queue.add(updateState.bind(this))
      }
      // Create the replicator
      this._replicator = new Replicator(this, this.options.replicationConcurrency)
      // For internal backwards compatibility,
      // to be removed in future releases
      this._loader = this._replicator
      // Hook up the callbacks to the Replicator
      this._replicator.onReplicationQueued = onReplicationQueued
      this._replicator.onReplicationProgress = onReplicationProgress
      this._replicator.onReplicationComplete = onReplicationComplete
    } catch (e) {
      console.error('Store Error:', e)
    }
    // TODO: verify if this is working since we don't seem to emit "replicated.progress" anywhere
    this.events.on('replicated.progress', (address, hash, entry, progress, have) => {
      this._procEntry(entry)
    })
    this.events.on('write', (topic, address, entry, heads) => {
      this._procEntry(entry)
      if (this.options.onWrite) {
        this.options.onWrite(topic, address, entry, heads);
      }
    })

    if (this.options.onOpen) {
      await this.options.onOpen(this);

    }
    this.initialized = true;
  }


  get oplog(): Log<any> {
    return this._oplog;
  }
  get cache(): Cache {
    return this._cache;
  }

  get key() {
    return this._key
  }

  get logOptions(): LogOptions<T> {
    return {
      logId: this.id,
      encoding: this.options.encoding,
      encryption: this.options.encryption ? {
        getAnySecret: this.options.encryption(this.replicationTopic).getAnySecret,
        getEncryptionKey: this.options.encryption(this.replicationTopic).getEncryptionKey
      } : undefined, //this.options.encryption
      access: this.accessController || this.fallbackAccessController,
      sortFn: this.options.sortFn,
      recycle: this.options.recycle,
    };
  }

  /**
   * Returns the database's current replication status information
   * @return {[Object]} [description]
   */
  get replicationStatus() {
    return this._replicationStatus
  }

  get replicationTopic() {
    return Store.getReplicationTopic(this.address, this.options)
  }

  static getReplicationTopic(address: Address | string, options: IStoreOptions<any>) {
    return options.replicationTopic ? (typeof options.replicationTopic === 'string' ? options.replicationTopic : options.replicationTopic()) : (typeof address === 'string' ? address : address.toString());
  }

  setPublicKey(publicKey: PublicKey) {
    this.publicKey = publicKey
    this._oplog.setPublicKey(publicKey)
  }

  freeze() {
    this._freezed = true;
  }

  checkMemory(): boolean {
    if (!v8) {
      return true; // Assume no memory checks
    }
    if (this.options.resourceOptions.heapSizeLimit) {
      const usedHeapSize = v8?.getHeapStatistics().used_heap_size;
      if (usedHeapSize > this.options.resourceOptions.heapSizeLimit()) {
        if (!this.sharding) {
          return true; // Assume no memory checks
        }
        this.sharding.onMemoryExceeded(this);

        return false;
      }
    }
    return true;
  }
  async close() {
    if (!this.initialized) {
      return
    };

    // Stop the Replicator
    await this._replicator?.stop()

    // Wait for the operations queue to finish processing
    // to make sure everything that all operations that have
    // been queued will be written to disk
    await this._queue?.onIdle()

    // Reset replication statistics
    this._replicationStatus?.reset()

    // Reset database statistics
    this._stats = {
      snapshot: {
        bytesLoaded: -1
      },
      syncRequestsReceieved: 0
    }

    if (this.options.onClose) {
      await this.options.onClose(this)
    }

    // Close store access controller
    if ((this.accessController || this.fallbackAccessController).close) {
      await (this.accessController || this.fallbackAccessController).close()
    }

    // Remove all event listeners
    for (const event in this.events["_events"]) {
      this.events.removeAllListeners(event)
    }

    this._oplog = null

    // Database is now closed

    this.initialized = false;
    return Promise.resolve()
  }

  /**
   * Drops a database and removes local data
   */
  async drop() {
    if (!this._oplog && !this._cache) {
      return; // already dropped
    }

    if (this.options.onDrop) {
      await this.options.onDrop(this)
    }

    await this._cache.del(this.localHeadsPath)
    await this._cache.del(this.remoteHeadsPath)
    await this._cache.del(this.snapshotPath)
    await this._cache.del(this.queuePath)
    await this._cache.del(this.manifestPath)

    await this.close()

    // Reset
    this._oplog = undefined;
    this._cache = undefined;
  }

  async load(amount?: number, opts: { fetchEntryTimeout?: number } = {}) {

    amount = amount || this.options.maxHistory
    const fetchEntryTimeout = opts.fetchEntryTimeout || this.options.fetchEntryTimeout

    if (this.options.onLoad) {
      await this.options.onLoad(this)
    }
    const localHeads: Entry<T>[] = (await this._cache.getBinary<HeadsCache<T>>(this.localHeadsPath, HeadsCache))?.heads || []
    const remoteHeads: Entry<T>[] = (await this._cache.getBinary<HeadsCache<T>>(this.remoteHeadsPath, HeadsCache))?.heads || []
    const heads = localHeads.concat(remoteHeads)

    if (heads.length > 0) {
      this.events.emit('load', this.address.toString(), heads)
    }

    // Update the replication status from the heads
    for (const head of heads) {
      const time = (await head.clock).time
      this._recalculateReplicationMax(time)
    }

    // Load the log
    const log = await Log.fromEntryHash(this._ipfs, this.publicKey, this.sign, heads.map(e => e.hash), {
      ...this.logOptions,
      length: amount,
      timeout: fetchEntryTimeout,
      onProgressCallback: this._onLoadProgress.bind(this),
      concurrency: this.options.replicationConcurrency,
    })

    this._oplog = log

    // Update the index
    if (heads.length > 0) {
      await this._updateIndex()
    }

    this.events.emit('ready', this.address.toString(), this._oplog.heads)
  }

  async sync(heads: Entry<T>[]) {

    if (this._freezed) {
      return
    }

    const mem = await this.checkMemory();
    if (!mem) {
      return;
    }

    this._stats.syncRequestsReceieved += 1
    logger.debug(`Sync request #${this._stats.syncRequestsReceieved} ${heads.length}`)
    if (heads.length === 0) {
      return
    }

    // To simulate network latency, uncomment this line
    // and comment out the rest of the function
    // That way the object (received as head message from pubsub)
    // doesn't get written to IPFS and so when the Replicator is fetching
    // the log, it'll fetch it from the network instead from the disk.
    // return this._replicator.load(heads)

    const saveToIpfs = async (head: Entry<T>) => {
      if (!head) {
        console.warn("Warning: Given input entry was 'null'.")
        return Promise.resolve(null)
      }
      /* 
            const identityProvider = this.identity.provider
            if (!identityProvider) throw new Error('Identity-provider is required, cannot verify entry')
       */

      // TODO Fix types
      head.init({
        encoding: this._oplog._encoding,
        encryption: this._oplog._encryption
      })
      try {
        const canAppend = await this.accessController.canAppend(head._payload, head._publicKey)
        if (!canAppend) {
          logger.info('Warning: Given input entry is not allowed in this log and was discarded (no write access).')
          return Promise.resolve(null)
        }
      } catch (error) {
        return Promise.resolve(null);
      }

      const logEntry = Entry.toEntryNoHash(head)
      const hash = await io.write(this._ipfs, 'dag-cbor', logEntry.serialize(), { links: Entry.IPLD_LINKS }) ///, onlyHash: true

      if (hash !== head.hash) {
        throw new Error("Head hash didn\'t match the contents")
      }

      return head
    }

    return mapSeries(heads, saveToIpfs)
      .then(async (saved) => {
        return this._replicator.load(saved.filter(e => e !== null))
      })
  }

  async save(ipfs: any, options?: {
    format?: string;
    pin?: boolean;
    timeout?: number;
  }): Promise<Address> {
    const address = await save(ipfs, this, options)
    this.address = address;
    return address;
  }

  static load(ipfs: any, address: Address, options?: {
    timeout?: number;
  }) {
    return load(ipfs, address, Store, options)
  }

  loadMoreFrom(amount, entries) {
    this._replicator.load(entries)
  }

  get replicate(): boolean {
    return this.options.replicate;
  }

  async getHeads(): Promise<Entry<T>[]> {
    if (!(this.cache)) {
      return [];
    }
    const localHeads = (await this.cache.getBinary<HeadsCache<T>>(this.localHeadsPath, HeadsCache))?.heads || []
    const remoteHeads = (await this.cache.getBinary<HeadsCache<T>>(this.remoteHeadsPath, HeadsCache))?.heads || []
    return [...localHeads, ...remoteHeads]
  }

  async saveSnapshot() {
    const unfinished = this._replicator.unfinished
    const snapshotData = this._oplog.toSnapshot()
    const buf = Buffer.from(serialize(new Snapshot({
      id: snapshotData.id,
      heads: snapshotData.heads,
      size: BigInt(snapshotData.values.length),
      values: snapshotData.values
    })))

    const snapshot = await this._ipfs.add(buf)
    snapshot["hash"] = snapshot.cid.toString();
    await this._cache.set(this.snapshotPath, snapshot)
    await this._cache.set(this.queuePath, unfinished)

    logger.debug(`Saved snapshot: ${snapshot.cid.toString()}, queue length: ${unfinished.length}`)

    return [snapshot]
  }

  async loadFromSnapshot() {
    if (this.options.onLoad) {
      await this.options.onLoad(this)
    }

    this.events.emit('load', this.address.toString()) // TODO emits inconsistent params, missing heads param

    const maxClock = (res: bigint, val: Entry<any>): bigint => bigIntMax(res, val.clock.time)
    this.sync([])

    const queue = (await this._cache.get(this.queuePath)) as string[]
    if (queue?.length > 0) {
      this._replicator.load(queue)
    }

    const snapshot = await this._cache.get(this.snapshotPath)

    if (snapshot) {
      const chunks = []
      for await (const chunk of this._ipfs.cat(snapshot["hash"])) {
        chunks.push(chunk)
      }
      const snapshotData = deserialize(joinUint8Arrays(chunks), Snapshot);

      // Fetch the entries
      // Timeout 1 sec to only load entries that are already fetched (in order to not get stuck at loading)
      this._recalculateReplicationMax(snapshotData.values.reduce(maxClock, 0n))
      if (snapshotData) {
        this._oplog = await Log.fromJSON(this._ipfs, this.publicKey, this.sign, snapshotData, {
          access: this.accessController,
          sortFn: this.options.sortFn,
          length: -1,
          timeout: 1000,
          onProgressCallback: this._onLoadProgress.bind(this)
        })
        await this._updateIndex()
        this.events.emit('replicated', this.address.toString()) // TODO: inconsistent params, count param not emited
      }
      this.events.emit('ready', this.address.toString(), this._oplog.heads)
    } else {
      throw new Error(`Snapshot for ${this.address} not found!`)
    }

    return this
  }

  async _updateIndex(entries?: Entry<T>[]) {
    if (this.options.onUpdate) {
      this.options.onUpdate(this._oplog, entries);
    }
  }

  async syncLocal() {
    const localHeads = (await this._cache.getBinary<HeadsCache<T>>(this.localHeadsPath, HeadsCache))?.heads || []
    const remoteHeads = (await this._cache.getBinary<HeadsCache<T>>(this.remoteHeadsPath, HeadsCache))?.heads || []
    const heads = localHeads.concat(remoteHeads)
    for (let i = 0; i < heads.length; i++) {
      const head = heads[i]
      if (!this._oplog.heads.includes(head)) {
        await this.load()
        break
      }
    }
  }

  async _addOperation(data: T, options: { onProgressCallback?: (any) => void, pin?: boolean, reciever?: EncryptionTemplateMaybeEncrypted } = {}) {
    const addOperation = async () => {
      if (this._oplog) {
        // check local cache for latest heads
        if (this.options.syncLocal) {
          await this.syncLocal()
        }

        const entry = await this._oplog.append(data, {
          pointerCount: this.options.referenceCount, pin: options.pin, reciever: options.reciever
        })
        this._recalculateReplicationStatus((await entry.clock).time)
        await this._cache.setBinary(this.localHeadsPath, new HeadsCache({ heads: [entry] }))
        await this._updateIndex([entry])

        // The row below will emit an "event", which is subscribed to on the orbit-db client (confusing enough)
        // there, the write is binded to the pubsub publish, with the entry. Which will send this entry 
        // to all the connected peers to tell them that a new entry has been added
        // TODO: don't use events, or make it more transparent that there is a vital subscription in the background
        // that is handling replication
        this.events.emit('write', this.replicationTopic, this.address.toString(), entry, this._oplog.heads)
        if (options?.onProgressCallback) options.onProgressCallback(entry)
        return entry.hash

      }
    }
    return this._queue.add(addOperation.bind(this))
  }



  _addOperationBatch(data, batchOperation?, lastOperation?, onProgressCallback?) {
    throw new Error('Not implemented!')
  }

  _procEntry(entry: Entry<T>) {
    /* const { op } = payload
    if (op) {
      this.events.emit(`log.op.${op}`, this.address.toString(), hash, payload)
    } else {
      this.events.emit('log.op.none', this.address.toString(), hash, payload)
    }
    this.events.emit('log.op', op, this.address.toString(), hash, payload) */
  }

  /* Replication Status state updates */
  _recalculateReplicationProgress() {
    this._replicationStatus.progress = bigIntMax(
      bigIntMin(this._replicationStatus.progress + 1n, this._replicationStatus.max),
      BigInt(this._oplog ? this._oplog.length : 0)
    )
  }

  _recalculateReplicationMax(max: bigint | number) {
    const bigMax = BigInt(max);
    this._replicationStatus.max = bigIntMax(
      this.replicationStatus.max,
      BigInt(this._oplog ? this._oplog.length : 0),
      (bigMax || 0n)
    )
  }

  _recalculateReplicationStatus(maxTotal: bigint | number) {
    this._recalculateReplicationMax(maxTotal)
    this._recalculateReplicationProgress()
  }

  /* Loading progress callback */
  _onLoadProgress(entry: Entry<any>) {
    this._recalculateReplicationStatus(entry.clock.time)
    this.events.emit('load.progress', this.address.toString(), entry.hash, entry, this.replicationStatus.progress, this.replicationStatus.max)
  }

  clone(newName: string): Store<T> {
    return new Store({
      name: newName,
      accessController: this.accessController.clone(newName)
    })
  }
}
