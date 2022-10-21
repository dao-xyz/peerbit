import path from 'path'
import mapSeries from 'p-each-series'
import PQueue from 'p-queue'
import { Log, ISortFunction, PruneOptions, LogOptions, Identity, max, min, CanAppend, Payload } from '@dao-xyz/ipfs-log'
import { Encoding, EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log'
import { Entry } from '@dao-xyz/ipfs-log'
import { Replicator } from './replicator.js'
import { ReplicationInfo } from './replication-info.js'
import io from '@dao-xyz/peerbit-io-utils'
import Cache from '@dao-xyz/peerbit-cache';
import { variant, field, vec, Constructor } from '@dao-xyz/borsh';
import { IPFS } from 'ipfs-core-types'

// @ts-ignore
import { serialize, deserialize } from '@dao-xyz/borsh';
import { Snapshot } from './snapshot.js'
import { AccessError, MaybeEncrypted, PublicKeyEncryptionResolver, SignatureWithKey } from "@dao-xyz/peerbit-crypto"

// @ts-ignore
import { v4 as uuid } from 'uuid';
import { joinUint8Arrays } from '@dao-xyz/peerbit-borsh-utils';
// @ts-ignore
import Logger from 'logplease'
import { Address, Addressable, load, save } from './io.js'
import { SystemBinaryPayload } from '@dao-xyz/peerbit-bpayload'

const logger = Logger.create('orbit-db.store', { color: Logger.Colors.Blue })
Logger.setLogLevel('ERROR')

export class CachedValue { }

@variant(0)
export class CID extends CachedValue {

  @field({ type: 'string' })
  hash: string

  constructor(opts?: {
    hash: string
  }) {
    super();
    if (opts) {
      this.hash = opts.hash;
    }
  }
}

@variant(1)
export class UnsfinishedReplication extends CachedValue {

  @field({ type: vec('string') })
  hashes: string[]

  constructor(opts?: {
    hashes: string[]
  }) {
    super();
    if (opts) {
      this.hashes = opts.hashes;
    }
  }
}



@variant(2)
export class HeadsCache<T> extends CachedValue {

  @field({ type: vec(Entry) })
  heads: Entry<T>[]

  constructor(opts?: {
    heads: Entry<T>[]
  }) {
    super();
    if (opts) {
      this.heads = opts.heads;
    }
  }
}


/* {
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

  onClose?: (store: Store<T>) => void,
  onDrop?: (store: Store<T>) => void,
  onLoad?: (store: Store<T>) => void,
  onLoadProgress?: (store: Store<T>, entry: Entry<T>) => void,
  onWrite?: (store: Store<T>, _entry: Entry<T>) => void
  onOpen?: (store: Store<any>) => Promise<void>,
  onReplicationQueued?: (store: Store<any>, entry: Entry<T>) => void,
  onReplicationProgress?: (store: Store<any>, entry: Entry<T>) => void,
  onReplicationComplete?: (store: Store<any>) => void
  onReady?: (store: Store<T>) => void,


  /**
   * Name to name conditioned some external property
   */
  /*   nameResolver?: (name: string) => string */

  encryption?: PublicKeyEncryptionResolver,
  maxHistory?: number,
  fetchEntryTimeout?: number,
  referenceCount?: number,
  replicationConcurrency?: number,
  syncLocal?: boolean,
  sortFn?: ISortFunction,
  prune?: PruneOptions,
  onUpdate?: (oplog: Log<T>, entries?: Entry<T>[]) => void,
  /*   resourceOptions?: ResourceOptions<T>,
   */
}

/* export type ResourceOptions<T> = { heapSizeLimit: () => number };
 */

export interface IInitializationOptions<T> extends IStoreOptions<T>, IInitializationOptionsDefault<T> {

  /* encryption?: {
    encrypt: (arr: Uint8Array, keyGroup: string) => Promise<{ bytes: Uint8Array, keyId: Uint8Array }>
    decrypt: (arr: Uint8Array, keyGroup: string, keyId: Uint8Array) => Promise<Uint8Array>
  }, */

  resolveCache: (address: Address) => Promise<Cache<CachedValue>> | Cache<CachedValue>

}

/* export const DefaultOptions: IInitializationOptions<any> = {

  fetchEntryTimeout: undefined,

  syncLocal: false,
  sortFn: undefined,
  maxHistory: -1,
  referenceCount: 32,
  replicationConcurrency: 32,
  typeMap: {},
  encoding: JSON_ENCODING,
  onClose: undefined,
  onDrop: undefined,
  onLoad: undefined,
  resolveCache: undefined,
  resourceOptions: undefined,

}
 */
interface IInitializationOptionsDefault<T> {

  maxHistory?: number,
  referenceCount?: number,
  replicationConcurrency?: number,
  typeMap?: { [key: string]: Constructor<any> }

}


export const DefaultOptions: IInitializationOptionsDefault<any> = {

  maxHistory: -1,
  referenceCount: 32,
  replicationConcurrency: 32,
  typeMap: {},
  /* nameResolver: (name: string) => name, */

}



export interface Initiable<T> {
  init?(ipfs: IPFS, identity: Identity, options: IInitializationOptions<T>): Promise<this>;
}
export interface Saveable {
  save(ipfs: any, options?: {
    format?: string;
    pin?: boolean;
    timeout?: number;
  }): Promise<Address>
}



@variant(0)
export class Store<T> extends SystemBinaryPayload implements Addressable, Initiable<T>, Saveable {

  @field({ type: 'string' })
  name: string;

  _canAppend?: CanAppend<T>;
  _onUpdate?: (oplog: Log<T>, entries?: Entry<T>[]) => void
  _onUpdateOption?: (oplog: Log<T>, entries?: Entry<T>[]) => void

  // An access controller that is note part of the store manifest, usefull for circular store -> access controller -> store structures

  _options: IInitializationOptions<T>;
  identity: Identity;

  /*   events: EventEmitter;
   */
  remoteHeadsPath: string;
  localHeadsPath: string;
  snapshotPath: string;
  queuePath: string;
  manifestPath: string;
  initialized: boolean;
  address: Address

  /*   allowForks: boolean = true;
   */

  _ipfs: IPFS;
  _cache: Cache<CachedValue>;
  _oplog: Log<T>;
  _queue: PQueue<any, any>
  _replicationStatus: ReplicationInfo;
  _stats: any;
  _replicator: Replicator<T>;
  _loader: Replicator<T>;
  _key: string;


  constructor(properties?: { name?: string, parent?: { name: string } }) {
    super();
    if (properties) {
      this.name = (properties.parent?.name ? (properties.parent?.name + '/') : '') + (properties.name || uuid());
    }
  }

  async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<T>): Promise<this> {

    if (this.initialized) {
      throw new Error("Already initialized");
    }

    // Set ipfs since we are to save the store
    this._ipfs = ipfs

    // Set the options (we will use the replicationTopic property after thiis)
    const opts = { ...DefaultOptions, ...options }
    this._options = opts


    // Save manifest
    await this.save(this._ipfs)

    if (!this.address) {
      throw new Error("Expecting adddress")
    }

    // Create IDs, names and paths
    this.identity = identity;
    this._onUpdateOption = options.onUpdate;

    /* this.events = new EventEmitter() */
    this.remoteHeadsPath = path.join(this.address.toString(), '_remoteHeads')
    this.localHeadsPath = path.join(this.address.toString(), '_localHeads')
    this.snapshotPath = path.join(this.address.toString(), 'snapshot')
    this.queuePath = path.join(this.address.toString(), 'queue')
    this.manifestPath = path.join(this.address.toString(), '_manifest')


    // External dependencies
    this._cache = await this._options.resolveCache(this.address);

    // Create the operations log
    this._oplog = new Log<T>(this._ipfs, identity, this.logOptions)

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
        const e = entry
        try {
          await e.getClock();
          this._recalculateReplicationMax(e.clock.time + 1n)
          this._options.onReplicationQueued && this._options.onReplicationQueued(this, e)
        } catch (error) {
          if (error instanceof AccessError) {
            logger.info("Failed to access clock of entry: " + e.hash);
            return; // Ignore, we cant access clock
          }
          throw error;
        }

      }

      const onReplicationProgress = async (entry: Entry<T>) => {
        const previousProgress = this.replicationStatus.progress
        const previousMax = this.replicationStatus.max

        // TODO below is not nice, do we really need replication status?
        try {
          this._recalculateReplicationStatus((await entry.getClock()).time + 1n)
        } catch (error) {
          this._recalculateReplicationStatus(0)
        }

        if (this._oplog.length + 1 > this.replicationStatus.progress ||
          this.replicationStatus.progress > previousProgress ||
          this.replicationStatus.max > previousMax) {
          this._options.onReplicationProgress && this._options.onReplicationProgress(this, entry)
        }
      }

      // Create the replicator
      this._replicator = new Replicator(this, this._options.replicationConcurrency)
      // For internal backwards compatibility,
      // to be removed in future releases
      this._loader = this._replicator
      // Hook up the callbacks to the Replicator
      this._replicator.onReplicationQueued = onReplicationQueued
      this._replicator.onReplicationProgress = onReplicationProgress
      this._replicator.onReplicationComplete = (logs: Log<T>[]) => {
        this._queue.add((() => this.updateStateFromLogs(logs)).bind(this))
      }
    } catch (e) {
      console.error('Store Error:', e)
    }

    /* this.events.on('write', (topic, address, entry, heads) => {
      if (this.options.onWrite) {
        this.options.onWrite(topic, address, entry, heads);
      }
    }) */

    if (this._options.onOpen) {
      await this._options.onOpen(this);

    }
    this.initialized = true;

    return this;
  }


  updateStateFromLogs = async (logs: Log<T>[]) => {
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
        this._options.onReplicationComplete && this._options.onReplicationComplete(this)

      }
    } catch (e) {
      throw e;
    }
  }


  get oplog(): Log<any> {
    return this._oplog;
  }

  get key() {
    return this._key
  }

  get logOptions(): LogOptions<T> {
    return {
      logId: this.address.toString(),
      encryption: this._options.encryption,
      sortFn: this._options.sortFn,
      prune: this._options.prune,
    };
  }

  /**
   * Returns the database's current replication status information
   * @return {[Object]} [description]
   */
  get replicationStatus() {
    return this._replicationStatus
  }

  setIdentity(identity: Identity) {
    this.identity = identity
    this._oplog.setIdentity(identity)
  }

  set canAppend(canAppend: CanAppend<T> | undefined) {
    this._canAppend = canAppend;
  }

  get canAppend(): (CanAppend<T> | undefined) {
    return this._canAppend;
  }

  set onUpdate(onUpdate: (oplog: Log<T>, entries?: Entry<T>[]) => void) {
    this._onUpdate = onUpdate;
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

    if (this._options.onClose) {
      await this._options.onClose(this)
    }

    this._oplog = null as any

    // Database is now closed

    return Promise.resolve()
  }

  /**
   * Drops a database and removes local data
   */
  async drop() {

    if (!this._oplog && !this._cache) {
      return; // already dropped
    }

    if (this._options.onDrop) {
      await this._options.onDrop(this)
    }

    await this._cache.del(this.localHeadsPath)
    await this._cache.del(this.remoteHeadsPath)
    await this._cache.del(this.snapshotPath)
    await this._cache.del(this.queuePath)
    await this._cache.del(this.manifestPath)

    await this.close()

    // Reset
    // TODO fix types
    this._oplog = undefined as any;
    this._cache = undefined as any;

    this.initialized = false; // call this last because (close() expect initialized to be able to function)


  }

  async load(amount?: number, opts: { fetchEntryTimeout?: number } = {}) {

    if (!this.initialized) {
      throw new Error("Store needs to be initialized before loaded")
    }

    amount = amount || this._options.maxHistory
    const fetchEntryTimeout = opts.fetchEntryTimeout || this._options.fetchEntryTimeout

    if (this._options.onLoad) {
      await this._options.onLoad(this)
    }
    const localHeads: Entry<any>[] = (await this._cache.getBinary(this.localHeadsPath, HeadsCache))?.heads || []
    const remoteHeads: Entry<any>[] = (await this._cache.getBinary(this.remoteHeadsPath, HeadsCache))?.heads || []
    const heads = localHeads.concat(remoteHeads)

    // Update the replication status from the heads
    for (const head of heads) {
      const time = (await head.clock).time
      this._recalculateReplicationMax(time + 1n)
    }

    // Load the log
    const log = await Log.fromEntry(this._ipfs, this.identity, heads, {
      ...this.logOptions,
      length: amount,
      timeout: fetchEntryTimeout,
      onProgressCallback: this._onLoadProgress.bind(this),
      concurrency: this._options.replicationConcurrency,
    })

    this._oplog = log

    // Update the index
    if (heads.length > 0) {
      await this._updateIndex()
    }

    this._replicator.start();
    this._options.onReady && this._options.onReady(this)
  }

  async sync(heads: (Entry<T>)[]) {


    /* const mem = await this.checkMemory();
    if (!mem) {
      return; // TODO state will not be accurate
    } */

    this._stats.syncRequestsReceieved += 1
    logger.debug(`Sync request #${this._stats.syncRequestsReceieved} ${heads.length}`)
    if (heads.length === 0) {
      return
    }

    const handle = async (headToHandle: Entry<T>) => {

      // TODO Fix types

      if (this.canAppend && !(await this.canAppend(headToHandle._payload, headToHandle._signature))) {
        return Promise.resolve(null)
      }

      const head = headToHandle;
      const hash = await io.write(this._ipfs, 'dag-cbor', head.serialize(), { links: Entry.IPLD_LINKS })
      if (hash !== head.hash) {
        throw new Error("Head hash didn\'t match the contents")
      }
      return headToHandle
    }

    return mapSeries(heads, handle)
      .then(async (saved) => {
        return this._replicator.load(saved.filter(e => e !== null))
      })
  }

  loadMoreFrom(entries: (string | Entry<any>)[]) {
    this._replicator.load(entries)
  }

  get replicate(): boolean {
    return !!this._options.replicate;
  }



  async getCachedHeads(): Promise<Entry<T>[]> {
    if (!(this._cache)) {
      return [];
    }
    const localHeads = ((await this._cache.getBinary(this.localHeadsPath, HeadsCache))?.heads || []) as Entry<T>[]
    const remoteHeads = ((await this._cache.getBinary(this.remoteHeadsPath, HeadsCache))?.heads || []) as Entry<T>[]
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

    await this._cache.setBinary(this.snapshotPath, new CID({ hash: snapshot.cid.toString() }))
    await this._cache.setBinary(this.queuePath, new UnsfinishedReplication({ hashes: unfinished }))

    logger.debug(`Saved snapshot: ${snapshot.cid.toString()}, queue length: ${unfinished.length}`)

    return [snapshot]
  }

  async loadFromSnapshot() {
    if (this._options.onLoad) {
      await this._options.onLoad(this)
    }

    const maxClock = (res: bigint, val: Entry<any>): bigint => max(res, val.clock.time)
    await this.sync([])

    const queue = (await this._cache.getBinary(this.queuePath, UnsfinishedReplication))?.hashes as string[]
    if (queue?.length > 0) {
      this._replicator.load(queue)
    }

    const snapshotCID = await this._cache.getBinary(this.snapshotPath, CID)

    if (snapshotCID) {
      const chunks = []
      for await (const chunk of this._ipfs.cat(snapshotCID.hash)) {
        chunks.push(chunk)
      }
      const snapshotData = deserialize(joinUint8Arrays(chunks), Snapshot);

      // Fetch the entries
      // Timeout 1 sec to only load entries that are already fetched (in order to not get stuck at loading)
      this._recalculateReplicationMax(snapshotData.values.reduce(maxClock, 0n) + 1n)
      if (snapshotData) {
        this._oplog = await Log.fromEntry(this._ipfs, this.identity, snapshotData.heads, {
          sortFn: this._options.sortFn,
          length: -1,
          timeout: 1000,
          onProgressCallback: this._onLoadProgress.bind(this)
        })
        await this._updateIndex()
        this._options.onReplicationComplete && this._options.onReplicationComplete(this)
      }
      this._options.onReady && this._options.onReady(this)
    } else {
      throw new Error(`Snapshot for ${this.address} not found!`)
    }

    return this
  }

  async _updateIndex(entries?: Entry<T>[]) {
    if (this._onUpdate) {
      this._onUpdate(this._oplog, entries);
    }
    if (this._onUpdateOption) {
      this._onUpdateOption(this._oplog, entries);
    }
  }

  async syncLocal() {
    const localHeads = (await this._cache.getBinary(this.localHeadsPath, HeadsCache))?.heads || []
    const remoteHeads = (await this._cache.getBinary(this.remoteHeadsPath, HeadsCache))?.heads || []
    const heads = localHeads.concat(remoteHeads)
    const headsHashes = new Set(this._oplog.heads.map(h => h.hash));
    for (let i = 0; i < heads.length; i++) {
      const head = heads[i]
      if (!headsHashes.has(head.hash)) {
        await this.load()
        break
      }
    }
  }

  async _addOperation(data: T, options: { encoding: Encoding<T>, skipCanAppendCheck?: boolean, nexts?: Entry<T>[], onProgressCallback?: (any: any) => void, pin?: boolean, reciever?: EncryptionTemplateMaybeEncrypted }): Promise<Entry<T>> {
    const addOperation = async () => {
      // check local cache for latest heads
      if (this._options.syncLocal) {
        await this.syncLocal()
      }

      const entry = await this._oplog.append(data, {
        nexts: options.nexts, pin: options.pin, reciever: options.reciever,
        encoding: options.encoding,
        canAppend: options.skipCanAppendCheck ? undefined : this.canAppend
      })

      // TODO below is not nice, do we really need replication status?
      try {
        this._recalculateReplicationStatus((await entry.getClock()).time + 1n)
      } catch (error) {
        this._recalculateReplicationStatus(0)
      }
      await this._cache.setBinary(this.localHeadsPath, new HeadsCache({ heads: [entry] }))
      await this._updateIndex([entry])

      // The row below will emit an "event", which is subscribed to on the orbit-db client (confusing enough)
      // there, the write is binded to the pubsub publish, with the entry. Which will send this entry 
      // to all the connected peers to tell them that a new entry has been added
      // TODO: don't use events, or make it more transparent that there is a vital subscription in the background
      // that is handling replication
      this._options.onWrite && this._options.onWrite(this, entry);

      /*      const headsFromWrite = await this.oplog.getHeads(entry.hash);
           this.events.emit('write', this.replicationTopic, this.address.toString(), entry, headsFromWrite) */
      if (options?.onProgressCallback) options.onProgressCallback(entry)
      return entry
    }
    return this._queue.add(addOperation.bind(this))
  }





  /* Replication Status state updates */
  _recalculateReplicationProgress() {
    this._replicationStatus.progress = max(
      min(this._replicationStatus.progress + 1n, this._replicationStatus.max),
      BigInt(this._oplog ? this._oplog.length : 0)
    )
  }

  _recalculateReplicationMax(value: bigint | number) {
    const bigMax = BigInt(value);
    this._replicationStatus.max = max(
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
    this._recalculateReplicationStatus(entry.clock.time + 1n)
    this._options.onLoadProgress && this._options.onLoadProgress(this, entry)
  }

  clone(): Store<T> {
    return deserialize(serialize(this), this.constructor as any as Constructor<any>);
  }

  async save(ipfs: IPFS, options?: {
    format?: string;
    pin?: boolean;
    timeout?: number;
  }): Promise<Address> {
    const address = await save(ipfs, this, options)
    this.address = address;
    return address;
  }

  static load(ipfs: IPFS, address: Address, options?: {
    timeout?: number;
  }): Promise<Store<any>> {
    return load(ipfs, address, Store, options) as Promise<Store<any>>
  }
}
