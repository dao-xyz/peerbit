import path from 'path'
import { EventEmitter } from 'events'
import mapSeries from 'p-each-series'
import { default as PQueue } from 'p-queue'
import { Log, Entry, ISortFunction, IOOptions } from '@dao-xyz/ipfs-log'
import { Index } from './store-index'
import { Replicator } from './replicator'
import { ReplicationInfo } from './replication-info'
import Logger from 'logplease'
import io from 'orbit-db-io'
import { IPFS } from 'ipfs-core-types/src/'
import { Identities, Identity } from '@dao-xyz/orbit-db-identity-provider'
import { RecycleOptions, AccessError } from '@dao-xyz/ipfs-log'
import OrbitDBAccessController from 'orbit-db-access-controllers/src/orbitdb-access-controller'
import stringify from 'json-stringify-deterministic'

export type Constructor<T> = new (...args: any[]) => T;

const logger = Logger.create('orbit-db.store', { color: Logger.Colors.Blue })
Logger.setLogLevel('ERROR')


interface ICreateOptions {
  /**
   * The directory where data will be stored (Default: uses directory option passed to OrbitDB constructor or ./orbitdb if none was provided).
   */
  directory?: string;

  /**
   * Overwrite an existing database (Default: false)
   */
  overwrite?: boolean;

  /**
   * Replicate the database with peers, requires IPFS PubSub. (Default: true)
   */
  replicate?: boolean;


  /**
   * Name to name conditioned some external property
   */
  nameResolver?: (name: string) => string
}

interface IOpenOptions {
  /**
   * f set to true, will throw an error if the database can't be found locally. (Default: false)
   */
  localOnly?: boolean;

  /**
   * The directory where data will be stored (Default: uses directory option passed to OrbitDB constructor or ./orbitdb if none was provided).
   */
  directory?: string;

  /**
   * Whether or not to create the database if a valid OrbitDB address is not provided. (Default: false, only if using the OrbitDB#open method, otherwise this is true by default)
   */
  create?: boolean;

  /**
   * A supported database type (i.e. eventlog or an added custom type).
   * Required if create is set to true.
   * Otherwise it's used to validate the manifest.
   * You ony need to set this if using OrbitDB#open
   */
  type?: TStoreType;

  /**
   * Overwrite an existing database (Default: false)
   */
  overwrite?: boolean;

  /**
   * Replicate the database with peers, requires IPFS PubSub. (Default: true)
   */
  replicate?: boolean;
}


export interface IStoreOptions<T, X extends Index<T>> extends ICreateOptions, IOpenOptions {
  Index?: Constructor<X>,
  maxHistory?: number,
  fetchEntryTimeout?: number,
  referenceCount?: number,
  replicationConcurrency?: number,
  syncLocal?: boolean,
  sortFn?: ISortFunction,
  cache?: any;
  accessController?: OrbitDBAccessController<T>,
  recycle?: RecycleOptions,
  typeMap?: { [key: string]: Constructor<any> },
  onlyObserver?: boolean,
  onClose?: (store: Store<T, X, any>) => void,
  onDrop?: (store: Store<T, X, any>) => void,
  onLoad?: (store: Store<T, X, any>) => void,
  encyption?: {
    encrypt: (arr: Uint8Array) => Uint8Array
    decrypt: (arr: Uint8Array) => Uint8Array
  },
  io?: IOOptions<T>

}
export const JSON_ENCODER = {
  encoder: (obj) => new Uint8Array(Buffer.from(stringify(obj))),
  decoder: (obj) => JSON.parse(Buffer.from(obj).toString())
};

export const DefaultOptions: IStoreOptions<any, Index<any>> = {
  Index: Index,
  maxHistory: -1,
  fetchEntryTimeout: null,
  referenceCount: 32,
  replicationConcurrency: 32,
  syncLocal: false,
  sortFn: undefined,
  onlyObserver: false,
  typeMap: {},
  nameResolver: (name: string) => name,
  io: JSON_ENCODER
}
export interface Address {
  root: string;
  path: string;
  toString(): string;
};

export class Store<T, X extends Index<T>, O extends IStoreOptions<T, X>> {

  options: O;
  _type: string;
  id: string;
  identity: Identity;
  address: Address;
  dbname: string;
  events: EventEmitter;
  remoteHeadsPath: string;
  localHeadsPath: string;
  snapshotPath: string;
  queuePath: string;
  manifestPath: string;
  _ipfs: IPFS;
  _cache: any;
  access: OrbitDBAccessController<T>;
  _oplog: Log<any>;
  _queue: PQueue<any, any>
  _index: X;
  _replicationStatus: ReplicationInfo;
  _stats: any;
  _replicator: Replicator;
  _loader: Replicator;
  _key: string;

  constructor(ipfs: IPFS, identity: Identity, address: Address | string, options: O) {
    if (!identity) {
      throw new Error('Identity required')
    }

    // Set the options
    const opts = Object.assign({}, DefaultOptions) as O
    Object.assign(opts, options)
    this.options = opts

    // Default type
    this._type = 'store'

    // Create IDs, names and paths
    this.id = address.toString()
    this.identity = identity
    this.address = address as Address
    this.dbname = (address as Address).path || ''
    this.events = new EventEmitter()

    this.remoteHeadsPath = path.join(this.id, '_remoteHeads')
    this.localHeadsPath = path.join(this.id, '_localHeads')
    this.snapshotPath = path.join(this.id, 'snapshot')
    this.queuePath = path.join(this.id, 'queue')
    this.manifestPath = path.join(this.id, '_manifest')

    // External dependencies
    this._ipfs = ipfs
    this._cache = options.cache

    // Access mapping
    this.access = options.accessController || {
      canAppend: (entry: Entry, _identityProvider: Identities) => (entry.data.identity.publicKey === identity.publicKey),
      type: undefined,
      address: undefined,
      close: undefined,
      load: undefined,
      save: undefined
    } as OrbitDBAccessController<T>

    // Create the operations log
    this._oplog = new Log(this._ipfs, this.identity, this.logOptions)

    // _addOperation and log-joins queue. Adding ops and joins to the queue
    // makes sure they get processed sequentially to avoid race conditions
    // between writes and joins (coming from Replicator)
    this._queue = new PQueue({ concurrency: 1 })

    // Create the index
    this._index = new this.options.Index(this.address.root)

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
      const onReplicationQueued = async (entry) => {
        // Update the latest entry state (latest is the entry with largest clock time)
        this._recalculateReplicationMax(entry.data.clock ? entry.data.clock.time : 0)
        this.events.emit('replicate', this.address.toString(), entry)
      }

      const onReplicationProgress = async (entry) => {
        const previousProgress = this.replicationStatus.progress
        const previousMax = this.replicationStatus.max
        this._recalculateReplicationStatus(entry.data.clock.time)
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
              await this._cache.set(this.remoteHeadsPath, heads)
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
    this.events.on('write', (address, entry, heads) => {
      this._procEntry(entry)
    })
  }

  get type() {
    return this._type
  }

  get key() {
    return this._key
  }

  get index() {
    return this._index;
  }

  get logOptions() {
    return { logId: this.id, io: this.options.io, access: this.access, sortFn: this.options.sortFn, recycle: this.options.recycle };
  }

  /**
   * Returns the database's current replication status information
   * @return {[Object]} [description]
   */
  get replicationStatus() {
    return this._replicationStatus
  }

  setIdentity(identity) {
    this.identity = identity
    this._oplog.setIdentity(identity)
  }

  async close() {
    // Stop the Replicator
    await this._replicator.stop()

    // Wait for the operations queue to finish processing
    // to make sure everything that all operations that have
    // been queued will be written to disk
    await this._queue.onIdle()

    // Reset replication statistics
    this._replicationStatus.reset()

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
    if (this.access.close) {
      await this.access.close()
    }

    // Remove all event listeners
    for (const event in this.events["_events"]) {
      this.events.removeAllListeners(event)
    }

    this._oplog = null

    // Database is now closed
    // TODO: afaik we don't use 'closed' event anymore,
    // to be removed in future releases
    this.events.emit('closed', this.address.toString())
    return Promise.resolve()
  }

  /**
   * Drops a database and removes local data
   * @return {[None]}
   */
  async drop() {
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
    this._index = new this.options.Index(this.address.root)
    this._oplog = new Log(this._ipfs, this.identity, this.logOptions)
    this._cache = this.options.cache
  }

  async load(amount?: number, opts: { fetchEntryTimeout?: number } = {}) {
    if (typeof amount === 'object') {
      opts = amount
      amount = undefined
    }
    amount = amount || this.options.maxHistory
    const fetchEntryTimeout = opts.fetchEntryTimeout || this.options.fetchEntryTimeout

    if (this.options.onLoad) {
      await this.options.onLoad(this)
    }
    const localHeads = await this._cache.get(this.localHeadsPath) || []
    const remoteHeads = await this._cache.get(this.remoteHeadsPath) || []
    const heads = localHeads.concat(remoteHeads)

    if (heads.length > 0) {
      this.events.emit('load', this.address.toString(), heads)
    }

    // Update the replication status from the heads
    heads.forEach(h => this._recalculateReplicationMax(h.clock.time))

    // Load the log
    const log = await Log.fromEntryHash(this._ipfs, this.identity, heads.map(e => e.hash), {
      ...this.logOptions,
      length: amount,
      timeout: fetchEntryTimeout,
    })

    this._oplog = log

    // Update the index
    if (heads.length > 0) {
      await this._updateIndex()
    }

    this.events.emit('ready', this.address.toString(), this._oplog.heads)
  }

  async sync(heads: Entry[]) {
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

    const saveToIpfs = async (head: Entry) => {
      if (!head) {
        console.warn("Warning: Given input entry was 'null'.")
        return Promise.resolve(null)
      }

      const identityProvider = this.identity.provider
      if (!identityProvider) throw new Error('Identity-provider is required, cannot verify entry')

      // TODO Fix types
      const canAppend = await this.access.canAppend(head, identityProvider as any)
      if (!canAppend) {
        logger.info('Warning: Given input entry is not allowed in this log and was discarded (no write access).')
        return Promise.resolve(null)
      }

      const logEntry = Entry.toEntry(head)
      const hash = await io.write(this._ipfs, Entry.getWriteFormat(), logEntry, { links: Entry.IPLD_LINKS, onlyHash: true })

      if (hash !== head.hash) {
        console.warn('"WARNING! Head hash didn\'t match the contents')
      }

      return head
    }

    return mapSeries(heads, saveToIpfs)
      .then(async (saved) => {
        return this._replicator.load(saved.filter(e => e !== null))
      })
  }

  loadMoreFrom(amount, entries) {
    this._replicator.load(entries)
  }

  async saveSnapshot() {
    const unfinished = this._replicator.unfinished

    const snapshotData = this._oplog.toSnapshot()
    const buf = Buffer.from(JSON.stringify({
      id: snapshotData.id,
      heads: snapshotData.heads,
      size: snapshotData.values.length,
      values: snapshotData.values,
      type: this.type
    }))

    const snapshot = await this._ipfs.add(buf)

    snapshot["hash"] = snapshot.cid.toString() // js-ipfs >= 0.41, ipfs.add results contain a cid property (a CID instance) instead of a string hash property
    await this._cache.set(this.snapshotPath, snapshot)
    await this._cache.set(this.queuePath, unfinished)

    logger.debug(`Saved snapshot: ${snapshot["hash"]}, queue length: ${unfinished.length}`)

    return [snapshot]
  }

  async loadFromSnapshot() {
    if (this.options.onLoad) {
      await this.options.onLoad(this)
    }

    this.events.emit('load', this.address.toString()) // TODO emits inconsistent params, missing heads param

    const maxClock = (res, val) => Math.max(res, val.clock.time)

    const queue = await this._cache.get(this.queuePath)
    this.sync(queue || [])

    const snapshot = await this._cache.get(this.snapshotPath)

    if (snapshot) {
      const chunks = []
      for await (const chunk of this._ipfs.cat(snapshot.hash)) {
        chunks.push(chunk)
      }
      const buffer = Buffer.concat(chunks)
      const snapshotData = JSON.parse(buffer.toString())

      // Fetch the entries
      // Timeout 1 sec to only load entries that are already fetched (in order to not get stuck at loading)
      this._recalculateReplicationMax(snapshotData.values.reduce(maxClock, 0))
      if (snapshotData) {
        this._oplog = await Log.fromJSON(this._ipfs, this.identity, snapshotData, {
          access: this.access,
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

  async _updateIndex() {
    await this._index.updateIndex(this._oplog)
  }

  async syncLocal() {
    const localHeads = await this._cache.get(this.localHeadsPath) || []
    const remoteHeads = await this._cache.get(this.remoteHeadsPath) || []
    const heads = localHeads.concat(remoteHeads)
    for (let i = 0; i < heads.length; i++) {
      const head = heads[i]
      if (!this._oplog.heads.includes(head)) {
        await this.load()
        break
      }
    }
  }

  async _addOperation(data, options: { onProgressCallback?: (any) => void, pin?: boolean } = {}) {
    async function addOperation() {
      if (this._oplog) {
        // check local cache for latest heads
        if (this.options.syncLocal) {
          await this.syncLocal()
        }
        const entry = await this._oplog.append(data, this.options.referenceCount, options.pin)
        this._recalculateReplicationStatus(entry.data.clock.time)
        await this._cache.set(this.localHeadsPath, [entry])
        await this._updateIndex()
        this.events.emit('write', this.address.toString(), entry, this._oplog.heads)
        if (options?.onProgressCallback) options.onProgressCallback(entry)
        return entry.hash
      }
    }
    return this._queue.add(addOperation.bind(this))
  }

  _addOperationBatch(data, batchOperation, lastOperation, onProgressCallback) {
    throw new Error('Not implemented!')
  }

  _procEntry(entry) {
    const { payload, hash } = entry
    const { op } = payload
    if (op) {
      this.events.emit(`log.op.${op}`, this.address.toString(), hash, payload)
    } else {
      this.events.emit('log.op.none', this.address.toString(), hash, payload)
    }
    this.events.emit('log.op', op, this.address.toString(), hash, payload)
  }

  /* Replication Status state updates */
  _recalculateReplicationProgress() {
    this._replicationStatus.progress = Math.max(
      Math.min(this._replicationStatus.progress + 1, this._replicationStatus.max),
      this._oplog ? this._oplog.length : 0
    )
  }

  _recalculateReplicationMax(max) {
    this._replicationStatus.max = Math.max.apply(null, [
      this.replicationStatus.max,
      this._oplog ? this._oplog.length : 0,
      (max || 0)
    ])
  }

  _recalculateReplicationStatus(maxTotal) {
    this._recalculateReplicationMax(maxTotal)
    this._recalculateReplicationProgress()
  }

  /* Loading progress callback */
  _onLoadProgress(entry) {
    this._recalculateReplicationStatus(entry.data.clock.time)
    this.events.emit('load.progress', this.address.toString(), entry.hash, entry, this.replicationStatus.progress, this.replicationStatus.max)
  }
}
