import path from "path";
import mapSeries from "p-each-series";
import PQueue from "p-queue";
import {
    Log,
    ISortFunction,
    PruneOptions,
    LogOptions,
    Identity,
    max,
    CanAppend,
    JSON_ENCODING,
} from "@dao-xyz/ipfs-log";
import { Encoding, EncryptionTemplateMaybeEncrypted } from "@dao-xyz/ipfs-log";
import { Entry } from "@dao-xyz/ipfs-log";
import { Replicator } from "./replicator.js";
import io from "@dao-xyz/peerbit-io-utils";
import Cache from "@dao-xyz/peerbit-cache";
import { variant, field, vec, Constructor } from "@dao-xyz/borsh";
import { IPFS } from "ipfs-core-types";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Snapshot } from "./snapshot.js";
import {
    AccessError,
    PublicKeyEncryptionResolver,
} from "@dao-xyz/peerbit-crypto";
import { joinUint8Arrays } from "@dao-xyz/peerbit-borsh-utils";
import { EntryWithRefs } from "./entry-with-refs.js";
import { waitForAsync } from "@dao-xyz/peerbit-time";

import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
const logger = loggerFn({ module: "store" });

export class CachedValue {}

export type AddOperationOptions<T> = {
    skipCanAppendCheck?: boolean;
    identity?: Identity;
    nexts?: Entry<T>[];
    onProgressCallback?: (any: any) => void;
    pin?: boolean;
    reciever?: EncryptionTemplateMaybeEncrypted;
};

@variant(0)
export class CID extends CachedValue {
    @field({ type: "string" })
    hash: string;

    constructor(opts?: { hash: string }) {
        super();
        if (opts) {
            this.hash = opts.hash;
        }
    }
}

@variant(1)
export class UnsfinishedReplication extends CachedValue {
    @field({ type: vec("string") })
    hashes: string[];

    constructor(opts?: { hashes: string[] }) {
        super();
        if (opts) {
            this.hashes = opts.hashes;
        }
    }
}

@variant(2)
export class HeadsCache<T> extends CachedValue {
    @field({ type: vec(Entry) })
    heads: Entry<T>[];

    constructor(opts?: { heads: Entry<T>[] }) {
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

    onClose?: (store: Store<T>) => void;
    onDrop?: (store: Store<T>) => void;
    onLoad?: (store: Store<T>) => void;
    onLoadProgress?: (store: Store<T>, entry: Entry<T>) => void;
    onWrite?: (store: Store<T>, _entry: Entry<T>) => void;
    onOpen?: (store: Store<any>) => Promise<void>;
    onReplicationQueued?: (store: Store<any>, entry: Entry<T>) => void;
    onReplicationProgress?: (store: Store<any>, entry: Entry<T>) => void;
    onReplicationComplete?: (store: Store<any>) => void;
    onReady?: (store: Store<T>) => void;

    encryption?: PublicKeyEncryptionResolver;
    maxHistory?: number;
    fetchEntryTimeout?: number;
    referenceCount?: number;
    replicationConcurrency?: number;
    syncLocal?: boolean;
    sortFn?: ISortFunction;
    prune?: PruneOptions;
    onUpdate?: (oplog: Log<T>, entries?: Entry<T>[]) => void;
}

export interface IInitializationOptions<T>
    extends IStoreOptions<T>,
        IInitializationOptionsDefault<T> {
    resolveCache: (
        store: Store<any>
    ) => Promise<Cache<CachedValue>> | Cache<CachedValue>;
}

interface IInitializationOptionsDefault<T> {
    maxHistory?: number;
    referenceCount?: number;
    replicationConcurrency?: number;
    typeMap?: { [key: string]: Constructor<any> };
}

export const DefaultOptions: IInitializationOptionsDefault<any> = {
    maxHistory: -1,
    referenceCount: 32,
    replicationConcurrency: 32,
    typeMap: {},
    /* nameResolver: (id: string) => name, */
};

export interface Initiable<T> {
    init?(
        ipfs: IPFS,
        identity: Identity,
        options: IInitializationOptions<T>
    ): Promise<this>;
}

@variant(0)
export class Store<T> implements Initiable<T> {
    @field({ type: "u32" })
    _storeIndex: number; // how to ensure unqiueness

    _canAppend?: CanAppend<T>;
    _onUpdate?: (oplog: Log<T>, entries?: Entry<T>[]) => Promise<void> | void;
    _onUpdateOption?: (
        oplog: Log<T>,
        entries?: Entry<T>[]
    ) => Promise<void> | void;

    // An access controller that is note part of the store manifest, usefull for circular store -> access controller -> store structures

    _options: IInitializationOptions<T>;
    identity: Identity;

    /*   events: EventEmitter;
     */
    remoteHeadsPath: string;
    localHeadsPath: string;
    snapshotPath: string;
    queuePath: string;
    initialized: boolean;
    encoding: Encoding<T> = JSON_ENCODING;

    /*   allowForks: boolean = true;
     */

    _ipfs: IPFS;
    _cache: Cache<CachedValue>;
    _oplog: Log<T>;
    _queue: PQueue<any, any>;
    /*     _replicationStatus: ReplicationInfo; */
    _stats: any;
    _replicator: Replicator<T>;
    _loader: Replicator<T>;
    _key: string;

    constructor(properties?: { storeIndex: number }) {
        if (properties) {
            this._storeIndex = properties?.storeIndex;
        }
    }

    setup(properties: {
        encoding: Encoding<T>;
        canAppend: CanAppend<T>;
        onUpdate: (oplog: Log<T>, entries?: Entry<T>[]) => void;
    }) {
        this.encoding = properties.encoding;
        this.onUpdate = properties.onUpdate;
        this.canAppend = properties.canAppend;
    }

    async init(
        ipfs: IPFS,
        identity: Identity,
        options: IInitializationOptions<T>
    ): Promise<this> {
        if (this.initialized) {
            throw new Error("Already initialized");
        }

        // Set ipfs since we are to save the store
        this._ipfs = ipfs;

        // Set the options (we will use the topic property after thiis)
        const opts = { ...DefaultOptions, ...options };
        this._options = opts;

        // Create IDs, names and paths
        this.identity = identity;
        this._onUpdateOption = options.onUpdate;

        /* this.events = new EventEmitter() */
        this.remoteHeadsPath = path.join(this.id, "_remoteHeads");
        this.localHeadsPath = path.join(this.id, "_localHeads");
        this.snapshotPath = path.join(this.id, "snapshot");
        this.queuePath = path.join(this.id, "queue");

        // External dependencies
        this._cache = await this._options.resolveCache(this);

        // Create the operations log
        this._oplog = new Log<T>(this._ipfs, identity, this.logOptions);

        // _addOperation and log-joins queue. Adding ops and joins to the queue
        // makes sure they get processed sequentially to avoid race conditions
        // between writes and joins (coming from Replicator)
        this._queue = new PQueue({ concurrency: 1 });

        // Replication progress info
        /*         this._replicationStatus = new ReplicationInfo();
         */
        // Statistics
        this._stats = {
            snapshot: {
                bytesLoaded: -1,
            },
            syncRequestsReceieved: 0,
        };

        try {
            const onReplicationQueued = async (entry: Entry<T>) => {
                // Update the latest entry state (latest is the entry with largest clock time)
                const e = entry;
                try {
                    await e.getClock();
                    /* this._recalculateReplicationMax(e.metadata.maxChainLength); */
                    this._options.onReplicationQueued &&
                        this._options.onReplicationQueued(this, e);
                } catch (error) {
                    if (error instanceof AccessError) {
                        logger.info(
                            "Failed to access clock of entry: " + e.hash
                        );
                        return; // Ignore, we cant access clock
                    }
                    throw error;
                }
            };

            const onReplicationProgress = async (entry: Entry<T>) => {
                const log = this._oplog;
                if (!log) {
                    logger.warn(
                        "Recieved replication event after close: " + entry.hash
                    );
                    return; // closed
                }
                this._options.onReplicationProgress &&
                    this._options.onReplicationProgress(this, entry);
            };

            // Create the replicator
            this._replicator = new Replicator(
                this,
                this._options.replicationConcurrency
            );
            // For internal backwards compatibility,
            // to be removed in future releases
            this._loader = this._replicator;
            // Hook up the callbacks to the Replicator
            this._replicator.onReplicationQueued = onReplicationQueued;
            this._replicator.onReplicationProgress = onReplicationProgress;
            this._replicator.onReplicationComplete = (logs: Log<T>[]) => {
                this._queue.add(
                    (() => this.updateStateFromLogs(logs)).bind(this)
                );
            };
        } catch (e) {
            console.error("Store Error:", e);
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
        if (this._oplog && logs.length > 0) {
            try {
                for (const log of logs) {
                    await this._oplog.join(log);
                }
            } catch (error: any) {
                if (error instanceof AccessError) {
                    logger.info(error.message);
                    return;
                }
                throw error;
            }

            // only store heads that has been verified and merges
            const heads = this._oplog.heads;
            await this._cache.setBinary(
                this.remoteHeadsPath,
                new HeadsCache({ heads })
            );
            logger.debug(
                `Saved heads ${heads.length} [${heads
                    .map((e) => e.hash)
                    .join(", ")}]`
            );

            // update the store's index after joining the logs
            // and persisting the latest heads
            await this._updateIndex();

            /*   if (this._oplog.length > this.replicationStatus.progress) {
                  this._recalculateReplicationStatus(heads, BigInt(this._oplog.length));
              } */
            this._options.onReplicationComplete &&
                this._options.onReplicationComplete(this);
        }
    };

    get id(): string {
        if (typeof this._storeIndex !== "number") {
            throw new Error("Store index not set");
        }
        return this._storeIndex.toString();
    }

    get oplog(): Log<any> {
        return this._oplog;
    }

    get key() {
        return this._key;
    }

    get logOptions(): LogOptions<T> {
        return {
            logId: this.id,
            encryption: this._options.encryption,
            encoding: this.encoding,
            sortFn: this._options.sortFn,
            prune: this._options.prune,
        };
    }

    /**
     * Returns the database's current replication status information
     * @return {[Object]} [description]
     */
    /*     get replicationStatus() {
            return this._replicationStatus;
        } */

    setIdentity(identity: Identity) {
        this.identity = identity;
        this._oplog.setIdentity(identity);
    }

    set canAppend(canAppend: CanAppend<T> | undefined) {
        this._canAppend = canAppend;
    }

    get canAppend(): CanAppend<T> | undefined {
        return this._canAppend;
    }

    set onUpdate(onUpdate: (oplog: Log<T>, entries?: Entry<T>[]) => void) {
        this._onUpdate = onUpdate;
    }

    async close() {
        if (!this.initialized) {
            return;
        }

        // Stop the Replicator
        await this._replicator?.stop();

        // Wait for the operations queue to finish processing
        // to make sure everything that all operations that have
        // been queued will be written to disk
        await this._queue?.onIdle();

        // Reset replication statistics
        /*  this._replicationStatus?.reset();
         */
        // Reset database statistics
        this._stats = {
            snapshot: {
                bytesLoaded: -1,
            },
            syncRequestsReceieved: 0,
        };

        if (this._options.onClose) {
            await this._options.onClose(this);
        }

        this._oplog = null as any;

        // Database is now closed

        return Promise.resolve();
    }

    /**
     * Drops a database and removes local data
     */
    async drop() {
        if (!this._oplog && !this._cache) {
            return; // already dropped
        }

        if (this._options.onDrop) {
            await this._options.onDrop(this);
        }

        await this._cache.del(this.localHeadsPath);
        await this._cache.del(this.remoteHeadsPath);
        await this._cache.del(this.snapshotPath);
        await this._cache.del(this.queuePath);

        await this.close();

        // Reset
        // TODO fix types
        this._oplog = undefined as any;
        this._cache = undefined as any;

        this.initialized = false; // call this last because (close() expect initialized to be able to function)
    }

    async load(amount?: number, opts: { fetchEntryTimeout?: number } = {}) {
        if (!this.initialized) {
            throw new Error("Store needs to be initialized before loaded");
        }

        amount = amount || this._options.maxHistory;
        const fetchEntryTimeout =
            opts.fetchEntryTimeout || this._options.fetchEntryTimeout;

        if (this._options.onLoad) {
            await this._options.onLoad(this);
        }
        const localHeads: Entry<any>[] =
            (await this._cache.getBinary(this.localHeadsPath, HeadsCache))
                ?.heads || [];
        const remoteHeads: Entry<any>[] =
            (await this._cache.getBinary(this.remoteHeadsPath, HeadsCache))
                ?.heads || [];
        const heads = localHeads.concat(remoteHeads);

        // Update the replication status from the heads
        /*    for (const head of heads) {
               this._recalculateReplicationMax((await head.maxChainLength));
           } */

        // Load the log
        const log = await Log.fromEntry(this._ipfs, this.identity, heads, {
            ...this.logOptions,
            length: amount,
            timeout: fetchEntryTimeout,
            onProgressCallback: this._onLoadProgress.bind(this),
            concurrency: this._options.replicationConcurrency,
        });

        this._oplog = log;

        // Update the index
        if (heads.length > 0) {
            await this._updateIndex();
        }

        this._replicator.start();
        this._options.onReady && this._options.onReady(this);
    }

    /**
     *
     * @param heads
     * @returns true, synchronization resolved in new entries
     */
    async sync(
        heads: (EntryWithRefs<T> | Entry<T>)[],
        options: { save: boolean } = { save: true }
    ): Promise<boolean> {
        this._stats.syncRequestsReceieved += 1;
        logger.debug(
            `Sync request #${this._stats.syncRequestsReceieved} ${heads.length}`
        );
        if (heads.length === 0) {
            return false;
        }

        const handle = async (headToHandle: EntryWithRefs<T> | Entry<T>) => {
            const allEntries =
                headToHandle instanceof Entry
                    ? [headToHandle]
                    : [headToHandle.entry, ...headToHandle.references];
            await Promise.all(
                allEntries.map((h) =>
                    h.init({
                        encoding: this.oplog._encoding,
                        encryption: this.oplog._encryption,
                    })
                )
            );
            if (
                this.canAppend &&
                !(await this.canAppend(
                    headToHandle instanceof Entry
                        ? headToHandle
                        : headToHandle.entry
                ))
            ) {
                return Promise.resolve(null);
            }
            if (options.save) {
                await Promise.all(
                    allEntries.map(async (head) => {
                        const headHash = head.hash;
                        head.hash = undefined as any;
                        const hash = await io.write(
                            this._ipfs,
                            "raw",
                            serialize(head)
                        );
                        head.hash = headHash;
                        if (head.hash === undefined) {
                            head.hash = hash; // can happen if you sync entries that you load directly from ipfs
                        } else if (hash !== head.hash) {
                            logger.error("Head hash didn't match the contents");
                            throw new Error(
                                "Head hash didn't match the contents"
                            );
                        }
                    })
                );
            }

            return headToHandle;
        };
        const hash = (entry: EntryWithRefs<T> | Entry<T>) => {
            if (entry instanceof Entry) {
                return entry.hash;
            }
            return entry.entry.hash;
        };
        const newHeads = heads.filter(
            (e) => !hash(e) || !this.oplog.has(hash(e))
        );
        if (newHeads.length === 0) {
            return false;
        }
        await mapSeries(newHeads, handle).then(async (saved) => {
            return this._replicator.load(saved.filter((e) => e !== null));
        });
        return true;
    }

    loadMoreFrom(entries: string[] | Entry<any>[] | EntryWithRefs<any>[]) {
        this._replicator.load(entries);
    }

    get replicate(): boolean {
        return !!this._options.replicate;
    }

    async getCachedHeads(): Promise<Entry<T>[]> {
        if (!this._cache) {
            return [];
        }
        const localHeads = ((
            await this._cache.getBinary(this.localHeadsPath, HeadsCache)
        )?.heads || []) as Entry<T>[];
        const remoteHeads = ((
            await this._cache.getBinary(this.remoteHeadsPath, HeadsCache)
        )?.heads || []) as Entry<T>[];
        return [...localHeads, ...remoteHeads];
    }

    async saveSnapshot() {
        const unfinished = this._replicator.unfinished;
        const snapshotData = this._oplog.toSnapshot();
        const buf = serialize(
            new Snapshot({
                id: snapshotData.id,
                heads: snapshotData.heads,
                size: BigInt(snapshotData.values.length),
                values: snapshotData.values,
            })
        );

        const snapshot = await this._ipfs.add(buf);

        await this._cache.setBinary(
            this.snapshotPath,
            new CID({ hash: snapshot.cid.toString() })
        );
        await this._cache.setBinary(
            this.queuePath,
            new UnsfinishedReplication({ hashes: unfinished })
        );
        await waitForAsync(
            async () =>
                (await this._cache.getBinary(this.snapshotPath, CID)) !==
                undefined,
            { delayInterval: 200, timeout: 10 * 1000 }
        );

        logger.debug(
            `Saved snapshot: ${snapshot.cid.toString()}, queue length: ${
                unfinished.length
            }`
        );
        return [snapshot];
    }

    async loadFromSnapshot() {
        if (this._options.onLoad) {
            await this._options.onLoad(this);
        }

        const maxChainLength = (res: bigint, val: Entry<any>): bigint =>
            max(res, val.metadata.maxChainLength);
        await this.sync([]);

        const queue = (
            await this._cache.getBinary(this.queuePath, UnsfinishedReplication)
        )?.hashes as string[];
        if (queue?.length > 0) {
            this._replicator.load(queue);
        }

        const snapshotCID = await this._cache.getBinary(this.snapshotPath, CID);
        if (snapshotCID) {
            const chunks: any[] = [];
            for await (const chunk of this._ipfs.cat(snapshotCID.hash)) {
                chunks.push(chunk);
            }
            const snapshotData = deserialize(joinUint8Arrays(chunks), Snapshot);

            // Fetch the entries
            // Timeout 1 sec to only load entries that are already fetched (in order to not get stuck at loading)
            /*   this._recalculateReplicationMax(
                  snapshotData.values.reduce(maxChainLength, 0n)
              ); */
            if (snapshotData) {
                this._oplog = await Log.fromEntry(
                    this._ipfs,
                    this.identity,
                    snapshotData.heads,
                    {
                        sortFn: this._options.sortFn,
                        length: -1,
                        timeout: 1000,
                        onProgressCallback: this._onLoadProgress.bind(this),
                    }
                );
                await this._updateIndex();
                this._options.onReplicationComplete &&
                    this._options.onReplicationComplete(this);
            }
            this._options.onReady && this._options.onReady(this);
        } else {
            throw new Error(`Snapshot for ${this.id} not found!`);
        }

        return this;
    }

    async _updateIndex(entries?: Entry<T>[]) {
        // TODO add better error handling
        try {
            if (this._onUpdate) {
                await this._onUpdate(this._oplog, entries);
            }
        } catch (error) {
            if (error instanceof AccessError) {
                // fail silently for now
                logger.info("Could not update index due to AccessError");
            } else {
                throw error;
            }
        }

        try {
            if (this._onUpdateOption) {
                await this._onUpdateOption(this._oplog, entries);
            }
        } catch (error) {
            if (error instanceof AccessError) {
                // fail silently for now
                logger.info("Could not update index due to AccessError");
            } else {
                throw error;
            }
        }
    }

    async syncLocal() {
        const localHeads =
            (await this._cache.getBinary(this.localHeadsPath, HeadsCache))
                ?.heads || [];
        const remoteHeads =
            (await this._cache.getBinary(this.remoteHeadsPath, HeadsCache))
                ?.heads || [];
        const heads = localHeads.concat(remoteHeads);
        const headsHashes = new Set(this._oplog.heads.map((h) => h.hash));
        for (let i = 0; i < heads.length; i++) {
            const head = heads[i];
            if (!headsHashes.has(head.hash)) {
                await this.load();
                break;
            }
        }
    }

    async _addOperation(
        data: T,
        options?: AddOperationOptions<T>
    ): Promise<Entry<T>> {
        const addOperation = async () => {
            // check local cache for latest heads
            if (this._options.syncLocal) {
                await this.syncLocal();
            }

            const entry = await this._oplog.append(data, {
                nexts: options?.nexts,
                pin: options?.pin,
                reciever: options?.reciever,
                canAppend: options?.skipCanAppendCheck
                    ? undefined
                    : this.canAppend,
                identity: options?.identity,
            });

            await this._cache.setBinary(
                this.localHeadsPath,
                new HeadsCache({ heads: [entry] })
            );
            await this._updateIndex([entry]);

            // The row below will emit an "event", which is subscribed to on the orbit-db client (confusing enough)
            // there, the write is binded to the pubsub publish, with the entry. Which will send this entry
            // to all the connected peers to tell them that a new entry has been added
            // TODO: don't use events, or make it more transparent that there is a vital subscription in the background
            // that is handling replication
            this._options.onWrite && this._options.onWrite(this, entry);
            if (options?.onProgressCallback) options.onProgressCallback(entry);
            return entry;
        };
        return this._queue.add(addOperation.bind(this));
    }

    /* Loading progress callback */
    _onLoadProgress(entry: Entry<any>) {
        this._options.onLoadProgress &&
            this._options.onLoadProgress(this, entry);
    }

    clone(): Store<T> {
        return deserialize(
            serialize(this),
            this.constructor as any as Constructor<any>
        );
    }
}
