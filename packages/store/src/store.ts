import mapSeries from "p-each-series";
import PQueue from "p-queue";
import {
    Log,
    ISortFunction,
    PruneOptions,
    LogOptions,
    Identity,
    CanAppend,
    JSON_ENCODING,
} from "@dao-xyz/peerbit-log";
import {
    Encoding,
    EncryptionTemplateMaybeEncrypted,
} from "@dao-xyz/peerbit-log";
import { Entry } from "@dao-xyz/peerbit-log";
import { stringifyCid, Blocks } from "@dao-xyz/peerbit-block";
import Cache from "@dao-xyz/peerbit-cache";
import { variant, option, field, vec, Constructor } from "@dao-xyz/borsh";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { Snapshot } from "./snapshot.js";
import {
    AccessError,
    PublicKeyEncryptionResolver,
} from "@dao-xyz/peerbit-crypto";
import { EntryWithRefs } from "./entry-with-refs.js";
import { waitForAsync } from "@dao-xyz/peerbit-time";
import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
import path from "path-browserify";
import { v4 as uuid } from "uuid";
import { join } from "./replicator.js";

const logger = loggerFn({ module: "store" });

export class CachedValue {}

export type AddOperationOptions<T> = {
    skipCanAppendCheck?: boolean;
    identity?: Identity;
    nexts?: Entry<T>[];
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
    @field({ type: vec("string") })
    heads: string[];

    @field({ type: option("string") })
    last?: string;

    constructor(heads: string[], last?: string) {
        super();
        this.heads = heads;
        this.last = last;
    }
}

export interface IStoreOptions<T> {
    /**
     * f set to true, will throw an error if the database can't be found locally. (Default: false)
     */
    localOnly?: boolean;

    /**
     * The directory where data will be stored
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
    onReplicationFetch?: (store: Store<any>, entry: Entry<T>) => void;
    onReplicationComplete?: (store: Store<any>) => void;
    onReady?: (store: Store<T>) => void;
    saveFile?: (file: any) => Promise<string>;
    loadFile?: (cid: string) => Promise<Uint8Array>;
    encryption?: PublicKeyEncryptionResolver;
    maxHistory?: number;
    fetchEntryTimeout?: number;
    replicationConcurrency?: number;
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
    replicationConcurrency?: number;
    typeMap?: { [key: string]: Constructor<any> };
}

export const DefaultOptions: IInitializationOptionsDefault<any> = {
    maxHistory: -1,
    replicationConcurrency: 32,
    typeMap: {},
};

export interface Initiable<T> {
    init?(
        blockStore: Blocks,
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
    headsPath: string;
    snapshotPath: string;
    initialized: boolean;
    encoding: Encoding<T> = JSON_ENCODING;

    _store: Blocks;
    _cache: Cache<CachedValue>;
    _oplog: Log<T>;
    _queue: PQueue<any, any>;
    _stats: any;
    _key: string;

    _saveFile: (file: any) => Promise<string>;
    _loadFile: (cid: string) => Promise<Uint8Array | undefined>;

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
        store: Blocks,
        identity: Identity,
        options: IInitializationOptions<T>
    ): Promise<this> {
        if (this.initialized) {
            throw new Error("Already initialized");
        }

        this._saveFile =
            options.saveFile || ((file) => store.put(file, "dag-cbor"));
        this._loadFile = options.loadFile || ((file) => store.get(file));

        // Set ipfs since we are to save the store
        this._store = store;

        // Set the options (we will use the topic property after thiis)
        const opts = { ...DefaultOptions, ...options };
        this._options = opts;

        // Create IDs, names and paths
        this.identity = identity;
        this._onUpdateOption = options.onUpdate;

        /* this.events = new EventEmitter() */
        this.headsPath = path.join(this.id, "_heads");
        this.snapshotPath = path.join(this.id, "snapshot");

        // External dependencies
        this._cache = await this._options.resolveCache(this);

        // Create the operations log
        this._oplog = new Log<T>(this._store, identity, this.logOptions);

        // _addOperation and log-joins queue. Adding ops and joins to the queue
        // makes sure they get processed sequentially to avoid race conditions
        // between writes and joins (coming from Replicator)
        this._queue = new PQueue({ concurrency: 1 });

        this._stats = {
            snapshot: {
                bytesLoaded: -1,
            },
            syncRequestsReceieved: 0,
        };

        if (this._options.onOpen) {
            await this._options.onOpen(this);
        }
        this.initialized = true;

        return this;
    }

    async updateHeadsCache(newHeads: string[], reset?: boolean) {
        // If 'reset' then dont keep references to old heads caches, assume new cache will fully describe all heads
        const last = reset
            ? undefined
            : await this._cache.get<string>(this.headsPath);
        const newHeadsPath = path.join(this.headsPath, uuid());
        await this._cache.set(this.headsPath, newHeadsPath);

        await this._cache.setBinary(
            newHeadsPath,
            new HeadsCache(newHeads, last)
        );
    }

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

        // Wait for the operations queue to finish processing
        // to make sure everything that all operations that have
        // been queued will be written to disk
        await this._queue?.onIdle();

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

        await this._cache.del(this.headsPath);
        await this._cache.del(this.snapshotPath);

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

        const heads = await this.getCachedHeads();

        // Load the log
        const log = await Log.fromEntryHash(this._store, this.identity, heads, {
            ...this.logOptions,
            length: amount,
            timeout: fetchEntryTimeout,
            onFetched: this._onLoadProgress.bind(this),
            concurrency: this._options.replicationConcurrency,
        });

        this._oplog = log;

        // Update the index
        if (heads.length > 0) {
            await this._updateIndex();
        }

        this._options.onReady && this._options.onReady(this);
    }

    /**
     *
     * @param entries
     * @returns change
     */
    async sync(
        entries: EntryWithRefs<T>[] | Entry<T>[] | string[],
        options: { canAppend?: CanAppend<T>; save: boolean } = { save: true }
    ): Promise<boolean> {
        this._stats.syncRequestsReceieved += 1;
        logger.debug(
            `Sync request #${this._stats.syncRequestsReceieved} ${entries.length}`
        );
        if (entries.length === 0) {
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
            const entry =
                headToHandle instanceof Entry
                    ? headToHandle
                    : headToHandle.entry;

            this._options.onReplicationQueued &&
                this._options.onReplicationQueued(this, entry);

            const canAppend = options?.canAppend || this.canAppend;
            if (canAppend && !(await canAppend(entry))) {
                logger.debug("Not allowd to append head " + entry.hash);
                return Promise.resolve(null);
            }
            await Promise.all(
                allEntries.map(async (head) => {
                    const headHash = head.hash;
                    head.hash = undefined as any;
                    try {
                        const hash = options?.save
                            ? await this._store.put(serialize(head), "raw")
                            : stringifyCid(
                                  (
                                      await this._store.block(
                                          serialize(head),
                                          "raw"
                                      )
                                  ).cid
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
                    } catch (error) {
                        logger.error(error);
                        throw error;
                    }
                })
            );

            return headToHandle;
        };
        const hash = (entry: EntryWithRefs<T> | Entry<T> | string) => {
            if (entry instanceof Entry) {
                return entry.hash;
            } else if (typeof entry === "string") {
                return entry;
            }
            return entry.entry.hash;
        };

        const newEntries: (Entry<T> | EntryWithRefs<T>)[] = [];
        for (const entry of entries) {
            const h = hash(entry);
            if (h && this.oplog.has(h)) {
                continue;
            }
            newEntries.push(
                typeof entry === "string"
                    ? await Entry.fromMultihash(this._store, entry)
                    : entry
            );
        }

        if (newEntries.length === 0) {
            return false;
        }

        const saved = await mapSeries(newEntries, handle);
        const { change } = await join(
            saved as EntryWithRefs<T>[] | Entry<T>[],
            this._oplog,
            {
                concurrency: this._options.replicationConcurrency,
                onFetched: (entry) =>
                    this._options.onReplicationFetch &&
                    this._options.onReplicationFetch(this, entry),
            }
        );

        // TODO add head cache 'reset' so that it becomes more accurate over time
        await this.updateHeadsCache(newEntries.map((x) => hash(x)));
        await this._updateIndex(change);
        this._options.onReplicationComplete &&
            this._options.onReplicationComplete(this);
        return true;
    }

    get replicate(): boolean {
        return !!this._options.replicate;
    }

    async getCachedHeads(): Promise<string[]> {
        if (!this._cache) {
            return [];
        }
        const result: string[] = [];
        let next = await this._cache.get<string>(this.headsPath);
        while (next) {
            const cache = await this._cache.getBinary(next, HeadsCache);
            next = cache?.last;
            cache?.heads.forEach((head) => {
                result.push(head);
            });
        }
        return result;
    }

    async saveSnapshot() {
        const snapshotData = this._oplog.toSnapshot();
        const buf = serialize(
            new Snapshot({
                id: snapshotData.id,
                heads: snapshotData.heads,
                size: BigInt(snapshotData.values.length),
                values: snapshotData.values,
            })
        );

        const snapshot = await this._saveFile(buf);
        await this._cache.setBinary(
            this.snapshotPath,
            new CID({ hash: snapshot })
        );

        await waitForAsync(
            async () =>
                (await this._cache.getBinary(this.snapshotPath, CID)) !==
                undefined,
            { delayInterval: 200, timeout: 10 * 1000 }
        );

        logger.debug(`Saved snapshot: ${snapshot}`);
        return [snapshot];
    }

    async loadFromSnapshot() {
        if (this._options.onLoad) {
            await this._options.onLoad(this);
        }
        await this.sync([]);

        const snapshotCID = await this._cache.getBinary(this.snapshotPath, CID);
        if (snapshotCID) {
            const file = await this._loadFile(snapshotCID.hash);
            if (!file) {
                throw new Error("Missing snapshot");
            }
            const snapshotData = deserialize(file, Snapshot);

            // Fetch the entries
            // Timeout 1 sec to only load entries that are already fetched (in order to not get stuck at loading)
            if (snapshotData) {
                this._oplog = await Log.fromEntry(
                    this._store,
                    this.identity,
                    snapshotData.heads,
                    {
                        sortFn: this._options.sortFn,
                        length: -1,
                        timeout: 1000,
                        onFetched: this._onLoadProgress.bind(this),
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

    async _addOperation(
        data: T,
        options?: AddOperationOptions<T>
    ): Promise<Entry<T>> {
        const isReferencingAllHeads = !options?.nexts;
        const entry = await this._oplog.append(data, {
            nexts: options?.nexts,
            pin: options?.pin,
            reciever: options?.reciever,
            canAppend: options?.skipCanAppendCheck ? undefined : this.canAppend,
            identity: options?.identity,
        });
        logger.debug("Appended entry with hash: " + entry.hash);
        await this.updateHeadsCache([entry.hash], isReferencingAllHeads);
        await this._updateIndex([entry]);

        // The row below will emit an "event", which is subscribed to on the orbit-db client (confusing enough)
        // there, the write is binded to the pubsub publish, with the entry. Which will send this entry
        // to all the connected peers to tell them that a new entry has been added
        // TODO: don't use events, or make it more transparent that there is a vital subscription in the background
        // that is handling replication
        this._options.onWrite && this._options.onWrite(this, entry);
        return entry;
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
