import { EntryIndex } from "./entry-index.js";
import { LogIO } from "./log-io.js";
import * as LogError from "./log-errors.js";
import * as Sorting from "./log-sorting.js";
import {
    EntryFetchAllOptions,
    EntryFetchOptions,
    strictFetchOptions,
} from "./entry-io.js";
import { isDefined } from "./is-defined.js";
import { findUniques } from "./find-uniques.js";
import {
    EncryptionTemplateMaybeEncrypted,
    Entry,
    Payload,
    CanAppend,
} from "./entry.js";
import {
    HLC,
    LamportClock as Clock,
    LamportClock,
    Timestamp,
} from "./clock.js";
import {
    PublicKeyEncryptionResolver,
    SignatureWithKey,
} from "@dao-xyz/peerbit-crypto";
import { serialize } from "@dao-xyz/borsh";

import { Encoding, JSON_ENCODING } from "./encoding.js";
import { Identity } from "./identity.js";
import { logger as parentLogger } from "./logger.js";
import { HeadsIndex } from "./heads.js";
import { Blocks } from "@dao-xyz/peerbit-block";
import { Values } from "./values.js";

const logger = parentLogger.child({ module: "log" });

const { LastWriteWins, NoZeroes } = Sorting;
const randomId = () => new Date().getTime().toString();
const getHash = <T>(e: Entry<T>) => e.hash;
/* const maxSizeReducer = <T>(res: bigint, acc: Entry<T>): bigint => bigIntMax(res, acc.cumulativeSize); */

const uniqueEntriesReducer = <T>(res: Map<string, Entry<T>>, acc: Entry<T>) => {
    res.set(acc.hash, acc);
    return res;
};

export type TrimToLengthOption = { to: number; from?: number };
export type TrimToByteLengthOption = { bytelength: number };
export type TrimOptions =
    | (TrimToByteLengthOption | TrimToLengthOption)[]
    | TrimToByteLengthOption
    | TrimToLengthOption;
export type Change<T> = { added: Entry<T>[]; removed?: Entry<T>[] };

export type LogOptions<T> = {
    encryption?: PublicKeyEncryptionResolver;
    encoding?: Encoding<T>;
    logId?: string;
    entries?: Entry<T>[];
    heads?: any;
    clock?: LamportClock;
    sortFn?: Sorting.ISortFunction;
    concurrency?: number;
    trim?: TrimOptions;
};

/**
 * @description
 * Log implements a G-Set CRDT and adds ordering.
 *
 * From:
 * "A comprehensive study of Convergent and Commutative Replicated Data Types"
 * https://hal.inria.fr/inria-00555588
 */

export class Log<T> {
    _sortFn: Sorting.ISortFunction;
    _storage: Blocks;
    _id: string;
    /*   _rootGid: string; */

    _hlc: HLC;

    // Identity
    _identity: Identity;

    // Keeping track of entries
    _entryIndex: EntryIndex<T>;
    _headsIndex: HeadsIndex<T>;
    _values: Values<T>;
    // Index of all next pointers in this log
    _nextsIndex: Map<string, Set<string>>;

    _trim: TrimOptions = [];
    _encryption?: PublicKeyEncryptionResolver;
    _encoding: Encoding<T>;

    joinConcurrency: number;

    constructor(
        store: Blocks,
        identity: Identity,
        options: LogOptions<T> = {}
    ) {
        if (!isDefined(store)) {
            throw LogError.BlockStoreNotDefinedError();
        }

        if (!isDefined(identity)) {
            throw new Error("Identity is required");
        }
        //
        const { logId, encoding, concurrency, trim, encryption } = options;
        let { sortFn, entries, heads } = options;

        if (isDefined(entries) && !Array.isArray(entries)) {
            throw new Error(
                "'entries' argument must be an array of Entry instances"
            );
        }

        if (isDefined(heads) && !Array.isArray(heads)) {
            throw new Error("'heads' argument must be an array");
        }

        if (!isDefined(sortFn)) {
            sortFn = LastWriteWins;
        }
        sortFn = sortFn as Sorting.ISortFunction;

        this._sortFn = NoZeroes(sortFn);

        this._storage = store;
        this._id = logId || randomId();
        /*     this._rootGid = rootGid;
         */

        // Identity
        this._identity = identity;

        // encoder/decoder
        this._encryption = encryption;
        this._encoding = encoding || JSON_ENCODING;

        // Add entries to the internal cache
        const uniqueEntries = (entries || []).reduce(
            uniqueEntriesReducer,
            new Map()
        );
        this._entryIndex = new EntryIndex(uniqueEntries);
        entries = [...uniqueEntries.values()];

        // Init io for entries (as these are not created with the append method)
        entries.map((e) => {
            e.init({ encryption: this._encryption, encoding: this._encoding });
        });

        // Set heads if not passed as an argument
        heads = heads || Log.findHeads(entries);
        this._headsIndex = new HeadsIndex({
            sortFn: this._sortFn,
            entries: heads.reduce(uniqueEntriesReducer, new Map()),
        });
        this._values = new Values(this._sortFn, entries);

        // Index of all next pointers in this log
        this._nextsIndex = new Map();

        // Clock
        this._hlc = new HLC();

        const addToNextsIndex = (e: Entry<T>) => {
            e.next.forEach((a) => {
                let nextIndexSet = this._nextsIndex.get(a);
                if (!nextIndexSet) {
                    nextIndexSet = new Set();
                    nextIndexSet.add(e.hash);
                    this._nextsIndex.set(a, nextIndexSet);
                } else {
                    nextIndexSet.add(e.hash);
                }
            });
        };

        entries.forEach(addToNextsIndex);

        // Set the length, we calculate the length manually internally

        // Set the clock
        this.joinConcurrency = concurrency || 16;
        this._trim = trim ? (Array.isArray(trim) ? trim : [trim]) : [];
    }

    /**
     * Returns the length of the log.
     */
    get length() {
        return this._entryIndex.length;
    }

    /**
     * Get all entries sorted
     */
    get values(): Entry<T>[] {
        return this._values.toArray();
    }

    /**
     * Returns the head index
     */
    get headsIndex(): HeadsIndex<T> {
        return this._headsIndex;
    }

    /**
     * Don't use this anywhere performance matters
     */
    get heads(): Entry<T>[] {
        return this.headsIndex.array;
    }

    /**
     * Returns an array of Entry objects that reference entries which
     * are not in the log currently.
     * @returns {Array<Entry<T>>}
     */
    get tails() {
        return Log.findTails(this.values);
    }

    /**
     * Returns an array of hashes that are referenced by entries which
     * are not in the log currently.
     * @returns {Array<string>} Array of hashes
     */
    get tailHashes() {
        return Log.findTailHashes(this.values);
    }

    /**
     * Get local HLC
     */
    get hlc(): HLC {
        return this._hlc;
    }

    /**
     * Set the identity for the log
     * @param {Identity} [identity] The identity to be set
     */
    setIdentity(identity: Identity) {
        this._identity = identity;
    }

    /**
     * Find an entry.
     * @param {string} [hash] The hashes of the entry
     * @returns {Entry|undefined}
     */
    get(hash: string) {
        return this._entryIndex.get(hash);
    }

    /**
     * Checks if a entry is part of the log
     * @param {string} hash The hash of the entry
     * @returns {boolean}
     */
    has(entry: Entry<string> | string) {
        if (entry instanceof Entry && !entry.hash) {
            throw new Error("Expected entry hash to be defined");
        }
        return (
            this._entryIndex.get(
                entry instanceof Entry ? entry.hash : entry
            ) !== undefined
        );
    }

    traverse(
        rootEntries: Entry<T>[],
        amount = -1,
        endHash?: string
    ): { [key: string]: Entry<T> } {
        // Sort the given given root entries and use as the starting stack
        let stack: Entry<T>[] = rootEntries.sort(this._sortFn).reverse();

        // Cache for checking if we've processed an entry already
        let traversed: { [key: string]: boolean } = {};
        // End result
        const result: { [key: string]: Entry<T> } = {};
        let count = 0;
        // Named function for getting an entry from the log
        const getEntry = (e: string) => this.get(e);

        // Add an entry to the stack and traversed nodes index
        const addToStack = (entry: Entry<T>) => {
            // If we've already processed the Entry<T>, don't add it to the stack
            if (!entry || traversed[entry.hash]) {
                return;
            }

            // Add the entry in front of the stack and sort
            stack = [entry, ...stack].sort(this._sortFn).reverse();
            // Add to the cache of processed entries
            traversed[entry.hash] = true;
        };

        const addEntry = (rootEntry: Entry<T>) => {
            result[rootEntry.hash] = rootEntry;
            traversed[rootEntry.hash] = true;
            count++;
        };

        // Start traversal
        // Process stack until it's empty (traversed the full log)
        // or when we have the requested amount of entries
        // If requested entry amount is -1, traverse all
        while (stack.length > 0 && (count < amount || amount < 0)) {
            // eslint-disable-line no-unmodified-loop-condition
            // Get the next element from the stack
            const entry = stack.shift();
            if (!entry) {
                throw new Error("Unexpected");
            }
            // Add to the result
            addEntry(entry);
            // If it is the specified end hash, break out of the while loop
            if (endHash && endHash === entry.hash) break;

            // Add entry's next references to the stack
            const entries = entry.next
                .map(getEntry)
                .filter((x) => !!x) as Entry<any>[];
            entries.forEach(addToStack);
        }

        stack = [];
        traversed = {};
        // End result
        return result;
    }

    getPow2Refs(pointerCount = 1): Entry<T>[] {
        // If pointer count is 4, returns 2
        // If pointer count is 8, returns 3 references
        // If pointer count is 512, returns 9 references
        // If pointer count is 2048, returns 11 references
        const getEveryPow2 = (maxDistance: number) => {
            const entries = new Set<Entry<T>>();

            if (maxDistance === 0) {
                return entries;
            }

            let next = this._values.head;
            if (next) {
                entries.add(next.value);
            }

            let prev = 1;
            outer: for (let i = 2; i <= maxDistance - 1; i *= 2) {
                for (let j = prev; j < i; j++) {
                    if (!next) {
                        break outer;
                    }
                    next = next?.next;
                }
                prev = i;
                if (next) {
                    entries.add(next?.value);
                }
            }
            return entries;
        };
        const references = getEveryPow2(
            Math.min(pointerCount, this._values.length)
        );

        // Always include the last known reference
        /* if (all.length < pointerCount && all[all.length - 1]) {
            references.add(all[all.length - 1]); // TODO can this yield a duplicate?
        } */
        return [...references];
    }

    /* getHeadsFromHashes(refs: string[]): Entry<T>[] {
        const headsFromRefs = new Map<string, Entry<T>>();
        refs.forEach((ref) => {
            const headsFromRef = this.getHeads(ref); // TODO allow forks
            headsFromRef.forEach((head) => {
                headsFromRefs.set(head.hash, head);
            });
        });
        const nexts = [...headsFromRefs.values()].sort(this._sortFn);
        return nexts;
    } */

    /**
     * Append an entry to the log.
     * @param {Entry} entry Entry to add
     * @return {Log} New Log containing the appended value
     */
    async append(
        data: T,
        options: {
            canAppend?: CanAppend<T>;
            gidSeed?: string;
            nexts?: Entry<any>[];
            pin?: boolean;
            identity?: Identity;
            signers?: ((data: Uint8Array) => Promise<SignatureWithKey>)[];
            reciever?: EncryptionTemplateMaybeEncrypted;
            onGidsShadowed?: (gids: string[]) => void;
            trim?: TrimOptions;
            timestamp?: Timestamp;
        } = { pin: false }
    ): Promise<{ entry: Entry<T>; removed: Entry<T>[] }> {
        if (options.reciever && !this._encryption) {
            throw new Error(
                "Message is intended to be encrypted but no encryption methods are provided for the log"
            );
        }

        // Update the clock (find the latest clock)
        if (options.nexts) {
            options.nexts.forEach((n) => {
                if (!n.hash)
                    throw new Error(
                        "Expecting nexts to already be saved. missing hash for one or more entries"
                    );
            });
        }

        const hasNext = !!options.nexts;
        const nexts: Entry<any>[] = options.nexts || [
            ...this.headsIndex._index.values(),
        ];

        // Calculate max time for log/graph
        const clock = new Clock({
            id: new Uint8Array(serialize(this._identity.publicKey)),
            timestamp: options.timestamp || this._hlc.now(),
        });

        const identity = options.identity || this._identity;

        const entry = await Entry.create<T>({
            store: this._storage,
            identity: identity,
            signers: options.signers,
            data,
            clock,
            encoding: this._encoding,
            next: nexts,
            gidSeed: options.gidSeed,
            pin: options.pin,
            encryption: options.reciever
                ? {
                      options: this._encryption as PublicKeyEncryptionResolver,
                      reciever: {
                          ...options.reciever,
                      },
                  }
                : undefined,
            canAppend: options.canAppend,
        });

        if (!isDefined(entry.hash)) {
            throw new Error("Unexpected");
        }

        nexts.forEach((e) => {
            let nextIndexSet = this._nextsIndex.get(e.hash);
            if (!nextIndexSet) {
                nextIndexSet = new Set();
                nextIndexSet.add(entry.hash);
                this._nextsIndex.set(e.hash, nextIndexSet);
            } else {
                nextIndexSet.add(entry.hash);
            }
        });

        const removedGids: Set<string> = new Set();
        if (hasNext) {
            nexts.forEach((next) => {
                const deletion = this._headsIndex.del(next);
                if (deletion.lastWithGid && next.gid !== entry.gid) {
                    removedGids.add(next.gid);
                }
            });
        } else {
            // next is all heads, which means we should just overwrite
            for (const key of this.headsIndex.gids.keys()) {
                if (key !== entry.gid) {
                    removedGids.add(key);
                }
            }
            this.headsIndex.reset([entry]);
        }

        this._entryIndex.set(entry.hash, entry);
        this._headsIndex.put(entry);
        this._values.put(entry);

        // if next contails all gids
        if (options.onGidsShadowed && removedGids.size > 0) {
            options.onGidsShadowed([...removedGids]);
        }

        entry.init({ encoding: this._encoding, encryption: this._encryption });

        const removed = await this.trim(options?.trim);
        return { entry, removed };
    }

    iterator(options?: {
        from?: "tail" | "head";
        amount?: number;
    }): IterableIterator<Entry<T>> {
        const from = options?.from || "tail";
        const amount =
            typeof options?.amount === "number" ? options?.amount : -1;
        let next = from === "tail" ? this._values.tail : this._values.head;
        const nextFn = from === "tail" ? (e) => e.prev : (e) => e.next;
        return (function* () {
            let counter = 0;
            while (next) {
                if (amount >= 0 && counter >= amount) {
                    return;
                }
                yield next.value;
                counter++;

                next = nextFn(next);
            }
        })();
    }

    /**
     * Join two logs.
     *
     * Joins another log into this one.
     *
     * @param {Log} log Log to join with this Log
     * @param {number} [size=-1] Max size of the joined log
     * @returns {Promise<Log>} This Log instance
     * @example
     * await log1.join(log2)
     */
    async join(
        log: Log<T>,
        options?: { verifySignatures?: boolean; trim?: TrimOptions }
    ): Promise<Change<T>> {
        // Get the difference of the logs
        const newItems = await Log.difference(log, this);
        const nextFromNew = new Set<string>();
        for (const e of newItems.values()) {
            if (options?.verifySignatures) {
                if (!(await e.verifySignatures())) {
                    throw new Error(
                        'Invalid signature entry with hash "' + e.hash + '"'
                    );
                }
            }
            if (!isDefined(e.hash)) {
                throw new Error("Unexpected");
            }
            const entry = this.get(e.hash);
            if (!entry) {
                // Update the internal entry index
                this._entryIndex.set(e.hash, e);
                this._values.put(e);
            }
            e.next.forEach((a) => {
                let nextIndexSet = this._nextsIndex[a];
                if (!nextIndexSet) {
                    nextIndexSet = new Set();
                    nextIndexSet.add(a);
                    this._nextsIndex[a] = nextIndexSet;
                } else {
                    nextIndexSet.add(a);
                }
                nextFromNew.add(a);
            });

            const clock = await e.getClock();
            this._hlc.update(clock.timestamp);
        }

        // Merge the heads
        const notReferencedByNewItems = (e: Entry<any>) =>
            !nextFromNew.has(e.hash);

        const notInCurrentNexts = (e: Entry<any>) =>
            !this._nextsIndex.has(e.hash);
        newItems.forEach((v, k) => {
            if (notInCurrentNexts(v) && notReferencedByNewItems(v)) {
                this.headsIndex.put(v);
            }
        });

        nextFromNew.forEach((next) => {
            this.headsIndex.del(this.get(next)!);
        });

        const removed = await this.trim(options?.trim);

        return {
            added: [...newItems.values()],
            removed,
        };
    }

    /**
     * @param options
     * @returns deleted entries
     */
    async trim(option: TrimOptions = this._trim): Promise<Entry<T>[]> {
        if (!option) {
            throw new Error("Prune options missing");
        }

        if (Array.isArray(option)) {
            const deleted: Entry<T>[] = [];
            for (const o of option) {
                deleted.push(...(await this.trim(o)));
            }
            return deleted;
        }

        const deleted: Entry<any>[] = [];
        // Slice to the requested size
        const promises: Promise<void>[] = [];
        if (typeof (option as TrimToLengthOption).to === "number") {
            const to = (option as TrimToLengthOption).to;
            const from = (option as TrimToLengthOption).from || to;
            if (this.length <= from) {
                return deleted;
            }

            // prune to length
            const len = this.length;
            for (let i = 0; i < len - to; i++) {
                const entry = this._values.pop();
                if (!entry) {
                    break;
                }
                deleted.push(entry);
                this._entryIndex.delete(entry.hash);
                this._headsIndex.del(entry);
                this._nextsIndex.delete(entry.hash);
                promises.push(this._storage.rm(entry.hash));
            }
        } else if (
            typeof (option as TrimToByteLengthOption).bytelength === "number"
        ) {
            // prune to max sum payload sizes in bytes
            const byteLength = (option as TrimToByteLengthOption).bytelength;
            while (this._values.byteLength > byteLength && this.length > 0) {
                const entry = this._values.pop();
                if (!entry) {
                    break;
                }
                deleted.push(entry);
                this._entryIndex.delete(entry.hash);
                this._headsIndex.del(entry);
                this._nextsIndex.delete(entry.hash);
                promises.push(this._storage.rm(entry.hash));
            }
        }
        await Promise.all(promises);
        return deleted;
    }

    async deleteRecursively(from: Entry<any> | Entry<any>[]) {
        const stack = Array.isArray(from) ? from : [from];
        const promises: Promise<void>[] = [];
        while (stack.length > 0) {
            const entry = stack.pop()!;

            this._values.delete(entry);
            this._entryIndex.delete(entry.hash);
            this._headsIndex.del(entry);
            this._nextsIndex.delete(entry.hash);
            for (const next of entry.next) {
                const ne = this.get(next);
                if (ne) {
                    stack.push(ne);
                }
            }
            promises.push(entry.delete(this._storage));
        }

        await Promise.all(promises);
    }

    async delete(entry: Entry<any>) {
        this._values.delete(entry);
        this._entryIndex.delete(entry.hash);
        this._headsIndex.del(entry);
        this._nextsIndex.delete(entry.hash);
        for (const next of entry.next) {
            const ne = this.get(next);
            if (ne) {
                const nexts = this._nextsIndex.get(next)!;
                nexts.delete(entry.hash);
                if (nexts.size === 0) {
                    this._headsIndex.put(ne);
                }
            }
        }
        return entry.delete(this._storage);
    }

    /**
     * Get the log in JSON format.
     * @returns {Object} An object with the id and heads properties
     */
    toJSON() {
        return {
            id: this._id,
            heads: [...this.headsIndex.index.values()]
                .sort(this._sortFn) // default sorting
                .reverse() // we want the latest as the first element
                .map(getHash), // return only the head hashes
        };
    }

    /**
     * Get the log in JSON format as a snapshot.
     * @returns {Object} An object with the id, heads and value properties
     */
    toSnapshot() {
        return {
            id: this._id,
            heads: [...this.headsIndex.index.values()],
            values: this.values,
        };
    }

    /**
     * Returns the log entries as a formatted string.
     * @returns {string}
     * @example
     * two
     * └─one
     *   └─three
     */
    toString(
        payloadMapper: (payload: Payload<T>) => string = (payload) =>
            (payload.getValue() as any).toString()
    ) {
        return this.values
            .slice()
            .reverse()
            .map((e, idx) => {
                const parents: Entry<any>[] = Entry.findDirectChildren(
                    e,
                    this.values
                );
                const len = parents.length;
                let padding = new Array(Math.max(len - 1, 0));
                padding = len > 1 ? padding.fill("  ") : padding;
                padding = len > 0 ? padding.concat(["└─"]) : padding;
                /* istanbul ignore next */
                return (
                    padding.join("") +
                    (payloadMapper ? payloadMapper(e.payload) : e.payload)
                );
            })
            .join("\n");
    }

    /**
     * Get the log's multihash.
     * @returns {Promise<string>} Multihash of the Log as Base58 encoded string.
     */
    toMultihash(options?: { format?: string }) {
        return LogIO.toMultihash(this._storage, this, options);
    }

    static async fromMultihash<T>(
        store: Blocks,
        identity: Identity,
        hash: string,
        options: { sortFn?: Sorting.ISortFunction } & EntryFetchAllOptions<T>
    ) {
        // TODO: need to verify the entries with 'key'
        const { logId, entries, heads } = await LogIO.fromMultihash(
            store,
            hash,
            {
                length: options?.length,
                shouldFetch: options?.shouldFetch,
                shouldQueue: options?.shouldQueue,
                timeout: options?.timeout,
                onFetched: options?.onFetched,
                concurrency: options?.concurrency,
                sortFn: options?.sortFn,
            }
        );
        return new Log<T>(store, identity, {
            encryption: options?.encryption,
            encoding: options?.encoding,
            logId,
            entries,
            heads,
            sortFn: options?.sortFn,
        });
    }

    static async fromEntryHash<T>(
        store: Blocks,
        identity: Identity,
        hash: string | string[],
        options: {
            encoding?: Encoding<T>;
            encryption?: PublicKeyEncryptionResolver;
            logId?: string;
            length?: number;
            exclude?: any[];
            shouldFetch?: (hash: string) => boolean;
            timeout?: number;
            concurrency?: number;
            sortFn?: any;
            onFetched?: any;
        } = { length: -1, exclude: [] }
    ): Promise<Log<T>> {
        // TODO: need to verify the entries with 'key'
        const { entries } = await LogIO.fromEntryHash(store, hash, {
            length: options.length,
            encryption: options?.encryption,
            encoding: options.encoding,
            shouldFetch: options.shouldFetch,
            timeout: options.timeout,
            concurrency: options.concurrency,
            onFetched: options.onFetched,
            sortFn: options.sortFn,
        });
        return new Log<T>(store, identity, {
            encryption: options?.encryption,
            encoding: options?.encoding,
            logId: options.logId,
            entries,
            sortFn: options.sortFn,
        });
    }

    /**
     * Create a log from a Log Snapshot JSON.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Identity} identity The identity instance
     * @param {Object} json Log snapshot as JSON object
     * @param {Object} options
     * @param {AccessController} options.access The access controller instance
     * @param {number} options.length How many entries to include in the log
     * @param {function(hash, entry,  parent, depth)} [options.onFetched]
     * @param {Function} options.sortFn The sort function - by default LastWriteWins
     * @return {Promise<Log>} New Log
     */
    static async fromJSON<T>(
        store: Blocks,
        identity: Identity,
        json: { id: string; heads: string[] },
        options: {
            encoding?: Encoding<T>;
            encryption?: PublicKeyEncryptionResolver;
            length?: number;
            timeout?: number;
            sortFn?: Sorting.ISortFunction;
            onFetched?: (entry: Entry<T>) => void;
        } = { encoding: JSON_ENCODING }
    ) {
        // TODO: need to verify the entries with 'key'
        const { logId, entries } = await LogIO.fromJSON(store, json, {
            length: options?.length,
            encryption: options?.encryption,
            encoding: options.encoding,
            timeout: options?.timeout,
            onFetched: options?.onFetched,
        });
        return new Log<T>(store, identity, {
            encryption: options?.encryption,
            encoding: options?.encoding,
            logId,
            entries,
            sortFn: options?.sortFn,
        });
    }

    static async fromEntry<T>(
        store: Blocks,
        identity: Identity,
        sourceEntries: Entry<T>[] | Entry<T>,
        options: EntryFetchOptions<T> & {
            shouldFetch?: (hash: string) => boolean;
            encryption?: PublicKeyEncryptionResolver;
            sortFn?: Sorting.ISortFunction;
        }
    ) {
        // TODO: need to verify the entries with 'key'
        options = strictFetchOptions(options);
        const { entries } = await LogIO.fromEntry(store, sourceEntries, {
            length: options.length,
            encryption: options?.encryption,
            encoding: options.encoding,
            timeout: options.timeout,
            concurrency: options.concurrency,
            shouldFetch: options.shouldFetch,
            onFetched: options.onFetched,
        });
        return new Log<T>(store, identity, {
            encryption: options?.encryption,
            encoding: options?.encoding,
            entries,
            sortFn: options.sortFn,
        });
    }

    /**
     * Find heads from a collection of entries.
     *
     * Finds entries that are the heads of this collection,
     * ie. entries that are not referenced by other entries.
     *
     * @param {Array<Entry<T>>} entries Entries to search heads from
     * @returns {Array<Entry<T>>}
     */
    static findHeads<T>(entriesOrLogs: (Entry<T> | Log<T>)[]) {
        const entries: Entry<T>[] = [];
        entriesOrLogs.forEach((entryOrLog) => {
            if (entryOrLog instanceof Entry) {
                entries.push(entryOrLog);
            } else {
                for (const head of entryOrLog.headsIndex.index.values()) {
                    entries.push(head);
                }
            }
        });

        const indexReducer = (
            res: { [key: string]: string },
            entry: Entry<any>,
            idx: number
        ) => {
            const addToResult = (e: string) => (res[e] = entry.hash);
            entry.next.forEach(addToResult);
            return res;
        };

        const items = entries.reduce(indexReducer, {});
        const exists = (e: Entry<T>) => items[e.hash] === undefined;
        const compareIds = (a: Entry<T>, b: Entry<T>) =>
            Clock.compare(a.metadata.clock, b.metadata.clock);

        return entries.filter(exists).sort(compareIds);
    }

    // Find entries that point to another entry that is not in the
    // input array
    static findTails<T>(entries: Entry<T>[]): Entry<T>[] {
        // Reverse index { next -> entry }
        const reverseIndex: { [key: string]: Entry<T>[] } = {};
        // Null index containing entries that have no parents (nexts)
        const nullIndex: Entry<T>[] = [];
        // Hashes for all entries for quick lookups
        const hashes: { [key: string]: boolean } = {};
        // Hashes of all next entries
        let nexts: string[] = [];

        const addToIndex = (e: Entry<T>) => {
            if (e.next.length === 0) {
                nullIndex.push(e);
            }
            const addToReverseIndex = (a: any) => {
                /* istanbul ignore else */
                if (!reverseIndex[a]) reverseIndex[a] = [];
                reverseIndex[a].push(e);
            };

            // Add all entries and their parents to the reverse index
            e.next.forEach(addToReverseIndex);
            // Get all next references
            nexts = nexts.concat(e.next);
            // Get the hashes of input entries
            hashes[e.hash] = true;
        };

        // Create our indices
        entries.forEach(addToIndex);

        const addUniques = (
            res: Entry<T>[],
            entries: Entry<T>[],
            _idx: any,
            _arr: any
        ) => res.concat(findUniques(entries, "hash"));
        const exists = (e: string) => hashes[e] === undefined;
        const findFromReverseIndex = (e: string) => reverseIndex[e];

        // Drop hashes that are not in the input entries
        const tails = nexts // For every hash in nexts:
            .filter(exists) // Remove undefineds and nulls
            .map(findFromReverseIndex) // Get the Entry from the reverse index
            .reduce(addUniques, []) // Flatten the result and take only uniques
            .concat(nullIndex); // Combine with tails the have no next refs (ie. first-in-their-chain)

        return findUniques(tails, "hash").sort(Entry.compare);
    }

    // Find the hashes to entries that are not in a collection
    // but referenced by other entries
    static findTailHashes(entries: Entry<any>[]) {
        const hashes: { [key: string]: boolean } = {};
        const addToIndex = (e: Entry<any>) => (hashes[e.hash] = true);
        const reduceTailHashes = (
            res: string[],
            entry: Entry<any>,
            idx: number,
            arr: Entry<any>[]
        ) => {
            const addToResult = (e: string) => {
                /* istanbul ignore else */
                if (hashes[e] === undefined) {
                    res.splice(0, 0, e);
                }
            };
            entry.next.reverse().forEach(addToResult);
            return res;
        };

        entries.forEach(addToIndex);
        return entries.reduce(reduceTailHashes, []);
    }

    static async difference<T>(
        from: Log<T>,
        into: Log<T>
    ): Promise<Map<string, Entry<T>>> {
        const stack: string[] = [...from._headsIndex._index.keys()];
        const traversed: { [key: string]: boolean } = {};
        const res: Map<string, Entry<T>> = new Map();

        const pushToStack = (hash: string) => {
            if (!traversed[hash] && !into.get(hash)) {
                stack.push(hash);
                traversed[hash] = true;
            }
        };

        while (stack.length > 0) {
            const hash = stack.shift();
            if (!hash) {
                throw new Error("Unexpected");
            }
            const entry = from.get(hash);
            if (entry && !into.get(hash)) {
                // TODO do we need to do som GID checks?
                res.set(entry.hash, entry);
                traversed[entry.hash] = true;

                // TODO init below is kind of flaky to do this here, but we dont want to iterate over all entries before the difference method is invoked in the join log method
                entry.init({
                    encryption: into._encryption,
                    encoding: into._encoding,
                });
                (await entry.getNext()).forEach(pushToStack);
            }
        }
        return res;
    }
}
