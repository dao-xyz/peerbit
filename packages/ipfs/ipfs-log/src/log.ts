import { EntryIndex } from "./entry-index.js";
import { GSet } from "./g-set.js";
import { LogIO } from "./log-io.js";
import * as LogError from "./log-errors.js";
import * as Sorting from "./log-sorting.js";
import {
    EntryFetchAllOptions,
    EntryFetchOptions,
    strictFetchOptions,
} from "./entry-io.js";
import { IPFS } from "ipfs-core-types";
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

const logger = parentLogger.child({ module: "ipfs-log" });

const { LastWriteWins, NoZeroes } = Sorting;
const randomId = () => new Date().getTime().toString();
const getHash = <T>(e: Entry<T>) => e.hash;
const flatMap = (res: any[], acc: any[]) => res.concat(acc);
const getNextPointers = (entry: Entry<any>) => entry.next;
/* const maxSizeReducer = <T>(res: bigint, acc: Entry<T>): bigint => bigIntMax(res, acc.cumulativeSize); */

const uniqueEntriesReducer = <T>(
    res: { [key: string]: Entry<T> },
    acc: Entry<T>
) => {
    res[acc.hash] = acc;
    return res;
};

export interface PruneOptions {
    maxLength: number; // Max length of oplog before cutting
    cutToLength: number; // When oplog shorter, cut to length
}

export type LogOptions<T> = {
    encryption?: PublicKeyEncryptionResolver;
    encoding?: Encoding<T>;
    logId?: string;
    entries?: Entry<T>[];
    heads?: any;
    clock?: LamportClock;
    sortFn?: Sorting.ISortFunction;
    concurrency?: number;
    prune?: PruneOptions;
};
/**
 * @description
 * Log implements a G-Set CRDT and adds ordering.
 *
 * From:
 * "A comprehensive study of Convergent and Commutative Replicated Data Types"
 * https://hal.inria.fr/inria-00555588
 */

export class Log<T> extends GSet {
    _sortFn: Sorting.ISortFunction;
    _storage: IPFS;
    _id: string;
    /*   _rootGid: string; */

    // Identity
    _identity: Identity;

    // Add entries to the internal cache
    _entryIndex: EntryIndex<T>;
    _headsIndex: HeadsIndex<T>;

    // Index of all next pointers in this log
    _nextsIndex: { [key: string]: Set<string> };

    // next -> entry
    _nextsIndexToHead: { [key: string]: Set<string> }; // TODO make to LRU since this will become invalid quickly (and potentially huge)

    // Set the length, we calculate the length manually internally
    _length: number; // Total amount of elements in the log
    /*  _clock: Clock; */
    _prune?: PruneOptions;
    _encryption?: PublicKeyEncryptionResolver;
    _encoding: Encoding<T>;
    _hlc: HLC;

    joinConcurrency: number;

    constructor(ipfs: IPFS, identity: Identity, options: LogOptions<T> = {}) {
        if (!isDefined(ipfs)) {
            throw LogError.IPFSNotDefinedError();
        }

        if (!isDefined(identity)) {
            throw new Error("Identity is required");
        }
        //
        const { logId, encoding, concurrency, prune, encryption } = options;
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

        super();

        this._sortFn = NoZeroes(sortFn);

        this._storage = ipfs;
        this._id = logId || randomId();
        /*     this._rootGid = rootGid;
         */

        // Identity
        this._identity = identity;

        // encoder/decoder
        this._encryption = encryption;
        this._encoding = encoding || JSON_ENCODING;

        // Add entries to the internal cache
        const uniqueEntries = (entries || []).reduce(uniqueEntriesReducer, {});
        this._entryIndex = new EntryIndex(uniqueEntries);
        entries = Object.values(uniqueEntries) || [];

        // Init io for entries (as these are not created with the append method)
        entries.map((e) => {
            e.init({ encryption: this._encryption, encoding: this._encoding });
        });

        // Set heads if not passed as an argument
        heads = heads || Log.findHeads(entries);
        this._headsIndex = new HeadsIndex({
            sortFn: this._sortFn,
            entries: heads.reduce(uniqueEntriesReducer, {}),
        });

        // Index of all next pointers in this log
        this._nextsIndex = {};
        this._nextsIndexToHead = {};

        // Clock
        this._hlc = new HLC();

        const addToNextsIndex = (e: Entry<T>) => {
            e.next.forEach((a) => {
                let nextIndexSet = this._nextsIndex[a];
                if (!nextIndexSet) {
                    nextIndexSet = new Set();
                    this._nextsIndex[a] = nextIndexSet;
                }
                this._nextsIndex[a].add(e.hash);
            });
        };

        entries.forEach(addToNextsIndex);

        // Set the length, we calculate the length manually internally
        this._length = entries.length;

        // Set the clock
        /*  const maxTime = bigIntMax(clock ? clock.time : 0n, this.heads.reduce(maxClockTimeReducer, 0n)) */
        // Take the given key as the clock id is it's a Key instance,
        // otherwise if key was given, take whatever it is,
        // and if it was null, take the given id as the clock id
        /*     this._clock = new Clock(new Uint8Array(serialize(publicKey)), maxTime) */

        this.joinConcurrency = concurrency || 16;

        this._prune = prune;
    }

    /**
     * Returns the length of the log.
     * @return {number} Length
     */
    get length() {
        return this._length;
    }

    /**
     * Returns the values in the log.
     * @returns {Array<Entry<T>>}
     */
    get values(): Entry<T>[] {
        return Object.values(this.traverse(this.heads)).reverse();
    }

    /**
     * Returns an array of heads.
     * @returns {Array<Entry<T>>}
     */
    get heads(): Entry<T>[] {
        return this._headsIndex.heads;
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

    getPow2Refs(pointerCount = 1, heads?: Entry<T>[]): Entry<T>[] {
        const headsToUse = heads || this.heads;
        const all = Object.values(
            this.traverse(headsToUse, Math.max(pointerCount, headsToUse.length))
        );

        // If pointer count is 4, returns 2
        // If pointer count is 8, returns 3 references
        // If pointer count is 512, returns 9 references
        // If pointer count is 2048, returns 11 references
        const getEveryPow2 = (maxDistance: number) => {
            const entries = new Set<Entry<T>>();
            for (let i = 1; i <= maxDistance; i *= 2) {
                const index = Math.min(i - 1, all.length - 1);
                entries.add(all[index]);
            }
            return entries;
        };
        const references = getEveryPow2(Math.min(pointerCount, all.length));

        // Always include the last known reference
        if (all.length < pointerCount && all[all.length - 1]) {
            references.add(all[all.length - 1]); // TODO can this yield a publicate?
        }
        return [...references];
    }

    getHeadsFromHashes(refs: string[]): Entry<T>[] {
        const headsFromRefs = new Map<string, Entry<T>>();
        refs.forEach((ref) => {
            const headsFromRef = this.getHeads(ref); // TODO allow forks
            headsFromRef.forEach((head) => {
                headsFromRefs.set(head.hash, head);
            });
        });
        const nexts = [...headsFromRefs.values()].sort(this._sortFn);
        return nexts;
    }

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
            timestamp?: Timestamp;
        } = { pin: false }
    ) {
        if (options.reciever && !this._encryption) {
            throw new Error(
                "Message is intended to be encrypted but no encryption methods are provided for the log"
            );
        }

        // nextsreolver
        // 1. all heads
        // 2. all heads that are references
        // 3. next = refs if length of refs = 1

        // Update the clock (find the latest clock)
        if (options.nexts) {
            options.nexts.forEach((n) => {
                if (!n.hash)
                    throw new Error(
                        "Expecting nexts to already be saved. missing hash for one or more entries"
                    );
            });
        }

        const currentHeads: Entry<T>[] = Object.values(
            this.heads.reverse().reduce(uniqueEntriesReducer, {})
        ); // TODO this invokes a double reverse

        const nexts: Entry<any>[] = options.nexts || currentHeads;

        // Some heads might not even be referenced by the refs, this will be merged into the headsIndex so we dont forget them
        const keepHeads: Entry<T>[] = options.nexts
            ? currentHeads.filter((h) => !nexts.find((e) => e.hash === h.hash))
            : []; // TODO improve performance

        // Calculate max time for log/graph
        /*  const newTime = ; */
        /*    nexts?.length > 0
               ? this.heads.concat(nexts).reduce(maxClockTimeReducer, 0n) + 1n
               : 0n; */
        const clock = new Clock({
            id: new Uint8Array(serialize(this._identity.publicKey)),
            timestamp: options.timestamp || this._hlc.now(),
        }); // TODO privacy leak?

        const identity = options.identity || this._identity;
        let gidsInHeds =
            options.onGidsShadowed && new Set(this.heads.map((h) => h.gid)); // could potentially be faster if we first groupBy

        const entry = await Entry.create<T>({
            ipfs: this._storage,
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

        this._entryIndex.set(entry.hash, entry);
        nexts.forEach((e) => {
            let nextIndexSet = this._nextsIndex[e.hash];
            if (!nextIndexSet) {
                nextIndexSet = new Set();
                this._nextsIndex[e.hash] = nextIndexSet;
            }
            this._nextsIndex[e.hash].add(entry.hash);
        });

        keepHeads.push(entry);
        this._headsIndex.reset(keepHeads);

        // Update the length
        this._length++;

        if (this._prune && this.length > this._prune.maxLength) {
            this.prune(this._prune.cutToLength);
        }

        // if next contails all gids
        if (options.onGidsShadowed) {
            const gidsInHeadsAfterEntry = new Set(this.heads.map((h) => h.gid)); // could potentially be faster if we first groupBy
            gidsInHeds = gidsInHeds as Set<string>;
            if (gidsInHeadsAfterEntry.size < gidsInHeds.size) {
                const missingGids: string[] = [];
                gidsInHeds.forEach((gid) => {
                    if (!gidsInHeadsAfterEntry.has(gid)) {
                        missingGids.push(gid);
                    }
                });

                // Call callback
                options.onGidsShadowed(missingGids);
            }
        }
        entry.init({ encoding: this._encoding, encryption: this._encryption });
        return entry;
    }

    /*
     * Creates a javscript iterator over log entries
     *
     * @param {Object} options
     * @param {string|Array} options.gt Beginning hash of the iterator, non-inclusive
     * @param {string|Array} options.gte Beginning hash of the iterator, inclusive
     * @param {string|Array} options.lt Ending hash of the iterator, non-inclusive
     * @param {string|Array} options.lte Ending hash of the iterator, inclusive
     * @param {amount} options.amount Number of entried to return to / from the gte / lte hash
     * @returns {Symbol.Iterator} Iterator object containing log entries
     *
     * @examples
     *
     * (async () => {
     *   log1 = new Log(ipfs, testIdentity, { gid:  'X' })
     *
     *   for (let i = 0; i <= 100; i++) {
     *     await log1.append('entry' + i)
     *   }
     *
     *   let it = log1.iterator({
     *     lte: 'zdpuApFd5XAPkCTmSx7qWQmQzvtdJPtx2K5p9to6ytCS79bfk',
     *     amount: 10
     *   })
     *
     *   [...it].length // 10
     * })()
     *
     *
     */
    iterator(options: {
        gt?: string;
        gte?: string;
        lt?: Entry<T>[] | string;
        lte?: Entry<T>[] | string;
        amount?: number;
    }): IterableIterator<Entry<T>> {
        if (options.amount === undefined) {
            options.amount = -1;
        }
        let { lt, lte } = options;
        const { gt, gte, amount } = options;

        // TODO make failsafe for missing log values

        if (amount === 0) return [][Symbol.iterator]();
        if (typeof lte === "string") lte = [this.get(lte)!];
        if (typeof lt === "string") lt = [this.get(this.get(lt)!.next[0])!];

        if (lte && !Array.isArray(lte))
            throw LogError.LtOrLteMustBeStringOrArray();
        if (lt && !Array.isArray(lt))
            throw LogError.LtOrLteMustBeStringOrArray();

        const start = (lte || lt || this.heads).filter(isDefined);
        const endHash = gte
            ? this.get(gte)!.hash
            : gt
            ? this.get(gt)!.hash
            : undefined;
        const count = endHash ? -1 : amount || -1;

        const entries = this.traverse(start, count, endHash);
        let entryValues = Object.values(entries);

        // Strip off last entry if gt is non-inclusive
        if (gt) entryValues.pop();

        // Deal with the amount argument working backwards from gt/gte
        if ((gt || gte) && amount > -1) {
            entryValues = entryValues.slice(
                entryValues.length - amount,
                entryValues.length
            );
        }

        return (function* () {
            for (const i in entryValues) {
                yield entryValues[i];
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
        options?: { size?: number; verifySignatures?: boolean }
    ) {
        // Get the difference of the logs
        const newItems = await Log.difference(log, this);
        /* let prevPeers = undefined; */
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
                this._length++; /* istanbul ignore else */
            }
            e.next.forEach((a) => {
                let nextIndexSet = this._nextsIndex[a];
                if (!nextIndexSet) {
                    nextIndexSet = new Set();
                    this._nextsIndex[a] = nextIndexSet;
                }
                this._nextsIndex[a].add(e.hash);
            });

            const clock = await e.getClock();
            this._hlc.update(clock.timestamp);
        }

        //    this._entryIndex.add(newItems)

        // Merge the heads
        const nextsFromNewItems = Object.values(newItems)
            .map(getNextPointers)
            .reduce(flatMap, []);
        const notReferencedByNewItems = (e: Entry<any>) =>
            !nextsFromNewItems.find((a) => a === e.hash);
        const notInCurrentNexts = (e: Entry<any>) => !this._nextsIndex[e.hash];
        const mergedHeads = Log.findHeads([this, log])
            .filter(notReferencedByNewItems)
            .filter(notInCurrentNexts)
            .reduce(uniqueEntriesReducer, {});

        this._headsIndex.reset(mergedHeads);

        if (typeof options?.size === "number") {
            this.prune(options.size);
        }

        return this;
    }

    getHeads(from: string): Entry<T>[] {
        const stack = [from];
        const traversed = new Set<string>();
        const res = new Set<string>();

        /*  let startSize = this.get(from).cumulativeSize; */
        const pushToStack = (hash: string) => {
            if (!traversed.has(hash)) {
                stack.push(hash);
                traversed.add(hash);
            }
        };

        while (stack.length > 0) {
            const hash = stack.shift();
            if (!hash) {
                logger.error("Missing hash when `getHeads`");
                continue;
            }
            const links = this._nextsIndex[hash];
            /*     const currentSize = this.get(hash).cumulativeSize; */
            const isConstrainedBySize = false; // currentSize - startSize > options.maxSize;
            if (!links || isConstrainedBySize) {
                // is head or we have to fork because of size constaint
                if (from !== hash && !isConstrainedBySize) {
                    let invertedMapToHead = this._nextsIndexToHead[from];
                    if (!invertedMapToHead) {
                        invertedMapToHead = new Set();
                        this._nextsIndexToHead[from] = invertedMapToHead;
                    }
                    invertedMapToHead.add(hash);
                }
                res.add(hash);
                traversed.add(hash);
            } else {
                const shortCutLinks = this._nextsIndexToHead[hash];
                (shortCutLinks || links).forEach(pushToStack);
            }
        }
        return [...res]
            .map((h) => this.get(h))
            .filter((x) => !!x) as Entry<T>[];
    }

    async deleteRecursively(from: Entry<any>) {
        const stack = [from];
        while (stack.length > 0) {
            const entry = stack.pop()!;
            await entry.delete(this._storage);
            this._entryIndex.delete(entry.hash);
            for (const next of entry.next) {
                const ne = this.get(next);
                if (ne) {
                    stack.push(ne);
                }
            }
        }
    }

    /**
     * Cut log to size
     * @param size
     */
    prune(size: number) {
        // Slice to the requested size
        let tmp = this.values;
        tmp = tmp.slice(-size);
        this._entryIndex = new EntryIndex(tmp.reduce(uniqueEntriesReducer, {}));
        this._headsIndex.reset(
            Log.findHeads(tmp).reduce(uniqueEntriesReducer, {})
        );
        this._length = this._entryIndex.length;
    }

    removeAll(heads: Entry<any>[]) {
        const stack: Entry<any>[] = [...heads];
        while (stack.length > 0) {
            const next = stack.shift();
            if (!next) {
                logger.error("Tried to remove null head");
                continue;
            }

            this._entryIndex.delete(next.hash);
            delete this._nextsIndexToHead[next.hash];
            this._headsIndex.del(next.hash);
            delete this._nextsIndex[next.hash];
            next.next.forEach((n) => {
                const value = this.get(n);
                if (value) {
                    stack.push(value);
                }
            });
        }
        this._length = this._entryIndex.length;
    }

    /**
     * Get the log in JSON format.
     * @returns {Object} An object with the id and heads properties
     */
    toJSON() {
        return {
            id: this._id,
            heads: this.heads
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
            heads: this.heads,
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

    /**
     * Create a log from a hashes.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Identity} identity The identity instance
     * @param {string} hash The log hash
     * @param {Object} options
     * @param {AccessController} options.access The access controller instance
     * @param {number} options.length How many items to include in the log
     * @param {Array<Entry<T>>} options.exclude Entries to not fetch (cached)
     * @param {function(hash, entry,  parent, depth)} options.onProgressCallback
     * @param {Function} options.sortFn The sort function - by default LastWriteWins
     * @returns {Promise<Log>}
     */
    static async fromMultihash<T>(
        ipfs: IPFS,
        identity: Identity,
        hash: string,
        options: { sortFn?: Sorting.ISortFunction } & EntryFetchAllOptions<T>
    ) {
        // TODO: need to verify the entries with 'key'
        const { logId, entries, heads } = await LogIO.fromMultihash(
            ipfs,
            hash,
            {
                length: options?.length,
                exclude: options?.exclude,
                shouldExclude: options?.shouldExclude,
                timeout: options?.timeout,
                onProgressCallback: options?.onProgressCallback,
                concurrency: options?.concurrency,
                sortFn: options?.sortFn,
            }
        );
        return new Log<T>(ipfs, identity, {
            encryption: options?.encryption,
            encoding: options?.encoding,
            logId,
            entries,
            heads,
            sortFn: options?.sortFn,
        });
    }

    /**
     * Create a log from a single entry's hash.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Identity} identity The identity instance
     * @param {string} hash The entry's hash
     * @param {Object} options
     * @param {string} options.logId The ID of the log
     * @param {AccessController} options.access The access controller instance
     * @param {number} options.length How many entries to include in the log
     * @param {Array<Entry<T>>} options.exclude Entries to not fetch (cached)
     * @param {function(hash, entry,  parent, depth)} options.onProgressCallback
     * @param {Function} options.sortFn The sort function - by default LastWriteWins
     * @return {Promise<Log>} New Log
     */
    static async fromEntryHash<T>(
        ipfs: IPFS,
        identity: Identity,
        hash: string | string[],
        options: {
            encoding?: Encoding<T>;
            encryption?: PublicKeyEncryptionResolver;
            logId?: string;
            length?: number;
            exclude?: any[];
            shouldExclude?: (hash: string) => boolean;
            timeout?: number;
            concurrency?: number;
            sortFn?: any;
            onProgressCallback?: any;
        } = { length: -1, exclude: [] }
    ): Promise<Log<T>> {
        // TODO: need to verify the entries with 'key'
        const { entries } = await LogIO.fromEntryHash(ipfs, hash, {
            length: options.length,
            exclude: options.exclude,
            encryption: options?.encryption,
            encoding: options.encoding,
            shouldExclude: options.shouldExclude,
            timeout: options.timeout,
            concurrency: options.concurrency,
            onProgressCallback: options.onProgressCallback,
            sortFn: options.sortFn,
        });
        return new Log<T>(ipfs, identity, {
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
     * @param {function(hash, entry,  parent, depth)} [options.onProgressCallback]
     * @param {Function} options.sortFn The sort function - by default LastWriteWins
     * @return {Promise<Log>} New Log
     */
    static async fromJSON<T>(
        ipfs: IPFS,
        identity: Identity,
        json: { id: string; heads: string[] },
        options: {
            encoding?: Encoding<T>;
            encryption?: PublicKeyEncryptionResolver;
            length?: number;
            timeout?: number;
            sortFn?: Sorting.ISortFunction;
            onProgressCallback?: (entry: Entry<T>) => void;
        } = { encoding: JSON_ENCODING }
    ) {
        // TODO: need to verify the entries with 'key'
        const { logId, entries } = await LogIO.fromJSON(ipfs, json, {
            length: options?.length,
            encryption: options?.encryption,
            encoding: options.encoding,
            timeout: options?.timeout,
            onProgressCallback: options?.onProgressCallback,
        });
        return new Log<T>(ipfs, identity, {
            encryption: options?.encryption,
            encoding: options?.encoding,
            logId,
            entries,
            sortFn: options?.sortFn,
        });
    }

    /**
     * Create a new log from an Entry instance.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Identity} identity The identity instance
     * @param {Entry|Array<Entry<T>>} sourceEntries An Entry or an array of entries to fetch a log from
     * @param {Object} options
     * @param {AccessController} options.access The access controller instance
     * @param {number} options.length How many entries to include. Default: infinite.
     * @param {Array<Entry<T>>} options.exclude Entries to not fetch (cached)
     * @param {function(hash, entry,  parent, depth)} [options.onProgressCallback]
     * @param {Function} options.sortFn The sort function - by default LastWriteWins
     * @return {Promise<Log>} New Log
     */
    static async fromEntry<T>(
        ipfs: IPFS,
        identity: Identity,
        sourceEntries: Entry<T>[] | Entry<T>,
        options: EntryFetchOptions<T> & {
            shouldExclude?: (hash: string) => boolean;
            encryption?: PublicKeyEncryptionResolver;
            sortFn?: Sorting.ISortFunction;
        }
    ) {
        // TODO: need to verify the entries with 'key'
        options = strictFetchOptions(options);
        const { entries } = await LogIO.fromEntry(ipfs, sourceEntries, {
            length: options.length,
            exclude: options.exclude,
            encryption: options?.encryption,
            encoding: options.encoding,
            timeout: options.timeout,
            concurrency: options.concurrency,
            shouldExclude: options.shouldExclude,
            onProgressCallback: options.onProgressCallback,
        });
        return new Log<T>(ipfs, identity, {
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
                entryOrLog.heads.forEach((head) => {
                    entries.push(head);
                });
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
        a: Log<T>,
        b: Log<T>
    ): Promise<Map<string, Entry<T>>> {
        const stack: string[] = [...a._headsIndex._index.keys()];
        const traversed: { [key: string]: boolean } = {};
        const res: Map<string, Entry<T>> = new Map();

        const pushToStack = (hash: string) => {
            if (!traversed[hash] && !b.get(hash)) {
                stack.push(hash);
                traversed[hash] = true;
            }
        };

        while (stack.length > 0) {
            const hash = stack.shift();
            if (!hash) {
                throw new Error("Unexpected");
            }
            const entry = a.get(hash);
            if (entry && !b.get(hash)) {
                // TODO do we need to do som GID checks?
                res.set(entry.hash, entry);
                traversed[entry.hash] = true;

                // TODO init below is kind of flaky to do this here, but we dont want to iterate over all entries before the difference method is invoked in the join log method
                entry.init({
                    encryption: b._encryption,
                    encoding: b._encoding,
                });
                (await entry.getNext()).forEach(pushToStack);
            }
        }
        return res;
    }
}
