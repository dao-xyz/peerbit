import { EncryptionTemplateMaybeEncrypted, Entry, LamportClock as Clock, LamportClock } from '@dao-xyz/ipfs-log-entry';
import { EntryIndex } from "./entry-index"
import pMap from 'p-map'
import { GSet } from './g-set'
import { LogIO } from './log-io'
import * as LogError from './log-errors'
import * as Sorting from './log-sorting'
import { EntryFetchAllOptions, EntryFetchOptions, strictFetchOptions } from "./entry-io"
import { IPFS } from "ipfs-core-types/src/"
import { CanAppendAccessController, DefaultAccessController } from "./default-access-controller"
import { isDefined } from './is-defined'
import { findUniques } from "./find-uniques"
import { IOOptions } from "@dao-xyz/ipfs-log-entry";
import { JSON_ENCODING_OPTIONS } from '@dao-xyz/ipfs-log-entry';
import { AccessError, PublicKeyEncryption } from '@dao-xyz/encryption-utils';
import { bigIntMax } from './utils';
import { Ed25519PublicKeyData, PublicKey } from '@dao-xyz/identity';
import { serialize } from '@dao-xyz/borsh';
import { Ed25519PublicKey } from 'sodium-plus';

const { LastWriteWins, NoZeroes } = Sorting
const randomId = () => new Date().getTime().toString()
const getHash = <T>(e: Entry<T>) => e.hash
const flatMap = (res, acc) => res.concat(acc)
const getNextPointers = entry => entry.next
const maxClockTimeReducer = <T>(res: bigint, acc: Entry<T>): bigint => bigIntMax(res, acc.clock.time);
/* const maxSizeReducer = <T>(res: bigint, acc: Entry<T>): bigint => bigIntMax(res, acc.cumulativeSize); */

const uniqueEntriesReducer = <T>(res: { [key: string]: Entry<T> }, acc: Entry<T>) => {
  res[acc.hash] = acc
  return res
}

export interface RecycleOptions {
  maxOplogLength: number, // Max length of oplog before cutting
  cutOplogToLength?: number, // When oplog shorter, cut to length
}

export type LogOptions<T> = { encoding?: IOOptions<T>, encryption?: PublicKeyEncryption, logId?: string, entries?: Entry<T>[], heads?: any, clock?: LamportClock, access?: CanAppendAccessController<T>, sortFn?: Sorting.ISortFunction, concurrency?: number, recycle?: RecycleOptions };
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
  _storage: any;
  _id: any;

  // Access Controller
  _access: CanAppendAccessController<T>

  // Identity
  _publicKey: PublicKey
  _sign: (data: Uint8Array) => Promise<Uint8Array>

  // Add entries to the internal cache
  _entryIndex: EntryIndex<T>
  _headsIndex: { [key: string]: Entry<T> };

  // Index of all next pointers in this log
  _nextsIndex: { [key: string]: Set<string> }

  // next -> entry
  _nextsIndexToHead: { [key: string]: Set<string> } // TODO make to LRU since this will become invalid quickly (and potentially huge)



  // Set the length, we calculate the length manually internally
  _length: number // Total amount of elements in the log
  _clock: Clock;
  _recycle?: RecycleOptions
  _encoding: IOOptions<T>
  _encryption: PublicKeyEncryption

  joinConcurrency: number;
  /**
   * Create a new Log instance
   * @param {IPFS} ipfs An IPFS instance
   * @param {Object} identity Identity (https://github.com/orbitdb/orbit-db-identity-provider/blob/master/src/identity.js)
   * @param {Object} options
   * @param {string} options.logId ID of the log
   * @param {Object} options.access AccessController (./default-access-controller)
   * @param {Array<Entry<T>>} options.entries An Array of Entries from which to create the log
   * @param {Array<Entry<T>>} options.heads Set the heads of the log
   * @param {Clock} options.clock Set the clock of the log
   * @param {Function} options.sortFn The sort function - by default LastWriteWins
   * @return {Log} The log instance
   */

  constructor(ipfs: IPFS, publicKey: PublicKey | Ed25519PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, options: LogOptions<T> = {}) {

    if (!isDefined(ipfs)) {
      throw LogError.IPFSNotDefinedError()
    }

    if (!isDefined(publicKey)) {
      throw new Error('Identity is required')
    }
    if (publicKey instanceof Ed25519PublicKey) {
      publicKey = new Ed25519PublicKeyData({
        publicKey
      })
    }

    if (publicKey instanceof Ed25519PublicKey) {
      publicKey = new Ed25519PublicKeyData({
        publicKey
      })
    }

    let { logId, access, entries, heads, clock, sortFn, concurrency, recycle, encoding, encryption } = options;
    if (!isDefined(access)) {
      access = new DefaultAccessController()
    }

    if (isDefined(entries) && !Array.isArray(entries)) {
      throw new Error('\'entries\' argument must be an array of Entry instances')
    }

    if (isDefined(heads) && !Array.isArray(heads)) {
      throw new Error('\'heads\' argument must be an array')
    }

    if (!isDefined(sortFn)) {
      sortFn = LastWriteWins
    }

    super()

    this._sortFn = NoZeroes(sortFn)

    this._storage = ipfs
    this._id = logId || randomId()

    // Access Controller
    this._access = access

    // Identity
    this._publicKey = publicKey
    this._sign = sign;

    // encoder/decoder
    this._encoding = encoding;
    if (!this._encoding) {
      this._encoding = JSON_ENCODING_OPTIONS;
    }

    this._encryption = encryption;

    // Add entries to the internal cache
    const uniqueEntries = (entries || []).reduce(uniqueEntriesReducer, {})
    this._entryIndex = new EntryIndex(uniqueEntries)
    entries = Object.values(uniqueEntries) || []

    // Init io for entries (as these are not created with the append method)
    entries.map(e => e.init({ encoding: this._encoding, encryption: this._encryption }));


    // Set heads if not passed as an argument
    heads = heads || Log.findHeads(entries)
    this._headsIndex = heads.reduce(uniqueEntriesReducer, {})

    // Index of all next pointers in this log
    this._nextsIndex = {}
    this._nextsIndexToHead = {}

    const addToNextsIndex = (e: Entry<T>) => {
      e.next.forEach(a => {
        let nextIndexSet = this._nextsIndex[a];
        if (!nextIndexSet) {
          nextIndexSet = new Set();
          this._nextsIndex[a] = nextIndexSet;
        }
        this._nextsIndex[a].add(e.hash);
      })

    }

    entries.forEach(addToNextsIndex)

    // Set the length, we calculate the length manually internally
    this._length = entries.length

    // Set the clock
    const maxTime = bigIntMax(clock ? clock.time : 0n, this.heads.reduce(maxClockTimeReducer, 0n))
    // Take the given key as the clock id is it's a Key instance,
    // otherwise if key was given, take whatever it is,
    // and if it was null, take the given id as the clock id
    this._clock = new Clock(new Uint8Array(serialize(publicKey)), maxTime)

    this.joinConcurrency = concurrency || 16

    this._recycle = { ...recycle };
    if (this._recycle.cutOplogToLength == undefined) {
      this._recycle.cutOplogToLength = this._recycle.maxOplogLength;
    }
  }

  /**
   * Returns the ID of the log.
   * @returns {string}
   */
  get id() {
    return this._id
  }

  /**
   * Returns the clock of the log.
   * @returns {string}
   */
  get clock() {
    return this._clock
  }

  /**
   * Returns the length of the log.
   * @return {number} Length
   */
  get length() {
    return this._length
  }

  /**
   * Returns the values in the log.
   * @returns {Array<Entry<T>>}
   */
  get values(): Entry<T>[] {
    return Object.values(this.traverse(this.heads)).reverse()
  }

  /**
   * Returns the values in the log.
   * @returns {Array<T>}
   */
  /*  get payloadsDecoded(): { payload: T, entry: Entry<T>}[] {
     return Object.values(this.traverse(this.heads)).reverse().map(entry => { return { Entry<T>, payload: this._encoding.decoder(entry.payload.value) } })
   } */

  /**
   * Returns an array of heads.
   * @returns {Array<Entry<T>>}
   */
  get heads(): Entry<T>[] {
    return Object.values(this._headsIndex).sort(this._sortFn).reverse()
  }

  /**
   * Returns an array of Entry objects that reference entries which
   * are not in the log currently.
   * @returns {Array<Entry<T>>}
   */
  get tails() {
    return Log.findTails(this.values)
  }

  /**
   * Returns an array of hashes that are referenced by entries which
   * are not in the log currently.
   * @returns {Array<string>} Array of hashes
   */
  get tailHashes() {
    return Log.findTailHashes(this.values)
  }

  /**
   * Set the identity for the log
   * @param {Identity} [identity] The identity to be set
   */
  setPublicKey(publicKey: PublicKey) {
    this._publicKey = publicKey
    // Find the latest clock from the heads
    const time = bigIntMax(this.clock.time, this.heads.reduce(maxClockTimeReducer, 0n))
    this._clock = new Clock(new Uint8Array(serialize(this._publicKey)), time)
  }


  /**
   * Find an entry.
   * @param {string} [hash] The hashes of the entry
   * @returns {Entry|undefined}
   */
  get(hash: string) {
    return this._entryIndex.get(hash)
  }

  /**
   * Checks if a entry is part of the log
   * @param {string} hash The hash of the entry
   * @returns {boolean}
   */
  has(entry: Entry<string> | string) {
    if (entry instanceof Entry && entry.hash) {
      throw new Error("Expected entry hash to be defined");
    }
    return this._entryIndex.get(entry instanceof Entry ? entry.hash : entry) !== undefined
  }

  traverse(rootEntries: Entry<T>[], amount: number = -1, endHash?: string): { [key: string]: Entry<T> } {
    // Sort the given given root entries and use as the starting stack
    let stack: Entry<T>[] = rootEntries.sort(this._sortFn).reverse()

    // Cache for checking if we've processed an entry already
    let traversed: { [key: string]: boolean } = {}
    // End result
    const result: { [key: string]: Entry<T> } = {}
    let count = 0
    // Named function for getting an entry from the log
    const getEntry = (e: string) => this.get(e)

    // Add an entry to the stack and traversed nodes index
    const addToStack = (entry: Entry<T>) => {
      // If we've already processed the Entry<T>, don't add it to the stack
      if (!entry || traversed[entry.hash]) {
        return
      }

      // Add the entry in front of the stack and sort
      stack = [entry, ...stack]
        .sort(this._sortFn)
        .reverse()
      // Add to the cache of processed entries
      traversed[entry.hash] = true
    }

    const addEntry = (rootEntry: Entry<T>) => {
      result[rootEntry.hash] = rootEntry
      traversed[rootEntry.hash] = true
      count++
    }

    // Start traversal
    // Process stack until it's empty (traversed the full log)
    // or when we have the requested amount of entries
    // If requested entry amount is -1, traverse all
    while (stack.length > 0 && (count < amount || amount < 0)) { // eslint-disable-line no-unmodified-loop-condition
      // Get the next element from the stack
      const entry = stack.shift()
      // Add to the result
      addEntry(entry)
      // If it is the specified end hash, break out of the while loop
      if (endHash && endHash === entry.hash) break

      // Add entry's next references to the stack
      const entries = entry.next.map(getEntry)
      const defined = entries.filter(isDefined)
      defined.forEach(addToStack)
    }

    stack = []
    traversed = {}
    // End result
    return result
  }

  getPow2Refs(pointerCount = 1) {

    const all = Object.values(this.traverse(this.heads, Math.max(pointerCount, this.heads.length)))

    // If pointer count is 4, returns 2
    // If pointer count is 8, returns 3 references
    // If pointer count is 512, returns 9 references
    // If pointer count is 2048, returns 11 references
    const getEveryPow2 = (maxDistance) => {
      const entries = new Set<Entry<T>>() // TODO set will not work since entry is not a string
      for (let i = 1; i <= maxDistance; i *= 2) {
        const index = Math.min(i - 1, all.length - 1)
        entries.add(all[index])
      }
      return entries
    }
    const references = getEveryPow2(Math.min(pointerCount, all.length))

    // Always include the last known reference
    if (all.length < pointerCount && all[all.length - 1]) {
      references.add(all[all.length - 1])// TODO can this yield a publicate?
    }
    return Array.from(references).map(getHash);

  }

  getNextsFromRefs(refs: string[]): string[] {
    let headsFromRefs = new Map<string, Entry<T>>();
    refs.forEach((ref) => {
      let headsFromRef = this.getHeads(ref); // TODO allow forks
      headsFromRef.forEach((head) => {
        headsFromRefs.set(head.hash, head);
      })
    })
    const nexts = [...headsFromRefs.values()].sort(this._sortFn).map(h => h.hash);
    return nexts;
  }

  /**
   * Append an entry to the log.
   * @param {Entry} entry Entry to add
   * @return {Log} New Log containing the appended value
   */
  async append(data: T, options: { refs?: string[], nexts?: string[], pin?: boolean, reciever?: EncryptionTemplateMaybeEncrypted } = { pin: false }) {

    if (options.reciever && !this._encryption) {
      throw new Error("Message is intended to be encrypted but no encryption methods are provided for the log")
    }

    // nextsreolver
    // 1. all heads
    // 2. all heads that are references
    // 3. next = refs if length of refs = 1

    // Update the clock (find the latest clock)
    const newTime = bigIntMax(this.clock.time, this.heads.reduce(maxClockTimeReducer, 0n)) + 1n
    this._clock = new Clock(this.clock.id, newTime)

    const currentHeads: Entry<T>[] = Object.values(this.heads.reverse().reduce(uniqueEntriesReducer, {})); // TODO this invokes a double reverse
    let refs: string[] = options.refs || this.getPow2Refs(); // custom || or pick refs using getPow2Refs
    let nexts: string[] = options.nexts || (options.refs ? this.getNextsFromRefs(refs) : currentHeads.map(h => h.hash)); // custom || get nexts from all heads (merging activity)

    // Some heads might not even be referenced by the refs, this will be merged into the headsIndex so we dont forget them
    let keepHeads: Entry<T>[] = options.nexts ? currentHeads.filter(h => !nexts.includes(h.hash)) : []; // TODO improve performance


    const isNotNext = (e: string) => !nexts.includes(e)
    refs = refs.filter(isNotNext);

    /*     const cumulativeSize = nexts.map(n => this.get(n)).reduce(maxSizeReducer, 0n)
     */

    const entry = await Entry.create<T>(
      {
        ipfs: this._storage,
        publicKey: this._publicKey,
        sign: this._sign,
        logId: this.id,
        data,
        // cumulativeSize
        next: nexts,
        clock: this.clock,
        refs,
        pin: options.pin,
        assertAllowed: async (payload, key) => {
          if (this._access.allowAll) {
            return;
          }
          const canAppend = await this._access.canAppend(payload, key);
          if (!canAppend) {
            throw new AccessError(`Could not append entry, key "${this._publicKey}" is not allowed to write to the log`)
          }
        },
        encodingOptions: this._encoding,
        encryption: (options.reciever) ? {
          options: this._encryption,
          reciever: {
            ...options.reciever
          }
        } : undefined
      }
    )

    if (!isDefined(entry.hash)) {
      throw new Error("Unexpected")
    }
    this._entryIndex.set(entry.hash, entry)
    nexts.forEach(e => {
      let nextIndexSet = this._nextsIndex[e];
      if (!nextIndexSet) {
        nextIndexSet = new Set();
        this._nextsIndex[e] = nextIndexSet;
      }
      this._nextsIndex[e].add(entry.hash);
    })
    this._headsIndex = {}
    this._headsIndex[entry.hash] = entry
    if (keepHeads) {
      keepHeads.forEach(head => {
        this._headsIndex[head.hash] = head;
      });
    }


    // Update the length
    this._length++

    if (this._recycle && this.length > this._recycle.maxOplogLength) {
      this.prune(this._recycle.cutOplogToLength);
    }
    return entry
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
   *   log1 = new Log(ipfs, testIdentity, { logId: 'X' })
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
  iterator({ gt = undefined, gte = undefined, lt = undefined, lte = undefined, amount = -1 } =
    {}) {
    if (amount === 0) return (function* () { })()
    if (typeof lte === 'string') lte = [this.get(lte)]
    if (typeof lt === 'string') lt = [this.get(this.get(lt).next[0])]

    if (lte && !Array.isArray(lte)) throw LogError.LtOrLteMustBeStringOrArray()
    if (lt && !Array.isArray(lt)) throw LogError.LtOrLteMustBeStringOrArray()

    const start = (lte || (lt || this.heads)).filter(isDefined)
    const endHash = gte ? this.get(gte).hash : gt ? this.get(gt).hash : null
    const count = endHash ? -1 : amount || -1

    const entries = this.traverse(start, count, endHash)
    let entryValues = Object.values(entries)

    // Strip off last entry if gt is non-inclusive
    if (gt) entryValues.pop()

    // Deal with the amount argument working backwards from gt/gte
    if ((gt || gte) && amount > -1) {
      entryValues = entryValues.slice(entryValues.length - amount, entryValues.length)
    }

    return (function* () {
      for (const i in entryValues) {
        yield entryValues[i]
      }
    })()
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
  async join(log: Log<T>, size = -1) {
    if (this.id !== log.id) return

    // Get the difference of the logs
    const newItems = await Log.difference(log, this)

    /*     const identityProvider = this._identity.provider
     */    // Verify if entries are allowed to be added to the log and throws if
    // there's an invalid entry
    // assume sorted 

    const permitted = async (entry: Entry<T>) => {
      entry.init({
        encoding: this._encoding,
        encryption: this._encryption
      })
      const canAppend = await this._access.canAppend(entry._payload, entry._publicKey)
      if (!canAppend) {
        throw new AccessError(`Could not append Entry<T>, key "${(await entry.publicKey)}" is not allowed to write to the log`)
      }
    }

    // Verify signature for each entry and throws if there's an invalid signature
    /*   const verify = async (entry: Entry<T>) => {
        const isValid = await Entry.verify(identityProvider, entry)
        const identity = await entry.identity;
        const signature = await entry.signature;
        const publicKey = identity.publicKey
        if (!isValid) throw new Error(`Could not validate signature "${signature}" for entry "${entry.hash}" and key "${publicKey}"`)
      } */

    const entriesToJoin = Object.values(newItems)
    await pMap(entriesToJoin, async (e: Entry<T>) => {
      e.init({ encoding: this._encoding, encryption: this._encryption })
      await permitted(e)
      /*  await verify(e)  */ // Assumes the access controller is verifying signatures
    }, { concurrency: this.joinConcurrency })


    // Update the internal next pointers index
    const addToNextsIndex = (e: Entry<any>) => {
      if (!isDefined(e.hash)) {
        throw new Error("Unexpected");
      }
      const entry = this.get(e.hash)
      if (!entry) this._length++ /* istanbul ignore else */
      e.next.forEach(a => {
        let nextIndexSet = this._nextsIndex[a];
        if (!nextIndexSet) {
          nextIndexSet = new Set();
          this._nextsIndex[a] = nextIndexSet;
        }
        this._nextsIndex[a].add(e.hash);
      })
    }

    Object.values(newItems).forEach(addToNextsIndex)

    // Update the internal entry index
    this._entryIndex.add(newItems)

    // Merge the heads
    const nextsFromNewItems = Object.values(newItems).map(getNextPointers).reduce(flatMap, [])
    const notReferencedByNewItems = (e: Entry<any>) => !nextsFromNewItems.find(a => a === e.hash)
    const notInCurrentNexts = (e: Entry<any>) => !this._nextsIndex[e.hash]
    const mergedHeads = Log.findHeads(Object.values(Object.assign({}, this._headsIndex, log._headsIndex)))
      .filter(notReferencedByNewItems)
      .filter(notInCurrentNexts)
      .reduce(uniqueEntriesReducer, {})

    this._headsIndex = mergedHeads

    // TODO add cumulatve size calc


    // Slice to the requested size
    if (size > -1) {
      this.prune(size);
    }

    // Find the latest clock from the heads
    const maxClock = Object.values(this._headsIndex).reduce(maxClockTimeReducer, 0n)
    this._clock = new Clock(this.clock.id, bigIntMax(this.clock.time, maxClock))

    return this
  }

  getHeads(from: string): Entry<T>[] {
    const stack = [from]
    const traversed = new Set<string>();
    const res = new Set<string>();

    /*  let startSize = this.get(from).cumulativeSize; */
    const pushToStack = (hash: string) => {
      if (!traversed.has(hash)) {
        stack.push(hash)
        traversed.add(hash)
      }
    }

    while (stack.length > 0) {
      const hash = stack.shift()
      const links = this._nextsIndex[hash];
      /*     const currentSize = this.get(hash).cumulativeSize; */
      const isConstrainedBySize = false; // currentSize - startSize > options.maxSize;
      if (!links || isConstrainedBySize) { // is head or we have to fork because of size constaint
        if (from !== hash && !isConstrainedBySize) {
          let invertedMapToHead = this._nextsIndexToHead[from];
          if (!invertedMapToHead) {
            invertedMapToHead = new Set();
            this._nextsIndexToHead[from] = invertedMapToHead;
          }
          invertedMapToHead.add(hash);
        }
        res.add(hash)
        traversed.add(hash)
      }
      else {
        let shortCutLinks = this._nextsIndexToHead[hash];
        (shortCutLinks || links).forEach(pushToStack);
      }
    }
    return [...res].map(h => this.get(h));
  }

  tickClock() {
    this.clock.time += 1n;
  }

  mergeClock(clock: LamportClock): LamportClock {
    this.clock.time = bigIntMax(this.clock.time, clock.time)
    return new LamportClock(this.clock.id, this.clock.time)
  }


  /**
   * Cut log to size
   * @param size 
   */
  prune(size: number) {

    // Slice to the requested size
    let tmp = this.values
    tmp = tmp.slice(-size)
    this._entryIndex = null
    this._entryIndex = new EntryIndex(tmp.reduce(uniqueEntriesReducer, {}))
    this._headsIndex = Log.findHeads(tmp).reduce(uniqueEntriesReducer, {})
    this._length = this._entryIndex.length
  }

  /**
   * Get the log in JSON format.
   * @returns {Object} An object with the id and heads properties
   */
  toJSON() {
    return {
      id: this.id,
      heads: this.heads
        .sort(this._sortFn) // default sorting
        .reverse() // we want the latest as the first element
        .map(getHash) // return only the head hashes
    }
  }

  /**
   * Get the log in JSON format as a snapshot.
   * @returns {Object} An object with the id, heads and value properties
   */
  toSnapshot() {
    return {
      id: this.id,
      heads: this.heads,
      values: this.values
    }
  }

  /**
   * Get the log as a Buffer.
   * @returns {Buffer}
   */
  toBuffer() {
    return Buffer.from(JSON.stringify(this.toJSON()))
  }

  /**
   * Returns the log entries as a formatted string.
   * @returns {string}
   * @example
   * two
   * └─one
   *   └─three
   */
  toString(payloadMapper?: (payload: any) => string) {
    return this.values
      .slice()
      .reverse()
      .map((e, idx) => {
        const parents = Entry.findChildren(e, this.values)
        const len = parents.length
        let padding = new Array(Math.max(len - 1, 0))
        padding = len > 1 ? padding.fill('  ') : padding
        padding = len > 0 ? padding.concat(['└─']) : padding
        /* istanbul ignore next */
        return padding.join('') + (payloadMapper ? payloadMapper(e.payload.value) : e.payload.value)
      })
      .join('\n')
  }

  /**
   * Check whether an object is a Log instance.
   * @param {Object} log An object to check
   * @returns {boolean}
   */
  static isLog(log) {
    return log.id !== undefined &&
      log.heads !== undefined &&
      log._entryIndex !== undefined
  }

  /**
   * Get the log's multihash.
   * @returns {Promise<string>} Multihash of the Log as Base58 encoded string.
   */
  toMultihash(options?: {
    format?: string;
  }) {
    return LogIO.toMultihash(this._storage, this, options)
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
  static async fromMultihash<T>(ipfs, key: PublicKey | Ed25519PublicKey, sign: (Uint8Array) => Promise<Uint8Array>, hash,
    options?: { encoding?: IOOptions<T>, encryption?: PublicKeyEncryption, access?: CanAppendAccessController<T>, sortFn?: Sorting.ISortFunction } & EntryFetchAllOptions<T>) {
    // TODO: need to verify the entries with 'key'
    const { logId, entries, heads } = await LogIO.fromMultihash(ipfs, hash,
      { length: options?.length, exclude: options?.exclude, shouldExclude: options?.shouldExclude, timeout: options?.timeout, onProgressCallback: options?.onProgressCallback, concurrency: options?.concurrency, sortFn: options?.sortFn })
    return new Log(ipfs, key, sign, { encoding: options?.encoding, encryption: options?.encryption, logId, access: options?.access, entries, heads, sortFn: options?.sortFn })
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
  static async fromEntryHash<T>(ipfs: IPFS, key: PublicKey | Ed25519PublicKey, sign: (Uint8Array) => Promise<Uint8Array>, hash: string | string[],
    options: { encoding?: IOOptions<T>, encryption?: PublicKeyEncryption, logId?: string, access?: any, length?: number, exclude?: any[], shouldExclude?: (string) => boolean, timeout?: number, concurrency?: number, sortFn?: any, onProgressCallback?: any } = { length: -1, exclude: [] }) {
    // TODO: need to verify the entries with 'key'
    const { entries } = await LogIO.fromEntryHash(ipfs, hash,
      { length: options.length, exclude: options.exclude, encryption: options?.encryption, shouldExclude: options.shouldExclude, timeout: options.timeout, concurrency: options.concurrency, onProgressCallback: options.onProgressCallback, sortFn: options.sortFn })
    return new Log(ipfs, key, sign, { encoding: options?.encoding, encryption: options?.encryption, logId: options.logId, access: options.access, entries, sortFn: options.sortFn })
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
  static async fromJSON<T>(ipfs: IPFS, key: PublicKey | Ed25519PublicKey, sign: (Uint8Array) => Promise<Uint8Array>, json: { id: string, heads: string[] },
    options?: { encoding?: IOOptions<T>, encryption?: PublicKeyEncryption, access?: CanAppendAccessController<T>, length?: number, timeout?: number, sortFn?: Sorting.ISortFunction, onProgressCallback?: (entry: Entry<T>) => void }) {
    // TODO: need to verify the entries with 'key'
    const { logId, entries } = await LogIO.fromJSON(ipfs, json,
      { length: options?.length, encryption: options?.encryption, timeout: options?.timeout, onProgressCallback: options?.onProgressCallback })
    return new Log(ipfs, key, sign, { encoding: options?.encoding, encryption: options?.encryption, logId, entries, access: options?.access, sortFn: options?.sortFn })
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
  static async fromEntry<T>(ipfs: IPFS, key: PublicKey | Ed25519PublicKey, sign: (Uint8Array) => Promise<Uint8Array>, sourceEntries: Entry<T>[] | Entry<T>, options: EntryFetchOptions<T> & { encoding?: IOOptions<T>, shouldExclude?: (string) => boolean, encryption?: PublicKeyEncryption, access?: CanAppendAccessController<T>, sortFn?: Sorting.ISortFunction }) {
    // TODO: need to verify the entries with 'key'
    options = strictFetchOptions(options);
    const { logId, entries } = await LogIO.fromEntry(ipfs, sourceEntries,
      { length: options.length, exclude: options.exclude, encryption: options?.encryption, timeout: options.timeout, concurrency: options.concurrency, shouldExclude: options.shouldExclude, onProgressCallback: options.onProgressCallback })
    return new Log(ipfs, key, sign, { encoding: options?.encoding, encryption: options?.encryption, logId, access: options.access, entries, sortFn: options.sortFn })
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
  static findHeads<T>(entries: Entry<T>[]) {
    const indexReducer = (res, entry, idx, arr) => {
      const addToResult = e => (res[e] = entry.hash)
      entry.next.forEach(addToResult)
      return res
    }

    const items = entries.reduce(indexReducer, {})

    const exists = (e: Entry<T>) => items[e.hash] === undefined
    const compareIds = (a: Entry<T>, b: Entry<T>) => Clock.compare(a.clock, b.clock);
    return entries.filter(exists).sort(compareIds)
  }

  // Find entries that point to another entry that is not in the
  // input array
  static findTails<T>(entries: Entry<T>[]): Entry<T>[] {
    // Reverse index { next -> entry }
    const reverseIndex: { [key: string]: Entry<T>[] } = {}
    // Null index containing entries that have no parents (nexts)
    const nullIndex: Entry<T>[] = []
    // Hashes for all entries for quick lookups
    const hashes = {}
    // Hashes of all next entries
    let nexts: string[] = []

    const addToIndex = (e: Entry<T>) => {
      if (e.next.length === 0) {
        nullIndex.push(e)
      }
      const addToReverseIndex = (a: any) => {
        /* istanbul ignore else */
        if (!reverseIndex[a]) reverseIndex[a] = []
        reverseIndex[a].push(e)
      }

      // Add all entries and their parents to the reverse index
      e.next.forEach(addToReverseIndex)
      // Get all next references
      nexts = nexts.concat(e.next)
      // Get the hashes of input entries
      hashes[e.hash] = true
    }



    // Create our indices
    entries.forEach(addToIndex)

    const addUniques = (res: Entry<T>[], entries: Entry<T>[], _idx, _arr) => res.concat(findUniques(entries, 'hash'))
    const exists = (e: string) => hashes[e] === undefined
    const findFromReverseIndex = (e: string) => reverseIndex[e]

    // Drop hashes that are not in the input entries
    const tails = nexts // For every hash in nexts:
      .filter(exists) // Remove undefineds and nulls
      .map(findFromReverseIndex) // Get the Entry from the reverse index
      .reduce(addUniques, []) // Flatten the result and take only uniques
      .concat(nullIndex) // Combine with tails the have no next refs (ie. first-in-their-chain)

    return findUniques(tails, 'hash').sort(Entry.compare)
  }


  // Find the hashes to entries that are not in a collection
  // but referenced by other entries
  static findTailHashes(entries: Entry<any>[]) {
    const hashes = {}
    const addToIndex = e => (hashes[e.hash] = true)
    const reduceTailHashes = (res, entry, idx, arr) => {
      const addToResult = (e) => {
        /* istanbul ignore else */
        if (hashes[e] === undefined) {
          res.splice(0, 0, e)
        }
      }
      entry.next.reverse().forEach(addToResult)
      return res
    }

    entries.forEach(addToIndex)
    return entries.reduce(reduceTailHashes, [])
  }

  static async difference<T>(a: Log<T>, b: Log<T>) {
    const stack = Object.keys(a._headsIndex)
    const traversed: { [key: string]: boolean } = {}
    const res: { [key: string]: Entry<T> } = {}

    const pushToStack = (hash: string) => {
      if (!traversed[hash] && !b.get(hash)) {
        stack.push(hash)
        traversed[hash] = true
      }
    }

    while (stack.length > 0) {
      const hash = stack.shift()
      const entry = a.get(hash)
      if (entry && !b.get(hash) && await entry.getId() === await b.id) {
        res[entry.hash] = entry
        traversed[entry.hash] = true
        entry.next.concat(entry.refs).forEach(pushToStack)
      }
    }
    return res
  }
}

