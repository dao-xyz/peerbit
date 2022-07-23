import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io';
import { IPFS } from 'ipfs-core-types/src/'
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
const IpfsNotDefinedError = () => new Error('Ipfs instance not defined')

/*
 * @description
 * An ipfs-log entry
 */


export interface EntrySerialized {
  data: Uint8Array
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  next?: string[] | Entry[]
  refs?: string[] // Array of hashes
}

@variant(0)
export class EntryData {

  @field({ type: 'String' })
  id: string // For determining a unique chain

  @field({
    serialize: (obj: Uint8Array, writer) => {
      writer.writeU32(obj.length);
      for (let i = 0; i < obj.length; i++) {
        writer.writeU8(obj[i])
      }
    },
    deserialize: (reader) => {
      const len = reader.readU32();
      const arr = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        arr[i] = reader.readU8();
      }
      return arr;
    }
  })
  payload: Uint8Array

  @field({ type: Clock })
  clock: Clock

  @field({ type: option(IdentitySerializable) })
  identity?: IdentitySerializable;

  @field({ type: option('String') })
  key?: string;

  @field({ type: option('String') })
  sig?: string;

  constructor(obj?: {
    id: string // For determining a unique chain
    payload: Uint8Array
    clock: Clock
    identity?: IdentitySerializable;
    key?: string;
    sig?: string;
  }) {
    if (obj) {
      this.id = obj.id;
      this.payload = obj.payload;
      this.clock = obj.clock;
      this.identity = obj.identity;
      this.key = obj.key;
      this.sig = obj.sig;
    }
  }

  static from(arr: Uint8Array): EntryData {
    return deserialize(Buffer.from(arr), EntryData)
  }

}

@variant(0)
export class Entry {

  @field({ type: EntryData })
  data: EntryData

  @field({ type: option(vec('String')) })
  next?: string[] | Entry[]

  @field({ type: option(vec('String')) })
  refs?: string[] // Array of hashes

  @field({ type: option('String') })
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry

  static IPLD_LINKS = ['next', 'refs']
  static getWriteFormat = () => "dag-cbor" // Only dag-cbor atm


  constructor(obj?: {
    data: EntryData
    next?: string[] | Entry[]
    refs?: string[] // Array of hashes
    hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  }) {
    if (obj) {
      this.data = obj.data;
      this.next = obj.next;
      this.refs = obj.refs;
      this.hash = obj.hash;
    }
  }


  serialize(): EntrySerialized {
    return {
      data: serialize(this.data),
      hash: this.hash,
      next: this.next,
      refs: this.refs
    }
  }
  /**
   * Create an Entry
   * @param {IPFS} ipfs An IPFS instance
   * @param {Identity} identity The identity instance
   * @param {string} logId The unique identifier for this log
   * @param {*} data Data of the entry to be added. Can be any JSON.stringifyable data
   * @param {Array<string|Entry>} [next=[]] Parent hashes or entries
   * @param {LamportClock} [clock] The lamport clock
   * @returns {Promise<Entry>}
   * @example
   * const entry = await Entry.create(ipfs, identity, 'hello')
   * console.log(entry)
   * // { hash: null, payload: "hello", next: [] }
   */
  static async create(ipfs: IPFS, identity: Identity, logId: string, data: Uint8Array, next: (Entry | string)[] = [], clock?: Clock, refs: string[] = [], pin?: boolean, assertAllowed?: (entry: Entry) => Promise<void>) {
    if (!isDefined(ipfs)) throw IpfsNotDefinedError()
    if (!isDefined(identity)) throw new Error('Identity is required, cannot create entry')
    if (!isDefined(logId)) throw new Error('Entry requires an id')
    if (!isDefined(data)) throw new Error('Entry requires data')
    if (!isDefined(next) || !Array.isArray(next)) throw new Error("'next' argument is not an array")

    // Clean the next objects and convert to hashes
    const toEntry = (e) => e.hash ? e.hash : e
    const nexts = next.filter(isDefined).map(toEntry)
    const entry: Entry = new Entry({
      data: new EntryData({
        id: logId, // For determining a unique chain
        payload: data, // Can be any JSON.stringifyable data
        clock: clock || new Clock(identity.publicKey),
      }),
      hash: null, // "zd...Foo", we'll set the hash after persisting the entry
      next: nexts, // Array of hashes
      refs: refs,
    })
    const identitySerializable = identity.toSerializable();

    if (assertAllowed) {
      entry.data.identity = identitySerializable
      await assertAllowed(entry);
      entry.data.identity = undefined;
    }

    const signature = await identity.provider.sign(identitySerializable, Entry.toBuffer(entry))
    entry.data.key = identity.publicKey
    entry.data.identity = identitySerializable
    entry.data.sig = signature
    entry.hash = await Entry.toMultihash(ipfs, entry, pin)

    return entry
  }

  /**
   * Verifies an entry signature.
   *
   * @param {IdentityProvider} identityProvider The identity provider to use
   * @param {Entry} entry The entry being verified
   * @return {Promise} A promise that resolves to a boolean value indicating if the signature is valid
   */
  static async verify(identityProvider: Identities, entry: Entry) {
    if (!identityProvider) throw new Error('Identity-provider is required, cannot verify entry')
    if (!Entry.isEntry(entry)) throw new Error('Invalid Log entry')
    if (!entry.data.key) throw new Error("Entry doesn't have a key")
    if (!entry.data.sig) throw new Error("Entry doesn't have a signature")

    const e = Entry.toEntryWithoutSignature(entry)
    const verifier = 'v1'
    return identityProvider.verify(entry.data.sig, entry.data.key, Entry.toBuffer(e), verifier)
  }

  /**
   * Transforms an entry into a Buffer.
   * @param {Entry} entry The entry
   * @return {Buffer} The buffer
   */
  static toBuffer(entry: Entry) {
    return Buffer.from(serialize(entry))
  }

  /**
   * Get the multihash of an Entry.
   * @param {IPFS} ipfs An IPFS instance
   * @param {Entry} entry Entry to get a multihash for
   * @returns {Promise<string>}
   * @example
   * const multihash = await Entry.toMultihash(ipfs, entry)
   * console.log(multihash)
   * // "Qm...Foo"
   * @deprecated
   */
  static async toMultihash(ipfs: IPFS, entry: Entry, pin = false) {
    if (!ipfs) throw IpfsNotDefinedError()
    if (!Entry.isEntry(entry)) throw new Error('Invalid object format, cannot generate entry hash')

    // Ensure `entry` follows the correct format
    const e = Entry.toEntry(entry) // Will remove the hash
    return io.write(ipfs, Entry.getWriteFormat(), e.serialize(), { links: Entry.IPLD_LINKS, pin })
  }

  static toEntry(entry: Entry | EntrySerialized): Entry {
    const e: Entry = new Entry({
      hash: null,
      data: entry.data instanceof Uint8Array ? EntryData.from(entry.data) : new EntryData({
        id: entry.data.id,
        payload: entry.data.payload,
        identity: entry.data.identity,
        key: entry.data.key,
        sig: entry.data.sig,
        clock: new LamportClock(entry.data.clock.id, entry.data.clock.time)
      }),
      next: entry.next,
      refs: entry.refs
    })
    return e
  }

  static toEntryWithoutSignature(entry: Entry | EntrySerialized): Entry {
    const e: Entry = new Entry({
      hash: undefined,
      data: entry.data instanceof Uint8Array ? EntryData.from(entry.data) : new EntryData({
        clock: new LamportClock(entry.data.clock.id, entry.data.clock.time),
        id: entry.data.id,
        payload: entry.data.payload,
        identity: undefined,
        key: undefined,
        sig: undefined
      }),
      next: entry.next,
      refs: entry.refs
    })
    return e
  }



  /**
   * Create an Entry from a hash.
   * @param {IPFS} ipfs An IPFS instance
   * @param {string} hash The hash to create an Entry from
   * @returns {Promise<Entry>}
   * @example
   * const entry = await Entry.fromMultihash(ipfs, "zd...Foo")
   * console.log(entry)
   * // { hash: "Zd...Foo", payload: "hello", next: [] }
   */
  static async fromMultihash(ipfs, hash) {
    if (!ipfs) throw IpfsNotDefinedError()
    if (!hash) throw new Error(`Invalid hash: ${hash}`)
    const e: EntrySerialized = await io.read(ipfs, hash, { links: Entry.IPLD_LINKS })

    const entry = Entry.toEntry(e)
    entry.hash = hash

    return entry
  }

  /**
   * Check if an object is an Entry.
   * @param {Entry} obj
   * @returns {boolean}
   */
  static isEntry(obj: any) {
    if (obj instanceof Entry === false) {
      return false;
    }
    if (!obj.data) {
      return false
    }
    return obj && obj.data.id !== undefined &&
      obj.next !== undefined &&
      obj.data.payload !== undefined &&
      obj.hash !== undefined &&
      obj.data.clock !== undefined && obj.refs !== undefined
  }

  /**
   * Compares two entries.
   * @param {Entry} a
   * @param {Entry} b
   * @returns {number} 1 if a is greater, -1 is b is greater
   */
  static compare(a: Entry, b: Entry) {
    const distance = Clock.compare(a.data.clock, b.data.clock)
    if (distance === 0) return a.data.clock.id < b.data.clock.id ? -1 : 1
    return distance
  }

  /**
   * Check if an entry equals another entry.
   * @param {Entry} a
   * @param {Entry} b
   * @returns {boolean}
   */
  static isEqual(a: Entry, b: Entry) {
    return a.hash === b.hash
  }

  /**
   * Check if an entry is a parent to another entry.
   * @param {Entry} entry1 Entry to check
   * @param {Entry} entry2 The parent Entry
   * @returns {boolean}
   */
  static isParent(entry1: Entry, entry2: Entry) {
    return entry2.next.indexOf(entry1.hash as any) > -1 // TODO fix types
  }

  /**
   * Find entry's children from an Array of entries.
   * Returns entry's children as an Array up to the last know child.
   * @param {Entry} entry Entry for which to find the parents
   * @param {Array<Entry>} values Entries to search parents from
   * @returns {Array<Entry>}
   */
  static findChildren<T>(entry: Entry, values: Entry[]) {
    let stack: Entry[] = []
    let parent = values.find((e) => Entry.isParent(entry, e))
    let prev = entry
    while (parent) {
      stack.push(parent)
      prev = parent
      parent = values.find((e) => Entry.isParent(prev, e))
    }
    stack = stack.sort((a, b) => Clock.compare(a.data.clock, b.data.clock))
    return stack
  }

}
