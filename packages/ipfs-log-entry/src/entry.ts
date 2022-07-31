import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io';
import { IPFS } from 'ipfs-core-types/src/'
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
import { U8IntArraySerializer, U8IntArraySerializerOptional } from '@dao-xyz/borsh-utils';
import { Ed25519PublicKey } from 'sodium-plus';
import { arraysEqual } from './utils';

const uintArrayEqual = (array1?: Uint8Array, array2?: Uint8Array) => {
  if (!!array1 != !!array2)
    return false;
  if (array1.byteLength != array2.byteLength) return false
  return array1.every((val, i) => val == array2[i])
}
export interface CryptOptions {
  encrypt: (data: Uint8Array) => { data: Uint8Array, nonce: Uint8Array, publicKey: Uint8Array },
  decrypt: (data: Uint8Array, publicKey: Uint8Array, nonce: Uint8Array) => Uint8Array
}


export interface IOOptions<T> {
  encoder: (data: T) => Uint8Array
  decoder: (bytes: Uint8Array) => T
}
export const JSON_IO_OPTIONS: IOOptions<any> = {
  encoder: (obj: any) => {
    return new Uint8Array(Buffer.from(JSON.stringify(obj)))
  },
  decoder: (bytes: Uint8Array) => {
    return JSON.parse(Buffer.from(bytes).toString())
  }
}

const IpfsNotDefinedError = () => new Error('Ipfs instance not defined')

/*
 * @description
 * An ipfs-log entry
 */



export interface EntrySerialized<T> {
  data: Uint8Array
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  next?: string[] | Entry<T>[]
  refs?: string[] // Array of hashes
}

@variant(0)
export class EntryDataBox<T> {

  _crypt: CryptOptions
  _io: IOOptions<T>

  init(io: IOOptions<T>, crypt?: CryptOptions) {
    this._crypt = crypt;
    this._io = io;
    return this;
  }

  get id(): string {
    throw new Error("Not implemented")
  }

  get payload(): T {
    throw new Error("Not implemented")
  }

  get payloadEncoded(): Uint8Array {
    throw new Error("Not implemented")
  }

  get clock(): LamportClock {
    throw new Error("Not implemented")
  }
  get identity(): IdentitySerializable | undefined {
    throw new Error("Not implemented")
  }
  get key(): Uint8Array | undefined {
    throw new Error("Not implemented")
  }
  get sig(): Uint8Array | undefined {
    throw new Error("Not implemented")
  }

  clone(signed: boolean = true): EntryDataBox<T> {
    throw new Error("Not implemented")
  }

  equals(other: EntryDataBox<T>): boolean {
    throw new Error("Not implemented")
  }

}

@variant(0)
export class EntryDataDecrypted<T> extends EntryDataBox<T> {

  @field({ type: 'String' })
  _id: string // For determining a unique chain

  @field(U8IntArraySerializer)
  _payload: Uint8Array

  @field({ type: Clock })
  _clock: Clock

  @field({ type: option(IdentitySerializable) })
  _identity?: IdentitySerializable;

  @field(U8IntArraySerializerOptional)
  _key?: Uint8Array;

  @field(U8IntArraySerializerOptional)
  _sig?: Uint8Array;

  constructor(obj?: {
    id: string // For determining a unique chain
    payload: Uint8Array
    clock: Clock
    identity?: IdentitySerializable;
    key?: Uint8Array;
    sig?: Uint8Array;
  }) {
    super();
    if (obj) {
      this._id = obj.id;
      this._payload = obj.payload;
      this._clock = obj.clock;
      this._identity = obj.identity;
      this._key = obj.key;
      this._sig = obj.sig;
    }
  }

  // we do _ on properties and create getters to signal that this is a readonly structure
  get id(): string {
    return this._id;
  }

  get payload(): T {
    return this._io.decoder(this._payload)
  }

  get payloadEncoded(): Uint8Array {
    return this._payload;
  }

  get clock(): LamportClock {
    return this._clock;
  }
  get identity(): IdentitySerializable | undefined {
    return this._identity
  }
  get key(): Uint8Array | undefined {
    return this._key;
  }
  get sig(): Uint8Array | undefined {
    return this._sig;
  }

  static from<T>(arr: Uint8Array): EntryDataDecrypted<T> {
    return deserialize<EntryDataDecrypted<T>>(Buffer.from(arr), EntryDataDecrypted)
  }

  encrypt(): EntryDataEncrypted<T> {
    const bytes = serialize(this)
    const enc = new EntryDataEncrypted<T>(this._crypt.encrypt(Buffer.from(bytes)))
    enc._decrypted = this;
    return enc;
  }

  clone(signed: boolean = true) {
    return new EntryDataDecrypted<T>({
      id: this.id,
      payload: this.payloadEncoded,
      clock: new LamportClock(this.clock.id, this.clock.time),
      identity: signed ? this.identity : undefined,
      key: signed ? this.key : undefined,
      sig: signed ? this.sig : undefined,
    }).init(this._io, this._crypt)
  }

  equals(other: EntryDataBox<T>): boolean {
    if (other instanceof EntryDataDecrypted) {
      return this.id === other.id && uintArrayEqual(this._payload, other._payload) && this.clock.equals(other.clock) && this.identity.equals(other.identity) && arraysEqual(this.sig, other.sig) && arraysEqual(this.key, other.key);
    }
    else {
      return false;
    }
  }

}


@variant(1)
export class EntryDataEncrypted<T> extends EntryDataBox<T> {

  @field(U8IntArraySerializer)
  _data: Uint8Array;

  @field(U8IntArraySerializer)
  _publicKey: Uint8Array

  @field(U8IntArraySerializer)
  _nonce: Uint8Array


  constructor(obj?: {
    data: Uint8Array;
    publicKey: Uint8Array;
    nonce: Uint8Array;
  }) {
    super();
    if (obj) {
      this._data = obj.data;
      this._nonce = obj.nonce;
      this._publicKey = obj.publicKey
    }
  }

  get id(): string {
    return this._decrypt().id;
  }

  get payload(): T {
    return this._decrypt().payload
  }

  get payloadEncoded(): Uint8Array {
    return this._decrypt()._payload;
  }

  get clock(): LamportClock {
    return this._decrypt().clock;
  }
  get identity(): IdentitySerializable | undefined {
    return this._decrypt().identity
  }
  get key(): Uint8Array | undefined {
    return this._decrypt().key;
  }
  get sig(): Uint8Array | undefined {
    return this._decrypt().sig;
  }

  _decrypted: EntryDataDecrypted<T>

  _decrypt(): EntryDataDecrypted<T> {
    if (this._decrypted) {
      return this._decrypted
    }
    let der: EntryDataBox<T> = this;
    let counter = 0;
    while (der instanceof EntryDataEncrypted) {
      der = deserialize<EntryDataBox<T>>(Buffer.from(this._crypt.decrypt(this._data, this._publicKey, this._nonce)), EntryDataBox)
      counter += 1;
      if (counter >= 10) {
        throw new Error("Unexpected decryption behaviour, data seems to always be in encrypted state")
      }
    }
    der.init(this._io, this._crypt,)
    this._decrypted = der as EntryDataDecrypted<T>
    return this._decrypted;
  }

  clone(signed: boolean) {
    const dec = this._decrypt().clone(signed);
    return dec.encrypt().init(this._io, this._crypt,)
  }

  equals(other: EntryDataBox<T>): boolean {
    if (other instanceof EntryDataEncrypted) {
      return uintArrayEqual(this._data, other._data) && uintArrayEqual(this._nonce, other._nonce)
    }
    else {
      return false;
    }
  }
}


@variant(0)
export class Entry<T> {

  @field({ type: EntryDataBox })
  data: EntryDataBox<T>

  @field({ type: option(vec('String')) })
  next?: string[] | Entry<T>[]

  @field({ type: option(vec('String')) })
  refs?: string[] // Array of hashes

  @field({ type: option('String') })
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry

  static IPLD_LINKS = ['next', 'refs']
  static getWriteFormat = () => "dag-cbor" // Only dag-cbor atm


  constructor(obj?: {
    data: EntryDataBox<T>
    next?: string[] | Entry<T>[]
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

  init(props: { io: IOOptions<T>, crypt?: CryptOptions } | Entry<T>): Entry<T> {
    props instanceof Entry ? this.data.init(props.data._io, props.data._crypt) : this.data.init(props.io, props.crypt);
    return this;
  }

  serialize(): EntrySerialized<T> {
    return {
      data: serialize(this.data),
      hash: this.hash,
      next: this.next,
      refs: this.refs
    }
  }

  equals(other: Entry<T>) {
    return other.hash === this.hash && arraysEqual(this.next, other.next) && arraysEqual(this.refs, other.refs) && this.data.equals(other.data)
  }

  /**
   * Create an Entry
   * @param {IPFS} ipfs An IPFS instance
   * @param {Identity} identity The identity instance
   * @param {string} logId The unique identifier for this log
   * @param {*} data Data of the entry to be added. Can be any JSON.stringifyable data
   * @param {Array<string|Entry>} [next=[]] Parent hashes or entries
   * @param {LamportClock} [clock] The lamport clock
   * @returns {Promise<Entry<T>>}
   * @example
   * const entry = await Entry.create(ipfs, identity, 'hello')
   * console.log(entry)
   * // { hash: null, payload: "hello", next: [] }
   */
  static async create<T>(options: { ipfs: IPFS, identity: Identity, logId: string, data: T, next?: (Entry<T> | string)[], ioOptions?: IOOptions<T>, clock?: Clock, refs?: string[], pin?: boolean, assertAllowed?: (entry: Entry<T>) => Promise<void>, cryptOptions?: CryptOptions }) {
    if (!options.ioOptions || !options.refs || !options.next) {
      options = {
        ...options,
        next: options.next ? options.next : [],
        refs: options.refs ? options.refs : [],
        ioOptions: options.ioOptions ? options.ioOptions : JSON_IO_OPTIONS
      }
    }

    if (!isDefined(options.ipfs)) throw IpfsNotDefinedError()
    if (!isDefined(options.identity)) throw new Error('Identity is required, cannot create entry')
    if (!isDefined(options.logId)) throw new Error('Entry requires an id')
    if (!isDefined(options.data)) throw new Error('Entry requires data')
    if (!isDefined(options.next) || !Array.isArray(options.next)) throw new Error("'next' argument is not an array")



    // Clean the next objects and convert to hashes
    const toEntry = (e) => e.hash ? e.hash : e
    const nexts = options.next.filter(isDefined).map(toEntry)
    let entryDataDecrypted = new EntryDataDecrypted<T>({
      id: options.logId, // For determining a unique chain
      payload: options.ioOptions.encoder(options.data), // Can be any JSON.stringifyable data
      clock: options.clock || new Clock(options.identity.publicKey),
    });
    const entry: Entry<T> = new Entry<T>({
      data: entryDataDecrypted,
      hash: null, // "zd...Foo", we'll set the hash after persisting the entry
      next: nexts, // Array of hashes
      refs: options.refs,
    })
    const identitySerializable = options.identity.toSerializable();

    if (options.assertAllowed) {
      entryDataDecrypted._identity = identitySerializable
      await options.assertAllowed(entry);
      entryDataDecrypted._identity = undefined;
    }

    const signature = await options.identity.provider.sign(identitySerializable, Entry.toBuffer(entry))
    entryDataDecrypted._key = options.identity.publicKey
    entryDataDecrypted._identity = identitySerializable
    entryDataDecrypted._sig = signature
    entryDataDecrypted.init(options.ioOptions, options.cryptOptions);
    if (options.cryptOptions) {
      entry.data = entryDataDecrypted.encrypt();
    }

    entry.hash = await Entry.toMultihash(options.ipfs, entry, options.pin)
    return entry
  }

  /**
   * Verifies an entry signature.
   *
   * @param {IdentityProvider} identityProvider The identity provider to use
   * @param {Entry} entry The entry being verified
   * @return {Promise} A promise that resolves to a boolean value indicating if the signature is valid
   */
  static async verify<T>(identityProvider: Identities, entry: Entry<T>) {
    if (!identityProvider) throw new Error('Identity-provider is required, cannot verify entry')
    if (!Entry.isEntry(entry)) throw new Error('Invalid Log entry')
    if (!entry.data.key) throw new Error("Entry doesn't have a key")
    if (!entry.data.sig) throw new Error("Entry doesn't have a signature")

    const e = Entry.toEntryWithoutSignature(entry)
    return identityProvider.verify(entry.data.sig, new Ed25519PublicKey(Buffer.from(entry.data.key)), Entry.toBuffer(e))
  }

  /**
   * Transforms an entry into a Buffer.
   * @param {Entry} entry The entry
   * @return {Buffer} The buffer
   */
  static toBuffer<T>(entry: Entry<T>) {
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
  static async toMultihash<T>(ipfs: IPFS, entry: Entry<T>, pin = false) {
    if (!ipfs) throw IpfsNotDefinedError()
    if (!Entry.isEntry(entry)) throw new Error('Invalid object format, cannot generate entry hash')

    // Ensure `entry` follows the correct format
    const e = Entry.toEntry(entry) // Will remove the hash
    return io.write(ipfs, Entry.getWriteFormat(), e.serialize(), { links: Entry.IPLD_LINKS, pin })
  }

  static toEntry<T>(entry: Entry<T> | EntrySerialized<T>): Entry<T> {
    const e: Entry<T> = new Entry<T>({
      hash: null,
      // TODO improve performance (unecessary cloning)
      data: entry.data instanceof Uint8Array ? deserialize<EntryDataBox<T>>(Buffer.from(entry.data), EntryDataBox) : entry.data.clone(),
      next: entry.next,
      refs: entry.refs
    })
    return e
  }

  static toEntryWithoutSignature<T>(entry: Entry<T> | EntrySerialized<T>): Entry<T> {
    const e: Entry<T> = new Entry<T>({
      hash: undefined,
      // TODO improve performance (unecessary cloning)
      data: entry.data instanceof Uint8Array ? EntryDataDecrypted.from<T>(entry.data).clone(false) : entry.data.clone(false),
      next: entry.next,
      refs: entry.refs
    })
    return e
  }



  /**
   * Create an Entry from a hash.
   * @param {IPFS} ipfs An IPFS instance
   * @param {string} hash The hash to create an Entry from
   * @returns {Promise<Entry<T>>}
   * @example
   * const entry = await Entry.fromMultihash(ipfs, "zd...Foo")
   * console.log(entry)
   * // { hash: "Zd...Foo", payload: "hello", next: [] }
   */
  static async fromMultihash<T>(ipfs, hash) {
    if (!ipfs) throw IpfsNotDefinedError()
    if (!hash) throw new Error(`Invalid hash: ${hash}`)
    const e: EntrySerialized<T> = await io.read(ipfs, hash, { links: Entry.IPLD_LINKS })

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
      obj.data._payload !== undefined &&
      obj.hash !== undefined &&
      obj.data.clock !== undefined && obj.refs !== undefined
  }

  /**
   * Compares two entries.
   * @param {Entry} a
   * @param {Entry} b
   * @returns {number} 1 if a is greater, -1 is b is greater
   */
  static compare<T>(a: Entry<T>, b: Entry<T>) {
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
  static isEqual<T>(a: Entry<T>, b: Entry<T>) {
    return a.hash === b.hash
  }

  /**
   * Check if an entry is a parent to another entry.
   * @param {Entry} entry1 Entry to check
   * @param {Entry} entry2 The parent Entry
   * @returns {boolean}
   */
  static isParent<T>(entry1: Entry<T>, entry2: Entry<T>) {
    return entry2.next.indexOf(entry1.hash as any) > -1 // TODO fix types
  }

  /**
   * Find entry's children from an Array of entries.
   * Returns entry's children as an Array up to the last know child.
   * @param {Entry} entry Entry for which to find the parents
   * @param {Array<Entry<T>>} values Entries to search parents from
   * @returns {Array<Entry<T>>}
   */
  static findChildren<T>(entry: Entry<T>, values: Entry<T>[]) {
    let stack: Entry<T>[] = []
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
