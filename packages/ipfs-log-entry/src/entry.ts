import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io';
import { IPFS } from 'ipfs-core-types/src/'
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
import { arraysEqual, joinUint8Arrays, U8IntArraySerializer } from '@dao-xyz/io-utils';
import { X25519PublicKey, Ed25519PublicKey } from 'sodium-plus';
import { CryptOptions, DecryptedThing, Encryption, X25519PublicKeySerializer } from './encryption';
import { IdentityWithSignature, IdentityWithSignatureSecure } from './identity';


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
  identityWithSignature: Uint8Array
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

  constructor(obj?: {
    id: string // For determining a unique chain
    payload: Uint8Array
    clock: Clock
  }) {
    super();
    if (obj) {
      this._id = obj.id;
      this._payload = obj.payload;
      this._clock = obj.clock;
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


  static from<T>(arr: Uint8Array): EntryDataDecrypted<T> {
    return deserialize<EntryDataDecrypted<T>>(Buffer.from(arr), EntryDataDecrypted)
  }

  async encrypt(recieverPublicKey: X25519PublicKey): Promise<EntryDataBoxEncrypted<T>> {
    const bytes = serialize(this)
    const { data, senderPublicKey } = await this._crypt.encrypt(Buffer.from(bytes), recieverPublicKey);
    const enc = new EntryDataBoxEncrypted<T>({ data, senderPublicKey, recieverPublicKey })
    enc._decrypted = this;
    return enc;
  }

  /* clone(signed: boolean = true) {
    return new EntryDataDecrypted<T>({
      id: this.id,
      payload: this.payloadEncoded,
      clock: new LamportClock(this.clock.id, this.clock.time),
      identity: signed ? this.identity : undefined,
      key: signed ? this.key : undefined,
      sig: signed ? this.sig : undefined,
    }).init(this._io, this._crypt)
  } */

  clone() {
    return new EntryDataDecrypted<T>({
      id: this.id,
      payload: this.payloadEncoded,
      clock: new LamportClock(this.clock.id, this.clock.time)
    }).init(this._io, this._crypt)
  }

  equals(other: EntryDataBox<T>): boolean {
    if (other instanceof EntryDataDecrypted) {
      return this.id === other.id && arraysEqual(this._payload, other._payload) && this.clock.equals(other.clock)
    }
    else {
      return false;
    }
  }

}


@variant(1)
export class EntryDataBoxEncrypted<T> extends EntryDataBox<T> {

  @field(U8IntArraySerializer)
  _data: Uint8Array;

  @field(X25519PublicKeySerializer)
  _senderPublicKey: X25519PublicKey

  @field(X25519PublicKeySerializer)
  _recieverPublicKey: X25519PublicKey

  constructor(obj?: {
    data: Uint8Array;
    senderPublicKey: X25519PublicKey;
    recieverPublicKey: X25519PublicKey;

  }) {
    super();
    if (obj) {
      this._data = obj.data;
      this._senderPublicKey = obj.senderPublicKey;
      this._recieverPublicKey = obj.recieverPublicKey
    }
  }

  get id(): string {
    return this.decrypted.id;
  }

  get payload(): T {
    return this.decrypted.payload
  }

  get payloadEncoded(): Uint8Array {
    return this.decrypted._payload;
  }

  get clock(): LamportClock {
    return this.decrypted.clock;
  }


  _decrypted: EntryDataDecrypted<T>
  get decrypted(): EntryDataDecrypted<T> {
    if (!this._decrypted) {
      throw new Error("Entry has not been decrypted, invoke decrypt method before")
    }
    return this._decrypted;
  }

  async decrypt(): Promise<EntryDataDecrypted<T>> {
    if (this._decrypted) {
      return this._decrypted
    }
    let der: EntryDataBox<T> = this;
    let counter = 0;
    while (der instanceof EntryDataBoxEncrypted) {
      der = deserialize<EntryDataBox<T>>(Buffer.from(await this._crypt.decrypt(this._data, this._senderPublicKey, this._recieverPublicKey)), EntryDataBox)
      counter += 1;
      if (counter >= 10) {
        throw new Error("Unexpected decryption behaviour, data seems to always be in encrypted state")
      }
    }
    der.init(this._io, this._crypt,)
    this._decrypted = der as EntryDataDecrypted<T>
    return this._decrypted;
  }

  /* clone(signed: boolean): EntryDataBoxEncrypted<T> {

    // TODO the only reasons we do this (below) is because the clone method has the signed argument, perhaps find a better solution where the clone does not need to have the signed argument
    const dec = this.decrypted;
    if (!!dec._sig != signed) {
      if (signed)
        throw new Error("Can not clone and encrypted entry and sign at the same time")
      else if (!signed) {
        throw new Error("Can not clone and encrypted entry removed signed data at the same time")
      }
    }

    const cloned = new EntryDataBoxEncrypted<T>({
      data: new Uint8Array(this._data),
      recieverPublicKey: new X25519PublicKey(this._recieverPublicKey.getBuffer()),
      senderPublicKey: new X25519PublicKey(this._senderPublicKey.getBuffer())
    }).init(this._io, this._crypt)

    return cloned
  } */

  clone(): EntryDataBoxEncrypted<T> {

    // TODO the only reasons we do this (below) is because the clone method has the signed argument, perhaps find a better solution where the clone does not need to have the signed argument
    const cloned = new EntryDataBoxEncrypted<T>({
      data: new Uint8Array(this._data),
      recieverPublicKey: new X25519PublicKey(this._recieverPublicKey.getBuffer()),
      senderPublicKey: new X25519PublicKey(this._senderPublicKey.getBuffer())
    }).init(this._io, this._crypt)

    return cloned
  }

  equals(other: EntryDataBox<T>): boolean {
    if (other instanceof EntryDataBoxEncrypted) {
      return arraysEqual(this._data, other._data) && Buffer.compare(this._senderPublicKey.getBuffer(), other._senderPublicKey.getBuffer()) === 0 && Buffer.compare(this._recieverPublicKey.getBuffer(), other._recieverPublicKey.getBuffer()) === 0
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

  @field({ type: IdentityWithSignatureSecure })
  identityWithSignature: IdentityWithSignatureSecure

  @field({ type: option(vec('String')) })
  next?: string[] | Entry<T>[] // TODO fix types

  @field({ type: option(vec('String')) })
  refs?: string[] // Array of hashes

  @field({ type: option('String') })
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry

  static IPLD_LINKS = ['next', 'refs']
  static getWriteFormat = () => "dag-cbor" // Only dag-cbor atm


  constructor(obj?: {
    data: EntryDataBox<T>
    identityWithSignature: IdentityWithSignatureSecure
    next?: string[] | Entry<T>[]
    refs?: string[] // Array of hashes
    hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  }) {
    if (obj) {
      this.data = obj.data;
      this.identityWithSignature = obj.identityWithSignature;
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
      identityWithSignature: serialize(this.identityWithSignature),
      hash: this.hash,
      next: this.next,
      refs: this.refs
    }
  }

  get dataToSign(): Buffer {
    const arrays: Uint8Array[] = [serialize(this.data)];
    if (this.next) {
      this.next.forEach((n) => {
        arrays.push(new Uint8Array(Buffer.from(n)));
      })
    }
    if (this.refs) {
      this.refs.forEach((r) => {
        arrays.push(new Uint8Array(Buffer.from(r)));
      })
    }
    return Buffer.from(joinUint8Arrays(arrays));
  }
  equals(other: Entry<T>) {
    return other.hash === this.hash && this.identityWithSignature.equals(other.identityWithSignature) && arraysEqual(this.next, other.next) && arraysEqual(this.refs, other.refs) && this.data.equals(other.data)
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
  static async create<T>(options: { ipfs: IPFS, identity: Identity, logId: string, data: T, next?: (Entry<T> | string)[], ioOptions?: IOOptions<T>, clock?: Clock, refs?: string[], pin?: boolean, assertAllowed?: (entryData: EntryDataBox<T>, identity: IdentitySerializable) => Promise<void>, encryption?: Encryption }) {
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

    const identitySerializable = options.identity.toSerializable();


    const entry: Entry<T> = new Entry<T>({
      data: entryDataDecrypted,
      identityWithSignature: null, // TODO pass identity with signature without signature?
      hash: null, // "zd...Foo", we'll set the hash after persisting the entry
      next: nexts, // Array of hashes
      refs: options.refs,
    })

    if (options.assertAllowed) {
      await options.assertAllowed(entryDataDecrypted, identitySerializable);
    }
    entry.next?.forEach((next) => {
      if (typeof next !== 'string') {
        throw new Error("Unsupported next type")
      }
    })
    const signature = await options.identity.provider.sign(entry.dataToSign, identitySerializable)

    entry.identityWithSignature = new IdentityWithSignatureSecure({
      identityWithSignature: new DecryptedThing({
        data: serialize(new IdentityWithSignature({
          identity: identitySerializable,
          signature
        }))
      })
    })
    entryDataDecrypted.init(options.ioOptions, options.encryption?.options);
    entry.identityWithSignature.init(options.encryption?.options);
    if (options.encryption) {
      entry.data = await entryDataDecrypted.encrypt(options.encryption.recieverPayload);
      await entry.identityWithSignature.encrypt(options.encryption.recieverIdentity);
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
    const key = await (await entry.identityWithSignature.identity).publicKey;
    if (!key) throw new Error("Entry doesn't have a key")
    const signature = await entry.identityWithSignature.signature;
    if (!signature) throw new Error("Entry doesn't have a signature")
    return identityProvider.verify(signature, new Ed25519PublicKey(Buffer.from(key)), entry.dataToSign)
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
    const e = Entry.toEntryNoHash(entry) // Will remove the hash
    return io.write(ipfs, Entry.getWriteFormat(), e.serialize(), { links: Entry.IPLD_LINKS, pin })
  }

  /**
   * TODO can cause sideffects because .data is not cloned if entry instance of Entry
   * @param entry 
   * @returns 
   */
  static toEntryNoHash<T>(entry: Entry<T> | EntrySerialized<T>): Entry<T> {
    const e: Entry<T> = new Entry<T>({
      hash: null,
      // TODO improve performance (unecessary cloning)
      data: entry.data instanceof Uint8Array ? deserialize<EntryDataBox<T>>(Buffer.from(entry.data), EntryDataBox) : entry.data,
      identityWithSignature: entry.identityWithSignature instanceof Uint8Array ? deserialize(Buffer.from(entry.identityWithSignature), IdentityWithSignatureSecure) : entry.identityWithSignature,
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
    const entry = Entry.toEntryNoHash(e)
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
      obj.next !== undefined && obj.hash !== undefined && obj.refs !== undefined
      &&
      obj.data.clock !== undefined //  && obj.data._payload !== undefined
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
