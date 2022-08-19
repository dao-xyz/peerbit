import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize, Constructor } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io';
import { IPFS } from 'ipfs-core-types/src/'
import { Identities, Identity, IdentitySerializable } from '@dao-xyz/orbit-db-identity-provider'
import { arraysEqual, joinUint8Arrays } from '@dao-xyz/io-utils';
import { Ed25519PublicKey, X25519PublicKey } from 'sodium-plus';
import { PublicKeyEncryption, DecryptedThing, EncryptedThing, Encryption, MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { Metadata, MetadataSecure } from './metadata';

export interface IOOptions<T> {
  encoder: (data: T) => Uint8Array
  decoder: (bytes: Uint8Array) => T
}
export const JSON_ENCODING_OPTIONS: IOOptions<any> = {
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
  payload: Uint8Array
  metadata: Uint8Array
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  next?: string[]
  refs?: string[] // Array of hashes
}

@variant(0)
export class Payload<T>
{
  _encryption: PublicKeyEncryption
  _encoding: IOOptions<T>

  @field({ type: MaybeEncrypted })
  _data: MaybeEncrypted<T>

  constructor(props?: {
    data: MaybeEncrypted<T>
  }) {
    if (props) {
      this._data = props.data;
    }
  }

  init(encoding: IOOptions<T>, encryption?: PublicKeyEncryption) {
    this._encryption = encryption;
    this._encoding = encoding;
    this._data.init(this._encryption);
    return this;
  }

  equals(other: Payload<T>): boolean {
    return this._data.equals(other._data);
  }

  /**
   * In place
   * @param recieverPublicKey 
   * @returns this
   */
  async encrypt(recieverPublicKey: X25519PublicKey): Promise<Payload<T>> {
    if (this._data instanceof DecryptedThing) {
      this._data = await this._data.encrypt(recieverPublicKey)
      return this;
    }
    else if (this._data instanceof EncryptedThing) {
      throw new Error("Already encrypted")
    }
    throw new Error("Unsupported")
  }

  /**
   * In place
   * @returns this
   */
  async decrypt(): Promise<DecryptedThing<T>> {
    if (this._data instanceof EncryptedThing) {
      await this._data.decrypt()
      return this._data.decrypted;
    }
    else if (this._data instanceof DecryptedThing) {
      return this._data;
    }
    throw new Error("Unsupported")
  }

  _value: T
  get value(): T {
    if (this._value)
      return this._value;
    let decrypted: Uint8Array = undefined;
    if (this._data instanceof DecryptedThing) {
      decrypted = this._data._data;
    }
    else if (this._data instanceof EncryptedThing) {
      decrypted = this._data.decrypted._data;
    }
    const decoded = this._encoding.decoder(decrypted)
    this._value = decoded;
    return this.value;
  }
}


/* @variant(0)
export class EntryDataBox<T> {

  


  get payload(): T {
    throw new Error("Not implemented")
  }

  get payloadEncoded(): Uint8Array {
    throw new Error("Not implemented")
  }


  clone(signed: boolean = true): EntryDataBox<T> {
    throw new Error("Not implemented")
  }

  equals(other: EntryDataBox<T>): boolean {
    throw new Error("Not implemented")
  }

} */

/* @variant(0)
export class EntryDataDecrypted<T> extends EntryDataBox<T> {


  @field(U8IntArraySerializer)
  _payload: Uint8Array



  constructor(obj?: {
    payload: Uint8Array
  }) {
    super();
    if (obj) {
      this._payload = obj.payload;
    }
  }


  get payload(): T {
    return this._encoding.decoder(this._payload)
  }

  get payloadEncoded(): Uint8Array {
    return this._payload;
  }



  static from<T>(arr: Uint8Array): EntryDataDecrypted<T> {
    return deserialize<EntryDataDecrypted<T>>(Buffer.from(arr), EntryDataDecrypted)
  }

  async encrypt(recieverPublicKey: X25519PublicKey): Promise<EntryDataBoxEncrypted<T>> {
    const bytes = serialize(this)
    const { data, senderPublicKey } = await this._encryption.encrypt(Buffer.from(bytes), recieverPublicKey);
    const enc = new EntryDataBoxEncrypted<T>({ data, senderPublicKey, recieverPublicKey })
    enc._decrypted = this;
    return enc;
  }


  clone() {
    return new EntryDataDecrypted<T>({
      payload: this.payloadEncoded,
    }).init(this._encoding, this._encryption)
  }

  equals(other: EntryDataBox<T>): boolean {
    if (other instanceof EntryDataDecrypted) {
      return arraysEqual(this._payload, other._payload)
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

  get payload(): T {
    return this.decrypted.payload
  }

  get payloadEncoded(): Uint8Array {
    return this.decrypted._payload;
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
      der = deserialize<EntryDataBox<T>>(Buffer.from(await this._encryption.decrypt(this._data, this._senderPublicKey, this._recieverPublicKey)), EntryDataBox)
      counter += 1;
      if (counter >= 10) {
        throw new Error("Unexpected decryption behaviour, data seems to always be in encrypted state")
      }
    }
    der.init(this._encoding, this._encryption,)
    this._decrypted = der as EntryDataDecrypted<T>
    return this._decrypted;
  }


  clone(): EntryDataBoxEncrypted<T> {

    // TODO the only reasons we do this (below) is because the clone method has the signed argument, perhaps find a better solution where the clone does not need to have the signed argument
    const cloned = new EntryDataBoxEncrypted<T>({
      data: new Uint8Array(this._data),
      recieverPublicKey: new X25519PublicKey(this._recieverPublicKey.getBuffer()),
      senderPublicKey: new X25519PublicKey(this._senderPublicKey.getBuffer())
    }).init(this._encoding, this._encryption)

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
} */


@variant(0)
export class Entry<T> {

  @field({ type: Payload })
  payload: Payload<T>

  @field({ type: MetadataSecure })
  metadata: MetadataSecure

  @field({ type: option(vec('String')) })
  next?: string[]

  @field({ type: option(vec('String')) })
  refs?: string[] // Array of hashes

  @field({ type: option('String') })
  hash?: string // "zd...Foo", we'll set the hash after persisting the entry

  static IPLD_LINKS = ['next', 'refs']
  static getWriteFormat = () => "dag-cbor" // Only dag-cbor atm


  constructor(obj?: {
    payload: Payload<T>
    metadata: MetadataSecure
    next?: string[]
    refs?: string[] // Array of hashes
    hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  }) {
    if (obj) {
      this.payload = obj.payload;
      this.metadata = obj.metadata;
      this.next = obj.next;
      this.refs = obj.refs;
      this.hash = obj.hash;
    }
  }

  init(props: { encoding: IOOptions<T>, encryption?: PublicKeyEncryption } | Entry<T>): Entry<T> {
    const encoding = props instanceof Entry ? props.payload._encoding : props.encoding;
    const encryption = props instanceof Entry ? props.payload._encryption : props.encryption;
    this.payload.init(encoding, encryption);
    this.metadata.init(encryption);
    return this;
  }

  serialize(): EntrySerialized<T> {
    return {
      payload: serialize(this.payload),
      metadata: serialize(this.metadata),
      hash: this.hash,
      next: this.next,
      refs: this.refs
    }
  }

  static createDataToSign(id: string, payload: Payload<any>, clock: LamportClock, next?: (any | string)[], refs?: string[]): Buffer { // TODO fix types
    const arrays: Uint8Array[] = [new Uint8Array(Buffer.from(id)), serialize(payload), serialize(clock)];
    if (next) {
      next.forEach((n) => {
        arrays.push(new Uint8Array(Buffer.from(n)));
      })
    }
    if (refs) {
      refs.forEach((r) => {
        arrays.push(new Uint8Array(Buffer.from(r)));
      })
    }
    return Buffer.from(joinUint8Arrays(arrays));
  }

  async createDataToSign(): Promise<Buffer> {
    return Entry.createDataToSign(await this.metadata.id, this.payload, await this.metadata.clock, this.next, this.refs)
  }


  equals(other: Entry<T>) {
    return other.hash === this.hash && this.metadata.equals(other.metadata) && arraysEqual(this.next, other.next) && arraysEqual(this.refs, other.refs) && this.payload.equals(other.payload)
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
  static async create<T>(options: { ipfs: IPFS, identity: Identity, logId: string, data: T, next?: (Entry<T> | string)[], encodingOptions?: IOOptions<T>, clock?: Clock, refs?: string[], pin?: boolean, assertAllowed?: (entryData: Payload<T>, identity: IdentitySerializable) => Promise<void>, encryption?: Encryption }) {
    if (!options.encodingOptions || !options.refs || !options.next) {
      options = {
        ...options,
        next: options.next ? options.next : [],
        refs: options.refs ? options.refs : [],
        encodingOptions: options.encodingOptions ? options.encodingOptions : JSON_ENCODING_OPTIONS
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
    const decryptedData = new DecryptedThing<T>({
      data: options.encodingOptions.encoder(options.data), // Can be any JSON.stringifyable data
    });
    let payload = new Payload({
      data: decryptedData
    });

    const id = options.logId; // For determining a unique chain
    const identitySerializable = options.identity.toSerializable();
    const clock = options.clock || new Clock(options.identity.publicKey);


    const entry: Entry<T> = new Entry<T>({
      payload,
      metadata: null, // TODO pass identity with signature without signature?
      hash: null, // "zd...Foo", we'll set the hash after persisting the entry
      next: nexts, // Array of hashes
      refs: options.refs,
    })

    if (options.assertAllowed) {
      await options.assertAllowed(payload, identitySerializable);
    }
    entry.next?.forEach((next) => {
      if (typeof next !== 'string') {
        throw new Error("Unsupported next type")
      }
    })

    // We are encrypting the payload before signing it
    // This is important because we want to verify the signature without decrypting the payload
    payload.init(options.encodingOptions, options.encryption?.options);
    if (options.encryption) {
      entry.payload = await new Payload({
        data: decryptedData
      }).init(options.encodingOptions, options.encryption.options).encrypt(options.encryption.recieverPayload);
    }

    // Sign id, encrypted payload, clock, nexts, refs 
    const signature = await options.identity.provider.sign(Entry.createDataToSign(id, entry.payload, clock, entry.next, entry.refs), identitySerializable)

    // Create encrypted metadata with data, and encrypt it
    entry.metadata = new MetadataSecure({
      metadata: new DecryptedThing({
        data: serialize(new Metadata({
          id,
          identity: identitySerializable,
          signature,
          clock
        }))
      })
    })

    entry.metadata.init(options.encryption?.options);
    if (options.encryption) {
      await entry.metadata.encrypt(options.encryption.recieverPayload);
    }


    // Append hash
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
    const key = await (await entry.metadata.identity).publicKey;
    if (!key) throw new Error("Entry doesn't have a key")
    const signature = await entry.metadata.signature;
    if (!signature) throw new Error("Entry doesn't have a signature")
    const verified = identityProvider.verify(signature, new Ed25519PublicKey(Buffer.from(key)), await entry.createDataToSign())
    return verified;
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
   * const multfihash = await Entry.toMultihash(ipfs, entry)
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
      payload: entry.payload instanceof Uint8Array ? deserialize<Payload<T>>(Buffer.from(entry.payload), Payload) : entry.payload,
      metadata: entry.metadata instanceof Uint8Array ? deserialize(Buffer.from(entry.metadata), MetadataSecure) : entry.metadata,
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
    if (!obj.payload) {
      return false
    }
    return obj &&
      obj.next !== undefined && obj.hash !== undefined && obj.refs !== undefined && obj.metadata !== undefined
  }

  /**
   * Compares two entries.
   * @param {Entry} a
   * @param {Entry} b
   * @returns {number} 1 if a is greater, -1 is b is greater
   */
  static compare<T>(a: Entry<T>, b: Entry<T>) {
    const aClock = a.metadata.clockDecrypted;
    const bClock = b.metadata.clockDecrypted;
    const distance = Clock.compare(aClock, bClock)
    if (distance === 0) return aClock.id < bClock.id ? -1 : 1
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
    stack = stack.sort((a, b) => Clock.compare(a.metadata.clockDecrypted, b.metadata.clockDecrypted))
    return stack
  }

}
