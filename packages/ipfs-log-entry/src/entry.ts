import { LamportClock as Clock, LamportClock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize } from '@dao-xyz/borsh';
import io from '@dao-xyz/orbit-db-io';
import { IPFS } from 'ipfs-core-types/src/'
import { arraysEqual, joinUint8Arrays, U8IntArraySerializer } from '@dao-xyz/io-utils';
import { X25519PublicKey } from 'sodium-plus';
import { PublicKeyEncryption, DecryptedThing, MaybeEncrypted } from '@dao-xyz/encryption-utils';
import { Id } from './id';
import { Signature } from './signature';
import { PublicKey } from '@dao-xyz/identity';

export type MaybeX25519PublicKey = (X25519PublicKey | X25519PublicKey[] | undefined);
export type EncryptionTemplateMaybeEncrypted = EntryEncryptionTemplate<MaybeX25519PublicKey, MaybeX25519PublicKey, MaybeX25519PublicKey, MaybeX25519PublicKey, MaybeX25519PublicKey>;
export interface EntryEncryption {
  reciever: EncryptionTemplateMaybeEncrypted,
  options: PublicKeyEncryption
}

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


export interface EntrySerialized<T> {
  id: Uint8Array,
  payload: Uint8Array,
  publicKey: Uint8Array,
  signature: Uint8Array,
  clock: Uint8Array,
  next?: string[]
  refs?: string[] // Array of hashes
}

@variant(0)
export class Payload<T>
{

  _encoding: IOOptions<T>

  @field(U8IntArraySerializer)
  _data: Uint8Array

  constructor(props?: {
    data: Uint8Array
    value?: T
  }) {
    if (props) {
      this._data = props.data;
      this._value = props.value;
    }
  }

  init(encoding: IOOptions<T>) {
    this._encoding = encoding;
    return this;
  }

  equals(other: Payload<T>): boolean {
    return Buffer.compare(Buffer.from(this._data), Buffer.from(other._data)) === 0;
  }

  _value: T
  get value(): T {
    if (this._value)
      return this._value;
    const decoded = this._encoding.decoder(this._data)
    this._value = decoded;
    return this._value;
  }
}

export interface EntryEncryptionTemplate<A, B, C, D, E> {
  id: A,
  clock: B
  payload: C,
  publicKey: D
  signature: E
}

@variant(0)
export class Entry<T> implements EntryEncryptionTemplate<string, Clock, Payload<T>, PublicKey, Signature> {

  @field({ type: MaybeEncrypted })
  _id: MaybeEncrypted<Id>

  @field({ type: MaybeEncrypted })
  _clock: MaybeEncrypted<Clock>

  @field({ type: MaybeEncrypted })
  _payload: MaybeEncrypted<Payload<T>>

  @field({ type: MaybeEncrypted })
  _publicKey: MaybeEncrypted<PublicKey>

  @field({ type: MaybeEncrypted })
  _signature: MaybeEncrypted<Signature>

  @field({ type: vec('string') })
  next: string[] // Array of hashes (the tree)

  @field({ type: vec('string') })
  refs: string[] // Array of hashes (jumps in the tree, indicating dependencies or used for jumping for faster iteration or fail safe behaviour if gaps occur)

  @field({ type: 'u8' })
  _state: number = 0; // reserved for states

  @field({ type: 'u8' })
  _reserved: number = 0; // reserved for future changes

  @field({ type: 'string' })
  hash: string // "zd...Foo", we'll set the hash after persisting the entry

  static IPLD_LINKS = ['next', 'refs']

  _encoding: IOOptions<T>
  _encryption?: PublicKeyEncryption

  peers: string[]; // peers hosting/owning this data

  constructor(obj?: {
    id: MaybeEncrypted<Id>,
    payload: MaybeEncrypted<Payload<T>>
    publicKey: MaybeEncrypted<PublicKey>,
    signature: MaybeEncrypted<Signature>,
    clock: MaybeEncrypted<Clock>;
    next: string[]
    refs: string[] // Array of hashes
    hash?: string // "zd...Foo", we'll set the hash after persisting the entry
  }) {
    if (obj) {
      this._id = obj.id;
      this._clock = obj.clock
      this._payload = obj.payload;
      this._signature = obj.signature;
      this._publicKey = obj.publicKey;
      this.next = obj.next;
      this.refs = obj.refs;
      this.hash = obj.hash;
    }
  }

  init(props: { encoding: IOOptions<T>, encryption?: PublicKeyEncryption } | Entry<T>): Entry<T> {
    const encryption = props instanceof Entry ? props._encryption : props.encryption;
    const encoding = props instanceof Entry ? props._encoding : props.encoding;
    this._encryption = encryption;
    this._encoding = encoding;
    this._payload.init(encryption);
    this._id.init(encryption);
    this._clock.init(encryption);
    this._signature.init(encryption);

    return this;
  }

  serialize(): EntrySerialized<T> {
    return {
      id: serialize(this._id),
      payload: serialize(this._payload),
      publicKey: serialize(this._publicKey),
      signature: serialize(this._signature),
      clock: serialize(this._clock),
      next: this.next,
      refs: this.refs
    }
  }

  get id(): string {
    return this._id.decrypted.getValue(Id).id
  }

  async getId(): Promise<string> {
    await this._id.decrypt();
    return this.id;
  }

  get clock(): Clock {
    return this._clock.decrypted.getValue(Clock)
  }

  async getClock(): Promise<Clock> {
    await this._clock.decrypt()
    return this.clock;
  }

  get payload(): Payload<T> {
    const payload = this._payload.decrypted.getValue(Payload)
    payload.init(this._encoding);
    return payload;
  }

  async getPayload(): Promise<Payload<T>> {
    await this._payload.decrypt()
    return this.payload;
  }

  get publicKey(): PublicKey {
    return this._publicKey.decrypted.getValue(PublicKey)
  }

  async getPublicKey(): Promise<PublicKey> {
    await this._publicKey.decrypt()
    return this.publicKey;
  }


  get signature(): Signature {
    return this._signature.decrypted.getValue(Signature)
  }

  async getSignature(): Promise<Signature> {
    await this._signature.decrypt()
    return this.signature;
  }



  static createDataToSign(id: MaybeEncrypted<Id>, payload: MaybeEncrypted<Payload<any>>, clock: MaybeEncrypted<Clock>, next?: (any | string)[], refs?: string[]): Buffer { // TODO fix types
    const arrays: Uint8Array[] = [serialize(id), serialize(payload), serialize(clock)];
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
    return Entry.createDataToSign(this._id, this._payload, this._clock, this.next, this.refs)
  }


  equals(other: Entry<T>) {
    return this._id.equals(other._id) && this._clock.equals(other._clock) && this._signature.equals(other._signature) && arraysEqual(this.next, other.next) && arraysEqual(this.refs, other.refs) && this._payload.equals(other._payload) // dont compare hashes because the hash is a function of the other properties
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
  static async create<T>(properties: { ipfs: IPFS, logId: string, data: T, encodingOptions?: IOOptions<T>, clock?: Clock, next?: (Entry<T> | string)[], refs?: string[], pin?: boolean, assertAllowed?: (entryData: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<PublicKey>) => Promise<void>, encryption?: EntryEncryption, publicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array> }) {
    if (!properties.encodingOptions || !properties.refs || !properties.next) {
      properties = {
        ...properties,
        next: properties.next ? properties.next : [],
        refs: properties.refs ? properties.refs : [],
        encodingOptions: properties.encodingOptions ? properties.encodingOptions : JSON_ENCODING_OPTIONS
      }
    }

    if (!isDefined(properties.ipfs)) throw IpfsNotDefinedError()
    if (!isDefined(properties.logId)) throw new Error('Entry requires an id')
    if (!isDefined(properties.data)) throw new Error('Entry requires data')
    if (!isDefined(properties.next) || !Array.isArray(properties.next)) throw new Error("'next' argument is not an array")



    // Clean the next objects and convert to hashes
    const toEntry = (e) => e.hash ? e.hash : e
    const nexts = properties.next.filter(isDefined).map(toEntry)

    let payloadToSave = new Payload<T>({
      data: properties.encodingOptions.encoder(properties.data),
      value: properties.data
    });


    const maybeEncrypt = async<Q>(thing: Q, reciever: X25519PublicKey | X25519PublicKey[] | undefined): Promise<MaybeEncrypted<Q>> => {
      const recievers = reciever ? (Array.isArray(reciever) ? reciever : [reciever]) : undefined
      return recievers?.length > 0 ? await new DecryptedThing<Q>({ data: serialize(thing), value: thing }).init(properties.encryption.options).encrypt(...recievers) : new DecryptedThing<Q>({
        data: serialize(thing),
        value: thing
      })
    }


    if (properties.assertAllowed) {
      await properties.assertAllowed(new DecryptedThing({ value: payloadToSave }), new DecryptedThing({
        value: properties.publicKey
      })); // TODO fix types
    }

    const clock = await maybeEncrypt(properties.clock || new Clock(serialize(properties.publicKey)), properties.encryption?.reciever.clock);
    const publicKey = await maybeEncrypt(properties.publicKey, properties.encryption?.reciever.publicKey);
    const id = await maybeEncrypt(new Id({
      id: properties.logId
    }), properties.encryption?.reciever.id);
    const payload = await maybeEncrypt(payloadToSave, properties.encryption?.reciever.payload);

    const entry: Entry<T> = new Entry<T>({
      payload,
      clock,
      id,
      publicKey,
      signature: null,
      hash: null, // "zd...Foo", we'll set the hash after persisting the entry
      next: nexts, // Array of hashes
      refs: properties.refs,
    })


    entry.next?.forEach((next) => {
      if (typeof next !== 'string') {
        throw new Error("Unsupported next type")
      }
    })

    // Sign id, encrypted payload, clock, nexts, refs 
    const signature = await properties.sign(Entry.createDataToSign(id, payload, clock, entry.next, entry.refs))

    // Append hash and signature
    entry._signature = await maybeEncrypt(new Signature({
      signature
    }), properties.encryption?.reciever.signature)
    entry.hash = await Entry.toMultihash(properties.ipfs, entry, properties.pin)
    entry.init({ encoding: properties.encodingOptions, encryption: properties.encryption?.options });
    return entry
  }

  /**
   * Verifies an entry signature.
   *
   * @param {IdentityProvider} identityProvider The identity provider to use
   * @param {Entry} entry The entry being verified
   * @return {Promise} A promise that resolves to a boolean value indicating if the signature is valid
   */
  /*   static async verify<T>(identityProvider: Identities, entry: Entry<T>) {
      if (!identityProvider) throw new Error('Identity-provider is required, cannot verify entry')
      if (!Entry.isEntry(entry)) throw new Error('Invalid Log entry')
      const key = (await entry.getIdentity()).publicKey;
      if (!key) throw new Error("Entry doesn't have a key")
      const signature = (await entry.getSignature()).signature;
      if (!signature) throw new Error("Entry doesn't have a signature")
      const verified = identityProvider.verify(signature, key, await entry.createDataToSign())
      return verified;
    } */


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

    return io.write(ipfs, 'dag-cbor', entry.serialize(), { links: Entry.IPLD_LINKS, pin })
  }

  /**
   * TODO can cause sideffects because .data is not cloned if entry instance of Entry
   * @param entry 
   * @returns 
   */
  static toEntryNoHash<T>(entry: Entry<T> | EntrySerialized<T>): Entry<T> {
    let clock: MaybeEncrypted<Clock> = undefined;
    let payload: MaybeEncrypted<Payload<T>> = undefined;
    let signature: MaybeEncrypted<Signature> = undefined;
    let publicKey: MaybeEncrypted<PublicKey> = undefined;
    let id: MaybeEncrypted<Id> = undefined;
    if (entry instanceof Entry) {
      clock = entry._clock;
      payload = entry._payload;
      publicKey = entry._publicKey;
      signature = entry._signature;
      id = entry._id
    }
    else {
      clock = deserialize<MaybeEncrypted<Clock>>(Buffer.from(entry.clock), MaybeEncrypted);
      payload = deserialize<MaybeEncrypted<Payload<T>>>(Buffer.from(entry.payload), MaybeEncrypted);
      signature = deserialize<MaybeEncrypted<Signature>>(Buffer.from(entry.signature), MaybeEncrypted);
      publicKey = deserialize<MaybeEncrypted<PublicKey>>(Buffer.from(entry.publicKey), MaybeEncrypted);
      id = deserialize<MaybeEncrypted<Id>>(Buffer.from(entry.id), MaybeEncrypted);
    }
    const e: Entry<T> = new Entry<T>({
      hash: null,
      clock,
      payload,
      signature,
      publicKey,
      id,
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
    if (!obj._payload) {
      return false
    }

    if (!obj._id) {
      return false
    }

    if (!obj._publicKey) {
      return false
    }

    if (!obj._clock) {
      return false
    }

    if (!obj._signature) {
      return false
    }
    return obj &&
      obj.next !== undefined && obj.hash !== undefined && obj.refs !== undefined
  }

  /**
   * Compares two entries.
   * @param {Entry} a
   * @param {Entry} b
   * @returns {number} 1 if a is greater, -1 is b is greater
   */
  static compare<T>(a: Entry<T>, b: Entry<T>) {
    const aClock = a.clock;
    const bClock = b.clock;
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
    stack = stack.sort((a, b) => Clock.compare(a.clock, b.clock))
    return stack
  }

}
