import { LamportClock as Clock } from './lamport-clock'
import { isDefined } from './is-defined'
import { variant, field, vec, option, serialize, deserialize, Constructor } from '@dao-xyz/borsh';
import io from '@dao-xyz/peerbit-io-utils';
import { IPFS } from 'ipfs-core-types'
import { arraysEqual, joinUint8Arrays, UInt8ArraySerializer } from '@dao-xyz/peerbit-borsh-utils';
import { DecryptedThing, MaybeEncrypted, MaybeX25519PublicKey, PublicSignKey, SignKey, X25519PublicKey, PublicKeyEncryptionResolver, SignatureWithKey, AccessError } from "@dao-xyz/peerbit-crypto";
import { max, toBase64 } from './utils.js';
import sodium from 'libsodium-wrappers';
import { Encoding, JSON_ENCODING } from './encoding';
import { Identity } from './identity.js';

export const maxClockTimeReducer = <T>(res: bigint, acc: Entry<T>): bigint => max(res, acc.clock.time);

export type EncryptionTemplateMaybeEncrypted = EntryEncryptionTemplate<MaybeX25519PublicKey, MaybeX25519PublicKey, MaybeX25519PublicKey>;
export interface EntryEncryption {
  reciever: EncryptionTemplateMaybeEncrypted,
  options: PublicKeyEncryptionResolver
}

function arrayToHex(arr: Uint8Array): string {
  return [...new Uint8Array(arr)]
    .map(b => b.toString(16).padStart(2, "0"))
    .join("");
}


export function toBufferLE(num: bigint, width: number): Uint8Array {
  const hex = num.toString(16);
  const padded = hex.padStart(width * 2, '0').slice(0, width * 2);
  const arr = padded.match(/.{1,2}/g)?.map((byte) => parseInt(byte, 16));
  if (!arr) {
    throw new Error("Unexpected")
  }
  const buffer = Uint8Array.from(arr);
  buffer.reverse();
  return buffer;
}

export function toBigIntLE(buf: Uint8Array): bigint {
  const reversed = buf.reverse();
  const hex = arrayToHex(reversed);
  if (hex.length === 0) {
    return BigInt(0);
  }
  return BigInt(`0x${hex}`);
}




const IpfsNotDefinedError = () => new Error('Ipfs instance not defined')

export type CanAppend<T> = (canAppend: Entry<T>) => Promise<boolean> | boolean

// TODO do we really need to cbor all this way, only two fields are "normal". Raw storage could perhaps work better
export interface EntrySerialized<T> {
  gid: string,
  payload: Uint8Array,
  signature: Uint8Array,
  clock: Uint8Array,
  maxChainLength: Uint8Array,
  state: Uint8Array,
  reserved: Uint8Array,
  next: string[],
  forks: string[],
}

@variant(0)
export class Payload<T>
{

  /*   _encoding: Encoding<T> */

  @field(UInt8ArraySerializer)
  data: Uint8Array

  _value?: T;
  constructor(props?: {
    data: Uint8Array
    value?: T
  }) {
    if (props) {
      this.data = props.data;
      this._value = props.value;
    }
  }

  /*   init(encoding: Encoding<T>) {
      this._encoding = encoding;
      return this;
    } */

  equals(other: Payload<T>): boolean {
    return Buffer.compare(Buffer.from(this.data), Buffer.from(other.data)) === 0;
  }

  getValue(encoding: Encoding<T> = JSON_ENCODING): T {
    if (this._value != undefined) {
      return this._value
    }
    return encoding.decoder(this.data);
  }

  /* 
    _value?: T
    get value(): T {
      if (this._value)
        return this._value;
      const decoded = this._encoding.decoder(this._data)
      this._value = decoded;
      return this._value;
    } */
}

export interface EntryEncryptionTemplate<A, B, C> {
  clock: A
  payload: B,
  signature: C
}
class String {

  @field({ type: 'string' })
  string: string

  constructor(string: string) {
    this.string = string;
  }
}

@variant(0)
export class Entry<T> implements EntryEncryptionTemplate<Clock, Payload<T>, SignatureWithKey> {

  @field({ type: 'string' })
  gid: string // graph id

  @field({ type: MaybeEncrypted })
  _clock: MaybeEncrypted<Clock>

  @field({ type: MaybeEncrypted })
  _payload: MaybeEncrypted<Payload<T>>

  @field({ type: MaybeEncrypted })
  _signature: MaybeEncrypted<SignatureWithKey>

  @field({ type: vec('string') })
  next: string[] // Array of hashes (the tree)

  @field(({ type: vec('string') }))
  _forks: string[]; // not used yet

  @field({ type: 'u64' })
  maxChainLength: bigint; // longest chain/merkle tree path frmo this node. maxChainLength := max ( maxChainLength(this.next) , 1)

  @field({ type: 'u8' })
  _state: number; // reserved for states

  @field({ type: 'u8' })
  _reserved: number; // reserved for future changes

  @field({ type: 'string' })
  hash: string // "zd...Foo", we'll set the hash after persisting the entry

  static IPLD_LINKS = ['next']

  _encryption?: PublicKeyEncryptionResolver
  _encoding?: Encoding<T>


  constructor(obj?: {
    gid: string,
    payload: MaybeEncrypted<Payload<T>>
    signature: MaybeEncrypted<SignatureWithKey>,
    clock: MaybeEncrypted<Clock>;
    next: string[]
    forks: string[], //  (not used)
    maxChainLength: bigint,
    state: 0, // intentational type 0 (not used)
    reserved: 0  // intentational type 0  (not used)
  }) {
    if (obj) {
      this.gid = obj.gid;
      this._clock = obj.clock
      this._payload = obj.payload;
      this._signature = obj.signature;
      this.maxChainLength = obj.maxChainLength;
      this.next = obj.next;
      this._forks = obj.forks;
      this._reserved = obj.reserved;
      this._state = obj.state;
    }
  }

  init(props: { encryption?: PublicKeyEncryptionResolver, encoding: Encoding<T> } | Entry<T>): Entry<T> {
    const encryption = props instanceof Entry ? props._encryption : props.encryption;
    this._encryption = encryption;
    this._encoding = props instanceof Entry ? props._encoding : props.encoding
    return this;
  }

  get encoding() {
    if (!this._encoding) {
      throw new Error("Not initialized")
    }
    return this._encoding;
  }

  serialize(): EntrySerialized<T> {
    return {
      gid: this.gid,
      payload: serialize(this._payload),
      signature: serialize(this._signature),
      clock: serialize(this._clock),
      maxChainLength: toBufferLE(this.maxChainLength, 8), // u64
      next: this.next,
      forks: this._forks,
      state: new Uint8Array([this._state]),
      reserved: new Uint8Array([this._reserved]),
    }
  }

  /*  get id(): string {
     return this._id.decrypted.getValue(Id).id
   }
 
   async getId(): Promise<string> {
     await this._id.decrypt();
     return this.id;
   }
  */
  get clock(): Clock {
    return this._clock.decrypted.getValue(Clock)
  }

  async getClock(): Promise<Clock> {
    await this._clock.decrypt(this._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))
    return this.clock;
  }

  get payload(): Payload<T> {
    const payload = this._payload.decrypted.getValue(Payload)
    /*     payload.init(this._encoding);
     */
    return payload;
  }

  async getPayload(): Promise<Payload<T>> {
    await this._payload.decrypt(this._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))
    return this.payload;
  }

  async getPayloadValue(): Promise<T> {
    const payload = await this.getPayload()
    return payload.getValue(this.encoding);
  }

  get publicKey(): PublicSignKey {
    return this.signature.publicKey
  }

  async getPublicKey(): Promise<PublicSignKey> {
    await this.getSignature();
    return this.signature.publicKey;
  }


  get signature(): SignatureWithKey {
    return this._signature.decrypted.getValue(SignatureWithKey)
  }

  async getSignature(): Promise<SignatureWithKey> {
    await this._signature.decrypt(this._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)))
    return this.signature;
  }



  static createDataToSign(gid: string, payload: MaybeEncrypted<Payload<any>>, clock: MaybeEncrypted<Clock>, next: string[], fork: string[], state: number, reserved: number,): Uint8Array { // TODO fix types
    const arrays: Uint8Array[] = [serialize(new String(gid)), serialize(payload), serialize(clock)];
    arrays.push(toBufferLE(BigInt(next.length), 4))
    next.forEach((n) => {
      arrays.push(new Uint8Array(Buffer.from(n)));
    })
    arrays.push(toBufferLE(BigInt(fork.length), 4))
    fork.forEach((f) => {
      arrays.push(new Uint8Array(Buffer.from(f)));
    })
    arrays.push(new Uint8Array([state, reserved]))
    return joinUint8Arrays(arrays);
  }

  async createDataToSign(): Promise<Uint8Array> {
    return Entry.createDataToSign(this.gid, this._payload, this._clock, this.next, this._forks, this._state, this._reserved)
  }


  equals(other: Entry<T>) {
    return this.gid === other.gid && this.maxChainLength === other.maxChainLength && this._reserved === other._reserved && this._state === other._state && this._clock.equals(other._clock) && this._signature.equals(other._signature) && arraysEqual(this.next, other.next) && arraysEqual(this._forks, other._forks) && this._payload.equals(other._payload) // dont compare hashes because the hash is a function of the other properties
  }

  static async createGid(seed?: string): Promise<string> {
    await sodium.ready;
    return toBase64((await sodium.crypto_generichash(32, seed || (await sodium.randombytes_buf(32)))));
  }

  static async create<T>(properties: { ipfs: IPFS, gid?: string, gidSeed?: string, data: T, encoding?: Encoding<T>, canAppend?: CanAppend<T>, next?: Entry<T>[], clock?: Clock, pin?: boolean, encryption?: EntryEncryption, identity: Identity }): Promise<Entry<T>> {
    if (!properties.encoding || !properties.next) {
      properties = {
        ...properties,
        next: properties.next ? properties.next : [],
        encoding: properties.encoding ? properties.encoding : JSON_ENCODING
      }
    }

    if (!properties.encoding) {
      throw new Error("Missing encoding options")
    }

    if (!isDefined(properties.ipfs)) throw IpfsNotDefinedError()
    if (!isDefined(properties.data)) throw new Error('Entry requires data')
    if (!isDefined(properties.next) || !Array.isArray(properties.next)) throw new Error("'next' argument is not an array")


    // Clean the next objects and convert to hashes
    const nexts = properties.next;

    let payloadToSave = new Payload<T>({
      data: properties.encoding.encoder(properties.data),
      value: properties.data
    });

    if (properties.encryption?.reciever && !properties.encryption) {

    }

    const maybeEncrypt = async<Q>(thing: Q, reciever?: X25519PublicKey | X25519PublicKey[]): Promise<MaybeEncrypted<Q>> => {

      const recievers = reciever ? (Array.isArray(reciever) ? reciever : [reciever]) : undefined
      if (recievers?.length && recievers?.length > 0) {
        if (!properties.encryption) {
          throw new Error("Encrpryption config not initialized")
        }
        return await new DecryptedThing<Q>({ data: serialize(thing), value: thing }).encrypt(properties.encryption.options.getEncryptionKeypair, ...recievers);
      }
      return new DecryptedThing<Q>({
        data: serialize(thing),
        value: thing
      })
    }


    let clockValue: Clock | undefined = properties.clock;
    if (!clockValue) {
      const newTime = nexts?.length > 0 ? nexts.reduce(maxClockTimeReducer, 0n) + 1n : 0n;
      if (properties.encryption?.reciever.signature && properties.encryption?.reciever.clock) {
        throw new Error("Signature is to be encrypted yet the clock is not, which contains the publicKey as id. Either provide a custom Clock value that is not sensitive or set the reciever (encryption target) for the clock")
      }
      clockValue = new Clock(new Uint8Array(serialize(properties.identity.publicKey)), newTime)
    }
    else {
      const cv = clockValue;
      // check if nexts, that all nexts are happening BEFORE this clock value (else clock make no sense)
      nexts.forEach((n) => {
        if (n.clock.time >= cv.time) {
          throw new Error("Expecting next(s) to happen before entry, got: " + n.clock.time + " > " + cv.time);
        }
      })
    }

    const clock = await maybeEncrypt(clockValue, properties.encryption?.reciever.clock);
    /* const id = await maybeEncrypt(new Id({
      id: properties.logId
    }), properties.encryption?.reciever.id); */
    const payload = await maybeEncrypt(payloadToSave, properties.encryption?.reciever.payload);


    const nextHashes: string[] = [];
    let gid!: string;
    let maxChainLength = 0n;
    let maxClock = 0n;
    if (nexts?.length > 0) {
      // take min gid as our gid
      nexts.forEach((n) => {
        if (!n.hash) {
          throw new Error("Expecting hash to be defined to next entries")
        }
        nextHashes.push(n.hash);
        if (maxChainLength < n.maxChainLength || maxChainLength == n.maxChainLength) {
          maxChainLength = n.maxChainLength;
          if (!gid) {
            gid = n.gid;
            return;
          }
          // replace gid if next is from alonger chain, or from a later time, or same time but "smaller" gid 
          else if (maxChainLength < n.maxChainLength || maxClock < n.clock.time || (maxClock == n.clock.time && n.gid < gid)) {
            gid = n.gid;
          }
        }

      })
      if (!gid) {
        throw new Error("Unexpected behaviour, could not find gid")
      }
    }
    else {
      gid = properties.gid || (await Entry.createGid(properties.gidSeed));
    }

    maxChainLength += 1n; // include this


    const next = nextHashes;
    next?.forEach((next) => {
      if (typeof next !== 'string') {
        throw new Error("Unsupported next type")
      }
    })
    const forks: string[] = [];
    const state = 0;
    const reserved = 0;
    // Sign id, encrypted payload, clock, nexts, refs 
    const signature = await properties.identity.sign(Entry.createDataToSign(gid, payload, clock, next, forks, state, reserved))

    const signatureEncrypted = await maybeEncrypt(new SignatureWithKey({
      publicKey: properties.identity.publicKey,
      signature
    }), properties.encryption?.reciever.signature);

    const entry: Entry<T> = new Entry<T>({
      payload,
      clock,
      gid,
      maxChainLength,
      signature: signatureEncrypted,
      forks,
      state,
      reserved,
      next, // Array of hashes
      /* refs: properties.refs, */
    })

    entry.init({ encryption: properties.encryption?.options, encoding: properties.encoding });

    if (properties.canAppend) {
      if (! await properties.canAppend(entry)) {
        throw new AccessError()
      }
    }
    // Append hash and signature
    entry.hash = await Entry.toMultihash(properties.ipfs, entry, properties.pin)
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
  static toEntry<T>(entry: EntrySerialized<T>): Entry<T> {
    let clock: MaybeEncrypted<Clock>;
    let payload: MaybeEncrypted<Payload<T>>;
    let signature: MaybeEncrypted<SignatureWithKey>;
    if (entry instanceof Entry) {
      clock = entry._clock;
      payload = entry._payload;
      signature = entry._signature;
    }
    else {
      clock = deserialize<MaybeEncrypted<Clock>>(Buffer.from(entry.clock), MaybeEncrypted);
      payload = deserialize<MaybeEncrypted<Payload<T>>>(Buffer.from(entry.payload), MaybeEncrypted);
      signature = deserialize<MaybeEncrypted<SignatureWithKey>>(Buffer.from(entry.signature), MaybeEncrypted);
    }
    const e: Entry<T> = new Entry<T>({
      clock,
      payload,
      signature,
      maxChainLength: toBigIntLE(entry.maxChainLength),
      gid: entry.gid,
      next: entry.next,
      forks: entry.forks,
      reserved: 0,
      state: 0
    })

    e._state = entry.state[0];
    e._reserved = entry.reserved[0];
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
  static async fromMultihash<T>(ipfs: IPFS, hash: string) {
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
    if (!obj._payload) {
      return false
    }

    if (!obj.gid) {
      return false
    }

    if (!obj._clock) {
      return false
    }

    if (!obj._signature) {
      return false
    }
    return obj &&
      obj.next !== undefined
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
  static isDirectParent<T>(entry1: Entry<T>, entry2: Entry<T>) {
    return entry2.next.indexOf(entry1.hash as any) > -1 // TODO fix types
  }

  /**
   * Find entry's children from an Array of entries.
   * Returns entry's children as an Array up to the last know child.
   * @param {Entry} entry Entry for which to find the parents
   * @param {Array<Entry<T>>} values Entries to search parents from
   * @returns {Array<Entry<T>>}
   */
  static findDirectChildren<T>(entry: Entry<T>, values: Entry<T>[]): Entry<T>[] {
    let stack: Entry<T>[] = []
    let parent = values.find((e) => Entry.isDirectParent(entry, e))
    let prev = entry
    while (parent) {
      stack.push(parent)
      prev = parent
      parent = values.find((e) => Entry.isDirectParent(prev, e))
    }
    stack = stack.sort((a, b) => Clock.compare(a.clock, b.clock))
    return stack
  }

}
