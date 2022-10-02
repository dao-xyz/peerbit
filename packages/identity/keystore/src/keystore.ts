const fs = (typeof window === 'object' || typeof self === 'object') ? null : eval('require("fs")') // eslint-disable-line
import { Level } from 'level';
import LRU from 'lru-cache';
import { variant, field, serialize, deserialize, option, Constructor } from '@dao-xyz/borsh';
import { U8IntArraySerializer, bufferSerializer, arraysEqual } from '@dao-xyz/borsh-utils';
import { X25519PublicKey, Ed25519PublicKey, X25519SecretKey, Ed25519PrivateKey, Keypair } from '@dao-xyz/peerbit-crypto';
import { waitFor } from '@dao-xyz/time';
import { createHash, Sign } from 'crypto';
import sodium from 'libsodium-wrappers';

export interface Type<T> extends Function {
  new(...args: any[]): T;
}
const DEFAULT_KEY_GROUP = '_';
const getGroupKey = (group: string) => group === DEFAULT_KEY_GROUP ? DEFAULT_KEY_GROUP : createHash('sha1').update(group).digest('base64')
const getIdKey = (id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey): string => {
  if (id instanceof X25519PublicKey || id instanceof Ed25519PublicKey) {
    return id.hashCode()
  }

  if (typeof id !== 'string') {
    id = Buffer.isBuffer(id) ? id.toString('base64') : Buffer.from(id).toString('base64')
  }
  return id;
}

const isId = (id: string) => id.indexOf('/') !== -1

const idFromKey = async (key: X25519PublicKey | Ed25519PublicKey): Promise<string> => {
  return key.hashCode()
}

/* import { ready, crypto_sign, crypto_sign_keypair, crypto_sign_verify_detached } from 'libsodium-wrappers';
 */
//import { ready, crypto_sign_keypair, crypto_sign, crypto_box_keypair, type KeyType as CryptoKeyType, KeyPair } from 'sodium-plus';
export const createStore = (path = './keystore'): Level => {
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(path, { recursive: true })
  }
  return new Level(path, { valueEncoding: 'view' })
}

const verifiedCache: { get(string: string): { publicKey: Ed25519PublicKey, data: Uint8Array }, set(string: string, value: { publicKey: Ed25519PublicKey, data: Uint8Array }) } = new LRU({ max: 1000 })


const NONCE_LENGTH = 24;

export type WithType<T> = Constructor<T> & { type: string };

export const getPath = (group: string, type: WithType<any>, key: string) => {
  return group + '/' + type.type + '/' + key
};

/**
 * Enc MSG with Metadata 
 */
@variant(0)
export class EncryptedMessage {


  @field(U8IntArraySerializer)
  nonce: Uint8Array

  @field(U8IntArraySerializer)
  cipher: Uint8Array

  constructor(props?: EncryptedMessage) {
    if (props) {
      this.nonce = props.nonce;
      this.cipher = props.cipher;
    }
  }
}

@variant(0)
export class KeyWithMeta {

  @field({ type: 'string' })
  group: string

  @field({ type: 'u64' })
  timestamp: bigint

  @field({ type: Keypair })
  keypair: Keypair

  constructor(props?: {
    timestamp: bigint,
    group: string,
    keypair: Keypair
  }) {
    if (props) {
      this.timestamp = props.timestamp;
      this.group = props.group
      this.keypair = props.keypair;
    }
  }

  static get type(): string {
    throw new Error("Unsupported")
  }

  equals(other: KeyWithMeta, ignoreMissingSecret: boolean = false) {
    return this.timestamp === other.timestamp && this.group === other.group
  }

  clone(sensitive: boolean): KeyWithMeta {
    throw new Error("Unsupported")
  }
}

/* 
@variant(0)
export class SignKeyWithMeta extends KeyWithMeta {

  @field({ type: Ed25519PublicKey })
  publicKey: Ed25519PublicKey

  @field({ type: option(Ed25519PrivateKey) })
  secretKey?: Ed25519PrivateKey

  constructor(props?: {
    publicKey: Ed25519PublicKey,
    secretKey?: Ed25519PrivateKey,
    group: string,
    timestamp: bigint
  }) {
    super({ group: props?.group, timestamp: props?.timestamp })
    if (props) {
      this.publicKey = props.publicKey;
      this.secretKey = props.secretKey;
    }
  }

  static get type(): string {
    return 'sign'
  }

  equals(other: KeyWithMeta, ignoreMissingSecret: boolean = false) {
    if (other instanceof SignKeyWithMeta) {
      if (!super.equals(other, ignoreMissingSecret)) {
        return false;
      }
      if (!this.publicKey.equals(other.publicKey)) {
        return false;
      }

      if (!this.secretKey !== !other.secretKey) {
        return ignoreMissingSecret;
      }
      if (!this.secretKey && !other.secretKey) {
        return true
      }
      return this.secretKey.equals(other.secretKey)

    }
    return false;
  }

  clone(sensitive: boolean) {
    return new SignKeyWithMeta({
      group: this.group,
      publicKey: this.publicKey,
      timestamp: this.timestamp,
      secretKey: sensitive ? this.secretKey : undefined
    })
  }

} */

/* 
@variant(1)
export class BoxKeyWithMeta extends KeyWithMeta {


  @field({ type: X25519PublicKey })
  publicKey: X25519PublicKey

  @field({ type: option(X25519SecretKey) })
  secretKey?: X25519SecretKey

  constructor(props?: {
    publicKey: X25519PublicKey,
    secretKey?: X25519SecretKey,
    group: string,
    timestamp: bigint
  }) {
    super({ group: props?.group, timestamp: props?.timestamp })
    if (props) {
      this.publicKey = props.publicKey;
      this.secretKey = props.secretKey;
    }
  }

  static get type(): string {
    return 'box'
  }

  equals(other: KeyWithMeta, ignoreMissingSecret: boolean = false) {
    if (other instanceof BoxKeyWithMeta) {
      if (!super.equals(other, ignoreMissingSecret)) {
        return false;
      }
      if (!this.publicKey.equals(other.publicKey)) {
        return false;
      }

      if (!this.secretKey !== !other.secretKey) {
        return ignoreMissingSecret;
      }
      if (!this.secretKey && !other.secretKey) {
        return true
      }
      return this.secretKey.equals(other.secretKey)

    }
    return false;
  }

  clone(sensitive: boolean) {
    return new BoxKeyWithMeta({
      group: this.group,
      publicKey: this.publicKey,
      timestamp: this.timestamp,
      secretKey: sensitive ? this.secretKey : undefined
    })
  }
}
 */






export class Keystore {

  _store: Level;
  _cache: LRU<string, KeyWithMeta>

  constructor(input: (Level | { store?: string } | { store?: Level }) & { cache?: any } | string = {}) {
    if (typeof input === 'string') {
      this._store = createStore(input)
    } else if (typeof input["open"] === 'function') {
      this._store = input as Level
    } else if (typeof input["store"] === 'string') {
      this._store = createStore(input["store"])
    } else {
      this._store = input["store"] || createStore()
    }
    this._cache = input["cache"] || new LRU({ max: 100 })
  }

  async openStore() {
    if (this._store) {
      await this._store.open()
      return Promise.resolve()
    }
    return Promise.reject(new Error('Keystore: No store found to open'))
  }

  async close(): Promise<void> {
    if (!this._store) return
    await this._store.close()
  }

  get groupStore() {
    return this._store.sublevel('group')
  }

  get keyStore() {
    return this._store.sublevel('key')
  }


  async hasKey(key: string | Buffer | Uint8Array, type: WithType<KeyWithMeta>, group: string = DEFAULT_KEY_GROUP): Promise<boolean> {
    if (!key) {
      throw new Error('id needed to check a key')
    }

    const idKey = getIdKey(key);

    if (this._store.status === 'opening') {
      await waitFor(() => this._store.status === 'open')
    }

    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }

    const storeId = getPath(group, type, idKey);
    return this.hasKeyById(storeId);
  }

  async hasKeyById(id: string): Promise<boolean> {
    let hasKey = false
    try {
      const storedKey = this._cache.get(id) || (isId(id) ? await this.groupStore.get(id, { valueEncoding: 'view' }) : await this.keyStore.get(id, { valueEncoding: 'view' }))
      hasKey = storedKey !== undefined && storedKey !== null
    } catch (e) {

      return undefined;
    }
    return hasKey
  }


  async createKey(id: string | Buffer | Uint8Array, keypair: Keypair, group?: string, options: { overwrite: boolean } = { overwrite: false }): Promise<KeyWithMeta> { // TODO fix types

    await sodium.ready;
    let key: { secretKey: X25519SecretKey, publicKey: X25519PublicKey } | { secretKey: Ed25519PrivateKey, publicKey: Ed25519PublicKey } = undefined;
    /* 
        if (type as any === BoxKeyWithMeta) { // TODO fix types
          let kp = await sodium.crypto_box_keypair();
          key = {
            secretKey: new X25519SecretKey({ secretKey: kp.privateKey }),
            publicKey: new X25519PublicKey({ publicKey: kp.publicKey }),
          }
        }
        else if (type as any === SignKeyWithMeta) { // TODO fix types
          let kp = await sodium.crypto_sign_keypair()
          key = {
            secretKey: new Ed25519PrivateKey({ secretKey: kp.privateKey }),
            publicKey: new Ed25519PublicKey({ publicKey: kp.publicKey }),
          }
        }
        else {
          throw new Error("Unuspported")
        } */
    const keyWithMeta = new KeyWithMeta({

      timestamp: BigInt(+new Date),
      group: group || DEFAULT_KEY_GROUP,
      keypair
    });

    await this.saveKey(keyWithMeta, id, options)
    return keyWithMeta;

  }

  async waitForOpen() {
    if (this._store.status === 'opening') {
      await waitFor(() => this._store.status === 'open')
    }
  }

  async saveKey<T extends KeyWithMeta>(toSave: T, id?: string | Buffer | Uint8Array, options: { overwrite: boolean } = { overwrite: false }): Promise<T> { // TODO fix types 
    const key = toSave as any as (BoxKeyWithMeta | SignKeyWithMeta);
    const idKey = id ? getIdKey(id) : await idFromKey(key.publicKey);
    await this.waitForOpen();
    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }



    // Normalize group names
    const groupHash = getGroupKey(key.group);
    const path = getPath(groupHash, key.constructor as any, idKey);


    if (!options.overwrite) {
      const existingKey = await this.getKeyById<BoxKeyWithMeta | SignKeyWithMeta>(path);
      if (existingKey && !existingKey.equals(key)) {

        if (!existingKey.equals(key, true)) {
          throw new Error("Key already exist with this id, and is different")
        }
        if (!key.secretKey) {
          key.secretKey = existingKey.secretKey; // Assign key
        }
        return key as any as T; // Already save, TODO fix types
      }
    }

    const ser = serialize(key);
    const publicKeyString = key.publicKey.hashCode();
    await this.groupStore.put(path, Buffer.from(ser), { valueEncoding: 'view' }) // TODO fix types, are just wrong 
    await this.keyStore.put(publicKeyString, Buffer.from(ser), { valueEncoding: 'view' }) // TODO fix types, are just wrong 
    this._cache.set(path, key)
    this._cache.set(publicKeyString, key)
    return key as any as T; // TODO fix types
  }

  async getKeyByPath(id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey, type: WithType<T> = SignKeyWithMeta as any, group = DEFAULT_KEY_GROUP): Promise<KeyWithMeta> { // TODO fix types of type
    if (!id) {
      throw new Error('id needed to get a key')
    }
    const idKey = getIdKey(id);

    if (!this._store) {
      await this.openStore()
    }

    await this.waitForOpen();

    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }

    // Normalize group names
    group = getGroupKey(group);
    const storeId = getPath(group, type, idKey);
    return this.getKeyById(storeId)
  }

  async getKeyById(id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey): Promise<KeyWithMeta> {

    id = getIdKey(id);

    const cachedKey = this._cache.get(id)

    let loadedKey: KeyWithMeta
    if (cachedKey)
      loadedKey = cachedKey
    else {
      let buffer = undefined;
      try {

        buffer = isId(id) ? await this.groupStore.get(id, { valueEncoding: 'view' }) as any as Uint8Array : await this.keyStore.get(id, { valueEncoding: 'view' }) as any as Uint8Array;
      } catch (e) {
        // not found
        return Promise.resolve(null)
      }
      loadedKey = deserialize(buffer, KeyWithMeta);
    }

    if (!loadedKey) {
      return
    }

    if (!cachedKey) {
      this._cache.set(id, loadedKey)
    }

    return loadedKey; // TODO fix types, we make assumptions here
  }

  async getKeys<T extends KeyWithMeta>(group: string, type?: WithType<T>): Promise<T[]> {
    if (!this._store) {
      await this.openStore()
    }

    await this.waitForOpen();


    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }

    try {

      // Normalize group names
      const groupHash = getGroupKey(group);
      let prefix = groupHash;

      // Add type suffix
      if (type) {
        prefix += '/' + type.type;
      }
      const iterator = this.groupStore.iterator<any, Uint8Array>({ gte: prefix, lte: prefix + "\xFF", valueEncoding: 'view' });
      const ret: KeyWithMeta[] = [];

      for await (const [_key, value] of iterator) {
        ret.push(deserialize(value, KeyWithMeta));
      }

      return ret as T[];
    } catch (e) {
      // not found
      return Promise.resolve(null)
    }
  }

  /* static async sign(arrayLike: string | Uint8Array | Buffer, key: SignKeyWithMeta | Ed25519PrivateKey, signHashed: boolean = false): Promise<Uint8Array> {
    key = key instanceof SignKeyWithMeta ? key.secretKey : key;
    if (!key) {
      throw new Error('No signing key given')
    }

    if (!arrayLike) {
      throw new Error('Given input data was undefined')
    }

    let data: Uint8Array = undefined;
    if (typeof arrayLike === 'string') {
      data = new Uint8Array(Buffer.from(arrayLike))
    }
    else if (arrayLike instanceof Buffer) {
      data = new Uint8Array(arrayLike);
    }
    else {
      data = arrayLike;
    }


    await sodium.ready;
    const signature = await new Uint8Array(await sodium.crypto_sign_detached(signHashed ? await sodium.crypto_generichash(32, data) : data, key.secretKey));
    //const verified = await crypto.crypto_sign_verify_detached(data, await crypto.crypto_sign_publickey(key), Buffer.from(signature));

    return signature
  } */


  /* await ready;
const keypair = await crypto_sign_keypair();
  const signature2 = await crypto_sign(data, keypair.privateKey);
     const verified2 = await crypto_sign_verify_detached(signature2, data, keypair.publicKey);
 */

  /* async encrypt(arrayLike: string | Uint8Array | Buffer, key: BoxKeyWithMeta, reciever: X25519PublicKey): Promise<Uint8Array> {
    if (!key) {
      throw new Error('No signing key given')
    }

    if (!arrayLike) {
      throw new Error('Given input data was undefined')
    }


    let data: Uint8Array = undefined;
    if (typeof arrayLike === 'string') {
      data = new Uint8Array(Buffer.from(arrayLike))
    }
    else if (arrayLike instanceof Buffer) {
      data = new Uint8Array(arrayLike);
    }
    else {
      data = arrayLike;
    }

    let encrypted = undefined;
    await sodium.ready;
    const nonce = new Uint8Array(await sodium.randombytes_buf(NONCE_LENGTH));
    reciever = reciever instanceof BoxKeyWithMeta ? reciever.publicKey : reciever;
    encrypted = new Uint8Array(await sodium.crypto_box_easy(data, nonce, reciever.publicKey, key.secretKey.secretKey))

    return serialize(new EncryptedMessage({
      cipher: encrypted,
      nonce
    }))
  } */

  /*   async decrypt(bytes: Uint8Array, key: BoxKeyWithMeta, sender: X25519PublicKey): Promise<Uint8Array> {
   */

  /* if (sender) { */
  // Nonce??
  /* const nonce = bytes.slice(0, NONCE_LENGTH)
  const cipher = bytes.slice(NONCE_LENGTH, bytes.length - X25519PublicKey_LENGTH * 2)
  const sender = bytes.slice(bytes.length - X25519PublicKey_LENGTH * 2, bytes.length - X25519PublicKey_LENGTH)
  const reciever = bytes.slice(bytes.length - X25519PublicKey_LENGTH, bytes.length) */
  /* await sodium.ready;

  const msg = deserialize(bytes, EncryptedMessage);
  return new Uint8Array(await sodium.crypto_box_open_easy(msg.cipher, msg.nonce, sender.publicKey, key.secretKey.secretKey)) */
  /* }
  else {
    const cipher = bytes.slice(NONCE_LENGTH, bytes.length)
    return new Uint8Array(await crypto.crypto_secretbox_open(Buffer.from(cipher), Buffer.from(nonce), key.secretKey))
  } */
  /* } */

  /*   async verify(signature: Uint8Array, publicKey: Ed25519PublicKey, data: Uint8Array, signedHash = false) {
      return Keystore.verify(signature, publicKey, data, signedHash)
    } */

  /*  static async verify(signature: Uint8Array, publicKey: Ed25519PublicKey, data: Uint8Array, signedHash = false) {
     const signatureString = Buffer.from(signature).toString()
     const cached = verifiedCache.get(signatureString)
     let res = false
     if (!cached) {
       await sodium.ready;
 
 
       try {
         const signedData = await Keystore.open(signature, publicKey);
         const verified = Buffer.compare(signedData, signedHash ? await sodium.crypto_generichash(32, data) : data) === 0;
         res = verified
         if (verified) {
           verifiedCache.set(signatureString, { publicKey, data })
         }
       } catch (error) {
         return false;
       }
 
     } else {
       const compare = (cached: Uint8Array, data: Uint8Array) => {
         return Buffer.isBuffer(data) ? Buffer.compare(cached, data) === 0 : arraysEqual(cached, data)
       }
       res = cached.publicKey.equals(publicKey) && compare(cached.data, data)
     }
     return res
   } */

  /*  async open(signature: Uint8Array, publicKey: Ed25519PublicKey): Promise<Buffer> {
     return Keystore.open(signature, publicKey)
   }
 
   static async open(signature: Uint8Array, publicKey: Ed25519PublicKey): Promise<Buffer> {
     const crypto = await _crypto;
     return await crypto.crypto_sign_open(Buffer.from(signature), publicKey);
   } */


}

