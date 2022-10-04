import { Level } from 'level';
import LRU from 'lru-cache';
import { variant, field, serialize, deserialize, option, Constructor } from '@dao-xyz/borsh';
import { U8IntArraySerializer, bufferSerializer, arraysEqual } from '@dao-xyz/borsh-utils';
import { X25519PublicKey, Ed25519PublicKey, X25519SecretKey, Ed25519PrivateKey, Keypair, X25519Keypair, Ed25519Keypair } from '@dao-xyz/peerbit-crypto';
import { waitFor } from '@dao-xyz/time';
import { createHash, Sign } from 'crypto';
import sodium, { KeyPair } from 'libsodium-wrappers';
import { StoreError } from './errors';

export interface Type<T> extends Function {
  new(...args: any[]): T;
}
const PATH_KEY = '.'
const DEFAULT_KEY_GROUP = '_';
const getGroupKey = (group: string) => group === DEFAULT_KEY_GROUP ? DEFAULT_KEY_GROUP : createHash('sha1').update(group).digest('base64')
const getIdKey = (id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey): string => {
  if (id instanceof X25519PublicKey || id instanceof Ed25519PublicKey) {
    return id.hashCode()
  }

  if (typeof id !== 'string') {
    id = Buffer.isBuffer(id) ? id.toString('base64') : Buffer.from(id).toString('base64')
  }
  else {
    if (isPath(id)) {
      throw new Error("Ids can not contain path key: " + PATH_KEY);
    }
  }
  return id;
}

const isPath = (id: string) => id.indexOf(PATH_KEY) !== -1

/* export type WithType<T> = Constructor<T> & { type: string };
 */
export const getPath = (group: string, key: string) => {
  return group + PATH_KEY + key
};

const idFromKey = async (keypair: Keypair): Promise<string> => {
  return publicKeyFromKeyPair(keypair).hashCode();
}

const publicKeyFromKeyPair = (keypair: Keypair) => {
  if (keypair instanceof X25519Keypair) {
    return keypair.publicKey;
  }
  else if (keypair instanceof Ed25519Keypair) {
    return keypair.publicKey;
  }
  throw new Error("Unsupported")
}

/* import { ready, crypto_sign, crypto_sign_keypair, crypto_sign_verify_detached } from 'libsodium-wrappers';
 */
//import { ready, crypto_sign_keypair, crypto_sign, crypto_box_keypair, type KeyType as CryptoKeyType, KeyPair } from 'sodium-plus';
/**
 * Node only
 * @param path 
 * @returns 
 */
export const createStore = async (path = './keystore'): Promise<Level> => {
  const fs = await import('fs');
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(path, { recursive: true })
  }
  return new Level(path, { valueEncoding: 'view' })
}

/* const verifiedCache: { get(string: string): { publicKey: Ed25519PublicKey, data: Uint8Array }, set(string: string, value: { publicKey: Ed25519PublicKey, data: Uint8Array }): void } = new LRU({ max: 1000 })
 */

const NONCE_LENGTH = 24;


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
export class KeyWithMeta<T extends Keypair> {

  @field({ type: 'string' })
  group: string

  @field({ type: 'u64' })
  timestamp: bigint

  @field({ type: Keypair })
  keypair: T

  constructor(props?: {
    timestamp: bigint,
    group: string,
    keypair: T
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

  equals(other: KeyWithMeta<T>, ignoreMissingSecret: boolean = false) {
    return this.timestamp === other.timestamp && this.group === other.group && this.keypair.equals(other.keypair);
  }

}

/* 
@variant(0)
export class KeyWithMeta<Ed25519Keypair> extends KeyWithMeta {

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
    if (other instanceof KeyWithMeta<Ed25519Keypair>) {
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
    return new KeyWithMeta<Ed25519Keypair>({
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
  _cache: LRU<string, KeyWithMeta<any>>

  constructor(store: Level, cache?: any) {
    this._store = store;
    if (!this.open && !this.opening && this._store.open) {
      this._store.open()
    }
    if (!this._store) {
      throw new Error("Store needs to be provided");
    }
    this._cache = cache || new LRU({ max: 100 })
  }

  async openStore() {
    if (this._store) {
      await this._store.open()
      return Promise.resolve()
    }
    return Promise.reject(new Error('Keystore: No store found to open'))
  }

  assertOpen() {
    if (!this.open) {
      throw new StoreError("Keystore not open")
    }
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


  async hasKey(id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey, group?: string): Promise<boolean> {
    const getKey = await this.getKey(id, group)
    return !!getKey;
  }



  async createEd25519Key(options: { id?: string | Buffer | Uint8Array, group?: string, overwrite?: boolean } = {}): Promise<KeyWithMeta<Ed25519Keypair>> {
    return this.createKey(await Ed25519Keypair.create(), options)
  }
  async createX25519Key(options: { id?: string | Buffer | Uint8Array, group?: string, overwrite?: boolean } = {}): Promise<KeyWithMeta<X25519Keypair>> {
    return this.createKey(await X25519Keypair.create(), options)
  }
  async createKey<T extends Keypair>(keypair: T, options: { id?: string | Buffer | Uint8Array, group?: string, overwrite?: boolean } = {}): Promise<KeyWithMeta<T>> {
    await sodium.ready;
    /*  let key: { secretKey: X25519SecretKey, publicKey: X25519PublicKey } | { secretKey: Ed25519PrivateKey, publicKey: Ed25519PublicKey } = undefined; */
    /* 
        if (type as any === BoxKeyWithMeta) { // TODO fix types
          let kp = await sodium.crypto_box_keypair();
          key = {
            secretKey: new X25519SecretKey({ secretKey: kp.privateKey }),
            publicKey: new X25519PublicKey({ publicKey: kp.publicKey }),
          }
        }
        else if (type as any === KeyWithMeta<Ed25519Keypair>) { // TODO fix types
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
      group: options.group || DEFAULT_KEY_GROUP,
      keypair
    });

    await this.saveKey(keyWithMeta, options)
    return keyWithMeta;

  }
  get opening(): boolean {
    try {
      return this._store.status === 'opening'
    } catch (error) {
      return false; // .status will throw error if not opening sometimes
    }
  }

  get open(): boolean {
    try {
      return this._store.status === 'open'
    } catch (error) {
      return false;  // .status will throw error if not opening sometimes
    }
  }
  async waitForOpen() {
    if (this.opening) {
      await waitFor(() => this.open)
    }
  }

  async saveKey<T extends Keypair>(key: KeyWithMeta<T>, options: { id?: string | Buffer | Uint8Array, overwrite?: boolean } = {}): Promise<KeyWithMeta<T>> { // TODO fix types 


    const idKey = options.id ? getIdKey(options.id) : await idFromKey(key.keypair);

    await this.waitForOpen();
    this.assertOpen();




    // Normalize group names
    const groupHash = getGroupKey(key.group);
    const path = getPath(groupHash, idKey);

    if (!options.overwrite) {
      const existingKey = await this.getKey(path);
      if (existingKey && !existingKey.equals(key)) {
        throw new Error("Key already exist with this id, and is different")
      }
    }
    /* if (!options.overwrite) {
      const existingKey = await this.getKeyById(path);
      if (existingKey && !existingKey.equals(key)) {

        if (!existingKey.equals(key, true)) {
          throw new Error("Key already exist with this id, and is different")
        }
        if (!key.secretKey) {
          key.secretKey = existingKey.secretKey; // Assign key
        }
        return key as any as T; // Already save, TODO fix types
      }
    } */

    const ser = serialize(key);
    const publicKeyString = publicKeyFromKeyPair(key.keypair).hashCode();
    await this.groupStore.put(path, Buffer.from(ser), { valueEncoding: 'view' }) // TODO fix types, are just wrong 
    await this.keyStore.put(publicKeyString, Buffer.from(ser), { valueEncoding: 'view' }) // TODO fix types, are just wrong 
    this._cache.set(path, key)
    this._cache.set(publicKeyString, key)
    return key
  }

  /* async getKeyByPath<T extends Keypair>(id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey,): Promise<KeyWithMeta<T> | null> { // TODO fix types of type
    if (!id) {
      throw new Error('id needed to get a key')
    }
    const idKey = getIdKey(id);

    if (!this._store) {
      await this.openStore()
    }

    await this.waitForOpen();

    this.assertOpen();

    // Normalize group names
    group = getGroupKey(group);
    const storeId = getPath(group, idKey);
    return this.getKeyById(storeId)
  } */


  async getKey<T extends Keypair>(id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey, group?: string): Promise<KeyWithMeta<T> | undefined> {

    this.assertOpen();
    let path: string;
    if (typeof id === 'string' && isPath(id)) {
      path = id;
      if (group !== undefined) {
        throw new Error("Id is already a path, group parameter is not needed")
      }
    }
    else {
      group = getGroupKey(group || DEFAULT_KEY_GROUP)
      path = getPath(group, getIdKey(id));
    }
    const cachedKey = this._cache.get(path)

    let loadedKey: KeyWithMeta<T>
    if (cachedKey)
      loadedKey = cachedKey
    else {
      let buffer: Uint8Array;
      try {

        buffer = id instanceof X25519PublicKey || id instanceof Ed25519PublicKey ? await this.keyStore.get(publicKeyFromKeyPair(id).hashCode(), { valueEncoding: 'view' }) : await this.groupStore.get(path, { valueEncoding: 'view' });
      } catch (e: any) {
        // not found
        return
      }
      loadedKey = deserialize(buffer, KeyWithMeta) as KeyWithMeta<T>;
    }

    if (!loadedKey) {
      return
    }

    if (!cachedKey) {
      this._cache.set(path, loadedKey)
    }

    return loadedKey; // TODO fix types, we make assumptions here
  }

  async getKeys(group: string): Promise<KeyWithMeta<any>[] | null> {
    if (!this._store) {
      await this.openStore()
    }

    await this.waitForOpen();


    this.assertOpen();


    try {

      // Normalize group names
      const groupHash = getGroupKey(group);
      let prefix = groupHash;

      const iterator = this.groupStore.iterator<any, Uint8Array>({ gte: prefix, lte: prefix + "\xFF", valueEncoding: 'view' });
      const ret: KeyWithMeta<any>[] = [];

      for await (const [_key, value] of iterator) {
        ret.push(deserialize(value, KeyWithMeta));
      }

      return ret;
    } catch (e: any) {
      // not found
      return Promise.resolve(null)
    }
  }

  /* static async sign(arrayLike: string | Uint8Array | Buffer, key: KeyWithMeta<Ed25519Keypair> | Ed25519PrivateKey, signHashed: boolean = false): Promise<Uint8Array> {
    key = key instanceof KeyWithMeta<Ed25519Keypair> ? key.secretKey : key;
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

