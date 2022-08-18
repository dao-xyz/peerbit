const fs = (typeof window === 'object' || typeof self === 'object') ? null : eval('require("fs")') // eslint-disable-line
import { Level } from 'level';

import LRU from 'lru';
import { variant, field, serialize, deserialize } from '@dao-xyz/borsh';
import { joinUint8Arrays, U64Serializer } from '@dao-xyz/io-utils';
import { SodiumPlus, CryptographyKey, X25519PublicKey, Ed25519PublicKey } from 'sodium-plus';
import { waitFor } from '@dao-xyz/time';
import { CryptographyKeySerializer } from '@dao-xyz/encryption-utils';
import { createHash } from 'crypto';

const DEFAULT_KEY_GROUP = '_';
const getGroupKey = (group: string) => group === DEFAULT_KEY_GROUP ? DEFAULT_KEY_GROUP : createHash('sha1').update(group).digest('base64')
const getIdKey = (id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey): string => {
  if (id instanceof X25519PublicKey || id instanceof Ed25519PublicKey) {
    return id.getBuffer().toString('base64');
  }

  if (typeof id !== 'string') {
    id = Buffer.isBuffer(id) ? id.toString('base64') : Buffer.from(id).toString('base64')
  }
  return id;
}

const isId = (id: string) => id.indexOf('/') !== -1

const idFromKey = async (key: CryptographyKey, type: KeyType): Promise<string> => {
  const crypto = await _crypto;
  if (type === 'sign') {
    return (await crypto.crypto_sign_publickey(key)).toString('base64');
  }
  if (type === 'box') {
    return (await crypto.crypto_box_publickey(key)).toString('base64');
  }
  return createHash('sha256').update(key.getBuffer()).digest('base64')
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

const verifiedCache: { get(string: string): { publicKey: Ed25519PublicKey, data: Uint8Array }, set(string: string, { publicKey: Ed25519PublicKey, data: Uint8Array }) } = new LRU(1000)
const _crypto = SodiumPlus.auto();
type KeyType = 'box' | 'sign'
const NONCE_LENGTH = 24;
const X25519PublicKey_LENGTH = 32;
export const getKeyId = (group: string, type: KeyType, key: string) => group + '/' + type + '/' + key;

@variant(0)
export class KeyWithMeta {

  @field(CryptographyKeySerializer)
  key: CryptographyKey

  @field({ type: 'String' })
  group: string

  @field(U64Serializer)
  timestamp: number


  constructor(props?: {
    key: CryptographyKey,
    group: string,
    timestamp: number
  }) {
    if (props) {
      this.group = props.group
      this.key = props.key; // secret + public key 
      this.timestamp = props.timestamp;
    }
  }


  equals(other: KeyWithMeta) {
    return this.timestamp === other.timestamp && this.group === other.group && Buffer.compare(this.key.getBuffer(), other.key.getBuffer()) === 0
  }
}


export class Keystore {

  _store: Level;
  _cache: LRU

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
    this._cache = input["cache"] || new LRU(100)
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


  async hasKey(key: string | Buffer | Uint8Array, type: KeyType, group: string = DEFAULT_KEY_GROUP): Promise<boolean> {
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

    const storeId = getKeyId(group, type, idKey);
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


  async createKey(id?: string | Buffer | Uint8Array, type: KeyType = 'sign', group?: string): Promise<KeyWithMeta> {

    const crypto = await _crypto;
    let key: CryptographyKey = undefined;
    if (type === 'box') {
      key = await crypto.crypto_box_keypair();
    }
    else if (type === 'sign') {
      key = await crypto.crypto_sign_keypair()
    }

    const keyWithMeta = await this.saveKey(key, id, type, group)
    return keyWithMeta;

  }

  async waitForOpen() {
    if (this._store.status === 'opening') {
      await waitFor(() => this._store.status === 'open')
    }
  }

  async saveKey(key: CryptographyKey, id?: string | Buffer | Uint8Array, type: KeyType = 'sign', group = DEFAULT_KEY_GROUP, timestamp = +new Date): Promise<KeyWithMeta> {
    const idKey = id ? getIdKey(id) : await idFromKey(key, type);

    await this.waitForOpen();


    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }

    // Normalize group names
    const groupHash = getGroupKey(group);
    const keyId = getKeyId(groupHash, type, idKey);
    const keyWithMeta = new KeyWithMeta({
      key,
      timestamp,
      group
    });

    const buffer = Buffer.from(serialize(keyWithMeta));
    const publicKeyString = (type === 'box' ? await Keystore.getPublicBox(key) : await Keystore.getPublicSign(key)).toString('base64')
    await this.groupStore.put(keyId, buffer, { valueEncoding: 'view' }) // TODO fix types, are just wrong 
    await this.keyStore.put(publicKeyString, key.getBuffer(), { valueEncoding: 'view' }) // TODO fix types, are just wrong 
    this._cache.set(keyId, keyWithMeta)
    this._cache.set(publicKeyString, keyWithMeta)
    return keyWithMeta;
  }

  async getKeyByPath(id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey, type: KeyType = 'sign', group = DEFAULT_KEY_GROUP): Promise<KeyWithMeta> {
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
    const storeId = getKeyId(group, type, idKey);
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
      loadedKey = deserialize(Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer), KeyWithMeta);
    }

    if (!loadedKey) {
      return
    }

    if (!cachedKey) {
      this._cache.set(id, loadedKey)
    }

    return loadedKey
  }

  async getKeys(group: string, type?: string): Promise<KeyWithMeta[]> {
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
        prefix += '/' + type;
      }
      const iterator = this.groupStore.iterator({ gte: prefix, lte: prefix + "\xFF", valueEncoding: 'view' });
      const ret: KeyWithMeta[] = [];

      for await (const [_key, value] of iterator) {
        ret.push(deserialize(Buffer.from(value), KeyWithMeta));
      }

      return ret;
    } catch (e) {
      // not found
      return Promise.resolve(null)
    }
  }

  async sign(arrayLike: string | Uint8Array | Buffer, key: CryptographyKey): Promise<Uint8Array> {
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

    /* await ready;
  const keypair = await crypto_sign_keypair();
    const signature2 = await crypto_sign(data, keypair.privateKey);
       const verified2 = await crypto_sign_verify_detached(signature2, data, keypair.publicKey);
   */

    const crypto = await _crypto;
    const signature = await new Uint8Array(await crypto.crypto_sign(Buffer.from(data), await crypto.crypto_sign_secretkey(key)));
    //const verified = await crypto.crypto_sign_verify_detached(data, await crypto.crypto_sign_publickey(key), Buffer.from(signature));

    return signature
  }

  async encrypt(arrayLike: string | Uint8Array | Buffer, key: CryptographyKey, reciever?: X25519PublicKey): Promise<Uint8Array> {
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
    const crypto = await _crypto;
    const nonce = new Uint8Array(await crypto.randombytes_buf(NONCE_LENGTH));
    const ret = [nonce];
    if (reciever) {
      const secret = await crypto.crypto_box_secretkey(key);
      encrypted = new Uint8Array(await crypto.crypto_box(Buffer.from(data), Buffer.from(nonce), secret, reciever))
      ret.push(encrypted);
      ret.push(new Uint8Array((await crypto.crypto_box_publickey_from_secretkey(secret)).getBuffer()))
    }
    else {
      encrypted = new Uint8Array(await crypto.crypto_secretbox(Buffer.from(data), Buffer.from(nonce), key))
      ret.push(encrypted)
    }
    return joinUint8Arrays(ret)
  }

  async decrypt(bytes: Uint8Array, key: CryptographyKey, sender?: X25519PublicKey): Promise<Uint8Array> {

    if (!key) {
      throw new Error('No signing key given')
    }
    const crypto = await _crypto;
    const nonce = bytes.slice(0, NONCE_LENGTH)
    const cipher = bytes.slice(NONCE_LENGTH, bytes.length - X25519PublicKey_LENGTH)

    if (sender) {
      // Nonce??
      return new Uint8Array(await crypto.crypto_box_open(Buffer.from(cipher), Buffer.from(nonce), await crypto.crypto_box_secretkey(key), sender))
    }
    else {
      const cipher = bytes.slice(NONCE_LENGTH, bytes.length)
      return new Uint8Array(await crypto.crypto_secretbox_open(Buffer.from(cipher), Buffer.from(nonce), key))
    }
  }



  static async getPublicSign(key: CryptographyKey): Promise<Ed25519PublicKey> {
    const crypto = await _crypto;
    return crypto.crypto_sign_publickey(key)
  }


  static async getPublicBox(key: CryptographyKey): Promise<X25519PublicKey> {
    const crypto = await _crypto;
    return crypto.crypto_box_publickey(key)
  }


  async verify(signature: Uint8Array, publicKey: Ed25519PublicKey, data: Uint8Array) {

    return Keystore.verify(signature, publicKey, data)
  }

  static async verify(signature: Uint8Array, publicKey: Ed25519PublicKey, data: Uint8Array) {
    const signatureString = Buffer.from(signature).toString()
    const cached = verifiedCache.get(signatureString)
    let res = false
    if (!cached) {
      const crypto = await _crypto;
      /*       const signedData = await crypto.crypto_sign_open(Buffer.from(signature), publicKey);
       */
      /*   const verified = await crypto.crypto_sign_verify_detached(signedData, publicKey, Buffer.from(signature));
       */
      /*       const open = await crypto.crypto_sign_open(Buffer.from(signature), await crypto.crypto_sign_publickey(key));
       */

      try {
        const signedData = await Keystore.open(signature, publicKey);
        const verified = Buffer.compare(signedData, Buffer.from(data)) === 0;
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
      res = Buffer.compare(cached.publicKey.getBuffer(), publicKey.getBuffer()) === 0 && compare(cached.data, data)
    }
    return res
  }

  async open(signature: Uint8Array, publicKey: Ed25519PublicKey): Promise<Buffer> {
    return Keystore.open(signature, publicKey)
  }

  static async open(signature: Uint8Array, publicKey: Ed25519PublicKey): Promise<Buffer> {
    const crypto = await _crypto;
    return await crypto.crypto_sign_open(Buffer.from(signature), publicKey);
  }


}


export const arraysEqual = (array1?: Uint8Array, array2?: Uint8Array) => {
  if (!!array1 != !!array2)
    return false;
  return array1.length === array2.length && array1.every(function (value, index) { return value === array2[index] });
}
