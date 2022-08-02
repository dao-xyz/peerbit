const fs = (typeof window === 'object' || typeof self === 'object') ? null : eval('require("fs")') // eslint-disable-line
import { Level } from 'level';
import LRU from 'lru';
import { variant, field, serialize, deserialize } from '@dao-xyz/borsh';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { SodiumPlus, CryptographyKey, X25519PublicKey, Ed25519PublicKey } from 'sodium-plus';
import { waitFor } from '@dao-xyz/time';
/* import { ready, crypto_sign, crypto_sign_keypair, crypto_sign_verify_detached } from 'libsodium-wrappers';
 */
//import { ready, crypto_sign_keypair, crypto_sign, crypto_box_keypair, type KeyType as CryptoKeyType, KeyPair } from 'sodium-plus';
function createStore(path = './keystore'): Level {
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(path, { recursive: true })
  }
  return new Level(path, { valueEncoding: 'view' })
}
const verifiedCache: { get(string: string): { publicKey: Ed25519PublicKey, data: Uint8Array }, set(string: string, { publicKey: Ed25519PublicKey, data: Uint8Array }) } = new LRU(1000)

const _crypto = SodiumPlus.auto();

@variant(0)
export class KeySet {

  @field(U8IntArraySerializer)
  key: Uint8Array

  constructor(props?: {
    key: Uint8Array
  }) {
    if (props) {
      this.key = props.key; // secret + public key 
    }
  }
}

type KeyType = 'box' | 'sign'
export const getStoreId = (type: KeyType, key: string) => type + '/' + key;

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

  async open() {
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


  async hasKey(id: string, type: KeyType): Promise<boolean> {
    if (!id) {
      throw new Error('id needed to check a key')
    }

    if (this._store.status === 'opening') {
      await waitFor(() => this._store.status === 'open')
    }

    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }
    const storeId = getStoreId(type, id);
    let hasKey = false
    try {
      const storedKey = this._cache.get(storeId) || await this._store.get(storeId)
      hasKey = storedKey !== undefined && storedKey !== null
    } catch (e) {
      // Catches 'Error: ENOENT: no such file or directory, open <path>'
      console.error('Error: ENOENT: no such file or directory')
    }

    return hasKey
  }

  async createKey(id: string | Buffer | Uint8Array, type: KeyType = 'sign'): Promise<CryptographyKey> {

    if (!id) {
      throw new Error('id needed to create a key')
    }

    if (typeof id !== 'string') {
      id = Buffer.isBuffer(id) ? id.toString('base64') : Buffer.from(id).toString('base64')
    }

    if (this._store.status === 'opening') {
      await waitFor(() => this._store.status === 'open')
    }

    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }

    // Throws error if seed is lower than 192 bit length.
    /* const keys = await unmarshal(ec.genKeyPair({ entropy: options.entropy }).getPrivate().toArrayLike(Buffer))
    const pubKey = keys.public.marshal()
    const decompressedKey = secp256k1.publicKeyConvert(Buffer.from(pubKey), false)
    const key = {
      publicKey: Buffer.from(decompressedKey).toString('hex'),
      privateKey: Buffer.from(keys.marshal()).toString('hex')
    } */

    const storeId = getStoreId(type, id);
    const crypto = await _crypto;
    const key = await (type === 'box' ? (crypto.crypto_box_keypair()) : (crypto.crypto_sign_keypair()))

    const keys = new KeySet({
      key: new Uint8Array(key.getBuffer())
    });

    try {
      const buffer = Buffer.from(serialize(keys));
      await this._store.put(storeId, buffer as any as string) // TODO fix types, are just wrong 
    } catch (e) {
      console.log(e)
    }
    this._cache.set(storeId, keys)
    return key
  }

  async getKey(id: string | Buffer | Uint8Array, type: KeyType = 'sign'): Promise<CryptographyKey> {
    if (!id) {
      throw new Error('id needed to get a key')
    }
    if (typeof id !== 'string') {
      id = Buffer.isBuffer(id) ? id.toString('base64') : Buffer.from(id).toString('base64')
    }
    if (!this._store) {
      await this.open()
    }

    if (this._store.status === 'opening') {
      await waitFor(() => this._store.status === 'open')
    }

    if (this._store.status && this._store.status !== 'open') {
      return Promise.resolve(null)
    }

    const storeId = getStoreId(type, id);
    const cachedKey = this._cache.get(storeId)

    let loadedKey: KeySet
    if (cachedKey)
      loadedKey = cachedKey
    else {
      let buffer = undefined;
      try {

        buffer = await this._store.get(storeId) as any as Uint8Array;
      } catch (e) {
        // not found
        return Promise.resolve(null)
      }

      loadedKey = deserialize(Buffer.from(buffer), KeySet);
    }

    if (!loadedKey) {
      return
    }

    if (!cachedKey) {
      this._cache.set(storeId, loadedKey)
    }

    return new CryptographyKey(Buffer.from(loadedKey.key))
  }

  async sign(key: CryptographyKey, arrayLike: string | Uint8Array | Buffer): Promise<Uint8Array> {
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
        const signedData = await crypto.crypto_sign_open(Buffer.from(signature), publicKey);
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
}

export const arraysEqual = (array1?: Uint8Array, array2?: Uint8Array) => {
  if (!!array1 != !!array2)
    return false;
  return array1.length === array2.length && array1.every(function (value, index) { return value === array2[index] });
}
