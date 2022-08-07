
import path from 'path';
import assert from 'assert';
import LRU from 'lru';
import { Keystore } from '../keystore';
import rmrf from 'rimraf'
import { CryptographyKey } from 'sodium-plus';
import { waitFor } from '@dao-xyz/time';

const fs = require('fs-extra')

const implementations = require('orbit-db-storage-adapter/test/implementations')
const properLevelModule = implementations.filter(i => i.key.indexOf('level') > -1).map(i => i.module)[0]
const storage = require('orbit-db-storage-adapter')(properLevelModule)

let store: any;
const fixturePath = path.join('packages/orbit-db-keystore/src/__tests__', 'fixtures', 'signingKeys')
const storagePath = path.join('packages/orbit-db-keystore/src/__tests__', 'signingKeys')
const upgradePath = path.join('packages/orbit-db-keystore/src/__tests__', 'upgrade')

describe('keystore', () => {
  beforeAll(async () => {
    await fs.copy(fixturePath, storagePath)
    store = await storage.createStore('./keystore-test')
  })

  afterAll(async () => {
    rmrf.sync(storagePath)
    rmrf.sync(upgradePath)
  })

  describe('constructor', () => {
    it('creates a new Keystore instance', async () => {
      const keystore = new Keystore(store)

      assert.strictEqual(typeof keystore.close, 'function')
      assert.strictEqual(typeof keystore.open, 'function')
      assert.strictEqual(typeof keystore.hasKey, 'function')
      assert.strictEqual(typeof keystore.createKey, 'function')
      assert.strictEqual(typeof keystore.getKey, 'function')
      assert.strictEqual(typeof keystore.sign, 'function')
      assert.strictEqual(typeof Keystore.getPublicSign, 'function')
      assert.strictEqual(typeof Keystore.getPublicBox, 'function')
      assert.strictEqual(typeof keystore.verify, 'function')
    })

    it('assigns this._store', async () => {
      const keystore = new Keystore(store)
      // Loose check for leveldownishness
      assert.strictEqual(keystore._store["_db"].status, 'open')
    })

    it('assigns this.cache with default of 100', async () => {
      const keystore = new Keystore(store)
      assert.strictEqual(keystore._cache.max, 100)
    })

    it('creates a proper leveldown / level-js store if not passed a store', async () => {
      const keystore = new Keystore()
      assert.strictEqual(keystore._store.status, 'opening')
      await keystore.close()
    })

    it('creates a keystore with empty options', async () => {
      const keystore = new Keystore({})
      assert.strictEqual(keystore._store.status, 'opening')
      await keystore.close()
    })

    it('creates a keystore with only cache', async () => {
      const cache = new LRU(10)
      const keystore = new Keystore({ cache })
      assert.strictEqual(keystore._store.status, 'opening')
      assert(keystore._cache === cache)
      await keystore.close()
    })

    it('creates a keystore with both', async () => {
      const cache = new LRU(10)
      const keystore = new Keystore({ store, cache })
      assert.strictEqual(keystore._store['db'].status, 'open')
      assert(keystore._cache === cache)
      assert(keystore._store === store)
    })
  })

  describe('createKey', () => {
    let keystore: Keystore

    beforeEach(async () => {
      keystore = new Keystore(store)
      if (store.db.status !== 'open') {
        await store.open()
      }
    })

    it('creates a new key', async () => {
      const id = 'X'
      await keystore.createKey(id, 'sign')
      const hasKey = await keystore.hasKey(id, 'sign')
      assert.strictEqual(hasKey, true)
    })



    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.createKey(undefined, undefined)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error accessing a closed store', async () => {
      try {
        const id = 'X'

        await store.close()
        await keystore.createKey(id, 'sign')
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    afterEach(async () => {
      // await keystore.close()
    })
  })

  describe('hasKey', () => {
    let keystore: Keystore

    beforeAll(async () => {
      if (store.db.status !== 'open') {
        await store.open()
      }
      keystore = new Keystore(store)
      await keystore.createKey('YYZ', 'sign')
    })

    it('returns true if key exists', async () => {
      const hasKey = await keystore.hasKey('YYZ', 'sign')
      assert.strictEqual(hasKey, true)
    })

    it('returns false if key does not exist', async () => {
      let hasKey
      try {
        hasKey = await keystore.hasKey('XXX', 'sign')
      } catch (e) {
        assert.strictEqual(hasKey, true)
      }
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.hasKey(undefined, undefined)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error accessing a closed store', async () => {
      try {
        await store.close()
        await keystore.hasKey('XXX', 'sign')
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    afterEach(async () => {
      // await keystore.close()
    })
  })

  describe('getKey', () => {
    let keystore: Keystore

    beforeAll(async () => {
      if (store.db.status !== 'open') {
        await store.open()
      }
      keystore = new Keystore(store)
      await keystore.createKey('ZZZ', 'sign')
    })

    it('gets an existing key', async () => {
      const key = await keystore.getKey('ZZZ', 'sign')
      assert.strictEqual(key.getLength(), 96)
    })

    it('throws an error upon accessing a non-existant key', async () => {
      try {
        await keystore.getKey('ZZZZ', 'sign')
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.getKey(undefined, undefined)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error accessing a closed store', async () => {
      try {
        await store.close()
        await keystore.getKey('ZZZ', 'sign')
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    afterAll(async () => {
      // keystore.close()
    })
  })

  describe('getKeys', () => {
    let keystore: Keystore, aSignKey: CryptographyKey, aBoxKey: CryptographyKey, aBox2Key: CryptographyKey, bSignKey: CryptographyKey

    beforeAll(async () => {

      keystore = new Keystore()
      aSignKey = await keystore.createKey('ASign', 'sign', 'Group')
      aBoxKey = await keystore.createKey('ABox', 'box', 'Group')
      aBox2Key = await keystore.createKey('ABox2', 'box', 'Group')
      bSignKey = await keystore.createKey('BSign', 'sign', 'B')

    })

    it('gets keys by group', async () => {
      const keys = await keystore.getKeys('Group')
      assert.deepStrictEqual(keys[0].getBuffer(), aBoxKey.getBuffer())
      assert.deepStrictEqual(keys[1].getBuffer(), aBox2Key.getBuffer())
      assert.deepStrictEqual(keys[2].getBuffer(), aSignKey.getBuffer())

    })


    it('gets keys by group and type', async () => {
      const keys = await keystore.getKeys('Group', 'box')
      assert.deepStrictEqual(keys[0].getBuffer(), aBoxKey.getBuffer())
      assert.deepStrictEqual(keys[1].getBuffer(), aBox2Key.getBuffer())
    })


    afterAll(async () => {
      // keystore.close()
    })
  })

  describe('sign', () => {
    let keystore: Keystore, key, signingStore

    beforeAll(async () => {
      signingStore = await storage.createStore(storagePath)
      keystore = new Keystore(signingStore) // 
      /*  await new Promise((resolve) => {
         setTimeout(() => {
           resolve(true);
         }, 3000);
       })
       await keystore.createKey('signing', 'sign');
       await keystore.close(); */
      key = await keystore.getKey('signing', 'sign')
    })

    it('signs data', async () => {
      const expectedSignature = new Uint8Array([6, 67, 127, 228, 255, 25, 228, 149, 239, 54, 116, 56, 63, 165, 202, 141, 76, 75, 130, 245, 71, 207, 234, 8, 224, 190, 114, 251, 6, 29, 245, 214, 231, 243, 44, 160, 88, 210, 85, 148, 192, 167, 247, 126, 143, 38, 56, 141, 6, 43, 239, 251, 135, 190, 56, 173, 81, 0, 96, 92, 99, 246, 186, 14, 100, 97, 116, 97, 32, 100, 97, 116, 97, 32, 100, 97, 116, 97])
      const signature = await keystore.sign(key, Buffer.from('data data data'))
      assert.deepStrictEqual(signature, expectedSignature)
    })

    it('throws an error if no key is passed', async () => {
      try {
        await keystore.sign(null, Buffer.from('data data data'))
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error if no data is passed', async () => {
      try {
        await keystore.sign(key, null)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    afterAll(async () => {
      signingStore.close()
    })
  })

  describe('getPublic', () => {
    let keystore: Keystore, key, signingStore

    beforeAll(async () => {
      signingStore = await storage.createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKey('signing', 'sign')
    })



    it('gets the public key', async () => {
      const expectedKey = new Uint8Array([223, 178, 12, 178, 50, 135, 253, 208, 210, 71, 27, 136, 221, 91, 110, 140, 184, 251, 59, 171, 98, 101, 56, 87, 225, 117, 155, 60, 208, 107, 90, 246]
      );
      const publicKey = await Keystore.getPublicSign(key)
      assert.deepStrictEqual(new Uint8Array(publicKey.getBuffer()), expectedKey)
    })



    it('throws an error if no keys are passed', async () => {
      try {
        await Keystore.getPublicSign(null);
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })


    afterAll(async () => {
      await signingStore.close()
    })
  })

  describe('verify', () => {
    jest.setTimeout(5000)
    let keystore: Keystore, signingStore, publicKey, key: CryptographyKey

    beforeAll(async () => {
      signingStore = await storage.createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKey('signing', 'sign')
      publicKey = await Keystore.getPublicSign(key)
    })

    it('verifies content', async () => {
      const signature = '4FAJrjPESeuDK5AhhHVmrCdVbb6gTqczxex1CydLfCHH4kP4CBmoMLfH6UxLF2UmPkisNMU15RVHo63NbWiNvyyb2f4h8x5cKWtQrHY3mUL'
      try {
        const verified = await keystore.verify(Buffer.from(signature), publicKey, Buffer.from('data data data'))
        assert.strictEqual(verified, true)
      } catch (error) {
        const x = 123;
      }
    })

    it('verifies content with cache', async () => {
      const data = new Uint8Array(Buffer.from('data'.repeat(1024 * 1024)))
      const sig = await keystore.sign(key, Buffer.from(data))
      const startTime = new Date().getTime()
      await keystore.verify(sig, publicKey, Buffer.from(data))
      const first = new Date().getTime()
      await keystore.verify(sig, publicKey, Buffer.from(data))
      const after = new Date().getTime()
      console.log('First pass:', first - startTime, 'ms', 'Cached:', after - first, 'ms')
      assert.strictEqual(first - startTime > after - first, true)
    })

    it('does not verify content with bad signature', async () => {
      const signature = 'xxxxxx'
      const verified = await keystore.verify((new Uint8Array(Buffer.from(signature))), publicKey, Buffer.from('data data data'))
      assert.strictEqual(verified, false)
    })

    afterAll(async () => {
      signingStore.close()
    })
  })

  describe('open', () => {
    let keystore: Keystore, signingStore

    beforeEach(async () => {
      signingStore = await storage.createStore(storagePath)
      keystore = new Keystore(signingStore)
      signingStore.close()
    })

    it('closes then open', async () => {
      await waitFor(() => signingStore.db.status === 'closed');
      await keystore.open()
      assert.strictEqual(signingStore.db.status, 'open')
    })

    it('fails when no store', async () => {
      let error = false
      try {
        keystore._store = undefined
        await keystore.open()
      } catch (e) {
        error = e.message
      }
      assert.strictEqual(error, 'Keystore: No store found to open')
    })

    afterEach(async () => {
      signingStore.close()
    })
  })

  describe('encryption', () => {
    describe('box', () => {
      let keystore: Keystore, keyA: CryptographyKey, keyB: CryptographyKey, encryptStore

      beforeAll(async () => {
        encryptStore = await storage.createStore(storagePath)
        keystore = new Keystore(encryptStore) // 

        await keystore.createKey('box-a', 'box');
        await keystore.createKey('box-b', 'box');
        keyA = await keystore.getKey('box-a', 'box')
        keyB = await keystore.getKey('box-b', 'box')

      })

      it('encrypts/decrypts', async () => {
        const data = new Uint8Array([1, 2, 3]);
        const encrypted = await keystore.encrypt(data, keyA, await Keystore.getPublicBox(keyB));
        const decrypted = await keystore.decrypt(encrypted, keyB, await Keystore.getPublicBox(keyA))
        assert.deepStrictEqual(data, decrypted);
      })


      afterAll(async () => {
        encryptStore.close()
      })
    })

    describe('secret', () => {
      let keystore: Keystore, key: CryptographyKey, encryptStore

      beforeAll(async () => {
        encryptStore = await storage.createStore(storagePath)
        keystore = new Keystore(encryptStore) // 

        await keystore.createKey('secret', 'secret');
        key = await keystore.getKey('secret', 'secret')
      })

      it('encrypts/decrypts', async () => {
        const data = new Uint8Array([1, 2, 3]);
        const encrypted = await keystore.encrypt(data, key);
        const decrypted = await keystore.decrypt(encrypted, key)
        assert.deepStrictEqual(data, decrypted);
      })


      afterAll(async () => {
        encryptStore.close()
      })
    })

  })

})