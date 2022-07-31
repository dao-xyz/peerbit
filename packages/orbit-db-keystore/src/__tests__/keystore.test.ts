
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

  describe('#createKey()', () => {
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

  describe('#hasKey()', () => {
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

  describe('#getKey()', () => {
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

  describe('#sign()', () => {
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
      const expectedSignature = new Uint8Array([128, 94, 90, 111, 244, 34, 12, 68, 67, 222, 198, 232, 113, 114, 19, 168, 15, 252, 93, 41, 116, 162, 218, 34, 230, 29, 101, 138, 255, 235, 195, 144, 251, 226, 68, 62, 242, 129, 222, 139, 107, 116, 47, 177, 223, 127, 16, 36, 28, 8, 201, 73, 50, 165, 138, 32, 160, 96, 186, 209, 126, 78, 230, 10, 100, 97, 116, 97, 32, 100, 97, 116, 97, 32, 100, 97, 116, 97])
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

  describe('#getPublic', () => {
    let keystore: Keystore, key, signingStore

    beforeAll(async () => {
      signingStore = await storage.createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKey('signing', 'sign')
    })



    it('gets the public key', async () => {
      const expectedKey = new Uint8Array([245, 250, 56, 119, 249, 255, 90, 188, 29, 84, 40, 192, 167, 161, 11, 133, 218, 49, 125, 194, 217, 14, 143, 205, 138, 146, 240, 240, 38, 20, 72, 77]);
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

  describe('#verify', () => {
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

  describe('#open', () => {
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
})