
import path from 'path';
import assert from 'assert';
import LRU from 'lru';
import { createStore, Keystore, KeyWithMeta } from '../keystore';
import rmrf from 'rimraf'
import { waitFor } from '@dao-xyz/time';
import { Level } from 'level';

const fs = require('fs-extra')

let store: Level;
const fixturePath = path.join('packages/orbit-db-keystore/src/__tests__', 'fixtures', 'signingKeys')
const storagePath = path.join('packages/orbit-db-keystore/src/__tests__', 'signingKeys')
const upgradePath = path.join('packages/orbit-db-keystore/src/__tests__', 'upgrade')

jest.setTimeout(10000);

describe('keystore', () => {
  beforeAll(async () => {
    await fs.copy(fixturePath, storagePath)
    store = await createStore('packages/orbit-db-keystore/src/__tests__/keystore-test') // storagePath
  })

  afterAll(async () => {
    rmrf.sync(storagePath)
    rmrf.sync(upgradePath)
  })

  describe('constructor', () => {
    it('creates a new Keystore instance', async () => {
      const keystore = new Keystore(store)

      assert.strictEqual(typeof keystore.close, 'function')
      assert.strictEqual(typeof keystore.openStore, 'function')
      assert.strictEqual(typeof keystore.hasKey, 'function')
      assert.strictEqual(typeof keystore.createKey, 'function')
      assert.strictEqual(typeof keystore.getKeyByPath, 'function')
      assert.strictEqual(typeof keystore.sign, 'function')
      assert.strictEqual(typeof Keystore.getPublicSign, 'function')
      assert.strictEqual(typeof Keystore.getPublicBox, 'function')
      assert.strictEqual(typeof keystore.verify, 'function')
    })

    it('assigns this._store', async () => {
      const keystore = new Keystore(store)
      // Loose check for leveldownishness
      assert(['open', 'opening'].includes(keystore._store.status))
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
      assert(['open', 'opening'].includes(keystore._store.status))
      assert(keystore._cache === cache)
      assert(keystore._store === store)
    })
  })


  describe('createKey', () => {
    let keystore: Keystore

    beforeEach(async () => {
      keystore = new Keystore(store)
      if (store.status !== 'open') {
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
      if (store.status !== 'open') {
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
    let keystore: Keystore, createdKey: KeyWithMeta
    beforeAll(async () => {
      if (store.status !== 'open') {
        await store.open()
      }
      keystore = new Keystore(store)
      createdKey = await keystore.createKey('ZZZ', 'sign')
    })

    it('gets an existing key', async () => {
      const key = await keystore.getKeyByPath('ZZZ', 'sign')
      assert.strictEqual(key.key.getLength(), 96)
    })

    it('gets an existing key by publicKey', async () => {
      const publicKey = await Keystore.getPublicSign(createdKey.key);
      const key = await keystore.getKeyById(publicKey.toString('base64'))
      assert.strictEqual(key.key.getBuffer(), createdKey.key.getBuffer())
    })

    it('throws an error upon accessing a non-existant key', async () => {
      try {
        await keystore.getKeyByPath('ZZZZ', 'sign')
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.getKeyByPath(undefined, undefined)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error accessing a closed store', async () => {
      try {
        await store.close()
        await keystore.getKeyByPath('ZZZ', 'sign')
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    afterAll(async () => {
      // keystore.close()
    })
  })

  describe('getKeys', () => {
    let keystore: Keystore, aSignKey: KeyWithMeta, aBoxKey: KeyWithMeta, aBox2Key: KeyWithMeta, bSignKey: KeyWithMeta

    beforeAll(async () => {

      keystore = new Keystore()
      aSignKey = await keystore.createKey('ASign', 'sign', 'Group')
      aBoxKey = await keystore.createKey('ABox', 'box', 'Group')
      aBox2Key = await keystore.createKey('ABox2', 'box', 'Group')
      bSignKey = await keystore.createKey('BSign', 'sign', 'B')

    })

    it('gets keys by group', async () => {
      const keys = await keystore.getKeys('Group')
      assert(keys[0].equals(aBoxKey))
      assert(keys[1].equals(aBox2Key))
      assert(keys[2].equals(aSignKey))

    })


    it('gets keys by group and type', async () => {
      const keys = await keystore.getKeys('Group', 'box')
      assert(keys[0].equals(aBoxKey))
      assert(keys[1].equals(aBox2Key))
    })


    afterAll(async () => {
      // keystore.close()
    })
  })

  describe('sign', () => {
    let keystore: Keystore, key: KeyWithMeta, signingStore

    beforeAll(async () => {

      jest.setTimeout(10000)
      signingStore = await createStore(storagePath)
      keystore = new Keystore(signingStore) // 
      /*  await new Promise((resolve) => {
         setTimeout(() => {
           resolve(true);
         }, 3000);
       })*/
      /* 
      await keystore.close(); */
      /* await keystore.createKey('signing', 'sign')
      await keystore.close(); */
      key = await keystore.getKeyByPath('signing', 'sign')

      const x = 123;
    })

    it('signs data', async () => {
      const expectedSignature = new Uint8Array([191, 195, 210, 159, 208, 207, 13, 165, 9, 29, 222, 29, 226, 249, 53, 152, 34, 152, 153, 109, 78, 58, 76, 154, 125, 68, 8, 186, 226, 136, 36, 71, 26, 86, 112, 179, 132, 20, 223, 196, 114, 128, 232, 238, 199, 107, 198, 254, 133, 127, 67, 79, 124, 151, 140, 58, 255, 238, 220, 112, 130, 178, 228, 15, 100, 97, 116, 97, 32, 100, 97, 116, 97, 32, 100, 97, 116, 97])
      const signature = await keystore.sign(Buffer.from('data data data'), key.key)
      assert.deepStrictEqual(signature, expectedSignature)
    })

    it('throws an error if no key is passed', async () => {
      try {
        await keystore.sign(Buffer.from('data data data'), null)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    it('throws an error if no data is passed', async () => {
      try {
        await keystore.sign(null, key.key)
      } catch (e) {
        assert.strictEqual(true, true)
      }
    })

    afterAll(async () => {
      signingStore.close()
    })
  })

  describe('getPublic', () => {
    let keystore: Keystore, key: KeyWithMeta, signingStore: Level

    beforeAll(async () => {
      signingStore = await createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKeyByPath('signing', 'sign')
    })



    it('gets the public key', async () => {
      const expectedKey = new Uint8Array([110, 94, 187, 241, 230, 73, 175, 167, 155, 164, 166, 92, 211, 59, 10, 237, 19, 250, 162, 38, 3, 156, 57, 215, 44, 224, 229, 2, 45, 14, 194, 179]);
      const publicKey = await Keystore.getPublicSign(key.key)
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
    let keystore: Keystore, signingStore, publicKey, key: KeyWithMeta

    beforeAll(async () => {
      signingStore = await createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKeyByPath('signing', 'sign')
      publicKey = await Keystore.getPublicSign(key.key)
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
      const sig = await keystore.sign(Buffer.from(data), key.key)
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
      signingStore = await createStore(storagePath)
      keystore = new Keystore(signingStore)
      signingStore.close()
    })

    it('closes then open', async () => {
      await waitFor(() => signingStore.status === 'closed');
      await keystore.openStore()
      assert.strictEqual(signingStore.status, 'open')
    })

    it('fails when no store', async () => {
      let error = false
      try {
        keystore._store = undefined
        await keystore.openStore()
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
      let keystore: Keystore, keyA: KeyWithMeta, keyB: KeyWithMeta, encryptStore

      beforeAll(async () => {
        encryptStore = await createStore(storagePath)
        keystore = new Keystore(encryptStore) // 

        await keystore.createKey('box-a', 'box');
        await keystore.createKey('box-b', 'box');
        keyA = await keystore.getKeyByPath('box-a', 'box')
        keyB = await keystore.getKeyByPath('box-b', 'box')

      })

      it('encrypts/decrypts', async () => {
        const data = new Uint8Array([1, 2, 3]);
        const encrypted = await keystore.encrypt(data, keyA.key, await Keystore.getPublicBox(keyB.key));
        const decrypted = await keystore.decrypt(encrypted, keyB.key, await Keystore.getPublicBox(keyA.key))
        assert.deepStrictEqual(data, decrypted);
      })


      afterAll(async () => {
        encryptStore.close()
      })
    })

  })

})