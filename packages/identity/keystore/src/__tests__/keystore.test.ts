
import path from 'path';
import assert from 'assert';
import LRU from 'lru-cache';
import { BoxKeyWithMeta, createStore, Keystore, KeyWithMeta, SignKeyWithMeta } from '../keystore';
import rmrf from 'rimraf'
import { waitFor } from '@dao-xyz/time';
import { Level } from 'level';
import { Ed25519PublicKey, X25519PublicKey, X25519SecretKey } from 'sodium-plus';

import fs from 'fs-extra'
let store: Level;
const fixturePath = path.join('packages/identity/orbit-db-keystore/src/__tests__', 'fixtures', 'signingKeys')
const storagePath = path.join('packages/identity/orbit-db-keystore/src/__tests__', 'signingKeys')
const upgradePath = path.join('packages/identity/orbit-db-keystore/src/__tests__', 'upgrade')
const tempKeyPath = "packages/identity/orbit-db-keystore/src/__tests__/keystore-test";
jest.useRealTimers();
jest.setTimeout(100000);

describe('keystore', () => {

  beforeAll(async () => {
    await fs.copy(fixturePath, storagePath)
    rmrf.sync(tempKeyPath)
    store = await createStore(tempKeyPath) // storagePath

  })

  afterAll(async () => {
    rmrf.sync(storagePath)
    rmrf.sync(upgradePath)
    rmrf.sync(tempKeyPath)

  })

  describe('constructor', () => {
    it('creates a new Keystore instance', async () => {
      const keystore = new Keystore(store)

      expect(typeof keystore.close).toEqual('function')
      expect(typeof keystore.openStore).toEqual('function')
      expect(typeof keystore.hasKey).toEqual('function')
      expect(typeof keystore.createKey).toEqual('function')
      expect(typeof keystore.getKeyByPath).toEqual('function')
    })

    it('assigns this._store', async () => {
      const keystore = new Keystore(store)
      // Loose check for leveldownishness
      assert(['open', 'opening'].includes(keystore._store.status))
    })

    it('assigns this.cache with default of 100', async () => {
      const keystore = new Keystore(store)
      expect(keystore._cache.max).toEqual(100)
    })

    it('creates a proper leveldown / level-js store if not passed a store', async () => {
      const keystore = new Keystore()
      expect(keystore._store.status).toEqual('opening')
      await keystore.close()
    })

    it('creates a keystore with empty options', async () => {
      const keystore = new Keystore({})
      expect(keystore._store.status).toEqual('opening')
      await keystore.close()
    })

    it('creates a keystore with only cache', async () => {
      const cache = new LRU({ max: 10 })
      const keystore = new Keystore({ cache })
      expect(keystore._store.status).toEqual('opening')
      assert(keystore._cache === cache)
      await keystore.close()
    })

    it('creates a keystore with both', async () => {
      const cache = new LRU({ max: 10 })
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
      const id = 'a new key'
      await keystore.createKey(id, SignKeyWithMeta)
      const hasKey = await keystore.hasKey(id, SignKeyWithMeta)
      expect(hasKey).toEqual(true)
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.createKey(undefined, undefined)
      } catch (e) {
        assert(true)
      }
    })
    it('throws an error if key already exist', async () => {
      const id = 'already'
      await keystore.createKey(id, SignKeyWithMeta)
      try {
        await keystore.createKey(id, SignKeyWithMeta)
      } catch (e) {
        assert(true)
      }
    })
    it('throws an error accessing a closed store', async () => {
      try {
        const id = 'X'

        await store.close()
        await keystore.createKey(id, SignKeyWithMeta)
      } catch (e) {
        assert(true)
      }
    })



    afterEach(async () => {
      // await keystore.close()
    })
  })

  describe('saveKey', () => {
    let keystore: Keystore

    beforeEach(async () => {
      keystore = new Keystore(store)
      if (store.status !== 'open') {
        await store.open()
      }
    })

    it('can overwrite if secret key is missing', async () => {
      const id = 'overwrite key'
      let keyWithMeta = new BoxKeyWithMeta({
        secretKey: undefined,
        publicKey: new X25519PublicKey(Buffer.from(new Array(32).fill(0))),
        timestamp: BigInt(+new Date),
        group: '_'
      });
      let savedKey = await keystore.saveKey(keyWithMeta, id)
      assert(!savedKey.secretKey);
      keyWithMeta = new BoxKeyWithMeta({
        secretKey: new X25519SecretKey(Buffer.from(new Array(32).fill(0))),
        publicKey: new X25519PublicKey(Buffer.from(new Array(32).fill(0))),
        timestamp: keyWithMeta.timestamp,
        group: '_'
      });
      savedKey = await keystore.saveKey(keyWithMeta, id)
      assert(!!savedKey.secretKey)
    })

    it('will return secret key if missing when saving', async () => {
      const id = 'overwrite key 2'
      let keyWithMeta = new BoxKeyWithMeta({
        secretKey: new X25519SecretKey(Buffer.from(new Array(32).fill(0))),
        publicKey: new X25519PublicKey(Buffer.from(new Array(32).fill(0))),
        timestamp: BigInt(+new Date),
        group: '_'
      });
      let savedKey = await keystore.saveKey(keyWithMeta, id)
      assert(!!savedKey.secretKey);
      keyWithMeta = new BoxKeyWithMeta({
        secretKey: undefined,
        publicKey: new X25519PublicKey(Buffer.from(new Array(32).fill(0))),
        timestamp: keyWithMeta.timestamp,
        group: '_'
      });
      savedKey = await keystore.saveKey(keyWithMeta, id)
      assert(!!savedKey.secretKey)
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.createKey(undefined, undefined)
      } catch (e) {
        assert(true)
      }
    })
    it('throws an error if key already exist', async () => {
      const id = 'already'
      await keystore.createKey(id, SignKeyWithMeta)
      try {
        await keystore.createKey(id, SignKeyWithMeta)
      } catch (e) {
        assert(true)
      }
    })
    it('throws an error accessing a closed store', async () => {
      try {
        const id = 'X'

        await store.close()
        await keystore.createKey(id, SignKeyWithMeta)
      } catch (e) {
        assert(true)
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
      await keystore.createKey('YYZ', SignKeyWithMeta)
    })

    it('returns true if key exists', async () => {
      const hasKey = await keystore.hasKey('YYZ', SignKeyWithMeta)
      expect(hasKey).toEqual(true)
    })

    it('returns false if key does not exist', async () => {
      let hasKey
      try {
        hasKey = await keystore.hasKey('XXX', SignKeyWithMeta)
      } catch (e) {
        expect(hasKey).toEqual(true)
      }
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.hasKey(undefined, undefined)
      } catch (e) {
        assert(true)
      }
    })

    it('throws an error accessing a closed store', async () => {
      try {
        await store.close()
        await keystore.hasKey('XXX', SignKeyWithMeta)
      } catch (e) {
        assert(true)
      }
    })

    afterEach(async () => {
      // await keystore.close()
    })
  })

  describe('getKey', () => {
    let keystore: Keystore, createdKey: SignKeyWithMeta
    beforeAll(async () => {
      if (store.status !== 'open') {
        await store.open()
      }
      keystore = new Keystore(store)
      createdKey = await keystore.createKey('ZZZ', SignKeyWithMeta)
    })

    it('gets an existing key', async () => {
      const key = await keystore.getKeyByPath('ZZZ', SignKeyWithMeta)
      expect(key.publicKey.getLength()).toEqual(32)
      expect(key.secretKey.getLength()).toEqual(64)
    })

    it('throws an error upon accessing a non-existant key', async () => {
      try {
        await keystore.getKeyByPath('ZZZZ', SignKeyWithMeta)
      } catch (e) {
        assert(true)
      }
    })

    it('throws an error upon not receiving an ID', async () => {
      try {
        await keystore.getKeyByPath(undefined, undefined)
      } catch (e) {
        assert(true)
      }
    })

    it('throws an error accessing a closed store', async () => {
      try {
        await store.close()
        await keystore.getKeyByPath('ZZZ', SignKeyWithMeta)
      } catch (e) {
        assert(true)
      }
    })

    afterAll(async () => {
      // keystore.close()
    })
  })

  describe('getKeys', () => {
    let keystore: Keystore, aSignKey: KeyWithMeta, aBoxKey: KeyWithMeta, aBox2Key: KeyWithMeta, bSignKey: KeyWithMeta

    it('gets keys by group amd type', async () => {

      keystore = new Keystore(store)
      aSignKey = await keystore.createKey('asign', SignKeyWithMeta, 'group', { overwrite: true })
      aBoxKey = await keystore.createKey('abox', BoxKeyWithMeta, 'group', { overwrite: true })
      aBox2Key = await keystore.createKey('abox2', BoxKeyWithMeta, 'group', { overwrite: true })
      bSignKey = await keystore.createKey('bsign', SignKeyWithMeta, 'group2', { overwrite: true })


      const keysByGroup = await keystore.getKeys('group')
      expect(keysByGroup).toHaveLength(3);
      expect(keysByGroup.map(k => (k as (BoxKeyWithMeta | SignKeyWithMeta)).publicKey.toString('hex'))).toContainAllValues([aBoxKey, aBox2Key, aSignKey].map(k => (k as (BoxKeyWithMeta | SignKeyWithMeta)).publicKey.toString('hex')));
      const keysByType = await keystore.getKeys('group', BoxKeyWithMeta)
      expect(keysByType.map(k => (k as (BoxKeyWithMeta | SignKeyWithMeta)).publicKey.toString('hex'))).toContainAllValues([aBoxKey, aBox2Key].map(k => (k as (BoxKeyWithMeta | SignKeyWithMeta)).publicKey.toString('hex')));

    })
  })

  describe(SignKeyWithMeta, () => {
    let keystore: Keystore, key: SignKeyWithMeta, signingStore

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
      /* const createdKey = await keystore.createKey('signing', SignKeyWithMeta, undefined, { overwrite: true })
      const y = deserialize(serialize(createdKey), KeyWithMeta); */
      key = await keystore.getKeyByPath('signing', SignKeyWithMeta)
      /* await keystore.close();  */ //
      const x = 123;
    })

    it('signs data', async () => {
      const signature = await Keystore.sign(Buffer.from('data data data'), key)
      expect(signature).toMatchSnapshot('signature');
    })

    it('throws an error if no key is passed', async () => {
      try {
        await Keystore.sign(Buffer.from('data data data'), null)
      } catch (e) {
        assert(true)
      }
    })

    it('throws an error if no data is passed', async () => {
      try {
        await Keystore.sign(null, key)
      } catch (e) {
        assert(true)
      }
    })

    afterAll(async () => {
      signingStore.close()
    })
  })

  describe('verify', () => {
    jest.setTimeout(5000)
    let keystore: Keystore, signingStore, publicKey: Ed25519PublicKey, key: SignKeyWithMeta

    beforeAll(async () => {
      signingStore = await createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKeyByPath('signing', SignKeyWithMeta)
      publicKey = key.publicKey
    })

    it('verifies content', async () => {
      const signature = '4FAJrjPESeuDK5AhhHVmrCdVbb6gTqczxex1CydLfCHH4kP4CBmoMLfH6UxLF2UmPkisNMU15RVHo63NbWiNvyyb2f4h8x5cKWtQrHY3mUL'
      try {
        const verified = await Keystore.verify(Buffer.from(signature), publicKey, Buffer.from('data data data'))
        expect(verified).toEqual(true)
      } catch (error) {
        const x = 123;
      }
    })

    it('verifies content with cache', async () => {
      const data = new Uint8Array(Buffer.from('data'.repeat(1024 * 1024)))
      const sig = await Keystore.sign(Buffer.from(data), key)
      const startTime = new Date().getTime()
      await Keystore.verify(sig, publicKey, Buffer.from(data))
      const first = new Date().getTime()
      await Keystore.verify(sig, publicKey, Buffer.from(data))
      const after = new Date().getTime()
      console.log('First pass:', first - startTime, 'ms', 'Cached:', after - first, 'ms')
      assert.strictEqual(first - startTime > after - first, true)
    })

    it('does not verify content with bad signature', async () => {
      const signature = 'xxxxxx'
      const verified = await Keystore.verify((new Uint8Array(Buffer.from(signature))), publicKey, Buffer.from('data data data'))
      expect(verified).toEqual(false)
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
      expect(signingStore.status).toEqual('open')
    })

    it('fails when no store', async () => {
      let error = false
      try {
        keystore._store = undefined
        await keystore.openStore()
      } catch (e) {
        error = e.message
      }
      expect(error).toEqual('Keystore: No store found to open')
    })

    afterEach(async () => {
      signingStore.close()
    })
  })

  describe('encryption', () => {
    describe(BoxKeyWithMeta, () => {
      let keystore: Keystore, keyA: BoxKeyWithMeta, keyB: BoxKeyWithMeta, encryptStore

      beforeAll(async () => {
        encryptStore = await createStore(storagePath)
        keystore = new Keystore(encryptStore) // 

        await keystore.createKey('box-a', BoxKeyWithMeta);
        await keystore.createKey('box-b', BoxKeyWithMeta);
        keyA = await keystore.getKeyByPath('box-a', BoxKeyWithMeta)
        keyB = await keystore.getKeyByPath('box-b', BoxKeyWithMeta)

      })

      it('encrypts/decrypts', async () => {
        const data = new Uint8Array([1, 2, 3]);
        const encrypted = await keystore.encrypt(data, keyA, keyB.publicKey);
        const decrypted = await keystore.decrypt(encrypted, keyB, keyA.publicKey)
        assert.deepStrictEqual(data, decrypted);
      })


      afterAll(async () => {
        encryptStore.close()
      })
    })

  })

})