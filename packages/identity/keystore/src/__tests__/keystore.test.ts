
import path from 'path';
import assert from 'assert';
import LRU from 'lru-cache';
import { createStore, Keystore, KeyWithMeta } from '../keystore';
import rmrf from 'rimraf'
import { Level } from 'level';
import { Ed25519Keypair, X25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { jest } from '@jest/globals';
import fs from 'fs-extra'
import { StoreError } from '../errors.js';
import { delay } from '@dao-xyz/time';
import { fixturePath } from './fixture.test.js';
import { dirname } from 'path';
import { fileURLToPath } from 'url';
// @ts-ignore
import { v4 as uuid } from 'uuid';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);
const storagePath = path.join(__dirname, 'signing-keys')
const tempKeyPath = path.join(__dirname, "keystore-test");

jest.setTimeout(600000)

let store: Level;

/* jest.useRealTimers();
; */

describe('keystore', () => {

  beforeAll(async () => {
    await fs.copy(fixturePath, storagePath)
    rmrf.sync(tempKeyPath)

  })

  afterAll(async () => {
    rmrf.sync(storagePath)
    rmrf.sync(tempKeyPath)

  })

  describe('constructor', () => {

    beforeAll(async () => {
      store = store || await createStore(tempKeyPath + '/1') // storagePath
    })

    it('creates a new Keystore instance', async () => {
      const keystore = new Keystore(store)

      expect(typeof keystore.close).toEqual('function')
      expect(typeof keystore.openStore).toEqual('function')
      expect(typeof keystore.hasKey).toEqual('function')
      expect(typeof keystore.createKey).toEqual('function')
      expect(typeof keystore.getKey).toEqual('function')
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



    it('creates a keystore with empty options', async () => {
      let store = await createStore();
      const keystore = new Keystore(store)
      expect(keystore._store.status).toEqual('opening')
    })



    it('creates a keystore with both', async () => {
      let store = await createStore();
      const cache = new LRU({ max: 10 })
      const keystore = new Keystore(store, { cache })
      assert(['open', 'opening'].includes(keystore._store.status))
      expect(keystore._cache === cache)
      expect(keystore._store === store)
    })
  })


  describe('createKey', () => {
    let keystore: Keystore

    beforeAll(async () => {
      keystore = new Keystore(store)
    })

    it('creates a new key ed25519 ', async () => {
      const id = new Uint8Array([1, 2, 4]);
      const key = await keystore.createEd25519Key({ id })
      expect(await keystore.hasKey(id))
      expect(await keystore.hasKey(key.keypair.publicKey))
      expect(await keystore.getKey(id))

      // Also its conversion
      expect(await keystore.hasKey(await X25519PublicKey.from(key.keypair.publicKey)))
      expect(await keystore.getKey(await X25519PublicKey.from(key.keypair.publicKey)))

    })


    it('creates id from key', async () => {
      const key = await keystore.createEd25519Key()
      expect(await keystore.getKey(key.keypair.publicKey))
      expect(await keystore.hasKey(key.keypair.publicKey));
    })



    it('throws an error if key already exist', async () => {
      const id = 'already'
      await keystore.createEd25519Key({ id })
      try {
        await keystore.createEd25519Key({ id })
        fail()
      } catch (e: any) {
      }
    })
    it('throws an error accessing a closed store', async () => {
      const closedKeysStore = new Keystore({ status: 'closed' } as any);
      try {
        const id = 'X'
        await closedKeysStore.createEd25519Key({ id })
      } catch (e: any) {
        expect(e).toBeInstanceOf(StoreError)
        expect((e as StoreError).message).toEqual('Keystore not open')
      }
    })



    afterEach(async () => {
      // await keystore.close()
    })
  })


  it('throws an error if key already exist', async () => {
    const id = 'already'
    const keystore = new Keystore(await createStore(tempKeyPath + '/unique'))
    await keystore.createEd25519Key({ id })
    try {
      await keystore.createEd25519Key({ id })
    } catch (e: any) {
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

    store = store || await createStore(tempKeyPath + '/1') // storagePath

    keystore = new Keystore(store)
    await keystore.waitForOpen();
    await keystore.createEd25519Key({ id: 'YYZ' })
  })

  it('returns true if key exists', async () => {
    const hasKey = await keystore.hasKey('YYZ')
    expect(hasKey).toEqual(true)
  })

  it('returns false if key does not exist', async () => {
    let hasKey = await keystore.hasKey('XXX')
    let getKey = await keystore.getKey('XXX')
    expect(getKey).toBeUndefined();
    expect(hasKey).toEqual(false)

  })


  it('throws an error accessing a closed store', async () => {
    const closedKeysStore = new Keystore({ status: 'closed' } as any);
    try {
      await closedKeysStore.hasKey('XXX')
    } catch (e: any) {
      expect(e).toBeInstanceOf(StoreError)
      expect((e as StoreError).message).toEqual('Keystore not open')
    }
  })

  afterEach(async () => {
    // await keystore.close()
  })
})

describe('getKey', () => {
  let keystore: Keystore, createdKey: KeyWithMeta<Ed25519Keypair>, createdKeyInGroup: KeyWithMeta<Ed25519Keypair>
  beforeAll(async () => {
    store = store || await createStore(tempKeyPath + '/1') // storagePath
    keystore = new Keystore(store)
    createdKey = await keystore.createEd25519Key({ id: 'ZZZ', overwrite: true })
    createdKeyInGroup = await keystore.createEd25519Key({ id: 'YYY', group: 'group', overwrite: true })

  })

  it('gets an existing key', async () => {
    const key = await keystore.getKey('ZZZ')
    expect(key?.keypair).toBeDefined();
  })

  it('gets an existing key in group', async () => {
    const key = await keystore.getKey('YYY', 'group')
    expect(key?.keypair).toBeDefined();
  })

  it('gets key in group by publickey', async () => {
    const key = await keystore.getKey(createdKeyInGroup.keypair.publicKey)
    expect(key?.keypair).toBeDefined();
  })

  it('throws an error upon accessing a non-existant key', async () => {
    try {
      await keystore.getKey('ZZZZ')
    } catch (e: any) {
      assert(true)
    }
  })



  it('throws an error accessing a closed store', async () => {
    const closedKeysStore = new Keystore({ status: 'closed' } as any);
    try {
      await closedKeysStore.getKey('ZZZ')
    } catch (e: any) {
      expect(e).toBeInstanceOf(StoreError)
      expect((e as StoreError).message).toEqual('Keystore not open')
    }
  })

  afterAll(async () => {
    // keystore.close()
  })
})

describe('getKeys', () => {
  let keystore: Keystore, aSignKey: KeyWithMeta<Ed25519Keypair>, aBoxKey: KeyWithMeta<X25519Keypair>, aBox2Key: KeyWithMeta<X25519Keypair>, bSignKey: KeyWithMeta<Ed25519Keypair>

  beforeAll(async () => {

    store = store || await createStore(tempKeyPath + '/1') // storagePath
  })

  it('gets keys by group', async () => {

    keystore = new Keystore(store)
    const group = uuid();
    const group2 = uuid();
    aSignKey = await keystore.createEd25519Key({ id: 'asign', group, overwrite: true })
    aBoxKey = await keystore.createX25519Key({ id: 'abox', group, overwrite: true })
    aBox2Key = await keystore.createX25519Key({ id: 'abox2', group, overwrite: true })
    bSignKey = await keystore.createEd25519Key({ id: 'bsign', group: group2, overwrite: true })


    const keysByGroup = await keystore.getKeys(group)
    expect(keysByGroup).toHaveLength(4); // because aSignKey with be also saved as X25519key
    expect(keysByGroup?.map(k => (k.keypair as (X25519Keypair | Ed25519Keypair)).publicKey.hashCode())).toContainAllValues([aBoxKey, aBox2Key, aSignKey, await KeyWithMeta.toX25519(aSignKey)].map(k => (k.keypair as (X25519Keypair | Ed25519Keypair)).publicKey.hashCode()));
    const keysByType = await keystore.getKeys(group2)
    expect(keysByType?.map(k => (k.keypair as (X25519Keypair | Ed25519Keypair)).publicKey.hashCode())).toContainAllValues([bSignKey, await KeyWithMeta.toX25519(bSignKey)].map(k => (k.keypair as (X25519Keypair | Ed25519Keypair)).publicKey.hashCode()));

  })


  /*  await new Promise((resolve) => {
         setTimeout(() => {
           resolve(true);
         }, 3000);
       })*/
  /* 
  await keystore.close(); */
  /* const createdKey = await keystore.createKey('signing', undefined, { overwrite: true })
  const y = deserialize(serialize(createdKey), KeyWithMeta); */
  /* await keystore.close();  */ //
  /* 
    describe(KeyWithMeta<Ed25519Keypair>, () => {
      let keystore: Keystore, key: KeyWithMeta<Ed25519Keypair>, signingStore
  
      beforeAll(async () => {
  
        jest.setTimeout(10000)
        signingStore = await createStore(storagePath)
        keystore = new Keystore(signingStore) // 
        
        key = await keystore.getKeyByPath('signing')
        const x = 123;
      })
  
      it('signs data', async () => {
        const signature = await Keystore.sign(Buffer.from('data data data'), key)
        expect(signature).toMatchSnapshot('signature');
      })
  
      it('throws an error if no key is passed', async () => {
        try {
          await Keystore.sign(Buffer.from('data data data'), null)
        } catch (e: any) {
          assert(true)
        }
      })
  
      it('throws an error if no data is passed', async () => {
        try {
          await Keystore.sign(null, key)
        } catch (e: any) {
          assert(true)
        }
      })
  
      afterAll(async () => {
        signingStore.close()
      })
    }) */



  /* describe('verify', () => {
    jest.setTimeout(5000)
    let keystore: Keystore, signingStore, publicKey: Ed25519PublicKey, key: KeyWithMeta<Ed25519Keypair>

    beforeAll(async () => {
      signingStore = await createStore(storagePath)
      keystore = new Keystore(signingStore)
      key = await keystore.getKeyByPath('signing')
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
      } catch (e: any) {
        error = e.message
      }
      expect(error).toEqual('Keystore: No store found to open')
    })

    afterEach(async () => {
      signingStore.close()
    })
  })

  describe('encryption', () => {
    describe(() => {
      let keystore: Keystore, keyA: keyB: encryptStore

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

  }) */

})