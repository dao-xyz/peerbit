
import assert from 'assert'
import { Store, DefaultOptions, HeadsCache, IInitializationOptions } from '../store.js'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore"
import { PublicKeyEncryptionResolver, X25519PublicKey } from '@dao-xyz/peerbit-crypto'
import { AccessError } from "@dao-xyz/peerbit-crypto"
import { SimpleIndex } from './utils.js'
import { Address } from '../io.js'
import { Controller } from 'ipfsd-ctl'
import { IPFS } from 'ipfs-core-types'
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { fileURLToPath } from 'url';
import path from 'path';
import { jest } from '@jest/globals';
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs,
  createStore
} from '@dao-xyz/orbit-db-test-utils'
import { Level } from 'level'
import { Entry } from '@dao-xyz/ipfs-log'
import { waitFor } from '@dao-xyz/time'
const API = 'js-ipfs'
describe(`addOperation`, function () {
  let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>, keystore: Keystore, identityStore: Level, store: Store<any>, cacheStore: Level, senderKey: KeyWithMeta<Ed25519Keypair>, recieverKey: KeyWithMeta<Ed25519Keypair>, encryption: PublicKeyEncryptionResolver
  let index: SimpleIndex<string>

  jest.setTimeout(config.timeout);

  const ipfsConfig = Object.assign({}, config, {
    repo: 'repo-entry' + __filenameBase + new Date().getTime()
  })

  beforeAll(async () => {
    identityStore = await createStore(__filenameBase + '/identity')
    keystore = new Keystore(identityStore)

    cacheStore = await createStore(__filenameBase + '/cache')

    signKey = await keystore.createEd25519Key()
    ipfsd = await startIpfs(API, ipfsConfig.daemon1)
    ipfs = ipfsd.api
    index = new SimpleIndex();
    senderKey = await keystore.createEd25519Key()
    recieverKey = await keystore.createEd25519Key()
    encryption = {
      getEncryptionKeypair: () => Promise.resolve(senderKey.keypair),
      getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
        for (let i = 0; i < publicKeys.length; i++) {
          if (publicKeys[i].equals(await X25519PublicKey.from(senderKey.keypair.publicKey))) {
            return {
              index: i,
              keypair: senderKey.keypair
            }
          }
          if (publicKeys[i].equals(await X25519PublicKey.from(recieverKey.keypair.publicKey))) {
            return {
              index: i,
              keypair: recieverKey.keypair
            }
          }
        }
      }
    }


  })

  afterAll(async () => {
    await store?.close()
    ipfsd && await stopIpfs(ipfsd)
    await identityStore?.close()
    await cacheStore?.close()
  })


  it('encrypted entry is appended known key', async () => {
    const data = { data: 12345 }

    let done = false;
    const onWrite = (store: Store<any>, entry: Entry<any>) => {
      try {
        const heads = store.oplog.heads;
        expect(heads.length).toEqual(1)
        assert(Address.isValid(store.address))
        assert.deepStrictEqual(entry.payload.getValue(), data)
        expect(store.replicationStatus.progress).toEqual(1n)
        expect(store.replicationStatus.max).toEqual(1n)
        assert.deepStrictEqual(index._index, heads)
        store._cache.getBinary(store.localHeadsPath, HeadsCache).then(async (localHeads) => {
          if (!localHeads) {
            fail()
          }
          localHeads.heads[0].init({
            encryption: store.oplog._encryption
          });
          await localHeads.heads[0].getPayload();
          assert.deepStrictEqual(localHeads.heads[0].payload.getValue(), data)
          assert(localHeads.heads[0].equals(heads[0]))
          expect(heads.length).toEqual(1)
          expect(localHeads.heads.length).toEqual(1)
          done = true;
        })
      } catch (error) {
        throw error;
      }
    }

    const cache = new Cache(cacheStore)
    const options: IInitializationOptions<any> = { ...DefaultOptions, resolveCache: () => Promise.resolve(cache), onUpdate: index.updateIndex.bind(index), encryption, onWrite }
    store = new Store({ name: 'name' })
    await store.init(ipfs, {
      ...signKey.keypair,
      sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
    }, options);

    await store._addOperation(data, {
      reciever: {
        clock: recieverKey.keypair.publicKey,
        payload: recieverKey.keypair.publicKey,
        signature: recieverKey.keypair.publicKey
      }
    })

    await waitFor(() => done);

  })

  it('encrypted entry is append unkown key', async () => {
    const data = { data: 12345 }
    let done = false;

    const onWrite = (store: Store<any>, entry: Entry<any>) => {
      const heads = store.oplog.heads;
      expect(heads.length).toEqual(1)
      assert(Address.isValid(store.address))
      assert.deepStrictEqual(entry.payload.getValue(), data)
      expect(store.replicationStatus.progress).toEqual(1n)
      expect(store.replicationStatus.max).toEqual(1n)
      assert.deepStrictEqual(index._index, heads)
      store._cache.getBinary(store.localHeadsPath, HeadsCache).then(async (localHeads) => {
        if (!localHeads) {
          fail();
        }

        localHeads.heads[0].init({
          encryption: store.oplog._encryption
        });
        try {
          await localHeads.heads[0].getPayload();
          assert(false);
        } catch (error) {
          expect(error).toBeInstanceOf(AccessError)
        }
        assert(localHeads.heads[0].equals(heads[0]))
        expect(heads.length).toEqual(1)
        expect(localHeads.heads.length).toEqual(1)
        done = true
      })
    }

    const cache = new Cache(cacheStore)
    const options: IInitializationOptions<any> = { ...DefaultOptions, resolveCache: () => Promise.resolve(cache), onUpdate: index.updateIndex.bind(index), encryption, onWrite }
    store = new Store({ name: 'name' })
    await store.init(ipfs, {
      ...signKey.keypair,
      sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
    }, options);

    const reciever = await keystore.createEd25519Key();
    await store._addOperation(data, {
      reciever: {
        clock: undefined,
        payload: reciever.keypair.publicKey,
        signature: reciever.keypair.publicKey,
      }
    })

    await waitFor(() => done);

  })


})

