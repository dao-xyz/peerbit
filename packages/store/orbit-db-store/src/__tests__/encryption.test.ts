
import assert from 'assert'
import { Store, DefaultOptions, HeadsCache, StorePublicKeyEncryption, IInitializationOptions } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { BoxKeyWithMeta, Keystore, KeyWithMeta, SignKeyWithMeta } from "@dao-xyz/orbit-db-keystore"
import { createStore } from './storage'
import { X25519PublicKey, SodiumPlus } from 'sodium-plus'
import { AccessError } from '@dao-xyz/encryption-utils'
import { SimpleAccessController, SimpleIndex } from './utils'
import { Address } from '../io'

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`addOperation ${IPFS}`, function () {
    let ipfsd, ipfs, signKey: SignKeyWithMeta, keystore: Keystore, identityStore, store: Store<any>, cacheStore, senderKey: BoxKeyWithMeta, recieverKey: BoxKeyWithMeta, encryption: StorePublicKeyEncryption
    let index: SimpleIndex<string>

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config, {
      repo: 'repo-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await createStore('identity')
      keystore = new Keystore(identityStore)

      cacheStore = await createStore('cache')
      const cache = new Cache(cacheStore)

      signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
      ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1)
      ipfs = ipfsd.api
      index = new SimpleIndex();
      senderKey = await keystore.createKey('sender', BoxKeyWithMeta, undefined, { overwrite: true });
      recieverKey = await keystore.createKey('reciever', BoxKeyWithMeta, undefined, { overwrite: true });
      encryption = (_) => {
        return {
          getEncryptionKey: () => Promise.resolve(senderKey.secretKey),
          getAnySecret: async (publicKeys: X25519PublicKey[]) => {
            for (let i = 0; i < publicKeys.length; i++) {
              if (Buffer.compare(publicKeys[i].getBuffer(), senderKey.publicKey.getBuffer()) === 0) {
                return {
                  index: i,
                  secretKey: senderKey.secretKey
                }
              }
              if (Buffer.compare(publicKeys[i].getBuffer(), recieverKey.publicKey.getBuffer()) === 0) {
                return {
                  index: i,
                  secretKey: recieverKey.secretKey
                }
              }

            }
          }
        }
      };
      const options: IInitializationOptions<any> = Object.assign({}, DefaultOptions, { resolveCache: () => Promise.resolve(cache), onUpdate: index.updateIndex.bind(index) })
      options.encryption = encryption
      store = new Store({ name: 'name', accessController: new SimpleAccessController() })
      await store.init(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), options);

    })

    afterAll(async () => {
      await store?.close()
      ipfsd && await stopIpfs(ipfsd)
      await identityStore?.close()
      await cacheStore?.close()
    })

    afterEach(async () => {
      await store.drop()
      await cacheStore.open()
      await identityStore.open()
    })

    it('encrypted entry is appended known key', (done) => {
      const data = { data: 12345 }

      store.events.on('write', (topic, address, entry, heads) => {
        try {
          expect(heads.length).toEqual(1)
          assert(Address.isValid(address))
          assert.deepStrictEqual(entry.payload.value, data)
          expect(store.replicationStatus.progress).toEqual(1n)
          expect(store.replicationStatus.max).toEqual(1n)
          assert.deepStrictEqual(index._index, heads)
          store._cache.getBinary(store.localHeadsPath, HeadsCache).then(async (localHeads) => {
            localHeads.heads[0].init({
              encoding: store.logOptions.encoding,
              encryption: store.logOptions.encryption
            });
            await localHeads.heads[0].getPayload();
            assert.deepStrictEqual(localHeads.heads[0].payload.value, data)
            assert(localHeads.heads[0].equals(heads[0]))
            expect(heads.length).toEqual(1)
            expect(localHeads.heads.length).toEqual(1)
            store.events.removeAllListeners('write')
            done()
          })
        } catch (error) {
          throw error;
        }
      })

      store._addOperation(data, {
        reciever: {
          clock: recieverKey.publicKey,
          publicKey: recieverKey.publicKey,
          payload: recieverKey.publicKey,
          signature: recieverKey.publicKey
        }
      })

    })

    it('encrypted entry is append unkown key', (done) => {
      const data = { data: 12345 }

      store.events.on('write', (topic, address, entry, heads) => {
        expect(heads.length).toEqual(1)
        assert(Address.isValid(address))
        assert.deepStrictEqual(entry.payload.value, data)
        expect(store.replicationStatus.progress).toEqual(1n)
        expect(store.replicationStatus.max).toEqual(1n)
        assert.deepStrictEqual(index._index, heads)
        store._cache.getBinary(store.localHeadsPath, HeadsCache).then(async (localHeads) => {
          localHeads.heads[0].init({
            encoding: store.logOptions.encoding,
            encryption: store.logOptions.encryption
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
          store.events.removeAllListeners('write')
          done()
        })
      })
      SodiumPlus.auto().then((sodium) => {
        sodium.crypto_box_keypair().then(kp => sodium.crypto_box_publickey(kp)).then((pk) => {
          store._addOperation(data, {
            reciever: {
              clock: undefined,
              publicKey: pk,
              payload: pk,
              signature: pk,
            }
          })
        })
      })
    })
  })
})
