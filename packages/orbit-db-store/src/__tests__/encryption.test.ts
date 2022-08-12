
import assert from 'assert'
import { Store, DefaultOptions, HeadsCache, IStoreOptions, StoreCryptOptions } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore, KeyWithMeta } from "@dao-xyz/orbit-db-keystore"
import { Identities, Identity } from '@dao-xyz/orbit-db-identity-provider'
import { EntryDataBoxEncrypted } from '@dao-xyz/ipfs-log-entry'
import { Index } from '../store-index'
import { createStore } from './storage'


// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`addOperation ${IPFS}`, function () {
    let ipfsd, ipfs, testIdentity: Identity, keystore: Keystore, identityStore, store: Store<any, any, any, any>, cacheStore, senderKey: KeyWithMeta, recieverKey: KeyWithMeta, crypt: StoreCryptOptions

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config.defaultIpfsConfig, {
      repo: config.defaultIpfsConfig.repo + '-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await createStore('identity')
      keystore = new Keystore(identityStore)

      cacheStore = await createStore('cache')
      const cache = new Cache(cacheStore)

      testIdentity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore })
      ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1)
      ipfs = ipfsd.api

      const address = 'test-address'
      senderKey = await keystore.createKey('sender', 'box');
      recieverKey = await keystore.createKey('sender', 'box');
      crypt = {
        decrypt: (data, sender, _reciever, _replicationTopic) => keystore.decrypt(data, recieverKey.key, sender),
        encrypt: async (data, reciever, _replicationTopic) => {
          return {
            data: await keystore.encrypt(data, senderKey.key, reciever),
            senderPublicKey: await Keystore.getPublicBox(senderKey.key)
          }
        }
      };
      const options: IStoreOptions<any, any, Index<any, any>> & {
        cache: Cache;
      } = Object.assign({}, DefaultOptions, { cache })
      options.crypt = crypt
      store = new Store(ipfs, testIdentity, address, options)

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

    it('entry is encrypted is appended', (done) => {
      const data = { data: 12345 }

      store.events.on('write', (topic, address, entry, heads) => {
        assert.strictEqual(heads.length, 1)
        assert.strictEqual(address, 'test-address')
        assert.deepStrictEqual(entry.data.payload, data)
        assert.strictEqual(store.replicationStatus.progress, 1)
        assert.strictEqual(store.replicationStatus.max, 1)
        assert.strictEqual(store.address.root, store._index.id)
        assert.deepStrictEqual(store._index._index, heads)
        store._cache.getBinary(store.localHeadsPath, HeadsCache).then(async (localHeads) => {
          localHeads.heads[0].init({
            io: store.logOptions.io,
            crypt: store.logOptions.crypt
          });
          assert(localHeads.heads[0].data instanceof EntryDataBoxEncrypted);
          await localHeads.heads[0].data.decrypt();
          assert.deepStrictEqual(localHeads.heads[0].data.payload, data)
          assert(localHeads.heads[0].equals(heads[0]))
          assert.strictEqual(heads.length, 1)
          assert.strictEqual(localHeads.heads.length, 1)
          store.events.removeAllListeners('write')
          done()
        })
      })

      Keystore.getPublicBox(recieverKey.key).then((reciever) => {
        store._addOperation(data, { reciever })
      })

    })


  })
})
