
import assert, { rejects } from 'assert'
import { Store, DefaultOptions, HeadsCache } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore } from "@dao-xyz/orbit-db-keystore"
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { JSON_ENCODING_OPTIONS } from '@dao-xyz/ipfs-log-entry'
import { createStore } from './storage'
import { SimpleAccessController, SimpleIndex } from './utils'
import { Address } from '../io'


// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`addOperation ${IPFS}`, function () {
    let ipfsd, ipfs, testIdentity, identityStore, store: Store<any>, cacheStore
    let index: SimpleIndex<string>

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config.defaultIpfsConfig, {
      repo: config.defaultIpfsConfig.repo + '-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await createStore('identity')
      const keystore = new Keystore(identityStore)

      cacheStore = await createStore('cache')
      const cache = new Cache(cacheStore)

      testIdentity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore })
      ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1)
      ipfs = ipfsd.api

      index = new SimpleIndex();
      store = new Store({ name: 'name', accessController: new SimpleAccessController() })
      await store.init(ipfs, testIdentity, Object.assign({}, DefaultOptions, { cache, onUpdate: index.updateIndex.bind(index) }));
      assert(Address.isValid(store.address));
    })

    afterAll(async () => {
      await store?.close()
      ipfsd && await stopIpfs(ipfsd)
      await identityStore?.close()
      await cacheStore?.close()
    })

    afterEach(async () => {
      await store?.drop()
      await cacheStore?.open()
      await identityStore?.open()
    })

    it('adds an operation and triggers the write event', (done) => {
      const data = { data: 12345 }
      store.events.on('write', (topic, address, entry, heads) => {
        assert.strictEqual(heads.length, 1)
        assert(Address.isValid(address))
        assert.deepStrictEqual(entry.payload.value, data)
        assert.strictEqual(store.replicationStatus.progress, 1n)
        assert.strictEqual(store.replicationStatus.max, 1n)
        assert.deepStrictEqual(index._index, heads)
        store._cache.getBinary(store.localHeadsPath, HeadsCache).then((localHeads) => {
          localHeads.heads[0].init({
            encoding: JSON_ENCODING_OPTIONS
          });
          assert.deepStrictEqual(localHeads.heads[0].payload.value, data)
          assert(localHeads.heads[0].equals(heads[0]))
          assert.strictEqual(heads.length, 1)
          assert.strictEqual(localHeads.heads.length, 1)
          store.events.removeAllListeners('write')
          done()
        })
      })
      store._addOperation(data).catch(error => {
        rejects(error);
      })

    })

    it('adds multiple operations and triggers multiple write events', async () => {
      const writes = 3
      let eventsFired = 0

      store.events.on('write', (topic, address, entry, heads) => {
        eventsFired++
        if (eventsFired === writes) {
          assert.strictEqual(heads.length, 1)
          assert.strictEqual(store.replicationStatus.progress, BigInt(writes))
          assert.strictEqual(store.replicationStatus.max, BigInt(writes))
          assert.strictEqual(index._index.length, writes)
          store._cache.getBinary(store.localHeadsPath, HeadsCache).then((localHeads) => {
            localHeads.heads[0].init({
              encoding: JSON_ENCODING_OPTIONS
            });
            assert.deepStrictEqual(localHeads.heads[0].payload.value, index._index[2].payload.value)
            store.events.removeAllListeners('write')
            return Promise.resolve()
          })
        }
      })

      for (let i = 0; i < writes; i++) {
        await store._addOperation({ step: i })
      }
    })

    it('Shows that batch writing is not yet implemented', async () => {
      try {
        await store._addOperationBatch({})
      } catch (e) {
        assert.strictEqual(e.message, 'Not implemented!')
      }
    })
  })
})
