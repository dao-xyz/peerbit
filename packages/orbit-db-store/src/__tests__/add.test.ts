
import assert from 'assert'
import { Store, DefaultOptions, HeadsCache } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore } from "@dao-xyz/orbit-db-keystore"
import { Identities } from '@dao-xyz/orbit-db-identity-provider'
import { Entry, JSON_IO_OPTIONS } from '@dao-xyz/ipfs-log-entry'
import { setTimeout } from 'timers/promises'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

const storage = require('orbit-db-storage-adapter')(require('memdown'))
Object.keys(testAPIs).forEach((IPFS) => {
  describe(`addOperation ${IPFS}`, function () {
    let ipfsd, ipfs, testIdentity, identityStore, store: Store<any, any, any, any>, cacheStore

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config.defaultIpfsConfig, {
      repo: config.defaultIpfsConfig.repo + '-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await storage.createStore('identity')
      const keystore = new Keystore(identityStore)

      cacheStore = await storage.createStore('cache')
      const cache = new Cache(cacheStore)

      testIdentity = await Identities.createIdentity({ id: new Uint8Array([0]), keystore })
      ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1)
      ipfs = ipfsd.api

      const address = 'test-address'
      const options = Object.assign({}, DefaultOptions, { cache })
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

    it('adds an operation and triggers the write event', (done) => {
      const data = { data: 12345 }
      store.events.on('write', (topic, address, entry, heads) => {
        assert.strictEqual(heads.length, 1)
        assert.strictEqual(address, 'test-address')
        assert.deepStrictEqual(entry.data.payload, data)
        assert.strictEqual(store.replicationStatus.progress, 1)
        assert.strictEqual(store.replicationStatus.max, 1)
        assert.strictEqual(store.address.root, store._index.id)
        assert.deepStrictEqual(store._index._index, heads)
        store._cache.getBinary(store.localHeadsPath, HeadsCache).then((localHeads) => {
          localHeads.heads[0].init({
            io: JSON_IO_OPTIONS
          });
          assert.deepStrictEqual(localHeads.heads[0].data.payload, data)
          assert(localHeads.heads[0].equals(heads[0]))
          assert.strictEqual(heads.length, 1)
          assert.strictEqual(localHeads.heads.length, 1)
          store.events.removeAllListeners('write')
          done()
        })
      })
      store._addOperation(data)
    })

    it('adds multiple operations and triggers multiple write events', async () => {
      const writes = 3
      let eventsFired = 0

      store.events.on('write', (topic, address, entry, heads) => {
        eventsFired++
        if (eventsFired === writes) {
          assert.strictEqual(heads.length, 1)
          assert.strictEqual(store.replicationStatus.progress, writes)
          assert.strictEqual(store.replicationStatus.max, writes)
          assert.strictEqual(store._index._index.length, writes)
          store._cache.getBinary(store.localHeadsPath, HeadsCache).then((localHeads) => {
            localHeads.heads[0].init({
              io: JSON_IO_OPTIONS
            });
            assert.deepStrictEqual(localHeads.heads[0].data.payload, store._index._index[2].data.payload)
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
