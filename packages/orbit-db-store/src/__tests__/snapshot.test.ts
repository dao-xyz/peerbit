import assert from 'assert'

import Cache from 'orbit-db-cache'
const Keystore = require('orbit-db-keystore')
import IdentityProvider from 'orbit-db-identity-provider'
import { Store, DefaultOptions } from '../store'

// Test utils
const {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} = require('orbit-db-test-utils')

const storage = require('orbit-db-storage-adapter')(require('memdown'))

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`Snapshots ${IPFS}`, function () {
    let ipfsd, ipfs, testIdentity, identityStore, store, cacheStore

    jest.setTimeout(config.timeout)

    const ipfsConfig = Object.assign({}, config.defaultIpfsConfig, {
      repo: config.defaultIpfsConfig.repo + '-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await storage.createStore('identity')
      const keystore = new Keystore(identityStore)

      cacheStore = await storage.createStore('cache')
      const cache = new Cache(cacheStore)

      testIdentity = await IdentityProvider.createIdentity({ id: 'userA', keystore })
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

    test('Saves a local snapshot', async () => {
      const writes = 10

      for (let i = 0; i < writes; i++) {
        await store._addOperation({ step: i })
      }
      const snapshot = await store.saveSnapshot()
      assert.strictEqual(snapshot[0].path.length, 46)
      assert.strictEqual(snapshot[0].hash.length, 46)
      assert.strictEqual(snapshot[0].path, snapshot[0].hash)
      assert.strictEqual(snapshot[0].size > writes * 200, true)
    })

    test('Successfully loads a saved snapshot', async () => {
      const writes = 10

      for (let i = 0; i < writes; i++) {
        await store._addOperation({ step: i })
      }
      await store.saveSnapshot()
      const storeFromSnapshot = await store.loadFromSnapshot()
      assert.strictEqual(storeFromSnapshot.index._index.length, 10)

      for (let i = 0; i < writes; i++) {
        assert.strictEqual(storeFromSnapshot.index._index[i].payload.step, i)
      }
    })
  })
})
