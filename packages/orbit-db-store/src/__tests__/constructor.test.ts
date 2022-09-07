import assert from 'assert'
import { Store, DefaultOptions } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore } from "@dao-xyz/orbit-db-keystore"
import { Identities, Identity } from '@dao-xyz/orbit-db-identity-provider'

// Test utils
import {
  config,
  testAPIs,
  startIpfs,
  stopIpfs
} from 'orbit-db-test-utils'
import { createStore } from './storage'
import { SimpleAccessController } from './utils'


Object.keys(testAPIs).forEach((IPFS) => {
  describe(`Constructor ${IPFS}`, function () {
    let ipfs, testIdentity: Identity, identityStore, store: Store<any>, storeWithCache: Store<any>, cacheStore

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
      ipfs = await startIpfs(IPFS, ipfsConfig.daemon1)

      store = new Store({ name: 'name', accessController: new SimpleAccessController() })
      store.init(ipfs, testIdentity, DefaultOptions);
      const options = Object.assign({}, DefaultOptions, { cache })
      storeWithCache = new Store({ name: 'name', accessController: new SimpleAccessController() })

      await storeWithCache.init(ipfs, testIdentity, options);

    })

    afterAll(async () => {
      await store?.close()
      await storeWithCache?.close()
      ipfs && await stopIpfs(ipfs)
      await identityStore?.close()
      await cacheStore?.close()
    })

    it('creates a new Store instance', async () => {
      assert.strictEqual(typeof store.options, 'object')
      assert.strictEqual(typeof store.id, 'string')
      assert.strictEqual(typeof store.address, 'string')
      assert.strictEqual(typeof store.dbname, 'string')
      assert.strictEqual(typeof store.events, 'object')
      assert.strictEqual(typeof store._ipfs, 'object')
      assert.strictEqual(typeof store._cache, 'undefined')
      assert.strictEqual(typeof store.access, 'object')
      assert.strictEqual(typeof store._oplog, 'object')
      assert.strictEqual(typeof store._replicationStatus, 'object')
      assert.strictEqual(typeof store._stats, 'object')
      assert.strictEqual(typeof store._loader, 'object')
    })

    it('properly defines a cache', async () => {
      assert.strictEqual(typeof storeWithCache._cache, 'object')
    })
  })
})
