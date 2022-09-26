import assert from 'assert'
import { Store, DefaultOptions } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore, SignKeyWithMeta } from "@dao-xyz/orbit-db-keystore"

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
    let ipfs, signKey: SignKeyWithMeta, identityStore, store: Store<any>, cacheStore

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config.defaultIpfsConfig, {
      repo: config.defaultIpfsConfig.repo + '-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await createStore('identity')
      const keystore = new Keystore(identityStore)

      cacheStore = await createStore('cache')
      const cache = new Cache(cacheStore)

      signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
      ipfs = await startIpfs(IPFS, ipfsConfig.daemon1)
      const options = Object.assign({}, DefaultOptions, { resolveCache: () => cache })
      store = new Store({ name: 'name', accessController: new SimpleAccessController() })
      await store.init(ipfs.api, signKey.publicKey, (data) => Keystore.sign(data, signKey), options);

    })

    afterAll(async () => {
      await store?.close()
      ipfs && await stopIpfs(ipfs)
      await identityStore?.close()
      await cacheStore?.close()
    })

    it('creates a new Store instance', async () => {
      assert.strictEqual(typeof store.options, 'object')
      assert.strictEqual(typeof store.id, 'string')
      assert.strictEqual(typeof store.address, 'object')
      assert.strictEqual(typeof store.dbname, 'string')
      assert.strictEqual(typeof store.events, 'object')
      assert.strictEqual(typeof store._ipfs, 'object')
      assert.strictEqual(typeof store._cache, 'object')
      assert.strictEqual(typeof store.accessController, 'object')
      assert.strictEqual(typeof store._oplog, 'object')
      assert.strictEqual(typeof store._replicationStatus, 'object')
      assert.strictEqual(typeof store._stats, 'object')
      assert.strictEqual(typeof store._loader, 'object')
    })

    it('properly defines a cache', async () => {
      assert.strictEqual(typeof store._cache, 'object')
    })
    it('can clone', async () => {
      const clone = store.clone();
      expect(clone).not.toEqual(store);
    })
  })
})
