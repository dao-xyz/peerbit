import { Store, DefaultOptions } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore, SignKeyWithMeta } from "@dao-xyz/orbit-db-keystore"

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'
import { createStore } from './storage'
import { SimpleAccessController } from './utils'


Object.keys(testAPIs).forEach((IPFS) => {
  describe(`Constructor ${IPFS}`, function () {
    let ipfs, signKey: SignKeyWithMeta, identityStore, store: Store<any>, cacheStore

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config, {
      repo: 'repo-entry' + new Date().getTime()
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
      expect(typeof store.options).toEqual('object')
      expect(typeof store.id).toEqual('string')
      expect(typeof store.address).toEqual('object')
      expect(typeof store.dbname).toEqual('string')
      expect(typeof store.events).toEqual('object')
      expect(typeof store._ipfs).toEqual('object')
      expect(typeof store._cache).toEqual('object')
      expect(typeof store.accessController).toEqual('object')
      expect(typeof store._oplog).toEqual('object')
      expect(typeof store._replicationStatus).toEqual('object')
      expect(typeof store._stats).toEqual('object')
      expect(typeof store._loader).toEqual('object')
    })

    it('properly defines a cache', async () => {
      expect(typeof store._cache).toEqual('object')
    })
    it('can clone', async () => {
      const clone = store.clone();
      expect(clone).not.toEqual(store);
    })
  })
})
