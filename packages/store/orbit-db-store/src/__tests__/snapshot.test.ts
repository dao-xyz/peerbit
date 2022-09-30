import assert from 'assert'

import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore';
import { Store, DefaultOptions } from '../store'
import { Entry } from '@dao-xyz/ipfs-log-entry';
import { createStore } from './storage';
import { SimpleAccessController, SimpleIndex } from './utils';

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs
} from '@dao-xyz/orbit-db-test-utils'

Object.keys(testAPIs).forEach((IPFS) => {
  describe(`Snapshots ${IPFS}`, function () {
    let ipfsd, ipfs, signKey: SignKeyWithMeta, identityStore, store: Store<any>, cacheStore
    let index: SimpleIndex<string>
    jest.setTimeout(config.timeout)

    const ipfsConfig = Object.assign({}, config, {
      repo: 'repo-entry' + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await createStore('identity')
      const keystore = new Keystore(identityStore)

      cacheStore = await createStore('cache')
      const cache = new Cache(cacheStore)

      signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
      ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1)
      ipfs = ipfsd.api

      index = new SimpleIndex();
      const options = Object.assign({}, DefaultOptions, { resolveCache: () => Promise.resolve(cache), onUpdate: index.updateIndex.bind(index) })
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

    it('Saves a local snapshot', async () => {
      const writes = 10

      for (let i = 0; i < writes; i++) {
        await store._addOperation({ step: i })
      }
      const snapshot = await store.saveSnapshot()
      expect(snapshot[0].path.length).toEqual(46)
      expect(snapshot[0].cid.toString().length).toEqual(46)
      expect(snapshot[0].path).toEqual(snapshot[0].cid.toString())
      assert.strictEqual(snapshot[0].size > writes * 200, true)
    })

    it('Successfully loads a saved snapshot', async () => {
      const writes = 10

      for (let i = 0; i < writes; i++) {
        await store._addOperation({ step: i })
      }
      await store.saveSnapshot()
      index._index = [];
      await store.loadFromSnapshot()
      expect(index._index.length).toEqual(10)

      for (let i = 0; i < writes; i++) {
        assert.strictEqual((index._index[i] as Entry<any>).payload.value.step, i)
      }
    })

    // TODO test resume unfishid replication
  })
})
