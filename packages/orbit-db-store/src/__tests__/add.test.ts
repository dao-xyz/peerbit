
import assert, { rejects } from 'assert'
import { Store, DefaultOptions, HeadsCache } from '../store'
import { default as Cache } from '@dao-xyz/orbit-db-cache'
import { Keystore, SignKeyWithMeta } from "@dao-xyz/orbit-db-keystore"
import { Entry, JSON_ENCODING_OPTIONS } from '@dao-xyz/ipfs-log-entry'
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
    let ipfsd, ipfs, signKey: SignKeyWithMeta, identityStore, store: Store<any>, cacheStore
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

      signKey = await keystore.getKeyByPath(new Uint8Array([0]), SignKeyWithMeta);
      ipfsd = await startIpfs(IPFS, ipfsConfig.daemon1)
      ipfs = ipfsd.api

      index = new SimpleIndex();
      store = new Store({ name: 'name', accessController: new SimpleAccessController() })
      await store.init(ipfs, signKey.publicKey, (data) => Keystore.sign(data, signKey), Object.assign({}, DefaultOptions, { resolveCache: () => Promise.resolve(cache), onUpdate: index.updateIndex.bind(index) }));
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
        expect(heads.length).toEqual(1)
        assert(Address.isValid(address))
        assert.deepStrictEqual(entry.payload.value, data)
        expect(store.replicationStatus.progress).toEqual(1n)
        expect(store.replicationStatus.max).toEqual(1n)
        assert.deepStrictEqual(index._index, heads)
        store._cache.getBinary(store.localHeadsPath, HeadsCache).then((localHeads) => {
          localHeads.heads[0].init({
            encoding: JSON_ENCODING_OPTIONS
          });
          assert.deepStrictEqual(localHeads.heads[0].payload.value, data)
          assert(localHeads.heads[0].equals(heads[0]))
          expect(heads.length).toEqual(1)
          expect(localHeads.heads.length).toEqual(1)
          store.events.removeAllListeners('write')
          done()
        })
      })
      store._addOperation(data).then((entry) => {
        expect(entry).toBeInstanceOf(Entry)
      }).catch(error => {
        rejects(error);
      })

    })

    it('adds multiple operations and triggers multiple write events', async () => {
      const writes = 3
      let eventsFired = 0

      store.events.on('write', (topic, address, entry, heads) => {
        eventsFired++
        if (eventsFired === writes) {
          expect(heads.length).toEqual(1)
          expect(store.replicationStatus.progress).toEqual(BigInt(writes))
          expect(store.replicationStatus.max).toEqual(BigInt(writes))
          expect(index._index.length).toEqual(writes)
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

  })
})
