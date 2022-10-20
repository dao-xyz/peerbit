
import assert, { rejects } from 'assert'
import { Store, DefaultOptions, HeadsCache } from '../store.js'
import { default as Cache } from '@dao-xyz/peerbit-cache'
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore"
import { SimpleIndex } from './utils.js'
import { Controller } from 'ipfsd-ctl'
import { IPFS } from 'ipfs-core-types'
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { delay, waitFor } from '@dao-xyz/time';
import { fileURLToPath } from 'url';
import { jest } from '@jest/globals';
import path, { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs,
  createStore
} from '@dao-xyz/peerbit-test-utils'
import { Level } from 'level'
import { JSON_ENCODING } from '@dao-xyz/ipfs-log'


Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication Status (${API})`, function () {
    jest.setTimeout(config.timeout)

    let ipfsd: Controller, ipfs: IPFS, signKey: KeyWithMeta<Ed25519Keypair>, identityStore: Level, cache1: Level, cache2: Level;

    let store1: Store<any>
    let store2: Store<any>
    let index1: SimpleIndex<string>
    let index2: SimpleIndex<string>

    jest.setTimeout(config.timeout);

    const ipfsConfig = Object.assign({}, config, {
      repo: 'repo-add' + __filenameBase + new Date().getTime()
    })

    beforeAll(async () => {
      identityStore = await createStore(path.join(__filename, 'identity'))

      const keystore = new Keystore(identityStore)
      signKey = await keystore.createEd25519Key()
      ipfsd = await startIpfs('js-ipfs', ipfsConfig.daemon1)
      ipfs = ipfsd.api

      cache1 = await createStore(path.join(__filename, 'cache1'))
      cache2 = await createStore(path.join(__filename, 'cache2'))

    })

    afterAll(async () => {
      await store1?.close()
      await store2?.close()
      ipfsd && await stopIpfs(ipfsd)
      await identityStore?.close()
      await cache1?.close()
      await cache2?.close()

    })

    beforeEach(async () => {
      await cache1.clear();
      await cache2.clear();
      index1 = new SimpleIndex();
      index2 = new SimpleIndex();
      store1 = new Store({ name: 'name' })
      await store1.init(ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { ...DefaultOptions, resolveCache: () => Promise.resolve(new Cache(cache1)), onUpdate: index1.updateIndex.bind(index1) });
      store2 = new Store({ name: 'name' })
      await store2.init(ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { ...DefaultOptions, resolveCache: () => Promise.resolve(new Cache(cache2)), onUpdate: index2.updateIndex.bind(index2) });
    })
    it('has correct status', async () => {
      // init
      assert.deepEqual(store1.replicationStatus, { progress: 0, max: 0 })

      // load
      await store1._addOperation('hello', { encoding: JSON_ENCODING })
      await delay(3000); // <-- cache is async so we need a to wait a bit so the load actually catches the new entry
      await store1.close()
      await store1.load()
      await waitFor((() => store1.replicationStatus.progress === 1n && store1.replicationStatus.max === 1n));
      await store1.close()

      // close 
      await store1.close()
      assert.deepEqual(store1.replicationStatus, { progress: 0, max: 0 })

      // sync 
      await store1.load()
      await store1._addOperation('hello2', { encoding: JSON_ENCODING })
      await store2.sync(store1._oplog.heads)
      await waitFor((() => store2.replicationStatus.progress === 2n && store2.replicationStatus.max === 2n));

      //  snapshot
      await store1._cache._store.open()
      await store1.saveSnapshot()
      await store1.close()
      await store1.loadFromSnapshot()
      assert.deepEqual(store1.replicationStatus, { progress: 2, max: 2 })
    })

  })
})
