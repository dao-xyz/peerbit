
import assert from 'assert'
import { Store, DefaultOptions } from '../store.js'
import { default as Cache } from '@dao-xyz/peerbit-cache'
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore"
import { SimpleIndex } from './utils.js'
import { Controller } from 'ipfsd-ctl'
import { IPFS } from 'ipfs-core-types'
import { Ed25519Keypair } from '@dao-xyz/peerbit-crypto'
import { delay, waitFor } from '@dao-xyz/peerbit-time';
import { fileURLToPath } from 'url';
import { jest } from '@jest/globals';
import { v4 as uuid } from 'uuid';

import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs,
  createStore,
  Session
} from '@dao-xyz/peerbit-test-utils'
import { Level } from 'level'


Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication Status (${API})`, function () {
    jest.setTimeout(config.timeout)

    let session: Session, signKey: KeyWithMeta<Ed25519Keypair>, identityStore: Level, cache1: Level, cache2: Level;

    let store1: Store<any>
    let store2: Store<any>
    let index1: SimpleIndex<string>
    let index2: SimpleIndex<string>

    jest.setTimeout(config.timeout);

    beforeAll(async () => {
      session = await Session.connected(1);
      identityStore = await createStore(path.join(__filename, 'identity' + uuid()))

      const keystore = new Keystore(identityStore)
      signKey = await keystore.createEd25519Key()


      cache1 = await createStore(path.join(__filename, 'cache1' + uuid()))
      cache2 = await createStore(path.join(__filename, 'cache2' + uuid()))

    })

    afterAll(async () => {
      await store1?.close()
      await store2?.close()
      session.stop();
      await identityStore?.close()
      await cache1?.close()
      await cache2?.close()

    })

    beforeEach(async () => {
      await cache1.clear();
      await cache2.clear();
      index1 = new SimpleIndex();
      index2 = new SimpleIndex();
      store1 = new Store({ storeIndex: 0 })
      await store1.init(session.peers[0].ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { ...DefaultOptions, resolveCache: () => Promise.resolve(new Cache(cache1)), onUpdate: index1.updateIndex.bind(index1) });
      store2 = new Store({ storeIndex: 0 })
      await store2.init(session.peers[0].ipfs, {
        ...signKey.keypair,
        sign: async (data: Uint8Array) => (await signKey.keypair.sign(data))
      }, { ...DefaultOptions, resolveCache: () => Promise.resolve(new Cache(cache2)), onUpdate: index2.updateIndex.bind(index2) });
    })

    it('has correct status', async () => {
      // init
      assert.deepEqual(store1.replicationStatus, { progress: 0, max: 0 })

      // load
      await store1._addOperation('hello')
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
      await store1._addOperation('hello2')
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
