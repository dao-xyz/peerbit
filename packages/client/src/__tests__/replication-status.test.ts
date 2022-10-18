
import { OrbitDB } from "../orbit-db"

import { EventStore } from "./utils/stores/event-store"

import assert from 'assert'
import rmrf from 'rimraf'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
// @ts-ignore
import { v4 as uuid } from 'uuid';
// Include test utilities
import {
  nodeConfig as config,
  startIpfs,
  stopIpfs,
  testAPIs,
} from '@dao-xyz/peerbit-test-utils'

const dbPath1 = './orbitdb/tests/create-open/1'
const dbPath2 = './orbitdb/tests/create-open/2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication Status (${API})`, function () {
    jest.setTimeout(config.timeout)

    let ipfsd: Controller, ipfs: IPFS, orbitdb1: OrbitDB, orbitdb2: OrbitDB, db: EventStore<string>, replicationTopic: string

    beforeAll(async () => {
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      ipfsd = await startIpfs(API, config.daemon1)
      ipfs = ipfsd.api
      orbitdb1 = await OrbitDB.createInstance(ipfs, { directory: dbPath1 })
      orbitdb2 = await OrbitDB.createInstance(ipfs, { directory: dbPath2 })
      replicationTopic = uuid();
      db = await orbitdb1.open(new EventStore<string>({ name: 'replication status tests' }), replicationTopic
      )
    })

    afterAll(async () => {
      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()

      if (ipfsd)
        await stopIpfs(ipfsd)
    })

    it('has correct initial state', async () => {
      assert.deepEqual(db.store.replicationStatus, { progress: 0, max: 0 })
    })

    it('has correct replication info after load', async () => {
      await db.add('hello')
      await db.store.close()
      await db.store.load()
      assert.deepEqual(db.store.replicationStatus, { progress: 1, max: 1 })
      await db.store.close()
    })

    it('has correct replication info after close', async () => {
      await db.store.close()
      assert.deepEqual(db.store.replicationStatus, { progress: 0, max: 0 })
    })

    it('has correct replication info after sync', async () => {
      await db.store.load()
      await db.add('hello2')

      const db2 = await orbitdb2.open(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db.address), replicationTopic)
      await db2.store.sync(db.store._oplog.heads)

      return new Promise((resolve, reject) => {
        setTimeout(() => {
          try {
            assert.deepEqual(db2.store.replicationStatus, { progress: 2, max: 2 })
            resolve(true)
          } catch (e: any) {
            reject(e)
          }
        }, 100)
      })
    })

    it('has correct replication info after loading from snapshot', async () => {
      await db.store._cache._store.open()
      await db.store.saveSnapshot()
      await db.store.close()
      await db.store.loadFromSnapshot()
      assert.deepEqual(db.store.replicationStatus, { progress: 2, max: 2 })
    })
  })
})
