
const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')
import { Entry } from '@dao-xyz/ipfs-log-entry'

import { OrbitDB } from '../orbit-db'
import { SimpleAccessController } from './utils/access'
import { EventStore, Operation } from './utils/stores/event-store'

// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
  connectPeers,
  waitForPeers,
} = require('orbit-db-test-utils')

const orbitdbPath1 = './orbitdb/tests/replication/1'
const orbitdbPath2 = './orbitdb/tests/replication/2'
const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication (${API})`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1, ipfsd2, ipfs1, ipfs2
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>, db4: EventStore<string>

    let timer

    beforeAll(async () => {
      ipfsd1 = await startIpfs(API, config.daemon1)
      ipfsd2 = await startIpfs(API, config.daemon2)
      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api
      // Connect the peers manually to speed up test times
      const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
      await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
      console.log("Peers connected")
    })

    afterAll(async () => {
      if (ipfsd1)
        await stopIpfs(ipfsd1)

      if (ipfsd2)
        await stopIpfs(ipfsd2)
    })

    beforeEach(async () => {
      clearInterval(timer)

      rmrf.sync(orbitdbPath1)
      rmrf.sync(orbitdbPath2)
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)

      orbitdb1 = await OrbitDB.createInstance(ipfs1, { directory: orbitdbPath1 })
      orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2 })


    })

    afterEach(async () => {
      clearInterval(timer)

      if (db1)
        await db1.drop()

      if (db2)
        await db2.drop()

      if (db3)
        await db3.drop()

      if (db4)
        await db4.drop()

      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()
    })

    it('replicates database of 1 entry', async () => {

      console.log("Waiting for peers to connect")
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      let options = {
        // Set write access for both clients
        accessController: new SimpleAccessController()
      }
      const replicationTopicFn = () => 'x';
      const replicationTopic = replicationTopicFn();
      db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
        , { ...Object.assign({}, options, { directory: dbPath1 }), replicationTopic })
      await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)

      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options, replicationTopic: replicationTopicFn })
      db3 = await orbitdb2.open(new EventStore<string>({ name: 'replication-tests-same-topic', accessController: new SimpleAccessController() }), { ...options, replicationTopic: replicationTopicFn })

      expect(await orbitdb1._ipfs.pubsub.ls()).toStrictEqual([replicationTopic])
      const ls2 = await orbitdb2._ipfs.pubsub.ls();
      expect(ls2).toContain(replicationTopic)
      expect(ls2).toHaveLength(2)

      let finished = false
      db1.add('hello')
      await new Promise((resolve, reject) => {
        let replicatedEventCount = 0
        db2.events.on('replicated', (address, length) => {
          replicatedEventCount++
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          finished = (all === 1)
        })

        timer = setInterval(() => {
          if (finished) {
            clearInterval(timer)
            const entries: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
            try {
              assert.equal(entries.length, 1)
              assert.equal(entries[0].payload.value.value, 'hello')
              assert.equal(replicatedEventCount, 1)
              const allFromDB3 = db3.iterator({ limit: -1 }).collect().length
              assert.equal(allFromDB3, 0) // Same replication topic but different DB (which means no entries should exist) 

            } catch (error) {
              reject(error)
            }
            resolve(true)
          }
        }, 100)
      })
    })

    it('request heads if new database but same topic and connection', async () => {

      console.log("Waiting for peers to connect")
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network

      const replicationTopicFn = () => 'x';
      const replicationTopic = replicationTopicFn();
      db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests', accessController: new SimpleAccessController() })
        , { directory: dbPath1, replicationTopic })
      db2 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests-2', accessController: new SimpleAccessController() })
        , { directory: dbPath1, replicationTopic })
      db1.add('hello')
      db2.add('world')

      await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)

      const options = { directory: dbPath2, sync: true }
      db3 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options, replicationTopic: replicationTopicFn })
      db4 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db2.address), { ...options, replicationTopic: replicationTopicFn })

      expect(await orbitdb1._ipfs.pubsub.ls()).toStrictEqual([replicationTopic])
      const ls2 = await orbitdb2._ipfs.pubsub.ls();
      expect(ls2).toContain(replicationTopic)
      expect(ls2).toHaveLength(2)

      let finished = false
      await new Promise((resolve, reject) => {
        let replicatedEventCount = 0
        db3.events.on('replicated', (address, length) => {
          replicatedEventCount++
          const all = db3.iterator({ limit: -1 }).collect().length + db4.iterator({ limit: -1 }).collect().length
          finished = (all === 2)
        })

        db4.events.on('replicated', (address, length) => {
          replicatedEventCount++
          const all = db3.iterator({ limit: -1 }).collect().length + db4.iterator({ limit: -1 }).collect().length
          finished = (all === 2)
        })



        timer = setInterval(() => {
          if (finished) {
            clearInterval(timer)
            const entries: Entry<Operation<string>>[] = db3.iterator({ limit: -1 }).collect()
            try {
              assert.equal(entries.length, 1)
              assert.equal(entries[0].payload.value.value, 'hello')

            } catch (error) {
              reject(error)
            }

            const entries2: Entry<Operation<string>>[] = db4.iterator({ limit: -1 }).collect()
            try {
              assert.equal(entries2.length, 1)
              assert.equal(entries2[0].payload.value.value, 'world')
            } catch (error) {
              reject(error)
            }

            assert.equal(replicatedEventCount, 2)
            resolve(true)
          }
        }, 100)
      })
    })

  })
})
