
import assert from 'assert'
import mapSeries from 'p-each-series'
import rmrf from 'rimraf'
import { Entry } from '@dao-xyz/ipfs-log'
import { waitFor } from '@dao-xyz/time'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
import { OrbitDB } from '../orbit-db'
import { SimpleAccessController } from './utils/access'
import { EventStore, Operation } from './utils/stores/event-store'
import { IInitializationOptions, IStoreOptions } from '@dao-xyz/orbit-db-store'

// Include test utilities
const {
  config,
  startIpfs,
  stopIpfs,
  testAPIs,
  connectPeers,
  waitForPeers,
} = require('@dao-xyz/orbit-db-test-utils')

const orbitdbPath1 = './orbitdb/tests/replication/1'
const orbitdbPath2 = './orbitdb/tests/replication/2'
const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication (${API})`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db1: EventStore<string>, db2: EventStore<string>

    let timer: any
    let options: IStoreOptions<any>;

    beforeAll(async () => {
      ipfsd1 = await startIpfs(API, config.daemon1)
      ipfsd2 = await startIpfs(API, config.daemon2)
      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api
      // Connect the peers manually to speed up test times
      const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
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


      options = Object.assign({}, options, { accessControler: new SimpleAccessController(), directory: dbPath1 })
      db1 = await orbitdb1.open(new EventStore<string>({ name: 'a', accessController: new SimpleAccessController() })
        , options)
    })

    afterEach(async () => {
      clearInterval(timer)
      options = {} as any

      if (db1)
        await db1.drop()

      if (db2)
        await db2.drop()

      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()
    })

    it('replicates database of 1 entry', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { directory: dbPath2 })
      let done = false;
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options, onReplicationComplete: () => {
          expect(db2.iterator({ limit: -1 }).collect().length).toEqual(1)

          clearInterval(timer)
          const db1Entries: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
          expect(db1Entries.length).toEqual(1)
          expect(orbitdb1.findReplicators(db1.replicationTopic, true, db1Entries[0].gid)).toContainValues([orbitdb1.id, orbitdb2.id]);
          expect(db1Entries[0].payload.getValue().value).toEqual(value)

          const db2Entries: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
          expect(db2Entries.length).toEqual(1)
          expect(orbitdb2.findReplicators(db2.replicationTopic, true, db2Entries[0].gid)).toContainValues([orbitdb1.id, orbitdb2.id]);
          expect(db2Entries[0].payload.getValue().value).toEqual(value)
          done = true;
        }
      })
      await waitFor(() => orbitdb1._directConnections.size === 1);
      await waitFor(() => orbitdb2._directConnections.size === 1);

      const value = 'hello';
      await db1.add(value)

      await waitFor(() => done);

    });

    it('replicates database of 100 entries', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

      options = Object.assign({}, options, { directory: dbPath2 })

      let done = false
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options, onReplicationComplete: () => {
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          done = (all === entryCount)
        }
      })

      /*  await waitFor(() => orbitdb1._directConnections.size === 1);
       await waitFor(() => orbitdb2._directConnections.size === 1); */

      const entryCount = 100
      const entryArr: number[] = []

      for (let i = 0; i < entryCount; i++) {
        entryArr.push(i)
      }

      const add = (i: number) => db1.add('hello' + i)
      await mapSeries(entryArr, add)

      await waitFor(() => done);
      const entries = db2.iterator({ limit: -1 }).collect()
      expect(entries.length).toEqual(entryCount)
      expect(entries[0].payload.getValue().value).toEqual('hello0')
      expect(entries[entries.length - 1].payload.getValue().value).toEqual('hello99')

    })

    it('emits correct replication info', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

      options = Object.assign({}, options, { directory: dbPath2, sync: true })

      // Test that none of the entries gets into the replication queue twice
      const replicateSet = new Set()

      // Verify that progress count increases monotonically by saving
      // each event's current progress into an array
      const progressEvents: bigint[] = []

      let done = false

      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options,
        onReplicationQueued: (store, entry) => {
          if (!replicateSet.has(entry.hash)) {
            replicateSet.add(entry.hash)
          } else {
            fail(new Error('Shouldn\'t have started replication twice for entry ' + entry.hash + '\n' + entry.payload.getValue().value))
          }
        },
        onReplicationProgress: (store) => {
          progressEvents.push(db2.replicationStatus.progress)

        },

        onReplicationComplete: (store) => {
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          done = (all === entryCount)
        }

      });

      const entryCount = 99

      // Trigger replication
      let adds = []
      for (let i = 0; i < entryCount; i++) {
        adds.push(i)
      }

      await mapSeries(adds, i => db1.add('hello ' + i))

      await waitFor(() => done);

      // All entries should be in the database
      expect(db2.iterator({ limit: -1 }).collect().length).toEqual(entryCount)
      // progress events should increase monotonically
      expect(progressEvents.length).toEqual(entryCount)
      for (const [idx, e] of progressEvents.entries()) {
        expect(e).toEqual(idx + 1)
      }
      // Verify replication status
      expect(db2.replicationStatus.progress).toEqual(entryCount)
      expect(db2.replicationStatus.max).toEqual(entryCount)
      // Verify replicator state
      expect(db2._replicator.tasksRunning).toEqual(0)
      expect(db2._replicator.tasksQueued).toEqual(0)
      expect(db2._replicator.unfinished.length).toEqual(0)
      // Replicator's internal caches should be empty
      expect(db2._replicator._logs.length).toEqual(0)
      expect(Object.keys(db2._replicator._fetching).length).toEqual(0)



    })

    it('emits correct replication info on fresh replication', async () => {
      const entryCount = 512

      // Trigger replication
      const adds = []
      for (let i = 0; i < entryCount; i++) {
        adds.push(i)
      }

      const add = async (i: number) => {
        process.stdout.write("\rWriting " + (i + 1) + " / " + entryCount + " ")
        await db1.add('hello ' + i)
      }

      await mapSeries(adds, add)

      // Open second instance again
      options = {
        directory: dbPath2,
      }

      // Test that none of the entries gets into the replication queue twice
      const replicateSet = new Set()

      // Verify that progress count increases monotonically by saving
      // each event's current progress into an array
      const progressEvents: bigint[] = []


      let replicatedEventCount = 0
      let done = false

      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options, onReplicationQueued: (store, entry) => {
          if (!replicateSet.has(entry.hash)) {
            replicateSet.add(entry.hash)
          } else {
            fail(new Error('Shouldn\'t have started replication twice for entry ' + entry.hash))
          }
        },
        onReplicationProgress: (store, entry) => {
          progressEvents.push(db2.replicationStatus.progress)

        },
        onReplicationComplete: (store) => {
          replicatedEventCount++
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          done = (all === entryCount)
        }
      })


      // All entries should be in the database
      expect(db2.iterator({ limit: -1 }).collect().length).toEqual(entryCount)
      // 'replicated' event should've been received only once
      expect(replicatedEventCount).toEqual(1)
      // progress events should increase monotonically
      expect(progressEvents.length).toEqual(entryCount)
      for (const [idx, e] of progressEvents.entries()) {
        expect(e).toEqual(idx + 1)
      }
      // Verify replication status
      expect(db2.replicationStatus.progress).toEqual(entryCount)
      expect(db2.replicationStatus.max).toEqual(entryCount)
      // Verify replicator state
      expect(db2._replicator.tasksRunning).toEqual(0)
      expect(db2._replicator.tasksQueued).toEqual(0)
      expect(db2._replicator.unfinished.length).toEqual(0)
      // Replicator's internal caches should be empty
      expect(db2._replicator._logs.length).toEqual(0)
      expect(Object.keys(db2._replicator._fetching).length).toEqual(0)

    })

    it('emits correct replication info in two-way replication', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

      const entryCount = 100

      // Trigger replication
      const adds = []
      for (let i = 0; i < entryCount; i++) {
        adds.push(i)
      }

      const add = async (i: number) => {
        process.stdout.write("\rWriting " + (i + 1) + " / " + entryCount + " ")
        await Promise.all([db1.add('hello-1-' + i), db2.add('hello-2-' + i)])
      }

      // Open second instance again
      let options = {
        directory: dbPath2 + '2',
        overwrite: true,
        sync: true,

      }
      // Test that none of the entries gets into the replication queue twice
      const replicateSet = new Set()
      let done = false

      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options, onReplicationComplete: (store) => {
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          done = (all === entryCount * 2)
        },
        onReplicationQueued: (store, entry) => {
          if (!replicateSet.has(entry.hash)) {
            replicateSet.add(entry.hash)
          } else {
            fail(new Error('Shouldn\'t have started replication twice for entry ' + entry.hash))
          }
        }
      })
      expect(db1.address.toString()).toEqual(db2.address.toString())

      await mapSeries(adds, add)
      await waitFor(() => done);

      // Database values should match
      const values1 = db1.iterator({ limit: -1 }).collect()
      const values2 = db2.iterator({ limit: -1 }).collect()
      expect(values1.length).toEqual(values2.length)
      for (let i = 0; i < values1.length; i++) {
        assert(values1[i].equals(values2[i]))
      }
      // All entries should be in the database
      expect(values1.length).toEqual(entryCount * 2)
      expect(values2.length).toEqual(entryCount * 2)
      // Verify replication status
      expect(db2.replicationStatus.progress).toEqual(BigInt(entryCount * 2))
      expect(db2.replicationStatus.max).toEqual(BigInt(entryCount * 2))
      // Verify replicator state
      expect(db2._replicator.tasksRunning).toEqual(0)
      expect(db2._replicator.tasksQueued).toEqual(0)
      expect(db2._replicator.unfinished.length).toEqual(0)
      // Replicator's internal caches should be empty
      expect(db2._replicator._logs.length).toEqual(0)
      expect(Object.keys(db2._replicator._fetching).length).toEqual(0)


    })

  })
})