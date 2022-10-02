
import assert from 'assert'
const mapSeries = require('p-each-series')
import rmrf from 'rimraf'
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { DirectChannel } from '@dao-xyz/ipfs-pubsub-direct-channel'
import { delay, waitFor } from '@dao-xyz/time'

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
} = require('@dao-xyz/orbit-db-test-utils')

const orbitdbPath1 = './orbitdb/tests/replication/1'
const orbitdbPath2 = './orbitdb/tests/replication/2'
const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication (${API})`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1, ipfsd2, ipfs1, ipfs2
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db1: EventStore<string>, db2: EventStore<string>

    let timer
    let options

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


      options = Object.assign({}, options, { accessControler: new SimpleAccessController(), directory: dbPath1 })
      db1 = await orbitdb1.open(new EventStore<string>({ name: 'a', accessController: new SimpleAccessController() })
        , options)
    })

    afterEach(async () => {
      clearInterval(timer)
      options = {}

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
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), options)
      await waitFor(() => orbitdb1._directConnections.size === 1);
      await waitFor(() => orbitdb2._directConnections.size === 1);

      let finished = false

      const value = 'hello';
      await db1.add(value)

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
            const db1Entries: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
            try {
              expect(db1Entries.length).toEqual(1)
              expect(orbitdb1.findReplicators(db1.replicationTopic, true, db1Entries[0].gid)).toContainValues([orbitdb1.id, orbitdb2.id]);
              expect(db1Entries[0].payload.value.value).toEqual(value)
            } catch (error) {
              reject(error)
            }

            const db2Entries: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
            try {
              expect(db2Entries.length).toEqual(1)
              expect(orbitdb2.findReplicators(db2.replicationTopic, true, db2Entries[0].gid)).toContainValues([orbitdb1.id, orbitdb2.id]);
              expect(db2Entries[0].payload.value.value).toEqual(value)
            } catch (error) {
              reject(error)
            }
            expect(replicatedEventCount).toEqual(1)
            resolve(true)
          }
        }, 100)
      })
    })

    it('replicates database of 100 entries', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

      options = Object.assign({}, options, { directory: dbPath2 })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), options)

      /*  await waitFor(() => orbitdb1._directConnections.size === 1);
       await waitFor(() => orbitdb2._directConnections.size === 1); */

      let finished = false
      const entryCount = 100
      const entryArr: number[] = []

      for (let i = 0; i < entryCount; i++) {
        entryArr.push(i)
      }

      await new Promise(async (resolve, reject) => {
        db2.events.on('replicated', () => {
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          finished = (all === entryCount)
        })

        try {
          const add = i => db1.add('hello' + i)
          await mapSeries(entryArr, add)
        } catch (e) {
          reject(e)
        }

        timer = setInterval(() => {
          if (finished) {
            clearInterval(timer)
            const entries = db2.iterator({ limit: -1 }).collect()
            try {
              expect(entries.length).toEqual(entryCount)
              expect(entries[0].payload.value.value).toEqual('hello0')
              expect(entries[entries.length - 1].payload.value.value).toEqual('hello99')
              resolve(true)
            } catch (error) {
              reject(error)
            }
          }
        }, 100)
      })
    })

    it('emits correct replication info', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), options)

      let finished = false
      const entryCount = 99

      return new Promise(async (resolve, reject) => {
        // Test that none of the entries gets into the replication queue twice
        const replicateSet = new Set()
        db2.events.on('replicate', (address, entry) => {
          if (!replicateSet.has(entry.hash)) {
            replicateSet.add(entry.hash)
          } else {
            reject(new Error('Shouldn\'t have started replication twice for entry ' + entry.hash + '\n' + entry.payload.value.value))
          }
        })

        // Verify that progress count increases monotonically by saving
        // each event's current progress into an array
        const progressEvents = []
        db2.events.on('replicate.progress', () => {
          progressEvents.push(db2.replicationStatus.progress)
        })

        db2.events.on('replicated', (address, length) => {
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          finished = (all === entryCount)
        })

        timer = setInterval(() => {
          try {

            if (finished) {
              clearInterval(timer)
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

              resolve(true)
            }
          } catch (e) {
            reject(e)
          }
        }, 1000)


        // Trigger replication
        let adds = []
        for (let i = 0; i < entryCount; i++) {
          adds.push(i)
        }

        await mapSeries(adds, i => db1.add('hello ' + i))
      })
    })

    it('emits correct replication info on fresh replication', async () => {
      await new Promise(async (resolve, reject) => {
        let finished = false
        const entryCount = 512

        // Trigger replication
        const adds = []
        for (let i = 0; i < entryCount; i++) {
          adds.push(i)
        }

        const add = async (i) => {
          process.stdout.write("\rWriting " + (i + 1) + " / " + entryCount + " ")
          await db1.add('hello ' + i)
        }

        await mapSeries(adds, add)

        // Open second instance again
        options = {
          directory: dbPath2,
          sync: true,

        }

        db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), options)

        // Test that none of the entries gets into the replication queue twice
        const replicateSet = new Set()
        db2.events.on('replicate', (address, entry) => {
          if (!replicateSet.has(entry.hash)) {
            replicateSet.add(entry.hash)
          } else {
            reject(new Error('Shouldn\'t have started replication twice for entry ' + entry.hash))
          }
        })

        // Verify that progress count increases monotonically by saving
        // each event's current progress into an array
        const progressEvents: bigint[] = []
        db2.events.on('replicate.progress', (address, hash, entry) => {
          progressEvents.push(db2.replicationStatus.progress)
        })

        let replicatedEventCount = 0
        db2.events.on('replicated', (address, length) => {
          replicatedEventCount++
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          finished = (all === entryCount)
        })

        timer = setInterval(async () => {
          if (finished) {
            clearInterval(timer)

            try {
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

              resolve(true)
            } catch (e) {
              reject(e)
            }
          }
        }, 100)
      })
    })

    it('emits correct replication info in two-way replication', async () => {
      await new Promise(async (resolve, reject) => {
        console.log("Waiting for peers to connect")
        await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

        let finished = false
        const entryCount = 100

        // Trigger replication
        const adds = []
        for (let i = 0; i < entryCount; i++) {
          adds.push(i)
        }

        const add = async (i) => {
          process.stdout.write("\rWriting " + (i + 1) + " / " + entryCount + " ")
          await Promise.all([db1.add('hello-1-' + i), db2.add('hello-2-' + i)])
        }

        // Open second instance again
        let options = {
          directory: dbPath2 + '2',
          overwrite: true,
          sync: true,

        }

        db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), options)
        expect(db1.address.toString()).toEqual(db2.address.toString())

        // Test that none of the entries gets into the replication queue twice
        const replicateSet = new Set()
        db2.events.on('replicate', (address, entry) => {
          if (!replicateSet.has(entry.hash)) {
            replicateSet.add(entry.hash)
          } else {
            reject(new Error('Shouldn\'t have started replication twice for entry ' + entry.hash))
          }
        })

        db2.events.on('replicated', (address, length) => {
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          finished = (all === entryCount * 2)
        })

        await mapSeries(adds, add)
        timer = setInterval(() => {
          if (finished) {
            clearInterval(timer)
            try {

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

              resolve(true)
            } catch (e) {
              reject(e)
            }
          }
        }, 500)

      })
    })


  })
})
