
import mapSeries from 'p-each-series'
import rmrf from 'rimraf'
import { OrbitDB } from '../orbit-db'

import { EventStore } from './utils/stores'
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
  connectPeers,
  waitForPeers,
  testAPIs,
} from '@dao-xyz/peerbit-test-utils'
import { waitFor } from '@dao-xyz/time';

const dbPath1 = './orbitdb/tests/multiple-databases/1'
const dbPath2 = './orbitdb/tests/multiple-databases/2'
const dbPath3 = './orbitdb/tests/multiple-databases/3'


Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Multiple Databases`, function () {
    jest.setTimeout(60000)

    let ipfsd1: Controller, ipfsd2: Controller, ipfsd3: Controller, ipfs1: IPFS, ipfs2: IPFS, ipfs3: IPFS
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB

    let localDatabases: EventStore<string>[] = []
    let remoteDatabasesA: EventStore<string>[] = []
    let remoteDatabasesB: EventStore<string>[] = []

    const dbCount = 2;

    // Create two IPFS instances and two OrbitDB instances (2 nodes/peers)
    beforeAll(async () => {
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      rmrf.sync(dbPath3)

      ipfsd1 = await startIpfs(API, config.daemon1)
      ipfsd2 = await startIpfs(API, config.daemon2)
      ipfsd3 = await startIpfs(API, config.daemon2)

      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api
      ipfs3 = ipfsd3.api

      // Connect the peers manually to speed up test times
      const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
      await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
      await connectPeers(ipfs2, ipfs3, { filter: isLocalhostAddress })

      console.log("Peers connected")
      orbitdb1 = await OrbitDB.createInstance(ipfs1, { directory: dbPath1 })
      orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: dbPath2 })
      orbitdb3 = await OrbitDB.createInstance(ipfs3, { directory: dbPath3 })
      orbitdb2.minReplicas = 3;
      orbitdb3.minReplicas = 3;
      orbitdb1.minReplicas = 3;

    })

    afterAll(async () => {
      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()

      if (orbitdb3)
        await orbitdb3.stop()

      if (ipfsd1)
        await stopIpfs(ipfsd1)

      if (ipfsd2)
        await stopIpfs(ipfsd2)

      if (ipfsd3)
        await stopIpfs(ipfsd3)
    })

    beforeEach(async () => {
      // Set write access for both clients


      console.log("Creating databases and waiting for peers to connect")

      // Open the databases on the first node
      const options = {}

      // Open the databases on the first node
      const replicationTopic = uuid();
      for (let i = 0; i < dbCount; i++) {
        const db = await orbitdb1.open(new EventStore<string>({ name: 'local-' + i }), replicationTopic, options)
        localDatabases.push(db)
      }
      for (let i = 0; i < dbCount; i++) {
        const db = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, localDatabases[i].address), replicationTopic, { directory: dbPath2, ...options })
        remoteDatabasesA.push(db)
      }

      for (let i = 0; i < dbCount; i++) {
        const db = await orbitdb3.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb3._ipfs, localDatabases[i].address), replicationTopic, { directory: dbPath3, ...options })
        remoteDatabasesB.push(db)
      }

      // Wait for the peers to connect
      await waitForPeers(ipfs1, [orbitdb2.id], replicationTopic)
      await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)
      await waitForPeers(ipfs3, [orbitdb1.id], replicationTopic)

      await waitFor(() => orbitdb1._directConnections.size === 2)
      await waitFor(() => orbitdb2._directConnections.size === 2)
      await waitFor(() => orbitdb3._directConnections.size === 2)

      console.log("Peers connected")

    })

    afterEach(async () => {
      for (let db of remoteDatabasesA)
        await db.store.drop()

      for (let db of remoteDatabasesB)
        await db.store.drop()

      for (let db of localDatabases)
        await db.store.drop()
    })

    it('replicates multiple open databases', async () => {
      const entryCount = 32
      const entryArr = []



      // Create an array that we use to create the db entries
      for (let i = 1; i < entryCount + 1; i++)
        entryArr.push(i)

      // Write entries to each database
      console.log("Writing to databases")
      for (let index = 0; index < dbCount; index++) {
        const db = localDatabases[index]
        entryArr.forEach((val) => db.add('hello-' + val))
      }

      // Function to check if all databases have been replicated
      const allReplicated = () => {
        return remoteDatabasesA.every(db => db.store._oplog.length === entryCount) && remoteDatabasesB.every(db => db.store._oplog.length === entryCount)
      }



      console.log("Waiting for replication to finish")

      // check data
      await new Promise((resolve, reject) => {
        const interval = setInterval(() => {
          if (allReplicated()) {
            clearInterval(interval)
            // Verify that the databases contain all the right entries
            remoteDatabasesA.forEach((db) => {
              try {
                const result = db.iterator({ limit: -1 }).collect().length;
                expect(result).toEqual(entryCount)
                expect(db.store._oplog.length).toEqual(entryCount)
              } catch (error) {
                reject(error)
              }
            })

            remoteDatabasesB.forEach((db) => {
              try {
                const result = db.iterator({ limit: -1 }).collect().length;
                expect(result).toEqual(entryCount)
                expect(db.store._oplog.length).toEqual(entryCount)
              } catch (error) {
                reject(error)
              }
            })
            resolve(true)
          }
        }, 200)
      })

      // check gracefully shut down (with no leak)
      let directConnections = 2;
      const subscriptions = (await orbitdb3._ipfs.pubsub.ls());
      expect(subscriptions.length).toEqual(directConnections + 1 + 1); //+ 1 for 1 replication topic + 1 for subcribing to "self" topic
      for (let i = 0; i < dbCount; i++) {
        await remoteDatabasesB[i].store.drop();
        const connections = (await orbitdb3._ipfs.pubsub.ls()).length;
        if (i < dbCount - 1) {
          expect(connections).toEqual(directConnections + 1 - (i + 1 - 1)) //  + 1 for replication topic, -  1 for subcribing to "self" topic
        }
        else {
          // Direct connection should close because no databases "in common" are open
          expect(connections).toEqual(0 + 1) // + 1 for subcribing to "self" topic
        }
      }
    })
  })
})
