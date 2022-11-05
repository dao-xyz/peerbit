import { Peerbit } from "../peer"

import { EventStore } from "./utils/stores/event-store"

import mapSeries from 'p-each-series'
import rmrf from 'rimraf'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
// Include test utilities
import {
  nodeConfig as config,
  startIpfs,
  stopIpfs,
  testAPIs,
  connectPeers,
  waitForPeers,
} from '@dao-xyz/peerbit-test-utils'
// @ts-ignore
import { v4 as uuid } from 'uuid';

const orbitdbPath1 = './orbitdb/tests/replicate-and-load/1'
const orbitdbPath2 = './orbitdb/tests/replicate-and-load/2'
const dbPath1 = './orbitdb/tests/replicate-and-load/1/db1'
const dbPath2 = './orbitdb/tests/replicate-and-load/2/db2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replicate and Load (${API})`, function () {
    jest.setTimeout(config.timeout)

    let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS
    let orbitdb1: Peerbit, orbitdb2: Peerbit

    beforeAll(async () => {
      rmrf.sync(orbitdbPath1)
      rmrf.sync(orbitdbPath2)
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      ipfsd1 = await startIpfs(API, config.daemon1)
      ipfsd2 = await startIpfs(API, config.daemon2)
      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api
      orbitdb1 = await Peerbit.create(ipfs1, { directory: orbitdbPath1 })
      orbitdb2 = await Peerbit.create(ipfs2, { directory: orbitdbPath2 })
      // Connect the peers manually to speed up test times
      const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
      await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
      console.log("Peers connected")
    })

    afterAll(async () => {
      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()

      if (ipfsd1)
        await stopIpfs(ipfsd1)

      if (ipfsd2)
        await stopIpfs(ipfsd2)

      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
    })

    describe('two peers', function () {
      let db1: EventStore<string>, db2: EventStore<string>, replicationTopic: string

      const openDatabases = async () => {
        // Set write access for both clients
        replicationTopic = uuid();
        db1 = await orbitdb1.open(new EventStore<string>({
          id: 'events',

        }), { replicationTopic, directory: dbPath1, })
        // Set 'localOnly' flag on and it'll error if the database doesn't exist locally
        db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address!), { replicationTopic, directory: dbPath2, })
      }

      beforeAll(async () => {
        await openDatabases()

        expect(db1.address!.toString()).toEqual(db2.address!.toString())

        console.log("Waiting for peers...")
        await waitForPeers(ipfs1, [orbitdb2.id], replicationTopic)
        await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)
      })

      afterAll(async () => {
        if (db1) {
          await db1.drop()
        }

        if (db2) {
          await db2.drop()
        }
      })

      it('replicates database of 100 entries and loads it from the disk', async () => {
        const entryCount = 100
        const entryArr: number[] = []
        let timer: any;

        for (let i = 0; i < entryCount; i++)
          entryArr.push(i)

        console.log("Writing to database...")
        await mapSeries(entryArr, (i) => db1.add('hello' + i))
        console.log("Done")

        return new Promise((resolve, reject) => {
          timer = setInterval(async () => {
            if (db2.store._oplog.length === entryCount) {
              clearInterval(timer)

              const items = db2.iterator({ limit: -1 }).collect()
              expect(items.length).toEqual(entryCount)
              expect(items[0].payload.getValue().value).toEqual('hello0')
              expect(items[items.length - 1].payload.getValue().value).toEqual('hello' + (items.length - 1));

              try {
                // Get the previous address to make sure nothing mutates it

                /* TODO, since new changes, below might not be applicable 

                // Open the database again (this time from the disk)
                options = Object.assign({}, options, { directory: dbPath1, create: false })
                const db3 = await orbitdb1.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb1._ipfs, db1.address), { replicationTopic, ...options }) // We set replicationTopic to "_" because if the replication topic is the same, then error will be thrown for opening the same store
                // Set 'localOnly' flag on and it'll error if the database doesn't exist locally
                options = Object.assign({}, options, { directory: dbPath2, localOnly: true })
                const db4 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), { replicationTopic, ...options }) // We set replicationTopic to "_" because if the replication topic is the same, then error will be thrown for opening the same store

                await db3.store.load()
                await db4.store.load()

                // Make sure we have all the entries in the databases
                const result1 = db3.iterator({ limit: -1 }).collect()
                const result2 = db4.iterator({ limit: -1 }).collect()
                expect(result1.length).toEqual(entryCount)
                expect(result2.length).toEqual(entryCount)

                await db3.drop()
                await db4.drop() */
              } catch (e: any) {
                reject(e)
              }
              resolve(true)
            }
          }, 1000)
        })
      })
    })
  })
})
