
import rmrf from 'rimraf'
import { Entry } from '@dao-xyz/ipfs-log'
import { waitFor } from '@dao-xyz/peerbit-time'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
import { OrbitDB } from '../orbit-db'

import { EventStore, Operation } from './utils/stores/event-store'
// @ts-ignore
import { v4 as uuid } from 'uuid';

// Include test utilities
import {
  nodeConfig as config,
  startIpfs,
  stopIpfs,
  testAPIs,
  connectPeers,
  waitForPeers,
} from '@dao-xyz/peerbit-test-utils'

const orbitdbPath1 = './orbitdb/tests/replication-topic/1'
const orbitdbPath2 = './orbitdb/tests/replication-topic/2'
const dbPath1 = './orbitdb/tests/replication-topic/1/db1'
const dbPath2 = './orbitdb/tests/replication-topic/2/db2'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Replication topic (${API})`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>, db4: EventStore<string>

    let timer: any

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
      let options = { directory: dbPath2 }
      const replicationTopic = uuid();
      db1 = await orbitdb1.open(new EventStore<string>({ name: 'replication-tests' })
        , { ...Object.assign({}, options, { directory: dbPath1 }), replicationTopic })
      await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic)

      options = { ...options, directory: dbPath2 }
      let replicatedEventCount = 0
      let done = false
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), {
        replicationTopic,
        ...options, onReplicationComplete: (store) => {
          replicatedEventCount++
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db2.iterator({ limit: -1 }).collect().length
          done = (all === 1)
        }
      })
      db3 = await orbitdb2.open(new EventStore<string>({ name: 'replication-tests-same-topic' }), { replicationTopic, ...options })

      await waitFor(() => orbitdb1._directConnections.size === 1);
      await waitFor(() => orbitdb2._directConnections.size === 1);

      db1.add('hello')
      await waitFor(() => done);
      const entries: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
      expect(entries.length).toEqual(1)
      expect(entries[0].payload.getValue().value).toEqual('hello')
      expect(replicatedEventCount).toEqual(1)
      const allFromDB3 = db3.iterator({ limit: -1 }).collect().length
      expect(allFromDB3).toEqual(0) // Same replication topic but different DB (which means no entries should exist) 
    })
  })
})
