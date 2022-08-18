
const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { OrbitDB } from '../orbit-db'
import { EventStore, EVENT_STORE_TYPE, Operation } from './utils/stores/event-store'
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
import { replicationTopicEncryption } from '../encryption'

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
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, db1: EventStore<string>, db2: EventStore<string>
    let recieverKey: KeyWithMeta

    let timer
    let options: IStoreOptions<any, any, any>

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

      orbitdb1 = await OrbitDB.createInstance(ipfs1, {
        directory: orbitdbPath1, canAccessKeys: (requester, _keyToAccess) => {
          const comp = Buffer.compare(requester.getBuffer(), Buffer.from(orbitdb2.identity.publicKey));
          return Promise.resolve(comp === 0) // allow orbitdb1 to share keys with orbitdb2
        }
      })
      orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2 })

      recieverKey = await orbitdb2.keystore.createKey('sender', 'box');

      options = {
        // Set write access for both clients
        accessController: {
          write: [
            orbitdb1.identity.id,
            orbitdb2.identity.id,
          ]
        }
      }

      options = Object.assign({}, options, { directory: dbPath1 })
      db1 = await orbitdb1.create('replication-tests', EVENT_STORE_TYPE, {
        ...options, encryption: replicationTopicEncryption(orbitdb1)
      })
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

    it('replicates database of 1 entry known keys', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { create: true, type: EVENT_STORE_TYPE, directory: dbPath2, sync: true })
      db2 = await orbitdb2.open(db1.address.toString(), { ...options, encryption: replicationTopicEncryption(orbitdb2) })


      let finished = false

      await db1.add('hello', { reciever: await Keystore.getPublicBox(recieverKey.key) })

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
            } catch (error) {
              reject(error)
            }
            resolve(true)
          }
        }, 100)
      })
    })


    it('replicates database of 1 entry unknown keys', async () => {

      console.log("Waiting for peers to connect")

      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { create: true, type: EVENT_STORE_TYPE, directory: dbPath2, sync: true })

      const unknownKey = await orbitdb1.keystore.createKey('unknown', 'box', db1.replicationTopic);

      // We expect during opening that keys are exchange
      db2 = await orbitdb2.open(db1.address.toString(), { ...options, encryption: replicationTopicEncryption(orbitdb2) })

      let finished = false

      // ... so that append with reciever key, it the reciever will be able to decrypt
      await db1.add('hello', { reciever: await Keystore.getPublicBox(unknownKey.key) })

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
            } catch (error) {
              reject(error)
            }
            resolve(true)
          }
        }, 100)
      })
    })
  })
})
