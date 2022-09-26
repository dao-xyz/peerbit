
const assert = require('assert')
const mapSeries = require('p-each-series')
const rmrf = require('rimraf')
import { Entry } from '@dao-xyz/ipfs-log-entry'
import { BoxKeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { OrbitDB } from '../orbit-db'
import { EventStore, Operation } from './utils/stores/event-store'
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey } from 'sodium-plus';
import { AccessError } from '@dao-xyz/encryption-utils'
import { SimpleAccessController } from './utils/access'


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
const orbitdbPath3 = './orbitdb/tests/replication/3'

const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'
const dbPath3 = './orbitdb/tests/replication/3/db3'
const testHello = async (addToDB: EventStore<string>, readFromDB: EventStore<string>, reciever: X25519PublicKey, timer: any) => {
  await addToDB.add('hello', { reciever: { clock: reciever, id: reciever, publicKey: reciever, payload: reciever, signature: reciever } })
  let finished = false;
  await new Promise((resolve, reject) => {
    let replicatedEventCount = 0
    readFromDB.events.on('replicated', (address, length) => {
      replicatedEventCount++
      // Once db2 has finished replication, make sure it has all elements
      // and process to the asserts below
      const all = readFromDB.iterator({ limit: -1 }).collect().length
      finished = (all === 1)
    })

    timer = setInterval(async () => {
      if (finished) {
        clearInterval(timer)
        const entries: Entry<Operation<string>>[] = readFromDB.iterator({ limit: -1 }).collect()
        try {
          expect(entries.length).toEqual(1)
          await entries[0].getPayload();
          expect(entries[0].payload.value.value).toEqual('hello')
          expect(replicatedEventCount).toEqual(1)
        } catch (error) {
          reject(error)
        }
        resolve(true)
      }
    }, 100)
  })
}

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - encryption`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1, ipfsd2, ipfsd3, ipfs1, ipfs2, ipfs3
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>
    let recieverKey: BoxKeyWithMeta

    let timer
    let options: IStoreOptions<any>


    beforeAll(async () => {
      ipfsd1 = await startIpfs(API, config.daemon1)
      ipfsd2 = await startIpfs(API, config.daemon2)
      ipfsd3 = await startIpfs(API, config.daemon2)

      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api
      ipfs3 = ipfsd3.api

      // Connect the peers manually to speed up test times
      const isLocalhostAddress = (addr) => addr.toString().includes('127.0.0.1')
      await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
      await connectPeers(ipfs1, ipfs3, { filter: isLocalhostAddress })
      await connectPeers(ipfs2, ipfs3, { filter: isLocalhostAddress })

      console.log("Peers connected")
    })

    afterAll(async () => {
      if (ipfsd1)
        await stopIpfs(ipfsd1)

      if (ipfsd2)
        await stopIpfs(ipfsd2)

      if (ipfsd3)
        await stopIpfs(ipfsd3)
    })

    beforeEach(async () => {
      clearInterval(timer)

      rmrf.sync(orbitdbPath1)
      rmrf.sync(orbitdbPath2)
      rmrf.sync(orbitdbPath3)
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      rmrf.sync(dbPath3)

      orbitdb1 = await OrbitDB.createInstance(ipfs1, {
        directory: orbitdbPath1, canAccessKeys: (requester, _keyToAccess) => {
          return Promise.resolve(requester.equals(orbitdb2.publicKey)) // allow orbitdb1 to share keys with orbitdb2
        }, waitForKeysTimout: 1000
      })
      orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2, waitForKeysTimout: 1000 })
      orbitdb3 = await OrbitDB.createInstance(ipfs3, { directory: orbitdbPath3, waitForKeysTimout: 1000 })

      recieverKey = await orbitdb2.keystore.createKey('sender', BoxKeyWithMeta);


      options = Object.assign({}, options, { directory: dbPath1 })
      db1 = await orbitdb1.open(new EventStore<string>({
        accessController: new SimpleAccessController()
      }), {
        ...options
      })
    })

    afterEach(async () => {
      clearInterval(timer)
      options = {}

      if (db1)
        await db1.drop()

      if (db2)
        await db2.drop()

      if (db3)
        await db3.drop()

      if (orbitdb1)
        await orbitdb1.stop()

      if (orbitdb2)
        await orbitdb2.stop()

      if (orbitdb3)
        await orbitdb3.stop()
    })

    it('replicates database of 1 entry known keys', async () => {
      console.log("Waiting for peers to connect")
      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options })
      await testHello(db1, db2, recieverKey.publicKey, timer)
    })


    it('replicates database of 1 entry unknown keys', async () => {

      console.log("Waiting for peers to connect")

      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { directory: dbPath2, sync: true })

      const unknownKey = await orbitdb1.keystore.createKey('unknown', BoxKeyWithMeta, db1.replicationTopic);

      // We expect during opening that keys are exchange
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options })

      // ... so that append with reciever key, it the reciever will be able to decrypt
      await testHello(db1, db2, unknownKey.publicKey, timer)

    })

    it('can ask for public keys even if not trusted', async () => {

      console.log("Waiting for peers to connect")

      await waitForPeers(ipfs3, [orbitdb1.id], db1.address.toString())
      options = Object.assign({}, options, { directory: dbPath3, sync: true })

      const db3Key = await orbitdb3.keystore.createKey('unknown', BoxKeyWithMeta, db1.replicationTopic);

      // We expect during opening that keys are exchange
      db3 = await orbitdb3.open<EventStore<string>>(await EventStore.load(orbitdb3._ipfs, db1.address), { ...options })

      const reciever = await orbitdb1.getEncryptionKey(db1.replicationTopic);

      assert.deepStrictEqual(reciever.secretKey, undefined); // because client 1 is not trusted by 3
      assert.deepStrictEqual(db3Key.publicKey.getBuffer(), reciever.publicKey.getBuffer());
      // ... so that append with reciever key, it the reciever will be able to decrypt
      await testHello(db1, db3, reciever.publicKey, timer)

    })

    it('can retrieve secret keys if trusted', async () => {

      console.log("Waiting for peers to connect")

      await waitForPeers(ipfs3, [orbitdb1.id], db1.address.toString())

      const db1Key = await orbitdb1.keystore.createKey('unknown', BoxKeyWithMeta, db1.replicationTopic);

      // Open store from orbitdb3 so that both client 1 and 2 is listening to the replication topic
      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      await orbitdb2.open(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options })

      const reciever = await orbitdb2.getEncryptionKey(db1.replicationTopic);

      assert(!!reciever.secretKey); // because client 1 is not trusted by 3
      assert.deepStrictEqual(db1Key.publicKey.getBuffer(), reciever.publicKey.getBuffer());
    })

    it('can relay with end to end encryption with public id and clock (E2EE-weak)', async () => {

      console.log("Waiting for peers to connect")

      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      await waitForPeers(ipfs3, [orbitdb1.id], db1.address.toString())

      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options })

      const client3Key = await orbitdb3.keystore.createKey('unknown', BoxKeyWithMeta);

      await db2.add('hello', { reciever: { id: undefined, clock: undefined, publicKey: client3Key.publicKey, payload: client3Key.publicKey, signature: client3Key.publicKey } })
      let finishedRelay = false;

      await new Promise((resolve, reject) => {
        let replicatedEventCount = 0

        db1.events.on('replicated', (address, length) => {
          replicatedEventCount++
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db1.iterator({ limit: -1 }).collect().length
          finishedRelay = (all === 1)
        })


        timer = setInterval(async () => {
          if (finishedRelay) {
            clearInterval(timer)


            const entriesRelay: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
            try {
              expect(entriesRelay.length).toEqual(1)
              try {
                await entriesRelay[0].getPayload(); // should fail, since relay can not see the message
                assert(false);
              } catch (error) {
                expect(error).toBeInstanceOf(AccessError);
              }
              expect(replicatedEventCount).toEqual(1)
            } catch (error) {
              reject(error)
            }


            const sender: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
            try {
              expect(sender.length).toEqual(1)
              await sender[0].getPayload();
              expect(sender[0].payload.value.value).toEqual('hello')
              expect(replicatedEventCount).toEqual(1)
            } catch (error) {
              reject(error)
            }
            resolve(true)
          }
        }, 100)
      })


      // Now close db2 and open db3 and make sure message are available
      await db2.drop();
      options = Object.assign({}, options, { directory: dbPath3, sync: true })
      db3 = await orbitdb3.open<EventStore<string>>(await EventStore.load(orbitdb3._ipfs, db1.address), { ...options })

      let finishedEnd = false;
      await new Promise((resolve, reject) => {
        let replicatedEventCount = 0

        db3.events.on('replicated', (address, length) => {
          replicatedEventCount++
          // Once db2 has finished replication, make sure it has all elements
          // and process to the asserts below
          const all = db3.iterator({ limit: -1 }).collect().length
          finishedEnd = (all === 1)
        })


        timer = setInterval(async () => {
          if (finishedEnd) {
            clearInterval(timer)
            const entriesRelay: Entry<Operation<string>>[] = db3.iterator({ limit: -1 }).collect()
            try {
              expect(entriesRelay.length).toEqual(1)
              await entriesRelay[0].getPayload(); // should pass since orbitdb3 got encryption key
              expect(replicatedEventCount).toEqual(1)
            } catch (error) {
              reject(error)
            }
            resolve(true);
          }
        }, 100)
      })
    })
  })
})
