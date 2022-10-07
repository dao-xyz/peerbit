
import assert from 'assert'
import mapSeries from 'p-each-series'
import rmrf from 'rimraf'
import { Entry } from '@dao-xyz/ipfs-log'
import { OrbitDB } from '../orbit-db'
import { EventStore, Operation } from './utils/stores/event-store'
import { IStoreOptions } from '@dao-xyz/orbit-db-store';
import { Ed25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { AccessError } from "@dao-xyz/peerbit-crypto"
import { SimpleAccessController } from './utils/access'
import { jest } from '@jest/globals';
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
import { KeyWithMeta } from '@dao-xyz/orbit-db-keystore'
import { waitFor } from '@dao-xyz/time'


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
const orbitdbPath3 = './orbitdb/tests/replication/3'

const dbPath1 = './orbitdb/tests/replication/1/db1'
const dbPath2 = './orbitdb/tests/replication/2/db2'
const dbPath3 = './orbitdb/tests/replication/3/db3'


const addHello = async (db: EventStore<string>, receiver: X25519PublicKey) => {
  await db.add('hello', { reciever: { clock: receiver, payload: receiver, signature: receiver } })

}
const checkHello = async (db: EventStore<string>) => {
  const entries: Entry<Operation<string>>[] = db.iterator({ limit: -1 }).collect()
  expect(entries.length).toEqual(1)
  await entries[0].getPayload();
  expect(entries[0].payload.getValue().value).toEqual('hello')
}

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - encryption`, function () {
    jest.setTimeout(config.timeout * 2)

    let ipfsd1: Controller, ipfsd2: Controller, ipfsd3: Controller, ipfs1: IPFS, ipfs2: IPFS, ipfs3: IPFS
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>
    let recieverKey: KeyWithMeta<Ed25519Keypair>
    let options: IStoreOptions<any>


    beforeAll(async () => {
      ipfsd1 = await startIpfs(API, config.daemon1)
      ipfsd2 = await startIpfs(API, config.daemon2)
      ipfsd3 = await startIpfs(API, config.daemon2)

      ipfs1 = ipfsd1.api
      ipfs2 = ipfsd2.api
      ipfs3 = ipfsd3.api

      // Connect the peers manually to speed up test times
      const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
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
      rmrf.sync(orbitdbPath1)
      rmrf.sync(orbitdbPath2)
      rmrf.sync(orbitdbPath3)
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      rmrf.sync(dbPath3)

      orbitdb1 = await OrbitDB.createInstance(ipfs1, {
        directory: orbitdbPath1, canAccessKeys: (requester, _keyToAccess) => {
          return Promise.resolve(requester.equals(orbitdb2.identity.publicKey)) // allow orbitdb1 to share keys with orbitdb2
        }, waitForKeysTimout: 1000
      })
      orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: orbitdbPath2, waitForKeysTimout: 1000 })
      orbitdb3 = await OrbitDB.createInstance(ipfs3, { directory: orbitdbPath3, waitForKeysTimout: 1000 })

      recieverKey = await orbitdb2.keystore.createEd25519Key();


      options = Object.assign({}, options, { directory: dbPath1 })
      db1 = await orbitdb1.open(new EventStore<string>({
        accessController: new SimpleAccessController()
      }), {
        ...options
      })
    })

    afterEach(async () => {

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

      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())

      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      let done = false;


      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options, onReplicationComplete: async (_store) => {
          await checkHello(db1);
          done = true;
        }
      })

      await addHello(db1, recieverKey.keypair.publicKey);
      await waitFor(() => done);
    })


    it('replicates database of 1 entry unknown keys', async () => {



      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      // Set 'sync' flag on. It'll prevent creating a new local database and rather
      // fetch the database from the network
      options = Object.assign({}, options, { directory: dbPath2, sync: true })

      const unknownKey = await orbitdb1.keystore.createEd25519Key({ id: 'unknown', group: db1.replicationTopic });

      // We expect during opening that keys are exchange
      let done = false;
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options, onReplicationComplete: async (_store) => {
          await checkHello(db1);
          done = true;
        }
      })

      // ... so that append with reciever key, it the reciever will be able to decrypt
      await addHello(db1, recieverKey.keypair.publicKey);
      await waitFor(() => done);

    })

    it('can ask for public keys even if not trusted', async () => {
      await waitForPeers(ipfs3, [orbitdb1.id], db1.address.toString())
      options = Object.assign({}, options, { directory: dbPath3, sync: true })

      const db3Key = await orbitdb3.keystore.createEd25519Key({ id: 'unknown', group: db1.replicationTopic });

      // We expect during opening that keys are exchange
      let done = false;
      db3 = await orbitdb3.open<EventStore<string>>(await EventStore.load(orbitdb3._ipfs, db1.address), {
        ...options, onReplicationComplete: async (_store) => {
          await checkHello(db1);
          done = true;
        }
      })

      const reciever = await orbitdb1.getEncryptionKey(db1.replicationTopic);

      assert.deepStrictEqual(reciever.secretKey, undefined); // because client 1 is not trusted by 3
      expect(db3Key.keypair.publicKey.equals(reciever.publicKey));

      // ... so that append with reciever key, it the reciever will be able to decrypt
      await addHello(db1, recieverKey.keypair.publicKey);
      await waitFor(() => done);

    })

    it('can retrieve secret keys if trusted', async () => {

      await waitForPeers(ipfs3, [orbitdb1.id], db1.address.toString())

      const db1Key = await orbitdb1.keystore.createEd25519Key({ id: 'unknown', group: db1.replicationTopic });

      // Open store from orbitdb3 so that both client 1 and 2 is listening to the replication topic
      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      let done = false;
      await orbitdb2.open(await EventStore.load(orbitdb2._ipfs, db1.address), {
        ...options
      })
      const reciever = await orbitdb2.getEncryptionKey(db1.replicationTopic);

      assert(!!reciever.secretKey); // because client 1 is not trusted by 3
      expect(db1Key.keypair.publicKey.equals(reciever.publicKey));
    })

    it('can relay with end to end encryption with public id and clock (E2EE-weak)', async () => {

      console.log("Waiting for peers to connect")

      await waitForPeers(ipfs2, [orbitdb1.id], db1.address.toString())
      await waitForPeers(ipfs3, [orbitdb1.id], db1.address.toString())

      options = Object.assign({}, options, { directory: dbPath2, sync: true })
      db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), { ...options })

      const client3Key = await orbitdb3.keystore.createEd25519Key({ id: 'unknown' });

      await db2.add('hello', { reciever: { clock: undefined, payload: client3Key.keypair.publicKey, signature: client3Key.keypair.publicKey } })

      // Wait for db1 (the relay) to get entry
      await waitFor(() => db1.oplog.values.length === 1)
      const entriesRelay: Entry<Operation<string>>[] = db1.iterator({ limit: -1 }).collect()
      expect(entriesRelay.length).toEqual(1)
      try {
        await entriesRelay[0].getPayload(); // should fail, since relay can not see the message
        assert(false);
      } catch (error) {
        expect(error).toBeInstanceOf(AccessError);
      }


      const sender: Entry<Operation<string>>[] = db2.iterator({ limit: -1 }).collect()
      expect(sender.length).toEqual(1)
      await sender[0].getPayload();
      expect(sender[0].payload.getValue().value).toEqual('hello')


      // Now close db2 and open db3 and make sure message are available
      await db2.drop();
      options = Object.assign({}, options, { directory: dbPath3, sync: true })
      db3 = await orbitdb3.open<EventStore<string>>(await EventStore.load(orbitdb3._ipfs, db1.address), {
        ...options, onReplicationComplete: async (store) => {
          const entriesRelay: Entry<Operation<string>>[] = db3.iterator({ limit: -1 }).collect()
          expect(entriesRelay.length).toEqual(1)
          await entriesRelay[0].getPayload(); // should pass since orbitdb3 got encryption key
        }
      })


    })
  })
})
