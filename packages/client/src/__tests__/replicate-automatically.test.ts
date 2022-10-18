
import { OrbitDB } from "../orbit-db"

import { EventStore, Operation } from "./utils/stores/event-store"
import { KeyValueStore } from "./utils/stores/key-value-store"
import assert from 'assert'
import mapSeries from 'p-each-series'
import rmrf from 'rimraf'
import { jest } from '@jest/globals';
// @ts-ignore
import { v4 as uuid } from 'uuid';
// Include test utilities
import {
  nodeConfig as config,
  testAPIs,
  Session
} from '@dao-xyz/peerbit-test-utils'
import { waitFor } from "@dao-xyz/time"

const dbPath1 = './orbitdb/tests/replicate-automatically/1'
const dbPath2 = './orbitdb/tests/replicate-automatically/2'
const dbPath3 = './orbitdb/tests/replicate-automatically/3'
const dbPath4 = './orbitdb/tests/replicate-automatically/4'

Object.keys(testAPIs).forEach(API => {
  describe(`orbit-db - Automatic Replication (${API})`, function () {
    jest.setTimeout(config.timeout * 14)

    /*  let ipfsd1: Controller, ipfsd2: Controller, ipfsd3: Controller, ipfsd4: Controller, ipfs1: IPFS, ipfs2: IPFS, ipfs3: IPFS, ipfs4: IPFS */
    let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, orbitdb4: OrbitDB
    let session: Session;
    beforeAll(async () => {
      rmrf.sync('./orbitdb')
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      rmrf.sync(dbPath3)
      rmrf.sync(dbPath4)
      session = await Session.connected(2);
      /*    ipfsd1 = await startIpfs(API, config.daemon1)
         ipfsd2 = await startIpfs(API, config.daemon2)
         ipfsd3 = await startIpfs(API, config.daemon2)
         ipfsd4 = await startIpfs(API, config.daemon2)
   
         ipfs1 = ipfsd1.api
         ipfs2 = ipfsd2.api
         ipfs3 = ipfsd3.api
         ipfs4 = ipfsd4.api */


    })

    afterAll(async () => {

      if (orbitdb1) {
        await orbitdb1.stop()
      }
      if (orbitdb2) {
        await orbitdb2.stop()
      }
      if (orbitdb3) {
        await orbitdb3.stop()
      }
      if (orbitdb4) {
        await orbitdb4.stop()
      }

      await session.stop();
      rmrf.sync(dbPath1)
      rmrf.sync(dbPath2)
      rmrf.sync(dbPath3)
      rmrf.sync(dbPath4)

    })

    it('starts replicating the database when peers connect', async () => {
      console.log('Peers connected')
      orbitdb1 = await OrbitDB.createInstance(session.peers[0].ipfs, { directory: dbPath1 })
      orbitdb2 = await OrbitDB.createInstance(session.peers[1].ipfs, { directory: dbPath2 })

      const entryCount = 33
      const entryArr = []

      const replicationTopic = uuid();

      const db1 = await orbitdb1.open(new EventStore<string>({ name: 'replicate-automatically-tests' }), replicationTopic, {})

      const db3 = await orbitdb1.open(new KeyValueStore<string>({ name: 'replicate-automatically-tests-kv' }), replicationTopic, {
        onReplicationComplete: (_) => {
          fail();
        }
      })

      // Create the entries in the first database
      for (let i = 0; i < entryCount; i++) {
        entryArr.push(i)
      }

      await mapSeries(entryArr, (i) => db1.add('hello' + i))

      // Open the second database
      let done = false
      const db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2._ipfs, db1.address), replicationTopic, {
        onReplicationComplete: (_) => {
          // Listen for the 'replicated' events and check that all the entries
          // were replicated to the second database
          expect(db2.iterator({ limit: -1 }).collect().length).toEqual(entryCount)
          const result1 = db1.iterator({ limit: -1 }).collect()
          const result2 = db2.iterator({ limit: -1 }).collect()
          expect(result1.length).toEqual(result2.length)
          for (let i = 0; i < result1.length; i++) {
            assert(result1[i].equals(result2[i]))
          }
          done = true;
        }
      })

      const db4 = await orbitdb2.open<KeyValueStore<string>>(await KeyValueStore.load<KeyValueStore<string>>(orbitdb2._ipfs, db3.address), replicationTopic, {
        onReplicationComplete: (_) => {
          fail();
        }
      })

      await waitFor(() => done);



      /*  it('will replicate among multiple peers', async () => {
         const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
         await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
         await connectPeers(ipfs2, ipfs3, { filter: isLocalhostAddress })
         await connectPeers(ipfs3, ipfs4, { filter: isLocalhostAddress })
         await connectPeers(ipfs1, ipfs3, { filter: isLocalhostAddress })
         await connectPeers(ipfs2, ipfs4, { filter: isLocalhostAddress })
         await connectPeers(ipfs1, ipfs4, { filter: isLocalhostAddress })
   
         orbitdb3 = await OrbitDB.createInstance(ipfs3, { directory: dbPath3, minReplicas: 3 })
         orbitdb4 = await OrbitDB.createInstance(ipfs4, { directory: dbPath4, minReplicas: 3 })
   
         // Create a write only peer and write two messsages and make sure another peer is replicating them
         const replicationTopic = 'x';
         const store = new EventStore<string>({ name: 'replication-tests' });
         const db1 = await orbitdb1.open(store, { replicate: false, replicationTopic }); // this would be a "light" client, write -only
         const db2 = await orbitdb2.open(store.clone(), { replicationTopic });
         const db3 = await orbitdb3.open(store.clone(), { replicationTopic });
         const db4 = await orbitdb4.open(store.clone(), { replicationTopic });
   
         const hello = await db1.add('hello', { nexts: [] });
   
         expect(store.oplog.heads).toHaveLength(1);
         await delay(15000);
              await waitFor(() => db2.oplog.values.length > 0, { timeout: 30000, delayInterval: 50 });
             await waitFor(() => db3.oplog.values.length > 0, { timeout: 30000, delayInterval: 50 });
             await waitFor(() => db4.oplog.values.length > 0, { timeout: 30000, delayInterval: 50 }); 
         const x = 123;
   
       }) */

      /* it('will run a chron job making sures stores stay replicated', async () => { 
  
        // TODO fix this
  
        const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
        await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
        await connectPeers(ipfs2, ipfs3, { filter: isLocalhostAddress })
        await connectPeers(ipfs3, ipfs4, { filter: isLocalhostAddress })
        await connectPeers(ipfs1, ipfs3, { filter: isLocalhostAddress })
        await connectPeers(ipfs2, ipfs4, { filter: isLocalhostAddress })
        await connectPeers(ipfs1, ipfs4, { filter: isLocalhostAddress })
  
        const minReplicas = 3;
        orbitdb1 = await OrbitDB.createInstance(ipfs1, { directory: dbPath1, minReplicas })
        orbitdb2 = await OrbitDB.createInstance(ipfs2, { directory: dbPath2, minReplicas })
        orbitdb3 = await OrbitDB.createInstance(ipfs3, { directory: dbPath3, minReplicas })
        orbitdb4 = await OrbitDB.createInstance(ipfs4, { directory: dbPath4, minReplicas })
  
  
        // Create a write only peer and write two messsages and make sure another peer is replicating them
        const replicationTopic = 'x';
        const store = new EventStore<string>({ name: 'replication-tests' });
        const db1 = await orbitdb1.open(store, { replicate: false, replicationTopic }); // this would be a "light" client, write -only
        const db2 = await orbitdb2.open(store.clone(), { replicationTopic });
        const db3 = await orbitdb3.open(store.clone(), { replicationTopic });
  
        await waitForPeers(ipfs2, [orbitdb3.id], DirectChannel.getTopic([orbitdb2.id, orbitdb3.id]))
  
  
        // await delay(10000); // Takes too logn time to get peers so we need to have this??
  
        const hello = await db1.add('hello', { nexts: [] });
        const world = await db1.add('world', { nexts: [hello] });
  
        expect(store.oplog.heads).toHaveLength(1);
  
        // On peer connected, emit replicator info?
        // On exchange heads, use as replicator info?
  
        //LEADER SELECTION MIN REPLCIATS FIX this, REPLICATE FALSE SHOULD NOT BE PART OF LEADER SELCTION
        //MIN_REPLCIATS OR LEADERS IS MAKING THIS NOT WORK ?
        // await delay(30000);
        await waitFor(() => db2.oplog.values.length == 2);
        await waitFor(() => db3.oplog.values.length == 2);
        expect(db2.oplog.heads).toHaveLength(1);
        expect(db2.oplog.heads[0].hash).toEqual(world.hash);
  
        const peers3 = (await orbitdb2.getPeers(new RequestReplicatorInfo({
          address: store.address,
          replicationTopic,
          heads: [world.hash],
        }), { waitForPeersTime: 3000 }));
        expect(peers3).toHaveLength(1); // 1 peer, 1 + 1 = 2 in total
        expect(peers3[0].publicKey).toEqual(orbitdb2.publicKey)
  
        const _db4 = await orbitdb4.open(store.clone(), { replicationTopic });
        await waitFor(() => !!orbitdb4.stores[replicationTopic]?.[store.address.toString()]);
        await waitForPeers(ipfs3, [orbitdb4.id], DirectChannel.getTopic([orbitdb3.id, orbitdb4.id]))
        await waitForPeers(ipfs2, [orbitdb4.id], DirectChannel.getTopic([orbitdb2.id, orbitdb4.id]))
  
        const peers4 = (await orbitdb1.getPeers(new RequestReplicatorInfo({
          address: store.address,
          replicationTopic,
          heads: [world.hash],
        }), { waitForPeersTime: 10000 }));
        await delay(10000);
        expect(peers4).toHaveLength(2); // same amount of peers, minReplicas is 2 
        expect(peers4.map(x => x.publicKey)).toEqual([orbitdb2.publicKey, orbitdb3.publicKey])
  
        const x = 123;
        // Now open a forth peer and make sure it does not start to replicate entries since it is not needed
  
  
      }) */

    })
  })
})