
import { OrbitDB } from "../orbit-db"

import { EventStore } from "./utils/stores/event-store"

import rmrf from 'rimraf'
import { jest } from '@jest/globals';

// Include test utilities
import {
    nodeConfig as config,
    testAPIs,
    Session,
} from '@dao-xyz/orbit-db-test-utils'
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { delay, waitFor, waitForAsync } from "@dao-xyz/time";

const dbPath1 = './orbitdb/tests/sharding/1'
const dbPath2 = './orbitdb/tests/sharding/2'
const dbPath3 = './orbitdb/tests/sharding/3'
Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - Automatic Replication (${API})`, function () {
        jest.setTimeout(config.timeout * 3)

        let session: Session;
        let orbitdb1: OrbitDB, orbitdb2: OrbitDB, orbitdb3: OrbitDB, db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>, replicationTopic: string;

        beforeEach(async () => {
            rmrf.sync('./orbitdb')
            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            session = await Session.connected(3);

            orbitdb1 = await OrbitDB.createInstance(session.peers[0].ipfs, { directory: dbPath1 })
            orbitdb2 = await OrbitDB.createInstance(session.peers[1].ipfs, { directory: dbPath2 })
            orbitdb3 = await OrbitDB.createInstance(session.peers[2].ipfs, { directory: dbPath2 })

            const network = await orbitdb1.openNetwork(new TrustedNetwork({ name: 'network-tests', rootTrust: orbitdb1.identity.publicKey }), { directory: dbPath1 })
            await orbitdb1.joinNetwork(network);

            // trust client 3
            await network.add(orbitdb2.id)
            await network.add(orbitdb2.identity.publicKey)
            await orbitdb2.openNetwork(network.address);
            await network.add(orbitdb3.id)
            await network.add(orbitdb3.identity.publicKey)
            await orbitdb3.openNetwork(network.address);

            replicationTopic = network.address.toString();

            db1 = await orbitdb1.open(new EventStore<string>({ name: 'sharding-tests' })
                , replicationTopic)

            db2 = await orbitdb2.open(db1.address, replicationTopic) as EventStore<string>

            db3 = await orbitdb3.open(db1.address, replicationTopic) as EventStore<string>

        })

        afterEach(async () => {
            if (orbitdb1) {
                await orbitdb1.stop()
            }

            if (orbitdb2) {
                await orbitdb2.stop()
            }
            if (orbitdb3) {
                await orbitdb3.stop()
            }

            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            rmrf.sync(dbPath3)
            await session.stop();

        })

        it('can distribute evenly among peers', async () => {

            const entryCount = 30;
            // expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
            const promises = [];
            for (let i = 0; i < entryCount; i++) {
                promises.push(db1.add(i.toString(), { nexts: [] }));
            }

            await Promise.all(promises);
            await waitFor(() => db1.oplog.values.length === entryCount)

            // this could failed, if we are unlucky probability wise
            await waitFor(() => db2.oplog.values.length > entryCount * 0.5 && db2.oplog.values.length < entryCount * 0.75)
            await waitFor(() => db3.oplog.values.length > entryCount * 0.5 && db3.oplog.values.length < entryCount * 0.75)


            const checkConverged = async (db: EventStore<any>) => {
                const a = db.oplog.values.length;
                await delay(5000); // arb delay
                return a === db.oplog.values.length
            }

            await waitForAsync(() => checkConverged(db2), { timeout: 20000, delayInterval: 5000 })
            await waitForAsync(() => checkConverged(db3), { timeout: 20000, delayInterval: 5000 })

        })

        /*     it('can control forking behaviour with `allowForks`', async () => {
     
                const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
                await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
                console.log('Peers connected')
                const entryCount = 2
     
                // Create the entries in the first database
                let prev: Entry<any> = undefined;
                for (let i = 0; i < entryCount; i++) {
                    prev = await db1.add('hello' + i, { refs: prev ? [prev.hash] : undefined });
     
                }
     
                // Open the second database
                db2 = await orbitdb2.open<EventStore<string>>(await EventStore.load(orbitdb2._ipfs, db1.address), {})
                await waitFor(() => db2.oplog.values.length === entryCount);
     
                db2.allowForks = false; // Only allow "changes"
     
                const _forkEntry = await db1.add('fork entry', { refs: [] }); // to reject since it is not referencing any prior logs
                const lastEntry = await db1.add('chained entry', { refs: [prev.hash] });
                await waitFor(() => db2.oplog.values.length > entryCount);
                expect(db2.oplog.values.length).toEqual(3);
                expect(db2.oplog.values[db2.oplog.values.length - 1].hash).toEqual(lastEntry.hash);
            }) */

        /*   it('will reject forks when reaching memory limit', async () => {
              const isLocalhostAddress = (addr: string) => addr.toString().includes('127.0.0.1')
              await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress })
              console.log('Peers connected')
     
     
     
              // Create the entries in the first database
              let prev: Entry<any> = undefined;
              const entryCount = 2
     
     
              // Open the second database and set a heap size limit and assume this heap size limit is set in the opened store
              // Now check whether this heap size limit makes `allowForks` false when we start to write alot of data
              const heapsizeLimitForForks = 30000 + v8.getHeapStatistics().used_heap_size;
              orbitdb3 = await OrbitDB.createInstance(ipfs2, { directory: dbPath3, heapsizeLimitForForks })
              db2 = await orbitdb3.open<EventStore<string>>(await EventStore.load(orbitdb3._ipfs, db1.address), {})
              expect(db2.options.resourceOptions.heapSizeLimit()).toEqual(heapsizeLimitForForks);
              let i = 0;
              expect(db2.allowForks);
              while (db2.allowForks && i < 100) {
                  for (let i = 0; i < entryCount; i++) {
                      prev = await db1.add('hello' + i, { refs: prev ? [prev.hash] : undefined });
                  }
                  i++;
              }
              expect(!db2.allowForks);
          }) */
    })
})
