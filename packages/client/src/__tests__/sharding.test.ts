
import { Peerbit } from "../peer"

import { EventStore } from "./utils/stores/event-store"

import rmrf from 'rimraf'
import { jest } from '@jest/globals';

// Include test utilities
import {
    nodeConfig as config,
    testAPIs,
    Session,
} from '@dao-xyz/peerbit-test-utils'
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { delay, waitFor, waitForAsync } from "@dao-xyz/peerbit-time";
import { PermissionedEventStore } from "./utils/stores/test-store";

const dbPath1 = './orbitdb/tests/sharding/1'
const dbPath2 = './orbitdb/tests/sharding/2'
const dbPath3 = './orbitdb/tests/sharding/3'
Object.keys(testAPIs).forEach(API => {
    describe(`orbit-db - Automatic Replication (${API})`, function () {
        jest.setTimeout(config.timeout * 3)

        let session: Session;
        let orbitdb1: Peerbit, orbitdb2: Peerbit, orbitdb3: Peerbit, db1: PermissionedEventStore, db2: PermissionedEventStore, db3: PermissionedEventStore, replicationTopic: string;

        beforeEach(async () => {
            rmrf.sync(dbPath1)
            rmrf.sync(dbPath2)
            rmrf.sync(dbPath3)

            session = await Session.connected(3);

            orbitdb1 = await Peerbit.create(session.peers[0].ipfs, { directory: dbPath1 })
            orbitdb2 = await Peerbit.create(session.peers[1].ipfs, { directory: dbPath2 })
            orbitdb3 = await Peerbit.create(session.peers[2].ipfs, { directory: dbPath2 })

            const network = new TrustedNetwork({ id: 'network-tests', rootTrust: orbitdb1.identity.publicKey });
            db1 = await orbitdb1.open<PermissionedEventStore>(new PermissionedEventStore({ network }), { directory: dbPath1 })

            await orbitdb1.joinNetwork(db1);

            // trust client 3
            await network.add(orbitdb2.id)
            await network.add(orbitdb2.identity.publicKey)
            db2 = await orbitdb2.open<PermissionedEventStore>(db1.address!);
            await network.add(orbitdb3.id)
            await network.add(orbitdb3.identity.publicKey)
            db3 = await orbitdb3.open<PermissionedEventStore>(db1.address!);

            /*   replicationTopic = network.address!.toString();
  
              db1 = await orbitdb1.open(new EventStore<string>({ id: 'sharding-tests' })
                  , { replicationTopic })
  
              db2 = await orbitdb2.open(db1.address!, { replicationTopic }) as EventStore<string>
  
              db3 = await orbitdb3.open(db1.address!, { replicationTopic }) as EventStore<string> */

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

            // TODO this test is flaky, because it sometimes timeouts because distribution of data among peers is random for small entry counts 

            const entryCount = 30;
            // expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
            const promises: Promise<any>[] = [];
            for (let i = 0; i < entryCount; i++) {
                promises.push(db1.store.add(i.toString(), { nexts: [] }));
            }

            await Promise.all(promises);
            await waitFor(() => db1.store.store.oplog.values.length === entryCount)

            // this could failed, if we are unlucky probability wise
            await waitFor(() => db2.store.store.oplog.values.length > entryCount * 0.5 && db2.store.store.oplog.values.length < entryCount * 0.75)
            await waitFor(() => db3.store.store.oplog.values.length > entryCount * 0.5 && db3.store.store.oplog.values.length < entryCount * 0.75)


            const checkConverged = async (db: EventStore<any>) => {
                const a = db.store.oplog.values.length;
                await delay(5000); // arb delay
                return a === db.store.oplog.values.length
            }

            await waitForAsync(() => checkConverged(db2.store), { timeout: 20000, delayInterval: 5000 })
            await waitForAsync(() => checkConverged(db3.store), { timeout: 20000, delayInterval: 5000 })
        })
    })
})
