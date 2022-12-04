import { getReplicationTopic, Peerbit } from "../peer";
import { EventStore } from "./utils/stores";
import { jest } from "@jest/globals";
import { v4 as uuid } from "uuid";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`Multiple Databases`, function () {
    jest.setTimeout(60000);

    let session: LSession;
    let orbitdb1: Peerbit, orbitdb2: Peerbit, orbitdb3: Peerbit;

    let localDatabases: EventStore<string>[] = [];
    let remoteDatabasesA: EventStore<string>[] = [];
    let remoteDatabasesB: EventStore<string>[] = [];

    const dbCount = 2;

    // Create two IPFS instances and two OrbitDB instances (2 nodes/peers)
    beforeAll(async () => {
        session = await LSession.connected(3, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        orbitdb1 = await Peerbit.create(session.peers[0], {});
        orbitdb2 = await Peerbit.create(session.peers[1], {});
        orbitdb3 = await Peerbit.create(session.peers[2], {});
        orbitdb2._minReplicas = 3;
        orbitdb3._minReplicas = 3;
        orbitdb1._minReplicas = 3;
    });

    afterAll(async () => {
        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();

        if (orbitdb3) await orbitdb3.stop();
        await session.stop();
    });

    beforeEach(async () => {
        // Set write access for both clients

        // Open the databases on the first node
        const options = {};

        // Open the databases on the first node
        const topic = uuid();
        for (let i = 0; i < dbCount; i++) {
            const db = await orbitdb1.open(
                new EventStore<string>({ id: "local-" + i }),
                { ...options, topic: topic }
            );
            localDatabases.push(db);
        }
        for (let i = 0; i < dbCount; i++) {
            const db = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._store,
                    localDatabases[i].address!
                ),
                { topic: topic, ...options }
            );
            remoteDatabasesA.push(db);
        }

        for (let i = 0; i < dbCount; i++) {
            const db = await orbitdb3.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb3._store,
                    localDatabases[i].address!
                ),
                { topic: topic, ...options }
            );
            remoteDatabasesB.push(db);
        }

        // Wait for the peers to connect
        await waitForPeers(
            session.peers[0],
            [orbitdb2.id],
            getReplicationTopic(topic)
        );
        await waitForPeers(
            session.peers[1],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );
        await waitForPeers(
            session.peers[2],
            [orbitdb1.id],
            getReplicationTopic(topic)
        );

        await waitFor(() => orbitdb1._directConnections.size === 2);
        await waitFor(() => orbitdb2._directConnections.size === 2);
        await waitFor(() => orbitdb3._directConnections.size === 2);
    });

    afterEach(async () => {
        /*  for (let db of remoteDatabasesA)
     await db.drop()

   for (let db of remoteDatabasesB)
     await db.drop()

   for (let db of localDatabases)
     await db.drop() */
    });

    it("replicates multiple open databases", async () => {
        const entryCount = 1;
        const entryArr: number[] = [];

        // Create an array that we use to create the db entries
        for (let i = 1; i < entryCount + 1; i++) entryArr.push(i);

        // Write entries to each database
        for (let index = 0; index < dbCount; index++) {
            const db = localDatabases[index];
            entryArr.forEach((val) => db.add("hello-" + val));
        }

        // Function to check if all databases have been replicated
        const allReplicated = () => {
            return (
                remoteDatabasesA.every(
                    (db) => db.store._oplog.length === entryCount
                ) &&
                remoteDatabasesB.every(
                    (db) => db.store._oplog.length === entryCount
                )
            );
        };

        // check data
        await new Promise((resolve, reject) => {
            const interval = setInterval(async () => {
                if (allReplicated()) {
                    clearInterval(interval);

                    await delay(10000); // add some delay, so that we absorb any extra (unwanted) replication

                    // Verify that the databases contain all the right entries
                    remoteDatabasesA.forEach((db) => {
                        try {
                            const result = db
                                .iterator({ limit: -1 })
                                .collect().length;
                            expect(result).toEqual(entryCount);
                            expect(db.store._oplog.length).toEqual(entryCount);
                        } catch (error) {
                            reject(error);
                        }
                    });

                    remoteDatabasesB.forEach((db) => {
                        try {
                            const result = db
                                .iterator({ limit: -1 })
                                .collect().length;
                            expect(result).toEqual(entryCount);
                            expect(db.store._oplog.length).toEqual(entryCount);
                        } catch (error) {
                            reject(error);
                        }
                    });
                    resolve(true);
                }
            }, 200);
        });

        // check gracefully shut down (with no leak)
        let directConnections = 2;
        const subscriptions = orbitdb3.libp2p.pubsub.getTopics();
        expect(subscriptions.length).toEqual(directConnections + 2 + 1); //+ 1 for 2 replication topic (observer and replicator) + block topic
        for (let i = 0; i < dbCount; i++) {
            await remoteDatabasesB[i].drop();
            if (i === dbCount - 1) {
                await delay(3000);
                const connections = orbitdb3.libp2p.pubsub.getTopics();

                // Direct connection should close because no databases "in common" are open
                expect(connections).toHaveLength(0 + 1); // + "block" topic
            }
        }
    });
});
