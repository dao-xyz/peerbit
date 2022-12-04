import { getReplicationTopic, Peerbit } from "../peer";
import { EventStore } from "./utils/stores/event-store";
import mapSeries from "p-each-series";

// Include test utilities
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
// @ts-ignore
import { v4 as uuid } from "uuid";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`Replicate and Load`, function () {
    let session: LSession;
    let orbitdb1: Peerbit, orbitdb2: Peerbit;

    beforeAll(async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
        orbitdb1 = await Peerbit.create(session.peers[0], {});
        orbitdb2 = await Peerbit.create(session.peers[1], {});

        // Connect the peers manually to speed up test times
    });

    afterAll(async () => {
        if (orbitdb1) await orbitdb1.stop();

        if (orbitdb2) await orbitdb2.stop();

        await session.stop();
    });

    describe("two peers", function () {
        let db1: EventStore<string>, db2: EventStore<string>, topic: string;

        const openDatabases = async () => {
            // Set write access for both clients
            topic = uuid();
            db1 = await orbitdb1.open(
                new EventStore<string>({
                    id: "events",
                }),
                { topic: topic }
            );
            // Set 'localOnly' flag on and it'll error if the database doesn't exist locally
            db2 = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._store,
                    db1.address!
                ),
                { topic: topic }
            );
        };

        beforeAll(async () => {
            await openDatabases();

            expect(db1.address!.toString()).toEqual(db2.address!.toString());

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
        });

        afterAll(async () => {
            if (db1) {
                await db1.drop();
            }

            if (db2) {
                await db2.drop();
            }
        });

        it("replicates database of 100 entries and loads it from the disk", async () => {
            const entryCount = 100;
            const entryArr: number[] = [];
            let timer: any;

            for (let i = 0; i < entryCount; i++) entryArr.push(i);

            await mapSeries(entryArr, (i) => db1.add("hello" + i));

            return new Promise((resolve, reject) => {
                timer = setInterval(async () => {
                    if (db2.store._oplog.length === entryCount) {
                        clearInterval(timer);

                        const items = db2.iterator({ limit: -1 }).collect();
                        expect(items.length).toEqual(entryCount);
                        expect(items[0].payload.getValue().value).toEqual(
                            "hello0"
                        );
                        expect(
                            items[items.length - 1].payload.getValue().value
                        ).toEqual("hello" + (items.length - 1));

                        try {
                            // Get the previous address to make sure nothing mutates it
                            /* TODO, since new changes, below might not be applicable 

                                // Open the database again (this time from the disk)
                                options = Object.assign({}, options, { directory: dbPath1, create: false })
                                const db3 = await orbitdb1.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb1.libp2p, db1.address), { replicationTopic, ...options }) // We set replicationTopic to "_" because if the replication topic is the same, then error will be thrown for opening the same store
                                // Set 'localOnly' flag on and it'll error if the database doesn't exist locally
                                options = Object.assign({}, options, { directory: dbPath2, localOnly: true })
                                const db4 = await orbitdb2.open<EventStore<string>>(await EventStore.load<EventStore<string>>(orbitdb2.libp2p, db1.address), { replicationTopic, ...options }) // We set replicationTopic to "_" because if the replication topic is the same, then error will be thrown for opening the same store

                                await db3.load()
                                await db4.load()

                                // Make sure we have all the entries in the databases
                                const result1 = db3.iterator({ limit: -1 }).collect()
                                const result2 = db4.iterator({ limit: -1 }).collect()
                                expect(result1.length).toEqual(entryCount)
                                expect(result2.length).toEqual(entryCount)

                                await db3.drop()
                                await db4.drop() */
                        } catch (e: any) {
                            reject(e);
                        }
                        resolve(true);
                    }
                }, 1000);
            });
        });
    });
});
