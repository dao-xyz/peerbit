import assert from "assert";
import mapSeries from "p-each-series";
import { Entry } from "@dao-xyz/peerbit-log";
import { waitFor } from "@dao-xyz/peerbit-time";
import { jest } from "@jest/globals";
import { Peerbit } from "../peer";
import { EventStore, Operation } from "./utils/stores/event-store";
import { IStoreOptions } from "@dao-xyz/peerbit-store";
import { v4 as uuid } from "uuid";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

describe(`Replication`, function () {
    jest.setTimeout(60000);

    let session: LSession;
    let client1: Peerbit,
        client2: Peerbit,
        db1: EventStore<string>,
        db2: EventStore<string>;
    let topic: string;
    let options: IStoreOptions<any>;

    beforeEach(async () => {
        session = await LSession.connected(2);
        topic = uuid();

        client1 = await Peerbit.create(session.peers[0], { topic });
        client2 = await Peerbit.create(session.peers[1], { topic });

        options = Object.assign({}, options, {});
        db1 = await client1.open(new EventStore<string>({ id: "a" }), {
            ...options
        });
    });

    afterEach(async () => {
        options = {} as any;

        if (db1) await db1.drop();

        if (db2) await db2.drop();

        if (client1) await client1.stop();

        if (client2) await client2.stop();
        await session.stop();
    });

    it("replicates database of 1 entry", async () => {
        options = Object.assign({}, options);
        let done = false;
        db2 = await client2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                client2._store,
                db1.address!
            ),
            {
                ...options,
                onReplicationComplete: async () => {
                    expect(
                        db2.iterator({ limit: -1 }).collect().length
                    ).toEqual(1);

                    const db1Entries: Entry<Operation<string>>[] = db1
                        .iterator({ limit: -1 })
                        .collect();
                    expect(db1Entries.length).toEqual(1);
                    expect(
                        await client1.findReplicators(
                            db1.address.toString(),
                            db1Entries[0].gid,
                            client1._minReplicas
                        )
                    ).toContainAllValues(
                        [client1.id, client2.id].map((p) => p.toString())
                    );
                    expect(db1Entries[0].payload.getValue().value).toEqual(
                        value
                    );

                    const db2Entries: Entry<Operation<string>>[] = db2
                        .iterator({ limit: -1 })
                        .collect();
                    expect(db2Entries.length).toEqual(1);
                    expect(
                        await client2.findReplicators(
                            db1.address.toString(),
                            db2Entries[0].gid,
                            client1._minReplicas
                        )
                    ).toContainValues(
                        [client1.id, client2.id].map((p) => p.toString())
                    );
                    expect(db2Entries[0].payload.getValue().value).toEqual(
                        value
                    );
                    done = true;
                },
            }
        );
        await waitForPeers(session.peers[1], [client1.id], topic);
        await waitForPeers(session.peers[0], [client2.id], topic);

        const value = "hello";
        await db1.add(value);

        await waitFor(() => done);
    });

    it("replicates database of 100 entries", async () => {
        await waitForPeers(session.peers[1], [client1.id], topic);

        options = Object.assign({}, options);

        let done = false;
        db2 = await client2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                client2._store,
                db1.address!
            ),
            {
                ...options,
                onReplicationComplete: () => {
                    // Once db2 has finished replication, make sure it has all elements
                    // and process to the asserts below
                    const all = db2.iterator({ limit: -1 }).collect().length;
                    done = all === entryCount;
                },
            }
        );

        const entryCount = 100;
        const entryArr: number[] = [];

        for (let i = 0; i < entryCount; i++) {
            entryArr.push(i);
        }

        const add = (i: number) => db1.add("hello" + i);
        await mapSeries(entryArr, add);

        await waitFor(() => done);
        const entries = db2.iterator({ limit: -1 }).collect();
        expect(entries.length).toEqual(entryCount);
        expect(entries[0].payload.getValue().value).toEqual("hello0");
        expect(entries[entries.length - 1].payload.getValue().value).toEqual(
            "hello99"
        );
    });

    it("emits correct replication info", async () => {
        await waitForPeers(session.peers[1], [client1.id], topic);

        options = Object.assign({}, options);

        // Test that none of the entries gets into the replication queue twice
        const replicateSet = new Set();

        // Verify that progress count increases monotonically by saving
        // each event's current progress into an array
        let progressEvents: number = 0;
        const progressEventsEntries: Entry<any>[] = [];

        let done = false;

        db2 = await client2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                client2._store,
                db1.address!
            ),
            {
                ...options,
                onReplicationQueued: (store, entry) => {
                    if (!replicateSet.has(entry.hash)) {
                        replicateSet.add(entry.hash);
                    } else {
                        fail(
                            new Error(
                                "Shouldn't have started replication twice for entry " +
                                entry.hash +
                                "\n" +
                                entry.payload.getValue().value
                            )
                        );
                    }
                },
                onReplicationFetch: (store, entry) => {
                    progressEvents += 1;
                    progressEventsEntries.push(entry);
                },

                onReplicationComplete: (store) => {
                    // Once db2 has finished replication, make sure it has all elements
                    // and process to the asserts below
                    const all = db2.iterator({ limit: -1 }).collect().length;
                    done = all === entryCount;
                },
            }
        );

        const entryCount = 99;
        // Trigger replication
        let adds: number[] = [];
        for (let i = 0; i < entryCount; i++) {
            adds.push(i);
        }

        await mapSeries(adds, (i) => db1.add("hello " + i));

        await waitFor(() => done);

        // All entries should be in the database
        expect(db2.iterator({ limit: -1 }).collect().length).toEqual(
            entryCount
        );

        // progress events should increase monotonically
        expect(progressEvents).toEqual(entryCount);
    });

    it("emits correct replication info on fresh replication", async () => {
        const entryCount = 15;

        // Trigger replication
        const adds: number[] = [];
        for (let i = 0; i < entryCount; i++) {
            adds.push(i);
        }

        const add = async (i: number) => {
            process.stdout.write(
                "\rWriting " + (i + 1) + " / " + entryCount + " "
            );
            await db1.add("hello " + i);
        };

        await mapSeries(adds, add);

        // Open second instance again
        options = {};

        // Test that none of the entries gets into the replication queue twice
        const replicateSet = new Set();

        // Verify that progress count increases monotonically by saving
        // each event's current progress into an array
        let progressEvents: number = 0;

        let replicatedEventCount = 0;
        let done = false;

        db2 = await client2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                client2._store,
                db1.address!
            ),
            {
                ...options,
                onReplicationQueued: (store, entry) => {
                    if (!replicateSet.has(entry.hash)) {
                        replicateSet.add(entry.hash);
                    } else {
                        fail(
                            new Error(
                                "Shouldn't have started replication twice for entry " +
                                entry.hash
                            )
                        );
                    }
                },
                onReplicationFetch: (store, entry) => {
                    progressEvents += 1;
                },
                onReplicationComplete: (store) => {
                    replicatedEventCount++;
                    // Once db2 has finished replication, make sure it has all elements
                    // and process to the asserts below
                    const all = db2.iterator({ limit: -1 }).collect().length;
                    done = all === entryCount;
                },
            }
        );

        await waitFor(() => done);

        // All entries should be in the database
        expect(db2.iterator({ limit: -1 }).collect().length).toEqual(
            entryCount
        );
        // 'replicated' event should've been received only once
        expect(replicatedEventCount).toEqual(1);

        // progress events should (increase monotonically)
        expect(progressEvents).toEqual(entryCount);
    });

    it("emits correct replication info in two-way replication", async () => {
        await waitForPeers(session.peers[1], [client1.id], topic);

        const entryCount = 15;

        // Trigger replication
        const adds: number[] = [];
        for (let i = 0; i < entryCount; i++) {
            adds.push(i);
        }

        const add = async (i: number) => {
            process.stdout.write(
                "\rWriting " + (i + 1) + " / " + entryCount + " "
            );
            await Promise.all([
                db1.add("hello-1-" + i),
                db2.add("hello-2-" + i),
            ]);
        };

        // Open second instance again
        let options = {
            overwrite: true,
        };
        // Test that none of the entries gets into the replication queue twice
        const replicateSet = new Set();
        let done = false;

        db2 = await client2.open<EventStore<string>>(
            await EventStore.load<EventStore<string>>(
                client2._store,
                db1.address!
            ),
            {
                ...options,
                onReplicationComplete: (store) => {
                    // Once db2 has finished replication, make sure it has all elements
                    // and process to the asserts below
                    const all = db2.iterator({ limit: -1 }).collect().length;
                    done = all === entryCount * 2;
                },
                onReplicationQueued: (store, entry) => {
                    if (!replicateSet.has(entry.hash)) {
                        replicateSet.add(entry.hash);
                    } else {
                        fail(
                            new Error(
                                "Shouldn't have started replication twice for entry " +
                                entry.hash
                            )
                        );
                    }
                },
            }
        );

        expect(db1.address).toBeDefined();
        expect(db2.address).toBeDefined();
        expect(db1.address!.toString()).toEqual(db2.address!.toString());

        await mapSeries(adds, add);
        await waitFor(() => done);

        // Database values should match

        try {
            await waitFor(
                () =>
                    db1.store.oplog.values.length ===
                    db2.store.oplog.values.length
            );
        } catch (error) {
            throw new Error(
                `${db1.store.oplog.values.length}  +" --- " + ${db2.store.oplog.values.length}`
            );
        }

        const values1 = db1.iterator({ limit: -1 }).collect();
        const values2 = db2.iterator({ limit: -1 }).collect();
        expect(values1.length).toEqual(values2.length);
        for (let i = 0; i < values1.length; i++) {
            assert(values1[i].equals(values2[i]));
        }
        // All entries should be in the database
        expect(values1.length).toEqual(entryCount * 2);
        expect(values2.length).toEqual(entryCount * 2);
    });
});
