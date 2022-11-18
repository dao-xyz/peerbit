import assert from "assert";
import mapSeries from "p-each-series";
import rmrf from "rimraf";
import { Entry } from "@dao-xyz/ipfs-log";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { jest } from "@jest/globals";
import { Controller } from "ipfsd-ctl";
import { IPFS } from "ipfs-core-types";
import { Peerbit } from "../peer";

import { EventStore, Operation } from "./utils/stores/event-store";
import { IStoreOptions } from "@dao-xyz/peerbit-store";
// @ts-ignore
import { v4 as uuid } from "uuid";
// Include test utilities
import {
    nodeConfig as config,
    startIpfs,
    stopIpfs,
    testAPIs,
    connectPeers,
    waitForPeers,
} from "@dao-xyz/peerbit-test-utils";

const orbitdbPath1 = "./orbitdb/tests/replication/1";
const orbitdbPath2 = "./orbitdb/tests/replication/2";
const dbPath1 = "./orbitdb/tests/replication/1/db1";
const dbPath2 = "./orbitdb/tests/replication/2/db2";

Object.keys(testAPIs).forEach((API) => {
    describe(`orbit-db - Replication (${API})`, function () {
        jest.setTimeout(config.timeout * 2);

        let ipfsd1: Controller, ipfsd2: Controller, ipfs1: IPFS, ipfs2: IPFS;
        let orbitdb1: Peerbit,
            orbitdb2: Peerbit,
            db1: EventStore<string>,
            db2: EventStore<string>;
        let replicationTopic: string;
        let options: IStoreOptions<any>;

        beforeAll(async () => {
            ipfsd1 = await startIpfs(API, config.daemon1);
            ipfsd2 = await startIpfs(API, config.daemon2);
            ipfs1 = ipfsd1.api;
            ipfs2 = ipfsd2.api;
            // Connect the peers manually to speed up test times
            const isLocalhostAddress = (addr: string) =>
                addr.toString().includes("127.0.0.1");
            await connectPeers(ipfs1, ipfs2, { filter: isLocalhostAddress });
        });

        afterAll(async () => {
            if (ipfsd1) await stopIpfs(ipfsd1);

            if (ipfsd2) await stopIpfs(ipfsd2);
        });

        beforeEach(async () => {
            rmrf.sync(orbitdbPath1);
            rmrf.sync(orbitdbPath2);
            rmrf.sync(dbPath1);
            rmrf.sync(dbPath2);

            orbitdb1 = await Peerbit.create(ipfs1, { directory: orbitdbPath1 });
            orbitdb2 = await Peerbit.create(ipfs2, { directory: orbitdbPath2 });

            options = Object.assign({}, options, { directory: dbPath1 });
            replicationTopic = uuid();
            db1 = await orbitdb1.open(new EventStore<string>({ id: "a" }), {
                ...options,
                replicationTopic,
            });
        });

        afterEach(async () => {
            options = {} as any;

            if (db1) await db1.drop();

            if (db2) await db2.drop();

            if (orbitdb1) await orbitdb1.stop();

            if (orbitdb2) await orbitdb2.stop();
        });

        it("replicates database of 1 entry", async () => {
            options = Object.assign({}, options, { directory: dbPath2 });
            let done = false;
            db2 = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._ipfs,
                    db1.address!
                ),
                {
                    replicationTopic,
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
                            await orbitdb1.findReplicators(
                                replicationTopic,
                                db1.address.toString(),
                                true,
                                db1Entries[0].gid,
                                orbitdb1._minReplicas
                            )
                        ).toContainValues(
                            [orbitdb1.id, orbitdb2.id].map((p) => p.toString())
                        );
                        expect(db1Entries[0].payload.getValue().value).toEqual(
                            value
                        );

                        const db2Entries: Entry<Operation<string>>[] = db2
                            .iterator({ limit: -1 })
                            .collect();
                        expect(db2Entries.length).toEqual(1);
                        expect(
                            await orbitdb2.findReplicators(
                                replicationTopic,
                                db1.address.toString(),
                                true,
                                db2Entries[0].gid,
                                orbitdb1._minReplicas
                            )
                        ).toContainValues(
                            [orbitdb1.id, orbitdb2.id].map((p) => p.toString())
                        );
                        expect(db2Entries[0].payload.getValue().value).toEqual(
                            value
                        );
                        done = true;
                    },
                }
            );
            await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic);

            await waitFor(() => orbitdb1._directConnections.size === 1);
            await waitFor(() => orbitdb2._directConnections.size === 1);

            const value = "hello";
            await db1.add(value);

            await waitFor(() => done);
        });

        it("replicates database of 100 entries", async () => {
            await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic);

            options = Object.assign({}, options, { directory: dbPath2 });

            let done = false;
            db2 = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._ipfs,
                    db1.address!
                ),
                {
                    replicationTopic,
                    ...options,
                    onReplicationComplete: () => {
                        // Once db2 has finished replication, make sure it has all elements
                        // and process to the asserts below
                        const all = db2
                            .iterator({ limit: -1 })
                            .collect().length;
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
            expect(
                entries[entries.length - 1].payload.getValue().value
            ).toEqual("hello99");
        });

        it("emits correct replication info", async () => {
            await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic);

            options = Object.assign({}, options, { directory: dbPath2 });

            // Test that none of the entries gets into the replication queue twice
            const replicateSet = new Set();

            // Verify that progress count increases monotonically by saving
            // each event's current progress into an array
            let progressEvents: number = 0;
            const progressEventsEntries: any[] = [];

            let done = false;

            db2 = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._ipfs,
                    db1.address!
                ),
                {
                    replicationTopic,
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
                    onReplicationProgress: (store, entry) => {
                        progressEvents += 1;
                        progressEventsEntries.push(entry);
                    },

                    onReplicationComplete: (store) => {
                        // Once db2 has finished replication, make sure it has all elements
                        // and process to the asserts below
                        const all = db2
                            .iterator({ limit: -1 })
                            .collect().length;
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

            // Verify replicator state
            expect(db2.store._replicator.tasksRunning).toEqual(0);
            expect(db2.store._replicator.tasksQueued).toEqual(0);
            expect(db2.store._replicator.unfinished.length).toEqual(0);
            // Replicator's internal caches should be empty
            expect(db2.store._replicator._logs.length).toEqual(0);
            expect(Object.keys(db2.store._replicator._fetching).length).toEqual(
                0
            );
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
            options = {
                directory: dbPath2,
            };

            // Test that none of the entries gets into the replication queue twice
            const replicateSet = new Set();

            // Verify that progress count increases monotonically by saving
            // each event's current progress into an array
            let progressEvents: number = 0;

            let replicatedEventCount = 0;
            let done = false;

            db2 = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._ipfs,
                    db1.address!
                ),
                {
                    replicationTopic,
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
                    onReplicationProgress: (store, entry) => {
                        progressEvents += 1;
                    },
                    onReplicationComplete: (store) => {
                        replicatedEventCount++;
                        // Once db2 has finished replication, make sure it has all elements
                        // and process to the asserts below
                        const all = db2
                            .iterator({ limit: -1 })
                            .collect().length;
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

            // Verify replicator state
            expect(db2.store._replicator.tasksRunning).toEqual(0);
            expect(db2.store._replicator.tasksQueued).toEqual(0);
            expect(db2.store._replicator.unfinished.length).toEqual(0);
            // Replicator's internal caches should be empty
            expect(db2.store._replicator._logs.length).toEqual(0);
            expect(Object.keys(db2.store._replicator._fetching).length).toEqual(
                0
            );
        });

        it("emits correct replication info in two-way replication", async () => {
            await waitForPeers(ipfs2, [orbitdb1.id], replicationTopic);

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
                directory: dbPath2 + "2",
                overwrite: true,
            };
            // Test that none of the entries gets into the replication queue twice
            const replicateSet = new Set();
            let done = false;

            db2 = await orbitdb2.open<EventStore<string>>(
                await EventStore.load<EventStore<string>>(
                    orbitdb2._ipfs,
                    db1.address!
                ),
                {
                    replicationTopic,
                    ...options,
                    onReplicationComplete: (store) => {
                        // Once db2 has finished replication, make sure it has all elements
                        // and process to the asserts below
                        const all = db2
                            .iterator({ limit: -1 })
                            .collect().length;
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

            await waitFor(
                () =>
                    db1.store.oplog.values.length ===
                    db2.store.oplog.values.length
            );
            const values1 = db1.iterator({ limit: -1 }).collect();
            const values2 = db2.iterator({ limit: -1 }).collect();
            expect(values1.length).toEqual(values2.length);
            for (let i = 0; i < values1.length; i++) {
                assert(values1[i].equals(values2[i]));
            }
            // All entries should be in the database
            expect(values1.length).toEqual(entryCount * 2);
            expect(values2.length).toEqual(entryCount * 2);

            // Verify replicator state
            expect(db2.store._replicator.tasksRunning).toEqual(0);
            expect(db2.store._replicator.tasksQueued).toEqual(0);
            expect(db2.store._replicator.unfinished.length).toEqual(0);
            // Replicator's internal caches should be empty
            expect(db2.store._replicator._logs.length).toEqual(0);
            expect(Object.keys(db2.store._replicator._fetching).length).toEqual(
                0
            );
        });
    });
});
