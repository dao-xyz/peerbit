import assert from "assert";
import mapSeries from "p-map-series";
import { Address } from "@dao-xyz/peerbit-program";
import { Peerbit } from "../peer.js";
import { EventStore } from "./utils/stores/event-store.js";
import { jest } from "@jest/globals";
// @ts-ignore
import { v4 as uuid } from "uuid";

// Include test utilities
import {
    nodeConfig as config,
    Session,
    createStore,
} from "@dao-xyz/peerbit-test-utils";
import { waitFor } from "@dao-xyz/peerbit-time";

const dbPath = "./orbitdb/tests/persistency";

/* tests.forEach(test => {*/
describe(`orbit-db - load (js-ipfs)`, () => {
    //${test.title}
    jest.setTimeout(config.timeout * 5);
    jest.retryTimes(1); // TODO Side effects may cause failures

    const entryCount = 10;

    describe("load", function () {
        let db: EventStore<string>, address: string, orbitdb1: Peerbit;
        let session: Session;

        beforeAll(async () => {
            session = await Session.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            orbitdb1 = await Peerbit.create(session.peers[0].ipfs, {
                directory: dbPath + "/" + uuid(),
                storage: {
                    createStore: (string: string) => createStore(string),
                },
            }); // We do custom store to prevent sideeffects when writing to disc

            const entryArr: number[] = [];

            for (let i = 0; i < entryCount; i++) entryArr.push(i);

            db = await orbitdb1.open(new EventStore<string>({}), {
                replicationTopic: uuid(),
            });
            address = db.address!.toString();
            await mapSeries(entryArr, (i) => db.add("hello" + i));
            await db.close();
        });

        afterEach(async () => {
            await db?.drop();

            if (orbitdb1) await orbitdb1.stop();
        });

        it("loads database from local cache", async () => {
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                { replicationTopic: uuid() }
            );
            await db.load();
            await waitFor(
                () => db.iterator({ limit: -1 }).collect().length === entryCount
            );
            const items = db.iterator({ limit: -1 }).collect();
            expect(items.length).toEqual(entryCount);
            expect(items[0].payload.getValue().value).toEqual("hello0");
            expect(items[items.length - 1].payload.getValue().value).toEqual(
                "hello" + (entryCount - 1)
            );
        });

        it("loads database partially", async () => {
            const amount = 3;
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                { replicationTopic: uuid() }
            );
            await db.store.load(amount);
            await waitFor(
                () => db.iterator({ limit: -1 }).collect().length === amount
            );
            const items = db.iterator({ limit: -1 }).collect();
            expect(items.length).toEqual(amount);
            expect(items[0].payload.getValue().value).toEqual(
                "hello" + (entryCount - amount)
            );
            expect(items[1].payload.getValue().value).toEqual(
                "hello" + (entryCount - amount + 1)
            );
            expect(items[items.length - 1].payload.getValue().value).toEqual(
                "hello" + (entryCount - 1)
            );
        });

        it("load and close several times", async () => {
            const amount = 8;
            for (let i = 0; i < amount; i++) {
                db = await orbitdb1.open(
                    await EventStore.load<EventStore<string>>(
                        orbitdb1._ipfs,
                        Address.parse(address)
                    ),
                    { replicationTopic: uuid() }
                );
                await db.load();
                await waitFor(
                    () =>
                        db.iterator({ limit: -1 }).collect().length ===
                        entryCount
                );
                const items = db.iterator({ limit: -1 }).collect();
                expect(items.length).toEqual(entryCount);
                expect(items[0].payload.getValue().value).toEqual("hello0");
                expect(items[1].payload.getValue().value).toEqual("hello1");
                expect(
                    items[items.length - 1].payload.getValue().value
                ).toEqual("hello" + (entryCount - 1));
                await db.close();
            }
        });

        /* it('closes database while loading', async () => { TODO fix
      db = await orbitdb1.open(address, { type: EVENT_STORE_TYPE, replicationConcurrency: 1 })
      return new Promise(async (resolve, reject) => {
        // don't wait for load to finish
        db.load()
          .then(() => reject("Should not finish loading?"))
          .catch(e => {
            if (e.toString() !== 'ReadError: Database is not open') {
              reject(e)
            } else {
              expect(db._cache._store).toEqual( null)
              resolve(true)
            }
          })
        await db.close()
      })
    }) */

        it("load, add one, close - several times", async () => {
            const amount = 8;
            for (let i = 0; i < amount; i++) {
                db = await orbitdb1.open(
                    await EventStore.load<EventStore<string>>(
                        orbitdb1._ipfs,
                        Address.parse(address)
                    ),
                    { replicationTopic: uuid() }
                );
                await db.load();
                await waitFor(
                    () =>
                        db.iterator({ limit: -1 }).collect().length ===
                        entryCount + i
                );
                await db.add("hello" + (entryCount + i));
                await waitFor(
                    () =>
                        db.iterator({ limit: -1 }).collect().length ===
                        entryCount + i + 1
                );
                const items = db.iterator({ limit: -1 }).collect();
                expect(items.length).toEqual(entryCount + i + 1);
                expect(
                    items[items.length - 1].payload.getValue().value
                ).toEqual("hello" + (entryCount + i));
                await db.close();
            }
        });

        it("loading a database emits 'ready' event", async () => {
            let done = false;
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                {
                    replicationTopic: uuid(),
                    onReady: async (store) => {
                        await waitFor(
                            () =>
                                db.iterator({ limit: -1 }).collect().length ===
                                entryCount
                        );
                        const items = db.iterator({ limit: -1 }).collect();
                        expect(items.length).toEqual(entryCount);
                        expect(items[0].payload.getValue().value).toEqual(
                            "hello0"
                        );
                        expect(
                            items[items.length - 1].payload.getValue().value
                        ).toEqual("hello" + (entryCount - 1));
                        done = true;
                    },
                }
            );
            await db.load();
            await waitFor(() => done);
        });

        it("loading a database emits 'load.progress' event", async () => {
            let count = 0;
            let done = false;
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                {
                    replicationTopic: uuid(),
                    onLoadProgress: (store, entry) => {
                        count++;
                        expect(address).toEqual(db.address!.toString());

                        const { progress, max } = db.store.replicationStatus;
                        expect(max).toEqual(BigInt(entryCount));
                        expect(progress).toEqual(BigInt(count));

                        assert.notEqual(entry.hash, null);
                        assert.notEqual(entry, null);

                        if (
                            progress === BigInt(entryCount) &&
                            count === entryCount
                        ) {
                            setTimeout(() => {
                                done = true;
                            }, 200);
                        }
                    },
                }
            );
            await db.load();
            await waitFor(() => done);
        });
    });

    describe("load from empty snapshot", function () {
        let db: EventStore<string>,
            address: string,
            orbitdb1: Peerbit,
            session: Session;

        beforeAll(async () => {
            session = await Session.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            orbitdb1 = await Peerbit.create(session.peers[0].ipfs, {
                directory: dbPath + "/" + uuid(),
            });
        });
        afterEach(async () => {
            await db.drop();
            if (orbitdb1) await orbitdb1.stop();
        });

        it("loads database from an empty snapshot", async () => {
            const options = { replicationTopic: uuid() };
            db = await orbitdb1.open(new EventStore<string>({}), options);
            address = db.address!.toString();
            await db.saveSnapshot();
            await db.close();

            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                options
            );
            await db.loadFromSnapshot();
            const items = db.iterator({ limit: -1 }).collect();
            expect(items.length).toEqual(0);
        });
    });

    describe("load from snapshot", function () {
        let db: EventStore<string>,
            address: string,
            orbitdb1: Peerbit,
            session: Session;

        beforeAll(async () => {
            session = await Session.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            orbitdb1 = await Peerbit.create(session.peers[0].ipfs, {
                directory: dbPath + "/" + uuid(),
                storage: {
                    createStore: (string: string) => createStore(string),
                },
            });

            const entryArr: number[] = [];

            for (let i = 0; i < entryCount; i++) entryArr.push(i);

            db = await orbitdb1.open(new EventStore<string>({}), {
                replicationTopic: uuid(),
            });
            address = db.address!.toString();
            await mapSeries(entryArr, (i) => db.add("hello" + i));
            await db.saveSnapshot();
            await db.close();
        });

        afterEach(async () => {
            await db?.drop();

            if (orbitdb1) await orbitdb1.stop();
        });

        it("loads database from snapshot", async () => {
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                { replicationTopic: uuid() }
            );
            await db.loadFromSnapshot();
            const items = db.iterator({ limit: -1 }).collect();
            expect(items.length).toEqual(entryCount);
            expect(items[0].payload.getValue().value).toEqual("hello0");
            expect(items[entryCount - 1].payload.getValue().value).toEqual(
                "hello" + (entryCount - 1)
            );
        });

        it("load, add one and save snapshot several times", async () => {
            const amount = 4;
            for (let i = 0; i < amount; i++) {
                db = await orbitdb1.open(
                    await EventStore.load<EventStore<string>>(
                        orbitdb1._ipfs,
                        Address.parse(address)
                    ),
                    { replicationTopic: uuid() }
                );
                await db.loadFromSnapshot();
                const expectedCount = entryCount + i;
                await waitFor(
                    () =>
                        db.iterator({ limit: -1 }).collect().length ===
                        expectedCount
                );
                await db.add("hello" + (entryCount + i));
                const items = db.iterator({ limit: -1 }).collect();
                expect(items.length).toEqual(expectedCount + 1);
                expect(items[0].payload.getValue().value).toEqual("hello0");
                expect(
                    items[items.length - 1].payload.getValue().value
                ).toEqual("hello" + (entryCount + i));
                await db.saveSnapshot();
                await db.close();
            }
        });

        it("throws an error when trying to load a missing snapshot", async () => {
            const replicationTopic = { replicationTopic: uuid() };
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                replicationTopic
            );
            await db.drop();
            db = null as any;
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                replicationTopic
            );

            let err;
            try {
                await db.loadFromSnapshot();
            } catch (e: any) {
                err = e.toString();
            }
            expect(err).toEqual(
                `Error: Snapshot for ${db.store.id} not found!`
            );
        });

        it("loading a database emits 'ready' event", async () => {
            let done = false;
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                {
                    replicationTopic: uuid(),
                    onReady: (store) => {
                        const items = db.iterator({ limit: -1 }).collect();
                        expect(items.length).toEqual(entryCount);
                        expect(items[0].payload.getValue().value).toEqual(
                            "hello0"
                        );
                        expect(
                            items[entryCount - 1].payload.getValue().value
                        ).toEqual("hello" + (entryCount - 1));
                        done = true;
                    },
                }
            );
            await db.loadFromSnapshot();
            await waitFor(() => done);
        });

        it("loading a database emits 'load.progress' event", async () => {
            let done = false;
            let count = 0;
            db = await orbitdb1.open(
                await EventStore.load<EventStore<string>>(
                    orbitdb1._ipfs,
                    Address.parse(address)
                ),
                {
                    replicationTopic: uuid(),
                    onLoadProgress: (store, entry) => {
                        count++;
                        expect(address).toEqual(db.address!.toString());
                        const { progress, max } = db.store.replicationStatus;
                        expect(max).toEqual(BigInt(entryCount));
                        expect(progress).toEqual(BigInt(count));

                        assert.notEqual(entry.hash, null);
                        assert.notEqual(entry, null);
                        if (
                            progress === BigInt(entryCount) &&
                            count === entryCount
                        ) {
                            done = true;
                        }
                    },
                }
            );
            await db.loadFromSnapshot();
            await waitFor(() => done);
        });
    });
});
/* }) */
