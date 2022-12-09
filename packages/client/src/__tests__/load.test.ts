import assert from "assert";
import mapSeries from "p-map-series";
import { Address, Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "../peer.js";
import { EventStore } from "./utils/stores/event-store.js";
import { jest } from "@jest/globals";
import { v4 as uuid } from "uuid";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { waitFor } from "@dao-xyz/peerbit-time";
import { field, variant } from "@dao-xyz/borsh";

const dbPath = "./tmp/tests/persistency";

describe(`load`, () => {
    jest.retryTimes(1); // TODO Side effects may cause failures

    const entryCount = 10;

    describe("load single", function () {
        let db: EventStore<string>, address: string, client1: Peerbit;
        let session: LSession;

        beforeAll(async () => {
            session = await LSession.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            client1 = await Peerbit.create(session.peers[0], {
                directory: dbPath + "/" + uuid(),
                storage: {
                    createStore: (string?: string) => createStore(string),
                },
            }); // We do custom store to prevent sideeffects when writing to disc

            const entryArr: number[] = [];

            for (let i = 0; i < entryCount; i++) entryArr.push(i);

            db = await client1.open(new EventStore<string>({}), {
                topic: uuid(),
            });
            address = db.address!.toString();
            await mapSeries(entryArr, (i) => db.add("hello" + i));
            await db.close();
        });

        afterEach(async () => {
            await db?.drop();

            if (client1) await client1.stop();
        });

        it("loads database from local cache", async () => {
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                { topic: uuid() }
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
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                { topic: uuid() }
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
                db = await client1.open(
                    await EventStore.load<EventStore<string>>(
                        client1._store,
                        Address.parse(address)
                    ),
                    { topic: uuid() }
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
      db = await client1.open(address, { type: EVENT_STORE_TYPE, replicationConcurrency: 1 })
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
                db = await client1.open(
                    await EventStore.load<EventStore<string>>(
                        client1._store,
                        Address.parse(address)
                    ),
                    { topic: uuid() }
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
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                {
                    topic: uuid(),
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
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                {
                    topic: uuid(),
                    onLoadProgress: (store, entry) => {
                        count++;
                        expect(address).toEqual(db.address!.toString());

                        assert.notEqual(entry.hash, null);
                        assert.notEqual(entry, null);
                        if (count === entryCount) {
                            done = true;
                        }
                    },
                }
            );
            await db.load();
            await waitFor(() => done);
        });
    });
    describe("load multiple", function () {
        @variant("multiple")
        class MultipleStores extends Program {
            @field({ type: EventStore })
            db: EventStore<string>;

            @field({ type: EventStore })
            db2: EventStore<string>;

            constructor(
                db: EventStore<string> = new EventStore(),
                db2: EventStore<string> = new EventStore()
            ) {
                super();
                this.db = db;
                this.db2 = db2;
            }
            async setup() {
                await this.db.setup();
                await this.db2.setup();
            }
        }
        let db: MultipleStores, address: string, client1: Peerbit;
        let session: LSession;

        beforeAll(async () => {
            session = await LSession.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            client1 = await Peerbit.create(session.peers[0], {
                directory: dbPath + "/" + uuid(),
                storage: {
                    createStore: (string?: string) => createStore(string),
                },
            }); // We do custom store to prevent sideeffects when writing to disc

            const entryArr: number[] = [];

            for (let i = 0; i < entryCount; i++) entryArr.push(i);

            db = await client1.open(new MultipleStores(), {
                topic: uuid(),
            });
            address = db.address!.toString();
            await mapSeries(entryArr, (i) => db.db.add("a" + i));
            await mapSeries(entryArr, (i) => db.db2.add("b" + i));
            await db.close();
        });

        afterEach(async () => {
            await db?.drop();

            if (client1) await client1.stop();
        });

        it("loads database from local cache", async () => {
            db = await client1.open(
                await MultipleStores.load<MultipleStores>(
                    client1._store,
                    Address.parse(address)
                ),
                { topic: uuid() }
            );
            await db.load();
            await waitFor(
                () =>
                    db.db.iterator({ limit: -1 }).collect().length ===
                    entryCount
            );
            await waitFor(
                () =>
                    db.db2.iterator({ limit: -1 }).collect().length ===
                    entryCount
            );

            const itemsA = db.db.iterator({ limit: -1 }).collect();
            expect(itemsA.length).toEqual(entryCount);
            expect(itemsA[0].payload.getValue().value).toEqual("a0");
            expect(itemsA[itemsA.length - 1].payload.getValue().value).toEqual(
                "a" + (entryCount - 1)
            );

            const itemsB = db.db2.iterator({ limit: -1 }).collect();
            expect(itemsB.length).toEqual(entryCount);
            expect(itemsB[0].payload.getValue().value).toEqual("b0");
            expect(itemsB[itemsB.length - 1].payload.getValue().value).toEqual(
                "b" + (entryCount - 1)
            );
        });

        /* it("loads database partially", async () => {
            const amount = 3;
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                { topic: uuid() }
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
                db = await client1.open(
                    await EventStore.load<EventStore<string>>(
                        client1._store,
                        Address.parse(address)
                    ),
                    { topic: uuid() }
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
        }); */

        /* it('closes database while loading', async () => { TODO fix
      db = await client1.open(address, { type: EVENT_STORE_TYPE, replicationConcurrency: 1 })
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

        /*  it("load, add one, close - several times", async () => {
             const amount = 8;
             for (let i = 0; i < amount; i++) {
                 db = await client1.open(
                     await EventStore.load<EventStore<string>>(
                         client1._store,
                         Address.parse(address)
                     ),
                     { topic: uuid() }
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
             db = await client1.open(
                 await EventStore.load<EventStore<string>>(
                     client1._store,
                     Address.parse(address)
                 ),
                 {
                     topic: uuid(),
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
             db = await client1.open(
                 await EventStore.load<EventStore<string>>(
                     client1._store,
                     Address.parse(address)
                 ),
                 {
                     topic: uuid(),
                     onLoadProgress: (store, entry) => {
                         count++;
                         expect(address).toEqual(db.address!.toString());
 
                         assert.notEqual(entry.hash, null);
                         assert.notEqual(entry, null);
                         if (count === entryCount) {
                             done = true;
                         }
                     },
                 }
             );
             await db.load();
             await waitFor(() => done);
         }); */
    });

    describe("load from empty snapshot", function () {
        let db: EventStore<string>,
            address: string,
            client1: Peerbit,
            session: LSession;

        beforeAll(async () => {
            session = await LSession.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            client1 = await Peerbit.create(session.peers[0], {
                directory: dbPath + "/" + uuid(),
            });
        });
        afterEach(async () => {
            await db.drop();
            if (client1) await client1.stop();
        });

        it("loads database from an empty snapshot", async () => {
            const options = { topic: uuid() };
            db = await client1.open(new EventStore<string>({}), options);
            address = db.address!.toString();
            await db.saveSnapshot();
            await db.close();

            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
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
            client1: Peerbit,
            session: LSession;

        beforeAll(async () => {
            session = await LSession.connected(1);
        });

        afterAll(async () => {
            if (session) await session.stop();
        });

        beforeEach(async () => {
            client1 = await Peerbit.create(session.peers[0], {
                directory: dbPath + "/" + uuid(),
                storage: {
                    createStore: (string?: string) => createStore(string),
                },
            });

            const entryArr: number[] = [];

            for (let i = 0; i < entryCount; i++) entryArr.push(i);

            db = await client1.open(new EventStore<string>({}), {
                topic: uuid(),
            });
            address = db.address!.toString();
            await mapSeries(entryArr, (i) => db.add("hello" + i));
            await db.saveSnapshot();
            await db.close();
        });

        afterEach(async () => {
            await db?.drop();

            if (client1) await client1.stop();
        });

        it("loads database from snapshot", async () => {
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                { topic: uuid() }
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
                db = await client1.open(
                    await EventStore.load<EventStore<string>>(
                        client1._store,
                        Address.parse(address)
                    ),
                    { topic: uuid() }
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
            const options = { topic: uuid() };
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                options
            );
            await db.drop();
            db = null as any;
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                options
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
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                {
                    topic: uuid(),
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
            db = await client1.open(
                await EventStore.load<EventStore<string>>(
                    client1._store,
                    Address.parse(address)
                ),
                {
                    topic: uuid(),
                    onLoadProgress: (store, entry) => {
                        count++;
                        expect(address).toEqual(db.address!.toString());

                        assert.notEqual(entry.hash, null);
                        assert.notEqual(entry, null);
                        if (count === entryCount) {
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
