import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Documents } from "../document-store";
import {
    FieldBigIntCompareQuery,
    FieldStringMatchQuery,
    MemoryCompareQuery,
    MemoryCompare,
    DocumentQueryRequest,
    Results,
    CreatedAtQuery,
    U64Compare,
    Compare,
    ModifiedAtQuery,
} from "../query.js";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions } from "@dao-xyz/peerbit-store";
import { Identity } from "@dao-xyz/peerbit-log";
import {
    Ed25519Keypair,
    EncryptedThing,
    X25519Keypair,
    X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import Cache from "@dao-xyz/peerbit-cache";
import { AbstractLevel } from "abstract-level";
import { v4 as uuid } from "uuid";
import { Address, Program } from "@dao-xyz/peerbit-program";
import { waitFor } from "@dao-xyz/peerbit-time";
import { DocumentIndex } from "../document-index.js";
import {
    HeadsMessage,
    LogEntryEncryptionQuery,
    LogQueryRequest,
} from "@dao-xyz/peerbit-logindex";
import { waitForPeers as waitForPeersStreams } from "@dao-xyz/libp2p-direct-stream";

const bigIntSort = <T extends number | bigint>(a: T, b: T): number =>
    a > b ? 1 : 0 || -(a < b);

describe("index", () => {
    let session: LSession;

    const createIdentity = () => {
        const ed = Ed25519Keypair.create();
        return {
            publicKey: ed.publicKey,
            sign: (data) => ed.sign(data),
        } as Identity;
    };

    describe("operations", () => {
        let cacheStores: AbstractLevel<any, string, Uint8Array>[] = [];

        @variant("document")
        class Document {
            @field({ type: "string" })
            id: string;

            @field({ type: option("string") })
            name?: string;

            @field({ type: option("u64") })
            number?: bigint;

            constructor(opts?: Document) {
                if (opts) {
                    Object.assign(this, opts);
                }
            }
        }

        @variant("test_documents")
        class TestStore extends Program {
            @field({ type: Documents })
            docs: Documents<Document>;

            constructor(properties?: { docs: Documents<Document> }) {
                super();
                if (properties) {
                    this.docs = properties.docs;
                }
            }
            async setup(): Promise<void> {
                await this.docs.setup({ type: Document });
            }
        }

        let peersCount = 3,
            stores: TestStore[] = [],
            writeStore: TestStore;

        beforeAll(async () => {
            session = await LSession.connected(peersCount);
            for (let i = 0; i < peersCount; i++) {
                cacheStores.push(await createStore());
            }
            // Create store
            for (let i = 0; i < peersCount; i++) {
                if (i > 0) {
                    await waitForPeersStreams(
                        session.peers[i].directblock,
                        session.peers[0].directblock
                    );
                }
                const store =
                    i > 0
                        ? await TestStore.load<TestStore>(
                              session.peers[i].directblock,
                              stores[0].address!
                          )
                        : new TestStore({
                              docs: new Documents<Document>({
                                  index: new DocumentIndex({
                                      indexBy: "id",
                                  }),
                                  canEdit: true,
                              }),
                          });
                const keypair = await X25519Keypair.create();
                await store.init(session.peers[i], await createIdentity(), {
                    replicate: i === 0,
                    store: {
                        ...DefaultOptions,
                        encryption: {
                            getEncryptionKeypair: () => keypair,
                            getAnyKeypair: async (
                                publicKeys: X25519PublicKey[]
                            ) => {
                                for (let i = 0; i < publicKeys.length; i++) {
                                    if (
                                        publicKeys[i].equals(
                                            (keypair as X25519Keypair).publicKey
                                        )
                                    ) {
                                        return {
                                            index: i,
                                            keypair: keypair as
                                                | Ed25519Keypair
                                                | X25519Keypair,
                                        };
                                    }
                                }
                            },
                        },
                        resolveCache: () => new Cache(cacheStores[i]),
                    },
                });
                stores.push(store);
            }

            writeStore = stores[0];

            let doc = new Document({
                id: "1",
                name: "hello",
                number: 1n,
            });
            let docEdit = new Document({
                id: "1",
                name: "hello world",
                number: 1n,
            });

            let doc2 = new Document({
                id: "2",
                name: "hello world",
                number: 4n,
            });

            let doc2Edit = new Document({
                id: "2",
                name: "hello world",
                number: 2n,
            });

            let doc3 = new Document({
                id: "3",
                name: "foo",
                number: 3n,
            });

            await writeStore.docs.put(doc);
            await waitFor(() => writeStore.docs.index.size === 1);
            await writeStore.docs.put(docEdit);
            await writeStore.docs.put(doc2);
            await waitFor(() => writeStore.docs.index.size === 2);
            await writeStore.docs.put(doc2Edit);
            await writeStore.docs.put(doc3);
            await waitFor(() => writeStore.docs.index.size === 3);
        });

        afterEach(async () => {});

        afterAll(async () => {
            await Promise.all(stores.map((x) => x.drop()));
            await Promise.all(cacheStores.map((x) => x.close()));
            await session.stop();
        });

        describe("crud", () => {
            it("can add and delete", async () => {
                const store = new TestStore({
                    docs: new Documents<Document>({
                        index: new DocumentIndex({
                            indexBy: "id",
                        }),
                    }),
                });
                await store.init(session.peers[0], await createIdentity(), {
                    replicate: true,
                    store: {
                        ...DefaultOptions,
                        resolveCache: () => new Cache(cacheStores[0]),
                    },
                });

                let doc = new Document({
                    id: uuid(),
                    name: "Hello world",
                });
                let doc2 = new Document({
                    id: uuid(),
                    name: "Hello world",
                });

                const putOperation = (await store.docs.put(doc)).entry;
                expect(store.docs._index.size).toEqual(1);
                const putOperation2 = (await store.docs.put(doc2)).entry;
                expect(store.docs._index.size).toEqual(2);
                expect(putOperation2.next).toContainAllValues([]); // because doc 2 is independent of doc 1

                // delete 1
                const deleteOperation = (await store.docs.del(doc.id)).entry;
                expect(deleteOperation.next).toContainAllValues([
                    putOperation.hash,
                ]); // because delete is dependent on put
                expect(store.docs._index.size).toEqual(1);
            });

            it("permanently delete", async () => {
                const store = new TestStore({
                    docs: new Documents<Document>({
                        index: new DocumentIndex({
                            indexBy: "id",
                        }),
                        canEdit: true,
                    }),
                });
                await store.init(session.peers[0], await createIdentity(), {
                    replicate: true,
                    store: {
                        ...DefaultOptions,

                        resolveCache: () => new Cache(cacheStores[0]),
                    },
                });

                let doc = new Document({
                    id: uuid(),
                    name: "Hello world",
                });
                let editDoc = new Document({
                    id: doc.id,
                    name: "Hello world 2",
                });

                const _putOperation = await store.docs.put(doc);
                expect(store.docs._index.size).toEqual(1);
                const putOperation2 = (await store.docs.put(editDoc)).entry;
                expect(store.docs._index.size).toEqual(1);
                expect(putOperation2.next).toHaveLength(1);

                // delete 1
                const deleteOperation = (
                    await store.docs.del(doc.id, {
                        permanent: true,
                    })
                ).entry;
                expect(store.docs._index.size).toEqual(0);
                expect(
                    store.docs.store.oplog.values.map((x) => x.hash)
                ).toEqual([deleteOperation.hash]); // the delete operation
            });
            it("trim and update index", async () => {
                const store = new TestStore({
                    docs: new Documents<Document>({
                        index: new DocumentIndex({
                            indexBy: "id",
                        }),
                        canEdit: true,
                    }),
                });

                await store.init(session.peers[0], await createIdentity(), {
                    replicate: true,
                    store: {
                        ...DefaultOptions,

                        resolveCache: () => new Cache(cacheStores[0]),
                    },
                });

                for (let i = 0; i < 100; i++) {
                    await store.docs.put(
                        new Document({
                            id: String(i),
                            name: "Hello world " + String(i),
                        }),
                        { trim: { to: 10 }, nexts: [] }
                    );
                }

                expect(store.docs.index.size).toEqual(10);
                expect(store.docs.store.oplog.values.length).toEqual(10);
                expect(store.docs.store.oplog._headsIndex._index.size).toEqual(
                    10
                );
            });
        });

        describe("query", () => {
            it("match locally", async () => {
                let response: Results<Document> = undefined as any;

                await stores[0].docs.index.query(
                    new DocumentQueryRequest({
                        queries: [],
                    }),
                    (r: Results<Document>) => {
                        response = r;
                    },
                    { remote: false }
                );
                expect(response.results).toHaveLength(3);
            });

            it("match all", async () => {
                let response: Results<Document> = undefined as any;

                await stores[1].docs.index.query(
                    new DocumentQueryRequest({
                        queries: [],
                    }),
                    (r: Results<Document>) => {
                        response = r;
                    },
                    { remote: { amount: 1 } }
                );
                expect(response.results).toHaveLength(3);
            });

            it("can match sync", async () => {
                expect(stores[1].docs.index.size).toEqual(0);
                await stores[1].docs.index.query(
                    new DocumentQueryRequest({
                        queries: [],
                    }),
                    (r: Results<Document>) => {
                        // dont do anything
                        const x = 123;
                    },
                    { remote: { amount: 1, sync: true } }
                );
                await waitFor(() => stores[1].docs.index.size === 3);
            });

            it("string", async () => {
                let response: Results<Document> = undefined as any;

                await stores[1].docs.index.query(
                    new DocumentQueryRequest({
                        queries: [
                            new FieldStringMatchQuery({
                                key: "name",
                                value: "ello",
                            }),
                        ],
                    }),
                    (r: Results<Document>) => {
                        response = r;
                    },
                    { remote: { amount: 1 } }
                );
                expect(response.results).toHaveLength(2);
                expect(response.results.map((x) => x.value.id)).toEqual([
                    "1",
                    "2",
                ]);
            });

            describe("time", () => {
                it("created before", async () => {
                    let response: Results<Document> = undefined as any;

                    const allDocs = [
                        ...writeStore.docs.index._index.values(),
                    ].sort((a, b) =>
                        Number(
                            a.entry.metadata.clock.timestamp.wallTime -
                                b.entry.metadata.clock.timestamp.wallTime
                        )
                    );
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new CreatedAtQuery({
                                    created: [
                                        new U64Compare({
                                            compare: Compare.Less,
                                            value: allDocs[1].entry.metadata
                                                .clock.timestamp.wallTime,
                                        }),
                                    ],
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(
                        response.results.map((x) => x.context.head)
                    ).toContainAllValues([
                        allDocs[0].entry.hash,
                        allDocs[1].entry.hash,
                    ]); // allDocs[1] is also included because it was edited before allDocs[1].entry.metadata.clock.timestamp.wallTime
                });
                it("created between", async () => {
                    let response: Results<Document> = undefined as any;

                    const allDocs = [
                        ...writeStore.docs.index._index.values(),
                    ].sort((a, b) =>
                        Number(
                            a.entry.metadata.clock.timestamp.wallTime -
                                b.entry.metadata.clock.timestamp.wallTime
                        )
                    );
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new CreatedAtQuery({
                                    created: [
                                        new U64Compare({
                                            compare: Compare.Greater,
                                            value: allDocs[1].entry.metadata
                                                .clock.timestamp.wallTime,
                                        }),
                                        new U64Compare({
                                            compare: Compare.LessOrEqual,
                                            value: allDocs[2].entry.metadata
                                                .clock.timestamp.wallTime,
                                        }),
                                    ],
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(
                        response.results.map((x) => x.context.head)
                    ).toContainAllValues([allDocs[2].entry.hash]);
                });

                it("modified between", async () => {
                    let response: Results<Document> = undefined as any;

                    const allDocs = [
                        ...writeStore.docs.index._index.values(),
                    ].sort((a, b) =>
                        Number(
                            a.entry.metadata.clock.timestamp.wallTime -
                                b.entry.metadata.clock.timestamp.wallTime
                        )
                    );
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new ModifiedAtQuery({
                                    modified: [
                                        new U64Compare({
                                            compare: Compare.GreaterOrEqual,
                                            value: allDocs[1].entry.metadata
                                                .clock.timestamp.wallTime,
                                        }),
                                        new U64Compare({
                                            compare: Compare.Less,
                                            value: allDocs[2].entry.metadata
                                                .clock.timestamp.wallTime,
                                        }),
                                    ],
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(
                        response.results.map((x) => x.context.head)
                    ).toContainAllValues([allDocs[1].entry.hash]);
                });
            });

            describe("number", () => {
                it("equal", async () => {
                    let response: Results<Document> = undefined as any;
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new FieldBigIntCompareQuery({
                                    key: "number",
                                    compare: Compare.Equal,
                                    value: 2n,
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(response.results).toHaveLength(1);
                    expect(response.results[0].value.number).toEqual(2n);
                });

                it("gt", async () => {
                    let response: Results<Document> = undefined as any;
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new FieldBigIntCompareQuery({
                                    key: "number",
                                    compare: Compare.Greater,
                                    value: 2n,
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(response.results).toHaveLength(1);
                    expect(response.results[0].value.number).toEqual(3n);
                });

                it("gte", async () => {
                    let response: Results<Document> = undefined as any;
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new FieldBigIntCompareQuery({
                                    key: "number",
                                    compare: Compare.GreaterOrEqual,
                                    value: 2n,
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    response.results.sort((a, b) =>
                        bigIntSort(
                            a.value.number as bigint,
                            b.value.number as bigint
                        )
                    );
                    expect(response.results).toHaveLength(2);
                    expect(response.results[0].value.number).toEqual(2n);
                    expect(response.results[1].value.number).toEqual(3n);
                });

                it("lt", async () => {
                    let response: Results<Document> = undefined as any;
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new FieldBigIntCompareQuery({
                                    key: "number",
                                    compare: Compare.Less,
                                    value: 2n,
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(response.results).toHaveLength(1);
                    expect(response.results[0].value.number).toEqual(1n);
                });

                it("lte", async () => {
                    let response: Results<Document> = undefined as any;
                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new FieldBigIntCompareQuery({
                                    key: "number",
                                    compare: Compare.LessOrEqual,
                                    value: 2n,
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    response.results.sort((a, b) =>
                        bigIntSort(
                            a.value.number as bigint,
                            b.value.number as bigint
                        )
                    );
                    expect(response.results).toHaveLength(2);
                    expect(response.results[0].value.number).toEqual(1n);
                    expect(response.results[1].value.number).toEqual(2n);
                });
            });

            describe("Memory compare query", () => {
                it("Can query by memory", async () => {
                    const numberToMatch = 123;

                    let doc2 = new Document({
                        id: "8",
                        name: "x",
                        number: BigInt(numberToMatch),
                    });

                    let doc3 = new Document({
                        id: "9",
                        name: "y",
                        number: BigInt(numberToMatch),
                    });

                    const bytes = serialize(doc3);
                    const numberOffset = 24;
                    expect(bytes[numberOffset]).toEqual(numberToMatch);
                    await writeStore.docs.put(doc2);
                    await writeStore.docs.put(doc3);

                    let response: Results<Document> = undefined as any;

                    await stores[1].docs.index.query(
                        new DocumentQueryRequest({
                            queries: [
                                new MemoryCompareQuery({
                                    compares: [
                                        new MemoryCompare({
                                            bytes: new Uint8Array([123, 0, 0]), // add some 0  trailing so we now we can match more than the exact value
                                            offset: BigInt(numberOffset),
                                        }),
                                    ],
                                }),
                            ],
                        }),
                        (r: Results<Document>) => {
                            response = r;
                        },
                        { remote: { amount: 1 } }
                    );
                    expect(response.results).toHaveLength(2);
                    expect(response.results[0].value.id).toEqual(doc2.id);
                    expect(response.results[1].value.id).toEqual(doc3.id);
                });
            });
            describe("Encryption query", () => {
                it("can query by payload key", async () => {
                    const someKey = await X25519PublicKey.create();

                    let doc = new Document({
                        id: "encrypted",
                        name: "encrypted",
                    });

                    // write from 1
                    const entry = (
                        await stores[1].docs.put(doc, {
                            reciever: {
                                payload: [someKey],
                                metadata: undefined,
                                next: undefined,
                                signatures: undefined,
                            },
                        })
                    ).entry;
                    expect(
                        (
                            stores[1].docs.store.oplog.heads[0]
                                ._payload as EncryptedThing<any>
                        )._decrypted
                    ).toBeDefined();
                    delete (
                        stores[1].docs.store.oplog.heads[0]
                            ._payload as EncryptedThing<any>
                    )._decrypted;
                    expect(
                        (
                            stores[1].docs.store.oplog.heads[0]
                                ._payload as EncryptedThing<any>
                        )._decrypted
                    ).toBeUndefined();
                    const preLength = writeStore.docs.store.oplog.values.length;
                    await writeStore.docs.store.sync(
                        stores[1].docs.store.oplog.heads
                    );
                    await waitFor(
                        () =>
                            writeStore.docs.store.oplog.values.length ===
                            preLength + 1
                    );
                    await waitFor(
                        () =>
                            writeStore.docs.logIndex.store.oplog.values
                                .length ===
                            preLength + 1
                    );
                    expect(
                        (
                            writeStore.docs.store.oplog.heads[0]
                                ._payload as EncryptedThing<any>
                        )._decrypted
                    ).toBeUndefined();

                    let response: HeadsMessage = undefined as any;

                    // read from observer 2
                    await stores[2].docs.logIndex.query.send(
                        new LogQueryRequest({
                            queries: [
                                new LogEntryEncryptionQuery({
                                    payload: [someKey],
                                    metadata: [],
                                    next: [],
                                    signatures: [],
                                }),
                            ],
                        }),
                        (r: HeadsMessage) => {
                            response = r;
                        },
                        { amount: 1 }
                    );
                    expect(response.heads).toHaveLength(1);
                    expect(response.heads[0].hash).toEqual(entry.hash);
                });
            });
        });
    });

    describe("program as value", () => {
        let cacheStores: AbstractLevel<any, string, Uint8Array>[] = [];

        @variant("subprogram")
        class SubProgram extends Program {
            constructor(
                properties?:
                    | {
                          id?: string | undefined;
                      }
                    | undefined
            ) {
                super(properties);
            }
            async setup() {}
        }

        @variant("test_program_documents")
        class TestStore extends Program {
            @field({ type: Documents })
            docs: Documents<SubProgram>;

            constructor(properties: { docs: Documents<SubProgram> }) {
                super();
                if (properties) {
                    this.docs = properties.docs;
                }
            }
            async setup(): Promise<void> {
                await this.docs.setup({
                    type: SubProgram,
                    canOpen: () => Promise.resolve(true),
                });
            }
        }

        let stores: { store: TestStore; openEvents: Program[] }[];
        let peersCount = 2;

        beforeAll(async () => {
            for (let i = 0; i < peersCount; i++) {
                cacheStores.push(await createStore());
            }
        });

        beforeEach(async () => {
            stores = [];
            session = await LSession.connected(peersCount);

            // Create store
            for (let i = 0; i < peersCount; i++) {
                if (i > 0) {
                    await waitForPeersStreams(
                        session.peers[i].directblock,
                        session.peers[0].directblock
                    );
                }
                const openEvents: Program[] = [];
                const store =
                    i > 0
                        ? await TestStore.load<TestStore>(
                              session.peers[i].directblock,
                              stores[0].store.address!
                          )
                        : new TestStore({
                              docs: new Documents<SubProgram>({
                                  index: new DocumentIndex({
                                      indexBy: "id",
                                  }),
                                  canEdit: true,
                              }),
                          });

                const keypair = await X25519Keypair.create();
                await store.init(session.peers[i], await createIdentity(), {
                    replicate: i === 0,
                    replicator: () => Promise.resolve(true),
                    open: async (program) => {
                        openEvents.push(program);
                        return program;
                    },
                    store: {
                        ...DefaultOptions,
                        encryption: {
                            getEncryptionKeypair: () => keypair,
                            getAnyKeypair: async (
                                publicKeys: X25519PublicKey[]
                            ) => {
                                for (let i = 0; i < publicKeys.length; i++) {
                                    if (
                                        publicKeys[i].equals(
                                            (keypair as X25519Keypair).publicKey
                                        )
                                    ) {
                                        return {
                                            index: i,
                                            keypair: keypair as
                                                | Ed25519Keypair
                                                | X25519Keypair,
                                        };
                                    }
                                }
                            },
                        },
                        resolveCache: () => new Cache(cacheStores[i]),
                    },
                });
                stores.push({ store, openEvents });
            }
        });
        afterEach(async () => {
            await Promise.all(stores.map((x) => x.store.drop()));
            await session.stop();
        });

        afterAll(async () => {
            await Promise.all(cacheStores.map((x) => x.close()));
        });

        it("can open a subprogram when put", async () => {
            const subProgram = new SubProgram();
            const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
            expect(stores[0].openEvents).toHaveLength(1);
            expect(stores[0].openEvents[0]).toEqual(subProgram);
        });

        it("non-replicator will not open by default", async () => {
            const subProgram = new SubProgram();
            const _result = await stores[1].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
            expect(stores[1].openEvents).toHaveLength(0);
        });

        it("can open program when sync ", async () => {
            const subProgram = new SubProgram();
            const _result = await stores[1].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
            await stores[0].store.docs.store.sync(
                stores[1].store.docs.store.oplog.values
            );
            expect(stores[0].openEvents).toHaveLength(1);
            expect(stores[1].openEvents).toHaveLength(0);
        });

        it("will close on delete", async () => {
            const subProgram = new SubProgram();
            const close = subProgram.close.bind(subProgram);
            let closed = false;

            subProgram.close = () => {
                closed = true;
                return close();
            };

            const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
            expect(stores[0].openEvents).toHaveLength(1);
            expect(stores[0].openEvents[0]).toEqual(subProgram);
            await stores[0].store.docs.del(subProgram.id);
            await waitFor(() => closed);
        });
    });
});
