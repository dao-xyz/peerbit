import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Documents, DocumentsChange } from "../document-store";
import {
	IntegerCompareQuery,
	StringMatchQuery,
	MemoryCompareQuery,
	MemoryCompare,
	DocumentQueryRequest,
	Results,
	CreatedAtQuery,
	U64Compare,
	Compare,
	MissingQuery,
	StringMatchMethod,
	SignedByQuery,
	EntryEncryptedByQuery,
} from "../query.js";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions } from "@dao-xyz/peerbit-store";
import { Identity } from "@dao-xyz/peerbit-log";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	EncryptedThing,
	X25519Keypair,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import Cache from "@dao-xyz/lazy-level";
import { v4 as uuid } from "uuid";
import {
	AbstractProgram,
	ObserverType,
	Program,
	ReplicatorType,
} from "@dao-xyz/peerbit-program";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { DocumentIndex } from "../document-index.js";

import { waitForPeers as waitForPeersStreams } from "@dao-xyz/libp2p-direct-stream";
import crypto from "crypto";

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
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

const bigIntSort = <T extends number | bigint>(a: T, b: T): number =>
	a > b ? 1 : 0 || -(a < b);

describe("index", () => {
	let session: LSession;

	const createIdentity = async () => {
		const ed = await Ed25519Keypair.create();
		return {
			publicKey: ed.publicKey,
			sign: (data) => ed.sign(data),
		} as Identity;
	};

	describe("operations", () => {
		describe("crud", () => {
			let store: TestStore;

			beforeAll(async () => {
				session = await LSession.connected(1);
			});
			afterEach(async () => {
				await store?.close();
			});

			afterAll(async () => {
				await session.stop();
			});

			it("can add and delete", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						index: new DocumentIndex({
							indexBy: "id",
						}),
					}),
				});
				await store.init(session.peers[0], await createIdentity(), {
					role: new ReplicatorType(),
					replicators: () => [
						[session.peers[0].directsub.publicKey.hashcode()],
					],
					store: {
						...DefaultOptions,
						resolveCache: () => new Cache(createStore()),
					},
				});

				const changes: DocumentsChange<Document>[] = [];
				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
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
				expect(store.docs.index.size).toEqual(1);

				expect(changes.length).toEqual(1);
				expect(changes[0].added).toHaveLength(1);
				expect(changes[0].added[0].id).toEqual(doc.id);
				expect(changes[0].removed).toHaveLength(0);

				const putOperation2 = (await store.docs.put(doc2)).entry;
				expect(store.docs.index.size).toEqual(2);
				expect(putOperation2.next).toContainAllValues([]); // because doc 2 is independent of doc 1

				expect(changes.length).toEqual(2);
				expect(changes[1].added).toHaveLength(1);
				expect(changes[1].added[0].id).toEqual(doc2.id);
				expect(changes[1].removed).toHaveLength(0);

				// delete 1
				const deleteOperation = (await store.docs.del(doc.id)).entry;
				expect(deleteOperation.next).toContainAllValues([putOperation.hash]); // because delete is dependent on put
				expect(store.docs.index.size).toEqual(1);

				expect(changes.length).toEqual(3);
				expect(changes[2].added).toHaveLength(0);
				expect(changes[2].removed).toHaveLength(1);
				expect(changes[2].removed[0].id).toEqual(doc.id);
			});

			it("many chunks", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						index: new DocumentIndex({
							indexBy: "id",
						}),
					}),
				});
				await store.init(session.peers[0], await createIdentity(), {
					role: new ReplicatorType(),
					replicators: () => [],
					store: {
						...DefaultOptions,
						resolveCache: () => new Cache(createStore()),
					},
				});
				const insertions = 100;
				const rngs: string[] = [];
				for (let i = 0; i < insertions; i++) {
					rngs.push(Buffer.from(crypto.randomBytes(1e5)).toString("base64"));
				}
				for (let i = 0; i < 20000; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: rngs[i],
						}),
						{ unique: true }
					);
				}
			});

			it("delete permanently", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						index: new DocumentIndex({
							indexBy: "id",
						}),
						immutable: false,
					}),
				});
				await store.init(session.peers[0], await createIdentity(), {
					replicators: () => [],
					role: new ReplicatorType(),
					store: {
						...DefaultOptions,
						resolveCache: () => new Cache(createStore()),
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
				expect(store.docs.index.size).toEqual(1);
				const putOperation2 = (await store.docs.put(editDoc)).entry;
				expect(store.docs.index.size).toEqual(1);
				expect(putOperation2.next).toHaveLength(1);

				// delete 1
				const deleteOperation = (await store.docs.del(doc.id)).entry;
				expect(store.docs.index.size).toEqual(0);
				expect(
					(await store.docs.store.oplog.values.toArray()).map((x) => x.hash)
				).toEqual([deleteOperation.hash]); // the delete operation
			});

			it("trim deduplicate changes", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						index: new DocumentIndex({
							indexBy: "id",
						}),
					}),
				});

				await store.init(session.peers[0], await createIdentity(), {
					role: new ReplicatorType(),
					replicators: () => [],
					store: {
						...DefaultOptions,
						resolveCache: () => new Cache(createStore()),
						trim: { type: "length", to: 1 },
					},
				});

				const changes: DocumentsChange<Document>[] = [];
				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				// put doc
				await store.docs.put(doc);
				expect(store.docs.index.size).toEqual(1);
				expect(changes.length).toEqual(1);
				expect(changes[0].added).toHaveLength(1);
				expect(changes[0].added[0].id).toEqual(doc.id);
				expect(changes[0].removed).toHaveLength(0);

				// put doc again and make sure it still exist in index with trim to 1 option
				await store.docs.put(doc);
				expect(store.docs.index.size).toEqual(1);
				expect(store.docs.store.oplog.values.length).toEqual(1);
				expect(changes.length).toEqual(2);
				expect(changes[1].added).toHaveLength(1);
				expect(changes[1].added[0].id).toEqual(doc.id);
				expect(changes[1].removed).toHaveLength(0);
			});

			it("trim and update index", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						index: new DocumentIndex({
							indexBy: "id",
						}),
						immutable: false,
					}),
				});

				await store.init(session.peers[0], await createIdentity(), {
					role: new ReplicatorType(),
					replicators: () => [],
					store: {
						...DefaultOptions,
						resolveCache: () => new Cache(createStore()),
						trim: { type: "length", to: 10 },
					},
				});

				for (let i = 0; i < 100; i++) {
					await store.docs.put(
						new Document({
							id: String(i),
							name: "Hello world " + String(i),
						}),
						{ nexts: [] }
					);
				}

				expect(store.docs.index.size).toEqual(10);
				expect(store.docs.store.oplog.values.length).toEqual(10);
				expect(store.docs.store.oplog.headsIndex.index.size).toEqual(10);
			});

			describe("field extractor", () => {
				it("filters field", async () => {
					let indexedNameField = "xyz";
					class FilteredStore extends TestStore {
						constructor(properties: { docs: Documents<Document> }) {
							super(properties);
						}
						async setup(): Promise<void> {
							await this.docs.setup({
								type: Document,
								indexFields: (obj) => {
									return { [indexedNameField]: obj.name };
								},
							});
						}
					}

					store = new FilteredStore({
						docs: new Documents<Document>({
							index: new DocumentIndex({
								indexBy: "id",
							}),
						}),
					});

					await store.init(session.peers[0], await createIdentity(), {
						role: new ReplicatorType(),
						replicators: () => [
							[session.peers[0].directsub.publicKey.hashcode()],
						],
						store: {
							...DefaultOptions,
							resolveCache: () => new Cache(createStore()),
						},
					});

					const changes: DocumentsChange<Document>[] = [];
					store.docs.events.addEventListener("change", (evt) => {
						changes.push(evt.detail);
					});

					let doc = new Document({
						id: uuid(),
						name: "Hello world",
					});

					await store.docs.put(doc);

					let indexedValues = [...store.docs.index.index.values()];
					expect(indexedValues).toHaveLength(1);
					expect(indexedValues[0].value).toEqual({
						[indexedNameField]: doc.name,
					});
				});
			});
		});

		describe("query", () => {
			let peersCount = 3,
				stores: TestStore[] = [],
				writeStore: TestStore;

			beforeAll(async () => {
				session = await LSession.connected(peersCount);
				// Create store
				for (let i = 0; i < peersCount; i++) {
					const store =
						i > 0
							? (await TestStore.load<TestStore>(
									session.peers[i].directblock,
									stores[0].address!
							  ))!
							: new TestStore({
									docs: new Documents<Document>({
										index: new DocumentIndex({
											indexBy: "id",
										}),
										immutable: false,
									}),
							  });
					const keypair = await X25519Keypair.create();
					await store.init(session.peers[i], await createIdentity(), {
						role: i === 0 ? new ReplicatorType() : new ObserverType(),
						replicators: () => [
							[session.peers[0].directsub.publicKey.hashcode()],
						],
						store: {
							...DefaultOptions,
							encryption: {
								getEncryptionKeypair: () => keypair,
								getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
									for (let i = 0; i < publicKeys.length; i++) {
										if (
											publicKeys[i].equals((keypair as X25519Keypair).publicKey)
										) {
											return {
												index: i,
												keypair: keypair as Ed25519Keypair | X25519Keypair,
											};
										}
									}
								},
							},

							resolveCache: () => new Cache(createStore()),
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
					name: "Hello World",
					number: 2n,
				});

				let doc3 = new Document({
					id: "3",
					name: "foo",
					number: 3n,
				});

				let doc4 = new Document({
					id: "4",
					name: undefined,
					number: undefined,
				});

				await writeStore.docs.put(doc);
				await waitFor(() => writeStore.docs.index.size === 1);
				await writeStore.docs.put(docEdit);
				await writeStore.docs.put(doc2);
				await waitFor(() => writeStore.docs.index.size === 2);
				await writeStore.docs.put(doc2Edit);
				await writeStore.docs.put(doc3);
				await writeStore.docs.put(doc4);
				await waitFor(() => writeStore.docs.index.size === 4);
			});

			afterAll(async () => {
				await Promise.all(stores.map((x) => x.drop()));

				await session.stop();
			});

			it("no-args", async () => {
				let response: Results<Document>[] = await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] })
				);
				expect(response[0].results).toHaveLength(4);
			});

			it("match locally", async () => {
				let response: Results<Document>[] = await stores[0].docs.index.query(
					new DocumentQueryRequest({
						queries: [],
					}),
					{ remote: false }
				);
				expect(response[0].results).toHaveLength(4);
			});

			it("match all", async () => {
				let responses: Results<Document>[] = await stores[1].docs.index.query(
					new DocumentQueryRequest({
						queries: [],
					}),
					{ remote: { amount: 1 } }
				);
				expect(responses).toHaveLength(1);
				expect(responses[0].results).toHaveLength(4);
			});

			it("can match sync", async () => {
				expect(stores[1].docs.index.size).toEqual(0);
				let results = await stores[1].docs.index.query(
					new DocumentQueryRequest({
						queries: [],
					}),
					{ remote: { amount: 1, sync: true } }
				);
				await waitFor(() => stores[1].docs.index.size === 4);
			});

			describe("string", () => {
				it("exact", async () => {
					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new StringMatchQuery({
									key: "name",
									value: "hello world",
									caseInsensitive: true,
								}),
							],
						})
					);
					expect(responses[0].results).toHaveLength(2);
					expect(
						responses[0].results.map((x) => x.value.id)
					).toContainAllValues(["1", "2"]);
				});

				it("exact-case-insensitive", async () => {
					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new StringMatchQuery({
									key: "name",
									value: "Hello World",
									caseInsensitive: true,
								}),
							],
						})
					);
					expect(responses[0].results).toHaveLength(2);
					expect(
						responses[0].results.map((x) => x.value.id)
					).toContainAllValues(["1", "2"]);
				});

				it("exact case sensitive", async () => {
					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new StringMatchQuery({
									key: "name",
									value: "Hello World",
									caseInsensitive: false,
								}),
							],
						})
					);
					expect(responses[0].results).toHaveLength(1);
					expect(
						responses[0].results.map((x) => x.value.id)
					).toContainAllValues(["2"]);
					responses = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new StringMatchQuery({
									key: "name",
									value: "hello world",
									caseInsensitive: false,
								}),
							],
						})
					);
					expect(responses[0].results).toHaveLength(1);
					expect(
						responses[0].results.map((x) => x.value.id)
					).toContainAllValues(["1"]);
				});
				it("prefix", async () => {
					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new StringMatchQuery({
									key: "name",
									value: "hel",
									method: StringMatchMethod.prefix,
									caseInsensitive: true,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(responses[0].results).toHaveLength(2);
					expect(
						responses[0].results.map((x) => x.value.id)
					).toContainAllValues(["1", "2"]);
				});

				it("contains", async () => {
					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new StringMatchQuery({
									key: "name",
									value: "ello",
									method: StringMatchMethod.contains,
									caseInsensitive: true,
								}),
							],
						})
					);
					expect(responses[0].results).toHaveLength(2);
					expect(
						responses[0].results.map((x) => x.value.id)
					).toContainAllValues(["1", "2"]);
				});
			});

			it("missing", async () => {
				let responses: Results<Document>[] = await stores[1].docs.index.query(
					new DocumentQueryRequest({
						queries: [
							new MissingQuery({
								key: "name",
							}),
						],
					}),
					{ remote: { amount: 1 } }
				);
				expect(responses[0].results).toHaveLength(1);
				expect(responses[0].results.map((x) => x.value.id)).toEqual(["4"]);
			});

			describe("time", () => {
				it("created before", async () => {
					const allDocs = [...writeStore.docs.index.index.values()].sort(
						(a, b) =>
							Number(
								a.entry.metadata.clock.timestamp.wallTime -
									b.entry.metadata.clock.timestamp.wallTime
							)
					);

					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new CreatedAtQuery({
									created: [
										new U64Compare({
											compare: Compare.LessOrEqual,
											value: allDocs[1].entry.metadata.clock.timestamp.wallTime,
										}),
									],
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(
						responses[0].results.map((x) => x.context.head)
					).toContainAllValues([allDocs[0].entry.hash, allDocs[1].entry.hash]); // allDocs[1] is also included because it was edited before allDocs[1].entry.metadata.clock.timestamp.wallTime
				});

				it("created between", async () => {
					const allDocs = [...writeStore.docs.index.index.values()].sort(
						(a, b) =>
							Number(
								a.entry.metadata.clock.timestamp.wallTime -
									b.entry.metadata.clock.timestamp.wallTime
							)
					);
					let responses: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new CreatedAtQuery({
									created: [
										new U64Compare({
											compare: Compare.Greater,
											value: allDocs[1].entry.metadata.clock.timestamp.wallTime,
										}),
										new U64Compare({
											compare: Compare.LessOrEqual,
											value: allDocs[2].entry.metadata.clock.timestamp.wallTime,
										}),
									],
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(
						responses[0].results.map((x) => x.context.head)
					).toContainAllValues([allDocs[2].entry.hash]);
				});

				/*
								it("modified between", async () => {
									let response: Results<Document> = undefined as any;
				
									const allDocs = [...writeStore.docs.index.index.values()].sort(
										(a, b) =>
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
															value: allDocs[1].entry.metadata.clock.timestamp.wallTime,
														}),
														new U64Compare({
															compare: Compare.Less,
															value: allDocs[2].entry.metadata.clock.timestamp.wallTime,
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
								}); */
			});

			describe("number", () => {
				it("equal", async () => {
					let response: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new IntegerCompareQuery({
									key: "number",
									compare: Compare.Equal,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(response[0].results).toHaveLength(1);
					expect(response[0].results[0].value.number).toEqual(2n);
				});

				it("gt", async () => {
					let response: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new IntegerCompareQuery({
									key: "number",
									compare: Compare.Greater,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(response[0].results).toHaveLength(1);
					expect(response[0].results[0].value.number).toEqual(3n);
				});

				it("gte", async () => {
					let response: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new IntegerCompareQuery({
									key: "number",
									compare: Compare.GreaterOrEqual,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					response[0].results.sort((a, b) =>
						bigIntSort(a.value.number as bigint, b.value.number as bigint)
					);
					expect(response[0].results).toHaveLength(2);
					expect(response[0].results[0].value.number).toEqual(2n);
					expect(response[0].results[1].value.number).toEqual(3n);
				});

				it("lt", async () => {
					let response: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new IntegerCompareQuery({
									key: "number",
									compare: Compare.Less,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(response[0].results).toHaveLength(1);
					expect(response[0].results[0].value.number).toEqual(1n);
				});

				it("lte", async () => {
					let response: Results<Document>[] = await stores[1].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new IntegerCompareQuery({
									key: "number",
									compare: Compare.LessOrEqual,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					response[0].results.sort((a, b) =>
						bigIntSort(a.value.number as bigint, b.value.number as bigint)
					);
					expect(response[0].results).toHaveLength(2);
					expect(response[0].results[0].value.number).toEqual(1n);
					expect(response[0].results[1].value.number).toEqual(2n);
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

					let responses: Results<Document>[] = await stores[1].docs.index.query(
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
						{ remote: { amount: 1 } }
					);
					expect(responses[0].results).toHaveLength(2);
					expect(responses[0].results[0].value.id).toEqual(doc2.id);
					expect(responses[0].results[1].value.id).toEqual(doc3.id);
				});
			});

			describe("signed by", () => {
				it("multiple signatures", async () => {
					const responses = await stores[2].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new SignedByQuery({
									publicKeys: [writeStore.identity.publicKey],
								}),
							],
						})
					);

					expect(responses).toHaveLength(1);
				});

				/* it("handles missing", async () => {
					const responses: HeadsMessage[] = (
						await logIndices[2].query.send(
							new LogQueryRequest({
								queries: [
									new SignedByQuery({
										publicKeys: [(await Ed25519Keypair.create()).publicKey],
									}),
								],
							}),
							{ amount: 1 }
						)
					).map((x) => x.response);
					expect(responses).toHaveLength(1);
					expect(responses[0].heads).toHaveLength(0);
				}); */
			});
			describe("Encryption query", () => {
				it("can query by payload key", async () => {
					const efn =
						writeStore.docs.store.oplog.encryption!.getEncryptionKeypair;
					const someKey = (await (typeof efn === "function" ? efn() : efn))
						.publicKey as Ed25519PublicKey;

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

					delete (entry._payload as EncryptedThing<any>)._decrypted;

					expect(
						(entry._payload as EncryptedThing<any>)._decrypted
					).toBeUndefined();

					const preLength = writeStore.docs.store.oplog.values.length;
					await writeStore.docs.store.sync(
						await stores[1].docs.store.oplog.getHeads()
					);
					await waitFor(
						() => writeStore.docs.store.oplog.values.length === preLength + 1
					);

					// read from observer 2
					let responses = await stores[2].docs.index.query(
						new DocumentQueryRequest({
							queries: [
								new EntryEncryptedByQuery({
									payload: [someKey],
									metadata: [],
									next: [],
									signatures: [],
								}),
							],
						}),
						{ local: false, remote: true }
					);
					expect(responses[0].results).toHaveLength(1);
					expect(responses[0].results[0].value.id).toEqual("encrypted");
				});
			});
		});
	});

	describe("program as value", () => {
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
			session = await LSession.connected(peersCount);
		});
		beforeEach(async () => {
			stores = [];

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
						? (await TestStore.load<TestStore>(
								session.peers[i].directblock,
								stores[0].store.address!
						  ))!
						: new TestStore({
								docs: new Documents<SubProgram>({
									index: new DocumentIndex({
										indexBy: "id",
									}),
									immutable: false,
								}),
						  });

				const keypair = await X25519Keypair.create();
				await store.init(session.peers[i], await createIdentity(), {
					role: i === 0 ? new ReplicatorType() : new ObserverType(),
					open: async (program) => {
						openEvents.push(program);
						program["_initialized"] = true;

						// we don't init, but in real use case we would init here
						return program;
					},
					replicators: () => [
						[session.peers[0].directsub.publicKey.hashcode()],
					],
					store: {
						...DefaultOptions,
						encryption: {
							getEncryptionKeypair: () => keypair,
							getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
								for (let i = 0; i < publicKeys.length; i++) {
									if (
										publicKeys[i].equals((keypair as X25519Keypair).publicKey)
									) {
										return {
											index: i,
											keypair: keypair as Ed25519Keypair | X25519Keypair,
										};
									}
								}
							},
						},
						replicator: () => Promise.resolve(true),
						resolveCache: () => new Cache(createStore()),
					},
				});
				stores.push({ store, openEvents });
			}
		});
		afterEach(async () => {
			await Promise.all(stores.map((x) => x.store.close()));
		});

		afterAll(async () => {
			await session.stop();
		});

		it("can open a subprogram when put", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			expect(stores[0].openEvents).toHaveLength(1);
			expect(stores[0].openEvents[0]).toEqual(subProgram);
		});

		it("will close subprogram after put", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			expect(stores[0].openEvents).toHaveLength(1);
			expect(stores[0].openEvents[0]).toEqual(subProgram);
			await stores[0].store.close();
			expect(subProgram.closed).toBeTrue();
		});
		it("will not close subprogram that is opened before put", async () => {
			const subProgram = new SubProgram();
			subProgram.init(session.peers[0], await createIdentity(), {
				role: new ReplicatorType(),
				replicators: () => [],
				store: {
					...DefaultOptions,
					replicator: () => Promise.resolve(true),
					resolveCache: () => new Cache(createStore()),
				},
			});
			const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			expect(stores[0].openEvents).toHaveLength(1);
			await stores[0].store.close();
			expect(subProgram.closed).toBeFalse();
			await subProgram.close();
			expect(subProgram.closed).toBeTrue();
		});

		it("non-replicator will not open by default", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[1].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			expect(stores[1].openEvents).toHaveLength(0);
		});

		it("can open program when sync", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[1].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			await stores[0].store.docs.store.sync(
				await stores[1].store.docs.store.oplog.values.toArray()
			);
			expect(stores[0].openEvents).toHaveLength(1);
			expect(stores[1].openEvents).toHaveLength(0);
		});

		it("will close on delete", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			expect(stores[0].openEvents).toHaveLength(1);
			expect(stores[0].openEvents[0]).toEqual(subProgram);
			await stores[0].store.docs.del(subProgram.id);
			await waitFor(() => subProgram.closed);
		});

		it("can prevent subprograms to be opened", async () => {
			stores[0].store.docs.canOpen = (_) => Promise.resolve(false);
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			subProgram.openedByPrograms = [undefined];
			expect(subProgram.closed).toBeTrue();
			subProgram["_closed"] = false;
			subProgram["_initialized"] = true;
			expect(subProgram.closed).toBeFalse();
			expect(stores[0].openEvents).toHaveLength(0);
			await stores[0].store.docs.del(subProgram.id);
			expect(subProgram.closed).toBeFalse();
		});
	});

	describe("query distribution", () => {
		describe("distribution", () => {
			let peersCount = 3,
				stores: TestStore[] = [];
			let counters: Array<number> = [];

			beforeAll(async () => {
				session = await LSession.connected(peersCount);
				// Create store
				for (let i = 0; i < peersCount; i++) {
					const store =
						i > 0
							? (await TestStore.load<TestStore>(
									session.peers[i].directblock,
									stores[0].address!
							  ))!
							: new TestStore({
									docs: new Documents<Document>({
										index: new DocumentIndex({
											indexBy: "id",
										}),
										immutable: false,
									}),
							  });
					const keypair = await X25519Keypair.create();
					await store.init(session.peers[i], await createIdentity(), {
						role: new ReplicatorType(),
						replicators: () => [
							session.peers.map((x) => x.directsub.publicKey.hashcode()),
						],
						store: {
							...DefaultOptions,
							encryption: {
								getEncryptionKeypair: () => keypair,
								getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
									for (let i = 0; i < publicKeys.length; i++) {
										if (
											publicKeys[i].equals((keypair as X25519Keypair).publicKey)
										) {
											return {
												index: i,
												keypair: keypair as Ed25519Keypair | X25519Keypair,
											};
										}
									}
								},
							},

							resolveCache: () => new Cache(createStore()),
						},
					});
					stores.push(store);
				}

				for (let i = 0; i < stores.length; i++) {
					const fn = stores[i].docs.index.queryHandler.bind(
						stores[i].docs.index
					);
					stores[i].docs.index.queryHandler = (a, b) => {
						counters[i] += 1;
						return fn(a, b);
					};
				}
			});

			beforeEach(() => {
				counters = new Array(stores.length).fill(0);
			});

			afterAll(async () => {
				await Promise.all(stores.map((x) => x.drop()));
				await session.stop();
			});

			it("queries all if undefined", async () => {
				stores[0].docs.index.replicators = () => undefined;
				await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] }),
					{ remote: { amount: 2 } }
				);
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});

			it("all", async () => {
				stores[0].docs.index.replicators = () => [
					[stores[1].libp2p.directsub.publicKey.hashcode()],
					[stores[2].libp2p.directsub.publicKey.hashcode()],
				];
				await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] })
				);
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});

			it("will always query locally", async () => {
				stores[0].docs.index.replicators = () => [];
				await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] })
				);
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(0);
				expect(counters[2]).toEqual(0);
			});

			it("one", async () => {
				stores[0].docs.index.replicators = () => [
					[stores[1].libp2p.directsub.publicKey.hashcode()],
				];
				await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] })
				);
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(0);
			});

			it("non-local", async () => {
				stores[0].docs.index.replicators = () => [
					[stores[1].libp2p.directsub.publicKey.hashcode()],
					[stores[2].libp2p.directsub.publicKey.hashcode()],
				];
				await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] }),
					{ local: false }
				);
				expect(counters[0]).toEqual(0);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});
			it("ignore shard if I am replicator", async () => {
				stores[0].docs.index.replicators = () => [
					[
						stores[0].libp2p.directsub.publicKey.hashcode(),
						stores[1].libp2p.directsub.publicKey.hashcode(),
					],
				];
				await stores[0].docs.index.query(
					new DocumentQueryRequest({ queries: [] })
				);
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(0);
				expect(counters[2]).toEqual(0);
			});

			describe("errors", () => {
				let fns: any[];

				beforeEach(() => {
					fns = stores.map((x) => x.docs.index.queryHandler.bind(x.docs.index));
				});

				afterEach(() => {
					stores.forEach((x, ix) => {
						x.docs.index.queryHandler = fns[ix];
					});
				});

				it("will iterate on shard until response", async () => {
					stores[0].docs.index.replicators = () => [
						[
							stores[1].libp2p.directsub.publicKey.hashcode(),
							stores[2].libp2p.directsub.publicKey.hashcode(),
						],
					];

					let failedOnce = false;
					for (let i = 1; i < stores.length; i++) {
						const fn = stores[i].docs.index.queryHandler.bind(
							stores[1].docs.index
						);
						stores[i].docs.index.queryHandler = (a, b) => {
							if (!failedOnce) {
								failedOnce = true;
								throw new Error("Expected error");
							}
							return fn(a, b);
						};
					}
					let timeout = 1000;
					await stores[0].docs.index.query(
						new DocumentQueryRequest({ queries: [] }),
						{ remote: { timeout } }
					);
					expect(failedOnce).toBeTrue();
					expect(counters[0]).toEqual(1);
					expect(counters[1] + counters[2]).toEqual(1);
					expect(counters[1]).not.toEqual(counters[2]);
				});

				it("will fail silently if can not reach all shards", async () => {
					stores[0].docs.index.replicators = () => [
						[
							stores[1].libp2p.directsub.publicKey.hashcode(),
							stores[2].libp2p.directsub.publicKey.hashcode(),
						],
					];
					for (let i = 1; i < stores.length; i++) {
						stores[i].docs.index.queryHandler = (a, b) => {
							throw new Error("Expected error");
						};
					}

					let timeout = 1000;

					await stores[0].docs.index.query(
						new DocumentQueryRequest({ queries: [] }),
						{ remote: { timeout } }
					);
					expect(counters[0]).toEqual(1);
					expect(counters[1]).toEqual(0);
					expect(counters[2]).toEqual(0);
				});
			});
		});
	});
});
