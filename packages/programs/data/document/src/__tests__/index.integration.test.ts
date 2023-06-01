import { field, fixedArray, option, variant, vec } from "@dao-xyz/borsh";
import { Documents, DocumentsChange } from "../document-store";
import {
	IntegerCompare,
	StringMatch,
	Compare,
	MissingField,
	And,
	SearchRequest,
	StringMatchMethod,
	Or,
	ByteMatchQuery,
	BoolQuery,
	Sort,
	SortDirection,
	SearchSortedRequest,
} from "../query.js";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { Identity, Log } from "@dao-xyz/peerbit-log";
import {
	Ed25519Keypair,
	X25519Keypair,
	X25519PublicKey,
	randomBytes,
} from "@dao-xyz/peerbit-crypto";
import Cache from "@dao-xyz/lazy-level";
import { v4 as uuid } from "uuid";
import {
	ObserverType,
	Program,
	ReplicatorType,
} from "@dao-xyz/peerbit-program";
import { waitFor } from "@dao-xyz/peerbit-time";
import { DocumentIndex } from "../document-index.js";
import { waitForPeers as waitForPeersStreams } from "@dao-xyz/libp2p-direct-stream";
import { waitForSubscribers } from "@dao-xyz/libp2p-direct-sub";

BigInt.prototype["toJSON"] = function () {
	return this.toString();
};

@variant("document")
class Document {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: option(vec("string")) })
	tags?: string[];

	@field({ type: option("bool") })
	bool?: boolean;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;

	constructor(opts: Document) {
		this.id = opts.id;
		this.name = opts.name;
		this.number = opts.number;
		this.tags = opts.tags;
		this.bool = opts.bool;
		this.data = opts.data;
	}
}

@variant("test_documents")
class TestStore extends Program {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<Document>;

	constructor(properties: { docs: Documents<Document> }) {
		super();

		this.id = randomBytes(32);
		this.docs = properties.docs;
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
			let store2: TestStore;

			beforeAll(async () => {
				session = await LSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
				await store2?.close();
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

					log: {
						replication: {
							replicators: () => [
								[session.peers[0].services.pubsub.publicKey.hashcode()],
							],
						},
						cache: () => new Cache(createStore()),
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

				// try close and load
				await store.docs.log.close();
				await store.docs.log.load();
				await store.docs.log.close();
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
					log: {
						replication: {
							replicators: () => [],
						},
						cache: () => new Cache(createStore()),
					},
				});
				const insertions = 100;
				const rngs: string[] = [];
				for (let i = 0; i < insertions; i++) {
					rngs.push(Buffer.from(randomBytes(1e5)).toString("base64"));
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
					role: new ReplicatorType(),
					log: {
						replication: {
							replicators: () => [],
						},
						cache: () => new Cache(createStore()),
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
					(await store.docs.log.values.toArray()).map((x) => x.hash)
				).toEqual([deleteOperation.hash]); // the delete operation
			});
		});

		describe("indexBy", () => {
			let store: Program;
			let store2: Program;

			beforeAll(async () => {
				session = await LSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
				await store2?.close();
			});

			afterAll(async () => {
				await session.stop();
			});

			describe("string", () => {
				class SimpleDocument {
					@field({ type: "string" })
					id: string;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: string; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				@variant("test_index_documents")
				class TestSimpleStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<SimpleDocument>;

					constructor(properties: { docs: Documents<SimpleDocument> }) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async setup(): Promise<void> {
						await this.docs.setup({ type: SimpleDocument });
					}
				}
				it("it will throw error if indexBy does not exist in document", async () => {
					store = new TestSimpleStore({
						docs: new Documents<SimpleDocument>({
							index: new DocumentIndex({
								indexBy: "__missing__",
							}),
						}),
					});
					await store.init(session.peers[0], await createIdentity(), {
						role: new ReplicatorType(),
					});

					let doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world",
					});

					// put doc
					await expect(
						(store as TestSimpleStore).docs.put(doc)
					).rejects.toThrowError(
						"The provided key value is null or undefined, expecting string or Uint8array"
					);
				});

				it("can StringQuery index", async () => {
					store = new TestSimpleStore({
						docs: new Documents<SimpleDocument>({
							index: new DocumentIndex({
								indexBy: "id",
							}),
						}),
					});
					await store.init(session.peers[0], await createIdentity(), {
						role: new ReplicatorType(),
					});

					let doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world",
					});

					await (store as TestSimpleStore).docs.put(doc);

					const results = await (store as TestSimpleStore).docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "id",
									value: "123",
									caseInsensitive: false,
									method: StringMatchMethod.contains,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(results).toHaveLength(1);
				});
			});

			describe("bytes", () => {
				class SimpleDocument {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: Uint8Array; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				@variant("test_index_documents")
				class TestSimpleStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<SimpleDocument>;

					constructor(properties: { docs: Documents<SimpleDocument> }) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async setup(): Promise<void> {
						await this.docs.setup({ type: SimpleDocument });
					}
				}

				it("index as Uint8array", async () => {
					store = new TestSimpleStore({
						docs: new Documents<SimpleDocument>({
							index: new DocumentIndex({
								indexBy: "id",
							}),
						}),
					});
					await store.init(session.peers[0], await createIdentity(), {
						role: new ReplicatorType(),
					});

					const id = new Uint8Array([1, 2, 3]);
					let doc = new SimpleDocument({
						id,
						value: "Hello world",
					});

					await (store as TestSimpleStore).docs.put(doc);
					const results = await (store as TestSimpleStore).docs.index.query(
						new SearchRequest({
							queries: [
								new ByteMatchQuery({
									key: "id",
									value: id,
								}),
							],
						})
					);
					expect(results).toHaveLength(1);
				});
			});
		});

		describe("index", () => {
			let store: TestStore;
			let store2: TestStore;

			beforeAll(async () => {
				session = await LSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
				await store2?.close();
			});

			afterAll(async () => {
				await session.stop();
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
					log: {
						replication: {
							replicators: () => [],
						},
						cache: () => new Cache(createStore()),
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
				expect(store.docs.log.values.length).toEqual(1);
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

					log: {
						replication: {
							replicators: () => [],
						},
						cache: () => new Cache(createStore()),
						trim: { type: "length", to: 10 },
					},
				});

				for (let i = 0; i < 100; i++) {
					await store.docs.put(
						new Document({
							id: Buffer.from(String(i)),
							name: "Hello world " + String(i),
						}),
						{ nexts: [] }
					);
				}

				expect(store.docs.index.size).toEqual(10);
				expect(store.docs.log.values.length).toEqual(10);
				expect(store.docs.log.headsIndex.index.size).toEqual(10);
			});

			describe("field extractor", () => {
				it("filters field", async () => {
					let indexedNameField = "xyz";
					class FilteredStore extends TestStore {
						constructor(properties: { docs: Documents<Document> }) {
							super({ ...properties });
						}
						async setup(): Promise<void> {
							await this.docs.setup({
								type: Document,
								index: {
									fields: (obj) => {
										return { [indexedNameField]: obj.name };
									},
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
						log: {
							replication: {
								replicators: () => [
									[session.peers[0].services.pubsub.publicKey.hashcode()],
								],
							},

							cache: () => new Cache(createStore()),
						},
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

					await waitForPeersStreams(
						session.peers[0].services.blocks,
						session.peers[1].services.blocks
					);

					store2 = (await FilteredStore.load<FilteredStore>(
						session.peers[1].services.blocks,
						store.address
					))!;

					await store2.init(session.peers[1], await createIdentity(), {
						role: new ReplicatorType(),
						log: {
							replication: {
								replicators: () => [
									[session.peers[0].services.pubsub.publicKey.hashcode()],
								],
							},
							cache: () => new Cache(createStore()),
						},
					});

					let results = await store2.docs.index.query(
						new SearchRequest({ queries: [] })
					);
					expect(results).toHaveLength(1);
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
									session.peers[i].services.blocks,
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
						log: {
							replication: {
								replicators: () => [
									[session.peers[0].services.pubsub.publicKey.hashcode()],
								],
							},
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

							cache: () => new Cache(createStore()),
						},
					});
					stores.push(store);
				}

				writeStore = stores[0];

				let doc = new Document({
					id: Buffer.from("1"),
					name: "hello",
					number: 1n,
				});

				let docEdit = new Document({
					id: Buffer.from("1"),
					name: "hello world",
					number: 1n,
					bool: true,
					data: new Uint8Array([1]),
				});

				let doc2 = new Document({
					id: Buffer.from("2"),
					name: "hello world",
					number: 4n,
				});

				let doc2Edit = new Document({
					id: Buffer.from("2"),
					name: "Hello World",
					number: 2n,
					data: new Uint8Array([2]),
				});

				let doc3 = new Document({
					id: Buffer.from("3"),
					name: "foo",
					number: 3n,
					data: new Uint8Array([3]),
				});

				let doc4 = new Document({
					id: Buffer.from("4"),
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
				let results: Document[] = await stores[0].docs.index.query(
					new SearchRequest({ queries: [] })
				);
				expect(results).toHaveLength(4);
			});

			it("match locally", async () => {
				let results: Document[] = await stores[0].docs.index.query(
					new SearchRequest({
						queries: [],
					}),
					{ remote: false }
				);
				expect(results).toHaveLength(4);
			});

			it("match all", async () => {
				let results: Document[] = await stores[1].docs.index.query(
					new SearchRequest({
						queries: [],
					}),
					{ remote: { amount: 1 } }
				);
				expect(results).toHaveLength(4);
			});

			describe("sync", () => {
				it("can match sync", async () => {
					expect(stores[1].docs.index.size).toEqual(0);
					let canAppendEvents = 0;
					let canAppend = stores[1].docs["_optionCanAppend"]?.bind(
						stores[1].docs
					);
					let syncEvents = 0;
					let sync = stores[1].docs.index["_sync"].bind(stores[1].docs.index);
					stores[1].docs.index["_sync"] = async (r) => {
						syncEvents += 1;
						return sync(r);
					};
					stores[1].docs["_optionCanAppend"] = async (e) => {
						canAppendEvents += 1;
						return !canAppend || canAppend(e);
					};

					await stores[1].docs.index.query(
						new SearchRequest({
							queries: [],
						}),
						{ remote: { amount: 1, sync: true } }
					);
					await waitFor(() => stores[1].docs.index.size === 4);
					expect(stores[1].docs.log.length).toEqual(6); // 4 documents where 2 have been edited once (4 + 2)
					expect(canAppendEvents).toEqual(6); // 4 documents where 2 have been edited once (4 + 2)
					expect(syncEvents).toEqual(1);

					await stores[1].docs.index.query(
						new SearchRequest({
							queries: [],
						}),
						{ remote: { amount: 1, sync: true } }
					);
					await waitFor(() => syncEvents == 2);
					expect(canAppendEvents).toEqual(6); // no new checks, since all docs already added
				});
				it("will not sync already existing", async () => {});
			});

			describe("string", () => {
				it("exact", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "name",
									value: "hello world",
									caseInsensitive: true,
								}),
							],
						})
					);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("exact-case-insensitive", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "name",
									value: "Hello World",
									caseInsensitive: true,
								}),
							],
						})
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("exact case sensitive", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "name",
									value: "Hello World",
									caseInsensitive: false,
								}),
							],
						})
					);
					expect(responses).toHaveLength(1);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["2"]);
					responses = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "name",
									value: "hello world",
									caseInsensitive: false,
								}),
							],
						})
					);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1"]);
				});
				it("prefix", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "name",
									value: "hel",
									method: StringMatchMethod.prefix,
									caseInsensitive: true,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("contains", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new StringMatch({
									key: "name",
									value: "ello",
									method: StringMatchMethod.contains,
									caseInsensitive: true,
								}),
							],
						})
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				describe("arr", () => {
					let docArray1 = new Document({
						id: Buffer.from("a"),
						name: "_",
						number: undefined,
						tags: ["Hello", "World"],
					});

					let docArray2 = new Document({
						id: Buffer.from("b"),
						name: "__",
						number: undefined,
						tags: ["Hello"],
					});
					beforeEach(async () => {
						await writeStore.docs.put(docArray1);
						await writeStore.docs.put(docArray2);
					});
					afterEach(async () => {
						await writeStore.docs.del(docArray1.id);
						await writeStore.docs.del(docArray2.id);
					});
					it("arr", async () => {
						let responses: Document[] = await stores[1].docs.index.query(
							new SearchRequest({
								queries: [
									new StringMatch({
										key: "tags",
										value: "world",
										method: StringMatchMethod.contains,
										caseInsensitive: true,
									}),
								],
							})
						);
						expect(responses).toHaveLength(1);
						expect(
							responses.map((x) => Buffer.from(x.id).toString("utf8"))
						).toContainAllValues(["a"]);
					});
				});
			});

			it("missing", async () => {
				let responses: Document[] = await stores[1].docs.index.query(
					new SearchRequest({
						queries: [
							new MissingField({
								key: "name",
							}),
						],
					}),
					{ remote: { amount: 1 } }
				);
				expect(responses).toHaveLength(1);
				expect(
					responses.map((x) => Buffer.from(x.id).toString("utf8"))
				).toEqual(["4"]);
			});

			it("bytes", async () => {
				let responses: Document[] = await stores[1].docs.index.query(
					new SearchRequest({
						queries: [
							new ByteMatchQuery({
								key: "data",
								value: Buffer.from([1]),
							}),
						],
					})
				);
				expect(responses).toHaveLength(1);
				expect(
					responses.map((x) => Buffer.from(x.id).toString("utf8"))
				).toEqual(["1"]);
			});

			it("bool", async () => {
				let responses: Document[] = await stores[1].docs.index.query(
					new SearchRequest({
						queries: [
							new BoolQuery({
								key: "bool",
								value: true,
							}),
						],
					}),
					{ remote: { amount: 1 } }
				);
				expect(responses).toHaveLength(1);
				expect(
					responses.map((x) => Buffer.from(x.id).toString("utf8"))
				).toEqual(["1"]);
			});

			describe("logical", () => {
				it("and", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new And([
									new StringMatch({
										key: "name",
										value: "hello",
										caseInsensitive: true,
										method: StringMatchMethod.contains,
									}),
									new StringMatch({
										key: "name",
										value: "world",
										caseInsensitive: true,
										method: StringMatchMethod.contains,
									}),
								]),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("or", async () => {
					let responses: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new Or([
									new ByteMatchQuery({
										key: "id",
										value: Buffer.from("1"),
									}),
									new ByteMatchQuery({
										key: "id",
										value: Buffer.from("2"),
									}),
								]),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});
			});

			describe("number", () => {
				it("equal", async () => {
					let response: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Equal,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(response).toHaveLength(1);
					expect(response[0].number).toEqual(2n);
				});

				it("gt", async () => {
					let response: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Greater,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(response).toHaveLength(1);
					expect(response[0].number).toEqual(3n);
				});

				it("gte", async () => {
					let response: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new IntegerCompare({
									key: "number",
									compare: Compare.GreaterOrEqual,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					response.sort((a, b) =>
						bigIntSort(a.number as bigint, b.number as bigint)
					);
					expect(response).toHaveLength(2);
					expect(response[0].number).toEqual(2n);
					expect(response[1].number).toEqual(3n);
				});

				it("lt", async () => {
					let response: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Less,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					expect(response).toHaveLength(1);
					expect(response[0].number).toEqual(1n);
				});

				it("lte", async () => {
					let response: Document[] = await stores[1].docs.index.query(
						new SearchRequest({
							queries: [
								new IntegerCompare({
									key: "number",
									compare: Compare.LessOrEqual,
									value: 2n,
								}),
							],
						}),
						{ remote: { amount: 1 } }
					);
					response.sort((a, b) =>
						bigIntSort(a.number as bigint, b.number as bigint)
					);
					expect(response).toHaveLength(2);
					expect(response[0].number).toEqual(1n);
					expect(response[1].number).toEqual(2n);
				});
			});

			describe("concurrently", () => {
				it("can query concurrently", async () => {
					// TODO add more concurrency
					let promises: Promise<Document[]>[] = [];
					let concurrency = 100;
					for (let i = 0; i < concurrency; i++) {
						if (i % 2 === 0) {
							promises.push(
								stores[1].docs.index.query(
									new SearchRequest({
										queries: [
											new IntegerCompare({
												key: "number",
												compare: Compare.GreaterOrEqual,
												value: 2n,
											}),
										],
									}),
									{ remote: { amount: 1 } }
								)
							);
						} else {
							promises.push(
								stores[1].docs.index.query(
									new SearchRequest({
										queries: [
											new IntegerCompare({
												key: "number",
												compare: Compare.Less,
												value: 2n,
											}),
										],
									}),
									{ remote: { amount: 1 } }
								)
							);
						}
					}

					let results = await Promise.all(promises);
					for (let i = 0; i < concurrency; i++) {
						if (i % 2 === 0) {
							// query1
							expect(results[i]).toHaveLength(2);
							results[i].sort((a, b) => Number(a.number! - b.number!));
							expect(results[i][0].number === 2n).toBeTrue(); // Jest can't seem to output BN if error, so we do equals manually
							expect(results[i][1].number === 3n).toBeTrue(); // Jest can't seem to output BN if error, so we do equals manually
						} else {
							// query2
							expect(results[i]).toHaveLength(1);
							expect(results[i][0].number === 1n).toBeTrue();
						}
					}
				});
			});
		});

		describe("sort", () => {
			let peersCount = 3,
				stores: TestStore[] = [];

			const put = async (storeIndex: number, id: number) => {
				let doc = new Document({
					id: Buffer.from(String(id)),
					name: String(id),
					number: BigInt(id),
				});
				return stores[storeIndex].docs.put(doc);
			};

			const checkIterate = async (
				fromStoreIndex: number,
				batches: bigint[][],
				query = new IntegerCompare({
					key: "number",
					compare: Compare.GreaterOrEqual,
					value: 0n,
				})
			) => {
				const req = new SearchSortedRequest({
					queries: [query],
					sort: [new Sort({ direction: SortDirection.ASC, key: "number" })],
				});
				const iterator = await stores[fromStoreIndex].docs.index.iterate(req);
				for (const batch of batches) {
					expect(iterator.done()).toBeFalse();
					const next = await iterator.next(batch.length);
					expect(next.map((x) => x.number)).toEqual(batch);
				}
				expect(iterator.done()).toBeTrue();
			};

			beforeAll(async () => {
				session = await LSession.connected(peersCount);
			});

			beforeEach(async () => {
				// Create store
				for (let i = 0; i < peersCount; i++) {
					const store =
						i > 0
							? (await TestStore.load<TestStore>(
									session.peers[i].services.blocks,
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
						log: {
							replication: {
								replicators: () =>
									session.peers.map((x) => [x.services.pubsub.publicKeyHash]),
							},
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

							cache: () => new Cache(createStore()),
						},
					});

					stores.push(store);
				}

				// Wait for ack that everone can connect to each outher through the rpc topic
				for (let i = 0; i < session.peers.length; i++) {
					await waitForSubscribers(
						session.peers[i],
						session.peers.filter((_v, ix) => ix !== i),
						stores[i].docs.index._query.rpcTopic
					);
				}
			});

			afterEach(async () => {
				await Promise.all(stores.map((x) => x.drop()));
				stores = [];
			});

			afterAll(async () => {
				await session.stop();
			});

			it("empty", async () => {
				for (let i = 0; i < session.peers.length; i++) {
					await checkIterate(i, []);
				}
			});
			it("one peer", async () => {
				await put(0, 0);
				await put(0, 1);
				await put(0, 2);
				expect(stores[0].docs.index.size).toEqual(3);
				for (let i = 0; i < session.peers.length; i++) {
					await checkIterate(i, [[0n], [1n], [2n]]);
					await checkIterate(i, [[0n, 1n, 2n]]);
					await checkIterate(i, [[0n, 1n], [2n]]);
					await checkIterate(i, [[0n], [1n, 2n]]);
				}
			});

			it("multiple peers", async () => {
				await put(0, 0);
				await put(0, 1);
				let e2 = await put(0, 2);
				await stores[1].docs.log.join([e2.entry]); // keep doc2 at both node 0 and 1
				await put(1, 3);
				await put(1, 4);
				expect(stores[1].docs.index.size).toEqual(3);
				for (let i = 0; i < session.peers.length; i++) {
					await checkIterate(i, [[0n, 1n, 2n, 3n, 4n]]);
					await checkIterate(i, [[0n], [1n, 2n, 3n, 4n]]);
				}
			});

			it("concurrently-multiple peers", async () => {
				await put(0, 0);
				await put(0, 1);
				let e2 = await put(0, 2);
				await stores[1].docs.log.join([e2.entry]); // keep doc2 at both node 0 and 1
				await put(1, 3);
				await put(1, 4);

				expect(stores[1].docs.index.size).toEqual(3);
				let promises: Promise<any>[] = [];
				for (let i = 0; i < session.peers.length; i++) {
					promises.push(checkIterate(i, [[0n, 1n, 2n, 3n, 4n]]));
					promises.push(checkIterate(i, [[0n], [1n, 2n, 3n, 4n]]));
					promises.push(
						checkIterate(i, [
							[0n, 1n],
							[2n, 3n, 4n],
						])
					);
					promises.push(
						checkIterate(i, [
							[0n, 1n, 2n],
							[3n, 4n],
						])
					);
					promises.push(checkIterate(i, [[0n, 1n, 2n, 3n], [4n]]));
				}
				await Promise.all(promises);
			});

			it("sorts by order", async () => {
				await put(0, 0);
				await put(0, 1);
				await put(0, 2);
				{
					const iterator = await stores[0].docs.index.iterate(
						new SearchSortedRequest({
							queries: [],
							sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
						})
					);
					expect(iterator.done()).toBeFalse();
					const next = await iterator.next(3);
					expect(next.map((x) => x.name)).toEqual(["0", "1", "2"]);
					expect(iterator.done()).toBeTrue();
				}
				{
					const iterator = await stores[0].docs.index.iterate(
						new SearchSortedRequest({
							queries: [],
							sort: [new Sort({ direction: SortDirection.DESC, key: "name" })],
						})
					);
					expect(iterator.done()).toBeFalse();
					const next = await iterator.next(3);
					expect(next.map((x) => x.name)).toEqual(["2", "1", "0"]);
					expect(iterator.done()).toBeTrue();
				}
			});

			it("strings", async () => {
				await put(0, 0);
				await put(0, 1);
				await put(0, 2);

				const iterator = await stores[0].docs.index.iterate(
					new SearchSortedRequest({
						queries: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
					})
				);
				expect(iterator.done()).toBeFalse();
				const next = await iterator.next(3);
				expect(next.map((x) => x.name)).toEqual(["0", "1", "2"]);
				expect(iterator.done()).toBeTrue();
			});

			// TODO test iterator.return() to stop pending promises

			// TODO deletion while sort

			// TODO session timeouts?
		});
	});

	describe("program as value", () => {
		@variant("subprogram")
		class SubProgram extends Program {
			@field({ type: fixedArray("u8", 32) })
			id: Uint8Array;
			@field({ type: Log })
			log: Log<any>;

			constructor() {
				super();
				this.id = randomBytes(32);
				this.log = new Log();
			}
			async setup() {
				return this.log.setup();
			}
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
			async setup(canOpen = () => Promise.resolve(true)): Promise<void> {
				await this.docs.setup({
					type: SubProgram,
					canOpen,
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
						session.peers[i].services.blocks,
						session.peers[0].services.blocks
					);
				}
				const openEvents: Program[] = [];
				const store =
					i > 0
						? (await TestStore.load<TestStore>(
								session.peers[i].services.blocks,
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

					log: {
						replication: {
							replicator: () => Promise.resolve(true),
							replicators: () => [
								[session.peers[0].services.pubsub.publicKey.hashcode()],
							],
						},
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
						cache: () => new Cache(createStore()),
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

		it("can put after open", async () => {
			const subProgram = new SubProgram();
			await subProgram.init(session.peers[0], await createIdentity(), {
				role: new ReplicatorType(),
			});
			await stores[0].store.docs.put(subProgram); // open by default, why or why not? Yes because replicate = true
			expect(stores[0].openEvents).toHaveLength(1);
			expect(stores[0].openEvents[0]).toEqual(subProgram);
		});

		// TODO test can open after put (?)

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
				log: {
					replication: {
						replicators: () => [],
					},
					cache: () => new Cache(createStore()),
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
			await stores[0].store.docs.log.join(
				await stores[1].store.docs.log.values.toArray()
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
									session.peers[i].services.blocks,
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
						log: {
							replication: {
								replicators: () => [
									session.peers.map((x) =>
										x.services.pubsub.publicKey.hashcode()
									),
								],
							},
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

							cache: () => new Cache(createStore()),
						},
					});
					stores.push(store);
				}

				for (let i = 0; i < stores.length; i++) {
					const fn = stores[i].docs.index.queryHandler.bind(
						stores[i].docs.index
					);
					stores[i].docs.index.queryHandler = (a) => {
						counters[i] += 1;
						return fn(a);
					};
					await waitForSubscribers(
						session.peers[i],
						session.peers.filter((_v, ix) => ix !== i),
						stores[i].docs.index._query.rpcTopic
					);
				}
			});

			beforeEach(() => {
				counters = new Array(stores.length).fill(0);
			});

			afterAll(async () => {
				await Promise.all(stores.map((x) => x.drop()));
				await session.stop();
			});

			/*  TODO query all if undefined?
			
			it("queries all if undefined", async () => {
				stores[0].docs.log["_replication"].replicators = () => undefined;
				await stores[0].docs.index.query(new SearchRequest({ queries: [] }), {
					remote: { amount: 2 },
				});
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			}); */

			it("all", async () => {
				stores[0].docs.log["_replication"].replicators = () => [
					[stores[1].libp2p.services.pubsub.publicKey.hashcode()],
					[stores[2].libp2p.services.pubsub.publicKey.hashcode()],
				];
				await stores[0].docs.index.query(new SearchRequest({ queries: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});

			it("will always query locally", async () => {
				stores[0].docs.log["_replication"].replicators = () => [];
				await stores[0].docs.index.query(new SearchRequest({ queries: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(0);
				expect(counters[2]).toEqual(0);
			});

			it("one", async () => {
				stores[0].docs.log["_replication"].replicators = () => [
					[stores[1].libp2p.services.pubsub.publicKey.hashcode()],
				];
				await stores[0].docs.index.query(new SearchRequest({ queries: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(0);
			});

			it("non-local", async () => {
				stores[0].docs.log["_replication"].replicators = () => [
					[stores[1].libp2p.services.pubsub.publicKey.hashcode()],
					[stores[2].libp2p.services.pubsub.publicKey.hashcode()],
				];
				await stores[0].docs.index.query(new SearchRequest({ queries: [] }), {
					local: false,
				});
				expect(counters[0]).toEqual(0);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});
			it("ignore shard if I am replicator", async () => {
				stores[0].docs.log["_replication"].replicators = () => [
					[
						stores[0].libp2p.services.pubsub.publicKey.hashcode(),
						stores[1].libp2p.services.pubsub.publicKey.hashcode(),
					],
				];
				await stores[0].docs.index.query(new SearchRequest({ queries: [] }));
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
					stores[0].docs.log["_replication"].replicators = () => [
						[
							stores[1].libp2p.services.pubsub.publicKey.hashcode(),
							stores[2].libp2p.services.pubsub.publicKey.hashcode(),
						],
					];

					let failedOnce = false;
					for (let i = 1; i < stores.length; i++) {
						const fn = stores[i].docs.index.queryHandler.bind(
							stores[1].docs.index
						);
						stores[i].docs.index.queryHandler = (a) => {
							if (!failedOnce) {
								failedOnce = true;
								throw new Error("Expected error");
							}
							return fn(a);
						};
					}
					let timeout = 1000;
					await stores[0].docs.index.query(new SearchRequest({ queries: [] }), {
						remote: { timeout },
					});
					expect(failedOnce).toBeTrue();
					expect(counters[0]).toEqual(1);
					expect(counters[1] + counters[2]).toEqual(1);
					expect(counters[1]).not.toEqual(counters[2]);
				});

				it("will fail silently if can not reach all shards", async () => {
					stores[0].docs.log["_replication"].replicators = () => [
						[
							stores[1].libp2p.services.pubsub.publicKey.hashcode(),
							stores[2].libp2p.services.pubsub.publicKey.hashcode(),
						],
					];
					for (let i = 1; i < stores.length; i++) {
						stores[i].docs.index.queryHandler = (a) => {
							throw new Error("Expected error");
						};
					}

					let timeout = 1000;

					await stores[0].docs.index.query(new SearchRequest({ queries: [] }), {
						remote: { timeout },
					});
					expect(counters[0]).toEqual(1);
					expect(counters[1]).toEqual(0);
					expect(counters[2]).toEqual(0);
				});
			});
		});
	});
});
