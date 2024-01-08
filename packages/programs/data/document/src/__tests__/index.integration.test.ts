import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec
} from "@dao-xyz/borsh";
import { Documents, DocumentsChange, SetupOptions } from "../document-store";
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
	AbstractSearchResult,
	NoAccess,
	AbstractSearchRequest,
	Results,
	CloseIteratorRequest
} from "../query.js";
import { TestSession } from "@peerbit/test-utils";
import { Entry, Log } from "@peerbit/log";
import {
	AccessError,
	PublicSignKey,
	randomBytes,
	sha256Base64Sync,
	toBase64
} from "@peerbit/crypto";
import { v4 as uuid } from "uuid";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { Operation, PutOperation } from "../document-index.js";
import { Program } from "@peerbit/program";
import pDefer from "p-defer";

import {
	AbsoluteReplicas,
	Observer,
	Replicator,
	decodeReplicas,
	encodeReplicas
} from "@peerbit/shared-log";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { SilentDelivery } from "@peerbit/stream-interface";

BigInt.prototype["toJSON"] = function () {
	return this.toString();
};

@variant(0)
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
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: Documents })
	docs: Documents<Document>;

	constructor(properties: { docs: Documents<Document> }) {
		super();

		this.id = randomBytes(32);
		this.docs = properties.docs;
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({
			...options,
			type: Document,
			index: { ...options?.index, key: "id" }
		});
	}
}

const bigIntSort = <T extends number | bigint>(a: T, b: T): number =>
	a > b ? 1 : 0 || -(a < b);

describe("index", () => {
	let session: TestSession;

	describe("operations", () => {
		describe("basic", () => {
			let store: TestStore;
			let store2: TestStore;

			beforeAll(async () => {
				session = await TestSession.connected(2);
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
					docs: new Documents<Document>()
				});
				await session.peers[0].open(store);
				const changes: DocumentsChange<Document>[] = [];

				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world"
				});
				let doc2 = new Document({
					id: uuid(),
					name: "Hello world"
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
				await store.docs.log.log.close();
				await store.docs.log.log.load();
				await store.docs.log.log.close();
			});

			it("replication degree", async () => {
				store = new TestStore({
					docs: new Documents<Document>()
				});
				await session.peers[0].open(store);
				const changes: DocumentsChange<Document>[] = [];

				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world"
				});

				const putOperation = (await store.docs.put(doc, { replicas: 123 }))
					.entry;
				expect(
					decodeReplicas(
						putOperation as {
							meta: {
								data: Uint8Array;
							};
						}
					).getValue(store.docs.log)
				).toEqual(123);
			});

			it("many chunks", async () => {
				store = new TestStore({
					docs: new Documents<Document>()
				});
				await session.peers[0].open(store);
				const insertions = 100;
				const rngs: string[] = [];
				for (let i = 0; i < insertions; i++) {
					rngs.push(Buffer.from(randomBytes(1e5)).toString("base64"));
				}
				for (let i = 0; i < 20000; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: rngs[i]
						}),
						{ unique: true }
					);
				}
			});

			it("delete permanently", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false
					})
				});
				await session.peers[0].open(store);

				let doc = new Document({
					id: uuid(),
					name: "Hello world"
				});
				let editDoc = new Document({
					id: doc.id,
					name: "Hello world 2"
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
					(await store.docs.log.log.values.toArray()).map((x) => x.hash)
				).toEqual([deleteOperation.hash]); // the delete operation
			});
		});

		describe("replication", () => {
			let store: TestStore, store2: TestStore, store3: TestStore;

			beforeAll(async () => {
				session = await TestSession.connected(3);
			});

			beforeEach(async () => {
				store = new TestStore({
					docs: new Documents<Document>()
				});
				await session.peers[0].open(store);
				store2 = await session.peers[1].open<TestStore>(store.clone());
			});

			afterEach(async () => {
				await store?.close();
				await store2?.close();
				await store3?.close();
			});

			afterAll(async () => {
				await session.stop();
			});

			it("drops when no longer replicating as observer", async () => {
				let COUNT = 10;
				await store.docs.updateRole({
					type: "replicator",
					factor: 1
				});
				for (let i = 0; i < COUNT; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: "Hello world"
						})
					);
				}

				await waitForResolved(() =>
					expect(store2.docs.index.index.size).toEqual(COUNT)
				);

				store3 = await session.peers[2].open<TestStore>(store.clone(), {
					args: {
						role: {
							type: "replicator",
							factor: 1
						}
					}
				});
				await store2.docs.updateRole("observer");
				await waitForResolved(() =>
					expect(store3.docs.index.index.size).toEqual(COUNT)
				);
				await waitForResolved(() =>
					expect(store2.docs.index.index.size).toEqual(0)
				);
			});

			it("drops when no longer replicating with factor 0", async () => {
				let COUNT = 10;
				await store.docs.updateRole({
					type: "replicator",
					factor: 1
				});
				for (let i = 0; i < COUNT; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: "Hello world"
						})
					);
				}

				await waitForResolved(() =>
					expect(store2.docs.index.index.size).toEqual(COUNT)
				);

				store3 = await session.peers[2].open<TestStore>(store.clone(), {
					args: {
						role: {
							type: "replicator",
							factor: 1
						}
					}
				});
				await store2.docs.updateRole({ type: "replicator", factor: 0 });
				await waitForResolved(() =>
					expect(store3.docs.index.index.size).toEqual(COUNT)
				);
				await waitForResolved(() =>
					expect(store2.docs.index.index.size).toEqual(0)
				);
			});
		});

		describe("memory", () => {
			let store: TestStore;

			beforeAll(async () => {
				session = await TestSession.connected(1, {
					directory: "./tmp/document-store/drop-test/"
				});
			});

			afterEach(async () => {
				await store?.close();
			});

			afterAll(async () => {
				await session.stop();
			});

			it("can load and drop", async () => {
				store = new TestStore({
					docs: new Documents<Document>()
				});

				await session.peers[0].open(store);

				const COUNT = 100;
				for (let i = 0; i < COUNT; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: "Hello world"
						})
					);
				}
				await store.close();
				store = await session.peers[0].open<TestStore>(store.address);
				expect(store.docs.index.size).toEqual(COUNT);
				await store.drop();
				store = await session.peers[0].open<TestStore>(
					deserialize(serialize(store), TestStore)
				);
				expect(store.docs.index.size).toEqual(0);
			});
		});

		describe("events", () => {
			let stores: TestStore[];

			beforeAll(async () => {
				session = await TestSession.connected(3);
			});
			beforeEach(() => {
				stores = [];
			});
			afterEach(async () => {
				await stores.map((x) => x.close());
			});

			afterAll(async () => {
				await session.stop();
			});

			it("emits event on replication", async () => {
				const store = new TestStore({
					docs: new Documents<Document>({
						immutable: false
					})
				});
				for (const [i, peer] of session.peers.entries()) {
					if (store.closed) {
						stores.push(
							await peer.open(store, {
								args: {
									role: {
										type: "replicator",
										factor: 1
									}
								}
							})
						);
					} else {
						stores.push(
							await TestStore.open(store.address, peer, {
								args: {
									role: {
										type: "replicator",
										factor: 1
									}
								}
							})
						);
					}
				}
				for (const [i, store] of stores.entries()) {
					for (const [j, peer] of session.peers.entries()) {
						if (i === j) {
							continue;
						}
						await store.waitFor(peer.peerId);
					}
				}

				const resolver: Map<string, () => void> = new Map();
				let promises: Promise<any>[] = [];

				stores[2].docs.events.addEventListener("change", (evt) => {
					for (const doc of evt.detail.added) {
						resolver.get(toBase64(doc.id))!();
					}
				});

				for (let i = 0; i < 100; i++) {
					const doc = new Document({ id: randomBytes(32) });
					const defer = pDefer();
					const timeout = setTimeout(() => {
						defer.reject(new Error("Timeout"));
					}, 10000);
					resolver.set(toBase64(doc.id), () => {
						clearTimeout(timeout);
						defer.resolve();
					});
					promises.push(defer.promise);
					await store.docs.put(doc);
				}

				await Promise.all(promises);
			});
		});
		describe("indexBy", () => {
			let store: Program;
			let store2: Program;

			beforeAll(async () => {
				session = await TestSession.connected(2);
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
				class TestIndexStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<SimpleDocument>;

					constructor(
						properties: { docs: Documents<SimpleDocument> },
						readonly indexBy: string = "id"
					) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async open(): Promise<void> {
						await this.docs.open({
							type: SimpleDocument,
							index: { key: this.indexBy }
						});
					}
				}
				it("will throw error if indexBy does not exist in document", async () => {
					store = new TestIndexStore(
						{
							docs: new Documents<SimpleDocument>()
						},
						"__missing__"
					);

					await session.peers[0].open(store);

					let doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world"
					});

					// put doc
					await expect(
						(store as TestIndexStore).docs.put(doc)
					).rejects.toThrowError(
						"The provided key value is null or undefined, expecting string or Uint8array"
					);
				});

				it("index by another property", async () => {
					store = new TestIndexStore(
						{
							docs: new Documents<SimpleDocument>()
						},
						"value"
					);

					await session.peers[0].open(store);

					let helloWorld = "Hello world";
					let doc = new SimpleDocument({
						id: "abc 123",
						value: helloWorld
					});

					// put doc
					await (store as TestIndexStore).docs.put(doc);

					expect(
						(await (store as TestIndexStore).docs.index.get(helloWorld))?.value
					).toEqual(helloWorld);
				});

				it("can StringQuery index", async () => {
					store = new TestIndexStore({
						docs: new Documents<SimpleDocument>()
					});
					await session.peers[0].open(store);

					let doc = new SimpleDocument({
						id: "abc 123",
						value: "Hello world"
					});

					await (store as TestIndexStore).docs.put(doc);

					const results = await (store as TestIndexStore).docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "id",
									value: "123",
									caseInsensitive: false,
									method: StringMatchMethod.contains
								})
							]
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

				@variant("test_simple_store")
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
					async open(): Promise<void> {
						await this.docs.open({
							type: SimpleDocument,
							index: { key: "id" }
						});
					}
				}

				it("index as Uint8array", async () => {
					store = new TestSimpleStore({
						docs: new Documents<SimpleDocument>()
					});
					await session.peers[0].open(store);

					const id = new Uint8Array([1, 2, 3]);
					let doc = new SimpleDocument({
						id,
						value: "Hello world"
					});

					await (store as TestSimpleStore).docs.put(doc);
					const results = await (store as TestSimpleStore).docs.index.search(
						new SearchRequest({
							query: [
								new ByteMatchQuery({
									key: "id",
									value: id
								})
							]
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
				session = await TestSession.connected(2);
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
					docs: new Documents<Document>()
				});

				await session.peers[0].open(store, {
					args: {
						log: {
							trim: { type: "length", to: 1 }
						},
						role: "observer" // if we instead would do 'replicator' trimming will not be done unless other peers has joined
					}
				});

				const changes: DocumentsChange<Document>[] = [];
				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world"
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
				expect(store.docs.log.log.values.length).toEqual(1);
				expect(changes.length).toEqual(2);
				expect(changes[1].added).toHaveLength(1);
				expect(changes[1].added[0].id).toEqual(doc.id);
				expect(changes[1].removed).toHaveLength(0);
			});

			it("trim and update index", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false
					})
				});

				await session.peers[0].open(store, {
					args: {
						log: {
							trim: { type: "length" as const, to: 10 }
						},
						role: "observer" // if we instead would do 'replicator' trimming will not be done unless other peers has joined
					}
				});

				for (let i = 0; i < 100; i++) {
					await store.docs.put(
						new Document({
							id: Buffer.from(String(i)),
							name: "Hello world " + String(i)
						}),
						{ meta: { next: [] } }
					);
				}

				expect(store.docs.index.size).toEqual(10);
				expect(store.docs.log.log.values.length).toEqual(10);
				expect(store.docs.log.log.headsIndex.index.size).toEqual(10);
			});

			describe("field extractor", () => {
				let indexedNameField = "xyz";

				// We can't seem to define this class inside of the test itself (will yield error when running all tests)
				@variant("filtered_store")
				class FilteredStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<Document>;

					constructor(properties: { docs: Documents<Document> }) {
						super();

						this.id = new Uint8Array(32);
						this.docs = properties.docs;
					}

					async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
						await this.docs.open({
							...options,
							type: Document,
							index: {
								key: "id",
								fields: async (obj) => {
									return { [indexedNameField]: obj.name };
								}
							}
						});
					}
				}

				it("filters field", async () => {
					store = new FilteredStore({
						docs: new Documents<Document>()
					});
					store.docs.log.log.id = new Uint8Array(32);

					await session.peers[0].open(store);

					let doc = new Document({
						id: uuid(),
						name: "Hello world"
					});

					await store.docs.put(doc);

					let indexedValues = [...store.docs.index.index.values()];

					expect(indexedValues).toHaveLength(1);

					expect(indexedValues[0].value).toEqual({
						[indexedNameField]: doc.name
					});
					expect(indexedValues[0].reference).toBeUndefined(); // Because we dont want to keep it in memory (by default)

					await session.peers[1].services.blocks.waitFor(
						session.peers[0].peerId
					);

					store2 = (await FilteredStore.load(
						store.address!,
						session.peers[1].services.blocks
					))!;

					await session.peers[1].open(store2, {
						args: {
							role: "observer"
						}
					});

					expect(store2.docs.log.role).toBeInstanceOf(Observer);

					await store2.docs.log.waitForReplicator(
						session.peers[0].identity.publicKey
					);

					let results = await store2.docs.index.search(
						new SearchRequest({ query: [] })
					);
					expect(results).toHaveLength(1);
				});
			});
		});

		describe("search", () => {
			let peersCount = 3,
				stores: TestStore[] = [],
				writeStore: TestStore,
				canRead: (
					| undefined
					| ((obj: any, publicKey: PublicSignKey) => Promise<boolean>)
				)[] = [],
				canSearch: (
					| undefined
					| ((
							query: AbstractSearchRequest,
							publicKey: PublicSignKey
					  ) => Promise<boolean>)
				)[] = [];
			beforeAll(async () => {
				session = await TestSession.connected(peersCount);
			});

			afterAll(async () => {
				await session.stop();
			});

			beforeEach(async () => {
				stores = [];
				// Create store
				for (let i = 0; i < peersCount; i++) {
					const store =
						i > 0
							? (await TestStore.load<TestStore>(
									stores[0].address!,
									session.peers[i].services.blocks
								))!
							: new TestStore({
									docs: new Documents<Document>()
								});
					await session.peers[i].open(store, {
						args: {
							role: i === 0 ? { type: "replicator", factor: 1 } : "observer",
							index: {
								canRead:
									i === 0
										? (obj, key) => {
												return canRead[i] ? canRead[i]!(obj, key) : true;
											}
										: undefined,
								canSearch:
									i === 0
										? (query, key) => {
												return canSearch[i] ? canSearch[i]!(query, key) : true;
											}
										: undefined
							}
						}
					});
					stores.push(store);
				}

				writeStore = stores[0];

				let doc = new Document({
					id: Buffer.from("1"),
					name: "hello",
					number: 1n
				});

				let docEdit = new Document({
					id: Buffer.from("1"),
					name: "hello world",
					number: 1n,
					bool: true,
					data: new Uint8Array([1])
				});

				let doc2 = new Document({
					id: Buffer.from("2"),
					name: "hello world",
					number: 4n
				});

				let doc2Edit = new Document({
					id: Buffer.from("2"),
					name: "Hello World",
					number: 2n,
					data: new Uint8Array([2])
				});

				let doc3 = new Document({
					id: Buffer.from("3"),
					name: "foo",
					number: 3n,
					data: new Uint8Array([3])
				});

				let doc4 = new Document({
					id: Buffer.from("4"),
					name: undefined,
					number: undefined
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

				expect(stores[0].docs.log.role).toBeInstanceOf(Replicator);
				expect(stores[1].docs.log.role).toBeInstanceOf(Observer);
				await stores[1].waitFor(session.peers[0].peerId);
				await stores[1].docs.log.waitForReplicator(
					session.peers[0].identity.publicKey
				);
				await stores[0].waitFor(session.peers[1].peerId);
				canRead = new Array(stores.length).fill(undefined);
				canSearch = new Array(stores.length).fill(undefined);
			});

			afterEach(async () => {
				await Promise.all(stores.map((x) => x.drop()));
			});

			it("no-args", async () => {
				let results: Document[] = await stores[0].docs.index.search(
					new SearchRequest({ query: [] })
				);
				expect(results).toHaveLength(4);
			});

			it("match locally", async () => {
				let results: Document[] = await stores[0].docs.index.search(
					new SearchRequest({
						query: []
					}),
					{ remote: false }
				);
				expect(results).toHaveLength(4);
			});

			it("match all", async () => {
				let results: Document[] = await stores[1].docs.index.search(
					new SearchRequest({
						query: []
					}),
					{ remote: { amount: 1 } }
				);
				expect(results).toHaveLength(4);
			});

			describe("sync", () => {
				it("can match sync", async () => {
					expect(stores[1].docs.index.size).toEqual(0);
					let canPerformEvents = 0;
					let canPerform = stores[1].docs["_optionCanPerform"]?.bind(
						stores[1].docs
					);
					let syncEvents = 0;
					let sync = stores[1].docs.index["_sync"].bind(stores[1].docs.index);
					stores[1].docs.index["_sync"] = async (r) => {
						syncEvents += 1;
						return sync(r);
					};
					stores[1].docs["_optionCanPerform"] = async (a, b) => {
						canPerformEvents += 1;
						return !canPerform || canPerform(a, b);
					};

					await stores[1].docs.index.search(
						new SearchRequest({
							query: []
						}),
						{ remote: { amount: 1, sync: true } }
					);
					await waitFor(() => stores[1].docs.index.size === 4);
					expect(stores[1].docs.log.log.length).toEqual(6); // 4 documents where 2 have been edited once (4 + 2)
					expect(canPerformEvents).toEqual(6); // 4 documents where 2 have been edited once (4 + 2)
					expect(syncEvents).toEqual(1);

					await stores[1].docs.index.search(
						new SearchRequest({
							query: []
						}),
						{ remote: { amount: 1, sync: true } }
					);
					await waitFor(() => syncEvents == 2);
					expect(canPerformEvents).toEqual(6); // no new checks, since all docs already added
				});
				it("will not sync already existing", async () => {});
			});

			describe("string", () => {
				it("exact", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "name",
									value: "hello world",
									caseInsensitive: true
								})
							]
						})
					);
					expect(
						responses.map((x) => Buffer.from(x.id).toString())
					).toContainAllValues(["1", "2"]);
				});

				it("exact-case-insensitive", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "name",
									value: "Hello World",
									caseInsensitive: true
								})
							]
						})
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("exact case sensitive", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "name",
									value: "Hello World",
									caseInsensitive: false
								})
							]
						})
					);
					expect(responses).toHaveLength(1);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["2"]);
					responses = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "name",
									value: "hello world",
									caseInsensitive: false
								})
							]
						})
					);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1"]);
				});
				it("prefix", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "name",
									value: "hel",
									method: StringMatchMethod.prefix,
									caseInsensitive: true
								})
							]
						}),
						{ remote: { amount: 1 } }
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("contains", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new StringMatch({
									key: "name",
									value: "ello",
									method: StringMatchMethod.contains,
									caseInsensitive: true
								})
							]
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
						tags: ["Hello", "World"]
					});

					let docArray2 = new Document({
						id: Buffer.from("b"),
						name: "__",
						number: undefined,
						tags: ["Hello"]
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
						let responses: Document[] = await stores[1].docs.index.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: "tags",
										value: "world",
										method: StringMatchMethod.contains,
										caseInsensitive: true
									})
								]
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
				let responses: Document[] = await stores[1].docs.index.search(
					new SearchRequest({
						query: [
							new MissingField({
								key: "name"
							})
						]
					}),
					{ remote: { amount: 1 } }
				);
				expect(responses).toHaveLength(1);
				expect(
					responses.map((x) => Buffer.from(x.id).toString("utf8"))
				).toEqual(["4"]);
			});

			it("bytes", async () => {
				let responses: Document[] = await stores[1].docs.index.search(
					new SearchRequest({
						query: [
							new ByteMatchQuery({
								key: "data",
								value: Buffer.from([1])
							})
						]
					})
				);
				expect(responses).toHaveLength(1);
				expect(
					responses.map((x) => Buffer.from(x.id).toString("utf8"))
				).toEqual(["1"]);
			});

			it("bool", async () => {
				let responses: Document[] = await stores[1].docs.index.search(
					new SearchRequest({
						query: [
							new BoolQuery({
								key: "bool",
								value: true
							})
						]
					}),
					{ remote: { amount: 1 } }
				);
				expect(responses).toHaveLength(1);
				expect(
					responses.map((x) => Buffer.from(x.id).toString("utf8"))
				).toEqual(["1"]);
			});

			describe("array", () => {
				describe("nested store", () => {
					@variant("test-nested-document-store")
					class NestedDocument extends Program<any> {
						@field({ type: Uint8Array })
						id: Uint8Array;

						@field({ type: Documents })
						documents: Documents<Document>;

						constructor(document: Documents<Document>) {
							super();
							this.id = randomBytes(32);
							this.documents = document;
						}
						open(args?: any): Promise<void> {
							return this.documents.open({ type: Document });
						}
					}

					@variant("test-nested-nested-document-store")
					class NestedDocumentStore extends Program<
						Partial<SetupOptions<Document>>
					> {
						@field({ type: Uint8Array })
						id: Uint8Array;

						@field({ type: Documents })
						documents: Documents<NestedDocument>;

						constructor(properties: { docs: Documents<NestedDocument> }) {
							super();
							this.id = randomBytes(32);
							this.documents = properties.docs;
						}

						async open(
							options?: Partial<SetupOptions<Document>>
						): Promise<void> {
							await this.documents.open({
								...options,
								type: NestedDocument,
								index: { ...options?.index, key: "id" },
								canOpen: () => true
							});
						}
					}

					it("nested document store", async () => {
						const nestedStore = await session.peers[0].open(
							new NestedDocumentStore({ docs: new Documents() })
						);
						const nestedDoc = new NestedDocument(new Documents());
						await session.peers[0].open(nestedDoc);
						const document = new Document({
							id: randomBytes(32),
							name: "hello"
						});
						await nestedDoc.documents.put(document);
						await nestedStore.documents.put(nestedDoc);

						const nestedStore2 =
							await session.peers[1].open<NestedDocumentStore>(
								nestedStore.address,
								{ args: { role: "observer" } }
							);
						await nestedStore2.documents.log.waitForReplicator(
							session.peers[0].identity.publicKey
						);
						const results = await nestedStore2.documents.index.search(
							new SearchRequest({
								query: [
									new StringMatch({
										key: ["documents", "name"],
										value: "hello"
									})
								]
							})
						);
						expect(results.length).toEqual(1);
					});
				});

				describe("multi-dimensional", () => {
					class MultiDimensionalDoc {
						@field({ type: Uint8Array })
						id: Uint8Array;

						@field({ type: option(vec(Uint8Array)) })
						bytesArrays?: Uint8Array[];

						@field({ type: option(vec(vec("u32"))) })
						matrix?: number[][];

						@field({ type: option(vec(Document)) })
						documents?: Document[];

						constructor(properties?: {
							bytesArrays?: Uint8Array[];
							matrix?: number[][];
							documents?: Document[];
						}) {
							this.id = randomBytes(32);
							this.matrix = properties?.matrix;
							this.bytesArrays = properties?.bytesArrays;
							this.documents = properties?.documents;
						}
					}

					@variant("test-multidim-doc-store")
					class MultiDimensionalDocStore extends Program<any> {
						@field({ type: Documents })
						documents: Documents<MultiDimensionalDoc>;

						constructor() {
							super();
							this.documents = new Documents<MultiDimensionalDoc>();
						}
						open(args?: Partial<SetupOptions<any>>): Promise<void> {
							return this.documents.open({
								...args,
								type: MultiDimensionalDoc
							});
						}
					}

					it("uint8array[]", async () => {
						const docs = await session.peers[0].open(
							new MultiDimensionalDocStore()
						);

						const d1 = new MultiDimensionalDoc({
							bytesArrays: [new Uint8Array([1]), new Uint8Array([2])]
						});
						await docs.documents.put(d1);
						await docs.documents.put(
							new MultiDimensionalDoc({ bytesArrays: [new Uint8Array([3])] })
						);

						const docsObserver =
							await session.peers[1].open<MultiDimensionalDocStore>(
								docs.address,
								{ args: { role: "observer" } }
							);
						await docsObserver.documents.log.waitForReplicator(
							session.peers[0].identity.publicKey
						);

						const results = await docsObserver.documents.index.search(
							new SearchRequest({
								query: [
									new ByteMatchQuery({
										key: "bytesArrays",
										value: new Uint8Array([2])
									})
								]
							})
						);
						expect(results.map((x) => x.id)).toEqual([new Uint8Array(d1.id)]);
					});

					it("number[][]", async () => {
						const docs = await session.peers[0].open(
							new MultiDimensionalDocStore()
						);

						const d1 = new MultiDimensionalDoc({ matrix: [[1, 2], [3]] });
						await docs.documents.put(d1);
						await docs.documents.put(
							new MultiDimensionalDoc({ matrix: [[4, 5]] })
						);

						const docsObserver =
							await session.peers[1].open<MultiDimensionalDocStore>(
								docs.address,
								{ args: { role: "observer" } }
							);
						await docsObserver.documents.log.waitForReplicator(
							session.peers[0].identity.publicKey
						);

						const results = await docsObserver.documents.index.search(
							new SearchRequest({
								query: new IntegerCompare({
									key: "matrix",
									compare: Compare.Equal,
									value: 2
								})
							})
						);
						expect(results.map((x) => x.id)).toEqual([new Uint8Array(d1.id)]);
					});

					it("Document[]", async () => {
						const docs = await session.peers[0].open(
							new MultiDimensionalDocStore()
						);

						const d1 = new MultiDimensionalDoc({
							documents: [new Document({ id: randomBytes(32), number: 123n })]
						});
						await docs.documents.put(d1);
						await docs.documents.put(
							new MultiDimensionalDoc({
								documents: [new Document({ id: randomBytes(32), number: 124n })]
							})
						);

						const docsObserver =
							await session.peers[1].open<MultiDimensionalDocStore>(
								docs.address,
								{ args: { role: "observer" } }
							);
						await docsObserver.documents.log.waitForReplicator(
							session.peers[0].identity.publicKey
						);

						const results = await docsObserver.documents.index.search(
							new SearchRequest({
								query: new IntegerCompare({
									key: ["documents", "number"],
									compare: Compare.Equal,
									value: 123n
								})
							})
						);
						expect(results.map((x) => x.id)).toEqual([new Uint8Array(d1.id)]);
					});
				});
			});

			describe("canRead", () => {
				it("no read access will return a response with 0 results", async () => {
					const canReadInvocation: [Document, PublicSignKey][] = [];
					canRead[0] = (a, b) => {
						canReadInvocation.push([a, b]);
						return Promise.resolve(false);
					};
					let allResponses: AbstractSearchResult<Document>[] = [];
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: []
						}),
						{
							local: false,
							remote: {
								onResponse: (r) => {
									allResponses.push(r);
								}
							}
						}
					);
					expect(responses).toHaveLength(0);
					expect(allResponses).toHaveLength(1);
					expect(allResponses[0]).toBeInstanceOf(Results);
					expect(canReadInvocation).toHaveLength(4); // 4 documents in store
					expect(canReadInvocation[0][0]).toBeInstanceOf(Document);
					expect(canReadInvocation[0][1]).toBeInstanceOf(Ed25519PublicKey);
				});
			});

			describe("canSearch", () => {
				it("no search access will return an error response", async () => {
					const canSearchInvocations: [AbstractSearchRequest, PublicSignKey][] =
						[];
					canSearch[0] = (a, b) => {
						canSearchInvocations.push([a, b]);
						return Promise.resolve(false);
					};
					let allResponses: AbstractSearchResult<Document>[] = [];
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: []
						}),
						{
							local: false,
							remote: {
								amount: 1,
								onResponse: (r) => {
									allResponses.push(r);
								}
							}
						}
					);
					expect(responses).toHaveLength(0);
					expect(allResponses).toHaveLength(1);
					expect(allResponses[0]).toBeInstanceOf(NoAccess);
					expect(canSearchInvocations).toHaveLength(1);
					expect(canSearchInvocations[0][0]).toBeInstanceOf(SearchRequest);
					expect(canSearchInvocations[0][1]).toBeInstanceOf(Ed25519PublicKey);
				});
			});

			describe("logical", () => {
				it("and", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new And([
									new StringMatch({
										key: "name",
										value: "hello",
										caseInsensitive: true,
										method: StringMatchMethod.contains
									}),
									new StringMatch({
										key: "name",
										value: "world",
										caseInsensitive: true,
										method: StringMatchMethod.contains
									})
								])
							]
						}),
						{ remote: { amount: 1 } }
					);
					expect(responses).toHaveLength(2);
					expect(
						responses.map((x) => Buffer.from(x.id).toString("utf8"))
					).toContainAllValues(["1", "2"]);
				});

				it("or", async () => {
					let responses: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new Or([
									new ByteMatchQuery({
										key: "id",
										value: Buffer.from("1")
									}),
									new ByteMatchQuery({
										key: "id",
										value: Buffer.from("2")
									})
								])
							]
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
					let response: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Equal,
									value: 2n
								})
							]
						}),
						{ remote: { amount: 1 } }
					);
					expect(response).toHaveLength(1);
					expect(response[0].number).toEqual(2n);
				});

				it("gt", async () => {
					let response: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Greater,
									value: 2n
								})
							]
						}),
						{ remote: { amount: 1 } }
					);
					expect(response).toHaveLength(1);
					expect(response[0].number).toEqual(3n);
				});

				it("gte", async () => {
					let response: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.GreaterOrEqual,
									value: 2n
								})
							]
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
					let response: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.Less,
									value: 2n
								})
							]
						}),
						{ remote: { amount: 1 } }
					);
					expect(response).toHaveLength(1);
					expect(response[0].number).toEqual(1n);
				});

				it("lte", async () => {
					let response: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [
								new IntegerCompare({
									key: "number",
									compare: Compare.LessOrEqual,
									value: 2n
								})
							]
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
								stores[1].docs.index.search(
									new SearchRequest({
										query: [
											new IntegerCompare({
												key: "number",
												compare: Compare.GreaterOrEqual,
												value: 2n
											})
										]
									}),
									{ remote: { amount: 1 } }
								)
							);
						} else {
							promises.push(
								stores[1].docs.index.search(
									new SearchRequest({
										query: [
											new IntegerCompare({
												key: "number",
												compare: Compare.Less,
												value: 2n
											})
										]
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

			let canRead: (
				| undefined
				| ((publicKey: PublicSignKey) => Promise<boolean>)
			)[] = [];

			const put = async (storeIndex: number, id: number) => {
				let doc = new Document({
					id: Buffer.from(String(id)),
					name: String(id),
					number: BigInt(id)
				});
				return stores[storeIndex].docs.put(doc);
			};

			const checkIterate = async (
				fromStoreIndex: number,
				batches: bigint[][],
				query = new IntegerCompare({
					key: "number",
					compare: Compare.GreaterOrEqual,
					value: 0n
				})
			) => {
				await waitForResolved(async () => {
					const req = new SearchRequest({
						query: [query],
						sort: [new Sort({ direction: SortDirection.ASC, key: "number" })]
					});
					const iterator = stores[fromStoreIndex].docs.index.iterate(req);

					if (batches.length === 0) {
						// No fetches has been made, so we don't know whether we are done yet
						expect(iterator.done()).toBeFalse();
					} else {
						for (const batch of batches) {
							expect(iterator.done()).toBeFalse();
							const next = await iterator.next(batch.length);
							expect(next.map((x) => x.number)).toEqual(batch);
						}
						expect(iterator.done()).toBeTrue();
					}
				});
			};

			beforeAll(async () => {
				session = await TestSession.connected(peersCount);
			});

			beforeEach(async () => {
				canRead = new Array(stores.length).fill(undefined);

				// Create store
				for (let i = 0; i < peersCount; i++) {
					const store =
						i > 0
							? (await TestStore.load<TestStore>(
									stores[0].address!,
									session.peers[i].services.blocks
								))!
							: new TestStore({
									docs: new Documents<Document>()
								});
					store.docs.log.append = async (a, b) => {
						// Omit synchronization so results are always the same (HACKY)
						b = {
							...b,
							meta: {
								...b?.meta,
								data: encodeReplicas(new AbsoluteReplicas(1))
							}
						};
						return store.docs.log.log.append(a, b);
					};
					await session.peers[i].open(store, {
						args: {
							index: {
								canRead: (_document, key) => {
									return canRead[i] ? canRead[i]!(key) : true;
								}
							},
							role: {
								type: "replicator",
								factor: 1
							},
							replicas: { min: 1 } // make sure documents only exist once
						}
					});
					stores.push(store);
				}
				// Wait for ack that everone can connect to each outher through the rpc topic
				for (let i = 0; i < session.peers.length; i++) {
					await stores[i].docs.log.waitForReplicator(
						...session.peers
							.filter((_v, ix) => ix !== i)
							.map((x) => x.identity.publicKey)
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

			// TODO make sure documents are evenly distrubted before querye
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
				await stores[1].docs.log.log.join([e2.entry]); // some overlap
				await put(1, 3);
				await put(1, 4);
				for (let i = 0; i < session.peers.length; i++) {
					await checkIterate(i, [[0n, 1n, 2n, 3n, 4n]]);
					await checkIterate(i, [[0n], [1n, 2n, 3n, 4n]]);
				}
			});

			it("deduplication on first entry", async () => {
				let e0 = await put(0, 0);
				await put(0, 1);
				await put(0, 2);
				await stores[1].docs.log.log.join([e0.entry]); // duplication on first entry
				await put(1, 3);
				await put(0, 4);
				await checkIterate(0, [
					[0n, 1n],
					[2n, 3n, 4n]
				]);
			});

			it("concurrently-multiple peers", async () => {
				let e0 = await put(0, 0);
				await put(0, 1);
				await put(0, 2);
				await stores[1].docs.log.log.join([e0.entry]);
				await put(1, 3);
				await put(0, 4);

				let promises: Promise<any>[] = [];
				for (let i = 0; i < 1; i++) {
					promises.push(checkIterate(i, [[0n, 1n, 2n, 3n, 4n]]));
					promises.push(checkIterate(i, [[0n], [1n, 2n, 3n, 4n]]));
					promises.push(
						checkIterate(i, [
							[0n, 1n],
							[2n, 3n, 4n]
						])
					);
					promises.push(
						checkIterate(i, [
							[0n, 1n, 2n],
							[3n, 4n]
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
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.ASC, key: "name" })]
						})
					);
					expect(iterator.done()).toBeFalse();
					const next = await iterator.next(3);
					expect(next.map((x) => x.name)).toEqual(["0", "1", "2"]);
					expect(iterator.done()).toBeTrue();
				}
				{
					const iterator = await stores[0].docs.index.iterate(
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.DESC, key: "name" })]
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
					new SearchRequest({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })]
					})
				);
				expect(iterator.done()).toBeFalse();
				const next = await iterator.next(3);
				expect(next.map((x) => x.name)).toEqual(["0", "1", "2"]);
				expect(iterator.done()).toBeTrue();
			});

			it("uses indexed fields", async () => {
				const KEY = "ABC";
				await stores[0].docs.index.open({
					fields: async (obj) => {
						return { [KEY]: obj.number };
					},
					dbType: Documents,
					canSearch: () => true,
					log: stores[0].docs.log,
					sync: () => undefined as any,
					type: Document,
					indexBy: ["id"]
				});

				await put(0, 0);
				await put(0, 1);
				await put(0, 2);

				const iterator = await stores[0].docs.index.iterate(
					new SearchRequest({
						query: [],
						sort: [new Sort({ direction: SortDirection.DESC, key: KEY })]
					}),
					{
						local: true,
						remote: false
					}
				);
				const next = await iterator.next(3);
				expect(next.map((x) => x.name)).toEqual(["2", "1", "0"]);
				expect(iterator.done()).toBeTrue();
			});

			it("will retrieve partial results of not having read access", async () => {
				await put(0, 0);
				await put(1, 1);
				await put(1, 2);
				canRead[0] = () => Promise.resolve(false);

				const iterator = await stores[2].docs.index.iterate(
					new SearchRequest({
						query: [],
						sort: [new Sort({ direction: SortDirection.ASC, key: "name" })]
					})
				);
				expect((await iterator.next(1)).map((x) => x.name)).toEqual(["1"]);
				expect(iterator.done()).toBeFalse();
				expect((await iterator.next(1)).map((x) => x.name)).toEqual(["2"]);
				expect(iterator.done()).toBeTrue();
			});

			describe("close", () => {
				it("by invoking close()", async () => {
					await put(0, 0);
					await put(0, 1);
					await put(0, 2);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await stores[1].docs.index.iterate(request);
					expect(iterator.done()).toBeFalse();
					await iterator.next(2); // fetch some, but not all
					expect(
						stores[0].docs.index["_resultsCollectQueue"].get(request.idString)!
							.arr
					).toHaveLength(1);
					await iterator.close();
					await waitForResolved(
						() =>
							expect(
								stores[0].docs.index["_resultsCollectQueue"].get(
									request.idString
								)
							).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});

				it("requires correct id", async () => {
					await put(0, 0);
					await put(0, 1);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await stores[1].docs.index.iterate(request);
					expect(iterator.done()).toBeFalse();
					await iterator.next(1); // fetch some, but not all
					expect(
						stores[0].docs.index["_resultsCollectQueue"].get(request.idString)!
							.arr
					).toHaveLength(1);

					const closeRequest = new CloseIteratorRequest({ id: request.id });

					// Try to send from another peer (that is not the owner of the iterator)
					await stores[2].docs.index["_query"].send(closeRequest, {
						mode: new SilentDelivery({
							to: [session.peers[0].identity.publicKey],
							redundancy: 1
						})
					});

					await delay(2000);
					expect(
						stores[0].docs.index["_resultsCollectQueue"].get(request.idString)
					).toBeDefined();

					// send from the owner
					await stores[1].docs.index["_query"].send(closeRequest, {
						mode: new SilentDelivery({
							to: [session.peers[0].identity.publicKey],
							redundancy: 1
						})
					});

					await waitForResolved(
						() =>
							expect(
								stores[0].docs.index["_resultsCollectQueue"].get(
									request.idString
								)
							).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});

				it("end of iterator", async () => {
					await put(0, 0);
					await put(0, 1);
					await put(0, 2);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await stores[1].docs.index.iterate(request);
					expect(iterator.done()).toBeFalse();
					await iterator.next(3); // fetch some, but not all
					await waitForResolved(
						() =>
							expect(
								stores[0].docs.index["_resultsCollectQueue"].get(
									request.idString
								)
							).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});

				it("end of iterator, multiple nexts", async () => {
					await put(0, 0);
					await put(0, 1);
					await put(0, 2);
					const request = new SearchRequest({
						query: []
					});
					const iterator = await stores[1].docs.index.iterate(request);
					await iterator.next(2);
					await iterator.next(1);
					expect(iterator.done()).toBeTrue();
					await waitForResolved(
						() =>
							expect(
								stores[0].docs.index["_resultsCollectQueue"].get(
									request.idString
								)
							).toBeUndefined(),
						{ timeout: 3000, delayInterval: 50 }
					);
				});
			});

			// TODO test iterator.close() to stop pending promises

			// TODO deletion while sort

			// TODO session timeouts?
		});
	});

	describe("canAppend", () => {
		let store: TestStore;
		beforeAll(async () => {
			session = await TestSession.connected(1);
		});
		afterEach(async () => {
			await store?.close();
		});

		afterAll(async () => {
			await session.stop();
		});

		it("reject entries with unexpected payloads", async () => {
			store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>()
				})
			);
			await expect(
				store.docs.log.log.append(
					new PutOperation({ key: "key", data: randomBytes(32) })
				)
			).rejects.toThrowError(AccessError);
		});

		it("reject entries with unexpected payloads", async () => {
			store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>()
				})
			);

			store["_canAppend"] = () => true; // ignore internal

			const canAppend = await store.docs.canAppend(
				(await Entry.create({
					data: new PutOperation({ key: "key", data: randomBytes(32) }),
					identity: store.node.identity,
					store: store.docs.log.log.blocks,
					canAppend: () => true,
					encoding: store.docs.log.log.encoding
				})) as Entry<Operation<Document>>
			);

			await expect(canAppend).toBeFalse();
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
			async open() {
				return this.log.open(this.node.services.blocks, this.node.identity);
			}
		}

		@variant("test_program_documents")
		class TestStoreSubPrograms extends Program {
			@field({ type: Documents })
			docs: Documents<SubProgram>;

			constructor(properties: { docs: Documents<SubProgram> }) {
				super();
				this.docs = properties.docs;
			}
			async open(options?: Partial<SetupOptions<SubProgram>>): Promise<void> {
				await this.docs.open({
					...options,
					type: SubProgram,
					index: {
						key: ["id"]
					}
				});
			}
		}

		let stores: { store: TestStoreSubPrograms }[];
		let peersCount = 2;

		beforeAll(async () => {
			session = await TestSession.connected(peersCount);
		});
		beforeEach(async () => {
			stores = [];

			// Create store
			for (let i = 0; i < peersCount; i++) {
				if (i > 0) {
					await session.peers[i].services.blocks.waitFor(
						session.peers[0].peerId
					);
				}
				const store =
					i > 0
						? (await TestStoreSubPrograms.load<TestStoreSubPrograms>(
								stores[0].store.address!,
								session.peers[i].services.blocks
							))!
						: new TestStoreSubPrograms({
								docs: new Documents<SubProgram>()
							});

				await session.peers[i].open(store, {
					args: {
						role: i === 0 ? { type: "replicator", factor: 1 } : "observer",
						canOpen: () => true
					}
				});
				stores.push({ store });
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
			const _result = await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).toBeFalse();
			expect(
				subProgram ==
					stores[0].store.docs.index.index.values().next().value.value
			).toBeTrue();
		});

		it("can put after open", async () => {
			const subProgram = new SubProgram();
			await session.peers[0].open(subProgram);
			await stores[0].store.docs.put(subProgram.clone());
			expect(subProgram.closed).toBeFalse();
			expect(
				subProgram ==
					stores[0].store.docs.index.index.values().next().value.value
			).toBeTrue();
		});

		it("can open after put", async () => {
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			await session.peers[0].open(subProgram, { existing: "reuse" });
			expect(subProgram.closed).toBeFalse();
			expect(
				subProgram ==
					stores[0].store.docs.index.index.values().next().value.value
			).toBeTrue();
		});

		it("will close subprogram after put", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).toBeFalse();
			expect(stores[0].store.closed).toBeFalse();
			expect(stores[0].store.docs.closed).toBeFalse();

			await stores[0].store.close();
			expect(stores[0].store.closed).toBeTrue();
			expect(stores[0].store.docs.closed).toBeTrue();
			expect(subProgram.closed).toBeTrue();
		});
		it("will not close subprogram that is opened before put", async () => {
			const subProgram = new SubProgram();
			await session.peers[0].open(subProgram);
			const _result = await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).toBeFalse();
			await stores[0].store.close();
			expect(subProgram.closed).toBeFalse();
			await subProgram.close();
			expect(subProgram.closed).toBeTrue();
		});

		it("non-replicator will not open by default", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[1].store.docs.put(subProgram);
			expect(subProgram.closed).toBeTrue();
		});

		it("can open program when sync", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[1].store.docs.put(subProgram);
			expect(subProgram.closed).toBeTrue(); // Because observer? Not open by default?
			await stores[0].store.docs.log.log.join(
				[...(await stores[1].store.docs.log.log.values.toArray()).values()].map(
					(x) => deserialize(serialize(x), Entry)
				)
			);
			expect(subProgram.closed).toBeTrue(); // Because observer? Not open by default?
			expect(
				stores[0].store.docs.index.index.values().next().value.value.closed
			).toBeFalse();
		});

		it("will drop on delete", async () => {
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).toBeFalse();

			let dropped = false;
			const subprogramDropped = subProgram.drop.bind(subProgram);
			subProgram.drop = (from) => {
				dropped = true;
				return subprogramDropped(from);
			};
			await stores[0].store.docs.del(subProgram.id);
			await waitForResolved(() => expect(subProgram.closed).toBeTrue());
			expect(dropped).toBeTrue();
		});

		it("can prevent subprograms to be opened", async () => {
			stores[0].store.docs.canOpen = (_) => Promise.resolve(false);
			const subProgram = new SubProgram();
			const _result = await stores[0].store.docs.put(subProgram);
			subProgram.parents = [undefined];
			expect(subProgram.closed).toBeTrue();
		});

		describe("index", () => {
			@variant("test_program_documents_custom_fields")
			class TestStoreSubPrograms extends Program {
				@field({ type: Documents })
				docs: Documents<SubProgram>;

				constructor() {
					super();
					this.docs = new Documents();
				}
				async open(): Promise<void> {
					await this.docs.open({
						canOpen: () => Promise.resolve(true),
						type: SubProgram,
						index: {
							key: ["id"],
							fields: (obj) => {
								return { id: obj.id, custom: obj.id };
							}
						}
					});
				}
			}
			let store: TestStoreSubPrograms, store2: TestStoreSubPrograms;

			afterEach(async () => {
				store?.close();
				store2?.close();
			});
			it("can index specific fields", async () => {
				store = await session.peers[0].open(new TestStoreSubPrograms());
				store2 = await session.peers[1].open(store.clone());
				const subProgram = new SubProgram();
				const _result = await store.docs.put(subProgram);
				expect(subProgram.closed).toBeFalse();
				await waitFor(() => store2.docs.index.size === 1);
				const stores = [store, store2];
				for (const s of stores) {
					const results = await s.docs.index.search(
						new SearchRequest({
							query: [
								new ByteMatchQuery({ key: "custom", value: subProgram.id })
							]
						})
					);
					expect(results).toHaveLength(1);
					expect(results[0].id).toEqual(subProgram.id);
					expect(results[0].closed).toBeFalse();
				}
			});
		});
	});

	describe("query distribution", () => {
		describe("distribution", () => {
			let peersCount = 3,
				stores: TestStore[] = [];
			let counters: Array<number> = [];

			beforeAll(async () => {
				session = await TestSession.connected(peersCount);
				// Create store
				for (let i = 0; i < peersCount; i++) {
					const store =
						i > 0
							? (await TestStore.load<TestStore>(
									stores[0].address!,
									session.peers[i].services.blocks
								))!
							: new TestStore({
									docs: new Documents<Document>()
								});
					await session.peers[i].open(store);
					stores.push(store);
				}

				for (let i = 0; i < stores.length; i++) {
					const fn = stores[i].docs.index.processFetchRequest.bind(
						stores[i].docs.index
					);
					stores[i].docs.index.processFetchRequest = (a, b, c) => {
						counters[i] += 1;
						return fn(a, b, c);
					};
					await stores[i].docs.waitFor(
						session.peers.filter((_v, ix) => ix !== i).map((x) => x.peerId)
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
			
			it("query all if undefined", async () => {
				stores[0].docs.log["_replication"].replicators = () => undefined;
				await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
					remote: { amount: 2 },
				});
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			}); */

			it("all", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[1].node.identity.publicKey.hashcode(),
					stores[2].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});

			it("will always query locally", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(0);
				expect(counters[2]).toEqual(0);
			});

			it("one", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[1].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(0);
			});

			it("non-local", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[1].node.identity.publicKey.hashcode(),
					stores[2].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
					local: false
				});
				expect(counters[0]).toEqual(0);
				expect(counters[1]).toEqual(1);
				expect(counters[2]).toEqual(1);
			});

			/*  TODO getReplicatorUnion to provide query alternatives
		
			it("ignore shard if I am replicator", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[0].node.identity.publicKey.hashcode(),
					stores[1].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).toEqual(1);
				expect(counters[1]).toEqual(0);
				expect(counters[2]).toEqual(0);
			}); */

			/* TODO getReplicatorUnion to provide query alternatives 
			
			it("ignore myself if I am a new replicator", async () => {
				// and the other peer has been around for longer
				await stores[0].docs.updateRole("observer")
				await stores[1].docs.updateRole({
			type: 'replicator',
			factor: 1
		})
				await stores[2].docs.updateRole({type: 'replicator', factor: 1})

				await delay(2000)
				await waitForResolved(() => expect(stores[0].docs.log.getReplicatorsSorted()?.toArray().map(x => x.publicKey.hashcode())).toContainAllValues([session.peers[1].identity.publicKey.hashcode(), session.peers[2].identity.publicKey.hashcode()]));

				const t1 = +new Date();
				const minAge = 1000;
				await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
					remote: { minAge }
				});
				expect(counters[0]).toEqual(1); // will always query locally
				expect(counters[1]).toEqual(1); // but now also remotely since we can not trust local only
				expect(counters[2]).toEqual(0);
				await waitFor(() => +new Date() - t1 > minAge + 100);

				await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
					remote: { minAge }
				});
				expect(counters[0]).toEqual(2); // will always query locally
				expect(counters[1]).toEqual(1); // we don't have to query remote since local will suffice since minAge time has passed
				expect(counters[2]).toEqual(0);
			}); */

			describe("errors", () => {
				let fns: any[];

				beforeEach(() => {
					fns = stores.map((x) =>
						x.docs.index.processFetchRequest.bind(x.docs.index)
					);
				});

				afterEach(() => {
					stores.forEach((x, ix) => {
						x.docs.index.processFetchRequest = fns[ix];
					});
				});

				it("will iterate on shard until response", async () => {
					stores[0].docs.log.getReplicatorUnion = () => [
						stores[1].node.identity.publicKey.hashcode(),
						stores[2].node.identity.publicKey.hashcode()
					];

					let failedOnce = false;
					for (let i = 1; i < stores.length; i++) {
						const fn = stores[i].docs.index.processFetchRequest.bind(
							stores[1].docs.index
						);
						stores[i].docs.index.processFetchRequest = (a, b, c) => {
							if (!failedOnce) {
								failedOnce = true;
								throw new Error("Expected error");
							}
							return fn(a, b, c);
						};
					}
					let timeout = 1000;
					await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
						remote: { timeout }
					});
					expect(failedOnce).toBeTrue();
					expect(counters[0]).toEqual(1);
					expect(counters[1] + counters[2]).toEqual(1);
					expect(counters[1]).not.toEqual(counters[2]);
				});

				it("will fail silently if can not reach all shards", async () => {
					stores[0].docs.log.getReplicatorUnion = () => [
						stores[1].node.identity.publicKey.hashcode(),
						stores[2].node.identity.publicKey.hashcode()
					];
					for (let i = 1; i < stores.length; i++) {
						stores[i].docs.index.processFetchRequest = (a) => {
							throw new Error("Expected error");
						};
					}

					let timeout = 1000;

					await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
						remote: { timeout }
					});
					expect(counters[0]).toEqual(1);
					expect(counters[1]).toEqual(0);
					expect(counters[2]).toEqual(0);
				});
			});
		});
	});
	describe("recover", () => {
		@variant(0)
		class OtherDoc {
			@field({ type: "string" })
			id: string;

			constructor(properties: { id: string }) {
				this.id = properties.id;
			}
		}

		@variant("alternative_store")
		class AlternativeStore extends Program<Partial<SetupOptions<OtherDoc>>> {
			@field({ type: Uint8Array })
			id: Uint8Array;

			@field({ type: Documents })
			docs: Documents<OtherDoc>;

			constructor(properties: { docs: Documents<OtherDoc> }) {
				super();

				this.id = randomBytes(32);
				this.docs = properties.docs;
			}

			async open(options?: Partial<SetupOptions<OtherDoc>>): Promise<void> {
				await this.docs.open({
					...options,
					type: OtherDoc,
					index: { ...options?.index, key: "id" }
				});
			}
		}

		let session: TestSession;
		let db1: TestStore;
		let db2: AlternativeStore;

		beforeEach(async () => {
			session = await TestSession.connected(1);

			db1 = await session.peers[0].open(
				new TestStore({ docs: new Documents() })
			);

			db2 = await session.peers[0].open(
				new AlternativeStore({ docs: new Documents() })
			);
		});

		afterEach(async () => {
			if (db1) await db1.drop();
			if (db2) await db2.drop();

			await session.stop();
		});

		it("can recover from too strict acl", async () => {
			// We are createing two document store for this, because
			// we want blocks in our block store that will mess the recovery process

			let sharedId = uuid();

			await db2.docs.put(new OtherDoc({ id: sharedId }));
			await db2.docs.put(new OtherDoc({ id: uuid() }));
			await db2.docs.put(new OtherDoc({ id: uuid() }));

			await db1.docs.put(new Document({ id: sharedId }));
			await db1.docs.put(new Document({ id: uuid() }));
			await db1.docs.put(new Document({ id: uuid() }));

			await db2.docs.del(sharedId);

			expect(db1.docs.index.size).toEqual(3);
			await db1.close();
			let canPerform = false;
			db1 = await session.peers[0].open(db1.clone(), {
				args: { canPerform: () => canPerform }
			});
			expect(db1.docs.index.size).toEqual(0);
			await db1.docs.log.log.headsIndex["_index"].clear();
			canPerform = true;
			await db1.docs.put(new Document({ id: uuid() }));
			await db1.docs.log.log.headsIndex.resetHeadsCache();
			await db1.close();
			db1 = await session.peers[0].open(db1.clone());
			expect(db1.docs.index.size).toEqual(1); // heads are ruined
			await db1.docs.recover();
			expect(db1.docs.index.size).toEqual(4);

			// recovering multi0ple time should work
			await db1.close();
			db1 = await session.peers[0].open(db1.clone());

			await db1.docs.recover();
			expect(db1.docs.index.size).toEqual(4);

			// next time opening db I should not have to recover any more
			await db1.close();
			db1 = await session.peers[0].open(db1.clone());
			expect(db1.docs.index.size).toEqual(4);
		});
	});
});
