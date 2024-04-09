// @ts-nocheck
// ts ignore implicit any


import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec
} from "@dao-xyz/borsh";
import { Documents, type DocumentsChange, type SetupOptions } from "../src/program.js";
import {
	IntegerCompare,
	StringMatch,
	Compare,
	SearchRequest,
	StringMatchMethod,
	ByteMatchQuery,
	Sort,
	SortDirection,
	AbstractSearchResult,
	NoAccess,
	AbstractSearchRequest,
	Results,
	CloseIteratorRequest
} from "@peerbit/document-interface";
import { TestSession } from "@peerbit/test-utils";
import { Entry, Log } from "@peerbit/log";
import { AccessError, PublicSignKey, randomBytes } from "@peerbit/crypto";
import { v4 as uuid } from "uuid";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { Operation, PutOperation } from "../src/search.js";
import { Program } from "@peerbit/program";
import pDefer from "p-defer";
import { expect } from "chai";

import {
	AbsoluteReplicas,
	Observer,
	Replicator,
	decodeReplicas,
	encodeReplicas
} from "@peerbit/shared-log";
import { Ed25519PublicKey } from "@peerbit/crypto";
import { AcknowledgeDelivery, SilentDelivery } from "@peerbit/stream-interface";
import { DirectSub } from "@peerbit/pubsub";
import { TestStore, Document } from "./data.js";


describe("index", () => {
	let session: TestSession;

	describe("operations", () => {
		describe("basic", () => {
			let store: TestStore;
			let store2: TestStore;

			before(async () => {
				session = await TestSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
				await store2?.close();
			});

			after(async () => {
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
				expect(await store.docs.index.getSize()).equal(1);

				expect(changes.length).equal(1);
				expect(changes[0].added).to.have.length(1);

				// same reference
				expect(changes[0].added[0] === doc).to.be.true;
				expect(changes[0].added[0].id).equal(doc.id);
				expect(changes[0].removed).to.be.empty;

				const putOperation2 = (await store.docs.put(doc2)).entry;
				expect(await store.docs.index.getSize()).equal(2);
				expect(putOperation2.next).to.have.members([]); // because doc 2 is independent of doc 1

				expect(changes.length).equal(2);
				expect(changes[1].added).to.have.length(1);
				expect(changes[1].added[0].id).equal(doc2.id);
				expect(changes[1].removed).to.be.empty;

				// delete 1
				const deleteOperation = (await store.docs.del(doc.id)).entry;
				expect(deleteOperation.next).to.have.members([putOperation.hash]); // because delete is dependent on put
				expect(await store.docs.index.getSize()).equal(1);

				expect(changes.length).equal(3);
				expect(changes[2].added).to.be.empty;
				expect(changes[2].removed).to.have.length(1);
				expect(changes[2].removed[0].id).equal(doc.id);
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
				).equal(123);
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

				await store.docs.put(doc);
				expect(await store.docs.index.getSize()).equal(1);
				const putOperation2 = (await store.docs.put(editDoc)).entry;
				expect(await store.docs.index.getSize()).equal(1);
				expect(putOperation2.next).to.have.length(1);

				// delete 1
				const deleteOperation = (await store.docs.del(doc.id)).entry;
				expect(await store.docs.index.getSize()).equal(0);
				expect(
					(await store.docs.log.log.values.toArray()).map((x) => x.hash)
				).to.deep.equal([deleteOperation.hash]); // the delete operation
			});

			it("rejects on max message size", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false
					})
				});
				await session.peers[0].open(store);

				// not ok
				await expect(
					store.docs.put(
						new Document({
							id: uuid(),
							data: randomBytes(5e6)
						})
					)
				).rejectedWith(
					"Document is too large (5.00005) mb). Needs to be less than 5 mb"
				);

				// ok
				await store.docs.put(
					new Document({
						id: uuid(),
						data: randomBytes(5e6 - 100)
					})
				);
			});
		});

		describe("replication", () => {
			let store: TestStore, store2: TestStore, store3: TestStore;

			before(async () => {
				session = await TestSession.connected(3);
			});

			beforeEach(async () => {
				store = new TestStore({
					docs: new Documents<Document>()
				});
				await session.peers[0].open(store);
				store2 = await session.peers[1].open<TestStore>(store.clone(), {
					args: {
						role: {
							type: "replicator",
							factor: 1
						}
					}
				});
			});

			afterEach(async () => {
				await store?.close();
				await store2?.close();
				await store3?.close();
			});

			after(async () => {
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

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(COUNT)
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
				await waitForResolved(async () =>
					expect(await store3.docs.index.getSize()).equal(COUNT)
				);
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(0)
				);
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

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(COUNT)
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
				await waitForResolved(async () =>
					expect(await store3.docs.index.getSize()).equal(COUNT)
				);
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(0)
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

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(COUNT)
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
				await waitForResolved(async () =>
					expect(await store3.docs.index.getSize()).equal(COUNT)
				);
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(0)
				);
			});
		});

		describe("memory", () => {
			let store: TestStore;

			before(async () => {
				session = await TestSession.connected(1, {
					directory: "./tmp/document-store/drop-test/"
				});
			});

			afterEach(async () => {
				await store?.close();
			});

			after(async () => {
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
				expect(await store.docs.index.getSize()).equal(COUNT);
				await store.drop();
				store = await session.peers[0].open<TestStore>(
					deserialize(serialize(store), TestStore)
				);
				expect(await store.docs.index.getSize()).equal(0);
			});

			it("preserves tombstone", async () => {
				store = new TestStore({
					docs: new Documents<Document>()
				});

				await session.peers[0].open(store);

				const id = uuid();

				const { entry } = await store.docs.put(
					new Document({
						id,
						name: "Hello world"
					})
				);
				await store.docs.del(id);
				await store.close();

				store = await session.peers[0].open<TestStore>(store.address);
				await store.docs.log.log.join([entry]);
				expect(await store.docs.index.getSize()).equal(0);
			});
		});

		describe("events", () => {
			let stores: TestStore[];

			before(async () => {
				session = await TestSession.connected(3);
			});
			beforeEach(() => {
				stores = [];
			});
			afterEach(async () => {
				await stores.map((x) => x.close());
			});

			after(async () => {
				await session.stop();
			});

			it("emits event on replication", async () => {
				const store = new TestStore({
					docs: new Documents<Document>({
						immutable: false
					})
				});
				for (const [_i, peer] of session.peers.entries()) {
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
						resolver.get(doc.id)!();
					}
				});

				for (let i = 0; i < 100; i++) {
					const doc = new Document({ id: uuid() });
					const defer = pDefer();
					const timeout = setTimeout(() => {
						defer.reject(new Error("Timeout"));
					}, 10000);
					resolver.set(doc.id, () => {
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
			let store: Program & { docs: Documents<any> };
			let store2: Program;

			before(async () => {
				session = await TestSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
				await store2?.close();
			});

			after(async () => {
				await session.stop();
			});

			const testIndex = async (
				store: Program & { docs: Documents<any> },
				doc: any
			) => {
				await store.docs.put(doc);
				let result = await store.docs.index.get(doc.id);
				expect(result).to.exist;
				await store.docs.del(doc.id);
				expect(await store.docs.index.getSize()).equal(0);
				result = await store.docs.index.get(doc.id);
				expect(result).equal(undefined);
			};

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
							index: { idProperty: this.indexBy }
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
					).rejectedWith(
						"The provided key value is null or undefined, expecting string, number, bigint, or Uint8array"
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
					).equal(helloWorld);
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
					expect(results).to.have.length(1);
				});
			});

			describe("bytes", () => {
				class DocumentUin8arrayId {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: Uint8Array; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				@variant("test_uint8array_id_store")
				class TestUint8arrayIdStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<DocumentUin8arrayId>;

					constructor(properties: { docs: Documents<DocumentUin8arrayId> }) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async open(): Promise<void> {
						await this.docs.open({
							type: DocumentUin8arrayId
						});
					}
				}

				it("index as Uint8array", async () => {
					store = new TestUint8arrayIdStore({
						docs: new Documents<DocumentUin8arrayId>()
					});
					await session.peers[0].open(store);

					const id = new Uint8Array([1, 2, 3]);
					let doc = new DocumentUin8arrayId({
						id,
						value: "Hello world"
					});
					await testIndex(store, doc);
				});
			});
			describe("number", () => {
				class DocumentNumberId {
					@field({ type: "u32" })
					id: number;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: number; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				@variant("test_bigint_id_store")
				class TestNumberIdStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<DocumentNumberId>;

					constructor(properties: { docs: Documents<DocumentNumberId> }) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async open(): Promise<void> {
						await this.docs.open({
							type: DocumentNumberId
						});
					}
				}
				it("index as number", async () => {
					store = new TestNumberIdStore({
						docs: new Documents<DocumentNumberId>()
					});
					await session.peers[0].open(store);

					const id = 123456789;
					let doc = new DocumentNumberId({
						id,
						value: "Hello world"
					});

					await testIndex(store, doc);
				});
			});

			describe("transformed id", () => {
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
						readonly indexBy: string = "transformed_id"
					) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async open(): Promise<void> {
						await this.docs.open({
							type: SimpleDocument,
							id: (obj: any) => obj.id,
							index: {
								idProperty: this.indexBy,
								fields: (obj: any) => {
									return { transformed_id: obj.id + "transformed" };
								}
							}
						});
					}
				}
				it("index", async () => {
					store = new TestIndexStore({
						docs: new Documents<SimpleDocument>()
					});
					await session.peers[0].open(store);

					const id = "123";
					let doc = new SimpleDocument({
						id,
						value: "Hello world"
					});
					await testIndex(store, doc);
				});
			});

			describe("bigint", () => {
				class DocumentBigintId {
					@field({ type: "u64" })
					id: bigint;

					@field({ type: "string" })
					value: string;

					constructor(properties: { id: bigint; value: string }) {
						this.id = properties.id;
						this.value = properties.value;
					}
				}

				@variant("test_bigint_id_store")
				class TestBigintIdStore extends Program {
					@field({ type: Uint8Array })
					id: Uint8Array;

					@field({ type: Documents })
					docs: Documents<DocumentBigintId>;

					constructor(properties: { docs: Documents<DocumentBigintId> }) {
						super();

						this.id = randomBytes(32);
						this.docs = properties.docs;
					}
					async open(): Promise<void> {
						await this.docs.open({
							type: DocumentBigintId
						});
					}
				}

				it("index as bigint", async () => {
					store = new TestBigintIdStore({
						docs: new Documents<DocumentBigintId>()
					});
					await session.peers[0].open(store);

					const id = 123456789n;
					let doc = new DocumentBigintId({
						id,
						value: "Hello world"
					});
					await testIndex(store, doc);
				});
			});
		});

		describe("index", () => {
			let store: TestStore;
			let store2: TestStore;

			before(async () => {
				session = await TestSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
				await store2?.close();
			});

			after(async () => {
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
				expect(await store.docs.index.getSize()).equal(1);
				expect(changes.length).equal(1);
				expect(changes[0].added).to.have.length(1);
				expect(changes[0].added[0].id).equal(doc.id);
				expect(changes[0].removed).to.be.empty;

				// put doc again and make sure it still exist in index with trim to 1 option
				await store.docs.put(doc);
				expect(await store.docs.index.getSize()).equal(1);
				expect(store.docs.log.log.values.length).equal(1);
				expect(changes.length).equal(2);
				expect(changes[1].added).to.have.length(1);
				expect(changes[1].added[0].id).equal(doc.id);
				expect(changes[1].removed).to.be.empty;
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
							id: String(i),
							name: "Hello world " + String(i)
						}),
						{ meta: { next: [] } }
					);
				}

				expect(await store.docs.index.getSize()).equal(10);
				expect(store.docs.log.log.values.length).equal(10);
				expect(store.docs.log.log.headsIndex.index.size).equal(10);
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
								idProperty: "id",
								fields: (obj: any) => {
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

					let indexedValues = [...store.docs.index.engine.iterator()];

					expect(indexedValues).to.have.length(1);

					expect(indexedValues[0][1].indexed).to.deep.equal({
						[indexedNameField]: doc.name
					});
					expect(indexedValues[0][1].indexed["value"]).equal(undefined); // Because we dont want to keep it in memory (by default)

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

					expect(store2.docs.log.role).to.be.instanceOf(Observer);

					await store2.docs.log.waitForReplicator(
						session.peers[0].identity.publicKey
					);

					let results = await store2.docs.index.search(
						new SearchRequest({ query: [] })
					);
					expect(results).to.have.length(1);
				});
			});
		});

		describe("search", () => {
			describe("fields", () => {
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
				before(async () => {
					session = await TestSession.connected(peersCount);
				});

				after(async () => {
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
											? (obj: any, key: any) => {
												return canRead[i] ? canRead[i]!(obj, key) : true;
											}
											: undefined,
									canSearch:
										i === 0
											? (query: any, key: any) => {
												return canSearch[i]
													? canSearch[i]!(query, key)
													: true;
											}
											: undefined
								}
							}
						});
						stores.push(store);
					}

					writeStore = stores[0];

					let doc = new Document({
						id: "1",
						name: "hello",
						number: 1n
					});

					let docEdit = new Document({
						id: "1",
						name: "hello world",
						number: 1n,
						bool: true,
						data: new Uint8Array([1])
					});

					let doc2 = new Document({
						id: "2",
						name: "hello world",
						number: 4n
					});

					let doc2Edit = new Document({
						id: "2",
						name: "Hello World",
						number: 2n,
						data: new Uint8Array([2])
					});

					let doc3 = new Document({
						id: "3",
						name: "foo",
						number: 3n,
						data: new Uint8Array([3])
					});

					let doc4 = new Document({
						id: "4",
						name: undefined,
						number: undefined
					});

					await writeStore.docs.put(doc);
					await waitForResolved(async () =>
						expect(await writeStore.docs.index.getSize()).equal(1)
					);
					await writeStore.docs.put(docEdit);
					await writeStore.docs.put(doc2);
					await waitForResolved(async () =>
						expect(await writeStore.docs.index.getSize()).equal(2)
					);
					await writeStore.docs.put(doc2Edit);
					await writeStore.docs.put(doc3);
					await writeStore.docs.put(doc4);
					await waitForResolved(async () =>
						expect(await writeStore.docs.index.getSize()).equal(4)
					);

					expect(stores[0].docs.log.role).to.be.instanceOf(Replicator);
					expect(stores[1].docs.log.role).to.be.instanceOf(Observer);
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
					expect(results).to.have.length(4);
				});

				it("match locally", async () => {
					let results: Document[] = await stores[0].docs.index.search(
						new SearchRequest({
							query: []
						}),
						{ remote: false }
					);
					expect(results).to.have.length(4);
				});

				it("match all", async () => {
					let results: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: []
						}),
						{ remote: { amount: 1 } }
					);
					expect(results).to.have.length(4);
				});

				describe("sync", () => {
					it("can match sync", async () => {
						expect(await stores[1].docs.index.getSize()).equal(0);
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
						stores[1].docs["_optionCanPerform"] = async (props) => {
							canPerformEvents += 1;
							return !canPerform || canPerform(props);
						};

						await stores[1].docs.index.search(
							new SearchRequest({
								query: []
							}),
							{ remote: { amount: 1, sync: true } }
						);
						await waitForResolved(async () =>
							expect(await stores[1].docs.index.getSize()).equal(4)
						);
						expect(stores[1].docs.log.log.length).equal(6); // 4 documents where 2 have been edited once (4 + 2)
						expect(canPerformEvents).equal(6); // 4 documents where 2 have been edited once (4 + 2)
						expect(syncEvents).equal(1);

						await stores[1].docs.index.search(
							new SearchRequest({
								query: []
							}),
							{ remote: { amount: 1, sync: true } }
						);
						await waitFor(() => syncEvents == 2);
						expect(canPerformEvents).equal(6); // no new checks, since all docs already added
					});
					it("will persist synced entries through prunes", async () => {
						stores[0].docs.log.replicas = {
							min: new AbsoluteReplicas(1)
						};
						stores[1].docs.log.replicas = {
							min: new AbsoluteReplicas(1)
						};

						// add new doc, now wirth min replicas set to 1
						await stores[0].docs.put(new Document({ id: uuid() }));

						await stores[1].docs.updateRole({ type: "replicator", factor: 0 });
						expect(await stores[1].docs.index.getSize()).equal(0);
						await stores[1].docs.index.search(
							new SearchRequest({
								query: []
							}),
							{ remote: { sync: true } }
						);
						expect(await stores[1].docs.index.getSize()).equal(5);
						await stores[1].docs.log.distribute();
						await delay(2000); // wait some time so that pruningacn take place
						expect(await stores[1].docs.index.getSize()).equal(5);
					});

					it("removes sync cache when delete", async () => {
						await stores[1].docs.index.search(
							new SearchRequest({
								query: []
							}),
							{ remote: { sync: true } }
						);

						expect(stores[1].docs["_manuallySynced"].size).equal(4);
						for (const [k, _v] of stores[0].docs.index.engine.iterator()) {
							await stores[0].docs.del(k, { target: "all" });
						}
						await waitForResolved(
							() => expect(stores[1].docs["_manuallySynced"].size).equal(0),
							{
								timeout: 3e4
							}
						);
					});
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
							Partial<SetupOptions<NestedDocument>>
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
								options?: Partial<SetupOptions<NestedDocument>>
							): Promise<void> {
								await this.documents.open({
									...options,
									type: NestedDocument,
									index: { ...options?.index, idProperty: "id" },
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
								id: uuid(),
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
							expect(results.length).equal(1);
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
						expect(responses).to.be.empty;
						expect(allResponses).to.have.length(1);
						expect(allResponses[0]).to.be.instanceOf(Results);
						expect(canReadInvocation).to.have.length(4); // 4 documents in store
						expect(canReadInvocation[0][0]).to.be.instanceOf(Document);
						expect(canReadInvocation[0][1]).to.be.instanceOf(Ed25519PublicKey);
					});
				});

				describe("canSearch", () => {
					it("no search access will return an error response", async () => {
						const canSearchInvocations: [
							AbstractSearchRequest,
							PublicSignKey
						][] = [];
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
						expect(responses).to.be.empty;
						expect(allResponses).to.have.length(1);
						expect(allResponses[0]).to.be.instanceOf(NoAccess);
						expect(canSearchInvocations).to.have.length(1);
						expect(canSearchInvocations[0][0]).to.be.instanceOf(SearchRequest);
						expect(canSearchInvocations[0][1]).to.be.instanceOf(Ed25519PublicKey);
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
								expect(results[i]).to.have.length(2);
								results[i].sort((a, b) => Number(a.number! - b.number!));
								expect(results[i][0].number === 2n).to.be.true;
								expect(results[i][1].number === 3n).to.be.true;
							} else {
								// query2
								expect(results[i]).to.have.length(1);
								expect(results[i][0].number === 1n).to.be.true;
							}
						}
					});
				});
			});

			describe("limited", () => {
				let peersCount = 2;
				let writeStore: TestStore;
				let readStore: TestStore;
				before(async () => {
					session = await TestSession.connected(peersCount);
				});

				after(async () => {
					await session.stop();
				});

				beforeEach(async () => {
					writeStore = new TestStore({
						docs: new Documents<Document>()
					});
					await session.peers[0].open(writeStore, {
						args: {
							role: {
								type: "replicator",
								factor: 1
							}
						}
					});
					readStore = await session.peers[1].open<TestStore>(
						writeStore.address,
						{
							args: {
								role: "observer"
							}
						}
					);
				});
				afterEach(async () => {
					await writeStore.close();
					await readStore.close();
				});

				it("can handle large document limits", async () => {
					for (let i = 0; i < 10; i++) {
						const doc = new Document({
							id: String(i),
							data: randomBytes(5e6 - 100)
						});
						await writeStore.docs.put(doc);
					}
					await readStore.docs.log.waitForReplicator(
						session.peers[0].identity.publicKey
					);
					const collected = await readStore.docs.index.search(
						new SearchRequest()
					);
					expect(collected).to.have.length(10);
				});
			});

			describe("redundancy", () => {
				let peersCount = 3;
				before(async () => {
					session = await TestSession.connected(peersCount);
				});

				after(async () => {
					await session.stop();
				});

				it("can search while keeping minimum amount of replicas", async () => {
					const store = new TestStore({
						docs: new Documents<Document>()
					});
					const store1 = await session.peers[0].open(store.clone(), {
						args: {
							role: {
								type: "replicator",
								factor: 0.111
							},
							replicas: {
								min: 1
							}
						}
					});

					const store2 = await session.peers[1].open(store.clone(), {
						args: {
							role: {
								type: "replicator",
								factor: 0.1
							},
							replicas: {
								min: 1
							}
						}
					});

					const store3 = await session.peers[2].open(store.clone(), {
						args: {
							role: {
								type: "replicator",
								factor: 0.2
							},
							replicas: {
								min: 1
							}
						}
					});

					await waitForResolved(() =>
						expect(
							store1.docs.log.getReplicatorsSorted()?.toArray().length
						).equal(3)
					);

					const count = 1000;

					for (let i = 0; i < count; i++) {
						const doc = new Document({
							id: uuid(),
							data: randomBytes(10)
						});
						await store1.docs.put(doc);
					}
					let lastLength = -1;

					// search while it is distributing/syncing
					for (let i = 0; i < 10; i++) {
						if (store1.docs.log.log.length === lastLength) {
							break;
						}
						lastLength = store1.docs.log.log.length;
						for (const store of [store1, store2, store3]) {
							let t0 = +new Date();
							const collected = await store.docs.index.search(
								new SearchRequest()
							);
							try {
								expect(collected.length).equal(count);
							} catch (error) {
								throw new Error(
									`Failed to collect all messages ${collected} < ${count}. Log lengths:  ${JSON.stringify([store1, store2, store3].map((x) => x.docs.log.log.length))}. Matured: ${store.docs.log
										.getReplicatorsSorted()
										?.toArray()
										.map(
											(x: any) =>
												Number(x.role.timestamp) - t0 >
												store.docs.log.timeUntilRoleMaturity
										)}`
								);
							}
						}
						await delay(100);
					}
				});
			});

			describe("concurrency", () => {
				before(async () => { });

				let abortController: AbortController,
					interval: ReturnType<typeof setInterval>;
				afterEach(() => {
					clearTimeout(interval);
					abortController.abort();
				});

				after(async () => {
					await session.stop();
				});

				it("query during sync load", async () => {
					session = await TestSession.disconnected(3, {
						libp2p: {
							services: {
								pubsub: (c) =>
									new DirectSub(c, {
										connectionManager: { dialer: false, pruner: false }
									}) // prevent autodialing
							}
						}
					});

					const writeStore = await session.peers[0].open(
						new TestStore({
							docs: new Documents<Document>()
						}),
						{
							args: {
								role: {
									type: "replicator",
									factor: 1
								},
								timeUntilRoleMaturity: 1000
							}
						}
					);

					for (let i = 0; i < session.peers.length - 1; i++) {
						await session.connect([[session.peers[i], session.peers[i + 1]]]);
					}
					const readStore = await session.peers[
						session.peers.length - 1
					].open<TestStore>(writeStore.address, {
						args: {
							role: {
								type: "replicator",
								factor: 1
							},
							timeUntilRoleMaturity: 1000
						}
					});

					await waitForResolved(() =>
						expect(writeStore.docs.log.getReplicatorsSorted()?.length).equal(
							2
						)
					);
					await waitForResolved(() =>
						expect(readStore.docs.log.getReplicatorsSorted()?.length).equal(2)
					);

					// introduce lag in the relay
					let lag = 500;
					const rawOutboundStream = (session.peers[1].services.pubsub as any)[
						"peers"
					].get(
						session.peers[2].identity.publicKey.hashcode()
					)!.rawOutboundStream;

					const sendFn = rawOutboundStream.sendData.bind(rawOutboundStream);
					abortController = new AbortController();
					rawOutboundStream.sendData = async (data: any) => {
						await delay(lag, { signal: abortController.signal });
						return sendFn(data);
					};

					// start insertion rapidly
					const ids: string[] = [];

					// Omit sending entries directly and rely on the sync mechanism instead
					// We do this so we know for sure trhat reader will query writer (reader needs to know they are "missing out" on something
					// and the sync protocol have this info)
					writeStore.docs.log.append = async (a: any, b: any) => {
						b = {
							...b,
							meta: {
								...b?.meta,
								data: encodeReplicas(new AbsoluteReplicas(1))
							}
						};
						return writeStore.docs.log.log.append(a, b);
					};

					const outboundStream = (session.peers[1].services.pubsub as any)["peers"].get(
						session.peers[2].identity.publicKey.hashcode()
					)!.outboundStream;

					let msgSize = 1e4;

					const insertFn = async () => {
						const id = uuid();
						ids.push(id);
						await writeStore.docs.put(
							new Document({ id, data: randomBytes(msgSize) })
						);
						await writeStore.docs.log.distribute();
						interval = setTimeout(() => insertFn(), lag / 2);
					};
					insertFn();

					await waitForResolved(() =>
						expect(outboundStream.readableLength).greaterThan(msgSize * 5)
					);
					await waitForResolved(() =>
						expect(readStore.docs.log["syncInFlight"].size).greaterThan(0)
					);

					// try two searches, one default (should work anyway)
					// and now less prioritized, should fail because clogging
					const prioritizedSearchByDefault = readStore.docs.index.search(
						new SearchRequest({
							query: new StringMatch({
								key: ["id"],
								value: ids[ids.length - 1]
							})
						}),
						{
							remote: {
								mode: AcknowledgeDelivery,
								throwOnMissing: true,
								timeout: 5e3
							}
						}
					);

					await expect(
						readStore.docs.index.search(
							new SearchRequest({
								query: new StringMatch({
									key: ["id"],
									value: ids[ids.length - 1]
								})
							}),
							{
								remote: {
									mode: AcknowledgeDelivery,
									throwOnMissing: true,
									priority: 0,
									timeout: 5e3
								}
							} // query will low prio and see that we reach an error
						)
					).rejectedWith("Did not receive responses from all shards");

					expect(await prioritizedSearchByDefault).to.have.length(1);
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
					id: String(id),
					name: String(id),
					number: BigInt(id)
				});
				const resp = await stores[storeIndex].docs.put(doc);
				// --- wait for all others to "want" this entry ---

				await stores[storeIndex].docs.log.distribute(); //  we need to call this to make other peers know that they are missing out of this hashes
				// because we have overriding the append to not send entries right away

				for (let i = 0; i < stores.length; i++) {
					if (i === storeIndex) {
						continue;
					}
					// when blow is true, we will be "forced" to query the other node for the data.
					// this allows use to test sorting where data is determenstically distributed
					// i.e put(1,123) will put a document at store 1 with id 123, and will never leave that store
					// store 0 and 2 who want to fetch all data will always have to ask node 1
					await waitForResolved(() =>
						expect(
							stores[i].docs.log["syncInFlight"]
								.get(stores[storeIndex].node.identity.publicKey.hashcode())
								.has(resp.entry.hash)
						).to.be.true
					);
				}
				return resp;
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
						expect(iterator.done()).to.be.false;
					} else {
						for (const batch of batches) {
							expect(iterator.done()).to.be.false;
							const next = await iterator.next(batch.length);
							expect(next.map((x) => x.number)).to.deep.equal(batch);
						}
						expect(iterator.done()).to.be.true;
					}
				});
			};

			before(async () => {
				session = await TestSession.connected(peersCount);
			});

			after(async () => {
				await session.stop();
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
					store.docs.log.append = async (a: any, b: any) => {
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
								canRead: (_document: any, key: any) => {
									return canRead[i] ? canRead[i]!(key) : true;
								}
							},
							role: {
								// TODO choose offset so data is perfectly distributed
								type: "replicator",
								factor: 1
							},
							timeUntilRoleMaturity: 0,
							replicas: { min: 1 } // make sure documents only exist once
						}
					});

					// Omit synchronization so results are always the same (HACKY)
					// TODO types
					const onMessage = store.docs.log.rpc["_responseHandler"];
					store.docs.log.rpc["_responseHandler"] = (msg: any, ctx: any) => {
						if (msg.constructor.name === "ExchangeHeadsMessage") {
							return;
						}
						return onMessage(msg, ctx);
					};

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
				expect(await stores[0].docs.index.getSize()).equal(3);
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
				await stores[1].docs.log.log.join([e2.entry]);
				await put(1, 3);
				await put(1, 4);
				for (let i = 1; i < session.peers.length; i++) {
					await checkIterate(i, [[0n, 1n, 2n, 3n, 4n]]);
					await checkIterate(i, [[0n], [1n, 2n, 3n, 4n]]);
				}
			});

			it("observer mixed sort", async () => {
				// TODO separate setup so we don't need to close store 2 test here
				await stores[2].close();
				await stores[0].docs.updateRole({ type: "observer" });
				await waitForResolved(() =>
					expect(
						stores[0].docs.log.getReplicatorsSorted()?.toArray().length
					).equal(1)
				);
				let data: number[] = [];
				for (let i = 0; i < 100; i++) {
					let doc = new Document({
						id: String(i),
						name: String(i),
						number: BigInt(i)
					});
					data.push(i);
					const { entry } = await stores[1].docs.put(doc, { target: "all" });
					if (i > 30) await stores[0].docs.log.log.join([entry]); // only join some entries to the observer
				}

				const req = new SearchRequest({
					query: [
						new IntegerCompare({
							key: "number",
							compare: Compare.GreaterOrEqual,
							value: 0n
						})
					],
					sort: [new Sort({ direction: SortDirection.ASC, key: "number" })]
				});
				const iterator = stores[0].docs.index.iterate(req);
				let acc: Document[] = [];
				while (iterator.done() === false) {
					const v = await iterator.next(20);
					acc = [...acc, ...v];
				}
				expect(acc.map((x) => Number(x.number))).to.deep.equal(data);
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
					expect(iterator.done()).to.be.false;
					const next = await iterator.next(3);
					expect(next.map((x) => x.name)).to.deep.equal(["0", "1", "2"]);
					expect(iterator.done()).to.be.true;
				}
				{
					const iterator = await stores[0].docs.index.iterate(
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.DESC, key: "name" })]
						})
					);
					expect(iterator.done()).to.be.false;
					const next = await iterator.next(3);
					expect(next.map((x) => x.name)).to.deep.equal(["2", "1", "0"]);
					expect(iterator.done()).to.be.true;
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
				expect(iterator.done()).to.be.false;
				const next = await iterator.next(3);
				expect(next.map((x) => x.name)).to.deep.equal(["0", "1", "2"]);
				expect(iterator.done()).to.be.true;
			});

			it("uses indexed fields", async () => {
				const KEY = "ABC";
				await stores[0].docs.index.open({
					fields: async (obj) => {
						return { [KEY]: obj.number };
					},
					indexBy: ["id"],
					dbType: Documents,
					canSearch: () => true,
					log: stores[0].docs.log,
					sync: () => undefined as any,
					type: Document
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
				expect(next.map((x) => x.name)).to.deep.equal(["2", "1", "0"]);
				expect(iterator.done()).to.be.true;
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
				expect((await iterator.next(1)).map((x) => x.name)).to.deep.equal(["1"]);
				expect(iterator.done()).to.be.false;
				expect((await iterator.next(1)).map((x) => x.name)).to.deep.equal(["2"]);
				expect(iterator.done()).to.be.true;
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
					expect(iterator.done()).to.be.false;
					await iterator.next(2); // fetch some, but not all
					expect(
						(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
							request.idString
						)!.arr
					).to.have.length(1);
					await iterator.close();
					await waitForResolved(
						() =>
							expect(
								(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
									request.idString
								)
							).equal(undefined),
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
					expect(iterator.done()).to.be.false;
					await iterator.next(1); // fetch some, but not all
					expect(
						(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
							request.idString
						)!.arr
					).to.have.length(1);

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
						(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
							request.idString
						)
					).to.exist;

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
								(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
									request.idString
								)
							).equal(undefined),
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
					expect(iterator.done()).to.be.false;
					await iterator.next(3); // fetch some, but not all
					await waitForResolved(
						() =>
							expect(
								(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
									request.idString
								)
							).equal(undefined),
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
					expect(iterator.done()).to.be.true;
					await waitForResolved(
						() =>
							expect(
								(stores[0].docs.index.engine as any)["_resultsCollectQueue"].get(
									request.idString
								)
							).equal(undefined),
						{ timeout: 3000, delayInterval: 50 }
					);
				});
			});

			// TODO test iterator.close() to stop pending promises

			// TODO deletion while sort

			// TODO session timeouts?
		});
	});

	describe("acl", () => {
		let store: TestStore;
		before(async () => {
			session = await TestSession.connected(1);
		});

		after(async () => {
			await session.stop();
		});

		afterEach(async () => {
			await store?.close();
		});



		it("reject entries with unexpected payloads", async () => {
			store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>()
				})
			);
			await expect(
				store.docs.log.log.append(new PutOperation({ data: randomBytes(32) }))
			).rejectedWith(AccessError);
		});

		it("reject entries with unexpected payloads", async () => {
			store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>()
				})
			);

			(store as any)["_canAppend"] = () => true; // ignore internal

			const canAppend = await store.docs.canAppend(
				(await Entry.create({
					data: new PutOperation({ data: randomBytes(32) }),
					identity: store.node.identity,
					store: store.docs.log.log.blocks,
					canAppend: () => true,
					encoding: store.docs.log.log.encoding
				})) as Entry<Operation>
			);

			await expect(canAppend).to.be.false;
		});

		describe("canPerform", () => {


			before(async () => {
				await session.stop();
				session = await TestSession.connected(1, {
					directory: "./tmp/document-store/acl/"
				});
			});
			after(async () => {
				await session.stop();
			});

			afterEach(async () => {
				await store?.close();
			});
			@variant("test_can_perform")
			class TestCanPerfom extends Program {
				@field({ type: Documents })
				documents: Documents<Document>;

				constructor() {
					super();
					this.documents = new Documents<Document>();
				}

				open(args?: any): Promise<void> {
					return this.documents.open({
						type: Document,
						canPerform: () => {
							return true;
						}
					});
				}
			}

			it("can use different acl on load", async () => {



				let store = await session.peers[0].open(new TestCanPerfom());
				await store.documents.put(
					new Document({ id: "1", name: "1", number: 1n })
				);
				await store.close();

				store = await session.peers[0].open(store.clone());
				expect(await store.documents.index.getSize()).equal(1);
			});
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
						idProperty: ["id"]
					}
				});
			}
		}

		let stores: { store: TestStoreSubPrograms }[];
		let peersCount = 2;

		before(async () => {
			session = await TestSession.connected(peersCount);
		});
		after(async () => {
			await session.stop();
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



		it("can open a subprogram when put", async () => {
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.false;
		});

		it("can put after open", async () => {
			const subProgram = new SubProgram();
			await session.peers[0].open(subProgram);
			await stores[0].store.docs.put(subProgram.clone());
			expect(subProgram.closed).to.be.false;
		});

		it("can open after put", async () => {
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			await session.peers[0].open(subProgram, { existing: "reuse" });
			expect(subProgram.closed).to.be.false;
		});

		it("will close subprogram after put", async () => {
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.false;
			expect(stores[0].store.closed).to.be.false;
			expect(stores[0].store.docs.closed).to.be.false;

			await stores[0].store.close();
			expect(stores[0].store.closed).to.be.true;
			expect(stores[0].store.docs.closed).to.be.true;
			expect(subProgram.closed).to.be.true;
		});
		it("will not close subprogram that is opened before put", async () => {
			const subProgram = new SubProgram();
			await session.peers[0].open(subProgram);
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.false;
			await stores[0].store.close();
			expect(subProgram.closed).to.be.false;
			await subProgram.close();
			expect(subProgram.closed).to.be.true;
		});

		it("non-replicator will not open by default", async () => {
			const subProgram = new SubProgram();
			await stores[1].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.true;
		});

		it("can open program when sync", async () => {
			const subProgram = new SubProgram();
			await stores[1].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.true; // Because observer? Not open by default?
			await stores[0].store.docs.log.log.join(
				[...(await stores[1].store.docs.log.log.values.toArray()).values()].map(
					(x) => deserialize(serialize(x), Entry)
				)
			);
			expect(subProgram.closed).to.be.true; // Because observer? Not open by default?

		});

		it("will drop on delete", async () => {
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.false;

			let dropped = false;
			const subprogramDropped = subProgram.drop.bind(subProgram);
			subProgram.drop = (from) => {
				dropped = true;
				return subprogramDropped(from);
			};
			await stores[0].store.docs.del(subProgram.id);
			await waitForResolved(() => expect(subProgram.closed).to.be.true);
			expect(dropped).to.be.true;
		});

		it("can prevent subprograms to be opened", async () => {
			stores[0].store.docs.canOpen = (_) => Promise.resolve(false);
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			subProgram.parents = [undefined];
			expect(subProgram.closed).to.be.true;
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
							idProperty: ["id"],
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
				await store.docs.put(subProgram);
				expect(subProgram.closed).to.be.false;
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(1)
				);
				const stores = [store, store2];
				for (const s of stores) {
					const results = await s.docs.index.search(
						new SearchRequest({
							query: [
								new ByteMatchQuery({ key: "custom", value: subProgram.id })
							]
						})
					);
					expect(results).to.have.length(1);
					expect(results[0].id).to.deep.equal(subProgram.id);
					expect(results[0].closed).to.be.false;
				}
			});
		});
	});

	describe("query distribution", () => {
		describe("distribution", () => {
			let peersCount = 3,
				stores: TestStore[] = [];
			let counters: Array<number> = [];

			before(async () => {
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
					const fn = stores[i].docs.index.processQuery.bind(
						stores[i].docs.index
					);
					stores[i].docs.index.processQuery = (a, b, c) => {
						counters[i] += 1;
						return fn(a, b, c);
					};
					await stores[i].docs.waitFor(
						session.peers.filter((_v, ix) => ix !== i).map((x) => x.peerId)
					);
				}
			});
			after(async () => {
				await Promise.all(stores.map((x) => x.drop()));
				await session.stop();
			});
			beforeEach(() => {
				counters = new Array(stores.length).fill(0);
			});





			it("all", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[1].node.identity.publicKey.hashcode(),
					stores[2].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(1);
				expect(counters[2]).equal(1);
			});

			it("will always query locally", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [] as any;
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(0);
				expect(counters[2]).equal(0);
			});

			it("one", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[1].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(1);
				expect(counters[2]).equal(0);
			});

			it("non-local", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[1].node.identity.publicKey.hashcode(),
					stores[2].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
					local: false
				});
				expect(counters[0]).equal(0);
				expect(counters[1]).equal(1);
				expect(counters[2]).equal(1);
			});


			describe("errors", () => {
				let fns: any[];

				beforeEach(() => {
					fns = stores.map((x) => x.docs.index.processQuery.bind(x.docs.index));
				});

				afterEach(() => {
					stores.forEach((x, ix) => {
						x.docs.index.processQuery = fns[ix];
					});
				});

				it("will iterate on shard until response", async () => {
					stores[0].docs.log.getReplicatorUnion = () => [
						stores[1].node.identity.publicKey.hashcode(),
						stores[2].node.identity.publicKey.hashcode()
					];

					let failedOnce = false;
					for (let i = 1; i < stores.length; i++) {
						const fn = stores[i].docs.index.processQuery.bind(
							stores[1].docs.index
						);
						stores[i].docs.index.processQuery = (a, b, c) => {
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
					expect(failedOnce).to.be.true;
					expect(counters[0]).equal(1);
					expect(counters[1] + counters[2]).equal(1);
					expect(counters[1]).not.equal(counters[2]);
				});

				it("will fail silently if can not reach all shards", async () => {
					stores[0].docs.log.getReplicatorUnion = () => [
						stores[1].node.identity.publicKey.hashcode(),
						stores[2].node.identity.publicKey.hashcode()
					];
					for (let i = 1; i < stores.length; i++) {
						stores[i].docs.index.processQuery = (a) => {
							throw new Error("Expected error");
						};
					}

					let timeout = 1000;

					await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
						remote: { timeout }
					});
					expect(counters[0]).equal(1);
					expect(counters[1]).equal(0);
					expect(counters[2]).equal(0);
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
					index: { ...options?.index, idProperty: "id" }
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

			expect(await db1.docs.index.getSize()).equal(3);
			await db1.close();
			let canPerform = false;
			db1 = await session.peers[0].open(db1.clone(), {
				args: {
					canPerform: () => canPerform
				}
			});
			expect(await db1.docs.index.getSize()).equal(0);
			await db1.docs.log.log.headsIndex["_index"].clear();
			canPerform = true;
			await db1.docs.put(new Document({ id: uuid() }));
			await db1.docs.log.log.headsIndex.resetHeadsCache();
			await db1.close();
			db1 = await session.peers[0].open(db1.clone());
			expect(await db1.docs.index.getSize()).equal(1); // heads are ruined
			await db1.docs.recover();
			expect(await db1.docs.index.getSize()).equal(4);

			// recovering multiple time should work
			await db1.close();
			db1 = await session.peers[0].open(db1.clone());
			expect(await db1.docs.index.getSize()).equal(4);
			await db1.docs.recover();
			expect(await db1.docs.index.getSize()).equal(4);

			// next time opening db I should not have to recover any more
			await db1.close();
			db1 = await session.peers[0].open(db1.clone());
			expect(await db1.docs.index.getSize()).equal(4);
		});
	});
});
/*  TODO query all if undefined?
		
		it("query all if undefined", async () => {
			stores[0].docs.log["_replication"].replicators = () => undefined;
			await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
				remote: { amount: 2 },
			});
			expect(counters[0]).equal(1);
			expect(counters[1]).equal(1);
			expect(counters[2]).equal(1);
		}); */

/*  TODO getReplicatorUnion to provide query alternatives
		
			it("ignore shard if I am replicator", async () => {
				stores[0].docs.log.getReplicatorUnion = () => [
					stores[0].node.identity.publicKey.hashcode(),
					stores[1].node.identity.publicKey.hashcode()
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(0);
				expect(counters[2]).equal(0);
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
	await waitForResolved(() => expect(stores[0].docs.log.getReplicatorsSorted()?.toArray().map(x => x.publicKey.hashcode())).to.have.members([session.peers[1].identity.publicKey.hashcode(), session.peers[2].identity.publicKey.hashcode()]));
	
	const t1 = +new Date();
	const minAge = 1000;
	await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
		remote: { minAge }
	});
	expect(counters[0]).equal(1); // will always query locally
	expect(counters[1]).equal(1); // but now also remotely since we can not trust local only
	expect(counters[2]).equal(0);
	await waitFor(() => +new Date() - t1 > minAge + 100);
	
	await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
		remote: { minAge }
	});
	expect(counters[0]).equal(2); // will always query locally
	expect(counters[1]).equal(1); // we don't have to query remote since local will suffice since minAge time has passed
	expect(counters[2]).equal(0);
}); */
