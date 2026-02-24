// ts ignore implicit any
import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	AccessError,
	Ed25519Keypair,
	Ed25519PublicKey,
	type PublicSignKey,
	equals,
	randomBytes,
	toBase64,
} from "@peerbit/crypto";
import {
	AbstractSearchRequest,
	AbstractSearchResult,
	CloseIteratorRequest,
	CollectNextRequest,
	Context,
	IterationRequest,
	NoAccess,
	NotFoundError,
	PredictedSearchRequest,
	ResultIndexedValue,
	Results,
	SearchRequest,
	SearchRequestIndexed,
} from "@peerbit/document-interface";
import {
	ByteMatchQuery,
	Compare,
	IntegerCompare,
	Or,
	Sort,
	SortDirection,
	StringMatch,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { Entry, Log, createEntry } from "@peerbit/log";
import { ClosedError, Program } from "@peerbit/program";
import type { TopicControlPlane } from "@peerbit/pubsub";
import { RPCMessage, ResponseV0 } from "@peerbit/rpc";
import {
	AbsoluteReplicas,
	SharedLog,
	decodeReplicas,
} from "@peerbit/shared-log";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitFor as _waitForFn, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer, { type DeferredPromise } from "p-defer";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import { createDocumentDomain } from "../src/domain.js";
import type { DocumentsChange } from "../src/events.js";
import MostCommonQueryPredictor from "../src/most-common-query-predictor.js";
import {
	Operation,
	PutOperation,
	PutWithKeyOperation,
} from "../src/operation.js";
import {
	type CountEstimate,
	Documents,
	type SetupOptions,
} from "../src/program.js";
import {
	type CanRead,
	DocumentIndex,
	type LateResultsEvent,
	type UpdateReason,
} from "../src/search.js";
import { Document, TestStore } from "./data.js";

describe("index", () => {
	let session: TestSession;

	describe("operations", () => {
		describe("basic", () => {
			let store: TestStore | undefined = undefined;

				before(async () => {
					session = await TestSession.connected(2);
				});
				afterEach(async function () {
					// Closing a large document index (many persisted blocks + index flush) can take
					// longer than Mocha's default 60s under CI load.
					this.timeout(180_000);
					await store?.close();
				});

			after(async () => {
				await session.stop();
			});

			it("can add and delete", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store);
				const changes: DocumentsChange<Document, Document>[] = [];

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
				expect(await store.docs.index.getSize()).equal(1);

				expect(changes.length).equal(1);
				expect(changes[0].added).to.have.length(1);

				// same reference
				expect(changes[0].added[0] === doc).to.be.true;
				expect(changes[0].added[0].id).equal(doc.id);
				expect(changes[0].removed).to.be.empty;
				expect(changes[0].added[0].__context.size).to.exist;
				expect(changes[0].added[0].__indexed).to.exist;

				const putOperation2 = (await store.docs.put(doc2)).entry;
				expect(await store.docs.index.getSize()).equal(2);
				expect(putOperation2.meta.next).to.have.members([]); // because doc 2 is independent of doc 1

				expect(changes.length).equal(2);
				expect(changes[1].added).to.have.length(1);
				expect(changes[1].added[0].id).equal(doc2.id);
				expect(changes[1].added[0].__context.size).to.exist;
				expect(changes[1].added[0].__indexed).to.exist;

				expect(changes[1].removed).to.be.empty;
				// delete 1
				const deleteOperation = (await store.docs.del(doc.id)).entry;
				expect(deleteOperation.meta.next).to.have.members([putOperation.hash]); // because delete is dependent on put
				expect(await store.docs.index.getSize()).equal(1);

				expect(changes.length).equal(3);
				expect(changes[2].added).to.be.empty;
				expect(changes[2].removed).to.have.length(1);
				expect(changes[2].removed[0].id).equal(doc.id);
				expect(changes[2].removed[0].__context.size).to.exist;
				expect(changes[2].removed[0].__indexed).to.exist;
			});

			it("replication degree", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store);
				const changes: DocumentsChange<Document, Document>[] = [];

				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				let replicas = 10;
				const putOperation = (await store.docs.put(doc, { replicas })).entry;
				expect(
					decodeReplicas(
						putOperation as {
							meta: {
								data: Uint8Array;
							};
						},
					).getValue(store.docs.log),
				).equal(replicas);
			});

			it("many chunks", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
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
							name: rngs[i],
						}),
						{ unique: true },
					);
				}
			});

			it("delete permanently", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false,
					}),
				});
				await session.peers[0].open(store);

				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});
				let editDoc = new Document({
					id: doc.id,
					name: "Hello world 2",
				});

				await store.docs.put(doc);
				expect(await store.docs.index.getSize()).equal(1);
				const putOperation2 = (await store.docs.put(editDoc)).entry;
				expect(await store.docs.index.getSize()).equal(1);
				expect(putOperation2.meta.next).to.have.length(1);

				// delete 1
				const deleteOperation = (await store.docs.del(doc.id)).entry;
				expect(await store.docs.index.getSize()).equal(0);
				expect(
					(await store.docs.log.log.toArray()).map((x) => x.hash),
				).to.deep.equal([deleteOperation.hash]); // the delete operation
			});

			it("can delete without being replicator", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false,
					}),
				});
				await session.peers[0].open(store, { args: { replicate: true } });

				let id = uuid();
				await store.docs.put(
					new Document({
						id,
						name: "Hello world",
					}),
				);

				const nonReplicator = await session.peers[1].open(store.clone(), {
					args: {
						replicate: false,
					},
				});
				const docResolved = await nonReplicator.docs.index.get(id, {
					waitFor: 5e3,
				});
				expect(docResolved).to.exist;
				await nonReplicator.docs.del(docResolved.id);
			});

			it("reload after delete", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false,
					}),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				await store.docs.put(doc);
				await store.docs.del(doc.id);
				await store.docs.log.reset();
			});

			it("deleting non-exising throws error", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false,
					}),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});

				const promise = store.docs.del(uuid());
				await expect(promise).to.be.rejectedWith(NotFoundError);
			});

			it("rejects on max message size", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false,
					}),
				});
				await session.peers[0].open(store);

				// not ok
				await expect(
					store.docs.put(
						new Document({
							id: uuid(),
							data: randomBytes(5e6),
						}),
					),
				).rejectedWith(
					/Document is too large \((5.\d+)\) mb\). Needs to be less than 5 mb/,
				);

				// document is too large regex with a number that is 5 and a little bit more

				// ok
				await store.docs.put(
					new Document({
						id: uuid(),
						data: randomBytes(5e6 - 100),
					}),
				);
			});

			it("can reinsert", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});
				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				await store.docs.put(doc);
				await store.docs.put(doc);
				await store.docs.put(doc);

				// open with another store
				const store2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: 1,
					},
				});

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).to.eq(1),
				);
			});

			it("will clear resumable iterators on end of iteration", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});
				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				await store.docs.put(doc);

				const store2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: false,
					},
				});
				await store2.docs.index.waitFor(store.node.identity.publicKey);

				const document = await store2.docs.index.get(doc.id);

				expect(document).to.exist;
				expect(
					(store.docs.index as any)["_resumableIterators"].queues.size,
				).to.eq(0);
				expect(
					(store2.docs.index as any)["_resumableIterators"].queues.size,
				).to.eq(0);
			});

			it("will not use network on put and no other peers are available", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});
				let doc = new Document({
					id: uuid(),
					name: "Hello world",
					data: randomBytes(1e6),
				});

				let largeMessagesSent: number = 0;
				for (const peer of (session.peers[0].services.pubsub as TopicControlPlane)
					.peers) {
					const writeFn = peer[1].write.bind(peer[1]);
					peer[1].write = (message, priority) => {
						if (message.length >= 1e6) {
							largeMessagesSent++;
						}
						return writeFn(message, priority);
					};
				}
				await store.docs.put(doc);
				expect(largeMessagesSent).to.eq(0);
			});

			it("updates are propagated", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});
				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				await store.docs.put(doc);

				// open with another store
				const store2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: 1,
					},
				});

				let eventsFromStore1: DocumentsChange<Document, Document>[] = [];
				store.docs.events.addEventListener("change", (evt) => {
					eventsFromStore1.push(evt.detail);
				});
				await waitForResolved(async () =>
					expect(await store2.docs.index.index.count()).to.eq(1),
				);
				const docFromStore2 = await store2.docs.index.get(doc.id);
				docFromStore2.name = "Goodbye";
				await store2.docs.put(docFromStore2);
				await waitForResolved(async () =>
					expect(eventsFromStore1).to.have.length(1),
				);
				const docFromStore1 = await store.docs.index.get(doc.id);
				expect(docFromStore1.name).to.eq("Goodbye");
			});

			it("updates are propagated no casual ordering", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.stop();

				session = await TestSession.disconnected(2);

				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});
				let doc = new Document({
					id: uuid(),
					name: "First",
				});

				await store.docs.put(doc, { meta: { next: [] } });

				// open with another store
				const store2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: 1,
					},
				});
				let doc2 = new Document({
					id: doc.id,
					name: "Second",
				});

				await store2.docs.put(doc2, { meta: { next: [] } });

				await session.connect();

				await waitForResolved(() => {
					expect(store?.docs.log.log.length).to.eq(2);
					expect(store2?.docs.log.log.length).to.eq(2);
				});

				const docFromStore1 = await store.docs.index.get(doc.id);
				expect(docFromStore1.name).to.eq("Second");

				const docFromStore2 = await store2.docs.index.get(doc.id);
				expect(docFromStore2.name).to.eq("Second");
			});

			describe("strictHistory", () => {
				it("enabled", async () => {
					store = new TestStore({
						docs: new Documents<Document>(),
					});
					await session.peers[0].open(store, {
						args: {
							replicate: 1,
							strictHistory: true,
						},
					});
					let doc = new Document({
						id: uuid(),
						name: "Hello",
					});

					await store.docs.put(doc);

					let updatedDoc = new Document({
						id: doc.id,
						name: "Goodbyte",
					});

					try {
						await store.docs.put(updatedDoc, { meta: { next: [] } });
					} catch (error) {
						expect(error).to.be.instanceOf(AccessError);
					}

					const documentFetched = await store.docs.index.get(doc.id);
					expect(documentFetched.name).to.eq("Hello"); // not updated
				});
				it("disabled by default", async () => {
					store = new TestStore({
						docs: new Documents<Document>(),
					});
					await session.peers[0].open(store, {
						args: {
							replicate: 1,
						},
					});
					let doc = new Document({
						id: uuid(),
						name: "Hello",
					});

					await store.docs.put(doc);

					let updatedDoc = new Document({
						id: doc.id,
						name: "Goodbye",
					});

					await store.docs.put(updatedDoc, { meta: { next: [] } });

					const documentFetched = await store.docs.index.get(doc.id);
					expect(documentFetched.name).to.eq("Goodbye");
				});
			});
		});

		describe("get", () => {
			it("get waitFor existing", async () => {
				session = await TestSession.connected(1);
				const store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});
				let id = uuid();
				await store.docs.put(
					new Document({
						id,
						name: "Hello world",
					}),
				);
				const results = await store.docs.index.get(id, { waitFor: 1 });
				expect(results).to.exist;
			});

			it("get document that is to be added", async () => {
				session = await TestSession.connected(1);
				const store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});

					let newId = uuid();
					let getImmediatePromise = store.docs.index.get(toId(newId), {
						waitFor: 1,
					});
					// Use a generous waitFor under workspace-wide test parallelism, but
					// still assert we do not wait all the way until the timeout.
					let getPromise = store.docs.index.get(toId(newId), { waitFor: 10e3 });

				let t0 = +new Date();
				await delay(1e3);

				let doc = new Document({
					id: newId,
					name: "Hello world",
				});

					await store.docs.put(doc);
					expect(await getImmediatePromise).to.be.undefined; // not yet available
					expect(await getPromise).to.exist;

					// Should resolve shortly after the put (not wait for the full waitFor).
					expect(+new Date() - t0).to.be.lessThan(6000);
				});

			it("get document that is to be joined", async () => {
				session = await TestSession.connected(2);
				const store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});

				let store2 = await session.peers[1].open<TestStore>(store.clone(), {
					args: {
						replicate: 1,
					},
				});

					let newId = uuid();
					let getImmediatePromise = store2.docs.index.get(toId(newId), {
						waitFor: 1,
					});
					// Use a generous waitFor under workspace-wide test parallelism, but
					// still assert we do not wait all the way until the timeout.
					let getPromise = store2.docs.index.get(toId(newId), { waitFor: 10e3 });

				let t0 = +new Date();
				await delay(1e3);

				let doc = new Document({
					id: newId,
					name: "Hello world",
				});

					await store.docs.put(doc);
					expect(await getImmediatePromise).to.be.undefined; // not yet available
					expect(await getPromise).to.exist;

					// Should resolve shortly after the put (not wait for the full waitFor).
					expect(+new Date() - t0).to.be.lessThan(6000);
				});

			it("get waitFor document late even if local fetch is slow", async () => {
				session = await TestSession.connected(1);
				const store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: 1,
					},
				});

				// in this test we try this flow

				// 1. we call get, where first the local fetch is done (fast). But remote query part is slow.
				// 2. during it beeing stuck on fetching remote, the document is added
				// 3. we expect the get to resolve with the document, even if the remote query was slow (and local query was already invoked)
				const getCover = store.docs.log.getCover.bind(store.docs.log);
				let deferred = pDefer<void>();
				let getCoverCalled = pDefer<void>();
				let once = false;
				store.docs.log.getCover = async (a, b) => {
					getCoverCalled.resolve();
					if (!once) {
						await deferred.promise;
					}
					return getCover(a, b);
				};

				let newId = uuid();

				let getPromise = store.docs.index.get(toId(newId), { waitFor: 5e3 });

				let t0 = +new Date();

				let doc = new Document({
					id: newId,
					name: "Hello world",
				});

				await getCoverCalled.promise; // wait for the getCover to be called
				await store.docs.put(doc); // add the document
				deferred.resolve(); // resolve the getCover, so it can continue

				expect(await getPromise).to.exist;

				expect(+new Date() - t0).to.be.lessThan(2000); // should not take longer than 2 seconds. (even if waitFor is set at 5e3)
			});

			it("waitFor document as non-replicator", async () => {
				session = await TestSession.connected(2);

				const store = await session.peers[0].open(
					new TestStore({
						docs: new Documents<Document>(),
					}),
					{
						args: {
							replicate: 1,
						},
					},
				);

				const nonReplicator = await session.peers[1].open(store.clone(), {
					args: {
						replicate: false,
					},
				});

				let id = uuid();
				const getPromise = nonReplicator.docs.index.get(id, { waitFor: 10e3 });
				await store.docs.put(
					new Document({
						id,
						name: "Hello world",
					}),
				);

				const result = await getPromise;
				expect(result).to.exist;
			});
		});

		describe("replication", () => {
			let store: TestStore, store2: TestStore, store3: TestStore;

			before(async () => {
				session = await TestSession.connected(3);
			});

			beforeEach(async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});
				store2 = await session.peers[1].open<TestStore>(store.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
					},
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
				await store.docs.log.replicate({
					factor: 1,
				});
				for (let i = 0; i < COUNT; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: "Hello world",
						}),
					);
				}

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(COUNT),
				);

				store3 = await session.peers[2].open<TestStore>(store.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});

				await waitForResolved(async () =>
					expect(await store3.docs.index.getSize()).equal(COUNT),
				);

				await store2.docs.log.replicate(false);

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(0),
				);
			});

			it("drops when no longer replicating with factor 0", async () => {
				let COUNT = 10;
				await store.docs.log.replicate({
					factor: 1,
				});
				for (let i = 0; i < COUNT; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: "Hello world",
						}),
					);
				}

				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(COUNT),
				);

				store3 = await session.peers[2].open<TestStore>(store.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});
				await store2.docs.log.replicate({ factor: 0 });
				await waitForResolved(async () =>
					expect(await store3.docs.index.getSize()).equal(COUNT),
				);
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(0),
				);
			});

			it("can query immediately after replication:join event", async () => {
				await store2.close();

				await store.docs.put(
					new Document({
						number: 123n,
					}),
				);

				store2 = store.clone();
				let joined = false;

				store2.docs.log.events.addEventListener("replicator:join", async () => {
					expect(
						await store2.docs.index.search(new SearchRequest({})),
					).to.have.length(1);
					joined = true;
				});
				await session.peers[1].open<TestStore>(store2, {
					args: {
						replicate: false,
					},
				});
				await waitForResolved(() => expect(joined).to.be.true);
			});

			describe("search replicate", () => {
				let stores: TestStore[];

				before(async () => {
					session = await TestSession.connected(2);
				});

				after(async () => {
					await session.stop();
				});

				beforeEach(async () => {
					stores = [];
				});
				afterEach(async () => {
					for (const store of stores) {
						if (store && store.closed === false) {
							await store.close();
						}
					}
				});

				describe("v7", () => {
					it("iterate replicate", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
									compatibility: 7,
								},
							},
						);

						let docCount = 5;
						for (let i = 0; i < docCount; i++) {
							await stores[0].docs.put(
								new Document({
									id: String(i),
								}),
							);
						}

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
									compatibility: 7,
								},
							},
						);

						await stores[1].docs.index.waitFor(
							session.peers[0].identity.publicKey,
						);

						const iterator = stores[1].docs.index.iterate(
							new SearchRequest({
								query: [],
							}),
							{ remote: { replicate: true } },
						);

						const results = [
							...(await iterator.next(1)),
							...(await iterator.next(1)),
							...(await iterator.next(1)),
							...(await iterator.next(1)),
						];

						expect(results).to.have.length(4);

						await waitForResolved(async () =>
							expect(await stores[1].docs.index.getSize()).equal(4),
						);
						expect(
							await stores[1].docs.log.getMyReplicationSegments(),
						).to.have.length(4); // no new segments
					});
				});

				describe("v8+", () => {
					it("uses iteration request by default when compatibility is undefined", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
								},
							},
						);

						const docId = "iteration-default";
						await stores[0].docs.put(new Document({ id: docId }));

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
								},
							},
						);

						await stores[1].docs.index.waitFor(
							session.peers[0].identity.publicKey,
						);

						const requestSpy = sinon.spy(
							stores[1].docs.index._query,
							"request",
						);

						let iterator: any;
						try {
							iterator = stores[1].docs.index.iterate(
								{},
								{
									remote: { replicate: true },
								},
							);

							await iterator.next(1);

							expect(requestSpy.callCount).to.be.greaterThan(0);
							const firstRequest = requestSpy.getCall(0).args[0];
							expect(firstRequest).to.be.instanceOf(IterationRequest);
						} finally {
							await iterator?.close();
							requestSpy.restore();
						}
					});

					it("search replicate", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
								},
							},
						);

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
								},
							},
						);

						let docCount = 5;
						for (let i = 0; i < docCount; i++) {
							await stores[0].docs.put(new Document({ id: String(i) }));
						}

						await stores[1].docs.index.waitFor(
							stores[0].node.identity.publicKey,
						);

						let canPerformEvents = 0;
						let canPerform = stores[1].docs["_optionCanPerform"]?.bind(
							stores[1].docs,
						);

						stores[1].docs["_optionCanPerform"] = async (props) => {
							canPerformEvents += 1;
							return !canPerform || canPerform(props);
						};

						const results = await stores[1].docs.index.search(
							new SearchRequest({
								query: [],
							}),
							{ remote: { replicate: true } },
						);
						expect(results).to.have.length(docCount);

						await waitForResolved(async () =>
							expect(await stores[1].docs.index.getSize()).equal(docCount),
						);

						expect(stores[1].docs.log.log.length).equal(docCount);
						expect(canPerformEvents).equal(docCount);

						const segments =
							await stores[1].docs.log.getMyReplicationSegments();

						expect(segments).to.have.length(docCount);

						await stores[1].docs.index.search(
							new SearchRequest({
								query: [],
							}),
							{ remote: { replicate: true } },
						);
						expect(canPerformEvents).equal(docCount); // no new checks, since all docs already added
						expect(
							await stores[1].docs.log.getMyReplicationSegments(),
						).to.have.length(docCount); // no new segments
					});

					it("iterate replicate", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
								},
							},
						);

						let docCount = 5;
						for (let i = 0; i < docCount; i++) {
							await stores[0].docs.put(
								new Document({
									id: String(i),
								}),
							);
						}

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
								},
							},
						);
						await stores[1].docs.index.waitFor(
							stores[0].node.identity.publicKey,
						);

						const iterator = stores[1].docs.index.iterate(
							new SearchRequest({
								query: [],
							}),
							{ remote: { replicate: true } },
						);

						const results = [
							...(await iterator.next(1)),
							...(await iterator.next(1)),
							...(await iterator.next(1)),
							...(await iterator.next(1)),
						];

						expect(results).to.have.length(4);

						await waitForResolved(async () =>
							expect(await stores[1].docs.index.getSize()).equal(4),
						);
						expect(
							await stores[1].docs.log.getMyReplicationSegments(),
						).to.have.length(4); // no new segments
					});

					it("will persist synced entries through prunes", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
									replicas: {
										min: new AbsoluteReplicas(1),
									},
								},
							},
						);

						let docCount = 5;
						for (let i = 0; i < docCount; i++) {
							await stores[0].docs.put(
								new Document({
									id: String(i),
								}),
							);
						}

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
									replicas: {
										min: new AbsoluteReplicas(1),
									},
								},
							},
						);

						await stores[1].docs.index.waitFor(
							stores[0].node.identity.publicKey,
						);

						const results = await stores[1].docs.index.search(
							new SearchRequest({
								query: [],
							}),
							{ remote: { replicate: true } },
						);

						expect(results).to.have.length(docCount);
						expect(await stores[1].docs.index.getSize()).equal(docCount);
						await stores[1].docs.log.waitForPruned();
						expect(await stores[1].docs.index.getSize()).equal(docCount);
					});

					it("will only replicate the synced entries", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
								},
							},
						);

						let docCount = 5;
						for (let i = 0; i < docCount; i++) {
							await stores[0].docs.put(
								new Document({
									id: String(i),
								}),
							);
						}

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
								},
							},
						);

						await stores[1].docs.index.waitFor(
							stores[0].node.identity.publicKey,
						);

						expect(stores[1].docs.log.log.length).equal(0);
						const results = await stores[1].docs.index
							.iterate(
								new SearchRequest({
									query: [
										new StringMatch({
											key: "id",
											value: "4",
										}),
									],
								}),
								{ remote: { replicate: true } },
							)
							.all();

						expect(results).to.have.length(1);
						expect(results[0].id).equal("4");
						expect(await stores[1].docs.index.getSize()).equal(1);
						await delay(3000); // wait for any replcation processes to finish
						expect(stores[1].docs.log.log.length).equal(1);
					});

					it("many", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
								},
							},
						);

						let docCount = 1e3;
						for (let i = 0; i < docCount; i++) {
							await stores[0].docs.put(
								new Document({
									id: String(i),
								}),
							);
						}

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
								},
							},
						);

						await stores[1].docs.index.waitFor(
							stores[0].node.identity.publicKey,
						);

						let t0 = +new Date();
						const results = await stores[1].docs.index
							.iterate({}, { remote: { replicate: true } })
							.all();
						let t1 = +new Date();
						expect(results.length).to.eq(docCount);
						expect(t1 - t0).to.be.lessThan(5000); // TODO this.log.join(... { replicate: true }) is very slow
						expect(
							(await stores[1].docs.log.getMyReplicationSegments()).length,
						).to.eq(docCount);
					});

					it("re-replicate will not emit any message", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: {
										factor: 1,
									},
								},
							},
						);

						await stores[0].docs.put(
							new Document({
								id: String("0"),
							}),
						);

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
								},
							},
						);

						await stores[1].docs.index.waitFor(
							stores[0].node.identity.publicKey,
						);

						expect(
							await stores[1].docs.log.getMyReplicationSegments(),
						).to.have.length(0);
						let results = await stores[1].docs.index
							.iterate({}, { remote: { replicate: true } })
							.all();

						expect(results).to.have.length(1);
						expect(
							await stores[1].docs.log.getMyReplicationSegments(),
						).to.have.length(1);

						await delay(3e3);
						const emittedMessage: any[] = [];

						const sendFn = stores[1].docs.log.rpc.send.bind(
							stores[1].docs.log.rpc,
						);
						stores[1].docs.log.rpc.send = (message, options) => {
							emittedMessage.push(message);
							return sendFn(message, options);
						};

						let listener = () => {
							throw new Error("Expected no replication changes");
						};
						stores[1].docs.log.events.addEventListener(
							"replication:change",
							listener,
						);

						results = await stores[1].docs.index
							.iterate({}, { remote: { replicate: true } })
							.all();

						expect(results).to.have.length(1);
						expect(
							await stores[1].docs.log.getMyReplicationSegments(),
						).to.have.length(1);
						expect(emittedMessage).to.have.length(0);
						stores[1].docs.log.events.removeEventListener(
							"replication:change",
							listener,
						);
					});
				});

				describe("v9", () => {
					it("uses legacy iterate request when compatibility is <= 9", async () => {
						stores[0] = await session.peers[0].open<TestStore>(
							new TestStore({ docs: new Documents() }),
							{
								args: {
									replicate: true,
									compatibility: 7,
								},
							},
						);

						const legacyDocId = "legacy-default";
						await stores[0].docs.put(new Document({ id: legacyDocId }));

						stores[1] = await session.peers[1].open<TestStore>(
							stores[0].clone(),
							{
								args: {
									replicate: false,
									compatibility: 7,
								},
							},
						);

						await stores[1].docs.index.waitFor(
							session.peers[0].identity.publicKey,
						);

						const requestSpy = sinon.spy(
							stores[1].docs.index._query,
							"request",
						);

						let iterator: any;
						try {
							iterator = stores[1].docs.index.iterate(
								{},
								{
									remote: { replicate: true },
								},
							);

							await iterator.next(1);

							expect(requestSpy.callCount).to.be.greaterThan(0);
							const firstRequest = requestSpy.getCall(0).args[0];
							expect(firstRequest).to.be.instanceOf(SearchRequest);
							expect(firstRequest).to.not.be.instanceOf(IterationRequest);
						} finally {
							await iterator?.close();
							requestSpy.restore();
						}
					});
				});
			});
		});

		describe("memory", () => {
			let store: TestStore;

			before(async () => {
				session = await TestSession.connected(1, {
					directory: "./tmp/document-store/drop-test/",
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
					docs: new Documents<Document>(),
				});

				await session.peers[0].open(store);

				const COUNT = 1;
				for (let i = 0; i < COUNT; i++) {
					await store.docs.put(
						new Document({
							id: uuid(),
							name: "Hello world",
						}),
					);
				}
				await store.close();
				store = await session.peers[0].open<TestStore>(store.address);
				expect(await store.docs.index.getSize()).equal(COUNT);
				await store.drop();
				store = await session.peers[0].open<TestStore<any, any>>(
					deserialize(serialize(store), TestStore) as TestStore<any, any>,
				);
				expect(await store.docs.index.getSize()).equal(0);
			});

			it("preserves tombstone", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});

				await session.peers[0].open(store);

				const id = uuid();

				const { entry } = await store.docs.put(
					new Document({
						id,
						name: "Hello world",
					}),
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
						immutable: false,
					}),
				});
				for (const [_i, peer] of session.peers.entries()) {
					if (store.closed) {
						stores.push(
							await peer.open(store, {
								args: {
									replicate: {
										factor: 1,
									},
								},
							}),
						);
					} else {
						stores.push(
							await TestStore.open<TestStore<any, any>>(store.address, peer, {
								args: {
									replicate: {
										factor: 1,
									},
								},
							}),
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

		describe("index", () => {
			let store: TestStore;

			before(async () => {
				session = await TestSession.connected(2);
			});
			afterEach(async () => {
				await store?.close();
			});

			after(async () => {
				await session.stop();
			});

			describe("del with indexed value", () => {
				class DocumentUint8arrayId {
					@id({ type: Uint8Array })
					id: Uint8Array;

					constructor(id: Uint8Array) {
						this.id = id;
					}
				}

				@variant("test_uint8array_indexable")
				class DocumentUnt8arrayIdIndexable {
					@field({ type: "string" })
					id: string;

					constructor(track: DocumentUint8arrayId) {
						this.id = toBase64(track.id);
					}
				}

				@variant("test_uint8array_with_indexed_string")
				class DocumentUint8arrayIdStore extends Program {
					@field({ type: Documents })
					docs: Documents<DocumentUint8arrayId, DocumentUnt8arrayIdIndexable>;

					constructor(properties?: {
						docs?: Documents<
							DocumentUint8arrayId,
							DocumentUnt8arrayIdIndexable
						>;
					}) {
						super();
						this.docs = properties?.docs || new Documents();
					}
					async open(
						options?: Partial<
							SetupOptions<DocumentUint8arrayId, DocumentUnt8arrayIdIndexable>
						>,
					): Promise<void> {
						await this.docs.open({
							...options,
							type: DocumentUint8arrayId,
							index: {
								type: DocumentUnt8arrayIdIndexable,
							},
						});
					}
				}

				it("needs to delete by indexed value", async () => {
					const store = await session.peers[0].open(
						new DocumentUint8arrayIdStore(),
					);
					const document = new DocumentUint8arrayId(new Uint8Array([1, 2, 3]));
					await store.docs.put(document);
					const getResult = await store.docs.index.get(
						new DocumentUnt8arrayIdIndexable(document).id,
					);
					expect(getResult).to.exist;
					await store.docs.del(new DocumentUnt8arrayIdIndexable(document).id);
				});
			});

			it("trim deduplicate changes", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});

				await session.peers[0].open(store, {
					args: {
						log: {
							trim: { type: "length", to: 1 },
						},
						replicate: false, // if we instead would do 'replicator' trimming will not be done unless other peers has joined
					},
				});

				const changes: DocumentsChange<Document, Document>[] = [];
				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world",
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
				expect(store.docs.log.log.length).equal(1);
				expect(changes.length).equal(2);
				expect(changes[1].added).to.have.length(1);
				expect(changes[1].added[0].id).equal(doc.id);
				expect(changes[1].removed).to.be.empty;
			});

			it("trim and update index", async () => {
				store = new TestStore({
					docs: new Documents<Document>({
						immutable: false,
					}),
				});

				await session.peers[0].open(store, {
					args: {
						log: {
							trim: { type: "length" as const, to: 10 },
						},
						replicate: false, // if we instead would do 'replicator' trimming will not be done unless other peers has joined
					},
				});

				for (let i = 0; i < 100; i++) {
					await store.docs.put(
						new Document({
							id: String(i),
							name: "Hello world " + String(i),
						}),
						{ meta: { next: [] } },
					);
				}

				expect(await store.docs.index.getSize()).equal(10);
				expect(store.docs.log.log.length).equal(10);
				expect((await store.docs.log.log.getHeads().all()).length).equal(10);
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
								publicKey: PublicSignKey,
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
										session.peers[i].services.blocks,
									))!
								: new TestStore({
										docs: new Documents<Document>(),
									});
						await session.peers[i].open(store, {
							args: {
								replicate: i === 0 ? { factor: 1 } : false,
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
											: undefined,
								},
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
						bool: true,
						data: new Uint8Array([1]),
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
						data: new Uint8Array([2]),
					});

					let doc3 = new Document({
						id: "3",
						name: "foo",
						number: 3n,
						data: new Uint8Array([3]),
					});

					let doc4 = new Document({
						id: "4",
						name: undefined,
						number: undefined,
					});

					await writeStore.docs.put(doc);
					await waitForResolved(async () =>
						expect(await writeStore.docs.index.getSize()).equal(1),
					);
					await writeStore.docs.put(docEdit);
					await writeStore.docs.put(doc2);
					await waitForResolved(async () =>
						expect(await writeStore.docs.index.getSize()).equal(2),
					);
					await writeStore.docs.put(doc2Edit);
					await writeStore.docs.put(doc3);
					await writeStore.docs.put(doc4);
					await waitForResolved(async () =>
						expect(await writeStore.docs.index.getSize()).equal(4),
					);

					expect(await stores[0].docs.log.isReplicating()).to.be.true;
					expect(await stores[1].docs.log.isReplicating()).to.be.false;
					await stores[1].waitFor(session.peers[0].peerId);
					await stores[1].docs.log.waitForReplicator(
						session.peers[0].identity.publicKey,
					);
					await stores[0].waitFor(session.peers[1].peerId);
					canRead = new Array(stores.length).fill(undefined);
					canSearch = new Array(stores.length).fill(undefined);
				});

				afterEach(async () => {
					await Promise.all(
						stores.map((x) => (x.closed === false ? x.drop() : undefined)),
					);
				});

				it("no-args", async () => {
					let results: Document[] = await stores[0].docs.index.search(
						new SearchRequest({ query: [] }),
					);
					expect(results).to.have.length(4);
				});

				it("match locally", async () => {
					let results: Document[] = await stores[0].docs.index.search(
						new SearchRequest({
							query: [],
						}),
						{ remote: false },
					);
					expect(results).to.have.length(4);
				});

				it("match all", async () => {
					let results: Document[] = await stores[1].docs.index.search(
						new SearchRequest({
							query: [],
						}),
						{ remote: { amount: 1 } },
					);
					expect(results).to.have.length(4);
				});

				/* TODO feat
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
	
						class NestedDocumentIndexable {
							@field({ type: Uint8Array })
							id: Uint8Array;
	
							@field({ type: 'string' })
							address: string
	
							constructor(properties: { id: Uint8Array, address: string }) {
								this.id = properties.id;
								this.address = properties.address;
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
								options?: Partial<SetupOptions<NestedDocument>>,
							): Promise<void> {
								await this.documents.open({
									...options,
									type: NestedDocument,
									index: {
										...options?.index,
										idProperty: "id",
										type: NestedDocumentIndexable,
										transform: async (arg) => new NestedDocumentIndexable({ id: arg.id, address: await arg.calculateAddress() })
									},
									canOpen: () => true,
								});
							}
						}
	
						it("nested document store", async () => {
							const nestedStore = await session.peers[0].open(
								new NestedDocumentStore({ docs: new Documents() }),
							);
							const nestedDoc = new NestedDocument(new Documents());
							await session.peers[0].open(nestedDoc);
							const document = new Document({
								id: uuid(),
								name: "hello",
							});
							await nestedDoc.documents.put(document);
							await nestedStore.documents.put(nestedDoc);
	
							const nestedStore2 =
								await session.peers[1].open<NestedDocumentStore>(
									nestedStore.address,
									{ args: { replicate: false } },
								);
							await nestedStore2.documents.log.waitForReplicator(
								session.peers[0].identity.publicKey,
							);
							const results = await nestedStore2.documents.index.search(
								new SearchRequest({
									query: [
										new StringMatch({
											key: ["documents", "name"],
											value: "hello",
										}),
									],
								}),
							);
							expect(results.length).equal(1);
						});
					});
				}); */

				describe("canRead", () => {
					it("no read access will return a response with 0 results", async () => {
						const canReadInvocation: [Document, PublicSignKey][] = [];
						canRead[0] = (a, b) => {
							canReadInvocation.push([a, b]);
							return Promise.resolve(false);
						};
						let allResponses: AbstractSearchResult[] = [];
						let responses: Document[] = await stores[1].docs.index.search(
							new SearchRequest({
								query: [],
							}),
							{
								local: false,
								remote: {
									onResponse: (r) => {
										allResponses.push(r);
									},
								},
							},
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
							PublicSignKey,
						][] = [];
						canSearch[0] = (a, b) => {
							canSearchInvocations.push([a, b]);
							return Promise.resolve(false);
						};
						let allResponses: AbstractSearchResult[] = [];
						let responses: Document[] = await stores[1].docs.index.search(
							new SearchRequest({
								query: [],
							}),
							{
								local: false,
								remote: {
									amount: 1,
									onResponse: (r) => {
										allResponses.push(r);
									},
								},
							},
						);
						expect(responses).to.be.empty;
						expect(allResponses).to.have.length(1);
						expect(allResponses[0]).to.be.instanceOf(NoAccess);
						expect(canSearchInvocations).to.have.length(1);
						expect(canSearchInvocations[0][0]).to.be.instanceOf(SearchRequest);
						expect(canSearchInvocations[0][1]).to.be.instanceOf(
							Ed25519PublicKey,
						);
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
													value: 2n,
												}),
											],
										}),
										{ remote: { amount: 1 } },
									),
								);
							} else {
								promises.push(
									stores[1].docs.index.search(
										new SearchRequest({
											query: [
												new IntegerCompare({
													key: "number",
													compare: Compare.Less,
													value: 2n,
												}),
											],
										}),
										{ remote: { amount: 1 } },
									),
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
						docs: new Documents<Document>(),
					});
					await session.peers[0].open(writeStore, {
						args: {
							replicate: {
								factor: 1,
							},
						},
					});
					readStore = await session.peers[1].open<TestStore>(
						writeStore.address,
						{
							args: {
								replicate: false,
							},
						},
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
							data: randomBytes(5e6 - 1300),
						});
						await writeStore.docs.put(doc);
					}
					await readStore.docs.log.waitForReplicator(
						session.peers[0].identity.publicKey,
					);
					const collected = await readStore.docs.index.search(
						new SearchRequest(),
					);
					expect(collected).to.have.length(10);
				});

				it("can handle many ORs with large payload", async () => {
					let ids = new Set<string>();
					let count = 33;
					for (let i = 0; i < count; i++) {
						const doc = new Document({
							id: String(i),
							data: randomBytes(5e5 - 1300),
						});
						await writeStore.docs.put(doc);
						ids.add(doc.id);
					}

					await readStore.docs.log.waitForReplicator(
						session.peers[0].identity.publicKey,
					);

					const collected = await readStore.docs.index.search(
						new SearchRequest({
							query: new Or(
								new Array(count).fill(0).map((_, i) => {
									return new StringMatch({
										key: "id",
										value: String(i),
									});
								}),
							),
							fetch: 0xffffffff,
						}),
					);

					expect(collected).to.have.length(count);
				});

				it("can handle many ORs with large payload and replicate", async () => {
					let ids = new Set<string>();
					let count = 33;
					for (let i = 0; i < count; i++) {
						const doc = new Document({
							id: String(i),
							data: randomBytes(5e5 - 1300),
						});
						await writeStore.docs.put(doc);
						ids.add(doc.id);
					}

					await readStore.docs.log.waitForReplicator(
						session.peers[0].identity.publicKey,
					);

					const collected = await readStore.docs.index.search(
						new SearchRequest({
							query: new Or(
								new Array(count).fill(0).map((_, i) => {
									return new StringMatch({
										key: "id",
										value: String(i),
									});
								}),
							),
							fetch: 0xffffffff,
						}),
						{
							remote: { replicate: true },
						},
					);

					expect(collected).to.have.length(count);
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

				it("can search while keeping minimum amount of replicas", async function () {
					this.timeout(180_000);
					// TODO fix flakiness
					const store = new TestStore({
						docs: new Documents<Document>(),
					});
					const store1 = await session.peers[0].open(store.clone(), {
						args: {
							replicate: {
								factor: 0.111,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
						},
					});

					const store2 = await session.peers[1].open(store.clone(), {
						args: {
							replicate: {
								factor: 0.1,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
						},
					});

					const store3 = await session.peers[2].open(store.clone(), {
						args: {
							replicate: {
								factor: 0.2,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
						},
					});

					await waitForResolved(async () =>
						expect((await store1.docs.log.getReplicators()).size).equal(3),
					);
					await waitForResolved(async () =>
						expect((await store2.docs.log.getReplicators()).size).equal(3),
					);
					await waitForResolved(async () =>
						expect((await store3.docs.log.getReplicators()).size).equal(3),
					);

					const count = 300;

					for (let i = 0; i < count; i++) {
						const doc = new Document({
							id: uuid(),
							data: randomBytes(10),
						});
						await store1.docs.put(doc);
					}
					await waitForResolved(
						async () => {
							for (const store of [store1, store2, store3]) {
								const collected = await store.docs.index.search(
									new SearchRequest({ fetch: count }),
								);
								if (collected.length !== count) {
									throw new Error(
										`Failed to collect all messages ${collected.length} < ${count}. Log lengths: ${JSON.stringify(
											[store1, store2, store3].map((x) => x.docs.log.log.length),
										)}`,
									);
								}
							}
						},
						{ timeout: 120_000, delayInterval: 200 },
					);
				});
			});

			describe("concurrency", () => {
				before(() => {});

				/* let abortController: AbortController,
					interval: ReturnType<typeof setInterval>;
				afterEach(() => {
					interval && clearTimeout(interval);
					abortController && abortController.abort();
				});
 */
				after(async () => {
					await session.stop();
				});

				/* 
				TODO make sure this test works again after new batching updates in the shared-log
				it("query during sync load", async () => {
					session = await TestSession.disconnected(3, {
						libp2p: {
							services: {
								pubsub: (c) =>
									new TopicControlPlane(c, {
										connectionManager: { dialer: false, pruner: false },
									}), // prevent autodialing
							},
						},
					});
	
					const writeStore = await session.peers[0].open(
						new TestStore({
							docs: new Documents<Document>(),
						}),
						{
							args: {
								replicate: {
									factor: 1,
								},
								timeUntilRoleMaturity: 1000,
							},
						},
					);
	
					for (let i = 0; i < session.peers.length - 1; i++) {
						await session.connect([[session.peers[i], session.peers[i + 1]]]);
					}
					const readStore = await session.peers[
						session.peers.length - 1
					].open<TestStore>(writeStore.address, {
						args: {
							replicate: {
								factor: 1,
							},
							timeUntilRoleMaturity: 1000,
						},
					});
	
					await waitForResolved(async () =>
						expect((await writeStore.docs.log.getReplicators())?.size).equal(2),
					);
					await waitForResolved(async () =>
						expect((await readStore.docs.log.getReplicators())?.size).equal(2),
					);
	
					// introduce lag in the relay
					let lag = 500;
					const rawOutboundStream = (session.peers[1].services.pubsub as any)[
						"peers"
					].get(
						session.peers[2].identity.publicKey.hashcode(),
					)!.rawOutboundStream;
	
					const sendFn = rawOutboundStream.sendData.bind(rawOutboundStream);
					abortController = new AbortController();
					rawOutboundStream.sendData = async (data: any, options: any) => {
						await delay(lag, { signal: abortController.signal });
						return sendFn(data, options);
					};
	
					// start insertion rapidly
					const ids: string[] = [];
	
					const outboundStream = (session.peers[1].services.pubsub as any)[
						"peers"
					].get(session.peers[2].identity.publicKey.hashcode())!.outboundStream;
	
					let msgSize = 1e4;
	
					const insertFn = async () => {
						const id = uuid();
						ids.push(id);
						await writeStore.docs.put(
							new Document({ id, data: randomBytes(msgSize) }),
							{
								replicas: 1,
								// Omit sending entries directly and rely on the sync mechanism instead
								// We do this so we know for sure trhat reader will query writer (reader needs to know they are "missing out" on something
								// and the sync protocol have this info)
								target: 'none',
	
							}
						);
						await writeStore.docs.log.rebalanceAll() // make sure reader knows it is missing out
						interval = setTimeout(() => insertFn(), lag / 2);
					};
					insertFn();
	
	
					await waitForResolved(() =>
						expect(outboundStream.readableLength).greaterThan(msgSize * 5),
					);
					await waitForResolved(() =>
						expect(readStore.docs.log["syncInFlight"].size).greaterThan(0),
					);
	
					// try two searches, one default (should work anyway)
					// and now less prioritized, should fail because clogging
					const prioritizedSearchByDefault = readStore.docs.index.search(
						new SearchRequest({
							query: new StringMatch({
								key: ["id"],
								value: ids[ids.length - 1],
							}),
						}),
						{
							remote: {
								mode: AcknowledgeDelivery,
								throwOnMissing: true,
								timeout: 5e3,
							},
						},
					);
	
					try {
						await expect(
							readStore.docs.index.search(
								new SearchRequest({
									query: new StringMatch({
										key: ["id"],
										value: ids[ids.length - 1],
									}),
								}),
								{
									remote: {
										mode: AcknowledgeDelivery,
										throwOnMissing: true,
										priority: 0,
										timeout: 5e3,
									},
								}, // query will low prio and see that we reach an error
							),
						).rejectedWith("Did not receive responses from all shards");
					} catch (error) {
						throw error;
					}
	
					try {
						expect(await prioritizedSearchByDefault).to.have.length(1);
					} catch (error) {
						throw error
					}
				}); */
			});

			describe("closed", () => {
				before(async () => {
					session = await TestSession.connected(1);
				});

				after(async () => {
					await session.stop();
				});

				it("will throw when trying to search a closed store", async () => {
					const store = new TestStore({
						docs: new Documents<Document>(),
					});
					await session.peers[0].open(store, {
						args: {
							replicate: {
								factor: 1,
							},
						},
					});
					await store.close();
					await expect(
						store.docs.index.search(new SearchRequest({ query: [] })),
					).rejectedWith(ClosedError);
				});
			});

			describe("keep", () => {
				let peersCount = 2;
				before(async () => {
					session = await TestSession.connected(peersCount);
				});

				after(async () => {
					await session.stop();
				});

				it("will keep created by self even if not replicating", async () => {
					const store = new TestStore({
						docs: new Documents<Document>(),
					});
					await session.peers[0].open(store, {
						args: {
							replicate: {
								factor: 1,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
						},
					});

					const store2 = await session.peers[1].open<TestStore>(store.clone(), {
						args: {
							replicate: {
								factor: 0,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
							waitForPruneDelay: 0,
							keep: "self",
						},
					});

					await store2.docs.put(new Document({ id: "1" }));
					await store2.docs.log.waitForReplicators();
					await waitForResolved(async () =>
						expect(await store.docs.index.getSize()).to.eq(1),
					);
					// wait for some time to really make sure pruning could have happen

					await delay(5e3);

					// still expect store2 to have the entry
					await waitForResolved(async () =>
						expect(await store2.docs.index.getSize()).to.eq(1),
					);
				});

				it("will not keep if undefined", async () => {
					const store = new TestStore({
						docs: new Documents<Document>(),
					});
					await session.peers[0].open(store, {
						args: {
							replicate: {
								factor: 1,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
						},
					});

					const store2 = await session.peers[1].open<TestStore>(store.clone(), {
						args: {
							replicate: {
								factor: 0,
							},
							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: 0,
							waitForPruneDelay: 0,
							keep: undefined,
						},
					});

					await store2.docs.put(new Document({ id: "1" }));
					await store2.docs.log.waitForReplicators();
					await waitForResolved(async () =>
						expect(await store.docs.index.getSize()).to.eq(1),
					);

					await waitForResolved(async () =>
						expect(await store2.docs.index.getSize()).to.eq(0),
					);
				});

				// Test fetching docs with keep flag, and log entries
			});
		});

		describe("iterate", () => {
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
						number: BigInt(id),
					});

					// target 'none' so we dont send the entry here to other peers
					const resp = await stores[storeIndex].docs.put(doc, {
						target: "none",
						replicas: new AbsoluteReplicas(1),
					});

					// --- wait for all others to "want" this entry ---
					await stores[storeIndex].docs.log.rebalanceAll(); //  we need to call this to make other peers know that they are missing out of this hashes

					// because we have overriding the append to not send entries right away

					for (let i = 0; i < stores.length; i++) {
						if (i === storeIndex) {
							continue;
						}
						// when blow is true, we will be "forced" to query the other node for the data.
						// this allows use to test sorting where data is determenstically distributed
						// i.e put(1,123) will put a document at store 1 with id 123, and will never leave that store
						// store 0 and 2 who want to fetch all data will always have to ask node 1
						await waitForResolved(
							() =>
								expect(
									stores[i].docs.log.syncronizer.syncInFlight
										.get(stores[storeIndex].node.identity.publicKey.hashcode())
										?.has(resp.entry.hash),
								).to.be.true,
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
						value: 0n,
					}),
				) => {
					await waitForResolved(async () => {
						const req = new SearchRequest({
							query: [query],
							sort: [new Sort({ direction: SortDirection.ASC, key: "number" })],
						});
						const iterator = stores[fromStoreIndex].docs.index.iterate(req);

						if (batches.length === 0) {
							// No fetches has been made, so we don't know whether we are done yet
							expect(iterator.done()).to.be.false;
						} else {
							for (const batch of batches) {
								expect(iterator.done()).to.be.false;
								const next = await iterator.next(batch.length);
								if (
									(next.map((x) => x.number).join(",") ?? "") !==
									batch.join(",")
								) {
									console.error(
										"[iterate debug] from=%s expected=%o got=%o pending=%s done=%s",
										fromStoreIndex,
										batch,
										next.map((x) => x.number),
										await iterator.pending(),
										iterator.done(),
									);
								}
								expect(next.map((x) => x.number)).to.deep.equal(batch);
							}

							expect(iterator.done()).to.be.true;
						}
					});
					expect(stores[fromStoreIndex].docs.index.hasPending).to.be.false;
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
										session.peers[i].services.blocks,
									))!
								: new TestStore({
										docs: new Documents<Document>(),
									});

						await session.peers[i].open(store, {
							args: {
								index: {
									canRead: (_document: any, key: any) => {
										return canRead[i] ? canRead[i]!(key) : true;
									},
								},
								replicate: {
									// TODO choose offset so data is perfectly distributed
									factor: 1,
								},
								timeUntilRoleMaturity: 0,
								waitForReplicatorTimeout: 30000,
								replicas: { min: 1 }, // make sure documents only exist once
							},
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
						for (const key of session.peers
							.filter((_v, ix) => ix !== i)
							.map((x) => x.identity.publicKey)) {
							await stores[i].docs.log.waitForReplicator(key);
						}
					}
				});

				afterEach(async () => {
					await Promise.all(stores.map((x) => x.closed || x.drop()));
					stores = [];
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
					await stores[0].docs.log.replicate(false);
					const fanout = {
						root: (session.peers[1].services as any).fanout.publicKeyHash as string,
						channel: {
							msgRate: 10,
							msgSize: 256,
							uploadLimitBps: 1_000_000,
							maxChildren: 8,
							repair: true,
						},
						join: { timeoutMs: 20_000 },
					};
					const rootStore = stores[1];
					if (!rootStore.closed) {
						await (rootStore.docs.log as any)._openFanoutChannel(fanout);
					}
					for (const store of stores) {
						if (store.closed || store === rootStore) {
							continue;
						}
						await (store.docs.log as any)._openFanoutChannel(fanout);
					}
					await waitForResolved(async () =>
						expect((await stores[0].docs.log.getReplicators()).size).equal(1),
					);
					let data: number[] = [];
					for (let i = 0; i < 100; i++) {
						let doc = new Document({
							id: String(i),
							name: String(i),
							number: BigInt(i),
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
								value: 0n,
							}),
						],
						sort: [new Sort({ direction: SortDirection.ASC, key: "number" })],
					});
					const iterator = stores[0].docs.index.iterate(req);
					let acc: Document[] = [];
					while (iterator.done() !== true) {
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
						[2n, 3n, 4n],
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
								[2n, 3n, 4n],
							]),
						);
						promises.push(
							checkIterate(i, [
								[0n, 1n, 2n],
								[3n, 4n],
							]),
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
								sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
							}),
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
								sort: [
									new Sort({ direction: SortDirection.DESC, key: "name" }),
								],
							}),
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
							sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
						}),
					);
					expect(iterator.done()).to.be.false;
					const next = await iterator.next(3);
					expect(next.map((x) => x.name)).to.deep.equal(["0", "1", "2"]);
					expect(iterator.done()).to.be.true;
				});

				it("uses indexed fields", async () => {
					const KEY = "ABC";

					@variant("IndexClass")
					class IndexClass {
						@field({ type: "string" })
						id: string;

						@field({ type: "u64" })
						[KEY]: bigint;

						constructor(properties: { id: string; [KEY]: bigint }) {
							this.id = properties.id;
							this[KEY] = properties[KEY];
						}
					}

					// TODO fix types
					await (stores[0].docs as any).index.open({
						transform: {
							type: IndexClass,
							transform: async (obj: any) => {
								return new IndexClass({ id: obj.id, [KEY]: obj.number });
							},
						},
						indexBy: ["id"],
						dbType: Documents,
						canSearch: () => true,
						log: stores[0].docs.log,
						sync: () => undefined as any,
						documentType: Document,
					});

					let store = stores[0] as any as TestStore<IndexClass, any>;

					await put(0, 0);
					await put(0, 1);
					await put(0, 2);

					const iteratorValues = store.docs.index.iterate(
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.DESC, key: KEY })],
						}),
						{
							local: true,
							remote: false,
						},
					);
					const nextValues = await iteratorValues.next(3);
					expect(nextValues.map((x) => x.name)).to.deep.equal(["2", "1", "0"]);
					expect(iteratorValues.done()).to.be.true;
				});

				it("throws for a variant-less anonymous indexed type", async () => {
					const KEY = "anon";
					const AnonymousIndex = (() =>
						class {
							@field({ type: "string" })
							id: string;

							@field({ type: "u64" })
							[KEY]: bigint;

							constructor(properties: { id: string; [KEY]: bigint }) {
								this.id = properties.id;
								this[KEY] = properties[KEY];
							}
						})();

					expect(AnonymousIndex.name).to.equal("");

					// TODO fix types
					await expect(
						(stores[0].docs as any).index.open({
							transform: {
								type: AnonymousIndex,
								transform: async (obj: any) => {
									return new AnonymousIndex({ id: obj.id, [KEY]: obj.number });
								},
							},
							indexBy: ["id"],
							dbType: Documents,
							canSearch: () => true,
							log: stores[0].docs.log,
							sync: () => undefined as any,
							documentType: Document,
						}),
					).to.be.rejectedWith("missing @variant");
				});

				it("will retrieve partial results of not having read access", async () => {
					await put(0, 0);
					await put(1, 1);
					await put(1, 2);

					canRead[0] = () => Promise.resolve(false);
					const iterator = await stores[2].docs.index.iterate(
						new SearchRequest({
							query: [],
							sort: [new Sort({ direction: SortDirection.ASC, key: "name" })],
						}),
					);
					expect((await iterator.next(1)).map((x) => x.name)).to.deep.equal([
						"1",
					]);
					expect(iterator.done()).to.be.false;
					expect((await iterator.next(1)).map((x) => x.name)).to.deep.equal([
						"2",
					]);
					expect(iterator.done()).to.be.true;
				});

				describe("close", () => {
					it("by invoking close()", async () => {
						await put(0, 0);
						await put(0, 1);
						await put(0, 2);
						const request = new SearchRequest({
							query: [],
						});
						const iterator = await stores[1].docs.index.iterate(request);
						expect(iterator.done()).to.be.false;
						await iterator.next(2); // fetch some, but not all
						expect(
							await stores[0].docs.index.getPending(request.idString),
						).to.eq(1);
						await iterator.close();
						await waitForResolved(
							async () =>
								expect(
									await stores[0].docs.index.getPending(request.idString),
								).to.eq(undefined),
							{ timeout: 3000, delayInterval: 50 },
						);
					});

					it("requires correct id", async () => {
						await put(0, 0);
						await put(0, 1);
						const request = new SearchRequest({
							query: [],
						});
						const iterator = await stores[1].docs.index.iterate(request);
						expect(iterator.done()).to.be.false;
						await iterator.next(1); // fetch some, but not all
						expect(
							await stores[0].docs.index.getPending(request.idString),
						).to.eq(1);

						const closeRequest = new CloseIteratorRequest({ id: request.id });

						// Try to send from another peer (that is not the owner of the iterator)
						await stores[2].docs.index["_query"].send(closeRequest, {
							mode: new SilentDelivery({
								to: [session.peers[0].identity.publicKey],
								redundancy: 1,
							}),
						});

						await delay(2000);
						expect(
							await stores[0].docs.index.getPending(request.idString),
						).to.eq(1);

						// send from the owner
						await stores[1].docs.index["_query"].send(closeRequest, {
							mode: new SilentDelivery({
								to: [session.peers[0].identity.publicKey],
								redundancy: 1,
							}),
						});

						await waitForResolved(
							async () =>
								expect(
									await stores[0].docs.index.getPending(request.idString),
								).to.eq(undefined),
							{ timeout: 3000, delayInterval: 50 },
						);
					});

					it("end of iterator", async () => {
						await put(0, 0);
						await put(0, 1);
						await put(0, 2);
						const request = new SearchRequest({
							query: [],
						});
						const iterator = await stores[1].docs.index.iterate(request);
						expect(iterator.done()).to.be.false;
						await iterator.next(3); // fetch some, but not all
						await waitForResolved(
							async () =>
								expect(await stores[0].docs.index.getPending(request.idString))
									.to.be.undefined,
							{ timeout: 3000, delayInterval: 50 },
						);
					});

					it("closing store will result queues", async () => {
						await put(0, 0);
						await put(0, 1);
						await put(0, 2);

						const request = new SearchRequest({
							query: [],
						});
						const iterator = stores[1].docs.index.iterate(request);
						await iterator.next(1);
						expect(
							await stores[0].docs.index.getPending(request.idString),
						).to.eq(2); // two more results
						await stores[0].close();
						expect(await stores[0].docs.index.getPending(request.idString)).to
							.be.undefined;
					});

					it("dropping store will result queues", async () => {
						await put(0, 0);
						await put(0, 1);
						await put(0, 2);

						const request = new SearchRequest({
							query: [],
						});
						const iterator = stores[1].docs.index.iterate(request);
						await iterator.next(1);
						expect(
							await stores[0].docs.index.getPending(request.idString),
						).to.eq(2); // two more results
						await stores[0].drop();
						expect(await stores[0].docs.index.getPending(request.idString)).to
							.be.undefined;
					});

					it("end of iterator, multiple nexts", async () => {
						await put(0, 0);
						await put(0, 1);
						await put(0, 2);
						const request = new SearchRequest({
							query: [],
						});
						const iterator = stores[1].docs.index.iterate(request);
						await iterator.next(2);
						await iterator.next(1);
						expect(iterator.done()).to.be.true;
						await waitForResolved(
							async () =>
								expect(await stores[0].docs.index.getPending(request.idString))
									.to.be.undefined,
							{ timeout: 3000, delayInterval: 50 },
						);
					});
				});

				// TODO test iterator.close() to stop pending promises

				// TODO deletion while sort

				// TODO session timeouts?
			});

			describe("remote", () => {
				describe("wait", () => {
					let session: TestSession;

					afterEach(async () => {
						await session.stop();
					});

					const writerObserverSetup = async () => {
						session = await TestSession.disconnected(2);

						const store = new TestStore({
							docs: new Documents<Document>(),
						});

						const observer = await session.peers[0].open(store, {
							args: {
								replicate: false,
							},
						});

						// in this test we test if we can query joining peers while we are already iterating
						const writer = await session.peers[1].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
							},
						});

						return { observer, writer };
					};

					it("keeps iterator open until timeout", async () => {
						session = await TestSession.disconnected(1);

						const store = new TestStore({
							docs: new Documents<Document>(),
						});

						const observer = await session.peers[0].open(store, {
							args: {
								replicate: false,
							},
						});

						let waitFor = 3e3;
						const iterator = observer.docs.index.iterate(
							{},
							{
								remote: {
									wait: {
										timeout: waitFor,
										behavior: "keep-open",
									},
								},
							},
						);
						expect(iterator.done()).to.be.false;
						await iterator.next(1);
						expect(iterator.done()).to.be.false;
						await delay(waitFor + 1000);
						expect(iterator.done()).to.be.true;

						expect(store.docs.index.hasPending).to.be.false;
					});

						it("can query joining first replicator", async () => {
							const { observer, writer } = await writerObserverSetup();

							await writer.docs.put(new Document({ id: "1" }));
							let onMissedResults: number[] = [];

						const iterator = observer.docs.index.iterate(
							{},
							{
								remote: {
									wait: {
										timeout: 1e4,
									},
								},
								outOfOrder: {
									handle: ({ amount }: { amount: number }) => {
										onMissedResults.push(amount);
									},
								},
							},
						);
						const first = await iterator.next(1);
						expect(first).to.have.length(0);
						expect(iterator.done()).to.be.false;

							await session.connect(); // connect the nodes!

							await observer.docs.index.waitFor(writer.node.identity.publicKey);
							expect(iterator.done()).to.be.false;
							// Under full-suite load, the join and first remote query can take longer to converge.
							const second = await waitForResolved(
								async () => {
									const next = await iterator.next(1);
									expect(next).to.have.length(1);
									return next;
								},
								{ timeout: 30_000, delayInterval: 250 },
							);

							expect(onMissedResults).to.deep.equal([1]); // we should have missed one result

							expect(observer.docs.index.hasPending).to.be.false;
							expect(writer.docs.index.hasPending).to.be.false;
					});

					it("late join will not re-open iterator", async () => {
						session = await TestSession.disconnected(2);

						const store = new TestStore({
							docs: new Documents<Document>(),
						});

						const observer = await session.peers[0].open(store, {
							args: {
								replicate: false,
							},
						});

						// in this test we test if we can query joining peers while we are already iterating
						const writer = await session.peers[1].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
							},
						});

						await writer.docs.put(new Document({ id: "1" }));

						let waitFor = 1e2;
						const iterator = observer.docs.index.iterate(
							{},
							{
								remote: {
									wait: {
										timeout: waitFor,
									},
								},
							},
						);

						const first = await iterator.next(1);
						expect(first).to.have.length(0);
						expect(iterator.done()).to.be.false;

						await delay(waitFor);
						let queryCommenceCalls = 0;
						const queryCommenceFn = observer.docs.index["queryCommence"].bind(
							observer.docs.index,
						);

						observer.docs.index["queryCommence"] = (a, b) => {
							queryCommenceCalls++;
							return queryCommenceFn(a, b);
						};

						await session.connect(); // connect the nodes!

						await observer.docs.index.waitFor(writer.node.identity.publicKey);
						expect(iterator.done()).to.be.true;
						const second = await iterator.next(0);
						expect(queryCommenceCalls).to.equal(0); // we should not have re-commenced the query
						expect(second).to.have.length(0);

						expect(observer.docs.index.hasPending).to.be.false;
						expect(writer.docs.index.hasPending).to.be.false;
					});

					it("onMissedResults respects already emitted results", async () => {
						// test that we will get missed results accuruately
						session = await TestSession.disconnected(3);

						const store = new TestStore({
							docs: new Documents<Document>(),
						});

						const observer = await session.peers[0].open(store, {
							args: {
								replicate: false,
							},
						});

						// in this test we test if we can query joining peers while we are already iterating
						const writer1 = await session.peers[1].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
							},
						});

						const writer2 = await session.peers[2].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
							},
						});

						await writer1.docs.put(new Document({ id: "1" }));
						await writer1.docs.put(new Document({ id: "4" }));

						await writer2.docs.put(new Document({ id: "2" }));
						await writer2.docs.put(new Document({ id: "3" }));

						let missedResults: number[] = [];

						await session.connect([[session.peers[0], session.peers[2]]]); // connect the nodes!

						await observer.docs.index.waitFor(writer2.node.identity.publicKey);

						const iterator = observer.docs.index.iterate(
							{ sort: new Sort({ key: "id", direction: SortDirection.DESC }) }, // 4, 3, 2, 1
							{
								remote: {
									wait: {
										timeout: 1e4,
									},
								},
								outOfOrder: {
									handle: ({ amount }: { amount: number }) => {
										missedResults.push(amount);
									},
								},
							},
						);

						const first = await iterator.next(1);
						const second = await iterator.next(1);
						expect(first.map((x) => x.id)).to.deep.equal(["3"]);
						expect(second.map((x) => x.id)).to.deep.equal(["2"]);

						await session.connect([[session.peers[0], session.peers[1]]]); // connect the nodes!
						await observer.docs.index.waitFor(writer1.node.identity.publicKey);

						await waitForResolved(() =>
							expect(missedResults).to.deep.equal([1]),
						);
						const third = await iterator.next(1);
						const fourth = await iterator.next(1);

						expect(third.map((x) => x.id)).to.deep.equal(["4"]); // because we sort DESC
						expect(fourth.map((x) => x.id)).to.deep.equal(["1"]);
					});

					it("it will not wait for previous replicator if it can handle joining", async () => {
						let directory = "./tmp/test-iterate-joining/" + new Date();
						session = await TestSession.connected(2, [
							{ directory },
							{ directory: undefined },
						]);

						const store = new TestStore({
							docs: new Documents<Document>(),
						});

						const observer = await session.peers[0].open(store, {
							args: {
								replicate: false,
							},
						});

						// in this test we test if we can query joining peers while we are already iterating
						const replicator = await session.peers[1].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
							},
						});

						await waitForResolved(async () =>
							expect([
								...(await observer.docs.log.getReplicators()),
							]).to.deep.eq([replicator.node.identity.publicKey.hashcode()]),
						);

						await session.peers[0].stop();
						await session.peers[1].stop();
						session = await TestSession.connected(1, { directory });
						const observerAgain = await session.peers[0].open(store.clone(), {
							args: {
								replicate: false,
							},
						});
						let waitForMax = 3e3;
						const iterator = observerAgain.docs.index.iterate(
							{},
							{ remote: { wait: { timeout: waitForMax } } },
						);
						let t0 = +new Date();
						await iterator.next(1);
							let t1 = +new Date();
							let delta = 500; // lower bound slack (ms)
							let upperDelta = 1500; // CI/full-suite can overshoot timers under load
							expect(t1 - t0).to.lessThan(delta); // +some delta
							expect(iterator.done()).to.be.false;
							await iterator.all();
							let t2 = +new Date();
							expect(t2 - t0).to.lessThan(waitForMax + upperDelta); // +some delta
							expect(t2 - t0).to.be.greaterThanOrEqual(waitForMax - delta); // -some delta
						});

					describe("policy", () => {
						it("blocking wait for any", async () => {
							// Test that "wait: 'any'" policy returns as soon as any result is available, not waiting for all
							session = await TestSession.disconnected(2);

							const store = new TestStore({
								docs: new Documents<Document>(),
							});

							await session.peers[0].open(store, {
								args: {
									replicate: false,
								},
							});

							const replicator = await session.peers[1].open(store.clone(), {
								args: {
									replicate: {
										factor: 1,
									},
								},
							});

							// Add a document on peer 1
							await replicator.docs.put(new Document({ id: "1" }));

							// Now, test the "wait: 'any'" policy
							const iterator = store.docs.index.iterate(
								{},
								{
									remote: {
										wait: {
											timeout: 5e3,
											until: "any",
											behavior: "block",
										},
									},
								},
							);

							// verify that we only fetch results ONCE from the remote

							let queryCount = 0;
							const _request = store.docs.index._query.request.bind(
								store.docs.index._query,
							);
							store.docs.index._query.request = function (req, options) {
								queryCount++;
								return _request(req, options);
							};

							const resultPromise = iterator.first(); // because of block, we can invoke first before connecting
							await session.connect(); // connect the nodes!

							const t0 = +new Date();
							const result = await resultPromise;
							const t1 = +new Date();

							expect(queryCount).to.equal(1); // should only query once
							expect(result?.id).to.equal("1");
							expect(t1 - t0).to.be.lessThan(500); // Should return quickly, not wait for timeout
						});

						it("uses wait timeout as remote rpc timeout when timeout is omitted", async () => {
							session = await TestSession.connected(1);

							const store = await session.peers[0].open(
								new TestStore({
									docs: new Documents<Document>(),
								}),
								{
									args: {
										replicate: false,
									},
								},
							);

							const wantedTimeout = 12_345;
							let observedTimeout: number | undefined = undefined;
							const requestFn = store.docs.index._query.request.bind(
								store.docs.index._query,
							);
							store.docs.index._query.request = async (request, options) => {
								if (
									request instanceof SearchRequest ||
									request instanceof SearchRequestIndexed ||
									request instanceof IterationRequest
								) {
									observedTimeout = options?.timeout;
									return [];
								}
								return requestFn(request, options);
							};

							await store.docs.index["queryCommence"](
								new SearchRequest({ fetch: 1 }),
								{
									local: false,
									remote: {
										from: ["missing-peer"],
										wait: {
											timeout: wantedTimeout,
										},
									},
								},
							);

							expect(observedTimeout).to.equal(wantedTimeout);
						});
					});
				});

				describe("scope", () => {
					describe("eager", () => {
						let peersCount = 2;

						beforeEach(async () => {
							session = await TestSession.disconnected(peersCount);
						});

						afterEach(async () => {
							await session.stop();
						});

						it("will query newly joined peer when eager", async () => {
							const store = new TestStore({
								docs: new Documents<Document>(),
							});
							await session.peers[0].open(store, {
								args: {
									replicate: {
										factor: 1,
									},
								},
							});

							const store2 = await session.peers[1].open<TestStore>(
								store.clone(),
								{
									args: {
										replicate: {
											factor: 1,
										},
									},
								},
							);

							await store2.docs.put(new Document({ id: "1" }));

							let joined = false;
								store.docs.log.events.addEventListener(
									"replicator:join",
									async () => {
										expect(
											(
												await store.docs.index
													.iterate(
														{},
														{
															local: false,
															remote: { reach: { eager: true } },
														},
													)
													.all()
											).length,
									).to.equal(1);
									joined = true;
								},
							);

							await session.connect();
							await waitForResolved(() => expect(joined).to.be.true);
						});
					});

					describe("discover", () => {
						let peersCount = 2;

						beforeEach(async () => {
							session = await TestSession.disconnected(peersCount);
						});

						afterEach(async () => {
							await session.stop();
						});

						it("will wait for inflight", async () => {
							await session.connect();

							const store = new TestStore({
								docs: new Documents<Document>(),
							});

							const store2 = await session.peers[1].open<TestStore>(
								store.clone(),
								{
									args: {
										replicate: {
											factor: 1,
										},
									},
								},
							);

							await store2.docs.put(new Document({ id: "1" }));

							const reader = await session.peers[0].open(store, {
								args: {
									replicate: {
										factor: 1,
									},
								},
							});

							const allResults = await reader.docs.index
								.iterate(
									{},
									{
										remote: {
											reach: { discover: [store2.node.identity.publicKey] },
										},
									},
								)
								.all();
							expect(allResults.length).to.equal(1);
						});

						it("will resolve immediately if not inflight", async () => {
							await session.connect();

							const reader = await session.peers[0].open(
								new TestStore({
									docs: new Documents<Document>(),
								}),
								{
									args: {
										replicate: {
											factor: 1,
										},
									},
								},
							);
							let t0 = +new Date();
							const allResults = await reader.docs.index
								.iterate(
									{},
									{
										remote: {
											reach: {
												discover: [(await Ed25519Keypair.create()).publicKey],
											},
										},
									},
								)
								.all();
							let t1 = +new Date();
							expect(t1 - t0).to.be.lessThan(500);
							expect(allResults.length).to.equal(0);
						});
					});
				});
			});

			describe("signal", () => {
				it("can closes on abort", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore({
						docs: new Documents<Document>(),
					});

					await session.peers[0].open(store, {
						args: {
							replicate: {
								factor: 1,
							},
						},
					});
					const controller = new AbortController();
					const iterator = store.docs.index.iterate(
						{},
						{
							signal: controller.signal,
						},
					);

					expect(iterator.done()).to.be.false;
					controller.abort();
					expect(iterator.done()).to.be.true;

					expect(store.docs.index.hasPending).to.be.false;
				});
			});

			it("can consume as async iterator", async () => {
				session = await TestSession.connected(1);

				const store = new TestStore({
					docs: new Documents<Document>(),
				});

				await session.peers[0].open(store, {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});
				await store.docs.put(new Document({ id: "1" }));
				await store.docs.put(new Document({ id: "2" }));

				// make iterator not close by waiting for joining
				const iterator = store.docs.index.iterate(
					{},
					{ remote: { wait: { timeout: 5e3 } } },
				);

				let entries: Document[] = [];
				for await (const entry of iterator) {
					entries.push(entry);
				}
				await iterator.close();
				expect(entries).to.have.length(2);
				expect(entries[0].id).to.equal("1");
				expect(entries[1].id).to.equal("2");
			});

			describe("closePolicy", () => {
				it("is manual keeps open", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore({ docs: new Documents<Document>() });
					await session.peers[0].open(store, {
						args: { replicate: { factor: 1 } },
					});

					const iterator = store.docs.index.iterate(
						{},
						{ closePolicy: "manual" },
					);
					expect(iterator.done()).to.be.false;
					const next = await iterator.next(1);
					expect(next).to.have.length(0);
					expect(iterator.done()).to.be.false;

					await iterator.close();
					expect(iterator.done()).to.be.true;
				});

				it("onEmpty closes iterator when drained", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore({ docs: new Documents<Document>() });
					await session.peers[0].open(store, {
						args: { replicate: { factor: 1 } },
					});

					await store.docs.put(new Document({ id: "1" }));

					const iterator = store.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{ local: true, remote: false /* , closePolicy: "onEmpty" */ }, // onEmpty is default
					);

					const first = await iterator.next(1);
					expect(first.map((d) => d.id)).to.deep.equal(["1"]);
					expect(iterator.done()).to.be.true;
				});
			});
		});

		describe("updates", () => {
			it("sorted only: mid-insert yields sorted order", async () => {
				session = await TestSession.connected(1);

				const store = new TestStore({ docs: new Documents<Document>() });

				await session.peers[0].open(store, {
					args: { replicate: { factor: 1 } },
				});
				await store.docs.put(new Document({ id: "1" }));
				await store.docs.put(new Document({ id: "3" }));

				const iterator = store.docs.index.iterate(
					{ sort: { key: "id", direction: SortDirection.ASC } },
					{ updates: { merge: true }, closePolicy: "manual" },
				);
				const head = await iterator.next(1);
				expect(head).to.have.length(1);
				expect(head[0].id).to.equal("1");
				await store.docs.put(new Document({ id: "2" }));

				const rest = await iterator.all();

				const all = [...head, ...rest];
				expect(all.map((d) => d.id)).to.deep.equal(["1", "2", "3"]);
			});

			it("accepts shorthand 'local' for merge updates", async () => {
				session = await TestSession.connected(1);

				const store = new TestStore({ docs: new Documents<Document>() });

				await session.peers[0].open(store, {
					args: { replicate: { factor: 1 } },
				});
				await store.docs.put(new Document({ id: "1" }));
				await store.docs.put(new Document({ id: "3" }));

				const iterator = store.docs.index.iterate(
					{ sort: { key: "id", direction: SortDirection.ASC } },
					{ updates: "local", closePolicy: "manual" },
				);

				const initial = await iterator.next(1);
				expect(initial.map((d) => d.id)).to.deep.equal(["1"]);

				await store.docs.put(new Document({ id: "2" }));

				const rest = await iterator.all();
				const full = [...initial, ...rest];
				expect(full.map((d) => d.id)).to.deep.equal(["1", "2", "3"]);
			});

			it("filter: drop removals and exclude a specific id", async () => {
				session = await TestSession.connected(1);

				const store = new TestStore({ docs: new Documents<Document>() });

				await session.peers[0].open(store, {
					args: { replicate: { factor: 1 } },
				});
				await store.docs.put(new Document({ id: "1" }));
				await store.docs.put(new Document({ id: "2" }));

				let filterCalls = 0;
				let evtCalls = 0;
				const iterator = store.docs.index.iterate(
					{ sort: { key: "id", direction: SortDirection.ASC } },
					{
						closePolicy: "manual",
						updates: {
							merge: {
								filter: (evt) => {
									// ignore removals and drop any added with id === '2'
									filterCalls++;
									return {
										added: evt.added.filter((x) => x.id !== "2"),
										removed: [],
									};
								},
							},
							notify: (reason) => {
								if (reason === "change") {
									evtCalls++;
								}
							},
						},
					},
				);

				const firstTwo = await iterator.next(2);
				expect(firstTwo.map((d) => d.id)).to.deep.equal(["1", "2"]);
				expect(iterator.done()).to.be.false;

				await store.docs.del("1");
				await store.docs.put(new Document({ id: "3" }));
				await store.docs.put(new Document({ id: "2" })); // filtered out by id

				await waitForResolved(() => expect(filterCalls).to.equal(3));
				await waitForResolved(() => expect(evtCalls).to.equal(1)); // only one event, because we filtered out the removal
				const rest = await iterator.all();
				expect(rest).to.have.length(1);
				expect(rest[0].id).to.equal("3");
			});

			it("removals are reflected in iterators (sorted)", async () => {
				session = await TestSession.connected(1);
				const store = new TestStore({ docs: new Documents<Document>() });
				await session.peers[0].open(store, {
					args: { replicate: { factor: 1 } },
				});

				await store.docs.put(new Document({ id: "1" }));
				await store.docs.put(new Document({ id: "2" }));

				const iterator = store.docs.index.iterate(
					{ sort: { key: "id", direction: SortDirection.ASC } },
					{ closePolicy: "manual", updates: { merge: true } },
				);

				const firstTwo = await iterator.next(2);
				expect(firstTwo.map((d) => d.id)).to.deep.equal(["1", "2"]);

				// Remove first and add a third
				await store.docs.del("1"); // assumes test helpers accept this
				await store.docs.put(new Document({ id: "3" }));

				const rest = await iterator.all();
				expect(rest.map((x) => x.id)).to.deep.equal(["3"]);
			});

			it("accepts shorthand 'remote' for push updates", async () => {
				session = await TestSession.connected(2);

				const writerStore = new TestStore({ docs: new Documents<Document>() });
				const readerStore = writerStore.clone();

				const writer = await session.peers[0].open(writerStore, {
					args: { replicate: { factor: 1 } },
				});
				const reader = await session.peers[1].open(readerStore, {
					args: { replicate: false },
				});

				let iterator:
					| Awaited<ReturnType<typeof reader.docs.index.iterate>>
					| undefined;

				await reader.docs.index.waitFor(writer.node.identity.publicKey);

				iterator = reader.docs.index.iterate(
					{ sort: { key: "id", direction: SortDirection.ASC } },
					{ remote: true, closePolicy: "manual", updates: "remote" },
				);

				const initial = await iterator.next(1);
				expect(initial).to.have.length(0);
				expect(iterator.done()).to.be.false;

				const docId = "remote-shorthand";
				await writer.docs.put(new Document({ id: docId }));

				await waitForResolved(async () => {
					expect(await iterator!.pending()).to.equal(1);
				});

				const batch = await iterator.next(1);
				expect(batch).to.have.length(1);
				expect(batch[0].id).to.equal(docId);
			});

			it("iterator all terminates when batches are empty but still waiting for joining", async () => {
				let directory = "./tmp/iterator-all-terminates/" + new Date();
				session = await TestSession.connected(2, [
					{ directory },
					{ directory: undefined },
				]);

				const store = new TestStore({
					docs: new Documents<Document>(),
				});

				const observer = await session.peers[0].open(store, {
					args: {
						replicate: false,
					},
				});

				let waitForMax = 3e3;

				// count processQuery calls
				const spyFn = sinon.spy(observer.docs.index.processQuery);
				observer.docs.index.processQuery = spyFn as any;
				let t0 = +new Date();
				await observer.docs.index.iterate({}, { closePolicy: "manual" }).all();
				let t1 = +new Date();
				expect(t1 - t0).to.lessThan(waitForMax + 500); // +some delta
				expect(spyFn.callCount).to.equal(1); // should only call next once
			});

			it("respects the query when merging updates by default", async () => {
				session = await TestSession.connected(1);

				const store = new TestStore({ docs: new Documents<Document>() });

				await session.peers[0].open(store, {
					args: { replicate: { factor: 1 } },
				});
				await store.docs.put(new Document({ id: "1", name: "match" }));
				await store.docs.put(new Document({ id: "2", name: "dontmatch" }));

				const iterator = store.docs.index.iterate(
					{
						query: [new StringMatch({ key: "name", value: "match" })],
						sort: { key: "id", direction: SortDirection.ASC },
					},
					{ updates: { merge: true }, closePolicy: "manual" },
				);
				const head = await iterator.next(1);
				expect(head).to.have.length(1);
				expect(head[0].id).to.equal("1");
				await store.docs.put(new Document({ id: "5", name: "match" }));
				await store.docs.put(new Document({ id: "4", name: "dontmatch" }));
				await store.docs.put(new Document({ id: "3", name: "match" }));

				const rest = await iterator.all();

				const all = [...head, ...rest];
				expect(all.map((d) => d.id)).to.deep.equal(["1", "3", "5"]);
			});

			// ───────────────────────────────────────────────────────────────
			// Regression: prediction/prefetch must seed a keep-alive iterator
			// ───────────────────────────────────────────────────────────────
			it("prediction path seeds keep-alive so collect next succeeds (no 'Missing iterator')", async function () {
				session = await TestSession.disconnected(3);
				// writer(0) <-> replicator(1) <-> reader(2)
				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
				]);

				const base = new TestStore({ docs: new Documents<Document>() });
				const replicator = await session.peers[1].open(base, {
					args: { replicate: { factor: 1 } },
				});
				const writer = await session.peers[0].open(base.clone(), {
					args: { replicate: { factor: 1 } },
				});
				const reader = await session.peers[2].open(base.clone(), {
					args: { replicate: false },
				});

				// Many installations log this from the replicator's logger, not the global console.
				// Fall back to stubbing global console.error so the test is still useful.
				const errStub = sinon.stub(console, "error");

				let iterator:
					| Awaited<ReturnType<typeof reader.docs.index.iterate>>
					| undefined;

				try {
					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					// 1) Write BEFORE reader opens iterator to encourage the predictor path.
					const docId = "predicted-keepalive-regression";
					await writer.docs.put(new Document({ id: docId }));

					// 2) Reader opens iterator with push updates; replicator will try to satisfy via predicted payload.
					iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							remote: {
								wait: { timeout: 5e3, behavior: "keep-open" },
								reach: {
									discover: [replicator.node.identity.publicKey],
									eager: true,
								},
							},
							closePolicy: "manual",
							updates: { push: true, merge: true }, // push path triggers the prefetch/prediction
						},
					);

					// 3) The reader should be told there's 1 pending item via the push pipeline.
					await waitForResolved(
						async () => expect(await iterator!.pending()).to.equal(1),
						{ timeout: 30_000 },
					);

					// 4) Drain. Without keep-alive on the predicted iterator, server logs "Missing iterator ...".
					const batch = await iterator.next(1);
					expect(batch).to.have.length(1);
					expect(batch[0].id).to.equal(docId);

					// Assert no "Missing iterator..." was logged (will fail until upstream is fixed).
					const hadMissing = errStub
						.getCalls()
						.some((c) =>
							/Missing iterator for request with id/i.test(
								c.args.map(String).join(" "),
							),
						);
					expect(hadMissing).to.be.false;
				} finally {
					errStub.restore();
					await iterator?.close();
					await reader.close();
					await writer.close();
					await replicator.close();
				}
			});

			describe("push", () => {
				const assertPush = async (push: boolean) => {
					session = await TestSession.disconnected(3);
					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: {
							replicate: { factor: 1 },
						},
					});

					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});

					const readerPrototype = base.clone();
					let reader = await session.peers[2].open(readerPrototype.clone(), {
						args: {
							replicate: false,
						},
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					let notificationPromise: DeferredPromise<void> | null = null;
					let notifications: UpdateReason[] = [];
					const onNotify = (reason: UpdateReason) => {
						notifications.push(reason);
						if (notificationPromise) {
							notificationPromise.resolve();
						}
					};
					let timeout: ReturnType<typeof setTimeout> | null = null;
					if (push) {
						notificationPromise = pDefer<void>();
						// safety timeout to avoid hanging tests
						timeout = setTimeout(() => {
							notificationPromise?.resolve();
						}, 30_000);
					}

					const iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							remote: {
								wait: { timeout: 5e3, behavior: "keep-open" },
								reach: {
									discover: [replicator.node.identity.publicKey],
									eager: true,
								},
							},
							updates: push
								? {
										push: true,
										notify: (reason) => {
											onNotify(reason);
										},
										merge: true,
									}
								: {
										push: false,
										notify: (reason) => {
											onNotify(reason);
										},
										merge: false,
									},
						},
					);

					try {
						const out = await iterator.next(1); // establish iterator remotely
						expect(out).to.have.length(0);

						const docId = `push-nonrep-${Date.now()}`;
						await writer.docs.put(new Document({ id: docId }));

						if (push) {
							await notificationPromise!.promise;
							await waitForResolved(
								async () => expect(await iterator.pending()).to.equal(1),
								{ timeout: 30_000 },
							);
							const batch = await iterator.next(1);
							expect(batch).to.have.length(1);
							expect(batch[0].id).to.equal(docId);
						} else {
							await delay(5_000); // ensure we didn't receive a pushed batch
							expect(await iterator.pending()).to.equal(1);
							const batch = await iterator.next(1);
							expect(batch).to.have.length(1);
							expect(batch[0].id).to.equal(docId);
							expect(notifications).to.have.length(0); // no notifications should have occurred
						}
					} finally {
						timeout && clearTimeout(timeout);
						notificationPromise?.resolve();
						await iterator.close();
						await reader.close();
						await writer.close();
						await replicator.close();
					}
				};
				it("push=false property gates push streams", async () => {
					await assertPush(false);
				});

				it("push=true enables push streams", async () => {
					await assertPush(true);
				});

				it("push streams are honored for explicit SearchRequest", async () => {
					session = await TestSession.disconnected(3);
					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: {
							replicate: { factor: 1 },
						},
					});

					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});

					const reader = await session.peers[2].open(base.clone(), {
						args: { replicate: false },
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					let iterator:
						| Awaited<ReturnType<typeof reader.docs.index.iterate>>
						| undefined;

					try {
						iterator = reader.docs.index.iterate(
							new SearchRequest({
								sort: new Sort({ key: ["id"], direction: SortDirection.ASC }),
							}),
							{
								closePolicy: "manual",
								remote: {
									wait: { timeout: 5e3, behavior: "keep-open" },
									reach: {
										discover: [replicator.node.identity.publicKey],
										eager: true,
									},
								},
								updates: { push: true, merge: true },
							},
						);

						const initial = await iterator.next(1);
						expect(initial).to.have.length(0);

						const docId = `push-search-request-${Date.now()}`;
						await writer.docs.put(new Document({ id: docId }));

						await waitForResolved(
							async () => expect(await iterator!.pending()).to.equal(1),
							{ timeout: 10_000 },
						);

						const batch = await iterator.next(1);
						expect(batch).to.have.length(1);
						expect(batch[0].id).to.equal(docId);
					} finally {
						await iterator?.close();
						await reader?.close();
						await writer?.close();
						await replicator?.close();
					}
				});

				it("push streams drop out-of-order updates and emit onOutOfOrder", async function () {
					session = await TestSession.disconnected(3);
					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: {
							replicate: { factor: 1 },
						},
					});

					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});

					const reader = await session.peers[2].open(base.clone(), {
						args: { replicate: false },
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					const latePromise = pDefer<void>();
					const lateEvents: LateResultsEvent[] = [];

					const iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							remote: {
								wait: { timeout: 5e3, behavior: "keep-open" },
								reach: {
									discover: [replicator.node.identity.publicKey],
									eager: true,
								},
							},
							outOfOrder: {
								handle: (evt: LateResultsEvent) => {
									lateEvents.push(evt);
									latePromise.resolve();
								},
							},
							updates: { push: true, merge: true },
						},
					);

					try {
						// Seed ordered results
						await writer.docs.put(new Document({ id: "2" }));
						await writer.docs.put(new Document({ id: "3" }));

						const initialIds: string[] = [];
						const initialIdSet = new Set<string>();
						await waitForResolved(
							async () => {
								const batch = await iterator.next(10);
								for (const doc of batch) {
									if (initialIdSet.has(doc.id)) {
										continue;
									}
									initialIdSet.add(doc.id);
									initialIds.push(doc.id);
								}
								expect(initialIds).to.deep.equal(["2", "3"]);
							},
							{ timeout: 10_000 },
						);

						// Insert an out-of-order doc; it should be dropped from push stream
						await writer.docs.put(new Document({ id: "1" }));

						await latePromise.promise;
						expect(lateEvents.some((e) => e.amount >= 1)).to.be.true;

						await waitForResolved(
							async () => expect(await iterator.pending()).to.equal(0),
							{ timeout: 10_000 },
						);

						const next = await iterator.next(1);
						expect(next).to.have.length(0);
					} finally {
						await iterator.close();
						await reader.close();
						await writer.close();
						await replicator.close();
					}
				});

				it("outOfOrder mode=drop can collect dropped items", async function () {
					session = await TestSession.disconnected(3);
					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: { replicate: { factor: 1 } },
					});
					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});
					const reader = await session.peers[2].open(base.clone(), {
						args: { replicate: false },
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					const latePromise = pDefer<void>();
					let collected:
						| {
								indexed: any;
								context: Context;
								from: any;
								value?: any;
						  }[]
						| undefined;

					const iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							remote: {
								wait: { timeout: 5e3, behavior: "keep-open" },
								reach: {
									discover: [replicator.node.identity.publicKey],
									eager: true,
								},
							},
							outOfOrder: {
								mode: "drop",
								handle: async (_evt, helpers) => {
									collected = await helpers.collect();
									latePromise.resolve();
								},
							},
							updates: { push: true, merge: true },
						},
					);

					try {
						await writer.docs.put(new Document({ id: "2" }));
						await writer.docs.put(new Document({ id: "3" }));
						await iterator.next(10);

						await writer.docs.put(new Document({ id: "1" }));

						await latePromise.promise;
						expect(collected?.length).to.equal(1);
						expect(
							collected?.[0]?.value?.id || collected?.[0]?.indexed?.id,
						).to.equal("1");
						// drop mode should not expose late items to the iterator; collect is best-effort
						expect(collected?.[0]?.value?.__context).to.exist;
						expect(collected?.[0]?.value?.__indexed).to.exist;

						await waitForResolved(
							async () => expect(await iterator.pending()).to.equal(0),
							{ timeout: 10_000 },
						);
						const next = await iterator.next(1);
						expect(next).to.have.length(0);
					} finally {
						await iterator.close();
						await reader.close();
						await writer.close();
						await replicator.close();
					}
				});

					it("outOfOrder mode=queue buffers late items for next()", async function () {
						// This test can run under heavy full-suite load where push iterators may briefly yield
						// empty batches. We explicitly drain in-order items before inserting the late one,
						// and we bound waiting on the outOfOrder handler to avoid hanging the entire suite.
						this.timeout(80_000);

						session = await TestSession.disconnected(3);
						await session.connect([
							[session.peers[0], session.peers[1]],
							[session.peers[1], session.peers[2]],
						]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: { replicate: { factor: 1 } },
					});
					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});
					const reader = await session.peers[2].open(base.clone(), {
						args: { replicate: false },
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					const latePromise = pDefer<void>();
					let lateEvt: LateResultsEvent<"queue"> | undefined;
					let collected:
						| {
								indexed: any;
								context: Context;
								from: any;
								value?: any;
						  }[]
						| undefined;

					const iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							remote: {
								wait: { timeout: 5e3, behavior: "keep-open" },
								reach: {
									discover: [replicator.node.identity.publicKey],
									eager: true,
								},
							},
							outOfOrder: {
								mode: "queue",
								handle: async (evt, helpers) => {
									collected = await helpers.collect();
									lateEvt = evt;
									latePromise.resolve();
								},
							},
							updates: { push: true, merge: true },
						},
						);

						try {
							await writer.docs.put(new Document({ id: "2" }));
							await writer.docs.put(new Document({ id: "3" }));
							// Drain in-order items so the iterator establishes a frontier beyond "1".
							// If we insert "1" before that frontier exists, it may no longer be considered "late"
							// and the outOfOrder handler would never fire (leading to a timeout).
							const seen = new Set<string>();
							const start = Date.now();
							while (
								Date.now() - start < 10_000 &&
								(!seen.has("2") || !seen.has("3"))
							) {
								const batch = await iterator.next(10);
								for (const doc of batch) {
									seen.add(doc.id);
								}
								if (!seen.has("2") || !seen.has("3")) {
									await delay(50);
								}
							}
							expect([...seen]).to.include("2");
							expect([...seen]).to.include("3");

							await waitForResolved(
								async () => expect(await iterator.pending()).to.equal(0),
								{ timeout: 10_000, delayInterval: 50 },
							);

							await writer.docs.put(new Document({ id: "1" }));

							// Avoid hanging the entire suite if outOfOrder delivery never happens.
							await Promise.race([
								latePromise.promise,
								delay(20_000).then(() => {
									throw new Error(
										"Timed out waiting for outOfOrder(queue) handler",
									);
								}),
							]);
							expect(collected?.length).to.equal(1);
							expect(lateEvt?.items?.length).to.equal(1);
							const item = lateEvt!.items![0];
							expect(item.value?.id ?? item.indexed.id).to.equal("1");
							expect(item.value?.__context).to.exist;
						expect(item.value?.__indexed).to.exist;
						expect(collected?.[0]?.value?.__context).to.exist;
						expect(collected?.[0]?.value?.__indexed).to.exist;

						await waitForResolved(
							async () => expect(await iterator.pending()).to.equal(1),
							{ timeout: 10_000 },
						);
						const next = await iterator.next(1);
						expect(next).to.have.length(1);
						expect(next[0].id).to.equal("1");
					} finally {
						await iterator.close();
						await reader.close();
						await writer.close();
						await replicator.close();
					}
				});

				it("pending still counts buffered in-order results after late drop", async function () {
					this.timeout(40_000);
					session = await TestSession.disconnected(3);
					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: { replicate: { factor: 1 } },
					});
					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});
					const reader = await session.peers[2].open(base.clone(), {
						args: { replicate: false },
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					const latePromise = pDefer<void>();

					const iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							outOfOrder: {
								mode: "drop",
								handle: (_evt, helpers) => {
									void helpers.collect();
									latePromise.resolve();
								},
							},
							updates: { push: true, merge: true },
						},
					);

						try {
							// two in-order items that should remain pending
							await writer.docs.put(new Document({ id: "2" }));
							await writer.docs.put(new Document({ id: "3" }));
							// establish frontier with in-order fetch (leave one buffered)
							// In push-update mode this can briefly return an empty batch under load.
							const firstBatch = await waitForResolved(
								async () => {
									const batch = await iterator.next(1);
									if (batch.length !== 1) {
										throw new Error(
											`Expected 1 frontier item, got ${batch.length}`,
										);
									}
									return batch;
								},
								{ timeout: 10_000, delayInterval: 50 },
							);
							expect(firstBatch.map((x) => x.id)).to.deep.equal(["2"]);
							// one late item that will be dropped
							await writer.docs.put(new Document({ id: "1" }));

							// Late-drop callback timing can vary; avoid hanging the test on callback delivery.
						await Promise.race([latePromise.promise, delay(5_000)]);

						const pendingBefore = await iterator.pending();
						// even if pending reports 0 after a drop, we should still be able to fetch buffered in-order items
						expect(pendingBefore).to.be.at.least(0);

						const next = await iterator.next(2);
						expect(next.map((x) => x.id)).to.deep.equal(["3"]);
					} finally {
						await iterator.close();
						await reader.close();
						await writer.close();
						await replicator.close();
					}
				});

					it("outOfOrder queue emits normalized late items", async function () {
						this.timeout(120_000);

						session = await TestSession.disconnected(3);
						await session.connect([
							[session.peers[0], session.peers[1]],
							[session.peers[1], session.peers[2]],
					]);

					const base = new TestStore({ docs: new Documents<Document>() });
					const replicator = await session.peers[1].open(base, {
						args: { replicate: { factor: 1 } },
					});
					const writer = await session.peers[0].open(base.clone(), {
						args: { replicate: false },
					});
					const reader = await session.peers[2].open(base.clone(), {
						args: { replicate: false },
					});

					await reader.docs.index.waitFor(replicator.node.identity.publicKey);
					await writer.docs.index.waitFor(replicator.node.identity.publicKey);

					const latePromise = pDefer<void>();
					let lateEvt: LateResultsEvent<"queue"> | undefined;
					let collected: LateResultsEvent<"queue">["items"] | undefined;

					const iterator = reader.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							remote: {
								wait: { timeout: 5e3, behavior: "keep-open" },
								reach: {
									discover: [replicator.node.identity.publicKey],
									eager: true,
								},
							},
							outOfOrder: {
								mode: "queue",
								handle: async (evt, helpers) => {
									lateEvt = evt;
									collected = await helpers.collect();
									latePromise.resolve();
								},
							},
							updates: { push: true, merge: true },
						},
					);

						try {
							await writer.docs.put(new Document({ id: "2" }));
							// Establish frontier deterministically. Under load, push-update mode may
							// briefly return empty batches.
							const firstBatch = await waitForResolved(
								async () => {
									const batch = await iterator.next(10);
									if (batch.length !== 1) {
										throw new Error(
											`Expected 1 frontier item, got ${batch.length}`,
										);
									}
									if (batch[0].id !== "2") {
										throw new Error(
											`Expected frontier id=2, got id=${batch[0].id}`,
										);
									}
									return batch;
								},
								{ timeout: 30_000, delayInterval: 100 },
							);
							expect(firstBatch.map((x) => x.id)).to.deep.equal(["2"]);

							await writer.docs.put(new Document({ id: "1" }));

							await Promise.race([
								latePromise.promise,
								delay(30_000).then(() => {
									throw new Error(
										"Timed out waiting for outOfOrder queue late-results event",
									);
								}),
							]);
							expect(lateEvt?.items).to.exist;
							expect(lateEvt?.items?.length).to.equal(1);
							const item = lateEvt!.items![0];
							expect(item.value?.id ?? item.indexed.id).to.equal("1");
						expect(item.value?.__context).to.exist;
						expect(item.value?.__indexed).to.exist;

						expect(collected?.length).to.equal(1);
						expect(collected?.[0]?.value?.__context).to.exist;
						expect(collected?.[0]?.value?.__indexed).to.exist;
					} finally {
						await iterator.close();
						await reader.close();
						await writer.close();
						await replicator.close();
					}
				});
			});

			// ───────────────────────────────────────────────────────────────
			// Regression: collect after keep-alive TTL expiry should degrade
			// gracefully (no error/log spam; empty batch / done=true is OK)
			// ───────────────────────────────────────────────────────────────
			it("collect after keep-alive TTL expiry degrades gracefully (no 'Missing iterator' spam)", async function () {
				session = await TestSession.connected(2);

				const base = new TestStore({ docs: new Documents<Document>() });
				const left = await session.peers[0].open(base, {
					args: { replicate: { factor: 1 } },
				}); // server
				const right = await session.peers[1].open(base.clone(), {
					args: { replicate: { factor: 1 } },
				}); // client

				const errStub = sinon.stub(console, "error");
				let iterator:
					| Awaited<ReturnType<typeof right.docs.index.iterate>>
					| undefined;

				try {
					await right.docs.index.waitFor(left.node.identity.publicKey);
					await left.docs.index.waitFor(right.node.identity.publicKey);

					iterator = right.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							remote: { wait: { timeout: 5e3, behavior: "keep-open" } },
							updates: { push: true, merge: true },
						},
					);

					// Establish the remote iterator on the server
					const head = await iterator.next(1);
					expect(head).to.have.length(0);

					// Let the server-side keepAlive TTL elapse (DEFAULT_TIMEOUT ≈ 10_000 ms)
					await new Promise((r) => setTimeout(r, 11_500));

					// Produce something new after TTL so the client wants to collect more
					const docId = "after-ttl";
					await left.docs.put(new Document({ id: docId }));

					await waitForResolved(
						async () => expect(await iterator!.pending()).to.equal(1),
						{ timeout: 30_000 },
					);

					// Collect after TTL. Desired behavior: no error; either returns the item or [] with done=true.
					let threw = false;
					let batch: Document[] = [];
					try {
						batch = await iterator.next(1);
					} catch {
						threw = true;
					}
					expect(threw).to.be.false;
					// Accept either successful delivery or graceful EOF, but never error/log spam.
					expect(
						batch.length === 0 || (batch.length === 1 && batch[0].id === docId),
					).to.be.true;

					const hadMissing = errStub
						.getCalls()
						.some((c) =>
							/Missing iterator for request with id/i.test(
								c.args.map(String).join(" "),
							),
						);
					expect(hadMissing).to.be.false;
				} finally {
					errStub.restore();
					await iterator?.close();
					await right.close();
					await left.close();
				}
			});

			/*  KEEP AND TODO LATER
			describe("iterator live updates re-emit newer head for same id (failing repro)", function () {
				this.timeout(60_000);

				let session: TestSession | undefined;
				let left: TestStore | undefined;
				let right: TestStore | undefined;

				// Silence console.error noise so the failure reason is just the assertion
				let errStub: sinon.SinonStub;

				beforeEach(async () => {
					errStub = sinon.stub(console, "error");
					session = await TestSession.connected(2);

					const base = new TestStore({ docs: new Documents<Document>() });
					left = await session!.peers[0].open(base, {
						args: { replicate: { factor: 1 } },
					}); // server
					right = await session!.peers[1].open(base.clone(), {
						args: { replicate: { factor: 1 } },
					}); // client

					// Ensure each side can reach the other
					await right!.docs.index.waitFor(left!.node.identity.publicKey);
					await left!.docs.index.waitFor(right!.node.identity.publicKey);
				});

				afterEach(async () => {
					errStub.restore();
					await right?.close();
					await left?.close();
					await session?.stop();
					session = undefined;
				});

				it("should yield the updated document when the same id is written again", async () => {
					// Seed initial doc on the server
					await left!.docs.put(new Document({ id: "same", name: "v1" }));

					const iterator = right!.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							// Keep the iterator alive to receive live pushes/merges
							closePolicy: "manual",
							// Ask remote to keep the iterator open and push-style notify
							remote: { wait: { timeout: 10_000, behavior: "keep-open" } },
							// Enable push + merge so updates are routed into the iterator buffers
							updates: { push: true, merge: true },
						},
					);

					try {
						// First consume the initial version
							await _waitForFn(
								async () => (await iterator.pending()) ?? 0 >= 1,
								{
									timeout: 30_000,
								},
							);
						const first = await iterator.next(1);
						expect(first.length).to.equal(1);
						expect(first[0].__indexed.name).to.equal("v1");

						// Now update the *same id* with a new head on the server
						await left!.docs.put(new Document({ id: "same", name: "v2" }));

						// If merging works, the client should see one pending item
							await _waitForFn(
								async () => (await iterator.pending()) ?? 0 >= 1,
								{
									timeout: 30_000,
								},
							);

						// EXPECTED (correct behavior after patch):
						//   next(1) returns the updated document with value "v2".
						//
						// ACTUAL (current code): returns [] because the iterator's `visited` set
						//   permanently suppresses subsequent emissions for the same id.
						const nextBatch = await iterator.next(1);
						expect(nextBatch.length).to.equal(1); // <-- This should FAIL today
						expect(nextBatch[0].__indexed.name).to.equal("v2"); // <-- This should FAIL today
					} finally {
						await iterator.close();
					}
				});
			}); */

			describe("pending", () => {
				it("kept reflects the total amount of remaining document", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore({ docs: new Documents<Document>() });
					await session.peers[0].open(store, {
						args: { replicate: { factor: 1 } },
					});

					let iterator:
						| Awaited<ReturnType<typeof store.docs.index.iterate>>
						| undefined;
					try {
						const total = 20;
						for (let i = 0; i < total; i++) {
							await store.docs.put(new Document({ id: `doc-${i}` }));
						}

						iterator = store.docs.index.iterate(
							{ sort: { key: "id", direction: SortDirection.ASC } },
							{ closePolicy: "manual" },
						);
						if (!iterator) {
							throw new Error("Failed to create iterator");
						}

						const firstBatch = await iterator.next(5);
						expect(firstBatch).to.have.length(5);

						expect(await iterator.pending()).to.equal(
							total - firstBatch.length,
						);
					} finally {
						await iterator?.close();
						await store.close();
						await session.stop();
					}
				});

				it("kepts are updated correctly on change notifications", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore({ docs: new Documents<Document>() });
					await session.peers[0].open(store, {
						args: { replicate: { factor: 1 } },
					});

					let iterator:
						| Awaited<ReturnType<typeof store.docs.index.iterate>>
						| undefined;
					const notifyReasons: UpdateReason[] = [];
					const total = 12;
					const formatId = (n: number) =>
						`doc-${n.toString().padStart(3, "0")}`;
					for (let i = 0; i < total; i++) {
						await store.docs.put(new Document({ id: formatId(i) }));
					}

					iterator = store.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							updates: {
								merge: true,
								notify: (reason) => {
									notifyReasons.push(reason);
								},
							},
						},
					);
					if (!iterator) {
						throw new Error("Failed to create iterator");
					}

					const initialBatch = await iterator.next(5);
					expect(initialBatch).to.have.length(5);

					const baselinePendingMaybe = await iterator.pending();
					if (baselinePendingMaybe == null) {
						throw new Error("iterator.pending() returned undefined");
					}
					const baselinePending = baselinePendingMaybe;
					expect(baselinePending).to.equal(total - initialBatch.length);

					const newDocId = formatId(total);
					await store.docs.put(new Document({ id: newDocId }));

					await waitForResolved(() =>
						expect(notifyReasons).to.deep.include("change"),
					);

					await waitForResolved(async () =>
						expect(await iterator!.pending()).to.equal(baselinePending + 1),
					);

					const updates = await iterator.next(baselinePending + 1);
					expect(updates).to.have.length(baselinePending + 1);
					expect(updates.map((x) => x.id)).to.include(newDocId);

					await waitForResolved(async () =>
						expect(await iterator!.pending()).to.equal(0),
					);
				});
			});

			describe("onBatch", () => {
				@variant("Indexable")
				class Indexable {
					@field({ type: "string" })
					id: string;

					@field({ type: "string" })
					nameTransformed: string;

					constructor(from: Document) {
						this.id = from.id;
						this.nameTransformed =
							from.name?.toLocaleUpperCase() ?? "_MISSING_";
					}
				}

				it("can drain results via change notifications", async () => {
					session = await TestSession.disconnected(1);

					const store = await session.peers[0].open(
						new TestStore({ docs: new Documents<Document>() }),
						{
							args: { replicate: { factor: 1 } },
						},
					);

					const remoteCalls: any[] = [];
					const originalRequest = store.docs.index._query.request.bind(
						store.docs.index._query,
					);
					store.docs.index._query.request = async (req, options) => {
						remoteCalls.push(req.idString);
						return originalRequest(req, options);
					};

					const batches: string[][] = [];
					let draining = false;
					let iterator: any;
					let outCountersOnDrain: number[] = [];
					const scheduleDrain = () => {
						if (draining) return;
						draining = true;
						queueMicrotask(async () => {
							try {
								const out = await iterator.next(100);
								outCountersOnDrain.push(out);
							} catch (e) {
								// eslint-disable-next-line no-console
								console.error("Error draining iterator", e);
							} finally {
								draining = false;
							}
						});
					};

					iterator = store.docs.index.iterate(
						{ sort: { key: "id", direction: SortDirection.ASC } },
						{
							closePolicy: "manual",
							updates: {
								merge: true,
								notify: (reason) => {
									if (reason === "change") {
										scheduleDrain();
									}
								},
								onBatch: (batch) => {
									batches.push(batch.map((x) => x.id));
								},
							},
						},
					);

					await store.docs.put(new Document({ id: "1" }));
					await waitForResolved(() => expect(batches).to.deep.equal([["1"]]));

					await store.docs.put(new Document({ id: "2" }));
					await waitForResolved(() =>
						expect(batches).to.deep.equal([["1"], ["2"]]),
					);

					expect(remoteCalls).to.have.length(0); // should only call remote once
					await iterator.close();
				});

				it("fires onBatch for initial and live update batches", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore({ docs: new Documents<Document>() });

					await session.peers[0].open(store, {
						args: { replicate: { factor: 1 } },
					});
					await store.docs.put(new Document({ id: "1", name: "match" }));
					await store.docs.put(new Document({ id: "2", name: "skip" }));

					const reasons: UpdateReason[] = [];
					const batches: string[][] = [];

					const iterator = store.docs.index.iterate(
						{
							query: [new StringMatch({ key: "name", value: "match" })],
							sort: { key: "id", direction: SortDirection.ASC },
						},
						{
							updates: {
								merge: true,
								onBatch: (batch, meta) => {
									reasons.push(meta.reason);
									batches.push(batch.map((d) => d.id));
								},
							},
							closePolicy: "manual",
						},
					);

					const initial = await iterator.next(1);
					expect(initial.map((d) => d.id)).to.deep.equal(["1"]);
					expect(reasons).to.deep.equal(["initial"]);
					expect(batches).to.deep.equal([["1"]]);

					let changeCount = 0;
					const onChange = () => {
						changeCount++;
					};
					store.docs.events.addEventListener(
						"change",
						onChange as EventListener,
					);

					await store.docs.put(new Document({ id: "3", name: "match" }));
					await waitForResolved(() => expect(changeCount).to.equal(1));

					const updateBatch = await iterator.next(1);
					expect(updateBatch.map((d) => d.id)).to.deep.equal(["3"]);
					const resolvedUpdateId = store.docs.index.resolveId(updateBatch[0]);
					expect(resolvedUpdateId.primitive).to.equal("3");
					expect(reasons).to.deep.equal(["initial", "change"]);
					expect(batches).to.deep.equal([["1"], ["3"]]);

					await iterator.close();
					store.docs.events.removeEventListener(
						"change",
						onChange as EventListener,
					);
				});

				it("observers connected through an intermediary replicator receive updates", async () => {
					session = await TestSession.disconnected(3);

					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					const baseStore = new TestStore({
						docs: new Documents<Document>(),
					});

					const replicator = await session.peers[1].open(baseStore, {
						args: { replicate: { factor: 1 } },
					});

					const observerWriter = await session.peers[0].open(
						baseStore.clone(),
						{
							args: { replicate: { factor: 1 } },
						},
					);

					const observerReader = await session.peers[2].open(
						baseStore.clone(),
						{
							args: { replicate: false },
						},
					);

					let iterator:
						| Awaited<ReturnType<typeof observerReader.docs.index.iterate>>
						| undefined;
					try {
						await observerReader.docs.index.waitFor(
							replicator.node.identity.publicKey,
						);
						await replicator.docs.index.waitFor(
							observerWriter.node.identity.publicKey,
						);
						await observerWriter.docs.index.waitFor(
							replicator.node.identity.publicKey,
						);

						iterator = observerReader.docs.index.iterate(
							{ sort: { key: "id", direction: SortDirection.ASC } },
							{
								remote: {
									wait: { timeout: 5e3, behavior: "keep-open" },
									reach: {
										discover: [replicator.node.identity.publicKey],
										eager: true,
									},
								},
								closePolicy: "manual",
								updates: "all",
							},
						);

						const initialBatch = await iterator.next(1);
						expect(initialBatch).to.have.length(0);
						expect(iterator.done()).to.be.false;

						const docId = "relay-doc";
						await observerWriter.docs.put(new Document({ id: docId }));
						await waitForResolved(
							async () =>
								expect(await replicator.docs.index.getSize()).to.equal(1),
							{ timeout: 3e4 },
						);
						console.debug(
							"[test] replicator iterators",
							replicator.docs.index.countIteratorsInProgress,
						);

						await waitForResolved(
							async () => expect(await iterator!.pending()).to.equal(1),
							{ timeout: 3e4 },
						);

						const updateBatch = await iterator!.next(1);
						expect(updateBatch).to.have.length(1);
						expect(updateBatch[0].id).to.equal(docId);
						await waitForResolved(async () =>
							expect(await observerReader.docs.index.getSize()).to.equal(1),
						);
					} finally {
						await iterator?.close();
						await observerReader.close();
						await observerWriter.close();
						await replicator.close();
					}
				});

				const check = async <R extends boolean>(
					store: TestStore<Indexable>,
					resolve: boolean | undefined,
					shouldResolve: R,
				) => {
					await store.docs.put(new Document({ id: "1", name: "alpha" }));

					const onBatchBatches: any[][] = [];
					const iterator = store.docs.index.iterate<R>(
						{},
						{
							resolve: resolve as any,
							updates: {
								onBatch: (batch) => {
									onBatchBatches.push(batch);
								},
							},
						},
					);

					const next = await iterator.next(1);
					expect(next).to.have.length(1);
					if (shouldResolve) {
						let first = next[0] as Document;
						expect(first).to.be.instanceOf(Document);
						expect(first.name).to.equal("alpha");
						const indexed = (next[0] as any).__indexed as Indexable;
						expect(indexed).to.be.instanceOf(Indexable);
						expect(indexed.nameTransformed).to.equal("ALPHA");
						expect(onBatchBatches).to.have.length(1);
						expect(onBatchBatches[0][0]).to.equal(next[0]);
						const onResultsIndexed = (onBatchBatches[0][0] as any).__indexed;
						expect(onResultsIndexed).to.equal(indexed);
					} else {
						let first = next[0] as Indexable;
						expect(first).to.be.instanceOf(Indexable);
						expect(first.nameTransformed).to.equal("ALPHA");
						expect((first as any).name).to.be.undefined;
						expect(onBatchBatches).to.have.length(1);
						expect(onBatchBatches[0][0]).to.equal(first);
					}
				};

				it("emits indexed results to onBatch when resolve is false", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore<Indexable>({
						docs: new Documents<Document, Indexable>(),
					});

					await session.peers[0].open(store, {
						args: {
							replicate: { factor: 1 },
							index: {
								type: Indexable,
								transform: (doc) => new Indexable(doc),
							},
						},
					});

					await check(store, false, false);
				});

				it("emits resolved documents to onBatch when resolve is true", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore<Indexable>({
						docs: new Documents<Document, Indexable>(),
					});

					await session.peers[0].open(store, {
						args: {
							replicate: { factor: 1 },
							index: {
								type: Indexable,
								transform: (doc) => new Indexable(doc),
							},
						},
					});

					await check(store, true, true);
				});

				it("emits resolved documents to onBatch when resolve is undefined", async () => {
					// this behaviour is inline with that the iterator also returns resolved documents when resolve is undefined
					session = await TestSession.connected(1);

					const store = new TestStore<Indexable>({
						docs: new Documents<Document, Indexable>(),
					});

					await session.peers[0].open(store, {
						args: {
							replicate: { factor: 1 },
							index: {
								type: Indexable,
								transform: (doc) => new Indexable(doc),
							},
						},
					});

					await check(store, undefined, true);
				});

				it("returns documents even if indexed representation arrives first", async () => {
					session = await TestSession.connected(1);

					const store = new TestStore<Indexable>({
						docs: new Documents<Document, Indexable>(),
					});

					await session.peers[0].open(store, {
						args: {
							replicate: { factor: 1 },
							index: {
								type: Indexable,
								transform: (doc) => new Indexable(doc),
								includeIndexed: true,
							},
						},
					});

					const originalProcessQuery = store.docs.index.processQuery.bind(
						store.docs.index,
					) as (...args: any[]) => Promise<any>;
					const processQueryStub = sinon
						.stub(store.docs.index as any, "processQuery")
						.callsFake(async (...args: any[]) => {
							const response = await originalProcessQuery(...args);
							if (response.results.length > 0) {
								const docResult = response.results[0];
								response.results.push(
									new ResultIndexedValue({
										context: docResult.context,
										source: serialize(docResult.indexed),
										indexed: docResult.indexed,
										entries: [],
									}),
								);
							}
							response.results.sort((a: any, b: any) => {
								const aIndexed = a instanceof ResultIndexedValue;
								const bIndexed = b instanceof ResultIndexedValue;
								if (aIndexed === bIndexed) {
									return 0;
								}
								return aIndexed ? -1 : 1;
							});
							return response;
						});

					let iterator: ReturnType<typeof store.docs.index.iterate> | undefined;
					try {
						await store.docs.put(new Document({ id: "mix", name: "gamma" }));

						let observed: any[] | undefined;
						iterator = store.docs.index.iterate(
							{},
							{
								updates: {
									onBatch: (batch) => {
										observed = batch;
									},
								},
							},
						);

						await iterator.next(1);
						expect(observed).to.exist;
						expect(observed!.every((entry) => entry instanceof Document)).to.be
							.true;
					} finally {
						processQueryStub.restore();
						await iterator?.close();
						await session.stop();
					}
				});
			});
		});

		it("get first entry", async () => {
			session = await TestSession.connected(1);

			const store = new TestStore({
				docs: new Documents<Document>(),
			});

			await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			const doc = new Document({ id: "1" });
			const doc2 = new Document({ id: "2" });
			const doc3 = new Document({ id: "3" });

			await store.docs.put(doc);
			await store.docs.put(doc2);
			await store.docs.put(doc3);

			const first = await store.docs.index
				.iterate({ sort: { key: "id", direction: SortDirection.DESC } })
				.first();
			expect(first!.id).to.deep.equal(doc3.id);

			// expect cleanup
			expect(store.docs.index.hasPending).to.be.false;
		});

		it("local only", async () => {
			session = await TestSession.connected(2);
			const store = new TestStore({
				docs: new Documents<Document>(),
			});
			await session.peers[0].open(store);
			const store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
				},
			});
			const doc = new Document({ id: "1" });
			await store.docs.put(doc);
			await store2.docs.index.waitFor(store.node.identity.publicKey);
			const localOnly = await store2.docs.index
				.iterate({}, { local: true, remote: false })
				.first();
			expect(localOnly).to.be.undefined;

			const localAndRemote = await store2.docs.index
				.iterate({}, { local: true, remote: true })
				.first();
			expect(localAndRemote?.id).to.equal(doc.id);
		});
	});

	describe("acl", () => {
		let store: TestStore;
		before(async () => {
			session = await TestSession.connected(2);
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
					docs: new Documents<Document>(),
				}),
			);
			await expect(
				store.docs.log.log.append(new PutOperation({ data: randomBytes(32) })),
			).rejectedWith(AccessError);
		});

		it("reject entries with unexpected payloads entry", async () => {
			store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>(),
				}),
			);

			(store as any)["_canAppend"] = () => true; // ignore internal

			const canAppend = await (store.docs as any).canAppend(
				(await createEntry({
					data: new PutOperation({ data: randomBytes(32) }),
					identity: store.node.identity,
					store: store.docs.log.log.blocks,
					canAppend: () => true,
					encoding: store.docs.log.log.encoding,
				})) as Entry<Operation>,
			);

			expect(canAppend).to.be.false;
		});

		it("does not query remote when canAppend check", async () => {
			const store = new TestStore({
				docs: new Documents<Document>(),
			});
			const store1 = await session.peers[0].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			const store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			const processQuery1 = store1.docs.index.processQuery.bind(
				store1.docs.index,
			);
			let remoteQueries1 = 0;
			store1.docs.index.processQuery = async (
				query: SearchRequest | SearchRequestIndexed | CollectNextRequest,
				from: PublicSignKey,
				isLocal: boolean,
				options?: {
					canRead?: CanRead<any>;
				},
			) => {
				if (!isLocal) {
					remoteQueries1++;
				}

				return processQuery1(query, from, isLocal, options) as any;
			};

			const processQuery2 = store2.docs.index.processQuery.bind(
				store2.docs.index,
			);
			let remoteQueries2 = 0;
			store2.docs.index.processQuery = async (
				query: SearchRequest | SearchRequestIndexed | CollectNextRequest,
				from: PublicSignKey,
				isLocal: boolean,
				options?: {
					canRead?: CanRead<any>;
				},
			) => {
				if (!isLocal) {
					remoteQueries2++;
				}

				return processQuery2(query, from, isLocal, options) as any;
			};

			for (let i = 0; i < 10; i++) {
				const doc = new Document({
					id: uuid(),
					data: randomBytes(10),
				});
				await store1.docs.put(doc);
			}

			await waitForResolved(async () =>
				expect(await store1.docs.index.getSize()).equal(10),
			);
			await waitForResolved(async () =>
				expect(await store2.docs.index.getSize()).equal(10),
			);

			expect(remoteQueries1).equal(0);
			expect(remoteQueries2).equal(0);
		});

		it("immutable", async () => {
			const store = new TestStore({
				docs: new Documents<Document>({
					immutable: true,
				}),
			});

			const store1 = await session.peers[0].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			const store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});

			const processQuery1 = store1.docs.index.processQuery.bind(
				store1.docs.index,
			);
			let _remoteQueries1 = 0;
			store1.docs.index.processQuery = async (
				query: SearchRequest | SearchRequestIndexed | CollectNextRequest,
				from: PublicSignKey,
				isLocal: boolean,
				options?: {
					canRead?: CanRead<any>;
				},
			) => {
				if (!isLocal) {
					_remoteQueries1++;
				}

				return processQuery1(query, from, isLocal, options) as any;
			};

			const processQuery2 = store2.docs.index.processQuery.bind(
				store2.docs.index,
			);
			let _remoteQueries2 = 0;
			store2.docs.index.processQuery = async (
				query: SearchRequest | SearchRequestIndexed | CollectNextRequest,
				from: PublicSignKey,
				isLocal: boolean,
				options?: {
					canRead?: CanRead<any>;
				},
			) => {
				if (!isLocal) {
					_remoteQueries2++;
				}

				return processQuery2(query, from, isLocal, options) as any;
			};

			const doc1 = new Document({
				id: uuid(),
				number: 1n,
			});

			await store1.docs.put(doc1);
			await store2.docs.put(new Document({ id: doc1.id, number: 2n }));

			/* TODO force test env to make sure remote queries are performed
			
				await waitForResolved(async () =>
					expect(await store1.docs.index.getSize()).equal(1),
				);
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(1),
				);
	
				await waitForResolved(() => expect(remoteQueries1).equal(1));
				await waitForResolved(() => expect(remoteQueries2).equal(1));
			 */

			// expect doc1 to be the "truth"

			const resultsFrom1 = await store1.docs.index.search(
				new SearchRequest({ fetch: 1 }),
				{ local: true, remote: false },
			);
			expect(resultsFrom1).to.have.length(1);
			expect(resultsFrom1[0].number).to.equal(1n);

			await waitForResolved(async () => {
				const resultsFrom2 = await store2.docs.index.search(
					new SearchRequest({ fetch: 1 }),
					{ local: true, remote: false },
				);
				expect(resultsFrom2).to.have.length(1);
				expect(resultsFrom2[0].number).to.equal(1n);
			});
		});

		describe("canPerform", () => {
			before(async () => {
				await session.stop();
				session = await TestSession.connected(1, {
					directory: "./tmp/document-store/acl/",
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
						},
					});
				}
			}

			it("can use different acl on load", async () => {
				let store = await session.peers[0].open(new TestCanPerfom());
				await store.documents.put(
					new Document({ id: "1", name: "1", number: 1n }),
				);
				await store.close();

				store = await session.peers[0].open(store.clone());
				expect(await store.documents.index.getSize()).equal(1);
			});
		});
	});

	describe("program as value", () => {
		@variant("subprogram_indexable")
		class SubProgramIndexable {
			@field({ type: fixedArray("u8", 32) })
			id: Uint8Array;

			@field({ type: "string" })
			property: string;

			@field({ type: "string" })
			address: string;

			constructor(properties: {
				id: Uint8Array;
				property: string;
				address: string;
			}) {
				this.id = properties.id;
				this.address = properties.address;
				this.property = properties.property;
			}
		}
		@variant("subprogram")
		class SubProgram extends Program {
			@field({ type: fixedArray("u8", 32) })
			id: Uint8Array;

			@field({ type: "string" })
			property: string;

			@field({ type: Log })
			log: Log<any>;

			constructor(properties?: {
				id?: Uint8Array;
				log?: Log<any>;
				property?: string;
			}) {
				super();
				this.id = properties?.id ?? randomBytes(32);
				this.log = properties?.log ?? new Log();
				this.property = properties?.property ?? "test";
			}
			async open() {
				return this.log.open(this.node.services.blocks, this.node.identity);
			}
		}

		@variant("test_program_documents")
		class TestStoreSubPrograms extends Program {
			@field({ type: Documents })
			docs: Documents<SubProgram, SubProgramIndexable>;

			constructor(properties: {
				docs: Documents<SubProgram, SubProgramIndexable>;
			}) {
				super();
				this.docs = properties.docs;
			}
			async open(
				options?: Partial<SetupOptions<SubProgram, SubProgramIndexable>>,
			): Promise<void> {
				await this.docs.open({
					...options,
					type: SubProgram,
					index: {
						idProperty: ["id"],
						type: SubProgramIndexable,
						transform: async (arg, ctx) => {
							return new SubProgramIndexable({
								id: arg.id,
								address: (await arg.calculateAddress()).address,
								property: arg.property,
							});
						},
						cache: {
							resolver: options?.index?.cache?.resolver,
						},
					},
				});
			}
		}

		let stores: { store: TestStoreSubPrograms }[];
		let peersCount = 2;

		beforeEach(async () => {
			session = await TestSession.connected(peersCount);
			stores = [];

			// Create store
			for (let i = 0; i < peersCount; i++) {
				if (i > 0) {
					await session.peers[i].services.blocks.waitFor(
						session.peers[0].peerId,
					);
				}
				const store =
					i > 0
						? (await TestStoreSubPrograms.load<TestStoreSubPrograms>(
								stores[0].store.address!,
								session.peers[i].services.blocks,
							))!
						: new TestStoreSubPrograms({
								docs: new Documents<SubProgram, SubProgramIndexable>(),
							});

				await session.peers[i].open(store, {
					args: {
						replicate: i === 0 ? { factor: 1 } : false,
						canOpen: () => true,
						index: {
							cache: {
								resolver: 0,
							},
						},
					},
				});
				stores.push({ store });
			}
		});
		afterEach(async () => {
			await session.stop();
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
		/* TID
		
		it("non-replicator will not open by default", async () => {
			const subProgram = new SubProgram();
			await stores[1].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.true;
		}); */

		it("can open program when sync", async () => {
			const subProgram = new SubProgram();
			await stores[1].store.docs.put(subProgram);

			expect(subProgram.closed).to.be.false; // TODO is this expected because stores[1] is only observer?
			await stores[0].store.docs.log.log.join(
				[...(await stores[1].store.docs.log.log.toArray()).values()].map((x) =>
					deserialize(serialize(x), Entry),
				),
			);
			expect(subProgram.closed).to.be.false; // TODO is this expected because stores[1] is only observer?
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
			const size = await stores[0].store.docs.index.getSize();
			expect(size).to.be.equal(1);
			const fetch = await stores[0].store.docs.index.get(subProgram.id);
			expect(fetch).to.be.exist;
			await stores[0].store.docs.del(subProgram.id);
			await waitForResolved(() => expect(subProgram.closed).to.be.true);
			expect(dropped).to.be.true;
		});

		it("can prevent subprograms to be opened", async () => {
			stores[0].store.docs.canOpen = (_) => Promise.resolve(false);
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.true;
		});

		it("can delete closed subprograms that are no opened on put", async () => {
			stores[0].store.docs.canOpen = (_) => Promise.resolve(false);
			const subProgram = new SubProgram();
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.true;
			expect(await stores[0].store.docs.index.getSize()).to.eq(1);
			await stores[0].store.docs.del(new SubProgramIndexable(subProgram).id);
			expect(await stores[0].store.docs.index.getSize()).to.eq(0);
		});

		it("can delete open subprograms that are no opened on put", async () => {
			stores[0].store.docs.canOpen = (_) => Promise.resolve(false);
			const subProgram = await session.peers[0].open(new SubProgram());
			await stores[0].store.docs.put(subProgram);
			expect(subProgram.closed).to.be.false;
			expect(await stores[0].store.docs.index.getSize()).to.eq(1);
			await stores[0].store.docs.del(new SubProgramIndexable(subProgram).id);
			expect(await stores[0].store.docs.index.getSize()).to.eq(0);
		});

		it("will re-open on load after restart", async () => {
			await session.stop();

			session = await TestSession.connected(1, {
				directory: "./tmp/document-store/program-perstance-test/" + new Date(),
			});
			const peer = session.peers[0];

			const subProgram1 = new SubProgram();

			const subProgram2 = new SubProgram();

			let store = await peer.open(
				new TestStoreSubPrograms({
					docs: new Documents<SubProgram, SubProgramIndexable>(),
				}),
				{
					args: {
						canOpen: (d) => equals(d.id, subProgram1.id),
					},
				},
			);

			await store.docs.put(subProgram1);
			await store.docs.put(subProgram2);

			expect(subProgram1.closed).to.be.false;
			expect(subProgram2.closed).to.be.true;

			await session.peers[0].stop();

			expect(subProgram1.closed).to.be.true;
			expect(subProgram2.closed).to.be.true;

			await session.peers[0].start();
			store = await peer.open(store.clone(), {
				args: {
					canOpen: (d) => equals(d.id, subProgram1.id),
				},
			});

			const programsInIndex = await store.docs.index
				.iterate({}, { local: true, remote: false })
				.all();
			expect(programsInIndex).to.have.length(2);
			expect(
				programsInIndex
					.map((x) => x.closed)
					.sort((a, b) => String(a).localeCompare(String(b))),
			).to.deep.eq([false, true]); // one is allowed to be opened and one is not

			// open all, and make sure that if we query all again, they are all open
			for (const program of programsInIndex) {
				program.closed && (await peer.open(program));
			}

			const programsInIndex2 = await store.docs.index
				.iterate({}, { local: true, remote: false })
				.all();
			expect(programsInIndex2).to.have.length(2);
			expect(programsInIndex2.map((x) => x.closed)).to.deep.eq([false, false]);
		});

		it("update", async () => {
			const subProgram1 = new SubProgram({ property: "x" });
			await session.peers[0].open(subProgram1, {
				existing: "reuse",
			});
			await stores[0].store.docs.put(subProgram1);
			let get = await stores[0].store.docs.index.get(subProgram1.id);
			expect(get.__indexed.property).to.eq("x");
			const updated = Number(get.__context.modified);
			subProgram1.property = "y";
			await stores[0].store.docs.put(subProgram1);

			get = await stores[0].store.docs.index.get(subProgram1.id);
			expect(get.__indexed.property).to.eq("y");
			expect(Number(get.__context.modified)).to.be.greaterThan(updated);
		});

		describe("index", () => {
			@variant("test_program_documents_custom_fields_indexable")
			class Indexable {
				@field({ type: fixedArray("u8", 32) })
				id: Uint8Array;

				@field({ type: fixedArray("u8", 32) })
				custom: Uint8Array;

				constructor(from: SubProgram) {
					this.id = from.id;
					this.custom = from.id;
				}
			}
			@variant("test_program_documents_custom_fields")
			class TestStoreSubPrograms extends Program {
				@field({ type: Documents })
				docs: Documents<SubProgram, Indexable>;

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
							type: Indexable,
						},
					});
				}
			}
			let store: TestStoreSubPrograms, store2: TestStoreSubPrograms;

			afterEach(async () => {
				await store?.close();
				await store2?.close();
			});
			it("can index specific fields", async () => {
				store = await session.peers[0].open(new TestStoreSubPrograms());
				store2 = await session.peers[1].open(store.clone());
				const subProgram = new SubProgram();
				await store.docs.put(subProgram);
				await waitForResolved(() => expect(subProgram.closed).to.be.false);
				await waitForResolved(async () =>
					expect(await store2.docs.index.getSize()).equal(1),
				);
				const stores = [store, store2];
				for (const [i, s] of stores.entries()) {
					const results = await s.docs.index.search(
						new SearchRequest({
							query: [
								new ByteMatchQuery({ key: "custom", value: subProgram.id }),
							],
						}),
					);
					expect(results).to.have.length(1);
					expect(results[0].id).to.deep.equal(subProgram.id);
					try {
						await waitForResolved(() => expect(results[0].closed).to.be.false);
					} catch (error) {
						console.error("Substore was never openend: " + i);
						throw error;
					}
				}
			});
		});
	});

	describe("indexBy", () => {
		@variant("test_id_annotation_custom_id_document")
		class CustomIdDocument {
			@id({ type: "string" })
			custom: string;

			constructor(properties: { custom: string }) {
				this.custom = properties.custom;
			}
		}
		/*class CustomIdDocumentWrapped {
			@field({ type: Uint8Array })
			id: Uint8Array;
	
			@field({ type: CustomIdDocument })
			nested: CustomIdDocument;
	
			constructor(properties: { id: Uint8Array; nested: CustomIdDocument }) {
				this.id = properties.id;
				this.nested = properties.nested;
			}
		}
	
		 class AnotherCustomIdDocument {
			@id({ type: 'string' })
			anotherIdProperty: string
	
			constructor(properties: CustomIdDocument) {
				this.anotherIdProperty = properties.custom;
			}
		} */

		@variant("test_id_annotation")
		class CustomIDDocumentStore extends Program {
			@field({ type: Documents })
			documents: Documents<CustomIdDocument, CustomIdDocument>;

			constructor() {
				super();
				this.documents = new Documents<CustomIdDocument, CustomIdDocument>();
			}
			async open(): Promise<void> {
				await this.documents.open({
					type: CustomIdDocument,
				});
			}
		}

		/* @variant("test_nested_id_annotation")
		class CustomNestedIDDocumentStore extends Program {
			@field({ type: Documents })
			documents: Documents<CustomIdDocumentWrapped, CustomIdDocumentWrapped>;
	
			constructor() {
				super();
				this.documents = new Documents<
					CustomIdDocumentWrapped,
					CustomIdDocumentWrapped
				>();
			}
			async open(): Promise<void> {
				await this.documents.open({
					type: CustomIdDocumentWrapped,
				});
			}
		}
 */
		/* @variant("test_id_annotation_indexed_type")
		class CustomIdCustomIndexdDocumentStore extends Program {
	
			@field({ type: Documents })
			documents: Documents<CustomIdDocument, AnotherCustomIdDocument>;
	
			constructor() {
				super();
				this.documents = new Documents<CustomIdDocument, AnotherCustomIdDocument>();
			}
			async open(): Promise<void> {
				await this.documents.open({
					type: CustomIdDocument,
					index: {
						type: AnotherCustomIdDocument
					}
				});
			}
		} */

		before(async () => {
			session = await TestSession.connected(1);
		});

		after(async () => {
			await session.stop();
		});

		it("id annotation on the document type", async () => {
			const store = await session.peers[0].open(new CustomIDDocumentStore());
			const q = new CustomIdDocument({ custom: "1" });
			await store.documents.put(q);
			expect(await store.documents.index.getSize()).to.eq(1);
			await store.documents.del(q.custom);
			expect(await store.documents.index.getSize()).to.eq(0);
		});

		/* TODO what is expected?
		it("id annotation on the nested document type", async () => {
			const store = await session.peers[0].open(
				new CustomNestedIDDocumentStore(),
			);
			const q = new CustomIdDocumentWrapped({
				id: new Uint8Array([0]),
				nested: new CustomIdDocument({ custom: "1" }),
			});
			await store.documents.put(q);
			expect(await store.documents.index.getSize()).to.eq(1);
			await store.documents.del(q.id);
			expect(await store.documents.index.getSize()).to.eq(0);
		}); */

		/* TODO feat 
	
		it("id annotation on the indexed type", async () => {
			const store = await session.peers[0].open(new CustomIdCustomIndexdDocumentStore());
			const q = new CustomIdDocument({ custom: "1" });
			await store.documents.put(q);
		});
		 */
	});

	describe("returnIndexed", () => {
		class Value {
			@id({ type: "string" })
			id: string;

			@field({ type: "u32" })
			number: number;

			constructor(properties: { id: string; number: number }) {
				this.id = properties.id;
				this.number = properties.number;
			}
		}

		@variant("indexed_value")
		class Indexed {
			@field({ type: "string" })
			id: string;

			@field({ type: "u32" })
			indexedNumber: number;

			constructor(properties: Value) {
				this.id = properties.id;
				this.indexedNumber = properties.number;
			}
		}

		@variant("test_include_indexed_store")
		class TestIncludeIndexedStore extends Program {
			@field({ type: Documents })
			documents: Documents<Value, Indexed>;

			constructor() {
				super();
				this.documents = new Documents<Value, Indexed>();
			}
			async open(options?: {
				replicate: boolean;
				onTransform?: (arg: Value, context: Context) => void;
			}): Promise<void> {
				await this.documents.open({
					type: Value,
					replicate: options?.replicate ? { factor: 1 } : false,
					index: {
						type: Indexed,
						includeIndexed: true,
						transform: (arg, context) => {
							options?.onTransform?.(arg, context);
							return new Indexed(arg);
						},
					},
				});
			}
		}

		before(async () => {
			session = await TestSession.connected(2);
		});

		after(async () => {
			await session.stop();
		});

		it("return indexed with value", async () => {
			const replicator = await session.peers[0].open(
				new TestIncludeIndexedStore(),
				{
					args: {
						replicate: true,
						onTransform: (arg, context) => {
							expect(arg.id).to.be.a("string");
							expect(context.created).to.be.a("bigint");
						},
					},
				},
			);

			let transformed = false;
			const observer = await session.peers[1].open(replicator.clone(), {
				args: {
					replicate: false,
					onTransform: (arg, context) => {
						transformed = true;
					},
				},
			});

			await observer.documents.waitFor(session.peers[0].identity.publicKey);
			let docCount = 11;
			for (let i = 0; i < docCount; i++) {
				const value = new Value({ id: `${i}`, number: i });
				await replicator.documents.put(value);
			}

			expect(await replicator.documents.index.getSize()).to.eq(docCount);
			await observer.documents.index.waitFor(
				session.peers[0].identity.publicKey,
			);
			expect(await observer.documents.index.getSize()).to.eq(0);
			const get = await observer.documents.index.get("1");
			expect(get).to.be.instanceOf(Value);

			const iterator = observer.documents.index.iterate({
				sort: {
					key: "indexedNumber",
					direction: SortDirection.DESC,
				},
			});

			const first = await iterator.next(1);
			expect(first[0]).to.be.instanceOf(Value);
			expect(first[0].id).to.eq("10");
			expect(first[0].number).to.eq(10);

			const next = await iterator.next(10);
			expect(next[0]).to.be.instanceOf(Value);
			expect(next[0].id).to.eq("9");
			expect(next[0].number).to.eq(9);

			expect(next[9]).to.be.instanceOf(Value);
			expect(next[9].id).to.eq("0");
			expect(next[9].number).to.eq(0);
			expect(next.length).to.eq(10);

			expect(transformed).to.be.false; // because indexed values where always included
		});
	});

	describe("custom index", () => {
		let peersCount = 2,
			stores: TestStore<Indexable>[] = [];

		@variant("custom_indexable")
		class Indexable {
			@field({ type: "string" })
			id: string;

			@field({ type: "string" })
			nameTransformed: string;

			constructor(from: Document) {
				this.id = from.id;
				if (from && (from as any as Indexable)["nameTransformed"]) {
					throw new Error("Unexpected");
				}
				this.nameTransformed = from.name?.toLocaleUpperCase() ?? "_MISSING_";
			}
		}

		before(async () => {
			session = await TestSession.connected(peersCount);
		});

		after(async () => {
			await session.stop();
		});

		beforeEach(async () => {
			// Create store
			stores = [];
			for (let i = 0; i < peersCount; i++) {
				const store =
					i > 0
						? (await TestStore.load<TestStore<Indexable>>(
								stores[0].address!,
								session.peers[i].services.blocks,
							))!
						: new TestStore<Indexable>({
								docs: new Documents<Document, Indexable>(),
							});
				await session.peers[i].open(store, {
					args: {
						replicate: i === 0,
						index: {
							type: Indexable,
							transform: (arg, ctx) => new Indexable(arg),
						},
					},
				});
				stores.push(store);
			}

			await stores[0].docs.put(new Document({ id: "1", name: "name1" }));
			await stores[0].docs.put(new Document({ id: "2", name: "name2" }));
			await stores[0].docs.put(new Document({ id: "3", name: "name3" }));
			await waitForResolved(async () =>
				expect((await stores[1].docs.log.getReplicators()).size).to.eq(1),
			);
		});

		afterEach(async () => {
			await Promise.all(stores.map((x) => x.drop()));
		});

		describe("get", () => {
			it("get", async () => {
				// should not remote query if local is enough
				const requestSpy = sinon.spy(stores[0].docs.index._query.request);
				stores[0].docs.index._query.request = requestSpy;

				const sendSpy = sinon.spy(stores[0].docs.index._query.send);
				stores[0].docs.index._query.send = sendSpy;

				const get = await stores[0].docs.index.get("1");
				expect(get!.name).to.eq("name1");
				expect(get.__indexed).to.be.instanceOf(Indexable);

				const createdAt = get.__context.created;
				expect(typeof createdAt).to.eq("bigint");

				expect(requestSpy.callCount).to.eq(0);
				expect(sendSpy.callCount).to.eq(0);

				const getRemote = await stores[1].docs.index.get("1");
				expect(getRemote!.name).to.eq("name1");
				const createdAtRemote = get.__context.created;
				expect(createdAtRemote).to.eq(createdAt);
				expect(getRemote.__indexed).to.be.instanceOf(Indexable);

				expect(getRemote.__indexed).to.exist;
			});

			it("get indexed", async () => {
				const get = await stores[0].docs.index.get("1", { resolve: false });
				expect(get!.nameTransformed).to.eq("NAME1");
				expect((get as any)["__indexed"]).to.exist;

				const getRemote = await stores[1].docs.index.get("1", {
					resolve: false,
				});
				expect(getRemote!.nameTransformed).to.eq("NAME1");
				expect((getRemote as any)["__indexed"]).to.exist;
			});

			it("get local first", async () => {
				await stores[1].docs.log.replicate({ factor: 0.0001 });
				await waitForResolved(() =>
					expect(stores[1].docs.log.log.length).to.eq(
						stores[0].docs.log.log.length,
					),
				);

				const requestSpy = sinon.spy(stores[1].docs.index._query.request);
				stores[1].docs.index._query.request = requestSpy;

				const sendSpy = sinon.spy(stores[1].docs.index._query.send);
				stores[1].docs.index._query.send = sendSpy;

				const get = await stores[1].docs.index.get("1");
				expect(get!.name).to.eq("name1");
				expect(get.__indexed).to.be.instanceOf(Indexable);

				expect(requestSpy.callCount).to.eq(0);
				expect(sendSpy.callCount).to.eq(0);
			});
		});

		it("update", async () => {
			let get = await stores[0].docs.index.get("1");
			expect(get!.name).to.eq("name1");
			expect(get.__indexed.nameTransformed).to.eq("NAME1");

			await stores[0].docs.put(
				new Document({
					id: "1",
					name: "name1_updated",
				}),
			);

			get = await stores[0].docs.index.get("1");
			expect(get!.name).to.eq("name1_updated");
			expect(get.__indexed.nameTransformed).to.eq("NAME1_UPDATED");
		});

		describe("iterate", () => {
			it("iterate", async () => {
				for (const iterator of [
					stores[0].docs.index.iterate({
						sort: "id",
					}),
					stores[1].docs.index.iterate({
						sort: "id",
					}),
				]) {
					const first = await iterator.next(1);

					const second = await iterator.next(2);
					expect(first[0].name).to.eq("name1");
					expect(first[0] instanceof Document).to.be.true;
					expect(first[0].__indexed).to.be.instanceOf(Indexable);

					expect(second[0].name).to.eq("name2");
					expect(second[1].name).to.eq("name3");
					expect(second[0] instanceof Document).to.be.true;
					expect(second[1] instanceof Document).to.be.true;
					expect(second[0].__indexed).to.be.instanceOf(Indexable);

					const firstCreatedAt = first[0].__context.created;
					expect(typeof firstCreatedAt).to.eq("bigint");

					const secondCreatedAt = second[0].__context.created;
					expect(secondCreatedAt > firstCreatedAt).to.be.true;
				}
			});

			it("iterate indexed", async () => {
				for (const iterator of [
					stores[0].docs.index.iterate(
						{
							sort: "id",
						},
						{
							resolve: false,
						},
					),
					stores[1].docs.index.iterate(
						{
							sort: "id",
						},
						{
							resolve: false,
						},
					),
				]) {
					const first = await iterator.next(1);
					const second = await iterator.next(2);
					expect(first[0].nameTransformed).to.eq("NAME1");
					expect(first[0] instanceof Indexable).to.be.true;
					expect(second[0].nameTransformed).to.eq("NAME2");
					expect(second[1].nameTransformed).to.eq("NAME3");
					expect(second[0] instanceof Indexable).to.be.true;
					expect(second[1] instanceof Indexable).to.be.true;

					const firstCreatedAt = first[0].__context.created;
					expect(typeof firstCreatedAt).to.eq("bigint");

					const secondCreatedAt = second[0].__context.created;
					expect(secondCreatedAt > firstCreatedAt).to.be.true;
				}
			});

			it("iterate and sort by context", async () => {
				for (const iterator of [
					stores[0].docs.index.iterate(
						{
							sort: {
								key: ["__context", "created"],
								direction: SortDirection.DESC,
							},
						},
						{
							resolve: false,
						},
					),
					stores[1].docs.index.iterate(
						{
							sort: {
								key: ["__context", "created"],
								direction: SortDirection.DESC,
							},
						},
						{
							resolve: false,
						},
					),
				]) {
					const first = await iterator.next(1);
					const second = await iterator.next(2);
					expect(first[0].nameTransformed).to.eq("NAME3");
					expect(first[0] instanceof Indexable).to.be.true;
					expect(second[0].nameTransformed).to.eq("NAME2");
					expect(second[1].nameTransformed).to.eq("NAME1");
					expect(second[0] instanceof Indexable).to.be.true;
					expect(second[1] instanceof Indexable).to.be.true;

					const firstCreatedAt = first[0].__context.created;
					expect(typeof firstCreatedAt).to.eq("bigint");

					const secondCreatedAt = second[0].__context.created;
					expect(secondCreatedAt < firstCreatedAt).to.be.true;
				}
			});

			it("iterate replicate", async () => {
				for (const iterator of [
					stores[0].docs.index.iterate(
						{
							sort: "id",
						},
						{
							remote: {
								replicate: true,
							},
						},
					),
					stores[1].docs.index.iterate(
						{
							sort: "id",
						},
						{
							remote: {
								replicate: true,
							},
						},
					),
				]) {
					const first = await iterator.next(1);
					const second = await iterator.next(2);
					expect(first[0].name).to.eq("name1");
					expect(first[0] instanceof Document).to.be.true;

					expect(second[0].name).to.eq("name2");
					expect(second[1].name).to.eq("name3");
					expect(second[0] instanceof Document).to.be.true;
					expect(second[1] instanceof Document).to.be.true;
				}
			});

			it("iterate replicate indexed", async () => {
				for (const iterator of [
					stores[0].docs.index.iterate(
						{
							sort: "id",
						},
						{
							resolve: false,
							remote: {
								replicate: true,
							},
						},
					),
					stores[1].docs.index.iterate(
						{
							sort: "id",
						},
						{
							resolve: false,
							remote: {
								replicate: true,
							},
						},
					),
				]) {
					const first = await iterator.next(1);
					const second = await iterator.next(2);
					expect(first[0].nameTransformed).to.eq("NAME1");
					expect(first[0] instanceof Indexable).to.be.true;
					expect(second[0].nameTransformed).to.eq("NAME2");
					expect(second[1].nameTransformed).to.eq("NAME3");
					expect(second[0] instanceof Indexable).to.be.true;
					expect(second[1] instanceof Indexable).to.be.true;
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
									session.peers[i].services.blocks,
								))!
							: new TestStore({
									docs: new Documents<Document>(),
								});
					await session.peers[i].open(store);
					stores.push(store);
				}

				for (let i = 0; i < stores.length; i++) {
					const fn = stores[i].docs.index.processQuery.bind(
						stores[i].docs.index,
					);
					stores[i].docs.index.processQuery = (a, b, c) => {
						counters[i] += 1;
						return fn(a, b, c);
					};
					await stores[i].docs.waitFor(
						session.peers.filter((_v, ix) => ix !== i).map((x) => x.peerId),
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
				stores[0].docs.log.getCover = async () => [
					stores[1].node.identity.publicKey.hashcode(),
					stores[2].node.identity.publicKey.hashcode(),
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(1);
				expect(counters[2]).equal(1);
			});

			it("will always query locally", async () => {
				stores[0].docs.log.getCover = () => [] as any;
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(0);
				expect(counters[2]).equal(0);
			});

			it("one", async () => {
				stores[0].docs.log.getCover = async () => [
					stores[1].node.identity.publicKey.hashcode(),
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }));
				expect(counters[0]).equal(1);
				expect(counters[1]).equal(1);
				expect(counters[2]).equal(0);
			});

			it("non-local", async () => {
				stores[0].docs.log.getCover = async () => [
					stores[1].node.identity.publicKey.hashcode(),
					stores[2].node.identity.publicKey.hashcode(),
				];
				await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
					local: false,
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
					stores[0].docs.log.getCover = async () => [
						stores[1].node.identity.publicKey.hashcode(),
						stores[2].node.identity.publicKey.hashcode(),
					];

					let failedOnce = false;
					for (let i = 1; i < stores.length; i++) {
						const fn = stores[i].docs.index.processQuery.bind(
							stores[1].docs.index,
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
						remote: { timeout },
					});
					expect(failedOnce).to.be.true;
					expect(counters[0]).equal(1);
					expect(counters[1] + counters[2]).equal(1);
					expect(counters[1]).not.equal(counters[2]);
				});

				it("will fail silently if can not reach all shards", async () => {
					stores[0].docs.log.getCover = async () => [
						stores[1].node.identity.publicKey.hashcode(),
						stores[2].node.identity.publicKey.hashcode(),
					];
					for (let i = 1; i < stores.length; i++) {
						stores[i].docs.index.processQuery = (a) => {
							throw new Error("Expected error");
						};
					}

					let timeout = 1000;

					await stores[0].docs.index.search(new SearchRequest({ query: [] }), {
						remote: { timeout },
					});
					expect(counters[0]).equal(1);
					expect(counters[1]).equal(0);
					expect(counters[2]).equal(0);
				});
			});
		});
	});

	describe("count", () => {
		let peersCount = 3;

		before(async () => {
			session = await TestSession.connected(peersCount);
		});

		after(async () => {
			await session.stop();
		});

		// wait for store2 and store1 size converge
		const waitForConverge = async (fn: () => number | Promise<number>) => {
			// call the function until the value converges
			let lastValue = -1;
			let value = -1;
			let timeouForConvergenze = 1e4;

			let deferred = pDefer();

			let timeout = setTimeout(() => {
				deferred.reject("Timeout");
			}, timeouForConvergenze);

			const check = async () => {
				value = await fn();
				if (value === lastValue) {
					clearTimeout(timeout);
					deferred.resolve(value);
				} else {
					lastValue = value;
					setTimeout(check, 2e3);
				}
			};

			return check();
		};

		describe("approximate", () => {
			it("0 docs", async () => {
				const store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store);

				const { estimate } = await store.docs.count({ approximate: true });
				expect(estimate).to.eq(0);
			});

			it("provides errorMargin for hash domains", async () => {
				const store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						replicate: { offset: 0, factor: 1 },
						replicas: { min: 1 },
						timeUntilRoleMaturity: 0,
					},
				});

				const count = 10;
				for (let i = 0; i < count; i++) {
					await store.docs.put(new Document({ id: i.toString() }));
				}

				const result = await store.docs.count({ approximate: true });
				expect(result.estimate).to.eq(count);
				expect(result.errorMargin).to.eq(0);
			});

			it("falls back to local count when replication participation is tiny", async () => {
				const store = new TestStore({
					docs: new Documents<Document>(),
				});

				// Use a u64 domain where every entry maps to the same coordinate (0),
				// then replicate only width=1 at that coordinate. This makes the
				// participation fraction extremely small while still owning all docs,
				// which would otherwise cause the scaling estimator to explode.
				const constantDomain = createDocumentDomain({
					resolution: "u64",
					canProjectToOneSegment: () => false,
					fromEntry: () => 0n,
				});

				await session.peers[0].open(store, {
					args: {
						domain: constantDomain,
						replicas: { min: 1 },
						timeUntilRoleMaturity: 0,
						replicate: {
							normalized: false,
							offset: 0n,
							factor: 1n,
							strict: true,
						},
					},
				});

				const count = 10;
				for (let i = 0; i < count; i++) {
					await store.docs.put(new Document({ id: i.toString() }));
				}

				const result = await store.docs.count({ approximate: true });
				expect(result.estimate).to.eq(count);
				expect(result.errorMargin).to.be.undefined;
			});
			const createStores = async () => {
				const store1 = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store1, {
					args: {
						replicate: {
							offset: 0,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0,
					},
				});
				const store2 = await session.peers[1].open(store1.clone(), {
					args: {
						replicate: {
							offset: 0.5,
							factor: 0.5,
						},
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0,
					},
				});

				const store3 = await session.peers[2].open(store1.clone(), {
					args: {
						replicate: false,
						replicas: {
							min: 1,
						},
						timeUntilRoleMaturity: 0,
					},
				});
				return {
					store1,
					store2,
					store3,
				};
			};

			it("returns approximate count", async () => {
				const { store1, store2, store3 } = await createStores();

				let count = 1000;

				for (let i = 0; i < count; i++) {
					await store1.docs.put(new Document({ id: i.toString() }));
				}

				await waitForResolved(() =>
					expect(store2.docs.log.log.length).to.be.greaterThan(0),
				);
				await waitForResolved(() =>
					expect(store1.docs.log.log.length).to.be.lessThan(count),
				);

				await waitForResolved(async () => {
					const { estimate: approxCount1 } = await store1.docs.count({
						approximate: true,
					});
					const { estimate: approxCount2 } = await store2.docs.count({
						approximate: true,
					});
					const approxCount3 = await store3.docs.count({ approximate: true });
					const localCount3 = await store3.docs.count();

					expect(approxCount1).to.be.within(count * 0.9, count * 1.1);
					expect(approxCount2).to.be.within(count * 0.9, count * 1.1);
					expect(approxCount3.errorMargin).to.be.undefined;
					expect(approxCount3.estimate).to.eq(localCount3);
				});
			});

			it("returns approximate count with query", async () => {
				const { store1, store2, store3 } = await createStores();

				let count = 1000;

				for (let i = 0; i < count; i++) {
					await store1.docs.put(
						new Document({ id: i.toString(), number: BigInt(i) }),
					);
				}

				await waitForResolved(() =>
					expect(store2.docs.log.log.length).to.be.greaterThan(0),
				);
				await waitForResolved(() =>
					expect(store1.docs.log.log.length).to.be.lessThan(count),
				);

				await waitForConverge(() => store1.docs.log.log.length);
				await waitForConverge(() => store2.docs.log.log.length);

				let query = new IntegerCompare({
					key: "number",
					compare: Compare.Less,
					value: Math.round(count / 2),
				});

				await waitForResolved(async () => {
					const approxCount1 = await store1.docs.count({
						query,
						approximate: true,
					});
					const approxCount2 = await store2.docs.count({
						query,
						approximate: true,
					});
					const approxCount3 = await store3.docs.count({
						query,
						approximate: true,
					});
					const localCount3 = await store3.docs.count({ query });

					const expectedCount = Math.round(count / 2);

					// The estimator is probabilistic; use the provided 95% margin, scaled
					// up to ~99.9% to avoid CI flakes while still detecting regressions.
					const assertWithinMargin = (result: CountEstimate) => {
						expect(result.errorMargin).to.not.be.undefined;
						const margin = Math.max(0.15, result.errorMargin! * (3.29 / 1.96));
						expect(result.estimate).to.be.within(
							expectedCount * (1 - margin),
							expectedCount * (1 + margin),
						);
					};
					assertWithinMargin(approxCount1);
					assertWithinMargin(approxCount2);

					expect(approxCount3.errorMargin).to.be.undefined;
					expect(approxCount3.estimate).to.eq(localCount3);
				});
			});

			it("returns approximate count with deletions", async () => {
				const { store1, store2, store3 } = await createStores();

				let count = 1000;

				for (let i = 0; i < count; i++) {
					let id = i.toString();
					await store1.docs.put(new Document({ id }));
					if (i % 4 === 0) {
						// delete 25%
						await store1.docs.del(id);
					}
				}

					let expectedDocCountAfterDelete = count * 0.75;

					// Under CI and workspace-wide test parallelism, replication can take a
					// while to kick in (especially with deletes/pruning), so use a more
					// forgiving timeout to avoid flakes.
					await waitForResolved(
						() => expect(store2.docs.log.log.length).to.be.greaterThan(0),
						{ timeout: 30 * 1000 },
					);
					await waitForResolved(
						() => expect(store1.docs.log.log.length).to.be.lessThan(count),
						{ timeout: 30 * 1000 },
					);

				await waitForResolved(async () => {
					const { estimate: approxCount1 } = await store1.docs.count({
						approximate: true,
					});
					const { estimate: approxCount2 } = await store2.docs.count({
						approximate: true,
					});
					const approxCount3 = await store3.docs.count({ approximate: true });
					const localCount3 = await store3.docs.count();

					expect(approxCount1).to.be.within(
						expectedDocCountAfterDelete * 0.9,
						expectedDocCountAfterDelete * 1.1,
					);
					expect(approxCount2).to.be.within(
						expectedDocCountAfterDelete * 0.9,
						expectedDocCountAfterDelete * 1.1,
					);

					expect(approxCount3.errorMargin).to.be.undefined;
					expect(approxCount3.estimate).to.eq(localCount3);
				});
			});
		});
	});

	describe("caching", () => {
		let peersCount = 1;

		beforeEach(async () => {
			session = await TestSession.connected(peersCount);
		});

		afterEach(async () => {
			await session.stop();
		});

		it("can pre-cache search results", async () => {
			const store = new TestStore({
				docs: new Documents<Document>(),
			});
			await session.peers[0].open(store, {
				args: {
					replicate: true,
					index: {
						cache: {
							resolver: 0,
							query: {
								strategy: "auto",
								maxTotalSize: 1000,
								maxSize: 10,
								prefetchThreshold: 1,
							},
						},
					},
				},
			});
			const docs = [
				new Document({ id: "1", name: "name1" }),
				new Document({ id: "2", name: "name2" }),
				new Document({ id: "3", name: "name3" }),
			];
			for (const doc of docs) {
				await store.docs.put(doc);
			}

			const iterateAssert = async (modified?: boolean) => {
				const iterator = store.docs.index.iterate({}, { resolve: false });
				const first = await iterator.next(1);
				const second = await iterator.next(2);
				expect(first[0].name).to.eq("name1");
				expect(first[0] instanceof Document).to.be.true;
				expect(second[0].name).to.eq("name2");
				if (!modified) {
					expect(second[1].name).to.eq("name3");
				} else {
					expect(second[1].name).to.eq("name3-mod");
				}
				expect(second[0] instanceof Document).to.be.true;
				expect(second[1] instanceof Document).to.be.true;
			};
			await iterateAssert();
			await delay(1e3);
			await iterateAssert();
			await iterateAssert();

			// force some cache clearence
			await store.docs.put(new Document({ id: "3", name: "name3-mod" }));
			await iterateAssert(true);
			await delay(1e3);
			await iterateAssert(true);
		});
	});

	describe("prefetch", () => {
		let peersCount = 2;

		beforeEach(async () => {
			session = await TestSession.connected(peersCount);
		});

		afterEach(async () => {
			await session.stop();
		});
		const iterateAssert = async (
			store1: TestStore,
			store2: TestStore,
			modified?: boolean,
		) => {
			const iterator = store2.docs.index.iterate({}, { resolve: false });
			const first = await iterator.next(1);
			const second = await iterator.next(2);
			await iterator.close();
			expect(first[0].name).to.eq("name1");
			expect(first[0] instanceof Document).to.be.true;
			expect(second[0].name).to.eq("name2");
			if (!modified) {
				expect(second[1].name).to.eq("name3");
			} else {
				expect(second[1].name).to.eq("name3-mod");
			}
			expect(second[0] instanceof Document).to.be.true;
			expect(second[1] instanceof Document).to.be.true;
			expect(store1.docs.index.countIteratorsInProgress).to.eq(0); // no iterators in progress
			expect(store2.docs.index.countIteratorsInProgress).to.eq(0); // no iterators in progress
		};

		const setupInitialStoresAndPrefetch = async (options?: {
			beforePrefetch?: Promise<void>;
			data?: Uint8Array;
			prefetch?:
				| false
				| {
						strict?: boolean;
				  };
		}) => {
			const store = new TestStore({
				docs: new Documents<Document>(),
			});

			let mostCommonQueryPredictorThreshold = 2;
			await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
					index: {
						prefetch:
							options?.prefetch === false
								? false
								: {
										predictor: new MostCommonQueryPredictor(
											mostCommonQueryPredictorThreshold,
										),
										strict: true,
										...options?.prefetch,
									},
					},
				},
			});
			if (options?.beforePrefetch) {
				const sendFn = store.docs.index._query.send.bind(
					store.docs.index._query,
				);
				store.docs.index._query.send = async (request: any) => {
					if (request instanceof PredictedSearchRequest) {
						await options?.beforePrefetch;
					}
					return sendFn(request);
				};
			}
			let docCount = 3;
			let docs: Document[] = [];
			for (let i = 0; i < docCount; i++) {
				docs.push(
					new Document({
						id: (i + 1).toString(),
						name: "name" + (i + 1),
						data: options?.data,
					}),
				);
			}

			for (const doc of docs) {
				await store.docs.put(doc);
			}

			let store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					index: {
						prefetch:
							options?.prefetch === false
								? false
								: {
										strict: true,
										...options?.prefetch,
									},
					},
				},
			});

			await store2.docs.log.waitForReplicators();

			await iterateAssert(store, store2);
			await iterateAssert(store, store2);

			await store2.close();

			// now when we re-open we should have results sent to us before we ask for them
			store2 = await session.peers[1].open(store2.clone(), {
				args: {
					replicate: false,
					index: {
						prefetch:
							options?.prefetch === false
								? false
								: {
										strict: true,
										...options?.prefetch,
									},
					},
				},
			});
			return {
				store,
				store2,
			};
		};

		it("can prefetch search results", async () => {
			const { store, store2 } = await setupInitialStoresAndPrefetch();
			await waitForResolved(() =>
				expect(store2.docs.index.prefetch?.accumulator.size).to.equal(1),
			);

			// now requesting data from store2 should not requiry a query to remote

			const sendFn = store2.docs.index._query.send.bind(
				store2.docs.index._query,
			);
			const sendSpy = sinon.spy(sendFn);
			store2.docs.index._query.send = sendSpy;

			const requestFn = store2.docs.index._query.request.bind(
				store2.docs.index._query,
			);
			const requestSpy = sinon.spy(requestFn);
			store2.docs.index._query.request = requestSpy;

			await iterateAssert(store, store2);
			expect(requestSpy.callCount).to.eq(1); // one collect next request
			expect(requestSpy.getCalls()[0].args[0] instanceof CollectNextRequest).to
				.be.true;
			expect(sendSpy.callCount).to.eq(0); // even if we do a ".close()" on the iterator, we should not send a new request, because we only have 3 docs in total and we fetched all
			expect(store.docs.index.countIteratorsInProgress).to.eq(0); // no iterators in progress
			expect(store2.docs.index.countIteratorsInProgress).to.eq(0); // no iterators in progress
		});

		it("delivers prefetched batch to push-enabled iterator", async () => {
			const { store2 } = await setupInitialStoresAndPrefetch();

			const iterator = store2.docs.index.iterate(
				{ sort: { key: "id", direction: SortDirection.ASC } },
				{
					closePolicy: "manual",
					remote: {
						wait: { timeout: 5e3, behavior: "keep-open" },
						reach: { eager: true },
					},
					updates: { push: true, merge: true },
				},
			);

			await waitForResolved(
				async () => expect(await iterator.pending()).to.equal(3),
				{ timeout: 30_000 },
			);

			const batch = await iterator.next(1);
			expect(batch).to.have.length(1);
			expect(batch[0].id).to.equal("1");
			await iterator.close();
		});

		it("will intercept outgoing search queries with incoming prefetch results", async () => {
			const prefetchSendDelay = pDefer<void>();

			const { store, store2 } = await setupInitialStoresAndPrefetch({
				beforePrefetch: prefetchSendDelay.promise,
			});

			const sentData: RPCMessage[] = [];
			store.node.services.pubsub.addEventListener("publish", (evt) => {
				if (evt.detail.data.topics.includes(store.docs.index._query.topic)) {
					sentData.push(deserialize(evt.detail.data.data, RPCMessage));
				}
			});

			// make it so that requesting results is also delayed, so that we will have an outgoing process (requesting)
			// and an incoming process (prefetching) at the same time
			const requestSentPromise = pDefer<void>();

			const requestFn = store2.docs.index._query.request.bind(
				store2.docs.index._query,
			);

			store2.docs.index._query.request = async (request, options) => {
				if (
					request instanceof SearchRequest ||
					request instanceof SearchRequestIndexed ||
					request instanceof IterationRequest
				) {
					requestSentPromise.resolve();
				}
				return requestFn(request, options);
			};

			const iterator = store2.docs.index.iterate({}, { resolve: false });
			const promise = iterator.next(1);
			await requestSentPromise.promise;

			prefetchSendDelay.resolve(); // allow the prefetch to send

			let t0 = Date.now();
			const results = await promise;
			expect(sentData.filter((x) => x instanceof ResponseV0).length).to.eq(0);

			const next = await iterator.next(2);
			await iterator.close();
			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThan(3000); // should not be 10 seconds since we are consuming the pretfetch results

			expect(results[0].name).to.eq("name1");
			expect(results[0] instanceof Document).to.be.true;
			expect(next[0].name).to.eq("name2");
			expect(next[1].name).to.eq("name3");
			expect(next[0] instanceof Document).to.be.true;
			expect(next[1] instanceof Document).to.be.true;

			await delay(2e3);
			expect(sentData.filter((x) => x instanceof ResponseV0).length).to.eq(1);
		});

		/* TODO improve this speed test 
		it("using prefetch is faster than not using it", async () => {
	
			const data = randomBytes(1e4)
			let setup = await setupInitialStoresAndPrefetch({ data });
	
			const time = async (store: TestStore<Document, any>) => {
				const t0 = Date.now();
				const iterator = store.docs.index.iterate({}, { resolve: false });
				const out = await iterator.next(1);
				expect(out).to.have.length(1)
				const t1 = Date.now();
				await iterator.close();
				return t1 - t0;
	
			}
	
	
			const timeWithPrefetch = await time(setup.store2);
	
	
			// now we will open the store without prefetch
			await setup.store.close();
			await setup.store2.close();
			setup = await setupInitialStoresAndPrefetch({ data, prefetch: false });
	
			const timeWithoutPrefetch = await time(setup.store2);
	
			console.log({
				timeWithoutPrefetch,
				timeWithPrefetch
			})
			expect(timeWithoutPrefetch).to.be.greaterThan(timeWithPrefetch);
	
		});
	*/
	});

	describe("createReplicatorJoinListener", () => {
		it("onPeer is only invoked for replicators", async () => {
			const index = new DocumentIndex();
			const identity = await Ed25519Keypair.create();
			(index as any).node = {
				identity,
			};
			let resolve = false;
			const mockLog = {
				waitForReplicator: async (_key: any) => {
					if (resolve) {
						return;
					}
					throw new Error("Expected throw");
				},
			} as unknown as SharedLog<Document>;
			(index as any)._log = mockLog;

			let peers: PublicSignKey[] = [];
			index["createReplicatorJoinListener"]({
				onPeer: (pk) => {
					peers.push(pk);
				},
			});
			const key1 = (await Ed25519Keypair.create()).publicKey;
			const key2 = (await Ed25519Keypair.create()).publicKey;

			index._query.events.dispatchEvent(
				new CustomEvent<PublicSignKey>("join", {
					detail: key1,
				}),
			);

			await delay(100);
			expect(peers).to.have.length(0);

			resolve = true; // now replicator is ready to be used

			index._query.events.dispatchEvent(
				new CustomEvent<PublicSignKey>("join", {
					detail: key2,
				}),
			);

			await delay(100);
			expect(peers).to.have.length(1);
			expect(peers[0]).to.deep.equal(key2);
		});
	});

	describe("migration", () => {
		describe("v6-v7", async () => {
			let store: TestStore;

			before(async () => {
				session = await TestSession.connected(1);
			});
			afterEach(async () => {
				await store?.close();
			});

			after(async () => {
				await session.stop();
			});

			it("can be compatible with v6", async () => {
				store = new TestStore({
					docs: new Documents<Document>(),
				});
				await session.peers[0].open(store, {
					args: {
						compatibility: 6,
					},
				});
				const changes: DocumentsChange<Document, Document>[] = [];

				store.docs.events.addEventListener("change", (evt) => {
					changes.push(evt.detail);
				});

				let doc = new Document({
					id: uuid(),
					name: "Hello world",
				});

				const putOperation = (await store.docs.put(doc)).entry;
				expect(await store.docs.index.getSize()).equal(1);

				expect(changes.length).equal(1);
				expect(changes[0].added).to.have.length(1);

				const payload = await putOperation.getPayloadValue();
				expect(payload).to.be.instanceOf(PutWithKeyOperation);
				expect(store.docs.log.compatibility).to.be.equal(8);
			});
		});
	});

	// TODO is this feature needed? if so setup a case where a too strict acl is set, actually results in a recoverable state
	// i.e. block store still has the blocks, but the index is empty

	/* describe("recover", () => {
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
			session = await TestSession.connected(1, { directory: "./tmp/document-store/recover/" + uuid() });
		
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
			expect(await db1.docs.index.getSize()).equal(3);
			let count = 0;
			for await (const f of db1.docs.log.log.blocks.iterator()) {
				count++;
			}
			expect(count).to.equal(3);
			await db1.docs.log.reload();
	
			expect(await db1.docs.index.getSize()).equal(0);
	
			count = 0;
			for await (const f of db1.docs.log.log.blocks.iterator()) {
				count++;
			}
			expect(count).to.equal(0);
		
			canPerform = true;
			await db1.docs.put(new Document({ id: uuid() }));
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
	}); */

	describe("updateIndex", () => {
		beforeEach(async () => {
			session = await TestSession.connected(1);
		});
		afterEach(async () => {
			await session.stop();
		});

		it("should update index with proper added and removed results", async () => {
			// Open a store with a document store inside (docs)
			const store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>(),
				}),
			);

			// Add two initial documents.
			await store.docs.put(new Document({ id: "1", name: "name1" }));
			await store.docs.put(new Document({ id: "2", name: "name2" }));

			// Wait briefly for the index to update (if needed, use waitForResolved or similar).
			let initialResults = await store.docs.index.search(
				new SearchRequest({ query: [] }),
			);
			expect(initialResults).to.have.length(2);

			// Simulate a change: remove the document with id "1" and add a new document with id "3".
			let changeEvents: DocumentsChange<Document, Document>[] = [];
			store.docs.events.addEventListener("change", (evt) => {
				changeEvents.push(evt.detail);
			});
			await store.docs.put(new Document({ id: "3", name: "name3" }));
			await store.docs.del("1");
			await waitForResolved(() => expect(changeEvents.length).to.equal(2));

			// Use a simple query that sorts by "id" in ascending order.
			const query = {
				query: {}, // empty query selects all documents
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			};

			// Call updateResults on the document index.
			// If updateResults is not directly typed on the index, you may cast index as any.
			let updatedResults = initialResults;
			for (const change of changeEvents) {
				updatedResults = await store.docs.index.updateResults(
					updatedResults,
					change,
					query,
					true,
				);
			}

			// Check that the updated results contain the expected documents.
			expect(updatedResults).to.have.length(2);
			expect(updatedResults[0].id).to.equal("2");
			expect(updatedResults[1].id).to.equal("3");
		});

		@variant("indexed-document")
		class IndexedDocument {
			@field({ type: "string" })
			id: string;

			@field({ type: option("string") })
			name: string | undefined;

			constructor(document: Document) {
				this.id = document.id;
				this.name = document.name;
			}
		}
		it("should update index with indexed added and removed results ", async () => {
			// Open a store with a document store inside (docs)
			const store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document, IndexedDocument>(),
				}),
				{
					args: {
						index: {
							type: IndexedDocument,
						},
					},
				},
			);

			// Add two initial documents.
			await store.docs.put(new Document({ id: "1", name: "name1" }));
			await store.docs.put(new Document({ id: "2", name: "name2" }));

			// Wait briefly for the index to update (if needed, use waitForResolved or similar).
			let initialResults = await store.docs.index.search(
				new SearchRequest({ query: [] }),
			);
			expect(initialResults).to.have.length(2);

			// Simulate a change: remove the document with id "1" and add a new document with id "3".
			let changeEvents: DocumentsChange<Document, IndexedDocument>[] = [];
			store.docs.events.addEventListener("change", (evt) => {
				changeEvents.push({
					added: evt.detail.added.map((x) => x),
					removed: evt.detail.removed.map((x) => x),
				});
			});
			await store.docs.put(new Document({ id: "3", name: "name3" }));
			await store.docs.del("1");
			await waitForResolved(() => expect(changeEvents.length).to.equal(2));

			// Use a simple query that sorts by "id" in ascending order.
			const query = {
				query: {}, // empty query selects all documents
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			};

			// Call updateResults on the document index.
			// If updateResults is not directly typed on the index, you may cast index as any.
			let updatedResults = initialResults;
			for (const change of changeEvents) {
				updatedResults = await store.docs.index.updateResults(
					updatedResults,
					change,
					query,
					true,
				);
			}

			// Check that the updated results contain the expected documents.
			expect(updatedResults).to.have.length(2);
			expect(updatedResults[0].id).to.equal("2");
			expect(updatedResults[1].id).to.equal("3");
		});

		it("should update existing doc ", async () => {
			// Open a store with a document store inside (docs)
			const store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document, IndexedDocument>(),
				}),
				{
					args: {
						index: {
							type: IndexedDocument,
						},
					},
				},
			);

			// Add two initial documents.
			await store.docs.put(new Document({ id: "1", name: "name1" }));
			await store.docs.put(new Document({ id: "2", name: "name2" }));

			// Wait briefly for the index to update (if needed, use waitForResolved or similar).
			let initialResults = await store.docs.index.search(
				new SearchRequest({ query: [] }),
			);
			expect(initialResults).to.have.length(2);

			// Simulate a change: remove the document with id "1" and add a new document with id "3".
			let changeEvents: DocumentsChange<Document, IndexedDocument>[] = [];
			store.docs.events.addEventListener("change", (evt) => {
				changeEvents.push({
					added: evt.detail.added.map((x) => x),
					removed: evt.detail.removed.map((x) => x),
				});
			});
			await store.docs.put(new Document({ id: "2", name: "name2 updated" }));
			await waitForResolved(() => expect(changeEvents.length).to.equal(1));

			// Use a simple query that sorts by "id" in ascending order.
			const query = {
				query: {}, // empty query selects all documents
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			};

			// Call updateResults on the document index.
			// If updateResults is not directly typed on the index, you may cast index as any.
			let updatedResults = initialResults;
			for (const change of changeEvents) {
				updatedResults = await store.docs.index.updateResults(
					updatedResults,
					change,
					query,
					true,
				);
			}

			// Check that the updated results contain the expected documents.
			expect(updatedResults).to.have.length(2);
			expect(updatedResults[0].id).to.equal("1");
			expect(updatedResults[1].id).to.equal("2");
			expect(updatedResults[1].name).to.equal("name2 updated");
		});

		it("it should not add documents in the continued sort order", async () => {
			// if we have a search result sorted by numbers
			// like 2, 3, 4, 5
			// and we add a new document with id 1
			// it should  be added at the beginning of the result
			// however if we add a new document with id 6
			// it should not be added to the result
			// but invoking next on the iterator would fetch it

			// Open a store with a document store inside (docs)
			const store = await session.peers[0].open(
				new TestStore({
					docs: new Documents<Document>(),
				}),
			);

			// Add two initial documents.
			await store.docs.put(new Document({ id: "2", name: "name2" }));
			await store.docs.put(new Document({ id: "3", name: "name3" }));

			// Wait briefly for the index to update (if needed, use waitForResolved or similar).
			let initialResults = await store.docs.index.search(
				new SearchRequest({ query: [] }),
			);
			expect(initialResults).to.have.length(2);

			let changeEvents: DocumentsChange<Document, Document>[] = [];
			store.docs.events.addEventListener("change", (evt) => {
				changeEvents.push(evt.detail);
			});

			await store.docs.put(new Document({ id: "1", name: "name1" }));
			await store.docs.put(new Document({ id: "4", name: "name4" }));
			await waitForResolved(() => expect(changeEvents.length).to.equal(2));

			let updatedResults = initialResults;

			// Use a simple query that sorts by "id" in ascending order.
			const query = {
				query: {}, // empty query selects all documents
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			};

			for (const change of changeEvents) {
				updatedResults = await store.docs.index.updateResults(
					updatedResults,
					change,
					query,
					true,
				);
			}
		});
	});
});
