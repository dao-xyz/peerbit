import { toId, toIdeable } from "@peerbit/indexer-interface";
import { EntryType } from "@peerbit/log";
import { NativeBackboneMemoryCoordinatePersistenceStore } from "@peerbit/native-backbone";
import { PersistedDeliveryError } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import { policy, transform } from "../src/index.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

const persistedDelivery = {
	reliability: "persisted" as const,
	minAcks: 1,
};

describe("document persisted delivery", function () {
	this.timeout(120_000);

	let session: TestSession;
	let store: TestStore | undefined;

	before(async () => {
		session = await TestSession.connected(1, createRustPeerbitOptions());
	});

	afterEach(async () => {
		await store?.close();
		store = undefined;
	});

	after(async () => {
		await session.stop();
	});

	const openStore = async (mode: "auto" | "native") => {
		store = new TestStore({ docs: new Documents<Document>() });
		await session.peers[0].open(store, {
			args:
				mode === "native"
					? {
							mode,
							replicate: false,
							nativeGraph: true,
							nativeBackbone: {
								optional: false,
								documentIndex: true,
								coordinatePersistence: {
									store: new NativeBackboneMemoryCoordinatePersistenceStore(),
									buffered: true,
									flushOnAppend: false,
								},
							},
							canPerform: policy.allowAll<Document>(),
							index: {
								type: Document,
								transform: transform.identity<Document>(),
							},
						}
					: {
							mode,
							replicate: false,
							nativeGraph: true,
						},
		});
		return store;
	};

	for (const mode of ["auto", "native"] as const) {
		it(`settles one persisted receipt batch after the ${mode} document commit`, async () => {
			const opened = await openStore(mode);
			const docs = [
				new Document({ id: uuid(), name: `${mode}-first` }),
				new Document({ id: uuid(), name: `${mode}-second` }),
			];
			const options = {
				unique: true,
				delivery: persistedDelivery,
			};
			let readableAtReceipt = false;
			const deliverStub = sinon
				.stub(opened.docs.log as any, "deliverPersistedAppendCommits")
				.callsFake(async () => {
					readableAtReceipt = (
						await Promise.all(docs.map((doc) => opened.docs.get(doc.id)))
					).every((doc) => doc != null);
				});
			const localBatchSpy = sinon.spy(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			);
			const providerNotifySpy = sinon.spy(
				(opened.docs.log as any).remoteBlocks,
				"notifyStoredDeferred",
			);

			const result = await opened.docs.putMany(docs, options);

			expect(result.entries).to.have.length(docs.length);
			expect(localBatchSpy.callCount).equal(1);
			expect(localBatchSpy.firstCall.args[1]).to.include({
				target: "none",
				delivery: false,
				replicate: false,
			});
			expect(providerNotifySpy.callCount).equal(0);
			expect(deliverStub.callCount).equal(1);
			expect(deliverStub.firstCall.args[0]).to.have.length(docs.length);
			expect(deliverStub.firstCall.args[2]).to.not.equal(options);
			expect(deliverStub.firstCall.args[2]).to.include({ unique: true });
			expect(deliverStub.firstCall.args[2].delivery).to.deep.include(
				persistedDelivery,
			);
			expect(readableAtReceipt).equal(true);
		});
	}

	it("lets the compat append settle one persisted delete receipt", async () => {
		const opened = await openStore("auto");
		const doc = new Document({ id: uuid(), name: "compat-delete" });
		const put = await opened.docs.put(doc, { unique: true });
		let absentAtReceipt = false;
		sinon
			.stub(opened.docs.log as any, "_appendDeliverToReplicators")
			.resolves();
		const settlement = sinon
			.stub(opened.docs.log as any, "settlePersistedDelivery")
			.callsFake(async () => {
				absentAtReceipt = (await opened.docs.get(doc.id)) == null;
			});
		const publicEntryDelivery = sinon.spy(
			opened.docs.log as any,
			"deliverPersistedEntries",
		);
		const publicCommitDelivery = sinon.spy(
			opened.docs.log as any,
			"deliverPersistedAppendCommits",
		);

		const deleted = await opened.docs.del(doc.id, {
			delivery: persistedDelivery,
			meta: { type: EntryType.APPEND, next: [] },
		});

		expect(deleted.entry.meta.type).equal(EntryType.CUT);
		expect(deleted.entry.meta.next).to.deep.equal([put.entry.hash]);
		expect(settlement.callCount).equal(1);
		expect(publicEntryDelivery.callCount).equal(0);
		expect(publicCommitDelivery.callCount).equal(0);
		expect(absentAtReceipt).equal(true);
		expect(await opened.docs.get(doc.id)).equal(undefined);
	});

	it("settles one strict-native delete receipt after its local commit", async () => {
		const opened = await openStore("native");
		const doc = new Document({ id: uuid(), name: "native-delete" });
		await opened.docs.put(doc, { unique: true });
		let absentAtReceipt = false;
		let deliveredHashes: string[] = [];
		const delivery = sinon
			.stub(opened.docs.log as any, "deliverPersistedAppendCommits")
			.callsFake(async (...args: unknown[]) => {
				const appendCommits = args[0] as Array<{ hash: string }>;
				deliveredHashes = appendCommits.map((commit) => commit.hash);
				absentAtReceipt = (await opened.docs.get(doc.id)) == null;
			});
		const localAppend = sinon.spy(
			opened.docs.log as any,
			"appendStrictNativeDocumentPayloadCommitOnly",
		);

		const deleted = await opened.docs.del(doc.id, {
			delivery: persistedDelivery,
		});

		expect(localAppend.callCount).equal(1);
		expect(localAppend.firstCall.args[1]).to.include({
			target: "none",
			delivery: false,
			replicate: false,
		});
		expect(delivery.callCount).equal(1);
		expect(delivery.firstCall.args[0]).to.have.length(1);
		expect(delivery.firstCall.args[2].delivery).to.deep.include(
			persistedDelivery,
		);
		expect(absentAtReceipt).equal(true);
		expect(deleted.entry.meta.type).equal(EntryType.CUT);
		expect(deliveredHashes).to.deep.equal([deleted.entry.hash]);
	});

	it("rejects an enum-zero custom delete type in strict-native mode", async () => {
		const opened = await openStore("native");
		const doc = new Document({ id: uuid(), name: "native-delete-type" });
		await opened.docs.put(doc, { unique: true });

		await expect(
			opened.docs.del(doc.id, {
				delivery: persistedDelivery,
				meta: { type: EntryType.APPEND },
			}),
		).to.be.rejectedWith("does not support custom entry type");
		expect((await opened.docs.get(doc.id))?.name).equal(doc.name);
		expect(opened.docs.log.log.length).equal(1);
	});

	it("snapshots persisted delete options and mutable byte ids at invocation", async () => {
		const opened = await openStore("auto");
		const seed = await opened.docs.put(
			new Document({ id: uuid(), name: "delete-snapshot-seed" }),
			{ unique: true },
		);
		const backend = (opened.docs as any)._documentBackend;
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const releasePromise = new Promise<void>((resolve) => (release = resolve));
		let capturedId: unknown;
		let capturedOptions: any;
		const deliver = sinon
			.stub(opened.docs.log as any, "deliverPersistedEntries")
			.resolves();
		const backendStub = sinon
			.stub(backend, "del")
			.callsFake(async (...args: unknown[]) => {
				capturedId = args[0];
				capturedOptions = args[1];
				entered();
				await releasePromise;
				return { entry: seed.entry, removed: [] };
			});
		const id = new Uint8Array([4, 5, 6]);
		const delivery: any = {
			reliability: "persisted",
			minAcks: 2,
			timeout: 5_000,
		};
		const options: any = {
			target: "replicators",
			replicate: false,
			delivery,
		};

		try {
			const pending = opened.docs.del(toId(id), options);
			await enteredPromise;
			id[0] = 99;
			delivery.reliability = "ack";
			delivery.minAcks = 0;
			delivery.timeout = 1;
			options.target = "none";
			options.replicate = true;
			release();
			await pending;

			expect(toIdeable(capturedId as any)).to.deep.equal(
				new Uint8Array([4, 5, 6]),
			);
			expect(capturedOptions).to.include({
				target: "replicators",
				replicate: false,
			});
			expect(capturedOptions.delivery).to.include({
				reliability: "persisted",
				minAcks: 2,
				timeout: 5_000,
			});
			expect(deliver.callCount).equal(1);
			expect(deliver.firstCall.args[1]).equal(capturedOptions);
		} finally {
			release();
			backendStub.restore();
		}
	});

	it("keeps persisted put and putMany settlement after caller downgrade", async () => {
		const opened = await openStore("native");
		const seed = await opened.docs.put(
			new Document({ id: uuid(), name: "snapshot-seed" }),
			{ unique: true },
		);
		const backend = (opened.docs as any)._documentBackend;
		const deliver = sinon
			.stub(opened.docs.log as any, "deliverPersistedEntries")
			.resolves();
		const run = async (many: boolean) => {
			let entered!: () => void;
			let release!: () => void;
			const enteredPromise = new Promise<void>(
				(resolve) => (entered = resolve),
			);
			const releasePromise = new Promise<void>(
				(resolve) => (release = resolve),
			);
			const method = many ? "putMany" : "put";
			const backendStub = sinon.stub(backend, method).callsFake(async () => {
				entered();
				await releasePromise;
				return many
					? { entries: [seed.entry], removed: [] }
					: { entry: seed.entry, removed: [] };
			});
			const delivery: any = {
				reliability: "persisted",
				minAcks: 2,
				timeout: 5_000,
			};
			const options: any = {
				unique: true,
				target: "replicators",
				replicate: false,
				replicas: 2,
				delivery,
			};
			try {
				const pending = many
					? opened.docs.putMany(
							[new Document({ id: uuid(), name: "snapshot-many" })],
							options,
						)
					: opened.docs.put(
							new Document({ id: uuid(), name: "snapshot-one" }),
							options,
						);
				await enteredPromise;
				delivery.reliability = "ack";
				delivery.minAcks = 0;
				delivery.timeout = 1;
				options.target = "none";
				options.replicate = true;
				options.replicas = 1;
				release();
				await pending;
				const captured = deliver.lastCall.args[1];
				expect(captured).to.include({
					unique: true,
					target: "replicators",
					replicate: false,
					replicas: 2,
				});
				expect(captured.delivery).to.include({
					reliability: "persisted",
					minAcks: 2,
					timeout: 5_000,
				});
			} finally {
				backendStub.restore();
			}
		};

		await run(false);
		await run(true);
		expect(deliver.callCount).to.equal(2);
	});

	it("does not upgrade an initially ack document write after backend await", async () => {
		const opened = await openStore("native");
		const seed = await opened.docs.put(
			new Document({ id: uuid(), name: "ack-seed" }),
			{ unique: true },
		);
		const backend = (opened.docs as any)._documentBackend;
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const releasePromise = new Promise<void>((resolve) => (release = resolve));
		const backendStub = sinon.stub(backend, "put").callsFake(async () => {
			entered();
			await releasePromise;
			return { entry: seed.entry, removed: [] };
		});
		const deliver = sinon.spy(
			opened.docs.log as any,
			"deliverPersistedEntries",
		);
		const delivery: any = { reliability: "ack", minAcks: 1 };
		const options: any = { unique: true, delivery };

		try {
			const pending = opened.docs.put(
				new Document({ id: uuid(), name: "ack-no-upgrade" }),
				options,
			);
			await enteredPromise;
			delivery.reliability = "persisted";
			release();
			await pending;
			expect(deliver.callCount).to.equal(0);
		} finally {
			backendStub.restore();
		}
	});

	it("pins ack delivery through a gated real compat put", async () => {
		const opened = await openStore("auto");
		const documents = opened.docs as any;
		const log = opened.docs.log as any;
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const releasePromise = new Promise<void>((resolve) => (release = resolve));
		const originalGetLocalIndexedContext =
			documents.getLocalIndexedContext.bind(documents);
		const indexGate = sinon
			.stub(documents, "getLocalIndexedContext")
			.callsFake(async (...args: unknown[]) => {
				entered();
				await releasePromise;
				return originalGetLocalIndexedContext(...args);
			});
		const persistedCapture = sinon.spy(
			log,
			"capturePersistedLocalAppendCommit",
		);
		const planningSnapshot = sinon.spy(
			log,
			"snapshotPersistedDeliveryPlanningEntry",
		);
		const planning = sinon.spy(log, "planPersistedDeliveryLeaders");
		const settlement = sinon.spy(log, "settlePersistedDelivery");
		const publicSingleDelivery = sinon.spy(log, "deliverPersistedEntries");
		const publicBatchDelivery = sinon.spy(log, "deliverPersistedAppendCommits");
		const delivery: any = { reliability: "ack", minAcks: 1 };
		const options: any = {
			target: "none",
			replicate: false,
			delivery,
			canAppend: async () => true,
		};
		const doc = new Document({ id: uuid(), name: "real-ack-no-upgrade" });

		try {
			const pending = opened.docs.put(doc, options);
			await enteredPromise;
			delivery.reliability = "persisted";
			release();
			const result = await pending;

			expect(result.entry.hash).to.be.a("string").and.not.empty;
			expect((await opened.docs.get(doc.id))?.name).equal(doc.name);
			expect(persistedCapture.callCount).equal(0);
			expect(planningSnapshot.callCount).equal(0);
			expect(planning.callCount).equal(0);
			expect(settlement.callCount).equal(0);
			expect(publicSingleDelivery.callCount).equal(0);
			expect(publicBatchDelivery.callCount).equal(0);
		} finally {
			release();
			indexGate.restore();
		}
	});

	it("pins false delivery through a gated real native putMany", async () => {
		const opened = await openStore("native");
		const documents = opened.docs as any;
		const log = opened.docs.log as any;
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const releasePromise = new Promise<void>((resolve) => (release = resolve));
		const originalPolicy =
			documents.assertNativeModePlainPutPolicySupported.bind(documents);
		let gated = false;
		const policyGate = sinon
			.stub(documents, "assertNativeModePlainPutPolicySupported")
			.callsFake(async (...args: unknown[]) => {
				if (!gated) {
					gated = true;
					entered();
					await releasePromise;
				}
				return originalPolicy(...args);
			});
		const evidencePath = sinon.spy(
			documents,
			"commitNativeDocumentAppendManyWithEvidence",
		);
		const persistedCapture = sinon.spy(
			log,
			"capturePersistedLocalAppendCommit",
		);
		const planningSnapshot = sinon.spy(
			log,
			"snapshotPersistedDeliveryPlanningEntry",
		);
		const planning = sinon.spy(log, "planPersistedDeliveryLeaders");
		const settlement = sinon.spy(log, "settlePersistedDelivery");
		const publicSingleDelivery = sinon.spy(log, "deliverPersistedEntries");
		const publicBatchDelivery = sinon.spy(log, "deliverPersistedAppendCommits");
		const options: any = {
			unique: true,
			target: "none",
			replicate: false,
			delivery: false,
		};
		const docs = [
			new Document({ id: uuid(), name: "real-false-first" }),
			new Document({ id: uuid(), name: "real-false-second" }),
		];

		try {
			const pending = opened.docs.putMany(docs, options);
			await enteredPromise;
			options.delivery = { ...persistedDelivery };
			options.target = "replicators";
			release();
			const result = await pending;

			expect(result.entries).to.have.length(docs.length);
			expect(evidencePath.callCount).equal(1);
			expect(evidencePath.firstCall.args[1]).equal(undefined);
			expect(persistedCapture.callCount).equal(0);
			expect(planningSnapshot.callCount).equal(0);
			expect(planning.callCount).equal(0);
			expect(settlement.callCount).equal(0);
			expect(publicSingleDelivery.callCount).equal(0);
			expect(publicBatchDelivery.callCount).equal(0);
		} finally {
			release();
			policyGate.restore();
		}
	});

	it("reuses one captured invocation through sequential putMany fallback", async () => {
		const opened = await openStore("auto");
		const documents = opened.docs as any;
		const log = opened.docs.log as any;
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const releasePromise = new Promise<void>((resolve) => (release = resolve));
		const originalGetLocalIndexedContext =
			documents.getLocalIndexedContext.bind(documents);
		let gated = false;
		const indexGate = sinon
			.stub(documents, "getLocalIndexedContext")
			.callsFake(async (...args: unknown[]) => {
				if (!gated) {
					gated = true;
					entered();
					await releasePromise;
				}
				return originalGetLocalIndexedContext(...args);
			});
		const invocationSnapshots = sinon.spy(log, "snapshotDocumentAppendOptions");
		const persistedCapture = sinon.spy(
			log,
			"capturePersistedLocalAppendCommit",
		);
		const planning = sinon.spy(log, "planPersistedDeliveryLeaders");
		const settlement = sinon.spy(log, "settlePersistedDelivery");
		const options: any = {
			target: "none",
			replicate: false,
			delivery: false,
			canAppend: async () => true,
		};
		const docs = [
			new Document({ id: uuid(), name: "sequential-first" }),
			new Document({ id: uuid(), name: "sequential-second" }),
		];

		try {
			const pending = opened.docs.putMany(docs, options);
			await enteredPromise;
			options.delivery = { ...persistedDelivery };
			options.target = "replicators";
			options.unique = true;
			options.checkRemote = true;
			release();
			const result = await pending;

			expect(result.entries).to.have.length(docs.length);
			expect(invocationSnapshots.callCount).equal(1);
			expect(persistedCapture.callCount).equal(0);
			expect(planning.callCount).equal(0);
			expect(settlement.callCount).equal(0);
		} finally {
			release();
			indexGate.restore();
		}
	});

	it("delivers native putMany commit facts before materializing public entries", async () => {
		const opened = await openStore("native");
		const docs = [
			new Document({ id: uuid(), name: "lazy-first" }),
			new Document({ id: uuid(), name: "lazy-second" }),
		];
		const log = opened.docs.log as any;
		const originalAppend =
			log.appendLocallyPreparedPayloadsManyIndependent.bind(log);
		let materializationCount = 0;
		let deliveredHashes: string[] = [];
		const localBatchStub = sinon
			.stub(log, "appendLocallyPreparedPayloadsManyIndependent")
			.callsFake(async (...args: unknown[]) => {
				const appended = await originalAppend(...args);
				appended.materializeEntries = appended.materializeEntries.map(
					(materializeEntry: () => unknown) => () => {
						materializationCount++;
						return materializeEntry();
					},
				);
				return appended;
			});
		const deliveryStub = sinon
			.stub(log, "deliverPersistedAppendCommits")
			.callsFake(async (...args: unknown[]) => {
				const appendCommits = args[0] as Array<{
					hash: string;
					coordinateFields?: unknown;
				}>;
				expect(materializationCount).equal(0);
				expect(appendCommits).to.have.length(docs.length);
				expect(
					appendCommits.every((commit) => commit.coordinateFields != null),
				).equal(true);
				deliveredHashes = appendCommits.map((commit) => commit.hash);
			});

		const result = await opened.docs.putMany(docs, {
			unique: true,
			delivery: persistedDelivery,
		});

		expect(localBatchStub.callCount).equal(1);
		expect(deliveryStub.callCount).equal(1);
		expect(materializationCount).equal(0);
		const entries = result.entries;
		expect(entries).to.have.length(docs.length);
		expect(entries.map((entry) => entry.hash)).to.deep.equal(deliveredHashes);
		expect(materializationCount).equal(docs.length);
	});

	it("persists document puts and a CUT on a durable remote replica", async () => {
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-persisted-put-many-"),
		);
		let writer: Peerbit | undefined;
		let receiver: Peerbit | undefined;
		let writerStore: TestStore | undefined;
		let receiverStore: TestStore | undefined;
		try {
			writer = await Peerbit.create();
			receiver = await Peerbit.create({ directory });
			await writer.dial(receiver);

			const storeId = Uint8Array.from(
				{ length: 32 },
				(_, index) => (index * 17 + 11) & 0xff,
			);
			const createStore = () => {
				const created = new TestStore({
					docs: new Documents<Document>({ id: storeId }),
				});
				created.id = storeId;
				return created;
			};
			const openArgs = (
				replicate: false | { offset: number; factor: number },
			) => ({
				replicas: { min: 1 },
				replicate,
				timeUntilRoleMaturity: 0,
				canPerform: policy.allowAll<Document>(),
				index: {
					type: Document,
					transform: transform.identity<Document>(),
				},
			});

			writerStore = createStore();
			receiverStore = createStore();
			await writer.open(writerStore, { args: openArgs(false) });
			await receiver.open(receiverStore, {
				args: openArgs({ offset: 0, factor: 1 }),
			});
			await writerStore.docs.log.waitForReplicator(
				receiver.identity.publicKey,
				{ roleAge: 0, timeout: 15_000 },
			);
			const receiverHash = receiver.identity.publicKey.hashcode();
			const capabilityDeadline = Date.now() + 15_000;
			while (
				(((writerStore.docs.log as any)._peerSyncCapabilities.get(
					receiverHash,
				) ?? 0) &
					(1 << 5)) ===
				0
			) {
				if (Date.now() >= capabilityDeadline) {
					throw new Error("Timed out waiting for persisted receipt capability");
				}
				await new Promise((resolve) => setTimeout(resolve, 25));
			}

			const docs = [
				new Document({ id: "durable-batch-first", name: "first" }),
				new Document({ id: "durable-batch-second", name: "second" }),
			];
			const result = await writerStore.docs.putMany(docs, {
				unique: true,
				delivery: { ...persistedDelivery, timeout: 15_000 },
			});

			expect(result.entries).to.have.length(docs.length);
			for (let index = 0; index < result.entries.length; index++) {
				const entry = result.entries[index]!;
				expect(entry.meta.data).instanceOf(Uint8Array);
				expect(entry.meta.data).to.have.length.greaterThan(0);
				const [block, lower, coordinate, document] = await Promise.all([
					(receiverStore.docs.log as any).remoteBlocks.localStore.has(
						entry.hash,
					),
					receiverStore.docs.log.log.entryIndex.properties.index.get(
						toId(entry.hash),
					),
					receiverStore.docs.log.entryCoordinatesIndex.get(toId(entry.hash)),
					receiverStore.docs.get(docs[index]!.id, {
						local: true,
						remote: false,
					}),
				]);
				expect(block).equal(true);
				expect(lower).to.exist;
				expect(coordinate).to.exist;
				expect(document?.name).equal(docs[index]!.name);
			}

			const deleted = await writerStore.docs.del(docs[0]!.id, {
				delivery: { ...persistedDelivery, timeout: 15_000 },
			});
			expect(deleted.entry.meta.type).equal(EntryType.CUT);
			const [cutBlock, cutLower, cutCoordinate] = await Promise.all([
				(receiverStore.docs.log as any).remoteBlocks.localStore.has(
					deleted.entry.hash,
				),
				receiverStore.docs.log.log.entryIndex.properties.index.get(
					toId(deleted.entry.hash),
				),
				receiverStore.docs.log.entryCoordinatesIndex.get(
					toId(deleted.entry.hash),
				),
			]);
			expect(cutBlock).equal(true);
			expect(cutLower).to.exist;
			expect(cutCoordinate).to.exist;

			const deleteVisibilityDeadline = Date.now() + 15_000;
			while (
				(await receiverStore.docs.get(docs[0]!.id, {
					local: true,
					remote: false,
				})) != null
			) {
				if (Date.now() >= deleteVisibilityDeadline) {
					throw new Error("Timed out waiting for remote delete visibility");
				}
				await new Promise((resolve) => setTimeout(resolve, 25));
			}
		} finally {
			await Promise.allSettled([writerStore?.close(), receiverStore?.close()]);
			await Promise.allSettled([writer?.stop(), receiver?.stop()]);
			await fs.rm(directory, { recursive: true, force: true });
		}
	});

	it("does not settle twice when a generic append owns the receipt", async () => {
		const opened = await openStore("auto");
		const doc = new Document({ id: uuid(), name: "generic-path" });
		const keepCache = new Set<string>();
		(opened.docs as any).keepCache = keepCache;
		let readableAtReceipt = false;
		let keptAtReceipt = false;
		sinon
			.stub(opened.docs.log as any, "_appendDeliverToReplicators")
			.resolves();
		const settleStub = sinon
			.stub(opened.docs.log as any, "settlePersistedDelivery")
			.callsFake(async (...args: unknown[]) => {
				const entries = args[0] as Array<{ canonicalHash: string }>;
				readableAtReceipt = (await opened.docs.get(doc.id)) != null;
				keptAtReceipt = keepCache.has(entries[0]!.canonicalHash);
			});
		const publicDeliverySpy = sinon.spy(
			opened.docs.log as any,
			"deliverPersistedEntries",
		);

		await opened.docs.put(doc, {
			unique: true,
			canAppend: async () => true,
			delivery: persistedDelivery,
		});

		expect(settleStub.callCount).equal(1);
		expect(publicDeliverySpy.callCount).equal(0);
		expect(readableAtReceipt).equal(true);
		expect(keptAtReceipt).equal(true);
	});

	it("preserves exact committed hashes for generic post-commit failures", async () => {
		const opened = await openStore("auto");
		const injectedFailure = new Error("injected generic change failure");
		sinon.stub(opened.docs as any, "handleChanges").throws(injectedFailure);

		let failure: unknown;
		try {
			await opened.docs.put(
				new Document({ id: uuid(), name: "generic-post-commit-failure" }),
				{
					unique: true,
					canAppend: async () => true,
					delivery: persistedDelivery,
				},
			);
		} catch (error) {
			failure = error;
		}

		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(1);
		expect(
			await opened.docs.log.log.has(persistedFailure.committedHashes[0]!),
		).equal(true);
	});

	it("classifies a trusted append failure after its lower commit", async () => {
		const opened = await openStore("auto");
		const injectedFailure = new Error("injected trusted post-commit failure");
		const finish = sinon
			.stub(opened.docs.log as any, "finishPreparedPayloadCommitOnlyAppend")
			.throws(injectedFailure);
		let failure: unknown;

		try {
			await opened.docs.put(
				new Document({ id: uuid(), name: "trusted-post-commit-failure" }),
				{
					unique: true,
					delivery: persistedDelivery,
				},
			);
		} catch (error) {
			failure = error;
		}

		expect(finish.callCount).equal(1);
		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(1);
		expect(
			await opened.docs.log.log.has(persistedFailure.committedHashes[0]!),
		).equal(true);
	});

	it("classifies a trusted prepared failure before the lower append returns", async () => {
		const opened = await openStore("auto");
		const injectedFailure = new Error(
			"injected trusted lower post-write failure",
		);
		const commitOnly = sinon
			.stub(opened.docs.log as any, "appendLocallyPreparedPayloadCommitOnly")
			.returns(undefined);
		const trim = sinon
			.stub(opened.docs.log.log as any, "trimIfConfigured")
			.rejects(injectedFailure);
		let failure: unknown;

		try {
			await opened.docs.put(
				new Document({ id: uuid(), name: "trusted-lower-post-write" }),
				{
					unique: true,
					delivery: persistedDelivery,
				},
			);
		} catch (error) {
			failure = error;
		}

		expect(commitOnly.callCount).equal(1);
		expect(trim.callCount).equal(1);
		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(1);
		expect(
			await opened.docs.log.log.has(persistedFailure.committedHashes[0]!),
		).equal(true);
	});

	it("classifies every trusted independent-batch hash before the lower return", async () => {
		const opened = await openStore("auto");
		const injectedFailure = new Error(
			"injected trusted lower batch post-write failure",
		);
		const nativeBatch = sinon
			.stub(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch",
			)
			.resolves(undefined);
		const trim = sinon
			.stub(opened.docs.log.log as any, "trimIfConfigured")
			.rejects(injectedFailure);
		const docs = [
			new Document({ id: uuid(), name: "trusted-lower-batch-first" }),
			new Document({ id: uuid(), name: "trusted-lower-batch-second" }),
		];
		let failure: unknown;

		try {
			await opened.docs.putMany(docs, {
				unique: true,
				delivery: persistedDelivery,
			});
		} catch (error) {
			failure = error;
		}

		expect(nativeBatch.callCount).equal(1);
		expect(trim.callCount).equal(1);
		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(docs.length);
		expect(new Set(persistedFailure.committedHashes).size).equal(docs.length);
		for (const hash of persistedFailure.committedHashes) {
			expect(await opened.docs.log.log.has(hash)).equal(true);
		}
	});

	it("classifies a strict-native single failure after final commit acknowledgement", async () => {
		const opened = await openStore("native");
		const injectedFailure = new Error("injected strict-native success failure");
		sinon
			.stub(
				opened.docs.log as any,
				"applyPreparedAppendFactsWithDeferredCoordinateDeletes",
			)
			.throws(injectedFailure);
		let failure: unknown;

		try {
			await opened.docs.put(
				new Document({ id: uuid(), name: "strict-native-post-commit" }),
				{
					unique: true,
					delivery: persistedDelivery,
				},
			);
		} catch (error) {
			failure = error;
		}

		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(1);
		expect(
			await opened.docs.log.log.has(persistedFailure.committedHashes[0]!),
		).equal(true);
	});

	it("classifies every strict-native batch hash after final commit acknowledgement", async () => {
		const opened = await openStore("native");
		const injectedFailure = new Error(
			"injected strict-native batch success failure",
		);
		sinon
			.stub(
				opened.docs.log as any,
				"applyPreparedAppendFactsWithDeferredCoordinateDeletes",
			)
			.throws(injectedFailure);
		const docs = [
			new Document({ id: uuid(), name: "strict-native-batch-first" }),
			new Document({ id: uuid(), name: "strict-native-batch-second" }),
		];
		let failure: unknown;

		try {
			await opened.docs.putMany(docs, {
				unique: true,
				delivery: persistedDelivery,
			});
		} catch (error) {
			failure = error;
		}

		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(docs.length);
		expect(new Set(persistedFailure.committedHashes).size).equal(docs.length);
		for (const hash of persistedFailure.committedHashes) {
			expect(await opened.docs.log.log.has(hash)).equal(true);
		}
	});

	it("keeps a 513-document independent commit in one persisted delivery batch", async () => {
		const opened = await openStore("auto");
		const docs = Array.from(
			{ length: 513 },
			(_, index) => new Document({ id: uuid(), name: `large-batch-${index}` }),
		);
		const deliverStub = sinon
			.stub(opened.docs.log as any, "deliverPersistedAppendCommits")
			.resolves();
		const localBatchSpy = sinon.spy(
			opened.docs.log as any,
			"appendLocallyPreparedPayloadsManyIndependent",
		);

		const result = await opened.docs.putMany(docs, {
			unique: true,
			delivery: persistedDelivery,
		});

		expect(result.entries).to.have.length(513);
		expect(localBatchSpy.callCount).equal(1);
		expect(deliverStub.callCount).equal(1);
		expect(deliverStub.firstCall.args[0]).to.have.length(513);
		expect(await opened.docs.index.getSize()).equal(513);
	});

	it("rejects invalid persisted receipt options before a local commit", async () => {
		const opened = await openStore("auto");
		const singleCommitSpy = sinon.spy(
			opened.docs as any,
			"commitNativeDocumentAppend",
		);
		const batchCommitSpy = sinon.spy(
			opened.docs as any,
			"commitNativeDocumentAppendMany",
		);
		const invalidMinAcks = {
			unique: true,
			delivery: { reliability: "persisted" as const, minAcks: 0 },
		};

		await expect(
			opened.docs.put(
				new Document({ id: uuid(), name: "invalid-single" }),
				invalidMinAcks,
			),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			opened.docs.putMany(
				[new Document({ id: uuid(), name: "invalid-batch" })],
				invalidMinAcks,
			),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			opened.docs.putMany(
				[new Document({ id: uuid(), name: "invalid-timeout" })],
				{
					unique: true,
					delivery: { ...persistedDelivery, timeout: 0 },
				},
			),
		).to.be.rejectedWith(
			"persisted delivery timeout must be a positive number no greater than 2147483647",
		);
		await expect(
			opened.docs.put(new Document({ id: uuid(), name: "invalid-target" }), {
				unique: true,
				target: "future-target",
				delivery: persistedDelivery,
			} as any),
		).to.be.rejectedWith(
			'persisted delivery requires target="replicators" (or an omitted target)',
		);
		await expect(
			opened.docs.putMany(
				[new Document({ id: uuid(), name: "fractional-quorum" })],
				{
					unique: true,
					delivery: { ...persistedDelivery, minAcks: 1.5 },
				},
			),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			opened.docs.putMany(
				[new Document({ id: uuid(), name: "unsafe-quorum" })],
				{
					unique: true,
					delivery: {
						...persistedDelivery,
						minAcks: Number.MAX_SAFE_INTEGER + 1,
					},
				},
			),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			opened.docs.putMany(
				[new Document({ id: uuid(), name: "overflow-timeout" })],
				{
					unique: true,
					delivery: {
						...persistedDelivery,
						timeout: 2_147_483_648,
					},
				},
			),
		).to.be.rejectedWith(
			"persisted delivery timeout must be a positive number no greater than 2147483647",
		);
		const cancellation = new Error("cancelled before append");
		const abortController = new AbortController();
		abortController.abort(cancellation);
		let abortedFailure: unknown;
		try {
			await opened.docs.putMany(
				[new Document({ id: uuid(), name: "pre-aborted" })],
				{
					unique: true,
					delivery: {
						...persistedDelivery,
						signal: abortController.signal,
					},
				},
			);
		} catch (error) {
			abortedFailure = error;
		}
		expect(abortedFailure).equal(cancellation);
		expect(singleCommitSpy.callCount).equal(0);
		expect(batchCommitSpy.callCount).equal(0);
		expect(opened.docs.log.log.length).equal(0);
		expect(await opened.docs.index.getSize()).equal(0);
	});

	it("rejects invalid persisted delete options before mutation", async () => {
		const opened = await openStore("auto");
		const doc = new Document({ id: uuid(), name: "invalid-delete" });
		await opened.docs.put(doc, { unique: true });
		const deleteSpy = sinon.spy((opened.docs as any)._documentBackend, "del");

		await expect(
			opened.docs.del(doc.id, {
				delivery: { reliability: "persisted", minAcks: 0 },
			}),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			opened.docs.del(doc.id, {
				target: "none",
				delivery: persistedDelivery,
			}),
		).to.be.rejectedWith(
			'persisted delivery requires target="replicators" (or an omitted target)',
		);
		await expect(
			opened.docs.del(doc.id, {
				delivery: { ...persistedDelivery, timeout: 0 },
			}),
		).to.be.rejectedWith(
			"persisted delivery timeout must be a positive number no greater than 2147483647",
		);
		const cancellation = new Error("cancelled before delete");
		const abortController = new AbortController();
		abortController.abort(cancellation);
		let abortedFailure: unknown;
		try {
			await opened.docs.del(doc.id, {
				delivery: {
					...persistedDelivery,
					signal: abortController.signal,
				},
			});
		} catch (error) {
			abortedFailure = error;
		}

		expect(abortedFailure).equal(cancellation);
		expect(deleteSpy.callCount).equal(0);
		expect(opened.docs.log.log.length).equal(1);
		expect((await opened.docs.get(doc.id))?.name).equal(doc.name);
	});

	for (const mode of ["auto", "native"] as const) {
		it(`reports the committed ${mode} CUT when receipt settlement fails`, async () => {
			const opened = await openStore(mode);
			const doc = new Document({ id: uuid(), name: `${mode}-failed-delete` });
			await opened.docs.put(doc, { unique: true });
			let failure: unknown;

			try {
				await opened.docs.del(doc.id, {
					delivery: { ...persistedDelivery, timeout: 250 },
				});
			} catch (error) {
				failure = error;
			}

			expect(failure).instanceOf(PersistedDeliveryError);
			const persistedFailure = failure as PersistedDeliveryError;
			expect(persistedFailure.localCommitSucceeded).equal(true);
			expect(persistedFailure.retrySafe).equal(false);
			expect(persistedFailure.committedHashes).to.have.length(1);
			const committedHash = persistedFailure.committedHashes[0]!;
			expect(await opened.docs.log.log.has(committedHash)).equal(true);
			expect((await opened.docs.log.log.get(committedHash))?.meta.type).equal(
				EntryType.CUT,
			);
			expect(await opened.docs.get(doc.id)).equal(undefined);
		});
	}

	it("classifies a strict-native delete failure after the CUT commit", async () => {
		const opened = await openStore("native");
		const doc = new Document({ id: uuid(), name: "native-post-cut-failure" });
		await opened.docs.put(doc, { unique: true });
		opened.docs.events.addEventListener("change", () => undefined);
		const injectedFailure = new Error("injected delete change failure");
		const dispatch = sinon
			.stub(opened.docs as any, "dispatchDocumentChangeIfObserved")
			.throws(injectedFailure);
		let failure: unknown;

		try {
			await opened.docs.del(doc.id, { delivery: persistedDelivery });
		} catch (error) {
			failure = error;
		}

		expect(dispatch.callCount).equal(1);
		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.have.length(1);
		const committedHash = persistedFailure.committedHashes[0]!;
		expect(await opened.docs.log.log.has(committedHash)).equal(true);
		expect((await opened.docs.log.log.get(committedHash))?.meta.type).equal(
			EntryType.CUT,
		);
		expect(await opened.docs.get(doc.id)).equal(undefined);
	});

	it("keeps locally committed documents readable when receipt settlement fails", async () => {
		const opened = await openStore("auto");
		const docs = [
			new Document({ id: uuid(), name: "committed-first" }),
			new Document({ id: uuid(), name: "committed-second" }),
		];
		const deliverSpy = sinon.spy(
			opened.docs.log as any,
			"deliverPersistedAppendCommits",
		);

		let failure: unknown;
		try {
			await opened.docs.putMany(docs, {
				unique: true,
				delivery: { ...persistedDelivery, timeout: 1_000 },
			});
		} catch (error) {
			failure = error;
		}

		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.localCommitSucceeded).equal(true);
		expect(persistedFailure.retrySafe).equal(false);
		expect(persistedFailure.message).contains("automatic retry is unsafe");
		expect(deliverSpy.callCount).equal(1);
		const deliveredCommits = deliverSpy.firstCall.args[0] as Array<{
			hash: string;
		}>;
		expect(persistedFailure.committedHashes).to.deep.equal(
			deliveredCommits.map((commit) => commit.hash),
		);
		expect(opened.docs.log.log.length).equal(docs.length);
		for (const doc of docs) {
			expect((await opened.docs.get(doc.id))?.name).equal(doc.name);
		}
	});

	it("reports the known committed hash when post-commit fact construction fails", async () => {
		const opened = await openStore("auto");
		const injectedFailure = new Error("injected fact construction failure");
		let committedHash: string | undefined;
		const constructFacts = sinon
			.stub(opened.docs as any, "createNativeCheckedDocumentAppendCommitFacts")
			.callsFake((...args: unknown[]) => {
				const appended = args[1] as { appendCommit: { hash: string } };
				committedHash = appended.appendCommit.hash;
				throw injectedFailure;
			});

		let failure: unknown;
		try {
			await opened.docs.put(
				new Document({ id: uuid(), name: "known-commit-failure" }),
				{
					unique: true,
					delivery: persistedDelivery,
				},
			);
		} catch (error) {
			failure = error;
		}

		expect(constructFacts.callCount).equal(1);
		expect(committedHash).to.be.a("string").and.not.equal("");
		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.deep.equal([committedHash]);
		expect(await opened.docs.log.log.has(committedHash!)).equal(true);
	});

	it("reports every committed hash when batch fact construction fails", async () => {
		const opened = await openStore("auto");
		const docs = [
			new Document({ id: uuid(), name: "batch-fact-first" }),
			new Document({ id: uuid(), name: "batch-fact-second" }),
			new Document({ id: uuid(), name: "batch-fact-third" }),
		];
		const injectedFailure = new Error(
			"injected batch fact construction failure",
		);
		let committedHashes: string[] = [];
		const constructFacts = sinon
			.stub(opened.docs as any, "createDocumentAppendCommitFactsBatch")
			.callsFake((...args: unknown[]) => {
				const inputs = args[0] as Array<{
					appended: { appendCommit: { hash: string } };
				}>;
				committedHashes = inputs.map(
					(input) => input.appended.appendCommit.hash,
				);
				throw injectedFailure;
			});

		let failure: unknown;
		try {
			await opened.docs.putMany(docs, {
				unique: true,
				delivery: persistedDelivery,
			});
		} catch (error) {
			failure = error;
		}

		expect(constructFacts.callCount).equal(1);
		expect(committedHashes).to.have.length(docs.length);
		expect(new Set(committedHashes).size).equal(docs.length);
		expect(failure).instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).equal(injectedFailure);
		expect(persistedFailure.committedHashes).to.deep.equal(committedHashes);
		for (const hash of committedHashes) {
			expect(await opened.docs.log.log.has(hash)).equal(true);
			expect((await opened.docs.log.log.get(hash))?.hash).equal(hash);
		}
	});
});
