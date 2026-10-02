import { deserialize, serialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import {
	type SQLiteIndices,
	create as createSQLite,
} from "@peerbit/indexer-sqlite3";
import { Entry } from "@peerbit/log";
import { PersistedDeliveryError } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import {
	type CanPerform,
	DocumentBatchCommitError,
	Documents,
	PutOperation,
	policy,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

// Real signed entries and JS authorization on the auto backend. Faults below
// are injected only at the named storage/projection/delivery boundaries.
describe("required JS-authorized document batching", function () {
	this.timeout(30_000);
	let client: Peerbit | undefined;
	let store: TestStore;
	let directory: string | undefined;
	const options = { batching: "required" as const, unique: true };
	const documents = () =>
		["first", "second", "third"].map(
			(id) => new Document({ id, name: `${id}-value` }),
		);
	const open = async (canPerform: CanPerform<Document>, sqlite = false) => {
		client = await Peerbit.create({
			...createRustPeerbitOptions({ network: false }),
			directory,
			...(sqlite ? { indexer: createSQLite } : {}),
		});
		store = await client.open(
			new TestStore({ docs: new Documents<Document>() }),
			{
				args: {
					mode: "auto",
					replicate: false,
					nativeGraph: !sqlite,
					nativeBackbone: false,
					canPerform,
				},
			},
		);
		return store;
	};
	afterEach(async () => {
		sinon.restore();
		await client?.stop();
		client = undefined;
		if (directory) {
			await rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});
	const blockWrites = () => {
		const storage = (store.docs.log.log as any)._storage;
		return ["put", "putKnown", "putMany", "putKnownMany", "putKnownManyColumns"]
			.filter((name) => typeof storage[name] === "function")
			.map((name) => ({ name, spy: sinon.spy(storage, name) }));
	};
	const failed = async (promise: Promise<unknown>) => {
		const error = await promise.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).instanceOf(DocumentBatchCommitError);
		return error as DocumentBatchCommitError;
	};
	const assertAbsent = async (docs = documents()) => {
		for (const doc of docs)
			expect(await store.docs.get(doc.id)).equal(undefined);
	};
	const assertProjected = async (doc: Document) => {
		const actual = await store.docs.get(doc.id);
		expect(actual).not.equal(undefined);
		// Query results carry __context/__indexed metadata in addition to the
		// document fields. Compare every encoded document field, not that envelope.
		expect(serialize(actual!)).deep.equal(serialize(doc));
	};
	const assertStoredBytes = async (hash: string, expected: Uint8Array) => {
		const stored = await store.docs.log.log.blocks.get(hash);
		expect(stored).not.equal(undefined);
		expect(Uint8Array.from(stored!)).deep.equal(Uint8Array.from(expected));
		// Recompute from the captured pre-storage signed entry: serializing the
		// returned entry after hash assignment would include the new hash field.
		expect(await Entry.prepareMultihash(deserialize(expected, Entry))).equal(
			hash,
		);
	};
	const assertCommitted = async (
		error: DocumentBatchCommitError,
		docs = documents(),
	) => {
		expect(error.localCommit).equal("committed");
		expect(error.retrySafe).equal(false);
		expect(error.recoveryRequired).equal(false);
		expect(error.committedItems.map(({ index }) => index)).deep.equal(
			docs.map((_, index) => index),
		);
		for (const { index, hash } of error.committedItems) {
			const entry = await store.docs.log.log.get(hash);
			expect(entry).not.equal(undefined);
			expect(entry!.meta.next).deep.equal([]);
			expect(await entry!.verifySignatures()).equal(true);
			expect(
				deserialize(
					((await entry!.getPayloadValue()) as PutOperation).data,
					Document,
				),
			).deep.equal(docs[index]);
		}
		expect(
			(await store.docs.log.log.getHeads().all()).map(({ hash }) => hash),
		).to.have.members(error.committedItems.map(({ hash }) => hash));
	};

	it("authorizes in input order and commits independent signed heads with the default target", async () => {
		const calls: string[] = [];
		const signedBytes: Uint8Array[] = [];
		await open(async (properties) => {
			expect(properties.type).equal("put");
			if (properties.type !== "put") return false;
			calls.push(properties.value.id);
			expect(await properties.entry.verifySignatures()).equal(true);
			signedBytes.push(properties.entry.getStorageBytes()!.slice());
			return true;
		});
		const writes = blockWrites();
		const append = sinon.spy(
			store.docs.log as any,
			"appendLocallyPreparedPayloadsManyIndependent",
		);
		const fallback = sinon.spy(store.docs as any, "putManySequential");
		const metadata = sinon.spy(store.docs.log.log as any, "putAppendEntries");
		const processing = sinon.spy(store.docs.log as any, "processLocalAppend");
		const batchProcessing = sinon.spy(
			store.docs.log as any,
			"processLocalAppendManyNativePlanned",
		);
		const planning = sinon.spy(
			store.docs.log as any,
			"planNativeAppendEntries",
		);
		const docs = documents();
		const result = await store.docs.putMany(docs, options);
		expect(calls).deep.equal(docs.map((doc) => doc.id));
		expect(append.callCount).equal(1);
		expect(fallback.callCount).equal(0);
		expect(metadata.callCount).equal(1);
		expect(
			writes
				.filter(({ name }) => name.includes("Many"))
				.reduce((count, { spy }) => count + spy.callCount, 0),
		).equal(1);
		expect(writes.find(({ name }) => name === "put")?.spy.callCount).equal(0);
		expect(
			writes.find(({ name }) => name === "putKnown")?.spy.callCount ?? 0,
		).equal(0);
		expect(planning.callCount).equal(1);
		expect(
			planning.firstCall.args[0].map(({ hash }: { hash: string }) => hash),
		).deep.equal(result.entries.map(({ hash }) => hash));
		const processedAsBatch =
			batchProcessing.called &&
			(await batchProcessing.firstCall.returnValue) === true;
		expect(processedAsBatch || processing.callCount === docs.length).equal(
			true,
		);
		expect(result.entries).length(docs.length);
		for (const [index, entry] of result.entries.entries()) {
			expect(entry.meta.next).deep.equal([]);
			expect(await entry.verifySignatures()).equal(true);
			await assertStoredBytes(entry.hash, signedBytes[index]!);
			const operation = await entry.getPayloadValue();
			expect(operation).instanceOf(PutOperation);
			expect(
				deserialize((operation as PutOperation).data, Document),
			).deep.equal(docs[index]);
			await assertProjected(docs[index]!);
		}
		expect(
			(await store.docs.log.log.getHeads().all()).map((entry) => entry.hash),
		).to.have.members(result.entries.map((entry) => entry.hash));
	});

	it("keeps every outer write and projection absent while final authorization is gated, then preserves its rejection cause", async () => {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const gate = new Promise<void>((resolve) => (release = resolve));
		const cause = new Error("final JS authorization rejected");
		const calls: string[] = [];
		await open(async (properties) => {
			if (properties.type !== "put") return true;
			calls.push(properties.value.id);
			if (properties.value.id === "third") {
				entered();
				await gate;
				throw cause;
			}
			return true;
		});
		const writes = blockWrites();
		const metadata = sinon.spy(store.docs.log.log as any, "putAppendEntries");
		const projection = sinon.spy(
			store.docs as any,
			"handlePreparedPlainPutManyCommit",
		);
		const processing = sinon.spy(store.docs.log as any, "processLocalAppend");
		const publish = sinon.spy(client!.services.pubsub, "publish");
		const pending = store.docs.putMany(documents(), options);
		try {
			// A rejected eligibility check must fail immediately, not leave a test
			// hanging forever waiting for an authorization callback that never ran.
			await Promise.race([
				enteredPromise,
				pending.then(() => {
					throw new Error("batch settled before authorization gate");
				}),
			]);
			expect(calls).deep.equal(["first", "second", "third"]);
			for (const { spy } of writes) expect(spy.callCount).equal(0);
			expect(metadata.callCount).equal(0);
			expect(projection.callCount).equal(0);
			expect(processing.callCount).equal(0);
			expect(publish.callCount).equal(0);
			expect(store.docs.log.log.length).equal(0);
			expect(await store.docs.log.log.getHeads().all()).deep.equal([]);
			await assertAbsent();
			release();
			const error = await failed(pending);
			expect(error.cause).equal(cause);
			// appendStarted precedes preparation; no hashes alone do not prove
			// replay safety under the existing conservative outcome contract.
			expect(error.localCommit).equal("indeterminate");
			expect(error.retrySafe).equal(false);
			expect(error.recoveryRequired).equal(true);
			expect(error.committedItems).deep.equal([]);
			for (const { spy } of writes) expect(spy.callCount).equal(0);
			expect(store.docs.log.log.length).equal(0);
			await assertAbsent();
		} finally {
			release();
			await pending.catch(() => undefined);
		}
	});

	it("owns caller snapshots and detached authorization views while preserving scalar hash timing", async () => {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const gate = new Promise<void>((resolve) => (release = resolve));
		const calls: string[] = [];
		const signedBytes: Uint8Array[] = [];
		let scalarHash: string | undefined;
		const batchHashes: Array<string | undefined> = [];
		await open(async (properties) => {
			if (properties.type !== "put") return true;
			if (properties.value.id === "scalar") {
				scalarHash = properties.entry.hash;
				return true;
			}
			const id = properties.value.id;
			calls.push(id);
			batchHashes.push(properties.entry.hash);
			signedBytes.push(properties.entry.getStorageBytes()!.slice());
			if (id === "first") {
				entered();
				await gate;
			}
			properties.value.id = "callback-id";
			properties.value.name = "callback-name";
			properties.value.data?.fill(81);
			properties.value.tags.push("callback-tag");
			properties.entry.payload.encoding.encoder(properties.operation).fill(80);
			properties.operation.data.fill(82);
			(await properties.entry.getPayloadValue()).data.fill(83);
			properties.entry.meta.next.push("callback-next");
			properties.entry.meta.clock.id.fill(84);
			(await properties.entry.getSignatures())[0]!.signature.fill(85);
			(await properties.entry.getPublicKeys())[0]!.bytes.fill(86);
			properties.entry.getStorageBytes()!.fill(87);
			return true;
		});
		await store.docs.put(new Document({ id: "scalar" }), {
			unique: true,
			target: "none",
		});
		const docs = documents();
		docs[1]!.data = new Uint8Array([1, 2, 3]);
		docs[1]!.tags = ["captured"];
		const captured = docs.map((doc) => deserialize(serialize(doc), Document));
		const mutableOptions: any = { ...options, delivery: false };
		const receipt = sinon.spy(
			store.docs.log as any,
			"deliverPersistedAppendCommits",
		);
		const pending = store.docs.putMany(docs, mutableOptions);
		try {
			await Promise.race([
				enteredPromise,
				pending.then(() => {
					throw new Error("batch settled before snapshot gate");
				}),
			]);
			docs[1]!.id = "caller-id";
			docs[1]!.name = "caller-name";
			docs[1]!.data!.fill(88);
			docs[1]!.tags.push("caller-tag");
			docs.reverse();
			docs.pop();
			mutableOptions.batching = undefined;
			mutableOptions.unique = false;
			mutableOptions.target = "all";
			mutableOptions.delivery = { reliability: "persisted", minAcks: 1 };
			release();
			const result = await pending;
			expect(calls).deep.equal(captured.map(({ id }) => id));
			expect(batchHashes).deep.equal(captured.map(() => scalarHash));
			expect(receipt.callCount).equal(0);
			for (const [index, entry] of result.entries.entries()) {
				await assertStoredBytes(entry.hash, signedBytes[index]!);
				expect(await entry.verifySignatures()).equal(true);
				expect(entry.meta.next).deep.equal([]);
				expect(
					deserialize(
						((await entry.getPayloadValue()) as PutOperation).data,
						Document,
					),
				).deep.equal(captured[index]);
				await assertProjected(captured[index]!);
			}
			await assertAbsent([
				new Document({ id: "caller-id" }),
				new Document({ id: "callback-id" }),
			]);
		} finally {
			release();
			await pending.catch(() => undefined);
		}
	});

	it("allows an awaited nested put and preserves it when later outer authorization fails", async () => {
		const nested = new Document({ id: "nested", name: "survives" });
		const cause = new Error("outer authorization failed after nested write");
		const calls: string[] = [];
		await open(async (properties) => {
			if (properties.type !== "put") return true;
			calls.push(properties.value.id);
			if (properties.value.id === "first")
				await store.docs.put(nested, { unique: true });
			if (properties.value.id === "second") {
				await assertProjected(nested);
				await assertAbsent();
			}
			if (properties.value.id === "third") throw cause;
			return true;
		});
		const error = await failed(store.docs.putMany(documents(), options));
		expect(calls).deep.equal(["first", "nested", "second", "third"]);
		expect(error.cause).equal(cause);
		expect(error.localCommit).equal("indeterminate");
		expect(error.committedItems).deep.equal([]);
		await assertProjected(nested);
		expect(store.docs.log.log.length).equal(1);
		expect(await store.docs.log.log.getHeads().all()).length(1);
		await assertAbsent();
	});

	it("isolates a valid encoder-byte mutation from persisted document projections", async () => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-required-js-encoder-"));
		const signedBytes: Uint8Array[] = [];
		await open((properties) => {
			if (properties.type !== "put") return true;
			signedBytes.push(properties.entry.getStorageBytes()!.slice());
			const encoded = properties.entry.payload.encoding.encoder(
				properties.operation,
			);
			const nameOffset = Buffer.from(encoded).indexOf(properties.value.name!);
			expect(nameOffset).at.least(0);
			// Keep the key, lengths, and Borsh tags valid. Corrupting the whole
			// payload can merely trigger a fallback to object-based encoding.
			encoded[nameOffset] = "X".charCodeAt(0);
			return true;
		});
		const clone = store.clone();
		const docs = documents();
		const result = await store.docs.putMany(docs, options);
		for (const [index, entry] of result.entries.entries()) {
			await assertStoredBytes(entry.hash, signedBytes[index]!);
			expect(await entry.verifySignatures()).equal(true);
			const row = await store.docs.index.index.get(toId(docs[index]!.id));
			expect(row).not.equal(undefined);
			expect(row!.value.name).equal(docs[index]!.name);
		}
		await client!.stop();
		client = await Peerbit.create({
			...createRustPeerbitOptions({ network: false }),
			directory,
		});
		store = await client.open(clone, {
			args: {
				mode: "auto",
				replicate: false,
				nativeGraph: true,
				nativeBackbone: false,
				canPerform: () => true,
			},
		});
		for (const doc of docs) {
			const row = await store.docs.index.index.get(toId(doc.id));
			expect(row).not.equal(undefined);
			expect(row!.value.name).equal(doc.name);
			await assertProjected(doc);
		}
	});

	for (const terminal of ["close", "drop"] as const) {
		it(`fails fast on reentrant lower ${terminal} without closing the store`, async () => {
			await open(async (properties) => {
				if (properties.type === "put" && properties.value.id === "first")
					await store.docs.log.log[terminal]();
				return true;
			});
			const error = await failed(store.docs.putMany(documents(), options));
			expect(error.cause).instanceOf(Error);
			expect((error.cause as Error).message).include(
				`Cannot ${terminal} a log while a mutation callback is running`,
			);
			expect(error.localCommit).equal("indeterminate");
			expect(error.committedItems).deep.equal([]);
			expect(store.docs.log.log.length).equal(0);
			await store.docs.put(new Document({ id: "after-terminal" }), {
				unique: true,
			});
			expect(await store.docs.get("after-terminal")).not.equal(undefined);
		});
	}

	it("keeps ordinary unflagged putMany sequential prefix visibility", async () => {
		const observed: Array<[string, number, boolean]> = [];
		await open(async (properties) => {
			if (properties.type === "put")
				observed.push([
					properties.value.id,
					store.docs.log.log.length,
					(await store.docs.get("first")) !== undefined,
				]);
			return true;
		});
		const sequential = sinon.spy(store.docs as any, "putManySequential");
		await store.docs.putMany(documents(), { unique: true });
		expect(sequential.callCount).equal(1);
		expect(observed).deep.equal([
			["first", 0, false],
			["second", 1, true],
			["third", 2, true],
		]);
	});

	it("reports every local append after a partial projection failure and never starts receipt delivery", async () => {
		await open(() => true);
		const cause = new Error("projection stopped after first document");
		const target = store.docs as any;
		const project = target.handlePreparedPlainPutManyCommit.bind(target);
		sinon
			.stub(target, "handlePreparedPlainPutManyCommit")
			.callsFake(async (...args: any[]) => {
				await project({ ...args[0], commits: args[0].commits.slice(0, 1) });
				throw cause;
			});
		const receipt = sinon.spy(
			store.docs.log as any,
			"deliverPersistedAppendCommits",
		);
		const error = await failed(
			store.docs.putMany(documents(), {
				...options,
				delivery: { reliability: "persisted", minAcks: 1 },
			}),
		);
		expect(error.cause).instanceOf(PersistedDeliveryError);
		expect((error.cause as PersistedDeliveryError).cause).equal(cause);
		await assertCommitted(error);
		await assertProjected(documents()[0]!);
		await assertAbsent(documents().slice(1));
		expect(receipt.callCount).equal(0);
	});

	it("retains complete local evidence when default-target processing fails before projection", async () => {
		await open(() => true);
		const cause = new Error("default-target processing failed");
		const processing = sinon
			.stub(store.docs.log as any, "planNativeAppendEntries")
			.rejects(cause);
		const projection = sinon.spy(
			store.docs as any,
			"handlePreparedPlainPutManyCommit",
		);
		const error = await failed(store.docs.putMany(documents(), options));
		expect(processing.callCount).equal(1);
		expect(projection.callCount).equal(0);
		expect(error.cause).equal(cause);
		await assertCommitted(error);
		await assertAbsent();
	});

	it("finishes projections before a persisted-receipt failure without treating receipt failure as rollback", async () => {
		await open(() => true);
		const cause = new Error("remote persisted receipt failed");
		const receipt = sinon
			.stub(store.docs.log as any, "deliverPersistedAppendCommits")
			.callsFake(async (...args: any[]) => {
				for (const doc of documents()) await assertProjected(doc);
				throw new PersistedDeliveryError(
					cause,
					args[0].map(({ hash }: { hash: string }) => hash),
				);
			});
		const error = await failed(
			store.docs.putMany(documents(), {
				...options,
				delivery: { reliability: "persisted", minAcks: 1 },
			}),
		);
		expect(receipt.callCount).equal(1);
		expect(error.cause).instanceOf(PersistedDeliveryError);
		expect((error.cause as PersistedDeliveryError).cause).equal(cause);
		await assertCommitted(error);
	});

	it("compensates a second 64-row metadata chunk failure and reopens with only the prior document", async () => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-required-js-batch-"));
		await open(() => true, true);
		const prior = new Document({ id: "prior", name: "unchanged" });
		const priorEntry = (
			await store.docs.put(prior, { unique: true, target: "none" })
		).entry;
		const clone = store.clone();
		const docs = Array.from(
			{ length: 65 },
			(_, index) => new Document({ id: `batch-${index}` }),
		);
		const cause = new Error("second metadata chunk rejected");
		const db = (client!.indexer as SQLiteIndices).properties.db;
		const exec = db.exec.bind(db);
		let chunks = 0;
		let released = 0;
		const fault = sinon.stub(db, "exec").callsFake(async (sql) => {
			if (sql.startsWith("SAVEPOINT peerbit_put_batch_") && ++chunks === 2) {
				expect(released).equal(1);
				throw cause;
			}
			const value = await exec(sql);
			if (sql.startsWith("RELEASE SAVEPOINT peerbit_put_batch_")) released++;
			return value;
		});
		const error = await failed(store.docs.putMany(docs, options));
		fault.restore();
		expect(chunks).equal(2);
		expect(error.cause).equal(cause);
		expect(error.localCommit).equal("indeterminate");
		expect(error.retrySafe).equal(false);
		expect(error.recoveryRequired).equal(true);
		expect(error.committedItems).deep.equal([]);
		expect(store.docs.log.log.length).equal(1);
		expect(
			(await store.docs.log.log.getHeads().all()).map(({ hash }) => hash),
		).deep.equal([priorEntry.hash]);
		await assertProjected(prior);
		await assertAbsent(docs);
		await client!.stop();
		client = await Peerbit.create({
			...createRustPeerbitOptions({ network: false }),
			directory,
			indexer: createSQLite,
		});
		store = await client.open(clone, {
			args: {
				mode: "auto",
				replicate: false,
				nativeGraph: false,
				nativeBackbone: false,
				canPerform: () => true,
			},
		});
		expect(store.docs.log.log.length).equal(1);
		expect(
			(await store.docs.log.log.getHeads().all()).map(({ hash }) => hash),
		).deep.equal([priorEntry.hash]);
		await assertProjected(prior);
		await assertAbsent(docs);
	});

	for (const configuration of [
		{
			label: "JS authorization with target none",
			canPerform: (() => true) as CanPerform<Document>,
			target: "none" as const,
		},
		{
			label: "a native descriptor in auto mode with the default target",
			canPerform: policy.allowAll<Document>(),
			target: undefined,
		},
	]) {
		it(`supports ${configuration.label} without sequential fallback`, async () => {
			await open(configuration.canPerform);
			const fallback = sinon.spy(store.docs as any, "putManySequential");
			const result = await store.docs.putMany(documents(), {
				...options,
				target: configuration.target,
			});
			expect(fallback.callCount).equal(0);
			expect(result.entries).length(3);
			for (const doc of documents()) await assertProjected(doc);
			expect(
				(await store.docs.log.log.getHeads().all()).map(({ hash }) => hash),
			).to.have.members(result.entries.map(({ hash }) => hash));
		});
	}

	it("delivers a default-target JS batch and a real persisted receipt batch to a durable remote replica", async () => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-required-js-receipt-"));
		let session: TestSession | undefined;
		try {
			// The receiver uses the default durable Level/SQLite stores, as in
			// the persisted-delivery fixture: RustIndex does not advertise the
			// crashSafeDurability contract required for persisted receipts.
			session = await TestSession.connected(2, [
				createRustPeerbitOptions(),
				{ directory },
			]);
			const calls: string[][] = [[], []];
			const canPerform =
				(index: number): CanPerform<Document> =>
				async (properties) => {
					if (properties.type === "put") {
						calls[index]!.push(properties.value.id);
						expect(await properties.entry.verifySignatures()).equal(true);
					}
					return true;
				};
			const args = {
				mode: "auto" as const,
				nativeGraph: true,
				nativeBackbone: false as const,
				replicas: { min: 1 },
				timeUntilRoleMaturity: 0,
			};
			store = await session.peers[0].open(
				new TestStore({ docs: new Documents<Document>() }),
				{
					args: { ...args, replicate: false, canPerform: canPerform(0) },
				},
			);
			const receiver = await session.peers[1].open(store.clone(), {
				args: {
					...args,
					replicate: { offset: 0, factor: 1 },
					canPerform: canPerform(1),
				},
			});
			await store.docs.log.waitForReplicator(
				session.peers[1].identity.publicKey,
				{ roleAge: 0, timeout: 15_000 },
			);
			await store.docs.log.waitForPersistedReceiptPeerReadiness(
				session.peers[1].identity.publicKey,
				{ timeout: 15_000 },
			);
			const ordinaryDocs = [
				new Document({ id: "default-first" }),
				new Document({ id: "default-second" }),
			];
			const persistedDocs = [
				new Document({ id: "receipt-first" }),
				new Document({ id: "receipt-second" }),
			];
			const fallback = sinon.spy(store.docs as any, "putManySequential");
			const ordinary = await store.docs.putMany(ordinaryDocs, options);
			await waitForResolved(async () => {
				for (const doc of ordinaryDocs) {
					const remote = await receiver.docs.get(doc.id, {
						local: true,
						remote: false,
					});
					expect(remote).not.equal(undefined);
					expect(serialize(remote!)).deep.equal(serialize(doc));
				}
			});
			const persisted = await store.docs.putMany(persistedDocs, {
				...options,
				target: "replicators",
				delivery: { reliability: "persisted", minAcks: 1, timeout: 15_000 },
			});
			expect(fallback.callCount).equal(0);
			expect(calls[0]).deep.equal(
				[...ordinaryDocs, ...persistedDocs].map(({ id }) => id),
			);
			expect(calls[1]).include.members(
				[...ordinaryDocs, ...persistedDocs].map(({ id }) => id),
			);
			for (const [index, entry] of persisted.entries.entries()) {
				const [block, row, coordinate, document] = await Promise.all([
					(receiver.docs.log as any).remoteBlocks.localStore.has(entry.hash),
					receiver.docs.log.log.entryIndex.properties.index.get(
						toId(entry.hash),
					),
					receiver.docs.log.entryCoordinatesIndex.get(toId(entry.hash)),
					receiver.docs.get(persistedDocs[index]!.id, {
						local: true,
						remote: false,
					}),
				]);
				expect(block).equal(true);
				expect(row).not.equal(undefined);
				expect(row!.value.head).equal(true);
				expect(coordinate).not.equal(undefined);
				expect(document).not.equal(undefined);
				expect(serialize(document!)).deep.equal(
					serialize(persistedDocs[index]!),
				);
				expect(await entry.verifySignatures()).equal(true);
			}
			expect(
				(await receiver.docs.log.log.getHeads().all()).map(({ hash }) => hash),
			).to.have.members(
				[...ordinary.entries, ...persisted.entries].map(({ hash }) => hash),
			);
		} finally {
			await session?.stop();
		}
	});
});
