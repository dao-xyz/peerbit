import { deserialize, serialize } from "@dao-xyz/borsh";
import { NativeBackboneMemoryCoordinatePersistenceStore } from "@peerbit/native-backbone";
import {
	NativeDurableCommitError,
	PersistedDeliveryError,
} from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import {
	DocumentBatchCommitError,
	Documents,
	PutOperation,
	policy,
	transform,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

describe("required document batching", function () {
	this.timeout(120_000);
	let session: TestSession;
	let store: TestStore | undefined;
	before(async () => {
		session = await TestSession.connected(1, createRustPeerbitOptions());
	});
	afterEach(async () => {
		sinon.restore();
		await store?.close();
		store = undefined;
	});
	after(async () => {
		await session.stop();
	});
	const openStore = async (mode: "auto" | "native" | "compat") => {
		store = new TestStore({ docs: new Documents<Document>() });
		await session.peers[0].open(store, {
			args: {
				mode,
				replicate: false,
				nativeGraph: true,
				...(mode === "native"
					? {
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
					: {}),
			},
		});
		return store;
	};
	const options = {
		batching: "required" as const,
		unique: true,
		target: "none" as const,
	};
	const documents = () => [
		new Document({ id: "first", name: "first-value" }),
		new Document({ id: "second", name: "second-value" }),
		new Document({ id: "third", name: "third-value" }),
	];
	const failed = async (promise: Promise<unknown>) => {
		const failure = await promise.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(failure).instanceOf(DocumentBatchCommitError);
		return failure as DocumentBatchCommitError;
	};
	const assertCommitted = (
		failure: DocumentBatchCommitError,
		hashes: string[],
	) => {
		expect(failure.localCommit).equal("committed");
		expect(failure.retrySafe).equal(false);
		expect(failure.recoveryRequired).equal(false);
		expect(failure.committedItems).deep.equal(
			hashes.map((hash, index) => ({ index, hash })),
		);
		expect(Object.isFrozen(failure)).equal(true);
		expect(Object.isFrozen(failure.committedItems)).equal(true);
		for (const item of failure.committedItems)
			expect(Object.isFrozen(item)).equal(true);
	};

	for (const mode of ["auto", "native"] as const) {
		it(`keeps success entries in captured input order in ${mode} mode`, async () => {
			const opened = await openStore(mode);
			const docs = documents();
			const append = sinon.spy(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			);
			const fallback = sinon.spy(opened.docs as any, "putManySequential");
			const result = await opened.docs.putMany(docs, options);
			expect(result.entries).length(docs.length);
			for (const [index, entry] of result.entries.entries()) {
				const operation = await entry.getPayloadValue();
				expect(
					deserialize((operation as PutOperation).data, Document),
				).deep.equal(docs[index]);
			}
			expect(append.callCount).equal(1);
			expect(fallback.callCount).equal(0);
		});

		it(`rejects duplicate keys before append without fallback in ${mode} mode`, async () => {
			const opened = await openStore(mode);
			const append = sinon.spy(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			);
			const fallback = sinon.spy(opened.docs as any, "putManySequential");
			const failure = await failed(
				opened.docs.putMany(
					[
						new Document({ id: "duplicate", name: "first" }),
						new Document({ id: "duplicate", name: "second" }),
					],
					options,
				),
			);
			expect(failure.localCommit).equal("not-started");
			expect(failure.retrySafe).equal(true);
			expect(failure.committedItems).deep.equal([]);
			expect(append.callCount).equal(0);
			expect(fallback.callCount).equal(0);
			expect(opened.docs.log.log.length).equal(0);
		});

		it(`reports exact immutable commit indexes after ${mode} projection failure`, async () => {
			const opened = await openStore(mode);
			const cause = new Error("projection failed after append");
			const append = sinon.spy(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			);
			sinon
				.stub(opened.docs as any, "handlePreparedPlainPutManyCommit")
				.rejects(cause);
			const failure = await failed(opened.docs.putMany(documents(), options));
			const result = await append.firstCall.returnValue;
			const hashes = result.appendCommits.map(
				(commit: { hash: string }) => commit.hash,
			);
			assertCommitted(failure, hashes);
			expect(failure.cause).equal(cause);
			for (const hash of hashes)
				expect(await opened.docs.log.log.has(hash)).equal(true);
		});

		it(`keeps local commit evidence separate from remote receipt failure in ${mode} mode`, async () => {
			const opened = await openStore(mode);
			const cause = new Error("remote receipt did not arrive");
			const receipt = sinon
				.stub(opened.docs.log as any, "deliverPersistedAppendCommits")
				.callsFake(async (...args: any[]) => {
					throw new PersistedDeliveryError(
						cause,
						args[0].map((commit: { hash: string }) => commit.hash),
					);
				});
			const failure = await failed(
				opened.docs.putMany(documents(), {
					batching: "required",
					unique: true,
					delivery: { reliability: "persisted", minAcks: 1 },
				}),
			);
			const hashes = receipt.firstCall.args[0].map(
				(commit: { hash: string }) => commit.hash,
			);
			assertCommitted(failure, hashes);
			expect(failure.cause).instanceOf(PersistedDeliveryError);
			expect((failure.cause as PersistedDeliveryError).cause).equal(cause);
		});
	}

	it("rejects unsupported compat mode and options without sequential fallback", async () => {
		const opened = await openStore("compat");
		const fallback = sinon.spy(opened.docs as any, "putManySequential");
		const failure = await failed(opened.docs.putMany(documents(), options));
		expect(failure.localCommit).equal("not-started");
		expect(failure.retrySafe).equal(true);
		expect(fallback.callCount).equal(0);
		expect(opened.docs.log.log.length).equal(0);
	});

	it("rejects custom per-entry append options before mutation", async () => {
		const opened = await openStore("auto");
		const append = sinon.spy(
			opened.docs.log as any,
			"appendLocallyPreparedPayloadsManyIndependent",
		);
		const failure = await failed(
			opened.docs.putMany(documents(), {
				...options,
				canAppend: () => true,
			}),
		);
		expect(failure.localCommit).equal("not-started");
		expect(append.callCount).equal(0);
		expect(opened.docs.log.log.length).equal(0);
	});

	it("preserves pre-aborted delivery cancellation as a not-started local outcome", async () => {
		const opened = await openStore("native");
		const abort = new AbortController();
		const cause = new Error("cancelled before local append");
		abort.abort(cause);
		const append = sinon.spy(
			opened.docs.log as any,
			"appendLocallyPreparedPayloadsManyIndependent",
		);
		const failure = await failed(
			opened.docs.putMany(documents(), {
				batching: "required",
				unique: true,
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					signal: abort.signal,
				},
			}),
		);
		expect(failure.localCommit).equal("not-started");
		expect(failure.retrySafe).equal(true);
		expect(failure.cause).equal(cause);
		expect(append.callCount).equal(0);
	});

	it("retains exact ordered hashes when lower append commits before throwing", async () => {
		const opened = await openStore("auto");
		const cause = new Error("trim failed after lower append commit");
		sinon
			.stub(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch",
			)
			.resolves(undefined);
		sinon.stub(opened.docs.log.log as any, "trimIfConfigured").rejects(cause);
		const failure = await failed(opened.docs.putMany(documents(), options));
		expect(failure.localCommit).equal("committed");
		expect(failure.cause).equal(cause);
		expect(failure.committedItems).length(3);
		for (const { index, hash } of failure.committedItems) {
			const entry = await opened.docs.log.log.get(hash);
			expect(entry).not.equal(undefined);
			const operation = await entry!.getPayloadValue();
			expect(deserialize((operation as PutOperation).data, Document).id).equal(
				documents()[index]!.id,
			);
		}
	});

	it("captures every encoded input and options before asynchronous admission", async () => {
		const opened = await openStore("native");
		const target = opened.docs as any;
		const original =
			target.assertNativeModePlainPutPolicySupported.bind(target);
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const gate = new Promise<void>((resolve) => (release = resolve));
		let gated = false;
		sinon
			.stub(target, "assertNativeModePlainPutPolicySupported")
			.callsFake(async (...args: unknown[]) => {
				if (!gated) {
					gated = true;
					entered();
					await gate;
				}
				return original(...args);
			});
		const docs = documents();
		docs[1]!.data = new Uint8Array([1, 2, 3]);
		docs[1]!.tags = ["captured"];
		const captured = docs.map(
			(doc) =>
				new Document({ ...doc, tags: [...doc.tags], data: doc.data?.slice() }),
		);
		const mutableOptions: any = {
			...options,
			delivery: false,
			replicate: false,
		};
		const receipt = sinon.spy(
			opened.docs.log as any,
			"deliverPersistedAppendCommits",
		);
		try {
			const pending = opened.docs.putMany(docs, mutableOptions);
			await enteredPromise;
			docs[1]!.id = "mutated";
			docs[1]!.name = "mutated";
			docs[1]!.data!.fill(9);
			docs[1]!.tags.push("mutated");
			docs.reverse();
			docs.pop();
			mutableOptions.batching = undefined;
			mutableOptions.unique = false;
			mutableOptions.target = "replicators";
			mutableOptions.delivery = { reliability: "persisted", minAcks: 1 };
			release();
			const result = await pending;
			expect(result.entries).length(captured.length);
			for (const [index, entry] of result.entries.entries()) {
				const operation = await entry.getPayloadValue();
				expect(
					deserialize((operation as PutOperation).data, Document),
				).deep.equal(captured[index]);
				expect(
					serialize((await opened.docs.get(captured[index]!.id))!),
				).deep.equal(serialize(captured[index]!));
			}
			expect(receipt.callCount).equal(0);
		} finally {
			release();
		}
	});

	for (const result of ["undefined", "throw"] as const) {
		it(`requires recovery after an append ${result} without commit evidence`, async () => {
			const opened = await openStore("auto");
			const append = sinon.stub(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			);
			if (result === "undefined") append.resolves(undefined);
			else append.rejects(new Error("unknown native commit outcome"));
			const fallback = sinon.spy(opened.docs as any, "putManySequential");
			const failure = await failed(opened.docs.putMany(documents(), options));
			expect(failure.localCommit).equal("indeterminate");
			expect(failure.retrySafe).equal(false);
			expect(failure.recoveryRequired).equal(true);
			expect(failure.committedItems).deep.equal([]);
			expect(fallback.callCount).equal(0);
		});
	}

	it("copies non-Uint8Array resolver keys before asynchronous index preparation", async () => {
		const opened = await openStore("auto");
		const docs = documents();
		const buffers = docs.map((_, index) => new Uint8Array([99, index, 42, 99]));
		sinon
			.stub(opened.docs as any, "idResolver")
			.callsFake((...args: unknown[]) => {
				const doc = args[0] as Document;
				const buffer = buffers[docs.indexOf(doc)]!;
				return new DataView(buffer.buffer, 1, 2);
			});
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const gate = new Promise<void>((resolve) => (release = resolve));
		let capturedKeys: Uint8Array[] = [];
		sinon
			.stub(opened.docs as any, "prepareNativeBackboneDocumentIndexCommitBatch")
			.callsFake(async (...args: unknown[]) => {
				entered();
				await gate;
				capturedKeys = (args[0] as Array<{ key: { key: Uint8Array } }>).map(
					({ key }) => key.key,
				);
				throw new Error("stop before native append");
			});
		try {
			const pending = failed(opened.docs.putMany(docs, options));
			await enteredPromise;
			for (const buffer of buffers) buffer.fill(7);
			release();
			const failure = await pending;
			expect(failure.localCommit).equal("not-started");
			expect(capturedKeys).deep.equal(
				docs.map((_, index) => new Uint8Array([index, 42])),
			);
		} finally {
			release();
		}
	});

	it("never invents input indexes from partial trusted evidence", async () => {
		const opened = await openStore("auto");
		sinon
			.stub(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			)
			.callsFake(async (...args: any[]) => {
				args[2].localCommitEvidence.committedHashes.add(
					"known-but-unmapped-hash",
				);
				throw new Error("incomplete evidence");
			});
		const failure = await failed(opened.docs.putMany(documents(), options));
		expect(failure.localCommit).equal("indeterminate");
		expect(failure.retrySafe).equal(false);
		expect(failure.recoveryRequired).equal(true);
		expect(failure.committedItems).deep.equal([]);
	});

	it("keeps native durability failures indeterminate even with complete append evidence", async () => {
		const opened = await openStore("auto");
		const hashes = ["first-hash", "second-hash", "third-hash"];
		const cause = new NativeDurableCommitError(
			new Error("durable barrier failed"),
			{ committedCids: hashes },
		);
		sinon
			.stub(
				opened.docs.log as any,
				"appendLocallyPreparedPayloadsManyIndependent",
			)
			.callsFake(async (...args: any[]) => {
				for (const hash of hashes)
					args[2].localCommitEvidence.committedHashes.add(hash);
				throw cause;
			});
		const failure = await failed(opened.docs.putMany(documents(), options));
		expect(failure.localCommit).equal("indeterminate");
		expect(failure.retrySafe).equal(false);
		expect(failure.recoveryRequired).equal(true);
		expect(failure.committedItems).deep.equal(
			hashes.map((hash, index) => ({ index, hash })),
		);
		expect(failure.cause).equal(cause);
	});

	it("retains legacy sequential fallback when batching is not required", async () => {
		const opened = await openStore("compat");
		const fallback = sinon.spy(opened.docs as any, "putManySequential");
		const result = await opened.docs.putMany(documents(), {
			unique: true,
			target: "none",
		});
		expect(result.entries).length(3);
		expect(fallback.callCount).equal(1);
	});

	it("does not append an empty required batch", async () => {
		const opened = await openStore("native");
		const append = sinon.spy(
			opened.docs.log as any,
			"appendLocallyPreparedPayloadsManyIndependent",
		);
		expect(await opened.docs.putMany([], options)).deep.equal({
			entries: [],
			removed: [],
		});
		expect(append.callCount).equal(0);
	});
});
