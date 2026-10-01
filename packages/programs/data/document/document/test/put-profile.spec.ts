import type { DiagnosticEvent, DiagnosticSink } from "@peerbit/diagnostics";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { type CanPerform, Documents } from "../src/index.js";
import { Document, TestStore } from "./data.js";

type ObserverMode = "disabled" | "recording" | "throwing" | "async-rejecting";
const observerModes: ObserverMode[] = [
	"disabled",
	"recording",
	"throwing",
	"async-rejecting",
];
const phases = [
	"documents.put.prepare",
	"documents.put.authorize",
	"documents.put.projection",
	"sharedLog.append.localProcessing",
	"log.append.createEntry",
	"log.append.entryIndex",
	"entry.create.payload",
	"entry.create.signable",
	"entry.create.sign",
	"entry.create.authorize",
	"entry.create.storage",
];
const putOptions = { unique: true, target: "none" as const };

describe("document put diagnostics", () => {
	let session: TestSession;
	let sandbox: sinon.SinonSandbox;
	let stores: TestStore[];
	let releaseGates: (() => void)[];
	let pending: Promise<unknown>[];
	let rejectedSinks: Promise<never>[];

	before(async () => {
		session = await TestSession.disconnected(1);
	});
	beforeEach(() => {
		sandbox = sinon.createSandbox();
		stores = [];
		releaseGates = [];
		pending = [];
		rejectedSinks = [];
	});
	afterEach(async () => {
		for (const release of releaseGates) release();
		try {
			await Promise.all(pending);
			// Unowned observer rejections must reach the strict runner, even when
			// an assertion fails; cleanup must not accidentally make them harmless.
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			await Promise.allSettled(rejectedSinks);
		} finally {
			sandbox.restore();
			await Promise.all(stores.map((store) => store.close()));
		}
	});
	after(async () => {
		await session?.stop();
	});

	const gate = () => {
		const deferred = pDefer<void>();
		releaseGates.push(() => deferred.resolve());
		return deferred;
	};
	const track = <T>(operation: Promise<T>): Promise<T> => {
		pending.push(
			operation.then(
				() => undefined,
				() => undefined,
			),
		);
		return operation;
	};
	const waitUntil = (entered: Promise<void>, operation: Promise<unknown>) =>
		Promise.race([
			entered,
			operation.then(() => {
				throw new Error("put completed before its controlled gate");
			}),
		]);
	const observer = (mode: ObserverMode = "recording") => {
		const events: DiagnosticEvent[] = [];
		const sentinel = new Error("private-observer-failure");
		const sink: DiagnosticSink | undefined =
			mode === "disabled"
				? undefined
				: (event) => {
						// sync.profile also reaches older emitters. Only the new invocation
						// trace promises observer isolation; never throw into legacy events.
						if (!event.traceId?.startsWith("documents.put:")) return;
						events.push(structuredClone(event));
						if (mode === "throwing") throw sentinel;
						if (mode === "async-rejecting") {
							const rejected = Promise.reject<never>(sentinel);
							rejectedSinks.push(rejected);
							return rejected;
						}
					};
		return { events, sink };
	};
	const open = async (
		profile?: DiagnosticSink,
		canPerform: CanPerform<Document> = () => true,
	) => {
		const store = new TestStore({ docs: new Documents<Document>() });
		stores.push(store);
		await session.peers[0].open(store, {
			args: {
				replicate: false,
				keep: "self",
				nativeGraph: false,
				nativeBackbone: false,
				nativeRangePlanner: false,
				canPerform,
				...(profile ? { sync: { profile } } : {}),
			},
		});
		return store.docs;
	};
	const terminals = (events: DiagnosticEvent[]) =>
		events.filter((event) => event.name === "documents.put.settle");
	const assertTrace = (
		events: DiagnosticEvent[],
		outcome: "success" | "error",
		complete = false,
	) => {
		expect(new Set(events.map((event) => event.traceId)).size).equal(1);
		expect(events[0]?.traceId).match(/^documents\.put:/);
		const settled = terminals(events);
		expect(settled).to.have.length(1);
		expect(settled[0]!.details?.outcome).equal(outcome);
		for (const event of events) {
			expect(event.details?.v).equal(1);
			for (const [key, value] of Object.entries(event.details ?? {})) {
				expect(key).not.match(
					/^(id|documentId|hash|error|message|payload|value)$/,
				);
				expect(
					value === undefined ||
						["string", "number", "boolean"].includes(typeof value),
				).equal(true);
			}
			if (event.durationMs !== undefined) {
				expect(Number.isFinite(event.durationMs)).equal(true);
				expect(event.durationMs).to.be.at.least(0);
				expect(event.durationMs).to.be.at.most(settled[0]!.durationMs!);
			}
		}
		expect(JSON.stringify(events)).not.contain("private-");
		if (complete)
			for (const name of phases) {
				const matching = events.filter(
					(event) => event.name === name && event.durationMs !== undefined,
				);
				expect(matching, name).to.have.length(1);
			}
	};

	for (const mode of observerModes) {
		it(`preserves interleaved results and change ordering with ${mode} observers`, async () => {
			const { events, sink } = observer(mode);
			const entered = Array.from({ length: 4 }, gate);
			const release = Array.from({ length: 4 }, gate);
			const docs = await open(sink, async (properties) => {
				if (properties.type === "put") {
					const index = Number(properties.value.name);
					entered[index]!.resolve();
					await release[index]!.promise;
				}
				return true;
			});
			const changes: string[] = [];
			docs.events.addEventListener("change", (event) => {
				changes.push(...event.detail.added.map((document) => document.id));
			});
			const writes = Array.from({ length: 4 }, (_, index) =>
				track(
					docs.put(
						new Document({ id: `private-put-${index}`, name: String(index) }),
						putOptions,
					),
				),
			);
			await Promise.all(
				entered.map((value, index) => waitUntil(value.promise, writes[index]!)),
			);
			expect(terminals(events)).to.have.length(0);
			const order = [2, 0, 3, 1];
			for (const index of order) {
				release[index]!.resolve();
				await writes[index];
			}
			expect(changes).deep.equal(order.map((index) => `private-put-${index}`));
			for (const [index, result] of (await Promise.all(writes)).entries()) {
				expect(await result.entry.verifySignatures()).equal(true);
				expect(
					(await docs.get(`private-put-${index}`, { remote: false }))?.name,
				).equal(String(index));
			}
			if (mode === "disabled") expect(events).to.have.length(0);
			else {
				const ids = new Set(events.map((event) => event.traceId));
				expect(ids.size).equal(4);
				for (const id of ids)
					assertTrace(
						events.filter((event) => event.traceId === id),
						"success",
						true,
					);
			}
		});

		it(`preserves precommit storage errors with ${mode} observers`, async () => {
			const { events, sink } = observer(mode);
			const docs = await open(sink);
			const sentinel = new Error("private-storage-failure");
			const storage = sandbox
				.stub(docs.log.log.blocks, "put")
				.rejects(sentinel);
			const changes = sandbox.spy();
			docs.events.addEventListener("change", changes);
			const error = await track(
				docs.put(new Document({ id: "private-storage" }), putOptions),
			).catch((failure) => failure);
			expect(error).equal(sentinel);
			expect(storage.callCount).equal(1);
			expect(docs.log.log.length).equal(0);
			expect(await docs.index.getSize()).equal(0);
			expect(changes.callCount).equal(0);
			if (mode === "disabled") expect(events).to.have.length(0);
			else assertTrace(events, "error");
		});
	}

	for (const failure of ["deny", "throw"] as const) {
		it(`keeps authorization ${failure} fail-closed`, async () => {
			const { events, sink } = observer();
			const sentinel = new Error("private-authorization-failure");
			const docs = await open(sink, () => {
				if (failure === "throw") throw sentinel;
				return false;
			});
			const storage = sandbox.spy(docs.log.log.blocks, "put");
			const error = await track(
				docs.put(new Document({ id: "private-denied" }), putOptions),
			).catch((value) => value);
			if (failure === "throw") expect(error).equal(sentinel);
			else expect(error).to.have.property("message", "Not allowed to append");
			expect(storage.callCount).equal(0);
			expect(docs.log.log.length).equal(0);
			expect(await docs.index.getSize()).equal(0);
			assertTrace(events, "error");
		});
	}

	for (const property of ["getter", "data"] as const) {
		it(`ignores caller profile ${property} injection in persisted put options`, async () => {
			const { events, sink } = observer();
			const docs = await open(sink);
			const injected = sandbox.spy();
			const getter = sandbox.spy(() => injected);
			const options = Object.defineProperty(
				{
					unique: true,
					delivery: { reliability: "persisted" as const, minAcks: 1 },
				},
				"__peerbitProfile",
				property === "getter"
					? { enumerable: true, get: getter }
					: { enumerable: true, value: injected },
			);
			// Exercise the real local append and both option snapshots, not remote
			// receipt validation. Settlement is stubbed only to keep this test local.
			sandbox.stub(docs.log as any, "_appendDeliverToReplicators").resolves();
			const settle = sandbox
				.stub(docs.log as any, "settlePersistedDelivery")
				.resolves();
			const result = await track(
				docs.put(new Document({ id: `private-profile-${property}` }), options),
			);
			expect(getter.callCount).equal(0);
			expect(injected.callCount).equal(0);
			expect(settle.callCount).equal(1);
			expect(docs.log.log.length).equal(1);
			expect(await docs.index.getSize()).equal(1);
			expect(await result.entry.verifySignatures()).equal(true);
			assertTrace(events, "success", true);
		});
	}

	it("does not evaluate an unrelated profile getter in direct Log.append options", async () => {
		const canPerform = sandbox.spy(() => true);
		const docs = await open(undefined, canPerform);
		const first = await track(
			docs.put(new Document({ id: "private-direct-log" }), putOptions),
		);
		const injected = sandbox.spy();
		const getter = sandbox.spy(() => injected);
		const options = Object.defineProperty(
			{ meta: { next: [first.entry], data: first.entry.meta.data } },
			"__peerbitProfile",
			{ enumerable: true, get: getter },
		);
		const result = await track(
			docs.log.log.append(await first.entry.getPayloadValue(), options),
		);
		expect(getter.callCount).equal(0);
		expect(injected.callCount).equal(0);
		expect(canPerform.callCount).equal(2);
		expect(result.entry.hash).not.equal(first.entry.hash);
		expect(await docs.log.log.has(result.entry.hash)).equal(true);
		expect(await result.entry.verifySignatures()).equal(true);
	});

	it("reports projection failure without pretending the committed entry rolled back", async () => {
		const { events, sink } = observer("throwing");
		const docs = await open(sink);
		const sentinel = new Error("private-projection-failure");
		const projection = sandbox.stub(docs.index, "put").rejects(sentinel);
		const entryPut = sandbox.spy(docs.log.log.entryIndex, "put");
		const changes = sandbox.spy();
		docs.events.addEventListener("change", changes);
		const error = await track(
			docs.put(new Document({ id: "private-projection" }), putOptions),
		).catch((failure) => failure);
		expect(error).equal(sentinel);
		expect(projection.callCount).equal(1);
		expect(entryPut.callCount).equal(1);
		const entry = entryPut.firstCall.args[0];
		expect(docs.log.log.length).equal(1);
		expect(await docs.log.log.has(entry.hash)).equal(true);
		expect(await docs.log.log.blocks.get(entry.hash, { remote: false })).to
			.exist;
		expect(await docs.index.getSize()).equal(0);
		expect(changes.callCount).equal(0);
		assertTrace(events, "error");
	});

	it("binds canonical commit evidence before notifying the index observer", async () => {
		let capture: sinon.SinonSpy | undefined;
		const observedHashes: Array<string | undefined> = [];
		const docs = await open((event) => {
			if (
				event.traceId?.startsWith("documents.put:") &&
				event.name === "log.append.entryIndex"
			) {
				observedHashes.push(capture?.returnValues[0]?.hash);
			}
		});
		capture = sandbox.spy(docs.log as any, "capturePersistedLocalAppendCommit");
		// Keep the real local evidence capture; mocked settlement proves only
		// callback ordering, not remote receipt or durability guarantees.
		sandbox.stub(docs.log as any, "_appendDeliverToReplicators").resolves();
		const settle = sandbox
			.stub(docs.log as any, "settlePersistedDelivery")
			.resolves();
		const result = await track(
			docs.put(new Document({ id: "private-evidence" }), {
				unique: true,
				delivery: { reliability: "persisted", minAcks: 1 },
			}),
		);
		expect(capture.callCount).equal(1);
		expect(observedHashes).deep.equal([result.entry.hash]);
		expect(settle.firstCall.args[0][0].canonicalHash).equal(result.entry.hash);
	});

	it("keeps the outer trace pending until persisted delivery settles", async () => {
		const { events, sink } = observer();
		const docs = await open(sink);
		const entered = gate(),
			release = gate();
		// Only settlement scheduling is controlled; this is not receipt or
		// remote durability evidence. Local append and indexing remain real.
		sandbox.stub(docs.log as any, "_appendDeliverToReplicators").resolves();
		const settle = sandbox
			.stub(docs.log as any, "settlePersistedDelivery")
			.callsFake(async () => {
				entered.resolve();
				await release.promise;
			});
		const operation = track(
			docs.put(new Document({ id: "private-delivery" }), {
				unique: true,
				delivery: { reliability: "persisted", minAcks: 1 },
			}),
		);
		await waitUntil(entered.promise, operation);
		expect(docs.log.log.length).equal(1);
		expect(await docs.index.getSize()).equal(1);
		expect(terminals(events)).to.have.length(0);
		release.resolve();
		await operation;
		expect(settle.callCount).equal(1);
		assertTrace(events, "success", true);
	});
});
