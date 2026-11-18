import type { DiagnosticEvent, DiagnosticSink } from "@peerbit/diagnostics";
import { NativeBackboneMemoryCoordinatePersistenceStore } from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { type CanPerform, Documents, policy } from "../src/index.js";
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
// Phases a compatibility put has finished before its policy callback runs.
const preAuthorizationPhases = [
	"documents.put.prepare",
	"documents.put.path",
	"entry.create.payload",
	"entry.create.signable",
	"entry.create.sign",
];
const preparedPhases = [
	"documents.put.prepare",
	"documents.put.path",
	"documents.put.settle",
];
const putOptions = { unique: true, target: "none" as const };
const persistedPutOptions = () => ({
	unique: true,
	delivery: { reliability: "persisted" as const, minAcks: 1 },
});

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
		// A JavaScript canPerform selects the compatibility path; null keeps the
		// store on the default prepared path.
		canPerform: CanPerform<Document> | null = () => true,
		sync: { profile?: DiagnosticSink } | undefined = profile
			? { profile }
			: undefined,
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
				...(canPerform ? { canPerform } : {}),
				...(sync ? { sync } : {}),
			},
		});
		return store.docs;
	};
	// SharedLog creates this map on the first traced append only, so it stays
	// undefined unless a put took the traced append path.
	const appendProfileState = (docs: Documents<Document>) =>
		(docs.log as any)._appendProfiles as WeakMap<object, unknown> | undefined;
	const terminals = (events: DiagnosticEvent[]) =>
		events.filter((event) => event.name === "documents.put.settle");
	const outcomes = (events: DiagnosticEvent[], name: string) =>
		events
			.filter((event) => event.name === name)
			.map((event) => event.details?.outcome);
	const sortedNames = (events: DiagnosticEvent[]) =>
		events.map((event) => event.name).sort();
	const traceNumber = (traceId: string) => Number(traceId.split(":")[1]);
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
			expect(event.details).not.have.property("inclusive");
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
			const enteredAt: number[] = [];
			const releasedAt: number[] = [];
			const docs = await open(sink, async (properties) => {
				if (properties.type === "put") {
					const index = Number(properties.value.name);
					enteredAt[index] = performance.now();
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
			if (mode !== "disabled") {
				// Every put is parked inside its policy callback, so neither
				// authorization span nor any later phase may have been reported yet.
				const parked = new Map<string, string[]>();
				for (const event of events) {
					parked.set(event.traceId!, [
						...(parked.get(event.traceId!) ?? []),
						event.name,
					]);
				}
				expect(parked.size).equal(4);
				for (const names of parked.values()) {
					expect(names.sort()).deep.equal([...preAuthorizationPhases].sort());
				}
			}
			const order = [2, 0, 3, 1];
			for (const index of order) {
				releasedAt[index] = performance.now();
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
			if (mode === "disabled") {
				expect(appendProfileState(docs)).equal(undefined);
				return;
			}
			expect(appendProfileState(docs)).instanceOf(WeakMap);
			// put() creates its trace synchronously, so trace ids follow call order.
			const ids = [...new Set(events.map((event) => event.traceId!))].sort(
				(left, right) => traceNumber(left) - traceNumber(right),
			);
			expect(ids).to.have.length(4);
			for (const [index, id] of ids.entries()) {
				const trace = events.filter((event) => event.traceId === id);
				assertTrace(trace, "success", true);
				// Both authorization spans cover the time parked in canPerform.
				const parkedMs = releasedAt[index]! - enteredAt[index]!;
				for (const name of [
					"documents.put.authorize",
					"entry.create.authorize",
				]) {
					const span = trace.find((event) => event.name === name);
					expect(span?.durationMs, name).to.be.at.least(parkedMs);
				}
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
			if (mode === "disabled") {
				expect(appendProfileState(docs)).equal(undefined);
			} else {
				assertTrace(events, "error");
				expect(outcomes(events, "entry.create.storage")).deep.equal(["error"]);
			}
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
			// A denial is a returned policy decision, so only the entry creation
			// fails; a throwing policy fails both authorization phases.
			expect(outcomes(events, "documents.put.authorize")).deep.equal([
				failure === "deny" ? "success" : "error",
			]);
			expect(outcomes(events, "entry.create.authorize")).deep.equal(["error"]);
			expect(outcomes(events, "log.append.createEntry")).deep.equal(["error"]);
			expect(sortedNames(events)).deep.equal(
				[
					...preAuthorizationPhases,
					"documents.put.authorize",
					"entry.create.authorize",
					"log.append.createEntry",
					"documents.put.settle",
				].sort(),
			);
		});
	}

	it("ignores caller profile getter injection in persisted put options", async () => {
		const { events, sink } = observer();
		const docs = await open(sink);
		const injected = sandbox.spy();
		const getter = sandbox.spy(() => injected);
		const options = Object.defineProperty(
			persistedPutOptions(),
			"__peerbitProfile",
			{ enumerable: true, get: getter },
		);
		// Exercise the real local append and both option snapshots, not remote
		// receipt validation. Settlement is stubbed only to keep this test local.
		sandbox.stub(docs.log as any, "_appendDeliverToReplicators").resolves();
		const settle = sandbox
			.stub(docs.log as any, "settlePersistedDelivery")
			.resolves();
		const result = await track(
			docs.put(new Document({ id: "private-profile-getter" }), options),
		);
		expect(getter.callCount).equal(0);
		expect(injected.callCount).equal(0);
		expect(settle.callCount).equal(1);
		expect(docs.log.log.length).equal(1);
		expect(await docs.index.getSize()).equal(1);
		expect(await result.entry.verifySignatures()).equal(true);
		assertTrace(events, "success", true);
	});

	it("does not forward a caller profile data property from put options", async () => {
		// The check on SharedLog.append's input catches a leak from Documents'
		// option snapshot. The sink-call assertion only guards SharedLog's
		// overwrite, which would drop such a leak before the lower log reads it.
		const docs = await open();
		const injected = sandbox.spy();
		const append = sandbox.spy(docs.log, "append");
		const options = Object.defineProperty(
			{ ...putOptions },
			"__peerbitProfile",
			{ enumerable: true, value: injected },
		);
		const result = await track(
			docs.put(new Document({ id: "private-profile-data" }), options),
		);
		expect(append.callCount).equal(1);
		expect(
			Object.getOwnPropertyDescriptor(
				append.firstCall.args[1],
				"__peerbitProfile",
			),
		).equal(undefined);
		expect(injected.callCount).equal(0);
		expect(appendProfileState(docs)).equal(undefined);
		expect(docs.log.log.length).equal(1);
		expect(await result.entry.verifySignatures()).equal(true);
	});

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

	for (const property of ["data property", "getter"] as const) {
		it(`never calls a caller profile ${property} passed to SharedLog.append`, async () => {
			const docs = await open();
			const first = await track(
				docs.put(new Document({ id: "private-shared-log" }), putOptions),
			);
			const injected = sandbox.spy();
			// The public option spread evaluates an enumerable getter, as it does
			// for every option; the sink it returns must still never be called.
			const options = Object.defineProperty(
				{ target: "none" as const, meta: { next: [first.entry] } },
				"__peerbitProfile",
				property === "getter"
					? { enumerable: true, get: () => injected }
					: { enumerable: true, value: injected },
			);
			const result = await track(
				docs.log.append(await first.entry.getPayloadValue(), options),
			);
			expect(injected.callCount).equal(0);
			expect(appendProfileState(docs)).equal(undefined);
			expect(result.entry.hash).not.equal(first.entry.hash);
			expect(await docs.log.log.has(result.entry.hash)).equal(true);
			expect(await result.entry.verifySignatures()).equal(true);
		});
	}

	it("reports no append phases for a direct SharedLog.append on a traced store", async () => {
		// Keep the name of every sync.profile event, not only of documents.put:
		// traces, so an append phase sent straight to the store's callback shows.
		const names: string[] = [];
		const docs = await open((event) => {
			names.push(event.name);
		});
		const appendPhases = (from: number) =>
			names
				.slice(from)
				.filter((name) =>
					/^(sharedLog\.append\.localProcessing|log\.append\..+|entry\.create\..+)$/.test(
						name,
					),
				)
				.sort();
		const first = await track(
			docs.put(new Document({ id: "private-direct-traced" }), putOptions),
		);
		// The callback is live, and the filter matches every append phase a
		// traced compatibility put reports.
		expect(appendPhases(0)).deep.equal(
			phases.filter((name) => !name.startsWith("documents.put.")).sort(),
		);
		const before = names.length;
		const result = await track(
			docs.log.append(await first.entry.getPayloadValue(), {
				target: "none",
				meta: { next: [first.entry] },
			}),
		);
		expect(result.entry.hash).not.equal(first.entry.hash);
		expect(await docs.log.log.has(result.entry.hash)).equal(true);
		expect(await result.entry.verifySignatures()).equal(true);
		expect(appendPhases(before)).deep.equal([]);
	});

	it("stops tracing later puts when sync.profile is removed after open", async () => {
		const { events, sink } = observer();
		const sync: { profile?: DiagnosticSink } = { profile: sink };
		const entered = gate();
		const release = gate();
		const docs = await open(
			undefined,
			async (properties) => {
				if (
					properties.type === "put" &&
					properties.value.id === "private-live-in-flight"
				) {
					entered.resolve();
					await release.promise;
				}
				return true;
			},
			sync,
		);
		// SharedLog reads the sync object it kept at open: the caller's object,
		// unless node-level shared-log defaults replaced it with a copy.
		const retained = (docs.log as any)._logProperties.sync;
		const inFlight = track(
			docs.put(new Document({ id: "private-live-in-flight" }), putOptions),
		);
		await waitUntil(entered.promise, inFlight);
		delete sync.profile;
		if (retained !== sync) delete retained.profile;
		release.resolve();
		await inFlight;
		// The put that started with the callback still finishes its own trace.
		assertTrace(events, "success", true);
		const traced = events.length;
		const later = await track(
			docs.put(new Document({ id: "private-live-later" }), putOptions),
		);
		expect(await later.entry.verifySignatures()).equal(true);
		expect(events).to.have.length(traced);
	});

	for (const end of ["close", "drop"] as const) {
		it(`releases the caller's sync object on ${end}`, async () => {
			const sync = { profile: observer().sink };
			const docs = await open(undefined, undefined, sync);
			expect((docs as any)._putSync).equal(sync);
			// Ending the parent store ends the documents program through its own
			// close/drop, as an application shutdown does.
			expect(await stores[stores.length - 1]![end]()).equal(true);
			expect((docs as any)._putSync).equal(undefined);
		});
	}

	it("reports only preparation, path and settle on the prepared path", async () => {
		const { events, sink } = observer();
		const docs = await open(sink, null);
		const createAppendEntry = sandbox.spy(
			docs.log.log as any,
			"createAppendEntry",
		);
		const result = await track(
			docs.put(new Document({ id: "private-prepared" }), putOptions),
		);
		// The JavaScript fallback encoded, signed and stored the entry untraced.
		expect(createAppendEntry.callCount).equal(1);
		expect(await result.entry.verifySignatures()).equal(true);
		expect(await docs.index.getSize()).equal(1);
		assertTrace(events, "success");
		expect(events.map((event) => event.name)).deep.equal(preparedPhases);
		expect(events[1]!.details?.path).equal("prepared");
		expect(appendProfileState(docs)).equal(undefined);
	});

	it("keeps a prepared put pending until its own persisted delivery settles", async () => {
		const { events, sink } = observer();
		const docs = await open(sink, null);
		const entered = gate(),
			release = gate();
		// A prepared put commits locally first; put() then delivers it itself.
		// Settlement is stubbed only to keep this test local, so this shows the
		// terminal's ordering, not receipt or remote durability evidence.
		sandbox.stub(docs.log as any, "_appendDeliverToReplicators").resolves();
		const deliver = sandbox.spy(docs.log, "deliverPersistedEntries");
		const settle = sandbox
			.stub(docs.log as any, "settlePersistedDelivery")
			.callsFake(async () => {
				entered.resolve();
				await release.promise;
			});
		const operation = track(
			docs.put(
				new Document({ id: "private-prepared-delivery" }),
				persistedPutOptions(),
			),
		);
		await waitUntil(entered.promise, operation);
		expect(docs.log.log.length).equal(1);
		expect(terminals(events)).to.have.length(0);
		release.resolve();
		await operation;
		expect(deliver.callCount).equal(1);
		expect(settle.callCount).equal(1);
		assertTrace(events, "success");
		expect(events.map((event) => event.name)).deep.equal(preparedPhases);
		expect(events[1]!.details?.path).equal("prepared");
	});

	// An explicit "replicators" target delivers like an omitted one, but without
	// persisted delivery only the omitted target stays on the prepared path.
	for (const target of [undefined, "replicators"] as const) {
		const path = target ? "compatibility" : "prepared";
		it(`takes the ${path} path for a put ${target ? `with target "${target}"` : "without a target"}`, async () => {
			const { events, sink } = observer();
			// No canPerform, so only the put options choose the path.
			const docs = await open(sink, null);
			// Only the remote delivery step is stubbed, to keep this test local.
			const deliver = sandbox
				.stub(docs.log as any, "_appendDeliverToReplicators")
				.resolves();
			const result = await track(
				docs.put(new Document({ id: "private-target" }), {
					unique: true,
					...(target ? { target } : {}),
				}),
			);
			expect(deliver.callCount).equal(1);
			expect(await result.entry.verifySignatures()).equal(true);
			expect(
				events.find((event) => event.name === "documents.put.path")?.details
					?.path,
			).equal(path);
			assertTrace(events, "success", path === "compatibility");
			if (path === "prepared") {
				expect(events.map((event) => event.name)).deep.equal(preparedPhases);
			}
		});
	}

	it('takes the prepared path for a persisted put with target "replicators"', async () => {
		const { events, sink } = observer();
		// No canPerform, so only the put options choose the path.
		const docs = await open(sink, null);
		// Delivery and settlement are stubbed only to keep this test local; this
		// shows the path and its phases, not receipt or remote durability.
		sandbox.stub(docs.log as any, "_appendDeliverToReplicators").resolves();
		const settle = sandbox
			.stub(docs.log as any, "settlePersistedDelivery")
			.resolves();
		const result = await track(
			docs.put(new Document({ id: "private-target-persisted" }), {
				...persistedPutOptions(),
				target: "replicators",
			}),
		);
		expect(
			events.find((event) => event.name === "documents.put.path")?.details
				?.path,
		).equal("prepared");
		expect(settle.callCount).equal(1);
		expect(await result.entry.verifySignatures()).equal(true);
		assertTrace(events, "success");
		expect(events.map((event) => event.name)).deep.equal(preparedPhases);
	});

	// A put that fails before choosing its write path reports no
	// documents.put.path, whichever path the store would have taken.
	for (const store of ["compatibility", "prepared"] as const) {
		const openStore = (profile: DiagnosticSink | undefined) =>
			open(profile, store === "compatibility" ? () => true : null);

		it(`reports only settle when put options are rejected up front on a ${store} store`, async () => {
			const { events, sink } = observer();
			const docs = await openStore(sink);
			const error = await track(
				docs.put(new Document({ id: "private-invalid-options" }), {
					...persistedPutOptions(),
					target: "none",
				}),
			).catch((failure) => failure);
			expect(error).to.have.property(
				"message",
				'persisted delivery requires target="replicators" (or an omitted target)',
			);
			expect(docs.log.log.length).equal(0);
			assertTrace(events, "error");
			expect(events.map((event) => event.name)).deep.equal([
				"documents.put.settle",
			]);
		});

		it(`reports a failed preparation without a path on a ${store} store`, async () => {
			const { events, sink } = observer();
			const docs = await openStore(sink);
			const withoutId = new Document({});
			// The constructor always assigns an id, so remove it afterwards.
			(withoutId as { id?: string }).id = undefined;
			const error = await track(docs.put(withoutId, putOptions)).catch(
				(failure) => failure,
			);
			expect(error)
				.to.have.property("message")
				.that.match(/null or undefined/);
			expect(docs.log.log.length).equal(0);
			assertTrace(events, "error");
			expect(events.map((event) => event.name)).deep.equal([
				"documents.put.prepare",
				"documents.put.settle",
			]);
			expect(outcomes(events, "documents.put.prepare")).deep.equal(["error"]);
		});

		it(`reports a failed existing-document lookup without a path on a ${store} store`, async () => {
			const { events, sink } = observer();
			const docs = await openStore(sink);
			const sentinel = new Error("private-lookup-failure");
			const lookup = sandbox
				.stub(docs as any, "getLocalIndexedContext")
				.rejects(sentinel);
			// Without `unique`, a put first looks up the document it may replace.
			const error = await track(
				docs.put(new Document({ id: "private-lookup" }), { target: "none" }),
			).catch((failure) => failure);
			expect(error).equal(sentinel);
			expect(lookup.callCount).equal(1);
			expect(docs.log.log.length).equal(0);
			assertTrace(events, "error");
			expect(events.map((event) => event.name)).deep.equal([
				"documents.put.prepare",
				"documents.put.settle",
			]);
			expect(outcomes(events, "documents.put.prepare")).deep.equal(["success"]);
		});
	}

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
		expect(outcomes(events, "documents.put.projection")).deep.equal(["error"]);
		expect(outcomes(events, "log.append.entryIndex")).deep.equal(["success"]);
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
			docs.put(new Document({ id: "private-evidence" }), persistedPutOptions()),
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
			docs.put(new Document({ id: "private-delivery" }), persistedPutOptions()),
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

describe("document put diagnostics with a durable replica", function () {
	this.timeout(60_000);

	let sandbox: sinon.SinonSandbox;
	let directory: string | undefined;
	let peers: Peerbit[];
	let stores: TestStore[];

	beforeEach(() => {
		sandbox = sinon.createSandbox();
		peers = [];
		stores = [];
	});
	afterEach(async () => {
		sandbox.restore();
		await Promise.allSettled(stores.map((store) => store.close()));
		await Promise.allSettled(peers.map((peer) => peer.stop()));
		if (directory) await fs.rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	for (const writePath of ["compatibility", "prepared"] as const) {
		it(`settles a traced ${writePath} put only after the remote persisted receipt`, async () => {
			directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-put-profile-"),
			);
			const writer = await Peerbit.create();
			peers.push(writer);
			const receiver = await Peerbit.create({ directory });
			peers.push(receiver);
			await writer.dial(receiver);

			const order: string[] = [];
			const events: DiagnosticEvent[] = [];
			const sink: DiagnosticSink = (event) => {
				if (!event.traceId?.startsWith("documents.put:")) return;
				events.push(structuredClone(event));
				if (event.name === "documents.put.settle") order.push("settle");
			};
			const args = (replicate: false | { offset: number; factor: number }) => ({
				replicas: { min: 1 },
				replicate,
				timeUntilRoleMaturity: 0,
				...(writePath === "compatibility" ? { canPerform: () => true } : {}),
			});
			const writerStore = new TestStore({ docs: new Documents<Document>() });
			stores.push(writerStore);
			await writer.open(writerStore, {
				args: { ...args(false), sync: { profile: sink } },
			});
			const receiverStore = await receiver.open(writerStore.clone(), {
				args: args({ offset: 0, factor: 1 }),
			});
			stores.push(receiverStore);
			const receiverKey = receiver.identity.publicKey;
			await writerStore.docs.log.waitForReplicator(receiverKey, {
				roleAge: 0,
				timeout: 15_000,
			});
			const ready =
				await writerStore.docs.log.waitForPersistedReceiptPeerReadiness(
					receiverKey,
					{ timeout: 15_000 },
				);
			expect(ready.status).equal("ready");

			// Observe the real settlement without replacing it: it resolves only
			// once the remote replica's persisted receipt has been accepted.
			const writerLog = writerStore.docs.log as any;
			const settlePersistedDelivery = writerLog.settlePersistedDelivery;
			const settlement = sandbox
				.stub(writerLog, "settlePersistedDelivery")
				.callsFake(async (...settleArgs: unknown[]) => {
					await settlePersistedDelivery.apply(writerLog, settleArgs);
					order.push("receipt");
				});

			const result = await writerStore.docs.put(
				new Document({ id: `private-durable-${writePath}` }),
				{
					unique: true,
					delivery: { reliability: "persisted", minAcks: 1, timeout: 15_000 },
				},
			);
			expect(settlement.callCount).equal(1);
			expect(order).deep.equal(["receipt", "settle"]);
			expect(new Set(events.map((event) => event.traceId)).size).equal(1);
			expect(
				events.find((event) => event.name === "documents.put.path")?.details
					?.path,
			).equal(writePath);
			if (writePath === "prepared") {
				expect(events.map((event) => event.name)).deep.equal(preparedPhases);
			}
			const settled = events.filter(
				(event) => event.name === "documents.put.settle",
			);
			expect(settled).to.have.length(1);
			expect(settled[0]!.details?.outcome).equal("success");
			expect(await receiverStore.docs.log.log.has(result.entry.hash)).equal(
				true,
			);
		});
	}
});

describe("document put diagnostics in strict native mode", function () {
	this.timeout(60_000);

	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("never traces a put when sync.profile is set after open", async () => {
		session = await TestSession.connected(1, createRustPeerbitOptions());
		const store = new TestStore({ docs: new Documents<Document>() });
		// mode: "native" rejects sync.profile at open, so the callback is added
		// to the same sync object afterwards.
		const sync: { profile?: DiagnosticSink } = {};
		await session.peers[0].open(store, {
			args: {
				mode: "native",
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
				sync,
			},
		});
		const traceIds: (string | undefined)[] = [];
		sync.profile = (event) => {
			traceIds.push(event.traceId);
		};
		const result = await store.docs.put(
			new Document({ id: "private-native", name: "native" }),
			{ unique: true, replicate: false, target: "none" },
		);
		expect(result.entry.hash).to.be.a("string").that.is.not.empty;
		expect(
			(await store.docs.get("private-native", { remote: false }))?.name,
		).equal("native");
		expect(
			traceIds.filter((traceId) => traceId?.startsWith("documents.put:")),
		).deep.equal([]);
		expect((store.docs as any)._putSync).equal(undefined);
	});
});
