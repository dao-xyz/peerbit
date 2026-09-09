import { Ed25519Keypair } from "@peerbit/crypto";
import type { DiagnosticEvent, DiagnosticSink } from "@peerbit/diagnostics";
import { Timestamp } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	Documents,
	SearchRequestIndexed,
	StringMatch,
	toId,
} from "../src/index.js";
import { Document, TestStore } from "./data.js";

type ObserverMode = "disabled" | "recording" | "throwing" | "async-rejecting";
const observerModes: ObserverMode[] = [
	"disabled",
	"recording",
	"throwing",
	"async-rejecting",
];

describe("document query diagnostics", function () {
	this.timeout(30_000);
	let session: TestSession;
	let sandbox: sinon.SinonSandbox;
	let stores: TestStore[];
	let releaseGates: (() => void)[];
	let pending: Promise<unknown>[];
	let rejectedSinks: Promise<never>[];

	before(async () => {
		session = await TestSession.disconnected(3);
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
			// Let an unowned observer rejection reach the strict runner before
			// adding test-owned cleanup handlers, including on assertion failure.
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			await Promise.allSettled(rejectedSinks);
		} finally {
			try {
				sandbox.restore();
			} finally {
				await Promise.all(stores.map((store) => store.close()));
			}
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
	const waitUntil = async (
		entered: Promise<void>,
		operation: Promise<unknown>,
	) => {
		await Promise.race([
			entered,
			operation.then(() => {
				throw new Error("operation completed before the controlled query gate");
			}),
		]);
	};
	const observer = (mode: ObserverMode = "recording") => {
		const events: DiagnosticEvent[] = [];
		const sentinel = new Error("query diagnostic observer failure");
		const sink: DiagnosticSink | undefined =
			mode === "disabled"
				? undefined
				: (event) => {
						// The same open option also reaches legacy SharedLog emitters. This
						// test claims isolation only for the new query/forwarded RPC trace.
						if (
							!event.name.startsWith("documents.query.") &&
							!event.name.startsWith("rpc.request.")
						)
							return;
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
	const open = async (peer: number, profile?: DiagnosticSink) => {
		const store = new TestStore({
			docs: new Documents<Document>({ immutable: true }),
		});
		stores.push(store);
		await session.peers[peer].open(store, {
			args: {
				mode: "compat",
				replicate: false,
				keep: "self",
				nativeGraph: false,
				nativeBackbone: false,
				nativeRangePlanner: false,
				...(profile ? { sync: { profile } } : {}),
			},
		});
		expect((store.docs as any).isNativeMode()).equal(false);
		return store.docs;
	};
	const seed = async (docs: Documents<Document>, id: string) => {
		sandbox.stub(docs.log, "getCover").resolves([]);
		await docs.put(new Document({ id, name: "older" }), {
			target: "none",
			meta: { timestamp: new Timestamp({ wallTime: 1_000_000n }) },
		});
	};
	const event = (events: DiagnosticEvent[], name: string) => {
		const matching = events.filter(
			(value) =>
				value.name === `documents.query.${name}` &&
				(!["local", "cover", "introduce"].includes(name) ||
					value.details?.edge === "end"),
		);
		expect(matching, name).to.have.length(1);
		return matching[0]!;
	};
	const assertOneTrace = (events: DiagnosticEvent[]) => {
		const ids = new Set(events.map((value) => value.traceId));
		expect(ids.size).equal(1);
		expect(events[0]?.traceId).match(/^query:/);
		for (const value of events) {
			expect(value.details?.v).equal(1);
			expect(value.details?.elapsedMs).to.be.at.least(0);
		}
	};
	const querySignature = (query: sinon.SinonStub, id: string) => {
		expect(query.callCount).equal(1);
		const [request, options] = query.firstCall.args;
		expect(request).instanceOf(SearchRequestIndexed);
		expect(request.query).to.have.length(1);
		expect(request.query[0]).instanceOf(StringMatch);
		expect(request.query[0].value).equal(id);
		const { profile: _profile, mode, ...rest } = options;
		return {
			requestType: request.constructor.name,
			query: request.query,
			fetch: request.fetch,
			modeType: mode.constructor.name,
			targets: [...mode.to],
			options: rest,
		};
	};

	// Only cover selection and the RPC call are controlled. Response bytes and
	// indexed contexts come from real processQuery calls on immutable stores;
	// these tests do not simulate transport delivery or fabricate receipts.
	it("observes held local/cover/query stages and returns real partial results", async () => {
		const { events, sink } = observer();
		const writer = await open(0, sink);
		const donor = await open(1);
		const id = "partial-profile-result";
		await seed(donor, id);
		const liveKey = session.peers[1].identity.publicKey;
		const missingHash = session.peers[2].identity.publicKey.hashcode();
		const coverEntered = gate(),
			coverRelease = gate();
		const queryEntered = gate(),
			queryRelease = gate();
		sandbox.stub(writer.log, "getCover").callsFake(async () => {
			coverEntered.resolve();
			await coverRelease.promise;
			return [liveKey.hashcode(), missingHash];
		});
		const query = sandbox
			.stub((writer.index as any)._query, "request")
			.callsFake(async (...args: unknown[]) => {
				const request = args[0] as SearchRequestIndexed;
				const response = await donor.index.processQuery(
					request,
					session.peers[0].identity.publicKey,
					false,
				);
				queryEntered.resolve();
				await queryRelease.promise;
				return [{ response, from: liveKey }];
			});
		const operation = track(
			writer.index.getDetailed(id, {
				resolve: false,
				local: true,
				remote: { throwOnMissing: false },
			}),
		);
		await waitUntil(coverEntered.promise, operation);
		expect(event(events, "local").entries).equal(0);
		expect(
			events.filter(
				(value) =>
					value.name === "documents.query.cover" &&
					value.details?.edge === "start",
			),
		).to.have.length(1);
		expect(
			events.some(
				(value) =>
					value.name === "documents.query.cover" &&
					value.details?.edge === "end",
			),
		).equal(false);
		expect(
			events.some((value) => value.name === "documents.query.settle"),
		).equal(false);
		coverRelease.resolve();
		await waitUntil(queryEntered.promise, operation);
		expect(event(events, "cover").targets).equal(2);
		expect(event(events, "targets").details?.requestTargets).equal(2);
		expect(
			events.some((value) => value.name === "documents.query.settle"),
		).equal(false);
		queryRelease.resolve();
		const results = await operation;
		expect(results).to.have.length(1);
		expect(results![0].results).to.have.length(1);
		expect(results![0].results[0].context.created).equal(1_000_000n);
		expect(event(events, "missing").count).equal(1);
		expect(event(events, "missing").details?.tolerated).equal(true);
		expect(event(events, "settle").details).include({
			outcome: "fulfilled",
			missingGroups: 1,
		});
		expect(writer.log.log.length).equal(0);
		querySignature(query, id);
		assertOneTrace(events);
	});

	it("keeps a held immutable put uncommitted and reports query success before admission rejection", async () => {
		const { events, sink } = observer();
		const writer = await open(0, sink);
		const empty = await open(1),
			older = await open(2);
		const id = "held-profile-conflict";
		await seed(older, id);
		const keys = session.peers.slice(1).map((peer) => peer.identity.publicKey);
		sandbox
			.stub(writer.log, "getCover")
			.resolves(keys.map((key) => key.hashcode()));
		const entered = gate(),
			release = gate();
		const query = sandbox
			.stub((writer.index as any)._query, "request")
			.callsFake(async (...args: unknown[]) => {
				const request = args[0] as SearchRequestIndexed;
				const responses = await Promise.all(
					[empty, older].map(async (docs, i) => ({
						response: await docs.index.processQuery(
							request,
							session.peers[0].identity.publicKey,
							false,
						),
						from: keys[i],
					})),
				);
				entered.resolve();
				await release.promise;
				return responses;
			});
		const operation = track(
			writer.put(new Document({ id, name: "newer" }), {
				target: "none",
				meta: { timestamp: new Timestamp({ wallTime: 2_000_000n }) },
			}),
		);
		await waitUntil(entered.promise, operation);
		expect(event(events, "local").entries).equal(0);
		expect(event(events, "cover").targets).equal(2);
		expect(
			events.some((value) => value.name === "documents.query.settle"),
		).equal(false);
		expect(writer.log.log.length).equal(0);
		expect(await writer.index.index.get(toId(id))).equal(undefined);
		release.resolve();
		const error = await operation.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).instanceOf(Error);
		expect((error as Error).message).match(/Not allowed to append/);
		expect(event(events, "settle").details).include({
			outcome: "fulfilled",
			missingGroups: 0,
		});
		expect(writer.log.log.length).equal(0);
		expect(await writer.index.index.get(toId(id))).equal(undefined);
		querySignature(query, id);
		assertOneTrace(events);
	});

	for (const kind of ["partial query", "immutable rejection"] as const) {
		it(`preserves ${kind} outcomes and request options across disabled and failing observers`, async () => {
			const donor = await open(1);
			const id = "observer-parity";
			await seed(donor, id);
			const liveKey = session.peers[1].identity.publicKey;
			const missingHash = session.peers[2].identity.publicKey.hashcode();
			const signatures: unknown[] = [];
			for (const mode of observerModes) {
				const { events, sink } = observer(mode);
				const writer = await open(0, sink);
				sandbox
					.stub(writer.log, "getCover")
					.resolves([liveKey.hashcode(), missingHash]);
				const query = sandbox
					.stub((writer.index as any)._query, "request")
					.callsFake(async (...args: unknown[]) => {
						const request = args[0] as SearchRequestIndexed;
						const options = args[1] as { profile?: DiagnosticSink };
						// Forwarded events are synthetic diagnostics only; the response is
						// still produced by the donor's real document query implementation.
						options.profile?.({
							name: "rpc.request.start",
							component: "rpc",
							traceId: "controlled-rpc",
							targets: 2,
						});
						const response = await donor.index.processQuery(
							request,
							session.peers[0].identity.publicKey,
							false,
						);
						return [{ response, from: liveKey }];
					});
				if (kind === "partial query") {
					const results = await writer.index.getDetailed(id, {
						resolve: false,
						local: true,
						remote: { throwOnMissing: false, timeout: 2_000 },
					});
					expect(results).to.have.length(1);
					expect(results![0].results[0].context.created).equal(1_000_000n);
				} else {
					const error = await writer
						.put(new Document({ id, name: "newer" }), {
							target: "none",
							meta: { timestamp: new Timestamp({ wallTime: 2_000_000n }) },
						})
						.then(
							() => undefined,
							(error: unknown) => error,
						);
					expect(error).instanceOf(Error);
					expect((error as Error).message).match(/Not allowed to append/);
				}
				expect(writer.log.log.length).equal(0);
				signatures.push(querySignature(query, id));
				if (mode === "disabled") expect(events).to.have.length(0);
				else {
					expect(event(events, "settle").details).include({
						outcome: "fulfilled",
						missingGroups: 1,
					});
					const forwarded = events.find(
						(value) => value.name === "rpc.request.start",
					)!;
					expect(forwarded.details?.requestTraceId).equal("controlled-rpc");
					assertOneTrace(events);
				}
			}
			for (const signature of signatures.slice(1))
				expect(signature).deep.equal(signatures[0]);
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			expect(rejectedSinks.length).greaterThan(0);
		});
	}

	it("records a rejected query without replacing the actual query error", async () => {
		const { events, sink } = observer("throwing");
		const writer = await open(0, sink);
		sandbox
			.stub(writer.log, "getCover")
			.resolves([session.peers[1].identity.publicKey.hashcode()]);
		const sentinel = new Error("actual query failure");
		sandbox.stub((writer.index as any)._query, "request").rejects(sentinel);
		const error = await writer.index
			.getDetailed("query-error", { resolve: false })
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(error).equal(sentinel);
		expect(event(events, "settle").details).include({
			outcome: "rejected",
			missingGroups: 0,
		});
		assertOneTrace(events);
	});

	it("samples only the first sixteen distinct selected remote targets", async () => {
		const { events, sink } = observer();
		const writer = await open(0, sink);
		const keys = await Promise.all(
			Array.from({ length: 20 }, () => Ed25519Keypair.create()),
		);
		const hashes = keys.map((key) => key.publicKey.hashcode());
		const self = session.peers[0].identity.publicKey.hashcode();
		const cover = sandbox
			.stub(writer.log, "getCover")
			.rejects(new Error("explicit targets must not query cover"));
		const query = sandbox
			.stub((writer.index as any)._query, "request")
			.resolves([]);
		const results = await writer.index.getDetailed("target-window", {
			resolve: false,
			remote: { from: [self, ...hashes, hashes[0]], throwOnMissing: false },
		});
		expect(results).to.have.length(0);
		expect(cover.callCount).equal(0);
		expect(querySignature(query, "target-window").targets).deep.equal(hashes);
		expect(event(events, "targets")).include({ targets: 20 });
		expect(event(events, "targets").details).include({
			requestTargets: 20,
			sampledTargets: 16,
		});
		const sampled = events.filter(
			(value) => value.name === "documents.query.target",
		);
		expect(sampled).to.have.length(16);
		expect(sampled.map((value) => value.peer)).deep.equal(hashes.slice(0, 16));
		for (const value of sampled)
			expect(value.details?.directPeerPresent).equal(false);
		expect(event(events, "settle").details).include({
			outcome: "fulfilled",
			missingGroups: 20,
		});
	});

	it("reserves the terminal event under a synthetic RPC diagnostic flood and ignores late events", async () => {
		const { events, sink } = observer();
		const writer = await open(0, sink),
			empty = await open(1);
		const liveKey = session.peers[1].identity.publicKey;
		sandbox.stub(writer.log, "getCover").resolves([liveKey.hashcode()]);
		let forward: DiagnosticSink | undefined;
		sandbox
			.stub((writer.index as any)._query, "request")
			.callsFake(async (...args: unknown[]) => {
				const request = args[0] as SearchRequestIndexed;
				const options = args[1] as { profile?: DiagnosticSink };
				forward = options.profile;
				for (let i = 0; i < 300; i++)
					forward?.({
						name: "rpc.request.response",
						component: "rpc",
						traceId: "synthetic-flood",
						count: i,
					});
				return [
					{
						response: await empty.index.processQuery(
							request,
							session.peers[0].identity.publicKey,
							false,
						),
						from: liveKey,
					},
				];
			});
		await writer.index.getDetailed("bounded-profile", { resolve: false });
		expect(forward).to.be.a("function");
		expect(events).to.have.length(257);
		const terminal = event(events, "settle");
		expect(events.at(-1)).equal(terminal);
		expect(terminal.details).include({
			emittedEvents: 257,
			outcome: "fulfilled",
			missingGroups: 0,
		});
		expect(terminal.details?.droppedEvents).greaterThan(0);
		assertOneTrace(events);
		forward!({
			name: "rpc.request.response",
			traceId: "synthetic-late",
			count: 1,
		});
		expect(events).to.have.length(257);
		expect(events.at(-1)).equal(terminal);
	});
});
