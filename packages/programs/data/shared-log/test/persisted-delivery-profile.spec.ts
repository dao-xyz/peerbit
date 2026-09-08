import { TestSession } from "@peerbit/test-utils";
import { AbortError, TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { PersistedDeliveryError } from "../src/errors.js";
import { createPersistedDeliveryProfile } from "../src/sync/persisted-delivery-profile.js";
import type { SyncProfileEvent, SyncProfileFn } from "../src/sync/profile.js";
import {
	ConfirmEntriesMessage,
	RequestPersistedEntriesV1,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

const PREFIX = "sharedLog.persistedDelivery.";
const peers = ["receipt-peer-one", "receipt-peer-two", "receipt-peer-three"];

const untilReleased = (gate: Promise<void>, signal: AbortSignal) =>
	new Promise<void>((resolve, reject) => {
		const abort = () => reject(signal.reason ?? new AbortError());
		if (signal.aborted) return abort();
		signal.addEventListener("abort", abort, { once: true });
		void gate.then(resolve, reject).finally(() => {
			signal.removeEventListener("abort", abort);
		});
	});

describe("append delivery options — persisted profiling", function () {
	this.timeout(60_000);
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	const openCommitted = async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				nativeGraph: false,
				nativeBackbone: false,
				nativeRangePlanner: false,
			},
		});
		const { entry } = await writer.add("profiling-private-payload", {
			target: "none",
		});
		return { log: writer.log as any, entry };
	};

	const setup = (log: any, entry: any, profile?: SyncProfileFn) => {
		const sandbox = sinon.createSandbox();
		const calls: string[] = [];
		const thirdEntered = pDefer<void>();
		const thirdRelease = pDefer<void>();
		const twoConsumed = pDefer<void>();
		const retryEntered = pDefer<void>();
		const retryRelease = pDefer<void>();
		const consumed = new Set<string>();
		const bindings = new Map(
			peers.map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer, generation: 1 },
				},
			]),
		);
		const previousProfile = log._logProperties.sync?.profile;
		log._logProperties.sync = { ...log._logProperties.sync, profile };
		const plan = sandbox
			.stub(log, "findLeadersFromEntry")
			.callsFake(async (...args: any[]) => {
				calls.push("plan");
				expect(args[0].hash).to.equal(entry.hash);
				expect(args[2].freshLeaderPlan).to.equal(true);
				return new Map(peers.map((peer) => [peer, { intersecting: true }]));
			});
		sandbox.stub(log, "persistedReceiptPeerSession").callsFake((peer: any) => {
			calls.push(`session:${peer}`);
			return bindings.get(peer);
		});
		sandbox.stub(log, "isReceiveOwnershipSnapshotStable").callsFake(() => {
			calls.push("ownership");
			return true;
		});
		for (const name of ["hasCurrentStateForPeer", "isLatestConfirmedForPeer"]) {
			sandbox.stub(log._v2Send, name).callsFake((options: any) => {
				calls.push(`${name}:${options.peerHash}`);
				return true;
			});
		}
		sandbox
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async (options: any, wait: any) => {
				const peer = options.peerHash;
				calls.push(`confirmation:${peer}`);
				if (peer === peers[2]) {
					thirdEntered.resolve();
					await untilReleased(thirdRelease.promise, wait.signal);
				}
			});
		const admission = sandbox
			.stub(log, "waitForPersistedTransferAdmission")
			.callsFake(async (peer: any) => {
				calls.push(`admission:${peer}`);
				return true;
			});
		const transfer = sandbox
			.stub(log, "pushEntryHashes")
			.callsFake(async (peer: any, hashes: any, options: any) => {
				calls.push(`transfer:${peer}`);
				expect(hashes).to.deep.equal([entry.hash]);
				options.onChunkAttempted(hashes);
				await options.onChunkSent(hashes);
			});
		sandbox
			.stub(log, "waitForPersistedReceiptEgressAdmission")
			.callsFake(async (peer: any) => {
				calls.push(`egress:${peer}`);
			});
		const respond = async (message: any, options: any) => {
			const peer = options.mode.to[0];
			calls.push(`request:${peer}`);
			expect(message).to.be.instanceOf(RequestPersistedEntriesV1);
			expect(message.hashes).to.deep.equal([entry.hash]);
			return [
				{
					response: new ConfirmEntriesMessage({ hashes: message.hashes }),
					from: {
						hashcode: () => {
							calls.push(`validate:${peer}`);
							consumed.add(peer);
							if (consumed.size === 2) twoConsumed.resolve();
							return peer;
						},
					},
					message: {
						header: { session: bindings.get(peer)!.capabilitySession },
					},
				},
			];
		};
		const request = sandbox.stub(log.rpc, "request").callsFake(respond);
		const retry = sandbox
			.stub(log, "waitPersistedReceiptRetry")
			.callsFake(async (signal: any) => {
				calls.push("retry");
				retryEntered.resolve();
				await untilReleased(retryRelease.promise, signal);
			});
		const recover = sandbox
			.stub(log, "waitForPersistedReceiptPeerReadiness")
			.callsFake(async () => {
				calls.push("recover");
			});
		const rearm = sandbox
			.stub(log._v2Receive, "reAdvertiseLocalCapabilityForRemoteFull")
			.callsFake(() => calls.push("rearm"));
		sandbox
			.stub(log, "_resolvePublicKeyFromHash")
			.callsFake(async (peer: any) => {
				calls.push(`resolve-key:${peer}`);
				return undefined;
			});
		return {
			calls,
			bindings,
			plan,
			transfer,
			admission,
			request,
			respond,
			retry,
			recover,
			rearm,
			thirdEntered,
			thirdRelease,
			twoConsumed,
			retryEntered,
			retryRelease,
			run: (signal?: AbortSignal, deadline?: any) => {
				const record = log.snapshotPersistedDeliveryPlanningEntry(entry);
				return log.settlePersistedDelivery(
					// Duplicate input is still one committed entry and one stable ordinal.
					[record, record],
					3,
					{ reliability: "persisted", minAcks: 3, timeout: 10_000, signal },
					undefined,
					deadline,
					true,
				);
			},
			close: () => {
				thirdRelease.resolve();
				retryRelease.resolve();
				log._logProperties.sync.profile = previousProfile;
				sandbox.restore();
			},
		};
	};

	const progressCounts = (events: SyncProfileEvent[]) =>
		events
			.filter((event) => event.name === `${PREFIX}progress`)
			.map((event) => event.details?.carriedAckCount);
	const terminal = (events: SyncProfileEvent[]) =>
		events.filter((event) => event.name === `${PREFIX}settle`);
	const record =
		(events: SyncProfileEvent[]): SyncProfileFn =>
		(event) => {
			if (event.name.startsWith(PREFIX)) events.push(event);
		};

	it("locates held N3 confirmation without changing delivery calls or fresh quorum validation", async () => {
		const { log, entry } = await openCommitted();
		const traces: string[][] = [];
		for (const mode of ["absent", "recording", "throwing"] as const) {
			const events: SyncProfileEvent[] = [];
			const sink: SyncProfileFn | undefined =
				mode === "absent"
					? undefined
					: (event) => {
							if (!event.name.startsWith(PREFIX)) return;
							events.push(event);
							if (mode === "throwing") throw new Error("diagnostic sink");
						};
			const fixture = setup(log, entry, sink);
			let settled = false;
			const delivery = fixture.run().then(() => {
				settled = true;
			});
			try {
				await Promise.all([
					fixture.thirdEntered.promise,
					fixture.twoConsumed.promise,
				]);
				expect(settled).to.equal(false);
				expect(fixture.transfer.callCount).to.equal(2);
				expect(fixture.request.callCount).to.equal(2);
				expect(fixture.plan.callCount).to.equal(1);
				if (mode !== "absent") {
					expect(progressCounts(events)).to.deep.equal([1, 2]);
					expect(
						events.find((event) => event.name === `${PREFIX}plan`)!.details,
					).to.include({
						entryIndex: 0,
						remoteLeaderCount: 3,
						selectedRequestPeerCount: 3,
						carriedAckCount: 0,
					});
					expect(terminal(events)).to.have.length(0);
					expect(
						events
							.filter((event) => event.peer === peers[2])
							.filter((event) => event.name === `${PREFIX}peerPhase`)
							.map((event) => [event.details?.phase, event.details?.edge]),
					).to.deep.equal([["confirmation", "start"]]);
				}
				fixture.thirdRelease.resolve();
				await delivery;
				expect(settled).to.equal(true);
				expect(fixture.plan.callCount).to.equal(2);
				expect(fixture.transfer.callCount).to.equal(3);
				expect(fixture.admission.callCount).to.equal(3);
				expect(fixture.request.callCount).to.equal(3);
				expect(fixture.retry.callCount).to.equal(0);
				expect(fixture.recover.callCount).to.equal(0);
				expect(fixture.rearm.callCount).to.equal(0);
				traces.push([...fixture.calls]);
				if (mode !== "absent") {
					expect(progressCounts(events)).to.deep.equal([1, 2, 3]);
					expect(terminal(events)).to.have.length(1);
					expect(terminal(events)[0]!.details?.outcome).to.equal(
						"quorum-validated",
					);
					expect(terminal(events)[0]!.details?.emittedEvents).to.equal(
						events.length,
					);
					expect(terminal(events)[0]!.details?.droppedEvents).to.equal(0);
					expect(new Set(events.map((event) => event.traceId)).size).to.equal(
						1,
					);
					for (const event of events) {
						expect(event.traceId).to.be.a("string").and.not.equal(entry.hash);
						expect(event.entries).to.equal(1);
						expect(event.details?.v).to.equal(1);
						for (const value of Object.values(event.details ?? {})) {
							expect(["string", "number", "boolean", "undefined"]).to.include(
								typeof value,
							);
						}
					}
					expect(
						events
							.filter((event) => event.details?.entryIndex !== undefined)
							.map((event) => event.details!.entryIndex),
					).to.satisfy(
						(indexes: number[]) =>
							indexes.length > 0 && indexes.every((index) => index === 0),
					);
					expect(JSON.stringify(events)).not.to.include(entry.hash);
					expect(JSON.stringify(events)).not.to.include(
						"profiling-private-payload",
					);
				}
			} finally {
				fixture.thirdRelease.resolve();
				fixture.retryRelease.resolve();
				try {
					await delivery;
				} finally {
					fixture.close();
				}
			}
		}
		expect(traces[1]).to.deep.equal(traces[0]);
		expect(traces[2]).to.deep.equal(traces[0]);
	});

	for (const kind of ["cancelled", "timed-out"] as const) {
		it(`emits one terminal event for ${kind} N3 without reporting quorum`, async () => {
			const { log, entry } = await openCommitted();
			const events: SyncProfileEvent[] = [];
			const fixture = setup(log, entry, record(events));
			const controller = new AbortController();
			const reason =
				kind === "cancelled" ? new AbortError() : new TimeoutError();
			const delivery = fixture.run(undefined, {
				deadline: Date.now() + 10_000,
				signal: controller.signal,
				dispose: () => {
					throw new Error("caller owns the injected deadline");
				},
			});
			const outcome = delivery.then(
				() => undefined,
				(error: unknown) => error,
			);
			try {
				await Promise.all([
					fixture.thirdEntered.promise,
					fixture.twoConsumed.promise,
				]);
				controller.abort(reason);
				const error = await outcome;
				expect(error).to.be.instanceOf(PersistedDeliveryError);
				expect((error as PersistedDeliveryError).cause).to.equal(reason);
				expect(progressCounts(events)).to.deep.equal([1, 2]);
				expect(terminal(events)).to.have.length(1);
				expect(terminal(events)[0]!.details?.outcome).to.equal(
					kind === "cancelled" ? "aborted" : "failed",
				);
				expect(terminal(events)[0]!.details?.reason).to.equal(
					kind === "cancelled" ? "signal" : "timeout",
				);
				expect(fixture.transfer.callCount).to.equal(2);
				expect(fixture.request.callCount).to.equal(2);
				expect(fixture.plan.callCount).to.equal(1);
				const count = events.length;
				fixture.thirdRelease.resolve();
				await Promise.resolve();
				expect(events).to.have.length(count);
			} finally {
				controller.abort(reason);
				fixture.thirdRelease.resolve();
				fixture.retryRelease.resolve();
				try {
					await outcome;
				} finally {
					fixture.close();
				}
			}
		});
	}

	it("profiles the existing no-peer failure without adding delivery work", async () => {
		const { log, entry } = await openCommitted();
		const events: SyncProfileEvent[] = [];
		const fixture = setup(log, entry, record(events));
		fixture.plan.resolves(new Map());
		try {
			const error = await fixture.run().catch((error: unknown) => error);
			expect(error).to.be.instanceOf(PersistedDeliveryError);
			expect(terminal(events)).to.have.length(1);
			expect(terminal(events)[0]!.details).to.include({
				outcome: "failed",
				reason: "no-peers",
			});
			expect(
				events.find((event) => event.name === `${PREFIX}plan`)!.details,
			).to.include({
				entryIndex: 0,
				remoteLeaderCount: 0,
				selectedRequestPeerCount: 0,
				carriedAckCount: 0,
			});
			expect(progressCounts(events)).to.deep.equal([]);
			expect(fixture.transfer.callCount).to.equal(0);
			expect(fixture.request.callCount).to.equal(0);
			expect(fixture.retry.callCount).to.equal(0);
		} finally {
			fixture.close();
		}
	});

	it("reports an exact leader without a current receipt session without counting it", async () => {
		const { log, entry } = await openCommitted();
		const events: SyncProfileEvent[] = [];
		const fixture = setup(log, entry, record(events));
		const missing = fixture.bindings.get(peers[2]!)!;
		fixture.bindings.delete(peers[2]!);
		fixture.thirdRelease.resolve();
		const delivery = fixture.run();
		try {
			await fixture.retryEntered.promise;
			expect(fixture.request.callCount).to.equal(2);
			expect(progressCounts(events)).to.deep.equal([1, 2]);
			expect(terminal(events)).to.have.length(0);
			expect(
				events.find((event) => event.name === `${PREFIX}plan`)!.details,
			).to.include({ remoteLeaderCount: 3, selectedRequestPeerCount: 2 });
			expect(
				events.find(
					(event) =>
						event.name === `${PREFIX}candidate` && event.peer === peers[2],
				)!.details?.status,
			).to.equal("leader-no-current-session");
			fixture.bindings.set(peers[2]!, missing);
			fixture.retryRelease.resolve();
			await delivery;
			expect(fixture.request.callCount).to.equal(3);
			expect(progressCounts(events)).to.deep.equal([1, 2, 3]);
			expect(terminal(events)[0]!.details?.outcome).to.equal(
				"quorum-validated",
			);
		} finally {
			fixture.bindings.set(peers[2]!, missing);
			fixture.thirdRelease.resolve();
			fixture.retryRelease.resolve();
			try {
				await delivery;
			} finally {
				fixture.close();
			}
		}
	});

	for (const invalidation of [
		"invalid-receipt",
		"replacement-session",
	] as const) {
		it(`does not profile an accepted receipt for ${invalidation} and revalidates the next round`, async () => {
			const { log, entry } = await openCommitted();
			const events: SyncProfileEvent[] = [];
			const fixture = setup(log, entry, record(events));
			let first = true;
			fixture.request.callsFake(async (message: any, options: any) => {
				const responses = await fixture.respond(message, options);
				if (options.mode.to[0] === peers[0] && first) {
					first = false;
					if (invalidation === "invalid-receipt") {
						responses[0]!.response = new ConfirmEntriesMessage({
							hashes: [entry.hash, entry.hash],
						});
					} else {
						fixture.bindings.set(peers[0]!, {
							capabilitySession: 99n,
							peerSession: { peer: peers[0]!, generation: 2 },
						});
					}
				}
				return responses;
			});
			fixture.thirdRelease.resolve();
			let settled = false;
			const delivery = fixture.run().then(() => {
				settled = true;
			});
			try {
				await fixture.retryEntered.promise;
				expect(settled).to.equal(false);
				expect(progressCounts(events)).to.deep.equal([1, 2]);
				expect(
					events
						.filter((event) => event.name === `${PREFIX}progress`)
						.map((event) => event.peer),
				).not.to.include(peers[0]);
				expect(terminal(events)).to.have.length(0);
				expect(fixture.request.callCount).to.equal(3);
				fixture.retryRelease.resolve();
				await delivery;
				expect(fixture.request.callCount).to.equal(4);
				expect(fixture.request.lastCall.args[1].mode.to).to.deep.equal([
					peers[0],
				]);
				expect(
					fixture.request.lastCall.args[0].expectedReceiverSession,
				).to.equal(invalidation === "replacement-session" ? 99n : 1n);
				expect(fixture.plan.callCount).to.equal(3);
				expect(progressCounts(events)).to.deep.equal([1, 2, 3]);
				expect(terminal(events)).to.have.length(1);
				expect(terminal(events)[0]!.details?.outcome).to.equal(
					"quorum-validated",
				);
			} finally {
				fixture.thirdRelease.resolve();
				fixture.retryRelease.resolve();
				try {
					await delivery;
				} finally {
					fixture.close();
				}
			}
		});
	}

	it("caps detail events, snapshots scalars and reserves exactly one terminal event", () => {
		const events: SyncProfileEvent[] = [];
		expect(createPersistedDeliveryProfile(undefined, 1, 3, 3)).to.equal(
			undefined,
		);
		const profile = createPersistedDeliveryProfile(
			(event) => events.push(event),
			17,
			3,
			3,
		)!;
		const details = { round: 1, entryIndex: 0, carriedAckCount: 0 };
		profile.emit("progress", details);
		details.carriedAckCount = 99;
		for (let index = 1; index < 300; index++) profile.emit("progress", details);
		profile.finish({ outcome: "failed" });
		profile.emit("progress", details);
		profile.finish({ outcome: "quorum-validated" });
		expect(events).to.have.length(257);
		expect(events[0]!.details?.carriedAckCount).to.equal(0);
		expect(terminal(events)).to.have.length(1);
		expect(events[256]!.details).to.include({
			outcome: "failed",
			emittedEvents: 257,
			droppedEvents: 44,
			entrySampleWindow: 16,
			entriesOutsideSampleWindow: 1,
		});
	});

	it("does not await or leak rejected asynchronous sinks", async () => {
		const sinkGate = pDefer<void>();
		const sink = sinon.spy(async () => sinkGate.promise);
		const profile = createPersistedDeliveryProfile(sink, 1, 3, 3)!;
		profile.emit("plan", { round: 1 });
		profile.finish({ outcome: "failed" });
		expect(sink.callCount).to.equal(2);
		sinkGate.reject(new Error("asynchronous diagnostic failure"));
		// The helper, not the test's drain below, must own the rejection before
		// Node checks unhandled promises at the next event-loop turn.
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
		await Promise.allSettled(sink.returnValues);
		profile.emit("progress", { round: 1 });
		expect(sink.callCount).to.equal(2);
	});
});
