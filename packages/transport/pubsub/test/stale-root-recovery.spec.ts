import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	ACK,
	DataMessage,
	DeliveryError,
	MessageHeader,
	TracedDelivery,
} from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { AssertionError, expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { Uint8ArrayList } from "uint8arraylist";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

const topic = "/peerbit/pubsub-shard/1/0";
const fixture = (count = 1) => {
	const claims = new Map(
		Array.from(
			{ length: count },
			(_, index) =>
				[
					`origin-${index}`,
					{
						bytes: new Uint8Array([index]),
						timestamp: 1n as bigint,
						expires: 100_000n,
						acceptUntil: 100_000,
					},
				] as const,
		),
	);
	const candidates = ["self", ...claims.keys()];
	const subject: any = Object.assign(
		Object.create(TopicControlPlane.prototype),
		{
			started: true,
			stopping: false,
			topicControlPlaneStopping: false,
			publicKeyHash: "self",
			publicKey: {},
			topicControlPlaneLifecycleRevision: 0,
			autoTopicRootCandidates: true,
			autoTopicRootCandidateSet: new Set(candidates),
			signedTopicRootCandidateClaims: claims,
			suppressedDepartedTopicRootCandidateClaims: new Map(),
			topicRootCandidateClaimReplayFloors: new Map(
				[...claims].map(([origin, claim]) => [origin, claim.timestamp]),
			),
			topicRootControlPlane: new TopicRootControlPlane({
				defaultCandidates: candidates,
			}),
			shardRootCache: new Map(),
			shardTopicPrefix: "/peerbit/pubsub-shard/1/",
			peers: new Map([["relay", {}]]),
			routes: {
				getRouteHints: sinon.stub().returns([]),
				isReachable: sinon.stub().returns(false),
			},
			createMessage: sinon.stub().callsFake(async (_data, options) => ({
				header: { mode: options.mode },
			})),
			publishMessage: sinon.stub().resolves(),
		},
	);
	subject.rebuildAutoTopicRootCandidatesFromClaims = sinon.spy(() => {
		const next = [
			"self",
			...[...claims.keys()].filter(
				(origin) =>
					!subject.suppressedDepartedTopicRootCandidateClaims.has(origin),
			),
		];
		subject.topicRootControlPlane.setTopicRootCandidates(next);
		subject.autoTopicRootCandidateSet = new Set(next);
	});
	const run = (
		options: {
			controller?: AbortController;
			attempt?: any;
			isCurrent?: () => boolean;
			localOnly?: boolean;
		} = {},
	) =>
		subject.preflightRelayRootCandidates(
			topic,
			options.attempt ?? {},
			options.controller?.signal,
			options.isCurrent ?? (() => true),
			options.localOnly,
		);
	return { subject, claims, run };
};

const openingFixture = () => {
	const f = fixture();
	Object.assign(f.subject, {
		fanoutChannels: new Map(),
		ensureFanoutChannelInFlight: new Map(),
		ensureFanoutChannelOnce: sinon
			.stub()
			.callsFake((_topic, options, _revision, _generation, attempt, current) =>
				f.subject.preflightRelayRootCandidates(
					topic,
					attempt,
					options.signal,
					current,
				),
			),
	});
	return f;
};

describe("pubsub relay root recovery", function () {
	let clock: sinon.SinonFakeTimers | undefined;
	const expectNoTimers = () => {
		// countTimers includes fake jobs; drain them without advancing deadlines.
		clock!.runMicrotasks();
		expect(clock!.countTimers()).to.equal(0);
	};
	let session:
		| TestSession<{ pubsub: TopicControlPlane; fanout: FanoutTree }>
		| undefined;
	afterEach(async () => {
		clock?.restore();
		clock = undefined;
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	it("does not conceal owned probe deadlines when draining queued jobs", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		f.subject.createMessage.returns(new Promise(() => {}));
		const controller = new AbortController();
		const result = f.run({ controller }).catch((error: unknown) => error);
		await clock.tickAsync(0);
		const cohort = f.subject.relayRootProbeCohort;
		const now = clock.now;
		const job = sinon.spy();
		try {
			queueMicrotask(job);
			expect(expectNoTimers).to.throw(AssertionError);
			expect(clock.now).to.equal(now);
			expect(clock.countTimers()).to.equal(2);
			expect(job.calledOnce).to.equal(true);
			expect(cohort.controller.signal.aborted).to.equal(false);
			expect(cohort.users.size).to.equal(1);
		} finally {
			controller.abort();
			await Promise.all([result, cohort.promise]);
		}
		expect(await result).to.equal(controller.signal.reason);
		queueMicrotask(job);
		expectNoTimers();
		expect(job.calledTwice).to.equal(true);
		expect(clock.now).to.equal(now);
		expect(cohort.users.size).to.equal(0);
		expect(f.subject.publishMessage.notCalled).to.equal(true);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
	});

	it("coalesces 64 origins across cold shards under one deadline and rebuilds once", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture(64);
		f.subject.publishMessage.returns(new Promise(() => {}));
		const first = f.run();
		const second = f.run();
		await clock.tickAsync(0);
		expect(f.subject.publishMessage.callCount).to.equal(64);
		expect(f.subject.relayRootProbeCohort.users.size).to.equal(2);
		await clock.tickAsync(2_000);
		await Promise.all([first, second]);
		expect(f.subject.publishMessage.callCount).to.equal(128);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			64,
		);
		expect(
			f.subject.rebuildAutoTopicRootCandidatesFromClaims.callCount,
		).to.equal(1);
		expect(f.subject.signedTopicRootCandidateClaims.size).to.equal(64);
		expect(f.subject.topicRootCandidateClaimReplayFloors.size).to.equal(64);
		expectNoTimers();
	});

	it("reuses positives briefly, not for the signed lease", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		await f.run();
		await f.run();
		expect(f.subject.publishMessage.callCount).to.equal(1);
		await clock.tickAsync(2_001);
		await f.run();
		expect(f.subject.publishMessage.callCount).to.equal(2);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
	});

	it("shares completed observations with a cold shard arriving during policy recheck", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const policy = pDefer<undefined>();
		f.subject.publishMessage.rejects(new DeliveryError("no ACK"));
		f.subject.topicRootControlPlane.setTopicRootResolver(() => policy.promise);
		const first = f.run();
		await clock.tickAsync(1_000);
		expect(f.subject.publishMessage.callCount).to.equal(2);
		const second = f.run();
		await clock.tickAsync(0);
		expect(f.subject.relayRootProbeCohort.users.size).to.equal(2);
		policy.resolve(undefined);
		await Promise.all([first, second]);
		expect(f.subject.publishMessage.callCount).to.equal(2);
		expect(
			f.subject.rebuildAutoTopicRootCandidatesFromClaims.callCount,
		).to.equal(1);
		expectNoTimers();
	});

	it("gives immediate delivery failure a midpoint retry without a new deadline", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		f.subject.publishMessage
			.onFirstCall()
			.rejects(new DeliveryError("not ready yet"));
		const result = f.run();
		await clock.tickAsync(999);
		expect(f.subject.publishMessage.callCount).to.equal(1);
		await clock.tickAsync(1);
		await result;
		expect(f.subject.publishMessage.callCount).to.equal(2);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
	});

	it("retains the first ACK wait for a timely 1.5 second reply after hedging", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const firstAck = pDefer<void>();
		f.subject.publishMessage.returns(new Promise(() => {}));
		f.subject.publishMessage.onFirstCall().returns(firstAck.promise);
		const result = f.run();
		await clock.tickAsync(1_500);
		expect(f.subject.publishMessage.callCount).to.equal(2);
		expect(f.subject.publishMessage.firstCall.args[4].aborted).to.equal(false);
		firstAck.resolve();
		await result;
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
		expectNoTimers();
	});

	it("does not mistake having no transport neighbors for failed origin reachability", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		f.subject.peers.clear();
		const result = f.run().catch((error: unknown) => error);
		await clock.tickAsync(2_000);
		expect(await result).to.be.instanceOf(DeliveryError);
		expect(f.subject.publishMessage.notCalled).to.equal(true);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
		expectNoTimers();
	});

	it("cancels the winning origin's blocked hedge while other origins are pending", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture(2);
		const firstAck = pDefer<void>();
		const signing = pDefer<any>();
		f.subject.publishMessage.returns(new Promise(() => {}));
		f.subject.publishMessage.onFirstCall().returns(firstAck.promise);
		f.subject.createMessage.onCall(2).returns(signing.promise);
		const result = f.run();
		await clock.tickAsync(1_000);
		firstAck.resolve();
		await clock.tickAsync(0);
		expect(f.subject.publishMessage.firstCall.args[4].aborted).to.equal(true);
		expect(f.subject.relayRootProbeCohort.controller.signal.aborted).to.equal(
			false,
		);
		signing.resolve({});
		await clock.tickAsync(0);
		expect(f.subject.publishMessage.callCount).to.equal(3);
		await clock.tickAsync(1_000);
		await result;
		expect([
			...f.subject.suppressedDepartedTopicRootCandidateClaims.keys(),
		]).to.deep.equal(["origin-1"]);
	});

	for (const failureKind of [
		"stalled signing",
		"local error",
		"foreign abort",
	] as const) {
		it(`shares one failed opening among three same-topic callers: ${failureKind}`, async () => {
			clock = sinon.useFakeTimers();
			const f = openingFixture();
			const failure =
				failureKind === "foreign abort"
					? new AbortError("foreign cancellation")
					: new Error("signing failed");
			if (failureKind === "stalled signing")
				f.subject.createMessage.returns(new Promise(() => {}));
			else f.subject.createMessage.rejects(failure);
			const results = Array.from({ length: 3 }, () =>
				f.subject.ensureFanoutChannel(topic).catch((error: unknown) => error),
			);
			await clock.tickAsync(2_000);
			const errors = await Promise.all(results);
			expect(f.subject.ensureFanoutChannelOnce.callCount).to.equal(1);
			expect(errors[1]).to.equal(errors[0]);
			expect(errors[2]).to.equal(errors[0]);
			if (failureKind === "stalled signing")
				expect(errors[0]).to.be.instanceOf(DeliveryError);
			else expect(errors[0]).to.equal(failure);
			expect(
				f.subject.suppressedDepartedTopicRootCandidateClaims.size,
			).to.equal(0);
			expectNoTimers();
		});
	}

	it("keeps shared work when one owner cancels and aborts synchronously after the last", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const signing = pDefer<any>();
		f.subject.createMessage.returns(signing.promise);
		const a = new AbortController();
		const b = new AbortController();
		const first = f.run({ controller: a }).catch((error: unknown) => error);
		const second = f.run({ controller: b }).catch((error: unknown) => error);
		await clock.tickAsync(0);
		const cohort = f.subject.relayRootProbeCohort;
		a.abort();
		expect(cohort.controller.signal.aborted).to.equal(false);
		signing.resolve({});
		b.abort(); // Signer's continuation is queued, but must never publish.
		expect(cohort.controller.signal.aborted).to.equal(true);
		await Promise.all([first, second, cohort.promise]);
		expect(f.subject.publishMessage.notCalled).to.equal(true);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
		expectNoTimers();
	});

	it("bounds a signer which ignores cancellation without declaring the origin dead", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const signing = pDefer<any>();
		f.subject.createMessage.returns(signing.promise);
		const result = f.run().catch((error: unknown) => error);
		await clock.tickAsync(2_000);
		expect(await result).to.be.instanceOf(DeliveryError);
		signing.resolve({});
		await clock.tickAsync(0);
		expect(f.subject.publishMessage.notCalled).to.equal(true);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
	});

	for (const phase of ["signing", "publish"] as const) {
		it(`preserves unexpected ${phase} error identity without suppression`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			const failure = new Error(`${phase} failed locally`);
			f.subject[
				phase === "signing" ? "createMessage" : "publishMessage"
			].rejects(failure);
			expect(await f.run().catch((error: unknown) => error)).to.equal(failure);
			expect(
				f.subject.suppressedDepartedTopicRootCandidateClaims.size,
			).to.equal(0);
			expectNoTimers();
		});
	}

	it("does not reuse a cancelled opening's negative observation on reopen", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const blocked = pDefer<void>();
		f.subject.publishMessage.returns(blocked.promise);
		let owner: object = {};
		const original = owner;
		const first = f
			.run({ isCurrent: () => owner === original })
			.catch((error: unknown) => error);
		await clock.tickAsync(0);
		owner = {};
		blocked.reject(new DeliveryError("old attempt failed"));
		await clock.tickAsync(1_000);
		expect(await first).to.be.instanceOf(AbortError);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
		f.subject.publishMessage.resolves();
		await f.run();
		expect(f.subject.publishMessage.callCount).to.equal(2);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
	});

	for (const changed of [
		"renewed claim",
		"direct reconnect",
		"new indirect route",
		"lifecycle",
	] as const) {
		it(`does not apply an old failure after ${changed}`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			const blocked = pDefer<void>();
			f.subject.publishMessage.returns(blocked.promise);
			const result = f.run().catch((error: unknown) => error);
			await clock.tickAsync(0);
			if (changed === "renewed claim") {
				f.claims.set("origin-0", {
					...f.claims.get("origin-0")!,
					timestamp: 2n,
				});
			} else if (changed === "direct reconnect")
				f.subject.peers.set("origin-0", {});
			else if (changed === "new indirect route") {
				f.subject.routes.isReachable.returns(true);
				f.subject.routes.getRouteHints.returns([
					{ nextHop: "relay", session: 2, updatedAt: 2 },
				]);
			} else f.subject.topicControlPlaneLifecycleRevision++;
			blocked.reject(new DeliveryError("no ACK"));
			await clock.tickAsync(1_000);
			await result;
			expect(
				f.subject.suppressedDepartedTopicRootCandidateClaims.size,
			).to.equal(0);
		});
	}

	it("does not mistake a stale cached route for new liveness", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		f.subject.routes.isReachable.returns(true);
		f.subject.routes.getRouteHints.returns([
			{ nextHop: "old-relay", session: 1, updatedAt: 1 },
		]);
		f.subject.publishMessage.returns(new Promise(() => {}));
		const result = f.run();
		await clock.tickAsync(2_000);
		await result;
		expect(
			f.subject.suppressedDepartedTopicRootCandidateClaims.has("origin-0"),
		).to.equal(true);
	});

	for (const policy of ["explicit", "resolver", "tracker"] as const) {
		it(`preserves ${policy} policy installed during the probe`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			const blocked = pDefer<void>();
			f.subject.publishMessage.returns(blocked.promise);
			const result = f.run();
			await clock.tickAsync(0);
			if (policy === "explicit")
				f.subject.topicRootControlPlane.setTopicRoot(topic, "configured");
			else if (policy === "resolver")
				f.subject.topicRootControlPlane.setTopicRootResolver(
					() => "configured",
				);
			else
				f.subject.topicRootControlPlane.setTopicRootTrackers([
					{ resolveRoot: () => "configured" },
				]);
			blocked.reject(new DeliveryError("no ACK"));
			await clock.tickAsync(1_000);
			await result;
			expect(
				f.subject.suppressedDepartedTopicRootCandidateClaims.size,
			).to.equal(0);
			expect(
				await f.subject.topicRootControlPlane.resolveTrackedTopicRoot(topic),
			).to.equal("configured");
		});
	}

	it("bounds a stalled policy recheck and never renews its budget on retry", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const attempt = {};
		f.subject.publishMessage.rejects(new DeliveryError("no ACK"));
		f.subject.topicRootControlPlane.setTopicRootResolver(
			() => new Promise(() => {}),
		);
		const result = f.run({ attempt }).catch((error: unknown) => error);
		await clock.tickAsync(3_000);
		expect(await result).to.be.instanceOf(AbortError);
		expect(
			await f.run({ attempt }).catch((error: unknown) => error),
		).to.be.instanceOf(AbortError);
		expect(f.subject.publishMessage.callCount).to.equal(2);
		expect(f.subject.suppressedDepartedTopicRootCandidateClaims.size).to.equal(
			0,
		);
	});

	it("incoming root queries never delegate to configured trackers", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const tracker = sinon
			.stub()
			.throws(new Error("recursive tracker must not run"));
		f.subject.topicRootControlPlane.setTopicRootTrackers([
			{ resolveRoot: tracker },
		]);
		Object.assign(f.subject, {
			topicRootResolutionAbortController: new AbortController(),
			topicRootCandidateResolutionAbortController: new AbortController(),
			ensureFanoutChannel: sinon.stub().resolves(),
		});
		await f.subject.resolveQueryableTopicRoot(topic);
		expect(tracker.notCalled).to.equal(true);
	});

	for (const path of ["opening", "query responder"] as const) {
		it(`bounds a replacement resolver stalling after the ${path} policy recheck`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			Object.assign(f.subject, {
				topicRootResolutionAbortController: new AbortController(),
				topicRootCandidateResolutionAbortController: new AbortController(),
				fanoutChannels: new Map(),
				ensureFanoutChannelInFlight: new Map(),
			});
			let calls = 0;
			f.subject.topicRootControlPlane.setTopicRootResolver((): undefined => {
				if (++calls === 2)
					f.subject.topicRootControlPlane.setTopicRootResolver(
						() => new Promise(() => {}),
					);
				return undefined;
			});
			const result = (
				path === "opening"
					? f.subject.ensureFanoutChannel(topic)
					: f.subject.resolveQueryableTopicRoot(topic)
			).catch((error: unknown) => error);
			await clock.tickAsync(2_000);
			expect(await result).to.be.instanceOf(AbortError);
			expect(calls).to.equal(2);
			expect(
				f.subject.suppressedDepartedTopicRootCandidateClaims.size,
			).to.equal(0);
			expect(f.subject.fanoutChannels.size).to.equal(0);
			expectNoTimers();
		});
	}

	it("last unsubscribe cancels a pre-channel opening and same-topic reopen owns new work", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const state = f.subject;
		Object.assign(state, {
			subscriptions: new Map(),
			pendingSubscriptions: new Set(),
			shardRefCounts: new Map(),
			fanoutChannels: new Map(),
			ensureFanoutChannelInFlight: new Map(),
			initializeTopic: sinon.stub(),
			untrackTopic: sinon.stub(),
			getShardTopicForUserTopic: () => topic,
			announceDirectSubscriptions: sinon.stub().resolves(),
			announceShardSubscriptions: sinon.stub().resolves(),
			scheduleReconcileShardOverlays: sinon.stub(),
			debounceSubscribeAggregator: { has: () => false },
			debounceUnsubscribeAggregator: { add: sinon.stub().resolves() },
			ensureFanoutChannelOnce: async (
				_topic: string,
				options: any,
				_revision: number,
				_generation: string,
				attempt: any,
				current: () => boolean,
			) =>
				state.preflightRelayRootCandidates(
					topic,
					attempt,
					options.signal,
					current,
				),
		});
		const oldWrite = pDefer<void>();
		state.publishMessage.returns(oldWrite.promise);
		const first = state
			._subscribe([{ key: "user-topic", counter: 1 }])
			.catch((error: unknown) => error);
		await clock.tickAsync(0);
		const old = state.relayRootProbeCohort;
		await state.unsubscribe("user-topic", { force: true });
		expect(old.controller.signal.aborted).to.equal(true);
		state.publishMessage.resolves();
		await state._subscribe([{ key: "user-topic", counter: 1 }]);
		expect(await first).to.be.instanceOf(AbortError);
		oldWrite.reject(new DeliveryError("late old miss"));
		await clock.tickAsync(0);
		expect(state.suppressedDepartedTopicRootCandidateClaims.size).to.equal(0);
		expect(state.announceShardSubscriptions.callCount).to.equal(1);
	});

	const realSession = async () => {
		const perPeer = new Map<
			string,
			{ fanout: FanoutTree; topicRootControlPlane: TopicRootControlPlane }
		>();
		const services = (components: any) => {
			const hash = getPublicKeyFromPeerId(components.peerId).hashcode();
			let value = perPeer.get(hash);
			if (!value) {
				const topicRootControlPlane = new TopicRootControlPlane();
				value = {
					topicRootControlPlane,
					fanout: new FanoutTree(components, {
						topicRootControlPlane,
						connectionManager: false,
					}),
				};
				perPeer.set(hash, value);
			}
			return value;
		};
		session = await TestSession.disconnected(3, {
			services: {
				fanout: (components: any) => services(components).fanout,
				pubsub: (components: any) =>
					new TopicControlPlane(components, {
						...services(components),
						connectionManager: false,
						canRelayMessage: true,
						shardCount: 16,
					}),
			},
		});
		return session.peers.map((peer) => peer.services.pubsub);
	};

	it("suppresses actual indirect route loss while retaining exact signed bytes and replay floor", async () => {
		const [receiver, relay, origin] = await realSession();
		const state = receiver as any;
		sinon.stub(state, "scheduleHostOwnedShardRoots");
		const stream = receiver!.addPeer(
			relay!.peerId,
			relay!.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"claim-relay",
		);
		const claim = (origin as any).localSignedTopicRootCandidateClaim;
		await state.importSignedTopicRootCandidateClaim(
			claim.bytes,
			relay!.publicKeyHash,
			stream,
		);
		state.rebuildAutoTopicRootCandidatesFromClaims(undefined, {
			immediate: true,
		});
		const retained = state.signedTopicRootCandidateClaims.get(
			origin!.publicKeyHash,
		);
		const floor = state.topicRootCandidateClaimReplayFloors.get(
			origin!.publicKeyHash,
		);
		expect(retained).to.not.equal(undefined);
		receiver!.updateSession(origin!.publicKey, Date.now());
		receiver!.addRouteConnection(
			receiver!.publicKeyHash,
			relay!.publicKeyHash,
			origin!.publicKey,
			1,
			Date.now(),
			Date.now(),
		);
		receiver!.onPeerUnreachable(origin!.publicKeyHash); // stale event; route is still current
		expect(receiver!.topicRootControlPlane.getTopicRootCandidates()).to.include(
			origin!.publicKeyHash,
		);
		receiver!.removePeerFromRoutes(origin!.publicKeyHash);
		expect(
			receiver!.topicRootControlPlane.getTopicRootCandidates(),
		).not.to.include(origin!.publicKeyHash);
		expect(
			state.signedTopicRootCandidateClaims.get(origin!.publicKeyHash),
		).to.equal(retained);
		expect(
			state.topicRootCandidateClaimReplayFloors.get(origin!.publicKeyHash),
		).to.equal(floor);
		await state.importSignedTopicRootCandidateClaim(
			claim.bytes,
			relay!.publicKeyHash,
			stream,
		);
		expect(
			receiver!.topicRootControlPlane.getTopicRootCandidates(),
		).not.to.include(origin!.publicKeyHash);
	});

	for (const loseFirstAck of [false, true]) {
		it(`requires a real ACK signed by the requested non-neighbor origin${loseFirstAck ? " after losing the first ACK" : ""}`, async () => {
			const [receiver, relay, origin] = await realSession();
			const state = receiver as any;
			sinon.stub(state, "scheduleHostOwnedShardRoots");
			const stream = receiver!.addPeer(
				relay!.peerId,
				relay!.publicKey,
				"/peerbit/topic-control-plane/2.1.0",
				"probe-relay",
			);
			const claim = (origin as any).localSignedTopicRootCandidateClaim;
			expect(
				await state.importSignedTopicRootCandidateClaim(
					claim.bytes,
					relay!.publicKeyHash,
					stream,
				),
			).to.equal(true);
			state.rebuildAutoTopicRootCandidatesFromClaims(undefined, {
				immediate: true,
			});
			const entered = pDefer<DataMessage>();
			const retried = pDefer<DataMessage>();
			let writes = 0;
			sinon.stub(state, "waitForPeerWrite").callsFake(async (_peer, bytes) => {
				const message = DataMessage.from(
					new Uint8ArrayList(bytes as Uint8Array),
				);
				if (!message.data?.length) {
					if (++writes === 1) entered.resolve(message);
					else retried.resolve(message);
				}
			});
			let settled = false;
			const result = state
				.preflightRelayRootCandidates(topic, {}, undefined, () => true)
				.then(() => {
					settled = true;
				});
			let request = await Promise.race([
				entered.promise,
				result.then(() => {
					throw new Error("preflight settled without an outbound probe");
				}),
			]);
			if (loseFirstAck) {
				const firstId = request.id;
				request = await retried.promise;
				expect(request.id).not.to.deep.equal(firstId);
			}
			const ackFrom = async (signer: TopicControlPlane) => {
				const ack = await new ACK({
					messageIdToAcknowledge: request.id,
					seenCounter: 0,
					header: new MessageHeader({
						session: 1,
						mode: new TracedDelivery([receiver!.publicKeyHash]),
						priority: 1,
					}),
				}).sign(signer.sign);
				const raw = ack.bytes();
				await receiver!.onAck(
					relay!.publicKey,
					stream,
					raw,
					ACK.from(new Uint8ArrayList(raw)),
				);
			};
			await ackFrom(relay!);
			expect(settled).to.equal(false);
			expect(state._ackCallbacks.size).to.equal(loseFirstAck ? 2 : 1);
			await ackFrom(origin!);
			await result;
			expect(
				state.suppressedDepartedTopicRootCandidateClaims.has(
					origin!.publicKeyHash,
				),
			).to.equal(false);
			expect(state._ackCallbacks.size).to.equal(0);
			expect(state.healthChecks.size).to.equal(0);
		});
	}

	it("keeps an explicit auto-mode root despite a differing direct-peer reply", async () => {
		const [receiver, _relay, origin] = await realSession();
		const state = receiver as any;
		sinon.stub(state, "scheduleHostOwnedShardRoots");
		receiver!.addPeer(
			origin!.peerId,
			origin!.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"explicit-origin",
		);
		receiver!.topicRootControlPlane.setTopicRoot(topic, origin!.publicKeyHash);
		const replies = sinon
			.stub(state, "queryTopicRootFromPeer")
			.resolves(receiver!.publicKeyHash);
		sinon.stub(receiver!.fanout, "joinChannel").resolves({} as any);
		await state.ensureFanoutChannel(topic);
		expect(state.fanoutChannels.get(topic).root).to.equal(
			origin!.publicKeyHash,
		);
		expect(replies.notCalled).to.equal(true);
		expect(state.suppressedDepartedTopicRootCandidateClaims.size).to.equal(0);
	});

	it("freshly resolves a replacement resolver after the bounded policy recheck", async () => {
		const [receiver, relay, origin] = await realSession();
		const state = receiver as any;
		sinon.stub(state, "scheduleHostOwnedShardRoots");
		const stream = receiver!.addPeer(
			relay!.peerId,
			relay!.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"policy-relay",
		);
		expect(
			await state.importSignedTopicRootCandidateClaim(
				(origin as any).localSignedTopicRootCandidateClaim.bytes,
				relay!.publicKeyHash,
				stream,
			),
		).to.equal(true);
		state.rebuildAutoTopicRootCandidatesFromClaims(undefined, {
			immediate: true,
		});
		const policyEntered = pDefer<void>();
		const oldPolicy = pDefer<string | undefined>();
		let calls = 0;
		receiver!.topicRootControlPlane.setTopicRootResolver(() => {
			if (++calls === 1) return undefined;
			policyEntered.resolve();
			return oldPolicy.promise;
		});
		sinon.stub(state, "publishMessage").resolves();
		sinon.stub(receiver!.fanout, "joinChannel").resolves({} as any);
		const opening = state.ensureFanoutChannel(topic);
		await policyEntered.promise;
		receiver!.topicRootControlPlane.setTopicRootResolver(
			() => origin!.publicKeyHash,
		);
		oldPolicy.resolve(undefined);
		await opening;
		expect(state.fanoutChannels.get(topic).root).to.equal(
			origin!.publicKeyHash,
		);
		expect(state.suppressedDepartedTopicRootCandidateClaims.size).to.equal(0);
	});
});
