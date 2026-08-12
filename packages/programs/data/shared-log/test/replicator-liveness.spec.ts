import type { PublicSignKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { ReplicationPingMessage } from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

type LivenessTestStore = EventStore<string, any>;
type LivenessTestHooks = {
	probeReplicatorLiveness(peerHash: string): Promise<void>;
	markReplicatorActivity(peerHash: string, now?: number): void;
	confirmReplicatorSubscriberPresence(peerHash: string): Promise<boolean>;
};

const getLivenessTestHooks = (store: LivenessTestStore): LivenessTestHooks =>
	(store.log as any)._liveness as LivenessTestHooks;

type SubscriberTestHooks = {
	_getTopicSubscribers(topic: string): Promise<PublicSignKey[] | undefined>;
};

const getSubscriberTestHooks = (
	store: LivenessTestStore,
): SubscriberTestHooks => store.log as unknown as SubscriberTestHooks;

describe("waitForReplicator liveness", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("evicts replicators that disappear without a clean close and emits leave", async () => {
		// Create a line topology: 0 <-> 2 <-> 1.
		// This mimics browser/relay scenarios where a peer can learn about a replicator via
		// broadcast replication announcements without having a direct connection that would
		// immediately emit unsubscribe/disconnect events.
		session = await TestSession.disconnectedMock(3);
		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			},
		});
		const peerHash = session.peers[1].identity.publicKey.hashcode();
		const leaveEvents: string[] = [];
		db0.log.events.addEventListener("replicator:leave", (event) => {
			leaveEvents.push(event.detail.publicKey.hashcode());
		});

		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			},
		});

		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 200 },
		);

		// Simulate abrupt tab-close: stop the peer without calling `Program.close()` / sending
		// replication reset messages.
		await session.peers[1].stop();

		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(1),
			{ timeout: 20_000, delayInterval: 200 },
		);
		expect(leaveEvents).to.deep.equal([peerHash]);
	});

	it("does not evict a healthy replicator after a single missed ping", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingFailuresLeft = 1;
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			return originalSend(...args);
		};
		// The single missed ping must be absorbed by the subscriber-presence
		// check, ahead of the failure counter and the recovery lane. Spying the
		// escalation proves WHICH mechanism keeps the replicator: if presence
		// confirmation ever stopped covering this, the probe would fall through
		// to scheduleReplicationInfoRequests and this pin would catch it.
		const log = db0.log as any;
		const scheduleRecovery = sinon.spy(log, "scheduleReplicationInfoRequests");

		try {
			await hooks.probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);
			expect(scheduleRecovery.notCalled).to.be.true;
			expect(log._liveness._replicatorLivenessFailures.has(peerHash)).to.be
				.false;

			await hooks.probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);
			expect(scheduleRecovery.notCalled).to.be.true;
		} finally {
			scheduleRecovery.restore();
			db0.log.rpc.send = originalSend;
		}
	});

	it("does not ping replicators that have been recently active", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingAttempts = 0;
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage) {
				pingAttempts++;
			}
			return originalSend(...args);
		};

		try {
			hooks.markReplicatorActivity(peerHash);
			await hooks.probeReplicatorLiveness(peerHash);
			expect(pingAttempts).to.equal(0);
			expect((await db0.log.getReplicators()).size).to.equal(2);
		} finally {
			db0.log.rpc.send = originalSend;
		}
	});

	it("does not let a stale liveness lookup evict a reconnected peer", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const peerHash = remoteKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const log = db0.log as any;
		const lookupStarted = pDefer<void>();
		const releaseLookup = pDefer<void>();
		const originalResolve = log._resolvePublicKeyFromHash.bind(log);
		let gateNextLookup = true;
		const resolve = sinon
			.stub(log, "_resolvePublicKeyFromHash")
			.callsFake(async (...args: unknown[]) => {
				const hash = args[0] as string;
				if (hash === peerHash && gateNextLookup) {
					gateNextLookup = false;
					lookupStarted.resolve();
					await releaseLookup.promise;
					return undefined;
				}
				return originalResolve(hash);
			});
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");

		try {
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			const staleProbe = hooks.probeReplicatorLiveness(peerHash);
			await lookupStarted.promise;

			// A fresh subscription advances the peer epoch while the old probe is
			// still resolving. The old continuation must not queue behind this
			// reconnect barrier and delete the newly current generation.
			await log._onSubscription({
				detail: { from: remoteKey, topics: [db0.log.topic] },
			});
			releaseLookup.resolve();
			await staleProbe;

			expect(disconnected.notCalled).to.be.true;
			expect(db0.log.uniqueReplicators.has(peerHash)).to.be.true;
			expect(log._liveness._replicatorLivenessFailures.has(peerHash)).to.be
				.false;
			expect(
				await db0.log.replicationIndex.count({ query: { hash: peerHash } }),
			).to.be.greaterThan(0);
		} finally {
			releaseLookup.resolve();
			resolve.restore();
			disconnected.restore();
		}
	});

	it("does not evict a peer that becomes active during a liveness lookup", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const log = db0.log as any;
		const lookupStarted = pDefer<void>();
		const releaseLookup = pDefer<void>();
		const originalResolve = log._resolvePublicKeyFromHash.bind(log);
		let gateNextLookup = true;
		const resolve = sinon
			.stub(log, "_resolvePublicKeyFromHash")
			.callsFake(async (...args: unknown[]) => {
				const hash = args[0] as string;
				if (hash === peerHash && gateNextLookup) {
					gateNextLookup = false;
					lookupStarted.resolve();
					await releaseLookup.promise;
					return undefined;
				}
				return originalResolve(hash);
			});
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");

		try {
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			const probe = hooks.probeReplicatorLiveness(peerHash);
			await lookupStarted.promise;

			// An ordinary replication/sync message does not advance the subscription
			// epoch, but it is positive liveness evidence and must cancel this probe.
			hooks.markReplicatorActivity(peerHash);
			releaseLookup.resolve();
			await probe;

			expect(disconnected.notCalled).to.be.true;
			expect(db0.log.uniqueReplicators.has(peerHash)).to.be.true;
			expect(
				await db0.log.replicationIndex.count({ query: { hash: peerHash } }),
			).to.be.greaterThan(0);
		} finally {
			releaseLookup.resolve();
			resolve.restore();
			disconnected.restore();
		}
	});

	it("does not evict a peer that becomes active while eviction is queued", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const log = db0.log as any;
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		const originalConfirmSubscriberPresence =
			hooks.confirmReplicatorSubscriberPresence.bind(hooks);
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage) {
				throw new Error("synthetic liveness miss");
			}
			return originalSend(...args);
		};
		hooks.confirmReplicatorSubscriberPresence = async () => false;

		const blockerStarted = pDefer<void>();
		const releaseBlocker = pDefer<void>();
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");

		try {
			const blocker = log.withReplicationInfoApplyQueue(peerHash, async () => {
				blockerStarted.resolve();
				await releaseBlocker.promise;
			});
			await blockerStarted.promise;
			const blockerTail = log._replicationInfoApplyQueueByPeer.get(peerHash);

			log._liveness._replicatorLastActivityAt.set(
				peerHash,
				Date.now() - 60_000,
			);
			// Seed one prior miss so this probe reaches the eviction threshold.
			log._liveness._replicatorLivenessFailures.set(peerHash, 1);
			const eviction = hooks.probeReplicatorLiveness(peerHash);
			await waitForResolved(() =>
				expect(log._replicationInfoApplyQueueByPeer.get(peerHash)).not.to.equal(
					blockerTail,
				),
			);

			// No subscription event occurs here: activity alone must revoke the
			// liveness observation before the queued removal reaches its lane head.
			hooks.markReplicatorActivity(peerHash);
			releaseBlocker.resolve();
			await Promise.all([blocker, eviction]);

			expect(disconnected.notCalled).to.be.true;
			expect(db0.log.uniqueReplicators.has(peerHash)).to.be.true;
			expect(log._liveness._replicatorLivenessFailures.has(peerHash)).to.be
				.false;
			expect(
				await db0.log.replicationIndex.count({ query: { hash: peerHash } }),
			).to.be.greaterThan(0);
		} finally {
			releaseBlocker.resolve();
			disconnected.restore();
			db0.log.rpc.send = originalSend;
			hooks.confirmReplicatorSubscriberPresence =
				originalConfirmSubscriberPresence;
		}
	});

	it("does not evict a subscribed replicator when liveness pings fail", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingFailuresLeft = 2;
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			return originalSend(...args);
		};

		try {
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);
		} finally {
			db0.log.rpc.send = originalSend;
		}
	});

	it("keeps a replicator when the broader subscriber view still sees it", async () => {
		session = await TestSession.disconnectedMock(3);
		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const subscriberHooks = getSubscriberTestHooks(db0);
		const pubsub = session.peers[0].services.pubsub;
		const originalGetSubscribers = pubsub.getSubscribers.bind(pubsub);
		const originalGetTopicSubscribers =
			subscriberHooks._getTopicSubscribers.bind(subscriberHooks);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingFailuresLeft = 2;
		pubsub.getSubscribers = (topic: string) => {
			if (topic === db0.log.rpc.topic) {
				return [];
			}
			return originalGetSubscribers(topic);
		};
		subscriberHooks._getTopicSubscribers = async (topic: string) => {
			if (topic === db0.log.rpc.topic) {
				return [session.peers[1].identity.publicKey];
			}
			return originalGetTopicSubscribers(topic);
		};
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			return originalSend(...args);
		};
		// Both missed pings must be absorbed by the broader subscriber view —
		// that is the mechanism this test is named for. Spying the escalation
		// pins it: were the wider view to stop counting, the second probe would
		// hit the eviction threshold via scheduleReplicationInfoRequests.
		const log = db0.log as any;
		const scheduleRecovery = sinon.spy(log, "scheduleReplicationInfoRequests");

		try {
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);

			expect((await db0.log.getReplicators()).size).to.equal(2);
			expect(scheduleRecovery.notCalled).to.be.true;
			expect(log._liveness._replicatorLivenessFailures.has(peerHash)).to.be
				.false;
		} finally {
			scheduleRecovery.restore();
			pubsub.getSubscribers = originalGetSubscribers;
			subscriberHooks._getTopicSubscribers = originalGetTopicSubscribers;
			db0.log.rpc.send = originalSend;
		}
	});

	it("starts subscriber discovery while rebalancing during open", async () => {
		session = await TestSession.connected(1);

		const store = new EventStore<string, any>();
		const hooks = getLivenessTestHooks(store);
		const subscriberHooks = getSubscriberTestHooks(store);
		const originalGetTopicSubscribers =
			subscriberHooks._getTopicSubscribers.bind(subscriberHooks);
		const originalRebalanceParticipation =
			store.log.rebalanceParticipation.bind(store.log);

		let rebalanceStarted = false;
		let subscribersStarted = false;
		let releaseRebalance!: () => void;
		const rebalanceGate = new Promise<void>((resolve) => {
			releaseRebalance = resolve;
		});

		subscriberHooks._getTopicSubscribers = async (topic: string) => {
			subscribersStarted = true;
			return originalGetTopicSubscribers(topic);
		};
		store.log.rebalanceParticipation = async () => {
			rebalanceStarted = true;
			await rebalanceGate;
			return originalRebalanceParticipation();
		};

		const openPromise = session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		try {
			await waitForResolved(() => expect(rebalanceStarted).to.be.true, {
				timeout: 5_000,
				delayInterval: 20,
			});
			await waitForResolved(() => expect(subscribersStarted).to.be.true, {
				timeout: 1_000,
				delayInterval: 20,
			});
		} finally {
			releaseRebalance();
			subscriberHooks._getTopicSubscribers = originalGetTopicSubscribers;
			store.log.rebalanceParticipation = originalRebalanceParticipation;
		}

		await openPromise;
	});

	it("invalidates cached topic subscribers when the cache is cleared", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		const hooks = getLivenessTestHooks(db0);
		const subscriberHooks = getSubscriberTestHooks(db0);
		const syntheticTopic = `${db0.log.topic}/synthetic-cache`;
		const pubsub = session.peers[0].services.pubsub;
		const originalGetSubscribers = pubsub.getSubscribers.bind(pubsub);

		pubsub.getSubscribers = ((topic: string) => {
			if (topic === syntheticTopic) {
				return [peerHash];
			}
			return originalGetSubscribers(topic);
		}) as typeof pubsub.getSubscribers;

		try {
			const cachedHashes = (
				await subscriberHooks._getTopicSubscribers(syntheticTopic)
			)?.map((key) => key.hashcode());
			expect(cachedHashes).to.include(peerHash);
			expect((db0.log as any)._topicSubscribersCache.has(syntheticTopic)).to.be
				.true;

			pubsub.getSubscribers = ((topic: string) => {
				if (topic === syntheticTopic) {
					return [];
				}
				return originalGetSubscribers(topic);
			}) as typeof pubsub.getSubscribers;

			const stillCached = (
				await subscriberHooks._getTopicSubscribers(syntheticTopic)
			)?.map((key) => key.hashcode());
			expect(stillCached).to.include(peerHash);

			(db0.log as any).invalidateTopicSubscribersCache(syntheticTopic);
			expect((db0.log as any)._topicSubscribersCache.has(syntheticTopic)).to.be
				.false;
		} finally {
			pubsub.getSubscribers = originalGetSubscribers;
		}
	});

	it("can relearn a liveness-evicted replicator from later replication info", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const peerHash = session.peers[1].identity.publicKey.hashcode();
		const joinEvents: string[] = [];
		const leaveEvents: string[] = [];
		db0.log.events.addEventListener("replicator:join", (event) => {
			joinEvents.push(event.detail.publicKey.hashcode());
		});
		db0.log.events.addEventListener("replicator:leave", (event) => {
			leaveEvents.push(event.detail.publicKey.hashcode());
		});

		const db1 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);
		await waitForResolved(
			() =>
				expect(
					joinEvents.filter((eventHash) => eventHash === peerHash),
				).to.have.length(1),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		const originalConfirmSubscriberPresence =
			hooks.confirmReplicatorSubscriberPresence.bind(hooks);
		let pingFailuresLeft = 2;
		let failRecoveryRequests = true;
		const originalResumeParkedRequest = (
			db0.log as any
		)._v2Receive.resumeParkedRequest.bind((db0.log as any)._v2Receive);
		// Recovery-request failure is expressed HERE, at the parked-request
		// resume, not at rpc.send: the request lane retries itself and swallows
		// its own send errors, so a send-level injection would never reach the
		// liveness path.
		const resumeParkedRequest = sinon
			.stub((db0.log as any)._v2Receive, "resumeParkedRequest")
			.callsFake((properties) =>
				failRecoveryRequests ? false : originalResumeParkedRequest(properties),
			);
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			return originalSend(...args);
		};
		hooks.confirmReplicatorSubscriberPresence = async () => false;

		try {
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);

			await waitForResolved(
				async () => expect((await db0.log.getReplicators()).size).to.equal(1),
				{ timeout: 5_000, delayInterval: 100 },
			);
			expect(leaveEvents).to.deep.equal([peerHash]);

			failRecoveryRequests = false;
			hooks.confirmReplicatorSubscriberPresence =
				originalConfirmSubscriberPresence;
			await db1.log.replicate({ factor: 1 }, { reset: true });

			await waitForResolved(
				async () => expect((await db0.log.getReplicators()).size).to.equal(2),
				{ timeout: 20_000, delayInterval: 100 },
			);
			await waitForResolved(
				() =>
					expect(
						joinEvents.filter((eventHash) => eventHash === peerHash),
					).to.have.length(2),
				{ timeout: 20_000, delayInterval: 100 },
			);
		} finally {
			resumeParkedRequest.restore();
			db0.log.rpc.send = originalSend;
			hooks.confirmReplicatorSubscriberPresence =
				originalConfirmSubscriberPresence;
		}
	});
});
