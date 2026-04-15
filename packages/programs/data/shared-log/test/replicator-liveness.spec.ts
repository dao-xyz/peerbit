import type { PublicSignKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	ReplicationPingMessage,
	RequestReplicationInfoMessage,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

type LivenessTestStore = EventStore<string, any>;
type LivenessTestHooks = {
	probeReplicatorLiveness(peerHash: string): Promise<void>;
	markReplicatorActivity(peerHash: string, now?: number): void;
	_getTopicSubscribers(topic: string): Promise<PublicSignKey[] | undefined>;
	confirmReplicatorSubscriberPresence(peerHash: string): Promise<boolean>;
};

const getLivenessTestHooks = (store: LivenessTestStore): LivenessTestHooks =>
	store.log as unknown as LivenessTestHooks;

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
		let failRecoveryRequests = true;
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			if (
				failRecoveryRequests &&
				message instanceof RequestReplicationInfoMessage
			) {
				throw new Error("synthetic replication-info miss");
			}
			return originalSend(...args);
		};

		try {
			await hooks.probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);

			failRecoveryRequests = false;
			await hooks.probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);
		} finally {
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
		const pubsub = session.peers[0].services.pubsub;
		const originalGetSubscribers = pubsub.getSubscribers.bind(pubsub);
		const originalGetTopicSubscribers =
			hooks._getTopicSubscribers.bind(hooks);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingFailuresLeft = 2;
		let failRecoveryRequests = true;
		pubsub.getSubscribers = (topic: string) => {
			if (topic === db0.log.rpc.topic) {
				return [];
			}
			return originalGetSubscribers(topic);
		};
		hooks._getTopicSubscribers = async (topic: string) => {
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
			if (
				failRecoveryRequests &&
				message instanceof RequestReplicationInfoMessage
			) {
				throw new Error("synthetic replication-info miss");
			}
			return originalSend(...args);
		};

		try {
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);
			hooks.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await hooks.probeReplicatorLiveness(peerHash);

			failRecoveryRequests = false;
			expect((await db0.log.getReplicators()).size).to.equal(2);
		} finally {
			pubsub.getSubscribers = originalGetSubscribers;
			hooks._getTopicSubscribers = originalGetTopicSubscribers;
			db0.log.rpc.send = originalSend;
		}
	});

	it("starts subscriber discovery while rebalancing during open", async () => {
		session = await TestSession.connected(1);

		const store = new EventStore<string, any>();
		const hooks = getLivenessTestHooks(store);
		const originalGetTopicSubscribers =
			hooks._getTopicSubscribers.bind(hooks);
		const originalRebalanceParticipation =
			store.log.rebalanceParticipation.bind(store.log);

		let rebalanceStarted = false;
		let subscribersStarted = false;
		let releaseRebalance!: () => void;
		const rebalanceGate = new Promise<void>((resolve) => {
			releaseRebalance = resolve;
		});

		hooks._getTopicSubscribers = async (topic: string) => {
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
			hooks._getTopicSubscribers = originalGetTopicSubscribers;
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
			const cachedHashes = (await hooks._getTopicSubscribers(syntheticTopic))?.map(
				(key) => key.hashcode(),
			);
			expect(cachedHashes).to.include(peerHash);
			expect((db0.log as any)._topicSubscribersCache.has(syntheticTopic)).to.be
				.true;

			pubsub.getSubscribers = ((topic: string) => {
				if (topic === syntheticTopic) {
					return [];
				}
				return originalGetSubscribers(topic);
			}) as typeof pubsub.getSubscribers;

			const stillCached = (await hooks._getTopicSubscribers(syntheticTopic))?.map(
				(key) => key.hashcode(),
			);
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
				expect(joinEvents.filter((eventHash) => eventHash === peerHash)).to.have.length(
					1,
				),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const hooks = getLivenessTestHooks(db0);
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		const originalConfirmSubscriberPresence =
			hooks.confirmReplicatorSubscriberPresence.bind(hooks);
		let pingFailuresLeft = 2;
		let failRecoveryRequests = true;
		db0.log.rpc.send = async (...args: Parameters<typeof originalSend>) => {
			const [message] = args;
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			if (
				failRecoveryRequests &&
				message instanceof RequestReplicationInfoMessage
			) {
				throw new Error("synthetic replication-info miss");
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
					expect(joinEvents.filter((eventHash) => eventHash === peerHash)).to.have
						.length(2),
				{ timeout: 20_000, delayInterval: 100 },
			);
		} finally {
			db0.log.rpc.send = originalSend;
			hooks.confirmReplicatorSubscriberPresence =
				originalConfirmSubscriberPresence;
		}
	});
});
