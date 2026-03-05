import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import type {
	UnsubcriptionEvent,
	UnsubscriptionReason,
} from "@peerbit/pubsub-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

describe("pubsub (unsubscribe reason)", function () {
	const createDisconnectedSession = async (
		peerCount: number,
		options?: {
			pubsub?: Partial<ConstructorParameters<typeof TopicControlPlane>[1]>;
		},
	) => {
		const topicRootControlPlane = new TopicRootControlPlane();
		const fanoutByHash = new Map<string, FanoutTree>();
		const getOrCreateFanout = (c: any) => {
			const hash = getPublicKeyFromPeerId(c.peerId).hashcode();
			let fanout = fanoutByHash.get(hash);
			if (!fanout) {
				fanout = new FanoutTree(c, {
					connectionManager: false,
					topicRootControlPlane,
				});
				fanoutByHash.set(hash, fanout);
			}
			return fanout;
		};

		return TestSession.disconnected<{
			pubsub: TopicControlPlane;
			fanout: FanoutTree;
		}>(peerCount, {
			services: {
				fanout: (c: any) => getOrCreateFanout(c),
				pubsub: (c: any) =>
					new TopicControlPlane(c, {
						canRelayMessage: true,
						connectionManager: false,
						topicRootControlPlane,
						fanout: getOrCreateFanout(c),
						shardCount: 16,
						fanoutJoin: {
							timeoutMs: 10_000,
							retryMs: 50,
							bootstrapEnsureIntervalMs: 200,
							trackerQueryIntervalMs: 200,
							joinReqTimeoutMs: 1_000,
							trackerQueryTimeoutMs: 1_000,
						},
						...(options?.pubsub || {}),
					}),
			},
		});
	};

	const setupTrackedSubscribers = async (
		topic: string,
		session: Awaited<ReturnType<typeof createDisconnectedSession>>,
	) => {
		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await session.connect([[session.peers[0], session.peers[1]]]);
		await Promise.all([a.subscribe(topic), b.subscribe(topic)]);

		await waitForResolved(() => {
			const aTopics = a.topics.get(topic);
			const bTopics = b.topics.get(topic);
			expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
			expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
		});

		return { a, b };
	};

	const setupTrackedSubscribersViaRelay = async (
		topic: string,
		session: Awaited<ReturnType<typeof createDisconnectedSession>>,
	) => {
		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;

		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

		const relayHash = relay.publicKeyHash;
		for (const peer of session.peers) {
			peer!.services.pubsub.setTopicRootCandidates([relayHash]);
		}
		await relay.hostShardRootsNow();

		await Promise.all([a.subscribe(topic), b.subscribe(topic)]);

		await waitForResolved(() => {
			const aTopics = a.topics.get(topic);
			const bTopics = b.topics.get(topic);
			expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
			expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
		});

		return { a, b };
	};

	const expectUnsubscribeEvent = async (properties: {
		events: UnsubcriptionEvent[];
		fromHash: string;
		topic: string;
		reason: UnsubscriptionReason;
	}) => {
		await waitForResolved(() => {
			const match = properties.events.find(
				(e) =>
					e.from.hashcode() === properties.fromHash &&
					e.topics.includes(properties.topic),
			);
			expect(match).to.not.equal(undefined);
			expect(match!.reason).to.equal(properties.reason);
		});
	};

	it("emits reason=remote-unsubscribe on explicit unsubscribe", async () => {
		const topic = "unsubscribe-reason-remote";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				await b.unsubscribe(topic);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "remote-unsubscribe",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("emits reason=peer-unreachable when a tracked peer becomes unreachable", async () => {
		const topic = "unsubscribe-reason-unreachable";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerUnreachable(b.publicKeyHash);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-unreachable",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("emits reason=peer-unreachable for tracked relay-only subscribers", async () => {
		const topic = "unsubscribe-reason-unreachable-relay";
		const session = await createDisconnectedSession(3);
		try {
			const { a, b } = await setupTrackedSubscribersViaRelay(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerUnreachable(b.publicKeyHash);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-unreachable",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("emits reason=peer-session-reset on peer session updates", async () => {
		const topic = "unsubscribe-reason-session";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);
			const bPublicKey = getPublicKeyFromPeerId(session.peers[1]!.peerId);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerSession(bPublicKey, 1);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-session-reset",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});
});
