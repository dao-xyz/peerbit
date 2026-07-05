// Mixed-topology interop for the topic-control-plane port: one rust-core
// peer and default (pure TS) peers exchanging subscriptions, data and
// availability control through a default relay that hosts the shard roots.
// Subscribe-state convergence across mixed peers is the named interop risk
// of this stage, so it is asserted explicitly in both directions, including
// the unsubscribe and peer-unavailable flows.
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
	waitForSubscribers,
} from "@peerbit/pubsub";
import type {
	DataEvent,
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import type { RustCoreStream } from "@peerbit/stream";
import { SilentDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { createRustCoreStream } from "../src/index.js";

type Session = TestSession<{
	pubsub: TopicControlPlane;
	fanout: FanoutTree;
}>;

describe("pubsub rust-core mixed topology", () => {
	let core: RustCoreStream;
	let session: Session;

	before(async () => {
		core = await createRustCoreStream();
	});

	afterEach(async () => {
		await session?.stop();
	});

	// a (rust-core) — relay (default) — b (default); every peer keeps its own
	// TopicRootControlPlane like independent nodes in a real network.
	const createMixedSession = async () => {
		const perPeer = (rustCore: RustCoreStream | false) => {
			let plane: TopicRootControlPlane | undefined;
			let fanout: FanoutTree | undefined;
			const getPlane = () => (plane ??= new TopicRootControlPlane());
			const getFanout = (c: any) =>
				(fanout ??= new FanoutTree(c, {
					connectionManager: false,
					topicRootControlPlane: getPlane(),
					rustCore,
				}));
			return {
				services: {
					fanout: (c: any) => getFanout(c),
					pubsub: (c: any) =>
						new TopicControlPlane(c, {
							canRelayMessage: true,
							connectionManager: false,
							topicRootControlPlane: getPlane(),
							fanout: getFanout(c),
							shardCount: 4,
							fanoutJoin: {
								timeoutMs: 10_000,
								retryMs: 50,
								bootstrapEnsureIntervalMs: 200,
								trackerQueryIntervalMs: 200,
								joinReqTimeoutMs: 1_000,
								trackerQueryTimeoutMs: 1_000,
							},
							rustCore,
						}),
				},
			};
		};

		session = await TestSession.disconnected(3, [
			perPeer(core),
			perPeer(false),
			perPeer(false),
		]);
		const [a, relay, b] = session.peers.map(
			(peer) => peer.services.pubsub,
		) as [TopicControlPlane, TopicControlPlane, TopicControlPlane];

		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);

		const relayHash = relay.publicKeyHash;
		for (const peer of session.peers) {
			peer.services.pubsub.setTopicRootCandidates([relayHash]);
		}
		await relay.hostShardRootsNow();

		return { a, relay, b };
	};

	const setupConvergedSubscribers = async (topic: string) => {
		const { a, relay, b } = await createMixedSession();
		await Promise.all([a.subscribe(topic), b.subscribe(topic)]);
		await waitForResolved(() => {
			expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(true);
			expect(b.topics.get(topic)?.has(a.publicKeyHash)).to.equal(true);
		});
		return { a, relay, b };
	};

	it("maps topics to the same shards and roots as default peers", async () => {
		const { a, relay, b } = await createMixedSession();
		for (const topic of ["topic-a", "topic-b", "日本語/💜", ""]) {
			const shardOfA = (a as any).getShardTopicForUserTopic(topic);
			const shardOfRelay = (relay as any).getShardTopicForUserTopic(topic);
			const shardOfB = (b as any).getShardTopicForUserTopic(topic);
			expect(shardOfA).to.equal(shardOfRelay);
			expect(shardOfA).to.equal(shardOfB);
			expect(await a.resolveTopicRoot(shardOfA)).to.equal(
				await b.resolveTopicRoot(shardOfB),
			);
		}
	});

	it("converges subscriptions in both directions across mixed peers", async () => {
		const topic = "mixed-subscription-convergence";
		const { a, b } = await createMixedSession();

		const subscribeEventsOnA: SubscriptionEvent[] = [];
		a.addEventListener("subscribe", (ev: CustomEvent<SubscriptionEvent>) =>
			subscribeEventsOnA.push(ev.detail),
		);

		await Promise.all([a.subscribe(topic), b.subscribe(topic)]);
		await waitForSubscribers(
			{ services: { pubsub: a } },
			[b.publicKeyHash],
			topic,
		);
		await waitForSubscribers(
			{ services: { pubsub: b } },
			[a.publicKeyHash],
			topic,
		);

		// tracked state matches on both sides, with watermarks recorded
		await waitForResolved(() => {
			expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(true);
			expect(b.topics.get(topic)?.has(a.publicKeyHash)).to.equal(true);
			expect(
				a.lastSubscriptionMessages.get(b.publicKeyHash)?.get(topic),
			).to.exist;
			expect(
				b.lastSubscriptionMessages.get(a.publicKeyHash)?.get(topic),
			).to.exist;
		});
		expect(
			subscribeEventsOnA.some(
				(ev) =>
					ev.from.hashcode() === b.publicKeyHash &&
					ev.topics.includes(topic),
			),
		).to.equal(true);

		const subscribersOfA = a
			.getSubscribers(topic)!
			.map((key) => key.hashcode());
		expect(subscribersOfA).to.include(a.publicKeyHash);
		expect(subscribersOfA).to.include(b.publicKeyHash);
	});

	it("delivers topic data in both directions across mixed peers", async () => {
		const topic = "mixed-data-delivery";
		const { a, b } = await setupConvergedSubscribers(topic);

		const dataOnA: DataEvent[] = [];
		const dataOnB: DataEvent[] = [];
		a.addEventListener("data", (ev: CustomEvent<DataEvent>) =>
			dataOnA.push(ev.detail),
		);
		b.addEventListener("data", (ev: CustomEvent<DataEvent>) =>
			dataOnB.push(ev.detail),
		);

		await b.publish(new Uint8Array([1, 2, 3]), { topics: [topic] });
		await waitForResolved(() => {
			expect(
				dataOnA.some(
					(ev) =>
						ev.data.topics.includes(topic) &&
						[...ev.data.data].toString() === "1,2,3",
				),
			).to.equal(true);
		});

		await a.publish(new Uint8Array([4, 5, 6]), { topics: [topic] });
		await waitForResolved(() => {
			expect(
				dataOnB.some(
					(ev) =>
						ev.data.topics.includes(topic) &&
						[...ev.data.data].toString() === "4,5,6",
				),
			).to.equal(true);
		});
	});

	it("delivers explicit-recipient publishes (the rpc path) both ways", async () => {
		const topic = "mixed-direct-delivery";
		const { a, b } = await setupConvergedSubscribers(topic);

		const dataOnA: DataEvent[] = [];
		const dataOnB: DataEvent[] = [];
		a.addEventListener("data", (ev: CustomEvent<DataEvent>) =>
			dataOnA.push(ev.detail),
		);
		b.addEventListener("data", (ev: CustomEvent<DataEvent>) =>
			dataOnB.push(ev.detail),
		);

		// SilentDelivery with explicit recipients is exactly what the RPC
		// program (and thereby shared-log) rides on.
		await a.publish(new Uint8Array([7]), {
			topics: [topic],
			mode: new SilentDelivery({ to: [b.publicKeyHash], redundancy: 1 }),
		});
		await waitForResolved(() => {
			expect(
				dataOnB.some((ev) => [...ev.data.data].toString() === "7"),
			).to.equal(true);
		});

		await b.publish(new Uint8Array([8]), {
			topics: [topic],
			mode: new SilentDelivery({ to: [a.publicKeyHash], redundancy: 1 }),
		});
		await waitForResolved(() => {
			expect(
				dataOnA.some((ev) => [...ev.data.data].toString() === "8"),
			).to.equal(true);
		});
	});

	it("propagates unsubscribe from a default peer to the rust-core peer", async () => {
		const topic = "mixed-unsubscribe-default-to-rust";
		const { a, b } = await setupConvergedSubscribers(topic);

		const events: UnsubcriptionEvent[] = [];
		a.addEventListener("unsubscribe", (ev: CustomEvent<UnsubcriptionEvent>) =>
			events.push(ev.detail),
		);

		await b.unsubscribe(topic);
		await waitForResolved(() => {
			expect(
				events.some(
					(ev) =>
						ev.from.hashcode() === b.publicKeyHash &&
						ev.topics.includes(topic) &&
						ev.reason === "remote-unsubscribe",
				),
			).to.equal(true);
			expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(false);
		});
	});

	it("propagates unsubscribe from the rust-core peer to a default peer", async () => {
		const topic = "mixed-unsubscribe-rust-to-default";
		const { a, b } = await setupConvergedSubscribers(topic);

		const events: UnsubcriptionEvent[] = [];
		b.addEventListener("unsubscribe", (ev: CustomEvent<UnsubcriptionEvent>) =>
			events.push(ev.detail),
		);

		await a.unsubscribe(topic);
		await waitForResolved(() => {
			expect(
				events.some(
					(ev) =>
						ev.from.hashcode() === a.publicKeyHash &&
						ev.topics.includes(topic) &&
						ev.reason === "remote-unsubscribe",
				),
			).to.equal(true);
			expect(b.topics.get(topic)?.has(a.publicKeyHash)).to.equal(false);
		});
	});

	it("evicts an abruptly departed default peer on the rust-core peer", async () => {
		const topic = "mixed-peer-unavailable";
		const { a, b } = await setupConvergedSubscribers(topic);
		const departedHash = b.publicKeyHash;

		const events: UnsubcriptionEvent[] = [];
		a.addEventListener("unsubscribe", (ev: CustomEvent<UnsubcriptionEvent>) =>
			events.push(ev.detail),
		);

		// Abrupt stop: no Unsubscribe/Goodbye reaches the network, so state
		// must be shed through the peer-unavailable flow.
		await session.peers[2].stop();

		await waitForResolved(
			() => {
				expect(
					events.some(
						(ev) =>
							ev.from.hashcode() === departedHash &&
							ev.topics.includes(topic) &&
							ev.reason === "peer-unreachable",
					),
				).to.equal(true);
				expect(a.topics.get(topic)?.has(departedHash)).to.equal(false);
			},
			{ timeout: 20_000 },
		);
	});

	it("evicts an abruptly departed rust-core peer on a default peer", async () => {
		const topic = "mixed-peer-unavailable-reverse";
		const { b } = await setupConvergedSubscribers(topic);
		const departedHash = session.peers[0].services.pubsub.publicKeyHash;

		const events: UnsubcriptionEvent[] = [];
		b.addEventListener("unsubscribe", (ev: CustomEvent<UnsubcriptionEvent>) =>
			events.push(ev.detail),
		);

		await session.peers[0].stop();

		await waitForResolved(
			() => {
				expect(
					events.some(
						(ev) =>
							ev.from.hashcode() === departedHash &&
							ev.topics.includes(topic) &&
							ev.reason === "peer-unreachable",
					),
				).to.equal(true);
				expect(b.topics.get(topic)?.has(departedHash)).to.equal(false);
			},
			{ timeout: 20_000 },
		);
	});

	it("answers topic-root queries across the mixed link", async () => {
		const { a, relay } = await createMixedSession();
		// The rust-core peer resolves shard roots by querying its default
		// neighbours (TopicRootQuery/Response over the native codec).
		const shardTopic = (a as any).getShardTopicForUserTopic(
			"root-query-topic",
		) as string;
		const resolved = await a.resolveTopicRoot(shardTopic);
		expect(resolved).to.equal(relay.publicKeyHash);
	});
});
