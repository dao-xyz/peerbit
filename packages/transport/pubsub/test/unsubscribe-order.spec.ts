import { Ed25519PublicKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { Subscribe, Unsubscribe } from "@peerbit/pubsub-interface";
import { expect } from "chai";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

const keys = Array.from(
	{ length: 9 },
	(_, i) => new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(i + 1) }),
);
const topic = "unsubscribe-order";
const otherTopic = "unsubscribe-order-other";
type Path = "direct" | "shard";

// Replay already verified envelopes at the two subscription-processing boundaries.
// No transport timing or signature generation is needed to control their ordering.
const fixture = () => {
	const receiver = Object.create(
		TopicControlPlane.prototype,
	) as TopicControlPlane;
	Object.assign(receiver, {
		topics: new Map([
			[topic, new Map()],
			[otherTopic, new Map()],
		]),
		peerToTopic: new Map(),
		peers: new Map(),
		peerKeyHashToPublicKey: new Map(),
		lastSubscriptionMessages: new Map(),
		subscriberCacheMaxEntries: 2,
		dispatchEvent: () => true,
	});
	const internals = receiver as any;
	const deliver = async (
		path: Path,
		subscribed: boolean,
		key: Ed25519PublicKey,
		timestamp: bigint,
		topics = [topic],
		session = 1n,
	) => {
		const stream = internals.peers.get(key.hashcode()) ?? { publicKey: key };
		internals.peers.set(key.hashcode(), stream);
		const input = {
			pubsubMessage: subscribed
				? new Subscribe({ topics, requestSubscribers: false })
				: new Unsubscribe({ topics }),
			message: {
				header: {
					session,
					timestamp,
					signatures: { publicKeys: [key], signatures: [{ publicKey: key }] },
				},
			},
			from: key,
			stream,
			shardTopic: "shard",
		};
		await (path === "direct"
			? internals.processDirectPubSubMessage(input)
			: internals.processShardPubSubMessage(input));
	};
	return { receiver, internals, deliver };
};

describe("pubsub (unsubscribe ordering)", () => {
	for (const path of ["direct", "shard"] as const) {
		it(`rejects an older Subscribe from the other path after ${path} Unsubscribe`, async () => {
			const { receiver, deliver } = fixture();
			const otherPath = path === "direct" ? "shard" : "direct";
			const key = keys[0]!;
			const hash = key.hashcode();
			await deliver(otherPath, true, key, 100n);
			await deliver(path, false, key, 200n);
			expect(receiver.topics.get(topic)?.has(hash)).to.equal(false);
			await deliver(otherPath, true, key, 150n);
			expect(receiver.topics.get(topic)?.has(hash)).to.equal(false);

			await deliver(otherPath, true, key, 300n);
			expect(receiver.topics.get(topic)?.has(hash)).to.equal(true);
			await deliver(path, false, key, 400n);
			await deliver(otherPath, true, key, 50n, [topic], 2n);
			expect(receiver.topics.get(topic)?.get(hash)?.session).to.equal(2n);
		});
	}

	it("bounds inactive watermarks per topic without evicting active subscriber floors", async () => {
		const { receiver, deliver } = fixture();
		for (const key of keys.slice(0, 2)) {
			await deliver("direct", true, key, 100n);
		}
		for (const key of keys.slice(2, 8)) {
			await deliver("shard", false, key, 200n, [topic, otherTopic]);
		}
		await deliver("direct", false, keys[6]!, 300n, [topic, otherTopic]);
		await deliver("shard", false, keys[8]!, 200n, [topic, otherTopic]);

		for (const trackedTopic of [topic, otherTopic]) {
			const inactive = [...receiver.lastSubscriptionMessages]
				.filter(
					([hash, messages]) =>
						messages.has(trackedTopic) &&
						!receiver.topics.get(trackedTopic)?.has(hash),
				)
				.map(([hash]) => hash);
			expect(inactive).to.deep.equal([
				keys[6]!.hashcode(),
				keys[8]!.hashcode(),
			]);
		}
		for (const key of keys.slice(0, 2)) {
			expect(
				receiver.lastSubscriptionMessages.get(key.hashcode())?.get(topic),
			).to.deep.equal({
				session: 1n,
				timestamp: 100n,
			});
		}
		expect(receiver.lastSubscriptionMessages.size).to.equal(4);
	});

	it("retains another topic's unsubscribe floor when an active subscriber is evicted", async () => {
		const { receiver, deliver } = fixture();
		await deliver("direct", true, keys[0]!, 100n, [topic, otherTopic]);
		await deliver("shard", false, keys[0]!, 200n, [otherTopic]);
		for (const key of keys.slice(1, 3)) {
			await deliver("direct", true, key, 100n);
		}
		expect(receiver.topics.get(topic)?.has(keys[0]!.hashcode())).to.equal(
			false,
		);
		expect(
			receiver.lastSubscriptionMessages
				.get(keys[0]!.hashcode())
				?.has(otherTopic),
		).to.equal(true);
		await deliver("direct", true, keys[0]!, 150n, [otherTopic]);
		expect(receiver.topics.get(otherTopic)?.has(keys[0]!.hashcode())).to.equal(
			false,
		);
	});

	it("removes active and inactive watermarks when their local topic is untracked", async () => {
		const { receiver, internals, deliver } = fixture();
		await deliver("direct", true, keys[0]!, 100n, [topic, otherTopic]);
		await deliver("shard", false, keys[0]!, 200n, [otherTopic]);
		await deliver("direct", false, keys[1]!, 200n);
		await deliver("direct", true, keys[2]!, 100n, [topic, otherTopic]);

		internals.untrackTopic(topic);
		expect(receiver.lastSubscriptionMessages.has(keys[1]!.hashcode())).to.equal(
			false,
		);
		for (const messages of receiver.lastSubscriptionMessages.values()) {
			expect([...messages.keys()]).to.deep.equal([otherTopic]);
		}
		expect(receiver.peerToTopic.get(keys[2]!.hashcode())).to.deep.equal(
			new Set([otherTopic]),
		);
		internals.untrackTopic(otherTopic);
		expect(receiver.lastSubscriptionMessages.size).to.equal(0);
		expect(receiver.peerToTopic.size).to.equal(0);
	});

	it("clears retained inactive watermarks on stop", async () => {
		const topicRootControlPlane = new TopicRootControlPlane();
		let fanout: FanoutTree;
		const session = await TestSession.disconnected<{
			pubsub: TopicControlPlane;
			fanout: FanoutTree;
		}>(1, {
			services: {
				fanout: (components: any) =>
					(fanout = new FanoutTree(components, {
						connectionManager: false,
						topicRootControlPlane,
					})),
				pubsub: (components: any) =>
					new TopicControlPlane(components, {
						fanout,
						topicRootControlPlane,
						connectionManager: false,
						shardCount: 1,
					}),
			},
		});
		try {
			const receiver = session.peers[0]!.services.pubsub;
			receiver.lastSubscriptionMessages.set(
				keys[0]!.hashcode(),
				new Map([[topic, { session: 1n, timestamp: 200n }]]),
			);
			await receiver.stop();
			expect(receiver.lastSubscriptionMessages.size).to.equal(0);
		} finally {
			await session.stop();
		}
	});
});
