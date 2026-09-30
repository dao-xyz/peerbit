import { Ed25519PublicKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { Subscribe, Unsubscribe } from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import { waitForResolved } from "@peerbit/time";
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

const connectedFixture = async () => {
	const perPeer = new Map<
		string,
		{ fanout: FanoutTree; topicRootControlPlane: TopicRootControlPlane }
	>();
	const session = await TestSession.disconnected<{
		pubsub: TopicControlPlane;
		fanout: FanoutTree;
	}>(2, {
		services: {
			fanout: (components: any) => {
				const topicRootControlPlane = new TopicRootControlPlane();
				const fanout = new FanoutTree(components, {
					connectionManager: false,
					topicRootControlPlane,
				});
				perPeer.set(components.peerId.toString(), {
					fanout,
					topicRootControlPlane,
				});
				return fanout;
			},
			pubsub: (components: any) =>
				new TopicControlPlane(components, {
					...perPeer.get(components.peerId.toString())!,
					connectionManager: false,
					shardCount: 1,
				}),
		},
	});
	try {
		const [receiver, sender] = session.peers.map(
			(peer) => peer.services.pubsub,
		);
		for (const pubsub of [receiver, sender]) {
			pubsub.setTopicRootCandidates([receiver.publicKeyHash]);
		}
		await receiver.hostShardRootsNow();
		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(receiver, sender);
		await Promise.all([receiver.subscribe(topic), sender.subscribe(topic)]);
		await waitForResolved(() =>
			expect(receiver.topics.get(topic)?.has(sender.publicKeyHash)).to.equal(
				true,
			),
		);
		return { session, receiver, sender };
	} catch (error) {
		await session.stop();
		throw error;
	}
};

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};

describe("pubsub (unsubscribe ordering)", () => {
	it("announces departure before a pending same-session reopen", async function () {
		this.timeout(15_000);
		const { session, receiver, sender } = await connectedFixture();
		const internals = sender as any;
		const aggregator = internals.debounceSubscribeAggregator;
		const originalAdd = aggregator.add;
		const resumeSubscribe = deferred();
		let reopened: Promise<void> | undefined;
		try {
			const events: string[] = [];
			for (const type of ["subscribe", "unsubscribe"] as const) {
				receiver.addEventListener(type, (event) => {
					if (
						event.detail.from.hashcode() === sender.publicKeyHash &&
						event.detail.topics.includes(topic)
					) {
						events.push(type);
					}
				});
			}
			// Freeze only the debounce boundary, not network delivery or signing.
			// Public subscribe() must already expose this topic as pending.
			aggregator.add = async (...args: any[]) => {
				await resumeSubscribe.promise;
				return originalAdd.apply(aggregator, args);
			};
			await sender.unsubscribe(topic);
			reopened = sender.subscribe(topic);
			expect(internals.pendingSubscriptions.has(topic)).to.equal(true);
			expect(internals.subscriptions.has(topic)).to.equal(false);
			await waitForResolved(
				() => expect(events).to.deep.equal(["unsubscribe"]),
				{ timeout: 5_000, delayInterval: 10 },
			);
			expect(receiver.topics.get(topic)?.has(sender.publicKeyHash)).to.equal(
				false,
			);
			resumeSubscribe.resolve();
			await reopened;
			await waitForResolved(
				() => expect(events).to.deep.equal(["unsubscribe", "subscribe"]),
				{ timeout: 5_000, delayInterval: 10 },
			);
			expect(receiver.topics.get(topic)?.has(sender.publicKeyHash)).to.equal(
				true,
			);
		} finally {
			resumeSubscribe.resolve();
			aggregator.add = originalAdd;
			await reopened?.catch(() => {});
			await session.stop();
		}
	});

	it("ignores an older signed departure delivered after a committed reopen", async function () {
		this.timeout(15_000);
		const { session, receiver, sender } = await connectedFixture();
		const internals = sender as any;
		const receiverInternals = receiver as any;
		const originalCreateMessage = internals.createMessage;
		const originalProcessUnsubscribe =
			receiverInternals.processUnsubscribeMessage;
		const resumeSigning = deferred();
		const timestamps: bigint[] = [];
		let receivedDepartures = 0;
		try {
			internals.createMessage = async (bytes: Uint8Array, ...args: any[]) => {
				const message = await originalCreateMessage.call(
					sender,
					bytes,
					...args,
				);
				const control = internals.decodePubSubMessage(bytes);
				if (control instanceof Unsubscribe && control.topics.includes(topic)) {
					timestamps.push(message.header.timestamp);
					await resumeSigning.promise;
				}
				return message;
			};
			receiverInternals.processUnsubscribeMessage = (...args: any[]) => {
				const result = originalProcessUnsubscribe.apply(receiver, args);
				if (args[2].hashcode() === sender.publicKeyHash) receivedDepartures++;
				return result;
			};
			await sender.unsubscribe(topic);
			await waitForResolved(() => expect(timestamps).to.have.length(2), {
				timeout: 5_000,
				delayInterval: 10,
			});
			await sender.subscribe(topic);
			await waitForResolved(() => {
				const current = receiver.lastSubscriptionMessages
					.get(sender.publicKeyHash)
					?.get(topic)?.timestamp;
				expect(current).to.exist;
				expect(timestamps.every((timestamp) => timestamp < current!)).to.equal(
					true,
				);
			});
			resumeSigning.resolve();
			await waitForResolved(
				() => expect(receivedDepartures).to.be.greaterThan(0),
				{ timeout: 5_000, delayInterval: 10 },
			);
			expect(receiver.topics.get(topic)?.has(sender.publicKeyHash)).to.equal(
				true,
			);
		} finally {
			resumeSigning.resolve();
			internals.createMessage = originalCreateMessage;
			receiverInternals.processUnsubscribeMessage = originalProcessUnsubscribe;
			await session.stop();
		}
	});

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
