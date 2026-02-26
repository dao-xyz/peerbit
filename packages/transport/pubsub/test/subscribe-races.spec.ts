import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

describe("pubsub (subscribe race regressions)", function () {
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

	it("discovers peers when subscribe and connect happen concurrently", async () => {
		const TOPIC = "concurrent-subscribe-connect-regression";
		const session = await createDisconnectedSession(2);

		try {
			const a = session.peers[0]!.services.pubsub;
			const b = session.peers[1]!.services.pubsub;

			await Promise.all([
				a.subscribe(TOPIC),
				b.subscribe(TOPIC),
				session.connect([[session.peers[0], session.peers[1]]]),
			]);

			await waitForResolved(() => {
				const aTopics = a.topics.get(TOPIC);
				const bTopics = b.topics.get(TOPIC);
				expect(aTopics).to.not.equal(undefined);
				expect(bTopics).to.not.equal(undefined);
				expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
				expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
			});
		} finally {
			await session.stop();
		}
	});

	it("does not track a topic on a peer that never subscribed", async () => {
		const TOPIC = "non-subscriber-should-not-track-regression";
		const session = await createDisconnectedSession(2);

		try {
			const a = session.peers[0]!.services.pubsub;
			const b = session.peers[1]!.services.pubsub;

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			await b.subscribe(TOPIC);
			await delay(250);

			expect(a.topics.has(TOPIC)).to.equal(false);
			expect(a.topics.get(TOPIC)).to.equal(undefined);
		} finally {
			await session.stop();
		}
	});

	it("does not advertise cancelled pending subscriptions to peers", async () => {
		const TOPIC = "subscribe-then-unsubscribe-before-debounce-regression";
		const session = await createDisconnectedSession(2, {
			pubsub: {
				subscriptionDebounceDelay: 500,
			},
		});

		try {
			const a = session.peers[0]!.services.pubsub;
			const b = session.peers[1]!.services.pubsub;

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			const pendingSubscribe = a.subscribe(TOPIC).catch(() => {});
			const removed = await a.unsubscribe(TOPIC);
			expect(removed).to.equal(false);

			await b.subscribe(TOPIC);
			await waitForResolved(() => {
				expect(a.topics.has(TOPIC)).to.equal(false);
				const bTopics = b.topics.get(TOPIC);
				if (!bTopics) {
					return;
				}
				expect(bTopics.has(a.publicKeyHash)).to.equal(false);
			});

			await pendingSubscribe;
		} finally {
			await session.stop();
		}
	});
});
