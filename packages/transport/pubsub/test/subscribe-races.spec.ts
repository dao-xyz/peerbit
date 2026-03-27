import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

describe("pubsub (subscribe race regressions)", function () {
	let session:
		| TestSession<{
				pubsub: TopicControlPlane;
				fanout: FanoutTree;
		  }>
		| undefined;

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

	const createDisconnectedSessionWithPerPeerRoots = async (
		peerCount: number,
		options?: {
			pubsub?: Partial<ConstructorParameters<typeof TopicControlPlane>[1]>;
		},
	) => {
		const perPeer = new Map<
			string,
			{ fanout: FanoutTree; topicRootControlPlane: TopicRootControlPlane }
		>();
		const getOrCreatePerPeer = (c: any) => {
			const hash = getPublicKeyFromPeerId(c.peerId).hashcode();
			let existing = perPeer.get(hash);
			if (!existing) {
				const topicRootControlPlane = new TopicRootControlPlane();
				const fanout = new FanoutTree(c, {
					connectionManager: false,
					topicRootControlPlane,
				});
				existing = { fanout, topicRootControlPlane };
				perPeer.set(hash, existing);
			}
			return existing;
		};

		return TestSession.disconnected<{
			pubsub: TopicControlPlane;
			fanout: FanoutTree;
		}>(peerCount, {
			services: {
				fanout: (c: any) => getOrCreatePerPeer(c).fanout,
				pubsub: (c: any) => {
					const { fanout, topicRootControlPlane } = getOrCreatePerPeer(c);
					return new TopicControlPlane(c, {
						canRelayMessage: true,
						connectionManager: false,
						topicRootControlPlane,
						fanout,
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
					});
				},
			},
		});
	};

	const topicHash32 = (topic: string) => {
		let hash = 0x811c9dc5; // FNV-1a
		for (let index = 0; index < topic.length; index++) {
			hash ^= topic.charCodeAt(index);
			hash = (hash * 0x01000193) >>> 0;
		}
		return hash >>> 0;
	};

	afterEach(async () => {
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("discovers peers when subscribe and connect happen concurrently", async () => {
		const TOPIC = "concurrent-subscribe-connect-regression";
		session = await createDisconnectedSession(2);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await Promise.all([
			a.subscribe(TOPIC),
			b.subscribe(TOPIC),
			session.connect([[session.peers[0], session.peers[1]]]),
		]);
		await waitForNeighbour(a, b);

		await waitForResolved(() => {
			const aTopics = a.topics.get(TOPIC);
			const bTopics = b.topics.get(TOPIC);
			expect(aTopics).to.not.equal(undefined);
			expect(bTopics).to.not.equal(undefined);
			expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
			expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
		});
	});

	it("does not track a topic on a peer that never subscribed", async () => {
		const TOPIC = "non-subscriber-should-not-track-regression";
		session = await createDisconnectedSession(2);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(a, b);

		await b.subscribe(TOPIC);
		await waitForResolved(() => {
			expect(b.subscriptions.has(TOPIC)).to.equal(true);
			const bSubscribers = b.getSubscribers(TOPIC);
			expect(
				bSubscribers?.some((subscriber) => subscriber.hashcode() === b.publicKeyHash),
			).to.equal(true);
		});

		expect(a.topics.has(TOPIC)).to.equal(false);
		expect(a.topics.get(TOPIC)).to.equal(undefined);
	});

	it("does not advertise cancelled pending subscriptions to peers", async () => {
		const TOPIC = "subscribe-then-unsubscribe-before-debounce-regression";
		const debounceDelayMs = 500;
		session = await createDisconnectedSession(2, {
			pubsub: {
				subscriptionDebounceDelay: debounceDelayMs,
			},
		});

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(a, b);

		const pendingSubscribe = a.subscribe(TOPIC);
		const removed = await a.unsubscribe(TOPIC);
		expect(removed).to.equal(false);

		await b.subscribe(TOPIC);

		// Wait for A's debounced subscribe cycle to settle before asserting.
		// This validates that A does not get (stale) advertised at flush time.
		await pendingSubscribe;
		await delay(debounceDelayMs + 100);

		expect(a.topics.has(TOPIC)).to.equal(false);
		const bTopics = b.topics.get(TOPIC);
		expect(bTopics).to.not.equal(undefined);
		expect(bTopics!.has(a.publicKeyHash)).to.equal(false);
	});

	it("converges sparse relay topology without forced shard-root candidates", async function () {
		this.timeout(120_000);

		const TOPIC = "sparse-relay-root-candidate-convergence";
		session = await createDisconnectedSessionWithPerPeerRoots(3);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[2]!.getMultiaddrs()[0]!);
		await session.peers[1]!.dial(session.peers[2]!.getMultiaddrs()[0]!);

		await waitForNeighbour(a, relay);
		await waitForNeighbour(b, relay);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		await waitForResolved(
			async () => {
				const candidateLists = [a, b, relay].map((peer) =>
					peer.topicRootControlPlane.getTopicRootCandidates().join(","),
				);
				expect(
					new Set(candidateLists).size,
					`candidate lists: ${candidateLists.join(" | ")}`,
				).to.equal(1);

				const roots = await Promise.all(
					[a, b, relay].map((peer) =>
						peer.topicRootControlPlane.resolveTopicRoot(shardTopic),
					),
				);
				expect(new Set(roots).size, `roots: ${roots.join(",")}`).to.equal(1);
			},
			{ timeout: 20_000, delayInterval: 100 },
		);

		const resolvedRoot = await a.topicRootControlPlane.resolveTopicRoot(shardTopic);
		expect(resolvedRoot).to.be.a("string");
		const rootPeer = [a, b, relay].find((peer) => peer.publicKeyHash === resolvedRoot);
		expect(rootPeer, `resolved root ${resolvedRoot}`).to.exist;

		await waitForResolved(
			() => {
				const rootChannel = (rootPeer as any).fanoutChannels?.get?.(shardTopic);
				expect(rootChannel, `expected root ${resolvedRoot} to host ${shardTopic}`).to
					.exist;
				expect(rootChannel.root).to.equal(resolvedRoot);
			},
			{ timeout: 20_000, delayInterval: 100 },
		);
	});

	it("resolves shard roots through a dialed gateway without explicit candidates", async function () {
		this.timeout(120_000);

		const TOPIC = "dial-gateway-root-tracker-discovery";
		session = await createDisconnectedSessionWithPerPeerRoots(3);

		const a = session.peers[0]!.services.pubsub;
		const gateway = session.peers[1]!.services.pubsub;
		const root = session.peers[2]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[1]!.getMultiaddrs()[0]!);
		await session.peers[1]!.dial(session.peers[2]!.getMultiaddrs()[0]!);

		await waitForNeighbour(a, gateway);
		await waitForNeighbour(gateway, root);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		a.setTopicRootCandidates([]);
		gateway.setTopicRootCandidates([root.publicKeyHash]);
		root.setTopicRootCandidates([root.publicKeyHash]);

		const resolvedRoot = await (a as any).resolveShardRoot(shardTopic);
		expect(resolvedRoot).to.equal(root.publicKeyHash);
	});

	it("hosts a shard before answering a direct root query for itself", async function () {
		this.timeout(120_000);

		const TOPIC = "dial-query-opens-missing-shard-root";
		session = await createDisconnectedSessionWithPerPeerRoots(2);

		const leaf = session.peers[0]!.services.pubsub;
		const root = session.peers[1]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[1]!.getMultiaddrs()[0]!);
		await waitForNeighbour(leaf, root);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		leaf.setTopicRootCandidates([]);
		root.setTopicRootCandidates([root.publicKeyHash]);

		await waitForResolved(
			() => {
				expect((root as any).fanoutChannels.get(shardTopic)).to.exist;
			},
			{ timeout: 20_000, delayInterval: 100 },
		);
		await (root as any).closeFanoutChannel(shardTopic, { force: true });
		expect((root as any).fanoutChannels.get(shardTopic)).to.not.exist;

		const resolvedRoot = await (leaf as any).resolveShardRoot(shardTopic);
		expect(resolvedRoot).to.equal(root.publicKeyHash);

		await waitForResolved(
			() => {
				const rootChannel = (root as any).fanoutChannels.get(shardTopic);
				expect(rootChannel, `expected root to host ${shardTopic}`).to.exist;
				expect(rootChannel.root).to.equal(root.publicKeyHash);
			},
			{ timeout: 20_000, delayInterval: 100 },
		);

		await leaf.subscribe(TOPIC);
	});
});
