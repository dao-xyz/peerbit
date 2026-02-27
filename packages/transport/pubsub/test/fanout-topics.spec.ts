import { getPublicKeyFromPeerId, randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	PubSubData,
	PubSubMessage,
	Subscribe,
	type DataEvent as PubSubDataEvent,
} from "@peerbit/pubsub-interface";
import { SilentDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

describe("pubsub (fanout topics)", function () {
	const createSession = async (
		peerCount: number,
		options?: {
			pubsub?: Partial<ConstructorParameters<typeof TopicControlPlane>[1]>;
		},
	) => {
		const DEFAULT_SHARD_COUNT = 16;
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

		const session: TestSession<{ pubsub: TopicControlPlane; fanout: FanoutTree }> =
			await TestSession.connected(peerCount, {
				services: {
					fanout: (c) => getOrCreateFanout(c),
						pubsub: (c) =>
							new TopicControlPlane(c, {
								canRelayMessage: true,
								connectionManager: false,
								topicRootControlPlane,
								fanout: getOrCreateFanout(c),
								shardCount: DEFAULT_SHARD_COUNT,
								// Make join tests fast/deterministic.
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

		const configureBootstraps = (trackerIndices: number[]) => {
			const addrs: any[] = [];
			for (const i of trackerIndices) {
				for (const a of session.peers[i]!.getMultiaddrs()) addrs.push(a);
			}
			for (const peer of session.peers) {
				const self = new Set(peer.getMultiaddrs().map((a) => a.toString()));
				const filtered = addrs.filter((a) => !self.has(a.toString()));
				peer.services.fanout.setBootstraps(filtered);
				}
			};

		const configureShards = async (routerIndices: number[]) => {
			const candidates = routerIndices.map(
				(i) => session.peers[i]!.services.pubsub.publicKeyHash,
			);
			topicRootControlPlane.setTopicRootCandidates(candidates);
			await Promise.all(
				routerIndices.map((i) =>
					session.peers[i]!.services.pubsub.hostShardRootsNow(),
				),
			);
		};

		return {
			session,
			topicRootControlPlane,
			configureBootstraps,
			configureShards,
			shardCount: DEFAULT_SHARD_COUNT,
			shardTopicPrefix: "/peerbit/pubsub-shard/1/",
		};
	};

	const topicHash32 = (topic: string) => {
		let hash = 0x811c9dc5; // FNV-1a
		for (let index = 0; index < topic.length; index++) {
			hash ^= topic.charCodeAt(index);
			hash = (hash * 0x01000193) >>> 0;
		}
		return hash >>> 0;
	};

	it("delivers over sharded fanout (no direct subscription gossip)", async () => {
		const { session, configureBootstraps, configureShards } = await createSession(4);

		try {
			const TOPIC = "fanout-backed-topic";
			await configureShards([0]);

			// Configure trackers/bootstraps for fanout join without self-dialing.
			configureBootstraps([1, 2]);

			// Track any subscription gossip (should be zero for the fanout-backed topic).
			const subscribesByPeer = new Array<number>(session.peers.length).fill(0);
			for (const [i, peer] of session.peers.entries()) {
				const pubsub = peer.services.pubsub;
				const onDataMessage = pubsub.onDataMessage.bind(pubsub);
				pubsub.onDataMessage = async (f, s, message, seenBefore) => {
					try {
						if (message.data) {
							const decoded = PubSubMessage.from(message.data);
							if (decoded instanceof Subscribe && decoded.topics.includes(TOPIC)) {
								subscribesByPeer[i] += 1;
							}
						}
					} catch {
						// ignore non-pubsub frames
					}
					return onDataMessage(f, s, message, seenBefore);
				};
			}

			// Subscribe a subset.
			for (const peer of session.peers.slice(0, 3)) {
				await peer.services.pubsub.subscribe(TOPIC);
			}

			const receivedByPeer: Uint8Array[][] = session.peers.map(
				(): Uint8Array[] => [],
			);
			for (const [i, peer] of session.peers.entries()) {
				peer.services.pubsub.addEventListener("data", (ev: any) => {
					const detail = ev.detail as PubSubDataEvent;
					if (!detail?.data?.topics?.includes?.(TOPIC)) return;
					receivedByPeer[i]!.push(detail.data.data);
				});
			}

			const payload = new Uint8Array([1, 2, 3, 4]);
			await session.peers[2].services.pubsub.publish(payload, { topics: [TOPIC] });

			await waitForResolved(() => {
				for (const [i, received] of receivedByPeer.entries()) {
					// Match DirectStream behaviour: don't dispatch self-signed messages.
					if (i === 2) {
						expect(received.length).to.equal(0);
						continue;
					}
					// Peer 3 is not subscribed, should not receive.
					if (i === 3) {
						expect(received.length).to.equal(0);
						continue;
					}
					expect(received.length).to.equal(1);
					expect([...received[0]!]).to.deep.equal([...payload]);
				}
			});

			// Assert: no pubsub subscription gossip was used for this topic.
			for (const count of subscribesByPeer) {
				expect(count).to.equal(0);
			}
		} finally {
			await session.stop();
		}
	});

	it("exposes unified route hints from directstream and fanout", async () => {
		const { session, configureBootstraps, configureShards } = await createSession(2);

		try {
			const TOPIC = "fanout-route-hints-topic";
			await configureShards([0]);
			configureBootstraps([0]);

			await session.peers[0]!.services.pubsub.subscribe(TOPIC);
			await session.peers[1]!.services.pubsub.subscribe(TOPIC);

			const targetHash = session.peers[1]!.services.pubsub.publicKeyHash;
			await waitForResolved(() => {
				const hints = session.peers[0]!.services.pubsub.getUnifiedRouteHints!(
					TOPIC,
					targetHash,
				);
				expect(hints.some((h) => h.kind === "directstream-ack")).to.equal(true);
				expect(hints.some((h) => h.kind === "fanout-token")).to.equal(true);
			});
		} finally {
			await session.stop();
		}
	});

	it("preserves publish id/priority/signatures for fanout-backed topics", async () => {
		const { session, configureBootstraps, configureShards } = await createSession(3);

		try {
			const TOPIC = "fanout-signed-topic";
			await configureShards([0]);
			configureBootstraps([1, 2]);

			for (const peer of session.peers) {
				await peer.services.pubsub.subscribe(TOPIC);
			}

			const received: PubSubDataEvent[] = [];
			const receiver = session.peers[1]!.services.pubsub;
			receiver.addEventListener("data", (ev: any) => {
				const detail = ev.detail as PubSubDataEvent;
				if (!detail?.data?.topics?.includes?.(TOPIC)) return;
				received.push(detail);
			});

			const publisher = session.peers[2]!.services.pubsub;
			const payload = new Uint8Array([9, 8, 7, 6]);
			const wantedId = randomBytes(32);
			const priority = 123;
			const returnedId = await publisher.publish(payload, {
				topics: [TOPIC],
				id: wantedId,
				priority,
			});
			expect(returnedId).to.exist;
			expect([...returnedId!]).to.deep.equal([...wantedId]);

			await waitForResolved(() => expect(received).to.have.length(1));

			const ev = received[0]!;
			expect([...ev.message.id]).to.deep.equal([...wantedId]);
			expect(ev.message.header.priority).to.equal(priority);
			expect(await ev.message.verify(true)).to.equal(true);
			expect(
				ev.message.header.signatures?.publicKeys[0]?.equals(publisher.publicKey),
			).to.equal(true);
		} finally {
			await session.stop();
		}
	});

	it("can publish on fanout topics without subscribing (ephemeral join auto-closes)", async () => {
		const fanoutPublishIdleCloseMs = 2_000;
		const {
			session,
			configureBootstraps,
			configureShards,
			shardCount,
			shardTopicPrefix,
		} = await createSession(4, {
				pubsub: {
					fanoutPublishIdleCloseMs,
				} as any,
			});

		try {
			const TOPIC = "fanout-ephemeral-publish-topic";
			await configureShards([0]);
			configureBootstraps([1, 2]);

			// Subscribe a subset (publisher stays unsubscribed).
			await session.peers[0]!.services.pubsub.subscribe(TOPIC);
			await session.peers[1]!.services.pubsub.subscribe(TOPIC);
			await session.peers[2]!.services.pubsub.subscribe(TOPIC);

			const received: Uint8Array[] = [];
			const receiver = session.peers[1]!.services.pubsub;
			receiver.addEventListener("data", (ev: any) => {
				const detail = ev.detail as PubSubDataEvent;
				if (!detail?.data?.topics?.includes?.(TOPIC)) return;
				received.push(detail.data.data);
			});

			const publisher = session.peers[3]!.services.pubsub;
			const payload = new Uint8Array([1, 2, 3]);
			await publisher.publish(payload, { topics: [TOPIC] });

			await waitForResolved(() => expect(received).to.have.length(1));
			expect([...received[0]!]).to.deep.equal([...payload]);

			const shardIndex = topicHash32(TOPIC) % shardCount;
			const shardTopic = `${shardTopicPrefix}${shardIndex}`;

			// Publisher is not subscribed, but should have an ephemeral shard channel right after publish.
			await waitForResolved(() => {
				expect(Boolean((publisher as any).fanoutChannels?.get(shardTopic))).to.equal(
					true,
				);
			});

			// After idle timeout, the ephemeral join should be closed.
			await waitForResolved(() => {
				expect(Boolean((publisher as any).fanoutChannels?.get(shardTopic))).to.equal(
					false,
				);
			});
		} finally {
			await session.stop();
		}
	});

	it("de-dups delivery when publishing to multiple topics across shards", async () => {
		const { session, configureBootstraps, configureShards, shardCount } =
			await createSession(4);

		try {
			await configureShards([0]);
			configureBootstraps([2, 3]);

			// Pick two topics that map to different shard indices.
			let TOPIC_A = "fanout-topic-a";
			let TOPIC_B = "fanout-topic-b";
			let a = topicHash32(TOPIC_A) % shardCount;
			let b = topicHash32(TOPIC_B) % shardCount;
			let k = 0;
			while (a === b && k < 1000) {
				k += 1;
				TOPIC_A = `fanout-topic-a-${k}`;
				TOPIC_B = `fanout-topic-b-${k}`;
				a = topicHash32(TOPIC_A) % shardCount;
				b = topicHash32(TOPIC_B) % shardCount;
			}
			expect(a === b).to.equal(false);

			for (const peer of session.peers) {
				await peer.services.pubsub.subscribe(TOPIC_A);
				await peer.services.pubsub.subscribe(TOPIC_B);
			}

			const received: PubSubDataEvent[] = [];
			const receiver = session.peers[2]!.services.pubsub;
			receiver.addEventListener("data", (ev: any) => {
				const detail = ev.detail as PubSubDataEvent;
				const topics = detail?.data?.topics || [];
				if (!topics.includes(TOPIC_A) && !topics.includes(TOPIC_B)) return;
				received.push(detail);
			});

			const publisher = session.peers[3]!.services.pubsub;
			const payload = new Uint8Array([4, 5, 6]);
			await publisher.publish(payload, { topics: [TOPIC_A, TOPIC_B] });

			await waitForResolved(() => expect(received).to.have.length(1));
			expect([...received[0]!.data.data]).to.deep.equal([...payload]);
			expect(received[0]!.data.topics.sort()).to.deep.equal(
				[TOPIC_A, TOPIC_B].sort(),
			);
		} finally {
			await session.stop();
		}
	});

	it("tracks pending subscribe immediately and cleans up if cancelled before debounce", async () => {
		const { session } = await createSession(1, {
			pubsub: {
				subscriptionDebounceDelay: 500,
			},
		});

		try {
			const topic = "pending-subscribe-topic";
			const pubsub = session.peers[0]!.services.pubsub as any;

			const subscribePromise = pubsub.subscribe(topic);
			expect(pubsub.topics.has(topic)).to.equal(true);
			expect(pubsub.subscriptions.has(topic)).to.equal(false);
			expect(pubsub.pendingSubscriptions.has(topic)).to.equal(true);
			expect(pubsub.getSubscriptionOverlap([topic])).to.deep.equal([topic]);

			const removed = await pubsub.unsubscribe(topic);
			expect(removed).to.equal(false);
			expect(pubsub.pendingSubscriptions.has(topic)).to.equal(false);
			expect(pubsub.subscriptions.has(topic)).to.equal(false);
			expect(pubsub.topics.has(topic)).to.equal(false);

			await subscribePromise;
		} finally {
			await session.stop();
		}
	});

	it("accepts strict direct delivery while subscribe is pending", async () => {
		const { session } = await createSession(2, {
			pubsub: {
				subscriptionDebounceDelay: 500,
			},
		});

		try {
			const topic = "pending-strict-delivery-topic";
			const sender = session.peers[0]!.services.pubsub;
			const receiver = session.peers[1]!.services.pubsub;
			const received: Uint8Array[] = [];

			receiver.addEventListener("data", (ev: any) => {
				const detail = ev.detail as PubSubDataEvent;
				if (!detail?.data?.topics?.includes?.(topic)) return;
				received.push(detail.data.data);
			});

			const pendingSubscribe = receiver.subscribe(topic);
			const payload = new Uint8Array([7, 9, 11, 13]);
			const strictMessage = await (sender as any).createMessage(
				new PubSubData({ topics: [topic], data: payload, strict: true }).bytes(),
				{
					mode: new SilentDelivery({
						to: [receiver.publicKeyHash],
						redundancy: 1,
					}),
					skipRecipientValidation: true,
				},
			);
			await receiver.onDataMessage(
				sender.publicKey,
				{} as any,
				strictMessage,
				0,
			);

			await waitForResolved(() => {
				expect(received).to.have.length(1);
				expect([...received[0]!]).to.deep.equal([...payload]);
			});

			await pendingSubscribe;
		} finally {
			await session.stop();
		}
	});
});
