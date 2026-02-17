import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	PubSubMessage,
	Subscribe,
	type DataEvent as PubSubDataEvent,
} from "@peerbit/pubsub-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "../src/index.js";

describe("pubsub (fanout topics)", function () {
	it("delivers without subscription gossip for fanout-backed topics", async () => {
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
			await TestSession.connected(4, {
				services: {
					fanout: (c) => getOrCreateFanout(c),
					pubsub: (c) =>
						new TopicControlPlane(c, {
							canRelayMessage: true,
							connectionManager: false,
							topicRootControlPlane,
							fanout: getOrCreateFanout(c),
							// Make join tests fast/deterministic.
							fanoutJoin: {
								timeoutMs: 10_000,
								retryMs: 50,
								bootstrapEnsureIntervalMs: 200,
								trackerQueryIntervalMs: 200,
								joinReqTimeoutMs: 1_000,
								trackerQueryTimeoutMs: 1_000,
							},
						}),
				},
			});

		try {
			const TOPIC = "fanout-backed-topic";
			const rootHash = session.peers[0].services.pubsub.publicKeyHash;
			topicRootControlPlane.setTopicRoot(TOPIC, rootHash);

			// Configure trackers/bootstraps for fanout join without self-dialing.
			const trackerA = session.peers[1];
			const trackerB = session.peers[2];
			const bootstrapAddrs = [
				...trackerA.getMultiaddrs(),
				...trackerB.getMultiaddrs(),
			];
			for (const peer of session.peers) {
				const self = new Set(peer.getMultiaddrs().map((a) => a.toString()));
				const filtered = bootstrapAddrs.filter((a) => !self.has(a.toString()));
				peer.services.fanout.setBootstraps(filtered);
			}

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

			// Subscribe all peers (subscribe = join overlay).
			for (const peer of session.peers) {
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
			await session.peers[3].services.pubsub.publish(payload, { topics: [TOPIC] });

			await waitForResolved(() => {
				for (const received of receivedByPeer) {
					expect(received.length).to.equal(1);
					expect([...received[0]!]).to.deep.equal([...payload]);
				}
			});

			// Assert: no pubsub subscription gossip was used for this topic.
			for (const count of subscribesByPeer) {
				expect(count).to.equal(0);
			}

			// Sanity: `getSubscribers()` returns a bounded overlay-local view.
			for (const peer of session.peers) {
				const subs = peer.services.pubsub.getSubscribers(TOPIC);
				expect(subs && subs.length >= 1).to.equal(true);
			}
		} finally {
			await session.stop();
		}
	});
});
