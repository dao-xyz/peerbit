/**
 * BUG 1: initializeTopic race condition in Subscribe handler
 *
 * When a remote peer sends a Subscribe message for a topic that the local
 * node hasn't initialized (hasn't subscribed to or called requestSubscribers),
 * the subscription is silently dropped because `this.topics.get(topic)` returns
 * null and the forEach body returns early.
 *
 * Impact: relay nodes that don't subscribe to a topic fail to track who is
 * subscribed, breaking message routing through them.
 *
 * Fix: call this.initializeTopic(topic) before this.topics.get(topic)
 * in the Subscribe message handler.
 */
import { TestSession } from "@peerbit/libp2p-test-utils";
import type { UnsubcriptionEvent } from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub, waitForSubscribers } from "../src/index.js";

describe("BUG 1: initializeTopic race in Subscribe handler", function () {
	this.timeout(60_000);

	describe("2-peer scenario: receiver has not initialized topic", () => {
		let session: TestSession<{ pubsub: DirectSub }>;

		beforeEach(async () => {
			session = await TestSession.disconnected(2, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false,
						}),
				},
			});
		});

		afterEach(async () => {
			await session.stop();
		});

		it("should track remote peer subscription even if local node hasn't initialized the topic", async () => {
			const TOPIC = "race-test-topic";

			// Only peer 0 subscribes. Peer 1 does NOT subscribe and does NOT
			// call requestSubscribers – so the topic is never initialized on peer 1.
			await session.peers[0].services.pubsub.subscribe(TOPIC);

			// Now connect them. onPeerReachable on peer 0 will send a Subscribe
			// message with requestSubscribers:true to peer 1.
			await session.connect([
				[session.peers[0], session.peers[1]],
			]);

			await waitForNeighbour(
				session.peers[0].services.pubsub,
				session.peers[1].services.pubsub,
			);

			// Give plenty of time for the Subscribe message to be processed
			await delay(3000);

			// BUG: peer 1 should have peer 0 registered as a subscriber for TOPIC.
			// Without the fix, this.topics.get(TOPIC) returns undefined on peer 1
			// because initializeTopic was never called, so the Subscribe handler
			// returned early and the subscription was silently dropped.
			const topicPeers = session.peers[1].services.pubsub.topics.get(TOPIC);

			// This assertion fails WITHOUT the fix (topicPeers is undefined)
			expect(topicPeers).to.not.be.undefined;
			expect(topicPeers!.has(session.peers[0].services.pubsub.publicKeyHash)).to
				.be.true;
		});
	});

	describe("3-peer relay scenario: relay doesn't subscribe but must track subscriptions", () => {
		let session: TestSession<{ pubsub: DirectSub }>;

		beforeEach(async () => {
			session = await TestSession.disconnected(3, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false,
						}),
				},
			});
		});

		afterEach(async () => {
			await session.stop();
		});

		it("relay node should learn about subscribers even without calling requestSubscribers", async () => {
			const TOPIC = "relay-race-topic";

			/*
			 * Topology: 0 ↔ 1(relay) ↔ 2
			 *
			 * Peers 0 and 2 subscribe to TOPIC.
			 * Peer 1 is a relay – it does NOT subscribe and does NOT call requestSubscribers.
			 *
			 * Without the fix: peer 1 drops Subscribe messages for TOPIC because
			 * the topic isn't in its topics Map. As a result, peer 1 can't route
			 * messages for TOPIC between peers 0 and 2.
			 */

			// Both end-nodes subscribe BEFORE connecting
			await session.peers[0].services.pubsub.subscribe(TOPIC);
			await session.peers[2].services.pubsub.subscribe(TOPIC);
			// Peer 1 does NOT subscribe – it's purely a relay

			await delay(500);

			// Connect in a line: 0 ↔ 1 ↔ 2
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			await waitForNeighbour(
				session.peers[0].services.pubsub,
				session.peers[1].services.pubsub,
			);
			await waitForNeighbour(
				session.peers[1].services.pubsub,
				session.peers[2].services.pubsub,
			);

			// Wait for subscription messages to propagate
			await delay(5000);

			// BUG CHECK: Relay (peer 1) should know about both subscribers
			const relayTopicPeers =
				session.peers[1].services.pubsub.topics.get(TOPIC);

			// Without fix: relayTopicPeers is undefined (topic never initialized on relay)
			expect(relayTopicPeers).to.not.be.undefined;

			const peer0Hash = session.peers[0].services.pubsub.publicKeyHash;
			const peer2Hash = session.peers[2].services.pubsub.publicKeyHash;

			// The relay should track both endpoints as subscribers
			expect(relayTopicPeers!.has(peer0Hash)).to.be.true;
			expect(relayTopicPeers!.has(peer2Hash)).to.be.true;
		});
	});
});
