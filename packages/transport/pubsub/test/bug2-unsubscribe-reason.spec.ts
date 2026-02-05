/**
 * BUG 2: UnsubcriptionEvent lacks .reason property
 *
 * The upstream UnsubcriptionEvent is dispatched in two different scenarios:
 *   1. Peer becomes unreachable (removeSubscriptions → "peer-unreachable")
 *   2. Peer explicitly unsubscribes (Unsubscribe message → "remote-unsubscribe")
 *
 * However, the event.detail has NO .reason field, making it impossible for
 * consumers to distinguish WHY an unsubscription happened. This is important
 * for connection management UX (e.g., showing "peer went offline" vs
 * "peer left the topic").
 *
 * Fix: set event.reason = "peer-unreachable" | "remote-unsubscribe" on the
 * UnsubcriptionEvent before dispatching.
 */
import { TestSession } from "@peerbit/libp2p-test-utils";
import type { UnsubcriptionEvent } from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub, waitForSubscribers } from "../src/index.js";

describe("BUG 2: UnsubcriptionEvent missing .reason property", function () {
	this.timeout(60_000);

	describe("peer-unreachable (removeSubscriptions path)", () => {
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

		it("should include reason='peer-unreachable' when a peer disconnects", async () => {
			const TOPIC = "reason-test-unreachable";
			const unsubEvents: (UnsubcriptionEvent & { reason?: string })[] = [];

			// Both peers subscribe
			await session.peers[0].services.pubsub.subscribe(TOPIC);
			await session.peers[1].services.pubsub.subscribe(TOPIC);

			// Connect
			await session.connect([
				[session.peers[0], session.peers[1]],
			]);

			await waitForSubscribers(
				session.peers[0],
				[session.peers[1]],
				TOPIC,
			);
			await waitForSubscribers(
				session.peers[1],
				[session.peers[0]],
				TOPIC,
			);

			// Listen for unsubscribe events on peer 1
			session.peers[1].services.pubsub.addEventListener(
				"unsubscribe",
				(e) => {
					unsubEvents.push(e.detail as any);
				},
			);

			// Stop peer 0 – this triggers removeSubscriptions on peer 1
			await delay(2000);
			await session.peers[0].stop();

			// Wait for unsubscribe event
			await waitForResolved(
				() => expect(unsubEvents).to.have.length.greaterThanOrEqual(1),
				{ timeout: 30_000 },
			);

			// BUG: Without the fix, event.reason is undefined
			const event = unsubEvents[0];
			expect(event.from.equals(session.peers[0].services.pubsub.publicKey))
				.to.be.true;
			expect(event.topics).to.include(TOPIC);

			// This assertion FAILS without the patch
			expect((event as any).reason).to.equal("peer-unreachable");
		});
	});

	describe("remote-unsubscribe (Unsubscribe message path)", () => {
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

		it("should include reason='remote-unsubscribe' when a peer explicitly unsubscribes", async () => {
			const TOPIC = "reason-test-unsubscribe";
			const unsubEvents: (UnsubcriptionEvent & { reason?: string })[] = [];

			// Connect first
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);

			await waitForNeighbour(
				session.peers[0].services.pubsub,
				session.peers[1].services.pubsub,
			);

			// Peer 1 needs to track subscriptions
			await session.peers[1].services.pubsub.requestSubscribers(TOPIC);

			// Peer 0 subscribes
			await session.peers[0].services.pubsub.subscribe(TOPIC);

			// Wait for peer 1 to learn about peer 0's subscription
			await waitForResolved(
				() =>
					expect(
						session.peers[1].services.pubsub.topics
							.get(TOPIC)
							?.has(session.peers[0].services.pubsub.publicKeyHash),
					).to.be.true,
			);

			// Listen for unsubscribe events on peer 1
			session.peers[1].services.pubsub.addEventListener(
				"unsubscribe",
				(e) => {
					unsubEvents.push(e.detail as any);
				},
			);

			// Allow debouncing to settle
			await delay(3000);

			// Peer 0 explicitly unsubscribes (sends Unsubscribe message)
			await session.peers[0].services.pubsub.unsubscribe(TOPIC);

			// Wait for unsubscribe event on peer 1
			await waitForResolved(
				() => expect(unsubEvents).to.have.length.greaterThanOrEqual(1),
				{ timeout: 15_000 },
			);

			const event = unsubEvents[0];
			expect(event.from.equals(session.peers[0].services.pubsub.publicKey))
				.to.be.true;
			expect(event.topics).to.include(TOPIC);

			// This assertion FAILS without the patch
			expect((event as any).reason).to.equal("remote-unsubscribe");
		});
	});
});
