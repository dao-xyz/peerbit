/**
 * BUG 1: initializeTopic race condition — topic not ready when Subscribe arrives
 *
 * `subscribe()` debounces via `debounceSubscribeAggregator`. The actual `_subscribe()`
 * handler (which calls `initializeTopic`) fires only after the debounce window. If a
 * remote Subscribe message arrives in that window, the handler finds
 * `this.topics.get(topic) === undefined` and silently drops the remote subscription.
 *
 * Fix: eagerly call `initializeTopic(topic)` inside `subscribe()` itself, so the
 * topic map exists before any debounce delay.
 */
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub } from "../src/index.js";

describe("BUG 1: initializeTopic race – eager topic init in subscribe()", function () {
	this.timeout(60_000);

	describe("unit: subscribe() eagerly initializes topic tracking", () => {
		let session: TestSession<{ pubsub: DirectSub }>;

		beforeEach(async () => {
			session = await TestSession.disconnected(1, {
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

		it("topics.has(topic) is true immediately after subscribe(), before debounce fires", async () => {
			const TOPIC = "eager-init-topic";
			const pubsub = session.peers[0].services.pubsub;

			// Before subscribe, topic should not exist
			expect(pubsub.topics.has(TOPIC)).to.be.false;

			// Call subscribe — this returns a promise from the debounce aggregator.
			// _subscribe() has NOT fired yet.
			const p = pubsub.subscribe(TOPIC);

			// Topic should be initialized IMMEDIATELY
			expect(pubsub.topics.has(TOPIC)).to.be.true;
			expect(pubsub.topics.get(TOPIC)).to.be.instanceOf(Map);

			await p;
		});
	});

	describe("integration: concurrent subscribe + connect", () => {
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

		it("peers discover each other when subscribing and connecting concurrently", async () => {
			const TOPIC = "concurrent-sub-topic";

			// Subscribe on both peers and connect at the same time.
			// Without the eager init, peer B's topic map may not exist when
			// peer A's Subscribe message arrives (debounce hasn't fired).
			await Promise.all([
				session.peers[0].services.pubsub.subscribe(TOPIC),
				session.peers[1].services.pubsub.subscribe(TOPIC),
				session.connect([[session.peers[0], session.peers[1]]]),
			]);

			// Both peers should eventually see each other
			await waitForResolved(() => {
				const peer0Topics = session.peers[0].services.pubsub.topics.get(TOPIC);
				const peer1Topics = session.peers[1].services.pubsub.topics.get(TOPIC);
				expect(peer0Topics).to.not.be.undefined;
				expect(peer1Topics).to.not.be.undefined;
				expect(
					peer0Topics!.has(session.peers[1].services.pubsub.publicKeyHash),
				).to.be.true;
				expect(
					peer1Topics!.has(session.peers[0].services.pubsub.publicKeyHash),
				).to.be.true;
			});
		});

		it("subscribe after connect still works (normal path)", async () => {
			const TOPIC = "post-connect-topic";

			// Connect first, then subscribe
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(
				session.peers[0].services.pubsub,
				session.peers[1].services.pubsub,
			);

			await session.peers[0].services.pubsub.subscribe(TOPIC);
			await session.peers[1].services.pubsub.subscribe(TOPIC);

			await waitForResolved(() => {
				const peer0Topics = session.peers[0].services.pubsub.topics.get(TOPIC);
				const peer1Topics = session.peers[1].services.pubsub.topics.get(TOPIC);
				expect(peer0Topics).to.not.be.undefined;
				expect(peer1Topics).to.not.be.undefined;
				expect(
					peer0Topics!.has(session.peers[1].services.pubsub.publicKeyHash),
				).to.be.true;
				expect(
					peer1Topics!.has(session.peers[0].services.pubsub.publicKeyHash),
				).to.be.true;
			});
		});
	});
});

