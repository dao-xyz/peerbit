import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub } from "../src/index.js";

function deferred<T = void>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: any) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
}

describe("Edge case: subscribe then unsubscribe inside debounce window", function () {
	this.timeout(60_000);

	it("does not advertise or retain topic if unsubscribe happens before debounced subscribe executes", async () => {
		const TOPIC = "subscribe-then-unsubscribe-before-debounce";

		const session = await TestSession.disconnected<{ pubsub: DirectSub }>(2, {
			services: {
				pubsub: (c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						connectionManager: false,
					}),
			},
		});

		try {
			const a = session.peers[0].services.pubsub;
			const b = session.peers[1].services.pubsub;

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			// Block A._subscribe so A never reaches "subscriptions set" state,
			// making this test specifically about pending-subscribe cancellation.
			const gate = deferred<void>();
			const aAny = a as any;
			expect(aAny._subscribe).to.be.a("function");
			const originalSubscribeImpl = aAny._subscribe.bind(aAny);
			aAny._subscribe = async (...args: any[]) => {
				await gate.promise;
				return originalSubscribeImpl(...args);
			};

			// Start subscribe (pending) but do not await it
			const aSubscribe = a.subscribe(TOPIC).catch(() => {
				// If unsubscribe cancels in a way that rejects the subscribe promise,
				// we don't want an unhandled rejection to fail the test.
			});

			// Immediately unsubscribe
			await a.unsubscribe(TOPIC);

			// Now B subscribes; if A still advertises "pending subscribe",
			// B may incorrectly record A as a subscriber.
			await b.subscribe(TOPIC);

			// Wait a bit for any requestSubscribers/Subscribe traffic to settle
			await delay(500);

			const bTopicMap = b.topics.get(TOPIC);
			if (bTopicMap) {
				expect(
					bTopicMap.has(a.publicKeyHash),
					"B should NOT record A as subscribed after A unsubscribed during debounce",
				).to.equal(false);
			}

			// Also assert A is not still tracking the topic (or at least not claiming it)
			// Depending on current semantics, you may want either:
			// - topics entry removed entirely, OR
			// - topics entry exists but empty and not advertised.
			//
			// This is the stricter expectation (recommended):
			await waitForResolved(() => {
				expect(a.topics.has(TOPIC)).to.equal(false);
			});

			// Cleanup: release gate and give a tick so any queued work can finish
			gate.resolve();
			await delay(50);
			await aSubscribe;
		} finally {
			await session.stop();
		}
	});
});

