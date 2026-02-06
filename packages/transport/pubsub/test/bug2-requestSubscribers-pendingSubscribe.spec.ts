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

describe("BUG 2: pending subscribe should be visible via requestSubscribers", function () {
	this.timeout(60_000);

	// Skip: DirectSub.requestSubscribers does not yet include pending subscribes.
	// This test documents the gap as a design probe -- when pending subscribes are
	// included in requestSubscribers responses, this test should be unskipped.
	it.skip("peer discovers remote subscription while remote _subscribe() is blocked (pending subscribe advertised)", async () => {
		const TOPIC = "pending-subscribe-advertised";

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

			// Connect first (so any handshake/requestSubscribers traffic can happen)
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			// Block A._subscribe() so that:
			// - A has called subscribe(TOPIC) (so debounce aggregator has it)
			// - but A never reaches the point where it sets `subscriptions`
			// This isolates the "pending subscribe counts" logic.
			const gate = deferred<void>();
			const aAny = a as any;

			expect(aAny._subscribe, "Expected DirectSub to have a _subscribe() method")
				.to.be.a("function");

			const originalSubscribeImpl = aAny._subscribe.bind(aAny);
			aAny._subscribe = async (...args: any[]) => {
				await gate.promise;
				return originalSubscribeImpl(...args);
			};

			let aSubscribeResolved = false;
			const aSubscribePromise = a.subscribe(TOPIC).then(() => {
				aSubscribeResolved = true;
			});

			// Now subscribe normally on B
			await b.subscribe(TOPIC);

			// Key assertion:
			// B should learn that A is subscribed *even though* A's subscribe hasn't resolved yet.
			// Without the "pending subscribe" inclusion in requestSubscribers, B has no basis to learn A.
			await waitForResolved(() => {
				expect(aSubscribeResolved, "A.subscribe should still be pending").to.be
					.false;

				const bTopicMap = b.topics.get(TOPIC);
				expect(bTopicMap).to.not.be.undefined;
				expect(
					bTopicMap!.has(a.publicKeyHash),
					"B should record A as a subscriber while A is pending",
				).to.be.true;
			});

			// Cleanup: release A and let subscribe resolve, so we don't leave dangling work.
			gate.resolve();
			await aSubscribePromise;
		} finally {
			await session.stop();
		}
	});

	it("a node that did NOT subscribe does NOT start tracking a topic just because it receives Subscribe traffic (design guard)", async () => {
		const TOPIC = "non-subscriber-should-not-track";

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
			const a = session.peers[0].services.pubsub; // will NOT subscribe
			const b = session.peers[1].services.pubsub; // WILL subscribe

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			await b.subscribe(TOPIC);

			// Give a moment for any Subscribe traffic to be exchanged
			await delay(250);

			// If we ever re-introduce "initializeTopic on incoming Subscribe",
			// this would start failing.
			expect(a.topics.has(TOPIC)).to.equal(false);
			expect(a.topics.get(TOPIC)).to.equal(undefined);
		} finally {
			await session.stop();
		}
	});
});

