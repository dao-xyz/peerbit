import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitForNeighbour } from "@peerbit/stream";
import { SeekDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { DirectSub } from "../src/index.js";

const deferred = <T = void>() => {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: any) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
};

describe("BUG 4: pending subscribe receives strict PubSubData", function () {
	this.timeout(20_000);

	it("delivers strict messages to a peer while subscribe() is still in the debounce window", async () => {
		const TOPIC = "pending-subscribe-receives-strict";

		const session = (await TestSession.disconnected(2, {
			services: {
				pubsub: (c) =>
					new DirectSub(c, {
						canRelayMessage: true,
						connectionManager: false,
					}),
			},
		})) as TestSession<{ pubsub: DirectSub }>;

		try {
			const a = session.peers[0].services.pubsub;
			const b = session.peers[1].services.pubsub;

			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForNeighbour(a, b);

			// Block A._subscribe() so that A remains "pending" (no `subscriptions` entry).
			const gate = deferred<void>();
			const aAny = a as any;
			const originalSubscribeImpl = aAny._subscribe.bind(aAny);
			aAny._subscribe = async (...args: any[]) => {
				await gate.promise;
				return originalSubscribeImpl(...args);
			};

			let received = false;
			const payload = new Uint8Array([1, 2, 3, 4]);

			const onData = (e: any) => {
				if (!e?.detail?.data) {
					return;
				}
				if (e.detail.data.topics?.includes(TOPIC)) {
					received = true;
				}
			};

			a.addEventListener("data", onData);
			try {
				// Start subscribe (pending), but keep it blocked in the debounce window.
				const aSubscribe = a.subscribe(TOPIC);

				// Publish a strict message to A. If pending subscribes are not treated as
				// local interest, A will incorrectly ignore this message.
				await b.publish(payload, {
					topics: [TOPIC],
					mode: new SeekDelivery({ redundancy: 1, to: [a.publicKeyHash] }),
				});

				await waitForResolved(() => {
					expect(received).to.equal(true);
				}, { timeout: 5_000 });

				// Cleanup
				gate.resolve();
				await aSubscribe;
			} finally {
				a.removeEventListener("data", onData);
			}
		} finally {
			await session.stop();
		}
	});
});
