/**
 * Regression: publishToChannelMaybe() throws "Channel not open"
 *
 * dontThrowIfDeliveryError() only recognises DeliveryError, TimeoutError, and
 * AbortError — it re-throws the "Channel not open" Error. This surfaces as an
 * unhandled promise rejection during teardown.
 *
 * The fix catches "Channel not open:" before calling dontThrowIfDeliveryError.
 */

import { TestSession } from "@peerbit/libp2p-test-utils";
import { expect } from "chai";
import { FanoutTree } from "../src/index.js";

type FanoutServices = { fanout: FanoutTree };

const createFanoutService = (components: any) =>
	new FanoutTree(components, { connectionManager: false });

const createFanoutTestSession = (n: number) =>
	TestSession.disconnected<FanoutServices>(n, {
		services: {
			fanout: createFanoutService,
		},
	});

describe("@peerbit/pubsub — Channel not open during shutdown", () => {
	it("publishToChannelMaybe() returns false after channel is closed (direct)", async () => {
		const session = await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "shutdown-repro";
			const root = fanout.publicKeyHash;

			fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 4,
				repair: false,
			});

			const payload = new Uint8Array([1, 2, 3]);
			const okResult = await fanout.publishToChannelMaybe(
				topic,
				root,
				payload,
			);
			expect(okResult).to.equal(true);

			fanout.closeChannel(topic, root);

			// On unpatched code this throws "Channel not open"
			const result = await fanout.publishToChannelMaybe(
				topic,
				root,
				payload,
			);
			expect(result).to.equal(false);
		} finally {
			await session.stop();
		}
	});

	it("publishToChannel() still throws for callers who want the error", async () => {
		const session = await createFanoutTestSession(1);

		try {
			const fanout = session.peers[0].services.fanout;
			const topic = "throw-check";
			const root = fanout.publicKeyHash;

			fanout.openChannel(topic, root, {
				role: "root",
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 4,
				repair: false,
			});

			fanout.closeChannel(topic, root);

			try {
				await fanout.publishToChannel(topic, root, new Uint8Array([1]));
				expect.fail("Expected publishToChannel to throw after channel close");
			} catch (err: any) {
				expect(err).to.be.instanceOf(Error);
				expect(err.message).to.match(/^Channel not open:/);
			}
		} finally {
			await session.stop();
		}
	});
});
