import { LSession } from "../session.js";
import { waitFor } from "@dao-xyz/peerbit-time";
import { logger } from "@dao-xyz/libp2p-direct-sub";
logger.level = "trace";
import { logger as logger2 } from "@dao-xyz/libp2p-direct-stream";

logger2.level = "trace";
describe("session", () => {
	let session: LSession;
	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	afterAll(async () => {
		await session.stop();
	});
	it("pubsub", async () => {
		let result: any = undefined;
		session.peers[0].services.pubsub.listenForSubscribers("x");
		session.peers[1].services.pubsub.listenForSubscribers("x");
		await session.peers[2].services.pubsub.subscribe("x");
		session.peers[2].services.pubsub.addEventListener("data", (evt) => {
			result = evt.detail;
		});
		await waitFor(
			() => session.peers[0].services.pubsub.getSubscribers("x")?.size === 1
		);
		await waitFor(
			() => session.peers[1].services.pubsub.getSubscribers("x")?.size === 1
		);

		session.peers[0].services.pubsub.publish(new Uint8Array([1, 2, 3]), {
			topics: ["x"],
		});
		await waitFor(() => !!result);
	});
});
