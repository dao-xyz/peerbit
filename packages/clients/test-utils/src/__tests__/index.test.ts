import { TestSession } from "../session.js";
import { waitFor } from "@peerbit/time";

describe("session", () => {
	let session: TestSession;
	beforeAll(async () => {
		session = await TestSession.connected(3);
	});

	afterAll(async () => {
		await session.stop();
	});
	it("pubsub", async () => {
		let result: any = undefined;

		await session.peers[0].services.pubsub.subscribe("x");
		await session.peers[1].services.pubsub.subscribe("x");
		await session.peers[2].services.pubsub.subscribe("x");

		await session.peers[0].services.pubsub.requestSubscribers("x");
		await session.peers[1].services.pubsub.requestSubscribers("x");
		await session.peers[2].services.pubsub.requestSubscribers("x");

		session.peers[2].services.pubsub.addEventListener("data", (evt) => {
			result = evt.detail;
		});
		await waitFor(
			async () =>
				(await session.peers[0].services.pubsub.getSubscribers("x"))?.length ===
				3
		);
		await waitFor(
			async () =>
				(await session.peers[1].services.pubsub.getSubscribers("x"))?.length ===
				3
		);

		session.peers[0].services.pubsub.publish(new Uint8Array([1, 2, 3]), {
			topics: ["x"]
		});
		await waitFor(() => !!result);
	});
});
