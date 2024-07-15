import { TestSession } from "../src/session.js";
import { waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";

describe("session", () => {
	let session: TestSession;
	before(async () => {
		session = await TestSession.connected(3);
	});

	after(async () => {
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
		await waitForResolved(async () =>
			expect(
				(await session.peers[0].services.pubsub.getSubscribers("x"))?.length
			).equal(3)
		);
		await waitForResolved(async () =>
			expect(
				(await session.peers[1].services.pubsub.getSubscribers("x"))?.length
			).equal(3)
		);
		session.peers[0].services.pubsub.publish(new Uint8Array([1, 2, 3]), {
			topics: ["x"]
		});
		await waitFor(() => !!result);
	});

	it("indexer", async () => {
		expect(session.peers[0].indexer).to.exist
		expect(session.peers[0].indexer != session.peers[1].indexer).to.be.true

	})
});
