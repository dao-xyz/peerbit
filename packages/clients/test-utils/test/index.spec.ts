import { waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import type { Peerbit } from "peerbit";
import { TestSession } from "../src/session.js";

describe("session", () => {
	let session: TestSession;
	before(async () => {});

	after(async () => {
		await session.stop();
	});
	it("pubsub", async () => {
		session = await TestSession.connected(3);

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
				(await session.peers[0].services.pubsub.getSubscribers("x"))?.length,
			).equal(3),
		);
		await waitForResolved(async () =>
			expect(
				(await session.peers[1].services.pubsub.getSubscribers("x"))?.length,
			).equal(3),
		);
		session.peers[0].services.pubsub.publish(new Uint8Array([1, 2, 3]), {
			topics: ["x"],
		});
		await waitFor(() => !!result);
	});

	it("indexer", async () => {
		session = await TestSession.connected(2);
		expect(session.peers[0].indexer).to.exist;
		expect(session.peers[0].indexer !== session.peers[1].indexer).to.be.true;
	});

	it("can stop individually", async () => {
		session = await TestSession.connected(2);
		const peers: Peerbit[] = session.peers as Peerbit[];
		await peers[0].stop();
		expect(peers.map((x) => x.libp2p.status)).to.deep.eq([
			"stopped",
			"started",
		]);
	});

	it("can create with directory", async () => {
		let directory = `tmp/test-utils/tests/can-create-with-directory-${Math.random()}`;
		session = await TestSession.connected(1, {
			directory,
		});

		let client = session.peers[0] as Peerbit;
		expect(client.directory).to.equal(directory);

		// put block
		const cid = await client.services.blocks.put(new Uint8Array([1, 2, 3]));
		expect(cid).to.exist;

		await session.stop();
		session = await TestSession.connected(1, {
			directory,
		});

		client = session.peers[0] as Peerbit;

		expect(client.directory).to.equal(directory);
		expect(client.libp2p.status).to.equal("started");
		const bytes = await client.services.blocks.get(cid);
		expect(bytes).to.deep.equal(new Uint8Array([1, 2, 3]));
	});
});
