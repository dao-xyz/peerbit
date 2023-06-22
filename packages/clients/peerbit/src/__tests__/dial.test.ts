import { LSession } from "@peerbit/test-utils";
import { createBlock, getBlockValue } from "@peerbit/blocks";
import { waitFor } from "@peerbit/time";

describe(`dial`, function () {
	let session: LSession;

	beforeEach(async () => {
		session = await LSession.disconnected(2);
	});

	afterEach(async () => {
		await session.stop();
	});

	it("waits for blocks", async () => {
		const cid = await session.peers[0].services.blocks.put(new Uint8Array([1]));
		await session.peers[0].dial(session.peers[1].getMultiaddrs()[0]);
		expect((await session.peers[0].services.blocks.get(cid))!).toEqual(
			new Uint8Array([1])
		);
	});

	it("waits for pubsub", async () => {
		let topic = "topic";
		await session.peers[1].services.pubsub.subscribe(topic);
		let data: Uint8Array | undefined = undefined;
		session.peers[1].services.pubsub.addEventListener("data", (d) => {
			data = d.detail.data.data;
		});
		await session.peers[0].dial(session.peers[1].getMultiaddrs()[0]);
		await session.peers[0].services.pubsub.publish(new Uint8Array([1]), {
			topics: [topic],
		});
		await waitFor(() => !!data);
		expect(data && new Uint8Array(data)).toEqual(new Uint8Array([1]));
	});

	it("autodials by default", async () => {
		expect(
			session.peers[0].services.pubsub["connectionManagerOptions"].autoDial
		).toBeTrue();
		expect(
			session.peers[1].services.pubsub["connectionManagerOptions"].autoDial
		).toBeTrue();
	});
});
