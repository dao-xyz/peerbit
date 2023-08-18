import { Peerbit } from "../peer.js";

describe("bootstrap", () => {
	let peer: Peerbit;

	beforeEach(async () => {
		peer = await Peerbit.create();
	});

	afterEach(async () => {
		await peer.stop();
	});

	it("remote", async () => {
		await peer.bootstrap();
		expect(peer.libp2p.services.pubsub.peers.size).toBeGreaterThan(0);
	});
});
