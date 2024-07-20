import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

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
		expect(peer.libp2p.services.pubsub.peers.size).greaterThan(0);
	});
});
