import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

describe("bootstrap", () => {
	let peer: Peerbit;

	beforeEach(async () => {});

	afterEach(async () => {
		await peer?.stop();
	});

	it("remote", async () => {
		for (let i = 0; i < 1000; i++) {
			console.log(`bootstrap ${i}`);
			peer = await Peerbit.create();
			await peer.bootstrap();
			expect(peer.libp2p.services.pubsub.peers.size).greaterThan(0);
			await peer?.stop();
		}
	});
});
