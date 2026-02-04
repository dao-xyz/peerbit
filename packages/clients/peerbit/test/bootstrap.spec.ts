import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

describe("bootstrap", () => {
	let peer: Peerbit;
	let bootstrapPeer: Peerbit;

	beforeEach(async () => {
		bootstrapPeer = await Peerbit.create();
		peer = await Peerbit.create();
	});

	afterEach(async () => {
		await peer?.stop();
		await bootstrapPeer?.stop();
	});

	it("local", async () => {
		await peer.bootstrap(bootstrapPeer.getMultiaddrs());
		expect(peer.libp2p.services.pubsub.peers.size).greaterThan(0);
	});

	it("remote", async function () {
		if (process.env.PEERBIT_RUN_REMOTE_BOOTSTRAP_TEST !== "1") {
			this.skip();
		}
		await peer.bootstrap();
		expect(peer.libp2p.services.pubsub.peers.size).greaterThan(0);
	});
});
