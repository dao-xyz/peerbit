import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

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

	(isNode ? it : it.skip)("local", async function () {
		// Root `pnpm -r test` runs many packages concurrently and can delay libp2p dials
		// beyond the default 60s mocha timeout. Bootstrap should still succeed.
		this.timeout(180_000);
		await peer.bootstrap(bootstrapPeer.getMultiaddrs());
		expect(peer.libp2p.services.pubsub.peers.size).greaterThan(0);
	});

	(isNode ? it : it.skip)("remote", async function () {
		this.timeout(180_000);
		if (process.env.PEERBIT_RUN_REMOTE_BOOTSTRAP_TEST !== "1") {
			this.skip();
		}
		await peer.bootstrap();
		expect(peer.libp2p.services.pubsub.peers.size).greaterThan(0);
	});
});
