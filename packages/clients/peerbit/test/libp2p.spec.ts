import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { createLibp2pExtended } from "../src/libp2p.js";

it("starts", async () => {
	await sodium.ready; // Some of the modules depends on sodium to be readyy
	const node = await createLibp2pExtended();
	await node.start();
	// if node we expect 2 addresse if browser 0

	if (typeof window === "undefined") {
		expect(node.getMultiaddrs()).to.have.length(2);
	} else {
		expect(node.getMultiaddrs()).to.have.length(0);
	}

	expect(node.services.pubsub).to.exist;
	expect(node.services.blocks).to.exist;
	await node.stop();
});
