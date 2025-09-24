import { expect } from "chai";
import sodium from "libsodium-wrappers";
import { createLibp2pExtended } from "../src/libp2p.js";

it("starts", async () => {
	await sodium.ready; // Some of the modules depends on sodium to be readyy
	const node = await createLibp2pExtended();
	await node.start();
	// if node we expect 2 addresse if browser 0

	if (typeof window === "undefined") {
		const addresses = node.getMultiaddrs().map((x) => x.toString());
		// check existance of ws, webrtc-direct, and tcp
		expect(addresses).to.have.length(2); // 3  TODO: add back when webrtc-direct is supported in browser
		expect(
			addresses.find((a) => a.includes("/ws")),
			"ws",
		).to.exist;
		/* expect(
			addresses.find((a) => a.includes("/webrtc-direct")),
			"webrtc-direct",
		).to.exist; */ // TODO: add back when webrtc-direct is supported in browser
		expect(
			addresses.find((a) => a.includes("/tcp")),
			"tcp",
		).to.exist;
	} else {
		expect(node.getMultiaddrs()).to.have.length(0);
	}

	expect(node.services.pubsub).to.exist;
	expect(node.services.blocks).to.exist;
	await node.stop();
});
