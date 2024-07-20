import { expect } from "chai";
import { createLibp2pExtended } from "../src/libp2p.js";

it("starts", async () => {
	const node = await createLibp2pExtended();
	await node.start();
	expect(node.getMultiaddrs()).to.have.length(2);
	expect(node.services.pubsub).to.exist;
	expect(node.services.blocks).to.exist;
	await node.stop();
});
