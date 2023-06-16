import { Peerbit } from "@dao-xyz/peerbit";
import { create } from "../client";

describe("client", () => {
	let client: Peerbit;
	afterEach(async () => {
		await client?.stop();
	});
	it("default config will relay messages", async () => {
		client = await create("./tmp/server-node/client/" + new Date());
		expect(client.libp2p.services.pubsub.canRelayMessage).toEqual(true);
		expect(client.libp2p.services.blocks).toBeDefined();
		expect(client.libp2p.services.relay).toBeDefined();
	});
});
