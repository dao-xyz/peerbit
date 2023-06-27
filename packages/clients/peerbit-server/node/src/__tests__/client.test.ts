import { create } from "../client";
import { Peerbit as IPeerbit } from "@peerbit/interface";

describe("client", () => {
	let client: IPeerbit;
	afterEach(async () => {
		await client?.stop();
	});
	it("default config will relay messages", async () => {
		client = await create("./tmp/server-node/client/" + new Date());
		expect(client.services.pubsub["canRelayMessage"]).toEqual(true);
		expect(client.services.blocks).toBeDefined();
		expect(client.services["relay"]).toBeDefined();
	});
});
