import { ProgramClient } from "@peerbit/program";
import { create } from "../client";

describe("client", () => {
	let client: ProgramClient;
	afterEach(async () => {
		await client?.stop();
	});
	it("default config will relay messages", async () => {
		client = await create({
			directory: "./tmp/server-node/client/" + new Date(),
		});
		expect(client.services.pubsub["canRelayMessage"]).toEqual(true);
		expect(client.services.blocks).toBeDefined();
		expect(client.services["relay"]).toBeDefined();
	});
});
