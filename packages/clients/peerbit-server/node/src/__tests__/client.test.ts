import { ProgramClient } from "@peerbit/program";
import { create } from "../peerbit";
import { LevelBlockStore } from "@peerbit/blocks";

describe("client", () => {
	let client: ProgramClient;
	afterEach(async () => {
		await client?.stop();
	});
	it("default config will relay messages", async () => {
		client = await create({
			directory: "./tmp/server-node/client/" + new Date(),
			listenPort: 9123,
		});
		expect(client.services.blocks["_localStore"]).toBeInstanceOf(
			LevelBlockStore
		);
		expect(client.services.blocks["canRelayMessage"]).toEqual(true);
		expect(client.services.pubsub["canRelayMessage"]).toEqual(true);
		expect(client.services["relay"]).toBeDefined();
	});
});
