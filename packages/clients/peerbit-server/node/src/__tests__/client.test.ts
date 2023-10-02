import { ProgramClient } from "@peerbit/program";
import { create } from "../peerbit";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";

describe("client", () => {
	let client: ProgramClient;
	afterEach(async () => {
		await client?.stop();
	});
	it("default config will relay messages", async () => {
		client = await create({
			peerId: await (await Ed25519Keypair.create()).toPeerId(),
			directory: "./tmp/server-node/client/" + new Date(),
			listenPort: 9123
		});
		expect(client.services.blocks["_localStore"]).toBeInstanceOf(AnyBlockStore);
		expect(client.services.blocks["canRelayMessage"]).toEqual(true);
		expect(client.services.pubsub["canRelayMessage"]).toEqual(true);
		expect(client.services["relay"]).toBeDefined();
	});
});
