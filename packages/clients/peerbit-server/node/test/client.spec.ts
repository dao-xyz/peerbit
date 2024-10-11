import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { type ProgramClient } from "@peerbit/program";
import { expect } from "chai";
import { create } from "../src/peerbit.js";

describe("client", () => {
	let client: ProgramClient;
	afterEach(async () => {
		await client?.stop();
	});
	it("default config will relay messages", async () => {
		client = await create({
			keypair: await Ed25519Keypair.create(),
			directory: "./tmp/server-node/client/" + new Date(),
			listenPort: 9123,
		});
		expect(
			(client.services.blocks as any)["remoteBlocks"].localStore,
		).to.be.instanceOf(AnyBlockStore);
		expect((client.services.blocks as any)["canRelayMessage"]).equal(true);
		expect((client.services.pubsub as any)["canRelayMessage"]).equal(true);
		expect((client.services as any)["relay"]).to.exist;
	});
});
