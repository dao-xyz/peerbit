import { type ProgramClient } from "@peerbit/program";
import { create } from "../src/peerbit.js";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from 'chai'
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
		expect((client.services.blocks as any)["remoteBlocks"].localStore).to.be.instanceOf(
			AnyBlockStore
		);
		expect((client.services.blocks as any)["canRelayMessage"]).equal(true);
		expect((client.services.pubsub as any)["canRelayMessage"]).equal(true);
		expect((client.services as any)["relay"]).to.exist;
	});
});
