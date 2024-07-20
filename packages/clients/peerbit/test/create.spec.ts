// Include test utilities
import { createEd25519PeerId } from "@libp2p/peer-id-factory";
import { expect } from "chai";
import fs from "fs";
import path from "path";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../src/peer.js";

const dbPath = path.join("tmp", "peerbit", "tests", "create-open");

describe("Create", function () {
	describe("with db", function () {
		let client: Peerbit;
		let clientDirectory: string;
		before(async () => {
			clientDirectory = dbPath + uuid();
			client = (await Peerbit.create({
				directory: clientDirectory,
			})) as Peerbit;
		});
		after(async () => {
			await client.stop();
		});

		it("directory exist", async () => {
			expect(client.directory).equal(clientDirectory);
		});

		it("creates directory", async () => {
			expect(fs.existsSync(clientDirectory)).equal(true);
		});

		it("block storage exist at path", async () => {
			/* const location: string = (
				client.libp2p.services.blocks["remoteBlocks"]
					.localStore as AnyBlockStore
			)["_store"].store["location"];
			expect(location.endsWith(path.join(client.directory!, "blocks").toString())).to.be.true; */
			expect(await client.libp2p.services.blocks.persisted()).to.be.true;
		});
	});

	it("can create with peerId", async () => {
		const peerId = await createEd25519PeerId();
		const client = await Peerbit.create({
			libp2p: { peerId },
		});
		expect(client.peerId.equals(peerId)).to.be.true;
		await client.stop();
	});

	it("relays by default", async () => {
		const client = await Peerbit.create();
		expect(client.services.blocks.canRelayMessage).equal(true);
		expect(client.services.pubsub.canRelayMessage).equal(true);
		await client.stop();
	});
});
