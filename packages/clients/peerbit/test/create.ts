/* eslint-disable @typescript-eslint/no-unused-expressions */
import { keys } from "@libp2p/crypto";
import { expect } from "chai";
import path from "path";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../src/peer.js";

describe("Create", function () {
	describe("with db", function () {
		let client: Peerbit;
		let clientDirectory: string;
		before(async () => {
			const dbPath = path.join("tmp", "peerbit", "tests", "create-open");
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
			const fs = await import("fs");
			expect(fs.existsSync(clientDirectory)).equal(true);
		});

		it("block storage exist at path", async () => {
			expect(await client.libp2p.services.blocks.persisted()).to.be.true;
		});
	});

	it("can create with peerId", async () => {
		const privateKey = await keys.generateKeyPair("Ed25519");
		const client = await Peerbit.create({
			libp2p: { privateKey },
		});
		expect(client.peerId.publicKey!.equals(privateKey.publicKey)).to.be.true;
		await client.stop();
	});

	it("relays by default", async () => {
		const client = await Peerbit.create();
		expect(client.services.blocks.canRelayMessage).equal(true);
		expect(client.services.pubsub.canRelayMessage).equal(true);
		expect(client.services.relay).to.exist;
		await client.stop();
	});
});
