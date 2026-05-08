/* eslint-disable @typescript-eslint/no-unused-expressions */
import { keys } from "@libp2p/crypto";
import { createStore } from "@peerbit/any-store";
import { RustAnyStore } from "@peerbit/any-store-rust";
import { Ed25519Keypair } from "@peerbit/crypto";
import { RustIndices } from "@peerbit/indexer-rust";
import { expect } from "chai";
import path from "path";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../src/peer.js";
import { createRustPeerbitOptions } from "../src/rust.js";

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

	it("can create with a local store factory", async () => {
		const clientDirectory = path.join(
			"tmp",
			"peerbit",
			"tests",
			"create-store-factory-" + uuid(),
		);
		const directories: string[] = [];
		const client = await Peerbit.create({
			directory: clientDirectory,
			storage: {
				storeFactory: (directory) => {
					directories.push(directory ?? "");
					return createStore(directory);
				},
			},
		});

		expect(directories).to.include(path.join(clientDirectory, "/cache"));
		expect(directories).to.include(path.join(clientDirectory, "/keychain"));
		expect(directories).to.include(path.join(clientDirectory, "/blocks"));
		await client.stop();
	});

	it("can create with the rust storage preset", async () => {
		const clientDirectory = path.join(
			"tmp",
			"peerbit",
			"tests",
			"create-rust-preset-" + uuid(),
		);
		const client = await Peerbit.create({
			directory: clientDirectory,
			...createRustPeerbitOptions(),
		});

		expect(client.storage).to.be.instanceOf(RustAnyStore);
		expect(client.indexer).to.be.instanceOf(RustIndices);
		expect(await client.storage.persisted()).to.be.true;
		expect(await client.indexer.persisted()).to.be.true;
		expect(await client.libp2p.services.blocks.persisted()).to.be.true;
		await client.stop();
	});

	it("can create with privateKey", async () => {
		const privateKey = await keys.generateKeyPair("Ed25519");
		const client = await Peerbit.create({
			libp2p: { privateKey },
		});
		expect(client.peerId.publicKey!.equals(privateKey.publicKey)).to.be.true;
		await client.stop();
	});

	it("throws when peerId is provided in libp2p options", async () => {
		const peerId = (await Ed25519Keypair.create()).toPeerId();
		await expect(
			Peerbit.create({
				libp2p: { peerId } as any,
			}),
		).to.be.rejectedWith(
			Error,
			"Invalid libp2p option 'peerId'. libp2p derives the peer id from 'privateKey', so pass 'privateKey' to control identity.",
		);
	});

	it("relays by default", async () => {
		const client = await Peerbit.create();
		expect(client.services.blocks.canRelayMessage).equal(true);
		expect(client.services.pubsub.canRelayMessage).equal(true);
		expect(client.services.relay).to.exist;
		await client.stop();
	});
});
