import fs from "fs-extra";
import path from "path";
import { Peerbit } from "../peer.js";

// @ts-ignore
import { v4 as uuid } from "uuid";

// Include test utilities
import { LevelBlockStore } from "@peerbit/blocks";
import { createEd25519PeerId } from "@libp2p/peer-id-factory";

const dbPath = path.join("./peerbit", "tests", "create-open");

describe("Create", function () {
	describe("with db", function () {
		let client: Peerbit;
		let clientDirectory: string;
		beforeAll(async () => {
			clientDirectory = dbPath + uuid();
			client = await Peerbit.create({
				directory: clientDirectory,
			});
		});
		afterAll(async () => {
			await client.stop();
		});

		it("directory exist", async () => {
			expect(client.directory).toEqual(clientDirectory);
		});

		it("creates directory", async () => {
			expect(fs.existsSync(clientDirectory)).toEqual(true);
		});

		it("block storage exist at path", async () => {
			const location = (
				client.libp2p.services.blocks["_localStore"] as LevelBlockStore
			)["_level"]._store["location"];
			expect(location).toEndWith(
				path.join(client.directory!, "blocks").toString()
			);
		});
	});

	it("can create with peerId", async () => {
		const peerId = await createEd25519PeerId();
		const client = await Peerbit.create({
			libp2p: { peerId },
		});
		expect(client.libp2p.peerId.equals(peerId)).toBeTrue();
		await client.stop();
	});
});
