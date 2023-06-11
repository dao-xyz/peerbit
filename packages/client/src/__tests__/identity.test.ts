import { Peerbit } from "../peer.js";
import { EventStore } from "./utils/stores/index.js";
import { Ed25519Keypair, X25519Keypair } from "@dao-xyz/peerbit-crypto";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";

describe(`identity`, function () {
	describe("set", () => {
		let session: LSession, client: Peerbit, options: any;
		let signKey1: Ed25519Keypair;

		beforeAll(async () => {
			session = await LSession.connected(1);
			const keyInfo = await session.peers[0].keychain.createKey(
				"some-key",
				"Ed25519"
			);
			const peerId = await session.peers[0].keychain.exportPeerId(keyInfo.name);
			signKey1 = Ed25519Keypair.fromPeerId(peerId);
			client = await Peerbit.create({ libp2p: session.peers[0] });
		});

		afterAll(async () => {
			if (client) await client.stop();

			await session.stop();
		});

		beforeEach(async () => {
			options = Object.assign({}, options, {});
		});

		it("sets identity", async () => {
			const db = await client.open(new EventStore<string>(), options);
			expect(db.log.identity.publicKey.equals(client.identity.publicKey));
			db.log.setIdentity(signKey1);
			expect(db.log.identity.publicKey.equals(signKey1.publicKey));
			await db.close();
		});
	});

	describe("restore", () => {
		let client: Peerbit;

		afterEach(async () => {
			await client.stop();
		});

		it("disc", async () => {
			const directory = "./tmp/" + +new Date();
			client = await Peerbit.create({ directory });
			const id1 = client.libp2p.peerId;

			const kp = await Ed25519Keypair.create();
			await client.importKeypair(kp);

			// stop
			await client.stop();

			// reopen same dir
			client = await Peerbit.create({ directory });

			expect(client.libp2p.peerId.equals(id1)).toBeTrue();

			const exportedKeypair = await client.exportKeypair(kp.publicKey);
			expect(exportedKeypair.equals(kp)).toBeTrue();
		});

		it("memory", async () => {
			client = await Peerbit.create({});
			const id1 = client.libp2p.peerId;

			const kp = await Ed25519Keypair.create();
			await client.importKeypair(kp);

			// stop
			await client.stop();

			// reopen, expect a clean slate
			client = await Peerbit.create({});
			expect(client.libp2p.peerId.equals(id1)).toBeFalse();
			await expect(client.exportKeypair(kp.publicKey)).rejects.toThrowError(
				"Not Found"
			);
		});
	});
});
