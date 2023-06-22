import { Peerbit } from "../peer.js";
import { Ed25519Keypair } from "@peerbit/crypto";

describe(`identity`, function () {
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
			await client.keychain?.import(kp, new Uint8Array([1, 2, 3]));

			// stop
			await client.stop();

			// reopen same dir
			client = await Peerbit.create({ directory });

			expect(client.libp2p.peerId.equals(id1)).toBeTrue();

			const exportedKeypair = await client.keychain?.exportByKey(kp.publicKey);
			expect(exportedKeypair!.equals(kp)).toBeTrue();
		});

		it("memory", async () => {
			client = await Peerbit.create({});
			const id1 = client.libp2p.peerId;

			const kp = await Ed25519Keypair.create();
			await client.keychain?.import(kp, new Uint8Array([1, 2, 3]));

			// stop
			await client.stop();

			// reopen, expect a clean slate
			client = await Peerbit.create({});
			expect(client.libp2p.peerId.equals(id1)).toBeFalse();
			await expect(
				client.keychain?.exportByKey(kp.publicKey)
			).rejects.toThrowError("Not Found");
		});
	});
});
