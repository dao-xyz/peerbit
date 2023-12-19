import { ProgramClient } from "@peerbit/program";
import { Peerbit } from "../peer.js";
import { Ed25519Keypair } from "@peerbit/crypto";

describe(`identity`, function () {
	describe("restore", () => {
		let client: ProgramClient;

		afterEach(async () => {
			await client?.stop();
		});

		it("disc", async () => {
			const directory = "./tmp/disc/" + +new Date();
			client = await Peerbit.create({ directory });
			const id1 = client.peerId;

			const kp = await Ed25519Keypair.create();
			await client.services.keychain?.import({
				keypair: kp,
				id: new Uint8Array([1, 2, 3])
			});

			// stop
			await client.stop();

			// reopen same dir
			client = await Peerbit.create({ directory });

			expect(client.peerId.equals(id1)).toBeTrue();

			const exportedKeypair = await client.services.keychain?.exportByKey(
				kp.publicKey
			);
			expect(exportedKeypair!.equals(kp)).toBeTrue();
		});

		it("memory", async () => {
			client = await Peerbit.create({});
			const id1 = client.peerId;

			const kp = await Ed25519Keypair.create();
			await client.services.keychain?.import({
				keypair: kp,
				id: new Uint8Array([1, 2, 3])
			});

			// stop
			await client.stop();

			// reopen, expect a clean slate
			client = await Peerbit.create({});
			expect(client.peerId.equals(id1)).toBeFalse();
			await expect(
				client.services.keychain?.exportByKey(kp.publicKey)
			).resolves.toBeUndefined();
		});
	});
});
