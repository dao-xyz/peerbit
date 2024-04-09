import { X25519Keypair, DecryptedThing } from "@peerbit/crypto";
import { DefaultKeychain } from "../src/index.js";
import { expect } from "chai";

describe("encryption", () => {
	it("decryptProvider", async () => {
		const senderKey = await X25519Keypair.create();

		const receiverKey1 = await X25519Keypair.create();
		const receiverKeychain1 = new DefaultKeychain();
		await receiverKeychain1.import({ keypair: receiverKey1 });

		const receiverKey2 = await X25519Keypair.create();
		const receiverKeychain2 = new DefaultKeychain();
		await receiverKeychain2.import({ keypair: receiverKey2 });

		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data
		});

		const encrypted = await decrypted.encrypt(senderKey, [
			receiverKey1.publicKey,
			receiverKey2.publicKey
		]);

		encrypted._decrypted = undefined;

		const decryptedFromEncrypted1 = await encrypted.decrypt(receiverKeychain1);
		expect(decryptedFromEncrypted1._data).to.deep.equal(data);

		const decryptedFromEncrypted2 = await encrypted.decrypt(receiverKeychain2);
		expect(decryptedFromEncrypted2._data).to.deep.equal(data);
	});
});
