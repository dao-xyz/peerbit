import { expect } from "chai";
import {
	DecryptedThing,
	X25519Keypair,
	createDecrypterFromKeyResolver,
	createLocalEncryptProvider,
} from "../src/index.js";

describe("encryption", function () {
	it("encrypt-provider", async () => {
		const senderKey = await X25519Keypair.create();
		const receiverKey1 = await X25519Keypair.create();
		const receiverKey2 = await X25519Keypair.create();

		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});

		const receiver1Config = createDecrypterFromKeyResolver(
			() => receiverKey1 as any,
		);
		const receiver2Config = createDecrypterFromKeyResolver(
			() => receiverKey2 as any,
		);

		const encrypted = await decrypted.encrypt(
			createLocalEncryptProvider(senderKey),
			{
				receiverPublicKeys: [receiverKey1.publicKey, receiverKey2.publicKey],
			},
		);

		encrypted._decrypted = undefined;

		const decryptedFromEncrypted1 = await encrypted.decrypt(receiver1Config);
		expect(decryptedFromEncrypted1._data).to.deep.equal(data);

		const decryptedFromEncrypted2 = await encrypted.decrypt(receiver2Config);
		expect(decryptedFromEncrypted2._data).to.deep.equal(data);
	});

	it("x25519", async () => {
		const senderKey = await X25519Keypair.create();
		const receiverKey1 = await X25519Keypair.create();
		const receiverKey2 = await X25519Keypair.create();

		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});

		const encrypted = await decrypted.encrypt(senderKey, [
			receiverKey1.publicKey,
			receiverKey2.publicKey,
		]);

		encrypted._decrypted = undefined;

		const decryptedFromEncrypted1 = await encrypted.decrypt(receiverKey1);
		expect(decryptedFromEncrypted1._data).to.deep.equal(data);

		const decryptedFromEncrypted2 = await encrypted.decrypt(receiverKey2);
		expect(decryptedFromEncrypted2._data).to.deep.equal(data);
	});

	// TODO feat
	/* it("it can use ed25519 for encryption", async () => {
		const senderKey = await Ed25519Keypair.create();
		const receiverKey1 = await Ed25519Keypair.create();
		const receiverKey2 = await Ed25519Keypair.create();
	
		const receiver1Config = config(receiverKey1);
		const receiver2Config = config(receiverKey2);
	
		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});
	
		const encrypted = await decrypted.encrypt(
			senderKey,
			receiverKey1.publicKey,
			receiverKey2.publicKey
		);
		encrypted._decrypted = undefined;
	
		const decryptedFromEncrypted1 = await encrypted.decrypt(
			receiver1Config.getAnyKeypair
		);
		expect(decryptedFromEncrypted1._data).to.deep.equal(data);
	
		const decryptedFromEncrypted2 = await encrypted.decrypt(
			receiver2Config.getAnyKeypair
		);
		expect(decryptedFromEncrypted2._data).to.deep.equal(data);
	}); */
});
