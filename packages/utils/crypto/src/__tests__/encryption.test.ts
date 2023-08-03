import {
	DecryptedThing,
	X25519Keypair,
	Keychain,
	PublicSignKey,
} from "../index.js";

describe("encryption", function () {
	const keychain = (keypair: X25519Keypair): Keychain => {
		return {
			exportById: async (id: Uint8Array) => undefined,
			exportByKey: async <T extends PublicSignKey, Q>(publicKey: T) =>
				publicKey.equals(keypair.publicKey) ? (keypair as Q) : undefined,
			import: (keypair: any, id: Uint8Array) => {
				throw new Error("No implemented+");
			},
		};
	};
	it("encrypt", async () => {
		const senderKey = await X25519Keypair.create();
		const receiverKey1 = await X25519Keypair.create();
		const receiverKey2 = await X25519Keypair.create();

		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});

		const receiver1Config = keychain(receiverKey1);
		const receiver2Config = keychain(receiverKey2);

		const encrypted = await decrypted.encrypt(
			senderKey,
			receiverKey1.publicKey,
			receiverKey2.publicKey
		);
		encrypted._decrypted = undefined;

		const decryptedFromEncrypted1 = await encrypted.decrypt(receiver1Config);
		expect(decryptedFromEncrypted1._data).toStrictEqual(data);

		const decryptedFromEncrypted2 = await encrypted.decrypt(receiver2Config);
		expect(decryptedFromEncrypted2._data).toStrictEqual(data);
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
		expect(decryptedFromEncrypted1._data).toStrictEqual(data);
	
		const decryptedFromEncrypted2 = await encrypted.decrypt(
			receiver2Config.getAnyKeypair
		);
		expect(decryptedFromEncrypted2._data).toStrictEqual(data);
	}); */
});
