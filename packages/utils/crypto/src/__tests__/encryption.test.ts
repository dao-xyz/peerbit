import { variant } from "@dao-xyz/borsh";
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
	it("encrypt with type", async () => {
		const senderKey = await X25519Keypair.create();
		const recieverKey1 = await X25519Keypair.create();
		const recieverKey2 = await X25519Keypair.create();

		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});

		const reciever1Config = keychain(recieverKey1);
		const reciever2Config = keychain(recieverKey2);

		const encrypted = await decrypted.encrypt(
			senderKey,
			recieverKey1.publicKey,
			recieverKey2.publicKey
		);
		encrypted._decrypted = undefined;

		const decryptedFromEncrypted1 = await encrypted.decrypt(reciever1Config);
		expect(decryptedFromEncrypted1.data).toStrictEqual(data);

		const decryptedFromEncrypted2 = await encrypted.decrypt(reciever2Config);
		expect(decryptedFromEncrypted2.data).toStrictEqual(data);
	});

	it("uint8array payload", async () => {
		const senderKey = await X25519Keypair.create();
		const recieverKey1 = await X25519Keypair.create();
		const recieverKey2 = await X25519Keypair.create();

		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});

		const reciever1Config = keychain(recieverKey1);
		const reciever2Config = keychain(recieverKey2);

		const encrypted = await decrypted.encrypt(
			senderKey,
			recieverKey1.publicKey,
			recieverKey2.publicKey
		);
		encrypted._decrypted = undefined;

		const decryptedFromEncrypted1 = await encrypted.decrypt(reciever1Config);
		expect(decryptedFromEncrypted1._data).toStrictEqual(data);

		const decryptedFromEncrypted2 = await encrypted.decrypt(reciever2Config);
		expect(decryptedFromEncrypted2._data).toStrictEqual(data);
	});

	// TODO feat
	/* it("it can use ed25519 for encryption", async () => {
		const senderKey = await Ed25519Keypair.create();
		const recieverKey1 = await Ed25519Keypair.create();
		const recieverKey2 = await Ed25519Keypair.create();
	
		const reciever1Config = config(recieverKey1);
		const reciever2Config = config(recieverKey2);
	
		const data = new Uint8Array([1, 2, 3]);
		const decrypted = new DecryptedThing({
			data,
		});
	
		const encrypted = await decrypted.encrypt(
			senderKey,
			recieverKey1.publicKey,
			recieverKey2.publicKey
		);
		encrypted._decrypted = undefined;
	
		const decryptedFromEncrypted1 = await encrypted.decrypt(
			reciever1Config.getAnyKeypair
		);
		expect(decryptedFromEncrypted1._data).toStrictEqual(data);
	
		const decryptedFromEncrypted2 = await encrypted.decrypt(
			reciever2Config.getAnyKeypair
		);
		expect(decryptedFromEncrypted2._data).toStrictEqual(data);
	}); */
});
