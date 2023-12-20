import { DefaultKeychain } from "../index.js";
import { Ed25519Keypair, X25519Keypair } from "@peerbit/crypto";

describe("keychain", () => {
	let keychains: DefaultKeychain[];

	beforeEach(() => {
		keychains = [new DefaultKeychain(), new DefaultKeychain()];
	});

	describe("ed25519", () => {
		it("import/export", async () => {
			for (const keychain of keychains) {
				const kp = await Ed25519Keypair.create();
				await keychain.import({ keypair: kp, id: new Uint8Array([1, 2, 3]) });
				expect(
					(
						await keychain.exportById(new Uint8Array([1, 2, 3]), Ed25519Keypair)
					)?.equals(kp)
				).toBeTrue();
				expect(
					(await keychain.exportByKey(kp.publicKey))?.equals(kp)
				).toBeTrue();
			}
		});
	});

	describe("x25519", () => {
		it("import/export", async () => {
			for (const keychain of keychains) {
				const kp = await Ed25519Keypair.create();
				const xkp = await X25519Keypair.from(kp);
				await keychain.import({ keypair: kp, id: new Uint8Array([1, 2, 3]) });
				expect(
					(
						await keychain.exportById(new Uint8Array([1, 2, 3]), X25519Keypair)
					)?.equals(xkp)
				).toBeTrue();
				expect(
					(await keychain.exportByKey(xkp.publicKey))?.equals(xkp)
				).toBeTrue();
			}
		});
	});
});
