import { Ed25519Keypair, X25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { DefaultCryptoKeychain } from "../src/crypto.js";

describe("keychain", () => {
	let keychains: DefaultCryptoKeychain[];

	beforeEach(() => {
		keychains = [new DefaultCryptoKeychain(), new DefaultCryptoKeychain()];
	});

	describe("ed25519", () => {
		it("import/export", async () => {
			for (const keychain of keychains) {
				const kp = await Ed25519Keypair.create();
				await keychain.import({ keypair: kp, id: new Uint8Array([1, 2, 3]) });
				expect(
					(
						await keychain.exportById<Ed25519Keypair>(
							new Uint8Array([1, 2, 3]),
							Ed25519Keypair,
						)
					)?.equals(kp),
				).to.be.true;
				expect((await keychain.exportByKey(kp.publicKey))?.equals(kp)).to.be
					.true;
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
						await keychain.exportById<X25519Keypair>(
							new Uint8Array([1, 2, 3]),
							X25519Keypair,
						)
					)?.equals(xkp),
				).to.be.true;
				expect((await keychain.exportByKey(xkp.publicKey))?.equals(xkp)).to.be
					.true;
			}
		});
	});
});
