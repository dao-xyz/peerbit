import { MemoryDatastore } from "datastore-core";
import { DefaultKeyChain } from "@libp2p/keychain";
import { Ed25519Keypair, X25519Keypair, ByteKey } from "@peerbit/crypto";
import { Cache } from "@peerbit/cache";
import { Keychain } from "../";

// TODO update tests

describe("keychain", () => {
	let keychains: Keychain[];

	beforeEach(() => {
		keychains = [
			new Libp2pKeychain(
				new DefaultKeyChain({ datastore: new MemoryDatastore() }, {})
			),
			new Libp2pKeychain(
				new DefaultKeyChain({ datastore: new MemoryDatastore() }, {}),
				{ cache: new Cache({ max: 1000 }) }
			)
		];
	});

	describe("ed25519", () => {
		it("import/export", async () => {
			for (const keychain of keychains) {
				const kp = await Ed25519Keypair.create();
				await keychain.import(kp, new Uint8Array([1, 2, 3]));
				expect(
					(
						await keychain.exportById(new Uint8Array([1, 2, 3]), "ed25519")
					)?.equals(kp)
				).toBeTrue();
				expect(
					(await keychain.exportByPublicKey(kp.publicKey))?.equals(kp)
				).toBeTrue();
			}
		});
	});

	describe("x25519", () => {
		it("import/export", async () => {
			for (const keychain of keychains) {
				const kp = await Ed25519Keypair.create();
				const xkp = await X25519Keypair.from(kp);
				await keychain.import(kp, new Uint8Array([1, 2, 3]));
				expect(
					(
						await keychain.exportById(new Uint8Array([1, 2, 3]), "x25519")
					)?.equals(xkp)
				).toBeTrue();
				expect(
					(await keychain.exportByPublicKey(xkp.publicKey))?.equals(xkp)
				).toBeTrue();
			}
		});
	});
});
