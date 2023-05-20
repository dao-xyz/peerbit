import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
	Ed25519Keypair,
	PublicKeyEncryptionResolver,
	X25519Keypair,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";

import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>, signKey2: KeyWithMeta<Ed25519Keypair>;

const last = <T>(arr: T[]): T => {
	return arr[arr.length - 1];
};

describe("Log - Encryption", function () {
	let keystore: Keystore,
		senderKey: KeyWithMeta<X25519Keypair>,
		recieverKey: KeyWithMeta<X25519Keypair>,
		store: BlockStore;

	beforeAll(async () => {
		rmrf.sync(testKeyStorePath(__filenameBase));

		await fs.copy(
			signingKeysFixturesPath(__dirname),
			testKeyStorePath(__filenameBase)
		);

		keystore = new Keystore(
			await createStore(testKeyStorePath(__filenameBase))
		);

		senderKey = await keystore.createKey(await X25519Keypair.create(), {
			id: "sender",
			overwrite: true,
		});
		recieverKey = await keystore.createKey(await X25519Keypair.create(), {
			id: "reciever",
			overwrite: true,
		});

		// The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
		signKey = (await keystore.getKey(
			new Uint8Array([0])
		)) as KeyWithMeta<Ed25519Keypair>;
		signKey2 = (await keystore.getKey(
			new Uint8Array([1])
		)) as KeyWithMeta<Ed25519Keypair>;

		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});

	describe("join", () => {
		let log1: Log<string>, log2: Log<string>;

		beforeEach(async () => {
			const logOptions = {
				encryption: {
					getEncryptionKeypair: () => senderKey.keypair,
					getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
						for (let i = 0; i < publicKeys.length; i++) {
							if (publicKeys[i].equals(senderKey.keypair.publicKey)) {
								return {
									index: i,
									keypair: senderKey.keypair,
								};
							}
							if (publicKeys[i].equals(recieverKey.keypair.publicKey)) {
								return {
									index: i,
									keypair: recieverKey.keypair,
								};
							}
						}
					},
				} as PublicKeyEncryptionResolver,
			};
			log1 = new Log();
			await log1.open(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				logOptions
			);
			log2 = new Log();
			await log2.open(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				logOptions
			);
		});

		it("can encrypt signatures with particular reciever", async () => {
			// dummy signer
			const extraSigner = await Ed25519Keypair.create();
			const extraSigner2 = await Ed25519Keypair.create();

			await log2.append("helloA1", {
				reciever: {
					metadata: undefined,
					signatures: {
						[await log2.identity.publicKey.hashcode()]:
							recieverKey.keypair.publicKey, // reciever 1
						[await extraSigner.publicKey.hashcode()]: [
							recieverKey.keypair.publicKey,
							(await X25519Keypair.create()).publicKey,
						], // reciever 1 again and 1 unknown reciever
						[await extraSigner2.publicKey.hashcode()]: (
							await X25519Keypair.create()
						).publicKey, // unknown reciever
					},
					payload: recieverKey.keypair.publicKey,
					next: recieverKey.keypair.publicKey,
				},
				signers: [
					log2.identity.sign.bind(log2.identity),
					extraSigner.sign.bind(extraSigner),
					extraSigner2.sign.bind(extraSigner2),
				],
			});

			// Remove decrypted caches of the log2 values
			(await log2.toArray()).forEach((value) => {
				value._metadata.clear();
				value._payload.clear();
				value._signatures!.signatures.forEach((signature) => signature.clear());
				value._next.clear();
			});

			await log1.join(log2);
			expect(log1.length).toEqual(1);
			const item = last(await log1.toArray());
			expect((await item.getNext()).length).toEqual(0);
			expect(
				(await item.getSignatures()).map((x) => x.publicKey.hashcode())
			).toContainAllValues([
				extraSigner.publicKey.hashcode(),
				log2.identity.publicKey.hashcode(),
			]);
		});

		it("joins encrypted identities only with knowledge of id and clock", async () => {
			await log1.append("helloA1", {
				reciever: {
					metadata: undefined,
					signatures: recieverKey.keypair.publicKey,
					payload: recieverKey.keypair.publicKey,
					next: recieverKey.keypair.publicKey,
				},
			});
			await log1.append("helloA2", {
				reciever: {
					metadata: undefined,
					signatures: recieverKey.keypair.publicKey,
					payload: recieverKey.keypair.publicKey,
					next: recieverKey.keypair.publicKey,
				},
			});
			await log2.append("helloB1", {
				reciever: {
					metadata: undefined,
					signatures: recieverKey.keypair.publicKey,
					payload: recieverKey.keypair.publicKey,
					next: recieverKey.keypair.publicKey,
				},
			});
			await log2.append("helloB2", {
				reciever: {
					metadata: undefined,
					signatures: recieverKey.keypair.publicKey,
					payload: recieverKey.keypair.publicKey,
					next: recieverKey.keypair.publicKey,
				},
			});

			// Remove decrypted caches of the log2 values
			(await log2.toArray()).forEach((value) => {
				value._metadata.clear();
				value._payload.clear();
				value._signatures!.signatures.forEach((signature) => signature.clear());
				value._next.clear();
			});

			await log1.join(log2);
			expect(log1.length).toEqual(4);
			const item = last(await log1.toArray());
			expect((await item.getNext()).length).toEqual(1);
		});
	});
});
