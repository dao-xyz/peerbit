import { Log } from "../log.js";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Keychain,
	X25519Keypair,
	X25519PublicKey,
} from "@peerbit/crypto";
import {
	BlockStore,
	LevelBlockStore,
	MemoryLevelBlockStore,
} from "@peerbit/blocks";
import { signKey, signKey2 } from "./fixtures/privateKey.js";
import { Level } from "level";
import LazyLevel from "@peerbit/lazy-level";
import { JSON_ENCODING } from "./utils/encoding.js";

const last = <T>(arr: T[]): T => {
	return arr[arr.length - 1];
};

const createKeychain = (keys: (Ed25519Keypair | X25519Keypair)[]) => {
	return {
		exportById: () => {
			throw new Error("Not implemented");
		},
		exportByKey: <T extends Ed25519PublicKey | X25519PublicKey, Q>(
			exportKey: T
		) => {
			for (const key of keys) {
				if (key.publicKey.equals(exportKey)) {
					return key as Q;
				}
			}
			return undefined;
		},
		import: () => {
			throw new Error("Not implemented");
		},
	} as Keychain;
};

describe("encryption", function () {
	let store: BlockStore;

	afterEach(async () => {
		await store.stop();
	});

	describe("join", () => {
		let log1: Log<string>, log2: Log<string>;
		let recieverKey: X25519Keypair;

		beforeEach(async () => {
			store = new MemoryLevelBlockStore();
			await store.start();

			const senderKey = await X25519Keypair.create();
			recieverKey = await X25519Keypair.create();
			const logOptions = {
				encoding: JSON_ENCODING,
				keychain: createKeychain([signKey, senderKey, recieverKey]),
			};

			log1 = new Log();
			await log1.open(store, signKey, logOptions);
			log2 = new Log();
			await log2.open(store, signKey2, logOptions);
		});

		it("can encrypt signatures with particular reciever", async () => {
			// dummy signer
			const extraSigner = await Ed25519Keypair.create();
			const extraSigner2 = await Ed25519Keypair.create();

			await log2.append("helloA1", {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						meta: undefined,
						signatures: {
							[await log2.identity.publicKey.hashcode()]: recieverKey.publicKey, // reciever 1
							[await extraSigner.publicKey.hashcode()]: [
								recieverKey.publicKey,
								(await X25519Keypair.create()).publicKey,
							], // reciever 1 again and 1 unknown reciever
							[await extraSigner2.publicKey.hashcode()]: (
								await X25519Keypair.create()
							).publicKey, // unknown reciever
						},
						payload: recieverKey.publicKey,
					},
				},
				signers: [
					log2.identity.sign.bind(log2.identity),
					extraSigner.sign.bind(extraSigner),
					extraSigner2.sign.bind(extraSigner2),
				],
			});

			// Remove decrypted caches of the log2 values
			(await log2.toArray()).forEach((value) => {
				value._meta.clear();
				value._payload.clear();
				value._signatures!.signatures.forEach((signature) => signature.clear());
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
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						meta: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
					},
				},
			});
			await log1.append("helloA2", {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						meta: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
					},
				},
			});
			await log2.append("helloB1", {
				encryption: {
					keypair: await X25519Keypair.create(),

					reciever: {
						meta: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
					},
				},
			});
			await log2.append("helloB2", {
				encryption: {
					keypair: await X25519Keypair.create(),

					reciever: {
						meta: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
					},
				},
			});

			// Remove decrypted caches of the log2 values
			(await log2.toArray()).forEach((value) => {
				value._meta.clear();
				value._payload.clear();
				value._signatures!.signatures.forEach((signature) => signature.clear());
			});

			await log1.join(log2);
			expect(log1.length).toEqual(4);
			const item = last(await log1.toArray());
			expect((await item.getNext()).length).toEqual(1);
		});
	});

	describe("load", () => {
		let cache: LazyLevel, level: Level, log: Log<any>;
		afterEach(async () => {
			await log?.close();
			await cache?.close();
			await level?.close();
			const q = 123;
		});

		it("loads encrypted entries", async () => {
			level = new Level(
				"./tmp/log/encryption/load/loads-encrypted-entries/" + +new Date()
			);
			const blocks = level.sublevel<string, Uint8Array>("blocks", {
				valueEncoding: "view",
			});
			store = new LevelBlockStore(blocks);
			await store.start();

			const encryptioKey = await X25519Keypair.create();
			const signingKey = await Ed25519Keypair.create();

			log = new Log();
			cache = new LazyLevel(
				level.sublevel<string, Uint8Array>("cache", { valueEncoding: "view" })
			);
			const logOptions = {
				keychain: createKeychain([signingKey, encryptioKey]),
				cache,
				encoding: JSON_ENCODING,
			};
			await log.open(store, signKey, logOptions);

			await log.append("helloA1", {
				encryption: {
					keypair: encryptioKey,
					reciever: {
						meta: encryptioKey.publicKey,
						signatures: encryptioKey.publicKey,
						payload: encryptioKey.publicKey,
					},
				},
			});
			expect(log.length).toEqual(1);
			await log.close();
			log = new Log();
			await log.open(store, signKey, logOptions);
			expect(log.headsIndex.headsCache).toBeDefined();
			expect(log.length).toEqual(0);
			await log.load();
			expect(log.length).toEqual(1);
		});
	});
});
