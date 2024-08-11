import { type AnyStore, createStore } from "@peerbit/any-store";
import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { Ed25519Keypair, X25519Keypair } from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import { create } from "@peerbit/indexer-sqlite3";
import { DefaultKeychain } from "@peerbit/keychain";
import { expect } from "chai";
import path from "path";
import type { EntryV0 } from "../src/entry-v0.js";
import { Log } from "../src/log.js";
import { signKey, signKey2 } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

const last = <T>(arr: T[]): T => {
	return arr[arr.length - 1];
};

const createKeychain = async (...keys: (Ed25519Keypair | X25519Keypair)[]) => {
	const keychain = new DefaultKeychain();
	for (const key of keys) {
		await keychain.import({ keypair: key });
	}
	return keychain;
};

describe("encryption", function () {
	let store: BlockStore;

	afterEach(async () => {
		await store.stop();
	});

	describe("join", () => {
		let log1: Log<string>, log2: Log<string>;
		let receiverKey: X25519Keypair;

		beforeEach(async () => {
			store = new AnyBlockStore();
			await store.start();

			const senderKey = await X25519Keypair.create();
			receiverKey = await X25519Keypair.create();
			const logOptions = {
				encoding: JSON_ENCODING,
				keychain: await createKeychain(signKey, senderKey, receiverKey),
			};

			log1 = new Log();
			await log1.open(store, signKey, logOptions);
			log2 = new Log();
			await log2.open(store, signKey2, logOptions);
		});

		it("can encrypt signatures with particular receiver", async () => {
			// dummy signer
			const extraSigner = await Ed25519Keypair.create();
			const extraSigner2 = await Ed25519Keypair.create();

			await log2.append("helloA1", {
				encryption: {
					keypair: await X25519Keypair.create(),
					receiver: {
						meta: undefined,
						signatures: {
							[await log2.identity.publicKey.hashcode()]: receiverKey.publicKey, // receiver 1
							[await extraSigner.publicKey.hashcode()]: [
								receiverKey.publicKey,
								(await X25519Keypair.create()).publicKey,
							], // receiver 1 again and 1 unknown receiver
							[await extraSigner2.publicKey.hashcode()]: (
								await X25519Keypair.create()
							).publicKey, // unknown receiver
						},
						payload: receiverKey.publicKey,
					},
				},
				signers: [
					log2.identity.sign.bind(log2.identity),
					extraSigner.sign.bind(extraSigner),
					extraSigner2.sign.bind(extraSigner2),
				],
			});

			// Remove decrypted caches of the log2 values
			((await log2.toArray()) as EntryV0<any>[]).forEach((value) => {
				value._meta.clear();
				value._payload.clear();
				value._signatures!.signatures.forEach((signature) => signature.clear());
			});

			await log1.join(log2);
			expect(log1.length).equal(1);
			const item = last(await log1.toArray());
			expect((await item.getNext()).length).equal(0);
			expect(
				(await item.getSignatures()).map((x) => x.publicKey.hashcode()),
			).to.have.members([
				extraSigner.publicKey.hashcode(),
				log2.identity.publicKey.hashcode(),
			]);
		});

		it("joins encrypted identities only with knowledge of id and clock", async () => {
			await log1.append("helloA1", {
				encryption: {
					keypair: await X25519Keypair.create(),
					receiver: {
						meta: undefined,
						signatures: receiverKey.publicKey,
						payload: receiverKey.publicKey,
					},
				},
			});
			await log1.append("helloA2", {
				encryption: {
					keypair: await X25519Keypair.create(),
					receiver: {
						meta: undefined,
						signatures: receiverKey.publicKey,
						payload: receiverKey.publicKey,
					},
				},
			});
			await log2.append("helloB1", {
				encryption: {
					keypair: await X25519Keypair.create(),

					receiver: {
						meta: undefined,
						signatures: receiverKey.publicKey,
						payload: receiverKey.publicKey,
					},
				},
			});
			await log2.append("helloB2", {
				encryption: {
					keypair: await X25519Keypair.create(),

					receiver: {
						meta: undefined,
						signatures: receiverKey.publicKey,
						payload: receiverKey.publicKey,
					},
				},
			});

			// Remove decrypted caches of the log2 values
			((await log2.toArray()) as EntryV0<any>[]).forEach((value) => {
				value._meta.clear();
				value._payload.clear();
				value._signatures!.signatures.forEach((signature) => signature.clear());
			});

			await log1.join(log2);
			expect(log1.length).equal(4);
			const item = last(await log1.toArray());
			expect((await item.getNext()).length).equal(1);
		});
	});

	describe("load", () => {
		let blocks: AnyStore, level: AnyStore, indices: Indices, log: Log<any>;
		afterEach(async () => {
			await log?.close();
			await blocks?.close();
			await level?.close();
			await indices?.stop();
		});

		it("loads encrypted entries", async () => {
			let rootDir =
				"./tmp/log/encryption/load/loads-encrypted-entries/" + +new Date();
			level = createStore(path.resolve(rootDir, "level"));

			store = new AnyBlockStore(await level.sublevel("blocks"));
			await store.start();

			const encryptioKey = await X25519Keypair.create();
			const signingKey = await Ed25519Keypair.create();

			log = new Log();

			blocks = await level.sublevel("cache");
			indices = await create(path.resolve(rootDir, "indices"));

			const logOptions = {
				keychain: await createKeychain(signingKey, encryptioKey),
				storage: blocks,
				encoding: JSON_ENCODING,
				indexer: indices,
			};

			await log.open(store, signKey, logOptions);

			await log.append("helloA1", {
				encryption: {
					keypair: encryptioKey,
					receiver: {
						meta: encryptioKey.publicKey,
						signatures: encryptioKey.publicKey,
						payload: encryptioKey.publicKey,
					},
				},
			});
			expect(log.length).equal(1);
			await log.close();
			log = new Log({ id: log.id });
			await log.open(store, signKey, logOptions);
			/* 		
			TODO already loaded (expected (?))
			expect(log.length).equal(0);
			await log.load();
				*/
			expect(log.length).equal(1);
		});
	});
});
