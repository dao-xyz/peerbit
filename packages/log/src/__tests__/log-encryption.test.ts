import { Log } from "../log.js";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	Keychain,
	X25519Keypair,
	X25519PublicKey,
} from "@peerbit/crypto";
import { BlockStore, MemoryLevelBlockStore } from "@peerbit/blocks";
import { signKey, signKey2 } from "./fixtures/privateKey.js";

const last = <T>(arr: T[]): T => {
	return arr[arr.length - 1];
};

describe("Log - Encryption", function () {
	let senderKey: X25519Keypair, recieverKey: X25519Keypair, store: BlockStore;

	beforeAll(async () => {
		senderKey = await X25519Keypair.create();
		recieverKey = await X25519Keypair.create();

		// The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
		store = new MemoryLevelBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});

	describe("join", () => {
		let log1: Log<string>, log2: Log<string>;

		beforeEach(async () => {
			const logOptions = {
				keychain: {
					exportById: () => {
						throw new Error("Not implemented");
					},
					exportByKey: <T extends Ed25519PublicKey | X25519PublicKey, Q>(
						key: T
					) => {
						if (key.equals(signKey.publicKey)) {
							return signKey as Q;
						}
						if (key.equals(senderKey.publicKey)) {
							return senderKey as Q;
						}
						if (key.equals(recieverKey.publicKey)) {
							return recieverKey as Q;
						}
						throw new Error("Not implemented");
					},
					import: () => {
						throw new Error("Not implemented");
					},
				} as Keychain,
			};

			log1 = new Log();
			await log1.open(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				logOptions
			);
			log2 = new Log();
			await log2.open(
				store,
				{
					...signKey2,
					sign: async (data: Uint8Array) => await signKey2.sign(data),
				},
				logOptions
			);
		});

		it("can encrypt signatures with particular reciever", async () => {
			// dummy signer
			const extraSigner = await Ed25519Keypair.create();
			const extraSigner2 = await Ed25519Keypair.create();

			await log2.append("helloA1", {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						metadata: undefined,
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
						next: recieverKey.publicKey,
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
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						metadata: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
						next: recieverKey.publicKey,
					},
				},
			});
			await log1.append("helloA2", {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						metadata: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
						next: recieverKey.publicKey,
					},
				},
			});
			await log2.append("helloB1", {
				encryption: {
					keypair: await X25519Keypair.create(),

					reciever: {
						metadata: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
						next: recieverKey.publicKey,
					},
				},
			});
			await log2.append("helloB2", {
				encryption: {
					keypair: await X25519Keypair.create(),

					reciever: {
						metadata: undefined,
						signatures: recieverKey.publicKey,
						payload: recieverKey.publicKey,
						next: recieverKey.publicKey,
					},
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
