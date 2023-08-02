import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { X25519Keypair, X25519PublicKey } from "@peerbit/crypto";
import { SharedLog } from "..";
import { LSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForResolved } from "@peerbit/time";

@variant("encrypt_store")
class SimpleStore extends Program {
	@field({ type: SharedLog })
	log: SharedLog<Uint8Array>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.log = new SharedLog();
	}

	async open(): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.log.open();
	}
}

describe("encryption", () => {
	describe("replicate", () => {
		let session: LSession;
		// This class extends Program which allows it to be replicated amongst peers

		beforeEach(async () => {
			session = await LSession.connected(3);
		});

		afterEach(async () => {
			await session.stop();
		});

		it("encrypts", async () => {
			const [client, client2, client3] = session.peers;

			const store = await client.open(new SimpleStore());
			expect(store.log.log.keychain).toBeDefined();

			await store.log.append(new Uint8Array([1]), {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						// Who can read the log entry metadata (e.g. timestamps)
						meta: [
							client.identity.publicKey,
							client2.identity.publicKey,
							client3.identity.publicKey,
						],

						// Who can read the message?
						payload: [client.identity.publicKey, client2.identity.publicKey],

						// Who can read the signature ?
						// (In order to validate entries you need to be able to read the signature)
						signatures: [
							client.identity.publicKey,
							client2.identity.publicKey,
							client3.identity.publicKey,
						],

						// Omitting any of the fields below will make it unencrypted
					},
				},
			});

			// A peer that can open
			const store2 = await client2.open<SimpleStore>(store.address!);
			await waitForResolved(() => expect(store2.log.log.length).toEqual(1));
			const entry = (await store2.log.log.values.toArray())[0];

			// use .getPayload() instead of .payload to decrypt the payload
			expect((await entry.getPayload()).getValue()).toEqual(
				new Uint8Array([1])
			);
		});
	});

	describe("load", () => {
		let session: LSession;
		// This class extends Program which allows it to be replicated amongst peers

		beforeEach(async () => {
			session = await LSession.connected(2, [
				{ directory: "./tmp/shared-log/access-error/1" + +new Date() },
				{ directory: "./tmp/shared-log/access-error/2" + +new Date() },
			]);
		});

		afterEach(async () => {
			await session.stop();
		});

		it("encrypted", async () => {
			// TODO move this test to shared log
			const [client] = session.peers;
			let store = new SimpleStore();
			await client.open(store);
			await store.log.append(new Uint8Array([1]), {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						meta: [client.identity.publicKey],
						signatures: [client.identity.publicKey],
						payload: [await X25519PublicKey.create()],
					},
				},
			});

			expect(store.log.log.values.length).toEqual(1);
			await store.close();
			store = await client.open(store.clone());
			expect(store.log.log.values.length).toEqual(1);
			await store.close();
		});

		it("handles AccessError gracefully", async () => {
			// TODO move this test to shared log
			const [client, client2] = session.peers;
			let store = new SimpleStore();
			await client.open(store);
			await store.log.append(new Uint8Array([1]), {
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						meta: [client.identity.publicKey],
						signatures: [client.identity.publicKey],
						payload: [await X25519PublicKey.create()],
					},
				},
			});

			expect(store.log.log.values.length).toEqual(1);
			await store.close();
			store = await client2.open(store.clone());
			expect(store.log.log.values.length).toEqual(0);
			await store.close();
		});
	});
});
