import assert from "assert";
import {
	Store,
	DefaultOptions,
	IInitializationOptions,
} from "../store.js";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
	EncryptedThing,
	PublicKeyEncryptionResolver,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { AccessError } from "@dao-xyz/peerbit-crypto";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { AbstractLevel } from "abstract-level";

// Test utils
import { createStore } from "@dao-xyz/peerbit-test-utils";
import { Entry } from "@dao-xyz/peerbit-log";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { BlockStore, MemoryLevelBlockStore } from "@dao-xyz/libp2p-direct-block";

describe(`addOperation`, function () {
	let signKey: KeyWithMeta<Ed25519Keypair>,
		keystore: Keystore,
		identityStore: AbstractLevel<any, string, Uint8Array>,
		store: Store<any>,
		cacheStore: AbstractLevel<any, string, Uint8Array>,
		senderKey: KeyWithMeta<Ed25519Keypair>,
		recieverKey: KeyWithMeta<Ed25519Keypair>,
		encryption: PublicKeyEncryptionResolver,
		blockStore: BlockStore;
	let index: SimpleIndex<string>;

	beforeEach(async () => {
		identityStore = await createStore();
		cacheStore = await createStore();
		keystore = new Keystore(identityStore);
		signKey = await keystore.createEd25519Key();
		senderKey = await keystore.createEd25519Key();
		recieverKey = await keystore.createEd25519Key();
		encryption = {
			getEncryptionKeypair: () => senderKey.keypair,
			getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
				for (let i = 0; i < publicKeys.length; i++) {
					if (
						publicKeys[i].equals(
							await X25519PublicKey.from(
								senderKey.keypair.publicKey
							)
						)
					) {
						return {
							index: i,
							keypair: senderKey.keypair,
						};
					}
					if (
						publicKeys[i].equals(
							await X25519PublicKey.from(
								recieverKey.keypair.publicKey
							)
						)
					) {
						return {
							index: i,
							keypair: recieverKey.keypair,
						};
					}
				}
			},
		};
		blockStore = new MemoryLevelBlockStore();
		await blockStore.open();
	});

	afterEach(async () => {
		await store?.close();
		await identityStore?.close();
		await cacheStore?.close();
		await blockStore?.close();
	});

	it("encrypted entry is appended known key", async () => {
		const data = { data: 12345 };

		let done = false;
		const onWrite = async (store: Store<any>, entry: Entry<any>) => {
			try {
				const heads = store.oplog.heads;
				expect(heads.length).toEqual(1);
				assert.deepStrictEqual(entry.payload.getValue(), data);
				/*   expect(store.replicationStatus.progress).toEqual(1n);
				  expect(store.replicationStatus.max).toEqual(1n); */
				assert.deepStrictEqual(index._index, heads);
				await delay(5000); // seems because write is async?
				const localHeads = await store.getCachedHeads();
				if (!localHeads) {
					fail();
				}
				const firstHead = store.oplog.get(localHeads[0])!;
				(firstHead._payload as EncryptedThing<any>)._decrypted =
					undefined;
				firstHead.init({
					encryption: store.oplog._encryption,
					encoding: store.oplog._encoding,
				});
				await firstHead.getPayload();
				assert.deepStrictEqual(firstHead.payload.getValue(), data);
				assert(firstHead.equals(heads[0]));
				expect(heads.length).toEqual(1);
				expect(localHeads.length).toEqual(1);
				done = true;
			} catch (error) {
				throw error;
			}
		};
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		const cache = new Cache(cacheStore);
		const options: IInitializationOptions<any> = {
			...DefaultOptions,
			resolveCache: () => Promise.resolve(cache),
			onUpdate: index.updateIndex.bind(index),
			encryption,
			onWrite,
		};

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			options
		);

		await store.addOperation(data, {
			reciever: {
				metadata: undefined,
				next: recieverKey.keypair.publicKey,
				payload: recieverKey.keypair.publicKey,
				signatures: recieverKey.keypair.publicKey,
			},
		});

		await waitFor(() => done);
	});

	it("encrypted entry is append unkown key", async () => {
		const data = { data: 12345 };
		let done = false;

		const onWrite = async (store: Store<any>, entry: Entry<any>) => {
			const heads = store.oplog.heads;
			expect(heads.length).toEqual(1);
			assert.deepStrictEqual(entry.payload.getValue(), data);
			/* expect(store.replicationStatus.progress).toEqual(1n);
			expect(store.replicationStatus.max).toEqual(1n); */
			assert.deepStrictEqual(index._index, heads);
			const localHeads = await store.getCachedHeads();

			if (!localHeads) {
				fail();
			}

			const firstHead = store.oplog.get(localHeads[0])!;
			(firstHead._payload as EncryptedThing<any>)._decrypted = undefined;
			firstHead.init({
				encryption: store.oplog._encryption,
				encoding: store.oplog._encoding,
			});
			try {
				await firstHead.getPayload();
				assert(false);
			} catch (error) {
				if (error instanceof AccessError === false) {
					console.error(error);
				}
				expect(error).toBeInstanceOf(AccessError);
			}
			assert(firstHead.equals(heads[0]));
			expect(heads.length).toEqual(1);
			expect(localHeads.length).toEqual(1);
			done = true;
		};

		const cache = new Cache(cacheStore);
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		const options: IInitializationOptions<any> = {
			...DefaultOptions,
			resolveCache: () => Promise.resolve(cache),
			onUpdate: index.updateIndex.bind(index),
			encryption,
			onWrite,
		};

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			options
		);

		const reciever = await keystore.createEd25519Key();
		await store.addOperation(data, {
			reciever: {
				metadata: undefined,
				next: reciever.keypair.publicKey,
				payload: reciever.keypair.publicKey,
				signatures: reciever.keypair.publicKey,
			},
		});

		await waitFor(() => done);
	});
});
