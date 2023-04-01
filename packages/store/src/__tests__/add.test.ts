import assert from "assert";
import { Store, DefaultOptions } from "../store.js";
import { default as Cache } from "@dao-xyz/lazy-level";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Entry } from "@dao-xyz/peerbit-log";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";

// Test utils
import { createStore } from "@dao-xyz/peerbit-test-utils";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
describe(`append`, function () {
	let blockStore: BlockStore,
		signKey: KeyWithMeta<Ed25519Keypair>,
		identityStore: AbstractLevel<any, string, Uint8Array>,
		store: Store<any>,
		cacheStore: AbstractLevel<any, string, Uint8Array>;
	let index: SimpleIndex<string>;

	beforeEach(async () => {
		identityStore = await createStore();
		const keystore = new Keystore(identityStore);
		signKey = await keystore.createEd25519Key();
		blockStore = new MemoryLevelBlockStore();
		await blockStore.open();
		cacheStore = await createStore();
	});

	afterEach(async () => {
		await store?.close();
		await blockStore?.close();
		await identityStore?.close();
		await cacheStore?.close();
	});

	it("adds an operation and triggers the write event", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		const onWrite = async (store: Store<any>, entry: Entry<any>) => {
			const heads = await store.oplog.getHeads();
			expect(heads.length).toEqual(1);
			assert.deepStrictEqual(entry.payload.getValue(), data);
			assert.deepStrictEqual(index._index, heads);
			store.getCachedHeads().then((localHeads) => {
				if (!localHeads) {
					fail();
				}
				assert.deepStrictEqual(localHeads[0], entry.hash);
				expect(localHeads[0]).toEqual(heads[0].hash);
				expect(heads.length).toEqual(1);
				expect(localHeads.length).toEqual(1);
				done = true;
			});
		};

		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: onWrite,
			}
		);

		const data = { data: 12345 };

		await store.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);
	});

	it("adds multiple operations and triggers multiple write events", async () => {
		const writes = 3;
		let eventsFired = 0;

		const cache = new Cache(cacheStore);
		let done = false;
		const onWrite = async (store: Store<any>, entry: Entry<any>) => {
			eventsFired++;
			if (eventsFired === writes) {
				const heads = await store.oplog.getHeads();
				expect(heads.length).toEqual(1);
				expect(index._index.length).toEqual(writes);
				store.getCachedHeads().then((localHeads) => {
					if (!localHeads) {
						fail();
					}
					expect(localHeads).toHaveLength(1);
					expect(localHeads[0]).toEqual(index._index[2].hash);
					done = true;
				});
			}
		};

		store = new Store({ storeIndex: 1 });
		index = new SimpleIndex(store);
		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: onWrite,
			}
		);
		await store.load();

		for (let i = 0; i < writes; i++) {
			await store.append({ step: i });
		}

		await waitFor(() => done);
	});

	it("adds multiple operations concurrently", async () => {
		const writes = 100;
		let eventsFired = 0;

		const cache = new Cache(cacheStore);
		let done = false;
		const onWrite = async (store: Store<any>, entry: Entry<any>) => {
			eventsFired++;
			if (eventsFired === writes) {
				done = true;
			}
		};

		store = new Store({ storeIndex: 1 });
		index = new SimpleIndex(store);
		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: onWrite,
			}
		);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < writes; i++) {
			promises.push(store.append({ step: i }, { nexts: [] }));
		}
		await Promise.all(promises);
		await waitFor(() => done);
		expect(store.oplog.values.length).toEqual(writes);
	});

	it("can add as unique heads", async () => {
		const writes = 3;
		let eventsFired = 0;

		const cache = new Cache(cacheStore);
		let done = false;
		const allAddedEntries: string[] = [];
		const onWrite = async (store: Store<any>, entry: Entry<any>) => {
			eventsFired++;
			if (eventsFired === writes) {
				const heads = await store.oplog.getHeads();
				expect(heads.length).toEqual(3);
				expect(index._index.length).toEqual(writes);
				store.getCachedHeads().then((localHeads) => {
					if (!localHeads) {
						fail();
					}
					expect(localHeads).toContainAllValues(allAddedEntries);
					done = true;
				});
			}
		};

		store = new Store({ storeIndex: 1 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: onWrite,
			}
		);

		for (let i = 0; i < writes; i++) {
			allAddedEntries.push(
				(await store.append({ step: i }, { nexts: [] })).entry.hash
			);
		}

		await waitFor(() => done);
	});
});
