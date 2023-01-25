import { Store, DefaultOptions, HeadsCache } from "../store.js";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Entry } from "@dao-xyz/peerbit-log";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";


const checkHashes = async (
	store: Store<any>,
	headsPath: string,
	hashes: string[][]
) => {
	await store.idle();
	let cachePath = await store._cache.get<string>(headsPath);
	let nextPath = cachePath!;
	let ret: string[] = [];
	if (hashes.length > 0) {
		for (let i = 0; i < hashes.length; i++) {
			ret.push(nextPath);
			let headCache = await store._cache.getBinary(nextPath!, HeadsCache);
			expect(headCache?.heads).toContainAllValues(hashes[i]);
			if (i === hashes.length - 1) {
				expect(headCache?.last).toBeUndefined();
			} else {
				expect(headCache?.last).toBeDefined();
				nextPath = headCache?.last!;
			}
		}
	} else {
		if (cachePath) {
			expect(
				await store._cache.getBinary(cachePath, HeadsCache)
			).toBeUndefined();
		}
	}

	return ret;
};

// Test utils
import { createStore } from "@dao-xyz/peerbit-test-utils";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
describe(`load`, function () {
	let blockStore: BlockStore,
		signKey: KeyWithMeta<Ed25519Keypair>,
		identityStore: AbstractLevel<any, string, Uint8Array>,
		store: Store<any>,
		cacheStore: AbstractLevel<any, string, Uint8Array>;
	let index: SimpleIndex<string>;

	beforeAll(async () => {
		identityStore = await createStore();

		const keystore = new Keystore(identityStore);

		signKey = await keystore.createEd25519Key();

		blockStore = new MemoryLevelBlockStore();
		await blockStore.open();

		cacheStore = await createStore();
	});

	afterAll(async () => {
		await store?.close();
		await blockStore?.close();
		await identityStore?.close();
		await cacheStore?.close();
	});

	beforeEach(async () => {
		await cacheStore.clear();
	});

	it("closes and loads", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
			}
		);

		const data = { data: 12345 };
		await store.addOperation(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);

		await store.close();
		await store.load();
		expect(store.oplog.values.length).toEqual(1);
	});

	it("loads when missing cache", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
			}
		);

		const data = { data: 12345 };
		await store.addOperation(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);

		await store.close();
		await store._cache.del(store.headsPath);
		await store.load();
		expect(store.oplog.values.length).toEqual(0);
	});

	it("loads when corrupt cache", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
				cacheId: "id",
			}
		);

		const data = { data: 12345 };
		await store.addOperation(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);

		await store.close();
		const headsPath = (await store._cache.get<string>(store.headsPath))!;
		await store._cache.set(headsPath, new Uint8Array([255]));
		await expect(() => store.load()).rejects.toThrowError();
	});

	it("will respect deleted heads", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
				cacheId: "id",
			}
		);

		const { entry: e1 } = await store.addOperation(
			{ data: 1 },
			{ nexts: [] }
		);
		const { entry: e2 } = await store.addOperation(
			{ data: 2 },
			{ nexts: [] }
		);
		const { entry: e3 } = await store.addOperation(
			{ data: 3 },
			{ nexts: [] }
		);

		expect(await store.getCachedHeads()).toContainAllValues([
			e1.hash,
			e2.hash,
			e3.hash,
		]);

		// Remove e1
		await store.removeOperation(e1);
		expect(await store.getCachedHeads()).toContainAllValues([
			e2.hash,
			e3.hash,
		]);

		/// Check that memeory is correctly stored
		await checkHashes(store, store.headsPath, [
			[e3.hash],
			[e2.hash],
			[e1.hash],
		]);
		await checkHashes(store, store.removedHeadsPath, [[e1.hash]]);

		// Remove e2
		await store.removeOperation(e2);
		expect(await store.getCachedHeads()).toContainAllValues([e3.hash]);

		/// Check that memeory is correctly stored
		const addedCacheKeys = await checkHashes(store, store.headsPath, [
			[e3.hash],
		]);
		const removedCacheKeys = await checkHashes(
			store,
			store.removedHeadsPath,
			[]
		);

		// Remove e3 (now cache should reset because there are no more heads)
		await store.removeOperation(e3);
		expect(await store.getCachedHeads()).toContainAllValues([]);

		/// Check that memeory is correctly stored
		await checkHashes(store, store.headsPath, []);
		await checkHashes(store, store.removedHeadsPath, []);

		for (const key of [...addedCacheKeys, ...removedCacheKeys]) {
			expect(
				await store._cache.getBinary(key, HeadsCache)
			).toBeUndefined();
		}
	});

	it("resets heads eventually", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
				trim: {
					to: 3,
				},
			}
		);
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push(
				(await store.addOperation({ data: i }, { nexts: [] })).entry
			);
		}
		const cachedHeads = await store.getCachedHeads();
		expect(cachedHeads).toContainAllValues(
			[
				entries[entries.length - 3],
				entries[entries.length - 2],
				entries[entries.length - 1],
			].map((x) => x!.hash)
		);

		// Since we have added 6 entries, we should have removed 3 entries, this means that removed >= added, which means the heads should reset
		await checkHashes(store, store.headsPath, [cachedHeads]);
	});

	it("resets heads when referencing all", async () => {
		const cache = new Cache(cacheStore);
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) =>
					await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
			}
		);
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 3; i++) {
			entries.push(
				(await store.addOperation({ data: i }, { nexts: [] })).entry
			);
		}
		expect(await store.getCachedHeads()).toHaveLength(3);
		const e4 = (await store.addOperation({ data: 4 }, { nexts: entries }))
			.entry;
		expect(await store.getCachedHeads()).toHaveLength(1);
		await checkHashes(store, store.headsPath, [[e4.hash]]);
		await checkHashes(store, store.removedHeadsPath, []);
	});
});
