import { Store, DefaultOptions, HeadsCache, CachePath } from "../store.js";
import { default as Cache } from "@dao-xyz/lazy-level";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Entry } from "@dao-xyz/peerbit-log";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";
import { deserialize } from "@dao-xyz/borsh";
// Test utils
import { createStore } from "@dao-xyz/peerbit-test-utils";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";

const checkHashes = async (
	store: Store<any>,
	headsPath: string,
	hashes: string[][]
) => {
	await store.idle();
	let cachePath = await store.cache
		.get(headsPath)
		.then((bytes) => bytes && deserialize(bytes, CachePath).path);
	let nextPath = cachePath!;
	let ret: string[] = [];
	if (hashes.length > 0) {
		for (let i = 0; i < hashes.length; i++) {
			ret.push(nextPath);
			let headCache = await store.cache
				.get(nextPath!)
				.then((bytes) => bytes && deserialize(bytes, HeadsCache));
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
				await store.cache
					.get(cachePath)
					.then((bytes) => bytes && deserialize(bytes, HeadsCache))
			).toBeUndefined();
		}
	}

	return ret;
};

describe(`load`, function () {
	let blockStore: BlockStore,
		signKey: KeyWithMeta<Ed25519Keypair>,
		identityStore: AbstractLevel<any, string, Uint8Array>,
		store: Store<any>;
	let index: SimpleIndex<string>;

	beforeEach(async () => {
		identityStore = await createStore();
		const keystore = new Keystore(identityStore);
		signKey = await keystore.createEd25519Key();
		blockStore = new MemoryLevelBlockStore();
		await blockStore.open();
	});

	afterEach(async () => {
		await store?.close();
		await blockStore?.close();
		await identityStore?.close();
	});

	it("closes and loads", async () => {
		let done = false;
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);
		const level = await createStore();
		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: async () => Promise.resolve(new Cache(level)),
				onUpdate: index.updateIndex.bind(index),
				onWrite: () => {
					done = true;
				},
			}
		);

		const data = { data: 12345 };
		await store.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);
		expect(store.initialized).toBeTrue();
		await store.close();
		expect(store.initialized).toBeTrue();
		await store.load();
		expect(store.initialized).toBeTrue();
		expect(store.oplog.values.length).toEqual(1);
	});

	it("loads when missing cache", async () => {
		const level = await createStore();
		const cache = new Cache(level);
		let done = false;
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
				onWrite: () => {
					done = true;
				},
			}
		);

		const data = { data: 12345 };
		await store.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);
		await store.close();
		await cache.open();
		await cache.del(store.headsPath);
		await store.load();
		expect(store.oplog.values.length).toEqual(0);
	});

	it("loads when corrupt cache", async () => {
		const cache = new Cache(await createStore());
		let done = false;
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
				onWrite: () => {
					done = true;
				},
				cacheId: "id",
			}
		);

		const data = { data: 12345 };
		await store.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);

		await store.idle();
		const headsPath = (
			await store.cache
				.get(store.headsPath)
				.then((bytes) => bytes && deserialize(bytes, CachePath))
		)?.path!;
		await store.cache.set(headsPath, new Uint8Array([255]));
		await expect(() => store.load()).rejects.toThrowError();
	});

	it("will respect deleted heads", async () => {
		const cache = new Cache(await createStore());
		let done = false;
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
				onWrite: () => {
					done = true;
				},
				cacheId: "id",
			}
		);

		const { entry: e1 } = await store.append({ data: 1 }, { nexts: [] });
		const { entry: e2 } = await store.append({ data: 2 }, { nexts: [] });
		const { entry: e3 } = await store.append({ data: 3 }, { nexts: [] });

		expect(await store.getCachedHeads()).toContainAllValues([
			e1.hash,
			e2.hash,
			e3.hash,
		]);

		// Remove e1
		await store.remove(e1);
		expect(await store.getCachedHeads()).toContainAllValues([e2.hash, e3.hash]);

		/// Check that memeory is correctly stored
		await checkHashes(store, store.headsPath, [
			[e3.hash],
			[e2.hash],
			[e1.hash],
		]);
		await checkHashes(store, store.removedHeadsPath, [[e1.hash]]);

		// Remove e2
		await store.remove(e2);
		expect(await store.getCachedHeads()).toContainAllValues([e3.hash]);

		/// Check that memory is correctly stored
		const addedCacheKeys = await checkHashes(store, store.headsPath, [
			[e3.hash],
		]);
		const removedCacheKeys = await checkHashes(
			store,
			store.removedHeadsPath,
			[]
		);

		// Remove e3 (now cache should reset because there are no more heads)
		await store.remove(e3);
		expect(await store.getCachedHeads()).toContainAllValues([]);

		/// Check that memeory is correctly stored
		await checkHashes(store, store.headsPath, []);
		await checkHashes(store, store.removedHeadsPath, []);

		for (const key of [...addedCacheKeys, ...removedCacheKeys]) {
			expect(
				await store.cache
					.get(key)
					.then((bytes) => bytes && deserialize(bytes, HeadsCache))
			).toBeUndefined();
		}
	});

	it("resets heads eventually", async () => {
		const cache = new Cache(await createStore());
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
				trim: {
					type: "length",
					to: 3,
				},
			}
		);
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push((await store.append({ data: i }, { nexts: [] })).entry);
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

	it("resets heads on load", async () => {
		const cache = new Cache(await createStore());
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
			}
		);
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push((await store.append({ data: i }, { nexts: [] })).entry);
		}
		const cachedHeads = await store.getCachedHeads();
		expect(cachedHeads).toContainAllValues(entries.map((x) => x!.hash));

		// Make sure that all hashes, one in each file
		await checkHashes(
			store,
			store.headsPath,
			entries.reverse().map((x) => [x.hash])
		);

		await store.close();
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
			}
		);
		await store.load();

		// Make sure that all hashes are in the first "file", since its reseted
		await checkHashes(store, store.headsPath, [
			entries.reverse().map((x) => x.hash),
		]);
	});

	it("can cache heads concurrently", async () => {
		const cache = new Cache(await createStore());
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
			}
		);
		await store.load();
		const entries: Promise<Entry<any>>[] = [];
		let entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			entries.push(
				store.append({ data: i }, { nexts: [] }).then((x) => x.entry)
			);
		}
		await Promise.all(entries);
		expect(store.oplog.length).toEqual(entryCount);
		expect(await store.oplog.getHeads()).toHaveLength(entryCount);
		const cachedHeads = await store.getCachedHeads();
		expect(cachedHeads).toHaveLength(entryCount);
	});

	it("resets heads when referencing all", async () => {
		const cache = new Cache(await createStore());
		let done = false;
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
				onWrite: () => {
					done = true;
				},
			}
		);
		await store.load();
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 3; i++) {
			entries.push((await store.append({ data: i }, { nexts: [] })).entry);
		}
		expect(await store.getCachedHeads()).toHaveLength(3);
		const e4 = (await store.append({ data: 4 }, { nexts: entries })).entry;
		expect(await store.getCachedHeads()).toHaveLength(1);
		await checkHashes(store, store.headsPath, [[e4.hash]]);
		await checkHashes(store, store.removedHeadsPath, []);
	});

	it("will load heads on write", async () => {
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);
		const level = await createStore();
		const cache = new Cache(await createStore());

		await store.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				resolveCache: async () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
			}
		);

		await store.append({ data: 1 });
		expect(store.oplog.values.length).toEqual(1);
		await store.close();

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
				resolveCache: async () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
			}
		);

		await store.append({ data: 2 });
		expect(store.oplog.values.length).toEqual(2);
	});
});
