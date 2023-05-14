import {
	HeadsCache,
	CachePath,
	HeadsCacheToSerialize,
} from "../heads-cache.js";
import { default as LazyLevel } from "@dao-xyz/lazy-level";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";
import { deserialize } from "@dao-xyz/borsh";
import { createStore } from "@dao-xyz/peerbit-test-utils";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { Log } from "../log.js";
import { Entry } from "../entry.js";

const checkHashes = async (
	log: Log<any>,
	headsPath: string,
	hashes: string[][]
) => {
	await log.idle();
	let cachePath = await log.headsIndex.headsCache?.cache
		.get(headsPath)
		.then((bytes) => bytes && deserialize(bytes, CachePath).path);
	let nextPath = cachePath!;
	let ret: string[] = [];
	if (hashes.length > 0) {
		for (let i = 0; i < hashes.length; i++) {
			ret.push(nextPath);
			let headCache = await log.headsIndex.headsCache?.cache
				.get(nextPath!)
				.then((bytes) => bytes && deserialize(bytes, HeadsCacheToSerialize));
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
				await log.headsIndex.headsCache?.cache
					.get(cachePath)
					.then((bytes) => bytes && deserialize(bytes, HeadsCacheToSerialize))
			).toBeUndefined();
		}
	}

	return ret;
};

describe(`load`, function () {
	let blockStore: BlockStore,
		signKey: KeyWithMeta<Ed25519Keypair>,
		identityStore: AbstractLevel<any, string, Uint8Array>,
		log: Log<any>;

	beforeEach(async () => {
		identityStore = await createStore();
		const keystore = new Keystore(identityStore);
		signKey = await keystore.createEd25519Key();
		blockStore = new MemoryLevelBlockStore();
		await blockStore.open();
	});

	afterEach(async () => {
		await log?.close();
		await blockStore?.close();
		await identityStore?.close();
	});

	const init = async (cache: LazyLevel, onWrite: () => void) => {
		log = new Log();
		await log.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				cache: () => Promise.resolve(cache),
				onWrite: onWrite,
			}
		);
	};

	it("updates cached heads on write one head", async () => {
		const level = new LazyLevel(await createStore());
		await init(level, () => {});
		const data = { data: 12345 };
		const { entry: e1 } = await log.append(data);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e1.hash]]);
		const { entry: e2 } = await log.append(data, { nexts: [e1] });
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e2.hash]]);
		expect((await log.headsIndex.headsCache?.getCachedHeads())?.length).toEqual(
			1
		);
	});

	it("updates cached heads on write multiple heads", async () => {
		const level = new LazyLevel(await createStore());
		await init(level, () => {});
		const data = { data: 12345 };
		const { entry: e1 } = await log.append(data);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e1.hash]]);
		const { entry: e2 } = await log.append(data, { nexts: [] });
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [
			[e2.hash],
			[e1.hash],
		]);
		expect((await log.headsIndex.headsCache?.getCachedHeads())?.length).toEqual(
			2
		);
	});

	it("closes and loads", async () => {
		let done = false;
		const level = new LazyLevel(await createStore());
		await init(level, () => {
			done = true;
		});

		const data = { data: 12345 };
		await log.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);
		expect(log.initialized).toBeTrue();
		await log.close();
		await init(level, () => {
			done = true;
		});
		expect(log.initialized).toBeTrue();
		await log.load();
		expect(log.initialized).toBeTrue();
		expect(log.values.length).toEqual(1);
	});

	it("loads when missing cache", async () => {
		const level = await createStore();
		const cache = new LazyLevel(level);
		let done = false;
		await init(cache, () => {
			done = true;
		});

		const data = { data: 12345 };
		await log.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);
		await log.close();
		await cache.open();
		await cache.del(log.headsIndex.headsCache!.headsPath); // delete head from cache, so with next load, it should not exist
		await init(cache, () => {
			done = true;
		});
		await log.load();
		expect(log.values.length).toEqual(0);
	});

	it("loads when corrupt cache", async () => {
		const cache = new LazyLevel(await createStore());
		let done = false;
		await init(cache, () => {
			done = true;
		});
		const data = { data: 12345 };
		await log.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await waitFor(() => done);

		await log.idle();
		const headsPath = (
			await log.headsIndex.headsCache?.cache
				.get(log.headsIndex.headsCache.headsPath)
				.then((bytes) => bytes && deserialize(bytes, CachePath))
		)?.path!;
		await log.headsIndex.headsCache?.cache.set(
			headsPath,
			new Uint8Array([255])
		);
		await log.headsIndex.headsCache?.cache.idle();
		await expect(() => log.load()).rejects.toThrowError();
	});

	it("will respect deleted heads", async () => {
		const cache = new LazyLevel(await createStore());
		let done = false;

		await init(cache, () => {
			done = true;
		});

		const { entry: e1 } = await log.append({ data: 1 }, { nexts: [] });
		const { entry: e2 } = await log.append({ data: 2 }, { nexts: [] });
		const { entry: e3 } = await log.append({ data: 3 }, { nexts: [] });

		expect(
			await log.headsIndex.headsCache!.getCachedHeads()
		).toContainAllValues([e1.hash, e2.hash, e3.hash]);

		// Remove e1
		await log.remove(e1);
		expect(
			await log.headsIndex.headsCache!.getCachedHeads()
		).toContainAllValues([e2.hash, e3.hash]);

		/// Check that memeory is correctly stored
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [
			[e3.hash],
			[e2.hash],
			[e1.hash],
		]);
		await checkHashes(log, log.headsIndex.headsCache!.removedHeadsPath, [
			[e1.hash],
		]);

		// Remove e2
		await log.remove(e2);
		expect(
			await log.headsIndex.headsCache!.getCachedHeads()
		).toContainAllValues([e3.hash]);

		/// Check that memory is correctly stored
		const addedCacheKeys = await checkHashes(
			log,
			log.headsIndex.headsCache!.headsPath,
			[[e3.hash]]
		);
		const removedCacheKeys = await checkHashes(
			log,
			log.headsIndex.headsCache!.removedHeadsPath,
			[]
		);

		// Remove e3 (now cache should reset because there are no more heads)
		await log.remove(e3);
		expect(
			await log.headsIndex.headsCache!.getCachedHeads()
		).toContainAllValues([]);

		/// Check that memeory is correctly stored
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, []);
		await checkHashes(log, log.headsIndex.headsCache!.removedHeadsPath, []);

		for (const key of [...addedCacheKeys, ...removedCacheKeys]) {
			expect(
				await log.headsIndex.headsCache?.cache
					.get(key)
					.then((bytes) => bytes && deserialize(bytes, HeadsCache))
			).toBeUndefined();
		}
	});

	it("resets heads eventually", async () => {
		const cache = new LazyLevel(await createStore());
		log = new Log();
		await log.init(
			blockStore,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				cache: () => Promise.resolve(cache),
				trim: {
					type: "length",
					to: 3,
				},
			}
		);
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push((await log.append({ data: i }, { nexts: [] })).entry);
		}
		const cachedHeads = await log.headsIndex.headsCache!.getCachedHeads();
		expect(cachedHeads).toContainAllValues(
			[
				entries[entries.length - 3],
				entries[entries.length - 2],
				entries[entries.length - 1],
			].map((x) => x!.hash)
		);

		// Since we have added 6 entries, we should have removed 3 entries, this means that removed >= added, which means the heads should reset
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [cachedHeads]);
	});

	it("resets heads on load", async () => {
		const cache = new LazyLevel(await createStore());
		await init(cache, () => {});
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push((await log.append({ data: i }, { nexts: [] })).entry);
		}
		const cachedHeads = await log.headsIndex.headsCache!.getCachedHeads();
		expect(cachedHeads).toContainAllValues(entries.map((x) => x!.hash));

		// Make sure that all hashes, one in each file
		await checkHashes(
			log,
			log.headsIndex.headsCache!.headsPath,
			entries.reverse().map((x) => [x.hash])
		);

		await log.close();
		await init(cache, () => {});
		await log.load();

		// Make sure that all hashes are in the first "file", since its reseted
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [
			entries.reverse().map((x) => x.hash),
		]);
	});

	it("can cache heads concurrently", async () => {
		const cache = new LazyLevel(await createStore());
		await init(cache, () => {});
		await log.load();
		const entries: Promise<Entry<any>>[] = [];
		let entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			entries.push(log.append({ data: i }, { nexts: [] }).then((x) => x.entry));
		}
		await Promise.all(entries);
		expect(log.length).toEqual(entryCount);
		expect(await log.getHeads()).toHaveLength(entryCount);
		const cachedHeads = await log.headsIndex.headsCache!.getCachedHeads();
		expect(cachedHeads).toHaveLength(entryCount);
	});

	it("resets heads when referencing all", async () => {
		const cache = new LazyLevel(await createStore());
		let done = false;
		await init(cache, () => {
			done = true;
		});

		await log.load();
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 3; i++) {
			entries.push((await log.append({ data: i }, { nexts: [] })).entry);
		}
		expect(await log.headsIndex.headsCache!.getCachedHeads()).toHaveLength(3);
		const e4 = (await log.append({ data: 4 }, { nexts: entries })).entry;
		expect(await log.headsIndex.headsCache!.getCachedHeads()).toHaveLength(1);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e4.hash]]);
		await checkHashes(log, log.headsIndex.headsCache!.removedHeadsPath, []);
	});

	it("will load heads on write", async () => {
		const cache = new LazyLevel(await createStore());
		await init(cache, () => {});

		await log.append({ data: 1 });
		expect(log.values.length).toEqual(1);
		await log.close();

		await init(cache, () => {});
		expect(log.values.length).toEqual(0);
		await log.append({ data: 2 });
		expect(log.values.length).toEqual(2);
	});
});
