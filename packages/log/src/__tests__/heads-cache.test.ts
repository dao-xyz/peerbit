import {
	HeadsCache,
	CachePath,
	HeadsCacheToSerialize
} from "../heads-cache.js";
import { AnyStore, createStore } from "@peerbit/any-store";
import { AbstractLevel } from "abstract-level";
import { deserialize } from "@dao-xyz/borsh";
import { BlockStore, AnyBlockStore } from "@peerbit/blocks";
import { Log } from "../log.js";
import { Entry } from "../entry.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

const checkHashes = async (
	log: Log<any>,
	headsPath: string,
	hashes: string[][]
) => {
	await log.idle();
	const cacheBytes = await log.headsIndex.headsCache?.cache?.get(headsPath);

	let cachePath = cacheBytes && deserialize(cacheBytes, CachePath).path;
	let nextPath = cachePath!;
	let ret: string[] = [];
	if (hashes.length > 0) {
		for (let i = 0; i < hashes.length; i++) {
			ret.push(nextPath);
			const nextBytes = await log.headsIndex.headsCache?.cache?.get(nextPath!);
			let headCache =
				nextBytes && deserialize(nextBytes, HeadsCacheToSerialize);
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
			const bytes = await log.headsIndex.headsCache?.cache?.get(cachePath);
			expect(
				bytes && deserialize(bytes, HeadsCacheToSerialize)
			).toBeUndefined();
		}
	}

	return ret;
};

describe(`head-cache`, function () {
	let blockStore: BlockStore, identityStore: AnyStore, log: Log<any>;
	let queueCounter: number = 0;

	beforeEach(async () => {
		identityStore = await createStore();
		blockStore = new AnyBlockStore();
		await blockStore.start();
		queueCounter = 0;
	});

	afterEach(async () => {
		await log?.close();
		await blockStore?.stop();
		await identityStore?.close();
	});

	const init = async (cache: AnyStore) => {
		log = new Log();
		await log.open(blockStore, signKey, {
			cache,
			encoding: JSON_ENCODING
		});
		const queueFn = log.headsIndex.headsCache!.queue.bind(
			log.headsIndex.headsCache
		);
		log.headsIndex.headsCache!.queue = (change) => {
			queueCounter += 1;
			return queueFn(change);
		};
	};

	it("updates cached heads on write one head", async () => {
		const level = createStore();
		await init(level);
		const data = { data: 12345 };
		const { entry: e1 } = await log.append(data);
		expect(queueCounter).toEqual(1);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e1.hash]]);
		const { entry: e2 } = await log.append(data, { meta: { next: [e1] } });
		expect(queueCounter).toEqual(2);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e2.hash]]);
		expect((await log.headsIndex.headsCache?.getCachedHeads())?.length).toEqual(
			1
		);
	});

	it("updates cached heads on write multiple heads", async () => {
		const level = createStore();
		await init(level);
		const data = { data: 12345 };
		const { entry: e1 } = await log.append(data);
		expect(queueCounter).toEqual(1);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e1.hash]]);
		const { entry: e2 } = await log.append(data, { meta: { next: [] } });
		expect(queueCounter).toEqual(2);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [
			[e2.hash],
			[e1.hash]
		]);
		expect((await log.headsIndex.headsCache?.getCachedHeads())?.length).toEqual(
			2
		);
	});

	it("closes and loads", async () => {
		const level = createStore();
		await init(level);

		const data = { data: 12345 };
		await log.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		expect(log.closed).toBeFalse();
		await log.close();
		await init(level);
		expect(log.closed).toBeFalse();
		await log.load();
		expect(log.closed).toBeFalse();
		expect(log.values.length).toEqual(1);

		await log.close();
		expect(await level.status()).toEqual("closed");
	});

	it("loads when missing cache", async () => {
		const cache = await createStore();
		await init(cache);

		const data = { data: 12345 };
		await log.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await log.close();
		await cache.open();
		await cache.clear();
		await init(cache);
		await log.load();
		expect(log.values.length).toEqual(0);
		await log.close();
	});

	it("loads when corrupt cache", async () => {
		const cache = createStore();
		await init(cache);
		const data = { data: 12345 };
		await log.append(data).then((entry) => {
			expect(entry.entry).toBeInstanceOf(Entry);
		});

		await log.idle();
		const bytes = await log.headsIndex.headsCache?.cache?.get(
			log.headsIndex.headsCache?.headsPath
		);
		const headsPath = ((await bytes) && deserialize(bytes!, CachePath))?.path!;
		await log.headsIndex.headsCache?.cache?.put(
			headsPath,
			new Uint8Array([255])
		);
		await expect(() => log.load()).rejects.toThrowError();
	});

	it("will respect deleted heads", async () => {
		const cache = createStore();

		await init(cache);

		const { entry: e1 } = await log.append({ data: 1 }, { meta: { next: [] } });
		const { entry: e2 } = await log.append({ data: 2 }, { meta: { next: [] } });
		const { entry: e3 } = await log.append({ data: 3 }, { meta: { next: [] } });

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
			[e1.hash]
		]);
		await checkHashes(log, log.headsIndex.headsCache!.removedHeadsPath, [
			[e1.hash]
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
			const bytes = await log.headsIndex.headsCache?.cache?.get(key);
			expect(bytes && deserialize(bytes, HeadsCache)).toBeUndefined();
		}
	});

	it("resets heads eventually", async () => {
		const cache = createStore();
		log = new Log();
		await log.open(blockStore, signKey, {
			cache,
			trim: {
				type: "length",
				to: 3
			},
			encoding: JSON_ENCODING
		});
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push(
				(await log.append({ data: i }, { meta: { next: [] } })).entry
			);
		}
		const cachedHeads = await log.headsIndex.headsCache!.getCachedHeads();
		expect(cachedHeads).toContainAllValues(
			[
				entries[entries.length - 3],
				entries[entries.length - 2],
				entries[entries.length - 1]
			].map((x) => x!.hash)
		);

		// Since we have added 6 entries, we should have removed 3 entries, this means that removed >= added, which means the heads should reset
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [cachedHeads]);
	});

	it("resets heads on load", async () => {
		const cache = createStore();
		await init(cache);
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 6; i++) {
			entries.push(
				(await log.append({ data: i }, { meta: { next: [] } })).entry
			);
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
		await init(cache);
		await log.load();

		// Make sure that all hashes are in the first "file", since its reseted
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [
			entries.reverse().map((x) => x.hash)
		]);
	});

	it("can cache heads concurrently", async () => {
		const cache = createStore();
		await init(cache);
		await log.load();
		const entries: Promise<Entry<any>>[] = [];
		let entryCount = 100;
		for (let i = 0; i < entryCount; i++) {
			entries.push(
				log.append({ data: i }, { meta: { next: [] } }).then((x) => x.entry)
			);
		}
		await Promise.all(entries);
		expect(log.length).toEqual(entryCount);
		expect(await log.getHeads()).toHaveLength(entryCount);
		const cachedHeads = await log.headsIndex.headsCache!.getCachedHeads();
		expect(cachedHeads).toHaveLength(entryCount);
	});

	it("resets heads when referencing all", async () => {
		const cache = createStore();
		await init(cache);

		await log.load();
		const entries: Entry<any>[] = [];
		for (let i = 0; i < 3; i++) {
			entries.push(
				(await log.append({ data: i }, { meta: { next: [] } })).entry
			);
		}
		expect(await log.headsIndex.headsCache!.getCachedHeads()).toHaveLength(3);
		const e4 = (await log.append({ data: 4 }, { meta: { next: entries } }))
			.entry;
		expect(await log.headsIndex.headsCache!.getCachedHeads()).toHaveLength(1);
		await checkHashes(log, log.headsIndex.headsCache!.headsPath, [[e4.hash]]);
		await checkHashes(log, log.headsIndex.headsCache!.removedHeadsPath, []);
	});

	it("will load heads on write", async () => {
		const cache = createStore();
		await init(cache);

		await log.append({ data: 1 });
		expect(log.values.length).toEqual(1);
		await log.close();

		await init(cache);
		expect(log.values.length).toEqual(0);
		await log.append({ data: 2 });
		expect(log.values.length).toEqual(2);
	});
});
