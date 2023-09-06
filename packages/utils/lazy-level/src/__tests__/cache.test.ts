import LazyLevel from "../index.js";
import assert from "assert";
import { MemoryLevel } from "memory-level";
import crypto from "crypto";
import { waitFor, waitForResolved } from "@peerbit/time";

export const createStore = (): MemoryLevel => {
	return new MemoryLevel({ valueEncoding: "view" });
};

describe(`LazyLevel - level`, function () {
	const data = [{ key: "boolean", value: true }];

	describe("sequential", () => {
		let cache: LazyLevel, store: MemoryLevel;
		beforeAll(async () => {
			store = await createStore();
			cache = new LazyLevel(store, { batch: false });
			await cache.open();
		});

		afterAll(async () => {
			await cache.close();
		});

		it(`set, get, delete`, async () => {
			for (const d of data) {
				await cache.put(d.key, Buffer.from(JSON.stringify(d.value)));
				const val = await cache.get(d.key);
				expect(new Uint8Array(val!)).toEqual(
					new Uint8Array(Buffer.from(JSON.stringify(d.value)))
				);
				try {
					await cache.get("fooKey");
				} catch (e: any) {
					fail();
				}

				await cache.put(d.key, Buffer.from(JSON.stringify(d.value)));
				await cache.del(d.key);
				try {
					await store.get(d.key);
					fail();
				} catch (e: any) {
					assert(true);
				}

				try {
					await cache.del("fooKey");
					fail();
				} catch (e: any) {
					assert(true);
				}
			}
		});
		/*  TODO feat (?)
		
		it("delete by prefix", async () => {
				await cache.put("a", crypto.randomBytes(8));
				await cache.put("ab", crypto.randomBytes(8));
				await cache.put("abc", crypto.randomBytes(8));
	
				await cache.deleteByPrefix("a");
				expect(await cache.get("a")).toBeUndefined();
				expect(await cache.get("ab")).toBeUndefined();
				expect(await cache.get("abc")).toBeUndefined();
				await cache.idle();
				expect(await cache.get("a")).toBeUndefined();
				expect(await cache.get("ab")).toBeUndefined();
				expect(await cache.get("abc")).toBeUndefined();
			}); 
			*/
	});

	describe("batched", () => {
		let cache: LazyLevel, store: MemoryLevel;

		let interval = 1000;

		let MAX_CACHE_SIZE = 1000;
		let MAX_QUEUE_SIZE = 1000;

		beforeEach(async () => {
			store = await createStore();
			cache = new LazyLevel(store, {
				batch: {
					interval: interval,
					queueMaxBytes: MAX_QUEUE_SIZE
				}
			});
			await cache.open();
		});

		afterEach(async () => {
			await cache.close();
		});

		it(`set, get, delete`, async () => {
			for (const d of data) {
				await cache.put(d.key, Buffer.from(JSON.stringify(d.value)));
				const val = await cache.get(d.key);
				expect(new Uint8Array(val!)).toEqual(
					new Uint8Array(Buffer.from(JSON.stringify(d.value)))
				);
				await cache.idle();
				await waitForResolved(() =>
					expect(cache.txQueue!.tempStore.size).toEqual(0)
				);
				expect(new Uint8Array(val!)).toEqual(
					new Uint8Array(Buffer.from(JSON.stringify(d.value)))
				);
				try {
					await cache.get("fooKey");
				} catch (e: any) {
					fail();
				}

				await cache.put(d.key, Buffer.from(JSON.stringify(d.value)));
				await cache.del(d.key);
				try {
					await store.get(d.key);
					fail();
				} catch (e: any) {
					assert(true);
				}

				try {
					await cache.del("fooKey");
					fail();
				} catch (e: any) {
					assert(true);
				}
			}
		});

		it("put many", async () => {
			let putSize = 8;
			let putCount = 100;
			for (let i = 0; i < putCount; i++) {
				await cache.put(String(i), crypto.randomBytes(putSize));
			}
			expect(cache.txQueue?.tempStore.size).toEqual(putCount);
			await cache.idle();
			await waitForResolved(() =>
				expect(cache.txQueue?.tempStore.size).toEqual(0)
			);
			await waitForResolved(() =>
				expect(cache.txQueue?.currentSize).toEqual(0)
			);
			for (let i = 0; i < 100; i++) {
				expect(await cache.get(String(i))).toBeDefined();
			}
		});

		it("delete", async () => {
			const key = "2";
			await cache.put(key, crypto.randomBytes(8));
			await cache.delAll([key]);
			expect(await cache.get(key)).toBeUndefined();
			await cache.idle();
			expect(await cache.get(key)).toBeUndefined();
			await waitFor(() => cache.txQueue?.tempDeleted.size === 0);
			await waitFor(() => cache.txQueue?.tempStore.size === 0);
			await waitForResolved(() =>
				expect(cache.txQueue?.currentSize).toEqual(0)
			);
		});

		it("put delete put", async () => {
			const key = "";
			cache.put(key, new Uint8Array([0]));
			cache.delAll([key]);
			cache.put(key, new Uint8Array([1]));
			expect(await cache.get(key)).toEqual(new Uint8Array([1]));
			await cache.idle();
			expect(new Uint8Array((await cache.get(key))!)).toEqual(
				new Uint8Array([1])
			);
			await waitFor(() => cache.txQueue?.tempDeleted.size === 0);
			await waitFor(() => cache.txQueue?.tempStore.size === 0);
			await waitForResolved(() =>
				expect(cache.txQueue?.currentSize).toEqual(0)
			);
		});

		it("queueMaxBytes", async () => {
			let putSize = 10;
			let putCount = 150;
			let processTxFn = cache.txQueue!.processTxQueue.bind(cache.txQueue);

			let invocationsWithSizes: number[] = [];
			cache.txQueue!.processTxQueue = () => {
				if (cache.txQueue!.currentSize > 0) {
					invocationsWithSizes.push(cache.txQueue!.currentSize);
				}
				return processTxFn();
			};
			for (let i = 0; i < putCount; i++) {
				await cache.put(String(i), crypto.randomBytes(putSize));
			}

			await cache.idle();

			// Two invocations, first triggered by MAX_QUEUE_SIZE
			expect(invocationsWithSizes).toEqual([
				MAX_QUEUE_SIZE,
				putCount * putSize - MAX_QUEUE_SIZE
			]);
		});
		/* TODO feat (?)
		
		it("delete by prefix", async () => {
			await cache.put("a", crypto.randomBytes(8));
			await cache.put("ab", crypto.randomBytes(8));
			await cache.put("abc", crypto.randomBytes(8));

			await cache.deleteByPrefix("a");
			expect(await cache.get("a")).toBeUndefined();
			expect(await cache.get("ab")).toBeUndefined();
			expect(await cache.get("abc")).toBeUndefined();
			await cache.idle();
			expect(await cache.get("a")).toBeUndefined();
			expect(await cache.get("ab")).toBeUndefined();
			expect(await cache.get("abc")).toBeUndefined();
		}); */

		it("can open and close many times", async () => {
			cache.put(" ", crypto.randomBytes(8));
			await cache.close();
			await cache.open();
			await cache.open();
			await cache.close();
			await cache.close();
		});

		it("iterator", async () => {
			await cache.put("0", new Uint8Array([0]));
			await cache.put("1", new Uint8Array([1]));
			await cache.idle();

			let c = 0;
			for await (const [key, value] of cache.iterator()) {
				expect(key).toEqual(String(c));
				expect(new Uint8Array(value)).toEqual(new Uint8Array([c]));
				c++;
			}
			expect(c).toEqual(2);
		});
	});

	describe("sublevel", () => {
		let level: LazyLevel;

		afterEach(async () => {
			await level?.close();
		});
		it("create from sublevel", async () => {
			let store = createStore();
			const sublevel = store.sublevel("sublevel");
			level = new LazyLevel(sublevel);
			await level.close();
			expect(sublevel.status).toEqual("closed");
			expect(store.status).toEqual("open");
		});

		it("clear sublevel", async () => {
			level = new LazyLevel(createStore());
			await level.open();
			await level.put("a", new Uint8Array([1]));
			const sublevel = await level.sublevel("sublevel");
			await sublevel.put("a", new Uint8Array([2]));
			expect(await level.get("a")).toEqual(new Uint8Array([1]));
			expect(await sublevel.get("a")).toEqual(new Uint8Array([2]));
			expect(new Uint8Array((await level.get("a"))!)).toEqual(
				new Uint8Array([1])
			);
			await level.clear();
			expect(await sublevel.get("a")).toBeUndefined();
			await sublevel.idle();
			sublevel.txQueue?.tempStore.clear();
			expect(await sublevel.get("a")).toBeUndefined();
		});
	});
});
