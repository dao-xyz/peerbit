import { field } from "@dao-xyz/borsh";
import Cache from "../index.js";

import assert from "assert";
const timeout = 50000;
import {} from "level";
import { MemoryLevel } from "memory-level";
import { jest } from "@jest/globals";

export const createStore = (): MemoryLevel => {
	return new MemoryLevel({ valueEncoding: "view" });
};

describe(`Cache - level`, function () {
	jest.setTimeout(timeout);

	let cache: Cache<any>, storage: {}, store: MemoryLevel;

	const data = [
		{ type: typeof true, key: "boolean", value: true },
		{ type: typeof 1.0, key: "number", value: 9000 },
		{ type: typeof "x", key: "strng", value: "string value" },
		{ type: typeof [], key: "array", value: [1, 2, 3, 4] },
		{
			type: typeof {},
			key: "object",
			value: { object: "object", key: "key" },
		},
	];

	beforeAll(async () => {
		try {
			store = await createStore();
		} catch (error) {
			const x = 123;
		}
		cache = new Cache(store);
	});

	afterAll(async () => {
		await store.close();
	});

	it(`set, get, delete`, async () => {
		for (const d of data) {
			await cache.set(d.key, d.value);
			const val = await cache.get(d.key);
			assert.deepStrictEqual(val, d.value);
			expect(typeof val).toEqual(d.type);

			try {
				await cache.get("fooKey");
			} catch (e: any) {
				fail();
			}

			await cache.set(d.key, JSON.stringify(d.value));
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

	it(`set get binary`, async () => {
		class TestStruct {
			@field({ type: "u32" })
			number: number;
		}

		const obj = Object.assign(new TestStruct(), { number: 123 });
		await cache.setBinary("key", obj);
		const val = await cache.getBinary("key", TestStruct);
		assert.deepStrictEqual(val, obj);
	});

	it(`get binary corrupt`, async () => {
		class TestStruct {
			@field({ type: "u32" })
			number: number;
		}

		await cache.setBinary("key", new Uint8Array([254, 253, 252]));
		await expect(() =>
			cache.getBinary("key", TestStruct)
		).rejects.toThrowError();
	});

	it("can create from sublevel", () => {
		const sublevel = store.sublevel("sublevel");
		const _sublevelCache = new Cache(sublevel);
	});
});
