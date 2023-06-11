import assert from "assert";
import { Log } from "../log.js";
import { SortByEntryHash } from "../log-sorting.js";

import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { signKey } from "./fixtures/privateKey.js";

describe("Log - Join Concurrent Entries", function () {
	let store: BlockStore;
	beforeAll(async () => {
		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();
	});

	describe("join ", () => {
		let log1: Log<string>, log2: Log<string>;

		beforeAll(async () => {
			log1 = new Log();
			await log1.open(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				{ sortFn: SortByEntryHash }
			);
			log2 = new Log();
			await log2.open(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				{ sortFn: SortByEntryHash }
			);
		});

		it("joins consistently", async () => {
			// joins consistently
			for (let i = 0; i < 10; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			expect(log1.length).toEqual(20);
			assert.deepStrictEqual(
				(await log1.toArray()).map((e) => e.payload.getValue()),
				(await log2.toArray()).map((e) => e.payload.getValue())
			);

			// Joining after concurrently appending same payload joins entry once
			for (let i = 10; i < 20; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			expect(log1.length).toEqual(log2.length);
			expect(log1.length).toEqual(40);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				(await log2.toArray()).map((e) => e.payload.getValue())
			);
		});

		/*  Below test is not true any more since we are using HLC
		it("Concurrently appending same payload after join results in same state", async () => {
			for (let i = 10; i < 20; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			await log1.append("same");
			await log2.append("same");

			const hash1 = await log1.toMultihash();
			const hash2 = await log2.toMultihash();

			expect(hash1).toEqual(hash2);
			expect(log1.length).toEqual(41);
			expect(log2.length).toEqual(41);
			assert.deepStrictEqual(
				log1.values.toArray().map((e) => e.payload.getValue()),
				log2.values.toArray().map((e) => e.payload.getValue())
			);
		}); */

		/*  it("Joining after concurrently appending same payload joins entry once", async () => {

		 }); */
	});
});
