import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import assert from "assert";
import { expect } from "chai";
import { SortByEntryHash } from "../src/log-sorting.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("concurrency", function () {
	let store: BlockStore;
	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("join", () => {
		let log1: Log<string>, log2: Log<string>;

		before(async () => {
			log1 = new Log();
			await log1.open(store, signKey, {
				sortFn: SortByEntryHash,
				encoding: JSON_ENCODING,
			});
			log2 = new Log();
			await log2.open(store, signKey, {
				sortFn: SortByEntryHash,
				encoding: JSON_ENCODING,
			});
		});

		it("joins consistently", async () => {
			// joins consistently
			for (let i = 0; i < 10; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			expect(log1.length).equal(20);
			assert.deepStrictEqual(
				(await log1.toArray()).map((e) => e.payload.getValue()),
				(await log2.toArray()).map((e) => e.payload.getValue()),
			);

			// Joining after concurrently appending same payload joins entry once
			for (let i = 10; i < 20; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			expect(log1.length).equal(log2.length);
			expect(log1.length).equal(40);
			expect(
				(await log1.toArray()).map((e) => e.payload.getValue()),
			).to.deep.equal((await log2.toArray()).map((e) => e.payload.getValue()));
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

			expect(hash1).equal(hash2);
			expect(log1.length).equal(41);
			expect(log2.length).equal(41);
			assert.deepStrictEqual(
				log1.toArray().map((e) => e.payload.getValue()),
				log2.toArray().map((e) => e.payload.getValue())
			);
		}); */

		/*  it("Joining after concurrently appending same payload joins entry once", async () => {

		 }); */
	});
});
