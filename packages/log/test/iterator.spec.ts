import assert from "assert";
import { Log } from "../src/log.js";
import { type BlockStore, AnyBlockStore } from "@peerbit/blocks";
import { Entry } from "../src/entry.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";
import { expect } from "chai";

describe("Iterator", function () {
	let store: BlockStore;

	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("Basic iterator functionality", () => {
		let log1: Log<string>;

		let entries: Entry<any>[];
		beforeEach(async () => {
			entries = [];
			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });

			for (let i = 0; i <= 100; i++) {
				entries.push((await log1.append("entry" + i)).entry);
			}
		});

		it("returns a Symbol.iterator object", async () => {
			const it = log1.iterator({
				amount: 0
			});

			expect(typeof it[Symbol.iterator]).equal("function");
			assert.deepStrictEqual(it.next(), {
				value: undefined,
				done: true
			});
		});

		it("returns length from tail and amount", async () => {
			const amount = 10;
			const it = log1.iterator({
				amount: amount
			});
			const length = [...it].length;
			expect(length).equal(10);
			let i = 0;
			for (const entry of it) {
				expect(entry).equal(entries[i++].hash);
			}
		});

		it("returns length from head and amount", async () => {
			const amount = 10;
			const it = log1.iterator({
				amount: amount,
				from: "head"
			});
			const length = [...it].length;
			expect(length).equal(10);
			let i = 0;
			for (const entry of it) {
				expect(entry).equal(entries[100 - i++].hash);
			}
		});

		it("returns all", async () => {
			const it = log1.iterator();
			const length = [...it].length;
			expect(length).equal(101);
			let i = 0;
			for (const entry of it) {
				expect(entry).equal(entries[i++].hash);
			}
		});
	});
});
