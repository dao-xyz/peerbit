import assert from "assert";
import { Log } from "../log.js";
import { BlockStore, MemoryLevelBlockStore } from "@peerbit/blocks";
import { Entry } from "../entry.js";
import { signKey, signKey2, signKey3 } from "./fixtures/privateKey.js";

describe("Log - Iterator", function () {
	let store: BlockStore;

	beforeAll(async () => {
		store = new MemoryLevelBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});

	describe("Basic iterator functionality", () => {
		let log1: Log<string>;

		let entries: Entry<any>[];
		beforeEach(async () => {
			entries = [];
			log1 = new Log();
			await log1.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});

			for (let i = 0; i <= 100; i++) {
				entries.push((await log1.append("entry" + i)).entry);
			}
		});

		it("returns a Symbol.iterator object", async () => {
			const it = log1.iterator({
				amount: 0,
			});

			expect(typeof it[Symbol.iterator]).toEqual("function");
			assert.deepStrictEqual(it.next(), {
				value: undefined,
				done: true,
			});
		});

		it("returns length from tail and amount", async () => {
			const amount = 10;
			const it = log1.iterator({
				amount: amount,
			});
			const length = [...it].length;
			expect(length).toEqual(10);
			let i = 0;
			for (const entry of it) {
				expect(entry).toEqual(entries[i++].hash);
			}
		});

		it("returns length from head and amount", async () => {
			const amount = 10;
			const it = log1.iterator({
				amount: amount,
				from: "head",
			});
			const length = [...it].length;
			expect(length).toEqual(10);
			let i = 0;
			for (const entry of it) {
				expect(entry).toEqual(entries[100 - i++].hash);
			}
		});

		it("returns all", async () => {
			const it = log1.iterator();
			const length = [...it].length;
			expect(length).toEqual(101);
			let i = 0;
			for (const entry of it) {
				expect(entry).toEqual(entries[i++].hash);
			}
		});
	});
});
