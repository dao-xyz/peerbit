import assert from "assert";

import { LastWriteWins } from "../log-sorting.js";
import { Entry } from "../entry.js";
import { Log } from "../log.js";
import { LogCreator } from "./utils/log-creator.js";
import { compare } from "@peerbit/uint8arrays";
import { delay } from "@peerbit/time";

// Alternate tiebreaker. Always does the opposite of LastWriteWins
const FirstWriteWins = (a: any, b: any) => LastWriteWins(a, b) * -1;
const BadComparatorReturnsZero = (a: any, b: any) => 0;

import { Ed25519Keypair, PublicSignKey } from "@peerbit/crypto";
import {
	BlockStore,
	MemoryLevelBlockStore,
	StoreStatus
} from "@peerbit/blocks";
import { Blocks, GetOptions, PutOptions } from "@peerbit/blocks-interface";
import { PeerId } from "@libp2p/interface/peer-id";
import { JSON_ENCODING } from "./utils/encoding.js";

const last = <T>(arr: T[]): T => {
	return arr[arr.length - 1];
};

class SlowBlockStore implements Blocks {
	_store: BlockStore;
	lag: number = 3000;
	constructor(store: BlockStore) {
		this._store = store;
	}
	async get<T>(
		cid: string,
		options?: GetOptions | undefined
	): Promise<Uint8Array | undefined> {
		await delay(this.lag);
		return this._store.get(cid, options);
	}
	put(value: Uint8Array): Promise<string> | string {
		return this._store.put(value);
	}

	has(cid: string) {
		return this._store.has(cid);
	}

	rm(cid: string): Promise<void> | void {
		return this._store.rm(cid);
	}

	async start(): Promise<void> {
		await this._store.start();
	}
	stop(): Promise<void> {
		return this._store.stop();
	}
	status(): StoreStatus {
		return this._store.status();
	}

	async waitFor(peer: PeerId | PublicSignKey): Promise<void> {
		return this._store.waitFor(peer);
	}
}

describe("from", function () {
	const firstWriteExpectedData = [
		"entryA10",
		"entryA9",
		"entryA8",
		"entryA7",
		"entryC0",
		"entryA6",
		"entryB5",
		"entryA5",
		"entryB4",
		"entryA4",
		"entryB3",
		"entryA3",
		"entryB2",
		"entryA2",
		"entryB1",
		"entryA1"
	];

	let store: BlockStore;
	let signKey: Ed25519Keypair,
		signKey2: Ed25519Keypair,
		signKey3: Ed25519Keypair,
		signKey4: Ed25519Keypair;
	let signKeys: Ed25519Keypair[];
	beforeAll(async () => {
		signKeys = [
			await Ed25519Keypair.create(),
			await Ed25519Keypair.create(),
			await Ed25519Keypair.create(),
			await Ed25519Keypair.create()
		];
		signKeys.sort((a, b) => {
			return compare(a.publicKey.publicKey, b.publicKey.publicKey);
		});

		signKey = signKeys[0];
		signKey2 = signKeys[1];
		signKey3 = signKeys[2];
		signKey4 = signKeys[3];
		store = new MemoryLevelBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});

	describe("fromEntryHash", () => {
		it("creates a log from an entry hash", async () => {
			const fixture = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const data = fixture.log;
			const heads = await fixture.log.getHeads();

			const log1 = await Log.fromEntry(store, signKey, heads[0], {
				encoding: JSON_ENCODING
			});
			const log2 = await Log.fromEntry(store, signKey, heads[1], {
				encoding: JSON_ENCODING
			});

			await log1.join(log2);
			expect((await log1.getHeads()).map((x) => x.gid)).toContainAllValues(
				(await data.getHeads()).map((x) => x.gid)
			);
			expect(log1.length).toEqual(16);
			const arr = (await log1.toArray()).map((e) => e.payload.getValue());
			assert.deepStrictEqual(arr, fixture.expectedData);
		});

		it("creates a log from an entry hash with custom tiebreaker", async () => {
			const fixture = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const heads = await fixture.log.getHeads();
			const log1 = await Log.fromEntry(store, signKey, heads[0], {
				sortFn: FirstWriteWins,
				encoding: JSON_ENCODING
			});
			const log2 = await Log.fromEntry(store, signKey, heads[1], {
				sortFn: FirstWriteWins,
				encoding: JSON_ENCODING
			});

			await log1.join(log2);

			expect(log1.length).toEqual(16);
			assert.deepStrictEqual(
				(await log1.toArray()).map((e) => e.payload.getValue()),
				firstWriteExpectedData
			);
		});

		it("respects timeout parameter", async () => {
			const timeout = 500;
			const st = new Date().getTime();
			try {
				await Log.fromEntry(
					new SlowBlockStore(store),
					signKey,
					"zdpuAwNuRc2Kc1aNDdcdSWuxfNpHRJQw8L8APBNHCEFXbogus",
					{ timeout, encoding: JSON_ENCODING }
				);
				throw new Error("Expected to fail");
			} catch (error) {
				const et = new Date().getTime();
				expect(et - st).toBeGreaterThan(timeout);
				expect(et - st).toBeLessThan(timeout * 100); // some upper bound
			}
		});
	});

	describe("fromEntry", () => {
		let log1: Log<any>, log2: Log<any>, log3: Log<any>, log4: Log<any>;

		beforeEach(async () => {
			const logOptions = { encoding: JSON_ENCODING };

			log1 = new Log();
			await log1.open(store, signKey, logOptions);
			log2 = new Log();
			await log2.open(store, signKey2, logOptions);
			log3 = new Log();
			await log3.open(store, signKey3, logOptions);
			log4 = new Log();
			await log4.open(store, signKey4, logOptions);
		});

		it("creates a log from an entry", async () => {
			const fixture = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const data = fixture.log;

			const log = await Log.fromEntry<string>(
				store,
				signKey,
				await data.getHeads(),
				{
					encoding: JSON_ENCODING
				}
			);
			expect((await log.getHeads())[0].gid).toEqual(
				(await data.getHeads())[0].gid
			);
			expect(log.length).toEqual(16);
			assert.deepStrictEqual(
				(await log.toArray()).map((e) => e.payload.getValue()),
				fixture.expectedData
			);
		});

		it("creates a log from an entry with custom tiebreaker", async () => {
			const fixture = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const data = fixture.log;

			const log = await Log.fromEntry<string>(
				store,
				signKey,
				await data.getHeads(),
				{ sortFn: FirstWriteWins, encoding: JSON_ENCODING }
			);
			expect(log.length).toEqual(16);
			assert.deepStrictEqual(
				(await log.toArray()).map((e) => e.payload.getValue()),
				firstWriteExpectedData
			);
		});

		/* TODO
		
		it("keeps the original heads", async () => {
			const fixture = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const data = fixture.log;
	
			const log1 = await Log.fromEntry<string>(
				store,
				signKey,
				await data.getHeads(),
				{ length: (await data.getHeads()).length }
			);
			expect((await log1.getHeads()).map(x => x.hash)).toContainValues(
				(await data.getHeads()).map(x => x.hash)
			);
			expect(log1.length).toEqual((await data.getHeads()).length);
			expect((await log1.toArray())[0].payload.getValue()).toEqual("entryC0");
			expect((await log1.toArray())[1].payload.getValue()).toEqual("entryA10");
	
			const log2 = await Log.fromEntry<string>(
				store,
				signKey,
				await data.getHeads(),
				{ length: 4 }
			);
			expect((await log2.getHeads())[0].gid).toEqual(
				(await data.getHeads())[0].gid
			);
			expect(log2.length).toEqual(4);
			expect((await log2.toArray())[0].payload.getValue()).toEqual("entryC0");
			expect((await log2.toArray())[1].payload.getValue()).toEqual("entryA8");
			expect((await log2.toArray())[2].payload.getValue()).toEqual("entryA9");
			expect((await log2.toArray())[3].payload.getValue()).toEqual("entryA10");
	
			const log3 = await Log.fromEntry<string>(
				store,
				signKey,
				await data.getHeads(),
				{ length: 7 }
			);
			expect((await log3.getHeads())[0].gid).toEqual(
				(await data.getHeads())[0].gid
			);
			expect(log3.length).toEqual(7);
			expect((await log3.toArray())[0].payload.getValue()).toEqual("entryB5");
			expect((await log3.toArray())[1].payload.getValue()).toEqual("entryA6");
			expect((await log3.toArray())[2].payload.getValue()).toEqual("entryC0");
			expect((await log3.toArray())[3].payload.getValue()).toEqual("entryA7");
			expect((await log3.toArray())[4].payload.getValue()).toEqual("entryA8");
			expect((await log3.toArray())[5].payload.getValue()).toEqual("entryA9");
			expect((await log3.toArray())[6].payload.getValue()).toEqual("entryA10");
		}); 
	
		it("retrieves partial log from an entry hash", async () => {
			const items1: Entry<string>[] = [];
			const items2: Entry<string>[] = [];
			const items3: Entry<string>[] = [];
			const amount = 100;
			for (let i = 1; i <= amount; i++) {
				const prev1 = last(items1);
				const prev2 = last(items2);
				const prev3 = last(items3);
				const n1 = await Entry.create({
					store,
					identity: log1.identity,
					gidSeed: Buffer.from("X"),
					data: "entryA" + i,
					next: prev1 ? [prev1] : undefined,
				});
				const n2 = await Entry.create({
					store,
					identity: log2.identity,
					gidSeed: Buffer.from("X"),
					data: "entryB" + i,
					next: prev2 ? [prev2, n1] : [n1],
				});
				const n3 = await Entry.create({
					store,
					identity: log3.identity,
					gidSeed: Buffer.from("X"),
					data: "entryC" + i,
					next: prev3 ? [prev3, n1, n2] : [n1, n2],
				});
				items1.push(n1);
				items2.push(n2);
				items3.push(n3);
			}
	
			// limit to 10 entries
			const a = await Log.fromEntry<string>(
				store,
				signKey,
				last(items1),
				{ length: 10 }
			);
			expect(a.length).toEqual(10);
	
			// limit to 42 entries
			const b = await Log.fromEntry<string>(
				store,
				signKey,
				last(items1),
				{ length: 42 }
			);
			expect(b.length).toEqual(42);
		});*/

		it("retrieves full log from an entry hash", async () => {
			const items1: Entry<Uint8Array>[] = [];
			const items2: Entry<Uint8Array>[] = [];
			const items3: Entry<Uint8Array>[] = [];
			const amount = 10;
			for (let i = 1; i <= amount; i++) {
				const prev1 = last(items1);
				const prev2 = last(items2);
				const prev3 = last(items3);
				const n1 = await Entry.create({
					store,
					identity: log1.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						next: prev1 ? [prev1] : undefined
					},
					data: new Uint8Array([0, i])
				});
				const n2 = await Entry.create({
					store,
					identity: log2.identity,
					meta: { gidSeed: Buffer.from("X"), next: prev2 ? [prev2, n1] : [n1] },
					data: new Uint8Array([1, i])
				});
				const n3 = await Entry.create({
					store,
					identity: log3.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						next: prev3 ? [prev3, n1, n2] : [n1, n2]
					},
					data: new Uint8Array([2, i])
				});
				items1.push(n1);
				items2.push(n2);
				items3.push(n3);
			}

			const a = await Log.fromEntry<Uint8Array>(store, signKey, [last(items1)]);
			expect(a.length).toEqual(amount);

			const b = await Log.fromEntry<Uint8Array>(store, signKey2, [
				last(items2)
			]);
			expect(b.length).toEqual(amount * 2);

			const c = await Log.fromEntry<Uint8Array>(store, signKey3, [
				last(items3)
			]);
			expect(c.length).toEqual(amount * 3);
		});

		it("retrieves full log from an entry hash 2", async () => {
			const items1: Entry<Uint8Array>[] = [];
			const items2: Entry<Uint8Array>[] = [];
			const items3: Entry<Uint8Array>[] = [];
			const amount = 10;
			for (let i = 1; i <= amount; i++) {
				const prev1 = last(items1);
				const prev2 = last(items2);
				const prev3 = last(items3);
				const n1 = await Entry.create({
					store,
					identity: log1.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						next: prev1 ? [prev1] : undefined
					},
					data: new Uint8Array([0, i])
				});
				const n2 = await Entry.create({
					store,
					identity: log2.identity,
					meta: { gidSeed: Buffer.from("X"), next: prev2 ? [prev2, n1] : [n1] },
					data: new Uint8Array([1, i])
				});
				const n3 = await Entry.create({
					store,
					identity: log3.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						next: prev3 ? [prev3, n1, n2] : [n1, n2]
					},
					data: new Uint8Array([2, i])
				});
				items1.push(n1);
				items2.push(n2);
				items3.push(n3);
			}

			const a = await Log.fromEntry<Uint8Array>(store, signKey, last(items1));
			expect(a.length).toEqual(amount);

			const b = await Log.fromEntry<Uint8Array>(store, signKey2, last(items2));
			expect(b.length).toEqual(amount * 2);

			const c = await Log.fromEntry<Uint8Array>(store, signKey3, last(items3));
			expect(c.length).toEqual(amount * 3);
		});

		it("retrieves full log from an entry hash 3", async () => {
			const items1: Entry<string>[] = [];
			const items2: Entry<string>[] = [];
			const items3: Entry<string>[] = [];
			const amount = 10;
			for (let i = 1; i <= amount; i++) {
				const prev1 = last(items1);
				const prev2 = last(items2);
				const prev3 = last(items3);
				/*        log1.tickClock()
			 log2.tickClock()
			 log3.tickClock() */
				const n1 = await Entry.create({
					store,
					identity: log1.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						clock:
							items1.length > 0
								? items1[items1.length - 1].meta.clock.advance()
								: undefined,
						next: prev1 ? [prev1] : undefined
					},
					data: "entryA" + i,
					encoding: JSON_ENCODING
				});
				const n2 = await Entry.create({
					store,
					identity: log2.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						clock:
							items2.length > 0
								? items2[items2.length - 1].meta.clock.advance()
								: undefined,
						next: prev2 ? [prev2, n1] : [n1]
					},
					data: "entryB" + i,
					encoding: JSON_ENCODING
				});
				const n3 = await Entry.create({
					store,
					identity: log3.identity,
					meta: {
						gidSeed: Buffer.from("X"),
						clock:
							items3.length > 0
								? items3[items3.length - 1].meta.clock.advance()
								: undefined,
						next: prev3 ? [prev3, n1, n2] : [n1, n2]
					},
					data: "entryC" + i,
					encoding: JSON_ENCODING
				});
				/*        log1.mergeClock(log2.clock)
			 log1.mergeClock(log3.clock)
			 log2.mergeClock(log1.clock)
			 log2.mergeClock(log3.clock)
			 log3.mergeClock(log1.clock)
			 log3.mergeClock(log2.clock) */
				items1.push(n1);
				items2.push(n2);
				items3.push(n3);
			}

			const a = await Log.fromEntry<string>(store, signKey, last(items1));
			expect(a.length).toEqual(amount);

			const itemsInB = [
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryB6",
				"entryA7",
				"entryB7",
				"entryA8",
				"entryB8",
				"entryA9",
				"entryB9",
				"entryA10",
				"entryB10"
			];

			const b = await Log.fromEntry<string>(store, signKey2, last(items2), {
				encoding: JSON_ENCODING
			});
			expect(b.length).toEqual(amount * 2);
			expect(
				(await b.toArray()).map((e) => e.payload.getValue())
			).toContainAllValues(itemsInB);

			const c = await Log.fromEntry<string>(store, signKey4, last(items3), {
				encoding: JSON_ENCODING
			});
			await c.append("EOF");
			expect(c.length).toEqual(amount * 3 + 1);

			const tmp = [
				"entryA1",
				"entryB1",
				"entryC1",
				"entryA2",
				"entryB2",
				"entryC2",
				"entryA3",
				"entryB3",
				"entryC3",
				"entryA4",
				"entryB4",
				"entryC4",
				"entryA5",
				"entryB5",
				"entryC5",
				"entryA6",
				"entryB6",
				"entryC6",
				"entryA7",
				"entryB7",
				"entryC7",
				"entryA8",
				"entryB8",
				"entryC8",
				"entryA9",
				"entryB9",
				"entryC9",
				"entryA10",
				"entryB10",
				"entryC10",
				"EOF"
			];

			expect(
				(await c.toArray()).map((e) => e.payload.getValue())
			).toContainAllValues(tmp);

			// make sure logX comes after A, B and C
			const logX = new Log<string>();
			await logX.open(store, signKey4, { encoding: JSON_ENCODING });
			await logX.append("1");
			await logX.append("2");
			await logX.append("3");
			const d = await Log.fromEntry<string>(
				store,
				signKey3,
				last(await logX.toArray()),
				{
					encoding: JSON_ENCODING
				}
			);

			await c.join(d);
			await d.join(c);

			await c.append("DONE");
			await d.append("DONE");
			const f = await Log.fromEntry<string>(
				store,
				signKey3,
				last(await c.toArray()),
				{ encoding: JSON_ENCODING }
			);
			const g = await Log.fromEntry<string>(
				store,
				signKey3,
				last(await d.toArray()),
				{ encoding: JSON_ENCODING }
			);

			/*  expect(f.toString()).toEqual(bigLogString) // Ignore these for know since we have removed the clock manipulation in the loop
	 expect(g.toString()).toEqual(bigLogString) */
		});

		it("retrieves full log of randomly joined log", async () => {
			for (let i = 1; i <= 5; i++) {
				await log1.append("entryA" + i);
				await log2.append("entryB" + i);
			}

			await log3.join(log1);
			await log3.join(log2);

			for (let i = 6; i <= 10; i++) {
				await log1.append("entryA" + i);
			}

			await log1.join(log3);

			for (let i = 11; i <= 15; i++) {
				await log1.append("entryA" + i);
			}

			const expectedData = [
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10",
				"entryA11",
				"entryA12",
				"entryA13",
				"entryA14",
				"entryA15"
			];

			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);
		});

		it("retrieves randomly joined log deterministically", async () => {
			for (let i = 1; i <= 5; i++) {
				await log1.append("entryA" + i);
				await log2.append("entryB" + i);
			}

			await log3.join(log1);
			await log3.join(log2);

			for (let i = 6; i <= 10; i++) {
				await log1.append("entryA" + i);
			}

			await log4.join(log3);
			await log4.append("entryC0");
			await log4.join(log1);

			const expectedData = [
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10",
				"entryC0"
			];

			expect(
				(await log4.toArray()).map((e) => e.payload.getValue())
			).toStrictEqual(expectedData);
		});

		it("sorts", async () => {
			const testLog = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const log = testLog.log;
			const expectedData = testLog.expectedData;

			const expectedData2 = [
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10"
			];

			const expectedData3 = [
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryC0",
				"entryA7",
				"entryA8",
				"entryA9"
			];

			const expectedData4 = [
				"entryA1",
				"entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryA6",
				"entryC0",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10"
			];

			const fetchOrder = (await log.toArray()).slice().sort(Entry.compare);
			assert.deepStrictEqual(
				fetchOrder.map((e) => e.payload.getValue()),
				expectedData
			);

			const reverseOrder = (await log.toArray())
				.slice()
				.reverse()
				.sort(Entry.compare);
			assert.deepStrictEqual(fetchOrder, reverseOrder);

			const hashOrder = (await log.toArray())
				.slice()
				.sort((a, b) => a.hash.localeCompare(b.hash))
				.sort(Entry.compare);
			assert.deepStrictEqual(fetchOrder, hashOrder);

			const randomOrder2 = (await log.toArray())
				.slice()
				.sort((a, b) => 0.5 - Math.random())
				.sort(Entry.compare);
			assert.deepStrictEqual(fetchOrder, randomOrder2);

			// partial data
			const partialLog = (await log.toArray())
				.filter((e) => e.payload.getValue() !== "entryC0")
				.sort(Entry.compare);
			assert.deepStrictEqual(
				partialLog.map((e) => e.payload.getValue()),
				expectedData2
			);

			const partialLog2 = (await log.toArray())
				.filter((e) => e.payload.getValue() !== "entryA10")
				.sort(Entry.compare);
			assert.deepStrictEqual(
				partialLog2.map((e) => e.payload.getValue()),
				expectedData3
			);

			const partialLog3 = (await log.toArray())
				.filter((e) => e.payload.getValue() !== "entryB5")
				.sort(Entry.compare);
			assert.deepStrictEqual(
				partialLog3.map((e) => e.payload.getValue()),
				expectedData4
			);
		});

		it("sorts deterministically from random order", async () => {
			const testLog = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const log = testLog.log;
			const expectedData = testLog.expectedData;

			const fetchOrder = (await log.toArray()).slice().sort(Entry.compare);
			assert.deepStrictEqual(
				fetchOrder.map((e) => e.payload.getValue()),
				expectedData
			);

			let sorted;
			for (let i = 0; i < 1000; i++) {
				const randomOrder = (await log.toArray())
					.slice()
					.sort((a, b) => 0.5 - Math.random());
				sorted = randomOrder.sort(Entry.compare);
				assert.deepStrictEqual(
					sorted.map((e) => e.payload.getValue()),
					expectedData
				);
			}
		});

		it("sorts entries correctly", async () => {
			const testLog = await LogCreator.createLogWithTwoHundredEntries(
				store,
				signKeys
			);
			const log = testLog.log;
			const expectedData = testLog.expectedData;
			assert.deepStrictEqual(
				(await log.toArray()).map((e) => e.payload.getValue()),
				expectedData
			);
		});

		it("sorts entries according to custom tiebreaker function", async () => {
			const testLog = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);

			const firstWriteWinsLog = new Log<string>();
			await firstWriteWinsLog.open(store, signKeys[0], {
				sortFn: FirstWriteWins,
				encoding: JSON_ENCODING
			});
			await firstWriteWinsLog.join(testLog.log);
			assert.deepStrictEqual(
				(await firstWriteWinsLog.toArray()).map((e) => e.payload.getValue()),
				firstWriteExpectedData
			);
		});

		it("throws an error if the tiebreaker returns zero", async () => {
			const testLog = await LogCreator.createLogWithSixteenEntries(
				store,
				signKeys
			);
			const firstWriteWinsLog = new Log<string>();
			await firstWriteWinsLog.open(store, signKeys[0], {
				sortFn: BadComparatorReturnsZero,
				encoding: JSON_ENCODING
			});
			await expect(() => firstWriteWinsLog.join(testLog.log)).rejects.toThrow();
		});

		/* TODO 
		
		it("retrieves partially joined log deterministically - single next pointer", async () => {
			for (let i = 1; i <= 5; i++) {
				await log1.append("entryA" + i, {
					meta: { next: await log1.getHeads() },
					timestamp: new Timestamp({
						wallTime: BigInt(i),
						logical: 0,
					}),
				});
				await log2.append("entryB" + i, {
					nexts: await log2.getHeads(),
					timestamp: new Timestamp({
						wallTime: BigInt(i),
						logical: 1,
					}),
				});
			}
	
			await log3.join(log1);
			await log3.join(log2);
	
			for (let i = 6; i <= 10; i++) {
				await log1.append("entryA" + i, {
					meta: { next: await log1.getHeads() },
					timestamp: new Timestamp({
						wallTime: BigInt(i),
						logical: 0,
					}),
				});
			}
	
			await log4.join(log3);
			await log4.append("entryC0", {
				nexts: await log2.getHeads(),
				timestamp: new Timestamp({
					wallTime: BigInt(11),
					logical: 0,
				}),
			});
	
			await log4.join(log1);
	
			// First 5
			let res = new Log();
			await res.init(store,signKey2);
			await res.join(await log4.getHeads(), { length: 5 });
	
			const first5 = ["entryB5", "entryA8", "entryA9", "entryA10", "entryC0"];
	
			assert.deepStrictEqual(
				(await res.toArray()).map((e) => e.payload.getValue()),
				first5
			);
	
			// First 11
			res = new Log();
			await res.init(store,signKey2);
			await res.join(await log4.getHeads(), { length: 11 });
	
			// TODO, is this really the expected order? Determins is a partial load is not super important,
			// since partial loading is done by someone who wants an approximate state of something
			const first11 = [
				"entryB2",
				"entryB3",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10",
				"entryC0",
			];
	
			expect((await res.toArray()).map((e) => e.payload.getValue())).toEqual(
				first11
			);
	
			// All but one
			res = new Log();
			await res.init(store,signKey2);
			await res.join(await log4.getHeads(), { length: 16 - 1 });
	
			const all = [
				 "entryB1",
				"entryA2",
				"entryB2",
				"entryA3",
				"entryB3",
				"entryA4",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10",
				"entryC0",
			];
	
			assert.deepStrictEqual(
				(await res.toArray()).map((e) => e.payload.getValue()),
				all
			);
		});
	
		it("retrieves partially joined log deterministically - multiple next pointers", async () => {
			for (let i = 1; i <= 5; i++) {
				await log1.append("entryA" + i, {
					meta: { next: await log1.getHeads() },
					timestamp: new Timestamp({
						wallTime: BigInt(i),
						logical: 0,
					}),
				});
				await log2.append("entryB" + i, {
					nexts: await log2.getHeads(),
					timestamp: new Timestamp({
						wallTime: BigInt(i),
						logical: 1,
					}),
				});
			}
			await log3.join(log1);
			await log3.join(log2);
	
			for (let i = 6; i <= 10; i++) {
				await log1.append("entryA" + i, {
					meta: { next: await log1.getHeads() },
					timestamp: new Timestamp({
						wallTime: BigInt(i),
						logical: 0,
					}),
				});
			}
	
			await log4.join(log3);
			await log4.append("entryC0", {
				nexts: await log2.getHeads(),
				timestamp: new Timestamp({
					wallTime: BigInt(11),
					logical: 0,
				}),
			});
	
			await log4.join(log1);
	
			// First 5
			let res = new Log();
			await res.init(store,signKey2);
			await res.join(await log4.getHeads(), { length: 5 });
	
			// TODO, make sure partial load is deterministic (ordered by time)
			const first5 = ["entryB5", "entryA8", "entryA9", "entryA10", "entryC0"];
	
			expect((await res.toArray()).map((e) => e.payload.getValue())).toEqual(
				first5
			);
	
			// First 11
			res = new Log();
			await res.init(store,signKey2);
			await res.join(await log4.getHeads(), { length: 11 });
	
			const first11 = [
				"entryB2",
				"entryB3",
				"entryB4",
				"entryA5",
				"entryB5",
				"entryA6",
				"entryA7",
				"entryA8",
				"entryA9",
				"entryA10",
				"entryC0",
			];
	
			expect((await res.toArray()).map((e) => e.payload.getValue())).toEqual(
				first11
			);
	
			// All but one
			res = new Log();
			await res.init(store,signKey2);
			await res.join(await log4.getHeads(), { length: 16 - 1 });
	
			const all = [
				 "entryB1",
			"entryA2",
			"entryB2",
			"entryA3",
			"entryB3",
			"entryA4",
			"entryB4",
			"entryA5",
			"entryB5",
			"entryA6",
			"entryA7",
			"entryA8",
			"entryA9",
			"entryA10",
			"entryC0",
			];
	
		expect((await res.toArray()).map((e) => e.payload.getValue())).toEqual(
			all
		);
	}); */

		describe("fetches a log", () => {
			const amount = 100;
			let items1: Entry<Uint8Array>[] = [];
			let items2: Entry<Uint8Array>[] = [];
			let items3: Entry<Uint8Array>[] = [];
			beforeEach(async () => {
				items1 = [];
				items2 = [];
				items3 = [];
				for (let i = 1; i <= amount; i++) {
					const prev1 = last(items1);
					const prev2 = last(items2);
					const prev3 = last(items3);
					const n1 = await Entry.create({
						store,
						identity: log1.identity,
						meta: {
							gidSeed: Buffer.from("X"),
							clock:
								items1.length > 0
									? items1[items1.length - 1].meta.clock.advance()
									: undefined,
							next: prev1 ? [prev1] : undefined
						},
						data: new Uint8Array([0, i])
					});
					const n2 = await Entry.create({
						store,
						identity: log2.identity,
						meta: {
							gidSeed: Buffer.from("X"),
							clock:
								items2.length > 0
									? items2[items2.length - 1].meta.clock.advance()
									: undefined,
							next: prev2 ? [prev2, n1] : [n1]
						},
						data: new Uint8Array([1, i])
					});
					const n3 = await Entry.create({
						store,
						identity: log3.identity,
						meta: {
							gidSeed: Buffer.from("X"),
							clock:
								items3.length > 0
									? items3[items3.length - 1].meta.clock.advance()
									: undefined,
							next: prev3 ? [prev3, n1, n2] : [n1, n2]
						},
						data: new Uint8Array([2, i])
					});
					items1.push(n1);
					items2.push(n2);
					items3.push(n3);
				}
			});

			it("returns all entries - no excluded entries", async () => {
				const a = await Log.fromEntry<Uint8Array>(store, signKey, last(items1));
				expect(a.length).toEqual(amount);
				expect((await a.toArray())[0].hash).toEqual(items1[0].hash);
			});
		});
	});
});
