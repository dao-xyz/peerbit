import assert from "assert";
import { Log } from "../log.js";
import { BlockStore, MemoryLevelBlockStore } from "@peerbit/blocks";
import {
	signKey,
	signKey2,
	signKey3,
	signKey4,
} from "./fixtures/privateKey.js";

const last = (arr: any[]) => {
	return arr[arr.length - 1];
};

describe("head-tails", function () {
	let store: BlockStore;

	beforeAll(async () => {
		store = new MemoryLevelBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});

	let log1: Log<string>,
		log2: Log<string>,
		log3: Log<string>,
		log4: Log<string>;

	beforeEach(async () => {
		log1 = new Log<string>();
		await log1.open(store, {
			...signKey,
			sign: async (data: Uint8Array) => await signKey.sign(data),
		});
		log2 = new Log<string>();
		await log2.open(store, {
			...signKey2,
			sign: async (data: Uint8Array) => await signKey2.sign(data),
		});
		log3 = new Log<string>();
		await log3.open(store, {
			...signKey3,
			sign: async (data: Uint8Array) => await signKey3.sign(data),
		});
		log4 = new Log<string>();
		await log4.open(store, {
			...signKey4,
			sign: async (data: Uint8Array) => await signKey4.sign(data),
		});
	});
	afterEach(async () => {
		await log1.close();
		await log2.close();
	});

	describe("heads", () => {
		it("finds one head after one entry", async () => {
			await log1.append("helloA1");
			expect((await log1.getHeads()).length).toEqual(1);
		});

		it("finds one head after two entries", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			expect((await log1.getHeads()).length).toEqual(1);
		});

		it("log contains the head entry", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			assert.deepStrictEqual(
				(await log1.get((await log1.getHeads())[0].hash))?.hash,
				(await log1.getHeads())[0].hash
			);
		});

		it("finds head after a join and append", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			await log2.append("helloB1");

			await log2.join(log1);
			await log2.append("helloB2");
			const expectedHead = last(await log2.toArray());

			expect((await log2.getHeads()).length).toEqual(1);
			assert.deepStrictEqual(
				(await log2.getHeads())[0].hash,
				expectedHead.hash
			);
		});

		it("finds two heads after a join", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			const expectedHead1 = last(await log1.toArray());

			await log2.append("helloB1");
			await log2.append("helloB2");
			const expectedHead2 = last(await log2.toArray());

			await log1.join(log2);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(2);
			expect(heads.map((x) => x.hash)).toContainAllValues([
				expectedHead1.hash,
				expectedHead2.hash,
			]);
		});

		it("finds two heads after two joins", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");

			await log2.append("helloB1");
			await log2.append("helloB2");

			await log1.join(log2);

			await log2.append("helloB3");

			await log1.append("helloA3");
			await log1.append("helloA4");
			const expectedHead2 = last(await log2.toArray());
			const expectedHead1 = last(await log1.toArray());

			await log1.join(log2);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(2);
			expect(heads[0].hash).toEqual(expectedHead1.hash);
			expect(heads[1].hash).toEqual(expectedHead2.hash);
		});

		it("finds two heads after three joins", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			await log2.append("helloB1");
			await log2.append("helloB2");
			await log1.join(log2);
			await log1.append("helloA3");
			await log1.append("helloA4");
			const expectedHead1 = last(await log1.toArray());
			await log3.append("helloC1");
			await log3.append("helloC2");
			await log2.join(log3);
			await log2.append("helloB3");
			const expectedHead2 = last(await log2.toArray());
			await log1.join(log2);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(2);
			expect(heads.map((x) => x.hash)).toContainAllValues([
				expectedHead1.hash,
				expectedHead2.hash,
			]);
		});

		it("finds three heads after three joins", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			await log2.append("helloB1");
			await log2.append("helloB2");
			await log1.join(log2);
			await log1.append("helloA3");
			await log1.append("helloA4");
			const expectedHead1 = last(await log1.toArray());
			await log3.append("helloC1");
			await log2.append("helloB3");
			await log3.append("helloC2");
			const expectedHead2 = last(await log2.toArray());
			const expectedHead3 = last(await log3.toArray());
			await log1.join(log2);
			await log1.join(log3);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(3);
			expect(heads.map((x) => x.hash)).toContainAllValues([
				expectedHead1.hash,
				expectedHead2.hash,
				expectedHead3.hash,
			]);
		});
	});

	describe("tails", () => {
		it("returns a tail", async () => {
			await log1.append("helloA1");
			expect((await log1.getTails()).length).toEqual(1);
		});

		it("tail is a Entry", async () => {
			await log1.append("helloA1");
		});

		it("returns tail entries", async () => {
			await log1.append("helloA1");
			await log2.append("helloB1");
			await log1.join(log2);
			expect((await log1.getTails()).length).toEqual(2);
		});

		it("returns tail hashes", async () => {
			log1 = new Log();
			await log1.open(store, signKey, { trim: { type: "length", to: 2 } });
			log2 = new Log();
			await log2.open(store, signKey, { trim: { type: "length", to: 2 } });
			const { entry: a1 } = await log1.append("helloA1");
			const { entry: b1 } = await log2.append("helloB1");
			const { entry: a2 } = await log1.append("helloA2");
			const { entry: b2 } = await log2.append("helloB2");
			await log1.join(log2);

			// the joined log will only contain the last two entries a2, b2
			expect((await log1.toArray()).map((x) => x.hash)).toContainAllValues([
				a2.hash,
				b2.hash,
			]);
			expect(await log1.getTailHashes()).toContainAllValues([a1.hash, b1.hash]);
		});

		it("returns no tail hashes if all entries point to empty nexts", async () => {
			await log1.append("helloA1");
			await log2.append("helloB1");
			await log1.join(log2);
			expect((await log1.getTailHashes()).length).toEqual(0);
		});

		/* TODO
		
		it("returns tails after loading a partial log", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			await log2.append("helloB1");
			await log2.append("helloB2");
			await log1.join(log2);
			await log4.join(await log1.getHeads(), { length: 2 });
			expect(log4.length).toEqual(2);
			expect((await log4.getTails()).length).toEqual(2);
			expect((await log4.getTails())[0].hash).toEqual(
				(await log4.toArray())[0].hash
			);
			expect((await log4.getTails())[1].hash).toEqual(
				(await log4.toArray())[1].hash
			);
		}); */

		it("returns tails sorted by public key", async () => {
			await log1.append("helloX1");
			await log2.append("helloB1");
			await log3.append("helloA1");
			await log3.join(log1);
			await log3.join(log2);
			await log4.join(log3);
			expect((await log4.getTails()).length).toEqual(3);

			expect((await log4.getTails())[0].metadata.clock.id).toEqual(
				signKey.publicKey.bytes
			);
			expect((await log4.getTails())[1].metadata.clock.id).toEqual(
				signKey2.publicKey.bytes
			);
			expect((await log4.getTails())[2].metadata.clock.id).toEqual(
				signKey3.publicKey.bytes
			);
		});
	});
});
