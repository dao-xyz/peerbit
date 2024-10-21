import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { HashmapIndices } from "@peerbit/indexer-simple";
import assert from "assert";
import { expect } from "chai";
import { Timestamp } from "../src/clock.js";
import { Log } from "../src/log.js";
import { signKey, signKey2, signKey3 } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("properties", function () {
	let store: BlockStore;
	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("constructor", () => {
		it("creates an empty log with default params", async () => {
			const log = new Log();
			await log.open(store, signKey, undefined);
			assert.notStrictEqual(log.entryIndex, null);
			assert.notStrictEqual(log.id, null);
			assert.notStrictEqual(log.id, null);
			assert.notStrictEqual(log.toArray(), null);
			assert.deepStrictEqual(await log.toArray(), []);
			assert.deepStrictEqual(await log.getHeads().all(), []);
			assert.deepStrictEqual(await log.getTailHashes(), []);
		});

		it("can not open twice", async () => {
			const log = new Log();
			await log.open(store, signKey, undefined);
			await expect(log.open(store, signKey, undefined)).rejectedWith();
		});
		it("sets an id", async () => {
			const log = new Log({ id: new Uint8Array(1) });
			await log.open(store, signKey);
			expect(log.id).to.deep.equal(new Uint8Array(1));
		});

		it("generates if id is not passed as an argument", async () => {
			const log = new Log();
			await log.open(store, signKey);
			expect(log.id).to.be.instanceOf(Uint8Array);
		});
	});

	describe("toString", () => {
		let log: Log<string>;
		const expectedData =
			'"five"\n└─"four"\n  └─"three"\n    └─"two"\n      └─"one"';

		beforeEach(async () => {
			log = new Log<string>();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("two", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("three", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("four", { meta: { gidSeed: Buffer.from("a") } });
			await log.append("five", { meta: { gidSeed: Buffer.from("a") } });
		});

		it("returns a nicely formatted string", async () => {
			expect(
				await log.toString((p) => Buffer.from(p.data).toString()),
			).to.deep.equal(expectedData);
		});
	});

	describe("get", () => {
		let log: Log<any>;

		beforeEach(async () => {
			log = new Log<Uint8Array>();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", {
				meta: {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				},
			});
		});

		it("returns an Entry", async () => {
			const entry = await log.get((await log.toArray())[0].hash)!;
			expect(entry?.hash).to.equal(
				"zb2rhYpDDgijHQyZRYovg3mKpgLDCBb89uFGFrRbYoiVCKGiX",
			);
		});

		it("returns undefined when Entry is not in the log", async () => {
			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
			);
			assert.deepStrictEqual(entry, undefined);
		});

		it("does not fetch from remotes by if missing block by default", async () => {
			const storeGetFn = store.get.bind(store);
			let remoteFetchOptions: any = undefined;
			let fetched = false;
			store.get = async (hash, options) => {
				remoteFetchOptions = options;
				fetched = true;
				return storeGetFn(hash, options);
			};
			log.entryIndex.has = () => Promise.resolve(true);

			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
			);
			assert.deepStrictEqual(entry, undefined);
			expect(fetched).to.be.true;
			expect(remoteFetchOptions.remote).to.be.undefined;
		});

		it("fetches remotes with timeout", async () => {
			const storeGetFn = store.get.bind(store);
			let timeout: any = undefined;
			let fetched = false;
			store.get = async (hash, options) => {
				timeout = (options?.remote as any)?.["timeout"];
				fetched = true;
				return storeGetFn(hash, options);
			};
			log.entryIndex.has = () => Promise.resolve(true);

			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
				{
					remote: {
						timeout: 123,
					},
				},
			);
			assert.deepStrictEqual(entry, undefined);
			expect(fetched).to.be.true;
			expect(timeout).to.eq(123);
		});
	});

	describe("setIdentity", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", { meta: { gidSeed: Buffer.from("a") } });
		});

		it("changes identity", async () => {
			expect((await log.toArray())[0].meta.clock.id).to.deep.equal(
				signKey.publicKey.bytes,
			);
			log.setIdentity(signKey2);
			await log.append("two", { meta: { gidSeed: Buffer.from("a") } });
			assert.deepStrictEqual(
				(await log.toArray())[1].meta.clock.id,
				signKey2.publicKey.bytes,
			);
			log.setIdentity(signKey3);
			await log.append("three", { meta: { gidSeed: Buffer.from("a") } });
			assert.deepStrictEqual(
				(await log.toArray())[2].meta.clock.id,
				signKey3.publicKey.bytes,
			);
		});
	});

	describe("has", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			await log.append("one", { meta: { gidSeed: Buffer.from("a") } });
		});

		it("returns true if it has an Entry", async () => {
			assert(await log.has((await log.toArray())[0].hash));
		});

		it("returns true if it has an Entry, hash lookup", async () => {
			assert(await log.has((await log.toArray())[0].hash));
		});

		it("returns false if it doesn't have the Entry", async () => {
			expect(
				await log.has("zb2rhbnwihVVVVEVVPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"),
			).equal(false);
		});
	});

	describe("values", () => {
		it("returns all entries in the log", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			expect((await log.toArray()) instanceof Array).equal(true);
			expect(log.length).equal(0);
			await log.append(new Uint8Array([1]));
			await log.append(new Uint8Array([2]));
			await log.append(new Uint8Array([3]));
			expect((await log.toArray()) instanceof Array).equal(true);
			expect(log.length).equal(3);
			expect((await log.toArray())[0].payload.getValue()).to.deep.equal(
				new Uint8Array([1]),
			);
			expect((await log.toArray())[1].payload.getValue()).to.deep.equal(
				new Uint8Array([2]),
			);
			expect((await log.toArray())[2].payload.getValue()).to.deep.equal(
				new Uint8Array([3]),
			);
		});
	});

	describe("size", () => {
		it("returns the sum of payloads", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			await log.append(new Uint8Array([1]));
			await log.append(new Uint8Array([2, 3]));
			await log.append(new Uint8Array([3, 4, 5]));
			const arr = await log.toArray();
			const size = arr.reduce(
				(acc, entry) => acc + entry.payload.byteLength,
				0,
			);
			expect(log.length).equal(3);
			expect(6n).equal(BigInt(size));
		});
	});

	describe("indexer", () => {
		it("unique", async () => {
			// TODO what is the purpose of this test?
			// if indices.scope is called we assert that scope needs to be created outside the open

			let indices = new HashmapIndices();

			const log1 = new Log();
			await log1.open(store, signKey, { indexer: await indices.scope("x") });

			const log2 = new Log();
			await log2.open(store, signKey, { indexer: await indices.scope("y") });
			await log1.append(new Uint8Array([0]));

			expect(await log1.toArray()).to.have.length(1);
			expect(await log2.toArray()).to.have.length(0);
		});
	});
});
