import assert from "assert";
import { Entry } from "../entry.js";
import { LamportClock as Clock, Timestamp } from "../clock.js";
import { Log } from "../log.js";
import { BlockStore, MemoryLevelBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { compare } from "@dao-xyz/uint8arrays";
import { signKey, signKey2, signKey3 } from "./fixtures/privateKey.js";

describe("Log", function () {
	let store: BlockStore;
	beforeAll(async () => {
		store = new MemoryLevelBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});

	describe("constructor", () => {
		it("creates an empty log with default params", async () => {
			const log = new Log();
			await log.open(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				undefined
			);
			assert.notStrictEqual(log.entryIndex, null);
			assert.notStrictEqual(log.headsIndex, null);
			assert.notStrictEqual(log.id, null);
			assert.notStrictEqual(log.id, null);
			assert.notStrictEqual(log.toArray(), null);
			assert.notStrictEqual(await log.getHeads(), null);
			assert.deepStrictEqual(await log.toArray(), []);
			assert.deepStrictEqual(await log.getHeads(), []);
			assert.deepStrictEqual(await log.getTails(), []);
			assert.deepStrictEqual(await log.getTailHashes(), []);
		});

		it("can not setup after open", async () => {
			const log = new Log();
			await log.open(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				undefined
			);
			await expect(() => log.setup()).rejects.toThrow();
		});
		it("sets an id", async () => {
			const log = new Log({ id: new Uint8Array(1) });
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			expect(log.id).toEqual(new Uint8Array(1));
		});

		it("generates if id is not passed as an argument", async () => {
			const log = new Log();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			expect(log.id).toBeInstanceOf(Uint8Array);
		});

		it("sets items if given as params", async () => {
			const one = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryA",
				next: [],
				clock: new Clock({ id: new Uint8Array([0]), timestamp: 0 }),
			});
			const two = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryB",
				next: [],
				clock: new Clock({ id: new Uint8Array([1]), timestamp: 0 }),
			});
			const three = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryC",
				next: [],
				clock: new Clock({ id: new Uint8Array([2]), timestamp: 0 }),
			});
			const log = new Log<string>();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.reset([one, two, three]);

			expect(log.length).toEqual(3);
			expect((await log.toArray())[0].payload.getValue()).toEqual("entryA");
			expect((await log.toArray())[1].payload.getValue()).toEqual("entryB");
			expect((await log.toArray())[2].payload.getValue()).toEqual("entryC");
		});

		it("sets heads if given as params", async () => {
			const one = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryA",
				next: [],
			});
			const two = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryB",
				next: [],
			});
			const three = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryC",
				next: [],
			});
			const log = new Log<string>();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.reset([one, two, three], [three]);

			expect((await log.getHeads()).length).toEqual(1);
			expect((await log.getHeads())[0].hash).toEqual(three.hash);
		});

		it("finds heads if heads not given as params", async () => {
			const one = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryA",
				next: [],
			});
			const two = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryB",
				next: [],
			});
			const three = await Entry.create({
				store,
				identity: {
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryC",
				next: [],
			});
			const log = new Log<string>();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.reset([one, two, three]);
			expect((await log.getHeads()).map((x) => x.hash)).toContainAllValues([
				one.hash,
				two.hash,
				three.hash,
			]);
		});
	});

	describe("toString", () => {
		let log: Log<string>;
		const expectedData =
			'"five"\n└─"four"\n  └─"three"\n    └─"two"\n      └─"one"';

		beforeEach(async () => {
			log = new Log<string>();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.append("one", { gidSeed: Buffer.from("a") });
			await log.append("two", { gidSeed: Buffer.from("a") });
			await log.append("three", { gidSeed: Buffer.from("a") });
			await log.append("four", { gidSeed: Buffer.from("a") });
			await log.append("five", { gidSeed: Buffer.from("a") });
		});

		it("returns a nicely formatted string", async () => {
			expect(await log.toString((p) => Buffer.from(p.data).toString())).toEqual(
				expectedData
			);
		});
	});

	describe("get", () => {
		let log: Log<any>;

		beforeEach(async () => {
			log = new Log<string>();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.append("one", {
				gidSeed: Buffer.from("a"),
				timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
			});
		});

		it("returns an Entry", async () => {
			const entry = await log.get((await log.toArray())[0].hash)!;
			expect(entry?.hash).toMatchSnapshot();
		});

		it("returns undefined when Entry is not in the log", async () => {
			const entry = await log.get(
				"zb2rhbnwihVVVVEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J"
			);
			assert.deepStrictEqual(entry, undefined);
		});
	});

	describe("setIdentity", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.append("one", { gidSeed: Buffer.from("a") });
		});

		it("changes identity", async () => {
			expect((await log.toArray())[0].metadata.clock.id).toEqual(
				signKey.publicKey.bytes
			);
			log.setIdentity(signKey2);
			await log.append("two", { gidSeed: Buffer.from("a") });
			assert.deepStrictEqual(
				(await log.toArray())[1].metadata.clock.id,
				signKey2.publicKey.bytes
			);
			log.setIdentity(signKey3);
			await log.append("three", { gidSeed: Buffer.from("a") });
			assert.deepStrictEqual(
				(await log.toArray())[2].metadata.clock.id,
				signKey3.publicKey.bytes
			);
		});
	});

	describe("has", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, {
				...signKey,
				sign: async (data: Uint8Array) => await signKey.sign(data),
			});
			await log.append("one", { gidSeed: Buffer.from("a") });
		});

		it("returns true if it has an Entry", async () => {
			assert(log.has((await log.toArray())[0].hash));
		});

		it("returns true if it has an Entry, hash lookup", async () => {
			assert(log.has((await log.toArray())[0].hash));
		});

		it("returns false if it doesn't have the Entry", async () => {
			expect(
				await log.has("zb2rhbnwihVVVVEVVPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J")
			).toEqual(false);
		});
	});

	describe("values", () => {
		it("returns all entries in the log", async () => {
			const log = new Log<string>();
			await log.open(store, {
				...signKey,
				sign: (data) => signKey.sign(data),
			});
			expect((await log.toArray()) instanceof Array).toEqual(true);
			expect(log.length).toEqual(0);
			await log.append("hello1");
			await log.append("hello2");
			await log.append("hello3");
			expect((await log.toArray()) instanceof Array).toEqual(true);
			expect(log.length).toEqual(3);
			expect((await log.toArray())[0].payload.getValue()).toEqual("hello1");
			expect((await log.toArray())[1].payload.getValue()).toEqual("hello2");
			expect((await log.toArray())[2].payload.getValue()).toEqual("hello3");
		});
	});
});
