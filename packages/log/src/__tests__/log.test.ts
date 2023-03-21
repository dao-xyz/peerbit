import assert from "assert";
import rmrf from "rimraf";
import { Entry, Payload } from "../entry.js";
import { LamportClock as Clock, Timestamp } from "../clock.js";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import fs from "fs-extra";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { createBlock, getBlockValue } from "@dao-xyz/libp2p-direct-block";

import { LastWriteWins } from "../log-sorting.js";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { compare } from "@dao-xyz/uint8arrays";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

// For tiebreaker testing
const FirstWriteWins = (a: any, b: any) => LastWriteWins(a, b) * -1;

let signKey: KeyWithMeta<Ed25519Keypair>,
	signKey2: KeyWithMeta<Ed25519Keypair>,
	signKey3: KeyWithMeta<Ed25519Keypair>;

describe("Log", function () {
	let keystore: Keystore;
	let store: BlockStore;
	beforeAll(async () => {
		await fs.copy(
			signingKeysFixturesPath(__dirname),
			testKeyStorePath(__filenameBase)
		);

		keystore = new Keystore(
			await createStore(testKeyStorePath(__filenameBase))
		);
		const signKeys: KeyWithMeta<Ed25519Keypair>[] = [];
		for (let i = 0; i < 3; i++) {
			signKeys.push(
				(await keystore.getKey(
					new Uint8Array([i])
				)) as KeyWithMeta<Ed25519Keypair>
			);
		}
		signKeys.sort((a, b) =>
			compare(a.keypair.publicKey.publicKey, b.keypair.publicKey.publicKey)
		);
		// @ts-ignore
		signKey = signKeys[0];
		// @ts-ignore
		signKey2 = signKeys[1];
		// @ts-ignore
		signKey3 = signKeys[2];
		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();
		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});

	describe("constructor", () => {
		it("creates an empty log with default params", async () => {
			const log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
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

		it("sets an id", async () => {
			const log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "ABC" }
			);
			expect(log.id).toEqual("ABC");
		});

		it("generates id string if id is not passed as an argument", () => {
			const log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				undefined
			);
			assert.strictEqual(typeof log.id === "string", true);
		});

		it("sets items if given as params", async () => {
			const one = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryA",
				next: [],
				clock: new Clock({ id: new Uint8Array([0]), timestamp: 0 }),
			});
			const two = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryB",
				next: [],
				clock: new Clock({ id: new Uint8Array([1]), timestamp: 0 }),
			});
			const three = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryC",
				next: [],
				clock: new Clock({ id: new Uint8Array([2]), timestamp: 0 }),
			});
			const log = new Log<string>(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A" }
			);
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
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryA",
				next: [],
			});
			const two = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryB",
				next: [],
			});
			const three = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryC",
				next: [],
			});
			const log = new Log<string>(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "B" }
			);
			await log.reset([one, two, three], [three]);

			expect((await log.getHeads()).length).toEqual(1);
			expect((await log.getHeads())[0].hash).toEqual(three.hash);
		});

		it("finds heads if heads not given as params", async () => {
			const one = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryA",
				next: [],
			});
			const two = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryB",
				next: [],
			});
			const three = await Entry.create({
				store,
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
				gidSeed: Buffer.from("a"),
				data: "entryC",
				next: [],
			});
			const log = new Log<string>(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A" }
			);
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
			log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A" }
			);
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
			log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "AAA" }
			);
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
			log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "AAA" }
			);
			await log.append("one", { gidSeed: Buffer.from("a") });
		});

		it("changes identity", async () => {
			assert.deepStrictEqual(
				(await log.toArray())[0].metadata.clock.id,
				new Uint8Array(signKey.keypair.publicKey.bytes)
			);
			log.setIdentity({
				...signKey2.keypair,
				sign: signKey2.keypair.sign,
			});
			await log.append("two", { gidSeed: Buffer.from("a") });
			assert.deepStrictEqual(
				(await log.toArray())[1].metadata.clock.id,
				new Uint8Array(signKey2.keypair.publicKey.bytes)
			);
			log.setIdentity({
				...signKey3.keypair,
				sign: signKey3.keypair.sign,
			});
			await log.append("three", { gidSeed: Buffer.from("a") });
			assert.deepStrictEqual(
				(await log.toArray())[2].metadata.clock.id,
				new Uint8Array(signKey3.keypair.publicKey.bytes)
			);
		});
	});

	describe("has", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "AAA" }
			);
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

	describe("serialize", () => {
		let log: Log<string>,
			logId: string = "AAA";

		beforeEach(async () => {
			log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId }
			);
			await log.append("one", { gidSeed: Buffer.from("a") });
			await log.append("two", { gidSeed: Buffer.from("a") });
			await log.append("three", { gidSeed: Buffer.from("a") });
		});

		describe("toJSON", () => {
			it("returns the log in JSON format", async () => {
				expect(JSON.stringify(await log.toJSON())).toEqual(
					JSON.stringify({
						id: logId,
						heads: [(await log.toArray())[2].hash],
					})
				);
			});
		});

		describe("toSnapshot", () => {
			it("returns the log snapshot", async () => {
				const expectedData = {
					id: logId,
					heads: [(await log.toArray())[2].hash],
					values: (await log.toArray()).map((x) => x.hash),
				};
				const snapshot = log.toSnapshot();
				expect(snapshot.id).toEqual(expectedData.id);
				expect(snapshot.heads.length).toEqual(expectedData.heads.length);
				expect(snapshot.heads[0]).toEqual(expectedData.heads[0]);
				let values = await snapshot.values;
				expect(values.length).toEqual(expectedData.values.length);
				expect(values[0].hash).toEqual(expectedData.values[0]);
				expect(values[1].hash).toEqual(expectedData.values[1]);
				expect(values[2].hash).toEqual(expectedData.values[2]);
			});
		});

		describe("toMultihash - cbor", () => {
			it("returns the log as ipfs CID", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const hash = await log.toMultihash();
				expect(hash).toMatchSnapshot();
			});

			it("log serialized to ipfs contains the correct data", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const hash = await log.toMultihash();
				expect(hash).toMatchSnapshot();
				const result = (await getBlockValue((await store.get(hash))!)) as {
					heads: string[];
				};
				const heads = result.heads.map((x) => x.toString()); // base58btc
				expect(heads).toMatchSnapshot();
			});

			it("throws an error if log items is empty", async () => {
				const emptyLog = new Log(store, {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				});
				let err;
				try {
					await emptyLog.toMultihash();
				} catch (e: any) {
					err = e;
				}
				assert.notStrictEqual(err, null);
				expect(err.message).toEqual("Can't serialize an empty log");
			});
		});

		describe("fromMultihash", () => {
			it("creates a log from ipfs CID - one entry", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "X" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const hash = await log.toMultihash();
				const res = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ length: -1 }
				);
				expect(JSON.stringify(await res.toJSON())).toMatchSnapshot();
				expect(res.length).toEqual(1);
				expect((await res.toArray())[0].payload.getValue()).toEqual("one");
				expect((await res.toArray())[0].metadata.clock.id).toEqual(
					new Uint8Array(signKey.keypair.publicKey.bytes)
				);
				expect(
					(await res.toArray())[0].metadata.clock.timestamp.logical
				).toEqual(0);
			});

			it("creates a log from ipfs CID - three entries", async () => {
				const hash = await log.toMultihash();
				const res = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ length: -1 }
				);
				expect(res.length).toEqual(3);
				expect((await res.toArray())[0].payload.getValue()).toEqual("one");
				expect((await res.toArray())[1].payload.getValue()).toEqual("two");
				expect((await res.toArray())[2].payload.getValue()).toEqual("three");
			});

			it("creates a log from ipfs multihash (backwards compat)", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "X" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const multihash = await log.toMultihash();
				const res = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					multihash,
					{ length: -1 }
				);
				expect(JSON.stringify(await res.toJSON())).toMatchSnapshot();
				expect(res.length).toEqual(1);
				expect((await res.toArray())[0].payload.getValue()).toEqual("one");
				expect((await res.toArray())[0].metadata.clock.id).toEqual(
					new Uint8Array(signKey.keypair.publicKey.bytes)
				);
				expect(
					(await res.toArray())[0].metadata.clock.timestamp.logical
				).toEqual(0);
			});

			it("has the right sequence number after creation and appending", async () => {
				const hash = await log.toMultihash();
				const res = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ length: -1 }
				);
				expect(res.length).toEqual(3);
				await res.append("four");
				expect(res.length).toEqual(4);
				expect((await res.toArray())[3].payload.getValue()).toEqual("four");
			});

			it("creates a log from ipfs CID that has three heads", async () => {
				const log1 = new Log<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				const log2 = new Log<string>(
					store,
					{
						...signKey2.keypair,
						sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
					},
					{ logId: "A" }
				);
				const log3 = new Log<string>(
					store,
					{
						...signKey3.keypair,
						sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
					},
					{ logId: "A" }
				);
				await log1.append("one"); // order is determined by the identity's publicKey
				await log2.append("two");
				await log3.append("three");
				await log1.join(log2);
				await log1.join(log3);
				const hash = await log1.toMultihash();
				const res = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ length: -1 }
				);
				expect(res.length).toEqual(3);
				expect((await res.getHeads()).length).toEqual(3);
				expect(
					(await res.getHeads()).map((x) => x.payload.getValue())
				).toContainAllValues(["one", "two", "three"]);
			});

			it("creates a log from ipfs CID that has three heads w/ custom tiebreaker", async () => {
				const log1 = new Log<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				const log2 = new Log<string>(
					store,
					{
						...signKey2.keypair,
						sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
					},
					{ logId: "A" }
				);
				const log3 = new Log<string>(
					store,
					{
						...signKey3.keypair,
						sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
					},
					{ logId: "A" }
				);
				const e1 = await log1.append("one"); // order is determined by the identity's publicKey
				const e2 = await log2.append("two");
				const e3 = await log3.append("three");
				await log1.join(log2);
				await log1.join(log3);
				const hash = await log1.toMultihash();
				const res = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ sortFn: FirstWriteWins }
				);
				expect(res.length).toEqual(3);
				expect((await res.getHeads()).length).toEqual(3);
				expect((await res.getHeads()).map((x) => x.hash)).toContainAllValues([
					e1.entry.hash,
					e2.entry.hash,
					e3.entry.hash,
				]);
			});

			it("creates a log from ipfs CID up to a size limit", async () => {
				const amount = 100;
				const size = amount / 2;
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				for (let i = 0; i < amount; i++) {
					await log.append(i.toString(), {
						timestamp: new Timestamp({
							wallTime: 0n,
							logical: i,
						}),
					});
				}
				const hash = await log.toMultihash();
				const res = await Log.fromMultihash(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ length: size }
				);
				expect(res.length).toEqual(size);
			});

			it("creates a log from ipfs CID up without size limit", async () => {
				const amount = 100;
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				for (let i = 0; i < amount; i++) {
					await log.append(i.toString());
				}
				const hash = await log.toMultihash();
				const res = await Log.fromMultihash(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{ length: -1 }
				);
				expect(res.length).toEqual(amount);
			});

			it("throws an error when data from CID is not instance of Log", async () => {
				const hash = await store.put(await createBlock({}, "dag-cbor"));
				let err;
				try {
					await Log.fromMultihash(
						store,
						{
							...signKey.keypair,
							sign: async (data: Uint8Array) =>
								await signKey.keypair.sign(data),
						},
						hash.toString(),
						undefined as any
					);
				} catch (e: any) {
					err = e;
				}
				expect(err.message).toEqual("Given argument is not an instance of Log");
			});

			it("onProgress callback is fired for each entry", async () => {
				const amount = 100;
				const log = new Log<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "A" }
				);
				for (let i = 0; i < amount; i++) {
					await log.append(i.toString());
				}

				const items = await log.toArray();
				let i = 0;
				const loadProgressCallback = (entry: Entry<string>) => {
					assert.notStrictEqual(entry, null);
					expect(entry.hash).toEqual(items[items.length - i - 1].hash);
					expect(entry.payload.getValue()).toEqual(
						items[items.length - i - 1].payload.getValue()
					);
					i++;
				};

				const hash = await log.toMultihash();
				const result = await Log.fromMultihash<string>(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					hash,
					{
						length: -1,
						onFetched: loadProgressCallback,
					}
				);

				// Make sure the onProgress callback was called for each entry
				expect(i).toEqual(amount);
				// Make sure the log entries are correct ones
				expect(
					(await result.toArray())[0].metadata.clock.timestamp.logical
				).toEqual(0);
				expect((await result.toArray())[0].payload.getValue()).toEqual("0");
				expect(
					Timestamp.compare(
						(await result.toArray())[result.length - 1].metadata.clock
							.timestamp,
						(await result.toArray())[0].metadata.clock.timestamp
					)
				).toBeGreaterThan(0);
				expect(
					(await result.toArray())[result.length - 1].payload.getValue()
				).toEqual("99");
			});
		});

		describe("fromEntryHash", () => {
			it("calls fromEntryHash", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "X" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const res = await Log.fromEntryHash(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					(
						await log.toArray()
					)[0].hash,
					{ logId: log.id, length: -1 }
				);
				expect(JSON.stringify(await res.toJSON())).toMatchSnapshot();
			});
		});

		describe("fromMultihash", () => {
			it("calls fromMultihash", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "X" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const multihash = await log.toMultihash();
				const res = await Log.fromMultihash(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					multihash,
					{ length: -1 }
				);
				expect(JSON.stringify(await res.toJSON())).toMatchSnapshot();
			});

			it("calls fromMultihash with custom tiebreaker", async () => {
				const log = new Log(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					{ logId: "X" }
				);
				await log.append("one", {
					gidSeed: Buffer.from("a"),
					timestamp: new Timestamp({ wallTime: 0n, logical: 0 }),
				});
				const multihash = await log.toMultihash();
				const res = await Log.fromMultihash(
					store,
					{
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					multihash,
					{ length: -1, sortFn: FirstWriteWins }
				);
				expect(JSON.stringify(await res.toJSON())).toMatchSnapshot();
			});
		});
	});

	describe("values", () => {
		it("returns all entries in the log", async () => {
			const log = new Log<string>(store, {
				...signKey.keypair,
				sign: (data) => signKey.keypair.sign(data),
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
