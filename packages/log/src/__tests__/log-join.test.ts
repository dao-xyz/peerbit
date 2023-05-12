import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Entry, EntryType } from "../entry.js";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { compare } from "@dao-xyz/uint8arrays";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { waitForPeers } from "@dao-xyz/libp2p-direct-stream";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>,
	signKey2: KeyWithMeta<Ed25519Keypair>,
	signKey3: KeyWithMeta<Ed25519Keypair>,
	signKey4: KeyWithMeta<Ed25519Keypair>;

const last = (arr: any[]) => {
	return arr[arr.length - 1];
};

const checkedStorage = async (log: Log<any>) => {
	for (const value of await log.values.toArray()) {
		expect(await log.storage.has(value.hash)).toBeTrue();
	}
};

describe("Log - Join", function () {
	let keystore: Keystore;
	let session: LSession;

	beforeAll(async () => {
		rmrf.sync(testKeyStorePath(__filenameBase));

		await fs.copy(
			signingKeysFixturesPath(__dirname),
			testKeyStorePath(__filenameBase)
		);

		keystore = new Keystore(
			await createStore(testKeyStorePath(__filenameBase))
		);

		// The ids are choosen so that the tests plays out "nicely", specifically the logs clock id sort will reflect the signKey suffix
		const keys: KeyWithMeta<Ed25519Keypair>[] = [];
		for (let i = 0; i < 4; i++) {
			keys.push(
				(await keystore.getKey(
					new Uint8Array([i])
				)) as KeyWithMeta<Ed25519Keypair>
			);
		}
		keys.sort((a, b) => {
			return compare(
				a.keypair.publicKey.publicKey,
				b.keypair.publicKey.publicKey
			);
		});
		signKey = keys[0];
		signKey2 = keys[1];
		signKey3 = keys[2];
		signKey4 = keys[3];
		session = await LSession.connected(3);
		await waitForPeers(
			session.peers[0].directblock,
			session.peers[1].directblock,
			session.peers[2].directblock
		);
	});

	afterAll(async () => {
		await session.stop();
		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});

	describe("join", () => {
		let log1: Log<string>,
			log2: Log<string>,
			log3: Log<string>,
			log4: Log<string>;

		beforeEach(async () => {
			log1 = new Log<string>();
			await log1.init(session.peers[0].directblock, {
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			});
			log2 = new Log<string>();
			await log2.init(session.peers[1].directblock, {
				...signKey2.keypair,
				sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
			});
			log3 = new Log<string>();
			await log3.init(session.peers[2].directblock, {
				...signKey3.keypair,
				sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
			});
			log4 = new Log<string>();
			await log4.init(
				session.peers[2].directblock, // [2] because we cannot create more than 3 peers when running tests in CI
				{
					...signKey4.keypair,
					sign: async (data: Uint8Array) => await signKey4.keypair.sign(data),
				}
			);
		});

		it("joins logs", async () => {
			const items1: Entry<string>[] = [];
			const items2: Entry<string>[] = [];
			const items3: Entry<string>[] = [];
			const amount = 100;

			for (let i = 1; i <= amount; i++) {
				const prev1 = last(items1);
				const prev2 = last(items2);
				const prev3 = last(items3);
				const n1 = await Entry.create({
					store: session.peers[0].directblock,
					identity: {
						...signKey.keypair,
						sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
					},
					gidSeed: Buffer.from("X" + i),
					data: "entryA" + i,
					next: prev1 ? [prev1] : undefined,
				});
				const n2 = await Entry.create({
					store: session.peers[0].directblock,
					identity: {
						...signKey2.keypair,
						sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
					},
					data: "entryB" + i,
					next: prev2 ? [prev2, n1] : [n1],
				});
				const n3 = await Entry.create({
					store: session.peers[1].directblock,
					identity: {
						...signKey3.keypair,
						sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
					},
					data: "entryC" + i,
					next: prev3 ? [prev3, n1, n2] : [n1, n2],
				});

				items1.push(n1);
				items2.push(n2);
				items3.push(n3);
			}

			// Here we're creating a log from entries signed by A and B
			// but we accept entries from C too
			const logA = await Log.fromEntry(
				session.peers[0].directblock,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				last(items2),
				{ timeout: 3000 }
			);

			// Here we're creating a log from entries signed by peer A, B and C
			// "logA" accepts entries from peer C so we can join logs A and B
			const logB = await Log.fromEntry(
				session.peers[1].directblock,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				last(items3),
				{ timeout: 3000 }
			);
			expect(logA.length).toEqual(items2.length + items1.length);
			expect(logB.length).toEqual(
				items3.length + items2.length + items1.length
			);

			expect((await logA.getHeads()).length).toEqual(1);
			await logA.join(await logB.getHeads());

			expect(logA.length).toEqual(
				items3.length + items2.length + items1.length
			);
			// The last Entry<T>, 'entryC100', should be the only head
			// (it points to entryB100, entryB100 and entryC99)
			expect((await logA.getHeads()).length).toEqual(1);

			await checkedStorage(logA);
			await checkedStorage(logB);
		});

		it("joins only unique items", async () => {
			await log1.append("helloA1");
			await log2.append("helloB1");
			await log1.append("helloA2");
			await log2.append("helloB2");
			await log1.join(await log2.getHeads());
			await log1.join(await log2.getHeads());

			const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

			expect(log1.length).toEqual(4);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);

			const item = last(await log1.toArray());
			expect(item.next.length).toEqual(1);
			expect((await log1.getHeads()).length).toEqual(2);
		});

		describe("cut", () => {
			let fetchEvents: number;
			let fetchHashes: Set<string>;
			let fromMultihash: any;
			beforeAll(() => {
				fetchEvents = 0;
				fetchHashes = new Set();
				fromMultihash = Entry.fromMultihash;

				// TODO monkeypatching might lead to sideeffects in other tests!
				Entry.fromMultihash = (s, h, o) => {
					fetchHashes.add(h);
					fetchEvents += 1;
					return fromMultihash(s, h, o);
				};
			});
			afterAll(() => {
				fetchHashes = new Set();
				fetchEvents = 0;
				Entry.fromMultihash = fromMultihash;
			});

			it("joins cut", async () => {
				const { entry: a1 } = await log1.append("helloA1");
				const { entry: b1 } = await log2.append("helloB1", {
					nexts: [a1],
					type: EntryType.CUT,
				});
				const { entry: a2 } = await log1.append("helloA2");
				await log1.join(await log2.getHeads());
				expect((await log1.toArray()).map((e) => e.hash)).toEqual([
					a1.hash,
					b1.hash,
					a2.hash,
				]);
				const { entry: b2 } = await log2.append("helloB1", {
					nexts: [a2],
					type: EntryType.CUT,
				});
				await log1.join(await log2.getHeads());
				expect((await log1.getHeads()).map((e) => e.hash)).toEqual([
					b1.hash,
					b2.hash,
				]);
				expect((await log1.toArray()).map((e) => e.hash)).toEqual([
					b1.hash,
					b2.hash,
				]);
			});

			it("will not reset if joining conflicting", async () => {
				const { entry: a1 } = await log1.append("helloA1");
				const b1 = await Entry.create({
					data: "helloB1",
					next: [a1],
					type: EntryType.CUT,
					identity: log1.identity,
					store: log1.storage,
				});
				const b2 = await Entry.create({
					data: "helloB2",
					next: [a1],
					type: EntryType.APPEND,
					identity: log1.identity,
					store: log1.storage,
				});

				// We need to store a1 somewhere else, becuse log1 will temporarely delete the block since due to the merge order
				// TODO make this work even though there is not a third party helping
				await log2.storage.get(a1.hash, { replicate: true });
				expect(await log2.storage.get(a1.hash)).toBeDefined();
				await log1.join([b1, b2]);
				expect((await log1.toArray()).map((e) => e.hash)).toEqual([
					a1.hash,
					b1.hash,
					b2.hash,
				]);
			});

			it("will not reset if joining conflicting (reversed)", async () => {
				const { entry: a1 } = await log1.append("helloA1");
				const b1 = await Entry.create({
					data: "helloB1",
					next: [a1],
					type: EntryType.APPEND,
					identity: log1.identity,
					store: log1.storage,
				});
				const b2 = await Entry.create({
					data: "helloB2",
					next: [a1],
					type: EntryType.CUT,
					identity: log1.identity,
					store: log1.storage,
				});
				await log1.join([b1, b2]);
				expect((await log1.toArray()).map((e) => e.hash)).toEqual([
					a1.hash,
					b1.hash,
					b2.hash,
				]);
			});

			it("joining multiple resets", async () => {
				const { entry: a1 } = await log2.append("helloA1");
				const { entry: b1 } = await log2.append("helloB1", {
					nexts: [a1],
					type: EntryType.CUT,
				});
				const { entry: b2 } = await log2.append("helloB2", {
					nexts: [a1],
					type: EntryType.CUT,
				});

				expect((await log2.getHeads()).map((x) => x.hash)).toEqual([
					b1.hash,
					b2.hash,
				]);
				fetchEvents = 0;
				await log1.join(await log2.getHeads());
				expect(fetchEvents).toEqual(0); // will not fetch a1 since b1 and b2 is CUT (no point iterating to nexts)
				expect((await log1.toArray()).map((e) => e.hash)).toEqual([
					b1.hash,
					b2.hash,
				]);
			});
		});

		it("joins heads", async () => {
			const { entry: a1 } = await log1.append("helloA1");
			const { entry: b1 } = await log2.append("helloB1", { nexts: [a1] });

			expect(log1.length).toEqual(1);
			expect(log2.length).toEqual(1);

			await log1.join(await log2.getHeads());
			const expectedData = ["helloA1", "helloB1"];
			expect(log1.length).toEqual(2);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);

			const item = last(await log1.toArray());
			expect(item.next.length).toEqual(1);
			expect((await log1.getHeads()).map((x) => x.hash)).toEqual([b1.hash]);
		});

		it("joins concurrently", async () => {
			let expectedData: string[] = [];
			let len = 2;
			let entries: Entry<any>[] = [];
			for (let i = 0; i < len; i++) {
				expectedData.push("" + i);
				entries.push((await log2.append("" + i)).entry);
			}
			let promises: Promise<any>[] = [];
			for (let i = 0; i < len; i++) {
				promises.push(log1.join([entries[i]]));
			}

			await Promise.all(promises);

			expect(log1.length).toEqual(len);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);

			const item = last(await log1.toArray());
			let allHeads = await log1.getHeads();
			expect(allHeads.length).toEqual(1);
			expect(item.next.length).toEqual(1);
		});

		it("joins with extra references", async () => {
			const e1 = await log1.append("helloA1");
			const e2 = await log1.append("helloA2");
			const e3 = await log1.append("helloA3");
			expect(log1.length).toEqual(3);
			await log2.join([e1.entry, e2.entry, e3.entry]);
			const expectedData = ["helloA1", "helloA2", "helloA3"];
			expect(log2.length).toEqual(3);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);
			const item = last(await log1.toArray());
			expect(item.next.length).toEqual(1);
			expect((await log1.getHeads()).length).toEqual(1);
		});

		it("joins logs two ways", async () => {
			const { entry: a1 } = await log1.append("helloA1");
			const { entry: b1 } = await log2.append("helloB1");
			const { entry: a2 } = await log1.append("helloA2");
			const { entry: b2 } = await log2.append("helloB2");
			await log1.join(await log2.getHeads());
			await log2.join(await log1.getHeads());

			const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

			expect(await log1.getHeads()).toContainAllValues([a2, b2]);
			expect(await log2.getHeads()).toContainAllValues([a2, b2]);
			expect(a2.next).toContainAllValues([a1.hash]);
			expect(b2.next).toContainAllValues([b1.hash]);

			expect((await log1.toArray()).map((e) => e.hash)).toEqual(
				(await log2.toArray()).map((e) => e.hash)
			);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);
			expect((await log2.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);
		});

		it("joins logs twice", async () => {
			const { entry: a1 } = await log1.append("helloA1");
			const { entry: b1 } = await log2.append("helloB1");
			await log2.join(await log1.getHeads());
			expect(log2.length).toEqual(2);
			expect(await log2.getHeads()).toContainAllValues([a1, b1]);

			const { entry: a2 } = await log1.append("helloA2");
			const { entry: b2 } = await log2.append("helloB2");
			await log2.join(await log1.getHeads());

			const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

			expect((await log2.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);
			expect(log2.length).toEqual(4);

			expect(await log1.getHeads()).toContainAllValues([a2]);
			expect(await log2.getHeads()).toContainAllValues([a2, b2]);
		});

		it("joins 2 logs two ways", async () => {
			await log1.append("helloA1");
			await log2.append("helloB1");
			await log2.join(await log1.getHeads());
			await log1.join(await log2.getHeads());
			const { entry: a2 } = await log1.append("helloA2");
			const { entry: b2 } = await log2.append("helloB2");
			await log2.join(await log1.getHeads());

			const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];

			expect(log2.length).toEqual(4);
			assert.deepStrictEqual(
				(await log2.toArray()).map((e) => e.payload.getValue()),
				expectedData
			);

			expect(await log1.getHeads()).toContainAllValues([a2]);
			expect(await log2.getHeads()).toContainAllValues([a2, b2]);
		});

		it("joins 2 logs two ways and has the right heads at every step", async () => {
			await log1.append("helloA1");
			expect((await log1.getHeads()).length).toEqual(1);
			expect((await log1.getHeads())[0].payload.getValue()).toEqual("helloA1");

			await log2.append("helloB1");
			expect((await log2.getHeads()).length).toEqual(1);
			expect((await log2.getHeads())[0].payload.getValue()).toEqual("helloB1");

			await log2.join(await log1.getHeads());
			expect((await log2.getHeads()).length).toEqual(2);
			expect((await log2.getHeads())[0].payload.getValue()).toEqual("helloB1");
			expect((await log2.getHeads())[1].payload.getValue()).toEqual("helloA1");

			await log1.join(await log2.getHeads());
			expect((await log1.getHeads()).length).toEqual(2);
			expect((await log1.getHeads())[1].payload.getValue()).toEqual("helloB1");
			expect((await log1.getHeads())[0].payload.getValue()).toEqual("helloA1");

			await log1.append("helloA2");
			expect((await log1.getHeads()).length).toEqual(1);
			expect((await log1.getHeads())[0].payload.getValue()).toEqual("helloA2");

			await log2.append("helloB2");
			expect((await log2.getHeads()).length).toEqual(1);
			expect((await log2.getHeads())[0].payload.getValue()).toEqual("helloB2");

			await log2.join(await log1.getHeads());
			expect((await log2.getHeads()).length).toEqual(2);
			expect((await log2.getHeads())[0].payload.getValue()).toEqual("helloB2");
			expect((await log2.getHeads())[1].payload.getValue()).toEqual("helloA2");
		});

		it("joins 4 logs to one", async () => {
			// order determined by identity's publicKey
			await log1.append("helloA1");
			await log2.append("helloB1");
			await log3.append("helloC1");
			await log4.append("helloD1");
			const { entry: a2 } = await log1.append("helloA2");
			const { entry: b2 } = await log2.append("helloB2");
			const { entry: c2 } = await log3.append("helloC2");
			const { entry: d2 } = await log4.append("helloD2");
			await log1.join(await log2.getHeads());
			await log1.join(await log3.getHeads());
			await log1.join(await log4.getHeads());

			const expectedData = [
				"helloA1",
				"helloB1",
				"helloC1",
				"helloD1",
				"helloA2",
				"helloB2",
				"helloC2",
				"helloD2",
			];

			expect(log1.length).toEqual(8);
			expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
				expectedData
			);

			expect(await log1.getHeads()).toContainAllValues([a2, b2, c2, d2]);
		});

		it("joins 4 logs to one is commutative", async () => {
			await log1.append("helloA1");
			await log1.append("helloA2");
			await log2.append("helloB1");
			await log2.append("helloB2");
			await log3.append("helloC1");
			await log3.append("helloC2");
			await log4.append("helloD1");
			await log4.append("helloD2");
			await log1.join(await log2.getHeads());
			await log1.join(await log3.getHeads());
			await log1.join(await log4.getHeads());
			await log2.join(await log1.getHeads());
			await log2.join(await log3.getHeads());
			await log2.join(await log4.getHeads());

			expect(log1.length).toEqual(8);
			assert.deepStrictEqual(
				(await log1.toArray()).map((e) => e.payload.getValue()),
				(await log2.toArray()).map((e) => e.payload.getValue())
			);
		});

		it("joins logs and updates clocks", async () => {
			const { entry: a1 } = await log1.append("helloA1");
			const { entry: b1 } = await log2.append("helloB1");
			await log2.join(await log1.getHeads());
			const { entry: a2 } = await log1.append("helloA2");
			const { entry: b2 } = await log2.append("helloB2");

			expect(a2.metadata.clock.id).toEqual(signKey.keypair.publicKey.bytes);
			expect(b2.metadata.clock.id).toEqual(signKey2.keypair.publicKey.bytes);
			expect(
				a2.metadata.clock.timestamp.compare(a1.metadata.clock.timestamp)
			).toBeGreaterThan(0);
			expect(
				b2.metadata.clock.timestamp.compare(b1.metadata.clock.timestamp)
			).toBeGreaterThan(0);

			await log3.join(await log1.getHeads());

			await log3.append("helloC1");
			const { entry: c2 } = await log3.append("helloC2");
			await log1.join(await log3.getHeads());
			await log1.join(await log2.getHeads());
			await log4.append("helloD1");
			const { entry: d2 } = await log4.append("helloD2");
			await log4.join(await log2.getHeads());
			await log4.join(await log1.getHeads());
			await log4.join(await log3.getHeads());
			const { entry: d3 } = await log4.append("helloD3");
			expect(d3.gid).toEqual(c2.gid); // because c2 is the longest
			await log4.append("helloD4");
			await log1.join(await log4.getHeads());
			await log4.join(await log1.getHeads());
			const { entry: d5 } = await log4.append("helloD5");
			expect(d5.gid).toEqual(c2.gid); // because c2 previously

			const { entry: a5 } = await log1.append("helloA5");
			expect(a5.gid).toEqual(c2.gid); // because log1 joined with lgo4 and log4 was c2 (and len log4 > log1)

			await log4.join(await log1.getHeads());
			const { entry: d6 } = await log4.append("helloD6");
			expect(d5.gid).toEqual(a5.gid);
			expect(d6.gid).toEqual(a5.gid);

			const expectedData = [
				{
					payload: "helloA1",
					gid: a1.gid,
				},
				{
					payload: "helloB1",
					gid: b1.gid,
				},

				{
					payload: "helloA2",
					gid: a2.gid,
				},
				{
					payload: "helloB2",
					gid: b2.gid,
				},
				{
					payload: "helloC1",
					gid: a1.gid,
				},
				{
					payload: "helloC2",
					gid: c2.gid,
				},
				{
					payload: "helloD1",
					gid: d2.gid,
				},
				{
					payload: "helloD2",
					gid: d2.gid,
				},
				{
					payload: "helloD3",
					gid: d3.gid,
				},
				{
					payload: "helloD4",
					gid: d3.gid,
				},
				{
					payload: "helloD5",
					gid: d5.gid,
				},
				{
					payload: "helloA5",
					gid: a5.gid,
				},
				{
					payload: "helloD6",
					gid: d6.gid,
				},
			];

			const transformed = (await log4.toArray()).map((e) => {
				return {
					payload: e.payload.getValue(),
					gid: e.gid,
				};
			});

			expect(log4.length).toEqual(13);
			expect(transformed).toEqual(expectedData);
		});

		it("joins logs from 4 logs", async () => {
			const { entry: a1 } = await log1.append("helloA1");
			await log1.join(await log2.getHeads());
			const { entry: b1 } = await log2.append("helloB1");
			await log2.join(await log1.getHeads());
			const { entry: a2 } = await log1.append("helloA2");
			await log2.append("helloB2");

			await log1.join(await log3.getHeads());
			// Sometimes failes because of clock ids are random TODO Fix
			expect(
				(await log1.getHeads())[(await log1.getHeads()).length - 1].gid
			).toEqual(a1.gid);
			expect(a2.metadata.clock.id).toEqual(signKey.keypair.publicKey.bytes);
			expect(
				a2.metadata.clock.timestamp.compare(a1.metadata.clock.timestamp)
			).toBeGreaterThan(0);

			await log3.join(await log1.getHeads());
			expect(
				(await log3.getHeads())[(await log3.getHeads()).length - 1].gid
			).toEqual(a1.gid); // because longest

			await log3.append("helloC1");
			await log3.append("helloC2");
			await log1.join(await log3.getHeads());
			await log1.join(await log2.getHeads());
			await log4.append("helloD1");
			await log4.append("helloD2");
			await log4.join(await log2.getHeads());
			await log4.join(await log1.getHeads());
			await log4.join(await log3.getHeads());
			await log4.append("helloD3");
			const { entry: d4 } = await log4.append("helloD4");

			expect(d4.metadata.clock.id).toEqual(signKey4.keypair.publicKey.bytes);

			const expectedData = [
				"helloA1",
				"helloB1",
				"helloA2",
				"helloB2",
				"helloC1",
				"helloC2",
				"helloD1",
				"helloD2",
				"helloD3",
				"helloD4",
			];

			expect(log4.length).toEqual(10);
			assert.deepStrictEqual(
				(await log4.toArray()).map((e) => e.payload.getValue()),
				expectedData
			);
		});

		describe("gid shadow callback", () => {
			it("it emits callback when gid is shadowed, triangle shape", async () => {
				/*  
				Either A or B shaded
				┌─┐┌─┐  
				│a││b│  
				└┬┘└┬┘  
				┌▽──▽──┐
				│a or b│
				└──────┘
				*/

				const { entry: a1 } = await log1.append("helloA1", {
					nexts: [],
				});
				const { entry: b1 } = await log1.append("helloB1", {
					nexts: [],
				});
				let callbackValue: string[] = undefined as any;
				const { entry: ab1 } = await log1.append("helloAB1", {
					nexts: [a1, b1],
					onGidsShadowed: (gids) => (callbackValue = gids),
				});
				expect(callbackValue).toHaveLength(1);
				expect(callbackValue[0]).toEqual(ab1.gid === a1.gid ? b1.gid : a1.gid); // if ab1 has gid a then b will be shadowed
			});

			it("it emits callback when gid is shadowed, N shape", async () => {
				/*  
					No shadows
					┌──┐┌───┐ 
					│a0││b1 │ 
					└┬─┘└┬─┬┘ 
					┌▽─┐ │┌▽─┐
					│a1│ ││b2│
					└┬─┘ │└──┘
					┌▽───▽┐   
					│a2   │   
					└─────┘   
				*/

				const { entry: a0 } = await log1.append("helloA0", {
					nexts: [],
				});
				const { entry: a1 } = await log1.append("helloA1", {
					nexts: [a0],
				});
				const { entry: b1 } = await log1.append("helloB1", {
					nexts: [],
				});
				await log1.append("helloB2", { nexts: [b1] });

				let callbackValue: any;
				// make sure gid is choosen from 1 bs

				await log1.append("helloA2", {
					nexts: [a1, b1],
					onGidsShadowed: (gids) => (callbackValue = gids),
				});
				expect(callbackValue).toBeUndefined();
			});
		});

		describe("entry-with-references", () => {
			let fetchCounter = 0;
			let joinEntryCounter = 0;
			let fromMultihashOrg: any;
			beforeAll(() => {
				fromMultihashOrg = Entry.fromMultihash;
				Entry.fromMultihash = (s, h, o) => {
					fetchCounter += 1;
					return fromMultihashOrg(s, h, o);
				};
			});
			afterAll(() => {
				Entry.fromMultihash = fromMultihashOrg;
			});

			beforeEach(() => {
				const joinEntryFn = log2["joinEntry"].bind(log2);
				log2["joinEntry"] = (e, n, s, o) => {
					joinEntryCounter += 1;
					return joinEntryFn(e, n, s, o);
				};
				fetchCounter = 0;
				joinEntryCounter = 0;
			});

			it("joins with references", async () => {
				const { entry: a1 } = await log1.append("helloA1");
				const { entry: a2 } = await log1.append("helloA2", { nexts: [a1] });
				await log2.join([{ entry: a2, references: [a1] }]);
				expect(log2.values.length).toEqual(2);
				expect(fetchCounter).toEqual(0); // no fetches since all entries where passed
				expect(joinEntryCounter).toEqual(2);
			});
		});
		// TODO move this into the prune test file
		describe("join and prune", () => {
			beforeEach(async () => {
				await log1.append("helloA1");
				await log2.append("helloB1");
				await log1.append("helloA2");
				await log2.append("helloB2");
			});

			it("joins only specified amount of entries - one entry", async () => {
				await log1.join(await log2.getHeads());
				await log1.trim({ type: "length", to: 1 });

				const expectedData = ["helloB2"];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).toEqual(1);
				assert.deepStrictEqual(
					(await log1.toArray()).map((e) => e.payload.getValue()),
					expectedData
				);
				expect(lastEntry.next.length).toEqual(1);
			});

			it("joins only specified amount of entries - two entries", async () => {
				await log1.join(await log2.getHeads());
				await log1.trim({ type: "length", to: 2 });

				const expectedData = ["helloA2", "helloB2"];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).toEqual(2);
				expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
					expectedData
				);
				expect(lastEntry.next.length).toEqual(1);
			});

			it("joins only specified amount of entries - three entries", async () => {
				await log1.join(await log2.getHeads());
				await log1.trim({ type: "length", to: 3 });

				const expectedData = ["helloB1", "helloA2", "helloB2"];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).toEqual(3);
				expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
					expectedData
				);
				expect(lastEntry.next.length).toEqual(1);
			});

			it("joins only specified amount of entries - (all) four entries", async () => {
				await log1.join(await log2.getHeads());
				await log1.trim({ type: "length", to: 4 });

				const expectedData = ["helloA1", "helloB1", "helloA2", "helloB2"];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).toEqual(4);
				expect((await log1.toArray()).map((e) => e.payload.getValue())).toEqual(
					expectedData
				);
				expect(lastEntry.next.length).toEqual(1);
			});
		});
	});
});
