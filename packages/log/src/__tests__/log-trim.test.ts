import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";

import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";

import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";
import { waitFor } from "@dao-xyz/peerbit-time";
const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>;

describe("Append trim", function () {
	let keystore: Keystore, store: BlockStore;

	beforeAll(async () => {
		rmrf.sync(testKeyStorePath(__filenameBase));

		await fs.copy(
			signingKeysFixturesPath(__dirname),
			testKeyStorePath(__filenameBase)
		);

		keystore = new Keystore(
			await createStore(testKeyStorePath(__filenameBase))
		);

		//@ts-ignore
		signKey = await keystore.getKey(new Uint8Array([0]));

		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});

	it("cut back to max oplog length", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				logId: "A",
				trim: {
					type: "length",
					from: 1,
					to: 1,
					filter: { canTrim: () => true },
				},
			}
		);
		await log.append("hello1");
		await log.trim();
		await log.append("hello2");
		await log.trim();
		await log.append("hello3");
		await log.trim();
		expect(log.length).toEqual(1);
		expect((await log.toArray())[0].payload.getValue()).toEqual("hello3");
	});

	it("respect canTrim for length type", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1", { nexts: [] }); // set nexts [] so all get unique gids
		const e2 = await log.append("hello2", { nexts: [] }); // set nexts [] so all get unique gids
		const e3 = await log.append("hello3", { nexts: [] }); // set nexts [] so all get unique gids
		await log.trim({
			type: "length",
			from: 2,
			to: 2,
			filter: {
				canTrim: (gid) => {
					canTrimInvocations += 1;
					return Promise.resolve(gid !== e1.entry.gid);
				},
			},
		});
		expect(log.length).toEqual(2);
		expect((await log.toArray())[0].payload.getValue()).toEqual("hello1");
		expect((await log.toArray())[1].payload.getValue()).toEqual("hello3");
		expect(canTrimInvocations).toEqual(2);
	});

	it("not recheck untrimmable gid", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1");
		const e2 = await log.append("hello2");
		const e3 = await log.append("hello3");
		await log.trim({
			type: "length",
			from: 2,
			to: 2,
			filter: {
				canTrim: (gid) => {
					canTrimInvocations += 1;
					return Promise.resolve(false);
				},
			},
		});
		expect(log.length).toEqual(3);
		expect(canTrimInvocations).toEqual(1);
	});

	it("not recheck gid in cache", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1", { nexts: [] }); // nexts: [] means unique gid
		const e2 = await log.append("hello2", { nexts: [] }); // nexts: [] means unique gid
		const e3 = await log.append("hello3", { nexts: [] }); // nexts: [] means unique gid
		const canTrim = (gid) => {
			canTrimInvocations += 1;
			return Promise.resolve(gid !== e1.entry.gid); // can not trim
		};
		const cacheId = () => "";
		await log.trim({
			type: "length",
			from: 2,
			to: 2,
			filter: {
				canTrim,
				cacheId,
			},
		});
		expect(log.length).toEqual(2);
		expect(canTrimInvocations).toEqual(2); // checks e1 then e2 (e2 we can delete)

		await log.trim({
			type: "length",
			from: 1,
			to: 1,
			filter: {
				canTrim,
				cacheId,
			},
		});

		expect(log.length).toEqual(1);
		expect(canTrimInvocations).toEqual(3); // Will start at e3 (and not loop around because tail and head is the same)
	});

	it("ignores invalid trim cache", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1", { nexts: [] }); // nexts: [] means unique gid
		const e2 = await log.append("hello2", { nexts: [] }); // nexts: [] means unique gid
		const e3 = await log.append("hello3", { nexts: [] }); // nexts: [] means unique gid
		const e4 = await log.append("hello4", { nexts: [] }); // nexts: [] means unique gid
		const canTrim = (gid) => {
			canTrimInvocations += 1;
			return Promise.resolve(gid !== e1.entry.gid); // can not trim
		};

		const cacheId = () => "";

		await log.trim({
			type: "length",
			from: 3,
			to: 3,
			filter: {
				canTrim,
				cacheId,
			},
		});

		expect(canTrimInvocations).toEqual(2); // checks e1 then e2 (e2 we can delete)
		await log.delete(e3.entry); // e3 is also cached as the next node to trim
		await log.trim({
			type: "length",
			from: 1,
			to: 1,
			filter: {
				canTrim,
				cacheId,
			},
		});

		expect(log.length).toEqual(1);
		expect(canTrimInvocations).toEqual(3); // Will start at e4 because e3 is cache is gone
	});

	it("uses trim cache cross sessions", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1", { nexts: [] }); // nexts: [] means unique gid
		const e2 = await log.append("hello2", { nexts: [] }); // nexts: [] means unique gid
		const e3 = await log.append("hello3", { nexts: [] }); // nexts: [] means unique gid
		const canTrim = (gid) => {
			canTrimInvocations += 1;
			return Promise.resolve(false); // can not trim
		};

		const cacheId = () => "";

		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim,
				cacheId,
			},
		});
		expect(canTrimInvocations).toEqual(3); // checks e1 then e2 (e2 we can delete)
		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim,
				cacheId,
			},
		});

		expect(canTrimInvocations).toEqual(3);

		const e4 = await log.append("hello4", { nexts: [] }); // nexts: [] means unique gid
		const result = await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim,
				cacheId,
			},
		});
		expect(canTrimInvocations).toEqual(4); // check e4 and check e1 again
	});

	it("drops cache if canTrim function changes", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1", { nexts: [] }); // nexts: [] means unique gid
		const e2 = await log.append("hello2", { nexts: [] }); // nexts: [] means unique gid
		const e3 = await log.append("hello3", { nexts: [] }); // nexts: [] means unique gid
		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim: (gid) => {
					canTrimInvocations += 1;
					return Promise.resolve(false); // can not trim
				},
			},
		});
		expect(canTrimInvocations).toEqual(3); // checks e1 then e2 (e2 we can delete)
		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim: (gid) => {
					canTrimInvocations += 1;
					return Promise.resolve(false); // can not trim
				},
			},
		});

		expect(canTrimInvocations).toEqual(6);
	});

	it("changing cacheId will reset cache", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		let canTrimInvocations = 0;
		const e1 = await log.append("hello1", { nexts: [] }); // nexts: [] means unique gid
		const e2 = await log.append("hello2", { nexts: [] }); // nexts: [] means unique gid
		const e3 = await log.append("hello3", { nexts: [] }); // nexts: [] means unique gid

		let trimGid: string | undefined = undefined;
		const canTrim = (gid) => {
			canTrimInvocations += 1;
			return Promise.resolve(gid === trimGid); // can not trim
		};
		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim,
				cacheId: () => "a",
			},
		});

		trimGid = e1.entry.gid;
		expect(canTrimInvocations).toEqual(3);
		expect(log.length).toEqual(3);

		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim,
				cacheId: () => "a",
			},
		});

		expect(canTrimInvocations).toEqual(3);
		expect(log.length).toEqual(3);
		await log.trim({
			type: "length",
			from: 0,
			to: 0,
			filter: {
				canTrim,
				cacheId: () => "b",
			},
		});
		expect(log.length).toEqual(2);
		expect(canTrimInvocations).toEqual(6); // cache resets, so will go through all entries
	});

	it("cut back to cut length", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A", trim: { type: "length", from: 3, to: 1 } } // when length > 3 cut back to 1
		);
		const { entry: a1 } = await log.append("hello1");
		const { entry: a2 } = await log.append("hello2");
		expect(await log.trim()).toHaveLength(0);
		expect(await log.storage.get(a1.hash)).toBeDefined();
		expect(await log.storage.get(a2.hash)).toBeDefined();
		expect(log.length).toEqual(2);
		const { entry: a3, removed } = await log.append("hello3");
		expect(removed.map((x) => x.hash)).toContainAllValues([a1.hash, a2.hash]);
		expect(log.length).toEqual(1);
		await (log.storage as MemoryLevelBlockStore).idle();
		expect(await log.storage.get(a1.hash)).toBeUndefined();
		expect(await log.storage.get(a2.hash)).toBeUndefined();
		expect(await log.storage.get(a3.hash)).toBeDefined();
		expect((await log.toArray())[0].payload.getValue()).toEqual("hello3");
	});

	it("trimming and concurrency", async () => {
		let canTrimInvocations = 0;
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				logId: "A",
				trim: {
					type: "length",
					from: 1,
					to: 1,
					filter: {
						canTrim: () => {
							canTrimInvocations += 1;
							return true;
						},
					},
				},
			} // when length > 3 cut back to 1
		);
		let promises: Promise<any>[] = [];
		for (let i = 0; i < 100; i++) {
			promises.push(log.append("hello" + i));
		}
		await Promise.all(promises);
		expect(canTrimInvocations).toBeLessThan(100); // even though conc. trimming is sync
		expect(log.length).toEqual(1);
		expect((await log.toArray())[0].payload.getValue()).toEqual("hello99");
	});

	it("cut back to bytelength", async () => {
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				logId: "A",
				trim: { type: "bytelength", to: 15, filter: { canTrim: () => true } },
			} // bytelength is 15 so for every new helloX we hav eto delete the previous helloY
		);
		const { entry: a1, removed: r1 } = await log.append("hello1");
		expect(r1).toHaveLength(0);
		expect(await log.storage.get(a1.hash)).toBeDefined();
		expect((await log.toArray()).map((x) => x.payload.getValue())).toEqual([
			"hello1",
		]);
		const { entry: a2, removed: r2 } = await log.append("hello2");
		expect(r2.map((x) => x.hash)).toContainAllValues([a1.hash]);
		expect(await log.storage.get(a2.hash)).toBeDefined();
		expect((await log.toArray()).map((x) => x.payload.getValue())).toEqual([
			"hello2",
		]);
		const { entry: a3, removed: r3 } = await log.append("hello3");
		expect(r3.map((x) => x.hash)).toContainAllValues([a2.hash]);
		expect(await log.storage.get(a3.hash)).toBeDefined();
		expect((await log.toArray()).map((x) => x.payload.getValue())).toEqual([
			"hello3",
		]);
		const { entry: a4, removed: r4 } = await log.append("hello4");
		expect(r4.map((x) => x.hash)).toContainAllValues([a3.hash]);
		expect((await log.toArray()).map((x) => x.payload.getValue())).toEqual([
			"hello4",
		]);
		await (log.storage as MemoryLevelBlockStore).idle();
		expect(await log.storage.get(a1.hash)).toBeUndefined();
		expect(await log.storage.get(a2.hash)).toBeUndefined();
		expect(await log.storage.get(a3.hash)).toBeUndefined();
		expect(await log.storage.get(a4.hash)).toBeDefined();
	});

	it("trim to time", async () => {
		const maxAge = 3000;
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				logId: "A",
				trim: { type: "time", maxAge },
			} // bytelength is 15 so for every new helloX we hav eto delete the previous helloY
		);

		let t0 = +new Date();
		const { entry: a1, removed: r1 } = await log.append("hello1");
		expect(r1).toHaveLength(0);
		expect(await log.storage.get(a1.hash)).toBeDefined();
		expect((await log.toArray()).map((x) => x.payload.getValue())).toEqual([
			"hello1",
		]);
		const { entry: a2, removed: r2 } = await log.append("hello2");
		expect(r2.map((x) => x.hash)).toContainAllValues([]);

		await waitFor(() => +new Date() - t0 > maxAge);
		const { entry: a3, removed: r3 } = await log.append("hello2");
		expect(r3.map((x) => x.hash)).toContainAllValues([a1.hash, a2.hash]);
	});
});
