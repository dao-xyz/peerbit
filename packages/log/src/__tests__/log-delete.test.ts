import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { EntryType } from "../entry.js";

describe("Log - Delete", function () {
	let store: BlockStore;
	let signKey: Ed25519Keypair;

	beforeEach(async () => {
		store = new MemoryLevelBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.open();
	});

	afterEach(async () => {
		await store.close();
	});

	const blockExists = async (hash: string): Promise<boolean> => {
		try {
			await (store as MemoryLevelBlockStore).idle();
			return !!(await store.get(hash, { timeout: 3000 }));
		} catch (error) {
			return false;
		}
	};

	describe("deleteRecursively", () => {
		it("deleted unreferences", async () => {
			const log = new Log<string>(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				{ logId: "A" }
			);
			const { entry: e1 } = await log.append("hello1");
			const { entry: e2 } = await log.append("hello2");
			const { entry: e3 } = await log.append("hello3");

			await log.deleteRecursively(e2);
			expect(log.nextsIndex.size).toEqual(0);
			expect((await log.toArray()).length).toEqual(1);
			expect(await log.get(e1.hash)).toBeUndefined();
			expect(await blockExists(e1.hash)).toBeFalse();
			expect(await log.get(e2.hash)).toBeUndefined();
			expect(await blockExists(e2.hash)).toBeFalse();
			expect(await log.get(e3.hash)).toBeDefined();
			expect(await blockExists(e3.hash)).toBeTrue();

			await log.deleteRecursively(e3);
			expect((await log.toArray()).length).toEqual(0);
			expect(await log.getHeads()).toHaveLength(0);
			expect(log.nextsIndex.size).toEqual(0);
			expect(log.entryIndex._cache.size).toEqual(0);
		});

		it("processes as long as alowed", async () => {
			const log = new Log<string>(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				{ logId: "A" }
			);
			const { entry: e1 } = await log.append("hello1");
			const { entry: e2 } = await log.append("hello2a");
			const { entry: e2b } = await log.append("hello2b", { nexts: [e2] });
			const { entry: e3 } = await log.append("hello3", {
				nexts: [e2],
				type: EntryType.CUT,
			});
			expect(await log.toArray()).toHaveLength(4);
			expect(log.nextsIndex.size).toEqual(2); // e1 ->  e2, e2 -> e2b
			await log.deleteRecursively(e2b);
			expect(log.nextsIndex.size).toEqual(0);
			expect((await log.toArray()).map((x) => x.hash)).toEqual([e3.hash]);
			expect(await log.get(e1.hash)).toBeUndefined();
			expect(await blockExists(e1.hash)).toBeFalse();
			expect(await log.get(e2.hash)).toBeUndefined();
			expect(await blockExists(e2.hash)).toBeFalse();
			expect(await log.get(e3.hash)).toBeDefined();
			expect(await blockExists(e3.hash)).toBeTrue();

			await log.deleteRecursively(e3);
			expect((await log.toArray()).length).toEqual(0);
			expect(await log.getHeads()).toHaveLength(0);
			expect(log.nextsIndex.size).toEqual(0);
			expect(log.entryIndex._cache.size).toEqual(0);
		});

		it("keeps references", async () => {
			const log = new Log<string>(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				{ logId: "A" }
			);
			const { entry: e1 } = await log.append("hello1");
			const { entry: e2a } = await log.append("hello2a", { nexts: [e1] });
			const { entry: e2b } = await log.append("hello2b", { nexts: [e1] });

			await log.deleteRecursively(e2a);
			expect(log.nextsIndex.size).toEqual(1);
			expect((await log.toArray()).length).toEqual(2);
			expect(await log.get(e1.hash)).toBeDefined();
			expect(await blockExists(e1.hash)).toBeTrue();
			expect(await log.get(e2a.hash)).toBeUndefined();
			expect(await blockExists(e2a.hash)).toBeFalse();
			expect(await log.get(e2b.hash)).toBeDefined();
			expect(await blockExists(e2b.hash)).toBeTrue();
			await log.deleteRecursively(e2b);
			expect((await log.toArray()).length).toEqual(0);
			expect(await log.getHeads()).toHaveLength(0);
			expect(log.nextsIndex.size).toEqual(0);
			expect(log.entryIndex._cache.size).toEqual(0);
		});
	});
});
