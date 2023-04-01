import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";

import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { EntryType } from "../entry.js";

describe("Log - Append", function () {
	let store: BlockStore;
	let signKey: Ed25519Keypair;

	const blockExists = async (hash: string): Promise<boolean> => {
		try {
			await (store as MemoryLevelBlockStore).idle();
			return !!(await store.get(hash, { timeout: 3000 }));
		} catch (error) {
			return false;
		}
	};

	beforeAll(async () => {
		store = new MemoryLevelBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.open();
	});

	afterAll(async () => {
		await store.close();
	});

	describe("append one", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log(
				store,
				{
					...signKey,
					sign: async (data: Uint8Array) => await signKey.sign(data),
				},
				{ logId: "A" }
			);
			await log.append("hello1");
		});

		it("added the correct amount of items", () => {
			expect(log.length).toEqual(1);
		});

		it("added the correct values", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.payload.getValue()).toEqual("hello1");
			});
		});

		it("added the correct amount of next pointers", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.next.length).toEqual(0);
			});
		});

		it("has the correct heads", async () => {
			for (const head of await log.getHeads()) {
				expect(head.hash).toEqual((await log.toArray())[0].hash);
			}
		});

		it("updated the clocks correctly", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.metadata.clock.id).toEqual(
					new Uint8Array(signKey.publicKey.bytes)
				);
				expect(entry.metadata.clock.timestamp.logical).toEqual(0);
			});
		});
	});

	describe("reset", () => {
		it("append", async () => {
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
			expect(await blockExists(e1.hash)).toBeTrue();
			expect(await blockExists(e2.hash)).toBeTrue();
			expect(log.nextsIndex.get(e1.hash)!.has(e2.hash)).toBeTrue();
			const { entry: e3 } = await log.append("hello3", { type: EntryType.CUT });
			// No forward pointers to next indices. We do this, so when we delete an entry, we can now whether an entry has a depenency of another entry which is not of type RESET
			expect(log.nextsIndex.get(e2.hash)).toBeUndefined();
			expect(await blockExists(e1.hash)).toBeFalse();
			expect(await blockExists(e2.hash)).toBeFalse();
			expect(await blockExists(e3.hash)).toBeTrue();
		});
	});
	describe("append 100 items to a log", () => {
		const amount = 100;
		const nextPointerAmount = 64;

		let log: Log<string>;

		beforeAll(async () => {
			// Do sign function really need to returnr publcikey
			log = new Log(
				store,
				{
					...signKey,
					sign: (data) => signKey.sign(data),
				},
				{ logId: "A" }
			);
			let prev: any = undefined;
			for (let i = 0; i < amount; i++) {
				prev = (
					await log.append("hello" + i, {
						nexts: prev ? [prev] : undefined,
					})
				).entry;
				// Make sure the log has the right heads after each append
				const values = await log.toArray();
				expect((await log.getHeads()).length).toEqual(1);
				expect((await log.getHeads())[0].hash).toEqual(
					values[values.length - 1].hash
				);
			}
		});

		it("added the correct amount of items", () => {
			expect(log.length).toEqual(amount);
		});

		it("added the correct values", async () => {
			(await log.toArray()).forEach((entry, index) => {
				expect(entry.payload.getValue()).toEqual("hello" + index);
			});
		});

		it("updated the clocks correctly", async () => {
			for (const [index, entry] of (await log.toArray()).entries()) {
				if (index > 0) {
					expect(
						entry.metadata.clock.timestamp.compare(
							(await log.toArray())[index - 1].metadata.clock.timestamp
						)
					).toBeGreaterThan(0);
				}
				expect(entry.metadata.clock.id).toEqual(
					new Uint8Array(signKey.publicKey.bytes)
				);
			}
		});

		/*    it('added the correct amount of refs pointers', async () => {
	   log.values.forEach((entry, index) => {
		 expect(entry.refs.length).toEqual(index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
	   })
	 }) */
	});
});
