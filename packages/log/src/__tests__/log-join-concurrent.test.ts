import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { SortByEntryHash } from "../log-sorting.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

describe("Log - Join Concurrent Entries", function () {
	let store: BlockStore,
		keystore: Keystore,
		signKey: KeyWithMeta<Ed25519Keypair>;

	beforeAll(async () => {
		rmrf.sync(testKeyStorePath(__filenameBase));

		await fs.copy(
			signingKeysFixturesPath(__dirname),
			testKeyStorePath(__filenameBase)
		);
		keystore = new Keystore(
			await createStore(testKeyStorePath(__filenameBase))
		);
		await keystore.waitForOpen();
		// @ts-ignore
		signKey = await keystore.getKey(new Uint8Array([0]));

		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));
	});

	describe("join ", () => {
		let log1: Log<string>, log2: Log<string>;

		beforeAll(async () => {
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A", sortFn: SortByEntryHash }
			);
			log2 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A", sortFn: SortByEntryHash }
			);
		});

		it("joins consistently", async () => {
			// joins consistently
			for (let i = 0; i < 10; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			const hash1 = await log1.toMultihash();
			const hash2 = await log2.toMultihash();

			expect(hash1).toEqual(hash2);
			expect(log1.length).toEqual(20);
			assert.deepStrictEqual(
				log1.values.map((e) => e.payload.getValue()),
				log2.values.map((e) => e.payload.getValue())
			);

			// Joining after concurrently appending same payload joins entry once
			for (let i = 10; i < 20; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			expect(log1.length).toEqual(log2.length);
			expect(log1.length).toEqual(40);
			expect(log1.values.map((e) => e.payload.getValue())).toEqual(
				log2.values.map((e) => e.payload.getValue())
			);
		});

		/*  Below test is not true any more since we are using HLC
		it("Concurrently appending same payload after join results in same state", async () => {
			for (let i = 10; i < 20; i++) {
				await log1.append("hello1-" + i);
				await log2.append("hello2-" + i);
			}

			await log1.join(log2);
			await log2.join(log1);

			await log1.append("same");
			await log2.append("same");

			const hash1 = await log1.toMultihash();
			const hash2 = await log2.toMultihash();

			expect(hash1).toEqual(hash2);
			expect(log1.length).toEqual(41);
			expect(log2.length).toEqual(41);
			assert.deepStrictEqual(
				log1.values.map((e) => e.payload.getValue()),
				log2.values.map((e) => e.payload.getValue())
			);
		}); */

		/*  it("Joining after concurrently appending same payload joins entry once", async () => {

		 }); */
	});
});
