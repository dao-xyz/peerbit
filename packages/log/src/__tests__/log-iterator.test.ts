import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { LogCreator } from "./utils/log-creator.js";
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
import { Entry } from "../entry.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>,
	signKey2: KeyWithMeta<Ed25519Keypair>,
	signKey3: KeyWithMeta<Ed25519Keypair>;

describe("Log - Iterator", function () {
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
		signKey = await keystore.getKey(new Uint8Array([3]));
		//@ts-ignore
		signKey2 = await keystore.getKey(new Uint8Array([2]));
		//@ts-ignore
		signKey3 = await keystore.getKey(new Uint8Array([1]));
		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});

	describe("Basic iterator functionality", () => {
		let log1: Log<string>;

		let entries: Entry<any>[];
		beforeEach(async () => {
			entries = [];
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);

			for (let i = 0; i <= 100; i++) {
				entries.push((await log1.append("entry" + i)).entry);
			}
		});

		it("returns a Symbol.iterator object", async () => {
			const it = log1.iterator({
				amount: 0,
			});

			expect(typeof it[Symbol.iterator]).toEqual("function");
			assert.deepStrictEqual(it.next(), {
				value: undefined,
				done: true,
			});
		});

		it("returns length from tail and amount", async () => {
			const amount = 10;
			const it = log1.iterator({
				amount: amount,
			});
			const length = [...it].length;
			expect(length).toEqual(10);
			let i = 0;
			for (const entry of it) {
				expect(entry).toEqual(entries[i++].hash);
			}
		});

		it("returns length from head and amount", async () => {
			const amount = 10;
			const it = log1.iterator({
				amount: amount,
				from: "head",
			});
			const length = [...it].length;
			expect(length).toEqual(10);
			let i = 0;
			for (const entry of it) {
				expect(entry).toEqual(entries[100 - i++].hash);
			}
		});

		it("returns all", async () => {
			const it = log1.iterator();
			const length = [...it].length;
			expect(length).toEqual(101);
			let i = 0;
			for (const entry of it) {
				expect(entry).toEqual(entries[i++].hash);
			}
		});
	});
});
