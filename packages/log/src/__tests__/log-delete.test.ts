import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { createStore } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let signKey: KeyWithMeta<Ed25519Keypair>;

describe("Log - Delete", function () {
	let keystore: Keystore;
	let store: BlockStore;

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

	it("deletes recursively", async () => {
		const blockExists = async (hash: string): Promise<boolean> => {
			try {
				await (store as MemoryLevelBlockStore).idle();
				return !!(await store.get(hash, { timeout: 3000 }));
			} catch (error) {
				return false;
			}
		};
		const log = new Log<string>(
			store,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{ logId: "A" }
		);
		const { entry: e1 } = await log.append("hello1");
		const { entry: e2 } = await log.append("hello2");
		const { entry: e3 } = await log.append("hello3");

		await log.deleteRecursively(e2);
		expect(log._nextsIndex.size).toEqual(0);
		expect(log.values.length).toEqual(1);
		expect(log.get(e1.hash)).toBeUndefined();
		expect(await blockExists(e1.hash)).toBeFalse();
		expect(log.get(e2.hash)).toBeUndefined();
		expect(await blockExists(e2.hash)).toBeFalse();
		expect(log.get(e3.hash)).toBeDefined();
		expect(await blockExists(e3.hash)).toBeTrue();

		await log.deleteRecursively(e3);
		expect(log.values.length).toEqual(0);
		expect(log.heads).toHaveLength(0);
		expect(log._nextsIndex.size).toEqual(0);
		expect(log._entryIndex.length).toEqual(0);
	});
});
