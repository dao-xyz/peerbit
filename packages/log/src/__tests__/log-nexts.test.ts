import rmrf from "rimraf";
import fs from "fs-extra";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Log } from "../log.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { createStore } from "./utils.js";

let signKey: KeyWithMeta<Ed25519Keypair>;

describe("Log - Nexts", function () {
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
		signKey = (await keystore.getKey(
			new Uint8Array([0])
		)) as KeyWithMeta<Ed25519Keypair>;
		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});
	describe("Custom next", () => {
		it("can fork explicitly", async () => {
			const log1 = new Log();
			await log1.open(store, {
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			});
			const { entry: e0 } = await log1.append("0", { nexts: [] });
			const { entry: e1 } = await log1.append("1", { nexts: [e0] });

			const { entry: e2a } = await log1.append("2a", {
				nexts: await log1.getHeads(),
			});
			expect((await log1.toArray())[0].next?.length).toEqual(0);
			expect((await log1.toArray())[1].next).toEqual([e0.hash]);
			expect((await log1.toArray())[2].next).toEqual([e1.hash]);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
			]);
			/*    expect([...log1._nextsIndexToHead[e0.hash]]).toEqual([e1.hash]); */

			// fork at root
			const { entry: e2ForkAtRoot } = await log1.append("2b", {
				nexts: [],
			});
			expect((await log1.toArray())[3].hash).toEqual(e2ForkAtRoot.hash); // Due to clock  // If we only use logical clok then it should be index 1 since clock is reset as this is a root "fork"
			expect((await log1.toArray())[2].hash).toEqual(e2a.hash);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
				e2ForkAtRoot.hash,
			]);

			// fork at 0
			const { entry: e2ForkAt0 } = await log1.append("2c", {
				nexts: [e0],
			});
			expect((await log1.toArray())[4].next).toEqual([e0.hash]);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
				e2ForkAtRoot.hash,
				e2ForkAt0.hash,
			]);

			// fork at 1
			const { entry: e2ForkAt1 } = await log1.append("2d", {
				nexts: [e1],
			});
			expect((await log1.toArray())[5].next).toEqual([e1.hash]);
			expect((await log1.getHeads()).map((h) => h.hash)).toContainAllValues([
				e2a.hash,
				e2ForkAtRoot.hash,
				e2ForkAt0.hash,
				e2ForkAt1.hash,
			]);
		});
	});
});
