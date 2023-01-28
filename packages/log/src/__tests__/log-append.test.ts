import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
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

let signKey: KeyWithMeta<Ed25519Keypair>;
describe("Log - Append", function () {
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

	describe("append one", () => {
		let log: Log<string>;

		beforeEach(async () => {
			log = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A" }
			);
			await log.append("hello1");
		});

		it("added the correct amount of items", () => {
			expect(log.length).toEqual(1);
		});

		it("added the correct values", async () => {
			log.values.forEach((entry) => {
				expect(entry.payload.getValue()).toEqual("hello1");
			});
		});

		it("added the correct amount of next pointers", async () => {
			log.values.forEach((entry) => {
				expect(entry.next.length).toEqual(0);
			});
		});

		it("has the correct heads", async () => {
			log.heads.forEach((head) => {
				expect(head.hash).toEqual(log.values[0].hash);
			});
		});

		it("updated the clocks correctly", async () => {
			log.values.forEach((entry) => {
				expect(entry.metadata.clock.id).toEqual(
					new Uint8Array(signKey.keypair.publicKey.bytes)
				);
				expect(entry.metadata.clock.timestamp.logical).toEqual(0);
			});
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
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
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
				const values = log.values;
				expect(log.heads.length).toEqual(1);
				expect(log.heads[0].hash).toEqual(values[values.length - 1].hash);
			}
		});

		it("added the correct amount of items", () => {
			expect(log.length).toEqual(amount);
		});

		it("added the correct values", async () => {
			log.values.forEach((entry, index) => {
				expect(entry.payload.getValue()).toEqual("hello" + index);
			});
		});

		it("updated the clocks correctly", async () => {
			log.values.forEach((entry, index) => {
				if (index > 0) {
					expect(
						entry.metadata.clock.timestamp.compare(
							log.values[index - 1].metadata.clock.timestamp
						)
					).toBeGreaterThan(0);
				}
				expect(entry.metadata.clock.id).toEqual(
					new Uint8Array(signKey.keypair.publicKey.bytes)
				);
			});
		});

		/*    it('added the correct amount of refs pointers', async () => {
	   log.values.forEach((entry, index) => {
		 expect(entry.refs.length).toEqual(index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
	   })
	 }) */
	});
});
