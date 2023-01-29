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

let signKey: KeyWithMeta<Ed25519Keypair>,
	signKey2: KeyWithMeta<Ed25519Keypair>,
	signKey3: KeyWithMeta<Ed25519Keypair>;

describe("Log - CRDT", function () {
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
		signKey2 = (await await keystore.getKey(
			new Uint8Array([2])
		)) as KeyWithMeta<Ed25519Keypair>;
		signKey3 = (await await keystore.getKey(
			new Uint8Array([3])
		)) as KeyWithMeta<Ed25519Keypair>;

		store = new MemoryLevelBlockStore();
		await store.open();
	});

	afterAll(async () => {
		await store.close();

		rmrf.sync(testKeyStorePath(__filenameBase));

		await keystore?.close();
	});

	describe("is a CRDT", () => {
		let log1: Log<any>, log2: Log<any>, log3: Log<any>;

		beforeEach(async () => {
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);
		});

		it("join is associative", async () => {
			const expectedElementsCount = 6;

			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });

			// a + (b + c)
			await log2.join(log3);
			await log1.join(log2);

			const res1 = log1.values.slice();

			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });

			// (a + b) + c
			await log1.join(log2);
			await log3.join(log1);

			const res2 = log3.values.slice();

			// associativity: a + (b + c) == (a + b) + c
			expect(res1.length).toEqual(expectedElementsCount);
			expect(res2.length).toEqual(expectedElementsCount);
			expect(res1.map((x) => x.payload.getValue())).toEqual(
				res2.map((x) => x.payload.getValue())
			);
		});

		it("join is commutative", async () => {
			const expectedElementsCount = 4;

			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });

			// b + a
			await log2.join(log1);
			const res1 = log2.values.slice();

			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });

			// a + b
			await log1.join(log2);
			const res2 = log1.values.slice();

			// commutativity: a + b == b + a
			expect(res1.length).toEqual(expectedElementsCount);
			expect(res2.length).toEqual(expectedElementsCount);
			expect(res1.map((x) => x.payload.getValue())).toEqual(
				res2.map((x) => x.payload.getValue())
			);
		});

		it("multiple joins are commutative", async () => {
			// b + a == a + b
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log2.join(log1);
			const resA1 = log2.toString();

			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log1.join(log2);
			const resA2 = log1.toString();

			expect(resA1).toEqual(resA2);

			// a + b == b + a
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log1.join(log2);
			const resB1 = log1.toString();

			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log2.join(log1);
			const resB2 = log2.toString();

			expect(resB1).toEqual(resB2);

			// a + c == c + a
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "A" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "A" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });
			await log3.join(log1);
			const resC1 = log3.toString();

			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });
			await log1.join(log3);
			const resC2 = log1.toString();

			expect(resC1).toEqual(resC2);

			// c + b == b + c
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);

			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });
			await log3.join(log2);
			const resD1 = log3.toString();

			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });
			await log2.join(log3);
			const resD2 = log2.toString();

			expect(resD1).toEqual(resD2);

			// a + b + c == c + b + a
			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });
			await log1.join(log2);
			await log1.join(log3);
			const logLeft = log1.toString();

			log1 = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log2 = new Log(
				store,
				{
					...signKey2.keypair,
					sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
				},
				{ logId: "X" }
			);
			log3 = new Log(
				store,
				{
					...signKey3.keypair,
					sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await log1.append("helloA1", { gidSeed: Buffer.from("a") });
			await log1.append("helloA2", { gidSeed: Buffer.from("a") });
			await log2.append("helloB1", { gidSeed: Buffer.from("a") });
			await log2.append("helloB2", { gidSeed: Buffer.from("a") });
			await log3.append("helloC1", { gidSeed: Buffer.from("a") });
			await log3.append("helloC2", { gidSeed: Buffer.from("a") });
			await log3.join(log2);
			await log3.join(log1);
			const logRight = log3.toString();

			expect(logLeft).toEqual(logRight);
		});

		it("join is idempotent", async () => {
			const expectedElementsCount = 3;

			const logA = new Log(
				store,
				{
					...signKey.keypair,
					sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
				},
				{ logId: "X" }
			);
			await logA.append("helloA1");
			await logA.append("helloA2");
			await logA.append("helloA3");

			// idempotence: a + a = a
			await logA.join(logA);
			expect(logA.length).toEqual(expectedElementsCount);
		});
	});
});
