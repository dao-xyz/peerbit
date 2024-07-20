import { AnyBlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import { Log } from "../src/log.js";
import { signKey, signKey2, signKey3 } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("crdt", function () {
	let store: AnyBlockStore;
	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("is a CRDT", () => {
		let log1: Log<any>, log2: Log<any>, log3: Log<any>;

		beforeEach(async () => {
			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
		});

		it("join is associative", async () => {
			const expectedElementsCount = 6;

			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });

			// a + (b + c)
			await log2.join(log3);
			await log1.join(log2);

			const res1 = (await log1.toArray()).slice();

			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });

			// (a + b) + c
			await log1.join(log2);
			await log3.join(log1);

			const res2 = (await log3.toArray()).slice();

			// associativity: a + (b + c) == (a + b) + c
			expect(res1.length).equal(expectedElementsCount);
			expect(res2.length).equal(expectedElementsCount);
			expect(res1.map((x) => x.payload.getValue())).to.deep.equal(
				res2.map((x) => x.payload.getValue()),
			);
		});

		it("join is commutative", async () => {
			const expectedElementsCount = 4;

			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });

			// b + a
			await log2.join(log1);
			const res1 = (await log2.toArray()).slice();

			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });

			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });

			// a + b
			await log1.join(log2);
			const res2 = (await log1.toArray()).slice();

			// commutativity: a + b == b + a
			expect(res1.length).equal(expectedElementsCount);
			expect(res2.length).equal(expectedElementsCount);
			expect(res1.map((x) => x.payload.getValue())).to.deep.equal(
				res2.map((x) => x.payload.getValue()),
			);
		});

		it("multiple joins are commutative", async () => {
			// b + a == a + b

			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.join(log1);
			const resA1 = await log2.toString();

			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log1.join(log2);
			const resA2 = await log1.toString();

			expect(resA1).equal(resA2);

			// a + b == b + a
			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });

			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log1.join(log2);
			const resB1 = await log1.toString();

			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });

			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.join(log1);
			const resB2 = await log2.toString();

			expect(resB1).equal(resB2);

			// a + c == c + a
			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });

			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.join(log1);
			const resC1 = await log3.toString();

			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });

			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });
			await log1.join(log3);
			const resC2 = await log1.toString();

			expect(resC1).equal(resC2);

			// c + b == b + c

			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });

			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.join(log2);
			const resD1 = await log3.toString();

			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.join(log3);
			const resD2 = await log2.toString();

			expect(resD1).equal(resD2);

			// a + b + c == c + b + a
			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });
			await log1.join(log2);
			await log1.join(log3);
			const logLeft = await log1.toString();
			log1 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			log2 = new Log();
			await log2.open(store, signKey2, { encoding: JSON_ENCODING });
			log3 = new Log();
			await log3.open(store, signKey3, { encoding: JSON_ENCODING });
			await log1.append("helloA1", { meta: { gidSeed: Buffer.from("a") } });
			await log1.append("helloA2", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB1", { meta: { gidSeed: Buffer.from("a") } });
			await log2.append("helloB2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC1", { meta: { gidSeed: Buffer.from("a") } });
			await log3.append("helloC2", { meta: { gidSeed: Buffer.from("a") } });
			await log3.join(log2);
			await log3.join(log1);
			const logRight = await log3.toString();

			expect(logLeft).equal(logRight);
		});

		it("join is idempotent", async () => {
			const expectedElementsCount = 3;

			await log1.append("helloA1");
			await log1.append("helloA2");
			await log1.append("helloA3");

			// idempotence: a + a = a
			await log1.join(log1);
			expect(log1.length).equal(expectedElementsCount);
		});
	});
});
