import { AnyBlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import { Log } from "../src/index.js";
import { signKey } from "./fixtures/privateKey.js";

describe("get-pow-2-refs", function () {
	let store: AnyBlockStore;
	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});
	describe("Single log", () => {
		let log1: Log<Uint8Array>;

		beforeEach(async () => {
			log1 = new Log();
			await log1.open(store, signKey);

			for (let i = 0; i <= 100; i++) {
				await log1.append(new Uint8Array([i]));
			}
		});
		it("get refs one", async () => {
			const heads = await log1.getHeads(true).all();
			expect(heads).to.have.length(1);
			const refs = await log1.getReferenceSamples(heads[0], {
				pointerCount: 1,
			});
			expect(refs).to.have.length(1);
			for (const head of heads) {
				expect(refs.find((x) => x.hash === head.hash)).to.exist;
			}
		});

		it("get refs 4", async () => {
			const heads = await log1.getHeads(true).all();
			const refs = await log1.getReferenceSamples(heads[0], {
				pointerCount: 4,
			});
			expect(refs).to.have.length(2); // 2**2 = 4
			for (const head of heads) {
				expect(refs.find((x) => x.hash === head.hash));
			}
			let i = 0;
			for (const entry of refs) {
				expect(entry.payload.getValue()).to.deep.equal(
					new Uint8Array([100 + 1 - 2 ** i++]),
				);
			}
		});

		it("get refs 8", async () => {
			const heads = await log1.getHeads(true).all();
			const refs = await log1.getReferenceSamples(heads[0], {
				pointerCount: 8,
			});
			expect(refs).to.have.length(3); // 2**3 = 8
			for (const head of heads) {
				expect(refs.find((x) => x.hash === head.hash));
			}
			let i = 0;
			for (const entry of refs) {
				expect(entry.payload.getValue()).to.deep.equal(
					new Uint8Array([100 + 1 - 2 ** i++]),
				);
			}
		});

		it("get refs with memory limit", async () => {
			const heads = await log1.getHeads(true).all();
			expect(heads).to.have.length(1);
			const refs = await log1.getReferenceSamples(heads[0], {
				pointerCount: Number.MAX_SAFE_INTEGER,
				memoryLimit: 5,
			});
			const sum = refs
				.map((r) => r.payload.byteLength)
				.reduce((sum, current) => {
					sum = sum || 0;
					sum += current;
					return sum;
				});
			expect(sum).lessThan(6);
			expect(sum).greaterThan(4);
		});
	});

	describe("multiple heads", () => {
		let log1: Log<Uint8Array>;

		beforeEach(async () => {
			log1 = new Log();
			await log1.open(store, signKey);

			for (let i = 0; i <= 10; i++) {
				await log1.append(new Uint8Array([i]), { meta: { next: [] } });
			}
		});

		it("no refs if no nexts", async () => {
			const heads = await log1.getHeads(true).all();
			const refs = await log1.getReferenceSamples(heads[0], {
				pointerCount: 8,
			});
			expect(refs).to.have.length(1); // because heads[0] has no nexts (all commits are roots)
			expect(heads[0].hash).equal(refs[0].hash);
		});
	});
});
