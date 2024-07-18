import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import { FirstWriteWins, LastWriteWins, compare } from "../src/log-sorting.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";

describe("sorting", function () {
	let store: BlockStore;
	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	let log: Log<Uint8Array>;

	beforeEach(async () => {
		log = new Log<Uint8Array>();
	});

	describe("last write wins", () => {
		it("sorts by last write wins", async () => {
			await log.open(new AnyBlockStore(), signKey, {
				sortFn: LastWriteWins,
			});

			const { entry: e0 } = await log.append(new Uint8Array([0]));
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append(new Uint8Array([2]));
			const { entry: e3 } = await log.append(new Uint8Array([3]));

			const entries = await log.toArray();
			expect(entries.map((e) => e.payload.value)).to.deep.equal([
				new Uint8Array([0]),
				new Uint8Array([1]),
				new Uint8Array([2]),
				new Uint8Array([3]),
			]);

			const first = await log.entryIndex.getBefore(e0);
			expect(first?.hash).to.be.undefined;
			expect((await log.entryIndex.getBefore(e1))?.hash).to.deep.equal(e0.hash);
			expect((await log.entryIndex.getBefore(e2))?.hash).to.deep.equal(e1.hash);

			const before = (await log.entryIndex.getBefore(e3))?.hash;
			expect(before).to.deep.equal(e2.hash);

			expect((await log.entryIndex.getAfter(e0))?.hash).to.deep.equal(e1.hash);
			expect((await log.entryIndex.getAfter(e1))?.hash).to.deep.equal(e2.hash);
			expect((await log.entryIndex.getAfter(e2))?.hash).to.deep.equal(e3.hash);
			const last = await log.entryIndex.getAfter(e3);
			expect(last?.hash).to.be.undefined;

			expect((await log.entryIndex.getOldest())?.hash).to.deep.equal(e0.hash);
			expect((await log.entryIndex.getNewest())?.hash).to.deep.equal(e3.hash);
		});
	});

	describe("compare", () => {
		it("last write wins", async () => {
			await log.open(new AnyBlockStore(), signKey, {
				sortFn: LastWriteWins,
			});
			const { entry: e0 } = await log.append(new Uint8Array([0]));
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			expect(compare(e0, e1, LastWriteWins)).to.be.lessThan(0);
		});

		it("first write wins", async () => {
			await log.open(new AnyBlockStore(), signKey, {
				sortFn: FirstWriteWins,
			});
			const { entry: e0 } = await log.append(new Uint8Array([0]));
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			expect(compare(e0, e1, FirstWriteWins)).to.be.greaterThan(0);
		});
	});
});
