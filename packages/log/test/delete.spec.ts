import { Log } from "../src/log.js";
import { type BlockStore, AnyBlockStore } from "@peerbit/blocks";
import { EntryType } from "../src/entry.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";
import { expect } from "chai";

describe("delete", function () {
	let store: BlockStore;

	beforeEach(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	afterEach(async () => {
		await store.stop();
	});

	const blockExists = async (hash: string): Promise<boolean> => {
		try {
			return !!(await store.get(hash, { timeout: 3000 }));
		} catch (error) {
			return false;
		}
	};

	describe("deleteRecursively", () => {
		it("deleted unreferences", async () => {
			const log = new Log();
			await log.open(store, signKey);
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append(new Uint8Array([2]));
			const { entry: e3 } = await log.append(new Uint8Array([3]));

			await log.deleteRecursively(e2);
			expect(log.nextsIndex.size).equal(0);
			expect((await log.toArray()).length).equal(1);
			expect(await log.get(e1.hash)).equal(undefined);
			expect(await blockExists(e1.hash)).to.be.false;
			expect(await log.get(e2.hash)).equal(undefined);
			expect(await blockExists(e2.hash)).to.be.false;
			expect(await log.get(e3.hash)).to.exist;
			expect(await blockExists(e3.hash)).to.be.true;

			await log.deleteRecursively(e3);
			expect((await log.toArray()).length).equal(0);
			expect(await log.getHeads()).to.be.empty;
			expect(log.nextsIndex.size).equal(0);
			expect(log.entryIndex._cache.size).equal(0);
		});

		it("processes as long as alowed", async () => {
			const log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append("hello2a");
			const { entry: e2b } = await log.append("hello2b", {
				meta: { next: [e2] }
			});
			const { entry: e3 } = await log.append(new Uint8Array([3]), {
				meta: {
					next: [e2],
					type: EntryType.CUT
				}
			});
			expect(await log.toArray()).to.have.length(4);
			expect(log.nextsIndex.size).equal(2); // e1 ->  e2, e2 -> e2b
			await log.deleteRecursively(e2b);
			expect(log.nextsIndex.size).equal(0);
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([e3.hash]);
			expect(await log.get(e1.hash)).equal(undefined);
			expect(await blockExists(e1.hash)).to.be.false;
			expect(await log.get(e2.hash)).equal(undefined);
			expect(await blockExists(e2.hash)).to.be.false;
			expect(await log.get(e3.hash)).to.exist;
			expect(await blockExists(e3.hash)).to.be.true;

			await log.deleteRecursively(e3);
			expect((await log.toArray()).length).equal(0);
			expect(await log.getHeads()).to.be.empty;
			expect(log.nextsIndex.size).equal(0);
			expect(log.entryIndex._cache.size).equal(0);
		});

		it("keeps references", async () => {
			const log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2a } = await log.append("hello2a", {
				meta: { next: [e1] }
			});
			const { entry: e2b } = await log.append("hello2b", {
				meta: { next: [e1] }
			});

			await log.deleteRecursively(e2a);
			expect(log.nextsIndex.size).equal(1);
			expect((await log.toArray()).length).equal(2);
			expect(await log.get(e1.hash)).to.exist;
			expect(await blockExists(e1.hash)).to.be.true;
			expect(await log.get(e2a.hash)).equal(undefined);
			expect(await blockExists(e2a.hash)).to.be.false;
			expect(await log.get(e2b.hash)).to.exist;
			expect(await blockExists(e2b.hash)).to.be.true;
			await log.deleteRecursively(e2b);
			expect((await log.toArray()).length).equal(0);
			expect(await log.getHeads()).to.be.empty;
			expect(log.nextsIndex.size).equal(0);
			expect(log.entryIndex._cache.size).equal(0);
		});
	});
});
