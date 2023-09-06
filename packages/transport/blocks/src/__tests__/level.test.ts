import { MemoryLevelBlockStore } from "../level";
import { createBlock, getBlockValue, stringifyCid } from "../block.js";
import { equals } from "uint8arrays";

describe(`level`, function () {
	let store: MemoryLevelBlockStore;

	afterEach(async () => {
		await store.stop();
	});

	it("rw", async () => {
		store = new MemoryLevelBlockStore();
		await store.start();
		const data = new Uint8Array([1, 2, 3]);
		const cid = await store.put(data);
		expect(cid).toEqual("zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU");

		const readData = await store.get(cid);
		expect(readData).toEqual(data);
	});

	it("iterate", async () => {
		store = new MemoryLevelBlockStore();
		await store.start();
		let datas = [new Uint8Array([0]), new Uint8Array([1])];
		const cids = await Promise.all(datas.map((x) => store.put(x)));
		await store.idle();
		let allKeys = new Set();
		for await (const [key, value] of store.iterator()) {
			let found = false;
			for (let i = 0; i < cids.length; i++) {
				if (key === cids[i] && equals(value, datas[i])) {
					found = true;
				}
			}

			expect(found).toBeTrue();

			allKeys.add(key);
		}

		expect(allKeys.size).toEqual(2);
	});
});
