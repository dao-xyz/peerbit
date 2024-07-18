import { expect } from "chai";
import { equals } from "uint8arrays";
import { AnyBlockStore } from "../src/any-blockstore.js";

describe(`level`, function () {
	let store: AnyBlockStore;

	afterEach(async () => {
		await store.stop();
	});

	it("rw", async () => {
		store = new AnyBlockStore();
		await store.start();
		const data = new Uint8Array([1, 2, 3]);
		const cid = await store.put(data);
		expect(cid).equal("zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU");

		const readData = await store.get(cid);
		expect(readData).to.deep.equal(data);
	});

	it("iterate", async () => {
		store = new AnyBlockStore();
		await store.start();
		let datas = [new Uint8Array([0]), new Uint8Array([1])];
		const cids = await Promise.all(datas.map((x) => store.put(x)));
		let allKeys = new Set();
		for await (const [key, value] of store.iterator()) {
			let found = false;
			for (let i = 0; i < cids.length; i++) {
				if (key === cids[i] && equals(new Uint8Array(value), datas[i])) {
					found = true;
				}
			}

			expect(found).to.be.true;

			allKeys.add(key);
		}

		expect(allKeys.size).equal(2);
	});
});
