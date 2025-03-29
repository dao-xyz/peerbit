import type { AnyStore } from "@peerbit/any-store";
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
	it("mabe put with condition", async () => {
		store = new AnyBlockStore();
		await store.start();
		const data = new Uint8Array([1, 2, 3]);
		const cid = await store.put(data);
		expect(cid).equal("zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU");
		let maybePutCid: string | undefined = undefined;

		const anyStore = store["_store"] as AnyStore;
		const put = anyStore.put.bind(anyStore);
		let putOnce = false;
		anyStore.put = async (key: string, value: Uint8Array) => {
			putOnce = true;
			return put(key, value);
		};
		await store.maybePut(data, async (cid) => {
			maybePutCid = cid;
			return false;
		});

		expect(maybePutCid).equal(cid);
		expect(putOnce).to.be.false;
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
