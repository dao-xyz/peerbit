import { expect } from "chai";
import { createStore as createRustStore } from "@peerbit/any-store-rust";
import { equals } from "uint8arrays";
import sinon from "sinon";
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

	it("puts many blocks", async () => {
		store = new AnyBlockStore();
		await store.start();
		const datas = [new Uint8Array([0]), new Uint8Array([1, 2])];
		const cids = await store.putMany(datas);

		expect(cids).to.have.length(2);
		expect(await store.get(cids[0])).to.deep.equal(datas[0]);
		expect(await store.get(cids[1])).to.deep.equal(datas[1]);
	});

	it("puts a single known cid block through store put", async () => {
		const backingStore = createRustStore();
		const putSpy = sinon.spy(backingStore, "put");
		const putManySpy = sinon.spy(backingStore, "putMany");
		store = new AnyBlockStore(backingStore);
		await store.start();
		const data = new Uint8Array([1, 2, 3]);
		const cid = "zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU";
		const cids = await store.putKnownMany([[cid, data]]);

		expect(cids).to.deep.equal([cid]);
		expect(await store.get(cid)).to.deep.equal(data);
		expect(putSpy.callCount).equal(1);
		expect(putManySpy.callCount).equal(0);
		putSpy.restore();
		putManySpy.restore();
	});

	it("puts a known cid block through store put", async () => {
		const backingStore = createRustStore();
		const putSpy = sinon.spy(backingStore, "put");
		const putManySpy = sinon.spy(backingStore, "putMany");
		store = new AnyBlockStore(backingStore);
		await store.start();
		const data = new Uint8Array([1, 2, 3]);
		const cid = "zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU";
		const storedCid = await store.putKnown(cid, data);

		expect(storedCid).equal(cid);
		expect(await store.get(cid)).to.deep.equal(data);
		expect(putSpy.callCount).equal(1);
		expect(putManySpy.callCount).equal(0);
		putSpy.restore();
		putManySpy.restore();
	});

	it("puts multiple known cid blocks through store batch helpers", async () => {
		const backingStore = createRustStore();
		const putSpy = sinon.spy(backingStore, "put");
		const putManySpy = sinon.spy(backingStore, "putMany");
		store = new AnyBlockStore(backingStore);
		await store.start();
		const blocks: Array<readonly [string, Uint8Array]> = [
			[
				"zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU",
				new Uint8Array([1, 2, 3]),
			],
			[
				"zb2rhekcCjeckfdqkhqVvqRBRCaZDM48NZwFSUbm8yzo5GQad",
				new Uint8Array([4, 5, 6]),
			],
		];
		const cids = await store.putKnownMany(blocks);

		expect(cids).to.deep.equal(blocks.map(([cid]) => cid));
		expect(await store.get(blocks[0][0])).to.deep.equal(blocks[0][1]);
		expect(await store.get(blocks[1][0])).to.deep.equal(blocks[1][1]);
		expect(putSpy.callCount).equal(0);
		expect(putManySpy.callCount).equal(1);
		putSpy.restore();
		putManySpy.restore();
	});

	it("gets and removes many blocks through store batch helpers", async () => {
		store = new AnyBlockStore(createRustStore());
		await store.start();
		const datas = [new Uint8Array([0]), new Uint8Array([1, 2])];
		const cids = await store.putMany(datas);

		expect(await store.getMany([...cids, "missing"])).to.deep.equal([
			datas[0],
			datas[1],
			undefined,
		]);
		expect(await store.hasMany(["missing", ...cids])).to.deep.equal([
			false,
			true,
			true,
		]);
		expect(await store.rmMany([cids[0], "missing"])).to.equal(1);
		expect(await store.getMany(cids)).to.deep.equal([undefined, datas[1]]);
		expect(await store.hasMany(cids)).to.deep.equal([false, true]);
	});
});
