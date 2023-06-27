import { MemoryLevelBlockStore } from "../level";
import { createBlock, getBlockValue, stringifyCid } from "../block.js";

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
});
