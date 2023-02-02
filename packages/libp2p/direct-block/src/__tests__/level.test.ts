import { MemoryLevelBlockStore } from "../level";
import { createBlock, getBlockValue, stringifyCid } from "../block.js";

describe(`level`, function () {
	let store: MemoryLevelBlockStore;

	afterEach(async () => {
		await store.close();
	});

	it("rw", async () => {
		store = new MemoryLevelBlockStore();
		await store.open();
		const data = new Uint8Array([1, 2, 3]);
		const cid = await store.put(await createBlock(data, "raw"));
		expect(stringifyCid(cid)).toEqual(
			"zb2rhWtC5SY6zV1y2SVN119ofpxsbEtpwiqSoK77bWVzHqeWU"
		);

		const readData = await store.get<Uint8Array>(stringifyCid(cid));
		expect(await getBlockValue(readData!)).toEqual(data);
	});
});
