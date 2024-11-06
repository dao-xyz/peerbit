import { id } from "@peerbit/indexer-interface";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

describe("sort", () => {
	// u64 is a special case since we need to shift values to fit into signed 64 bit integers

	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	class Document {
		@id({ type: "string" })
		id: string;

		constructor(id: string) {
			this.id = id;
		}
	}

	it("sorts by default by id ", async () => {
		// this test is to insure that the iterator is stable. I.e. default sorting is applied
		index = await setup({ schema: Document }, create);
		const store = index.store as SQLLiteIndex<Document>;
		expect(store.tables.size).to.equal(1);
		await index.store.put(new Document("3"));
		await index.store.put(new Document("2"));
		await index.store.put(new Document("1"));

		const iterator = await index.store.iterate();
		const [first, second, third] = [
			...(await iterator.next(1)),
			...(await iterator.next(1)),
			...(await iterator.next(1)),
		];
		expect(first.value.id).to.equal("1");
		expect(second.value.id).to.equal("2");
		expect(third.value.id).to.equal("3");
	});
});
