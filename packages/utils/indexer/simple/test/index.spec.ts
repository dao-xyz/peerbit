import { field } from "@dao-xyz/borsh";
import {
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	id,
} from "@peerbit/indexer-interface";
import { tests } from "@peerbit/indexer-tests";
import { expect } from "chai";
import { create } from "../src/index.js";

class BatchDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	tag: string;

	constructor(id: string, tag: string) {
		this.id = id;
		this.tag = tag;
	}
}

describe("all", () => {
	tests(create, "transient", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: false,
	});

	it("applies puts in a batch", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });

		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
			new BatchDocument("c", "other"),
		]);

		const results = await index
			.iterate({
				query: new StringMatch({
					key: "tag",
					value: "peerbit",
					method: StringMatchMethod.exact,
				}),
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			})
			.all();
		expect(results.map((result) => result.value.id)).to.deep.equal(["a", "b"]);

		await indices.drop();
	});
});
