import { field } from "@dao-xyz/borsh";
import {
	And,
	Or,
	Sort,
	StringMatch,
	StringMatchMethod,
	id,
} from "@peerbit/indexer-interface";
import { tests } from "@peerbit/indexer-tests";
import { expect } from "chai";
import { create } from "../src/index.js";

class BridgeDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	tag: string;

	@field({ type: "string" })
	title: string;

	constructor(id: string, tag: string, title: string) {
		this.id = id;
		this.tag = tag;
		this.title = title;
	}
}

describe("all", () => {
	tests(create, "persist", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: false,
	});
	tests(create, "transient", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: false,
	});
});

describe("native planner bridge", () => {
	it("uses supported native candidates with residual predicates", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		await index.put(new BridgeDocument("a", "peerbit", "native index"));
		await index.put(new BridgeDocument("b", "other", "native bridge"));
		await index.put(new BridgeDocument("c", "peerbit", "typescript fallback"));

		const results = await index
			.iterate({
				query: new And([
					new StringMatch({ key: "tag", value: "peerbit" }),
					new StringMatch({
						key: "title",
						value: "native",
						method: StringMatchMethod.contains,
					}),
				]),
			})
			.all();

		expect(results.map((result) => result.value.id)).to.deep.equal(["a"]);
		await indices.drop();
	});

	it("falls back safely when an or branch is not native", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		await index.put(new BridgeDocument("a", "peerbit", "native index"));
		await index.put(new BridgeDocument("b", "other", "native bridge"));
		await index.put(new BridgeDocument("c", "peerbit", "typescript fallback"));

		const results = await index
			.iterate({
				query: new Or([
					new StringMatch({ key: "tag", value: "peerbit" }),
					new StringMatch({
						key: "title",
						value: "native",
						method: StringMatchMethod.contains,
					}),
				]),
				sort: new Sort({ key: "id" }),
			})
			.all();

		expect(results.map((result) => result.value.id)).to.deep.equal([
			"a",
			"b",
			"c",
		]);
		await indices.drop();
	});
});
