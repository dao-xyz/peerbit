import { field, vec } from "@dao-xyz/borsh";
import {
	And,
	Compare,
	IntegerCompare,
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

class BridgeArrayDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: vec("u32") })
	numbers: number[];

	constructor(id: string, numbers: number[]) {
		this.id = id;
		this.numbers = numbers;
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
	it("evaluates exact and contains predicates in native rust", async () => {
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

	it("keeps array and predicates scoped to the same native element", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeArrayDocument });
		await index.put(new BridgeArrayDocument("a", [1]));
		await index.put(new BridgeArrayDocument("b", [2]));
		await index.put(new BridgeArrayDocument("c", [0, 3]));

		const results = await index
			.iterate({
				query: new And([
					new IntegerCompare({
						key: "numbers",
						compare: Compare.Less,
						value: 2,
					}),
					new IntegerCompare({
						key: "numbers",
						compare: Compare.GreaterOrEqual,
						value: 1,
					}),
				]),
			})
			.all();

		expect(results.map((result) => result.value.id)).to.deep.equal(["a"]);
		await indices.drop();
	});

	it("evaluates string or predicates in native rust", async () => {
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

	it("pages exact native candidates without materializing the full result", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		await index.put(new BridgeDocument("a", "peerbit", "native index"));
		await index.put(new BridgeDocument("b", "peerbit", "native bridge"));
		await index.put(new BridgeDocument("c", "other", "typescript fallback"));
		await index.put(new BridgeDocument("d", "peerbit", "native count"));
		await index.put(new BridgeDocument("e", "peerbit", "native page"));

		const query = new StringMatch({ key: "tag", value: "peerbit" });
		expect(await index.count({ query })).to.equal(4);

		const iterator = index.iterate({ query });
		expect(await iterator.pending()).to.equal(4);
		expect((await iterator.next(2)).map((result) => result.value.id)).to.deep.equal([
			"a",
			"b",
		]);
		expect(iterator.done()).to.equal(false);
		expect(await iterator.pending()).to.equal(2);
		expect((await iterator.next(2)).map((result) => result.value.id)).to.deep.equal([
			"d",
			"e",
		]);
		expect(iterator.done()).to.equal(true);
		expect(await iterator.pending()).to.equal(0);

		await indices.drop();
	});
});
