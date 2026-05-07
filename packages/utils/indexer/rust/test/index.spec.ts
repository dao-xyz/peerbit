import { field, vec } from "@dao-xyz/borsh";
import {
	And,
	Compare,
	IntegerCompare,
	Nested,
	Or,
	Sort,
	SortDirection,
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

class BridgeMetricDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	tag: string;

	@field({ type: "u32" })
	value: number;

	constructor(id: string, tag: string, value: number) {
		this.id = id;
		this.tag = tag;
		this.value = value;
	}
}

class BridgeNestedItem {
	@field({ type: "string" })
	tag: string;

	@field({ type: "u32" })
	score: number;

	constructor(tag: string, score: number) {
		this.tag = tag;
		this.score = score;
	}
}

class BridgeNestedDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: vec(BridgeNestedItem) })
	items: BridgeNestedItem[];

	constructor(id: string, items: BridgeNestedItem[]) {
		this.id = id;
		this.items = items;
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
	it("does not expose the previous typescript query fallback evaluator", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });

		expect((index as unknown as Record<string, unknown>).handleFieldQuery).to.equal(
			undefined,
		);
		expect((index as unknown as Record<string, unknown>).handleQueryObject).to.equal(
			undefined,
		);

		await indices.drop();
	});

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

	it("evaluates explicit nested queries in native rust", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeNestedDocument });
		await index.put(
			new BridgeNestedDocument("a", [
				new BridgeNestedItem("left", 1),
				new BridgeNestedItem("right", 3),
			]),
		);
		await index.put(
			new BridgeNestedDocument("b", [new BridgeNestedItem("left", 4)]),
		);
		await index.put(
			new BridgeNestedDocument("c", [new BridgeNestedItem("right", 5)]),
		);

		const query = new Nested({
			path: "items",
			query: [
				new StringMatch({ key: "tag", value: "left" }),
				new IntegerCompare({
					key: "score",
					compare: Compare.Greater,
					value: 2,
				}),
			],
		});
		const results = await index.iterate({ query }).all();

		expect(results.map((result) => result.value.id)).to.deep.equal(["b"]);
		expect(await index.count({ query })).to.equal(1);
		await indices.drop();
	});

	it("sums and deletes through native rust queries", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeMetricDocument });
		await index.put(new BridgeMetricDocument("a", "peerbit", 1));
		await index.put(new BridgeMetricDocument("b", "other", 2));
		await index.put(new BridgeMetricDocument("c", "peerbit", 3));

		const query = new StringMatch({ key: "tag", value: "peerbit" });
		expect(await index.sum({ key: "value" })).to.equal(6);
		expect(await index.sum({ key: "value", query })).to.equal(4);

		const deleted = await index.del({ query });
		expect(deleted.map((id) => id.primitive)).to.deep.equal(["a", "c"]);
		expect(await index.count()).to.equal(1);
		expect(await index.sum({ key: "value" })).to.equal(2);

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

	it("pages sorted native candidates in rust", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		await index.put(new BridgeDocument("a", "peerbit", "delta"));
		await index.put(new BridgeDocument("b", "peerbit", "alpha"));
		await index.put(new BridgeDocument("c", "other", "zero"));
		await index.put(new BridgeDocument("d", "peerbit", "charlie"));
		await index.put(new BridgeDocument("e", "peerbit", "bravo"));

		const iterator = index.iterate({
			query: new StringMatch({ key: "tag", value: "peerbit" }),
			sort: new Sort({ key: "title" }),
		});

		expect(await iterator.pending()).to.equal(4);
		expect((await iterator.next(2)).map((result) => result.value.id)).to.deep.equal([
			"b",
			"e",
		]);
		expect(iterator.done()).to.equal(false);
		expect((await iterator.next(2)).map((result) => result.value.id)).to.deep.equal([
			"d",
			"a",
		]);
		expect(iterator.done()).to.equal(true);

		const allIterator = index.iterate({
			sort: new Sort({ key: "title" }),
		});
		expect((await allIterator.next(5)).map((result) => result.value.id)).to.deep.equal([
			"b",
			"e",
			"d",
			"a",
			"c",
		]);
		expect(allIterator.done()).to.equal(true);

		const descIterator = index.iterate({
			sort: new Sort({ key: "title", direction: SortDirection.DESC }),
		});
		expect((await descIterator.next(3)).map((result) => result.value.id)).to.deep.equal([
			"c",
			"a",
			"d",
		]);

		await indices.drop();
	});
});
