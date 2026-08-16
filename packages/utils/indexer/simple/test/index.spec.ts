import { field } from "@dao-xyz/borsh";
import {
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	id,
} from "@peerbit/indexer-interface";
import { tests, tieParityTests } from "@peerbit/indexer-tests";
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

	tieParityTests(create);

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

	it("lazily evaluates unsorted pages", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		let tagReads = 0;

		for (let i = 0; i < 64; i++) {
			const value = new Proxy(new BatchDocument(String(i), "peerbit"), {
				get: (target, property, receiver) => {
					if (property === "tag") {
						tagReads++;
					}
					return Reflect.get(target, property, receiver);
				},
			});
			await index.put(value);
		}

		const iterator = index.iterate(
			{
				query: new StringMatch({
					key: "tag",
					value: "peerbit",
					method: StringMatchMethod.exact,
				}),
			},
			{ reference: true },
		);
		const first = await iterator.next(2);

		expect(first.map((result) => result.value.id)).to.deep.equal(["0", "1"]);
		// Two returned rows plus one lookahead are evaluated. The remaining 61
		// rows stay behind the Map cursor instead of being materialized eagerly.
		expect(tagReads).to.equal(3);
		expect(iterator.done()).to.equal(false);

		await iterator.close();
		expect(tagReads).to.equal(3);
		await indices.drop();
	});

	it("drains unsorted pending rows without losing the first result", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
			new BatchDocument("c", "peerbit"),
		]);

		const iterator = index.iterate();
		expect(await iterator.pending()).to.equal(3);
		expect(
			(await iterator.next(2)).map((result) => result.value.id),
		).to.deep.equal(["a", "b"]);
		expect(await iterator.pending()).to.equal(1);
		expect(
			(await iterator.all()).map((result) => result.value.id),
		).to.deep.equal(["c"]);
		expect(iterator.done()).to.equal(true);

		await indices.drop();
	});

	it("does not chase rows appended after unsorted iteration starts", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
		]);

		const iterator = index.iterate();
		expect(
			(await iterator.next(1)).map((result) => result.value.id),
		).to.deep.equal(["a"]);
		await index.put(new BatchDocument("c", "peerbit"));
		expect(
			(await iterator.all()).map((result) => result.value.id),
		).to.deep.equal(["b"]);
		expect(iterator.done()).to.equal(true);

		await indices.drop();
	});

	it("cancels an in-flight unsorted next when closed", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
		]);
		const iterator = index.iterate({
			query: new StringMatch({
				key: "tag",
				value: "peerbit",
				method: StringMatchMethod.exact,
			}),
		});

		const next = iterator.next(2);
		await iterator.close();

		expect(await next).to.deep.equal([]);
		expect(iterator.done()).to.equal(true);
		expect(await iterator.next(1)).to.deep.equal([]);
		await indices.drop();
	});

	it("cancels an in-flight unsorted pending scan when closed", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
		]);
		const iterator = index.iterate({
			query: new StringMatch({
				key: "tag",
				value: "peerbit",
				method: StringMatchMethod.exact,
			}),
		});

		const pending = iterator.pending();
		await iterator.close();

		expect(await pending).to.equal(0);
		expect(iterator.done()).to.equal(true);
		expect(await iterator.all()).to.deep.equal([]);
		await indices.drop();
	});
});
