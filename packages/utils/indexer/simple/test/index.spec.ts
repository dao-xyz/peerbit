import { field, variant } from "@dao-xyz/borsh";
import {
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	id,
	toId,
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

@variant("versioned_document_context")
class VersionContext {
	@field({ type: "string" })
	head: string;

	constructor(head: string) {
		this.head = head;
	}
}

@variant(0)
class VersionedDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: VersionContext })
	__context: VersionContext;

	constructor(id: string, head: string) {
		this.id = id;
		this.__context = new VersionContext(head);
	}
}

describe("all", () => {
	tests(create, "transient", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: false,
	});

	tieParityTests(create);

	it("keeps a version put while a conditional delete evaluates the row", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: VersionedDocument });
		const replacement = new VersionedDocument("a", "replacement");
		let putDuringMatch = false;
		// The delete reads __context while it matches the stored row. Replace the
		// row at that moment, after the row was read and before it is deleted.
		await index.put(
			new Proxy(new VersionedDocument("a", "original"), {
				get: (target, property, receiver) => {
					if (property === "__context" && putDuringMatch) {
						putDuringMatch = false;
						index.put(replacement);
					}
					return Reflect.get(target, property, receiver);
				},
			}),
		);

		putDuringMatch = true;
		const deleted = await index.del({
			query: [
				new StringMatch({ key: "id", value: "a" }),
				new StringMatch({ key: ["__context", "head"], value: "original" }),
			],
		});

		expect(putDuringMatch).to.equal(false);
		expect(deleted).to.deep.equal([]);
		expect((await index.get(toId("a")))?.value).to.equal(replacement);

		await indices.drop();
	});

	it("reads only the row a query names by id", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: VersionedDocument });
		// Each stored row records the queries that read it.
		const read = new Set<string>();
		for (let i = 0; i < 100; i++) {
			await index.put(
				new Proxy(new VersionedDocument(String(i), "first"), {
					get: (target, property, receiver) => {
						read.add(target.id);
						return Reflect.get(target, property, receiver);
					},
				}),
			);
		}
		const idAndHead = (id: string, head: string) => [
			new StringMatch({ key: "id", value: id }),
			new StringMatch({ key: ["__context", "head"], value: head }),
		];

		read.clear();
		expect(await index.del({ query: idAndHead("7", "second") })).to.deep.equal(
			[],
		);
		expect([...read]).to.deep.equal(["7"]);

		read.clear();
		const deleted = await index.del({ query: idAndHead("7", "first") });
		expect(deleted.map((key) => key.primitive)).to.deep.equal(["7"]);
		expect([...read]).to.deep.equal(["7"]);
		expect(await index.getSize()).to.equal(99);

		read.clear();
		expect(await index.count({ query: idAndHead("8", "first") })).to.equal(1);
		expect([...read]).to.deep.equal(["8"]);

		await indices.drop();
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

	it("keeps default unsorted pages on an eager snapshot", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "original-a"),
			new BatchDocument("b", "original-b"),
			new BatchDocument("c", "original-c"),
		]);

		const iterator = index.iterate(undefined, { reference: true });
		expect(
			(await iterator.next(1)).map((result) => result.value.id),
		).to.deep.equal(["a"]);

		await index.del({
			query: new StringMatch({
				key: "id",
				value: "b",
				method: StringMatchMethod.exact,
			}),
		});
		await index.put(new BatchDocument("c", "replacement-c"));
		await index.put(new BatchDocument("d", "appended-d"));

		const remaining = await iterator.all();
		expect(
			remaining.map((result) => [result.value.id, result.value.tag]),
		).to.deep.equal([
			["b", "original-b"],
			["c", "original-c"],
		]);
		expect(iterator.done()).to.equal(true);

		await indices.drop();
	});

	it("lazily evaluates explicitly weak unsorted pages", async () => {
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
				consistency: "weak",
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
		// Only the two returned rows are evaluated. The remaining 62 rows stay
		// behind the Map cursor instead of being materialized eagerly.
		expect(tagReads).to.equal(2);
		expect(iterator.done()).to.equal(false);

		await iterator.close();
		expect(tagReads).to.equal(2);
		await indices.drop();
	});

	it("marks an exact final weak page done without a lookahead", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		let tagReads = 0;

		for (let i = 0; i < 3; i++) {
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
				consistency: "weak",
				query: new StringMatch({
					key: "tag",
					value: "peerbit",
					method: StringMatchMethod.exact,
				}),
			},
			{ reference: true },
		);

		expect(
			(await iterator.next(3)).map((result) => result.value.id),
		).to.deep.equal(["0", "1", "2"]);
		expect(tagReads).to.equal(3);
		expect(iterator.done()).to.equal(true);
		expect(await iterator.next(1)).to.deep.equal([]);
		expect(tagReads).to.equal(3);

		await indices.drop();
	});

	it("drains weak unsorted pending rows without losing the first result", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
			new BatchDocument("c", "peerbit"),
		]);

		const iterator = index.iterate({ consistency: "weak" });
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

	it("keeps explicitly weak unsorted iteration finite across appends", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
		]);

		const iterator = index.iterate({ consistency: "weak" });
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

	it("cancels an in-flight weak unsorted next when closed", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
		]);
		const iterator = index.iterate({
			consistency: "weak",
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

	it("cancels an in-flight weak unsorted pending scan when closed", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BatchDocument });
		await index.putBatch?.([
			new BatchDocument("a", "peerbit"),
			new BatchDocument("b", "peerbit"),
		]);
		const iterator = index.iterate({
			consistency: "weak",
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
