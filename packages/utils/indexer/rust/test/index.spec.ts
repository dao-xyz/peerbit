import { field, serialize, variant, vec } from "@dao-xyz/borsh";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	IntegerCompare,
	Nested,
	Or,
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	id,
	toId,
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

class BridgeContext {
	@field({ type: "string" })
	head: string;

	constructor(head: string) {
		this.head = head;
	}
}

class BridgeDocumentWithContext {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	tag: string;

	@field({ type: "string" })
	title: string;

	@field({ type: BridgeContext })
	__context: BridgeContext;

	constructor(value: BridgeDocument, context: BridgeContext) {
		this.id = value.id;
		this.tag = value.tag;
		this.title = value.title;
		this.__context = context;
	}
}

class BridgeBytesDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: Uint8Array })
	payload: Uint8Array;

	constructor(id: string, payload: Uint8Array) {
		this.id = id;
		this.payload = payload;
	}
}

class BridgeCoordinateDocument {
	@id({ type: "string" })
	hash: string;

	@field({ type: "u64" })
	hashNumber: bigint;

	@field({ type: "string" })
	gid: string;

	@field({ type: vec("u64") })
	coordinates: bigint[];

	@field({ type: "u64" })
	wallTime: bigint;

	@field({ type: "bool" })
	assignedToRangeBoundary: boolean;

	@field({ type: Uint8Array })
	_meta: Uint8Array;

	constructor(
		hash: string,
		hashNumber: bigint,
		gid: string,
		coordinates: bigint[],
		wallTime: bigint,
		assignedToRangeBoundary: boolean,
		meta: Uint8Array,
	) {
		this.hash = hash;
		this.hashNumber = hashNumber;
		this.gid = gid;
		this.coordinates = coordinates;
		this.wallTime = wallTime;
		this.assignedToRangeBoundary = assignedToRangeBoundary;
		this._meta = meta;
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

@variant("bridge_variant_item")
class BridgeVariantNestedItem {
	@field({ type: "string" })
	tag: string;

	@field({ type: "u32" })
	score: number;

	constructor(tag: string, score: number) {
		this.tag = tag;
		this.score = score;
	}
}

@variant("bridge_variant_document")
class BridgeVariantNestedDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: vec(BridgeVariantNestedItem) })
	items: BridgeVariantNestedItem[];

	constructor(id: string, items: BridgeVariantNestedItem[]) {
		this.id = id;
		this.items = items;
	}
}

const isNodeRuntime = () =>
	Boolean(
		(
			globalThis as {
				process?: { versions?: { node?: string } };
			}
		).process?.versions?.node,
	);

const loadNodePersistenceHelpers = async () => {
	const fsPromises = "fs/promises";
	const osModule = "os";
	const pathModule = "path";
	const { mkdtemp, readFile, rm, stat, writeFile } = (await import(
		fsPromises
	)) as typeof import("fs/promises");
	const { tmpdir } = (await import(osModule)) as typeof import("os");
	const { join } = (await import(pathModule)) as typeof import("path");
	const directory = await mkdtemp(join(tmpdir(), "peerbit-indexer-rust-"));
	return { directory, join, readFile, rm, stat, writeFile };
};

const createPersistenceDirectory = (): string =>
	`peerbit-indexer-rust-${Date.now()}-${Math.random().toString(16).slice(2)}`;

const removeNodeDirectoryIfNeeded = async (directory: string): Promise<void> => {
	if (!isNodeRuntime()) {
		return;
	}
	const fsPromises = "fs/promises";
	const { rm } = (await import(fsPromises)) as typeof import("fs/promises");
	await rm(directory, { recursive: true, force: true });
};

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
	it("hands compiled borsh schema ir to native rust", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeNestedDocument });
		const { nativeSchemaIrStats: stats } = index as unknown as {
			nativeSchemaIrStats?: {
				rootFields: number;
				nodeCount: number;
				genericNodes: number;
			};
		};

		expect(stats).to.deep.equal({
			rootFields: 2,
			nodeCount: 6,
			genericNodes: 0,
		});

		await indices.drop();
	});

	it("indexes borsh-encoded document bytes in native rust", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeNestedDocument });
		(index as unknown as { fieldEncoder: () => never }).fieldEncoder = () => {
			throw new Error("TypeScript field encoder should not run");
		};
		await index.put(
			new BridgeNestedDocument("a", [
				new BridgeNestedItem("left", 1),
				new BridgeNestedItem("right", 3),
			]),
		);
		await index.put(
			new BridgeNestedDocument("b", [new BridgeNestedItem("left", 4)]),
		);

		const results = await index
			.iterate({
				query: new Nested({
					path: "items",
					query: [
						new StringMatch({ key: "tag", value: "left" }),
						new IntegerCompare({
							key: "score",
							compare: Compare.Greater,
							value: 2,
						}),
					],
				}),
			})
			.all();

		expect(results.map((result) => result.value.id)).to.deep.equal(["b"]);
		await indices.drop();
	});

	it("indexes borsh variant-prefixed document bytes in native rust", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeVariantNestedDocument });
		(index as unknown as { fieldEncoder: () => never }).fieldEncoder = () => {
			throw new Error("TypeScript field encoder should not run");
		};
		await index.put(
			new BridgeVariantNestedDocument("a", [
				new BridgeVariantNestedItem("left", 1),
				new BridgeVariantNestedItem("right", 3),
			]),
		);
		await index.put(
			new BridgeVariantNestedDocument("b", [
				new BridgeVariantNestedItem("left", 4),
			]),
		);

		const results = await index
			.iterate({
				query: new Nested({
					path: "items",
					query: [
						new StringMatch({ key: "tag", value: "left" }),
						new IntegerCompare({
							key: "score",
							compare: Compare.Greater,
							value: 2,
						}),
					],
				}),
			})
			.all();

		expect(results.map((result) => result.value.id)).to.deep.equal(["b"]);
		await indices.drop();
	});

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

	it("applies puts in a native batch", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const batchIndex = index as typeof index & {
			putBatch: (values: BridgeDocument[]) => Promise<void>;
		};

		await batchIndex.putBatch([
			new BridgeDocument("a", "peerbit", "native index"),
			new BridgeDocument("b", "peerbit", "batch put"),
			new BridgeDocument("c", "other", "separate"),
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

	it("coalesces a put and matching deletes through the native index", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const coalescedIndex = index as typeof index & {
			putAndDelete: (
				value: BridgeDocument,
				deleteOptions: { query: StringMatch },
			) => Promise<ReturnType<typeof toId>[]>;
		};

		await index.put(new BridgeDocument("a", "stale", "old"));
		await index.put(new BridgeDocument("b", "keep", "current"));
		const deleted = await coalescedIndex.putAndDelete(
			new BridgeDocument("c", "fresh", "new"),
			{ query: new StringMatch({ key: "tag", value: "stale" }) },
		);

		expect(deleted.map((id) => id.primitive)).to.deep.equal(["a"]);
		const results = await index
			.iterate({
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			})
			.all();
		expect(results.map((result) => result.value.id)).to.deep.equal(["b", "c"]);

		await indices.drop();
	});

	it("coalesces a put and exact id deletes through the native index", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const coalescedIndex = index as typeof index & {
			putAndDeleteIds: (
				value: BridgeDocument,
				deleteIds: string[],
			) => Promise<ReturnType<typeof toId>[]>;
		};

		await index.put(new BridgeDocument("a", "stale", "old"));
		await index.put(new BridgeDocument("b", "keep", "current"));
		const deleted = await coalescedIndex.putAndDeleteIds(
			new BridgeDocument("c", "fresh", "new"),
			["a"],
		);

		expect(deleted.map((id) => id.primitive)).to.deep.equal(["a"]);
		const results = await index
			.iterate({
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			})
			.all();
		expect(results.map((result) => result.value.id)).to.deep.equal(["b", "c"]);

		await indices.drop();
	});

	it("deletes exact ids through the native index", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const exactDeleteIndex = index as typeof index & {
			delIds: (deleteIds: string[]) => Promise<ReturnType<typeof toId>[]>;
		};

		await index.put(new BridgeDocument("a", "stale", "old"));
		await index.put(new BridgeDocument("b", "keep", "current"));
		const deleted = await exactDeleteIndex.delIds(["a"]);

		expect(deleted.map((id) => id.primitive)).to.deep.equal(["a"]);
		const results = await index.iterate().all();
		expect(results.map((result) => result.value.id)).to.deep.equal(["b"]);

		await indices.drop();
	});

	it("indexes shared-log coordinate fields through the typed native path", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeCoordinateDocument });
		const coordinateIndex = index as typeof index & {
			putSharedLogCoordinateAndDeleteIds: (
				value: BridgeCoordinateDocument,
				fields: {
					hash: string;
					hashNumber: bigint;
					gid: string;
					coordinates: bigint[];
					wallTime: bigint;
					assignedToRangeBoundary: boolean;
					metaBytes: Uint8Array;
				},
				deleteIds?: string[],
			) => Promise<ReturnType<typeof toId>[]>;
			putSharedLogCoordinateFieldsAndDeleteIds: (
				fields: {
					hash: string;
					hashNumber: bigint;
					gid: string;
					coordinates: bigint[];
					wallTime: bigint;
					assignedToRangeBoundary: boolean;
					metaBytes: Uint8Array;
				},
				deleteIds?: string[],
			) => Promise<ReturnType<typeof toId>[]>;
		};
		const meta = new Uint8Array([1, 2, 3]);
		const first = new BridgeCoordinateDocument(
			"a",
			10n,
			"gid-a",
			[4n, 8n],
			12n,
			true,
			meta,
		);
		await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds({
			hash: first.hash,
			hashNumber: first.hashNumber,
			gid: first.gid,
			coordinates: first.coordinates,
			wallTime: first.wallTime,
			assignedToRangeBoundary: first.assignedToRangeBoundary,
			metaBytes: first._meta,
		});

		const matches = await index
			.iterate({
				query: new And([
					new StringMatch({ key: "gid", value: "gid-a" }),
					new IntegerCompare({
						key: "coordinates",
						compare: Compare.Equal,
						value: 8n,
					}),
					new BoolQuery({
						key: "assignedToRangeBoundary",
						value: true,
					}),
					new ByteMatchQuery({ key: "_meta", value: meta }),
				]),
			})
			.all();
		expect(matches.map((entry) => entry.value.hash)).to.deep.equal(["a"]);

		const second = new BridgeCoordinateDocument(
			"b",
			11n,
			"gid-b",
			[16n],
			13n,
			false,
			new Uint8Array([4]),
		);
		const deleted = await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds(
			{
				hash: second.hash,
				hashNumber: second.hashNumber,
				gid: second.gid,
				coordinates: second.coordinates,
				wallTime: second.wallTime,
				assignedToRangeBoundary: second.assignedToRangeBoundary,
				metaBytes: second._meta,
			},
			["a"],
		);

		expect(deleted.map((id) => id.primitive)).to.deep.equal(["a"]);
		const remaining = await index.iterate().all();
		expect(remaining.map((entry) => entry.value.hash)).to.deep.equal(["b"]);

		await indices.drop();
	});

	it("accepts contextual document puts through the native index hook", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextualIndex = index as typeof index & {
			putWithContext: (
				value: BridgeDocument,
				id: ReturnType<typeof toId>,
				context: BridgeContext,
				options?: {
					replace?: boolean;
					encodedValueParts?: { prefix: Uint8Array; suffix: Uint8Array };
				},
			) => Promise<void>;
		};
		(index as unknown as { fieldEncoder: () => never }).fieldEncoder = () => {
			throw new Error("TypeScript field encoder should not run");
		};
		const document = new BridgeDocument("a", "peerbit", "native index");
		const context = new BridgeContext("head-a");

		await contextualIndex.putWithContext(
			document,
			toId("a"),
			context,
			{
				replace: false,
				encodedValueParts: {
					prefix: serialize(document),
					suffix: serialize(context),
				},
			},
		);

		const result = await index.get(toId("a"));
		expect(result?.value.__context.head).equal("head-a");
		expect(result?.value.title).equal("native index");

		const indexed = await index
			.iterate({
				query: new StringMatch({ key: "tag", value: "peerbit" }),
			})
			.all();
		expect(indexed.map((entry) => entry.value.__context.head)).to.deep.equal([
			"head-a",
		]);

		await indices.drop();
	});

	it("batch resolves contextual documents by head through the native index hook", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextualIndex = index as typeof index & {
			putWithContextBatch: (
				values: Array<{
					value: BridgeDocument;
					id: ReturnType<typeof toId>;
					context: BridgeContext;
					options?: {
						replace?: boolean;
						encodedValueParts?: { prefix: Uint8Array; suffix: Uint8Array };
					};
				}>,
			) => Promise<void>;
			getByContextHeadBatch: (
				heads: string[],
			) => Array<
				| { id: ReturnType<typeof toId>; value: BridgeDocumentWithContext }
				| undefined
			>;
		};
		const first = new BridgeDocument("a", "peerbit", "first");
		const second = new BridgeDocument("b", "peerbit", "second");
		const firstContext = new BridgeContext("head-a");
		const secondContext = new BridgeContext("head-b");
		await contextualIndex.putWithContextBatch([
			{
				value: first,
				id: toId("a"),
				context: firstContext,
				options: {
					encodedValueParts: {
						prefix: serialize(first),
						suffix: serialize(firstContext),
					},
				},
			},
			{
				value: second,
				id: toId("b"),
				context: secondContext,
				options: {
					encodedValueParts: {
						prefix: serialize(second),
						suffix: serialize(secondContext),
					},
				},
			},
		]);

		const resolved = contextualIndex.getByContextHeadBatch([
			"head-b",
			"missing",
			"head-a",
		]);
		expect(resolved.map((entry) => entry?.id.primitive)).to.deep.equal([
			"b",
			undefined,
			"a",
		]);

		await indices.drop();
	});

	it("keeps exact byte matching for large byte arrays without indexing every byte by default", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeBytesDocument });
		const payload = new Uint8Array(300).fill(7);
		await index.put(new BridgeBytesDocument("large", payload));

		const exactMatches = await index
			.iterate({
				query: new ByteMatchQuery({
					key: "payload",
					value: payload,
				}),
			})
			.all();
		expect(exactMatches.map((result) => result.value.id)).to.deep.equal([
			"large",
		]);

		const byteElementMatches = await index
			.iterate({
				query: new IntegerCompare({
					key: "payload",
					compare: Compare.Equal,
					value: 7,
				}),
			})
			.all();
		expect(byteElementMatches).to.be.empty;

		await indices.drop();
	});

	it("can opt into per-byte indexing for large byte arrays", async () => {
		const indices = create(undefined, {
			byteElementIndexLimit: Number.POSITIVE_INFINITY,
		});
		await indices.start();
		const index = await indices.init({ schema: BridgeBytesDocument });
		const payload = new Uint8Array(300).fill(7);
		await index.put(new BridgeBytesDocument("large", payload));

		const byteElementMatches = await index
			.iterate({
				query: new IntegerCompare({
					key: "payload",
					compare: Compare.Equal,
					value: 7,
				}),
			})
			.all();
		expect(byteElementMatches.map((result) => result.value.id)).to.deep.equal([
			"large",
		]);

		await indices.drop();
	});

	it("can opt out of per-byte indexing while keeping exact byte matching", async () => {
		const indices = create(undefined, { byteElementIndexLimit: 0 });
		await indices.start();
		const index = await indices.init({ schema: BridgeBytesDocument });
		const payload = new Uint8Array([7]);
		await index.put(new BridgeBytesDocument("small", payload));

		const exactMatches = await index
			.iterate({
				query: new ByteMatchQuery({
					key: "payload",
					value: payload,
				}),
			})
			.all();
		expect(exactMatches.map((result) => result.value.id)).to.deep.equal([
			"small",
		]);

		const byteElementMatches = await index
			.iterate({
				query: new IntegerCompare({
					key: "payload",
					compare: Compare.Equal,
					value: 7,
				}),
			})
			.all();
		expect(byteElementMatches).to.be.empty;

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

	it("replays durable puts before the writer is stopped", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			await writerIndex.put(new BridgeDocument("a", "peerbit", "durable put"));

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocument });
			const result = await readerIndex
				.iterate({
					query: new StringMatch({ key: "tag", value: "peerbit" }),
				})
				.all();

			expect(result.map((entry) => entry.value.id)).to.deep.equal(["a"]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable contextual encoded puts from prepared bytes", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		const reader = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocumentWithContext });
			const contextualWriter = writerIndex as typeof writerIndex & {
				putWithContext: (
					value: BridgeDocument & { __context?: BridgeContext },
					id: ReturnType<typeof toId>,
					context: BridgeContext,
					options?: {
						replace?: boolean;
						encodedValueParts?: { prefix: Uint8Array; suffix: Uint8Array };
					},
				) => Promise<void>;
			};
			(writerIndex as unknown as { fieldEncoder: () => never }).fieldEncoder =
				() => {
					throw new Error("TypeScript field encoder should not run");
				};

			const encodedDocument = new BridgeDocument(
				"a",
				"peerbit",
				"prepared durable",
			);
			const context = new BridgeContext("head-a");
			const journalValue = Object.create(
				BridgeDocumentWithContext.prototype,
			) as BridgeDocument & { __context?: BridgeContext };
			Object.defineProperties(journalValue, {
				id: { value: "a", enumerable: true },
				tag: {
					get() {
						throw new Error("journal should use prepared bytes");
					},
					enumerable: true,
				},
				title: {
					get() {
						throw new Error("journal should use prepared bytes");
					},
					enumerable: true,
				},
				__context: { value: context, enumerable: true },
			});

			await contextualWriter.putWithContext(journalValue, toId("a"), context, {
				replace: false,
				encodedValueParts: {
					prefix: serialize(encodedDocument),
					suffix: serialize(context),
				},
			});

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocumentWithContext });
			const result = await readerIndex.get(toId("a"));
			expect(result?.value.title).equal("prepared durable");
			expect(result?.value.__context.head).equal("head-a");

			const indexed = await readerIndex
				.iterate({
					query: new StringMatch({ key: "tag", value: "peerbit" }),
				})
				.all();
			expect(indexed.map((entry) => entry.value.__context.head)).to.deep.equal([
				"head-a",
			]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable contextual encoded put batches from prepared bytes", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		const reader = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocumentWithContext });
			const contextualWriter = writerIndex as typeof writerIndex & {
				putWithContextBatch: (
					values: Array<{
						value: BridgeDocument & { __context?: BridgeContext };
						id: ReturnType<typeof toId>;
						context: BridgeContext;
						options?: {
							replace?: boolean;
							encodedValueParts?: { prefix: Uint8Array; suffix: Uint8Array };
						};
					}>,
				) => Promise<void>;
			};
			(writerIndex as unknown as { fieldEncoder: () => never }).fieldEncoder =
				() => {
					throw new Error("TypeScript field encoder should not run");
				};

			const createJournalValue = (id: string, context: BridgeContext) => {
				const value = Object.create(
					BridgeDocumentWithContext.prototype,
				) as BridgeDocument & { __context?: BridgeContext };
				Object.defineProperties(value, {
					id: { value: id, enumerable: true },
					tag: {
						get() {
							throw new Error("journal batch should use prepared bytes");
						},
						enumerable: true,
					},
					title: {
						get() {
							throw new Error("journal batch should use prepared bytes");
						},
						enumerable: true,
					},
					__context: { value: context, enumerable: true },
				});
				return value;
			};

			const firstContext = new BridgeContext("head-a");
			const secondContext = new BridgeContext("head-b");
			await contextualWriter.putWithContextBatch([
				{
					value: createJournalValue("a", firstContext),
					id: toId("a"),
					context: firstContext,
					options: {
						replace: false,
						encodedValueParts: {
							prefix: serialize(
								new BridgeDocument("a", "peerbit", "first durable batch"),
							),
							suffix: serialize(firstContext),
						},
					},
				},
				{
					value: createJournalValue("b", secondContext),
					id: toId("b"),
					context: secondContext,
					options: {
						replace: false,
						encodedValueParts: {
							prefix: serialize(
								new BridgeDocument("b", "peerbit", "second durable batch"),
							),
							suffix: serialize(secondContext),
						},
					},
				},
			]);

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocumentWithContext });
			const result = await readerIndex
				.iterate({
					query: new StringMatch({ key: "tag", value: "peerbit" }),
					sort: new Sort({ key: "title" }),
				})
				.all();

			expect(result.map((entry) => entry.value.title)).to.deep.equal([
				"first durable batch",
				"second durable batch",
			]);
			expect(result.map((entry) => entry.value.__context.head)).to.deep.equal([
				"head-a",
				"head-b",
			]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable deletes before the writer is stopped", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			await writerIndex.put(new BridgeDocument("a", "peerbit", "delete me"));
			await writerIndex.put(new BridgeDocument("b", "other", "keep me"));
			await writerIndex.del({
				query: new StringMatch({ key: "tag", value: "peerbit" }),
			});

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocument });
			const result = await readerIndex.iterate().all();

			expect(result.map((entry) => entry.value.id)).to.deep.equal(["b"]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable coalesced put and deletes before the writer is stopped", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			const coalescedIndex = writerIndex as typeof writerIndex & {
				putAndDelete: (
					value: BridgeDocument,
					deleteOptions: { query: StringMatch },
				) => Promise<ReturnType<typeof toId>[]>;
			};
			await writerIndex.put(new BridgeDocument("a", "stale", "delete me"));
			await writerIndex.put(new BridgeDocument("b", "other", "keep me"));
			await coalescedIndex.putAndDelete(
				new BridgeDocument("c", "fresh", "new"),
				{ query: new StringMatch({ key: "tag", value: "stale" }) },
			);

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocument });
			const result = await readerIndex
				.iterate({
					sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
				})
				.all();

			expect(result.map((entry) => entry.value.id)).to.deep.equal(["b", "c"]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable coalesced put and exact id deletes before the writer is stopped", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			const coalescedIndex = writerIndex as typeof writerIndex & {
				putAndDeleteIds: (
					value: BridgeDocument,
					deleteIds: string[],
				) => Promise<ReturnType<typeof toId>[]>;
			};
			await writerIndex.put(new BridgeDocument("a", "stale", "delete me"));
			await writerIndex.put(new BridgeDocument("b", "other", "keep me"));
			await coalescedIndex.putAndDeleteIds(
				new BridgeDocument("c", "fresh", "new"),
				["a"],
			);

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocument });
			const result = await readerIndex
				.iterate({
					sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
				})
				.all();

			expect(result.map((entry) => entry.value.id)).to.deep.equal(["b", "c"]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable exact id deletes before the writer is stopped", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			const exactDeleteIndex = writerIndex as typeof writerIndex & {
				delIds: (deleteIds: string[]) => Promise<ReturnType<typeof toId>[]>;
			};
			await writerIndex.put(new BridgeDocument("a", "stale", "delete me"));
			await writerIndex.put(new BridgeDocument("b", "other", "keep me"));
			await exactDeleteIndex.delIds(["a"]);

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeDocument });
			const result = await readerIndex.iterate().all();

			expect(result.map((entry) => entry.value.id)).to.deep.equal(["b"]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("replays durable shared-log coordinate fields from the typed native path", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeCoordinateDocument });
			const writerIndexInternal = writerIndex as any;
			const originalAppendPut = writerIndexInternal.appendPut.bind(writerIndex);
			const originalAppendPutAndDeletes =
				writerIndexInternal.appendPutAndDeletes.bind(writerIndex);
			let appendPutCalls = 0;
			let appendPutAndDeletesCalls = 0;
			writerIndexInternal.appendPut = (...args: any[]) => {
				appendPutCalls++;
				return originalAppendPut(...args);
			};
			writerIndexInternal.appendPutAndDeletes = (...args: any[]) => {
				appendPutAndDeletesCalls++;
				return originalAppendPutAndDeletes(...args);
			};
			const coordinateIndex = writerIndex as typeof writerIndex & {
				putSharedLogCoordinateFieldsAndDeleteIds: (
					fields: {
						hash: string;
						hashNumber: bigint;
						gid: string;
						coordinates: bigint[];
						wallTime: bigint;
						assignedToRangeBoundary: boolean;
						metaBytes: Uint8Array;
					},
					deleteIds?: string[],
				) => Promise<ReturnType<typeof toId>[]>;
			};
			const value = new BridgeCoordinateDocument(
				"a",
				10n,
				"gid-a",
				[4n, 8n],
				12n,
				true,
				new Uint8Array([1, 2, 3]),
			);
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds({
				hash: value.hash,
				hashNumber: value.hashNumber,
				gid: value.gid,
				coordinates: value.coordinates,
				wallTime: value.wallTime,
				assignedToRangeBoundary: value.assignedToRangeBoundary,
				metaBytes: value._meta,
			});
			expect(appendPutCalls).to.equal(1);
			expect(appendPutAndDeletesCalls).to.equal(0);

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeCoordinateDocument });
			const result = await readerIndex
				.iterate({
					query: new IntegerCompare({
						key: "coordinates",
						compare: Compare.Equal,
						value: 8n,
					}),
				})
				.all();

			expect(result.map((entry) => entry.value.hash)).to.deep.equal(["a"]);
		} finally {
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("compacts the journal into a snapshot on stop", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, join, readFile, rm, stat } =
			await loadNodePersistenceHelpers();
		const indices = create(directory);
		try {
			await indices.start();
			const index = await indices.init({ schema: BridgeDocument });
			await index.put(new BridgeDocument("a", "peerbit", "snapshot"));

			const indexDirectory = join(directory, "id");
			expect((await stat(join(indexDirectory, "index.wal"))).size).to.be.greaterThan(
				0,
			);

			await indices.stop();

			await stat(join(indexDirectory, "index.bin"));
			try {
				await readFile(join(indexDirectory, "index.wal"));
				throw new Error("Expected journal to be removed after compaction");
			} catch (error: any) {
				expect(error?.code).to.equal("ENOENT");
			}

			const reopened = create(directory);
			await reopened.start();
			const reopenedIndex = await reopened.init({ schema: BridgeDocument });
			const result = await reopenedIndex.iterate().all();
			expect(result.map((entry) => entry.value.id)).to.deep.equal(["a"]);
			await reopened.drop();
		} finally {
			await indices.drop();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("compacts the journal after the configured operation threshold", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, join, readFile, rm, stat } =
			await loadNodePersistenceHelpers();
		const indices = create(directory, {
			persistence: { compactAfterOperations: 1 },
		});
		try {
			await indices.start();
			const index = await indices.init({ schema: BridgeDocument });
			await index.put(new BridgeDocument("a", "peerbit", "snapshot"));

			const indexDirectory = join(directory, "id");
			await stat(join(indexDirectory, "index.bin"));
			try {
				await readFile(join(indexDirectory, "index.wal"));
				throw new Error("Expected journal to be removed after compaction");
			} catch (error: any) {
				expect(error?.code).to.equal("ENOENT");
			}
		} finally {
			await indices.drop();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("recovers a compacted temp snapshot when the primary snapshot is torn", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, join, readFile, rm, writeFile } =
			await loadNodePersistenceHelpers();
		const indices = create(directory);
		let reopened: ReturnType<typeof create> | undefined;
		try {
			await indices.start();
			const index = await indices.init({ schema: BridgeDocument });
			await index.put(new BridgeDocument("a", "peerbit", "recoverable"));
			await indices.stop();

			const indexDirectory = join(directory, "id");
			const snapshotPath = join(indexDirectory, "index.bin");
			const snapshotBytes = await readFile(snapshotPath);
			await writeFile(join(indexDirectory, "index.bin.tmp"), snapshotBytes);
			await writeFile(snapshotPath, snapshotBytes.subarray(0, 8));

			reopened = create(directory);
			await reopened.start();
			const reopenedIndex = await reopened.init({ schema: BridgeDocument });
			const result = await reopenedIndex.iterate().all();
			expect(result.map((entry) => entry.value.id)).to.deep.equal(["a"]);
		} finally {
			await reopened?.drop();
			await indices.drop();
			await rm(directory, { recursive: true, force: true });
		}
	});
});
