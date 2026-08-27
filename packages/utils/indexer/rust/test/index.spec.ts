import {
	BinaryWriter,
	field,
	serialize,
	serializer,
	variant,
	vec,
} from "@dao-xyz/borsh";
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

class CountingBridgeDocument {
	static serializeCalls = 0;

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

	@serializer()
	serializeValue(writer: BinaryWriter): void {
		CountingBridgeDocument.serializeCalls++;
		writer.string(this.id);
		writer.string(this.tag);
		writer.string(this.title);
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

class BridgeFloatDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "f64" })
	score: number;

	constructor(id: string, score: number) {
		this.id = id;
		this.score = score;
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

type BridgeEncodedParts = {
	prefix: Uint8Array;
	suffix: Uint8Array;
};

type BridgeContextHooks = {
	putWithContext(
		value: BridgeDocument,
		id: ReturnType<typeof toId>,
		context: BridgeContext,
		options: { encodedValueParts: BridgeEncodedParts },
	): Promise<void> | void;
	putWithContextBatch(
		values: Array<{
			value: BridgeDocument;
			id: ReturnType<typeof toId>;
			context: BridgeContext;
			options: { encodedValueParts: BridgeEncodedParts };
		}>,
	): Promise<void>;
	putStoredContextualEncodedValue(
		id: ReturnType<typeof toId>,
		encodedValueParts: BridgeEncodedParts,
	): Promise<void> | void | false;
	putStoredContextualEncodedValueBatch(
		values: Array<{
			id: ReturnType<typeof toId>;
			encodedValueParts: BridgeEncodedParts;
		}>,
	): Promise<boolean>;
};

const bridgeEncodedParts = (
	value: BridgeDocument,
	context: BridgeContext,
): BridgeEncodedParts => ({
	prefix: serialize(value),
	suffix: serialize(context),
});

const captureRejection = async (operation: () => unknown): Promise<unknown> => {
	try {
		await operation();
	} catch (error) {
		return error;
	}
	throw new Error("Expected operation to reject");
};

describe("all", () => {
	tests(create, "persist", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: true,
	});
	tests(create, "transient", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: true,
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

	it("preserves encoded runtime failures without a fallback mutation", async () => {
		for (const encodedFailure of [
			new Error("forced native encoded put failure"),
			new WebAssembly.RuntimeError("forced native encoded runtime failure"),
		]) {
			const indices = create();
			await indices.start();
			const index = await indices.init({ schema: BridgeDocument });
			const internal = index as unknown as {
				native: {
					put_encoded: (...args: unknown[]) => void;
					put: (...args: unknown[]) => void;
				};
			};
			const native = internal.native;
			let encodedCalls = 0;
			let fallbackCalls = 0;

			try {
				internal.native = new Proxy(native, {
					get(target, property, receiver) {
						if (property === "put_encoded") {
							return () => {
								encodedCalls++;
								throw encodedFailure;
							};
						}
						if (property === "put") {
							return () => {
								fallbackCalls++;
							};
						}
						return Reflect.get(target, property, receiver);
					},
				});

				let rejection: unknown;
				try {
					await index.put(
						new BridgeDocument("a", "peerbit", "native error"),
					);
				} catch (error) {
					rejection = error;
				}

				expect(rejection).to.equal(encodedFailure);
				expect(encodedCalls).to.equal(1);
				expect(fallbackCalls).to.equal(0);
			} finally {
				internal.native = native;
				await indices.drop();
			}
		}
	});

	it("retains the field-encoder fallback for bridge extraction rejections", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const internal = index as unknown as {
			native: {
				put_encoded: (...args: unknown[]) => void;
				put: (...args: unknown[]) => void;
			};
		};
		const native = internal.native;
		const nativePut = native.put.bind(native);
		let encodedCalls = 0;
		let fallbackCalls = 0;

		try {
			internal.native = new Proxy(native, {
				get(target, property, receiver) {
					if (property === "put_encoded") {
						return () => {
							encodedCalls++;
							// wasm-bindgen throws Rust bridge rejections as strings.
							// eslint-disable-next-line @typescript-eslint/only-throw-error
							throw "forced bridge extraction rejection";
						};
					}
					if (property === "put") {
						return (...args: unknown[]) => {
							fallbackCalls++;
							nativePut(...args);
						};
					}
					return Reflect.get(target, property, receiver);
				},
			});

			await index.put(new BridgeDocument("a", "peerbit", "fallback"));
			expect(encodedCalls).to.equal(1);
			expect(fallbackCalls).to.equal(1);
			expect((await index.get(toId("a")))?.value.title).to.equal("fallback");
		} finally {
			internal.native = native;
			await indices.drop();
		}
	});

	it("preserves encoded-parts runtime failures without fallback", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextualIndex = index as unknown as {
			putWithEncodedValueParts: (
				value: BridgeDocumentWithContext,
				id: ReturnType<typeof toId>,
				encodedValueParts: { prefix: Uint8Array; suffix: Uint8Array },
			) => Promise<void>;
		};
		const internal = index as unknown as {
			native: {
				put_encoded_parts: (...args: unknown[]) => void;
				put: (...args: unknown[]) => void;
			};
		};
		const native = internal.native;
		const encodedFailure = new WebAssembly.RuntimeError(
			"forced native encoded-parts runtime failure",
		);
		let encodedCalls = 0;
		let fallbackCalls = 0;

		try {
			internal.native = new Proxy(native, {
				get(target, property, receiver) {
					if (property === "put_encoded_parts") {
						return () => {
							encodedCalls++;
							throw encodedFailure;
						};
					}
					if (property === "put") {
						return () => {
							fallbackCalls++;
						};
					}
					return Reflect.get(target, property, receiver);
				},
			});

			const document = new BridgeDocument("a", "peerbit", "native parts");
			const context = new BridgeContext("head-a");
			let rejection: unknown;
			try {
				await contextualIndex.putWithEncodedValueParts(
					new BridgeDocumentWithContext(document, context),
					toId("a"),
					{
						prefix: serialize(document),
						suffix: serialize(context),
					},
				);
			} catch (error) {
				rejection = error;
			}

			expect(rejection).to.equal(encodedFailure);
			expect(encodedCalls).to.equal(1);
			expect(fallbackCalls).to.equal(0);
		} finally {
			internal.native = native;
			await indices.drop();
		}
	});

	it("preserves stored encoded runtime failures without fallback mutations", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextual = index as unknown as BridgeContextHooks;
		const internal = index as unknown as { native: object };
		const native = internal.native;
		const singleFailure = new WebAssembly.RuntimeError(
			"forced stored single runtime failure",
		);
		const batchFailure = new Error("forced stored batch runtime failure");
		const calls = { single: 0, batch: 0, fallback: 0 };
		let mode: "single" | "batch" = "single";
		let singleRejection: unknown;
		let batchRejection: unknown;

		try {
			try {
				internal.native = new Proxy(native, {
					get(target, property, receiver) {
						if (property === "put_encoded_parts_stored" && mode === "single") {
							return () => {
								calls.single++;
								throw singleFailure;
							};
						}
						if (
							property === "put_encoded_parts_stored_batch" &&
							mode === "batch"
						) {
							return () => {
								calls.batch++;
								throw batchFailure;
							};
						}
						if (
							property === "put_encoded_parts" ||
							property === "put_encoded_parts_batch" ||
							property === "put"
						) {
							return () => {
								calls.fallback++;
							};
						}
						return Reflect.get(target, property, receiver);
					},
				});

				const single = new BridgeDocument("single", "peerbit", "single");
				const singleContext = new BridgeContext("head-single");
				singleRejection = await captureRejection(() =>
					contextual.putWithContext(single, toId(single.id), singleContext, {
						encodedValueParts: bridgeEncodedParts(single, singleContext),
					}),
				);

				mode = "batch";
				const first = new BridgeDocument("a", "peerbit", "first");
				const second = new BridgeDocument("b", "peerbit", "second");
				const firstContext = new BridgeContext("head-a");
				const secondContext = new BridgeContext("head-b");
				batchRejection = await captureRejection(() =>
					contextual.putWithContextBatch([
						{
							value: first,
							id: toId(first.id),
							context: firstContext,
							options: {
								encodedValueParts: bridgeEncodedParts(first, firstContext),
							},
						},
						{
							value: second,
							id: toId(second.id),
							context: secondContext,
							options: {
								encodedValueParts: bridgeEncodedParts(second, secondContext),
							},
						},
					]),
				);
			} finally {
				internal.native = native;
			}

			expect(singleRejection).to.equal(singleFailure);
			expect((singleRejection as Error).message).to.equal(singleFailure.message);
			expect(batchRejection).to.equal(batchFailure);
			expect((batchRejection as Error).message).to.equal(batchFailure.message);
			expect(calls).to.deep.equal({ single: 1, batch: 1, fallback: 0 });
			expect(await index.get(toId("single"))).to.equal(undefined);
			expect(await index.get(toId("a"))).to.equal(undefined);
			expect(await index.get(toId("b"))).to.equal(undefined);
		} finally {
			internal.native = native;
			await indices.drop();
		}
	});

	it("preserves encoded-parts batch runtime failures without per-entry retry", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextual = index as unknown as BridgeContextHooks;
		const internal = index as unknown as { native: object };
		const native = internal.native;
		const batchFailure = new WebAssembly.RuntimeError(
			"forced encoded-parts batch runtime failure",
		);
		const calls = { storedBatch: 0, encodedBatch: 0, perEntry: 0 };
		let rejection: unknown;

		try {
			try {
				internal.native = new Proxy(native, {
					get(target, property, receiver) {
						if (property === "put_encoded_parts_stored_batch") {
							return () => {
								calls.storedBatch++;
								// eslint-disable-next-line @typescript-eslint/only-throw-error
								throw "forced stored batch bridge rejection";
							};
						}
						if (property === "put_encoded_parts_batch") {
							return () => {
								calls.encodedBatch++;
								throw batchFailure;
							};
						}
						if (property === "put_encoded_parts" || property === "put") {
							return () => {
								calls.perEntry++;
							};
						}
						return Reflect.get(target, property, receiver);
					},
				});

				const first = new BridgeDocument("a", "peerbit", "first");
				const second = new BridgeDocument("b", "peerbit", "second");
				const firstContext = new BridgeContext("head-a");
				const secondContext = new BridgeContext("head-b");
				rejection = await captureRejection(() =>
					contextual.putWithContextBatch([
						{
							value: first,
							id: toId(first.id),
							context: firstContext,
							options: {
								encodedValueParts: bridgeEncodedParts(first, firstContext),
							},
						},
						{
							value: second,
							id: toId(second.id),
							context: secondContext,
							options: {
								encodedValueParts: bridgeEncodedParts(second, secondContext),
							},
						},
					]),
				);
			} finally {
				internal.native = native;
			}

			expect(rejection).to.equal(batchFailure);
			expect((rejection as Error).message).to.equal(batchFailure.message);
			expect(calls).to.deep.equal({
				storedBatch: 1,
				encodedBatch: 1,
				perEntry: 0,
			});
			expect(await index.get(toId("a"))).to.equal(undefined);
			expect(await index.get(toId("b"))).to.equal(undefined);
		} finally {
			internal.native = native;
			await indices.drop();
		}
	});

	it("preserves validation runtime failures before persistence", async () => {
		const directory = createPersistenceDirectory();
		const indices = create(directory);
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextual = index as unknown as BridgeContextHooks;
		const internal = index as unknown as {
			native: object;
			appendPut: (...args: unknown[]) => Promise<void>;
			snapshotFile: {
				appendPutBatch: (...args: unknown[]) => Promise<void>;
			};
		};
		const native = internal.native;
		const originalAppendPut = internal.appendPut;
		const originalAppendPutBatch = internal.snapshotFile.appendPutBatch;
		const singleFailure = new Error("forced validation single runtime failure");
		const batchFailure = new WebAssembly.RuntimeError(
			"forced validation batch runtime failure",
		);
		const calls = {
			validateSingle: 0,
			validateBatch: 0,
			appendSingle: 0,
			appendBatch: 0,
			stored: 0,
		};
		let runtimeFailure = true;
		let singleRejection: unknown;
		let batchRejection: unknown;
		let singleFallback: unknown;
		let batchFallback: unknown;

		try {
			try {
				internal.appendPut = async () => {
					calls.appendSingle++;
				};
				internal.snapshotFile.appendPutBatch = async () => {
					calls.appendBatch++;
				};
				internal.native = new Proxy(native, {
					get(target, property, receiver) {
						if (property === "validate_encoded_parts") {
							return () => {
								calls.validateSingle++;
								if (runtimeFailure) {
									throw singleFailure;
								}
								// eslint-disable-next-line @typescript-eslint/only-throw-error
								throw "forced validation single bridge rejection";
							};
						}
						if (property === "validate_encoded_parts_batch") {
							return () => {
								calls.validateBatch++;
								if (runtimeFailure) {
									throw batchFailure;
								}
								// eslint-disable-next-line @typescript-eslint/only-throw-error
								throw "forced validation batch bridge rejection";
							};
						}
						if (
							property === "put_encoded_parts_stored" ||
							property === "put_encoded_parts_stored_batch"
						) {
							return () => {
								calls.stored++;
							};
						}
						return Reflect.get(target, property, receiver);
					},
				});

				const first = new BridgeDocument("a", "peerbit", "first");
				const second = new BridgeDocument("b", "peerbit", "second");
				const firstParts = bridgeEncodedParts(first, new BridgeContext("head-a"));
				const secondParts = bridgeEncodedParts(
					second,
					new BridgeContext("head-b"),
				);

				singleRejection = await captureRejection(() =>
					contextual.putStoredContextualEncodedValue(
						toId(first.id),
						firstParts,
					),
				);
				batchRejection = await captureRejection(() =>
					contextual.putStoredContextualEncodedValueBatch([
						{ id: toId(first.id), encodedValueParts: firstParts },
						{ id: toId(second.id), encodedValueParts: secondParts },
					]),
				);

				runtimeFailure = false;
				singleFallback = await contextual.putStoredContextualEncodedValue(
					toId(first.id),
					firstParts,
				);
				batchFallback =
					await contextual.putStoredContextualEncodedValueBatch([
						{ id: toId(first.id), encodedValueParts: firstParts },
						{ id: toId(second.id), encodedValueParts: secondParts },
					]);
			} finally {
				internal.native = native;
				internal.appendPut = originalAppendPut;
				internal.snapshotFile.appendPutBatch = originalAppendPutBatch;
			}

			expect(singleRejection).to.equal(singleFailure);
			expect((singleRejection as Error).message).to.equal(singleFailure.message);
			expect(batchRejection).to.equal(batchFailure);
			expect((batchRejection as Error).message).to.equal(batchFailure.message);
			expect(singleFallback).to.equal(false);
			expect(batchFallback).to.equal(false);
			expect(calls).to.deep.equal({
				validateSingle: 2,
				validateBatch: 2,
				appendSingle: 0,
				appendBatch: 0,
				stored: 0,
			});
			expect(await index.get(toId("a"))).to.equal(undefined);
			expect(await index.get(toId("b"))).to.equal(undefined);
		} finally {
			internal.native = native;
			internal.appendPut = originalAppendPut;
			internal.snapshotFile.appendPutBatch = originalAppendPutBatch;
			await indices.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
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

	it("keeps a sorted iterator stable across a native put batch", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const batchIndex = index as typeof index & {
			putBatch: (values: BridgeDocument[]) => Promise<void>;
		};

		await index.put(new BridgeDocument("a", "peerbit", "first"));
		await index.put(new BridgeDocument("c", "peerbit", "third"));
		const iterator = index.iterate({
			sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
		});
		expect(
			(await iterator.next(1)).map((result) => result.value.id),
		).to.deep.equal(["a"]);

		await batchIndex.putBatch([
			new BridgeDocument("b", "peerbit", "second"),
			new BridgeDocument("d", "peerbit", "fourth"),
		]);

		expect(
			(await iterator.all()).map((result) => result.value.id),
		).to.deep.equal(["b", "c", "d"]);
		await indices.drop();
	});

	it("invalidates iterators after a visible put even when trailing compaction fails", async () => {
		const directory = createPersistenceDirectory();
		const indices = create(directory);
		try {
			await indices.start();
			const index = await indices.init({ schema: BridgeDocument });
			await index.put(new BridgeDocument("b", "peerbit", "second"));
			await index.put(new BridgeDocument("c", "peerbit", "third"));
			const internal = index as unknown as {
				appendPut: (...args: unknown[]) => Promise<void>;
				compactIfNeeded: () => Promise<void>;
				mutationVersion: number;
			};
			const appendPut = internal.appendPut.bind(index);
			const versionBeforeAppendFailure = internal.mutationVersion;
			internal.appendPut = async () => {
				throw new Error("forced pre-mutation append failure");
			};
			try {
				await index.put(new BridgeDocument("z", "peerbit", "not visible"));
				expect.fail("put should reject before the native mutation");
			} catch (error) {
				expect((error as Error).message).to.equal(
					"forced pre-mutation append failure",
				);
			} finally {
				internal.appendPut = appendPut;
			}
			expect(internal.mutationVersion).to.equal(versionBeforeAppendFailure);

			const iterator = index.iterate({
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			});
			expect(
				(await iterator.next(1)).map((result) => result.value.id),
			).to.deep.equal(["b"]);

			const compactIfNeeded = internal.compactIfNeeded.bind(index);
			internal.compactIfNeeded = async () => {
				throw new Error("forced trailing compaction failure");
			};
			let rejected = false;
			try {
				await index.put(new BridgeDocument("a", "peerbit", "first"));
			} catch (error) {
				rejected = true;
				expect((error as Error).message).to.equal(
					"forced trailing compaction failure",
				);
			} finally {
				internal.compactIfNeeded = compactIfNeeded;
			}
			expect(rejected).to.be.true;

			expect(
				(await iterator.next(1)).map((result) => result.value.id),
			).to.deep.equal(["a"]);
			expect(
				(await iterator.all()).map((result) => result.value.id),
			).to.deep.equal(["c"]);
		} finally {
			await indices.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
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

	it("does not skip after exact-id deletion of an already yielded row", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocument });
		const exactDeleteIndex = index as typeof index & {
			delIds: (deleteIds: string[]) => Promise<ReturnType<typeof toId>[]>;
		};

		await index.put(new BridgeDocument("a", "stale", "first"));
		await index.put(new BridgeDocument("b", "keep", "second"));
		await index.put(new BridgeDocument("c", "keep", "third"));
		const iterator = index.iterate({
			sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
		});
		expect(
			(await iterator.next(1)).map((result) => result.value.id),
		).to.deep.equal(["a"]);

		await exactDeleteIndex.delIds(["a"]);

		expect(
			(await iterator.all()).map((result) => result.value.id),
		).to.deep.equal(["b", "c"]);
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

	it("indexes shared-log coordinate fields through the hash-delete native path", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeCoordinateDocument });
		const coordinateIndex = index as typeof index & {
			putSharedLogCoordinateFieldsAndDeleteHashes: (
				fields: {
					hash: string;
					hashNumber: bigint;
					gid: string;
					coordinates: bigint[];
					wallTime: bigint;
					assignedToRangeBoundary: boolean;
					metaBytes: Uint8Array;
				},
				deleteHashes?: string[],
			) => Promise<ReturnType<typeof toId>[]>;
		};

		await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashes({
			hash: "a",
			hashNumber: 10n,
			gid: "gid-a",
			coordinates: [4n],
			wallTime: 12n,
			assignedToRangeBoundary: true,
			metaBytes: new Uint8Array([1, 2, 3]),
		});
		const deleted =
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashes(
				{
					hash: "b",
					hashNumber: 11n,
					gid: "gid-b",
					coordinates: [8n],
					wallTime: 13n,
					assignedToRangeBoundary: false,
					metaBytes: new Uint8Array([4]),
				},
				["a"],
			);

		expect(deleted.map((id) => id.primitive)).to.deep.equal(["a"]);
		const remaining = await index.iterate().all();
		expect(remaining.map((entry) => entry.value.hash)).to.deep.equal(["b"]);

		await indices.drop();
	});

		it("indexes shared-log coordinate fields through the no-return hash-delete native path", async () => {
			const indices = create();
			await indices.start();
			const index = await indices.init({ schema: BridgeCoordinateDocument });
			const coordinateIndex = index as typeof index & {
			putSharedLogCoordinateFieldsAndDeleteHashesNoReturn: (
				fields: {
					hash: string;
					hashNumber: bigint;
					gid: string;
					coordinates: bigint[];
					wallTime: bigint;
					assignedToRangeBoundary: boolean;
					metaBytes: Uint8Array;
				},
				deleteHashes?: string[],
			) => Promise<void> | void;
		};

		await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn({
			hash: "a",
			hashNumber: 10n,
			gid: "gid-a",
			coordinates: [4n],
			wallTime: 12n,
			assignedToRangeBoundary: true,
			metaBytes: new Uint8Array([1, 2, 3]),
		});
		const result =
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn(
				{
					hash: "b",
					hashNumber: 11n,
					gid: "gid-b",
					coordinates: [8n],
					wallTime: 13n,
					assignedToRangeBoundary: false,
					metaBytes: new Uint8Array([4]),
				},
				["a"],
			);

		expect(result).equal(undefined);
		const remaining = await index.iterate().all();
		expect(remaining.map((entry) => entry.value.hash)).to.deep.equal(["b"]);

			await indices.drop();
		});

		it("indexes shared-log coordinate fields through the encoded no-return native path", async () => {
			const indices = create();
			await indices.start();
			const index = await indices.init({ schema: BridgeCoordinateDocument });
			const coordinateIndex = index as typeof index & {
				putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn: (
					fields: {
						hash: string;
						hashNumber: bigint;
						gid: string;
						coordinates: bigint[];
						wallTime: bigint;
						assignedToRangeBoundary: boolean;
						metaBytes: Uint8Array;
					},
					deleteHashes?: string[],
				) => Promise<void> | void;
			};

			await coordinateIndex.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn(
				{
					hash: "a",
					hashNumber: 10n,
					gid: "gid-a",
					coordinates: [4n],
					wallTime: 12n,
					assignedToRangeBoundary: true,
					metaBytes: new Uint8Array([1, 2, 3]),
				},
			);
			const result =
				await coordinateIndex.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn(
					{
						hash: "b",
						hashNumber: 11n,
						gid: "gid-b",
						coordinates: [8n],
						wallTime: 13n,
						assignedToRangeBoundary: false,
						metaBytes: new Uint8Array([4]),
					},
					["a"],
				);

			expect(result).equal(undefined);
			const remaining = await index.iterate().all();
			expect(remaining.map((entry) => entry.value.hash)).to.deep.equal(["b"]);
			expect(remaining[0].value).to.be.instanceOf(BridgeCoordinateDocument);

			await indices.drop();
		});

		it("indexes shared-log coordinate fields through the no-return hash-delete native batch path", async () => {
			const indices = create();
			await indices.start();
		const index = await indices.init({ schema: BridgeCoordinateDocument });
		const coordinateIndex = index as typeof index & {
			putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn: (
				values: Array<{
					fields: {
						hash: string;
						hashNumber: bigint;
						gid: string;
						coordinates: bigint[];
						wallTime: bigint;
						assignedToRangeBoundary: boolean;
						metaBytes: Uint8Array;
					};
					deleteHashes?: string[];
				}>,
			) => Promise<void> | void;
		};

		const result =
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn(
				[
					{
						fields: {
							hash: "a",
							hashNumber: 10n,
							gid: "gid-a",
							coordinates: [4n],
							wallTime: 12n,
							assignedToRangeBoundary: true,
							metaBytes: new Uint8Array([1, 2, 3]),
						},
					},
					{
						fields: {
							hash: "b",
							hashNumber: 11n,
							gid: "gid-b",
							coordinates: [8n],
							wallTime: 13n,
							assignedToRangeBoundary: false,
							metaBytes: new Uint8Array([4]),
						},
						deleteHashes: ["a"],
					},
				],
			);

		expect(result).equal(undefined);
		const remaining = await index.iterate().all();
		expect(remaining.map((entry) => entry.value.hash)).to.deep.equal(["b"]);

		await indices.drop();
	});

	it("skips durable coordinate encoding for transient shared-log coordinate puts", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeCoordinateDocument });
		const coordinateIndex = index as typeof index & {
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
			putSharedLogCoordinateFieldsAndDeleteIdsBatch: (
				values: Array<{
					fields: {
						hash: string;
						hashNumber: bigint;
						gid: string;
						coordinates: bigint[];
						wallTime: bigint;
						assignedToRangeBoundary: boolean;
						metaBytes: Uint8Array;
					};
					deleteIds?: string[];
				}>,
			) => Promise<ReturnType<typeof toId>[]>;
			putSharedLogCoordinateFieldsAndDeleteHashesBatch: (
				values: Array<{
					fields: {
						hash: string;
						hashNumber: bigint;
						gid: string;
						coordinates: bigint[];
						wallTime: bigint;
						assignedToRangeBoundary: boolean;
						metaBytes: Uint8Array;
					};
					deleteHashes?: string[];
				}>,
			) => Promise<ReturnType<typeof toId>[]>;
		};
		const indexInternal = index as any;
		const originalEncode =
			indexInternal.encodeSharedLogCoordinatePersistenceValue.bind(index);
		let encodeCalls = 0;
		indexInternal.encodeSharedLogCoordinatePersistenceValue = (...args: any[]) => {
			encodeCalls++;
			return originalEncode(...args);
		};
		try {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds({
				hash: "a",
				hashNumber: 10n,
				gid: "gid-a",
				coordinates: [4n, 8n],
				wallTime: 12n,
				assignedToRangeBoundary: true,
				metaBytes: new Uint8Array([1, 2, 3]),
			});

			const deleted =
				await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch([
					{
						fields: {
							hash: "b",
							hashNumber: 11n,
							gid: "gid-b",
							coordinates: [16n],
							wallTime: 13n,
							assignedToRangeBoundary: false,
							metaBytes: new Uint8Array([4]),
						},
						deleteIds: ["a"],
					},
				]);

			expect(deleted.map((id) => id.primitive)).to.deep.equal(["a"]);
			const hashBatchDeleted =
				await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch([
					{
						fields: {
							hash: "c",
							hashNumber: 12n,
							gid: "gid-c",
							coordinates: [32n],
							wallTime: 14n,
							assignedToRangeBoundary: false,
							metaBytes: new Uint8Array([5]),
						},
						deleteHashes: ["b"],
					},
				]);
			expect(hashBatchDeleted.map((id) => id.primitive)).to.deep.equal(["b"]);
			expect(encodeCalls).to.equal(0);
		} finally {
			indexInternal.encodeSharedLogCoordinatePersistenceValue = originalEncode;
			await indices.drop();
		}
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

	it("wraps already context-mutated document puts in the index schema", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextualIndex = index as typeof index & {
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
		const document = new BridgeDocument("a", "peerbit", "native index") as
			BridgeDocument & { __context?: BridgeContext };
		const context = new BridgeContext("head-a");
		document.__context = context;

		await contextualIndex.putWithContext(document, toId("a"), context, {
			replace: false,
			encodedValueParts: {
				prefix: serialize(document),
				suffix: serialize(context),
			},
		});

		const indexed = await index
			.iterate({
				query: new StringMatch({ key: "tag", value: "peerbit" }),
			})
			.all();
		expect(indexed.map((entry) => entry.value.__context.head)).to.deep.equal([
			"head-a",
		]);
		expect(indexed.map((entry) => entry.value.title)).to.deep.equal([
			"native index",
		]);

		await indices.drop();
	});

	it("stores contextual encoded document puts without reading JS document fields", async () => {
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
		const encodedDocument = new BridgeDocument("a", "peerbit", "stored bytes");
		const context = new BridgeContext("head-a");
		const unreadableDocument = Object.create(
			BridgeDocument.prototype,
		) as BridgeDocument;
		Object.defineProperties(unreadableDocument, {
			id: { value: "a", enumerable: true },
			tag: {
				get() {
					throw new Error("encoded contextual put should not read tag");
				},
				enumerable: true,
			},
			title: {
				get() {
					throw new Error("encoded contextual put should not read title");
				},
				enumerable: true,
			},
		});

		await contextualIndex.putWithContext(
			unreadableDocument,
			toId("a"),
			context,
			{
				encodedValueParts: {
					prefix: serialize(encodedDocument),
					suffix: serialize(context),
				},
			},
		);

		const result = await index.get(toId("a"));
		expect(result?.value.__context.head).equal("head-a");
		expect(result?.value.title).equal("stored bytes");
		const byHeadId = (
			index as typeof index & {
				getIdByContextHead: (head: string) => ReturnType<typeof toId> | undefined;
			}
		).getIdByContextHead("head-a");
		expect(byHeadId?.primitive).equal("a");

		const indexed = await index
			.iterate({
				query: new StringMatch({ key: "tag", value: "peerbit" }),
			})
			.all();
		expect(indexed.map((entry) => entry.value.title)).to.deep.equal([
			"stored bytes",
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

	it("coalesces native-backbone primary contextual encoded batches", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextualIndex = index as typeof index & {
			attachNativeBackboneDocumentIndex: (backbone: unknown) => boolean;
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
		};
		let batchCalls = 0;
		let singleCalls = 0;
		let batchKeys: string[] = [];
		const backbone = {
			documentIndexLength: 0,
			configureDocumentSchemaIr: () => ({
				rootFields: 0,
				nodeCount: 0,
				genericNodes: 0,
			}),
			setDocumentContextHeadField: () => {},
			setDocumentContextFields: () => {},
			clearDocumentIndex: () => {},
			putDocumentEncodedPartsStored: () => {
				singleCalls++;
			},
			putDocumentEncodedPartsStoredBatch: (
				values: Array<{ key: string }>,
			) => {
				batchCalls++;
				batchKeys = values.map((value) => value.key);
			},
			documentEntry: () => undefined,
			documentQuery: () => [],
			documentQueryPage: () => [],
			documentCount: () => 0,
			documentSum: () => ["none", "0"] as const,
			deleteDocument: () => false,
		};
		expect(contextualIndex.attachNativeBackboneDocumentIndex(backbone)).equal(
			true,
		);

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

		expect(batchCalls).equal(1);
		expect(singleCalls).equal(0);
		expect(batchKeys).to.deep.equal(["string:a", "string:b"]);

		await indices.drop();
	});

	it("coalesces stored contextual encoded batches through the native backbone hook", async () => {
		const indices = create();
		await indices.start();
		const index = await indices.init({ schema: BridgeDocumentWithContext });
		const contextualIndex = index as typeof index & {
			attachNativeBackboneDocumentIndex: (backbone: unknown) => boolean;
			putStoredContextualEncodedValueBatch: (
				values: Array<{
					id: ReturnType<typeof toId>;
					encodedValueParts: { prefix: Uint8Array; suffix: Uint8Array };
				}>,
			) => Promise<boolean>;
		};
		let batchCalls = 0;
		let singleCalls = 0;
		let batchKeys: string[] = [];
		const backbone = {
			documentIndexLength: 0,
			configureDocumentSchemaIr: () => ({
				rootFields: 0,
				nodeCount: 0,
				genericNodes: 0,
			}),
			setDocumentContextHeadField: () => {},
			setDocumentContextFields: () => {},
			clearDocumentIndex: () => {},
			putDocumentEncodedPartsStored: () => {
				singleCalls++;
			},
			putDocumentEncodedPartsStoredBatch: (
				values: Array<{ key: string }>,
			) => {
				batchCalls++;
				batchKeys = values.map((value) => value.key);
			},
			documentEntry: () => undefined,
			documentQuery: () => [],
			documentQueryPage: () => [],
			documentCount: () => 0,
			documentSum: () => ["none", "0"] as const,
			deleteDocument: () => false,
		};
		expect(contextualIndex.attachNativeBackboneDocumentIndex(backbone)).equal(
			true,
		);

		const first = new BridgeDocument("a", "peerbit", "first");
		const second = new BridgeDocument("b", "peerbit", "second");
		const stored = await contextualIndex.putStoredContextualEncodedValueBatch([
			{
				id: toId("a"),
				encodedValueParts: {
					prefix: serialize(first),
					suffix: serialize(new BridgeContext("head-a")),
				},
			},
			{
				id: toId("b"),
				encodedValueParts: {
					prefix: serialize(second),
					suffix: serialize(new BridgeContext("head-b")),
				},
			},
		]);

		expect(stored).equal(true);
		expect(batchCalls).equal(1);
		expect(singleCalls).equal(0);
		expect(batchKeys).to.deep.equal(["string:a", "string:b"]);

		await indices.drop();
	});

	it("keeps a persisted stored batch visible after trailing compaction rejects", async () => {
		const directory = createPersistenceDirectory();
		const indices = create(directory);
		try {
			await indices.start();
			const index = await indices.init({ schema: BridgeDocumentWithContext });
			const contextualIndex = index as typeof index & {
				putStoredContextualEncodedValueBatch: (
					values: Array<{
						id: ReturnType<typeof toId>;
						encodedValueParts: {
							prefix: Uint8Array;
							suffix: Uint8Array;
						};
					}>,
				) => Promise<boolean>;
			};
			await index.put(
				new BridgeDocumentWithContext(
					new BridgeDocument("b", "peerbit", "second"),
					new BridgeContext("head-b"),
				),
			);
			await index.put(
				new BridgeDocumentWithContext(
					new BridgeDocument("d", "peerbit", "fourth"),
					new BridgeContext("head-d"),
				),
			);

			const iterator = index.iterate({
				sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
			});
			const first = (await iterator.next(1)).map((result) => result.value.id);
			expect(first).to.deep.equal(["b"]);

			const internal = index as unknown as {
				compactIfNeeded: () => Promise<void>;
				mutationVersion: number;
			};
			const compactIfNeeded = internal.compactIfNeeded.bind(index);
			const versionBeforeBatch = internal.mutationVersion;
			internal.compactIfNeeded = async () => {
				throw new Error("forced trailing batch compaction failure");
			};
			let rejected = false;
			try {
				await contextualIndex.putStoredContextualEncodedValueBatch([
					{
						id: toId("a"),
						encodedValueParts: {
							prefix: serialize(
								new BridgeDocument("a", "peerbit", "first"),
							),
							suffix: serialize(new BridgeContext("head-a")),
						},
					},
					{
						id: toId("c"),
						encodedValueParts: {
							prefix: serialize(
								new BridgeDocument("c", "peerbit", "third"),
							),
							suffix: serialize(new BridgeContext("head-c")),
						},
					},
				]);
				expect.fail("stored batch should reject after the native mutation");
			} catch (error) {
				rejected = true;
				expect((error as Error).message).to.equal(
					"forced trailing batch compaction failure",
				);
			} finally {
				internal.compactIfNeeded = compactIfNeeded;
			}
			expect(rejected).to.be.true;
			expect(internal.mutationVersion).to.be.greaterThan(versionBeforeBatch);

			const remaining = (await iterator.all()).map(
				(result) => result.value.id,
			);
			expect(remaining).to.deep.equal(["a", "c", "d"]);
			expect(new Set([...first, ...remaining]).size).to.equal(4);
			iterator.close();
		} finally {
			await indices.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("coalesces native-backbone exact deletes with result flags", async () => {
		const directory = createPersistenceDirectory();
		const indices = create(directory);
		try {
			await indices.start();
			const index = await indices.init({ schema: BridgeDocumentWithContext });
			const contextualIndex = index as typeof index & {
				attachNativeBackboneDocumentIndex: (backbone: unknown) => boolean;
				delIds: (
					deleteIds: Array<ReturnType<typeof toId> | string>,
				) => ReturnType<typeof toId>[] | Promise<ReturnType<typeof toId>[]>;
			};
			let existsCalls = 0;
			let batchCalls = 0;
			let fallbackCalls = 0;
			let existsKeys: string[] = [];
			let batchKeys: string[] = [];
			const backbone = {
				documentIndexLength: 0,
				configureDocumentSchemaIr: () => ({
					rootFields: 0,
					nodeCount: 0,
					genericNodes: 0,
				}),
				setDocumentContextHeadField: () => {},
				setDocumentContextFields: () => {},
				clearDocumentIndex: () => {},
				putDocumentEncodedPartsStored: () => {},
				documentEntry: () => {
					fallbackCalls++;
					return undefined;
				},
				documentKeysExist: (keys: string[]) => {
					existsCalls++;
					existsKeys = [...keys];
					return Uint8Array.from([1, 0, 1]);
				},
				documentQuery: () => [],
				documentQueryPage: () => [],
				documentCount: () => 0,
				documentSum: () => ["none", "0"] as const,
				deleteDocument: () => {
					fallbackCalls++;
					return false;
				},
				deleteDocumentsResult: (keys: string[]) => {
					batchCalls++;
					batchKeys = [...keys];
					return Uint8Array.from([1, 0, 1]);
				},
			};
			expect(contextualIndex.attachNativeBackboneDocumentIndex(backbone)).equal(
				true,
			);

			const deleted = await contextualIndex.delIds(["a", "missing", "b"]);

			expect(existsCalls).equal(1);
			expect(batchCalls).equal(1);
			expect(fallbackCalls).equal(0);
			expect(existsKeys).to.deep.equal([
				"string:a",
				"string:missing",
				"string:b",
			]);
			expect(batchKeys).to.deep.equal([
				"string:a",
				"string:missing",
				"string:b",
			]);
			expect(deleted.map((id) => id.primitive)).to.deep.equal(["a", "b"]);
		} finally {
			await indices.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
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

	it("replays durable exact id deletes without returning deleted entries", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			const exactDeleteIndex = writerIndex as typeof writerIndex & {
				delIdsNoReturn: (deleteIds: string[]) => Promise<void> | void;
			};
			await writerIndex.put(new BridgeDocument("a", "stale", "delete me"));
			await writerIndex.put(new BridgeDocument("b", "other", "keep me"));
			const deleted = await exactDeleteIndex.delIdsNoReturn(["a"]);
			expect(deleted).equal(undefined);

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

	it("counts durable exact id deletes without returning deleted entries", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			const exactDeleteIndex = writerIndex as typeof writerIndex & {
				delIdsCount: (deleteIds: string[]) => Promise<number> | number;
			};
			await writerIndex.put(new BridgeDocument("a", "stale", "delete me"));
			await writerIndex.put(new BridgeDocument("b", "other", "keep me"));
			const deleted = await exactDeleteIndex.delIdsCount(["a", "missing"]);
			expect(deleted).equal(1);

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

	it("reuses owned V1 bytes for snapshot and journal winners", async () => {
		const directory = createPersistenceDirectory();
		const snapshotWriter = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		const journalWriter = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		const reader = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		type NativePrototype = {
			put_encoded: (
				this: NativePrototype,
				storeKey: string,
				id: ReturnType<typeof toId>,
				value: CountingBridgeDocument,
				valueBytes: Uint8Array,
				byteElementIndexLimit: number,
			) => void;
			put: (this: NativePrototype, ...args: unknown[]) => void;
		};
		let nativePrototype: NativePrototype | undefined;
		let originalPutEncoded: NativePrototype["put_encoded"] | undefined;
		let originalPut: NativePrototype["put"] | undefined;

		const updatedA = new CountingBridgeDocument("a", "peerbit", "updated a");
		const retainedE = new CountingBridgeDocument("e", "peerbit", "retained e");
		const addedD = new CountingBridgeDocument("d", "peerbit", "added d");
		const reinsertedC = new CountingBridgeDocument(
			"c",
			"peerbit",
			"reinserted c",
		);
		const expected = [updatedA, retainedE, addedD, reinsertedC];
		const expectedBytes = new Map(
			expected.map((value) => [value.id, serialize(value)]),
		);
		const captured: Array<{
			value: CountingBridgeDocument;
			valueBytes: Uint8Array;
		}> = [];
		let fallbackCalls = 0;

		try {
			await snapshotWriter.start();
			const snapshotIndex = await snapshotWriter.init({
				schema: CountingBridgeDocument,
			});
			for (const value of [
				new CountingBridgeDocument("a", "peerbit", "snapshot a"),
				new CountingBridgeDocument("b", "peerbit", "snapshot b"),
				new CountingBridgeDocument("c", "peerbit", "snapshot c"),
				retainedE,
			]) {
				await snapshotIndex.put(value);
			}
			await snapshotWriter.stop();

			await journalWriter.start();
			const journalIndex = await journalWriter.init({
				schema: CountingBridgeDocument,
			});
			expect(
				(
					journalIndex as unknown as {
						nativeSchemaIrStats: { genericNodes: number };
					}
				).nativeSchemaIrStats.genericNodes,
			).to.equal(0);
			await journalIndex.put(updatedA);
			await journalIndex.del({
				query: new StringMatch({ key: "id", value: "b" }),
			});
			await journalIndex.put(addedD);
			await journalIndex.del({
				query: new StringMatch({ key: "id", value: "c" }),
			});
			await journalIndex.put(reinsertedC);

			const native = (journalIndex as unknown as { native: object }).native;
			nativePrototype = Object.getPrototypeOf(native) as NativePrototype;
			originalPutEncoded = nativePrototype.put_encoded;
			originalPut = nativePrototype.put;
			nativePrototype.put_encoded = function (
				storeKey,
				id,
				value,
				valueBytes,
				byteElementIndexLimit,
			) {
				captured.push({ value, valueBytes });
				return originalPutEncoded!.call(
					this,
					storeKey,
					id,
					value,
					valueBytes,
					byteElementIndexLimit,
				);
			};
			nativePrototype.put = function (...args) {
				fallbackCalls++;
				return originalPut!.apply(this, args);
			};
			CountingBridgeDocument.serializeCalls = 0;

			await reader.start();
			const readerIndex = await reader.init({ schema: CountingBridgeDocument });
			expect(CountingBridgeDocument.serializeCalls).to.equal(0);
			const restored = (await readerIndex.iterate().all()).map(
				(entry) => entry.value,
			);

			expect(fallbackCalls).to.equal(0);
			expect(restored.map((value) => value.id)).to.deep.equal([
				"a",
				"e",
				"d",
				"c",
			]);
			expect(restored.map((value) => value.title)).to.deep.equal([
				"updated a",
				"retained e",
				"added d",
				"reinserted c",
			]);
			expect(captured.map((entry) => entry.value.id)).to.deep.equal([
				"a",
				"e",
				"d",
				"c",
			]);
			for (const { value, valueBytes } of captured) {
				expect(valueBytes.byteOffset).to.equal(0);
				expect(valueBytes.buffer.byteLength).to.equal(valueBytes.byteLength);
				expect([...valueBytes]).to.deep.equal([
					...expectedBytes.get(value.id)!,
				]);
			}

			const queried = await readerIndex
				.iterate({
					query: new StringMatch({
						key: "title",
						value: "reinserted c",
					}),
				})
				.all();
			expect(queried.map((entry) => entry.value.id)).to.deep.equal(["c"]);
		} finally {
			if (nativePrototype && originalPutEncoded && originalPut) {
				nativePrototype.put_encoded = originalPutEncoded;
				nativePrototype.put = originalPut;
			}
			await snapshotWriter.drop();
			await journalWriter.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("keeps generic persisted schemas on the field-encoder restore path", async () => {
		const directory = createPersistenceDirectory();
		const writer = create(directory);
		const reader = create(directory);
		type RustPrototype = {
			putNativeDocument: (
				this: RustPrototype,
				storeKey: string,
				id: ReturnType<typeof toId>,
				value: BridgeFloatDocument,
				preparedEncodedValue?: Uint8Array,
			) => void;
		};
		let rustPrototype: RustPrototype | undefined;
		let originalPutNativeDocument:
			| RustPrototype["putNativeDocument"]
			| undefined;
		const preparedValues: Array<Uint8Array | undefined> = [];

		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeFloatDocument });
			await writerIndex.put(new BridgeFloatDocument("a", 1.25));
			await writer.stop();

			rustPrototype = Object.getPrototypeOf(writerIndex) as RustPrototype;
			originalPutNativeDocument = rustPrototype.putNativeDocument;
			rustPrototype.putNativeDocument = function (
				storeKey,
				id,
				value,
				preparedEncodedValue,
			) {
				preparedValues.push(preparedEncodedValue);
				return originalPutNativeDocument!.call(
					this,
					storeKey,
					id,
					value,
					preparedEncodedValue,
				);
			};

			await reader.start();
			const readerIndex = await reader.init({ schema: BridgeFloatDocument });
			const internal = readerIndex as unknown as {
				nativeSchemaIrStats: { genericNodes: number };
			};

			expect(internal.nativeSchemaIrStats.genericNodes).to.be.greaterThan(0);
			expect(preparedValues).to.deep.equal([undefined]);
			expect((await readerIndex.get(toId("a")))?.value.score).to.equal(1.25);
		} finally {
			if (rustPrototype && originalPutNativeDocument) {
				rustPrototype.putNativeDocument = originalPutNativeDocument;
			}
			await writer.drop();
			await reader.drop();
			await removeNodeDirectoryIfNeeded(directory);
		}
	});

	it("reserializes magicless legacy snapshots on restore", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, join, readFile, rm, writeFile } =
			await loadNodePersistenceHelpers();
		const writer = create(directory);
		const reader = create(directory);
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: CountingBridgeDocument });
			await writerIndex.put(
				new CountingBridgeDocument("a", "peerbit", "legacy snapshot"),
			);
			await writer.stop();

			const snapshotPath = join(directory, "id", "index.bin");
			const snapshotBytes = await readFile(snapshotPath);
			await writeFile(snapshotPath, snapshotBytes.subarray(16));
			CountingBridgeDocument.serializeCalls = 0;

			await reader.start();
			const readerIndex = await reader.init({ schema: CountingBridgeDocument });
			expect(CountingBridgeDocument.serializeCalls).to.equal(1);
			expect((await readerIndex.get(toId("a")))?.value.title).to.equal(
				"legacy snapshot",
			);
		} finally {
			await writer.drop();
			await reader.drop();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("aligns V1 journal bytes after a magicless legacy snapshot", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, join, readFile, rm, writeFile } =
			await loadNodePersistenceHelpers();
		const snapshotWriter = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		const journalWriter = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		const reader = create(directory, {
			persistence: { compactAfterOperations: 1000 },
		});
		type RustPrototype = {
			putNativeDocument: (
				this: RustPrototype,
				storeKey: string,
				id: ReturnType<typeof toId>,
				value: CountingBridgeDocument,
				preparedEncodedValue?: Uint8Array,
			) => void;
		};
		let rustPrototype: RustPrototype | undefined;
		let originalPutNativeDocument:
			| RustPrototype["putNativeDocument"]
			| undefined;
		const preparedValues: Array<{
			id: string;
			bytes?: Uint8Array;
		}> = [];

		try {
			await snapshotWriter.start();
			const snapshotIndex = await snapshotWriter.init({
				schema: CountingBridgeDocument,
			});
			await snapshotIndex.put(
				new CountingBridgeDocument("a", "peerbit", "legacy a"),
			);
			await snapshotIndex.put(
				new CountingBridgeDocument("b", "peerbit", "legacy b"),
			);
			await snapshotWriter.stop();

			const snapshotPath = join(directory, "id", "index.bin");
			const snapshotBytes = await readFile(snapshotPath);
			await writeFile(snapshotPath, snapshotBytes.subarray(16));

			await journalWriter.start();
			const journalIndex = await journalWriter.init({
				schema: CountingBridgeDocument,
			});
			const journalValue = new CountingBridgeDocument(
				"c",
				"peerbit",
				"journal c",
			);
			await journalIndex.put(journalValue);
			const expectedJournalBytes = serialize(journalValue);

			rustPrototype = Object.getPrototypeOf(journalIndex) as RustPrototype;
			originalPutNativeDocument = rustPrototype.putNativeDocument;
			rustPrototype.putNativeDocument = function (
				storeKey,
				id,
				value,
				preparedEncodedValue,
			) {
				preparedValues.push({
					id: value.id,
					bytes: preparedEncodedValue,
				});
				return originalPutNativeDocument!.call(
					this,
					storeKey,
					id,
					value,
					preparedEncodedValue,
				);
			};
			CountingBridgeDocument.serializeCalls = 0;

			await reader.start();
			const readerIndex = await reader.init({ schema: CountingBridgeDocument });
			expect(CountingBridgeDocument.serializeCalls).to.equal(2);
			expect(preparedValues.map(({ id }) => id)).to.deep.equal(["a", "b", "c"]);
			expect(preparedValues[0].bytes).to.equal(undefined);
			expect(preparedValues[1].bytes).to.equal(undefined);
			expect([...(preparedValues[2].bytes ?? [])]).to.deep.equal([
				...expectedJournalBytes,
			]);

			const queried = await readerIndex
				.iterate({
					query: new StringMatch({ key: "title", value: "journal c" }),
				})
				.all();
			expect(queried.map((entry) => entry.value.id)).to.deep.equal(["c"]);
		} finally {
			if (rustPrototype && originalPutNativeDocument) {
				rustPrototype.putNativeDocument = originalPutNativeDocument;
			}
			await snapshotWriter.drop();
			await journalWriter.drop();
			await reader.drop();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("reserves native capacity before restoring durable state", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, rm } = await loadNodePersistenceHelpers();
		const writer = create(directory);
		const reader = create(directory);
		type NativePrototype = {
			reserve_documents: (
				this: NativePrototype,
				additional: number,
			) => void;
			len: () => number;
		};
		let prototype: NativePrototype | undefined;
		let originalReserve: NativePrototype["reserve_documents"] | undefined;
		const reservations: number[] = [];
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			await writerIndex.put(new BridgeDocument("a", "peerbit", "first"));
			await writerIndex.put(new BridgeDocument("b", "peerbit", "second"));
			const writerNative = (writerIndex as unknown as { native: object }).native;
			prototype = Object.getPrototypeOf(writerNative) as NativePrototype;
			originalReserve = prototype.reserve_documents;
			await writer.stop();

			prototype.reserve_documents = function (additional) {
				expect(this.len()).to.equal(0);
				reservations.push(additional);
				return originalReserve!.call(this, additional);
			};

			await reader.start();
			const reopened = await reader.init({ schema: BridgeDocument });
			expect(reopened.getSize()).to.equal(2);
			expect((await reopened.get(toId("a")))?.value.title).to.equal("first");
			expect((await reopened.get(toId("b")))?.value.title).to.equal("second");
			expect(reservations).to.deep.equal([2]);
		} finally {
			if (prototype && originalReserve) {
				prototype.reserve_documents = originalReserve;
			}
			await writer.drop();
			await reader.drop();
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("shares and evicts a failed restore so the same indices can retry", async function () {
		if (!isNodeRuntime()) {
			this.skip();
		}
		const { directory, rm } = await loadNodePersistenceHelpers();
		const writer = create(directory);
		const reader = create(directory);
		type NativePrototype = {
			put_encoded: (...args: unknown[]) => unknown;
			put: (...args: unknown[]) => unknown;
		};
		let prototype: NativePrototype | undefined;
		let originalPutEncoded: NativePrototype["put_encoded"] | undefined;
		let originalPut: NativePrototype["put"] | undefined;
		try {
			await writer.start();
			const writerIndex = await writer.init({ schema: BridgeDocument });
			await writerIndex.put(
				new BridgeDocument("a", "peerbit", "retryable restore"),
			);
			const writerNative = (writerIndex as unknown as { native: object }).native;
			prototype = Object.getPrototypeOf(writerNative) as NativePrototype;
			originalPutEncoded = prototype.put_encoded;
			originalPut = prototype.put;
			await writer.stop();

			const restoreFailure = new WebAssembly.RuntimeError(
				"forced native restore runtime failure",
			);
			let encodedCalls = 0;
			let fallbackCalls = 0;
			prototype.put_encoded = () => {
				encodedCalls++;
				throw restoreFailure;
			};
			prototype.put = () => {
				fallbackCalls++;
			};

			await reader.start();
			let rejections: unknown[] = [];
			try {
				const firstAttempt = reader.init({ schema: BridgeDocument });
				const concurrentAttempt = reader.init({ schema: BridgeDocument });
				rejections = await Promise.all([
					captureRejection(() => firstAttempt),
					captureRejection(() => concurrentAttempt),
				]);
			} finally {
				prototype.put_encoded = originalPutEncoded;
				prototype.put = originalPut;
			}

			expect(rejections[0]).to.equal(restoreFailure);
			expect(rejections[1]).to.equal(restoreFailure);
			expect(
				rejections.map((rejection) => (rejection as Error).message),
			).to.deep.equal([
				"forced native restore runtime failure",
				"forced native restore runtime failure",
			]);
			expect(encodedCalls).to.equal(1);
			expect(fallbackCalls).to.equal(0);

			const retried = await reader.init({ schema: BridgeDocument });
			expect((await retried.get(toId("a")))?.value.title).to.equal(
				"retryable restore",
			);
		} finally {
			if (prototype && originalPutEncoded && originalPut) {
				prototype.put_encoded = originalPutEncoded;
				prototype.put = originalPut;
			}
			await writer.drop();
			await reader.drop();
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
