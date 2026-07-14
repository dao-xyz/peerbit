/* eslint-disable @typescript-eslint/no-unused-vars */
import { field, option, variant, vec } from "@dao-xyz/borsh";
import {
	BoolQuery,
	Compare,
	IntegerCompare,
	Or,
	Sort,
	SortDirection,
	StringMatch,
	id,
} from "@peerbit/indexer-interface";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { v4 as uuid } from "uuid";
import { SQLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

// Keep the projection path warm and repeatedly exercised without turning the
// correctness suite into a machine-dependent benchmark. A generous absolute
// budget still catches pathological query/decoding regressions, while the
// bounded sample count keeps all three cases well below Mocha's suite timeout.
const SHAPE_SAMPLE_RUNS = 6;
const SHAPE_SAMPLE_MAX_MS = 15_000;
const SHAPE_TEST_TIMEOUT_MS = 30_000;
const MAX_SHAPED_SLOWDOWN = 5;
const SHAPED_NOISE_ALLOWANCE_MS = 1_000;
const now = () => globalThis.performance?.now?.() ?? Date.now();

const runBoundedProjectionSamples = async (
	label: string,
	unshaped: () => Promise<string[]>,
	shaped: () => Promise<string[]>,
) => {
	const sample = async (
		shapedFirst: boolean,
		totals?: { unshaped: number; shaped: number },
	) => {
		const run = async (
			sampleKind: "unshaped" | "shaped",
			query: () => Promise<string[]>,
		) => {
			const started = now();
			const ids = await query();
			if (totals) {
				totals[sampleKind] += now() - started;
			}
			return ids;
		};

		let fullIds: string[];
		let projectedIds: string[];
		if (shapedFirst) {
			projectedIds = await run("shaped", shaped);
			fullIds = await run("unshaped", unshaped);
		} else {
			fullIds = await run("unshaped", unshaped);
			projectedIds = await run("shaped", shaped);
		}
		expect(projectedIds).to.deep.equal(fullIds);
	};

	// Prepare statements and verify the same contract once before timing.
	await sample(false);

	// Alternate which path runs first so cache warmth and scheduler pauses are not
	// consistently charged to one side of the comparison.
	const totals = { unshaped: 0, shaped: 0 };
	for (let i = 0; i < SHAPE_SAMPLE_RUNS; i++) {
		await sample(i % 2 === 1, totals);
	}
	const { unshaped: unshapedMs, shaped: shapedMs } = totals;
	const elapsed = unshapedMs + shapedMs;
	expect(elapsed, `${label} ${SHAPE_SAMPLE_RUNS}-sample budget`).to.be.lessThan(
		SHAPE_SAMPLE_MAX_MS,
	);
	expect(shapedMs, `${label} projected-query regression`).to.be.at.most(
		unshapedMs * MAX_SHAPED_SLOWDOWN + SHAPED_NOISE_ALLOWANCE_MS,
	);
};

@variant(0)
class ArrayDocument /*  extends ArrayDocumentBase */ {
	@id({ type: "string" })
	id: string;

	// create some extra fields to make the ndex more complicated
	@field({ type: "string" })
	a: string;

	// create some extra fields to make the index more complicated
	@field({ type: "string" })
	b: string;

	@field({ type: vec("u32") })
	value: number[];

	constructor(id: string, value: bigint[]) {
		/*   super(); */
		this.id = id;
		this.value = value.map((x) => Number(x));
		this.a = uuid();
		this.b = uuid();
	}
}

@variant("DocumentWithProperties")
class DocumentWithProperties {
	// create some extra fields to make the ndex more complicated
	@field({ type: "string" })
	a: string;

	// create some extra fields to make the index more complicated
	@field({ type: "string" })
	b: string;

	// we will query this field
	@field({ type: "bool" })
	bool: boolean;

	// create some extra fields to make the ndex more complicated
	@field({ type: "string" })
	c: string;

	// create some extra fields to make the index more complicated
	@field({ type: "string" })
	d: string;

	constructor(properties?: {
		bool?: boolean;
		a?: string;
		b?: string;
		c?: string;
		d?: string;
	}) {
		this.bool = properties?.bool ?? Math.random() > 0.5;
		this.a = properties?.a ?? uuid();
		this.b = properties?.b ?? uuid();
		this.c = properties?.c ?? uuid();
		this.d = properties?.d ?? uuid();
	}
}

abstract class Base {}

@variant(0)
// @ts-ignore
class Type0 extends Base {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	value: string;

	constructor(id: string, value: string) {
		super();
		this.id = id;
		this.value = value;
	}
}

@variant(1)
class NestedBoolQueryDocument extends Base {
	@id({ type: "string" })
	id: string;

	@field({ type: vec(DocumentWithProperties) })
	nested: DocumentWithProperties[];

	constructor(id: string, nested: DocumentWithProperties[]) {
		super();
		this.id = id;
		this.nested = nested;
	}
}

describe("shape", () => {
	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index?.store.stop();
	});

	it("shaped sort with when query is split", async () => {
		index = await setup(
			{ schema: DocumentWithProperties, indexBy: ["a"] },
			create,
		);
		index.store as SQLiteIndex<DocumentWithProperties>;
		await index.store.put(new DocumentWithProperties({ a: "1" }));
		await index.store.put(new DocumentWithProperties({ a: "2" }));

		const iterator = index.store.iterate(
			{
				query: new Or([
					new StringMatch({ key: "a", value: "1" }),
					new StringMatch({ key: "a", value: "2" }),
				]),
				sort: new Sort({ key: "b", direction: SortDirection.ASC }),
			},
			{ shape: { id: true } },
		);
		expect(await iterator.all()).to.have.length(2);
	});

	describe("simple array", () => {
		/*   abstract class ArrayDocumentBase { } */

		it("returns equivalent bounded shape samples", async () => {
			index = await setup({ schema: ArrayDocument }, create);
			index.store as SQLiteIndex<ArrayDocument>;
			const count = 256;
			const itemsToQuery: bigint[] = [];
			for (let i = 0; i < count; i++) {
				const offset = BigInt(i) * 3n;
				if (itemsToQuery.length < 3) {
					itemsToQuery.push(offset);
				}
				await index.store.put(
					new ArrayDocument(`array-${i}`, [
						offset + 0n,
						offset + 1n,
						offset + 2n,
					]),
				);
			}

			const compares: IntegerCompare[] = itemsToQuery.map(
				(x) =>
					new IntegerCompare({
						key: "value",
						value: x,
						compare: Compare.Equal,
					}),
			);

			const fetch = 30;
			await runBoundedProjectionSamples(
				"simple array shape",
				async () => {
					const iterator = index.store.iterate({ query: new Or(compares) });
					try {
						const results = await iterator.next(fetch);
						expect(results).to.have.length(itemsToQuery.length);
						for (const item of results) {
							expect(item.value.value).to.have.length(3);
						}
						return results.map((item) => String(item.id.primitive));
					} finally {
						await iterator.close();
					}
				},
				async () => {
					const iterator = index.store.iterate(
						{ query: new Or(compares) },
						{ shape: { id: true } },
					);
					try {
						const results = await iterator.next(fetch);
						expect(results).to.have.length(itemsToQuery.length);
						for (const item of results) {
							expect(item.value).to.have.all.keys("id");
							expect(item.value.id).to.equal(item.id.primitive);
						}
						return results.map((item) => String(item.id.primitive));
					} finally {
						await iterator.close();
					}
				},
			);
		}).timeout(SHAPE_TEST_TIMEOUT_MS);
	});

	describe("document array", () => {
		it("returns equivalent bounded shape samples", async () => {
			index = await setup({ schema: Base }, create);
			index.store as SQLiteIndex<Base>;
			const count = 128;
			for (let i = 0; i < count; i++) {
				if (i % 5 === 0) {
					await index.store.put(
						new NestedBoolQueryDocument(`document-array-${i}`, []),
					);
				} else {
					await index.store.put(
						new NestedBoolQueryDocument(`document-array-${i}`, [
							new DocumentWithProperties({ bool: i % 2 === 0 ? true : false }),
						]),
					);
				}
			}
			const fetch = 30;
			const query = () =>
				new BoolQuery({ key: ["nested", "bool"], value: true });

			await runBoundedProjectionSamples(
				"document array shape",
				async () => {
					const iterator = index.store.iterate({ query: query() });
					try {
						const results = await iterator.next(fetch);
						expect(results).to.have.length(fetch);
						for (const item of results) {
							expect(item.value.nested[0].bool).to.equal(true);
						}
						return results.map((item) => String(item.id.primitive));
					} finally {
						await iterator.close();
					}
				},
				async () => {
					const iterator = index.store.iterate(
						{ query: query() },
						{ shape: { id: true } },
					);
					try {
						const results = await iterator.next(fetch);
						expect(results).to.have.length(fetch);
						for (const item of results) {
							expect(item.value).to.have.all.keys("id");
							expect(item.value.id).to.equal(item.id.primitive);
						}
						return results.map((item) => String(item.id.primitive));
					} finally {
						await iterator.close();
					}
				},
			);
		}).timeout(SHAPE_TEST_TIMEOUT_MS);
	});

	describe("nested document", () => {
		// u64 is a special case since we need to shift values to fit into signed 64 bit integers

		let index: Awaited<ReturnType<typeof setup<any>>>;

		afterEach(async () => {
			await index?.store.stop();
		});

		@variant("Nested_shape_nested_document")
		class Nested {
			// create some extra fields to make the ndex more complicated
			@field({ type: "string" })
			a: string;

			// create some extra fields to make the index more complicated
			@field({ type: "string" })
			b: string;

			// we will query this field
			@field({ type: "bool" })
			bool: boolean;

			// create some extra fields to make the ndex more complicated
			@field({ type: "string" })
			c: string;

			// create some extra fields to make the index more complicated
			@field({ type: "string" })
			d: string;

			constructor(bool: boolean) {
				this.bool = bool;
				this.a = uuid();
				this.b = uuid();
				this.c = uuid();
				this.d = uuid();
			}
		}

		abstract class Base {}

		@variant(0)
		// @ts-ignore
		class Type0 extends Base {
			@id({ type: "string" })
			id: string;

			@field({ type: "string" })
			value: string;

			constructor(id: string, value: string) {
				super();
				this.id = id;
				this.value = value;
			}
		}

		@variant(1)
		class NestedBoolQueryDocument extends Base {
			@id({ type: "string" })
			id: string;

			@field({ type: option(Nested) })
			nested?: Nested;

			constructor(id: string, nested?: Nested) {
				super();
				this.id = id;
				this.nested = nested;
			}
		}

		it("returns equivalent bounded shape samples", async () => {
			index = await setup({ schema: NestedBoolQueryDocument }, create);
			index.store as SQLiteIndex<NestedBoolQueryDocument>;
			const count = 128;
			for (let i = 0; i < count; i++) {
				if (i % 5 === 0) {
					await index.store.put(
						new NestedBoolQueryDocument(`nested-document-${i}`),
					);
				} else {
					await index.store.put(
						new NestedBoolQueryDocument(
							`nested-document-${i}`,
							new Nested(i % 2 === 0 ? true : false),
						),
					);
				}
			}
			const fetch = 30;
			const query = () =>
				new BoolQuery({ key: ["nested", "bool"], value: true });

			await runBoundedProjectionSamples(
				"nested document shape",
				async () => {
					const iterator = index.store.iterate({ query: query() });
					try {
						const results = await iterator.next(fetch);
						expect(results).to.have.length(fetch);
						for (const item of results) {
							expect(item.value.nested?.bool).to.equal(true);
						}
						return results.map((item) => String(item.id.primitive));
					} finally {
						await iterator.close();
					}
				},
				async () => {
					const iterator = index.store.iterate(
						{ query: query() },
						{ shape: { id: true } },
					);
					try {
						const results = await iterator.next(fetch);
						expect(results).to.have.length(fetch);
						for (const item of results) {
							expect(item.value).to.have.all.keys("id");
							expect(item.value.id).to.equal(item.id.primitive);
						}
						return results.map((item) => String(item.id.primitive));
					} finally {
						await iterator.close();
					}
				},
			);
		}).timeout(SHAPE_TEST_TIMEOUT_MS);
	});
});
