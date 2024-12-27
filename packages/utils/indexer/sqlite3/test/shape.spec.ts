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
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

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
		await index.store.stop();
	});

	it("shaped sort with when query is split", async () => {
		index = await setup(
			{ schema: DocumentWithProperties, indexBy: ["a"] },
			create,
		);
		index.store as SQLLiteIndex<DocumentWithProperties>;
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

		it("shaped queries are faster", async () => {
			index = await setup({ schema: ArrayDocument }, create);
			index.store as SQLLiteIndex<ArrayDocument>;
			let count = 5e4;
			let itemsToQuery: bigint[] = [];
			for (let i = 0; i < count; i++) {
				let offset = BigInt(i) * 3n;
				if (itemsToQuery.length < 1) {
					itemsToQuery.push(offset);
				}
				await index.store.put(
					new ArrayDocument(uuid(), [offset + 0n, offset + 1n, offset + 2n]),
				);
			}

			const queryCount = 1e4;

			const compares: IntegerCompare[] = itemsToQuery.map(
				(x) =>
					new IntegerCompare({
						key: "value",
						value: x,
						compare: Compare.Equal,
					}),
			);

			const iterator = index.store.iterate(
				{ query: new Or(compares) },
				{ shape: { id: true } },
			);
			await iterator.next(1);
			await iterator.close();

			const iterator2 = index.store.iterate({ query: new Or(compares) });
			await iterator2.next(1);
			await iterator2.close();

			const t1 = +new Date();

			let fetch = 30;
			for (let i = 0; i < queryCount; i++) {
				const iterator = index.store.iterate(
					{ query: new Or(compares) },
					{ shape: { id: true } },
				);
				/* const out1 = */ await iterator.next(fetch);
				await iterator.close();

				/*   if (out1.length !== itemsToQuery.length) {
					  throw new Error("Expected " + itemsToQuery.length + " but got " + out1.length);
				  } */
			}

			const t2 = +new Date();

			const t3 = +new Date();
			for (let i = 0; i < queryCount; i++) {
				const iterator = index.store.iterate({ query: new Or(compares) });
				const out2 = await iterator.next(fetch);
				await iterator.close();
				if (out2.length !== itemsToQuery.length) {
					throw new Error(
						"Expected " + itemsToQuery.length + " but got " + out2.length,
					);
				}
			}

			const t4 = +new Date();

			console.log(t4 - t3, t2 - t1);
			expect(t4 - t3).to.greaterThan(t2 - t1);
		});
	});

	describe("document array", () => {
		it("shaped queries are faster", async () => {
			index = await setup({ schema: Base }, create);
			index.store as SQLLiteIndex<Base>;
			let count = 1e4;
			for (let i = 0; i < count; i++) {
				if (i % 5 === 0) {
					await index.store.put(new NestedBoolQueryDocument(uuid(), []));
				} else {
					await index.store.put(
						new NestedBoolQueryDocument(uuid(), [
							new DocumentWithProperties({ bool: i % 2 === 0 ? true : false }),
						]),
					);
				}
			}
			const fetch = 30;
			const queryCount = 1e4;

			let iterator = index.store.iterate({
				query: new BoolQuery({ key: ["nested", "bool"], value: true }),
			});
			await iterator.next(1);
			await iterator.close();

			let iteratorShaped = index.store.iterate(
				{
					query: new BoolQuery({ key: ["nested", "bool"], value: true }),
				},
				{
					shape: { id: true },
				},
			);
			await iteratorShaped.next(1);
			await iteratorShaped.close();

			const t1 = +new Date();
			let allResults = [];

			for (let i = 0; i < queryCount; i++) {
				let iterator = index.store.iterate({
					query: new BoolQuery({ key: ["nested", "bool"], value: true }),
				});
				const result = await iterator.next(fetch);
				if (result.length !== fetch) {
					throw new Error(
						"Expected to fetch " + fetch + " but got " + result.length,
					);
				}
				for (const item of result) {
					if (item.value.nested[0].bool !== true) {
						throw new Error("Expected to fetch only true values");
					}
				}
				for (const item of result) {
					allResults.push(item);
				}
				await iterator.close();
			}

			const t2 = +new Date();
			const t3 = +new Date();

			let c = 0;
			for (let i = 0; i < queryCount; i++) {
				let iteratorShaped = index.store.iterate(
					{
						query: new BoolQuery({ key: ["nested", "bool"], value: true }),
					},
					{
						shape: { id: true },
					},
				);
				const result = await iteratorShaped.next(fetch);
				if (result.length !== fetch) {
					throw new Error(
						"Expected to fetch " + fetch + " but got " + result.length,
					);
				}

				for (const item of result) {
					if (item.id.primitive !== allResults[c].id.primitive) {
						throw new Error(
							"Mismatch: " +
								item.id.primitive +
								" !== " +
								allResults[c].id.primitive,
						);
					}
					c++;
				}
				await iteratorShaped.close();
			}

			const t4 = +new Date();
			expect(t4 - t3).to.lessThan(t2 - t1);
			console.log(t4 - t3, t2 - t1);
			expect(allResults.length).to.equal(queryCount * fetch);
		});
	});

	describe("nested document", () => {
		// u64 is a special case since we need to shift values to fit into signed 64 bit integers

		let index: Awaited<ReturnType<typeof setup<any>>>;

		afterEach(async () => {
			await index.store.stop();
		});

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

		it("shaped queries are faster", async () => {
			index = await setup({ schema: NestedBoolQueryDocument }, create);
			index.store as SQLLiteIndex<NestedBoolQueryDocument>;
			let count = 1e4;
			for (let i = 0; i < count; i++) {
				if (i % 5 === 0) {
					await index.store.put(new NestedBoolQueryDocument(uuid()));
				} else {
					await index.store.put(
						new NestedBoolQueryDocument(
							uuid(),
							new Nested(i % 2 === 0 ? true : false),
						),
					);
				}
			}
			const fetch = 30;
			const queryCount = 1e4;
			const t1 = +new Date();
			let allResults = [];
			for (let i = 0; i < queryCount; i++) {
				let iterator = index.store.iterate({
					query: new BoolQuery({ key: ["nested", "bool"], value: true }),
				});
				const result = await iterator.next(fetch);
				if (result.length !== fetch) {
					throw new Error(
						"Expected to fetch " + fetch + " but got " + result.length,
					);
				}
				for (const item of result) {
					if (item.value.nested.bool !== true) {
						throw new Error("Expected to fetch only true values");
					}
				}
				for (const item of result) {
					allResults.push(item);
				}
				await iterator.close();
			}

			const t2 = +new Date();

			const t3 = +new Date();
			/*  let c = 0; */
			for (let i = 0; i < queryCount; i++) {
				let iteratorShaped = index.store.iterate(
					{
						query: new BoolQuery({ key: ["nested", "bool"], value: true }),
					},
					{
						shape: { id: true },
					},
				);
				const result = await iteratorShaped.next(fetch);
				if (result.length !== fetch) {
					throw new Error(
						"Expected to fetch " + fetch + " but got " + result.length,
					);
				}

				/*  for (const item of result) {
					 if (item.id.primitive !== allResults[c].id.primitive) {
						 throw new Error(
							 "Mismatch: " +
							 item.id.primitive +
							 " !== " +
							 allResults[c].id.primitive,
						 );
					 }
					 c++;
				 } */

				await iteratorShaped.close();
			}

			const t4 = +new Date();
			expect(t4 - t3).to.lessThan(t2 - t1);
			console.log(t4 - t3, t2 - t1);
			expect(allResults.length).to.equal(queryCount * fetch);
		});
	});
});
