import { field, option, variant, vec } from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import {
	/* 
Compare,
IntegerCompare,
Or, */
	StringMatch,
	id,
} from "@peerbit/indexer-interface";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

describe("simple array", () => {
	// u64 is a special case since we need to shift values to fit into signed 64 bit integers

	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	abstract class ArrayDocumentBase {}

	/* @variant(0)
	class ArrayDocument extends ArrayDocumentBase {
		@id({ type: "u64" })
		id: bigint;

		@field({ type: vec("u64") })
		value: bigint[];

		constructor(id: bigint, value: bigint[]) {
			super();
			this.id = id;
			this.value = value;
		}
	}
 */
	@variant(1)
	// @ts-ignore
	class _AnotherArrayDocument extends ArrayDocumentBase {
		@id({ type: "u64" })
		id: bigint;

		@field({ type: vec("u64") })
		anotherValue: bigint[];

		constructor(id: bigint, value: bigint[]) {
			super();
			this.id = id;
			this.anotherValue = value;
		}
	}

	@variant(0)
	class ArrayDocumentSingle {
		@id({ type: "u64" })
		id: bigint;

		@field({ type: vec("u64") })
		value: bigint[];

		constructor(id: bigint, value: bigint[]) {
			this.id = id;
			this.value = value;
		}
	}

	@variant(0)
	class BlobArrayDocument {
		@id({ type: "u64" })
		id: bigint;

		@field({ type: vec(Uint8Array) })
		value: Uint8Array[];

		constructor(id: bigint, value: Uint8Array[]) {
			this.id = id;
			this.value = value;
		}
	}

	/* it("query inner items does not take too long time", async () => {
		index = await setup({ schema: ArrayDocumentBase }, create);
		const store = index.store as SQLLiteIndex<ArrayDocument>;
		expect(store.tables.size).to.equal(4);
		let count = 1000;
		let itemsToQuery: bigint[] = [];
		for (let i = 0; i < count; i++) {
			let offset = BigInt(i) * 3n;
			if (itemsToQuery.length < 30) {
				itemsToQuery.push(offset);
			}
			await index.store.put(
				new ArrayDocument(BigInt(i), [offset + 0n, offset + 1n, offset + 2n]),
			);
		}

		const t1 = +new Date();
		const out = await index.store.iterate({}).all();
		const t2 = +new Date();
		expect(out.length).to.equal(count);

		const t3 = +new Date();
		let compares: IntegerCompare[] = itemsToQuery.map(
			(x) =>
				new IntegerCompare({ key: "value", value: x, compare: Compare.Equal }),
		);
		const out2 = await index.store.iterate({ query: new Or(compares) }).all();
		const t4 = +new Date();

		expect(t4 - t3).to.lessThan(t2 - t1);
		expect(out2.length).to.equal(itemsToQuery.length);
	});

	it("poly-morphic base resolving many items is sufficiently fast", async () => {
		index = await setup({ schema: ArrayDocumentBase }, create);
		const store = index.store as SQLLiteIndex<ArrayDocument>;
		expect(store.tables.size).to.equal(4);
		let count = 1e4;
		for (let i = 0; i < count; i++) {
			await index.store.put(
				new ArrayDocument(BigInt(i), [
					BigInt(Math.round(Math.random() * Number.MAX_SAFE_INTEGER)),
					BigInt(Math.round(Math.random() * Number.MAX_SAFE_INTEGER)),
					BigInt(Math.round(Math.random() * Number.MAX_SAFE_INTEGER)),
				]),
			);
		}
		const t1 = +new Date();
		const out = await index.store.iterate({}).all();
		const t2 = +new Date();

		console.log(`Time to resolve ${count} items: ${t2 - t1} ms`);
		expect(out.length).to.equal(count);
		expect(t2 - t1).to.lessThan(1000);
	}); */

	it("simple-base resolving many items is sufficiently fast", async () => {
		index = await setup({ schema: ArrayDocumentSingle }, create);
		let count = 1e4;
		for (let i = 0; i < count; i++) {
			await index.store.put(
				new ArrayDocumentSingle(BigInt(i), [
					BigInt(Math.round(Math.random() * Number.MAX_SAFE_INTEGER)),
					BigInt(Math.round(Math.random() * Number.MAX_SAFE_INTEGER)),
					BigInt(Math.round(Math.random() * Number.MAX_SAFE_INTEGER)),
				]),
			);
		}
		const t1 = +new Date();
		const out = await index.store.iterate({}).all();
		const t2 = +new Date();

			console.log(`Time to resolve ${count} items: ${t2 - t1} ms`);
			expect(out.length).to.equal(count);
			// This is a coarse regression guard, not a strict benchmark.
			// These tests may run alongside other heavy suites, so keep a conservative threshold.
			expect(t2 - t1).to.lessThan(15_000);
		});

	it("blob array items is sufficiently fast", async () => {
		index = await setup({ schema: BlobArrayDocument }, create);
		let count = 1e4;
		for (let i = 0; i < count; i++) {
			await index.store.put(
				new BlobArrayDocument(BigInt(i), [
					randomBytes(32),
					randomBytes(32),
					randomBytes(32),
				]),
			);
		}
		const t1 = +new Date();
		const out = await index.store.iterate({}).all();
		const t2 = +new Date();

			console.log(`Time to resolve ${count} items: ${t2 - t1} ms`);
			expect(out.length).to.equal(count);
			// This is a coarse regression guard, not a strict benchmark.
			// These tests may run alongside other heavy suites, so keep a conservative threshold.
			expect(t2 - t1).to.lessThan(15_000);
		});
	});

describe("document array", () => {
	// u64 is a special case since we need to shift values to fit into signed 64 bit integers

	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	abstract class Base {}

	@variant("av0")
	class AV0 extends Base {
		@field({ type: option("u64") })
		number?: bigint;

		constructor(opts: AV0) {
			super();
			this.number = opts.number;
		}
	}

	@variant("av1")
	class AV1 extends Base {
		@field({ type: option("string") })
		string?: string;

		constructor(opts: AV1) {
			super();
			this.string = opts.string;
		}
	}

	@variant("PolymorpArrayDocument")
	class PolymorpArrayDocument {
		@id({ type: "string" })
		id: string;

		@field({ type: vec(Base) })
		array: Base[];

		constructor(opts: PolymorpArrayDocument) {
			this.id = opts.id;
			this.array = opts.array;
		}
	}

	beforeEach(async () => {
		index = await setup({ schema: PolymorpArrayDocument }, create);
	});

	it("can query multiple versions at once", async () => {
		const store = index.store as SQLLiteIndex<PolymorpArrayDocument>;
		await store.put(
			new PolymorpArrayDocument({
				id: "1",
				array: [
					new AV0({
						number: 0n,
					}),
					new AV1({
						string: "hello",
					}),
				],
			}),
		);

		const doc2 = new PolymorpArrayDocument({
			id: "2",
			array: [
				new AV1({
					string: "world",
				}),
				new AV0({
					number: 123n,
				}),
			],
		});

		await store.put(doc2);

		const response = await store
			.iterate({
				query: [
					new StringMatch({
						key: ["array", "string"],
						value: "world",
					}),
				],
			})
			.all();

		expect(response).to.have.length(1);
		expect(response[0].value.id).to.equal("2");
		expect(response[0].value.array).to.have.length(2);
		expect(response[0].value.array[0]).to.be.instanceOf(AV1);
		expect(response[0].value.array[1]).to.be.instanceOf(AV0);
		expect((response[0].value.array[0] as AV1).string).to.equal("world");
		expect((response[0].value.array[1] as AV0).number).to.equal(123n);
	});

	it("all", async () => {
		const store = index.store as SQLLiteIndex<PolymorpArrayDocument>;
		await store.put(
			new PolymorpArrayDocument({
				id: "1",
				array: [
					new AV0({
						number: 0n,
					}),
					new AV1({
						string: "hello",
					}),
				],
			}),
		);

		const doc2 = new PolymorpArrayDocument({
			id: "2",
			array: [
				new AV1({
					string: "world",
				}),
				new AV0({
					number: 123n,
				}),
			],
		});

		await store.put(doc2);

		const response = await store.iterate({}).all();
		expect(response).to.have.length(2);
	});
});
