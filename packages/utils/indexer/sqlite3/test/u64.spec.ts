import { field } from "@dao-xyz/borsh";
import { type IndexedResults, id } from "@peerbit/indexer-interface";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

describe("u64", () => {
	// u64 is a special case since we need to shift values to fit into signed 64 bit integers

	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	class DocumentWithBigint {
		@id({ type: "u64" })
		id: bigint;

		@field({ type: "u64" })
		value: bigint;

		constructor(id: bigint, value: bigint) {
			this.id = id;
			this.value = value;
		}
	}

	it("all", async () => {
		index = await setup({ schema: DocumentWithBigint }, create);
		await index.store.put(new DocumentWithBigint(0n, 0n));
		await index.store.put(
			new DocumentWithBigint(18446744073709551615n, 18446744073709551615n),
		);
		await index.store.put(new DocumentWithBigint(123n, 123n));

		const all: IndexedResults<DocumentWithBigint> = await index.store
			.iterate()
			.all();
		expect(all.length).to.equal(3);
	});

	it("fetch bounds ", async () => {
		index = await setup({ schema: DocumentWithBigint }, create);
		const store = index.store as SQLLiteIndex<DocumentWithBigint>;
		expect(store.tables.size).to.equal(1);
		await index.store.put(new DocumentWithBigint(0n, 0n));
		await index.store.put(
			new DocumentWithBigint(18446744073709551615n, 18446744073709551615n),
		);
		await index.store.put(new DocumentWithBigint(123n, 123n));

		const checkValue = async (value: bigint) => {
			const max: IndexedResults<DocumentWithBigint> = await index.store
				.iterate({ query: { value: value } })
				.all();
			expect(max.length).to.equal(1);
			expect(max[0].id.primitive).to.equal(value);
			expect(max[0].value.id).to.equal(value);
			expect(max[0].value.value).to.equal(value);
		};

		await checkValue(0n);
		await checkValue(18446744073709551615n);
		await checkValue(123n);
	});

	it("summing not supported", async () => {
		index = await setup({ schema: DocumentWithBigint }, create);
		const store = index.store as SQLLiteIndex<DocumentWithBigint>;
		await expect(store.sum({ key: "value" })).eventually.rejectedWith(
			"Summing is not supported for u64 fields",
		);
	});
});
