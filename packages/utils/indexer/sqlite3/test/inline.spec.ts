/* eslint-disable @typescript-eslint/no-unused-vars */
import { field, variant } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

describe("inline", () => {
	describe("nested", () => {
		let index: Awaited<ReturnType<typeof setup<any>>>;

		afterEach(async () => {
			await index.store.stop();
		});

		// TODO what is expected? if we do this, we can not migrate, on the other hand we get performance benefits
		it("inline if only variant", async () => {
			@variant(0)
			class MultifieldNested {
				@field({ type: "bool" })
				bool: boolean;

				@field({ type: "u32" })
				number: number;

				constructor(bool: boolean, number: number) {
					this.bool = bool;
					this.number = number;
				}
			}

			@variant("NestedBoolQueryDocument")
			class NestedBoolQueryDocument {
				@id({ type: "string" })
				id: string;

				@field({ type: MultifieldNested })
				nested: MultifieldNested;

				constructor(id: string, nested: MultifieldNested) {
					this.id = id;
					this.nested = nested;
				}
			}

			index = await setup({ schema: NestedBoolQueryDocument }, create);
			const store = index.store as SQLLiteIndex<NestedBoolQueryDocument>;
			expect(store.tables.size).to.equal(2);
			expect(store.rootTables).to.have.length(1);
			const first = store.rootTables[0];
			expect(first.inline).to.be.false;
			expect(first.children).to.have.length(1);
			const nested = first.children[0];
			expect(nested.inline).to.be.true;
		});

		it("separated if multipel variants", async () => {
			abstract class Base {}

			@variant(0)
			class MultifieldNestedV0 extends Base {
				@field({ type: "bool" })
				bool: boolean;

				@field({ type: "u32" })
				number: number;

				constructor(bool: boolean, number: number) {
					super();
					this.bool = bool;
					this.number = number;
				}
			}

			@variant(1)
			class MultifieldNestedV1 extends Base {
				@field({ type: "bool" })
				bool: boolean;

				@field({ type: "u32" })
				number: number;

				constructor(bool: boolean, number: number) {
					super();
					this.bool = bool;
					this.number = number;
				}
			}

			@variant("NestedBoolQueryDocument")
			class NestedBoolQueryDocument {
				@id({ type: "string" })
				id: string;

				@field({ type: Base })
				nested: Base;

				constructor(id: string, nested: Base) {
					this.id = id;
					this.nested = nested;
				}
			}

			index = await setup({ schema: NestedBoolQueryDocument }, create);
			const store = index.store as SQLLiteIndex<NestedBoolQueryDocument>;
			expect(store.tables.size).to.equal(3);
			expect(store.rootTables).to.have.length(1);
			const first = store.rootTables[0];
			expect(first.inline).to.be.false;
			expect(first.children).to.have.length(2);
			const nested = first.children[0];
			expect(nested.inline).to.be.false;
			const nested2 = first.children[1];
			expect(nested2.inline).to.be.false;
		});
	});
});
