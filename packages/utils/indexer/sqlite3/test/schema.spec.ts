import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import {
	fromRowToObj,
	getInlineTableFieldName,
	getSQLTable,
	getTableName,
} from "../src/schema.js";
import { DocumentNoVariant, DocumentWithVariant } from "./fixtures.js";

describe("schema", () => {
	it("fromRowToObj", () => {
		const obj = { id: 1 };
		const parsed = fromRowToObj(obj, DocumentNoVariant);
		expect(parsed).to.be.instanceOf(DocumentNoVariant);
		expect(parsed.id).to.equal(1);
	});

	describe("table", () => {
		it("throws when no variant", () => {
			expect(() => getTableName(["scope"], DocumentNoVariant)).to.throw(
				"has no variant",
			);
		});

		it("uses variant for table name", () => {
			const table = getTableName(["scope"], DocumentWithVariant);
			expect(table).to.equal("scope__v_0");
		});

		it("uses declared primary key for child FKs", () => {
			abstract class ChildBase {}

			@variant(0)
			class _ChildV0 extends ChildBase {
				@field({ type: "string" })
				value: string;

				constructor(value: string) {
					super();
					this.value = value;
				}
			}

			@variant(1)
			class _ChildV1 extends ChildBase {
				@field({ type: "string" })
				value: string;

				constructor(value: string) {
					super();
					this.value = value;
				}
			}

			@variant("Root")
			class Root {
				// Intentionally declared before the primary key.
				@field({ type: ChildBase })
				child: ChildBase;

				@id({ type: "string" })
				id: string;

				constructor(id: string, child: ChildBase) {
					this.id = id;
					this.child = child;
				}
			}

			const primary = getInlineTableFieldName(["id"]);
			const [rootTable] = getSQLTable(Root, [], primary, false, undefined, false);
			expect(rootTable).to.exist;
			expect(rootTable.children.length).to.equal(2);

			for (const childTable of rootTable.children) {
				expect(childTable.inline).to.equal(false);

				const parentIdField = childTable.fields.find(
					(f) => f.name === "__parent_id",
				);
				expect(parentIdField?.type).to.equal("TEXT");

				const fkConstraint = childTable.constraints.find(
					(c) => c.name === "__parent_id_fk",
				);
				expect(fkConstraint?.definition).to.include(
					`REFERENCES ${rootTable.name}(${primary})`,
				);
			}
		});

		it("uses bytes primary key for child FKs", () => {
			abstract class ChildBase {}

			@variant(0)
			class _ChildV0 extends ChildBase {
				@field({ type: "string" })
				value: string;

				constructor(value: string) {
					super();
					this.value = value;
				}
			}

			@variant("Root")
			class Root {
				// Intentionally declared before the primary key.
				@field({ type: ChildBase })
				child: ChildBase;

				@id({ type: fixedArray("u8", 32) })
				id: Uint8Array;

				constructor(id: Uint8Array, child: ChildBase) {
					this.id = id;
					this.child = child;
				}
			}

			const primary = getInlineTableFieldName(["id"]);
			const [rootTable] = getSQLTable(Root, [], primary, false, undefined, false);
			expect(rootTable).to.exist;
			expect(rootTable.children.length).to.equal(1);

			const [childTable] = rootTable.children;
			expect(childTable.inline).to.equal(false);

			const parentIdField = childTable.fields.find((f) => f.name === "__parent_id");
			expect(parentIdField?.type).to.equal("BLOB");

			const fkConstraint = childTable.constraints.find(
				(c) => c.name === "__parent_id_fk",
			);
			expect(fkConstraint?.definition).to.include(
				`REFERENCES ${rootTable.name}(${primary})`,
			);
		});
	});
});
