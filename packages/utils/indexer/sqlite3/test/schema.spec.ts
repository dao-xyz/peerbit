import { expect } from "chai";
import { fromRowToObj, getTableName } from "../src/schema.js";
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
	});
});
