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
		it("uses class for table name when no variant", () => {
			const table = getTableName(["scope"], DocumentNoVariant);
			expect(table).to.equal("scope__class_DocumentNoVariant");
		});

		it("uses variant for table name", () => {
			const table = getTableName(["scope"], DocumentWithVariant);
			expect(table).to.equal("scope__v_0");
		});
	});
});
