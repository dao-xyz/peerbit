import { field } from "@dao-xyz/borsh";
import { fromRowToObj } from "../src/schema.js";
import { expect } from "chai";

describe("schema", () => {
	class Document {
		@field({ type: "u8" })
		id: number;

		constructor(obj: any) { }
	}

	it("fromRowToObj", () => {
		const obj = { id: 1 };
		const parsed = fromRowToObj(obj, Document);
		expect(parsed).to.be.instanceOf(Document);
		expect(parsed.id).to.equal(1);
	});
});
