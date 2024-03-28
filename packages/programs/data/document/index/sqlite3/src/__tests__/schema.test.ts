import { field } from "@dao-xyz/borsh";
import { fromRowToObj } from "../schema";

describe("schema", () => {
	class Document {
		@field({ type: "u8" })
		id: number;

		constructor(obj: any) {}
	}

	it("fromRowToObj", () => {
		const obj = { id: 1 };
		const parsed = fromRowToObj(obj, Document);
		expect(parsed).toBeInstanceOf(Document);
		expect(parsed.id).toBe(1);
	});
});
