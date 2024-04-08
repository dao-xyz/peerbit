import { toId } from "../src/id.js";
import { expect } from "chai";
describe("toId", () => {
	it("throws when given an unexpected index key", () => {
		expect(() => toId(undefined as any)).to.throw(
			"Unexpected index key: undefined, expected: string, number, bigint or Uint8Array"
		);
	});
});
