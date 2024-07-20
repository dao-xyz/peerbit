import { serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import { toId } from "../src/id.js";
import { getIdProperty, id } from "../src/utils.js";

describe("toId", () => {
	it("throws when given an unexpected index key", () => {
		expect(() => toId(undefined as any)).to.throw(
			"Unexpected index key: undefined, expected: string, number, bigint or Uint8Array",
		);
	});
});

describe("id decorator", () => {
	it("should assign the property to be the id of the object for indexing", () => {
		// Arrange
		class Test {
			@id({ type: "string" })
			xyz: string;

			constructor() {
				this.xyz = "abc";
			}
		}

		expect(getIdProperty(Test)).to.deep.equal(["xyz"]);

		// check that serialization still work

		const obj = new Test();
		expect(serialize(obj)).to.deep.equal(
			new Uint8Array([3, 0, 0, 0, 97, 98, 99]),
		);
	});
});
