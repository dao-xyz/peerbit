import { toId } from "../id";

describe("toId", () => {
	it("throws when given an unexpected index key", () => {
		expect(() => toId(undefined as any)).toThrow(
			"Unexpected index key: undefined"
		);
	});
});
