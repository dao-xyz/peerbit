import { type ResultsIterator } from "../src/index.js";

describe("types", () => {
	it("ResultsIterator", () => {
		// @ts-expect-error unused
		const iterator: ResultsIterator<any> = {
			next: async (count: number) => {
				return [] as any[];
			},
			done: () => true,
			close: async () => {},
		};
	});
});
