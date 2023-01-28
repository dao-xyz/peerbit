import { Range } from "../range.js";
import { StringOperation, applyOperations } from "../string-index.js";

describe("operations", () => {
	it("add", async () => {
		const operations: StringOperation[] = [
			{
				index: new Range({
					offset: 0,
					length: "hello".length,
				}),
				value: "hello",
			},
			{
				index: new Range({
					offset: "hello".length,
					length: " ".length,
				}),
				value: " ",
			},
			{
				index: new Range({
					offset: "hello ".length,
					length: "world".length,
				}),
				value: "world",
			},
		];

		let string = await applyOperations(
			"",
			operations.map((v, ix) => {
				{
					return {
						hash: ix.toString(),
						payload: {
							getValue: () => v,
						},
						getPayload: async () => {
							return { getValue: () => v };
						},
					} as any;
				}
			})
		);
		expect(string).toEqual("hello world");
	});

	it("replace", async () => {
		const operations: StringOperation[] = [
			{
				index: new Range({
					offset: 0,
					length: "hello".length,
				}),
				value: "hello",
			},
			{
				index: new Range({
					offset: "hello".length,
					length: "w".length,
				}),
				value: "w",
			},
			{
				index: new Range({
					offset: "hello ".length,
					length: "world".length,
				}),
				value: "world",
			},
			{
				index: new Range({
					offset: "hello".length,
					length: " ".length,
				}),
				value: " ",
			},
		];

		let string = await applyOperations(
			"",
			operations.map((v, ix) => {
				{
					return {
						hash: ix.toString(),
						payload: {
							getValue: () => v,
						},
						getPayload: async () => {
							return { getValue: () => v };
						},
					} as any;
				}
			})
		);
		expect(string).toEqual("hello world");
	});

	it("delete", async () => {
		const operations: StringOperation[] = [
			{
				index: new Range({
					offset: 0,
					length: 0,
				}),
				value: "hello world",
			},
			{
				index: new Range({
					offset: "hello".length,
					length: "hello world".length,
				}),
			},
		];

		let string = await applyOperations(
			"",
			operations.map((v, ix) => {
				{
					return {
						hash: ix.toString(),
						payload: {
							getValue: () => v,
						},
						getPayload: async () => {
							return { getValue: () => v };
						},
					} as any;
				}
			})
		);
		expect(string).toEqual("hello");
	});
});
