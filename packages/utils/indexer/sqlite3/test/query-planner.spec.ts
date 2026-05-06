import {
	And,
	Compare,
	IntegerCompare,
	Or,
	type Query,
	Sort,
	SortDirection,
	toQuery,
} from "@peerbit/indexer-interface";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import {
	PlannableQuery,
	QueryPlanner,
	flattenQuery,
} from "../src/query-planner.js";

describe("PlannableQuery", () => {
	describe("IntegerCompare", () => {
		it("key same for small change", async () => {
			// TODO test all query types

			const plannable = new PlannableQuery({
				query: [
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key",
						value: 1,
					}),
				],
				sort: [new Sort({ key: "key", direction: SortDirection.ASC })],
			});

			const plannableOtherValues = new PlannableQuery({
				query: [
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key",
						value: 2,
					}),
				],
				sort: [new Sort({ key: "key", direction: SortDirection.ASC })],
			});

			expect(plannable.key).to.eq(plannableOtherValues.key);
		});

		it("key different for large diff", async () => {
			const plannable = new PlannableQuery({
				query: [
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key",
						value: 1,
					}),
				],
				sort: [new Sort({ key: "key", direction: SortDirection.ASC })],
			});

			const plannableOtherValues = new PlannableQuery({
				query: [
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key",
						value: 2147483647 + 1,
					}),
				],
				sort: [new Sort({ key: "key", direction: SortDirection.ASC })],
			});

			expect(plannable.key).to.not.eq(plannableOtherValues.key);
		});

		it("can generate key from query with nesting", async () => {
			const ors: And[] = [];
			for (const point of [1, 2]) {
				ors.push(
					new And([
						new And([
							new IntegerCompare({
								key: "key1",
								compare: Compare.LessOrEqual,
								value: point,
							}),
							new IntegerCompare({
								key: "key2",
								compare: Compare.Greater,
								value: point,
							}),
						]),
						new And([
							new IntegerCompare({
								key: "key3",
								compare: Compare.LessOrEqual,
								value: point,
							}),
							new IntegerCompare({
								key: "key4",
								compare: Compare.Greater,
								value: point,
							}),
						]),
					]),
				);
			}
			let complicatedQuery = [
				new Or(ors),
				new IntegerCompare({
					key: "key5",
					compare: Compare.Greater,
					value: 0,
				}),
			];

			const plannable = new PlannableQuery({
				query: toQuery(complicatedQuery),
				sort: [new Sort({ key: "key5", direction: SortDirection.ASC })],
			});

			expect(plannable.key).to.be.a("string");
		});
	});
});

describe("QueryPlanner", () => {
	it("can concurrently with same index", async () => {
		let executed: string[] = [];
		let execDelay = 3000;

		const planner = new QueryPlanner({
			exec: async (query: string) => {
				await delay(execDelay);
				executed.push(query);
			},
		});
		const query = new PlannableQuery({ query: [] });
		const scope1 = planner.scope(query);
		const scope2 = planner.scope(query);

		const index1 = scope1.resolveIndex("table", ["field1"]);
		const index2 = scope2.resolveIndex("table", ["field1"]);
		expect(index1).to.eq(index2);

		const prepare2 = scope2.beforePrepare(); // prepare 2 before 1 even though resolved index form 1
		await delay(1e2);
		const prepare1 = scope1.beforePrepare();

		const perform1 = prepare1.then(() =>
			scope1.perform(async () => {
				expect(executed).to.have.length(1);
				expect(executed[0]).to.contain(index1);
			}),
		);
		const perform2 = prepare2.then(() =>
			scope1.perform(async () => {
				expect(executed).to.have.length(1);
				expect(executed[0]).to.contain(index1);
			}),
		);

		await perform1;
		await perform2;
	});

	it("batches new index candidates into one exec", async () => {
		let executed: string[] = [];

		const planner = new QueryPlanner({
			exec: async (query: string) => {
				executed.push(query);
			},
		});
		const query = new PlannableQuery({ query: [] });
		const scope = planner.scope(query);

		scope.resolveIndex("table", ["field1", "field2"]);
		await scope.beforePrepare();

		expect(executed).to.have.length(1);
		expect(executed[0]).to.contain(
			"create index if not exists table_index_field1_field2",
		);
		expect(executed[0]).to.contain("PRAGMA optimize");
	});

	it("orders candidates by equality, sort, and range semantics", async () => {
		let executed: string[] = [];

		const planner = new QueryPlanner({
			exec: async (query: string) => {
				executed.push(query);
			},
		});
		const query = new PlannableQuery({
			query: [
				new IntegerCompare({
					key: "a",
					compare: Compare.Equal,
					value: 1,
				}),
				new IntegerCompare({
					key: "b",
					compare: Compare.GreaterOrEqual,
					value: 0,
				}),
			],
			sort: new Sort({ key: "c", direction: SortDirection.ASC }),
		});
		const scope = planner.scope(query);

		scope.resolveIndex("table", ["c", "a", "b"]);
		await scope.beforePrepare();

		expect(executed).to.have.length(1);
		expect(executed[0]).to.contain(
			"create index if not exists table_index_a_c_b",
		);
		expect(executed[0]).to.contain(
			"create index if not exists table_index_a_b_c",
		);
	});

	it("does not mutate resolved column order while building stats keys", async () => {
		const planner = new QueryPlanner({
			exec: async () => {},
		});
		const query = new PlannableQuery({ query: [] });
		const scope = planner.scope(query);
		const columns = ["field2", "field1"];

		scope.resolveIndex("table", columns);

		expect(columns).to.deep.equal(["field2", "field1"]);
	});

	it("keeps hard INDEXED BY compatibility mode by default", async () => {
		const defaultPlanner = new QueryPlanner({
			exec: async () => {},
		});
		const relaxedPlanner = new QueryPlanner({
			exec: async () => {},
			forceIndexes: false,
		});

		const defaultScope = defaultPlanner.scope(new PlannableQuery({ query: [] }));
		const relaxedScope = relaxedPlanner.scope(new PlannableQuery({ query: [] }));

		expect(defaultScope.forceIndex).to.equal(true);
		expect(relaxedScope.forceIndex).to.equal(false);
	});

	it("relaxes ambiguous child-table predicates until the index has warmed", async () => {
		const planner = new QueryPlanner({
			exec: async () => {},
		});
		const scope = planner.scope(new PlannableQuery({ query: [] }));

		scope.resolveIndex("child", ["__parent_id", "value"]);
		expect(scope.forceIndex).to.equal(false);

		scope.resolveIndex("root", ["value"]);
		expect(scope.forceIndex).to.equal(true);

		for (let i = 0; i < 6_000; i++) {
			scope.resolveIndex("child", ["__parent_id", "value"]);
		}
		expect(scope.forceIndex).to.equal(true);
	});
});

const generatorAsList = <T>(gen: Generator<T>) => {
	let result: T[] = [];
	for (const value of gen) {
		result.push(value);
	}
	return result;
};
describe("flattenQuery", () => {
	it("or, and", () => {
		let or = new Or([
			new IntegerCompare({
				compare: Compare.Equal,
				key: "key1",
				value: 1,
			}),
			new IntegerCompare({
				compare: Compare.Equal,
				key: "key2",
				value: 2,
			}),
		]);

		let result = generatorAsList(
			flattenQuery({
				query: [
					or,
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key3",
						value: 3,
					}),
				],
				sort: [],
			}),
		);

		expect(result).to.have.length(2);
		expect(
			result[0]!.query.map((x) => (x as IntegerCompare).key[0]),
		).to.have.members(["key1", "key3"]);
		expect(
			result[1]!.query.map((x) => (x as IntegerCompare).key[0]),
		).to.have.members(["key2", "key3"]);
	});

	it("only flattens ors less than size 4", async () => {
		let or = new Or([
			new IntegerCompare({
				compare: Compare.Equal,
				key: "key1",
				value: 1,
			}),
			new IntegerCompare({
				compare: Compare.Equal,
				key: "key2",
				value: 2,
			}),
			new IntegerCompare({
				compare: Compare.Equal,
				key: "key3",
				value: 3,
			}),
			new IntegerCompare({
				compare: Compare.Equal,
				key: "key3",
				value: 4,
			}),
		]);

		let result = generatorAsList(
			flattenQuery({
				query: [
					or,
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key4",

						value: 4,
					}),
				],
				sort: [],
			}),
		);

		expect(result).to.have.length(1);
	});

	it("and, and", () => {
		let result = generatorAsList(
			flattenQuery({
				query: [
					new And([
						new IntegerCompare({
							compare: Compare.Equal,
							key: "key1",
							value: 1,
						}),
						new IntegerCompare({
							compare: Compare.Equal,
							key: "key2",
							value: 2,
						}),
					]),
					new And([
						new IntegerCompare({
							compare: Compare.Equal,
							key: "key3",
							value: 3,
						}),
						new IntegerCompare({
							compare: Compare.Equal,
							key: "key4",
							value: 4,
						}),
					]),
				],
				sort: [],
			}),
		);

		expect(result).to.have.length(1);
		expect(result[0]!.query).to.have.length(4);
		expect(
			result[0]!.query.map((x) => (x as IntegerCompare).key[0]),
		).to.deep.eq(["key1", "key2", "key3", "key4"]);
	});

	it("and(or, and)", () => {
		let result = generatorAsList(
			flattenQuery({
				query: [
					new And([
						new Or([
							new IntegerCompare({
								compare: Compare.Equal,
								key: "key1",
								value: 1,
							}),
							new IntegerCompare({
								compare: Compare.Equal,
								key: "key2",
								value: 2,
							}),
						]),
						new IntegerCompare({
							compare: Compare.Equal,
							key: "key3",
							value: 3,
						}),
					]),
					new IntegerCompare({
						compare: Compare.Equal,
						key: "key4",
						value: 4,
					}),
				],
				sort: [],
			}),
		);

		expect(result).to.have.length(2);
		expect(
			result[0]!.query.map((x) => (x as IntegerCompare).key[0]),
		).to.have.members(["key1", "key3", "key4"]);
		expect(
			result[1]!.query.map((x) => (x as IntegerCompare).key[0]),
		).to.have.members(["key2", "key3", "key4"]);
	});

	it("or(and(and)), and", () => {
		const ors: And[] = [];
		for (const point of [1, 2]) {
			ors.push(
				new And([
					new And([
						new IntegerCompare({
							key: "key1",
							compare: Compare.LessOrEqual,
							value: point,
						}),
						new IntegerCompare({
							key: "key2",
							compare: Compare.Greater,
							value: point,
						}),
					]),
					new And([
						new IntegerCompare({
							key: "key3",
							compare: Compare.LessOrEqual,
							value: point,
						}),
						new IntegerCompare({
							key: "key4",
							compare: Compare.Greater,
							value: point,
						}),
					]),
				]),
			);
		}
		let complicatedQuery = [
			new Or(ors),
			new IntegerCompare({
				key: "key5",
				compare: Compare.Greater,
				value: 0,
			}),
		];

		let result = generatorAsList(
			flattenQuery({
				query: toQuery(complicatedQuery),
				sort: [],
			}),
		);

		expect(result).to.have.length(2);

		const checkResult = (result: Query[], value: number) => {
			expect(result).to.have.length(2);
			expect((result[0] as IntegerCompare).key[0]).to.eq("key5");
			const and = result[1] as And;
			const andAnd = and.and.map((x) => (x as And).and).flat(); // stuff inside or is not flattened
			expect(andAnd.map((x) => (x as IntegerCompare).value.value)).to.deep.eq([
				value,
				value,
				value,
				value,
			]);
		};
		checkResult(result[0]!.query, 1);
		checkResult(result[1]!.query, 2);
	});
});
