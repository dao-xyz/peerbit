import { field, variant } from "@dao-xyz/borsh";
import { StringMatch, id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import type { SQLiteIndices } from "../src/engine.js";
import { setup } from "./utils.js";

@variant("primary_key_plan_context")
class PlanContext {
	@field({ type: "string" })
	head: string;

	constructor(head: string) {
		this.head = head;
	}
}

@variant("primary_key_plan_document")
class PlanDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	@field({ type: PlanContext })
	__context: PlanContext;

	constructor(id: string, name: string, head: string) {
		this.id = id;
		this.name = name;
		this.__context = new PlanContext(head);
	}
}

const createdIndexes = async (indices: SQLiteIndices) => {
	const statement = await indices.properties.db.prepare(
		"select name from sqlite_master where type = 'index' and sql is not null order by name",
	);
	const rows: { name: string }[] = await statement.all([]);
	return rows.map((row) => row.name);
};

// A query that matches the primary key by equality reaches at most one row
// through the primary key, so an index on its other columns cannot help it.
// The planner must not create one: the index would be stored and maintained on
// every later write.
describe("primary key equality", () => {
	const setupPlan = async () => {
		const { indices, store } = await setup({ schema: PlanDocument });
		await store.put(new PlanDocument("a", "first", "head-a"));
		await store.put(new PlanDocument("b", "second", "head-b"));
		return { indices: indices as SQLiteIndices, store };
	};

	it("creates no index for a delete that matches the primary key", async () => {
		const { indices, store } = await setupPlan();
		const before = await createdIndexes(indices);

		const stale = await store.del({
			query: [
				new StringMatch({ key: "id", value: "a" }),
				new StringMatch({ key: ["__context", "head"], value: "head-b" }),
			],
		});
		const current = await store.del({
			query: [
				new StringMatch({ key: "id", value: "a" }),
				new StringMatch({ key: ["__context", "head"], value: "head-a" }),
			],
		});

		expect(stale).to.deep.equal([]);
		expect(current.map((key) => key.primitive)).to.deep.equal(["a"]);
		expect(await store.getSize()).to.equal(1);
		expect(await createdIndexes(indices)).to.deep.equal(before);

		// Without the primary key the planner still indexes the matched columns.
		const byName = await store.del({
			query: [
				new StringMatch({ key: "name", value: "second" }),
				new StringMatch({ key: ["__context", "head"], value: "head-b" }),
			],
		});
		expect(byName.map((key) => key.primitive)).to.deep.equal(["b"]);
		expect((await createdIndexes(indices)).length).to.be.greaterThan(
			before.length,
		);
	});

	it("creates no index for a search that matches the primary key", async () => {
		const { indices, store } = await setupPlan();
		const before = await createdIndexes(indices);

		const results = await store
			.iterate({
				query: [
					new StringMatch({ key: "id", value: "b" }),
					new StringMatch({ key: ["__context", "head"], value: "head-b" }),
				],
			})
			.all();

		expect(results.map((result) => result.value.name)).to.deep.equal([
			"second",
		]);
		expect(await createdIndexes(indices)).to.deep.equal(before);
	});
});
