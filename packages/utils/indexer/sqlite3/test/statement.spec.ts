import { field, variant } from "@dao-xyz/borsh";
import {
	StringMatch,
	StringMatchMethod,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { SQLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

describe("statement", () => {
	let index: Awaited<ReturnType<typeof setup<any>>>;
	let store: SQLiteIndex<DocumentWithFromProperty>;

	@variant("DocumentWithFromProperty")
	class DocumentWithFromProperty {
		@id({ type: "string" })
		id: string;

		@field({ type: "string" })
		from: string;

		constructor(id: string, from: string) {
			this.id = id;
			this.from = from;
		}
	}

	let defaultStatementCount: number;

	beforeEach(async () => {
		index = await setup({ schema: DocumentWithFromProperty }, create);
		store = index.store as SQLiteIndex<DocumentWithFromProperty>;
		expect(store.tables.size).to.equal(1);
		defaultStatementCount = store.properties.db.statements.size;
		expect(defaultStatementCount).to.be.greaterThan(0);
	});

	afterEach(async () => {
		await index.store.stop();
	});

	describe("reuse", () => {
		it("get", async () => {
			await store.put(new DocumentWithFromProperty("1", "from"));
			await store.get(toId("1"));
			await store.get(toId("2"));
			await store.get(toId("3"));

			expect(store.properties.db.statements.size).to.equal(
				defaultStatementCount + 1,
			); // + get stmt
		});

		it("put", async () => {
			await store.put(new DocumentWithFromProperty("1", "from1"));
			await store.put(new DocumentWithFromProperty("2", "from2"));
			await store.put(new DocumentWithFromProperty("3", "from3"));

			expect(store!.properties.db.statements.size).to.equal(
				defaultStatementCount,
			); // no new statements
		});

		it("count", async () => {
			await store.put(new DocumentWithFromProperty("1", "from1"));

			expect(await store.count()).to.eq(1);
			expect(await store.count()).to.eq(1);
			expect(store.properties.db.statements.size).to.equal(
				defaultStatementCount + 1,
			); // + count stmt
		});

		it("count with a foreign query instance", async () => {
			await store.put(new DocumentWithFromProperty("1", "from1"));
			const { StringMatch: ForeignStringMatch } = await import(
				pathToFileURL(
					path.resolve(process.cwd(), "../interface/dist/src/query.js"),
				).href,
			);

			expect(
				await store.count({
					query: [
						new ForeignStringMatch({
							key: "from",
							value: "from1",
							method: StringMatchMethod.exact,
						}),
					],
				}),
			).to.eq(1);
		});

		it("query", async () => {
			for (let i = 0; i < 11; i++) {
				await store.put(new DocumentWithFromProperty(String(i), "from" + i));
			}
			// one
			expect(
				(await store.iterate({ query: { from: "from5" } }).all()).length,
			).to.eq(1);

			// in parts
			let fetch = 3;
			const rq = {
				query: [
					new StringMatch({
						key: "from",
						value: "from",
						method: StringMatchMethod.prefix,
					}),
				],
			};

			let iterator = await store.iterate(rq);
			const results = await iterator.next(fetch); // sohuld match all
			expect(results.length).to.eq(fetch);

			let nextFetch = 7;
			const next = await iterator.next(nextFetch);

			expect(next.length).to.eq(nextFetch);

			expect(await iterator.pending()).to.eq(1); // 11 - 3 - 7 = 1
		});
	});
});
