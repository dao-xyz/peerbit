import { field } from "@dao-xyz/borsh";
import {
	type Index,
	type IndexEngineInitProperties,
	type Indices,
	StringMatch,
	StringMatchMethod,
	getIdProperty,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";

const setup = async <T extends Record<string, any>>(
	properties: Partial<IndexEngineInitProperties<T, any>> & { schema: any },
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
): Promise<{ indices: Indices; store: Index<T, any>; directory?: string }> => {
	const indices = await createIndicies();
	await indices.start();
	const indexProps: IndexEngineInitProperties<T, any> = {
		...{
			indexBy: getIdProperty(properties.schema) || ["id"],
			iterator: { batch: { maxSize: 5e6, sizeProperty: ["__size"] } },
		},
		...properties,
	};
	const store = await indices.init(indexProps);
	return { indices, store };
};

describe("statement", () => {
	let index: Awaited<ReturnType<typeof setup<any>>>;
	let store: SQLLiteIndex<DocumentWithFromProperty>;

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

	const defaultStatementCount = 2;

	beforeEach(async () => {
		index = await setup({ schema: DocumentWithFromProperty }, create);
		store = index.store as SQLLiteIndex<DocumentWithFromProperty>;
		expect(store.tables.size).to.equal(1);
		expect(store.properties.db.statements.size).to.equal(defaultStatementCount); // put + replace stmt
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
