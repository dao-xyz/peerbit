import { field, variant } from "@dao-xyz/borsh";
import {
	Or,
	Sort,
	SortDirection,
	StringMatch,
	id,
} from "@peerbit/indexer-interface";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { SQLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { MissingFieldError } from "../src/schema.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

describe("sort", () => {
	// u64 is a special case since we need to shift values to fit into signed 64 bit integers

	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	@variant("Document")
	class Document {
		@id({ type: "string" })
		id: string;

		constructor(id: string) {
			this.id = id;
		}
	}

	const captureSQL = (store: SQLiteIndex<any>) => {
		const statements: string[] = [];
		const prepare = store.properties.db.prepare.bind(store.properties.db);
		store.properties.db.prepare = (...args: Parameters<typeof prepare>) => {
			statements.push(args[0]);
			return prepare(...args);
		};
		return statements;
	};

	for (const missingPosition of [0, 1, 2]) {
		for (const mode of ["all", "pages"]) {
			it(`preserves descending order with a skipped OR arm at ${missingPosition} (${mode})`, async () => {
				index = await setup({ schema: Document }, create);
				await index.store.put(new Document("1"));
				await index.store.put(new Document("2"));
				const statements = captureSQL(index.store as SQLiteIndex<Document>);
				const arms = ["1", "2"].map(
					(value) => new StringMatch({ key: "id", value }),
				);
				arms.splice(
					missingPosition,
					0,
					new StringMatch({ key: "absent", value: "x" }),
				);
				const iterator = index.store.iterate({
					query: new Or(arms),
					sort: new Sort({ key: "id", direction: SortDirection.DESC }),
				});
				try {
					const results =
						mode === "all"
							? await iterator.all()
							: [...(await iterator.next(1)), ...(await iterator.next(1))];
					expect(results.map(({ value }) => value.id)).to.deep.equal([
						"2",
						"1",
					]);
					if (mode === "pages") expect(await iterator.next(1)).to.be.empty;
					expect(statements).to.have.length(1);
					expect(statements[0]).to.match(/ORDER BY .* DESC/);
				} finally {
					await iterator.close();
				}
			});
		}
	}

	abstract class Root {
		abstract id: string;
		abstract rank: number;
	}
	@variant("first")
	class FirstRoot extends Root {
		@id({ type: "string" })
		id: string;

		@field({ type: "u32" })
		rank: number;

		@field({ type: "string" })
		a = "yes";

		constructor(id: string, rank: number) {
			super();
			this.id = id;
			this.rank = rank;
		}
	}
	@variant("second")
	class SecondRoot extends Root {
		@id({ type: "string" })
		id: string;

		@field({ type: "u32" })
		rank: number;

		@field({ type: "string" })
		b = "yes";

		constructor(id: string, rank: number) {
			super();
			this.id = id;
			this.rank = rank;
		}
	}

	for (const key of ["a", "b"]) {
		it(`uses only the participating root's sort alias (${key})`, async () => {
			index = await setup({ schema: Root }, create);
			const store = index.store as SQLiteIndex<Root>;
			await store.put(new FirstRoot("1", 10));
			await store.put(new FirstRoot("3", 30));
			await store.put(new SecondRoot("2", 20));
			await store.put(new SecondRoot("4", 40));
			expect(store.rootTables).to.have.length(2);
			const table = store.rootTables.find((root) =>
				root.fields.some((field) => field.name === key),
			)!;
			expect(store.rootTables.indexOf(table)).to.equal(key === "a" ? 0 : 1);
			const statements = captureSQL(store);
			const iterator = store.iterate(
				{
					query: new StringMatch({ key, value: "yes" }),
					sort: new Sort({ key: "rank", direction: SortDirection.DESC }),
				},
				{ shape: { id: true } },
			);
			try {
				const results = await iterator.all();
				expect(results.map(({ value }) => value.id)).to.deep.equal(
					key === "a" ? ["3", "1"] : ["4", "2"],
				);
				for (const { value } of results) {
					expect(value).to.have.all.keys("id");
				}
				expect(statements).to.have.length(1);
				expect(statements[0]).to.include(`ORDER BY "${table.name}#rank" DESC`);
			} finally {
				await iterator.close();
			}
		});
	}

	it("keeps ordering for an unflattened valid OR", async () => {
		index = await setup({ schema: Document }, create);
		for (const id of ["1", "2", "3", "4"]) {
			await index.store.put(new Document(id));
		}
		const iterator = index.store.iterate({
			query: new Or(
				["1", "2", "3", "4"].map(
					(value) => new StringMatch({ key: "id", value }),
				),
			),
			sort: new Sort({ key: "id", direction: SortDirection.DESC }),
		});
		try {
			expect((await iterator.all()).map(({ value }) => value.id)).to.deep.equal(
				["4", "3", "2", "1"],
			);
		} finally {
			await iterator.close();
		}
	});

	for (const missing of ["all arms", "sort", "unflattened arm"]) {
		it(`retains missing-field rejection for ${missing}`, async () => {
			index = await setup({ schema: Document }, create);
			await index.store.put(new Document("1"));
			const arms =
				missing === "all arms"
					? ["absent", "other"].map(
							(key) => new StringMatch({ key, value: "x" }),
						)
					: ["id", "id", "id", "absent"].map(
							(key) => new StringMatch({ key, value: "1" }),
						);
			const iterator = index.store.iterate({
				query: missing === "sort" ? [] : new Or(arms),
				sort: new Sort({
					key: missing === "sort" ? "absent" : "id",
					direction: SortDirection.DESC,
				}),
			});
			try {
				await expect(iterator.all()).to.be.rejectedWith(MissingFieldError);
			} finally {
				await iterator.close();
			}
		});
	}

	it("sorts by default by id ", async () => {
		// this test is to insure that the iterator is stable. I.e. default sorting is applied
		index = await setup({ schema: Document }, create);
		const store = index.store as SQLiteIndex<Document>;
		expect(store.tables.size).to.equal(1);
		await index.store.put(new Document("3"));
		await index.store.put(new Document("2"));
		await index.store.put(new Document("1"));

		const prepare = store.properties.db.prepare.bind(store.properties.db);
		let preparedStatement: string[] = [];
		store.properties.db.prepare = function (sql: string) {
			preparedStatement.push(sql);
			return prepare(sql);
		};

		const iterator = await index.store.iterate();
		const [first, second, third] = [
			...(await iterator.next(1)),
			...(await iterator.next(1)),
			...(await iterator.next(1)),
		];

		expect(preparedStatement).to.have.length(1);
		expect(preparedStatement[0]).to.contain("ORDER BY");

		expect(first.value.id).to.equal("1");
		expect(second.value.id).to.equal("2");
		expect(third.value.id).to.equal("3");
	});

	it("will not sort by default when fetching all", async () => {
		// this test is to insure that the iterator is stable. I.e. default sorting is applied
		index = await setup({ schema: Document }, create);
		const store = index.store as SQLiteIndex<Document>;
		expect(store.tables.size).to.equal(1);
		await index.store.put(new Document("3"));
		await index.store.put(new Document("2"));
		await index.store.put(new Document("1"));

		const prepare = store.properties.db.prepare.bind(store.properties.db);
		let preparedStatement: string[] = [];
		store.properties.db.prepare = function (sql: string) {
			preparedStatement.push(sql);
			return prepare(sql);
		};

		const iterator = index.store.iterate();
		const results = await iterator.all();

		expect(preparedStatement).to.have.length(1);
		expect(preparedStatement[0]).to.not.contain("ORDER BY");

		expect(results.map((x) => x.id.primitive).sort()).to.deep.equal([
			"1",
			"2",
			"3",
		]);
	});

	it("will sort correctly when query is split", async () => {
		index = await setup({ schema: Document }, create);
		const store = index.store as SQLiteIndex<Document>;
		expect(store.tables.size).to.equal(1);
		await index.store.put(new Document("3"));
		await index.store.put(new Document("2"));
		await index.store.put(new Document("1"));

		const prepare = store.properties.db.prepare.bind(store.properties.db);
		let preparedStatement: string[] = [];
		store.properties.db.prepare = function (sql: string) {
			preparedStatement.push(sql);
			return prepare(sql);
		};
		const iterator = index.store.iterate({
			query: new Or([
				new StringMatch({ key: "id", value: "1" }),
				new StringMatch({ key: "id", value: "2" }),
			]),
			sort: new Sort({ key: "id", direction: SortDirection.DESC }),
		});
		const results = await iterator.all();
		expect(results).to.have.length(2);

		expect(preparedStatement).to.have.length(1);
		expect(preparedStatement[0].match(/DESC/g)).to.have.length(1);
		expect(preparedStatement[0].match(/ASC/g)).to.be.null;
	});
});
