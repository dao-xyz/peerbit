import {
	Or,
	Sort,
	SortDirection,
	StringMatch,
	id,
} from "@peerbit/indexer-interface";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { SQLLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import { setup } from "./utils.js";

use(chaiAsPromised);

describe("sort", () => {
	// u64 is a special case since we need to shift values to fit into signed 64 bit integers

	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index.store.stop();
	});

	class Document {
		@id({ type: "string" })
		id: string;

		constructor(id: string) {
			this.id = id;
		}
	}

	it("sorts by default by id ", async () => {
		// this test is to insure that the iterator is stable. I.e. default sorting is applied
		index = await setup({ schema: Document }, create);
		const store = index.store as SQLLiteIndex<Document>;
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
		const store = index.store as SQLLiteIndex<Document>;
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

		expect(results.map((x) => x.id.primitive)).to.deep.equal(["3", "2", "1"]); // insertion order (seems to be the default order when not sorting)
	});

	it("will sort correctly when query is split", async () => {
		index = await setup({ schema: Document }, create);
		const store = index.store as SQLLiteIndex<Document>;
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
