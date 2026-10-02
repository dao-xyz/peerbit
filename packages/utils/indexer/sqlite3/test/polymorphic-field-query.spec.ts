import { field, variant } from "@dao-xyz/borsh";
import { type Index, Sort, StringMatch, id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLiteIndices } from "../src/engine.js";
import { createDatabase } from "../src/index.js";
import { MissingFieldError } from "../src/schema.js";

abstract class Child {}

@variant("unrelated_nested")
class Unrelated {
	@field({ type: "string" })
	string: string;

	constructor(string: string) {
		this.string = string;
	}
}

@variant("left")
class Left extends Child {
	@field({ type: "u32" })
	number: number;

	@field({ type: Unrelated })
	unrelated: Unrelated;

	constructor(number: number, unrelated: Unrelated) {
		super();
		this.number = number;
		this.unrelated = unrelated;
	}
}

@variant("right")
class Right extends Child {
	@field({ type: "string" })
	string: string;

	constructor(string: string) {
		super();
		this.string = string;
	}
}

@variant("polymorphic_field_root")
class Root {
	@id({ type: "string" })
	id: string;

	@field({ type: Child })
	child: Child;

	constructor(id: string, child: Child) {
		this.id = id;
		this.child = child;
	}
}

describe("SQLite polymorphic terminal fields", () => {
	let indices: SQLiteIndices | undefined;
	let store: Index<Root, any>;
	let matching: Root[];

	beforeEach(async () => {
		indices = new SQLiteIndices({ db: await createDatabase() });
		await indices.start();
		store = await indices.init({ schema: Root, indexBy: ["id"] });
		matching = [
			new Root("b", new Right("match")),
			new Root("c", new Right("match")),
		];
		for (const value of [
			new Root("a", new Left(7, new Unrelated("match"))),
			...matching,
			new Root("d", new Right("other")),
		]) {
			await store.put(value);
		}
	});

	afterEach(async () => {
		await indices?.stop();
		indices = undefined;
	});

	const query = async (key: string[], value: string) => {
		const iterator = store.iterate({
			query: new StringMatch({ key, value }),
			sort: [new Sort({ key: "id" })],
		});
		try {
			return await iterator.all();
		} finally {
			await iterator.close();
		}
	};

	it("matches terminal fields across variants without unrelated-child matches or duplicates", async () => {
		const rows = await query(["child", "string"], "match");
		expect(rows.map((row) => row.id.primitive)).to.deep.equal(["b", "c"]);
		expect(rows.map((row) => row.value)).to.deep.equal(matching);
		for (const row of rows) {
			expect(row.value).to.be.instanceOf(Root);
			expect(row.value.child).to.be.instanceOf(Right);
		}
	});

	it("returns no rows for a nonmatching terminal value", async () => {
		expect(await query(["child", "string"], "absent")).to.deep.equal([]);
	});

	it("still resolves the sibling's field through its explicit nested path", async () => {
		const rows = await query(["child", "unrelated", "string"], "match");
		expect(rows.map((row) => row.id.primitive)).to.deep.equal(["a"]);
		expect(rows[0].value.child).to.be.instanceOf(Left);
		expect(
			await query(["child", "unrelated", "string"], "absent"),
		).to.deep.equal([]);
		expect(
			await store.count({
				query: new StringMatch({
					key: ["child", "unrelated", "string"],
					value: "match",
				}),
			}),
		).to.equal(1);
	});

	it("rejects a terminal field absent from every variant", async () => {
		await expect(query(["child", "absent"], "match")).to.be.rejectedWith(
			MissingFieldError,
		);
	});

	it("rejects an invalid intermediate prefix even when the terminal field exists", async () => {
		await expect(
			query(["child", "absent", "string"], "match"),
		).to.be.rejectedWith(MissingFieldError);
	});
});
