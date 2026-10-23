import { type AbstractType, field, variant, vec } from "@dao-xyz/borsh";
import {
	And,
	BoolQuery,
	Compare,
	IntegerCompare,
	Or,
	Sort,
	SortDirection,
	id,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { type SQLiteIndex, SQLiteIndices } from "../src/engine.js";
import { createDatabase } from "../src/index.js";
import { convertSearchRequestToQuery } from "../src/schema.js";

const MAX_U64 = (1n << 64n) - 1n;

abstract class CoordinateBase {
	abstract id: string;
	abstract coords: bigint[];
}

@variant("paged_coordinates")
class Coordinates {
	@id({ type: "string" })
	id: string;

	@field({ type: vec("u64") })
	coords: bigint[];

	@field({ type: "bool" })
	boundary: boolean;

	@field({ type: "u32" })
	rank: number;

	constructor(id: string, coords: bigint[], boundary = false, rank = 0) {
		this.id = id;
		this.coords = coords;
		this.boundary = boundary;
		this.rank = rank;
	}
}

@variant("first_paged_coordinates")
class FirstCoordinates extends CoordinateBase {
	@id({ type: "string" })
	id: string;

	@field({ type: vec("u64") })
	coords: bigint[];

	constructor(id: string, coords: bigint[]) {
		super();
		this.id = id;
		this.coords = coords;
	}
}

@variant("other_paged_coordinates")
class OtherCoordinates extends CoordinateBase {
	@id({ type: "string" })
	id: string;

	@field({ type: vec("u64") })
	coords: bigint[];

	constructor(id: string, coords: bigint[]) {
		super();
		this.id = id;
		this.coords = coords;
	}
}

@variant("nested_paged_coordinates")
class NestedCoordinates {
	@field({ type: vec("u64") })
	coords: bigint[];

	constructor(coords: bigint[]) {
		this.coords = coords;
	}
}

@variant("deep_paged_coordinates")
class DeepCoordinates {
	@id({ type: "string" })
	id: string;

	@field({ type: vec(NestedCoordinates) })
	groups: NestedCoordinates[];

	constructor(id: string, groups: bigint[][]) {
		this.id = id;
		this.groups = groups.map((coords) => new NestedCoordinates(coords));
	}
}

const documents = () => [
	new Coordinates("a", [0n, 2n, 2n, 40n, MAX_U64], false, 5),
	new Coordinates("b", [2n, 3n, 100n], false, 4),
	new Coordinates("c", [10n, MAX_U64], false, 3),
	new Coordinates("d", [], true, 2),
	new Coordinates("e", [51n, 52n], false, 1),
	new Coordinates("f", [1n << 63n, MAX_U64 - 1n], false, 0),
];

const compareCoordinate = (compare: Compare, value: bigint) =>
	new IntegerCompare({ key: "coords", compare, value });

describe("SQLite paged array hydration", () => {
	let opened: SQLiteIndices[];

	beforeEach(() => {
		opened = [];
	});

	afterEach(async () => {
		await Promise.all(opened.map((indices) => indices.stop()));
	});

	const open = async <T extends Record<string, any>>(
		schema: AbstractType<T>,
		values: T[],
	) => {
		const db = await createDatabase();
		const indices = new SQLiteIndices({ db });
		opened.push(indices);
		await indices.start();
		const store = await indices.init<T, never>({ schema, indexBy: ["id"] });
		// Deliberately insert in reverse order: pages must use primary-key order.
		for (const value of [...values].reverse()) await store.put(value);
		const sql: string[] = [];
		const prepare = db.prepare.bind(db);
		db.prepare = (query, statementId) => {
			if (/json_group_array/i.test(query)) sql.push(query);
			return prepare(query, statementId);
		};
		return { store, sql };
	};

	it("pages root IDs before reconstructing complete arrays", async () => {
		const expected = documents();
		const { store, sql } = await open(Coordinates, expected);
		const iterator = store.iterate();
		try {
			for (let offset = 0; offset < expected.length; offset += 2) {
				const page = await iterator.next(2);
				expect(page.map((row) => row.id.primitive)).to.deep.equal(
					expected.slice(offset, offset + 2).map((value) => value.id),
				);
				expect(page.map((row) => row.value)).to.deep.equal(
					expected.slice(offset, offset + 2),
				);
			}
			expect(await iterator.next(2)).to.deep.equal([]);
			expect(sql).to.have.length(1);
			const pageIds = sql[0].match(
				/\bIN\s*\(\s*(select DISTINCT[\s\S]*?limit \? offset \?)\)\s+GROUP BY/i,
			);
			expect(pageIds, sql[0]).not.to.equal(null);
			expect(pageIds![1]).not.to.match(/\bJOIN\b/i);
			expect(sql[0].slice(0, pageIds!.index)).to.match(/\bJOIN\b/i);
			expect(sql[0].match(/limit \? offset \?/gi)).to.have.length(1);
		} finally {
			await iterator.close();
		}
	});

	it("uses child matches for selection without filtering hydrated values", async () => {
		const expected = documents();
		const { store, sql } = await open(Coordinates, expected);
		const iterator = store.iterate({
			query: compareCoordinate(Compare.Equal, 2n),
		});
		try {
			for (const value of expected.slice(0, 2)) {
				const page = await iterator.next(1);
				expect(page.map((row) => row.value)).to.deep.equal([value]);
			}
			expect(await iterator.next(1)).to.deep.equal([]);
			expect(sql).to.have.length(1);
			expect(sql[0]).to.match(/\bIN\s*\(\s*select DISTINCT/i);
		} finally {
			await iterator.close();
		}
	});

	it("keeps disjoint OR ranges and empty boundary rows on the general path", async () => {
		const values = documents();
		const expected = values.filter((value) => value.id !== "e");
		const { store, sql } = await open(Coordinates, values);
		const iterator = store.iterate({
			query: new Or([
				new And([
					compareCoordinate(Compare.GreaterOrEqual, 0n),
					compareCoordinate(Compare.LessOrEqual, 3n),
				]),
				new And([
					compareCoordinate(Compare.GreaterOrEqual, MAX_U64 - 1n),
					compareCoordinate(Compare.LessOrEqual, MAX_U64),
				]),
				new BoolQuery({ key: "boundary", value: true }),
			]),
		});
		try {
			const result: Coordinates[] = [];
			for (let i = 0; i < 3; i++) {
				result.push(...(await iterator.next(2)).map((row) => row.value));
			}
			expect(result).to.deep.equal(expected);
			expect(await iterator.next(2)).to.deep.equal([]);
			expect(sql).to.have.length(1);
			expect(sql[0]).to.include("UNION");
			expect(sql[0]).not.to.match(/\bIN\s*\(\s*select DISTINCT/i);
		} finally {
			await iterator.close();
		}
	});

	it("preserves mutable-iterator rescan and exact unique results", async () => {
		const values = documents();
		const { store } = await open(Coordinates, values);
		const iterator = store.iterate();
		try {
			const first = await iterator.next(2);
			expect(first.map((row) => row.value)).to.deep.equal(values.slice(0, 2));
			const inserted = new Coordinates("aa", [MAX_U64, 7n, 7n]);
			const replaced = new Coordinates("e", [0n, MAX_U64]);
			await store.put(inserted);
			await store.put(new Coordinates("b", [999n]));
			await store.put(replaced);
			await store.del({ query: { id: "c" } });
			expect(await iterator.pending()).to.equal(4);
			const rest = [...(await iterator.next(2)), ...(await iterator.next(2))];
			expect(rest.map((row) => row.value)).to.deep.equal([
				inserted,
				values[3],
				replaced,
				values[5],
			]);
			const ids = [...first, ...rest].map((row) => row.id.primitive);
			expect(new Set(ids).size).to.equal(6);
			expect(await iterator.next(2)).to.deep.equal([]);
			expect(await iterator.pending()).to.equal(0);
		} finally {
			await iterator.close();
		}
	});

	it("leaves custom sort and fetch-all reconstruction on their existing paths", async () => {
		const values = documents();
		const { store, sql } = await open(Coordinates, values);
		const iterator = store.iterate({
			sort: new Sort({ key: "rank", direction: SortDirection.ASC }),
		});
		try {
			const result = [...(await iterator.next(3)), ...(await iterator.next(3))];
			expect(result.map((row) => row.value)).to.deep.equal(
				[...values].reverse(),
			);
			expect(sql[0]).not.to.match(/\bIN\s*\(\s*select DISTINCT/i);
		} finally {
			await iterator.close();
		}
		sql.length = 0;
		const all = await store.iterate().all();
		expect(
			all.map((row) => row.value).sort((a, b) => a.id.localeCompare(b.id)),
		).to.deep.equal(values);
		expect(sql).to.have.length(1);
		expect(sql[0]).not.to.match(/\bIN\s*\(\s*select DISTINCT|\blimit\b/i);
	});

	it("keeps multiple root schemas on the general SQL path", async () => {
		const { store } = await open(CoordinateBase, []);
		const index = store as unknown as SQLiteIndex<CoordinateBase>;
		expect(index.rootTables).to.have.length(2);
		expect(index.rootTables.map((table) => table.ctor)).to.have.members([
			FirstCoordinates,
			OtherCoordinates,
		]);
		// Route-selection coverage only, not a claim about polymorphic hydration.
		const { sql } = convertSearchRequestToQuery(
			undefined,
			index.tables,
			index.rootTables,
		);
		expect(sql).to.include("UNION");
		expect(sql).not.to.match(/\bIN\s*\(\s*select DISTINCT/i);
		expect(sql.match(/limit \? offset \?/gi)).to.have.length(1);
	});

	it("keeps deeply nested array reconstruction on the general path", async () => {
		const values = [
			new DeepCoordinates("a", [[0n, 2n, 2n], [MAX_U64]]),
			new DeepCoordinates("b", [[9n], []]),
		];
		const { store, sql } = await open(DeepCoordinates, values);
		const iterator = store.iterate();
		try {
			for (const value of values) {
				expect((await iterator.next(1)).map((row) => row.value)).to.deep.equal([
					value,
				]);
			}
			expect(await iterator.next(1)).to.deep.equal([]);
			expect(sql[0]).not.to.match(/\bIN\s*\(\s*select DISTINCT/i);
		} finally {
			await iterator.close();
		}
	});
});
