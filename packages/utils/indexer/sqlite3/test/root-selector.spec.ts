import { field, variant, vec } from "@dao-xyz/borsh";
import {
	And,
	BoolQuery,
	IntegerCompare,
	IsNull,
	Not,
	Or,
	type Query,
	Sort,
	SortDirection,
	StringMatch,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLiteIndex } from "../src/engine.js";
import { PlannableQuery } from "../src/query-planner.js";
import {
	convertCountRequestToQuery,
	convertDeleteRequestToQuery,
	convertSearchRequestToQuery,
	convertSumRequestToQuery,
} from "../src/schema.js";
import { setup } from "./utils.js";

const MAX = (1n << 64n) - 1n;
const HALF = 1n << 63n;

@variant("root-selector-row")
class Row {
	@id({ type: "string" })
	id: string;
	@field({ type: "u32" })
	rank: number;
	@field({ type: "bool" })
	enabled: boolean;
	@field({ type: vec("u64") })
	coords: bigint[];
	@field({ type: vec("string") })
	tags: string[];

	constructor(id: string, coords: bigint[], rank = 0, enabled = false) {
		this.id = id;
		this.coords = coords;
		this.rank = rank;
		this.enabled = enabled;
		this.tags = ["x", "y", "x"];
	}
}

@variant("root-selector-u64")
class U64Row {
	@id({ type: "u64" })
	id: bigint;
	@field({ type: vec("u64") })
	coords: bigint[];
	constructor(id: bigint) {
		this.id = id;
		this.coords = [MAX, id, 0n, id];
	}
}

@variant("root-selector-bytes")
class BytesRow {
	@id({ type: Uint8Array })
	id: Uint8Array;
	@field({ type: vec("u64") })
	coords: bigint[];
	constructor(id: Uint8Array) {
		this.id = id;
		this.coords = [5n, 1n, 5n];
	}
}

@variant("root-selector-child")
class Child {
	@field({ type: "u32" })
	a: number;
	@field({ type: "u32" })
	b: number;
	@field({ type: vec("u64") })
	coords: bigint[] = [5n];
	constructor(a = 0, b = 0) {
		this.a = a;
		this.b = b;
	}
}

@variant("root-selector-nested")
class NestedRow {
	@id({ type: "string" })
	id: string;
	@field({ type: vec(Child) })
	children: Child[];
	constructor(id = "nested", children = [new Child()]) {
		this.id = id;
		this.children = children;
	}
}

const cmp = (value: bigint, compare: "eq" | "gte" | "lt" = "eq") =>
	new IntegerCompare({ key: "coords", value, compare });
const range = () => new And([cmp(4n, "gte"), cmp(6n, "lt")]);
const ids = (rows: { value: { id: string } }[]) => rows.map((x) => x.value.id);
const order = (direction = SortDirection.ASC) =>
	["rank", "id"].map((key) => new Sort({ key, direction }));

describe("root selector hydration", () => {
	const fixture = () => [
		new Row("a-gap", [1n, 9n], 1),
		new Row("b-hit", [9n, 5n, 1n, 5n], 1),
		new Row("c-empty", [], 0, true),
		new Row("d-zero", [0n, 0n], 0),
		new Row("e-seam", [HALF - 1n, HALF, HALF + 1n], 2),
		new Row("f-max", [MAX], 2),
	];
	const open = async (rows = fixture()) => {
		const { store } = await setup<Row>({ schema: Row });
		for (const row of rows) await store.put(row);
		return store as SQLiteIndex<Row>;
	};

	it("keeps same-element witnesses, full array order and repeated values", async () => {
		const rows = fixture();
		const store = await open(rows);
		const results = await store.iterate<undefined>({ query: range() }).all();
		expect(ids(results)).to.deep.equal(["b-hit"]);
		expect(results[0].value).to.deep.equal(rows[1]);
		// [1, 9] must not provide separate witnesses for >= 4 and < 6.
		const rendered = convertSearchRequestToQuery(
			{ query: [range()] },
			store.tables,
			store.rootTables,
		);
		expect(rendered.sql).to.contain(" IN (SELECT ");
	});

	it("preserves NOT, NULL, root OR, overlapping UNIONs and u64 bounds", async () => {
		const rows = fixture();
		const store = await open(rows);
		const cases: [Query, (row: Row) => boolean][] = [
			// Legacy NOT is row-wise over the joined array, not NOT EXISTS.
			[new Not(cmp(5n)), (r) => r.coords.some((x) => x !== 5n)],
			[new IsNull({ key: "coords" }), (r) => r.coords.length === 0],
			[
				new Or([cmp(5n), new BoolQuery({ key: "enabled", value: true })]),
				(r) => r.enabled || r.coords.includes(5n),
			],
			[
				new Or([cmp(1n), cmp(9n)]),
				(r) => r.coords.some((x) => x === 1n || x === 9n),
			],
			[
				new Or([cmp(0n), cmp(5n), cmp(HALF), cmp(MAX)]),
				(r) => r.coords.some((x) => [0n, 5n, HALF, MAX].includes(x)),
			],
			[
				new And([cmp(HALF, "gte"), cmp(MAX, "lt")]),
				(r) => r.coords.some((x) => x >= HALF && x < MAX),
			],
		];
		for (const [query, matches] of cases) {
			const actual = await store
				.iterate<undefined>({ query, sort: new Sort({ key: "id" }) })
				.all();
			expect(actual.map((x) => x.value)).to.deep.equal(rows.filter(matches));
		}
	});

	it("separates two array predicates without truncating either array", async () => {
		const rows = fixture();
		rows[1].tags = ["other", "x", "x"];
		rows[0].tags = [];
		const store = await open(rows);
		const query = new And([
			range(),
			new StringMatch({ key: "tags", value: "x" }),
		]);
		const actual = await store.iterate<undefined>({ query }).all();
		expect(actual.map((x) => x.value)).to.deep.equal([rows[1]]);
	});

	it("requires one object to witness cross-field AND inside an array", async () => {
		const { store } = await setup<NestedRow>({ schema: NestedRow });
		const split = new NestedRow("split", [new Child(1, 0), new Child(0, 1)]);
		const same = new NestedRow("same", [new Child(1, 1), new Child(0, 0)]);
		await store.put(split);
		await store.put(same);
		const query = new And(
			["a", "b"].map(
				(key) =>
					new IntegerCompare({
						key: ["children", key],
						value: 1,
						compare: "eq",
					}),
			),
		);
		expect(
			(await store.iterate<undefined>({ query }).all()).map((x) => x.value),
		).to.deep.equal([same]);
	});

	it("returns identical hydrated roots with forced and unforced indexes", async () => {
		for (const forceIndexes of [true, false]) {
			const rows = fixture();
			const store = await open(rows);
			store.planner.props.forceIndexes = forceIndexes;
			const request = { query: [range()], sort: [new Sort({ key: "id" })] };
			const planner = store.planner.scope(new PlannableQuery(request));
			const { sql } = convertSearchRequestToQuery(
				request,
				store.tables,
				store.rootTables,
				{ planner },
			);
			await planner.beforePrepare();
			expect(sql.includes("INDEXED BY")).to.equal(forceIndexes);
			expect(sql).to.contain(" IN (SELECT ");
			expect(
				(await store.iterate<undefined>(request).all()).map((x) => x.value),
			).to.deep.equal([rows[1]]);
		}
	});

	it("preserves grouped shapes and characterizes legacy id-only duplicates", async () => {
		const store = await open();
		const request = { query: [cmp(5n)], sort: [new Sort({ key: "id" })] };
		const full = await store.iterate<undefined>(request).all();
		const partial = await store
			.iterate(request, { shape: { id: true, coords: true } })
			.all();
		expect(partial.map((x) => ({ ...x.value }))).to.deep.equal([
			{ id: "b-hit", coords: full[0].value.coords },
		]);
		const idOnly = await store.iterate(request, { shape: { id: true } }).all();
		// No aggregate existed here: retain both matching query rows.
		expect(idOnly.map((x) => ({ ...x.value }))).to.deep.equal([
			{ id: "b-hit" },
			{ id: "b-hit" },
		]);
		const sql = convertSearchRequestToQuery(
			request,
			store.tables,
			store.rootTables,
			{
				shape: { id: true },
			},
		).sql;
		expect(sql).not.to.contain(" IN (SELECT ");
	});

	for (const direction of [SortDirection.ASC, SortDirection.DESC]) {
		it(`keeps explicit rank/id tie-break order across pages (${direction})`, async () => {
			const rows = fixture().filter((x) => x.coords.length > 0);
			const store = await open(rows);
			const expected = [...rows].sort(
				(a, b) => a.rank - b.rank || a.id.localeCompare(b.id),
			);
			if (direction === SortDirection.DESC) expected.reverse();
			const iterator = store.iterate<undefined>({
				query: cmp(0n, "gte"),
				sort: order(direction),
			});
			try {
				for (const row of expected) {
					expect((await iterator.next(1)).map((x) => x.value)).to.deep.equal([
						row,
					]);
				}
				expect(await iterator.next(1)).to.deep.equal([]);
			} finally {
				await iterator.close();
			}
		});
	}

	it("leaves child sorting on the original renderer path", async () => {
		const store = await open();
		const sql = convertSearchRequestToQuery(
			{ query: [range()], sort: [new Sort({ key: "coords" })] },
			store.tables,
			store.rootTables,
		).sql;
		expect(sql).not.to.contain(" IN (SELECT ");
	});

	it("leaves root-only and deep predicates on the original renderer path", async () => {
		const store = await open();
		const rootOnly = convertSearchRequestToQuery(
			{ query: [new StringMatch({ key: "id", value: "b-hit" })] },
			store.tables,
			store.rootTables,
		).sql;
		expect(rootOnly).not.to.contain(" IN (SELECT ");
		const { store: nested } = await setup<NestedRow>({ schema: NestedRow });
		await nested.put(new NestedRow());
		const sqlStore = nested as SQLiteIndex<NestedRow>;
		const deep = convertSearchRequestToQuery(
			{
				query: [
					new IntegerCompare({
						key: ["children", "coords"],
						value: 5n,
						compare: "eq",
					}),
				],
			},
			sqlStore.tables,
			sqlStore.rootTables,
		).sql;
		expect(deep).not.to.contain(" IN (SELECT ");
	});

	it("does not change count, sum or delete selectors", async () => {
		const store = await open();
		const request = { query: range() };
		const root = store.rootTables[0];
		const count = convertCountRequestToQuery(request, store.tables, root);
		const sum = convertSumRequestToQuery(
			{ ...request, key: "rank" },
			store.tables,
			root,
		);
		const del = convertDeleteRequestToQuery(request, store.tables, root);
		expect(count.sql).not.to.contain(" IN (SELECT ");
		expect(sum.sql).not.to.contain(" IN (SELECT ");
		// DELETE already has exactly one root-id subquery of its own.
		expect(del.sql.match(/ IN \(SELECT /g)).to.have.length(1);
		expect(await store.count(request)).to.equal(1);
		// Native SQLite returns bigint; the WASM backend returns number here.
		expect(BigInt(await store.sum({ ...request, key: "rank" }))).to.equal(2n);
		expect((await store.del(request)).map((x) => x.key)).to.deep.equal([
			"b-hit",
		]);
		expect(await store.count()).to.equal(5);
	});

	it("rescans serial between-call mutations without re-yielding owned ids", async () => {
		const store = await open([
			new Row("a", [5n], 0),
			new Row("b", [5n], 1),
			new Row("c", [5n], 2),
			new Row("d", [5n], 3),
			new Row("e", [1n], 4),
		]);
		const iterator = store.iterate<undefined>({
			query: range(),
			sort: order(),
		});
		try {
			expect(ids(await iterator.next(1))).to.deep.equal(["a"]);
			await store.put(new Row("b", [1n], 1)); // leaves membership
			await store.put(new Row("d", [5n, 5n], 0)); // sort movement
			await store.del({ query: { id: "c" } }); // unseen deletion
			await store.put(new Row("e", [5n], 1)); // enters membership
			await store.put(new Row("f", [5n], 2)); // new matching root
			await store.del({ query: { id: "a" } });
			await store.put(new Row("a", [5n], 9)); // seen id reinserted
			expect(iterator.markYielded).to.be.a("function");
			await iterator.markYielded!([toId("f")]); // another path owns f
			expect(await iterator.pending()).to.equal(2);
			expect(ids(await iterator.next(1))).to.deep.equal(["d"]);
			await store.put(new Row("b", [5n], 0)); // enters before prior cursor
			expect(await iterator.pending()).to.equal(2);
			expect(ids(await iterator.next(1))).to.deep.equal(["b"]);
			expect(ids(await iterator.all())).to.deep.equal(["e"]);
			expect(await iterator.pending()).to.equal(0);
		} finally {
			await iterator.close();
		}
		expect(await iterator.next(1)).to.deep.equal([]);
	});

	it("rescans beyond 128 owned rows after serial mutations", async () => {
		const key = (i: number) => `row-${i.toString().padStart(3, "0")}`;
		const rows = Array.from(
			{ length: 260 },
			(_, i) => new Row(key(i), [5n, 5n], i),
		);
		const store = await open(rows);
		const iterator = store.iterate<undefined>({
			query: range(),
			sort: order(),
		});
		try {
			expect(ids(await iterator.next(1))).to.deep.equal([key(0)]);
			await store.del({ query: { id: key(150) } });
			await store.put(new Row(key(260), [5n], 260));
			await iterator.markYielded!(
				rows.slice(1, 130).map((row) => toId(row.id)),
			);
			// The first internal 128-row rescan page contains no unowned result.
			expect(ids(await iterator.next(2))).to.deep.equal([key(130), key(131)]);
			expect(await iterator.pending()).to.equal(128);
			const expected = rows.slice(132).filter((row) => row.id !== key(150));
			expected.push(new Row(key(260), [5n], 260));
			expect((await iterator.all()).map((x) => x.value)).to.deep.equal(
				expected,
			);
			expect(await iterator.pending()).to.equal(0);
		} finally {
			await iterator.close();
		}
	});

	it("keeps unsigned u64 primary keys and edge-coordinate reconstruction", async () => {
		const { store } = await setup<U64Row>({ schema: U64Row });
		const rows = [0n, HALF, MAX].map((id) => new U64Row(id));
		for (const row of [...rows].reverse()) await store.put(row);
		for (const direction of [SortDirection.ASC, SortDirection.DESC]) {
			const iterator = store.iterate<undefined>({
				query: cmp(MAX),
				sort: new Sort({ key: "id", direction }),
			});
			const expected =
				direction === SortDirection.ASC ? rows : [...rows].reverse();
			try {
				for (const row of expected) {
					const [result] = await iterator.next(1);
					expect(result.id.key).to.equal(row.id);
					expect(result.value).to.deep.equal(row);
				}
				expect(await iterator.next(1)).to.deep.equal([]);
			} finally {
				await iterator.close();
			}
		}
	});

	it("keeps byte primary keys distinct and reconstructs all repeated values", async () => {
		const { store } = await setup<BytesRow>({ schema: BytesRow });
		const rows = [[0], [0, 1], [255]].map(
			(id) => new BytesRow(new Uint8Array(id)),
		);
		for (const row of [...rows].reverse()) await store.put(row);
		const actual = await store
			.iterate<undefined>({ query: range(), sort: new Sort({ key: "id" }) })
			.all();
		expect(actual.map((x) => x.id.key)).to.deep.equal(rows.map((x) => x.id));
		expect(actual.map((x) => x.value)).to.deep.equal(rows);
	});
});
