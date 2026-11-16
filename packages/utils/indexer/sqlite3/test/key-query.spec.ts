import { type AbstractType, field, option, variant, vec } from "@dao-xyz/borsh";
import {
	And,
	BoolQuery,
	Compare,
	IntegerCompare,
	Or,
	type Query,
	type Shape,
	Sort,
	SortDirection,
	StringMatch,
	id,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import type { SQLiteIndex } from "../src/engine.js";
import { PlannableQuery } from "../src/query-planner.js";
import {
	AFTER_CURSOR_ALIAS,
	type BindableValue,
	KeyQueryParameter,
	type KeyTermType,
	KeyTypeMismatchError,
	MissingFieldError,
	type OrderKey,
	PRIMARY_KEY_ALIAS,
	ROOT_ORDINAL_ALIAS,
	type Table,
	bindKeyQuery,
	buildKeysetPredicate,
	convertKeysToRowsQuery,
	convertSearchRequestToKeyQuery,
	convertSearchRequestToPageQuery,
	convertToSQLType,
	encodeKeyList,
	keyListSubquery,
	resolveInstanceFromValue,
	sortTermAlias,
	u64ToI64,
} from "../src/schema.js";
import { setup } from "./utils.js";

const MAX_U64 = (1n << 64n) - 1n;
const MIN_I64 = -(1n << 63n);
const MAX_I64 = (1n << 63n) - 1n;

@variant("key_query_meta")
class Meta {
	@field({ type: "u32" })
	score: number;

	@field({ type: option("string") })
	tag?: string;

	constructor(score: number, tag?: string) {
		this.score = score;
		this.tag = tag;
	}
}

@variant("key_query_document")
class Document {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	@field({ type: option("string") })
	label?: string;

	@field({ type: "u64" })
	big: bigint;

	@field({ type: Meta })
	meta: Meta;

	@field({ type: option(Meta) })
	extra?: Meta;

	@field({ type: vec("u32") })
	tags: number[];

	constructor(properties: {
		id: string;
		rank: number;
		label?: string;
		big: bigint;
		meta: Meta;
		extra?: Meta;
		tags: number[];
	}) {
		this.id = properties.id;
		this.rank = properties.rank;
		this.label = properties.label;
		this.big = properties.big;
		this.meta = properties.meta;
		this.extra = properties.extra;
		this.tags = properties.tags;
	}
}

const BIG = [0n, 1n, (1n << 53n) + 1n, 1n << 63n, MAX_U64];

// 40 rows inserted out of key order. Ranks tie, `label` and `extra` are unset
// in every combination, and `big` spans the u64 range.
const documents = () =>
	Array.from(
		{ length: 40 },
		(_, i) =>
			new Document({
				id: `d${String((i * 17) % 40).padStart(2, "0")}`,
				rank: i % 3,
				label: i % 3 === 0 ? undefined : `l${i % 4}`,
				big: BIG[i % 5],
				meta: new Meta(i % 3, i % 2 ? `t${i % 5}` : undefined),
				extra: i % 4 === 0 ? undefined : new Meta((i * 7) % 5),
				tags: i % 6 === 0 ? [] : [i % 5, (i * 3) % 7],
			}),
	);

@variant("key_query_u64_document")
class U64Document {
	@id({ type: "u64" })
	id: bigint;

	@field({ type: "u32" })
	rank: number;

	@field({ type: option("string") })
	label?: string;

	constructor(id: bigint, rank: number, label?: string) {
		this.id = id;
		this.rank = rank;
		this.label = label;
	}
}

const U64_IDS = [
	MAX_U64,
	0n,
	(1n << 53n) + 1n,
	1n,
	1n << 63n,
	(1n << 53n) - 1n,
	2n,
	(1n << 63n) - 1n,
	1000n,
	MAX_U64 - 1n,
	(1n << 63n) + 1n,
];
const u64Documents = () =>
	U64_IDS.map(
		(key, i) =>
			new U64Document(key, i % 3, i % 4 === 0 ? undefined : `l${i % 3}`),
	);

@variant("key_query_bytes_document")
class BytesDocument {
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u32" })
	rank: number;

	@field({ type: option("string") })
	label?: string;

	constructor(id: Uint8Array, rank: number, label?: string) {
		this.id = id;
		this.rank = rank;
		this.label = label;
	}
}

// Byte keys compare by content, then by length.
const BYTES_IDS = [
	[255, 255],
	[0],
	[1, 0, 0],
	[128],
	[0, 1],
	[1],
	[255],
	[0, 0],
	[127],
	[1, 0],
	[255, 0],
];
const bytesDocuments = () =>
	BYTES_IDS.map(
		(key, i) =>
			new BytesDocument(
				new Uint8Array(key),
				i % 3,
				i % 4 === 0 ? undefined : `l${i % 3}`,
			),
	);

abstract class Root {
	abstract id: string;
	abstract rank: number;
}

@variant("key_query_first")
class First extends Root {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	@field({ type: "string" })
	a: string;

	constructor(id: string, rank: number) {
		super();
		this.id = id;
		this.rank = rank;
		this.a = "x";
	}
}

@variant("key_query_second")
class Second extends Root {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	@field({ type: "string" })
	b: string;

	constructor(id: string, rank: number) {
		super();
		this.id = id;
		this.rank = rank;
		this.b = "y";
	}
}

// Odd keys in the first root table, even keys in the second, and "p04" in both.
const rootDocuments = () => [
	...Array.from({ length: 12 }, (_, i) => {
		const key = `p${String(i).padStart(2, "0")}`;
		return i % 2 ? new First(key, i % 3) : new Second(key, i % 3);
	}),
	new First("p04", 2),
];

// Root tables that store a term of K in different SQL types.

// `rank` holds numbers in one root table and TEXT in the other.
abstract class MixedRank {}

@variant("key_query_number_rank")
class NumberRank extends MixedRank {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	constructor(id: string, rank: number) {
		super();
		this.id = id;
		this.rank = rank;
	}
}

@variant("key_query_text_rank")
class TextRank extends MixedRank {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	rank: string;

	constructor(id: string, rank: string) {
		super();
		this.id = id;
		this.rank = rank;
	}
}

// Keys are numbers in one root table and TEXT in the other.
abstract class MixedKey {}

@variant("key_query_number_key")
class NumberKey extends MixedKey {
	@id({ type: "u32" })
	id: number;

	/** A field the other root table lacks. */
	@field({ type: "u32" })
	n: number;

	constructor(id: number) {
		super();
		this.id = id;
		this.n = id % 7;
	}
}

@variant("key_query_text_key")
class TextKey extends MixedKey {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	t: string;

	constructor(id: string) {
		super();
		this.id = id;
		this.t = id;
	}
}

// Keys are numbers in both root tables, but u64 keys are stored shifted into
// the signed range.
abstract class ShiftedKey {}

@variant("key_query_u64_key")
class U64Key extends ShiftedKey {
	@id({ type: "u64" })
	id: bigint;

	constructor(id: bigint) {
		super();
		this.id = id;
	}
}

@variant("key_query_u32_key")
class U32Key extends ShiftedKey {
	@id({ type: "u32" })
	id: number;

	constructor(id: number) {
		super();
		this.id = id;
	}
}

// Keys are BLOBs in one root table and TEXT in the other.
abstract class HexKey {}

@variant("key_query_bytes_key")
class BytesKey extends HexKey {
	@id({ type: Uint8Array })
	id: Uint8Array;

	constructor(id: number[]) {
		super();
		this.id = new Uint8Array(id);
	}
}

@variant("key_query_hex_text_key")
class HexTextKey extends HexKey {
	@id({ type: "string" })
	id: string;

	constructor(id: string) {
		super();
		this.id = id;
	}
}

// `a` is an integer in one root table and a REAL in the other, `b` TEXT in one
// and a BLOB in the other: SQLite compares each pair without converting.
abstract class AlikeTerms {}

@variant("key_query_alike_first")
class AlikeFirst extends AlikeTerms {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	a: number;

	@field({ type: "string" })
	b: string;

	constructor(id: string, a: number, b: string) {
		super();
		this.id = id;
		this.a = a;
		this.b = b;
	}
}

@variant("key_query_alike_second")
class AlikeSecond extends AlikeTerms {
	@id({ type: "string" })
	id: string;

	@field({ type: "f64" })
	a: number;

	@field({ type: Uint8Array })
	b: Uint8Array;

	constructor(id: string, a: number, b: number[]) {
		super();
		this.id = id;
		this.a = a;
		this.b = new Uint8Array(b);
	}
}

@variant("key_query_text_key_document")
class TextKeyDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	constructor(id: string, rank: number) {
		this.id = id;
		this.rank = rank;
	}
}

type Row = Record<string, unknown>;
type Request = { query: Query[]; sort?: Sort[] };
type Store = SQLiteIndex<any>;

const asc = (key: string | string[]) =>
	new Sort({ key, direction: SortDirection.ASC });
const desc = (key: string | string[]) =>
	new Sort({ key, direction: SortDirection.DESC });
const integer = (key: string, compare: Compare, value: number | bigint) =>
	new IntegerCompare({ key, compare, value });

const open = async <T extends Record<string, any>>(
	schema: AbstractType<T>,
	values: T[],
) => {
	const { store } = await setup<T>({ schema });
	for (const value of values) {
		await store.put(value);
	}
	return store as unknown as Store;
};

const run = async (
	store: Store,
	sql: string,
	values: unknown[] = [],
): Promise<Row[]> => {
	const statement = await store.properties.db.prepare(sql, sql);
	return (await statement.all(values as BindableValue[])) as Row[];
};

const planner = (store: Store, request: Request) =>
	store.planner.scope(
		new PlannableQuery({ query: request.query, sort: request.sort }),
	);

const hex = (bytes: Uint8Array) =>
	Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

/** A stored SQL value as text: integers by value, whether number or bigint. */
const show = (value: unknown): string => {
	if (value == null) {
		return "NULL";
	}
	if (typeof value === "bigint" || typeof value === "number") {
		return typeof value === "number" && !Number.isInteger(value)
			? String(value)
			: BigInt(value).toString();
	}
	if (typeof value === "string") {
		return JSON.stringify(value);
	}
	if (value instanceof Uint8Array) {
		return `x'${hex(value)}'`;
	}
	throw new Error(`Unexpected SQL value: ${String(value)}`);
};

/** A key statement row's identity: root table ordinal and primary key. */
const identity = (row: Row) =>
	`${show(row[ROOT_ORDINAL_ALIAS] ?? 0)}:${show(row[PRIMARY_KEY_ALIAS])}`;

const sortTerms = (order: OrderKey) =>
	order.terms.filter((term) => term.alias.startsWith("__s"));

const sortValues = (order: OrderKey, row: Row) =>
	sortTerms(order)
		.map((term) => show(row[term.alias]))
		.join(", ");

const cursorOf = (order: OrderKey, row: Row) =>
	order.terms.map((term) => row[term.alias]);

const nullPattern = (cursor: unknown[]) => cursor.map((value) => value == null);

const utf8 = new TextEncoder();
const compareBytes = (a: Uint8Array, b: Uint8Array) => {
	for (let i = 0; i < Math.min(a.length, b.length); i++) {
		if (a[i] !== b[i]) {
			return a[i] - b[i];
		}
	}
	return a.length - b.length;
};
const storageClass = (value: unknown) =>
	value == null
		? 0
		: typeof value === "number" || typeof value === "bigint"
			? 1
			: typeof value === "string"
				? 2
				: 3;

/** Two numbers (INTEGER or REAL) compared exactly, as SQLite compares them. */
const compareNumbers = (a: number | bigint, b: number | bigint): number => {
	if (typeof a === "number" && typeof b === "number") {
		return a < b ? -1 : a > b ? 1 : 0;
	}
	const whole = (value: number | bigint) =>
		typeof value === "bigint" ? value : BigInt(Math.floor(value));
	const fraction = (value: number | bigint) =>
		typeof value === "bigint" ? 0 : value - Math.floor(value);
	const [left, right] = [whole(a), whole(b)];
	return left !== right
		? left < right
			? -1
			: 1
		: Math.sign(fraction(a) - fraction(b));
};

/** SQLite's order of stored values: NULL, numbers, TEXT as UTF-8, BLOB. */
const compareSqlValues = (a: unknown, b: unknown): number => {
	const classes = storageClass(a) - storageClass(b);
	if (classes !== 0 || a == null) {
		return classes;
	}
	if (typeof a === "string") {
		return compareBytes(utf8.encode(a), utf8.encode(b as string));
	}
	if (a instanceof Uint8Array) {
		return compareBytes(a, b as Uint8Array);
	}
	return compareNumbers(a as number | bigint, b as number | bigint);
};

const compareInOrder = (order: OrderKey, a: Row, b: Row) => {
	for (const term of order.terms) {
		const result = compareSqlValues(a[term.alias], b[term.alias]);
		if (result !== 0) {
			return term.direction === SortDirection.DESC ? -result : result;
		}
	}
	return 0;
};

/**
 * Checks the order inside each tie on every sort term, from the request alone
 * (not from the builder's order key): keys run in the direction of a sort on
 * the key itself, else of the last sort, else ASC; one key in two root tables
 * runs by root ordinal. Returns the number of ties seen.
 */
const expectTiebreak = (
	order: OrderKey,
	request: Request,
	rows: Row[],
	label: string,
) => {
	const sorts = request.sort ?? [];
	const direction =
		(
			sorts.find((sort) => sort.key.length === 1 && sort.key[0] === "id") ??
			sorts[sorts.length - 1]
		)?.direction ?? SortDirection.ASC;
	let ties = 0;
	for (let i = 1; i < rows.length; i++) {
		const [before, after] = [rows[i - 1], rows[i]];
		if (sortValues(order, before) !== sortValues(order, after)) {
			continue;
		}
		ties++;
		const keys = compareSqlValues(
			before[PRIMARY_KEY_ALIAS],
			after[PRIMARY_KEY_ALIAS],
		);
		if (keys === 0) {
			expect(
				Number(before[ROOT_ORDINAL_ALIAS]),
				`${label}: tie at ${i}`,
			).to.be.lessThan(Number(after[ROOT_ORDINAL_ALIAS]));
		} else {
			expect(
				direction === SortDirection.DESC ? -keys : keys,
				`${label}: tie at ${i}`,
			).to.be.lessThan(0);
		}
	}
	return ties;
};

/** Every NULL pattern a cursor of `order` can have. */
const nullPatterns = (order: OrderKey) =>
	order.terms.reduce<boolean[][]>(
		(patterns, term) =>
			patterns.flatMap((pattern) =>
				term.nullable
					? [
							[...pattern, false],
							[...pattern, true],
						]
					: [[...pattern, false]],
			),
		[[]],
	);

/** The key statements of one search, prepared as an iterator would. */
const keyStatements = async (store: Store, request: Request, shape?: Shape) => {
	const scope = planner(store, request);
	const { tables, rootTables } = store;
	const keyList = convertSearchRequestToKeyQuery(request, tables, rootTables, {
		planner: scope,
	});
	const head = convertSearchRequestToPageQuery(request, tables, rootTables, {
		planner: scope,
		shape,
	});
	const after = new Map<
		string,
		ReturnType<typeof convertSearchRequestToPageQuery>
	>();
	const recheck = new Map<
		string,
		ReturnType<typeof convertSearchRequestToKeyQuery>
	>();
	for (const nulls of nullPatterns(keyList.order)) {
		after.set(
			nulls.join(),
			convertSearchRequestToPageQuery(request, tables, rootTables, {
				planner: scope,
				shape,
				after: nulls,
			}),
		);
		recheck.set(
			nulls.join(),
			convertSearchRequestToKeyQuery(request, tables, rootTables, {
				planner: scope,
				restrictToKeys: true,
				after: nulls,
			}),
		);
	}
	const recheckAll = convertSearchRequestToKeyQuery(
		request,
		tables,
		rootTables,
		{ planner: scope, restrictToKeys: true },
	);
	await scope.beforePrepare();
	return { order: keyList.order, keyList, head, after, recheck, recheckAll };
};
type KeyStatements = Awaited<ReturnType<typeof keyStatements>>;
type PageStatements = Pick<KeyStatements, "order" | "head" | "after">;

/**
 * S1 and S2 alone, without a planner: for root tables whose keys a key list
 * (S4) cannot match.
 */
const pageStatements = (store: Store, request: Request): PageStatements => {
	const { tables, rootTables } = store;
	const head = convertSearchRequestToPageQuery(request, tables, rootTables);
	return {
		order: head.order,
		head,
		after: new Map(
			nullPatterns(head.order).map((nulls) => [
				nulls.join(),
				convertSearchRequestToPageQuery(request, tables, rootTables, {
					after: nulls,
				}),
			]),
		),
	};
};

const readKeyList = (store: Store, statements: KeyStatements) =>
	run(
		store,
		statements.keyList.sql,
		bindKeyQuery(statements.keyList.bindable, {}),
	);

/**
 * Pages through S1 and S2 the way K pages: LIMIT n + 1, then continue after
 * the last row returned.
 */
const MAX_PAGED_ROWS = 1000;

const pageThrough = async (
	store: Store,
	statements: PageStatements,
	pageSize: number,
) => {
	const rows: Row[] = [];
	const patterns = new Set<string>();
	let statementCount = 1;
	let page = await run(
		store,
		statements.head.sql,
		bindKeyQuery(statements.head.bindable, { limit: pageSize + 1 }),
	);
	for (;;) {
		rows.push(...page.slice(0, pageSize));
		if (page.length <= pageSize) {
			break;
		}
		if (rows.length > MAX_PAGED_ROWS) {
			// A predicate that lets a row through again would page forever.
			throw new Error(`The pages did not end after ${rows.length} rows`);
		}
		const cursor = cursorOf(statements.order, rows[rows.length - 1]);
		const nulls = nullPattern(cursor).join();
		patterns.add(nulls);
		const next = statements.after.get(nulls)!;
		page = await run(
			store,
			next.sql,
			bindKeyQuery(next.bindable, { cursor, limit: pageSize + 1 }),
		);
		statementCount++;
	}
	return { rows, patterns, statementCount };
};

/** The rows the iterator returns today, through its OFFSET pages. */
const pagedIterator = async (store: Store, request: Request) => {
	const iterator = store.iterate(request);
	const rows = [];
	try {
		for (;;) {
			const page = await iterator.next(7);
			if (page.length === 0) {
				break;
			}
			rows.push(...page);
		}
	} finally {
		await iterator.close();
	}
	return rows;
};

const rootOrdinal = (store: Store, value: unknown) =>
	store.rootTables.findIndex(
		(table) => (value as object).constructor === table.ctor,
	);

const iteratorIdentity = (
	store: Store,
	result: {
		id: { key: string | number | bigint | Uint8Array };
		value: unknown;
	},
) => {
	const ordinal = rootOrdinal(store, result.value);
	const table = store.rootTables[ordinal];
	return `${ordinal}:${show(
		convertToSQLType(result.id.key, table.primaryField!.from!.type as any),
	)}`;
};

/** Every row as the iterator hydrates it, by identity. */
const storedValues = async (store: Store) => {
	const values = new Map<string, unknown>();
	for (const result of await store.iterate().all()) {
		values.set(iteratorIdentity(store, result), result.value);
	}
	return values;
};

const decode = (store: Store, table: Table, row: Row, shape?: Shape) =>
	resolveInstanceFromValue(
		row,
		store.tables,
		table,
		store["resolveDependencies"].bind(store),
		true,
		shape,
	);

const tableOf = (store: Store, row: Row) =>
	store.rootTables[Number(row[ROOT_ORDINAL_ALIAS] ?? 0)];

/** The raw primary key of every row, from S3 over the whole index. */
const allKeys = async (store: Store) => {
	const statements = await keyStatements(store, { query: [] });
	return (await readKeyList(store, statements)).map(
		(row) => row[PRIMARY_KEY_ALIAS],
	);
};

/** Cursor positions: both ends, the middle and the first row of each NULL pattern. */
const cursorPositions = (order: OrderKey, rows: Row[]) => {
	const positions = new Set<number>([0, rows.length - 1, rows.length >> 1]);
	const seen = new Set<string>();
	rows.forEach((row, i) => {
		const nulls = nullPattern(cursorOf(order, row)).join();
		if (!seen.has(nulls)) {
			seen.add(nulls);
			positions.add(i);
		}
	});
	return [...positions].filter((i) => i >= 0 && i < rows.length);
};

type Fixture = {
	name: string;
	open: () => Promise<Store>;
	sorts: Record<string, Sort[]>;
	queries: Record<string, Query[]>;
	absent: unknown[];
};

const FIXTURES: Fixture[] = [
	{
		name: "string keys, nested and optional fields",
		open: () => open(Document, documents()),
		sorts: {
			"no sort": [],
			"id DESC": [desc("id")],
			"rank ASC": [asc("rank")],
			"rank DESC": [desc("rank")],
			"label ASC": [asc("label")],
			"label DESC": [desc("label")],
			"rank DESC, label ASC": [desc("rank"), asc("label")],
			"label DESC, rank ASC": [desc("label"), asc("rank")],
			"label ASC, extra.score DESC": [asc("label"), desc(["extra", "score"])],
			"meta.score ASC, meta.tag DESC": [
				asc(["meta", "score"]),
				desc(["meta", "tag"]),
			],
			"extra.score DESC": [desc(["extra", "score"])],
			"big DESC": [desc("big")],
			"rank ASC, id DESC": [asc("rank"), desc("id")],
			"id ASC, rank DESC": [asc("id"), desc("rank")],
		},
		queries: {
			all: [],
			"rank = 1": [integer("rank", Compare.Equal, 1)],
			"tags = 2": [integer("tags", Compare.Equal, 2)],
			"rank = 0 OR label = l1": [
				new Or([
					integer("rank", Compare.Equal, 0),
					new StringMatch({ key: "label", value: "l1" }),
				]),
			],
		},
		absent: ["d99", "absent"],
	},
	{
		name: "u64 keys",
		open: () => open(U64Document, u64Documents()),
		sorts: {
			"no sort": [],
			"id DESC": [desc("id")],
			"rank DESC": [desc("rank")],
			"label ASC, rank DESC": [asc("label"), desc("rank")],
		},
		queries: {
			all: [],
			"rank = 1": [integer("rank", Compare.Equal, 1)],
		},
		absent: [u64ToI64(3n), u64ToI64(1n << 62n)],
	},
	{
		name: "byte keys",
		open: () => open(BytesDocument, bytesDocuments()),
		sorts: {
			"no sort": [],
			"id DESC": [desc("id")],
			"rank ASC": [asc("rank")],
			"label DESC, rank ASC": [desc("label"), asc("rank")],
		},
		queries: {
			all: [],
			"rank = 1": [integer("rank", Compare.Equal, 1)],
		},
		absent: [new Uint8Array([9, 9]), new Uint8Array([0, 0, 0])],
	},
];

const requestsOf = (fixture: Fixture) =>
	Object.entries(fixture.queries).flatMap(([queryName, query]) =>
		Object.entries(fixture.sorts).map(([sortName, sort]) => ({
			label: `${queryName} / ${sortName}`,
			request: { query, sort } as Request,
		})),
	);

describe("key statements", () => {
	describe("order key", () => {
		it("ends with the primary key, in the direction of the last sort", async () => {
			const store = await open(Document, []);
			const order = (sort?: Sort[]) =>
				convertSearchRequestToKeyQuery(
					{ query: [], sort },
					store.tables,
					store.rootTables,
				).order;
			const term = (
				alias: string,
				direction: SortDirection,
				nullable: boolean,
				type: KeyTermType,
			) => ({ alias, direction, nullable, type });
			const { ASC, DESC } = SortDirection;

			expect(order()).to.deep.equal({
				terms: [term("__pk", ASC, false, "text")],
				sorted: false,
			});
			expect(order([desc("rank")]).terms).to.deep.equal([
				term(sortTermAlias(0), DESC, true, "numeric"),
				term("__pk", DESC, false, "text"),
			]);
			expect(order([desc("rank"), asc("label")]).terms).to.deep.equal([
				term("__s0", DESC, true, "numeric"),
				term("__s1", ASC, true, "text"),
				term("__pk", ASC, false, "text"),
			]);
			expect(
				order([asc(["meta", "score"]), desc(["meta", "tag"])]).terms,
			).to.deep.equal([
				term("__s0", ASC, true, "numeric"),
				term("__s1", DESC, true, "text"),
				term("__pk", DESC, false, "text"),
			]);
			// A sort on the primary key ends K: the key is unique.
			expect(order([asc("rank"), desc("id")]).terms).to.deep.equal([
				term("__s0", ASC, true, "numeric"),
				term("__pk", DESC, false, "text"),
			]);
			expect(order([asc("id"), desc("rank")])).to.deep.equal({
				terms: [term("__pk", ASC, false, "text")],
				sorted: true,
			});

			for (const [schema, type] of [
				[U64Document, "numeric"],
				[BytesDocument, "blob"],
			] as const) {
				const keyed = await open<any>(schema, []);
				expect(
					convertSearchRequestToKeyQuery(
						{ query: [], sort: [desc("label")] },
						keyed.tables,
						keyed.rootTables,
					).order.terms,
				).to.deep.equal([
					term("__s0", DESC, true, "text"),
					term("__pk", DESC, false, type),
				]);
			}

			const roots = await open(Root, []);
			expect(
				convertSearchRequestToKeyQuery(
					{ query: [], sort: [desc("rank")] },
					roots.tables,
					roots.rootTables,
				).order.terms,
			).to.deep.equal([
				term("__s0", DESC, true, "numeric"),
				term("__pk", DESC, false, "text"),
				term("__t", ASC, false, "numeric"),
			]);
		});

		it("puts NULL first in ASC and last in DESC", async () => {
			const values = documents();
			const store = await open(Document, values);
			const unset = (key: "label" | "extra") =>
				values.filter((value) => value[key] === undefined).length;
			for (const [sort, nulls] of [
				[asc("label"), unset("label")],
				[desc("label"), unset("label")],
				[asc(["extra", "score"]), unset("extra")],
				[desc(["extra", "score"]), unset("extra")],
			] as const) {
				const rows = await readKeyList(
					store,
					await keyStatements(store, { query: [], sort: [sort] }),
				);
				const first = sort.direction === SortDirection.ASC;
				expect(nulls).to.be.greaterThan(0);
				expect(rows.map((row) => row[sortTermAlias(0)] == null)).to.deep.equal(
					rows.map((_, i) => (first ? i < nulls : i >= rows.length - nulls)),
				);
			}
		});

		it("continues after a cursor with every NULL pattern of two optional terms", async () => {
			const store = await open(Document, documents());
			const statements = await keyStatements(store, {
				query: [],
				sort: [asc("label"), desc(["extra", "score"])],
			});
			expect([...statements.after.keys()].sort()).to.deep.equal([
				"false,false,false",
				"false,true,false",
				"true,false,false",
				"true,true,false",
			]);
			const { rows, patterns } = await pageThrough(store, statements, 1);
			expect(rows.map(identity)).to.deep.equal(
				(await readKeyList(store, statements)).map(identity),
			);
			expect([...patterns].sort()).to.deep.equal(
				[...statements.after.keys()].sort(),
			);
		});
	});

	describe("shapes", () => {
		it("pages a shaped search with its key terms, each row once", async () => {
			const store = await open(Document, documents());
			// Two array elements of one row can match; the shape leaves out the array.
			const request = {
				query: [integer("tags", Compare.Equal, 2)],
				sort: [desc("rank")],
			};
			const shape: Shape = { id: true, rank: true };
			const statements = await keyStatements(store, request, shape);
			const keyRows = await readKeyList(store, statements);
			expect(keyRows.map((row) => row[PRIMARY_KEY_ALIAS])).to.include("d09");
			for (const pageSize of [1, 7]) {
				const { rows } = await pageThrough(store, statements, pageSize);
				expect(rows.map(identity)).to.deep.equal(keyRows.map(identity));
				for (const row of rows) {
					const value = await decode(store, store.rootTables[0], row, shape);
					expect({ ...value }).to.deep.equal({
						id: row[PRIMARY_KEY_ALIAS],
						rank: Number(row[sortTermAlias(0)]),
					});
				}
			}
		});
	});

	describe("arms and grouping", () => {
		it("reports each statement's arms and whether they group by the key", async () => {
			const store = await open(Document, []);
			const roots = await open(Root, []);
			// The facts describe the SQL: one SELECT per arm, and a GROUP BY
			// in each arm that groups.
			const armsOf = (statement: {
				sql: string;
				arms: number;
				grouped: boolean;
			}) => {
				expect(statement.sql.split(" UNION ").length).to.equal(statement.arms);
				expect(statement.sql.includes(" GROUP BY ")).to.equal(
					statement.grouped,
				);
				return { arms: statement.arms, grouped: statement.grouped };
			};
			const flat: Shape = { id: true, rank: true };
			type Facts = [arms: number, grouped: boolean];
			const cases: [
				label: string,
				index: Store,
				request: Request,
				keys: Facts,
				page: Facts,
				flatPage: Facts,
			][] = [
				// one root table, one branch: the hydration aggregates `tags`
				["all", store, { query: [] }, [1, false], [1, true], [1, false]],
				// a query on a child table groups every statement by the key
				[
					"tags = 2",
					store,
					{ query: [integer("tags", Compare.Equal, 2)] },
					[1, true],
					[1, true],
					[1, true],
				],
				// an Or over root columns splits into one arm per branch
				[
					"rank = 0 OR label = l1",
					store,
					{
						query: [
							new Or([
								integer("rank", Compare.Equal, 0),
								new StringMatch({ key: "label", value: "l1" }),
							]),
						],
					},
					[2, false],
					[2, true],
					[2, false],
				],
				// one arm per root table, and none for a table without the sort
				[
					"two root tables",
					roots,
					{ query: [] },
					[2, false],
					[2, false],
					[2, false],
				],
				[
					"a sort only one root table has",
					roots,
					{ query: [], sort: [desc("a")] },
					[1, false],
					[1, false],
					[1, false],
				],
			];
			for (const [label, index, request, keys, page, flatPage] of cases) {
				const expected = ([arms, grouped]: Facts) => ({ arms, grouped });
				const { tables, rootTables } = index;
				expect(
					armsOf(convertSearchRequestToKeyQuery(request, tables, rootTables)),
					`${label}: S3`,
				).to.deep.equal(expected(keys));
				expect(
					armsOf(
						convertSearchRequestToKeyQuery(request, tables, rootTables, {
							restrictToKeys: true,
						}),
					),
					`${label}: S4`,
				).to.deep.equal(expected(keys));
				expect(
					armsOf(convertSearchRequestToPageQuery(request, tables, rootTables)),
					`${label}: S1`,
				).to.deep.equal(expected(page));
				expect(
					armsOf(
						convertSearchRequestToPageQuery(request, tables, rootTables, {
							shape: flat,
						}),
					),
					`${label}: S1 with a shape without arrays`,
				).to.deep.equal(expected(flatPage));
			}
			const [table] = store.rootTables;
			for (const [rowsByKey, grouped] of [
				[convertKeysToRowsQuery(table, undefined), true],
				[convertKeysToRowsQuery(table, flat), false],
				[convertKeysToRowsQuery(roots.rootTables[0], undefined), false],
			] as const) {
				expect(rowsByKey.grouped).to.equal(grouped);
				expect(rowsByKey.sql.includes(" GROUP BY ")).to.equal(grouped);
			}
		});
	});

	for (const fixture of FIXTURES) {
		describe(fixture.name, () => {
			it("orders the key list (S3) as the iterator pages rows, ties broken by key", async () => {
				const store = await fixture.open();
				let ties = 0;
				for (const { label, request } of requestsOf(fixture)) {
					const statements = await keyStatements(store, request);
					const { order } = statements;
					const keyRows = await readKeyList(store, statements);
					const keys = keyRows.map(identity);
					const iterated = (await pagedIterator(store, request)).map((result) =>
						iteratorIdentity(store, result),
					);

					expect(keys.length, label).to.be.greaterThan(0);
					expect([...keys].sort(), label).to.deep.equal([...iterated].sort());
					expect(new Set(keys).size, label).to.equal(keys.length);
					// The same sequence of sort values: the orders differ at most
					// inside a tie, which the iterator leaves to the plan.
					const valuesByKey = new Map(
						keyRows.map((row) => [identity(row), sortValues(order, row)]),
					);
					expect(
						iterated.map((key) => valuesByKey.get(key)),
						label,
					).to.deep.equal(keyRows.map((row) => sortValues(order, row)));
					// K is strictly increasing: ties are broken by the primary key.
					for (let i = 1; i < keyRows.length; i++) {
						expect(
							compareInOrder(order, keyRows[i - 1], keyRows[i]),
							`${label} at ${i}`,
						).to.be.lessThan(0);
					}
					// `order` is the builder's own; the direction of the key inside
					// a tie is checked against the request instead.
					ties += expectTiebreak(order, request, keyRows, label);
				}
				expect(ties).to.be.greaterThan(0);
			});

			it("continues pages (S1, S2) in key-list order at page sizes 1, 7 and 256", async () => {
				const store = await fixture.open();
				const stored = await storedValues(store);
				for (const { label, request } of requestsOf(fixture)) {
					const statements = await keyStatements(store, request);
					const keyRows = await readKeyList(store, statements);
					const keys = keyRows.map(identity);
					for (const pageSize of [1, 7, 256]) {
						const { rows, patterns, statementCount } = await pageThrough(
							store,
							statements,
							pageSize,
						);
						const at = `${label}, pages of ${pageSize}`;
						expect(rows.map(identity), at).to.deep.equal(keys);
						expect(
							rows.map((row) => sortValues(statements.order, row)),
							at,
						).to.deep.equal(
							keyRows.map((row) => sortValues(statements.order, row)),
						);
						// The extra row of each page says whether another follows, so
						// no statement reads past the end.
						expect(statementCount, at).to.equal(
							Math.max(1, Math.ceil(keys.length / pageSize)),
						);
						if (pageSize === 1) {
							// Every row but the last is a cursor once.
							expect([...patterns].sort(), at).to.deep.equal(
								[
									...new Set(
										keyRows
											.slice(0, -1)
											.map((row) =>
												nullPattern(cursorOf(statements.order, row)).join(),
											),
									),
								].sort(),
							);
						}
						for (const row of rows) {
							expect(
								await decode(store, tableOf(store, row), row),
								`${at}: ${identity(row)}`,
							).to.deep.equal(stored.get(identity(row)));
						}
					}
				}
			});

			it("re-checks a key list (S4): the given keys that match, flagged after a cursor", async () => {
				const store = await fixture.open();
				const everyKey = await allKeys(store);
				for (const { label, request } of requestsOf(fixture)) {
					const statements = await keyStatements(store, request);
					const { order } = statements;
					const keyRows = await readKeyList(store, statements);
					const matching = new Set(
						keyRows.map((row) => show(row[PRIMARY_KEY_ALIAS])),
					);
					const others = everyKey.filter((key) => !matching.has(show(key)));
					const keySets = [
						// every other match, three rows that do not match, absent keys
						[
							...keyRows
								.filter((_, i) => i % 2 === 0)
								.map((row) => row[PRIMARY_KEY_ALIAS]),
							...others.slice(0, 3),
							...fixture.absent,
						],
						// every key
						[...everyKey, ...fixture.absent],
					];
					for (const asked of keySets) {
						const askedKeys = new Set(asked.map(show));
						const keys = encodeKeyList(asked);
						const expected = keyRows.flatMap((row, i) =>
							askedKeys.has(show(row[PRIMARY_KEY_ALIAS])) ? [{ row, i }] : [],
						);

						const plain = await run(
							store,
							statements.recheckAll.sql,
							bindKeyQuery(statements.recheckAll.bindable, { keys }),
						);
						expect(plain.map(identity), label).to.deep.equal(
							expected.map(({ row }) => identity(row)),
						);
						expect(
							plain.map((row) => sortValues(order, row)),
							label,
						).to.deep.equal(expected.map(({ row }) => sortValues(order, row)));

						for (const position of cursorPositions(order, keyRows)) {
							const cursor = cursorOf(order, keyRows[position]);
							const statement = statements.recheck.get(
								nullPattern(cursor).join(),
							)!;
							const rows = await run(
								store,
								statement.sql,
								bindKeyQuery(statement.bindable, { cursor, keys }),
							);
							expect(
								rows.map(
									(row) =>
										`${identity(row)} ${Number(row[AFTER_CURSOR_ALIAS])}`,
								),
								`${label}, cursor at ${position}`,
							).to.deep.equal(
								expected.map(
									({ row, i }) => `${identity(row)} ${i > position ? 1 : 0}`,
								),
							);
						}
					}
				}
			});
		});
	}

	describe("several root tables", () => {
		it("orders the key list by primary key across root tables", async () => {
			const store = await open(Root, rootDocuments());
			expect(store.rootTables.map((table) => table.ctor)).to.deep.equal([
				First,
				Second,
			]);
			const statements = await keyStatements(store, { query: [] });
			const keyRows = await readKeyList(store, statements);
			expect(keyRows.map(identity)).to.deep.equal([
				'1:"p00"',
				'0:"p01"',
				'1:"p02"',
				'0:"p03"',
				'0:"p04"',
				'1:"p04"',
				'0:"p05"',
				'1:"p06"',
				'0:"p07"',
				'1:"p08"',
				'0:"p09"',
				'1:"p10"',
				'0:"p11"',
			]);

			// The iterator's OFFSET pages order by the first root table's key
			// alias, which is NULL in the second table's rows: they come first.
			const iterated = (await pagedIterator(store, { query: [] })).map(
				(result) => result.id.primitive as string,
			);
			expect(iterated.slice(0, 6).sort()).to.deep.equal([
				"p00",
				"p02",
				"p04",
				"p06",
				"p08",
				"p10",
			]);
			expect(iterated.slice(6)).to.deep.equal([
				"p01",
				"p03",
				"p04",
				"p05",
				"p07",
				"p09",
				"p11",
			]);
		});

		it("orders, pages and re-checks a sort across root tables", async () => {
			const store = await open(Root, rootDocuments());
			const stored = await storedValues(store);
			for (const sort of [[], [desc("rank")], [asc("rank"), desc("id")]]) {
				const request = { query: [], sort };
				const statements = await keyStatements(store, request);
				const { order } = statements;
				const keyRows = await readKeyList(store, statements);
				expect(keyRows).to.have.length(13);
				for (let i = 1; i < keyRows.length; i++) {
					expect(
						compareInOrder(order, keyRows[i - 1], keyRows[i]),
					).to.be.lessThan(0);
				}
				expect(
					expectTiebreak(order, request, keyRows, JSON.stringify(sort)),
				).to.be.greaterThan(0);
				for (const pageSize of [1, 7, 256]) {
					const { rows } = await pageThrough(store, statements, pageSize);
					expect(rows.map(identity)).to.deep.equal(keyRows.map(identity));
					for (const row of rows) {
						expect(await decode(store, tableOf(store, row), row)).to.deep.equal(
							stored.get(identity(row)),
						);
					}
				}
				// A key in both root tables is re-checked in both.
				const keys = encodeKeyList(["p04", "p05", "absent"]);
				const position = keyRows.findIndex(
					(row) => identity(row) === '0:"p04"',
				);
				const cursor = cursorOf(order, keyRows[position]);
				const statement = statements.recheck.get(nullPattern(cursor).join())!;
				const rows = await run(
					store,
					statement.sql,
					bindKeyQuery(statement.bindable, { cursor, keys }),
				);
				expect(
					rows.map(
						(row) => `${identity(row)} ${Number(row[AFTER_CURSOR_ALIAS])}`,
					),
				).to.deep.equal(
					keyRows.flatMap((row, i) =>
						["p04", "p05"].includes(row[PRIMARY_KEY_ALIAS] as string)
							? [`${identity(row)} ${i > position ? 1 : 0}`]
							: [],
					),
				);
			}
		});

		it("skips root tables and branches that lack a field, as the iterator does", async () => {
			const store = await open(Root, rootDocuments());
			const keys = async (request: Request) =>
				(await readKeyList(store, await keyStatements(store, request))).map(
					identity,
				);
			const iterated = async (request: Request) =>
				(await store.iterate(request).all())
					.map((result) => iteratorIdentity(store, result))
					.sort();

			for (const request of [
				// "a" is a field of the first root table only, "b" of the second
				{
					query: [
						new Or([
							new StringMatch({ key: "a", value: "x" }),
							new StringMatch({ key: "absent", value: "x" }),
						]),
					],
				},
				{ query: [], sort: [desc("a")] },
				{
					query: [
						new Or([
							new StringMatch({ key: "a", value: "x" }),
							new StringMatch({ key: "b", value: "y" }),
						]),
					],
					sort: [asc("rank")],
				},
			]) {
				const keyList = await keys(request);
				expect(keyList.length).to.be.greaterThan(0);
				expect([...keyList].sort()).to.deep.equal(await iterated(request));
			}
			expect(
				(await keys({ query: [], sort: [desc("a")] })).every((key) =>
					key.startsWith("0:"),
				),
			).to.be.true;

			for (const request of [
				{ query: [new StringMatch({ key: "absent", value: "x" })] },
				{ query: [], sort: [asc("absent")] },
			]) {
				expect(() =>
					convertSearchRequestToKeyQuery(
						request,
						store.tables,
						store.rootTables,
					),
				).to.throw(MissingFieldError);
				expect(() =>
					convertSearchRequestToPageQuery(
						request,
						store.tables,
						store.rootTables,
					),
				).to.throw(MissingFieldError);
			}
		});

		it("reads rows by key (S5) once per root table", async () => {
			const store = await open(Root, rootDocuments());
			const stored = await storedValues(store);
			const keys = encodeKeyList(["p04", "p05", "p06", "absent"]);
			for (const [ordinal, table] of store.rootTables.entries()) {
				const statement = convertKeysToRowsQuery(table, undefined);
				const rows = await run(
					store,
					statement.sql,
					bindKeyQuery(statement.bindable, { keys }),
				);
				const found = rows
					.map((row) => `${ordinal}:${show(row[PRIMARY_KEY_ALIAS])}`)
					.sort();
				expect(found).to.deep.equal(
					ordinal === 0 ? ['0:"p04"', '0:"p05"'] : ['1:"p04"', '1:"p06"'],
				);
				for (const row of rows) {
					expect(await decode(store, table, row)).to.deep.equal(
						stored.get(`${ordinal}:${show(row[PRIMARY_KEY_ALIAS])}`),
					);
				}
			}
		});

		it("refuses a cursor across root tables that store a sort term as numbers and as TEXT", async () => {
			const store = await open(MixedRank, [
				new NumberRank("n0", 12),
				new NumberRank("n1", 1),
				new NumberRank("n2", 10),
				new NumberRank("n3", 5),
				new TextRank("t0", "10"),
				new TextRank("t1", "3"),
				new TextRank("t2", "abc"),
				new TextRank("t3", "5"),
				new TextRank("t4", ""),
			]);
			const { tables, rootTables } = store;
			expect(rootTables.map((table) => table.ctor)).to.deep.equal([
				NumberRank,
				TextRank,
			]);
			// Storage classes: numbers, then TEXT by its bytes.
			const ascending = [
				"1",
				"5",
				"10",
				"12",
				'""',
				'"10"',
				'"3"',
				'"5"',
				'"abc"',
			];
			for (const sort of [[asc("rank")], [desc("rank")]]) {
				const request = { query: [], sort };
				const label = JSON.stringify(sort);
				const keyList = convertSearchRequestToKeyQuery(
					request,
					tables,
					rootTables,
				);
				expect(
					keyList.order.terms.map((term) => term.type),
					label,
				).to.deep.equal([undefined, "text", "numeric"]);
				// S3 and S1 order by storage class and convert nothing.
				const keyRows = await run(
					store,
					keyList.sql,
					bindKeyQuery(keyList.bindable, {}),
				);
				expect(
					keyRows.map((row) => show(row[sortTermAlias(0)])),
					label,
				).to.deep.equal(
					sort[0].direction === SortDirection.ASC
						? ascending
						: [...ascending].reverse(),
				);
				const head = convertSearchRequestToPageQuery(
					request,
					tables,
					rootTables,
				);
				expect(
					(
						await run(
							store,
							head.sql,
							bindKeyQuery(head.bindable, { limit: Infinity }),
						)
					).map(identity),
					label,
				).to.deep.equal(keyRows.map(identity));
				// A cursor is converted to each column's type: TEXT '10' would
				// compare as 10 with the numbers, and 5 as '5' with the TEXT.
				const after = keyList.order.terms.map(() => false);
				for (const build of [
					() =>
						convertSearchRequestToPageQuery(request, tables, rootTables, {
							after,
						}),
					() =>
						convertSearchRequestToKeyQuery(request, tables, rootTables, {
							after,
						}),
					() =>
						convertSearchRequestToKeyQuery(request, tables, rootTables, {
							restrictToKeys: true,
							after,
						}),
				]) {
					expect(build, label).to.throw(KeyTypeMismatchError, '"__s0"');
				}
				// A key list compares only keys, TEXT in both root tables.
				const recheck = convertSearchRequestToKeyQuery(
					request,
					tables,
					rootTables,
					{ restrictToKeys: true },
				);
				const rows = await run(
					store,
					recheck.sql,
					bindKeyQuery(recheck.bindable, {
						keys: encodeKeyList(["n2", "t0", "absent"]),
					}),
				);
				expect(rows.map(identity), label).to.deep.equal(
					keyRows
						.map(identity)
						.filter((key) => key === '0:"n2"' || key === '1:"t0"'),
				);
			}

			// Without a sort K leaves `rank` out, and a cursor continues across
			// both root tables.
			const statements = await keyStatements(store, { query: [] });
			const { rows } = await pageThrough(store, statements, 2);
			expect(rows.map(identity)).to.deep.equal(
				(await readKeyList(store, statements)).map(identity),
			);
		});

		it("refuses a key list across root tables that store their keys differently", async () => {
			// Numeric keys in one root table, TEXT keys in the other.
			const store = await open(MixedKey, [
				new NumberKey(1),
				new NumberKey(10),
				new NumberKey(30),
				new TextKey("10"),
				new TextKey("3"),
				new TextKey("abc"),
			]);
			const { tables, rootTables } = store;
			expect(rootTables.map((table) => table.ctor)).to.deep.equal([
				NumberKey,
				TextKey,
			]);
			const keyList = convertSearchRequestToKeyQuery(
				{ query: [] },
				tables,
				rootTables,
			);
			expect(keyList.order.terms.map((term) => term.type)).to.deep.equal([
				undefined,
				"numeric",
			]);
			expect((await run(store, keyList.sql, [])).map(identity)).to.deep.equal([
				"0:1",
				"0:10",
				"0:30",
				'1:"10"',
				'1:"3"',
				'1:"abc"',
			]);
			// TEXT '10' would match the key 10 of the first table, and compare
			// as 10 after a cursor there.
			expect(() =>
				convertSearchRequestToKeyQuery({ query: [] }, tables, rootTables, {
					restrictToKeys: true,
				}),
			).to.throw(KeyTypeMismatchError, "key list");
			expect(() =>
				convertSearchRequestToPageQuery({ query: [] }, tables, rootTables, {
					after: [false, false],
				}),
			).to.throw(KeyTypeMismatchError, '"__pk"');
			// Rows by key read one root table, with that table's keys.
			for (const [ordinal, key, found] of [
				[0, 10, "10"],
				[1, "10", '"10"'],
			] as const) {
				const statement = convertKeysToRowsQuery(
					rootTables[ordinal],
					undefined,
				);
				expect(
					(
						await run(
							store,
							statement.sql,
							bindKeyQuery(statement.bindable, { keys: encodeKeyList([key]) }),
						)
					).map((row) => show(row[PRIMARY_KEY_ALIAS])),
				).to.deep.equal([found]);
			}
			// A sort on a field that only the first root table has reads one
			// root table, where a cursor and a key list compare alike.
			const request = { query: [], sort: [desc("n")] };
			const statements = await keyStatements(store, request);
			const keyRows = await readKeyList(store, statements);
			// n = id % 7: 3, 2 and 1
			expect(keyRows.map(identity)).to.deep.equal(["0:10", "0:30", "0:1"]);
			const { rows } = await pageThrough(store, statements, 1);
			expect(rows.map(identity)).to.deep.equal(keyRows.map(identity));
			expect(
				(
					await run(
						store,
						statements.recheckAll.sql,
						bindKeyQuery(statements.recheckAll.bindable, {
							keys: encodeKeyList([1, 30]),
						}),
					)
				).map(identity),
			).to.deep.equal(["0:30", "0:1"]);

			// u64 keys are stored shifted into the signed range, u32 keys are
			// not: both are numbers, so a cursor compares alike, but one stored
			// value is a different key in each table.
			const shifted = await open(ShiftedKey, [
				new U64Key(5n),
				new U64Key((1n << 63n) + 5n),
				new U32Key(5),
				new U32Key(7),
			]);
			const shiftedList = convertSearchRequestToKeyQuery(
				{ query: [] },
				shifted.tables,
				shifted.rootTables,
			);
			const shiftedRows = await run(shifted, shiftedList.sql, []);
			expect(shiftedRows.map(identity)).to.deep.equal([
				`0:${u64ToI64(5n)}`,
				"0:5",
				"1:5",
				"1:7",
			]);
			expect(
				(
					await pageThrough(shifted, pageStatements(shifted, { query: [] }), 1)
				).rows.map(identity),
			).to.deep.equal(shiftedRows.map(identity));
			expect(() =>
				convertSearchRequestToKeyQuery(
					{ query: [] },
					shifted.tables,
					shifted.rootTables,
					{ restrictToKeys: true },
				),
			).to.throw(KeyTypeMismatchError, "u64");

			// BLOB keys in one root table, TEXT keys in the other: a cursor
			// compares alike (TEXT before BLOB, nothing converted), but a key
			// list sends x'0102' and '0102' as one JSON string.
			expect(encodeKeyList([new Uint8Array([1, 2])])).to.equal(
				encodeKeyList(["0102"]),
			);
			const hexKeys = await open(HexKey, [
				new BytesKey([1, 2]),
				new BytesKey([0]),
				new HexTextKey("0102"),
				new HexTextKey("ff"),
			]);
			expect(hexKeys.rootTables.map((table) => table.ctor)).to.deep.equal([
				BytesKey,
				HexTextKey,
			]);
			const hexList = convertSearchRequestToKeyQuery(
				{ query: [] },
				hexKeys.tables,
				hexKeys.rootTables,
			);
			expect(hexList.order.terms.map((term) => term.type)).to.deep.equal([
				undefined,
				"numeric",
			]);
			const hexRows = await run(hexKeys, hexList.sql, []);
			expect(hexRows.map(identity)).to.deep.equal([
				'1:"0102"',
				'1:"ff"',
				"0:x'00'",
				"0:x'0102'",
			]);
			expect(
				(
					await pageThrough(hexKeys, pageStatements(hexKeys, { query: [] }), 1)
				).rows.map(identity),
			).to.deep.equal(hexRows.map(identity));
			expect(() =>
				convertSearchRequestToKeyQuery(
					{ query: [] },
					hexKeys.tables,
					hexKeys.rootTables,
					{ restrictToKeys: true },
				),
			).to.throw(KeyTypeMismatchError, "key list");
		});

		it("continues and re-checks across root tables whose key terms compare alike", async () => {
			const store = await open(AlikeTerms, [
				new AlikeFirst("a0", 1, "10"),
				new AlikeFirst("a1", 5, "abc"),
				new AlikeFirst("a2", 12, ""),
				new AlikeSecond("b0", 0.5, [0x31]),
				new AlikeSecond("b1", 5, [0x61, 0x62, 0x63]),
				new AlikeSecond("b2", 7.25, []),
				new AlikeSecond("b3", 1e300, [5]),
			]);
			for (const sort of [
				[asc("a")],
				[desc("a")],
				[desc("b")],
				[asc("b"), desc("a")],
			]) {
				const request = { query: [], sort };
				const label = JSON.stringify(sort);
				const statements = await keyStatements(store, request);
				const { order } = statements;
				const keyRows = await readKeyList(store, statements);
				expect(keyRows, label).to.have.length(7);
				for (let i = 1; i < keyRows.length; i++) {
					expect(
						compareInOrder(order, keyRows[i - 1], keyRows[i]),
						`${label} at ${i}`,
					).to.be.lessThan(0);
				}
				for (const pageSize of [1, 3]) {
					const { rows } = await pageThrough(store, statements, pageSize);
					expect(
						rows.map(identity),
						`${label}, pages of ${pageSize}`,
					).to.deep.equal(keyRows.map(identity));
				}
				const keys = encodeKeyList(
					keyRows.map((row) => row[PRIMARY_KEY_ALIAS]),
				);
				for (const [position, row] of keyRows.entries()) {
					const cursor = cursorOf(order, row);
					const statement = statements.recheck.get(nullPattern(cursor).join())!;
					const rows = await run(
						store,
						statement.sql,
						bindKeyQuery(statement.bindable, { cursor, keys }),
					);
					expect(
						rows.map(
							(recheck) =>
								`${identity(recheck)} ${Number(recheck[AFTER_CURSOR_ALIAS])}`,
						),
						`${label}, cursor at ${position}`,
					).to.deep.equal(
						keyRows.map(
							(other, i) => `${identity(other)} ${i > position ? 1 : 0}`,
						),
					);
				}
			}
		});
	});

	describe("plans", () => {
		@variant("key_query_plan_string")
		class PlanString {
			@id({ type: "string" })
			id: string;

			@field({ type: "u32" })
			rank: number;

			constructor(i: number) {
				this.id = `k${String(i).padStart(3, "0")}`;
				this.rank = i % 3;
			}
		}

		@variant("key_query_plan_u64")
		class PlanU64 {
			@id({ type: "u64" })
			id: bigint;

			@field({ type: "u32" })
			rank: number;

			constructor(i: number) {
				this.id = BigInt(i) * 1000n;
				this.rank = i % 3;
			}
		}

		@variant("key_query_plan_bytes")
		class PlanBytes {
			@id({ type: Uint8Array })
			id: Uint8Array;

			@field({ type: "u32" })
			rank: number;

			constructor(i: number) {
				this.id = new Uint8Array([i >> 8, i & 255, 7]);
				this.rank = i % 3;
			}
		}

		// SQLite plans by table statistics: at this size it reads an INTEGER
		// primary key through the key list instead of scanning the table.
		const ROWS = 500;

		// Every step that sorts rows for ORDER BY or GROUP BY: all of the order,
		// or the part of it that the index does not serve.
		const SORTER_STEP =
			/USE TEMP B-TREE FOR (LAST TERM OF |RIGHT PART OF )?(ORDER|GROUP) BY/;

		for (const { name, schema, path } of [
			{ name: "string", schema: PlanString, path: "PRIMARY KEY (id" },
			{ name: "u64", schema: PlanU64, path: "INTEGER PRIMARY KEY (rowid" },
			{ name: "bytes", schema: PlanBytes, path: "PRIMARY KEY (id" },
		] as const) {
			it(`reads ${name} keys through the primary key`, async () => {
				const { store: index } = await setup<any>({ schema });
				const store = index as unknown as Store;
				await store.putBatch(
					Array.from({ length: ROWS }, (_, i) => new schema(i)),
				);
				const table = store.rootTables[0];
				const root = table.name;
				const keyRows = await readKeyList(
					store,
					await keyStatements(store, { query: [] }),
				);
				const keys = encodeKeyList(
					[3, 250, 499].map((i) => keyRows[i][PRIMARY_KEY_ALIAS]),
				);
				const explain = async (
					statement: { sql: string; bindable: unknown[] },
					cursor: unknown[] = [],
				) =>
					(
						await run(
							store,
							`EXPLAIN QUERY PLAN ${statement.sql}`,
							bindKeyQuery(statement.bindable, { cursor, keys, limit: 11 }),
						)
					).map((row) => String(row.detail));

				for (const [label, request] of Object.entries({
					"key order": { query: [] },
					"key order DESC": { query: [], sort: [desc("id")] },
					"rank = 1": { query: [integer("rank", Compare.Equal, 1)] },
					"rank > 0": { query: [integer("rank", Compare.Greater, 0)] },
					"rank ASC": { query: [], sort: [asc("rank")] },
					"rank DESC": { query: [], sort: [desc("rank")] },
				} as Record<string, Request>)) {
					const scope = planner(store, request);
					const { tables, rootTables } = store;
					const keyList = convertSearchRequestToKeyQuery(
						request,
						tables,
						rootTables,
						{ planner: scope },
					);
					const { order } = keyList;
					const after = order.terms.map(() => false);
					const continuation = convertSearchRequestToPageQuery(
						request,
						tables,
						rootTables,
						{ planner: scope, after },
					);
					const recheck = convertSearchRequestToKeyQuery(
						request,
						tables,
						rootTables,
						{ planner: scope, restrictToKeys: true, after },
					);
					const rowsByKey = convertKeysToRowsQuery(table, undefined, {
						planner: scope,
					});
					await scope.beforePrepare();
					const cursor = [
						...sortTerms(order).map(() => 1),
						keyRows[250][PRIMARY_KEY_ALIAS],
					];

					if (label.startsWith("key order")) {
						const operator =
							order.terms[0].direction === SortDirection.DESC ? "<" : ">";
						expect(
							await explain(continuation, cursor),
							`${label}: S2`,
						).to.deep.equal([`SEARCH ${root} USING ${path}${operator}?)`]);
					}
					if (
						["key order", "key order DESC", "rank ASC", "rank DESC"].includes(
							label,
						)
					) {
						// K is the key, or one sort term and the key in that term's
						// direction: the primary key, or an index on the term (which
						// ends in the key), serves it in either direction.
						const plan = await explain(keyList);
						expect(
							plan.filter((detail) => SORTER_STEP.test(detail)),
							`${label}: S3 ${JSON.stringify(plan)}`,
						).to.deep.equal([]);
					}
					for (const [statementName, statement, statementCursor] of [
						["S4", recheck, cursor],
						["S5", rowsByKey, []],
					] as const) {
						const plan = await explain(statement, [...statementCursor]);
						const at = `${label}: ${statementName} ${JSON.stringify(plan)}`;
						expect(
							plan.some((detail) =>
								new RegExp(`^SCAN ${root}( |$)`).test(detail),
							),
							at,
						).to.be.false;
						expect(
							plan.some((detail) =>
								new RegExp(`^SEARCH ${root} USING .*\\((id|rowid)=\\?`).test(
									detail,
								),
							),
							at,
						).to.be.true;
					}
				}
			});
		}
	});

	describe("rows by key (S5)", () => {
		@variant("key_query_coordinates")
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

		@variant("key_query_nested_coordinates")
		class NestedCoordinates {
			@field({ type: vec("u64") })
			coords: bigint[];

			constructor(coords: bigint[]) {
				this.coords = coords;
			}
		}

		@variant("key_query_deep_coordinates")
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

		const coordinates = () => [
			new Coordinates("a", [0n, 2n, 2n, 40n, MAX_U64], false, 5),
			new Coordinates("b", [2n, 3n, 100n], false, 4),
			new Coordinates("c", [10n, MAX_U64], false, 3),
			new Coordinates("d", [], true, 2),
			new Coordinates("e", [51n, 52n], false, 1),
			new Coordinates("f", [1n << 63n, MAX_U64 - 1n], false, 0),
		];

		const compareCoordinate = (compare: Compare, value: bigint) =>
			new IntegerCompare({ key: "coords", compare, value });

		// Inserted in reverse: reads must not depend on insertion order.
		const openReversed = <T extends Record<string, any>>(
			schema: AbstractType<T>,
			values: T[],
		) => open(schema, [...values].reverse());

		const keysOf = async (store: Store, request: Request) =>
			(await readKeyList(store, await keyStatements(store, request))).map(
				(row) => row[PRIMARY_KEY_ALIAS],
			);

		const rowsByKey = async (store: Store, keys: unknown[]) => {
			const table = store.rootTables[0];
			const scope = planner(store, { query: [] });
			const statement = convertKeysToRowsQuery(table, undefined, {
				planner: scope,
			});
			await scope.beforePrepare();
			const rows = await run(
				store,
				statement.sql,
				bindKeyQuery(statement.bindable, { keys: encodeKeyList(keys) }),
			);
			const values = await Promise.all(
				rows.map((row) => decode(store, table, row)),
			);
			return values.sort((a: any, b: any) => (a.id < b.id ? -1 : 1));
		};

		it("uses child matches for selection without filtering hydrated values", async () => {
			const values = coordinates();
			const store = await openReversed(Coordinates, values);
			const keys = await keysOf(store, {
				query: [compareCoordinate(Compare.Equal, 2n)],
			});
			// "a" holds 2 twice and is listed once.
			expect(keys).to.deep.equal(["a", "b"]);
			expect(await rowsByKey(store, [...keys, "absent"])).to.deep.equal(
				values.slice(0, 2),
			);
		});

		it("keeps disjoint OR ranges and empty boundary rows", async () => {
			const values = coordinates();
			const expected = values.filter((value) => value.id !== "e");
			const store = await openReversed(Coordinates, values);
			const request = {
				query: [
					new Or([
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
				],
			};
			const keys = await keysOf(store, request);
			expect(keys).to.deep.equal(expected.map((value) => value.id));
			expect(await rowsByKey(store, keys)).to.deep.equal(expected);

			const statements = await keyStatements(store, request);
			const { rows } = await pageThrough(store, statements, 2);
			expect(
				await Promise.all(
					rows.map((row) => decode(store, store.rootTables[0], row)),
				),
			).to.deep.equal(expected);
		});

		it("keeps deeply nested array reconstruction", async () => {
			const values = [
				new DeepCoordinates("a", [[0n, 2n, 2n], [MAX_U64]]),
				new DeepCoordinates("b", [[9n], []]),
			];
			const store = await openReversed(DeepCoordinates, values);
			const keys = await keysOf(store, { query: [] });
			expect(keys).to.deep.equal(["a", "b"]);
			expect(await rowsByKey(store, keys)).to.deep.equal(values);

			const statements = await keyStatements(store, { query: [] });
			const { rows } = await pageThrough(store, statements, 1);
			expect(
				await Promise.all(
					rows.map((row) => decode(store, store.rootTables[0], row)),
				),
			).to.deep.equal(values);
		});
	});

	describe("keyset predicate", () => {
		const { ASC, DESC } = SortDirection;

		it("is a row value when one direction orders every row", () => {
			expect(
				buildKeysetPredicate(["a"], [ASC], [false], [false]),
			).to.deep.equal({ sql: "a > ?", binds: [0] });
			expect(
				buildKeysetPredicate(
					["a", "b"],
					[ASC, ASC],
					[false, false],
					[false, false],
				),
			).to.deep.equal({ sql: "(a, b) > (?, ?)", binds: [0, 1] });
			expect(
				buildKeysetPredicate(
					["a", "b"],
					[DESC, DESC],
					[false, false],
					[false, false],
				),
			).to.deep.equal({ sql: "(a, b) < (?, ?)", binds: [0, 1] });
			// In ASC a NULL sorts first, so a row value that is NULL for a row
			// with NULL drops a row that is before the cursor anyway.
			expect(
				buildKeysetPredicate(
					["l", "k"],
					[ASC, ASC],
					[true, false],
					[false, false],
				),
			).to.deep.equal({ sql: "(l, k) > (?, ?)", binds: [0, 1] });
		});

		it("expands NULL-aware terms, led by a range when it keeps every later row", () => {
			// NULL cursor term in ASC: every non-NULL value follows it.
			expect(
				buildKeysetPredicate(
					["l", "k"],
					[ASC, ASC],
					[true, false],
					[true, false],
				),
			).to.deep.equal({
				sql: "(l IS NOT NULL OR (l IS NULL AND k > ?))",
				binds: [1],
			});
			// In DESC a NULL sorts last: it follows every value.
			expect(
				buildKeysetPredicate(
					["l", "k"],
					[DESC, DESC],
					[true, false],
					[false, false],
				),
			).to.deep.equal({
				sql: "((l < ? OR l IS NULL) OR (l = ? AND k < ?))",
				binds: [0, 0, 1],
			});
			expect(
				buildKeysetPredicate(
					["l", "k"],
					[DESC, DESC],
					[true, false],
					[true, false],
				),
			).to.deep.equal({ sql: "(0 OR (l IS NULL AND k < ?))", binds: [1] });
			expect(
				buildKeysetPredicate(
					["r", "l", "k"],
					[DESC, ASC, ASC],
					[false, true, false],
					[false, false, false],
				),
			).to.deep.equal({
				sql: "r <= ? AND (r < ? OR (r = ? AND (l > ? OR (l = ? AND k > ?))))",
				binds: [0, 0, 0, 1, 1, 2],
			});
			expect(
				buildKeysetPredicate(
					["l", "r", "k"],
					[ASC, DESC, DESC],
					[true, false, false],
					[false, false, false],
				),
			).to.deep.equal({
				sql: "l >= ? AND (l > ? OR (l = ? AND (r < ? OR (r = ? AND k < ?))))",
				binds: [0, 0, 0, 1, 1, 2],
			});
		});

		it("rejects a NULL in a term that cannot hold one, and mismatched terms", () => {
			expect(() =>
				buildKeysetPredicate(["a"], [ASC], [false], [true]),
			).to.throw("Key term 0 cannot be NULL");
			expect(() =>
				buildKeysetPredicate(["a", "b"], [ASC], [false, false], [false, false]),
			).to.throw();
			expect(() => buildKeysetPredicate([], [], [], [])).to.throw();
		});
	});

	describe("key lists", () => {
		it("round-trips integers exactly, a number and a bigint alike", async () => {
			const store = await open(Document, []);
			const integers = [
				MIN_I64,
				-(1n << 53n) - 1n,
				-1n,
				0n,
				1n,
				(1n << 53n) + 1n,
				MAX_I64,
			];
			const rows = await run(
				store,
				"SELECT value, typeof(value) AS type FROM json_each(?)",
				[encodeKeyList(integers)],
			);
			expect(rows.map((row) => BigInt(row.value as bigint))).to.deep.equal(
				integers,
			);
			expect(rows.map((row) => row.type)).to.deep.equal(
				integers.map(() => "integer"),
			);
			expect(encodeKeyList([7, 2 ** 60, -3])).to.equal(
				encodeKeyList([7n, 1n << 60n, -3n]),
			);
			expect(encodeKeyList([])).to.equal("[]");
		});

		it("round-trips strings and bytes", async () => {
			const store = await open(Document, []);
			const strings = ["", "a", 'quote " and \\', "ü", "😀", " "];
			expect(
				(
					await run(store, "SELECT value FROM json_each(?)", [
						encodeKeyList(strings),
					])
				).map((row) => row.value),
			).to.deep.equal(strings);

			const bytes = [[0], [0, 15, 255], [255, 0]].map(
				(value) => new Uint8Array(value),
			);
			const blobs = await run(
				store,
				"SELECT unhex(value) AS value, typeof(unhex(value)) AS type FROM json_each(?)",
				[encodeKeyList(bytes)],
			);
			expect(blobs.map((row) => hex(row.value as Uint8Array))).to.deep.equal(
				bytes.map(hex),
			);
			expect(blobs.map((row) => row.type)).to.deep.equal(
				bytes.map(() => "blob"),
			);
		});

		it("matches string keys as SQLite stores them: lone surrogates and U+0000", async () => {
			const ids = [
				"\ud800x",
				"x\udfff",
				"\udfff\ud800",
				"a\u0000b",
				"\u0000",
				"😀",
				"plain",
			];
			const store = await open(
				TextKeyDocument,
				ids.map((key, i) => new TextKeyDocument(key, i)),
			);
			const table = store.rootTables[0];
			const rowsByKey = convertKeysToRowsQuery(table, undefined);
			const recheck = convertSearchRequestToKeyQuery(
				{ query: [] },
				store.tables,
				store.rootTables,
				{ restrictToKeys: true },
			);
			// Compared as hex in SQL: the WASM engine reads TEXT only up to U+0000.
			const keysOf = async (sql: string, values: unknown[]) =>
				(
					await run(
						store,
						`SELECT hex("${PRIMARY_KEY_ALIAS}") AS h FROM (${sql})`,
						values,
					)
				).map((row) => row.h);
			for (const key of ids) {
				const at = JSON.stringify(key);
				// Both engines bind a lone surrogate as U+FFFD, as TextEncoder
				// encodes it, and U+0000 as a 0 byte.
				const stored = hex(utf8.encode(key)).toUpperCase();
				expect(
					(
						await run(
							store,
							`SELECT hex(id) AS h FROM ${table.name} WHERE id = ?`,
							[key],
						)
					).map((row) => row.h),
					at,
				).to.deep.equal([stored]);
				const keys = encodeKeyList([key]);
				expect(
					(
						await run(store, "SELECT hex(value) AS h FROM json_each(?)", [keys])
					).map((row) => row.h),
					at,
				).to.deep.equal([stored]);
				expect(
					await keysOf(
						rowsByKey.sql,
						bindKeyQuery(rowsByKey.bindable, { keys }),
					),
					`${at}: S5`,
				).to.deep.equal([stored]);
				expect(
					await keysOf(recheck.sql, bindKeyQuery(recheck.bindable, { keys })),
					`${at}: S4`,
				).to.deep.equal([stored]);
			}
		});

		it("rejects keys SQLite cannot hold", () => {
			expect(() => encodeKeyList([MAX_I64 + 1n])).to.throw(RangeError);
			expect(() => encodeKeyList([MIN_I64 - 1n])).to.throw(RangeError);
			for (const key of [null, undefined, NaN, Infinity, true, {}]) {
				expect(() => encodeKeyList([key]), String(key)).to.throw();
			}
		});

		it("reads BLOB keys through unhex", async () => {
			const bytes = await open(BytesDocument, []);
			const strings = await open(Document, []);
			const integers = await open(U64Document, []);
			expect(keyListSubquery(bytes.rootTables[0])).to.equal(
				"SELECT unhex(value) FROM json_each(?)",
			);
			expect(keyListSubquery(strings.rootTables[0])).to.equal(
				"SELECT value FROM json_each(?)",
			);
			expect(keyListSubquery(integers.rootTables[0])).to.equal(
				"SELECT value FROM json_each(?)",
			);
		});
	});

	describe("parameters", () => {
		it("binds the cursor, the key list and the limit in place", () => {
			expect(
				bindKeyQuery(
					[
						1,
						"x",
						KeyQueryParameter.cursor(1),
						KeyQueryParameter.keys,
						KeyQueryParameter.cursor(0),
						KeyQueryParameter.limit,
					],
					{ cursor: ["c0", 5n], keys: "[1]", limit: 8 },
				),
			).to.deep.equal([1, "x", 5n, "[1]", "c0", 8]);
			// SQLite reads to the end with a negative limit.
			expect(
				bindKeyQuery([KeyQueryParameter.limit], { limit: Infinity }),
			).to.deep.equal([-1]);
		});

		it("refuses a missing value", () => {
			expect(() =>
				bindKeyQuery([KeyQueryParameter.cursor(1)], { cursor: ["a", null] }),
			).to.throw("Missing a cursor value for key term 1");
			expect(() => bindKeyQuery([KeyQueryParameter.keys], {})).to.throw(
				"Missing the key list",
			);
			expect(() =>
				bindKeyQuery([KeyQueryParameter.limit], { limit: 1.5 }),
			).to.throw("Invalid limit");
			expect(() => bindKeyQuery([KeyQueryParameter.limit], {})).to.throw(
				"Invalid limit",
			);
		});

		it("reads every row of a head page bound to Infinity", async () => {
			const store = await open(Document, documents());
			const statements = await keyStatements(store, {
				query: [],
				sort: [desc("rank")],
			});
			const rows = await run(
				store,
				statements.head.sql,
				bindKeyQuery(statements.head.bindable, { limit: Infinity }),
			);
			expect(rows.map(identity)).to.deep.equal(
				(await readKeyList(store, statements)).map(identity),
			);
		});
	});
});
