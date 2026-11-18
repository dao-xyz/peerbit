import { field, variant, vec } from "@dao-xyz/borsh";
import {
	Compare,
	type IndexIterator,
	IntegerCompare,
	Or,
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import type { SQLiteIndex } from "../src/engine.js";
import { create } from "../src/index.js";
import type { Statement } from "../src/types.js";
import { setup } from "./utils.js";

@variant("mat_doc")
class Doc {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	@field({ type: vec("u64") })
	coords: bigint[];

	constructor(id: string, rank = 0, coords: bigint[] = []) {
		this.id = id;
		this.rank = rank;
		this.coords = coords;
	}
}

@variant("mat_u64")
class U64Doc {
	@id({ type: "u64" })
	id: bigint;

	@field({ type: "u32" })
	rank: number;

	constructor(id: bigint, rank = 0) {
		this.id = id;
		this.rank = rank;
	}
}

@variant("mat_bytes")
class BytesDoc {
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u32" })
	rank: number;

	constructor(id: Uint8Array, rank = 0) {
		this.id = id;
		this.rank = rank;
	}
}

@variant("mat_row")
class Row {
	@id({ type: "u32" })
	id: number;

	@field({ type: "u32" })
	value: number;

	constructor(id: number, value = 0) {
		this.id = id;
		this.value = value;
	}
}

abstract class Base {}

@variant(0)
class VariantA extends Base {
	@field({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	constructor(id: string, rank = 0) {
		super();
		this.id = id;
		this.rank = rank;
	}
}

@variant(1)
class VariantB extends Base {
	@field({ type: "string" })
	id: string;

	@field({ type: "u32" })
	rank: number;

	@field({ type: "string" })
	extra: string;

	constructor(id: string, rank = 0) {
		super();
		this.id = id;
		this.rank = rank;
		this.extra = "b";
	}
}

// Root tables that store their keys differently: a u64 key is stored shifted
// into the signed range, a u32 key is not.
abstract class ShiftedRoot {}

@variant("mat_u64_root")
class U64Root extends ShiftedRoot {
	@id({ type: "u64" })
	id: bigint;

	@field({ type: "u32" })
	rank: number;

	constructor(id: bigint, rank: number) {
		super();
		this.id = id;
		this.rank = rank;
	}
}

@variant("mat_u32_root")
class U32Root extends ShiftedRoot {
	@id({ type: "u32" })
	id: number;

	@field({ type: "u32" })
	rank: number;

	constructor(id: number, rank: number) {
		super();
		this.id = id;
		this.rank = rank;
	}
}

type Journal = { capacity: number; readonly readerCount: number };
const journalOf = (store: unknown) =>
	(store as { changeJournal: Journal }).changeJournal;

const range = (start: number, end: number) =>
	Array.from({ length: end - start }, (_, i) => i + start);

const pad = (i: number) => String(i).padStart(6, "0");

/** Key lists (S3): ordered keys of a search, no rows, no LIMIT. */
const isKeyList = (sql: string) =>
	/"__pk"/.test(sql) && !/json_each/.test(sql) && !/\blimit \?\s*$/i.test(sql);
/** Re-checks (S4): the search restricted to a key list, in order. */
const isRecheck = (sql: string) =>
	/json_each/.test(sql) && /ORDER BY/.test(sql);
/** Rows by key (S5). */
const isRowsByKey = (sql: string) =>
	/json_each/.test(sql) && !/ORDER BY/.test(sql);
/** Pages (S1, S2): the search's rows in order, `LIMIT ?`. */
const isPage = (sql: string) =>
	/"__pk"/.test(sql) && /\blimit \?\s*$/i.test(sql.trim());
/** The count of the matching rows. */
const isCount = (sql: string) => /\bcount\s*\(/i.test(sql);

/** What each statement an iterator ran does, in order. */
const kinds = (runs: string[]) =>
	runs.map((sql) =>
		isPage(sql)
			? "page"
			: isKeyList(sql)
				? "keys"
				: isRecheck(sql)
					? "recheck"
					: isRowsByKey(sql)
						? "rows"
						: isCount(sql)
							? "count"
							: "other",
	);

/**
 * Wraps `all` of every statement the index prepares from now on, and of
 * those it prepared before: an iterator prepares each statement once.
 */
const wrapStatements = (
	store: SQLiteIndex<any>,
	wrap: (sql: string, statement: Statement) => void,
) => {
	const db = store.properties.db;
	const prepare = db.prepare.bind(db);
	const wrapped = new Set<Statement>();
	const once = (sql: string, statement: Statement) => {
		if (!wrapped.has(statement)) {
			wrapped.add(statement);
			wrap(sql, statement);
		}
		return statement;
	};
	db.prepare = async (sql: string, key?: string) =>
		once(sql, await prepare(sql, key));
	const prepared = db.statements as unknown as Map<string, Statement>;
	if (typeof prepared.entries === "function") {
		for (const [key, statement] of prepared.entries()) {
			// Iterators prepare their statements with the SQL as the id.
			once(key, statement);
		}
	}
};

/** Counts the rows every statement returns and the OFFSET of paged ones. */
const instrument = (store: SQLiteIndex<any>) => {
	const stats = {
		rows: 0,
		offsets: [] as number[],
		/** The SQL of each statement run, in order (counts included). */
		runs: [] as string[],
	};
	wrapStatements(store, (sql, statement) => {
		const all = statement.all.bind(statement);
		statement.all = async (values?: any[]) => {
			const rows = await all(values as any);
			stats.rows += rows.length;
			stats.runs.push(sql);
			if (/offset \?\s*$/i.test(sql.trim()) && values?.length) {
				stats.offsets.push(Number(values[values.length - 1]));
			}
			return rows;
		};
		if (isCount(sql)) {
			const get = statement.get.bind(statement);
			statement.get = async (values?: any[]) => {
				const row = await get(values as any);
				stats.runs.push(sql);
				return row;
			};
		}
	});
	return stats;
};

/** Holds or fails one read of the statements that `target` selects. */
const interceptReads = (
	store: SQLiteIndex<any>,
	target: (sql: string) => boolean,
	action: {
		hold?: Promise<void>;
		fail?: Error;
		/** reads to let through before acting */
		after?: number;
	},
) => {
	let entered!: () => void;
	const reached = new Promise<void>((resolve) => (entered = resolve));
	let used = false;
	let reads = 0;
	wrapStatements(store, (sql, statement) => {
		if (!target(sql)) {
			return;
		}
		const all = statement.all.bind(statement);
		statement.all = async (values?: any[]) => {
			const rows = await all(values as any);
			if (!used && reads++ >= (action.after ?? 0)) {
				used = true;
				entered();
				if (action.hold) await action.hold;
				if (action.fail) throw action.fail;
			}
			return rows;
		};
	});
	return reached;
};

const idsOf = (rows: { id: { primitive: unknown } }[]) =>
	rows.map((row) => String(row.id.primitive));

/** The error a call throws, or undefined. */
const failed = (call: unknown) =>
	Promise.resolve(call).then(
		() => undefined,
		(error: unknown) => error,
	);

const drain = async (
	iterator: IndexIterator<any, any>,
	page: number,
	between?: (pageIndex: number) => Promise<void>,
) => {
	const ids: string[] = [];
	let pages = 0;
	while (true) {
		const results = await iterator.next(page);
		pages++;
		ids.push(...results.map((x) => String(x.id.primitive)));
		if (results.length === 0 || iterator.done()) {
			break;
		}
		await between?.(pages);
	}
	return ids;
};

describe("materialized iterator", () => {
	let index: Awaited<ReturnType<typeof setup<any>>>;

	afterEach(async () => {
		await index?.store.stop();
	});

	it("a delete queued before the next page does not skip a live row", async () => {
		index = await setup({ schema: Doc }, create);
		for (let i = 0; i < 5; i++) {
			await index.store.put(new Doc(String(i)));
		}
		const iterator = index.store.iterate({
			query: [],
			sort: [new Sort({ key: "id", direction: SortDirection.ASC })],
		});
		expect((await iterator.next(2)).map((x) => x.value.id)).to.deep.equal([
			"0",
			"1",
		]);
		// A batch holds the connection; the delete queues behind it and the
		// page behind the delete, so the page runs after the delete although
		// it was requested before the delete committed.
		const writes = index.store.putBatch!(
			Array.from({ length: 3000 }, (_, i) => new Doc("z" + pad(i))),
		);
		const deleted = index.store.del({
			query: new StringMatch({
				key: "id",
				value: "0",
				method: StringMatchMethod.exact,
			}),
		});
		for (let i = 0; i < 20; i++) {
			await Promise.resolve(); // let the delete plan and queue
		}
		const page = iterator.next(2);
		await Promise.all([writes, deleted]);
		expect((await page).map((x) => x.value.id)).to.deep.equal(["2", "3"]);
		expect((await iterator.next(2)).map((x) => x.value.id)).to.deep.equal([
			"4",
			"z000000",
		]);
	});

	it("reads each row a bounded number of times when a write lands between pages", async () => {
		index = await setup({ schema: Doc }, create);
		const n = 1000;
		await index.store.putBatch!(
			Array.from({ length: n }, (_, i) => new Doc(pad(i), i)),
		);
		const stats = instrument(index.store as SQLiteIndex<any>);
		const ids = await drain(index.store.iterate({}), 50, async (p) => {
			await index.store.put(new Doc(pad(p * 50 - 1), -1)); // already yielded
		});
		expect(ids).to.have.length(n);
		expect(new Set(ids).size).to.equal(n);
		// a key list, each row once by key, and one re-check per write
		expect(stats.rows).to.be.lessThan(3 * n);
		expect(stats.offsets.filter((offset) => offset > 0)).to.deep.equal([]);
	});

	it("does not read skipped rows when paging without writes", async () => {
		index = await setup({ schema: Doc }, create);
		const n = 1000;
		await index.store.putBatch!(
			Array.from({ length: n }, (_, i) => new Doc(pad(i), i, [BigInt(i)])),
		);
		const stats = instrument(index.store as SQLiteIndex<any>);
		const ids = await drain(
			index.store.iterate({
				query: [],
				sort: [new Sort({ key: "rank", direction: SortDirection.DESC })],
			}),
			50,
		);
		expect(ids).to.deep.equal(
			Array.from({ length: n }, (_, i) => pad(n - 1 - i)),
		);
		expect(stats.offsets.filter((offset) => offset > 0)).to.deep.equal([]);
		expect(stats.rows).to.be.lessThan(3 * n);
	});

	it("pages a long primary-key-order scan without OFFSET", async () => {
		index = await setup({ schema: Doc }, create);
		const n = 5000;
		await index.store.putBatch!(
			Array.from({ length: n }, (_, i) => new Doc(pad(i), i)),
		);
		const stats = instrument(index.store as SQLiteIndex<any>);
		const ids = await drain(index.store.iterate({}), 100);
		expect(ids).to.deep.equal(Array.from({ length: n }, (_, i) => pad(i)));
		expect(stats.offsets.filter((offset) => offset > 0)).to.deep.equal([]);
		expect(stats.rows).to.be.lessThan(3 * n);
	});

	it("re-checks changed rows of a sorted scan through the primary key", async () => {
		index = await setup({ schema: Doc }, create);
		const n = 500;
		await index.store.putBatch!(
			Array.from({ length: n }, (_, i) => new Doc(pad(i), i, [BigInt(i)])),
		);
		const db = (index.store as SQLiteIndex<any>).properties.db;
		const prepare = db.prepare.bind(db);
		let recheck: { sql: string; values: any[] } | undefined;
		wrapStatements(index.store as SQLiteIndex<any>, (sql, statement) => {
			if (!isRecheck(sql)) {
				return;
			}
			const all = statement.all.bind(statement);
			statement.all = async (values?: any[]) => {
				recheck = { sql, values: values ?? [] };
				return all(values as any);
			};
		});
		const iterator = index.store.iterate({
			query: [],
			sort: [new Sort({ key: "rank", direction: SortDirection.DESC })],
		});
		await iterator.next(10);
		await iterator.next(10);
		await index.store.put(new Doc(pad(5), 5, [5n])); // ahead of the cursor
		await iterator.next(10);
		await iterator.close();
		expect(recheck).to.not.equal(undefined);
		const plan: { detail: string }[] = await (
			await prepare("EXPLAIN QUERY PLAN " + recheck!.sql)
		).all(recheck!.values);
		// the root table is searched by key, not scanned through a sort index
		expect(
			plan
				.map((row) => row.detail)
				.filter((detail) => /^SCAN /.test(detail) && !/json_each/.test(detail)),
		).to.deep.equal([]);
	});

	it("reads everything with next(Infinity) on a fresh iterator", async () => {
		index = await setup({ schema: Doc }, create);
		for (let i = 0; i < 5; i++) {
			await index.store.put(new Doc(pad(i), i));
		}
		const iterator = index.store.iterate({});
		expect(await iterator.next(Infinity)).to.have.length(5);
		expect(iterator.done()).to.equal(true);
	});

	it("done() is true after the call that returns the last row", async () => {
		for (const sort of [
			[],
			[new Sort({ key: "rank", direction: SortDirection.DESC })],
		]) {
			index = await setup({ schema: Doc }, create);
			for (let i = 0; i < 4; i++) {
				await index.store.put(new Doc(pad(i), i));
			}
			const iterator = index.store.iterate({ query: [], sort });
			expect(iterator.done()).to.equal(undefined);
			expect(await iterator.next(2)).to.have.length(2);
			expect(iterator.done()).to.equal(false);
			expect(await iterator.next(2)).to.have.length(2);
			expect(iterator.done()).to.equal(true);
			expect(await iterator.pending()).to.equal(0);

			const counted = index.store.iterate({ query: [], sort });
			expect(await counted.pending()).to.equal(4);
			expect(counted.done()).to.equal(false);
			expect(await counted.next(4)).to.have.length(4);
			expect(counted.done()).to.equal(true);
			await counted.close();

			// A row written after the end brings the iterator back.
			await index.store.put(new Doc(pad(10), 0));
			expect(await iterator.pending()).to.equal(1);
			expect(iterator.done()).to.equal(false);
			expect(await iterator.next(2)).to.have.length(1);
			expect(iterator.done()).to.equal(true);
			await iterator.close();
		}
	});

	it("yields rows inserted or changed mid-scan once, at their sort position", async () => {
		index = await setup({ schema: Doc }, create);
		for (const i of [0, 2, 4, 6, 8]) {
			await index.store.put(new Doc(pad(i), i));
		}
		const iterator = index.store.iterate({
			query: [],
			sort: [new Sort({ key: "rank", direction: SortDirection.ASC })],
		});
		expect((await iterator.next(2)).map((x) => x.value.rank)).to.deep.equal([
			0, 2,
		]);
		await index.store.put(new Doc(pad(1), 1)); // behind the cursor
		await index.store.put(new Doc(pad(5), 5)); // ahead of the cursor
		await index.store.put(new Doc(pad(4), 9)); // moves an unseen row to the end
		await index.store.put(new Doc(pad(2), 7)); // changes a yielded row
		const rest = (await iterator.all()).map((x) => x.value.rank);
		expect(rest).to.deep.equal([1, 5, 6, 8, 9]);
		expect(await iterator.pending()).to.equal(0);
	});

	it("keeps marks, pending and resurrection after the key list exists", async () => {
		index = await setup({ schema: Doc }, create);
		for (let i = 0; i < 6; i++) {
			await index.store.put(new Doc(pad(i), i));
		}
		const iterator = index.store.iterate({});
		await iterator.next(2);
		await iterator.next(1);
		await index.store.put(new Doc(pad(10), 10));
		await iterator.markYielded!([toId(pad(4)), toId(pad(10))]);
		expect(await iterator.pending()).to.equal(2); // 3 and 5
		expect((await iterator.next(10)).map((x) => x.value.rank)).to.deep.equal([
			3, 5,
		]);
		expect(iterator.done()).to.be.true;
		await index.store.put(new Doc(pad(11), 11));
		expect(await iterator.pending()).to.equal(1);
		expect((await iterator.next(10)).map((x) => x.value.rank)).to.deep.equal([
			11,
		]);
	});

	it("lists the keys again when it falls behind the change journal", async () => {
		index = await setup({ schema: Doc }, create);
		const store = index.store as SQLiteIndex<any>;
		await store.putBatch(range(0, 300).map((i) => new Doc(pad(i * 2), i * 2)));
		const journal = journalOf(store);
		journal.capacity = 64;
		const stats = instrument(store);
		const iterator = store.iterate({});
		const first = await iterator.next(100);
		await store.put(new Doc(pad(0), 0)); // a returned row
		first.push(...(await iterator.next(1)));
		expect(journal.readerCount).to.equal(1);
		// More writes than the journal keeps: new keys between the returned
		// rows and ahead of them.
		await store.putBatch(range(0, 200).map((i) => new Doc(pad(1 + i * 2), i)));
		const rest = await drain(iterator, 50);
		const all = [...first.map((x) => String(x.id.primitive)), ...rest];
		expect(new Set(all).size).to.equal(all.length);
		expect(all.length).to.equal(300 + 200);
		const returned = new Set(all.slice(0, first.length));
		expect(rest).to.deep.equal(
			[
				...range(0, 300).map((i) => i * 2),
				...range(0, 200).map((i) => 1 + i * 2),
			]
				.sort((a, b) => a - b)
				.map(pad)
				.filter((key) => !returned.has(key)),
		);
		// Once at the start, once after the journal dropped unread entries.
		expect(stats.runs.filter(isKeyList)).to.have.length(2);
	});

	for (const [name, schema, key] of [
		["u64", U64Doc, (i: number) => BigInt(i) * 1_000_000_007n],
		["bytes", BytesDoc, (i: number) => new Uint8Array([i >> 8, i & 255, 7])],
	] as const) {
		it(`pages ${name} keys by key list across writes`, async () => {
			index = await setup({ schema: schema as any }, create);
			const n = 300;
			for (let i = 0; i < n; i++) {
				await index.store.put(new (schema as any)(key(i), i));
			}
			const ids = await drain(
				index.store.iterate({
					query: [],
					sort: [new Sort({ key: "rank", direction: SortDirection.DESC })],
				}),
				40,
				async (p) => {
					await index.store.put(new (schema as any)(key(p), 1000 + p));
				},
			);
			expect(new Set(ids).size).to.equal(ids.length);
			expect(ids.length).to.equal(n);
		});
	}

	it("pages polymorphic root tables with an Or query across writes", async () => {
		index = await setup({ schema: Base, indexBy: ["id"] }, create);
		for (let i = 0; i < 200; i++) {
			await index.store.put(
				i % 2 ? new VariantA(pad(i), i) : new VariantB(pad(i), i),
			);
		}
		const ids = await drain(
			index.store.iterate({
				query: new Or([
					new StringMatch({
						key: "id",
						value: "0001",
						method: StringMatchMethod.prefix,
					}),
					new StringMatch({
						key: "id",
						value: "0000",
						method: StringMatchMethod.prefix,
					}),
				]),
				sort: [new Sort({ key: "rank", direction: SortDirection.ASC })],
			}),
			30,
			async (p) => {
				await index.store.put(new VariantB(pad(1000 + p), p)); // no match
				const i = 150 + p; // an existing row moves behind the cursor
				await index.store.put(
					i % 2 ? new VariantA(pad(i), 1) : new VariantB(pad(i), 1),
				);
			},
		);
		expect(new Set(ids).size).to.equal(ids.length);
		expect(ids.length).to.equal(200);
	});

	it("orders a polymorphic scan by key across root tables", async () => {
		index = await setup({ schema: Base, indexBy: ["id"] }, create);
		for (let i = 0; i < 12; i++) {
			await index.store.put(
				i % 2 ? new VariantA(pad(i), i) : new VariantB(pad(i), i),
			);
		}
		const iterator = index.store.iterate({});
		const ids = await drain(iterator, 5);
		expect(ids).to.deep.equal(range(0, 12).map(pad));
	});

	it("returns a key present in two root tables once, the first in order", async () => {
		index = await setup({ schema: Base, indexBy: ["id"] }, create);
		for (let i = 0; i < 6; i++) {
			await index.store.put(new VariantB(pad(i), i));
		}
		await index.store.put(new VariantA(pad(2), 9));
		for (const sort of [
			[],
			[new Sort({ key: "rank", direction: SortDirection.DESC })],
		]) {
			const iterator = index.store.iterate({ query: [], sort });
			const rows = [
				...(await iterator.next(2)),
				...(await iterator.next(Infinity)),
			];
			expect(rows.map((row) => row.id.primitive)).to.deep.equal(
				sort.length > 0
					? [pad(2), pad(5), pad(4), pad(3), pad(1), pad(0)]
					: range(0, 6).map(pad),
			);
			// VariantA's root table comes first; with the sort, its rank does.
			expect(
				rows.find((row) => row.id.primitive === pad(2))!.value,
			).to.be.instanceOf(VariantA);
			expect(iterator.done()).to.equal(true);
			await iterator.close();
		}
	});

	it("pages root tables that store their keys differently, each key once", async () => {
		index = await setup({ schema: ShiftedRoot }, create);
		// u64 2^63 + 5 is stored as 5, as u32 5 is; u64 5 and u32 5 are one
		// key.
		for (const value of [
			new U32Root(5, 1),
			new U64Root(5n, 3),
			new U64Root((1n << 63n) + 5n, 4),
			new U32Root(7, 7),
			new U32Root(8, 8),
			new U64Root(9n, 9),
		]) {
			await index.store.put(value);
		}
		const iterator = index.store.iterate({
			query: [],
			sort: [new Sort({ key: "rank", direction: SortDirection.ASC })],
		});
		const ids = (rows: { id: { primitive: unknown } }[]) =>
			rows.map((row) => String(row.id.primitive));
		expect(ids(await iterator.next(2))).to.deep.equal([
			"5",
			String((1n << 63n) + 5n),
		]);
		// Both move behind the cursor: they come first, re-checked with a key
		// list per root table.
		await index.store.put(new U64Root(9n, 2));
		await index.store.put(new U32Root(8, 0));
		expect(await iterator.pending()).to.equal(3);
		expect(ids(await iterator.next(10))).to.deep.equal(["8", "9", "7"]);
		expect(iterator.done()).to.equal(true);

		// A mark names a key, whatever root table holds it.
		const marked = index.store.iterate({});
		await marked.markYielded!([toId(5n)]);
		// In key order: u64 keys as stored, shifted, then u32 keys.
		expect(ids(await marked.next(Infinity))).to.deep.equal([
			"9",
			String((1n << 63n) + 5n),
			"7",
			"8",
		]);
	});

	it("treats a lone surrogate in a key as U+FFFD, as SQLite stores it", async () => {
		index = await setup({ schema: Doc }, create);
		for (const key of ["a", "a\uFFFD", "b"]) {
			await index.store.put(new Doc(key));
		}
		const iterator = index.store.iterate({});
		await iterator.markYielded!([toId("a\uD800")]);
		expect(
			(await iterator.next(10)).map((row) => row.id.primitive),
		).to.deep.equal(["a", "b"]);
	});

	it("reads the change journal only while it holds a key list with rows left", async () => {
		index = await setup({ schema: Doc }, create);
		const store = index.store as SQLiteIndex<any>;
		const journal = journalOf(store);
		await store.putBatch(range(0, 10).map((i) => new Doc(pad(i), i)));
		const iterate = () =>
			store.iterate({
				query: [],
				sort: [new Sort({ key: "rank", direction: SortDirection.ASC })],
			});
		const scan = async (iterator: IndexIterator<any, any>) => {
			expect(await iterator.next(4)).to.have.length(4);
			expect(await iterator.next(4)).to.have.length(4);
			expect(journal.readerCount).to.equal(1);
		};

		// Done: nothing left to read.
		const done = iterate();
		await scan(done);
		expect(await done.next(4)).to.have.length(2);
		expect(done.done()).to.equal(true);
		expect(journal.readerCount).to.equal(0);
		// A write after the end: the iterator lists the keys again.
		await store.put(new Doc(pad(20), 20));
		expect(await done.pending()).to.equal(1);
		expect(journal.readerCount).to.equal(1);
		expect(await done.next(4)).to.have.length(1);
		expect(journal.readerCount).to.equal(0);

		// Closed.
		const closed = iterate();
		await scan(closed);
		await closed.close();
		expect(journal.readerCount).to.equal(0);

		// Expired: it drops its key list and lists the keys again.
		const expired = iterate();
		await scan(expired);
		for (const key of [...store.cursor.keys()]) {
			await store["clearupIterator"](key);
		}
		expect(journal.readerCount).to.equal(0);
		expect((await expired.next(10)).map((row) => row.value.rank)).to.deep.equal(
			[8, 9, 20],
		);
		expect(expired.done()).to.equal(true);
		expect(journal.readerCount).to.equal(0);

		// Stopped: it continues once the index starts again.
		const stopped = iterate();
		await scan(stopped);
		await store.stop();
		expect(journal.readerCount).to.equal(0);
		await store.start();
		expect((await stopped.next(10)).map((row) => row.value.rank)).to.deep.equal(
			[8, 9, 20],
		);
		expect(stopped.done()).to.equal(true);
		expect(journal.readerCount).to.equal(0);
	});

	// A done iterator holds no journal reader: after a write, its next call
	// lists the keys again, and registers a reader that starts after the write.
	for (const shape of ["a fresh all()", "a key list read to the end"]) {
		for (const call of ["next", "pending"] as const) {
			it(`still sees a row written after ${shape} when listing the keys fails once (${call})`, async () => {
				index = await setup({ schema: Doc }, create);
				const store = index.store as SQLiteIndex<any>;
				await store.putBatch(
					range(0, 10).map((i) => new Doc(pad(i), i, [BigInt(i)])),
				);
				// Sorted rows with a child table: the key list from the first call.
				const iterator =
					shape === "a fresh all()"
						? store.iterate({})
						: store.iterate({
								query: [],
								sort: [new Sort({ key: "rank", direction: SortDirection.ASC })],
							});
				expect(
					shape === "a fresh all()"
						? await iterator.all()
						: await iterator.next(100),
				).to.have.length(10);
				expect(iterator.done()).to.equal(true);
				expect(journalOf(store).readerCount).to.equal(0);
				await store.put(new Doc(pad(50), 50, [50n])); // matches, after the end
				const failure = new Error("key list read failed");
				interceptReads(store, isKeyList, { fail: failure });
				expect(
					await failed(
						call === "next" ? iterator.next(10) : iterator.pending(),
					),
				).to.equal(failure);
				if (call === "pending") {
					expect(await iterator.pending()).to.equal(1);
					expect(iterator.done()).to.equal(false);
				}
				expect(idsOf(await iterator.next(10))).to.deep.equal([pad(50)]);
				expect(iterator.done()).to.equal(true);
				expect(journalOf(store).readerCount).to.equal(0);
			});
		}
	}

	describe("pages", () => {
		const fill = async (n: number) => {
			index = await setup({ schema: Doc }, create);
			await index.store.putBatch!(
				range(0, n).map((i) => new Doc(pad(i), i, [BigInt(i)])),
			);
			return index.store as SQLiteIndex<any>;
		};

		it("reads a probe with one statement and one row ahead", async () => {
			const store = await fill(1000);
			const stats = instrument(store);
			const iterator = store.iterate({});
			const rows = await iterator.next(1);
			expect(idsOf(rows)).to.deep.equal([pad(0)]);
			expect(rows[0].value.coords).to.deep.equal([0n]);
			expect(iterator.done()).to.equal(false);
			expect(kinds(stats.runs)).to.deep.equal(["page"]);
			expect(stats.rows).to.equal(2);
			// Pages hold no key list and no journal reader.
			expect(journalOf(store).readerCount).to.equal(0);
			await iterator.close();
		});

		it("reads the next page after the last key returned", async () => {
			const store = await fill(1000);
			const db = store.properties.db;
			const prepare = db.prepare.bind(db);
			const stats = instrument(store);
			let after: { sql: string; values: any[] } | undefined;
			wrapStatements(store, (sql, statement) => {
				const all = statement.all.bind(statement);
				statement.all = async (values?: any[]) => {
					after = { sql, values: values ?? [] };
					return all(values as any);
				};
			});
			const iterator = store.iterate({});
			const first = await iterator.next(100);
			const second = await iterator.next(100);
			expect(idsOf([...first, ...second])).to.deep.equal(
				range(0, 200).map(pad),
			);
			expect(kinds(stats.runs)).to.deep.equal(["page", "page"]);
			// Each page reads one row ahead, which the next page reads again.
			expect(stats.rows).to.equal(202);
			expect(journalOf(store).readerCount).to.equal(0);
			// The second page seeks past the last key; it scans no row before it.
			const plan: { detail: string }[] = await (
				await prepare("EXPLAIN QUERY PLAN " + after!.sql)
			).all(after!.values);
			expect(
				plan.some((row) => /^SEARCH .*\(id>\?\)/.test(row.detail)),
			).to.equal(true);
			await iterator.close();
		});

		it("reads by pages for up to 2,048 rows, then lists the keys", async () => {
			const store = await fill(3000);
			const stats = instrument(store);
			const ids = await drain(store.iterate({}), 100);
			expect(ids).to.deep.equal(range(0, 3000).map(pad));
			// 21 pages read 2,121 rows (one ahead each). The 22nd page lists the
			// keys without the 2,100 returned, and reads its rows by key.
			expect(kinds(stats.runs)).to.deep.equal([
				...range(0, 21).map(() => "page"),
				"keys",
				...range(0, 9).map(() => "rows"),
			]);
			expect(stats.offsets).to.deep.equal([]);
		});

		it("lists the keys at the first write, and returns a row written behind the pages first", async () => {
			const store = await fill(100);
			const stats = instrument(store);
			const iterator = store.iterate({});
			expect(idsOf(await iterator.next(10))).to.deep.equal(
				range(0, 10).map(pad),
			);
			expect(idsOf(await iterator.next(10))).to.deep.equal(
				range(10, 20).map(pad),
			);
			await store.put(new Doc(pad(5) + "x", 0)); // new, behind the last key
			await store.put(new Doc(pad(3), 3)); // returned
			expect(idsOf(await iterator.next(10))).to.deep.equal([
				pad(5) + "x",
				...range(20, 29).map(pad),
			]);
			expect(kinds(stats.runs)).to.deep.equal(["page", "page", "keys", "rows"]);
			expect(journalOf(store).readerCount).to.equal(1);
			await iterator.close();
			expect(journalOf(store).readerCount).to.equal(0);
		});

		it("lists the keys once a key is marked", async () => {
			const store = await fill(100);
			const stats = instrument(store);
			const iterator = store.iterate({});
			await iterator.next(10);
			await iterator.markYielded!([toId(pad(12))]);
			expect(idsOf(await iterator.next(10))).to.deep.equal(
				range(10, 21)
					.filter((i) => i !== 12)
					.map(pad),
			);
			expect(kinds(stats.runs)).to.deep.equal(["page", "keys", "rows"]);
		});

		it("counts pending rows once until a write", async () => {
			const store = await fill(100);
			const stats = instrument(store);
			const iterator = store.iterate({});
			await iterator.next(10);
			expect(await iterator.pending()).to.equal(90);
			expect(await iterator.pending()).to.equal(90);
			await iterator.next(10);
			expect(await iterator.pending()).to.equal(80);
			expect(kinds(stats.runs)).to.deep.equal(["page", "count", "page"]);
			// A write: pending() lists the keys, and the pages end.
			await store.put(new Doc(pad(200), 200));
			expect(await iterator.pending()).to.equal(81);
			expect(kinds(stats.runs).slice(3)).to.deep.equal(["keys"]);
			expect(await iterator.next(100)).to.have.length(81);
			expect(iterator.done()).to.equal(true);
		});

		it("reads the rest with one statement after a page", async () => {
			const store = await fill(1000);
			const stats = instrument(store);
			const iterator = store.iterate({});
			await iterator.next(10);
			expect(idsOf(await iterator.all())).to.deep.equal(
				range(10, 1000).map(pad),
			);
			expect(kinds(stats.runs)).to.deep.equal(["page", "page"]);
			expect(iterator.done()).to.equal(true);
		});

		it("keeps paging after it expires", async () => {
			const store = await fill(100);
			const stats = instrument(store);
			const iterator = store.iterate({});
			await iterator.next(10);
			for (const key of [...store.cursor.keys()]) {
				await store["clearupIterator"](key);
			}
			expect(idsOf(await iterator.next(10))).to.deep.equal(
				range(10, 20).map(pad),
			);
			expect(kinds(stats.runs)).to.deep.equal(["page", "page"]);
		});
	});

	describe("lifecycle", () => {
		const fill = async () => {
			index = await setup({ schema: Row }, create);
			await index.store.putBatch!(range(0, 1152).map((i) => new Row(i)));
		};
		const sorted = () =>
			index.store.iterate({ query: [], sort: [new Sort({ key: "id" })] });

		it("keeps completed mutations visible between public calls", async () => {
			await fill();
			const iterator = sorted();
			await iterator.next(64);
			await index.store.del({
				query: new IntegerCompare({
					key: "id",
					compare: Compare.Equal,
					value: 0,
				}),
			});
			await index.store.put(new Row(1151, 7));
			await index.store.put(new Row(1152, 8));
			const rows = await iterator.next(Infinity);
			expect(rows.map((row) => row.value.id)).to.deep.equal(range(64, 1153));
			expect(rows.slice(-2).map((row) => row.value.value)).to.deep.equal([
				7, 8,
			]);
			expect(await iterator.pending()).to.equal(0);
			expect(iterator.done()).to.equal(true);
		});

		it("keeps result ids when the shape projects out the primary key", async () => {
			await fill();
			const iterator = index.store.iterate(
				{ query: [], sort: [new Sort({ key: "id" })] },
				{ shape: { value: true } },
			);
			const first = await iterator.next(64);
			expect(first.map((row) => row.id.primitive)).to.deep.equal(range(0, 64));
			await index.store.put(new Row(1151, 7));
			expect(await iterator.pending()).to.equal(1088);
			const rest = await iterator.next(Infinity);
			expect(rest.map((row) => row.id.primitive)).to.deep.equal(
				range(64, 1152),
			);
			for (const row of rest) expect(row.value).not.to.have.property("id");
			expect(await iterator.pending()).to.equal(0);
		});

		it("leaves out a row marked while its page is read (key list)", async () => {
			await fill();
			let release!: () => void;
			const hold = new Promise<void>((resolve) => (release = resolve));
			const iterator = sorted();
			await iterator.next(64);
			// A write: the next page lists the keys and reads its rows by key.
			await index.store.put(new Row(1151, 1));
			const held = interceptReads(
				index.store as SQLiteIndex<any>,
				isRowsByKey,
				{ hold },
			);
			const page = iterator.next(Infinity);
			await held;
			await iterator.markYielded!([toId(1024)]);
			release();
			expect((await page).map((row) => row.value.id)).to.deep.equal(
				range(64, 1152).filter((id) => id !== 1024),
			);
			expect(await iterator.pending()).to.equal(0);
		});

		it("discards a page closed before delivery (key list)", async () => {
			await fill();
			let release!: () => void;
			const hold = new Promise<void>((resolve) => (release = resolve));
			const iterator = sorted();
			await iterator.next(64);
			await index.store.put(new Row(1151, 1)); // the next page lists the keys
			const held = interceptReads(
				index.store as SQLiteIndex<any>,
				isRowsByKey,
				{ hold },
			);
			const page = iterator.next(100);
			await held;
			await iterator.close();
			release();
			expect(await page).to.deep.equal([]);
			expect(await iterator.pending()).to.equal(0);
			expect(iterator.done()).to.equal(true);
			expect(await iterator.next(1)).to.deep.equal([]);
		});

		it("consumes nothing when a page read fails", async () => {
			await fill();
			const iterator = sorted();
			await iterator.next(64);
			await index.store.put(new Row(2000, 7)); // the next page lists the keys
			const failure = new Error("page read failed");
			interceptReads(index.store as SQLiteIndex<any>, isRowsByKey, {
				fail: failure,
			});
			const error = await Promise.resolve(iterator.next(Infinity)).then(
				() => undefined,
				(e: unknown) => e,
			);
			expect(error).to.equal(failure);
			expect(
				(await iterator.next(Infinity)).map((row) => row.value.id),
			).to.deep.equal([...range(64, 1152), 2000]);
			expect(await iterator.pending()).to.equal(0);
		});

		// No write: the next page continues after the last key returned.
		it("leaves out a row marked while its page is read (paged)", async () => {
			await fill();
			let release!: () => void;
			const hold = new Promise<void>((resolve) => (release = resolve));
			const iterator = sorted();
			await iterator.next(64);
			const held = interceptReads(index.store as SQLiteIndex<any>, isPage, {
				hold,
			});
			const page = iterator.next(64);
			await held;
			await iterator.markYielded!([toId(100)]);
			release();
			expect((await page).map((row) => row.value.id)).to.deep.equal(
				range(64, 128).filter((id) => id !== 100),
			);
			expect(await iterator.pending()).to.equal(1024);
			expect(
				(await iterator.next(Infinity)).map((row) => row.value.id),
			).to.deep.equal(range(128, 1152));
		});

		it("discards a page closed before delivery (paged)", async () => {
			await fill();
			let release!: () => void;
			const hold = new Promise<void>((resolve) => (release = resolve));
			const iterator = sorted();
			await iterator.next(64);
			const held = interceptReads(index.store as SQLiteIndex<any>, isPage, {
				hold,
			});
			const page = iterator.next(100);
			await held;
			await iterator.close();
			release();
			expect(await page).to.deep.equal([]);
			expect(await iterator.pending()).to.equal(0);
			expect(iterator.done()).to.equal(true);
			expect(await iterator.next(1)).to.deep.equal([]);
		});

		it("consumes nothing when a page read fails (paged)", async () => {
			await fill();
			const iterator = sorted();
			await iterator.next(64);
			const failure = new Error("page read failed");
			interceptReads(index.store as SQLiteIndex<any>, isPage, {
				fail: failure,
			});
			const error = await Promise.resolve(iterator.next(64)).then(
				() => undefined,
				(e: unknown) => e,
			);
			expect(error).to.equal(failure);
			expect(
				(await iterator.next(Infinity)).map((row) => row.value.id),
			).to.deep.equal(range(64, 1152));
			expect(await iterator.pending()).to.equal(0);
			expect(iterator.done()).to.equal(true);
		});
	});
});
