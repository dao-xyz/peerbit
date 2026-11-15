import { field, option, variant } from "@dao-xyz/borsh";
import {
	type IdPrimitive,
	type Index,
	type IndexKeyScan,
	NotStartedError,
	StringMatch,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import { SQLiteIndex } from "../src/engine.js";
import { createDatabase } from "../src/index.js";
import type { Database, Statement } from "../src/types.js";

@variant("sqlite_key_inventory")
class Row {
	@id({ type: "string" })
	id: string;
	@field({ type: "string" })
	value: string;
	constructor(id: string) {
		this.id = id;
		this.value = "not needed by inventory";
	}
}

@variant("key_inventory_number")
class NumberRow {
	@id({ type: "u32" }) id!: number;
}
@variant("key_inventory_bigint")
class BigintRow {
	@id({ type: "u64" }) id!: bigint;
}
@variant("key_inventory_bytes")
class BytesRow {
	@id({ type: Uint8Array }) id!: Uint8Array;
}
@variant("key_inventory_optional_bigint")
class OptionalBigintRow {
	@id({ type: option("u64") }) id?: bigint;
}
@variant("key_inventory_optional_number")
class OptionalNumberRow {
	@id({ type: option("u32") }) id?: number;
}
abstract class VariantRow {}
@variant(0)
class FirstRow extends VariantRow {
	@id({ type: "string" }) id!: string;
}
@variant(1)
class SecondRow extends VariantRow {
	@id({ type: "string" }) id!: string;
}

const gate = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};

describe("SQLite raw key inventory", () => {
	let db: Database;
	let index: SQLiteIndex<Row>;
	let scans: IndexKeyScan[];
	let releases: (() => void)[];
	let pending: Promise<unknown>[];
	let reads: { sql: string; rows: number; values: unknown[] }[];
	let afterRead: (() => Promise<void>) | undefined;
	let otherIndices: SQLiteIndex<any>[];

	beforeEach(async () => {
		scans = [];
		releases = [];
		pending = [];
		reads = [];
		afterRead = undefined;
		otherIndices = [];
		db = await createDatabase();
		await db.open();
		index = new SQLiteIndex<Row>({ db, schema: Row, scope: [] }).init({
			schema: Row,
		});
		await index.start();
		const prepare = db.prepare.bind(db);
		const wrapped = new Set<Statement>();
		sinon.stub(db, "prepare").callsFake(async (sql, id) => {
			const stmt = await prepare(sql, id);
			if (/^select\b/i.test(sql) && !wrapped.has(stmt)) {
				wrapped.add(stmt);
				const all = stmt.all.bind(stmt);
				sinon.stub(stmt, "all").callsFake(async (values, ...rest) => {
					const rows = await all(values, ...rest);
					reads.push({ sql, rows: rows.length, values: [...values] });
					await afterRead?.();
					return rows;
				});
			}
			return stmt;
		});
	});

	afterEach(async () => {
		try {
			for (const release of releases) release();
			for (const scan of scans) await scan.close();
			await Promise.all(pending);
		} finally {
			sinon.restore();
			try {
				for (const other of otherIndices) await other.stop();
				await index?.stop();
			} finally {
				await db?.close();
			}
		}
	});

	const track = <T>(task: Promise<T>) => {
		pending.push(
			task.then(
				() => undefined,
				() => undefined,
			),
		);
		return task;
	};
	const scan = (pageSize: number, signal?: AbortSignal) => {
		const create = (index as Index<Row>).scanKeyPrimitives;
		expect(create).to.be.a("function");
		const cursor = create!.call(index, { pageSize, signal });
		scans.push(cursor);
		return cursor;
	};
	const all = async (cursor: IndexKeyScan) => {
		const keys: IdPrimitive[] = [];
		for (let i = 0; i < 2000; i++) {
			const page = await cursor.next();
			expect(["more", "complete"]).to.include(page.status);
			keys.push(...page.keys);
			if (page.status === "complete") return keys;
		}
		throw new Error("inventory did not terminate");
	};
	const openOther = async <T extends Record<string, any>>(
		schema: new (...args: any[]) => T,
	) => {
		const other = new SQLiteIndex<T>({
			db,
			schema,
			scope: [schema.name],
		}).init({ schema, indexBy: ["id"] });
		otherIndices.push(other);
		await other.start();
		return other;
	};

	for (const count of [0, 4, 5, 1152]) {
		it(`reads ${count} canonical keys with bounded primary-key seeks only`, async () => {
			const expected = Array.from(
				{ length: count },
				(_, i) => `key-${String(i).padStart(4, "0")}`,
			);
			await index.putBatch([...expected].reverse().map((key) => new Row(key)));
			sinon
				.stub(index, "iterate")
				.throws(new Error("query iteration forbidden"));
			sinon.stub(index, "get").throws(new Error("document decoding forbidden"));
			sinon.stub(index, "count").throws(new Error("count forbidden"));
			const cursor = scan(4);
			expect(await all(cursor)).to.deep.equal(expected);
			expect(reads.length).to.equal(Math.floor(count / 4) + 1);
			for (const [i, read] of reads.entries()) {
				expect(read.rows).to.be.at.most(4);
				expect(read.values.at(-1)).to.equal(4);
				expect(read.sql).not.to.match(/\b(count|offset|join)\b|select\s+\*/i);
				if (i > 0) expect(read.sql).to.match(/where.+>\s*\?/i);
			}
			expect(index.cursor.size).to.equal(0);
			const statements = reads.length;
			await cursor.close();
			await index.put(new Row("later"));
			expect(await cursor.next()).to.deep.equal({
				status: "complete",
				keys: [],
			});
			expect(reads.length).to.equal(statements);
		});
	}

	it("validates the page bound and requires an open owner", async () => {
		for (const pageSize of [0, -1, 1.5, NaN, Infinity, 4097]) {
			expect(() => scan(pageSize)).to.throw(RangeError);
		}
		expect(await scan(4096).next()).to.deep.equal({
			status: "complete",
			keys: [],
		});
		await index.stop();
		expect(() => scan(1)).to.throw(NotStartedError);
	});

	for (const [schema, ids] of [
		[NumberRow, [0, 1, 4294967295]],
		[OptionalNumberRow, [0, 1, 4294967295]],
		[OptionalBigintRow, [0n, 1n, 18446744073709551615n]],
		[
			BigintRow,
			[
				0n,
				1n,
				9223372036854775807n,
				9223372036854775808n,
				18446744073709551615n,
			],
		],
		[
			BytesRow,
			[
				new Uint8Array(),
				new Uint8Array([0]),
				new Uint8Array([0, 1]),
				new Uint8Array([255]),
			],
		],
	] as const) {
		it(`seeks stored ${schema.name} keys and returns canonical primitives`, async () => {
			const other = await openOther(schema as typeof NumberRow);
			await other.putBatch(
				[...ids]
					.reverse()
					.map((id) => Object.assign(new schema(), { id })) as NumberRow[],
			);
			const cursor = other.scanKeyPrimitives!({ pageSize: 1 });
			scans.push(cursor);
			expect(await all(cursor)).to.deep.equal(
				ids.map((id) => toId(id).primitive),
			);
		});
	}

	it("does not advertise a partial or duplicate inventory of variant tables", async () => {
		const other = await openOther(VariantRow as typeof FirstRow);
		await other.put(Object.assign(new FirstRow(), { id: "shared" }));
		await other.put(Object.assign(new SecondRow(), { id: "shared" }));
		expect(other.scanKeyPrimitives).to.equal(undefined);
	});

	it("snapshots page size and removes abort listeners on terminal results", async () => {
		await index.putBatch([new Row("a"), new Row("b")]);
		const controller = new AbortController();
		const add = sinon.spy(controller.signal, "addEventListener");
		const remove = sinon.spy(controller.signal, "removeEventListener");
		const options = { pageSize: 1, signal: controller.signal };
		const cursor = index.scanKeyPrimitives!(options);
		scans.push(cursor);
		options.pageSize = 4096;
		expect(await cursor.next()).to.deep.equal({ status: "more", keys: ["a"] });
		await cursor.close();
		expect(add.calledOnce).to.equal(true);
		expect(remove.calledOnce).to.equal(true);
		expect(remove.firstCall.args.slice(0, 2)).to.deep.equal(
			add.firstCall.args.slice(0, 2),
		);
		controller.abort();
		expect(await scan(1, controller.signal).next()).to.deep.equal({
			status: "aborted",
			keys: [],
		});
		expect(add.calledOnce).to.equal(true);
	});

	for (const mutation of [
		"put",
		"replace",
		"batch",
		"delete",
		"failed batch",
		"ordered prefix",
	] as const) {
		it(`invalidates on ${mutation}, including failed or partially successful work`, async () => {
			await index.putBatch([new Row("a"), new Row("b")]);
			const cursor = scan(1);
			expect(await cursor.next()).to.deep.equal({
				status: "more",
				keys: ["a"],
			});
			if (mutation === "put") await index.put(new Row("c"));
			if (mutation === "replace") await index.put(new Row("a"));
			if (mutation === "batch") await index.putBatch([new Row("c")]);
			if (mutation === "delete")
				await index.del({
					query: new StringMatch({ key: "id", value: "absent" }),
				});
			if (mutation === "failed batch") {
				await expect(index.putBatch([new Row("c"), {} as Row])).to.be.rejected;
				expect(await index.get(toId("c"))).to.equal(undefined);
			}
			if (mutation === "ordered prefix") {
				const error = new Error("callback failure after committed prefix");
				await expect(
					index.withOrderedWriteSession(async (session) => {
						await session.put(new Row("c"));
						throw error;
					}),
				).to.be.rejectedWith(error);
				expect((await index.get(toId("c")))?.value.id).to.equal("c");
			}
			expect(await cursor.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			expect(await cursor.next()).to.deep.equal({
				status: "invalidated",
				keys: [],
			});
			expect(await all(scan(2))).to.include("a");
		});
	}

	it("invalidates immediately at mutation admission, before the database queue drains", async () => {
		await index.put(new Row("a"));
		const cursor = scan(1);
		const release = await index["acquireDatabaseBarrier"]();
		releases.push(release);
		const write = track(index.put(new Row("b")));
		expect(await cursor.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await scan(1).next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(reads).to.deep.equal([]);
		release();
		await write;
		expect(await all(scan(1))).to.deep.equal(["a", "b"]);
	});

	for (const action of ["close", "abort", "mutation", "stop"] as const) {
		it(`discards an in-flight page after ${action} and releases admission`, async () => {
			await index.put(new Row("a"));
			const entered = gate(),
				release = gate();
			releases.push(release.resolve);
			afterRead = async () => {
				entered.resolve();
				await release.promise;
			};
			const controller = new AbortController();
			const cursor = scan(2, controller.signal);
			const read = track(Promise.resolve(cursor.next()));
			await Promise.race([
				entered.promise,
				read.then(() => {
					throw new Error("page was not held");
				}),
			]);
			let task: Promise<unknown> | undefined;
			if (action === "close") await cursor.close();
			if (action === "abort") controller.abort();
			if (action === "mutation") task = track(index.put(new Row("b")));
			if (action === "stop") task = track(index.stop());
			release.resolve();
			const status = {
				close: "closed",
				abort: "aborted",
				mutation: "invalidated",
				stop: "closed",
			}[action];
			expect(await read).to.deep.equal({ status, keys: [] });
			await task;
			expect(await cursor.next()).to.deep.equal({ status, keys: [] });
			if (action === "stop") await index.start();
			afterRead = undefined;
			await index.put(new Row("after"));
			expect((await index.get(toId("after")))?.value.id).to.equal("after");
		});
	}

	it("preserves unexpected read failure identity and stays failed", async () => {
		await index.put(new Row("a"));
		const error = new Error("injected after real select");
		afterRead = async () => {
			throw error;
		};
		const cursor = scan(1);
		await expect(Promise.resolve(cursor.next())).to.be.rejectedWith(error);
		expect(await cursor.next()).to.deep.equal({ status: "failed", keys: [] });
		afterRead = undefined;
		expect(await all(scan(1))).to.deep.equal(["a"]);
	});

	for (const readFails of [false, true]) {
		it(`preserves ${readFails ? "read" : "reset"} failure identity when cleanup fails`, async () => {
			const readError = new Error("read failed");
			const resetError = new Error("reset failed");
			const reset = sinon.stub().rejects(resetError);
			sinon.stub(index as any, "getOrPrepareStatement").resolves({
				all: async () => {
					if (readFails) throw readError;
					return [];
				},
				reset,
			});
			const cursor = scan(1);
			await expect(Promise.resolve(cursor.next())).to.be.rejectedWith(
				readFails ? readError : resetError,
			);
			expect(await cursor.next()).to.deep.equal({ status: "failed", keys: [] });
			expect(reset.calledOnce).to.equal(true);
		});
	}

	it("does not revive an inventory after stop/start", async () => {
		await index.put(new Row("a"));
		const cursor = scan(1);
		await index.stop();
		await index.start();
		expect(await cursor.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(await all(scan(1))).to.deep.equal(["a"]);
	});
});
