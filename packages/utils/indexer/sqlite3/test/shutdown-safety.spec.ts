/**
 * Regression: SQLiteIndex crashes during and after shutdown
 *
 * 4 sub-bugs:
 * 1. Getters throw NotStartedError after close (should return empty defaults)
 * 2. Fields uninitialized before init() (backing fields are undefined)
 * 3. stop() and drop() crash on undefined fields
 * 4. Late calls during active shutdown should be no-ops, but calls after
 *    shutdown completes should throw NotStartedError
 */
import { field, variant } from "@dao-xyz/borsh";
import { NotStartedError, id, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLiteIndex } from "../src/engine.js";
import type { Database, Statement } from "../src/types.js";

@variant(0)
class Document {
	@id({ type: "string" })
	key!: string;

	@field({ type: "string" })
	value!: string;

	constructor(opts?: { key: string; value: string }) {
		if (opts) {
			this.key = opts.key;
			this.value = opts.value;
		}
	}
}

const createMockStatement = (sid: string): Statement => ({
	id: sid,
	bind: async () => createMockStatement(sid),
	run: async () => undefined,
	get: async () => undefined,
	all: async () => [],
	reset: async () => createMockStatement(sid),
});

const expectNotStarted = async (fn: () => any) => {
	try {
		await fn();
	} catch (error) {
		expect(error).to.be.instanceOf(NotStartedError);
		return;
	}
	expect.fail("Expected NotStartedError");
};

const createMockDatabase = (options?: {
	status?: () => Promise<"open" | "closed"> | "open" | "closed";
}): Database => {
	const statements = new Map<string, Statement>();
	return {
		exec: async (sql: string) => {},
		prepare: async (sql: string, key?: string) => {
			const statement = createMockStatement(key ?? sql);
			if (key) {
				statements.set(key, statement);
			}
			return statement;
		},
		open: async () => undefined,
		close: async () => undefined,
		drop: async () => undefined,
		status: options?.status ?? (() => "open"),
		statements: {
			get: (k: string) => statements.get(k),
			get size() {
				return statements.size;
			},
		},
	};
};

describe("@peerbit/indexer-sqlite3 — shutdown safety", () => {
	let db: Database;

	beforeEach(() => {
		db = createMockDatabase();
	});

	// Bug 1: getters throw NotStartedError after close
	it("getters should not throw after close", async () => {
		const index = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});
		index.init({ indexBy: ["key"], schema: Document });
		await index.start();

		expect(index.tables).to.be.instanceOf(Map);
		expect(index.rootTables).to.be.an("array");
		expect(index.cursor).to.be.instanceOf(Map);

		await index.stop();

		// After stop, these should return safe defaults — not throw.
		expect(() => index.tables).to.not.throw();
		expect(() => index.rootTables).to.not.throw();
		expect(() => index.cursor).to.not.throw();

		const tables = index.tables;
		const rootTables = index.rootTables;
		const cursor = index.cursor;

		expect(tables).to.be.instanceOf(Map);
		expect(tables.size).to.equal(0);
		expect(rootTables).to.be.an("array");
		expect(rootTables).to.have.length(0);
		expect(cursor).to.be.instanceOf(Map);
		expect(cursor.size).to.equal(0);
	});

	// Bug 2: fields uninitialized before init()
	it("getters should not throw before init", () => {
		const index = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});

		expect(() => index.tables).to.not.throw();
		expect(() => index.rootTables).to.not.throw();
		expect(() => index.cursor).to.not.throw();

		const tables = index.tables;
		const rootTables = index.rootTables;
		const cursor = index.cursor;

		expect(tables).to.be.instanceOf(Map);
		expect(tables.size).to.equal(0);
		expect(rootTables).to.be.an("array");
		expect(rootTables).to.have.length(0);
		expect(cursor).to.be.instanceOf(Map);
		expect(cursor.size).to.equal(0);
	});

	// Bug 3: stop() and drop() crash when fields are uninitialized
	it("stop and drop should not crash when fields are uninitialized", async () => {
		const index = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});

		try {
			await index.drop();
		} catch (err) {
			expect.fail(
				`drop() should not throw on an uninitialized index, got: ${err}`,
			);
		}

		const index2 = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});
		(index2 as any).state = "open";
		(index2 as any).closed = false;
		try {
			await index2.stop();
		} catch (err) {
			expect.fail(
				`stop() should not throw on an uninitialized index, got: ${err}`,
			);
		}
	});

	it("public APIs should throw NotStartedError after close completes", async () => {
		const index = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});
		index.init({ indexBy: ["key"], schema: Document });
		await index.start();

		await index.stop();

		const lateDoc = new Document({ key: "b", value: "world" });
		await expectNotStarted(() => index.put(lateDoc));
		await expectNotStarted(() => index.get(toId("b")));
		await expectNotStarted(() =>
			index.del({ query: { key: "b" } as Record<string, any> }),
		);
		await expectNotStarted(() => index.count());
		await expectNotStarted(() => index.getSize());
		await expectNotStarted(() => index.sum({ key: "value" }));
		expect(() => index.iterate()).to.throw(NotStartedError);
	});

	it("public APIs should return neutral results while close is in progress", async () => {
		let resolveStatus!: (status: "open") => void;
		let statusCalled!: () => void;
		const statusStarted = new Promise<void>((resolve) => {
			statusCalled = resolve;
		});
		const status = new Promise<"open">((resolve) => {
			resolveStatus = resolve;
		});
		db = createMockDatabase({
			status: () => {
				statusCalled();
				return status;
			},
		});

		const index = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});
		index.init({ indexBy: ["key"], schema: Document });
		await index.start();

		const stopPromise = index.stop();
		await statusStarted;

		expect(
			await index.put(new Document({ key: "b", value: "world" })),
		).to.equal(undefined);
		expect(await index.get(toId("b"))).to.equal(undefined);
		expect(
			await index.del({ query: { key: "b" } as Record<string, any> }),
		).to.deep.equal([]);
		expect(await index.count()).to.equal(0);
		expect(await index.getSize()).to.equal(0);
		expect(await index.sum({ key: "value" })).to.equal(0);
		const iterator = index.iterate();

		expect(index.cursor.size).to.equal(0);
		expect(index.cursorCount).to.equal(0);
		expect(await iterator.pending()).to.equal(0);
		expect(await iterator.next(1)).to.deep.equal([]);
		expect(await iterator.all()).to.deep.equal([]);
		expect(iterator.done()).to.equal(true);

		await iterator.close();

		expect(index.cursor.size).to.equal(0);
		expect(index.cursorCount).to.equal(0);

		resolveStatus("open");
		await stopPromise;

		await expectNotStarted(() =>
			index.put(new Document({ key: "c", value: "closed" })),
		);
	});
});
