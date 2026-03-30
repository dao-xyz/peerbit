/**
 * Regression: SQLiteIndex crashes during and after shutdown
 *
 * 4 sub-bugs:
 * 1. Getters throw NotStartedError after close (should return empty defaults)
 * 2. Fields uninitialized before init() (backing fields are undefined)
 * 3. stop() and drop() crash on undefined fields
 * 4. put() throws NotStartedError when closed (should return undefined)
 */

import { expect } from "chai";
import { field, variant } from "@dao-xyz/borsh";
import { id, NotStartedError } from "@peerbit/indexer-interface";
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

const createMockDatabase = (): Database => {
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
		status: () => "open",
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
		(index2 as any).closed = false;
		try {
			await index2.stop();
		} catch (err) {
			expect.fail(
				`stop() should not throw on an uninitialized index, got: ${err}`,
			);
		}
	});

	// Bug 4: put() throws NotStartedError when closed
	it("put should return undefined when closed, not throw NotStartedError", async () => {
		const index = new SQLiteIndex<Document>({
			scope: [],
			db,
			schema: Document,
		});
		index.init({ indexBy: ["key"], schema: Document });
		await index.start();

		await index.stop();

		const lateDoc = new Document({ key: "b", value: "world" });
		let result: any;
		let threw = false;
		try {
			result = await index.put(lateDoc);
		} catch (err) {
			threw = true;
			expect(err).to.not.be.instanceOf(
				NotStartedError,
				"put() after close should not throw NotStartedError; it should return undefined",
			);
		}

		if (!threw) {
			expect(result).to.equal(undefined);
		}
	});
});
