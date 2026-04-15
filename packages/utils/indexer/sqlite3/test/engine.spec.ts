import { field, variant } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex, SQLiteIndices } from "../src/engine.js";
import type { Database, Statement } from "../src/types.js";

const createMockStatement = (id: string): Statement => ({
	id,
	bind: async () => createMockStatement(id),
	run: async () => undefined,
	get: async () => undefined,
	all: async () => [],
	reset: async () => createMockStatement(id),
});

const createMockDatabase = (
	initialStatus: "open" | "closed" = "open",
): Database & { executedSql: string[]; openCalls: number } => {
	const statements = new Map<string, Statement>();
	const executedSql: string[] = [];
	let status: "open" | "closed" = initialStatus;
	let openCalls = 0;
	return {
		executedSql,
		get openCalls() {
			return openCalls;
		},
		exec: async (sql: string) => {
			executedSql.push(sql);
		},
		prepare: async (sql: string, id?: string) => {
			const statement = createMockStatement(id ?? sql);
			if (id) {
				statements.set(id, statement);
			}
			return statement;
		},
		open: async () => {
			openCalls++;
			status = "open";
		},
		close: async () => {
			status = "closed";
		},
		drop: async () => undefined,
		status: () => status,
		statements: {
			get: (key: string) => statements.get(key),
			get size() {
				return statements.size;
			},
		},
	};
};

describe("engine", () => {
	it("uses WITHOUT ROWID for non-integer primary-key root tables", async () => {
		@variant("root")
		class RootDoc {
			@id({ type: "string" })
			id!: string;

			@field({ type: Uint8Array })
			bytes!: Uint8Array;
		}

		const db = createMockDatabase();
		const index = new SQLLiteIndex({
			scope: [],
			db,
			schema: RootDoc,
		}).init({
			schema: RootDoc,
		});

		await index.start();
		await index.stop();

		expect(
			db.executedSql.some((sql) =>
				sql.toLowerCase().includes("create table if not exists") &&
				sql.toLowerCase().includes("strict, without rowid"),
			),
		).to.equal(true);
	});

	it("keeps rowid-backed tables for integer primary keys", async () => {
		@variant("root")
		class RootDoc {
			@id({ type: "u32" })
			id!: number;

			@field({ type: "string" })
			value!: string;
		}

		const db = createMockDatabase();
		const index = new SQLLiteIndex({
			scope: [],
			db,
			schema: RootDoc,
		}).init({
			schema: RootDoc,
		});

		await index.start();
		await index.stop();

		expect(
			db.executedSql.some((sql) =>
				sql.toLowerCase().includes("create table if not exists") &&
				sql.toLowerCase().includes("without rowid"),
			),
		).to.equal(false);
	});

	it("does not reopen a root sqlite database that is already open", async () => {
		const db = createMockDatabase("open");
		const indices = new SQLiteIndices({
			scope: [],
			db,
			directory: "repo/test/index",
		});

		await indices.start();

		expect(db.openCalls).to.equal(0);
	});

	it("opens a root sqlite database when it starts closed", async () => {
		const db = createMockDatabase("closed");
		const indices = new SQLiteIndices({
			scope: [],
			db,
			directory: "repo/test/index",
		});

		await indices.start();

		expect(db.openCalls).to.equal(1);
	});
});
