import { field, variant } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { SQLLiteIndex } from "../src/engine.js";
import type { Database, Statement } from "../src/types.js";

const createMockStatement = (id: string): Statement => ({
	id,
	bind: async () => createMockStatement(id),
	run: async () => undefined,
	get: async () => undefined,
	all: async () => [],
	reset: async () => createMockStatement(id),
});

const createMockDatabase = (): Database & { executedSql: string[] } => {
	const statements = new Map<string, Statement>();
	const executedSql: string[] = [];
	return {
		executedSql,
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
		open: async () => undefined,
		close: async () => undefined,
		drop: async () => undefined,
		status: () => "open",
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
});
