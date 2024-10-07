import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";
import { fromBase64URL, toBase64URL } from "@peerbit/crypto";
import {
	type OpfsSAHPoolDatabase,
	type SAHPoolUtil,
	type Database as SQLDatabase,
	type PreparedStatement as SQLStatement,
	default as sqlite3InitModule,
} from "@sqlite.org/sqlite-wasm";
import { v4 as uuid } from "uuid";
import type { BindableValue } from "./schema.js";
import {
	type Statement as IStatement,
	type StatementGetResult,
} from "./types.js";

export const encodeName = (name: string): string => {
	// since "/" and perhaps other characters might not be allowed we do encode
	const writer = new BinaryWriter();
	writer.string(name);
	return toBase64URL(writer.finalize());
};

export const decodeName = (name: string): string => {
	// since "/" and perhaps other characters might not be allowed we do encode
	const writer = new BinaryReader(fromBase64URL(name));
	return writer.string();
};

class Statement implements IStatement {
	constructor(
		readonly statement: SQLStatement,
		readonly id: string,
	) {}

	async bind(values: any[]) {
		await this.statement.bind(values);
		return this as IStatement;
	}

	async finalize() {
		const out = await this.statement.finalize();
		if (out != null && out > 0) {
			throw new Error("Error finalizing statement");
		}
	}

	get(values?: BindableValue[]) {
		if (values && values?.length > 0) {
			this.statement.bind(values);
		}
		let step = this.statement.step();
		if (!step) {
			// no data available
			this.statement.reset();
			return undefined;
		}
		const results = this.statement.get({});
		this.statement.reset();
		return results as StatementGetResult;
	}

	run(values: BindableValue[]) {
		this.statement.bind(values as any);
		this.statement.stepReset();
	}

	async reset() {
		await this.statement.reset();
		return this as IStatement;
	}

	all(values: BindableValue[]) {
		if (values && values.length > 0) {
			this.statement.bind(values as any);
		}

		let results = [];
		while (this.statement.step()) {
			results.push(this.statement.get({}));
		}
		this.statement.reset();
		return results;
	}

	step() {
		return this.statement.step();
	}
}

// eslint-disable-next-line no-console
const log = (...args: any) => console.log(...args);
// eslint-disable-next-line no-console
const error = (...args: any) => console.error(...args);

let poolUtil: SAHPoolUtil | undefined = undefined;
let sqlite3: Awaited<ReturnType<typeof sqlite3InitModule>> | undefined =
	undefined;

const create = async (directory?: string) => {
	let statements: Map<string, Statement> = new Map();

	sqlite3 =
		sqlite3 || (await sqlite3InitModule({ print: log, printErr: error }));
	let sqliteDb: OpfsSAHPoolDatabase | SQLDatabase | undefined = undefined;
	let close: (() => Promise<any> | any) | undefined = async () => {
		await Promise.all([...statements.values()].map((x) => x.finalize?.()));
		statements.clear();

		await sqliteDb?.close();
		sqliteDb = undefined;
	};
	let open = async () => {
		if (sqliteDb) {
			return sqliteDb;
		}
		if (directory) {
			// directory has to be absolute path. Remove leading dot if any
			// TODO show warning if directory is not absolute?
			directory = directory.replace(/^\./, "");

			let dbFileName = `${directory}/db.sqlite`;

			poolUtil =
				poolUtil ||
				(await sqlite3!.installOpfsSAHPoolVfs({
					directory: "peerbit/sqlite", // encodeName("peerbit")
				}));

			await poolUtil.reserveMinimumCapacity(100);
			sqliteDb = new poolUtil.OpfsSAHPoolDb(dbFileName);
		} else {
			sqliteDb = new sqlite3!.oo1.DB(":memory:");
		}

		sqliteDb.exec("PRAGMA journal_mode = WAL");
		sqliteDb.exec("PRAGMA foreign_keys = on");
	};

	return {
		close,
		exec: (sql: string) => {
			return sqliteDb!.exec(sql);
		},
		open,
		prepare: async (sql: string, id?: string) => {
			if (id == null) {
				id = uuid();
			}
			let prev = statements.get(id);
			if (prev) {
				await prev.reset();
				return prev;
			}

			const statement = sqliteDb!.prepare(sql);
			const wrappedStatement = new Statement(statement, id);
			statements.set(id, wrappedStatement);
			return wrappedStatement;
		},
		get(sql: string) {
			return sqliteDb!.exec({ sql, rowMode: "array" });
		},

		run(sql: string, bind: any[]) {
			return sqliteDb!.exec(sql, { bind, rowMode: "array" });
		},
		status: () => (sqliteDb?.isOpen() ? "open" : "closed"),
		statements,
	};
};

export { create };
