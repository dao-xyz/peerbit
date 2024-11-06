import DB from "better-sqlite3";
import fs from "fs";
import type {
	Database as IDatabase,
	Statement as IStatement,
} from "./types.js";

let create = async (directory?: string) => {
	let db: DB.Database | undefined = undefined;
	let statements: Map<string, IStatement> = new Map();

	let close = () => {
		for (const stmt of statements.values()) {
			stmt.finalize?.();
		}
		statements.clear();

		if (db) {
			db.close();
			db = undefined;
		}
	};
	let open = () => {
		if (db) {
			return db;
		}

		let dbFileName: string;
		if (directory) {
			// if directory is provided, check if directory exist, if not create it
			if (!fs.existsSync(directory)) {
				fs.mkdirSync(directory, { recursive: true });
			}
			dbFileName = `${directory}/db.sqlite`;
		} else {
			dbFileName = ":memory:";
		}

		db = new DB(dbFileName, {
			fileMustExist: false,
			readonly: false /* , verbose: (message) => console.log(message)  */,
		});
		// TODO this test makes things faster, but for benchmarking it might yield wierd results where some runs are faster than others
		db.pragma("journal_mode = WAL");
		db.pragma("foreign_keys = on");
		db.defaultSafeIntegers(true);
	};

	return {
		exec: (sql: string) => {
			if (!db) throw new Error("Database not open");
			return db.exec(sql);
		},
		async prepare(sql: string, id?: string) {
			if (!db) throw new Error("Database not open");
			if (id != null) {
				let prev = statements.get(id);

				if (prev) {
					await prev.reset?.();
					return prev;
				}
			}
			const stmt = db.prepare(sql) as any as IStatement; // TODO types
			if (id != null) {
				statements.set(id, stmt);
			}
			return stmt;
		},
		statements,
		close,
		open,
		status: () => (db ? "open" : "closed"),
	} as IDatabase; // TODO fix this
};

export { create };
