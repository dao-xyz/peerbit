import DB from "better-sqlite3";
import fs from "fs";
import { normalizeSQLiteDirectory } from "./directory.js";
import type { SQLitePragmaOptions } from "./sqlite3-messages.worker.js";
import type {
	Database as IDatabase,
	Statement as IStatement,
} from "./types.js";

const applyPragmas = (db: DB.Database, pragmas?: SQLitePragmaOptions) => {
	db.pragma("journal_mode = WAL");
	db.pragma("foreign_keys = on");
	db.pragma(`synchronous = ${(pragmas?.synchronous ?? "FULL").toUpperCase()}`);
	if (pragmas?.lockingMode) {
		db.pragma(`locking_mode = ${pragmas.lockingMode.toUpperCase()}`);
	}
	if (pragmas?.tempStore && pragmas.tempStore !== "DEFAULT") {
		db.pragma(`temp_store = ${pragmas.tempStore.toUpperCase()}`);
	}
	db.defaultSafeIntegers(true);
};

let create = async (
	directory?: string,
	options?: { pragmas?: SQLitePragmaOptions },
) => {
	const persistentDirectory = normalizeSQLiteDirectory(directory);
	let db: DB.Database | undefined = undefined;
	let statements: Map<string, IStatement> = new Map();
	let dbFileName: string;

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
	let drop = () => {
		if (db && !db?.memory) {
			fs.rmSync(dbFileName);
			db = undefined;
		}
		return close();
	};
	let open = () => {
		if (db?.open) {
			return db;
		}

		if (!db) {
			if (persistentDirectory) {
				// if directory is provided, check if directory exist, if not create it
				if (!fs.existsSync(persistentDirectory)) {
					fs.mkdirSync(persistentDirectory, { recursive: true });
				}
				dbFileName = `${persistentDirectory}/db.sqlite`;
			} else {
				dbFileName = ":memory:";
			}

			db = new DB(dbFileName, {
				fileMustExist: false,
				readonly: false /* , verbose: (message) => console.log(message)  */,
			});
		}

		applyPragmas(db, options?.pragmas);
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
		drop,
		open,
		status: () => (db ? "open" : "closed"),
		crashSafeDurability:
			persistentDirectory != null &&
			(options?.pragmas?.synchronous ?? "FULL").toUpperCase() === "FULL"
				? {
						crashSafe: true,
						barrier: () => {
							if (!db) throw new Error("Database not open");
							// FULL commits sync the WAL; checkpointing here supplies a concrete
							// fence for every prior transaction before a receipt is issued.
							db.pragma("wal_checkpoint(PASSIVE)");
						},
					}
				: undefined,
	} as IDatabase; // TODO fix this
};

export { create };
