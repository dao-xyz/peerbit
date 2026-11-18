import DB from "better-sqlite3";
import fs from "fs";
import { normalizeSQLiteDirectory } from "./directory.js";
import type { SQLitePragmaOptions } from "./sqlite3-messages.worker.js";
import type {
	Database as IDatabase,
	Statement as IStatement,
} from "./types.js";

/**
 * The bind parameters `values` (an argument list as better-sqlite3 takes it:
 * values, arrays of values, or named parameters) with each string that holds
 * a lone surrogate made well formed, U+FFFD in its place; undefined when no
 * string needs it.
 */
const wellFormedParameters = (
	values: readonly unknown[],
): unknown[] | undefined => {
	let out: unknown[] | undefined;
	for (let i = 0; i < values.length; i++) {
		const value = values[i];
		let bound: unknown = value;
		if (typeof value === "string") {
			if (!value.isWellFormed()) {
				bound = value.toWellFormed();
			}
		} else if (Array.isArray(value)) {
			bound = wellFormedParameters(value) ?? value;
		} else if (
			value != null &&
			typeof value === "object" &&
			(Object.getPrototypeOf(value) === Object.prototype ||
				Object.getPrototypeOf(value) === null)
		) {
			const entries = Object.entries(value);
			const named = wellFormedParameters(entries.map((entry) => entry[1]));
			if (named) {
				bound = Object.fromEntries(
					entries.map(([name], j) => [name, named[j]]),
				);
			}
		}
		if (bound !== value) {
			out ??= values.slice();
			out[i] = bound;
		}
	}
	return out;
};

let driverBindsWellFormed: boolean | undefined;

/**
 * Whether better-sqlite3 binds a lone surrogate as U+FFFD itself (Node 24),
 * rather than as its own three bytes (Node 22). Asked once per process: where
 * it does, statements bind their values as they are.
 */
const bindsWellFormed = (db: DB.Database) =>
	(driverBindsWellFormed ??=
		(db.prepare("select hex(?) as bytes").get("\uD800") as { bytes: string })
			.bytes === "EFBFBD");

/**
 * Binds strings as the WASM engine and better-sqlite3 on Node 24 do: a lone
 * surrogate as U+FFFD. On Node 22 better-sqlite3 binds the surrogate's own
 * three bytes, which are not UTF-8 and read back as three U+FFFD, so a value
 * read from a row would bind to other bytes than the ones stored: an iterator
 * could not find that row again by its key.
 */
const bindWellFormed = (statement: DB.Statement): DB.Statement => {
	const target = statement as unknown as Record<
		string,
		(...values: unknown[]) => unknown
	>;
	for (const method of ["run", "get", "all", "iterate", "bind"]) {
		const call = target[method];
		if (typeof call !== "function") {
			continue;
		}
		target[method] = (...values: unknown[]) =>
			call.apply(statement, wellFormedParameters(values) ?? values);
	}
	return statement;
};

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
			const prepared = db.prepare(sql);
			const stmt = (bindsWellFormed(db)
				? prepared
				: bindWellFormed(prepared)) as any as IStatement; // TODO types
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
