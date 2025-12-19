import { BinaryReader, BinaryWriter } from "@dao-xyz/borsh";
import { fromBase64URL, toBase64URL } from "@peerbit/crypto";
import {
	type OpfsSAHPoolDatabase,
	type SAHPoolUtil,
	type Database as SQLDatabase,
	type PreparedStatement as SQLStatement,
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
		try {
			await this.statement.reset(); // this returns the last rc
		} catch (e: any) {
			// sqlite3_reset() can surface the prior step/exec rc.
			// The statement *is* reset; ignore benign codes.
			const msg = e?.message || "";
			const rc = e?.rc;
			const code = e?.code;

			const isFk =
				code === "SQLITE_CONSTRAINT_FOREIGNKEY" ||
				rc === 787 ||
				msg.includes("SQLITE_CONSTRAINT_FOREIGNKEY") ||
				msg.includes("FOREIGN KEY constraint failed");

			const isBusy = code === "SQLITE_BUSY" || rc === 5; // optional

			if (isFk || isBusy) {
				return this as IStatement; // swallow; stmt is reset
			}
			// (keep throwing for other unexpected errors)
			throw e;
		}
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

type Sqlite3InitModule = typeof import("@sqlite.org/sqlite-wasm").default;
type Sqlite3Module = Awaited<ReturnType<Sqlite3InitModule>>;
type Sqlite3InitModuleState = {
	debugModule?: (...args: unknown[]) => void;
	wasmFilename?: string;
	sqlite3Dir?: string;
};

const SQLITE3_ASSET_BASE = "/peerbit/sqlite3";
const SQLITE3_ASSET_DIR = `${SQLITE3_ASSET_BASE}/`;
const SQLITE3_WASM_PATH = `${SQLITE3_ASSET_BASE}/sqlite3.wasm`;

let sqlite3InitModulePromise: Promise<Sqlite3InitModule> | undefined;

const ensureSqlite3InitModuleState = () => {
	const globalWithSqlite = globalThis as typeof globalThis & {
		sqlite3InitModuleState?: Sqlite3InitModuleState;
	};
	const existing = globalWithSqlite.sqlite3InitModuleState ?? {};
	const debugModule =
		typeof existing.debugModule === "function"
			? existing.debugModule
			: () => {};
	existing.debugModule = debugModule;
	existing.wasmFilename = SQLITE3_WASM_PATH;
	existing.sqlite3Dir = SQLITE3_ASSET_DIR;
	globalWithSqlite.sqlite3InitModuleState = existing;
};

const loadSqlite3InitModule = async (): Promise<Sqlite3InitModule> => {
	if (!sqlite3InitModulePromise) {
		sqlite3InitModulePromise = import("@sqlite.org/sqlite-wasm").then(
			(mod) => mod.default,
		);
	}
	const sqlite3InitModule = await sqlite3InitModulePromise;
	// sqlite-wasm reads sqlite3InitModuleState when sqlite3InitModule() runs.
	ensureSqlite3InitModuleState();
	return sqlite3InitModule;
};

let poolUtil: SAHPoolUtil | undefined = undefined;
let sqlite3: Sqlite3Module | undefined = undefined;

const create = async (directory?: string) => {
	let statements: Map<string, Statement> = new Map();

	const sqlite3InitModule = await loadSqlite3InitModule();
	sqlite3 =
		sqlite3 ||
		(await sqlite3InitModule({
			print: log,
			printErr: error,
			locateFile: (file: string) => `${SQLITE3_ASSET_BASE}/${file}`,
		}));
	let sqliteDb: OpfsSAHPoolDatabase | SQLDatabase | undefined = undefined;
	let closeInternal = async () => {
		await Promise.all([...statements.values()].map((x) => x.finalize?.()));
		statements.clear();

		await sqliteDb?.close();
		sqliteDb = undefined;
	};
	let dbFileName: string;

	const cleanupPool = async (_label: string, preserveDbFile: boolean) => {
		if (!poolUtil || dbFileName == null) {
			return;
		}

		const relatedFiles = new Set([
			dbFileName,
			`${dbFileName}-journal`,
			`${dbFileName}-wal`,
			`${dbFileName}-shm`,
		]);

		for (const fileName of relatedFiles) {
			if (preserveDbFile && fileName === dbFileName) {
				continue;
			}
			try {
				poolUtil.unlink(fileName);
			} catch {
				// ignore unlink failures
			}
		}

		const wipePool = async () => {
			if (preserveDbFile || !poolUtil?.wipeFiles) {
				return;
			}
			try {
				await poolUtil.wipeFiles();
			} catch {
				// ignore wipe failures
			}
		};

		const directoryPrefix = directory
			? `${directory.replace(/\/$/, "")}/`
			: undefined;
		if (!directoryPrefix) {
			await wipePool();
			return;
		}
		let poolFiles: string[] = [];
		try {
			poolFiles = poolUtil.getFileNames?.() ?? [];
		} catch {
			poolFiles = [];
		}
		for (const name of poolFiles) {
			if (preserveDbFile && name === dbFileName) {
				continue;
			}
			if (relatedFiles.has(name)) {
				continue;
			}
			if (name.startsWith(directoryPrefix)) {
				try {
					poolUtil.unlink(name);
				} catch {
					// ignore unlink failures
				}
			}
		}
		await wipePool();
	};

	let close: (() => Promise<any> | any) | undefined = async () => {
		await closeInternal();
		const preserve = Boolean(directory);
		await cleanupPool("close", preserve);
	};

	let drop = async () => {
		await closeInternal();
		await cleanupPool("drop", false);
	};
	let open = async () => {
		if (sqliteDb) {
			return sqliteDb;
		}
		if (directory) {
			// directory has to be absolute path. Remove leading dot if any
			// TODO show warning if directory is not absolute?
			directory = directory.replace(/^\./, "");

			dbFileName = `${directory}/db.sqlite`;
			const poolDirectory = `${directory}/peerbit/sqlite-opfs-pool`; // we do a unique directory else we will get problem open a client in multiple tabs
			const activePoolUtil =
				poolUtil ||
				(await sqlite3!.installOpfsSAHPoolVfs({
					directory: poolDirectory,
				}));
			poolUtil = activePoolUtil;

			await activePoolUtil.reserveMinimumCapacity(100);
			sqliteDb = new activePoolUtil.OpfsSAHPoolDb(dbFileName);
		} else {
			sqliteDb = new sqlite3!.oo1.DB(":memory:");
		}

		if (!sqliteDb) {
			throw new Error("Failed to open sqlite database");
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
		drop,
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
