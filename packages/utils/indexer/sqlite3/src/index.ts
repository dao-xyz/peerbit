import { BinaryWriter } from "@dao-xyz/borsh";
import { sha256Sync, toBase58 } from "@peerbit/crypto";
import { normalizeSQLiteDirectory } from "./directory.js";
import { SQLiteIndex, SQLiteIndices } from "./engine.js";
import type {
	SQLiteLockingMode,
	SQLitePragmaOptions,
	SQLiteSynchronousMode,
	SQLiteTempStoreMode,
	SqliteWorkerProtocol,
} from "./sqlite3-messages.worker.js";
import type {
	SQLiteBrowserOptions,
	SQLiteProfileSample,
} from "./sqlite3.browser.js";
import { create as sqlite3 } from "./sqlite3.js";

export const encodeName = (name: string): string => {
	const writer = new BinaryWriter();
	writer.string(name);
	return toBase58(sha256Sync(writer.finalize()));
};

const create = async (
	directory?: string,
	options?: SQLiteBrowserOptions,
): Promise<SQLiteIndices> => {
	const persistentDirectory = normalizeSQLiteDirectory(directory);
	const db = await sqlite3(persistentDirectory, options);
	return new SQLiteIndices({ db, directory: persistentDirectory });
};

const createDatabase = (directory?: string, options?: SQLiteBrowserOptions) =>
	sqlite3(normalizeSQLiteDirectory(directory), options);

export {
	create,
	createDatabase,
	SQLiteIndices,
	SQLiteIndex,
	SQLiteIndex as SQLLiteIndex,
};
export type {
	SQLiteBrowserOptions,
	SQLiteLockingMode,
	SQLitePragmaOptions,
	SQLiteProfileSample,
	SQLiteSynchronousMode,
	SQLiteTempStoreMode,
	SqliteWorkerProtocol,
};
