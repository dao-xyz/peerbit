import { BinaryWriter } from "@dao-xyz/borsh";
import { sha256Sync, toBase58 } from "@peerbit/crypto";
import { SQLiteIndex, SQLiteIndices } from "./engine.js";
import { create as sqlite3 } from "./sqlite3.js";
import type {
	SQLiteBrowserOptions,
	SQLiteProfileSample,
} from "./sqlite3.browser.js";
import type {
	SQLiteLockingMode,
	SQLitePragmaOptions,
	SQLiteSynchronousMode,
	SQLiteTempStoreMode,
	SqliteWorkerProtocol,
} from "./sqlite3-messages.worker.js";

export const encodeName = (name: string): string => {
	const writer = new BinaryWriter();
	writer.string(name);
	return toBase58(sha256Sync(writer.finalize()));
};

const create = async (
	directory?: string,
	options?: SQLiteBrowserOptions,
): Promise<SQLiteIndices> => {
	const db = await sqlite3(directory, options);
	return new SQLiteIndices({ db, directory });
};

const createDatabase = (
	directory?: string,
	options?: SQLiteBrowserOptions,
) => sqlite3(directory, options);

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
