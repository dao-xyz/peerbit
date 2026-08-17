import { deserialize, serialize } from "@dao-xyz/borsh";
import { PublicSignKey, sha256Sync, toHexString } from "@peerbit/crypto";
import type { createDatabase } from "@peerbit/indexer-sqlite3";
import type { NativeDurabilityLock } from "@peerbit/native-backbone";
import { captureBoundedUint8Array } from "./bounded-bytes.js";
import {
	CUSTODY_HANDOFF_PROFILE_ID,
	CUSTODY_HANDOFF_PROFILE_MASK,
} from "./custody-handoff-codec.js";
import {
	type CustodyRecordBinding,
	type CustodyRecordPersistence,
	type CustodyRecordRole,
	type CustodyRecordSlot,
	CustodyRecordStore,
	type CustodyStoreLimits,
	DEFAULT_CUSTODY_STORE_LIMITS,
} from "./custody-store.js";
import { MAX_U64 } from "./integers.js";

const CUSTODY_RECORD_DIRECTORY = "custody-records-v1";
const CUSTODY_DATABASE_FILE = "db.sqlite";
const CUSTODY_DATABASE_APPLICATION_ID = 0x50424355;
const CUSTODY_DATABASE_USER_VERSION = 1;
const MAX_BINDING_BYTES = 2 * 1024;
const MAX_NODE_PATH_BYTES = 4 * 1024;
const MAX_IDENTITY_BYTES = 512;
const MAX_SCHEMA_BYTES = 4 * 1024;
const SQLITE_FULL_SYNCHRONOUS = 2n;
const MOVE_KEY_PATTERN = /^[0-9a-f]{64}$/;
const DECIMAL_PATTERN = /^(0|[1-9][0-9]*)$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

const META_SCHEMA =
	"CREATE TABLE custody_meta (id INTEGER PRIMARY KEY CHECK (id = 1), binding BLOB NOT NULL CHECK (typeof(binding) = 'blob' AND octet_length(binding) BETWEEN 1 AND 2048), namespace_epoch BLOB NOT NULL CHECK (typeof(namespace_epoch) = 'blob' AND octet_length(namespace_epoch) = 32), domain_id BLOB NOT NULL CHECK (typeof(domain_id) = 'blob' AND octet_length(domain_id) = 32), writer_epoch TEXT NOT NULL CHECK (typeof(writer_epoch) = 'text' AND length(writer_epoch) BETWEEN 1 AND 20 AND writer_epoch NOT GLOB '*[^0-9]*'), writer_owner BLOB NOT NULL CHECK (typeof(writer_owner) = 'blob' AND octet_length(writer_owner) = 32)) STRICT, WITHOUT ROWID";
const RECORD_SCHEMA =
	"CREATE TABLE custody_records (move_key TEXT NOT NULL CHECK (typeof(move_key) = 'text' AND length(move_key) = 64 AND move_key NOT GLOB '*[^0-9a-f]*'), slot TEXT NOT NULL CHECK (slot IN ('a', 'b')), frame BLOB NOT NULL CHECK (typeof(frame) = 'blob' AND octet_length(frame) BETWEEN 1 AND 16384), domain_id BLOB NOT NULL CHECK (typeof(domain_id) = 'blob' AND octet_length(domain_id) = 32), writer_epoch TEXT NOT NULL CHECK (typeof(writer_epoch) = 'text' AND length(writer_epoch) BETWEEN 1 AND 20 AND writer_epoch NOT GLOB '*[^0-9]*'), writer_owner BLOB NOT NULL CHECK (typeof(writer_owner) = 'blob' AND octet_length(writer_owner) = 32), PRIMARY KEY (move_key, slot)) STRICT, WITHOUT ROWID";

type SqliteDatabase = Awaited<ReturnType<typeof createDatabase>>;
type SqliteStatement = Awaited<ReturnType<SqliteDatabase["prepare"]>>;

type NativeBackboneLockModule = Readonly<{
	acquireNativeDurabilityNodeLock(
		directory: string,
	): Promise<NativeDurabilityLock>;
}>;

export type CustodyRecordNodeDirectoryHandle = Readonly<{
	sync(): Promise<void>;
	close(): Promise<void>;
}>;

export type CustodyRecordNodePathFacts = Readonly<{
	isDirectory(): boolean;
	isFile(): boolean;
	isSymbolicLink(): boolean;
}>;

export type CustodyRecordNodeFileSystem = Readonly<{
	realpath(path: string): Promise<string>;
	stat(path: string): Promise<CustodyRecordNodePathFacts>;
	lstat(path: string): Promise<CustodyRecordNodePathFacts>;
	mkdir(path: string): Promise<unknown>;
	open(path: string, flags: "r"): Promise<CustodyRecordNodeDirectoryHandle>;
}>;

export type CustodyRecordNodePath = Readonly<{
	join(...parts: string[]): string;
	dirname(path: string): string;
}>;

export type CustodyRecordNodeCrypto = Readonly<{
	randomBytes(size: number): Uint8Array;
}>;

export type CustodyRecordSqliteModule = Readonly<{
	createDatabase: typeof createDatabase;
}>;

export type CustodyRecordNodeModules = Readonly<{
	fs: CustodyRecordNodeFileSystem;
	path: CustodyRecordNodePath;
	crypto: CustodyRecordNodeCrypto;
	native: NativeBackboneLockModule;
	sqlite: CustodyRecordSqliteModule;
}>;

export type CustodyRecordNodePersistenceFacts = Readonly<{
	namespace: string;
	namespaceEpoch: string;
	writerEpoch: bigint;
	writerOwner: string;
	domainId: string;
}>;

/** @internal Deterministic fault-injection seam for the direct adapter spec. */
export type CustodyRecordNodePersistenceDependencies = Readonly<{
	loadNodeModules?(): Promise<CustodyRecordNodeModules>;
	onPersistenceCreated?(
		persistence: CustodyRecordPersistence,
		facts: CustodyRecordNodePersistenceFacts,
	): void;
}>;

export type OpenNodeCustodyRecordStoreOptions = Readonly<{
	nodeDirectory: string;
	logId: Uint8Array;
	localPublicKey: Uint8Array;
	role: CustodyRecordRole;
	limits?: Partial<CustodyStoreLimits>;
}>;

type CapturedNodeOptions = Readonly<{
	nodeDirectory: string;
	binding: CustodyRecordBinding;
	limits: CustodyStoreLimits;
}>;

type CapturedMetadata = Readonly<{
	namespaceEpoch: Uint8Array;
	domainId: Uint8Array;
	writerEpoch: bigint;
	writerOwner: Uint8Array;
}>;

type PreparedStatements = Readonly<{
	read: SqliteStatement;
	write: SqliteStatement;
	barrierTarget: SqliteStatement;
	checkpoint: SqliteStatement;
}>;

const dynamicImport = <T>(specifier: string): Promise<T> =>
	import(/* @vite-ignore */ specifier) as Promise<T>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
	value != null && typeof value === "object" && !Array.isArray(value);

const hasErrorCode = (error: unknown, code: string): boolean => {
	const seen = new Set<unknown>();
	let current = error;
	while (current != null && typeof current === "object" && !seen.has(current)) {
		seen.add(current);
		if ((current as { code?: unknown }).code === code) return true;
		current = (current as { cause?: unknown }).cause;
	}
	return false;
};

const isWellFormedString = (value: string): boolean => {
	for (let index = 0; index < value.length; index++) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = value.charCodeAt(++index);
			if (next < 0xdc00 || next > 0xdfff) return false;
		} else if (code >= 0xdc00 && code <= 0xdfff) {
			return false;
		}
	}
	return true;
};

const assertMoveKey = (value: unknown): string => {
	if (
		typeof value !== "string" ||
		value.length !== 64 ||
		!MOVE_KEY_PATTERN.test(value)
	) {
		throw new Error("Invalid custody record move key");
	}
	return value;
};

const assertSlot = (value: unknown): CustodyRecordSlot => {
	if (value !== "a" && value !== "b") {
		throw new Error("Invalid custody record slot");
	}
	return value;
};

const assertPositiveLimit = (
	value: unknown,
	name: string,
	hardMaximum: number,
): number => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value <= 0 ||
		value > hardMaximum
	) {
		throw new RangeError(`Invalid ${name}`);
	}
	return value;
};

const captureLimits = (
	value: Partial<CustodyStoreLimits> | undefined,
): CustodyStoreLimits =>
	Object.freeze({
		maxArtifactBytes: assertPositiveLimit(
			value?.maxArtifactBytes ?? DEFAULT_CUSTODY_STORE_LIMITS.maxArtifactBytes,
			"custody artifact byte bound",
			DEFAULT_CUSTODY_STORE_LIMITS.maxArtifactBytes,
		),
		maxFrameBytes: assertPositiveLimit(
			value?.maxFrameBytes ?? DEFAULT_CUSTODY_STORE_LIMITS.maxFrameBytes,
			"custody frame byte bound",
			DEFAULT_CUSTODY_STORE_LIMITS.maxFrameBytes,
		),
		maxPendingOperations: assertPositiveLimit(
			value?.maxPendingOperations ??
				DEFAULT_CUSTODY_STORE_LIMITS.maxPendingOperations,
			"custody pending-operation bound",
			DEFAULT_CUSTODY_STORE_LIMITS.maxPendingOperations,
		),
	});

const captureIdentityBytes = (value: unknown, name: string): Uint8Array => {
	try {
		return captureBoundedUint8Array(value, 1, MAX_IDENTITY_BYTES, name);
	} catch {
		throw new TypeError(`${name} must contain 1-${MAX_IDENTITY_BYTES} bytes`);
	}
};

const captureCanonicalPublicKeyBytes = (value: unknown): Uint8Array => {
	const bytes = captureIdentityBytes(value, "localPublicKey");
	let key: PublicSignKey;
	try {
		key = deserialize(bytes, PublicSignKey);
	} catch (error) {
		throw new Error("Invalid local canonical public key", { cause: error });
	}
	if (!(key instanceof PublicSignKey)) {
		throw new Error("Invalid local canonical public key");
	}
	const canonical = serialize(key);
	if (
		canonical.byteLength !== bytes.byteLength ||
		canonical.some((byte, index) => byte !== bytes[index])
	) {
		throw new Error("Invalid local canonical public key");
	}
	return bytes;
};

const captureOptions = (
	value: OpenNodeCustodyRecordStoreOptions,
): CapturedNodeOptions => {
	const nodeDirectory = value?.nodeDirectory;
	const logId = value?.logId;
	const localPublicKey = value?.localPublicKey;
	const role = value?.role;
	const limits = value?.limits;
	if (
		typeof nodeDirectory !== "string" ||
		nodeDirectory.length === 0 ||
		nodeDirectory.length > MAX_NODE_PATH_BYTES ||
		!isWellFormedString(nodeDirectory) ||
		encoder.encode(nodeDirectory).byteLength > MAX_NODE_PATH_BYTES
	) {
		throw new TypeError("nodeDirectory must be a bounded non-empty string");
	}
	if (role !== "source" && role !== "destination") {
		throw new TypeError("Invalid custody record role");
	}
	return Object.freeze({
		nodeDirectory,
		binding: Object.freeze({
			logId: captureIdentityBytes(logId, "logId"),
			localPublicKey: captureCanonicalPublicKeyBytes(localPublicKey),
			role,
		}),
		limits: captureLimits(limits),
	});
};

const syncDirectoryStrict = async (
	fs: CustodyRecordNodeFileSystem,
	directory: string,
): Promise<void> => {
	let handle: CustodyRecordNodeDirectoryHandle | undefined;
	let operationError: unknown;
	try {
		handle = await fs.open(directory, "r");
		await handle.sync();
	} catch (error) {
		operationError = error;
	}
	let closeError: unknown;
	try {
		await handle?.close();
	} catch (error) {
		closeError = error;
	}
	if (operationError !== undefined && closeError !== undefined) {
		throw new AggregateError(
			[operationError, closeError],
			`Failed to sync and close custody directory ${directory}`,
		);
	}
	if (operationError !== undefined) throw operationError;
	if (closeError !== undefined) throw closeError;
};

const ensureCanonicalChildDirectory = async (
	fs: CustodyRecordNodeFileSystem,
	path: CustodyRecordNodePath,
	canonicalParent: string,
	name: string,
): Promise<string> => {
	const requested = path.join(canonicalParent, name);
	try {
		await fs.mkdir(requested);
	} catch (error) {
		if (!hasErrorCode(error, "EEXIST")) throw error;
	}
	const facts = await fs.stat(requested);
	if (!facts.isDirectory()) {
		throw new Error(
			`Custody persistence path is not a directory: ${requested}`,
		);
	}
	// Repeat this after EEXIST too: a prior creation may have become visible
	// before its parent-directory sync failed.
	await syncDirectoryStrict(fs, canonicalParent);
	const canonical = await fs.realpath(requested);
	if (canonical !== requested || path.dirname(canonical) !== canonicalParent) {
		throw new Error(
			`Custody persistence directory is not its canonical requested path: ${canonical}`,
		);
	}
	return canonical;
};

const assertCanonicalDatabaseFile = async (
	fs: CustodyRecordNodeFileSystem,
	databasePath: string,
	allowMissing: boolean,
): Promise<boolean> => {
	let facts: CustodyRecordNodePathFacts;
	try {
		facts = await fs.lstat(databasePath);
	} catch (error) {
		if (allowMissing && hasErrorCode(error, "ENOENT")) return false;
		throw error;
	}
	if (!facts.isFile() || facts.isSymbolicLink()) {
		throw new Error(
			`Custody database is not a canonical regular file: ${databasePath}`,
		);
	}
	return true;
};

const randomNonZeroGeneration = (
	crypto: CustodyRecordNodeCrypto,
	name: string,
): Uint8Array => {
	for (let attempt = 0; attempt < 8; attempt++) {
		let copy: Uint8Array;
		try {
			copy = captureBoundedUint8Array(
				crypto.randomBytes(32),
				32,
				32,
				`random ${name}`,
			);
		} catch {
			throw new Error(`Invalid random ${name}`);
		}
		if (copy.some((byte) => byte !== 0)) return copy;
	}
	throw new Error(`Failed to create non-zero ${name}`);
};

const u16 = (view: DataView, offset: number, value: number) => {
	view.setUint16(offset, value, true);
	return offset + 2;
};

const u32 = (view: DataView, offset: number, value: number) => {
	view.setUint32(offset, value, true);
	return offset + 4;
};

const bindingBytes = (
	canonicalRoot: string,
	binding: CustodyRecordBinding,
): Uint8Array => {
	if (
		canonicalRoot.length === 0 ||
		canonicalRoot.length > MAX_NODE_PATH_BYTES ||
		!isWellFormedString(canonicalRoot)
	) {
		throw new Error("Canonical node path exceeds custody byte bound");
	}
	const rootBytes = encoder.encode(canonicalRoot);
	if (
		rootBytes.byteLength === 0 ||
		rootBytes.byteLength > MAX_NODE_PATH_BYTES
	) {
		throw new Error("Canonical node path exceeds custody byte bound");
	}
	const rootDigest = sha256Sync(rootBytes);
	const profileBytes = encoder.encode(CUSTODY_HANDOFF_PROFILE_ID);
	const domain = encoder.encode(
		"peerbit/shared-log/custody-record-namespace/v1",
	);
	const total =
		2 +
		domain.byteLength +
		rootDigest.byteLength +
		2 +
		binding.logId.byteLength +
		2 +
		binding.localPublicKey.byteLength +
		1 +
		2 +
		profileBytes.byteLength +
		4;
	if (total > MAX_BINDING_BYTES) {
		throw new Error("Custody namespace binding exceeds byte bound");
	}
	const bytes = new Uint8Array(total);
	const view = new DataView(bytes.buffer);
	let offset = u16(view, 0, domain.byteLength);
	bytes.set(domain, offset);
	offset += domain.byteLength;
	bytes.set(rootDigest, offset);
	offset += rootDigest.byteLength;
	offset = u16(view, offset, binding.logId.byteLength);
	bytes.set(binding.logId, offset);
	offset += binding.logId.byteLength;
	offset = u16(view, offset, binding.localPublicKey.byteLength);
	bytes.set(binding.localPublicKey, offset);
	offset += binding.localPublicKey.byteLength;
	bytes[offset++] = binding.role === "source" ? 1 : 2;
	offset = u16(view, offset, profileBytes.byteLength);
	bytes.set(profileBytes, offset);
	offset += profileBytes.byteLength;
	offset = u32(view, offset, CUSTODY_HANDOFF_PROFILE_MASK);
	if (offset !== bytes.byteLength) {
		throw new Error("Custody namespace binding encoder mismatch");
	}
	return bytes;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array) => {
	if (left.byteLength !== right.byteLength) return false;
	let different = 0;
	for (let index = 0; index < left.byteLength; index++) {
		different |= left[index]! ^ right[index]!;
	}
	return different === 0;
};

const boundedBytes = (
	value: unknown,
	reportedLength: unknown,
	maximum: number,
	name: string,
): Uint8Array => {
	if (
		typeof reportedLength !== "bigint" ||
		reportedLength <= 0n ||
		reportedLength > BigInt(maximum)
	) {
		throw new Error(`Invalid bounded custody ${name}`);
	}
	try {
		const exact = Number(reportedLength);
		return captureBoundedUint8Array(value, exact, exact, `custody ${name}`);
	} catch (error) {
		throw new Error(`Invalid bounded custody ${name}`, { cause: error });
	}
};

const boundedText = (
	value: unknown,
	reportedLength: unknown,
	maximum: number,
	name: string,
): string => decoder.decode(boundedBytes(value, reportedLength, maximum, name));

const parseWriterEpoch = (value: string): bigint => {
	if (value.length === 0 || value.length > 20 || !DECIMAL_PATTERN.test(value)) {
		throw new Error("Invalid custody writer epoch");
	}
	const parsed = BigInt(value);
	if (parsed <= 0n || parsed > MAX_U64) {
		throw new Error("Invalid custody writer epoch");
	}
	return parsed;
};

const asRows = (
	value: unknown,
	name: string,
	maxRows: number,
): Record<string, unknown>[] => {
	if (!Array.isArray(value)) {
		throw new Error(`Invalid ${name} result`);
	}
	const length = value.length;
	if (!Number.isSafeInteger(length) || length < 0 || length > maxRows) {
		throw new Error(`Invalid ${name} result cardinality`);
	}
	const rows: Record<string, unknown>[] = [];
	for (let index = 0; index < length; index++) {
		const row = value[index];
		if (!isRecord(row)) throw new Error(`Invalid ${name} result row`);
		rows.push(row);
	}
	return rows;
};

const assertPragmas = async (database: SqliteDatabase) => {
	const read = async (sql: string) => {
		const statement = await database.prepare(sql);
		return asRows(await statement.all([]), sql, 1);
	};
	const journal = await read("PRAGMA journal_mode");
	const synchronous = await read("PRAGMA synchronous");
	const locking = await read("PRAGMA locking_mode");
	if (
		journal.length !== 1 ||
		journal[0]!.journal_mode !== "wal" ||
		synchronous.length !== 1 ||
		BigInt(synchronous[0]!.synchronous as number | bigint) !==
			SQLITE_FULL_SYNCHRONOUS ||
		locking.length !== 1 ||
		locking[0]!.locking_mode !== "exclusive"
	) {
		throw new Error("Custody SQLite durability pragmas are not strict");
	}
};

const assertCheckpoint = (value: unknown) => {
	const rows = asRows(value, "custody SQLite checkpoint", 1);
	if (rows.length !== 1) {
		throw new Error("Invalid custody SQLite checkpoint result");
	}
	const busy = rows[0]!.busy;
	const log = rows[0]!.log;
	const checkpointed = rows[0]!.checkpointed;
	if (
		(typeof busy !== "bigint" && typeof busy !== "number") ||
		(typeof log !== "bigint" && typeof log !== "number") ||
		(typeof checkpointed !== "bigint" && typeof checkpointed !== "number") ||
		BigInt(busy) !== 0n ||
		BigInt(log) < 0n ||
		BigInt(checkpointed) < 0n ||
		BigInt(log) !== BigInt(checkpointed)
	) {
		throw new Error("Custody SQLite checkpoint did not fully complete");
	}
};

const readPragmaInteger = async (
	database: SqliteDatabase,
	pragma: "application_id" | "user_version",
): Promise<bigint> => {
	const statement = await database.prepare(`PRAGMA ${pragma}`);
	const rows = asRows(await statement.all([]), `custody SQLite ${pragma}`, 1);
	const value = rows[0]?.[pragma];
	if (
		rows.length !== 1 ||
		(typeof value !== "number" && typeof value !== "bigint")
	) {
		throw new Error(`Invalid custody SQLite ${pragma}`);
	}
	return BigInt(value);
};

const schemaObjectCount = async (database: SqliteDatabase): Promise<number> => {
	const statement = await database.prepare(
		"SELECT 1 AS marker FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 3",
	);
	const rows = asRows(await statement.all([]), "custody SQLite schema", 3);
	if (rows.some((row) => BigInt(row.marker as number | bigint) !== 1n)) {
		throw new Error("Invalid custody SQLite schema marker");
	}
	return rows.length;
};

const assertSchemaObject = async (
	database: SqliteDatabase,
	name: "custody_meta" | "custody_records",
	expectedSql: string,
) => {
	const statement = await database.prepare(
		"SELECT octet_length(type) AS type_bytes, substr(CAST(type AS BLOB), 1, 17) AS type_prefix, octet_length(sql) AS sql_bytes, substr(CAST(sql AS BLOB), 1, ?) AS sql_prefix FROM sqlite_schema WHERE name = ? LIMIT 2",
	);
	const rows = asRows(
		await statement.all([MAX_SCHEMA_BYTES + 1, name]),
		"custody SQLite schema object",
		2,
	);
	if (rows.length !== 1) {
		throw new Error(`Invalid custody SQLite schema object ${name}`);
	}
	const type = boundedText(
		rows[0]!.type_prefix,
		rows[0]!.type_bytes,
		16,
		"schema type",
	);
	const sql = boundedText(
		rows[0]!.sql_prefix,
		rows[0]!.sql_bytes,
		MAX_SCHEMA_BYTES,
		"schema SQL",
	);
	if (type !== "table" || sql !== expectedSql) {
		throw new Error(`Unexpected custody SQLite schema object ${name}`);
	}
};

const assertExistingSchema = async (database: SqliteDatabase) => {
	if ((await schemaObjectCount(database)) !== 2) {
		throw new Error("Unexpected custody SQLite schema cardinality");
	}
	await assertSchemaObject(database, "custody_meta", META_SCHEMA);
	await assertSchemaObject(database, "custody_records", RECORD_SCHEMA);
};

const readMetadata = async (
	database: SqliteDatabase,
	expectedBinding: Uint8Array,
): Promise<CapturedMetadata> => {
	const statement = await database.prepare(
		"SELECT octet_length(binding) AS binding_bytes, substr(binding, 1, ?) AS binding_prefix, octet_length(namespace_epoch) AS namespace_epoch_bytes, substr(namespace_epoch, 1, 33) AS namespace_epoch_prefix, octet_length(domain_id) AS domain_id_bytes, substr(domain_id, 1, 33) AS domain_id_prefix, octet_length(CAST(writer_epoch AS BLOB)) AS writer_epoch_bytes, substr(CAST(writer_epoch AS BLOB), 1, 21) AS writer_epoch_prefix, octet_length(writer_owner) AS writer_owner_bytes, substr(writer_owner, 1, 33) AS writer_owner_prefix FROM custody_meta WHERE id = 1 LIMIT 2",
	);
	const rows = asRows(
		await statement.all([MAX_BINDING_BYTES + 1]),
		"custody SQLite metadata",
		2,
	);
	if (rows.length !== 1) {
		throw new Error("Missing or duplicate custody SQLite metadata");
	}
	const row = rows[0]!;
	const binding = boundedBytes(
		row.binding_prefix,
		row.binding_bytes,
		MAX_BINDING_BYTES,
		"namespace binding",
	);
	if (!bytesEqual(binding, expectedBinding)) {
		throw new Error("Custody SQLite namespace binding mismatch");
	}
	const namespaceEpoch = boundedBytes(
		row.namespace_epoch_prefix,
		row.namespace_epoch_bytes,
		32,
		"namespace epoch",
	);
	const domainId = boundedBytes(
		row.domain_id_prefix,
		row.domain_id_bytes,
		32,
		"domain id",
	);
	const writerEpoch = parseWriterEpoch(
		boundedText(
			row.writer_epoch_prefix,
			row.writer_epoch_bytes,
			20,
			"writer epoch",
		),
	);
	const writerOwner = boundedBytes(
		row.writer_owner_prefix,
		row.writer_owner_bytes,
		32,
		"writer owner",
	);
	if (
		!namespaceEpoch.some((byte) => byte !== 0) ||
		!domainId.some((byte) => byte !== 0) ||
		!writerOwner.some((byte) => byte !== 0)
	) {
		throw new Error("Custody SQLite metadata contains a zero generation");
	}
	return Object.freeze({
		namespaceEpoch,
		domainId,
		writerEpoch,
		writerOwner,
	});
};

const runTransaction = async <T>(
	database: SqliteDatabase,
	operation: () => Promise<T>,
): Promise<T> => {
	await database.exec("BEGIN IMMEDIATE");
	try {
		const result = await operation();
		await database.exec("COMMIT");
		return result;
	} catch (error) {
		let rollbackError: unknown;
		try {
			await database.exec("ROLLBACK");
		} catch (candidate) {
			rollbackError = candidate;
		}
		if (rollbackError !== undefined) {
			throw new AggregateError(
				[error, rollbackError],
				"Failed to initialize and roll back custody SQLite metadata",
			);
		}
		throw error;
	}
};

const initializeMetadata = async (
	database: SqliteDatabase,
	crypto: CustodyRecordNodeCrypto,
	expectedBinding: Uint8Array,
	allowGenesis: boolean,
): Promise<CapturedMetadata> => {
	const objectCount = await schemaObjectCount(database);
	if (objectCount === 0) {
		if (
			!allowGenesis ||
			(await readPragmaInteger(database, "application_id")) !== 0n ||
			(await readPragmaInteger(database, "user_version")) !== 0n
		) {
			throw new Error("Custody SQLite genesis is not a new empty database");
		}
		const metadata: CapturedMetadata = Object.freeze({
			namespaceEpoch: randomNonZeroGeneration(
				crypto,
				"custody namespace epoch",
			),
			domainId: randomNonZeroGeneration(crypto, "custody domain id"),
			writerEpoch: 1n,
			writerOwner: randomNonZeroGeneration(crypto, "custody writer owner"),
		});
		return runTransaction(database, async () => {
			await database.exec(`${META_SCHEMA}; ${RECORD_SCHEMA}`);
			await database.exec(
				`PRAGMA application_id = ${CUSTODY_DATABASE_APPLICATION_ID}`,
			);
			await database.exec(
				`PRAGMA user_version = ${CUSTODY_DATABASE_USER_VERSION}`,
			);
			const insert = await database.prepare(
				"INSERT INTO custody_meta (id, binding, namespace_epoch, domain_id, writer_epoch, writer_owner) VALUES (1, ?, ?, ?, ?, ?)",
			);
			await insert.run([
				expectedBinding,
				metadata.namespaceEpoch,
				metadata.domainId,
				metadata.writerEpoch.toString(),
				metadata.writerOwner,
			]);
			return metadata;
		});
	}
	if (
		(await readPragmaInteger(database, "application_id")) !==
			BigInt(CUSTODY_DATABASE_APPLICATION_ID) ||
		(await readPragmaInteger(database, "user_version")) !==
			BigInt(CUSTODY_DATABASE_USER_VERSION)
	) {
		throw new Error("Custody SQLite database version mismatch");
	}
	await assertExistingSchema(database);
	const previous = await readMetadata(database, expectedBinding);
	if (previous.writerEpoch === MAX_U64) {
		throw new Error("Custody SQLite writer epoch is exhausted");
	}
	const next: CapturedMetadata = Object.freeze({
		namespaceEpoch: previous.namespaceEpoch,
		domainId: previous.domainId,
		writerEpoch: previous.writerEpoch + 1n,
		writerOwner: randomNonZeroGeneration(crypto, "custody writer owner"),
	});
	await runTransaction(database, async () => {
		const update = await database.prepare(
			"UPDATE custody_meta SET writer_epoch = ?, writer_owner = ? WHERE id = 1",
		);
		await update.run([next.writerEpoch.toString(), next.writerOwner]);
		const confirmed = await readMetadata(database, expectedBinding);
		if (
			confirmed.writerEpoch !== next.writerEpoch ||
			!bytesEqual(confirmed.writerOwner, next.writerOwner) ||
			!bytesEqual(confirmed.domainId, next.domainId) ||
			!bytesEqual(confirmed.namespaceEpoch, next.namespaceEpoch)
		) {
			throw new Error("Custody SQLite writer fence update did not persist");
		}
	});
	return next;
};

const prepareStatements = async (
	database: SqliteDatabase,
): Promise<PreparedStatements> =>
	Object.freeze({
		read: await database.prepare(
			"SELECT typeof(frame) AS frame_type, octet_length(frame) AS frame_bytes, substr(frame, 1, ?) AS frame_prefix, octet_length(domain_id) AS domain_id_bytes, substr(domain_id, 1, 33) AS domain_id_prefix, octet_length(CAST(writer_epoch AS BLOB)) AS writer_epoch_bytes, substr(CAST(writer_epoch AS BLOB), 1, 21) AS writer_epoch_prefix, octet_length(writer_owner) AS writer_owner_bytes, substr(writer_owner, 1, 33) AS writer_owner_prefix FROM custody_records WHERE move_key = ? AND slot = ? LIMIT 2",
			"custody-record-read",
		),
		write: await database.prepare(
			"INSERT INTO custody_records (move_key, slot, frame, domain_id, writer_epoch, writer_owner) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (move_key, slot) DO UPDATE SET frame = excluded.frame, domain_id = excluded.domain_id, writer_epoch = excluded.writer_epoch, writer_owner = excluded.writer_owner",
			"custody-record-write",
		),
		barrierTarget: await database.prepare(
			"SELECT octet_length(domain_id) AS domain_id_bytes, substr(domain_id, 1, 33) AS domain_id_prefix, octet_length(CAST(writer_epoch AS BLOB)) AS writer_epoch_bytes, substr(CAST(writer_epoch AS BLOB), 1, 21) AS writer_epoch_prefix, octet_length(writer_owner) AS writer_owner_bytes, substr(writer_owner, 1, 33) AS writer_owner_prefix FROM custody_records WHERE move_key = ? AND slot = ? LIMIT 2",
			"custody-record-barrier-target",
		),
		checkpoint: await database.prepare(
			"PRAGMA wal_checkpoint(FULL)",
			"custody-record-checkpoint",
		),
	});

class NodeCustodyRecordPersistence implements CustodyRecordPersistence {
	private tail: Promise<void> = Promise.resolve();
	private closing = false;
	private closed = false;
	private closePromise?: Promise<void>;

	constructor(
		private readonly database: SqliteDatabase,
		private readonly statements: PreparedStatements,
		private readonly lock: NativeDurabilityLock,
		private readonly fs: CustodyRecordNodeFileSystem,
		private readonly namespace: string,
		private readonly metadata: CapturedMetadata,
		private readonly maxFrameBytes: number,
	) {}

	get facts(): CustodyRecordNodePersistenceFacts {
		return Object.freeze({
			namespace: this.namespace,
			namespaceEpoch: toHexString(this.metadata.namespaceEpoch),
			writerEpoch: this.metadata.writerEpoch,
			writerOwner: toHexString(this.metadata.writerOwner),
			domainId: toHexString(this.metadata.domainId),
		});
	}

	read(
		moveKey: string,
		slot: CustodyRecordSlot,
		maxBytes: number,
	): Promise<Uint8Array | undefined> {
		this.assertAccepting();
		const capturedMoveKey = assertMoveKey(moveKey);
		const capturedSlot = assertSlot(slot);
		const capturedLimit = assertPositiveLimit(
			maxBytes,
			"custody persistence read bound",
			this.maxFrameBytes,
		);
		return this.enqueue(async () => {
			const rows = asRows(
				await this.statements.read.all([
					capturedLimit + 1,
					capturedMoveKey,
					capturedSlot,
				]),
				"custody SQLite record read",
				2,
			);
			if (rows.length === 0) return undefined;
			if (rows.length !== 1) {
				throw new Error("Duplicate custody SQLite record");
			}
			const row = rows[0]!;
			if (row.frame_type !== "blob") {
				throw new Error("Custody SQLite record is not a BLOB");
			}
			if (
				typeof row.frame_bytes !== "bigint" ||
				row.frame_bytes <= 0n ||
				row.frame_bytes > BigInt(capturedLimit)
			) {
				throw new RangeError("Custody SQLite record exceeds read byte bound");
			}
			const frame = boundedBytes(
				row.frame_prefix,
				row.frame_bytes,
				capturedLimit,
				"record frame",
			);
			this.assertWriterFence(row);
			return frame;
		});
	}

	write(
		moveKey: string,
		slot: CustodyRecordSlot,
		bytes: Uint8Array,
	): Promise<void> {
		this.assertAccepting();
		const capturedMoveKey = assertMoveKey(moveKey);
		const capturedSlot = assertSlot(slot);
		let captured: Uint8Array;
		try {
			captured = captureBoundedUint8Array(
				bytes,
				1,
				this.maxFrameBytes,
				"custody persistence write",
			);
		} catch (error) {
			return Promise.reject(
				new RangeError("Invalid custody persistence write", { cause: error }),
			);
		}
		return this.enqueue(async () => {
			await this.statements.write.run([
				capturedMoveKey,
				capturedSlot,
				captured,
				this.metadata.domainId,
				this.metadata.writerEpoch.toString(),
				this.metadata.writerOwner,
			]);
		});
	}

	durableBarrier(moveKey: string, slot: CustodyRecordSlot): Promise<void> {
		this.assertAccepting();
		const capturedMoveKey = assertMoveKey(moveKey);
		const capturedSlot = assertSlot(slot);
		return this.enqueue(async () => {
			const targets = asRows(
				await this.statements.barrierTarget.all([
					capturedMoveKey,
					capturedSlot,
				]),
				"custody SQLite barrier target",
				2,
			);
			if (targets.length !== 1) {
				throw new Error("Missing or duplicate custody SQLite barrier target");
			}
			this.assertWriterFence(targets[0]!);
			assertCheckpoint(await this.statements.checkpoint.all([]));
			await syncDirectoryStrict(this.fs, this.namespace);
		});
	}

	close(_options?: { flush?: false }): Promise<void> {
		if (this.closePromise) return this.closePromise;
		this.closing = true;
		this.closePromise = (async () => {
			await this.tail;
			let databaseError: unknown;
			try {
				await this.lock.runWhileHeld(async () => {
					await this.database.close();
				});
			} catch (error) {
				databaseError = error;
			}
			let lockError: unknown;
			try {
				await this.lock.close();
			} catch (error) {
				lockError = error;
			} finally {
				this.closed = true;
			}
			if (databaseError !== undefined && lockError !== undefined) {
				throw new AggregateError(
					[databaseError, lockError],
					"Failed to close custody SQLite database and directory lock",
				);
			}
			if (databaseError !== undefined) throw databaseError;
			if (lockError !== undefined) throw lockError;
		})();
		return this.closePromise;
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		this.assertAccepting();
		const result = this.tail.then(() => this.lock.runWhileHeld(operation));
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	private assertAccepting() {
		if (this.closing || this.closed) {
			throw new Error("Custody SQLite persistence is closing");
		}
	}

	private assertWriterFence(row: Record<string, unknown>) {
		const domainId = boundedBytes(
			row.domain_id_prefix,
			row.domain_id_bytes,
			32,
			"record domain id",
		);
		const writerEpoch = parseWriterEpoch(
			boundedText(
				row.writer_epoch_prefix,
				row.writer_epoch_bytes,
				20,
				"record writer epoch",
			),
		);
		const writerOwner = boundedBytes(
			row.writer_owner_prefix,
			row.writer_owner_bytes,
			32,
			"record writer owner",
		);
		if (
			!bytesEqual(domainId, this.metadata.domainId) ||
			writerEpoch > this.metadata.writerEpoch ||
			!writerOwner.some((byte) => byte !== 0) ||
			(writerEpoch === this.metadata.writerEpoch &&
				!bytesEqual(writerOwner, this.metadata.writerOwner))
		) {
			throw new Error("Custody SQLite record has an invalid writer fence");
		}
	}
}

const loadNodeModules = async (): Promise<CustodyRecordNodeModules> => {
	const processLike = (
		globalThis as { process?: { versions?: { node?: string } } }
	).process;
	if (!processLike?.versions?.node) {
		throw new Error("Persistent custody records are only supported in Node.js");
	}
	const [fs, path, crypto, native, sqlite] = await Promise.all([
		dynamicImport<CustodyRecordNodeFileSystem>("node:fs/promises"),
		dynamicImport<CustodyRecordNodePath>("node:path"),
		dynamicImport<CustodyRecordNodeCrypto>("node:crypto"),
		dynamicImport<NativeBackboneLockModule>("@peerbit/native-backbone"),
		dynamicImport<CustodyRecordSqliteModule>("@peerbit/indexer-sqlite3"),
	]);
	return { fs, path, crypto, native, sqlite };
};

const cleanupCreationFailure = async (
	primary: unknown,
	database: SqliteDatabase | undefined,
	lock: NativeDurabilityLock,
): Promise<never> => {
	const errors: unknown[] = [primary];
	if (database !== undefined) {
		try {
			await lock.runWhileHeld(async () => {
				await database.close();
			});
		} catch (error) {
			errors.push(error);
		}
	}
	try {
		await lock.close();
	} catch (error) {
		errors.push(error);
	}
	if (errors.length > 1) {
		throw new AggregateError(
			errors,
			"Failed to construct and close custody SQLite persistence",
		);
	}
	throw primary;
};

const createNodePersistence = async (
	options: CapturedNodeOptions,
	dependencies: CustodyRecordNodePersistenceDependencies,
): Promise<NodeCustodyRecordPersistence> => {
	const { fs, path, crypto, native, sqlite } = await (
		dependencies.loadNodeModules ?? loadNodeModules
	)();
	const canonicalRoot = await fs.realpath(options.nodeDirectory);
	const rootFacts = await fs.stat(canonicalRoot);
	if (!rootFacts.isDirectory()) {
		throw new Error(
			`Peerbit node directory is not a directory: ${canonicalRoot}`,
		);
	}
	const binding = bindingBytes(canonicalRoot, options.binding);
	const custodyRoot = await ensureCanonicalChildDirectory(
		fs,
		path,
		canonicalRoot,
		CUSTODY_RECORD_DIRECTORY,
	);
	const logNamespace = await ensureCanonicalChildDirectory(
		fs,
		path,
		custodyRoot,
		toHexString(sha256Sync(options.binding.logId)),
	);
	const identityNamespace = await ensureCanonicalChildDirectory(
		fs,
		path,
		logNamespace,
		toHexString(sha256Sync(options.binding.localPublicKey)),
	);
	const namespace = await ensureCanonicalChildDirectory(
		fs,
		path,
		identityNamespace,
		options.binding.role,
	);
	await syncDirectoryStrict(fs, namespace);
	const databasePath = path.join(namespace, CUSTODY_DATABASE_FILE);
	const databaseExistedBeforeLock = await assertCanonicalDatabaseFile(
		fs,
		databasePath,
		true,
	);
	const companionPaths = [
		`${databasePath}-wal`,
		`${databasePath}-shm`,
		`${databasePath}-journal`,
	];
	const companionPresence = await Promise.all(
		companionPaths.map((candidate) =>
			assertCanonicalDatabaseFile(fs, candidate, true),
		),
	);
	if (!databaseExistedBeforeLock && companionPresence.some(Boolean)) {
		throw new Error("Custody SQLite companions exist without their database");
	}

	const lock = await native.acquireNativeDurabilityNodeLock(namespace);
	let database: SqliteDatabase | undefined;
	try {
		// Repeat the complete file observation while holding the namespace lock.
		// Only absence observed under exclusive ownership may authorize genesis;
		// a concurrent opener could otherwise create an empty database between
		// the early path check and lock acquisition.
		const databaseExisted = await assertCanonicalDatabaseFile(
			fs,
			databasePath,
			true,
		);
		const lockedCompanionPresence = await Promise.all(
			companionPaths.map((candidate) =>
				assertCanonicalDatabaseFile(fs, candidate, true),
			),
		);
		if (!databaseExisted && lockedCompanionPresence.some(Boolean)) {
			throw new Error("Custody SQLite companions exist without their database");
		}
		database = await sqlite.createDatabase(namespace, {
			pragmas: {
				synchronous: "FULL",
				lockingMode: "EXCLUSIVE",
				tempStore: "MEMORY",
			},
		});
		await database.open();
		await assertCanonicalDatabaseFile(fs, databasePath, false);
		await Promise.all(
			companionPaths.map((candidate) =>
				assertCanonicalDatabaseFile(fs, candidate, true),
			),
		);
		await database.exec("PRAGMA trusted_schema = OFF");
		await database.exec("PRAGMA foreign_keys = ON");
		await assertPragmas(database);
		const metadata = await initializeMetadata(
			database,
			crypto,
			binding,
			!databaseExisted,
		);
		await assertExistingSchema(database);
		if (
			(await readPragmaInteger(database, "application_id")) !==
				BigInt(CUSTODY_DATABASE_APPLICATION_ID) ||
			(await readPragmaInteger(database, "user_version")) !==
				BigInt(CUSTODY_DATABASE_USER_VERSION)
		) {
			throw new Error("Custody SQLite database version did not persist");
		}
		const statements = await prepareStatements(database);
		assertCheckpoint(await statements.checkpoint.all([]));
		await syncDirectoryStrict(fs, namespace);
		return new NodeCustodyRecordPersistence(
			database,
			statements,
			lock,
			fs,
			namespace,
			metadata,
			options.limits.maxFrameBytes,
		);
	} catch (error) {
		return cleanupCreationFailure(error, database, lock);
	}
};

const closeAfterOpenFailure = async (
	persistence: CustodyRecordPersistence,
	primary: unknown,
): Promise<never> => {
	let closeError: unknown;
	try {
		await persistence.close?.({ flush: false });
	} catch (error) {
		closeError = error;
	}
	if (closeError !== undefined) {
		throw new AggregateError(
			[primary, closeError],
			"Failed to open and close custody record persistence",
		);
	}
	throw primary;
};

/**
 * Open a strict Node custody-record store atomically with its lifetime
 * cross-process directory lock.
 *
 * The SQLite namespace is exactly bound to the node root, log, local canonical
 * public key, source/destination role, and fixed custody profile. Its stable
 * `namespaceEpoch` is storage metadata only: it is never the destination
 * `custodyEpoch` authenticated by a handoff receipt. This factory adds no
 * transfer, pin issuance, release, deletion, or prune authority.
 */
export const openNodeCustodyRecordStore = async (
	input: OpenNodeCustodyRecordStoreOptions,
	dependencies: CustodyRecordNodePersistenceDependencies = {},
): Promise<CustodyRecordStore> => {
	const options = captureOptions(input);
	const persistence = await createNodePersistence(options, dependencies);
	try {
		dependencies.onPersistenceCreated?.(persistence, persistence.facts);
		return await CustodyRecordStore.open({
			persistence,
			durability: "strict",
			limits: options.limits,
			binding: options.binding,
		});
	} catch (error) {
		return closeAfterOpenFailure(persistence, error);
	}
};
