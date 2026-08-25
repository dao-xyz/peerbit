import { deserialize, serialize } from "@dao-xyz/borsh";
import { cidifyString, stringifyCid } from "@peerbit/blocks-interface";
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
	type CustodyRecordCatalogCandidate,
	type CustodyRecordCatalogCandidateRead,
	type CustodyRecordCatalogCursor,
	type CustodyRecordCatalogFence,
	type CustodyRecordCatalogMigrationPage,
	type CustodyRecordCatalogPage,
	type CustodyRecordCatalogStatus,
	type CustodyRecordPersistence,
	type CustodyRecordRole,
	type CustodyRecordSlot,
	type CustodyRecordState,
	CustodyRecordStore,
	type CustodyStoreLimits,
	DEFAULT_CUSTODY_STORE_LIMITS,
	selectCustodyRecordCatalogFrame,
} from "./custody-store.js";
import { MAX_U64 } from "./integers.js";

const CUSTODY_RECORD_DIRECTORY = "custody-records-v1";
const CUSTODY_DATABASE_FILE = "db.sqlite";
const CUSTODY_DATABASE_APPLICATION_ID = 0x50424355;
const CUSTODY_DATABASE_USER_VERSION = 2;
const MAX_BINDING_BYTES = 2 * 1024;
const MAX_NODE_PATH_BYTES = 4 * 1024;
const MAX_IDENTITY_BYTES = 512;
const MAX_SCHEMA_BYTES = 4 * 1024;
const MAX_CATALOG_ENTRY_HASH_BYTES = 512;
const DEFAULT_CATALOG_SCAN_ROWS = 64;
const MAX_CATALOG_SCAN_ROWS = 256;
const DEFAULT_CATALOG_SCAN_BYTES = 64 * 1024;
const MAX_CATALOG_SCAN_BYTES = 256 * 1024;
const DEFAULT_CATALOG_MIGRATION_KEYS = 32;
const MAX_CATALOG_MIGRATION_KEYS = 64;
const DEFAULT_CATALOG_MIGRATION_BYTES = 1024 * 1024;
const MAX_CATALOG_MIGRATION_BYTES = 2 * 1024 * 1024;
const SQLITE_FULL_SYNCHRONOUS = 2n;
const MOVE_KEY_PATTERN = /^[0-9a-f]{64}$/;
const DECIMAL_PATTERN = /^(0|[1-9][0-9]*)$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

const META_SCHEMA =
	"CREATE TABLE custody_meta (id INTEGER PRIMARY KEY CHECK (id = 1), binding BLOB NOT NULL CHECK (typeof(binding) = 'blob' AND octet_length(binding) BETWEEN 1 AND 2048), namespace_epoch BLOB NOT NULL CHECK (typeof(namespace_epoch) = 'blob' AND octet_length(namespace_epoch) = 32), domain_id BLOB NOT NULL CHECK (typeof(domain_id) = 'blob' AND octet_length(domain_id) = 32), writer_epoch TEXT NOT NULL CHECK (typeof(writer_epoch) = 'text' AND length(writer_epoch) BETWEEN 1 AND 20 AND writer_epoch NOT GLOB '*[^0-9]*'), writer_owner BLOB NOT NULL CHECK (typeof(writer_owner) = 'blob' AND octet_length(writer_owner) = 32)) STRICT, WITHOUT ROWID";
const RECORD_SCHEMA =
	"CREATE TABLE custody_records (move_key TEXT NOT NULL CHECK (typeof(move_key) = 'text' AND length(move_key) = 64 AND move_key NOT GLOB '*[^0-9a-f]*'), slot TEXT NOT NULL CHECK (slot IN ('a', 'b')), frame BLOB NOT NULL CHECK (typeof(frame) = 'blob' AND octet_length(frame) BETWEEN 1 AND 16384), domain_id BLOB NOT NULL CHECK (typeof(domain_id) = 'blob' AND octet_length(domain_id) = 32), writer_epoch TEXT NOT NULL CHECK (typeof(writer_epoch) = 'text' AND length(writer_epoch) BETWEEN 1 AND 20 AND writer_epoch NOT GLOB '*[^0-9]*'), writer_owner BLOB NOT NULL CHECK (typeof(writer_owner) = 'blob' AND octet_length(writer_owner) = 32), PRIMARY KEY (move_key, slot)) STRICT, WITHOUT ROWID";
const CATALOG_META_SCHEMA =
	"CREATE TABLE custody_catalog_meta (id INTEGER PRIMARY KEY CHECK (id = 1), catalog_epoch BLOB NOT NULL CHECK (typeof(catalog_epoch) = 'blob' AND octet_length(catalog_epoch) = 32), last_mutation_sequence BLOB NOT NULL CHECK (typeof(last_mutation_sequence) = 'blob' AND octet_length(last_mutation_sequence) = 8), migration_state TEXT NOT NULL CHECK (migration_state IN ('building', 'ready')), migration_after TEXT CHECK (migration_after IS NULL OR (typeof(migration_after) = 'text' AND length(migration_after) = 64 AND migration_after NOT GLOB '*[^0-9a-f]*')), domain_id BLOB NOT NULL CHECK (typeof(domain_id) = 'blob' AND octet_length(domain_id) = 32)) STRICT, WITHOUT ROWID";
const CATALOG_HEAD_SCHEMA =
	"CREATE TABLE custody_heads (move_key BLOB PRIMARY KEY CHECK (typeof(move_key) = 'blob' AND octet_length(move_key) = 32), record_sequence BLOB NOT NULL CHECK (typeof(record_sequence) = 'blob' AND octet_length(record_sequence) = 8), mutation_sequence BLOB NOT NULL CHECK (typeof(mutation_sequence) = 'blob' AND octet_length(mutation_sequence) = 8), slot INTEGER NOT NULL CHECK (slot IN (0, 1)), state_tag INTEGER NOT NULL CHECK (state_tag BETWEEN 0 AND 5), entry_hash BLOB CHECK (entry_hash IS NULL OR (typeof(entry_hash) = 'blob' AND octet_length(entry_hash) BETWEEN 1 AND 512)), handoff_id BLOB CHECK (handoff_id IS NULL OR (typeof(handoff_id) = 'blob' AND octet_length(handoff_id) = 32)), frame_checksum BLOB NOT NULL CHECK (typeof(frame_checksum) = 'blob' AND octet_length(frame_checksum) = 32), domain_id BLOB NOT NULL CHECK (typeof(domain_id) = 'blob' AND octet_length(domain_id) = 32), writer_epoch TEXT NOT NULL CHECK (typeof(writer_epoch) = 'text' AND length(writer_epoch) BETWEEN 1 AND 20 AND writer_epoch NOT GLOB '*[^0-9]*'), writer_owner BLOB NOT NULL CHECK (typeof(writer_owner) = 'blob' AND octet_length(writer_owner) = 32), CHECK ((state_tag = 0 AND entry_hash IS NULL AND handoff_id IS NULL) OR (state_tag <> 0 AND entry_hash IS NOT NULL AND handoff_id IS NOT NULL))) STRICT, WITHOUT ROWID";
const CATALOG_RECOVERY_INDEX_SCHEMA =
	"CREATE INDEX custody_heads_recovery ON custody_heads(state_tag, mutation_sequence, move_key)";
const CATALOG_ENTRY_PIN_INDEX_SCHEMA =
	"CREATE INDEX custody_heads_entry_pin ON custody_heads(entry_hash, mutation_sequence, move_key) WHERE state_tag IN (4, 5)";

const catalogHeadProjection = (prefix: string) =>
	`octet_length(${prefix}move_key) AS move_key_bytes, substr(${prefix}move_key, 1, 33) AS move_key_prefix, octet_length(${prefix}record_sequence) AS record_sequence_bytes, substr(${prefix}record_sequence, 1, 9) AS record_sequence_prefix, octet_length(${prefix}mutation_sequence) AS mutation_sequence_bytes, substr(${prefix}mutation_sequence, 1, 9) AS mutation_sequence_prefix, ${prefix}slot AS slot, ${prefix}state_tag AS state_tag, octet_length(${prefix}entry_hash) AS entry_hash_bytes, substr(${prefix}entry_hash, 1, 513) AS entry_hash_prefix, octet_length(${prefix}handoff_id) AS handoff_id_bytes, substr(${prefix}handoff_id, 1, 33) AS handoff_id_prefix, octet_length(${prefix}frame_checksum) AS frame_checksum_bytes, substr(${prefix}frame_checksum, 1, 33) AS frame_checksum_prefix, octet_length(${prefix}domain_id) AS domain_id_bytes, substr(${prefix}domain_id, 1, 33) AS domain_id_prefix, octet_length(CAST(${prefix}writer_epoch AS BLOB)) AS writer_epoch_bytes, substr(CAST(${prefix}writer_epoch AS BLOB), 1, 21) AS writer_epoch_prefix, octet_length(${prefix}writer_owner) AS writer_owner_bytes, substr(${prefix}writer_owner, 1, 33) AS writer_owner_prefix`;

const CATALOG_META_PROJECTION =
	"octet_length(catalog_epoch) AS catalog_epoch_bytes, substr(catalog_epoch, 1, 33) AS catalog_epoch_prefix, octet_length(last_mutation_sequence) AS last_mutation_sequence_bytes, substr(last_mutation_sequence, 1, 9) AS last_mutation_sequence_prefix, octet_length(CAST(migration_state AS BLOB)) AS migration_state_bytes, substr(CAST(migration_state AS BLOB), 1, 9) AS migration_state_prefix, octet_length(CAST(migration_after AS BLOB)) AS migration_after_bytes, substr(CAST(migration_after AS BLOB), 1, 65) AS migration_after_prefix, octet_length(domain_id) AS domain_id_bytes, substr(domain_id, 1, 33) AS domain_id_prefix";

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
	catalogEpoch: Uint8Array;
}>;

type CapturedBaseMetadata = Omit<CapturedMetadata, "catalogEpoch">;

type PreparedStatements = Readonly<{
	read: SqliteStatement;
	write: SqliteStatement;
	readPair: SqliteStatement;
	readHead: SqliteStatement;
	writeHead: SqliteStatement;
	readCatalogMeta: SqliteStatement;
	writeCatalogMeta: SqliteStatement;
	barrierTarget: SqliteStatement;
	catalogStatus: SqliteStatement;
	catalogFence: SqliteStatement;
	recoveryScan: SqliteStatement;
	entryPinScan: SqliteStatement;
	candidateRead: SqliteStatement;
	migrationKeys: SqliteStatement;
	migrationFrames: SqliteStatement;
	migrationAdvance: SqliteStatement;
	checkpoint: SqliteStatement;
}>;

type CatalogMeta = Readonly<{
	catalogEpoch: Uint8Array;
	lastMutationSequence: bigint;
	migrationState: "building" | "ready";
	migrationAfter?: string;
	domainId: Uint8Array;
}>;

type CatalogHead = Readonly<{
	moveKey: string;
	recordSequence: bigint;
	mutationSequence: bigint;
	slot: CustodyRecordSlot;
	state: CustodyRecordState;
	entryHash?: string;
	handoffId?: string;
	frameChecksum: string;
	domainId: Uint8Array;
	writerEpoch: bigint;
	writerOwner: Uint8Array;
}>;

type SelectedCatalogRecord = Awaited<
	ReturnType<typeof selectCustodyRecordCatalogFrame>
> &
	Readonly<{
		domainId: Uint8Array;
		writerEpoch: bigint;
		writerOwner: Uint8Array;
		frameBytes: Uint8Array;
	}>;

type CatalogFenceRegistration = Readonly<{
	owner: NodeCustodyRecordPersistence;
	catalogEpoch: string;
	upperMutationSequence: bigint;
}>;

const catalogFenceRegistrations = new WeakMap<
	CustodyRecordCatalogFence,
	CatalogFenceRegistration
>();

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

const STATE_TAGS: Readonly<Record<CustodyRecordState, number>> = Object.freeze({
	absent: 0,
	"source-prepared": 1,
	"source-receipt-durable": 2,
	"destination-collecting": 3,
	"destination-pinned": 4,
	"destination-receipted": 5,
});

const STATES_BY_TAG = Object.freeze([
	"absent",
	"source-prepared",
	"source-receipt-durable",
	"destination-collecting",
	"destination-pinned",
	"destination-receipted",
] as const satisfies readonly CustodyRecordState[]);

const assertCatalogState = (value: unknown): CustodyRecordState => {
	if (
		typeof value !== "string" ||
		!Object.prototype.hasOwnProperty.call(STATE_TAGS, value)
	) {
		throw new Error("Invalid custody catalog state");
	}
	return value as CustodyRecordState;
};

const u64Bytes = (value: bigint, name: string): Uint8Array => {
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error(`Invalid ${name}`);
	}
	const bytes = new Uint8Array(8);
	let remaining = value;
	for (let index = 7; index >= 0; index--) {
		bytes[index] = Number(remaining & 0xffn);
		remaining >>= 8n;
	}
	return bytes;
};

const parseU64Bytes = (value: unknown, name: string): bigint => {
	let bytes: Uint8Array;
	try {
		bytes = captureBoundedUint8Array(value, 8, 8, name);
	} catch (error) {
		throw new Error(`Invalid ${name}`, { cause: error });
	}
	let result = 0n;
	for (let index = 0; index < 8; index++) {
		result = (result << 8n) | BigInt(bytes[index]!);
	}
	return result;
};

const hexBytes = (value: unknown, name: string): Uint8Array => {
	const hex = assertMoveKey(value);
	const bytes = new Uint8Array(32);
	for (let index = 0; index < 32; index++) {
		const parsed = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
		if (!Number.isSafeInteger(parsed)) throw new Error(`Invalid ${name}`);
		bytes[index] = parsed;
	}
	return bytes;
};

const boundedEntryHash = (value: unknown, name: string): string => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > MAX_CATALOG_ENTRY_HASH_BYTES ||
		!isWellFormedString(value)
	) {
		throw new Error(`Invalid ${name}`);
	}
	const bytes = encoder.encode(value);
	if (
		bytes.byteLength === 0 ||
		bytes.byteLength > MAX_CATALOG_ENTRY_HASH_BYTES
	) {
		throw new Error(`Invalid ${name}`);
	}
	let canonical: string;
	try {
		canonical = stringifyCid(cidifyString(value));
	} catch (error) {
		throw new Error(`Invalid ${name}`, { cause: error });
	}
	if (canonical !== value) throw new Error(`Invalid ${name}`);
	return canonical;
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
	let operationFailed = false;
	let operationError: unknown;
	try {
		handle = await fs.open(directory, "r");
		await handle.sync();
	} catch (error) {
		operationFailed = true;
		operationError = error;
	}
	let closeFailed = false;
	let closeError: unknown;
	try {
		await handle?.close();
	} catch (error) {
		closeFailed = true;
		closeError = error;
	}
	if (operationFailed && closeFailed) {
		throw new AggregateError(
			[operationError, closeError],
			`Failed to sync and close custody directory ${directory}`,
		);
	}
	if (operationFailed) throw operationError;
	if (closeFailed) throw closeError;
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

const schemaObjectCount = async (
	database: SqliteDatabase,
	maximum: number,
): Promise<number> => {
	const statement = await database.prepare(
		"SELECT 1 AS marker FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT ?",
	);
	const rows = asRows(
		await statement.all([maximum + 1]),
		"custody SQLite schema",
		maximum + 1,
	);
	if (rows.some((row) => BigInt(row.marker as number | bigint) !== 1n)) {
		throw new Error("Invalid custody SQLite schema marker");
	}
	return rows.length;
};

const assertSchemaObject = async (
	database: SqliteDatabase,
	name:
		| "custody_meta"
		| "custody_records"
		| "custody_catalog_meta"
		| "custody_heads"
		| "custody_heads_recovery"
		| "custody_heads_entry_pin",
	typeExpected: "table" | "index",
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
	if (type !== typeExpected || sql !== expectedSql) {
		throw new Error(`Unexpected custody SQLite schema object ${name}`);
	}
};

const assertV1Schema = async (database: SqliteDatabase) => {
	if ((await schemaObjectCount(database, 2)) !== 2) {
		throw new Error("Unexpected custody SQLite schema cardinality");
	}
	await assertSchemaObject(database, "custody_meta", "table", META_SCHEMA);
	await assertSchemaObject(database, "custody_records", "table", RECORD_SCHEMA);
};

const assertExistingSchema = async (database: SqliteDatabase) => {
	if ((await schemaObjectCount(database, 6)) !== 6) {
		throw new Error("Unexpected custody SQLite schema cardinality");
	}
	await assertV1SchemaObjects(database);
	await assertSchemaObject(
		database,
		"custody_catalog_meta",
		"table",
		CATALOG_META_SCHEMA,
	);
	await assertSchemaObject(
		database,
		"custody_heads",
		"table",
		CATALOG_HEAD_SCHEMA,
	);
	await assertSchemaObject(
		database,
		"custody_heads_recovery",
		"index",
		CATALOG_RECOVERY_INDEX_SCHEMA,
	);
	await assertSchemaObject(
		database,
		"custody_heads_entry_pin",
		"index",
		CATALOG_ENTRY_PIN_INDEX_SCHEMA,
	);
};

const assertV1SchemaObjects = async (database: SqliteDatabase) => {
	await assertSchemaObject(database, "custody_meta", "table", META_SCHEMA);
	await assertSchemaObject(database, "custody_records", "table", RECORD_SCHEMA);
};

const readMetadata = async (
	database: SqliteDatabase,
	expectedBinding: Uint8Array,
): Promise<CapturedBaseMetadata> => {
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

class CustodyTransactionRollbackError extends AggregateError {}

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
		let rollbackFailed = false;
		let rollbackError: unknown;
		try {
			await database.exec("ROLLBACK");
		} catch (candidate) {
			rollbackFailed = true;
			rollbackError = candidate;
		}
		if (rollbackFailed) {
			throw new CustodyTransactionRollbackError(
				[error, rollbackError],
				"Failed to initialize and roll back custody SQLite metadata",
			);
		}
		throw error;
	}
};

const readCatalogMetaFromDatabase = async (
	database: SqliteDatabase,
): Promise<CatalogMeta> => {
	const statement = await database.prepare(
		`SELECT ${CATALOG_META_PROJECTION} FROM custody_catalog_meta WHERE id = 1 LIMIT 2`,
	);
	const rows = asRows(await statement.all([]), "custody catalog metadata", 2);
	if (rows.length !== 1) throw new Error("Missing custody catalog metadata");
	return parseCatalogMeta(rows[0]!);
};

const parseCatalogMeta = (row: Record<string, unknown>): CatalogMeta => {
	const catalogEpoch = boundedBytes(
		row.catalog_epoch_prefix,
		row.catalog_epoch_bytes,
		32,
		"catalog epoch",
	);
	const domainId = boundedBytes(
		row.domain_id_prefix,
		row.domain_id_bytes,
		32,
		"catalog domain id",
	);
	if (
		!catalogEpoch.some((byte) => byte !== 0) ||
		!domainId.some((byte) => byte !== 0)
	) {
		throw new Error("Custody catalog metadata contains a zero generation");
	}
	const migrationState = boundedText(
		row.migration_state_prefix,
		row.migration_state_bytes,
		8,
		"catalog migration state",
	);
	if (migrationState !== "building" && migrationState !== "ready") {
		throw new Error("Invalid custody catalog migration state");
	}
	let migrationAfter: string | undefined;
	if (row.migration_after_bytes !== null) {
		migrationAfter = assertMoveKey(
			boundedText(
				row.migration_after_prefix,
				row.migration_after_bytes,
				64,
				"catalog migration cursor",
			),
		);
	}
	if (migrationState === "ready" && migrationAfter !== undefined) {
		throw new Error("Ready custody catalog retains a migration cursor");
	}
	return Object.freeze({
		catalogEpoch,
		lastMutationSequence: parseU64Bytes(
			boundedBytes(
				row.last_mutation_sequence_prefix,
				row.last_mutation_sequence_bytes,
				8,
				"catalog mutation sequence",
			),
			"catalog mutation sequence",
		),
		migrationState,
		...(migrationAfter === undefined ? {} : { migrationAfter }),
		domainId,
	});
};

const initializeMetadata = async (
	database: SqliteDatabase,
	crypto: CustodyRecordNodeCrypto,
	expectedBinding: Uint8Array,
	allowGenesis: boolean,
): Promise<CapturedMetadata> => {
	const objectCount = await schemaObjectCount(database, 6);
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
			catalogEpoch: randomNonZeroGeneration(crypto, "custody catalog epoch"),
		});
		return runTransaction(database, async () => {
			await database.exec(
				`${META_SCHEMA}; ${RECORD_SCHEMA}; ${CATALOG_META_SCHEMA}; ${CATALOG_HEAD_SCHEMA}; ${CATALOG_RECOVERY_INDEX_SCHEMA}; ${CATALOG_ENTRY_PIN_INDEX_SCHEMA}`,
			);
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
			const catalogInsert = await database.prepare(
				"INSERT INTO custody_catalog_meta (id, catalog_epoch, last_mutation_sequence, migration_state, migration_after, domain_id) VALUES (1, ?, ?, 'ready', NULL, ?)",
			);
			await catalogInsert.run([
				metadata.catalogEpoch,
				u64Bytes(0n, "catalog mutation sequence"),
				metadata.domainId,
			]);
			return metadata;
		});
	}
	if (
		(await readPragmaInteger(database, "application_id")) !==
		BigInt(CUSTODY_DATABASE_APPLICATION_ID)
	) {
		throw new Error("Custody SQLite database version mismatch");
	}
	const version = await readPragmaInteger(database, "user_version");
	if (version === 1n) {
		await assertV1Schema(database);
		const prior = await readMetadata(database, expectedBinding);
		const catalogEpoch = randomNonZeroGeneration(
			crypto,
			"custody catalog epoch",
		);
		const probe = await database.prepare(
			"SELECT 1 AS marker FROM custody_records LIMIT 1",
		);
		const probeRows = asRows(
			await probe.all([]),
			"custody v1 migration probe",
			1,
		);
		if (
			probeRows.length === 1 &&
			((typeof probeRows[0]!.marker !== "bigint" &&
				typeof probeRows[0]!.marker !== "number") ||
				BigInt(probeRows[0]!.marker as bigint | number) !== 1n)
		) {
			throw new Error("Invalid custody v1 migration probe");
		}
		const populated = probeRows.length === 1;
		await runTransaction(database, async () => {
			await database.exec(
				`${CATALOG_META_SCHEMA}; ${CATALOG_HEAD_SCHEMA}; ${CATALOG_RECOVERY_INDEX_SCHEMA}; ${CATALOG_ENTRY_PIN_INDEX_SCHEMA}`,
			);
			const insert = await database.prepare(
				"INSERT INTO custody_catalog_meta (id, catalog_epoch, last_mutation_sequence, migration_state, migration_after, domain_id) VALUES (1, ?, ?, ?, NULL, ?)",
			);
			await insert.run([
				catalogEpoch,
				u64Bytes(0n, "catalog mutation sequence"),
				populated ? "building" : "ready",
				prior.domainId,
			]);
			await database.exec(
				`PRAGMA user_version = ${CUSTODY_DATABASE_USER_VERSION}`,
			);
		});
	} else if (version !== BigInt(CUSTODY_DATABASE_USER_VERSION)) {
		throw new Error("Custody SQLite database version mismatch");
	}
	await assertExistingSchema(database);
	const previous = await readMetadata(database, expectedBinding);
	const catalog = await readCatalogMetaFromDatabase(database);
	if (!bytesEqual(catalog.domainId, previous.domainId)) {
		throw new Error("Custody catalog domain mismatch");
	}
	if (previous.writerEpoch === MAX_U64) {
		throw new Error("Custody SQLite writer epoch is exhausted");
	}
	const next: CapturedMetadata = Object.freeze({
		namespaceEpoch: previous.namespaceEpoch,
		domainId: previous.domainId,
		writerEpoch: previous.writerEpoch + 1n,
		writerOwner: randomNonZeroGeneration(crypto, "custody writer owner"),
		catalogEpoch: catalog.catalogEpoch,
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
			"custody-record-indexed-write",
		),
		readPair: await database.prepare(
			"SELECT slot, typeof(frame) AS frame_type, octet_length(frame) AS frame_bytes, substr(frame, 1, ?) AS frame_prefix, octet_length(domain_id) AS domain_id_bytes, substr(domain_id, 1, 33) AS domain_id_prefix, octet_length(CAST(writer_epoch AS BLOB)) AS writer_epoch_bytes, substr(CAST(writer_epoch AS BLOB), 1, 21) AS writer_epoch_prefix, octet_length(writer_owner) AS writer_owner_bytes, substr(writer_owner, 1, 33) AS writer_owner_prefix FROM custody_records WHERE move_key = ? ORDER BY slot LIMIT 3",
			"custody-record-pair-read",
		),
		readHead: await database.prepare(
			`SELECT ${catalogHeadProjection("")} FROM custody_heads WHERE move_key = ? LIMIT 2`,
			"custody-catalog-head-read",
		),
		writeHead: await database.prepare(
			"INSERT INTO custody_heads (move_key, record_sequence, mutation_sequence, slot, state_tag, entry_hash, handoff_id, frame_checksum, domain_id, writer_epoch, writer_owner) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (move_key) DO UPDATE SET record_sequence = excluded.record_sequence, mutation_sequence = excluded.mutation_sequence, slot = excluded.slot, state_tag = excluded.state_tag, entry_hash = excluded.entry_hash, handoff_id = excluded.handoff_id, frame_checksum = excluded.frame_checksum, domain_id = excluded.domain_id, writer_epoch = excluded.writer_epoch, writer_owner = excluded.writer_owner",
			"custody-catalog-head-write",
		),
		readCatalogMeta: await database.prepare(
			`SELECT ${CATALOG_META_PROJECTION} FROM custody_catalog_meta WHERE id = 1 LIMIT 2`,
			"custody-catalog-meta-read",
		),
		writeCatalogMeta: await database.prepare(
			"UPDATE custody_catalog_meta SET last_mutation_sequence = ? WHERE id = 1 AND last_mutation_sequence = ?",
			"custody-catalog-meta-write",
		),
		barrierTarget: await database.prepare(
			`SELECT r.slot AS record_slot, typeof(r.frame) AS frame_type, octet_length(r.frame) AS frame_bytes, substr(r.frame, 1, ?) AS frame_prefix, octet_length(r.domain_id) AS record_domain_id_bytes, substr(r.domain_id, 1, 33) AS record_domain_id_prefix, octet_length(CAST(r.writer_epoch AS BLOB)) AS record_writer_epoch_bytes, substr(CAST(r.writer_epoch AS BLOB), 1, 21) AS record_writer_epoch_prefix, octet_length(r.writer_owner) AS record_writer_owner_bytes, substr(r.writer_owner, 1, 33) AS record_writer_owner_prefix, ${catalogHeadProjection("h.")} FROM custody_records r JOIN custody_heads h ON h.move_key = ? AND h.slot = CASE r.slot WHEN 'a' THEN 0 ELSE 1 END WHERE r.move_key = ? AND r.slot = ? LIMIT 2`,
			"custody-record-barrier-target",
		),
		catalogStatus: await database.prepare(
			`SELECT ${CATALOG_META_PROJECTION} FROM custody_catalog_meta WHERE id = 1 LIMIT 2`,
			"custody-catalog-status",
		),
		catalogFence: await database.prepare(
			`SELECT ${CATALOG_META_PROJECTION} FROM custody_catalog_meta WHERE id = 1 LIMIT 2`,
			"custody-catalog-fence",
		),
		recoveryScan: await database.prepare(
			`SELECT ${catalogHeadProjection("")} FROM custody_heads WHERE state_tag = ? AND mutation_sequence <= ? AND (mutation_sequence > ? OR (mutation_sequence = ? AND move_key > ?)) ORDER BY mutation_sequence, move_key LIMIT ?`,
			"custody-catalog-recovery-scan",
		),
		entryPinScan: await database.prepare(
			`SELECT ${catalogHeadProjection("")} FROM custody_heads WHERE entry_hash = ? AND state_tag IN (4, 5) AND mutation_sequence <= ? AND (mutation_sequence > ? OR (mutation_sequence = ? AND move_key > ?)) ORDER BY mutation_sequence, move_key LIMIT ?`,
			"custody-catalog-entry-pin-scan",
		),
		candidateRead: await database.prepare(
			`SELECT ${catalogHeadProjection("h.")}, typeof(r.frame) AS frame_type, octet_length(r.frame) AS frame_bytes, substr(r.frame, 1, ?) AS frame_prefix, octet_length(r.domain_id) AS record_domain_id_bytes, substr(r.domain_id, 1, 33) AS record_domain_id_prefix, octet_length(CAST(r.writer_epoch AS BLOB)) AS record_writer_epoch_bytes, substr(CAST(r.writer_epoch AS BLOB), 1, 21) AS record_writer_epoch_prefix, octet_length(r.writer_owner) AS record_writer_owner_bytes, substr(r.writer_owner, 1, 33) AS record_writer_owner_prefix FROM custody_heads h JOIN custody_records r ON r.move_key = lower(hex(h.move_key)) AND r.slot = CASE h.slot WHEN 0 THEN 'a' ELSE 'b' END WHERE h.move_key = ? LIMIT 2`,
			"custody-catalog-candidate-read",
		),
		migrationKeys: await database.prepare(
			"SELECT octet_length(CAST(move_key AS BLOB)) AS move_key_bytes, substr(CAST(move_key AS BLOB), 1, 65) AS move_key_prefix FROM custody_records WHERE move_key > ? GROUP BY move_key ORDER BY move_key LIMIT ?",
			"custody-catalog-migration-keys",
		),
		migrationFrames: await database.prepare(
			"SELECT slot, typeof(frame) AS frame_type, octet_length(frame) AS frame_bytes, substr(frame, 1, ?) AS frame_prefix, octet_length(domain_id) AS domain_id_bytes, substr(domain_id, 1, 33) AS domain_id_prefix, octet_length(CAST(writer_epoch AS BLOB)) AS writer_epoch_bytes, substr(CAST(writer_epoch AS BLOB), 1, 21) AS writer_epoch_prefix, octet_length(writer_owner) AS writer_owner_bytes, substr(writer_owner, 1, 33) AS writer_owner_prefix FROM custody_records WHERE move_key = ? ORDER BY slot LIMIT 3",
			"custody-catalog-migration-frames",
		),
		migrationAdvance: await database.prepare(
			"UPDATE custody_catalog_meta SET migration_state = ?, migration_after = ? WHERE id = 1 AND migration_state = 'building' AND ((migration_after IS NULL AND ? IS NULL) OR migration_after = ?)",
			"custody-catalog-migration-advance",
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
	private poisoned = false;
	private poisonCause?: unknown;
	private closePromise?: Promise<void>;

	constructor(
		private readonly database: SqliteDatabase,
		private readonly statements: PreparedStatements,
		private readonly lock: NativeDurabilityLock,
		private readonly fs: CustodyRecordNodeFileSystem,
		private readonly namespace: string,
		private readonly metadata: CapturedMetadata,
		private readonly limits: CustodyStoreLimits,
		private readonly binding: CustodyRecordBinding,
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

	isPoisoned(): boolean {
		return this.poisoned;
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
			this.limits.maxFrameBytes,
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
				this.limits.maxFrameBytes,
				"custody persistence write",
			);
		} catch (error) {
			return Promise.reject(
				new RangeError("Invalid custody persistence write", { cause: error }),
			);
		}
		return this.enqueue(async () => {
			await runTransaction(this.database, async () => {
				await this.statements.write.run([
					capturedMoveKey,
					capturedSlot,
					captured,
					this.metadata.domainId,
					this.metadata.writerEpoch.toString(),
					this.metadata.writerOwner,
				]);
				const selected = await this.selectPair(
					capturedMoveKey,
					this.statements.readPair,
				);
				await this.indexSelected(selected);
			});
		});
	}

	durableBarrier(moveKey: string, slot: CustodyRecordSlot): Promise<void> {
		this.assertAccepting();
		const capturedMoveKey = assertMoveKey(moveKey);
		const capturedSlot = assertSlot(slot);
		return this.enqueue(async () => {
			const selectedBefore = await this.selectPair(
				capturedMoveKey,
				this.statements.readPair,
			);
			if (selectedBefore.slot !== capturedSlot) {
				throw new Error("Custody barrier target is not the selected frame");
			}
			const beforeMeta = await this.readCatalogMeta(
				this.statements.readCatalogMeta,
			);
			const beforeHeads = asRows(
				await this.statements.readHead.all([
					this.moveKeyBytes(capturedMoveKey),
				]),
				"custody catalog barrier head",
				2,
			);
			if (beforeHeads.length > 1) {
				throw new Error("Duplicate custody catalog barrier head");
			}
			let repair = false;
			if (beforeHeads.length === 0) {
				if (beforeMeta.migrationState !== "building") {
					throw new Error("Missing custody catalog barrier head");
				}
				repair = true;
			} else {
				const beforeHead = this.parseHead(beforeHeads[0]!);
				if (!this.selectedMatchesHead(selectedBefore, beforeHead)) {
					if (
						!(await this.headReferencesInvalidFrame(
							capturedMoveKey,
							beforeHead,
						))
					) {
						throw new Error(
							"Valid custody catalog head does not match selected frame",
						);
					}
					repair = true;
				}
			}
			if (repair) {
				await runTransaction(this.database, async () => {
					await this.indexSelected(selectedBefore);
				});
			}
			const readTargets = async () =>
				asRows(
					await this.statements.barrierTarget.all([
						this.limits.maxFrameBytes + 1,
						this.moveKeyBytes(capturedMoveKey),
						capturedMoveKey,
						capturedSlot,
					]),
					"custody SQLite barrier target",
					2,
				);
			const targets = await readTargets();
			if (targets.length !== 1) {
				throw new Error("Missing or duplicate custody SQLite barrier target");
			}
			const target = targets[0]!;
			const head = this.parseHead(target);
			const targetFrame = this.captureFrameRow(target);
			const targetFence = this.assertRecordWriterFenceDirect(target);
			const selected = await this.selectPair(
				capturedMoveKey,
				this.statements.readPair,
			);
			if (selected.slot !== capturedSlot) {
				throw new Error("Custody barrier target is not the selected frame");
			}
			this.assertSelectedMatchesHead(selected, head);
			if (
				!bytesEqual(targetFrame, selected.frameBytes) ||
				!bytesEqual(targetFence.domainId, selected.domainId) ||
				targetFence.writerEpoch !== selected.writerEpoch ||
				!bytesEqual(targetFence.writerOwner, selected.writerOwner)
			) {
				throw new Error("Custody barrier target changed during confirmation");
			}
			const meta = await this.readCatalogMeta(this.statements.readCatalogMeta);
			if (
				!bytesEqual(meta.catalogEpoch, this.metadata.catalogEpoch) ||
				!bytesEqual(meta.domainId, this.metadata.domainId) ||
				meta.lastMutationSequence < head.mutationSequence
			) {
				throw new Error("Custody catalog barrier metadata mismatch");
			}
			assertCheckpoint(await this.statements.checkpoint.all([]));
			await syncDirectoryStrict(this.fs, this.namespace);
		});
	}

	readCatalogStatus(): Promise<CustodyRecordCatalogStatus> {
		this.assertAccepting();
		return this.enqueue(async () =>
			this.publicCatalogStatus(
				await this.readCatalogMeta(this.statements.catalogStatus),
			),
		);
	}

	captureCatalogFence(): Promise<CustodyRecordCatalogFence> {
		this.assertAccepting();
		return this.enqueue(async () => {
			const meta = await this.readReadyCatalogMeta(
				this.statements.catalogFence,
			);
			const catalogEpoch = toHexString(meta.catalogEpoch);
			const fence = Object.freeze({
				catalogEpoch,
				upperMutationSequence: meta.lastMutationSequence,
			}) as CustodyRecordCatalogFence;
			catalogFenceRegistrations.set(
				fence,
				Object.freeze({
					owner: this,
					catalogEpoch,
					upperMutationSequence: meta.lastMutationSequence,
				}),
			);
			return fence;
		});
	}

	scanRecoveryPage(input: {
		fence: CustodyRecordCatalogFence;
		state: CustodyRecordState;
		after?: CustodyRecordCatalogCursor;
		maxRows?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogPage> {
		this.assertAccepting();
		if (!input || typeof input !== "object") {
			throw new Error("Invalid custody catalog recovery scan");
		}
		const stateValue = input.state;
		const maxRowsValue = input.maxRows;
		const maxBytesValue = input.maxBytes;
		const afterValue = input.after;
		const fence = input.fence;
		const state = assertCatalogState(stateValue);
		const limits = this.captureScanLimits(maxRowsValue, maxBytesValue);
		const after = this.captureCursor(afterValue);
		return this.enqueue(async () => {
			await this.assertCurrentFence(fence);
			const rows = asRows(
				await this.statements.recoveryScan.all([
					STATE_TAGS[state],
					u64Bytes(fence.upperMutationSequence, "catalog fence"),
					u64Bytes(after.mutationSequence, "catalog cursor"),
					u64Bytes(after.mutationSequence, "catalog cursor"),
					this.moveKeyBytes(after.moveKey),
					limits.maxRows + 1,
				]),
				"custody catalog recovery page",
				limits.maxRows + 1,
			);
			return this.pageFromRows(fence, rows, limits, after, { state });
		});
	}

	scanEntryPinsPage(input: {
		fence: CustodyRecordCatalogFence;
		entryHash: string;
		after?: CustodyRecordCatalogCursor;
		maxRows?: number;
		maxBytes?: number;
	}): Promise<CustodyRecordCatalogPage> {
		this.assertAccepting();
		if (!input || typeof input !== "object") {
			throw new Error("Invalid custody catalog entry pin scan");
		}
		const entryHashValue = input.entryHash;
		const maxRowsValue = input.maxRows;
		const maxBytesValue = input.maxBytes;
		const afterValue = input.after;
		const fence = input.fence;
		const entryHash = boundedEntryHash(
			entryHashValue,
			"custody catalog entry hash",
		);
		const limits = this.captureScanLimits(maxRowsValue, maxBytesValue);
		const after = this.captureCursor(afterValue);
		return this.enqueue(async () => {
			await this.assertCurrentFence(fence);
			const rows = asRows(
				await this.statements.entryPinScan.all([
					encoder.encode(entryHash),
					u64Bytes(fence.upperMutationSequence, "catalog fence"),
					u64Bytes(after.mutationSequence, "catalog cursor"),
					u64Bytes(after.mutationSequence, "catalog cursor"),
					this.moveKeyBytes(after.moveKey),
					limits.maxRows + 1,
				]),
				"custody catalog entry pin page",
				limits.maxRows + 1,
			);
			return this.pageFromRows(fence, rows, limits, after, { entryHash });
		});
	}

	readCatalogCandidate(
		candidateValue: CustodyRecordCatalogCandidate,
		maxBytes = this.limits.maxFrameBytes,
	): Promise<CustodyRecordCatalogCandidateRead> {
		this.assertAccepting();
		const candidate = this.captureCandidate(candidateValue);
		const limit = assertPositiveLimit(
			maxBytes,
			"custody catalog candidate read bound",
			this.limits.maxFrameBytes,
		);
		return this.enqueue(async () => {
			const meta = await this.readReadyCatalogMeta(
				this.statements.readCatalogMeta,
			);
			if (toHexString(meta.catalogEpoch) !== candidate.catalogEpoch) {
				return Object.freeze({ status: "changed" as const });
			}
			if (candidate.mutationSequence > meta.lastMutationSequence) {
				throw new Error("Custody catalog candidate exceeds its waterline");
			}
			const rows = asRows(
				await this.statements.candidateRead.all([
					limit + 1,
					this.moveKeyBytes(candidate.moveKey),
				]),
				"custody catalog candidate read",
				2,
			);
			if (rows.length === 0) {
				throw new Error("Missing custody catalog candidate head or frame");
			}
			if (rows.length !== 1) {
				throw new Error("Duplicate custody catalog candidate");
			}
			const row = rows[0]!;
			const currentHead = this.parseHead(row);
			if (currentHead.mutationSequence > meta.lastMutationSequence) {
				throw new Error("Custody catalog candidate exceeds its waterline");
			}
			const current = this.publicCandidate(currentHead);
			if (!this.sameCandidate(current, candidate)) {
				return Object.freeze({ status: "changed" as const });
			}
			const frame = this.captureFrameRow(row, limit);
			const directFence = this.assertRecordWriterFenceDirect(row);
			const selected = await this.selectPair(
				candidate.moveKey,
				this.statements.readPair,
			);
			if (selected.slot !== candidate.slot) {
				throw new Error("Custody catalog candidate is not the selected frame");
			}
			this.assertSelectedMatchesHead(selected, this.parseHead(row));
			if (!bytesEqual(frame, selected.frameBytes)) {
				throw new Error(
					"Custody catalog candidate frame changed during point read",
				);
			}
			if (
				!bytesEqual(directFence.domainId, selected.domainId) ||
				directFence.writerEpoch !== selected.writerEpoch ||
				!bytesEqual(directFence.writerOwner, selected.writerOwner)
			) {
				throw new Error(
					"Custody catalog candidate fence changed during point read",
				);
			}
			return Object.freeze({
				status: "current" as const,
				candidate: current,
				frame: new Uint8Array(selected.frameBytes),
			});
		});
	}

	migrateCatalogPage(
		input: { maxMoveKeys?: number; maxBytes?: number } = {},
	): Promise<CustodyRecordCatalogMigrationPage> {
		this.assertAccepting();
		let maxMoveKeys: number;
		let maxBytes: number;
		try {
			if (!input || typeof input !== "object") {
				throw new Error("Invalid custody catalog migration options");
			}
			const maxMoveKeysValue = input.maxMoveKeys;
			const maxBytesValue = input.maxBytes;
			maxMoveKeys = assertPositiveLimit(
				maxMoveKeysValue ?? DEFAULT_CATALOG_MIGRATION_KEYS,
				"custody catalog migration key bound",
				MAX_CATALOG_MIGRATION_KEYS,
			);
			maxBytes = assertPositiveLimit(
				maxBytesValue ?? DEFAULT_CATALOG_MIGRATION_BYTES,
				"custody catalog migration byte bound",
				MAX_CATALOG_MIGRATION_BYTES,
			);
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueue(async () => {
			let committed = false;
			try {
				const page = await runTransaction(this.database, async () => {
					const meta = await this.readCatalogMeta(
						this.statements.readCatalogMeta,
					);
					if (meta.migrationState === "ready") {
						return Object.freeze({
							migrationState: "ready" as const,
							processed: 0,
						});
					}
					const previousAfter = meta.migrationAfter;
					const rows = asRows(
						await this.statements.migrationKeys.all([
							previousAfter ?? "",
							maxMoveKeys + 1,
						]),
						"custody catalog migration keys",
						maxMoveKeys + 1,
					);
					const processCount = Math.min(rows.length, maxMoveKeys);
					let consumedBytes = 0;
					let processed = 0;
					let migrationAfter = previousAfter;
					for (let index = 0; index < processCount; index++) {
						const moveKey = assertMoveKey(
							boundedText(
								rows[index]!.move_key_prefix,
								rows[index]!.move_key_bytes,
								64,
								"custody catalog migration move key",
							),
						);
						const frameRows = asRows(
							await this.statements.migrationFrames.all([
								this.limits.maxFrameBytes + 1,
								moveKey,
							]),
							"custody catalog migration frames",
							3,
						);
						if (frameRows.length === 0 || frameRows.length > 2) {
							throw new Error("Invalid custody catalog migration frame pair");
						}
						let pairBytes = 0;
						for (
							let frameIndex = 0;
							frameIndex < frameRows.length;
							frameIndex++
						) {
							const length = frameRows[frameIndex]!.frame_bytes;
							if (typeof length !== "bigint" || length <= 0n) {
								throw new Error("Invalid custody catalog migration frame size");
							}
							pairBytes += Number(length);
						}
						if (consumedBytes + pairBytes > maxBytes) {
							if (processed === 0) {
								throw new RangeError(
									"Custody catalog migration item exceeds page byte bound",
								);
							}
							break;
						}
						const selected = await this.selectRows(moveKey, frameRows);
						await this.indexSelected(selected);
						consumedBytes += pairBytes;
						processed++;
						migrationAfter = moveKey;
					}
					const hasMore = processed < rows.length;
					const state = hasMore ? "building" : "ready";
					await this.statements.migrationAdvance.run([
						state,
						state === "ready" ? null : (migrationAfter ?? null),
						previousAfter ?? null,
						previousAfter ?? null,
					]);
					const advanced = await this.readCatalogMeta(
						this.statements.readCatalogMeta,
					);
					if (
						advanced.migrationState !== state ||
						advanced.migrationAfter !==
							(state === "ready" ? undefined : migrationAfter)
					) {
						throw new Error("Custody catalog migration cursor did not persist");
					}
					return Object.freeze({
						migrationState: state,
						...(state === "building" && migrationAfter
							? { migrationAfter }
							: {}),
						processed,
					});
				});
				committed = true;
				assertCheckpoint(await this.statements.checkpoint.all([]));
				await syncDirectoryStrict(this.fs, this.namespace);
				return page;
			} catch (error) {
				if (committed || error instanceof CustodyTransactionRollbackError) {
					this.poisoned = true;
					this.poisonCause = error;
				}
				throw new Error("Failed to durably migrate custody catalog page", {
					cause: error,
				});
			}
		});
	}

	close(_options?: { flush?: false }): Promise<void> {
		if (this.closePromise) return this.closePromise;
		this.closing = true;
		this.closePromise = (async () => {
			await this.tail;
			let databaseFailed = false;
			let databaseError: unknown;
			try {
				await this.lock.runWhileHeld(async () => {
					await this.database.close();
				});
			} catch (error) {
				databaseFailed = true;
				databaseError = error;
			}
			let lockFailed = false;
			let lockError: unknown;
			try {
				await this.lock.close();
			} catch (error) {
				lockFailed = true;
				lockError = error;
			} finally {
				this.closed = true;
			}
			if (databaseFailed && lockFailed) {
				throw new AggregateError(
					[databaseError, lockError],
					"Failed to close custody SQLite database and directory lock",
				);
			}
			if (databaseFailed) throw databaseError;
			if (lockFailed) throw lockError;
		})();
		return this.closePromise;
	}

	private moveKeyBytes(value: string) {
		return hexBytes(value, "custody catalog move key");
	}

	private async readCatalogMeta(
		statement: SqliteStatement,
	): Promise<CatalogMeta> {
		const rows = asRows(await statement.all([]), "custody catalog metadata", 2);
		if (rows.length !== 1) throw new Error("Missing custody catalog metadata");
		const meta = parseCatalogMeta(rows[0]!);
		if (
			!bytesEqual(meta.catalogEpoch, this.metadata.catalogEpoch) ||
			!bytesEqual(meta.domainId, this.metadata.domainId)
		) {
			throw new Error("Custody catalog metadata mismatch");
		}
		return meta;
	}

	private async readReadyCatalogMeta(statement: SqliteStatement) {
		const meta = await this.readCatalogMeta(statement);
		if (meta.migrationState !== "ready") {
			throw new Error("Custody catalog migration is still building");
		}
		return meta;
	}

	private publicCatalogStatus(meta: CatalogMeta): CustodyRecordCatalogStatus {
		return Object.freeze({
			catalogEpoch: toHexString(meta.catalogEpoch),
			lastMutationSequence: meta.lastMutationSequence,
			migrationState: meta.migrationState,
			...(meta.migrationAfter === undefined
				? {}
				: { migrationAfter: meta.migrationAfter }),
		});
	}

	private captureFrameRow(
		row: Record<string, unknown>,
		limit = this.limits.maxFrameBytes,
	) {
		if (row.frame_type !== "blob") {
			throw new Error("Custody SQLite record is not a BLOB");
		}
		if (
			typeof row.frame_bytes !== "bigint" ||
			row.frame_bytes <= 0n ||
			row.frame_bytes > BigInt(limit)
		) {
			throw new RangeError("Custody SQLite record exceeds read byte bound");
		}
		return boundedBytes(
			row.frame_prefix,
			row.frame_bytes,
			limit,
			"record frame",
		);
	}

	private async selectPair(moveKey: string, statement: SqliteStatement) {
		const rows = asRows(
			await statement.all([this.limits.maxFrameBytes + 1, moveKey]),
			"custody SQLite record pair",
			3,
		);
		return this.selectRows(moveKey, rows);
	}

	private async selectRows(
		moveKey: string,
		rows: Record<string, unknown>[],
	): Promise<SelectedCatalogRecord> {
		if (rows.length === 0 || rows.length > 2) {
			throw new Error("Invalid custody SQLite record pair");
		}
		const frames: { slot: CustodyRecordSlot; bytes: Uint8Array }[] = [];
		const fences = new Map<
			CustodyRecordSlot,
			Readonly<{
				domainId: Uint8Array;
				writerEpoch: bigint;
				writerOwner: Uint8Array;
			}>
		>();
		for (let index = 0; index < rows.length; index++) {
			const row = rows[index]!;
			const slot = assertSlot(row.slot);
			fences.set(slot, this.assertWriterFence(row));
			frames.push({ slot, bytes: this.captureFrameRow(row) });
		}
		const selected = await selectCustodyRecordCatalogFrame({
			moveKey,
			frames,
			durability: "strict",
			limits: this.limits,
			binding: this.binding,
		});
		const fence = fences.get(selected.slot);
		if (!fence) throw new Error("Missing selected custody record writer fence");
		const selectedBytes = frames.find(
			(value) => value.slot === selected.slot,
		)?.bytes;
		if (!selectedBytes)
			throw new Error("Missing selected custody record bytes");
		return Object.freeze({
			...selected,
			...fence,
			frameBytes: new Uint8Array(selectedBytes),
		});
	}

	private async headReferencesInvalidFrame(
		moveKey: string,
		head: CatalogHead,
	): Promise<boolean> {
		const rows = asRows(
			await this.statements.read.all([
				this.limits.maxFrameBytes + 1,
				moveKey,
				head.slot,
			]),
			"custody catalog referenced frame",
			2,
		);
		if (rows.length === 0) return true;
		if (rows.length !== 1) {
			throw new Error("Duplicate custody catalog referenced frame");
		}
		const row = rows[0]!;
		const frame = this.captureFrameRow(row);
		this.assertWriterFence(row);
		try {
			await selectCustodyRecordCatalogFrame({
				moveKey,
				frames: [{ slot: head.slot, bytes: frame }],
				durability: "strict",
				limits: this.limits,
				binding: this.binding,
			});
		} catch (error) {
			if (error instanceof AggregateError) return true;
			throw error;
		}
		return false;
	}

	private async indexSelected(selected: SelectedCatalogRecord) {
		const currentRows = asRows(
			await this.statements.readHead.all([this.moveKeyBytes(selected.moveKey)]),
			"custody catalog head read",
			2,
		);
		if (currentRows.length > 1)
			throw new Error("Duplicate custody catalog head");
		const meta = await this.readCatalogMeta(this.statements.readCatalogMeta);
		if (
			currentRows.length === 1 &&
			this.selectedMatchesHead(selected, this.parseHead(currentRows[0]!))
		) {
			if (
				this.parseHead(currentRows[0]!).mutationSequence >
				meta.lastMutationSequence
			) {
				throw new Error("Custody catalog head exceeds its mutation waterline");
			}
			return;
		}
		if (meta.lastMutationSequence === MAX_U64) {
			throw new Error("Custody catalog mutation sequence is exhausted");
		}
		const mutationSequence = meta.lastMutationSequence + 1n;
		await this.statements.writeHead.run([
			this.moveKeyBytes(selected.moveKey),
			u64Bytes(selected.recordSequence, "custody record sequence"),
			u64Bytes(mutationSequence, "custody catalog mutation sequence"),
			selected.slot === "a" ? 0 : 1,
			STATE_TAGS[selected.state],
			selected.entryHash === undefined
				? null
				: encoder.encode(selected.entryHash),
			selected.handoffId === undefined
				? null
				: hexBytes(selected.handoffId, "custody handoff id"),
			hexBytes(selected.frameChecksum, "custody frame checksum"),
			selected.domainId,
			selected.writerEpoch.toString(),
			selected.writerOwner,
		]);
		await this.statements.writeCatalogMeta.run([
			u64Bytes(mutationSequence, "custody catalog mutation sequence"),
			u64Bytes(meta.lastMutationSequence, "custody catalog mutation sequence"),
		]);
		const confirmed = await this.readCatalogMeta(
			this.statements.readCatalogMeta,
		);
		if (confirmed.lastMutationSequence !== mutationSequence) {
			throw new Error("Custody catalog mutation allocation did not persist");
		}
		const confirmedHeads = asRows(
			await this.statements.readHead.all([this.moveKeyBytes(selected.moveKey)]),
			"custody catalog head confirmation",
			2,
		);
		if (confirmedHeads.length !== 1) {
			throw new Error("Custody catalog head update did not persist");
		}
		const confirmedHead = this.parseHead(confirmedHeads[0]!);
		if (
			confirmedHead.mutationSequence !== mutationSequence ||
			!this.selectedMatchesHead(selected, confirmedHead)
		) {
			throw new Error("Custody catalog head update did not persist");
		}
	}

	private parseHead(row: Record<string, unknown>): CatalogHead {
		const moveKeyBytes = boundedBytes(
			row.move_key_prefix,
			row.move_key_bytes,
			32,
			"custody catalog move key",
		);
		if (
			(typeof row.state_tag !== "bigint" &&
				typeof row.state_tag !== "number") ||
			(typeof row.state_tag === "number" &&
				!Number.isSafeInteger(row.state_tag))
		) {
			throw new Error("Invalid custody catalog state tag");
		}
		const stateTagBig = BigInt(row.state_tag);
		if (stateTagBig < 0n || stateTagBig > 5n) {
			throw new Error("Invalid custody catalog state tag");
		}
		const stateTag = Number(stateTagBig);
		const state = STATES_BY_TAG[stateTag];
		if (state === undefined) {
			throw new Error("Invalid custody catalog state tag");
		}
		if (
			(typeof row.slot !== "bigint" && typeof row.slot !== "number") ||
			(typeof row.slot === "number" && !Number.isSafeInteger(row.slot))
		) {
			throw new Error("Invalid custody catalog slot");
		}
		const slotBig = BigInt(row.slot);
		const slotNumber = Number(slotBig);
		if (slotNumber !== 0 && slotNumber !== 1) {
			throw new Error("Invalid custody catalog slot");
		}
		let entryHash: string | undefined;
		let handoffId: string | undefined;
		if (state !== "absent") {
			const entryBytes = boundedBytes(
				row.entry_hash_prefix,
				row.entry_hash_bytes,
				MAX_CATALOG_ENTRY_HASH_BYTES,
				"custody catalog entry hash",
			);
			entryHash = boundedEntryHash(
				decoder.decode(entryBytes),
				"custody catalog entry hash",
			);
			handoffId = toHexString(
				boundedBytes(
					row.handoff_id_prefix,
					row.handoff_id_bytes,
					32,
					"custody catalog handoff id",
				),
			);
		} else if (row.entry_hash_bytes !== null || row.handoff_id_bytes !== null) {
			throw new Error("Invalid absent custody catalog head");
		}
		const domainId = boundedBytes(
			row.domain_id_prefix,
			row.domain_id_bytes,
			32,
			"custody catalog domain id",
		);
		const writerOwner = boundedBytes(
			row.writer_owner_prefix,
			row.writer_owner_bytes,
			32,
			"custody catalog writer owner",
		);
		const writerEpoch = parseWriterEpoch(
			boundedText(
				row.writer_epoch_prefix,
				row.writer_epoch_bytes,
				20,
				"custody catalog writer epoch",
			),
		);
		if (
			!bytesEqual(domainId, this.metadata.domainId) ||
			writerEpoch > this.metadata.writerEpoch ||
			!writerOwner.some((byte) => byte !== 0) ||
			(writerEpoch === this.metadata.writerEpoch &&
				!bytesEqual(writerOwner, this.metadata.writerOwner))
		) {
			throw new Error("Custody catalog head has an invalid writer fence");
		}
		const recordSequence = parseU64Bytes(
			boundedBytes(
				row.record_sequence_prefix,
				row.record_sequence_bytes,
				8,
				"custody catalog record sequence",
			),
			"custody catalog record sequence",
		);
		const mutationSequence = parseU64Bytes(
			boundedBytes(
				row.mutation_sequence_prefix,
				row.mutation_sequence_bytes,
				8,
				"custody catalog mutation sequence",
			),
			"custody catalog mutation sequence",
		);
		const expectedSequence =
			state === "absent"
				? 1n
				: state === "source-prepared" || state === "destination-collecting"
					? 2n
					: state === "source-receipt-durable" || state === "destination-pinned"
						? 3n
						: 4n;
		const sourceState =
			state === "source-prepared" || state === "source-receipt-durable";
		if (
			recordSequence !== expectedSequence ||
			((recordSequence & 1n) === 0n ? "a" : "b") !==
				(slotNumber === 0 ? "a" : "b") ||
			mutationSequence === 0n ||
			(state !== "absent" &&
				((this.binding.role === "source" && !sourceState) ||
					(this.binding.role === "destination" && sourceState)))
		) {
			throw new Error("Unreachable custody catalog head");
		}
		return Object.freeze({
			moveKey: toHexString(moveKeyBytes),
			recordSequence,
			mutationSequence,
			slot: slotNumber === 0 ? "a" : "b",
			state,
			...(entryHash === undefined ? {} : { entryHash }),
			...(handoffId === undefined ? {} : { handoffId }),
			frameChecksum: toHexString(
				boundedBytes(
					row.frame_checksum_prefix,
					row.frame_checksum_bytes,
					32,
					"custody catalog frame checksum",
				),
			),
			domainId,
			writerEpoch,
			writerOwner,
		});
	}

	private selectedMatchesHead(
		selected: SelectedCatalogRecord,
		head: CatalogHead,
	) {
		return (
			selected.moveKey === head.moveKey &&
			selected.recordSequence === head.recordSequence &&
			selected.slot === head.slot &&
			selected.state === head.state &&
			selected.entryHash === head.entryHash &&
			selected.handoffId === head.handoffId &&
			selected.frameChecksum === head.frameChecksum &&
			bytesEqual(selected.domainId, head.domainId) &&
			selected.writerEpoch === head.writerEpoch &&
			bytesEqual(selected.writerOwner, head.writerOwner)
		);
	}

	private assertSelectedMatchesHead(
		selected: SelectedCatalogRecord,
		head: CatalogHead,
	) {
		if (!this.selectedMatchesHead(selected, head)) {
			throw new Error("Custody catalog head does not match selected frame");
		}
	}

	private publicCandidate(head: CatalogHead): CustodyRecordCatalogCandidate {
		return Object.freeze({
			catalogEpoch: toHexString(this.metadata.catalogEpoch),
			mutationSequence: head.mutationSequence,
			moveKey: head.moveKey,
			recordSequence: head.recordSequence,
			slot: head.slot,
			state: head.state,
			...(head.entryHash === undefined ? {} : { entryHash: head.entryHash }),
			...(head.handoffId === undefined ? {} : { handoffId: head.handoffId }),
			frameChecksum: head.frameChecksum,
			domainId: toHexString(head.domainId),
			writerEpoch: head.writerEpoch,
			writerOwner: toHexString(head.writerOwner),
		});
	}

	private pageFromRows(
		fence: CustodyRecordCatalogFence,
		rows: Record<string, unknown>[],
		limits: { maxRows: number; maxBytes: number },
		after: CustodyRecordCatalogCursor,
		expected: Readonly<{ state?: CustodyRecordState; entryHash?: string }>,
	): CustodyRecordCatalogPage {
		const candidates: CustodyRecordCatalogCandidate[] = [];
		let bytes = 0;
		let index = 0;
		let previous = after;
		for (; index < rows.length && candidates.length < limits.maxRows; index++) {
			const candidate = this.publicCandidate(this.parseHead(rows[index]!));
			if (
				candidate.mutationSequence > fence.upperMutationSequence ||
				candidate.mutationSequence < previous.mutationSequence ||
				(candidate.mutationSequence === previous.mutationSequence &&
					candidate.moveKey <= previous.moveKey) ||
				(expected.state !== undefined && candidate.state !== expected.state) ||
				(expected.entryHash !== undefined &&
					(candidate.entryHash !== expected.entryHash ||
						(candidate.state !== "destination-pinned" &&
							candidate.state !== "destination-receipted")))
			) {
				throw new Error("Invalid custody catalog scan tuple");
			}
			previous = Object.freeze({
				mutationSequence: candidate.mutationSequence,
				moveKey: candidate.moveKey,
			});
			const projected =
				32 +
				32 +
				8 +
				8 +
				1 +
				1 +
				(candidate.entryHash === undefined
					? 0
					: encoder.encode(candidate.entryHash).byteLength) +
				(candidate.handoffId === undefined ? 0 : 32) +
				32 +
				32 +
				20 +
				32;
			if (bytes + projected > limits.maxBytes) {
				if (candidates.length === 0) {
					throw new RangeError(
						"Custody catalog candidate exceeds page byte bound",
					);
				}
				break;
			}
			bytes += projected;
			candidates.push(candidate);
		}
		const hasMore = index < rows.length;
		const last = candidates.at(-1);
		return Object.freeze({
			fence,
			candidates: Object.freeze(candidates),
			...(hasMore && last
				? {
						next: Object.freeze({
							mutationSequence: last.mutationSequence,
							moveKey: last.moveKey,
						}),
					}
				: {}),
		});
	}

	private captureScanLimits(maxRows: unknown, maxBytes: unknown) {
		return Object.freeze({
			maxRows: assertPositiveLimit(
				maxRows ?? DEFAULT_CATALOG_SCAN_ROWS,
				"custody catalog scan row bound",
				MAX_CATALOG_SCAN_ROWS,
			),
			maxBytes: assertPositiveLimit(
				maxBytes ?? DEFAULT_CATALOG_SCAN_BYTES,
				"custody catalog scan byte bound",
				MAX_CATALOG_SCAN_BYTES,
			),
		});
	}

	private captureCursor(
		value: CustodyRecordCatalogCursor | undefined,
	): CustodyRecordCatalogCursor {
		if (value === undefined) {
			return Object.freeze({ mutationSequence: 0n, moveKey: "0".repeat(64) });
		}
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog cursor");
		}
		const mutationSequence = value.mutationSequence;
		const moveKey = value.moveKey;
		if (
			typeof mutationSequence !== "bigint" ||
			mutationSequence < 0n ||
			mutationSequence > MAX_U64
		) {
			throw new Error("Invalid custody catalog cursor");
		}
		return Object.freeze({
			mutationSequence,
			moveKey: assertMoveKey(moveKey),
		});
	}

	private async assertCurrentFence(fence: CustodyRecordCatalogFence) {
		if (!fence || typeof fence !== "object") {
			throw new Error("Invalid custody catalog fence");
		}
		const registration = catalogFenceRegistrations.get(fence);
		if (
			!registration ||
			registration.owner !== this ||
			registration.catalogEpoch !== fence.catalogEpoch ||
			registration.upperMutationSequence !== fence.upperMutationSequence
		) {
			throw new Error("Invalid custody catalog fence");
		}
		const meta = await this.readReadyCatalogMeta(
			this.statements.readCatalogMeta,
		);
		if (
			toHexString(meta.catalogEpoch) !== registration.catalogEpoch ||
			meta.lastMutationSequence < registration.upperMutationSequence
		) {
			throw new Error("Stale custody catalog fence");
		}
	}

	private captureCandidate(
		value: CustodyRecordCatalogCandidate,
	): CustodyRecordCatalogCandidate {
		if (!value || typeof value !== "object") {
			throw new Error("Invalid custody catalog candidate");
		}
		const catalogEpochValue = value.catalogEpoch;
		const mutationSequence = value.mutationSequence;
		const moveKeyValue = value.moveKey;
		const recordSequence = value.recordSequence;
		const slotValue = value.slot;
		const stateValue = value.state;
		const entryHashValue = value.entryHash;
		const handoffIdValue = value.handoffId;
		const frameChecksumValue = value.frameChecksum;
		const domainIdValue = value.domainId;
		const writerEpoch = value.writerEpoch;
		const writerOwnerValue = value.writerOwner;
		const state = assertCatalogState(stateValue);
		const entryHash =
			entryHashValue === undefined
				? undefined
				: boundedEntryHash(entryHashValue, "custody catalog entry hash");
		const handoffId =
			handoffIdValue === undefined ? undefined : assertMoveKey(handoffIdValue);
		if (
			(state === "absent") !==
			(entryHash === undefined && handoffId === undefined)
		) {
			throw new Error("Invalid custody catalog candidate artifacts");
		}
		if (
			typeof mutationSequence !== "bigint" ||
			mutationSequence <= 0n ||
			mutationSequence > MAX_U64 ||
			typeof recordSequence !== "bigint" ||
			recordSequence <= 0n ||
			recordSequence > MAX_U64 ||
			typeof writerEpoch !== "bigint" ||
			writerEpoch <= 0n ||
			writerEpoch > MAX_U64
		) {
			throw new Error("Invalid custody catalog candidate sequence");
		}
		return Object.freeze({
			catalogEpoch: assertMoveKey(catalogEpochValue),
			mutationSequence,
			moveKey: assertMoveKey(moveKeyValue),
			recordSequence,
			slot: assertSlot(slotValue),
			state,
			...(entryHash === undefined ? {} : { entryHash }),
			...(handoffId === undefined ? {} : { handoffId }),
			frameChecksum: assertMoveKey(frameChecksumValue),
			domainId: assertMoveKey(domainIdValue),
			writerEpoch,
			writerOwner: assertMoveKey(writerOwnerValue),
		});
	}

	private sameCandidate(
		left: CustodyRecordCatalogCandidate,
		right: CustodyRecordCatalogCandidate,
	) {
		return (
			left.catalogEpoch === right.catalogEpoch &&
			left.mutationSequence === right.mutationSequence &&
			left.moveKey === right.moveKey &&
			left.recordSequence === right.recordSequence &&
			left.slot === right.slot &&
			left.state === right.state &&
			left.entryHash === right.entryHash &&
			left.handoffId === right.handoffId &&
			left.frameChecksum === right.frameChecksum &&
			left.domainId === right.domainId &&
			left.writerEpoch === right.writerEpoch &&
			left.writerOwner === right.writerOwner
		);
	}

	private assertRecordWriterFenceDirect(row: Record<string, unknown>) {
		const domainId = boundedBytes(
			row.record_domain_id_prefix,
			row.record_domain_id_bytes,
			32,
			"custody record domain id",
		);
		const owner = boundedBytes(
			row.record_writer_owner_prefix,
			row.record_writer_owner_bytes,
			32,
			"custody record writer owner",
		);
		const epoch = parseWriterEpoch(
			boundedText(
				row.record_writer_epoch_prefix,
				row.record_writer_epoch_bytes,
				20,
				"custody record writer epoch",
			),
		);
		if (
			!bytesEqual(domainId, this.metadata.domainId) ||
			epoch > this.metadata.writerEpoch ||
			!owner.some((byte) => byte !== 0) ||
			(epoch === this.metadata.writerEpoch &&
				!bytesEqual(owner, this.metadata.writerOwner))
		) {
			throw new Error("Custody SQLite record has an invalid writer fence");
		}
		return Object.freeze({
			domainId,
			writerEpoch: epoch,
			writerOwner: owner,
		});
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		this.assertAccepting();
		const result = this.tail.then(() => {
			this.assertNotPoisoned();
			return this.lock.runWhileHeld(operation);
		});
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
		this.assertNotPoisoned();
	}

	private assertNotPoisoned() {
		if (this.poisoned) {
			throw new Error("Custody SQLite persistence is poisoned", {
				cause: this.poisonCause,
			});
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
		return Object.freeze({ domainId, writerEpoch, writerOwner });
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
			options.limits,
			options.binding,
		);
	} catch (error) {
		return cleanupCreationFailure(error, database, lock);
	}
};

const closeAfterOpenFailure = async (
	persistence: CustodyRecordPersistence,
	primary: unknown,
): Promise<never> => {
	let closeFailed = false;
	let closeError: unknown;
	try {
		await persistence.close?.({ flush: false });
	} catch (error) {
		closeFailed = true;
		closeError = error;
	}
	if (closeFailed) {
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
