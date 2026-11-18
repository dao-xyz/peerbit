import {
	type AbstractType,
	type Constructor,
	type FieldType,
	getSchema,
} from "@dao-xyz/borsh";
import type {
	Index,
	IndexEngineInitProperties,
	IndexedResult,
	Shape,
} from "@peerbit/indexer-interface";
import * as types from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";
import {
	ChangeJournal,
	type ChangeJournalPull,
	type ChangeJournalReader,
} from "./change-journal.js";
import {
	PlannableQuery,
	type PlanningSession,
	QueryPlanner,
} from "./query-planner.js";
import {
	type BindableValue,
	MissingFieldError,
	PRIMARY_KEY_ALIAS,
	ROOT_ORDINAL_ALIAS,
	type Table,
	bindKeyQuery,
	buildJoin,
	coerceLocalQueries,
	coerceLocalSorts,
	convertCountRequestToQuery,
	convertDeleteRequestToQuery,
	convertFromSQLType,
	convertKeysToRowsQuery,
	convertSearchRequestToKeyQuery,
	convertSearchRequestToPageQuery,
	convertSearchRequestToQuery,
	convertSumRequestToQuery,
	convertToSQLType,
	encodeKeyList,
	escapeColumnName,
	generateSelectQuery,
	getInlineTableFieldName,
	getSQLTable,
	getTablePrefixedField,
	insert,
	primaryKeyEncoding,
	resolveInstanceFromValue,
	resolveTable,
	selectAllFieldsFromTable,
	selectChildren,
	u64ToI64,
} from "./schema.js";
import type { Database, Statement } from "./types.js";
import { isFKError, isUniqueConstraintError } from "./utils.js";

const escapePathToSQLName = (path: string[]) => {
	return path.map((x) => x.replace(/[^a-zA-Z0-9]/g, "_"));
};

const putStatementKey = (table: Table) => table.name + "_put";
const insertKnownIdStatementKey = (table: Table) =>
	table.name + "_insert_known_id";
const putStatementBatchNoReturnKey = (table: Table, rows: number) =>
	table.name + "_put_batch_noreturn_" + rows;
const replaceStatementKey = (table: Table) => table.name + "_replicate";
const resolveChildrenStatement = (table: Table) =>
	table.name + "_resolve_children";

type FKMode = "strict" | "race-tolerant";

type DatabaseCoordinator = {
	tail: Promise<void>;
	nextSavepointId: number;
	crashSafeDurability?: types.CrashSafeIndexDurability;
};

// SQLite transactions and schema changes belong to the connection, not to a
// scoped index. Keep every connection-level operation in one queue so reads
// cannot observe a chunk that later rolls back and a savepoint cannot capture
// another scope's lazy CREATE INDEX or table DDL.
const databaseCoordinators = new WeakMap<Database, DatabaseCoordinator>();

const getDatabaseCoordinator = (database: Database): DatabaseCoordinator => {
	let coordinator = databaseCoordinators.get(database);
	if (!coordinator) {
		coordinator = {
			tail: Promise.resolve(),
			nextSavepointId: 0,
		};
		databaseCoordinators.set(database, coordinator);
	}
	return coordinator;
};

const PUT_BATCH_CHUNK_SIZE = 64;

/**
 * Keys an iterator reads from its key list per statement, and the most changed
 * keys it re-checks in one call: past that, it lists the keys again.
 */
const SNAPSHOT_WINDOW = 4096;

/**
 * Rows an iterator in primary-key order reads by pages after its first one
 * (S2) before it lists its keys. A short read (a probe, a few pages) holds
 * only the keys it returned.
 */
const PAGED_ROWS_BUDGET = 2048;

/** A primary key as one root table stores it. */
type StoredKey = { t: number; pk: unknown };

const HEX_BYTES = Array.from({ length: 256 }, (_, i) =>
	i.toString(16).padStart(2, "0"),
);

const toHex = (bytes: Uint8Array) => {
	let out = "";
	for (let i = 0; i < bytes.length; i++) {
		out += HEX_BYTES[bytes[i]];
	}
	return out;
};

/** `value` with each lone surrogate replaced by U+FFFD, as SQLite stores it. */
const wellFormed = (value: string): string => {
	if (typeof value.isWellFormed === "function") {
		return value.isWellFormed() ? value : value.toWellFormed();
	}
	let out = "";
	for (let i = 0; i < value.length; i++) {
		const unit = value.charCodeAt(i);
		if (unit >= 0xd800 && unit <= 0xdbff) {
			const low = value.charCodeAt(i + 1);
			if (low >= 0xdc00 && low <= 0xdfff) {
				out += value[i] + value[i + 1];
				i++;
			} else {
				out += "\uFFFD";
			}
		} else {
			out += unit >= 0xdc00 && unit <= 0xdfff ? "\uFFFD" : value[i];
		}
	}
	return out;
};

const normalizeKey = (value: unknown) =>
	typeof value === "string" ? wellFormed(value) : value;

/**
 * One string per key value: a bigint and a number of equal value are one key,
 * and bytes are compared by content. Takes a normalized value
 * (`KeyIdentity.value`, `KeyIdentity.mark`).
 */
const keyString = (value: unknown): string => {
	switch (typeof value) {
		case "string":
			return "s" + value;
		case "bigint":
			return "i" + value;
		case "number":
			return Number.isInteger(value)
				? "i" + (Number.isSafeInteger(value) ? value : BigInt(value))
				: "r" + value;
		default:
			return value instanceof Uint8Array
				? "x" + toHex(value)
				: "o" + String(value);
	}
};

const keyRank = (value: unknown) =>
	typeof value === "string"
		? 0
		: typeof value === "bigint" ||
			  (typeof value === "number" && Number.isInteger(value))
			? 1
			: typeof value === "number"
				? 2
				: value instanceof Uint8Array
					? 3
					: 4;

/**
 * A total order on normalized key values that agrees with `keyString`
 * equality. It only serves lookups: SQLite decides every order a caller sees.
 */
const compareKeys = (a: any, b: any): number => {
	const rankA = keyRank(a);
	const rankB = keyRank(b);
	if (rankA !== rankB) {
		return rankA - rankB;
	}
	if (rankA === 3) {
		const length = Math.min(a.length, b.length);
		for (let i = 0; i < length; i++) {
			if (a[i] !== b[i]) {
				return a[i] - b[i];
			}
		}
		return a.length - b.length;
	}
	if (rankA === 4) {
		a = String(a);
		b = String(b);
	}
	return a < b ? -1 : a > b ? 1 : 0;
};

const MIN_I64 = -(1n << 63n);
const MAX_I64 = (1n << 63n) - 1n;
const U64_END = 1n << 64n;

/**
 * How an iterator names a row: by the value of its primary key, whichever
 * root table holds it. A bigint and a number of equal value are one key,
 * bytes are compared by content, and a lone surrogate counts as U+FFFD, as
 * SQLite stores it. `markYielded` names rows the same way.
 *
 * When every root table stores its keys the same way, the stored SQL value
 * stands for the key, and one key list serves every root table. Otherwise the
 * decoded value does, and each root table gets its own key list.
 */
class KeyIdentity {
	readonly uniform: boolean;
	private readonly encodings: (string | undefined)[];
	private readonly types: (FieldType | undefined)[];

	constructor(rootTables: Table[]) {
		this.encodings = rootTables.map(primaryKeyEncoding);
		this.types = rootTables.map((table) => table.primaryField?.from?.type);
		this.uniform = this.encodings.every(
			(encoding) => encoding === this.encodings[0],
		);
	}

	/** The normalized value of a key that root table `t` stores as `stored`. */
	value(t: number, stored: unknown): unknown {
		return normalizeKey(
			this.uniform
				? stored
				: convertFromSQLType(
						stored as string | number | bigint | Uint8Array,
						this.types[t],
					),
		);
	}

	/** The normalized value of a key passed to `markYielded`. */
	mark(key: unknown): unknown {
		if (this.uniform && this.encodings[0]?.endsWith("(u64)") && isU64(key)) {
			return u64ToI64(key);
		}
		return normalizeKey(key);
	}

	/**
	 * How root table `t` stores the key of normalized value `value` (with
	 * several encodings), or undefined when that table cannot hold it.
	 */
	encode(t: number, value: unknown): unknown {
		const encoding = this.encodings[t];
		if (encoding?.startsWith("numeric")) {
			if (typeof value === "number" && !Number.isInteger(value)) {
				return encoding.endsWith("(u64)") ? undefined : value;
			}
			if (typeof value !== "bigint" && typeof value !== "number") {
				return undefined;
			}
			if (encoding.endsWith("(u64)")) {
				return isU64(value) ? u64ToI64(value) : undefined;
			}
			const integer = BigInt(value);
			return integer >= MIN_I64 && integer <= MAX_I64 ? value : undefined;
		}
		if (encoding === "text") {
			return typeof value === "string" ? value : undefined;
		}
		if (encoding === "blob") {
			return value instanceof Uint8Array ? value : undefined;
		}
		return undefined;
	}
}

const isU64 = (value: unknown): value is number | bigint =>
	(typeof value === "bigint" && value >= 0n && value < U64_END) ||
	(typeof value === "number" && Number.isSafeInteger(value) && value >= 0);

/**
 * A key list in an iterator's order: the stored keys, one per row identity.
 */
type Snapshot = {
	pk: unknown[] | BigInt64Array;
	/** The root ordinal of each key, with several root tables. */
	t?: Uint8Array | Uint16Array;
	/**
	 * Positions ordered by `compareKeys`, or null when `pk` already is; built
	 * by the first lookup.
	 */
	byKey?: Int32Array | null;
};

const snapshotTable = (snapshot: Snapshot, i: number) =>
	snapshot.t ? snapshot.t[i] : 0;

/** The position of the key with normalized value `value`, or -1. */
const snapshotPositionOf = (
	snapshot: Snapshot,
	identity: KeyIdentity,
	value: unknown,
): number => {
	const keys = snapshot.pk;
	const valueAt = (i: number) =>
		identity.value(snapshotTable(snapshot, i), keys[i]);
	if (snapshot.byKey === undefined) {
		let sorted = true;
		for (let i = 1; i < keys.length; i++) {
			if (compareKeys(valueAt(i - 1), valueAt(i)) > 0) {
				sorted = false;
				break;
			}
		}
		if (sorted) {
			snapshot.byKey = null;
		} else {
			const values =
				identity.uniform && keys instanceof BigInt64Array
					? keys
					: Array.from({ length: keys.length }, (_, i) => valueAt(i));
			const order = new Int32Array(keys.length);
			for (let i = 0; i < order.length; i++) {
				order[i] = i;
			}
			order.sort((x, y) => compareKeys(values[x], values[y]));
			snapshot.byKey = order;
		}
	}
	const order = snapshot.byKey;
	let low = 0;
	let high = keys.length - 1;
	while (low <= high) {
		const middle = (low + high) >>> 1;
		const at = order ? order[middle] : middle;
		const comparison = compareKeys(valueAt(at), value);
		if (comparison === 0) {
			return at;
		}
		if (comparison < 0) {
			low = middle + 1;
		} else {
			high = middle - 1;
		}
	}
	return -1;
};

/** Stored keys in a compact array: a BigInt64Array when every key is a bigint. */
const compactKeys = (keys: unknown[]): unknown[] | BigInt64Array => {
	if (keys.length === 0 || !keys.every((key) => typeof key === "bigint")) {
		return keys;
	}
	return BigInt64Array.from(keys as bigint[]);
};

const compactOrdinals = (ordinals: number[]): Uint8Array | Uint16Array =>
	ordinals.some((ordinal) => ordinal > 255)
		? Uint16Array.from(ordinals)
		: Uint8Array.from(ordinals);

/** Thrown inside an admission when the iterator was closed meanwhile. */
const ITERATOR_CLOSED = Symbol("iterator closed");

async function safeReset(stmt?: Statement) {
	if (!stmt?.reset) return;
	try {
		await stmt.reset();
	} catch (e) {
		if (isFKError(e)) return; // swallow FK-reset noise
		throw e;
	}
}

async function runIgnoreFK(stmt: Statement, values: any[]) {
	try {
		await stmt.run(values);
		await safeReset(stmt); // success path: reset safely
		return;
	} catch (e) {
		if (isFKError(e)) {
			await safeReset(stmt); // swallow FK + swallow reset error
			return; // pretend no-op
		}
		// real error
		await safeReset(stmt); // best effort
		throw e;
	}
}

async function getIgnoreFK(stmt: Statement, values: any[]) {
	try {
		const out = await stmt.get(values);
		await safeReset(stmt); // success path
		return out;
	} catch (e) {
		if (isFKError(e)) {
			await safeReset(stmt); // swallow FK + reset error
			return undefined;
		}
		await safeReset(stmt);
		throw e;
	}
}

const createBatchInsertSQL = (table: Table, rows: number) => {
	const columns = table.fields
		.map((field) => escapeColumnName(field.name))
		.join(", ");
	const rowPlaceholder = `(${table.fields.map(() => "?").join(", ")})`;
	return `insert into ${table.name} (${columns}) VALUES ${Array.from({
		length: rows,
	})
		.map(() => rowPlaceholder)
		.join(", ")};`;
};

const createInsertReturningSQL = (table: Table) =>
	`insert into ${table.name}  (${table.fields.map((field) => escapeColumnName(field.name)).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")}) RETURNING ${table.primary};`;

const createInsertKnownIdSQL = (table: Table) =>
	`insert into ${table.name} (${table.fields.map((field) => escapeColumnName(field.name)).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")});`;

const createReplaceSQL = (table: Table) =>
	`insert or replace into ${table.name} (${table.fields.map((field) => escapeColumnName(field.name)).join(", ")}) VALUES (${table.fields.map((_x) => "?").join(", ")});`;

const canUseWithoutRowId = (table: Table) => {
	if (table.inline || table.primary === false || !table.primaryField) {
		return false;
	}
	return !/^INTEGER\b/i.test(table.primaryField.type);
};

export class SQLiteIndex<T extends Record<string, any>>
	implements Index<T, any>
{
	// SQLite transactions are connection-wide, while this API awaits between
	// statements (root insert -> child inserts -> reconstruction reads). Admit
	// complete database operations in one FIFO across every scope sharing the
	// connection. Besides preventing put/del interleaving, this keeps reads and
	// lazy schema DDL out of savepoints that may still roll back.
	// TODO(perf): This is intentionally coarse-grained for correctness. Any
	// relaxation needs explicit transaction ownership plus read/schema isolation
	// across Node, browser, and webworker runners.
	private _operationBarrier: Promise<void> = Promise.resolve();
	private readonly databaseCoordinator: DatabaseCoordinator;

	private async acquireDatabaseBarrier(): Promise<() => void> {
		const prev = this.databaseCoordinator.tail;
		let releaseBarrier!: () => void;
		const next = new Promise<void>((resolve) => (releaseBarrier = resolve));
		// Keep the chain alive even if `prev` rejected.
		this.databaseCoordinator.tail = prev.then(
			() => next,
			() => next,
		);

		const pending = prev.catch(() => undefined).then(() => next);
		this._operationBarrier = pending.then(
			() => undefined,
			() => undefined,
		);
		// Wait for the previous database operation without propagating its error.
		await prev.catch(() => undefined);
		let released = false;
		return () => {
			if (released) {
				return;
			}
			released = true;
			releaseBarrier();
		};
	}

	private async withDatabaseBarrier<R>(fn: () => Promise<R>): Promise<R> {
		const release = await this.acquireDatabaseBarrier();
		try {
			return await fn();
		} finally {
			release();
		}
	}

	primaryKeyArr!: string[];
	primaryKeyString!: string;
	planner: QueryPlanner;
	private scopeString?: string;
	private _rootTables: Table[] = [];
	private _tables: Map<string, Table> = new Map();
	/**
	 * Iterators that have rows left, by id. `cursorPruner` releases the ones
	 * idle past `iteratorTimeout`, and stop() and drop() release them all.
	 */
	private _cursor: Map<
		string,
		{
			expire: number;
			/** Drops the iterator's key list and journal reader. */
			release: () => void;
		}
	> = new Map(); // TODO choose limit better
	private cursorPruner: ReturnType<typeof setInterval> | undefined;

	iteratorTimeout: number;
	closed: boolean = true;
	private state: "closed" | "open" | "closing" = "closed";
	private fkMode: FKMode;
	private mutationVersion = 0;
	/**
	 * Root keys of committed writes, for iterators that need to know which rows
	 * changed between two of their calls. It records only while a reader is
	 * registered: an iterator registers one while it holds a key list with rows
	 * left, and unregisters when it is done, closed, expired or stopped.
	 */
	private readonly changeJournal = new ChangeJournal();
	private keyScanGeneration?: symbol;
	private keyScanMutations = 0;

	private async withKeyMutation<R>(operation: () => Promise<R>): Promise<R> {
		this.keyScanGeneration = undefined;
		this.keyScanMutations++;
		try {
			return await operation();
		} finally {
			this.keyScanMutations--;
		}
	}

	id: string;
	constructor(
		readonly properties: {
			scope: string[];
			db: Database;
			schema: AbstractType<any>;
			persisted?: boolean;
			start?: () => Promise<void> | void;
			stop?: () => Promise<void> | void;
		},
		options?: { iteratorTimeout?: number; fkMode?: FKMode },
	) {
		this.fkMode = options?.fkMode || "race-tolerant";
		this.databaseCoordinator = getDatabaseCoordinator(properties.db);
		this.closed = true;
		this.id = uuid();
		this.scopeString =
			properties.scope.length > 0
				? "_" + escapePathToSQLName(properties.scope).join("_")
				: undefined;
		this.iteratorTimeout = options?.iteratorTimeout || 60e3;
		this.planner = new QueryPlanner({
			exec: (query) =>
				this.withDatabaseIfOpen(undefined, () =>
					Promise.resolve(this.properties.db.exec(query)).then(() => undefined),
				),
		});
	}

	persisted(): boolean {
		return this.properties.persisted ?? true;
	}

	get crashSafeDurability(): types.CrashSafeIndexDurability | undefined {
		const durability = this.properties.db.crashSafeDurability;
		if (!durability) {
			return undefined;
		}
		// The barrier's checkpoint belongs to the connection and fails while a
		// write transaction is open. Admit it after the operations already queued
		// on the connection, so it never runs inside another scope's savepoint.
		// Scopes on one connection share one barrier object.
		return (this.databaseCoordinator.crashSafeDurability ??= {
			crashSafe: true,
			barrier: () =>
				this.withDatabaseBarrier(async () => {
					await durability.barrier();
				}),
		});
	}

	private static readonly _emptyTables = new Map<string, Table>();
	private static readonly _emptyRootTables: Table[] = [];
	private static readonly _emptyCursor = new Map();

	private static closedIterator<
		T extends Record<string, any>,
		S extends Shape | undefined,
	>(): types.IndexIterator<T, S> {
		return {
			all: async () => [],
			close: async () => undefined,
			done: () => true,
			next: async () => [],
			pending: async () => 0,
		};
	}

	private async ifOpen<R>(fallback: R, fn: () => Promise<R>): Promise<R> {
		if (this.isClosing()) {
			return fallback;
		}
		this.assertOpen();
		try {
			return await fn();
		} catch (error) {
			if (this.isClosing()) {
				return fallback;
			}
			throw error;
		}
	}

	private async withDatabaseIfOpen<R>(
		fallback: R,
		fn: () => Promise<R>,
	): Promise<R> {
		if (this.isClosing()) {
			return fallback;
		}
		this.assertOpen();
		return this.withDatabaseBarrier(() => this.ifOpen(fallback, fn));
	}

	private assertOpen() {
		if (this.state !== "open") {
			throw new types.NotStartedError();
		}
	}

	private isClosing() {
		return this.state === "closing";
	}

	private setClosing() {
		this.keyScanGeneration = undefined;
		this.state = "closing";
		this.closed = true;
	}

	private setClosed() {
		this.state = "closed";
		this.closed = true;
	}

	private setOpen() {
		this.keyScanGeneration = undefined;
		this.state = "open";
		this.closed = false;
	}

	get tables() {
		if (this.closed) {
			return SQLiteIndex._emptyTables;
		}
		return this._tables;
	}

	get rootTables() {
		if (this.closed) {
			return SQLiteIndex._emptyRootTables;
		}
		return this._rootTables;
	}

	get cursor() {
		if (this.closed) {
			return SQLiteIndex._emptyCursor;
		}
		return this._cursor;
	}

	init(properties: IndexEngineInitProperties<T, any>) {
		this.keyScanGeneration = undefined;
		if (properties.indexBy) {
			this.primaryKeyArr = Array.isArray(properties.indexBy)
				? properties.indexBy
				: [properties.indexBy];
		} else {
			const indexBy = types.getIdProperty(properties.schema);

			if (!indexBy) {
				throw new Error(
					"No indexBy property defined nor schema has a property decorated with `id()`",
				);
			}

			this.primaryKeyArr = indexBy;
		}

		if (!this.properties.schema) {
			throw new Error("Missing schema");
		}

		this.primaryKeyString = getInlineTableFieldName(this.primaryKeyArr);

		return this;
	}

	async start(): Promise<void> {
		if (this.state === "open") {
			return;
		}
		if (this.state === "closing") {
			throw new types.NotStartedError();
		}

		if (this.primaryKeyArr == null || this.primaryKeyArr.length === 0) {
			throw new Error("Not initialized");
		}

		await this.properties.start?.();

		this._tables = new Map();
		this._cursor = new Map();

		const tables = getSQLTable(
			this.properties.schema!,
			this.scopeString ? [this.scopeString] : [],
			getInlineTableFieldName(this.primaryKeyArr), // TODO fix this, should be array
			false,
			undefined,
			false,
			/* getTableName(this.scopeString, this.properties.schema!) */
		);

		this._rootTables = tables.filter((x) => x.parent == null);

		const allTables = tables;
		const startupStatements: { id: string; sql: string }[] = [];
		const startupTableStatements = new Map<string, string>();

		for (const table of allTables) {
			this._tables.set(table.name, table);

			for (const child of table.children) {
				allTables.push(child);
			}

			if (table.inline) {
				// this table does not 'really' exist as a separate table
				// but its fields are in the root table
				continue;
			}

			const tableOptions = canUseWithoutRowId(table)
				? " strict, without rowid"
				: " strict";
			const sqlCreateTable = `create table if not exists ${table.name} (${[...table.fields, ...table.constraints].map((s) => s.definition).join(", ")})${tableOptions}`;
			startupTableStatements.set(table.name, sqlCreateTable);

			startupStatements.push(
				{
					id: putStatementKey(table),
					sql: createInsertReturningSQL(table),
				},
				{
					id: insertKnownIdStatementKey(table),
					sql: createInsertKnownIdSQL(table),
				},
				{
					id: replaceStatementKey(table),
					sql: createReplaceSQL(table),
				},
			);
			if (table.parent) {
				startupStatements.push({
					id: resolveChildrenStatement(table),
					sql: selectChildren(table),
				});
			}

			/* const fieldsToIndex = table.fields.filter(
				(field) =>
					field.key !== ARRAY_INDEX_COLUMN && field.key !== table.primary,
			);
			if (fieldsToIndex.length > 0) {
				let arr = fieldsToIndex.map((field) => escapeColumnName(field.name));
		
				const createIndex = async (columns: string[]) => {
					const key = createIndexKey(table.name, columns)
					const command = `create index if not exists ${key} on ${table.name} (${columns.map((n) => escapeColumnName(n)).join(", ")})`;
					await this.properties.db.exec(command);
					table.indices.add(key);
		
		
		
					const rev = columns.reverse()
					const key2 = createIndexKey(table.name, rev)
					const command2 = `create index if not exists ${key2} on ${table.name} (${rev.join(", ")})`;
					await this.properties.db.exec(command2);
					table.indices.add(key2);
				}
				await createIndex(fieldsToIndex.map(x => x.name));
				await createIndex([table.primary as string, ...fieldsToIndex.map(x => x.name)]);
		
				if (arr.length > 1) {
					for (const field of fieldsToIndex) {
						await createIndex([field.name]);
						await createIndex([table.primary as string, field.name]);
		
					}
				}
			} */
		}

		await this.withDatabaseBarrier(async () => {
			if (startupTableStatements.size > 0) {
				const existingTables = await this.getExistingSQLiteObjects("table", [
					...startupTableStatements.keys(),
				]);
				const missingTableStatements = [...startupTableStatements.entries()]
					.filter(([tableName]) => !existingTables.has(tableName))
					.map(([, sql]) => sql);
				if (missingTableStatements.length > 0) {
					await this.properties.db.exec(missingTableStatements.join(";"));
				}
			}

			if (this.properties.db.prepareMany) {
				await this.properties.db.prepareMany(startupStatements);
			} else {
				for (const statement of startupStatements) {
					await this.properties.db.prepare(statement.sql, statement.id);
				}
			}
		});

		this.cursorPruner = setInterval(() => {
			const now = Date.now();
			for (const [k, v] of this._cursor) {
				if (v.expire < now) {
					this.clearupIterator(k);
				}
			}
		}, this.iteratorTimeout);

		this.setOpen();
	}

	private async getExistingSQLiteObjects(
		type: "table" | "index",
		names: string[],
	): Promise<Set<string>> {
		if (names.length === 0) {
			return new Set();
		}

		const sql = `select name from sqlite_master where type = ? and name in (${names
			.map(() => "?")
			.join(", ")})`;
		const statement =
			this.properties.db.statements.get(sql) ||
			(await this.properties.db.prepare(sql, sql));
		const rows = await statement.all([type, ...names]);
		await statement.reset?.();
		return new Set(
			(rows as Array<{ name?: string }>)
				.map((row) => row.name)
				.filter((name): name is string => typeof name === "string"),
		);
	}

	private async clearStatements() {
		if ((await this.properties.db.status()) === "closed") {
			// TODO this should never be true, but if we remove this statement the tests faiL for browser tests?
			return;
		}
	}

	async stop(): Promise<void> {
		if (this.state === "closed") {
			return;
		}
		if (this.state === "closing") {
			await this._operationBarrier.catch(() => undefined);
			return;
		}
		this.setClosing();

		try {
			clearInterval(this.cursorPruner!);

			await this._operationBarrier.catch(() => undefined);

			await this.clearStatements();

			this._tables?.clear();

			if (this._cursor) {
				for (const [k, _v] of this._cursor) {
					await this.clearupIterator(k);
				}
			}

			await this.planner.stop();
		} finally {
			// The tables close: readers must rebase once they are open again.
			this.changeJournal.beginEpoch();
			this.setClosed();
		}
	}

	async drop(): Promise<void> {
		const wasOpen = this.state === "open";
		if (wasOpen) {
			this.setClosing();
		}

		try {
			if (this.cursorPruner != null) {
				clearInterval(this.cursorPruner);
				this.cursorPruner = undefined;
			}

			// Enqueue the complete database phase before the first await. A concurrent
			// stop() that observes `closing` will then wait for this operation too.
			const databaseClosed = await this.withDatabaseBarrier(async () => {
				const status = await this.properties.db.status?.();
				if (status === "closed") {
					this._tables?.clear();
					return true;
				}

				await this.clearStatements();

				if (this._rootTables) {
					for (const table of this._rootTables) {
						await this.properties.db.exec(`drop table if exists ${table.name}`);
					}
				}
				return false;
			});
			if (databaseClosed) {
				return;
			}

			this._tables?.clear();

			if (this._cursor) {
				for (const [k, _v] of this._cursor) {
					await this.clearupIterator(k);
				}
			}
			await this.planner.stop();
		} finally {
			// The tables are gone: readers must rebase once they are recreated.
			this.changeJournal.beginEpoch();
			this.setClosed();
		}
	}

	private async resolveDependencies(
		parentId: any,
		table: Table,
	): Promise<any[]> {
		const stmt = await this.getOrPrepareStatement(
			resolveChildrenStatement(table),
			selectChildren(table),
		);
		const results = await stmt.all([parentId]);
		await stmt.reset?.();
		return results;
	}

	private async getOrPrepareStatement(key: string, sql: string) {
		const existing = this.properties.db.statements.get(key);
		if (existing) {
			return existing;
		}
		return this.properties.db.prepare(sql, key);
	}
	async get(
		id: types.IdKey,
		options?: { shape: Shape },
	): Promise<IndexedResult<T> | undefined> {
		return this.withDatabaseIfOpen(undefined, () =>
			this.getUnlocked(id, options),
		);
	}

	private async getUnlocked(
		id: types.IdKey,
		options?: { shape: Shape },
	): Promise<IndexedResult<T> | undefined> {
		for (const table of this._rootTables) {
			const { join: joinMap, selects } = selectAllFieldsFromTable(
				table,
				options?.shape,
			);
			const sql = `${generateSelectQuery(table, selects)} ${buildJoin(joinMap).join} where ${table.name}.${this.primaryKeyString} = ? limit 1`;
			const stmt = await this.properties.db.prepare(sql, sql);
			const rows = await stmt.get([
				table.primaryField?.from?.type
					? convertToSQLType(id.key, table.primaryField.from.type)
					: id.key,
			]);
			if (
				rows?.[getTablePrefixedField(table, table.primary as string)] == null
			) {
				continue;
			}
			return {
				value: (await resolveInstanceFromValue(
					rows,
					this.tables,
					table,
					this.resolveDependencies.bind(this),
					true,
					options?.shape,
				)) as unknown as T,
				id,
			};
		}
		return undefined;
	}

	async put(
		value: T,
		_id?: any,
		options?: { replace?: boolean },
	): Promise<void> {
		return this.withKeyMutation(() =>
			this.withDatabaseIfOpen(undefined, async () => {
				await this.withPutSavepoint(() => this.putUnlocked(value, options));
				this.mutationVersion++;
			}),
		);
	}

	private async putUnlocked(
		value: T,
		options?: { replace?: boolean },
	): Promise<void> {
		const classOfValue = value.constructor as Constructor<T>;
		const rootTable = resolveTable(
			this.scopeString ? [this.scopeString] : [],
			this.tables,
			classOfValue,
			true,
		);
		await insert(
			async (values, table) => {
				let preId = values[table.primaryIndex];
				let statement: Statement | undefined = undefined;
				try {
					if (preId != null) {
						const shouldReplace = options?.replace ?? true;
						if (!shouldReplace) {
							statement = await this.getOrPrepareStatement(
								insertKnownIdStatementKey(table),
								createInsertKnownIdSQL(table),
							);
							try {
								this.fkMode === "race-tolerant"
									? await runIgnoreFK(statement, values)
									: await statement.run(values);
							} catch (error) {
								if (!isUniqueConstraintError(error)) {
									throw error;
								}
								await statement.reset?.();
								statement = await this.getOrPrepareStatement(
									replaceStatementKey(table),
									createReplaceSQL(table),
								);
								this.fkMode === "race-tolerant"
									? await runIgnoreFK(statement, values)
									: await statement.run(values);
							}
						} else {
							statement = await this.getOrPrepareStatement(
								replaceStatementKey(table),
								createReplaceSQL(table),
							);
							this.fkMode === "race-tolerant"
								? await runIgnoreFK(statement, values)
								: await statement.run(values);
						}
						if (table === rootTable && this.changeJournal.recording) {
							this.recordRootKey(rootTable, preId);
						}
						return preId;
					} else {
						statement = await this.getOrPrepareStatement(
							putStatementKey(table),
							createInsertReturningSQL(table),
						);
						const out =
							this.fkMode === "race-tolerant"
								? await getIgnoreFK(statement, values)
								: await statement.get(values);

						// TODO types
						if (out == null) {
							return undefined;
						}
						const id = out[table.primary as string];
						if (table === rootTable && this.changeJournal.recording) {
							this.recordRootKey(rootTable, id);
						}
						return id;
					}
				} finally {
					await statement?.reset?.();
				}
			},
			value,
			this.tables,
			rootTable,
			getSchema(classOfValue).fields,
			(_fn) => {
				throw new Error("Unexpected");
			},
			undefined,
			undefined,
			{
				insertSimpleVecRows: async (rows, table) => {
					if (rows.length === 0) {
						return;
					}
					const key = putStatementBatchNoReturnKey(table, rows.length);
					const sql = createBatchInsertSQL(table, rows.length);
					const statement =
						this.properties.db.statements.get(key) ||
						(await this.properties.db.prepare(sql, key));
					const values = rows.flat();
					this.fkMode === "race-tolerant"
						? await runIgnoreFK(statement, values)
						: await statement.run(values);
				},
			},
		);
	}

	/**
	 * Stages the key of a root row this admission wrote, as soon as its
	 * statement succeeded: if a nested row then fails and the rollback fails
	 * too, the row may still commit. The key is published when the caller's
	 * transaction commits (see withPutSavepoint and the ordered session's
	 * flush), and discarded if its savepoint rolls back. Callers check
	 * `recording` first, inline: it is read once per value.
	 */
	private recordRootKey(table: Table, key: unknown): void {
		this.changeJournal.record(this.journalTableOf(table), key);
	}

	/**
	 * The journal's ordinal for a root table: its position in `_rootTables`,
	 * or else the position of the root table with its name. A delete planned
	 * before stop() and start() holds the previous Table objects, which are no
	 * longer there. -1 if none matches, which `record` treats as a change it
	 * cannot name.
	 */
	private journalTableOf(table: Table): number {
		const ordinal = this._rootTables.indexOf(table);
		return ordinal >= 0
			? ordinal
			: this._rootTables.findIndex((root) => root.name === table.name);
	}

	private async withPutSavepoint(write: () => Promise<void>): Promise<void> {
		const savepoint = `peerbit_put_batch_${this.databaseCoordinator.nextSavepointId++}`;
		const journalSavepoint = this.changeJournal.savepoint();
		await this.properties.db.exec(`SAVEPOINT ${savepoint}`);
		try {
			await write();
			await this.properties.db.exec(`RELEASE SAVEPOINT ${savepoint}`);
		} catch (error) {
			try {
				await this.properties.db.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
				await this.properties.db.exec(`RELEASE SAVEPOINT ${savepoint}`);
			} catch (rollbackError) {
				throw new AggregateError(
					[error, rollbackError],
					"SQLite write and rollback both failed",
				);
			}
			this.changeJournal.rollback(journalSavepoint);
			throw error;
		} finally {
			// This savepoint is the outermost one, so a release committed. After a
			// failed rollback the writes may persist: publish their keys too.
			this.changeJournal.commit();
		}
	}

	async putBatch(values: T[]): Promise<void> {
		return this.withKeyMutation(() =>
			this.withDatabaseIfOpen(undefined, async () => {
				for (
					let offset = 0;
					offset < values.length;
					offset += PUT_BATCH_CHUNK_SIZE
				) {
					await this.withPutSavepoint(async () => {
						for (const value of values.slice(
							offset,
							offset + PUT_BATCH_CHUNK_SIZE,
						)) {
							await this.putUnlocked(value);
						}
					});
					this.mutationVersion++;
				}
			}),
		);
	}

	async withOrderedWriteSession<R>(
		operation: (session: types.OrderedIndexWriteSession<T>) => Promise<R> | R,
	): Promise<R> {
		return this.withKeyMutation(() => this.orderedWriteSession(operation));
	}

	private async orderedWriteSession<R>(
		operation: (session: types.OrderedIndexWriteSession<T>) => Promise<R> | R,
	): Promise<R> {
		this.assertOpen();
		let active = true;
		let releaseAdmission: (() => void) | undefined;
		let chunkSavepoint: string | undefined;
		let successfulWrites = 0;

		const assertActive = () => {
			if (!active) {
				throw new Error("Ordered index write session is no longer active");
			}
		};

		const acquireAdmission = async () => {
			if (releaseAdmission) {
				return;
			}
			this.assertOpen();
			const release = await this.acquireDatabaseBarrier();
			if (this.state !== "open") {
				release();
				throw new types.NotStartedError();
			}
			releaseAdmission = release;
		};

		const flush = async () => {
			const release = releaseAdmission;
			if (!release) {
				return;
			}
			const savepoint = chunkSavepoint;
			const writes = successfulWrites;
			try {
				if (savepoint) {
					await this.properties.db.exec(`RELEASE SAVEPOINT ${savepoint}`);
					if (writes > 0) {
						this.mutationVersion++;
					}
				}
			} finally {
				// Publish before another admission can read. If the release failed,
				// the chunk's writes may still commit later: publish them anyway.
				this.changeJournal.commit();
				chunkSavepoint = undefined;
				successfulWrites = 0;
				releaseAdmission = undefined;
				release();
			}
		};

		const session: types.OrderedIndexWriteSession<T> = {
			get: async (id, options) => {
				assertActive();
				await acquireAdmission();
				try {
					return await this.getUnlocked(id, options);
				} finally {
					// Match an ordinary scalar get's admission window when there is
					// no successful prefix whose transaction must remain open.
					if (!chunkSavepoint) {
						await flush();
					}
				}
			},
			put: async (value, _id, options) => {
				assertActive();
				await acquireAdmission();
				if (!chunkSavepoint) {
					const nextChunkSavepoint = `peerbit_ordered_chunk_${this.databaseCoordinator.nextSavepointId++}`;
					await this.properties.db.exec(`SAVEPOINT ${nextChunkSavepoint}`);
					chunkSavepoint = nextChunkSavepoint;
				}

				const rowSavepoint = `peerbit_ordered_row_${this.databaseCoordinator.nextSavepointId++}`;
				const journalSavepoint = this.changeJournal.savepoint();
				let rowOpen = false;
				try {
					await this.properties.db.exec(`SAVEPOINT ${rowSavepoint}`);
					rowOpen = true;
					await this.putUnlocked(value, options);
					await this.properties.db.exec(`RELEASE SAVEPOINT ${rowSavepoint}`);
					rowOpen = false;
					successfulWrites++;
					if (successfulWrites >= PUT_BATCH_CHUNK_SIZE) {
						await flush();
					}
				} catch (error) {
					let cleanupError: unknown;
					if (rowOpen) {
						try {
							await this.properties.db.exec(
								`ROLLBACK TO SAVEPOINT ${rowSavepoint}`,
							);
							await this.properties.db.exec(
								`RELEASE SAVEPOINT ${rowSavepoint}`,
							);
							this.changeJournal.rollback(journalSavepoint);
						} catch (rollbackError) {
							cleanupError = rollbackError;
						}
					}
					try {
						await flush();
					} catch (flushError) {
						cleanupError = cleanupError
							? new AggregateError(
									[cleanupError, flushError],
									"SQLite ordered write cleanup failed",
								)
							: flushError;
					}
					if (cleanupError) {
						throw new AggregateError(
							[error, cleanupError],
							"SQLite ordered write and cleanup both failed",
						);
					}
					throw error;
				}
			},
			flush: async () => {
				assertActive();
				await flush();
			},
		};

		try {
			const result = await operation(session);
			await flush();
			return result;
		} catch (error) {
			try {
				await flush();
			} catch (flushError) {
				throw new AggregateError(
					[error, flushError],
					"SQLite ordered callback and prefix commit both failed",
				);
			}
			throw error;
		} finally {
			active = false;
			releaseAdmission?.();
			releaseAdmission = undefined;
		}
	}

	get scanKeyPrimitives(): Index<T>["scanKeyPrimitives"] {
		// Independent variant tables can contain the same key. Do not advertise a
		// union inventory until it can deduplicate without retaining all keys.
		if (this._rootTables.length > 1) return undefined;
		return (options) => this.createKeyScan(options);
	}

	private createKeyScan({
		pageSize,
		signal,
	}: types.IndexKeyScanOptions): types.IndexKeyScan {
		if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 4096) {
			throw new RangeError(
				"Key scan pageSize must be an integer from 1 to 4096",
			);
		}
		this.assertOpen();
		if (this._rootTables.length !== 1)
			throw new Error("Key inventory requires one root table");
		const table = this._rootTables[0]!;
		const primary = escapeColumnName(table.primary as string);
		const generation = (this.keyScanGeneration ??= Symbol());
		let owner: SQLiteIndex<T> | undefined = this;
		let anchor: BindableValue | undefined;
		let terminal: Exclude<types.IndexKeyScanPage["status"], "more"> | undefined;
		const finish = (status: NonNullable<typeof terminal>) => {
			if (terminal) return;
			terminal = status;
			owner = undefined;
			anchor = undefined;
			signal?.removeEventListener("abort", onAbort);
		};
		const onAbort = () => finish("aborted");
		const checkOwner = () => {
			if (terminal) return;
			if (signal?.aborted) finish("aborted");
			else if (owner!.state !== "open") finish("closed");
			else if (
				owner!.keyScanGeneration !== generation ||
				owner!.keyScanMutations
			)
				finish("invalidated");
		};
		checkOwner();
		if (!terminal) signal?.addEventListener("abort", onAbort, { once: true });
		return {
			next: async () => {
				checkOwner();
				if (terminal) return { status: terminal, keys: [] };
				const current = owner!;
				try {
					return await current.withDatabaseBarrier(
						async (): Promise<types.IndexKeyScanPage> => {
							checkOwner();
							if (terminal) return { status: terminal, keys: [] };
							const sql = `select ${primary} as key from ${table.name}${anchor === undefined ? "" : ` where ${primary} > ?`} order by ${primary} limit ?`;
							const statement = await current.getOrPrepareStatement(sql, sql);
							checkOwner();
							if (terminal) return { status: terminal, keys: [] };
							let rows: { key: any }[];
							try {
								rows = await statement.all(
									anchor === undefined ? [pageSize] : [anchor, pageSize],
								);
							} catch (error) {
								try {
									await statement.reset?.();
								} catch {
									// Preserve the read error if cleanup also fails.
								}
								throw error;
							}
							await statement.reset?.();
							checkOwner();
							if (terminal) return { status: terminal, keys: [] };
							const keys = rows.map(
								({ key }) =>
									types.toId(
										convertFromSQLType(key, table.primaryField!.unwrappedType),
									).primitive,
							);
							if (rows.length < pageSize) {
								finish("complete");
								return { status: "complete", keys };
							}
							const last = rows[rows.length - 1]!.key;
							// Seek with the stored SQL value, not a decoded u64/byte primitive.
							anchor = last instanceof Uint8Array ? last.slice() : last;
							return { status: "more", keys };
						},
					);
				} catch (error) {
					finish("failed");
					throw error;
				}
			},
			close: () => finish("closed"),
		};
	}

	iterate<S extends Shape | undefined>(
		request?: types.IterateOptions,
		options?: { shape?: S; reference?: boolean },
	): types.IndexIterator<T, S> {
		if (this.isClosing()) {
			return SQLiteIndex.closedIterator<T, S>();
		}
		this.assertOpen();
		type Result = IndexedResult<types.ReturnTypeFromShape<T, S>>;
		/** A result and the string of its key (`keyString`). */
		type Keyed = { k: string; result: Result };
		type KeyStatement = { sql: string; bindable: readonly unknown[] };

		// An iterator reads its search in the order key K (the sort terms, the
		// primary key, then the root table) and binds no OFFSET, from one of two
		// sources.
		//
		// - Pages (K): a search over one root table and one branch, with no
		//   child table in its query, reads its first page with the search's
		//   own statement, LIMIT n+1 (S1); the extra row tells whether more
		//   remain. In primary-key order later pages continue after the last
		//   key returned (S2) while nothing is written or marked, for up to
		//   PAGED_ROWS_BUDGET rows. Pages hold the keys they returned (Y), and
		//   no key list or journal reader.
		// - Key list (S): every other call lists the keys of the matching rows
		//   in K order, one statement (S3), without the keys returned so far,
		//   and every page reads its rows by key (S5). While it holds that key
		//   list it reads the change journal: each call re-checks the keys
		//   written since the last one with the search restricted to those keys
		//   (S4), and returns a changed row at its current position.
		//
		// Every call (next, pending, all) runs all of its statements in one
		// database admission, and changes nothing unless they all succeed. It
		// sees the rows that match when it is admitted, in K order, without the
		// rows that an earlier call returned or `markYielded` named (Y), and one
		// row per primary key value (the first in K order).
		const shape = options?.shape;
		const normalizedQuery = new PlannableQuery({
			query: coerceLocalQueries(request?.query),
			sort: coerceLocalSorts(request?.sort),
		});
		const identity = new KeyIdentity(this._rootTables);
		const requestId = uuid();

		let explicitlyClosed = false;
		/** A call was made: all() no longer reads everything in one statement. */
		let called = false;
		/** A call was admitted, so done() is known. */
		let started = false;
		/** Whether the last call left a row to return. */
		let hasMore = true;
		/** The writes and journal epoch when the last call left no row. */
		let doneAt: { version: number; epoch: number } | undefined;

		// Y is the key list's positions before `pos` that were not touched, and
		// `extra`.
		let snapshot: Snapshot | undefined;
		let pos = 0;
		/** Keys written since the key list was read: never read from it again. */
		const touched = new Map<string, unknown>();
		/** Touched keys, not in Y, whose current row was not checked yet. */
		const dirty = new Map<string, StoredKey>();
		/** Keys in Y that the key list's positions before `pos` do not cover. */
		const extra = new Map<string, unknown>();
		/** Keys `markYielded` named that no admission has applied yet. */
		const pendingMarks: { k: string; value: unknown }[] = [];
		let reader: ChangeJournalReader | undefined;
		/** The key list was dropped: the next call lists the keys again. */
		let needsRebase = false;
		/** Calls in flight keep the iterator's state until the last returns. */
		let inFlight = 0;
		/** The index closed during a call: drop the reader when none runs. */
		let releaseWhenIdle = false;

		// Pages (K). Y is `extra`: the keys the pages returned.
		/** The writes and journal epoch when the first page was read. */
		let pagedAt: { version: number; epoch: number } | undefined;
		/** Rows the pages read, the extra rows included. */
		let pagedRows = 0;
		/** The primary key (as stored) of the last row a page returned. */
		let pagedCursor: unknown;
		/** A mark was applied: pages cannot skip it, so the key list serves. */
		let marked = false;
		/** The matching rows for pending(), while nothing is written. */
		let counted: { version: number; epoch: number; count: number } | undefined;

		type Plan = {
			/** The journal epoch the statements were planned in. */
			epoch: number;
			scope: PlanningSession;
			severalRoots: boolean;
			/** S3, the key list, and its UNION arms. */
			keys: KeyStatement & { arms: number };
			/** S4, the search restricted to a key list. */
			recheck: KeyStatement;
			/** S5, rows by key, per root table. */
			rows: KeyStatement[];
			/** S1, the head page, when the search can page (K). */
			head?: KeyStatement;
			/** S2, the page after a primary key, in primary-key order. */
			after?: KeyStatement;
			/** The count of matching rows, when the search can page. */
			count?: KeyStatement;
		};
		let plan: Plan | undefined;
		/** This iterator's prepared statements, by SQL. */
		const statements = new Map<string, Statement>();

		// Planned outside the admission: the planner may create an index, which
		// takes the admission itself. Planned again once the tables closed and
		// opened (a new journal epoch).
		const planStatements = async (): Promise<Plan> => {
			const epoch = this.changeJournal.epoch;
			if (plan?.epoch === epoch) {
				return plan;
			}
			const scope = this.planner.scope(normalizedQuery);
			const rootTables = this._rootTables;
			const keys = convertSearchRequestToKeyQuery(
				normalizedQuery,
				this.tables,
				rootTables,
				{ planner: scope },
			);
			const recheck = convertSearchRequestToKeyQuery(
				normalizedQuery,
				this.tables,
				rootTables,
				{
					planner: scope,
					restrictToKeys: true,
					keysPerRoot: !identity.uniform,
				},
			);
			const rows = rootTables.map((table) =>
				convertKeysToRowsQuery(table, shape, { planner: scope }),
			);
			// Pages need one root table and one branch, and no child table in the
			// query: then S1 stops after its LIMIT, without grouping every row.
			let head: KeyStatement | undefined;
			let after: KeyStatement | undefined;
			let count: KeyStatement | undefined;
			if (rootTables.length === 1 && keys.arms === 1 && !keys.grouped) {
				const first = convertSearchRequestToPageQuery(
					normalizedQuery,
					this.tables,
					rootTables,
					{ planner: scope, shape },
				);
				// Sorting rows whose hydration groups child rows reads every row
				// before the first: the key list serves that search.
				if (!first.order.sorted || !first.grouped) {
					head = first;
					// K is the primary key alone.
					if (first.order.terms.length === 1) {
						after = convertSearchRequestToPageQuery(
							normalizedQuery,
							this.tables,
							rootTables,
							{ planner: scope, shape, after: [false] },
						);
					}
					count = convertCountRequestToQuery(
						{ query: normalizedQuery.query },
						this.tables,
						rootTables[0],
					);
				}
			}
			await scope.beforePrepare();
			statements.clear();
			return (plan = {
				epoch,
				scope,
				severalRoots: rootTables.length > 1,
				keys,
				recheck,
				rows,
				head,
				after,
				count,
			});
		};

		const prepare = async (statement: KeyStatement) => {
			let prepared = statements.get(statement.sql);
			if (!prepared) {
				prepared = await this.properties.db.prepare(
					statement.sql,
					statement.sql,
				);
				statements.set(statement.sql, prepared);
			}
			return prepared;
		};
		const run = async (
			current: Plan,
			statement: KeyStatement,
			values: Parameters<typeof bindKeyQuery>[1] = {},
		): Promise<Record<string, any>[]> => {
			const bound = bindKeyQuery(statement.bindable, values);
			const prepared = await prepare(statement);
			const rows = await current.scope.perform(async () => prepared.all(bound));
			if (explicitlyClosed) {
				throw ITERATOR_CLOSED;
			}
			return rows;
		};

		const rowKey = (current: Plan, row: Record<string, any>): StoredKey => ({
			t: current.severalRoots ? Number(row[ROOT_ORDINAL_ALIAS]) : 0,
			pk: row[PRIMARY_KEY_ALIAS],
		});
		const keyOf = (key: StoredKey) => keyString(identity.value(key.t, key.pk));

		const hydrate = async (
			rows: { k: string; row: Record<string, any>; key: StoredKey }[],
		): Promise<Keyed[]> => {
			const results = await Promise.all(
				rows.map(async ({ k, row, key }) => {
					const table = this._rootTables[key.t];
					return {
						k,
						result: {
							value: await resolveInstanceFromValue<T, S>(
								row,
								this.tables,
								table,
								this.resolveDependencies.bind(this),
								true,
								shape,
							),
							id: types.toId(
								convertFromSQLType(
									key.pk as string | number | bigint | Uint8Array,
									table.primaryField!.from!.type,
								),
							),
						},
					};
				}),
			);
			if (explicitlyClosed) {
				throw ITERATOR_CLOSED;
			}
			return results;
		};

		/** S5 for `keys`, once per root table, in the order of `keys`. */
		const fetchRows = async (
			current: Plan,
			keys: StoredKey[],
		): Promise<Keyed[]> => {
			const byTable = new Map<number, unknown[]>();
			for (const key of keys) {
				let list = byTable.get(key.t);
				if (!list) {
					byTable.set(key.t, (list = []));
				}
				list.push(key.pk);
			}
			const found = new Map<string, Record<string, any>>();
			for (const [t, list] of byTable) {
				const rows = await run(current, current.rows[t], {
					keys: encodeKeyList(list),
				});
				for (const row of rows) {
					found.set(keyOf({ t, pk: row[PRIMARY_KEY_ALIAS] }), row);
				}
			}
			const ordered: { k: string; row: Record<string, any>; key: StoredKey }[] =
				[];
			for (const key of keys) {
				const k = keyOf(key);
				const row = found.get(k);
				// No row: deleted by a write the journal does not see (raw SQL,
				// another index instance). It is skipped.
				if (row) {
					ordered.push({
						k,
						row,
						key: { t: key.t, pk: row[PRIMARY_KEY_ALIAS] },
					});
				}
			}
			return hydrate(ordered);
		};

		/** The key lists for S4: one list, or one per root table in its encoding. */
		const keyLists = (
			current: Plan,
			keys: Iterable<StoredKey>,
		): Parameters<typeof bindKeyQuery>[1] => {
			if (identity.uniform) {
				return { keys: encodeKeyList(Array.from(keys, (key) => key.pk)) };
			}
			const lists: unknown[][] = current.rows.map(() => []);
			for (const key of keys) {
				const value = identity.value(key.t, key.pk);
				for (let t = 0; t < lists.length; t++) {
					const stored = t === key.t ? key.pk : identity.encode(t, value);
					if (stored !== undefined) {
						lists[t].push(stored);
					}
				}
			}
			return { rootKeys: lists.map((list) => encodeKeyList(list)) };
		};

		const keyAt = (list: Snapshot, i: number) =>
			keyString(identity.value(snapshotTable(list, i), list.pk[i]));
		/** Whether position `i` is never read: its key was touched or is in `extra`. */
		const skipped = (list: Snapshot, i: number) => {
			const k = keyAt(list, i);
			return touched.has(k) || extra.has(k);
		};
		const isYielded = (k: string, value: unknown) => {
			if (extra.has(k)) {
				return true;
			}
			if (!snapshot || pos === 0 || touched.has(k)) {
				return false;
			}
			const at = snapshotPositionOf(snapshot, identity, value);
			return at >= 0 && at < pos;
		};
		const yieldedKeys = (): Map<string, unknown> => {
			const yielded = new Map<string, unknown>();
			if (snapshot) {
				for (let i = 0; i < pos; i++) {
					const value = identity.value(
						snapshotTable(snapshot, i),
						snapshot.pk[i],
					);
					const k = keyString(value);
					if (!touched.has(k)) {
						yielded.set(k, value);
					}
				}
			}
			for (const [k, value] of extra) {
				yielded.set(k, value);
			}
			return yielded;
		};

		const applyMarks = () => {
			for (const { k, value } of pendingMarks) {
				dirty.delete(k);
				extra.set(k, value);
				marked = true;
			}
			pendingMarks.length = 0;
		};
		/**
		 * Marks that arrived while a call ran take their rows out of its result:
		 * another delivery path owns them.
		 */
		const withoutMarked = (rows: Keyed[]): Result[] => {
			if (pendingMarks.length === 0) {
				return rows.map((row) => row.result);
			}
			const marked = new Set(pendingMarks.map((mark) => mark.k));
			applyMarks();
			return rows.filter((row) => !marked.has(row.k)).map((row) => row.result);
		};

		/** A journal entry: the key changed since the key list was read. */
		const absorb = (t: number, stored: unknown) => {
			const value = identity.value(t, stored);
			const k = keyString(value);
			if (isYielded(k, value)) {
				return; // never returned again, whatever the write did
			}
			touched.set(k, value);
			dirty.set(k, { t, pk: stored });
		};

		/**
		 * Lists the keys again (S3), without Y. Correct whatever happened since
		 * the last call: it continues from the first remaining row in K order.
		 */
		const rebase = async (current: Plan) => {
			// The key list held is stale until the one below is read. If the
			// call fails before then, the next call lists the keys again: a
			// reader registered here starts after the writes it must see.
			needsRebase = true;
			const yielded = yieldedKeys();
			let pull: ChangeJournalPull | undefined;
			if (reader) {
				pull = this.changeJournal.pull(reader);
			} else {
				// Before the key list is read: every later write is journaled.
				// The iterator expires, and drops the reader, if it idles.
				reader = this.changeJournal.register();
				this._cursor.set(requestId, cursorEntry);
			}
			const rows = await run(current, current.keys);
			const keys: unknown[] = [];
			const ordinals: number[] = [];
			const seen = current.keys.arms > 1 ? new Set<string>() : undefined;
			for (const row of rows) {
				const key = rowKey(current, row);
				if (seen) {
					// One row per key: the first in K order.
					const k = keyOf(key);
					if (seen.has(k)) {
						continue;
					}
					seen.add(k);
				}
				keys.push(key.pk);
				ordinals.push(key.t);
			}
			const list: Snapshot = {
				pk: compactKeys(keys),
				t: current.severalRoots ? compactOrdinals(ordinals) : undefined,
			};
			// What was yielded is usually a prefix of the new list.
			let at = 0;
			while (
				at < keys.length &&
				yielded.size > 0 &&
				yielded.delete(keyAt(list, at))
			) {
				at++;
			}
			snapshot = list;
			pos = at;
			touched.clear();
			dirty.clear();
			extra.clear();
			for (const [k, value] of yielded) {
				extra.set(k, value);
			}
			needsRebase = false;
			if (pull) {
				this.changeJournal.advance(reader, pull);
			}
		};

		/**
		 * Brings the key list to this admission: reads the journal since the
		 * last call, or lists the keys again. Returns the pull to advance past
		 * once the call succeeded.
		 */
		const sync = async (
			current: Plan,
		): Promise<ChangeJournalPull | undefined> => {
			if (snapshot && reader && !needsRebase) {
				const pull = this.changeJournal.pull(reader, absorb);
				// Behind the journal, or a bulk change that one key list costs
				// less than re-checking on every page.
				if (!pull.rebase && dirty.size <= SNAPSHOT_WINDOW) {
					return pull;
				}
			}
			await rebase(current);
			return undefined;
		};

		/**
		 * The next `amount` rows from the key list and the changed keys,
		 * without committing anything.
		 */
		const readSnapshot = async (current: Plan, amount: number) => {
			const list = snapshot!;
			const size = list.pk.length;
			const skipping = touched.size > 0 || extra.size > 0;
			/** Changed keys returned, and changed keys that no longer match. */
			const emitted = new Map<string, unknown>();
			const dropped = new Set<string>();
			const changedLeft = () => {
				const left: [string, StoredKey][] = [];
				for (const entry of dirty) {
					if (!emitted.has(entry[0]) && !dropped.has(entry[0])) {
						left.push(entry);
					}
				}
				return left;
			};
			const rows: Keyed[] = [];
			let cursor = pos;
			for (;;) {
				const want = Math.min(amount - rows.length, SNAPSHOT_WINDOW);
				if (want <= 0) {
					break;
				}
				const window: number[] = [];
				let scan = cursor;
				while (window.length < want && scan < size) {
					if (!skipping || !skipped(list, scan)) {
						window.push(scan);
					}
					scan++;
				}
				while (skipping && scan < size && skipped(list, scan)) {
					scan++;
				}
				/** A key of the list is left after the window. */
				const more = scan < size;
				const changed = changedLeft();
				let page: StoredKey[];
				if (changed.length === 0) {
					page = window.map((i) => ({
						t: snapshotTable(list, i),
						pk: list.pk[i],
					}));
					cursor = scan;
				} else {
					// Merge the window, unchanged since the list was read (so it
					// still matches, in the same order), with the changed keys in
					// their current order (S4). A changed row after the window's
					// last row waits while the list has more: those come first.
					const windowAt = new Map<string, number>();
					const candidates: StoredKey[] = [];
					for (const i of window) {
						windowAt.set(keyAt(list, i), i);
						candidates.push({ t: snapshotTable(list, i), pk: list.pk[i] });
					}
					const changedKeys = new Map(changed);
					for (const [, key] of changed) {
						candidates.push(key);
					}
					const ordered = await run(
						current,
						current.recheck,
						keyLists(current, candidates),
					);
					const last = window.length > 0 ? window[window.length - 1] : -1;
					let passedLast = false;
					let lastRead = -1;
					const seen = new Set<string>();
					page = [];
					for (const row of ordered) {
						const key = rowKey(current, row);
						const value = identity.value(key.t, key.pk);
						const k = keyString(value);
						if (seen.has(k)) {
							continue; // one row per key: the first in K order
						}
						seen.add(k);
						if (page.length >= want) {
							continue;
						}
						const at = windowAt.get(k);
						if (at !== undefined) {
							page.push(key);
							lastRead = at;
							passedLast ||= at === last;
						} else if (changedKeys.has(k) && !(more && passedLast)) {
							page.push(key);
							emitted.set(k, value);
						}
					}
					for (const k of changedKeys.keys()) {
						if (!seen.has(k)) {
							dropped.add(k);
						}
					}
					// A short page returned every window row it found. A full one
					// stops after the last window row it returned.
					cursor =
						page.length < want ? scan : lastRead >= 0 ? lastRead + 1 : cursor;
				}
				if (page.length > 0) {
					rows.push(...(await fetchRows(current, page)));
				}
				if (!more && (page.length === 0 || changedLeft().length === 0)) {
					break;
				}
			}
			return { rows, pos: cursor, emitted, dropped };
		};

		/**
		 * After a call: moves `pos` past the keys it never reads, and sets
		 * `hasMore`, which is exact: an unread key of the list is unchanged, so
		 * it still matches, and every dirty key left was re-checked and matches.
		 */
		const settleSnapshot = () => {
			const list = snapshot!;
			if (touched.size > 0 || extra.size > 0) {
				while (pos < list.pk.length && skipped(list, pos)) {
					pos++;
				}
			}
			hasMore = pos < list.pk.length || dirty.size > 0;
		};

		/** Inside the admission, after every statement of a call succeeded. */
		const settle = (pull: ChangeJournalPull | undefined) => {
			if (pull && reader) {
				this.changeJournal.advance(reader, pull);
			}
			if (hasMore) {
				doneAt = undefined;
				this._cursor.set(requestId, cursorEntry);
				return;
			}
			// Nothing left: no reader until a write could make a row match.
			doneAt = {
				version: this.mutationVersion,
				epoch: this.changeJournal.epoch,
			};
			if (reader) {
				this.changeJournal.unregister(reader);
				reader = undefined;
			}
			this._cursor.delete(requestId);
		};

		/** Done, and nothing was written since: no row can match. */
		const unchangedSinceDone = () =>
			doneAt !== undefined &&
			doneAt.version === this.mutationVersion &&
			doneAt.epoch === this.changeJournal.epoch;

		/**
		 * Nothing was written, marked or reopened since the first page: every
		 * key in Y matches, and the pages read a prefix of K order.
		 */
		const pagesHold = (current: Plan) =>
			current.head !== undefined &&
			!snapshot &&
			!marked &&
			(!pagedAt ||
				(pagedAt.version === this.mutationVersion &&
					pagedAt.epoch === this.changeJournal.epoch));

		/**
		 * Whether pages serve the next page: the first one (S1), or one after
		 * the last key returned (S2) within the budget.
		 */
		const pagesServe = (current: Plan) =>
			pagesHold(current) &&
			(!pagedAt ||
				(current.after !== undefined &&
					pagedCursor !== undefined &&
					pagedRows <= PAGED_ROWS_BUDGET));

		/** The next page from S1 or S2: `amount` rows, and one to look ahead. */
		const readPaged = async (current: Plan, amount: number) => {
			const first = !pagedAt;
			const limit = amount === Infinity ? Infinity : amount + 1;
			const rows = first
				? await run(current, current.head!, { limit })
				: await run(current, current.after!, {
						cursor: [pagedCursor],
						limit,
					});
			const page = (rows.length > amount ? rows.slice(0, amount) : rows).map(
				(row) => {
					const key = rowKey(current, row);
					return { k: keyOf(key), row, key };
				},
			);
			const keyed = await hydrate(page);
			// Every statement succeeded: commit.
			if (first) {
				pagedAt = {
					version: this.mutationVersion,
					epoch: this.changeJournal.epoch,
				};
			}
			pagedRows += rows.length;
			if (page.length > 0) {
				pagedCursor = page[page.length - 1].key.pk;
			}
			for (const { k, key } of page) {
				extra.set(k, identity.value(key.t, key.pk));
			}
			hasMore = rows.length > amount;
			const results = withoutMarked(keyed);
			settle(undefined);
			return results;
		};

		const readPage = async (current: Plan, amount: number) => {
			applyMarks();
			if (unchangedSinceDone()) {
				return [];
			}
			if (pagesServe(current)) {
				return readPaged(current, amount);
			}
			// The key list serves: list the keys (without Y) when there is none.
			const pull = await sync(current);
			const page = await readSnapshot(current, amount);
			// Every statement succeeded: commit.
			pos = page.pos;
			for (const [k, value] of page.emitted) {
				dirty.delete(k);
				extra.set(k, value);
			}
			for (const k of page.dropped) {
				dirty.delete(k); // stays touched until it changes again
			}
			const results = withoutMarked(page.rows);
			settleSnapshot();
			settle(pull);
			return results;
		};

		const countPending = async (current: Plan) => {
			applyMarks();
			if (unchangedSinceDone()) {
				return 0;
			}
			if (pagesHold(current)) {
				// The matching rows minus Y, which all match: one COUNT per write.
				const version = this.mutationVersion;
				const epoch = this.changeJournal.epoch;
				if (counted?.version !== version || counted.epoch !== epoch) {
					const statement = await prepare(current.count!);
					const row = await statement.get(
						current.count!.bindable as BindableValue[],
					);
					if (explicitlyClosed) {
						throw ITERATOR_CLOSED;
					}
					counted = { version, epoch, count: Number(row?.count ?? 0) };
				}
				const count = Math.max(counted.count - extra.size, 0);
				hasMore = count > 0;
				settle(undefined);
				return count;
			}
			const pull = await sync(current);
			const list = snapshot!;
			// Positions from `pos` on that a page skips: yielded elsewhere, or
			// touched (counted below when they still match).
			let skippedAhead = 0;
			for (const value of extra.values()) {
				if (snapshotPositionOf(list, identity, value) >= pos) {
					skippedAhead++;
				}
			}
			for (const [k, value] of touched) {
				if (!extra.has(k) && snapshotPositionOf(list, identity, value) >= pos) {
					skippedAhead++;
				}
			}
			let changed = 0;
			if (dirty.size > 0) {
				const matched = new Set<string>();
				for (const row of await run(
					current,
					current.recheck,
					keyLists(current, dirty.values()),
				)) {
					matched.add(keyOf(rowKey(current, row)));
				}
				changed = matched.size;
			}
			const count = Math.max(list.pk.length - pos - skippedAhead, 0) + changed;
			hasMore = count > 0;
			settle(pull);
			return count;
		};

		// The index closed while a call ran: the call returns nothing, and the
		// iterator drops its journal reader once no call runs.
		const closeAsDone = () => {
			started = true;
			hasMore = false;
			doneAt = undefined;
			releaseWhenIdle = true;
		};

		/** Runs `work` in one admission, planned before it. */
		const admit = async <R>(
			fallback: R,
			work: (current: Plan) => Promise<R>,
		): Promise<R> => {
			try {
				for (;;) {
					if (this.isClosing()) {
						closeAsDone();
						return fallback;
					}
					this.assertOpen();
					const current = await planStatements();
					if (explicitlyClosed) {
						return fallback;
					}
					if (this.closed) {
						closeAsDone();
						return fallback;
					}
					const outcome = await this.withDatabaseBarrier(async () => {
						if (explicitlyClosed) {
							return { value: fallback };
						}
						if (this.isClosing()) {
							closeAsDone();
							return { value: fallback };
						}
						this.assertOpen();
						if (current.epoch !== this.changeJournal.epoch) {
							return undefined; // the tables closed meanwhile: plan again
						}
						started = true;
						cursorEntry.expire = Date.now() + this.iteratorTimeout;
						return { value: await work(current) };
					});
					if (outcome) {
						return explicitlyClosed ? fallback : outcome.value;
					}
				}
			} catch (error) {
				if (error === ITERATOR_CLOSED || explicitlyClosed || this.isClosing()) {
					if (!explicitlyClosed) {
						closeAsDone();
					}
					return fallback;
				}
				throw error;
			}
		};

		/**
		 * all() as the first call, with no marks: one statement, as when nothing
		 * is iterated (unordered without a sort). Every key it returns is in Y.
		 */
		const readAllFresh = async (): Promise<Result[]> => {
			try {
				if (this.isClosing()) {
					closeAsDone();
					return [];
				}
				this.assertOpen();
				const scope = this.planner.scope(normalizedQuery);
				// A sorted search over several root tables reads its head page to
				// the end (S1, LIMIT -1), in K order across the tables. The search
				// statement orders by the first table's sort columns, which are
				// NULL in the other tables' rows.
				const inKeyOrder =
					normalizedQuery.sort.length > 0 && this._rootTables.length > 1;
				let sql: string;
				let bindable: BindableValue[];
				if (inKeyOrder) {
					const head = convertSearchRequestToPageQuery(
						normalizedQuery,
						this.tables,
						this._rootTables,
						{ planner: scope, shape },
					);
					sql = head.sql;
					bindable = bindKeyQuery(head.bindable, { limit: Infinity });
				} else {
					({ sql, bindable } = convertSearchRequestToQuery(
						normalizedQuery,
						this.tables,
						this._rootTables,
						{ planner: scope, shape, fetchAll: true },
					));
				}
				await scope.beforePrepare();
				if (explicitlyClosed) {
					return [];
				}
				if (this.closed) {
					closeAsDone();
					return [];
				}
				const page = await this.withDatabaseBarrier(async () => {
					if (explicitlyClosed) {
						return [];
					}
					if (this.isClosing()) {
						closeAsDone();
						return [];
					}
					this.assertOpen();
					started = true;
					cursorEntry.expire = Date.now() + this.iteratorTimeout;
					const statement = await this.properties.db.prepare(sql, sql);
					if (explicitlyClosed) {
						return [];
					}
					const rows: Record<string, any>[] = await scope.perform(async () =>
						statement.all(bindable),
					);
					if (explicitlyClosed) {
						return [];
					}
					const rootTables = this._rootTables;
					const seen = new Set<string>();
					const found: {
						k: string;
						row: Record<string, any>;
						key: StoredKey;
					}[] = [];
					for (const row of rows) {
						let key: StoredKey;
						if (inKeyOrder) {
							key = {
								t: Number(row[ROOT_ORDINAL_ALIAS]),
								pk: row[PRIMARY_KEY_ALIAS],
							};
						} else {
							const t = rootTables.findIndex(
								(table) =>
									row[getTablePrefixedField(table, this.primaryKeyString)] !=
									null,
							);
							key = {
								t,
								pk: row[
									getTablePrefixedField(rootTables[t], this.primaryKeyString)
								],
							};
						}
						const k = keyOf(key);
						if (seen.has(k)) {
							continue; // one row per key: sorted, the first in K order
						}
						seen.add(k);
						found.push({ k, row, key });
					}
					const results = await hydrate(found);
					// Every key is in Y: a later call returns only rows that a
					// write made match.
					snapshot = {
						pk: compactKeys(found.map(({ key }) => key.pk)),
						t:
							rootTables.length > 1
								? compactOrdinals(found.map(({ key }) => key.t))
								: undefined,
					};
					pos = found.length;
					touched.clear();
					dirty.clear();
					extra.clear();
					hasMore = false;
					const out = withoutMarked(results);
					settle(undefined);
					return out;
				});
				// Closed while the page was delivered: discard it.
				return explicitlyClosed ? [] : page;
			} catch (error) {
				if (error === ITERATOR_CLOSED || explicitlyClosed || this.isClosing()) {
					if (!explicitlyClosed) {
						closeAsDone();
					}
					return [];
				}
				throw error;
			}
		};

		// close() releases the state once no call is in flight.
		const releaseState = () => {
			if (reader) {
				this.changeJournal.unregister(reader);
				reader = undefined;
			}
			pagedCursor = undefined;
			counted = undefined;
			snapshot = undefined;
			touched.clear();
			dirty.clear();
			extra.clear();
			pendingMarks.length = 0;
			statements.clear();
			plan = undefined;
		};
		/**
		 * Drops the unread part of the key list and the journal reader, and
		 * keeps Y: the next call lists the keys again.
		 */
		const dropKeyList = () => {
			if (reader) {
				this.changeJournal.unregister(reader);
				reader = undefined;
			}
			if (snapshot && pos < snapshot.pk.length) {
				snapshot = {
					pk: snapshot.pk.slice(0, pos),
					t: snapshot.t?.slice(0, pos),
				};
			}
			dirty.clear();
			needsRebase = true;
		};
		const tracked = async <R>(call: () => Promise<R>): Promise<R> => {
			inFlight++;
			try {
				return await call();
			} finally {
				if (--inFlight === 0) {
					if (explicitlyClosed) {
						releaseState();
					} else if (releaseWhenIdle) {
						releaseWhenIdle = false;
						dropKeyList();
					} else if (reader && !this._cursor.has(requestId)) {
						// It expired while a call ran, and the call failed before it
						// could renew the expiry: keep the reader releasable.
						cursorEntry.expire = Date.now() + this.iteratorTimeout;
						this._cursor.set(requestId, cursorEntry);
					}
				}
			}
		};
		const cursorEntry = {
			expire: Date.now() + this.iteratorTimeout,
			release: () => {
				if (inFlight > 0) {
					// A running call keeps what it reads, and refreshes the expiry.
					// While the index closes, release once it returns.
					if (explicitlyClosed || this.isClosing() || this.closed) {
						releaseWhenIdle = true;
					}
					return;
				}
				if (explicitlyClosed) {
					releaseState();
				} else {
					dropKeyList();
				}
			},
		};
		this.cursor.set(requestId, cursorEntry);

		const next = (amount: number): Promise<Result[]> =>
			tracked(async () => {
				if (explicitlyClosed) {
					return [];
				}
				const limit = amount === Infinity ? Infinity : Math.floor(amount);
				if (!(limit > 0)) {
					return [];
				}
				called = true;
				return admit([] as Result[], (current) => readPage(current, limit));
			});

		return {
			all: () =>
				tracked(async () => {
					if (explicitlyClosed) {
						return [];
					}
					if (!called && pendingMarks.length === 0) {
						called = true;
						return readAllFresh();
					}
					called = true;
					return admit([] as Result[], (current) =>
						readPage(current, Infinity),
					);
				}),
			close: () => {
				explicitlyClosed = true;
				started = true;
				hasMore = false;
				this.clearupIterator(requestId);
				if (inFlight === 0) {
					releaseState();
				}
			},
			next,
			markYielded: (ids: Iterable<types.IdKey>) => {
				if (explicitlyClosed) {
					return;
				}
				for (const id of ids) {
					const value = identity.mark(id.key);
					pendingMarks.push({ k: keyString(value), value });
				}
			},
			pending: () =>
				tracked(async () => {
					if (explicitlyClosed) {
						return 0;
					}
					called = true;
					return admit(0, countPending);
				}),
			done: () => {
				if (explicitlyClosed || this.isClosing()) {
					return true;
				}
				return started ? !hasMore : undefined;
			},
		};
	}

	private async clearupIterator(id: string) {
		const cache = this._cursor.get(id);
		if (!cache) {
			return; // already cleared
		}
		this._cursor.delete(id);
		cache.release();
	}

	async getSize(): Promise<number> {
		return this.ifOpen(0, async () => {
			if (this.tables.size === 0) {
				return 0;
			}

			/* const stmt = await this.properties.db.prepare(`select count(*) as total from ${this.rootTableName}`);
			const result = await stmt.get()
			stmt.finalize?.();
			return result.total as number */
			return this.count();
		});
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
		return this.withKeyMutation(() => this.deleteWithPlanning(query));
	}

	private async deleteWithPlanning(
		query: types.DeleteOptions,
	): Promise<types.IdKey[]> {
		if (this.isClosing()) {
			return [];
		}
		this.assertOpen();

		// Lazy planner DDL takes the connection-level writer admission itself.
		// Resolve it before admitting the DELETE so the planner does not attempt a
		// nested acquisition while this operation already owns that admission.
		const planned: Array<{
			bindable: any[];
			planningScope: ReturnType<QueryPlanner["scope"]>;
			sql: string;
			table: Table;
		}> = [];
		let lastError: Error | undefined = undefined;
		for (const table of this._rootTables) {
			try {
				const planningScope = this.planner.scope(
					new PlannableQuery({
						query: coerceLocalQueries(query.query),
					}),
				);
				const { sql, bindable } = convertDeleteRequestToQuery(
					query,
					this.tables,
					table,
					{
						planner: planningScope,
					},
				);
				await planningScope.beforePrepare();
				planned.push({ bindable, planningScope, sql, table });
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}
				throw error;
			}
		}

		if (planned.length === 0) {
			throw lastError!;
		}
		if (this.closed) {
			return [];
		}

		return this.withDatabaseIfOpen([], async () => {
			const ret: types.IdKey[] = [];
			for (const { bindable, planningScope, sql, table } of planned) {
				const stmt = await this.properties.db.prepare(sql, sql);
				const results: any[] = await planningScope.perform(async () =>
					stmt.all(bindable),
				);
				if (this.changeJournal.recording) {
					// `table` may be from planning before a stop() and start().
					const journalTable = this.journalTableOf(table);
					for (const result of results) {
						this.changeJournal.record(
							journalTable,
							result[table.primary as string],
						);
					}
					// Each DELETE commits on its own, before a later one can fail.
					this.changeJournal.commit();
				}

				// TODO types
				for (const result of results) {
					ret.push(
						types.toId(
							convertFromSQLType(
								result[table.primary as string],
								table.primaryField!.from!.type,
							),
						),
					);
				}
			}

			if (ret.length > 0) {
				this.mutationVersion++;
			}

			return ret;
		});
	}

	async sum(query: types.SumOptions): Promise<number | bigint> {
		if (this.isClosing()) {
			return 0;
		}
		this.assertOpen();

		const planned: Array<{
			bindable: any[];
			planningScope: ReturnType<QueryPlanner["scope"]>;
			sql: string;
		}> = [];
		let lastError: Error | undefined = undefined;
		const inlinedName = getInlineTableFieldName(query.key);
		for (const table of this._rootTables) {
			try {
				if (table.fields.find((x) => x.name === inlinedName) == null) {
					lastError = new MissingFieldError(
						"Missing field: " +
							(Array.isArray(query.key) ? query.key : [query.key]).join("."),
					);
					continue;
				}

				const planningScope = this.planner.scope(
					new PlannableQuery({
						query: coerceLocalQueries(query.query),
					}),
				);
				const { sql, bindable } = convertSumRequestToQuery(
					query,
					this.tables,
					table,
					{
						planner: planningScope,
					},
				);
				await planningScope.beforePrepare();
				planned.push({ bindable, planningScope, sql });
			} catch (error) {
				if (error instanceof MissingFieldError) {
					lastError = error;
					continue;
				}
				throw error;
			}
		}

		if (planned.length === 0) {
			throw lastError!;
		}
		if (this.closed) {
			return 0;
		}

		return this.withDatabaseIfOpen(0, async () => {
			let ret: number | bigint | undefined = undefined;
			let once = false;
			for (const { bindable, planningScope, sql } of planned) {
				const stmt = await this.properties.db.prepare(sql, sql);
				const result = await planningScope.perform(async () =>
					stmt.get(bindable),
				);
				if (result != null) {
					const value = result.sum as number;
					if (ret == null) {
						ret = value;
					} else {
						ret += value;
					}
					once = true;
				}
			}

			if (!once) {
				throw lastError!;
			}
			return ret ?? 0;
		});
	}

	async count(request?: types.CountOptions): Promise<number> {
		return this.withDatabaseIfOpen(0, async () => {
			let ret: number = 0;
			let once = false;
			let lastError: Error | undefined = undefined;
			for (const table of this._rootTables) {
				try {
					const { sql, bindable } = convertCountRequestToQuery(
						request,
						this.tables,
						table,
					);
					const stmt = await this.properties.db.prepare(sql, sql);
					const result = await stmt.get(bindable);
					if (result != null) {
						ret += Number(result.count);
						once = true;
					}
				} catch (error) {
					if (error instanceof MissingFieldError) {
						lastError = error;
						continue;
					}

					throw error;
				}
			}

			if (!once) {
				throw lastError!;
			}
			return ret;
		});
	}

	get cursorCount(): number {
		return this.closed ? 0 : this._cursor.size;
	}
}

export class SQLiteIndices implements types.Indices {
	private _scope: string[];
	private scopes: Map<string, { scope: SQLiteIndices; ready: Promise<void> }>;
	private indices: {
		schema: any;
		index: Index<any, any>;
		ready: Promise<void>;
	}[];
	private state: "closed" | "open" | "closing" = "closed";
	private startPromise?: Promise<void>;
	private shutdownPromise?: Promise<void>;
	private shutdownAction?: "stop" | "drop";
	private readonly pendingAdmissions = new Set<Promise<void>>();

	constructor(
		readonly properties: {
			scope?: string[];
			db: Database;
			parent?: SQLiteIndices;
			directory?: string;
		},
	) {
		this._scope = properties.scope || [];
		this.scopes = new Map();
		this.indices = [];
	}

	private assertAcceptingWork() {
		let owner: SQLiteIndices | undefined = this;
		while (owner) {
			if (owner.state === "closing") {
				throw new types.NotStartedError();
			}
			owner = owner.properties.parent;
		}
	}

	private trackAdmission<T>(operation: Promise<T>): Promise<T> {
		const settled = operation.then(
			() => undefined,
			() => undefined,
		);
		this.pendingAdmissions.add(settled);
		void settled.then(() => this.pendingAdmissions.delete(settled));
		return operation;
	}

	private async drainAdmissions(): Promise<void> {
		while (this.pendingAdmissions.size > 0) {
			await Promise.all([...this.pendingAdmissions]);
		}
	}

	private snapshotStartIntent(): {
		shouldStart: boolean;
		startPromises: Promise<void>[];
	} {
		const startPromises: Promise<void>[] = [];
		let owner: SQLiteIndices | undefined = this;
		while (owner) {
			if (owner.startPromise) {
				startPromises.push(owner.startPromise);
			}
			owner = owner.properties.parent;
		}
		return {
			shouldStart: this.state === "open" || startPromises.length > 0,
			startPromises,
		};
	}

	private async activateIndex(
		index: Index<any, any>,
		startPromises: Promise<void>[],
	): Promise<void> {
		if (startPromises.length > 0) {
			await Promise.all(startPromises);
		}
		await index.start();
	}

	private async activateScope(
		scope: SQLiteIndices,
		startPromises: Promise<void>[],
	): Promise<void> {
		if (startPromises.length > 0) {
			await Promise.all(startPromises);
		}
		await scope.start();
	}

	async init<T extends Record<string, any>, NestedType>(
		properties: IndexEngineInitProperties<T, NestedType>,
	): Promise<Index<T, NestedType>> {
		this.assertAcceptingWork();
		const existing = this.indices.find((x) => x.schema === properties.schema);
		if (existing) {
			const { shouldStart, startPromises } = this.snapshotStartIntent();
			const ready = this.trackAdmission(
				existing.ready.then(() =>
					shouldStart
						? this.activateIndex(existing.index, startPromises)
						: undefined,
				),
			);
			existing.ready = ready;
			try {
				await ready;
			} catch (error) {
				if (existing.ready === ready) {
					existing.ready = Promise.resolve();
				}
				throw error;
			}
			return existing.index as Index<T, NestedType>;
		}

		const index: types.Index<T, any> = new SQLiteIndex({
			db: this.properties.db,
			schema: properties.schema,
			scope: this._scope,
			persisted: this.persisted(),
		});
		index.init(properties);

		const entry = {
			schema: properties.schema,
			index,
			ready: Promise.resolve(),
		};
		this.indices.push(entry);
		const { shouldStart, startPromises } = this.snapshotStartIntent();
		const ready = this.trackAdmission(
			shouldStart
				? this.activateIndex(index, startPromises)
				: Promise.resolve(),
		);
		entry.ready = ready;
		try {
			await ready;
		} catch (error) {
			const position = this.indices.indexOf(entry);
			if (position !== -1) {
				this.indices.splice(position, 1);
			}
			throw error;
		}
		return index;
	}

	async scope(name: string): Promise<types.Indices> {
		this.assertAcceptingWork();
		const existing = this.scopes.get(name);
		if (!existing) {
			const scope = new SQLiteIndices({
				scope: [...this._scope, name],
				db: this.properties.db,
				parent: this,
				directory: this.properties.directory,
			});
			const entry = { scope, ready: Promise.resolve() };
			this.scopes.set(name, entry);
			const { shouldStart, startPromises } = this.snapshotStartIntent();
			entry.ready = this.trackAdmission(
				shouldStart
					? this.activateScope(scope, startPromises)
					: Promise.resolve(),
			);
			try {
				await entry.ready;
			} catch (error) {
				if (this.scopes.get(name) === entry) {
					this.scopes.delete(name);
				}
				throw error;
			}
			return scope;
		}

		const { shouldStart, startPromises } = this.snapshotStartIntent();
		await this.trackAdmission(
			existing.ready.then(() =>
				shouldStart
					? this.activateScope(existing.scope, startPromises)
					: undefined,
			),
		);
		return existing.scope;
	}

	persisted(): boolean {
		return this.properties.directory != null;
	}

	preservesDataOnStop(): boolean {
		return this.properties.directory != null;
	}

	private async startAll(): Promise<void> {
		if (!this.properties.parent) {
			const status = await this.properties.db.status();
			if (status !== "open") {
				await this.properties.db.open();
			}
		}

		for (const { scope } of this.scopes.values()) {
			await scope.start();
		}

		for (const index of this.indices) {
			await index.index.start();
		}
	}

	start(): Promise<void> {
		try {
			this.assertAcceptingWork();
		} catch (error) {
			return Promise.reject(error);
		}
		if (this.state === "open") {
			return this.startPromise ?? Promise.resolve();
		}

		this.state = "open";
		const operation = this.startAll();
		let tracked!: Promise<void>;
		tracked = operation
			.catch((error) => {
				if (this.state === "open") {
					this.state = "closed";
				}
				throw error;
			})
			.finally(() => {
				if (this.startPromise === tracked) {
					this.startPromise = undefined;
				}
			});
		this.startPromise = tracked;
		return tracked;
	}

	private markClosing(): void {
		this.state = "closing";
	}

	private async stopAll(): Promise<void> {
		await this.startPromise?.catch(() => undefined);
		await this.drainAdmissions();

		for (const { scope } of this.scopes.values()) {
			await scope.stop();
		}

		for (const index of this.indices) {
			await index.index.stop();
		}

		if (!this.properties.parent) {
			await this.properties.db.close();
		}
	}

	private async dropAll(): Promise<void> {
		await this.startPromise?.catch(() => undefined);
		await this.drainAdmissions();

		for (const { scope } of this.scopes.values()) {
			await scope.drop();
		}

		if (!this.properties.parent) {
			for (const index of this.indices) {
				await index.index.stop();
			}
			await this.properties.db.drop();
		} else {
			for (const index of this.indices) {
				await index.index.drop();
			}
		}
		this.scopes.clear();
	}

	private beginShutdown(action: "stop" | "drop"): Promise<void> {
		if (this.shutdownPromise) {
			if (action === "drop" && this.shutdownAction === "stop") {
				// The native adapter can only unlink a persistent database while its
				// handle is open. Do not report a successful drop after stop closes it.
				return Promise.reject(new types.NotStartedError());
			}
			return this.shutdownPromise;
		}

		this.markClosing();
		this.shutdownAction = action;
		const operation = action === "stop" ? this.stopAll() : this.dropAll();
		let tracked!: Promise<void>;
		tracked = operation.finally(() => {
			this.state = "closed";
			if (this.shutdownPromise === tracked) {
				this.shutdownPromise = undefined;
				this.shutdownAction = undefined;
			}
		});
		this.shutdownPromise = tracked;
		return tracked;
	}

	stop(): Promise<void> {
		return this.beginShutdown("stop");
	}

	drop(): Promise<void> {
		return this.beginShutdown("drop");
	}
}

export { SQLiteIndex as SQLLiteIndex };
