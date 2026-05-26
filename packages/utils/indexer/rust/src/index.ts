import {
	BinaryWriter,
	type FieldType,
	FixedArrayKind,
	OptionKind,
	StringType,
	VecKind,
	deserialize,
	getSchemasBottomUp,
	serialize,
} from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";
import {
	type EncodedValue,
	type PersistenceOptions,
	type SnapshotFile,
	createSnapshotFile,
} from "./persistence.js";

export type RustIndexerOptions = {
	persistence?: PersistenceOptions;
	/**
	 * Byte fields always support exact ByteMatch queries. Individual byte facts are
	 * only indexed up to this length to avoid exploding large blob-like fields.
	 * Defaults to 0, which disables per-byte facts while preserving exact byte
	 * matching.
	 */
	byteElementIndexLimit?: number;
};

type NativeRustIndex<T extends Record<string, any>> = {
	configure_schema_ir: (schemaIr: Uint8Array) => [number, number, number];
	put: (key: string, id: types.IdKey, value: T, fields: Uint8Array) => void;
	put_encoded: (
		key: string,
		id: types.IdKey,
		value: T,
		valueBytes: Uint8Array,
		byteElementIndexLimit: number,
	) => void;
	put_encoded_parts?: (
		key: string,
		id: types.IdKey,
		value: T,
		valuePrefixBytes: Uint8Array,
		valueSuffixBytes: Uint8Array,
		byteElementIndexLimit: number,
	) => void;
	validate_encoded_parts?: (
		valuePrefixBytes: Uint8Array,
		valueSuffixBytes: Uint8Array,
		byteElementIndexLimit: number,
	) => void;
	put_encoded_parts_stored?: (
		key: string,
		id: types.IdKey,
		valuePrefixBytes: Uint8Array,
		valueSuffixBytes: Uint8Array,
		byteElementIndexLimit: number,
	) => void;
	put_encoded_parts_batch?: (
		keys: string[],
		ids: types.IdKey[],
		values: T[],
		valuePrefixBytes: Uint8Array[],
		valueSuffixBytes: Uint8Array[],
		byteElementIndexLimit: number,
	) => void;
	validate_encoded_parts_batch?: (
		valuePrefixBytes: Uint8Array[],
		valueSuffixBytes: Uint8Array[],
		byteElementIndexLimit: number,
	) => void;
	put_encoded_parts_stored_batch?: (
		keys: string[],
		ids: types.IdKey[],
		valuePrefixBytes: Uint8Array[],
		valueSuffixBytes: Uint8Array[],
		byteElementIndexLimit: number,
	) => void;
	put_and_delete_matching: (
		key: string,
		id: types.IdKey,
		value: T,
		fields: Uint8Array,
		query: Uint8Array,
	) => Array<[types.IdKey, T]>;
	put_and_delete_keys: (
		key: string,
		id: types.IdKey,
		value: T,
		fields: Uint8Array,
		keys: string[],
	) => Array<[types.IdKey, T]>;
	put_shared_log_coordinate?: (
		key: string,
		id: types.IdKey,
		value: T,
		hashField: number,
		hashNumberField: number,
		gidField: number,
		coordinatesField: number,
		coordinatesArrayField: number,
		wallTimeField: number,
		assignedToRangeBoundaryField: number,
		metaField: number,
		hash: string,
		hashNumber: string,
		gid: string,
		coordinates: string[],
		wallTime: string,
		assignedToRangeBoundary: boolean,
		metaBytes: Uint8Array,
		byteElementIndexLimit: number,
	) => void;
	put_shared_log_coordinate_and_delete_keys?: (
		key: string,
		id: types.IdKey,
		value: T,
		hashField: number,
		hashNumberField: number,
		gidField: number,
		coordinatesField: number,
		coordinatesArrayField: number,
		wallTimeField: number,
		assignedToRangeBoundaryField: number,
		metaField: number,
		hash: string,
		hashNumber: string,
		gid: string,
		coordinates: string[],
		wallTime: string,
		assignedToRangeBoundary: boolean,
		metaBytes: Uint8Array,
		byteElementIndexLimit: number,
		keys: string[],
	) => Array<[types.IdKey, T]>;
	put_shared_log_coordinate_and_delete_keys_void?: (
		key: string,
		id: types.IdKey,
		value: T,
		hashField: number,
		hashNumberField: number,
		gidField: number,
		coordinatesField: number,
		coordinatesArrayField: number,
		wallTimeField: number,
		assignedToRangeBoundaryField: number,
		metaField: number,
		hash: string,
		hashNumber: string,
		gid: string,
		coordinates: string[],
		wallTime: string,
		assignedToRangeBoundary: boolean,
		metaBytes: Uint8Array,
		byteElementIndexLimit: number,
		keys: string[],
	) => void;
	put_shared_log_coordinates_and_delete_keys_void?: (
		keys: string[],
		ids: types.IdKey[],
		values: T[],
		hashField: number,
		hashNumberField: number,
		gidField: number,
		coordinatesField: number,
		coordinatesArrayField: number,
		wallTimeField: number,
		assignedToRangeBoundaryField: number,
		metaField: number,
		hashes: string[],
		hashNumbers: string[],
		gids: string[],
		coordinates: string[][],
		wallTimes: string[],
		assignedToRangeBoundaries: Uint8Array,
		metaBytes: Uint8Array[],
		byteElementIndexLimit: number,
		deleteKeys: string[][],
	) => void;
	put_shared_log_coordinate_encoded_and_delete_keys_void?: (
		key: string,
		id: types.IdKey,
		valueBytes: Uint8Array,
		hashField: number,
		hashNumberField: number,
		gidField: number,
		coordinatesField: number,
		coordinatesArrayField: number,
		wallTimeField: number,
		assignedToRangeBoundaryField: number,
		metaField: number,
		hash: string,
		hashNumber: string,
		gid: string,
		coordinates: string[],
		wallTime: string,
		assignedToRangeBoundary: boolean,
		metaBytes: Uint8Array,
		byteElementIndexLimit: number,
		keys: string[],
	) => void;
	get: (key: string) => [types.IdKey, T] | undefined;
	clear: () => void;
	len: () => number;
	entries: () => Array<[types.IdKey, T]>;
	query: (query: Uint8Array, sort: Uint8Array) => Array<[types.IdKey, T]>;
	query_page: (
		query: Uint8Array,
		sort: Uint8Array,
		offset: number,
		limit: number,
	) => Array<[types.IdKey, T]>;
	query_exact_string_first_batch?: (
		field: number,
		values: string[],
	) => Array<[types.IdKey, T] | undefined>;
	count: (query: Uint8Array) => number;
	sum: (query: Uint8Array, field: number) => [NativeSumKind, string];
	delete_matching: (query: Uint8Array) => Array<[types.IdKey, T]>;
	delete_keys: (keys: string[]) => Array<[types.IdKey, T]>;
	delete_keys_void?: (keys: string[]) => void;
	delete_keys_count?: (keys: string[]) => number;
};

type NativeBackboneDocumentIndexTarget = {
	readonly documentIndexLength?: number;
	configureDocumentSchemaIr?: (schemaIr: Uint8Array) => {
		rootFields: number;
		nodeCount: number;
		genericNodes: number;
	};
	setDocumentContextHeadField?: (field: number) => void;
	setDocumentContextFields?: (fields: {
		created: number;
		modified: number;
		head: number;
		gid: number;
		size: number;
	}) => void;
	documentExactStringFirstKey?: (
		field: number,
		value: string,
	) => string | undefined;
	documentContext?: (
		key: string,
	) => [string, string, string, string, number] | undefined;
	documentContextBatch?: (
		keys: string[],
	) => Array<[string, string, string, string, number] | undefined>;
	documentValueBytes?: (key: string) => Uint8Array | undefined;
	documentEntry?: (key: string) => [string, Uint8Array] | undefined;
	documentQuery?: (
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
	) => Array<[string, Uint8Array]>;
	documentQueryPage?: (
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
		offset: number,
		limit: number,
	) => Array<[string, Uint8Array]>;
	documentCount?: (queryBytes: Uint8Array) => number;
	documentSum?: (
		queryBytes: Uint8Array,
		field: number,
	) => [NativeSumKind, string];
	putDocumentEncodedPartsStored?: (
		key: string,
		valuePrefixBytes: Uint8Array,
		valueSuffixBytes: Uint8Array,
		byteElementIndexLimit?: number,
	) => void;
	putDocumentEncodedPartsStoredBatch?: (
		values: Array<{
			key: string;
			valuePrefixBytes: Uint8Array;
			valueSuffixBytes: Uint8Array;
		}>,
		byteElementIndexLimit?: number,
	) => void;
	deleteDocument?: (key: string) => boolean;
	clearDocumentIndex?: () => void;
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeRustIndex: new <T extends Record<string, any>>() => NativeRustIndex<T>;
};

type NativeValue =
	| { type: "bool"; value: boolean }
	| { type: "i64"; value: number | bigint }
	| { type: "u64"; value: number | bigint }
	| { type: "string"; value: string }
	| { type: "bytes"; value: Uint8Array };
type NativeIntegerValue = Extract<
	NativeValue,
	{ type: "i64" } | { type: "u64" }
>;

type NativeFieldCursor = {
	field: string;
	fieldId: number;
	arrayFieldId: number;
};

type NativeQuerySpec =
	| { op: "all" }
	| { op: "exact"; field: number; value: NativeValue }
	| {
			op: "range";
			field: number;
			compare: "eq" | "gt" | "gte" | "lt" | "lte";
			value: NativeValue;
	  }
	| { op: "and"; queries: NativeQuerySpec[] }
	| { op: "or"; queries: NativeQuerySpec[] }
	| { op: "not"; query: NativeQuerySpec }
	| {
			op: "string";
			field: number;
			value: string;
			method: "exact" | "prefix" | "contains";
			caseInsensitive: boolean;
	  }
	| { op: "is_null"; field: number };

type NativeQueryCompileResult = {
	spec: NativeQuerySpec;
	exact: boolean;
};

type NativeSortField = {
	field: number;
	direction: "asc" | "desc";
};

type NativeSumKind = "none" | "i64" | "u64";

type NativeCandidatePage = {
	compiled: NativeQueryCompileResult;
	sort: NativeSortField[];
	offset: number;
	limit?: number;
};

const BRIDGE_VERSION = 1;
const DEFAULT_JOURNAL_COMPACT_AFTER_OPERATIONS = 64 * 1024;
const DEFAULT_BYTE_ELEMENT_INDEX_LIMIT = 0;
const MAX_NATIVE_BYTE_ELEMENT_INDEX_LIMIT = 0xffffffff;

// Keep bridge enum tags in sync with the Rust Borsh DTO declaration order.
const enum NativeValueTag {
	Bool = 0,
	I64 = 1,
	U64 = 2,
	String = 3,
	Bytes = 4,
}

const enum NativeQueryTag {
	All = 0,
	Exact = 1,
	Range = 2,
	And = 3,
	Or = 4,
	Not = 5,
	StringMatch = 6,
	IsNull = 7,
}

const enum NativeCompareTag {
	Equal = 0,
	Greater = 1,
	GreaterOrEqual = 2,
	Less = 3,
	LessOrEqual = 4,
}

const enum NativeStringMatchMethodTag {
	Exact = 0,
	Prefix = 1,
	Contains = 2,
}

const enum NativeSortDirectionTag {
	Asc = 0,
	Desc = 1,
}

const enum NativeSchemaNodeTag {
	Bool = 0,
	U8 = 1,
	U16 = 2,
	U32 = 3,
	U64 = 4,
	U128 = 5,
	U256 = 6,
	U512 = 7,
	I8 = 8,
	I16 = 9,
	I32 = 10,
	I64 = 11,
	String = 12,
	Uint8Array = 13,
	Object = 14,
	Option = 15,
	Vec = 16,
	FixedArray = 17,
	Generic = 18,
}

const textEncoder = new TextEncoder();

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/indexer_rust.js";
		wasmModulePromise = import(
			/* @vite-ignore */ wasmModulePath
		) as Promise<WasmModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		const processLike = (
			globalThis as { process?: { versions?: { node?: string } } }
		).process;
		if (processLike?.versions?.node) {
			const fsPromises = "fs/promises";
			const { readFile } = (await import(
				/* @vite-ignore */ fsPromises
			)) as typeof import("fs/promises");
			const bytes = await readFile(
				new URL("../wasm/indexer_rust_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL(
					"../wasm/indexer_rust_bg.wasm",
					import.meta.url,
				),
			});
		}
		wasmInitialized = true;
	}

	return wasm;
};

const keyToStoreKey = (id: types.IdKey | types.Ideable): string => {
	const idKey = id instanceof types.IdKey ? id : types.toId(id);
	const key = types.toIdeable(idKey);
	if (key instanceof Uint8Array || ArrayBuffer.isView(key)) {
		return `bytes:${idKey.primitive.toString()}`;
	}
	return `${typeof key}:${key.toString()}`;
};

const storeKeyToIdKey = (key: string): types.IdKey | undefined => {
	const separator = key.indexOf(":");
	if (separator <= 0) {
		return;
	}
	const type = key.slice(0, separator);
	const value = key.slice(separator + 1);
	if (type === "string") {
		return types.toId(value);
	}
	if (type === "number") {
		const number = Number(value);
		return Number.isSafeInteger(number) ? types.toId(number) : undefined;
	}
	if (type === "bigint") {
		try {
			return types.toId(BigInt(value));
		} catch {
			return;
		}
	}
	return;
};

const stringToStoreKey = (key: string): string => `string:${key}`;

const nativeFieldKey = (path: string[]): string => JSON.stringify(path);

const nativeArrayElementFieldKey = (path: string[]): string =>
	JSON.stringify(["\u0000array", ...path]);

const appendNativeFieldKey = (
	parent: string | undefined,
	key: string,
): string => {
	const encoded = JSON.stringify(key);
	return parent ? `${parent.slice(0, -1)},${encoded}]` : `[${encoded}]`;
};

const nativeArrayElementFieldKeyFromFieldKey = (field: string): string =>
	`[${JSON.stringify("\u0000array")},${field.slice(1)}`;

type NativeFieldDictionary = {
	id: (field: string) => number;
};

const nativeFieldHash = (field: string): number => {
	let hash = 0x811c9dc5;
	for (let i = 0; i < field.length; i++) {
		hash = Math.imul(hash ^ field.charCodeAt(i), 0x01000193);
	}
	return hash >>> 0;
};

const createNativeFieldDictionary = (): NativeFieldDictionary => {
	const ids = new Map<string, number>();
	const fields = new Map<number, string>();
	return {
		id: (field: string) => {
			let id = ids.get(field);
			if (id == null) {
				id = nativeFieldHash(field);
				const existing = fields.get(id);
				if (existing != null && existing !== field) {
					throw new Error(
						`Native Rust indexer field id collision between ${existing} and ${field}`,
					);
				}
				ids.set(field, id);
				fields.set(id, field);
			}
			return id;
		},
	};
};

const nativeFieldCursor = (
	dictionary: NativeFieldDictionary,
	field: string,
): NativeFieldCursor => ({
	field,
	fieldId: dictionary.id(field),
	arrayFieldId: dictionary.id(nativeArrayElementFieldKeyFromFieldKey(field)),
});

const appendNativeFieldCursor = (
	dictionary: NativeFieldDictionary,
	parent: NativeFieldCursor | undefined,
	key: string,
): NativeFieldCursor =>
	nativeFieldCursor(dictionary, appendNativeFieldKey(parent?.field, key));

const nativeIntegerValue = (
	value: number | bigint,
): NativeIntegerValue | undefined => {
	if (typeof value === "bigint") {
		if (value >= 0n && value <= 18446744073709551615n) {
			return { type: "u64", value };
		}
		if (value >= -9223372036854775808n && value <= 9223372036854775807n) {
			return { type: "i64", value };
		}
		return undefined;
	}
	if (!Number.isSafeInteger(value)) {
		return undefined;
	}
	return value >= 0 ? { type: "u64", value } : { type: "i64", value };
};

type NativeFactCollectorState = {
	seen?: WeakSet<object>;
	nextScope: number;
};

const markNativeFieldSeen = (
	state: NativeFactCollectorState,
	value: object,
): boolean => {
	const seen = (state.seen ??= new WeakSet());
	if (seen.has(value)) {
		return false;
	}
	seen.add(value);
	return true;
};

const compareToNative = (
	compare: types.Compare,
): "eq" | "gt" | "gte" | "lt" | "lte" => {
	switch (compare) {
		case types.Compare.Equal:
			return "eq";
		case types.Compare.Greater:
			return "gt";
		case types.Compare.GreaterOrEqual:
			return "gte";
		case types.Compare.Less:
			return "lt";
		case types.Compare.LessOrEqual:
			return "lte";
		default:
			throw new Error("Unexpected compare");
	}
};

const nativeCompareTag = (compare: "eq" | "gt" | "gte" | "lt" | "lte") => {
	switch (compare) {
		case "eq":
			return NativeCompareTag.Equal;
		case "gt":
			return NativeCompareTag.Greater;
		case "gte":
			return NativeCompareTag.GreaterOrEqual;
		case "lt":
			return NativeCompareTag.Less;
		case "lte":
			return NativeCompareTag.LessOrEqual;
	}
};

const nativeStringMatchMethodTag = (
	method: "exact" | "prefix" | "contains",
) => {
	switch (method) {
		case "exact":
			return NativeStringMatchMethodTag.Exact;
		case "prefix":
			return NativeStringMatchMethodTag.Prefix;
		case "contains":
			return NativeStringMatchMethodTag.Contains;
	}
};

const nativeSortDirectionTag = (direction: "asc" | "desc") => {
	switch (direction) {
		case "asc":
			return NativeSortDirectionTag.Asc;
		case "desc":
			return NativeSortDirectionTag.Desc;
	}
};

const writeNativeValue = (writer: BinaryWriter, value: NativeValue): void => {
	switch (value.type) {
		case "bool":
			writer.u8(NativeValueTag.Bool);
			writer.bool(value.value);
			return;
		case "i64":
			writer.u8(NativeValueTag.I64);
			writer.u64(BigInt.asUintN(64, BigInt(value.value)));
			return;
		case "u64":
			writer.u8(NativeValueTag.U64);
			writer.u64(BigInt(value.value));
			return;
		case "string":
			writer.u8(NativeValueTag.String);
			writer.string(value.value);
			return;
		case "bytes":
			writer.u8(NativeValueTag.Bytes);
			writer.u32(value.value.byteLength);
			for (const byte of value.value) {
				writer.u8(byte);
			}
			return;
	}
};

const writeNativeQuerySpec = (
	writer: BinaryWriter,
	query: NativeQuerySpec,
): void => {
	switch (query.op) {
		case "all":
			writer.u8(NativeQueryTag.All);
			return;
		case "exact":
			writer.u8(NativeQueryTag.Exact);
			writer.u32(query.field);
			writeNativeValue(writer, query.value);
			return;
		case "range":
			writer.u8(NativeQueryTag.Range);
			writer.u32(query.field);
			writer.u8(nativeCompareTag(query.compare));
			writeNativeValue(writer, query.value);
			return;
		case "and":
			writer.u8(NativeQueryTag.And);
			writer.u32(query.queries.length);
			for (const child of query.queries) {
				writeNativeQuerySpec(writer, child);
			}
			return;
		case "or":
			writer.u8(NativeQueryTag.Or);
			writer.u32(query.queries.length);
			for (const child of query.queries) {
				writeNativeQuerySpec(writer, child);
			}
			return;
		case "not":
			writer.u8(NativeQueryTag.Not);
			writeNativeQuerySpec(writer, query.query);
			return;
		case "string":
			writer.u8(NativeQueryTag.StringMatch);
			writer.u32(query.field);
			writer.string(query.value);
			writer.u8(nativeStringMatchMethodTag(query.method));
			writer.bool(query.caseInsensitive);
			return;
		case "is_null":
			writer.u8(NativeQueryTag.IsNull);
			writer.u32(query.field);
			return;
	}
};

const encodeNativeQuerySpec = (query: NativeQuerySpec): Uint8Array => {
	const writer = new BinaryWriter();
	writer.u8(BRIDGE_VERSION);
	writeNativeQuerySpec(writer, query);
	return writer.finalize();
};

const nativeFieldId = (
	dictionary: NativeFieldDictionary,
	path: string[],
): number => dictionary.id(nativeFieldKey(path));

const nativeArrayElementFieldId = (
	dictionary: NativeFieldDictionary,
	path: string[],
): number => dictionary.id(nativeArrayElementFieldKey(path));

const nativeSortFields = (
	dictionary: NativeFieldDictionary,
	sort?: types.Sort | types.Sort[],
): NativeSortField[] =>
	types.toSort(sort).map((field) => ({
		field: nativeFieldId(dictionary, field.key),
		direction: field.direction === types.SortDirection.ASC ? "asc" : "desc",
	}));

const encodeNativeSort = (sort: NativeSortField[] = []): Uint8Array => {
	const writer = new BinaryWriter();
	writer.u8(BRIDGE_VERSION);
	writer.u32(sort.length);
	for (const field of sort) {
		writer.u32(field.field);
		writer.u8(nativeSortDirectionTag(field.direction));
	}
	return writer.finalize();
};

const writeUint32 = (view: DataView, offset: number, value: number): number => {
	view.setUint32(offset, value, true);
	return offset + 4;
};

const writeUint64 = (
	view: DataView,
	offset: number,
	value: number | bigint,
): number => {
	view.setBigUint64(offset, BigInt(value), true);
	return offset + 8;
};

const writeBytes = (
	output: Uint8Array,
	view: DataView,
	offset: number,
	bytes: Uint8Array,
): number => {
	offset = writeUint32(view, offset, bytes.byteLength);
	output.set(bytes, offset);
	return offset + bytes.byteLength;
};

class NativeFieldWriter {
	private output: Uint8Array;
	private view: DataView;
	private offset = 5;
	private facts = 0;

	constructor(initialSize = 256) {
		this.output = new Uint8Array(initialSize);
		this.view = new DataView(this.output.buffer);
		this.output[0] = BRIDGE_VERSION;
	}

	writeBool(scope: number, fieldId: number, value: boolean): void {
		this.writeHeader(scope, fieldId, NativeValueTag.Bool, 1);
		this.output[this.offset++] = value ? 1 : 0;
	}

	writeI64(scope: number, fieldId: number, value: number | bigint): void {
		this.writeHeader(scope, fieldId, NativeValueTag.I64, 8);
		this.offset = writeUint64(
			this.view,
			this.offset,
			BigInt.asUintN(64, BigInt(value)),
		);
	}

	writeU64(scope: number, fieldId: number, value: number | bigint): void {
		this.writeHeader(scope, fieldId, NativeValueTag.U64, 8);
		this.offset = writeUint64(this.view, this.offset, value);
	}

	writeString(scope: number, fieldId: number, value: string): void {
		this.writeStringBytes(scope, fieldId, textEncoder.encode(value));
	}

	writeStringBytes(
		scope: number,
		fieldId: number,
		valueBytes: Uint8Array,
	): void {
		this.writeHeader(
			scope,
			fieldId,
			NativeValueTag.String,
			4 + valueBytes.byteLength,
		);
		this.offset = writeBytes(this.output, this.view, this.offset, valueBytes);
	}

	writeBytesValue(
		scope: number,
		fieldId: number,
		valueBytes: Uint8Array,
	): void {
		this.writeHeader(
			scope,
			fieldId,
			NativeValueTag.Bytes,
			4 + valueBytes.byteLength,
		);
		this.offset = writeBytes(this.output, this.view, this.offset, valueBytes);
	}

	finish(): Uint8Array {
		this.view.setUint32(1, this.facts, true);
		return this.output.subarray(0, this.offset);
	}

	private writeHeader(
		scope: number,
		fieldId: number,
		tag: NativeValueTag,
		valueSize: number,
	): void {
		this.ensure(4 + 4 + 1 + valueSize);
		this.offset = writeUint32(this.view, this.offset, scope);
		this.offset = writeUint32(this.view, this.offset, fieldId);
		this.output[this.offset++] = tag;
		this.facts++;
	}

	private ensure(extra: number): void {
		const needed = this.offset + extra;
		if (needed <= this.output.byteLength) {
			return;
		}
		let nextSize = this.output.byteLength;
		while (nextSize < needed) {
			nextSize *= 2;
		}
		const next = new Uint8Array(nextSize);
		next.set(this.output);
		this.output = next;
		this.view = new DataView(this.output.buffer);
	}
}

class NativeSchemaIrWriter {
	private output: Uint8Array;
	private view: DataView;
	private offset = 0;

	constructor(initialSize = 256) {
		this.output = new Uint8Array(initialSize);
		this.view = new DataView(this.output.buffer);
	}

	u8(value: number): void {
		this.ensure(1);
		this.output[this.offset++] = value;
	}

	u32(value: number): void {
		this.ensure(4);
		this.offset = writeUint32(this.view, this.offset, value);
	}

	string(value: string): void {
		const bytes = textEncoder.encode(value);
		this.u32(bytes.byteLength);
		this.raw(bytes);
	}

	raw(bytes: Uint8Array): void {
		this.ensure(bytes.byteLength);
		this.output.set(bytes, this.offset);
		this.offset += bytes.byteLength;
	}

	finish(): Uint8Array {
		return this.output.subarray(0, this.offset);
	}

	private ensure(extra: number): void {
		const needed = this.offset + extra;
		if (needed <= this.output.byteLength) {
			return;
		}
		let nextSize = this.output.byteLength;
		while (nextSize < needed) {
			nextSize *= 2;
		}
		const next = new Uint8Array(nextSize);
		next.set(this.output);
		this.output = next;
		this.view = new DataView(this.output.buffer);
	}
}

type NativeFieldEncoder<T extends Record<string, any>> = (
	value: T,
) => Uint8Array;
type NativeSchemaIrStats = {
	rootFields: number;
	nodeCount: number;
	genericNodes: number;
};
type SharedLogCoordinateNativeFields = {
	hash: string;
	hashNumber: number | bigint;
	hashNumberString?: string;
	gid: string;
	coordinates: Array<number | bigint>;
	coordinateStrings?: string[];
	wallTime: number | bigint;
	wallTimeString?: string;
	assignedToRangeBoundary: boolean;
	metaBytes: Uint8Array;
};

const sharedLogCoordinateTextEncoder = new TextEncoder();
const sharedLogCoordinateU32Variant =
	sharedLogCoordinateTextEncoder.encode("entry-u32");
const sharedLogCoordinateU64Variant =
	sharedLogCoordinateTextEncoder.encode("entry-u64");

const sharedLogCoordinateSetU32 = (
	view: DataView,
	offset: number,
	value: number,
): number => {
	view.setUint32(offset, value, true);
	return offset + 4;
};

const sharedLogCoordinateSetU64 = (
	view: DataView,
	offset: number,
	value: number | bigint,
): number => {
	view.setBigUint64(offset, BigInt(value), true);
	return offset + 8;
};

const sharedLogCoordinateWriteBytes = (
	output: Uint8Array,
	view: DataView,
	offset: number,
	bytes: Uint8Array,
): number => {
	offset = sharedLogCoordinateSetU32(view, offset, bytes.byteLength);
	output.set(bytes, offset);
	return offset + bytes.byteLength;
};

const encodeSharedLogCoordinateValue = (
	fields: SharedLogCoordinateNativeFields,
	useU64: boolean,
): Uint8Array => {
	const variantBytes = useU64
		? sharedLogCoordinateU64Variant
		: sharedLogCoordinateU32Variant;
	const hashBytes = sharedLogCoordinateTextEncoder.encode(fields.hash);
	const gidBytes = sharedLogCoordinateTextEncoder.encode(fields.gid);
	const numberBytes = useU64 ? 8 : 4;
	const output = new Uint8Array(
		4 +
			variantBytes.byteLength +
			4 +
			hashBytes.byteLength +
			numberBytes +
			4 +
			gidBytes.byteLength +
			4 +
			numberBytes * fields.coordinates.length +
			8 +
			1 +
			4 +
			fields.metaBytes.byteLength,
	);
	const view = new DataView(
		output.buffer,
		output.byteOffset,
		output.byteLength,
	);
	let offset = 0;
	offset = sharedLogCoordinateWriteBytes(output, view, offset, variantBytes);
	offset = sharedLogCoordinateWriteBytes(output, view, offset, hashBytes);
	if (useU64) {
		offset = sharedLogCoordinateSetU64(view, offset, fields.hashNumber);
	} else {
		offset = sharedLogCoordinateSetU32(view, offset, Number(fields.hashNumber));
	}
	offset = sharedLogCoordinateWriteBytes(output, view, offset, gidBytes);
	offset = sharedLogCoordinateSetU32(view, offset, fields.coordinates.length);
	for (const coordinate of fields.coordinates) {
		if (useU64) {
			offset = sharedLogCoordinateSetU64(view, offset, coordinate);
		} else {
			offset = sharedLogCoordinateSetU32(view, offset, Number(coordinate));
		}
	}
	offset = sharedLogCoordinateSetU64(view, offset, fields.wallTime);
	output[offset++] = fields.assignedToRangeBoundary ? 1 : 0;
	offset = sharedLogCoordinateWriteBytes(
		output,
		view,
		offset,
		fields.metaBytes,
	);
	return output;
};

type NativeEncodedValueParts = {
	prefix: Uint8Array;
	suffix: Uint8Array;
};

const EMPTY_NATIVE_ENCODED_SUFFIX = new Uint8Array(0);

type NativeEncodedPutOptions = {
	replace?: boolean;
	encodedValue?: Uint8Array;
	encodedValueParts?: NativeEncodedValueParts;
};
type NativeFieldValueWriterFn = (
	value: any,
	writer: NativeFieldWriter,
	state: NativeFactCollectorState,
	scope: number,
) => void;
type NativeFieldValueWriter = NativeFieldValueWriterFn & { needsSeen: boolean };

const nativeFieldValueWriter = (
	write: NativeFieldValueWriterFn,
	needsSeen = false,
): NativeFieldValueWriter => Object.assign(write, { needsSeen });

const writeNativeIntegerFact = (
	writer: NativeFieldWriter,
	scope: number,
	fieldId: number,
	value: number | bigint,
): void => {
	const nativeValue = nativeIntegerValue(value);
	if (!nativeValue) {
		return;
	}
	if (nativeValue.type === "u64") {
		writer.writeU64(scope, fieldId, nativeValue.value);
	} else {
		writer.writeI64(scope, fieldId, nativeValue.value);
	}
};

const writeNativeScalarFact = (
	writer: NativeFieldWriter,
	scope: number,
	fieldId: number,
	value: any,
): void => {
	if (typeof value === "boolean") {
		writer.writeBool(scope, fieldId, value);
		return;
	}
	if (typeof value === "string") {
		writer.writeString(scope, fieldId, value);
		return;
	}
	if (typeof value === "number" || typeof value === "bigint") {
		writeNativeIntegerFact(writer, scope, fieldId, value);
	}
};

const writeNativeBytesFacts = (
	writer: NativeFieldWriter,
	state: NativeFactCollectorState,
	scope: number,
	fieldId: number,
	value: any,
	byteElementIndexLimit: number,
): void => {
	if (!(value instanceof Uint8Array || ArrayBuffer.isView(value))) {
		return;
	}
	const bytes =
		value instanceof Uint8Array
			? value
			: new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
	writer.writeBytesValue(scope, fieldId, bytes);
	if (bytes.byteLength > byteElementIndexLimit) {
		return;
	}
	for (const byte of bytes) {
		writer.writeU64(state.nextScope++, fieldId, byte);
	}
};

const writeNativeFieldsGeneric = (
	value: any,
	cursor: NativeFieldCursor | undefined,
	dictionary: NativeFieldDictionary,
	writer: NativeFieldWriter,
	state: NativeFactCollectorState,
	scope: number,
	byteElementIndexLimit: number,
): void => {
	if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
		if (cursor) {
			writeNativeBytesFacts(
				writer,
				state,
				scope,
				cursor.fieldId,
				value,
				byteElementIndexLimit,
			);
		}
		return;
	}

	if (
		typeof value === "boolean" ||
		typeof value === "string" ||
		typeof value === "number" ||
		typeof value === "bigint"
	) {
		if (cursor) {
			writeNativeScalarFact(writer, scope, cursor.fieldId, value);
		}
		return;
	}

	if (!value || typeof value !== "object") {
		return;
	}
	if (!markNativeFieldSeen(state, value)) {
		return;
	}

	if (Array.isArray(value)) {
		for (const item of value) {
			const itemScope = state.nextScope++;
			if (cursor) {
				writer.writeBool(itemScope, cursor.arrayFieldId, true);
			}
			writeNativeFieldsGeneric(
				item,
				cursor,
				dictionary,
				writer,
				state,
				itemScope,
				byteElementIndexLimit,
			);
		}
		return;
	}

	for (const key in value) {
		if (!Object.prototype.hasOwnProperty.call(value, key)) {
			continue;
		}
		writeNativeFieldsGeneric(
			value[key],
			appendNativeFieldCursor(dictionary, cursor, key),
			dictionary,
			writer,
			state,
			scope,
			byteElementIndexLimit,
		);
	}
};

const encodeNativeFieldsGeneric = <T extends Record<string, any>>(
	value: T,
	dictionary: NativeFieldDictionary,
	byteElementIndexLimit: number,
): Uint8Array => {
	const writer = new NativeFieldWriter();
	writeNativeFieldsGeneric(
		value,
		undefined,
		dictionary,
		writer,
		{ nextScope: 1 },
		0,
		byteElementIndexLimit,
	);
	return writer.finish();
};

const integerFieldTypes = new Set([
	"u8",
	"u16",
	"u32",
	"u64",
	"u128",
	"u256",
	"u512",
	"i8",
	"i16",
	"i32",
	"i64",
]);

const nativeSchemaIntegerNodeTag = (
	fieldType: string,
): NativeSchemaNodeTag | undefined => {
	switch (fieldType) {
		case "u8":
			return NativeSchemaNodeTag.U8;
		case "u16":
			return NativeSchemaNodeTag.U16;
		case "u32":
			return NativeSchemaNodeTag.U32;
		case "u64":
			return NativeSchemaNodeTag.U64;
		case "u128":
			return NativeSchemaNodeTag.U128;
		case "u256":
			return NativeSchemaNodeTag.U256;
		case "u512":
			return NativeSchemaNodeTag.U512;
		case "i8":
			return NativeSchemaNodeTag.I8;
		case "i16":
			return NativeSchemaNodeTag.I16;
		case "i32":
			return NativeSchemaNodeTag.I32;
		case "i64":
			return NativeSchemaNodeTag.I64;
		default:
			return undefined;
	}
};

const encodeNativeSchemaIr = (
	schema: Function,
	dictionary: NativeFieldDictionary,
): Uint8Array => {
	const writer = new NativeSchemaIrWriter();
	writer.u8(BRIDGE_VERSION);
	writeNativeSchemaNode(writer, schema, dictionary, undefined, new Set());
	return writer.finish();
};

const encodeNativeSchemaVariantPrefix = (
	schemas: ReturnType<typeof getSchemasBottomUp>,
): Uint8Array => {
	const writer = new NativeSchemaIrWriter();
	for (const schema of schemas) {
		const variant = schema.variant;
		if (variant == null) {
			continue;
		}
		if (typeof variant === "string") {
			writer.string(variant);
			continue;
		}
		if (typeof variant === "number") {
			writer.u8(variant);
			continue;
		}
		if (Array.isArray(variant)) {
			for (const part of variant) {
				writer.u8(part);
			}
		}
	}
	return writer.finish();
};

const normalizeNativeByteElementIndexLimit = (limit: number): number => {
	if (!Number.isFinite(limit)) {
		return MAX_NATIVE_BYTE_ELEMENT_INDEX_LIMIT;
	}
	return Math.max(
		0,
		Math.min(MAX_NATIVE_BYTE_ELEMENT_INDEX_LIMIT, Math.floor(limit)),
	);
};

const writeNativeSchemaObjectNode = (
	writer: NativeSchemaIrWriter,
	ctor: Function,
	dictionary: NativeFieldDictionary,
	cursor: NativeFieldCursor | undefined,
	active: Set<Function>,
): void => {
	if (active.has(ctor)) {
		writer.u8(NativeSchemaNodeTag.Generic);
		return;
	}
	let schemas: ReturnType<typeof getSchemasBottomUp>;
	try {
		schemas = getSchemasBottomUp(ctor);
	} catch {
		writer.u8(NativeSchemaNodeTag.Generic);
		return;
	}
	if (!schemas?.length) {
		writer.u8(NativeSchemaNodeTag.Generic);
		return;
	}

	active.add(ctor);
	const fields = schemas.flatMap((nextSchema) => nextSchema.fields);
	const variantPrefix = encodeNativeSchemaVariantPrefix(schemas);
	writer.u8(NativeSchemaNodeTag.Object);
	writer.u32(variantPrefix.byteLength);
	writer.raw(variantPrefix);
	writer.u32(fields.length);
	for (const field of fields) {
		const fieldCursor = appendNativeFieldCursor(dictionary, cursor, field.key);
		writer.string(field.key);
		writer.u32(fieldCursor.fieldId);
		writer.u32(fieldCursor.arrayFieldId);
		writeNativeSchemaNode(writer, field.type, dictionary, fieldCursor, active);
	}
	active.delete(ctor);
};

const writeNativeSchemaNode = (
	writer: NativeSchemaIrWriter,
	fieldType: FieldType | Function,
	dictionary: NativeFieldDictionary,
	cursor: NativeFieldCursor | undefined,
	active: Set<Function>,
): void => {
	if (fieldType instanceof OptionKind) {
		writer.u8(NativeSchemaNodeTag.Option);
		writeNativeSchemaNode(
			writer,
			fieldType.elementType,
			dictionary,
			cursor,
			active,
		);
		return;
	}

	if (fieldType instanceof VecKind || fieldType instanceof FixedArrayKind) {
		if (fieldType instanceof FixedArrayKind) {
			writer.u8(NativeSchemaNodeTag.FixedArray);
			writer.u32(fieldType.length);
		} else {
			writer.u8(NativeSchemaNodeTag.Vec);
		}
		writeNativeSchemaNode(
			writer,
			fieldType.elementType,
			dictionary,
			cursor,
			active,
		);
		return;
	}

	if (fieldType === Uint8Array) {
		writer.u8(NativeSchemaNodeTag.Uint8Array);
		return;
	}

	if (fieldType instanceof StringType) {
		writer.u8(NativeSchemaNodeTag.String);
		return;
	}

	if (typeof fieldType === "string") {
		if (fieldType === "bool") {
			writer.u8(NativeSchemaNodeTag.Bool);
			return;
		}
		if (fieldType === "string") {
			writer.u8(NativeSchemaNodeTag.String);
			return;
		}
		writer.u8(
			nativeSchemaIntegerNodeTag(fieldType) ?? NativeSchemaNodeTag.Generic,
		);
		return;
	}

	if (typeof fieldType === "function") {
		writeNativeSchemaObjectNode(writer, fieldType, dictionary, cursor, active);
		return;
	}

	writer.u8(NativeSchemaNodeTag.Generic);
};

const compileNativeFieldEncoder = <T extends Record<string, any>>(
	schema: Function,
	dictionary: NativeFieldDictionary,
	options: RustIndexerOptions,
): NativeFieldEncoder<T> => {
	const byteElementIndexLimit =
		options.byteElementIndexLimit ?? DEFAULT_BYTE_ELEMENT_INDEX_LIMIT;
	const objectWriterCache = new WeakMap<
		Function,
		Map<string, NativeFieldValueWriter | undefined>
	>();

	const getObjectWriter = (
		ctor: Function,
		cursor: NativeFieldCursor | undefined,
	): NativeFieldValueWriter | undefined => {
		let byPrefix = objectWriterCache.get(ctor);
		if (!byPrefix) {
			byPrefix = new Map();
			objectWriterCache.set(ctor, byPrefix);
		}
		const prefix = cursor?.field ?? "";
		if (byPrefix.has(prefix)) {
			return byPrefix.get(prefix);
		}

		// Break recursive schemas by falling back to the generic walker for
		// recursive edges. The common path still gets a fully compiled writer.
		byPrefix.set(prefix, undefined);
		let schemas: ReturnType<typeof getSchemasBottomUp>;
		try {
			schemas = getSchemasBottomUp(ctor);
		} catch {
			return undefined;
		}
		if (!schemas?.length) {
			return undefined;
		}

		const fields: Array<{
			key: string;
			write: NativeFieldValueWriter;
		}> = [];
		for (const nextSchema of schemas) {
			for (const field of nextSchema.fields) {
				fields.push({
					key: field.key,
					write: compileFieldValueWriter(
						field.type,
						appendNativeFieldCursor(dictionary, cursor, field.key),
						getObjectWriter,
						dictionary,
						byteElementIndexLimit,
					),
				});
			}
		}
		const needsSeen = fields.some((field) => field.write.needsSeen);

		const objectWriter = nativeFieldValueWriter(
			(value, writer, state, scope) => {
				if (!value || typeof value !== "object") {
					return;
				}
				if (needsSeen && !markNativeFieldSeen(state, value)) {
					return;
				}
				for (const field of fields) {
					field.write(value[field.key], writer, state, scope);
				}
			},
			needsSeen,
		);
		byPrefix.set(prefix, objectWriter);
		return objectWriter;
	};

	const rootWriter = getObjectWriter(schema, undefined);
	if (!rootWriter) {
		return (value: T) =>
			encodeNativeFieldsGeneric(value, dictionary, byteElementIndexLimit);
	}

	return (value: T) => {
		const writer = new NativeFieldWriter();
		rootWriter(value, writer, { nextScope: 1 }, 0);
		return writer.finish();
	};
};

const compileFieldValueWriter = (
	fieldType: FieldType,
	cursor: NativeFieldCursor,
	getObjectWriter: (
		ctor: Function,
		cursor: NativeFieldCursor | undefined,
	) => NativeFieldValueWriter | undefined,
	dictionary: NativeFieldDictionary,
	byteElementIndexLimit: number,
): NativeFieldValueWriter => {
	if (fieldType instanceof OptionKind) {
		const writeElement = compileFieldValueWriter(
			fieldType.elementType,
			cursor,
			getObjectWriter,
			dictionary,
			byteElementIndexLimit,
		);
		return nativeFieldValueWriter((value, writer, state, scope) => {
			if (value != null) {
				writeElement(value, writer, state, scope);
			}
		}, writeElement.needsSeen);
	}

	if (fieldType instanceof VecKind || fieldType instanceof FixedArrayKind) {
		if (fieldType.elementType === "u8") {
			return nativeFieldValueWriter((value, writer, state, scope) =>
				writeNativeBytesFacts(
					writer,
					state,
					scope,
					cursor.fieldId,
					value,
					byteElementIndexLimit,
				),
			);
		}
		const writeElement = compileFieldValueWriter(
			fieldType.elementType,
			cursor,
			getObjectWriter,
			dictionary,
			byteElementIndexLimit,
		);
		return nativeFieldValueWriter((value, writer, state, scope) => {
			if (!Array.isArray(value)) {
				return;
			}
			if (!markNativeFieldSeen(state, value)) {
				return;
			}
			for (const item of value) {
				const itemScope = state.nextScope++;
				writer.writeBool(itemScope, cursor.arrayFieldId, true);
				writeElement(item, writer, state, itemScope);
			}
		}, true);
	}

	if (fieldType === Uint8Array) {
		return nativeFieldValueWriter((value, writer, state, scope) =>
			writeNativeBytesFacts(
				writer,
				state,
				scope,
				cursor.fieldId,
				value,
				byteElementIndexLimit,
			),
		);
	}

	if (fieldType instanceof StringType) {
		return nativeFieldValueWriter((value, writer, _state, scope) => {
			if (typeof value === "string") {
				writer.writeString(scope, cursor.fieldId, value);
			}
		});
	}

	if (typeof fieldType === "string") {
		if (fieldType === "bool") {
			return nativeFieldValueWriter((value, writer, _state, scope) => {
				if (typeof value === "boolean") {
					writer.writeBool(scope, cursor.fieldId, value);
				}
			});
		}
		if (fieldType === "string") {
			return nativeFieldValueWriter((value, writer, _state, scope) => {
				if (typeof value === "string") {
					writer.writeString(scope, cursor.fieldId, value);
				}
			});
		}
		if (integerFieldTypes.has(fieldType)) {
			return nativeFieldValueWriter((value, writer, _state, scope) => {
				if (typeof value === "number" || typeof value === "bigint") {
					writeNativeIntegerFact(writer, scope, cursor.fieldId, value);
				}
			});
		}
		return nativeFieldValueWriter((value, writer, _state, scope) =>
			writeNativeScalarFact(writer, scope, cursor.fieldId, value),
		);
	}

	if (typeof fieldType === "function") {
		return nativeFieldValueWriter((value, writer, state, scope) => {
			if (!value || typeof value !== "object") {
				return;
			}
			const ctor =
				typeof value.constructor === "function" && value.constructor !== Object
					? value.constructor
					: fieldType;
			const objectWriter =
				getObjectWriter(ctor, cursor) ??
				(ctor === fieldType ? undefined : getObjectWriter(fieldType, cursor));
			if (objectWriter) {
				objectWriter(value, writer, state, scope);
			} else {
				writeNativeFieldsGeneric(
					value,
					cursor,
					dictionary,
					writer,
					state,
					scope,
					byteElementIndexLimit,
				);
			}
		}, true);
	}

	return nativeFieldValueWriter(
		(value, writer, state, scope) =>
			writeNativeFieldsGeneric(
				value,
				cursor,
				dictionary,
				writer,
				state,
				scope,
				byteElementIndexLimit,
			),
		true,
	);
};

const decodeNativeSum = ([kind, value]: [NativeSumKind, string]):
	| number
	| bigint => {
	if (kind === "none") {
		return 0;
	}
	const sum = BigInt(value);
	if (
		sum >= BigInt(Number.MIN_SAFE_INTEGER) &&
		sum <= BigInt(Number.MAX_SAFE_INTEGER)
	) {
		return Number(sum);
	}
	return sum;
};

const describeNativeQueries = (queries: types.Query[]) =>
	queries.map((query) => query.constructor.name).join(", ") || "All";

const compileNativeQueries = (
	queries: types.Query[],
	dictionary: NativeFieldDictionary,
	prefix: string[] = [],
): NativeQueryCompileResult | undefined => {
	return compileNativeAnd(queries, dictionary, prefix);
};

const compileNativeAnd = (
	queries: types.Query[],
	dictionary: NativeFieldDictionary,
	prefix: string[] = [],
): NativeQueryCompileResult | undefined => {
	const compiled: NativeQuerySpec[] = [];

	for (const query of queries) {
		const next = compileNativeQuery(query, dictionary, prefix);
		if (!next) {
			return;
		}
		compiled.push(next.spec);
	}

	if (compiled.length === 0) {
		return { spec: { op: "all" }, exact: true };
	}
	if (compiled.length === 1) {
		return { spec: compiled[0], exact: true };
	}
	return { spec: { op: "and", queries: compiled }, exact: true };
};

const compileNativeQuery = (
	query: types.Query,
	dictionary: NativeFieldDictionary,
	prefix: string[] = [],
): NativeQueryCompileResult | undefined => {
	if (query instanceof types.And) {
		return compileNativeAnd(query.and, dictionary, prefix);
	}
	if (query instanceof types.Or) {
		const compiled: NativeQuerySpec[] = [];
		for (const child of query.or) {
			const next = compileNativeQuery(child, dictionary, prefix);
			if (!next) {
				return;
			}
			compiled.push(next.spec);
		}
		return { spec: { op: "or", queries: compiled }, exact: true };
	}
	if (query instanceof types.Not) {
		const child = compileNativeQuery(query.not, dictionary, prefix);
		if (!child) {
			return;
		}
		return { spec: { op: "not", query: child.spec }, exact: true };
	}
	if (query instanceof types.Nested) {
		const nestedPath = [...prefix, ...query.path];
		const nested = compileNativeAnd(query.query, dictionary, nestedPath);
		if (!nested) {
			return;
		}
		const arrayElementMarker: NativeQuerySpec = {
			op: "exact",
			field: nativeArrayElementFieldId(dictionary, nestedPath),
			value: { type: "bool", value: true },
		};
		if (nested.spec.op === "all") {
			return { spec: arrayElementMarker, exact: true };
		}
		return {
			spec: { op: "and", queries: [arrayElementMarker, nested.spec] },
			exact: true,
		};
	}
	if (query instanceof types.BoolQuery) {
		return {
			spec: {
				op: "exact",
				field: nativeFieldId(dictionary, [...prefix, ...query.key]),
				value: { type: "bool", value: query.value },
			},
			exact: true,
		};
	}
	if (query instanceof types.StringMatch) {
		return {
			spec: {
				op: "string",
				field: nativeFieldId(dictionary, [...prefix, ...query.key]),
				value: query.value,
				method:
					query.method === types.StringMatchMethod.exact
						? "exact"
						: query.method === types.StringMatchMethod.prefix
							? "prefix"
							: "contains",
				caseInsensitive: query.caseInsensitive,
			},
			exact: true,
		};
	}
	if (query instanceof types.ByteMatchQuery) {
		return {
			spec: {
				op: "exact",
				field: nativeFieldId(dictionary, [...prefix, ...query.key]),
				value: { type: "bytes", value: query.value },
			},
			exact: true,
		};
	}
	if (query instanceof types.IntegerCompare) {
		const value = nativeIntegerValue(query.value.value);
		if (!value) {
			return;
		}
		if (query.compare === types.Compare.Equal) {
			return {
				spec: {
					op: "exact",
					field: nativeFieldId(dictionary, [...prefix, ...query.key]),
					value,
				},
				exact: true,
			};
		}
		return {
			spec: {
				op: "range",
				field: nativeFieldId(dictionary, [...prefix, ...query.key]),
				compare: compareToNative(query.compare),
				value,
			},
			exact: true,
		};
	}
	if (query instanceof types.IsNull) {
		if (prefix.length > 0) {
			return;
		}
		return {
			spec: {
				op: "is_null",
				field: nativeFieldId(dictionary, query.key),
			},
			exact: true,
		};
	}
	return;
};

const cloneResults = <T>(
	indexed: types.IndexedValue<T>[],
	schema: any,
): types.IndexedValue<T>[] => {
	return indexed.map((x) => {
		return { id: x.id, value: deserialize(serialize(x.value), schema) };
	});
};

const concatEncodedParts = (
	prefix: Uint8Array,
	suffix: Uint8Array,
): Uint8Array => {
	const encoded = new Uint8Array(prefix.byteLength + suffix.byteLength);
	encoded.set(prefix, 0);
	encoded.set(suffix, prefix.byteLength);
	return encoded;
};

type MaybePromise<T> = Promise<T> | T;

const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> => {
	return value != null && typeof (value as Promise<T>).then === "function";
};

export class RustIndex<T extends Record<string, any>, NestedType = any>
	implements types.Index<T, NestedType>
{
	private snapshotFile?: SnapshotFile;
	private native?: NativeRustIndex<T>;
	private indexByArr!: string[];
	private properties!: types.IndexEngineInitProperties<T, NestedType>;
	private fieldDictionary!: NativeFieldDictionary;
	private fieldEncoder!: NativeFieldEncoder<T>;
	private nativeEncodedValueEncoder?: NativeFieldEncoder<T>;
	private nativeSchemaIrStats?: NativeSchemaIrStats;
	private nativeBackboneDocumentIndex?: NativeBackboneDocumentIndexTarget;
	private nativeBackboneDocumentIndexPrimary = false;
	private byteElementIndexLimit!: number;
	private nativeByteElementIndexLimit!: number;
	private sharedLogCoordinateFieldIds?: {
		hash: number;
		hashNumber: number;
		gid: number;
		coordinates: number;
		coordinatesArray: number;
		wallTime: number;
		assignedToRangeBoundary: number;
		meta: number;
	};
	private persistQueue: Promise<void> = Promise.resolve();
	private mutationQueue: Promise<void> = Promise.resolve();
	private state: "closed" | "open" | "closing" = "closed";

	constructor(
		private readonly directory?: string,
		private readonly path: string[] = [],
		private readonly options: RustIndexerOptions = {},
	) {}

	private closedIterator<
		S extends types.Shape | undefined,
	>(): types.IndexIterator<T, S> {
		return {
			all: async () => [],
			next: async () => [],
			done: () => true,
			pending: async () => 0,
			close: async () => undefined,
		};
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
		this.state = "closing";
	}

	private setClosed() {
		this.state = "closed";
	}

	private setOpen() {
		this.state = "open";
	}

	async init(properties: types.IndexEngineInitProperties<T, NestedType>) {
		this.properties = properties;
		if (properties.indexBy) {
			this.indexByArr = Array.isArray(properties.indexBy)
				? properties.indexBy
				: [properties.indexBy];
		} else {
			const indexBy = types.getIdProperty(properties.schema);

			if (!indexBy) {
				throw new Error(
					"No indexBy property defined nor schema has a property decorated with `id({ type: '...' })`",
				);
			}

			this.indexByArr = indexBy;
		}

		const wasm = await loadWasm();
		this.native = new wasm.NativeRustIndex<T>();
		this.fieldDictionary = createNativeFieldDictionary();
		this.byteElementIndexLimit =
			this.options.byteElementIndexLimit ?? DEFAULT_BYTE_ELEMENT_INDEX_LIMIT;
		this.nativeByteElementIndexLimit = normalizeNativeByteElementIndexLimit(
			this.byteElementIndexLimit,
		);
		this.fieldEncoder = compileNativeFieldEncoder(
			properties.schema,
			this.fieldDictionary,
			this.options,
		);
		const [rootFields, nodeCount, genericNodes] =
			this.native.configure_schema_ir(
				encodeNativeSchemaIr(properties.schema, this.fieldDictionary),
			);
		this.nativeSchemaIrStats = { rootFields, nodeCount, genericNodes };
		if (genericNodes === 0) {
			this.nativeEncodedValueEncoder = (value) => serialize(value);
		}
		this.snapshotFile = await createSnapshotFile(
			this.directory,
			this.path,
			this.indexByArr,
			this.options.persistence,
		);
		if (this.snapshotFile) {
			for (const value of (await this.snapshotFile.read(
				properties.schema,
			)) as T[]) {
				const id = types.toId(types.extractFieldValue(value, this.indexByArr));
				const storeKey = keyToStoreKey(id);
				this.putNativeDocument(storeKey, id, value);
			}
		}
		return this;
	}

	attachNativeBackboneDocumentIndex(
		backbone: NativeBackboneDocumentIndexTarget | undefined,
	): boolean {
		this.nativeBackboneDocumentIndexPrimary = false;
		if (
			!backbone ||
			!this.nativeEncodedValueEncoder ||
			typeof backbone.configureDocumentSchemaIr !== "function" ||
			typeof backbone.putDocumentEncodedPartsStored !== "function"
		) {
			this.nativeBackboneDocumentIndex = undefined;
			return false;
		}
		try {
			backbone.configureDocumentSchemaIr(
				encodeNativeSchemaIr(this.properties.schema, this.fieldDictionary),
			);
			const contextFields = {
				created: nativeFieldId(this.fieldDictionary, ["__context", "created"]),
				modified: nativeFieldId(this.fieldDictionary, [
					"__context",
					"modified",
				]),
				head: nativeFieldId(this.fieldDictionary, ["__context", "head"]),
				gid: nativeFieldId(this.fieldDictionary, ["__context", "gid"]),
				size: nativeFieldId(this.fieldDictionary, ["__context", "size"]),
			};
			backbone.setDocumentContextFields?.(contextFields);
			backbone.setDocumentContextHeadField?.(contextFields.head);
			backbone.clearDocumentIndex?.();
			this.populateNativeBackboneDocumentIndex(backbone);
			this.nativeBackboneDocumentIndex = backbone;
			this.nativeBackboneDocumentIndexPrimary =
				this.canUseNativeBackboneDocumentIndexAsPrimary(backbone);
			return true;
		} catch {
			backbone.clearDocumentIndex?.();
			this.nativeBackboneDocumentIndex = undefined;
			this.nativeBackboneDocumentIndexPrimary = false;
			return false;
		}
	}

	get(
		id: types.IdKey,
		_options?: { shape: types.Shape },
	): types.IndexedResult<T> | undefined {
		if (this.isClosing()) {
			return;
		}
		this.assertOpen();
		const nativeBackboneValue = this.getNativeBackboneDocumentEntry(
			keyToStoreKey(id),
		);
		if (nativeBackboneValue) {
			return nativeBackboneValue;
		}
		if (this.nativeBackboneDocumentIndexPrimary) {
			return;
		}
		const value = this.getNative().get(keyToStoreKey(id));
		if (!value) {
			return;
		}
		return {
			id,
			value: this.decodeNativeStoredValue(value[1]),
		};
	}

	getContextById(
		id: types.IdKey,
	):
		| {
				created: bigint;
				modified: bigint;
				head: string;
				gid: string;
				size: number;
		  }
		| undefined {
		if (this.isClosing()) {
			return;
		}
		this.assertOpen();
		if (!this.nativeBackboneDocumentIndexPrimary) {
			return;
		}
		const row = this.nativeBackboneDocumentIndex?.documentContext?.(
			keyToStoreKey(id),
		);
		if (!row) {
			return;
		}
		return {
			created: BigInt(row[0]),
			modified: BigInt(row[1]),
			head: row[2],
			gid: row[3],
			size: row[4],
		};
	}

	getContextByIdBatch(ids: types.IdKey[]): Array<
		| {
				created: bigint;
				modified: bigint;
				head: string;
				gid: string;
				size: number;
		  }
		| undefined
	> {
		if (ids.length === 0) {
			return [];
		}
		if (this.isClosing()) {
			return ids.map(() => undefined);
		}
		this.assertOpen();
		if (!this.nativeBackboneDocumentIndexPrimary) {
			return ids.map(() => undefined);
		}
		const keys = ids.map((id) => keyToStoreKey(id));
		const rows =
			this.nativeBackboneDocumentIndex?.documentContextBatch?.(keys) ??
			keys.map((key) =>
				this.nativeBackboneDocumentIndex?.documentContext?.(key),
			);
		return rows.map((row) =>
			row
				? {
						created: BigInt(row[0]),
						modified: BigInt(row[1]),
						head: row[2],
						gid: row[3],
						size: row[4],
					}
				: undefined,
		);
	}

	getByContextHead(head: string): types.IndexedResult<T> | undefined {
		const field = nativeFieldId(this.fieldDictionary, ["__context", "head"]);
		const nativeBackboneResult = this.getNativeBackboneExactStringFirst(
			field,
			head,
		);
		if (nativeBackboneResult) {
			return nativeBackboneResult;
		}
		if (this.nativeBackboneDocumentIndexPrimary) {
			return;
		}
		return this.getNativeExactStringFirst(field, head);
	}

	getByContextHeadBatch(
		heads: string[],
	): Array<types.IndexedResult<T> | undefined> {
		if (heads.length === 0) {
			return [];
		}
		const field = nativeFieldId(this.fieldDictionary, ["__context", "head"]);
		const nativeBackbone = this.nativeBackboneDocumentIndex;
		if (
			nativeBackbone?.documentExactStringFirstKey &&
			nativeBackbone.documentValueBytes
		) {
			return heads.map(
				(head) =>
					this.getNativeBackboneExactStringFirst(field, head) ??
					(this.nativeBackboneDocumentIndexPrimary
						? undefined
						: this.getNativeExactStringFirst(field, head)),
			);
		}
		const native = this.getNative();
		const nativeBatch = native.query_exact_string_first_batch;
		if (!nativeBatch) {
			return heads.map((head) => this.getByContextHead(head));
		}
		const rows = nativeBatch.call(native, field, heads);
		return rows.map((result) =>
			result
				? { id: result[0], value: this.decodeNativeStoredValue(result[1]) }
				: undefined,
		);
	}

	getIdByContextHead(head: string): types.IdKey | undefined {
		const field = nativeFieldId(this.fieldDictionary, ["__context", "head"]);
		const backbone = this.nativeBackboneDocumentIndex;
		if (
			backbone?.documentExactStringFirstKey &&
			(backbone.documentValueBytes || this.nativeBackboneDocumentIndexPrimary)
		) {
			const key = backbone.documentExactStringFirstKey(field, head);
			return key ? storeKeyToIdKey(key) : undefined;
		}
		if (this.nativeBackboneDocumentIndexPrimary) {
			return;
		}
		return this.getNativeExactStringFirst(field, head)?.id;
	}

	put(
		value: T,
		id?: types.IdKey,
		_options?: { replace?: boolean },
	): MaybePromise<void> {
		if (this.isClosing()) {
			return;
		}
		this.assertOpen();
		id = id ?? types.toId(types.extractFieldValue(value, this.indexByArr));
		const encodedValue = this.tryNativeEncodedValue(value);
		if (encodedValue) {
			return this.putWithEncodedValue(value, id, encodedValue);
		}
		if (!this.snapshotFile) {
			this.putNativeDocument(keyToStoreKey(id), id, value);
			return;
		}
		return this.putWithEncodedFields(value, id, this.fieldEncoder(value));
	}

	putWithContext(
		value: Record<string, any>,
		id: types.IdKey,
		context: Record<string, any>,
		options?: NativeEncodedPutOptions,
	): MaybePromise<void> {
		if (options?.encodedValueParts) {
			const storedPut = this.putWithEncodedValuePartsStored(
				id,
				options.encodedValueParts,
			);
			if (storedPut !== false) {
				return storedPut;
			}
		}
		const contextualValue = this.asContextualValue(value, context);
		if (options?.encodedValue) {
			return this.putWithEncodedValue(
				contextualValue,
				id,
				options.encodedValue,
			);
		}
		return this.put(contextualValue, id, options);
	}

	async putWithContextBatch(
		values: Array<{
			value: Record<string, any>;
			id: types.IdKey;
			context: Record<string, any>;
			options?: NativeEncodedPutOptions;
		}>,
	): Promise<void> {
		if (values.length === 0) {
			return;
		}
		if (
			values.every((entry) => entry.options?.encodedValueParts) &&
			(await this.putWithEncodedValuePartsStoredBatch(
				values.map((entry) => ({
					id: entry.id,
					encodedValueParts: entry.options!.encodedValueParts!,
				})),
			))
		) {
			return;
		}
		if (values.some((entry) => entry.options?.replace === true)) {
			for (const entry of values) {
				await this.putWithContext(
					entry.value,
					entry.id,
					entry.context,
					entry.options,
				);
			}
			return;
		}
		await this.putPreparedBatch(
			values.map((entry) => ({
				value: this.asContextualValue(entry.value, entry.context),
				id: entry.id,
				encodedValue: entry.options?.encodedValue,
				encodedValueParts: entry.options?.encodedValueParts,
			})),
		);
	}

	async putBatch(values: T[]): Promise<void> {
		if (values.length === 0) {
			return;
		}
		await this.putPreparedBatch(
			values.map((value) => ({
				value,
				id: types.toId(types.extractFieldValue(value, this.indexByArr)),
			})),
		);
	}

	private async putPreparedBatch(
		values: Array<{
			value: T;
			id: types.IdKey;
			encodedValue?: Uint8Array;
			encodedValueParts?: NativeEncodedValueParts;
		}>,
	): Promise<void> {
		if (this.nativeBackboneDocumentIndexPrimary) {
			const prepared = values.map((entry) => ({
				encodedValue:
					entry.encodedValue ??
					(entry.encodedValueParts
						? undefined
						: this.tryNativeEncodedValue(entry.value)),
				encodedValueParts: entry.encodedValueParts,
				storeKey: keyToStoreKey(entry.id),
				value: entry.value,
			}));
			if (
				prepared.some((item) => !item.encodedValue && !item.encodedValueParts)
			) {
				throw new Error("Native backbone document batch value encoding failed");
			}
			if (!this.snapshotFile) {
				for (const item of prepared) {
					this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
						item.storeKey,
						item.encodedValue,
						item.encodedValueParts,
					);
				}
				return;
			}
			await this.enqueueMutation(async () => {
				await this.enqueuePersistence(async () => {
					await this.snapshotFile!.appendPutBatch(
						prepared.map((item) => ({
							key: item.storeKey,
							value:
								item.encodedValue || item.encodedValueParts
									? undefined
									: item.value,
							encodedValue: item.encodedValueParts ?? item.encodedValue,
						})),
						this.properties.schema,
					);
				});
				for (const item of prepared) {
					this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
						item.storeKey,
						item.encodedValue,
						item.encodedValueParts,
					);
				}
				await this.compactIfNeeded();
			});
			return;
		}
		if (!this.snapshotFile) {
			if (this.putNativeDocumentEncodedPartsBatch(values)) {
				return;
			}
			for (const entry of values) {
				const storeKey = keyToStoreKey(entry.id);
				const encodedValue =
					entry.encodedValue ??
					(entry.encodedValueParts
						? undefined
						: this.tryNativeEncodedValue(entry.value));
				if (
					!this.putNativeDocumentWithPreparedFields(
						storeKey,
						entry.id,
						entry.value,
						encodedValue,
						undefined,
						entry.encodedValueParts,
					)
				) {
					this.getNative().put(
						storeKey,
						entry.id,
						entry.value,
						this.fieldEncoder(entry.value),
					);
				}
			}
			return;
		}

		await this.enqueueMutation(async () => {
			const prepared = values.map((entry) => {
				const encodedValue =
					entry.encodedValue ??
					(entry.encodedValueParts
						? undefined
						: this.tryNativeEncodedValue(entry.value));
				return {
					encodedValue,
					encodedValueParts: entry.encodedValueParts,
					fields:
						encodedValue || entry.encodedValueParts
							? undefined
							: this.fieldEncoder(entry.value),
					id: entry.id,
					storeKey: keyToStoreKey(entry.id),
					value: entry.value,
				};
			});
			await this.enqueuePersistence(async () => {
				await this.snapshotFile!.appendPutBatch(
					prepared.map((item) => ({
						key: item.storeKey,
						value: item.value,
						encodedValue: item.encodedValueParts ?? item.encodedValue,
					})),
					this.properties.schema,
				);
			});
			if (this.putNativeDocumentEncodedPartsBatch(prepared)) {
				await this.compactIfNeeded();
				return;
			}
			for (const item of prepared) {
				if (
					!this.putNativeDocumentWithPreparedFields(
						item.storeKey,
						item.id,
						item.value,
						item.encodedValue,
						item.fields,
						item.encodedValueParts,
					)
				) {
					this.getNative().put(
						item.storeKey,
						item.id,
						item.value,
						this.fieldEncoder(item.value),
					);
				}
			}
			await this.compactIfNeeded();
		});
	}

	async putAndDelete(
		value: T,
		deleteOptions: types.DeleteOptions,
		id = types.toId(types.extractFieldValue(value, this.indexByArr)),
	): Promise<types.IdKey[]> {
		const compiled = this.requireNativePlan(
			types.toQuery(deleteOptions.query),
			{
				allowAll: true,
			},
		);
		const storeKey = keyToStoreKey(id);
		const fields = this.fieldEncoder(value);
		const queryBytes = encodeNativeQuerySpec(compiled.spec);
		if (this.nativeBackboneDocumentIndexPrimary) {
			const storedValue = this.tryNativeEncodedValue(value);
			if (!storedValue) {
				throw new Error("Native backbone document value encoding failed");
			}
			if (!this.snapshotFile) {
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					storedValue,
				);
				const deletedEntries = this.getNativeCandidatesForPlan({
					compiled,
					sort: [],
					offset: 0,
				});
				this.deleteNativeBackboneDocumentKeys(
					deletedEntries.map((entry) => keyToStoreKey(entry.id)),
				);
				return deletedEntries.map((entry) => entry.id);
			}

			return this.enqueueMutation(async () => {
				await this.appendPut(storeKey, undefined, storedValue);
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					storedValue,
				);
				const deletedEntries = this.getNativeCandidatesForPlan({
					compiled,
					sort: [],
					offset: 0,
				});
				if (deletedEntries.length > 0) {
					await this.appendDeletes(
						deletedEntries.map((entry) => keyToStoreKey(entry.id)),
					);
					this.deleteNativeBackboneDocumentKeys(
						deletedEntries.map((entry) => keyToStoreKey(entry.id)),
					);
				}
				await this.compactIfNeeded();
				return deletedEntries.map((entry) => entry.id);
			});
		}
		if (!this.snapshotFile) {
			return this.getNative()
				.put_and_delete_matching(storeKey, id, value, fields, queryBytes)
				.map((entry) => entry[0]);
		}

		return this.enqueueMutation(async () => {
			await this.appendPut(storeKey, value);
			this.getNative().put(storeKey, id, value, fields);
			const deletedEntries = this.getNativeCandidatesForPlan({
				compiled,
				sort: [],
				offset: 0,
			});
			if (deletedEntries.length > 0) {
				await this.appendDeletes(
					deletedEntries.map((entry) => keyToStoreKey(entry.id)),
				);
				this.getNative().delete_matching(queryBytes);
			}
			await this.compactIfNeeded();
			return deletedEntries.map((entry) => entry.id);
		});
	}

	async putAndDeleteIds(
		value: T,
		deleteIds: Array<types.IdKey | types.Ideable>,
		id = types.toId(types.extractFieldValue(value, this.indexByArr)),
	): Promise<types.IdKey[]> {
		return this.putWithEncodedFieldsAndDeleteKeys(
			value,
			id,
			this.fieldEncoder(value),
			deleteIds.map(keyToStoreKey),
		);
	}

	putSharedLogCoordinateAndDeleteIds(
		value: T,
		fields: SharedLogCoordinateNativeFields,
		deleteIds: Array<types.IdKey | types.Ideable> = [],
		id = types.toId(types.extractFieldValue(value, this.indexByArr)),
	): MaybePromise<types.IdKey[]> {
		return this.putSharedLogCoordinateValueAndDeleteKeys(
			value,
			id,
			deleteIds.map(keyToStoreKey),
			fields,
		);
	}

	putSharedLogCoordinateFieldsAndDeleteIds(
		fields: SharedLogCoordinateNativeFields,
		deleteIds: Array<types.IdKey | types.Ideable> = [],
		id = types.toId(fields.hash),
	): MaybePromise<types.IdKey[]> {
		return this.putSharedLogCoordinateValueAndDeleteKeys(
			this.createSharedLogCoordinateValue(fields),
			id,
			deleteIds.map(keyToStoreKey),
			fields,
		);
	}

	putSharedLogCoordinateFieldsAndDeleteHashes(
		fields: SharedLogCoordinateNativeFields,
		deleteHashes: string[] = [],
		id = types.toId(fields.hash),
	): MaybePromise<types.IdKey[]> {
		return this.putSharedLogCoordinateValueAndDeleteKeys(
			this.createSharedLogCoordinateValue(fields),
			id,
			deleteHashes.map(stringToStoreKey),
			fields,
			id.primitive === fields.hash
				? stringToStoreKey(fields.hash)
				: keyToStoreKey(id),
		);
	}

	putSharedLogCoordinateFieldsAndDeleteHashesNoReturn(
		fields: SharedLogCoordinateNativeFields,
		deleteHashes: string[] = [],
		id = types.toId(fields.hash),
	): MaybePromise<void> {
		return this.putSharedLogCoordinateValueAndDeleteKeysNoReturn(
			this.createSharedLogCoordinateValue(fields),
			id,
			deleteHashes.map(stringToStoreKey),
			fields,
			id.primitive === fields.hash
				? stringToStoreKey(fields.hash)
				: keyToStoreKey(id),
		);
	}

	putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn(
		fields: SharedLogCoordinateNativeFields,
		deleteHashes: string[] = [],
		id = types.toId(fields.hash),
	): MaybePromise<void> {
		const encodedValue = this.encodeSharedLogCoordinatePersistenceValue(fields);
		if (
			!encodedValue ||
			!this.getNative().put_shared_log_coordinate_encoded_and_delete_keys_void
		) {
			return this.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn(
				fields,
				deleteHashes,
				id,
			);
		}
		const storeKey =
			id.primitive === fields.hash
				? stringToStoreKey(fields.hash)
				: keyToStoreKey(id);
		const deleteKeys = deleteHashes.map(stringToStoreKey);
		if (!this.snapshotFile) {
			this.putSharedLogCoordinateNativeEncodedValueAndDeleteKeysNoReturn(
				encodedValue,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
			return;
		}
		return this.enqueueMutation(async () => {
			if (deleteKeys.length === 0) {
				await this.appendPut(storeKey, undefined, encodedValue);
			} else {
				await this.appendPutAndDeletes(
					storeKey,
					undefined,
					deleteKeys,
					encodedValue,
				);
			}
			this.putSharedLogCoordinateNativeEncodedValueAndDeleteKeysNoReturn(
				encodedValue,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
			await this.compactIfNeeded();
		});
	}

	putSharedLogCoordinatesAndDeleteIdsBatch(
		values: Array<{
			value: T;
			fields: SharedLogCoordinateNativeFields;
			deleteIds?: Array<types.IdKey | types.Ideable>;
			id?: types.IdKey;
		}>,
	): MaybePromise<types.IdKey[]> {
		if (values.length === 0) {
			return [];
		}
		const prepared = values.map((entry) => {
			const id =
				entry.id ??
				types.toId(types.extractFieldValue(entry.value, this.indexByArr));
			const encodedValue = this.snapshotFile
				? this.encodeSharedLogCoordinatePersistenceValue(entry.fields)
				: undefined;
			return {
				value: entry.value,
				id,
				storeKey: keyToStoreKey(id),
				fields: this.encodeSharedLogCoordinateFields(entry.fields),
				deleteKeys: (entry.deleteIds ?? []).map(keyToStoreKey),
				encodedValue,
			};
		});

		return this.putSharedLogCoordinatePreparedBatch(prepared);
	}

	putSharedLogCoordinateFieldsAndDeleteHashesBatch(
		values: Array<{
			fields: SharedLogCoordinateNativeFields;
			deleteHashes?: string[];
			id?: types.IdKey;
		}>,
	): MaybePromise<types.IdKey[]> {
		if (values.length === 0) {
			return [];
		}
		if (!this.getNative().put_shared_log_coordinate_and_delete_keys) {
			return this.putSharedLogCoordinatesAndDeleteIdsBatch(
				values.map((entry) => ({
					value: this.createSharedLogCoordinateValue(entry.fields),
					fields: entry.fields,
					deleteIds: entry.deleteHashes,
					id: entry.id ?? types.toId(entry.fields.hash),
				})),
			);
		}
		const prepared = values.map((entry) => {
			const id = entry.id ?? types.toId(entry.fields.hash);
			const encodedValue = this.snapshotFile
				? this.encodeSharedLogCoordinatePersistenceValue(entry.fields)
				: undefined;
			return {
				value: this.createSharedLogCoordinateValue(entry.fields),
				id,
				storeKey:
					id.primitive === entry.fields.hash
						? stringToStoreKey(entry.fields.hash)
						: keyToStoreKey(id),
				nativeFields: entry.fields,
				deleteKeys: (entry.deleteHashes ?? []).map(stringToStoreKey),
				encodedValue,
			};
		});

		if (!this.snapshotFile) {
			return prepared.flatMap((entry) =>
				this.putSharedLogCoordinateNativeValueAndDeleteKeys(
					entry.value,
					entry.id,
					entry.deleteKeys,
					entry.nativeFields,
					entry.storeKey,
				),
			);
		}

		return this.enqueueMutation(async () => {
			await this.appendPutAndDeletesBatch(prepared);
			const deletedEntries = prepared.flatMap((entry) =>
				this.putSharedLogCoordinateNativeValueAndDeleteKeys(
					entry.value,
					entry.id,
					entry.deleteKeys,
					entry.nativeFields,
					entry.storeKey,
				),
			);
			await this.compactIfNeeded();
			return deletedEntries;
		});
	}

	putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn(
		values: Array<{
			fields: SharedLogCoordinateNativeFields;
			deleteHashes?: string[];
			id?: types.IdKey;
		}>,
	): MaybePromise<void> {
		if (values.length === 0) {
			return;
		}
		const native = this.getNative();
		if (
			!native.put_shared_log_coordinate_and_delete_keys_void &&
			!native.put_shared_log_coordinate
		) {
			const result =
				this.putSharedLogCoordinateFieldsAndDeleteHashesBatch(values);
			return isPromiseLike(result) ? result.then(() => undefined) : undefined;
		}
		const prepared = values.map((entry) => {
			const id = entry.id ?? types.toId(entry.fields.hash);
			const encodedValue = this.snapshotFile
				? this.encodeSharedLogCoordinatePersistenceValue(entry.fields)
				: undefined;
			return {
				value: this.createSharedLogCoordinateValue(entry.fields),
				id,
				storeKey:
					id.primitive === entry.fields.hash
						? stringToStoreKey(entry.fields.hash)
						: keyToStoreKey(id),
				nativeFields: entry.fields,
				deleteKeys: (entry.deleteHashes ?? []).map(stringToStoreKey),
				encodedValue,
			};
		});

		if (!this.snapshotFile) {
			this.putSharedLogCoordinateNativeValuesAndDeleteKeysBatchNoReturn(
				prepared,
			);
			return;
		}

		return this.enqueueMutation(async () => {
			await this.appendPutAndDeletesBatch(prepared);
			this.putSharedLogCoordinateNativeValuesAndDeleteKeysBatchNoReturn(
				prepared,
			);
			await this.compactIfNeeded();
		});
	}

	private putSharedLogCoordinatePreparedBatch(
		prepared: Array<{
			value: T;
			id: types.IdKey;
			storeKey: string;
			fields: Uint8Array;
			deleteKeys: string[];
			encodedValue: Uint8Array | undefined;
		}>,
	): MaybePromise<types.IdKey[]> {
		if (!this.snapshotFile) {
			return prepared.flatMap((entry) =>
				this.getNative()
					.put_and_delete_keys(
						entry.storeKey,
						entry.id,
						entry.value,
						entry.fields,
						entry.deleteKeys,
					)
					.map((deleted) => deleted[0]),
			);
		}

		return this.enqueueMutation(async () => {
			await this.appendPutAndDeletesBatch(prepared);
			const deletedEntries = prepared.flatMap((entry) =>
				this.getNative().put_and_delete_keys(
					entry.storeKey,
					entry.id,
					entry.value,
					entry.fields,
					entry.deleteKeys,
				),
			);
			await this.compactIfNeeded();
			return deletedEntries.map((entry) => entry[0]);
		});
	}

	putSharedLogCoordinateFieldsAndDeleteIdsBatch(
		values: Array<{
			fields: SharedLogCoordinateNativeFields;
			deleteIds?: Array<types.IdKey | types.Ideable>;
			id?: types.IdKey;
		}>,
	): MaybePromise<types.IdKey[]> {
		return this.putSharedLogCoordinatesAndDeleteIdsBatch(
			values.map((entry) => ({
				value: this.createSharedLogCoordinateValue(entry.fields),
				fields: entry.fields,
				deleteIds: entry.deleteIds,
				id: entry.id ?? types.toId(entry.fields.hash),
			})),
		);
	}

	delIds(
		deleteIds: Array<types.IdKey | types.Ideable>,
	): MaybePromise<types.IdKey[]> {
		const deleteKeys = deleteIds.map(keyToStoreKey);
		if (deleteKeys.length === 0) {
			return [];
		}
		if (this.nativeBackboneDocumentIndexPrimary) {
			if (!this.snapshotFile) {
				return this.deleteNativeBackboneDocumentKeys(deleteKeys);
			}
			const deletedIds = this.getNativeBackboneExistingIds(deleteKeys);
			if (deletedIds.length === 0) {
				return [];
			}
			return this.enqueueMutation(async () => {
				await this.appendDeletes(deleteKeys);
				this.deleteNativeBackboneDocumentKeys(deleteKeys);
				await this.compactIfNeeded();
				return deletedIds;
			});
		}
		if (!this.snapshotFile) {
			const deletedEntries = this.getNative().delete_keys(deleteKeys);
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			return deletedEntries.map((entry) => entry[0]);
		}
		return this.enqueueMutation(async () => {
			await this.appendDeletes(deleteKeys);
			const deletedEntries = this.getNative().delete_keys(deleteKeys);
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			await this.compactIfNeeded();
			return deletedEntries.map((entry) => entry[0]);
		});
	}

	delIdsNoReturn(deleteIds: Array<types.IdKey | types.Ideable>): MaybePromise<void> {
		const deleteKeys = deleteIds.map(keyToStoreKey);
		if (deleteKeys.length === 0) {
			return;
		}
		if (this.nativeBackboneDocumentIndexPrimary) {
			if (!this.snapshotFile) {
				this.deleteNativeBackboneDocumentKeysNoReturn(deleteKeys);
				return;
			}
			if (!this.hasNativeBackboneDocumentKeys(deleteKeys)) {
				return;
			}
			return this.enqueueMutation(async () => {
				await this.appendDeletes(deleteKeys);
				this.deleteNativeBackboneDocumentKeysNoReturn(deleteKeys);
				await this.compactIfNeeded();
			});
		}
		const native = this.getNative();
		if (!this.snapshotFile && native.delete_keys_void) {
			native.delete_keys_void(deleteKeys);
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			return;
		}
		if (!this.snapshotFile) {
			native.delete_keys(deleteKeys);
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			return;
		}
		return this.enqueueMutation(async () => {
			await this.appendDeletes(deleteKeys);
			if (native.delete_keys_void) {
				native.delete_keys_void(deleteKeys);
			} else {
				native.delete_keys(deleteKeys);
			}
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			await this.compactIfNeeded();
		});
	}

	delIdsCount(deleteIds: Array<types.IdKey | types.Ideable>): MaybePromise<number> {
		const deleteKeys = deleteIds.map(keyToStoreKey);
		if (deleteKeys.length === 0) {
			return 0;
		}
		if (this.nativeBackboneDocumentIndexPrimary) {
			const result = this.delIds(deleteIds);
			return isPromiseLike(result) ? result.then((deleted) => deleted.length) : result.length;
		}
		const native = this.getNative();
		if (!this.snapshotFile && native.delete_keys_count) {
			const deleted = native.delete_keys_count(deleteKeys);
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			return deleted;
		}
		if (!this.snapshotFile) {
			const deleted = native.delete_keys(deleteKeys);
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			return deleted.length;
		}
		return this.enqueueMutation(async () => {
			await this.appendDeletes(deleteKeys);
			const deleted = native.delete_keys_count
				? native.delete_keys_count(deleteKeys)
				: native.delete_keys(deleteKeys).length;
			this.deleteNativeBackboneDocumentKeys(deleteKeys);
			await this.compactIfNeeded();
			return deleted;
		});
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
		if (this.isClosing()) {
			return [];
		}
		this.assertOpen();
		if (!this.snapshotFile) {
			const compiled = this.requireNativePlan(types.toQuery(query.query), {
				allowAll: true,
			});
			const deletedEntries = this.getNativeCandidatesForPlan({
				compiled,
				sort: [],
				offset: 0,
			});
			if (deletedEntries.length > 0) {
				this.deleteNativeBackboneDocumentKeys(
					deletedEntries.map((entry) => keyToStoreKey(entry.id)),
				);
				if (!this.nativeBackboneDocumentIndexPrimary) {
					this.getNative().delete_matching(
						encodeNativeQuerySpec(compiled.spec),
					);
				}
			}
			return deletedEntries.map((entry) => entry.id);
		}
		return this.enqueueMutation(async () => {
			const compiled = this.requireNativePlan(types.toQuery(query.query), {
				allowAll: true,
			});
			const deletedEntries = this.getNativeCandidatesForPlan({
				compiled,
				sort: [],
				offset: 0,
			});
			if (deletedEntries.length > 0) {
				const deleteKeys = deletedEntries.map((entry) =>
					keyToStoreKey(entry.id),
				);
				await this.appendDeletes(deleteKeys);
				this.deleteNativeBackboneDocumentKeys(deleteKeys);
				if (!this.nativeBackboneDocumentIndexPrimary) {
					this.getNative().delete_matching(
						encodeNativeQuerySpec(compiled.spec),
					);
				}
				await this.compactIfNeeded();
			}
			return deletedEntries.map((entry) => entry.id);
		});
	}

	getSize(): number {
		if (this.isClosing()) {
			return 0;
		}
		this.assertOpen();
		if (
			this.nativeBackboneDocumentIndexPrimary &&
			typeof this.nativeBackboneDocumentIndex?.documentIndexLength === "number"
		) {
			return this.nativeBackboneDocumentIndex.documentIndexLength;
		}
		return this.getNative().len();
	}

	persisted(): boolean {
		return Boolean(this.snapshotFile);
	}

	iterator() {
		if (this.isClosing()) {
			return [][Symbol.iterator]();
		}
		this.assertOpen();
		return this.snapshot()[Symbol.iterator]();
	}

	start(): void {
		if (this.state === "open") {
			return;
		}
		if (this.state === "closing") {
			throw new types.NotStartedError();
		}
		// The wasm module is initialized during init.
		this.setOpen();
	}

	async stop(): Promise<void> {
		if (this.state === "closed") {
			return;
		}
		if (this.state === "closing") {
			await this.mutationQueue.catch(() => undefined);
			return;
		}
		this.setClosing();
		try {
			await this.mutationQueue.catch(() => undefined);
			await this.compactPersistence();
		} finally {
			this.nativeBackboneDocumentIndex = undefined;
			this.nativeBackboneDocumentIndexPrimary = false;
			this.setClosed();
		}
	}

	async drop(): Promise<void> {
		this.setClosing();
		try {
			await this.mutationQueue.catch(() => undefined);
			this.native?.clear();
			this.nativeBackboneDocumentIndex?.clearDocumentIndex?.();
			this.nativeBackboneDocumentIndex = undefined;
			this.nativeBackboneDocumentIndexPrimary = false;
			await this.snapshotFile?.remove();
		} finally {
			this.setClosed();
		}
	}

	async sum(query: types.SumOptions): Promise<number | bigint> {
		if (this.isClosing()) {
			return 0;
		}
		this.assertOpen();
		const compiled = this.requireNativePlan(types.toQuery(query.query), {
			allowAll: true,
		});
		const field = nativeFieldId(
			this.fieldDictionary,
			Array.isArray(query.key) ? query.key : [query.key],
		);
		const queryBytes = encodeNativeQuerySpec(compiled.spec);
		const nativeBackbone = this.nativeBackboneDocumentIndex;
		if (
			this.canQueryNativeBackboneDocumentIndex() &&
			typeof nativeBackbone?.documentSum === "function"
		) {
			try {
				return decodeNativeSum(nativeBackbone.documentSum(queryBytes, field));
			} catch (error) {
				if (this.nativeBackboneDocumentIndexPrimary) {
					throw error;
				}
			}
		}
		return decodeNativeSum(this.getNative().sum(queryBytes, field));
	}

	async count(query?: types.CountOptions): Promise<number> {
		if (this.isClosing()) {
			return 0;
		}
		this.assertOpen();
		const queryCoerced = types.toQuery(query?.query);
		if (queryCoerced.length === 0) {
			return this.getSize();
		}
		return this.countNativePlan(
			this.requireNativePlan(queryCoerced, { allowAll: true }),
		);
	}

	iterate<S extends types.Shape | undefined>(
		query?: types.IterateOptions,
		properties?: { shape?: S; reference?: boolean },
	): types.IndexIterator<T, S> {
		if (this.isClosing()) {
			return this.closedIterator<S>();
		}
		this.assertOpen();
		const nativePagePlan = this.requireNativePagePlan(
			types.toQuery(query?.query),
			query,
		);
		let done: boolean | undefined = undefined;
		let offset = 0;
		let total: number | undefined;
		const getTotal = () =>
			(total ??= this.countNativePlan(nativePagePlan.compiled) ?? 0);
		const clonePage = (batch: types.IndexedValue<T>[]) =>
			(properties?.reference
				? batch
				: cloneResults(batch, this.properties.schema)) as types.IndexedResults<
				types.ReturnTypeFromShape<T, S>
			>;

		const fetch = async (
			n: number,
		): Promise<types.IndexedResults<types.ReturnTypeFromShape<T, S>>> => {
			const closeAsDone = () => {
				done = true;
				return [] as types.IndexedResults<types.ReturnTypeFromShape<T, S>>;
			};
			if (this.isClosing()) {
				return closeAsDone();
			}
			this.assertOpen();
			if (done) {
				return [];
			}
			const remaining = getTotal() - offset;
			const wanted = Math.max(
				0,
				Math.min(Number.isFinite(n) ? Math.floor(n) : remaining, remaining),
			);
			if (wanted === 0) {
				done = remaining === 0;
				return [];
			}
			const batch = this.getNativeCandidatesForPlan({
				compiled: nativePagePlan.compiled,
				sort: nativePagePlan.sort,
				offset,
				limit: wanted,
			});
			offset += batch.length;
			done = offset >= getTotal();
			return clonePage(batch);
		};

		return {
			all: async () => fetch(Infinity),
			next: (n: number) => fetch(n),
			done: () => (this.isClosing() ? true : done),
			pending: async () => {
				if (this.isClosing()) {
					done = true;
					return 0;
				}
				this.assertOpen();
				return done ? 0 : Math.max(0, getTotal() - offset);
			},
			close: () => {
				done = true;
			},
		};
	}

	private getNativePlan(
		queryCoerced: types.Query[],
		options?: { allowAll?: boolean },
	): NativeQueryCompileResult | undefined {
		const compiled = compileNativeQueries(queryCoerced, this.fieldDictionary);
		if (!compiled || (compiled.spec.op === "all" && !options?.allowAll)) {
			return;
		}
		return compiled;
	}

	private requireNativePlan(
		queryCoerced: types.Query[],
		options?: { allowAll?: boolean },
	): NativeQueryCompileResult {
		const compiled = this.getNativePlan(queryCoerced, options);
		if (!compiled) {
			throw new Error(
				`Query is not supported by the native Rust indexer: ${describeNativeQueries(queryCoerced)}`,
			);
		}
		return compiled;
	}

	private getNativePagePlan(
		queryCoerced: types.Query[],
		query?: types.IterateOptions,
	): NativeCandidatePage | undefined {
		const compiled = this.getNativePlan(queryCoerced, { allowAll: true });
		if (!compiled?.exact) {
			return;
		}
		return {
			compiled,
			sort: nativeSortFields(this.fieldDictionary, query?.sort),
			offset: 0,
		};
	}

	private requireNativePagePlan(
		queryCoerced: types.Query[],
		query?: types.IterateOptions,
	): NativeCandidatePage {
		const page = this.getNativePagePlan(queryCoerced, query);
		if (!page) {
			throw new Error(
				`Query is not supported by the native Rust indexer: ${describeNativeQueries(queryCoerced)}`,
			);
		}
		return page;
	}

	private countNativePlan(compiled: NativeQueryCompileResult): number {
		const queryBytes = encodeNativeQuerySpec(compiled.spec);
		const nativeBackbone = this.nativeBackboneDocumentIndex;
		if (
			this.canQueryNativeBackboneDocumentIndex() &&
			typeof nativeBackbone?.documentCount === "function"
		) {
			try {
				return nativeBackbone.documentCount(queryBytes);
			} catch (error) {
				if (this.nativeBackboneDocumentIndexPrimary) {
					throw error;
				}
				// Fall back to the primary Rust index if the experimental native
				// backbone query bridge cannot decode this query yet.
			}
		}
		return this.getNative().count(queryBytes);
	}

	private getNativeCandidatesForPlan(
		page: NativeCandidatePage,
	): types.IndexedValue<T>[] {
		const queryBytes = encodeNativeQuerySpec(page.compiled.spec);
		const sortBytes = encodeNativeSort(page.sort);
		const nativeBackboneResults = this.getNativeBackboneCandidatesForPlan(
			page,
			queryBytes,
			sortBytes,
		);
		if (nativeBackboneResults) {
			return nativeBackboneResults;
		}
		const results =
			page.limit == null
				? this.getNative().query(queryBytes, sortBytes)
				: this.getNative().query_page(
						queryBytes,
						sortBytes,
						page.offset,
						page.limit,
					);
		return results.map((value) => ({
			id: value[0],
			value: this.decodeNativeStoredValue(value[1]),
		}));
	}

	private getNativeBackboneCandidatesForPlan(
		page: NativeCandidatePage,
		queryBytes: Uint8Array,
		sortBytes: Uint8Array,
	): types.IndexedValue<T>[] | undefined {
		const nativeBackbone = this.nativeBackboneDocumentIndex;
		if (!this.canQueryNativeBackboneDocumentIndex()) {
			return;
		}
		try {
			const rows =
				page.limit == null
					? nativeBackbone?.documentQuery?.(queryBytes, sortBytes)
					: nativeBackbone?.documentQueryPage?.(
							queryBytes,
							sortBytes,
							page.offset,
							page.limit,
						);
			if (!rows) {
				return;
			}
			return rows
				.map((row) => this.decodeNativeBackboneDocumentEntry(row))
				.filter((row): row is types.IndexedValue<T> => row != null);
		} catch (error) {
			if (this.nativeBackboneDocumentIndexPrimary) {
				throw error;
			}
			return;
		}
	}

	private putWithEncodedFields(
		value: T,
		id: types.IdKey,
		fields: Uint8Array,
		encodedValue?: EncodedValue,
	): MaybePromise<void> {
		const storeKey = keyToStoreKey(id);
		if (this.nativeBackboneDocumentIndexPrimary) {
			const storedValue =
				encodedValue instanceof Uint8Array
					? encodedValue
					: this.tryNativeEncodedValue(value);
			if (!storedValue) {
				throw new Error("Native backbone document value encoding failed");
			}
			if (!this.snapshotFile) {
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					storedValue,
				);
				return;
			}
			return this.enqueueMutation(async () => {
				await this.appendPut(storeKey, undefined, storedValue);
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					storedValue,
				);
				await this.compactIfNeeded();
			});
		}
		if (!this.snapshotFile) {
			this.getNative().put(storeKey, id, value, fields);
			return;
		}
		return this.enqueueMutation(async () => {
			await this.appendPut(storeKey, value, encodedValue);
			this.getNative().put(storeKey, id, value, fields);
			await this.compactIfNeeded();
		});
	}

	private putWithEncodedValue(
		value: T,
		id: types.IdKey,
		encodedValue: Uint8Array,
	): MaybePromise<void> {
		const storeKey = keyToStoreKey(id);
		if (this.nativeBackboneDocumentIndexPrimary) {
			if (!this.snapshotFile) {
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					encodedValue,
				);
				return;
			}
			return this.enqueueMutation(async () => {
				await this.appendPut(storeKey, undefined, encodedValue);
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					encodedValue,
				);
				await this.compactIfNeeded();
			});
		}
		if (!this.snapshotFile) {
			if (
				this.putNativeDocumentWithPreparedFields(
					storeKey,
					id,
					value,
					encodedValue,
				)
			) {
				return;
			}
			this.getNative().put(storeKey, id, value, this.fieldEncoder(value));
			return;
		}
		return this.enqueueMutation(async () => {
			await this.appendPut(storeKey, value, encodedValue);
			if (
				!this.putNativeDocumentWithPreparedFields(
					storeKey,
					id,
					value,
					encodedValue,
				)
			) {
				this.getNative().put(storeKey, id, value, this.fieldEncoder(value));
			}
			await this.compactIfNeeded();
		});
	}

	private putWithEncodedValueParts(
		value: T,
		id: types.IdKey,
		encodedValueParts: NativeEncodedValueParts,
	): MaybePromise<void> {
		const storeKey = keyToStoreKey(id);
		if (this.nativeBackboneDocumentIndexPrimary) {
			if (!this.snapshotFile) {
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					undefined,
					encodedValueParts,
				);
				return;
			}
			return this.enqueueMutation(async () => {
				await this.appendPut(storeKey, undefined, encodedValueParts);
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					undefined,
					encodedValueParts,
				);
				await this.compactIfNeeded();
			});
		}
		if (!this.snapshotFile) {
			if (
				this.putNativeDocumentWithPreparedFields(
					storeKey,
					id,
					value,
					undefined,
					undefined,
					encodedValueParts,
				)
			) {
				return;
			}
			this.getNative().put(storeKey, id, value, this.fieldEncoder(value));
			return;
		}
		return this.enqueueMutation(async () => {
			await this.appendPut(storeKey, value, encodedValueParts);
			if (
				!this.putNativeDocumentWithPreparedFields(
					storeKey,
					id,
					value,
					undefined,
					undefined,
					encodedValueParts,
				)
			) {
				this.getNative().put(storeKey, id, value, this.fieldEncoder(value));
			}
			await this.compactIfNeeded();
		});
	}

	private async putWithEncodedFieldsAndDeleteKeys(
		value: T,
		id: types.IdKey,
		fields: Uint8Array,
		deleteKeys: string[],
		encodedValue?: EncodedValue,
	): Promise<types.IdKey[]> {
		if (deleteKeys.length === 0) {
			await this.putWithEncodedFields(value, id, fields, encodedValue);
			return [];
		}
		const storeKey = keyToStoreKey(id);
		if (this.nativeBackboneDocumentIndexPrimary) {
			const storedValue =
				encodedValue instanceof Uint8Array
					? encodedValue
					: this.tryNativeEncodedValue(value);
			if (!storedValue) {
				throw new Error("Native backbone document value encoding failed");
			}
			const deletedIds = this.getNativeBackboneExistingIds(deleteKeys);
			if (!this.snapshotFile) {
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					storedValue,
				);
				this.deleteNativeBackboneDocumentKeys(deleteKeys);
				return deletedIds;
			}

			return this.enqueueMutation(async () => {
				await this.appendPutAndDeletes(
					storeKey,
					undefined,
					deleteKeys,
					storedValue,
				);
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					storedValue,
				);
				this.deleteNativeBackboneDocumentKeys(deleteKeys);
				await this.compactIfNeeded();
				return deletedIds;
			});
		}
		if (!this.snapshotFile) {
			return this.getNative()
				.put_and_delete_keys(storeKey, id, value, fields, deleteKeys)
				.map((entry) => entry[0]);
		}

		return this.enqueueMutation(async () => {
			await this.appendPutAndDeletes(storeKey, value, deleteKeys, encodedValue);
			const deletedEntries = this.getNative().put_and_delete_keys(
				storeKey,
				id,
				value,
				fields,
				deleteKeys,
			);
			await this.compactIfNeeded();
			return deletedEntries.map((entry) => entry[0]);
		});
	}

	private putSharedLogCoordinateValueAndDeleteKeys(
		value: T,
		id: types.IdKey,
		deleteKeys: string[],
		fields: SharedLogCoordinateNativeFields,
		storeKey = keyToStoreKey(id),
	): MaybePromise<types.IdKey[]> {
		const nativePutCoordinate =
			this.getNative().put_shared_log_coordinate_and_delete_keys;
		if (!nativePutCoordinate) {
			return this.putWithEncodedFieldsAndDeleteKeys(
				value,
				id,
				this.encodeSharedLogCoordinateFields(fields),
				deleteKeys,
				this.encodeSharedLogCoordinatePersistenceValue(fields),
			);
		}

		if (!this.snapshotFile) {
			return this.putSharedLogCoordinateNativeValueAndDeleteKeys(
				value,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
		}

		return this.enqueueMutation(async () => {
			const encodedValue =
				this.encodeSharedLogCoordinatePersistenceValue(fields);
			if (deleteKeys.length === 0) {
				await this.appendPut(storeKey, value, encodedValue);
				this.putSharedLogCoordinateNativeValueAndDeleteKeys(
					value,
					id,
					deleteKeys,
					fields,
					storeKey,
				);
				await this.compactIfNeeded();
				return [];
			} else {
				await this.appendPutAndDeletes(
					storeKey,
					value,
					deleteKeys,
					encodedValue,
				);
			}
			const deletedEntries =
				this.putSharedLogCoordinateNativeValueAndDeleteKeys(
					value,
					id,
					deleteKeys,
					fields,
					storeKey,
				);
			await this.compactIfNeeded();
			return deletedEntries;
		});
	}

	private putSharedLogCoordinateValueAndDeleteKeysNoReturn(
		value: T,
		id: types.IdKey,
		deleteKeys: string[],
		fields: SharedLogCoordinateNativeFields,
		storeKey = keyToStoreKey(id),
	): MaybePromise<void> {
		const native = this.getNative();
		if (
			!native.put_shared_log_coordinate_and_delete_keys_void &&
			!(deleteKeys.length === 0 && native.put_shared_log_coordinate)
		) {
			const result = this.putSharedLogCoordinateValueAndDeleteKeys(
				value,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
			return isPromiseLike(result) ? result.then(() => undefined) : undefined;
		}

		if (!this.snapshotFile) {
			this.putSharedLogCoordinateNativeValueAndDeleteKeysNoReturn(
				value,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
			return;
		}

		return this.enqueueMutation(async () => {
			const encodedValue =
				this.encodeSharedLogCoordinatePersistenceValue(fields);
			if (deleteKeys.length === 0) {
				await this.appendPut(storeKey, value, encodedValue);
			} else {
				await this.appendPutAndDeletes(
					storeKey,
					value,
					deleteKeys,
					encodedValue,
				);
			}
			this.putSharedLogCoordinateNativeValueAndDeleteKeysNoReturn(
				value,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
			await this.compactIfNeeded();
		});
	}

	private putSharedLogCoordinateNativeValueAndDeleteKeys(
		value: T,
		id: types.IdKey,
		deleteKeys: string[],
		fields: SharedLogCoordinateNativeFields,
		storeKey: string,
	): types.IdKey[] {
		const native = this.getNative();
		const nativePutCoordinate =
			native.put_shared_log_coordinate_and_delete_keys;
		if (!nativePutCoordinate) {
			throw new Error("Native shared-log coordinate put is unavailable");
		}
		if (deleteKeys.length === 0) {
			const nativePutCoordinateNoDeletes = native.put_shared_log_coordinate;
			if (nativePutCoordinateNoDeletes) {
				nativePutCoordinateNoDeletes.call(
					native,
					storeKey,
					id,
					value,
					...this.getSharedLogCoordinateNativePutArgs(fields),
				);
				return [];
			}
		}
		return nativePutCoordinate
			.call(
				native,
				storeKey,
				id,
				value,
				...this.getSharedLogCoordinateNativePutArgs(fields),
				deleteKeys,
			)
			.map((entry) => entry[0]);
	}

	private putSharedLogCoordinateNativeValueAndDeleteKeysNoReturn(
		value: T,
		id: types.IdKey,
		deleteKeys: string[],
		fields: SharedLogCoordinateNativeFields,
		storeKey: string,
	): void {
		const native = this.getNative();
		if (deleteKeys.length === 0) {
			const nativePutCoordinateNoDeletes = native.put_shared_log_coordinate;
			if (nativePutCoordinateNoDeletes) {
				nativePutCoordinateNoDeletes.call(
					native,
					storeKey,
					id,
					value,
					...this.getSharedLogCoordinateNativePutArgs(fields),
				);
				return;
			}
		}
		const nativePutCoordinate =
			native.put_shared_log_coordinate_and_delete_keys_void;
		if (!nativePutCoordinate) {
			this.putSharedLogCoordinateNativeValueAndDeleteKeys(
				value,
				id,
				deleteKeys,
				fields,
				storeKey,
			);
			return;
		}
		nativePutCoordinate.call(
			native,
			storeKey,
			id,
			value,
			...this.getSharedLogCoordinateNativePutArgs(fields),
			deleteKeys,
		);
	}

	private putSharedLogCoordinateNativeValuesAndDeleteKeysBatchNoReturn(
		entries: Array<{
			value: T;
			id: types.IdKey;
			storeKey: string;
			nativeFields: SharedLogCoordinateNativeFields;
			deleteKeys: string[];
		}>,
	): void {
		const native = this.getNative();
		const nativePutBatch =
			native.put_shared_log_coordinates_and_delete_keys_void;
		if (!nativePutBatch) {
			for (const entry of entries) {
				this.putSharedLogCoordinateNativeValueAndDeleteKeysNoReturn(
					entry.value,
					entry.id,
					entry.deleteKeys,
					entry.nativeFields,
					entry.storeKey,
				);
			}
			return;
		}

		nativePutBatch.call(
			native,
			entries.map((entry) => entry.storeKey),
			entries.map((entry) => entry.id),
			entries.map((entry) => entry.value),
			...this.getSharedLogCoordinateNativeFieldIdArgs(),
			entries.map((entry) => entry.nativeFields.hash),
			entries.map(
				(entry) =>
					entry.nativeFields.hashNumberString ??
					entry.nativeFields.hashNumber.toString(),
			),
			entries.map((entry) => entry.nativeFields.gid),
			entries.map(
				(entry) =>
					entry.nativeFields.coordinateStrings ??
					entry.nativeFields.coordinates.map((coordinate) =>
						coordinate.toString(),
					),
			),
			entries.map(
				(entry) =>
					entry.nativeFields.wallTimeString ??
					entry.nativeFields.wallTime.toString(),
			),
			new Uint8Array(
				entries.map((entry) =>
					entry.nativeFields.assignedToRangeBoundary ? 1 : 0,
				),
			),
			entries.map((entry) => entry.nativeFields.metaBytes),
			this.byteElementIndexLimit,
			entries.map((entry) => entry.deleteKeys),
		);
	}

	private putSharedLogCoordinateNativeEncodedValueAndDeleteKeysNoReturn(
		encodedValue: Uint8Array,
		id: types.IdKey,
		deleteKeys: string[],
		fields: SharedLogCoordinateNativeFields,
		storeKey: string,
	): void {
		const nativePutCoordinate =
			this.getNative().put_shared_log_coordinate_encoded_and_delete_keys_void;
		if (!nativePutCoordinate) {
			throw new Error(
				"Native encoded shared-log coordinate put is unavailable",
			);
		}
		nativePutCoordinate.call(
			this.getNative(),
			storeKey,
			id,
			encodedValue,
			...this.getSharedLogCoordinateNativePutArgs(fields),
			deleteKeys,
		);
	}

	private getSharedLogCoordinateFieldIds(): NonNullable<
		RustIndex<T, NestedType>["sharedLogCoordinateFieldIds"]
	> {
		return (this.sharedLogCoordinateFieldIds ??= {
			hash: nativeFieldId(this.fieldDictionary, ["hash"]),
			hashNumber: nativeFieldId(this.fieldDictionary, ["hashNumber"]),
			gid: nativeFieldId(this.fieldDictionary, ["gid"]),
			coordinates: nativeFieldId(this.fieldDictionary, ["coordinates"]),
			coordinatesArray: nativeArrayElementFieldId(this.fieldDictionary, [
				"coordinates",
			]),
			wallTime: nativeFieldId(this.fieldDictionary, ["wallTime"]),
			assignedToRangeBoundary: nativeFieldId(this.fieldDictionary, [
				"assignedToRangeBoundary",
			]),
			meta: nativeFieldId(this.fieldDictionary, ["_meta"]),
		});
	}

	private getSharedLogCoordinateNativeFieldIdArgs(): [
		number,
		number,
		number,
		number,
		number,
		number,
		number,
		number,
	] {
		const ids = this.getSharedLogCoordinateFieldIds();
		return [
			ids.hash,
			ids.hashNumber,
			ids.gid,
			ids.coordinates,
			ids.coordinatesArray,
			ids.wallTime,
			ids.assignedToRangeBoundary,
			ids.meta,
		];
	}

	private getSharedLogCoordinateNativePutArgs(
		fields: SharedLogCoordinateNativeFields,
	): [
		number,
		number,
		number,
		number,
		number,
		number,
		number,
		number,
		string,
		string,
		string,
		string[],
		string,
		boolean,
		Uint8Array,
		number,
	] {
		return [
			...this.getSharedLogCoordinateNativeFieldIdArgs(),
			fields.hash,
			fields.hashNumberString ?? fields.hashNumber.toString(),
			fields.gid,
			fields.coordinateStrings ??
				fields.coordinates.map((coordinate) => coordinate.toString()),
			fields.wallTimeString ?? fields.wallTime.toString(),
			fields.assignedToRangeBoundary,
			fields.metaBytes,
			this.byteElementIndexLimit,
		];
	}

	private createSharedLogCoordinateValue(
		fields: SharedLogCoordinateNativeFields,
	): T {
		const value = Object.create(
			(this.properties.schema as { prototype?: object }).prototype ??
				Object.prototype,
		) as Record<string, any>;
		value.hash = fields.hash;
		value.hashNumber = fields.hashNumber;
		value.gid = fields.gid;
		value.coordinates = fields.coordinates;
		value.wallTime = fields.wallTime;
		value.assignedToRangeBoundary = fields.assignedToRangeBoundary;
		value._meta = fields.metaBytes;
		return value as T;
	}

	private encodeSharedLogCoordinatePersistenceValue(
		fields: SharedLogCoordinateNativeFields,
	): Uint8Array | undefined {
		const schemaName = (this.properties.schema as { name?: string }).name;
		if (
			schemaName === "EntryReplicatedU32" &&
			typeof fields.hashNumber !== "bigint"
		) {
			return encodeSharedLogCoordinateValue(fields, false);
		}
		if (
			schemaName === "EntryReplicatedU64" &&
			typeof fields.hashNumber === "bigint"
		) {
			return encodeSharedLogCoordinateValue(fields, true);
		}
		return undefined;
	}

	private encodeSharedLogCoordinateFields(
		fields: SharedLogCoordinateNativeFields,
	): Uint8Array {
		const ids = this.getSharedLogCoordinateFieldIds();
		const writer = new NativeFieldWriter();
		const state = { nextScope: 1 };
		writer.writeString(0, ids.hash, fields.hash);
		writer.writeU64(0, ids.hashNumber, fields.hashNumber);
		writer.writeString(0, ids.gid, fields.gid);
		for (const coordinate of fields.coordinates) {
			const scope = state.nextScope++;
			writer.writeBool(scope, ids.coordinatesArray, true);
			writer.writeU64(scope, ids.coordinates, coordinate);
		}
		writer.writeU64(0, ids.wallTime, fields.wallTime);
		writer.writeBool(
			0,
			ids.assignedToRangeBoundary,
			fields.assignedToRangeBoundary,
		);
		writeNativeBytesFacts(
			writer,
			state,
			0,
			ids.meta,
			fields.metaBytes,
			this.byteElementIndexLimit,
		);
		return writer.finish();
	}

	private putNativeDocument(storeKey: string, id: types.IdKey, value: T): void {
		const encodedValue = this.tryNativeEncodedValue(value);
		if (
			this.putNativeDocumentWithPreparedFields(
				storeKey,
				id,
				value,
				encodedValue,
			)
		) {
			return;
		}
		this.getNative().put(storeKey, id, value, this.fieldEncoder(value));
	}

	private putNativeDocumentEncodedPartsBatch(
		values: Array<{
			value: T;
			id: types.IdKey;
			storeKey?: string;
			encodedValue?: Uint8Array;
			encodedValueParts?: NativeEncodedValueParts;
		}>,
	): boolean {
		if (values.length === 0) {
			return false;
		}
		const native = this.getNative();
		const putEncodedPartsBatch = native.put_encoded_parts_batch;
		if (!putEncodedPartsBatch) {
			return false;
		}
		const keys = new Array<string>(values.length);
		const ids = new Array<types.IdKey>(values.length);
		const storedValues = new Array<T>(values.length);
		const prefixes = new Array<Uint8Array>(values.length);
		const suffixes = new Array<Uint8Array>(values.length);
		for (let i = 0; i < values.length; i++) {
			const entry = values[i]!;
			if (entry.encodedValue || !entry.encodedValueParts) {
				return false;
			}
			keys[i] = entry.storeKey ?? keyToStoreKey(entry.id);
			ids[i] = entry.id;
			storedValues[i] = entry.value;
			prefixes[i] = entry.encodedValueParts.prefix;
			suffixes[i] = entry.encodedValueParts.suffix;
		}
		try {
			putEncodedPartsBatch.call(
				native,
				keys,
				ids,
				storedValues,
				prefixes,
				suffixes,
				this.nativeByteElementIndexLimit,
			);
			return true;
		} catch {
			// Fall back to the per-entry native call, which already falls back again
			// to TypeScript field encoding for schemas outside the native extractor.
			return false;
		}
	}

	private putNativeDocumentEncodedPartsStored(
		storeKey: string,
		id: types.IdKey,
		encodedValueParts: NativeEncodedValueParts,
	): boolean {
		const native = this.getNative();
		const putEncodedPartsStored = native.put_encoded_parts_stored;
		if (!putEncodedPartsStored) {
			return false;
		}
		try {
			putEncodedPartsStored.call(
				native,
				storeKey,
				id,
				encodedValueParts.prefix,
				encodedValueParts.suffix,
				this.nativeByteElementIndexLimit,
			);
			return true;
		} catch {
			return false;
		}
	}

	private putNativeBackboneDocumentEncodedPartsStored(
		storeKey: string,
		encodedValueParts: NativeEncodedValueParts,
	): boolean {
		const backbone = this.nativeBackboneDocumentIndex;
		if (!backbone?.putDocumentEncodedPartsStored) {
			return false;
		}
		try {
			backbone.putDocumentEncodedPartsStored(
				storeKey,
				encodedValueParts.prefix,
				encodedValueParts.suffix,
				this.nativeByteElementIndexLimit,
			);
			return true;
		} catch {
			return false;
		}
	}

	private putNativeBackboneDocumentEncodedValueStored(
		storeKey: string,
		encodedValue: Uint8Array,
	): boolean {
		return this.putNativeBackboneDocumentEncodedPartsStored(storeKey, {
			prefix: encodedValue,
			suffix: EMPTY_NATIVE_ENCODED_SUFFIX,
		});
	}

	private putNativeBackboneDocumentPreparedValueStored(
		storeKey: string,
		encodedValue?: Uint8Array,
		encodedValueParts?: NativeEncodedValueParts,
	): boolean {
		if (encodedValueParts) {
			return this.putNativeBackboneDocumentEncodedPartsStored(
				storeKey,
				encodedValueParts,
			);
		}
		return (
			encodedValue != null &&
			this.putNativeBackboneDocumentEncodedValueStored(storeKey, encodedValue)
		);
	}

	private putNativeBackboneDocumentPreparedValueStoredOrThrow(
		storeKey: string,
		encodedValue?: Uint8Array,
		encodedValueParts?: NativeEncodedValueParts,
	): void {
		if (
			!this.putNativeBackboneDocumentPreparedValueStored(
				storeKey,
				encodedValue,
				encodedValueParts,
			)
		) {
			throw new Error("Native backbone document put failed");
		}
	}

	private putNativeDocumentEncodedPartsStoredBatch(
		values: Array<{
			id: types.IdKey;
			storeKey?: string;
			encodedValueParts: NativeEncodedValueParts;
		}>,
	): boolean {
		if (values.length === 0) {
			return false;
		}
		const native = this.getNative();
		const putEncodedPartsStoredBatch = native.put_encoded_parts_stored_batch;
		if (!putEncodedPartsStoredBatch) {
			return false;
		}
		const keys = new Array<string>(values.length);
		const ids = new Array<types.IdKey>(values.length);
		const prefixes = new Array<Uint8Array>(values.length);
		const suffixes = new Array<Uint8Array>(values.length);
		for (let i = 0; i < values.length; i++) {
			const entry = values[i]!;
			keys[i] = entry.storeKey ?? keyToStoreKey(entry.id);
			ids[i] = entry.id;
			prefixes[i] = entry.encodedValueParts.prefix;
			suffixes[i] = entry.encodedValueParts.suffix;
		}
		try {
			putEncodedPartsStoredBatch.call(
				native,
				keys,
				ids,
				prefixes,
				suffixes,
				this.nativeByteElementIndexLimit,
			);
			return true;
		} catch {
			return false;
		}
	}

	private putNativeBackboneDocumentEncodedPartsStoredBatch(
		values: Array<{
			id: types.IdKey;
			storeKey?: string;
			encodedValueParts: NativeEncodedValueParts;
		}>,
	): boolean {
		const backbone = this.nativeBackboneDocumentIndex;
		if (backbone?.putDocumentEncodedPartsStoredBatch && values.length > 0) {
			try {
				backbone.putDocumentEncodedPartsStoredBatch(
					values.map((entry) => ({
						key: entry.storeKey ?? keyToStoreKey(entry.id),
						valuePrefixBytes: entry.encodedValueParts.prefix,
						valueSuffixBytes: entry.encodedValueParts.suffix,
					})),
					this.nativeByteElementIndexLimit,
				);
				return true;
			} catch {
				// Fall back to single puts so older native-backbone objects remain usable.
			}
		}
		for (const entry of values) {
			if (!this.putNativeBackboneDocumentEncodedPartsStored(
				entry.storeKey ?? keyToStoreKey(entry.id),
				entry.encodedValueParts,
			)) {
				return false;
			}
		}
		return true;
	}

	private populateNativeBackboneDocumentIndex(
		backbone: NativeBackboneDocumentIndexTarget,
	): void {
		const putDocument = backbone.putDocumentEncodedPartsStored;
		if (!putDocument || !this.nativeEncodedValueEncoder) {
			return;
		}
		for (const [id, rawValue] of this.getNative().entries()) {
			const value = this.decodeNativeStoredValue(rawValue);
			putDocument.call(
				backbone,
				keyToStoreKey(id),
				this.nativeEncodedValueEncoder(value),
				EMPTY_NATIVE_ENCODED_SUFFIX,
				this.nativeByteElementIndexLimit,
			);
		}
	}

	private canUseNativeBackboneDocumentIndexAsPrimary(
		backbone: NativeBackboneDocumentIndexTarget,
	): boolean {
		return (
			typeof backbone.documentIndexLength === "number" &&
			typeof backbone.documentEntry === "function" &&
			typeof backbone.documentQuery === "function" &&
			typeof backbone.documentQueryPage === "function" &&
			typeof backbone.documentCount === "function" &&
			typeof backbone.documentSum === "function" &&
			typeof backbone.deleteDocument === "function" &&
			typeof backbone.putDocumentEncodedPartsStored === "function"
		);
	}

	private deleteNativeBackboneDocumentKeys(storeKeys: string[]): types.IdKey[] {
		const deleteDocument = this.nativeBackboneDocumentIndex?.deleteDocument;
		if (!deleteDocument || storeKeys.length === 0) {
			return [];
		}
		const deletedIds: types.IdKey[] = [];
		for (const key of storeKeys) {
			if (deleteDocument.call(this.nativeBackboneDocumentIndex, key)) {
				const id = storeKeyToIdKey(key);
				if (id) {
					deletedIds.push(id);
				}
			}
		}
		return deletedIds;
	}

	private deleteNativeBackboneDocumentKeysNoReturn(storeKeys: string[]): void {
		const deleteDocument = this.nativeBackboneDocumentIndex?.deleteDocument;
		if (!deleteDocument || storeKeys.length === 0) {
			return;
		}
		for (const key of storeKeys) {
			deleteDocument.call(this.nativeBackboneDocumentIndex, key);
		}
	}

	private hasNativeBackboneDocumentKeys(storeKeys: string[]): boolean {
		const documentEntry = this.nativeBackboneDocumentIndex?.documentEntry;
		if (!documentEntry || storeKeys.length === 0) {
			return false;
		}
		for (const key of storeKeys) {
			if (documentEntry.call(this.nativeBackboneDocumentIndex, key)) {
				return true;
			}
		}
		return false;
	}

	private getNativeBackboneExistingIds(storeKeys: string[]): types.IdKey[] {
		const documentEntry = this.nativeBackboneDocumentIndex?.documentEntry;
		if (!documentEntry || storeKeys.length === 0) {
			return [];
		}
		const ids: types.IdKey[] = [];
		for (const key of storeKeys) {
			if (documentEntry.call(this.nativeBackboneDocumentIndex, key)) {
				const id = storeKeyToIdKey(key);
				if (id) {
					ids.push(id);
				}
			}
		}
		return ids;
	}

	private getNativeBackboneDocumentEntry(
		storeKey: string,
	): types.IndexedResult<T> | undefined {
		const entry = this.nativeBackboneDocumentIndex?.documentEntry?.(storeKey);
		return entry ? this.decodeNativeBackboneDocumentEntry(entry) : undefined;
	}

	private getNativeBackboneExactStringFirst(
		field: number,
		value: string,
	): types.IndexedResult<T> | undefined {
		const backbone = this.nativeBackboneDocumentIndex;
		if (
			!backbone?.documentExactStringFirstKey ||
			!backbone.documentValueBytes
		) {
			return;
		}
		const key = backbone.documentExactStringFirstKey(field, value);
		if (!key) {
			return;
		}
		const id = storeKeyToIdKey(key);
		if (!id) {
			return;
		}
		const bytes = backbone.documentValueBytes(key);
		if (!bytes) {
			return;
		}
		return {
			id,
			value: this.decodeNativeStoredValue(bytes),
		};
	}

	private canQueryNativeBackboneDocumentIndex(): boolean {
		const nativeBackbone = this.nativeBackboneDocumentIndex;
		if (this.nativeBackboneDocumentIndexPrimary) {
			return nativeBackbone != null;
		}
		return (
			nativeBackbone != null &&
			typeof nativeBackbone.documentIndexLength === "number" &&
			nativeBackbone.documentIndexLength === this.getNative().len()
		);
	}

	private decodeNativeBackboneDocumentEntry(
		entry: [string, Uint8Array],
	): types.IndexedValue<T> | undefined {
		const id = storeKeyToIdKey(entry[0]);
		if (!id) {
			return;
		}
		return {
			id,
			value: this.decodeNativeStoredValue(entry[1]),
		};
	}

	private getNativeExactStringFirst(
		field: number,
		value: string,
	): types.IndexedResult<T> | undefined {
		const result = this.getNative().query_page(
			encodeNativeQuerySpec({
				op: "exact",
				field,
				value: { type: "string", value },
			}),
			encodeNativeSort(),
			0,
			1,
		)[0];
		return result
			? { id: result[0], value: this.decodeNativeStoredValue(result[1]) }
			: undefined;
	}

	private validateNativeDocumentEncodedParts(
		encodedValueParts: NativeEncodedValueParts,
	): boolean {
		const native = this.getNative();
		const validate = native.validate_encoded_parts;
		if (!validate) {
			return false;
		}
		try {
			validate.call(
				native,
				encodedValueParts.prefix,
				encodedValueParts.suffix,
				this.nativeByteElementIndexLimit,
			);
			return true;
		} catch {
			return false;
		}
	}

	private validateNativeDocumentEncodedPartsBatch(
		values: Array<{ encodedValueParts: NativeEncodedValueParts }>,
	): boolean {
		if (values.length === 0) {
			return false;
		}
		const native = this.getNative();
		const validateBatch = native.validate_encoded_parts_batch;
		if (!validateBatch) {
			return false;
		}
		try {
			validateBatch.call(
				native,
				values.map((entry) => entry.encodedValueParts.prefix),
				values.map((entry) => entry.encodedValueParts.suffix),
				this.nativeByteElementIndexLimit,
			);
			return true;
		} catch {
			return false;
		}
	}

	private putWithEncodedValuePartsStored(
		id: types.IdKey,
		encodedValueParts: NativeEncodedValueParts,
	): MaybePromise<void> | false {
		const storeKey = keyToStoreKey(id);
		if (!this.snapshotFile) {
			if (this.nativeBackboneDocumentIndexPrimary) {
				this.putNativeBackboneDocumentPreparedValueStoredOrThrow(
					storeKey,
					undefined,
					encodedValueParts,
				);
				return;
			}
			const stored = this.putNativeDocumentEncodedPartsStored(
				storeKey,
				id,
				encodedValueParts,
			);
			if (!stored) {
				return false;
			}
			this.putNativeBackboneDocumentEncodedPartsStored(
				storeKey,
				encodedValueParts,
			);
			return;
		}
		if (!this.validateNativeDocumentEncodedParts(encodedValueParts)) {
			return false;
		}
		return this.enqueueMutation(async () => {
			await this.appendPut(storeKey, undefined, encodedValueParts);
			if (this.nativeBackboneDocumentIndexPrimary) {
				if (
					!this.putNativeBackboneDocumentEncodedPartsStored(
						storeKey,
						encodedValueParts,
					)
				) {
					throw new Error("Native backbone contextual document put failed");
				}
				await this.compactIfNeeded();
				return;
			}
			if (
				!this.putNativeDocumentEncodedPartsStored(
					storeKey,
					id,
					encodedValueParts,
				)
			) {
				throw new Error("Native encoded contextual document put failed");
			}
			this.putNativeBackboneDocumentEncodedPartsStored(
				storeKey,
				encodedValueParts,
			);
			await this.compactIfNeeded();
		});
	}

	putStoredContextualEncodedValue(
		id: types.IdKey,
		encodedValueParts: NativeEncodedValueParts,
		_options?: { replace?: boolean },
	): MaybePromise<void> | false {
		if (this.isClosing()) {
			return;
		}
		this.assertOpen();
		return this.putWithEncodedValuePartsStored(id, encodedValueParts);
	}

	private async putWithEncodedValuePartsStoredBatch(
		values: Array<{
			id: types.IdKey;
			encodedValueParts: NativeEncodedValueParts;
		}>,
	): Promise<boolean> {
		if (values.length === 0) {
			return false;
		}
		if (!this.snapshotFile) {
			if (this.nativeBackboneDocumentIndexPrimary) {
				if (!this.putNativeBackboneDocumentEncodedPartsStoredBatch(values)) {
					throw new Error("Native backbone contextual document batch put failed");
				}
				return true;
			}
			const stored = this.putNativeDocumentEncodedPartsStoredBatch(values);
			if (stored) {
				this.putNativeBackboneDocumentEncodedPartsStoredBatch(values);
			}
			return stored;
		}
		if (!this.validateNativeDocumentEncodedPartsBatch(values)) {
			return false;
		}
		await this.enqueueMutation(async () => {
			await this.snapshotFile!.appendPutBatch(
				values.map((entry) => ({
					key: keyToStoreKey(entry.id),
					encodedValue: entry.encodedValueParts,
				})),
				this.properties.schema,
			);
			if (this.nativeBackboneDocumentIndexPrimary) {
				if (!this.putNativeBackboneDocumentEncodedPartsStoredBatch(values)) {
					throw new Error(
						"Native backbone contextual document batch put failed",
					);
				}
				await this.compactIfNeeded();
				return;
			}
			if (!this.putNativeDocumentEncodedPartsStoredBatch(values)) {
				throw new Error("Native encoded contextual document batch put failed");
			}
			this.putNativeBackboneDocumentEncodedPartsStoredBatch(values);
			await this.compactIfNeeded();
		});
		return true;
	}

	private putNativeDocumentWithPreparedFields(
		storeKey: string,
		id: types.IdKey,
		value: T,
		encodedValue?: Uint8Array,
		fields?: Uint8Array,
		encodedValueParts?: NativeEncodedValueParts,
	): boolean {
		if (encodedValueParts) {
			const native = this.getNative();
			const putEncodedParts = native.put_encoded_parts;
			try {
				if (!putEncodedParts) {
					return false;
				}
				putEncodedParts.call(
					native,
					storeKey,
					id,
					value,
					encodedValueParts.prefix,
					encodedValueParts.suffix,
					this.nativeByteElementIndexLimit,
				);
				return true;
			} catch {
				// Fall back to the proven TypeScript fact encoder for schemas whose
				// Borsh bytes are not covered by the native extractor yet.
			}
		}
		if (encodedValue) {
			try {
				this.getNative().put_encoded(
					storeKey,
					id,
					value,
					encodedValue,
					this.nativeByteElementIndexLimit,
				);
				return true;
			} catch {
				// Fall back to the proven TypeScript fact encoder for schemas whose
				// Borsh bytes are not covered by the native extractor yet.
			}
		}
		if (fields) {
			this.getNative().put(storeKey, id, value, fields);
			return true;
		}
		return false;
	}

	private tryNativeEncodedValue(value: T): Uint8Array | undefined {
		try {
			return this.nativeEncodedValueEncoder?.(value);
		} catch {
			return;
		}
	}

	private createContextualValue(
		value: Record<string, any>,
		context: Record<string, any>,
	): T {
		const wrapped = Object.assign(
			Object.create((this.properties.schema as any).prototype),
			value,
		);
		wrapped.__context = context;
		return wrapped as T;
	}

	private asContextualValue(
		value: Record<string, any>,
		context: Record<string, any>,
	): T {
		if (
			value.__context === context &&
			Object.getPrototypeOf(value) === (this.properties.schema as any).prototype
		) {
			return value as T;
		}
		return this.createContextualValue(value, context);
	}

	private snapshot(): types.IndexedValue<T>[] {
		if (this.nativeBackboneDocumentIndexPrimary) {
			const rows = this.nativeBackboneDocumentIndex?.documentQuery?.(
				encodeNativeQuerySpec({ op: "all" }),
				encodeNativeSort(),
			);
			if (!rows) {
				return [];
			}
			return rows
				.map((row) => this.decodeNativeBackboneDocumentEntry(row))
				.filter((row): row is types.IndexedValue<T> => row != null);
		}
		return this.getNative()
			.entries()
			.map((entry) => {
				return { id: entry[0], value: this.decodeNativeStoredValue(entry[1]) };
			});
	}

	private decodeNativeStoredValue(
		value: T | Uint8Array | [Uint8Array, Uint8Array],
	): T {
		if (value instanceof Uint8Array) {
			return deserialize(value, this.properties.schema) as T;
		}
		if (
			Array.isArray(value) &&
			value.length === 2 &&
			value[0] instanceof Uint8Array &&
			value[1] instanceof Uint8Array
		) {
			return deserialize(
				concatEncodedParts(value[0], value[1]),
				this.properties.schema,
			) as T;
		}
		return value as T;
	}

	private getNative(): NativeRustIndex<T> {
		if (!this.native) {
			throw new Error("Index has not been initialized");
		}
		return this.native;
	}

	private enqueueMutation<R>(work: () => Promise<R>): Promise<R> {
		const next = this.mutationQueue.then(work, work);
		this.mutationQueue = next.then(
			() => undefined,
			() => undefined,
		);
		return next;
	}

	private enqueuePersistence(work: () => Promise<void>): Promise<void> {
		if (!this.snapshotFile) {
			return Promise.resolve();
		}
		const next = this.persistQueue.then(work, work);
		this.persistQueue = next.then(
			() => undefined,
			() => undefined,
		);
		return next;
	}

	private appendPut(
		storeKey: string,
		value: T | undefined,
		encodedValue?: EncodedValue,
	): Promise<void> {
		return this.enqueuePersistence(() =>
			this.snapshotFile!.appendPut(
				storeKey,
				value,
				this.properties.schema,
				encodedValue,
			),
		);
	}

	private appendPutAndDeletes(
		storeKey: string,
		value: T | undefined,
		deleteKeys: string[],
		encodedValue?: EncodedValue,
	): Promise<void> {
		return this.appendPutAndDeletesBatch([
			{ storeKey, value, deleteKeys, encodedValue },
		]);
	}

	private appendPutAndDeletesBatch(
		entries: Array<{
			storeKey: string;
			value?: T;
			deleteKeys: string[];
			encodedValue?: EncodedValue;
		}>,
	): Promise<void> {
		return this.enqueuePersistence(() =>
			this.snapshotFile!.appendPutAndDeleteBatch(
				entries.map((entry) => ({
					key: entry.storeKey,
					value: entry.value,
					encodedValue: entry.encodedValue,
					deleteKeys: entry.deleteKeys,
				})),
				this.properties.schema,
			),
		);
	}

	private appendDeletes(storeKeys: string[]): Promise<void> {
		return this.enqueuePersistence(() =>
			this.snapshotFile!.appendDeleteBatch(storeKeys),
		);
	}

	private async compactIfNeeded(): Promise<void> {
		const compactAfterOperations =
			this.options.persistence?.compactAfterOperations ??
			DEFAULT_JOURNAL_COMPACT_AFTER_OPERATIONS;
		if (
			this.snapshotFile &&
			compactAfterOperations > 0 &&
			this.snapshotFile.pendingOperations() >= compactAfterOperations
		) {
			await this.compactPersistence();
		}
	}

	private async compactPersistence(): Promise<void> {
		if (!this.snapshotFile || !this.native) {
			return;
		}
		await this.enqueuePersistence(() =>
			this.snapshotFile!.compact(
				this.snapshot().map((entry) => entry.value),
				this.properties.schema,
			),
		);
	}
}

export class RustIndices implements types.Indices {
	private scopes: Map<string, types.Indices>;
	private indices: { schema: any; index: RustIndex<any, any> }[] = [];
	private closed: boolean;

	constructor(
		private readonly directory?: string,
		private readonly path: string[] = [],
		private readonly options: RustIndexerOptions = {},
	) {
		this.scopes = new Map();
		this.closed = true;
	}

	async init<T extends Record<string, any>, NestedType>(
		properties: types.IndexEngineInitProperties<T, any>,
	) {
		const existingIndex = this.indices.find(
			(i) => i.schema === properties.schema,
		);
		if (existingIndex) {
			return existingIndex.index as RustIndex<T, NestedType>;
		}
		const index = new RustIndex<T, NestedType>(
			this.directory,
			this.path,
			this.options,
		);
		this.indices.push({ schema: properties.schema, index });
		await index.init(properties);

		if (!this.closed) {
			await index.start();
		}

		return index;
	}

	async scope(name: string): Promise<types.Indices> {
		let scope = this.scopes.get(name);
		if (!scope) {
			scope = new RustIndices(
				this.directory,
				[...this.path, name],
				this.options,
			);
			if (!this.closed) {
				await scope.start();
			}
			this.scopes.set(name, scope);
		}
		return scope;
	}

	persisted(): boolean {
		return Boolean(this.directory);
	}

	async start(): Promise<void> {
		this.closed = false;
		for (const scope of this.scopes.values()) {
			await scope.start();
		}

		for (const index of this.indices) {
			await index.index.start();
		}
	}

	async stop(): Promise<void> {
		this.closed = true;
		for (const scope of this.scopes.values()) {
			await scope.stop();
		}

		for (const index of this.indices) {
			await index.index.stop();
		}
	}

	async drop(): Promise<void> {
		for (const scope of this.scopes.values()) {
			await scope.drop();
		}

		for (const index of this.indices) {
			await index.index.drop();
		}

		this.scopes.clear();
	}
}

const create = (directory?: string, options?: RustIndexerOptions) =>
	new RustIndices(directory, [], options);
export { create };
