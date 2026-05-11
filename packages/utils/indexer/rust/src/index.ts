import {
	BinaryWriter,
	FixedArrayKind,
	OptionKind,
	StringType,
	VecKind,
	deserialize,
	getSchemasBottomUp,
	serialize,
	type FieldType,
} from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";
import {
	createSnapshotFile,
	type EncodedValue,
	type PersistenceOptions,
	type SnapshotFile,
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
	put: (
		key: string,
		id: types.IdKey,
		value: T,
		fields: Uint8Array,
	) => void;
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
	get: (key: string) => [types.IdKey, T] | undefined;
	clear: () => void;
	len: () => number;
	entries: () => Array<[types.IdKey, T]>;
	query: (
		query: Uint8Array,
		sort: Uint8Array,
	) => Array<[types.IdKey, T]>;
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
		wasmModulePromise = import(wasmModulePath) as Promise<WasmModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		const processLike = (globalThis as { process?: { versions?: { node?: string } } })
			.process;
		if (processLike?.versions?.node) {
			const fsPromises = "fs/promises";
			const { readFile } = (await import(fsPromises)) as typeof import("fs/promises");
			const bytes = await readFile(
				new URL("../wasm/indexer_rust_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL("../wasm/indexer_rust_bg.wasm", import.meta.url),
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
	return value >= 0
		? { type: "u64", value }
		: { type: "i64", value };
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
		direction:
			field.direction === types.SortDirection.ASC ? "asc" : "desc",
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

const writeUint32 = (
	view: DataView,
	offset: number,
	value: number,
): number => {
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

	writeBytesValue(scope: number, fieldId: number, valueBytes: Uint8Array): void {
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

type NativeFieldEncoder<T extends Record<string, any>> = (value: T) => Uint8Array;
type NativeSchemaIrStats = {
	rootFields: number;
	nodeCount: number;
	genericNodes: number;
};
type SharedLogCoordinateNativeFields = {
	hash: string;
	hashNumber: number | bigint;
	gid: string;
	coordinates: Array<number | bigint>;
	wallTime: number | bigint;
	assignedToRangeBoundary: boolean;
	metaBytes: Uint8Array;
};

type NativeEncodedValueParts = {
	prefix: Uint8Array;
	suffix: Uint8Array;
};

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
	return Math.max(0, Math.min(MAX_NATIVE_BYTE_ELEMENT_INDEX_LIMIT, Math.floor(limit)));
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
		writeNativeSchemaNode(
			writer,
			field.type,
			dictionary,
			fieldCursor,
			active,
		);
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

		const objectWriter = nativeFieldValueWriter((value, writer, state, scope) => {
			if (!value || typeof value !== "object") {
				return;
			}
			if (needsSeen && !markNativeFieldSeen(state, value)) {
				return;
			}
			for (const field of fields) {
				field.write(value[field.key], writer, state, scope);
			}
		}, needsSeen);
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

const decodeNativeSum = ([kind, value]: [NativeSumKind, string]): number | bigint => {
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

	constructor(
		private readonly directory?: string,
		private readonly path: string[] = [],
		private readonly options: RustIndexerOptions = {},
	) {}

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
		const [rootFields, nodeCount, genericNodes] = this.native.configure_schema_ir(
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
			for (const value of (await this.snapshotFile.read(properties.schema)) as T[]) {
				const id = types.toId(types.extractFieldValue(value, this.indexByArr));
				const storeKey = keyToStoreKey(id);
				this.putNativeDocument(storeKey, id, value);
			}
		}
		return this;
	}

	get(
		id: types.IdKey,
		_options?: { shape: types.Shape },
	): types.IndexedResult<T> | undefined {
		const value = this.getNative().get(keyToStoreKey(id));
		if (!value) {
			return;
		}
		return {
			id,
			value: value[1],
		};
	}

	getByContextHead(head: string): types.IndexedResult<T> | undefined {
		const result = this.getNative().query_page(
			encodeNativeQuerySpec({
				op: "exact",
				field: nativeFieldId(this.fieldDictionary, ["__context", "head"]),
				value: { type: "string", value: head },
			}),
			encodeNativeSort(),
			0,
			1,
		)[0];
		return result ? { id: result[0], value: result[1] } : undefined;
	}

	getByContextHeadBatch(
		heads: string[],
	): Array<types.IndexedResult<T> | undefined> {
		if (heads.length === 0) {
			return [];
		}
		const native = this.getNative();
		const nativeBatch = native.query_exact_string_first_batch;
		if (!nativeBatch) {
			return heads.map((head) => this.getByContextHead(head));
		}
		const rows = nativeBatch.call(
			native,
			nativeFieldId(this.fieldDictionary, ["__context", "head"]),
			heads,
		);
		return rows.map((result) =>
			result ? { id: result[0], value: result[1] } : undefined,
		);
	}

	async put(
		value: T,
		id = types.toId(types.extractFieldValue(value, this.indexByArr)),
		_options?: { replace?: boolean },
	): Promise<void> {
		const encodedValue = this.tryNativeEncodedValue(value);
		if (encodedValue) {
			await this.putWithEncodedValue(value, id, encodedValue);
			return;
		}
		await this.putWithEncodedFields(value, id, this.fieldEncoder(value));
	}

	async putWithContext(
		value: Record<string, any>,
		id: types.IdKey,
		context: Record<string, any>,
		options?: NativeEncodedPutOptions,
	): Promise<void> {
		const contextualValue = this.asContextualValue(value, context);
		if (options?.encodedValueParts) {
			await this.putWithEncodedValueParts(
				contextualValue,
				id,
				options.encodedValueParts,
			);
			return;
		}
		if (options?.encodedValue) {
			await this.putWithEncodedValue(contextualValue, id, options.encodedValue);
			return;
		}
		await this.put(contextualValue, id, options);
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
		if (!this.snapshotFile) {
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
		const compiled = this.requireNativePlan(types.toQuery(deleteOptions.query), {
			allowAll: true,
		});
		const storeKey = keyToStoreKey(id);
		const fields = this.fieldEncoder(value);
		const queryBytes = encodeNativeQuerySpec(compiled.spec);
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

	async putSharedLogCoordinateAndDeleteIds(
		value: T,
		fields: SharedLogCoordinateNativeFields,
		deleteIds: Array<types.IdKey | types.Ideable> = [],
		id = types.toId(types.extractFieldValue(value, this.indexByArr)),
	): Promise<types.IdKey[]> {
		return this.putWithEncodedFieldsAndDeleteKeys(
			value,
			id,
			this.encodeSharedLogCoordinateFields(fields),
			deleteIds.map(keyToStoreKey),
		);
	}

	async putSharedLogCoordinatesAndDeleteIdsBatch(
		values: Array<{
			value: T;
			fields: SharedLogCoordinateNativeFields;
			deleteIds?: Array<types.IdKey | types.Ideable>;
			id?: types.IdKey;
		}>,
	): Promise<types.IdKey[]> {
		if (values.length === 0) {
			return [];
		}
		const prepared = values.map((entry) => {
			const id =
				entry.id ?? types.toId(types.extractFieldValue(entry.value, this.indexByArr));
			return {
				value: entry.value,
				id,
				storeKey: keyToStoreKey(id),
				fields: this.encodeSharedLogCoordinateFields(entry.fields),
				deleteKeys: (entry.deleteIds ?? []).map(keyToStoreKey),
			};
		});

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
			await this.enqueuePersistence(async () => {
				for (const entry of prepared) {
					await this.snapshotFile!.appendPut(
						entry.storeKey,
						entry.value,
						this.properties.schema,
					);
					for (const deleteKey of entry.deleteKeys) {
						await this.snapshotFile!.appendDelete(deleteKey);
					}
				}
			});
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

	async delIds(deleteIds: Array<types.IdKey | types.Ideable>): Promise<types.IdKey[]> {
		const deleteKeys = deleteIds.map(keyToStoreKey);
		if (deleteKeys.length === 0) {
			return [];
		}
		if (!this.snapshotFile) {
			return this.getNative()
				.delete_keys(deleteKeys)
				.map((entry) => entry[0]);
		}
		return this.enqueueMutation(async () => {
			await this.appendDeletes(deleteKeys);
			const deletedEntries = this.getNative().delete_keys(deleteKeys);
			await this.compactIfNeeded();
			return deletedEntries.map((entry) => entry[0]);
		});
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
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
				this.getNative().delete_matching(encodeNativeQuerySpec(compiled.spec));
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
				await this.appendDeletes(
					deletedEntries.map((entry) => keyToStoreKey(entry.id)),
				);
				this.getNative().delete_matching(encodeNativeQuerySpec(compiled.spec));
				await this.compactIfNeeded();
			}
			return deletedEntries.map((entry) => entry.id);
		});
	}

	getSize(): number {
		return this.getNative().len();
	}

	persisted(): boolean {
		return Boolean(this.snapshotFile);
	}

	iterator() {
		return this.snapshot()[Symbol.iterator]();
	}

	start(): void {
		// The wasm module is initialized during init.
	}

	async stop(): Promise<void> {
		await this.mutationQueue;
		await this.compactPersistence();
	}

	async drop(): Promise<void> {
		await this.mutationQueue;
		this.native?.clear();
		await this.snapshotFile?.remove();
	}

	async sum(query: types.SumOptions): Promise<number | bigint> {
		const compiled = this.requireNativePlan(types.toQuery(query.query), {
			allowAll: true,
		});
		const field = nativeFieldId(
			this.fieldDictionary,
			Array.isArray(query.key) ? query.key : [query.key],
		);
		return decodeNativeSum(
			this.getNative().sum(encodeNativeQuerySpec(compiled.spec), field),
		);
	}

	async count(query?: types.CountOptions): Promise<number> {
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
			done: () => done,
			pending: async () => (done ? 0 : Math.max(0, getTotal() - offset)),
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
		return this.getNative().count(encodeNativeQuerySpec(compiled.spec));
	}

	private getNativeCandidatesForPlan(
		page: NativeCandidatePage,
	): types.IndexedValue<T>[] {
		const queryBytes = encodeNativeQuerySpec(page.compiled.spec);
		const sortBytes = encodeNativeSort(page.sort);
		const results =
			page.limit == null
				? this.getNative().query(queryBytes, sortBytes)
				: this.getNative().query_page(
						queryBytes,
						sortBytes,
						page.offset,
						page.limit,
					);
		return results.map((value) => ({ id: value[0], value: value[1] }));
	}

	private async putWithEncodedFields(
		value: T,
		id: types.IdKey,
		fields: Uint8Array,
	): Promise<void> {
		const storeKey = keyToStoreKey(id);
		if (!this.snapshotFile) {
			this.getNative().put(storeKey, id, value, fields);
			return;
		}
		await this.enqueueMutation(async () => {
			await this.appendPut(storeKey, value);
			this.getNative().put(storeKey, id, value, fields);
			await this.compactIfNeeded();
		});
	}

	private async putWithEncodedValue(
		value: T,
		id: types.IdKey,
		encodedValue: Uint8Array,
	): Promise<void> {
		const storeKey = keyToStoreKey(id);
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
		await this.enqueueMutation(async () => {
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

	private async putWithEncodedValueParts(
		value: T,
		id: types.IdKey,
		encodedValueParts: NativeEncodedValueParts,
	): Promise<void> {
		const storeKey = keyToStoreKey(id);
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
		await this.enqueueMutation(async () => {
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
	): Promise<types.IdKey[]> {
		if (deleteKeys.length === 0) {
			await this.putWithEncodedFields(value, id, fields);
			return [];
		}
		const storeKey = keyToStoreKey(id);
		if (!this.snapshotFile) {
			return this.getNative()
				.put_and_delete_keys(storeKey, id, value, fields, deleteKeys)
				.map((entry) => entry[0]);
		}

		return this.enqueueMutation(async () => {
			await this.appendPut(storeKey, value);
			await this.appendDeletes(deleteKeys);
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
		if (value.__context === context) {
			return value as T;
		}
		return this.createContextualValue(value, context);
	}

	private snapshot(): types.IndexedValue<T>[] {
		return this.getNative()
			.entries()
			.map((entry) => {
				return { id: entry[0], value: entry[1] };
			});
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
		value: T,
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

	private appendDeletes(storeKeys: string[]): Promise<void> {
		return this.enqueuePersistence(async () => {
			for (const storeKey of storeKeys) {
				await this.snapshotFile!.appendDelete(storeKey);
			}
		});
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
