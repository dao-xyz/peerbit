import { BinaryWriter, deserialize, serialize } from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";
import {
	createSnapshotFile,
	type PersistenceOptions,
	type SnapshotFile,
} from "./persistence.js";

export type RustIndexerOptions = {
	persistence?: PersistenceOptions;
};

type NativeRustIndex<T extends Record<string, any>> = {
	put: (
		key: string,
		id: types.IdKey,
		value: T,
		fields: Uint8Array,
	) => void;
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
	count: (query: Uint8Array) => number;
	sum: (query: Uint8Array, field: string) => [NativeSumKind, string];
	delete_matching: (query: Uint8Array) => Array<[types.IdKey, T]>;
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
	| { type: "string"; value: string };

type NativeFieldFact = {
	scope: number;
	field: string;
	value: NativeValue;
};

type NativeQuerySpec =
	| { op: "all" }
	| { op: "exact"; field: string; value: NativeValue }
	| {
			op: "range";
			field: string;
			compare: "eq" | "gt" | "gte" | "lt" | "lte";
			value: NativeValue;
	  }
	| { op: "and"; queries: NativeQuerySpec[] }
	| { op: "or"; queries: NativeQuerySpec[] }
	| { op: "not"; query: NativeQuerySpec }
	| {
			op: "string";
			field: string;
			value: string;
			method: "exact" | "prefix" | "contains";
			caseInsensitive: boolean;
	  }
	| { op: "is_null"; field: string };

type NativeQueryCompileResult = {
	spec: NativeQuerySpec;
	exact: boolean;
};

type NativeSortField = {
	field: string;
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

// Keep bridge enum tags in sync with the Rust Borsh DTO declaration order.
const enum NativeValueTag {
	Bool = 0,
	I64 = 1,
	U64 = 2,
	String = 3,
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

const textEncoder = new TextEncoder();
const nativeFieldBytes = new Map<string, Uint8Array>();

const getNativeFieldBytes = (field: string): Uint8Array => {
	let bytes = nativeFieldBytes.get(field);
	if (!bytes) {
		bytes = textEncoder.encode(field);
		nativeFieldBytes.set(field, bytes);
	}
	return bytes;
};

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

const keyToStoreKey = (id: types.IdKey): string => {
	const key = types.toIdeable(id);
	if (key instanceof Uint8Array || ArrayBuffer.isView(key)) {
		return `bytes:${id.primitive.toString()}`;
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

const bytesToNativeString = (bytes: Uint8Array): string => {
	let hex = "";
	for (const byte of bytes) {
		hex += byte.toString(16).padStart(2, "0");
	}
	return `\u0000bytes:${hex}`;
};

const nativeIntegerValue = (value: number | bigint): NativeValue | undefined => {
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

const scalarToNativeValue = (value: any): NativeValue | undefined => {
	if (typeof value === "boolean") {
		return { type: "bool", value };
	}
	if (typeof value === "string") {
		return { type: "string", value };
	}
	if (typeof value === "number" || typeof value === "bigint") {
		return nativeIntegerValue(value);
	}
	if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
		const bytes =
			value instanceof Uint8Array
				? value
				: new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
		return { type: "string", value: bytesToNativeString(bytes) };
	}
	return undefined;
};

type NativeFactCollectorState = {
	seen: WeakSet<object>;
	nextScope: number;
};

const collectNativeFieldFacts = (
	value: any,
	field: string | undefined = undefined,
	facts: NativeFieldFact[] = [],
	state: NativeFactCollectorState = {
		seen: new WeakSet(),
		nextScope: 1,
	},
	scope = 0,
): NativeFieldFact[] => {
	if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
		if (field) {
			const bytes =
				value instanceof Uint8Array
					? value
					: new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
			facts.push({
				scope,
				field,
				value: { type: "string", value: bytesToNativeString(bytes) },
			});
			for (const byte of bytes) {
				const byteScope = state.nextScope++;
				facts.push({
					scope: byteScope,
					field,
					value: { type: "u64", value: byte },
				});
			}
		}
		return facts;
	}

	const nativeValue = scalarToNativeValue(value);
	if (nativeValue) {
		if (field) {
			facts.push({ scope, field, value: nativeValue });
		}
		return facts;
	}

	if (!value || typeof value !== "object") {
		return facts;
	}
	if (state.seen.has(value)) {
		return facts;
	}
	state.seen.add(value);

	if (Array.isArray(value)) {
		for (const item of value) {
			const itemScope = state.nextScope++;
			if (field) {
				facts.push({
					scope: itemScope,
					field: nativeArrayElementFieldKeyFromFieldKey(field),
					value: { type: "bool", value: true },
				});
			}
			collectNativeFieldFacts(item, field, facts, state, itemScope);
		}
		return facts;
	}

	for (const key in value) {
		if (!Object.prototype.hasOwnProperty.call(value, key)) {
			continue;
		}
		collectNativeFieldFacts(
			value[key],
			appendNativeFieldKey(field, key),
			facts,
			state,
			scope,
		);
	}
	return facts;
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
			writer.string(query.field);
			writeNativeValue(writer, query.value);
			return;
		case "range":
			writer.u8(NativeQueryTag.Range);
			writer.string(query.field);
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
			writer.string(query.field);
			writer.string(query.value);
			writer.u8(nativeStringMatchMethodTag(query.method));
			writer.bool(query.caseInsensitive);
			return;
		case "is_null":
			writer.u8(NativeQueryTag.IsNull);
			writer.string(query.field);
			return;
	}
};

const encodeNativeQuerySpec = (query: NativeQuerySpec): Uint8Array => {
	const writer = new BinaryWriter();
	writer.u8(BRIDGE_VERSION);
	writeNativeQuerySpec(writer, query);
	return writer.finalize();
};

const nativeSortFields = (sort?: types.Sort | types.Sort[]): NativeSortField[] =>
	types.toSort(sort).map((field) => ({
		field: nativeFieldKey(field.key),
		direction:
			field.direction === types.SortDirection.ASC ? "asc" : "desc",
	}));

const encodeNativeSort = (sort: NativeSortField[] = []): Uint8Array => {
	const writer = new BinaryWriter();
	writer.u8(BRIDGE_VERSION);
	writer.u32(sort.length);
	for (const field of sort) {
		writer.string(field.field);
		writer.u8(nativeSortDirectionTag(field.direction));
	}
	return writer.finalize();
};

type EncodedNativeFieldFact = {
	fact: NativeFieldFact;
	fieldBytes: Uint8Array;
	stringBytes?: Uint8Array;
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

const encodeNativeFieldFacts = (facts: NativeFieldFact[]): Uint8Array => {
	const encodedFacts: EncodedNativeFieldFact[] = [];
	let totalSize = 1 + 4;
	for (const fact of facts) {
		const encoded: EncodedNativeFieldFact = {
			fact,
			fieldBytes: getNativeFieldBytes(fact.field),
		};
		totalSize += 4 + 4 + encoded.fieldBytes.byteLength + 1;
		switch (fact.value.type) {
			case "bool":
				totalSize += 1;
				break;
			case "i64":
			case "u64":
				totalSize += 8;
				break;
			case "string":
				encoded.stringBytes = textEncoder.encode(fact.value.value);
				totalSize += 4 + encoded.stringBytes.byteLength;
				break;
		}
		encodedFacts.push(encoded);
	}

	const output = new Uint8Array(totalSize);
	const view = new DataView(output.buffer, output.byteOffset, output.byteLength);
	let offset = 0;
	output[offset++] = BRIDGE_VERSION;
	offset = writeUint32(view, offset, encodedFacts.length);
	for (const { fact, fieldBytes, stringBytes } of encodedFacts) {
		offset = writeUint32(view, offset, fact.scope);
		offset = writeBytes(output, view, offset, fieldBytes);
		switch (fact.value.type) {
			case "bool":
				output[offset++] = NativeValueTag.Bool;
				output[offset++] = fact.value.value ? 1 : 0;
				break;
			case "i64":
				output[offset++] = NativeValueTag.I64;
				offset = writeUint64(
					view,
					offset,
					BigInt.asUintN(64, BigInt(fact.value.value)),
				);
				break;
			case "u64":
				output[offset++] = NativeValueTag.U64;
				offset = writeUint64(view, offset, fact.value.value);
				break;
			case "string":
				output[offset++] = NativeValueTag.String;
				offset = writeBytes(output, view, offset, stringBytes!);
				break;
		}
	}
	return output;
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
	prefix: string[] = [],
): NativeQueryCompileResult | undefined => {
	return compileNativeAnd(queries, prefix);
};

const compileNativeAnd = (
	queries: types.Query[],
	prefix: string[] = [],
): NativeQueryCompileResult | undefined => {
	const compiled: NativeQuerySpec[] = [];

	for (const query of queries) {
		const next = compileNativeQuery(query, prefix);
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
	prefix: string[] = [],
): NativeQueryCompileResult | undefined => {
	if (query instanceof types.And) {
		return compileNativeAnd(query.and, prefix);
	}
	if (query instanceof types.Or) {
		const compiled: NativeQuerySpec[] = [];
		for (const child of query.or) {
			const next = compileNativeQuery(child, prefix);
			if (!next) {
				return;
			}
			compiled.push(next.spec);
		}
		return { spec: { op: "or", queries: compiled }, exact: true };
	}
	if (query instanceof types.Not) {
		const child = compileNativeQuery(query.not, prefix);
		if (!child) {
			return;
		}
		return { spec: { op: "not", query: child.spec }, exact: true };
	}
	if (query instanceof types.Nested) {
		const nestedPath = [...prefix, ...query.path];
		const nested = compileNativeAnd(query.query, nestedPath);
		if (!nested) {
			return;
		}
		const arrayElementMarker: NativeQuerySpec = {
			op: "exact",
			field: nativeArrayElementFieldKey(nestedPath),
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
				field: nativeFieldKey([...prefix, ...query.key]),
				value: { type: "bool", value: query.value },
			},
			exact: true,
		};
	}
	if (query instanceof types.StringMatch) {
		return {
			spec: {
				op: "string",
				field: nativeFieldKey([...prefix, ...query.key]),
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
				field: nativeFieldKey([...prefix, ...query.key]),
				value: { type: "string", value: bytesToNativeString(query.value) },
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
					field: nativeFieldKey([...prefix, ...query.key]),
					value,
				},
				exact: true,
			};
		}
		return {
			spec: {
				op: "range",
				field: nativeFieldKey([...prefix, ...query.key]),
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
				field: nativeFieldKey(query.key),
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

	async put(
		value: T,
		id = types.toId(types.extractFieldValue(value, this.indexByArr)),
		_options?: { replace?: boolean },
	): Promise<void> {
		await this.enqueueMutation(async () => {
			const storeKey = keyToStoreKey(id);
			const fields = encodeNativeFieldFacts(collectNativeFieldFacts(value));
			if (this.snapshotFile) {
				await this.appendPut(storeKey, value);
			}
			this.getNative().put(storeKey, id, value, fields);
			if (this.snapshotFile) {
				await this.compactIfNeeded();
			}
		});
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
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
				if (this.snapshotFile) {
					await this.appendDeletes(
						deletedEntries.map((entry) => keyToStoreKey(entry.id)),
					);
				}
				this.getNative().delete_matching(encodeNativeQuerySpec(compiled.spec));
				if (this.snapshotFile) {
					await this.compactIfNeeded();
				}
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
		const field = nativeFieldKey(Array.isArray(query.key) ? query.key : [query.key]);
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
		const compiled = compileNativeQueries(queryCoerced);
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
			sort: nativeSortFields(query?.sort),
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

	private putNativeDocument(storeKey: string, id: types.IdKey, value: T): void {
		this.getNative().put(
			storeKey,
			id,
			value,
			encodeNativeFieldFacts(collectNativeFieldFacts(value)),
		);
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

	private appendPut(storeKey: string, value: T): Promise<void> {
		return this.enqueuePersistence(() =>
			this.snapshotFile!.appendPut(storeKey, value, this.properties.schema),
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
