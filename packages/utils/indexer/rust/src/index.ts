import { BinaryWriter, deserialize, serialize } from "@dao-xyz/borsh";
import * as types from "@peerbit/indexer-interface";
import { logger as loggerFn } from "@peerbit/logger";
import { equals } from "uint8arrays";
import { createSnapshotFile, type SnapshotFile } from "./persistence.js";

const logger = loggerFn("peerbit:indexer:rust");
const warn = logger.newScope("warn");

type NativeIndexStore<T extends Record<string, any>> = {
	put: (key: string, id: types.IdKey, value: T) => void;
	get: (key: string) => [types.IdKey, T] | undefined;
	delete: (key: string) => boolean;
	clear: () => void;
	len: () => number;
	entries: () => Array<[types.IdKey, T]>;
};

type NativeQueryPlanner = {
	put_document: (id: string, fields: Uint8Array) => void;
	delete_document: (id: string) => void;
	clear: () => void;
	len: () => number;
	query: (query: Uint8Array, sort: Uint8Array) => string[];
	query_page: (
		query: Uint8Array,
		sort: Uint8Array,
		offset: number,
		limit: number,
	) => string[];
	count: (query: Uint8Array) => number;
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeIndexStore: new <T extends Record<string, any>>() => NativeIndexStore<T>;
	NativeQueryPlanner: new () => NativeQueryPlanner;
};

type NativeValue =
	| { type: "bool"; value: boolean }
	| { type: "i64"; value: string }
	| { type: "u64"; value: string }
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

type NativeCandidatePage = {
	compiled: NativeQueryCompileResult;
	sort: NativeSortField[];
	offset: number;
	limit?: number;
};

const BRIDGE_VERSION = 1;

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
			return { type: "u64", value: value.toString() };
		}
		if (value >= -9223372036854775808n && value <= 9223372036854775807n) {
			return { type: "i64", value: value.toString() };
		}
		return undefined;
	}
	if (!Number.isSafeInteger(value)) {
		return undefined;
	}
	return value >= 0
		? { type: "u64", value: value.toString() }
		: { type: "i64", value: value.toString() };
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
	path: string[] = [],
	facts: NativeFieldFact[] = [],
	state: NativeFactCollectorState = {
		seen: new WeakSet(),
		nextScope: 1,
	},
	scope = 0,
): NativeFieldFact[] => {
	if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
		if (path.length > 0) {
			const bytes =
				value instanceof Uint8Array
					? value
					: new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
			facts.push({
				scope,
				field: nativeFieldKey(path),
				value: { type: "string", value: bytesToNativeString(bytes) },
			});
			for (const byte of bytes) {
				const byteScope = state.nextScope++;
				facts.push({
					scope: byteScope,
					field: nativeFieldKey(path),
					value: { type: "u64", value: byte.toString() },
				});
			}
		}
		return facts;
	}

	const nativeValue = scalarToNativeValue(value);
	if (nativeValue) {
		if (path.length > 0) {
			facts.push({ scope, field: nativeFieldKey(path), value: nativeValue });
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
			collectNativeFieldFacts(item, path, facts, state, state.nextScope++);
		}
		return facts;
	}

	for (const [key, child] of Object.entries(value)) {
		collectNativeFieldFacts(child, [...path, key], facts, state, scope);
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

const encodeNativeFieldFacts = (facts: NativeFieldFact[]): Uint8Array => {
	const writer = new BinaryWriter();
	writer.u8(BRIDGE_VERSION);
	writer.u32(facts.length);
	for (const fact of facts) {
		writer.u32(fact.scope);
		writer.string(fact.field);
		writeNativeValue(writer, fact.value);
	}
	return writer.finalize();
};

const compileNativeQueries = (
	queries: types.Query[],
): NativeQueryCompileResult | undefined => {
	return compileNativeAnd(queries);
};

const compileNativeAnd = (
	queries: types.Query[],
): NativeQueryCompileResult | undefined => {
	const compiled: NativeQuerySpec[] = [];

	for (const query of queries) {
		const next = compileNativeQuery(query);
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
): NativeQueryCompileResult | undefined => {
	if (query instanceof types.And) {
		return compileNativeAnd(query.and);
	}
	if (query instanceof types.Or) {
		const compiled: NativeQuerySpec[] = [];
		for (const child of query.or) {
			const next = compileNativeQuery(child);
			if (!next) {
				return;
			}
			compiled.push(next.spec);
		}
		return { spec: { op: "or", queries: compiled }, exact: true };
	}
	if (query instanceof types.Not) {
		const child = compileNativeQuery(query.not);
		if (!child) {
			return;
		}
		return { spec: { op: "not", query: child.spec }, exact: true };
	}
	if (query instanceof types.BoolQuery) {
		return {
			spec: {
				op: "exact",
				field: nativeFieldKey(query.key),
				value: { type: "bool", value: query.value },
			},
			exact: true,
		};
	}
	if (query instanceof types.StringMatch) {
		return {
			spec: {
				op: "string",
				field: nativeFieldKey(query.key),
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
				field: nativeFieldKey(query.key),
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
					field: nativeFieldKey(query.key),
					value,
				},
				exact: true,
			};
		}
		return {
			spec: {
				op: "range",
				field: nativeFieldKey(query.key),
				compare: compareToNative(query.compare),
				value,
			},
			exact: true,
		};
	}
	if (query instanceof types.IsNull) {
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

const getBatchFromResults = <T extends Record<string, any>>(
	results: types.IndexedValue<T>[],
	wantedSize: number,
) => {
	const batch: types.IndexedValue<T>[] = [];
	for (const result of results) {
		batch.push(result);
		if (wantedSize <= batch.length) {
			break;
		}
	}
	results.splice(0, batch.length);
	return batch;
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
	private store?: NativeIndexStore<T>;
	private planner?: NativeQueryPlanner;
	private indexByArr!: string[];
	private properties!: types.IndexEngineInitProperties<T, NestedType>;
	private persistQueue: Promise<void> = Promise.resolve();
	private dirtyVersion = 0;
	private persistedVersion = 0;

	constructor(
		private readonly directory?: string,
		private readonly path: string[] = [],
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
		this.store = new wasm.NativeIndexStore<T>();
		this.planner = new wasm.NativeQueryPlanner();
		this.snapshotFile = await createSnapshotFile(
			this.directory,
			this.path,
			this.indexByArr,
		);
		if (this.snapshotFile) {
			for (const value of await this.snapshotFile.read(properties.schema)) {
				const id = types.toId(types.extractFieldValue(value, this.indexByArr));
				const storeKey = keyToStoreKey(id);
				this.store.put(storeKey, id, value);
				this.indexNativeDocument(storeKey, value);
			}
		}
		return this;
	}

	get(
		id: types.IdKey,
		_options?: { shape: types.Shape },
	): types.IndexedResult<T> | undefined {
		const value = this.getStore().get(keyToStoreKey(id));
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
		const storeKey = keyToStoreKey(id);
		this.getStore().put(storeKey, id, value);
		this.indexNativeDocument(storeKey, value);
		this.markDirty();
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
		const deleted: types.IdKey[] = [];
		for (const doc of await this.queryAll(query)) {
			const storeKey = keyToStoreKey(doc.id);
			if (this.getStore().delete(storeKey)) {
				this.planner?.delete_document(storeKey);
				deleted.push(doc.id);
			}
		}
		if (deleted.length > 0) {
			this.markDirty();
		}
		return deleted;
	}

	getSize(): number {
		return this.getStore().len();
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
		await this.persistSnapshot();
	}

	async drop(): Promise<void> {
		this.store?.clear();
		this.planner?.clear();
		this.persistedVersion = this.dirtyVersion;
		await this.snapshotFile?.remove();
	}

	async sum(query: types.SumOptions): Promise<number | bigint> {
		let sum: undefined | number | bigint = undefined;
		outer: for (const doc of await this.queryAll(query)) {
			let value: any = doc.value;
			for (const path of Array.isArray(query.key) ? query.key : [query.key]) {
				value = value[path];
				if (!value) {
					continue outer;
				}
			}

			if (typeof value === "number") {
				sum = ((sum as unknown as number) || 0) + value;
			} else if (typeof value === "bigint") {
				sum = ((sum as unknown as bigint) || 0n) + value;
			}
		}
		return sum != null ? sum : 0;
	}

	async count(query?: types.CountOptions): Promise<number> {
		const queryCoerced = types.toQuery(query?.query);
		if (queryCoerced.length === 0) {
			return this.getSize();
		}
		const nativeCount = this.countNativeExact(queryCoerced);
		if (nativeCount != null) {
			return nativeCount;
		}
		return (await this.queryAll(query)).length;
	}

	private async queryAll(
		query?:
			| types.IterateOptions
			| types.DeleteOptions
			| types.CountOptions
			| types.SumOptions,
	): Promise<types.IndexedValue<T>[]> {
		const queryCoerced = types.toQuery(query?.query);
		if (
			queryCoerced.length === 1 &&
			(queryCoerced[0] instanceof types.ByteMatchQuery ||
				queryCoerced[0] instanceof types.StringMatch) &&
			types.stringArraysEquals(queryCoerced[0].key, this.indexByArr)
		) {
			const firstQuery = queryCoerced[0];
			if (firstQuery instanceof types.ByteMatchQuery) {
				const doc = this.getStore().get(
					keyToStoreKey(types.toId(firstQuery.value)),
				);
				return doc ? [{ id: doc[0], value: doc[1] }] : [];
			} else if (
				firstQuery instanceof types.StringMatch &&
				firstQuery.method === types.StringMatchMethod.exact &&
				firstQuery.caseInsensitive === false
			) {
				const doc = this.getStore().get(
					keyToStoreKey(types.toId(firstQuery.value)),
				);
				return doc ? [{ id: doc[0], value: doc[1] }] : [];
			}
		}

		const nativeResults = this.getNativeCandidates(queryCoerced);
		if (nativeResults) {
			return nativeResults;
		}

		const indexedDocuments = await this.queryDocuments(
			async (doc) => {
				const innerHits = new Map();
				for (const f of queryCoerced) {
					if (!(await this.handleQueryObject(f, doc.value, innerHits))) {
						return false;
					}
				}
				return true;
			},
		);

		return indexedDocuments;
	}

	iterate<S extends types.Shape | undefined>(
		query?: types.IterateOptions,
		properties?: { shape?: S; reference?: boolean },
	): types.IndexIterator<T, S> {
		const nativePagePlan = this.getNativePagePlan(types.toQuery(query?.query), query);
		if (nativePagePlan) {
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

		let done: boolean | undefined = undefined;
		let queue:
			| {
					arr: types.IndexedValue<T>[];
					reference: boolean | undefined;
			  }
			| undefined = undefined;
		const fetch = async (
			n: number,
		): Promise<types.IndexedResults<types.ReturnTypeFromShape<T, S>>> => {
			if (!queue && !done) {
				const indexedDocuments = await this.queryAll(query);
				if (indexedDocuments.length > 1) {
					if (query?.sort) {
						const sortArr = Array.isArray(query.sort)
							? query.sort
							: [query.sort];
						sortArr.length > 0 &&
							indexedDocuments.sort((a, b) =>
								types.extractSortCompare(a.value, b.value, sortArr),
							);
					}
				}

				if (indexedDocuments.length > 0) {
					queue = {
						arr: indexedDocuments,
						reference: properties?.reference,
					};
					done = false;
				} else {
					done = true;
				}
			}
			if (queue && queue.arr.length <= n) {
				done = true;
			}

			if (!queue) {
				return [];
			}

			const batch = getBatchFromResults<T>(queue.arr, n);

			return (
				queue.reference ? batch : cloneResults(batch, this.properties.schema)
			) as types.IndexedResults<types.ReturnTypeFromShape<T, S>>;
		};

		return {
			all: async () => {
				const results = await fetch(Infinity);
				return results;
			},
			next: (n: number) => fetch(n),
			done: () => done,
			pending: async () => {
				if (done == null) {
					await fetch(0);
				}
				return done ? 0 : (queue?.arr.length ?? 0);
			},
			close: () => {
				done = true;
				queue = undefined;
			},
		};
	}

	private async handleFieldQuery(
		f: types.StateFieldQuery,
		obj: any,
		skipKeys: number,
		innerHits: Map<string, any[] | false>,
		buildInnerHits = true,
	): Promise<boolean | undefined> {
		const handleArrayResults = async (
			path: string[],
			obj: any[] | Uint8Array,
			skipKeys: number,
		): Promise<boolean> => {
			const pathKey = buildInnerHits ? path.join(".") : undefined;
			const innerHitsValue = pathKey ? innerHits.get(pathKey) : undefined;
			if (pathKey && innerHitsValue === false) {
				return false;
			}

			const fromInnerHits = pathKey;
			const objArr =
				fromInnerHits && innerHitsValue && (innerHitsValue as []).length > 0
					? (innerHitsValue as any[])
					: obj;
			const newInnerHits: any[] | undefined = fromInnerHits ? [] : undefined;
			for (const element of objArr!) {
				if (await this.handleFieldQuery(f, element, skipKeys, innerHits)) {
					if (!buildInnerHits) {
						return true;
					}
					newInnerHits!.push(element);
				}
			}
			if (!fromInnerHits) {
				return false;
			}

			if (newInnerHits!.length === 0) {
				innerHits.set(pathKey!, false);
				return false;
			}

			innerHits.set(pathKey!, newInnerHits!);
			return true;
		};

		if (
			Array.isArray(obj) ||
			(obj instanceof Uint8Array && f instanceof types.ByteMatchQuery === false)
		) {
			return handleArrayResults(f.key, obj, skipKeys);
		}

		for (let i = skipKeys; i < f.key.length; i++) {
			obj = obj[f.key[i]];
			if (
				Array.isArray(obj) ||
				(obj instanceof Uint8Array &&
					f instanceof types.ByteMatchQuery === false)
			) {
				return handleArrayResults(f.key.slice(0, i + 1), obj, i + 1);
			}
			if (this.properties.nested?.match(obj)) {
				const queryCloned = f.clone();
				queryCloned.key.splice(0, i + 1);
				const results = await this.properties.nested.iterate(obj, {
					query: [queryCloned],
				});
				return results.length > 0 ? true : false;
			}
		}

		if (f instanceof types.IsNull) {
			if (obj == null) {
				return true;
			}
			return false;
		}

		if (obj == null) {
			return undefined;
		}

		if (f instanceof types.StringMatch) {
			let compare = f.value;
			if (f.caseInsensitive) {
				compare = compare.toLowerCase();
			}

			if (this.handleStringMatch(f, compare, obj)) {
				return true;
			}
			return false;
		} else if (f instanceof types.ByteMatchQuery) {
			if (obj instanceof Uint8Array === false) {
				if (types.stringArraysEquals(f.key, this.indexByArr)) {
					return f.valueString === obj;
				}
				return false;
			}
			return equals(obj as Uint8Array, f.value);
		} else if (f instanceof types.IntegerCompare) {
			const value: bigint | number = obj as any as bigint | number;

			if (typeof value !== "bigint" && typeof value !== "number") {
				return false;
			}
			return types.compare(value, f.compare, f.value.value);
		} else if (f instanceof types.BoolQuery) {
			return obj === f.value;
		}
		warn("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private async handleQueryObject(
		f: types.Query,
		value: Record<string, any> | T,
		innerHits: Map<string, any[]>,
		skipKeys = 0,
	): Promise<{ result: true; innerHits: any[] } | boolean | undefined> {
		if (f instanceof types.StateFieldQuery) {
			return this.handleFieldQuery(f, value as T, skipKeys, innerHits);
		} else if (f instanceof types.Nested) {
			let arr = value;

			for (let i = skipKeys; i < f.path.length; i++) {
				arr = arr[f.path[i]];
			}

			if (!Array.isArray(arr)) {
				throw new Error("Nested field is not an array");
			}

			const newSkipKeys = skipKeys + f.path.length;
			outer: for (const element of arr) {
				for (const query of f.query) {
					if (
						!(await this.handleQueryObject(
							query,
							element,
							innerHits,
							newSkipKeys,
						))
					) {
						continue outer;
					}
				}
				return true;
			}
			return false;
		} else if (f instanceof types.LogicalQuery) {
			if (f instanceof types.And) {
				for (const and of f.and) {
					const ret = await this.handleQueryObject(
						and,
						value,
						innerHits,
						skipKeys,
					);
					if (!ret) {
						return ret;
					}
				}
				return true;
			}

			if (f instanceof types.Or) {
				for (const or of f.or) {
					const innerHits = new Map();
					const ret = await this.handleQueryObject(
						or,
						value,
						innerHits,
						skipKeys,
					);
					if (ret === true) {
						return true;
					} else if (ret === undefined) {
						return undefined;
					}
				}
				return false;
			}
			if (f instanceof types.Not) {
				const ret = await this.handleQueryObject(
					f.not,
					value,
					innerHits,
					skipKeys,
				);
				if (ret === undefined) {
					return undefined;
				}
				return !ret;
			}
		}

		logger("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private handleStringMatch(f: types.StringMatch, compare: string, fv: string) {
		if (typeof fv !== "string") {
			return false;
		}
		if (f.caseInsensitive) {
			fv = fv.toLowerCase();
		}
		if (f.method === types.StringMatchMethod.exact) {
			return fv === compare;
		}
		if (f.method === types.StringMatchMethod.prefix) {
			return fv.startsWith(compare);
		}
		if (f.method === types.StringMatchMethod.contains) {
			return fv.includes(compare);
		}
		throw new Error("Unsupported");
	}

	private async queryDocuments(
		filter: (doc: types.IndexedValue<T>) => Promise<boolean>,
		documents = this.snapshot(),
	): Promise<types.IndexedValue<T>[]> {
		const results: types.IndexedValue<T>[] = [];
		for (const value of documents) {
			if (await filter(value)) {
				results.push(value);
			}
		}
		return results;
	}

	private getNativeCandidates(
		queryCoerced: types.Query[],
	): types.IndexedValue<T>[] | undefined {
		const compiled = this.getNativePlan(queryCoerced);
		if (!compiled) {
			return;
		}
		return this.getNativeCandidatesForPlan({ compiled, sort: [], offset: 0 });
	}

	private getNativePlan(
		queryCoerced: types.Query[],
		options?: { allowAll?: boolean },
	): NativeQueryCompileResult | undefined {
		if (!this.planner) {
			return;
		}
		const compiled = compileNativeQueries(queryCoerced);
		if (!compiled || (compiled.spec.op === "all" && !options?.allowAll)) {
			return;
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

	private countNativeExact(queryCoerced: types.Query[]): number | undefined {
		const compiled = this.getNativePlan(queryCoerced);
		if (!compiled?.exact) {
			return;
		}
		return this.countNativePlan(compiled);
	}

	private countNativePlan(
		compiled: NativeQueryCompileResult,
	): number | undefined {
		if (!this.planner) {
			return;
		}
		return this.planner.count(encodeNativeQuerySpec(compiled.spec));
	}

	private getNativeCandidatesForPlan(
		page: NativeCandidatePage,
	): types.IndexedValue<T>[] {
		const results: types.IndexedValue<T>[] = [];
		const queryBytes = encodeNativeQuerySpec(page.compiled.spec);
		const sortBytes = encodeNativeSort(page.sort);
		const storeKeys =
			page.limit == null
				? this.planner!.query(queryBytes, sortBytes)
				: this.planner!.query_page(queryBytes, sortBytes, page.offset, page.limit);
		for (const storeKey of storeKeys) {
			const value = this.getStore().get(storeKey);
			if (value) {
				results.push({ id: value[0], value: value[1] });
			}
		}
		return results;
	}

	private indexNativeDocument(storeKey: string, value: T): void {
		this.planner?.put_document(
			storeKey,
			encodeNativeFieldFacts(collectNativeFieldFacts(value)),
		);
	}

	private snapshot(): types.IndexedValue<T>[] {
		return this.getStore()
			.entries()
			.map((entry) => {
				return { id: entry[0], value: entry[1] };
			});
	}

	private getStore(): NativeIndexStore<T> {
		if (!this.store) {
			throw new Error("Index has not been initialized");
		}
		return this.store;
	}

	private async persistSnapshot(): Promise<void> {
		if (
			!this.snapshotFile ||
			!this.store ||
			this.persistedVersion === this.dirtyVersion
		) {
			return;
		}
		const version = this.dirtyVersion;
		const persist = async () => {
			if (this.persistedVersion >= version) {
				return;
			}
			await this.snapshotFile!.write(
				this.snapshot().map((entry) => entry.value),
				this.properties.schema,
			);
		};
		const next = this.persistQueue.then(persist, persist);
		this.persistQueue = next.then(
			() => {
				this.persistedVersion = Math.max(this.persistedVersion, version);
			},
			() => undefined,
		);
		await next;
	}

	private markDirty(): void {
		this.dirtyVersion++;
	}
}

export class RustIndices implements types.Indices {
	private scopes: Map<string, types.Indices>;
	private indices: { schema: any; index: RustIndex<any, any> }[] = [];
	private closed: boolean;

	constructor(
		private readonly directory?: string,
		private readonly path: string[] = [],
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
		const index = new RustIndex<T, NestedType>(this.directory, this.path);
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
			scope = new RustIndices(this.directory, [...this.path, name]);
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

const create = (directory?: string) => new RustIndices(directory);
export { create };
