import { deserialize, serialize } from "@dao-xyz/borsh";
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

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeIndexStore: new <T extends Record<string, any>>() => NativeIndexStore<T>;
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
		this.snapshotFile = await createSnapshotFile(
			this.directory,
			this.path,
			this.indexByArr,
		);
		if (this.snapshotFile) {
			for (const value of await this.snapshotFile.read(properties.schema)) {
				const id = types.toId(types.extractFieldValue(value, this.indexByArr));
				this.store.put(keyToStoreKey(id), id, value);
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
		this.getStore().put(keyToStoreKey(id), id, value);
		this.markDirty();
	}

	async del(query: types.DeleteOptions): Promise<types.IdKey[]> {
		const deleted: types.IdKey[] = [];
		for (const doc of await this.queryAll(query)) {
			if (this.getStore().delete(keyToStoreKey(doc.id))) {
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

		const indexedDocuments = await this.queryDocuments(async (doc) => {
			const innerHits = new Map();
			for (const f of queryCoerced) {
				if (!(await this.handleQueryObject(f, doc.value, innerHits))) {
					return false;
				}
			}
			return true;
		});

		return indexedDocuments;
	}

	iterate<S extends types.Shape | undefined>(
		query?: types.IterateOptions,
		properties?: { shape?: S; reference?: boolean },
	): types.IndexIterator<T, S> {
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
	): Promise<types.IndexedValue<T>[]> {
		const results: types.IndexedValue<T>[] = [];
		for (const value of this.snapshot()) {
			if (await filter(value)) {
				results.push(value);
			}
		}
		return results;
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
