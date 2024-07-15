import * as types from "@peerbit/indexer-interface";
import { Cache } from "@peerbit/cache";
import { equals } from "uint8arrays";
import { logger as loggerFn } from "@peerbit/logger";
import { serialize, deserialize } from "@dao-xyz/borsh";

const logger = loggerFn({ module: "simple-index-engine" });

const getBatchFromResults = <T extends Record<string, any>>(
	results: types.IndexedValue<T>[],
	wantedSize: number,
	/* properties: types.IteratorBatchProperties */
) => {

	const batch: types.IndexedValue<T>[] = [];
	/* let size = 0; */
	for (const result of results) {
		batch.push(result);
		/* 	size += types.extractFieldValue<number>(result, properties.sizeProperty);
			if (size > properties.maxSize) {
				break;
			} */
		if (wantedSize <= batch.length) {
			break;
		}
	}
	results.splice(0, batch.length);
	return batch;
};

/* const resolveNestedAliasesRecursively = (request: types.SearchRequest) => {
	const map = new Map();
	for (const query of request.query) {
		_resolveNestedAliasesRecursively(query, map);
	}
	return map;
}
 */
const cloneResults = <T>(indexed: types.IndexedValue<T>[], schema: any): types.IndexedValue<T>[] => {
	try {
		return indexed.map(x => { return { id: x.id, value: deserialize(serialize(x.value), schema) } })
	} catch (error) {
		throw error;
	}
}
/* 
const _resolveNestedAliasesRecursively = (query: types.Query, aliases: Map<string, string>) => {

	if (query instanceof types.Nested) {
		aliases.set(query.id, query.path);
		for (const subQuery of query.query) {
			_resolveNestedAliasesRecursively(subQuery, aliases);
		}
	}
	else if (query instanceof types.And) {
		for (const subQuery of query.and) {
			_resolveNestedAliasesRecursively(subQuery, aliases);
		}
	}
	else if (query instanceof types.Or) {
		for (const subQuery of query.or) {
			_resolveNestedAliasesRecursively(subQuery, aliases);
		}
	}
	else if (query instanceof types.Not) {
		_resolveNestedAliasesRecursively(query.not, aliases);
	}

}
 */


export class HashmapIndex<T extends Record<string, any>, NestedType = any>
	implements types.Index<T, NestedType> {

	private _index: Map<string | bigint | number, types.IndexedValue<T>>;
	private _resultsCollectQueue: Cache<{
		arr: types.IndexedValue<T>[];
		reference: boolean | undefined
	}>;

	private indexByArr: string[];
	private properties: types.IndexEngineInitProperties<T, NestedType>;

	init(properties: types.IndexEngineInitProperties<T, NestedType>) {
		this.properties = properties;
		this._index = new Map();
		this._resultsCollectQueue = new Cache({ max: 10000 }); // TODO choose limit better
		if (properties.indexBy) {
			this.indexByArr = Array.isArray(properties.indexBy)
				? properties.indexBy
				: [properties.indexBy];
		}
		else {
			const indexBy = types.getIdProperty(properties.schema)

			if (!indexBy) {
				throw new Error("No indexBy property defined nor schema has a property decorated with `id({ type: '...' })`")
			}

			this.indexByArr = indexBy
		}

		return this;
	}

	async get(id: types.IdKey): Promise<types.IndexedResult<T> | undefined> {
		const value = this._index.get(id.primitive);
		if (!value) {
			return;
		}
		return {
			id,
			value: value.value
		};
	}

	put(value: T, id = types.toId(types.extractFieldValue(value, this.indexByArr))): void {
		this._index.set(id.primitive, { id, value });
	}

	async del(query: types.DeleteRequest): Promise<types.IdKey[]> {
		let deleted: types.IdKey[] = [];
		for (const doc of await this.queryAll(query)) {
			if (this._index.delete(doc.id.primitive)) {
				deleted.push(doc.id);
			}
		}
		return deleted;

	}

	getSize(): number | Promise<number> {
		return this._index.size;
	}

	iterator() {
		// return a iterator if key value pairs, where the value is the indexed record
		return this._index.entries();
	}

	start(): void | Promise<void> {
		// nothing to do
	}

	stop(): void | Promise<void> {
		this._resultsCollectQueue.clear()
	}

	drop() {

		this._index.clear()
		this._resultsCollectQueue.clear()
		/* for (const subindex of this.subIndices) {
			subindex[1].clear()
		} */
	}

	/* 	subindex(name: string): types.IndexEngine<any> {
	
			const subIndex = new HashmapIndexEngine();
			this.subIndices.set(name, subIndex);
			return subIndex;
		}
	 */

	async sum(query: types.SumRequest): Promise<number | bigint> {
		let sum: undefined | number | bigint = undefined;
		for (const doc of await this.queryAll(query)) {
			let value: any = doc.value;
			for (const path of query.key) {
				value = value[path];
			}

			if (typeof value === 'number') {
				sum = (sum as number || 0) + value;
			}
			else if (typeof value === 'bigint') {
				sum = (sum as bigint || 0n) + value;
			}
		}
		return sum != null ? sum : 0;
	}


	async count(query: types.CountRequest): Promise<number> {
		return (await this.queryAll(query)).length
	}


	private async queryAll(query: types.SearchRequest | types.DeleteRequest | types.CountRequest | types.SumRequest): Promise<types.IndexedValue<T>[]> {
		if (
			query.query.length === 1 &&
			(query.query[0] instanceof types.ByteMatchQuery ||
				query.query[0] instanceof types.StringMatch) &&
			types.stringArraysEquals(query.query[0].key, this.indexByArr)
		) {
			const firstQuery = query.query[0];
			if (firstQuery instanceof types.ByteMatchQuery) {
				const doc = this._index.get(types.toId(firstQuery.value).primitive);
				return doc ? [doc] : [];
			} else if (
				firstQuery instanceof types.StringMatch &&
				firstQuery.method === types.StringMatchMethod.exact &&
				firstQuery.caseInsensitive === false
			) {
				const doc = this._index.get(firstQuery.value);
				return doc ? [doc] : [];
			}
		}

		// Handle query normally
		const indexedDocuments = await this._queryDocuments(async (doc) => {
			for (const f of query.query) {
				if (!(await this.handleQueryObject(f, doc.value))) {
					return false;
				}
			}
			return true;
		});

		return indexedDocuments
	}


	async query(
		query: types.SearchRequest,
		properties: { reference?: boolean }
	): Promise<types.IndexedResults<T>> {
		const indexedDocuments = await this.queryAll(query);
		if (indexedDocuments.length <= 1) {
			return {
				kept: 0,
				results: indexedDocuments
			}
		}


		/* const aliases = resolveNestedAliasesRecursively(query) */

		// Sort
		indexedDocuments.sort((a, b) => types.extractSortCompare(a.value, b.value, query.sort));
		const batch = getBatchFromResults<T>(
			indexedDocuments,
			query.fetch,
			/* 	this.properties.iterator.batch, */
		);

		if (indexedDocuments.length > 0) {
			this._resultsCollectQueue.add(query.idString, {
				arr: indexedDocuments,
				reference: properties?.reference
			}); // cache resulst not returned
		}

		// TODO dont leak kept if canRead is defined, or return something random
		return {
			kept: indexedDocuments.length,
			results: properties?.reference ? batch : cloneResults(batch, this.properties.schema)
		};
	}

	async next(query: types.CollectNextRequest): Promise<types.IndexedResults<T>> {
		const results = this._resultsCollectQueue.get(query.idString);
		if (!results) {
			return {
				results: [],
				kept: 0
			};
		}

		const batch = getBatchFromResults<T>(
			results.arr,
			query.amount,
			/* this.properties.iterator.batch */
		);

		if (results.arr.length === 0) {
			this._resultsCollectQueue.del(query.idString); // TODO add tests for proper cleanup/timeouts
		}

		// TODO dont leak kept if canRead is defined, or return something random
		return { results: results.reference ? batch : cloneResults(batch, this.properties.schema), kept: results.arr.length };
	}

	close(query: types.CloseIteratorRequest): void {
		this._resultsCollectQueue.del(query.idString);
	}

	private async handleFieldQuery(
		f: types.StateFieldQuery,
		obj: any,
		startIndex: number,
	): Promise<boolean | undefined> {
		// this clause is needed if we have a field that is of type [][] (we will recursively go through each subarray)

		if (
			Array.isArray(obj) ||
			(obj instanceof Uint8Array && f instanceof types.ByteMatchQuery === false)
		) {
			for (const element of obj) {
				if (await this.handleFieldQuery(f, element, startIndex)) {
					return true;
				}
			}
			return false;
		}

		// Resolve the field from the key path. If we reach an array or nested Document store,
		// then do a recursive call or a search to look into them
		for (let i = startIndex; i < f.key.length; i++) {
			obj = obj[f.key[i]];
			if (
				Array.isArray(obj) ||
				(obj instanceof Uint8Array &&
					f instanceof types.ByteMatchQuery === false)
			) {
				for (const element of obj) {
					if (await this.handleFieldQuery(f, element, i + 1)) {
						return true;
					}
				}
				return false;
			}
			if (this.properties.nested?.match(obj)) {
				const queryCloned = f.clone();
				queryCloned.key.splice(0, i + 1); // remove key path until the document store
				const results = await this.properties.nested.query(
					obj,
					new types.SearchRequest({ query: [queryCloned] })
				);
				return results.length > 0 ? true : false; // TODO return INNER HITS?
			}
		}



		if (f instanceof types.IsNull) {
			if (obj == null) {
				return true

			}
			return false
		}

		if (obj == null) {
			return undefined;
		}


		//  When we reach here, the field value (obj) is comparable
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
			return obj === f.value; // true/false
		}
		logger.warn("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private async handleQueryObject(f: types.Query, value: Record<string, any> | T): Promise<boolean | undefined> {
		if (f instanceof types.StateFieldQuery) {
			return this.handleFieldQuery(f, value as T, 0);
		} else if (f instanceof types.Nested) {
			// assume field valua is of array type and iterate over each object and match its parts
			let arr = value[f.path]
			if (!Array.isArray(arr)) {
				throw new Error("Nested field is not an array")
			}

			for (const element of arr) {
				for (const query of f.query) {
					if (await this.handleQueryObject(query, element)) {
						return true;
					}
				}

				return false;
			}

		}
		else if (f instanceof types.LogicalQuery) {
			if (f instanceof types.And) {
				for (const and of f.and) {
					const ret = await this.handleQueryObject(and, value)
					if (!ret) {
						return ret;
					}
				}
				return true;
			}

			if (f instanceof types.Or) {
				for (const or of f.or) {
					const ret = await this.handleQueryObject(or, value)
					if (ret === true) {
						return true;
					}
					else if (ret === undefined) {
						return undefined;
					}
				}
				return false;
			}
			if (f instanceof types.Not) {
				const ret = await this.handleQueryObject(f.not, value)
				if (ret === undefined) {
					return undefined
				}
				return !ret;
			}



		}

		logger.info("Unsupported query type: " + f.constructor.name);
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

	private async _queryDocuments(
		filter: (doc: types.IndexedValue) => Promise<boolean>
	): Promise<types.IndexedValue<T>[]> {
		// Whether we return the full operation data or just the db value
		const results: types.IndexedValue<T>[] = [];
		for (const value of this._index.values()) {
			if (await filter(value)) {
				results.push(value);
			}
		}
		return results;
	}

	getPending(cursorId: string): number | undefined {
		return this._resultsCollectQueue.get(cursorId)?.arr.length;
	}

	get cursorCount(): number {
		return this._resultsCollectQueue.size;
	}
}


export class HashmapIndices implements types.Indices {
	private scopes: Map<string, types.Indices>;
	private indices: { schema: any, index: HashmapIndex<any, any> }[] = [];
	private closed: boolean;
	constructor() {
		this.scopes = new Map();
		this.closed = true;
	}

	async init<T extends Record<string, any>, NestedType>(properties: types.IndexEngineInitProperties<T, any>) {


		const existingIndex = this.indices.find(i => i.schema === properties.schema)
		if (existingIndex) {
			return existingIndex.index as HashmapIndex<T, NestedType>
		}
		const index = new HashmapIndex<T, NestedType>();
		this.indices.push({ schema: properties.schema, index })
		await index.init(properties);

		if (!this.closed) {
			await index.start()
		}

		return index
	}

	async scope(name: string): Promise<types.Indices> {
		let scope = this.scopes.get(name);
		if (!scope) {
			scope = new HashmapIndices();
			if (!this.closed) {
				await scope.start();
			}
			this.scopes.set(name, scope);
		}
		return scope;
	}

	async start(): Promise<void> {

		this.closed = false;
		for (const scope of this.scopes.values()) {
			await scope.start()
		}

		for (const index of this.indices) {
			await index.index.start()
		}
	}

	async stop(): Promise<void> {

		this.closed = true;
		for (const scope of this.scopes.values()) {
			await scope.stop()
		}

		for (const index of this.indices) {
			await index.index.stop()
		}
	}
	async drop(): Promise<void> {

		for (const scope of this.scopes.values()) {
			await scope.drop()
		}

		for (const index of this.indices) {
			await index.index.drop()
		}

		this.scopes.clear()
	}

}



const create = () => new HashmapIndices();
export { create }