import {
	IndexEngine,
	IndexEngineInitProperties,
	IndexedResult,
	IndexedResults,
	IndexedValue
} from "./index-engine";
import {
	compare,
	And,
	BoolQuery,
	ByteMatchQuery,
	CloseIteratorRequest,
	CollectNextRequest,
	IntegerCompare,
	LogicalQuery,
	MissingField,
	Query,
	SearchRequest,
	StateFieldQuery,
	StringMatch,
	StringMatchMethod,
	Or
} from "./query.js";
import { stringArraysEquals } from "./utils.js";
import { IndexKeyPrimitiveType, asKey, keyAsIndexable } from "./types.js";
import { Cache } from "@peerbit/cache";
import { PublicSignKey } from "@peerbit/crypto";
import { equals } from "@peerbit/uint8arrays";
import { resolvedSort } from "./query-utils.js";
import { MAX_DOCUMENT_SIZE } from "./constants.js";
import { logger as loggerFn } from "@peerbit/logger";

const logger = loggerFn({ module: "map-engine" });

const getBatchFromResults = <T extends Record<string, any>>(
	results: IndexedValue<T>[],
	wantedSize: number,
	maxSize: number = MAX_DOCUMENT_SIZE
) => {
	const batch: IndexedValue<T>[] = [];
	let size = 0;
	for (const result of results) {
		batch.push(result);
		size += result.size;
		if (size > maxSize) {
			break;
		}
		if (wantedSize <= batch.length) {
			break;
		}
	}
	results.splice(0, batch.length);
	return batch;
};

export class HashmapIndexEngine<T extends Record<string, any>>
	implements IndexEngine
{
	private _index: Map<string | bigint | number, IndexedValue<T>>;

	private _resultsCollectQueue: Cache<{
		from: PublicSignKey;
		arr: IndexedValue<T>[];
	}>;

	private indexByArr: string[];
	private properties: IndexEngineInitProperties;

	init(properties: IndexEngineInitProperties) {
		this.properties = properties;
		this._index = new Map();
		this._resultsCollectQueue = new Cache({ max: 10000 }); // TODO choose limit better
		this.indexByArr = Array.isArray(properties.indexBy)
			? properties.indexBy
			: [properties.indexBy];
	}

	async query(
		query: SearchRequest,
		from: PublicSignKey
	): Promise<IndexedResults> {
		// Special case querying ids

		if (
			query.query.length === 1 &&
			(query.query[0] instanceof ByteMatchQuery ||
				query.query[0] instanceof StringMatch) &&
			stringArraysEquals(query.query[0].key, this.indexByArr)
		) {
			const firstQuery = query.query[0];
			if (firstQuery instanceof ByteMatchQuery) {
				const doc = this._index.get(keyAsIndexable(firstQuery.value));
				return doc
					? {
							results: [doc],
							kept: 0
						}
					: { results: [], kept: 0 };
			} else if (
				firstQuery instanceof StringMatch &&
				firstQuery.method === StringMatchMethod.exact &&
				firstQuery.caseInsensitive === false
			) {
				const doc = this._index.get(firstQuery.value);
				return doc
					? {
							results: [doc],
							kept: 0
						}
					: { results: [], kept: 0 };
			}
		}

		// Handle query normally
		const indexedDocuments = await this._queryDocuments(async (doc) => {
			for (const f of query.query) {
				if (!(await this.handleQueryObject(f, doc))) {
					return false;
				}
			}
			return true;
		});

		// Sort
		await resolvedSort(indexedDocuments, query.sort);
		const batch = getBatchFromResults(indexedDocuments, query.fetch);

		if (indexedDocuments.length > 0) {
			this._resultsCollectQueue.add(query.idString, {
				arr: indexedDocuments,
				from
			}); // cache resulst not returned
		}

		// TODO dont leak kept if canRead is defined, or return something random
		return {
			kept: indexedDocuments.length,
			results: batch
		};
	}

	async next(
		query: CollectNextRequest,
		from: PublicSignKey
	): Promise<IndexedResults> {
		const results = this._resultsCollectQueue.get(query.idString);
		if (!results) {
			return {
				results: [],
				kept: 0
			};
		}

		if (!results.from.equals(from)) {
			logger.warn(
				"Received a next iterator request for a iterator that does not belong to the requesting peer"
			);
			return {
				results: [],
				kept: 0
			};
		}

		const batch = getBatchFromResults(results.arr, query.amount);

		if (results.arr.length === 0) {
			this._resultsCollectQueue.del(query.idString); // TODO add tests for proper cleanup/timeouts
		}

		// TODO dont leak kept if canRead is defined, or return something random
		return { results: batch, kept: results.arr.length };
	}

	async get(id: IndexKeyPrimitiveType): Promise<IndexedResult | undefined> {
		const key = asKey(id);
		const value = this._index.get(key.indexKey);
		if (!value) {
			return;
		}
		return {
			key,
			indexed: value.indexed,
			context: value.context
		};
	}
	put(id: IndexKeyPrimitiveType, value: IndexedValue<T>): void {
		this._index.set(id, value);
	}
	del(id: IndexKeyPrimitiveType): void {
		this._index.delete(id);
	}
	get size(): number {
		return this._index.size;
	}

	close(query: CloseIteratorRequest, from: PublicSignKey): void {
		const entry = this._resultsCollectQueue.get(query.idString);
		if (entry?.from.equals(from)) {
			this._resultsCollectQueue.del(query.idString);
		} else if (entry) {
			logger.warn(
				"Received a close iterator request for a iterator that does not belong to the requesting peer"
			);
		}
	}

	private async handleFieldQuery(
		f: StateFieldQuery,
		obj: any,
		startIndex: number
	) {
		// this clause is needed if we have a field that is of type [][] (we will recursively go through each subarray)
		if (Array.isArray(obj)) {
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
			if (Array.isArray(obj)) {
				for (const element of obj) {
					if (await this.handleFieldQuery(f, element, i + 1)) {
						return true;
					}
				}
				return false;
			}
			if (obj instanceof this.properties.nestedType) {
				const nobj = obj as {
					index: { search: (query: SearchRequest) => Promise<T[]> };
				};
				const queryCloned = f.clone();
				queryCloned.key.splice(0, i + 1); // remove key path until the document store
				const results = await nobj.index.search(
					new SearchRequest({ query: [queryCloned] })
				);
				return results.length > 0 ? true : false; // TODO return INNER HITS?
			}
		}

		//  When we reach here, the field value (obj) is comparable
		if (f instanceof StringMatch) {
			let compare = f.value;
			if (f.caseInsensitive) {
				compare = compare.toLowerCase();
			}

			if (this.handleStringMatch(f, compare, obj)) {
				return true;
			}
			return false;
		} else if (f instanceof ByteMatchQuery) {
			if (obj instanceof Uint8Array === false) {
				if (stringArraysEquals(f.key, this.indexByArr)) {
					return f.valueString === obj;
				}
				return false;
			}
			return equals(obj as Uint8Array, f.value);
		} else if (f instanceof IntegerCompare) {
			const value: bigint | number = obj as any as bigint | number;

			if (typeof value !== "bigint" && typeof value !== "number") {
				return false;
			}
			return compare(value, f.compare, f.value.value);
		} else if (f instanceof MissingField) {
			return obj == null; // null or undefined
		} else if (f instanceof BoolQuery) {
			return obj === f.value; // true/false
		}
		logger.warn("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private async handleQueryObject(f: Query, doc: IndexedValue<T>) {
		if (f instanceof StateFieldQuery) {
			return this.handleFieldQuery(f, doc.indexed as T, 0);
		} else if (f instanceof LogicalQuery) {
			if (f instanceof And) {
				for (const and of f.and) {
					if (!(await this.handleQueryObject(and, doc))) {
						return false;
					}
				}
				return true;
			}

			if (f instanceof Or) {
				for (const or of f.or) {
					if (await this.handleQueryObject(or, doc)) {
						return true;
					}
				}
				return false;
			}
			return false;
		}

		logger.info("Unsupported query type: " + f.constructor.name);
		return false;
	}

	iterator() {
		// return a iterator if key value pairs, where the value is the indexed record
		return this._index.entries();
	}

	private handleStringMatch(f: StringMatch, compare: string, fv: string) {
		if (typeof fv !== "string") {
			return false;
		}
		if (f.caseInsensitive) {
			fv = fv.toLowerCase();
		}
		if (f.method === StringMatchMethod.exact) {
			return fv === compare;
		}
		if (f.method === StringMatchMethod.prefix) {
			return fv.startsWith(compare);
		}
		if (f.method === StringMatchMethod.contains) {
			return fv.includes(compare);
		}
		throw new Error("Unsupported");
	}

	async _queryDocuments(
		filter: (doc: IndexedValue<T>) => Promise<boolean>
	): Promise<IndexedValue<T>[]> {
		// Whether we return the full operation data or just the db value
		const results: IndexedValue<T>[] = [];
		for (const value of this._index.values()) {
			if (await filter(value)) {
				results.push(value);
			}
		}
		return results;
	}
}
