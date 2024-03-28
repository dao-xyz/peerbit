import * as types from "@peerbit/document-interface";
import { Cache } from "@peerbit/cache";
import { PublicSignKey } from "@peerbit/crypto";
import { equals } from "@peerbit/uint8arrays";
import { logger as loggerFn } from "@peerbit/logger";

declare module "expect" {
	interface AsymmetricMatchers {
		toBeWithinRange(floor: number, ceiling: number): void;
	}
	interface Matchers<R> {
		toBeWithinRange(floor: number, ceiling: number): R;
	}
}

const logger = loggerFn({ module: "simple-index-engine" });

const getBatchFromResults = (
	results: types.IndexedValue[],
	wantedSize: number,
	maxSize: number
) => {
	const batch: types.IndexedValue[] = [];
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

export class HashmapIndexEngine<T extends Record<string, any>, N = any>
	implements types.IndexEngine
{
	private _index: Map<string | bigint | number, types.IndexedValue>;

	private _resultsCollectQueue: Cache<{
		from: PublicSignKey;
		arr: types.IndexedValue[];
	}>;

	private indexByArr: string[];
	private properties: types.IndexEngineInitProperties<N>;

	init(properties: types.IndexEngineInitProperties<N>) {
		this.properties = properties;
		this._index = new Map();
		this._resultsCollectQueue = new Cache({ max: 10000 }); // TODO choose limit better
		this.indexByArr = Array.isArray(properties.indexBy)
			? properties.indexBy
			: [properties.indexBy];
	}

	async get(id: types.IdKey): Promise<types.IndexedResult | undefined> {
		const value = this._index.get(id.primitive);
		if (!value) {
			return;
		}
		return {
			id,
			indexed: value.indexed,
			context: value.context
		};
	}

	put(value: types.IndexedValue): void {
		this._index.set(value.id.primitive, value);
	}

	del(id: types.IdKey): void {
		this._index.delete(id.primitive);
	}

	getSize(): number | Promise<number> {
		return this._index.size;
	}

	iterator() {
		// return a iterator if key value pairs, where the value is the indexed record
		return this._index.entries();
	}

	async query(
		query: types.SearchRequest,
		from: PublicSignKey
	): Promise<types.IndexedResults> {
		// Special case querying ids

		if (
			query.query.length === 1 &&
			(query.query[0] instanceof types.ByteMatchQuery ||
				query.query[0] instanceof types.StringMatch) &&
			types.stringArraysEquals(query.query[0].key, this.indexByArr)
		) {
			const firstQuery = query.query[0];
			if (firstQuery instanceof types.ByteMatchQuery) {
				const doc = this._index.get(types.toIdeable(firstQuery.value));
				return doc
					? {
							results: [doc],
							kept: 0
						}
					: { results: [], kept: 0 };
			} else if (
				firstQuery instanceof types.StringMatch &&
				firstQuery.method === types.StringMatchMethod.exact &&
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
		await types.resolvedSort(indexedDocuments, query.sort);
		const batch = getBatchFromResults(
			indexedDocuments,
			query.fetch,
			this.properties.maxBatchSize
		);

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
		query: types.CollectNextRequest,
		from: PublicSignKey
	): Promise<types.IndexedResults> {
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

		const batch = getBatchFromResults(
			results.arr,
			query.amount,
			this.properties.maxBatchSize
		);

		if (results.arr.length === 0) {
			this._resultsCollectQueue.del(query.idString); // TODO add tests for proper cleanup/timeouts
		}

		// TODO dont leak kept if canRead is defined, or return something random
		return { results: batch, kept: results.arr.length };
	}

	close(query: types.CloseIteratorRequest, from: PublicSignKey): void {
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
		f: types.StateFieldQuery,
		obj: any,
		startIndex: number
	) {
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
		} else if (f instanceof types.MissingField) {
			return obj == null; // null or undefined
		} else if (f instanceof types.BoolQuery) {
			return obj === f.value; // true/false
		}
		logger.warn("Unsupported query type: " + f.constructor.name);
		return false;
	}

	private async handleQueryObject(f: types.Query, doc: types.IndexedValue) {
		if (f instanceof types.StateFieldQuery) {
			return this.handleFieldQuery(f, doc.indexed as T, 0);
		} else if (f instanceof types.LogicalQuery) {
			if (f instanceof types.And) {
				for (const and of f.and) {
					if (!(await this.handleQueryObject(and, doc))) {
						return false;
					}
				}
				return true;
			}

			if (f instanceof types.Or) {
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
	): Promise<types.IndexedValue[]> {
		// Whether we return the full operation data or just the db value
		const results: types.IndexedValue[] = [];
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
