import { field, fixedArray, option, variant, vec } from "@dao-xyz/borsh";
import { randomBytes, sha256Base64Sync } from "@peerbit/crypto";
import { Query, Sort, toQuery, toSort } from "@peerbit/indexer-interface";
import {
	AbstractSearchResult,
	type Result,
	type ResultIndexedValue,
	type ResultValue,
} from "./query.js";

// for SearchRequest we wnat to return ResultsWithSource<T> for IndexedSearchRequest we want to return ResultsIndexed<T>
export type ResultTypeFromRequest<R, T, I> = R extends SearchRequest
	? ResultValue<T>
	: R extends SearchRequestIndexed
		? ResultIndexedValue<I>
		: R extends IterationRequest
			? ResultValue<T> | ResultIndexedValue<I>
			: ResultValue<T> | ResultIndexedValue<I>;

/**
 * Search with query and collect with sort conditionss
 */

export abstract class AbstractSearchRequest {
	abstract set id(id: Uint8Array);
	abstract get id(): Uint8Array;

	private _idString: string;
	private _idStringSet: Uint8Array;
	get idString(): string {
		if (this.id !== this._idStringSet) {
			this._idString = undefined;
		}
		this._idStringSet = this.id;
		return this._idString || (this._idString = sha256Base64Sync(this.id));
	}
}

@variant(0)
export class SearchRequest extends AbstractSearchRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // Session id

	@field({ type: vec(Query) })
	query: Query[];

	@field({ type: vec(Sort) })
	sort: Sort[];

	@field({ type: "u32" })
	fetch: number;

	constructor(props?: {
		query?:
			| Query[]
			| Query
			| Record<
					string,
					string | number | bigint | Uint8Array | boolean | null | undefined
			  >;
		sort?: Sort[] | Sort;
		fetch?: number;
	}) {
		super();
		this.id = randomBytes(32);
		this.query = props?.query ? toQuery(props.query) : [];
		this.sort = toSort(props?.sort as any);
		this.fetch = props?.fetch ?? 10; // default fetch 10 documents
	}
}

@variant(1)
export class SearchRequestIndexed extends AbstractSearchRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // Session id

	@field({ type: vec(Query) })
	query: Query[];

	@field({ type: vec(Sort) })
	sort: Sort[];

	@field({ type: "u32" })
	fetch: number;

	@field({ type: "bool" })
	replicate: boolean; // is the intent to replicate?

	constructor(props?: {
		query?:
			| Query[]
			| Query
			| Record<
					string,
					string | number | bigint | Uint8Array | boolean | null | undefined
			  >;
		sort?: Sort[] | Sort;
		fetch?: number;
		replicate?: boolean;
	}) {
		super();
		this.id = randomBytes(32);
		this.query = props?.query ? toQuery(props.query) : [];
		this.sort = toSort(props?.sort as any);
		this.fetch = props?.fetch ?? 10; // default fetch 10 documents
		this.replicate = props.replicate ?? false;
	}
}

/**
 * Collect documents from peers using 'collect' session ids. This is used for distributed sorting internally
 */

@variant(2)
export class CollectNextRequest extends AbstractSearchRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // collect with id

	@field({ type: "u32" })
	amount: number; // number of documents to ask for

	constructor(properties: { id: Uint8Array; amount: number }) {
		super();
		this.id = properties.id;
		this.amount = properties.amount;
	}
}

@variant(3)
export class CloseIteratorRequest extends AbstractSearchRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // collect with id

	constructor(properties: { id: Uint8Array }) {
		super();
		this.id = properties.id;
	}
}

@variant(4)
export class PredictedSearchRequest<
	_R extends Result,
> extends AbstractSearchRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // collect with id

	@field({ type: AbstractSearchRequest })
	request: SearchRequest | SearchRequestIndexed | IterationRequest;

	@field({ type: AbstractSearchResult })
	results: AbstractSearchResult;

	constructor(properties: {
		id?: Uint8Array;
		request: SearchRequest | SearchRequestIndexed | IterationRequest;
		results: AbstractSearchResult;
	}) {
		super();
		this.id = properties.id || randomBytes(32);
		this.request = properties.request;
		this.results = properties.results;
	}
}

export enum PushUpdatesMode {
	NOTIFY = 0,
	STREAM = 1,
}

@variant(5)
export class IterationRequest extends AbstractSearchRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: vec(Query) })
	query: Query[];

	@field({ type: vec(Sort) })
	sort: Sort[];

	@field({ type: "u32" })
	fetch: number;

	@field({ type: "bool" })
	resolve: boolean;

	@field({ type: "bool" })
	replicate: boolean;

	@field({ type: option("u64") })
	keepAliveTtl?: bigint;

	@field({ type: option("u8") })
	pushUpdates?: PushUpdatesMode;

	@field({ type: option("bool") })
	mergeUpdates?: boolean;

	constructor(properties?: {
		query?:
			| Query[]
			| Query
			| Record<
					string,
					string | number | bigint | Uint8Array | boolean | null | undefined
			  >;
		sort?: Sort[] | Sort;
		fetch?: number;
		resolve?: boolean;
		replicate?: boolean;
		keepAliveTtl?: number | bigint;
		pushUpdates?: boolean | PushUpdatesMode;
		mergeUpdates?: boolean;
	}) {
		super();
		this.id = randomBytes(32);
		this.query = properties?.query ? toQuery(properties.query) : [];
		this.sort = toSort(properties?.sort as any);
		this.fetch = properties?.fetch ?? 10;
		this.resolve = properties?.resolve ?? true;
		this.replicate = properties?.replicate ?? false;
		this.keepAliveTtl =
			properties?.keepAliveTtl != null
				? BigInt(properties.keepAliveTtl)
				: undefined;
		this.pushUpdates =
			typeof properties?.pushUpdates === "number"
				? properties.pushUpdates
				: properties?.pushUpdates
					? PushUpdatesMode.STREAM
					: undefined;
		this.mergeUpdates = properties?.mergeUpdates;
	}
}

export abstract class AbstractDeleteRequest {}

@variant(0)
export class DeleteRequest extends AbstractDeleteRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // Session id

	@field({ type: vec(Query) })
	query: Query[];

	constructor(props: {
		query:
			| Query[]
			| Query
			| Record<
					string,
					string | number | bigint | Uint8Array | boolean | null | undefined
			  >;
	}) {
		super();
		this.id = randomBytes(32);
		this.query = toQuery(props.query);
	}
}

export abstract class AbstractAggregationRequest {}

@variant(0)
export class SumRequest extends AbstractAggregationRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: vec(Query) })
	query: Query[];

	@field({ type: vec("string") })
	key: string[];

	constructor(props: {
		query?:
			| Query[]
			| Query
			| Record<
					string,
					string | number | bigint | Uint8Array | boolean | null | undefined
			  >;
		key: string[] | string;
	}) {
		super();
		this.id = randomBytes(32);
		this.query = props.query ? toQuery(props.query) : [];
		this.key = Array.isArray(props.key) ? props.key : [props.key];
	}
}

export abstract class AbstractCountRequest {}

@variant(0)
export class CountRequest extends AbstractCountRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array; // Session id

	@field({ type: vec(Query) })
	query: Query[];

	constructor(
		props: {
			query:
				| Query[]
				| Query
				| Record<
						string,
						string | number | bigint | Uint8Array | boolean | null | undefined
				  >;
		} = { query: [] },
	) {
		super();
		this.id = randomBytes(32);
		this.query = toQuery(props.query);
	}
}
