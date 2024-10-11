import { field, fixedArray, variant, vec } from "@dao-xyz/borsh";
import { randomBytes, sha256Base64Sync } from "@peerbit/crypto";
import { Query, Sort, toQuery } from "@peerbit/indexer-interface";

/**
 * Search with query and collect with sort conditionss
 */

const toArray = <T>(arr: T | T[] | undefined) =>
	(arr ? (Array.isArray(arr) ? arr : [arr]) : undefined) || [];

export abstract class AbstractSearchRequest {}

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
		this.sort = toArray(props?.sort);
		this.fetch = props?.fetch ?? 10; // default fetch 10 documents
	}

	private _idString: string;
	get idString(): string {
		return this._idString || (this._idString = sha256Base64Sync(this.id));
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

	private _idString: string;
	get idString(): string {
		return this._idString || (this._idString = sha256Base64Sync(this.id));
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

	private _idString: string;
	get idString(): string {
		return this._idString || (this._idString = sha256Base64Sync(this.id));
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
