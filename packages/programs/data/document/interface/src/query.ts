import {
	type AbstractType,
	deserialize,
	field,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";
import type { SearchRequest } from "./request.js";

/// ----- RESULTS -----

export abstract class Result {}

@variant(0)
export class Context {
	@field({ type: "u64" })
	created: bigint;

	@field({ type: "u64" })
	modified: bigint;

	@field({ type: "string" })
	head: string;

	@field({ type: "string" })
	gid: string;

	@field({ type: "u32" })
	size: number; // bytes, we index this so we can query documents and understand their representation sizes

	constructor(properties: {
		created: bigint;
		modified: bigint;
		head: string;
		gid: string;
		size: number;
	}) {
		this.created = properties.created;
		this.modified = properties.modified;
		this.head = properties.head;
		this.gid = properties.gid;
		this.size = properties.size;
	}
}

@variant(0)
export class ResultValue<T> extends Result {
	@field({ type: Uint8Array })
	_source: Uint8Array;

	@field({ type: Context })
	context: Context;

	_type: AbstractType<T>;
	constructor(opts: {
		source?: Uint8Array;
		context: Context;
		value?: T;
		indexed?: Record<string, any>;
	}) {
		super();
		this._source = opts.source;
		this.context = opts.context;
		this._value = opts.value;
		this.indexed = opts.indexed;
	}

	init(type: AbstractType<T>) {
		this._type = type;
	}

	_value?: T;
	get value(): T {
		if (this._value) {
			return this._value;
		}
		if (!this._source) {
			throw new Error("Missing source binary");
		}
		this._value = deserialize(this._source, this._type);
		return this._value;
	}

	// we never send this over the wire since we can always reconstruct it from value
	indexed?: Record<string, any>;
}

@variant(1)
export class ResultIndexedValue<I> extends Result {
	@field({ type: Uint8Array })
	_source: Uint8Array;

	@field({ type: vec(Entry) })
	entries: Entry<any>[];

	@field({ type: Context })
	context: Context;

	_type: AbstractType<I>;

	constructor(opts: {
		source: Uint8Array;
		indexed: I;
		entries: Entry<any>[];
		context: Context;
	}) {
		super();
		this._source = opts.source;
		this.context = opts.context;
		this.indexed = opts.indexed;
		this.entries = opts.entries;
		this._value = opts.indexed;
	}

	init(type: AbstractType<I>) {
		this._type = type;
	}

	_value?: I;
	get value(): I {
		if (this._value) {
			return this._value;
		}
		if (!this._source) {
			throw new Error("Missing source binary");
		}
		this._value = deserialize(this._source, this._type);
		return this._value;
	}

	// we never send this over the wire since we can always reconstruct it from value
	indexed?: I;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export abstract class AbstractSearchResult {}

@variant(0)
export class Results<R extends Result> extends AbstractSearchResult {
	@field({ type: vec(Result) })
	results: R[];

	@field({ type: "u64" })
	kept: bigint; // how many results that were not sent, but can be collected later

	constructor(properties: { results: R[]; kept: bigint }) {
		super();
		this.kept = properties.kept;
		this.results = properties.results;
	}
}

@variant(1)
export class NoAccess extends AbstractSearchResult {}

// for SearchRequest we wnat to return ResultsWithSource<T> for IndexedSearchRequest we want to return ResultsIndexed<T>
export type ResultTypeFromRequest<R, T, I> = R extends SearchRequest
	? ResultValue<T>
	: ResultIndexedValue<I>;

/* @variant(5)
export class LogQuery extends Query { } */

/**
 * Find logs that can be decrypted by certain keys
 */
/* 
@variant(0)
export class EntryEncryptedByQuery
	extends LogQuery
	implements
	EntryEncryptionTemplate<
		X25519PublicKey[],
		X25519PublicKey[],
		X25519PublicKey[],
		X25519PublicKey[]
	>
{
	@field({ type: vec(X25519PublicKey) })
	meta: X25519PublicKey[];

	@field({ type: vec(X25519PublicKey) })
	payload: X25519PublicKey[];

	@field({ type: vec(X25519PublicKey) })
	next: X25519PublicKey[];

	@field({ type: vec(X25519PublicKey) })
	signatures: X25519PublicKey[];

	constructor(properties?: {
		meta: X25519PublicKey[];
		next: X25519PublicKey[];
		payload: X25519PublicKey[];
		signatures: X25519PublicKey[];
	}) {
		super();
		if (properties) {
			this.metadata = properties.metadata;
			this.payload = properties.payload;
			this.next = properties.next;
			this.signatures = properties.signatures;
		}
	}
}

@variant(1)
export class SignedByQuery extends LogQuery {
	@field({ type: vec(PublicSignKey) })
	_publicKeys: PublicSignKey[];

	constructor(properties: { publicKeys: PublicSignKey[] }) {
		super();
		if (properties) {
			this._publicKeys = properties.publicKeys;
		}
	}

	get publicKeys() {
		return this._publicKeys;
	}
}
 */
