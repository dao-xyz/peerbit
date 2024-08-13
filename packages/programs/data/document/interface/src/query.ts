import {
	type AbstractType,
	deserialize,
	field,
	variant,
	vec,
} from "@dao-xyz/borsh";

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

	constructor(properties: {
		created: bigint;
		modified: bigint;
		head: string;
		gid: string;
	}) {
		this.created = properties.created;
		this.modified = properties.modified;
		this.head = properties.head;
		this.gid = properties.gid;
	}
}

@variant(0)
export class ResultWithSource<T> extends Result {
	@field({ type: Uint8Array })
	_source: Uint8Array;

	@field({ type: Context })
	context: Context;

	_type: AbstractType<T>;
	constructor(opts: {
		source: Uint8Array;
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

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export abstract class AbstractSearchResult<T> {}

@variant(0)
export class Results<T> extends AbstractSearchResult<T> {
	@field({ type: vec(ResultWithSource) })
	results: ResultWithSource<T>[];

	@field({ type: "u64" })
	kept: bigint; // how many results that were not sent, but can be collected later

	constructor(properties: { results: ResultWithSource<T>[]; kept: bigint }) {
		super();
		this.kept = properties.kept;
		this.results = properties.results;
	}
}

@variant(1)
export class NoAccess extends AbstractSearchResult<any> {}

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
