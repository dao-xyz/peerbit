import {
	type AbstractType,
	deserialize,
	field,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { Entry } from "@peerbit/log";

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
export class ResultValue<T, I = Record<string, any>> extends Result {
	@field({ type: Uint8Array })
	_source: Uint8Array;

	@field({ type: Context })
	context: Context;

	_type: AbstractType<T>;
	constructor(opts: {
		source?: Uint8Array;
		context: Context;
		value?: T;
		indexed?: I;
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
	indexed?: I;
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

@variant(2)
// eslint-disable-next-line @typescript-eslint/no-unused-vars
// @ts-ignore
class _ResultValueWithIndexedResultValue<T, I> extends Result {
	// not used yet, but
	@field({ type: Uint8Array })
	_sourceValue: Uint8Array;

	@field({ type: Uint8Array })
	_sourceIndexed: Uint8Array;

	@field({ type: vec(Entry) })
	entries: Entry<any>[];

	@field({ type: Context })
	context: Context;

	_type: AbstractType<T>;
	_indexedtype: AbstractType<I>;

	constructor(opts: {
		context: Context;

		source: Uint8Array;
		sourceIndexed: Uint8Array;
		entries: Entry<any>[];
		value: T;
		indexed: I;
	}) {
		super();
		this._sourceValue = opts.source;
		this._sourceIndexed = opts.sourceIndexed;
		this.context = opts.context;
		this._value = opts.value;
		this.entries = opts.entries;
		this.indexed = opts.indexed;
	}

	init({
		type,
		indexedType,
	}: {
		type: AbstractType<T>;
		indexedType: AbstractType<I>;
	}) {
		this._type = type;
		this._indexedtype = indexedType;
	}

	_value?: T;
	get value(): T {
		if (this._value) {
			return this._value;
		}
		if (!this._sourceValue) {
			throw new Error("Missing source binary");
		}
		this._value = deserialize(this._sourceValue, this._type);
		return this._value;
	}

	indexed?: I;
	get indexedValue(): I {
		if (this.indexed) {
			return this.indexed;
		}
		if (!this._sourceIndexed) {
			throw new Error("Missing source binary for indexed value");
		}
		this.indexed = deserialize(this._sourceIndexed, this._indexedtype);
		return this.indexed;
	}
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
