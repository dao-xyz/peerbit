import { field, option, variant, vec } from "@dao-xyz/borsh";

@variant(0)
export class RangeMetadata {
	@field({ type: "u64" })
	offset: bigint;

	@field({ type: "u64" })
	length: bigint;

	constructor(opts?: { offset: bigint; length: bigint }) {
		if (opts) {
			Object.assign(this, opts);
		}
	}
}

@variant(0)
export class RangeMetadatas {
	@field({ type: vec(RangeMetadata) })
	metadatas: RangeMetadata[];

	constructor(opts: { metadatas: RangeMetadata[] }) {
		this.metadatas = opts.metadatas;
	}
}

/// ----- QUERY -----

@variant(0)
export class StringMatch {
	@field({ type: "string" })
	value: string;

	@field({ type: "u8" })
	exactMatch: boolean;

	constructor(properties: { value: string; exactMatch: boolean }) {
		this.value = properties.value;
		this.exactMatch = properties.exactMatch;
	}
	preprocess(string: string): string {
		if (this.exactMatch) {
			return string.toLowerCase();
		}
		return string;
	}
}

@variant(0)
export class SearchRequest {
	@field({ type: vec(StringMatch) })
	query!: StringMatch[];

	constructor(properties: { query: StringMatch[] }) {
		this.query = properties.query;
	}
}

/// ----- RESULTS -----
export abstract class AbstractSearchResult {}

@variant(0)
export class StringResult extends AbstractSearchResult {
	@field({ type: "string" })
	string: string;

	@field({ type: option(RangeMetadatas) })
	metadatas?: RangeMetadatas;

	constructor(properties: { string: string; metadatas?: RangeMetadatas }) {
		super();
		this.string = properties.string;
		this.metadatas = properties.metadatas;
	}
}

@variant(1)
export class NoAccess extends AbstractSearchResult {}
