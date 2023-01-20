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

	constructor(opts?: { metadatas: RangeMetadata[] }) {
		if (opts) {
			this.metadatas = opts.metadatas;
		}
	}
}

/// ----- QUERY -----

@variant(0)
export class StringMatchQuery {
	@field({ type: "string" })
	value: string;

	@field({ type: "u8" })
	exactMatch: boolean;

	constructor(properties?: { value: string; exactMatch: boolean }) {
		if (properties) {
			Object.assign(this, properties);
		}
	}
	preprocess(string: string): string {
		if (this.exactMatch) {
			return string.toLowerCase();
		}
		return string;
	}
}

@variant(0)
export class StringQueryRequest {
	@field({ type: vec(StringMatchQuery) })
	queries!: StringMatchQuery[];

	constructor(properties?: { queries: StringMatchQuery[] }) {
		if (properties) {
			this.queries = properties.queries;
		}
	}
}

/// ----- RESULTS -----
@variant(0)
export class StringResult {
	@field({ type: "string" })
	string: string;

	@field({ type: option(RangeMetadatas) })
	metadatas?: RangeMetadatas;

	constructor(properties?: { string: string; metadatas?: RangeMetadatas }) {
		if (properties) {
			this.string = properties.string;
			this.metadatas = properties.metadatas;
		}
	}
}
