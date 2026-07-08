import {
	type AbstractType,
	type CustomField,
	type SimpleField,
	field,
	getSchema,
} from "@dao-xyz/borsh";
import { type IdKey } from "./id.js";
import { type Sort, SortDirection } from "./query.js";

const idByteEncoder = new TextEncoder();

const compareBytes = (a: Uint8Array, b: Uint8Array): number => {
	const length = Math.min(a.length, b.length);
	for (let i = 0; i < length; i++) {
		if (a[i] !== b[i]) {
			return a[i] < b[i] ? -1 : 1;
		}
	}
	return a.length - b.length;
};

const idKindRank = (key: string | number | bigint | Uint8Array): number => {
	if (typeof key === "number" || typeof key === "bigint") {
		return 0;
	}
	if (key instanceof Uint8Array) {
		return 1;
	}
	return 2;
};

/**
 * Break sort ties by the document primary-key id using the id's NATURAL TYPED
 * order, so equal-sort-key results order identically to the default sqlite3
 * backend (which scans its primary-key index):
 *   - integer ids (number/bigint) compare NUMERICALLY   (sqlite INTEGER: 2 < 10)
 *   - byte ids (Uint8Array)       compare by raw memcmp  (sqlite BLOB)
 *   - string ids                  compare as UTF-8 bytes (sqlite TEXT / BINARY
 *                                 collation) — deliberately NOT localeCompare,
 *                                 whose locale-aware order diverges from sqlite.
 * A single index only ever holds one id kind; the kind-rank fallback is a
 * defensive tie-breaker for mixed kinds. Returns a negative/zero/positive number
 * suitable for Array.prototype.sort. Callers reverse the result when the primary
 * (last) sort field is DESC so ties follow the scan direction.
 */
export const compareIds = (a: IdKey, b: IdKey): number => {
	const ak = a.key;
	const bk = b.key;
	if (
		(typeof ak === "number" || typeof ak === "bigint") &&
		(typeof bk === "number" || typeof bk === "bigint")
	) {
		// Compare as bigint so a mixed number/bigint pair (u32 IntegerKey vs u64
		// LargeIntegerKey) still orders by true numeric value.
		const an = BigInt(ak);
		const bn = BigInt(bk);
		return an < bn ? -1 : an > bn ? 1 : 0;
	}
	if (ak instanceof Uint8Array && bk instanceof Uint8Array) {
		return compareBytes(ak, bk);
	}
	if (typeof ak === "string" && typeof bk === "string") {
		return compareBytes(
			idByteEncoder.encode(ak),
			idByteEncoder.encode(bk),
		);
	}
	return idKindRank(ak) - idKindRank(bk);
};

export const stringArraysEquals = (
	a: string[] | string,
	b: string[] | string,
) => {
	if (a.length !== b.length) {
		return false;
	}
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false;
		}
	}
	return true;
};

/* export const resolvedSort = async <
	Q extends { indexed: Record<string, any> }
>(
	arr: Q[],
	sorts: Sort[],
	aliases?: Map<string, string>
) => {
	arr.sort((a, b) => extractSortCompare(a.indexed, b.indexed, sorts));
	return arr;
};
 */
export const extractSortCompare = (
	a: Record<string, any>,
	b: Record<string, any>,
	sorts: Sort[],
	aliases?: Map<string, string>,
	ids?: { a: IdKey; b: IdKey },
) => {
	for (const sort of sorts) {
		const av = extractFieldValue(a, sort.key, aliases);
		const bv = extractFieldValue(b, sort.key, aliases);
		const cmp = sortCompare(av, bv);
		if (cmp !== 0) {
			if (sort.direction === SortDirection.ASC) {
				return cmp;
			} else {
				return -cmp;
			}
		}
	}
	// All sort fields compared equal: break the tie deterministically by the
	// document primary-key id in its natural typed order, so the result order is
	// content-deterministic and matches the default sqlite3 backend instead of
	// depending on insertion order. The default backend scans its primary-key
	// index in the sort direction, so reverse the id order when the primary (last)
	// sort field is DESC.
	if (ids) {
		const idCmp = compareIds(ids.a, ids.b);
		if (idCmp !== 0) {
			const primaryDirection =
				sorts.length > 0 ? sorts[sorts.length - 1].direction : SortDirection.ASC;
			return primaryDirection === SortDirection.DESC ? -idCmp : idCmp;
		}
	}
	return 0;
};

export const sortCompare = (av: any, bv: any) => {
	if (typeof av === "string" && typeof bv === "string") {
		return av.localeCompare(bv);
	}
	if (av < bv) {
		return -1;
	} else if (av > bv) {
		return 1;
	}
	return 0;
};

export const extractFieldValue = <T>(
	doc: any,
	path: string[],
	aliases?: Map<string, string>,
): T => {
	for (let i = 0; i < path.length; i++) {
		doc = doc[aliases?.get(path[i]) || path[i]];
	}
	return doc;
};

const INDEXED_ID_META_PROPERY = "_index_id";
type Stage3DecoratorContext = {
	kind: string;
	name: string | symbol;
	metadata?: Record<PropertyKey, unknown>;
	addInitializer?(initializer: (this: any) => void): void;
	static?: boolean;
};

const isStage3DecoratorContext = (
	value: unknown,
): value is Stage3DecoratorContext => {
	return !!value && typeof value === "object" && "kind" in value;
};

const ensureLegacyMetadata = (target: any, name: string | symbol): void => {
	target.constructor[INDEXED_ID_META_PROPERY] = name;
};

const storeMetadataOnContext = (context: Stage3DecoratorContext): void => {
	context.metadata &&
		(context.metadata[INDEXED_ID_META_PROPERY] = context.name);
	// Best-effort legacy compatibility so classes compiled with stage-3
	// decorators still expose the legacy static hint.
	context.addInitializer?.(function () {
		const owner = context.static ? this : this?.constructor;
		if (owner && owner[INDEXED_ID_META_PROPERY] == null) {
			owner[INDEXED_ID_META_PROPERY] = context.name;
		}
	});
};

export function id(properties: SimpleField | CustomField<any>) {
	const innerFn = field(properties);
	return (targetOrValue: any, nameOrContext: any) => {
		if (isStage3DecoratorContext(nameOrContext)) {
			const result = (innerFn as unknown as (value: any, context: any) => any)(
				targetOrValue,
				nameOrContext,
			);
			storeMetadataOnContext(nameOrContext);
			return result;
		}
		(innerFn as unknown as (target: any, key: string | symbol) => any)(
			targetOrValue,
			nameOrContext,
		);
		ensureLegacyMetadata(targetOrValue, nameOrContext);
	};
}

const getMetadataIdProperty = (
	clazz: AbstractType<any>,
): string | symbol | undefined => {
	if (typeof Symbol === "undefined") {
		return undefined;
	}
	const metadataSymbol = (Symbol as any).metadata;
	if (!metadataSymbol) {
		return undefined;
	}
	return (clazz as any)[metadataSymbol]?.[INDEXED_ID_META_PROPERY] as
		| string
		| symbol
		| undefined;
};

export const getIdProperty = (
	clazz: AbstractType<any>,
): string[] | undefined => {
	// TODO nested id property
	const property =
		(clazz as any)[INDEXED_ID_META_PROPERY] ?? getMetadataIdProperty(clazz);
	if (!property) {
		// look into children

		const fields = getSchema(clazz)?.fields;
		if (!fields) {
			return;
		}

		for (const field of fields) {
			if (typeof field.type === "function") {
				const idFromChild = getIdProperty(field.type);
				if (idFromChild) {
					return [field.key, ...idFromChild];
				}
			}
		}

		return undefined;
	}
	return [property as string];
};
