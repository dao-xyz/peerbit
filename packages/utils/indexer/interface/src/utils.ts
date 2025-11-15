import {
	type AbstractType,
	type CustomField,
	type SimpleField,
	field,
	getSchema,
} from "@dao-xyz/borsh";
import { type Sort, SortDirection } from "./query.js";

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

const ensureLegacyMetadata = (
	target: any,
	name: string | symbol,
): void => {
	target.constructor[INDEXED_ID_META_PROPERY] = name;
};

const storeMetadataOnContext = (
	context: Stage3DecoratorContext,
): void => {
	context.metadata && (context.metadata[INDEXED_ID_META_PROPERY] = context.name);
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
