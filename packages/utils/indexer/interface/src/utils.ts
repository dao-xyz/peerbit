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

export function id(properties: SimpleField | CustomField<any>) {
	const innerFn = field(properties);
	return (target: any, name: string) => {
		innerFn(target, name);
		target.constructor[INDEXED_ID_META_PROPERY] = name;
	};
}

export const getIdProperty = (
	clazz: AbstractType<any>,
): string[] | undefined => {
	// TODO nested id property
	const property = (clazz as any)[INDEXED_ID_META_PROPERY];
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
