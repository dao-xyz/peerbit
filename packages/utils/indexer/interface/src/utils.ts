import { Sort, SortDirection } from "./query.js";
import { field, type SimpleField, type CustomField, type AbstractType } from "@dao-xyz/borsh";

export const stringArraysEquals = (
	a: string[] | string,
	b: string[] | string
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

export const resolvedSort = async <
	Q extends { value: Record<string, any> }
>(
	arr: Q[],
	sorts: Sort[]
) => {
	arr.sort((a, b) => extractSortCompare(a.value, b.value, sorts));
	return arr;
};

export const extractSortCompare = (
	a: Record<string, any>,
	b: Record<string, any>,
	sorts: Sort[]
) => {
	for (const sort of sorts) {
		const av = extractFieldValue(a, sort.key);
		const bv = extractFieldValue(b, sort.key);
		const cmp = sortCompare(av, bv);
		if (cmp != 0) {
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

export const extractFieldValue = <T>(doc: any, path: string[]): T => {
	for (let i = 0; i < path.length; i++) {
		doc = doc[path[i]];
	}
	return doc;
};



const INDEXED_ID_META_PROPERY = "_index_id"

export function id(properties: SimpleField | CustomField<any>) {
	const innerFn = field(properties);
	return (target: any, name: string) => {
		innerFn(target, name);
		target.constructor[INDEXED_ID_META_PROPERY] = name;
	};
}


export const getIdProperty = (clazz: AbstractType<any>): string[] | undefined => {
	// TODO nested id property
	const property = (clazz as any)[INDEXED_ID_META_PROPERY];
	if (!property) {
		return undefined;
	}
	return [property as string];
}