import { Context, Sort, SortDirection } from "./query.js";

export const resolvedSort = async <
	Q extends { indexed: Record<string, any>; context: Context }
>(
	arr: Q[],
	sorts: Sort[]
) => {
	arr.sort((a, b) => extractSortCompare(a.indexed, b.indexed, sorts));
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
