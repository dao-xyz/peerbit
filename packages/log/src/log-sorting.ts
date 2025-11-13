import {
	Compare,
	IntegerCompare,
	Sort,
	SortDirection,
	type StateFieldQuery,
} from "@peerbit/indexer-interface";
import type { Timestamp } from "./clock.js";

export type SortableEntry = {
	meta: {
		clock: {
			timestamp: Timestamp;
		};
		gid: string;
	};
	hash: string;
};

export const ENTRY_SORT_SHAPE = {
	hash: true,
	meta: { gid: true, clock: true },
} as const;

export type SortFn = {
	sort: Sort[];
	before: (entry: SortableEntry) => StateFieldQuery[];
	after: (entry: SortableEntry) => StateFieldQuery[];
};

/**
 * Sort two entries as Last-Write-Wins (LWW).
 *
 * Last Write Wins is a conflict resolution strategy for sorting elements
 * where the element with a greater clock (latest) is chosen as the winner.
 *
 */
export const LastWriteWins: SortFn = {
	sort: [
		// sort first by clock
		new Sort({
			key: ["meta", "clock", "timestamp", "wallTime"],
			direction: SortDirection.ASC,
		}),

		// then by logical
		new Sort({
			key: ["meta", "clock", "timestamp", "logical"],
			direction: SortDirection.ASC,
		}),
	],
	before: (entry: SortableEntry) => [
		new IntegerCompare({
			key: ["meta", "clock", "timestamp", "wallTime"],
			value: entry.meta.clock.timestamp.wallTime,
			compare: Compare.Less,
		}),
	],
	after: (entry: SortableEntry) => [
		new IntegerCompare({
			key: ["meta", "clock", "timestamp", "wallTime"],
			value: entry.meta.clock.timestamp.wallTime,
			compare: Compare.Greater,
		}),
	],
};

export const FirstWriteWins: SortFn = {
	sort: [
		new Sort({
			key: ["meta", "clock", "timestamp", "wallTime"],
			direction: SortDirection.DESC,
		}),

		// then by logical
		new Sort({
			key: ["meta", "clock", "timestamp", "logical"],
			direction: SortDirection.DESC,
		}),
	],
	before: (entry: SortableEntry) => [
		new IntegerCompare({
			key: ["meta", "clock", "timestamp", "wallTime"],
			value: entry.meta.clock.timestamp.wallTime,
			compare: Compare.Greater,
		}),
	],
	after: (entry: SortableEntry) => [
		new IntegerCompare({
			key: ["meta", "clock", "timestamp", "wallTime"],
			value: entry.meta.clock.timestamp.wallTime,
			compare: Compare.Less,
		}),
	],
};
export const SortByEntryHash: SortFn = {
	sort: [
		// sort first by clock
		new Sort({
			key: ["hash"],
			direction: SortDirection.ASC,
		}),
	],
	before: (entry: SortableEntry) => [
		new IntegerCompare({
			key: ["hash"],
			value: entry.meta.clock.timestamp.wallTime,
			compare: Compare.Less,
		}),
	],
	after: (entry: SortableEntry) => [
		new IntegerCompare({
			key: ["hash"],
			value: entry.meta.clock.timestamp.wallTime,
			compare: Compare.Greater,
		}),
	],
};

export const compare = (a: SortableEntry, b: SortableEntry, sortFn: SortFn) => {
	for (const sort of sortFn.sort) {
		const aVal = sort.key.reduce((prev, curr) => (prev as any)[curr], a);
		const bVal = sort.key.reduce((prev, curr) => (prev as any)[curr], b);
		let multiplier = sort.direction === SortDirection.ASC ? 1 : -1;
		if (aVal < bVal) {
			return -1 * multiplier;
		}
		if (aVal > bVal) {
			return 1 * multiplier;
		}
	}

	return 0;
};
