import { type ShallowOrFullEntry } from "./entry.js";
import { Compare, IntegerCompare, Sort, SortDirection, StateFieldQuery } from "@peerbit/indexer-interface";


export type SortFn<T = any> = { sort: Sort[], before: (entry: ShallowOrFullEntry<T>) => StateFieldQuery[], after: (entry: ShallowOrFullEntry<T>) => StateFieldQuery[] };

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
			key: ['meta', 'clock', 'timestamp', 'wallTime'], direction: SortDirection.ASC
		}),

		// then by logical
		new Sort({
			key: ['meta', 'clock', 'timestamp', 'logical'], direction: SortDirection.ASC
		})
	],
	before: (entry: ShallowOrFullEntry<any>) => [new IntegerCompare({ key: ['meta', 'clock', 'timestamp', 'wallTime'], value: entry.meta.clock.timestamp.wallTime, compare: Compare.Less })],
	after: (entry: ShallowOrFullEntry<any>) => [new IntegerCompare({ key: ['meta', 'clock', 'timestamp', 'wallTime'], value: entry.meta.clock.timestamp.wallTime, compare: Compare.Greater })],
}

export const FirstWriteWins: SortFn = {
	sort: [
		new Sort({
			key: ['meta', 'clock', 'timestamp', 'wallTime'], direction: SortDirection.DESC
		}),

		// then by logical
		new Sort({
			key: ['meta', 'clock', 'timestamp', 'logical'], direction: SortDirection.DESC
		})
	],
	before: (entry: ShallowOrFullEntry<any>) => [new IntegerCompare({ key: ['meta', 'clock', 'timestamp', 'wallTime'], value: entry.meta.clock.timestamp.wallTime, compare: Compare.Greater })],
	after: (entry: ShallowOrFullEntry<any>) => [new IntegerCompare({ key: ['meta', 'clock', 'timestamp', 'wallTime'], value: entry.meta.clock.timestamp.wallTime, compare: Compare.Less })],
}
export const SortByEntryHash: SortFn = {
	sort: [
		// sort first by clock
		new Sort({
			key: ['hash'], direction: SortDirection.ASC
		}),

	],
	before: (entry: ShallowOrFullEntry<any>) => [new IntegerCompare({ key: ['hash'], value: entry.meta.clock.timestamp.wallTime, compare: Compare.Less })],
	after: (entry: ShallowOrFullEntry<any>) => [new IntegerCompare({ key: ['hash'], value: entry.meta.clock.timestamp.wallTime, compare: Compare.Greater })],
}


export const compare = (a: ShallowOrFullEntry<any>, b: ShallowOrFullEntry<any>, sortFn: SortFn) => {

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

}