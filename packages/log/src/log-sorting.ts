import { Entry, ShallowEntry } from "./entry.js";
import { LamportClock as Clock } from "./clock.js";
import { compare } from "@peerbit/uint8arrays";

const First = (a: any, b: any) => a;

export type ISortFunction = <T>(
	a: ShallowEntry,
	b: ShallowEntry,
	resolveConflict?: (a: ShallowEntry, b: ShallowEntry) => number
) => number;
/**
 * Sort two entries as Last-Write-Wins (LWW).
 *
 * Last Write Wins is a conflict resolution strategy for sorting elements
 * where the element with a greater clock (latest) is chosen as the winner.
 *
 * @param {Entry} a First entry
 * @param {Entry} b Second entry
 * @returns {number} 1 if a is latest, -1 if b is latest
 */
export const LastWriteWins: ISortFunction = <T>(
	a: ShallowEntry,
	b: ShallowEntry
) => {
	// Ultimate conflict resolution (take the first/left arg)
	// Sort two entries by their clock id, if the same always take the first
	const sortById = (a: ShallowEntry, b: ShallowEntry) =>
		SortByClockId(a, b, First);
	// Sort two entries by their clock time, if concurrent,
	// determine sorting using provided conflict resolution function
	const sortByEntryClocks = (a: ShallowEntry, b: ShallowEntry) =>
		SortByClocks(a, b, sortById);
	// Sort entries by clock time as the primary sort criteria
	return sortByEntryClocks(a, b);
};

/**
 * Sort two entries by their hash.
 *
 * @param {Entry} a First entry
 * @param {Entry} b Second entry
 * @returns {number} 1 if a is latest, -1 if b is latest
 */
export const SortByEntryHash: ISortFunction = (a, b) => {
	// Ultimate conflict resolution (compare hashes)
	const compareHash = (a: ShallowEntry, b: ShallowEntry) =>
		a.hash < b.hash ? -1 : 1;
	// Sort two entries by their clock id, if the same then compare hashes
	const sortById = (a: ShallowEntry, b: ShallowEntry) =>
		SortByClockId(a, b, compareHash);
	// Sort two entries by their clock time, if concurrent,
	// determine sorting using provided conflict resolution function
	const sortByEntryClocks = (a: ShallowEntry, b: ShallowEntry) =>
		SortByClocks(a, b, sortById);
	// Sort entries by clock time as the primary sort criteria
	return sortByEntryClocks(a, b);
};

/**
 * Sort two entries by their clock time.
 * @param {Entry} a First entry to compare
 * @param {Entry} b Second entry to compare
 * @param {function(a, b)} resolveConflict A function to call if entries are concurrent (happened at the same time). The function should take in two entries and return 1 if the first entry should be chosen and -1 if the second entry should be chosen.
 * @returns {number} 1 if a is greater, -1 if b is greater
 */
export const SortByClocks: ISortFunction = <T>(
	a: ShallowEntry,
	b: ShallowEntry,
	resolveConflict?: (a: ShallowEntry, b: ShallowEntry) => number
) => {
	// Compare the clocks
	const diff = Clock.compare(a.meta.clock, b.meta.clock);
	// If the clocks are concurrent, use the provided
	// conflict resolution function to determine which comes first
	return diff === 0 ? (resolveConflict || First)(a, b) : diff;
};

/**
 * Sort two entries by their clock id.
 * @param {Entry} a First entry to compare
 * @param {Entry} b Second entry to compare
 * @param {function(a, b)} resolveConflict A function to call if the clocks ids are the same. The function should take in two entries and return 1 if the first entry should be chosen and -1 if the second entry should be chosen.
 * @returns {number} 1 if a is greater, -1 if b is greater
 */
export const SortByClockId: ISortFunction = (a, b, resolveConflict) => {
	// Sort by ID if clocks are concurrent,
	// take the entry with a "greater" clock id
	const clockCompare = compare(a.meta.clock.id, b.meta.clock.id);
	return clockCompare === 0 ? (resolveConflict || First)(a, b) : clockCompare;
};

/**
 * A wrapper function to throw an error if the results of a passed function return zero
 * @param {function(a, b)} [tiebreaker] The tiebreaker function to validate.
 * @returns {function(a, b)} 1 if a is greater, -1 if b is greater
 * @throws {Error} if func ever returns 0
 */
export const NoZeroes = (func: ISortFunction) => {
	const comparator = <T>(a: ShallowEntry, b: ShallowEntry) => {
		// Validate by calling the function
		const result = func(a, b, (a, b) => -1);
		if (result === 0) {
			throw Error(
				`Your log's tiebreaker function, ${func.name}, has returned zero and therefore cannot be`
			);
		}
		return result;
	};

	return comparator;
};
