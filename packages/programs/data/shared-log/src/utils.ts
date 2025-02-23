import { Entry, ShallowEntry } from "@peerbit/log";
import type { EntryWithRefs } from "./exchange-heads.js";
import { type EntryReplicated, isEntryReplicated } from "./ranges.js";

export const groupByGid = async <
	T extends
		| ShallowEntry
		| Entry<any>
		| EntryWithRefs<any>
		| EntryReplicated<any>,
>(
	entries: T[],
): Promise<Map<string, T[]>> => {
	const groupByGid: Map<string, T[]> = new Map();
	for (const head of entries) {
		const gid =
			head instanceof Entry
				? (await head.getMeta()).gid
				: head instanceof ShallowEntry
					? head.meta.gid
					: isEntryReplicated(head)
						? head.gid
						: (await head.entry.getMeta()).gid;
		let value = groupByGid.get(gid);
		if (!value) {
			value = [];
			groupByGid.set(gid, value);
		}
		value.push(head);
	}
	return groupByGid;
};

export const groupByGidSync = async <
	T extends ShallowEntry | EntryReplicated<any>,
>(
	entries: T[],
): Promise<Map<string, T[]>> => {
	const groupByGid: Map<string, T[]> = new Map();
	for (const head of entries) {
		const gid =
			head instanceof Entry
				? (await head.getMeta()).gid
				: head instanceof ShallowEntry
					? head.meta.gid
					: head.gid;
		let value = groupByGid.get(gid);
		if (!value) {
			value = [];
			groupByGid.set(gid, value);
		}
		value.push(head);
	}
	return groupByGid;
};
