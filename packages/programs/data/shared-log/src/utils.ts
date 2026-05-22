import { Entry, ShallowEntry } from "@peerbit/log";
import {
	type EntryWithRefs,
	getPreparedRawExchangeHeadGid,
	getPreparedRawExchangeGid,
} from "./exchange-heads.js";
import { type EntryReplicated, isEntryReplicated } from "./ranges.js";

const getEntryGid = async (entry: Entry<any>): Promise<string> => {
	try {
		return entry.meta.gid;
	} catch {
		return (await entry.getMeta()).gid;
	}
};

const getEntryGidSync = (entry: Entry<any>): string | undefined => {
	try {
		return entry.meta.gid;
	} catch {
		return undefined;
	}
};

const getHeadGidSync = <
	T extends
		| ShallowEntry
		| Entry<any>
		| EntryWithRefs<any>
		| EntryReplicated<any>,
>(
	head: T,
): string | undefined =>
	head instanceof Entry
		? getPreparedRawExchangeGid(head) ?? getEntryGidSync(head)
		: head instanceof ShallowEntry
			? head.meta.gid
			: isEntryReplicated(head)
				? head.gid
				: getPreparedRawExchangeHeadGid(head) ??
					getPreparedRawExchangeGid(head.entry) ??
					getEntryGidSync(head.entry);

export const tryGroupByGidSync = <
	T extends
		| ShallowEntry
		| Entry<any>
		| EntryWithRefs<any>
		| EntryReplicated<any>,
>(
	entries: T[],
): Map<string, T[]> | undefined => {
	const groupByGid: Map<string, T[]> = new Map();
	for (const head of entries) {
		const gid = getHeadGidSync(head);
		if (gid == null) {
			return undefined;
		}
		let value = groupByGid.get(gid);
		if (!value) {
			value = [];
			groupByGid.set(gid, value);
		}
		value.push(head);
	}
	return groupByGid;
};

export const groupByGid = async <
	T extends
		| ShallowEntry
		| Entry<any>
		| EntryWithRefs<any>
		| EntryReplicated<any>,
>(
	entries: T[],
): Promise<Map<string, T[]>> => {
	const syncGrouped = tryGroupByGidSync(entries);
	if (syncGrouped) {
		return syncGrouped;
	}

	const groupByGid: Map<string, T[]> = new Map();
	for (const head of entries) {
		const gid =
			head instanceof Entry
				? await getEntryGid(head)
				: head instanceof ShallowEntry
					? head.meta.gid
					: isEntryReplicated(head)
						? head.gid
						: getPreparedRawExchangeHeadGid(head) ??
							(await getEntryGid(head.entry));
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
