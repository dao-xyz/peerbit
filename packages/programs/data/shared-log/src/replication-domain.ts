import type { PublicSignKey } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, ShallowEntry } from "@peerbit/log";
import { debounceAcculmulator } from "./debounce.js";
import type { EntryReplicated, ReplicationRangeIndexable } from "./ranges.js";
import type { ReplicationLimits } from "./replication.js";
import { MAX_U32 } from "./role.js";

export type u32 = number;
export type ReplicationDomainMapper<T> = (
	entry: Entry<T> | ShallowEntry | EntryReplicated,
) => Promise<u32> | u32;

export type Log = {
	replicas: ReplicationLimits;
	node: {
		identity: {
			publicKey: PublicSignKey;
		};
	};
	syncInFlight: Map<string, Map<string, { timestamp: number }>>;
	replicationIndex: Index<ReplicationRangeIndexable>;
	getDefaultMinRoleAge: () => Promise<number>;
};
export type ReplicationDomainCoverSet<Args> = (
	log: Log,
	roleAge: number | undefined,
	args: Args,
) => Promise<string[]> | string[]; // minimum set of peers that covers all the data

type CoverRange = {
	offset: number | PublicSignKey;
	length?: number;
};
export type ReplicationChanges = ReplicationChange[];
export type ReplicationChange =
	| {
			type: "added";
			range: ReplicationRangeIndexable;
	  }
	| {
			type: "removed";
			range: ReplicationRangeIndexable;
	  }
	| {
			type: "updated";
			range: ReplicationRangeIndexable;
			prev: ReplicationRangeIndexable;
	  };

export const mergeReplicationChanges = (
	changes: ReplicationChanges | ReplicationChanges[],
): ReplicationChanges => {
	let first = changes[0];
	if (!Array.isArray(first)) {
		return changes as ReplicationChanges;
	}
	return (changes as ReplicationChanges[]).flat();
};

export const debounceAggregationChanges = (
	fn: (changeOrChanges: ReplicationChange[]) => void,
	delay: number,
) => {
	return debounceAcculmulator(
		(result) => {
			return fn([...result.values()]);
		},
		() => {
			let aggregated: Map<string, ReplicationChange> = new Map();
			return {
				add: (change: ReplicationChange) => {
					const prev = aggregated.get(change.range.idString);
					if (prev) {
						if (prev.range.timestamp < change.range.timestamp) {
							aggregated.set(change.range.idString, change);
						}
					} else {
						aggregated.set(change.range.idString, change);
					}
				},
				delete: (key: string) => {
					aggregated.delete(key);
				},
				size: () => aggregated.size,
				value: aggregated,
			};
		},
		delay,
	);
};

export type ReplicationDomain<Args, T> = {
	type: string;
	fromEntry: ReplicationDomainMapper<T>;
	fromArgs: (
		args: Args | undefined,
		log: Log,
	) => Promise<CoverRange> | CoverRange;

	// to rebalance will return an async iterator of objects that will be added to the log
	/* toRebalance(
		change: ReplicationChange,
		index: Index<EntryWithCoordinate>
	): AsyncIterable<{ gid: string, entries: { coordinate: number, hash: string }[] }> | Promise<AsyncIterable<{ gid: string, entries: EntryWithCoordinate[] }>>; */
};

export const uniformToU32 = (cursor: number) => {
	return cursor * MAX_U32;
};

export type ExtractDomainArgs<T> =
	T extends ReplicationDomain<infer Args, any> ? Args : never;
