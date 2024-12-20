import type { PublicSignKey } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, ShallowEntry } from "@peerbit/log";
import { debounceAccumulator } from "./debounce.js";
import type { ReplicationRangeIndexable } from "./index.js";
import type { NumberFromType } from "./integers.js";
import type { EntryReplicated } from "./ranges.js";
import type { ReplicationLimits } from "./replication.js";

export type ReplicationDomainMapper<T, R extends "u32" | "u64"> = (
	entry: Entry<T> | ShallowEntry | EntryReplicated<R>,
) => Promise<NumberFromType<R>> | NumberFromType<R>;

export type Log = {
	replicas: ReplicationLimits;
	node: {
		identity: {
			publicKey: PublicSignKey;
		};
	};
	replicationIndex: Index<ReplicationRangeIndexable<any>>;
	getDefaultMinRoleAge: () => Promise<number>;
};
export type ReplicationDomainCoverSet<Args> = (
	log: Log,
	roleAge: number | undefined,
	args: Args,
) => Promise<string[]> | string[]; // minimum set of peers that covers all the data

type CoverRange<T extends number | bigint> = {
	offset: T | PublicSignKey;
	length?: T;
};
export type ReplicationChanges = ReplicationChange[];
export type ReplicationChange =
	| {
			type: "added";
			range: ReplicationRangeIndexable<any>;
	  }
	| {
			type: "removed";
			range: ReplicationRangeIndexable<any>;
	  }
	| {
			type: "updated";
			range: ReplicationRangeIndexable<any>;
			prev: ReplicationRangeIndexable<any>;
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
	return debounceAccumulator(
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

export type ReplicationDomain<Args, T, R extends "u32" | "u64"> = {
	resolution: R;
	type: string;
	fromEntry: ReplicationDomainMapper<T, R>;
	fromArgs: (
		args: Args | undefined,
		log: Log,
	) => Promise<CoverRange<NumberFromType<R>>> | CoverRange<NumberFromType<R>>;
};

export type ExtractDomainArgs<T> =
	T extends ReplicationDomain<infer Args, any, any> ? Args : never;
