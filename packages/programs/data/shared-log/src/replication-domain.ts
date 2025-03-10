import type { PublicSignKey } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, ShallowEntry } from "@peerbit/log";
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

export type CoverRange<T extends number | bigint> = {
	offset: T | PublicSignKey;
	length?: T;
};

export type ReplicationDomain<Args, T, R extends "u32" | "u64"> = {
	resolution: R;
	type: string;
	fromEntry: ReplicationDomainMapper<T, R>;
	fromArgs: (
		args: Args | undefined,
	) => Promise<CoverRange<NumberFromType<R>>> | CoverRange<NumberFromType<R>>;
	canMerge?: (
		from: ReplicationRangeIndexable<R>,
		into: ReplicationRangeIndexable<R>,
	) => boolean;
};

export type ReplicationDomainConstructor<
	D extends ReplicationDomain<any, any, any>,
> = (log: Log) => D;
export type ExtractDomainArgs<T> =
	T extends ReplicationDomain<infer Args, any, any> ? Args : never;
