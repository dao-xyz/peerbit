import type { PublicSignKey } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, ShallowEntry } from "@peerbit/log";
import type {
	ReplicationLimits,
	ReplicationRangeIndexable,
} from "./replication.js";
import { MAX_U32 } from "./role.js";

export type u32 = number;
export type ReplicationDomainMapper<T> = (
	entry: Entry<T> | ShallowEntry,
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
export type ReplicationDomain<Args, T> = {
	type: string;
	fromEntry: ReplicationDomainMapper<T>;
	fromArgs: (
		args: Args | undefined,
		log: Log,
	) => Promise<CoverRange> | CoverRange;
};

export const uniformToU32 = (cursor: number) => {
	return cursor * MAX_U32;
};

export type ExtractDomainArgs<T> =
	T extends ReplicationDomain<infer Args, any> ? Args : never;
