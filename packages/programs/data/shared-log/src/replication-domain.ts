import type { PublicSignKey } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import type { Entry, ShallowEntry } from "@peerbit/log";
import type {
	ReplicationLimits,
	ReplicationRangeIndexable,
} from "./replication";

export type ReplicationDomainMapper = (
	entry: Entry<any> | ShallowEntry,
) => Promise<number> | number;

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
export type ReplicationDistribution = (
	cursor: number,
	peers: Index<ReplicationRangeIndexable>,
	amount: number,
	roleAge: number,
) => Promise<string[]> | string[]; // distribute data to the peers

export type ReplicationDomain<Args> = {
	mapper: ReplicationDomainMapper;
	collect: ReplicationDomainCoverSet<Args>;
	distribute: ReplicationDistribution;
};
