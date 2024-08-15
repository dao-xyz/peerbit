import {
	Compare,
	type Index,
	IntegerCompare,
	SearchRequest,
} from "@peerbit/indexer-interface";
import type { ShallowOrFullEntry } from "@peerbit/log";
import { getCoverSet } from "./ranges.js";
import type {
	Log,
	ReplicationDomain,
	ReplicationDomainCoverSet,
	ReplicationDomainMapper,
} from "./replication-domain.js";
import { ReplicationRangeIndexable } from "./replication.js";

export const timeTransformer: ReplicationDomainMapper = async (
	entry: ShallowOrFullEntry<any>,
) => {
	const cursor = entry.meta.clock.timestamp.wallTime / 1000000n;
	return Number(cursor);
};

type TimeRange = { from: number; to: number };

const getReplicatorUnion: ReplicationDomainCoverSet<TimeRange> = async (
	log: Log,
	roleAge: number,
	args: TimeRange,
) => {
	const ranges = await getCoverSet(
		log.replicationIndex,
		roleAge,
		args.from,
		args.to - args.from,
		undefined,
	);
	return [...ranges];
};

export const ReplicationDomainTime: ReplicationDomain<TimeRange> = {
	mapper: timeTransformer,
	collect: getReplicatorUnion,
	distribute: async (
		cursor: number,
		peers: Index<ReplicationRangeIndexable>,
		amount: number,
		roleAge: number,
	) => {
		const interesecting = await peers.query(
			new SearchRequest({
				query: [
					new IntegerCompare({
						key: "start1",
						compare: Compare.GreaterOrEqual,
						value: cursor,
					}),
					new IntegerCompare({
						key: "end1",
						compare: Compare.Less,
						value: cursor,
					}),
				],
			}),
		);
		const uniquePeers: Set<string> = new Set();
		for (const peer of interesecting.results) {
			uniquePeers.add(peer.value.hash);
		}
		return [...uniquePeers];
	},
};
