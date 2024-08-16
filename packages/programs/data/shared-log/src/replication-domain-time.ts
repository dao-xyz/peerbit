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
	u32,
} from "./replication-domain.js";
import { ReplicationRangeIndexable } from "./replication.js";


type TimeUnit = 'seconds' | 'milliseconds' | 'microseconds' | 'nanoseconds'

const scalarNanoToUnit = {
	seconds: BigInt(1e9),
	milliseconds: BigInt(1e6),
	microseconds: BigInt(1e3),
	nanoseconds: BigInt(1),
}
const scalarMilliToUnit = {
	seconds: 1e3,
	milliseconds: 1,
	microseconds: 1e-3,
	nanoseconds: 1e-6,
}


export const fromEntry = (origin: Date, unit: TimeUnit = 'milliseconds'): ReplicationDomainMapper => {

	const scalar = scalarNanoToUnit[unit];
	const originTime = (+origin) / scalarMilliToUnit[unit];

	const fn = (
		entry: ShallowOrFullEntry<any>,
	) => {
		const cursor = entry.meta.clock.timestamp.wallTime / scalar
		return Math.round((Number(cursor) - originTime));
	};
	return fn;
}




type TimeRange = { from: number; to: number };

const getReplicatorUnion: ReplicationDomainCoverSet<TimeRange> = async (
	log: Log,
	roleAge: number | undefined,
	args: TimeRange,
) => {

	roleAge = roleAge ?? (await log.getDefaultMinRoleAge());

	const ranges = await getCoverSet(
		log.replicationIndex,
		roleAge,
		args.from,
		args.to - args.from,
		undefined,
	);
	return [...ranges];
};

export type ReplicationDomainTime = ReplicationDomain<TimeRange> & { fromTime: (time: number | Date) => u32 };
export const createReplicationDomainTime = (origin: Date, unit: TimeUnit = 'milliseconds'): ReplicationDomainTime => {
	const originScaled = (+origin) * scalarMilliToUnit[unit];
	const fromMilliToUnit = scalarMilliToUnit[unit];
	return {
		fromTime: (time: number | Date) => {
			// return a uniform number between 0 and 1
			return ((typeof time === 'number' ? time : +time * fromMilliToUnit) - originScaled);
		},
		fromEntry: fromEntry(origin, unit),
		collect: getReplicatorUnion,
		distribute: async (
			cursor: u32,
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
	}
};
