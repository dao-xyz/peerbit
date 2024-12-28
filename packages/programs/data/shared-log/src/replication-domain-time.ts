import type { ShallowOrFullEntry } from "@peerbit/log";
import { type EntryReplicated } from "./ranges.js";
import {
	type ReplicationDomain,
	type ReplicationDomainMapper,
} from "./replication-domain.js";

type TimeUnit = "seconds" | "milliseconds" | "microseconds" | "nanoseconds";

const scalarNanoToUnit = {
	seconds: BigInt(1e9),
	milliseconds: BigInt(1e6),
	microseconds: BigInt(1e3),
	nanoseconds: BigInt(1),
};
const scalarMilliToUnit = {
	seconds: 1e3,
	milliseconds: 1,
	microseconds: 1e-3,
	nanoseconds: 1e-6,
};

export const fromEntry = (
	origin: Date,
	unit: TimeUnit = "milliseconds",
): ReplicationDomainMapper<any, "u32"> => {
	const scalar = scalarNanoToUnit[unit];
	const originTime = +origin / scalarMilliToUnit[unit];

	const fn = (entry: ShallowOrFullEntry<any> | EntryReplicated<"u32">) => {
		const cursor = entry.meta.clock.timestamp.wallTime / scalar;
		return Math.round(Number(cursor) - originTime);
	};
	return fn;
};

type TimeRange = { from: number; to: number };

export type ReplicationDomainTime = ReplicationDomain<TimeRange, any, "u32"> & {
	fromTime: (time: number | Date) => number;
	fromDuration: (duration: number) => number;
};

export const createReplicationDomainTime = (
	origin: Date,
	unit: TimeUnit = "milliseconds",
): ReplicationDomainTime => {
	const originScaled = +origin * scalarMilliToUnit[unit];
	const fromMilliToUnit = scalarMilliToUnit[unit];
	const fromTime = (time: number | Date): number => {
		return (
			(typeof time === "number" ? time : +time * fromMilliToUnit) - originScaled
		);
	};

	const fromDuration = (duration: number): number => {
		return duration;
	};
	return {
		resolution: "u32",
		type: "time",
		fromTime,
		fromDuration,
		fromEntry: fromEntry(origin, unit),
		fromArgs: async (args: TimeRange | undefined, log) => {
			if (!args) {
				return {
					offset: log.node.identity.publicKey,
				};
			}
			return {
				offset: fromTime(args.from),
				length: fromDuration(args.to - args.from),
			};
			/* 	roleAge = roleAge ?? (await log.getDefaultMinRoleAge());
				const ranges = await getCoverSet(
					log.replicationIndex,
					roleAge,
					fromTime(args.from),
					fromDuration(args.to - args.from),
					undefined,
				);
				return [...ranges]; */
		},
	};
};
