import type { ShallowOrFullEntry } from "@peerbit/log";
import {
	type EntryReplicated,
	type ReplicationRangeIndexable,
} from "./ranges.js";
import {
	type Log,
	type ReplicationDomain,
	type ReplicationDomainConstructor,
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

export const createReplicationDomainTime =
	(properties: {
		origin?: Date;
		unit?: TimeUnit;
		canMerge?: (
			from: ReplicationRangeIndexable<"u32">,
			into: ReplicationRangeIndexable<"u32">,
		) => boolean;
	}): ReplicationDomainConstructor<ReplicationDomainTime> =>
	(log: Log) => {
		const origin = properties.origin || new Date();
		const unit = properties.unit || "milliseconds";
		const originScaled = +origin * scalarMilliToUnit[unit];
		const fromMilliToUnit = scalarMilliToUnit[unit];
		const fromTime = (time: number | Date): number => {
			return (
				(typeof time === "number" ? time : +time * fromMilliToUnit) -
				originScaled
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
			fromArgs: async (args: TimeRange | undefined) => {
				if (!args) {
					return {
						offset: log.node.identity.publicKey,
					};
				}
				return {
					offset: fromTime(args.from),
					length: fromDuration(args.to - args.from),
				};
			},
			canMerge: properties.canMerge,
		};
	};
