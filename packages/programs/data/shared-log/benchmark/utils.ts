import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { LamportClock, Meta } from "@peerbit/log";
import {
	type NumberFromType,
	createNumbers,
	denormalizer,
} from "../src/integers.js";
import {
	EntryReplicatedU32,
	EntryReplicatedU64,
	ReplicationIntent,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
} from "../src/ranges.js";

export const getEntryAndRangeConstructors = <R extends "u32" | "u64">(
	resolution: R,
) => {
	const numbers = createNumbers(resolution);
	const rangeClass =
		resolution === "u32"
			? ReplicationRangeIndexableU32
			: ReplicationRangeIndexableU64;
	const denormalizeFn = denormalizer(resolution);

	const entryClass =
		resolution === "u32" ? EntryReplicatedU32 : EntryReplicatedU64;

	const createEntryReplicated = (properties: {
		coordinate: NumberFromType<any>;
		hash: string;
		meta?: Meta;
		assignedToRangeBoundary: boolean;
	}) => {
		return new entryClass({
			coordinates: [properties.coordinate],
			assignedToRangeBoundary: properties.assignedToRangeBoundary,
			hash: properties.hash,
			meta:
				properties.meta ||
				new Meta({
					clock: new LamportClock({ id: randomBytes(32) }),
					gid: "a",
					next: [],
					type: 0,
					data: undefined,
				}),
		} as any);
	};

	const createReplicationRangeFromNormalized = (properties: {
		id?: Uint8Array;
		publicKey: PublicSignKey;
		length: number;
		offset: number;
		timestamp?: bigint;
		mode?: ReplicationIntent;
	}) => {
		return new rangeClass({
			id: properties.id,
			publicKey: properties.publicKey,
			mode: properties.mode,
			// @ts-ignore
			length: denormalizeFn(properties.length),
			// @ts-ignore
			offset: denormalizeFn(properties.offset),
			timestamp: properties.timestamp,
		});
	};

	return {
		createEntry: createEntryReplicated,
		createRange: createReplicationRangeFromNormalized,
		entryClass,
		rangeClass,
		numbers,
	};
};
