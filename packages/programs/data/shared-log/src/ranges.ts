import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import type { Cache } from "@peerbit/cache";
import {
	PublicSignKey,
	equals,
	randomBytes,
	sha256Base64Sync,
	toBase64,
} from "@peerbit/crypto";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	type Index,
	type IndexIterator,
	type IndexedResult,
	type IndexedResults,
	IntegerCompare,
	Not,
	Or,
	type Query,
	type ReturnTypeFromShape,
	type Shape,
	Sort,
	SortDirection,
	StringMatch,
	iteratorInSeries,
} from "@peerbit/indexer-interface";
import { id } from "@peerbit/indexer-interface";
import { Meta, ShallowMeta } from "@peerbit/log";
import { debounceAccumulator } from "@peerbit/time";
import {
	MAX_U32,
	MAX_U64,
	type NumberFromType,
	type Numbers,
} from "./integers.js";

export enum ReplicationIntent {
	NonStrict = 0, // indicates that the segment will be replicated and nearby data might be replicated as well
	Strict = 1, // only replicate data in the segment to the specified replicator, not any other data
}

const min = (a: number | bigint, b: number | bigint) => (a < b ? a : b);

const getSegmentsFromOffsetAndRange = <T extends number | bigint>(
	offset: T,
	factor: T,
	zero: T,
	max: T,
): [[T, T], [T, T]] => {
	let start1 = offset;
	// @ts-ignore
	let end1Unscaled = offset + factor; // only add factor if it is not 1 to prevent numerical issues (like (0.9 + 1) % 1 => 0.8999999)
	let end1: T = min(end1Unscaled, max) as T;
	return [
		[start1, end1],
		/* eslint-disable no-irregular-whitespace */
		// @ts-ignore
		end1Unscaled > max
			? /* eslint-disable no-irregular-whitespace */
				// @ts-ignore
				[zero, (factor !== max ? offset + factor : offset) % max]
			: [start1, end1],
	];
};

export const shouldAssigneToRangeBoundary = (
	leaders:
		| Map<
				string,
				{
					intersecting: boolean;
				}
		  >
		| false,
	minReplicas: number,
) => {
	let assignedToRangeBoundary = leaders === false || leaders.size < minReplicas;
	if (!assignedToRangeBoundary && leaders) {
		for (const [_, value] of leaders) {
			if (!value.intersecting) {
				assignedToRangeBoundary = true;
				break;
			}
		}
	}
	return assignedToRangeBoundary;
};
export interface EntryReplicated<R extends "u32" | "u64"> {
	hash: string; // id of the entry
	hashNumber: NumberFromType<R>; // hash of the entry in number format
	gid: string;
	coordinates: NumberFromType<R>[];
	wallTime: bigint;
	assignedToRangeBoundary: boolean;
	get meta(): ShallowMeta;
}

export const isEntryReplicated = (x: any): x is EntryReplicated<any> => {
	return x instanceof EntryReplicatedU32 || x instanceof EntryReplicatedU64;
};

@variant("entry-u32")
export class EntryReplicatedU32 implements EntryReplicated<"u32"> {
	@id({ type: "string" })
	hash: string;

	@field({ type: "u32" })
	hashNumber: number;

	@field({ type: "string" })
	gid: string;

	@field({ type: vec("u32") })
	coordinates: number[];

	@field({ type: "u64" })
	wallTime: bigint;

	@field({ type: "bool" })
	assignedToRangeBoundary: boolean;

	@field({ type: Uint8Array })
	private _meta: Uint8Array;

	private _metaResolved: ShallowMeta;

	constructor(properties: {
		coordinates: number[];
		hash: string;
		meta: Meta;
		assignedToRangeBoundary: boolean;
		hashNumber: number;
	}) {
		this.coordinates = properties.coordinates;
		this.hash = properties.hash;
		this.gid = properties.meta.gid;
		this.wallTime = properties.meta.clock.timestamp.wallTime;
		this.hashNumber = properties.hashNumber;
		const shallow =
			properties.meta instanceof Meta
				? new ShallowMeta(properties.meta)
				: properties.meta;
		this._meta = serialize(shallow);
		this._metaResolved = deserialize(this._meta, ShallowMeta);
		this._metaResolved = properties.meta;
		this.assignedToRangeBoundary = properties.assignedToRangeBoundary;
	}

	get meta(): ShallowMeta {
		if (!this._metaResolved) {
			this._metaResolved = deserialize(this._meta, ShallowMeta);
		}
		return this._metaResolved;
	}
}

@variant("entry-u64")
export class EntryReplicatedU64 implements EntryReplicated<"u64"> {
	@id({ type: "string" })
	hash: string;

	@field({ type: "u64" })
	hashNumber: bigint;

	@field({ type: "string" })
	gid: string;

	@field({ type: vec("u64") })
	coordinates: bigint[];

	@field({ type: "u64" })
	wallTime: bigint;

	@field({ type: "bool" })
	assignedToRangeBoundary: boolean;

	@field({ type: Uint8Array })
	private _meta: Uint8Array;

	private _metaResolved: ShallowMeta;

	constructor(properties: {
		coordinates: bigint[];
		hash: string;
		meta: Meta;
		assignedToRangeBoundary: boolean;
		hashNumber: bigint;
	}) {
		this.coordinates = properties.coordinates;
		this.hash = properties.hash;
		this.hashNumber = properties.hashNumber;
		this.gid = properties.meta.gid;
		this.wallTime = properties.meta.clock.timestamp.wallTime;
		const shallow =
			properties.meta instanceof Meta
				? new ShallowMeta(properties.meta)
				: properties.meta;
		this._meta = serialize(shallow);
		this._metaResolved = deserialize(this._meta, ShallowMeta);
		this._metaResolved = properties.meta;
		this.assignedToRangeBoundary = properties.assignedToRangeBoundary;
	}

	get meta(): ShallowMeta {
		if (!this._metaResolved) {
			this._metaResolved = deserialize(this._meta, ShallowMeta);
		}
		return this._metaResolved;
	}
}

export const isReplicationRangeMessage = (
	x: any,
): x is ReplicationRangeMessage<any> => {
	return x instanceof ReplicationRangeMessage;
};

export abstract class ReplicationRangeMessage<R extends "u32" | "u64"> {
	abstract id: Uint8Array;
	abstract timestamp: bigint;
	abstract mode: ReplicationIntent;
	abstract get offset(): NumberFromType<R>;
	abstract get factor(): NumberFromType<R>;
	abstract toReplicationRangeIndexable(
		key: PublicSignKey,
	): ReplicationRangeIndexable<R>;
}

@variant(0)
export class ReplicationRangeMessageU32 extends ReplicationRangeMessage<"u32"> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	private _offset: number;

	@field({ type: "u32" })
	private _factor: number;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(properties: {
		id: Uint8Array;
		offset: number;
		factor: number;
		timestamp: bigint;
		mode: ReplicationIntent;
	}) {
		super();
		const { id, offset, factor, timestamp, mode } = properties;
		this.id = id;
		this._offset = offset;
		this._factor = factor;
		this.timestamp = timestamp;
		this.mode = mode;
	}

	get factor(): number {
		return this._factor;
	}

	get offset(): number {
		return this._offset;
	}

	toReplicationRangeIndexable(
		key: PublicSignKey,
	): ReplicationRangeIndexableU32 {
		return new ReplicationRangeIndexableU32({
			id: this.id,
			publicKeyHash: key.hashcode(),
			offset: this.offset,
			width: this.factor,
			timestamp: this.timestamp,
			mode: this.mode,
		});
	}
}

@variant(1)
export class ReplicationRangeMessageU64 extends ReplicationRangeMessage<"u64"> {
	@field({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u64" })
	private _offset: bigint;

	@field({ type: "u64" })
	private _factor: bigint;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(properties: {
		id: Uint8Array;
		offset: bigint;
		factor: bigint;
		timestamp: bigint;
		mode: ReplicationIntent;
	}) {
		super();
		const { id, offset, factor, timestamp, mode } = properties;
		this.id = id;
		this._offset = offset;
		this._factor = factor;
		this.timestamp = timestamp;
		this.mode = mode;
	}

	get factor(): bigint {
		return this._factor;
	}

	get offset(): bigint {
		return this._offset;
	}

	toReplicationRangeIndexable(
		key: PublicSignKey,
	): ReplicationRangeIndexableU64 {
		return new ReplicationRangeIndexableU64({
			id: this.id,
			publicKeyHash: key.hashcode(),
			offset: this.offset,
			width: this.factor,
			timestamp: this.timestamp,
			mode: this.mode,
		});
	}
}

class HashableSegmentU32 {
	@field({ type: "u32" })
	start1!: number;

	@field({ type: "u32" })
	end1!: number;

	@field({ type: "u32" })
	start2!: number;

	@field({ type: "u32" })
	end2!: number;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(properties: {
		start1: number;
		start2: number;
		end1: number;
		end2: number;
		mode: ReplicationIntent;
	}) {
		this.start1 = properties.start1;
		this.end1 = properties.end1;
		this.start2 = properties.start2;
		this.end2 = properties.end2;
		this.mode = properties.mode;
	}
}

class HashableSegmentU64 {
	@field({ type: "u64" })
	start1!: bigint;

	@field({ type: "u64" })
	end1!: bigint;

	@field({ type: "u64" })
	start2!: bigint;

	@field({ type: "u64" })
	end2!: bigint;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(properties: {
		start1: bigint;
		start2: bigint;
		end1: bigint;
		end2: bigint;
		mode: ReplicationIntent;
	}) {
		this.start1 = properties.start1;
		this.end1 = properties.end1;
		this.start2 = properties.start2;
		this.end2 = properties.end2;
		this.mode = properties.mode;
	}
}

export interface ReplicationRangeIndexable<R extends "u32" | "u64"> {
	id: Uint8Array;
	idString: string;
	hash: string;
	timestamp: bigint;
	start1: NumberFromType<R>;
	end1: NumberFromType<R>;
	start2: NumberFromType<R>;
	end2: NumberFromType<R>;
	width: NumberFromType<R>;
	widthNormalized: number;
	mode: ReplicationIntent;
	wrapped: boolean;
	toUniqueSegmentId(): string;
	toReplicationRange(): ReplicationRangeMessage<R>;
	contains(point: NumberFromType<R>): boolean;
	equalRange(other: ReplicationRangeIndexable<R>): boolean;
	overlaps(other: ReplicationRangeIndexable<R>): boolean;
	toString(): string;
	get rangeHash(): string;
}

export type NumericType = "u32" | "u64";

/**
 * Convert a GeneralRange<N> into one or two `[bigint, bigint]` segments.
 * - If it’s not wrapped, there’s one segment: [start1, end1).
 * - If it’s wrapped, there’s two: [start1, end1) and [start2, end2).
 *
 * We always do the conversion to bigints internally.
 */
export function toSegmentsBigInt<N extends NumericType>(
	range: ReplicationRangeIndexable<N>,
): Array<[bigint, bigint]> {
	// Safely convert the numeric fields to bigint
	const s1: bigint =
		typeof range.start1 === "number" ? BigInt(range.start1) : range.start1;
	const e1: bigint =
		typeof range.end1 === "number" ? BigInt(range.end1) : range.end1;
	const s2: bigint =
		typeof range.start2 === "number" ? BigInt(range.start2) : range.start2;
	const e2: bigint =
		typeof range.end2 === "number" ? BigInt(range.end2) : range.end2;

	const segments: Array<[bigint, bigint]> = [];

	segments.push([s1, e1]);

	if (s2 !== s1 && s2 !== e2) {
		segments.push([s2, e2]);
	}

	return segments;
}

/**
 * Build an array of new GeneralRange<N> objects from leftover `[bigint, bigint]` segments.
 * We split them in pairs, each range can hold up to two segments:
 *
 * - [seg1Start, seg1End)
 * - [seg2Start, seg2End) (if available)
 *
 * We convert bigints back to the correct numeric type, if needed.
 */
function buildRangesFromBigIntSegments<N extends NumericType>(
	segments: Array<[bigint, bigint]>,
	templateRange: ReplicationRangeIndexable<N>,
): Array<ReplicationRangeIndexable<N>> {
	// Sort by start
	segments.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

	const result: Array<ReplicationRangeIndexable<N>> = [];
	let i = 0;
	const proto = Object.getPrototypeOf(templateRange);

	while (i < segments.length) {
		const seg1 = segments[i];
		i++;
		let seg2: [bigint, bigint] | null = null;
		if (i < segments.length) {
			seg2 = segments[i];
			i++;
		}

		// Convert back to the original numeric type
		const [s1, e1] = toOriginalType<N>(seg1, templateRange);
		const [s2, e2] = seg2
			? toOriginalType<N>(seg2, templateRange)
			: ([s1, e1] as [NumberFromType<N>, NumberFromType<N>]);

		// Build a new range object. You can clone or replicate metadata as needed.
		const newRange = Object.assign(Object.create(proto), {
			...templateRange,
			start1: s1,
			end1: e1,
			start2: s2,
			end2: e2,
		});

		result.push(newRange);
	}
	return result;
}

/**
 * Subtract one bigint segment [bStart, bEnd) from [aStart, aEnd).
 * Returns 0..2 leftover segments in bigint form.
 */

function subtractBigIntSegment(
	aStart: bigint,
	aEnd: bigint,
	bStart: bigint,
	bEnd: bigint,
): Array<[bigint, bigint]> {
	const result: Array<[bigint, bigint]> = [];

	// No overlap
	if (bEnd <= aStart || bStart >= aEnd) {
		result.push([aStart, aEnd]);
		return result;
	}

	// Fully contained
	if (bStart <= aStart && bEnd >= aEnd) {
		return [];
	}

	// Partial overlaps
	if (bStart > aStart) {
		result.push([aStart, bStart]);
	}
	if (bEnd < aEnd) {
		result.push([bEnd, aEnd]);
	}

	return result;
}

/**
 * Helper: convert `[bigint, bigint]` to `[number, number]` if N is "u32",
 * or keep as `[bigint, bigint]` if N is "u64".
 */
function toOriginalType<N extends NumericType>(
	segment: [bigint, bigint],
	templateRange: ReplicationRangeIndexable<N>,
): [NumberFromType<N>, NumberFromType<N>] {
	const [start, end] = segment;
	if (isU32Range(templateRange)) {
		// Convert back to number
		return [Number(start), Number(end)] as any;
	} else {
		// Keep as bigint
		return [start, end] as any;
	}
}

/**
 * Merge any adjacent or overlapping `[bigint, bigint]` segments.
 * E.g. [10,20) and [20,25) => [10,25)
 */
export function mergeBigIntSegments(
	segments: Array<[bigint, bigint]>,
): Array<[bigint, bigint]> {
	if (segments.length < 2) return segments;

	// Sort by start
	segments.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

	const merged: Array<[bigint, bigint]> = [];
	let current = segments[0];

	for (let i = 1; i < segments.length; i++) {
		const next = segments[i];
		// If current overlaps or touches next
		if (current[1] >= next[0]) {
			// Merge
			current = [current[0], current[1] > next[1] ? current[1] : next[1]];
		} else {
			merged.push(current);
			current = next;
		}
	}
	merged.push(current);

	return merged;
}

/**
 * Figure out if a given range is "u32" or "u64".
 * You might also store this in the object itself if you prefer.
 */
function isU32Range<N extends NumericType>(
	range: ReplicationRangeIndexable<N>,
): boolean {
	// If you store a separate `type: "u32" | "u64"` in the range, you can just check that:
	// return range.type === "u32";
	// or we do a hack by checking the type of start1, e.g.:

	// If "start1" is a number (not a bigint), we treat it as u32
	return typeof range.start1 === "number";
}

export function symmetricDifferenceRanges<N extends NumericType>(
	rangeA: ReplicationRangeIndexable<N>,
	rangeB: ReplicationRangeIndexable<N>,
): {
	rangesFromA: Array<ReplicationRangeIndexable<N>>;
	rangesFromB: Array<ReplicationRangeIndexable<N>>;
} {
	const segmentsA = toSegmentsBigInt(rangeA);
	const segmentsB = toSegmentsBigInt(rangeB);

	const resultSegmentsA: Array<[bigint, bigint]> = [];
	const resultSegmentsB: Array<[bigint, bigint]> = [];

	// Compute symmetric difference for A
	for (const [aStart, aEnd] of segmentsA) {
		let leftover = [[aStart, aEnd]] as Array<[bigint, bigint]>;
		for (const [bStart, bEnd] of segmentsB) {
			const newLeftover = [];
			for (const [start, end] of leftover) {
				newLeftover.push(...subtractBigIntSegment(start, end, bStart, bEnd));
			}
			leftover = newLeftover;
		}
		resultSegmentsA.push(...leftover);
	}

	// Compute symmetric difference for B
	for (const [bStart, bEnd] of segmentsB) {
		let leftover = [[bStart, bEnd]] as Array<[bigint, bigint]>;
		for (const [aStart, aEnd] of segmentsA) {
			const newLeftover = [];
			for (const [start, end] of leftover) {
				newLeftover.push(...subtractBigIntSegment(start, end, aStart, aEnd));
			}
			leftover = newLeftover;
		}
		resultSegmentsB.push(...leftover);
	}

	// Remove zero-length or invalid segments
	const validSegmentsA = resultSegmentsA.filter(([start, end]) => start < end);
	const validSegmentsB = resultSegmentsB.filter(([start, end]) => start < end);

	// Merge and deduplicate segments
	const mergedSegmentsA = mergeBigIntSegments(validSegmentsA);
	const mergedSegmentsB = mergeBigIntSegments(validSegmentsB);

	// Build ranges
	const rangesFromA = buildRangesFromBigIntSegments(mergedSegmentsA, rangeA);
	const rangesFromB = buildRangesFromBigIntSegments(mergedSegmentsB, rangeB);

	return {
		rangesFromA,
		rangesFromB,
	};
}

@variant("range-u32")
export class ReplicationRangeIndexableU32
	implements ReplicationRangeIndexable<"u32">
{
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "string" })
	hash: string;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	start1!: number;

	@field({ type: "u32" })
	end1!: number;

	@field({ type: "u32" })
	start2!: number;

	@field({ type: "u32" })
	end2!: number;

	@field({ type: "u32" })
	width!: number;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(
		properties: {
			id?: Uint8Array;
			offset: number;
			width: number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		} & ({ publicKeyHash: string } | { publicKey: PublicSignKey }),
	) {
		this.id = properties.id ?? randomBytes(32);
		this.hash =
			(properties as { publicKeyHash: string }).publicKeyHash ||
			(properties as { publicKey: PublicSignKey }).publicKey.hashcode();
		this.transform({ width: properties.width, offset: properties.offset });

		this.mode = properties.mode ?? ReplicationIntent.NonStrict;
		this.timestamp = properties.timestamp || BigInt(0);
	}

	private transform(properties: { offset: number; width: number }) {
		const ranges = getSegmentsFromOffsetAndRange(
			properties.offset,
			properties.width,
			0,
			MAX_U32,
		);
		this.start1 = Math.round(ranges[0][0]);
		this.end1 = Math.round(ranges[0][1]);
		this.start2 = Math.round(ranges[1][0]);
		this.end2 = Math.round(ranges[1][1]);

		this.width =
			this.end1 -
			this.start1 +
			(this.end2 < this.end1 ? this.end2 - this.start2 : 0);

		if (
			this.start1 > MAX_U32 ||
			this.end1 > MAX_U32 ||
			this.start2 > MAX_U32 ||
			this.end2 > MAX_U32 ||
			this.width > MAX_U32 ||
			this.width < 0
		) {
			throw new Error("Segment coordinate out of bounds");
		}
	}

	get idString() {
		return toBase64(this.id);
	}

	get rangeHash() {
		const ser = serialize(this);
		return sha256Base64Sync(ser);
	}

	contains(point: number) {
		return (
			(point >= this.start1 && point < this.end1) ||
			(point >= this.start2 && point < this.end2)
		);
	}

	overlaps(other: ReplicationRangeIndexableU32, checkOther = true): boolean {
		if (
			this.contains(other.start1) ||
			this.contains(other.start2) ||
			this.contains(other.end1 - 1) ||
			this.contains(other.end2 - 1)
		) {
			return true;
		}

		if (checkOther) {
			return other.overlaps(this, false);
		}
		return false;
	}
	toReplicationRange() {
		return new ReplicationRangeMessageU32({
			id: this.id,
			offset: this.start1,
			factor: this.width,
			timestamp: this.timestamp,
			mode: this.mode,
		});
	}

	get wrapped() {
		return this.end2 < this.end1;
	}

	get widthNormalized() {
		return this.width / MAX_U32;
	}

	equals(other: ReplicationRangeIndexableU32) {
		if (
			equals(this.id, other.id) &&
			this.hash === other.hash &&
			this.timestamp === other.timestamp &&
			this.mode === other.mode &&
			this.start1 === other.start1 &&
			this.end1 === other.end1 &&
			this.start2 === other.start2 &&
			this.end2 === other.end2 &&
			this.width === other.width
		) {
			return true;
		}

		return false;
	}

	equalRange(other: ReplicationRangeIndexableU32) {
		return (
			this.hash === other.hash &&
			this.start1 === other.start1 &&
			this.end1 === other.end1 &&
			this.start2 === other.start2 &&
			this.end2 === other.end2
		);
	}

	toString() {
		let roundToTwoDecimals = (num: number) => Math.round(num * 100) / 100;

		if (Math.abs(this.start1 - this.start2) < 0.0001) {
			return `([${roundToTwoDecimals(this.start1 / MAX_U32)}, ${roundToTwoDecimals(this.end1 / MAX_U32)}])`;
		}
		return `([${roundToTwoDecimals(this.start1 / MAX_U32)}, ${roundToTwoDecimals(this.end1 / MAX_U32)}] [${roundToTwoDecimals(this.start2 / MAX_U32)}, ${roundToTwoDecimals(this.end2 / MAX_U32)}])`;
	}

	toStringDetailed() {
		return `(hash ${this.hash} range: ${this.toString()})`;
	}

	toUniqueSegmentId() {
		// return a unique id as a function of the segments location and the replication intent
		const hashable = new HashableSegmentU32(this);
		return sha256Base64Sync(serialize(hashable));
	}
}

@variant("range-u64")
export class ReplicationRangeIndexableU64
	implements ReplicationRangeIndexable<"u64">
{
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "string" })
	hash: string; // publickey hash

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u64" })
	start1!: bigint;

	@field({ type: "u64" })
	end1!: bigint;

	@field({ type: "u64" })
	start2!: bigint;

	@field({ type: "u64" })
	end2!: bigint;

	@field({ type: "u64" })
	width!: bigint;

	@field({ type: "u8" })
	mode: ReplicationIntent;

	constructor(
		properties: {
			id?: Uint8Array;
			offset: bigint | number;
			width: bigint | number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		} & ({ publicKeyHash: string } | { publicKey: PublicSignKey }),
	) {
		this.id = properties.id ?? randomBytes(32);
		this.hash =
			(properties as { publicKeyHash: string }).publicKeyHash ||
			(properties as { publicKey: PublicSignKey }).publicKey.hashcode();
		this.transform({ width: properties.width, offset: properties.offset });

		this.mode = properties.mode ?? ReplicationIntent.NonStrict;
		this.timestamp = properties.timestamp || BigInt(0);
	}

	private transform(properties: {
		offset: bigint | number;
		width: bigint | number;
	}) {
		const ranges = getSegmentsFromOffsetAndRange(
			BigInt(properties.offset),
			BigInt(properties.width),
			0n,
			MAX_U64,
		);
		this.start1 = ranges[0][0];
		this.end1 = ranges[0][1];
		this.start2 = ranges[1][0];
		this.end2 = ranges[1][1];

		this.width =
			this.end1 -
			this.start1 +
			(this.end2 < this.end1 ? this.end2 - this.start2 : 0n);

		if (
			this.start1 > MAX_U64 ||
			this.end1 > MAX_U64 ||
			this.start2 > MAX_U64 ||
			this.end2 > MAX_U64 ||
			this.width > MAX_U64 ||
			this.width < 0n
		) {
			throw new Error("Segment coordinate out of bounds");
		}
	}

	get idString() {
		return toBase64(this.id);
	}

	contains(point: bigint) {
		return (
			(point >= this.start1 && point < this.end1) ||
			(point >= this.start2 && point < this.end2)
		);
	}

	get rangeHash() {
		const ser = serialize(this);
		return sha256Base64Sync(ser);
	}

	overlaps(other: ReplicationRangeIndexableU64, checkOther = true): boolean {
		if (
			this.contains(other.start1) ||
			this.contains(other.start2) ||
			this.contains(other.end1 - 1n) ||
			this.contains(other.end2 - 1n)
		) {
			return true;
		}

		if (checkOther) {
			return other.overlaps(this, false);
		}
		return false;
	}
	toReplicationRange() {
		return new ReplicationRangeMessageU64({
			id: this.id,
			offset: this.start1,
			factor: this.width,
			timestamp: this.timestamp,
			mode: this.mode,
		});
	}

	get wrapped() {
		return this.end2 < this.end1;
	}

	get widthNormalized() {
		return Number(this.width) / Number(MAX_U64);
	}

	equals(other: ReplicationRangeIndexableU64) {
		if (
			equals(this.id, other.id) &&
			this.hash === other.hash &&
			this.timestamp === other.timestamp &&
			this.mode === other.mode &&
			this.start1 === other.start1 &&
			this.end1 === other.end1 &&
			this.start2 === other.start2 &&
			this.end2 === other.end2 &&
			this.width === other.width
		) {
			return true;
		}

		return false;
	}

	equalRange(other: ReplicationRangeIndexableU64) {
		return (
			this.hash === other.hash &&
			this.start1 === other.start1 &&
			this.end1 === other.end1 &&
			this.start2 === other.start2 &&
			this.end2 === other.end2
		);
	}

	toString() {
		let roundToTwoDecimals = (num: number) => Math.round(num * 100) / 100;

		if (Math.abs(Number(this.start1 - this.start2)) < 0.0001) {
			return `([${roundToTwoDecimals(Number(this.start1) / Number(MAX_U64))}, ${roundToTwoDecimals(Number(this.start1) / Number(MAX_U64))}])`;
		}
		return `([${roundToTwoDecimals(Number(this.start1) / Number(MAX_U64))}, ${roundToTwoDecimals(Number(this.start1) / Number(MAX_U64))}] [${roundToTwoDecimals(Number(this.start2) / Number(MAX_U64))}, ${roundToTwoDecimals(Number(this.end2) / Number(MAX_U64))}])`;
	}

	toStringDetailed() {
		return `(hash ${this.hash} range: ${this.toString()})`;
	}

	toUniqueSegmentId() {
		// return a unique id as a function of the segments location and the replication intent
		const hashable = new HashableSegmentU64(this);
		return sha256Base64Sync(serialize(hashable));
	}
}

export const mergeRanges = <R extends "u32" | "u64">(
	segments: ReplicationRangeIndexable<R>[],
	numbers: { zero: NumberFromType<R>; maxValue: NumberFromType<R> },
) => {
	if (segments.length === 0) {
		throw new Error("No segments to merge");
	}
	if (segments.length === 1) {
		return segments[0];
	}

	// only allow merging from same publicKeyHash
	const sameHash = segments.every((x) => x.hash === segments[0].hash);
	if (!sameHash) {
		throw new Error("Segments have different publicKeyHash");
	}

	// 1) Sort by start offset (avoid subtracting bigints).
	//    We do slice() to avoid mutating the original 'segments'.
	const sorted = segments.slice().sort((a, b) => {
		if (a.start1 < b.start1) return -1;
		if (a.start1 > b.start1) return 1;
		return 0;
	});

	// 2) Merge overlapping arcs in a purely functional way
	//    so we don’t mutate any intermediate objects.
	const merged = sorted.reduce<ReplicationRangeIndexable<R>[]>(
		(acc, current) => {
			if (acc.length === 0) {
				return [current];
			}

			const last = acc[acc.length - 1];

			// Check overlap: next arc starts before (or exactly at) last arc's end => overlap
			if (current.start1 <= last.end2) {
				// Merge them:
				// - end2 is the max of last.end2 and current.end2
				// - width is adjusted so total covers both
				// - mode is strict if either arc is strict
				const newEnd2 = last.end2 > current.end2 ? last.end2 : current.end2;
				const extendedWidth = Number(newEnd2 - last.start1); // safe if smaller arcs

				// If you need to handle big widths carefully, you might do BigInt logic here.
				const newMode =
					last.mode === ReplicationIntent.Strict ||
					current.mode === ReplicationIntent.Strict
						? ReplicationIntent.Strict
						: ReplicationIntent.NonStrict;

				// Create a new merged arc object (no mutation of last)
				const proto = segments[0].constructor as any;
				const mergedArc = new proto({
					width: extendedWidth,
					offset: last.start1,
					publicKeyHash: last.hash,
					mode: newMode,
					id: last.id, // re-use id
				});

				// Return a new array with the last item replaced by mergedArc
				return [...acc.slice(0, -1), mergedArc];
			} else {
				// No overlap => just append current
				return [...acc, current];
			}
		},
		[],
	);

	// After the merge pass:
	if (merged.length === 1) {
		// Everything merged into one arc already
		return merged[0];
	}

	// 3) OPTIONAL: If your existing logic always wants to produce a single ring arc
	//    that covers "everything except the largest gap," do it here:

	// Determine if any arc ended up Strict
	const finalMode = merged.some((m) => m.mode === ReplicationIntent.Strict)
		? ReplicationIntent.Strict
		: ReplicationIntent.NonStrict;

	// Find the largest gap on a ring among these disjoint arcs
	const { largestGap, largestGapIndex } = merged.reduce<{
		largestGap: NumberFromType<R>;
		largestGapIndex: number;
	}>(
		(acc, arc, i, arr) => {
			// next arc in a ring
			const nextArc = arr[(i + 1) % arr.length];

			// measure gap from arc.end2 -> nextArc.start1
			let gap: NumberFromType<R>;
			if (nextArc.start1 < arc.end2) {
				// wrap-around scenario
				gap = (numbers.maxValue -
					arc.end2 +
					(nextArc.start1 - numbers.zero)) as NumberFromType<R>;
			} else {
				gap = (nextArc.start1 - arc.end2) as NumberFromType<R>;
			}

			if (gap > acc.largestGap) {
				return { largestGap: gap, largestGapIndex: (i + 1) % arr.length };
			}
			return acc;
		},
		{ largestGap: numbers.zero, largestGapIndex: -1 },
	);

	// Single arc coverage = "the ring minus largestGap"
	const totalCoverage = (numbers.maxValue - largestGap) as number;
	const offset = merged[largestGapIndex].start1;

	const proto = segments[0].constructor as any;
	return new proto({
		width: totalCoverage,
		offset,
		publicKeyHash: segments[0].hash,
		mode: finalMode,
	});
};

const createContainingPointQuery = <R extends "u32" | "u64">(
	points: NumberFromType<R>[] | NumberFromType<R>,
	options?: {
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	const or: Query[] = [];
	for (const point of Array.isArray(points) ? points : [points]) {
		or.push(
			new And([
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: point,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.Greater,
					value: point,
				}),
			]),
		);
		or.push(
			new And([
				new IntegerCompare({
					key: "start2",
					compare: Compare.LessOrEqual,
					value: point,
				}),
				new IntegerCompare({
					key: "end2",
					compare: Compare.Greater,
					value: point,
				}),
			]),
		);
	}
	if (options?.time) {
		let queries = [
			new Or(or),
			new IntegerCompare({
				key: "timestamp",
				compare: options.time.matured ? Compare.LessOrEqual : Compare.Greater,
				value: BigInt(options.time.now - options.time.roleAgeLimit),
			}),
		];
		return queries;
	} else {
		return new Or(or);
	}
};

const createContainingPartialPointQuery = <R extends "u32" | "u64">(
	point: NumberFromType<R>,
	first: boolean,
	options?: {
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	let query: Query[];
	if (first) {
		query = [
			new IntegerCompare({
				key: "start1",
				compare: Compare.LessOrEqual,
				value: point,
			}),
			new IntegerCompare({
				key: "end1",
				compare: Compare.Greater,
				value: point,
			}),
		];
	} else {
		query = [
			new IntegerCompare({
				key: "start2",
				compare: Compare.LessOrEqual,
				value: point,
			}),
			new IntegerCompare({
				key: "end2",
				compare: Compare.Greater,
				value: point,
			}),
		];
	}

	if (options?.time) {
		query.push(
			new IntegerCompare({
				key: "timestamp",
				compare: options.time.matured ? Compare.LessOrEqual : Compare.Greater,
				value: BigInt(options.time.now - options.time.roleAgeLimit),
			}),
		);
	}

	return query;
};

const iterateRangesContainingPoint = <
	S extends Shape | undefined,
	R extends "u32" | "u64",
>(
	rects: Index<ReplicationRangeIndexable<R>>,
	points: NumberFromType<R>[] | NumberFromType<R>,

	options?: {
		shape?: S;
		sort?: Sort[];
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
): IndexIterator<ReplicationRangeIndexable<R>, S> => {
	// point is between 0 and 1, and the range can start at any offset between 0 and 1 and have length between 0 and 1

	return rects.iterate(
		{
			query: createContainingPointQuery(points, {
				time: options?.time,
			}), // new Or(points.map(point => new And(createContainingPointQuery(point, roleAgeLimit, matured, now)))
			sort: options?.sort,
		},
		options,
	);
};

const allRangesContainingPoint = async <
	S extends Shape | undefined,
	R extends "u32" | "u64",
>(
	rects: Index<ReplicationRangeIndexable<R>>,
	points: NumberFromType<R>[] | NumberFromType<R>,
	options?: {
		shape?: S;
		sort?: Sort[];
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	// point is between 0 and 1, and the range can start at any offset between 0 and 1 and have length between 0 and 1

	let allResults: IndexedResult<
		ReturnTypeFromShape<ReplicationRangeIndexable<R>, S>
	>[] = [];
	for (const point of Array.isArray(points) ? points : [points]) {
		const firstIterator = rects.iterate(
			{
				query: createContainingPartialPointQuery(point, false, options),
				sort: options?.sort,
			},
			options,
		);

		const secondIterator = rects.iterate(
			{
				query: createContainingPartialPointQuery(point, true, options),
				sort: options?.sort,
			},
			options,
		);

		[...(await firstIterator.all()), ...(await secondIterator.all())].forEach(
			(x) => allResults.push(x),
		);
	}
	return allResults;
	/* return [...await iterateRangesContainingPoint(rects, points, options).all()]; */
};

const countRangesContainingPoint = async <R extends "u32" | "u64">(
	rects: Index<ReplicationRangeIndexable<R>>,
	point: NumberFromType<R>,
	options?: {
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	return rects.count({
		query: createContainingPointQuery(point, options),
	});
};

export const appromixateCoverage = async <R extends "u32" | "u64">(properties: {
	peers: Index<ReplicationRangeIndexable<R>>;
	samples: number;
	numbers: Numbers<R>;
	roleAge?: number;
	normalized?: boolean; // if true, we dont care about the actual number of ranges, only if there is a range, hence the output will be between 0 and 1
}) => {
	const grid = properties.numbers.getGrid(
		properties.numbers.zero,
		properties.samples,
	);
	/* const now = +new Date(); */
	let hits = 0;
	for (const point of grid) {
		const count = await countRangesContainingPoint(
			properties.peers,
			point,
			/* properties?.roleAge ?? 0,
			true,
			now, */
		);
		hits += properties.normalized ? (count > 0 ? 1 : 0) : count;
	}
	return hits / properties.samples;
};
export const calculateCoverage = async <R extends "u32" | "u64">(properties: {
	peers: Index<ReplicationRangeIndexable<R>>;
	numbers: Numbers<R>;
	/** Optional: start of the content range (inclusive) */
	start?: NumberFromType<R>;
	/** Optional: end of the content range (exclusive) */
	end?: NumberFromType<R>;

	/** Optional: role age limit in milliseconds */
	roleAge?: number;
}): Promise<number> => {
	// Use the provided content range if given; otherwise use the default full range.
	const contentStart = properties.start ?? properties.numbers.zero;
	const contentEnd = properties.end ?? properties.numbers.maxValue;

	// Optional: Validate that the range is nonempty.
	if (contentStart > contentEnd) {
		// calculate coveragare for two ranges and take the min (wrapped)
		const coverage1 = await calculateCoverage({
			peers: properties.peers,
			numbers: properties.numbers,
			start: contentStart,
			end: properties.numbers.maxValue,
			roleAge: properties.roleAge,
		});
		const coverage2 = await calculateCoverage({
			peers: properties.peers,
			numbers: properties.numbers,
			start: properties.numbers.zero,
			end: contentEnd,
			roleAge: properties.roleAge,
		});

		return Math.min(coverage1, coverage2);
	}

	const endpoints: { point: NumberFromType<R>; delta: -1 | 1 }[] = [];

	// For each range, record its start and end as events.
	const timeThresholdQuery =
		properties?.roleAge != null
			? [
					new IntegerCompare({
						key: "timestamp",
						compare: Compare.LessOrEqual,
						value: BigInt(Date.now() - properties.roleAge),
					}),
				]
			: undefined;
	for (const r of await properties.peers
		.iterate({
			query: timeThresholdQuery,
		})
		.all()) {
		endpoints.push({ point: r.value.start1, delta: +1 });
		endpoints.push({ point: r.value.end1, delta: -1 });

		// Process the optional second range if it differs.
		if (r.value.start1 !== r.value.start2) {
			endpoints.push({ point: r.value.start2, delta: +1 });
			endpoints.push({ point: r.value.end2, delta: -1 });
		}
	}

	// Sort endpoints.
	// When points are equal, process a start (delta +1) before an end (delta -1)
	endpoints.sort((a, b) => {
		if (a.point === b.point) return b.delta - a.delta;
		return Number(a.point) - Number(b.point);
	});

	// If there are no endpoints at all, nothing covers the content range.
	if (endpoints.length === 0) {
		return 0;
	}

	// Process events occurring before or at contentStart so we have the correct
	// initial coverage at contentStart.
	let currentCoverage = 0;
	let idx = 0;
	while (idx < endpoints.length && endpoints[idx].point <= contentStart) {
		currentCoverage += endpoints[idx].delta;
		idx++;
	}

	// If no range covers the very beginning of the content space, return 0.
	if (currentCoverage <= 0) {
		return 0;
	}

	let minCoverage = currentCoverage;
	let lastPoint = contentStart;

	// Process remaining endpoints.
	for (; idx < endpoints.length; idx++) {
		const e = endpoints[idx];

		// Only process if the event point advances our sweep.
		if (e.point > lastPoint) {
			// Restrict the segment to our content space.
			const segStart = properties.numbers.max(lastPoint, contentStart);
			const segEnd = properties.numbers.min(e.point, contentEnd);
			if (segStart < segEnd) {
				minCoverage = Math.min(minCoverage, currentCoverage);
			}
			lastPoint = e.point;
		}

		// Once we've passed the content end, we can stop processing.
		if (lastPoint >= contentEnd) {
			break;
		}

		currentCoverage += e.delta;
	}

	// Process any tail at the end of the content range.
	if (lastPoint < contentEnd) {
		minCoverage = Math.min(minCoverage, currentCoverage);
	}

	// If any segment has zero (or negative) coverage, or nothing was covered, return 0.
	return minCoverage === Infinity || minCoverage <= 0 ? 0 : minCoverage;
};

const getClosest = <S extends Shape | undefined, R extends "u32" | "u64">(
	direction: "above" | "below",
	rects: Index<ReplicationRangeIndexable<R>>,
	point: NumberFromType<R>,
	includeStrict: boolean,
	numbers: Numbers<R>,
	options?: {
		shape?: S;
		hash?: string;
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
): IndexIterator<ReplicationRangeIndexable<R>, S> => {
	const createQueries = (p: NumberFromType<R>, equality: boolean) => {
		let queries: Query[];
		if (direction === "below") {
			queries = [
				new IntegerCompare({
					key: "end2",
					compare: equality ? Compare.LessOrEqual : Compare.Less,
					value: p,
				}),
			];
		} else {
			queries = [
				new IntegerCompare({
					key: "start1",
					compare: equality ? Compare.GreaterOrEqual : Compare.Greater,
					value: p,
				}),
			];
		}

		if (options?.time) {
			queries.push(
				new IntegerCompare({
					key: "timestamp",
					compare: options?.time?.matured
						? Compare.LessOrEqual
						: Compare.GreaterOrEqual,
					value: BigInt(options.time.now - options.time.roleAgeLimit),
				}),
			);
		}

		queries.push(
			new IntegerCompare({ key: "width", compare: Compare.Greater, value: 0 }),
		);

		if (!includeStrict) {
			queries.push(
				new IntegerCompare({
					key: "mode",
					compare: Compare.Equal,
					value: ReplicationIntent.NonStrict,
				}),
			);
		}
		if (options?.hash) {
			queries.push(new StringMatch({ key: "hash", value: options.hash }));
		}
		return queries;
	};

	const sortByOldest = new Sort({ key: "timestamp", direction: "asc" });
	const sortByHash = new Sort({ key: "hash", direction: "asc" }); // when breaking even

	const iterator = rects.iterate(
		{
			query: createQueries(point, false),
			sort: [
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
				sortByOldest,
				sortByHash,
			],
		},
		options,
	);

	const iteratorWrapped = rects.iterate(
		{
			query: createQueries(
				direction === "below" ? numbers.maxValue : numbers.zero,
				true,
			),
			sort: [
				direction === "below"
					? new Sort({ key: ["end2"], direction: "desc" })
					: new Sort({ key: ["start1"], direction: "asc" }),
				sortByOldest,
				sortByHash,
			],
		},
		options,
	);

	return joinIterator<S, R>(
		[iterator, iteratorWrapped],
		point,
		direction,
		numbers,
	);
};

export const getCoveringRangeQuery = (range: {
	start1: number | bigint;
	end1: number | bigint;
	start2: number | bigint;
	end2: number | bigint;
}) => {
	return [
		new Or([
			new And([
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: range.start1,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.GreaterOrEqual,
					value: range.end1,
				}),
			]),
			new And([
				new IntegerCompare({
					key: "start2",
					compare: Compare.LessOrEqual,
					value: range.start1,
				}),
				new IntegerCompare({
					key: "end2",
					compare: Compare.GreaterOrEqual,
					value: range.end1,
				}),
			]),
		]),
		new Or([
			new And([
				new IntegerCompare({
					key: "start1",
					compare: Compare.LessOrEqual,
					value: range.start2,
				}),
				new IntegerCompare({
					key: "end1",
					compare: Compare.GreaterOrEqual,
					value: range.end2,
				}),
			]),
			new And([
				new IntegerCompare({
					key: "start2",
					compare: Compare.LessOrEqual,
					value: range.start2,
				}),
				new IntegerCompare({
					key: "end2",
					compare: Compare.GreaterOrEqual,
					value: range.end2,
				}),
			]),
		]),
	];
};
export const countCoveringRangesSameOwner = async <R extends "u32" | "u64">(
	rects: Index<ReplicationRangeIndexable<R>>,
	range: ReplicationRangeIndexable<R>,
) => {
	return (
		(await rects.count({
			query: [
				...getCoveringRangeQuery(range),
				new StringMatch({
					key: "hash",
					value: range.hash,
				}),
				// assume that we are looking for other ranges, not want to update an existing one
				new Not(
					new ByteMatchQuery({
						key: "id",
						value: range.id,
					}),
				),
			],
		})) > 0
	);
};

export const getCoveringRangesSameOwner = <R extends "u32" | "u64">(
	rects: Index<ReplicationRangeIndexable<R>>,
	range: {
		start1: number | bigint;
		end1: number | bigint;
		start2: number | bigint;
		end2: number | bigint;
		hash: string;
		id: Uint8Array;
	},
) => {
	return rects.iterate({
		query: [
			...getCoveringRangeQuery(range),
			new StringMatch({
				key: "hash",
				value: range.hash,
			}),
			// assume that we are looking for other ranges, not want to update an existing one
			new Not(
				new ByteMatchQuery({
					key: "id",
					value: range.id,
				}),
			),
		],
	});
};

// TODO
export function getDistance(
	from: any,
	to: any,
	direction: "above" | "below" | "closest",
	end: any,
): any {
	const abs = (value: number | bigint): number | bigint =>
		value < 0 ? -value : value;
	const diff = <T extends number | bigint>(a: T, b: T): T => abs(a - b) as T;

	if (direction === "closest") {
		if (from === to) {
			return typeof from === "number" ? 0 : 0n; // returns 0 of the correct type
		}
		return diff(from, to) < diff(end, diff(from, to))
			? diff(from, to)
			: diff(end, diff(from, to));
	}

	if (direction === "above") {
		if (from <= to) {
			return end - to + from;
		}
		return from - to;
	}

	if (direction === "below") {
		if (from >= to) {
			return end - from + to;
		}
		return to - from;
	}

	throw new Error("Invalid direction");
}

const joinIterator = <S extends Shape | undefined, R extends "u32" | "u64">(
	iterators: IndexIterator<ReplicationRangeIndexable<R>, S>[],
	point: NumberFromType<R>,
	direction: "above" | "below" | "closest",
	numbers: Numbers<R>,
): IndexIterator<ReplicationRangeIndexable<R>, S> => {
	let queues: {
		elements: {
			result: IndexedResult<
				ReturnTypeFromShape<ReplicationRangeIndexable<R>, S>
			>;
			dist: NumberFromType<R>;
		}[];
	}[] = [];

	return {
		next: async (
			count: number,
		): Promise<
			IndexedResults<ReturnTypeFromShape<ReplicationRangeIndexable<R>, S>>
		> => {
			let results: IndexedResults<
				ReturnTypeFromShape<ReplicationRangeIndexable<R>, S>
			> = [];
			for (let i = 0; i < iterators.length; i++) {
				let queue = queues[i];
				if (!queue) {
					queue = { elements: [] };
					queues[i] = queue;
				}
				let iterator = iterators[i];
				if (queue.elements.length < count && iterator.done() !== true) {
					let res = await iterator.next(count);

					for (const el of res) {
						const closest = el.value;

						let dist: NumberFromType<R>;
						if (direction === "closest") {
							dist = numbers.min(
								getDistance(
									closest.start1,
									point as any,
									direction,
									numbers.maxValue as any,
								) as NumberFromType<R>,
								getDistance(
									closest.end2,
									point as any,
									direction,
									numbers.maxValue as any,
								) as NumberFromType<R>,
							);
						} else if (direction === "above") {
							dist = getDistance(
								closest.start1,
								point as any,
								direction,
								numbers.maxValue as any,
							) as NumberFromType<R>;
						} else if (direction === "below") {
							dist = getDistance(
								closest.end2,
								point as any,
								direction,
								numbers.maxValue as any,
							) as NumberFromType<R>;
						} else {
							throw new Error("Invalid direction");
						}

						queue.elements.push({ result: el, dist });
					}
				}
			}

			// pull the 'count' the closest element from one of the queue

			for (let i = 0; i < count; i++) {
				let closestQueue = -1;
				let closestDist: bigint | number = Number.MAX_VALUE;
				for (let j = 0; j < queues.length; j++) {
					let queue = queues[j];
					if (queue && queue.elements.length > 0) {
						let closest = queue.elements[0];
						if (closest.dist < closestDist) {
							closestDist = closest.dist;
							closestQueue = j;
						}
					}
				}

				if (closestQueue === -1) {
					break;
				}

				let closest = queues[closestQueue]?.elements.shift();
				if (closest) {
					results.push(closest.result);
				}
			}
			return results;
		},
		pending: async () => {
			let allPending = await Promise.all(iterators.map((x) => x.pending()));
			return allPending.reduce((acc, x) => acc + x, 0);
		},
		done: () => iterators.every((x) => x.done() === true),
		close: async () => {
			for (const iterator of iterators) {
				await iterator.close();
			}
		},
		all: async () => {
			let results: IndexedResult<
				ReturnTypeFromShape<ReplicationRangeIndexable<R>, S>
			>[] = [];
			for (const iterator of iterators) {
				let res = await iterator.all();
				results.push(...res);
			}
			return results;
		},
	};
};

const getClosestAroundOrContaining = <
	S extends (Shape & { timestamp: true }) | undefined,
	R extends "u32" | "u64",
>(
	peers: Index<ReplicationRangeIndexable<R>>,
	point: NumberFromType<R>,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
	numbers: Numbers<R>,
	options?: {
		shape?: S;
		hash?: string;
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	const closestBelow = getClosest<S, R>(
		"below",
		peers,
		point,
		includeStrictBelow,
		numbers,
		options,
	);
	const closestAbove = getClosest<S, R>(
		"above",
		peers,
		point,
		includeStrictAbove,
		numbers,
		options,
	);
	const containing = iterateRangesContainingPoint<S, R>(peers, point, options);

	return iteratorInSeries(
		containing,
		joinIterator<S, R>([closestBelow, closestAbove], point, "closest", numbers),
	);
};

export const getAdjecentSameOwner = async <R extends "u32" | "u64">(
	peers: Index<ReplicationRangeIndexable<R>>,
	range: {
		idString?: string;
		start1: NumberFromType<R>;
		end2: NumberFromType<R>;
		hash: string;
	},
	numbers: Numbers<R>,
): Promise<{
	below?: ReplicationRangeIndexable<R>;
	above?: ReplicationRangeIndexable<R>;
}> => {
	const closestBelowIterator = getClosest<undefined, R>(
		"below",
		peers,
		range.start1,
		true,
		numbers,
		{
			hash: range.hash,
		},
	);
	const closestBelow = await closestBelowIterator.next(1);
	closestBelowIterator.close();
	const closestAboveIterator = getClosest<undefined, R>(
		"above",
		peers,
		range.end2,
		true,
		numbers,
		{
			hash: range.hash,
		},
	);
	const closestAbove = await closestAboveIterator.next(1);
	closestAboveIterator.close();
	return {
		below:
			range.idString === closestBelow[0]?.value.idString
				? undefined
				: closestBelow[0]?.value,
		above:
			closestBelow[0]?.id.primitive === closestAbove[0]?.id.primitive ||
			range.idString === closestBelow[0]?.value.idString
				? undefined
				: closestAbove[0]?.value,
	};
};

export const getAllMergeCandiates = async <R extends "u32" | "u64">(
	peers: Index<ReplicationRangeIndexable<R>>,
	range: {
		idString?: string;
		start1: NumberFromType<R>;
		start2: NumberFromType<R>;
		end1: NumberFromType<R>;
		end2: NumberFromType<R>;
		hash: string;
		id: Uint8Array;
	},
	numbers: Numbers<R>,
): Promise<Map<string, ReplicationRangeIndexable<R>>> => {
	const adjacent = await getAdjecentSameOwner(peers, range, numbers);
	const covering = await getCoveringRangesSameOwner(peers, range).all();

	let ret: Map<string, ReplicationRangeIndexable<R>> = new Map();
	if (adjacent.below) {
		ret.set(adjacent.below.idString, adjacent.below);
	}
	if (adjacent.above) {
		ret.set(adjacent.above.idString, adjacent.above);
	}
	for (const range of covering) {
		ret.set(range.value.idString, range.value);
	}
	return ret;
};

export const isMatured = (
	segment: { timestamp: bigint },
	now: number,
	minAge: number,
) => {
	return now - Number(segment.timestamp) >= minAge;
};
const collectClosestAround = async <R extends "u32" | "u64">(
	roleAge: number,
	peers: Index<ReplicationRangeIndexable<R>>,
	collector: (rect: { hash: string }, matured: boolean) => void,
	point: NumberFromType<R>,
	now: number,
	numbers: Numbers<R>,
	done: () => boolean = () => true,
) => {
	const closestBelow = getClosest<undefined, R>(
		"below",
		peers,
		point,
		false,
		numbers,
	);
	const closestAbove = getClosest<undefined, R>(
		"above",
		peers,
		point,
		false,
		numbers,
	);

	const aroundIterator = joinIterator<undefined, R>(
		[/* containingIterator,  */ closestBelow, closestAbove],
		point,
		"closest",
		numbers,
	);

	let visited = new Set<string>();
	while (aroundIterator.done() !== true && done() !== true) {
		const res = await aroundIterator.next(100);
		for (const rect of res) {
			visited.add(rect.value.idString);
			collector(rect.value, isMatured(rect.value, now, roleAge));
			if (done()) {
				return;
			}
		}
	}
};

// get peer sample that are responsible for the cursor point
// will return a list of peers that want to replicate the data,
// but also if necessary a list of peers that are responsible for the data
// but have not explicitly replicating a range that cover the cursor point
export const getSamples = async <R extends "u32" | "u64">(
	cursor: NumberFromType<R>[],
	peers: Index<ReplicationRangeIndexable<R>>,
	roleAge: number,
	numbers: Numbers<R>,
	options?: {
		onlyIntersecting?: boolean;
		uniqueReplicators?: Set<string>;
		peerFilter?: Set<string>;
	},
): Promise<Map<string, { intersecting: boolean }>> => {
	const leaders: Map<string, { intersecting: boolean }> = new Map();
	if (!peers) {
		return new Map();
	}

	const now = +new Date();
	let matured = 0;

	let uniqueVisited = new Set<string>();
	const peerFilter = options?.peerFilter;
	for (let i = 0; i < cursor.length; i++) {
		let point = cursor[i];

		const allContaining = await allRangesContainingPoint<undefined, R>(
			peers,
			point,
		);

		for (const rect of allContaining) {
			if (peerFilter && !peerFilter.has(rect.value.hash)) {
				continue;
			}
			uniqueVisited.add(rect.value.hash);
			let prev = leaders.get(rect.value.hash);
			if (!prev) {
				if (isMatured(rect.value, now, roleAge)) {
					matured++;
				}
				leaders.set(rect.value.hash, { intersecting: true });
			} else {
				prev.intersecting = true;
			}
		}

		if (options?.uniqueReplicators && options.uniqueReplicators.size > 0) {
			if (
				options.uniqueReplicators.size === leaders.size ||
				options.uniqueReplicators.size === uniqueVisited.size
			) {
				break; // nothing more to find
			}
		}

		if (options?.onlyIntersecting || matured > i) {
			continue;
		}

		let foundOneUniqueMatured = false;
		await collectClosestAround(
			roleAge,
			peers,
			(rect, m) => {
				if (peerFilter && !peerFilter.has(rect.hash)) {
					return;
				}
				uniqueVisited.add(rect.hash);
				const prev = leaders.get(rect.hash);
				if (m) {
					if (!prev) {
						matured++;
						leaders.set(rect.hash, { intersecting: false });
					}
					if (matured > i) {
						foundOneUniqueMatured = true;
					}
				}
			},
			point,
			now,
			numbers,
			() => foundOneUniqueMatured,
		);
		/* if (!foundOneUniqueMatured) {
			missingForCursors.push(point);
		} */
	}
	/* if (leaders.size < cursor.length) {
		throw new Error("Missing leaders got: " + leaders.size + " -- expected -- " + cursor.length + " role age " + roleAge + " missing " + missingForCursors.length + " replication index size: " + (await peers.count()));
	} */

	return leaders;
};

const fetchOne = async <S extends Shape | undefined, R extends "u32" | "u64">(
	iterator: IndexIterator<ReplicationRangeIndexable<R>, S>,
) => {
	const value = await iterator.next(1);
	await iterator.close();
	return value[0]?.value;
};

export const minimumWidthToCover = async <R extends "u32" | "u64">(
	minReplicas: number /* , replicatorCount: number */,
	numbers: Numbers<R>,
) => {
	/* minReplicas = Math.min(minReplicas, replicatorCount); */ // TODO do we need this?

	// If min replicas = 2
	// then we need to make sure we cover 0.5 of the total 'width' of the replication space
	// to make sure we reach sufficient amount of nodes such that at least one one has
	// the entry we are looking for

	let widthToCoverScaled = numbers.divRound(numbers.maxValue, minReplicas);
	return widthToCoverScaled;
};

export const getCoverSet = async <R extends "u32" | "u64">(properties: {
	peers: Index<ReplicationRangeIndexable<R>>;
	start: NumberFromType<R> | PublicSignKey | undefined;
	widthToCoverScaled: NumberFromType<R>;
	roleAge: number;
	numbers: Numbers<R>;
	eager?:
		| {
				unmaturedFetchCoverSize?: number;
		  }
		| boolean;
}): Promise<Set<string>> => {
	const { peers, start, widthToCoverScaled, roleAge } = properties;

	const now = Date.now();
	const { startNode, startLocation, endLocation } = await getStartAndEnd<
		undefined,
		R
	>(peers, start, widthToCoverScaled, properties.numbers, {
		time: {
			roleAgeLimit: roleAge,
			now,
			matured: true,
		},
	});

	let ret = new Set<string>();

	// if start node (assume is self) and not mature, ask all known remotes if limited
	// TODO consider a more robust stragety here in a scenario where there are many nodes, lets say
	// a social media app with 1m user, then it does not makes sense to query "all" just because we started
	if (properties.eager) {
		const eagerFetch =
			properties.eager === true
				? 1000
				: (properties.eager.unmaturedFetchCoverSize ?? 1000);

		// pull all umatured
		const iterator = peers.iterate({
			query: [
				new IntegerCompare({
					key: "timestamp",
					compare: Compare.GreaterOrEqual,
					value: BigInt(now - roleAge),
				}),
			],
		});
		const rects = await iterator.next(eagerFetch);
		await iterator.close();
		for (const rect of rects) {
			ret.add(rect.value.hash);
		}
	}

	const endIsWrapped = endLocation <= startLocation;

	if (!startNode) {
		return ret;
	}

	let current = startNode;

	// push edges
	ret.add(current.hash);

	const resolveNextContaining = async (
		nextLocation: NumberFromType<R>,
		roleAge: number,
	) => {
		const next = await fetchOne(
			iterateRangesContainingPoint<undefined, R>(peers, nextLocation, {
				sort: [new Sort({ key: "end2", direction: SortDirection.DESC })],
				time: {
					matured: true,
					roleAgeLimit: roleAge,
					now,
				},
			}),
		); // get intersecting sort by largest end2
		return next;
	};

	const resolveNextAbove = async (
		nextLocation: NumberFromType<R>,
		roleAge: number,
	) => {
		// if not get closest from above
		const next = await fetchOne<undefined, R>(
			getClosest("above", peers, nextLocation, true, properties.numbers, {
				time: {
					matured: true,
					roleAgeLimit: roleAge,
					now,
				},
			}),
		);
		return next;
	};

	const resolveNext = async (
		nextLocation: NumberFromType<R>,
		roleAge: number,
	): Promise<[ReplicationRangeIndexable<R> | undefined, boolean]> => {
		const containing = await resolveNextContaining(nextLocation, roleAge);
		if (containing) {
			return [containing, true];
		}
		return [await resolveNextAbove(nextLocation, roleAge), false];
	};

	// fill the middle

	let coveredLength = properties.numbers.zero;

	let startIsMature = isMatured(startNode, now, roleAge);

	let wrappedOnce = false;

	const addLength = (
		to: ReplicationRangeIndexable<R>,
		from: NumberFromType<R>,
	) => {
		const toEnd2 = properties.numbers.increment(to.end2); // TODO investigate why this is needed
		if (toEnd2 < from) {
			wrappedOnce = true;
			// @ts-ignore
			coveredLength += properties.numbers.maxValue - from;
			// @ts-ignore
			coveredLength += toEnd2;
		} else if (to.wrapped) {
			// When the range is wrapped and `from` is in the second segment (near zero),
			// the distance to `end2` does not wrap. Otherwise we must wrap to reach `end2`.
			if (from < to.end2) {
				// @ts-ignore
				coveredLength += toEnd2 - from;
			} else {
				wrappedOnce = true;
				// @ts-ignore
				coveredLength += properties.numbers.maxValue - from;
				// @ts-ignore
				coveredLength += toEnd2;
			}
		} else {
			// @ts-ignore
			coveredLength += to.end1 - from;
		}
	};

	addLength(current, startLocation);

	let maturedCoveredLength = startIsMature
		? coveredLength
		: 0; /* TODO we only increase matured length when startNode is matured? i.e. do isMatured(startNode, now, roleAge) ? coveredLength : 0;, however what is the optimal choice here? */
	let nextLocation = current.end2; /* startIsMature
		? current.end2
		: properties.numbers.increment(current.start1);  */ // <--- this clause does not seem to work as expected (run ranges tests to see why)*/

	while (
		maturedCoveredLength < widthToCoverScaled && // eslint-disable-line no-unmodified-loop-condition
		(coveredLength <= properties.numbers.maxValue || !wrappedOnce) // eslint-disable-line no-unmodified-loop-condition
	) {
		let distanceBefore = coveredLength;
		const nextLocationBefore = nextLocation;

		let nextCandidate = await resolveNext(nextLocation, roleAge);
		let matured = true;

		if (!nextCandidate[0]) {
			matured = false;
			nextCandidate = await resolveNext(nextLocation, 0);
		}

		if (!nextCandidate[0]) {
			break;
		}

		let nextIsCurrent = equals(nextCandidate[0].id, current.id);
		let extraDistanceForNext = false;
		if (nextIsCurrent) {
			let containing = nextCandidate[1];
			if (containing) {
				extraDistanceForNext = true;
			} else {
				break;
			}
		}

		addLength(nextCandidate[0], nextLocation);

		let last = current;
		current = nextCandidate[0];

		const isLast =
			distanceBefore < widthToCoverScaled &&
			coveredLength >= widthToCoverScaled;

		const lastDistanceToEndLocation = properties.numbers.min(
			getDistance(
				last.start1,
				endLocation,
				"closest",
				properties.numbers.maxValue,
			),
			getDistance(
				last.end2,
				endLocation,
				"closest",
				properties.numbers.maxValue,
			),
		);

		const currentDistanceToEndLocation = properties.numbers.min(
			getDistance(
				current.start1,
				endLocation,
				"closest",
				properties.numbers.maxValue,
			),
			getDistance(
				current.end2,
				endLocation,
				"closest",
				properties.numbers.maxValue,
			),
		);

		if (
			!isLast ||
			nextCandidate[1] ||
			lastDistanceToEndLocation >= currentDistanceToEndLocation
		) {
			ret.add(current.hash);
		}

		if (matured) {
			maturedCoveredLength = coveredLength;
		}

		let startForNext = extraDistanceForNext
			? properties.numbers.increment(nextLocation)
			: current.end2;
		nextLocation = endIsWrapped
			? wrappedOnce
				? properties.numbers.min(startForNext, endLocation)
				: startForNext
			: properties.numbers.min(startForNext, endLocation);

		// Safety: ensure we always make progress to avoid infinite loops (can happen when
		// the chosen range is the same and `nextLocation` doesn't advance).
		if (
			nextLocation === nextLocationBefore &&
			coveredLength === distanceBefore
		) {
			break;
		}

		if (
			(typeof nextLocation === "bigint" &&
				nextLocation === (endLocation as bigint)) ||
			(typeof nextLocation === "number" &&
				nextLocation === (endLocation as number))
		) {
			break;
		}
	}

	start instanceof PublicSignKey && ret.add(start.hashcode());
	return ret;
};

export const matchEntriesInRangeQuery = (range: {
	start1: number | bigint;
	end1: number | bigint;
	start2: number | bigint;
	end2: number | bigint;
}) => {
	const c1 = new And([
		new IntegerCompare({
			key: "coordinates",
			compare: "gte",
			value: range.start1,
		}),
		new IntegerCompare({
			key: "coordinates",
			compare: "lt",
			value: range.end1,
		}),
	]);

	// if range2 has length 0 or range 2 is equal to range 1 only make one query
	if (
		range.start2 === range.end2 ||
		(range.start1 === range.start2 && range.end1 === range.end2)
	) {
		return c1;
	}

	let ors = [
		c1,
		new And([
			new IntegerCompare({
				key: "coordinates",
				compare: "gte",
				value: range.start2,
			}),
			new IntegerCompare({
				key: "coordinates",
				compare: "lt",
				value: range.end2,
			}),
		]),
	];
	return new Or(ors);
};

export const createAssignedRangesQuery = (
	changes: {
		range: {
			start1: number | bigint;
			end1: number | bigint;
			start2: number | bigint;
			end2: number | bigint;
			mode: ReplicationIntent;
		};
	}[],
	options?: { strict?: boolean },
) => {
	let ors: Query[] = [];
	let onlyStrict = true;
	// TODO what if the ranges are many many?
	for (const change of changes) {
		const matchRange = matchEntriesInRangeQuery(change.range);
		ors.push(matchRange);
		if (change.range.mode === ReplicationIntent.NonStrict) {
			onlyStrict = false;
		}
	}

	// entry is assigned to a range boundary, meaning it is due to be inspected
	if (!options?.strict) {
		if (!onlyStrict || changes.length === 0) {
			ors.push(
				new BoolQuery({
					key: "assignedToRangeBoundary",
					value: true,
				}),
			);
		}
	}

	// entry is not sufficiently replicated, and we are to still keep it
	return new Or(ors);
};

export type ReplicationChanges<
	T extends ReplicationRangeIndexable<any> = ReplicationRangeIndexable<any>,
> = ReplicationChange<T>[];
export type ReplicationChange<
	T extends ReplicationRangeIndexable<any> = ReplicationRangeIndexable<any>,
> = (
	| {
			type: "added";
			range: T;
			matured?: boolean;
	  }
	| {
			type: "removed";
			range: T;
	  }
	| {
			type: "replaced";
			range: T;
	  }
) & { timestamp: bigint };

export const debounceAggregationChanges = <
	T extends ReplicationRangeIndexable<any>,
>(
	fn: (changeOrChanges: ReplicationChange<T>[]) => void,
	delay: number,
) => {
	return debounceAccumulator(
		(result) => {
			if (result.size === 0) {
				return;
			}
			return fn([...result.values()]);
		},
		() => {
			let aggregated: Map<string, ReplicationChange<T>> = new Map();
			return {
				add: (change: ReplicationChange<T>) => {
					// Keep different change types for the same segment id. In particular, range
					// updates produce a `replaced` + `added` pair; collapsing by id would drop the
					// "removed" portion and prevent correct rebalancing/pruning.
					const key = `${change.type}:${change.range.idString}`;
					const prev = aggregated.get(key);
					if (prev) {
						if (prev.range.timestamp < change.range.timestamp) {
							aggregated.set(key, change);
						}
					} else {
						aggregated.set(key, change);
					}
				},
				delete: (key: string) => {
					aggregated.delete(key);
				},
				size: () => aggregated.size,
				value: aggregated,
				has: (key: string) => aggregated.has(key),
			};
		},
		delay,
	);
};

export const mergeReplicationChanges = <R extends NumericType>(
	changesOrChangesArr:
		| ReplicationChanges<ReplicationRangeIndexable<R>>
		| ReplicationChanges<ReplicationRangeIndexable<R>>[],
	rebalanceHistory: Cache<string>,
): ReplicationChange<ReplicationRangeIndexable<R>>[] => {
	let first = changesOrChangesArr[0];
	let changes: ReplicationChange<ReplicationRangeIndexable<R>>[];
	if (!Array.isArray(first)) {
		changes = changesOrChangesArr as ReplicationChange<
			ReplicationRangeIndexable<R>
		>[];
	} else {
		changes = changesOrChangesArr.flat() as ReplicationChange<
			ReplicationRangeIndexable<R>
		>[];
	}

	// group by hash so we can cancel out changes
	const grouped = new Map<
		string,
		ReplicationChange<ReplicationRangeIndexable<R>>[]
	>();
	for (const change of changes) {
		const prev = grouped.get(change.range.hash);
		if (prev) {
			prev.push(change);
		} else {
			grouped.set(change.range.hash, [change]);
		}
	}

	let all: ReplicationChange<ReplicationRangeIndexable<R>>[] = [];
	for (const [_k, v] of grouped) {
		if (v.length > 1) {
			// sort by timestamp so newest is last
			v.sort((a, b) =>
				a.range.timestamp < b.range.timestamp
					? -1
					: a.range.timestamp > b.range.timestamp
						? 1
						: 0,
			);

			let results: ReplicationChange<ReplicationRangeIndexable<R>>[] = [];
			let consumed: Set<number> = new Set();
			for (let i = 0; i < v.length; i++) {
				// If segment is removed and we have previously processed it then go over each
				// overlapping added segment and remove the overlap. Equivalent to: (1 - 1 + 1) = 1.
				if (v[i].type === "removed" || v[i].type === "replaced") {
					if (rebalanceHistory.has(v[i].range.rangeHash)) {
						let adjusted = false;
						const vStart = v.length;
						for (let j = i + 1; j < vStart; j++) {
							const newer = v[j];
							if (newer.type === "added" && !newer.matured) {
								adjusted = true;
								const {
									rangesFromA: updatedRemoved,
									rangesFromB: updatedNewer,
								} = symmetricDifferenceRanges(v[i].range, newer.range);

								for (const diff of updatedRemoved) {
									results.push({
										range: diff,
										type: "removed" as const,
										timestamp: v[i].timestamp,
									});
								}
								for (const diff of updatedNewer) {
									v.push({
										range: diff,
										type: "added" as const,
										timestamp: newer.timestamp,
									});
								}
								consumed.add(j);
							}
						}
						rebalanceHistory.del(v[i].range.rangeHash);
						if (!adjusted) {
							results.push(v[i]);
						}
					} else {
						results.push(v[i]);
					}
				} else if (v[i].type === "added") {
					// TODO should the below clause be used?
					// after testing it seems that certain changes are not propagating as expected using this
					/* if (rebalanceHistory.has(v[i].range.rangeHash)) {
						continue;
					} */

					rebalanceHistory.add(v[i].range.rangeHash);
					if (!consumed.has(i)) {
						results.push(v[i]);
					}
				} else {
					results.push(v[i]);
				}
			}

			all.push(...results);
		} else {
			rebalanceHistory.add(v[0].range.rangeHash);
			all.push(v[0]);
		}
	}
	return all;
};

export const toRebalance = <R extends "u32" | "u64">(
	changeOrChanges:
		| ReplicationChanges<ReplicationRangeIndexable<R>>
		| ReplicationChanges<ReplicationRangeIndexable<R>>[],
	index: Index<EntryReplicated<R>>,
	rebalanceHistory: Cache<string>,
): AsyncIterable<EntryReplicated<R>> => {
	const change = mergeReplicationChanges(changeOrChanges, rebalanceHistory);
	return {
		[Symbol.asyncIterator]: async function* () {
			const iterator = index.iterate({
				query: createAssignedRangesQuery(change),
			});

			while (iterator.done() !== true) {
				const entries = await iterator.all(); // TODO choose right batch sizes here for optimal memory usage / speed
				for (const entry of entries) {
					yield entry.value;
				}
			}
		},
	};
};

export const fetchOneFromPublicKey = async <
	S extends (Shape & { timestamp: true }) | undefined,
	R extends "u32" | "u64",
>(
	publicKey: PublicSignKey,
	index: Index<ReplicationRangeIndexable<R>>,
	numbers: Numbers<R>,
	options?: {
		shape?: S;
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	let iterator = index.iterate<S>(
		{
			query: [new StringMatch({ key: "hash", value: publicKey.hashcode() })],
		},
		options,
	);
	let result = await iterator.next(1);
	await iterator.close();
	let node = result[0]?.value;
	if (node) {
		if (
			options?.time &&
			!isMatured(node, options.time.now, options.time.roleAgeLimit)
		) {
			const matured = await fetchOne(
				getClosestAroundOrContaining<S, R>(
					index,
					node.start1,
					false,
					false,
					numbers,
					options,
				),
			);
			if (matured) {
				node = matured;
			}
		}
	}
	return node;
};

export const getStartAndEnd = async <
	S extends (Shape & { timestamp: true }) | undefined,
	R extends "u32" | "u64",
>(
	peers: Index<ReplicationRangeIndexable<R>>,
	start: NumberFromType<R> | PublicSignKey | undefined | undefined,
	widthToCoverScaled: NumberFromType<R>,
	numbers: Numbers<R>,
	options?: {
		shape?: S;
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
): Promise<{
	startNode: ReturnTypeFromShape<ReplicationRangeIndexable<R>, S> | undefined;
	startLocation: NumberFromType<R>;
	endLocation: NumberFromType<R>;
}> => {
	// find a good starting point
	let startNode:
		| ReturnTypeFromShape<ReplicationRangeIndexable<R>, S>
		| undefined = undefined;
	let startLocation: NumberFromType<R> | undefined = undefined;

	const nodeFromPoint = async (point = numbers.random()) => {
		startLocation = point;
		startNode = await fetchOneClosest<S, R>(
			peers,
			startLocation,
			false,
			true,
			numbers,
			options,
		);
	};

	if (start instanceof PublicSignKey) {
		// start at our node (local first)
		startNode = await fetchOneFromPublicKey(start, peers, numbers, options);
		if (!startNode) {
			// fetch randomly
			await nodeFromPoint();
		} else {
			startLocation = startNode.start1;
		}
	} else if (typeof start === "number" || typeof start === "bigint") {
		await nodeFromPoint(start);
	} else {
		await nodeFromPoint();
	}

	if (!startNode || startLocation == null) {
		return {
			startNode: undefined,
			startLocation: numbers.zero,
			endLocation: numbers.zero,
		};
	}

	// @ts-ignore
	let endLocation: T = (startLocation + widthToCoverScaled) % numbers.maxValue;

	// if the start node range is not containing the start point, then figure out if the startNode is ideal
	if (!startNode.contains(startLocation)) {
		let coveredDistanceToStart = numbers.zero;
		if (startNode.start1 < startLocation) {
			coveredDistanceToStart +=
				numbers.maxValue - startLocation + startNode.start1;
		} else {
			coveredDistanceToStart += ((startNode.start1 as any) -
				startLocation) as any;
		}

		// in this case, the gap to the start point is larger than the width we want to cover. Assume there are no good points
		if (
			startNode.mode === ReplicationIntent.Strict &&
			coveredDistanceToStart > widthToCoverScaled
		) {
			return {
				startNode: undefined,
				startLocation: numbers.zero,
				endLocation: numbers.zero,
			};
		}
	}

	return {
		startNode,
		startLocation,
		endLocation,
	};
};

export const fetchOneClosest = <
	S extends (Shape & { timestamp: true }) | undefined,
	R extends "u32" | "u64",
>(
	peers: Index<ReplicationRangeIndexable<R>>,
	point: NumberFromType<R>,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
	numbers: Numbers<R>,
	options?: {
		shape?: S;
		time?: {
			roleAgeLimit: number;
			matured: boolean;
			now: number;
		};
	},
) => {
	return fetchOne<S, R>(
		getClosestAroundOrContaining<S, R>(
			peers,
			point,

			includeStrictBelow,
			includeStrictAbove,
			numbers,
			options,
		),
	);
};
