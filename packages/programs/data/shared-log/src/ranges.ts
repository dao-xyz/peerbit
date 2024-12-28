import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
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
	/* 	iteratorInSeries, */
} from "@peerbit/indexer-interface";
import { id } from "@peerbit/indexer-interface";
import { Meta, ShallowMeta } from "@peerbit/log";
import {
	MAX_U32,
	MAX_U64,
	type NumberFromType,
	type Numbers,
} from "./integers.js";
import { type ReplicationChanges } from "./replication-domain.js";

export enum ReplicationIntent {
	NonStrict = 0, // indicates that the segment will be replicated and nearby data might be replicated as well
	Strict = 1, // only replicate data in the segment to the specified replicator, not any other data
}

export enum SyncStatus {
	Unsynced = 0,
	Synced = 1,
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
	gid: string;
	coordinates: NumberFromType<R>[];
	wallTime: bigint;
	assignedToRangeBoundary: boolean;
	get meta(): ShallowMeta;
}

export const isEntryReplicated = (x: any): x is EntryReplicated<any> => {
	return x instanceof EntryReplicatedU32 || x instanceof EntryReplicatedU64;
};

export class EntryReplicatedU32 implements EntryReplicated<"u32"> {
	@id({ type: "string" })
	hash: string;

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
	}) {
		this.coordinates = properties.coordinates;
		this.hash = properties.hash;
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

export class EntryReplicatedU64 implements EntryReplicated<"u64"> {
	@id({ type: "string" })
	hash: string;

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
	}) {
		this.coordinates = properties.coordinates;
		this.hash = properties.hash;
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

export interface ReplicationRangeMessage<R extends "u32" | "u64"> {
	id: Uint8Array;
	timestamp: bigint;
	get offset(): NumberFromType<R>;
	get factor(): NumberFromType<R>;
	mode: ReplicationIntent;
	toReplicationRangeIndexable(key: PublicSignKey): ReplicationRangeIndexable<R>;
}

export const isReplicationRangeMessage = (
	x: any,
): x is ReplicationRangeMessage<any> => {
	return x instanceof ReplicationRangeMessage;
};

export abstract class ReplicationRangeMessage<R extends "u32" | "u64"> {}

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
			length: this.factor,
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
			length: this.factor,
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
}

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
			length: number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		} & ({ publicKeyHash: string } | { publicKey: PublicSignKey }),
	) {
		this.id = properties.id ?? randomBytes(32);
		this.hash =
			(properties as { publicKeyHash: string }).publicKeyHash ||
			(properties as { publicKey: PublicSignKey }).publicKey.hashcode();
		this.transform({ length: properties.length, offset: properties.offset });

		this.mode = properties.mode ?? ReplicationIntent.NonStrict;
		this.timestamp = properties.timestamp || BigInt(0);
	}

	private transform(properties: { offset: number; length: number }) {
		const ranges = getSegmentsFromOffsetAndRange(
			properties.offset,
			properties.length,
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

export class ReplicationRangeIndexableU64
	implements ReplicationRangeIndexable<"u64">
{
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "string" })
	hash: string;

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
			length: bigint | number;
			mode?: ReplicationIntent;
			timestamp?: bigint;
		} & ({ publicKeyHash: string } | { publicKey: PublicSignKey }),
	) {
		this.id = properties.id ?? randomBytes(32);
		this.hash =
			(properties as { publicKeyHash: string }).publicKeyHash ||
			(properties as { publicKey: PublicSignKey }).publicKey.hashcode();
		this.transform({ length: properties.length, offset: properties.offset });

		this.mode = properties.mode ?? ReplicationIntent.NonStrict;
		this.timestamp = properties.timestamp || BigInt(0);
	}

	private transform(properties: {
		offset: bigint | number;
		length: bigint | number;
	}) {
		const ranges = getSegmentsFromOffsetAndRange(
			BigInt(properties.offset),
			BigInt(properties.length),
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

	// only allow merging segments with length 1 (trivial)
	const sameLength = segments.every((x) => x.width === 1 || x.width === 1n);
	if (!sameLength) {
		throw new Error(
			"Segments have different length, only merging of segments length 1 is supported",
		);
	}

	const sorted = segments.sort((a, b) => Number(a.start1 - b.start1));

	let calculateLargeGap = (): [NumberFromType<R>, number] => {
		let last = sorted[sorted.length - 1];
		let largestArc = numbers.zero;
		let largestArcIndex = -1;
		for (let i = 0; i < sorted.length; i++) {
			const current = sorted[i];
			if (current.start1 !== last.start1) {
				let arc = numbers.zero;
				if (current.start1 < last.end2) {
					arc += ((numbers.maxValue as any) - last.end2) as any;

					arc += (current.start1 - numbers.zero) as any;
				} else {
					arc += (current.start1 - last.end2) as any;
				}

				if (arc > largestArc) {
					largestArc = arc;
					largestArcIndex = i;
				}
			}
			last = current;
		}

		return [largestArc, largestArcIndex];
	};
	const [largestArc, largestArcIndex] = calculateLargeGap();

	let totalLengthFinal: number = numbers.maxValue - largestArc;

	if (largestArcIndex === -1) {
		return segments[0]; // all ranges are the same
	}
	// use segments[0] constructor to create a new object

	const proto = segments[0].constructor;
	return new (proto as any)({
		length: totalLengthFinal,
		offset: segments[largestArcIndex].start1,
		publicKeyHash: segments[0].hash,
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

const getClosest = <S extends Shape | undefined, R extends "u32" | "u64">(
	direction: "above" | "below",
	rects: Index<ReplicationRangeIndexable<R>>,
	point: NumberFromType<R>,
	roleAgeLimit: number,
	matured: boolean,
	now: number,
	includeStrict: boolean,
	numbers: Numbers<R>,
	options?: { shape?: S },
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
				new IntegerCompare({
					key: "timestamp",
					compare: matured ? Compare.LessOrEqual : Compare.GreaterOrEqual,
					value: BigInt(now - roleAgeLimit),
				}),
			];
		} else {
			queries = [
				new IntegerCompare({
					key: "start1",
					compare: equality ? Compare.GreaterOrEqual : Compare.Greater,
					value: p,
				}),
				new IntegerCompare({
					key: "timestamp",
					compare: matured ? Compare.LessOrEqual : Compare.GreaterOrEqual,
					value: BigInt(now - roleAgeLimit),
				}),
			];
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
export const iHaveCoveringRange = async <R extends "u32" | "u64">(
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

const getClosestAround = <
	S extends (Shape & { timestamp: true }) | undefined,
	R extends "u32" | "u64",
>(
	peers: Index<ReplicationRangeIndexable<R>>,
	point: NumberFromType<R>,
	roleAge: number,
	now: number,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
	numbers: Numbers<R>,
	options?: { shape?: S },
) => {
	const closestBelow = getClosest<S, R>(
		"below",
		peers,
		point,
		roleAge,
		true,
		now,
		includeStrictBelow,
		numbers,
		options,
	);
	const closestAbove = getClosest<S, R>(
		"above",
		peers,
		point,
		roleAge,
		true,
		now,
		includeStrictAbove,
		numbers,
		options,
	);
	/* const containing = iterateRangesContainingPoint<S, R>(
		peers,
		point,
		{
			time: {
				roleAgeLimit: roleAge,
				matured: true,
				now,
			}
		}
	);

	return iteratorInSeries(
		containing,
		joinIterator<S, R>([closestBelow, closestAbove], point, "closest", numbers),
	); */
	return joinIterator<S, R>(
		[closestBelow, closestAbove],
		point,
		"closest",
		numbers,
	);
};

export const isMatured = (
	segment: { timestamp: bigint },
	now: number,
	minAge: number,
) => {
	return now - Number(segment.timestamp) >= minAge;
};
/* 

const collectNodesAroundPoint = async <R extends "u32" | "u64">(
	roleAge: number,
	peers: Index<ReplicationRangeIndexable<R>>,
	collector: (
		rect: { hash: string },
		matured: boolean,
		intersecting: boolean,
	) => void,
	point: NumberFromType<R>,
	now: number,
	numbers: Numbers<R>,
	done: () => boolean = () => true,
) => {
	const containing = iterateRangesContainingPoint<
		{ timestamp: true, hash: true },
		R
	>(peers, point, 0, true, now, { shape: { timestamp: true, hash: true } as const });
	const allContaining = await containing.all();
	for (const rect of allContaining) {
		collector(rect.value, isMatured(rect.value, now, roleAge), true);
	}

	if (done()) {
		return;
	}

	const closestBelow = getClosest<undefined, R>(
		"below",
		peers,
		point,
		0,
		true,
		now,
		false,
		numbers
	);
	const closestAbove = getClosest<undefined, R>(
		"above",
		peers,
		point,
		0,
		true,
		now,
		false,
		numbers
	);
	const aroundIterator = joinIterator<undefined, R>(
		[closestBelow, closestAbove],
		point,
		"closest",
		numbers,
	);
	while (aroundIterator.done() !== true && done() !== true) {
		const res = await aroundIterator.next(1);
		for (const rect of res) {
			collector(rect.value, isMatured(rect.value, now, roleAge), false);
			if (done()) {
				return;
			}
		}
	}
};
 */

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
		0,
		true,
		now,
		false,
		numbers,
	);
	const closestAbove = getClosest<undefined, R>(
		"above",
		peers,
		point,
		0,
		true,
		now,
		false,
		numbers,
	);
	/* 	const containingIterator = iterateRangesContainingPoint<undefined, R>(
			peers,
			point,
		);
	 */
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
	},
): Promise<Map<string, { intersecting: boolean }>> => {
	const leaders: Map<string, { intersecting: boolean }> = new Map();
	if (!peers) {
		return new Map();
	}

	const now = +new Date();
	let matured = 0;

	/* let missingForCursors: NumberFromType<R>[] = [] */
	let uniqueVisited = new Set<string>();
	for (let i = 0; i < cursor.length; i++) {
		let point = cursor[i];

		const allContaining = await allRangesContainingPoint<undefined, R>(
			peers,
			point,
		);

		for (const rect of allContaining) {
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

		if (options?.uniqueReplicators) {
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
	>(peers, start, widthToCoverScaled, roleAge, now, properties.numbers);

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
		let next = await fetchOne(
			iterateRangesContainingPoint<undefined, R>(peers, nextLocation, {
				sort: [new Sort({ key: "end2", direction: SortDirection.DESC })],
				time: {
					matured: true,
					roleAgeLimit: roleAge,
					now,
				},
			}),
		); // get entersecting sort by largest end2
		return next;
	};

	const resolveNextAbove = async (
		nextLocation: NumberFromType<R>,
		roleAge: number,
	) => {
		// if not get closest from above
		let next = await fetchOne<undefined, R>(
			getClosest(
				"above",
				peers,
				nextLocation,
				roleAge,
				true,
				now,
				true,
				properties.numbers,
			),
		);
		return next;
	};

	const resolveNext = async (
		nextLocation: NumberFromType<R>,
		roleAge: number,
	): Promise<[ReplicationRangeIndexable<R>, boolean]> => {
		const containing = await resolveNextContaining(nextLocation, roleAge);
		if (containing) {
			return [containing, true];
		}
		return [await resolveNextAbove(nextLocation, roleAge), false];
	};

	// fill the middle
	let wrappedOnce = current.end2 < current.end1;

	let coveredLength = properties.numbers.zero;
	const addLength = (from: NumberFromType<R>) => {
		if (current.end2 < from || current.wrapped) {
			wrappedOnce = true;
			// @ts-ignore
			coveredLength += properties.numbers.maxValue - from;
			// @ts-ignore
			coveredLength += current.end2;
		} else {
			// @ts-ignore
			coveredLength += current.end1 - from;
		}
	};
	addLength(startLocation);

	let maturedCoveredLength =
		coveredLength; /* TODO only increase matured length when startNode is matured? i.e. do isMatured(startNode, now, roleAge) ? coveredLength : 0; */
	let nextLocation = current.end2;

	while (
		maturedCoveredLength < widthToCoverScaled && // eslint-disable-line no-unmodified-loop-condition
		coveredLength <= properties.numbers.maxValue // eslint-disable-line no-unmodified-loop-condition
	) {
		let nextCandidate = await resolveNext(nextLocation, roleAge);
		/* let fromAbove = false; */
		let matured = true;

		if (!nextCandidate[0]) {
			matured = false;
			nextCandidate = await resolveNext(nextLocation, 0);
			/* fromAbove = true; */
		}

		if (!nextCandidate[0]) {
			break;
		}

		let nextIsCurrent = equals(nextCandidate[0].id, current.id);
		if (nextIsCurrent) {
			break;
		}
		let last = current;
		current = nextCandidate[0];

		let distanceBefore = coveredLength;

		addLength(nextLocation);

		let isLast =
			distanceBefore < widthToCoverScaled &&
			coveredLength >= widthToCoverScaled;

		if (
			!isLast ||
			nextCandidate[1] ||
			properties.numbers.min(
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
			) >
				properties.numbers.min(
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
				)
		) {
			ret.add(current.hash);
		}

		if (isLast && !nextCandidate[1] /*  || equals(endRect.id, current.id) */) {
			break;
		}

		if (matured) {
			maturedCoveredLength = coveredLength;
		}

		nextLocation = endIsWrapped
			? wrappedOnce
				? properties.numbers.min(current.end2, endLocation)
				: current.end2
			: properties.numbers.min(current.end2, endLocation);
	}

	start instanceof PublicSignKey && ret.add(start.hashcode());
	return ret;
};
/* export const getReplicationDiff = (changes: ReplicationChange) => {
	// reduce the change set to only regions that are changed for each peer
	// i.e. subtract removed regions from added regions, and vice versa
	const result = new Map<string, { range: ReplicationRangeIndexable, added: boolean }[]>();
	
	for (const addedChange of changes.added ?? []) {
		let prev = result.get(addedChange.hash) ?? [];
		for (const [_hash, ranges] of result.entries()) {
			for (const r of ranges) {
	
			}
		}
	}
}
 */

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

	if (range.start2 === range.end2) {
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
export const toRebalance = <R extends "u32" | "u64">(
	changes: ReplicationChanges,
	index: Index<EntryReplicated<R>>,
): AsyncIterable<EntryReplicated<R>> => {
	const assignedRangesQuery = (changes: ReplicationChanges) => {
		let ors: Query[] = [];
		for (const change of changes) {
			const matchRange = matchEntriesInRangeQuery(change.range);
			if (change.type === "updated") {
				// assuming a range is to be removed, is this entry still enoughly replicated
				const prevMatchRange = matchEntriesInRangeQuery(change.prev);
				ors.push(prevMatchRange);
				ors.push(matchRange);
			} else {
				ors.push(matchRange);
			}
		}

		// entry is assigned to a range boundary, meaning it is due to be inspected
		ors.push(
			new BoolQuery({
				key: "assignedToRangeBoundary",
				value: true,
			}),
		);

		// entry is not sufficiently replicated, and we are to still keep it
		return new Or(ors);
	};
	return {
		[Symbol.asyncIterator]: async function* () {
			const iterator = index.iterate({
				query: assignedRangesQuery(changes),
			});

			while (iterator.done() !== true) {
				const entries = await iterator.all(); // TODO choose right batch sizes here for optimal memory usage / speed

				/* const grouped = await groupByGidSync(entries.map((x) => x.value));
				for (const [gid, entries] of grouped.entries()) {
					yield { gid, entries };
				} */

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
	roleAge: number,
	now: number,
	numbers: Numbers<R>,
	options?: {
		shape: S;
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
		if (!isMatured(node, now, roleAge)) {
			const matured = await fetchOne(
				getClosestAround<S, R>(
					index,
					node.start1,
					roleAge,
					now,
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
	roleAge: number,
	now: number,
	numbers: Numbers<R>,
	options?: { shape: S },
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
			roleAge,
			now,
			false,
			true,
			numbers,
			options,
		);
	};

	if (start instanceof PublicSignKey) {
		// start at our node (local first)
		startNode = await fetchOneFromPublicKey(
			start,
			peers,
			roleAge,
			now,
			numbers,
			options,
		);
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
	roleAge: number,
	now: number,
	includeStrictBelow: boolean,
	includeStrictAbove: boolean,
	numbers: Numbers<R>,
	options?: { shape?: S },
) => {
	return fetchOne<S, R>(
		getClosestAround<S, R>(
			peers,
			point,
			roleAge,
			now,
			includeStrictBelow,
			includeStrictAbove,
			numbers,
			options,
		),
	);
};
