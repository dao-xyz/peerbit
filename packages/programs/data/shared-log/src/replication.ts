import {
	variant,
	deserialize,
	serialize,
	field,
	BinaryReader,
	vec
} from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import { SEGMENT_COORDINATE_SCALE } from "./role.js";
import { id, type Index } from "@peerbit/indexer-interface";
import { PublicSignKey, equals, randomBytes } from "@peerbit/crypto";

export type ReplicationLimits = { min: MinReplicas; max?: MinReplicas };
/* 
export class ReplicatorRect {

	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: 'string' })
	hash: string;

	@field({ type: vec(ReplicationSegment) })
	segments: ReplicationSegment[];

	constructor(properties: { hash: string; segments: ReplicationSegment[] }) {
		this.id = randomBytes(32);
		this.hash = properties.hash;
		this.segments = properties.segments;
	}
};
 */

export enum ReplicationIntent {
	Explicit = 0,
	Automatic = 1
}


export const getSegmentsFromOffsetAndRange = (offset: number, factor: number): [[number, number], [number, number]] => {

	let start1 = offset;
	let end1Unscaled = offset + factor; // only add factor if it is not 1 to prevent numerical issues (like (0.9 + 1) % 1 => 0.8999999)
	let end1 = Math.min(end1Unscaled, 1);
	return [[start1, end1], end1Unscaled > 1 ? [0, (factor !== 1 ? offset + factor : offset) % 1] : [start1, end1]];

}

export class ReplicationRange {

	@field({ type: Uint8Array })
	private id: Uint8Array;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	private _offset: number;

	@field({ type: 'u32' })
	private _factor: number;

	constructor(properties: {
		id: Uint8Array;
		offset: number;
		factor: number;
		timestamp: bigint;
	}) {
		const { id, offset, factor, timestamp } = properties;
		this.id = id;
		this._offset = Math.round(offset * SEGMENT_COORDINATE_SCALE);
		this._factor = Math.round(factor * SEGMENT_COORDINATE_SCALE);
		this.timestamp = timestamp
	}


	get factor(): number {
		return this._factor / SEGMENT_COORDINATE_SCALE
	}

	get offset(): number {
		return this._offset / SEGMENT_COORDINATE_SCALE
	}

	toReplicationRangeIndexable(key: PublicSignKey): ReplicationRangeIndexable {
		return new ReplicationRangeIndexable({
			id: this.id,
			publicKeyHash: key.hashcode(),
			offset: this.offset,
			length: this.factor,
			timestamp: this.timestamp
		})
	}
}

export class ReplicationRangeIndexable {

	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: 'string' })
	hash: string;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	start1: number;

	@field({ type: 'u32' })
	end1: number;

	@field({ type: "u32" })
	start2: number;

	@field({ type: 'u32' })
	end2: number;

	@field({ type: 'u32' })
	width: number;

	@field({ type: 'u8' })
	replicationIntent: ReplicationIntent;



	constructor(properties: {
		id?: Uint8Array

		offset: number;
		length: number;
		replicationIntent?: ReplicationIntent;
		timestamp?: bigint;
	} & ({ publicKeyHash: string } | { publicKey: PublicSignKey })) {
		this.id = properties.id ?? randomBytes(32);
		this.hash = (properties as { publicKeyHash: string }).publicKeyHash || (properties as { publicKey: PublicSignKey }).publicKey.hashcode();
		this.transform({ length: properties.length, offset: properties.offset })
		this.replicationIntent = properties.replicationIntent ?? ReplicationIntent.Explicit
		this.timestamp = properties.timestamp || BigInt(0);
	}

	private transform(properties: { offset: number, length: number }) {
		const ranges = getSegmentsFromOffsetAndRange(properties.offset, properties.length)
		this.start1 = Math.round(ranges[0][0] * SEGMENT_COORDINATE_SCALE)
		this.end1 = Math.round(ranges[0][1] * SEGMENT_COORDINATE_SCALE)
		this.start2 = Math.round(ranges[1][0] * SEGMENT_COORDINATE_SCALE)
		this.end2 = Math.round(ranges[1][1] * SEGMENT_COORDINATE_SCALE)


		this.width = this.end1 - this.start1 + (this.end2 < this.end1 ? this.end2 - this.start2 : 0)

		if (this.start1 > 0xffffffff || this.end1 > 0xffffffff || this.start2 > 0xffffffff || this.end2 > 0xffffffff || this.width > 0xffffffff) {
			throw new Error("Segment coordinate out of bounds")
		}

	}

	contains(point: number) {
		return point >= this.start1 && point < this.end1 || (point >= this.start2 && point < this.end2);
	}

	overlaps(other: ReplicationRangeIndexable, checkOther = true): boolean {
		if (this.contains(other.start1) || this.contains(other.start2) || this.contains(other.end1) || this.contains(other.end2)) {
			return true;
		}

		if (checkOther) {
			return other.overlaps(this, false)
		}
		return false;
	}
	toReplicationRange() {
		return new ReplicationRange({
			id: this.id,
			offset: this.start1 / SEGMENT_COORDINATE_SCALE,
			factor: this.width / SEGMENT_COORDINATE_SCALE,
			timestamp: this.timestamp
		})
	}


	distanceTo(point: number) {
		let wrappedPoint = (SEGMENT_COORDINATE_SCALE - point)
		return Math.min(Math.abs(this.start1 - point), Math.abs(this.end2 - point), Math.abs(this.start1 - wrappedPoint), Math.abs(this.end2 - wrappedPoint))
	}
	get wrapped() {
		return this.end2 < this.end1;

	}

	get widthNormalized() {
		return this.width / SEGMENT_COORDINATE_SCALE
	}

	equals(other: ReplicationRangeIndexable) {
		if (equals(this.id, other.id) && this.hash === other.hash && this.timestamp === other.timestamp && this.replicationIntent === other.replicationIntent && this.start1 === other.start1 && this.end1 === other.end1 && this.start2 === other.start2 && this.end2 === other.end2 && this.width === other.width) {
			return true;
		}

		return false;

	}

	toString() {
		let roundToTwoDecimals = (num: number) => Math.round(num * 100) / 100

		if (Math.abs(this.start1 - this.start2) < 0.0001) {
			return `([${roundToTwoDecimals(this.start1 / SEGMENT_COORDINATE_SCALE)}, ${roundToTwoDecimals(this.end1 / SEGMENT_COORDINATE_SCALE)}])`
		}
		return `([${roundToTwoDecimals(this.start1 / SEGMENT_COORDINATE_SCALE)}, ${roundToTwoDecimals(this.end1 / SEGMENT_COORDINATE_SCALE)}] [${roundToTwoDecimals(this.start2 / SEGMENT_COORDINATE_SCALE)}, ${roundToTwoDecimals(this.end2 / SEGMENT_COORDINATE_SCALE)}])`

	}
}


interface SharedLog {
	replicas: Partial<ReplicationLimits>;
	replicationIndex: Index<ReplicationRangeIndexable> | undefined;
}

export class MinReplicas {
	getValue(log: SharedLog): number {
		throw new Error("Not implemented");
	}
}

@variant(0)
export class AbsoluteReplicas extends MinReplicas {

	@field({ type: "u32" })
	_value: number;

	constructor(value: number) {
		super();
		this._value = value;
	}

	getValue(_log: SharedLog): number {
		return this._value;
	}
}

@variant([1, 0])
export class RequestReplicationInfoMessage extends TransportMessage {

	constructor() {
		super();
	}
}

@variant([1, 1])
export class ResponseReplicationInfoMessage extends TransportMessage {

	@field({ type: vec(ReplicationRange) })
	segments: ReplicationRange[];

	constructor(properties: { segments: ReplicationRange[] }) {
		super();
		this.segments = properties.segments;
	}
}

@variant([1, 2])
export class StartedReplicating extends TransportMessage {

	@field({ type: vec(ReplicationRange) })
	segments: ReplicationRange[];

	constructor(properties: { segments: ReplicationRange[] }) {
		super();
		this.segments = properties.segments;
	}
}

@variant([1, 3])
export class StoppedReplicating extends TransportMessage {

	@field({ type: vec(Uint8Array) })
	segmentIds: Uint8Array[];

	constructor(properties: { segmentIds: Uint8Array[] }) {
		super();
		this.segmentIds = properties.segmentIds;
	}
}


/* 
@variant(1)
export class RelativeMinReplicas extends MinReplicas {
	_value: number; // (0, 1]

	constructor(value: number) {
		super();
		this._value = value;
	}
	getValue(log: SharedLog): number {
		return Math.ceil(this._value * log.getReplicatorsSorted()!.length); // TODO TYPES
	}
}
 */

export const encodeReplicas = (minReplicas: MinReplicas): Uint8Array => {
	return serialize(minReplicas);
};

export class ReplicationError extends Error {
	constructor(message: string) {
		super(message);
	}
}
export const decodeReplicas = (entry: {
	meta: { data?: Uint8Array };
}): MinReplicas => {
	if (!entry.meta.data) {
		throw new ReplicationError("Missing meta data from error");
	}
	return deserialize(entry.meta.data, MinReplicas);
};

export const maxReplicas = (
	log: SharedLog,
	entries:
		| { meta: { data?: Uint8Array } }[]
		| IterableIterator<{ meta: { data?: Uint8Array } }>
) => {
	let max = 0;
	for (const entry of entries) {
		max = Math.max(decodeReplicas(entry).getValue(log), max);
	}
	const lower = log.replicas.min?.getValue(log) || 1;
	const higher = log.replicas.max?.getValue(log) ?? Number.MAX_SAFE_INTEGER;
	const numberOfLeaders = Math.max(Math.min(higher, max), lower);
	return numberOfLeaders;
};

export const hashToUniformNumber = (hash: Uint8Array) => {
	const seedNumber = new BinaryReader(
		hash.subarray(hash.length - 4, hash.length)
	).u32();
	return seedNumber / 0xffffffff; // bounded between 0 and 1
};
