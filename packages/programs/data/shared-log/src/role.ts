/* import { field, variant, vec } from "@dao-xyz/borsh"; */

/* export const overlaps = (x1: number, x2: number, y1: number, y2: number) => {
	if (x1 <= y2 && y1 <= x2) {
		return true;
	}
	return false;
}; */

/* export abstract class Role {
	abstract equals(other: Role): boolean; 
}
 
export const NO_TYPE_VARIANT = new Uint8Array([0]);

@variant(0)
export class NoType extends Role {
	equals(other: Role) {
		return other instanceof NoType;
	}
}

export const OBSERVER_TYPE_VARIANT = new Uint8Array([1]);

@variant(1)
export class Observer extends Role {
	equals(other: Role) {
		return other instanceof Observer;
	}
}

export const REPLICATOR_TYPE_VARIANT = new Uint8Array([2]);*/

export const SEGMENT_COORDINATE_SCALE = 4294967295;
/* export class ReplicationSegment {

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	start: number;

	@field({ type: 'u32' })
	end: number;



	constructor(properties: {
		start: number;
		end: number;
		timestamp: bigint;
	}) {
		const { start, end, timestamp } = properties;

		if (start > end) {
			throw new Error("Range 'start' needs to be lower or equal to 'end'")
		}
		this.start = Math.round(start * SEGMENT_COORDINATE_SCALE);
		this.end = Math.round(end * SEGMENT_COORDINATE_SCALE);
		this.timestamp = timestamp
	}



}
 */

/* abstract class Capacity { }

@variant(2)
export class Replicator extends Role {

	@field({ type: vec(Capacity) })
	capacity: Capacity[];

	constructor(properties?: { capacity: Capacity[] }) {
		super();
		this.capacity = properties?.capacity || [];
	} */

/* constructor(properties: {
	timestamp?: bigint;
	factor: number;
	offset: number;
}) {
	super();
	let timestamp = properties.timestamp || BigInt(+new Date);
	let ranges = getSegmentsFromOffsetAndRange(properties.offset, properties.factor);
	this.segments = ranges.map(x => new ReplicationSegment({ start: x[0], end: x[1], timestamp }));
}
*/
/* get factor(): number {
	return this.segments[0]!.factor;
}

get offset(): number {
	return this.segments[0]!.offset;
}

get timestamp(): bigint {
	return this.segments[0]!.timestamp;
} */

/* equals(other: Role) {
	return (
		other instanceof Replicator &&
		other.factor === this.factor &&
		other.offset === this.offset
	);
} */
/* } */
