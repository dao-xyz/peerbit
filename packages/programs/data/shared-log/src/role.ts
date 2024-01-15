import { field, option, variant, vec } from "@dao-xyz/borsh";

export abstract class Role {
	abstract equals(other: Role);
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

export const REPLICATOR_TYPE_VARIANT = new Uint8Array([2]);

class ReplicationSegment {
	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u32" })
	private factorNominator: number;

	@field({ type: "u32" })
	private offsetNominator: number;

	constructor(properties: {
		factor: number;
		offset: number;
		timestamp?: bigint;
	}) {
		const { factor, timestamp, offset } = properties;
		if (factor > 1 || factor < 0) {
			throw new Error("Expecting factor to be between 0 and 1, got: " + factor);
		}

		this.timestamp = timestamp ?? BigInt(+new Date());
		this.factorNominator = Math.round(4294967295 * factor);

		if (offset > 1 || offset < 0) {
			throw new Error("Expecting offset to be between 0 and 1, got: " + offset);
		}
		this.offsetNominator = Math.round(4294967295 * offset);
	}

	get factor(): number {
		return this.factorNominator / 4294967295;
	}

	get offset(): number {
		return this.offsetNominator / 4294967295;
	}
}

@variant(2)
export class Replicator extends Role {
	@field({ type: vec(ReplicationSegment) })
	segments: ReplicationSegment[];

	constructor(properties: {
		factor: number;
		timestamp?: bigint;
		offset: number;
	}) {
		super();
		const segment: ReplicationSegment = new ReplicationSegment(properties);
		this.segments = [segment];
	}

	get factor(): number {
		return this.segments[0]!.factor;
	}

	get offset(): number {
		return this.segments[0]!.offset;
	}

	get timestamp(): bigint {
		return this.segments[0]!.timestamp;
	}

	equals(other: Role) {
		return (
			other instanceof Replicator &&
			other.factor === this.factor &&
			other.offset === this.offset
		);
	}
}
