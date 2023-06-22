import { field, variant } from "@dao-xyz/borsh";

export abstract class SubscriptionType {}

export const NO_TYPE_VARIANT = new Uint8Array([0]);

@variant(0)
export class NoType extends SubscriptionType {}

export const OBSERVER_TYPE_VARIANT = new Uint8Array([1]);
@variant(1)
export class Observer extends SubscriptionType {}

export const REPLICATOR_TYPE_VARIANT = new Uint8Array([2]);

@variant(2)
export class Replicator extends SubscriptionType {
	@field({ type: "u32" })
	multiplier: number; // 1 means I do the same amount of work as anyone else, 2 means double

	constructor() {
		// multiplier is unsupported for now, so contructor is empty
		super();
		this.multiplier = 1;
	}
}
