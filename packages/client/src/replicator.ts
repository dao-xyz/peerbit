import { variant } from "@dao-xyz/borsh";

export abstract class SubscriptionType {}

export const OBSERVER_TYPE_VARIANT = new Uint8Array([0]);
@variant(0)
export class ObserverType extends SubscriptionType {}

export const REPLICATOR_TYPE_VARIANT = new Uint8Array([1]);
@variant(1)
export class ReplicatorType extends SubscriptionType {
	multiplier: number; // 1 means I do the same amount of work as anyone else, 2 means double

	constructor(multiplier = 1) {
		super();
		this.multiplier = multiplier;
	}
}
