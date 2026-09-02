import {
	type BlockStoreSafety,
	normalizeBlockStoreSafety,
} from "@peerbit/blocks";
import type { StoreFactory } from "./peer.js";

// Keep preset provenance private and attached to the exact factory object.
// This prevents a composed preset from looking like an explicit caller claim
// when Peerbit is actually reusing an external/custom blocks service.
const factorySafety = new WeakMap<StoreFactory, BlockStoreSafety>();

export const bindBlockStoreFactorySafety = (
	factory: StoreFactory,
	safety: BlockStoreSafety,
): StoreFactory => {
	factorySafety.set(factory, normalizeBlockStoreSafety(safety));
	return factory;
};

export const getBlockStoreFactorySafety = (
	factory: StoreFactory,
): BlockStoreSafety | undefined => factorySafety.get(factory);
