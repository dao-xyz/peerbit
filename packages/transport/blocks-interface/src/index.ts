import { type WaitForPeer } from "@peerbit/stream-interface";
import { type Block } from "multiformats/block";

export type GetOptions = {
	remote?:
		| {
				signal?: AbortSignal;
				timeout?: number;
				replicate?: boolean;
				from?: string[];
				priority?: number;
		  }
		| boolean;
};
export type PutOptions = {
	timeout?: number;
};

type MaybePromise<T> = Promise<T> | T;
export type CrashSafeBlockDurability = {
	readonly crashSafe: true;
	barrier(): MaybePromise<void>;
};

/**
 * Scope across which local references to a content-addressed block may alias.
 *
 * `block-service` is the scope of Peerbit's built-in stores: the backing
 * namespace belongs to one blocks service, but every program using that
 * service shares it. `caller-exclusive` is an explicit assertion that the
 * declaring caller controls the entire physical namespace and every reference
 * in it. `shared` means references may exist outside this block service;
 * `unknown` makes no reliable scope assertion. Neither persistence nor a
 * backend class implies any scope.
 */
export type BlockStoreReferenceDomain =
	| "block-service"
	| "caller-exclusive"
	| "shared"
	| "unknown";

/**
 * Immutable facts needed before a caller considers physical block deletion.
 *
 * `enforcedReclamation: "none"` means the store exposes no reference-safe
 * lease, fencing, atomic delete-if-unreferenced, or isolated-namespace
 * primitive. A raw `rm` is therefore safe only for an authority that controls
 * the complete reference domain. This field is deliberately separate from
 * `persisted()`.
 */
export type BlockStoreSafety = Readonly<{
	referenceDomain: BlockStoreReferenceDomain;
	enforcedReclamation: "none";
}>;

export const UNKNOWN_BLOCK_STORE_SAFETY: BlockStoreSafety = Object.freeze({
	referenceDomain: "unknown",
	enforcedReclamation: "none",
});

export const BLOCK_SERVICE_BLOCK_STORE_SAFETY: BlockStoreSafety = Object.freeze(
	{
		referenceDomain: "block-service",
		enforcedReclamation: "none",
	},
);

/**
 * Validate, defensively copy, and freeze block-store safety metadata.
 *
 * The input is intentionally `unknown`: declarations can cross JavaScript and
 * configuration boundaries where TypeScript's static shape is unavailable.
 */
export const normalizeBlockStoreSafety = (
	value: unknown,
): BlockStoreSafety => {
	if (value === undefined) return UNKNOWN_BLOCK_STORE_SAFETY;
	if (value === null || typeof value !== "object") {
		throw new TypeError("Invalid block-store safety metadata");
	}
	const referenceDomain = (value as { referenceDomain?: unknown })
		.referenceDomain;
	const enforcedReclamation = (value as { enforcedReclamation?: unknown })
		.enforcedReclamation;
	if (
		referenceDomain !== "block-service" &&
		referenceDomain !== "caller-exclusive" &&
		referenceDomain !== "shared" &&
		referenceDomain !== "unknown"
	) {
		throw new TypeError("Invalid block-store reference domain");
	}
	if (enforcedReclamation !== "none") {
		throw new TypeError("Unsupported block-store reclamation capability");
	}
	return Object.freeze({ referenceDomain, enforcedReclamation });
};

export interface Blocks extends WaitForPeer {
	/**
	 * Safety metadata for this service's local physical block namespace.
	 * Absence is equivalent to {@link UNKNOWN_BLOCK_STORE_SAFETY} so older and
	 * custom implementations fail closed.
	 */
	readonly localStoreSafety?: BlockStoreSafety;
	put(
		data: Uint8Array | { block: Block<any, any, any, any>; cid: string },
	): MaybePromise<string>;
	putMany?(
		data: Array<Uint8Array | { block: Block<any, any, any, any>; cid: string }>,
	): MaybePromise<string[]>;
	/**
	 * Store raw block bytes when the caller has already computed and verified
	 * the matching CID. Implementations must not recalculate the CID on this path.
	 */
	putKnown?(cid: string, bytes: Uint8Array): MaybePromise<string>;
	putKnownMany?(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): MaybePromise<string[]>;
	has(cid: string): MaybePromise<boolean>;
	/**
	 * Return local-storage presence flags aligned with the input CIDs.
	 *
	 * This must not perform remote reads; callers use it to replace repeated
	 * `has(...)` probes without changing block-fetch semantics.
	 */
	hasMany?(cids: string[]): MaybePromise<boolean[]>;
	get(cid: string, options?: GetOptions): MaybePromise<Uint8Array | undefined>;
	getMany?(
		cids: string[],
		options?: GetOptions,
	): MaybePromise<Array<Uint8Array | undefined>>;
	/**
	 * Best-effort provider hints for `get(..., { remote: true })` without explicit `remote.from`.
	 *
	 * Implementations should treat hints as advisory and keep them bounded (LRU/TTL).
	 */
	hintProviders?(cid: string, providers: string[]): void;
	rm(cid: string): MaybePromise<void>;
	iterator(): AsyncGenerator<[string, Uint8Array], void, void>;
	size(): MaybePromise<number>;
	persisted(): MaybePromise<boolean>;
	readonly crashSafeDurability?: CrashSafeBlockDurability;
}

export {
	cidifyString,
	stringifyCid,
	createBlock,
	getBlockValue,
	calculateRawCid,
	checkDecodeBlock,
	verifyBlockBytes,
	codecCodes,
	defaultHasher,
	codecMap,
} from "./block.js";
export type { VerifyBlockBytesOptions } from "./block.js";
