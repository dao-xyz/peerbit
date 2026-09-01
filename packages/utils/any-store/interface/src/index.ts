export type MaybePromise<T> = Promise<T> | T;
export type CrashSafeDurability = {
	readonly crashSafe: true;
	barrier(): MaybePromise<void>;
	/**
	 * Atomically replaces one value and durably fences that replacement before
	 * resolving. If the call is interrupted or rejects, reopening must observe
	 * either the complete previous value or the complete replacement value,
	 * never a missing or torn intermediate value.
	 *
	 * Implementations must reject non-Uint8Array or detached input and capture
	 * the exact bytes before returning control to the caller.
	 *
	 * This capability is optional because `persisted()` alone does not prove
	 * crash atomicity. Callers that require it must refine the store with
	 * `CrashSafeAtomicReplaceStore` before writing.
	 */
	atomicReplace?(key: string, value: Uint8Array): MaybePromise<void>;
};

export type CrashSafeAtomicReplaceDurability = CrashSafeDurability & {
	atomicReplace(key: string, value: Uint8Array): MaybePromise<void>;
};

export interface AnyStore {
	status(): MaybePromise<"opening" | "open" | "closing" | "closed">;
	close(): MaybePromise<void>;
	open(): MaybePromise<void>;
	get(key: string): MaybePromise<Uint8Array | undefined>;
	put(key: string, value: Uint8Array): MaybePromise<void>;
	del(key: string): MaybePromise<void>;
	sublevel(name: string): MaybePromise<AnyStore>;
	iterator: () => {
		[Symbol.asyncIterator]: () => AsyncIterator<
			[string, Uint8Array],
			void,
			void
		>;
	};
	clear(): MaybePromise<void>;
	/**
	 * Returns the bytes this level's backend accounts to successful writes.
	 *
	 * The accounting is based on values when put() succeeds, not on later
	 * mutations to caller-owned Uint8Array instances. Replacements, deletions,
	 * and clears update the accounted size when those operations succeed.
	 * Persistent backends may return an approximation and may include storage
	 * overhead, so this is not necessarily the sum of current value byteLengths.
	 */
	size(): MaybePromise<number>;
	persisted(): MaybePromise<boolean>;
	/**
	 * Optional, per-instance proof that prior awaited mutations can be fenced by
	 * a crash-safe physical barrier. Absence deliberately fails closed.
	 */
	readonly crashSafeDurability?: CrashSafeDurability;
}

/** An AnyStore that explicitly proves crash-safe atomic value replacement. */
export type CrashSafeAtomicReplaceStore = AnyStore & {
	readonly crashSafeDurability: CrashSafeAtomicReplaceDurability;
};
