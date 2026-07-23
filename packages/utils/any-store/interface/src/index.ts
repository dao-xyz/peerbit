export type MaybePromise<T> = Promise<T> | T;

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
}
