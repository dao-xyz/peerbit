export type MaybePromise<T> = Promise<T> | T;

export interface AnyStore {
	status(): MaybePromise<"opening" | "open" | "closing" | "closed">;
	close(): MaybePromise<void>;
	open(): MaybePromise<void>;
	get(key: string): MaybePromise<Uint8Array | undefined>;
	put(key: string, value: Uint8Array);
	del(key): MaybePromise<void>;
	sublevel(name: string): MaybePromise<AnyStore>;
	iterator: () => {
		[Symbol.asyncIterator]: () => AsyncIterator<
			[string, Uint8Array],
			void,
			void
		>;
	};
	clear(): MaybePromise<void>;
	size(): MaybePromise<number>;
}
