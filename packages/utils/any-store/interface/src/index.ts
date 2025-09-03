export type MaybePromise<T> = Promise<T> | T;

export abstract class AnyStore {
	abstract status(): MaybePromise<"opening" | "open" | "closing" | "closed">;
	abstract close(): MaybePromise<void>;
	abstract open(): MaybePromise<void>;
	abstract get(key: string): MaybePromise<Uint8Array | undefined>;
	abstract put(key: string, value: Uint8Array): MaybePromise<void>;
	abstract del(key: string): MaybePromise<void>;
	abstract sublevel(name: string): MaybePromise<AnyStore>;
	iterator: () => {
		[Symbol.asyncIterator]: () => AsyncIterator<
			[string, Uint8Array],
			void,
			void
		>;
	};
	abstract clear(): MaybePromise<void>;
	abstract size(): MaybePromise<number>;
	abstract persisted(): MaybePromise<boolean>;
}
