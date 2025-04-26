/* ---------------------------------------------------------------- imports */
import type {
	CountOptions,
	DeleteOptions,
	Index,
	IndexEngineInitProperties,
	IndexIterator,
	IterateOptions,
	Shape,
	SumOptions,
} from "@peerbit/indexer-interface";
import { IteratorCache, type QueryCacheOptions } from "./cache.js";

export { type QueryCacheOptions };
export class CachedIndex<T extends Record<string, any>, Nested = unknown>
	implements Index<T, Nested>
{
	private cache: IteratorCache<T>;

	constructor(
		/** the real index implementation */
		private readonly origin: Index<T, Nested>,
		opts: QueryCacheOptions = {
			strategy: "auto",
			maxSize: 50,
			maxTotalSize: 150,
			prefetchThreshold: 2,
		}, // default options,
	) {
		this.cache = new IteratorCache<T>(opts, (iterate, maxSize) =>
			this.origin.iterate(iterate, {
				reference: true,
			}),
		);
	}

	/* -------------------------- normal Index life-cycle -------------------- */

	init(props: IndexEngineInitProperties<T, Nested>) {
		return this.origin.init(props);
	}
	start() {
		return this.origin.start?.();
	}
	stop() {
		this.cache?.clear();
		return this.origin.stop?.();
	}
	drop() {
		this.cache?.clear();
		return this.origin.drop();
	}

	/* --------------------- read operations (may use cache) ------------------ */

	get(id: any, o?: { shape: Shape }) {
		return this.origin.get(id, o);
	}
	sum(opts: SumOptions) {
		return this.origin.sum(opts);
	}
	count(opts?: CountOptions) {
		return this.origin.count(opts);
	}
	getSize() {
		return this.origin.getSize();
	}

	async put(value: T, id?: any) {
		await this.origin.put(value, id);
		await this.cache.refresh();
	}

	async del(q: DeleteOptions) {
		const res = await this.origin.del(q);
		await this.cache.refresh();
		return res;
	}

	iterate<S extends Shape | undefined = undefined>(
		iter?: IterateOptions,
		options?: { shape?: S; reference?: boolean },
	): IndexIterator<T, S> {
		if (!this.cache || options?.reference === false)
			return this.origin.iterate(iter, options);
		return this.cache.acquire(iter) as IndexIterator<T, S>;
	}

	get iteratorCache() {
		return this.cache;
	}
}
