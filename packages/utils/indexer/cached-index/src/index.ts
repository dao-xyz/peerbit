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
	private _cache: IteratorCache<T>;

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
		this._cache = new IteratorCache<T>(opts, (iterate, maxSize) =>
			this.origin.iterate(iterate, {
				reference: true,
			}),
		);
		this.attachExactDeleteHelpers();
	}

	private attachExactDeleteHelpers() {
		const origin = this.origin as {
			delIds?: (deleteIds: any[]) => any;
			delIdsNoReturn?: (deleteIds: any[]) => any;
			delIdsCount?: (deleteIds: any[]) => any;
		};
		const self = this as unknown as {
			delIds?: (deleteIds: any[]) => Promise<any>;
			delIdsNoReturn?: (deleteIds: any[]) => Promise<void>;
			delIdsCount?: (deleteIds: any[]) => Promise<number>;
		};
		if (origin.delIds) {
			self.delIds = async (deleteIds) => {
				const res = await origin.delIds!.call(this.origin, deleteIds);
				await this._cache.refresh();
				return res;
			};
		}
		if (origin.delIdsNoReturn) {
			self.delIdsNoReturn = async (deleteIds) => {
				await origin.delIdsNoReturn!.call(this.origin, deleteIds);
				await this._cache.refresh();
			};
		}
		if (origin.delIdsCount) {
			self.delIdsCount = async (deleteIds) => {
				const res = await origin.delIdsCount!.call(this.origin, deleteIds);
				await this._cache.refresh();
				return res;
			};
		}
	}

	/* -------------------------- normal Index life-cycle -------------------- */

	init(props: IndexEngineInitProperties<T, Nested>) {
		return this.origin.init(props);
	}
	start() {
		return this.origin.start?.();
	}
	async stop() {
		await this._cache?.clear();
		return this.origin.stop?.();
	}
	async drop() {
		await this._cache?.clear();
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

	persisted() {
		return this.origin.persisted();
	}

	async put(value: T, id?: any, options?: { replace?: boolean }) {
		await this.origin.put(value, id, options);
		await this._cache.refresh();
	}

	async putBatch(values: T[]) {
		if (this.origin.putBatch) {
			await this.origin.putBatch(values);
		} else {
			for (const value of values) {
				await this.origin.put(value);
			}
		}
		await this._cache.refresh();
	}

	async del(q: DeleteOptions) {
		const res = await this.origin.del(q);
		await this._cache.refresh();
		return res;
	}

	iterate<S extends Shape | undefined = undefined>(
		iter?: IterateOptions,
		options?: { shape?: S; reference?: boolean },
	): IndexIterator<T, S> {
		if (!this._cache || options?.reference === false)
			return this.origin.iterate(iter, options);
		return this._cache.acquire(iter) as IndexIterator<T, S>;
	}

	get iteratorCache() {
		return this._cache;
	}
}
