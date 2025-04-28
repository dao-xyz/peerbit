/* ---------------------------------------------------------------- imports */
import { field, serialize, vec } from "@dao-xyz/borsh";
import { toBase64 } from "@peerbit/crypto";
import {
	type IndexIterator,
	type IndexedResults,
	type IterateOptions,
	Query,
	Sort,
} from "@peerbit/indexer-interface";
import { toQuery, toSort } from "@peerbit/indexer-interface";

/* -------------------------------------------------------- key helpers */
class KeyableIterate {
	@field({ type: vec(Query) }) query: Query[];
	@field({ type: vec(Sort) }) sort: Sort[];
	constructor(query: Query[], sort: Sort[]) {
		this.query = query;
		this.sort = sort;
	}
}
const iterateKey = (o?: IterateOptions) =>
	toBase64(serialize(new KeyableIterate(toQuery(o?.query), toSort(o?.sort))));

/* ------------------------------------------------------ cache options */
export type QueryCacheOptions =
	| {
			strategy: "auto";
			maxTotalSize: number;
			maxSize: number;
			prefetchThreshold?: number;
			keepAlive?: number;
	  }
	| {
			strategy: "manual";
			queries: { iterate: IterateOptions; maxSize: number }[];
	  };

type QueryCacheConfig = {
	shouldCache: (k: string) => boolean;
	entryMax: number;
	totalMax: number;
	prefetchThreshold: number;
	keepAlive?: number; // ms before a COLD entry is pruned
};

/* ----------------------------------------------------- buffer wrapper */
function wrapWithBuffer<T extends Record<string, any>>(
	src: IndexIterator<T, undefined>,
	warm: IndexedResults<T>,
): IndexIterator<T, undefined> {
	const buf = [...warm];
	return {
		async next(n) {
			const out = buf.splice(0, n);
			if (out.length < n) out.push(...(await src.next(n - out.length)));
			return out;
		},
		async all() {
			const rest = await src.all();
			return [...buf.splice(0, buf.length), ...rest];
		},
		done: () => (buf.length ? false : (src.done?.() ?? false)),
		pending: async () => (buf.length ? buf.length : (src.pending?.() ?? 0)),
		close: () => src.close(),
	};
}

/* -------------------------------------------------------- data shapes */
type CachedIter<I extends Record<string, any>> = {
	it: IndexIterator<I, undefined>;
	size: number;
	hits: number;
	ts: number;
};
type Entry<I extends Record<string, any>> = {
	cached: CachedIter<I>;
	opts: IterateOptions;
};

/* ----------------------------------------------------------- main class */
export class IteratorCache<I extends Record<string, any>> {
	private readonly map = new Map<string, Entry<I>>();
	private readonly pendingJob = new Map<string, Promise<void>>();
	private total = 0;

	private readonly cfg: QueryCacheConfig;

	constructor(
		opts: QueryCacheOptions,
		private readonly factory: (
			o: IterateOptions,
			max: number,
		) => IndexIterator<I, undefined>,
	) {
		if (opts.strategy === "manual") {
			const wl = new Map(
				opts.queries.map((q) => [iterateKey(q.iterate), q.maxSize]),
			);
			this.cfg = {
				shouldCache: (k) => wl.has(k),
				entryMax: Math.max(...opts.queries.map((q) => q.maxSize)),
				totalMax: Number.MAX_SAFE_INTEGER,
				prefetchThreshold: 1,
				keepAlive: Number.MAX_SAFE_INTEGER,
			};
			opts.queries.forEach((q) => this._ensureEntry(q.iterate));
		} else {
			this.cfg = {
				shouldCache: () => true,
				entryMax: opts.maxSize,
				totalMax: opts.maxTotalSize,
				prefetchThreshold: opts.prefetchThreshold ?? 1,
				keepAlive: opts.keepAlive ?? 1e4,
			};
		}
	}

	/* --------------------------------------------------------- public API */

	acquire(opts: IterateOptions = {}): IndexIterator<I, undefined> {
		if (this.cfg.keepAlive) this.pruneStale();

		const key = iterateKey(opts);
		const entry = this._ensureEntry(opts);

		/* ------------------------- warm path ------------------------- */
		if (entry.cached.size > 0) {
			const warmIt = entry.cached.it;

			/* replace keeper & restart warm-up */
			entry.cached = {
				it: this.factory(opts, this.cfg.entryMax),
				size: 0,
				hits: entry.cached.hits + 1,
				ts: Date.now(),
			};
			return warmIt; // caller gets prefetched rows
		}

		/* ------------------ cold / warming path ---------------------- */
		entry.cached.hits++;
		entry.cached.ts = Date.now();
		if (
			entry.cached.hits >= this.cfg.prefetchThreshold &&
			!this.pendingJob.has(key)
		) {
			this._startWarmup(key, opts);
		}
		return this.factory(opts, this.cfg.entryMax); // caller gets cold iterator
	}

	async refresh() {
		const keep = [...this.map.values()].map((e) => e.opts);
		await this.clear();
		for (const o of keep) this.acquire(o);
	}

	async clear() {
		await Promise.all([...this.map.values()].map((e) => e.cached.it.close()));
		this.map.clear();
		this.pendingJob.clear();
		this.total = 0;
	}

	/* ------------------------------------------------------- internals */

	private _ensureEntry(opts: IterateOptions): Entry<I> {
		const key = iterateKey(opts);
		let e = this.map.get(key);
		if (e) return e;
		e = {
			cached: {
				it: this.factory(opts, this.cfg.entryMax),
				size: 0,
				hits: 0,
				ts: Date.now(),
			},
			opts,
		};
		this.map.set(key, e);
		return e;
	}

	private _startWarmup(key: string, opt: IterateOptions) {
		if (this.pendingJob.has(key)) return;
		const job = (async () => {
			try {
				const e = this.map.get(key);
				if (!e || e.cached.size > 0) return;
				const warm = await e.cached.it.next(this.cfg.entryMax);
				e.cached.it = wrapWithBuffer(e.cached.it, warm);
				e.cached.size = warm.length;
				this.total += warm.length;
				await this._evictIfNeeded();
			} finally {
				this.pendingJob.delete(key);
			}
		})();
		this.pendingJob.set(key, job);
	}

	private async _evictIfNeeded() {
		if (this.total <= this.cfg.totalMax) return;
		const victims = [...this.map.entries()].sort(([, a], [, b]) =>
			a.cached.hits === b.cached.hits
				? a.cached.ts - b.cached.ts
				: a.cached.hits - b.cached.hits,
		);
		for (const [k, v] of victims) {
			if (this.pendingJob.has(k)) continue;
			await v.cached.it.close();
			this.total -= v.cached.size;
			this.map.delete(k);
			if (this.total <= this.cfg.totalMax) break;
		}
	}

	/** prune cold entries that exceweded keepAlive */
	pruneStale() {
		const now = Date.now();
		for (const [k, e] of this.map) {
			// if cached size > 0 then it is "active" and should not be pruned
			// cached.size === 0 means that the entry is "cold" and can be pruned
			if (e.cached.size === 0 && now - e.cached.ts > this.cfg.keepAlive!) {
				this.map.delete(k);
			}
		}
	}

	/* ------------------------------------------------ diagnostics helper */
	get _debugStats() {
		return {
			prefetchedRows: this.total,
			cachedQueries: this.map.size,
			activeQueries: [...this.map]
				.filter(([, e]) => e.cached.size > 0)
				.map(([k]) => k),
			pending: [...this.pendingJob.keys()],
			queryIsActive: (options?: IterateOptions) => {
				const key = iterateKey(options);
				return (this.map.get(key)?.cached.size ?? 0) > 0 || false;
			},
			getCached: (key: string) => {
				return this.map.get(key);
			},
		};
	}
}
