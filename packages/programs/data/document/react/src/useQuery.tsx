import { ClosedError, Documents, NotStartedError } from "@peerbit/document";
import type {
	AbstractSearchRequest,
	AbstractSearchResult,
	Context,
	LateResultsEvent,
	RemoteQueryOptions,
	ResultsIterator,
	UpdateReason,
	WithContext,
} from "@peerbit/document";
import type { UpdateOptions, WithIndexedContext } from "@peerbit/document";
import * as indexerTypes from "@peerbit/indexer-interface";
import { useEffect, useMemo, useRef, useState } from "react";
import { v4 as uuid } from "uuid";

/**
 * Check if an error is a benign lifecycle error that should be silently ignored.
 * These errors can occur during component unmount when the database is closing.
 */
const isBenignLifecycleError = (e: unknown): boolean => {
	return e instanceof ClosedError || e instanceof NotStartedError;
};

type QueryOptions = { query: QueryLike; id?: string };

type LoadMoreFn = (
	n?: number,
	opts?: { force?: boolean; reason?: MergeReason },
) => Promise<boolean>;

type MergeReason = UpdateReason | "batch" | "late";

type ApplyResultsHandler<Item> = (
	prev: Item[],
	change: { items: Item[]; reason: MergeReason },
	helpers: { defaultMerge: () => Item[]; loadMore: LoadMoreFn },
) => Item[] | Promise<Item[]>;

type LateResultsHandler<Item> = (
	evt: LateResultsEvent<"queue">,
	helpers: {
		loadMore: LoadMoreFn;
		inject: (
			items: Item | Item[],
			opts?: { position?: "start" | "end" | number },
		) => void;
		items: () => Item[];
	},
) => void | Promise<void>;
type LateResultsQueueItems = NonNullable<LateResultsEvent<"queue">["items"]>;
type LateResultsQueueHelpers = {
	collect?: () => Promise<LateResultsQueueItems>;
};

/* ────────────── helper types ────────────── */
export type QueryLike = {
	/** Mongo-style selector or array of selectors */
	query?: indexerTypes.QueryLike | indexerTypes.Query[];
	/** Sort definition compatible with `@peerbit/indexer-interface` */
	sort?: indexerTypes.SortLike | indexerTypes.Sort | indexerTypes.Sort[];
};

/**
 * All the non-DB-specific options supported by the original single-DB hook.
 * They stay fully backward-compatible.
 */
export type UseQuerySharedOptions<
	T,
	I,
	R extends boolean | undefined,
	RT = R extends false ? WithContext<I> : WithIndexedContext<T, I>,
> = {
	/* original behavioural flags */
	resolve?: R;
	transform?: (r: RT) => Promise<RT>;
	debounce?: number;
	debug?: boolean | string;
	reverse?: boolean;
	batchSize?: number;
	prefetch?: boolean;
	/*     onChange?: {
            merge?:
                | boolean
                | ((
                      c: DocumentsChange<T, I>
                  ) =>
                      | DocumentsChange<T, I>
                      | Promise<DocumentsChange<T, I>>
                      | undefined);
            update?: (
                prev: RT[],
                change: DocumentsChange<T, I>
            ) => RT[] | Promise<RT[]>;
        }; */
	updates?: UpdateOptions<T, I, R>;
	local?: boolean;
	remote?:
		| boolean
		| RemoteQueryOptions<AbstractSearchRequest, AbstractSearchResult, any>;
	onLateResults?: LateResultsHandler<RT>;
	applyResults?: ApplyResultsHandler<RT>;
} & QueryOptions;

/* ────────────────────────── Main Hook ────────────────────────── */
/**
 * `useQuery` – unified hook that accepts **either**
 *   1. a single `Documents` instance
 *   2. an array of `Documents` instances
 *   3. *or* omits the first argument and provides `dbs` inside the `options` object.
 *
 * It supersedes the original single-DB version as well as the experimental
 * `useMultiQuery` so callers never have to choose between two APIs.
 */
export const useQuery = <
	T extends Record<string, any>,
	I extends Record<string, any>,
	R extends boolean | undefined = true,
	RT = R extends false ? WithContext<I> : WithIndexedContext<T, I>,
>(
	/** Single DB or list of DBs. 100 % backward-compatible with the old single param. */
	dbOrDbs: Documents<T, I> | Documents<T, I>[] | undefined,
	options: UseQuerySharedOptions<T, I, R, RT>,
) => {
	/* ─────── internal type alias for convenience ─────── */
	type Item = RT;
	type IteratorRef = {
		id: string;
		db: Documents<T, I>;
		iterator: ResultsIterator<Item>;
		itemsConsumed: number;
	};

	/* ────────────── normalise DBs input ────────────── */
	const dbs = useMemo<(Documents<T, I> | undefined)[]>(() => {
		if (Array.isArray(dbOrDbs)) return dbOrDbs;
		if (dbOrDbs) return [dbOrDbs];
		return [];
	}, [dbOrDbs]);

	/* ────────────── state & refs ────────────── */
	const [all, setAll] = useState<Item[]>([]);
	const allRef = useRef<Item[]>([]);
	const [isLoading, setIsLoading] = useState(false);
	const iteratorRefs = useRef<IteratorRef[]>([]);
	const itemIdRef = useRef(new WeakMap<object, string>());
	const emptyResultsRef = useRef(false);
	const closeControllerRef = useRef<AbortController | null>(null);
	const waitedOnceRef = useRef(false);
	const loadMoreRef = useRef<LoadMoreFn | undefined>(undefined);

	/* keep an id mostly for debugging – mirrors original behaviour */
	const [id, setId] = useState<string | undefined>(options.id);

	const reverseRef = useRef(options.reverse);
	useEffect(() => {
		reverseRef.current = options.reverse;
	}, [options.reverse]);

	/* ────────────── utilities ────────────── */
	const log = (...a: any[]) => {
		if (!options.debug) return;
		if (typeof options.debug === "boolean") console.log(...a);
		else console.log(options.debug, ...a);
	};

	const updateAll = (combined: Item[]) => {
		allRef.current = combined;
		setAll(combined);
	};

	const mutateAll = (fn: (prev: Item[]) => Item[]) => {
		updateAll(fn(allRef.current));
	};

	const reset = () => {
		iteratorRefs.current?.forEach(({ iterator }) => iterator.close());
		iteratorRefs.current = [];

		closeControllerRef.current?.abort(new Error("Reset"));
		closeControllerRef.current = new AbortController();
		emptyResultsRef.current = false;
		waitedOnceRef.current = false;

		allRef.current = [];
		itemIdRef.current = new WeakMap();
		setAll([]);
		setIsLoading(false);
		log("Iterators reset");
	};

	const resolveItemKey = (item: Item): string | null => {
		const cached = itemIdRef.current.get(item as object);
		if (cached) return cached;
		for (const { db } of iteratorRefs.current) {
			const candidate =
				(item as any)?.__indexed ??
				(item as any)?.__context?.__indexed ??
				(item as WithContext<I> | WithIndexedContext<T, I>);
			const id = db.index.resolveId(candidate);
			const key = idToKey(id.primitive);
			itemIdRef.current.set(item as object, key);
			return key; // return the first result
		}
		return null;
	};

	const injectItems = (
		incoming: Item | Item[],
		opts?: { position?: "start" | "end" | number },
	) => {
		const arr = (Array.isArray(incoming) ? incoming : [incoming]).filter(
			Boolean,
		) as Item[];
		if (!arr.length) return;
		mutateAll((prev) => {
			// Fast path for front insert to reduce copies
			if (!opts?.position || opts.position === "start") {
				const existingKeys = new Set<string>();
				prev.forEach((item) => {
					const key = resolveItemKey(item);
					if (key) existingKeys.add(key);
				});
				const deduped = arr.filter((item) => {
					const key = resolveItemKey(item);
					return !(key && existingKeys.has(key));
				});
				if (deduped.length === 0) return prev;
				const out = new Array(deduped.length + prev.length);
				let i = 0;
				for (const item of deduped) out[i++] = item;
				for (const item of prev) out[i++] = item;
				deduped.forEach((item) => {
					const key = resolveItemKey(item);
					if (key) itemIdRef.current.set(item as object, key);
				});
				return out as Item[];
			}

			// Fallback: splice semantics
			const working = [...prev];
			const keyIndex = new Map<string, number>();
			working.forEach((item, idx) => {
				const key = resolveItemKey(item);
				if (key) keyIndex.set(key, idx);
			});

			for (const item of arr) {
				const key = resolveItemKey(item);
				if (key && keyIndex.has(key)) {
					const idx = keyIndex.get(key)!;
					working.splice(idx, 1);
				}
			}

			let at = 0;
			if (opts?.position === "end") {
				at = working.length;
			} else if (
				typeof opts?.position === "number" &&
				Number.isFinite(opts.position)
			) {
				at = Math.max(0, Math.min(working.length, opts.position));
			}

			working.splice(at, 0, ...arr);
			arr.forEach((item, offset) => {
				const key = resolveItemKey(item);
				if (key) {
					itemIdRef.current.set(item as object, key);
					keyIndex.set(key, at + offset);
				}
			});
			return working;
		});
	};

	/* ────────── rebuild iterators when db list / query etc. change ────────── */
	useEffect(() => {
		/* derive canonical list of open DBs */
		const openDbs = dbs.filter((d): d is Documents<T, I> =>
			Boolean(d && !d.closed),
		);
		const { query, resolve } = options;

		if (!openDbs.length || query == null) {
			reset();
			return;
		}

		reset();
		const abortSignal = closeControllerRef.current?.signal;
		const resolveRemoteOptions = () => {
			if (options.remote === false) return false;
			if (!options.remote) return undefined;
			if (typeof options.remote === "object") {
				return {
					...options.remote,
					wait: {
						...options.remote.wait,
						timeout: options.remote.wait?.timeout ?? 5000,
					},
				};
			}
			return {};
		};
		const onMissedResults = async (
			evt: LateResultsEvent<"queue">,
			lateHelpers?: LateResultsQueueHelpers,
		) => {
			log("Late results", evt);
			const loadMoreHelper: LoadMoreFn = (...args) =>
				loadMoreRef.current
					? loadMoreRef.current(...args)
					: Promise.resolve(false);
			const helpers = {
				loadMore: loadMoreHelper,
				inject: injectItems,
				items: () => allRef.current,
			};
			const maybeUser = options.onLateResults?.(evt, helpers);
			if (maybeUser) {
				await maybeUser;
				return;
			}

			// default handling: if we have concrete items, inject them at the edge
			const items =
				evt.items ?? (lateHelpers?.collect && (await lateHelpers.collect()));
			if (items && items.length) {
				const values = items
					.map((it: any) => it?.value ?? it?.indexed ?? it)
					.filter(Boolean) as Item[];
				if (values.length) {
					injectItems(values, {
						position: reverseRef.current ? "end" : "start",
					});
				}
				return;
			}
			// fallback: force a pull for late items
			await loadMoreHelper(batchSize, { force: true, reason: "late" });
		};
		let draining = false;
		const scheduleDrain = (
			ref: ResultsIterator<RT>,
			amount: number,
			opts?: { force?: boolean; reason?: MergeReason },
		) => {
			log("Schedule drain", draining, ref, amount);
			if (draining) return;
			draining = true;
			loadMore(amount, opts)
				.catch((e) => {
					if (!isBenignLifecycleError(e)) throw e;
				})
				.finally(() => {
					draining = false;
				});
		};

		iteratorRefs.current = openDbs.map((db) => {
			let currentRef: IteratorRef | undefined;
			const iterator = db.index.iterate(query ?? {}, {
				closePolicy: "manual",
				local: options.local ?? true,
				remote: resolveRemoteOptions(),
				outOfOrder: { mode: "queue", handle: onMissedResults },
				resolve,
				signal: abortSignal,
				updates: {
					push:
						typeof options.updates === "boolean"
							? options.updates
							: typeof options.updates === "object" && options.updates.push
								? true
								: false,
					merge:
						typeof options.updates === "boolean" && options.updates
							? true
							: typeof options.updates === "object" && options.updates.merge
								? true
								: false,
					notify: (reason) => {
						log("notify", { reason, currentRef: !!currentRef });
						if (reason === "change" || reason === "push" || reason === "join") {
							const drainAmount = options.batchSize ?? 10;
							scheduleDrain(iterator as ResultsIterator<RT>, drainAmount, {
								force: true,
								reason,
							});
						}
					},
					onBatch: (
						batch: (WithContext<I> | WithIndexedContext<T, I>)[],
						props,
					) => {
						log("onBatch", { batch, props, currentRef: !!currentRef });
						if (
							props.reason === "join" ||
							props.reason === "change" ||
							props.reason === "push"
						) {
							if (!currentRef) return;
							handleBatch(
								iteratorRefs.current,
								[{ ref: currentRef, items: batch as Item[] }],
								props.reason,
							);
						}
					},
				},
			}) as ResultsIterator<Item>;

			const ref: IteratorRef = {
				id: uuid(),
				db,
				iterator,
				itemsConsumed: 0,
			};
			currentRef = ref;
			log("Iterator init", ref.id, "db", db.address);
			return ref;
		});

		/* store a deterministic id (useful for external keys) */
		setId(uuid());

		/* prefetch if requested */
		if (options.prefetch) void loadMore();
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [
		dbs.map((d) => d?.address).join("|"),
		options.query,
		options.resolve,
		options.reverse,
		options.batchSize,
		options.local,
		options.remote,
	]);

	/* ────────────── loadMore implementation ────────────── */
	const batchSize = options.batchSize ?? 10;

	const shouldWait = (): boolean => {
		if (waitedOnceRef.current) return false;
		if (options.remote === false) return false;
		return true; // mimic original behaviour – wait once if remote allowed
	};

	const markWaited = () => {
		waitedOnceRef.current = true;
	};

	/* helper to turn primitive ids into stable map keys */
	const idToKey = (value: indexerTypes.IdPrimitive): string => {
		switch (typeof value) {
			case "string":
				return `s:${value}`;
			case "number":
				return `n:${value}`;
			default:
				return `b:${value.toString()}`;
		}
	};

	const handleBatch = async (
		iterators: IteratorRef[],
		batches: { ref: IteratorRef; items: Item[] }[],
		reason: MergeReason = "batch",
	): Promise<boolean> => {
		if (!iterators.length) {
			log("No iterators in handleBatch");
			return false;
		}

		const totalFetched = batches.reduce(
			(sum, batch) => sum + batch.items.length,
			0,
		);
		if (totalFetched === 0) {
			log("No items fetched");
			emptyResultsRef.current = iterators.every((i) => i.iterator.done());
			return !emptyResultsRef.current;
		}

		let processed = batches;
		if (options.transform) {
			const transform = options.transform;
			processed = await Promise.all(
				batches.map(async ({ ref, items }) => ({
					ref,
					items: await Promise.all(items.map(transform)),
				})),
			);
		}

		const prev = allRef.current;
		const next = [...prev];
		const resolveIndexedForSort = (item: Item) =>
			(item as any).__indexed ??
			((item as any).__context && (item as any).__context.__indexed) ??
			item;
		const sortSpec = (options.query as any)?.sort;
		const normalizeSort = (sort: any) => {
			if (!sort) return undefined;
			const arr = Array.isArray(sort) ? sort : [sort];
			return arr.map((s) => ({
				...s,
				key: Array.isArray(s.key) ? s.key : [s.key],
			}));
		};
		const normalizedSort = normalizeSort(sortSpec);
		const sortComparator: ((a: Item, b: Item) => number) | undefined =
			normalizedSort &&
			((a: Item, b: Item) => {
				const aIndexed = resolveIndexedForSort(a);
				const bIndexed = resolveIndexedForSort(b);
				if (aIndexed == null || bIndexed == null) {
					return 0;
				}
				return indexerTypes.extractSortCompare(
					aIndexed as any,
					bIndexed as any,
					normalizedSort as any,
				);
			});
		const keyIndex = new Map<string, number>();
		prev.forEach((item, idx) => {
			const key = itemIdRef.current.get(item as object);
			if (key) keyIndex.set(key, idx);
		});

		const seenHeads = new Set(prev.map((x) => (x as any).__context?.head));
		const freshItems: Item[] = [];
		let hasMutations = false;

		log("Processing batches", { processed, keyIndex });
		for (const { ref, items } of processed) {
			const db = ref.db;
			for (const item of items) {
				const ctx = (item as WithContext<any>).__context;
				const head = ctx?.head;

				let key: string | null = null;
				try {
					key = idToKey(
						db.index.resolveId(
							item as WithContext<I> | WithIndexedContext<T, I>,
						).primitive,
					);
				} catch (error) {
					log("useQuery: failed to resolve id", error);
				}

				if (key && keyIndex.has(key)) {
					const existingIndex = keyIndex.get(key)!;
					const current = next[existingIndex];
					const currentContext: Context | undefined = (
						current as WithContext<any>
					)?.__context;
					const incomingContext: Context | undefined = ctx;
					const shouldReplace =
						!currentContext ||
						!incomingContext ||
						currentContext.modified <= incomingContext.modified;

					if (shouldReplace && current !== item) {
						itemIdRef.current.delete(current as object);
						next[existingIndex] = item;
						hasMutations = true;
					}

					if (key) {
						itemIdRef.current.set(item as object, key);
						keyIndex.set(key, existingIndex);
					}
					if (head != null) seenHeads.add(head);
					continue;
				}

				if (head != null && seenHeads.has(head)) continue;
				if (head != null) seenHeads.add(head);

				freshItems.push(item);
				if (key) {
					itemIdRef.current.set(item as object, key);
					keyIndex.set(key, prev.length + freshItems.length - 1);
				}
			}
		}

		if (!freshItems.length && !hasMutations) {
			emptyResultsRef.current = iterators.every((i) => i.iterator.done());
			log("No new items or mutations");
			return !emptyResultsRef.current;
		}

		const incomingItems = freshItems.length
			? [...freshItems]
			: processed.flatMap((p) => p.items);

		let combinedDefault: Item[];
		if (sortComparator) {
			let combined = [...next, ...freshItems];
			try {
				combined.sort(sortComparator);
			} catch (error) {
				console.warn("Failed to sort results", error);
			}
			if (reverseRef.current) {
				combined.reverse();
			}
			combinedDefault = combined;
		} else if (reason === "late") {
			let combined = [...freshItems, ...next];
			if (reverseRef.current) {
				combined.reverse();
			}
			combinedDefault = combined;
		} else if (reverseRef.current) {
			const combined = [...freshItems].reverse();
			combined.push(...next);
			combinedDefault = combined;
		} else {
			next.push(...freshItems);
			combinedDefault = next;
		}

		const defaultMerge = () => combinedDefault;
		const loadMoreHelper: LoadMoreFn = (...args) =>
			loadMoreRef.current
				? loadMoreRef.current(...args)
				: Promise.resolve(false);
		const mergedCandidate = options.applyResults
			? await options.applyResults(
					allRef.current,
					{ items: incomingItems, reason },
					{
						defaultMerge,
						loadMore: loadMoreHelper,
					},
				)
			: undefined;
		const merged = mergedCandidate ?? combinedDefault;

		log("Updating all with", {
			prevLength: prev.length,
			freshLength: freshItems.length,
			combinedLength: merged.length,
		});
		updateAll(merged);

		emptyResultsRef.current = iterators.every((i) => i.iterator.done());
		return !emptyResultsRef.current;
	};

	const drainRoundRobin = async (
		iterators: IteratorRef[],
		n: number,
		reason: MergeReason = "batch",
	): Promise<boolean> => {
		const batches: { ref: IteratorRef; items: Item[] }[] = [];
		for (const ref of iterators) {
			if (ref.iterator.done()) continue;
			const batch = await ref.iterator.next(n);
			log("Iterator", ref.id, "fetched", batch.length, "items");
			if (batch.length) {
				ref.itemsConsumed += batch.length;
				batches.push({ ref, items: batch });
			}
		}
		return handleBatch(iterators, batches, reason);
	};

	const loadMore = async (
		n: number = batchSize,
		opts?: { force?: boolean; reason?: MergeReason },
	): Promise<boolean> => {
		const iterators = iteratorRefs.current;
		if (!iterators.length) {
			log("No iterators or already empty", {
				length: iterators.length,
				emptyResultsRef: emptyResultsRef.current,
			});
			return false;
		}
		if (emptyResultsRef.current && !opts?.force) {
			log("Skipping loadMore due to empty state");
			return false;
		} else if (opts?.force) {
			emptyResultsRef.current = false;
		}

		setIsLoading(true);
		try {
			/* one-time replicator warm-up across all DBs */
			if (shouldWait()) {
				/*   if (
                     typeof options.remote === "object" &&
                     options.remote.wait
                 ) {
                     await Promise.all(
                         iterators.map(async ({ db }) => {
                             try {  
                                 await db.log.waitForReplicators({
                                     timeout: (options.remote as { warmup })
                                         .warmup,
                                     signal: closeControllerRef.current?.signal,
                                 });
                             } catch (e) {
                                 if (
                                     e instanceof AbortError ||
                                     e instanceof NoPeersError
                                 )
                                     return;
                                 console.warn("Remote replicators not ready", e);
                             }
                         })
                     );
				}*/
				markWaited();
			}

			return drainRoundRobin(iterators, n, opts?.reason ?? "batch");
		} catch (e) {
			if (!isBenignLifecycleError(e)) throw e;
			return false;
		} finally {
			setIsLoading(false);
		}
	};

	loadMoreRef.current = loadMore;

	/* ────────────── live-merge listeners ────────────── */
	useEffect(() => {
		if (!options.updates) {
			return;
		}

		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [
		iteratorRefs.current.map((r) => r.db.address).join("|"),
		options.updates,
		options.query,
		options.resolve,
		options.reverse,
	]);

	/* ────────────── public API – unchanged from the caller's perspective ────────────── */
	return {
		items: all,
		loadMore,
		isLoading,
		empty: () => emptyResultsRef.current,
		id,
	};
};
