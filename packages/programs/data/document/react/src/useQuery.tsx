import { useState, useEffect, useRef, useMemo } from "react";
import {
    ClosedError,
    Documents,
} from "@peerbit/document";
import type {
    AbstractSearchRequest,
    AbstractSearchResult,
    Context,
    RemoteQueryOptions,
    ResultsIterator,
    WithContext,
} from "@peerbit/document";
import type { WithIndexedContext, UpdateOptions } from "@peerbit/document";
import * as indexerTypes from "@peerbit/indexer-interface";
import { v4 as uuid } from "uuid";

type QueryOptions = { query: QueryLike; id?: string };

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
    options: UseQuerySharedOptions<T, I, R, RT>
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

    /* ────────── rebuild iterators when db list / query etc. change ────────── */
    useEffect(() => {
        /* derive canonical list of open DBs */
        const openDbs = dbs.filter((d): d is Documents<T, I> =>
            Boolean(d && !d.closed)
        );
        const { query, resolve } = options;

        if (!openDbs.length || query == null) {
            reset();
            return;
        }

        reset();
        const abortSignal = closeControllerRef.current?.signal;
        const onMissedResults = (evt: { amount: number }) => {
            console.error("Not effective yet: missed results", evt);
            /* if (allRef.current.length > 0 || typeof options.remote !== "object" || !options.updates) {
                return;
            }
            console.log("Missed results, loading more", evt.amount);
            loadMore(evt.amount); */
        };
        let draining = false;
        const scheduleDrain = (ref: ResultsIterator<RT>, amount: number) => {
            log("Schedule drain", draining, ref, amount);
            if (draining) return;
            draining = true;
            loadMore(amount)
                .catch((e) => {
                    if (!(e instanceof ClosedError)) throw e;
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
                remote: options.remote
                    ? {
                        ...(typeof options?.remote === "object"
                            ? {
                                ...options.remote,
                                onLateResults: onMissedResults,
                                wait: {
                                    ...options?.remote?.wait,
                                    timeout:
                                        options?.remote?.wait?.timeout ??
                                        5000,
                                },
                            }
                            : options?.remote
                                ? {
                                    onLateResults: onMissedResults,
                                }
                                : undefined),
                    }
                    : undefined,
                resolve,
                signal: abortSignal,
                updates: {
                    push: true,
                    merge:
                        typeof options.updates === "boolean" && options.updates
                            ? true
                            : typeof options.updates === "object" &&
                                options.updates.merge
                                ? true
                                : false,
                    onChange: (evt) => {
                        log("Live update", evt);
                        if (evt.added.length > 0) {
                            scheduleDrain(
                                iterator as ResultsIterator<RT>,
                                evt.added.length
                            );
                        }
                    },
                    onResults: (batch, props) => {
                        log("onResults", { batch, props, currentRef: !!currentRef });
                        if (
                            props.reason === "join" ||
                            props.reason === "change"
                        ) {
                            if (!currentRef) return;
                            handleBatch(iteratorRefs.current, [
                                { ref: currentRef, items: batch as Item[] },
                            ]);
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
        batches: { ref: IteratorRef; items: Item[] }[]
    ): Promise<boolean> => {
        if (!iterators.length) {
            log("No iterators in handleBatch");
            return false;
        }

        const totalFetched = batches.reduce(
            (sum, batch) => sum + batch.items.length,
            0
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
                }))
            );
        }

        const prev = allRef.current;
        const next = [...prev];
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
                            item as WithContext<I> | WithIndexedContext<T, I>
                        ).primitive
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


        const combined = reverseRef.current
            ? [...freshItems.reverse(), ...next]
            : [...next, ...freshItems];

        log("Updating all with", {
            prevLength: prev.length,
            freshLength: freshItems.length,
            combinedLength: combined.length,
        });
        updateAll(combined);

        emptyResultsRef.current = iterators.every((i) => i.iterator.done());
        return !emptyResultsRef.current;
    };

    const drainRoundRobin = async (
        iterators: IteratorRef[],
        n: number
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
        return handleBatch(iterators, batches);
    };

    /*  maybe make the rule that if results are empty and we get results from joining  
     set the results to the joining results 
     when results are not empty use onMerge option to merge the results ?  */

    const loadMore = async (n: number = batchSize): Promise<boolean> => {
        const iterators = iteratorRefs.current;
        if (!iterators.length || emptyResultsRef.current) {
            log("No iterators or already empty", {
                length: iterators.length,
                emptyResultsRef: emptyResultsRef.current,
            });
            return false;
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

            return drainRoundRobin(iterators, n);
        } catch (e) {
            if (!(e instanceof ClosedError)) throw e;
            return false;
        } finally {
            setIsLoading(false);
        }
    };

    /* ────────────── live-merge listeners ────────────── */
    useEffect(() => {
        if (!options.updates) {
            return;
        }

        /* const listeners = iteratorRefs.current.map(({ db, id: itId }) => {
             const mergeFn =
                typeof options.onChange?.merge === "function"
                    ? options.onChange.merge
                    : (c: DocumentsChange<T, I>) => c; 

            const handler = async (e: CustomEvent<DocumentsChange<T, I>>) => {
                log("Merge change", e.detail, "it", itId);
                const filtered = await mergeFn(e.detail);
                if (
                    !filtered ||
                    (!filtered.added.length && !filtered.removed.length)
                )
                    return;

                let merged: Item[];
                if (options.onChange?.update) {
                    merged = await options.onChange.update(
                        allRef.current,
                        filtered
                    );
                } else {
                    merged = await db.index.updateResults(
                        allRef.current as WithContext<RT>[],
                        filtered,
                        options.query || {},
                        options.resolve ?? true
                    );
                }
                updateAll(options.reverse ? merged.reverse() : merged);
            };
            db.events.addEventListener("change", handler); 
            return { db, handler };
        });

        return () => {
            listeners.forEach(({ db, handler }) =>
                db.events.removeEventListener("change", handler)
            );
        }; */

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
