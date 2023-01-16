import pMap from "p-map";
import pDoWhilst from "p-do-whilst";
import { Entry } from "./entry.js";
import { PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";
import { Encoding, JSON_ENCODING } from "./encoding.js";
import { Timestamp } from "./clock.js";
import { BlockStore } from "@dao-xyz/libp2p-direct-block";

export interface EntryFetchOptions<T> {
    length?: number;
    timeout?: number;
    onFetched?: (entry: Entry<T>) => void;
    concurrency?: number;
    encoding?: Encoding<T>;
    encryption?: PublicKeyEncryptionResolver;
}
interface EntryFetchStrictOptions<T> {
    length: number;
    timeout?: number;
    onFetched?: (entry: Entry<T>) => void;
    concurrency: number;
    encoding?: Encoding<T>;
    encryption?: PublicKeyEncryptionResolver;
}

export interface EntryFetchAllOptions<T> extends EntryFetchOptions<T> {
    shouldFetch?: (string: string) => boolean;
    shouldQueue?: (string: string) => boolean;
    onFetch?: (hash: string) => void;
    onQueueCallback?: (hash: string) => void;
    cache?: Map<string, Entry<any>>;
    delay?: number;
}
interface EntryFetchAllStrictOptions<T> extends EntryFetchStrictOptions<T> {
    shouldFetch?: (string: string) => boolean;
    shouldQueue?: (string: string) => boolean;
    onQueueCallback?: (hash: string) => void;
    onFetch?: (hash: string) => void;
    cache?: Map<string, Entry<any>>;
    delay: number;
}

export const strictAllFetchOptions = <T>(
    options: EntryFetchAllOptions<T>
): EntryFetchAllStrictOptions<T> => {
    const ret: EntryFetchAllStrictOptions<T> = {
        ...options,
    } as any;
    if (ret.length == undefined) {
        ret.length = -1;
    }
    if (ret.concurrency == undefined) {
        ret.concurrency = 32;
    }
    if (ret.delay == undefined) {
        ret.delay = 0;
    }
    return ret;
};
export const strictFetchOptions = <T>(
    options: EntryFetchOptions<T>
): EntryFetchStrictOptions<T> => {
    const ret: EntryFetchStrictOptions<T> = {
        ...options,
    } as any;
    if (ret.length == undefined) {
        ret.length = -1;
    }

    if (ret.concurrency == undefined) {
        ret.concurrency = 32;
    }
    return ret;
};

export class EntryIO {
    // Fetch log graphs in parallel
    static async fetchParallel<T>(
        store: BlockStore,
        hashes: string | string[],
        options: EntryFetchAllOptions<T>
    ): Promise<Entry<T>[]> {
        const queued = new Set<string>();
        const fetched = new Set<string>();

        const onQueueCallback = (hash: string) => {
            queued.add(hash);
            options.onQueueCallback && options.onQueueCallback(hash);
        };

        const onFetched = (entry: Entry<any>) => {
            fetched.add(entry.hash);
            options.onFetched && options.onFetched(entry);
        };

        const fetchOptions = {
            ...options,
            onQueueCallback,
            onFetched,
            shouldQueue: (hash) => {
                if (queued.has(hash)) {
                    return false;
                }
                if (options.shouldQueue) {
                    if (!options.shouldQueue(hash)) {
                        return false;
                    }
                }
                return true;
            },
            shouldFetch: (hash) => {
                if (fetched.has(hash)) {
                    return false;
                }
                if (options.shouldFetch) {
                    if (!options.shouldFetch(hash)) {
                        return false;
                    }
                }
                return true;
            },
        };
        const fetchOne = async (hash: string) =>
            EntryIO.fetchAll(store, hash, fetchOptions);
        const concatArrays = (arr1: any[], arr2: any) => arr1.concat(arr2);
        const flatten = (arr: any[]) => arr.reduce(concatArrays, []);
        const res = await pMap(hashes, fetchOne, {
            concurrency: Math.max(options.concurrency || hashes.length, 1),
        });
        return flatten(res);
    }

    static async fetchAll<T>(
        store: BlockStore,
        hashes: string | string[],
        fetchOptions: EntryFetchAllOptions<T>
    ): Promise<Entry<T>[]> {
        const options = strictAllFetchOptions(fetchOptions);

        const result: Entry<T>[] = [];
        const cache = new Set<string>();
        const loadingCache = new Set();
        const loadingQueue: { ts: bigint; hash: string }[] =
            []; /* { [key: number | string]: string[] } =
            Array.isArray(hashes) ? { 0: hashes.slice() } : { 0: [hashes] } */
        let running = 0; // keep track of how many entries are being fetched at any time
        let maxClock = new Timestamp({ wallTime: 0n, logical: 0 }); // keep track of the latest clock time during load
        let minClock = new Timestamp({ wallTime: 0n, logical: 0 }); // keep track of the minimum clock time during load
        // const shouldFetch = options.shouldFetch || (() => true); // default fn returns false to not exclude any hash
        const shouldFetch = (hash: string) =>
            !options.shouldFetch || options.shouldFetch(hash);
        // Does the loading queue have more to process?
        const loadingQueueHasMore = () => loadingQueue.length > 0;
        //Object.values(loadingQueue).find(hasItems) !== undefined;

        // Add a multihash to the loading queue
        const addToLoadingQueue = (e: Entry<T> | string, ts: bigint) => {
            const hash = e instanceof Entry ? e.hash : e;
            if (
                !loadingCache.has(hash) &&
                (!options.shouldQueue || options.shouldQueue(hash))
            ) {
                options.onQueueCallback && options.onQueueCallback(hash);
                loadingQueue.push({ hash, ts });
                loadingQueue.sort((a, b) =>
                    a.ts > b.ts ? -1 : a.ts === b.ts ? 0 : 1
                ); // ascending
                loadingCache.add(hash);
            }
        };

        (Array.isArray(hashes) ? [...hashes] : [hashes]).forEach((hash) => {
            addToLoadingQueue(hash, 0n);
        });

        // Get the next items to process from the loading queue
        const getNextFromQueue = (length = 1) => {
            return loadingQueue
                .splice(0, Math.min(length, loadingQueue.length))
                .map((x) => x.hash);
        };

        const addToResults = async (entry: Entry<T>) => {
            if (!cache.has(entry.hash) && shouldFetch(entry.hash)) {
                entry.init({
                    encryption: options.encryption,
                    encoding: options.encoding || JSON_ENCODING,
                });

                // Todo check bigint conversions
                const ts = (await entry.getClock()).timestamp;

                // Update min/max clocks'
                maxClock = Timestamp.bigger(maxClock, ts);
                minClock =
                    result.length > 0
                        ? Timestamp.bigger(
                              (await result[result.length - 1].metadata).clock
                                  .timestamp,
                              minClock
                          )
                        : maxClock;

                const isLater =
                    result.length >= options.length && ts >= minClock;

                // Add the entry to the results if
                // 1) we're fetching all entries
                // 2) results is not filled yet
                // the clock of the entry is later than current known minimum clock time
                if (
                    (options.length < 0 ||
                        result.length < options.length ||
                        isLater) &&
                    shouldFetch(entry.hash) &&
                    !cache.has(entry.hash)
                ) {
                    result.push(entry);
                    cache.add(entry.hash);
                }
                const nextSorted = [...entry.next].sort();
                if (options.length < 0) {
                    // If we're fetching all entries (length === -1), adds nexts and refs to the queue
                    nextSorted.forEach((e) =>
                        addToLoadingQueue(e, ts.wallTime)
                    );
                } else {
                    // If we're fetching entries up to certain length,
                    // fetch the next if result is filled up, to make sure we "check"
                    // the next entry if its clock is later than what we have in the result
                    if (
                        result.length < options.length ||
                        ts.compare(minClock) > 0 ||
                        (ts.compare(minClock) === 0 &&
                            !cache.has(entry.hash) &&
                            shouldFetch(entry.hash))
                    ) {
                        nextSorted.forEach(
                            (e) =>
                                addToLoadingQueue(
                                    e,
                                    ts.wallTime
                                    /* ,
									maxClock.wallTime - ts.wallTime */
                                ) // approximation, we ignore logical
                        );
                    }
                }
            }
        };

        // Fetch one entry and add it to the results
        const fetchEntry = async (hash: string) => {
            if (!hash || cache.has(hash) || !shouldFetch(hash)) {
                return;
            }

            if (options.cache) {
                const entry = options.cache.get(hash);
                if (entry) {
                    await addToResults(entry);
                    return;
                }
            }

            /* eslint-disable no-async-promise-executor */
            return new Promise(async (resolve, reject) => {
                // Resolve the promise after a timeout (if given) in order to
                // not get stuck loading a block that is unreachable
                const timer =
                    options.timeout && options.timeout > 0
                        ? setTimeout(() => {
                              console.warn(
                                  `Warning: Couldn't fetch entry '${hash}', request timed out (${options.timeout}ms)`
                              );
                              resolve(undefined);
                          }, options.timeout)
                        : null;

                if (options.onFetch) {
                    options.onFetch(hash);
                }

                try {
                    // Load the entry
                    const entry = await Entry.fromMultihash<T>(store, hash);
                    // Simulate network latency (for debugging purposes)
                    if (options.delay > 0) {
                        const sleep = (ms = 0) =>
                            new Promise((resolve) => setTimeout(resolve, ms));
                        await sleep(options.delay);
                    }
                    // Add it to the results
                    await addToResults(entry);
                    if (options.onFetched) {
                        options.onFetched(entry);
                    }
                    resolve(undefined);
                } catch (e: any) {
                    reject(e);
                } finally {
                    if (timer) clearTimeout(timer);
                }
            });
        };

        // One loop of processing the loading queue
        const _processQueue = async () => {
            if (running < options.concurrency) {
                const nexts = getNextFromQueue(options.concurrency);
                running += nexts.length;
                await pMap(nexts, fetchEntry, {
                    concurrency: options.concurrency,
                });
                running -= nexts.length;
            }
        };

        // Fetch entries
        await pDoWhilst(_processQueue, loadingQueueHasMore);

        return result;
    }
}
