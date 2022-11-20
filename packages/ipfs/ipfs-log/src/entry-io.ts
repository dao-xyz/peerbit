import pMap from "p-map";
import pDoWhilst from "p-do-whilst";
import { Entry } from "./entry";
import { IPFS } from "ipfs-core-types";
import { PublicKeyEncryptionResolver } from "@dao-xyz/peerbit-crypto";
import { Encoding, JSON_ENCODING } from "./encoding";
import { Timestamp } from "./clock";

export interface EntryFetchOptions<T> {
    length?: number;
    timeout?: number;
    exclude?: any[];
    onProgressCallback?: (entry: Entry<T>) => void;
    concurrency?: number;
    encoding?: Encoding<T>;
    encryption?: PublicKeyEncryptionResolver;
}
interface EntryFetchStrictOptions<T> {
    length: number;
    timeout?: number;
    exclude: any[];
    onProgressCallback?: (entry: Entry<T>) => void;
    concurrency: number;
    encoding?: Encoding<T>;
    encryption?: PublicKeyEncryptionResolver;
}

export interface EntryFetchAllOptions<T> extends EntryFetchOptions<T> {
    shouldExclude?: (string: string) => boolean;
    onStartProgressCallback?: any;
    delay?: number;
}
interface EntryFetchAllStrictOptions<T> extends EntryFetchStrictOptions<T> {
    shouldExclude?: (string: string) => boolean;
    onStartProgressCallback?: any;
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
    if (ret.exclude == undefined) {
        ret.exclude = [];
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
    if (ret.exclude == undefined) {
        ret.exclude = [];
    }
    if (ret.concurrency == undefined) {
        ret.concurrency = 32;
    }
    return ret;
};

export class EntryIO {
    // Fetch log graphs in parallel
    static async fetchParallel<T>(
        ipfs: IPFS,
        hashes: string | string[],
        options: EntryFetchAllOptions<T>
    ): Promise<Entry<T>[]> {
        const fetchOne = async (hash: string) =>
            EntryIO.fetchAll(ipfs, hash, options);
        const concatArrays = (arr1: any[], arr2: any) => arr1.concat(arr2);
        const flatten = (arr: any[]) => arr.reduce(concatArrays, []);
        const res = await pMap(hashes, fetchOne, {
            concurrency: Math.max(options.concurrency || hashes.length, 1),
        });
        return flatten(res);
    }

    /**
     * Fetch log entries
     *
     * @param {IPFS} [ipfs] An IPFS instance
     * @param {string} [hash] Multihash of the entry to fetch
     * @param {string} [parent] Parent of the node to be fetched
     * @param {Object} [all] Entries to skip
     * @param {Number} [amount=-1] How many entries to fetch
     * @param {Number} [depth=0] Current depth of the recursion
     * @param {function(entry)} shouldExclude A function that can be passed to determine whether a specific hash should be excluded, ie. not fetched. The function should return true to indicate exclusion, otherwise return false.
     * @param {function(entry)} onProgressCallback Called when an entry was fetched
     * @returns {Promise<Array<Entry<T>>>}
     */
    static async fetchAll<T>(
        ipfs: IPFS,
        hashes: string | string[],
        fetchOptions: EntryFetchAllOptions<T>
    ): Promise<Entry<T>[]> {
        const options = strictAllFetchOptions(fetchOptions);

        const result: Entry<T>[] = [];
        const cache: { [key: string]: any } = {};
        const loadingCache: { [key: string]: any } = {};
        const loadingQueue: { ts: bigint; hash: string }[] =
            []; /* { [key: number | string]: string[] } =
            Array.isArray(hashes) ? { 0: hashes.slice() } : { 0: [hashes] } */
        let running = 0; // keep track of how many entries are being fetched at any time
        let maxClock = new Timestamp({ wallTime: 0n, logical: 0 }); // keep track of the latest clock time during load
        let minClock = new Timestamp({ wallTime: 0n, logical: 0 }); // keep track of the minimum clock time during load
        const shouldExclude = options.shouldExclude || (() => false); // default fn returns false to not exclude any hash

        // Does the loading queue have more to process?
        const loadingQueueHasMore = () => loadingQueue.length > 0;
        //Object.values(loadingQueue).find(hasItems) !== undefined;

        // Add a multihash to the loading queue
        const addToLoadingQueue = (e: Entry<T> | string, ts: bigint) => {
            const hash = e instanceof Entry ? e.hash : e;
            if (!loadingCache[hash] && !shouldExclude(hash)) {
                loadingQueue.push({ hash, ts });
                loadingQueue.sort((a, b) =>
                    a.ts > b.ts ? -1 : a.ts === b.ts ? 0 : 1
                ); // ascending
                loadingCache[hash] = true;
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

        // Add entries that we don't need to fetch to the "cache"
        const addToExcludeCache = (e: Entry<any> | string) => {
            cache[e instanceof Entry ? e.hash : e] = true;
        };

        // Fetch one entry and add it to the results
        const fetchEntry = async (hash: string) => {
            if (!hash || cache[hash] || shouldExclude(hash)) {
                return;
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

                const addToResults = async (entry: Entry<T>) => {
                    if (!cache[entry.hash] && !shouldExclude(entry.hash)) {
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
                                      (await result[result.length - 1].metadata)
                                          .clock.timestamp,
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
                            !shouldExclude(entry.hash) &&
                            !cache[entry.hash]
                        ) {
                            result.push(entry);
                            cache[entry.hash] = true;

                            if (options.onProgressCallback) {
                                options.onProgressCallback(entry);
                            }
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
                                    !cache[entry.hash] &&
                                    !shouldExclude(entry.hash))
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

                if (options.onStartProgressCallback) {
                    options.onStartProgressCallback(
                        hash,
                        null,
                        0,
                        result.length
                    );
                }

                try {
                    // Load the entry
                    const entry = await Entry.fromMultihash<T>(ipfs, hash);
                    // Simulate network latency (for debugging purposes)
                    if (options.delay > 0) {
                        const sleep = (ms = 0) =>
                            new Promise((resolve) => setTimeout(resolve, ms));
                        await sleep(options.delay);
                    }
                    // Add it to the results
                    await addToResults(entry);
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

        // Add entries to exclude from processing to the cache before we start
        options.exclude.forEach(addToExcludeCache);

        // Fetch entries
        await pDoWhilst(_processQueue, loadingQueueHasMore);

        return result;
    }
}
