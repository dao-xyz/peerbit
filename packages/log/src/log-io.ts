import { Entry } from "./entry.js";
import {
    EntryFetchAllOptions,
    EntryIO,
    strictFetchOptions,
} from "./entry-io.js";
import { ISortFunction, LastWriteWins, NoZeroes } from "./log-sorting.js";
import * as LogError from "./log-errors.js";
import { Blocks } from "@dao-xyz/peerbit-block";
import { isDefined } from "./is-defined.js";
import { findUniques } from "./find-uniques.js";
import { difference } from "./difference.js";
import { Log } from "./log.js";
import { JSON_ENCODING } from "./encoding.js";

const IPLD_LINKS = ["heads"];

const last = (arr: any[], n: number) =>
    arr.slice(arr.length - Math.min(arr.length, n), arr.length);

export class LogIO {
    //
    /**
     * Get the multihash of a Log.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Log} log Log to get a multihash for
     * @returns {Promise<string>}
     * @deprecated
     */
    static async toMultihash(
        store: Blocks,
        log: Log<any>,
        options: { format?: string } = {}
    ) {
        let format = options.format;
        if (!isDefined(format)) {
            format = "dag-cbor";
        }
        if (log.values.length < 1)
            throw new Error("Can't serialize an empty log");

        return store.put(log.toJSON(), format!, {
            links: IPLD_LINKS,
        });
    }

    static async fromMultihash<T>(
        store: Blocks,
        hash: string,
        options: EntryFetchAllOptions<T> & { sortFn: any }
    ) {
        if (!isDefined(hash)) throw new Error(`Invalid hash: ${hash}`);

        const logData = await store.get<{ id: string; heads: string[] }>(hash, {
            links: IPLD_LINKS,
        });

        if (!logData) {
            throw new Error("Not found");
        }

        if (!logData.heads || !logData.id) throw LogError.NotALogError();

        // Use user provided sorting function or the default one
        const sortFn = options.sortFn || NoZeroes(LastWriteWins);
        const isHead = (e: Entry<any>) => logData.heads.includes(e.hash);

        const all = await EntryIO.fetchAll(
            store,
            logData.heads as any as string[],
            strictFetchOptions(options)
        ); // TODO fix typings
        const length = options.length || -1;
        const logId = logData.id;
        const entries = length > -1 ? last(all.sort(sortFn), length) : all;
        const heads = entries.filter(isHead);
        return { logId, entries, heads };
    }

    static async fromEntryHash<T>(
        store: Blocks,
        hash: string[] | string,
        options: EntryFetchAllOptions<T> & { sortFn?: ISortFunction }
    ) {
        if (!isDefined(hash)) throw new Error("'hash' must be defined");
        const length = options.length || -1;

        // Convert input hash(s) to an array
        const hashes = Array.isArray(hash) ? hash : [hash];
        // Fetch given length, return size at least the given input entries
        if (length > -1) {
            options = {
                ...options,
                length: Math.max(length, 1),
            };
        }

        const all = await EntryIO.fetchParallel<T>(store, hashes, options);
        // Cap the result at the right size by taking the last n entries,
        // or if given length is -1, then take all
        options.sortFn = options.sortFn || NoZeroes(LastWriteWins);
        const entries =
            length > -1 ? last(all.sort(options.sortFn), length) : all;
        return { entries };
    }

    /**
     * Creates a log data from a JSON object, to be passed to a Log constructor
     *
     * @param {IPFS} ipfs An IPFS instance
     * @param {json} json A json object containing valid log data
     * @param {Object} options
     * @param {number} options.length How many entries to include
     * @param {function(hash, entry,  parent, depth)} options.onFetched
     **/
    static async fromJSON<T>(
        store: Blocks,
        json: { id: string; heads: string[] },
        options: EntryFetchAllOptions<T>
    ) {
        const { id, heads } = json;
        const all: Entry<T>[] = await EntryIO.fetchParallel(
            store,
            heads,
            options
        );
        const entries = all.sort(Entry.compare);
        return { logId: id, entries, heads };
    }

    static async fromEntry<T>(
        store: Blocks,
        sourceEntries: Entry<T>[] | Entry<T>,
        options: EntryFetchAllOptions<T>
    ): Promise<{ entries: Entry<T>[] }> {
        if (!Array.isArray(sourceEntries)) {
            sourceEntries = [sourceEntries];
        }
        const length = options.length || -1;

        // Fetch given length, return size at least the given input entries
        if (length > -1) {
            options = {
                ...options,
                length: Math.max(length, sourceEntries.length),
            };
        }

        // Make sure we pass hashes instead of objects to the fetcher function
        let hashes: string[] = [];
        const cache = options.cache || new Map<string, Entry<any>>();
        for (const e of sourceEntries) {
            e.init({
                encryption: options.encryption,
                encoding: options.encoding || JSON_ENCODING,
            });

            cache.set(e.hash, e);
            (await e.getNext()).forEach((n) => {
                hashes.push(n);
            });
        }

        if (options.shouldFetch) {
            hashes = hashes.filter((h) =>
                (options.shouldFetch as (h: string) => boolean)(h)
            );
        }

        if (options.onFetched) {
            for (const entry of sourceEntries) {
                options.onFetched(entry);
            }
        }

        // Fetch the entries
        const all = await EntryIO.fetchParallel(store, hashes, {
            ...options,
            cache,
        });

        // Combine the fetches with the source entries and take only uniques
        const combined = sourceEntries.concat(all);
        const uniques = findUniques(combined, "hash").sort(Entry.compare);

        // Cap the result at the right size by taking the last n entries
        const sliced = uniques.slice(length > -1 ? -length : -uniques.length);

        // Make sure that the given input entries are present in the result
        // in order to not lose references
        const missingSourceEntries = difference(sliced, sourceEntries, "hash");

        const replaceInFront = (
            a: Entry<T>[],
            withEntries: Entry<T>[]
        ): Entry<T>[] => {
            const sliced = a.slice(withEntries.length, a.length);
            return withEntries.concat(sliced);
        };

        // Add the input entries at the beginning of the array and remove
        // as many elements from the array before inserting the original entries
        const entries = replaceInFront(sliced, missingSourceEntries);
        return { entries };
    }
}
