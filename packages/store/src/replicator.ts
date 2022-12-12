import { Log } from "@dao-xyz/peerbit-log";
import { Entry } from "@dao-xyz/peerbit-log";
import { EntryWithRefs } from "./entry-with-refs";

export const join = async <T>(
    entry: EntryWithRefs<T>[] | Entry<T>[] | string[],
    into: Log<T>,
    options?: {
        concurrency?: number;
        onFetched?: (entry: Entry<any>) => void;
    }
): Promise<{ change: Entry<T>[] }> => {
    // Notify the Store that we made progress

    const shouldFetch = (h: string) => {
        return !!h && !into.has(h);
    };

    let log: Log<any>;
    if (
        typeof entry === "string" ||
        (Array.isArray(entry) &&
            (entry.length === 0 || typeof entry[0] === "string"))
    ) {
        log = await Log.fromEntryHash<T>(
            into._storage,
            into._identity,
            entry as string | string[],
            {
                // TODO, load all store options?
                encryption: into._encryption,
                encoding: into._encoding,
                sortFn: into._sortFn,
                length: -1,
                exclude: [],
                shouldFetch,
                concurrency: options?.concurrency,
                onFetched: options?.onFetched,
            }
        );
    } else {
        let entries: Entry<any>[];
        if (Array.isArray(entry)) {
            if (entry[0] instanceof Entry) {
                entries = entry as Entry<any>[];
            } else {
                entries = [];
                for (const e of entry as EntryWithRefs<any>[]) {
                    entries.push(e.entry);
                    e.references.forEach((ref) => {
                        entries.push(ref);
                    });
                }
            }
        } else {
            entries = [entry];
        }

        log = await Log.fromEntry(into._storage, into._identity, entries, {
            // TODO, load all store options?
            encryption: into._encryption,
            encoding: into._encoding,
            sortFn: into._sortFn,
            length: -1,
            shouldFetch,
            concurrency: options?.concurrency,
            onFetched: options?.onFetched,
        });
    }
    return into.join(log);
};
