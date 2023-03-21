import { Change, Log } from "@dao-xyz/peerbit-log";
import { Entry } from "@dao-xyz/peerbit-log";
import { EntryWithRefs } from "./entry-with-refs";

export const join = async <T>(
	entry: EntryWithRefs<T>[] | Entry<T>[] | string[],
	into: Log<T>,
	options?: {
		concurrency?: number;
		onFetched?: (entry: Entry<any>) => void;
	}
): Promise<Change<T>> => {
	// Notify the Store that we made progress

	const shouldFetch = (h: string) => {
		if (!h) {
			throw new Error("Unexpected");
		}
		return !into.has(h);
	};

	let log: Log<any>;
	if (
		typeof entry === "string" ||
		(Array.isArray(entry) &&
			(entry.length === 0 || typeof entry[0] === "string"))
	) {
		log = await Log.fromEntryHash<T>(
			into.storage,
			into.identity,
			entry as string | string[],
			{
				// TODO, load all store options?
				encryption: into.encryption,
				encoding: into.encoding,
				sortFn: into.sortFn,
				length: -1,
				exclude: [],
				shouldFetch,
				concurrency: options?.concurrency,
				onFetched: options?.onFetched,
				replicate: true,
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

		log = await Log.fromEntry(into.storage, into.identity, entries, {
			// TODO, load all store options?
			encryption: into.encryption,
			encoding: into.encoding,
			sortFn: into.sortFn,
			length: -1,
			shouldFetch,
			concurrency: options?.concurrency,
			onFetched: options?.onFetched,
			replicate: true,
		});
	}
	return into.join(log);
};
