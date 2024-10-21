import { field, fixedArray, variant, vec } from "@dao-xyz/borsh";
import { Entry, EntryType, type ShallowEntry } from "@peerbit/log";
import { Log } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { TransportMessage } from "./message.js";

const logger = loggerFn({ module: "exchange-heads" });

/**
 * This thing allows use to faster sync since we can provide
 * references that can be read concurrently to
 * the entry when doing Log.fromEntry or Log.fromEntryHash
 */
@variant(0)
export class EntryWithRefs<T> {
	@field({ type: Entry })
	entry: Entry<T>;

	@field({ type: vec("string") })
	gidRefrences: string[]; // are some parents to the entry

	constructor(properties: { entry: Entry<T>; gidRefrences: string[] }) {
		this.entry = properties.entry;
		this.gidRefrences = properties.gidRefrences;
	}
}

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends TransportMessage {
	@field({ type: vec(EntryWithRefs) })
	heads: EntryWithRefs<T>[];

	@field({ type: fixedArray("u8", 4) })
	reserved: Uint8Array = new Uint8Array(4);

	constructor(props: { heads: EntryWithRefs<T>[] }) {
		super();
		this.heads = props.heads;
	}
}

@variant([0, 1])
export class RequestMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 2])
export class ResponseMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 3])
export class RequestIPrune extends TransportMessage {
	// Hashes which I want to prune
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 4])
export class ResponseIPrune extends TransportMessage {
	// Hashes I am allowed to prune
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

const MAX_EXCHANGE_MESSAGE_SIZE = 1e5; // 100kb. Too large size might not be faster (even if we can do 5mb)

export const createExchangeHeadsMessages = async function* (
	log: Log<any>,
	heads: Entry<any>[] | string[],
): AsyncGenerator<ExchangeHeadsMessage<any>, void, void> {
	let size = 0;
	let current: EntryWithRefs<any>[] = [];
	const visitedHeads = new Set<string>();
	for (const fromHead of heads) {
		let entry = fromHead instanceof Entry ? fromHead : await log.get(fromHead);
		if (!entry) {
			continue; // missing this entry, could be deleted while iterating
		}

		visitedHeads.add(entry.hash);

		// TODO eventually we don't want to load all refs
		// since majority of the old leader would not be interested in these anymore
		const refs = (await allEntriesWithUniqueGids(log, entry)).filter((x) => {
			if (visitedHeads.has(x.hash)) {
				return false;
			}
			visitedHeads.add(x.hash);
			return true;
		});

		if (refs.length > 1000) {
			logger.warn("Large refs count: ", refs.length);
		}
		current.push(
			new EntryWithRefs({
				entry,
				gidRefrences: refs.map((x) => x.meta.gid),
			}),
		);

		size += entry.size;
		if (size > MAX_EXCHANGE_MESSAGE_SIZE) {
			size = 0;
			yield new ExchangeHeadsMessage({
				heads: current,
			});
			current = [];
			continue;
		}
	}
	if (current.length > 0) {
		yield new ExchangeHeadsMessage({
			heads: current,
		});
	}
};

export const allEntriesWithUniqueGids = async (
	log: Log<any>,
	entry: Entry<any>,
): Promise<Entry<any>[]> => {
	// TODO optimize this
	const map: Map<string, ShallowEntry | Entry<any>> = new Map();
	let curr: (Entry<any> | ShallowEntry)[] = [entry];
	while (curr.length > 0) {
		const nexts: (Entry<any> | ShallowEntry)[] = [];
		for (const element of curr) {
			if (!map.has(element.meta.gid)) {
				map.set(element.meta.gid, element);
				if (element.meta.type === EntryType.APPEND) {
					for (const next of element.meta.next) {
						const indexedEntry = await log.entryIndex.getShallow(next);
						if (!indexedEntry) {
							logger.error(
								"Failed to find indexed entry for hash when fetching references: " +
									next,
							);
						} else {
							nexts.push(indexedEntry.value);
						}
					}
				}
			}
			curr = nexts;
		}
	}
	const value = [
		...(await Promise.all(
			[...map.values()].map((x) =>
				x instanceof Entry ? x : log.entryIndex.get(x.hash),
			),
		)),
	].filter((x) => !!x) as Entry<any>[];
	return value;
};
