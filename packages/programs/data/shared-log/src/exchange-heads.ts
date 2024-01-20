import { variant, field, vec, fixedArray } from "@dao-xyz/borsh";
import { Entry, EntryType, ShallowEntry } from "@peerbit/log";
import { Log } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { TransportMessage } from "./message.js";
import { Cache } from "@peerbit/cache";

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

const MAX_EXCHANGE_MESSAGE_SIZE = 5e6; // 5mb (since stream limits are 10mb)

export const createExchangeHeadsMessages = async (
	log: Log<any>,
	heads: Entry<any>[],
	gidParentCache: Cache<Entry<any>[]>
): Promise<ExchangeHeadsMessage<any>[]> => {
	const messages: ExchangeHeadsMessage<any>[] = [];
	let size = 0;
	let current: EntryWithRefs<any>[] = [];
	const visitedHeads = new Set<string>();
	for (const fromHead of heads) {
		visitedHeads.add(fromHead.hash);

		// TODO eventually we don't want to load all refs
		// since majority of the old leader would not be interested in these anymore
		const refs = (
			await allEntriesWithUniqueGids(log, fromHead, gidParentCache)
		).filter((x) => {
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
				entry: fromHead,
				gidRefrences: refs.map((x) => x.meta.gid)
			})
		);

		size += fromHead.size;
		if (size > MAX_EXCHANGE_MESSAGE_SIZE) {
			size = 0;
			messages.push(
				new ExchangeHeadsMessage({
					heads: current
				})
			);
			current = [];
			continue;
		}
	}
	if (current.length > 0) {
		messages.push(
			new ExchangeHeadsMessage({
				heads: current
			})
		);
	}
	return messages;
};

export const allEntriesWithUniqueGids = async (
	log: Log<any>,
	entry: Entry<any>,
	gidParentCache: Cache<Entry<any>[]>
): Promise<Entry<any>[]> => {
	const cachedValue = gidParentCache.get(entry.hash);
	if (cachedValue != null) {
		return cachedValue;
	}

	// TODO optimize this
	const map: Map<string, ShallowEntry> = new Map();
	let curr: ShallowEntry[] = [entry];
	while (curr.length > 0) {
		const nexts: ShallowEntry[] = [];
		for (const element of curr) {
			if (!map.has(element.meta.gid)) {
				map.set(element.meta.gid, element);
				if (element.meta.type === EntryType.APPEND) {
					for (const next of element.meta.next) {
						const indexedEntry = log.entryIndex.getShallow(next);
						if (!indexedEntry) {
							logger.error(
								"Failed to find indexed entry for hash when fetching references: " +
									next
							);
						} else {
							nexts.push(indexedEntry);
						}
					}
				}
			}
			curr = nexts;
		}
	}
	const value = [
		...(await Promise.all(
			[...map.values()].map((x) => log.entryIndex.get(x.hash))
		))
	].filter((x) => !!x) as Entry<any>[];
	gidParentCache.add(entry.hash, value);
	return value;
};
