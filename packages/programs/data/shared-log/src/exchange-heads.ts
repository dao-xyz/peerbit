import { variant, field, vec, fixedArray } from "@dao-xyz/borsh";
import { Entry, ShallowEntry } from "@peerbit/log";
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

	@field({ type: vec(Entry) })
	references: Entry<T>[]; // are some parents to the entry

	constructor(properties: { entry: Entry<T>; references: Entry<T>[] }) {
		this.entry = properties.entry;
		this.references = properties.references;
	}
}

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends TransportMessage {
	@field({ type: vec(EntryWithRefs) })
	heads: EntryWithRefs<T>[];

	@field({ type: fixedArray("u8", 4) })
	reserved: Uint8Array = new Uint8Array(4);

	constructor(props: { logId: Uint8Array; heads: EntryWithRefs<T>[] }) {
		super();
		this.heads = props.heads;
	}
}

@variant([0, 1])
export class RequestHeadsMessage extends TransportMessage {
	@field({ type: "string" })
	address: string;

	constructor(props: { topic: string; address: string }) {
		super();
		if (props) {
			this.address = props.address;
		}
	}
}

@variant([0, 2])
export class RequestHasEntries extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 3])
export class ResponseHasEntries extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

export const createExchangeHeadsMessage = async (
	log: Log<any>,
	heads: Entry<any>[],
	includeReferences: boolean
) => {
	const headsSet = new Set(heads);
	const headsWithRefs = await Promise.all(
		heads.map(async (head) => {
			const refs = !includeReferences
				? []
				: (await allEntriesWithUniqueGids(log, head)) // 1mb total limit split on all heads
						.filter((r) => !headsSet.has(r)); // pick a proportional amount of refs so we can efficiently load the log. TODO should be equidistant for good performance?
			return new EntryWithRefs({
				entry: head,
				references: refs,
			});
		})
	);
	logger.debug(`Send latest heads of '${log.id}'`);
	return new ExchangeHeadsMessage({
		logId: log.id!,
		heads: headsWithRefs,
	});
};

export const allEntriesWithUniqueGids = async (
	log: Log<any>,
	entry: Entry<any>
): Promise<Entry<any>[]> => {
	// TODO optimize this
	const map: Map<string, ShallowEntry> = new Map();
	let curr: ShallowEntry[] = [entry];
	while (curr.length > 0) {
		const nexts: ShallowEntry[] = [];
		for (const element of curr) {
			if (!map.has(element.meta.gid)) {
				map.set(element.meta.gid, element);
				for (const next of element.meta.next) {
					const indexedEntry = log.entryIndex.getShallow(next);
					if (!indexedEntry) {
						logger.error("Failed to find indexed entry for hash: " + next);
					} else {
						nexts.push(indexedEntry);
					}
				}
			}
			curr = nexts;
		}
	}
	return [
		...(await Promise.all(
			[...map.values()].map((x) => log.entryIndex.get(x.hash))
		)),
	].filter((x) => !!x) as Entry<any>[];
};
