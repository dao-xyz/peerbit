import { Entry } from "./entry.js";
import { type Blocks } from "@peerbit/blocks-interface";
import {
	BinaryReader,
	BinaryWriter,
	deserialize,
	field,
	fixedArray,
	serialize,
	variant,
	vec
} from "@dao-xyz/borsh";
import { waitFor } from "@peerbit/time";
import { type AnyStore } from "@peerbit/any-store";
import { logger } from "./logger.js";

@variant(0)
export class Snapshot {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: vec("string") })
	heads: string[];

	@field({ type: "u64" })
	size: bigint; // we do a size field, because the "true" log size can be larger then the size of values provided below

	@field({ type: vec(Entry) })
	values: Entry<any>[];

	constructor(props: {
		id: Uint8Array;
		heads: string[];
		size: bigint;
		values: Entry<any>[];
	}) {
		this.heads = props.heads;
		this.id = props.id;
		this.size = props.size;
		this.values = props.values;
	}
}

export const save = async <T>(
	snapshotPath: string,
	blockstore: Blocks,
	cache: AnyStore,
	log: {
		id: Uint8Array;
		getHeads: () => Promise<string[]>;
		getValues: () => Promise<Entry<T>[]> | Entry<T>[];
	}
): Promise<string> => {
	const values = await log.getValues();
	const buf = serialize(
		new Snapshot({
			id: log.id,
			heads: await log.getHeads(),
			size: BigInt(values.length),
			values: values
		})
	);

	const snapshot = await blockstore.put(buf);
	const writer = new BinaryWriter();
	writer.string(snapshot);
	await cache.put(snapshotPath, writer.finalize());

	await waitFor(async () => (await cache.get(snapshotPath)) != null, {
		delayInterval: 200,
		timeout: 10 * 1000
	});

	logger.debug(`Saved snapshot: ${snapshot}`);
	return snapshot;
};

export const load = async (
	hash: string,
	blockstore: Blocks
): Promise<Snapshot> => {
	const block = await blockstore.get(hash);
	if (!block) {
		throw new Error("Missing snapshot for CID: " + hash);
	}
	return deserialize(block, Snapshot);
};

export const loadFromCache = async (
	path: string,
	blockstore: Blocks,
	cache: AnyStore
) => {
	const snapshotOrCID = await cache.get(path);
	if (!snapshotOrCID) {
		throw new Error("Missing snapshot CID from local store");
	}
	const reader = new BinaryReader(snapshotOrCID);
	const snapshotCIDString = reader.string();
	return load(snapshotCIDString, blockstore);
};
