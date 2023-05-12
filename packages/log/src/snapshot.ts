import { Entry } from "./entry.js";
import { BlockStore } from "@dao-xyz/libp2p-direct-block";
import { createBlock, getBlockValue } from "@dao-xyz/libp2p-direct-block";
import {
	BinaryReader,
	BinaryWriter,
	deserialize,
	field,
	fixedArray,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { waitForAsync } from "@dao-xyz/peerbit-time";
import LocalStore from "@dao-xyz/lazy-level";
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

	constructor(props?: {
		id: Uint8Array;
		heads: string[];
		size: bigint;
		values: Entry<any>[];
	}) {
		if (props) {
			this.heads = props.heads;
			this.id = props.id;
			this.size = props.size;
			this.values = props.values;
		}
	}
}

export const save = async <T>(
	snapshotPath: string,
	blockstore: BlockStore,
	cache: LocalStore,
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
			values: values,
		})
	);

	const snapshot = await blockstore.put(await createBlock(buf, "raw"));
	const writer = new BinaryWriter();
	writer.string(snapshot);
	await cache.set(snapshotPath, writer.finalize());

	await waitForAsync(() => cache.get(snapshotPath).then((bytes) => !!bytes), {
		delayInterval: 200,
		timeout: 10 * 1000,
	});

	logger.debug(`Saved snapshot: ${snapshot}`);
	return snapshot;
};

export const load = async (
	hash: string,
	blockstore: BlockStore
): Promise<Snapshot> => {
	const block = await blockstore.get<Uint8Array>(hash);
	if (!block) {
		throw new Error("Missing snapshot for CID: " + hash);
	}
	return deserialize(await getBlockValue<Uint8Array>(block), Snapshot);
};

export const loadFromCache = async (
	path: string,
	blockstore: BlockStore,
	cache: LocalStore
) => {
	const snapshotOrCID = await cache.get(path);
	if (!snapshotOrCID) {
		throw new Error("Missing snapshot CID from local store");
	}
	const reader = new BinaryReader(snapshotOrCID);
	const snapshotCIDString = reader.string();
	return load(snapshotCIDString, blockstore);
};
