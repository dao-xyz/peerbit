import type { Blocks } from "@peerbit/blocks-interface";
import type { Identity, SignatureWithKey } from "@peerbit/crypto";
import type { LamportClock as Clock } from "./clock.js";
import type { Encoding } from "./encoding.js";
import type { EntryType } from "./entry-type.js";
import { type EntryEncryption, EntryV0 } from "./entry-v0.js";
import { EntryV1 } from "./entry-v1.js";
import type { CanAppend, Entry } from "./entry.js";
import type { SortableEntry } from "./log-sorting.js";

export const createEntry = async <T>(properties: {
	store: Blocks;
	data: T;
	meta?: {
		clock?: Clock;
		gid?: string;
		type?: EntryType;
		gidSeed?: Uint8Array;
		data?: Uint8Array;
		next?: SortableEntry[];
	};
	encoding?: Encoding<T>;
	canAppend?: CanAppend<T>;
	encryption?: EntryEncryption;
	identity: Identity;
	signers?: ((
		data: Uint8Array,
	) => Promise<SignatureWithKey> | SignatureWithKey)[];
}): Promise<Entry<T>> => {
	if (properties.encryption) {
		return EntryV0.create(properties);
	}
	return EntryV1.create(properties);
};
