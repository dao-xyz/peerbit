import type { Blocks } from "@peerbit/blocks-interface";
import type { Identity, SignatureWithKey } from "@peerbit/crypto";
import type { LamportClock as Clock } from "./clock.js";
import type { Encoding } from "./encoding.js";
import type { EntryType } from "./entry-type.js";
import { type EntryEncryption, EntryV0 } from "./entry-v0.js";
import type { CanAppend, Entry } from "./entry.js";
import type { SortableEntry } from "./log-sorting.js";

type Bytes = Uint8Array<ArrayBufferLike>;

// If T is any Uint8Array<…>, normalize to Bytes; otherwise leave T untouched
type NormalizeBytes<T> = T extends Uint8Array<any> ? Bytes : T;

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
}): Promise<Promise<Entry<NormalizeBytes<T>>>> => {
	return EntryV0.create(properties as any) as Promise<Entry<NormalizeBytes<T>>>;
};
