import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	type Blocks,
	type GetOptions,
	calculateRawCid,
	cidifyString,
} from "@peerbit/blocks-interface";
import type { PublicSignKey, SignatureWithKey } from "@peerbit/crypto";
import type { CryptoKeychain } from "@peerbit/keychain";
import { LamportClock as Clock } from "./clock.js";
import type { Encoding } from "./encoding.js";
import type { ShallowEntry } from "./entry-shallow.js";
import type { EntryType } from "./entry-type.js";
import type { Payload } from "./payload.js";

export type CanAppend<T> = (canAppend: Entry<T>) => Promise<boolean> | boolean;
export type ShallowOrFullEntry<T> = ShallowEntry | Entry<T>;
export type PreparedEntryBlock = Awaited<ReturnType<typeof calculateRawCid>>;
export type PreparedAppendFacts = {
	hash: string;
	gid: string;
	next: string[];
	wallTime: bigint;
	logical: number;
	clockId?: Uint8Array;
	type?: EntryType;
	metaData?: Uint8Array;
	payloadSize: number;
	metaBytes?: Uint8Array;
	hashDigestBytes?: Uint8Array;
};
export type PreparedNativeLogEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	head?: boolean;
	payloadSize?: number;
	data?: Uint8Array;
	clock: {
		timestamp: {
			wallTime: bigint | number | string;
			logical?: number;
		};
	};
};
export type PreparedAppendChain<T> = {
	entries: Entry<T>[];
	blocks?: PreparedEntryBlock[];
	shallowEntries: ShallowEntry[];
	appendFacts?: PreparedAppendFacts[];
	nativeEntries?: PreparedNativeLogEntry[];
	nativeGraphUpdated?: boolean;
	nativeBlocksCommitted?: boolean;
};
export type PreparedAppendCommitOnlyChain<T> = {
	materializeEntry: (index?: number) => Entry<T>;
	materializeEntries: () => Entry<T>[];
	blocks?: PreparedEntryBlock[];
	shallowEntries: ShallowEntry[];
	appendFacts: PreparedAppendFacts[];
	nativeEntries?: PreparedNativeLogEntry[];
	trimmedNativeEntries?: PreparedNativeLogEntry[];
	trimmedNativeEntryHashes?: string[];
	trimmedNativeBlocksDeleted?: boolean;
	nativeGraphUpdated?: boolean;
	nativeBlocksCommitted?: boolean;
};

const preparedEntryBlocks = new WeakMap<object, PreparedEntryBlock>();
const preparedShallowEntries = new WeakMap<object, ShallowEntry>();
const preparedNativeLogEntries = new WeakMap<object, PreparedNativeLogEntry>();

const preparedEntryBlockFromBytes = (
	bytes: Uint8Array,
	cid: string,
): PreparedEntryBlock => {
	let cidObject: ReturnType<typeof cidifyString> | undefined;
	return {
		block: {
			bytes,
			get cid() {
				cidObject ??= cidifyString(cid);
				return cidObject;
			},
			value: bytes,
		} as PreparedEntryBlock["block"],
		cid,
	};
};

const preparedEntryBlockFromBytesSource = (
	bytesSource: () => Uint8Array,
	cid: string,
): PreparedEntryBlock => {
	let cidObject: ReturnType<typeof cidifyString> | undefined;
	let bytesValue: Uint8Array | undefined;
	return {
		block: {
			get bytes() {
				return (bytesValue ??= bytesSource());
			},
			get cid() {
				cidObject ??= cidifyString(cid);
				return cidObject;
			},
			get value() {
				return (bytesValue ??= bytesSource());
			},
		} as PreparedEntryBlock["block"],
		cid,
	};
};

interface Meta {
	clock: Clock;
	gid: string; // graph id
	next: string[];
	type: EntryType;
	data?: Uint8Array;
}

export interface Entry<T> {
	meta: Meta;
	payload: Payload<T>;
	signatures: SignatureWithKey[];
	hash: string;
	size: number;
	createdLocally?: boolean;
	publicKeys: PublicSignKey[];
	toShallow(isHead: boolean): ShallowEntry;
}

export abstract class Entry<T> {
	abstract init(
		props:
			| {
					keychain?: CryptoKeychain;
					encoding: Encoding<T>;
			  }
			| Entry<T>,
	): this;

	abstract getMeta(): Promise<Meta> | Meta;
	abstract getNext(): Promise<string[]> | string[];
	abstract verifySignatures(): Promise<boolean> | boolean;
	abstract getSignatures(): Promise<SignatureWithKey[]> | SignatureWithKey[];
	abstract getClock(): Promise<Clock> | Clock;
	abstract equals(other: Entry<T>): boolean;
	abstract getPayloadValue(): Promise<T> | T;
	abstract toSignable(): Entry<T>;

	getSignableBytes(): Uint8Array {
		return serialize(this.toSignable());
	}

	getStorageBytes(): Uint8Array {
		return serialize(this);
	}

	/**
	 * Return a fully-materialized, read-oriented version of this entry: an
	 * instance whose `meta`/`payload`/`signatures` fields are populated (i.e. a
	 * concrete {@link EntryV0}), suitable for being cached and read many times.
	 *
	 * Concrete entries ({@link EntryV0}) are already fully materialized and
	 * return `this` at zero cost. Lazy wrapper entries (e.g. the native shared
	 * log's stash-backed head wrapper, which keeps the block bytes in wasm and
	 * only exposes generic getters) override this to decode themselves into a
	 * full entry so that consumers reading via field access or an
	 * `instanceof EntryV0` gated comparison (see {@link EntryV0.equals}) see the
	 * canonical entry rather than the hollow wrapper.
	 *
	 * This must only be called at a read boundary (where a consumer actually
	 * needs the entry's contents), never on the wire/sync fusion path — a lazy
	 * wrapper materializing here copies its block bytes out of native memory.
	 */
	toMaterialized(): Entry<T> {
		return this;
	}

	async getPublicKeys(): Promise<PublicSignKey[]> {
		const signatures = await this.getSignatures();
		return signatures.map((s) => s.publicKey);
	}

	/**
	 * Compares two entries.
	 * @param {Entry} a
	 * @param {Entry} b
	 * @returns {number} 1 if a is greater, -1 is b is greater
	 */
	static compare<T>(a: Entry<T>, b: Entry<T>) {
		const aClock = a.meta.clock;
		const bClock = b.meta.clock;
		const distance = Clock.compare(aClock, bClock);
		if (distance === 0) return aClock.id < bClock.id ? -1 : 1;
		return distance;
	}

	static toMultihash<T>(
		store: Blocks,
		entry: Entry<T>,
	): Promise<string> | string {
		if (entry.hash) {
			throw new Error("Expected hash to be missing");
		}

		const bytes = entry.getStorageBytes();
		entry.size = bytes.length;
		return store.put(bytes);
	}

	static async prepareMultihash<T>(entry: Entry<T>): Promise<string> {
		if (entry.hash) {
			throw new Error("Expected hash to be missing");
		}

		const bytes = entry.getStorageBytes();
		entry.size = bytes.length;
		const prepared = await calculateRawCid(bytes);
		preparedEntryBlocks.set(entry, prepared);
		return prepared.cid;
	}

	static prepareMultihashBytes<T>(
		entry: Entry<T>,
		bytes: Uint8Array,
		cid: string,
	): string {
		if (entry.hash) {
			throw new Error("Expected hash to be missing");
		}

		entry.size = bytes.length;
		preparedEntryBlocks.set(entry, preparedEntryBlockFromBytes(bytes, cid));
		return cid;
	}

	/**
	 * Like {@link Entry.prepareMultihashBytes} but defers pulling the block
	 * bytes until a consumer actually reads them (block store put, storage
	 * bytes). Callers that keep entry bytes in an external store (e.g. the
	 * native wire stash) use this so entries handed to change consumers do
	 * not copy bytes they may never touch.
	 */
	static prepareMultihashBytesLazy<T>(
		entry: Entry<T>,
		cid: string,
		size: number,
		bytesSource: () => Uint8Array,
	): string {
		if (entry.hash) {
			throw new Error("Expected hash to be missing");
		}

		entry.size = size;
		preparedEntryBlocks.set(
			entry,
			preparedEntryBlockFromBytesSource(bytesSource, cid),
		);
		return cid;
	}

	static preparedBlockFromBytes(bytes: Uint8Array, cid: string): PreparedEntryBlock {
		return preparedEntryBlockFromBytes(bytes, cid);
	}

	static toMultihashBytes<T>(
		store: Blocks,
		entry: Entry<T>,
		bytes: Uint8Array,
		cid: string,
	): Promise<string> | string {
		if (entry.hash) {
			throw new Error("Expected hash to be missing");
		}

		entry.size = bytes.length;
		return store.put(preparedEntryBlockFromBytes(bytes, cid));
	}

	static takePreparedBlock<T>(entry: Entry<T>): PreparedEntryBlock | undefined {
		const prepared = preparedEntryBlocks.get(entry);
		if (prepared) {
			preparedEntryBlocks.delete(entry);
		}
		return prepared;
	}

	static hasPreparedBlock<T>(entry: Entry<T>): boolean {
		return preparedEntryBlocks.has(entry);
	}

	static getPreparedStorageBytes<T>(entry: Entry<T>): Uint8Array | undefined {
		const prepared = preparedEntryBlocks.get(entry);
		const block = prepared?.block as
			| { bytes?: Uint8Array; value?: Uint8Array }
			| undefined;
		return block?.bytes ?? block?.value;
	}

	static prepareShallowEntry<T>(entry: Entry<T>, shallow: ShallowEntry): void {
		preparedShallowEntries.set(entry, shallow);
	}

	static hasPreparedShallowEntry<T>(entry: Entry<T>): boolean {
		return preparedShallowEntries.has(entry);
	}

	static takePreparedShallowEntry<T>(
		entry: Entry<T>,
		isHead: boolean,
	): ShallowEntry | undefined {
		const prepared = preparedShallowEntries.get(entry);
		if (prepared) {
			preparedShallowEntries.delete(entry);
			prepared.head = isHead;
		}
		return prepared;
	}

	static prepareNativeLogEntry<T>(
		entry: Entry<T>,
		nativeEntry: PreparedNativeLogEntry,
	): void {
		preparedNativeLogEntries.set(entry, nativeEntry);
	}

	static hasPreparedNativeLogEntry<T>(entry: Entry<T>): boolean {
		return preparedNativeLogEntries.has(entry);
	}

	static takePreparedNativeLogEntry<T>(
		entry: Entry<T>,
		isHead: boolean,
	): PreparedNativeLogEntry | undefined {
		const prepared = preparedNativeLogEntries.get(entry);
		if (prepared) {
			preparedNativeLogEntries.delete(entry);
			prepared.head = isHead;
		}
		return prepared;
	}

	static fromMultihash = async <T>(
		store: Blocks,
		hash: string,
		options?: GetOptions,
	) => {
		if (!hash) {
			throw new Error(`Invalid hash: ${hash}`);
		}
		const bytes = await store.get(hash, options);
		if (!bytes) {
			throw new Error("Failed to resolve block: " + hash);
		}
		const entry = deserialize(bytes, Entry);
		Entry.prepareMultihashBytes(entry, bytes, hash);
		entry.hash = hash;
		entry.size = bytes.length;
		return entry as Entry<T>;
	};

	/**
	 * Check if an entry equals another entry.
	 * @param {Entry} a
	 * @param {Entry} b
	 * @returns {boolean}
	 */
	static isEqual<T>(a: Entry<T>, b: Entry<T>) {
		return a.hash === b.hash;
	}

	/**
	 * Check if an entry is a parent to another entry.
	 * @param {Entry} entry1 Entry to check
	 * @param {Entry} entry2 The parent Entry
	 * @returns {boolean}
	 */
	static isDirectParent<T>(entry1: Entry<T>, entry2: Entry<T>) {
		return entry2.meta.next.includes(entry1.hash as any); // TODO fix types
	}

	/**
	 * Find entry's children from an Array of entries.
	 * Returns entry's children as an Array up to the last know child.
	 * @param {Entry} entry Entry for which to find the parents
	 * @param {Array<Entry<T>>} values Entries to search parents from
	 * @returns {Array<Entry<T>>}
	 */
	static findDirectChildren<T>(
		entry: Entry<T>,
		values: Entry<T>[],
	): Entry<T>[] {
		let stack: Entry<T>[] = [];
		let parent = values.find((e) => Entry.isDirectParent(entry, e));
		let prev = entry;
		while (parent) {
			stack.push(parent);
			prev = parent;
			parent = values.find((e) => Entry.isDirectParent(prev, e));
		}
		stack = stack.sort((a, b) => Clock.compare(a.meta.clock, b.meta.clock));
		return stack;
	}
}
