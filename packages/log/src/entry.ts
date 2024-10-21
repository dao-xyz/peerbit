import { deserialize, serialize } from "@dao-xyz/borsh";
import { type Blocks } from "@peerbit/blocks-interface";
import type { PublicSignKey, SignatureWithKey } from "@peerbit/crypto";
import type { Keychain } from "@peerbit/keychain";
import { LamportClock as Clock } from "./clock.js";
import type { Encoding } from "./encoding.js";
import type { ShallowEntry } from "./entry-shallow.js";
import type { EntryType } from "./entry-type.js";
import type { Payload } from "./payload.js";

export type CanAppend<T> = (canAppend: Entry<T>) => Promise<boolean> | boolean;
export type ShallowOrFullEntry<T> = ShallowEntry | Entry<T>;

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
					keychain?: Keychain;
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

		const bytes = serialize(entry);
		entry.size = bytes.length;
		return store.put(bytes);
	}

	static fromMultihash = async <T>(
		store: Blocks,
		hash: string,
		options?: { remote?: { timeout?: number; replicate?: boolean } },
	) => {
		if (!hash) {
			throw new Error(`Invalid hash: ${hash}`);
		}
		const bytes = await store.get(hash, options);
		if (!bytes) {
			throw new Error("Failed to resolve block: " + hash);
		}
		const entry = deserialize(bytes, Entry);
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
