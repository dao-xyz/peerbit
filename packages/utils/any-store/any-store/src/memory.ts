import { type AnyStore } from "@peerbit/any-store-interface";

export class MemoryStore implements AnyStore {
	private store: Map<string, Uint8Array>;
	private sublevels: Map<string, MemoryStore>;
	private isOpen: boolean;
	/**
	 * Logical byte lengths captured when each value is stored. MemoryStore keeps
	 * caller-owned Uint8Array references, whose live byteLength may later change
	 * if their backing buffer is resized or detached. Tracking those external
	 * mutations would require scanning all values, so size() reports the bytes
	 * credited at put time instead.
	 */
	private storedByteLengths: Map<string, number>;
	private storedBytes: number;
	constructor() {
		this.sublevels = new Map();
		this.store = new Map();
		this.storedByteLengths = new Map();
		this.storedBytes = 0;
	}

	status() {
		return this.isOpen ? "open" : "closed";
	}

	close() {
		this.isOpen = false;
		for (const level of this.sublevels) {
			level[1].close();
		}
	}

	open() {
		this.isOpen = true;
	}

	get(key: string): Uint8Array | undefined {
		return this.store.get(key);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for await (const [key, value] of this.store) {
			yield [key, value];
		}
	}

	clear(): void {
		this.store.clear();
		this.storedByteLengths.clear();
		this.storedBytes = 0;
		for (const [_s, sub] of this.sublevels) {
			sub.clear();
		}
	}

	put(key: string, value: Uint8Array) {
		// Read and validate before either map is mutated. Uint8Array subclasses can
		// override this getter, so it is part of the operation's failure boundary.
		const byteLength = value.byteLength;
		if (!Number.isSafeInteger(byteLength) || byteLength < 0) {
			throw new RangeError(
				"MemoryStore value byteLength must be a non-negative safe integer",
			);
		}
		const previousByteLength = this.storedByteLengths.get(key) ?? 0;
		this.store.set(key, value);
		this.storedByteLengths.set(key, byteLength);
		this.storedBytes += byteLength - previousByteLength;
	}

	// Remove a value and key from the cache
	del(key: string) {
		const previousByteLength = this.storedByteLengths.get(key);
		if (previousByteLength !== undefined && this.store.delete(key)) {
			this.storedByteLengths.delete(key);
			this.storedBytes -= previousByteLength;
		}
	}

	sublevel(name: string) {
		const existing = this.sublevels.get(name);
		if (existing) {
			return existing;
		}

		const sub = new MemoryStore();
		this.sublevels.set(name, sub);

		if (this.isOpen) {
			sub.open();
		}
		return sub;
	}

	size() {
		return this.storedBytes;
	}

	persisted() {
		return false;
	}
}
