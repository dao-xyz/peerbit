import { AnyStore } from "./interface.js";

export class MemoryStore implements AnyStore {
	private store: Map<string, Uint8Array>;
	private sublevels: Map<string, MemoryStore>;
	private isOpen: boolean;
	constructor() {
		this.sublevels = new Map();
		this.store = new Map();
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
		for (const [_s, sub] of this.sublevels) {
			sub.clear();
		}
	}

	put(key: string, value: Uint8Array) {
		return this.store.set(key, value);
	}

	// Remove a value and key from the cache
	del(key: string) {
		this.store.delete(key);
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
		let size = 0;
		for (const [k, v] of this.store) {
			size += v.byteLength;
		}
		return size;
	}
}
