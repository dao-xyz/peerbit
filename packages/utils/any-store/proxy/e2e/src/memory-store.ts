import type { AnyStore } from "@peerbit/any-store-interface";

export class MemoryStore implements AnyStore {
	private data = new Map<string, Uint8Array>();
	private sublevels = new Map<string, MemoryStore>();
	private opened = false;

	status() {
		return this.opened ? "open" : "closed";
	}

	open() {
		this.opened = true;
	}

	close() {
		this.opened = false;
	}

	get(key: string) {
		return this.data.get(key);
	}

	put(key: string, value: Uint8Array) {
		this.data.set(key, value);
	}

	del(key: string) {
		this.data.delete(key);
	}

	sublevel(name: string) {
		let sub = this.sublevels.get(name);
		if (!sub) {
			sub = new MemoryStore();
			if (this.opened) sub.open();
			this.sublevels.set(name, sub);
		}
		return sub;
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const entry of this.data.entries()) {
			yield entry;
		}
	}

	clear() {
		this.data.clear();
	}

	size() {
		let size = 0;
		for (const value of this.data.values()) {
			size += value.byteLength;
		}
		return size;
	}

	persisted() {
		return false;
	}
}
