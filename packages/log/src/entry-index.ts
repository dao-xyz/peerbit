import { Entry } from "./entry.js";
export class EntryIndex<T> {
	_cache: Map<string, Entry<T>>;
	constructor(entries = new Map<string, Entry<T>>()) {
		this._cache = entries;
	}

	set(k: string, v: Entry<T>) {
		this._cache.set(k, v);
	}

	get(k: string): Entry<T> | undefined {
		return this._cache.get(k);
	}

	delete(k: string) {
		return this._cache.delete(k);
	}

	get length(): number {
		return this._cache.size;
	}
}
