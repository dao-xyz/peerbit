// Fifo
import yallist from "yallist";

export type CacheData<T> = { value?: T | null; time: number; size: number };
type Key = string | bigint | number;
export class Cache<T = undefined> {
	private _map: Map<Key, CacheData<T>>;
	private deleted: Set<Key>;
	private list: yallist<Key>;
	currentSize: number;
	deletedSize: number;

	max: number;
	ttl?: number;
	constructor(options: { max: number; ttl?: number }) {
		if (options.max <= 0) {
			throw new Error("Expecting max >= 0");
		}
		this.max = options.max;
		this.ttl = options.ttl;
		this.clear();
	}
	has(key: Key) {
		this.trim();
		if (this.deleted.has(key)) {
			return false;
		}
		return this._map.has(key);
	}

	get map(): Map<Key, CacheData<T>> {
		return this._map;
	}

	get(key: Key): T | null | undefined {
		this.trim();
		if (this.deleted.has(key)) {
			return undefined;
		}
		return this._map.get(key)?.value;
	}

	trim(time = +new Date()) {
		for (;;) {
			const headKey = this.list.head;
			if (headKey?.value !== undefined) {
				const cacheValue = this._map.get(headKey.value)!;
				const outOfDate =
					this.ttl !== undefined && cacheValue.time < time - this.ttl;
				if (outOfDate || this.currentSize > this.max) {
					this.list.shift();
					this._map.delete(headKey.value);
					const wasDeleted = this.deleted.delete(headKey.value);
					if (!wasDeleted) {
						this.currentSize -= cacheValue.size;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}

	del(key: Key) {
		const cacheValue = this._map.get(key)!;
		if (cacheValue && !this.deleted.has(key)) {
			this.deleted.add(key);
			this.currentSize -= cacheValue.size;
			return cacheValue;
		}
		return undefined;
	}

	add(key: Key, value?: T, size = 1) {
		this.deleted.delete(key);
		const time = +new Date();
		if (!this._map.has(key)) {
			this.list.push(key);
			this.currentSize += size;
		}
		this._map.set(key, { time, value: value ?? null, size });
		this.trim(time);
	}
	clear() {
		this.list = yallist.create();
		this._map = new Map();
		this.deleted = new Set();
		this.currentSize = 0;
	}

	get size() {
		return this.currentSize;
	}
}
