// Fifo
import yallist from "yallist";

export type CacheData<T> = { value?: T | null; time: number; size: number };
export class Cache<T = undefined> {
	private _map: Map<string, CacheData<T>>;
	private deleted: Set<string>;
	private list: yallist<string>;
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
	has(key: string) {
		this.trim();
		if (this.deleted.has(key)) {
			return false;
		}
		return this._map.has(key);
	}

	get map(): Map<string, CacheData<T>> {
		return this._map;
	}

	get(key: string): T | null | undefined {
		this.trim();
		if (this.deleted.has(key)) {
			return undefined;
		}
		return this._map.get(key)?.value;
	}

	trim(time = +new Date()) {
		const peek = this.list.head;
		let outOfDate =
			peek &&
			this.ttl !== undefined &&
			this._map.get(peek.value)!.time < time - this.ttl;
		while (outOfDate || this.currentSize > this.max) {
			const key = this.list.shift();
			if (key !== undefined) {
				const cacheValue = this._map.get(key)!;
				outOfDate = this.ttl !== undefined && cacheValue.time < time - this.ttl;
				this._map.delete(key);
				const wasDeleted = this.deleted.delete(key);
				if (!wasDeleted) {
					this.currentSize -= cacheValue.size;
				}
			} else {
				break;
			}
		}
	}
	/* 

		trim(time = +new Date()) {
		const peek = this.list.head;
		let outOfDate =
			peek &&
			this.ttl !== undefined &&
			this._map.get(peek.value)!.time < time - this.ttl;
		while (outOfDate || this.currentSize > this.max) {
			const key = this.list.shift();
			if (key !== undefined) {
				const cacheValue = this.del(key);
				if (cacheValue) {
					outOfDate = this.ttl !== undefined && cacheValue.time < time - this.ttl;
					this._map.delete(key);
				}

			} else {
				break;
			}
		}
	}
	*/

	del(key: string) {
		const cacheValue = this._map.get(key)!;
		if (cacheValue && !this.deleted.has(key)) {
			this.deleted.add(key);
			this.currentSize -= cacheValue.size;
			return cacheValue;
		}
		return undefined;
	}

	add(key: string, value?: T, size = 1) {
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
