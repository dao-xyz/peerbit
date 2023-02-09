// Fifo
import yallist from "yallist";

export type CacheData<T> = { value?: T | null; time: number };
export class Cache<T = undefined> {
	private _map: Map<string, CacheData<T>>;
	private deleted: Set<string>;
	private list: yallist<string>;
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
		while (outOfDate || this.list.length > this.max) {
			const key = this.list.shift();
			if (key !== undefined) {
				outOfDate =
					this.ttl !== undefined && this._map.get(key)!.time < time - this.ttl;
				this._map.delete(key);
				this.deleted.delete(key);
			} else {
				break;
			}
		}
	}

	del(key: string) {
		this.deleted.add(key);
	}

	add(key: string, value?: T) {
		const time = +new Date();
		this.trim(time);
		if (!this._map.has(key)) {
			this.list.push(key);
		}
		this._map.set(key, { time, value: value ?? null });
		this.deleted.delete(key);
	}
	clear() {
		this.list = yallist.create();
		this._map = new Map();
		this.deleted = new Set();
	}

	get size() {
		return this.list.length - this.deleted.size;
	}
}
