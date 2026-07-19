// Fifo
import { Node, Yallist } from "yallist";

export interface CacheData<T> {
	value?: T | null;
	time: number;
	size: number;
}
type Key = string | bigint | number;
export class Cache<T = undefined> {
	private _map: Map<Key, CacheData<T>>;
	private list: Yallist<Key>;
	private nodes: Map<Key, Node<Key>>;
	currentSize: number;

	max: number;
	ttl?: number;
	constructor(options: { max: number; ttl?: number }) {
		if (options.max <= 0) {
			throw new Error("Expecting max > 0");
		}
		this.max = options.max;
		this.ttl = options.ttl;
		this.clear();
	}

	has(key: Key): boolean {
		this.trim();
		return this._map.has(key);
	}

	get map(): Map<Key, CacheData<T>> {
		return this._map;
	}

	get(key: Key): T | null | undefined {
		this.trim();
		return this._map.get(key)?.value;
	}

	trim(time = Date.now()): void {
		for (;;) {
			const headKey = this.list.head;
			if (headKey?.value != null) {
				const cacheValue = this._map.get(headKey.value);
				if (!cacheValue) {
					throw new Error("Cache list/map invariant broken");
				}
				const outOfDate = this.ttl != null && cacheValue.time < time - this.ttl;
				if (outOfDate || this.currentSize > this.max) {
					this.list.shift();
					this._map.delete(headKey.value);
					this.nodes.delete(headKey.value);
					this.currentSize -= cacheValue.size;
				} else {
					break;
				}
			} else {
				break;
			}
		}
	}

	del(key: Key): CacheData<T> | undefined {
		const cacheValue = this._map.get(key);
		if (!cacheValue) return undefined;
		const node = this.nodes.get(key);
		if (!node) throw new Error("Cache node not found");
		this.list.removeNode(node);
		this.nodes.delete(key);
		this._map.delete(key);
		this.currentSize -= cacheValue.size;
		return cacheValue;
	}

	add(key: Key, value?: T, size = 1): void {
		const time = Date.now();
		const previous = this._map.get(key);
		if (!previous) {
			const node = new Node(key);
			this.list.pushNode(node);
			this.nodes.set(key, node);
			this.currentSize += size;
		} else {
			const node = this.nodes.get(key);
			if (!node) throw new Error("Cache node not found");
			this.list.removeNode(node);
			this.list.pushNode(node);
			this.currentSize += size - previous.size;
		}
		this._map.set(key, { time, value: value ?? null, size });
		this.trim(time);
	}

	addMany(
		entries: Iterable<readonly [key: Key, value?: T, size?: number]>,
	): void {
		const time = Date.now();
		let changed = false;
		for (const [key, value, size = 1] of entries) {
			const previous = this._map.get(key);
			if (!previous) {
				const node = new Node(key);
				this.list.pushNode(node);
				this.nodes.set(key, node);
				this.currentSize += size;
			} else {
				const node = this.nodes.get(key);
				if (!node) throw new Error("Cache node not found");
				this.list.removeNode(node);
				this.list.pushNode(node);
				this.currentSize += size - previous.size;
			}
			this._map.set(key, { time, value: value ?? null, size });
			changed = true;
		}
		if (changed) {
			this.trim(time);
		}
	}

	clear(): void {
		this.list = Yallist.create();
		this._map = new Map();
		this.nodes = new Map();
		this.currentSize = 0;
	}

	get size(): number {
		return this.currentSize;
	}
}
