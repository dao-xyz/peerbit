// Fifo
import yallist from "yallist";

export class Cache<T = undefined> {
	private map: Map<string, { value?: T | null; time: number }>;
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
		return this.map.has(key);
	}

	get(key: string): T | null | undefined {
		this.trim();
		if (this.deleted.has(key)) {
			return undefined;
		}
		return this.map.get(key)?.value;
	}

	trim(time = +new Date()) {
		const peek = this.list.head;
		let outOfDate =
			peek &&
			this.ttl !== undefined &&
			this.map.get(peek.value)!.time < time - this.ttl;
		while (outOfDate || this.list.length > this.max) {
			const key = this.list.shift();
			if (key !== undefined) {
				outOfDate =
					this.ttl !== undefined && this.map.get(key)!.time < time - this.ttl;
				this.map.delete(key);
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
		if (!this.map.has(key)) {
			this.list.push(key);
		}
		this.map.set(key, { time, value: value ?? null });
		this.deleted.delete(key);
	}
	clear() {
		this.list = yallist.create();
		this.map = new Map();
		this.deleted = new Set();
	}

	get size() {
		return this.list.length - this.deleted.size;
	}
}
