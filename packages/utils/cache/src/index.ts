// Fifo
import yallist from "yallist";

export class Cache<T = undefined> {
	map: Map<string, { value?: T | null; time: number }>;
	list: yallist<string>;
	max: number;
	ttl: number;
	constructor(options: { max: number; ttl: number }) {
		if (options.max <= 0) {
			throw new Error("Expecting max >= 0");
		}
		this.max = options.max;
		this.ttl = options.ttl;
		this.clear();
	}
	has(key: string) {
		this.trim();
		return this.map.has(key);
	}

	get(key: string): T | null | undefined {
		this.trim();
		return this.map.get(key)?.value;
	}

	trim(time = +new Date()) {
		const peek = this.list.head;
		let outOfDate = peek && this.map.get(peek.value)!.time < time - this.ttl;
		while (outOfDate || this.list.length > this.max) {
			const key = this.list.shift();
			if (key !== undefined) {
				outOfDate = this.map.get(key)!.time < time - this.ttl;
				this.map.delete(key);
			} else {
				break;
			}
		}
	}

	add(key: string, value?: T) {
		const time = +new Date();
		this.trim(time);
		if (!this.map.has(key)) {
			this.list.push(key);
		}
		this.map.set(key, { time, value: value ?? null });
	}
	clear() {
		this.list = yallist.create();
		this.map = new Map();
	}
}
