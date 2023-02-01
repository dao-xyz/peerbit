// Fifo
import yallist from "yallist";

export class Cache {
	map: Map<string, number>;
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

	trim(time = +new Date()) {
		const peek = this.list.head;
		let outOfDate = peek && this.map.get(peek.value)! < time - this.ttl;
		while (outOfDate || this.list.length > this.max) {
			const key = this.list.shift();
			if (key !== undefined) {
				outOfDate = this.map.get(key)! < time - this.ttl;
				this.map.delete(key);
			} else {
				break;
			}
		}
	}

	add(key: string) {
		const time = +new Date();
		this.trim(time);
		this.list.push(key);
		this.map.set(key, time);
	}
	clear() {
		this.list = yallist.create();
		this.map = new Map();
	}
}
