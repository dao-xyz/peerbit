import { PublicSignKey } from "@peerbit/crypto";
/* 
export class Frequency {

	private interval: ReturnType<typeof setInterval>;
	private lastFrequency: number;
	private events = 0;
	private intervalTime: number;
	constructor(properties: { intervalTime } = { intervalTime: 10 * 1000 }) {
		this.intervalTime = properties.intervalTime;
		this.interval = setInterval(() => {
			this.lastFrequency = this.events / this.intervalTime;
			this.events = 0;
		}, properties.intervalTime);
	}

	count(event: number) {
		this.events += event;
	}

	close() {
		clearInterval(this.interval);
		this.events = 0;
	}

	get frequency() {
		return this.lastFrequency;
	}
}

export class RouteFrequency {
	private interval: ReturnType<typeof setInterval>;
	private lastFrequency: Map<string, number>;
	private events: Map<string, number>;
	private intervalTime: number;
	constructor(properties: { intervalTime } = { intervalTime: 10 * 1000 }) {
		this.intervalTime = properties.intervalTime;
		this.events = new Map();
	}

	start() {
		this.interval = setInterval(() => {
			this.lastFrequency = this.events;
			this.events = new Map();
		}, this.intervalTime);
	}

	increment(to: string, bytes: Uint8Array) {
		let value = this.events.get(to);
		if (value == null) {
			value = 1;
			this.events.set(to, bytes.length);
		} else {
			this.events.set(to, value + bytes.length);
		}
	}

	close() {
		clearInterval(this.interval);
		this.events.clear();
	}

	getFrequency(to: PublicSignKey) {
		const count = this.lastFrequency.get(to.hashcode());
		if (count) {
			return count / this.intervalTime;
		}

		return undefined;
	}
}
 */

export class MovingAverageTracker {
	private lastTS = 0;

	value = 0;

	constructor(readonly tau = 10) {
		this.lastTS = +new Date();
	}
	add(number: number) {
		const now = +new Date();
		const dt = (now - this.lastTS) / 1000;
		const alpha_t = 1 - Math.exp(-dt / this.tau);
		this.value = (1 - alpha_t) * this.value + (alpha_t * number) / dt;
		this.lastTS = now;
	}
}
