import { PublicSignKey } from "@peerbit/crypto";

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
