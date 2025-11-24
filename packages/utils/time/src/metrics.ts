import { hrtime } from "./hrtime.js";

export class MovingAverageTracker {
	private lastTS: bigint;

	value = 0;

	constructor(readonly tau = 10) {
		this.lastTS = hrtime.bigint();
	}

	add(number: number): void {
		const now = hrtime.bigint();
		let diff = Number(now - this.lastTS);
		if (diff <= 0) {
			diff = 1; // prevent Math.exp below become NaN
		}
		const dt = diff / 1e9;
		this.lastTS = now;
		const alphaT = 1 - Math.exp(-dt / this.tau);
		this.value = (1 - alphaT) * this.value + (alphaT * number) / dt;
	}
}
