import { MovingAverageTracker } from "@peerbit/time";

export class BandwidthTracker {
	private ma: MovingAverageTracker;
	private interval: ReturnType<typeof setInterval>;
	constructor(tau = 0.2) {
		this.ma = new MovingAverageTracker(tau);
	}
	start() {
		// Add 0 every second to make the tracker go to 0 over time
		this.interval = setInterval(() => {
			this.ma.add(0);
		}, 1000);
	}
	get value() {
		return this.ma.value;
	}
	add(number: number) {
		this.ma.add(number);
	}
	stop() {
		clearInterval(this.interval);
	}
}
