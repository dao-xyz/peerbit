export interface CPUUsage {
	start?(): void;
	stop?(): void;
	value: () => number;
}

/**
 * Calculate the CPU load by the timing of setinterval
 * using a sliding mean and mapping to a 0,1 by naively setting a upper bound
 * on how much lag can occur at 100% CPU usage (or thottling behaviour in an inactive browser tab...)
 */

export class CPUUsageIntervalLag implements CPUUsage {
	dt: number[];
	interval: ReturnType<typeof setInterval>;
	sum: number;
	constructor(
		readonly properties: {
			windowSize: number;
			intervalTime: number;
			upperBoundLag: number;
		} = { windowSize: 50, intervalTime: 100, upperBoundLag: 1000 }
	) {}
	private mean() {
		return this.sum / this.dt.length;
	}

	value() {
		return (
			Math.min(
				Math.max(this.mean() - this.properties.intervalTime, 0),
				this.properties.upperBoundLag
			) / this.properties.upperBoundLag
		); // 1 if lagging more than MAX_INTERVAL seconds
	}

	start() {
		this.dt = new Array<number>(this.properties.windowSize).fill(
			this.properties.intervalTime
		);
		this.sum = this.properties.windowSize * this.properties.intervalTime;

		let ts = +new Date();
		this.interval = setInterval(() => {
			const now = +new Date();
			this.sum -= this.dt.shift()!;
			const dt = now - ts;
			this.sum += dt;
			this.dt.push(dt);
			ts = now;
		}, this.properties.intervalTime);
	}

	stop() {
		clearInterval(this.interval);
	}
}
