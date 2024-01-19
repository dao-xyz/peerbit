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
	ts: number[];
	interval: ReturnType<typeof setInterval>;
	constructor(
		readonly properties: {
			windowSize: number;
			intervalTime: number;
			upperBoundLag: number;
		} = { windowSize: 10, intervalTime: 100, upperBoundLag: 1000 }
	) {}
	private mean() {
		let sum = 0;
		for (const t of this.ts) {
			sum += t;
		}
		return sum / this.ts.length;
	}

	value() {
		this.ts = new Array<number>(this.properties.windowSize).fill(
			this.properties.intervalTime
		);
		return (
			Math.min(
				Math.max(this.mean() - this.properties.intervalTime, 0),
				this.properties.upperBoundLag
			) / this.properties.upperBoundLag
		); // 1 if lagging more than MAX_INTERVAL seconds
	}

	start() {
		this.ts = [];
		let ts = +new Date();
		this.interval = setInterval(() => {
			const now = +new Date();
			this.ts.push(now - ts);
			this.ts.slice(0, this.properties.windowSize);
			ts = now;
		}, this.properties.intervalTime);
	}

	stop() {
		clearInterval(this.interval);
	}
}
