export type ReplicationErrorFunction = (objectives: {
	coverage: number;
	balance: number;
	memory: number;
}) => number;

export class PIDReplicationController {
	integral = 0;
	prevError = 0;
	prevMemoryUsage = 0;
	lastTs = 0;
	kp: number;
	ki: number;
	kd: number;
	errorFunction: ReplicationErrorFunction;
	targetMemoryLimit?: number;
	constructor(
		options: {
			errorFunction?: ReplicationErrorFunction;
			targetMemoryLimit?: number;
			kp?: number;
			ki?: number;
			kd?: number;
		} = {}
	) {
		const {
			targetMemoryLimit,
			kp = 0.1,
			ki = 0 /* 0.01, */,
			kd = 0.1,
			errorFunction = ({ balance, coverage, memory }) =>
				memory * 0.8 + balance * 0.1 + coverage * 0.1
		} = options;
		this.kp = kp;
		this.ki = ki;
		this.kd = kd;
		this.targetMemoryLimit = targetMemoryLimit;
		this.errorFunction = errorFunction;
	}

	async adjustReplicationFactor(
		memoryUsage: number,
		currentFactor: number,
		totalFactor: number,
		peerCount: number
	) {
		this.prevMemoryUsage = memoryUsage;
		if (memoryUsage <= 0) {
			this.integral = 0;
		}

		const estimatedTotalSize = memoryUsage / currentFactor;
		const errorCoverage = Math.min(1 - totalFactor, 1);

		let errorMemory = 0;
		const errorTarget = 1 / peerCount - currentFactor;

		if (this.targetMemoryLimit != null) {
			errorMemory =
				currentFactor > 0 && memoryUsage > 0
					? Math.max(
							Math.min(1, this.targetMemoryLimit / estimatedTotalSize),
							0
						) - currentFactor
					: 0.0001;
		}

		const totalError = this.errorFunction({
			balance: errorTarget,
			coverage: errorCoverage,
			memory: errorMemory
		});

		if (this.lastTs === 0) {
			this.lastTs = +new Date();
		}
		const kpAdjusted = Math.min(
			Math.max(this.kp, (+new Date() - this.lastTs) / 100),
			0.8
		);
		const pTerm = kpAdjusted * totalError;

		this.lastTs = +new Date();

		// Integral term
		this.integral += totalError;
		const beta = 0.4;
		this.integral = beta * totalError + (1 - beta) * this.integral;

		const iTerm = this.ki * this.integral;

		// Derivative term
		const derivative = totalError - this.prevError;
		const dTerm = this.kd * derivative;

		// Calculate the new replication factor
		const newFactor = currentFactor + pTerm + iTerm + dTerm;

		// Update state for the next iteration
		this.prevError = totalError;

		return Math.max(Math.min(newFactor, 1), 0);
	}
}
