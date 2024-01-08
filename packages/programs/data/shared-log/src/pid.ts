export type ReplicationErrorFunction = (objectives: {
	coverage: number;
	balance: number;
	memory: number;
}) => number;

export class PIDReplicationController {
	integral: number;
	prevError: number;
	prevMemoryUsage: number;
	lastTs: number;
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
			kp = 0.5,
			ki = 0.1,
			kd = 0.25,
			errorFunction = ({ balance, coverage, memory }) =>
				memory * 0.8 + balance * 0.1 + coverage * 0.1
		} = options;
		this.reset();
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

		const estimatedTotalSize = memoryUsage / currentFactor;

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

		const errorCoverageUnmodified = Math.min(1 - totalFactor, 1);
		const includeCoverageError =
			Math.max(Math.abs(errorTarget), Math.abs(errorMemory)) < 0.01;
		const errorCoverage = includeCoverageError ? errorCoverageUnmodified : 0; /// 1 / (Math.max(Math.abs(errorTarget), Math.abs(errorMemory))) * errorCoverage / 100;

		let totalError = this.errorFunction({
			balance: errorTarget,
			coverage: errorCoverage,
			memory: errorMemory
		});

		if (totalError === 0 && !includeCoverageError) {
			totalError = this.errorFunction({
				balance: errorTarget,
				coverage: errorCoverageUnmodified,
				memory: errorMemory
			});
		}

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
		const beta = 0.5;
		this.integral = beta * totalError + (1 - beta) * this.integral;

		const iTerm = this.ki * this.integral;

		// Derivative term
		const derivative = totalError - this.prevError;
		const dTerm = this.kd * derivative;

		// Calculate the new replication factor
		const change = pTerm + iTerm + dTerm;
		const newFactor = currentFactor + change;

		// Update state for the next iteration
		this.prevError = totalError;

		if (newFactor < 0 || newFactor > 1) {
			this.integral = 0;
		}

		/* console.log({
			newFactor,
			currentFactor,
			pTerm,
			dTerm,
			iTerm,
			kpAdjusted,
			totalError,
			errorTarget,
			errorCoverage,
			errorMemory,
			peerCount,
			totalFactor
		}); */

		return Math.max(Math.min(newFactor, 1), 0);
	}

	reset() {
		this.prevError = 0;
		this.integral = 0;
		this.prevMemoryUsage = 0;
		this.lastTs = 0;
	}
}
