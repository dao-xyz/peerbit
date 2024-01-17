export type ReplicationErrorFunction = (objectives: {
	coverage: number;
	balance: number;
	memory: number;
}) => number;

export class PIDReplicationController {
	integral: number;
	prevError: number;
	prevMemoryUsage: number;
	prevTotalFactor: number;
	kp: number;
	ki: number;
	kd: number;
	errorFunction: ReplicationErrorFunction;
	targetMemoryLimit?: number;
	constructor(
		readonly id: string,
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
			kp = 0.7,
			ki = 0.025,
			kd = 0.05,
			errorFunction = ({ balance, coverage, memory }) => {
				return memory < 0
					? memory * 0.9 + balance * 0.07 + coverage * 0.03
					: balance * 0.6 + coverage * 0.4;
			}
		} = options;
		this.reset();
		this.kp = kp;
		this.ki = ki;
		this.kd = kd;
		this.targetMemoryLimit = targetMemoryLimit;
		this.errorFunction = errorFunction;
	}

	/**
	 * Call this function on a period interval since it does not track time passed
	 * @param memoryUsage
	 * @param currentFactor
	 * @param totalFactor
	 * @param peerCount
	 * @returns
	 */
	async adjustReplicationFactor(
		memoryUsage: number,
		currentFactor: number,
		totalFactor: number,
		peerCount: number
	) {
		const totalFactorDiff = totalFactor - this.prevTotalFactor;
		this.prevTotalFactor = totalFactor;
		this.prevMemoryUsage = memoryUsage;

		const estimatedTotalSize = memoryUsage / currentFactor;

		let errorMemory = 0;

		if (this.targetMemoryLimit != null) {
			errorMemory =
				currentFactor > 0 && memoryUsage > 0
					? Math.max(
							Math.min(1, this.targetMemoryLimit / estimatedTotalSize),
							0
						) - currentFactor
					: 0;
		}

		const errorCoverageUnmodified = Math.min(1 - totalFactor, 1);
		const errorCoverage =
			(this.targetMemoryLimit ? 1 - Math.sqrt(Math.abs(errorMemory)) : 1) *
			errorCoverageUnmodified;

		const errorFromEven = 1 / peerCount - currentFactor;

		const balanceErrorScaler = this.targetMemoryLimit
			? Math.abs(errorMemory)
			: 1 - Math.abs(errorCoverage);

		const errorBalance = (this.targetMemoryLimit ? errorMemory > -0.01 : true)
			? errorFromEven > 0
				? balanceErrorScaler * errorFromEven
				: 0
			: 0;

		const totalError = this.errorFunction({
			balance: errorBalance,
			coverage: errorCoverage,
			memory: errorMemory
		});

		const pTerm = this.kp * totalError;

		// Integral term
		this.integral += totalError;

		// Beta controls how much of the accumulated error we should forget
		const beta = 0.8;
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

		// prevent drift when everone wants to do less
		/* if (newFactor < currentFactor && totalFactorDiff < 0 && totalFactor < 0.5) {
			newFactor = currentFactor;
			this.integral = 0;
		}
			*/

		/* console.log({
			id: this.id,
			currentFactor,
			newFactor,
			factorDiff: newFactor - currentFactor,
			pTerm,
			dTerm,
			iTerm,
			totalError,
			errorTarget: errorBalance,
			errorCoverage,
			errorMemory,
			peerCount,
			totalFactor,
			totalFactorDiff,
			targetScaler: balanceErrorScaler,
			memoryUsage,
			estimatedTotalSize
		}); */

		return Math.max(Math.min(newFactor, 1), 0);
	}

	reset() {
		this.prevError = 0;
		this.integral = 0;
		this.prevMemoryUsage = 0;
	}
}
