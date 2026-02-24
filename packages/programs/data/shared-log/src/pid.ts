export class PIDReplicationController {
	integral!: number;
	prevError!: number;
	prevMemoryUsage!: number;
	prevTotalFactor!: number;
	kp: number;
	ki: number;
	kd: number;
	maxMemoryLimit?: number;
	maxCPUUsage?: number;
	constructor(
		readonly id: string,
		options: {
			storage?: { max: number };
			cpu?: { max: number };
			kp?: number;
			ki?: number;
			kd?: number;
		} = {},
	) {
		const { storage: memory, cpu, kp = 0.7, ki = 0.025, kd = 0.05 } = options;
		this.kp = kp;
		this.ki = ki;
		this.kd = kd;
		this.maxMemoryLimit = memory?.max;
		this.maxCPUUsage = cpu?.max;
		this.reset();
	}

	/**
	 * Call this function on a period interval since it does not track time passed
	 */
	step(properties: {
		memoryUsage: number;
		currentFactor: number;
		totalFactor: number;
		peerCount: number;
		cpuUsage: number | undefined;
	}) {
		let { memoryUsage, totalFactor, peerCount, cpuUsage, currentFactor } =
			properties;

		this.prevTotalFactor = totalFactor;
		this.prevMemoryUsage = memoryUsage;

		const estimatedTotalSize =
			currentFactor > 0 ? memoryUsage / currentFactor : 1e5;

		let errorMemory = 0;

		if (this.maxMemoryLimit != null) {
			errorMemory =
				currentFactor > 0 && memoryUsage > 0
					? Math.max(Math.min(1, this.maxMemoryLimit / estimatedTotalSize), 0) -
						currentFactor
					: 0;
			// Math.max(Math.min((this.maxMemoryLimit - memoryUsage) / 100e5, 1), -1)// Math.min(Math.max((this.maxMemoryLimit - memoryUsage, 0) / 10e5, 0), 1);
		}

		const errorCoverageUnmodified = Math.min(1 - totalFactor, 1);
		const errorCoverage =
			(this.maxMemoryLimit ? 1 - Math.sqrt(Math.abs(errorMemory)) : 1) *
			errorCoverageUnmodified;

		const errorFromEven = 1 / peerCount - currentFactor;
		// When the network is under-covered (`totalFactor < 1`) balancing "down" (negative
		// error) can further reduce coverage and force constrained peers (memory/CPU limited)
		// to take boundary assignments that exceed their budgets.
		//
		// Use a soft clamp: only suppress negative balance strongly when the coverage deficit
		// is material. This avoids oscillations around `totalFactor ~= 1`.
		const coverageDeficit = Math.max(0, errorCoverageUnmodified); // ~= max(0, 1 - totalFactor)
		const negativeBalanceScale =
			coverageDeficit <= 0
				? 1
				: 1 - Math.min(1, coverageDeficit / 0.1); // full clamp at 10% deficit
		const errorFromEvenForBalance =
			errorFromEven >= 0 ? errorFromEven : errorFromEven * negativeBalanceScale;

		const balanceErrorScaler = this.maxMemoryLimit
			? Math.abs(errorMemory)
			: 1 - Math.abs(errorCoverage);

		// Balance should be symmetric (allow negative error) so a peer can *reduce*
		// participation when peerCount increases. Otherwise early joiners can get
		// "stuck" over-replicating even after new peers join (no memory/CPU limits).
		const errorBalance = this.maxMemoryLimit
			? // Only balance when we have spare memory headroom. When memory is
				// constrained (`errorMemory < 0`) the memory term will dominate anyway.
				errorMemory > 0
				? balanceErrorScaler * errorFromEvenForBalance
				: 0
			: balanceErrorScaler * errorFromEvenForBalance;

		const errorCPU = peerCount > 1 ? -1 * (cpuUsage || 0) : 0;

		// Calculate the total error
		// Hardcoded parameters are set by optimizing how fast the sharding tests pass
		// TODO make these self-optimizing

		let totalError: number;
		const errorMemoryFactor = 0.9;
		const errorBalanceFactor = 0.6;

		totalError =
			errorBalance * errorBalanceFactor +
			errorCoverage * (1 - errorBalanceFactor);

		// Computer is getting too full?
		if (errorMemory < 0) {
			totalError =
				errorMemory * errorMemoryFactor + totalError * (1 - errorMemoryFactor);
		}

		// Computer is getting too hot?
		if (this.maxCPUUsage != null && (cpuUsage || 0) > this.maxCPUUsage) {
			const errorCpuFactor = 0.5;
			totalError =
				totalError * (1 - errorCpuFactor) +
				errorCpuFactor * (errorCPU - this.maxCPUUsage!);
		}

		// Update p term
		const pTerm = this.kp * totalError;

		// Integral term
		this.integral += totalError;

		// Beta controls how much of the accumulated error we should forget
		const beta = 0.3;
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

		// reset integral term if we are "way" out of bounds
		// does not make sense having a lagging integral term if we
		// are out of bounds
		if (newFactor < 0 && this.integral < 0) {
			this.integral = 0;
		} else if (newFactor > 1 && this.integral > 0) {
			this.integral = 0;
		}

		/* if (this.id === "3YUU2tgXPB1v7NMdPob37WDcixg4vi7qF1PkbSJFNc4=")
			console.log({
				id: this.id,
				currentFactor,
				newFactor,
				factorDiff: newFactor - currentFactor,
				pTerm,
				dTerm,
				iTerm,
				totalError,
				errorFromEven,
				errorTarget: errorBalance,
				errorCoverage,
				errorMemory,
				errorCPU,
				peerCount,
				totalFactor,
				targetScaler: balanceErrorScaler,
				memoryUsage,
				estimatedTotalSize,
			}); */

		return Math.max(Math.min(newFactor, 1), 0);
	}

	reset() {
		this.prevError = 0;
		this.integral = 0;
		this.prevMemoryUsage = 0;
	}
}
