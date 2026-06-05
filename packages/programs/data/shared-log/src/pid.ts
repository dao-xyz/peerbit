const MIN_MEMORY_HEADROOM_BALANCE_SCALER = 0.25;
const MEMORY_TARGET_UTILIZATION = 0.95;
const MEMORY_UNDERFILLED_UTILIZATION = 0.65;
const MEMORY_OVERFILLED_UTILIZATION = 1.12;

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
		const memoryLimit = this.maxMemoryLimit;
		const hasMemoryLimit = memoryLimit != null;
		const hasPositiveMemoryLimit = memoryLimit != null && memoryLimit > 0;

		if (memoryLimit != null) {
			// Treat the configured storage limit as a ceiling, not the exact control
			// target. A small reserve prevents discrete entry sizes and delayed checked
			// prunes from repeatedly settling just above the hard budget.
			const effectiveMemoryLimit =
				memoryLimit > 0 ? memoryLimit * MEMORY_TARGET_UTILIZATION : 0;
			if (effectiveMemoryLimit <= 0) {
				errorMemory = -currentFactor;
			} else if (currentFactor > 0 && memoryUsage > 0) {
				errorMemory =
					Math.max(Math.min(1, effectiveMemoryLimit / estimatedTotalSize), 0) -
					currentFactor;
			} else {
				// A memory-limited peer can shrink to zero width, or start empty, while
				// still having storage headroom. Without a positive memory error, the
				// balance term is disabled for constrained peers and the peer can get
				// stuck underfilled forever. If the zero-width peer is already over the
				// limit, keep memory neutral instead of negative so coverage repair can
				// expand from zero when the ring is under-covered.
				errorMemory = Math.max(
					Math.min(1, (effectiveMemoryLimit - memoryUsage) / effectiveMemoryLimit),
					0,
				);
			}
			// Math.max(Math.min((this.maxMemoryLimit - memoryUsage) / 100e5, 1), -1)// Math.min(Math.max((this.maxMemoryLimit - memoryUsage, 0) / 10e5, 0), 1);
		}

		const errorCoverageUnmodified = Math.min(1 - totalFactor, 1);
		const coverageDeficit = Math.max(0, errorCoverageUnmodified);
		const hasMemoryHeadroom =
			hasPositiveMemoryLimit && errorMemory > 0;
		let errorCoverage =
			(hasMemoryLimit
				? hasPositiveMemoryLimit
					? 1 - Math.sqrt(Math.abs(errorMemory))
					: 0
				: 1) *
			errorCoverageUnmodified;
		if (hasMemoryHeadroom && coverageDeficit > 0) {
			// For unequal storage budgets, the larger peer has to grow past an even
			// share when smaller constrained peers shed coverage. The coverage term is
			// otherwise weakest exactly when memory has the most headroom, which can
			// leave the ring underfilled under timer/load pressure.
			errorCoverage = Math.max(
				errorCoverage,
				errorMemory * Math.min(1, coverageDeficit / 0.25),
			);
		}

		const errorFromEven = 1 / peerCount - currentFactor;
		// When the network is under-covered (`totalFactor < 1`) balancing "down" (negative
		// error) can further reduce coverage and force constrained peers (memory/CPU limited)
		// to take boundary assignments that exceed their budgets.
		//
		// Use a soft clamp: only suppress negative balance strongly when the coverage deficit
		// is material. This avoids oscillations around `totalFactor ~= 1`.
		const negativeBalanceScale =
			coverageDeficit <= 0 ? 1 : 1 - Math.min(1, coverageDeficit / 0.1); // full clamp at 10% deficit
		let errorFromEvenForBalance =
			errorFromEven >= 0 ? errorFromEven : errorFromEven * negativeBalanceScale;
		if (hasMemoryHeadroom && coverageDeficit > 0 && errorFromEvenForBalance < 0) {
			errorFromEvenForBalance = 0;
		}

		if (hasMemoryHeadroom && errorFromEvenForBalance > 0) {
			// Coverage surplus often means another peer has not pruned yet. Do not let
			// that transient surplus cancel a constrained peer that is still below an
			// even share and has storage headroom to take more work.
			errorCoverage = Math.max(errorCoverage, 0);
		}

		const balanceErrorScaler = hasMemoryLimit
			? hasMemoryHeadroom
				? Math.max(
						Math.abs(errorMemory),
						MIN_MEMORY_HEADROOM_BALANCE_SCALER,
					)
				: Math.abs(errorMemory)
			: 1 - Math.abs(errorCoverage);

		// Balance should be symmetric (allow negative error) so a peer can *reduce*
		// participation when peerCount increases. Otherwise early joiners can get
		// "stuck" over-replicating even after new peers join (no memory/CPU limits).
		const errorBalance = hasMemoryLimit
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
		let errorMemoryFactor = 0.9;
		const errorBalanceFactor = 0.6;

		totalError =
			errorBalance * errorBalanceFactor +
			errorCoverage * (1 - errorBalanceFactor);

		// Computer is getting too full?
		if (errorMemory < 0) {
			if (
				this.maxMemoryLimit != null &&
				this.maxMemoryLimit > 0 &&
				coverageDeficit > 0
			) {
				// When the ring is materially under-covered, shrinking a memory-limited
				// range can increase gap-boundary assignments and make local memory usage
				// worse, not better. Let the coverage term dominate until the floor is
				// restored, while preserving the hard shrink behavior for zero-capacity peers.
				errorMemoryFactor = Math.max(
					0.2,
					errorMemoryFactor - 0.7 * Math.min(1, coverageDeficit / 0.25),
				);
			}
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
		let newFactor = currentFactor + change;

		if (
			hasPositiveMemoryLimit &&
			currentFactor > 0 &&
			memoryUsage > 0 &&
			memoryLimit != null
		) {
			const targetMemoryFactor = Math.max(
				Math.min(1, (memoryLimit * MEMORY_TARGET_UTILIZATION) / estimatedTotalSize),
				0,
			);
			const memoryUtilization = memoryUsage / memoryLimit;
			if (
				(memoryUtilization < MEMORY_UNDERFILLED_UTILIZATION &&
					currentFactor < 1 / peerCount &&
					newFactor < targetMemoryFactor) ||
				(memoryUtilization > MEMORY_OVERFILLED_UTILIZATION &&
					newFactor > targetMemoryFactor)
			) {
				newFactor = targetMemoryFactor;
				this.integral = 0;
			}
		}

		if (this.maxCPUUsage != null && this.maxMemoryLimit == null) {
			// CPU pressure may shed surplus replicas, but it must not create a
			// coverage gap where the network no longer has one full copy.
			const coverageSurplus = Math.max(0, totalFactor - 1);
			if (newFactor < currentFactor) {
				newFactor =
					coverageSurplus <= 0
						? currentFactor
						: Math.max(newFactor, currentFactor - coverageSurplus);
			}
		}

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
