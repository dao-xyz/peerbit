export const MIN_MEASURED_SAMPLES = 7;
export const MAX_MEASURED_SAMPLES = 1_000;
export const GATE_CONFIG = Object.freeze({
	peerCounts: Object.freeze([1, 8, 64]),
	warmupSamples: 3,
	measuredSamples: 15,
	iterations: 500,
});

const SCENARIOS = [
	"simple-dispatch-quota",
	"rateless-target-lifecycle",
	"disconnect-reconnect-retained-physical",
];
const SCENARIO_SET = new Set(SCENARIOS);
const RUNTIME_FIELDS = ["node", "v8", "platform", "arch", "cpu"];
// schemaVersion 1 fixes this workload to the production cap used by the A
// baseline. A cap or scenario change requires a new benchmark schema.
const RETAINED_PHYSICAL_PERMIT_CAP = 32;

const median = (values) => {
	const sorted = [...values].sort((left, right) => left - right);
	const middle = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? (sorted[middle - 1] + sorted[middle]) / 2
		: sorted[middle];
};

const binomialCoefficient = (n, k) => {
	let result = 1;
	for (let index = 1; index <= k; index += 1) {
		result = (result * (n - index + 1)) / index;
	}
	return result;
};

const binomialHalfCdf = (n, k) => {
	let sum = 0;
	for (let index = 0; index <= k; index += 1) {
		sum += binomialCoefficient(n, index);
	}
	return sum / 2 ** n;
};

// Distribution-free two-sided 95% confidence interval for the median, using
// order statistics and the exact Binomial(n, 0.5) tail. Seven samples is the
// minimum that produces a useful finite interval at this confidence level;
// 1,000 stays below the IEEE-754 overflow boundary used by the exact CDF.
export const medianConfidenceInterval = (values, confidence = 0.95) => {
	if (
		!Array.isArray(values) ||
		values.length < MIN_MEASURED_SAMPLES ||
		values.length > MAX_MEASURED_SAMPLES
	) {
		throw new Error(
			`median confidence interval requires ${MIN_MEASURED_SAMPLES}-${MAX_MEASURED_SAMPLES} samples`,
		);
	}
	if (values.some((value) => !Number.isFinite(value) || value <= 0)) {
		throw new Error("benchmark samples must be finite positive numbers");
	}
	if (!Number.isFinite(confidence) || confidence <= 0 || confidence >= 1) {
		throw new Error(
			"median confidence must be a finite number between 0 and 1",
		);
	}
	const sorted = [...values].sort((left, right) => left - right);
	const tailProbability = (1 - confidence) / 2;
	let tailCount = -1;
	for (let index = 0; index < Math.floor(sorted.length / 2); index += 1) {
		if (binomialHalfCdf(sorted.length, index) <= tailProbability) {
			tailCount = index;
		} else {
			break;
		}
	}
	if (tailCount < 0) {
		throw new Error(
			`${sorted.length} samples cannot form a ${confidence * 100}% median interval`,
		);
	}
	return {
		median: median(sorted),
		low: sorted[tailCount],
		high: sorted[sorted.length - 1 - tailCount],
		confidence,
	};
};

const taskKey = (task) => `${task.scenario}/peers=${task.peers}`;

const isRecord = (value) =>
	typeof value === "object" && value !== null && !Array.isArray(value);

const isPositiveSafeInteger = (value) =>
	Number.isSafeInteger(value) && value > 0;

const isNonNegativeSafeInteger = (value) =>
	Number.isSafeInteger(value) && value >= 0;

const isFinitePositive = (value) => Number.isFinite(value) && value > 0;

const approximatelyEqual = (left, right) =>
	Math.abs(left - right) <=
	Number.EPSILON * 16 * Math.max(1, Math.abs(left), Math.abs(right));

const normalizedConfig = (config) => ({
	peerCounts: [...config.peerCounts],
	warmupSamples: config.warmupSamples,
	measuredSamples: config.measuredSamples,
	iterations: config.iterations,
});

const isGateConfig = (config) =>
	JSON.stringify(config) === JSON.stringify(GATE_CONFIG);

const validateDocument = (document, label) => {
	if (
		!isRecord(document) ||
		document.schemaVersion !== 1 ||
		document.benchmark !== "sync-peer-state" ||
		!Array.isArray(document.tasks)
	) {
		throw new Error(`${label} is not a sync-peer-state schemaVersion 1 result`);
	}

	if (!isRecord(document.runtime)) {
		throw new Error(`${label} is missing benchmark runtime metadata`);
	}
	for (const field of RUNTIME_FIELDS) {
		if (
			typeof document.runtime[field] !== "string" ||
			document.runtime[field].length === 0
		) {
			throw new Error(`${label} runtime.${field} must be a non-empty string`);
		}
	}

	const config = document.config;
	if (
		!isRecord(config) ||
		!Array.isArray(config.peerCounts) ||
		config.peerCounts.length === 0 ||
		config.peerCounts.some(
			(peers) => !isPositiveSafeInteger(peers) || peers > 64,
		) ||
		new Set(config.peerCounts).size !== config.peerCounts.length ||
		!isPositiveSafeInteger(config.warmupSamples) ||
		!Number.isSafeInteger(config.measuredSamples) ||
		config.measuredSamples < MIN_MEASURED_SAMPLES ||
		config.measuredSamples > MAX_MEASURED_SAMPLES ||
		!isPositiveSafeInteger(config.iterations)
	) {
		throw new Error(
			`${label} config must contain unique peer counts in [1, 64], positive warmup/iteration counts, and ${MIN_MEASURED_SAMPLES}-${MAX_MEASURED_SAMPLES} measured samples`,
		);
	}

	const expectedTaskKeys = new Set(
		config.peerCounts.flatMap((peers) =>
			SCENARIOS.map((scenario) => `${scenario}/peers=${peers}`),
		),
	);
	const tasks = new Map();
	for (const task of document.tasks) {
		if (
			!isRecord(task) ||
			typeof task.scenario !== "string" ||
			!SCENARIO_SET.has(task.scenario) ||
			!isPositiveSafeInteger(task.peers) ||
			!config.peerCounts.includes(task.peers) ||
			!Array.isArray(task.samples)
		) {
			throw new Error(`${label} contains an invalid task`);
		}
		const key = taskKey(task);
		if (tasks.has(key)) {
			throw new Error(`${label} contains duplicate task ${key}`);
		}
		if (task.samples.length !== config.measuredSamples) {
			throw new Error(
				`${label} ${key} has ${task.samples.length} samples; expected ${config.measuredSamples}`,
			);
		}

		const expectedPeerOperations = config.iterations * task.peers;
		if (!Number.isSafeInteger(expectedPeerOperations)) {
			throw new Error(`${label} ${key} peer operation count is not safe`);
		}
		const expectedRetainedPhysicalPermits =
			task.scenario === "disconnect-reconnect-retained-physical"
				? Math.min(task.peers, RETAINED_PHYSICAL_PERMIT_CAP)
				: 0;
		for (const [sampleIndex, sample] of task.samples.entries()) {
			if (
				!isRecord(sample) ||
				!isFinitePositive(sample.elapsedMs) ||
				!isFinitePositive(sample.nsPerOperation) ||
				!isFinitePositive(sample.nsPerPeerOperation) ||
				!isNonNegativeSafeInteger(sample.retainedPhysicalPermits) ||
				!isNonNegativeSafeInteger(sample.checksum)
			) {
				throw new Error(`${label} ${key} sample ${sampleIndex} is invalid`);
			}
			if (
				sample.operations !== config.iterations ||
				sample.peerOperations !== expectedPeerOperations ||
				sample.checksum !== expectedPeerOperations ||
				sample.retainedPhysicalPermits !== expectedRetainedPhysicalPermits
			) {
				throw new Error(
					`${label} ${key} sample ${sampleIndex} performed unexpected work`,
				);
			}
			const expectedNsPerOperation =
				(sample.elapsedMs * 1e6) / sample.operations;
			const expectedNsPerPeerOperation =
				(sample.elapsedMs * 1e6) / sample.peerOperations;
			if (
				!approximatelyEqual(sample.nsPerOperation, expectedNsPerOperation) ||
				!approximatelyEqual(
					sample.nsPerPeerOperation,
					expectedNsPerPeerOperation,
				)
			) {
				throw new Error(
					`${label} ${key} sample ${sampleIndex} has inconsistent timing fields`,
				);
			}
		}

		const expectedMedianNsPerOperation = median(
			task.samples.map((sample) => sample.nsPerOperation),
		);
		const expectedMedianNsPerPeerOperation = median(
			task.samples.map((sample) => sample.nsPerPeerOperation),
		);
		if (
			!isFinitePositive(task.medianNsPerOperation) ||
			!isFinitePositive(task.medianNsPerPeerOperation) ||
			!approximatelyEqual(
				task.medianNsPerOperation,
				expectedMedianNsPerOperation,
			) ||
			!approximatelyEqual(
				task.medianNsPerPeerOperation,
				expectedMedianNsPerPeerOperation,
			)
		) {
			throw new Error(`${label} ${key} has invalid median fields`);
		}
		tasks.set(key, task);
	}

	const keys = [...tasks.keys()].sort();
	const expectedKeys = [...expectedTaskKeys].sort();
	if (JSON.stringify(keys) !== JSON.stringify(expectedKeys)) {
		throw new Error(
			`${label} does not contain the complete benchmark task set`,
		);
	}

	return {
		config: normalizedConfig(config),
		runtime: Object.fromEntries(
			RUNTIME_FIELDS.map((field) => [field, document.runtime[field]]),
		),
		tasks,
		keys,
	};
};

export const compareSyncPeerStateResults = (
	baseline,
	candidate,
	{ maxRegression = 0.05 } = {},
) => {
	if (!Number.isFinite(maxRegression) || maxRegression < 0) {
		throw new Error("maxRegression must be a finite non-negative number");
	}
	const baselineDocument = validateDocument(baseline, "baseline");
	const candidateDocument = validateDocument(candidate, "candidate");
	if (
		JSON.stringify(baselineDocument.runtime) !==
		JSON.stringify(candidateDocument.runtime)
	) {
		throw new Error(
			"baseline and candidate must use the same exact Node, V8, platform, architecture, and CPU",
		);
	}
	if (
		JSON.stringify(baselineDocument.config) !==
		JSON.stringify(candidateDocument.config)
	) {
		throw new Error(
			"baseline and candidate must use the same peer counts, warmups, measured samples, and iterations",
		);
	}
	if (
		JSON.stringify(baselineDocument.keys) !==
		JSON.stringify(candidateDocument.keys)
	) {
		throw new Error("baseline and candidate task sets differ");
	}

	const comparisons = baselineDocument.keys.map((key) => {
		const baselineTask = baselineDocument.tasks.get(key);
		const candidateTask = candidateDocument.tasks.get(key);
		const baselineInterval = medianConfidenceInterval(
			baselineTask.samples.map((sample) => sample.nsPerOperation),
		);
		const candidateInterval = medianConfidenceInterval(
			candidateTask.samples.map((sample) => sample.nsPerOperation),
		);
		const ratio = candidateInterval.median / baselineInterval.median;
		const exceedsThreshold = ratio > 1 + maxRegression;
		const thresholdSeparated =
			candidateInterval.low > baselineInterval.high * (1 + maxRegression);
		return {
			key,
			ratio,
			regressionPercent: (ratio - 1) * 100,
			baseline: baselineInterval,
			candidate: candidateInterval,
			pointEstimateExceedsThreshold: exceedsThreshold,
			credibleRegression: exceedsThreshold && thresholdSeparated,
		};
	});
	const regressions = comparisons.filter(
		(comparison) => comparison.credibleRegression,
	);
	const uncertainRegressions = comparisons.filter(
		(comparison) =>
			comparison.pointEstimateExceedsThreshold &&
			!comparison.credibleRegression,
	);
	const gateEligible = isGateConfig(baselineDocument.config);
	const status = !gateEligible
		? "inconclusive"
		: regressions.length > 0
			? "regression"
			: uncertainRegressions.length > 0
				? "inconclusive"
				: "pass";
	return {
		status,
		maxRegression,
		method:
			"median ratio plus distribution-free 95% median confidence intervals separated by the regression threshold",
		gate: {
			eligible: gateEligible,
			canonicalConfig: GATE_CONFIG,
			reasons: gateEligible
				? []
				: ["run configuration differs from the canonical gate configuration"],
		},
		comparisons,
		regressions: regressions.map((comparison) => comparison.key),
		inconclusive: uncertainRegressions.map((comparison) => comparison.key),
	};
};
