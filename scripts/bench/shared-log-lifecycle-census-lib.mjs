import { parseArgs } from "node:util";

export const LIFECYCLE_CENSUS_NAME = "shared-log-lifecycle-census";
export const LIFECYCLE_CENSUS_SCENARIOS = Object.freeze(["fresh", "history"]);

const parsePositiveInteger = (value, label) => {
	const normalized = String(value).replaceAll("_", "");
	if (!/^[1-9][0-9]*$/.test(normalized)) {
		throw new Error(`${label} must be a positive integer, got '${value}'`);
	}
	const parsed = Number(normalized);
	if (!Number.isSafeInteger(parsed)) {
		throw new Error(`${label} exceeds JavaScript's safe integer range`);
	}
	return parsed;
};

const parseScenario = (value) => {
	if (!LIFECYCLE_CENSUS_SCENARIOS.includes(value)) {
		throw new Error(`Unknown lifecycle-census scenario '${value}'`);
	}
	return value;
};

const validateWorkload = ({ historyCount, retain, batchSize }) => {
	if (historyCount <= retain) {
		throw new Error("history-count must be greater than retain");
	}
	if (batchSize > retain) {
		throw new Error(
			"batch-size must not exceed retain; oversized batches do not prove steady-state trimming",
		);
	}
};

export const parseLifecycleCensusArgs = (args, env = {}) => {
	const normalizedArgs = args[0] === "--" ? args.slice(1) : args;
	const { values } = parseArgs({
		args: normalizedArgs,
		strict: true,
		allowPositionals: false,
		options: {
			"history-count": { type: "string" },
			retain: { type: "string" },
			"batch-size": { type: "string" },
			runs: { type: "string" },
			output: { type: "string" },
			json: { type: "boolean" },
			help: { type: "boolean" },
			worker: { type: "boolean" },
			scenario: { type: "string" },
			phase: { type: "string" },
			run: { type: "string" },
			directory: { type: "string" },
			"probe-hash": { type: "string" },
			"retained-probe-hash": { type: "string" },
		},
	});

	if (values.help) return { mode: "help" };

	const historyCount = parsePositiveInteger(
		values["history-count"] ??
			env.SHARED_LOG_LIFECYCLE_HISTORY_COUNT ??
			"100000",
		"history-count",
	);
	const retain = parsePositiveInteger(
		values.retain ?? env.SHARED_LOG_LIFECYCLE_RETAIN ?? "1000",
		"retain",
	);
	const batchSize = parsePositiveInteger(
		values["batch-size"] ?? env.SHARED_LOG_LIFECYCLE_BATCH_SIZE ?? "256",
		"batch-size",
	);
	validateWorkload({ historyCount, retain, batchSize });

	if (values.worker) {
		if (values.output) {
			throw new Error("worker mode does not accept --output");
		}
		if (!values.scenario || !values.phase || !values.run || !values.directory) {
			throw new Error(
				"worker mode requires --scenario, --phase, --run, and --directory",
			);
		}
		const scenario = parseScenario(values.scenario);
		if (values.phase !== "seed" && values.phase !== "reopen") {
			throw new Error("worker phase must be seed or reopen");
		}
		if (
			values.phase === "reopen" &&
			(!values["probe-hash"] || !values["retained-probe-hash"])
		) {
			throw new Error(
				"reopen worker mode requires --probe-hash and --retained-probe-hash",
			);
		}
		if (
			values.phase === "seed" &&
			(values["probe-hash"] || values["retained-probe-hash"])
		) {
			throw new Error("seed worker mode does not accept probe-hash options");
		}
		return {
			mode: "worker",
			scenario,
			phase: values.phase,
			run: parsePositiveInteger(values.run, "run"),
			directory: values.directory,
			historyCount,
			retain,
			batchSize,
			...(values["probe-hash"] ? { probeHash: values["probe-hash"] } : {}),
			...(values["retained-probe-hash"]
				? { retainedProbeHash: values["retained-probe-hash"] }
				: {}),
		};
	}

	if (
		values.scenario ||
		values.phase ||
		values.run ||
		values.directory ||
		values["probe-hash"] ||
		values["retained-probe-hash"]
	) {
		throw new Error(
			"--scenario, --phase, --run, --directory, and probe-hash options are worker-only",
		);
	}

	return {
		mode: "parent",
		historyCount,
		retain,
		batchSize,
		runs: parsePositiveInteger(
			values.runs ?? env.SHARED_LOG_LIFECYCLE_RUNS ?? "1",
			"runs",
		),
		...((values.output ?? env.SHARED_LOG_LIFECYCLE_OUTPUT)
			? { output: values.output ?? env.SHARED_LOG_LIFECYCLE_OUTPUT }
			: {}),
		json: values.json === true || env.BENCH_JSON === "1",
	};
};

export const LIFECYCLE_LIVE_VALUE_FIELDS = Object.freeze([
	"logRows",
	"graphRows",
	"nativeLogRows",
	"headRows",
	"nativeHeadRows",
	"rustCoordinateRows",
	"residentCoordinateRows",
	"coordinateIndexRows",
	"coordinateValueRows",
	"nativeCoordinateHashes",
	"documentRows",
	"nativeDocumentIndexRows",
	"nativeDocumentValueRows",
	"enumeratedDocumentRows",
	"durableBlockBytes",
	"replicationRanges",
	"replicators",
	"activeReplicators",
	"assignedHeads",
]);

export const LIFECYCLE_LIVE_FINGERPRINT_FIELDS = Object.freeze([
	"documentsFingerprint",
]);

const subtract = (left, right) => left - right;
const ratio = (numerator, denominator) =>
	numerator == null || denominator == null || denominator === 0
		? null
		: numerator / denominator;
const nullableSubtract = (left, right) =>
	left == null || right == null ? null : subtract(left, right);

export const buildLifecycleComparison = (fresh, history, historicalEntries) => {
	const freshState = fresh.reopen.state;
	const historyState = history.reopen.state;
	const unequalLiveValues = LIFECYCLE_LIVE_VALUE_FIELDS.filter(
		(field) => freshState[field] !== historyState[field],
	);
	const unequalLiveFingerprints = LIFECYCLE_LIVE_FINGERPRINT_FIELDS.filter(
		(field) => freshState[field] !== historyState[field],
	);
	const freshDisk = fresh.reopen.disk;
	const historyDisk = history.reopen.disk;
	const logicalDiskOverheadBytes = subtract(
		historyDisk.logicalBytes,
		freshDisk.logicalBytes,
	);
	const allocatedDiskOverheadBytes = nullableSubtract(
		historyDisk.allocatedBytes,
		freshDisk.allocatedBytes,
	);
	const reopenMsDelta = subtract(history.reopen.openMs, fresh.reopen.openMs);
	const reopenRssDeltaBytes = subtract(
		history.reopen.memory.afterValidation.rss,
		fresh.reopen.memory.afterValidation.rss,
	);
	const freshMeasuredRssBytes = subtract(
		fresh.reopen.memory.afterValidation.rss,
		fresh.reopen.memory.beforeOpen.rss,
	);
	const historyMeasuredRssBytes = subtract(
		history.reopen.memory.afterValidation.rss,
		history.reopen.memory.beforeOpen.rss,
	);
	return {
		historicalEntries,
		liveStateMatchesFresh:
			unequalLiveValues.length === 0 && unequalLiveFingerprints.length === 0,
		unequalLiveValues,
		unequalLiveFingerprints,
		disk: {
			freshLogicalBytes: freshDisk.logicalBytes,
			historyLogicalBytes: historyDisk.logicalBytes,
			logicalDiskOverheadBytes,
			logicalBytesPerHistoricalEntry:
				historicalEntries === 0
					? 0
					: logicalDiskOverheadBytes / historicalEntries,
			freshAllocatedBytes: freshDisk.allocatedBytes,
			historyAllocatedBytes: historyDisk.allocatedBytes,
			allocatedDiskOverheadBytes,
			allocatedBytesPerHistoricalEntry:
				allocatedDiskOverheadBytes == null
					? null
					: historicalEntries === 0
						? 0
						: allocatedDiskOverheadBytes / historicalEntries,
			logicalGrowthRatio: ratio(
				historyDisk.logicalBytes,
				freshDisk.logicalBytes,
			),
			allocatedGrowthRatio: ratio(
				historyDisk.allocatedBytes,
				freshDisk.allocatedBytes,
			),
		},
		reopen: {
			freshMs: fresh.reopen.openMs,
			historyMs: history.reopen.openMs,
			reopenMsDelta,
			growthRatio: ratio(history.reopen.openMs, fresh.reopen.openMs),
			freshRssBytes: fresh.reopen.memory.afterValidation.rss,
			historyRssBytes: history.reopen.memory.afterValidation.rss,
			reopenRssDeltaBytes,
			rssGrowthRatio: ratio(
				history.reopen.memory.afterValidation.rss,
				fresh.reopen.memory.afterValidation.rss,
			),
			freshMeasuredRssBytes,
			historyMeasuredRssBytes,
			measuredRssDeltaBytes: subtract(
				historyMeasuredRssBytes,
				freshMeasuredRssBytes,
			),
			measuredRssGrowthRatio: ratio(
				historyMeasuredRssBytes,
				freshMeasuredRssBytes,
			),
		},
	};
};

export const buildLifecycleCensusReport = ({
	historyCount,
	retain,
	batchSize,
	runs,
	rows,
	host,
	activeRow = null,
	failure = null,
}) => ({
	name: LIFECYCLE_CENSUS_NAME,
	schemaVersion: 1,
	meta: {
		historyCount,
		retain,
		batchSize,
		runs,
		isolation:
			"fresh and historical controls use separate data directories; seed and reopen use separate processes",
		measurement:
			"real durable Peerbit document append, bounded trim, clean close, cold reopen, live state, memory, and filesystem footprint",
		...host,
	},
	progress: {
		expectedRows: runs,
		completedRows: rows.length,
		complete: rows.length === runs && activeRow === null && failure === null,
		activeRow,
		...(failure ? { failure } : {}),
	},
	rows,
});
