import { parseArgs } from "node:util";
import {
	LIFECYCLE_PARSE_OPTIONS,
	buildLifecycleResourceComparison,
	classifyLifecycleCensusFile,
	parseLifecycleExecutionOptions,
	parsePositiveInteger,
	reportProgress,
} from "./shared-log-lifecycle-census-common.mjs";

export { classifyLifecycleCensusFile };

export const LIFECYCLE_CENSUS_NAME = "shared-log-lifecycle-census";
export const LIFECYCLE_CENSUS_SCENARIOS = Object.freeze(["fresh", "history"]);

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
			...LIFECYCLE_PARSE_OPTIONS,
			"history-count": { type: "string" },
			retain: { type: "string" },
			"batch-size": { type: "string" },
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
	return parseLifecycleExecutionOptions({
		values,
		env,
		envPrefix: "SHARED_LOG_LIFECYCLE",
		scenarios: LIFECYCLE_CENSUS_SCENARIOS,
		scenarioLabel: "lifecycle-census",
		workload: { historyCount, retain, batchSize },
		workerOnly: ["probe-hash", "retained-probe-hash"],
		workerExtras: (workerValues) => {
			const probeHash = workerValues["probe-hash"];
			const retainedProbeHash = workerValues["retained-probe-hash"];
			if (
				workerValues.phase === "reopen" &&
				(!probeHash || !retainedProbeHash)
			) {
				throw new Error(
					"reopen worker mode requires --probe-hash and --retained-probe-hash",
				);
			}
			if (workerValues.phase === "seed" && (probeHash || retainedProbeHash)) {
				throw new Error("seed worker mode does not accept probe-hash options");
			}
			return {
				...(probeHash ? { probeHash } : {}),
				...(retainedProbeHash ? { retainedProbeHash } : {}),
			};
		},
	});
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

export const buildLifecycleComparison = (fresh, history, historicalEntries) => {
	const freshState = fresh.reopen.state;
	const historyState = history.reopen.state;
	const unequalLiveValues = LIFECYCLE_LIVE_VALUE_FIELDS.filter(
		(field) => freshState[field] !== historyState[field],
	);
	const unequalLiveFingerprints = LIFECYCLE_LIVE_FINGERPRINT_FIELDS.filter(
		(field) => freshState[field] !== historyState[field],
	);
	const resources = buildLifecycleResourceComparison(fresh, history);
	return {
		historicalEntries,
		liveStateMatchesFresh:
			unequalLiveValues.length === 0 && unequalLiveFingerprints.length === 0,
		unequalLiveValues,
		unequalLiveFingerprints,
		disk: {
			...resources.disk,
			logicalBytesPerHistoricalEntry:
				historicalEntries === 0
					? 0
					: resources.disk.logicalDiskOverheadBytes / historicalEntries,
			allocatedBytesPerHistoricalEntry:
				resources.disk.allocatedDiskOverheadBytes == null
					? null
					: historicalEntries === 0
						? 0
						: resources.disk.allocatedDiskOverheadBytes / historicalEntries,
		},
		reopen: resources.reopen,
	};
};

export const buildLifecycleCensusReport = ({
	historyCount,
	retain,
	batchSize,
	compactMaxJournalBytes,
	compactMaxJournalRecords,
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
		coordinateCompaction: {
			maxJournalBytes: compactMaxJournalBytes ?? null,
			maxJournalRecords: compactMaxJournalRecords ?? null,
		},
		isolation:
			"fresh and historical controls use separate data directories; seed and reopen use separate processes",
		measurement:
			"real durable Peerbit document append, bounded trim, clean close, cold reopen, live state, memory, and filesystem footprint",
		...host,
	},
	progress: reportProgress({ expectedRows: runs, rows, activeRow, failure }),
	rows,
});
