import { parseArgs } from "node:util";
import {
	LIFECYCLE_PARSE_OPTIONS,
	buildLifecycleResourceComparison,
	parseLifecycleExecutionOptions,
	parsePositiveInteger,
	recordExactFields,
	reportProgress,
} from "./shared-log-lifecycle-census-common.mjs";

export const CUT_LIFECYCLE_CENSUS_NAME = "shared-log-cut-lifecycle-census";
export const CUT_LIFECYCLE_CENSUS_SCENARIOS = ["fresh", "history"];

export const deleteHeadMatches = (remaining, hashes) => {
	let unexpected = 0;
	for (const hash of hashes) if (!remaining.delete(hash)) unexpected++;
	return unexpected;
};

const validateWorkload = ({ historyOperations, keyCount, batchSize }) => {
	const freshOperations = keyCount * 2;
	if (historyOperations <= freshOperations) {
		throw new Error(
			"history-operations must exceed one put/delete cycle for every key",
		);
	}
	if (historyOperations % freshOperations !== 0) {
		throw new Error(
			"history-operations must be divisible by two times key-count so every scenario ends after a complete delete cycle",
		);
	}
	if (batchSize > keyCount) {
		throw new Error("batch-size must not exceed key-count");
	}
};

export const parseCutLifecycleCensusArgs = (args, env = {}) => {
	const normalizedArgs = args[0] === "--" ? args.slice(1) : args;
	const { values } = parseArgs({
		args: normalizedArgs,
		strict: true,
		allowPositionals: false,
		options: {
			...LIFECYCLE_PARSE_OPTIONS,
			"history-operations": { type: "string" },
			"key-count": { type: "string" },
			"batch-size": { type: "string" },
		},
	});

	if (values.help) return { mode: "help" };

	const historyOperations = parsePositiveInteger(
		values["history-operations"] ??
			env.SHARED_LOG_CUT_LIFECYCLE_HISTORY_OPERATIONS ??
			"200",
		"history-operations",
	);
	const keyCount = parsePositiveInteger(
		values["key-count"] ?? env.SHARED_LOG_CUT_LIFECYCLE_KEY_COUNT ?? "10",
		"key-count",
	);
	const batchSize = parsePositiveInteger(
		values["batch-size"] ?? env.SHARED_LOG_CUT_LIFECYCLE_BATCH_SIZE ?? "10",
		"batch-size",
	);
	validateWorkload({ historyOperations, keyCount, batchSize });
	return parseLifecycleExecutionOptions({
		values,
		env,
		envPrefix: "SHARED_LOG_CUT_LIFECYCLE",
		scenarios: CUT_LIFECYCLE_CENSUS_SCENARIOS,
		scenarioLabel: "CUT lifecycle-census",
		workload: { historyOperations, keyCount, batchSize },
	});
};

export const buildCutLifecycleComparison = (fresh, history) => {
	const freshState = fresh.reopen.state;
	const historyState = history.reopen.state;
	const visibleFields = [
		"documentRows",
		"nativeDocumentIndexRows",
		"nativeDocumentValueRows",
		"enumeratedDocumentRows",
		"documentsFingerprint",
	];
	const unequalVisibleFields = visibleFields.filter(
		(field) => freshState[field] !== historyState[field],
	);
	const extraOperations = history.seed.operations - fresh.seed.operations;
	return {
		extraOperations,
		visibleStateMatchesFresh: unequalVisibleFields.length === 0,
		unequalVisibleFields,
		logicalHistory: {
			freshCutHeads: freshState.cutHeadRows,
			historyCutHeads: historyState.cutHeadRows,
			cutHeadOverhead: historyState.cutHeadRows - freshState.cutHeadRows,
			freshLogRows: freshState.logRows,
			historyLogRows: historyState.logRows,
			logRowOverhead: historyState.logRows - freshState.logRows,
		},
		...buildLifecycleResourceComparison(fresh, history, "afterOpen"),
	};
};

export const validateCutLifecycleState = ({
	state,
	expectedOperations,
	expectedCutHeads,
	phase,
}) => {
	const failures = [];
	const expectedLoadedHeads =
		phase === "seed" ? expectedOperations : expectedCutHeads;
	recordExactFields(failures, state, [
		[expectedOperations, ["logRows", "graphRows", "nativeLogRows"]],
		[expectedLoadedHeads, ["nativeBlockRows", "headRows", "nativeHeadRows"]],
		[
			expectedCutHeads,
			[
				"cutHeadRows",
				"residentCoordinateRows",
				"coordinateIndexRows",
				"coordinateValueRows",
				"nativeCoordinateHashes",
				"assignedHeads",
			],
		],
		[phase === "seed" ? expectedCutHeads : 0, ["nonCutHeadRows"]],
		[
			0,
			[
				"nativeHeadDuplicateRows",
				"lowerHeadNotNativeRows",
				"nativeHeadNotLowerRows",
				"retainedLowerShallowMissing",
				"retainedNativeGraphMissing",
				"retainedDurableBlockMissing",
				"rustCoordinateRows",
				"documentRows",
				"nativeDocumentIndexRows",
				"nativeDocumentValueRows",
				"enumeratedDocumentRows",
			],
		],
		[1, ["replicationRanges", "replicators", "activeReplicators"]],
	]);
	if (state.durableBlockBytes <= 0) {
		failures.push("durable CUT block footprint must be positive");
	}
	if (state.debt.nonzero.length > 0) {
		failures.push(
			`observable debt did not drain: ${state.debt.nonzero
				.map(({ field, value }) => `${field}=${value}`)
				.join(", ")}`,
		);
	}
	if (failures.length > 0) {
		throw new Error(
			`CUT lifecycle ${phase} validation failed: ${failures.join("; ")}`,
		);
	}
	return {
		baselineInvariantsPassed: true,
		exactLogRows: expectedOperations,
		exactCutHeads: expectedCutHeads,
		visibleDocuments: 0,
		observableDebtDrained: true,
		debtObservabilityGaps: state.debt.unobserved,
	};
};

export const buildCutLifecycleCensusReport = ({
	historyOperations,
	keyCount,
	batchSize,
	compactMaxJournalBytes,
	compactMaxJournalRecords,
	runs,
	rows,
	host,
	activeRow = null,
	failure = null,
}) => ({
	name: CUT_LIFECYCLE_CENSUS_NAME,
	schemaVersion: 1,
	meta: {
		historyOperations,
		freshOperations: keyCount * 2,
		keyCount,
		batchSize,
		runs,
		physicalCheckpointPolicy: {
			coupledWalMaxJournalBytes: compactMaxJournalBytes ?? null,
			coupledWalMaxJournalRecords: compactMaxJournalRecords ?? null,
			entryBlockPolicy:
				"production adaptive checkpoint policy from createRustPeerbitOptions; not overridden by the census",
		},
		isolation:
			"fresh and historical controls use separate directories; seed and reopen use separate processes",
		measurement:
			"pre-compaction baseline of repeated generic Documents put/delete cycles ending in the same empty visible state; exact row-per-operation assertions are not candidate acceptance bounds",
		reopenCacheState:
			"new Node process with potentially warm operating-system page cache",
		...host,
	},
	progress: reportProgress({ expectedRows: runs, rows, activeRow, failure }),
	rows,
});
