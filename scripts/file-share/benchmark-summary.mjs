const round = (value) => Number(value.toFixed(1));
const TIMING_DISTRIBUTION_DEFINITION =
	"avg/min/max and linearly interpolated p25/median/p75 over passed runs only";
export const PRIMARY_DOWNLOAD_METRIC = "libraryStreamWallMs";
export const STANDARD_PRIMARY_DOWNLOAD_SINK = "hash-only";

const average = (values) =>
	values.length > 0
		? Number(
				(values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(
					1,
				),
			)
		: null;

const percentile = (sortedValues, fraction) => {
	if (sortedValues.length === 0) {
		return null;
	}
	const index = (sortedValues.length - 1) * fraction;
	const lowerIndex = Math.floor(index);
	const upperIndex = Math.ceil(index);
	if (lowerIndex === upperIndex) {
		return round(sortedValues[lowerIndex]);
	}
	const weight = index - lowerIndex;
	return round(
		sortedValues[lowerIndex] * (1 - weight) + sortedValues[upperIndex] * weight,
	);
};

const passedNumbers = (results, getter) =>
	results
		.filter((result) => result.status === "passed")
		.map(getter)
		.filter((value) => typeof value === "number" && Number.isFinite(value));

const optionalLocalityValue = (result, key) =>
	result?.[key] ?? result?.invocation?.[key] ?? null;

export const uploadLocalityCohortDimensions = (result) => ({
	mode: result.mode,
	readerLocalChunkTarget: optionalLocalityValue(
		result,
		"readerLocalChunkTarget",
	),
	readerLocalChunkMaxOvershoot: optionalLocalityValue(
		result,
		"readerLocalChunkMaxOvershoot",
	),
	readerLocalChunkBlockCount: result.readerLocalChunkBlockCount ?? null,
	readerLocalChunkIndexRowCount: result.readerLocalChunkIndexRowCount ?? null,
	readerLocalityCohortKey: result.readerLocalityCohortKey ?? null,
});

/**
 * Read-ahead may make repeated controlled-locality runs settle at different
 * exact block/index prefixes. Keep those cohorts separate so an aggregate can
 * never hide that difference in a single timing distribution.
 */
export const groupUploadResultsByLocalityCohort = (results) => {
	const grouped = new Map();
	for (const result of results) {
		const dimensions = uploadLocalityCohortDimensions(result);
		const key = JSON.stringify(dimensions);
		const group = grouped.get(key) ?? { dimensions, results: [] };
		group.results.push(result);
		grouped.set(key, group);
	}
	return [...grouped.values()];
};

export const summarizeDistribution = (values) => {
	const sortedValues = values
		.filter((value) => typeof value === "number" && Number.isFinite(value))
		.toSorted((left, right) => left - right);
	return {
		avg: average(sortedValues),
		p25: percentile(sortedValues, 0.25),
		median: percentile(sortedValues, 0.5),
		p75: percentile(sortedValues, 0.75),
		min: sortedValues.length > 0 ? sortedValues[0] : null,
		max: sortedValues.length > 0 ? sortedValues.at(-1) : null,
	};
};

const flattenDistribution = (prefix, distribution) => ({
	[`${prefix}Avg`]: distribution.avg,
	[`${prefix}P25`]: distribution.p25,
	[`${prefix}Median`]: distribution.median,
	[`${prefix}P75`]: distribution.p75,
	[`${prefix}Min`]: distribution.min,
	[`${prefix}Max`]: distribution.max,
});

/**
 * Keep the end-to-end readiness timings next to the legacy post-settlement
 * listing metric in every aggregate. The explicit alias documents that
 * listingDurationMs does not start when the user selects the file.
 */
export const summarizeUploadPerformance = (results) => {
	const uploadDuration = summarizeDistribution(
		passedNumbers(results, (result) => result.uploadDurationMs),
	);
	const timeToWriterReady = summarizeDistribution(
		passedNumbers(results, (result) => result.timeToWriterReadyMs),
	);
	const timeToReaderReady = summarizeDistribution(
		passedNumbers(results, (result) => result.timeToReaderReadyMs),
	);
	const postSettlementListingDuration = summarizeDistribution(
		passedNumbers(results, (result) => result.listingDurationMs),
	);
	const downloadDuration = summarizeDistribution(
		passedNumbers(results, (result) => result.downloadDurationMs),
	);
	const libraryStreamWall = summarizeDistribution(
		passedNumbers(results, (result) => result.libraryStreamWallMs),
	);
	const sinkWriteAwait = summarizeDistribution(
		passedNumbers(results, (result) => result.sinkWriteAwaitMs),
	);
	const sinkAwaitSubtractedDiagnostic = summarizeDistribution(
		passedNumbers(results, (result) => result.sinkAwaitSubtractedDiagnosticMs),
	);
	const passedDownloadSinks = new Set(
		results
			.filter((result) => result.status === "passed")
			.map((result) => result.downloadSink),
	);
	const downloadSink =
		passedDownloadSinks.size === 1 ? [...passedDownloadSinks][0] : null;
	return {
		timingDistributionDefinition: TIMING_DISTRIBUTION_DEFINITION,
		primaryDownloadMetric: PRIMARY_DOWNLOAD_METRIC,
		downloadSink,
		primaryDownloadAuthoritative:
			downloadSink === STANDARD_PRIMARY_DOWNLOAD_SINK,
		...flattenDistribution("uploadDurationMs", uploadDuration),
		...flattenDistribution("timeToWriterReadyMs", timeToWriterReady),
		...flattenDistribution("timeToReaderReadyMs", timeToReaderReady),
		// Kept for summary-schema compatibility.
		listingDurationMsAvg: postSettlementListingDuration.avg,
		...flattenDistribution(
			"postSettlementListingDurationMs",
			postSettlementListingDuration,
		),
		...flattenDistribution("downloadDurationMs", downloadDuration),
		...flattenDistribution("libraryStreamWallMs", libraryStreamWall),
		...flattenDistribution("sinkWriteAwaitMs", sinkWriteAwait),
		...flattenDistribution(
			"sinkAwaitSubtractedDiagnosticMs",
			sinkAwaitSubtractedDiagnostic,
		),
	};
};

const compareModeMetric = (adaptiveResults, fixedResults, getter) => {
	const adaptiveAvgMs = average(passedNumbers(adaptiveResults, getter));
	const fixed1AvgMs = average(passedNumbers(fixedResults, getter));
	if (adaptiveAvgMs == null || fixed1AvgMs == null) {
		return null;
	}
	const deltaMs = round(adaptiveAvgMs - fixed1AvgMs);
	return {
		adaptiveAvgMs,
		fixed1AvgMs,
		deltaMs,
		adaptiveVsFixed1Pct:
			fixed1AvgMs === 0 ? null : round((deltaMs / fixed1AvgMs) * 100),
	};
};

export const compareUploadPerformanceModes = (results) => {
	const passedResults = results.filter((result) => result.status === "passed");
	const downloadSinks = new Set(
		passedResults.map((result) => result.downloadSink),
	);
	if (downloadSinks.size !== 1) {
		return null;
	}
	const downloadSink = [...downloadSinks][0];
	const adaptive = results.filter((result) => result.mode === "adaptive");
	const fixed1 = results.filter((result) => result.mode === "fixed1");
	const upload = compareModeMetric(
		adaptive,
		fixed1,
		(result) => result.uploadDurationMs,
	);
	const writerReady = compareModeMetric(
		adaptive,
		fixed1,
		(result) => result.timeToWriterReadyMs,
	);
	const readerReady = compareModeMetric(
		adaptive,
		fixed1,
		(result) => result.timeToReaderReadyMs,
	);
	const primaryDownload = compareModeMetric(
		adaptive,
		fixed1,
		(result) => result.libraryStreamWallMs,
	);
	if (!upload || !writerReady || !readerReady || !primaryDownload) {
		return null;
	}
	return {
		downloadSink,
		primaryDownloadMetric: PRIMARY_DOWNLOAD_METRIC,
		primaryDownloadAuthoritative:
			downloadSink === STANDARD_PRIMARY_DOWNLOAD_SINK,
		adaptiveAvgMs: upload.adaptiveAvgMs,
		fixed1AvgMs: upload.fixed1AvgMs,
		deltaMs: upload.deltaMs,
		adaptiveVsFixed1Pct: upload.adaptiveVsFixed1Pct,
		adaptiveWriterReadyAvgMs: writerReady.adaptiveAvgMs,
		fixed1WriterReadyAvgMs: writerReady.fixed1AvgMs,
		writerReadyDeltaMs: writerReady.deltaMs,
		adaptiveVsFixed1WriterReadyPct: writerReady.adaptiveVsFixed1Pct,
		adaptiveReaderReadyAvgMs: readerReady.adaptiveAvgMs,
		fixed1ReaderReadyAvgMs: readerReady.fixed1AvgMs,
		readerReadyDeltaMs: readerReady.deltaMs,
		adaptiveVsFixed1ReaderReadyPct: readerReady.adaptiveVsFixed1Pct,
		adaptiveLibraryStreamWallAvgMs: primaryDownload.adaptiveAvgMs,
		fixed1LibraryStreamWallAvgMs: primaryDownload.fixed1AvgMs,
		libraryStreamWallDeltaMs: primaryDownload.deltaMs,
		adaptiveVsFixed1LibraryStreamWallPct: primaryDownload.adaptiveVsFixed1Pct,
	};
};

export const isCompletePassedBenchmarkPlan = (results, outcomeCounts) =>
	Array.isArray(results) &&
	Number.isSafeInteger(outcomeCounts?.planned) &&
	outcomeCounts.planned > 0 &&
	outcomeCounts.completed === outcomeCounts.planned &&
	outcomeCounts.passed === outcomeCounts.planned &&
	outcomeCounts.failed === 0 &&
	results.length === outcomeCounts.planned &&
	results.every((result) => result.status === "passed");

export const compareUploadPerformanceModesForCompletePlan = (
	results,
	outcomeCounts,
) =>
	isCompletePassedBenchmarkPlan(results, outcomeCounts)
		? compareUploadPerformanceModes(results)
		: null;

export const uploadTimingTableColumns = (result) => ({
	timeToWriterReadyMs: result.timeToWriterReadyMs ?? null,
	timeToReaderReadyMs: result.timeToReaderReadyMs ?? null,
	postSettlementListingMs: result.listingDurationMs ?? null,
});
