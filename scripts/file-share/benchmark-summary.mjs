const round = (value) => Number(value.toFixed(1));

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
	return {
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
	if (!upload || !writerReady || !readerReady) {
		return null;
	}
	return {
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
	};
};

export const uploadTimingTableColumns = (result) => ({
	timeToWriterReadyMs: result.timeToWriterReadyMs ?? null,
	timeToReaderReadyMs: result.timeToReaderReadyMs ?? null,
	postSettlementListingMs: result.listingDurationMs ?? null,
});
