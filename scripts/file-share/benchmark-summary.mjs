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

const explicitBenchmarkDimension = (result, key) => ({
	present:
		Object.hasOwn(result ?? {}, key) ||
		Object.hasOwn(result?.invocation ?? {}, key),
	value: optionalLocalityValue(result, key),
});

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
 * exact block/index prefixes. Effective writer/reader policies can also differ
 * from requested inputs. Keep both dimensions separate so one timing
 * distribution can never hide either difference.
 */
export const groupUploadResultsByLocalityCohort = (results) => {
	const runtimeEvidence = results.map((result) =>
		extractRuntimeConfigurationPairEvidence(result),
	);
	const anyRuntimeEvidence = runtimeEvidence.some(
		(evidence) => evidence.present,
	);
	const browserStorageEvidence = results.map((result) =>
		explicitBenchmarkDimension(result, "browserStorageMode"),
	);
	const readerPersistenceEvidence = results.map((result) =>
		explicitBenchmarkDimension(result, "readerPersistChunkReads"),
	);
	const anyBrowserStorageEvidence = browserStorageEvidence.some(
		(evidence) => evidence.present,
	);
	const anyReaderPersistenceEvidence = readerPersistenceEvidence.some(
		(evidence) => evidence.present,
	);
	const grouped = new Map();
	for (const [index, result] of results.entries()) {
		const dimensions = {
			...uploadLocalityCohortDimensions(result),
			...(anyBrowserStorageEvidence
				? { browserStorageMode: browserStorageEvidence[index].value }
				: {}),
			...(anyReaderPersistenceEvidence
				? {
						readerPersistChunkReads: readerPersistenceEvidence[index].value,
					}
				: {}),
			...(anyRuntimeEvidence
				? {
						runtimeConfigurationCohortKey: runtimeEvidence[index].cohortKey,
					}
				: {}),
		};
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

const isRecord = (value) =>
	value != null && typeof value === "object" && !Array.isArray(value);

const finiteValues = (values) =>
	values.filter((value) => typeof value === "number" && Number.isFinite(value));

const optionalDistribution = (values) => {
	const numbers = finiteValues(values);
	return numbers.length > 0 ? summarizeDistribution(numbers) : null;
};

const phaseMemoryWindow = (series, startedAt, finishedAt, metrics) => {
	if (
		!Array.isArray(series?.samples) ||
		!Number.isSafeInteger(startedAt) ||
		!Number.isSafeInteger(finishedAt) ||
		finishedAt < startedAt
	) {
		return null;
	}
	const samples = series.samples.filter(
		(sample) => sample?.sampleKind !== "terminal",
	);
	if (samples.length === 0) {
		return null;
	}
	let startIndex = -1;
	for (let index = 0; index < samples.length; index += 1) {
		if (samples[index]?.capturedAt <= startedAt) {
			startIndex = index;
		} else {
			break;
		}
	}
	if (startIndex < 0) {
		startIndex = samples.findIndex((sample) => sample?.capturedAt >= startedAt);
	}
	if (startIndex < 0) {
		return null;
	}
	let endIndex = samples.findIndex(
		(sample, index) => index >= startIndex && sample?.capturedAt >= finishedAt,
	);
	if (endIndex < 0) {
		return null;
	}
	if (endIndex < startIndex) {
		return null;
	}
	const windowSamples = samples.slice(startIndex, endIndex + 1);
	const summary = {};
	for (const [metricName, sampleField] of Object.entries(metrics)) {
		const values = finiteValues(
			windowSamples.map((sample) => sample?.[sampleField]),
		);
		const startValue = samples[startIndex]?.[sampleField];
		const endValue = samples[endIndex]?.[sampleField];
		if (
			values.length !== windowSamples.length ||
			typeof startValue !== "number" ||
			!Number.isFinite(startValue) ||
			typeof endValue !== "number" ||
			!Number.isFinite(endValue)
		) {
			continue;
		}
		summary[metricName] = {
			peakBytes: Math.max(...values),
			endDeltaBytes: endValue - startValue,
		};
	}
	return Object.keys(summary).length > 0 ? summary : null;
};

const MEMORY_SERIES = {
	readerRenderer: {
		seriesName: "readerJsHeap",
		metrics: {
			usedBytes: "usedBytes",
			embedderHeapUsedBytes: "embedderHeapUsedBytes",
			backingStorageBytes: "backingStorageBytes",
		},
	},
	writerRenderer: {
		seriesName: "writerJsHeap",
		metrics: {
			usedBytes: "usedBytes",
			embedderHeapUsedBytes: "embedderHeapUsedBytes",
			backingStorageBytes: "backingStorageBytes",
		},
	},
	host: {
		seriesName: "hostRss",
		metrics: {
			rssBytes: "combinedBytes",
			nodeExternalBytes: "nodeExternalBytes",
			nodeArrayBuffersBytes: "nodeArrayBuffersBytes",
		},
	},
};

const extractMemoryPhases = (result) => {
	const telemetry = result?.downloadMemoryTelemetry;
	if (telemetry?.profile !== "download-memory-v3") {
		return null;
	}
	const timestamps = result.timestamps;
	const boundaries = {
		transfer: [timestamps?.downloadStartedAt, timestamps?.downloadFinishedAt],
		soak: [
			timestamps?.postTransferSoakStartedAt,
			timestamps?.postTransferSoakFinishedAt,
		],
	};
	const phases = {};
	for (const [phaseName, [startedAt, finishedAt]] of Object.entries(
		boundaries,
	)) {
		const phase = {};
		for (const [scope, { seriesName, metrics }] of Object.entries(
			MEMORY_SERIES,
		)) {
			const window = phaseMemoryWindow(
				telemetry[seriesName],
				startedAt,
				finishedAt,
				metrics,
			);
			if (window) {
				phase[scope] = window;
			}
		}
		if (Object.keys(phase).length > 0) {
			phases[phaseName] = phase;
		}
	}
	return Object.keys(phases).length > 0 ? phases : null;
};

const summarizeMemoryPhases = (results) => {
	const records = results
		.filter((result) => result.status === "passed")
		.map((result) => extractMemoryPhases(result))
		.filter(Boolean);
	if (records.length === 0) {
		return null;
	}
	const phases = {};
	for (const phaseName of ["transfer", "soak"]) {
		const phase = {};
		for (const [scope, { metrics }] of Object.entries(MEMORY_SERIES)) {
			const scopeSummary = {};
			for (const metricName of Object.keys(metrics)) {
				const peakBytes = optionalDistribution(
					records.map(
						(record) => record?.[phaseName]?.[scope]?.[metricName]?.peakBytes,
					),
				);
				const endDeltaBytes = optionalDistribution(
					records.map(
						(record) =>
							record?.[phaseName]?.[scope]?.[metricName]?.endDeltaBytes,
					),
				);
				if (peakBytes || endDeltaBytes) {
					scopeSummary[metricName] = { peakBytes, endDeltaBytes };
				}
			}
			if (Object.keys(scopeSummary).length > 0) {
				phase[scope] = scopeSummary;
			}
		}
		if (Object.keys(phase).length > 0) {
			phases[phaseName] = phase;
		}
	}
	return {
		profile: "download-memory-v3",
		unit: "bytes",
		evidenceRunCount: records.length,
		phaseBoundaryDefinition:
			"last live sample at or before the phase start through the first live sample at or after the phase end; terminal post-shutdown samples are excluded, exact sample timestamps retain the boundary overhang, and validated evidence bounds every adjacent capturedAt gap by sampleIntervalMs + operationTimeoutMs + schedulingToleranceMs",
		...phases,
	};
};

const summarizeResourceStorage = (results) => {
	const records = results
		.filter((result) => result.status === "passed")
		.map((result) => result?.resourceEvidence)
		.filter((evidence) => isRecord(evidence?.intervals));
	if (records.length === 0) {
		return null;
	}
	const phases = {};
	for (const phaseName of [
		"timedReadEnvelope",
		"postTransferWork",
		"soak",
		"total",
	]) {
		const phase = {};
		for (const role of ["writer", "reader"]) {
			const storageKey = `${role}Storage`;
			const peerbitLogUsageDeltaBytes = optionalDistribution(
				records.map(
					(record) =>
						record.intervals?.[phaseName]?.[storageKey]
							?.peerbitLogUsageDeltaBytes,
				),
			);
			const backingStorageUsageDeltaBytes = optionalDistribution(
				records.map(
					(record) =>
						record.intervals?.[phaseName]?.[storageKey]
							?.backingStorageUsageDeltaBytes,
				),
			);
			if (peerbitLogUsageDeltaBytes || backingStorageUsageDeltaBytes) {
				phase[role] = {
					peerbitLogUsageDeltaBytes,
					backingStorageUsageDeltaBytes,
				};
			}
		}
		if (Object.keys(phase).length > 0) {
			phases[phaseName] = phase;
		}
	}
	return {
		unit: "bytes",
		evidenceRunCount: records.length,
		definition:
			"later-minus-earlier deltas over resource-evidence v2 checkpoints; peerbitLog is Peerbit logical storage while backingStorage is the browser origin-wide navigator.storage estimate",
		...phases,
	};
};

const EAGER_LIMIT_KEYS = [
	"maxEntries",
	"maxBytes",
	"maxBlockBytes",
	"ttlMs",
	"validationConcurrency",
	"maxPendingBytes",
	"maxPendingEntries",
];

const orderedEagerLimits = (limits) =>
	isRecord(limits)
		? Object.fromEntries(
				EAGER_LIMIT_KEYS.map((key) => [key, limits[key] ?? null]),
			)
		: null;

const extractRuntimeConfiguration = (result, role) => {
	const runtime =
		result?.resourceEvidence?.snapshots?.beforeTimedRead?.[role]?.runtime;
	if (
		!isRecord(runtime?.nativeGraph) ||
		!isRecord(runtime?.eagerBlocks) ||
		!isRecord(runtime?.pubsub)
	) {
		return null;
	}
	const fanout = runtime.pubsub.snapshot?.fanout;
	const telemetry = runtime.eagerBlocks.telemetry;
	return {
		nativeGraph: {
			active: runtime.nativeGraph.active ?? null,
			useHeads: runtime.nativeGraph.useHeads ?? null,
		},
		eagerBlocks: {
			telemetryAvailable: runtime.eagerBlocks.telemetryAvailable ?? null,
			enabled: runtime.eagerBlocks.enabled ?? null,
			limits: orderedEagerLimits(telemetry?.limits),
		},
		pubsub: {
			runtimeSnapshotAvailable: runtime.pubsub.runtimeSnapshotAvailable ?? null,
			error: runtime.pubsub.error ?? null,
			rootUploadLimitBps: fanout?.root?.uploadLimitBps ?? null,
			nodeUploadLimitBps: fanout?.node?.uploadLimitBps ?? null,
		},
	};
};

const extractRuntimeConfigurationPairEvidence = (result) => {
	const beforeTimedRead = result?.resourceEvidence?.snapshots?.beforeTimedRead;
	const present =
		beforeTimedRead?.writer?.runtime != null ||
		beforeTimedRead?.reader?.runtime != null;
	const writer = extractRuntimeConfiguration(result, "writer");
	const reader = extractRuntimeConfiguration(result, "reader");
	const pair = writer && reader ? { writer, reader } : null;
	return {
		present,
		pair,
		cohortKey: pair ? JSON.stringify(pair) : null,
	};
};

const groupedConfigurations = (configurations) => {
	const grouped = new Map();
	for (const configuration of configurations) {
		if (!configuration) {
			continue;
		}
		const key = JSON.stringify(configuration);
		const group = grouped.get(key) ?? { configuration, runCount: 0 };
		group.runCount += 1;
		grouped.set(key, group);
	}
	return [...grouped.entries()]
		.toSorted(([left], [right]) => left.localeCompare(right))
		.map(([, group]) => group);
};

const summarizeRuntimeConfigurations = (results) => {
	const passedRecords = results
		.filter((result) => result.status === "passed")
		.map((result) => {
			const evidence = extractRuntimeConfigurationPairEvidence(result);
			return {
				writer: extractRuntimeConfiguration(result, "writer"),
				reader: extractRuntimeConfiguration(result, "reader"),
				pair: evidence.pair,
				cohortKey: evidence.cohortKey,
			};
		});
	const records = passedRecords.filter(
		(record) => record.writer || record.reader,
	);
	if (records.length === 0) {
		return null;
	}
	const writer = groupedConfigurations(records.map((record) => record.writer));
	const reader = groupedConfigurations(records.map((record) => record.reader));
	const pairKeys = new Set(passedRecords.map((record) => record.cohortKey));
	const cohortKey =
		passedRecords.every((record) => record.pair) && pairKeys.size === 1
			? passedRecords[0].cohortKey
			: null;
	return {
		definition:
			"normalized effective writer/reader pair of native graph, eager-cache limits, and pubsub fanout upload policy captured before the timed read",
		evidenceRunCount: records.length,
		writer,
		reader,
		...(cohortKey == null ? {} : { cohortKey }),
	};
};

const EAGER_REJECTION_KEYS = [
	"rejectedCid",
	"rejectedCodec",
	"rejectedSize",
	"rejectedPending",
	"rejectedIntegrity",
	"rejectedLifecycle",
];

const extractAfterSoakEagerTelemetry = (result, role) =>
	result?.resourceEvidence?.snapshots?.afterSoak?.[role]?.runtime?.eagerBlocks
		?.telemetry ?? null;

const eagerIntervalSummary = (results, role, phaseName) => {
	const eagerKey = `${role}Eager`;
	const deltas = results.map(
		(result) => result?.resourceEvidence?.intervals?.[phaseName]?.[eagerKey],
	);
	const evictions = optionalDistribution(
		deltas.map((delta) => delta?.evictions),
	);
	const expirations = optionalDistribution(
		deltas.map((delta) => delta?.expirations),
	);
	const admitted = optionalDistribution(deltas.map((delta) => delta?.admitted));
	const hits = optionalDistribution(deltas.map((delta) => delta?.hits));
	const rejections = {};
	for (const key of EAGER_REJECTION_KEYS) {
		const distribution = optionalDistribution(
			deltas.map((delta) => delta?.[key]),
		);
		if (distribution) {
			rejections[key] = distribution;
		}
	}
	const rejectionTotals = deltas.map((delta) => {
		const values = EAGER_REJECTION_KEYS.map((key) => delta?.[key]);
		return values.every(
			(value) => typeof value === "number" && Number.isFinite(value),
		)
			? values.reduce((total, value) => total + value, 0)
			: null;
	});
	const total = optionalDistribution(rejectionTotals);
	if (
		!admitted &&
		!hits &&
		!evictions &&
		!expirations &&
		Object.keys(rejections).length === 0
	) {
		return null;
	}
	return {
		admitted,
		hits,
		evictions,
		expirations,
		rejections: { total, ...rejections },
	};
};

const eagerRoleSummary = (results, role) => {
	const telemetry = results.map((result) =>
		extractAfterSoakEagerTelemetry(result, role),
	);
	const lifetimePeaksAfterSoak = {};
	for (const key of [
		"peakEntries",
		"peakBytes",
		"peakPendingEntries",
		"peakPendingBytes",
	]) {
		const distribution = optionalDistribution(
			telemetry.map((value) => value?.[key]),
		);
		if (distribution) {
			lifetimePeaksAfterSoak[key] = distribution;
		}
	}
	const currentAfterSoak = {};
	for (const key of ["entries", "bytes", "pendingEntries", "pendingBytes"]) {
		const distribution = optionalDistribution(
			telemetry.map((value) => value?.[key]),
		);
		if (distribution) {
			currentAfterSoak[key] = distribution;
		}
	}
	const intervals = {};
	for (const phaseName of [
		"timedReadEnvelope",
		"postTransferWork",
		"soak",
		"total",
	]) {
		const summary = eagerIntervalSummary(results, role, phaseName);
		if (summary) {
			intervals[phaseName] = summary;
		}
	}
	if (
		Object.keys(lifetimePeaksAfterSoak).length === 0 &&
		Object.keys(currentAfterSoak).length === 0 &&
		Object.keys(intervals).length === 0
	) {
		return null;
	}
	return { currentAfterSoak, lifetimePeaksAfterSoak, ...intervals };
};

const summarizeEagerCache = (results) => {
	const passed = results.filter((result) => result.status === "passed");
	const writer = eagerRoleSummary(passed, "writer");
	const reader = eagerRoleSummary(passed, "reader");
	if (!writer && !reader) {
		return null;
	}
	const evidenceRunCount = passed.filter(
		(result) =>
			extractAfterSoakEagerTelemetry(result, "writer") ||
			extractAfterSoakEagerTelemetry(result, "reader"),
	).length;
	return {
		unit: "entries-or-bytes-as-named",
		evidenceRunCount,
		definition:
			"current occupancy and lifetime peaks at the after-soak snapshot plus per-interval deltas of admitted blocks, hits, evictions, expirations, and rejection counters",
		...(writer ? { writer } : {}),
		...(reader ? { reader } : {}),
	};
};

const summarizeRuntimeEvidence = (results) => {
	const memoryPhases = summarizeMemoryPhases(results);
	const resourceStorageDeltas = summarizeResourceStorage(results);
	const effectiveRuntimeConfiguration = summarizeRuntimeConfigurations(results);
	const eagerCache = summarizeEagerCache(results);
	if (
		!memoryPhases &&
		!resourceStorageDeltas &&
		!effectiveRuntimeConfiguration &&
		!eagerCache
	) {
		return null;
	}
	return {
		...(memoryPhases ? { memoryPhases } : {}),
		...(resourceStorageDeltas ? { resourceStorageDeltas } : {}),
		...(effectiveRuntimeConfiguration ? { effectiveRuntimeConfiguration } : {}),
		...(eagerCache ? { eagerCache } : {}),
	};
};

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
	const runtimeEvidence = summarizeRuntimeEvidence(results);
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
		...(runtimeEvidence ? { runtimeEvidence } : {}),
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

const comparableRuntimeConfigurationCohortKey = (passedResults) => {
	const evidence = passedResults.map((result) =>
		extractRuntimeConfigurationPairEvidence(result),
	);
	if (evidence.every((value) => !value.present)) {
		return { legacy: true, cohortKey: null };
	}
	if (
		evidence.some((value) => !value.present || value.pair == null) ||
		new Set(evidence.map((value) => value.cohortKey)).size !== 1
	) {
		return null;
	}
	return { legacy: false, cohortKey: evidence[0].cohortKey };
};

const comparableExplicitBenchmarkDimension = (passedResults, key) => {
	const evidence = passedResults.map((result) =>
		explicitBenchmarkDimension(result, key),
	);
	if (evidence.every((entry) => !entry.present)) {
		return { legacy: true, value: null };
	}
	if (
		evidence.some((entry) => !entry.present) ||
		new Set(evidence.map((entry) => JSON.stringify(entry.value))).size !== 1
	) {
		return null;
	}
	return { legacy: false, value: evidence[0].value };
};

export const compareUploadPerformanceModes = (results) => {
	const passedResults = results.filter((result) => result.status === "passed");
	const downloadSinks = new Set(
		passedResults.map((result) => result.downloadSink),
	);
	if (downloadSinks.size !== 1) {
		return null;
	}
	const runtimeConfiguration =
		comparableRuntimeConfigurationCohortKey(passedResults);
	if (!runtimeConfiguration) {
		return null;
	}
	const browserStorage = comparableExplicitBenchmarkDimension(
		passedResults,
		"browserStorageMode",
	);
	const readerPersistence = comparableExplicitBenchmarkDimension(
		passedResults,
		"readerPersistChunkReads",
	);
	if (!browserStorage || !readerPersistence) {
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
		...(browserStorage.legacy
			? {}
			: { browserStorageMode: browserStorage.value }),
		...(readerPersistence.legacy
			? {}
			: { readerPersistChunkReads: readerPersistence.value }),
		...(runtimeConfiguration.legacy
			? {}
			: {
					runtimeConfigurationCohortKey: runtimeConfiguration.cohortKey,
				}),
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
