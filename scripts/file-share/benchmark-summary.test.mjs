import assert from "node:assert/strict";
import test from "node:test";
import {
	compareUploadPerformanceModes,
	compareUploadPerformanceModesForCompletePlan,
	groupUploadResultsByLocalityCohort,
	isCompletePassedBenchmarkPlan,
	summarizeUploadPerformance,
	uploadTimingTableColumns,
} from "./benchmark-summary.mjs";

test("keeps exact observer-locality cohorts separate in aggregates", () => {
	const uncontrolled = {
		status: "passed",
		mode: "adaptive",
	};
	const cold = {
		status: "passed",
		mode: "fixed1",
		readerLocalChunkTarget: 0,
		readerLocalChunkMaxOvershoot: 0,
		readerLocalChunkBlockCount: 0,
		readerLocalChunkIndexRowCount: 0,
		readerLocalityCohortKey: "observer-persistent-prefix-b0-i0",
	};
	const warmBlockOnly = {
		status: "passed",
		mode: "fixed1",
		readerLocalChunkTarget: 1,
		readerLocalChunkMaxOvershoot: 1,
		readerLocalChunkBlockCount: 1,
		readerLocalChunkIndexRowCount: 0,
		readerLocalityCohortKey: "observer-persistent-prefix-b1-i0",
	};
	const warmIndexed = {
		...warmBlockOnly,
		readerLocalChunkIndexRowCount: 1,
		readerLocalityCohortKey: "observer-persistent-prefix-b1-i1",
	};
	const failedBeforeObservation = {
		status: "failed",
		mode: "fixed1",
		invocation: {
			readerLocalChunkTarget: 1,
			readerLocalChunkMaxOvershoot: 1,
		},
	};
	const groups = groupUploadResultsByLocalityCohort([
		uncontrolled,
		cold,
		warmBlockOnly,
		{ ...warmBlockOnly },
		warmIndexed,
		failedBeforeObservation,
	]);

	assert.equal(groups.length, 5);
	assert.deepEqual(
		groups.map(({ dimensions, results }) => ({
			...dimensions,
			runs: results.length,
		})),
		[
			{
				mode: "adaptive",
				readerLocalChunkTarget: null,
				readerLocalChunkMaxOvershoot: null,
				readerLocalChunkBlockCount: null,
				readerLocalChunkIndexRowCount: null,
				readerLocalityCohortKey: null,
				runs: 1,
			},
			{
				mode: "fixed1",
				readerLocalChunkTarget: 0,
				readerLocalChunkMaxOvershoot: 0,
				readerLocalChunkBlockCount: 0,
				readerLocalChunkIndexRowCount: 0,
				readerLocalityCohortKey: "observer-persistent-prefix-b0-i0",
				runs: 1,
			},
			{
				mode: "fixed1",
				readerLocalChunkTarget: 1,
				readerLocalChunkMaxOvershoot: 1,
				readerLocalChunkBlockCount: 1,
				readerLocalChunkIndexRowCount: 0,
				readerLocalityCohortKey: "observer-persistent-prefix-b1-i0",
				runs: 2,
			},
			{
				mode: "fixed1",
				readerLocalChunkTarget: 1,
				readerLocalChunkMaxOvershoot: 1,
				readerLocalChunkBlockCount: 1,
				readerLocalChunkIndexRowCount: 1,
				readerLocalityCohortKey: "observer-persistent-prefix-b1-i1",
				runs: 1,
			},
			{
				mode: "fixed1",
				readerLocalChunkTarget: 1,
				readerLocalChunkMaxOvershoot: 1,
				readerLocalChunkBlockCount: null,
				readerLocalChunkIndexRowCount: null,
				readerLocalityCohortKey: null,
				runs: 1,
			},
		],
	);
});

test("keeps storage and explicit reader-persistence cohorts separate in merged aggregates", () => {
	const base = {
		status: "passed",
		mode: "fixed1",
		readerLocalChunkTarget: 0,
		readerLocalChunkMaxOvershoot: 0,
		readerLocalChunkBlockCount: 0,
		readerLocalChunkIndexRowCount: 0,
		readerLocalityCohortKey: "reused-locality-key",
		browserStorageMode: "memory",
		readerPersistChunkReads: true,
	};
	const invocationOnly = {
		...base,
		browserStorageMode: undefined,
		readerPersistChunkReads: undefined,
		invocation: {
			browserStorageMode: "memory",
			readerPersistChunkReads: true,
		},
	};
	delete invocationOnly.browserStorageMode;
	delete invocationOnly.readerPersistChunkReads;
	const differentStorage = {
		...base,
		browserStorageMode: "opfs",
	};
	const differentPersistence = {
		...base,
		readerPersistChunkReads: false,
	};
	const legacy = { ...base };
	delete legacy.browserStorageMode;
	delete legacy.readerPersistChunkReads;

	const groups = groupUploadResultsByLocalityCohort([
		base,
		invocationOnly,
		differentStorage,
		differentPersistence,
		legacy,
	]);

	assert.deepEqual(
		groups.map(({ dimensions, results }) => ({
			browserStorageMode: dimensions.browserStorageMode,
			readerPersistChunkReads: dimensions.readerPersistChunkReads,
			runs: results.length,
		})),
		[
			{
				browserStorageMode: "memory",
				readerPersistChunkReads: true,
				runs: 2,
			},
			{
				browserStorageMode: "opfs",
				readerPersistChunkReads: true,
				runs: 1,
			},
			{
				browserStorageMode: "memory",
				readerPersistChunkReads: false,
				runs: 1,
			},
			{
				browserStorageMode: null,
				readerPersistChunkReads: null,
				runs: 1,
			},
		],
	);
});

test("propagates end-to-end readiness and labels post-settlement listing", () => {
	const results = [
		{
			status: "passed",
			uploadDurationMs: 80,
			timeToWriterReadyMs: 100,
			timeToReaderReadyMs: 140,
			listingDurationMs: 20,
			downloadDurationMs: 50,
			libraryStreamWallMs: 45,
			sinkWriteAwaitMs: 5,
			sinkAwaitSubtractedDiagnosticMs: 40,
			downloadSink: "hash-only",
			sinkWriteDurationMs: 4,
		},
		{
			status: "passed",
			uploadDurationMs: 120,
			timeToWriterReadyMs: 200,
			timeToReaderReadyMs: 260,
			listingDurationMs: 40,
			downloadDurationMs: 70,
			libraryStreamWallMs: 65,
			sinkWriteAwaitMs: 15,
			sinkAwaitSubtractedDiagnosticMs: 50,
			downloadSink: "hash-only",
			sinkWriteDurationMs: 14,
		},
		{
			status: "failed",
			timeToWriterReadyMs: 9_999,
			timeToReaderReadyMs: 9_999,
			listingDurationMs: 9_999,
		},
	];

	assert.deepEqual(summarizeUploadPerformance(results), {
		timingDistributionDefinition:
			"avg/min/max and linearly interpolated p25/median/p75 over passed runs only",
		primaryDownloadMetric: "libraryStreamWallMs",
		downloadSink: "hash-only",
		primaryDownloadAuthoritative: true,
		uploadDurationMsAvg: 100,
		uploadDurationMsP25: 90,
		uploadDurationMsMedian: 100,
		uploadDurationMsP75: 110,
		uploadDurationMsMin: 80,
		uploadDurationMsMax: 120,
		timeToWriterReadyMsAvg: 150,
		timeToWriterReadyMsP25: 125,
		timeToWriterReadyMsMedian: 150,
		timeToWriterReadyMsP75: 175,
		timeToWriterReadyMsMin: 100,
		timeToWriterReadyMsMax: 200,
		timeToReaderReadyMsAvg: 200,
		timeToReaderReadyMsP25: 170,
		timeToReaderReadyMsMedian: 200,
		timeToReaderReadyMsP75: 230,
		timeToReaderReadyMsMin: 140,
		timeToReaderReadyMsMax: 260,
		listingDurationMsAvg: 30,
		postSettlementListingDurationMsAvg: 30,
		postSettlementListingDurationMsP25: 25,
		postSettlementListingDurationMsMedian: 30,
		postSettlementListingDurationMsP75: 35,
		postSettlementListingDurationMsMin: 20,
		postSettlementListingDurationMsMax: 40,
		downloadDurationMsAvg: 60,
		downloadDurationMsP25: 55,
		downloadDurationMsMedian: 60,
		downloadDurationMsP75: 65,
		downloadDurationMsMin: 50,
		downloadDurationMsMax: 70,
		libraryStreamWallMsAvg: 55,
		libraryStreamWallMsP25: 50,
		libraryStreamWallMsMedian: 55,
		libraryStreamWallMsP75: 60,
		libraryStreamWallMsMin: 45,
		libraryStreamWallMsMax: 65,
		sinkWriteAwaitMsAvg: 10,
		sinkWriteAwaitMsP25: 7.5,
		sinkWriteAwaitMsMedian: 10,
		sinkWriteAwaitMsP75: 12.5,
		sinkWriteAwaitMsMin: 5,
		sinkWriteAwaitMsMax: 15,
		sinkAwaitSubtractedDiagnosticMsAvg: 45,
		sinkAwaitSubtractedDiagnosticMsP25: 42.5,
		sinkAwaitSubtractedDiagnosticMsMedian: 45,
		sinkAwaitSubtractedDiagnosticMsP75: 47.5,
		sinkAwaitSubtractedDiagnosticMsMin: 40,
		sinkAwaitSubtractedDiagnosticMsMax: 50,
	});
	assert.deepEqual(uploadTimingTableColumns(results[0]), {
		timeToWriterReadyMs: 100,
		timeToReaderReadyMs: 140,
		postSettlementListingMs: 20,
	});
});

const memorySamples = (base, host = false) =>
	[900, 1_500, 2_100, 2_900, 3_500, 4_100, 5_000].map((capturedAt, index) =>
		host
			? {
					capturedAt,
					combinedBytes: base + 1_000 + index * 100,
					nodeExternalBytes: base + 2_000 + index * 20,
					nodeArrayBuffersBytes: base + 3_000 + index * 5,
				}
			: {
					capturedAt,
					usedBytes: base + index * 10,
					embedderHeapUsedBytes: base + 100 + index * 20,
					backingStorageBytes: base + 200 + index * 30,
				},
	);

const eagerTelemetry = (overrides = {}) => ({
	entries: 8,
	bytes: 400,
	peakEntries: 20,
	peakBytes: 1_000,
	pendingEntries: 1,
	pendingBytes: 100,
	peakPendingEntries: 3,
	peakPendingBytes: 300,
	admitted: 12,
	hits: 7,
	limits: {
		maxEntries: 1_000,
		maxBytes: 32 * 1024 * 1024,
		maxBlockBytes: 10 * 1024 * 1024,
		ttlMs: 10_000,
		validationConcurrency: 2,
		maxPendingBytes: 20 * 1024 * 1024,
		maxPendingEntries: 64,
	},
	...overrides,
});

const runtimeSnapshot = (overrides = {}) => ({
	nativeGraph: { active: true, useHeads: false },
	eagerBlocks: {
		telemetryAvailable: true,
		enabled: true,
		telemetry: eagerTelemetry(),
	},
	pubsub: {
		runtimeSnapshotAvailable: true,
		error: null,
		snapshot: {
			fanout: {
				root: { uploadLimitBps: 5_000_000 },
				node: { uploadLimitBps: 5_000_000 },
			},
		},
	},
	...overrides,
});

const eagerDelta = (overrides = {}) => ({
	admitted: 5,
	hits: 3,
	evictions: 2,
	expirations: 1,
	rejectedCid: 1,
	rejectedCodec: 2,
	rejectedSize: 0,
	rejectedPending: 0,
	rejectedIntegrity: 0,
	rejectedLifecycle: 0,
	...overrides,
});

const storageDelta = (
	role,
	peerbitLogUsageDeltaBytes,
	backingStorageUsageDeltaBytes,
) => ({
	role,
	peerbitLogUsageDeltaBytes,
	backingStorageUsageDeltaBytes,
});

const runtimeEvidenceResult = () => ({
	status: "passed",
	mode: "fixed1",
	downloadSink: "hash-only",
	uploadDurationMs: 10,
	timeToWriterReadyMs: 20,
	timeToReaderReadyMs: 30,
	listingDurationMs: 5,
	downloadDurationMs: 1_000,
	libraryStreamWallMs: 900,
	sinkWriteAwaitMs: 10,
	sinkAwaitSubtractedDiagnosticMs: 890,
	timestamps: {
		downloadStartedAt: 1_000,
		downloadFinishedAt: 2_000,
		postTransferSoakStartedAt: 3_000,
		postTransferSoakFinishedAt: 4_000,
	},
	downloadMemoryTelemetry: {
		profile: "download-memory-v3",
		readerJsHeap: { samples: memorySamples(100) },
		writerJsHeap: { samples: memorySamples(200) },
		hostRss: { samples: memorySamples(300, true) },
	},
	resourceEvidence: {
		schemaVersion: 2,
		snapshots: {
			beforeTimedRead: {
				writer: { runtime: runtimeSnapshot() },
				reader: { runtime: runtimeSnapshot() },
			},
			afterSink: {
				writer: { runtime: runtimeSnapshot() },
				reader: { runtime: runtimeSnapshot() },
			},
			beforeSoak: {
				writer: { runtime: runtimeSnapshot() },
				reader: { runtime: runtimeSnapshot() },
			},
			afterSoak: {
				writer: { runtime: runtimeSnapshot() },
				reader: {
					runtime: runtimeSnapshot({
						eagerBlocks: {
							telemetryAvailable: true,
							enabled: true,
							telemetry: eagerTelemetry({
								peakEntries: 10,
								peakBytes: 500,
							}),
						},
					}),
				},
			},
		},
		intervals: {
			timedReadEnvelope: {
				writerStorage: storageDelta("writer", 100, 200),
				readerStorage: storageDelta("reader", 300, 400),
				writerEager: eagerDelta(),
				readerEager: eagerDelta({ evictions: 1, rejectedCodec: 0 }),
			},
			postTransferWork: {
				writerStorage: storageDelta("writer", 5, 10),
				readerStorage: storageDelta("reader", 15, 20),
				writerEager: eagerDelta({ admitted: 1, hits: 0, evictions: 0 }),
				readerEager: eagerDelta({ admitted: 0, hits: 1, evictions: 0 }),
			},
			soak: {
				writerStorage: storageDelta("writer", -10, 20),
				readerStorage: storageDelta("reader", -30, 40),
				writerEager: eagerDelta({ evictions: 0, rejectedCid: 0 }),
				readerEager: eagerDelta({ evictions: 0, rejectedCid: 0 }),
			},
			total: {
				writerStorage: storageDelta("writer", 95, 230),
				readerStorage: storageDelta("reader", 285, 460),
				writerEager: eagerDelta({ evictions: 2 }),
				readerEager: eagerDelta({ evictions: 1, rejectedCodec: 0 }),
			},
		},
	},
});

test("summarizes v3 memory, resource, runtime, and eager evidence", () => {
	const summary = summarizeUploadPerformance([runtimeEvidenceResult()]);
	const evidence = summary.runtimeEvidence;
	assert.equal(evidence.memoryPhases.evidenceRunCount, 1);
	assert.equal(
		evidence.memoryPhases.transfer.readerRenderer.usedBytes.peakBytes.avg,
		120,
	);
	assert.equal(
		evidence.memoryPhases.transfer.readerRenderer.embedderHeapUsedBytes
			.endDeltaBytes.avg,
		40,
	);
	assert.equal(
		evidence.memoryPhases.soak.writerRenderer.backingStorageBytes.peakBytes.avg,
		550,
	);
	assert.equal(
		evidence.memoryPhases.transfer.host.rssBytes.endDeltaBytes.avg,
		200,
	);
	assert.equal(
		evidence.memoryPhases.soak.host.nodeExternalBytes.peakBytes.avg,
		2_400,
	);
	assert.equal(
		evidence.memoryPhases.soak.host.nodeArrayBuffersBytes.endDeltaBytes.avg,
		10,
	);

	assert.equal(
		evidence.resourceStorageDeltas.timedReadEnvelope.writer
			.peerbitLogUsageDeltaBytes.avg,
		100,
	);
	assert.equal(
		evidence.resourceStorageDeltas.postTransferWork.reader
			.backingStorageUsageDeltaBytes.avg,
		20,
	);
	assert.equal(
		evidence.resourceStorageDeltas.soak.reader.backingStorageUsageDeltaBytes
			.avg,
		40,
	);
	assert.equal(
		evidence.resourceStorageDeltas.total.writer.peerbitLogUsageDeltaBytes.avg,
		95,
	);

	const runtime = evidence.effectiveRuntimeConfiguration;
	assert.equal(runtime.writer.length, 1);
	assert.equal(runtime.writer[0].runCount, 1);
	assert.deepEqual(runtime.writer[0].configuration, {
		nativeGraph: { active: true, useHeads: false },
		eagerBlocks: {
			telemetryAvailable: true,
			enabled: true,
			limits: eagerTelemetry().limits,
		},
		pubsub: {
			runtimeSnapshotAvailable: true,
			error: null,
			rootUploadLimitBps: 5_000_000,
			nodeUploadLimitBps: 5_000_000,
		},
	});
	assert.equal(
		runtime.cohortKey,
		JSON.stringify({
			writer: runtime.writer[0].configuration,
			reader: runtime.reader[0].configuration,
		}),
	);

	assert.equal(evidence.eagerCache.writer.currentAfterSoak.entries.avg, 8);
	assert.equal(evidence.eagerCache.writer.currentAfterSoak.bytes.avg, 400);
	assert.equal(
		evidence.eagerCache.writer.currentAfterSoak.pendingEntries.avg,
		1,
	);
	assert.equal(
		evidence.eagerCache.writer.currentAfterSoak.pendingBytes.avg,
		100,
	);
	assert.equal(
		evidence.eagerCache.writer.lifetimePeaksAfterSoak.peakBytes.avg,
		1_000,
	);
	assert.equal(
		evidence.eagerCache.reader.lifetimePeaksAfterSoak.peakEntries.avg,
		10,
	);
	assert.equal(evidence.eagerCache.writer.timedReadEnvelope.admitted.avg, 5);
	assert.equal(evidence.eagerCache.writer.timedReadEnvelope.hits.avg, 3);
	assert.equal(evidence.eagerCache.writer.timedReadEnvelope.evictions.avg, 2);
	assert.equal(
		evidence.eagerCache.writer.timedReadEnvelope.rejections.total.avg,
		3,
	);
	assert.equal(evidence.eagerCache.writer.postTransferWork.admitted.avg, 1);
	assert.equal(
		evidence.eagerCache.reader.timedReadEnvelope.rejections.rejectedCodec.avg,
		0,
	);
});

test("excludes terminal teardown samples from soak memory summaries", () => {
	const result = runtimeEvidenceResult();
	const readerSamples = result.downloadMemoryTelemetry.readerJsHeap.samples;
	const manualSample = readerSamples.find(
		(sample) => sample.capturedAt === 4_100,
	);
	manualSample.sampleKind = "manual";
	readerSamples.splice(readerSamples.indexOf(manualSample), 0, {
		capturedAt: 4_050,
		sampleKind: "terminal",
		usedBytes: 9_999_999,
		embedderHeapUsedBytes: 9_999_999,
		backingStorageBytes: 9_999_999,
	});

	const soak = summarizeUploadPerformance([result]).runtimeEvidence.memoryPhases
		.soak.readerRenderer;
	assert.equal(soak.usedBytes.peakBytes.avg, 150);
	assert.equal(soak.usedBytes.endDeltaBytes.avg, 20);

	const missingLiveEndpoint = runtimeEvidenceResult();
	missingLiveEndpoint.downloadMemoryTelemetry.readerJsHeap.samples =
		missingLiveEndpoint.downloadMemoryTelemetry.readerJsHeap.samples.filter(
			(sample) => sample.capturedAt < 4_000,
		);
	assert.equal(
		summarizeUploadPerformance([missingLiveEndpoint]).runtimeEvidence
			.memoryPhases.soak.readerRenderer,
		undefined,
	);
});

test("normalizes a writer-reader runtime pair even when the roles differ", () => {
	const result = runtimeEvidenceResult();
	result.resourceEvidence.snapshots.beforeTimedRead.reader.runtime.nativeGraph.active = false;
	const runtime = summarizeUploadPerformance([result]).runtimeEvidence
		.effectiveRuntimeConfiguration;
	assert.equal(
		runtime.cohortKey,
		JSON.stringify({
			writer: runtime.writer[0].configuration,
			reader: runtime.reader[0].configuration,
		}),
	);
	assert.equal(runtime.writer.length, 1);
	assert.equal(runtime.reader.length, 1);
});

test("groups timing rows by runtime pair in addition to locality", () => {
	const first = runtimeEvidenceResult();
	const samePair = structuredClone(first);
	const differentPair = structuredClone(first);
	differentPair.resourceEvidence.snapshots.beforeTimedRead.writer.runtime.pubsub.snapshot.fanout.root.uploadLimitBps = 4_000_000;
	const legacy = {
		status: "failed",
		mode: "fixed1",
	};
	const groups = groupUploadResultsByLocalityCohort([
		first,
		samePair,
		differentPair,
		legacy,
	]);

	assert.equal(groups.length, 3);
	assert.deepEqual(
		groups.map(({ dimensions, results }) => ({
			cohortKey: dimensions.runtimeConfigurationCohortKey,
			runs: results.length,
		})),
		[
			{
				cohortKey: summarizeUploadPerformance([first]).runtimeEvidence
					.effectiveRuntimeConfiguration.cohortKey,
				runs: 2,
			},
			{
				cohortKey: summarizeUploadPerformance([differentPair]).runtimeEvidence
					.effectiveRuntimeConfiguration.cohortKey,
				runs: 1,
			},
			{ cohortKey: null, runs: 1 },
		],
	);
	assert.equal(
		Object.hasOwn(
			summarizeUploadPerformance([first, differentPair]).runtimeEvidence
				.effectiveRuntimeConfiguration,
			"cohortKey",
		),
		false,
	);
});

const comparableRuntimeModeResults = () => {
	const adaptive = runtimeEvidenceResult();
	adaptive.mode = "adaptive";
	adaptive.uploadDurationMs = 80;
	adaptive.timeToWriterReadyMs = 70;
	adaptive.timeToReaderReadyMs = 120;
	adaptive.libraryStreamWallMs = 40;
	const fixed1 = runtimeEvidenceResult();
	fixed1.mode = "fixed1";
	fixed1.uploadDurationMs = 100;
	fixed1.timeToWriterReadyMs = 80;
	fixed1.timeToReaderReadyMs = 150;
	fixed1.libraryStreamWallMs = 50;
	return { adaptive, fixed1 };
};

test("compares modes only inside one exact runtime pair", () => {
	const { adaptive, fixed1 } = comparableRuntimeModeResults();
	const comparison = compareUploadPerformanceModes([adaptive, fixed1]);
	assert.equal(
		comparison.runtimeConfigurationCohortKey,
		summarizeUploadPerformance([adaptive]).runtimeEvidence
			.effectiveRuntimeConfiguration.cohortKey,
	);
});

test("compares modes only inside one storage and reader-persistence cohort", () => {
	const { adaptive, fixed1 } = comparableRuntimeModeResults();
	for (const result of [adaptive, fixed1]) {
		result.browserStorageMode = "memory";
		result.readerPersistChunkReads = true;
	}

	const comparison = compareUploadPerformanceModes([adaptive, fixed1]);
	assert.deepEqual(
		{
			browserStorageMode: comparison.browserStorageMode,
			readerPersistChunkReads: comparison.readerPersistChunkReads,
		},
		{
			browserStorageMode: "memory",
			readerPersistChunkReads: true,
		},
	);

	for (const [key, value] of [
		["browserStorageMode", "opfs"],
		["readerPersistChunkReads", false],
	]) {
		const mutated = structuredClone(fixed1);
		mutated[key] = value;
		assert.equal(
			compareUploadPerformanceModes([adaptive, mutated]),
			null,
			`comparison must reject a ${key} mutation`,
		);
	}

	for (const key of ["browserStorageMode", "readerPersistChunkReads"]) {
		const missingEvidence = structuredClone(fixed1);
		delete missingEvidence[key];
		assert.equal(
			compareUploadPerformanceModes([adaptive, missingEvidence]),
			null,
			`comparison must reject mixed legacy/current ${key} evidence`,
		);
	}
});

test("rejects a mode comparison across an upload-limit mismatch", () => {
	const { adaptive, fixed1 } = comparableRuntimeModeResults();
	fixed1.resourceEvidence.snapshots.beforeTimedRead.reader.runtime.pubsub.snapshot.fanout.root.uploadLimitBps = 4_999_999;
	assert.equal(compareUploadPerformanceModes([adaptive, fixed1]), null);
});

test("rejects a mode comparison across a native-graph mismatch", () => {
	const { adaptive, fixed1 } = comparableRuntimeModeResults();
	fixed1.resourceEvidence.snapshots.beforeTimedRead.writer.runtime.nativeGraph.active = false;
	assert.equal(compareUploadPerformanceModes([adaptive, fixed1]), null);
});

test("rejects a mode comparison across an eager-limit mismatch", () => {
	const { adaptive, fixed1 } = comparableRuntimeModeResults();
	fixed1.resourceEvidence.snapshots.beforeTimedRead.reader.runtime.eagerBlocks.telemetry.limits.maxBytes -= 1;
	assert.equal(compareUploadPerformanceModes([adaptive, fixed1]), null);
});

test("rejects a mode comparison with mixed runtime-evidence availability", () => {
	const { adaptive, fixed1 } = comparableRuntimeModeResults();
	delete fixed1.resourceEvidence;
	assert.equal(compareUploadPerformanceModes([adaptive, fixed1]), null);
	assert.equal(
		Object.hasOwn(
			summarizeUploadPerformance([adaptive, fixed1]).runtimeEvidence
				.effectiveRuntimeConfiguration,
			"cohortKey",
		),
		false,
	);
});

test("suppresses mode comparisons unless the entire planned cohort passed", () => {
	const passed = [
		{
			status: "passed",
			mode: "adaptive",
			uploadDurationMs: 80,
			timeToWriterReadyMs: 70,
			timeToReaderReadyMs: 120,
			libraryStreamWallMs: 40,
			downloadSink: "hash-only",
		},
		{
			status: "passed",
			mode: "fixed1",
			uploadDurationMs: 100,
			timeToWriterReadyMs: 80,
			timeToReaderReadyMs: 150,
			libraryStreamWallMs: 50,
			downloadSink: "hash-only",
		},
	];
	const complete = { planned: 2, completed: 2, passed: 2, failed: 0 };
	assert.equal(isCompletePassedBenchmarkPlan(passed, complete), true);
	assert.deepEqual(
		compareUploadPerformanceModesForCompletePlan(passed, complete),
		compareUploadPerformanceModes(passed),
	);

	const failed = [passed[0], { ...passed[1], status: "failed" }];
	assert.equal(
		compareUploadPerformanceModesForCompletePlan(failed, {
			planned: 2,
			completed: 2,
			passed: 1,
			failed: 1,
		}),
		null,
	);
	assert.equal(
		compareUploadPerformanceModesForCompletePlan([passed[0]], {
			planned: 2,
			completed: 1,
			passed: 1,
			failed: 0,
		}),
		null,
	);
});

test("compares modes using upload and end-to-end readiness", () => {
	assert.deepEqual(
		compareUploadPerformanceModes([
			{
				status: "passed",
				mode: "adaptive",
				uploadDurationMs: 80,
				timeToWriterReadyMs: 70,
				timeToReaderReadyMs: 120,
				libraryStreamWallMs: 40,
				downloadSink: "hash-only",
			},
			{
				status: "passed",
				mode: "fixed1",
				uploadDurationMs: 100,
				timeToWriterReadyMs: 80,
				timeToReaderReadyMs: 150,
				libraryStreamWallMs: 50,
				downloadSink: "hash-only",
			},
		]),
		{
			downloadSink: "hash-only",
			primaryDownloadMetric: "libraryStreamWallMs",
			primaryDownloadAuthoritative: true,
			adaptiveAvgMs: 80,
			fixed1AvgMs: 100,
			deltaMs: -20,
			adaptiveVsFixed1Pct: -20,
			adaptiveWriterReadyAvgMs: 70,
			fixed1WriterReadyAvgMs: 80,
			writerReadyDeltaMs: -10,
			adaptiveVsFixed1WriterReadyPct: -12.5,
			adaptiveReaderReadyAvgMs: 120,
			fixed1ReaderReadyAvgMs: 150,
			readerReadyDeltaMs: -30,
			adaptiveVsFixed1ReaderReadyPct: -20,
			adaptiveLibraryStreamWallAvgMs: 40,
			fixed1LibraryStreamWallAvgMs: 50,
			libraryStreamWallDeltaMs: -10,
			adaptiveVsFixed1LibraryStreamWallPct: -20,
		},
	);
});

test("never compares primary downloads across sink cohorts", () => {
	assert.equal(
		compareUploadPerformanceModes([
			{
				status: "passed",
				mode: "adaptive",
				downloadSink: "hash-only",
				uploadDurationMs: 80,
				timeToWriterReadyMs: 70,
				timeToReaderReadyMs: 120,
				libraryStreamWallMs: 40,
			},
			{
				status: "passed",
				mode: "fixed1",
				downloadSink: "opfs",
				uploadDurationMs: 100,
				timeToWriterReadyMs: 80,
				timeToReaderReadyMs: 150,
				libraryStreamWallMs: 50,
			},
		]),
		null,
	);
});
