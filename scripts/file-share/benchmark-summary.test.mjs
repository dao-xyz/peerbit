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
