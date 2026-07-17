import assert from "node:assert/strict";
import test from "node:test";
import {
	compareUploadPerformanceModes,
	summarizeUploadPerformance,
	uploadTimingTableColumns,
} from "./benchmark-summary.mjs";

test("propagates end-to-end readiness and labels post-settlement listing", () => {
	const results = [
		{
			status: "passed",
			uploadDurationMs: 80,
			timeToWriterReadyMs: 100,
			timeToReaderReadyMs: 140,
			listingDurationMs: 20,
			downloadDurationMs: 50,
		},
		{
			status: "passed",
			uploadDurationMs: 120,
			timeToWriterReadyMs: 200,
			timeToReaderReadyMs: 260,
			listingDurationMs: 40,
			downloadDurationMs: 70,
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
	});
	assert.deepEqual(uploadTimingTableColumns(results[0]), {
		timeToWriterReadyMs: 100,
		timeToReaderReadyMs: 140,
		postSettlementListingMs: 20,
	});
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
			},
			{
				status: "passed",
				mode: "fixed1",
				uploadDurationMs: 100,
				timeToWriterReadyMs: 80,
				timeToReaderReadyMs: 150,
			},
		]),
		{
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
		},
	);
});
