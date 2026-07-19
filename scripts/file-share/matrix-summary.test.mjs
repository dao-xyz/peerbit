import assert from "node:assert/strict";
import test from "node:test";
import {
	compareAdaptiveAcrossVariants,
	summarizeUploadMatrix,
} from "./run-file-share-benchmark-matrix.mjs";

const uploadRow = ({
	mode,
	runtimeKey,
	uploadDurationMsAvg,
	libraryStreamWallMsAvg,
	readerLocalityCohortKey = null,
}) => ({
	mode,
	...(runtimeKey === undefined
		? {}
		: { runtimeConfigurationCohortKey: runtimeKey }),
	runs: 1,
	passed: 1,
	failed: 0,
	errorCount: 0,
	incompleteErrorCollections: 0,
	requestFailureCount: 0,
	downloadSink: "hash-only",
	primaryDownloadMetric: "libraryStreamWallMs",
	primaryDownloadAuthoritative: true,
	uploadDurationMsAvg,
	timeToWriterReadyMsAvg: uploadDurationMsAvg - 10,
	timeToWriterReadyMsMedian: uploadDurationMsAvg - 10,
	timeToReaderReadyMsAvg: uploadDurationMsAvg + 10,
	timeToReaderReadyMsMedian: uploadDurationMsAvg + 10,
	libraryStreamWallMsAvg,
	libraryStreamWallMsMedian: libraryStreamWallMsAvg,
	readerLocalChunkTarget:
		mode === "fixed1" && readerLocalityCohortKey !== null ? 0 : null,
	readerLocalChunkMaxOvershoot:
		mode === "fixed1" && readerLocalityCohortKey !== null ? 0 : null,
	readerLocalChunkBlockCount:
		mode === "fixed1" && readerLocalityCohortKey !== null ? 0 : null,
	readerLocalChunkIndexRowCount:
		mode === "fixed1" && readerLocalityCohortKey !== null ? 0 : null,
	readerLocalityCohortKey,
});

test("matrix upload summary preserves the legacy row shape", () => {
	const rows = summarizeUploadMatrix([
		{
			variant: "baseline",
			summary: [
				uploadRow({
					mode: "adaptive",
					uploadDurationMsAvg: 80,
					libraryStreamWallMsAvg: 40,
				}),
				uploadRow({
					mode: "fixed1",
					uploadDurationMsAvg: 100,
					libraryStreamWallMsAvg: 50,
				}),
			],
			comparison: {
				downloadSink: "hash-only",
				primaryDownloadMetric: "libraryStreamWallMs",
				primaryDownloadAuthoritative: true,
				adaptiveAvgMs: 80,
				fixed1AvgMs: 100,
				adaptiveVsFixed1Pct: -20,
				libraryStreamWallDeltaMs: -10,
				adaptiveVsFixed1LibraryStreamWallPct: -20,
			},
		},
	]);

	assert.equal(rows.length, 1);
	assert.equal(Object.hasOwn(rows[0], "runtimeConfigurationCohortKey"), false);
	assert.equal(rows[0].adaptiveAvgMs, 80);
	assert.equal(rows[0].fixed1AvgMs, 100);
	assert.equal(rows[0].adaptiveVsFixed1Pct, -20);
});

test("matrix upload summary preserves every legacy controlled-locality row", () => {
	const rows = summarizeUploadMatrix([
		{
			variant: "baseline",
			summary: [
				uploadRow({
					mode: "adaptive",
					uploadDurationMsAvg: 80,
					libraryStreamWallMsAvg: 40,
				}),
				uploadRow({
					mode: "fixed1",
					uploadDurationMsAvg: 100,
					libraryStreamWallMsAvg: 50,
					readerLocalityCohortKey: "observer-prefix-b0-i0",
				}),
				uploadRow({
					mode: "fixed1",
					uploadDurationMsAvg: 110,
					libraryStreamWallMsAvg: 60,
					readerLocalityCohortKey: "observer-prefix-b1-i1",
				}),
			],
			comparison: null,
		},
	]);

	assert.deepEqual(
		rows.map((row) => [
			row.readerLocalityCohortKey,
			row.adaptiveAvgMs,
			row.fixed1AvgMs,
		]),
		[
			["observer-prefix-b0-i0", 80, 100],
			["observer-prefix-b1-i1", 80, 110],
		],
	);
	assert.ok(
		rows.every((row) => !Object.hasOwn(row, "runtimeConfigurationCohortKey")),
	);
});

test("matrix upload summary partitions and preserves every runtime cohort", () => {
	const runtimeA = '{"pubsub":"a"}';
	const runtimeB = '{"pubsub":"b"}';
	const rows = summarizeUploadMatrix([
		{
			variant: "candidate",
			summary: [
				uploadRow({
					mode: "adaptive",
					runtimeKey: runtimeA,
					uploadDurationMsAvg: 80,
					libraryStreamWallMsAvg: 40,
				}),
				uploadRow({
					mode: "fixed1",
					runtimeKey: runtimeA,
					uploadDurationMsAvg: 100,
					libraryStreamWallMsAvg: 50,
				}),
				uploadRow({
					mode: "adaptive",
					runtimeKey: runtimeB,
					uploadDurationMsAvg: 180,
					libraryStreamWallMsAvg: 140,
				}),
				uploadRow({
					mode: "fixed1",
					runtimeKey: runtimeB,
					uploadDurationMsAvg: 200,
					libraryStreamWallMsAvg: 150,
				}),
			],
			comparison: null,
		},
	]);

	assert.deepEqual(
		rows.map((row) => ({
			runtimeKey: row.runtimeConfigurationCohortKey,
			adaptiveAvgMs: row.adaptiveAvgMs,
			fixed1AvgMs: row.fixed1AvgMs,
			adaptiveVsFixed1Pct: row.adaptiveVsFixed1Pct,
		})),
		[
			{
				runtimeKey: runtimeA,
				adaptiveAvgMs: 80,
				fixed1AvgMs: 100,
				adaptiveVsFixed1Pct: null,
			},
			{
				runtimeKey: runtimeB,
				adaptiveAvgMs: 180,
				fixed1AvgMs: 200,
				adaptiveVsFixed1Pct: null,
			},
		],
	);
});

test("matrix upload summary never joins keyed and missing runtime evidence", () => {
	const runtimeA = '{"pubsub":"a"}';
	const rows = summarizeUploadMatrix([
		{
			variant: "candidate",
			summary: [
				uploadRow({
					mode: "adaptive",
					runtimeKey: runtimeA,
					uploadDurationMsAvg: 80,
					libraryStreamWallMsAvg: 40,
				}),
				uploadRow({
					mode: "fixed1",
					uploadDurationMsAvg: 100,
					libraryStreamWallMsAvg: 50,
				}),
			],
			comparison: {
				runtimeConfigurationCohortKey: runtimeA,
				adaptiveAvgMs: 80,
				fixed1AvgMs: 100,
				adaptiveVsFixed1Pct: -20,
			},
		},
	]);

	assert.equal(rows.length, 2);
	assert.deepEqual(
		rows.map((row) => [
			row.runtimeConfigurationCohortKey,
			row.adaptiveAvgMs,
			row.fixed1AvgMs,
			row.adaptiveVsFixed1Pct,
		]),
		[
			[null, null, 100, null],
			[runtimeA, 80, null, null],
		],
	);
});

test("cross-variant adaptive rows require one shared runtime cohort", () => {
	const runtimeA = '{"pubsub":"a"}';
	const summaries = ["baseline", "candidate"].map((variant, index) => ({
		variant,
		summary: [
			uploadRow({
				mode: "adaptive",
				runtimeKey: runtimeA,
				uploadDurationMsAvg: 80 + index,
				libraryStreamWallMsAvg: 40 + index,
			}),
		],
	}));

	const comparison = compareAdaptiveAcrossVariants(summaries, "upload");
	assert.equal(comparison.length, 2);
	assert.ok(
		comparison.every((row) => row.runtimeConfigurationCohortKey === runtimeA),
	);

	const mismatched = structuredClone(summaries);
	mismatched[1].summary[0].runtimeConfigurationCohortKey = '{"pubsub":"b"}';
	assert.equal(compareAdaptiveAcrossVariants(mismatched, "upload"), null);

	const mixed = structuredClone(summaries);
	delete mixed[1].summary[0].runtimeConfigurationCohortKey;
	assert.equal(compareAdaptiveAcrossVariants(mixed, "upload"), null);

	const split = structuredClone(summaries);
	split[0].summary.push(
		uploadRow({
			mode: "adaptive",
			runtimeKey: '{"pubsub":"b"}',
			uploadDurationMsAvg: 180,
			libraryStreamWallMsAvg: 140,
		}),
	);
	assert.equal(compareAdaptiveAcrossVariants(split, "upload"), null);
});

test("cross-variant adaptive rows retain legacy behavior without evidence", () => {
	const comparison = compareAdaptiveAcrossVariants(
		["baseline", "candidate"].map((variant, index) => ({
			variant,
			summary: [
				uploadRow({
					mode: "adaptive",
					uploadDurationMsAvg: 80 + index,
					libraryStreamWallMsAvg: 40 + index,
				}),
			],
		})),
		"upload",
	);

	assert.equal(comparison.length, 2);
	assert.ok(
		comparison.every(
			(row) => !Object.hasOwn(row, "runtimeConfigurationCohortKey"),
		),
	);
});
