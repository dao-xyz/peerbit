import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { LIFECYCLE_CENSUS_NAME } from "./shared-log-lifecycle-census-lib.mjs";

const SCRIPT = fileURLToPath(
	new URL("./shared-log-lifecycle-census.mjs", import.meta.url),
);

test("round-trips a trimmed real Peerbit store in fresh processes", async () => {
	const directory = await mkdtemp(join(os.tmpdir(), "lifecycle-census-test-"));
	try {
		const output = join(directory, "report.json");
		const child = spawnSync(
			process.execPath,
			[
				SCRIPT,
				"--history-count",
				"12",
				"--retain",
				"4",
				"--batch-size",
				"4",
				"--runs",
				"1",
				"--compact-max-journal-records",
				"1",
				"--output",
				output,
				"--json",
			],
			{ encoding: "utf8", env: process.env, maxBuffer: 5 * 1024 * 1024 },
		);
		assert.equal(child.status, 0, child.stderr || child.stdout);
		const report = JSON.parse(child.stdout);
		assert.deepEqual(JSON.parse(await readFile(output, "utf8")), report);
		assert.equal(report.name, LIFECYCLE_CENSUS_NAME);
		assert.equal(report.progress.complete, true);
		assert.deepEqual(report.meta.coordinateCompaction, {
			maxJournalBytes: null,
			maxJournalRecords: 1,
		});
		assert.equal(report.rows.length, 1);
		const [row] = report.rows;
		assert.equal(row.fresh.seed.count, 4);
		assert.equal(row.fresh.seed.documentOffset, 8);
		assert.equal(row.history.seed.count, 12);
		assert.equal(row.history.seed.documentOffset, 0);
		assert.deepEqual(row.fresh.reopen.state.probe.firstDocument, {
			id: "doc-8",
			name: "value-8",
		});
		assert.deepEqual(
			row.fresh.reopen.state.probe.oldestRetained,
			row.history.reopen.state.probe.oldestRetained,
		);
		assert.deepEqual(
			row.fresh.reopen.state.probe.newestRetained,
			row.history.reopen.state.probe.newestRetained,
		);
		assert.equal(
			row.fresh.reopen.state.documentsFingerprint,
			row.history.reopen.state.documentsFingerprint,
		);
		assert.equal(row.history.seed.state.logRows, 4);
		for (const measurement of [
			row.fresh.seed,
			row.fresh.reopen,
			row.history.seed,
			row.history.reopen,
		]) {
			assert.ok(
				measurement.disk.categories.coordinateCheckpoint?.files > 0,
				"compaction should leave a classified coordinate checkpoint",
			);
			assert.equal(measurement.state.enumeratedDocumentRows, 4);
			assert.equal(measurement.state.retainedLowerShallowMissing, 0);
			assert.equal(measurement.state.retainedNativeGraphMissing, 0);
			assert.equal(measurement.state.retainedDurableBlockMissing, 0);
			assert.equal(measurement.state.unexpectedDocumentRows, 0);
			assert.equal(measurement.state.missingDocumentRows, 0);
			assert.equal(measurement.state.duplicateDocumentRows, 0);
			assert.equal(
				measurement.state.documentsFingerprint,
				measurement.state.expectedDocumentsFingerprint,
			);
		}
		assert.equal(row.history.seed.state.gidHistoryRows, 4);
		assert.equal(row.history.reopen.state.gidHistoryRows, 0);
		assert.equal(row.history.reopen.state.probe.lowerShallow, false);
		assert.equal(row.history.reopen.state.probe.durableBlock, false);
		assert.equal(row.history.reopen.state.probe.firstDocument, null);
		assert.equal(row.history.reopen.state.probe.retained.lowerShallow, true);
		assert.equal(row.history.reopen.state.probe.retained.durableBlock, true);
		assert.equal(row.comparison.liveStateMatchesFresh, true);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
