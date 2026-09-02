import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { CUT_LIFECYCLE_CENSUS_NAME } from "./shared-log-cut-lifecycle-census-lib.mjs";

const SCRIPT = fileURLToPath(
	new URL("./shared-log-cut-lifecycle-census.mjs", import.meta.url),
);

test("reopens matched empty CUT histories in separate processes", async () => {
	const directory = await mkdtemp(join(os.tmpdir(), "cut-census-"));
	try {
		const output = join(directory, "report.json");
		const child = spawnSync(
			process.execPath,
			[
				SCRIPT,
				..."--history-operations 12 --key-count 2 --batch-size 2 --runs 2 --compact-max-journal-records 1".split(
					" ",
				),
				"--output",
				output,
				"--json",
			],
			{ encoding: "utf8", env: process.env, maxBuffer: 10 * 1024 * 1024 },
		);
		assert.equal(child.status, 0, child.stderr || child.stdout);
		const report = JSON.parse(child.stdout);
		assert.deepEqual(JSON.parse(await readFile(output, "utf8")), report);
		assert.deepEqual(
			[report.name, report.progress.complete, report.rows.length],
			[CUT_LIFECYCLE_CENSUS_NAME, true, 2],
		);
		const [row, reverse] = report.rows;
		assert.deepEqual(row.executionOrder, ["fresh", "history"]);
		assert.deepEqual(reverse.executionOrder, ["history", "fresh"]);
		assert.deepEqual(
			[
				row.fresh.reopen.state.logRows,
				row.history.reopen.state.logRows,
				row.comparison.logicalHistory.logRowOverhead,
				row.comparison.logicalHistory.cutHeadOverhead,
				row.comparison.visibleStateMatchesFresh,
			],
			[4, 12, 8, 4, true],
		);
		for (const side of [row.fresh, row.history]) {
			for (const measurement of [side.seed, side.reopen]) {
				assert.equal(measurement.state.documentRows, 0);
				assert.equal(measurement.state.debt.nonzero.length, 0);
				assert.deepEqual(measurement.state.debt.unobserved, []);
				for (const phase of ["beforeOpen", "afterOpen", "afterValidation"]) {
					assert.ok(measurement.memory[phase].rss > 0);
				}
				assert.ok(measurement.throughClosePeakRssBytes > 0);
				assert.ok(measurement.disk.categories.coordinateCheckpoint?.files > 0);
			}
			assert.equal(side.reopen.validation.baselineInvariantsPassed, true);
			assert.ok(side.validation.stableFieldsMatchAfterProcessColdReopen);
		}
		assert.ok(!("duplicateReplayIdempotent" in row.fresh.reopen.validation));
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
