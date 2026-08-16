import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
	SCALE_CENSUS_NAME,
	SCALE_CENSUS_PERSISTENT_SCENARIOS,
} from "./shared-log-scale-census-lib.mjs";

const SCALE_CENSUS_SCRIPT = fileURLToPath(
	new URL("./shared-log-scale-census.mjs", import.meta.url),
);

test("checkpoints and round-trips compact snapshots", async () => {
	const directory = await mkdtemp(join(os.tmpdir(), "scale-census-test-"));
	try {
		const output = join(directory, "report.json");
		const child = spawnSync(
			process.execPath,
			[
				SCALE_CENSUS_SCRIPT,
				"--counts",
				"3",
				"--scenarios",
				SCALE_CENSUS_PERSISTENT_SCENARIOS.join(","),
				"--runs",
				"1",
				"--output",
				output,
				"--json",
			],
			{ encoding: "utf8", env: process.env, maxBuffer: 1024 * 1024 },
		);
		assert.equal(child.status, 0, child.stderr || child.stdout);
		const report = JSON.parse(child.stdout);
		assert.deepEqual(JSON.parse(await readFile(output, "utf8")), report);
		assert.equal(report.name, SCALE_CENSUS_NAME);
		assert.deepEqual(report.progress, {
			expectedRows: SCALE_CENSUS_PERSISTENT_SCENARIOS.length,
			completedRows: SCALE_CENSUS_PERSISTENT_SCENARIOS.length,
			complete: true,
			activeRow: null,
		});
		assert.equal(report.rows.length, SCALE_CENSUS_PERSISTENT_SCENARIOS.length);
		for (const row of report.rows) {
			assert.equal(row.kind, "persistent-reopen");
			assert.equal(row.count, 3);
			assert.equal(row.validation.seededEntries, 3);
			assert.equal(row.validation.reopenedEntries, 3);
			assert.equal(row.validation.logicalFootprintStable, true);
			assert.equal(row.seed.fixture, "indexer-snapshot-file-compact");
		}
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
