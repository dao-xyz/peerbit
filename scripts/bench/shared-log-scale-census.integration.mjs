import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
	SCALE_CENSUS_NAME,
	SCALE_CENSUS_PERSISTENT_SCENARIOS,
} from "./shared-log-scale-census-lib.mjs";

const SCALE_CENSUS_SCRIPT = fileURLToPath(
	new URL("./shared-log-scale-census.mjs", import.meta.url),
);

test("round-trips compact snapshots through fresh persistent workers", () => {
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
			"--json",
		],
		{ encoding: "utf8", env: process.env, maxBuffer: 1024 * 1024 },
	);
	assert.equal(child.status, 0, child.stderr || child.stdout);
	const report = JSON.parse(child.stdout);
	assert.equal(report.name, SCALE_CENSUS_NAME);
	assert.equal(report.rows.length, SCALE_CENSUS_PERSISTENT_SCENARIOS.length);
	for (const row of report.rows) {
		assert.equal(row.kind, "persistent-reopen");
		assert.equal(row.count, 3);
		assert.equal(row.validation.seededEntries, 3);
		assert.equal(row.validation.reopenedEntries, 3);
		assert.equal(row.validation.logicalFootprintStable, true);
		assert.equal(row.seed.fixture, "indexer-snapshot-file-compact");
	}
});
