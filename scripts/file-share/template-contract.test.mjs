import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { repoRoot } from "./common.mjs";

const templates = [
	"upload-benchmark.local.e2e.spec.ts",
	"seeder-probe.e2e.spec.ts",
];

for (const name of templates) {
	test(`${name} emits the atomic v2 invocation envelope`, async () => {
		const contents = await readFile(
			path.join(repoRoot, "scripts", "file-share", "templates", name),
			"utf8",
		);
		for (const required of [
			'id: "peerbit-file-share-benchmark"',
			"version: 2",
			"PW_BENCHMARK_RUN_NONCE",
			"PW_BENCHMARK_INVOCATION",
			"PW_BENCHMARK_PROVENANCE",
			'process.env.PW_BENCH !== "1"',
			'serverMode: "production-preview"',
			"schema: RESULT_SCHEMA",
			"runNonce: RUN_NONCE",
			"invocation: INVOCATION",
			"provenance: PROVENANCE",
			"await rename(temporaryPath, RESULT_FILE)",
			"await rm(temporaryPath, { force: true })",
		]) {
			assert.ok(contents.includes(required), `missing ${required}`);
		}
		assert.ok(
			contents.indexOf("await writeFile(temporaryPath") <
				contents.indexOf("await rename(temporaryPath, RESULT_FILE)"),
			"result must be fully written before its atomic rename",
		);
		assert.ok(
			contents.indexOf("await rename(temporaryPath, RESULT_FILE)") <
				contents.indexOf("await rm(temporaryPath, { force: true })"),
			"a failed atomic rename must clean its temporary result",
		);
		assert.ok(
			!contents.includes("`error:${error.message}`"),
			"seeder-count failures must reject instead of becoming sample strings",
		);
	});
}

test("seeder probe records enforceable convergence timing evidence", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"seeder-probe.e2e.spec.ts",
		),
		"utf8",
	);
	for (const required of [
		"readyDeadlineAt = probeStartedAt + READY_TIMEOUT_MS",
		"current.writerSeeders >= TARGET_SEEDERS",
		"current.readerSeeders >= TARGET_SEEDERS",
		"probeDurationMs",
		"timeToTargetMs",
		"targetSampleLabel",
		"effectiveSampleIntervalMs",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
});

test("upload probe fails closed and records bounded scheduling tolerances", async () => {
	const contents = await readFile(
		path.join(
			repoRoot,
			"scripts",
			"file-share",
			"templates",
			"upload-benchmark.local.e2e.spec.ts",
		),
		"utf8",
	);
	for (const required of [
		"readSeederCount(writer",
		"readSeederCount(reader",
		"POST_MONITOR_SCHEDULING_TOLERANCE_MS",
		"TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS",
		"Measured transfer duration exceeded",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
});
