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
		for (const required of [
			"getLightweightSnapshot",
			"diagnostics?.programAddress",
			"diagnostics.programClosed === false",
			"await hooks.setReplicationRole(role)",
			"timeout: READY_TIMEOUT_MS",
		]) {
			assert.ok(
				contents.includes(required),
				`${name} must wait for a live program before applying a role`,
			);
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
		"timeToWriterReadyMs",
		"timeToReaderReadyMs",
		"writerListedAt - uploadStartedAt",
		"readerListedAt - uploadStartedAt",
		"readyTimeoutMs: READY_TIMEOUT_MS",
		"READY_TIMEOUT_MS +",
		"POST_MONITOR_SCHEDULING_TOLERANCE_MS",
		"TRANSFER_TIMEOUT_SCHEDULING_TOLERANCE_MS",
		"Measured transfer duration exceeded",
	]) {
		assert.ok(contents.includes(required), `missing ${required}`);
	}
	assert.ok(
		contents.includes(
			"const MIN_READY_SEEDERS = Number(process.env.PW_MIN_READY_SEEDERS)",
		),
		"the template must consume the resolved invocation value",
	);
	assert.ok(
		!contents.includes('MODE === "adaptive" ? "2" : "0"'),
		"the template must not redefine mode-specific ready-seeder defaults",
	);
	assert.ok(
		!contents.includes("MIN_READY_SEEDERS, 180_000"),
		"the upload probe must honor the invocation readiness timeout",
	);
	for (const peer of ["writer", "reader"]) {
		assert.match(
			contents,
			new RegExp(
				`expectSeedersAtLeast\\(\\s*${peer},\\s*MIN_READY_SEEDERS,\\s*READY_TIMEOUT_MS,?\\s*\\)`,
			),
			`${peer} readiness must use the invocation timeout`,
		);
	}
});
