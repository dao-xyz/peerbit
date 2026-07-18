import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
	TINY_FILE_CUTOFF_BYTES,
	assertBenchmarkFileSize,
	assertMatrixInvocationUsesLocalApp,
	createBenchmarkInvocation,
	createNonceIsolatedResultPath,
	createPlaywrightBenchmarkEnvironment,
	resolveBenchmarkDownloadSink,
	resolveBenchmarkPreviewOptions,
	resolveBenchmarkPreviewProtocol,
} from "./benchmark-invocation.mjs";

const invocation = (overrides = {}) =>
	createBenchmarkInvocation({
		scenario: "upload",
		mode: "adaptive",
		network: "local",
		integrationMode: "link",
		fileMb: 5,
		fixtureSeed: "deterministic-seed",
		...overrides,
	});

test("resolves every default and requested optional knob", () => {
	const resolved = invocation({
		uploadTimeoutMs: 101,
		downloadTimeoutMs: 202,
		postUploadMonitorMs: 303,
		pollMs: 4,
		minReadySeeders: 5,
		readyTimeoutMs: 606,
		sampleMs: 7,
		sampleCount: 8,
		targetSeeders: 9,
		protocol: "http",
		enableVisibilityProbe: true,
		verbose: true,
		localPackages: ["peerbit", "@peerbit/document"],
	});
	assert.deepEqual(
		{
			downloadSink: resolved.downloadSink,
			uploadTimeoutMs: resolved.uploadTimeoutMs,
			downloadTimeoutMs: resolved.downloadTimeoutMs,
			postUploadMonitorMs: resolved.postUploadMonitorMs,
			pollMs: resolved.pollMs,
			minReadySeeders: resolved.minReadySeeders,
			readyTimeoutMs: resolved.readyTimeoutMs,
			sampleMs: resolved.sampleMs,
			sampleCount: resolved.sampleCount,
			targetSeeders: resolved.targetSeeders,
			readerLocalChunkTarget: resolved.readerLocalChunkTarget,
			readerLocalChunkMaxOvershoot: resolved.readerLocalChunkMaxOvershoot,
			protocol: resolved.protocol,
			viteMode: resolved.viteMode,
			viteConfig: resolved.viteConfig,
			enableVisibilityProbe: resolved.enableVisibilityProbe,
			verbose: resolved.verbose,
			localPackages: resolved.localPackages,
			serverMode: resolved.serverMode,
			serverHost: resolved.serverHost,
		},
		{
			downloadSink: "hash-only",
			uploadTimeoutMs: 101,
			downloadTimeoutMs: 202,
			postUploadMonitorMs: 303,
			pollMs: 4,
			minReadySeeders: 5,
			readyTimeoutMs: 606,
			sampleMs: 7,
			sampleCount: 8,
			targetSeeders: 9,
			readerLocalChunkTarget: null,
			readerLocalChunkMaxOvershoot: null,
			protocol: "http",
			viteMode: null,
			viteConfig: null,
			enableVisibilityProbe: true,
			verbose: true,
			localPackages: ["@peerbit/document", "peerbit"],
			serverMode: "production-preview",
			serverHost: "127.0.0.1",
		},
	);
});

test("uses the same ready-seeder baseline for comparable replication modes", () => {
	assert.equal(invocation({ mode: "adaptive" }).minReadySeeders, 2);
	assert.equal(invocation({ mode: "fixed1" }).minReadySeeders, 2);
	assert.equal(invocation({ mode: "observer" }).minReadySeeders, 0);
	assert.equal(
		invocation({ mode: "fixed1", minReadySeeders: 4 }).minReadySeeders,
		4,
	);
});

test("removes all ambient PW variables and installs the exact invocation", () => {
	const resolved = invocation();
	const environment = createPlaywrightBenchmarkEnvironment({
		baseEnvironment: {
			PATH: "/bin",
			HOST: "0.0.0.0",
			PORT: "9999",
			E2E_PORT: "9998",
			PW_BASE_URL: "https://attacker.invalid",
			PW_FILE_MB: "999",
			PW_DOWNLOAD_SINK: "node-file",
			PW_POST_UPLOAD_MONITOR_MS: "0",
			PW_PORT: "4444",
			PW_PROTOCOL: "https",
			PW_VITE_CONFIG: "injected.config.ts",
			PW_VITE_MODE: "staging",
			PW_FUTURE_BYPASS: "yes",
			PW_BENCH: "1",
		},
		invocation: resolved,
		resultFile: "/tmp/result.json",
		runNonce: "123e4567-e89b-42d3-a456-426614174000",
		provenance: { bound: true },
	});
	assert.equal(environment.PATH, "/bin");
	assert.equal(environment.PORT, undefined);
	assert.equal(environment.E2E_PORT, undefined);
	assert.equal(environment.PW_BASE_URL, "");
	assert.equal(environment.HOST, "127.0.0.1");
	assert.equal(environment.PW_FILE_MB, "5");
	assert.equal(environment.PW_DOWNLOAD_SINK, "hash-only");
	assert.equal(environment.PW_POST_UPLOAD_MONITOR_MS, "5000");
	assert.equal(environment.PW_READER_LOCAL_CHUNK_TARGET, "");
	assert.equal(environment.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT, "");
	assert.equal(environment.PW_FUTURE_BYPASS, undefined);
	assert.equal(environment.PW_PORT, undefined);
	assert.equal(environment.PW_BENCH, "1");
	assert.equal(environment.PW_PROTOCOL, "http");
	assert.equal(environment.PW_VITE_CONFIG, "");
	assert.equal(environment.PW_VITE_MODE, "");
	assert.deepEqual(JSON.parse(environment.PW_BENCHMARK_INVOCATION), resolved);
});

test("binds optional observer-prefix locality control to a fixed1 writer", () => {
	const resolved = invocation({
		mode: "fixed1",
		readerLocalChunkTarget: 0,
		readerLocalChunkMaxOvershoot: 0,
	});
	assert.equal(resolved.readerLocalChunkTarget, 0);
	assert.equal(resolved.readerLocalChunkMaxOvershoot, 0);
	assert.equal(resolved.minReadySeeders, 1);
	const environment = createPlaywrightBenchmarkEnvironment({
		baseEnvironment: {
			PW_READER_LOCAL_CHUNK_TARGET: "attacker",
			PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT: "attacker",
		},
		invocation: resolved,
		resultFile: "/tmp/result.json",
		runNonce: "123e4567-e89b-42d3-a456-426614174000",
		provenance: { bound: true },
	});
	assert.equal(environment.PW_READER_LOCAL_CHUNK_TARGET, "0");
	assert.equal(environment.PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT, "0");
	for (const [overrides, pattern] of [
		[
			{
				mode: "adaptive",
				readerLocalChunkTarget: 4,
				readerLocalChunkMaxOvershoot: 2,
			},
			/requires fixed1 mode/,
		],
		[
			{
				mode: "observer",
				readerLocalChunkTarget: 4,
				readerLocalChunkMaxOvershoot: 2,
			},
			/requires fixed1 mode/,
		],
		[
			{
				mode: "fixed1",
				readerLocalChunkTarget: 4,
				readerLocalChunkMaxOvershoot: 2,
				minReadySeeders: 2,
			},
			/requires minReadySeeders = 1/,
		],
		[
			{
				scenario: "seeder-probe",
				mode: "fixed1",
				fileMb: 1,
				readerLocalChunkTarget: 1,
				readerLocalChunkMaxOvershoot: 1,
			},
			/only supported by upload benchmarks/,
		],
		[
			{ mode: "fixed1", readerLocalChunkTarget: 4 },
			/must be provided together/,
		],
		[
			{ mode: "fixed1", readerLocalChunkMaxOvershoot: 2 },
			/must be provided together/,
		],
		[
			{
				mode: "fixed1",
				readerLocalChunkTarget: 4,
				readerLocalChunkMaxOvershoot: 9,
			},
			/must not exceed 8/,
		],
	]) {
		assert.throws(() => invocation(overrides), pattern);
	}
});

test("defaults to the hash-only sink and validates explicit sink cohorts", () => {
	assert.equal(
		resolveBenchmarkDownloadSink(undefined, { scenario: "upload" }),
		"hash-only",
	);
	for (const sink of ["hash-only", "opfs", "node-file"]) {
		assert.equal(invocation({ downloadSink: sink }).downloadSink, sink);
	}
	assert.throws(
		() => invocation({ downloadSink: "browser-download" }),
		/Unsupported benchmark download sink/,
	);
	assert.equal(
		invocation({ scenario: "seeder-probe", fileMb: 1 }).downloadSink,
		null,
	);
	assert.throws(
		() =>
			invocation({
				scenario: "seeder-probe",
				fileMb: 1,
				downloadSink: "hash-only",
			}),
		/only supported by upload benchmarks/,
	);
});

test("upload benchmarks cannot exercise the TinyFile path", () => {
	const cutoffMb = TINY_FILE_CUTOFF_BYTES / (1024 * 1024);
	assert.throws(
		() => assertBenchmarkFileSize({ scenario: "upload", fileMb: cutoffMb }),
		/5,000,000 byte TinyFile cutoff/,
	);
	assert.throws(
		() => invocation({ fileMb: cutoffMb }),
		/5,000,000 byte TinyFile cutoff/,
	);
	assert.doesNotThrow(() =>
		assertBenchmarkFileSize({ scenario: "upload", fileMb: 5 }),
	);
	assert.doesNotThrow(() =>
		assertBenchmarkFileSize({ scenario: "seeder-probe", fileMb: 1 }),
	);
});

test("production preview has one attributable HTTP configuration", () => {
	assert.equal(resolveBenchmarkPreviewProtocol(undefined), "http");
	assert.equal(resolveBenchmarkPreviewProtocol("http"), "http");
	for (const protocol of ["https", "ws"]) {
		assert.throws(
			() => resolveBenchmarkPreviewProtocol(protocol),
			/local production preview only serves HTTP/,
		);
	}
	assert.deepEqual(resolveBenchmarkPreviewOptions(), {
		protocol: "http",
		viteMode: null,
		viteConfig: null,
	});
	assert.throws(
		() => resolveBenchmarkPreviewOptions({ viteMode: "staging" }),
		/--vite-mode.*production preview command does not consume it/,
	);
	assert.throws(
		() =>
			resolveBenchmarkPreviewOptions({ viteConfig: "vite.config.remote.ts" }),
		/--vite-config.*production build does not consume it/,
	);
	assert.throws(
		() => invocation({ protocol: "https" }),
		/local production preview only serves HTTP/,
	);
	assert.throws(
		() => invocation({ viteMode: "staging" }),
		/--vite-mode.*production preview command does not consume it/,
	);
	assert.throws(
		() => invocation({ viteConfig: "vite.config.remote.ts" }),
		/--vite-config.*production build does not consume it/,
	);
});

test("rejects non-comparable timeout and seeder knobs before Playwright", () => {
	assert.throws(
		() => invocation({ integrationMode: "overlay" }),
		/Unsupported benchmark integration mode overlay/,
	);
	for (const [overrides, pattern] of [
		[{ uploadTimeoutMs: 0 }, /uploadTimeoutMs must be a positive safe integer/],
		[
			{ downloadTimeoutMs: 1.5 },
			/downloadTimeoutMs must be a positive safe integer/,
		],
		[{ pollMs: 0 }, /pollMs must be a positive safe integer/],
		[
			{ postUploadMonitorMs: 0.5 },
			/postUploadMonitorMs must be a non-negative safe integer/,
		],
		[
			{ minReadySeeders: 0.5 },
			/minReadySeeders must be a non-negative safe integer/,
		],
		[{ readyTimeoutMs: 0 }, /readyTimeoutMs must be a positive safe integer/],
		[{ sampleMs: 0 }, /sampleMs must be a positive safe integer/],
		[
			{ targetSeeders: 0.5 },
			/targetSeeders must be a non-negative safe integer/,
		],
	]) {
		assert.throws(() => invocation(overrides), pattern);
	}
	assert.throws(
		() => invocation({ localPackages: ["peerbit", "peerbit"] }),
		/localPackages must not contain duplicates/,
	);
});

test("core harness rejects external app origins without deployment provenance", () => {
	assert.doesNotThrow(() => assertMatrixInvocationUsesLocalApp(invocation()));
	assert.throws(
		() => invocation({ baseUrl: "https://files.example" }),
		/dedicated deployed-app benchmark harness.*deployment provenance/,
	);
	assert.throws(
		() =>
			assertMatrixInvocationUsesLocalApp({
				baseUrl: "https://files.example",
			}),
		/dedicated deployed-app benchmark harness.*deployment provenance/,
	);
});

test("standalone runner rejects external origins before mutating outputs", async () => {
	const temporaryRoot = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-origin-preflight-"),
	);
	const summaryFile = path.join(temporaryRoot, "summary.json");
	await writeFile(summaryFile, "sentinel\n");
	try {
		const child = spawnSync(
			process.execPath,
			[
				path.resolve("scripts/file-share/run-file-share-benchmark.mjs"),
				"--base-url",
				"https://files.example",
				"--summary-file",
				summaryFile,
			],
			{ encoding: "utf8" },
		);
		assert.equal(child.status, 1);
		assert.match(
			child.stderr,
			/dedicated deployed-app benchmark harness.*deployment provenance/,
		);
		assert.equal(await readFile(summaryFile, "utf8"), "sentinel\n");
	} finally {
		await rm(temporaryRoot, { recursive: true, force: true });
	}
});

test("standalone runner rejects invalid integration modes before mutating outputs", async () => {
	const temporaryRoot = await mkdtemp(
		path.join(os.tmpdir(), "peerbit-integration-preflight-"),
	);
	const summaryFile = path.join(temporaryRoot, "summary.json");
	await writeFile(summaryFile, "sentinel\n");
	try {
		for (const [args, error] of [
			[["--integration-mode", "overlay"], /Unsupported --integration-mode/],
			[
				["--download-sink", "browser-download"],
				/Unsupported benchmark download sink/,
			],
			[
				["--integration-mode", "link", "--local-packages", ","],
				/link integration requires at least one local Peerbit package/,
			],
			[
				[
					"--reader-local-chunk-target",
					"1",
					"--reader-local-chunk-max-overshoot",
					"1",
					"--mode",
					"adaptive",
				],
				/requires --scenario upload --mode fixed1/,
			],
			[
				[
					"--reader-local-chunk-target",
					"1",
					"--reader-local-chunk-max-overshoot",
					"1",
					"--mode",
					"fixed1",
					"--min-ready-seeders",
					"2",
				],
				/requires --min-ready-seeders 1/,
			],
		]) {
			const child = spawnSync(
				process.execPath,
				[
					path.resolve("scripts/file-share/run-file-share-benchmark.mjs"),
					...args,
					"--summary-file",
					summaryFile,
				],
				{ encoding: "utf8" },
			);
			assert.equal(child.status, 1);
			assert.match(child.stderr, error);
			assert.equal(await readFile(summaryFile, "utf8"), "sentinel\n");
		}
	} finally {
		await rm(temporaryRoot, { recursive: true, force: true });
	}
});

test("result files are isolated by invocation nonce", () => {
	const first = createNonceIsolatedResultPath({
		resultsDir: "/tmp/results/session-a",
		runNonce: "nonce-a",
	});
	const second = createNonceIsolatedResultPath({
		resultsDir: "/tmp/results/session-a",
		runNonce: "nonce-b",
	});
	assert.notEqual(first, second);
	assert.equal(
		first,
		path.join(
			"/tmp/results/session-a",
			"invocations",
			"nonce-a",
			"result.json",
		),
	);
});
