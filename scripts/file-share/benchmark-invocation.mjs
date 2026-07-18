import path from "node:path";
import { isDeepStrictEqual } from "node:util";

export const BENCHMARK_INVOCATION_SCHEMA = {
	id: "peerbit-file-share-benchmark-invocation",
	version: 3,
};

export const TINY_FILE_CUTOFF_BYTES = 5_000_000;
export const BENCHMARK_SERVER_MODE = "production-preview";
export const BENCHMARK_SERVER_HOST = "127.0.0.1";
export const BENCHMARK_DOWNLOAD_SINKS = Object.freeze([
	"hash-only",
	"opfs",
	"node-file",
]);
export const DEFAULT_BENCHMARK_DOWNLOAD_SINK = "hash-only";
export const MAX_READER_LOCAL_CHUNK_OVERSHOOT = 8;

const DEPLOYED_APP_HARNESS_MESSAGE =
	"The locally instrumented core benchmark harness cannot use --base-url because it cannot attribute an external deployment to the selected Peerbit checkout; use a dedicated deployed-app benchmark harness that records deployment provenance";

const DEFAULTS = {
	uploadTimeoutMs: 600_000,
	postUploadMonitorMs: 5_000,
	pollMs: 1_000,
	readyTimeoutMs: 180_000,
	sampleMs: 15_000,
	sampleCount: 4,
	targetSeeders: 2,
};

const asNullableString = (value) => {
	if (value == null || value === "") {
		return null;
	}
	return String(value);
};

export const resolveBenchmarkDownloadSink = (requestedSink, { scenario }) => {
	const sink = asNullableString(requestedSink);
	if (scenario !== "upload") {
		if (sink !== null) {
			throw new Error("--download-sink is only supported by upload benchmarks");
		}
		return null;
	}
	const resolved = sink ?? DEFAULT_BENCHMARK_DOWNLOAD_SINK;
	if (!BENCHMARK_DOWNLOAD_SINKS.includes(resolved)) {
		throw new Error(
			`Unsupported benchmark download sink ${JSON.stringify(resolved)}; expected one of ${BENCHMARK_DOWNLOAD_SINKS.join(", ")}`,
		);
	}
	return resolved;
};

export const resolveBenchmarkPreviewProtocol = (requestedProtocol) => {
	const protocol = asNullableString(requestedProtocol) ?? "http";
	if (protocol !== "http") {
		throw new Error(
			`The local production preview only serves HTTP; unsupported benchmark preview protocol ${JSON.stringify(protocol)}`,
		);
	}
	return protocol;
};

export const resolveBenchmarkPreviewOptions = ({
	protocol,
	viteMode,
	viteConfig,
} = {}) => {
	const normalizedViteMode = asNullableString(viteMode);
	if (normalizedViteMode !== null) {
		throw new Error(
			"--vite-mode is not supported by PW_BENCH=1 because the production preview command does not consume it",
		);
	}
	const normalizedViteConfig = asNullableString(viteConfig);
	if (normalizedViteConfig !== null) {
		throw new Error(
			"--vite-config is not supported by PW_BENCH=1 because the production build does not consume it and the preview command cannot safely attribute a partial custom configuration",
		);
	}
	return {
		protocol: resolveBenchmarkPreviewProtocol(protocol),
		viteMode: null,
		viteConfig: null,
	};
};

export const assertCoreBenchmarkUsesLocalApp = (baseUrl) => {
	if (asNullableString(baseUrl) !== null) {
		throw new Error(DEPLOYED_APP_HARNESS_MESSAGE);
	}
	return null;
};

const requireNonNegativeSafeInteger = (value, label) => {
	if (!Number.isSafeInteger(value) || value < 0) {
		throw new Error(`${label} must be a non-negative safe integer`);
	}
	return value;
};

const requirePositiveSafeInteger = (value, label) => {
	if (!Number.isSafeInteger(value) || value <= 0) {
		throw new Error(`${label} must be a positive safe integer`);
	}
	return value;
};

export const assertBenchmarkFileSize = ({ scenario, fileMb }) => {
	const sizeBytes = fileMb * 1024 * 1024;
	if (
		typeof fileMb !== "number" ||
		!Number.isFinite(fileMb) ||
		fileMb <= 0 ||
		!Number.isSafeInteger(sizeBytes)
	) {
		throw new Error(
			"--file-mb must resolve to a positive safe integer byte count",
		);
	}
	if (scenario === "upload" && sizeBytes <= TINY_FILE_CUTOFF_BYTES) {
		throw new Error(
			`Upload benchmarks must exceed the ${TINY_FILE_CUTOFF_BYTES.toLocaleString("en-US")} byte TinyFile cutoff; choose a larger --file-mb value`,
		);
	}
	return sizeBytes;
};

const normalizeLocalPackages = (localPackages) => {
	if (!Array.isArray(localPackages)) {
		throw new Error("localPackages must be an array");
	}
	const normalized = localPackages.map((name) => {
		if (typeof name !== "string" || name.trim().length === 0) {
			throw new Error("localPackages must contain non-empty package names");
		}
		return name.trim();
	});
	if (new Set(normalized).size !== normalized.length) {
		throw new Error("localPackages must not contain duplicates");
	}
	return normalized.toSorted((left, right) => left.localeCompare(right));
};

/**
 * Resolve every benchmark input before Playwright starts. Templates receive this
 * exact object and results must echo it, so defaults and optional CLI knobs are
 * part of the validity contract rather than ambient process state.
 */
export const createBenchmarkInvocation = ({
	scenario,
	mode,
	network,
	integrationMode,
	fileMb,
	fixtureSeed,
	downloadSink,
	uploadTimeoutMs,
	downloadTimeoutMs,
	postUploadMonitorMs,
	pollMs,
	minReadySeeders,
	readyTimeoutMs,
	sampleMs,
	sampleCount,
	targetSeeders,
	readerLocalChunkTarget,
	readerLocalChunkMaxOvershoot,
	baseUrl,
	protocol,
	viteMode,
	viteConfig,
	localPackages = [],
	enableVisibilityProbe = false,
	verbose = false,
}) => {
	if (!["upload", "seeder-probe"].includes(scenario)) {
		throw new Error(`Unsupported benchmark scenario ${String(scenario)}`);
	}
	if (!["adaptive", "fixed1", "observer"].includes(mode)) {
		throw new Error(`Unsupported benchmark mode ${String(mode)}`);
	}
	if (!["local", "remote"].includes(network)) {
		throw new Error(`Unsupported benchmark network ${String(network)}`);
	}
	if (!["none", "link"].includes(integrationMode)) {
		throw new Error(
			`Unsupported benchmark integration mode ${String(integrationMode)}`,
		);
	}
	const resolvedBaseUrl = assertCoreBenchmarkUsesLocalApp(baseUrl);
	const previewOptions = resolveBenchmarkPreviewOptions({
		protocol,
		viteMode,
		viteConfig,
	});
	const sizeBytes = assertBenchmarkFileSize({ scenario, fileMb });
	const resolvedDownloadSink = resolveBenchmarkDownloadSink(downloadSink, {
		scenario,
	});
	if (typeof fixtureSeed !== "string" || fixtureSeed.length === 0) {
		throw new Error("fixtureSeed must be a non-empty string");
	}

	const resolvedUploadTimeoutMs = requirePositiveSafeInteger(
		uploadTimeoutMs ?? DEFAULTS.uploadTimeoutMs,
		"uploadTimeoutMs",
	);
	const resolvedDownloadTimeoutMs = requirePositiveSafeInteger(
		downloadTimeoutMs ?? resolvedUploadTimeoutMs,
		"downloadTimeoutMs",
	);
	const resolvedPostUploadMonitorMs = requireNonNegativeSafeInteger(
		postUploadMonitorMs ?? DEFAULTS.postUploadMonitorMs,
		"postUploadMonitorMs",
	);
	const resolvedPollMs = requirePositiveSafeInteger(
		pollMs ?? DEFAULTS.pollMs,
		"pollMs",
	);
	const resolvedReaderLocalChunkTarget =
		readerLocalChunkTarget == null
			? null
			: requireNonNegativeSafeInteger(
					readerLocalChunkTarget,
					"readerLocalChunkTarget",
				);
	const resolvedReaderLocalChunkMaxOvershoot =
		readerLocalChunkMaxOvershoot == null
			? null
			: requireNonNegativeSafeInteger(
					readerLocalChunkMaxOvershoot,
					"readerLocalChunkMaxOvershoot",
				);
	if (
		(resolvedReaderLocalChunkTarget === null) !==
		(resolvedReaderLocalChunkMaxOvershoot === null)
	) {
		throw new Error(
			"readerLocalChunkTarget and readerLocalChunkMaxOvershoot must be provided together",
		);
	}
	if (
		resolvedReaderLocalChunkMaxOvershoot !== null &&
		resolvedReaderLocalChunkMaxOvershoot > MAX_READER_LOCAL_CHUNK_OVERSHOOT
	) {
		throw new Error(
			`readerLocalChunkMaxOvershoot must not exceed ${MAX_READER_LOCAL_CHUNK_OVERSHOOT}`,
		);
	}
	const resolvedMinReadySeeders = requireNonNegativeSafeInteger(
		minReadySeeders ??
			(mode === "observer"
				? 0
				: resolvedReaderLocalChunkTarget === null
					? 2
					: 1),
		"minReadySeeders",
	);
	const resolvedReadyTimeoutMs = requirePositiveSafeInteger(
		readyTimeoutMs ?? DEFAULTS.readyTimeoutMs,
		"readyTimeoutMs",
	);
	const resolvedSampleMs = requirePositiveSafeInteger(
		sampleMs ?? DEFAULTS.sampleMs,
		"sampleMs",
	);
	const resolvedSampleCount = requirePositiveSafeInteger(
		sampleCount ?? DEFAULTS.sampleCount,
		"sampleCount",
	);
	const resolvedTargetSeeders = requireNonNegativeSafeInteger(
		targetSeeders ?? DEFAULTS.targetSeeders,
		"targetSeeders",
	);
	if (resolvedReaderLocalChunkTarget !== null) {
		if (scenario !== "upload") {
			throw new Error(
				"readerLocalChunkTarget is only supported by upload benchmarks",
			);
		}
		if (mode !== "fixed1") {
			throw new Error(
				"readerLocalChunkTarget requires fixed1 mode for the writer baseline",
			);
		}
		if (resolvedMinReadySeeders !== 1) {
			throw new Error(
				"readerLocalChunkTarget requires minReadySeeders = 1 because the reader starts as an observer",
			);
		}
	}

	return {
		schema: BENCHMARK_INVOCATION_SCHEMA,
		scenario,
		mode,
		networkMode: network,
		integrationMode,
		fileSizeMb: fileMb,
		fileSizeBytes: sizeBytes,
		fixtureSeed,
		downloadSink: resolvedDownloadSink,
		uploadTimeoutMs: resolvedUploadTimeoutMs,
		downloadTimeoutMs: resolvedDownloadTimeoutMs,
		postUploadMonitorMs: resolvedPostUploadMonitorMs,
		pollMs: resolvedPollMs,
		minReadySeeders: resolvedMinReadySeeders,
		readyTimeoutMs: resolvedReadyTimeoutMs,
		sampleMs: resolvedSampleMs,
		sampleCount: resolvedSampleCount,
		targetSeeders: resolvedTargetSeeders,
		readerLocalChunkTarget: resolvedReaderLocalChunkTarget,
		readerLocalChunkMaxOvershoot: resolvedReaderLocalChunkMaxOvershoot,
		baseUrl: resolvedBaseUrl,
		protocol: previewOptions.protocol,
		viteMode: previewOptions.viteMode,
		viteConfig: previewOptions.viteConfig,
		localPackages: normalizeLocalPackages(localPackages),
		serverMode: BENCHMARK_SERVER_MODE,
		serverHost: BENCHMARK_SERVER_HOST,
		enableVisibilityProbe: Boolean(enableVisibilityProbe),
		verbose: Boolean(verbose),
	};
};

const stringValue = (value) => (value == null ? "" : String(value));

/**
 * Strip every PW_* variable, including future ones unknown to this harness, then
 * install the complete effective benchmark contract. This prevents a caller's
 * shell from changing the app URL, Vite mode, timeouts, or validation knobs.
 */
export const createPlaywrightBenchmarkEnvironment = ({
	baseEnvironment = process.env,
	invocation,
	resultFile,
	runNonce,
	provenance,
}) => {
	const environment = {};
	for (const [key, value] of Object.entries(baseEnvironment)) {
		if (!key.startsWith("PW_") && key !== "E2E_PORT" && key !== "PORT") {
			environment[key] = value;
		}
	}
	Object.assign(environment, {
		HOST: BENCHMARK_SERVER_HOST,
		PW_BASE_URL: stringValue(invocation.baseUrl),
		PW_BENCH: "1",
		PW_BENCHMARK_INVOCATION: JSON.stringify(invocation),
		PW_BENCHMARK_PROVENANCE: JSON.stringify(provenance),
		PW_BENCHMARK_RUN_NONCE: runNonce,
		PW_BENCHMARK_SCENARIO: invocation.scenario,
		PW_DOWNLOAD_SINK: stringValue(invocation.downloadSink),
		PW_DOWNLOAD_TIMEOUT_MS: String(invocation.downloadTimeoutMs),
		PW_ENABLE_VISIBILITY_PROBE: invocation.enableVisibilityProbe ? "1" : "0",
		PW_FILE_MB: String(invocation.fileSizeMb),
		PW_FIXTURE_SEED: invocation.fixtureSeed,
		PW_IGNORE_HTTPS_ERRORS: "0",
		PW_MIN_READY_SEEDERS: String(invocation.minReadySeeders),
		PW_NETWORK_MODE: invocation.networkMode,
		PW_POLL_MS: String(invocation.pollMs),
		PW_POST_UPLOAD_MONITOR_MS: String(invocation.postUploadMonitorMs),
		PW_PROTOCOL: stringValue(invocation.protocol),
		PW_READY_TIMEOUT_MS: String(invocation.readyTimeoutMs),
		PW_REPLICATION_MODE: invocation.mode,
		PW_RESULT_FILE: resultFile,
		PW_SAMPLE_COUNT: String(invocation.sampleCount),
		PW_SAMPLE_MS: String(invocation.sampleMs),
		PW_TARGET_SEEDERS: String(invocation.targetSeeders),
		PW_READER_LOCAL_CHUNK_TARGET: stringValue(
			invocation.readerLocalChunkTarget,
		),
		PW_READER_LOCAL_CHUNK_MAX_OVERSHOOT: stringValue(
			invocation.readerLocalChunkMaxOvershoot,
		),
		PW_UPLOAD_TIMEOUT_MS: String(invocation.uploadTimeoutMs),
		PW_VERBOSE: invocation.verbose ? "1" : "0",
		PW_VITE_CONFIG: stringValue(invocation.viteConfig),
		PW_VITE_MODE: stringValue(invocation.viteMode),
	});
	return environment;
};

export const assertMatrixInvocationUsesLocalApp = (invocation) => {
	assertCoreBenchmarkUsesLocalApp(invocation.baseUrl);
};

export const createNonceIsolatedResultPath = ({ resultsDir, runNonce }) => {
	if (typeof runNonce !== "string" || runNonce.length === 0) {
		throw new Error("A run nonce is required for an isolated result path");
	}
	return path.join(resultsDir, "invocations", runNonce, "result.json");
};

export const assertInvocationUnchanged = (actual, expected, label) => {
	if (!isDeepStrictEqual(actual, expected)) {
		throw new Error(`${label} changed while the benchmark was running`);
	}
};
