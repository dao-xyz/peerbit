import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const DOWNLOAD_MEMORY_PROFILE = "download-memory-v2";
export const DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS = 5_000;
export const DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS = 4_000;
export const DOWNLOAD_MEMORY_CLEANUP_TIMEOUT_MS = 9_000;
export const DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS = 30_000;
export const DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS = 30_000;
export const DOWNLOAD_MEMORY_WINDOW_DEFINITION =
	"samplers-armed-after-reader-locality-stabilization-immediately-before-download-click-through-selected-sink-completion";
export const DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES = 4_096;
export const DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE = 2;
export const DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS = 16;
export const DOWNLOAD_MEMORY_MAX_CLEANUP_WARNINGS = 16;
export const DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH = 512;
export const DOWNLOAD_MEMORY_MAX_BROWSER_ROLES = 32;
export const DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES = 256;
export const DOWNLOAD_MEMORY_MAX_BROWSER_ROLE_NAME_LENGTH = 128;
export const DOWNLOAD_MEMORY_HOST_ATTRIBUTION =
	"aggregate-rss-with-chromium-process-role-groups";
export const DOWNLOAD_MEMORY_HOST_SCOPE =
	"chromium-processes-and-playwright-worker-node";
export const DOWNLOAD_MEMORY_NODE_SCOPE =
	"playwright-worker-node-including-in-process-local-bootstrap";
export const DOWNLOAD_MEMORY_HOST_ATTRIBUTION_LIMITATIONS =
	"Chromium RSS is grouped by process role and cannot be attributed reliably to the reader or writer page; Node RSS is the Playwright worker process and includes the in-process bootstrap peer in local mode; RSS is not PSS or USS.";

export const calculateDownloadMemoryMaxSamples = ({
	downloadTimeoutMs,
	schedulingToleranceMs,
}) => {
	if (
		!Number.isSafeInteger(downloadTimeoutMs) ||
		downloadTimeoutMs <= 0 ||
		!Number.isSafeInteger(schedulingToleranceMs) ||
		schedulingToleranceMs < 0
	) {
		throw new Error("Download memory sampling requires bounded timeout values");
	}
	return Math.min(
		DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES,
		Math.ceil(
			(downloadTimeoutMs + schedulingToleranceMs) /
				DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		) + DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE,
	);
};

const cloneValue = (value) => structuredClone(value);

const boundedErrorMessage = (error) => {
	const message = error instanceof Error ? error.message : String(error);
	return message.slice(0, DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH);
};

const isOnlyOperationTimeouts = (error) => {
	if (error?.code === "DOWNLOAD_MEMORY_OPERATION_TIMEOUT") {
		return true;
	}
	return (
		error instanceof AggregateError &&
		error.errors.length > 0 &&
		error.errors.every(isOnlyOperationTimeouts)
	);
};

export const withDownloadMemoryOperationDeadline = async (
	operation,
	label,
	timeoutMs = DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	{ onLateResolution } = {},
) => {
	if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
		throw new Error("Memory operation timeout must be a positive safe integer");
	}
	return await new Promise((resolve, reject) => {
		let settled = false;
		const timer = setTimeout(() => {
			if (settled) {
				return;
			}
			settled = true;
			const error = new Error(`${label} exceeded ${timeoutMs}ms`);
			error.code = "DOWNLOAD_MEMORY_OPERATION_TIMEOUT";
			reject(error);
		}, timeoutMs);
		Promise.resolve(operation).then(
			(value) => {
				if (settled) {
					Promise.resolve()
						.then(() => onLateResolution?.(value))
						.catch(() => {});
					return;
				}
				settled = true;
				clearTimeout(timer);
				resolve(value);
			},
			(error) => {
				if (settled) {
					return;
				}
				settled = true;
				clearTimeout(timer);
				reject(error);
			},
		);
	});
};

/**
 * Runs one sample at a time, reserves one bounded slot for the forced terminal
 * sample, and makes stop idempotent. Errors are evidence rather than detached
 * promise rejections, and are themselves bounded so a failed probe cannot make
 * the benchmark result grow without limit.
 */
export const startBoundedSerialSampler = async ({
	intervalMs,
	maxSamples,
	readSample,
	cleanup = async () => {},
	operationTimeoutMs = DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	cleanupTimeoutMs = DOWNLOAD_MEMORY_CLEANUP_TIMEOUT_MS,
	now = Date.now,
	setTimer = setTimeout,
	clearTimer = clearTimeout,
}) => {
	if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0) {
		throw new Error("Serial sampler interval must be a positive safe integer");
	}
	if (!Number.isSafeInteger(maxSamples) || maxSamples < 2) {
		throw new Error("Serial sampler must reserve initial and final samples");
	}
	if (typeof readSample !== "function" || typeof cleanup !== "function") {
		throw new Error("Serial sampler requires read and cleanup functions");
	}
	if (
		!Number.isSafeInteger(operationTimeoutMs) ||
		operationTimeoutMs <= 0 ||
		!Number.isSafeInteger(cleanupTimeoutMs) ||
		cleanupTimeoutMs <= 0
	) {
		throw new Error("Serial sampler operation deadlines must be positive");
	}

	const startedAt = now();
	const samples = [];
	const samplingErrors = [];
	const cleanupWarnings = [];
	let samplingErrorOverflowCount = 0;
	let cleanupWarningOverflowCount = 0;
	let finishedAt = null;
	let timer;
	let activeSample;
	let stopped = false;
	let terminalSampleFailure = false;
	let stopSamplingPromise;
	let cleanupPromise;

	const recordError = (error) => {
		if (samplingErrors.length < DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS) {
			samplingErrors.push(boundedErrorMessage(error));
		} else {
			samplingErrorOverflowCount += 1;
		}
	};
	const recordCleanupWarning = (error) => {
		const message = `cleanup-timeout: ${boundedErrorMessage(error)}`.slice(
			0,
			DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH,
		);
		if (cleanupWarnings.length < DOWNLOAD_MEMORY_MAX_CLEANUP_WARNINGS) {
			cleanupWarnings.push(message);
		} else {
			cleanupWarningOverflowCount += 1;
		}
	};

	const takeSample = async (forceTerminal = false) => {
		const limit = forceTerminal ? maxSamples : maxSamples - 1;
		if (samples.length >= limit || terminalSampleFailure) {
			return;
		}
		try {
			const values = await withDownloadMemoryOperationDeadline(
				Promise.resolve().then(() => readSample()),
				"Memory sample",
				operationTimeoutMs,
			);
			if (
				values == null ||
				typeof values !== "object" ||
				Array.isArray(values)
			) {
				throw new Error("Memory sampler returned a non-object sample");
			}
			samples.push({ capturedAt: now(), ...values });
		} catch (error) {
			recordError(error);
			if (error?.code === "DOWNLOAD_MEMORY_OPERATION_TIMEOUT") {
				terminalSampleFailure = true;
			}
		}
	};

	const schedule = () => {
		if (stopped || terminalSampleFailure || samples.length >= maxSamples - 1) {
			return;
		}
		timer = setTimer(() => {
			activeSample = takeSample()
				.catch(recordError)
				.finally(() => {
					activeSample = undefined;
					schedule();
				});
		}, intervalMs);
	};

	const snapshot = () => ({
		startedAt,
		finishedAt,
		samples: cloneValue(samples),
		samplingErrors: [...samplingErrors],
		samplingErrorOverflowCount,
		cleanupWarnings: [...cleanupWarnings],
		cleanupWarningOverflowCount,
	});

	await takeSample();
	schedule();

	const stopSampling = () => {
		if (stopSamplingPromise) {
			return stopSamplingPromise;
		}
		stopped = true;
		stopSamplingPromise = (async () => {
			if (timer !== undefined) {
				clearTimer(timer);
				timer = undefined;
			}
			await activeSample;
			await takeSample(true);
			finishedAt = now();
			return snapshot();
		})();
		return stopSamplingPromise;
	};
	const runCleanup = () => {
		if (cleanupPromise) {
			return cleanupPromise;
		}
		cleanupPromise = (async () => {
			await stopSampling();
			try {
				await withDownloadMemoryOperationDeadline(
					Promise.resolve().then(() => cleanup()),
					"Memory sampler cleanup",
					cleanupTimeoutMs,
				);
			} catch (error) {
				if (isOnlyOperationTimeouts(error)) {
					recordCleanupWarning(error);
				} else {
					recordError(error);
				}
			}
			return snapshot();
		})();
		return cleanupPromise;
	};

	return {
		snapshot,
		stopSampling,
		cleanup: runCleanup,
		stop: runCleanup,
	};
};

const summarizeHeap = (scope, state) => {
	const usedBytes = state.samples.map((sample) => sample.usedBytes);
	return {
		memoryKind: "javascript-heap",
		scope,
		metric: "JSHeapUsedSize",
		unit: "bytes",
		sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		startedAt: state.startedAt,
		finishedAt: state.finishedAt,
		sampleCount: state.samples.length,
		startBytes: usedBytes[0] ?? null,
		endBytes: usedBytes.at(-1) ?? null,
		peakBytes: usedBytes.length > 0 ? Math.max(...usedBytes) : null,
		samples: state.samples,
		samplingErrors: state.samplingErrors,
		samplingErrorOverflowCount: state.samplingErrorOverflowCount,
		cleanupWarnings: state.cleanupWarnings,
		cleanupWarningOverflowCount: state.cleanupWarningOverflowCount,
	};
};

const startPageJsHeapSampler = async ({
	page,
	scope,
	maxSamples,
	operationTimeoutMs,
	cleanupTimeoutMs,
}) => {
	let session;
	let setupError;
	try {
		session = await withDownloadMemoryOperationDeadline(
			page.context().newCDPSession(page),
			"Page JS heap CDP session creation",
			operationTimeoutMs,
			{
				onLateResolution: async (lateSession) => {
					await withDownloadMemoryOperationDeadline(
						lateSession.detach(),
						"Late page JS heap CDP detach",
						operationTimeoutMs,
					);
				},
			},
		);
		await withDownloadMemoryOperationDeadline(
			session.send("Performance.enable"),
			"Page JS heap Performance.enable",
			operationTimeoutMs,
		);
	} catch (error) {
		setupError = error;
	}
	const sampler = await startBoundedSerialSampler({
		intervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		maxSamples,
		operationTimeoutMs,
		cleanupTimeoutMs,
		readSample: async () => {
			if (setupError) {
				throw setupError;
			}
			if (!session) {
				throw new Error("Page JS heap CDP session is unavailable");
			}
			const response = await session.send("Performance.getMetrics");
			const heapMetric = response.metrics.find(
				(metric) => metric.name === "JSHeapUsedSize",
			);
			if (
				!heapMetric ||
				!Number.isFinite(heapMetric.value) ||
				heapMetric.value < 0
			) {
				throw new Error("Performance.getMetrics omitted JSHeapUsedSize");
			}
			return { usedBytes: heapMetric.value };
		},
		cleanup: async () => {
			if (!session) {
				return;
			}
			const cleanupErrors = [];
			try {
				await withDownloadMemoryOperationDeadline(
					session.send("Performance.disable"),
					"Page JS heap Performance.disable",
					operationTimeoutMs,
				);
			} catch (error) {
				cleanupErrors.push(error);
			}
			try {
				await withDownloadMemoryOperationDeadline(
					session.detach(),
					"Page JS heap CDP detach",
					operationTimeoutMs,
				);
			} catch (error) {
				cleanupErrors.push(error);
			} finally {
				session = undefined;
			}
			if (cleanupErrors.length > 0) {
				throw new AggregateError(
					cleanupErrors,
					cleanupErrors.map(boundedErrorMessage).join("; "),
				);
			}
		},
	});
	return {
		snapshot: () => summarizeHeap(scope, sampler.snapshot()),
		stopSampling: async () =>
			summarizeHeap(scope, await sampler.stopSampling()),
		cleanup: async () => summarizeHeap(scope, await sampler.cleanup()),
		stop: async () => summarizeHeap(scope, await sampler.stop()),
	};
};

const readProcessRssBytes = async (processIds) => {
	if (processIds.length === 0) {
		throw new Error("Chromium did not expose operating-system process IDs");
	}
	const { stdout } = await execFileAsync(
		"ps",
		["-o", "pid=,rss=", "-p", processIds.join(",")],
		{
			maxBuffer: 1024 * 1024,
			timeout: DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
		},
	);
	const processBytes = new Map();
	for (const line of String(stdout).trim().split(/\n+/).filter(Boolean)) {
		const [processId, rssKiB, ...extra] = line.trim().split(/\s+/).map(Number);
		if (
			extra.length !== 0 ||
			!Number.isSafeInteger(processId) ||
			processId <= 0 ||
			!Number.isFinite(rssKiB) ||
			rssKiB <= 0
		) {
			throw new Error("ps did not return valid Chromium RSS values");
		}
		const rssBytes = rssKiB * 1024;
		if (!Number.isSafeInteger(rssBytes) || rssBytes <= 0) {
			throw new Error("ps returned Chromium RSS outside the safe byte range");
		}
		processBytes.set(processId, rssBytes);
	}
	if (processBytes.size === 0) {
		throw new Error("ps did not return Chromium RSS values");
	}
	return processBytes;
};

const summarizeHostRss = (state) => {
	const first = state.samples[0] ?? null;
	const last = state.samples.at(-1) ?? null;
	const peak = (name) =>
		state.samples.length > 0
			? Math.max(...state.samples.map((sample) => sample[name]))
			: null;
	const peakBrowserRoleBytes = {};
	for (const sample of state.samples) {
		for (const [role, bytes] of Object.entries(sample.browserRoleBytes)) {
			peakBrowserRoleBytes[role] = Math.max(
				peakBrowserRoleBytes[role] ?? 0,
				bytes,
			);
		}
	}
	return {
		memoryKind: "resident-set-size",
		scope: DOWNLOAD_MEMORY_HOST_SCOPE,
		nodeScope: DOWNLOAD_MEMORY_NODE_SCOPE,
		metric: "RSS",
		attribution: DOWNLOAD_MEMORY_HOST_ATTRIBUTION,
		attributionLimitations: DOWNLOAD_MEMORY_HOST_ATTRIBUTION_LIMITATIONS,
		unit: "bytes",
		sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		startedAt: state.startedAt,
		finishedAt: state.finishedAt,
		sampleCount: state.samples.length,
		startBrowserBytes: first?.browserBytes ?? null,
		endBrowserBytes: last?.browserBytes ?? null,
		peakBrowserBytes: peak("browserBytes"),
		startNodeBytes: first?.nodeBytes ?? null,
		endNodeBytes: last?.nodeBytes ?? null,
		peakNodeBytes: peak("nodeBytes"),
		startCombinedBytes: first?.combinedBytes ?? null,
		endCombinedBytes: last?.combinedBytes ?? null,
		peakCombinedBytes: peak("combinedBytes"),
		startBrowserProcessCount: first?.browserProcessCount ?? null,
		endBrowserProcessCount: last?.browserProcessCount ?? null,
		peakBrowserProcessCount: peak("browserProcessCount"),
		startBrowserRoleBytes: first ? cloneValue(first.browserRoleBytes) : null,
		endBrowserRoleBytes: last ? cloneValue(last.browserRoleBytes) : null,
		peakBrowserRoleBytes,
		samples: state.samples,
		samplingErrors: state.samplingErrors,
		samplingErrorOverflowCount: state.samplingErrorOverflowCount,
		cleanupWarnings: state.cleanupWarnings,
		cleanupWarningOverflowCount: state.cleanupWarningOverflowCount,
	};
};

const startHostRssSampler = async ({
	browser,
	maxSamples,
	operationTimeoutMs,
	cleanupTimeoutMs,
}) => {
	let session;
	let setupError;
	try {
		session = await withDownloadMemoryOperationDeadline(
			browser.newBrowserCDPSession(),
			"Browser RSS CDP session creation",
			operationTimeoutMs,
			{
				onLateResolution: async (lateSession) => {
					await withDownloadMemoryOperationDeadline(
						lateSession.detach(),
						"Late browser RSS CDP detach",
						operationTimeoutMs,
					);
				},
			},
		);
	} catch (error) {
		setupError = error;
	}
	const sampler = await startBoundedSerialSampler({
		intervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		maxSamples,
		operationTimeoutMs,
		cleanupTimeoutMs,
		readSample: async () => {
			if (setupError) {
				throw setupError;
			}
			if (!session) {
				throw new Error("Browser RSS CDP session is unavailable");
			}
			const { processInfo } = await session.send("SystemInfo.getProcessInfo");
			const processIds = [
				...new Set(
					processInfo
						.map((process) => Number(process.id))
						.filter(
							(processId) => Number.isSafeInteger(processId) && processId > 0,
						),
				),
			];
			if (processIds.length > DOWNLOAD_MEMORY_MAX_BROWSER_PROCESSES) {
				throw new Error("Chromium process count exceeds the telemetry cap");
			}
			const browserProcessBytes = await readProcessRssBytes(processIds);
			const nodeBytes = process.memoryUsage().rss;
			if (!Number.isSafeInteger(nodeBytes) || nodeBytes <= 0) {
				throw new Error("Playwright worker returned invalid Node RSS");
			}
			const browserRoleBytes = {};
			for (const processEntry of processInfo) {
				const bytes = browserProcessBytes.get(Number(processEntry.id));
				if (bytes == null) {
					continue;
				}
				const role = String(processEntry.type || "unknown");
				if (
					role.length === 0 ||
					role.length > DOWNLOAD_MEMORY_MAX_BROWSER_ROLE_NAME_LENGTH ||
					!/^[\x20-\x7e]+$/.test(role)
				) {
					throw new Error("Chromium exposed an invalid process role name");
				}
				browserRoleBytes[role] = (browserRoleBytes[role] ?? 0) + bytes;
			}
			if (
				Object.keys(browserRoleBytes).length === 0 ||
				Object.keys(browserRoleBytes).length > DOWNLOAD_MEMORY_MAX_BROWSER_ROLES
			) {
				throw new Error("Chromium process-role attribution is unbounded");
			}
			const browserBytes = Object.values(browserRoleBytes).reduce(
				(total, bytes) => total + bytes,
				0,
			);
			if (
				!Number.isSafeInteger(browserBytes) ||
				!Number.isSafeInteger(browserBytes + nodeBytes)
			) {
				throw new Error("Host RSS totals exceed the safe byte range");
			}
			return {
				browserBytes,
				nodeBytes,
				combinedBytes: browserBytes + nodeBytes,
				browserProcessCount: browserProcessBytes.size,
				browserRoleBytes,
			};
		},
		cleanup: async () => {
			if (session) {
				try {
					await withDownloadMemoryOperationDeadline(
						session.detach(),
						"Browser RSS CDP detach",
						operationTimeoutMs,
					);
				} finally {
					session = undefined;
				}
			}
		},
	});
	return {
		snapshot: () => summarizeHostRss(sampler.snapshot()),
		stopSampling: async () => summarizeHostRss(await sampler.stopSampling()),
		cleanup: async () => summarizeHostRss(await sampler.cleanup()),
		stop: async () => summarizeHostRss(await sampler.stop()),
	};
};

export const startDownloadMemoryTelemetry = async ({
	browser,
	reader,
	writer,
	downloadTimeoutMs,
	schedulingToleranceMs,
	operationTimeoutMs = DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	cleanupTimeoutMs = DOWNLOAD_MEMORY_CLEANUP_TIMEOUT_MS,
}) => {
	const maxSamplesPerSeries = calculateDownloadMemoryMaxSamples({
		downloadTimeoutMs,
		schedulingToleranceMs,
	});
	const [readerSampler, writerSampler, hostSampler] = await Promise.all([
		startPageJsHeapSampler({
			page: reader,
			scope: "reader-renderer",
			maxSamples: maxSamplesPerSeries,
			operationTimeoutMs,
			cleanupTimeoutMs,
		}),
		startPageJsHeapSampler({
			page: writer,
			scope: "writer-renderer",
			maxSamples: maxSamplesPerSeries,
			operationTimeoutMs,
			cleanupTimeoutMs,
		}),
		startHostRssSampler({
			browser,
			maxSamples: maxSamplesPerSeries,
			operationTimeoutMs,
			cleanupTimeoutMs,
		}),
	]);
	let complete = false;
	let cleanupComplete = false;
	let stopSamplingPromise;
	let cleanupPromise;
	const build = ({ readerJsHeap, writerJsHeap, hostRss }) => ({
		profile: DOWNLOAD_MEMORY_PROFILE,
		sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		windowDefinition: DOWNLOAD_MEMORY_WINDOW_DEFINITION,
		maxSamplesPerSeries,
		complete,
		cleanupComplete,
		startedAt: Math.min(
			readerJsHeap.startedAt,
			writerJsHeap.startedAt,
			hostRss.startedAt,
		),
		finishedAt:
			readerJsHeap.finishedAt == null ||
			writerJsHeap.finishedAt == null ||
			hostRss.finishedAt == null
				? null
				: Math.max(
						readerJsHeap.finishedAt,
						writerJsHeap.finishedAt,
						hostRss.finishedAt,
					),
		readerJsHeap,
		writerJsHeap,
		hostRss,
	});
	const stopSampling = () => {
		if (stopSamplingPromise) {
			return stopSamplingPromise;
		}
		stopSamplingPromise = Promise.all([
			readerSampler.stopSampling(),
			writerSampler.stopSampling(),
			hostSampler.stopSampling(),
		]).then(([readerJsHeap, writerJsHeap, hostRss]) => {
			complete = true;
			return build({ readerJsHeap, writerJsHeap, hostRss });
		});
		return stopSamplingPromise;
	};
	const runCleanup = () => {
		if (cleanupPromise) {
			return cleanupPromise;
		}
		cleanupPromise = Promise.all([
			readerSampler.cleanup(),
			writerSampler.cleanup(),
			hostSampler.cleanup(),
		]).then(([readerJsHeap, writerJsHeap, hostRss]) => {
			complete = true;
			cleanupComplete = true;
			return build({ readerJsHeap, writerJsHeap, hostRss });
		});
		return cleanupPromise;
	};
	return {
		snapshot: () =>
			build({
				readerJsHeap: readerSampler.snapshot(),
				writerJsHeap: writerSampler.snapshot(),
				hostRss: hostSampler.snapshot(),
			}),
		stopSampling,
		cleanup: runCleanup,
		stop: runCleanup,
	};
};
