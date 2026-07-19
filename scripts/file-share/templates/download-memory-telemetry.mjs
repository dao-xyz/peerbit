import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const DOWNLOAD_MEMORY_PROFILE = "download-memory-v3";
export const DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS = 5_000;
export const DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS = 4_000;
export const DOWNLOAD_MEMORY_CLEANUP_TIMEOUT_MS = 9_000;
export const DOWNLOAD_MEMORY_SETUP_ALLOWANCE_MS = 30_000;
export const DOWNLOAD_MEMORY_TERMINAL_ALLOWANCE_MS = 30_000;
export const DOWNLOAD_MEMORY_WINDOW_DEFINITION =
	"samplers-armed-after-any-requested-reader-locality-prefix-stabilization-before-any-requested-bounded-pre-read-transport-counter-gate-and-download-click-through-selected-sink-completion-requested-post-transfer-soak-live-checkpoint-bounded-peer-shutdown-and-terminal-sample";
export const DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION =
	"for each exact transfer or soak phase, the last non-terminal sample at or before phase start through the first non-terminal sample at or after phase finish must exist and every adjacent capturedAt gap must be at most sampleIntervalMs + operationTimeoutMs + schedulingToleranceMs";
export const DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES = 4_096;
export const DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE = 3;
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
	"Chromium RSS is grouped by process role and cannot be attributed reliably to the reader or writer page; Node RSS is the Playwright worker process and includes the in-process bootstrap peer in local mode; RSS is not PSS or USS; Node external and ArrayBuffer bytes are overlapping allocation diagnostics that are not additive with RSS and are never added to the combined RSS total.";

export const calculateDownloadMemoryMaxSamples = ({
	samplingWindowBudgetMs,
}) => {
	if (
		!Number.isSafeInteger(samplingWindowBudgetMs) ||
		samplingWindowBudgetMs <= 0
	) {
		throw new Error(
			"Download memory sampling requires a positive bounded sampling window budget",
		);
	}
	const totalWindowMs = BigInt(samplingWindowBudgetMs);
	const intervalMs = BigInt(DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS);
	const intervalSamples = (totalWindowMs + intervalMs - 1n) / intervalMs;
	return Number(
		intervalSamples + BigInt(DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE) >
			BigInt(DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES)
			? BigInt(DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES)
			: intervalSamples + BigInt(DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE),
	);
};

export const calculateDownloadMemoryMaxLiveSampleGapMs = ({
	schedulingToleranceMs,
	operationTimeoutMs = DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
}) => {
	if (
		!Number.isSafeInteger(schedulingToleranceMs) ||
		schedulingToleranceMs < 0 ||
		!Number.isSafeInteger(operationTimeoutMs) ||
		operationTimeoutMs <= 0
	) {
		throw new Error(
			"Download memory live-sample coverage requires bounded scheduling and operation tolerances",
		);
	}
	const maxGapMs =
		DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS +
		operationTimeoutMs +
		schedulingToleranceMs;
	if (!Number.isSafeInteger(maxGapMs) || maxGapMs <= 0) {
		throw new Error("Download memory live-sample gap exceeds the safe range");
	}
	return maxGapMs;
};

export const assertDownloadMemoryLiveSampleCoverage = ({
	samples,
	phaseStartedAt,
	phaseFinishedAt,
	maxGapMs,
	label,
}) => {
	if (
		!Array.isArray(samples) ||
		!Number.isSafeInteger(phaseStartedAt) ||
		phaseStartedAt < 0 ||
		!Number.isSafeInteger(phaseFinishedAt) ||
		phaseFinishedAt < phaseStartedAt ||
		!Number.isSafeInteger(maxGapMs) ||
		maxGapMs <= 0 ||
		typeof label !== "string" ||
		label.length === 0
	) {
		throw new Error("Download memory live-sample coverage input is invalid");
	}
	const liveSamples = [];
	let previousCapturedAt = null;
	for (const [index, sample] of samples.entries()) {
		if (sample == null || typeof sample !== "object" || Array.isArray(sample)) {
			throw new Error(`${label} memory sample ${index} is invalid`);
		}
		if (
			!Number.isSafeInteger(sample.capturedAt) ||
			sample.capturedAt < 0 ||
			(previousCapturedAt !== null && sample.capturedAt < previousCapturedAt)
		) {
			throw new Error(`${label} memory sample timestamps are invalid`);
		}
		if (
			!new Set(["initial", "periodic", "manual", "terminal"]).has(
				sample.sampleKind,
			)
		) {
			throw new Error(`${label} memory sample kind is invalid`);
		}
		previousCapturedAt = sample.capturedAt;
		if (sample.sampleKind !== "terminal") {
			liveSamples.push(sample);
		}
	}
	let firstIndex = -1;
	for (let index = 0; index < liveSamples.length; index += 1) {
		if (liveSamples[index].capturedAt <= phaseStartedAt) {
			firstIndex = index;
		} else {
			break;
		}
	}
	const lastIndex = liveSamples.findIndex(
		(sample) => sample.capturedAt >= phaseFinishedAt,
	);
	if (firstIndex < 0 || lastIndex < firstIndex) {
		throw new Error(
			`${label} memory samples do not bracket the exact phase window`,
		);
	}
	const coverageSamples = liveSamples.slice(firstIndex, lastIndex + 1);
	let maxObservedGapMs = 0;
	for (let index = 1; index < coverageSamples.length; index += 1) {
		const gapMs =
			coverageSamples[index].capturedAt - coverageSamples[index - 1].capturedAt;
		maxObservedGapMs = Math.max(maxObservedGapMs, gapMs);
		if (gapMs > maxGapMs) {
			throw new Error(
				`${label} memory live-sample gap ${gapMs}ms exceeds ${maxGapMs}ms`,
			);
		}
	}
	return {
		firstSampleAt: coverageSamples[0].capturedAt,
		lastSampleAt: coverageSamples.at(-1).capturedAt,
		startOverhangMs: phaseStartedAt - coverageSamples[0].capturedAt,
		endOverhangMs: coverageSamples.at(-1).capturedAt - phaseFinishedAt,
		maxObservedGapMs,
		sampleCount: coverageSamples.length,
	};
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
 * Runs one sample at a time, reserves one bounded slot for a live checkpoint and
 * one for the forced terminal sample, and makes checkpoint/stop operations
 * concurrency-safe. Errors are evidence rather than detached promise
 * rejections, and are themselves bounded so a failed probe cannot make the
 * benchmark result grow without limit.
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
	if (
		!Number.isSafeInteger(maxSamples) ||
		maxSamples < DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE
	) {
		throw new Error(
			"Serial sampler must reserve initial, live checkpoint, and final samples",
		);
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
	let serialSampleChain = Promise.resolve();
	let scheduledSamplePromise;
	let sampleNowPromise;
	let stopped = false;
	let samplingDisabledAfterTimeout = false;
	let capacityExhaustedBeforeTerminal = false;
	let periodicSampleCount = 0;
	let manualSampleCount = 0;
	let lastManualSampleAt = null;
	let terminalSampleAttempted = false;
	let terminalSampleCaptured = false;
	let terminalSampleAt = null;
	let stopSamplingPromise;
	let cleanupPromise;
	const periodicSampleLimit =
		maxSamples - DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE;

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

	const takeSample = async (sampleKind) => {
		const terminal = sampleKind === "terminal";
		if (terminal) {
			terminalSampleAttempted = true;
		}
		const sampleCapacityReached =
			sampleKind === "periodic"
				? periodicSampleCount >= periodicSampleLimit
				: samples.length >= (terminal ? maxSamples : maxSamples - 1);
		if (sampleCapacityReached) {
			if (!terminal) {
				capacityExhaustedBeforeTerminal = true;
			}
			return false;
		}
		if (samplingDisabledAfterTimeout) {
			return false;
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
			const capturedAt = now();
			samples.push({
				...values,
				capturedAt,
				sampleKind,
			});
			if (sampleKind === "manual") {
				manualSampleCount += 1;
				lastManualSampleAt = capturedAt;
			}
			if (sampleKind === "periodic") {
				periodicSampleCount += 1;
			}
			if (terminal) {
				terminalSampleCaptured = true;
				terminalSampleAt = capturedAt;
			}
			return true;
		} catch (error) {
			recordError(error);
			if (error?.code === "DOWNLOAD_MEMORY_OPERATION_TIMEOUT") {
				samplingDisabledAfterTimeout = true;
			}
			return false;
		}
	};

	const enqueueSample = (sampleKind) => {
		const samplePromise = serialSampleChain.then(() => takeSample(sampleKind));
		serialSampleChain = samplePromise.catch(recordError);
		return samplePromise;
	};

	const schedule = () => {
		if (stopped || samplingDisabledAfterTimeout || timer !== undefined) {
			return;
		}
		timer = setTimer(() => {
			timer = undefined;
			if (stopped || samplingDisabledAfterTimeout) {
				return;
			}
			if (periodicSampleCount >= periodicSampleLimit) {
				capacityExhaustedBeforeTerminal = true;
				return;
			}
			const currentSample = enqueueSample("periodic");
			scheduledSamplePromise = currentSample;
			const finishScheduledSample = () => {
				if (scheduledSamplePromise === currentSample) {
					scheduledSamplePromise = undefined;
				}
				schedule();
			};
			currentSample.then(finishScheduledSample, (error) => {
				recordError(error);
				finishScheduledSample();
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
		maxSamples,
		periodicSampleLimit,
		periodicSampleCount,
		capacityExhaustedBeforeTerminal,
		samplingCapacitySufficient: !capacityExhaustedBeforeTerminal,
		manualSampleCount,
		lastManualSampleAt,
		terminalSampleAttempted,
		terminalSampleCaptured,
		terminalSampleAt,
	});

	await takeSample("initial");
	schedule();

	const sampleNow = () => {
		if (stopSamplingPromise) {
			return stopSamplingPromise;
		}
		if (sampleNowPromise) {
			return sampleNowPromise;
		}
		if (timer !== undefined) {
			clearTimer(timer);
			timer = undefined;
		}
		const currentSample = enqueueSample("manual");
		const currentSnapshot = currentSample.then(() => snapshot());
		sampleNowPromise = currentSnapshot;
		currentSnapshot.then(schedule, (error) => {
			recordError(error);
			schedule();
		});
		return currentSnapshot;
	};

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
			await sampleNowPromise;
			await scheduledSamplePromise;
			await enqueueSample("terminal");
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
		sampleNow,
		stopSampling,
		cleanup: runCleanup,
		stop: runCleanup,
	};
};

const summarizeHeap = (scope, state) => {
	const first = state.samples[0] ?? null;
	const last = state.samples.at(-1) ?? null;
	const peak = (name) =>
		state.samples.length > 0
			? Math.max(...state.samples.map((sample) => sample[name]))
			: null;
	return {
		memoryKind: "runtime-heap",
		scope,
		metric: "Runtime.getHeapUsage",
		unit: "bytes",
		sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		startedAt: state.startedAt,
		finishedAt: state.finishedAt,
		sampleCount: state.samples.length,
		startBytes: first?.usedBytes ?? null,
		endBytes: last?.usedBytes ?? null,
		peakBytes: peak("usedBytes"),
		startUsedBytes: first?.usedBytes ?? null,
		endUsedBytes: last?.usedBytes ?? null,
		peakUsedBytes: peak("usedBytes"),
		startTotalBytes: first?.totalBytes ?? null,
		endTotalBytes: last?.totalBytes ?? null,
		peakTotalBytes: peak("totalBytes"),
		startEmbedderHeapUsedBytes: first?.embedderHeapUsedBytes ?? null,
		endEmbedderHeapUsedBytes: last?.embedderHeapUsedBytes ?? null,
		peakEmbedderHeapUsedBytes: peak("embedderHeapUsedBytes"),
		startBackingStorageBytes: first?.backingStorageBytes ?? null,
		endBackingStorageBytes: last?.backingStorageBytes ?? null,
		peakBackingStorageBytes: peak("backingStorageBytes"),
		samples: state.samples,
		samplingErrors: state.samplingErrors,
		samplingErrorOverflowCount: state.samplingErrorOverflowCount,
		cleanupWarnings: state.cleanupWarnings,
		cleanupWarningOverflowCount: state.cleanupWarningOverflowCount,
		maxSamples: state.maxSamples,
		periodicSampleLimit: state.periodicSampleLimit,
		periodicSampleCount: state.periodicSampleCount,
		capacityExhaustedBeforeTerminal: state.capacityExhaustedBeforeTerminal,
		samplingCapacitySufficient: state.samplingCapacitySufficient,
		manualSampleCount: state.manualSampleCount,
		lastManualSampleAt: state.lastManualSampleAt,
		terminalSampleAttempted: state.terminalSampleAttempted,
		terminalSampleCaptured: state.terminalSampleCaptured,
		terminalSampleAt: state.terminalSampleAt,
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
			const response = await session.send("Runtime.getHeapUsage");
			const values = {
				usedBytes: response.usedSize,
				totalBytes: response.totalSize,
				embedderHeapUsedBytes: response.embedderHeapUsedSize,
				backingStorageBytes: response.backingStorageSize,
			};
			if (
				Object.values(values).some(
					(value) => !Number.isSafeInteger(value) || value < 0,
				)
			) {
				throw new Error("Runtime.getHeapUsage returned invalid heap values");
			}
			return values;
		},
		cleanup: async () => {
			if (!session) {
				return;
			}
			const cleanupErrors = [];
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
		sampleNow: async () => summarizeHeap(scope, await sampler.sampleNow()),
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
		startNodeExternalBytes: first?.nodeExternalBytes ?? null,
		endNodeExternalBytes: last?.nodeExternalBytes ?? null,
		peakNodeExternalBytes: peak("nodeExternalBytes"),
		startNodeArrayBuffersBytes: first?.nodeArrayBuffersBytes ?? null,
		endNodeArrayBuffersBytes: last?.nodeArrayBuffersBytes ?? null,
		peakNodeArrayBuffersBytes: peak("nodeArrayBuffersBytes"),
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
		maxSamples: state.maxSamples,
		periodicSampleLimit: state.periodicSampleLimit,
		periodicSampleCount: state.periodicSampleCount,
		capacityExhaustedBeforeTerminal: state.capacityExhaustedBeforeTerminal,
		samplingCapacitySufficient: state.samplingCapacitySufficient,
		manualSampleCount: state.manualSampleCount,
		lastManualSampleAt: state.lastManualSampleAt,
		terminalSampleAttempted: state.terminalSampleAttempted,
		terminalSampleCaptured: state.terminalSampleCaptured,
		terminalSampleAt: state.terminalSampleAt,
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
			const nodeMemory = process.memoryUsage();
			const nodeBytes = nodeMemory.rss;
			const nodeExternalBytes = nodeMemory.external;
			const nodeArrayBuffersBytes = nodeMemory.arrayBuffers;
			if (
				!Number.isSafeInteger(nodeBytes) ||
				nodeBytes <= 0 ||
				!Number.isSafeInteger(nodeExternalBytes) ||
				nodeExternalBytes < 0 ||
				!Number.isSafeInteger(nodeArrayBuffersBytes) ||
				nodeArrayBuffersBytes < 0
			) {
				throw new Error(
					"Playwright worker returned invalid Node memory values",
				);
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
				nodeExternalBytes,
				nodeArrayBuffersBytes,
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
		sampleNow: async () => summarizeHostRss(await sampler.sampleNow()),
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
	postTransferSoakMs,
	samplingWindowBudgetMs,
	operationTimeoutMs = DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	cleanupTimeoutMs = DOWNLOAD_MEMORY_CLEANUP_TIMEOUT_MS,
}) => {
	const maxSamplesPerSeries = calculateDownloadMemoryMaxSamples({
		samplingWindowBudgetMs,
	});
	const liveSampleMaxGapMs = calculateDownloadMemoryMaxLiveSampleGapMs({
		schedulingToleranceMs,
		operationTimeoutMs,
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
	let sampleNowPromise;
	let stopSamplingPromise;
	let cleanupPromise;
	const build = ({ readerJsHeap, writerJsHeap, hostRss }) => {
		const series = [readerJsHeap, writerJsHeap, hostRss];
		const capacityExhaustedBeforeTerminal = series.some(
			(value) => value.capacityExhaustedBeforeTerminal,
		);
		return {
			profile: DOWNLOAD_MEMORY_PROFILE,
			sampleIntervalMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
			windowDefinition: DOWNLOAD_MEMORY_WINDOW_DEFINITION,
			downloadTimeoutMs,
			schedulingToleranceMs,
			operationTimeoutMs,
			postTransferSoakMs,
			samplingWindowBudgetMs,
			liveSampleMaxGapMs,
			liveSampleCoverageDefinition:
				DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION,
			endpointSampleAllowance: DOWNLOAD_MEMORY_ENDPOINT_SAMPLE_ALLOWANCE,
			maxSamplesPerSeries,
			capacityExhaustedBeforeTerminal,
			samplingCapacitySufficient: !capacityExhaustedBeforeTerminal,
			manualCheckpointComplete: series.every(
				(value) => value.manualSampleCount > 0,
			),
			terminalCheckpointComplete: series.every(
				(value) => value.terminalSampleCaptured,
			),
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
		};
	};
	const sampleNow = () => {
		if (stopSamplingPromise) {
			return stopSamplingPromise;
		}
		if (sampleNowPromise) {
			return sampleNowPromise;
		}
		const currentSample = Promise.all([
			readerSampler.sampleNow(),
			writerSampler.sampleNow(),
			hostSampler.sampleNow(),
		]).then(([readerJsHeap, writerJsHeap, hostRss]) =>
			build({ readerJsHeap, writerJsHeap, hostRss }),
		);
		sampleNowPromise = currentSample;
		return currentSample;
	};
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
		sampleNow,
		stopSampling,
		cleanup: runCleanup,
		stop: runCleanup,
	};
};
