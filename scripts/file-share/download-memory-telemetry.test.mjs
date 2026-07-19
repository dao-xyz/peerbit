import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import {
	DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH,
	DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES,
	DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS,
	calculateDownloadMemoryMaxSamples,
	startBoundedSerialSampler,
	startDownloadMemoryTelemetry,
	withDownloadMemoryOperationDeadline,
} from "./templates/download-memory-telemetry.mjs";

test("derives a deadline-bounded sample cap with an absolute result-size ceiling", () => {
	assert.equal(
		calculateDownloadMemoryMaxSamples({
			downloadTimeoutMs: 600_000,
			schedulingToleranceMs: 5_000,
		}),
		123,
	);
	assert.equal(
		calculateDownloadMemoryMaxSamples({
			downloadTimeoutMs: Number.MAX_SAFE_INTEGER - 10_000,
			schedulingToleranceMs: 5_000,
		}),
		DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES,
	);
	assert.throws(
		() =>
			calculateDownloadMemoryMaxSamples({
				downloadTimeoutMs: 0,
				schedulingToleranceMs: 5_000,
			}),
		/bounded timeout values/,
	);
});

test("serializes samples, reserves the forced endpoint, and cleans up once", async () => {
	let active = 0;
	let peakActive = 0;
	let cleanupCount = 0;
	let value = 0;
	const sampler = await startBoundedSerialSampler({
		intervalMs: 1,
		maxSamples: 3,
		readSample: async () => {
			active += 1;
			peakActive = Math.max(peakActive, active);
			await delay(2);
			active -= 1;
			return { value: (value += 1) };
		},
		cleanup: async () => {
			cleanupCount += 1;
		},
	});
	await delay(12);
	const [firstStop, secondStop] = await Promise.all([
		sampler.stop(),
		sampler.stop(),
	]);
	assert.deepEqual(firstStop, secondStop);
	assert.equal(peakActive, 1);
	assert.equal(cleanupCount, 1);
	assert.equal(firstStop.samples.length, 3);
	assert.equal(firstStop.finishedAt >= firstStop.startedAt, true);
});

test("bounds and truncates repeated sampling failures", async () => {
	const longMessage = "x".repeat(DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH * 2);
	const sampler = await startBoundedSerialSampler({
		intervalMs: 1,
		maxSamples: 2,
		readSample: async () => {
			throw new Error(longMessage);
		},
	});
	await delay(100);
	const result = await sampler.stop();
	assert.equal(
		result.samplingErrors.length,
		DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS,
	);
	assert.equal(result.samplingErrorOverflowCount > 0, true);
	assert.equal(
		result.samplingErrors.every(
			(message) => message.length === DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH,
		),
		true,
	);
	assert.deepEqual(result.samples, []);
});

test("bounds never-settling initial, active, and cleanup operations", async () => {
	let initialCleanupCount = 0;
	const initial = await startBoundedSerialSampler({
		intervalMs: 1,
		maxSamples: 4,
		operationTimeoutMs: 5,
		cleanupTimeoutMs: 5,
		readSample: async () => await new Promise(() => {}),
		cleanup: async () => {
			initialCleanupCount += 1;
		},
	});
	const initialResult = await initial.stop();
	assert.equal(initialResult.samples.length, 0);
	assert.equal(initialResult.samplingErrors.length, 1);
	assert.match(initialResult.samplingErrors[0], /Memory sample exceeded 5ms/);
	assert.equal(initialCleanupCount, 1);

	let activeCalls = 0;
	let activeCleanupCount = 0;
	const active = await startBoundedSerialSampler({
		intervalMs: 1,
		maxSamples: 4,
		operationTimeoutMs: 5,
		cleanupTimeoutMs: 5,
		readSample: async () => {
			activeCalls += 1;
			return activeCalls === 1 ? { value: 1 } : await new Promise(() => {});
		},
		cleanup: async () => {
			activeCleanupCount += 1;
		},
	});
	await delay(2);
	const activeResult = await active.stop();
	await delay(8);
	assert.equal(activeCalls, 2);
	assert.equal(activeResult.samples.length, 1);
	assert.equal(activeResult.samplingErrors.length, 1);
	assert.equal(activeCleanupCount, 1);

	const cleanup = await startBoundedSerialSampler({
		intervalMs: 100,
		maxSamples: 3,
		operationTimeoutMs: 5,
		cleanupTimeoutMs: 5,
		readSample: async () => ({ value: 1 }),
		cleanup: async () => await new Promise(() => {}),
	});
	const cleanupResult = await cleanup.stop();
	assert.equal(cleanupResult.samples.length, 2);
	assert.deepEqual(cleanupResult.samplingErrors, []);
	assert.equal(cleanupResult.cleanupWarnings.length, 1);
	assert.match(
		cleanupResult.cleanupWarnings[0],
		/Memory sampler cleanup exceeded 5ms/,
	);
});

test("finalizes the terminal sample before post-sampling cleanup", async () => {
	let cleanupCount = 0;
	const sampler = await startBoundedSerialSampler({
		intervalMs: 100,
		maxSamples: 3,
		readSample: async () => ({ value: 1 }),
		cleanup: async () => {
			cleanupCount += 1;
		},
	});
	const sampled = await sampler.stopSampling();
	assert.equal(sampled.samples.length, 2);
	assert.equal(sampled.finishedAt >= sampled.startedAt, true);
	assert.equal(cleanupCount, 0);

	const cleaned = await sampler.cleanup();
	assert.equal(cleaned.finishedAt, sampled.finishedAt);
	assert.equal(cleanupCount, 1);
});

test("keeps non-timeout cleanup failures fatal", async () => {
	const sampler = await startBoundedSerialSampler({
		intervalMs: 100,
		maxSamples: 3,
		readSample: async () => ({ value: 1 }),
		cleanup: async () => {
			throw new Error("detach rejected");
		},
	});
	const result = await sampler.cleanup();
	assert.deepEqual(result.cleanupWarnings, []);
	assert.deepEqual(result.samplingErrors, ["detach rejected"]);
});

test("absorbs late operation settlement and cleans late-created values", async () => {
	let lateCleanupCount = 0;
	await assert.rejects(
		withDownloadMemoryOperationDeadline(
			new Promise((resolve) => setTimeout(() => resolve("late"), 12)),
			"late operation",
			2,
			{
				onLateResolution: async (value) => {
					assert.equal(value, "late");
					lateCleanupCount += 1;
				},
			},
		),
		/late operation exceeded 2ms/,
	);
	await assert.rejects(
		withDownloadMemoryOperationDeadline(
			new Promise((_, reject) =>
				setTimeout(() => reject(new Error("late rejection")), 12),
			),
			"late rejection",
			2,
		),
		/late rejection exceeded 2ms/,
	);
	await delay(20);
	assert.equal(lateCleanupCount, 1);
});

test("collects forced endpoint heap and attributed RSS samples", async () => {
	let detachedPageSessions = 0;
	let detachedBrowserSessions = 0;
	const page = (usedBytes) => ({
		context: () => ({
			newCDPSession: async () => ({
				send: async (method) =>
					method === "Performance.getMetrics"
						? { metrics: [{ name: "JSHeapUsedSize", value: usedBytes }] }
						: {},
				detach: async () => {
					detachedPageSessions += 1;
				},
			}),
		}),
	});
	const browser = {
		newBrowserCDPSession: async () => ({
			send: async (method) => {
				assert.equal(method, "SystemInfo.getProcessInfo");
				return {
					processInfo: [{ id: process.pid, type: "browser" }],
				};
			},
			detach: async () => {
				detachedBrowserSessions += 1;
			},
		}),
	};
	const telemetry = await startDownloadMemoryTelemetry({
		browser,
		reader: page(100),
		writer: page(200),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
	});
	const result = await telemetry.stop();
	assert.equal(result.complete, true);
	assert.equal(result.cleanupComplete, true);
	assert.equal(result.readerJsHeap.sampleCount, 2);
	assert.equal(result.writerJsHeap.sampleCount, 2);
	assert.equal(result.hostRss.sampleCount, 2);
	assert.equal(result.hostRss.samples[0].browserBytes > 0, true);
	assert.equal(
		result.hostRss.samples[0].combinedBytes,
		result.hostRss.samples[0].browserBytes +
			result.hostRss.samples[0].nodeBytes,
	);
	assert.equal(detachedPageSessions, 2);
	assert.equal(detachedBrowserSessions, 1);
});

test("records post-sampling CDP timeouts as cleanup warnings", async () => {
	let detachStarted = 0;
	const page = (hangOnCleanup) => ({
		context: () => ({
			newCDPSession: async () => ({
				send: async (method) => {
					if (method === "Performance.getMetrics") {
						return { metrics: [{ name: "JSHeapUsedSize", value: 100 }] };
					}
					if (method === "Performance.disable" && hangOnCleanup) {
						return await new Promise(() => {});
					}
					return {};
				},
				detach: async () => {
					detachStarted += 1;
					if (hangOnCleanup) {
						return await new Promise(() => {});
					}
				},
			}),
		}),
	});
	const browser = {
		newBrowserCDPSession: async () => ({
			send: async () => ({
				processInfo: [{ id: process.pid, type: "browser" }],
			}),
			detach: async () => {},
		}),
	};
	const telemetry = await startDownloadMemoryTelemetry({
		browser,
		reader: page(true),
		writer: page(false),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		operationTimeoutMs: 3,
		cleanupTimeoutMs: 8,
	});
	const sampled = await telemetry.stopSampling();
	assert.equal(sampled.complete, true);
	assert.equal(sampled.cleanupComplete, false);
	assert.equal(detachStarted, 0);

	const result = await telemetry.cleanup();
	assert.equal(result.cleanupComplete, true);
	assert.deepEqual(result.readerJsHeap.samplingErrors, []);
	assert.equal(result.readerJsHeap.cleanupWarnings.length, 1);
	assert.match(
		result.readerJsHeap.cleanupWarnings[0],
		/Performance\.disable exceeded 3ms; Page JS heap CDP detach exceeded 3ms/,
	);
	assert.equal(detachStarted, 2);
});

test("bounds setup and detaches a CDP session created after its deadline", async () => {
	let lateDetachCount = 0;
	const session = (usedBytes, onDetach = () => {}) => ({
		send: async (method) =>
			method === "Performance.getMetrics"
				? { metrics: [{ name: "JSHeapUsedSize", value: usedBytes }] }
				: {},
		detach: async () => onDetach(),
	});
	const lateReaderSession = session(100, () => {
		lateDetachCount += 1;
	});
	const reader = {
		context: () => ({
			newCDPSession: async () =>
				await new Promise((resolve) =>
					setTimeout(() => resolve(lateReaderSession), 15),
				),
		}),
	};
	const writer = {
		context: () => ({
			newCDPSession: async () => session(200),
		}),
	};
	const browser = {
		newBrowserCDPSession: async () => ({
			send: async () => ({
				processInfo: [{ id: process.pid, type: "browser" }],
			}),
			detach: async () => {},
		}),
	};
	const telemetry = await startDownloadMemoryTelemetry({
		browser,
		reader,
		writer,
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		operationTimeoutMs: 3,
		cleanupTimeoutMs: 8,
	});
	assert.equal(
		telemetry
			.snapshot()
			.readerJsHeap.samplingErrors.some((message) =>
				message.includes("CDP session creation exceeded 3ms"),
			),
		true,
	);
	await telemetry.stop();
	await delay(25);
	assert.equal(lateDetachCount, 1);
});
