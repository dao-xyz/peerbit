import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import {
	DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION,
	DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH,
	DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES,
	DOWNLOAD_MEMORY_MAX_SAMPLING_ERRORS,
	DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS,
	DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
	assertDownloadMemoryLiveSampleCoverage,
	calculateDownloadMemoryMaxLiveSampleGapMs,
	calculateDownloadMemoryMaxSamples,
	startBoundedSerialSampler,
	startDownloadMemoryTelemetry,
	withDownloadMemoryOperationDeadline,
} from "./templates/download-memory-telemetry.mjs";

test("requires bounded non-terminal coverage for each exact live phase", () => {
	const maxGapMs = calculateDownloadMemoryMaxLiveSampleGapMs({
		schedulingToleranceMs: 5_000,
	});
	assert.equal(
		maxGapMs,
		DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS +
			DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS +
			5_000,
	);
	assert.deepEqual(
		assertDownloadMemoryLiveSampleCoverage({
			samples: [
				{ capturedAt: 99, sampleKind: "initial" },
				{ capturedAt: 101, sampleKind: "manual" },
				{ capturedAt: 102, sampleKind: "terminal" },
			],
			phaseStartedAt: 100,
			phaseFinishedAt: 101,
			maxGapMs,
			label: "short phase",
		}),
		{
			firstSampleAt: 99,
			lastSampleAt: 101,
			startOverhangMs: 1,
			endOverhangMs: 0,
			maxObservedGapMs: 2,
			sampleCount: 2,
		},
	);
	assert.throws(
		() =>
			assertDownloadMemoryLiveSampleCoverage({
				samples: [
					{ capturedAt: 1, sampleKind: "initial" },
					{ capturedAt: maxGapMs + 2, sampleKind: "manual" },
				],
				phaseStartedAt: 2,
				phaseFinishedAt: maxGapMs + 1,
				maxGapMs,
				label: "long phase",
			}),
		/live-sample gap/,
	);
	assert.throws(
		() =>
			assertDownloadMemoryLiveSampleCoverage({
				samples: [
					{ capturedAt: 1, sampleKind: "initial" },
					{ capturedAt: 10, sampleKind: "terminal" },
				],
				phaseStartedAt: 2,
				phaseFinishedAt: 9,
				maxGapMs,
				label: "terminal-only endpoint",
			}),
		/do not bracket/,
	);
});

const createFakeTimerClock = () => {
	let currentTime = 0;
	let nextId = 1;
	const timers = new Map();
	return {
		now: () => currentTime,
		setTimer: (callback, timeoutMs) => {
			const id = nextId++;
			timers.set(id, { callback, dueAt: currentTime + timeoutMs });
			return id;
		},
		clearTimer: (id) => timers.delete(id),
		advanceBy: (durationMs) => {
			currentTime += durationMs;
		},
		runNext: async () => {
			const next = [...timers.entries()].sort(
				([leftId, left], [rightId, right]) =>
					left.dueAt - right.dueAt || leftId - rightId,
			)[0];
			assert.ok(next, "expected a scheduled fake timer");
			const [id, { callback, dueAt }] = next;
			timers.delete(id);
			currentTime = Math.max(currentTime, dueAt);
			callback();
			await Promise.resolve();
			await Promise.resolve();
		},
	};
};

test("derives a deadline-bounded sample cap with an absolute result-size ceiling", () => {
	assert.equal(
		calculateDownloadMemoryMaxSamples({
			samplingWindowBudgetMs: 600_000,
		}),
		123,
	);
	assert.equal(
		calculateDownloadMemoryMaxSamples({
			samplingWindowBudgetMs: 665_001,
		}),
		137,
	);
	assert.equal(
		calculateDownloadMemoryMaxSamples({
			samplingWindowBudgetMs: Number.MAX_SAFE_INTEGER,
		}),
		DOWNLOAD_MEMORY_MAX_SAMPLES_PER_SERIES,
	);
	assert.throws(
		() =>
			calculateDownloadMemoryMaxSamples({
				samplingWindowBudgetMs: 0,
			}),
		/positive bounded sampling window budget/,
	);
	assert.throws(
		() =>
			calculateDownloadMemoryMaxSamples({
				samplingWindowBudgetMs: -1,
			}),
		/positive bounded sampling window budget/,
	);
	assert.throws(
		() => calculateDownloadMemoryMaxSamples({}),
		/positive bounded sampling window budget/,
	);
	assert.equal(
		calculateDownloadMemoryMaxSamples({
			samplingWindowBudgetMs: DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS,
		}),
		4,
	);
});

test("serializes periodic and live samples while reserving the terminal endpoint", async () => {
	const clock = createFakeTimerClock();
	let active = 0;
	let peakActive = 0;
	let cleanupCount = 0;
	let value = 0;
	let releasePeriodic;
	const periodicGate = new Promise((resolve) => {
		releasePeriodic = resolve;
	});
	const sampler = await startBoundedSerialSampler({
		intervalMs: 1,
		maxSamples: 5,
		now: clock.now,
		setTimer: clock.setTimer,
		clearTimer: clock.clearTimer,
		readSample: async () => {
			active += 1;
			peakActive = Math.max(peakActive, active);
			const nextValue = (value += 1);
			if (nextValue === 2) {
				await periodicGate;
			}
			active -= 1;
			return { value: nextValue };
		},
		cleanup: async () => {
			cleanupCount += 1;
		},
	});
	await clock.runNext();
	while (value < 2) {
		await Promise.resolve();
	}
	const firstCheckpoint = sampler.sampleNow();
	const secondCheckpoint = sampler.sampleNow();
	assert.strictEqual(firstCheckpoint, secondCheckpoint);
	assert.equal(peakActive, 1);
	releasePeriodic();
	const checkpoint = await firstCheckpoint;
	assert.equal(peakActive, 1);
	assert.deepEqual(
		checkpoint.samples.map((sample) => sample.sampleKind),
		["initial", "periodic", "manual"],
	);
	assert.equal(checkpoint.manualSampleCount, 1);
	assert.equal(checkpoint.terminalSampleAttempted, false);
	assert.deepEqual(await sampler.sampleNow(), checkpoint);
	assert.equal(value, 3);
	clock.advanceBy(1);
	const [firstStop, secondStop] = await Promise.all([
		sampler.stop(),
		sampler.stop(),
	]);
	assert.deepEqual(firstStop, secondStop);
	assert.equal(peakActive, 1);
	assert.equal(cleanupCount, 1);
	assert.deepEqual(
		firstStop.samples.map((sample) => sample.sampleKind),
		["initial", "periodic", "manual", "terminal"],
	);
	assert.equal(firstStop.lastManualSampleAt < firstStop.terminalSampleAt, true);
	assert.equal(firstStop.terminalSampleAttempted, true);
	assert.equal(firstStop.terminalSampleCaptured, true);
	assert.equal(firstStop.capacityExhaustedBeforeTerminal, false);
	assert.equal(firstStop.finishedAt >= firstStop.startedAt, true);
});

test("reports premature periodic capacity exhaustion and still captures terminal evidence", async () => {
	const clock = createFakeTimerClock();
	let value = 0;
	const sampler = await startBoundedSerialSampler({
		intervalMs: 5,
		maxSamples: 3,
		now: clock.now,
		setTimer: clock.setTimer,
		clearTimer: clock.clearTimer,
		readSample: async () => ({ value: (value += 1) }),
	});

	await clock.runNext();
	const exhausted = sampler.snapshot();
	assert.equal(exhausted.capacityExhaustedBeforeTerminal, true);
	assert.equal(exhausted.samplingCapacitySufficient, false);
	assert.equal(exhausted.terminalSampleAttempted, false);

	const live = await sampler.sampleNow();
	assert.deepEqual(
		live.samples.map((sample) => sample.sampleKind),
		["initial", "manual"],
	);
	clock.advanceBy(1);
	const terminal = await sampler.stopSampling();
	assert.deepEqual(
		terminal.samples.map((sample) => sample.sampleKind),
		["initial", "manual", "terminal"],
	);
	assert.equal(terminal.capacityExhaustedBeforeTerminal, true);
	assert.equal(terminal.terminalSampleAttempted, true);
	assert.equal(terminal.terminalSampleCaptured, true);
	assert.equal(terminal.lastManualSampleAt < terminal.terminalSampleAt, true);
});

test("cleanup waits for an in-flight live checkpoint before taking the terminal sample", async () => {
	let sampleCalls = 0;
	let releaseCheckpoint;
	const checkpointGate = new Promise((resolve) => {
		releaseCheckpoint = resolve;
	});
	let cleanupCount = 0;
	const sampler = await startBoundedSerialSampler({
		intervalMs: 60_000,
		maxSamples: 3,
		readSample: async () => {
			sampleCalls += 1;
			if (sampleCalls === 2) {
				await checkpointGate;
			}
			return { value: sampleCalls };
		},
		cleanup: async () => {
			cleanupCount += 1;
		},
	});
	const checkpoint = sampler.sampleNow();
	const cleanup = sampler.cleanup();
	await Promise.resolve();
	assert.equal(cleanupCount, 0);
	releaseCheckpoint();
	await checkpoint;
	const result = await cleanup;
	assert.equal(cleanupCount, 1);
	assert.deepEqual(
		result.samples.map((sample) => sample.sampleKind),
		["initial", "manual", "terminal"],
	);
});

test("bounds and truncates repeated sampling failures", async () => {
	const longMessage = "x".repeat(DOWNLOAD_MEMORY_MAX_ERROR_MESSAGE_LENGTH * 2);
	const sampler = await startBoundedSerialSampler({
		intervalMs: 1,
		maxSamples: 20,
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

const createRuntimeHeapPage = () => ({
	context: () => ({
		newCDPSession: async () => ({
			send: async (method) => {
				assert.equal(method, "Runtime.getHeapUsage");
				return {
					usedSize: 100,
					totalSize: 110,
					embedderHeapUsedSize: 120,
					backingStorageSize: 130,
				};
			},
			detach: async () => {},
		}),
	}),
});

test("requires one or two unique browser instances", async () => {
	const browser = { newBrowserCDPSession: async () => ({}) };
	const options = {
		reader: createRuntimeHeapPage(),
		writer: createRuntimeHeapPage(),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 0,
		samplingWindowBudgetMs: 65_000,
	};
	await assert.rejects(
		startDownloadMemoryTelemetry({
			...options,
			browsers: [],
			expectedBrowserCount: 1,
		}),
		/requires exactly 1 browser/,
	);
	await assert.rejects(
		startDownloadMemoryTelemetry({
			...options,
			browsers: [browser, browser],
			expectedBrowserCount: 2,
		}),
		/requires unique browser instances/,
	);
	await assert.rejects(
		startDownloadMemoryTelemetry({
			...options,
			browsers: [
				browser,
				{ newBrowserCDPSession: async () => ({}) },
				{ newBrowserCDPSession: async () => ({}) },
			],
			expectedBrowserCount: 2,
		}),
		/requires exactly 2 browsers/,
	);
});

test("fails closed on conflicting cross-browser PID types and detaches every session", async () => {
	let detachedBrowserSessions = 0;
	const browser = (processInfo, detachFails = false) => ({
		newBrowserCDPSession: async () => ({
			send: async (method) => {
				assert.equal(method, "SystemInfo.getProcessInfo");
				return { processInfo };
			},
			detach: async () => {
				detachedBrowserSessions += 1;
				if (detachFails) {
					throw new Error("synthetic browser detach failure");
				}
			},
		}),
	});
	const telemetry = await startDownloadMemoryTelemetry({
		browsers: [
			browser([{ id: process.pid, type: "browser" }], true),
			browser([
				{ id: process.ppid, type: "browser" },
				{ id: process.pid, type: "renderer" },
			]),
		],
		expectedBrowserCount: 2,
		reader: createRuntimeHeapPage(),
		writer: createRuntimeHeapPage(),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 0,
		samplingWindowBudgetMs: 65_000,
	});
	const initial = telemetry.snapshot();
	assert.equal(initial.hostRss.sampleCount, 0);
	assert.equal(
		initial.hostRss.samplingErrors.some((message) =>
			message.includes("conflicting process types"),
		),
		true,
	);
	const result = await telemetry.stop();
	assert.equal(detachedBrowserSessions, 2);
	assert.equal(
		result.hostRss.samplingErrors.includes("synthetic browser detach failure"),
		true,
	);
});

test("fails closed when separate sessions report the same browser root", async () => {
	const browser = () => ({
		newBrowserCDPSession: async () => ({
			send: async (method) => {
				assert.equal(method, "SystemInfo.getProcessInfo");
				return {
					processInfo: [{ id: process.pid, type: "browser" }],
				};
			},
			detach: async () => {},
		}),
	});
	const telemetry = await startDownloadMemoryTelemetry({
		browsers: [browser(), browser()],
		expectedBrowserCount: 2,
		reader: createRuntimeHeapPage(),
		writer: createRuntimeHeapPage(),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 0,
		samplingWindowBudgetMs: 65_000,
	});
	assert.equal(
		telemetry
			.snapshot()
			.hostRss.samplingErrors.some((message) =>
				message.includes("distinct browser root processes"),
			),
		true,
	);
	await telemetry.stop();
});

test("applies the Chromium process cap after cross-browser PID deduplication", async () => {
	let detachedBrowserSessions = 0;
	const browser = (processInfo) => ({
		newBrowserCDPSession: async () => ({
			send: async () => ({ processInfo }),
			detach: async () => {
				detachedBrowserSessions += 1;
			},
		}),
	});
	const telemetry = await startDownloadMemoryTelemetry({
		browsers: [
			browser(
				Array.from({ length: 128 }, (_, index) => ({
					id: index + 1,
					type: index === 0 ? "browser" : "renderer",
				})),
			),
			browser(
				Array.from({ length: 129 }, (_, index) => ({
					id: index + 129,
					type: index === 0 ? "browser" : "renderer",
				})),
			),
		],
		expectedBrowserCount: 2,
		reader: createRuntimeHeapPage(),
		writer: createRuntimeHeapPage(),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 0,
		samplingWindowBudgetMs: 65_000,
	});
	assert.equal(
		telemetry
			.snapshot()
			.hostRss.samplingErrors.some((message) =>
				message.includes("process count exceeds the telemetry cap"),
			),
		true,
	);
	await telemetry.stop();
	assert.equal(detachedBrowserSessions, 2);
});

test("collects forced endpoint heaps and deduplicated multi-browser host samples", async () => {
	let detachedPageSessions = 0;
	let detachedBrowserSessions = 0;
	const browserSessionSendCounts = [0, 0];
	const page = (initialUsedBytes) => ({
		context: () => ({
			newCDPSession: async () => {
				let sampleIndex = 0;
				return {
					send: async (method) => {
						assert.equal(method, "Runtime.getHeapUsage");
						const usedBytes = initialUsedBytes + sampleIndex;
						sampleIndex += 1;
						return {
							usedSize: usedBytes,
							totalSize: usedBytes + 10,
							embedderHeapUsedSize: usedBytes + 20,
							backingStorageSize: usedBytes + 30,
						};
					},
					detach: async () => {
						detachedPageSessions += 1;
					},
				};
			},
		}),
	});
	const browser = (index, processInfo) => ({
		newBrowserCDPSession: async () => ({
			send: async (method) => {
				assert.equal(method, "SystemInfo.getProcessInfo");
				browserSessionSendCounts[index] += 1;
				return { processInfo };
			},
			detach: async () => {
				detachedBrowserSessions += 1;
			},
		}),
	});
	const telemetry = await startDownloadMemoryTelemetry({
		browsers: [
			browser(0, [{ id: process.pid, type: "browser" }]),
			browser(1, [{ id: process.ppid, type: "browser" }]),
		],
		expectedBrowserCount: 2,
		reader: page(100),
		writer: page(200),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 60_000,
		samplingWindowBudgetMs: 130_000,
	});
	const [firstCheckpoint, secondCheckpoint] = await Promise.all([
		telemetry.sampleNow(),
		telemetry.sampleNow(),
	]);
	assert.deepEqual(firstCheckpoint, secondCheckpoint);
	assert.equal(firstCheckpoint.manualCheckpointComplete, true);
	assert.equal(firstCheckpoint.terminalCheckpointComplete, false);
	const result = await telemetry.stop();
	assert.equal(result.complete, true);
	assert.equal(result.cleanupComplete, true);
	assert.equal(result.readerJsHeap.sampleCount, 3);
	assert.equal(result.writerJsHeap.sampleCount, 3);
	assert.equal(result.hostRss.sampleCount, 3);
	assert.equal(result.hostRss.browserInstanceCount, 2);
	assert.equal(result.hostRss.browserSessionCount, 2);
	assert.equal(result.profile, "download-memory-v3");
	assert.equal(result.operationTimeoutMs, DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS);
	assert.equal(
		result.liveSampleMaxGapMs,
		DOWNLOAD_MEMORY_SAMPLE_INTERVAL_MS +
			DOWNLOAD_MEMORY_OPERATION_TIMEOUT_MS +
			5_000,
	);
	assert.equal(
		result.liveSampleCoverageDefinition,
		DOWNLOAD_MEMORY_LIVE_SAMPLE_COVERAGE_DEFINITION,
	);
	assert.equal(result.postTransferSoakMs, 60_000);
	assert.equal(result.samplingWindowBudgetMs, 130_000);
	assert.equal(result.samplingCapacitySufficient, true);
	assert.equal(result.capacityExhaustedBeforeTerminal, false);
	assert.equal(result.manualCheckpointComplete, true);
	assert.equal(result.terminalCheckpointComplete, true);
	assert.equal(result.readerJsHeap.metric, "Runtime.getHeapUsage");
	assert.equal(result.readerJsHeap.startUsedBytes, 100);
	assert.equal(result.readerJsHeap.endUsedBytes, 102);
	assert.equal(result.readerJsHeap.peakTotalBytes, 112);
	assert.equal(result.readerJsHeap.endEmbedderHeapUsedBytes, 122);
	assert.equal(result.readerJsHeap.peakBackingStorageBytes, 132);
	assert.deepEqual(
		result.readerJsHeap.samples.map((sample) => sample.sampleKind),
		["initial", "manual", "terminal"],
	);
	assert.equal(result.hostRss.samples[0].browserBytes > 0, true);
	assert.equal(result.hostRss.samples[0].browserInstanceCount, 2);
	assert.equal(result.hostRss.samples[0].browserRootProcessCount, 2);
	assert.equal(result.hostRss.samples[0].browserProcessCount, 2);
	assert.deepEqual(Object.keys(result.hostRss.samples[0].browserRoleBytes), [
		"browser",
	]);
	assert.equal(result.hostRss.samples[0].nodeExternalBytes >= 0, true);
	assert.equal(result.hostRss.samples[0].nodeArrayBuffersBytes >= 0, true);
	assert.equal(
		result.hostRss.startNodeExternalBytes,
		result.hostRss.samples[0].nodeExternalBytes,
	);
	assert.equal(
		result.hostRss.endNodeArrayBuffersBytes,
		result.hostRss.samples.at(-1).nodeArrayBuffersBytes,
	);
	assert.equal(
		result.hostRss.peakNodeExternalBytes,
		Math.max(
			...result.hostRss.samples.map((sample) => sample.nodeExternalBytes),
		),
	);
	assert.equal(
		result.hostRss.peakNodeArrayBuffersBytes,
		Math.max(
			...result.hostRss.samples.map((sample) => sample.nodeArrayBuffersBytes),
		),
	);
	assert.equal(
		result.hostRss.samples[0].combinedBytes,
		result.hostRss.samples[0].browserBytes +
			result.hostRss.samples[0].nodeBytes,
	);
	assert.equal(detachedPageSessions, 2);
	assert.equal(detachedBrowserSessions, 2);
	assert.deepEqual(browserSessionSendCounts, [3, 3]);
});

test("records post-sampling CDP timeouts as cleanup warnings", async () => {
	let detachStarted = 0;
	const page = (hangOnCleanup) => ({
		context: () => ({
			newCDPSession: async () => ({
				send: async (method) => {
					assert.equal(method, "Runtime.getHeapUsage");
					return {
						usedSize: 100,
						totalSize: 110,
						embedderHeapUsedSize: 120,
						backingStorageSize: 130,
					};
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
		browsers: [browser],
		expectedBrowserCount: 1,
		reader: page(true),
		writer: page(false),
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 0,
		samplingWindowBudgetMs: 65_000,
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
		/Page JS heap CDP detach exceeded 3ms/,
	);
	assert.equal(detachStarted, 2);
});

test("bounds setup and detaches a CDP session created after its deadline", async () => {
	let lateDetachCount = 0;
	const session = (usedBytes, onDetach = () => {}) => ({
		send: async (method) => {
			assert.equal(method, "Runtime.getHeapUsage");
			return {
				usedSize: usedBytes,
				totalSize: usedBytes + 10,
				embedderHeapUsedSize: usedBytes + 20,
				backingStorageSize: usedBytes + 30,
			};
		},
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
		browsers: [browser],
		expectedBrowserCount: 1,
		reader,
		writer,
		downloadTimeoutMs: 60_000,
		schedulingToleranceMs: 5_000,
		postTransferSoakMs: 0,
		samplingWindowBudgetMs: 65_000,
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
