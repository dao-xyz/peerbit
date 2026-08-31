/* eslint-disable no-console */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";

const workspace = process.env.PERSISTED_BENCH_WORKSPACE;
const distModule = (relativePath) =>
	workspace
		? pathToFileURL(
				join(
					workspace,
					"packages/programs/data/document/document/dist",
					relativePath,
				),
			).href
		: new URL(`../dist/${relativePath}`, import.meta.url).href;
const peerbitModule = workspace
	? pathToFileURL(join(workspace, "packages/clients/peerbit/dist/src/index.js"))
			.href
	: "peerbit";
const { Peerbit } = await import(peerbitModule);
const { policy, transform } = await import(distModule("src/index.js"));
const { Documents } = await import(distModule("src/program.js"));
const { Document, TestStore } = await import(distModule("test/data.js"));

const parseInteger = (name, fallback, minimum) => {
	const raw = process.env[name];
	const value = raw == null ? fallback : Number.parseInt(raw, 10);
	if (!Number.isSafeInteger(value) || value < minimum) {
		throw new Error(`${name} must be an integer >= ${minimum}, got ${raw}`);
	}
	return value;
};

const mode = process.env.PERSISTED_BENCH_MODE ?? "persisted";
if (mode !== "persisted" && mode !== "no-receipt") {
	throw new Error(
		`PERSISTED_BENCH_MODE must be "persisted" or "no-receipt", got ${mode}`,
	);
}
const count = parseInteger("PERSISTED_BENCH_COUNT", 100_000, 1);
const warmupCount = parseInteger("PERSISTED_BENCH_WARMUP", 256, 0);
const timeout = parseInteger("PERSISTED_BENCH_TIMEOUT_MS", 1_800_000, 1);
const progressIntervalMs = parseInteger("PERSISTED_BENCH_PROGRESS_MS", 0, 0);
const verifyReopen = process.env.PERSISTED_BENCH_VERIFY_REOPEN !== "0";
const label = process.env.PERSISTED_BENCH_LABEL;
const directory = await mkdtemp(join(tmpdir(), "peerbit-persisted-bench-"));

const HISTOGRAM_BOUNDS = [1, 4, 16, 64, 128, 256, 512, 1_024, 5_000];
const createSummary = () => ({
	count: 0,
	total: 0,
	max: null,
	buckets: Array(HISTOGRAM_BOUNDS.length + 1).fill(0),
});
const observe = (summary, value) => {
	if (!Number.isFinite(value)) return;
	summary.count++;
	summary.total += value;
	summary.max = summary.max == null ? value : Math.max(summary.max, value);
	const bucket = HISTOGRAM_BOUNDS.findIndex(
		(upperBound) => value <= upperBound,
	);
	summary.buckets[bucket < 0 ? HISTOGRAM_BOUNDS.length : bucket]++;
};
const summarize = (summary) => ({
	count: summary.count,
	total: summary.total,
	mean: summary.count === 0 ? null : summary.total / summary.count,
	max: summary.max,
	histogram: {
		upperBounds: HISTOGRAM_BOUNDS,
		buckets: summary.buckets,
	},
});

const receiptMetrics = {
	requestCount: 0,
	requestedHashes: 0,
	confirmedHashes: 0,
	chunkSizes: createSummary(),
	requestDurationsMs: createSummary(),
};
const admissionMetrics = {
	waitCount: 0,
	confirmedCount: 0,
	hashes: 0,
	chunkSizes: createSummary(),
	durationsMs: createSummary(),
};

const storeId = Uint8Array.from(
	{ length: 32 },
	(_, index) => (index * 17 + 11) & 0xff,
);
const createStore = () => {
	const store = new TestStore({ docs: new Documents({ id: storeId }) });
	store.id = storeId;
	return store;
};
const openArgs = (replicate) => ({
	replicas: { min: 1 },
	replicate,
	timeUntilRoleMaturity: 0,
	canPerform: policy.allowAll(),
	index: {
		type: Document,
		transform: transform.identity(),
	},
});
const documentId = (prefix, index) =>
	`${prefix}-${index.toString().padStart(6, "0")}`;
const makeDocuments = (prefix, length) =>
	Array.from(
		{ length },
		(_, index) =>
			new Document({
				id: documentId(prefix, index),
				name: `deterministic-${index % 97}`,
				number: BigInt(index),
				data: Uint8Array.from(
					{ length: 32 },
					(_value, byte) => (index + byte * 13) & 0xff,
				),
			}),
	);
const waitFor = async (predicate, description, timeoutMs = timeout) => {
	const deadline = Date.now() + timeoutMs;
	let lastError;
	while (Date.now() < deadline) {
		try {
			if (await predicate()) return;
		} catch (error) {
			lastError = error;
		}
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(
		`Timed out waiting for ${description}${lastError ? `: ${String(lastError)}` : ""}`,
	);
};
const cleanupStep = async (name, action) => {
	const deadline = Date.now() + 60_000;
	let retries = 0;
	while (true) {
		try {
			await action?.();
			if (process.env.PERSISTED_BENCH_DEBUG_CLEANUP === "1") {
				console.error(`cleanup: ${name} completed after ${retries} retries`);
			}
			return;
		} catch (error) {
			if (
				error?.name !== "TerminalOperationNotStartedError" ||
				Date.now() >= deadline
			) {
				throw error;
			}
			retries++;
			await new Promise((resolve) => setTimeout(resolve, 25));
		}
	}
};
const readEndpoints = async (store) => {
	const [first, last, size] = await Promise.all([
		store.docs.get(documentId("measured", 0), {
			local: true,
			remote: false,
		}),
		store.docs.get(documentId("measured", count - 1), {
			local: true,
			remote: false,
		}),
		store.docs.index.getSize(),
	]);
	if (
		size !== warmupCount + count ||
		first?.name !== "deterministic-0" ||
		last?.name !== `deterministic-${(count - 1) % 97}`
	) {
		throw new Error(
			`Document verification failed: ${JSON.stringify({
				size,
				first: first?.name,
				last: last?.name,
			})}`,
		);
	}
	return { size, first: first.name, last: last.name };
};

let writer;
let receiver;
let writerStore;
let receiverStore;
let rssSampler;
let progressSampler;
let peakRssBytes;
let measuredStartedAt;
let localCommitMs;
let deliveryWaitMs;
let transferWaitMs;
let phase = "setup";

try {
	writer = await Peerbit.create();
	receiver = await Peerbit.create({ directory });
	await writer.dial(receiver);

	writerStore = createStore();
	receiverStore = createStore();
	await writer.open(writerStore, { args: openArgs(false) });
	await receiver.open(receiverStore, {
		args: openArgs({ offset: 0, factor: 1 }),
	});

	const sharedLog = writerStore.docs.log;
	const receiverHash = receiver.identity.publicKey.hashcode();
	await sharedLog.waitForReplicator(receiver.identity.publicKey, {
		roleAge: 0,
		timeout,
	});
	await waitFor(
		() =>
			((sharedLog._peerSyncCapabilities.get(receiverHash) ?? 0) & (1 << 5)) !==
			0,
		"the receiver persisted-receipt capability",
	);

	const originalRequest = sharedLog.rpc.request.bind(sharedLog.rpc);
	sharedLog.rpc.request = async (message, options) => {
		const measuredReceipt =
			phase === "measured" &&
			message?.constructor?.name === "RequestPersistedEntriesV1";
		const chunkSize = measuredReceipt ? (message.hashes?.length ?? 0) : 0;
		if (measuredReceipt) {
			receiptMetrics.requestCount++;
			receiptMetrics.requestedHashes += chunkSize;
			observe(receiptMetrics.chunkSizes, chunkSize);
		}
		const startedAt = performance.now();
		try {
			const responses = await originalRequest(message, options);
			if (measuredReceipt) {
				const confirmed = responses.reduce(
					(sum, response) => sum + (response.response?.hashes?.length ?? 0),
					0,
				);
				receiptMetrics.confirmedHashes += confirmed;
			}
			return responses;
		} finally {
			if (measuredReceipt) {
				observe(
					receiptMetrics.requestDurationsMs,
					performance.now() - startedAt,
				);
			}
		}
	};

	const originalAdmissionWait =
		sharedLog.waitForPersistedTransferAdmission.bind(sharedLog);
	sharedLog.waitForPersistedTransferAdmission = async (
		peer,
		hashes,
		...args
	) => {
		if (phase !== "measured") {
			return originalAdmissionWait(peer, hashes, ...args);
		}
		admissionMetrics.waitCount++;
		admissionMetrics.hashes += hashes.length;
		observe(admissionMetrics.chunkSizes, hashes.length);
		const startedAt = performance.now();
		try {
			const confirmed = await originalAdmissionWait(peer, hashes, ...args);
			if (confirmed) admissionMetrics.confirmedCount++;
			return confirmed;
		} finally {
			observe(admissionMetrics.durationsMs, performance.now() - startedAt);
		}
	};

	if (mode === "persisted") {
		const originalDeliver =
			sharedLog.deliverPersistedPlanningEntries.bind(sharedLog);
		sharedLog.deliverPersistedPlanningEntries = async (...args) => {
			if (phase !== "measured") return originalDeliver(...args);
			const deliveryStartedAt = performance.now();
			localCommitMs = deliveryStartedAt - measuredStartedAt;
			try {
				return await originalDeliver(...args);
			} finally {
				deliveryWaitMs = performance.now() - deliveryStartedAt;
			}
		};
	}

	const putOptions =
		mode === "persisted"
			? {
					unique: true,
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						timeout,
					},
				}
			: {
					unique: true,
					target: "none",
					replicate: false,
					delivery: false,
				};
	const putBatch = async (batch) => {
		const result = await writerStore.docs.putMany(batch, putOptions);
		if (mode === "persisted" || result.entries.length === 0) return result;

		const transferStartedAt = performance.now();
		if (phase === "measured") {
			localCommitMs = transferStartedAt - measuredStartedAt;
		}
		// Match persisted delivery's staged local commit, fresh ownership plan and
		// admitted 256-entry bulk transfer. Only the receipt RPC is omitted.
		await sharedLog.planPersistedDeliveryLeaders(
			result.entries,
			1,
			sharedLog.captureReplicationOwnershipLifecycle(),
		);
		const captured = sharedLog.persistedReceiptPeerSession(receiverHash);
		if (!captured) {
			throw new Error(
				"Receiver receipt capability/session is no longer current",
			);
		}
		const signal = AbortSignal.timeout(timeout);
		const isStillCurrent = () => {
			const current = sharedLog.persistedReceiptPeerSession(receiverHash);
			return (
				!signal.aborted &&
				current?.capabilitySession === captured.capabilitySession &&
				current?.peerSession === captured.peerSession
			);
		};
		await sharedLog.pushEntryHashes(
			receiverHash,
			result.entries.map((entry) => entry.hash),
			{
				acknowledge: false,
				chunkSize: 256,
				isStillCurrent,
				onChunkSent: async (chunk) => {
					await sharedLog.waitForPersistedTransferAdmission(
						receiverHash,
						chunk,
						captured,
						signal,
						isStillCurrent,
					);
					// Admission confirmations are best-effort and can be lost after
					// the data committed (the 100k first chunk reliably exercises this).
					// Production persisted delivery resolves that uncertainty through
					// its receipt round. The receipt-free cohort keeps the same 2-second
					// pacing attempt, then continues; exact receiver count and endpoint
					// checks below still fail the run if any data was actually lost.
					return true;
				},
				signal,
			},
		);
		if (phase === "measured") {
			transferWaitMs = performance.now() - transferStartedAt;
		}
		return result;
	};

	if (warmupCount > 0) {
		phase = "warmup";
		await putBatch(makeDocuments("warmup", warmupCount));
		await waitFor(
			async () => (await receiverStore.docs.index.getSize()) === warmupCount,
			"warmup convergence",
		);
	}

	const documents = makeDocuments("measured", count);
	globalThis.gc?.();
	const rssBeforeBytes = process.memoryUsage().rss;
	peakRssBytes = rssBeforeBytes;
	rssSampler = setInterval(() => {
		peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
	}, 25);
	rssSampler.unref?.();
	const cpuBefore = process.cpuUsage();
	phase = "measured";
	measuredStartedAt = performance.now();
	if (progressIntervalMs > 0) {
		let progressReadInFlight = false;
		progressSampler = setInterval(() => {
			if (progressReadInFlight) return;
			progressReadInFlight = true;
			void receiverStore.docs.index
				.getSize()
				.then((receiverCount) => {
					console.error(
						JSON.stringify({
							event: "persisted-benchmark-progress",
							label,
							mode,
							elapsedMs: performance.now() - measuredStartedAt,
							receiverCount,
							receiptRequests: receiptMetrics.requestCount,
							receiptRequestedHashes: receiptMetrics.requestedHashes,
							admissionWaits: admissionMetrics.waitCount,
							admissionConfirmed: admissionMetrics.confirmedCount,
							rssBytes: process.memoryUsage().rss,
							peakRssBytes,
						}),
					);
				})
				.catch((error) => {
					console.error(
						JSON.stringify({
							event: "persisted-benchmark-progress-error",
							error: String(error),
						}),
					);
				})
				.finally(() => {
					progressReadInFlight = false;
				});
		}, progressIntervalMs);
		progressSampler.unref?.();
	}

	await putBatch(documents);
	const operationWallMs = performance.now() - measuredStartedAt;
	const expectedReceiverCount = warmupCount + count;
	await waitFor(
		async () =>
			(await receiverStore.docs.index.getSize()) === expectedReceiverCount,
		"receiver document-index convergence",
	);
	const receiverConvergenceWallMs = performance.now() - measuredStartedAt;
	const cpu = process.cpuUsage(cpuBefore);
	clearInterval(rssSampler);
	rssSampler = undefined;
	if (progressSampler) {
		clearInterval(progressSampler);
		progressSampler = undefined;
	}
	phase = "verify";

	const writerCount = await writerStore.docs.index.getSize();
	if (writerCount !== expectedReceiverCount) {
		throw new Error(
			`Writer index expected ${expectedReceiverCount}, received ${writerCount}`,
		);
	}
	const receiverDocuments = await readEndpoints(receiverStore);
	const receiverLogLength = receiverStore.docs.log.log.length;
	if (receiverLogLength !== expectedReceiverCount) {
		throw new Error(
			`Receiver log expected ${expectedReceiverCount}, received ${receiverLogLength}`,
		);
	}
	if (mode === "no-receipt" && receiptMetrics.requestCount !== 0) {
		throw new Error("The no-receipt baseline unexpectedly sent a receipt RPC");
	}

	let reopen = null;
	if (verifyReopen) {
		const reopenStartedAt = performance.now();
		await cleanupStep("receiver store before reopen", () =>
			receiverStore?.close(),
		);
		receiverStore = undefined;
		await cleanupStep("receiver peer before reopen", () => receiver?.stop());
		receiver = undefined;

		receiver = await Peerbit.create({ directory });
		receiverStore = createStore();
		await receiver.open(receiverStore, {
			args: openArgs({ offset: 0, factor: 1 }),
		});
		await waitFor(
			async () =>
				(await receiverStore.docs.index.getSize()) === expectedReceiverCount,
			"reopened receiver document index",
		);
		reopen = {
			wallMs: performance.now() - reopenStartedAt,
			...(await readEndpoints(receiverStore)),
			logLength: receiverStore.docs.log.log.length,
		};
		if (reopen.logLength !== expectedReceiverCount) {
			throw new Error(
				`Reopened log expected ${expectedReceiverCount}, received ${reopen.logLength}`,
			);
		}
	}

	console.log(
		JSON.stringify({
			label,
			mode,
			count,
			warmupCount,
			timeout,
			operationWallMs,
			receiverConvergenceWallMs,
			documentsPerSecond: count / (receiverConvergenceWallMs / 1_000),
			localCommitMs: localCommitMs ?? null,
			deliveryWaitMs: deliveryWaitMs ?? null,
			transferWaitMs: transferWaitMs ?? null,
			receipts: {
				requestCount: receiptMetrics.requestCount,
				requestedHashes: receiptMetrics.requestedHashes,
				confirmedHashes: receiptMetrics.confirmedHashes,
				chunkSizes: summarize(receiptMetrics.chunkSizes),
				requestDurationsMs: summarize(receiptMetrics.requestDurationsMs),
			},
			admission: {
				waitCount: admissionMetrics.waitCount,
				confirmedCount: admissionMetrics.confirmedCount,
				hashes: admissionMetrics.hashes,
				chunkSizes: summarize(admissionMetrics.chunkSizes),
				durationsMs: summarize(admissionMetrics.durationsMs),
			},
			writerCount,
			receiver: {
				...receiverDocuments,
				logLength: receiverLogLength,
			},
			reopen,
			rssBeforeBytes,
			peakRssBytes,
			rssAfterBytes: process.memoryUsage().rss,
			cpuUserMs: cpu.user / 1_000,
			cpuSystemMs: cpu.system / 1_000,
			node: process.version,
			platform: `${process.platform}-${process.arch}`,
		}),
	);
} catch (error) {
	console.error(
		JSON.stringify({
			event: "persisted-benchmark-failure",
			label,
			mode,
			count,
			phase,
			elapsedMs:
				measuredStartedAt == null
					? undefined
					: performance.now() - measuredStartedAt,
			receiptRequests: receiptMetrics.requestCount,
			receiptRequestedHashes: receiptMetrics.requestedHashes,
			admissionWaits: admissionMetrics.waitCount,
			admissionConfirmed: admissionMetrics.confirmedCount,
			error:
				error instanceof Error
					? `${error.name}: ${error.message}`
					: String(error),
		}),
	);
	throw error;
} finally {
	if (rssSampler) clearInterval(rssSampler);
	if (progressSampler) clearInterval(progressSampler);
	await cleanupStep("writer store", () => writerStore?.close());
	await cleanupStep("receiver store", () => receiverStore?.close());
	await cleanupStep("writer peer", () => writer?.stop());
	await cleanupStep("receiver peer", () => receiver?.stop());
	await rm(directory, { recursive: true, force: true });
}

// The native benchmark runtime keeps a background handle alive after explicit
// shutdown, so terminate only after every store, peer and temporary file closes.
process.exit(0);
