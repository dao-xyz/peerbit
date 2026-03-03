// Benchmarks catch-up sync behavior while varying repair sweep batch size.
//
// Run with:
//   cd packages/programs/data/shared-log
//   SWEEP_BATCH_SIZES="256,512,1024,4096,16384,65536" SWEEP_ENTRY_COUNT=20000 \
//     SWEEP_RUNS=1 SWEEP_TIMEOUT=120000 node --loader ts-node/esm ./benchmark/sync-batch-sweep.ts
//
// CI assertion env vars:
// - SWEEP_ASSERT_MAX_RATIO: max allowed (slowest_mean / fastest_mean)
// - SWEEP_ASSERT_REQUIRE_STARTSYNC_FOR_BATCH_GTE: require StartSync traffic for batches >= value
// - SWEEP_ASSERT_MAX_MEAN_MS: comma list "batch:maxMs,batch:maxMs"
import { keys } from "@libp2p/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { performance } from "node:perf_hooks";
import { createReplicationDomainHash, type ReplicationDomainHash } from "../src/index.js";
import {
	MoreSymbols,
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";
import {
	RequestMaybeSync,
	RequestMaybeSyncCoordinate,
} from "../src/sync/simple.js";
import { EventStore } from "../test/utils/stores/event-store.js";

type MessageCounters = {
	startSync: number;
	moreSymbols: number;
	requestAll: number;
	requestMaybeSync: number;
	requestMaybeSyncCoordinate: number;
};

type BatchTask = {
	name: string;
	batchSize: number;
	mean_ms: number;
	hz: number;
	rme: null;
	samples: number;
	startSyncTotal: number;
	moreSymbolsTotal: number;
	requestAllTotal: number;
	requestMaybeSyncTotal: number;
	requestMaybeSyncCoordinateTotal: number;
};

const parsePositiveInteger = (value: string | undefined, fallback: number) => {
	if (!value) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		throw new Error(`Expected a positive integer, got '${value}'`);
	}
	return parsed;
};

const parseNonNegativeInteger = (value: string | undefined, fallback: number) => {
	if (!value) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed < 0) {
		throw new Error(`Expected a non-negative integer, got '${value}'`);
	}
	return parsed;
};

const parseNumberList = (value: string | undefined, defaults: number[]) => {
	if (!value) return defaults;
	const parsed = value
		.split(",")
		.map((x) => Number.parseInt(x.trim(), 10))
		.filter((x) => Number.isFinite(x) && x > 0);
	return parsed.length > 0 ? parsed : defaults;
};

const parseBatchThresholdMap = (value: string | undefined) => {
	const thresholds = new Map<number, number>();
	if (!value) return thresholds;
	for (const token of value.split(",")) {
		const [batchRaw, thresholdRaw] = token.split(":");
		if (!batchRaw || !thresholdRaw) {
			throw new Error(
				`Invalid SWEEP_ASSERT_MAX_MEAN_MS token '${token}', expected 'batch:maxMs'`,
			);
		}
		const batch = Number.parseInt(batchRaw.trim(), 10);
		const threshold = Number.parseFloat(thresholdRaw.trim());
		if (!Number.isFinite(batch) || batch <= 0) {
			throw new Error(
				`Invalid batch '${batchRaw}' in SWEEP_ASSERT_MAX_MEAN_MS token '${token}'`,
			);
		}
		if (!Number.isFinite(threshold) || threshold <= 0) {
			throw new Error(
				`Invalid threshold '${thresholdRaw}' in SWEEP_ASSERT_MAX_MEAN_MS token '${token}'`,
			);
		}
		thresholds.set(batch, threshold);
	}
	return thresholds;
};

const parseOptionalPositiveFloat = (value: string | undefined) => {
	if (!value) {
		return undefined;
	}
	const parsed = Number.parseFloat(value);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		throw new Error(`Expected a positive number, got '${value}'`);
	}
	return parsed;
};

const parseOptionalPositiveInteger = (value: string | undefined) => {
	if (!value) {
		return undefined;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		throw new Error(`Expected a positive integer, got '${value}'`);
	}
	return parsed;
};

const createEmptyCounters = (): MessageCounters => ({
	startSync: 0,
	moreSymbols: 0,
	requestAll: 0,
	requestMaybeSync: 0,
	requestMaybeSyncCoordinate: 0,
});

const mergeCounters = (
	left: MessageCounters,
	right: MessageCounters,
): MessageCounters => ({
	startSync: left.startSync + right.startSync,
	moreSymbols: left.moreSymbols + right.moreSymbols,
	requestAll: left.requestAll + right.requestAll,
	requestMaybeSync: left.requestMaybeSync + right.requestMaybeSync,
	requestMaybeSyncCoordinate:
		left.requestMaybeSyncCoordinate + right.requestMaybeSyncCoordinate,
});

const attachMessageCounter = (
	store: EventStore<string, ReplicationDomainHash<"u64">>,
) => {
	const counters = createEmptyCounters();
	const original = store.log.onMessage.bind(store.log);

	store.log.onMessage = async (msg, context) => {
		if (msg instanceof StartSync) counters.startSync += 1;
		else if (msg instanceof MoreSymbols) counters.moreSymbols += 1;
		else if (msg instanceof RequestAll) counters.requestAll += 1;
		else if (msg instanceof RequestMaybeSync) counters.requestMaybeSync += 1;
		else if (msg instanceof RequestMaybeSyncCoordinate)
			counters.requestMaybeSyncCoordinate += 1;

		return original(msg, context);
	};

	return {
		read: () => ({ ...counters }),
		restore: () => {
			store.log.onMessage = original;
		},
	};
};

const fixedKeys = [
	{
		libp2p: {
			privateKey: keys.privateKeyFromRaw(
				new Uint8Array([
					204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
					168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
					159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53, 142,
					57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72, 148, 82,
					66, 138, 199, 185,
				]),
			),
		},
	},
	{
		libp2p: {
			privateKey: keys.privateKeyFromRaw(
				new Uint8Array([
					237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
					157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120, 122,
					222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251, 100, 168,
					49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6, 174, 212, 159,
					187, 2, 137, 47, 192,
				]),
			),
		},
	},
];

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
};

const runCatchup = async (properties: {
	batchSize: number;
	entryCount: number;
	timeoutMs: number;
}) => {
	const session = await TestSession.disconnected(2, fixedKeys);
	const store = new EventStore<string, ReplicationDomainHash<"u64">>();
	let db1: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	let db2: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	let counter1:
		| ReturnType<typeof attachMessageCounter>
		| undefined;
	let counter2:
		| ReturnType<typeof attachMessageCounter>
		| undefined;

	try {
		db1 = await session.peers[0].open(store.clone(), {
			args: {
				replicate: { factor: 2 },
				setup,
				sync: {
					repairSweepTargetBufferSize: properties.batchSize,
				},
			},
		});

		// Preload while only a single replica is active to avoid delivery-timeout
		// artifacts when very large entry counts are written offline.
		for (let i = 0; i < properties.entryCount; i++) {
			await db1.add(`entry-${i}`, { meta: { next: [] } });
		}

		expect(db1.log.log.length).to.equal(properties.entryCount);

		db2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 2 },
				setup,
				sync: {
					repairSweepTargetBufferSize: properties.batchSize,
				},
			},
		});

		expect(db2.log.log.length).to.equal(0);

		counter1 = attachMessageCounter(db1);
		counter2 = attachMessageCounter(db2);

		await waitForResolved(() =>
			session.peers[0].dial(session.peers[1].getMultiaddrs()),
		);

		const t0 = performance.now();
		await waitForResolved(
			() => expect(db2!.log.log.length).to.equal(properties.entryCount),
			{ timeout: properties.timeoutMs, delayInterval: 250 },
		);
		const catchupMs = performance.now() - t0;

		const counters = mergeCounters(counter1.read(), counter2.read());
		return { catchupMs, counters };
	} finally {
		counter1?.restore();
		counter2?.restore();
		await session.stop();
	}
};

const mean = (values: number[]) =>
	values.reduce((acc, value) => acc + value, 0) / values.length;

const batchSizes = parseNumberList(process.env.SWEEP_BATCH_SIZES, [
	256,
	512,
	1024,
	4096,
	16384,
	65536,
]);
const entryCount = parsePositiveInteger(process.env.SWEEP_ENTRY_COUNT, 20_000);
const timeoutMs = parsePositiveInteger(process.env.SWEEP_TIMEOUT, 120_000);
const warmupRuns = parseNonNegativeInteger(process.env.SWEEP_WARMUP_RUNS, 0);
const measuredRuns = parsePositiveInteger(process.env.SWEEP_RUNS, 1);
const assertMaxRatio = parseOptionalPositiveFloat(process.env.SWEEP_ASSERT_MAX_RATIO);
const assertStartSyncForBatchGte = parseOptionalPositiveInteger(
	process.env.SWEEP_ASSERT_REQUIRE_STARTSYNC_FOR_BATCH_GTE,
);
const assertMaxMeanByBatch = parseBatchThresholdMap(
	process.env.SWEEP_ASSERT_MAX_MEAN_MS,
);

const tasks: BatchTask[] = [];

for (const batchSize of batchSizes) {
	let counterTotals = createEmptyCounters();
	const samples: number[] = [];
	const totalRuns = warmupRuns + measuredRuns;

	for (let run = 0; run < totalRuns; run++) {
		const result = await runCatchup({ batchSize, entryCount, timeoutMs });
		if (run < warmupRuns) {
			continue;
		}
		samples.push(result.catchupMs);
		counterTotals = mergeCounters(counterTotals, result.counters);
	}

	const meanMs = mean(samples);
	tasks.push({
		name: `repairSweepTargetBufferSize=${batchSize}`,
		batchSize,
		mean_ms: meanMs,
		hz: meanMs > 0 ? 1000 / meanMs : 0,
		rme: null,
		samples: samples.length,
		startSyncTotal: counterTotals.startSync,
		moreSymbolsTotal: counterTotals.moreSymbols,
		requestAllTotal: counterTotals.requestAll,
		requestMaybeSyncTotal: counterTotals.requestMaybeSync,
		requestMaybeSyncCoordinateTotal: counterTotals.requestMaybeSyncCoordinate,
	});
}

const failures: string[] = [];

if (assertMaxRatio != null && tasks.length > 1) {
	const means = tasks.map((task) => task.mean_ms);
	const ratio = Math.max(...means) / Math.min(...means);
	if (ratio > assertMaxRatio) {
		failures.push(
			`max mean ratio ${ratio.toFixed(3)} exceeded threshold ${assertMaxRatio.toFixed(3)}`,
		);
	}
}

if (assertStartSyncForBatchGte != null) {
	for (const task of tasks) {
		if (
			task.batchSize >= assertStartSyncForBatchGte &&
			task.startSyncTotal === 0
		) {
			failures.push(
				`batch ${task.batchSize} produced zero StartSync messages (expected rateless path)`,
			);
		}
	}
}

for (const [batchSize, maxMeanMs] of assertMaxMeanByBatch) {
	const task = tasks.find((x) => x.batchSize === batchSize);
	if (!task) {
		failures.push(`missing configured batch size ${batchSize} for max mean assertion`);
		continue;
	}
	if (task.mean_ms > maxMeanMs) {
		failures.push(
			`batch ${batchSize} mean ${task.mean_ms.toFixed(3)}ms exceeded max ${maxMeanMs.toFixed(3)}ms`,
		);
	}
}

const output = {
	name: "shared-log-sync-batch-sweep",
	tasks,
	meta: {
		entryCount,
		timeoutMs,
		warmupRuns,
		measuredRuns,
		batchSizes,
	},
};

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(JSON.stringify(output, null, 2));
} else {
	console.table(
		tasks.map((task) => ({
			batch: task.batchSize,
			mean_ms: task.mean_ms.toFixed(2),
			hz: task.hz.toFixed(2),
			startSync: task.startSyncTotal,
			moreSymbols: task.moreSymbolsTotal,
			requestAll: task.requestAllTotal,
			requestMaybeSync: task.requestMaybeSyncTotal,
			requestMaybeSyncCoordinate: task.requestMaybeSyncCoordinateTotal,
		})),
	);
}

if (failures.length > 0) {
	throw new Error(
		`sync-batch-sweep assertions failed:\n- ${failures.join("\n- ")}`,
	);
}
