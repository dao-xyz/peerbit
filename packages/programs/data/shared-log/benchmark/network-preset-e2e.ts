// End-to-end benchmark of the native network preset (`peerbit/rust`) against
// the all-default TS client. Two real Peerbit nodes in one process, connected
// over real TCP (127.0.0.1), open the same EventStore — mirroring
// `test/network-e2e-native.spec.ts`. Three scenarios:
//
//   cold-sync       N independent entries written on peer1 before peer2
//                   connects; measures entries/s until peer2 converges.
//   live-puts       sustained awaited puts on peer1 while peer2 replicates;
//                   measures peer2-visible throughput and per-put visibility
//                   latency (p50/p95, attributed by log-length polling).
//   stash-pressure  native leg only: a large-payload burst below and above
//                   the wire-sync stash caps (512 msgs / 64 MB). The inbound
//                   wire keeps end-to-end pace with the consumer in a 2-node
//                   run (per-frame dispatch backpressure), so to reach the
//                   FIFO boundary the receiver's program dispatch is gated
//                   (held and replayed in order) while the burst arrives —
//                   the stalled-consumer situation the caps exist for.
//                   Asserts convergence (evicted messages are recovered by
//                   the TS fallback decode) and reports how the commit-phase
//                   throughput degrades across the eviction boundary.
//
// Legs: `default` = `Peerbit.create()` (pure TS wire path) and `native` =
// `Peerbit.create(createRustPeerbitOptions())` (rust core stream + wire-sync
// receive fusion + native shared-log defaults).
//
// Strictly sequential: legs and runs never overlap; each run creates and
// stops its own pair of clients. Defaults: 2 warmup + 5 measured runs.
//
// Run with:
//   cd packages/programs/data/shared-log
//   node --loader ts-node/esm ./benchmark/network-preset-e2e.ts
//   NET_BENCH_SCENARIOS=cold-sync NET_BENCH_RUNS=3 node --loader ts-node/esm ./benchmark/network-preset-e2e.ts
//   NET_BENCH_COLD_COUNT=5000 BENCH_JSON=1 node --loader ts-node/esm ./benchmark/network-preset-e2e.ts
//
// Caveats:
// - Both nodes share one process (and one JS thread), like the rest of the
//   networked benchmarks in this directory: numbers measure the combined
//   sender+receiver pipeline, not isolated per-node cost.
// - Per-put latency attributes visibility by log-length order, which is an
//   approximation when entries commit out of order on the receiver.
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { performance } from "node:perf_hooks";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import type { SyncProfileEvent } from "../src/sync/index.js";
import { EventStore } from "../test/utils/stores/event-store.js";

type LegName = "default" | "native";

const envInt = (name: string, fallback: number) => {
	const value = process.env[name];
	return value ? Number.parseInt(value, 10) : fallback;
};

const warmupRuns = envInt("NET_BENCH_WARMUP", 2);
const measuredRuns = envInt("NET_BENCH_RUNS", 5);
const timeoutMs = envInt("NET_BENCH_TIMEOUT", 120_000);
const coldCount = envInt("NET_BENCH_COLD_COUNT", 8000);
const coldPayload = envInt("NET_BENCH_COLD_PAYLOAD", 1024);
const liveCount = envInt("NET_BENCH_LIVE_COUNT", 2000);
const livePayload = envInt("NET_BENCH_LIVE_PAYLOAD", 256);
const stashPayload = envInt("NET_BENCH_STASH_PAYLOAD", 128 * 1024);
const stashBelowCount = envInt("NET_BENCH_STASH_BELOW_COUNT", 256);
const stashAboveCount = envInt("NET_BENCH_STASH_ABOVE_COUNT", 1024);
const captureProfile = process.env.NET_BENCH_PROFILE !== "0";

const allScenarios = ["cold-sync", "live-puts", "stash-pressure"] as const;
type ScenarioName = (typeof allScenarios)[number];
const scenarios: ScenarioName[] = (
	process.env.NET_BENCH_SCENARIOS?.split(",")
		.map((x) => x.trim())
		.filter(Boolean) ?? [...allScenarios]
).map((name) => {
	if (!allScenarios.includes(name as ScenarioName)) {
		throw new Error(`Unknown scenario '${name}'`);
	}
	return name as ScenarioName;
});
const legs: LegName[] = (
	process.env.NET_BENCH_LEGS?.split(",")
		.map((x) => x.trim())
		.filter(Boolean) ?? ["default", "native"]
).map((name) => {
	if (name !== "default" && name !== "native") {
		throw new Error(`Unknown leg '${name}'`);
	}
	return name;
});

const createClient = (leg: LegName): Promise<Peerbit> =>
	leg === "native"
		? Peerbit.create({ ...createRustPeerbitOptions() })
		: Peerbit.create();

const makePayload = (index: number, size: number) => {
	const prefix = `${index}-`;
	return prefix.length >= size
		? prefix
		: prefix + "x".repeat(size - prefix.length);
};

const mean = (samples: number[]) =>
	samples.reduce((acc, x) => acc + x, 0) / samples.length;

const stdev = (samples: number[]) => {
	if (samples.length < 2) {
		return 0;
	}
	const m = mean(samples);
	return Math.sqrt(
		samples.reduce((acc, x) => acc + (x - m) * (x - m), 0) /
			(samples.length - 1),
	);
};

const percentile = (samples: number[], p: number) => {
	if (samples.length === 0) {
		return 0;
	}
	const sorted = [...samples].sort((a, b) => a - b);
	const index = Math.min(
		sorted.length - 1,
		Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
	);
	return sorted[index];
};

const fmt = (value: number, digits = 1) =>
	Number.isFinite(value) ? value.toFixed(digits) : "n/a";

type WireSyncCountersView = {
	stashed: number;
	evicted: number;
	metaReads: number;
	blockCopyOuts: number;
	released: number;
};

const wireSyncOf = (client: Peerbit) =>
	client.nativeNetwork?.wireSync as
		| { counters?: () => WireSyncCountersView; stashLength?: number }
		| undefined;

const sumProfile = (
	events: SyncProfileEvent[],
	name: string,
	pick: (event: SyncProfileEvent) => number,
) =>
	events
		.filter((event) => event.name === name)
		.reduce((sum, event) => sum + pick(event), 0);

type PeerPair = {
	peer1: Peerbit;
	peer2: Peerbit;
	db1: EventStore<string, any>;
	db2: EventStore<string, any>;
	events1: SyncProfileEvent[];
	events2: SyncProfileEvent[];
};

const openPair = async (leg: LegName): Promise<PeerPair> => {
	const peer1 = await createClient(leg);
	const peer2 = await createClient(leg);
	const events1: SyncProfileEvent[] = [];
	const events2: SyncProfileEvent[] = [];
	const store = new EventStore<string, any>();
	// no onChange consumer so the native-leg client defaults (raw
	// exchange-heads + wire-sync receive fusion) stay applicable; a
	// profile-only sync option does not override them
	const openArgs = (events: SyncProfileEvent[]) => ({
		args: {
			replicate: { factor: 1 },
			timeUntilRoleMaturity: 0,
			sync: captureProfile
				? { profile: (event: SyncProfileEvent) => events.push(event) }
				: undefined,
		},
	});
	const db1 = await peer1.open(store.clone(), openArgs(events1));
	const db2 = await peer2.open(store.clone(), openArgs(events2));
	return { peer1, peer2, db1, db2, events1, events2 };
};

const stopPair = async (pair: PeerPair) => {
	await pair.peer1.stop();
	await pair.peer2.stop();
};

type BulkSyncResult = {
	dtMs: number;
	entriesPerSecond: number;
	mbPerSecond: number;
	senderSendMs: number;
	counters?: WireSyncCountersView;
	maxStashLength: number;
	deserializeFallbacks: number;
};

/** Shared by cold-sync and stash-pressure: seed peer1, dial, time convergence. */
const bulkSyncOnce = async (
	leg: LegName,
	entryCount: number,
	payloadSize: number,
): Promise<BulkSyncResult> => {
	const pair = await openPair(leg);
	const { peer1, peer2, db1, db2, events1, events2 } = pair;
	let sampler: ReturnType<typeof setInterval> | undefined;
	try {
		for (let index = 0; index < entryCount; index++) {
			await db1.add(makePayload(index, payloadSize), { meta: { next: [] } });
		}
		expect(db1.log.log.length).to.equal(entryCount);

		let maxStashLength = 0;
		const wireSync = wireSyncOf(peer2);
		if (wireSync) {
			sampler = setInterval(() => {
				const length = wireSync.stashLength;
				if (typeof length === "number" && length > maxStashLength) {
					maxStashLength = length;
				}
			}, 2);
		}

		await peer2.dial(peer1.getMultiaddrs());
		const t0 = performance.now();
		await waitForResolved(
			() => {
				expect(db2.log.log.length).to.equal(entryCount);
			},
			{
				timeout: timeoutMs,
				delayInterval: 10,
				timeoutMessage: `${leg} bulk sync (n=${entryCount})`,
			},
		);
		const dtMs = performance.now() - t0;
		if (sampler) {
			clearInterval(sampler);
			sampler = undefined;
		}

		return {
			dtMs,
			entriesPerSecond: (entryCount / dtMs) * 1000,
			mbPerSecond: ((entryCount * payloadSize) / (1024 * 1024) / dtMs) * 1000,
			senderSendMs: sumProfile(
				events1,
				"simple.exchangeHeads",
				(event) => event.durationMs ?? 0,
			),
			counters: wireSyncOf(peer2)?.counters?.(),
			maxStashLength,
			deserializeFallbacks: sumProfile(
				events2,
				"sharedLog.rawReceive.deserializeFallback",
				() => 1,
			),
		};
	} finally {
		if (sampler) {
			clearInterval(sampler);
		}
		await stopPair(pair);
	}
};

type StashPressureResult = {
	totalDtMs: number;
	commitDtMs: number;
	entriesPerSecondCommit: number;
	mbPerSecondCommit: number;
	heldFrames: number;
	stashed: number;
	evicted: number;
	maxStashLength: number;
	deserializeFallbacks: number;
};

/**
 * Native leg only. Seeds peer1, then holds peer2's pubsub `data` dispatch for
 * the shared-log topic (frames >= the entry payload size, i.e. the raw
 * exchange-head bulk frames; small control RPC passes through) while the
 * burst arrives. The wire keeps decoding + stashing, so the stash crosses its
 * FIFO caps and evicts. The held events are then replayed in arrival order:
 * retained ids commit through the fused stash path, evicted ids fall back to
 * the TS decode path. Convergence is asserted; the commit phase is timed.
 */
const stashPressureOnce = async (
	entryCount: number,
	payloadSize: number,
): Promise<StashPressureResult> => {
	const pair = await openPair("native");
	const { peer1, peer2, db1, db2, events2 } = pair;
	const pubsub = peer2.services.pubsub as unknown as {
		dispatchEvent: (event: Event) => boolean;
	};
	const originalDispatch = pubsub.dispatchEvent.bind(peer2.services.pubsub);
	let gateInstalled = false;
	let sampler: ReturnType<typeof setInterval> | undefined;
	try {
		for (let index = 0; index < entryCount; index++) {
			await db1.add(makePayload(index, payloadSize), { meta: { next: [] } });
		}

		const wireSync = wireSyncOf(peer2);
		if (!wireSync?.counters) {
			throw new Error("native leg without a wire-sync session");
		}
		const topic = db2.log.rpc.topic;
		const held: Event[] = [];
		let gateOpen = false;
		pubsub.dispatchEvent = (event: Event): boolean => {
			if (!gateOpen && event.type === "data") {
				const detail = (
					event as CustomEvent<{
						data?: { topics?: string[]; data?: Uint8Array };
					}>
				).detail;
				if (
					detail?.data?.topics?.includes(topic) &&
					(detail.data.data?.byteLength ?? 0) >= payloadSize
				) {
					held.push(event);
					return true;
				}
			}
			return originalDispatch(event);
		};
		gateInstalled = true;

		let maxStashLength = 0;
		sampler = setInterval(() => {
			const length = wireSync.stashLength;
			if (typeof length === "number" && length > maxStashLength) {
				maxStashLength = length;
			}
		}, 2);

		await peer2.dial(peer1.getMultiaddrs());
		const t0 = performance.now();

		// hold until the burst has fully arrived: the stash-activity counter
		// stops moving (all bulk frames are either stashed or evicted)
		let lastStashed = -1;
		let stableSince = performance.now();
		await waitForResolved(
			() => {
				const stashed = wireSync.counters!().stashed;
				const now = performance.now();
				if (stashed !== lastStashed) {
					lastStashed = stashed;
					stableSince = now;
				}
				expect(stashed).to.be.greaterThan(0);
				expect(now - stableSince).to.be.greaterThanOrEqual(500);
			},
			{
				timeout: timeoutMs,
				delayInterval: 100,
				timeoutMessage: `stash pressure burst arrival (n=${entryCount})`,
			},
		);

		const commitStart = performance.now();
		gateOpen = true;
		const heldFrames = held.length;
		for (const event of held) {
			originalDispatch(event);
		}
		pubsub.dispatchEvent = originalDispatch;
		gateInstalled = false;

		await waitForResolved(
			() => {
				expect(db2.log.log.length).to.equal(entryCount);
			},
			{
				timeout: timeoutMs,
				delayInterval: 10,
				timeoutMessage: `stash pressure convergence (n=${entryCount})`,
			},
		);
		const tEnd = performance.now();
		clearInterval(sampler);
		sampler = undefined;

		const commitDtMs = tEnd - commitStart;
		const counters = wireSync.counters();
		return {
			totalDtMs: tEnd - t0,
			commitDtMs,
			entriesPerSecondCommit: (entryCount / commitDtMs) * 1000,
			mbPerSecondCommit:
				((entryCount * payloadSize) / (1024 * 1024) / commitDtMs) * 1000,
			heldFrames,
			stashed: counters.stashed,
			evicted: counters.evicted,
			maxStashLength,
			deserializeFallbacks: sumProfile(
				events2,
				"sharedLog.rawReceive.deserializeFallback",
				() => 1,
			),
		};
	} finally {
		if (gateInstalled) {
			pubsub.dispatchEvent = originalDispatch;
		}
		if (sampler) {
			clearInterval(sampler);
		}
		await stopPair(pair);
	}
};

type LivePutsResult = {
	putsPerSecond: number;
	latencyP50Ms: number;
	latencyP95Ms: number;
	latencyMeanMs: number;
	addMeanMs: number;
};

const livePutsOnce = async (
	leg: LegName,
	putCount: number,
	payloadSize: number,
): Promise<LivePutsResult> => {
	const pair = await openPair(leg);
	const { peer1, peer2, db1, db2 } = pair;
	let poller: ReturnType<typeof setInterval> | undefined;
	try {
		await peer2.dial(peer1.getMultiaddrs());
		await db1.log.waitForReplicator(peer2.identity.publicKey, {
			timeout: timeoutMs,
		});
		await db2.log.waitForReplicator(peer1.identity.publicKey, {
			timeout: timeoutMs,
		});

		// warm the path so the first measured put does not pay one-time costs
		await db1.add(makePayload(0, payloadSize), { meta: { next: [] } });
		await waitForResolved(
			() => {
				expect(db2.log.log.length).to.equal(1);
			},
			{ timeout: timeoutMs, timeoutMessage: `${leg} live-puts warm put` },
		);
		const baseline = db2.log.log.length;

		const putStart = new Array<number>(putCount);
		const putResolved = new Array<number>(putCount);
		const visibleAt = new Array<number>(putCount);
		let seen = 0;
		const observe = () => {
			const visible = Math.min(db2.log.log.length - baseline, putCount);
			if (visible > seen) {
				const now = performance.now();
				while (seen < visible) {
					visibleAt[seen++] = now;
				}
			}
		};
		poller = setInterval(observe, 1);

		for (let index = 0; index < putCount; index++) {
			putStart[index] = performance.now();
			await db1.add(makePayload(index + 1, payloadSize), {
				meta: { next: [] },
			});
			putResolved[index] = performance.now();
		}
		await waitForResolved(
			() => {
				observe();
				expect(seen).to.equal(putCount);
			},
			{
				timeout: timeoutMs,
				delayInterval: 5,
				timeoutMessage: `${leg} live puts convergence (n=${putCount})`,
			},
		);
		clearInterval(poller);
		poller = undefined;

		const latencies = visibleAt.map((visible, index) => {
			return visible - putStart[index];
		});
		const addDurations = putResolved.map(
			(resolved, index) => resolved - putStart[index],
		);
		const wallMs = visibleAt[putCount - 1] - putStart[0];
		return {
			putsPerSecond: (putCount / wallMs) * 1000,
			latencyP50Ms: percentile(latencies, 50),
			latencyP95Ms: percentile(latencies, 95),
			latencyMeanMs: mean(latencies),
			addMeanMs: mean(addDurations),
		};
	} finally {
		if (poller) {
			clearInterval(poller);
		}
		await stopPair(pair);
	}
};

type TaskReport = {
	name: string;
	samples: number;
	metrics: Record<string, { mean: number; stdev: number; unit: string }>;
	notes?: string;
};

const tasks: TaskReport[] = [];

const collect = async <T extends Record<string, number>>(
	name: string,
	runOnce: () => Promise<T>,
	units: Record<keyof T & string, string>,
	describe?: (samplesByMetric: Record<string, number[]>) => string,
): Promise<void> => {
	console.error(`# ${name}: ${warmupRuns} warmup + ${measuredRuns} measured`);
	for (let index = 0; index < warmupRuns; index++) {
		await runOnce();
		console.error(`  warmup ${index + 1}/${warmupRuns} done`);
	}
	const samplesByMetric: Record<string, number[]> = {};
	for (let index = 0; index < measuredRuns; index++) {
		const result = await runOnce();
		for (const [key, value] of Object.entries(result)) {
			(samplesByMetric[key] ??= []).push(value);
		}
		console.error(`  run ${index + 1}/${measuredRuns} done`);
	}
	const metrics: TaskReport["metrics"] = {};
	for (const [key, samples] of Object.entries(samplesByMetric)) {
		metrics[key] = {
			mean: mean(samples),
			stdev: stdev(samples),
			unit: units[key as keyof T & string] ?? "",
		};
	}
	tasks.push({
		name,
		samples: measuredRuns,
		metrics,
		notes: describe?.(samplesByMetric),
	});
};

for (const scenario of scenarios) {
	if (scenario === "cold-sync") {
		for (const leg of legs) {
			await collect(
				`cold-sync/${leg} (n=${coldCount}, payload=${coldPayload}B)`,
				async () => {
					const result = await bulkSyncOnce(leg, coldCount, coldPayload);
					return {
						dtMs: result.dtMs,
						entriesPerSecond: result.entriesPerSecond,
						mbPerSecond: result.mbPerSecond,
						senderSendMs: result.senderSendMs,
						senderSendShare:
							result.dtMs > 0 ? result.senderSendMs / result.dtMs : 0,
						evicted: result.counters?.evicted ?? 0,
						deserializeFallbacks: result.deserializeFallbacks,
					};
				},
				{
					dtMs: "ms",
					entriesPerSecond: "entries/s",
					mbPerSecond: "MB/s",
					senderSendMs: "ms",
					senderSendShare: "of wall",
					evicted: "msgs",
					deserializeFallbacks: "msgs",
				},
			);
		}
	} else if (scenario === "live-puts") {
		for (const leg of legs) {
			await collect(
				`live-puts/${leg} (n=${liveCount}, payload=${livePayload}B)`,
				async () => {
					const result = await livePutsOnce(leg, liveCount, livePayload);
					return { ...result };
				},
				{
					putsPerSecond: "puts/s",
					latencyP50Ms: "ms",
					latencyP95Ms: "ms",
					latencyMeanMs: "ms",
					addMeanMs: "ms",
				},
			);
		}
	} else {
		// stash-pressure runs only on the native leg: the FIFO caps are a
		// property of the wire-sync stash
		if (!legs.includes("native")) {
			continue;
		}
		for (const [label, count] of [
			["below-cap", stashBelowCount],
			["above-cap", stashAboveCount],
		] as const) {
			const totalMb = (count * stashPayload) / (1024 * 1024);
			await collect(
				`stash-pressure/native ${label} (n=${count}, payload=${stashPayload}B, ~${fmt(totalMb, 0)}MB)`,
				// convergence is asserted inside stashPressureOnce; evicted
				// stash entries must have been recovered by the TS fallback
				// decode for the run to get here
				() => stashPressureOnce(count, stashPayload),
				{
					totalDtMs: "ms",
					commitDtMs: "ms",
					entriesPerSecondCommit: "entries/s",
					mbPerSecondCommit: "MB/s",
					heldFrames: "msgs",
					stashed: "msgs",
					evicted: "msgs",
					maxStashLength: "msgs",
					deserializeFallbacks: "msgs",
				},
				(samples) =>
					label === "above-cap" && (samples.evicted ?? []).every((x) => x === 0)
						? "warning: burst did not exceed the stash caps (no evictions observed)"
						: "converged on every run",
			);
		}
	}
}

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(
		JSON.stringify(
			{
				name: "shared-log-network-preset-e2e",
				tasks,
				meta: {
					warmupRuns,
					measuredRuns,
					timeoutMs,
					coldCount,
					coldPayload,
					liveCount,
					livePayload,
					stashPayload,
					stashBelowCount,
					stashAboveCount,
					scenarios,
					legs,
				},
			},
			null,
			2,
		),
	);
} else {
	for (const task of tasks) {
		console.log(`\n## ${task.name} (${task.samples} runs)`);
		console.log("| metric | mean | stdev | unit |");
		console.log("| --- | --- | --- | --- |");
		for (const [key, metric] of Object.entries(task.metrics)) {
			console.log(
				`| ${key} | ${fmt(metric.mean, 2)} | ${fmt(metric.stdev, 2)} | ${metric.unit} |`,
			);
		}
		if (task.notes) {
			console.log(`\n${task.notes}`);
		}
	}
}
