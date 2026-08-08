import { Cache } from "@peerbit/cache";
import { cpus } from "node:os";
import { performance } from "node:perf_hooks";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import {
	MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

// Deterministic, in-process microbenchmark for the state that a future
// per-peer fold would move. It deliberately avoids network/storage I/O: those
// end-to-end benches are useful context, but are too noisy to be the sole
// regression gate for Map/Set/AbortController lifecycle changes.
//
// Run with:
//   pnpm --filter @peerbit/shared-log run benchmark:sync-peer-state
//   BENCH_JSON=1 pnpm --silent --filter @peerbit/shared-log run benchmark:sync-peer-state > candidate.json
//
// Fast smoke:
//   SYNC_PEER_STATE_WARMUP=1 SYNC_PEER_STATE_SAMPLES=2 \
//     SYNC_PEER_STATE_ITERATIONS=25 BENCH_JSON=1 \
//     pnpm --filter @peerbit/shared-log run benchmark:sync-peer-state

type Scenario =
	| "simple-dispatch-quota"
	| "rateless-target-lifecycle"
	| "disconnect-reconnect-retained-physical";

type Sample = {
	elapsedMs: number;
	operations: number;
	peerOperations: number;
	nsPerOperation: number;
	nsPerPeerOperation: number;
	retainedPhysicalPermits: number;
	checksum: number;
};

type TaskResult = {
	scenario: Scenario;
	peers: number;
	samples: Sample[];
	medianNsPerOperation: number;
	medianNsPerPeerOperation: number;
};

// Keep the exact-binomial confidence interval in the comparator below the
// IEEE-754 overflow boundary (2 ** 1024). Smoke runs may use fewer than the
// comparator's seven-sample minimum, but no result may exceed this ceiling.
const MAX_MEASURED_SAMPLES = 1_000;

const parsePositiveInteger = (
	value: string | undefined,
	fallback: number,
	name: string,
	maximum = Number.MAX_SAFE_INTEGER,
): number => {
	if (value == null || value === "") {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > maximum) {
		throw new Error(
			`${name} must be a positive integer at most ${maximum}, got '${value}'`,
		);
	}
	return parsed;
};

const parsePeerCounts = (value: string | undefined): number[] => {
	const counts = (value ?? "1,8,64")
		.split(",")
		.map((token) => Number.parseInt(token.trim(), 10));
	if (
		counts.length === 0 ||
		counts.some(
			(count) => !Number.isSafeInteger(count) || count <= 0 || count > 64,
		)
	) {
		throw new Error(
			`SYNC_PEER_STATE_PEERS must contain integers in [1, 64], got '${value}'`,
		);
	}
	return [...new Set(counts)];
};

const median = (values: number[]): number => {
	const sorted = [...values].sort((left, right) => left - right);
	const middle = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? (sorted[middle - 1]! + sorted[middle]!) / 2
		: sorted[middle]!;
};

const createSimple = () =>
	new SimpleSyncronizer<"u64">({
		rpc: { send: async () => {} } as any,
		entryIndex: {} as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 128 }),
	});

const createRateless = () =>
	new RatelessIBLTSynchronizer<"u64">({
		rpc: { send: async () => {} } as any,
		rangeIndex: {} as any,
		entryIndex: {} as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 128 }),
		numbers: { maxValue: 2n ** 64n - 1n } as any,
	} as any);

const peerNames = (count: number): string[] =>
	Array.from({ length: count }, (_, index) => `peer-${index}`);

const runCleanupSteps = async (
	...steps: Array<() => void | Promise<void>>
): Promise<void> => {
	let failed = false;
	let firstError: unknown;
	for (const step of steps) {
		try {
			await step();
		} catch (error) {
			if (!failed) {
				failed = true;
				firstError = error;
			}
		}
	}
	if (failed) {
		throw firstError;
	}
};

const assertExpectedWorkload = (
	sample: Sample,
	properties: { scenario: Scenario; peers: string[]; iterations: number },
): void => {
	const expectedOperations = properties.iterations;
	const expectedPeerOperations =
		properties.iterations * properties.peers.length;
	const expectedRetainedPhysicalPermits =
		properties.scenario === "disconnect-reconnect-retained-physical"
			? Math.min(
					properties.peers.length,
					MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL,
				)
			: 0;
	if (!Number.isSafeInteger(expectedPeerOperations)) {
		throw new Error(
			"sync-peer-state peer operation count is not a safe integer",
		);
	}
	if (
		sample.operations !== expectedOperations ||
		sample.peerOperations !== expectedPeerOperations ||
		sample.checksum !== expectedPeerOperations ||
		sample.retainedPhysicalPermits !== expectedRetainedPhysicalPermits
	) {
		throw new Error(
			`${properties.scenario} benchmark performed unexpected work: ` +
				JSON.stringify({
					operations: sample.operations,
					peerOperations: sample.peerOperations,
					checksum: sample.checksum,
					retainedPhysicalPermits: sample.retainedPhysicalPermits,
					expectedOperations,
					expectedPeerOperations,
					expectedRetainedPhysicalPermits,
				}),
		);
	}
};

const measure = async (properties: {
	scenario: Scenario;
	peers: string[];
	iterations: number;
}): Promise<Sample> => {
	let checksum = 0;
	let retainedPhysicalPermits = 0;
	let runRound: () => void = () => {};
	let cleanup: () => Promise<void> = async () => {};

	try {
		if (properties.scenario === "simple-dispatch-quota") {
			const sync = createSimple();
			const state = sync as any;
			cleanup = () =>
				runCleanupSteps(
					() => {
						if (
							state.pendingCoordinateLookupCount !== 0 ||
							state.pendingCoordinateLookupCountByPeer.size !== 0 ||
							state.peerSlotRows.rows.size !== 0 ||
							state.syncDispatchRegistry.activeTargets.size !== 0
						) {
							throw new Error("simple dispatch/quota benchmark leaked state");
						}
					},
					() => sync.close(),
				);
			runRound = () => {
				const lifecycle = state.captureSyncDispatchLifecycle(properties.peers);
				try {
					for (
						let offset = 0;
						offset < properties.peers.length;
						offset += MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL
					) {
						const permits: Array<{ release: () => void }> = [];
						try {
							for (const peer of properties.peers.slice(
								offset,
								offset + MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL,
							)) {
								const permit = state.tryAcquireCoordinateLookup(peer);
								if (!permit) {
									throw new Error(
										`simple lookup quota unexpectedly rejected ${peer}`,
									);
								}
								permits.push(permit);
							}
						} finally {
							for (const permit of permits) {
								permit.release();
							}
						}
					}
					checksum += lifecycle.targets.size;
				} finally {
					state.finishSyncDispatchLifecycle(lifecycle);
				}
			};
		} else if (properties.scenario === "rateless-target-lifecycle") {
			const sync = createRateless();
			const state = sync as any;
			cleanup = () =>
				runCleanupSteps(
					() => {
						if (state.ratelessDispatchRegistry.activeTargets.size !== 0) {
							throw new Error(
								"rateless target-lifecycle benchmark leaked state",
							);
						}
					},
					() => sync.close(),
				);
			runRound = () => {
				const lifecycle = state.captureRatelessDispatchLifecycle(
					properties.peers,
				);
				for (const target of lifecycle.targets.values()) {
					target.retainedByProcess = true;
					target.responseLeases = 1;
				}
				state.finishRatelessDispatchLifecycle(lifecycle);
				for (const target of lifecycle.targets.values()) {
					target.retainedByProcess = false;
					target.responseLeases = 0;
				}
				state.maybeDisposeRatelessDispatchLifecycle(lifecycle);
				checksum += lifecycle.targets.size;
			};
		} else {
			const sync = createSimple();
			const state = sync as any;
			// Non-abortable physical lookups stay charged throughout the timed
			// reconnect churn (one per peer until the real global cap is saturated).
			// Production promises may literally never settle; the harness releases
			// these permits only after timing so repeated samples do not leak state.
			const retainedPeers = properties.peers.slice(
				0,
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_GLOBAL,
			);
			const retainedPermits: Array<{ row: any; release: () => void }> = [];
			const retainedRows: Array<{ attached: boolean; lookups: number }> = [];
			cleanup = () =>
				runCleanupSteps(
					() => {
						if (
							state.pendingCoordinateLookupCount !== retainedPermits.length ||
							state.pendingCoordinateLookupCountByPeer.size !== 0 ||
							state.peerSlotRows.rows.size !== 0 ||
							retainedRows.some((row) => row.attached || row.lookups !== 1)
						) {
							throw new Error(
								"retained physical work changed during logical churn",
							);
						}
					},
					() => {
						for (const permit of retainedPermits) {
							permit.release();
						}
					},
					() => {
						if (state.pendingCoordinateLookupCount !== 0) {
							throw new Error(
								"retained physical permits did not settle exactly once",
							);
						}
					},
					() => sync.close(),
				);
			for (const peer of retainedPeers) {
				const permit = state.tryAcquireCoordinateLookup(peer);
				if (!permit) {
					throw new Error(
						`retained physical lookup unexpectedly rejected ${peer}`,
					);
				}
				retainedPermits.push(permit);
				retainedRows.push(permit.row);
			}
			retainedPhysicalPermits = retainedPermits.length;
			for (const peer of retainedPeers) {
				sync.onPeerDisconnected(peer);
			}
			if (
				state.pendingCoordinateLookupCount !== retainedPermits.length ||
				state.pendingCoordinateLookupCountByPeer.size !== 0 ||
				state.peerSlotRows.rows.size !== 0
			) {
				throw new Error("retained physical setup violated detach accounting");
			}

			runRound = () => {
				for (const peer of properties.peers) {
					const row = state.getOrCreateSyncResponseSlotRow(peer);
					let released = false;
					const releaseLogical = () => {
						if (released) return;
						released = true;
						row.activeReleases.delete(releaseLogical);
						checksum += 1;
					};
					row.activeReleases.add(releaseLogical);
				}
				for (const peer of properties.peers) {
					sync.onPeerDisconnected(peer);
				}
			};
		}

		const startedAt = performance.now();
		for (let iteration = 0; iteration < properties.iterations; iteration += 1) {
			runRound();
		}
		const elapsedMs = performance.now() - startedAt;
		const peerOperations = properties.iterations * properties.peers.length;
		const sample = {
			elapsedMs,
			operations: properties.iterations,
			peerOperations,
			nsPerOperation: (elapsedMs * 1e6) / properties.iterations,
			nsPerPeerOperation: (elapsedMs * 1e6) / peerOperations,
			retainedPhysicalPermits,
			checksum,
		};
		assertExpectedWorkload(sample, properties);
		return sample;
	} finally {
		await cleanup();
	}
};

const peerCounts = parsePeerCounts(process.env.SYNC_PEER_STATE_PEERS);
const warmupSamples = parsePositiveInteger(
	process.env.SYNC_PEER_STATE_WARMUP,
	3,
	"SYNC_PEER_STATE_WARMUP",
);
const measuredSamples = parsePositiveInteger(
	process.env.SYNC_PEER_STATE_SAMPLES,
	15,
	"SYNC_PEER_STATE_SAMPLES",
	MAX_MEASURED_SAMPLES,
);
const iterations = parsePositiveInteger(
	process.env.SYNC_PEER_STATE_ITERATIONS,
	500,
	"SYNC_PEER_STATE_ITERATIONS",
);
const scenarios: Scenario[] = [
	"simple-dispatch-quota",
	"rateless-target-lifecycle",
	"disconnect-reconnect-retained-physical",
];
const tasks: TaskResult[] = [];

for (const peers of peerCounts) {
	const names = peerNames(peers);
	for (const scenario of scenarios) {
		for (let index = 0; index < warmupSamples; index += 1) {
			await measure({ scenario, peers: names, iterations });
		}
		const samples: Sample[] = [];
		for (let index = 0; index < measuredSamples; index += 1) {
			samples.push(await measure({ scenario, peers: names, iterations }));
		}
		tasks.push({
			scenario,
			peers,
			samples,
			medianNsPerOperation: median(
				samples.map((sample) => sample.nsPerOperation),
			),
			medianNsPerPeerOperation: median(
				samples.map((sample) => sample.nsPerPeerOperation),
			),
		});
	}
}

const result = {
	schemaVersion: 1,
	benchmark: "sync-peer-state",
	runtime: {
		node: process.versions.node,
		v8: process.versions.v8,
		platform: process.platform,
		arch: process.arch,
		cpu: cpus()[0]?.model ?? "unknown",
	},
	config: { peerCounts, warmupSamples, measuredSamples, iterations },
	tasks,
};

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} else {
	console.table(
		tasks.map((task) => ({
			scenario: task.scenario,
			peers: task.peers,
			"median us/round": task.medianNsPerOperation / 1e3,
			"median ns/peer": task.medianNsPerPeerOperation,
			samples: task.samples.length,
		})),
	);
}
