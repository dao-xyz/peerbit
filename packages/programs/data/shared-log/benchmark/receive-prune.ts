// Focused receive/prune hot-path benchmark.
//
// Run from packages/programs/data/shared-log:
//   RECEIVE_PRUNE_COUNTS=100,1000,5000 RECEIVE_PRUNE_RUNS=1 BENCH_JSON=1 pnpm run benchmark:receive-prune
//   RECEIVE_PRUNE_SCENARIOS=raw-receive-native,raw-receive-native-coordinate-wal,request-prune-native-confirm RECEIVE_PRUNE_COUNTS=1000 pnpm run benchmark:receive-prune
import { create as createRustIndexer } from "@peerbit/indexer-rust";
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { performance } from "node:perf_hooks";
import { v4 as uuid } from "uuid";
import {
	type RawExchangeHeadsMessage,
	RequestIPrune,
	createRawExchangeHeadsMessages,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import type { SyncProfileEvent } from "../src/sync/index.js";
import { EventStore } from "../test/utils/stores/event-store.js";

type Scenario =
	| "raw-receive-native"
	| "raw-receive-native-coordinate-wal"
	| "request-prune-native-confirm"
	| "request-prune-pending-ihave";

type ProfileSummary = {
	name: string;
	count: number;
	totalMs: number;
	maxMs: number;
	entries: number;
	messages: number;
	nativeBackboneOnly: number;
};

type BenchRow = {
	scenario: Scenario;
	count: number;
	run: number;
	elapsedMs: number;
	opsPerSecond: number;
	profile: ProfileSummary[];
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

const parseCounts = (value: string | undefined) =>
	(value ?? "100,1000,5000")
		.split(",")
		.map((part) => part.trim())
		.filter(Boolean)
		.map((part) => parsePositiveInteger(part, 0));

const parseScenarios = (value: string | undefined): Scenario[] => {
	const scenarios = (value ?? [
		"raw-receive-native",
		"raw-receive-native-coordinate-wal",
		"request-prune-native-confirm",
		"request-prune-pending-ihave",
	].join(","))
		.split(",")
		.map((part) => part.trim())
		.filter(Boolean);
	for (const scenario of scenarios) {
		if (
			scenario !== "raw-receive-native" &&
			scenario !== "raw-receive-native-coordinate-wal" &&
			scenario !== "request-prune-native-confirm" &&
			scenario !== "request-prune-pending-ihave"
		) {
			throw new Error(`Unknown receive/prune scenario '${scenario}'`);
		}
	}
	return scenarios as Scenario[];
};

const counts = parseCounts(process.env.RECEIVE_PRUNE_COUNTS);
const runs = parsePositiveInteger(process.env.RECEIVE_PRUNE_RUNS, 1);
const scenarios = parseScenarios(process.env.RECEIVE_PRUNE_SCENARIOS);

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-receive-prune",
};

const summarizeProfileEvents = (
	events: SyncProfileEvent[],
): ProfileSummary[] => {
	const summaries = new Map<string, ProfileSummary>();
	for (const event of events) {
		let summary = summaries.get(event.name);
		if (!summary) {
			summary = {
				name: event.name,
				count: 0,
				totalMs: 0,
				maxMs: 0,
				entries: 0,
				messages: 0,
				nativeBackboneOnly: 0,
			};
			summaries.set(event.name, summary);
		}
		const durationMs = event.durationMs ?? 0;
		summary.count += 1;
		summary.totalMs += durationMs;
		summary.maxMs = Math.max(summary.maxMs, durationMs);
		summary.entries += event.entries ?? 0;
		summary.messages += event.messages ?? 0;
		summary.nativeBackboneOnly +=
			typeof event.details?.nativeBackboneOnly === "number"
				? event.details.nativeBackboneOnly
				: 0;
	}
	return [...summaries.values()].sort((a, b) => b.totalMs - a.totalMs);
};

const createOpenArgs = (
	profileEvents: SyncProfileEvent[],
	options?: { coordinateWal?: boolean },
) => {
	const nativeBackbone = options?.coordinateWal
		? {
				optional: false,
				coordinatePersistence: new NativeBackboneCoordinatePersistence(
					new NativeBackboneMemoryCoordinatePersistenceStore(),
				),
			}
		: undefined;
	return {
		replicate: false,
		setup,
		nativeGraph: true,
		nativeBackbone,
		keep: () => true,
		timeUntilRoleMaturity: 0,
		respondToIHaveTimeout: 1,
		sync: {
			rawExchangeHeads: true,
			profile: (event: SyncProfileEvent) => profileEvents.push(event),
		},
	};
};

const appendIndependentEntries = async (
	store: EventStore<string, any>,
	count: number,
) => {
	const hashes: string[] = [];
	for (let i = 0; i < count; i++) {
		const { entry } = await store.add(uuid(), {
			meta: {
				next: [],
				gidSeed: new Uint8Array([i & 0xff, (i >>> 8) & 0xff]),
			},
			replicate: false,
			target: "none",
		});
		hashes.push(entry.hash);
	}
	return hashes;
};

const createRawMessages = async (
	store: EventStore<string, any>,
	hashes: string[],
) => {
	const messages: RawExchangeHeadsMessage[] = [];
	for await (const message of createRawExchangeHeadsMessages(
		store.log.log,
		hashes,
	)) {
		messages.push(message as RawExchangeHeadsMessage);
	}
	return messages;
};

const runRawReceive = async (
	count: number,
	run: number,
	options?: { coordinateWal?: boolean },
): Promise<BenchRow> => {
	const session = await TestSession.disconnected(2, {
		indexer: (directory) => createRustIndexer(directory),
	});
	const profileEvents: SyncProfileEvent[] = [];
	try {
		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: createOpenArgs([]),
		});
		const db2 = await session.peers[1].open(new EventStore<string, any>(), {
			args: createOpenArgs(profileEvents, options),
		});
		const hashes = await appendIndependentEntries(db1, count);
		const messages = await createRawMessages(db1, hashes);

		const started = performance.now();
		for (const message of messages) {
			await db2.log.onMessage(message, {
				from: db1.node.identity.publicKey,
			} as any);
		}
		const elapsed = performance.now() - started;

		if (db2.log.log.length !== count) {
			throw new Error(
				`Expected ${count} raw received entries, got ${db2.log.log.length}`,
			);
		}

		return {
			scenario: options?.coordinateWal
				? "raw-receive-native-coordinate-wal"
				: "raw-receive-native",
			count,
			run,
			elapsedMs: elapsed,
			opsPerSecond: (count / elapsed) * 1000,
			profile: summarizeProfileEvents(profileEvents),
		};
	} finally {
		await session.stop();
	}
};

const installFastLeaderWaits = (store: EventStore<string, any>) => {
	const log = store.log as any;
	const selfHash = store.node.identity.publicKey.hashcode();
	const originalWaitForGid = log._waitForGidReplicators;
	const originalWaitForEntry = log._waitForEntryReplicators;
	log._waitForGidReplicators = async (
		_gid: string,
		_replicas: number,
		_waitFor: unknown,
		options: { onLeader?: (key: string) => void } | undefined,
	) => {
		options?.onLeader?.(selfHash);
		return new Map([[selfHash, { intersecting: true }]]);
	};
	log._waitForEntryReplicators = async (
		_entry: unknown,
		_replicas: number,
		_waitFor: unknown,
		options: { onLeader?: (key: string) => void } | undefined,
	) => {
		options?.onLeader?.(selfHash);
		return new Map([[selfHash, { intersecting: true }]]);
	};
	return () => {
		log._waitForGidReplicators = originalWaitForGid;
		log._waitForEntryReplicators = originalWaitForEntry;
	};
};

const suppressPruneResponses = (store: EventStore<string, any>) => {
	const debounced = (store.log as any).responseToPruneDebouncedFn;
	const originalAdd = debounced.add;
	debounced.add = () => undefined;
	return () => {
		debounced.add = originalAdd;
	};
};

const clearPendingIHaves = (store: EventStore<string, any>) => {
	const pending = (store.log as any)._pendingIHave as Map<
		string,
		{ clear?: () => void }
	>;
	for (const value of pending.values()) {
		value.clear?.();
	}
	pending.clear();
};

const runRequestPruneNativeConfirm = async (
	count: number,
	run: number,
): Promise<BenchRow> => {
	const session = await TestSession.disconnected(2, {
		indexer: (directory) => createRustIndexer(directory),
	});
	const profileEvents: SyncProfileEvent[] = [];
	try {
		const db = await session.peers[1].open(new EventStore<string, any>(), {
			args: {
				...createOpenArgs(profileEvents),
				replicate: { factor: 1 },
			},
		});
		const hashes = await appendIndependentEntries(db, count);
		const restoreWaits = installFastLeaderWaits(db);
		const restoreResponses = suppressPruneResponses(db);
		try {
			const started = performance.now();
			await db.log.onMessage(new RequestIPrune({ hashes }), {
				from: session.peers[0].identity.publicKey,
			} as any);
			const elapsed = performance.now() - started;

			return {
				scenario: "request-prune-native-confirm",
				count,
				run,
				elapsedMs: elapsed,
				opsPerSecond: (count / elapsed) * 1000,
				profile: summarizeProfileEvents(profileEvents),
			};
		} finally {
			restoreResponses();
			restoreWaits();
			clearPendingIHaves(db);
		}
	} finally {
		await session.stop();
	}
};

const runRequestPrunePendingIHave = async (
	count: number,
	run: number,
): Promise<BenchRow> => {
	const session = await TestSession.disconnected(2, {
		indexer: (directory) => createRustIndexer(directory),
	});
	const sourceProfileEvents: SyncProfileEvent[] = [];
	const targetProfileEvents: SyncProfileEvent[] = [];
	try {
		const source = await session.peers[0].open(new EventStore<string, any>(), {
			args: createOpenArgs(sourceProfileEvents),
		});
		const target = await session.peers[1].open(new EventStore<string, any>(), {
			args: createOpenArgs(targetProfileEvents),
		});
		const hashes = await appendIndependentEntries(source, count);

		try {
			const started = performance.now();
			await target.log.onMessage(new RequestIPrune({ hashes }), {
				from: source.node.identity.publicKey,
			} as any);
			const elapsed = performance.now() - started;

			return {
				scenario: "request-prune-pending-ihave",
				count,
				run,
				elapsedMs: elapsed,
				opsPerSecond: (count / elapsed) * 1000,
				profile: summarizeProfileEvents(targetProfileEvents),
			};
		} finally {
			clearPendingIHaves(target);
		}
	} finally {
		await session.stop();
	}
};

const runScenario = async (
	scenario: Scenario,
	count: number,
	run: number,
): Promise<BenchRow> => {
	if (scenario === "raw-receive-native") {
		return runRawReceive(count, run);
	}
	if (scenario === "raw-receive-native-coordinate-wal") {
		return runRawReceive(count, run, { coordinateWal: true });
	}
	if (scenario === "request-prune-native-confirm") {
		return runRequestPruneNativeConfirm(count, run);
	}
	return runRequestPrunePendingIHave(count, run);
};

const rows: BenchRow[] = [];
for (const scenario of scenarios) {
	for (const count of counts) {
		for (let run = 0; run < runs; run++) {
			rows.push(await runScenario(scenario, count, run));
		}
	}
}

const aggregateRows = [...new Set(rows.map((row) => row.scenario))].flatMap(
	(scenario) =>
		counts.map((count) => {
			const samples = rows.filter(
				(row) => row.scenario === scenario && row.count === count,
			);
			const meanMs =
				samples.reduce((sum, row) => sum + row.elapsedMs, 0) / samples.length;
			const meanOps =
				samples.reduce((sum, row) => sum + row.opsPerSecond, 0) /
				samples.length;
			return {
				scenario,
				count,
				runs: samples.length,
				meanMs: Math.round(meanMs * 100) / 100,
				meanOpsPerSecond: Math.round(meanOps),
				profile: samples[0]?.profile,
			};
		}),
);

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(
		JSON.stringify(
			{
				name: "shared-log-receive-prune",
				rows,
				aggregateRows,
			},
			null,
			2,
		),
	);
} else {
	console.table(
		aggregateRows.map((row) => ({
			scenario: row.scenario,
			count: row.count,
			runs: row.runs,
			meanMs: row.meanMs,
			meanOpsPerSecond: row.meanOpsPerSecond,
		})),
	);
	console.dir(aggregateRows, { depth: 6 });
}
process.exit(process.exitCode ?? 0);
