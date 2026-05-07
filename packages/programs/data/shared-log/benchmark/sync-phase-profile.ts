// Profiles where catch-up sync spends time across simple and rateless phases.
//
// Run with:
//   cd packages/programs/data/shared-log
//   node --loader ts-node/esm ./benchmark/sync-phase-profile.ts --entries 20000 --seededEntries 10000 --runs 1
//
// JSON output:
//   node --loader ts-node/esm ./benchmark/sync-phase-profile.ts --json
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { performance } from "node:perf_hooks";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "../src/index.js";
import type { SyncProfileEvent, SyncProfileFn } from "../src/sync/index.js";
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

type ProfileRecord = SyncProfileEvent & {
	peer: string;
	atMs: number;
};

type PhaseSummary = {
	phase: string;
	count: number;
	totalMs: number;
	meanMs: number;
	maxMs: number;
	entries: number;
	symbols: number;
	messages: number;
	bytes: number;
	peers: Record<string, number>;
};

const parseArgs = () => {
	const args = process.argv.slice(2);
	const readNumber = (name: string, fallback: number) => {
		const index = args.indexOf(`--${name}`);
		const raw =
			index >= 0
				? args[index + 1]
				: process.env[`SYNC_PROFILE_${name.toUpperCase()}`];
		if (!raw) {
			return fallback;
		}
		const parsed = Number.parseInt(raw, 10);
		if (!Number.isFinite(parsed) || parsed <= 0) {
			throw new Error(`Expected --${name} to be a positive integer`);
		}
		return parsed;
	};
	const readNonNegativeNumber = (name: string, fallback: number) => {
		const index = args.indexOf(`--${name}`);
		const raw =
			index >= 0
				? args[index + 1]
				: process.env[`SYNC_PROFILE_${name.toUpperCase()}`];
		if (!raw) {
			return fallback;
		}
		const parsed = Number.parseInt(raw, 10);
		if (!Number.isFinite(parsed) || parsed < 0) {
			throw new Error(`Expected --${name} to be a non-negative integer`);
		}
		return parsed;
	};

	return {
		entryCount: readNumber("entries", 10_000),
		seededEntries: readNonNegativeNumber("seededEntries", 1_000),
		batchSize: readNumber("batchSize", 16_384),
		timeoutMs: readNumber("timeoutMs", 120_000),
		drainMs: readNonNegativeNumber("drainMs", 250),
		runs: readNumber("runs", 1),
		warmupRuns: readNonNegativeNumber("warmupRuns", 0),
		json: args.includes("--json") || process.env.BENCH_JSON === "1",
	};
};

const writeStdout = (text: string) =>
	new Promise<void>((resolve, reject) => {
		process.stdout.write(text, (error) => {
			if (error) {
				reject(error);
				return;
			}
			resolve();
		});
	});

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

class SyncProfileCollector {
	readonly records: ProfileRecord[] = [];

	profile(peer: string): SyncProfileFn {
		return (event) => {
			this.records.push({
				...event,
				peer,
				atMs: performance.now(),
			});
		};
	}
}

const summarizeRecords = (records: ProfileRecord[]): PhaseSummary[] => {
	const summaries = new Map<
		string,
		{
			count: number;
			totalMs: number;
			maxMs: number;
			entries: number;
			symbols: number;
			messages: number;
			bytes: number;
			peers: Map<string, number>;
		}
	>();

	for (const record of records) {
		let summary = summaries.get(record.name);
		if (!summary) {
			summary = {
				count: 0,
				totalMs: 0,
				maxMs: 0,
				entries: 0,
				symbols: 0,
				messages: 0,
				bytes: 0,
				peers: new Map(),
			};
			summaries.set(record.name, summary);
		}

		const durationMs = record.durationMs ?? 0;
		summary.count += 1;
		summary.totalMs += durationMs;
		summary.maxMs = Math.max(summary.maxMs, durationMs);
		summary.entries += record.entries ?? 0;
		summary.symbols += record.symbols ?? 0;
		summary.messages += record.messages ?? 0;
		summary.bytes += record.bytes ?? 0;
		summary.peers.set(record.peer, (summary.peers.get(record.peer) ?? 0) + 1);
	}

	return [...summaries.entries()]
		.map(([phase, summary]) => ({
			phase,
			count: summary.count,
			totalMs: summary.totalMs,
			meanMs: summary.count > 0 ? summary.totalMs / summary.count : 0,
			maxMs: summary.maxMs,
			entries: summary.entries,
			symbols: summary.symbols,
			messages: summary.messages,
			bytes: summary.bytes,
			peers: Object.fromEntries(summary.peers),
		}))
		.sort((a, b) => b.totalMs - a.totalMs || b.count - a.count);
};

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
};

const runCatchup = async (properties: {
	batchSize: number;
	entryCount: number;
	seededEntries: number;
	timeoutMs: number;
	drainMs: number;
	run: number;
}) => {
	const collector = new SyncProfileCollector();
	const session = await TestSession.disconnected(2);
	const store = new EventStore<string, ReplicationDomainHash<"u64">>();
	let db1: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	let db2: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	let counter1: ReturnType<typeof attachMessageCounter> | undefined;
	let counter2: ReturnType<typeof attachMessageCounter> | undefined;
	let catchupMs = 0;
	let counters = createEmptyCounters();

	try {
		db1 = await session.peers[0].open(store.clone(), {
			args: {
				replicate: { factor: 2 },
				setup,
				sync: {
					repairSweepTargetBufferSize: properties.batchSize,
					profile: collector.profile("source"),
				},
			},
		});

		db2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 2 },
				setup,
				sync: {
					repairSweepTargetBufferSize: properties.batchSize,
					profile: collector.profile("joiner"),
				},
			},
		});

		for (let i = 0; i < properties.seededEntries; i++) {
			const out = await db1.add(`seed-${i}`, { meta: { next: [] } });
			await db2.log.join([out.entry]);
		}

		for (let i = 0; i < properties.entryCount; i++) {
			await db1.add(`entry-${i}`, { meta: { next: [] } });
		}

		const expectedLength = properties.seededEntries + properties.entryCount;
		expect(db1.log.log.length).to.equal(expectedLength);
		expect(db2.log.log.length).to.equal(properties.seededEntries);

		counter1 = attachMessageCounter(db1);
		counter2 = attachMessageCounter(db2);

		await waitForResolved(() =>
			session.peers[0].dial(session.peers[1].getMultiaddrs()),
		);

		const t0 = performance.now();
		await waitForResolved(
			() => expect(db2!.log.log.length).to.equal(expectedLength),
			{ timeout: properties.timeoutMs, delayInterval: 250 },
		);
		catchupMs = performance.now() - t0;

		if (properties.drainMs > 0) {
			await new Promise((resolve) => setTimeout(resolve, properties.drainMs));
		}

		counters = mergeCounters(counter1.read(), counter2.read());
	} finally {
		counter1?.restore();
		counter2?.restore();
		await session.stop();
	}

	const records = [...collector.records];
	return {
		run: properties.run,
		catchupMs,
		counters,
		records,
		phases: summarizeRecords(records),
	};
};

const options = parseArgs();
const measuredRuns = [];
const allMeasuredRecords: ProfileRecord[] = [];
let counterTotals = createEmptyCounters();
const totalRuns = options.warmupRuns + options.runs;

for (let run = 0; run < totalRuns; run++) {
	const result = await runCatchup({
		batchSize: options.batchSize,
		entryCount: options.entryCount,
		seededEntries: options.seededEntries,
		timeoutMs: options.timeoutMs,
		drainMs: options.drainMs,
		run,
	});
	if (run < options.warmupRuns) {
		continue;
	}
	measuredRuns.push({
		run: result.run,
		catchupMs: result.catchupMs,
		counters: result.counters,
		phases: result.phases,
	});
	counterTotals = mergeCounters(counterTotals, result.counters);
	allMeasuredRecords.push(...result.records);
}

const catchupSamples = measuredRuns.map((run) => run.catchupMs);
const meanCatchupMs =
	catchupSamples.reduce((sum, value) => sum + value, 0) / catchupSamples.length;
const aggregatePhases = summarizeRecords(allMeasuredRecords);
const output = {
	name: "shared-log-sync-phase-profile",
	meta: {
		entryCount: options.entryCount,
		seededEntries: options.seededEntries,
		batchSize: options.batchSize,
		timeoutMs: options.timeoutMs,
		drainMs: options.drainMs,
		warmupRuns: options.warmupRuns,
		measuredRuns: options.runs,
	},
	task: {
		name: "rateless-catchup",
		mean_ms: meanCatchupMs,
		hz: meanCatchupMs > 0 ? 1000 / meanCatchupMs : 0,
		rme: null,
		samples: catchupSamples.length,
		counters: counterTotals,
		phases: aggregatePhases,
	},
	runs: measuredRuns,
};

if (options.json) {
	await writeStdout(`${JSON.stringify(output, null, 2)}\n`);
} else {
	console.log(
		`shared-log sync phase profile: entries=${options.entryCount} seededEntries=${options.seededEntries} batchSize=${options.batchSize} drainMs=${options.drainMs} runs=${options.runs}`,
	);
	console.log(
		`catchup mean=${meanCatchupMs.toFixed(2)}ms hz=${output.task.hz.toFixed(2)}`,
	);
	console.table(counterTotals);
	console.table(
		aggregatePhases.map((phase) => ({
			phase: phase.phase,
			count: phase.count,
			total_ms: phase.totalMs.toFixed(2),
			mean_ms: phase.meanMs.toFixed(2),
			max_ms: phase.maxMs.toFixed(2),
			entries: phase.entries,
			symbols: phase.symbols,
			messages: phase.messages,
			peers: Object.entries(phase.peers)
				.map(([peer, count]) => `${peer}:${count}`)
				.join(","),
		})),
	);
}

process.exit(process.exitCode ?? 0);
