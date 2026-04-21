// Benchmarks late historical join backfill without test-driven rebalance.
//
// Run with:
//   pnpm --filter @peerbit/shared-log run benchmark:join-backfill-repair -- --entries 10000 --runs 1 --json
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { performance } from "node:perf_hooks";
import {
	AbsoluteReplicas,
	createReplicationDomainHash,
	decodeReplicas,
	type ReplicationDomainHash,
} from "../src/index.js";
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

type BenchmarkArgs = {
	entries: number;
	runs: number;
	timeoutMs: number;
	json: boolean;
};

type MessageCounters = {
	startSync: number;
	moreSymbols: number;
	requestAll: number;
	requestMaybeSync: number;
	requestMaybeSyncCoordinate: number;
};

type RepairMetricBucket = {
	dispatches: number;
	entries: number;
	ratelessFirstPasses: number;
	simpleFallbackPasses: number;
};

type RepairMetrics = Record<"join-warmup" | "join-authoritative" | "churn", RepairMetricBucket>;

type RunResult = {
	run: number;
	entries: number;
	hydrationMs: number;
	recoveryPath: string;
	repair: RepairMetrics;
	messages: MessageCounters;
};

const defaults: BenchmarkArgs = {
	entries: 10_000,
	runs: 1,
	timeoutMs: 180_000,
	json: false,
};

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
};

const usage = () => {
	console.log(`Run with "pnpm --filter @peerbit/shared-log run benchmark:join-backfill-repair -- [options]"

Options:
  --entries N      historical entries before the joiner opens (default: ${defaults.entries})
  --runs N         repeated runs (default: ${defaults.runs})
  --timeoutMs N    hydration timeout in ms (default: ${defaults.timeoutMs})
  --json           emit JSON
  --help           show this message
`);
};

const parseArgs = (argv: string[]): BenchmarkArgs => {
	const out = { ...defaults };
	const consume = (index: number) => {
		const value = argv[index + 1];
		if (value == null) throw new Error(`Missing value for ${argv[index]}`);
		return value;
	};
	for (let i = 0; i < argv.length; i++) {
		if (argv[i] === "--") continue;
		switch (argv[i]) {
			case "--entries":
				out.entries = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--runs":
				out.runs = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--timeoutMs":
				out.timeoutMs = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--json":
				out.json = true;
				break;
			case "--help":
				usage();
				process.exit(0);
			default:
				if (argv[i].startsWith("--")) {
					throw new Error(`Unknown argument: ${argv[i]}`);
				}
		}
	}
	if (!Number.isFinite(out.entries) || out.entries <= 0) throw new Error(`Expected --entries > 0, got '${out.entries}'`);
	if (!Number.isFinite(out.runs) || out.runs <= 0) throw new Error(`Expected --runs > 0, got '${out.runs}'`);
	if (!Number.isFinite(out.timeoutMs) || out.timeoutMs <= 0) throw new Error(`Expected --timeoutMs > 0, got '${out.timeoutMs}'`);
	return out;
};

const emptyCounters = (): MessageCounters => ({
	startSync: 0,
	moreSymbols: 0,
	requestAll: 0,
	requestMaybeSync: 0,
	requestMaybeSyncCoordinate: 0,
});

const emptyRepairMetrics = (): RepairMetrics => ({
	"join-warmup": { dispatches: 0, entries: 0, ratelessFirstPasses: 0, simpleFallbackPasses: 0 },
	"join-authoritative": { dispatches: 0, entries: 0, ratelessFirstPasses: 0, simpleFallbackPasses: 0 },
	churn: { dispatches: 0, entries: 0, ratelessFirstPasses: 0, simpleFallbackPasses: 0 },
});

const addCounters = (left: MessageCounters, right: MessageCounters): MessageCounters => ({
	startSync: left.startSync + right.startSync,
	moreSymbols: left.moreSymbols + right.moreSymbols,
	requestAll: left.requestAll + right.requestAll,
	requestMaybeSync: left.requestMaybeSync + right.requestMaybeSync,
	requestMaybeSyncCoordinate: left.requestMaybeSyncCoordinate + right.requestMaybeSyncCoordinate,
});

const addRepairMetrics = (left: RepairMetrics, right: RepairMetrics): RepairMetrics => ({
	"join-warmup": {
		dispatches: left["join-warmup"].dispatches + right["join-warmup"].dispatches,
		entries: left["join-warmup"].entries + right["join-warmup"].entries,
		ratelessFirstPasses: left["join-warmup"].ratelessFirstPasses + right["join-warmup"].ratelessFirstPasses,
		simpleFallbackPasses: left["join-warmup"].simpleFallbackPasses + right["join-warmup"].simpleFallbackPasses,
	},
	"join-authoritative": {
		dispatches: left["join-authoritative"].dispatches + right["join-authoritative"].dispatches,
		entries: left["join-authoritative"].entries + right["join-authoritative"].entries,
		ratelessFirstPasses: left["join-authoritative"].ratelessFirstPasses + right["join-authoritative"].ratelessFirstPasses,
		simpleFallbackPasses: left["join-authoritative"].simpleFallbackPasses + right["join-authoritative"].simpleFallbackPasses,
	},
	churn: {
		dispatches: left.churn.dispatches + right.churn.dispatches,
		entries: left.churn.entries + right.churn.entries,
		ratelessFirstPasses: left.churn.ratelessFirstPasses + right.churn.ratelessFirstPasses,
		simpleFallbackPasses: left.churn.simpleFallbackPasses + right.churn.simpleFallbackPasses,
	},
});

const diffRepairMetrics = (after: RepairMetrics, before: RepairMetrics): RepairMetrics => ({
	"join-warmup": {
		dispatches: after["join-warmup"].dispatches - before["join-warmup"].dispatches,
		entries: after["join-warmup"].entries - before["join-warmup"].entries,
		ratelessFirstPasses: after["join-warmup"].ratelessFirstPasses - before["join-warmup"].ratelessFirstPasses,
		simpleFallbackPasses: after["join-warmup"].simpleFallbackPasses - before["join-warmup"].simpleFallbackPasses,
	},
	"join-authoritative": {
		dispatches: after["join-authoritative"].dispatches - before["join-authoritative"].dispatches,
		entries: after["join-authoritative"].entries - before["join-authoritative"].entries,
		ratelessFirstPasses: after["join-authoritative"].ratelessFirstPasses - before["join-authoritative"].ratelessFirstPasses,
		simpleFallbackPasses: after["join-authoritative"].simpleFallbackPasses - before["join-authoritative"].simpleFallbackPasses,
	},
	churn: {
		dispatches: after.churn.dispatches - before.churn.dispatches,
		entries: after.churn.entries - before.churn.entries,
		ratelessFirstPasses: after.churn.ratelessFirstPasses - before.churn.ratelessFirstPasses,
		simpleFallbackPasses: after.churn.simpleFallbackPasses - before.churn.simpleFallbackPasses,
	},
});

const snapshotRepairMetrics = (store?: EventStore<string, ReplicationDomainHash<"u64">>): RepairMetrics => {
	const metrics = (store?.log as any)?._repairMetrics;
	if (!metrics) return emptyRepairMetrics();
	return JSON.parse(JSON.stringify(metrics));
};

const attachMessageCounter = (store: EventStore<string, ReplicationDomainHash<"u64">>) => {
	const counters = emptyCounters();
	const original = store.log.onMessage.bind(store.log);
	store.log.onMessage = async (msg, context) => {
		if (msg instanceof StartSync) counters.startSync += 1;
		else if (msg instanceof MoreSymbols) counters.moreSymbols += 1;
		else if (msg instanceof RequestAll) counters.requestAll += 1;
		else if (msg instanceof RequestMaybeSync) counters.requestMaybeSync += 1;
		else if (msg instanceof RequestMaybeSyncCoordinate) counters.requestMaybeSyncCoordinate += 1;
		return original(msg, context);
	};
	return {
		read: () => ({ ...counters }),
		restore: () => {
			store.log.onMessage = original;
		},
	};
};

const classifyRecoveryPath = (repair: RepairMetrics) => {
	if (repair["join-authoritative"].dispatches > 0) {
		return repair["join-authoritative"].simpleFallbackPasses > 0
			? "join-authoritative+simple-fallback"
			: "join-authoritative-rateless";
	}
	if (repair["join-warmup"].dispatches > 0) {
		return repair["join-warmup"].simpleFallbackPasses > 0
			? "join-warmup+simple-fallback"
			: "join-warmup-only";
	}
	if (repair.churn.dispatches > 0) {
		return "churn-repair";
	}
	return "no-repair-dispatch";
};

const runScenario = async (run: number, args: BenchmarkArgs): Promise<RunResult> => {
	const session = await TestSession.connected(3);
	let db1: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	let db2: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	let db3: EventStore<string, ReplicationDomainHash<"u64">> | undefined;
	const counters: Array<ReturnType<typeof attachMessageCounter>> = [];

	try {
		db1 = await session.peers[0].open(new EventStore<string, ReplicationDomainHash<"u64">>(), {
			args: {
				replicate: { factor: 1 },
				setup,
			},
		});

		for (let i = 0; i < args.entries; i++) {
			await db1.add(`entry-${i}`, {
				replicas: new AbsoluteReplicas(3),
				meta: { next: [] },
			});
		}

		db2 = (await EventStore.open<EventStore<string, ReplicationDomainHash<"u64">>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: { factor: 1 },
					setup,
				},
			},
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);
		await waitForResolved(async () => {
			expect(db2!.log.log.length).to.equal(args.entries);
		}, { timeout: args.timeoutMs, delayInterval: 1_000 });

		counters.push(attachMessageCounter(db1), attachMessageCounter(db2));
		const repairBefore = addRepairMetrics(snapshotRepairMetrics(db1), snapshotRepairMetrics(db2));

		const startedAt = performance.now();
		db3 = (await EventStore.open<EventStore<string, ReplicationDomainHash<"u64">>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicate: { factor: 1 },
					setup,
				},
			},
		))!;
		counters.push(attachMessageCounter(db3));

		await waitForResolved(async () => {
			const entries = await db3!.log.log.toArray();
			expect(entries.length).to.equal(args.entries);
			let fullyReplicated = 0;
			for (const entry of entries) {
				if (decodeReplicas(entry).getValue(db3!.log) === 3) {
					fullyReplicated += 1;
				}
			}
			expect(fullyReplicated).to.equal(args.entries);
		}, { timeout: args.timeoutMs, delayInterval: 1_000 });
		const hydrationMs = Number((performance.now() - startedAt).toFixed(1));

		const repairAfter = addRepairMetrics(
			addRepairMetrics(snapshotRepairMetrics(db1), snapshotRepairMetrics(db2)),
			snapshotRepairMetrics(db3),
		);
		const repair = diffRepairMetrics(repairAfter, repairBefore);
		const messages = counters.reduce(
			(sum, counter) => addCounters(sum, counter.read()),
			emptyCounters(),
		);

		return {
			run,
			entries: args.entries,
			hydrationMs,
			recoveryPath: classifyRecoveryPath(repair),
			repair,
			messages,
		};
	} finally {
		for (const counter of counters) counter.restore();
		await db3?.drop().catch(() => {});
		await db2?.drop().catch(() => {});
		await db1?.drop().catch(() => {});
		await session.stop().catch(() => {});
	}
};

const average = (values: number[]) =>
	values.reduce((sum, value) => sum + value, 0) / values.length;

const args = parseArgs(process.argv.slice(2));
const results: RunResult[] = [];
for (let run = 1; run <= args.runs; run++) {
	results.push(await runScenario(run, args));
}

const summary = {
	runs: results.length,
	entries: args.entries,
	hydrationMsAvg: Number(average(results.map((x) => x.hydrationMs)).toFixed(1)),
	recoveryPaths: results.map((x) => x.recoveryPath),
	repair: results.reduce((sum, result) => addRepairMetrics(sum, result.repair), emptyRepairMetrics()),
	messages: results.reduce((sum, result) => addCounters(sum, result.messages), emptyCounters()),
};

if (args.json) {
	console.log(JSON.stringify({ args, results, summary }, null, 2));
} else {
	console.table(
		results.map((result) => ({
			run: result.run,
			hydrationMs: result.hydrationMs,
			recoveryPath: result.recoveryPath,
			joinWarmupDispatches: result.repair["join-warmup"].dispatches,
			joinAuthoritativeDispatches: result.repair["join-authoritative"].dispatches,
			simpleFallbackPasses:
				result.repair["join-warmup"].simpleFallbackPasses +
				result.repair["join-authoritative"].simpleFallbackPasses,
			startSync: result.messages.startSync,
			requestMaybeSync: result.messages.requestMaybeSync,
		})),
	);
	console.log("summary", summary);
}
