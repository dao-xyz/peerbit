// @ts-nocheck
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { EventStore } from "../test/utils/stores/event-store.js";

type Mode = "adaptive" | "fixed";
type BenchmarkArgs = {
	mode: Mode | "both";
	entries: number;
	payloadBytes: number;
	runs: number;
	adaptiveIntervalMs: number;
	burstIdleMs?: number;
	settleTimeoutMs: number;
	target: "replicators" | "none";
	json: boolean;
};

type RunResult = {
	mode: Mode;
	run: number;
	appendMs: number;
	settleMs: number;
	rebalanceScheduled: number;
	rebalanceExecuted: number;
	announceCalls: number;
	pruneQueueAdds: number;
	pruneCalls: number;
	writerEntries: number;
	followerEntries: number;
	settleTimedOut: boolean;
};

const defaults: BenchmarkArgs = {
	mode: "both",
	entries: 200,
	payloadBytes: 4 * 1024,
	runs: 3,
	adaptiveIntervalMs: 50,
	burstIdleMs: 500,
	settleTimeoutMs: 10_000,
	target: "replicators",
	json: false,
};

const usage = () => {
	console.log(`Run with "node --loader ts-node/esm ./benchmark/adaptive-ingest.ts [options]"

Options:
  --mode adaptive|fixed|both   benchmark mode (default: both)
  --entries N                  entries per run (default: ${defaults.entries})
  --payloadBytes N             payload bytes per entry (default: ${defaults.payloadBytes})
  --runs N                     runs per mode (default: ${defaults.runs})
  --adaptiveIntervalMs N       adaptive controller interval (default: ${defaults.adaptiveIntervalMs})
  --burstIdleMs N              override burst idle window for local runs (default: ${defaults.burstIdleMs})
  --settleTimeoutMs N          max wait for deferred work to flush (default: ${defaults.settleTimeoutMs})
  --target replicators|none    append target (default: ${defaults.target})
  --json                       emit JSON instead of tables
  --help                       show this message
`);
};

const parseArgs = (argv: string[]): BenchmarkArgs => {
	const out = { ...defaults };
	const consume = (index: number) => {
		const value = argv[index + 1];
		if (value == null) {
			throw new Error(`Missing value for ${argv[index]}`);
		}
		return value;
	};

	for (let i = 0; i < argv.length; i++) {
		if (argv[i] === "--") {
			continue;
		}
		switch (argv[i]) {
			case "--mode":
				out.mode = consume(i) as BenchmarkArgs["mode"];
				i++;
				break;
			case "--entries":
				out.entries = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--payloadBytes":
				out.payloadBytes = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--runs":
				out.runs = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--adaptiveIntervalMs":
				out.adaptiveIntervalMs = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--burstIdleMs":
				out.burstIdleMs = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--settleTimeoutMs":
				out.settleTimeoutMs = Number.parseInt(consume(i), 10);
				i++;
				break;
			case "--target":
				out.target = consume(i) as BenchmarkArgs["target"];
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

	if (!["adaptive", "fixed", "both"].includes(out.mode)) {
		throw new Error(`Expected --mode adaptive|fixed|both, got '${out.mode}'`);
	}
	if (!Number.isFinite(out.entries) || out.entries <= 0) {
		throw new Error(`Expected --entries > 0, got '${out.entries}'`);
	}
	if (!Number.isFinite(out.payloadBytes) || out.payloadBytes <= 0) {
		throw new Error(
			`Expected --payloadBytes > 0, got '${out.payloadBytes}'`,
		);
	}
	if (!Number.isFinite(out.runs) || out.runs <= 0) {
		throw new Error(`Expected --runs > 0, got '${out.runs}'`);
	}
	if (!Number.isFinite(out.adaptiveIntervalMs) || out.adaptiveIntervalMs <= 0) {
		throw new Error(
			`Expected --adaptiveIntervalMs > 0, got '${out.adaptiveIntervalMs}'`,
		);
	}
	if (
		out.burstIdleMs != null &&
		(!Number.isFinite(out.burstIdleMs) || out.burstIdleMs <= 0)
	) {
		throw new Error(`Expected --burstIdleMs > 0, got '${out.burstIdleMs}'`);
	}
	if (
		!Number.isFinite(out.settleTimeoutMs) ||
		out.settleTimeoutMs <= 0
	) {
		throw new Error(
			`Expected --settleTimeoutMs > 0, got '${out.settleTimeoutMs}'`,
		);
	}
	if (!["replicators", "none"].includes(out.target)) {
		throw new Error(
			`Expected --target replicators|none, got '${out.target}'`,
		);
	}

	return out;
};

const getReplicateArgs = (mode: Mode, adaptiveIntervalMs: number) => {
	return mode === "adaptive"
		? {
				limits: {
					interval: adaptiveIntervalMs,
				},
			}
		: { factor: 1 };
};

const average = (values: number[]) =>
	values.reduce((sum, value) => sum + value, 0) / values.length;

const summarize = (results: RunResult[]) => {
	const byMode = new Map<Mode, RunResult[]>();
	for (const result of results) {
		const arr = byMode.get(result.mode) ?? [];
		arr.push(result);
		byMode.set(result.mode, arr);
	}

	return [...byMode.entries()].map(([mode, modeResults]) => ({
		mode,
		runs: modeResults.length,
		appendMsAvg: Number(average(modeResults.map((x) => x.appendMs)).toFixed(1)),
		settleMsAvg: Number(average(modeResults.map((x) => x.settleMs)).toFixed(1)),
		rebalanceScheduledAvg: Number(
			average(modeResults.map((x) => x.rebalanceScheduled)).toFixed(1),
		),
		rebalanceExecutedAvg: Number(
			average(modeResults.map((x) => x.rebalanceExecuted)).toFixed(1),
		),
		announceCallsAvg: Number(
			average(modeResults.map((x) => x.announceCalls)).toFixed(1),
		),
		pruneQueueAddsAvg: Number(
			average(modeResults.map((x) => x.pruneQueueAdds)).toFixed(1),
		),
		pruneCallsAvg: Number(
			average(modeResults.map((x) => x.pruneCalls)).toFixed(1),
		),
		writerEntriesAvg: Number(
			average(modeResults.map((x) => x.writerEntries)).toFixed(1),
		),
		followerEntriesAvg: Number(
			average(modeResults.map((x) => x.followerEntries)).toFixed(1),
		),
		settleTimeouts: modeResults.filter((x) => x.settleTimedOut).length,
	}));
};

const runScenario = async (
	mode: Mode,
	run: number,
	args: BenchmarkArgs,
): Promise<RunResult> => {
	const session = await TestSession.connected(2);
	const replicate = getReplicateArgs(mode, args.adaptiveIntervalMs);
	const payload = "x".repeat(args.payloadBytes);
	let writer: EventStore<string, any> | undefined;
	let follower: EventStore<string, any> | undefined;

	try {
		writer = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate,
			},
		});
		follower = (await EventStore.open<EventStore<string, any>>(
			writer.address!,
			session.peers[1],
			{
				args: {
					replicate,
				},
			},
		))!;

		await writer.waitFor(session.peers[1].peerId);
		await follower.waitFor(session.peers[0].peerId);

		if (args.burstIdleMs != null) {
			(writer.log as any).adaptiveRebalanceIdleMs = args.burstIdleMs;
			(follower.log as any).adaptiveRebalanceIdleMs = args.burstIdleMs;
		}

		const rebalanceScheduled = sinonSpy(
			(writer.log as any).rebalanceParticipationDebounced,
			"call",
		);
		const rebalanceExecuted = sinonSpy(writer.log, "rebalanceParticipation");
		const announceCalls = sinonSpy(writer.log, "startAnnounceReplicating");
		const pruneQueueAdds = sinonSpy(writer.log.pruneDebouncedFn, "add");
		const pruneCalls = sinonSpy(writer.log, "prune");

		const appendStart = performance.now();
		for (let i = 0; i < args.entries; i++) {
			await writer.add(`${i}:${payload}`, { target: args.target });
		}
		const appendMs = performance.now() - appendStart;

		const settleStart = performance.now();
		let settleTimedOut = false;
		try {
			await waitForResolved(() => {
				const writerLog = writer.log as any;
				const shouldDelayAdaptiveRebalance =
					typeof writerLog.shouldDelayAdaptiveRebalance === "function"
						? writerLog.shouldDelayAdaptiveRebalance()
						: false;
				return !shouldDelayAdaptiveRebalance && writerLog._pendingDeletes.size === 0;
			}, {
				timeout: args.settleTimeoutMs,
				delayInterval: 25,
			});
		} catch {
			settleTimedOut = true;
		}
		const settleMs = performance.now() - settleStart;

		return {
			mode,
			run,
			appendMs: Number(appendMs.toFixed(1)),
			settleMs: Number(settleMs.toFixed(1)),
			rebalanceScheduled: rebalanceScheduled.callCount,
			rebalanceExecuted: rebalanceExecuted.callCount,
			announceCalls: announceCalls.callCount,
			pruneQueueAdds: pruneQueueAdds.callCount,
			pruneCalls: pruneCalls.callCount,
			writerEntries: writer.log.log.length,
			followerEntries: follower.log.log.length,
			settleTimedOut,
		};
	} finally {
		await Promise.allSettled([
			writer?.close?.(),
			follower?.close?.(),
		]);
		await session.stop();
	}
};

const sinonSpy = <T extends object, K extends keyof T & string>(
	target: T,
	method: K,
) => {
	if (!target || typeof target[method] !== "function") {
		return {
			get callCount() {
				return 0;
			},
		};
	}

	const original = target[method];
	let callCount = 0;
	target[method] = ((...params: any[]) => {
		callCount++;
		return (original as any).apply(target, params);
	}) as T[K];

	return {
		get callCount() {
			return callCount;
		},
	};
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const modes: Mode[] =
		args.mode === "both" ? ["adaptive", "fixed"] : [args.mode];
	const results: RunResult[] = [];

	for (const mode of modes) {
		for (let run = 1; run <= args.runs; run++) {
			console.log(
				`Running ${mode} (${run}/${args.runs}) with ${args.entries} entries of ${args.payloadBytes} bytes`,
			);
			results.push(await runScenario(mode, run, args));
		}
	}

	if (args.json) {
		console.log(
			JSON.stringify(
				{
					args,
					results,
					summary: summarize(results),
				},
				null,
				2,
			),
		);
		return;
	}

	console.log("\nPer-run results");
	console.table(results);
	console.log("\nSummary");
	console.table(summarize(results));
};

main()
	.then(() => {
		process.exit(process.exitCode ?? 0);
	})
	.catch((error) => {
		console.error(error);
		process.exit(1);
	});
