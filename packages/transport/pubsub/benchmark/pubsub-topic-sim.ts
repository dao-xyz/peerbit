/**
 * In-memory DirectSub simulator (1 writer -> many subscribers).
 *
 * Goal: stress the real @peerbit/pubsub + @peerbit/stream code paths (routing,
 * ACK learning, backpressure/lanes, dialer/pruner) while avoiding real sockets
 * and heavy crypto costs.
 */

import {
	formatPubsubTopicSimResult,
	resolvePubsubTopicSimParams,
	runPubsubTopicSim,
} from "./pubsub-topic-sim-lib.js";

const parseArgs = (argv: string[]) => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};
	const has = (key: string) => argv.includes(key);

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(
			[
				"pubsub-topic-sim.ts",
				"",
				"Args:",
				"  --nodes N                     total nodes (default: 2000)",
				"  --degree K                    target undirected degree (default: 6)",
				"  --writerIndex I               writer node index (default: 0)",
				"  --subscribers N               number of subscribers (default: nodes-1)",
				"  --messages M                  messages to publish (default: 200)",
				"  --msgSize BYTES               payload bytes (default: 1024)",
				"  --intervalMs MS               delay between publishes (default: 0)",
				"  --strict 0|1                  alias for --silent (default: 0)",
				"  --silent 0|1                  publish with SilentDelivery (default: 0)",
				"  --redundancy R                stream redundancy (default: 2)",
				"  --seed S                      RNG seed (default: 1)",
				"  --topic NAME                  topic name (default: concert)",
				"  --subscribeModel real|preseed (default: preseed)",
				"  --subscriptionDebounceDelayMs MS (default: 0)",
				"  --warmupMs MS                 warmup delay before measuring (default: 0)",
				"  --warmupMessages N            warmup messages before measuring (default: 0)",
				"  --settleMs MS                 time to wait for delivery after publish (default: 200)",
				"  --timeoutMs MS                global timeout (default: 300000)",
				"  --dialConcurrency N           dial concurrency (default: 256)",
				"  --dialer 0|1                  enable stream autodial (default: 0)",
				"  --pruner 0|1                  enable stream pruning (default: 0)",
				"  --prunerIntervalMs MS         (default: 50)",
				"  --prunerMaxBufferBytes BYTES  (default: 65536)",
				"  --dialDelayMs MS              artificial dial delay (default: 0)",
				"  --streamRxDelayMs MS          per-chunk inbound delay in shim (default: 0)",
				"  --streamHighWaterMarkBytes B  backpressure threshold (default: 262144)",
				"  --dropDataFrameRate P         drop rate for stream data frames (default: 0)",
				"  --maxLatencySamples N         reservoir sample size (default: 1000000)",
				"  --churnEveryMs MS             churn interval (default: 0, off)",
				"  --churnDownMs MS              offline duration per churn (default: 0, off)",
				"  --churnFraction F             fraction to churn per event (default: 0, off)",
				"  --churnRedialIntervalMs MS    graph redial tick (default: 50)",
				"  --churnRedialConcurrency N    redial concurrency (default: 128)",
				"",
				"Examples:",
				"  pnpm -C packages/transport/pubsub run bench -- topic-sim --nodes 3 --degree 2 --subscribers 2 --messages 5 --msgSize 32 --intervalMs 0 --seed 1 --subscribeModel preseed",
				"  pnpm -C packages/transport/pubsub run bench -- topic-sim --nodes 2000 --degree 6 --subscribers 1500 --messages 200 --msgSize 1024 --intervalMs 0 --seed 1 --subscribeModel preseed",
			].join("\n"),
		);
		process.exit(0);
	}

	const nodes = Number(get("--nodes") ?? 2000);
	const strict = has("--strict") ? String(get("--strict") ?? "0") === "1" : undefined;
	const silent = has("--silent") ? String(get("--silent") ?? "0") === "1" : undefined;

	return resolvePubsubTopicSimParams({
		nodes,
		degree: Number(get("--degree") ?? 6),
		writerIndex: Number(get("--writerIndex") ?? 0),
		subscribers: Number(get("--subscribers") ?? nodes - 1),
		messages: Number(get("--messages") ?? 200),
		msgSize: Number(get("--msgSize") ?? 1024),
		intervalMs: Number(get("--intervalMs") ?? 0),
		silent: silent ?? strict ?? false,
		redundancy: Number(get("--redundancy") ?? 2),
		seed: Number(get("--seed") ?? 1),
		topic: String(get("--topic") ?? "concert"),
		subscribeModel: (String(get("--subscribeModel") ?? "preseed") as any) ?? "preseed",
		subscriptionDebounceDelayMs: Number(get("--subscriptionDebounceDelayMs") ?? 0),
		warmupMs: Number(get("--warmupMs") ?? 0),
		warmupMessages: Number(get("--warmupMessages") ?? 0),
		settleMs: Number(get("--settleMs") ?? 200),
		timeoutMs: Number(get("--timeoutMs") ?? 300_000),
		dialConcurrency: Number(get("--dialConcurrency") ?? 256),
		dialer: String(get("--dialer") ?? "0") === "1",
		pruner: String(get("--pruner") ?? "0") === "1",
		prunerIntervalMs: Number(get("--prunerIntervalMs") ?? 50),
		prunerMaxBufferBytes: Number(get("--prunerMaxBufferBytes") ?? 64 * 1024),
		dialDelayMs: Number(get("--dialDelayMs") ?? 0),
		streamRxDelayMs: Number(get("--streamRxDelayMs") ?? 0),
		streamHighWaterMarkBytes: Number(get("--streamHighWaterMarkBytes") ?? 256 * 1024),
		dropDataFrameRate: Number(get("--dropDataFrameRate") ?? 0),
		maxLatencySamples: Number(get("--maxLatencySamples") ?? 1_000_000),
		churnEveryMs: Number(get("--churnEveryMs") ?? 0),
		churnDownMs: Number(get("--churnDownMs") ?? 0),
		churnFraction: Number(get("--churnFraction") ?? 0),
		churnRedialIntervalMs: Number(get("--churnRedialIntervalMs") ?? 50),
		churnRedialConcurrency: Number(get("--churnRedialConcurrency") ?? 128),
	});
};

const main = async () => {
	const params = parseArgs(process.argv.slice(2));
	const result = await runPubsubTopicSim(params);
	console.log(formatPubsubTopicSimResult(result));
};

try {
	await main();
} catch (e: any) {
	console.error(e?.message ?? e);
	process.exit(String(e?.message ?? "").includes("timed out") ? 124 : 1);
}

