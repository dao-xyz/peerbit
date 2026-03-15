import { tcp } from "@libp2p/tcp";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import crypto from "crypto";
import fs from "node:fs/promises";
import path from "node:path";
import {
	DirectStream,
	type DirectStreamComponents,
	waitForNeighbour,
} from "../src/index.js";

type BenchMode = "silent" | "ack";

type CliOptions = {
	sizes: number[];
	iterations: number;
	warmup: number;
	runs: number;
	timeoutMs: number;
	mode: BenchMode | "both";
	json?: string;
};

type IterationResult = {
	iteration: number;
	publishResolvedMs: number;
	receiverObservedMs: number;
	publisherTailMs: number;
	completeMs: number;
};

type CaseResult = {
	mode: BenchMode;
	sizeBytes: number;
	iterations: number;
	warmup: number;
	runs: number;
	metrics: {
		publishResolvedMsAvg: number;
		receiverObservedMsAvg: number;
		publisherTailMsAvg: number;
		publisherTailMsMedian: number;
		publisherTailMsP95: number;
		completeMsAvg: number;
		completeMsMedian: number;
		completeMsP95: number;
		opsPerSecond: number;
		mbPerSecond: number;
	};
	iterationResults: IterationResult[];
};

class TestStreamImpl extends DirectStream {
	constructor(c: DirectStreamComponents) {
		super(c, ["bench/0.0.0"], {
			canRelayMessage: true,
			connectionManager: false,
		});
	}
}

const parseArgs = (argv: string[]): CliOptions => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(
			[
				"transfer.ts",
				"",
				"Real libp2p direct-stream transfer benchmark.",
				"",
				"Args:",
				"  --sizes BYTES[,BYTES...]   payload sizes (default: 262144,1048576)",
				"  --iterations N            measured iterations per run (default: 10)",
				"  --warmup N                warmup iterations per run (default: 2)",
				"  --runs N                  repeated runs per case (default: 3)",
				"  --timeoutMs N             per-iteration timeout (default: 30000)",
				"  --mode silent|ack|both    delivery mode (default: both)",
				"  --json PATH               write JSON results to file",
				"",
				"Examples:",
				"  pnpm -C packages/transport/stream run bench -- transfer",
				"  pnpm -C packages/transport/stream run bench -- transfer --sizes 1048576 --mode ack --runs 5",
			].join("\n"),
		);
		process.exit(0);
	}

	const sizes = (get("--sizes") ?? "262144,1048576")
		.split(",")
		.map((value) => Number(value.trim()))
		.filter((value) => Number.isFinite(value) && value > 0);
	if (sizes.length === 0) {
		throw new Error("Expected at least one positive size in --sizes");
	}

	const mode = (get("--mode") ?? "both") as CliOptions["mode"];
	if (!["silent", "ack", "both"].includes(mode)) {
		throw new Error(`Unsupported --mode "${mode}"`);
	}

	return {
		sizes,
		iterations: Number(get("--iterations") ?? 10),
		warmup: Number(get("--warmup") ?? 2),
		runs: Number(get("--runs") ?? 3),
		timeoutMs: Number(get("--timeoutMs") ?? 30_000),
		mode,
		json: get("--json"),
	};
};

const average = (values: number[]) =>
	values.reduce((sum, value) => sum + value, 0) / values.length;

const percentile = (values: number[], p: number) => {
	const sorted = [...values].sort((a, b) => a - b);
	const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1));
	return sorted[idx]!;
};

const round = (value: number, digits = 2) =>
	Number(value.toFixed(digits));

const makePayload = (size: number, sequence: number) => {
	const payload = crypto.randomBytes(size);
	if (size >= 8) {
		payload.writeUInt32BE(sequence >>> 0, 0);
		payload.writeUInt32BE((sequence ^ 0x9e3779b9) >>> 0, 4);
	}
	return payload;
};

const matchesPayload = (data: Uint8Array, sequence: number) => {
	if (data.byteLength < 8) {
		return true;
	}
	const view = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
	return (
		view.readUInt32BE(0) === (sequence >>> 0) &&
		view.readUInt32BE(4) === ((sequence ^ 0x9e3779b9) >>> 0)
	);
};

const waitForPayload = (
	receiver: DirectStream,
	sequence: number,
	timeoutMs: number,
) =>
	new Promise<void>((resolve, reject) => {
		const timer = setTimeout(() => {
			cleanup();
			reject(new Error(`Timed out waiting for payload sequence=${sequence}`));
		}, timeoutMs);

		const onData = (event: Event) => {
			const data = (event as CustomEvent<{ data: Uint8Array }>).detail.data;
			if (!matchesPayload(data, sequence)) {
				return;
			}
			cleanup();
			resolve();
		};

		const cleanup = () => {
			clearTimeout(timer);
			receiver.removeEventListener("data", onData as EventListener);
		};

		receiver.addEventListener("data", onData as EventListener);
	});

const modeOptions = (mode: BenchMode, receiver: DirectStream) =>
	mode === "ack"
		? {
				mode: new AcknowledgeDelivery({
					redundancy: 1,
					to: [receiver.publicKey],
				}),
			}
		: {
				to: [receiver.publicKey],
			};

const summarize = (
	mode: BenchMode,
	sizeBytes: number,
	iterations: number,
	warmup: number,
	runs: number,
	iterationResults: IterationResult[],
): CaseResult => {
	const publishResolved = iterationResults.map((result) => result.publishResolvedMs);
	const receiverObserved = iterationResults.map((result) => result.receiverObservedMs);
	const publisherTail = iterationResults.map((result) => result.publisherTailMs);
	const complete = iterationResults.map((result) => result.completeMs);
	const elapsedSeconds =
		complete.reduce((sum, value) => sum + value, 0) / 1_000;
	const totalMb = (sizeBytes * iterationResults.length) / (1024 * 1024);

	return {
		mode,
		sizeBytes,
		iterations,
		warmup,
		runs,
		metrics: {
			publishResolvedMsAvg: round(average(publishResolved)),
			receiverObservedMsAvg: round(average(receiverObserved)),
			publisherTailMsAvg: round(average(publisherTail)),
			publisherTailMsMedian: round(percentile(publisherTail, 0.5)),
			publisherTailMsP95: round(percentile(publisherTail, 0.95)),
			completeMsAvg: round(average(complete)),
			completeMsMedian: round(percentile(complete, 0.5)),
			completeMsP95: round(percentile(complete, 0.95)),
			opsPerSecond: round(iterationResults.length / elapsedSeconds),
			mbPerSecond: round(totalMb / elapsedSeconds),
		},
		iterationResults,
	};
};

const main = async () => {
	const options = parseArgs(process.argv.slice(2));
	const session = await TestSession.disconnected(4, {
		transports: [tcp()],
		services: { directstream: (c: any) => new TestStreamImpl(c) },
	});

	try {
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
			[session.peers[2], session.peers[3]],
		]);

		const stream = (i: number): TestStreamImpl =>
			session.peers[i].services.directstream;

		await waitForNeighbour(stream(0), stream(1));
		await waitForNeighbour(stream(1), stream(2));
		await waitForNeighbour(stream(2), stream(3));

		await stream(0).publish(new Uint8Array([123]), {
			mode: new AcknowledgeDelivery({
				redundancy: 1,
				to: [stream(session.peers.length - 1).publicKey],
			}),
		});
		await waitForResolved(() =>
			stream(0).routes.isReachable(
				stream(0).publicKeyHash,
				stream(3).publicKeyHash,
			),
		);

		const sender = stream(0);
		const receiver = stream(3);
		const modes: BenchMode[] =
			options.mode === "both" ? ["silent", "ack"] : [options.mode];
		const results: CaseResult[] = [];
		let sequence = 0;

		for (const mode of modes) {
			for (const sizeBytes of options.sizes) {
				console.log(
					`Running transfer benchmark mode=${mode} sizeBytes=${sizeBytes} runs=${options.runs} iterations=${options.iterations} warmup=${options.warmup}`,
				);
				const iterationResults: IterationResult[] = [];

				for (let run = 0; run < options.runs; run++) {
					for (
						let warmupIteration = 0;
						warmupIteration < options.warmup;
						warmupIteration++
					) {
						sequence++;
						const payload = makePayload(sizeBytes, sequence);
						const receivePromise = waitForPayload(
							receiver,
							sequence,
							options.timeoutMs,
						);
						await Promise.all([
							sender.publish(payload, modeOptions(mode, receiver)),
							receivePromise,
						]);
					}

					for (
						let measuredIteration = 0;
						measuredIteration < options.iterations;
						measuredIteration++
					) {
						sequence++;
						const payload = makePayload(sizeBytes, sequence);
						let publishResolvedMs = 0;
						let receiverObservedMs = 0;
						const startedAt = performance.now();
						const receivePromise = waitForPayload(
							receiver,
							sequence,
							options.timeoutMs,
						).then(() => {
							receiverObservedMs = performance.now() - startedAt;
						});
						const publishPromise = sender
							.publish(payload, modeOptions(mode, receiver))
							.then(() => {
								publishResolvedMs = performance.now() - startedAt;
							});
						await Promise.all([publishPromise, receivePromise]);
						const publisherTailMs = Math.max(
							0,
							publishResolvedMs - receiverObservedMs,
						);
						iterationResults.push({
							iteration: run * options.iterations + measuredIteration + 1,
							publishResolvedMs: round(publishResolvedMs, 3),
							receiverObservedMs: round(receiverObservedMs, 3),
							publisherTailMs: round(publisherTailMs, 3),
							completeMs: round(
								Math.max(publishResolvedMs, receiverObservedMs),
								3,
							),
						});
					}
				}

				results.push(
					summarize(
						mode,
						sizeBytes,
						options.iterations,
						options.warmup,
						options.runs,
						iterationResults,
					),
				);
			}
		}

		console.table(
			results.map((result) => ({
				mode: result.mode,
				sizeBytes: result.sizeBytes,
				runs: result.runs,
				iterationsPerRun: result.iterations,
				completeMsAvg: result.metrics.completeMsAvg,
				completeMsMedian: result.metrics.completeMsMedian,
				completeMsP95: result.metrics.completeMsP95,
				publishResolvedMsAvg: result.metrics.publishResolvedMsAvg,
				receiverObservedMsAvg: result.metrics.receiverObservedMsAvg,
				publisherTailMsAvg: result.metrics.publisherTailMsAvg,
				publisherTailMsMedian: result.metrics.publisherTailMsMedian,
				publisherTailMsP95: result.metrics.publisherTailMsP95,
				opsPerSecond: result.metrics.opsPerSecond,
				mbPerSecond: result.metrics.mbPerSecond,
			})),
		);

		const output = {
			options,
			results,
		};

		if (options.json) {
			const outPath = path.resolve(options.json);
			await fs.mkdir(path.dirname(outPath), { recursive: true });
			await fs.writeFile(outPath, `${JSON.stringify(output, null, 2)}\n`);
		}

		console.log(JSON.stringify(output, null, 2));
	} finally {
		await session.stop();
	}
};

main().catch((error) => {
	console.error(error);
	process.exitCode = 1;
});
