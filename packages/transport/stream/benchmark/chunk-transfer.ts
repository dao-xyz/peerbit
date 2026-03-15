import { tcp } from "@libp2p/tcp";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import {
	DirectStream,
	type DirectStreamComponents,
	waitForNeighbour,
} from "../src/index.js";

type BenchMode = "silent" | "ack";

type Sample = {
	senderCompleteMs: number;
	receiverCompleteMs: number;
	senderAfterReceiverMs: number;
	receiverAfterSenderMs: number;
};

type Task = {
	name: string;
	hz: number;
	mean_ms: number;
	rme: null;
	samples: number;
};

const modes = ["silent", "ack"] as const satisfies readonly BenchMode[];

const envInt = (name: string, fallback: number) => {
	const value = process.env[name];
	if (value == null) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	return Number.isFinite(parsed) ? parsed : fallback;
};

const chunkBytes = Math.max(1, envInt("CHUNK_TRANSFER_CHUNK_BYTES", 262144));
const chunkCount = Math.max(1, envInt("CHUNK_TRANSFER_CHUNKS", 24));
const warmupIterations = Math.max(0, envInt("CHUNK_TRANSFER_WARMUP", 1));
const iterations = Math.max(1, envInt("CHUNK_TRANSFER_ITERATIONS", 3));
const timeoutMs = Math.max(1_000, envInt("CHUNK_TRANSFER_TIMEOUT_MS", 30_000));

class TestStreamImpl extends DirectStream {
	constructor(components: DirectStreamComponents) {
		super(components, ["bench/0.0.0"], {
			canRelayMessage: true,
			connectionManager: false,
		});
	}
}

const round = (value: number, digits = 3) =>
	Number.isFinite(value) ? Number(value.toFixed(digits)) : value;

const average = (values: number[]) =>
	values.reduce((sum, value) => sum + value, 0) / values.length;

const summarizeTask = (name: string, values: number[]): Task => {
	const meanMs = average(values);
	return {
		name,
		hz: round(1000 / meanMs, 6),
		mean_ms: round(meanMs, 6),
		rme: null,
		samples: values.length,
	};
};

const makePayload = (size: number, sequence: number) => {
	const payload = new Uint8Array(size);
	payload.fill(sequence % 251);
	if (size >= 8) {
		const view = Buffer.from(
			payload.buffer,
			payload.byteOffset,
			payload.byteLength,
		);
		view.writeUInt32BE(sequence >>> 0, 0);
		view.writeUInt32BE((sequence ^ 0x9e3779b9) >>> 0, 4);
	}
	return payload;
};

const readSequence = (data: Uint8Array) => {
	if (data.byteLength < 8) {
		return null;
	}
	const view = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
	return view.readUInt32BE(0);
};

const waitForPayloadBatch = (
	receiver: DirectStream,
	startSequence: number,
	count: number,
	timeout: number,
) =>
	new Promise<void>((resolve, reject) => {
		const pending = new Set<number>();
		for (let i = 0; i < count; i++) {
			pending.add(startSequence + i);
		}

		const timer = setTimeout(() => {
			cleanup();
			reject(
				new Error(
					`Timed out waiting for payload batch start=${startSequence} count=${count} pending=${pending.size}`,
				),
			);
		}, timeout);

		const onData = (event: Event) => {
			const data = (event as CustomEvent<{ data: Uint8Array }>).detail.data;
			const sequence = readSequence(data);
			if (sequence == null || !pending.has(sequence)) {
				return;
			}
			pending.delete(sequence);
			if (pending.size === 0) {
				cleanup();
				resolve();
			}
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

const runIteration = async (
	sender: DirectStream,
	receiver: DirectStream,
	mode: BenchMode,
	startSequence: number,
) => {
	const payloads = Array.from({ length: chunkCount }, (_, index) =>
		makePayload(chunkBytes, startSequence + index),
	);

	const startedAt = performance.now();
	let receiverCompleteMs = 0;
	const receivePromise = waitForPayloadBatch(
		receiver,
		startSequence,
		payloads.length,
		timeoutMs,
	).then(() => {
		receiverCompleteMs = performance.now() - startedAt;
	});

	for (const payload of payloads) {
		await sender.publish(payload, modeOptions(mode, receiver));
	}

	const senderCompleteMs = performance.now() - startedAt;
	await receivePromise;

	return {
		senderCompleteMs,
		receiverCompleteMs,
		senderAfterReceiverMs: Math.max(0, senderCompleteMs - receiverCompleteMs),
		receiverAfterSenderMs: Math.max(0, receiverCompleteMs - senderCompleteMs),
	} satisfies Sample;
};

const prepareSession = async () => {
	const session = await TestSession.disconnected(4, {
		transports: [tcp()],
		services: { directstream: (c: any) => new TestStreamImpl(c) },
	});

	await session.connect([
		[session.peers[0], session.peers[1]],
		[session.peers[1], session.peers[2]],
		[session.peers[2], session.peers[3]],
	]);

	const stream = (i: number): TestStreamImpl => session.peers[i].services.directstream;

	await waitForNeighbour(stream(0), stream(1));
	await waitForNeighbour(stream(1), stream(2));
	await waitForNeighbour(stream(2), stream(3));

	await stream(0).publish(new Uint8Array([1, 2, 3, 4]), {
		mode: new AcknowledgeDelivery({
			redundancy: 1,
			to: [stream(3).publicKey],
		}),
	});

	await waitForResolved(() =>
		stream(0).routes.isReachable(stream(0).publicKeyHash, stream(3).publicKeyHash),
	);

	return {
		session,
		sender: stream(0),
		receiver: stream(3),
	};
};

const runMode = async (mode: BenchMode) => {
	let sequence = 1_000;
	const { session, sender, receiver } = await prepareSession();
	const samples: Sample[] = [];

	try {
		for (let i = 0; i < warmupIterations; i++) {
			await runIteration(sender, receiver, mode, sequence);
			sequence += chunkCount;
		}
		for (let i = 0; i < iterations; i++) {
			samples.push(await runIteration(sender, receiver, mode, sequence));
			sequence += chunkCount;
		}
	} finally {
		await session.stop();
	}

	return samples;
};

const results = await Promise.all(
	modes.map(async (mode) => ({
		mode,
		samples: await runMode(mode),
	})),
);

const totalBytes = chunkBytes * chunkCount;
const totalMb = totalBytes / (1024 * 1024);
const tasks: Task[] = [];

for (const result of results) {
	tasks.push(
		summarizeTask(
			`${result.mode}: sender-complete`,
			result.samples.map((sample) => sample.senderCompleteMs),
		),
	);
	tasks.push(
		summarizeTask(
			`${result.mode}: receiver-complete`,
			result.samples.map((sample) => sample.receiverCompleteMs),
		),
	);
	tasks.push(
		summarizeTask(
			`${result.mode}: sender-after-receiver`,
			result.samples.map((sample) => sample.senderAfterReceiverMs),
		),
	);
	tasks.push(
		summarizeTask(
			`${result.mode}: receiver-after-sender`,
			result.samples.map((sample) => sample.receiverAfterSenderMs),
		),
	);
}

const output = {
	name: "chunk-transfer",
	tasks,
	meta: {
		chunkBytes,
		chunkCount,
		totalBytes,
		totalMb: round(totalMb, 3),
		hops: 3,
		warmupIterations,
		iterations,
		timeoutMs,
		results: results.map((result) => ({
			mode: result.mode,
			senderCompleteMsAvg: round(
				average(result.samples.map((sample) => sample.senderCompleteMs)),
			),
			receiverCompleteMsAvg: round(
				average(result.samples.map((sample) => sample.receiverCompleteMs)),
			),
			senderAfterReceiverMsAvg: round(
				average(result.samples.map((sample) => sample.senderAfterReceiverMs)),
			),
			receiverAfterSenderMsAvg: round(
				average(result.samples.map((sample) => sample.receiverAfterSenderMs)),
			),
			senderMbPerSecondAvg: round(
				average(
					result.samples.map(
						(sample) => totalMb / (sample.senderCompleteMs / 1000),
					),
				),
			),
			receiverMbPerSecondAvg: round(
				average(
					result.samples.map(
						(sample) => totalMb / (sample.receiverCompleteMs / 1000),
					),
				),
			),
			samples: result.samples.map((sample) => ({
				senderCompleteMs: round(sample.senderCompleteMs),
				receiverCompleteMs: round(sample.receiverCompleteMs),
				senderAfterReceiverMs: round(sample.senderAfterReceiverMs),
				receiverAfterSenderMs: round(sample.receiverAfterSenderMs),
			})),
		})),
	},
};

if (process.env.BENCH_JSON === "1") {
	await new Promise<void>((resolve, reject) => {
		process.stdout.write(JSON.stringify(output, null, 2), (error) => {
			if (error) {
				reject(error);
				return;
			}
			resolve();
		});
	});
} else {
	console.table(
		tasks.map((task) => ({
			task: task.name,
			mean_ms: task.mean_ms,
			ops_s: round(task.hz, 3),
			samples: task.samples,
		})),
	);
}

process.exit(0);
