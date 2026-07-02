/**
 * TS decode+verify vs native (wasm) batch decode+verify on the same corpus
 * of signed DataMessage frames — the DirectStream inbound hot path.
 *
 * Run with: pnpm --filter @peerbit/network-rust run benchmark
 */
import { Ed25519Keypair, PreHash } from "@peerbit/crypto";
import {
	DataMessage,
	Message,
	MessageHeader,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { Uint8ArrayList } from "uint8arraylist";
import { createNativeWire } from "../src/index.js";

const messageCount = Number(process.env.PEERBIT_WIRE_BENCH_COUNT ?? 2000);
const payloadSizes = [32, 1024, 16 * 1024];
const batchSizes = [1, 16, 64, Number.MAX_SAFE_INTEGER];

const wire = await createNativeWire();
const keypair = await Ed25519Keypair.create();
const results: Record<string, unknown>[] = [];

for (const payloadSize of payloadSizes) {
	const frames: Uint8Array[] = [];
	for (let i = 0; i < messageCount; i++) {
		const message = await new DataMessage({
			header: new MessageHeader({
				session: 1,
				mode: new SilentDelivery({ to: ["target-hash"], redundancy: 1 }),
			}),
			data: new Uint8Array(payloadSize).map((_, j) => (i + j) % 251),
		}).sign((bytes) => keypair.sign(bytes, PreHash.SHA_256));
		const bytes = message.bytes();
		frames.push(bytes instanceof Uint8Array ? bytes : bytes.subarray());
	}

	// TS: per-frame decode + verify (what processMessage/verifyAndProcess do)
	{
		const started = performance.now();
		for (const frame of frames) {
			const message = Message.from(new Uint8ArrayList(frame));
			if (!(await message.verify(true))) {
				throw new Error("ts verification failed");
			}
		}
		const elapsed = performance.now() - started;
		results.push({
			impl: "ts",
			payload: payloadSize,
			batch: 1,
			messages: messageCount,
			elapsedMs: Math.round(elapsed * 100) / 100,
			msgsPerSecond: Math.round((messageCount / elapsed) * 1000),
		});
	}

	// Rust: batched decode + verify
	for (const batchSize of batchSizes) {
		const started = performance.now();
		for (let offset = 0; offset < frames.length; offset += batchSize) {
			const batch = frames.slice(offset, offset + batchSize);
			const records = wire.decodeAndVerifyBatch(batch, Date.now());
			for (let i = 0; i < batch.length; i++) {
				const word0 = records[i * 4];
				if (((word0 >>> 16) & 0xff) !== 1) {
					throw new Error("native verification failed");
				}
			}
		}
		const elapsed = performance.now() - started;
		results.push({
			impl: "rust",
			payload: payloadSize,
			batch: Math.min(batchSize, messageCount),
			messages: messageCount,
			elapsedMs: Math.round(elapsed * 100) / 100,
			msgsPerSecond: Math.round((messageCount / elapsed) * 1000),
		});
	}
}

console.table(results);
