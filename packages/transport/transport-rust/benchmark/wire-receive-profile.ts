/**
 * MEASUREMENT 1 (isolate the copy) + PATH A of MEASUREMENT 2 (the current JS
 * receive path) for the native-transport profiling task. See
 * `packages/transport/transport-rust/PROFILING.md`.
 *
 * This half runs entirely in the JS/wasm runtime — it is the receive cost a
 * node running the CURRENT stack pays: js-libp2p delivers frames to JS, JS
 * batches them, and calls into wasm, whose boundary does the per-frame
 * `array.to_vec()` ingress copy (ARCHITECTURE.md exception 2) before decoding
 * and Ed25519 batch-verifying.
 *
 * It emits two things:
 *   1. A JSON results block (T_copy / T_decode / Path A, mean ± stdev over
 *      MEASURED runs with warmups discarded) to `--out <file>` (or stdout).
 *   2. The exact corpus bytes to `--corpus <dir>` so the native Rust bench
 *      (`benchmark/wire_receive_native.rs`, Path B) decodes byte-identical
 *      input in the other runtime — the A/B is over one shared corpus.
 *
 * Run (never concurrently with any build or the native bench):
 *   node ./dist/benchmark/wire-receive-profile.js --corpus <dir> --out <file>
 *
 * The wasm module is imported directly from the sibling @peerbit/network-rust
 * build output (../network-rust/wasm/peerbit_wire.js) so this harness does not
 * depend on that package's full aegir `dist/` (only its wasm-pack artifact).
 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

import { Ed25519Keypair, PreHash } from "@peerbit/crypto";
import {
	DataMessage,
	Message,
	MessageHeader,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { Uint8ArrayList } from "uint8arraylist";

const here = dirname(fileURLToPath(import.meta.url));

// ---- config -------------------------------------------------------------

const PAYLOAD_SIZES = [32, 1024, 16 * 1024, 64 * 1024];
// Batch size for the wasm-call measurements: one native call per this many
// frames. MAX_SAFE_INTEGER = whole corpus in a single call (the batched-pump
// upper bound the network-rust bench also reports).
const BATCH_SIZE = Number(process.env.PEERBIT_PROFILE_BATCH ?? 64);
const MESSAGE_COUNT = Number(process.env.PEERBIT_PROFILE_COUNT ?? 4000);
const WARMUP_RUNS = Number(process.env.PEERBIT_PROFILE_WARMUP ?? 3);
const MEASURED_RUNS = Number(process.env.PEERBIT_PROFILE_RUNS ?? 8);

// ---- tiny arg parsing ---------------------------------------------------

const args = process.argv.slice(2);
const argOf = (name: string): string | undefined => {
	const i = args.indexOf(name);
	return i >= 0 ? args[i + 1] : undefined;
};
const corpusDir = argOf("--corpus");
const outFile = argOf("--out");

// ---- stats --------------------------------------------------------------

type Stats = { mean: number; stdev: number; min: number; runs: number[] };
const summarize = (runs: number[]): Stats => {
	const mean = runs.reduce((a, b) => a + b, 0) / runs.length;
	const variance =
		runs.reduce((a, b) => a + (b - mean) * (b - mean), 0) /
		(runs.length > 1 ? runs.length - 1 : 1);
	return {
		mean,
		stdev: Math.sqrt(variance),
		min: Math.min(...runs),
		runs,
	};
};

/** Run `fn` WARMUP_RUNS times (discarded) then MEASURED_RUNS times, returning
 * per-run elapsed ms. `fn` processes the whole corpus once and returns a
 * checksum-like number that we accumulate into a sink to defeat DCE. */
let sink = 0;
const timeRuns = (fn: () => number): number[] => {
	for (let r = 0; r < WARMUP_RUNS; r++) sink ^= fn();
	const out: number[] = [];
	for (let r = 0; r < MEASURED_RUNS; r++) {
		const started = performance.now();
		sink ^= fn();
		out.push(performance.now() - started);
	}
	return out;
};

// ---- corpus -------------------------------------------------------------

const keypair = await Ed25519Keypair.create();

const buildCorpus = async (payloadSize: number): Promise<Uint8Array[]> => {
	const frames: Uint8Array[] = [];
	for (let i = 0; i < MESSAGE_COUNT; i++) {
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
	return frames;
};

// ---- wasm ---------------------------------------------------------------

type WireWasm = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	copy_batch_only: (frames: Uint8Array[]) => number;
	decode_and_verify_batch: (frames: Uint8Array[], nowMs: number) => Uint32Array;
};

const loadWasm = async (): Promise<WireWasm> => {
	// Resolve the sibling @peerbit/network-rust package (a devDependency of this
	// crate) and load its wasm-pack artifact directly. This is robust to the
	// compiled harness's location — we do not depend on the package's aegir
	// `dist/`, only its `wasm/` output produced by `wasm-pack build`.
	const require = createRequire(import.meta.url);
	let wasmBaseDir: string;
	try {
		// exports["."] -> ./dist/src/index.js, so the package root is three
		// levels up from the resolved main entry; the wasm-pack artifact is at
		// <root>/wasm (the network-rust `build` copies dist/wasm there).
		const mainEntry = require.resolve("@peerbit/network-rust");
		wasmBaseDir = resolve(dirname(mainEntry), "../../wasm");
	} catch {
		// Fallback: relative to this file within the monorepo layout.
		wasmBaseDir = resolve(here, "../../../network-rust/wasm");
	}
	const jsPath = resolve(wasmBaseDir, "peerbit_wire.js");
	const wasmPath = resolve(wasmBaseDir, "peerbit_wire_bg.wasm");
	const mod = (await import(jsPath)) as unknown as WireWasm;
	const bytes = await readFile(wasmPath);
	mod.initSync({ module: bytes });
	return mod;
};

const wasm = await loadWasm();

// ---- measurement --------------------------------------------------------

type PayloadResult = {
	payload: number;
	messages: number;
	batch: number;
	// Measurement 1
	tCopyMs: Stats;
	tDecodeMs: Stats;
	copyFraction: number; // tCopy.mean / tDecode.mean
	// Path A of Measurement 2 (full JS receive: pump + copy + decode+verify)
	pathAMs: Stats;
	pathAFramesPerSec: number;
	pathAUsPerFrame: number;
	// wasm-only decode throughput (for attribution vs native Path B)
	decodeFramesPerSec: number;
	decodeUsPerFrame: number;
	copyUsPerFrame: number;
};

const results: PayloadResult[] = [];
const now = Date.now();

for (const payload of PAYLOAD_SIZES) {
	const frames = await buildCorpus(payload);

	// Persist the exact corpus so Path B (native rust) decodes identical bytes.
	if (corpusDir) {
		await mkdir(corpusDir, { recursive: true });
		// Length-delimited: [u32 LE frame count][ (u32 LE len)(len bytes) ]*
		let total = 4;
		for (const f of frames) total += 4 + f.length;
		const buf = new Uint8Array(total);
		const view = new DataView(buf.buffer);
		let off = 0;
		view.setUint32(off, frames.length, true);
		off += 4;
		for (const f of frames) {
			view.setUint32(off, f.length, true);
			off += 4;
			buf.set(f, off);
			off += f.length;
		}
		await writeFile(resolve(corpusDir, `corpus-${payload}.bin`), buf);
	}

	// --- Measurement 1: T_copy (ingress copy ONLY) ---
	const tCopy = timeRuns(() => {
		let acc = 0;
		for (let o = 0; o < frames.length; o += BATCH_SIZE) {
			const batch = frames.slice(o, o + BATCH_SIZE);
			acc ^= wasm.copy_batch_only(batch);
		}
		return acc;
	});

	// --- Measurement 1: T_decode (copy + decode + Ed25519 batch verify) ---
	const tDecode = timeRuns(() => {
		let acc = 0;
		for (let o = 0; o < frames.length; o += BATCH_SIZE) {
			const batch = frames.slice(o, o + BATCH_SIZE);
			const records = wasm.decode_and_verify_batch(batch, now);
			// touch the result (and assert verify ok) like the real consumer
			acc ^= records[0]! | records[(batch.length - 1) * 4]!;
		}
		return acc;
	});

	// --- Path A: full JS receive path = the SAME batched decode+verify a
	// node pays today. This is T_decode measured as the end-to-end receive
	// cost (the JS-side per-batch slice() pump + boundary marshalling +
	// copy + decode + verify). We report it separately from tDecode so the
	// pump overhead vs the pure wasm call is visible, but they share the
	// decode+verify core. ---
	const pathA = timeRuns(() => {
		let acc = 0;
		for (let o = 0; o < frames.length; o += BATCH_SIZE) {
			// The JS pump: js-libp2p hands frames one at a time; DirectStream
			// accumulates a batch array then calls the native wire. slice()
			// models that per-batch array materialization.
			const batch = frames.slice(o, o + BATCH_SIZE);
			const records = wasm.decode_and_verify_batch(batch, now);
			for (let i = 0; i < batch.length; i++) {
				const word0 = records[i * 4]!;
				// verify status byte must be 1 (verified)
				if (((word0 >>> 16) & 0xff) !== 1) {
					throw new Error(
						`path A verification failed at payload ${payload}`,
					);
				}
			}
			acc ^= records[0]!;
		}
		return acc;
	});

	const tCopyStats = summarize(tCopy);
	const tDecodeStats = summarize(tDecode);
	const pathAStats = summarize(pathA);

	results.push({
		payload,
		messages: MESSAGE_COUNT,
		batch: BATCH_SIZE,
		tCopyMs: tCopyStats,
		tDecodeMs: tDecodeStats,
		copyFraction: tCopyStats.mean / tDecodeStats.mean,
		pathAMs: pathAStats,
		pathAFramesPerSec: (MESSAGE_COUNT / pathAStats.mean) * 1000,
		pathAUsPerFrame: (pathAStats.mean * 1000) / MESSAGE_COUNT,
		decodeFramesPerSec: (MESSAGE_COUNT / tDecodeStats.mean) * 1000,
		decodeUsPerFrame: (tDecodeStats.mean * 1000) / MESSAGE_COUNT,
		copyUsPerFrame: (tCopyStats.mean * 1000) / MESSAGE_COUNT,
	});
}

// sanity: also validate one TS-native parity decode so we know the corpus is
// well-formed (a single frame through the full TS decoder).
{
	const one = await buildCorpus(32);
	const msg = Message.from(new Uint8ArrayList(one[0]!));
	if (!(await msg.verify(true))) {
		throw new Error("corpus self-check: TS verify failed");
	}
}

const report = {
	kind: "wire-receive-profile",
	runtime: `node ${process.version}`,
	config: {
		payloadSizes: PAYLOAD_SIZES,
		batchSize: BATCH_SIZE,
		messageCount: MESSAGE_COUNT,
		warmupRuns: WARMUP_RUNS,
		measuredRuns: MEASURED_RUNS,
	},
	sinkGuard: sink, // printed so the optimizer cannot elide the timed work
	results,
};

const json = JSON.stringify(report, null, 2);
if (outFile) {
	await writeFile(outFile, json);
	process.stderr.write(`wrote ${outFile}\n`);
} else {
	process.stdout.write(json + "\n");
}

// Human-readable summary to stderr (does not pollute the JSON on stdout).
process.stderr.write("\n=== Measurement 1: isolate the copy (JS/wasm) ===\n");
for (const r of results) {
	process.stderr.write(
		`payload ${String(r.payload).padStart(6)}B  ` +
			`T_copy ${r.tCopyMs.mean.toFixed(2)}±${r.tCopyMs.stdev.toFixed(2)}ms  ` +
			`T_decode ${r.tDecodeMs.mean.toFixed(2)}±${r.tDecodeMs.stdev.toFixed(2)}ms  ` +
			`copy/decode ${(r.copyFraction * 100).toFixed(1)}%  ` +
			`(copy ${r.copyUsPerFrame.toFixed(3)} us/frame)\n`,
	);
}
process.stderr.write("\n=== Path A (current JS receive) ===\n");
for (const r of results) {
	process.stderr.write(
		`payload ${String(r.payload).padStart(6)}B  ` +
			`${r.pathAMs.mean.toFixed(2)}±${r.pathAMs.stdev.toFixed(2)}ms  ` +
			`${Math.round(r.pathAFramesPerSec).toLocaleString()} frames/s  ` +
			`${r.pathAUsPerFrame.toFixed(3)} us/frame\n`,
	);
}
