import { createStore as createDefaultStore } from "@peerbit/any-store";
import { type AnyStore } from "@peerbit/any-store-interface";
import { createStore as createRustStore } from "@peerbit/any-store-rust";
import crypto from "crypto";
import { mkdtemp, rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { performance } from "perf_hooks";
import { AnyBlockStore } from "../src/any-blockstore.js";

type BenchMode = "level" | "rust" | "rust-batch";

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

const entries = parsePositiveInteger(process.env.PEERBIT_BLOCKS_STORE_ENTRIES, 10_000);
const valueBytes = parsePositiveInteger(process.env.PEERBIT_BLOCKS_STORE_VALUE_BYTES, 256);

const createModeStore = async (
	mode: BenchMode,
): Promise<{ store: AnyStore; cleanup(): Promise<void> }> => {
	const directory = await mkdtemp(join(tmpdir(), `peerbit-blocks-${mode}-`));
	return {
		store: mode === "level" ? createDefaultStore(directory) : createRustStore(directory),
		cleanup: async () => {
			await rm(directory, { recursive: true, force: true });
		},
	};
};

const measure = async (fn: () => Promise<void>) => {
	const started = performance.now();
	await fn();
	const elapsedMs = performance.now() - started;
	return {
		elapsedMs: Math.round(elapsedMs),
		opsPerSecond: Math.round((entries / elapsedMs) * 1000),
	};
};

const runMode = async (mode: BenchMode) => {
	const { store, cleanup } = await createModeStore(mode);
	const blocks = Array.from({ length: entries }, () => crypto.randomBytes(valueBytes));
	const blockstore = new AnyBlockStore(store);
	await blockstore.start();
	try {
		let cids: string[] = [];
		const put = await measure(async () => {
			if (mode === "rust-batch") {
				cids = await blockstore.putMany(blocks);
			} else {
				for (const block of blocks) {
					cids.push(await blockstore.put(block));
				}
			}
		});
		const get = await measure(async () => {
			for (const cid of cids) {
				const bytes = await blockstore.get(cid);
				if (!bytes || bytes.byteLength !== valueBytes) {
					throw new Error(`Missing block ${cid}`);
				}
			}
		});
		return {
			mode,
			entries,
			valueBytes,
			putOpsPerSecond: put.opsPerSecond,
			putMs: put.elapsedMs,
			getOpsPerSecond: get.opsPerSecond,
			getMs: get.elapsedMs,
			verifiedBlocks: cids.length,
		};
	} finally {
		await blockstore.stop();
		await cleanup();
	}
};

const rows = [];
for (const mode of ["level", "rust", "rust-batch"] as const) {
	rows.push(await runMode(mode));
}
console.table(rows);
