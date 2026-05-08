import { createStore as createDefaultStore } from "@peerbit/any-store";
import { type AnyStore } from "@peerbit/any-store-interface";
import crypto from "crypto";
import { mkdtemp, rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { performance } from "perf_hooks";
import { RustAnyStore, createStore as createRustStore } from "../src/index.js";

type BenchMode =
	| "memory"
	| "level"
	| "rust-memory"
	| "rust-memory-batch"
	| "rust-redb-memory"
	| "rust-redb-memory-batch"
	| "rust-persist"
	| "rust-persist-batch"
	| "rust-persist-strict"
	| "rust-persist-strict-batch";

type StoreMode =
	| "memory"
	| "level"
	| "rust-memory"
	| "rust-redb-memory"
	| "rust-persist"
	| "rust-persist-strict";

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

const entries = parsePositiveInteger(process.env.PEERBIT_ANY_STORE_RUST_ENTRIES, 10_000);
const valueBytes = parsePositiveInteger(process.env.PEERBIT_ANY_STORE_RUST_VALUE_BYTES, 256);

const isBatchMode = (mode: BenchMode) => mode.endsWith("-batch");

const storeMode = (mode: BenchMode): StoreMode =>
	(isBatchMode(mode) ? mode.slice(0, -"batch".length - 1) : mode) as StoreMode;

const createModeStore = async (
	mode: BenchMode,
): Promise<{ store: AnyStore; cleanup(): Promise<void> }> => {
	const baseMode = storeMode(mode);
	if (baseMode === "memory") {
		return { store: createDefaultStore(), cleanup: async () => undefined };
	}
	if (baseMode === "rust-memory") {
		return { store: createRustStore(), cleanup: async () => undefined };
	}
	if (baseMode === "rust-redb-memory") {
		return {
			store: createRustStore(undefined, { engine: "redb" }),
			cleanup: async () => undefined,
		};
	}
	const directory = await mkdtemp(join(tmpdir(), `peerbit-any-store-${baseMode}-`));
	const store =
		baseMode === "level"
			? createDefaultStore(directory)
			: createRustStore(directory, {
					durability: baseMode === "rust-persist-strict" ? "strict" : "normal",
				});
	return {
		store,
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
	const value = crypto.randomBytes(valueBytes);
	const keys = Array.from({ length: entries }, (_, i) => `key-${i}`);
	await store.open();
	try {
		const put = await measure(async () => {
			if (isBatchMode(mode)) {
				await (store as RustAnyStore).putMany(keys.map((key) => [key, value] as const));
			} else {
				for (let i = 0; i < entries; i++) {
					await store.put(keys[i], value);
				}
			}
		});
		const get = await measure(async () => {
			if (isBatchMode(mode)) {
				const values = await (store as RustAnyStore).getMany(keys);
				for (let i = 0; i < entries; i++) {
					if (!values[i] || values[i]!.byteLength !== valueBytes) {
						throw new Error(`Missing value for ${keys[i]}`);
					}
				}
			} else {
				for (let i = 0; i < entries; i++) {
					const bytes = await store.get(keys[i]);
					if (!bytes || bytes.byteLength !== valueBytes) {
						throw new Error(`Missing value for ${keys[i]}`);
					}
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
			size: await store.size(),
		};
	} finally {
		await store.close();
		await cleanup();
	}
};

const modes: BenchMode[] = [
	"memory",
	"rust-memory",
	"rust-memory-batch",
	"rust-redb-memory",
	"rust-redb-memory-batch",
	"level",
	"rust-persist",
	"rust-persist-batch",
	"rust-persist-strict",
	"rust-persist-strict-batch",
];
const rows = [];
for (const mode of modes) {
	rows.push(await runMode(mode));
}

console.table(rows);
