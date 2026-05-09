import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { Log } from "../src/log.js";

type BenchRow = {
	name: string;
	nativeGraph: boolean;
	entries: number;
	iterations: number;
	elapsedMs: number;
	opsPerSecond: number;
};

const entries = Number(process.env.PEERBIT_LOG_NATIVE_GRAPH_ENTRIES ?? 1_000);
const iterations = Number(
	process.env.PEERBIT_LOG_NATIVE_GRAPH_ITERATIONS ?? 1_000,
);

const key = await Ed25519Keypair.create();

const createHeadsLog = async (nativeGraph: boolean) => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	for (let i = 0; i < entries; i++) {
		await log.append(new Uint8Array([i & 0xff]), { meta: { next: [] } });
	}
	return { log, store };
};

const createChainLog = async (nativeGraph: boolean) => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	const root = (await log.append(new Uint8Array([0]), { meta: { next: [] } }))
		.entry;
	let previous = root;
	for (let i = 1; i < entries; i++) {
		previous = (
			await log.append(new Uint8Array([i & 0xff]), {
				meta: { next: [previous] },
			})
		).entry;
	}
	return { log, rootHash: root.hash, store };
};

const measure = async (
	name: string,
	nativeGraph: boolean,
	fn: () => Promise<void>,
): Promise<BenchRow> => {
	const started = performance.now();
	for (let i = 0; i < iterations; i++) {
		await fn();
	}
	const elapsed = performance.now() - started;
	return {
		name,
		nativeGraph,
		entries,
		iterations,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((iterations / elapsed) * 1000),
	};
};

const rows: BenchRow[] = [];

for (const nativeGraph of [false, true]) {
	const { log, store } = await createHeadsLog(nativeGraph);
	rows.push(
		await measure("getHeads().all()", nativeGraph, async () => {
			await log.getHeads().all();
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, rootHash, store } = await createChainLog(nativeGraph);
	rows.push(
		await measure("countHasNext(root)", nativeGraph, async () => {
			await log.entryIndex.countHasNext(rootHash);
		}),
	);
	await log.close();
	await store.stop();
}

console.table(rows);
