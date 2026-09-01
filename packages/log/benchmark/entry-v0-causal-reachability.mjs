/* eslint-disable no-console */
import assert from "node:assert/strict";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";

const workspace = process.env.PEERBIT_CAUSAL_BENCH_WORKSPACE ?? process.cwd();
const workspaceModule = (relativePath) =>
	pathToFileURL(join(workspace, relativePath)).href;
const { AnyBlockStore } = await import(
	workspaceModule("packages/transport/blocks/dist/src/index.js")
);
const { calculateRawCid } = await import(
	workspaceModule("packages/transport/blocks-interface/dist/src/index.js")
);
const { Ed25519Keypair } = await import(
	workspaceModule("packages/utils/crypto/dist/src/index.js")
);
const { createEntry } = await import(
	workspaceModule("packages/log/dist/src/entry-create.js")
);
const { Entry } = await import(
	workspaceModule("packages/log/dist/src/entry.js")
);
const causalReachabilityModule = workspaceModule(
	"packages/log/dist/src/entry-v0-causal-reachability.js",
);
const { checkBoundedEntryV0CausalReachability } = await import(
	causalReachabilityModule
);

const parsePositiveInteger = (name, fallback) => {
	const raw = process.env[name];
	const value = raw == null ? fallback : Number.parseInt(raw, 10);
	if (!Number.isSafeInteger(value) || value < 1) {
		throw new Error(`${name} must be a positive safe integer, got ${raw}`);
	}
	return value;
};

const width = parsePositiveInteger("PEERBIT_CAUSAL_BENCH_WIDTH", 4_096);
const batchSize = parsePositiveInteger("PEERBIT_CAUSAL_BENCH_BATCH_SIZE", 1);
const label = process.env.PEERBIT_CAUSAL_BENCH_LABEL;
const progressEvery = Number.parseInt(
	process.env.PEERBIT_CAUSAL_BENCH_PROGRESS_EVERY ?? "0",
	10,
);

const store = new AnyBlockStore();
await store.start();
const identity = await Ed25519Keypair.create();
let sampler;
let sequence = 0;
const createRawEntry = async (parents = []) => {
	const data = new Uint8Array(4);
	new DataView(data.buffer).setUint32(0, sequence++, true);
	const entry = await createEntry({
		store,
		identity,
		data,
		meta: { next: parents },
		deferStore: true,
	});
	const prepared = Entry.takePreparedBlock(entry);
	assert(prepared, "expected prepared EntryV0 storage");
	return {
		entry,
		cid: prepared.cid,
		bytes: new Uint8Array(prepared.block.bytes),
	};
};

try {
	const setupStartedAt = performance.now();
	const leaves = new Array(width);
	for (let i = 0; i < width; i++) {
		leaves[i] = await createRawEntry();
		if (progressEvery > 0 && (i + 1) % progressEvery === 0) {
			console.error(
				JSON.stringify({ event: "causal-benchmark-setup", completed: i + 1 }),
			);
		}
	}
	const descendant = await createRawEntry(leaves.map((leaf) => leaf.entry));
	const unrelatedAncestor = (
		await calculateRawCid(new Uint8Array([0xff, 0xfe, 0xfd, 0xfc]))
	).cid;
	const values = new Map(leaves.map((leaf) => [leaf.cid, leaf.bytes]));
	const totalBytes =
		descendant.bytes.byteLength +
		leaves.reduce((sum, leaf) => sum + leaf.bytes.byteLength, 0);
	let maxEntryBytes = descendant.bytes.byteLength;
	for (const leaf of leaves) {
		maxEntryBytes = Math.max(maxEntryBytes, leaf.bytes.byteLength);
	}
	leaves.length = 0;
	globalThis.gc?.();
	const setupMs = performance.now() - setupStartedAt;

	const rssBeforeBytes = process.memoryUsage().rss;
	let peakRssBytes = rssBeforeBytes;
	sampler = setInterval(() => {
		peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
	}, 5);
	const startedAt = performance.now();
	const result = await checkBoundedEntryV0CausalReachability({
		ancestorCid: unrelatedAncestor,
		descendant,
		limits: {
			maxEntryBytes,
			maxDirectParents: width,
			maxVisitedEntries: width + 1,
			maxTotalBytes: totalBytes,
			maxParentLinks: width,
			maxResolveBatchSize: batchSize,
		},
		resolve: async (cids) => new Map(cids.map((cid) => [cid, values.get(cid)])),
	});
	const traversalMs = performance.now() - startedAt;
	clearInterval(sampler);
	peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
	assert.equal(result.status, "not-ancestor");
	assert.equal(result.visited.entries, width + 1);
	assert.equal(result.visited.resolverCalls, Math.ceil(width / batchSize));

	console.log(
		JSON.stringify({
			label,
			width,
			batchSize,
			setupMs,
			traversalMs,
			entriesPerSecond: width / (traversalMs / 1_000),
			rssBeforeBytes,
			peakRssBytes,
			peakRssDeltaBytes: peakRssBytes - rssBeforeBytes,
			visited: result.visited,
			node: process.version,
			platform: `${process.platform}-${process.arch}`,
		}),
	);
} finally {
	clearInterval(sampler);
	await store.stop();
}
