import { readdir, stat } from "node:fs/promises";
import { join, relative } from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";
import {
	makeScaleCensusGid,
	makeScaleCensusHash,
	memoryDeltas,
} from "./shared-log-scale-census-lib.mjs";

const INDEXER_RUST_URL = new URL(
	"../../packages/utils/indexer/rust/dist/src/index.js",
	import.meta.url,
);
const INDEXER_INTERFACE_URL = new URL(
	"../../packages/utils/indexer/interface/dist/src/index.js",
	import.meta.url,
);
const INDEXER_PERSISTENCE_URL = new URL(
	"../../packages/utils/indexer/rust/dist/src/persistence.js",
	import.meta.url,
);
const LOG_URL = new URL(
	"../../packages/log/dist/src/index.js",
	import.meta.url,
);
const LOG_RUST_URL = new URL(
	"../../packages/log/rust/dist/src/index.js",
	import.meta.url,
);
const SHARED_LOG_URL = new URL(
	"../../packages/programs/data/shared-log/dist/src/index.js",
	import.meta.url,
);
const BLOCKS_URL = new URL(
	"../../packages/transport/blocks/dist/src/index.js",
	import.meta.url,
);
const CRYPTO_URL = new URL(
	"../../packages/utils/crypto/dist/src/index.js",
	import.meta.url,
);

const CLOCK_ID = new Uint8Array(32).fill(7);
const LOG_ID = new Uint8Array(32).fill(11);

const isHeadGraphScenario = (scenario) =>
	scenario === "persistent-head-index-graph-chain" ||
	scenario === "persistent-head-index-graph-roots";

const collectGarbage = () => {
	if (typeof globalThis.gc !== "function") {
		throw new Error("scale-census workers require Node.js --expose-gc");
	}
	globalThis.gc();
	globalThis.gc();
};

const memorySnapshot = () => {
	const usage = process.memoryUsage();
	return {
		rss: usage.rss,
		heapTotal: usage.heapTotal,
		heapUsed: usage.heapUsed,
		external: usage.external,
		arrayBuffers: usage.arrayBuffers,
	};
};

const roundedMilliseconds = (value) => Math.round(value * 1_000) / 1_000;

const directoryFootprint = async (directory, count) => {
	const files = [];
	const visit = async (current) => {
		for (const entry of await readdir(current, { withFileTypes: true })) {
			const path = join(current, entry.name);
			if (entry.isDirectory()) {
				await visit(path);
				continue;
			}
			if (!entry.isFile()) {
				continue;
			}
			const file = await stat(path);
			files.push({
				path: relative(directory, path).split("\\").join("/"),
				logicalBytes: file.size,
				allocatedBytes:
					typeof file.blocks === "number" ? file.blocks * 512 : null,
			});
		}
	};
	await visit(directory);
	files.sort((left, right) => left.path.localeCompare(right.path));
	const logicalBytes = files.reduce((sum, file) => sum + file.logicalBytes, 0);
	const allocatedValues = files.map((file) => file.allocatedBytes);
	const allocatedBytes = allocatedValues.every((value) => value != null)
		? allocatedValues.reduce((sum, value) => sum + value, 0)
		: null;
	return {
		logicalBytes,
		logicalBytesPerEntry: Math.round(logicalBytes / count),
		allocatedBytes,
		allocatedBytesPerEntry:
			allocatedBytes == null ? null : Math.round(allocatedBytes / count),
		fileCount: files.length,
		files,
	};
};

const buildHeadRows = ({
	start,
	end,
	count,
	topology,
	ShallowEntry,
	ShallowMeta,
	LamportClock,
	Timestamp,
}) => {
	const rows = new Array(end - start);
	for (let index = start; index < end; index++) {
		const gid =
			topology === "chain" ? makeScaleCensusGid(0) : makeScaleCensusGid(index);
		rows[index - start] = new ShallowEntry({
			hash: makeScaleCensusHash(index),
			meta: new ShallowMeta({
				gid,
				clock: new LamportClock({
					id: CLOCK_ID,
					timestamp: new Timestamp({ wallTime: BigInt(index + 1) }),
				}),
				type: 0,
				next:
					topology === "chain" && index > 0
						? [makeScaleCensusHash(index - 1)]
						: [],
			}),
			payloadSize: 1,
			head: topology === "roots" || index === count - 1,
		});
	}
	return rows;
};

const createCoordinateMetaBytes = ({
	EntryReplicatedU32,
	ShallowMeta,
	LamportClock,
	Timestamp,
}) =>
	new EntryReplicatedU32({
		hash: makeScaleCensusHash(0),
		hashNumber: 0,
		coordinates: [0],
		assignedToRangeBoundary: false,
		meta: new ShallowMeta({
			gid: makeScaleCensusGid(0),
			clock: new LamportClock({
				id: CLOCK_ID,
				timestamp: new Timestamp({ wallTime: 1n }),
			}),
			type: 0,
			next: [],
		}),
	}).getMetaBytes();

const buildCoordinateRows = ({ start, end, metaBytes, EntryReplicatedU32 }) => {
	const rows = new Array(end - start);
	for (let index = start; index < end; index++) {
		rows[index - start] = new EntryReplicatedU32({
			hash: makeScaleCensusHash(index),
			hashNumber: index >>> 0,
			gid: makeScaleCensusGid(index),
			coordinates: [index >>> 0],
			wallTime: BigInt(index + 1),
			assignedToRangeBoundary: false,
			metaBytes,
		});
	}
	return rows;
};

export const seedPersistentScaleScenario = async ({
	scenario,
	count,
	run,
	directory,
}) => {
	const processStart = memorySnapshot();
	const [{ createSnapshotFile }, logModule, sharedLogModule] =
		await Promise.all([
			import(INDEXER_PERSISTENCE_URL),
			import(LOG_URL),
			import(SHARED_LOG_URL),
		]);
	const { ShallowEntry, ShallowMeta, LamportClock, Timestamp } = logModule;
	const { EntryReplicatedU32 } = sharedLogModule;
	const schema = isHeadGraphScenario(scenario)
		? ShallowEntry
		: EntryReplicatedU32;
	const snapshotFile = await createSnapshotFile(
		directory,
		isHeadGraphScenario(scenario) ? ["heads"] : [],
		["hash"],
	);
	if (!snapshotFile) {
		throw new Error("persistent snapshot fixture requires a directory");
	}
	const topology = scenario.endsWith("chain") ? "chain" : "roots";
	const metaBytes = isHeadGraphScenario(scenario)
		? undefined
		: createCoordinateMetaBytes({
				EntryReplicatedU32,
				ShallowMeta,
				LamportClock,
				Timestamp,
			});
	collectGarbage();
	const fixedRuntime = memorySnapshot();
	const buildStarted = performance.now();
	let rows = isHeadGraphScenario(scenario)
		? buildHeadRows({
				start: 0,
				end: count,
				count,
				topology,
				ShallowEntry,
				ShallowMeta,
				LamportClock,
				Timestamp,
			})
		: buildCoordinateRows({
				start: 0,
				end: count,
				metaBytes,
				EntryReplicatedU32,
			});
	const buildMs = performance.now() - buildStarted;
	const snapshotStarted = performance.now();
	await snapshotFile.compact(rows, schema);
	const snapshotMs = performance.now() - snapshotStarted;
	const retainedEntries = rows.length;
	rows = [];
	collectGarbage();
	return {
		scenario,
		count,
		run,
		phase: "seed",
		fixture: "indexer-snapshot-file-compact",
		buildMs: roundedMilliseconds(buildMs),
		snapshotMs: roundedMilliseconds(snapshotMs),
		snapshotRowsPerSecond: Math.round((count / snapshotMs) * 1_000),
		fixedRuntimeRssBytes: fixedRuntime.rss - processStart.rss,
		maxRssBytes: process.resourceUsage().maxRSS * 1_024,
		disk: await directoryFootprint(directory, count),
		validation: { retainedEntriesInSnapshot: retainedEntries },
	};
};

const warmRustIndex = async (create, schema, scope) => {
	const indices = create();
	await indices.start();
	const owner = scope ? await indices.scope(scope) : indices;
	await owner.init({ schema });
	await indices.drop();
};

const warmNativeGraph = async (createLogGraphIndex) => {
	const graph = await createLogGraphIndex();
	graph.clear();
};

const reopenCoordinateIndex = async ({ scenario, count, run, directory }) => {
	const processStart = memorySnapshot();
	const [{ create }, { toId }, { EntryReplicatedU32 }] = await Promise.all([
		import(INDEXER_RUST_URL),
		import(INDEXER_INTERFACE_URL),
		import(SHARED_LOG_URL),
	]);
	await warmRustIndex(create, EntryReplicatedU32);
	collectGarbage();
	const empty = memorySnapshot();
	const indices = create(directory);
	const openStarted = performance.now();
	await indices.start();
	const index = await indices.init({ schema: EntryReplicatedU32 });
	const openMs = performance.now() - openStarted;
	if (index.getSize() !== count) {
		throw new Error(
			`coordinate reopen retained ${index.getSize()} of ${count} rows`,
		);
	}
	const retainedEntries = index.getSize();
	const [first, last] = await Promise.all([
		index.get(toId(makeScaleCensusHash(0))),
		index.get(toId(makeScaleCensusHash(count - 1))),
	]);
	if (
		first?.value?.coordinates?.[0] !== 0 ||
		last?.value?.coordinates?.[0] !== (count - 1) >>> 0
	) {
		throw new Error("coordinate reopen failed endpoint validation");
	}
	collectGarbage();
	const opened = memorySnapshot();
	const openMaxRssBytes = process.resourceUsage().maxRSS * 1_024;
	const closeStarted = performance.now();
	await indices.stop();
	const closeMs = performance.now() - closeStarted;
	return {
		scenario,
		count,
		run,
		phase: "reopen",
		openMs: roundedMilliseconds(openMs),
		openEntriesPerSecond: Math.round((count / openMs) * 1_000),
		closeMs: roundedMilliseconds(closeMs),
		fixedRuntimeRssBytes: empty.rss - processStart.rss,
		openMaxRssBytes,
		maxRssBytes: process.resourceUsage().maxRSS * 1_024,
		...memoryDeltas(empty, opened, count),
		disk: await directoryFootprint(directory, count),
		validation: {
			retainedEntries,
			endpointCoordinates: [
				first.value.coordinates[0],
				last.value.coordinates[0],
			],
		},
	};
};

const reopenHeadIndexAndGraph = async ({ scenario, count, run, directory }) => {
	const processStart = memorySnapshot();
	const [indexerModule, logModule, logRustModule, blocksModule, cryptoModule] =
		await Promise.all([
			import(INDEXER_RUST_URL),
			import(LOG_URL),
			import(LOG_RUST_URL),
			import(BLOCKS_URL),
			import(CRYPTO_URL),
		]);
	const { create } = indexerModule;
	const { Log, ShallowEntry } = logModule;
	const { createLogGraphIndex } = logRustModule;
	const { AnyBlockStore } = blocksModule;
	const { Ed25519Keypair } = cryptoModule;
	await warmRustIndex(create, ShallowEntry, "heads");
	await warmNativeGraph(createLogGraphIndex);
	const store = new AnyBlockStore();
	await store.start();
	const key = await Ed25519Keypair.create();
	collectGarbage();
	const empty = memorySnapshot();
	const indices = create(directory);
	const log = new Log({ id: LOG_ID });
	const openStarted = performance.now();
	await log.open(store, key, {
		indexer: indices,
		nativeGraph: true,
	});
	const openMs = performance.now() - openStarted;
	const graph = log.entryIndex.properties.nativeGraph?.graph;
	if (!graph || graph.length !== count || log.length !== count) {
		throw new Error(
			`graph reopen retained ${graph?.length ?? 0} of ${count} entries`,
		);
	}
	if (
		!graph.has(makeScaleCensusHash(0)) ||
		!graph.has(makeScaleCensusHash(count - 1))
	) {
		throw new Error("graph reopen failed endpoint membership validation");
	}
	const topology = scenario.endsWith("chain") ? "chain" : "roots";
	const sampledGids =
		topology === "chain" || count === 1
			? [makeScaleCensusGid(0)]
			: [makeScaleCensusGid(0), makeScaleCensusGid(count - 1)];
	const headSamples = sampledGids.flatMap((gid) => graph.heads(gid));
	if (
		(topology === "chain" &&
			(headSamples.length !== 1 ||
				headSamples[0] !== makeScaleCensusHash(count - 1))) ||
		(topology === "roots" && headSamples.length !== sampledGids.length)
	) {
		throw new Error("graph reopen failed head validation");
	}
	collectGarbage();
	const opened = memorySnapshot();
	const openMaxRssBytes = process.resourceUsage().maxRSS * 1_024;
	const closeStarted = performance.now();
	await log.close();
	const closeMs = performance.now() - closeStarted;
	await store.stop();
	return {
		scenario,
		count,
		run,
		phase: "reopen",
		openMs: roundedMilliseconds(openMs),
		openEntriesPerSecond: Math.round((count / openMs) * 1_000),
		closeMs: roundedMilliseconds(closeMs),
		fixedRuntimeRssBytes: empty.rss - processStart.rss,
		openMaxRssBytes,
		maxRssBytes: process.resourceUsage().maxRSS * 1_024,
		...memoryDeltas(empty, opened, count),
		disk: await directoryFootprint(directory, count),
		validation: {
			retainedEntries: count,
			endpointHashesPresent: true,
			topology,
			headSampleCount: headSamples.length,
		},
	};
};

export const reopenPersistentScaleScenario = async (options) =>
	options.scenario === "persistent-coordinate-index"
		? reopenCoordinateIndex(options)
		: reopenHeadIndexAndGraph(options);
