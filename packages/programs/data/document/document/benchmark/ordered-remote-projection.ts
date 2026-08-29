/* eslint-disable no-console */
import { create as createSqliteIndexer } from "@peerbit/indexer-sqlite3";
import { SQLiteIndices } from "@peerbit/indexer-sqlite3";
import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import type { Operation } from "../src/operation.js";
import {
	FsDocument,
	FsProjection,
	FsStore,
	createFsDocuments,
	projectFsDocument,
} from "./shared-fs-fixture.js";

const fileCount = Math.max(
	1,
	Number.parseInt(process.env.FS_PROJECTION_FILES ?? "2000", 10),
);
const chunkCount = Math.max(
	1,
	Number.parseInt(process.env.FS_PROJECTION_CHUNKS ?? "2200", 10),
);
const transformDelayMs = Math.max(
	1,
	Number.parseInt(process.env.FS_TRANSFORM_HOLD_MS ?? "50", 10),
);
const sampleCount = Math.max(
	1,
	Number.parseInt(process.env.FS_PROJECTION_SAMPLES ?? "3", 10),
);

const openArgs = {
	replicate: false as const,
	index: {
		type: FsProjection,
		transform: projectFsDocument,
	},
};

const documents = createFsDocuments(fileCount, chunkCount);

type ProjectionCounters = {
	contextReads: number;
	orderedChunks: number;
	publicScalarPuts: number;
	rowSavepoints: number;
	rowWrites: number;
};

const runProjection = async (
	store: FsStore,
	entries: Entry<Operation>[],
	mode: "ordered" | "scalar",
): Promise<
	ProjectionCounters & {
		count: number;
		documentsPerSecond: number;
		elapsedMs: number;
	}
> => {
	const backend = store.docs.index.index as any;
	const database = (store.node.indexer as SQLiteIndices).properties.db;
	const counters: ProjectionCounters = {
		contextReads: 0,
		orderedChunks: 0,
		publicScalarPuts: 0,
		rowSavepoints: 0,
		rowWrites: 0,
	};
	const originalGetUnlocked = backend.getUnlocked.bind(backend);
	const originalPutUnlocked = backend.putUnlocked.bind(backend);
	const originalPut = backend.put.bind(backend);
	const originalExec = database.exec.bind(database);
	const originalOrdered = backend.withOrderedWriteSession;
	backend.getUnlocked = (...args: unknown[]) => {
		counters.contextReads++;
		return originalGetUnlocked(...args);
	};
	backend.putUnlocked = (...args: unknown[]) => {
		counters.rowWrites++;
		return originalPutUnlocked(...args);
	};
	backend.put = (...args: unknown[]) => {
		counters.publicScalarPuts++;
		return originalPut(...args);
	};
	database.exec = (sql: string) => {
		if (sql.startsWith("SAVEPOINT peerbit_ordered_chunk_")) {
			counters.orderedChunks++;
		} else if (sql.startsWith("SAVEPOINT peerbit_ordered_row_")) {
			counters.rowSavepoints++;
		}
		return originalExec(sql);
	};
	if (mode === "scalar") {
		backend.withOrderedWriteSession = undefined;
	}

	try {
		const startedAt = performance.now();
		await (store.docs as any).handleChanges({
			added: entries.map((entry) => ({ head: true, entry })),
			removed: [],
		});
		const elapsedMs = performance.now() - startedAt;
		const count = await backend.count();
		if (count !== documents.length) {
			throw new Error(`Expected ${documents.length} rows, received ${count}`);
		}
		return {
			...counters,
			count,
			elapsedMs,
			documentsPerSecond: (documents.length * 1000) / elapsedMs,
		};
	} finally {
		backend.getUnlocked = originalGetUnlocked;
		backend.putUnlocked = originalPutUnlocked;
		backend.put = originalPut;
		backend.withOrderedWriteSession = originalOrdered;
		database.exec = originalExec;
	}
};

const materializeEntries = async (
	store: FsStore,
	entries: Entry<Operation>[],
): Promise<Entry<Operation>[]> => {
	const materialized: Entry<Operation>[] = [];
	const materializationBatchSize = 256;
	for (
		let offset = 0;
		offset < entries.length;
		offset += materializationBatchSize
	) {
		const batch = await Promise.all(
			entries
				.slice(offset, offset + materializationBatchSize)
				.map(async (entry) => {
					const reloaded = await Entry.fromMultihash<Operation>(
						store.docs.log.log.blocks,
						entry.hash,
					);
					reloaded.init({
						encoding: store.docs.log.log.encoding,
						keychain: store.docs.log.log.keychain,
					});
					return reloaded;
				}),
		);
		materialized.push(...batch);
	}
	return materialized;
};

const median = (values: number[]): number => {
	const sorted = [...values].sort((left, right) => left - right);
	const midpoint = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? (sorted[midpoint - 1]! + sorted[midpoint]!) / 2
		: sorted[midpoint]!;
};

const measureAsyncTransformHold = async () => {
	const indices = await createSqliteIndexer();
	await indices.start();
	const first = await indices.init<FsProjection, never>({
		schema: FsProjection,
		indexBy: ["id"],
	});
	const second = await (
		await indices.scope("competitor")
	).init<FsProjection, never>({ schema: FsProjection, indexBy: ["id"] });
	let markStarted!: () => void;
	const started = new Promise<void>((resolve) => (markStarted = resolve));
	const ordered = Promise.resolve(
		first.withOrderedWriteSession!(async (session) => {
			await session.put(
				new FsProjection(
					new FsDocument({ id: "hold:first", kind: "benchmark" }),
				),
			);
			markStarted();
			await new Promise((resolve) => setTimeout(resolve, transformDelayMs));
			await session.put(
				new FsProjection(
					new FsDocument({ id: "hold:second", kind: "benchmark" }),
				),
			);
		}),
	);
	await started;
	const competitorStartedAt = performance.now();
	await Promise.all([
		ordered,
		second.put(
			new FsProjection(new FsDocument({ id: "competitor", kind: "benchmark" })),
		),
	]);
	const competitorLatencyMs = performance.now() - competitorStartedAt;
	await indices.stop();
	await indices.drop();
	return competitorLatencyMs;
};

const rootDirectory = await mkdtemp(
	join(tmpdir(), "peerbit-shared-fs-projection-"),
);
const modes = Array.from({ length: sampleCount }, (_, index) =>
	index % 2 === 0
		? (["scalar", "ordered"] as const)
		: (["ordered", "scalar"] as const),
).flat();
const runDirectories = modes.map((mode, index) =>
	join(rootDirectory, `${String(index).padStart(2, "0")}-${mode}`),
);
let firstSession: TestSession | undefined;
let reopenSession: TestSession | undefined;
try {
	firstSession = await TestSession.disconnected(1 + modes.length, [
		{},
		...runDirectories.map((directory) => ({
			indexer: () => createSqliteIndexer(directory),
		})),
	]);
	const source = await firstSession.peers[0].open(new FsStore(), {
		args: openArgs,
	});
	const preparationStartedAt = performance.now();
	const appended = await source.docs.putMany(documents, {
		unique: true,
		replicate: false,
		target: "none",
	});
	const preparationMs = performance.now() - preparationStartedAt;
	const samples: Array<
		ProjectionCounters & {
			mode: "ordered" | "scalar";
			run: number;
			count: number;
			documentsPerSecond: number;
			elapsedMs: number;
		}
	> = [];
	for (let index = 0; index < modes.length; index++) {
		const mode = modes[index]!;
		const target = await firstSession.peers[index + 1]!.open(source.clone(), {
			args: openArgs,
		});
		// Every timed run receives a distinct Entry graph materialized from the
		// same immutable blocks. No candidate inherits scalar payload decoding.
		const entries = await materializeEntries(source, appended.entries);
		samples.push({
			mode,
			run: Math.floor(index / 2) + 1,
			...(await runProjection(target, entries, mode)),
		});
	}
	const scalarReopen = source.clone();
	const orderedReopen = source.clone();
	const scalarReopenDirectory = runDirectories[modes.lastIndexOf("scalar")]!;
	const orderedReopenDirectory = runDirectories[modes.lastIndexOf("ordered")]!;
	await firstSession.stop();
	firstSession = undefined;

	reopenSession = await TestSession.disconnected(2, [
		{ indexer: () => createSqliteIndexer(scalarReopenDirectory) },
		{ indexer: () => createSqliteIndexer(orderedReopenDirectory) },
	]);
	const reopenedScalar = await reopenSession.peers[0].open(scalarReopen, {
		args: openArgs,
	});
	const reopenedOrdered = await reopenSession.peers[1].open(orderedReopen, {
		args: openArgs,
	});
	const reopenedScalarCount = await reopenedScalar.docs.index.getSize();
	const reopenedOrderedCount = await reopenedOrdered.docs.index.getSize();
	if (
		reopenedScalarCount !== documents.length ||
		reopenedOrderedCount !== documents.length
	) {
		throw new Error(
			`Reopen verification failed: scalar=${reopenedScalarCount}, ordered=${reopenedOrderedCount}`,
		);
	}
	const asyncTransformCompetitorLatencyMs = await measureAsyncTransformHold();
	const scalarSamples = samples.filter((sample) => sample.mode === "scalar");
	const orderedSamples = samples.filter((sample) => sample.mode === "ordered");
	const scalarMedianMs = median(
		scalarSamples.map((sample) => sample.elapsedMs),
	);
	const orderedMedianMs = median(
		orderedSamples.map((sample) => sample.elapsedMs),
	);

	console.log(
		JSON.stringify({
			workload: {
				files: fileCount,
				chunks: chunkCount,
				documents: documents.length,
				projectionChildVectors: ["chunkRefs", "causalRefs"],
			},
			preparationMs,
			fairness: {
				order: modes,
				independentEntriesPerRun: true,
				note: "Every run rematerializes entries from the same source blocks before timing, with counterbalanced scalar/ordered order.",
			},
			samples,
			median: {
				scalarElapsedMs: scalarMedianMs,
				scalarDocumentsPerSecond: (documents.length * 1000) / scalarMedianMs,
				orderedElapsedMs: orderedMedianMs,
				orderedDocumentsPerSecond: (documents.length * 1000) / orderedMedianMs,
				speedup: scalarMedianMs / orderedMedianMs,
			},
			reopen: {
				scalarCount: reopenedScalarCount,
				orderedCount: reopenedOrderedCount,
			},
			admission: {
				configuredAsyncTransformDelayMs: transformDelayMs,
				asyncTransformCompetitorLatencyMs,
				note: "The 64-write bound is not a wall-clock bound: an async transform after an uncommitted write retains connection admission.",
			},
		}),
	);
} finally {
	await firstSession?.stop();
	await reopenSession?.stop();
	await rm(rootDirectory, { recursive: true, force: true });
}
