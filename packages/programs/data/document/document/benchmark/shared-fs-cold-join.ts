/* eslint-disable no-console */
import {
	SQLiteIndex,
	create as createSqliteIndexer,
} from "@peerbit/indexer-sqlite3";
import {
	ExchangeHeadsMessage,
	RawExchangeHeadsMessage,
} from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import {
	FsDocument,
	FsProjection,
	FsStore,
	createFsDocuments,
} from "./shared-fs-fixture.js";

const fileCount = Math.max(
	1,
	Number.parseInt(process.env.FS_RECEIVE_FILES ?? "2000", 10),
);
const chunkCount = Math.max(
	1,
	Number.parseInt(process.env.FS_RECEIVE_CHUNKS ?? "2200", 10),
);
const syncMode = process.env.FS_SYNC_MODE ?? "raw";
if (syncMode !== "raw" && syncMode !== "plain") {
	throw new Error(`FS_SYNC_MODE must be raw or plain, received ${syncMode}`);
}
const orderedSessionMode = process.env.FS_ORDERED_SESSION ?? "enabled";
if (orderedSessionMode !== "enabled" && orderedSessionMode !== "disabled") {
	throw new Error(
		`FS_ORDERED_SESSION must be enabled or disabled, received ${orderedSessionMode}`,
	);
}
if (orderedSessionMode === "disabled") {
	// This is set before any target index exists, so sync cannot race the A/B
	// switch. The candidate then follows the same public scalar path as release.
	(SQLiteIndex.prototype as any).withOrderedWriteSession = undefined;
}

type SqlCounters = {
	execCalls: number;
	orderedChunks: number;
	orderedRows: number;
	prepareCalls: number;
	statementRuns: number;
};

const instrumentSqlite = async (directory: string) => {
	const indices = await createSqliteIndexer(directory);
	const database = indices.properties.db;
	const counters: SqlCounters = {
		execCalls: 0,
		orderedChunks: 0,
		orderedRows: 0,
		prepareCalls: 0,
		statementRuns: 0,
	};
	const instrumentedStatements = new WeakSet<object>();
	const instrumentStatement = <T extends { run: (...args: any[]) => any }>(
		statement: T,
	): T => {
		if (instrumentedStatements.has(statement)) {
			return statement;
		}
		instrumentedStatements.add(statement);
		const run = statement.run.bind(statement);
		statement.run = (...args: any[]) => {
			counters.statementRuns++;
			return run(...args);
		};
		return statement;
	};
	const exec = database.exec.bind(database);
	database.exec = (sql: string) => {
		counters.execCalls++;
		if (sql.startsWith("SAVEPOINT peerbit_ordered_chunk_")) {
			counters.orderedChunks++;
		} else if (sql.startsWith("SAVEPOINT peerbit_ordered_row_")) {
			counters.orderedRows++;
		}
		return exec(sql);
	};
	const prepare = database.prepare.bind(database);
	database.prepare = async (sql: string, id?: string) => {
		counters.prepareCalls++;
		return instrumentStatement(await prepare(sql, id));
	};
	if (database.prepareMany) {
		const prepareMany = database.prepareMany.bind(database);
		database.prepareMany = async (statements) => {
			counters.prepareCalls += statements.length;
			return (await prepareMany(statements)).map(instrumentStatement);
		};
	}
	return { counters, indices };
};

const validateDocument = async (document: FsDocument): Promise<boolean> => {
	// An arbitrary async policy callback deliberately keeps this receive path out
	// of descriptor/native batching, matching the downstream shared-fs shape.
	await Promise.resolve();
	if (
		!(document instanceof FsDocument) ||
		document.id.length === 0 ||
		document.kind.length === 0
	) {
		return false;
	}
	if (document.kind === "chunk") {
		return (
			document.id.startsWith("chunk:") && document.bytes.byteLength === 1024
		);
	}
	if (document.kind === "version") {
		return (
			document.id.startsWith("version:") &&
			document.bytes.byteLength === 64 &&
			document.chunkRefs.length === 1 &&
			document.causalRefs.length === 1
		);
	}
	return document.kind === "name" && document.id.startsWith("name:");
};

const assertProjectedValues = async (
	store: FsStore,
	label: string,
): Promise<void> => {
	const representatives = [
		new FsDocument({ id: "name:0", kind: "name" }),
		new FsDocument({
			id: `version:${fileCount - 1}:0`,
			kind: "version",
			bytes: new Uint8Array(64).fill((fileCount - 1) % 251),
			chunkRefs: [`chunk:${(fileCount - 1) % chunkCount}`],
			causalRefs: [`name:${fileCount - 1}`],
		}),
		new FsDocument({
			id: `chunk:${chunkCount - 1}`,
			kind: "chunk",
			bytes: new Uint8Array(1024).fill((chunkCount - 1) % 251),
		}),
	];
	for (const document of representatives) {
		const expected = new FsProjection(document);
		const actual = await store.docs.index.get(document.id, {
			local: true,
			remote: false,
			resolve: false,
		});
		if (
			!actual ||
			actual.id !== expected.id ||
			actual.kind !== expected.kind ||
			actual.byteLength !== expected.byteLength ||
			JSON.stringify(actual.chunkRefs) !== JSON.stringify(expected.chunkRefs) ||
			JSON.stringify(actual.causalRefs) !== JSON.stringify(expected.causalRefs)
		) {
			throw new Error(
				`${label} projection mismatch for ${document.id}: ${JSON.stringify(actual)}`,
			);
		}
	}
};

const documents = createFsDocuments(fileCount, chunkCount);
const rootDirectory = await mkdtemp(join(tmpdir(), "peerbit-shared-fs-sync-"));
const targetDirectory = join(rootDirectory, syncMode);
let session: TestSession | undefined;
let reopenSession: TestSession | undefined;
try {
	const targetSqlite = await instrumentSqlite(targetDirectory);
	session = await TestSession.connected(2, [
		{},
		{ indexer: () => targetSqlite.indices },
	]);
	let sourcePolicyCalls = 0;
	let sourceTransformCalls = 0;
	const source = await session.peers[0].open(new FsStore(), {
		args: {
			canPerform: async ({ type, value }) => {
				sourcePolicyCalls++;
				return type === "put" && (await validateDocument(value));
			},
			index: {
				type: FsProjection,
				transform: async (document) => {
					sourceTransformCalls++;
					await Promise.resolve();
					return new FsProjection(document);
				},
			},
			replicate: { factor: 1 },
			replicas: { min: 1 },
			sync: syncMode === "raw" ? { rawExchangeHeads: true } : undefined,
		},
	});
	const target = source.clone();
	let eventAdded = 0;
	let eventBatches = 0;
	// Borsh-deserialized programs re-establish listener accounting in open().
	// Initialize it before registering so receive events during open are observed.
	(target.docs as any).trackDocumentChangeListeners();
	target.docs.events.addEventListener("change", (event) => {
		eventBatches++;
		eventAdded += event.detail.added.length;
	});

	const preparationStartedAt = performance.now();
	await source.docs.putMany(documents, {
		unique: true,
		replicate: false,
		target: "none",
	});
	const preparationMs = performance.now() - preparationStartedAt;

	let rawExchangeHeads = 0;
	let plainExchangeHeads = 0;
	const send = source.docs.log.rpc.send.bind(source.docs.log.rpc);
	source.docs.log.rpc.send = async (message, options) => {
		if (message instanceof RawExchangeHeadsMessage) {
			rawExchangeHeads++;
		} else if (message instanceof ExchangeHeadsMessage) {
			plainExchangeHeads++;
		}
		return send(message, options);
	};

	let targetPolicyCalls = 0;
	let targetTransformCalls = 0;
	const receiveStartedAt = performance.now();
	await session.peers[1].open(target, {
		args: {
			canPerform: async ({ type, value }) => {
				targetPolicyCalls++;
				return type === "put" && (await validateDocument(value));
			},
			index: {
				type: FsProjection,
				transform: async (document) => {
					targetTransformCalls++;
					await Promise.resolve();
					return new FsProjection(document);
				},
			},
			replicate: { factor: 1 },
			replicas: { min: 1 },
			sync: syncMode === "raw" ? { rawExchangeHeads: true } : undefined,
		},
	});
	await waitForResolved(
		async () => {
			if ((await target.docs.index.getSize()) !== documents.length) {
				throw new Error("target projection is incomplete");
			}
			if (eventAdded !== documents.length) {
				throw new Error("target change events are incomplete");
			}
		},
		{
			timeout: 120_000,
			timeoutMessage: `${syncMode} shared-fs cold join`,
		},
	);
	const receiveMs = performance.now() - receiveStartedAt;
	const receivedCount = await target.docs.index.getSize();
	const targetLogLength = target.docs.log.log.length;
	if (
		receivedCount !== documents.length ||
		targetLogLength !== documents.length ||
		targetPolicyCalls !== documents.length ||
		targetTransformCalls !== documents.length ||
		eventAdded !== documents.length
	) {
		throw new Error(
			`Receive stage mismatch: ${JSON.stringify({ receivedCount, targetLogLength, targetPolicyCalls, targetTransformCalls, eventAdded })}`,
		);
	}
	const shouldUseOrderedProjection =
		syncMode === "raw" && orderedSessionMode === "enabled";
	if (
		(shouldUseOrderedProjection &&
			(targetSqlite.counters.orderedRows !== documents.length ||
				targetSqlite.counters.orderedChunks === 0)) ||
		(!shouldUseOrderedProjection &&
			(targetSqlite.counters.orderedRows !== 0 ||
				targetSqlite.counters.orderedChunks !== 0))
	) {
		throw new Error(
			`Unexpected ordered projection statements: ${JSON.stringify(targetSqlite.counters)}`,
		);
	}
	await assertProjectedValues(target, "received");
	const targetReopen = source.clone();
	await session.stop();
	session = undefined;

	reopenSession = await TestSession.disconnected(1, [
		{ indexer: () => createSqliteIndexer(targetDirectory) },
	]);
	const reopened = await reopenSession.peers[0].open(targetReopen, {
		args: {
			index: {
				type: FsProjection,
				transform: async (document) => new FsProjection(document),
			},
			replicate: false,
		},
	});
	const reopenCount = await reopened.docs.index.getSize();
	if (reopenCount !== documents.length) {
		throw new Error(
			`Persisted reopen expected ${documents.length}, received ${reopenCount}`,
		);
	}
	await assertProjectedValues(reopened, "reopened");
	if (syncMode === "raw" && rawExchangeHeads === 0) {
		throw new Error("Raw mode sent no RawExchangeHeadsMessage");
	}
	if (syncMode === "raw" && plainExchangeHeads !== 0) {
		throw new Error("Raw mode unexpectedly sent ExchangeHeadsMessage");
	}
	if (syncMode === "plain" && plainExchangeHeads === 0) {
		throw new Error("Plain mode sent no ExchangeHeadsMessage");
	}

	console.log(
		JSON.stringify({
			syncMode,
			orderedSessionMode,
			workload: {
				files: fileCount,
				chunks: chunkCount,
				documents: documents.length,
				projectionChildVectors: ["chunkRefs", "causalRefs"],
				arbitraryAsyncCanPerform: true,
			},
			preparation: {
				elapsedMs: preparationMs,
				policyCalls: sourcePolicyCalls,
				transformCalls: sourceTransformCalls,
			},
			receive: {
				elapsedMs: receiveMs,
				documentsPerSecond: (documents.length * 1000) / receiveMs,
				indexCount: receivedCount,
				logLength: targetLogLength,
				policyCalls: targetPolicyCalls,
				transformCalls: targetTransformCalls,
				eventBatches,
				eventAdded,
				rawExchangeHeads,
				plainExchangeHeads,
				sqlite: targetSqlite.counters,
			},
			reopen: { indexCount: reopenCount },
		}),
	);
} finally {
	await session?.stop();
	await reopenSession?.stop();
	await rm(rootDirectory, { recursive: true, force: true });
}
