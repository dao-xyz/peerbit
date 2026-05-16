import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { create as createSqliteIndexer } from "@peerbit/indexer-sqlite3";
import { Log } from "@peerbit/log";
import {
	NativeBackboneNodeCoordinatePersistence,
	createNativePeerbitBackbone,
	defaultNativeBackboneCoordinateFlushMaxPendingBytes,
} from "@peerbit/native-backbone";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { createRustPeerbitOptions } from "peerbit/rust";
import { Documents, type SetupOptions, policy } from "../src/index.js";

// Run with:
//   cd packages/programs/data/document/document
//   node --loader ts-node/esm ./benchmark/document-put.ts
//
// Env:
// - DOC_WARMUP=100
// - DOC_ITERATIONS=1000
// - DOC_BYTES=1200
// - DOC_COORDINATE_WAL_FLUSH_BYTES=1048576
// - DOC_COORDINATE_WAL_FLUSH_INTERVAL_MS unset by default
// - DOC_SCENARIOS=compat-path,hybrid-anystore,simple-index,sqlite-index,native-graph,native-block-store,rust-peerbit,rust-peerbit-local,rust-peerbit-transient-index,rust-peerbit-backbone-local,rust-peerbit-backbone-coordinate-wal,rust-peerbit-backbone-coordinate-wal-buffered,native-ceiling,native-backbone-ceiling
//   Add "-nonunique" to any scenario name to use default update-safe put semantics.
//   Add "-local" to a rust-peerbit scenario to disable replication and default trim.
//   Add "-trim" to a local rust-peerbit scenario to keep length trim enabled.
//   Add "-putmany" to any unique scenario name to use one putMany call per measured batch.
//   Add "-policy-allow-all" to open with canPerform: policy.allowAll().
//   Add "-policy-signed-public-key" to open with canPerform: policy.signedByPublicKey(local public key).
//   Add "-policy-put-signed-public-key" to open with canPerform: policy.put(policy.signedByPublicKey(local public key)).
//   Add "-policy-put-signed-field" to open with canPerform: policy.put(policy.signedByField("signer")).
//   Add "-canperform-allow-all" to open with canPerform: () => true.
// - DOC_PROFILE_DEEP=1 reports lower shared-log/log phase timings.
// - BENCH_JSON=1

const payloadBytes = Math.max(
	1,
	Number.parseInt(process.env.DOC_BYTES || "1200", 10) || 1200,
);
const warmupIterations = Math.max(
	0,
	Number.parseInt(process.env.DOC_WARMUP || "100", 10) || 0,
);
const iterations = Math.max(
	1,
	Number.parseInt(process.env.DOC_ITERATIONS || "1000", 10) || 1000,
);
const coordinateWalFlushBytes = Math.max(
	0,
	Number.parseInt(
		process.env.DOC_COORDINATE_WAL_FLUSH_BYTES ||
			String(defaultNativeBackboneCoordinateFlushMaxPendingBytes),
		10,
	) || defaultNativeBackboneCoordinateFlushMaxPendingBytes,
);
const coordinateWalFlushIntervalMs =
	process.env.DOC_COORDINATE_WAL_FLUSH_INTERVAL_MS == null
		? undefined
		: Math.max(
				0,
				Number.parseInt(process.env.DOC_COORDINATE_WAL_FLUSH_INTERVAL_MS, 10) ||
					0,
			);

const scenarioNames = (
	process.env.DOC_SCENARIOS ||
	"compat-path,hybrid-anystore,simple-index,sqlite-index,native-graph,native-block-store,rust-peerbit,rust-peerbit-transient-index"
)
	.split(",")
	.map((x) => x.trim())
	.filter(Boolean);

const scenarioBaseName = (name: string) =>
	name.replace(
		/(?:-(?:putmany|nonunique|local|trim|buffered|coordinate-wal|policy-allow-all|policy-signed-public-key|policy-put-signed-public-key|policy-put-signed-field|canperform-allow-all))*$/,
		"",
	);
const scenarioUsesUniquePuts = (name: string) => !name.includes("-nonunique");
const scenarioUsesPutMany = (name: string) => name.endsWith("-putmany");
const scenarioUsesLocalStore = (name: string) =>
	scenarioBaseName(name).startsWith("rust-peerbit") && name.includes("-local");
const scenarioUsesTrim = (name: string) => name.includes("-trim");
const scenarioUsesCoordinateWal = (name: string) =>
	name.includes("-coordinate-wal");
const scenarioUsesBufferedCoordinateWal = (name: string) =>
	name.includes("-coordinate-wal-buffered");
const scenarioUsesPolicyAllowAll = (name: string) =>
	name.includes("-policy-allow-all");
const scenarioUsesPolicySignedPublicKey = (name: string) =>
	name.includes("-policy-signed-public-key");
const scenarioUsesPolicyPutSignedPublicKey = (name: string) =>
	name.includes("-policy-put-signed-public-key");
const scenarioUsesPolicyPutSignedField = (name: string) =>
	name.includes("-policy-put-signed-field");
const scenarioUsesCanPerformAllowAll = (name: string) =>
	name.includes("-canperform-allow-all");
const profileDeep = process.env.DOC_PROFILE_DEEP === "1";

let currentSignerFieldBytes: Uint8Array | undefined;

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	@field({ type: option(Uint8Array) })
	signer?: Uint8Array;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
			this.bytes = opts.bytes;
			this.signer = opts.signer;
		}
	}
}

@variant("test_documents")
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor(properties?: { docs: Documents<Document> }) {
		super();
		this.docs = properties?.docs ?? new Documents<Document>();
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({ ...options, type: Document });
	}
}

type Profile = {
	serializeMs: number;
	existingHeadLookupMs: number;
	sharedAppendMs: number;
	sharedProcessLocalAppendMs: number;
	sharedProcessLocalAppendBatchMs: number;
	sharedPlanEntryLeadersMs: number;
	sharedLeaderContextMs: number;
	sharedCoordinatePrepareMs: number;
	sharedPersistCoordinateMs: number;
	sharedNativeBackboneStorageTransactionMs: number;
	nativeBackbonePrepareStorageAppendMs: number;
	logAppendMs: number;
	logAppendNativeCommitOnlyMs: number;
	logGetNextsForAppendMs: number;
	logCreateNativeAppendChainMs: number;
	logPutNativeCommittedAppendMs: number;
	logPutAppendEntriesMs: number;
	logTrimMs: number;
	logTrimUnfilteredLengthMs: number;
	logConsumeNativeTrimmedEntriesMs: number;
	remoteBlockPutKnownMs: number;
	documentIndexPutMs: number;
	documentIndexTransformMs: number;
	documentBackendIndexPutMs: number;
	totalPutMs: number;
};

type BenchRow = Profile & {
	name: string;
	iterations: number;
	payloadBytes: number;
	opsPerSecond: number;
	cleanupMs: number;
};

const deepProfileKeys = new Set<keyof Profile>([
	"sharedProcessLocalAppendMs",
	"sharedProcessLocalAppendBatchMs",
	"sharedPlanEntryLeadersMs",
	"sharedLeaderContextMs",
	"sharedCoordinatePrepareMs",
	"sharedPersistCoordinateMs",
	"sharedNativeBackboneStorageTransactionMs",
	"nativeBackbonePrepareStorageAppendMs",
	"logAppendNativeCommitOnlyMs",
	"logGetNextsForAppendMs",
	"logCreateNativeAppendChainMs",
	"logPutNativeCommittedAppendMs",
	"logPutAppendEntriesMs",
	"logTrimMs",
	"logTrimUnfilteredLengthMs",
	"logConsumeNativeTrimmedEntriesMs",
	"remoteBlockPutKnownMs",
]);

const emptyProfile = (): Profile => ({
	serializeMs: 0,
	existingHeadLookupMs: 0,
	sharedAppendMs: 0,
	sharedProcessLocalAppendMs: 0,
	sharedProcessLocalAppendBatchMs: 0,
	sharedPlanEntryLeadersMs: 0,
	sharedLeaderContextMs: 0,
	sharedCoordinatePrepareMs: 0,
	sharedPersistCoordinateMs: 0,
	sharedNativeBackboneStorageTransactionMs: 0,
	nativeBackbonePrepareStorageAppendMs: 0,
	logAppendMs: 0,
	logAppendNativeCommitOnlyMs: 0,
	logGetNextsForAppendMs: 0,
	logCreateNativeAppendChainMs: 0,
	logPutNativeCommittedAppendMs: 0,
	logPutAppendEntriesMs: 0,
	logTrimMs: 0,
	logTrimUnfilteredLengthMs: 0,
	logConsumeNativeTrimmedEntriesMs: 0,
	remoteBlockPutKnownMs: 0,
	documentIndexPutMs: 0,
	documentIndexTransformMs: 0,
	documentBackendIndexPutMs: 0,
	totalPutMs: 0,
});

const payload = new Uint8Array(payloadBytes);
for (let i = 0; i < payload.length; i++) {
	payload[i] = i % 256;
}

const fromHex = (hex: string) =>
	Uint8Array.from(
		hex.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? [],
	);

const nativeBackbonePrivateKey = fromHex(
	"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
);
const nativeBackbonePublicKey = fromHex(
	"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
);

let idCounter = 0;
const createDocument = () =>
	new Document({
		id: String(idCounter++),
		name: "hello",
		number: 1n,
		bytes: payload,
		signer: currentSignerFieldBytes,
	});

const time = async <T>(
	profile: Profile,
	key: keyof Profile,
	fn: () => Promise<T>,
): Promise<T> => {
	const started = performance.now();
	try {
		return await fn();
	} finally {
		profile[key] += performance.now() - started;
	}
};

const isPromiseLike = <T>(value: T | Promise<T>): value is Promise<T> =>
	!!value && typeof (value as { then?: unknown }).then === "function";

const patchAsyncMethod = (
	target: any,
	key: string,
	profile: Profile,
	profileKey: keyof Profile,
) => {
	const original = target[key];
	if (typeof original !== "function") {
		return () => {};
	}
	target[key] = function patched(this: unknown, ...args: unknown[]) {
		const started = performance.now();
		try {
			const result = original.apply(this, args);
			if (isPromiseLike(result)) {
				return result.finally(() => {
					profile[profileKey] += performance.now() - started;
				});
			}
			profile[profileKey] += performance.now() - started;
			return result;
		} catch (error) {
			profile[profileKey] += performance.now() - started;
			throw error;
		}
	};
	return () => {
		target[key] = original;
	};
};

const timeSync = <T>(profile: Profile, key: keyof Profile, fn: () => T): T => {
	const started = performance.now();
	try {
		return fn();
	} finally {
		profile[key] += performance.now() - started;
	}
};

const patchSyncMethod = (
	target: any,
	key: string,
	profile: Profile,
	profileKey: keyof Profile,
) => {
	const original = target[key];
	if (typeof original !== "function") {
		return () => {};
	}
	target[key] = function patched(this: unknown, ...args: unknown[]) {
		return timeSync(profile, profileKey, () => original.apply(this, args));
	};
	return () => {
		target[key] = original;
	};
};

const createNodeCoordinatePersistence = async (buffered: boolean) => {
	const [{ mkdtemp, rm }, { tmpdir }, { join }] = await Promise.all([
		import("node:fs/promises"),
		import("node:os"),
		import("node:path"),
	]);
	const directory = await mkdtemp(
		join(tmpdir(), "peerbit-doc-coordinate-wal-"),
	);
	const persistence = new NativeBackboneNodeCoordinatePersistence(directory, {
		flushOnAppend: !buffered,
		...(buffered ? { flushMaxPendingBytes: coordinateWalFlushBytes } : {}),
		...(buffered ? { writeBufferMaxBytes: coordinateWalFlushBytes } : {}),
		...(buffered && coordinateWalFlushIntervalMs != null
			? { flushIntervalMs: coordinateWalFlushIntervalMs }
			: {}),
	});
	return {
		persistence,
		cleanup: async () => {
			await persistence.close();
			await rm(directory, { recursive: true, force: true });
		},
	};
};

const openScenario = async (name: string) => {
	const baseName = scenarioBaseName(name);
	const rustOptions =
		baseName === "native-block-store" ||
		baseName === "rust-peerbit" ||
		baseName === "rust-peerbit-transient-index" ||
		baseName === "rust-peerbit-backbone"
			? createRustPeerbitOptions()
			: undefined;
	const session = await TestSession.connected(1, {
		...(rustOptions ? { storage: rustOptions.storage } : {}),
		indexer:
			baseName === "simple-index"
				? createSimpleIndexer
				: baseName === "sqlite-index"
					? createSqliteIndexer
					: baseName === "rust-peerbit"
						? rustOptions?.indexer
						: baseName === "rust-peerbit-transient-index"
							? () => rustOptions!.indexer(undefined)
							: baseName === "rust-peerbit-backbone"
								? () => rustOptions!.indexer(undefined)
								: undefined,
	});
	const coordinateWal =
		baseName === "rust-peerbit-backbone" && scenarioUsesCoordinateWal(name)
			? await createNodeCoordinatePersistence(
					scenarioUsesBufferedCoordinateWal(name),
				)
			: undefined;
	const store = new TestStore({
		docs: new Documents<Document>(),
	});
	const client: ProgramClient = session.peers[0];
	currentSignerFieldBytes = scenarioUsesPolicyPutSignedField(name)
		? session.peers[0].identity.publicKey.bytes
		: undefined;
	try {
		await client.open(store, {
			args: {
				replicate: scenarioUsesLocalStore(name) ? false : { factor: 1 },
				...(scenarioUsesPolicyAllowAll(name)
					? { canPerform: policy.allowAll<Document>() }
					: scenarioUsesPolicySignedPublicKey(name)
						? {
								canPerform: policy.signedByPublicKey<Document>(
									session.peers[0].identity.publicKey,
								),
							}
						: scenarioUsesPolicyPutSignedPublicKey(name)
							? {
									canPerform: policy.put(
										policy.signedByPublicKey<Document>(
											session.peers[0].identity.publicKey,
										),
									),
								}
							: scenarioUsesPolicyPutSignedField(name)
								? {
										canPerform: policy.put(
											policy.signedByField<Document>("signer"),
										),
									}
								: scenarioUsesCanPerformAllowAll(name)
									? { canPerform: () => true }
									: {}),
				nativeGraph:
					baseName === "native-graph" ||
					baseName === "rust-peerbit" ||
					baseName === "rust-peerbit-transient-index" ||
					baseName === "rust-peerbit-backbone",
				...(baseName === "rust-peerbit-backbone"
					? {
							nativeBackbone: {
								optional: false,
								...(coordinateWal
									? { coordinatePersistence: coordinateWal.persistence }
									: {}),
							},
						}
					: {}),
				...(scenarioUsesLocalStore(name) && !scenarioUsesTrim(name)
					? {}
					: {
							log: {
								trim: { type: "length" as const, to: 100 },
							},
						}),
			},
		});
		return { session, store, cleanup: coordinateWal?.cleanup };
	} catch (error) {
		await coordinateWal?.cleanup();
		await session.stop();
		throw error;
	}
};

const runPuts = async (
	store: TestStore,
	count: number,
	scenario: string,
	profile?: Profile,
) => {
	const canAppend = () => true;
	const appendOptions = {
		replicate: false,
		target: "none" as const,
		...(scenarioUsesUniquePuts(scenario) ? { unique: true } : {}),
		...(scenarioBaseName(scenario) === "compat-path" ? { canAppend } : {}),
	};
	if (scenarioUsesPutMany(scenario)) {
		const docs = Array.from({ length: count }, () => createDocument());
		if (profile) {
			await time(profile, "totalPutMs", () =>
				store.docs.putMany(docs, appendOptions),
			);
		} else {
			await store.docs.putMany(docs, appendOptions);
		}
		return;
	}
	for (let i = 0; i < count; i++) {
		const doc = createDocument();
		if (profile) {
			await time(profile, "totalPutMs", () =>
				store.docs.put(doc, appendOptions),
			);
		} else {
			await store.docs.put(doc, appendOptions);
		}
	}
};

const runScenario = async (name: string): Promise<BenchRow> => {
	const { session, store, cleanup } = await openScenario(name);
	let row: BenchRow | undefined;
	try {
		await runPuts(store, warmupIterations, name);

		const profile = emptyProfile();
		const backendIndex = store.docs.index.index as any;
		const restores = [
			patchAsyncMethod(
				store.docs as any,
				"getLocalIndexedContext",
				profile,
				"existingHeadLookupMs",
			),
			patchAsyncMethod(store.docs.log, "append", profile, "sharedAppendMs"),
			patchAsyncMethod(
				store.docs.log,
				"appendLocallyValidated",
				profile,
				"sharedAppendMs",
			),
			patchAsyncMethod(
				store.docs.log,
				"appendLocallyPrepared",
				profile,
				"sharedAppendMs",
			),
			patchAsyncMethod(
				store.docs.log as any,
				"appendLocallyPreparedPayloadCommitOnly",
				profile,
				"sharedAppendMs",
			),
			patchAsyncMethod(
				store.docs.log as any,
				"appendLocallyPreparedManyIndependent",
				profile,
				"sharedAppendMs",
			),
			patchAsyncMethod(store.docs.log.log, "append", profile, "logAppendMs"),
			patchAsyncMethod(
				store.docs.log.log as any,
				"appendLocallyPrepared",
				profile,
				"logAppendMs",
			),
			patchAsyncMethod(
				store.docs.log.log as any,
				"appendLocallyPreparedCommitOnly",
				profile,
				"logAppendMs",
			),
			patchAsyncMethod(
				store.docs.log.log as any,
				"appendLocallyPreparedNativeNoNextCommitOnly",
				profile,
				"logAppendMs",
			),
			patchAsyncMethod(
				store.docs.log.log as any,
				"appendLocallyPreparedManyIndependent",
				profile,
				"logAppendMs",
			),
			patchAsyncMethod(
				store.docs.index,
				"transformer",
				profile,
				"documentIndexTransformMs",
			),
			patchAsyncMethod(
				backendIndex,
				typeof backendIndex.putWithContext === "function"
					? "putWithContext"
					: "put",
				profile,
				"documentBackendIndexPutMs",
			),
			patchAsyncMethod(
				backendIndex,
				typeof backendIndex.putWithContextBatch === "function"
					? "putWithContextBatch"
					: "putBatch",
				profile,
				"documentBackendIndexPutMs",
			),
			patchAsyncMethod(
				backendIndex,
				"putStoredContextualEncodedValue",
				profile,
				"documentBackendIndexPutMs",
			),
			patchAsyncMethod(
				store.docs.index,
				"putWithContext",
				profile,
				"documentIndexPutMs",
			),
			patchAsyncMethod(
				store.docs.index,
				"_putStoredIdentityWithContext",
				profile,
				"documentIndexPutMs",
			),
			patchAsyncMethod(
				store.docs.index,
				"_putIdentityWithContext",
				profile,
				"documentIndexPutMs",
			),
			patchAsyncMethod(
				store.docs.index,
				"putManyWithContext",
				profile,
				"documentIndexPutMs",
			),
			patchAsyncMethod(
				store.docs.index,
				"_putManyIdentityWithContext",
				profile,
				"documentIndexPutMs",
			),
		];
		if (profileDeep) {
			restores.push(
				patchAsyncMethod(
					store.docs.log as any,
					"processLocalAppend",
					profile,
					"sharedProcessLocalAppendMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"processNativePreparedTargetNoneAppendTransaction",
					profile,
					"sharedProcessLocalAppendMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"processLocalAppendManyNativePlanned",
					profile,
					"sharedProcessLocalAppendBatchMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"planEntryLeaders",
					profile,
					"sharedPlanEntryLeadersMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"createLeaderSelectionContext",
					profile,
					"sharedLeaderContextMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"planNativeLocalAppendEntry",
					profile,
					"sharedPlanEntryLeadersMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"planNativeLocalAppendFacts",
					profile,
					"sharedPlanEntryLeadersMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"planNativeAppendEntry",
					profile,
					"sharedPlanEntryLeadersMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"planNativeAppendFacts",
					profile,
					"sharedPlanEntryLeadersMs",
				),
				patchSyncMethod(
					store.docs.log as any,
					"createCoordinatePersistenceEntryFromNativePlanFacts",
					profile,
					"sharedCoordinatePrepareMs",
				),
				patchSyncMethod(
					store.docs.log as any,
					"createCoordinatePersistenceEntryFromNativePlan",
					profile,
					"sharedCoordinatePrepareMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"persistCoordinate",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"persistPreparedCoordinate",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"persistPreparedCoordinateNativeTransaction",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"persistPreparedBackboneCoordinateNativeTransaction",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"persistCoordinatesBatch",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"appendLocallyPreparedPayloadNativeBackboneStorageTransaction",
					profile,
					"sharedNativeBackboneStorageTransactionMs",
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainStorageAppendTransaction",
					profile,
					"nativeBackbonePrepareStorageAppendMs",
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainNoNextStorageAppendTransaction",
					profile,
					"nativeBackbonePrepareStorageAppendMs",
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainCommittedStorageAppendTransaction",
					profile,
					"nativeBackbonePrepareStorageAppendMs",
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainCommittedNoNextStorageAppendTransaction",
					profile,
					"nativeBackbonePrepareStorageAppendMs",
				),
				patchAsyncMethod(
					(store.docs.log as any).remoteBlocks ?? {},
					"putKnown",
					profile,
					"remoteBlockPutKnownMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"getNextsForAppend",
					profile,
					"logGetNextsForAppendMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"appendLocallyPreparedNativeCommitOnly",
					profile,
					"logAppendNativeCommitOnlyMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"createNativePlainAppendChain",
					profile,
					"logCreateNativeAppendChainMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"createNativePlainAppendCommitOnly",
					profile,
					"logCreateNativeAppendChainMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"createNativePlainAppendEntriesBatch",
					profile,
					"logCreateNativeAppendChainMs",
				),
				patchAsyncMethod(
					store.docs.log.log.entryIndex as any,
					"putNativeCommittedAppend",
					profile,
					"logPutNativeCommittedAppendMs",
				),
				patchAsyncMethod(
					store.docs.log.log.entryIndex as any,
					"putNativeCommittedAppendFacts",
					profile,
					"logPutNativeCommittedAppendMs",
				),
				patchAsyncMethod(
					store.docs.log.log.entryIndex as any,
					"consumeNativeTrimmedEntriesMaybe",
					profile,
					"logConsumeNativeTrimmedEntriesMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"putAppendEntries",
					profile,
					"logPutAppendEntriesMs",
				),
				patchAsyncMethod(store.docs.log.log, "trim", profile, "logTrimMs"),
				patchAsyncMethod(
					(store.docs.log.log as any)._trim,
					"trimUnfilteredLength",
					profile,
					"logTrimUnfilteredLengthMs",
				),
			);
		}

		const serializeStarted = performance.now();
		for (let i = 0; i < iterations; i++) {
			serialize(createDocument());
		}
		profile.serializeMs = performance.now() - serializeStarted;

		try {
			await runPuts(store, iterations, name, profile);
		} finally {
			for (const restore of restores.reverse()) {
				restore();
			}
		}

		row = {
			name,
			iterations,
			payloadBytes,
			opsPerSecond: Math.round((iterations / profile.totalPutMs) * 1000),
			cleanupMs: 0,
			...Object.fromEntries(
				Object.entries(profile)
					.filter(
						([key]) =>
							profileDeep || !deepProfileKeys.has(key as keyof Profile),
					)
					.map(([key, value]) => [key, Math.round(value * 100) / 100]),
			),
		} as BenchRow;
	} finally {
		const cleanupStarted = performance.now();
		try {
			try {
				await store.drop();
			} finally {
				try {
					await session.stop();
				} finally {
					await cleanup?.();
				}
			}
		} finally {
			if (row) {
				row.cleanupMs =
					Math.round((performance.now() - cleanupStarted) * 100) / 100;
			}
		}
	}
	if (!row) {
		throw new Error(`Benchmark scenario ${name} did not produce a row`);
	}
	return row;
};

const runNativeCeilingScenario = async (name: string): Promise<BenchRow> => {
	const session = await TestSession.disconnected(1);
	const rustOptions = createRustPeerbitOptions({
		storage: { nativeLogBlocks: true },
	});
	const blockStore = rustOptions.storage.blocksStoreFactory!() as any;
	blockStore.rm ??= (key: string) => blockStore.del(key);
	blockStore.rmMany ??= async (keys: string[]) => {
		await Promise.all(keys.map((key) => blockStore.rm(key)));
		return keys.length;
	};
	await blockStore.open?.();
	const log = new Log<Uint8Array>();
	await log.open(blockStore as any, session.peers[0].identity, {
		nativeGraph: true,
		encoding: {
			encoder: (value) => value,
			decoder: (bytes) => bytes,
		},
		trim: { type: "length", to: 100 },
	});

	const append = async (count: number, profile?: Profile) => {
		for (let i = 0; i < count; i++) {
			const runAppend = () =>
				log.appendLocallyPreparedCommitOnly(
					undefined as any,
					{ meta: { next: [] } as any },
					{
						payloadData: payload,
						includeMaterializationBytes: false,
						includeAppendFactsBytes: true,
						resolveTrimmedEntries: false,
						skipMissingNextJoin: true,
					},
				);
			if (profile) {
				await time(profile, "totalPutMs", async () => {
					await runAppend();
				});
			} else {
				await runAppend();
			}
		}
	};

	try {
		await append(warmupIterations);
		const profile = emptyProfile();
		const restores = [
			patchAsyncMethod(
				log as any,
				"appendLocallyPreparedCommitOnly",
				profile,
				"logAppendMs",
			),
			patchAsyncMethod(
				log as any,
				"getNextsForAppend",
				profile,
				"logGetNextsForAppendMs",
			),
			patchAsyncMethod(
				log as any,
				"createNativePlainAppendCommitOnly",
				profile,
				"logCreateNativeAppendChainMs",
			),
			patchAsyncMethod(
				log.entryIndex as any,
				"putNativeCommittedAppendFacts",
				profile,
				"logPutNativeCommittedAppendMs",
			),
			patchAsyncMethod(log, "trim", profile, "logTrimMs"),
			patchAsyncMethod(
				(log as any)._trim,
				"trimUnfilteredLength",
				profile,
				"logTrimUnfilteredLengthMs",
			),
		];
		try {
			await append(iterations, profile);
		} finally {
			for (const restore of restores.reverse()) {
				restore();
			}
		}
		return {
			name,
			iterations,
			payloadBytes,
			opsPerSecond: Math.round((iterations / profile.totalPutMs) * 1000),
			cleanupMs: 0,
			...Object.fromEntries(
				Object.entries(profile)
					.filter(
						([key]) =>
							profileDeep || !deepProfileKeys.has(key as keyof Profile),
					)
					.map(([key, value]) => [key, Math.round(value * 100) / 100]),
			),
		} as BenchRow;
	} finally {
		await log.close();
		await blockStore.close?.();
		await session.stop();
	}
};

const runNativeBackboneCeilingScenario = async (
	name: string,
): Promise<BenchRow> => {
	const backbone = await createNativePeerbitBackbone({
		clockId: nativeBackbonePublicKey,
		privateKey: nativeBackbonePrivateKey,
		publicKey: nativeBackbonePublicKey,
	});

	const append = (count: number, profile?: Profile) => {
		for (let i = 0; i < count; i++) {
			const runAppend = () =>
				backbone.appendPlainNoNextTransaction({
					wallTime: BigInt(Date.now()),
					logical: i,
					gid: `gid-${i}`,
					payloadData: payload,
					replicas: 1,
					selfHash: "native-backbone-ceiling-peer",
					trimLengthTo: 100,
				});
			if (profile) {
				timeSync(profile, "totalPutMs", runAppend);
			} else {
				runAppend();
			}
		}
	};

	append(warmupIterations);
	const profile = emptyProfile();
	append(iterations, profile);

	return {
		name,
		iterations,
		payloadBytes,
		opsPerSecond: Math.round((iterations / profile.totalPutMs) * 1000),
		cleanupMs: 0,
		...Object.fromEntries(
			Object.entries(profile)
				.filter(
					([key]) => profileDeep || !deepProfileKeys.has(key as keyof Profile),
				)
				.map(([key, value]) => [key, Math.round(value * 100) / 100]),
		),
	} as BenchRow;
};

const rows: BenchRow[] = [];
for (const name of scenarioNames) {
	const baseName = scenarioBaseName(name);
	rows.push(
		baseName === "native-ceiling"
			? await runNativeCeilingScenario(name)
			: baseName === "native-backbone-ceiling"
				? await runNativeBackboneCeilingScenario(name)
				: await runScenario(name),
	);
}

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(
		JSON.stringify(
			{
				name: "document-put",
				rows,
				meta: {
					payloadBytes,
					warmupIterations,
					iterations,
					profileDeep,
					coordinateWalFlushBytes,
					coordinateWalFlushIntervalMs,
				},
			},
			null,
			2,
		),
	);
} else {
	console.table(rows);
}

process.exit(process.exitCode ?? 0);
