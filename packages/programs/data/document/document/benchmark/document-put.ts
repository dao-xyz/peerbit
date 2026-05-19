import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { create as createSqliteIndexer } from "@peerbit/indexer-sqlite3";
import { Log } from "@peerbit/log";
import {
	NativeBackboneNodeCoordinatePersistenceStore,
	createNativePeerbitBackbone,
	defaultNativeBackboneCoordinateFlushMaxPendingBytes,
} from "@peerbit/native-backbone";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { createRustPeerbitOptions } from "peerbit/rust";
import {
	Documents,
	type SetupOptions,
	policy,
	transform,
} from "../src/index.js";

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
// - DOC_SCENARIOS=compat-path,hybrid-anystore,simple-index,sqlite-index,native-graph,native-block-store,rust-peerbit,rust-peerbit-local,rust-peerbit-transient-index,rust-peerbit-backbone-local,rust-peerbit-backbone-local-document-index,rust-peerbit-backbone-coordinate-wal,rust-peerbit-backbone-coordinate-wal-buffered,native-ceiling,native-backbone-ceiling,native-backbone-storage-ceiling
//   Add "-nonunique" to any scenario name to use default update-safe put semantics with new ids.
//   Add "-update" to any scenario name to repeatedly update one document id.
//   Add "-local" to a rust-peerbit scenario to disable replication and default trim.
//   Add "-trim" to a local rust-peerbit scenario to keep length trim enabled.
//   Add "-no-trim" to any rust-peerbit scenario to disable length trim.
//   Add "-putmany" to any unique scenario name to use one putMany call per measured batch.
//   Add "-document-index" to a rust-peerbit-backbone scenario to enable nativeBackbone.documentIndex.
//   Add "-policy-allow-all" to open with canPerform: policy.allowAll().
//   Add "-policy-signed-public-key" to open with canPerform: policy.signedByPublicKey(local public key).
//   Add "-policy-put-signed-public-key" to open with canPerform: policy.put(policy.signedByPublicKey(local public key)).
//   Add "-policy-put-signed-field" to open with canPerform: policy.put(policy.signedByField("signer")).
//   Add "-canperform-allow-all" to open with canPerform: () => true.
//   Add "-transform-identity", "-transform-pick", "-transform-project-context", or "-transform-arbitrary" to compare index transform paths.
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
		/(?:-(?:putmany|nonunique|update|local|no-trim|trim|buffered|coordinate-wal|document-index|policy-allow-all|policy-signed-public-key|policy-put-signed-public-key|policy-put-signed-field|canperform-allow-all|transform-identity|transform-pick|transform-project-context|transform-arbitrary))*$/,
		"",
	);
const scenarioUsesUpdatePuts = (name: string) => name.includes("-update");
const scenarioUsesUniquePuts = (name: string) =>
	!name.includes("-nonunique") && !scenarioUsesUpdatePuts(name);
const scenarioUsesPutMany = (name: string) => name.endsWith("-putmany");
const scenarioUsesLocalStore = (name: string) =>
	scenarioBaseName(name).startsWith("rust-peerbit") && name.includes("-local");
const scenarioDisablesTrim = (name: string) => name.includes("-no-trim");
const scenarioUsesTrim = (name: string) =>
	name.includes("-trim") && !scenarioDisablesTrim(name);
const scenarioUsesCoordinateWal = (name: string) =>
	name.includes("-coordinate-wal");
const scenarioUsesBufferedCoordinateWal = (name: string) =>
	name.includes("-coordinate-wal-buffered");
const scenarioUsesNativeBackboneDocumentIndex = (name: string) =>
	name.includes("-document-index");
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
const scenarioUsesTransformIdentity = (name: string) =>
	name.includes("-transform-identity");
const scenarioUsesTransformPick = (name: string) =>
	name.includes("-transform-pick");
const scenarioUsesTransformProjectContext = (name: string) =>
	name.includes("-transform-project-context");
const scenarioUsesTransformArbitrary = (name: string) =>
	name.includes("-transform-arbitrary");
const profileDeep = process.env.DOC_PROFILE_DEEP === "1";
const profileNativeBackbone = process.env.DOC_NATIVE_PROFILE === "1";

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
class TestStore extends Program<Partial<SetupOptions<Document, any>>> {
	@field({ type: Documents })
	docs: Documents<Document, any>;

	constructor(properties?: { docs: Documents<Document, any> }) {
		super();
		this.docs = properties?.docs ?? new Documents<Document, any>();
	}

	async open(options?: Partial<SetupOptions<Document, any>>): Promise<void> {
		await this.docs.open({ ...options, type: Document });
	}
}

@variant("document_put_bench_pick_indexable")
class PickIndexable {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	constructor(properties?: Partial<PickIndexable>) {
		this.id = properties?.id || "";
		this.name = properties?.name;
	}
}

@variant("document_put_bench_project_indexable")
class ProjectIndexable {
	@field({ type: "string" })
	id: string;

	@field({ type: "u64" })
	created: bigint;

	@field({ type: option(Uint8Array) })
	signer?: Uint8Array;

	constructor(properties?: Partial<ProjectIndexable>) {
		this.id = properties?.id || "";
		this.created = properties?.created || 0n;
		this.signer = properties?.signer;
	}
}

type Profile = {
	serializeMs: number;
	existingHeadLookupMs: number;
	documentCommitPlainPutPlanMs: number;
	documentCommitNativeAppendMs: number;
	documentCreateAppendCommitFactsMs: number;
	documentHandlePreparedCommitMs: number;
	sharedAppendMs: number;
	sharedProcessLocalAppendMs: number;
	sharedProcessLocalAppendBatchMs: number;
	sharedPlanEntryLeadersMs: number;
	sharedLeaderContextMs: number;
	sharedCoordinatePrepareMs: number;
	sharedPersistCoordinateMs: number;
	sharedApplyPreparedFactsMs: number;
	sharedCreateAppendCommitMs: number;
	sharedMaterializeEntryMs: number;
	sharedCoordinateIndexPutMs: number;
	sharedNativeBackboneStorageTransactionMs: number;
	nativeBackbonePrepareStorageAppendMs: number;
	nativeBackbonePrepareNoNextStorageAppendMs: number;
	nativeBackbonePrepareStorageAppendWithNextMs: number;
	nativeBackbonePrepareCommittedNoNextStorageAppendMs: number;
	nativeBackbonePrepareCommittedStorageAppendMs: number;
	nativeBackboneStorageAppendInnerMs: number;
	nativeBackboneInputCopyMs: number;
	nativeBackboneLogTotalMs: number;
	nativeBackboneLogNextCloneMs: number;
	nativeBackboneLogEntryCoreMs: number;
	nativeBackboneLogEncodeMetaMs: number;
	nativeBackboneLogEncodePayloadMs: number;
	nativeBackboneLogEncodeSignableMs: number;
	nativeBackboneLogSignMs: number;
	nativeBackboneLogEncodeSignatureMs: number;
	nativeBackboneLogEncodeStorageMs: number;
	nativeBackboneLogCidMs: number;
	nativeBackboneLogIndexEntryMs: number;
	nativeBackboneLogFactsMs: number;
	nativeBackboneLogBlockPutMs: number;
	nativeBackboneLogGraphPutMs: number;
	nativeBackboneLogTrimMs: number;
	nativeBackboneEntryRowMs: number;
	nativeBackboneTrimRowsMs: number;
	nativeBackboneHashNumberMs: number;
	nativeBackboneCoordinatePlanMs: number;
	nativeBackboneCoordinateCoreMs: number;
	nativeBackboneCoordinateFieldsBuildMs: number;
	nativeBackboneCoordinateValueEncodeMs: number;
	nativeBackboneCoordinateJournalPutMs: number;
	nativeBackboneCoordinateIndexPutMs: number;
	nativeBackboneCoordinateValuePutMs: number;
	nativeBackboneCoordinateDeleteMs: number;
	nativeBackboneDocumentIndexCommitMs: number;
	nativeBackboneDocumentIndexContextEncodeMs: number;
	nativeBackboneDocumentIndexExtractMs: number;
	nativeBackboneDocumentIndexValueBuildMs: number;
	nativeBackboneDocumentIndexPutMs: number;
	nativeBackboneDocumentValuePutMs: number;
	nativeBackboneResultRowMs: number;
	nativeGraphPrepareEntryCommitMs: number;
	nativeSharedLogCommitCoordinatesMs: number;
	nativeBackboneCommitCoordinatesMs: number;
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
	remoteBlockNotifyStoredMs: number;
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
	"documentCommitPlainPutPlanMs",
	"documentCommitNativeAppendMs",
	"documentCreateAppendCommitFactsMs",
	"documentHandlePreparedCommitMs",
	"sharedProcessLocalAppendMs",
	"sharedProcessLocalAppendBatchMs",
	"sharedPlanEntryLeadersMs",
	"sharedLeaderContextMs",
	"sharedCoordinatePrepareMs",
	"sharedPersistCoordinateMs",
	"sharedApplyPreparedFactsMs",
	"sharedCreateAppendCommitMs",
	"sharedMaterializeEntryMs",
	"sharedCoordinateIndexPutMs",
	"sharedNativeBackboneStorageTransactionMs",
	"nativeBackbonePrepareStorageAppendMs",
	"nativeBackbonePrepareNoNextStorageAppendMs",
	"nativeBackbonePrepareStorageAppendWithNextMs",
	"nativeBackbonePrepareCommittedNoNextStorageAppendMs",
	"nativeBackbonePrepareCommittedStorageAppendMs",
	"nativeBackboneStorageAppendInnerMs",
	"nativeBackboneInputCopyMs",
	"nativeBackboneLogTotalMs",
	"nativeBackboneLogNextCloneMs",
	"nativeBackboneLogEntryCoreMs",
	"nativeBackboneLogEncodeMetaMs",
	"nativeBackboneLogEncodePayloadMs",
	"nativeBackboneLogEncodeSignableMs",
	"nativeBackboneLogSignMs",
	"nativeBackboneLogEncodeSignatureMs",
	"nativeBackboneLogEncodeStorageMs",
	"nativeBackboneLogCidMs",
	"nativeBackboneLogIndexEntryMs",
	"nativeBackboneLogFactsMs",
	"nativeBackboneLogBlockPutMs",
	"nativeBackboneLogGraphPutMs",
	"nativeBackboneLogTrimMs",
	"nativeBackboneEntryRowMs",
	"nativeBackboneTrimRowsMs",
	"nativeBackboneHashNumberMs",
	"nativeBackboneCoordinatePlanMs",
	"nativeBackboneCoordinateCoreMs",
	"nativeBackboneCoordinateFieldsBuildMs",
	"nativeBackboneCoordinateValueEncodeMs",
	"nativeBackboneCoordinateJournalPutMs",
	"nativeBackboneCoordinateIndexPutMs",
	"nativeBackboneCoordinateValuePutMs",
	"nativeBackboneCoordinateDeleteMs",
	"nativeBackboneDocumentIndexCommitMs",
	"nativeBackboneDocumentIndexContextEncodeMs",
	"nativeBackboneDocumentIndexExtractMs",
	"nativeBackboneDocumentIndexValueBuildMs",
	"nativeBackboneDocumentIndexPutMs",
	"nativeBackboneDocumentValuePutMs",
	"nativeBackboneResultRowMs",
	"nativeGraphPrepareEntryCommitMs",
	"nativeSharedLogCommitCoordinatesMs",
	"nativeBackboneCommitCoordinatesMs",
	"logAppendNativeCommitOnlyMs",
	"logGetNextsForAppendMs",
	"logCreateNativeAppendChainMs",
	"logPutNativeCommittedAppendMs",
	"logPutAppendEntriesMs",
	"logTrimMs",
	"logTrimUnfilteredLengthMs",
	"logConsumeNativeTrimmedEntriesMs",
	"remoteBlockPutKnownMs",
	"remoteBlockNotifyStoredMs",
]);

const nativeBackboneProfileKeys = new Set<keyof Profile>([
	"nativeBackboneStorageAppendInnerMs",
	"nativeBackboneInputCopyMs",
	"nativeBackboneLogTotalMs",
	"nativeBackboneLogNextCloneMs",
	"nativeBackboneLogEntryCoreMs",
	"nativeBackboneLogEncodeMetaMs",
	"nativeBackboneLogEncodePayloadMs",
	"nativeBackboneLogEncodeSignableMs",
	"nativeBackboneLogSignMs",
	"nativeBackboneLogEncodeSignatureMs",
	"nativeBackboneLogEncodeStorageMs",
	"nativeBackboneLogCidMs",
	"nativeBackboneLogIndexEntryMs",
	"nativeBackboneLogFactsMs",
	"nativeBackboneLogBlockPutMs",
	"nativeBackboneLogGraphPutMs",
	"nativeBackboneLogTrimMs",
	"nativeBackboneEntryRowMs",
	"nativeBackboneTrimRowsMs",
	"nativeBackboneHashNumberMs",
	"nativeBackboneCoordinatePlanMs",
	"nativeBackboneCoordinateCoreMs",
	"nativeBackboneCoordinateFieldsBuildMs",
	"nativeBackboneCoordinateValueEncodeMs",
	"nativeBackboneCoordinateJournalPutMs",
	"nativeBackboneCoordinateIndexPutMs",
	"nativeBackboneCoordinateValuePutMs",
	"nativeBackboneCoordinateDeleteMs",
	"nativeBackboneDocumentIndexCommitMs",
	"nativeBackboneDocumentIndexContextEncodeMs",
	"nativeBackboneDocumentIndexExtractMs",
	"nativeBackboneDocumentIndexValueBuildMs",
	"nativeBackboneDocumentIndexPutMs",
	"nativeBackboneDocumentValuePutMs",
	"nativeBackboneResultRowMs",
]);

const shouldIncludeProfileKey = (key: string): boolean => {
	const profileKey = key as keyof Profile;
	return (
		(profileDeep || !deepProfileKeys.has(profileKey)) &&
		(profileNativeBackbone || !nativeBackboneProfileKeys.has(profileKey))
	);
};

const emptyProfile = (): Profile => ({
	serializeMs: 0,
	existingHeadLookupMs: 0,
	documentCommitPlainPutPlanMs: 0,
	documentCommitNativeAppendMs: 0,
	documentCreateAppendCommitFactsMs: 0,
	documentHandlePreparedCommitMs: 0,
	sharedAppendMs: 0,
	sharedProcessLocalAppendMs: 0,
	sharedProcessLocalAppendBatchMs: 0,
	sharedPlanEntryLeadersMs: 0,
	sharedLeaderContextMs: 0,
	sharedCoordinatePrepareMs: 0,
	sharedPersistCoordinateMs: 0,
	sharedApplyPreparedFactsMs: 0,
	sharedCreateAppendCommitMs: 0,
	sharedMaterializeEntryMs: 0,
	sharedCoordinateIndexPutMs: 0,
	sharedNativeBackboneStorageTransactionMs: 0,
	nativeBackbonePrepareStorageAppendMs: 0,
	nativeBackbonePrepareNoNextStorageAppendMs: 0,
	nativeBackbonePrepareStorageAppendWithNextMs: 0,
	nativeBackbonePrepareCommittedNoNextStorageAppendMs: 0,
	nativeBackbonePrepareCommittedStorageAppendMs: 0,
	nativeBackboneStorageAppendInnerMs: 0,
	nativeBackboneInputCopyMs: 0,
	nativeBackboneLogTotalMs: 0,
	nativeBackboneLogNextCloneMs: 0,
	nativeBackboneLogEntryCoreMs: 0,
	nativeBackboneLogEncodeMetaMs: 0,
	nativeBackboneLogEncodePayloadMs: 0,
	nativeBackboneLogEncodeSignableMs: 0,
	nativeBackboneLogSignMs: 0,
	nativeBackboneLogEncodeSignatureMs: 0,
	nativeBackboneLogEncodeStorageMs: 0,
	nativeBackboneLogCidMs: 0,
	nativeBackboneLogIndexEntryMs: 0,
	nativeBackboneLogFactsMs: 0,
	nativeBackboneLogBlockPutMs: 0,
	nativeBackboneLogGraphPutMs: 0,
	nativeBackboneLogTrimMs: 0,
	nativeBackboneEntryRowMs: 0,
	nativeBackboneTrimRowsMs: 0,
	nativeBackboneHashNumberMs: 0,
	nativeBackboneCoordinatePlanMs: 0,
	nativeBackboneCoordinateCoreMs: 0,
	nativeBackboneCoordinateFieldsBuildMs: 0,
	nativeBackboneCoordinateValueEncodeMs: 0,
	nativeBackboneCoordinateJournalPutMs: 0,
	nativeBackboneCoordinateIndexPutMs: 0,
	nativeBackboneCoordinateValuePutMs: 0,
	nativeBackboneCoordinateDeleteMs: 0,
	nativeBackboneDocumentIndexCommitMs: 0,
	nativeBackboneDocumentIndexContextEncodeMs: 0,
	nativeBackboneDocumentIndexExtractMs: 0,
	nativeBackboneDocumentIndexValueBuildMs: 0,
	nativeBackboneDocumentIndexPutMs: 0,
	nativeBackboneDocumentValuePutMs: 0,
	nativeBackboneResultRowMs: 0,
	nativeGraphPrepareEntryCommitMs: 0,
	nativeSharedLogCommitCoordinatesMs: 0,
	nativeBackboneCommitCoordinatesMs: 0,
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
	remoteBlockNotifyStoredMs: 0,
	documentIndexPutMs: 0,
	documentIndexTransformMs: 0,
	documentBackendIndexPutMs: 0,
	totalPutMs: 0,
});

const payload = new Uint8Array(payloadBytes);
for (let i = 0; i < payload.length; i++) {
	payload[i] = i % 256;
}

const writeU32 = (out: number[], value: number) => {
	out.push(
		value & 0xff,
		(value >> 8) & 0xff,
		(value >> 16) & 0xff,
		value >>> 24,
	);
};

const writeString = (out: number[], value: string) => {
	const bytes = new TextEncoder().encode(value);
	writeU32(out, bytes.byteLength);
	out.push(...bytes);
};

const nativeCeilingContextSchemaIr = () => {
	const out: number[] = [1, 14];
	writeU32(out, 1);
	out.push(0);
	writeU32(out, 5);
	writeString(out, "created");
	writeU32(out, 1);
	writeU32(out, 101);
	out.push(4);
	writeString(out, "modified");
	writeU32(out, 2);
	writeU32(out, 102);
	out.push(4);
	writeString(out, "head");
	writeU32(out, 3);
	writeU32(out, 103);
	out.push(12);
	writeString(out, "gid");
	writeU32(out, 4);
	writeU32(out, 104);
	out.push(12);
	writeString(out, "size");
	writeU32(out, 5);
	writeU32(out, 105);
	out.push(3);
	return Uint8Array.from(out);
};

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

type ProfileKey = keyof Profile | readonly (keyof Profile)[];

const addProfileTime = (
	profile: Profile,
	profileKey: ProfileKey,
	durationMs: number,
) => {
	const keys: readonly (keyof Profile)[] =
		typeof profileKey === "string" ? [profileKey] : profileKey;
	for (const key of keys) {
		profile[key] += durationMs;
	}
};

const patchAsyncMethod = (
	target: any,
	key: string,
	profile: Profile,
	profileKey: ProfileKey,
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
					addProfileTime(profile, profileKey, performance.now() - started);
				});
			}
			addProfileTime(profile, profileKey, performance.now() - started);
			return result;
		} catch (error) {
			addProfileTime(profile, profileKey, performance.now() - started);
			throw error;
		}
	};
	return () => {
		target[key] = original;
	};
};

const timeSync = <T>(profile: Profile, key: ProfileKey, fn: () => T): T => {
	const started = performance.now();
	try {
		return fn();
	} finally {
		addProfileTime(profile, key, performance.now() - started);
	}
};

const patchSyncMethod = (
	target: any,
	key: string,
	profile: Profile,
	profileKey: ProfileKey,
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
	const store = new NativeBackboneNodeCoordinatePersistenceStore(directory);
	const persistence = {
		store,
		flushOnAppend: !buffered,
		...(buffered ? { flushMaxPendingBytes: coordinateWalFlushBytes } : {}),
		...(buffered
			? { buffered: { maxBufferedBytes: coordinateWalFlushBytes } }
			: {}),
		...(buffered && coordinateWalFlushIntervalMs != null
			? { flushIntervalMs: coordinateWalFlushIntervalMs }
			: {}),
	};
	return {
		persistence,
		cleanup: async () => {
			await store.close();
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
	const indexOptions = scenarioUsesTransformIdentity(name)
		? { transform: transform.identity<Document>() }
		: scenarioUsesTransformPick(name)
			? {
					type: PickIndexable,
					transform: transform.pick<Document, PickIndexable>(["id", "name"]),
				}
			: scenarioUsesTransformProjectContext(name)
				? {
						type: ProjectIndexable,
						transform: transform.project<Document, ProjectIndexable>({
							id: transform.field("id"),
							created: transform.context("created"),
							signer: transform.field("signer"),
						}),
					}
				: scenarioUsesTransformArbitrary(name)
					? {
							type: PickIndexable,
							transform: (document: Document) =>
								new PickIndexable({
									id: document.id,
									name: document.name,
								}),
						}
					: undefined;
	try {
		await client.open(store, {
			args: {
				replicate: scenarioUsesLocalStore(name) ? false : { factor: 1 },
				...(indexOptions ? { index: indexOptions } : {}),
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
								...(scenarioUsesNativeBackboneDocumentIndex(name)
									? { documentIndex: true }
									: {}),
								...(coordinateWal
									? { coordinatePersistence: coordinateWal.persistence }
									: {}),
							},
						}
					: {}),
				...(scenarioDisablesTrim(name) ||
				(scenarioUsesLocalStore(name) && !scenarioUsesTrim(name))
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
	if (scenarioUsesUpdatePuts(scenario)) {
		const id = String(idCounter++);
		for (let i = 0; i < count; i++) {
			const doc = new Document({
				id,
				name: `hello-${i}`,
				number: BigInt(i),
				bytes: payload,
				signer: currentSignerFieldBytes,
			});
			if (profile) {
				await time(profile, "totalPutMs", () =>
					store.docs.put(doc, appendOptions),
				);
			} else {
				await store.docs.put(doc, appendOptions);
			}
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
			patchAsyncMethod(
				store.docs as any,
				"commitPlainPutPlan",
				profile,
				"documentCommitPlainPutPlanMs",
			),
			patchAsyncMethod(
				store.docs as any,
				"commitNativeDocumentAppend",
				profile,
				"documentCommitNativeAppendMs",
			),
			patchAsyncMethod(
				store.docs as any,
				"createDocumentAppendCommitFacts",
				profile,
				"documentCreateAppendCommitFactsMs",
			),
			patchAsyncMethod(
				store.docs as any,
				"handlePreparedPlainPutCommit",
				profile,
				"documentHandlePreparedCommitMs",
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
				"_putPreparedNativeBackboneDocumentIndexWithContext",
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
					"persistBackboneCoordinateFieldsNativeTransaction",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log as any,
					"persistCoordinatesBatch",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchSyncMethod(
					store.docs.log as any,
					"applyPreparedAppendFactsWithDeferredCoordinateDeletes",
					profile,
					"sharedApplyPreparedFactsMs",
				),
				patchSyncMethod(
					store.docs.log as any,
					"createPreparedLocalAppendCommitFromFacts",
					profile,
					"sharedCreateAppendCommitMs",
				),
				patchSyncMethod(
					store.docs.log as any,
					"materializePreparedAppendResultEntry",
					profile,
					"sharedMaterializeEntryMs",
				),
				patchAsyncMethod(
					(store.docs.log as any).entryCoordinatesIndex ?? {},
					"putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn",
					profile,
					"sharedCoordinateIndexPutMs",
				),
				patchAsyncMethod(
					(store.docs.log as any).entryCoordinatesIndex ?? {},
					"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
					profile,
					"sharedCoordinateIndexPutMs",
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
					[
						"nativeBackbonePrepareStorageAppendMs",
						"nativeBackbonePrepareStorageAppendWithNextMs",
					],
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainNoNextStorageAppendTransaction",
					profile,
					[
						"nativeBackbonePrepareStorageAppendMs",
						"nativeBackbonePrepareNoNextStorageAppendMs",
					],
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainCommittedStorageAppendTransaction",
					profile,
					[
						"nativeBackbonePrepareStorageAppendMs",
						"nativeBackbonePrepareCommittedStorageAppendMs",
					],
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"preparePlainCommittedNoNextStorageAppendTransaction",
					profile,
					[
						"nativeBackbonePrepareStorageAppendMs",
						"nativeBackbonePrepareCommittedNoNextStorageAppendMs",
					],
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone?.graph ?? {},
					"prepareEntryV0PlainEntryCommit",
					profile,
					"nativeGraphPrepareEntryCommitMs",
				),
				patchSyncMethod(
					(store.docs.log.log.entryIndex as any).properties?.nativeGraph
						?.graph ?? {},
					"prepareEntryV0PlainEntryCommit",
					profile,
					"nativeGraphPrepareEntryCommitMs",
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeSharedLogState ?? {},
					"commitEntryCoordinates",
					profile,
					"nativeSharedLogCommitCoordinatesMs",
				),
				patchSyncMethod(
					(store.docs.log as any)._nativeBackbone ?? {},
					"commitEntryCoordinates",
					profile,
					"nativeBackboneCommitCoordinatesMs",
				),
				patchAsyncMethod(
					(store.docs.log as any).remoteBlocks ?? {},
					"putKnown",
					profile,
					"remoteBlockPutKnownMs",
				),
				patchAsyncMethod(
					(store.docs.log as any).remoteBlocks ?? {},
					"notifyStored",
					profile,
					"remoteBlockNotifyStoredMs",
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
					"appendLocallyPreparedNativeKnownNoNextCommitOnly",
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

		const nativeBackbone = (store.docs.log as any)._nativeBackbone;
		if (profileNativeBackbone && nativeBackbone?.setAppendProfileEnabled) {
			nativeBackbone.resetAppendProfile?.();
			nativeBackbone.setAppendProfileEnabled(true);
		}

		const serializeStarted = performance.now();
		for (let i = 0; i < iterations; i++) {
			serialize(createDocument());
		}
		profile.serializeMs = performance.now() - serializeStarted;

		try {
			await runPuts(store, iterations, name, profile);
			if (profileNativeBackbone && nativeBackbone?.appendProfile) {
				Object.assign(profile, nativeBackbone.appendProfile());
			}
		} finally {
			nativeBackbone?.setAppendProfileEnabled?.(false);
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
					.filter(([key]) => shouldIncludeProfileKey(key))
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
					.filter(([key]) => shouldIncludeProfileKey(key))
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
	const useDocumentIndex = scenarioUsesNativeBackboneDocumentIndex(name);
	if (useDocumentIndex) {
		backbone.configureDocumentSchemaIr(nativeCeilingContextSchemaIr());
	}
	const useCommittedStorageTransaction =
		scenarioBaseName(name) === "native-backbone-storage-ceiling";
	if (scenarioUsesCoordinateWal(name)) {
		backbone.setCoordinateJournalEnabled(true);
	}
	const documentValuePrefix = new Uint8Array(0);

	const append = (count: number, profile?: Profile) => {
		for (let i = 0; i < count; i++) {
			const documentIndex = useDocumentIndex
				? {
						key: `native-backbone-ceiling-doc-${i}`,
						valuePrefixBytes: documentValuePrefix,
						byteElementIndexLimit: 0,
					}
				: undefined;
			const appendInput = {
				wallTime: BigInt(Date.now()),
				logical: i,
				gid: `gid-${i}`,
				payloadData: payload,
				replicas: 1,
				selfHash: "native-backbone-ceiling-peer",
				trimLengthTo: 100,
				documentIndex,
			};
			const runAppend = () =>
				useCommittedStorageTransaction
					? backbone.preparePlainCommittedNoNextStorageAppendTransaction(
							appendInput,
						)
					: backbone.appendPlainNoNextTransaction(appendInput);
			if (profile) {
				timeSync(profile, "totalPutMs", runAppend);
			} else {
				runAppend();
			}
		}
	};

	const profile = emptyProfile();
	append(warmupIterations);
	if (profileNativeBackbone) {
		backbone.resetAppendProfile();
		backbone.setAppendProfileEnabled(true);
	}
	try {
		append(iterations, profile);
		if (profileNativeBackbone) {
			Object.assign(profile, backbone.appendProfile());
		}
	} finally {
		backbone.setAppendProfileEnabled(false);
	}

	return {
		name,
		iterations,
		payloadBytes,
		opsPerSecond: Math.round((iterations / profile.totalPutMs) * 1000),
		cleanupMs: 0,
		...Object.fromEntries(
			Object.entries(profile)
				.filter(([key]) => shouldIncludeProfileKey(key))
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
			: baseName === "native-backbone-ceiling" ||
				  baseName === "native-backbone-storage-ceiling"
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
					profileNativeBackbone,
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
