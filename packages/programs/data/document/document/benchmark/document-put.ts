import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { create as createSqliteIndexer } from "@peerbit/indexer-sqlite3";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { createRustPeerbitOptions } from "peerbit/rust";
import { Documents, type SetupOptions } from "../src/program.js";

// Run with:
//   cd packages/programs/data/document/document
//   node --loader ts-node/esm ./benchmark/document-put.ts
//
// Env:
// - DOC_WARMUP=100
// - DOC_ITERATIONS=1000
// - DOC_BYTES=1200
// - DOC_SCENARIOS=compat-path,hybrid-anystore,simple-index,sqlite-index,native-graph,native-block-store,rust-peerbit,rust-peerbit-transient-index
//   Add "-nonunique" to any scenario name to use default update-safe put semantics.
//   Add "-putmany" to any unique scenario name to use one putMany call per measured batch.
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

const scenarioNames = (
	process.env.DOC_SCENARIOS ||
	"compat-path,hybrid-anystore,simple-index,sqlite-index,native-graph,native-block-store,rust-peerbit,rust-peerbit-transient-index"
)
	.split(",")
	.map((x) => x.trim())
	.filter(Boolean);

const scenarioBaseName = (name: string) =>
	name.replace(/-putmany$/, "").replace(/-nonunique$/, "");
const scenarioUsesUniquePuts = (name: string) => !name.includes("-nonunique");
const scenarioUsesPutMany = (name: string) => name.endsWith("-putmany");
const profileDeep = process.env.DOC_PROFILE_DEEP === "1";

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

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
			this.bytes = opts.bytes;
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
	sharedPersistCoordinateMs: number;
	logAppendMs: number;
	logGetNextsForAppendMs: number;
	logCreateNativeAppendChainMs: number;
	logPutNativeCommittedAppendMs: number;
	logPutAppendEntriesMs: number;
	logTrimMs: number;
	logTrimUnfilteredLengthMs: number;
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
};

const deepProfileKeys = new Set<keyof Profile>([
	"sharedProcessLocalAppendMs",
	"sharedProcessLocalAppendBatchMs",
	"sharedPlanEntryLeadersMs",
	"sharedPersistCoordinateMs",
	"logGetNextsForAppendMs",
	"logCreateNativeAppendChainMs",
	"logPutNativeCommittedAppendMs",
	"logPutAppendEntriesMs",
	"logTrimMs",
	"logTrimUnfilteredLengthMs",
]);

const emptyProfile = (): Profile => ({
	serializeMs: 0,
	existingHeadLookupMs: 0,
	sharedAppendMs: 0,
	sharedProcessLocalAppendMs: 0,
	sharedProcessLocalAppendBatchMs: 0,
	sharedPlanEntryLeadersMs: 0,
	sharedPersistCoordinateMs: 0,
	logAppendMs: 0,
	logGetNextsForAppendMs: 0,
	logCreateNativeAppendChainMs: 0,
	logPutNativeCommittedAppendMs: 0,
	logPutAppendEntriesMs: 0,
	logTrimMs: 0,
	logTrimUnfilteredLengthMs: 0,
	documentIndexPutMs: 0,
	documentIndexTransformMs: 0,
	documentBackendIndexPutMs: 0,
	totalPutMs: 0,
});

const payload = new Uint8Array(payloadBytes);
for (let i = 0; i < payload.length; i++) {
	payload[i] = i % 256;
}

let idCounter = 0;
const createDocument = () =>
	new Document({
		id: String(idCounter++),
		name: "hello",
		number: 1n,
		bytes: payload,
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
	target[key] = async function patched(this: unknown, ...args: unknown[]) {
		return time(profile, profileKey, () => original.apply(this, args));
	};
	return () => {
		target[key] = original;
	};
};

const openScenario = async (name: string) => {
	const baseName = scenarioBaseName(name);
	const rustOptions =
		baseName === "native-block-store" ||
		baseName === "rust-peerbit" ||
		baseName === "rust-peerbit-transient-index"
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
						: undefined,
	});
	const store = new TestStore({
		docs: new Documents<Document>(),
	});
	const client: ProgramClient = session.peers[0];
	await client.open(store, {
		args: {
			replicate: {
				factor: 1,
			},
			nativeGraph:
				baseName === "native-graph" ||
				baseName === "rust-peerbit" ||
				baseName === "rust-peerbit-transient-index",
			log: {
				trim: { type: "length" as const, to: 100 },
			},
		},
	});
	return { session, store };
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
	const { session, store } = await openScenario(name);
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
				store.docs.index,
				"putWithContext",
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
					"persistCoordinatesBatch",
					profile,
					"sharedPersistCoordinateMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"getNextsForAppend",
					profile,
					"logGetNextsForAppendMs",
				),
				patchAsyncMethod(
					store.docs.log.log as any,
					"createNativePlainAppendChain",
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

		return {
			name,
			iterations,
			payloadBytes,
			opsPerSecond: Math.round((iterations / profile.totalPutMs) * 1000),
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
		await store.drop();
		await session.stop();
	}
};

const rows: BenchRow[] = [];
for (const name of scenarioNames) {
	rows.push(await runScenario(name));
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
