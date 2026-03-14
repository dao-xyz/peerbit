import { field, option, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { delay } from "@peerbit/time";
import {
	Documents,
	SearchRequest,
	StringMatch,
	type SetupOptions,
} from "../src/index.js";

type BenchMode = "adaptive" | "fixed1";

type Args = {
	replicate: any;
};

type Sample = {
	writerCompleteMs: number;
	observerReadyMs: number;
	observerTailMs: number;
	manifestPolls: number;
	chunkPolls: number;
};

type Task = {
	name: string;
	hz: number;
	mean_ms: number;
	rme: null;
	samples: number;
};

const modes = ["adaptive", "fixed1"] as const satisfies readonly BenchMode[];
const envInt = (name: string, fallback: number) => {
	const value = process.env[name];
	if (value == null) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	return Number.isFinite(parsed) ? parsed : fallback;
};

const chunkBytes = Math.max(1, envInt("FILE_INGEST_CHUNK_BYTES", 262144));
const chunks = Math.max(1, envInt("FILE_INGEST_CHUNKS", 24));
const warmupIterations = Math.max(0, envInt("FILE_INGEST_WARMUP", 1));
const iterations = Math.max(1, envInt("FILE_INGEST_ITERATIONS", 3));
const readyTimeoutMs = Math.max(
	1_000,
	envInt("FILE_INGEST_READY_TIMEOUT_MS", 30_000),
);
const pollDelayMs = Math.max(10, envInt("FILE_INGEST_POLL_DELAY_MS", 50));

const replicationByMode: Record<BenchMode, any> = {
	adaptive: {
		limits: {
			cpu: {
				max: 1,
			},
		},
	},
	fixed1: {
		factor: 1,
	},
};

@variant("bench_file_record")
class FileRecord {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	@field({ type: "string" })
	kind: "manifest" | "chunk";

	@field({ type: "u32" })
	size: number;

	@field({ type: "bool" })
	ready: boolean;

	@field({ type: option("string") })
	parentId?: string;

	@field({ type: option("u32") })
	index?: number;

	@field({ type: option("u32") })
	chunkCount?: number;

	@field({ type: option("string") })
	finalHash?: string;

	@field({ type: option(Uint8Array) })
	bytes?: Uint8Array;

	constructor(properties?: Partial<FileRecord>) {
		this.id = properties?.id ?? "";
		this.name = properties?.name ?? "";
		this.kind = (properties?.kind as "manifest" | "chunk") ?? "chunk";
		this.size = properties?.size ?? 0;
		this.ready = properties?.ready ?? false;
		this.parentId = properties?.parentId;
		this.index = properties?.index;
		this.chunkCount = properties?.chunkCount;
		this.finalHash = properties?.finalHash;
		this.bytes = properties?.bytes;
	}
}

@variant("bench_file_indexable")
class FileRecordIndexable {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	name: string;

	@field({ type: "string" })
	kind: string;

	@field({ type: "u32" })
	size: number;

	@field({ type: "bool" })
	ready: boolean;

	@field({ type: option("string") })
	parentId?: string;

	@field({ type: option("u32") })
	index?: number;

	constructor(record: FileRecord) {
		this.id = record.id;
		this.name = record.name;
		this.kind = record.kind;
		this.size = record.size;
		this.ready = record.ready;
		this.parentId = record.parentId;
		this.index = record.index;
	}
}

@variant("bench_file_store")
class FileStore extends Program<Partial<SetupOptions<FileRecord>> & Args> {
	@field({ type: Documents })
	files: Documents<FileRecord, FileRecordIndexable>;

	constructor() {
		super();
		this.files = new Documents();
	}

	async open(
		options?: Partial<SetupOptions<FileRecord>> & Partial<Args>,
	): Promise<void> {
		await this.files.open({
			type: FileRecord,
			index: {
				type: FileRecordIndexable,
			},
			replicate: options?.replicate,
			replicas: { min: 1 },
		});
	}
}

const average = (values: number[]) =>
	values.reduce((sum, value) => sum + value, 0) / values.length;

const round = (value: number, digits = 3) =>
	Number.isFinite(value) ? Number(value.toFixed(digits)) : value;

const makePayloadChunks = (): Uint8Array[] => {
	const all = new Uint8Array(chunkBytes * chunks);
	for (let i = 0; i < all.length; i++) {
		all[i] = i % 251;
	}
	return Array.from({ length: chunks }, (_, index) =>
		all.subarray(index * chunkBytes, (index + 1) * chunkBytes),
	);
};

const uploadChunkedFile = async (
	store: FileStore,
	fileName: string,
	payloadChunks: Uint8Array[],
) => {
	const uploadId = `${fileName}-upload`;
	const totalSize = payloadChunks.reduce(
		(sum, value) => sum + value.byteLength,
		0,
	);
	const hash = Buffer.from(uploadId).toString("base64url");

	await store.files.put(
		new FileRecord({
			id: uploadId,
			name: fileName,
			kind: "manifest",
			size: totalSize,
			chunkCount: payloadChunks.length,
			ready: false,
		}),
	);

	for (let index = 0; index < payloadChunks.length; index++) {
		const bytes = payloadChunks[index]!;
		await store.files.put(
			new FileRecord({
				id: `${uploadId}:${index}`,
				name: `${fileName}/${index}`,
				kind: "chunk",
				size: bytes.byteLength,
				parentId: uploadId,
				index,
				ready: true,
				bytes,
			}),
		);
	}

	await store.files.put(
		new FileRecord({
			id: uploadId,
			name: fileName,
			kind: "manifest",
			size: totalSize,
			chunkCount: payloadChunks.length,
			ready: true,
			finalHash: hash,
		}),
	);

	return uploadId;
};

const queryOptions = {
	local: true,
	remote: {
		throwOnMissing: false,
		replicate: true,
	},
} as const;

const waitForObserverReady = async (
	observer: FileStore,
	uploadId: string,
	expectedChunks: number,
) => {
	const startedAt = performance.now();
	const deadline = startedAt + readyTimeoutMs;
	let manifestPolls = 0;
	let chunkPolls = 0;

	while (performance.now() < deadline) {
		manifestPolls += 1;
		const manifests = await observer.files.index.search(
			new SearchRequest({
				query: new StringMatch({
					key: "id",
					value: uploadId,
				}),
				fetch: 1,
			}),
			queryOptions,
		);
		const manifest = manifests[0] as FileRecord | undefined;

		if (manifest?.ready) {
			chunkPolls += 1;
			const chunkResults = await observer.files.index.search(
				new SearchRequest({
					query: new StringMatch({
						key: "parentId",
						value: uploadId,
					}),
					fetch: 0xffffffff,
				}),
				queryOptions,
			);
			if (chunkResults.length === expectedChunks) {
				return {
					manifestPolls,
					chunkPolls,
				};
			}
		}

		await delay(pollDelayMs);
	}

	throw new Error(
		`Timed out waiting for observer readiness for upload ${uploadId}`,
	);
};

const runIteration = async (
	session: Awaited<ReturnType<typeof TestSession.connected>>,
	mode: BenchMode,
	sequence: number,
	payloadChunks: Uint8Array[],
): Promise<Sample> => {
	const writer = await session.peers[0].open(new FileStore(), {
		args: {
			replicate: replicationByMode[mode],
		},
	});

	const seeder = await session.peers[1].open<FileStore>(writer.address!, {
		args: {
			replicate: replicationByMode[mode],
		},
	});

	const observer = await session.peers[2].open<FileStore>(writer.address!, {
		args: {
			replicate: false,
		},
	});

	try {
		await Promise.all([
			writer.files.log.waitForReplicator(seeder.node.identity.publicKey, {
				timeout: readyTimeoutMs,
			}),
			observer.files.log.waitForReplicator(writer.node.identity.publicKey, {
				timeout: readyTimeoutMs,
			}),
		]);

		const startedAt = performance.now();
		const uploadId = await uploadChunkedFile(
			writer,
			`${mode}-file-${sequence}`,
			payloadChunks,
		);
		const writerCompleteMs = performance.now() - startedAt;

		const observerProgress = await waitForObserverReady(
			observer,
			uploadId,
			payloadChunks.length,
		);
		const observerReadyMs = performance.now() - startedAt;

		return {
			writerCompleteMs,
			observerReadyMs,
			observerTailMs: observerReadyMs - writerCompleteMs,
			manifestPolls: observerProgress.manifestPolls,
			chunkPolls: observerProgress.chunkPolls,
		};
	} finally {
		await Promise.allSettled([observer.close(), seeder.close(), writer.close()]);
	}
};

const summarizeTask = (name: string, values: number[]): Task => {
	const meanMs = average(values);
	return {
		name,
		hz: round(1000 / meanMs, 6),
		mean_ms: round(meanMs, 6),
		rme: null,
		samples: values.length,
	};
};

const runMode = async (mode: BenchMode, payloadChunks: Uint8Array[]) => {
	const session = await TestSession.connected(3);
	const samples: Sample[] = [];

	try {
		for (let i = 0; i < warmupIterations; i++) {
			await runIteration(session, mode, -1 - i, payloadChunks);
		}
		for (let i = 0; i < iterations; i++) {
			samples.push(await runIteration(session, mode, i, payloadChunks));
		}
	} finally {
		await session.stop();
	}

	return samples;
};

const payloadChunks = makePayloadChunks();
const results = await Promise.all(
	modes.map(async (mode) => ({
		mode,
		samples: await runMode(mode, payloadChunks),
	})),
);

const tasks: Task[] = [];
for (const result of results) {
	tasks.push(
		summarizeTask(
			`${result.mode}: writer-complete`,
			result.samples.map((sample) => sample.writerCompleteMs),
		),
	);
	tasks.push(
		summarizeTask(
			`${result.mode}: observer-ready`,
			result.samples.map((sample) => sample.observerReadyMs),
		),
	);
	tasks.push(
		summarizeTask(
			`${result.mode}: observer-tail`,
			result.samples.map((sample) => sample.observerTailMs),
		),
	);
}

const output = {
	name: "file-ingest",
	tasks,
	meta: {
		chunks,
		chunkBytes,
		totalBytes: chunks * chunkBytes,
		warmupIterations,
		iterations,
		readyTimeoutMs,
		pollDelayMs,
		results: results.map((result) => ({
			mode: result.mode,
			writerCompleteMsAvg: round(
				average(result.samples.map((sample) => sample.writerCompleteMs)),
			),
			observerReadyMsAvg: round(
				average(result.samples.map((sample) => sample.observerReadyMs)),
			),
			observerTailMsAvg: round(
				average(result.samples.map((sample) => sample.observerTailMs)),
			),
			manifestPollsAvg: round(
				average(result.samples.map((sample) => sample.manifestPolls)),
			),
			chunkPollsAvg: round(
				average(result.samples.map((sample) => sample.chunkPolls)),
			),
			samples: result.samples.map((sample) => ({
				writerCompleteMs: round(sample.writerCompleteMs),
				observerReadyMs: round(sample.observerReadyMs),
				observerTailMs: round(sample.observerTailMs),
				manifestPolls: sample.manifestPolls,
				chunkPolls: sample.chunkPolls,
			})),
		})),
	},
};

if (process.env.BENCH_JSON === "1") {
	await new Promise<void>((resolve, reject) => {
		process.stdout.write(JSON.stringify(output, null, 2), (error) => {
			if (error) {
				reject(error);
				return;
			}
			resolve();
		});
	});
} else {
	console.table(
		tasks.map((task) => ({
			task: task.name,
			mean_ms: task.mean_ms,
			ops_s: round(task.hz, 3),
			samples: task.samples,
		})),
	);
}

process.exit(0);
