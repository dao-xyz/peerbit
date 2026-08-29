import { deserialize, serialize } from "@dao-xyz/borsh";
import type { PublicSignKey } from "@peerbit/crypto";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { performance } from "node:perf_hooks";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	createExchangeHeadsMessages,
} from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import type { SyncProfileEvent } from "../src/sync/index.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import {
	EventStore,
	type Operation,
} from "../test/utils/stores/event-store.js";

type Args = {
	entries: number;
	payloadBytes: number;
	rounds: number;
	json: boolean;
	profile: WorkloadProfile;
	skipNoCanAppend: boolean;
};

type Mode = "scalar" | "batch" | "no-canappend";

type WorkloadProfile = "uniform" | "shared-fs";

type WorkloadKind = "uniform" | "naming" | "file-version" | "file-chunk";

type KindCounts = Record<WorkloadKind, number>;

type SharedFsShapedValue =
	| {
			kind: "naming";
			id: string;
			nodeId: string;
			parentId: string;
			name: string;
			deleted: boolean;
			causalDepth: number;
			parentNamingIdsJson: string;
			observedContentHeadsJson: string;
			createdAt: number;
			authorKey: string;
			machineLabel: string;
			changesetId: string;
	  }
	| {
			kind: "file-version";
			id: string;
			nodeId: string;
			parentVersionIdsJson: string;
			causalDepth: number;
			contentHash: string;
			size: number;
			chunkIdsJson: string;
			createdAt: number;
			authorKey: string;
			machineLabel: string;
			conflictResolution: boolean;
			changesetId: string;
	  }
	| {
			kind: "file-chunk";
			id: string;
			bytes: string;
			hash: string;
	  };

type BenchmarkValue = string | SharedFsShapedValue;

type Sample = {
	mode: Mode;
	elapsedMs: number;
	scalarSignatureCalls: number;
	nativeBatchCalls: number;
	nativeBatchSignatureEntries: number;
	callbackCalls: number;
	callbackKindCounts: KindCounts;
	acceptedEntries: number;
};

const defaults: Args = {
	entries: 6_200,
	payloadBytes: 1_024,
	rounds: 4,
	json: false,
	profile: "uniform",
	skipNoCanAppend: false,
};

const NATIVE_BATCH_MIN_ENTRIES = 16;
const SHARED_FS_NAMING_ENTRIES = 2_000;
const SHARED_FS_VERSION_ENTRIES = 2_000;
const SHARED_FS_CHUNK_ENTRIES = 2_200;

const emptyKindCounts = (): KindCounts => ({
	uniform: 0,
	naming: 0,
	"file-version": 0,
	"file-chunk": 0,
});

const addKindCounts = (target: KindCounts, source: KindCounts) => {
	for (const kind of Object.keys(target) as WorkloadKind[]) {
		target[kind] += source[kind];
	}
	return target;
};

const kindCountsEqual = (left: KindCounts, right: KindCounts) =>
	(Object.keys(left) as WorkloadKind[]).every(
		(kind) => left[kind] === right[kind],
	);

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-batch-canappend-signatures-benchmark",
};

const parseArgs = (argv: string[]): Args => {
	const args = { ...defaults };
	const readNumber = (name: string, index: number) => {
		const value = argv[index + 1];
		if (!value) throw new Error(`Missing value for ${name}`);
		const parsed = Number.parseInt(value, 10);
		if (!Number.isFinite(parsed) || parsed <= 0) {
			throw new Error(`Expected ${name} to be a positive integer`);
		}
		return parsed;
	};
	for (let i = 0; i < argv.length; i++) {
		switch (argv[i]) {
			case "--profile": {
				const profile = argv[++i];
				if (profile !== "uniform" && profile !== "shared-fs") {
					throw new Error("--profile must be uniform or shared-fs");
				}
				args.profile = profile;
				break;
			}
			case "--entries":
				args.entries = readNumber("--entries", i++);
				break;
			case "--payloadBytes":
				args.payloadBytes = readNumber("--payloadBytes", i++);
				break;
			case "--rounds":
				args.rounds = readNumber("--rounds", i++);
				break;
			case "--json":
				args.json = true;
				break;
			case "--skipNoCanAppend":
				args.skipNoCanAppend = true;
				break;
			case "--":
				break;
			default:
				if (argv[i]!.startsWith("--")) {
					throw new Error(`Unknown argument: ${argv[i]}`);
				}
		}
	}
	if (args.payloadBytes < 8) {
		throw new Error("--payloadBytes must be at least 8");
	}
	if (args.profile === "shared-fs") {
		args.entries =
			SHARED_FS_NAMING_ENTRIES +
			SHARED_FS_VERSION_ENTRIES +
			SHARED_FS_CHUNK_ENTRIES;
	}
	return args;
};

const median = (values: number[]) => {
	const sorted = [...values].sort((left, right) => left - right);
	const middle = Math.floor(sorted.length / 2);
	return sorted.length % 2 === 0
		? (sorted[middle - 1]! + sorted[middle]!) / 2
		: sorted[middle]!;
};

const collectGarbage = () =>
	(globalThis as typeof globalThis & { gc?: () => void }).gc?.();

const createMessageBytes = <T>(entries: Entry<T>[]) =>
	serialize(
		new ExchangeHeadsMessage({
			heads: entries.map(
				(entry) => new EntryWithRefs({ entry, gidRefrences: [] }),
			),
		}),
	);

const createWireSizedMessageBytes = async <T>(
	log: EventStore<any, any>["log"]["log"],
	entries: Entry<T>[],
) => {
	const messages: Uint8Array[] = [];
	const entryCounts: number[] = [];
	for await (const message of createExchangeHeadsMessages(log, entries)) {
		messages.push(serialize(message));
		entryCounts.push(message.heads.length);
	}
	return { messages, entryCounts };
};

const decodeMessage = <T>(bytes: Uint8Array) => {
	const message = deserialize(bytes, TransportMessage);
	if (!(message instanceof ExchangeHeadsMessage)) {
		throw new Error("Expected an ExchangeHeadsMessage");
	}
	return message as ExchangeHeadsMessage<T>;
};

const padded = (value: number) => value.toString().padStart(8, "0");

const digest = (value: number) =>
	`${value.toString(16).padStart(8, "0")}${"a".repeat(56)}`;

const createSharedFsShapedValues = (): SharedFsShapedValue[] => {
	const authorKey = `z${"a".repeat(47)}`;
	const machineLabel = "shared-fs-benchmark-writer";
	const naming = Array.from(
		{ length: SHARED_FS_NAMING_ENTRIES },
		(_, index): SharedFsShapedValue => ({
			kind: "naming",
			id: `naming:${digest(index)}`,
			nodeId: `node:${padded(index)}`,
			parentId: `directory:${padded(index % 200)}`,
			name: `file-${padded(index)}.txt`,
			deleted: false,
			causalDepth: 1,
			parentNamingIdsJson: "[]",
			observedContentHeadsJson: "[]",
			createdAt: 1_700_000_000_000 + index,
			authorKey,
			machineLabel,
			changesetId: `changeset:${padded(Math.floor(index / 500))}`,
		}),
	);
	const versions = Array.from(
		{ length: SHARED_FS_VERSION_ENTRIES },
		(_, index): SharedFsShapedValue => {
			const chunkHash = digest(index);
			return {
				kind: "file-version",
				id: `version:${digest(index)}`,
				nodeId: `node:${padded(index)}`,
				parentVersionIdsJson: "[]",
				causalDepth: 1,
				contentHash: digest(index + 10_000),
				size: 32 + (index % 65),
				chunkIdsJson: JSON.stringify([`chunk:${chunkHash}`]),
				createdAt: 1_700_000_100_000 + index,
				authorKey,
				machineLabel,
				conflictResolution: false,
				changesetId: `changeset:${padded(Math.floor(index / 500))}`,
			};
		},
	);
	const chunks = Array.from(
		{ length: SHARED_FS_CHUNK_ENTRIES },
		(_, index): SharedFsShapedValue => {
			const hash = digest(index);
			return {
				kind: "file-chunk",
				id: `chunk:${hash}`,
				// EventStore's benchmark encoding is JSON, so use an honestly labelled,
				// exactly-sized string instead of a Uint8Array (which JSON would expand
				// into 1,024 numeric object properties). This matches the 1 KiB chunk
				// body in the shared-fs-shaped Documents fixture.
				bytes: `${padded(index)}${"x".repeat(1_016)}`,
				hash,
			};
		},
	);
	return [...naming, ...versions, ...chunks];
};

const workloadKind = (value: BenchmarkValue): WorkloadKind =>
	typeof value === "string" ? "uniform" : value.kind;

const validateBenchmarkValue = (value: BenchmarkValue) => {
	if (typeof value === "string") {
		return value.length >= 8;
	}
	switch (value.kind) {
		case "naming":
			return (
				value.id.startsWith("naming:") &&
				value.nodeId.startsWith("node:") &&
				value.parentId.startsWith("directory:") &&
				value.causalDepth >= 1
			);
		case "file-version":
			return (
				value.id.startsWith("version:") &&
				value.nodeId.startsWith("node:") &&
				value.contentHash.length === 64 &&
				value.chunkIdsJson.startsWith('["chunk:')
			);
		case "file-chunk":
			return value.id === `chunk:${value.hash}` && value.bytes.length === 1_024;
	}
};

const prepareWorkload = async (args: Args) => {
	const session = await TestSession.disconnected(1);
	const store = new EventStore<BenchmarkValue, any>();
	try {
		const source = await session.peers[0].open(store.clone(), {
			args: {
				replicate: false,
				setup,
				timeUntilRoleMaturity: 0,
			},
		});
		const values: BenchmarkValue[] =
			args.profile === "shared-fs"
				? createSharedFsShapedValues()
				: Array.from({ length: args.entries }, (_, index) => {
						const suffix = "x".repeat(args.payloadBytes - 8);
						return `${padded(index)}${suffix}`;
					});
		const expectedKindCounts = emptyKindCounts();
		for (const value of values) expectedKindCounts[workloadKind(value)]++;
		const entries: Entry<Operation<BenchmarkValue>>[] = [];
		// Keep preparation out of the receive measurement, and bound each local
		// append transaction so the benchmark does not exceed SQLite's expression
		// depth before the receive path is reached.
		for (let offset = 0; offset < values.length; offset += 256) {
			const result = await source.addMany(values.slice(offset, offset + 256), {
				meta: { next: [] },
				replicate: false,
				target: "none",
			});
			entries.push(...result.entries);
		}
		if (entries.length !== args.entries) {
			throw new Error("Entry preparation count mismatch");
		}
		const wireMessages = await createWireSizedMessageBytes(
			source.log.log,
			entries,
		);
		return {
			store,
			from: source.node.identity.publicKey,
			messageBytes: wireMessages.messages,
			messageEntryCounts: wireMessages.entryCounts,
			expectedKindCounts,
			warmupMessageBytes: [createMessageBytes(entries.slice(0, 16))],
		};
	} finally {
		await session.stop();
	}
};

const runReceive = async (properties: {
	mode: Mode;
	profile: WorkloadProfile;
	store: EventStore<BenchmarkValue, any>;
	from: PublicSignKey;
	messageBytes: Uint8Array[];
	messageEntryCounts: number[];
	expectedKindCounts: KindCounts;
}): Promise<Sample> => {
	const session = await TestSession.disconnected(1);
	const profile: SyncProfileEvent[] = [];
	let callbackCalls = 0;
	const callbackKindCounts = emptyKindCounts();
	const canAppend =
		properties.profile === "shared-fs"
			? async (entry: Entry<Operation<BenchmarkValue>>) => {
					callbackCalls++;
					const operation = await entry.getPayloadValue();
					const kind = workloadKind(operation.value!);
					callbackKindCounts[kind]++;
					// This remains an arbitrary application closure. It branches across
					// all three shapes and crosses an async boundary once per entry.
					await Promise.resolve();
					return validateBenchmarkValue(operation.value!);
				}
			: () => {
					callbackCalls++;
					callbackKindCounts.uniform++;
					return true;
				};
	try {
		const target = await session.peers[0].open(properties.store.clone(), {
			args: {
				replicate: { factor: 1 },
				setup,
				...(properties.mode === "no-canappend"
					? {}
					: {
							canAppend,
						}),
				sync: { profile: (event) => profile.push(event) },
				timeUntilRoleMaturity: 0,
			},
		});
		if (properties.mode === "scalar") {
			// A/B control: leave the complete receive path unchanged except for
			// declining this candidate's operation-scoped crypto preverification.
			(target.log as any).preverifyReceiveSignaturesBatch = async () =>
				undefined;
		}

		// Decode the exact same serialized bytes outside the measured onMessage
		// interval, as the RPC layer does before dispatching a plain message.
		const messages = properties.messageBytes.map((bytes) =>
			decodeMessage<Operation<BenchmarkValue>>(bytes),
		);
		let scalarSignatureCalls = 0;
		for (const message of messages) {
			for (const { entry } of message.heads) {
				const verifySignatures = entry.verifySignatures.bind(entry);
				entry.verifySignatures = async () => {
					scalarSignatureCalls++;
					return verifySignatures();
				};
			}
		}

		const startedAt = performance.now();
		for (const message of messages) {
			await target.log.onMessage(message, { from: properties.from } as any);
		}
		const elapsedMs = performance.now() - startedAt;
		const nativeBatchEvents = profile.filter(
			(event) =>
				event.name === "sharedLog.canAppendBatch.verifySignatures" &&
				event.details?.programCanAppendDeferred === true,
		);
		const sample: Sample = {
			mode: properties.mode,
			elapsedMs,
			scalarSignatureCalls,
			nativeBatchCalls: nativeBatchEvents.length,
			nativeBatchSignatureEntries: nativeBatchEvents.reduce(
				(total, event) => total + (event.entries ?? 0),
				0,
			),
			callbackCalls,
			callbackKindCounts,
			acceptedEntries: target.log.log.length,
		};
		const expectedEntries = messages.reduce(
			(total, message) => total + message.heads.length,
			0,
		);
		const expectedCallbacks =
			properties.mode === "no-canappend" ? 0 : expectedEntries;
		const expectedCallbackKindCounts =
			properties.mode === "no-canappend"
				? emptyKindCounts()
				: properties.expectedKindCounts;
		if (
			sample.callbackCalls !== expectedCallbacks ||
			sample.acceptedEntries !== expectedEntries ||
			!kindCountsEqual(sample.callbackKindCounts, expectedCallbackKindCounts)
		) {
			throw new Error(
				`${properties.mode} receive changed callback/acceptance semantics`,
			);
		}
		if (
			properties.mode === "scalar" &&
			sample.scalarSignatureCalls !== expectedEntries
		) {
			throw new Error("Scalar control did not verify every signature");
		}
		const expectedNativeBatchCalls = properties.messageEntryCounts.filter(
			(count) => count >= NATIVE_BATCH_MIN_ENTRIES,
		).length;
		const expectedNativeBatchEntries = properties.messageEntryCounts
			.filter((count) => count >= NATIVE_BATCH_MIN_ENTRIES)
			.reduce((total, count) => total + count, 0);
		const expectedCandidateScalarEntries =
			expectedEntries - expectedNativeBatchEntries;
		if (
			properties.mode === "batch" &&
			(sample.scalarSignatureCalls !== expectedCandidateScalarEntries ||
				sample.nativeBatchCalls !== expectedNativeBatchCalls ||
				sample.nativeBatchSignatureEntries !== expectedNativeBatchEntries)
		) {
			throw new Error("Candidate did not batch every eligible signature");
		}
		return sample;
	} finally {
		await session.stop();
	}
};

const summarize = (samples: Sample[], entries: number) => {
	const elapsed = samples.map((sample) => sample.elapsedMs);
	const medianMs = median(elapsed);
	return {
		medianMs: Number(medianMs.toFixed(3)),
		entriesPerSecond: Number(((entries * 1_000) / medianMs).toFixed(2)),
		scalarSignatureCalls: samples.reduce(
			(total, sample) => total + sample.scalarSignatureCalls,
			0,
		),
		nativeBatchCalls: samples.reduce(
			(total, sample) => total + sample.nativeBatchCalls,
			0,
		),
		nativeBatchSignatureEntries: samples.reduce(
			(total, sample) => total + sample.nativeBatchSignatureEntries,
			0,
		),
		callbackCalls: samples.reduce(
			(total, sample) => total + sample.callbackCalls,
			0,
		),
		callbackKindCounts: samples.reduce(
			(total, sample) => addKindCounts(total, sample.callbackKindCounts),
			emptyKindCounts(),
		),
		callbackKindCountsPerSample: samples.map(
			(sample) => sample.callbackKindCounts,
		),
		acceptedEntries: samples.map((sample) => sample.acceptedEntries),
		samplesMs: elapsed.map((value) => Number(value.toFixed(3))),
	};
};

const run = async () => {
	const args = parseArgs(process.argv.slice(2));
	const workload = await prepareWorkload(args);
	const warmupExpectedKindCounts = emptyKindCounts();
	warmupExpectedKindCounts[
		args.profile === "shared-fs" ? "naming" : "uniform"
	] = 16;

	// Warm both complete receive paths with the production threshold before
	// collecting samples. Each warmup and measured leg gets a fresh peer/store.
	for (const mode of ["scalar", "batch"] as const) {
		await runReceive({
			mode,
			profile: args.profile,
			store: workload.store,
			from: workload.from,
			messageBytes: workload.warmupMessageBytes,
			messageEntryCounts: [16],
			expectedKindCounts: warmupExpectedKindCounts,
		});
		collectGarbage();
	}

	const samples: Sample[] = [];
	for (let round = 0; round < args.rounds; round++) {
		const order: Mode[] =
			round % 2 === 0 ? ["scalar", "batch"] : ["batch", "scalar"];
		for (const mode of order) {
			samples.push(
				await runReceive({
					mode,
					profile: args.profile,
					store: workload.store,
					from: workload.from,
					messageBytes: workload.messageBytes,
					messageEntryCounts: workload.messageEntryCounts,
					expectedKindCounts: workload.expectedKindCounts,
				}),
			);
			collectGarbage();
		}
	}
	if (!args.skipNoCanAppend) {
		await runReceive({
			mode: "no-canappend",
			profile: args.profile,
			store: workload.store,
			from: workload.from,
			messageBytes: workload.warmupMessageBytes,
			messageEntryCounts: [16],
			expectedKindCounts: emptyKindCounts(),
		});
		collectGarbage();
		for (let round = 0; round < args.rounds; round++) {
			samples.push(
				await runReceive({
					mode: "no-canappend",
					profile: args.profile,
					store: workload.store,
					from: workload.from,
					messageBytes: workload.messageBytes,
					messageEntryCounts: workload.messageEntryCounts,
					expectedKindCounts: emptyKindCounts(),
				}),
			);
			collectGarbage();
		}
	}

	const scalar = summarize(
		samples.filter((sample) => sample.mode === "scalar"),
		args.entries,
	);
	const batch = summarize(
		samples.filter((sample) => sample.mode === "batch"),
		args.entries,
	);
	const noCanAppend = args.skipNoCanAppend
		? undefined
		: summarize(
				samples.filter((sample) => sample.mode === "no-canappend"),
				args.entries,
			);
	const result = {
		benchmark: "ordinary ExchangeHeadsMessage onMessage receive",
		measurementBoundary:
			"all onMessage calls; identical serialized messages decoded first",
		workload: {
			profile: args.profile,
			entries: args.entries,
			payloadBytes: args.profile === "uniform" ? args.payloadBytes : undefined,
			kindCounts: workload.expectedKindCounts,
			sharedFsChunkPayload:
				args.profile === "shared-fs" ? "1024-char" : undefined,
			messages: workload.messageBytes.length,
			serializedMessageBytes: workload.messageBytes.reduce(
				(total, bytes) => total + bytes.byteLength,
				0,
			),
			serializedMessageBytesMin: Math.min(
				...workload.messageBytes.map((bytes) => bytes.byteLength),
			),
			serializedMessageBytesMax: Math.max(
				...workload.messageBytes.map((bytes) => bytes.byteLength),
			),
			messageEntryCounts: workload.messageEntryCounts,
			productionSizing:
				"createExchangeHeadsMessages production 100 kB target (one head may cross it)",
			rounds: args.rounds,
			comparisonCanAppend: true,
			arbitraryAsyncCanAppend: args.profile === "shared-fs",
			additionalNoCanAppendLeg: !args.skipNoCanAppend,
			freshTargetPerLeg: true,
		},
		scalar,
		batch,
		noCanAppend,
		speedup: Number((scalar.medianMs / batch.medianMs).toFixed(3)),
	};
	if (args.json) {
		console.log(JSON.stringify(result));
	} else {
		console.table([scalar, batch]);
		console.log(`speedup: ${result.speedup}x`);
	}
};

await run();
