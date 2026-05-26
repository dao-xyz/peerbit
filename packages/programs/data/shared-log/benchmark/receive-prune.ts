// Focused receive/prune hot-path benchmark.
//
// Run from packages/programs/data/shared-log:
//   RECEIVE_PRUNE_COUNTS=100,1000,5000 RECEIVE_PRUNE_WARMUP_RUNS=1 RECEIVE_PRUNE_RUNS=1 BENCH_JSON=1 pnpm run benchmark:receive-prune
//   RECEIVE_PRUNE_SCENARIOS=raw-receive-native,raw-receive-native-backbone,raw-receive-native-coordinate-wal,raw-receive-native-backbone-replicating,raw-receive-native-coordinate-wal-replicating,raw-receive-native-coordinate-wal-replicating-defer-verify,raw-receive-native-coordinate-wal-half,raw-receive-native-coordinate-wal-verify-prepare,raw-receive-native-backbone-select-all,raw-receive-native-coordinate-wal-select-all,raw-receive-native-backbone-select-half,raw-receive-native-coordinate-wal-select-half,raw-receive-native-backbone-drop,raw-receive-native-backbone-drop-verify-prepare,request-prune-native-confirm,request-prune-native-backbone-confirm RECEIVE_PRUNE_COUNTS=1000 pnpm run benchmark:receive-prune
import { create as createRustIndexer } from "@peerbit/indexer-rust";
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { performance } from "node:perf_hooks";
import { v4 as uuid } from "uuid";
import {
	type RawExchangeHeadsMessage,
	RequestIPrune,
	createRawExchangeHeadsMessages,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import type { SyncProfileEvent } from "../src/sync/index.js";
import { EventStore } from "../test/utils/stores/event-store.js";

type Scenario =
	| "raw-receive-native"
	| "raw-receive-native-backbone"
	| "raw-receive-native-coordinate-wal"
	| "raw-receive-native-backbone-verify-prepare"
	| "raw-receive-native-coordinate-wal-verify-prepare"
	| "raw-receive-native-backbone-replicating"
	| "raw-receive-native-backbone-replicating-defer-verify"
	| "raw-receive-native-backbone-replicating-verify-prepare"
	| "raw-receive-native-coordinate-wal-replicating"
	| "raw-receive-native-coordinate-wal-replicating-defer-verify"
	| "raw-receive-native-coordinate-wal-replicating-verify-prepare"
	| "raw-receive-native-backbone-half"
	| "raw-receive-native-backbone-half-verify-prepare"
	| "raw-receive-native-coordinate-wal-half"
	| "raw-receive-native-coordinate-wal-half-verify-prepare"
	| "raw-receive-native-backbone-select-all"
	| "raw-receive-native-coordinate-wal-select-all"
	| "raw-receive-native-backbone-select-half"
	| "raw-receive-native-coordinate-wal-select-half"
	| "raw-receive-native-backbone-drop"
	| "raw-receive-native-backbone-drop-verify-prepare"
	| "raw-receive-native-coordinate-wal-drop"
	| "raw-receive-native-coordinate-wal-drop-verify-prepare"
	| "request-prune-native-confirm"
	| "request-prune-native-backbone-confirm"
	| "request-prune-native-backbone-coordinate-wal-confirm"
	| "request-prune-pending-ihave";

type ProfileSummary = {
	name: string;
	count: number;
	totalMs: number;
	maxMs: number;
	entries: number;
	messages: number;
	nativeBackboneOnly: number;
};

type BenchRow = {
	scenario: Scenario;
	count: number;
	run: number;
	elapsedMs: number;
	opsPerSecond: number;
	profile: ProfileSummary[];
};

const parsePositiveInteger = (value: string | undefined, fallback: number) => {
	if (!value) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		throw new Error(`Expected a positive integer, got '${value}'`);
	}
	return parsed;
};

const parseNonNegativeInteger = (
	value: string | undefined,
	fallback: number,
) => {
	if (!value) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed < 0) {
		throw new Error(`Expected a non-negative integer, got '${value}'`);
	}
	return parsed;
};

const parseCounts = (value: string | undefined) =>
	(value ?? "100,1000,5000")
		.split(",")
		.map((part) => part.trim())
		.filter(Boolean)
		.map((part) => parsePositiveInteger(part, 0));

const parseScenarios = (value: string | undefined): Scenario[] => {
	const scenarios = (value ?? [
		"raw-receive-native",
		"raw-receive-native-backbone",
		"raw-receive-native-coordinate-wal",
		"raw-receive-native-backbone-verify-prepare",
		"raw-receive-native-coordinate-wal-verify-prepare",
		"raw-receive-native-backbone-replicating",
		"raw-receive-native-backbone-replicating-verify-prepare",
		"raw-receive-native-coordinate-wal-replicating",
		"raw-receive-native-coordinate-wal-replicating-verify-prepare",
		"raw-receive-native-backbone-drop",
		"raw-receive-native-backbone-drop-verify-prepare",
		"raw-receive-native-coordinate-wal-drop",
		"raw-receive-native-coordinate-wal-drop-verify-prepare",
		"request-prune-native-confirm",
		"request-prune-native-backbone-confirm",
		"request-prune-native-backbone-coordinate-wal-confirm",
		"request-prune-pending-ihave",
	].join(","))
		.split(",")
		.map((part) => part.trim())
		.filter(Boolean);
	for (const scenario of scenarios) {
		if (
			scenario !== "raw-receive-native" &&
			scenario !== "raw-receive-native-backbone" &&
			scenario !== "raw-receive-native-coordinate-wal" &&
			scenario !== "raw-receive-native-backbone-verify-prepare" &&
			scenario !== "raw-receive-native-coordinate-wal-verify-prepare" &&
			scenario !== "raw-receive-native-backbone-replicating" &&
			scenario !==
				"raw-receive-native-backbone-replicating-defer-verify" &&
			scenario !==
				"raw-receive-native-backbone-replicating-verify-prepare" &&
			scenario !== "raw-receive-native-coordinate-wal-replicating" &&
			scenario !==
				"raw-receive-native-coordinate-wal-replicating-defer-verify" &&
			scenario !==
				"raw-receive-native-coordinate-wal-replicating-verify-prepare" &&
			scenario !== "raw-receive-native-backbone-half" &&
			scenario !== "raw-receive-native-backbone-half-verify-prepare" &&
			scenario !== "raw-receive-native-coordinate-wal-half" &&
			scenario !== "raw-receive-native-coordinate-wal-half-verify-prepare" &&
			scenario !== "raw-receive-native-backbone-select-all" &&
			scenario !== "raw-receive-native-coordinate-wal-select-all" &&
			scenario !== "raw-receive-native-backbone-select-half" &&
			scenario !== "raw-receive-native-coordinate-wal-select-half" &&
			scenario !== "raw-receive-native-backbone-drop" &&
			scenario !== "raw-receive-native-backbone-drop-verify-prepare" &&
			scenario !== "raw-receive-native-coordinate-wal-drop" &&
			scenario !== "raw-receive-native-coordinate-wal-drop-verify-prepare" &&
			scenario !== "request-prune-native-confirm" &&
			scenario !== "request-prune-native-backbone-confirm" &&
			scenario !== "request-prune-native-backbone-coordinate-wal-confirm" &&
			scenario !== "request-prune-pending-ihave"
		) {
			throw new Error(`Unknown receive/prune scenario '${scenario}'`);
		}
	}
	return scenarios as Scenario[];
};

const counts = parseCounts(process.env.RECEIVE_PRUNE_COUNTS);
const runs = parsePositiveInteger(process.env.RECEIVE_PRUNE_RUNS, 1);
const warmupRuns = parseNonNegativeInteger(
	process.env.RECEIVE_PRUNE_WARMUP_RUNS,
	0,
);
const scenarios = parseScenarios(process.env.RECEIVE_PRUNE_SCENARIOS);

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-receive-prune",
};

const summarizeProfileEvents = (
	events: SyncProfileEvent[],
): ProfileSummary[] => {
	const summaries = new Map<string, ProfileSummary>();
	for (const event of events) {
		let summary = summaries.get(event.name);
		if (!summary) {
			summary = {
				name: event.name,
				count: 0,
				totalMs: 0,
				maxMs: 0,
				entries: 0,
				messages: 0,
				nativeBackboneOnly: 0,
			};
			summaries.set(event.name, summary);
		}
		const durationMs = event.durationMs ?? 0;
		summary.count += 1;
		summary.totalMs += durationMs;
		summary.maxMs = Math.max(summary.maxMs, durationMs);
		summary.entries += event.entries ?? 0;
		summary.messages += event.messages ?? 0;
		summary.nativeBackboneOnly +=
			typeof event.details?.nativeBackboneOnly === "number"
				? event.details.nativeBackboneOnly
				: 0;
	}
	return [...summaries.values()].sort((a, b) => b.totalMs - a.totalMs);
};

const createOpenArgs = (
	profileEvents: SyncProfileEvent[],
	options?: {
		nativeBackbone?: boolean;
		coordinateWal?: boolean;
		verifySignaturesDuringPrepare?: boolean;
		replicating?: boolean;
		drop?: boolean;
		keepEvery?: number;
		nativeSelectEvery?: number;
		keepHashes?: Set<string>;
	},
) => {
	const nativeBackbone =
		options?.nativeBackbone || options?.coordinateWal
			? {
					optional: false,
					...(options?.coordinateWal
						? {
								coordinatePersistence:
									new NativeBackboneCoordinatePersistence(
										new NativeBackboneMemoryCoordinatePersistenceStore(),
									),
							}
						: {}),
				}
			: undefined;
	return {
		replicate: options?.replicating ? { factor: 1 } : false,
		setup,
		nativeGraph: true,
		nativeBackbone,
		keep: options?.keepHashes
			? (entry: { hash: string }) => options.keepHashes!.has(entry.hash)
			: options?.drop || options?.nativeSelectEvery
				? undefined
				: () => true,
		timeUntilRoleMaturity: 0,
		respondToIHaveTimeout: 1,
		sync: {
			rawExchangeHeads: true,
			...(options?.verifySignaturesDuringPrepare === undefined
				? {}
				: {
						rawExchangeHeadsVerifySignaturesDuringPrepare:
							options.verifySignaturesDuringPrepare,
					}),
			profile: (event: SyncProfileEvent) => profileEvents.push(event),
		},
	};
};

type RawReceiveBenchEntry = { hash: string; gid: string };

const appendIndependentEntryInfos = async (
	store: EventStore<string, any>,
	count: number,
) => {
	const entries: RawReceiveBenchEntry[] = [];
	for (let i = 0; i < count; i++) {
		const { entry } = await store.add(uuid(), {
			meta: {
				next: [],
				gidSeed: new Uint8Array([i & 0xff, (i >>> 8) & 0xff]),
			},
			replicate: false,
			target: "none",
		});
		entries.push({ hash: entry.hash, gid: entry.meta.gid });
	}
	return entries;
};

const appendIndependentEntries = async (
	store: EventStore<string, any>,
	count: number,
) => (await appendIndependentEntryInfos(store, count)).map((entry) => entry.hash);

const createRawMessages = async (
	store: EventStore<string, any>,
	hashes: string[],
) => {
	const messages: RawExchangeHeadsMessage[] = [];
	for await (const message of createRawExchangeHeadsMessages(
		store.log.log,
		hashes,
	)) {
		messages.push(message as RawExchangeHeadsMessage);
	}
	return messages;
};

const integerBigInt = (value: bigint | number | string) =>
	typeof value === "bigint" ? value : BigInt(String(value));

const seedNativeSelectionRanges = (
	store: EventStore<string, any>,
	entries: RawReceiveBenchEntry[],
	retainedPeerHash: string,
	keepEvery: number,
) => {
	const backbone = (store.log as any)._nativeBackbone;
	if (!backbone) {
		throw new Error("Expected native backbone for native selection bench");
	}
	const retainedHashes = new Set<string>();
	for (let i = 0; i < entries.length; i++) {
		const entry = entries[i]!;
		const retained = i % keepEvery === 0;
		if (retained) {
			retainedHashes.add(entry.hash);
		}
		// Raw entries in this benchmark currently request two replicas; seed both
		// coordinates so dropped entries do not retain through fallback leaders.
		for (const [coordinateIndex, coordinate] of backbone
			.getGidCoordinates(entry.gid, 2)
			.entries()) {
			const start = integerBigInt(coordinate);
			const end = start + 1n;
			backbone.putRange({
				id: `native-select-${i}-${coordinateIndex}`,
				hash: retained
					? retainedPeerHash
					: `native-select-other-${i}-${coordinateIndex}`,
				timestamp: 0,
				start1: start,
				end1: end,
				start2: start,
				end2: end,
				width: 1,
				mode: 0,
			});
		}
	}
	return retainedHashes;
};

const runRawReceive = async (
	count: number,
	run: number,
	options?: {
		nativeBackbone?: boolean;
		coordinateWal?: boolean;
		verifySignaturesDuringPrepare?: boolean;
		replicating?: boolean;
		drop?: boolean;
		keepEvery?: number;
		nativeSelectEvery?: number;
	},
): Promise<BenchRow> => {
	const session = await TestSession.disconnected(2, {
		indexer: (directory) => createRustIndexer(directory),
	});
	const profileEvents: SyncProfileEvent[] = [];
	const keepHashes = options?.keepEvery ? new Set<string>() : undefined;
	try {
		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: createOpenArgs([]),
		});
		const db2 = await session.peers[1].open(new EventStore<string, any>(), {
			args: createOpenArgs(profileEvents, {
				...options,
				keepHashes,
			}),
		});
		const entryInfos = await appendIndependentEntryInfos(db1, count);
		const hashes = entryInfos.map((entry) => entry.hash);
		let nativeSelectedHashes: Set<string> | undefined;
		if (keepHashes) {
			for (let i = 0; i < hashes.length; i++) {
				if (i % options!.keepEvery! === 0) {
					keepHashes.add(hashes[i]!);
				}
			}
		}
		if (options?.nativeSelectEvery) {
			nativeSelectedHashes = seedNativeSelectionRanges(
				db2,
				entryInfos,
				db2.node.identity.publicKey.hashcode(),
				options.nativeSelectEvery,
			);
		}
		const messages = await createRawMessages(db1, hashes);

		const started = performance.now();
		for (const message of messages) {
			await db2.log.onMessage(message, {
				from: db1.node.identity.publicKey,
			} as any);
		}
		const elapsed = performance.now() - started;

		const expectedReceived = options?.drop
			? 0
			: nativeSelectedHashes
				? nativeSelectedHashes.size
			: keepHashes
				? keepHashes.size
				: count;
		if (db2.log.log.length !== expectedReceived) {
			throw new Error(
				`Expected ${expectedReceived} raw received entries, got ${db2.log.log.length}`,
			);
		}
		if (options?.nativeSelectEvery) {
			const expectedDropped = count - expectedReceived;
			const nativeSelectEvents = profileEvents.filter(
				(event) => event.name === "sharedLog.rawReceive.nativeSelect",
			);
			if (nativeSelectEvents.length === 0 && expectedDropped > 0) {
				throw new Error("Expected native raw receive selection profile event");
			}
			const retainedByNativeSelect = nativeSelectEvents.reduce(
				(sum, event) => sum + (event.count ?? 0),
				0,
			);
			const droppedByNativeSelect = nativeSelectEvents.reduce(
				(sum, event) =>
					sum +
					(typeof event.details?.dropped === "number"
						? event.details.dropped
						: 0),
				0,
			);
			if (
				expectedDropped > 0 &&
				retainedByNativeSelect !== expectedReceived
			) {
				throw new Error(
					`Expected native raw receive selection to retain ${expectedReceived} entries, retained ${retainedByNativeSelect}`,
				);
			}
			if (droppedByNativeSelect !== expectedDropped) {
				throw new Error(
					`Expected native raw receive selection to drop ${expectedDropped} entries, dropped ${droppedByNativeSelect}`,
				);
			}
		}

		return {
			scenario:
				options?.coordinateWal &&
				options.drop &&
				options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-coordinate-wal-drop-verify-prepare"
				: options?.nativeBackbone &&
					  options.drop &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-backbone-drop-verify-prepare"
				: options?.coordinateWal && options.drop
					? "raw-receive-native-coordinate-wal-drop"
				: options?.nativeBackbone && options.drop
					? "raw-receive-native-backbone-drop"
				: options?.coordinateWal &&
					  options.keepEvery === 2 &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-coordinate-wal-half-verify-prepare"
				: options?.nativeBackbone &&
					  options.keepEvery === 2 &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-backbone-half-verify-prepare"
				: options?.coordinateWal && options.keepEvery === 2
					? "raw-receive-native-coordinate-wal-half"
				: options?.nativeBackbone && options.keepEvery === 2
					? "raw-receive-native-backbone-half"
				: options?.coordinateWal && options.nativeSelectEvery === 1
					? "raw-receive-native-coordinate-wal-select-all"
				: options?.nativeBackbone && options.nativeSelectEvery === 1
					? "raw-receive-native-backbone-select-all"
				: options?.coordinateWal && options.nativeSelectEvery === 2
					? "raw-receive-native-coordinate-wal-select-half"
				: options?.nativeBackbone && options.nativeSelectEvery === 2
					? "raw-receive-native-backbone-select-half"
				: options?.coordinateWal &&
					  options.replicating &&
					  options.verifySignaturesDuringPrepare === false
					? "raw-receive-native-coordinate-wal-replicating-defer-verify"
				: options?.nativeBackbone &&
					  options.replicating &&
					  options.verifySignaturesDuringPrepare === false
					? "raw-receive-native-backbone-replicating-defer-verify"
				: options?.coordinateWal &&
					  options.replicating &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-coordinate-wal-replicating-verify-prepare"
				: options?.nativeBackbone &&
					  options.replicating &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-backbone-replicating-verify-prepare"
				: options?.coordinateWal && options.replicating
					? "raw-receive-native-coordinate-wal-replicating"
				: options?.nativeBackbone && options.replicating
					? "raw-receive-native-backbone-replicating"
				: options?.coordinateWal &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-coordinate-wal-verify-prepare"
				: options?.nativeBackbone &&
					  options.verifySignaturesDuringPrepare === true
					? "raw-receive-native-backbone-verify-prepare"
				: options?.coordinateWal
					? "raw-receive-native-coordinate-wal"
				: options?.nativeBackbone
					? "raw-receive-native-backbone"
				: "raw-receive-native",
			count,
			run,
			elapsedMs: elapsed,
			opsPerSecond: (count / elapsed) * 1000,
			profile: summarizeProfileEvents(profileEvents),
		};
	} finally {
		await session.stop();
	}
};

const installFastLeaderWaits = (store: EventStore<string, any>) => {
	const log = store.log as any;
	const selfHash = store.node.identity.publicKey.hashcode();
	const originalWaitForGid = log._waitForGidReplicators;
	const originalWaitForEntry = log._waitForEntryReplicators;
	log._waitForGidReplicators = async (
		_gid: string,
		_replicas: number,
		_waitFor: unknown,
		options: { onLeader?: (key: string) => void } | undefined,
	) => {
		options?.onLeader?.(selfHash);
		return new Map([[selfHash, { intersecting: true }]]);
	};
	log._waitForEntryReplicators = async (
		_entry: unknown,
		_replicas: number,
		_waitFor: unknown,
		options: { onLeader?: (key: string) => void } | undefined,
	) => {
		options?.onLeader?.(selfHash);
		return new Map([[selfHash, { intersecting: true }]]);
	};
	return () => {
		log._waitForGidReplicators = originalWaitForGid;
		log._waitForEntryReplicators = originalWaitForEntry;
	};
};

const suppressPruneResponses = (store: EventStore<string, any>) => {
	const debounced = (store.log as any).responseToPruneDebouncedFn;
	const originalAdd = debounced.add;
	debounced.add = () => undefined;
	return () => {
		debounced.add = originalAdd;
	};
};

const clearPendingIHaves = (store: EventStore<string, any>) => {
	const pending = (store.log as any)._pendingIHave as Map<
		string,
		{ clear?: () => void }
	>;
	for (const value of pending.values()) {
		value.clear?.();
	}
	pending.clear();
};

const runRequestPruneNativeConfirm = async (
	count: number,
	run: number,
	options?: { nativeBackbone?: boolean; coordinateWal?: boolean },
): Promise<BenchRow> => {
	const session = await TestSession.disconnected(2, {
		indexer: (directory) => createRustIndexer(directory),
	});
	const profileEvents: SyncProfileEvent[] = [];
	try {
		const db = await session.peers[1].open(new EventStore<string, any>(), {
			args: {
				...createOpenArgs(profileEvents, options),
				replicate: { factor: 1 },
			},
		});
		const hashes = await appendIndependentEntries(db, count);
		const restoreWaits = installFastLeaderWaits(db);
		const restoreResponses = suppressPruneResponses(db);
		try {
			const started = performance.now();
			await db.log.onMessage(new RequestIPrune({ hashes }), {
				from: session.peers[0].identity.publicKey,
			} as any);
			const elapsed = performance.now() - started;

			return {
				scenario: options?.coordinateWal
					? "request-prune-native-backbone-coordinate-wal-confirm"
					: options?.nativeBackbone
						? "request-prune-native-backbone-confirm"
					: "request-prune-native-confirm",
				count,
				run,
				elapsedMs: elapsed,
				opsPerSecond: (count / elapsed) * 1000,
				profile: summarizeProfileEvents(profileEvents),
			};
		} finally {
			restoreResponses();
			restoreWaits();
			clearPendingIHaves(db);
		}
	} finally {
		await session.stop();
	}
};

const runRequestPrunePendingIHave = async (
	count: number,
	run: number,
): Promise<BenchRow> => {
	const session = await TestSession.disconnected(2, {
		indexer: (directory) => createRustIndexer(directory),
	});
	const sourceProfileEvents: SyncProfileEvent[] = [];
	const targetProfileEvents: SyncProfileEvent[] = [];
	try {
		const source = await session.peers[0].open(new EventStore<string, any>(), {
			args: createOpenArgs(sourceProfileEvents),
		});
		const target = await session.peers[1].open(new EventStore<string, any>(), {
			args: createOpenArgs(targetProfileEvents),
		});
		const hashes = await appendIndependentEntries(source, count);

		try {
			const started = performance.now();
			await target.log.onMessage(new RequestIPrune({ hashes }), {
				from: source.node.identity.publicKey,
			} as any);
			const elapsed = performance.now() - started;

			return {
				scenario: "request-prune-pending-ihave",
				count,
				run,
				elapsedMs: elapsed,
				opsPerSecond: (count / elapsed) * 1000,
				profile: summarizeProfileEvents(targetProfileEvents),
			};
		} finally {
			clearPendingIHaves(target);
		}
	} finally {
		await session.stop();
	}
};

const runScenario = async (
	scenario: Scenario,
	count: number,
	run: number,
): Promise<BenchRow> => {
	if (scenario === "raw-receive-native") {
		return runRawReceive(count, run);
	}
	if (scenario === "raw-receive-native-backbone") {
		return runRawReceive(count, run, { nativeBackbone: true });
	}
	if (scenario === "raw-receive-native-coordinate-wal") {
		return runRawReceive(count, run, { coordinateWal: true });
	}
	if (scenario === "raw-receive-native-backbone-verify-prepare") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-verify-prepare") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-backbone-replicating") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			replicating: true,
		});
	}
	if (scenario === "raw-receive-native-backbone-replicating-defer-verify") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			replicating: true,
			verifySignaturesDuringPrepare: false,
		});
	}
	if (scenario === "raw-receive-native-backbone-replicating-verify-prepare") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			replicating: true,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-replicating") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			replicating: true,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-replicating-defer-verify") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			replicating: true,
			verifySignaturesDuringPrepare: false,
		});
	}
	if (
		scenario === "raw-receive-native-coordinate-wal-replicating-verify-prepare"
	) {
		return runRawReceive(count, run, {
			coordinateWal: true,
			replicating: true,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-backbone-half") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			keepEvery: 2,
		});
	}
	if (scenario === "raw-receive-native-backbone-half-verify-prepare") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			keepEvery: 2,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-half") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			keepEvery: 2,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-half-verify-prepare") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			keepEvery: 2,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-backbone-select-half") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			nativeSelectEvery: 2,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-select-half") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			nativeSelectEvery: 2,
		});
	}
	if (scenario === "raw-receive-native-backbone-select-all") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			nativeSelectEvery: 1,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-select-all") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			nativeSelectEvery: 1,
		});
	}
	if (scenario === "raw-receive-native-backbone-drop") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			drop: true,
		});
	}
	if (scenario === "raw-receive-native-backbone-drop-verify-prepare") {
		return runRawReceive(count, run, {
			nativeBackbone: true,
			drop: true,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-drop") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			drop: true,
		});
	}
	if (scenario === "raw-receive-native-coordinate-wal-drop-verify-prepare") {
		return runRawReceive(count, run, {
			coordinateWal: true,
			drop: true,
			verifySignaturesDuringPrepare: true,
		});
	}
	if (scenario === "request-prune-native-confirm") {
		return runRequestPruneNativeConfirm(count, run);
	}
	if (scenario === "request-prune-native-backbone-confirm") {
		return runRequestPruneNativeConfirm(count, run, {
			nativeBackbone: true,
		});
	}
	if (scenario === "request-prune-native-backbone-coordinate-wal-confirm") {
		return runRequestPruneNativeConfirm(count, run, {
			coordinateWal: true,
		});
	}
	return runRequestPrunePendingIHave(count, run);
};

const rows: BenchRow[] = [];
for (const scenario of scenarios) {
	for (const count of counts) {
		for (let warmup = 0; warmup < warmupRuns; warmup++) {
			await runScenario(scenario, count, -1 - warmup);
		}
		for (let run = 0; run < runs; run++) {
			rows.push(await runScenario(scenario, count, run));
		}
	}
}

const aggregateRows = [...new Set(rows.map((row) => row.scenario))].flatMap(
	(scenario) =>
		counts.map((count) => {
			const samples = rows.filter(
				(row) => row.scenario === scenario && row.count === count,
			);
			const meanMs =
				samples.reduce((sum, row) => sum + row.elapsedMs, 0) / samples.length;
			const meanOps =
				samples.reduce((sum, row) => sum + row.opsPerSecond, 0) /
				samples.length;
			return {
				scenario,
				count,
				runs: samples.length,
				meanMs: Math.round(meanMs * 100) / 100,
				meanOpsPerSecond: Math.round(meanOps),
				profile: samples[0]?.profile,
			};
		}),
);

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(
		JSON.stringify(
			{
				name: "shared-log-receive-prune",
				warmupRuns,
				rows,
				aggregateRows,
			},
			null,
			2,
		),
	);
} else {
	console.table(
		aggregateRows.map((row) => ({
			scenario: row.scenario,
			count: row.count,
			runs: row.runs,
			meanMs: row.meanMs,
			meanOpsPerSecond: row.meanOpsPerSecond,
		})),
	);
	console.dir(aggregateRows, { depth: 6 });
}
process.exit(process.exitCode ?? 0);
