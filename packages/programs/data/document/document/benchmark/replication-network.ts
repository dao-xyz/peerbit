import { field, option, variant } from "@dao-xyz/borsh";
import { create as createSimpleIndexer } from "@peerbit/indexer-simple";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { performance } from "node:perf_hooks";
import { v4 as uuid } from "uuid";
import { Documents, type SetupOptions } from "../src/program.js";

/**
 * Run examples:
 * node --loader ts-node/esm ./benchmark/replication-network.ts --nodes 5 --docs 20 --topology full --replicateFactor 1
 * node --loader ts-node/esm ./benchmark/replication-network.ts --nodes 100 --docs 50 --topology random --degree 8 --replicateFactor 1 --mockCrypto true --fanoutJoinTimeoutMs 240000
 */

@variant("bench_document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	payload?: string;

	constructor(properties?: { id: string; payload?: string }) {
		this.id = properties?.id ?? "";
		this.payload = properties?.payload;
	}
}

@variant("bench_documents_store")
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor() {
		super();
		this.docs = new Documents();
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({ ...options, type: Document });
	}
}

type Topology = "full" | "line" | "random";

type Args = {
	nodes: number;
	docs: number;
	warmup: number;
	topology: Topology;
	degree: number;
	replicateFactor: number;
	writer: number;
	concurrency: number;
	payloadBytes: number;
	timeoutMs: number;
	openTimeoutMs: number;
	seed: number;
	mockCrypto: boolean;
	timeUntilRoleMaturity: number;
	fanoutJoinTimeoutMs: number;
	joinReqTimeoutMs: number;
	joinAttemptsPerRound: number;
	trackerCandidates: number;
	trackerQueryTimeoutMs: number;
	candidateShuffleTopK: number;
	candidateCooldownMs: number;
	bootstrapMaxPeers: number;
	trackerWarmupMs: number;
	admissionBatchSize: number;
	admissionPauseMs: number;
	failOnTimeout: boolean;
};

type DocLatency = {
	id: string;
	firstRemoteMs: number | null;
	convergedMs: number | null;
	timedOut: boolean;
	targetsReached: number;
	targetsTotal: number;
};

type Tracker = {
	startMs: number;
	pendingTargets: Set<number>;
	targetsTotal: number;
	firstRemoteMs: number | null;
	resolve: (latency: DocLatency) => void;
	timer: ReturnType<typeof setTimeout>;
};

const DEFAULTS: Args = {
	nodes: 5,
	docs: 20,
	warmup: 3,
	topology: "full",
	degree: 3,
	replicateFactor: 1,
	writer: 0,
	concurrency: 1,
	payloadBytes: 256,
	timeoutMs: 20_000,
	openTimeoutMs: 120_000,
	seed: 1,
	mockCrypto: true,
	timeUntilRoleMaturity: 0,
	fanoutJoinTimeoutMs: 180_000,
	joinReqTimeoutMs: 500,
	joinAttemptsPerRound: 2,
	trackerCandidates: 8,
	trackerQueryTimeoutMs: 500,
	candidateShuffleTopK: 4,
	candidateCooldownMs: 1_000,
	bootstrapMaxPeers: 2,
	trackerWarmupMs: 5_000,
	admissionBatchSize: 25,
	admissionPauseMs: 1_000,
	failOnTimeout: true,
};

const parseBool = (value: string): boolean => {
	const normalized = value.trim().toLowerCase();
	if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
	if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
	throw new Error(`Invalid boolean '${value}'`);
};

const parseArgs = (argv: string[]): Args => {
	const out: Args = { ...DEFAULTS };
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i]!;
		const next = argv[i + 1];
		if (arg === "--help" || arg === "-h") {
			console.log(
				[
					"replication-network benchmark",
					"",
					"Flags:",
					"  --nodes N                  number of peers (default: 5)",
					"  --docs N                   measured writes (default: 20)",
					"  --warmup N                 warmup writes before measuring (default: 3)",
					"  --topology full|line|random (default: full)",
					"  --degree N                 random-graph degree when --topology random (default: 3)",
					"  --replicateFactor X        replication factor [0..1], where 1 = full-domain (default: 1)",
					"  --writer N                 writer peer index (default: 0)",
					"  --concurrency N            in-flight puts (default: 1)",
					"  --payloadBytes N           payload size in bytes (default: 256)",
					"  --timeoutMs N              timeout per doc convergence (default: 20000)",
					"  --openTimeoutMs N          timeout per peer open/bootstrap step (default: 120000)",
					"  --seed N                   random seed for random topology (default: 1)",
					"  --mockCrypto true|false    in-memory crypto shortcut (default: true)",
					"  --timeUntilRoleMaturity N  role maturity (ms) for opened stores (default: 0)",
					"  --fanoutJoinTimeoutMs N    fanout join timeout used during bootstrap (default: 180000)",
					"  --joinReqTimeoutMs N       timeout per JOIN_REQ attempt (default: 500)",
					"  --joinAttemptsPerRound N   join attempts per retry round (default: 2)",
					"  --trackerCandidates N      tracker candidates requested per query (default: 8)",
					"  --trackerQueryTimeoutMs N  timeout for tracker reply (default: 500)",
					"  --candidateShuffleTopK N   shuffled candidate window (default: 4)",
					"  --candidateCooldownMs N    candidate cooldown after failures (default: 1000)",
					"  --bootstrapMaxPeers N      bootstrap peers to keep dialed (default: 2)",
					"  --trackerWarmupMs N        wait before opening stores in sparse topologies (default: 5000)",
					"  --admissionBatchSize N     stores opened before pause (default: 25)",
					"  --admissionPauseMs N       pause between admission batches (default: 1000)",
					"  --failOnTimeout true|false exit non-zero if any write does not converge (default: true)",
				].join("\n"),
			);
			process.exit(0);
		}

		const consume = (): string => {
			if (next == null || next.startsWith("--")) {
				throw new Error(`Missing value for ${arg}`);
			}
			i += 1;
			return next;
		};

		switch (arg) {
			case "--nodes":
				out.nodes = Number.parseInt(consume(), 10);
				break;
			case "--docs":
				out.docs = Number.parseInt(consume(), 10);
				break;
			case "--warmup":
				out.warmup = Number.parseInt(consume(), 10);
				break;
			case "--topology":
				out.topology = consume() as Topology;
				break;
			case "--degree":
				out.degree = Number.parseInt(consume(), 10);
				break;
			case "--replicateFactor":
				out.replicateFactor = Number.parseFloat(consume());
				break;
			case "--writer":
				out.writer = Number.parseInt(consume(), 10);
				break;
			case "--concurrency":
				out.concurrency = Number.parseInt(consume(), 10);
				break;
			case "--payloadBytes":
				out.payloadBytes = Number.parseInt(consume(), 10);
				break;
			case "--timeoutMs":
				out.timeoutMs = Number.parseInt(consume(), 10);
				break;
			case "--openTimeoutMs":
				out.openTimeoutMs = Number.parseInt(consume(), 10);
				break;
			case "--seed":
				out.seed = Number.parseInt(consume(), 10);
				break;
			case "--mockCrypto":
				out.mockCrypto = parseBool(consume());
				break;
			case "--timeUntilRoleMaturity":
				out.timeUntilRoleMaturity = Number.parseInt(consume(), 10);
				break;
			case "--failOnTimeout":
				out.failOnTimeout = parseBool(consume());
				break;
			case "--fanoutJoinTimeoutMs":
				out.fanoutJoinTimeoutMs = Number.parseInt(consume(), 10);
				break;
			case "--joinReqTimeoutMs":
				out.joinReqTimeoutMs = Number.parseInt(consume(), 10);
				break;
			case "--joinAttemptsPerRound":
				out.joinAttemptsPerRound = Number.parseInt(consume(), 10);
				break;
			case "--trackerCandidates":
				out.trackerCandidates = Number.parseInt(consume(), 10);
				break;
			case "--trackerQueryTimeoutMs":
				out.trackerQueryTimeoutMs = Number.parseInt(consume(), 10);
				break;
			case "--candidateShuffleTopK":
				out.candidateShuffleTopK = Number.parseInt(consume(), 10);
				break;
			case "--candidateCooldownMs":
				out.candidateCooldownMs = Number.parseInt(consume(), 10);
				break;
			case "--bootstrapMaxPeers":
				out.bootstrapMaxPeers = Number.parseInt(consume(), 10);
				break;
			case "--trackerWarmupMs":
				out.trackerWarmupMs = Number.parseInt(consume(), 10);
				break;
			case "--admissionBatchSize":
				out.admissionBatchSize = Number.parseInt(consume(), 10);
				break;
			case "--admissionPauseMs":
				out.admissionPauseMs = Number.parseInt(consume(), 10);
				break;
			default:
				if (arg.startsWith("--")) {
					throw new Error(`Unknown flag: ${arg}`);
				}
				break;
		}
	}

	if (!Number.isFinite(out.nodes) || out.nodes < 2) {
		throw new Error(`Expected --nodes >= 2, got '${out.nodes}'`);
	}
	if (!Number.isFinite(out.docs) || out.docs < 1) {
		throw new Error(`Expected --docs >= 1, got '${out.docs}'`);
	}
	if (!Number.isFinite(out.warmup) || out.warmup < 0) {
		throw new Error(`Expected --warmup >= 0, got '${out.warmup}'`);
	}
	if (!["full", "line", "random"].includes(out.topology)) {
		throw new Error(`Expected --topology to be full|line|random, got '${out.topology}'`);
	}
	if (!Number.isFinite(out.degree) || out.degree < 1) {
		throw new Error(`Expected --degree >= 1, got '${out.degree}'`);
	}
	if (
		!Number.isFinite(out.replicateFactor) ||
		out.replicateFactor < 0 ||
		out.replicateFactor > 1
	) {
		throw new Error(
			`Expected --replicateFactor in [0,1], got '${out.replicateFactor}'`,
		);
	}
	if (!Number.isFinite(out.writer) || out.writer < 0 || out.writer >= out.nodes) {
		throw new Error(
			`Expected --writer in [0, ${out.nodes - 1}], got '${out.writer}'`,
		);
	}
	if (!Number.isFinite(out.concurrency) || out.concurrency < 1) {
		throw new Error(`Expected --concurrency >= 1, got '${out.concurrency}'`);
	}
	if (!Number.isFinite(out.payloadBytes) || out.payloadBytes < 0) {
		throw new Error(`Expected --payloadBytes >= 0, got '${out.payloadBytes}'`);
	}
	if (!Number.isFinite(out.timeoutMs) || out.timeoutMs < 1) {
		throw new Error(`Expected --timeoutMs >= 1, got '${out.timeoutMs}'`);
	}
	if (!Number.isFinite(out.openTimeoutMs) || out.openTimeoutMs < 1) {
		throw new Error(`Expected --openTimeoutMs >= 1, got '${out.openTimeoutMs}'`);
	}
	if (
		!Number.isFinite(out.timeUntilRoleMaturity) ||
		out.timeUntilRoleMaturity < 0
	) {
		throw new Error(
			`Expected --timeUntilRoleMaturity >= 0, got '${out.timeUntilRoleMaturity}'`,
		);
	}
	if (!Number.isFinite(out.fanoutJoinTimeoutMs) || out.fanoutJoinTimeoutMs < 1) {
		throw new Error(
			`Expected --fanoutJoinTimeoutMs >= 1, got '${out.fanoutJoinTimeoutMs}'`,
		);
	}
	if (!Number.isFinite(out.joinReqTimeoutMs) || out.joinReqTimeoutMs < 1) {
		throw new Error(`Expected --joinReqTimeoutMs >= 1, got '${out.joinReqTimeoutMs}'`);
	}
	if (!Number.isFinite(out.joinAttemptsPerRound) || out.joinAttemptsPerRound < 1) {
		throw new Error(
			`Expected --joinAttemptsPerRound >= 1, got '${out.joinAttemptsPerRound}'`,
		);
	}
	if (!Number.isFinite(out.trackerCandidates) || out.trackerCandidates < 1) {
		throw new Error(`Expected --trackerCandidates >= 1, got '${out.trackerCandidates}'`);
	}
	if (
		!Number.isFinite(out.trackerQueryTimeoutMs) ||
		out.trackerQueryTimeoutMs < 1
	) {
		throw new Error(
			`Expected --trackerQueryTimeoutMs >= 1, got '${out.trackerQueryTimeoutMs}'`,
		);
	}
	if (!Number.isFinite(out.candidateShuffleTopK) || out.candidateShuffleTopK < 0) {
		throw new Error(
			`Expected --candidateShuffleTopK >= 0, got '${out.candidateShuffleTopK}'`,
		);
	}
	if (!Number.isFinite(out.candidateCooldownMs) || out.candidateCooldownMs < 0) {
		throw new Error(
			`Expected --candidateCooldownMs >= 0, got '${out.candidateCooldownMs}'`,
		);
	}
	if (!Number.isFinite(out.bootstrapMaxPeers) || out.bootstrapMaxPeers < 1) {
		throw new Error(`Expected --bootstrapMaxPeers >= 1, got '${out.bootstrapMaxPeers}'`);
	}
	if (!Number.isFinite(out.trackerWarmupMs) || out.trackerWarmupMs < 0) {
		throw new Error(`Expected --trackerWarmupMs >= 0, got '${out.trackerWarmupMs}'`);
	}
	if (!Number.isFinite(out.admissionBatchSize) || out.admissionBatchSize < 1) {
		throw new Error(
			`Expected --admissionBatchSize >= 1, got '${out.admissionBatchSize}'`,
		);
	}
	if (!Number.isFinite(out.admissionPauseMs) || out.admissionPauseMs < 0) {
		throw new Error(`Expected --admissionPauseMs >= 0, got '${out.admissionPauseMs}'`);
	}
	out.nodes = Math.floor(out.nodes);
	out.docs = Math.floor(out.docs);
	out.warmup = Math.floor(out.warmup);
	out.degree = Math.floor(out.degree);
	out.writer = Math.floor(out.writer);
	out.concurrency = Math.floor(out.concurrency);
	out.payloadBytes = Math.floor(out.payloadBytes);
	out.timeoutMs = Math.floor(out.timeoutMs);
	out.openTimeoutMs = Math.floor(out.openTimeoutMs);
	out.seed = Math.floor(out.seed);
	out.timeUntilRoleMaturity = Math.floor(out.timeUntilRoleMaturity);
	out.fanoutJoinTimeoutMs = Math.floor(out.fanoutJoinTimeoutMs);
	out.joinReqTimeoutMs = Math.floor(out.joinReqTimeoutMs);
	out.joinAttemptsPerRound = Math.floor(out.joinAttemptsPerRound);
	out.trackerCandidates = Math.floor(out.trackerCandidates);
	out.trackerQueryTimeoutMs = Math.floor(out.trackerQueryTimeoutMs);
	out.candidateShuffleTopK = Math.floor(out.candidateShuffleTopK);
	out.candidateCooldownMs = Math.floor(out.candidateCooldownMs);
	out.bootstrapMaxPeers = Math.floor(out.bootstrapMaxPeers);
	out.trackerWarmupMs = Math.floor(out.trackerWarmupMs);
	out.admissionBatchSize = Math.floor(out.admissionBatchSize);
	out.admissionPauseMs = Math.floor(out.admissionPauseMs);
	return out;
};

const percentile = (values: number[], p: number): number => {
	if (values.length === 0) return Number.NaN;
	const sorted = [...values].sort((a, b) => a - b);
	const pos = Math.min(sorted.length - 1, Math.max(0, Math.floor(p * (sorted.length - 1))));
	return sorted[pos]!;
};

const stats = (values: number[]) => {
	if (values.length === 0) {
		return {
			count: 0,
			min: Number.NaN,
			max: Number.NaN,
			mean: Number.NaN,
			p50: Number.NaN,
			p95: Number.NaN,
			p99: Number.NaN,
		};
	}
	let sum = 0;
	let min = Number.POSITIVE_INFINITY;
	let max = Number.NEGATIVE_INFINITY;
	for (const v of values) {
		sum += v;
		min = Math.min(min, v);
		max = Math.max(max, v);
	}
	return {
		count: values.length,
		min,
		max,
		mean: sum / values.length,
		p50: percentile(values, 0.5),
		p95: percentile(values, 0.95),
		p99: percentile(values, 0.99),
	};
};

const formatMs = (value: number) => (Number.isFinite(value) ? `${value.toFixed(2)} ms` : "n/a");
const formatRate = (value: number) =>
	Number.isFinite(value) ? `${value.toFixed(2)} /s` : "n/a";

const buildPayload = (payloadBytes: number) => {
	if (payloadBytes <= 0) return "";
	return "x".repeat(payloadBytes);
};

const sleep = (ms: number) =>
	new Promise<void>((resolve) => {
		setTimeout(resolve, ms);
	});

const withTimeout = async <T>(
	promise: Promise<T>,
	timeoutMs: number,
	label: string,
): Promise<T> => {
	let timeoutId: ReturnType<typeof setTimeout> | undefined;
	const timeoutPromise = new Promise<never>((_, reject) => {
		timeoutId = setTimeout(() => {
			reject(new Error(`${label} timed out after ${timeoutMs} ms`));
		}, timeoutMs);
	});
	try {
		return await Promise.race([promise, timeoutPromise]);
	} finally {
		if (timeoutId) clearTimeout(timeoutId);
	}
};

const buildOpenOrder = (args: Args, bootstrapIndices: number[]): number[] => {
	const seen = new Set<number>();
	const out: number[] = [];
	const push = (i: number) => {
		if (i < 0 || i >= args.nodes) return;
		if (seen.has(i)) return;
		seen.add(i);
		out.push(i);
	};

	push(args.writer);
	for (const i of bootstrapIndices) push(i);
	for (let i = 0; i < args.nodes; i++) push(i);
	return out;
};

const configureTopology = async (
	session: TestSession,
	args: Args,
): Promise<{ edgeCount: number; topologyNote: string }> => {
	if (args.topology === "full") {
		await session.connect();
		const edgeCount = Math.floor((args.nodes * (args.nodes - 1)) / 2);
		return { edgeCount, topologyNote: "fully connected" };
	}

	if (args.topology === "line") {
		const peers = session.peers;
		const groups: Array<[any, any]> = [];
		for (let i = 0; i < peers.length - 1; i++) {
			groups.push([peers[i]!, peers[i + 1]!]);
		}
		await session.connect(groups as any);
		return {
			edgeCount: Math.max(0, args.nodes - 1),
			topologyNote: "line (pairwise groups)",
		};
	}

	const requestedDegree = Math.min(args.degree, Math.max(1, args.nodes - 1));
	const adjacency = await session.connectRandomGraph({
		degree: requestedDegree,
		seed: args.seed,
	});

	let edgeCount = 0;
	for (const graph of adjacency) {
		for (let i = 0; i < graph.length; i++) {
			for (const j of graph[i]!) {
				if (j > i) edgeCount += 1;
			}
		}
	}

	return {
		edgeCount,
		topologyNote: `random bounded-degree (degree<=${requestedDegree})`,
	};
};

const main = async () => {
	const args = parseArgs(process.argv.slice(2));
	const payloadTemplate = buildPayload(args.payloadBytes);
	const targets = new Set<number>();
	for (let i = 0; i < args.nodes; i++) {
		if (i !== args.writer) targets.add(i);
	}

	console.log("Starting replication-network benchmark");
	console.log(
		JSON.stringify(
			{
				nodes: args.nodes,
				docs: args.docs,
				warmup: args.warmup,
				topology: args.topology,
				degree: args.degree,
				replicateFactor: args.replicateFactor,
				writer: args.writer,
				concurrency: args.concurrency,
				payloadBytes: args.payloadBytes,
				timeoutMs: args.timeoutMs,
				openTimeoutMs: args.openTimeoutMs,
				seed: args.seed,
				mockCrypto: args.mockCrypto,
				timeUntilRoleMaturity: args.timeUntilRoleMaturity,
				fanoutJoinTimeoutMs: args.fanoutJoinTimeoutMs,
				joinReqTimeoutMs: args.joinReqTimeoutMs,
				joinAttemptsPerRound: args.joinAttemptsPerRound,
				trackerCandidates: args.trackerCandidates,
				trackerQueryTimeoutMs: args.trackerQueryTimeoutMs,
				candidateShuffleTopK: args.candidateShuffleTopK,
				candidateCooldownMs: args.candidateCooldownMs,
				bootstrapMaxPeers: args.bootstrapMaxPeers,
				trackerWarmupMs: args.trackerWarmupMs,
				admissionBatchSize: args.admissionBatchSize,
				admissionPauseMs: args.admissionPauseMs,
				failOnTimeout: args.failOnTimeout,
			},
			null,
			2,
		),
	);

	let session: TestSession | undefined;
	let stores: Array<TestStore | undefined> = [];
	const listeners: Array<() => void> = [];
	const trackers = new Map<string, Tracker>();
	const perNodeReceivedCount: number[] = new Array(args.nodes).fill(0);

	try {
		session = await TestSession.disconnectedInMemory(args.nodes, {
			mockCrypto: args.mockCrypto,
			seed: args.seed,
			indexer: createSimpleIndexer,
		});

		const bootstrapCount = Math.min(8, args.nodes);
		const bootstrapIndices = Array.from({ length: bootstrapCount }, (_, i) => i);
		const bootstrapAddrs = bootstrapIndices.flatMap((index) =>
			((session!.peers[index] as any)?.getMultiaddrs?.() ?? [])
				.slice(0, 1)
				.map((ma: any) => ma?.toString?.())
				.filter((x: unknown): x is string => typeof x === "string" && x.length > 0),
		);
		if (bootstrapAddrs.length > 0) {
			console.log(`bootstraps selected: ${bootstrapAddrs.length} peers`);
		}
		for (const peer of session.peers as any[]) {
			const fanout = peer?.services?.fanout as any;
			try {
				fanout?.setBootstraps?.(bootstrapAddrs);
			} catch {
				// ignore
			}
		}

		const topologyInfo = await configureTopology(session, args);
		console.log(
			`Connected topology: ${topologyInfo.topologyNote}, edges=${topologyInfo.edgeCount}`,
		);
		if (bootstrapAddrs.length > 0) {
			console.log(
				`join bootstraps configured: ${bootstrapAddrs.length} peers`,
			);
		}
		for (const peer of session.peers as any[]) {
			const pubsub = peer?.services?.pubsub as any;
			if (!pubsub) continue;
			pubsub.fanoutJoinOptions = {
				...(pubsub.fanoutJoinOptions ?? {}),
				timeoutMs: args.fanoutJoinTimeoutMs,
				joinReqTimeoutMs: args.joinReqTimeoutMs,
				joinAttemptsPerRound: args.joinAttemptsPerRound,
				trackerCandidates: args.trackerCandidates,
				trackerQueryTimeoutMs: args.trackerQueryTimeoutMs,
				candidateShuffleTopK: args.candidateShuffleTopK,
				candidateCooldownMs: args.candidateCooldownMs,
				bootstrap: bootstrapAddrs,
				bootstrapMaxPeers: Math.min(bootstrapAddrs.length, args.bootstrapMaxPeers),
			};
		}
		await Promise.all(
			bootstrapIndices.map(async (index) => {
				try {
					await (session!.peers[index] as any)?.services?.pubsub?.hostShardRootsNow?.();
				} catch {
					// ignore
				}
			}),
		);
		if (args.topology !== "full" && args.trackerWarmupMs > 0) {
			console.log(`tracker warmup: ${args.trackerWarmupMs} ms`);
			await sleep(args.trackerWarmupMs);
		}

		let address: string | undefined;
		const openOrder = buildOpenOrder(args, bootstrapIndices);
		let openCount = 0;
		for (const i of openOrder) {
			const peer = session.peers[i]!;
			const openArgs = {
				replicate: { factor: args.replicateFactor },
				timeUntilRoleMaturity: args.timeUntilRoleMaturity,
			};
			let store: TestStore | undefined;
			const maxAttempts = 3;
			for (let attempt = 1; attempt <= maxAttempts; attempt++) {
				try {
					store =
						address == null
							? await withTimeout(
									peer.open(new TestStore(), { args: openArgs }),
									args.openTimeoutMs,
									`open peer=${i} new-store`,
							  )
							: await withTimeout(
									peer.open<TestStore>(address, { args: openArgs }),
									args.openTimeoutMs,
									`open peer=${i} address=${address}`,
							  );
					break;
				} catch (error) {
					if (attempt >= maxAttempts) {
						throw error;
					}
					const waitMs = Math.min(5_000, attempt * 1_000);
					console.log(
						`open retry peer=${i} attempt=${attempt}/${maxAttempts} wait=${waitMs}ms`,
					);
					await sleep(waitMs);
				}
			}
			if (!store) {
				throw new Error(`Failed to open store on peer ${i}`);
			}
			address = store.address;
			stores[i] = store;
			openCount += 1;
			if (
				openCount === 1 ||
				openCount === args.nodes ||
				openCount % Math.max(1, Math.floor(args.nodes / 20)) === 0
			) {
				console.log(`opened stores ${openCount}/${args.nodes}`);
			}
			if (
				args.admissionBatchSize > 0 &&
				openCount < args.nodes &&
				openCount % args.admissionBatchSize === 0 &&
				args.admissionPauseMs > 0
			) {
				console.log(`admission pause: ${args.admissionPauseMs} ms`);
				await sleep(args.admissionPauseMs);
			} else {
				await sleep(25);
			}
		}

		const writerStore = stores[args.writer]!;

		const settle = (index: number, docId: string) => {
			const tracker = trackers.get(docId);
			if (!tracker) return;
			if (!tracker.pendingTargets.has(index)) return;

			tracker.pendingTargets.delete(index);
			perNodeReceivedCount[index] += 1;

			if (tracker.firstRemoteMs == null) {
				tracker.firstRemoteMs = performance.now() - tracker.startMs;
			}

			if (tracker.pendingTargets.size === 0) {
				clearTimeout(tracker.timer);
				const latency: DocLatency = {
					id: docId,
					firstRemoteMs: tracker.firstRemoteMs,
					convergedMs: performance.now() - tracker.startMs,
					timedOut: false,
					targetsReached: tracker.targetsTotal,
					targetsTotal: tracker.targetsTotal,
				};
				trackers.delete(docId);
				tracker.resolve(latency);
			}
		};

		stores.forEach((store, index) => {
			if (!store) return;
			const listener = (event: any) => {
				for (const added of event.detail?.added ?? []) {
					if (typeof added?.id === "string") {
						settle(index, added.id);
					}
				}
			};
			store.docs.events.addEventListener("change", listener);
			listeners.push(() => store.docs.events.removeEventListener("change", listener));
		});

		const measureOne = async (kind: "warmup" | "measure", seq: number): Promise<DocLatency> => {
			const id = uuid();
			const payload = payloadTemplate.length > 0 ? `${payloadTemplate}-${seq}` : undefined;
			const doc = new Document({ id, payload });
			const startMs = performance.now();
			const pendingTargets = new Set(targets);

			if (pendingTargets.size === 0) {
				await writerStore.docs.put(doc, { unique: true });
				return {
					id,
					firstRemoteMs: 0,
					convergedMs: 0,
					timedOut: false,
					targetsReached: 0,
					targetsTotal: 0,
				};
			}

			const done = new Promise<DocLatency>((resolve) => {
				const timer = setTimeout(() => {
					const remaining = pendingTargets.size;
					const reached = targets.size - remaining;
					const latency: DocLatency = {
						id,
						firstRemoteMs:
							reached > 0 ? performance.now() - startMs : null,
						convergedMs: null,
						timedOut: true,
						targetsReached: reached,
						targetsTotal: targets.size,
					};
					trackers.delete(id);
					resolve(latency);
				}, args.timeoutMs);

				trackers.set(id, {
					startMs,
					pendingTargets,
					targetsTotal: targets.size,
					firstRemoteMs: null,
					resolve,
					timer,
				});
			});

			await writerStore.docs.put(doc, { unique: true });
			const result = await done;
			if (kind === "warmup") {
				const warmupStatus = result.timedOut ? "timeout" : "ok";
				console.log(
					`warmup ${seq + 1}/${args.warmup}: ${warmupStatus}, reached=${result.targetsReached}/${result.targetsTotal}`,
				);
			}
			return result;
		};

		for (let i = 0; i < args.warmup; i++) {
			await measureOne("warmup", i);
		}

		const measured: DocLatency[] = [];
		const inFlight = new Set<Promise<void>>();
		let started = 0;
		const benchmarkStart = performance.now();

		const startOne = (index: number) => {
			const p = measureOne("measure", index)
				.then((result) => {
					measured.push(result);
				})
				.finally(() => {
					inFlight.delete(p);
				});
			inFlight.add(p);
		};

		while (started < args.docs) {
			while (started < args.docs && inFlight.size < args.concurrency) {
				startOne(started);
				started += 1;
			}
			if (inFlight.size > 0) {
				await Promise.race(inFlight);
			}
		}
		await Promise.all(inFlight);
		const benchmarkEnd = performance.now();

		const firstRemoteValues = measured
			.filter((x) => x.firstRemoteMs != null && !x.timedOut)
			.map((x) => x.firstRemoteMs as number);
		const convergedValues = measured
			.filter((x) => x.convergedMs != null && !x.timedOut)
			.map((x) => x.convergedMs as number);
		const timeouts = measured.filter((x) => x.timedOut);
		const timeoutCount = timeouts.length;
		const successCount = measured.length - timeoutCount;
		const durationSeconds = (benchmarkEnd - benchmarkStart) / 1000;
		const docsPerSecond = measured.length / Math.max(1e-9, durationSeconds);
		const deliveredCopies = measured.reduce((sum, x) => sum + x.targetsReached, 0);
		const copyDeliveriesPerSecond =
			deliveredCopies / Math.max(1e-9, durationSeconds);
		const bytesReplicated =
			deliveredCopies * Math.max(0, args.payloadBytes);
		const bytesPerSecond = bytesReplicated / Math.max(1e-9, durationSeconds);

		const firstStats = stats(firstRemoteValues);
		const convergeStats = stats(convergedValues);

		console.log("");
		console.log("Results");
		console.log(`  measured docs: ${measured.length}`);
		console.log(`  succeeded: ${successCount}`);
		console.log(`  timed out: ${timeoutCount}`);
		console.log(`  duration: ${(benchmarkEnd - benchmarkStart).toFixed(2)} ms`);
		console.log(`  throughput (docs): ${formatRate(docsPerSecond)}`);
		console.log(`  throughput (replicated copies): ${formatRate(copyDeliveriesPerSecond)}`);
		console.log(`  throughput (payload bytes only): ${formatRate(bytesPerSecond)}`);
		console.log("");
		console.log("Latency to first remote replica");
		console.log(`  count: ${firstStats.count}`);
		console.log(`  min: ${formatMs(firstStats.min)}`);
		console.log(`  mean: ${formatMs(firstStats.mean)}`);
		console.log(`  p50: ${formatMs(firstStats.p50)}`);
		console.log(`  p95: ${formatMs(firstStats.p95)}`);
		console.log(`  p99: ${formatMs(firstStats.p99)}`);
		console.log(`  max: ${formatMs(firstStats.max)}`);
		console.log("");
		console.log("Latency to full convergence (all target peers)");
		console.log(`  count: ${convergeStats.count}`);
		console.log(`  min: ${formatMs(convergeStats.min)}`);
		console.log(`  mean: ${formatMs(convergeStats.mean)}`);
		console.log(`  p50: ${formatMs(convergeStats.p50)}`);
		console.log(`  p95: ${formatMs(convergeStats.p95)}`);
		console.log(`  p99: ${formatMs(convergeStats.p99)}`);
		console.log(`  max: ${formatMs(convergeStats.max)}`);
		console.log("");
		const remoteCounts = perNodeReceivedCount.filter((_, i) => i !== args.writer);
		const remoteMin = remoteCounts.length > 0 ? Math.min(...remoteCounts) : 0;
		const remoteMax = remoteCounts.length > 0 ? Math.max(...remoteCounts) : 0;
		const remoteAvg =
			remoteCounts.length > 0
				? remoteCounts.reduce((a, b) => a + b, 0) / remoteCounts.length
				: 0;
		console.log("Per-remote-node received docs");
		console.log(`  min: ${remoteMin}`);
		console.log(`  mean: ${remoteAvg.toFixed(2)}`);
		console.log(`  max: ${remoteMax}`);

		if (timeoutCount > 0) {
			console.log("");
			console.log("Timeout samples");
			for (const sample of timeouts.slice(0, 10)) {
				console.log(
					`  id=${sample.id} reached=${sample.targetsReached}/${sample.targetsTotal}`,
				);
			}
			if (timeouts.length > 10) {
				console.log(`  ... ${timeouts.length - 10} more`);
			}
		}

		if (args.failOnTimeout && timeoutCount > 0) {
			throw new Error(
				`Benchmark had ${timeoutCount} timeouts out of ${measured.length} measured docs`,
			);
		}
	} finally {
		for (const cleanup of listeners) {
			try {
				cleanup();
			} catch {
				// ignore cleanup errors in benchmark mode
			}
		}
		for (const tracker of trackers.values()) {
			clearTimeout(tracker.timer);
		}
		trackers.clear();
		await Promise.all(
			stores.map(async (store) => {
				if (!store) return;
				try {
					await store.close();
				} catch {
					// ignore close errors in benchmark mode
				}
			}),
		);
		stores = [];
		if (session) {
			try {
				await session.stop();
			} catch {
				// ignore stop errors in benchmark mode
			}
		}
	}
};

try {
	await main();
	process.exit(0);
} catch (error) {
	console.error(error);
	process.exit(1);
}
