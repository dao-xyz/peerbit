import type { PeerId } from "@libp2p/interface";
import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import { AcknowledgeAnyWhere, SilentDelivery } from "@peerbit/stream-interface";
import { delay } from "@peerbit/time";
import { TopicControlPlane, type TopicControlPlaneComponents } from "../src/index.js";
import {
	InMemoryConnectionManager,
	InMemoryNetwork,
} from "./sim/inmemory-libp2p.js";

export type PubsubTopicSimParams = {
	nodes: number;
	degree: number;
	writerIndex: number;
	subscribers: number;

	messages: number;
	msgSize: number;
	intervalMs: number;

	/**
	 * When true, publish with `SilentDelivery` so the bench can continue even when
	 * some recipients are temporarily unreachable (e.g. under churn).
	 *
	 * Note: TopicControlPlane still embeds an explicit receiver set in the message header
	 * (from `topicsToPeers`), so this is not a scalable 1->1M mode. It's here to
	 * exercise the real stream routing under load.
	 */
	silent: boolean;
	redundancy: number;

	seed: number;
	topic: string;
	subscribeModel: "real" | "preseed";
	subscriptionDebounceDelayMs: number;

	warmupMs: number;
	warmupMessages: number;
	settleMs: number;
	timeoutMs: number;

	dialConcurrency: number;
	dialer: boolean;
	pruner: boolean;
	prunerIntervalMs: number;
	prunerMaxBufferBytes: number;

	dialDelayMs: number;
	streamRxDelayMs: number;
	streamHighWaterMarkBytes: number;
	dropDataFrameRate: number;
	maxLatencySamples: number;

	churnEveryMs: number;
	churnDownMs: number;
	churnFraction: number;
	churnRedialIntervalMs: number;
	churnRedialConcurrency: number;
};

export type PubsubTopicSimResult = {
	params: PubsubTopicSimParams;

	subscriberCount: number;
	writerKnown: number;
	subscribeMs: number;
	warmupMs: number;

	publishMs: number;
	expected: number;
	expectedOnline: number;
	deliveredUnique: number;
	deliveredPct: number;
	deliveredOnlinePct: number;
	duplicates: number;
	publishErrors: number;

	latencySamples: number;
	latencyP50: number;
	latencyP95: number;
	latencyP99: number;
	latencyMax: number;

	modeToLenAvg: number;
	modeToLenMax: number;
	modeToLenSamples: number;

	churnEvents: number;
	churnedPeersTotal: number;

	connectionsNow: number;
	avgNeighbours: number;
	avgRoutes: number;
	avgQueuedBytes: number;
	maxQueuedBytes: number;

	framesSent: number;
	dataFramesSent: number;
	ackFramesSent: number;
	goodbyeFramesSent: number;
	otherFramesSent: number;
	framesDropped: number;
	dataFramesDropped: number;
	bytesSent: number;
	bytesDropped: number;

	memoryRssMiB: number;
	memoryHeapUsedMiB: number;
	memoryHeapTotalMiB: number;
};

const BENCH_ID_PREFIX = Uint8Array.from([0x50, 0x53, 0x49, 0x4d]); // "PSIM"

const isBenchId = (id: Uint8Array) =>
	id.length === 32 &&
	id[0] === BENCH_ID_PREFIX[0] &&
	id[1] === BENCH_ID_PREFIX[1] &&
	id[2] === BENCH_ID_PREFIX[2] &&
	id[3] === BENCH_ID_PREFIX[3];

const writeU32BE = (buf: Uint8Array, offset: number, value: number) => {
	buf[offset + 0] = (value >>> 24) & 0xff;
	buf[offset + 1] = (value >>> 16) & 0xff;
	buf[offset + 2] = (value >>> 8) & 0xff;
	buf[offset + 3] = value & 0xff;
};

const readU32BE = (buf: Uint8Array, offset: number) =>
	((buf[offset + 0] << 24) |
		(buf[offset + 1] << 16) |
		(buf[offset + 2] << 8) |
		buf[offset + 3]) >>> 0;

const mulberry32 = (seed: number) => {
	let t = seed >>> 0;
	return () => {
		t += 0x6d2b79f5;
		let x = t;
		x = Math.imul(x ^ (x >>> 15), x | 1);
		x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
		return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
	};
};

const int = (rng: () => number, maxExclusive: number) =>
	Math.floor(rng() * maxExclusive);

const pickDistinct = (
	rng: () => number,
	n: number,
	k: number,
	exclude: number,
): number[] => {
	const max = Math.max(0, Math.min(n - 1, Math.floor(k)));
	if (max === 0) return [];
	if (max >= n - 1) {
		const out: number[] = [];
		for (let i = 0; i < n; i++) if (i !== exclude) out.push(i);
		return out;
	}

	const pool: number[] = [];
	for (let i = 0; i < n; i++) {
		if (i === exclude) continue;
		pool.push(i);
	}
	// Partial Fisher–Yates shuffle (only first k positions).
	for (let i = 0; i < max; i++) {
		const j = i + int(rng, pool.length - i);
		const tmp = pool[i]!;
		pool[i] = pool[j]!;
		pool[j] = tmp;
	}
	pool.length = max;
	return pool;
};

const buildRandomGraph = (
	n: number,
	targetDegree: number,
	rng: () => number,
): number[][] => {
	if (n <= 0) throw new Error("nodes must be > 0");
	if (targetDegree < 0) throw new Error("degree must be >= 0");
	if (targetDegree >= n) {
		throw new Error("degree must be < nodes for a simple graph");
	}

	const adj: Set<number>[] = Array.from({ length: n }, () => new Set<number>());
	const degree = new Uint16Array(n);

	const connect = (a: number, b: number) => {
		if (a === b) return false;
		if (adj[a]!.has(b)) return false;
		if (degree[a]! >= targetDegree || degree[b]! >= targetDegree) return false;
		adj[a]!.add(b);
		adj[b]!.add(a);
		degree[a]! += 1;
		degree[b]! += 1;
		return true;
	};

	// Seed connectivity.
	if (targetDegree >= 2 && n >= 3) {
		for (let i = 0; i < n; i++) connect(i, (i + 1) % n);
	} else if (targetDegree >= 1 && n >= 2) {
		for (let i = 0; i < n - 1; i++) connect(i, i + 1);
	}

	const available: number[] = [];
	const pos = new Int32Array(n).fill(-1);
	for (let i = 0; i < n; i++) {
		if (degree[i]! < targetDegree) {
			pos[i] = available.length;
			available.push(i);
		}
	}
	const removeAvailable = (id: number) => {
		const p = pos[id]!;
		if (p < 0) return;
		const last = available.pop()!;
		if (last !== id) {
			available[p] = last;
			pos[last] = p;
		}
		pos[id] = -1;
	};

	const maxAttempts = n * Math.max(1, targetDegree) * 200;
	let attempts = 0;
	while (available.length > 1 && attempts < maxAttempts) {
		attempts++;
		const a = available[int(rng, available.length)]!;
		const b = available[int(rng, available.length)]!;
		if (a === b) continue;
		if (!connect(a, b)) continue;
		if (degree[a]! >= targetDegree) removeAvailable(a);
		if (degree[b]! >= targetDegree) removeAvailable(b);
	}

	return adj.map((s) => [...s]);
};

const runWithConcurrency = async <T>(
	tasks: Array<() => Promise<T>>,
	concurrency: number,
): Promise<T[]> => {
	const results: T[] = new Array(tasks.length);
	let index = 0;
	const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
		for (;;) {
			const i = index++;
			if (i >= tasks.length) return;
			results[i] = await tasks[i]!();
		}
	});
	await Promise.all(workers);
	return results;
};

const waitForProtocolStreams = async (
	peers: { sub: TopicControlPlane }[],
	timeoutMs = 30_000,
) => {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		let missing = 0;
		for (const p of peers) {
			const protocols = p.sub.multicodecs;
			for (const conn of p.sub.components.connectionManager.getConnections()) {
				const streams = conn.streams as any as Array<{
					protocol?: string;
					direction?: string;
				}>;
				const hasOutbound = streams.some(
					(s) =>
						s.protocol &&
						protocols.includes(s.protocol) &&
						s.direction === "outbound",
				);
				const hasInbound = streams.some(
					(s) =>
						s.protocol &&
						protocols.includes(s.protocol) &&
						s.direction === "inbound",
				);
				if (!hasOutbound || !hasInbound) missing++;
			}
		}
		if (missing === 0) return;
		await delay(0);
	}
	throw new Error("Timeout waiting for protocol streams to become duplex");
};

class SimTopicControlPlane extends TopicControlPlane {
	constructor(
		c: TopicControlPlaneComponents,
		opts: {
			dialer: boolean;
			pruner: boolean;
			prunerIntervalMs: number;
			prunerMaxBufferBytes: number;
			subscriptionDebounceDelayMs: number;
		},
		private readonly recordModeToLen?: (len: number) => void,
	) {
		super(c, {
			canRelayMessage: true,
			subscriptionDebounceDelay: opts.subscriptionDebounceDelayMs,
			connectionManager: {
				dialer: opts.dialer ? { retryDelay: 1_000 } : false,
				pruner: opts.pruner
					? {
							interval: opts.prunerIntervalMs,
							maxBuffer: opts.prunerMaxBufferBytes,
						}
					: false,
				maxConnections: Number.MAX_SAFE_INTEGER,
				minConnections: 0,
			},
		});

		// Fast/mock signing: keep signer identity semantics but skip crypto work.
		this.sign = async () =>
			new SignatureWithKey({
				signature: new Uint8Array([0]),
				publicKey: this.publicKey,
				prehash: PreHash.NONE,
			});
	}

	public async verifyAndProcess(message: any) {
		// Skip expensive crypto verify for large sims, but keep session handling
		// behavior consistent with the real implementation.
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}

	public async publishMessage(
		from: any,
		message: any,
		to?: any,
		relayed?: boolean,
		signal?: AbortSignal,
	): Promise<void> {
		if (this.recordModeToLen && message?.id instanceof Uint8Array && isBenchId(message.id)) {
			const mode = message?.header?.mode;
			const toLen = Array.isArray(mode?.to) ? mode.to.length : undefined;
			if (toLen != null) this.recordModeToLen(toLen);
		}
		return super.publishMessage(from, message, to, relayed, signal);
	}
}

const quantile = (sorted: number[], q: number) => {
	if (sorted.length === 0) return NaN;
	const idx = Math.min(
		sorted.length - 1,
		Math.max(0, Math.floor(q * (sorted.length - 1))),
	);
	return sorted[idx]!;
};

const clampInt = (value: number, min: number, max: number) =>
	Math.max(min, Math.min(max, Math.floor(value)));

export const resolvePubsubTopicSimParams = (
	input: Partial<PubsubTopicSimParams>,
): PubsubTopicSimParams => {
	const nodes = clampInt(Number(input.nodes ?? 2000), 1, 1_000_000_000);
	const degree = clampInt(Number(input.degree ?? 6), 0, Math.max(0, nodes - 1));
	const writerIndex = clampInt(Number(input.writerIndex ?? 0), 0, nodes - 1);
	const subscribers = clampInt(Number(input.subscribers ?? nodes - 1), 0, nodes - 1);

	return {
		nodes,
		degree,
		writerIndex,
		subscribers,

		messages: clampInt(Number(input.messages ?? 200), 0, 1_000_000_000),
		msgSize: clampInt(Number(input.msgSize ?? 1024), 0, 1_000_000_000),
		intervalMs: clampInt(Number(input.intervalMs ?? 0), 0, 1_000_000_000),

		silent: Boolean(input.silent ?? false),
		redundancy: clampInt(Number(input.redundancy ?? 2), 0, 1_000_000_000),

		seed: clampInt(Number(input.seed ?? 1), 0, 0xffff_ffff),
		topic: String(input.topic ?? "concert"),
		subscribeModel: (input.subscribeModel ?? "real") as PubsubTopicSimParams["subscribeModel"],
		subscriptionDebounceDelayMs: clampInt(
			Number(input.subscriptionDebounceDelayMs ?? 0),
			0,
			1_000_000_000,
		),

		warmupMs: clampInt(Number(input.warmupMs ?? 0), 0, 1_000_000_000),
		warmupMessages: clampInt(Number(input.warmupMessages ?? 0), 0, 1_000_000_000),
		settleMs: clampInt(Number(input.settleMs ?? 200), 0, 1_000_000_000),
		timeoutMs: clampInt(Number(input.timeoutMs ?? 300_000), 0, 1_000_000_000),

		dialConcurrency: clampInt(Number(input.dialConcurrency ?? 256), 1, 1_000_000_000),
		dialer: Boolean(input.dialer ?? false),
		pruner: Boolean(input.pruner ?? false),
		prunerIntervalMs: clampInt(Number(input.prunerIntervalMs ?? 50), 1, 1_000_000_000),
		prunerMaxBufferBytes: clampInt(
			Number(input.prunerMaxBufferBytes ?? 64 * 1024),
			0,
			1_000_000_000,
		),

		dialDelayMs: clampInt(Number(input.dialDelayMs ?? 0), 0, 1_000_000_000),
		streamRxDelayMs: clampInt(Number(input.streamRxDelayMs ?? 0), 0, 1_000_000_000),
		streamHighWaterMarkBytes: clampInt(
			Number(input.streamHighWaterMarkBytes ?? 256 * 1024),
			0,
			1_000_000_000,
		),
		dropDataFrameRate: Math.max(0, Math.min(1, Number(input.dropDataFrameRate ?? 0))),
		maxLatencySamples: clampInt(Number(input.maxLatencySamples ?? 1_000_000), 0, 1_000_000_000),

		churnEveryMs: clampInt(Number(input.churnEveryMs ?? 0), 0, 1_000_000_000),
		churnDownMs: clampInt(Number(input.churnDownMs ?? 0), 0, 1_000_000_000),
		churnFraction: Math.max(0, Math.min(1, Number(input.churnFraction ?? 0))),
		churnRedialIntervalMs: clampInt(Number(input.churnRedialIntervalMs ?? 50), 0, 1_000_000_000),
		churnRedialConcurrency: clampInt(
			Number(input.churnRedialConcurrency ?? 128),
			1,
			1_000_000_000,
		),
	};
};

export const formatPubsubTopicSimResult = (r: PubsubTopicSimResult) => {
	const p = r.params;
	const lines: string[] = [];
	lines.push("pubsub pubsub-topic-sim results");
	lines.push(`- nodes: ${p.nodes}, degree: ${p.degree}`);
	lines.push(
		`- writerIndex: ${p.writerIndex}, subscribers: ${r.subscriberCount}, topic: ${p.topic}, silent: ${p.silent ? "on" : "off"}`,
	);
	lines.push(
		`- messages: ${p.messages}, msgSize: ${p.msgSize}B, intervalMs: ${p.intervalMs}, redundancy: ${p.redundancy}`,
	);
	lines.push(
		`- dialer: ${p.dialer ? "on" : "off"}, pruner: ${p.pruner ? "on" : "off"} (interval=${p.prunerIntervalMs}ms, maxBuffer=${p.prunerMaxBufferBytes}B)`,
	);
	lines.push(
		`- transport: dialDelay=${p.dialDelayMs}ms, rxDelay=${p.streamRxDelayMs}ms, hwm=${p.streamHighWaterMarkBytes}B`,
	);
	lines.push(
		`- subscribe: model=${p.subscribeModel}, requested=${r.subscriberCount}, writerKnown=${r.writerKnown}, time=${r.subscribeMs}ms`,
	);
	lines.push(
		`- churn: everyMs=${p.churnEveryMs} downMs=${p.churnDownMs} fraction=${p.churnFraction} events=${r.churnEvents} peers=${r.churnedPeersTotal}`,
	);
	lines.push(
		`- publish: time=${r.publishMs}ms, expected=${r.expected}, observedUnique=${r.deliveredUnique} (${r.deliveredPct.toFixed(2)}%), observedOnline=${r.expectedOnline} (${r.deliveredOnlinePct.toFixed(2)}%), duplicates=${r.duplicates}, publishErrors=${r.publishErrors}`,
	);
	if (r.latencySamples > 0) {
		lines.push(
			`- latency ms (sample n=${r.latencySamples}): p50=${r.latencyP50.toFixed(1)}, p95=${r.latencyP95.toFixed(1)}, p99=${r.latencyP99.toFixed(1)}, max=${r.latencyMax.toFixed(1)}`,
		);
	} else {
		lines.push(`- latency ms: (no samples)`);
	}
	lines.push(
		`- mode.to length (writer bench msgs): avg=${r.modeToLenAvg.toFixed(1)}, max=${r.modeToLenMax}, samples=${r.modeToLenSamples}`,
	);
	lines.push(
		`- connections: now=${r.connectionsNow} (avg neighbours/node=${r.avgNeighbours.toFixed(2)})`,
	);
	lines.push(`- routes: avg/node=${r.avgRoutes.toFixed(2)}`);
	lines.push(
		`- queuedBytes: avg/node=${r.avgQueuedBytes.toFixed(0)}, max/node=${r.maxQueuedBytes.toFixed(0)}`,
	);
	lines.push(
		`- frames: total=${r.framesSent} (data=${r.dataFramesSent}, ack=${r.ackFramesSent}, goodbye=${r.goodbyeFramesSent}, other=${r.otherFramesSent})`,
	);
	lines.push(`- drops: frames=${r.framesDropped} (data=${r.dataFramesDropped}), bytes=${r.bytesDropped}`);
	lines.push(`- bytes sent (framed): ${r.bytesSent}`);
	lines.push(
		`- memory: rss=${r.memoryRssMiB}MiB heapUsed=${r.memoryHeapUsedMiB}MiB heapTotal=${r.memoryHeapTotalMiB}MiB`,
	);
	return lines.join("\n");
};

export const runPubsubTopicSim = async (
	input: Partial<PubsubTopicSimParams>,
): Promise<PubsubTopicSimResult> => {
	const params = resolvePubsubTopicSimParams(input);
	const timeoutMs = Math.max(0, params.timeoutMs);
	const timeoutController = new AbortController();
	const timeoutSignal = timeoutController.signal;

	let timeout: ReturnType<typeof setTimeout> | undefined;
	if (timeoutMs > 0) {
		timeout = setTimeout(() => {
			timeoutController.abort(
				new Error(
					`pubsub-topic-sim timed out after ${timeoutMs}ms (override with --timeoutMs)`,
				),
			);
		}, timeoutMs);
	}

	const rng = mulberry32(params.seed);
	const nodes = params.nodes;
	const subscriberCount = clampInt(params.subscribers, 0, Math.max(0, nodes - 1));

	let churnEvents = 0;
	let churnedPeersTotal = 0;

	let publishErrors = 0;

	let modeToLenCount = 0;
	let modeToLenSum = 0;
	let modeToLenMax = 0;
	const recordModeToLen = (len: number) => {
		modeToLenCount += 1;
		modeToLenSum += len;
		if (len > modeToLenMax) modeToLenMax = len;
	};

	const graph = buildRandomGraph(params.nodes, params.degree, rng);

	const network = new InMemoryNetwork({
		streamRxDelayMs: params.streamRxDelayMs,
		streamHighWaterMarkBytes: params.streamHighWaterMarkBytes,
		dialDelayMs: params.dialDelayMs,
		dropDataFrameRate: params.dropDataFrameRate,
		dropSeed: params.seed,
	});

	const peers: {
		peerId: PeerId;
		sub: SimTopicControlPlane;
	}[] = [];

	try {
		const basePort = 40_000;
		for (let i = 0; i < params.nodes; i++) {
			const port = basePort + i;
			const { runtime } = InMemoryNetwork.createPeer({ index: i, port, network });
			runtime.connectionManager = new InMemoryConnectionManager(network, runtime);
			network.registerPeer(runtime, port);

			const components: TopicControlPlaneComponents = {
				peerId: runtime.peerId,
				privateKey: runtime.privateKey,
				addressManager: runtime.addressManager as any,
				registrar: runtime.registrar as any,
				connectionManager: runtime.connectionManager as any,
				peerStore: runtime.peerStore as any,
				events: runtime.events,
			};

			const record = i === params.writerIndex ? recordModeToLen : undefined;
			peers.push({
				peerId: runtime.peerId,
				sub: new SimTopicControlPlane(
					components,
					{
						dialer: params.dialer,
						pruner: params.pruner,
						prunerIntervalMs: params.prunerIntervalMs,
						prunerMaxBufferBytes: params.prunerMaxBufferBytes,
						subscriptionDebounceDelayMs: params.subscriptionDebounceDelayMs,
					},
					record,
				),
			});
		}

		await Promise.all(peers.map((p) => p.sub.start()));

		// Establish initial graph via dials (bounded concurrency).
		const addrs = peers.map(
			(p) => (p.sub.components.addressManager as any).getAddresses()[0] as any,
		);
		const dialTasks: Array<() => Promise<void>> = [];
		for (let a = 0; a < graph.length; a++) {
			for (const b of graph[a]!) {
				if (b <= a) continue;
				const addrB = addrs[b]!;
				dialTasks.push(async () => {
					await peers[a]!.sub.components.connectionManager.openConnection(addrB);
				});
			}
		}
		await runWithConcurrency(dialTasks, params.dialConcurrency);

		await waitForProtocolStreams(peers);

		const writer = peers[params.writerIndex]!.sub;
		const subscriberIndices = pickDistinct(
			rng,
			params.nodes,
			subscriberCount,
			params.writerIndex,
		);

		// Subscribe
		const subscribeStart = Date.now();
		if (params.subscribeModel === "preseed") {
			for (const idx of subscriberIndices) {
				const node = peers[idx]!.sub;
				(node as any).listenForSubscribers?.(params.topic);
				node.subscriptions.set(params.topic, { counter: 1 });
			}

			// Ensure the writer can publish without first running the subscription gossip.
			(writer as any).listenForSubscribers?.(params.topic);
			const subscriberHashes = subscriberIndices.map(
				(i) => peers[i]!.sub.publicKeyHash,
			);
			writer.topicsToPeers.set(params.topic, new Set(subscriberHashes));
		} else {
			// Ensure the writer is actually listening for subscribers.
			void writer.requestSubscribers(params.topic).catch(() => {});

			// Subscribe (real protocol path)
			await Promise.all(
				subscriberIndices.map(async (idx) => {
					await peers[idx]!.sub.subscribe(params.topic);
				}),
			);

			// Wait for writer to learn subscribers for the topic (best-effort).
			const subTimeoutMs = 120_000;
			const subWaitStart = Date.now();
			let lastRequest = 0;
			while (Date.now() - subWaitStart < subTimeoutMs) {
				if (timeoutSignal.aborted) {
					throw timeoutSignal.reason ?? new Error("pubsub-topic-sim aborted");
				}
				const known = writer.topicsToPeers.get(params.topic)?.size ?? 0;
				if (known >= subscriberCount) break;
				const now = Date.now();
				if (now - lastRequest >= 1_000) {
					lastRequest = now;
					void writer.requestSubscribers(params.topic).catch(() => {});
				}
				await delay(10, { signal: timeoutSignal });
			}
		}
		const subscribeDone = Date.now();

		if (params.warmupMs > 0) {
			await delay(params.warmupMs, { signal: timeoutSignal });
		}

		if (params.warmupMessages > 0) {
			// Route warmup: SilentDelivery is economical but does not learn routes.
			// Prime routing using acknowledged anywhere-probes (no explicit `to=[all]`).
			const subscriberHashes = subscriberIndices.map(
				(i) => peers[i]!.sub.publicKeyHash,
			);
			// Use redundancy=1 so every ACK we learn is distance=0 ("closest path"),
			// which enables SilentDelivery routing (origin otherwise falls back to flooding).
			const probeRedundancy = 1;
			for (let i = 0; i < params.warmupMessages; i++) {
				if (timeoutSignal.aborted) {
					throw timeoutSignal.reason ?? new Error("pubsub-topic-sim aborted");
				}
				const msg = await (writer as any).createMessage(undefined, {
					mode: new AcknowledgeAnyWhere({ redundancy: probeRedundancy }),
				});
				await writer.publishMessage(
					writer.publicKey,
					msg,
					undefined,
					undefined,
					timeoutSignal,
				);
				// Yield so route updates/acks can be processed between probes.
				await delay(0, { signal: timeoutSignal });
			}
			if (subscriberHashes.length > 0) {
				await writer.waitFor(subscriberHashes, {
					settle: "all",
					timeout: 30_000,
					signal: timeoutSignal,
				});
			}
		}

		// Delivery + latency tracking (unique deliveries per subscriber/seq).
		const sendTimes = new Float64Array(params.messages);
		const receivedFlags = subscriberIndices.map(
			() => new Uint8Array(params.messages),
		);

		let expectedOnline = 0;
		let deliveredUnique = 0;
		let duplicates = 0;

		let deliveredSamplesSeen = 0;
		const samples: number[] = [];
		const maxSamples = Math.max(0, params.maxLatencySamples);
		const recordLatency = (ms: number) => {
			deliveredSamplesSeen += 1;
			if (samples.length < maxSamples) {
				samples.push(ms);
				return;
			}
			if (maxSamples === 0) return;
			const j = int(rng, deliveredSamplesSeen);
			if (j < maxSamples) samples[j] = ms;
		};

		for (let s = 0; s < subscriberIndices.length; s++) {
			const idx = subscriberIndices[s]!;
			const node = peers[idx]!.sub;
			node.addEventListener("data", (ev: any) => {
				const msg = ev?.detail?.message;
				const id = msg?.id;
				if (!(id instanceof Uint8Array) || !isBenchId(id)) return;
				const seq = readU32BE(id, 4);
				if (seq >= params.messages) return;
				const t0 = sendTimes[seq];
				if (!t0) return;

				const flags = receivedFlags[s]!;
				if (flags[seq] === 1) {
					duplicates += 1;
					return;
				}
				flags[seq] = 1;
				deliveredUnique += 1;
				recordLatency(Date.now() - t0);
			});
		}

		// Churn + redial (keep the underlay graph roughly stable).
		const churnController = new AbortController();
		const churnSignal = churnController.signal;
		const redialController = new AbortController();
		const redialSignal = redialController.signal;

		const redialPending = new Map<number, number>(); // idx -> nextAttemptAt
		const scheduleRedial = (idx: number, at: number) => {
			const prev = redialPending.get(idx);
			if (prev == null || at < prev) redialPending.set(idx, at);
		};

		const churnLoop = async () => {
			const everyMs = Math.max(0, Math.floor(params.churnEveryMs));
			const downMs = Math.max(0, Math.floor(params.churnDownMs));
			const fraction = Math.max(0, Math.min(1, Number(params.churnFraction)));
			if (everyMs <= 0 || downMs <= 0 || fraction <= 0) return;
			if (subscriberIndices.length === 0) return;

			for (;;) {
				if (churnSignal.aborted) return;
				await delay(everyMs, { signal: churnSignal });
				if (churnSignal.aborted) return;
				if (timeoutSignal.aborted) return;

				const target = Math.min(
					subscriberIndices.length,
					Math.max(1, Math.floor(subscriberIndices.length * fraction)),
				);
				const chosen = new Set<number>();
				const maxAttempts = Math.max(10, target * 20);
				for (let tries = 0; chosen.size < target && tries < maxAttempts; tries++) {
					const idx = subscriberIndices[int(rng, subscriberIndices.length)]!;
					const peer = peers[idx]!;
					if (network.isPeerOffline(peer.peerId)) continue;
					chosen.add(idx);
				}
				if (chosen.size === 0) continue;

				churnEvents += 1;
				churnedPeersTotal += chosen.size;

				const now = Date.now();
				await Promise.all(
					[...chosen].map(async (idx) => {
						const peer = peers[idx]!;
						network.setPeerOffline(peer.peerId, downMs, now);
						await network.disconnectPeer(peer.peerId);
						scheduleRedial(idx, now + downMs + 1);
					}),
				);
			}
		};

		const redialLoop = async () => {
			const tickMs = Math.max(0, Math.floor(params.churnRedialIntervalMs));
			if (tickMs <= 0) return;
			const concurrency = Math.max(1, Math.floor(params.churnRedialConcurrency));
			for (;;) {
				if (redialSignal.aborted) return;
				await delay(tickMs, { signal: redialSignal });
				if (redialSignal.aborted) return;
				if (timeoutSignal.aborted) return;

				const now = Date.now();
				const ready: number[] = [];
				for (const [idx, at] of redialPending) {
					if (at <= now) ready.push(idx);
				}
				if (ready.length === 0) continue;

				const tasks: Array<() => Promise<void>> = [];
				for (const idx of ready) {
					const peer = peers[idx]!;
					if (network.isPeerOffline(peer.peerId, now)) {
						scheduleRedial(idx, now + tickMs);
						continue;
					}
					const cm = peer.sub.components.connectionManager;
					const neighbors = graph[idx] ?? [];
					let missing = 0;
					for (const nb of neighbors) {
						const other = peers[nb]!;
						if (network.isPeerOffline(other.peerId, now)) continue;
						const open = cm.getConnections(other.peerId).some((c: any) => c.status === "open");
						if (open) continue;
						missing += 1;
						const addrB = addrs[nb]!;
						tasks.push(async () => {
							try {
								await cm.openConnection(addrB);
							} catch {
								// ignored (offline or temporary no address)
							}
						});
					}
					if (missing > 0) {
						// Retry a little later until we have a stable degree again.
						scheduleRedial(idx, now + tickMs * 4);
					} else {
						redialPending.delete(idx);
					}
				}

				if (tasks.length > 0) {
					await runWithConcurrency(tasks, concurrency);
				}
			}
		};

		const churnPromise = churnLoop().catch(() => {});
		const redialPromise = redialLoop().catch(() => {});

		// Publish
		const payload = new Uint8Array(Math.max(0, params.msgSize));
		const publishStart = Date.now();
		try {
			for (let i = 0; i < params.messages; i++) {
				if (timeoutSignal.aborted) {
					throw timeoutSignal.reason ?? new Error("pubsub-topic-sim aborted");
				}

				const id = new Uint8Array(32);
				id.set(BENCH_ID_PREFIX, 0);
				writeU32BE(id, 4, i);

				const now = Date.now();
				sendTimes[i] = now;
				if (params.churnEveryMs > 0 && subscriberIndices.length > 0) {
					let online = 0;
					for (const idx of subscriberIndices) {
						if (!network.isPeerOffline(peers[idx]!.peerId, now)) online += 1;
					}
					expectedOnline += online;
				} else {
					expectedOnline += subscriberIndices.length;
				}

				try {
					if (params.silent) {
						const tos = [...writer.getPeersOnTopics([params.topic])];
						await writer.publish(payload, {
							id,
							topics: [params.topic],
							mode: new SilentDelivery({ to: tos, redundancy: params.redundancy }),
							signal: timeoutSignal,
						} as any);
					} else {
						await writer.publish(payload, {
							id,
							topics: [params.topic],
							signal: timeoutSignal,
						});
					}
				} catch {
					publishErrors += 1;
				}

				if (params.intervalMs > 0) {
					await delay(params.intervalMs, { signal: timeoutSignal });
				}
			}
		} finally {
			churnController.abort();
			redialController.abort();
			await churnPromise;
			await redialPromise;
			(churnSignal as any).clear?.();
			(redialSignal as any).clear?.();
		}

		// Give in-flight messages time to arrive
		if (params.settleMs > 0) {
			await delay(params.settleMs, { signal: timeoutSignal });
		}
		const publishDone = Date.now();

		// Aggregate stats
		const expected = subscriberIndices.length * params.messages;
		samples.sort((a, b) => a - b);

		let peerEdges = 0;
		let neighbourSum = 0;
		let routeSum = 0;
		let queuedSum = 0;
		let queuedMax = 0;

		for (const p of peers) {
			peerEdges += p.sub.components.connectionManager.getConnections().length;
			neighbourSum += p.sub.peers.size;
			routeSum += p.sub.routes.count();
			const queued = p.sub.getQueuedBytes();
			queuedSum += queued;
			if (queued > queuedMax) queuedMax = queued;
		}

		const m = network.metrics;
		const connectionsNow = peerEdges / 2;
		const avgNeighbours = neighbourSum / peers.length;
		const avgRoutes = routeSum / peers.length;
		const avgQueuedBytes = queuedSum / peers.length;

		const mem = process.memoryUsage();

		const deliveredPct = expected === 0 ? 100 : (100 * deliveredUnique) / expected;
		const deliveredOnlinePct =
			expectedOnline === 0 ? 100 : (100 * deliveredUnique) / expectedOnline;

		return {
			params,
			subscriberCount: subscriberIndices.length,
			writerKnown: writer.topicsToPeers.get(params.topic)?.size ?? 0,
			subscribeMs: subscribeDone - subscribeStart,
			warmupMs: params.warmupMs,

			publishMs: publishDone - publishStart,
			expected,
			expectedOnline,
			deliveredUnique,
			deliveredPct,
			deliveredOnlinePct,
			duplicates,
			publishErrors,

			latencySamples: samples.length,
			latencyP50: quantile(samples, 0.5),
			latencyP95: quantile(samples, 0.95),
			latencyP99: quantile(samples, 0.99),
			latencyMax: samples.length ? samples[samples.length - 1]! : NaN,

			modeToLenAvg: modeToLenCount ? modeToLenSum / modeToLenCount : 0,
			modeToLenMax,
			modeToLenSamples: modeToLenCount,

			churnEvents,
			churnedPeersTotal,

			connectionsNow,
			avgNeighbours,
			avgRoutes,
			avgQueuedBytes,
			maxQueuedBytes: queuedMax,

			framesSent: m.framesSent,
			dataFramesSent: m.dataFramesSent,
			ackFramesSent: m.ackFramesSent,
			goodbyeFramesSent: m.goodbyeFramesSent,
			otherFramesSent: m.otherFramesSent,
			framesDropped: m.framesDropped,
			dataFramesDropped: m.dataFramesDropped,
			bytesSent: m.bytesSent,
			bytesDropped: m.bytesDropped,

			memoryRssMiB: Math.round(mem.rss / 1024 / 1024),
			memoryHeapUsedMiB: Math.round(mem.heapUsed / 1024 / 1024),
			memoryHeapTotalMiB: Math.round(mem.heapTotal / 1024 / 1024),
		};
	} catch (e) {
		// `delay(..., { signal })` rejects with a generic AbortError (no message),
		// so surface the actual timeout reason if we aborted due to `timeoutMs`.
		if (timeoutSignal.aborted) {
			throw timeoutSignal.reason ?? e;
		}
		throw e;
	} finally {
		if (timeout) clearTimeout(timeout);
		await Promise.all(peers.map((p) => p.sub.stop().catch(() => {})));
	}
};
