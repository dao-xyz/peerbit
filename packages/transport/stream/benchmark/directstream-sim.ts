/**
 * In-memory "real DirectStream" simulator.
 *
 * Goal: run most of @peerbit/stream's real logic (routing, ACK learning, dialer,
 * pruning) but swap the underlying libp2p transport for a lightweight, in-memory
 * shim so we can explore 100s–1000s of nodes.
 *
 * Run:
 *   node --loader ts-node/esm ./packages/transport/stream/benchmark/directstream-sim.ts --nodes 200 --degree 4 --messages 20 --targets 5 --redundancy 2 --seed 1
 */

import type { PeerId } from "@libp2p/interface";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import { delay } from "@peerbit/time";
import { DirectStream, type DirectStreamComponents } from "../src/index.js";
import {
	InMemoryConnectionManager,
	InMemoryNetwork,
} from "./sim/inmemory-libp2p.js";

type SimParams = {
	nodes: number;
	degree: number;
	messages: number;
	targetsPerMessage: number;
	redundancy: number;
	seed: number;
	dialer: boolean;
	pruner: boolean;
	prunerIntervalMs: number;
	prunerMaxBufferBytes: number;
	dialDelayMs: number;
	streamRxDelayMs: number;
	streamHighWaterMarkBytes: number;
};

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
	const out = new Set<number>();
	while (out.size < k) {
		const candidate = int(rng, n);
		if (candidate === exclude) continue;
		out.add(candidate);
	}
	return [...out];
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

class SimDirectStream extends DirectStream {
	constructor(
		c: DirectStreamComponents,
		opts: {
			dialer: boolean;
			pruner: boolean;
			prunerIntervalMs: number;
			prunerMaxBufferBytes: number;
		},
	) {
		super(c, ["sim/stream/0.0.0"], {
			canRelayMessage: true,
			connectionManager: {
				// Keep it simple by default; can be expanded later.
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

		// Fast/mock signing: we only need the signer identity to flow through the
		// signatures list for routing semantics; crypto verification is skipped.
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
		if (this.peers.has(from.hashcode())) {
			// do nothing
		} else {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}
}

const parseArgs = (argv: string[]): SimParams => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(
			[
				"directstream-sim.ts",
				"",
				"Args:",
				"  --nodes N            number of nodes (default: 200)",
				"  --degree K           target undirected degree (default: 4)",
				"  --messages M         number of seek waves (default: 20)",
				"  --targets T          targets per message (default: 5)",
				"  --redundancy R       seek redundancy (default: 2)",
				"  --seed S             RNG seed (default: 1)",
				"  --dialer 0|1         enable autodial (default: 1)",
				"  --pruner 0|1         enable pruning (default: 0)",
				"  --prunerIntervalMs X pruner interval (default: 50)",
				"  --prunerMaxBufferBytes B prune when queued bytes > B (default: 65536)",
				"  --dialDelayMs X      artificial dial delay (default: 0)",
				"  --streamRxDelayMs X  per-chunk inbound delay in shim (default: 0)",
				"  --streamHighWaterMarkBytes B backpressure threshold (default: 262144)",
				"",
				"Example:",
				"  node --loader ts-node/esm ./packages/transport/stream/benchmark/directstream-sim.ts --nodes 500 --degree 6 --messages 50 --targets 20 --redundancy 2 --seed 42",
			].join("\n"),
		);
		process.exit(0);
	}

	return {
		nodes: Number(get("--nodes") ?? 200),
		degree: Number(get("--degree") ?? 4),
		messages: Number(get("--messages") ?? 20),
		targetsPerMessage: Number(get("--targets") ?? 5),
		redundancy: Number(get("--redundancy") ?? 2),
		seed: Number(get("--seed") ?? 1),
		dialer: String(get("--dialer") ?? "1") !== "0",
		pruner: String(get("--pruner") ?? "0") === "1",
		prunerIntervalMs: Number(get("--prunerIntervalMs") ?? 50),
		prunerMaxBufferBytes: Number(get("--prunerMaxBufferBytes") ?? 64 * 1024),
		dialDelayMs: Number(get("--dialDelayMs") ?? 0),
		streamRxDelayMs: Number(get("--streamRxDelayMs") ?? 0),
		streamHighWaterMarkBytes: Number(get("--streamHighWaterMarkBytes") ?? 256 * 1024),
	};
};

const waitForProtocolStreams = async (
	peers: { stream: DirectStream }[],
	timeoutMs = 30_000,
) => {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		let missing = 0;
		for (const p of peers) {
			const protocols = p.stream.multicodecs;
			for (const conn of p.stream.components.connectionManager.getConnections()) {
				const streams = conn.streams as any as Array<{
					protocol?: string;
					direction?: string;
				}>;
				const hasOutbound = streams.some(
					(s) => s.protocol && protocols.includes(s.protocol) && s.direction === "outbound",
				);
				const hasInbound = streams.some(
					(s) => s.protocol && protocols.includes(s.protocol) && s.direction === "inbound",
				);
				if (!hasOutbound || !hasInbound) missing++;
			}
		}
		if (missing === 0) return;
		await delay(0);
	}
	throw new Error("Timeout waiting for protocol streams to become duplex");
};

const main = async () => {
	const params = parseArgs(process.argv.slice(2));
	const rng = mulberry32(params.seed);
	const graph = buildRandomGraph(params.nodes, params.degree, rng);

	const network = new InMemoryNetwork({
		streamRxDelayMs: params.streamRxDelayMs,
		streamHighWaterMarkBytes: params.streamHighWaterMarkBytes,
		dialDelayMs: params.dialDelayMs,
	});

	const peers: {
		peerId: PeerId;
		stream: SimDirectStream;
	}[] = [];

	const basePort = 30_000;
	for (let i = 0; i < params.nodes; i++) {
		const port = basePort + i;
		const { runtime } = InMemoryNetwork.createPeer({ index: i, port, network });
		runtime.connectionManager = new InMemoryConnectionManager(network, runtime);
		network.registerPeer(runtime, port);

		const components: DirectStreamComponents = {
			peerId: runtime.peerId,
			privateKey: runtime.privateKey,
			addressManager: runtime.addressManager as any,
			registrar: runtime.registrar as any,
			connectionManager: runtime.connectionManager as any,
			peerStore: runtime.peerStore as any,
			events: runtime.events,
		};

		peers.push({
			peerId: runtime.peerId,
			stream: new SimDirectStream(components, {
				dialer: params.dialer,
				pruner: params.pruner,
				prunerIntervalMs: params.prunerIntervalMs,
				prunerMaxBufferBytes: params.prunerMaxBufferBytes,
			}),
		});
	}

	await Promise.all(peers.map((p) => p.stream.start()));

	// Establish initial graph via dials.
	for (let a = 0; a < graph.length; a++) {
		for (const b of graph[a]!) {
			if (b <= a) continue;
			const addrB = (peers[b]!.stream.components.addressManager as any).getAddresses()[0];
			await peers[a]!.stream.components.connectionManager.openConnection(addrB);
		}
	}

	await waitForProtocolStreams(peers);

	let delivered = 0;
	for (let i = 0; i < params.messages; i++) {
		const source = int(rng, params.nodes);
		const targets = pickDistinct(
			rng,
			params.nodes,
			Math.min(params.targetsPerMessage, params.nodes - 1),
			source,
		);
		const to = targets.map((t) => peers[t]!.stream.publicKey);

		await peers[source]!.stream.publish(new Uint8Array([1]), {
			mode: new AcknowledgeDelivery({
				to,
				redundancy: params.redundancy,
			}),
		});
		delivered += to.length;
	}

	// Allow any dialer follow-ups / pruning timers to run briefly.
	await delay(50);

	const m = network.metrics;
	let peerEdges = 0;
	let neighbourSum = 0;
	let routeSum = 0;
	for (const p of peers) {
		peerEdges += p.stream.components.connectionManager.getConnections().length;
		neighbourSum += p.stream.peers.size;
		routeSum += p.stream.routes.count();
	}
	const connectionsNow = peerEdges / 2;
	const avgNeighbours = neighbourSum / peers.length;
	const avgRoutes = routeSum / peers.length;

	const lines: string[] = [];
	lines.push("stream directstream-sim results");
	lines.push(`- nodes: ${params.nodes}, degree: ${params.degree}`);
	lines.push(
		`- messages: ${params.messages}, targets/message: ${params.targetsPerMessage}, redundancy: ${params.redundancy}`,
	);
	lines.push(
		`- dialer: ${params.dialer ? "on" : "off"}, pruner: ${
			params.pruner ? "on" : "off"
		} (interval=${params.prunerIntervalMs}ms, maxBuffer=${params.prunerMaxBufferBytes}B)`,
	);
	lines.push(
		`- transport: rxDelay=${params.streamRxDelayMs}ms, hwm=${params.streamHighWaterMarkBytes}B`,
	);
	lines.push(`- target deliveries attempted: ${delivered}`);
	lines.push(
		`- connections: opened=${m.connectionsOpened}, closed=${m.connectionsClosed}, dials=${m.dials}`,
	);
	lines.push(
		`- connections now: ${connectionsNow} (avg neighbours/node=${avgNeighbours.toFixed(
			2,
		)}, avg routes/node=${avgRoutes.toFixed(2)})`,
	);
	lines.push(`- protocol streams opened: ${m.streamsOpened}`);
	lines.push(
		`- frames sent: total=${m.framesSent} (data=${m.dataFramesSent}, ack=${m.ackFramesSent}, goodbye=${m.goodbyeFramesSent}, other=${m.otherFramesSent})`,
	);
	lines.push(`- bytes sent: ${m.bytesSent}`);

	console.log(lines.join("\n"));

	await Promise.all(peers.map((p) => p.stream.stop()));
};

await main();
