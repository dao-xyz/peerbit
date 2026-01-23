/**
 * Lightweight discrete-event simulator for @peerbit/stream-style routing.
 *
 * This is intentionally NOT libp2p-backed. It is meant for 1k–10k+ node
 * experiments (topology / convergence / message overhead) that would be
 * infeasible with real libp2p peers.
 *
 * Run:
 *   node --loader ts-node/esm ./packages/transport/stream/benchmark/topology-sim.ts --nodes 2000 --degree 4 --messages 200 --targets 10 --redundancy 2 --seed 1
 */

import {
	computeSeekAckRouteUpdate,
	selectSeekRelayTargets,
	shouldAcknowledgeDataMessage,
	shouldIgnoreDataMessage,
} from "../src/core/seek-routing.js";

type NodeId = number;

type RouteEntry = { via: NodeId; distance: number };

type PathNode = { node: NodeId; prev?: PathNode };

type DataEvent = {
	t: number;
	kind: "data";
	from: NodeId;
	to: NodeId;
	signer: PathNode; // last signer (= sender)
	hop: number;
};

type AckEvent = {
	t: number;
	kind: "ack";
	from: NodeId; // downstream (towards target)
	to: NodeId; // current signer in trace
	trace: PathNode; // trace.node === to
	target: NodeId; // ACK signer (final recipient of the data msg)
	seenCounter: number;
};

type Event = DataEvent | AckEvent;

class MinHeap<T> {
	private heap: T[] = [];
	constructor(private compare: (a: T, b: T) => number) {}

	get size() {
		return this.heap.length;
	}

	push(item: T) {
		this.heap.push(item);
		this.bubbleUp(this.heap.length - 1);
	}

	pop(): T | undefined {
		if (this.heap.length === 0) return undefined;
		const root = this.heap[0]!;
		const last = this.heap.pop()!;
		if (this.heap.length > 0) {
			this.heap[0] = last;
			this.bubbleDown(0);
		}
		return root;
	}

	private bubbleUp(index: number) {
		while (index > 0) {
			const parentIndex = (index - 1) >> 1;
			if (this.compare(this.heap[index]!, this.heap[parentIndex]!) >= 0) break;
			[this.heap[index], this.heap[parentIndex]] = [
				this.heap[parentIndex]!,
				this.heap[index]!,
			];
			index = parentIndex;
		}
	}

	private bubbleDown(index: number) {
		const length = this.heap.length;
		for (;;) {
			const left = index * 2 + 1;
			const right = left + 1;
			let smallest = index;

			if (
				left < length &&
				this.compare(this.heap[left]!, this.heap[smallest]!) < 0
			) {
				smallest = left;
			}
			if (
				right < length &&
				this.compare(this.heap[right]!, this.heap[smallest]!) < 0
			) {
				smallest = right;
			}
			if (smallest === index) break;
			[this.heap[index], this.heap[smallest]] = [
				this.heap[smallest]!,
				this.heap[index]!,
			];
			index = smallest;
		}
	}
}

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

const intBetween = (
	rng: () => number,
	minInclusive: number,
	maxInclusive: number,
) => minInclusive + int(rng, maxInclusive - minInclusive + 1);

const pickDistinct = (
	rng: () => number,
	n: number,
	k: number,
	exclude: NodeId,
): NodeId[] => {
	const out = new Set<NodeId>();
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
): NodeId[][] => {
	if (n <= 0) throw new Error("nodes must be > 0");
	if (targetDegree < 0) throw new Error("degree must be >= 0");
	if (targetDegree >= n) {
		throw new Error("degree must be < nodes for a simple graph");
	}

	const adj: Set<NodeId>[] = Array.from({ length: n }, () => new Set<NodeId>());
	const degree = new Uint16Array(n);

	const connect = (a: NodeId, b: NodeId) => {
		if (a === b) return false;
		if (adj[a]!.has(b)) return false;
		if (degree[a]! >= targetDegree || degree[b]! >= targetDegree) return false;
		adj[a]!.add(b);
		adj[b]!.add(a);
		degree[a]! += 1;
		degree[b]! += 1;
		return true;
	};

	// Seed connectivity so we don't start disconnected.
	if (targetDegree >= 2 && n >= 3) {
		for (let i = 0; i < n; i++) connect(i, (i + 1) % n);
	} else if (targetDegree >= 1 && n >= 2) {
		for (let i = 0; i < n - 1; i++) connect(i, i + 1);
	}

	const available: NodeId[] = [];
	const pos = new Int32Array(n).fill(-1);
	for (let i = 0; i < n; i++) {
		if (degree[i]! < targetDegree) {
			pos[i] = available.length;
			available.push(i);
		}
	}
	const removeAvailable = (id: NodeId) => {
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

const addRoute = (
	routesByFrom: Map<NodeId, Map<NodeId, RouteEntry[]>>,
	from: NodeId,
	target: NodeId,
	via: NodeId,
	distance: number,
	maxPerTarget = 8,
) => {
	let byTarget = routesByFrom.get(from);
	if (!byTarget) {
		byTarget = new Map();
		routesByFrom.set(from, byTarget);
	}
	let list = byTarget.get(target);
	if (!list) {
		list = [];
		byTarget.set(target, list);
	}

	const existing = list.find((x) => x.via === via);
	if (existing) {
		existing.distance = Math.min(existing.distance, distance);
	} else {
		list.push({ via, distance });
	}

	list.sort((a, b) => a.distance - b.distance);
	if (list.length > maxPerTarget) {
		list.length = maxPerTarget;
	}
};

type SimParams = {
	nodes: number;
	degree: number;
	messages: number;
	targetsPerMessage: number;
	redundancy: number;
	seed: number;
	dropRate: number;
	minDelay: number;
	maxDelay: number;
};

type SimResult = {
	dataTx: number;
	ackTx: number;
	targetDeliveries: number;
	targetExpected: number;
	firstHopSum: number;
	firstHopCount: number;
	avgRouteEntriesPerNode: number;
	avgRouteTargetsPerNode: number;
	warn?: string;
};

const traceHas = (trace: PathNode | undefined, node: NodeId): boolean => {
	let current = trace;
	while (current) {
		if (current.node === node) return true;
		current = current.prev;
	}
	return false;
};

const simulate = (params: SimParams): SimResult => {
	const rng = mulberry32(params.seed);
	const graph = buildRandomGraph(params.nodes, params.degree, rng);

	const warn =
		graph.some((nbs) => nbs.length < Math.min(params.degree, params.nodes - 1)) &&
		"graph generation hit degree constraints (some nodes < --degree)";

	const routes: Map<NodeId, Map<NodeId, RouteEntry[]>>[] = Array.from(
		{ length: params.nodes },
		() => new Map(),
	);

	// Seed direct neighbor routes: from=self -> target=neighbor via neighbor at distance=-1.
	for (let u = 0; u < params.nodes; u++) {
		for (const v of graph[u]!) {
			addRoute(routes[u]!, u, v, v, -1);
		}
	}

	let dataTx = 0;
	let ackTx = 0;
	let targetDeliveries = 0;
	let targetExpected = 0;
	let firstHopSum = 0;
	let firstHopCount = 0;

	const seen = new Uint8Array(params.nodes);
	const deliveredToTarget = new Uint8Array(params.nodes);

	const heap = new MinHeap<Event>((a, b) => a.t - b.t);

	const scheduleData = (
		now: number,
		from: NodeId,
		to: NodeId,
		signer: PathNode,
		hop: number,
	) => {
		if (params.dropRate > 0 && rng() < params.dropRate) return;
		const delay = intBetween(rng, params.minDelay, params.maxDelay);
		dataTx++;
		heap.push({ t: now + delay, kind: "data", from, to, signer, hop });
	};

	const scheduleAck = (
		now: number,
		from: NodeId,
		to: NodeId,
		trace: PathNode,
		target: NodeId,
		seenCounter: number,
	) => {
		if (params.dropRate > 0 && rng() < params.dropRate) return;
		const delay = intBetween(rng, params.minDelay, params.maxDelay);
		ackTx++;
		heap.push({
			t: now + delay,
			kind: "ack",
			from,
			to,
			trace,
			target,
			seenCounter,
		});
	};

	for (let msg = 0; msg < params.messages; msg++) {
		seen.fill(0);
		deliveredToTarget.fill(0);
		while (heap.pop() != null) {
			// drain any leftover events (shouldn't happen since we drain per message)
		}

		const source = int(rng, params.nodes);
		const targets = pickDistinct(
			rng,
			params.nodes,
			Math.min(params.targetsPerMessage, params.nodes - 1),
			source,
		);
		const targetsSet = new Set<NodeId>(targets);
		targetExpected += targets.length;

		const sourceSigner: PathNode = { node: source };
		const t0 = 0;
		for (const nb of graph[source]!) {
			scheduleData(t0, source, nb, sourceSigner, 1);
		}

		while (heap.size > 0) {
			const ev = heap.pop()!;
			if (ev.kind === "data") {
				const to = ev.to;
				const seenBefore = seen[to]!;
				const ignored = shouldIgnoreDataMessage({
					signedBySelf: traceHas(ev.signer, to),
					seenBefore,
					mode: { kind: "seek", redundancy: params.redundancy },
				});
				if (ignored) {
					continue;
				}
				seen[to]! = Math.min(255, seenBefore + 1);

				if (
					shouldAcknowledgeDataMessage({
						isRecipient: targetsSet.has(to),
						seenBefore,
						redundancy: params.redundancy,
					})
				) {
					if (deliveredToTarget[to] === 0) {
						deliveredToTarget[to] = 1;
						targetDeliveries++;
						firstHopSum += ev.hop;
						firstHopCount++;
					}

					// ACK travels backwards along the signer trace.
					scheduleAck(ev.t, to, ev.signer.node, ev.signer, to, seenBefore);
				}

				// SeekDelivery-style forwarding: forward until local seenBefore reaches redundancy.
				const newSigner: PathNode = { node: to, prev: ev.signer };
				const relayTo = selectSeekRelayTargets({
					candidates: graph[to]!,
					getCandidateId: (id) => id,
					inboundId: ev.from,
					hasSigned: (id) => traceHas(ev.signer, id),
				});
				for (const nb of relayTo) {
					scheduleData(ev.t, to, nb, newSigner, ev.hop + 1);
				}
			} else {
				// ACK processing at `ev.to` (a signer on the trace).
				const current = ev.to;
				if (ev.trace.node !== current) {
					// Should not happen; indicates a bug in the simulator.
					continue;
				}

				const upstream = ev.trace.prev?.node;
				const routeUpdate = computeSeekAckRouteUpdate({
					current,
					upstream,
					downstream: ev.from,
					target: ev.target,
					distance: ev.seenCounter,
				});
				addRoute(
					routes[current]!,
					routeUpdate.from,
					routeUpdate.target,
					routeUpdate.neighbour,
					routeUpdate.distance,
				);

				if (upstream != null) {
					scheduleAck(ev.t, current, upstream, ev.trace.prev!, ev.target, ev.seenCounter);
				}
			}
		}
	}

	let totalRouteEntries = 0;
	let totalRouteTargets = 0;
	for (let i = 0; i < params.nodes; i++) {
		for (const [_from, byTarget] of routes[i]!) {
			totalRouteTargets += byTarget.size;
			for (const [_target, list] of byTarget) {
				totalRouteEntries += list.length;
			}
		}
	}

	return {
		dataTx,
		ackTx,
		targetDeliveries,
		targetExpected,
		firstHopSum,
		firstHopCount,
		avgRouteEntriesPerNode: totalRouteEntries / params.nodes,
		avgRouteTargetsPerNode: totalRouteTargets / params.nodes,
		warn: warn || undefined,
	};
};

const parseArgs = (argv: string[]) => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};
	const has = (key: string) => argv.includes(key);

	if (has("--help") || has("-h")) {
		console.log(
			[
				"topology-sim.ts",
				"",
				"Args:",
				"  --nodes N            number of nodes (default: 2000)",
				"  --degree K           target undirected degree (default: 4)",
				"  --messages M         number of seek waves (default: 200)",
				"  --targets T          targets per message (default: 10)",
				"  --redundancy R       seek redundancy (default: 2)",
				"  --seed S             RNG seed (default: 1)",
				"  --drop P             per-edge drop rate [0..1] (default: 0)",
				"  --minDelay MS        min per-edge delay (default: 1)",
				"  --maxDelay MS        max per-edge delay (default: 5)",
				"",
				"Example:",
				"  node --loader ts-node/esm ./packages/transport/stream/benchmark/topology-sim.ts --nodes 5000 --degree 6 --messages 500 --targets 20 --redundancy 2 --drop 0.01 --seed 42",
			].join("\n"),
		);
		process.exit(0);
	}

	const nodes = Number(get("--nodes") ?? 2000);
	const degree = Number(get("--degree") ?? 4);
	const messages = Number(get("--messages") ?? 200);
	const targetsPerMessage = Number(get("--targets") ?? 10);
	const redundancy = Number(get("--redundancy") ?? 2);
	const seed = Number(get("--seed") ?? 1);
	const dropRate = Number(get("--drop") ?? 0);
	const minDelay = Number(get("--minDelay") ?? 1);
	const maxDelay = Number(get("--maxDelay") ?? 5);

	return {
		nodes,
		degree,
		messages,
		targetsPerMessage,
		redundancy,
		seed,
		dropRate,
		minDelay,
		maxDelay,
	} satisfies SimParams;
};

const main = async () => {
	const params = parseArgs(process.argv.slice(2));
	const started = Date.now();
	const result = simulate(params);
	const elapsedMs = Date.now() - started;

	const deliveryPct =
		result.targetExpected === 0
			? 100
			: (result.targetDeliveries / result.targetExpected) * 100;
	const avgFirstHop =
		result.firstHopCount === 0 ? 0 : result.firstHopSum / result.firstHopCount;

	const lines: string[] = [];
	lines.push("stream topology-sim results");
	lines.push(`- nodes: ${params.nodes}, degree: ${params.degree}`);
	lines.push(
		`- messages: ${params.messages}, targets/message: ${params.targetsPerMessage}, redundancy: ${params.redundancy}`,
	);
	lines.push(
		`- drop: ${params.dropRate}, delay: ${params.minDelay}..${params.maxDelay} (sim units)`,
	);
	lines.push(`- tx: data=${result.dataTx}, ack=${result.ackTx}`);
	lines.push(
		`- delivery: ${result.targetDeliveries}/${result.targetExpected} (${deliveryPct.toFixed(
			2,
		)}%)`,
	);
	lines.push(`- avg first-delivery hop: ${avgFirstHop.toFixed(2)}`);
	lines.push(
		`- avg route targets/node: ${result.avgRouteTargetsPerNode.toFixed(
			2,
		)}, entries/node: ${result.avgRouteEntriesPerNode.toFixed(2)}`,
	);
	if (result.warn) {
		lines.push(`- warn: ${result.warn}`);
	}
	lines.push(`- wall time: ${elapsedMs}ms`);
	console.log(lines.join("\n"));
};

await main();
