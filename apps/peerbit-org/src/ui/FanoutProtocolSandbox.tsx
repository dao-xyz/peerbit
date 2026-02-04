/**
 * Browser-friendly sandbox that runs the real @peerbit/pubsub + @peerbit/stream logic
 * over an in-memory libp2p shim.
 *
 * Default settings are "small N" to keep the UI responsive, but you can crank it up.
 */

import type { MouseEvent as ReactMouseEvent } from "react";
import type { ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

import type { PeerId } from "@libp2p/interface";
import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import { DirectSub, type DirectSubComponents } from "@peerbit/pubsub";
import { AcknowledgeAnyWhere } from "@peerbit/stream-interface";

import {
	InMemoryConnectionManager,
	type InMemoryFrameSentEvent,
	InMemoryNetwork,
} from "../sim/inmemory-libp2p.js";

type Vec2 = { x: number; y: number };
type Graph = {
	nodes: Array<{ id: number; pos: Vec2 }>;
	edges: Array<{ a: number; b: number }>;
	adj: number[][];
};

type Layout = "force" | "scatter" | "circle";

type RunStatus = "idle" | "setting-up" | "running" | "done" | "error";

type FlowCapture = "setup+publish" | "publish-only";

type FlowMode = "bench" | "stream";

type FlowBuffer = {
	cap: number;
	next: number;
	from: Uint16Array;
	to: Uint16Array;
	startMs: Float64Array;
	durationMs: Uint16Array;
	kind: Uint8Array; // 0=data, 1=ack
	seq: Uint16Array;
};

type Result = {
	nodes: number;
	degree: number;
	subscribers: number;
	messages: number;
	msgSize: number;
	subscribeModel: "preseed" | "real";
	writerKnown: number;
	expectedDeliveries: number;
	observedDeliveries: number;
	subscribeMs: number;
	publishMs: number;
	p50?: number;
	p95?: number;
	p99?: number;
	max?: number;
	framesSent: number;
	bytesSent: number;
	flowMode: FlowMode;
	flowDataFrames: number;
	flowAckFrames: number;
};

const InfoPopover = ({ children }: { children: ReactNode }) => (
	<details className="relative inline-block align-middle">
		<summary
			className="inline-flex h-5 w-5 cursor-pointer list-none items-center justify-center rounded-full border border-slate-200 bg-white text-[10px] font-semibold text-slate-600 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800 [&::-webkit-details-marker]:hidden"
			aria-label="Info"
		>
			i
		</summary>
		<div className="absolute right-0 z-20 mt-2 w-72 rounded-xl border border-slate-200 bg-white p-3 text-xs text-slate-700 shadow-lg dark:border-slate-800 dark:bg-slate-950 dark:text-slate-200">
			{children}
		</div>
	</details>
);

const clamp = (v: number, min: number, max: number) => Math.max(min, Math.min(max, v));

const readIntAttr = (value: unknown, fallback: number, min: number, max: number) => {
	const v = typeof value === "string" ? Number(value) : typeof value === "number" ? value : NaN;
	if (!Number.isFinite(v)) return fallback;
	return clamp(Math.floor(v), min, max);
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

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const EDGE_KEY_BASE = 2048;

const createFlowBuffer = (cap: number): FlowBuffer => {
	const safeCap = clamp(Math.floor(cap), 256, 40_000);
	return {
		cap: safeCap,
		next: 0,
		from: new Uint16Array(safeCap),
		to: new Uint16Array(safeCap),
		startMs: new Float64Array(safeCap),
		durationMs: new Uint16Array(safeCap),
		kind: new Uint8Array(safeCap),
		seq: new Uint16Array(safeCap),
	};
};

const clearFlowBuffer = (buf: FlowBuffer) => {
	buf.next = 0;
	buf.startMs.fill(0);
	buf.durationMs.fill(0);
	buf.kind.fill(0);
	buf.seq.fill(0);
};

const pushFlow = (
	buf: FlowBuffer,
	flow: { from: number; to: number; startMs: number; durationMs: number; kind: 0 | 1; seq: number },
) => {
	const i = buf.next;
	buf.from[i] = flow.from;
	buf.to[i] = flow.to;
	buf.startMs[i] = flow.startMs;
	buf.durationMs[i] = clamp(Math.round(flow.durationMs), 1, 65_000);
	buf.kind[i] = flow.kind;
	buf.seq[i] = clamp(Math.floor(flow.seq), 0, 65_000);
	buf.next = (i + 1) % buf.cap;
};

const buildEdges = (adj: number[][]): Graph["edges"] => {
	const edges: Graph["edges"] = [];
	for (let a = 0; a < adj.length; a++) {
		for (const b of adj[a] ?? []) {
			if (b > a) edges.push({ a, b });
		}
	}
	return edges;
};

const buildRandomGraph = (n: number, targetDegree: number, rng: () => number): number[][] => {
	if (n <= 0) throw new Error("nodes must be > 0");
	if (targetDegree < 0) throw new Error("degree must be >= 0");
	if (targetDegree >= n) throw new Error("degree must be < nodes");

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

	// Seed edges (avoid the "always a ring" topology so layouts don't look circular).
	if (targetDegree >= 2 && n >= 2) {
		// Bounded-degree spanning tree.
		const parents: number[] = [0];
		const parentPos = new Int32Array(n).fill(-1);
		parentPos[0] = 0;
		const removeParent = (id: number) => {
			const p = parentPos[id]!;
			if (p < 0) return;
			const last = parents.pop()!;
			if (last !== id) {
				parents[p] = last;
				parentPos[last] = p;
			}
			parentPos[id] = -1;
		};

		for (let i = 1; i < n; i++) {
			const parent = parents[Math.floor(rng() * parents.length)]!;
			connect(i, parent);
			if (degree[parent]! >= targetDegree) removeParent(parent);
			parentPos[i] = parents.length;
			parents.push(i);
		}
	} else if (targetDegree >= 1 && n >= 2) {
		// With max degree 1, a fully connected graph is impossible for n>2; create a best-effort matching.
		for (let i = 0; i < n - 1; i += 2) connect(i, i + 1);
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
		const a = available[Math.floor(rng() * available.length)]!;
		const b = available[Math.floor(rng() * available.length)]!;
		if (a === b) continue;
		if (!connect(a, b)) continue;
		if (degree[a]! >= targetDegree) removeAvailable(a);
		if (degree[b]! >= targetDegree) removeAvailable(b);
	}

	return adj.map((s) => [...s]);
};

const layoutScatter = (adj: number[][], width: number, height: number, rng: () => number): Graph => {
	const n = adj.length;
	const margin = 26;
	const w = Math.max(1, width);
	const h = Math.max(1, height);
	const nodes: Graph["nodes"] = [];
	for (let i = 0; i < n; i++) {
		nodes.push({
			id: i,
			pos: {
				x: margin + rng() * Math.max(1, w - margin * 2),
				y: margin + rng() * Math.max(1, h - margin * 2),
			},
		});
	}
	return { nodes, edges: buildEdges(adj), adj };
};

const layoutCircle = (adj: number[][], width: number, height: number, rng: () => number): Graph => {
	const n = adj.length;
	const margin = 26;
	const w = Math.max(1, width);
	const h = Math.max(1, height);
	const cx = w / 2;
	const cy = h / 2;
	const radius = Math.max(60, Math.min(w, h) / 2 - margin);

	const nodes: Graph["nodes"] = [];
	for (let i = 0; i < n; i++) {
		const a = (2 * Math.PI * i) / Math.max(1, n);
		nodes.push({
			id: i,
			pos: {
				x: cx + radius * Math.cos(a) + (rng() - 0.5) * 10,
				y: cy + radius * Math.sin(a) + (rng() - 0.5) * 10,
			},
		});
	}
	return { nodes, edges: buildEdges(adj), adj };
};

const layoutForce = (adj: number[][], width: number, height: number, rng: () => number): Graph => {
	const n = adj.length;
	const margin = 28;
	const w = Math.max(1, width);
	const h = Math.max(1, height);

	// Start scattered (deterministic via seed) so the graph doesn't look like a circle.
	const pos: Vec2[] = [];
	for (let i = 0; i < n; i++) {
		pos.push({
			x: margin + rng() * Math.max(1, w - margin * 2),
			y: margin + rng() * Math.max(1, h - margin * 2),
		});
	}

	const edges = buildEdges(adj);

	// Force-directed layout:
	// - full repulsion for small n (O(n^2))
	// - sampled repulsion for large n (O(n*k)) to keep 1000-node demos from freezing instantly
	const area = w * h;
	const k = Math.sqrt(area / Math.max(1, n));

	const isLarge = n > 250;
	const iterations = isLarge ? 24 : Math.min(220, Math.max(60, Math.floor(20 * Math.log2(n + 1))));
	const repulsionSamplesPerNode = isLarge ? 64 : n;
	let temperature = Math.min(w, h) / 10;

	const disp: Vec2[] = Array.from({ length: n }, () => ({ x: 0, y: 0 }));
	for (let iter = 0; iter < iterations; iter++) {
		for (let i = 0; i < n; i++) {
			disp[i]!.x = 0;
			disp[i]!.y = 0;
		}

		if (!isLarge) {
			for (let i = 0; i < n; i++) {
				for (let j = i + 1; j < n; j++) {
					const dx = pos[i]!.x - pos[j]!.x;
					const dy = pos[i]!.y - pos[j]!.y;
					const dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
					const force = (k * k) / dist;
					const fx = (dx / dist) * force;
					const fy = (dy / dist) * force;
					disp[i]!.x += fx;
					disp[i]!.y += fy;
					disp[j]!.x -= fx;
					disp[j]!.y -= fy;
				}
			}
		} else {
			for (let i = 0; i < n; i++) {
				for (let s = 0; s < repulsionSamplesPerNode; s++) {
					const j = Math.floor(rng() * n);
					if (j === i) continue;
					const dx = pos[i]!.x - pos[j]!.x;
					const dy = pos[i]!.y - pos[j]!.y;
					const dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
					const force = (k * k) / dist;
					const fx = (dx / dist) * force;
					const fy = (dy / dist) * force;
					disp[i]!.x += fx;
					disp[i]!.y += fy;
					disp[j]!.x -= fx;
					disp[j]!.y -= fy;
				}
			}
		}

		for (const e of edges) {
			const dx = pos[e.a]!.x - pos[e.b]!.x;
			const dy = pos[e.a]!.y - pos[e.b]!.y;
			const dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
			const force = (dist * dist) / k;
			const fx = (dx / dist) * force;
			const fy = (dy / dist) * force;
			disp[e.a]!.x -= fx;
			disp[e.a]!.y -= fy;
			disp[e.b]!.x += fx;
			disp[e.b]!.y += fy;
		}

		for (let i = 0; i < n; i++) {
			const dx = disp[i]!.x;
			const dy = disp[i]!.y;
			const dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
			const step = Math.min(dist, temperature);
			pos[i]!.x += (dx / dist) * step;
			pos[i]!.y += (dy / dist) * step;
			pos[i]!.x = clamp(pos[i]!.x, margin, w - margin);
			pos[i]!.y = clamp(pos[i]!.y, margin, h - margin);
		}

		temperature *= 0.93;
	}

	return {
		nodes: pos.map((p, id) => ({ id, pos: p })),
		edges,
		adj,
	};
};

// "PSIM" prefix to identify bench messages.
const BENCH_ID_PREFIX = Uint8Array.from([0x50, 0x53, 0x49, 0x4d]);
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
	((buf[offset + 0] << 24) | (buf[offset + 1] << 16) | (buf[offset + 2] << 8) | buf[offset + 3]) >>> 0;

const hasBenchPrefixAt = (buf: Uint8Array, offset: number) =>
	offset + 4 <= buf.length &&
	buf[offset + 0] === BENCH_ID_PREFIX[0] &&
	buf[offset + 1] === BENCH_ID_PREFIX[1] &&
	buf[offset + 2] === BENCH_ID_PREFIX[2] &&
	buf[offset + 3] === BENCH_ID_PREFIX[3];

// DirectStream message bytes format (for DataMessage):
// [0]=DATA_VARIANT, [1]=MessageHeader variant, [2..33]=id (32 bytes)
// Our bench id layout: [0..3]="PSIM", [4..7]=seq (u32be)
const parseBenchSeqFromDataPayload = (payload: Uint8Array): number | undefined => {
	// Fast-path: expected layout in current borsh schema:
	// [0]=DATA_VARIANT, [1]=MessageHeader variant, [2..33]=id
	if (payload.length >= 10 && payload[0] === 0 && payload[1] === 0 && hasBenchPrefixAt(payload, 2)) {
		return readU32BE(payload, 6);
	}

	// Fallback: find the PSIM prefix near the front. This is robust across minor schema changes.
	const max = Math.min(64, payload.length - 8);
	for (let i = 0; i <= max; i++) {
		if (!hasBenchPrefixAt(payload, i)) continue;
		return readU32BE(payload, i + 4);
	}
};

// ACK payload ends with: [messageIdToAcknowledge (32 bytes)] [seenCounter (1 byte)]
const parseBenchSeqFromAckPayload = (payload: Uint8Array): number | undefined => {
	if (payload.length < 8) return;
	if (payload.length >= 33) {
		const idStart = payload.length - 33;
		if (hasBenchPrefixAt(payload, idStart)) {
			return readU32BE(payload, idStart + 4);
		}
	}

	// Fallback: search close to the tail (ACK contains a 32-byte message id).
	const start = Math.max(0, payload.length - 140);
	for (let i = start; i <= payload.length - 8; i++) {
		if (!hasBenchPrefixAt(payload, i)) continue;
		return readU32BE(payload, i + 4);
	}
};

// Bench flow colors: keep in a blue→purple range so they don't get confused with
// writer (red) and "received" (green) node states.
const benchHue = (seq: number) => 220 + ((seq * 53) % 120);
const benchColor = (seq: number, alpha: number) => `hsla(${benchHue(seq)}, 92%, 56%, ${alpha})`;

const quantile = (sorted: number[], q: number) => {
	if (sorted.length === 0) return 0;
	const idx = (sorted.length - 1) * clamp(q, 0, 1);
	const lo = Math.floor(idx);
	const hi = Math.min(sorted.length - 1, lo + 1);
	const t = idx - lo;
	return sorted[lo]! * (1 - t) + sorted[hi]! * t;
};

class SimDirectSub extends DirectSub {
	constructor(
		components: DirectSubComponents,
		opts: { subscriptionDebounceDelayMs: number; seekTimeoutMs: number },
	) {
		super(components, {
			canRelayMessage: true,
			subscriptionDebounceDelay: opts.subscriptionDebounceDelayMs,
			seekTimeout: opts.seekTimeoutMs,
			connectionManager: {
				// Keep it simple for browser demos.
				dialer: false,
				pruner: false,
				maxConnections: Number.MAX_SAFE_INTEGER,
				minConnections: 0,
			},
		});

		// Fast/mock signing: we want the routing + stream behavior, not crypto cost.
		this.sign = async () =>
			new SignatureWithKey({
				signature: new Uint8Array([0]),
				publicKey: this.publicKey,
				prehash: PreHash.NONE,
			});
	}

	public async verifyAndProcess(message: any) {
		// Skip expensive crypto verify, but keep session handling semantics stable.
		const from = message?.header?.signatures?.publicKeys?.[0];
		if (from) {
			const hash = typeof from.hashcode === "function" ? from.hashcode() : undefined;
			if (hash && !this.peers.has(hash)) {
				this.updateSession(from, Number(message.header.session));
			}
		}
		return true;
	}
}

const waitForProtocolStreams = async (peers: Array<{ sub: SimDirectSub }>) => {
	const start = Date.now();
	const timeoutMs = 30_000;
	for (;;) {
		if (Date.now() - start > timeoutMs) {
			throw new Error("Timeout waiting for protocol streams to become duplex");
		}
		let missing = 0;
		for (const p of peers) {
			const protocols = p.sub.multicodecs;
			for (const conn of p.sub.components.connectionManager.getConnections()) {
				const streams = conn.streams as any as Array<{ protocol?: string; direction?: string }>;
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
};

type FanoutProtocolSandboxProps = {
	node?: unknown;
	className?: string;
	nodes?: string;
	degree?: string;
	subscribers?: string;
	messages?: string;
	msgSize?: string;
	intervalMs?: string;
	streamRxDelayMs?: string;
	dialDelayMs?: string;
	seed?: string;
	height?: string;
};

export function FanoutProtocolSandbox({
	className,
	nodes: nodesAttr,
	degree: degreeAttr,
	subscribers: subscribersAttr,
	messages: messagesAttr,
	msgSize: msgSizeAttr,
	intervalMs: intervalMsAttr,
	streamRxDelayMs: streamRxDelayMsAttr,
	dialDelayMs: dialDelayMsAttr,
	seed: seedAttr,
	height: heightAttr,
}: FanoutProtocolSandboxProps) {
	const initialNodes = readIntAttr(nodesAttr, 20, 3, 1000);
	const initialDegree = readIntAttr(degreeAttr, 4, 1, 16);
	const initialSubscribers = readIntAttr(subscribersAttr, Math.max(1, initialNodes - 1), 1, initialNodes - 1);
	const initialMessages = readIntAttr(messagesAttr, 1, 1, 200);
	const initialMsgSize = readIntAttr(msgSizeAttr, 32, 0, 1_000_000);
	const initialIntervalMs = readIntAttr(intervalMsAttr, 0, 0, 10_000);
	// Default to a slightly "slow motion" rx delay so flow comets and deliveries are easier to follow.
	const initialStreamRxDelayMs = readIntAttr(streamRxDelayMsAttr, 1000, 0, 10_000);
	const initialDialDelayMs = readIntAttr(dialDelayMsAttr, 0, 0, 10_000);
	const initialSeed = readIntAttr(seedAttr, 1, 0, 1_000_000_000);
	const initialHeight = readIntAttr(heightAttr, 520, 240, 900);

	const [nodes, setNodes] = useState(initialNodes);
	const [degree, setDegree] = useState(initialDegree);
	const [subscribers, setSubscribers] = useState(initialSubscribers);
	const [messages, setMessages] = useState(initialMessages);
	const [msgSize, setMsgSize] = useState(initialMsgSize);
	const [intervalMs, setIntervalMs] = useState(initialIntervalMs);
	const [streamRxDelayMs, setStreamRxDelayMs] = useState(initialStreamRxDelayMs);
	const [dialDelayMs, setDialDelayMs] = useState(initialDialDelayMs);
	const [seed, setSeed] = useState(initialSeed);

	const [layout, setLayout] = useState<Layout>("force");
	const [layoutNote, setLayoutNote] = useState<string | null>(null);

	const [subscribeModel, setSubscribeModel] = useState<Result["subscribeModel"]>("preseed");
	const [subscriptionDebounceDelayMs, setSubscriptionDebounceDelayMs] = useState(0);
	const [showEdgeFlows, setShowEdgeFlows] = useState(true);
	const [flowMode, setFlowMode] = useState<FlowMode>("bench");
	const [flowDurationMs, setFlowDurationMs] = useState(1000);
	const [syncFlowToRxDelay, setSyncFlowToRxDelay] = useState(true);
	const [flowCapture, setFlowCapture] = useState<FlowCapture>("publish-only");
	const [showAckFlows, setShowAckFlows] = useState(false);

	const [status, setStatus] = useState<RunStatus>("idle");
	const [error, setError] = useState<string | null>(null);
	const [result, setResult] = useState<Result | null>(null);
	const [publishProgress, setPublishProgress] = useState<{ seq: number; total: number } | null>(null);

	const [writerIndex, setWriterIndex] = useState(0);
	const [graph, setGraph] = useState<Graph | null>(null);

	const containerRef = useRef<HTMLDivElement | null>(null);
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	const [size, setSize] = useState<{ w: number; h: number }>({ w: 0, h: initialHeight });

	const prevNodesRef = useRef(nodes);

	// Keep "subscribe everyone except writer" intuitive:
	// if the user had Subscribers=N-1 and changes Nodes, keep Subscribers in sync.
	useEffect(() => {
		setSubscribers((prev) => {
			const prevMax = Math.max(1, prevNodesRef.current - 1);
			const nextMax = Math.max(1, nodes - 1);
			const wasAll = prev === prevMax;
			return wasAll ? nextMax : clamp(prev, 1, nextMax);
		});
		setWriterIndex((prev) => (prev >= nodes ? 0 : prev));
		prevNodesRef.current = nodes;
	}, [nodes]);

	// Live-ish receive visualization (update via rAF, not per packet).
	const pulseAtRef = useRef<Float64Array | null>(null);
	const receivedCountRef = useRef<Uint32Array | null>(null);
	const flowBufRef = useRef<FlowBuffer | null>(null);
	const edgeHeatRef = useRef<Float32Array | null>(null);
	const edgeIndexByKeyRef = useRef<Map<number, number> | null>(null);
	const flowCaptureStartMsRef = useRef<number>(0);
	const invalidateRafRef = useRef<number | null>(null);
	const [, setPaintTick] = useState(0);
	const invalidate = () => {
		if (invalidateRafRef.current != null) return;
		invalidateRafRef.current = requestAnimationFrame(() => {
			invalidateRafRef.current = null;
			setPaintTick((t) => (t + 1) % 1_000_000);
		});
	};

	const runRef = useRef<{
		network: InMemoryNetwork;
		peers: Array<{ peerId: PeerId; sub: SimDirectSub }>;
		stop: () => Promise<void>;
	} | null>(null);

	useEffect(() => {
		const el = containerRef.current;
		if (!el) return;
		const obs = new ResizeObserver((entries) => {
			const box = entries[0]?.contentRect;
			if (!box) return;
			setSize({ w: Math.floor(box.width), h: initialHeight });
		});
		obs.observe(el);
		return () => obs.disconnect();
	}, [initialHeight]);

	const regenerateGraph = () => {
		setLayoutNote(null);
		const n = clamp(nodes, 3, 1000);
		const k = clamp(degree, 1, Math.max(1, n - 1));
		const w = Math.max(1, size.w || 760);
		const h = Math.max(1, size.h || initialHeight);
		const rng = mulberry32(seed);
		const adj = buildRandomGraph(n, Math.min(k, n - 1), rng);

		// Basic connectivity note: disconnected graphs will never fully deliver.
		{
			const visited = new Uint8Array(n);
			const stack = [0];
			visited[0] = 1;
			while (stack.length) {
				const at = stack.pop()!;
				for (const next of adj[at] ?? []) {
					if (visited[next]) continue;
					visited[next] = 1;
					stack.push(next);
				}
			}
			let reachable = 0;
			for (let i = 0; i < visited.length; i++) reachable += visited[i]!;
			if (reachable < n) {
				setLayoutNote(
					`Network is disconnected (${reachable}/${n} reachable). Increase Degree or change Seed.`,
				);
			}
		}

		if (layout === "force" && n >= 300) {
			setLayoutNote("Force layout can be slow at this scale. If it feels stuck, switch to Scatter or Circle.");
		} else if (n >= 400) {
			setLayoutNote("Large node counts can freeze the tab. Start with ~50–200 and scale up gradually.");
		}

		const g =
			layout === "circle"
				? layoutCircle(adj, w, h, rng)
				: layout === "scatter"
					? layoutScatter(adj, w, h, rng)
					: layoutForce(adj, w, h, rng);
		setGraph(g);
		setWriterIndex((x) => clamp(x, 0, n - 1));
		pulseAtRef.current = new Float64Array(n).fill(-1);
		receivedCountRef.current = new Uint32Array(n);

		const edgeIndexByKey = new Map<number, number>();
		for (let i = 0; i < g.edges.length; i++) {
			const e = g.edges[i]!;
			const a = Math.min(e.a, e.b);
			const b = Math.max(e.a, e.b);
			edgeIndexByKey.set(a * EDGE_KEY_BASE + b, i);
		}
		edgeIndexByKeyRef.current = edgeIndexByKey;
		edgeHeatRef.current = new Float32Array(g.edges.length);
		flowBufRef.current = createFlowBuffer(Math.max(2_000, Math.min(20_000, g.edges.length * 8)));
	};

	useEffect(() => {
		if (size.w > 0) regenerateGraph();
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [size.w]);

	useEffect(() => {
		return () => {
			if (invalidateRafRef.current != null) cancelAnimationFrame(invalidateRafRef.current);
		};
	}, []);

	const stop = async (opts?: { keepStatus?: boolean }) => {
		const run = runRef.current;
		runRef.current = null;
		if (!opts?.keepStatus) {
			setStatus("idle");
			setError(null);
		}
		if (run) {
			await run.stop();
		}
	};

	useEffect(() => {
		return () => {
			void stop();
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

	const start = async () => {
		setError(null);
		setResult(null);
		setPublishProgress(null);
		setStatus("setting-up");

		await stop({ keepStatus: true });

		const g = graph;
		if (!g) {
			setStatus("error");
			setError("Graph not ready yet");
			return;
		}

			// Reset flow visuals early so we can optionally include setup/subscription traffic in the view.
			edgeHeatRef.current?.fill(0);
			if (flowBufRef.current) clearFlowBuffer(flowBufRef.current);
			flowCaptureStartMsRef.current = flowCapture === "setup+publish" ? 0 : Number.POSITIVE_INFINITY;

		const n = g.nodes.length;
			const subscriberCount = clamp(subscribers, 1, Math.max(1, n - 1));
			const topic = "concert";

				// When syncing to rx delay, keep comets aligned with real inbound read timing.
				const dataFlowDurationMs = clamp(
					syncFlowToRxDelay ? Math.max(80, streamRxDelayMs) : flowDurationMs,
					80,
					10_000,
				);
				const ackFlowDurationMs = clamp(Math.round(dataFlowDurationMs * 0.8), 80, 10_000);
				let flowDataFrames = 0;
				let flowAckFrames = 0;
				const peerIndexById = new Map<string, number>();
			const network = new InMemoryNetwork({
				streamRxDelayMs,
				dialDelayMs,
				onFrameSent: showEdgeFlows
					? (ev: InMemoryFrameSentEvent) => {
							const now = performance.now();
							if (now < flowCaptureStartMsRef.current) return;

							const from = peerIndexById.get(ev.from.toString());
							const to = peerIndexById.get(ev.to.toString());
							if (from == null || to == null) return;

							const buf = flowBufRef.current;
							const heat = edgeHeatRef.current;
							const indexByKey = edgeIndexByKeyRef.current;

							const bumpHeat = () => {
								if (!heat || !indexByKey) return;
								const a = Math.min(from, to);
								const b = Math.max(from, to);
								const idx = indexByKey.get(a * EDGE_KEY_BASE + b);
								if (idx != null) heat[idx] = Math.min(1, heat[idx] + 0.22);
							};

							const readPayload = () => {
								if (ev.payloadLength <= 0) return;
								const end = Math.min(ev.encodedFrame.length, ev.payloadOffset + ev.payloadLength);
								if (end <= ev.payloadOffset) return;
								return ev.encodedFrame.subarray(ev.payloadOffset, end);
							};

							if (ev.type === "data") {
								if (flowMode === "bench") {
									const payload = readPayload();
									if (!payload) return;
									const seq = parseBenchSeqFromDataPayload(payload);
									if (seq == null) return;
									flowDataFrames += 1;
									if (buf) {
										pushFlow(buf, {
											from,
											to,
											startMs: now,
											durationMs: dataFlowDurationMs,
											kind: 0,
											seq,
										});
									}
									bumpHeat();
									return;
								}

								flowDataFrames += 1;
								if (buf) {
									pushFlow(buf, {
										from,
										to,
										startMs: now,
										durationMs: dataFlowDurationMs,
										kind: 0,
										seq: 0,
									});
								}
								bumpHeat();
								return;
							}

							if (ev.type === "ack") {
								if (!showAckFlows) return;

								if (flowMode === "bench") {
									const payload = readPayload();
									if (!payload) return;
									const seq = parseBenchSeqFromAckPayload(payload);
									if (seq == null) return;
									flowAckFrames += 1;
									if (buf) {
										pushFlow(buf, {
											from,
											to,
											startMs: now,
											durationMs: ackFlowDurationMs,
											kind: 1,
											seq,
										});
									}
									return;
								}

								flowAckFrames += 1;
								if (buf) {
									pushFlow(buf, {
										from,
										to,
										startMs: now,
										durationMs: ackFlowDurationMs,
										kind: 1,
										seq: 0,
									});
								}
							}
					  }
					: undefined,
			});
			const peers: Array<{ peerId: PeerId; sub: SimDirectSub }> = [];

			const seekTimeoutMs = clamp(
				10_000 + streamRxDelayMs * Math.round(8 + 2 * Math.log2(n + 1)),
				10_000,
				5 * 60_000,
			);

			const basePort = 30_000;
			for (let i = 0; i < n; i++) {
				const port = basePort + i;
				const { runtime } = InMemoryNetwork.createPeer({ index: i, port, network });
				runtime.connectionManager = new InMemoryConnectionManager(network, runtime);
			network.registerPeer(runtime, port);
			peerIndexById.set(runtime.peerId.toString(), i);

			const components: DirectSubComponents = {
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
					sub: new SimDirectSub(components, { subscriptionDebounceDelayMs, seekTimeoutMs }),
				});

			// Yield occasionally so the UI doesn't look frozen on larger node counts.
			if (i > 0 && i % 50 === 0) {
				await delay(0);
			}
		}

		const stopAll = async () => {
			await Promise.allSettled(peers.map((p) => p.sub.stop()));
		};
		runRef.current = { network, peers, stop: stopAll };

		try {
			await Promise.all(peers.map((p) => p.sub.start()));

				// Establish initial graph via dials.
				let dialed = 0;
				for (let a = 0; a < g.adj.length; a++) {
					for (const b of g.adj[a]!) {
						if (b <= a) continue;
						const addrB = (peers[b]!.sub.components.addressManager as any).getAddresses()[0];
						await peers[a]!.sub.components.connectionManager.openConnection(addrB);
						dialed += 1;
						if (dialed % 200 === 0) {
							await delay(0);
						}
					}
				}

			await waitForProtocolStreams(peers);

			const writer = peers[writerIndex]!.sub;

			// Pick subscriber indices (exclude writer)
			const subscriberIndices: number[] = [];
			for (let i = 0; i < n && subscriberIndices.length < subscriberCount; i++) {
				if (i === writerIndex) continue;
				subscriberIndices.push(i);
			}

				const subscribeStart = Date.now();
				if (subscribeModel === "preseed") {
					for (const idx of subscriberIndices) {
						peers[idx]!.sub.subscriptions.set(topic, { counter: 1 });
					}
					const subscriberHashes = subscriberIndices.map((i) => peers[i]!.sub.publicKeyHash);
					writer.topicsToPeers.set(topic, new Set(subscriberHashes));

					// Warm up DirectStream routes so subsequent SilentDelivery publishes can travel beyond one relay hop.
					// In the absence of routing info, relays intentionally avoid flooding SilentDelivery traffic
					// (otherwise it degenerates into FloodSub-like O(E) fanout).
					// Keep the warmup very fast; we still render the actual publish phase in "slow motion".
					const warmupRxDelayMs = 0;
					if (warmupRxDelayMs !== network.streamRxDelayMs) {
						network.setStreamRxDelayMs(warmupRxDelayMs);
					}
					try {
						const warmup = await writer.createMessage(undefined, {
							mode: new AcknowledgeAnyWhere({ redundancy: 1 }),
							priority: 1,
						});
						await writer.publishMessage(writer.publicKey, warmup);
						// Wait until all intended subscribers become reachable from the writer.
						// This keeps the real publish phase "economic" (SilentDelivery) while avoiding the large receiver list
						// overhead during warmup.
						const warmupTimeoutMs = clamp(3000 + n * Math.max(1, degree), 3000, 30_000);
						await writer.waitFor(subscriberHashes, { timeout: warmupTimeoutMs, settle: "all" });
					} finally {
						if (network.streamRxDelayMs !== streamRxDelayMs) {
							network.setStreamRxDelayMs(streamRxDelayMs);
						}
					}
				} else {
					void writer.requestSubscribers(topic).catch(() => {});
					await Promise.all(
						subscriberIndices.map(async (idx) => {
							await peers[idx]!.sub.subscribe(topic);
					}),
				);
			}
			const subscribeDone = Date.now();

			// Reset visualization state.
			pulseAtRef.current?.fill(-1);
			receivedCountRef.current?.fill(0);
			invalidate();

			setStatus("running");

			const sendTimes = new Float64Array(messages);
			let observedDeliveries = 0;
			const latencies: number[] = [];

			for (const idx of subscriberIndices) {
				const node = peers[idx]!.sub;
				node.addEventListener("data", (ev: any) => {
					const msg = ev?.detail?.message;
					const id = msg?.id;
					if (!(id instanceof Uint8Array) || !isBenchId(id)) return;
					const seq = readU32BE(id, 4);
					const t0 = sendTimes[seq];
					if (!t0) return;
					observedDeliveries += 1;
					latencies.push(Date.now() - t0);
					if (pulseAtRef.current) {
						pulseAtRef.current[idx] = performance.now();
					}
					if (receivedCountRef.current) {
						receivedCountRef.current[idx] += 1;
					}
					invalidate();
				});
			}

			const payload = new Uint8Array(Math.max(0, msgSize));
			const publishStart = Date.now();
			flowCaptureStartMsRef.current =
				flowCapture === "setup+publish" ? flowCaptureStartMsRef.current : performance.now();
			const expectedDeliveries = subscriberCount * messages;

			setPublishProgress({ seq: 0, total: messages });
				for (let i = 0; i < messages; i++) {
					setPublishProgress({ seq: i, total: messages });
					const id = new Uint8Array(32);
					id.set(BENCH_ID_PREFIX, 0);
					writeU32BE(id, 4, i);
					sendTimes[i] = Date.now();

				await writer.publish(payload, { id, topics: [topic] } as any);

					// Always yield at least one macrotask so the canvas can animate.
					await delay(intervalMs);
				}

			setPublishProgress(null);
			await delay(200);
			const publishDone = Date.now();

			latencies.sort((a, b) => a - b);
			const writerKnown = writer.topicsToPeers.get(topic)?.size ?? 0;

			setResult({
				nodes: n,
				degree,
				subscribers: subscriberCount,
				messages,
				msgSize,
				subscribeModel,
				writerKnown,
				expectedDeliveries,
				observedDeliveries,
				subscribeMs: subscribeDone - subscribeStart,
				publishMs: publishDone - publishStart,
				p50: latencies.length ? quantile(latencies, 0.5) : undefined,
				p95: latencies.length ? quantile(latencies, 0.95) : undefined,
				p99: latencies.length ? quantile(latencies, 0.99) : undefined,
					max: latencies.length ? latencies[latencies.length - 1] : undefined,
					framesSent: network.metrics.framesSent,
					bytesSent: network.metrics.bytesSent,
					flowMode,
					flowDataFrames,
					flowAckFrames,
				});
			setStatus("done");
		} catch (e: any) {
			await stopAll();
			runRef.current = null;
			setPublishProgress(null);
			setStatus("error");
			setError(e?.message ?? String(e));
		}
	};

	useEffect(() => {
		const canvas = canvasRef.current;
		if (!canvas) return;
		const ctx = canvas.getContext("2d");
		if (!ctx) return;

		let raf = 0;
			const draw = () => {
				const g = graph;
				const w = Math.max(1, size.w || 760);
				const h = Math.max(1, size.h || initialHeight);

			canvas.width = w * devicePixelRatio;
			canvas.height = h * devicePixelRatio;
			canvas.style.width = `${w}px`;
			canvas.style.height = `${h}px`;
			ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
			ctx.clearRect(0, 0, w, h);

			if (!g) {
				ctx.fillStyle = "#64748b";
				ctx.font = "14px ui-sans-serif, system-ui, -apple-system";
				ctx.fillText("Generating…", 16, 24);
				raf = requestAnimationFrame(draw);
				return;
			}

				// Edges
				ctx.lineWidth = 1;
				ctx.strokeStyle = "rgba(100, 116, 139, 0.35)";
				for (const e of g.edges) {
					const a = g.nodes[e.a]!.pos;
					const b = g.nodes[e.b]!.pos;
					ctx.beginPath();
					ctx.moveTo(a.x, a.y);
					ctx.lineTo(b.x, b.y);
					ctx.stroke();
				}

				const now = performance.now();
				if (showEdgeFlows) {
					const benchColors = flowMode === "bench";
					const heat = edgeHeatRef.current;
					if (heat) {
						for (let i = 0; i < g.edges.length; i++) {
							const v = heat[i]!;
							if (v <= 0.01) continue;
							const e = g.edges[i]!;
							const a = g.nodes[e.a]!.pos;
							const b = g.nodes[e.b]!.pos;
							ctx.strokeStyle = `rgba(14,165,233,${0.9 * v})`;
							ctx.lineWidth = 1.5 + v * 4.5;
							ctx.beginPath();
							ctx.moveTo(a.x, a.y);
							ctx.lineTo(b.x, b.y);
							ctx.stroke();
							heat[i] = v * 0.97;
						}
					}

					const buf = flowBufRef.current;
					if (buf) {
						for (let i = 0; i < buf.cap; i++) {
							const dur = buf.durationMs[i]!;
							if (dur === 0) continue;
							const age = now - buf.startMs[i]!;
							if (age < 0 || age > dur) continue;
							const t = age / dur;
							const kind = buf.kind[i] ?? 0;
							const seq = buf.seq[i] ?? 0;
							const from = buf.from[i]!;
							const to = buf.to[i]!;
							const a = g.nodes[from]?.pos;
							const b = g.nodes[to]?.pos;
							if (!a || !b) continue;
							const x = a.x + (b.x - a.x) * t;
							const y = a.y + (b.y - a.y) * t;
							const alpha = kind === 1 ? 0.85 : 0.9;
							const radius = kind === 1 ? 2.6 : 3.2;

							// Draw a short "comet tail" along the edge so movement reads as a line animation.
							{
								const tail = 0.12;
								const t0 = Math.max(0, t - tail);
								const x0 = a.x + (b.x - a.x) * t0;
								const y0 = a.y + (b.y - a.y) * t0;
								const tailAlpha = kind === 1 ? 0.42 : 0.5;
								if (kind === 1) {
									ctx.strokeStyle = `rgba(249,115,22,${tailAlpha})`;
								} else if (benchColors) {
									ctx.strokeStyle = benchColor(seq, tailAlpha);
								} else {
								ctx.strokeStyle = `rgba(14,165,233,${tailAlpha})`;
							}
								ctx.lineWidth = kind === 1 ? 2 : 2.4;
								ctx.lineCap = "round";
								ctx.beginPath();
								ctx.moveTo(x0, y0);
								ctx.lineTo(x, y);
								ctx.stroke();
							}

							if (kind === 1) {
								ctx.fillStyle = `rgba(249,115,22,${alpha})`;
							} else if (benchColors) {
								ctx.fillStyle = benchColor(seq, alpha);
							} else {
								ctx.fillStyle = `rgba(14,165,233,${alpha})`;
							}
							ctx.beginPath();
							ctx.arc(x, y, radius, 0, Math.PI * 2);
							ctx.fill();
						}
					}
				}

							const pulseAt = pulseAtRef.current;
							const receivedCount = receivedCountRef.current;
							const pulseDurationMs = 1500;

			// Nodes
			for (const n of g.nodes) {
				const isWriter = n.id === writerIndex;
				const received = (receivedCount?.[n.id] ?? 0) > 0;

				ctx.fillStyle = isWriter
					? "rgba(239,68,68,0.95)"
					: received
						? "rgba(34,197,94,0.95)"
						: "rgba(148,163,184,0.95)";
				ctx.beginPath();
				ctx.arc(n.pos.x, n.pos.y, 7, 0, Math.PI * 2);
				ctx.fill();

				if (pulseAt && Number.isFinite(pulseAt[n.id]!) && now - pulseAt[n.id]! <= pulseDurationMs) {
					const age = now - pulseAt[n.id]!;
					const t = age / pulseDurationMs;
					const r = 9 + t * 18;
					ctx.strokeStyle = `rgba(34,197,94,${0.45 * (1 - t)})`;
					ctx.lineWidth = 2;
					ctx.beginPath();
					ctx.arc(n.pos.x, n.pos.y, r, 0, Math.PI * 2);
					ctx.stroke();
				}
			}

			raf = requestAnimationFrame(draw);
		};

		raf = requestAnimationFrame(draw);
		return () => cancelAnimationFrame(raf);
	}, [flowMode, graph, initialHeight, showEdgeFlows, size.h, size.w, writerIndex]);

	const onClickCanvas = (ev: ReactMouseEvent<HTMLCanvasElement>) => {
		const g = graph;
		if (!g) return;
		const rect = ev.currentTarget.getBoundingClientRect();
		const x = ev.clientX - rect.left;
		const y = ev.clientY - rect.top;
		let best = -1;
		let bestDist2 = 16 * 16;
		for (const n of g.nodes) {
			const dx = n.pos.x - x;
			const dy = n.pos.y - y;
			const d2 = dx * dx + dy * dy;
			if (d2 < bestDist2) {
				bestDist2 = d2;
				best = n.id;
			}
		}
		if (best >= 0) setWriterIndex(best);
	};

	const statusLabel = useMemo(() => {
		if (status === "idle") return "Idle";
		if (status === "setting-up") return "Setting up…";
		if (status === "running") return "Running…";
		if (status === "done") return "Done";
		return "Error";
	}, [status]);

	return (
		<div className={className} ref={containerRef}>
			<div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-800 dark:bg-slate-950">
				<div className="flex flex-col gap-4">
					<div className="flex flex-wrap items-start justify-between gap-2">
						<div>
							<div className="text-sm font-semibold text-slate-900 dark:text-slate-50">
								Real protocol sandbox (DirectSub over in-memory libp2p)
							</div>
							<div className="text-xs text-slate-500 dark:text-slate-400">
								Status: <span className="font-mono">{statusLabel}</span>
							</div>
						</div>
					</div>

					<div className="grid gap-4 lg:grid-cols-[360px,1fr]">
						<div className="flex flex-col gap-3">
							<details
								open
								className="rounded-xl border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-950"
							>
								<summary className="flex cursor-pointer list-none items-center justify-between gap-2 px-3 py-2 text-sm font-semibold text-slate-900 dark:text-slate-50 [&::-webkit-details-marker]:hidden">
									<span>Quick settings</span>
									<span className="text-xs font-normal text-slate-500 dark:text-slate-400">common</span>
								</summary>
								<div className="px-3 pb-3 pt-1">
									<div className="grid gap-3 sm:grid-cols-2">
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Nodes (max 1000)
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={3}
												max={1000}
												value={nodes}
												onChange={(e) => setNodes(readIntAttr(e.target.value, nodes, 3, 1000))}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Degree
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={1}
												max={16}
												value={degree}
												onChange={(e) => setDegree(readIntAttr(e.target.value, degree, 1, 16))}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Layout
											<select
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												value={layout}
												onChange={(e) => setLayout(e.target.value as Layout)}
												disabled={status === "setting-up" || status === "running"}
											>
												<option value="force">Force (network-like)</option>
												<option value="scatter">Scatter</option>
												<option value="circle">Circle</option>
											</select>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Subscribers
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={1}
												max={Math.max(1, nodes - 1)}
												value={subscribers}
												onChange={(e) =>
													setSubscribers(
														readIntAttr(e.target.value, subscribers, 1, Math.max(1, nodes - 1)),
													)
												}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Messages
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={1}
												max={200}
												value={messages}
												onChange={(e) => setMessages(readIntAttr(e.target.value, messages, 1, 200))}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Flow speed (ms)
												<input
													className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm disabled:opacity-60 dark:border-slate-800 dark:bg-slate-900"
													type="number"
													min={80}
													max={10_000}
												value={
													syncFlowToRxDelay
															? Math.max(80, streamRxDelayMs)
															: flowDurationMs
													}
													onChange={(e) => {
														const next = readIntAttr(
															e.target.value,
															syncFlowToRxDelay ? streamRxDelayMs : flowDurationMs,
															80,
															10_000,
														);
														setFlowDurationMs(next);
														if (syncFlowToRxDelay) setStreamRxDelayMs(next);
													}}
													disabled={
														!showEdgeFlows ||
														status === "setting-up" ||
														status === "running"
													}
												/>
										</label>
										<label className="flex items-center gap-2 pt-1 text-xs text-slate-600 dark:text-slate-300">
											<input
												type="checkbox"
												checked={showEdgeFlows}
												onChange={(e) => setShowEdgeFlows(e.target.checked)}
												disabled={status === "setting-up" || status === "running"}
											/>
											Show flow
										</label>
											<label className="flex items-center gap-2 pt-1 text-xs text-slate-600 dark:text-slate-300">
												<input
													type="checkbox"
													checked={syncFlowToRxDelay}
													onChange={(e) => {
														const checked = e.target.checked;
														setSyncFlowToRxDelay(checked);
														// Keep the visible speed stable when toggling, but if enabling sync
														// also align rx delay so comets and "received" pulses match.
														if (checked) {
															setStreamRxDelayMs(clamp(flowDurationMs, 0, 10_000));
														} else {
															setFlowDurationMs(clamp(Math.max(80, streamRxDelayMs), 80, 10_000));
														}
													}}
													disabled={
														!showEdgeFlows || status === "setting-up" || status === "running"
													}
												/>
												Sync flow speed to rx delay
										</label>
									</div>
								</div>
							</details>

							<details className="rounded-xl border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-950">
								<summary className="flex cursor-pointer list-none items-center justify-between gap-2 px-3 py-2 text-sm font-semibold text-slate-900 dark:text-slate-50 [&::-webkit-details-marker]:hidden">
									<span>Advanced</span>
									<span className="text-xs font-normal text-slate-500 dark:text-slate-400">protocol + timing</span>
								</summary>
								<div className="px-3 pb-3 pt-1">
									<div className="grid gap-3 sm:grid-cols-2">
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Subscribe model
											<select
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												value={subscribeModel}
												onChange={(e) =>
													setSubscribeModel(e.target.value as Result["subscribeModel"])
												}
												disabled={status === "setting-up" || status === "running"}
											>
												<option value="preseed">Preseed (no subscribe gossip)</option>
												<option value="real">Real subscribe (gossipy)</option>
											</select>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Message size (bytes)
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={0}
												max={1_000_000}
												value={msgSize}
												onChange={(e) =>
													setMsgSize(readIntAttr(e.target.value, msgSize, 0, 1_000_000))
												}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Interval (ms)
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={0}
												max={10_000}
												value={intervalMs}
												onChange={(e) =>
													setIntervalMs(readIntAttr(e.target.value, intervalMs, 0, 10_000))
												}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Stream rx delay (ms)
												<input
													className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
													type="number"
													min={0}
													max={10_000}
													value={streamRxDelayMs}
													onChange={(e) =>
														setStreamRxDelayMs(
															readIntAttr(e.target.value, streamRxDelayMs, 0, 10_000),
														)
													}
													disabled={status === "setting-up" || status === "running"}
												/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Dial delay (ms)
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={0}
												max={10_000}
												value={dialDelayMs}
												onChange={(e) =>
													setDialDelayMs(readIntAttr(e.target.value, dialDelayMs, 0, 10_000))
												}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Seed
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={0}
												max={1_000_000_000}
												value={seed}
												onChange={(e) =>
													setSeed(readIntAttr(e.target.value, seed, 0, 1_000_000_000))
												}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Flow source
											<select
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												value={flowMode}
												onChange={(e) => setFlowMode(e.target.value as FlowMode)}
												disabled={
													!showEdgeFlows || status === "setting-up" || status === "running"
												}
											>
												<option value="bench">Bench messages (PSIM)</option>
												<option value="stream">All stream frames (advanced)</option>
											</select>
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Flow capture
											<select
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												value={flowCapture}
												onChange={(e) => setFlowCapture(e.target.value as FlowCapture)}
												disabled={
													!showEdgeFlows || status === "setting-up" || status === "running"
												}
											>
												<option value="publish-only">Publish only (hide setup)</option>
												<option value="setup+publish">Include setup + subscribe</option>
											</select>
										</label>
										<label className="flex items-center gap-2 pt-1 text-xs text-slate-600 dark:text-slate-300">
											<input
												type="checkbox"
												checked={showAckFlows}
												onChange={(e) => setShowAckFlows(e.target.checked)}
												disabled={
													!showEdgeFlows || status === "setting-up" || status === "running"
												}
											/>
											Show ACK return path
										</label>
										<label className="block text-xs text-slate-600 dark:text-slate-300">
											Subscription debounce (ms)
											<input
												className="mt-1 w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm dark:border-slate-800 dark:bg-slate-900"
												type="number"
												min={0}
												max={10_000}
												value={subscriptionDebounceDelayMs}
												onChange={(e) =>
													setSubscriptionDebounceDelayMs(
														readIntAttr(
															e.target.value,
															subscriptionDebounceDelayMs,
															0,
															10_000,
														),
													)
												}
												disabled={status === "setting-up" || status === "running"}
											/>
										</label>
									</div>
								</div>
							</details>

							{layoutNote ? (
								<div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-900/40 dark:bg-amber-950/40 dark:text-amber-200">
									{layoutNote}
								</div>
							) : null}
						</div>

						<div className="flex flex-col gap-3">
							<div className="flex flex-wrap items-center justify-between gap-2">
								<div className="flex flex-wrap items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
									<span>
										Click a node to set the writer (red). Pulses = data received on subscribers.
									</span>
									<InfoPopover>
										<div className="space-y-2">
											<p>
												Flow comets show traffic along graph edges. In <span className="font-mono">Bench</span>{" "}
												mode they follow only published <span className="font-mono">PSIM</span> messages and
												are colored by message sequence (0..messages-1).
											</p>
											<p>
												Because we <span className="font-mono">await publish()</span>, messages are sent
												sequentially (you may see bursts separated by a bit, especially with rx delay).
												Comet speed is an animation choice: if you make it very slow (and don’t sync to
												rx delay), nodes can turn green before a comet reaches them.
											</p>
											<p>
												Blue edge glow is just “recently used edge” heat. Optional orange comets show ACK
												return paths. This runs the real DirectSub/DirectStream logic, but skips crypto
												verification for speed.
											</p>
										</div>
									</InfoPopover>
								</div>

								<div className="flex flex-wrap justify-end gap-2">
									<button
										className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-900 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-50 dark:hover:bg-slate-800"
										onClick={regenerateGraph}
										disabled={status === "setting-up" || status === "running"}
									>
										Generate network
									</button>
									<button
										className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:opacity-60 dark:bg-slate-50 dark:text-slate-900 dark:hover:bg-slate-200"
										onClick={() => void start()}
										disabled={status === "setting-up" || status === "running" || !graph}
									>
										Run
									</button>
									<button
										className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-900 hover:bg-slate-50 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-50 dark:hover:bg-slate-800"
										onClick={() => void stop()}
										disabled={status === "idle"}
									>
										Stop
									</button>
								</div>
							</div>

							{showEdgeFlows && flowMode === "bench" ? (
								<div className="flex flex-wrap items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
									<span>Message colors:</span>
									{Array.from({ length: Math.min(12, messages) }, (_, i) => (
										<span key={i} className="inline-flex items-center gap-1 font-mono">
											<span
												className="inline-block h-2.5 w-2.5 rounded-sm"
												style={{ backgroundColor: benchColor(i, 1) }}
											/>
											{i}
										</span>
									))}
									{messages > 12 ? <span>+{messages - 12} more</span> : null}
									{status === "running" && publishProgress ? (
										<span className="ml-1">
											· publishing {publishProgress.seq + 1}/{publishProgress.total}
										</span>
									) : null}
								</div>
							) : null}

							<div className="overflow-hidden rounded-xl border border-slate-200 dark:border-slate-800">
								<canvas ref={canvasRef} onClick={onClickCanvas} />
							</div>

							{error ? (
								<div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-800 dark:border-red-900/40 dark:bg-red-950/40 dark:text-red-200">
									{error}
								</div>
							) : null}

							{result ? (
								<div className="grid gap-2 sm:grid-cols-2">
									<div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-200">
										<div className="font-medium">Delivery</div>
										<div className="mt-1 font-mono">
											observed {result.observedDeliveries}/{result.expectedDeliveries} (writerKnown=
											{result.writerKnown})
										</div>
										<div className="mt-1 text-slate-500 dark:text-slate-400">
											subscribe {result.subscribeMs}ms · publish {result.publishMs}ms
										</div>
									</div>
									<div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-200">
										<div className="font-medium">Latency + transport</div>
										<div className="mt-1 font-mono">
											{result.p50 != null ? `p50=${result.p50.toFixed(1)}ms` : "p50=–"}{" "}
											{result.p95 != null ? `p95=${result.p95.toFixed(1)}ms` : "p95=–"}{" "}
											{result.p99 != null ? `p99=${result.p99.toFixed(1)}ms` : "p99=–"}{" "}
											{result.max != null ? `max=${result.max.toFixed(1)}ms` : "max=–"}
										</div>
										<div className="mt-1 text-slate-500 dark:text-slate-400">
											framesSent={result.framesSent} · bytesSent={result.bytesSent}
										</div>
										<div className="mt-1 text-slate-500 dark:text-slate-400">
											flow={result.flowMode} data={result.flowDataFrames} ack={result.flowAckFrames} ·
											overhead=
											{(result.flowDataFrames / Math.max(1, result.expectedDeliveries)).toFixed(2)}x
										</div>
									</div>
								</div>
							) : (
								<div className="text-xs text-slate-500 dark:text-slate-400">
									Press Run to build a small in-memory libp2p network and publish.
								</div>
							)}
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}
