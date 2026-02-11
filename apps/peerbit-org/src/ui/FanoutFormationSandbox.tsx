/**
 * Interactive demo: how a FanoutTree topology forms as peers join via a bootstrap/tracker.
 *
 * This runs the real @peerbit/pubsub FanoutTree join logic over the in-memory libp2p shim
 * (no sockets). The visual is a simplified tree layout focused on parent selection + capacity.
 */

import type { ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

import type { DirectStreamComponents } from "@peerbit/stream";
import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import { FanoutChannel, FanoutTree } from "@peerbit/pubsub";

import {
	InMemoryConnectionManager,
	InMemoryNetwork,
	type InMemoryNetworkMetrics,
} from "../sim/inmemory-libp2p.js";

type Vec2 = { x: number; y: number };

type RunStatus = "idle" | "initializing" | "ready" | "joining" | "done" | "error";

type Edge = { from: number; to: number };

type NodeState = "root" | "online" | "orphan" | "offline";

type LayoutResult = {
	pos: Vec2[];
	levelByNode: Uint16Array;
	maxLevel: number;
};

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

class SimFanoutTree extends FanoutTree {
	constructor(components: DirectStreamComponents, opts?: { random?: () => number }) {
		super(components, {
			connectionManager: false,
			random: opts?.random,
		});

		// Fast/mock signing: we want join selection + routing semantics, not crypto cost.
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

const computeLayout = (opts: {
	joined: number;
	levelByNode: Uint16Array;
	width: number;
	height: number;
}): LayoutResult => {
	const joined = Math.max(1, Math.floor(opts.joined));
	const width = Math.max(240, Math.floor(opts.width));
	const height = Math.max(220, Math.floor(opts.height));

	let maxLevel = 0;
	for (let i = 0; i < joined; i++) maxLevel = Math.max(maxLevel, opts.levelByNode[i] ?? 0);

	const perLevel: number[][] = Array.from({ length: maxLevel + 1 }, () => []);
	for (let i = 0; i < joined; i++) {
		const lvl = opts.levelByNode[i] ?? 0;
		perLevel[Math.min(maxLevel, lvl)]!.push(i);
	}

	const marginX = 24;
	const marginY = 24;
	const availableW = Math.max(1, width - marginX * 2);
	const availableH = Math.max(1, height - marginY * 2);
	const gapY = maxLevel > 0 ? availableH / maxLevel : 0;

	const pos: Vec2[] = Array.from({ length: joined }, () => ({ x: 0, y: 0 }));
	for (let lvl = 0; lvl <= maxLevel; lvl++) {
		const nodes = perLevel[lvl]!;
		const y = marginY + lvl * gapY;
		const count = nodes.length;
		for (let k = 0; k < count; k++) {
			const i = nodes[k]!;
			const x = marginX + ((k + 1) * availableW) / (count + 1);
			pos[i] = { x, y };
		}
	}

	return { pos, levelByNode: opts.levelByNode, maxLevel };
};

type FanoutFormationSandboxProps = {
	node?: unknown;
	className?: string;
	nodes?: string;
	rootMaxChildren?: string;
	nodeMaxChildren?: string;
	joinIntervalMs?: string;
	seed?: string;
	height?: string;
};

type RunState = {
	network: InMemoryNetwork;
	abort: AbortController;
	config: {
		nodes: number;
		rootMaxChildren: number;
		nodeMaxChildren: number;
		seed: number;
	};
	peers: Array<{
		index: number;
		fanout: SimFanoutTree;
		channel: FanoutChannel;
		hash: string;
	}>;
	hashToIndex: Map<string, number>;
	rootAddr: string;
	topic: string;
	rootHash: string;
	metrics: () => InMemoryNetworkMetrics;
	stop: () => Promise<void>;
};

type Pulse = {
	from: number;
	to: number;
	startMs: number;
	durationMs: number;
	color: string;
};

export function FanoutFormationSandbox({
	className,
	nodes: nodesAttr,
	rootMaxChildren: rootMaxChildrenAttr,
	nodeMaxChildren: nodeMaxChildrenAttr,
	joinIntervalMs: joinIntervalMsAttr,
	seed: seedAttr,
	height: heightAttr,
}: FanoutFormationSandboxProps) {
	const initialNodes = readIntAttr(nodesAttr, 80, 3, 1000);
	const initialRootMaxChildren = readIntAttr(rootMaxChildrenAttr, 4, 1, 64);
	const initialNodeMaxChildren = readIntAttr(nodeMaxChildrenAttr, 4, 0, 64);
	const initialJoinIntervalMs = readIntAttr(joinIntervalMsAttr, 250, 0, 10_000);
	const initialSeed = readIntAttr(seedAttr, 1, 0, 1_000_000_000);
	const initialHeight = readIntAttr(heightAttr, 420, 240, 900);

	const [nodes, setNodes] = useState(initialNodes);
	const [rootMaxChildren, setRootMaxChildren] = useState(initialRootMaxChildren);
	const [nodeMaxChildren, setNodeMaxChildren] = useState(initialNodeMaxChildren);
	const [joinIntervalMs, setJoinIntervalMs] = useState(initialJoinIntervalMs);
	const [seed, setSeed] = useState(initialSeed);
	const [height, setHeight] = useState(initialHeight);

	// Inputs are editable, but changes take effect only after Reset.
	const [applied, setApplied] = useState(() => ({
		nodes: initialNodes,
		rootMaxChildren: initialRootMaxChildren,
		nodeMaxChildren: initialNodeMaxChildren,
		seed: initialSeed,
	}));

	const [status, setStatus] = useState<RunStatus>("idle");
	const [error, setError] = useState<string | null>(null);

	const [joined, setJoined] = useState(0);
	const joinedRef = useRef(0);
	const [edges, setEdges] = useState<Edge[]>([]);
	const [events, setEvents] = useState<string[]>([]);

	const containerRef = useRef<HTMLDivElement | null>(null);
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	const [width, setWidth] = useState(640);
	const widthRef = useRef(640);
	const heightRef = useRef(initialHeight);

	const runRef = useRef<RunState | null>(null);
	const pulsesRef = useRef<Pulse[]>([]);
	const rafRef = useRef<number | null>(null);

	const parentByNodeRef = useRef<Array<number | undefined>>([]);
	const levelByNodeRef = useRef<Uint16Array>(new Uint16Array(1024));
	const edgesRef = useRef<Edge[]>([]);
	const appliedRef = useRef(applied);
	const statusRef = useRef<RunStatus>(status);
	const nodeStateByIndexRef = useRef<NodeState[]>([]);
	const offlineByIndexRef = useRef<boolean[]>([]);
	const [offlineCount, setOfflineCount] = useState(0);

	const appendEvent = (line: string) => {
		setEvents((prev) => {
			const next = [line, ...prev];
			return next.slice(0, 10);
		});
	};

	useEffect(() => {
		joinedRef.current = joined;
	}, [joined]);

	useEffect(() => {
		edgesRef.current = edges;
	}, [edges]);

	useEffect(() => {
		appliedRef.current = applied;
	}, [applied]);

	useEffect(() => {
		statusRef.current = status;
	}, [status]);

	useEffect(() => {
		widthRef.current = width;
	}, [width]);

	useEffect(() => {
		heightRef.current = height;
	}, [height]);

	const stopRun = async () => {
		const run = runRef.current;
		runRef.current = null;
		if (!run) return;
		try {
			run.abort.abort();
		} catch {
			// ignore
		}
		await run.stop();
	};

	const initLevelArrays = (target: number) => {
		const want = Math.max(8, Math.ceil(target / 8) * 8);
		if (levelByNodeRef.current.length < want) {
			levelByNodeRef.current = new Uint16Array(want);
		}
		levelByNodeRef.current.fill(0);
		parentByNodeRef.current = [];
		parentByNodeRef.current[0] = -1;
		levelByNodeRef.current[0] = 0;
		nodeStateByIndexRef.current = new Array(target).fill("online");
		nodeStateByIndexRef.current[0] = "root";
		offlineByIndexRef.current = new Array(target).fill(false);
		setOfflineCount(0);
	};

	const createPeer = async (run: RunState, index: number) => {
		const port = 40_000 + index;
		const { runtime } = InMemoryNetwork.createPeer({ index, port, network: run.network });
		runtime.connectionManager = new InMemoryConnectionManager(run.network, runtime);
		run.network.registerPeer(runtime, port);

		const rng = mulberry32((run.config.seed ^ (index * 0x9e3779b1)) >>> 0);
		const components: DirectStreamComponents = {
			peerId: runtime.peerId,
			privateKey: runtime.privateKey,
			addressManager: runtime.addressManager as any,
			registrar: runtime.registrar as any,
			connectionManager: runtime.connectionManager as any,
			peerStore: runtime.peerStore as any,
			events: runtime.events,
		};

		const fanout = new SimFanoutTree(components, { random: rng });
		await fanout.start();

		const hash = fanout.publicKeyHash;
		run.hashToIndex.set(hash, index);

		const channel = new FanoutChannel(fanout, { topic: run.topic, root: run.rootHash });
		return { index, fanout, channel, hash };
	};

	const reset = async () => {
		setError(null);
		setStatus("initializing");
		setJoined(0);
		setEdges([]);
		setEvents([]);
		pulsesRef.current = [];
		await stopRun();

		const abort = new AbortController();
		const network = new InMemoryNetwork();
		const hashToIndex = new Map<string, number>();
		const topic = "formation-demo";

		const run: RunState = {
			network,
			abort,
			config: {
				nodes: initialNodes,
				rootMaxChildren: initialRootMaxChildren,
				nodeMaxChildren: initialNodeMaxChildren,
				seed: initialSeed,
			},
			peers: [],
			hashToIndex,
			rootAddr: "",
			topic,
			rootHash: "",
			metrics: () => network.metrics,
			stop: async () => {
				await Promise.allSettled(run.peers.map((p) => p.fanout.stop()));
			},
		};
		runRef.current = run;

		try {
			run.config = {
				nodes,
				rootMaxChildren,
				nodeMaxChildren,
				seed,
			};
			setApplied(run.config);
			initLevelArrays(nodes);

			// Root (also acts as the bootstrap/tracker for this demo).
			const rootPeer = await createPeer(run, 0);
			run.peers.push(rootPeer);
			run.rootHash = rootPeer.hash;
			hashToIndex.set(rootPeer.hash, 0);

			const rootAddr = (rootPeer.fanout.components.addressManager as any).getAddresses?.()?.[0];
			run.rootAddr = rootAddr?.toString?.() ?? "";

			// Open root channel so it can accept JOIN_REQ and act as tracker.
			const rootChannel = FanoutChannel.fromSelf(rootPeer.fanout, topic);
			rootChannel.openAsRoot({
				msgRate: 30,
				msgSize: 256,
				uploadLimitBps: 1_000_000_000,
				maxChildren: Math.max(1, Math.floor(run.config.rootMaxChildren)),
				repair: false,
				allowKick: false,
			});

			setJoined(1);
			setStatus("ready");
			appendEvent("root opened channel and is ready for joins");
		} catch (e) {
			setStatus("error");
			setError(e instanceof Error ? e.message : String(e));
		}
	};

	const rebuildDerivedGraph = (joinedNow: number) => {
		const parentByNode = parentByNodeRef.current;
		const offlineByIndex = offlineByIndexRef.current;
		const nodeStateByIndex = nodeStateByIndexRef.current;

		const max = Math.max(1, joinedNow);
		const children: number[][] = Array.from({ length: max }, () => []);

		for (let i = 1; i < joinedNow; i++) {
			if (offlineByIndex[i]) continue;
			const parent = parentByNode[i];
			if (typeof parent !== "number" || parent < 0 || parent >= joinedNow) {
				nodeStateByIndex[i] = "orphan";
				continue;
			}
			if (offlineByIndex[parent]) {
				nodeStateByIndex[i] = "orphan";
				continue;
			}
			nodeStateByIndex[i] = "online";
			children[parent]!.push(i);
		}

		// BFS levels from root.
		levelByNodeRef.current.fill(0);
		levelByNodeRef.current[0] = 0;
		const q: number[] = [0];
		while (q.length > 0) {
			const p = q.shift()!;
			const lvl = levelByNodeRef.current[p] ?? 0;
			for (const c of children[p]!) {
				levelByNodeRef.current[c] = (lvl + 1) & 0xffff;
				q.push(c);
			}
		}

		const nextEdges: Edge[] = [];
		for (let i = 1; i < joinedNow; i++) {
			if (offlineByIndex[i]) continue;
			const parent = parentByNode[i];
			if (typeof parent !== "number" || parent < 0 || parent >= joinedNow) continue;
			if (offlineByIndex[parent]) continue;
			nextEdges.push({ from: parent, to: i });
		}

		const prev = edgesRef.current;
		let same = prev.length === nextEdges.length;
		if (same) {
			for (let i = 0; i < prev.length; i++) {
				const a = prev[i]!;
				const b = nextEdges[i]!;
				if (a.from !== b.from || a.to !== b.to) {
					same = false;
					break;
				}
			}
		}
		if (!same) setEdges(nextEdges);
	};

	const dropNode = async (index: number) => {
		const run = runRef.current;
		if (!run) return;
		if (index <= 0) {
			appendEvent("root cannot be deleted in this demo (it is also the tracker)");
			return;
		}
		if (offlineByIndexRef.current[index]) return;

		const peer = run.peers.find((p) => p.index === index);
		if (!peer) return;

		offlineByIndexRef.current[index] = true;
		nodeStateByIndexRef.current[index] = "offline";
		setOfflineCount(offlineByIndexRef.current.filter(Boolean).length);
		appendEvent(`node ${index} dropped (children will rejoin)`);

		try {
			// Close all streams/connections so remaining peers observe the disconnect quickly.
			const cm = (peer.fanout as any).components?.connectionManager as any;
			const conns = (cm?.getConnections?.() ?? []) as any[];
			const remotePeers = new Set<any>();
			for (const c of conns) {
				const rp = (c as any)?.remotePeer;
				if (rp) remotePeers.add(rp);
			}
			await Promise.allSettled(
				[...remotePeers.values()].map((rp) => cm?.closeConnections?.(rp)),
			);
		} catch {
			// ignore
		}

		try {
			await peer.fanout.stop();
		} catch {
			// ignore
		}

		try {
			run.network.unregisterPeer((peer.fanout as any).components.peerId);
		} catch {
			// ignore
		}

		// Mark any direct children as temporarily orphaned in the visual until they reattach.
		for (let i = 1; i < joinedRef.current; i++) {
			if (offlineByIndexRef.current[i]) continue;
			if (parentByNodeRef.current[i] === index) {
				parentByNodeRef.current[i] = undefined;
				nodeStateByIndexRef.current[i] = "orphan";
			}
		}

		rebuildDerivedGraph(joinedRef.current);
		requestDraw();
	};

	const joinAt = async (index: number) => {
		const run = runRef.current;
		if (!run) return;
		if (index <= 0) return;
		if (index >= run.config.nodes) return;
		try {
			const p = await createPeer(run, index);
			run.peers.push(p);
			if (index % 50 === 0) await delay(0);

			const maxChildren = Math.max(0, Math.floor(run.config.nodeMaxChildren));
			await p.channel.join(
				{
					msgRate: 30,
					msgSize: 256,
					uploadLimitBps: 1_000_000_000,
					maxChildren,
					repair: false,
					allowKick: false,
				},
				{
					bootstrap: run.rootAddr ? [run.rootAddr] : undefined,
					timeoutMs: 10_000,
					retryMs: 50,
					joinReqTimeoutMs: 1_000,
					trackerQueryTimeoutMs: 500,
					announceIntervalMs: 250,
					announceTtlMs: 5_000,
					bootstrapEnsureIntervalMs: 250,
					trackerQueryIntervalMs: 250,
					candidateCooldownMs: 500,
					joinAttemptsPerRound: 6,
					trackerCandidates: 24,
					candidateShuffleTopK: 8,
				},
			);

			const stats = p.fanout.getChannelStats(run.topic, run.rootHash);
			const parentHash = stats?.parent;
			const parentIndex = parentHash ? run.hashToIndex.get(parentHash) : undefined;
			if (parentIndex == null) {
				throw new Error(`Join succeeded but parent is unknown (${String(parentHash)})`);
			}

			parentByNodeRef.current[index] = parentIndex;
			nodeStateByIndexRef.current[index] = "online";
			rebuildDerivedGraph(index + 1);
			setJoined(index + 1);

			const pulseDurationMs = clamp(Math.max(250, joinIntervalMs), 250, 2_000);
			pulsesRef.current.push({
				from: parentIndex,
				to: index,
				startMs: performance.now(),
				durationMs: pulseDurationMs,
				color: "rgba(239, 68, 68, 0.95)", // red comet for JOIN_ACCEPT edge
			});

			const level = levelByNodeRef.current[index] ?? 0;
			appendEvent(`node ${index} joined via parent ${parentIndex} (level ${level})`);
		} catch (e) {
			setError(e instanceof Error ? e.message : String(e));
			throw e;
		}
	};

	const reconcileParents = () => {
		const run = runRef.current;
		if (!run) return;
		const joinedNow = joinedRef.current;
		if (joinedNow <= 1) return;

		let changed = false;
		const offlineByIndex = offlineByIndexRef.current;

		for (const p of run.peers) {
			const idx = p.index;
			if (idx <= 0) continue;
			if (idx >= joinedNow) continue;
			if (offlineByIndex[idx]) continue;

			const stats = p.fanout.getChannelStats(run.topic, run.rootHash);
			const parentHash = stats?.parent;
			const parentIndex = parentHash ? run.hashToIndex.get(parentHash) : undefined;
			const prev = parentByNodeRef.current[idx];

			if (parentIndex == null || offlineByIndex[parentIndex]) {
				if (prev !== undefined) {
					parentByNodeRef.current[idx] = undefined;
					nodeStateByIndexRef.current[idx] = "orphan";
					changed = true;
				}
				continue;
			}

			if (prev !== parentIndex) {
				parentByNodeRef.current[idx] = parentIndex;
				nodeStateByIndexRef.current[idx] = "online";
				changed = true;

				const pulseDurationMs = clamp(Math.max(250, joinIntervalMs), 250, 2_000);
				pulsesRef.current.push({
					from: parentIndex,
					to: idx,
					startMs: performance.now(),
					durationMs: pulseDurationMs,
					color: "rgba(59, 130, 246, 0.9)", // blue comet for reparent edge
				});
			}
		}

		if (changed) {
			rebuildDerivedGraph(joinedNow);
			requestDraw();
		}
	};

	useEffect(() => {
		const id = setInterval(() => reconcileParents(), 300);
		return () => clearInterval(id);
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [joinIntervalMs]);

	const joinOne = async () => {
		const run = runRef.current;
		if (!run) return;
		if (status === "initializing") return;
		if (status === "joining") return;
		const index = joinedRef.current;
		if (index >= run.config.nodes) return;

		setStatus("joining");
		setError(null);
		try {
			await joinAt(index);
			setStatus(index + 1 >= run.config.nodes ? "done" : "ready");
		} catch {
			setStatus("error");
		}
	};

	const joinAll = async () => {
		const run = runRef.current;
		if (!run) return;
		if (status === "initializing") return;
		if (status === "joining") return;
		setError(null);
		setStatus("joining");

		try {
			for (let i = joinedRef.current; i < run.config.nodes; i++) {
				if (run.abort.signal.aborted) break;
				// eslint-disable-next-line no-await-in-loop
				await joinAt(i);
				if (joinIntervalMs > 0) {
					// eslint-disable-next-line no-await-in-loop
					await delay(joinIntervalMs);
				} else {
					// Yield so the UI remains interactive.
					// eslint-disable-next-line no-await-in-loop
					await delay(0);
				}
			}
			if (!run.abort.signal.aborted) setStatus("done");
		} catch {
			if (!run.abort.signal.aborted) setStatus("error");
		} finally {
			if (run.abort.signal.aborted) {
				setStatus(joinedRef.current >= nodes ? "done" : "ready");
			}
		}
	};

	const stop = async () => {
		const run = runRef.current;
		if (!run) return;
		try {
			run.abort.abort();
		} catch {
			// ignore
		}
		setStatus(joinedRef.current >= run.config.nodes ? "done" : "ready");
	};

	useEffect(() => {
		if (!containerRef.current) return;
		const el = containerRef.current;
		const ro = new ResizeObserver(() => {
			const rect = el.getBoundingClientRect();
			setWidth(Math.max(240, Math.floor(rect.width)));
		});
		ro.observe(el);
		return () => ro.disconnect();
	}, []);

	const childCounts = useMemo(() => {
		const out = new Uint16Array(Math.max(1, joined));
		for (const e of edges) {
			if (e.from >= 0 && e.from < out.length) out[e.from] += 1;
		}
		return out;
	}, [edges, joined]);

	const layout = useMemo(
		() =>
			computeLayout({
				joined: Math.max(1, joined),
				levelByNode: levelByNodeRef.current,
				width,
				height,
			}),
		[joined, edges, width, height],
	);

	const stats = useMemo(() => {
		const maxLevel = layout.maxLevel;
		const rootChildren = joined > 0 ? childCounts[0] ?? 0 : 0;
		const networkMetrics = runRef.current?.metrics?.();
		return {
			maxLevel,
			rootChildren,
			dials: networkMetrics?.dials ?? 0,
			connectionsOpened: networkMetrics?.connectionsOpened ?? 0,
			streamsOpened: networkMetrics?.streamsOpened ?? 0,
		};
	}, [layout.maxLevel, childCounts, joined]);

	const requestDraw = () => {
		if (rafRef.current != null) return;
		rafRef.current = requestAnimationFrame(function tick(now) {
			const canvas = canvasRef.current;
			const run = runRef.current;
			if (!canvas || !run) {
				if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
				rafRef.current = null;
				return;
			}
			const ctx = canvas.getContext("2d");
			if (!ctx) {
				rafRef.current = null;
				return;
			}

			const joinedNow = joinedRef.current;
			const edgesNow = edgesRef.current;
			const appliedNow = appliedRef.current;
			const statusNow = statusRef.current;

			const dpr = Math.max(1, Math.floor(window.devicePixelRatio || 1));
			const w = Math.max(240, Math.floor(widthRef.current));
			const h = Math.max(220, Math.floor(heightRef.current));
			if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
				canvas.width = w * dpr;
				canvas.height = h * dpr;
				canvas.style.width = `${w}px`;
				canvas.style.height = `${h}px`;
				ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
			}

			const layoutNow = computeLayout({
				joined: Math.max(1, joinedNow),
				levelByNode: levelByNodeRef.current,
				width: w,
				height: h,
			});

			const childCountsNow = new Uint16Array(Math.max(1, joinedNow));
			for (const e of edgesNow) {
				if (e.from >= 0 && e.from < childCountsNow.length) childCountsNow[e.from] += 1;
			}

			ctx.clearRect(0, 0, w, h);

			// Edges
			ctx.lineWidth = 1;
			ctx.strokeStyle = "rgba(148, 163, 184, 0.7)"; // slate-400-ish
			for (const e of edgesNow) {
				const a = layoutNow.pos[e.from];
				const b = layoutNow.pos[e.to];
				if (!a || !b) continue;
				ctx.beginPath();
				ctx.moveTo(a.x, a.y);
				ctx.lineTo(b.x, b.y);
				ctx.stroke();
			}

			// Pulses (JOIN_ACCEPT comets)
			const pulses = pulsesRef.current;
			const alive: Pulse[] = [];
			for (const p of pulses) {
				const t = (now - p.startMs) / p.durationMs;
				if (t < 0) continue;
				if (t > 1) continue;
				const a = layoutNow.pos[p.from];
				const b = layoutNow.pos[p.to];
				if (!a || !b) continue;
				const x = a.x + (b.x - a.x) * t;
				const y = a.y + (b.y - a.y) * t;
				ctx.fillStyle = p.color;
				ctx.beginPath();
				ctx.arc(x, y, 3.2, 0, Math.PI * 2);
				ctx.fill();
				alive.push(p);
			}
			pulsesRef.current = alive;

			// Nodes
			const r = clamp(9 - Math.log2(Math.max(2, joinedNow)), 3.5, 7.5);
			for (let i = 0; i < joinedNow; i++) {
				const p = layoutNow.pos[i]!;
				const state: NodeState =
					i === 0
						? "root"
						: offlineByIndexRef.current[i]
							? "offline"
							: nodeStateByIndexRef.current[i] ?? "online";
				const max = i === 0 ? appliedNow.rootMaxChildren : appliedNow.nodeMaxChildren;
				const used = childCountsNow[i] ?? 0;
				const full = max > 0 && used >= max;
				const isRoot = state === "root";

				let fill = "rgba(34, 197, 94, 0.95)"; // green (online)
				if (state === "offline") fill = "rgba(148, 163, 184, 0.95)"; // slate
				else if (state === "orphan") fill = "rgba(59, 130, 246, 0.95)"; // blue
				else if (full && !isRoot) fill = "rgba(245, 158, 11, 0.95)"; // amber
				if (isRoot) fill = "rgba(239, 68, 68, 0.95)"; // red

				ctx.fillStyle = fill;
				ctx.beginPath();
				ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
				ctx.fill();

				ctx.strokeStyle = "rgba(15, 23, 42, 0.25)";
				ctx.lineWidth = 1;
				ctx.stroke();
			}

			const keepAnimating =
				pulsesRef.current.length > 0 || statusNow === "joining";
			if (keepAnimating) {
				rafRef.current = requestAnimationFrame(tick);
			} else {
				rafRef.current = null;
			}
		});
	};

	useEffect(() => {
		requestDraw();
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [edges, joined, width, height, applied.rootMaxChildren, applied.nodeMaxChildren, status]);

	useEffect(() => {
		void reset();
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

	useEffect(() => {
		return () => {
			void stopRun();
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

	return (
		<div className={["w-full", className].filter(Boolean).join(" ")}>
			<div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-800 dark:bg-slate-950">
				<div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
					<div className="min-w-0">
						<h3 className="text-base font-semibold text-slate-900 dark:text-slate-50">
							Network formation sandbox (FanoutTree join over in-memory libp2p)
						</h3>
						<p className="mt-1 text-sm text-slate-600 dark:text-slate-300">
					Status:{" "}
							<span className="font-medium">
								{status === "idle"
									? "Idle"
									: status === "initializing"
										? "Initializing"
										: status === "ready"
											? "Ready"
											: status === "joining"
												? "Joining"
												: status === "done"
													? "Done"
													: "Error"}
							</span>{" "}
							· joined {Math.max(0, joined)}/{applied.nodes} · offline {offlineCount} · max
							level {stats.maxLevel}
						</p>
					</div>
					<div className="flex flex-wrap items-center justify-start gap-2 md:justify-end">
						<button
							type="button"
							className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-800 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100 dark:hover:bg-slate-900"
							onClick={() => void reset()}
							disabled={status === "initializing" || status === "joining"}
						>
							Reset
						</button>
						<button
							type="button"
							className="rounded-xl bg-slate-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:opacity-50 dark:bg-slate-100 dark:text-slate-950 dark:hover:bg-slate-200"
							onClick={() => void joinOne()}
							disabled={status !== "ready" || joined >= applied.nodes}
						>
							Step join
						</button>
						<button
							type="button"
							className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-800 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100 dark:hover:bg-slate-900"
							onClick={() => void joinAll()}
							disabled={status !== "ready" || joined >= applied.nodes}
						>
							Auto
						</button>
						<button
							type="button"
							className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-800 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100 dark:hover:bg-slate-900"
							onClick={() => void stop()}
							disabled={status !== "joining"}
						>
							Stop
						</button>
					</div>
				</div>

				{error ? (
					<div className="mt-3 rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800 dark:border-rose-900/60 dark:bg-rose-950/40 dark:text-rose-200">
						{error}
					</div>
				) : null}

				<details className="mt-4 rounded-xl border border-slate-200 p-3 dark:border-slate-800">
					<summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold text-slate-900 dark:text-slate-50 [&::-webkit-details-marker]:hidden">
						<span className="flex items-center gap-2">
							Settings{" "}
							<InfoPopover>
								<div className="space-y-2">
									<p>
										This demo focuses on join formation. The bootstrap node (index 0) also
										acts as a tracker and will redirect joiners once it is full.
									</p>
									<p>
										Churn: click a non-root node in the graph to drop it (simulate going
										offline). Orphans (blue) will reattach by re-running the join loop.
									</p>
									<p>
										Root capacity is controlled by <code>rootMaxChildren</code>. Nodes with{" "}
										<code>nodeMaxChildren=0</code> become leaves and will not accept children.
									</p>
								</div>
							</InfoPopover>
						</span>
						<span className="text-xs font-medium text-slate-500 dark:text-slate-400">
							formation knobs
						</span>
					</summary>

					<div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
						<label className="block">
							<span className="text-xs font-medium text-slate-700 dark:text-slate-300">
								Nodes (max 1000)
							</span>
							<input
								className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-400 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100"
								type="number"
								min={3}
								max={1000}
								value={nodes}
								onChange={(e) => setNodes(clamp(Number(e.target.value || 0), 3, 1000))}
								disabled={status === "joining" || status === "initializing"}
							/>
						</label>
						<label className="block">
							<span className="text-xs font-medium text-slate-700 dark:text-slate-300">
								Root max children
							</span>
							<input
								className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-400 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100"
								type="number"
								min={1}
								max={64}
								value={rootMaxChildren}
								onChange={(e) =>
									setRootMaxChildren(clamp(Number(e.target.value || 0), 1, 64))
								}
								disabled={status === "joining" || status === "initializing"}
							/>
						</label>
						<label className="block">
							<span className="text-xs font-medium text-slate-700 dark:text-slate-300">
								Node max children
							</span>
							<input
								className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-400 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100"
								type="number"
								min={0}
								max={64}
								value={nodeMaxChildren}
								onChange={(e) =>
									setNodeMaxChildren(clamp(Number(e.target.value || 0), 0, 64))
								}
								disabled={status === "joining" || status === "initializing"}
							/>
						</label>
						<label className="block">
							<span className="text-xs font-medium text-slate-700 dark:text-slate-300">
								Join interval (ms)
							</span>
							<input
								className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-400 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100"
								type="number"
								min={0}
								max={10_000}
								value={joinIntervalMs}
								onChange={(e) =>
									setJoinIntervalMs(clamp(Number(e.target.value || 0), 0, 10_000))
								}
								disabled={status === "joining" || status === "initializing"}
							/>
						</label>
						<label className="block">
							<span className="text-xs font-medium text-slate-700 dark:text-slate-300">
								Seed
							</span>
							<input
								className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-400 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100"
								type="number"
								min={0}
								max={1_000_000_000}
								value={seed}
								onChange={(e) =>
									setSeed(clamp(Number(e.target.value || 0), 0, 1_000_000_000))
								}
								disabled={status === "joining" || status === "initializing"}
							/>
						</label>
						<label className="block">
							<span className="text-xs font-medium text-slate-700 dark:text-slate-300">
								Height (px)
							</span>
							<input
								className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-400 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-100"
								type="number"
								min={240}
								max={900}
								value={height}
								onChange={(e) => setHeight(clamp(Number(e.target.value || 0), 240, 900))}
							/>
						</label>
					</div>

					<div className="mt-3 text-xs text-slate-600 dark:text-slate-300">
						Bootstrapping: joiners dial node 0, query it for candidates, then JOIN_REQ a parent.
						When node 0 is full, it returns redirects to other nodes that still have free slots.
						<span className="ml-1">
							(Changes to nodes/capacity/seed apply after Reset.)
						</span>
					</div>
				</details>

				<div className="mt-4 flex flex-wrap items-center gap-3 text-xs text-slate-600 dark:text-slate-300">
					<div>
						Root children:{" "}
						<span className="font-medium text-slate-900 dark:text-slate-50">
							{stats.rootChildren}/{applied.rootMaxChildren}
						</span>
					</div>
					<div>
						Connections opened:{" "}
						<span className="font-medium text-slate-900 dark:text-slate-50">
							{stats.connectionsOpened}
						</span>
					</div>
					<div>
						Streams opened:{" "}
						<span className="font-medium text-slate-900 dark:text-slate-50">
							{stats.streamsOpened}
						</span>
					</div>
				</div>

				<div className="mt-4" ref={containerRef}>
					<canvas
						ref={canvasRef}
						className="w-full cursor-pointer rounded-xl border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-950"
						aria-label="FanoutTree formation graph"
						onClick={(e) => {
							const run = runRef.current;
							if (!run) return;
							const canvas = canvasRef.current;
							if (!canvas) return;
							const rect = canvas.getBoundingClientRect();
							const x = e.clientX - rect.left;
							const y = e.clientY - rect.top;

							const joinedNow = joinedRef.current;
							const layoutNow = computeLayout({
								joined: Math.max(1, joinedNow),
								levelByNode: levelByNodeRef.current,
								width: Math.max(240, Math.floor(widthRef.current)),
								height: Math.max(220, Math.floor(heightRef.current)),
							});

							let best = -1;
							let bestD2 = Number.POSITIVE_INFINITY;
							for (let i = 0; i < joinedNow; i++) {
								const p = layoutNow.pos[i];
								if (!p) continue;
								const dx = p.x - x;
								const dy = p.y - y;
								const d2 = dx * dx + dy * dy;
								if (d2 < bestD2) {
									bestD2 = d2;
									best = i;
								}
							}

							const hitRadius =
								clamp(9 - Math.log2(Math.max(2, joinedNow)), 3.5, 7.5) + 6;
							if (best >= 0 && bestD2 <= hitRadius * hitRadius) {
								void dropNode(best);
							}
						}}
					/>
				</div>

				{events.length > 0 ? (
					<div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-200">
						<div className="mb-2 font-semibold text-slate-900 dark:text-slate-50">
							Recent events
						</div>
						<ul className="space-y-1">
							{events.map((e, i) => (
								<li key={`${i}:${e}`}>{e}</li>
							))}
						</ul>
					</div>
				) : null}
			</div>
		</div>
	);
}
