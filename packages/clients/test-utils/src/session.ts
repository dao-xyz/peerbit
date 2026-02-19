import { yamux } from "@chainsafe/libp2p-yamux";
import { DirectBlock } from "@peerbit/blocks";
import { createStore } from "@peerbit/any-store";
import { keychain } from "@peerbit/keychain";
import { DefaultCryptoKeychain } from "@peerbit/keychain";
import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import {
	TestSession as SSession,
	listenFast,
	transportsFast,
} from "@peerbit/libp2p-test-utils";
import type { Indices, Index, IndexEngineInitProperties } from "@peerbit/indexer-interface";
import { type ProgramClient } from "@peerbit/program";
import { FanoutTree, TopicControlPlane, TopicRootControlPlane } from "@peerbit/pubsub";
import {
	type DirectStream,
	waitForNeighbour as waitForPeersStreams,
} from "@peerbit/stream";
import { type Libp2pOptions } from "libp2p";
import path from "path";
import {
	type Libp2pCreateOptions,
	type Libp2pCreateOptionsWithServices,
	type Libp2pExtendServices,
} from "peerbit";
import { Peerbit } from "peerbit";
import { InMemoryNetwork, InMemorySession } from "./inmemory-libp2p.js";

export type LibP2POptions = Libp2pOptions<Libp2pExtendServices>;

type CreateOptions = { libp2p?: Libp2pCreateOptions; directory?: string };

export type InMemoryPeerbitSessionOptions = {
	/**
	 * Create a sparse underlay graph by default (recommended for large n).
	 * If omitted, the session is returned disconnected and you can call `connect*()`.
	 */
	degree?: number;
	seed?: number;
	concurrency?: number;

	/**
	 * In-memory transport knobs.
	 */
	network?: ConstructorParameters<typeof InMemoryNetwork>[0];
	basePort?: number;

	/**
	 * Skip expensive crypto (sign/verify) in stream-based services.
	 * Keeps identity/session semantics but uses dummy signatures.
	 */
	mockCrypto?: boolean;

	/**
	 * Use an ultra-light indexer by default so thousands of peers don't each spin up
	 * a sqlite3 instance. Override if you need program/indexer behavior in a test.
	 */
	indexer?: (directory?: string) => Promise<Indices> | Indices;
};

class NoopIndices implements Indices {
	private readonly store = createStore();

	async init<T extends Record<string, any>, NestedType>(
		_properties: IndexEngineInitProperties<T, NestedType>,
	): Promise<Index<T, NestedType>> {
		throw new Error(
			"NoopIndices: indexing disabled for this session (pass `indexer` to enable).",
		);
	}

	async scope(_name: string): Promise<Indices> {
		return this;
	}

	async start(): Promise<void> {
		await this.store.open();
	}

	async stop(): Promise<void> {
		await this.store.close();
	}

	async drop(): Promise<void> {
		await this.store.clear();
	}
}

class SimTopicControlPlane extends TopicControlPlane {
	constructor(c: any, opts?: any, mockCrypto = true) {
		super(c, opts);
		if (mockCrypto) {
			this.sign = async () =>
				new SignatureWithKey({
					signature: new Uint8Array([0]),
					publicKey: this.publicKey,
					prehash: PreHash.NONE,
				});
		}
	}

	public async verifyAndProcess(message: any) {
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}
}

class SimFanoutTree extends FanoutTree {
	constructor(c: any, opts?: any, mockCrypto = true) {
		super(c, opts);
		if (mockCrypto) {
			this.sign = async () =>
				new SignatureWithKey({
					signature: new Uint8Array([0]),
					publicKey: this.publicKey,
					prehash: PreHash.NONE,
				});
		}
	}

	public async verifyAndProcess(message: any) {
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}
}

class SimDirectBlock extends DirectBlock {
	constructor(c: any, opts?: any, mockCrypto = true) {
		super(c, opts);
		if (mockCrypto) {
			this.sign = async () =>
				new SignatureWithKey({
					signature: new Uint8Array([0]),
					publicKey: this.publicKey,
					prehash: PreHash.NONE,
				});
		}
	}

	public async verifyAndProcess(message: any) {
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}
}

type SessionLike = {
	peers: any[];
	connect(groups?: any[][]): Promise<any>;
	stop(): Promise<any>;
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

const parseSimPeerIndex = (peerId: any): number => {
	const s = String(peerId?.toString?.() ?? "");
	const m = s.match(/sim-(\d+)/);
	if (!m) return 0;
	const n = Number(m[1]);
	return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 0;
};

const int = (rng: () => number, maxExclusive: number) =>
	Math.floor(rng() * maxExclusive);

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

	// Seed connectivity (line or ring).
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
	if (tasks.length === 0) return [];
	const results: T[] = new Array(tasks.length);
	let next = 0;
	const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, async () => {
		for (;;) {
			const i = next++;
			if (i >= tasks.length) return;
			results[i] = await tasks[i]!();
		}
	});
	await Promise.all(workers);
	return results;
};

export class TestSession {
	private session: SessionLike;
	private _peers: Peerbit[];
	private connectedGroups: Set<Peerbit>[] | undefined;
	readonly inMemory?: { session: InMemorySession<any>; network: InMemoryNetwork };

	constructor(
		session: SessionLike,
		peers: Peerbit[],
		opts?: { inMemory?: { session: InMemorySession<any>; network: InMemoryNetwork } },
	) {
		this.session = session;
		this._peers = peers;
		this.inMemory = opts?.inMemory;
		this.wrapPeerStartForReconnect();
	}

	public get peers(): ProgramClient[] {
		return this._peers;
	}

		private async configureFanoutShardRoots() {
			if (!this.connectedGroups) return;

			// Build a view of direct underlay adjacency from the original dial-groups.
			// This is used to pick "universally reachable" routers in sparse topologies
			// (e.g. a line) so that sharded pubsub roots are directly dialable.
			const adjacency = new Map<Peerbit, Set<Peerbit>>();
			const addAdj = (a: Peerbit, b: Peerbit) => {
				let sa = adjacency.get(a);
				if (!sa) {
					sa = new Set();
					adjacency.set(a, sa);
				}
				sa.add(b);
			};
			for (const groupSet of this.connectedGroups) {
				const peers = [...groupSet];
				for (let i = 0; i < peers.length; i++) {
					for (let j = i + 1; j < peers.length; j++) {
						const a = peers[i]!;
						const b = peers[j]!;
						addAdj(a, b);
						addAdj(b, a);
					}
				}
			}

			// `connect(groups)` is sometimes used to form sparse topologies by passing
			// overlapping "dial groups" (e.g. [[a,b],[b,c]] for a line). For shard-root
			// resolution we must instead configure candidates consistently per connected
			// component; otherwise a peer that appears in multiple groups would have its
		// candidate set overwritten and pubsub/rpc can deadlock.
		const mergedComponents: Set<Peerbit>[] = [];
		for (const groupSet of this.connectedGroups) {
			const intersections: number[] = [];
			for (let i = 0; i < mergedComponents.length; i++) {
				const existing = mergedComponents[i]!;
				for (const peer of groupSet) {
					if (existing.has(peer)) {
						intersections.push(i);
						break;
					}
				}
			}
			if (intersections.length === 0) {
				mergedComponents.push(new Set(groupSet));
				continue;
			}

			const union = new Set<Peerbit>(groupSet);
			intersections.sort((a, b) => b - a); // splice from end
			for (const idx of intersections) {
				for (const peer of mergedComponents[idx]!) union.add(peer);
				mergedComponents.splice(idx, 1);
			}
			mergedComponents.push(union);
		}

		const peerIndex = new Map<Peerbit, number>();
		for (let i = 0; i < this._peers.length; i++) {
			peerIndex.set(this._peers[i]!, i);
		}

			for (const groupSet of mergedComponents) {
				const group = [...groupSet].sort(
					(a, b) => (peerIndex.get(a) ?? 0) - (peerIndex.get(b) ?? 0),
				);
				if (group.length === 0) continue;

				// Prefer routers that are directly connected to every peer in this component
				// (e.g. the center node in a line). This avoids picking a root that some peers
				// cannot dial directly, which can cause fanout join timeouts in sparse graphs.
				const universalRouters = group.filter((p) => {
					const n = adjacency.get(p);
					if (!n) return false;
					for (const other of group) {
						if (other === p) continue;
						if (!n.has(other)) return false;
					}
					return true;
				});

				// Keep a small deterministic router set so shard roots are always hosted.
				// If no universally reachable peers exist, fall back to a bounded prefix.
				const routers =
					universalRouters.length > 0
						? universalRouters.slice(0, 8)
						: group.slice(
								0,
								Math.min(
									8,
									Math.max(1, Math.ceil(Math.log2(Math.max(1, group.length)))),
								),
							);
			const routerHashes = routers
				.map(
					(p) =>
						(p as any)?.services?.pubsub?.publicKeyHash ??
						(p as any)?.identity?.publicKey?.hashcode?.(),
				)
				.filter((x): x is string => typeof x === "string" && x.length > 0);

			if (routerHashes.length === 0) continue;

				for (const peer of group) {
					const servicesAny: any = (peer as any).services;
					// Prefer the pubsub service helper so we also disable its auto-candidate
					// mode (otherwise it can keep gossiping/expanding candidates and shard-root
					// resolution can diverge in sparse topologies).
					try {
						servicesAny?.pubsub?.setTopicRootCandidates?.(routerHashes);
					} catch {
						// ignore
					}

					// Also update the shared topic-root control plane directly for any
					// services that are not wired through pubsub.
					const fanoutPlane = servicesAny?.fanout?.topicRootControlPlane;
					const pubsubPlane = servicesAny?.pubsub?.topicRootControlPlane;
					const planes = [...new Set([fanoutPlane, pubsubPlane].filter(Boolean))];
					for (const plane of planes) {
						try {
							plane.setTopicRootCandidates(routerHashes);
						} catch {
							// ignore
						}
					}
				}

				// Ensure subsequent root resolution (and root hosting) uses the updated
				// candidate set rather than any earlier auto-candidate cached mapping.
				for (const peer of group) {
					try {
						(peer as any)?.services?.pubsub?.shardRootCache?.clear?.();
					} catch {
						// ignore
					}
				}

					// Bring up shard roots on router peers.
					await Promise.all(
						routers.map(async (peer) => {
							try {
								await (peer as any)?.services?.pubsub?.hostShardRootsNow?.();
						} catch {
							// ignore
						}
					}),
				);

				// If programs were opened before `connect()`, they may have subscribed under the
				// initial local-only shard mapping. After updating candidates, force a best-effort
				// reconcile so pubsub overlay joins + subscription announcements converge.
				await Promise.all(
					group.map(async (peer) => {
						try {
							const pubsub: any = (peer as any)?.services?.pubsub;
							pubsub?.shardRootCache?.clear?.();
							await pubsub?.reconcileShardOverlays?.();
						} catch {
							// ignore
						}
					}),
				);
			}
		}

	async connect(groups?: ProgramClient[][]) {
		await this.session.connect(groups?.map((x) => x.map((y) => y)));
		this.connectedGroups = groups
			? groups.map((group) => new Set(group as Peerbit[]))
			: [new Set(this._peers)];
		await this.configureFanoutShardRoots();
		return;
	}

	/**
	 * Connect peers in a sparse random graph (bounded degree) to enable large-n sims.
	 *
	 * Returns the adjacency list (by index within each group).
	 */
	async connectRandomGraph(opts: {
		degree: number;
		seed?: number;
		groups?: ProgramClient[][];
		concurrency?: number;
	}): Promise<number[][][]> {
		const groups = opts.groups ?? [this._peers];
		const degree = Math.max(0, Math.floor(opts.degree));
		const seed = Math.max(0, Math.floor(opts.seed ?? 1));
		const concurrency = Math.max(1, Math.floor(opts.concurrency ?? 100));
		const rng = mulberry32(seed);

		const out: number[][][] = [];
		for (const group of groups) {
			const n = group.length;
			if (n === 0) {
				out.push([]);
				continue;
			}
			const d = Math.min(degree, Math.max(0, n - 1));
			const graph = buildRandomGraph(n, d, rng);
			out.push(graph);

			const tasks: Array<() => Promise<boolean>> = [];
			for (let a = 0; a < graph.length; a++) {
				for (const b of graph[a]!) {
					if (b <= a) continue;
					const pa = group[a] as Peerbit;
					const pb = group[b] as Peerbit;
					tasks.push(() => pa.dial(pb));
				}
			}

			await runWithConcurrency(tasks, concurrency);
		}

		this.connectedGroups = groups.map((group) => new Set(group as Peerbit[]));
		await this.configureFanoutShardRoots();
		return out;
	}

	private wrapPeerStartForReconnect() {
		const patchedKey = Symbol.for("@peerbit/test-session.reconnect-on-start");
		for (const peer of this._peers) {
			const anyPeer = peer as any;
			if (anyPeer[patchedKey]) {
				continue;
			}
			anyPeer[patchedKey] = true;

			const originalStart = peer.start.bind(peer);
			peer.start = async () => {
				await originalStart();

				// Only auto-reconnect for sessions that have been explicitly connected.
				// This preserves `TestSession.disconnected*()` semantics.
				if (!this.connectedGroups || peer.libp2p.status !== "started") {
					return;
				}

				const peerHash = peer.identity.publicKey.hashcode();
				const peersToDial = new Set<Peerbit>();
				for (const group of this.connectedGroups) {
					if (!group.has(peer)) continue;
					for (const other of group) {
						if (other === peer) continue;
						if (other.libp2p.status !== "started") continue;
						peersToDial.add(other);
					}
				}

				// Re-establish connectivity after a full stop/start. Without this, tests that
				// restart a peer can fail to resolve programs/blocks because no node dials.
				await Promise.all(
					[...peersToDial].map(async (other) => {
						await peer.dial(other);

						// Also wait for the reverse direction to be fully established; some
						// protocols require a writable stream on both sides to reply.
						await Promise.all([
							other.services.pubsub.waitFor(peerHash, {
								target: "neighbor",
								timeout: 10_000,
							}),
							other.services.blocks.waitFor(peerHash, {
								target: "neighbor",
								timeout: 10_000,
							}),
							other.services.fanout.waitFor(peerHash, {
								target: "neighbor",
								timeout: 10_000,
							}),
						]);
					}),
				);
			};
		}
	}
	async stop() {
		await Promise.all(this._peers.map((peer) => peer.stop()));
		// `Peerbit.stop()` stops libp2p for sessions created by `Peerbit.create()`,
		// but in case a test injected an already-started external libp2p instance,
		// ensure it's stopped (without double-stopping).
		await Promise.all(
			this._peers.map(async (peer) => {
				if (peer.libp2p.status !== "stopped") {
					await peer.libp2p.stop();
				}
			}),
		);
	}

	/**
	 * Create a "mock-ish" session intended for fast and stable Node.js tests.
	 *
	 * Uses TCP-only transport (no WebRTC/WebSockets/circuit-relay) and disables
	 * the libp2p relay service by default.
	 */
	static async connectedMock(
		n: number,
		options?: CreateOptions | CreateOptions[],
	) {
		const session = await TestSession.disconnectedMock(n, options);
		await session.connect();
		// TODO types
		await waitForPeersStreams(
			...session.peers.map(
				(x) => x.services.blocks as any as DirectStream<any>,
			),
		);
		// Sharded pubsub/fanout uses its own DirectStream protocols; ensure those
		// neighbor streams are established before tests start opening programs.
		await waitForPeersStreams(
			...session.peers.map(
				(x) => x.services.pubsub as any as DirectStream<any>,
			),
		);
		await waitForPeersStreams(
			...session.peers.map(
				(x) => (x.services as any).fanout as any as DirectStream<any>,
			),
		);
		return session;
	}

	static async disconnectedMock(
		n: number,
		options?: CreateOptions | CreateOptions[],
	) {
		const applyMockDefaults = (
			o?: CreateOptions,
		): CreateOptions | undefined => {
			if (!o) {
				return {
					libp2p: {
						transports: transportsFast(),
						addresses: { listen: listenFast() },
						services: { relay: null },
					} as any,
				};
			}

			return {
				...o,
				libp2p: {
					...(o.libp2p ?? {}),
					transports: o.libp2p?.transports ?? transportsFast(),
					addresses: {
						...(o.libp2p?.addresses ?? {}),
						listen: o.libp2p?.addresses?.listen ?? listenFast(),
					},
					services: {
						...(o.libp2p?.services ?? {}),
						relay: o.libp2p?.services?.relay ?? null,
					},
				} as any,
			};
		};

		const optionsWithMockDefaults = Array.isArray(options)
			? options.map(applyMockDefaults)
			: applyMockDefaults(options);

		return TestSession.disconnected(n, optionsWithMockDefaults as any);
	}

	static async connected(n: number, options?: CreateOptions | CreateOptions[]) {
		const session = await TestSession.disconnected(n, options);
		await session.connect();
		// TODO types
		await waitForPeersStreams(
			...session.peers.map(
				(x) => x.services.blocks as any as DirectStream<any>,
			),
		);
		// Sharded pubsub/fanout uses its own DirectStream protocols; ensure those
		// neighbor streams are established before tests start opening programs.
		await waitForPeersStreams(
			...session.peers.map(
				(x) => x.services.pubsub as any as DirectStream<any>,
			),
		);
		await waitForPeersStreams(
			...session.peers.map(
				(x) => (x.services as any).fanout as any as DirectStream<any>,
			),
		);
		return session;
	}

	static async disconnected(
		n: number,
		options?: CreateOptions | CreateOptions[],
	) {
		const useMockSession =
			process.env.PEERBIT_TEST_SESSION === "mock" ||
			process.env.PEERBIT_TEST_SESSION === "fast" ||
			process.env.PEERBIT_TEST_SESSION === "tcp";

			const m = (o?: CreateOptions): Libp2pCreateOptionsWithServices => {
				const blocksDirectory = o?.directory
					? path.join(o.directory, "/blocks").toString()
					: undefined;

				const libp2pOptions: Libp2pCreateOptions = {
					...(o?.libp2p ?? {}),
				};

				// `SSession.disconnected(n, opts)` can re-use the same `opts` object across many
				// libp2p peers. Memoizing a single instance in a closure would accidentally
				// share one FanoutTree (and TopicRootControlPlane) across the entire session.
				// That breaks shard-root resolution (everyone would inherit the first peer's
				// candidate set) and can deadlock fanout joins in disconnected tests.
				const perPeer = new WeakMap<
					any,
					{ fanout: FanoutTree; topicRootControlPlane: TopicRootControlPlane }
				>();
				const getOrCreatePerPeer = (c: any) => {
					const key = c?.privateKey ?? c;
					let existing = perPeer.get(key);
					if (existing) return existing;

					const topicRootControlPlane = new TopicRootControlPlane();
					const fanout = new FanoutTree(c, {
						connectionManager: false,
						topicRootControlPlane,
					});
					existing = { fanout, topicRootControlPlane };
					perPeer.set(key, existing);
					return existing;
				};
				const getOrCreateFanout = (c: any) => getOrCreatePerPeer(c).fanout;

			if (useMockSession) {
				libp2pOptions.transports = libp2pOptions.transports ?? transportsFast();
				libp2pOptions.addresses = {
					...(libp2pOptions.addresses ?? {}),
					listen: libp2pOptions.addresses?.listen ?? listenFast(),
				};
				libp2pOptions.services = {
					...(libp2pOptions.services ?? {}),
					relay: libp2pOptions.services?.relay ?? null,
				};
			}

			return {
				...libp2pOptions,
				services: {
					blocks: (c: any) =>
						new DirectBlock(c, {
							directory: blocksDirectory,
						}),
					pubsub: (c: any) =>
						new TopicControlPlane(c, {
							canRelayMessage: true,
							topicRootControlPlane: getOrCreatePerPeer(c).topicRootControlPlane,
							fanout: getOrCreatePerPeer(c).fanout,
						}),
					fanout: (c: any) => getOrCreateFanout(c),
					keychain: keychain(),
					...libp2pOptions.services,
				} as any, /// TODO types
				streamMuxers: [yamux()],
				connectionMonitor: {
					enabled: false,
				},
				start: false, /// make Peerbit.create to start the client instead, this allows also so that Peerbit will terminate the client
			};
		};
		let optionsWithServices:
			| Libp2pCreateOptionsWithServices
			| Libp2pCreateOptionsWithServices[] = Array.isArray(options)
			? options.map(m)
			: m(options);

		const session = await SSession.disconnected(n, optionsWithServices);
		return new TestSession(
			session,
			(await Promise.all(
				session.peers.map((x, ix) =>
					Array.isArray(options)
						? Peerbit.create({ libp2p: x, directory: options[ix]?.directory })
						: Peerbit.create({ libp2p: x, directory: options?.directory }),
				),
			)) as Peerbit[],
		);
	}

	/**
	 * Create a large-n capable Peerbit session using an in-memory libp2p transport shim.
	 *
	 * This avoids TCP/noise costs and (optionally) skips expensive crypto verification.
	 *
	 * Notes:
	 * - By default this uses a no-op indexer to avoid spinning up sqlite for each peer.
	 * - If you pass `degree`, it will connect a bounded-degree random graph automatically.
	 */
	static async disconnectedInMemory(
		n: number,
		opts: InMemoryPeerbitSessionOptions & { directory?: string } = {},
	) {
		const mockCrypto = opts.mockCrypto !== false;
		const indexer = opts.indexer ?? (() => new NoopIndices());
		const seed = Math.max(0, Math.floor(opts.seed ?? 1));

		const perPeer = new Map<
			string,
			{ fanout: SimFanoutTree; topicRootControlPlane: TopicRootControlPlane }
		>();
		const getOrCreatePerPeer = (c: any) => {
			const key = c?.peerId?.toString?.() ?? "";
			const existing = perPeer.get(key);
			if (existing) return existing;

			const topicRootControlPlane = new TopicRootControlPlane();
			const fanout = new SimFanoutTree(
				c,
				{
					connectionManager: false,
					random: mulberry32((seed >>> 0) ^ parseSimPeerIndex(c?.peerId)),
					topicRootControlPlane,
				},
				mockCrypto,
			);
			const created = { fanout, topicRootControlPlane };
			perPeer.set(key, created);
			return created;
		};

		const inMemory = await InMemorySession.disconnected<Libp2pExtendServices>(n, {
			start: false,
			basePort: opts.basePort ?? 30_000,
			networkOpts: opts.network,
			services: {
				blocks: (c: any) =>
					new SimDirectBlock(
						c,
						{
							directory: opts.directory ? path.join(opts.directory, "/blocks") : undefined,
							canRelayMessage: true,
						},
						mockCrypto,
					),
				pubsub: (c: any) =>
					new SimTopicControlPlane(
						c,
						(() => {
							const { fanout, topicRootControlPlane } = getOrCreatePerPeer(c);
							return {
								canRelayMessage: true,
								topicRootControlPlane,
								fanout,
							};
						})(),
						mockCrypto,
					),
				fanout: (c: any) => getOrCreatePerPeer(c).fanout,
				keychain: () =>
					// Avoid libp2p keychain (datastore dependency) for large sims.
					// Peerbit only requires the CryptoKeychain surface in these sessions.
					new DefaultCryptoKeychain({ store: createStore() }) as any,
			} as any,
		});

		const peers = (await Promise.all(
			inMemory.peers.map((x) =>
				Peerbit.create({
					libp2p: x as any,
					directory: opts.directory,
					indexer,
				}),
			),
		)) as Peerbit[];

		const sessionLike: SessionLike = {
			peers: inMemory.peers as any[],
			connect: async (groups?: any[][]) => inMemory.connectFully(groups as any),
			stop: async () => inMemory.stop(),
		};

		return new TestSession(sessionLike, peers, {
			inMemory: { session: inMemory, network: inMemory.network },
		});
	}

	static async connectedInMemory(
		n: number,
		opts: InMemoryPeerbitSessionOptions & { directory?: string } = {},
	) {
		const session = await TestSession.disconnectedInMemory(n, opts);
		if (opts.degree != null) {
			await session.connectRandomGraph({
				degree: opts.degree,
				seed: opts.seed,
				concurrency: opts.concurrency,
			});
			return session;
		}
		await session.connect();
		return session;
	}
}
