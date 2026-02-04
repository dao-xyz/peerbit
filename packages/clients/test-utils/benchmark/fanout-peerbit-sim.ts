/**
 * FanoutTree sim using full Peerbit clients over an in-memory libp2p shim.
 *
 * This is intended as a “real integration” complement to `fanout-tree-sim`,
 * focusing on large-n local runs without TCP/noise costs.
 *
 * Run:
 *   pnpm -C packages/clients/test-utils run bench -- fanout-peerbit-sim --nodes 1000 --degree 6 --messages 300 --msgSize 1024 --msgRate 30 --seed 1
 */

import { delay } from "@peerbit/time";
import { TestSession } from "../src/session.js";

type Params = {
	nodes: number;
	degree: number;
	seed: number;
	concurrency: number;
	bootstrapCount: number;
	bootstrapMaxPeers: number;

	rootIndex: number;
	subscribers: number;
	relayFraction: number;

	messages: number;
	msgRate: number;
	msgSize: number;
	intervalMs: number;
	settleMs: number;
	deadlineMs: number;

	timeoutMs: number;
	joinTimeoutMs: number;
	joinReqTimeoutMs: number;

	rootUploadLimitBps: number;
	rootMaxChildren: number;
	relayUploadLimitBps: number;
	relayMaxChildren: number;

	repair: boolean;
	neighborRepair: boolean;
	allowKick: boolean;
	mockCrypto: boolean;

	dialDelayMs: number;
	streamRxDelayMs: number;
	streamHighWaterMarkBytes: number;

	dropDataFrameRate: number;
	dropSeed: number;

	churnEveryMs: number;
	churnDownMs: number;
	churnFraction: number;

	assertMinJoinedPct: number;
	assertMinDeliveryPct: number;
	assertMinDeadlineDeliveryPct: number;
	assertMaxOverheadFactor: number;
	assertMaxUploadFracPct: number;
	assertMaxControlBpp: number;
	assertMaxTrackerBpp: number;
	assertMaxRepairBpp: number;
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

const parseArgs = (argv: string[]): Params => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};

	const maybeNumber = (key: string) => {
		const v = get(key);
		if (v == null) return undefined;
		const n = Number(v);
		return Number.isFinite(n) ? n : undefined;
	};

	const maybeString = (key: string) => {
		const v = get(key);
		if (v == null) return undefined;
		return String(v);
	};

	const bool = (key: string, defaultValue: boolean) => {
		const v = get(key);
		if (v == null) return defaultValue;
		return v !== "0" && v !== "false";
	};

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(
			[
				"fanout-peerbit-sim",
				"",
				"Args:",
				"  --preset NAME            preset workload (ci-small|ci-loss|live|reliable|scale-1k)",
				"  --nodes N                 (default: 1000)",
				"  --degree K                (default: 6)",
				"  --seed S                  (default: 1)",
				"  --concurrency C           join+dial concurrency (default: 200)",
				"  --bootstrapCount N        tracker/bootstraps to use (default: 3, excludes rootIndex)",
				"  --bootstrapMaxPeers N     max bootstraps to dial/query per node (default: 1, 0=all)",
				"  --rootIndex I             (default: 0)",
				"  --subscribers N           number of subscribers (default: nodes-1)",
				"  --relayFraction F         fraction of subscribers that can relay (default: 0.25)",
				"  --messages M              (default: 300)",
				"  --msgRate R               messages/sec (default: 30)",
				"  --msgSize B               bytes (default: 1024)",
				"  --intervalMs X            override publish interval (default: derived from msgRate)",
				"  --settleMs X              wait after publish (default: 2000)",
				"  --deadlineMs X            deadline for 'within deadline' metric (default: 2000)",
				"  --timeoutMs X             global timeout (default: 300000)",
				"  --joinTimeoutMs X         per-peer join timeout (default: 60000)",
				"  --joinReqTimeoutMs X      per-attempt join req timeout (default: 2000)",
				"  --rootUploadLimitBps BPS  (default: 100000000)",
				"  --rootMaxChildren N       (default: 64)",
				"  --relayUploadLimitBps BPS (default: 20000000)",
				"  --relayMaxChildren N      (default: 32)",
				"  --repair 0|1              (default: 1)",
				"  --neighborRepair 0|1      (default: 0)",
				"  --allowKick 0|1           (default: 1)",
				"  --mockCrypto 0|1          (default: 1)",
				"  --dialDelayMs X           (default: 0)",
				"  --streamRxDelayMs X       (default: 0)",
				"  --streamHighWaterMarkBytes B (default: 262144)",
				"  --dropDataFrameRate P     drop rate for low-priority FanoutTree data frames (default: 0)",
				"  --dropSeed S              RNG seed for drops (default: seed)",
				"  --churnEveryMs X          churn interval (default: 0, off)",
				"  --churnDownMs X           offline duration per churn event (default: 0)",
				"  --churnFraction F         fraction of joined subscribers to churn (default: 0)",
				"  --assertMinJoinedPct X    fail if join% below X (default: 0, off)",
				"  --assertMinDeliveryPct X  fail if delivery% below X (default: 0, off)",
				"  --assertMinDeadlineDeliveryPct X  fail if deadline delivery% below X (default: 0, off)",
				"  --assertMaxOverheadFactor X  fail if overheadFactorData above X (default: 0, off)",
				"  --assertMaxUploadFracPct X   fail if peak upload above X% of cap (default: 0, off)",
				"  --assertMaxControlBpp X      fail if control bytes / delivered payload bytes above X (default: 0, off)",
				"  --assertMaxTrackerBpp X      fail if tracker bytes / delivered payload bytes above X (default: 0, off)",
				"  --assertMaxRepairBpp X       fail if repair bytes / delivered payload bytes above X (default: 0, off)",
			].join("\n"),
		);
		process.exit(0);
	}

	const preset = maybeString("--preset");
	const presetOpts: Partial<Params> =
		preset === "ci-small"
			? {
					nodes: 25,
					degree: 4,
					subscribers: 20,
					relayFraction: 0.3,
					messages: 20,
					msgRate: 50,
					msgSize: 64,
					settleMs: 500,
					deadlineMs: 500,
					timeoutMs: 20_000,
					bootstrapCount: 3,
					bootstrapMaxPeers: 1,
					rootMaxChildren: 4,
					relayMaxChildren: 4,
					repair: true,
					neighborRepair: false,
					assertMinJoinedPct: 99.9,
					assertMinDeliveryPct: 99.9,
			  }
			: preset === "ci-loss"
				? {
						nodes: 40,
						degree: 4,
						subscribers: 30,
						relayFraction: 0.35,
						messages: 40,
						msgRate: 50,
						msgSize: 64,
						settleMs: 2_500,
						deadlineMs: 2_000,
						timeoutMs: 40_000,
						bootstrapCount: 3,
						bootstrapMaxPeers: 1,
						rootMaxChildren: 4,
						relayMaxChildren: 4,
						repair: true,
						neighborRepair: true,
						dropDataFrameRate: 0.1,
						churnEveryMs: 200,
						churnDownMs: 100,
						churnFraction: 0.05,
				  }
				: preset === "live"
					? {
							nodes: 2000,
							degree: 6,
							bootstrapCount: 3,
							bootstrapMaxPeers: 1,
							messages: 30 * 60,
							msgRate: 30,
							msgSize: 1024,
							settleMs: 2_000,
							deadlineMs: 2_000,
							rootUploadLimitBps: 20_000_000,
							rootMaxChildren: 64,
							relayUploadLimitBps: 10_000_000,
							relayMaxChildren: 32,
							repair: true,
							neighborRepair: true,
							dropDataFrameRate: 0.01,
							churnEveryMs: 2_000,
							churnDownMs: 1_000,
							churnFraction: 0.005,
					  }
					: preset === "reliable"
						? {
								nodes: 2000,
								degree: 6,
								bootstrapCount: 3,
								bootstrapMaxPeers: 1,
								messages: 30 * 60,
								msgRate: 30,
								msgSize: 1024,
								settleMs: 10_000,
								deadlineMs: 10_000,
								rootUploadLimitBps: 20_000_000,
								rootMaxChildren: 64,
								relayUploadLimitBps: 10_000_000,
								relayMaxChildren: 32,
								repair: true,
								neighborRepair: true,
								dropDataFrameRate: 0.01,
								churnEveryMs: 2_000,
								churnDownMs: 1_000,
								churnFraction: 0.005,
						  }
						: preset === "scale-1k"
							? {
									nodes: 1000,
									degree: 6,
									bootstrapCount: 3,
									bootstrapMaxPeers: 1,
									messages: 200,
									msgRate: 30,
									msgSize: 1024,
									settleMs: 5_000,
									deadlineMs: 2_000,
									timeoutMs: 300_000,
									repair: true,
									neighborRepair: true,
							  }
							: {};

	const nodes = Math.max(1, Math.floor(maybeNumber("--nodes") ?? presetOpts.nodes ?? 1000));
	const msgRate = Math.max(1, Math.floor(maybeNumber("--msgRate") ?? presetOpts.msgRate ?? 30));
	const seed = Math.max(0, Math.floor(maybeNumber("--seed") ?? presetOpts.seed ?? 1));

	return {
		nodes,
		degree: Math.max(0, Math.floor(maybeNumber("--degree") ?? presetOpts.degree ?? 6)),
		seed,
		concurrency: Math.max(
			1,
			Math.floor(maybeNumber("--concurrency") ?? presetOpts.concurrency ?? 200),
		),
		bootstrapCount: Math.max(
			0,
			Math.floor(maybeNumber("--bootstrapCount") ?? presetOpts.bootstrapCount ?? 3),
		),
		bootstrapMaxPeers: Math.max(
			0,
			Math.floor(maybeNumber("--bootstrapMaxPeers") ?? presetOpts.bootstrapMaxPeers ?? 1),
		),

		rootIndex: Math.max(
			0,
			Math.min(nodes - 1, Math.floor(maybeNumber("--rootIndex") ?? presetOpts.rootIndex ?? 0)),
		),
		subscribers: Math.max(
			0,
			Math.min(
				nodes - 1,
				Math.floor(maybeNumber("--subscribers") ?? presetOpts.subscribers ?? nodes - 1),
			),
		),
		relayFraction: Math.max(
			0,
			Math.min(1, Number(maybeNumber("--relayFraction") ?? presetOpts.relayFraction ?? 0.25)),
		),

		messages: Math.max(0, Math.floor(maybeNumber("--messages") ?? presetOpts.messages ?? 300)),
		msgRate,
		msgSize: Math.max(1, Math.floor(maybeNumber("--msgSize") ?? presetOpts.msgSize ?? 1024)),
		intervalMs: Math.max(0, Math.floor(maybeNumber("--intervalMs") ?? presetOpts.intervalMs ?? Math.floor(1000 / msgRate))),
		settleMs: Math.max(0, Math.floor(maybeNumber("--settleMs") ?? presetOpts.settleMs ?? 2000)),
		deadlineMs: Math.max(0, Math.floor(maybeNumber("--deadlineMs") ?? presetOpts.deadlineMs ?? 2000)),

		timeoutMs: Math.max(1, Math.floor(maybeNumber("--timeoutMs") ?? presetOpts.timeoutMs ?? 300_000)),
		joinTimeoutMs: Math.max(1, Math.floor(maybeNumber("--joinTimeoutMs") ?? presetOpts.joinTimeoutMs ?? 60_000)),
		joinReqTimeoutMs: Math.max(1, Math.floor(maybeNumber("--joinReqTimeoutMs") ?? presetOpts.joinReqTimeoutMs ?? 2_000)),

		rootUploadLimitBps: Math.max(
			0,
			Math.floor(maybeNumber("--rootUploadLimitBps") ?? presetOpts.rootUploadLimitBps ?? 100_000_000),
		),
		rootMaxChildren: Math.max(0, Math.floor(maybeNumber("--rootMaxChildren") ?? presetOpts.rootMaxChildren ?? 64)),
		relayUploadLimitBps: Math.max(
			0,
			Math.floor(maybeNumber("--relayUploadLimitBps") ?? presetOpts.relayUploadLimitBps ?? 20_000_000),
		),
		relayMaxChildren: Math.max(0, Math.floor(maybeNumber("--relayMaxChildren") ?? presetOpts.relayMaxChildren ?? 32)),

		repair: bool("--repair", presetOpts.repair ?? true),
		neighborRepair: bool("--neighborRepair", presetOpts.neighborRepair ?? false),
		allowKick: bool("--allowKick", presetOpts.allowKick ?? true),
		mockCrypto: bool("--mockCrypto", presetOpts.mockCrypto ?? true),

		dialDelayMs: Math.max(0, Math.floor(maybeNumber("--dialDelayMs") ?? presetOpts.dialDelayMs ?? 0)),
		streamRxDelayMs: Math.max(0, Math.floor(maybeNumber("--streamRxDelayMs") ?? presetOpts.streamRxDelayMs ?? 0)),
		streamHighWaterMarkBytes: Math.max(
			1,
			Math.floor(maybeNumber("--streamHighWaterMarkBytes") ?? presetOpts.streamHighWaterMarkBytes ?? 256 * 1024),
		),

		dropDataFrameRate: Math.max(0, Math.min(1, Number(maybeNumber("--dropDataFrameRate") ?? presetOpts.dropDataFrameRate ?? 0))),
		dropSeed: Math.max(0, Math.floor(maybeNumber("--dropSeed") ?? presetOpts.dropSeed ?? seed)),

		churnEveryMs: Math.max(0, Math.floor(maybeNumber("--churnEveryMs") ?? presetOpts.churnEveryMs ?? 0)),
		churnDownMs: Math.max(0, Math.floor(maybeNumber("--churnDownMs") ?? presetOpts.churnDownMs ?? 0)),
		churnFraction: Math.max(0, Math.min(1, Number(maybeNumber("--churnFraction") ?? presetOpts.churnFraction ?? 0))),

		assertMinJoinedPct: Math.max(0, Number(maybeNumber("--assertMinJoinedPct") ?? presetOpts.assertMinJoinedPct ?? 0)),
		assertMinDeliveryPct: Math.max(0, Number(maybeNumber("--assertMinDeliveryPct") ?? presetOpts.assertMinDeliveryPct ?? 0)),
		assertMinDeadlineDeliveryPct: Math.max(
			0,
			Number(maybeNumber("--assertMinDeadlineDeliveryPct") ?? presetOpts.assertMinDeadlineDeliveryPct ?? 0),
		),
		assertMaxOverheadFactor: Math.max(
			0,
			Number(maybeNumber("--assertMaxOverheadFactor") ?? presetOpts.assertMaxOverheadFactor ?? 0),
		),
		assertMaxUploadFracPct: Math.max(
			0,
			Number(maybeNumber("--assertMaxUploadFracPct") ?? presetOpts.assertMaxUploadFracPct ?? 0),
		),
		assertMaxControlBpp: Math.max(
			0,
			Number(maybeNumber("--assertMaxControlBpp") ?? presetOpts.assertMaxControlBpp ?? 0),
		),
		assertMaxTrackerBpp: Math.max(
			0,
			Number(maybeNumber("--assertMaxTrackerBpp") ?? presetOpts.assertMaxTrackerBpp ?? 0),
		),
		assertMaxRepairBpp: Math.max(
			0,
			Number(maybeNumber("--assertMaxRepairBpp") ?? presetOpts.assertMaxRepairBpp ?? 0),
		),
	};
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

const quantile = (sorted: number[], q: number): number => {
	if (sorted.length === 0) return 0;
	const i = (sorted.length - 1) * q;
	const lo = Math.floor(i);
	const hi = Math.ceil(i);
	if (lo === hi) return sorted[lo]!;
	const t = i - lo;
	return sorted[lo]! * (1 - t) + sorted[hi]! * t;
};

const anySignal = (signals: AbortSignal[]): AbortSignal & { clear?: () => void } => {
	// Prefer built-in AbortSignal.any when available (Node >=18),
	// otherwise fall back to a small polyfill.
	const anyFn = (AbortSignal as any).any as undefined | ((signals: AbortSignal[]) => AbortSignal);
	if (typeof anyFn === "function") {
		return anyFn(signals) as any;
	}

	const controller = new AbortController();
	const onAbort = (ev: any) => {
		const sig = ev?.target as AbortSignal | undefined;
		controller.abort(sig?.reason ?? new Error("aborted"));
	};
	for (const s of signals) {
		if (s.aborted) {
			controller.abort(s.reason ?? new Error("aborted"));
			break;
		}
		s.addEventListener("abort", onAbort, { once: true });
	}
	(controller.signal as any).clear = () => {
		for (const s of signals) s.removeEventListener("abort", onAbort);
	};
	return controller.signal as any;
};

const main = async () => {
	const params = parseArgs(process.argv.slice(2));
	const timeout = new AbortController();
	const timeoutId = setTimeout(() => {
		timeout.abort(new Error(`timeout after ${params.timeoutMs}ms`));
	}, params.timeoutMs);

	try {
		const session = await TestSession.disconnectedInMemory(params.nodes, {
			seed: params.seed,
			concurrency: params.concurrency,
			mockCrypto: params.mockCrypto,
			network: {
				dialDelayMs: params.dialDelayMs,
				streamRxDelayMs: params.streamRxDelayMs,
				streamHighWaterMarkBytes: params.streamHighWaterMarkBytes,
				dropDataFrameRate: params.dropDataFrameRate,
				dropSeed: params.dropSeed,
			},
		});

		// Sparse underlay (bounded degree) for large-n.
		const graph = (await session.connectRandomGraph({
			degree: Math.min(params.degree, Math.max(0, params.nodes - 1)),
			seed: params.seed,
			concurrency: params.concurrency,
		}))[0] ?? [];

		const rng = mulberry32(params.seed);

		const rootPeer = session.peers[params.rootIndex]!;
		const root = (rootPeer as any).services.fanout as any;
		const topic = "concert";
		const rootId = root.publicKeyHash as string;
		const rootNeighbors = new Set<number>(graph[params.rootIndex] ?? []);
		const bootstrapIndices: number[] = [];
		for (let i = 0; i < params.nodes && bootstrapIndices.length < params.bootstrapCount; i++) {
			if (i === params.rootIndex) continue;
			bootstrapIndices.push(i);
		}
		const bootstrapAddrs = [
			...new Set(
				bootstrapIndices.flatMap((idx) => {
					const p: any = session.peers[idx] as any;
					const addrs: any[] = p?.libp2p?.getMultiaddrs?.() ?? [];
					return addrs.map((a) => (typeof a === "string" ? a : a.toString()));
				}),
			),
		];

		// Use bootstraps as rendezvous trackers (announcements + queries).
		if (bootstrapAddrs.length > 0) {
			root.setBootstraps(bootstrapAddrs);
		}

		// Choose subscribers (exclude root).
		const candidates: number[] = [];
		for (let i = 0; i < params.nodes; i++) {
			if (i === params.rootIndex) continue;
			candidates.push(i);
		}
		for (let i = candidates.length - 1; i > 0; i--) {
			const j = Math.floor(rng() * (i + 1));
			const tmp = candidates[i]!;
			candidates[i] = candidates[j]!;
			candidates[j] = tmp;
		}
		const subscriberIndices = candidates.slice(0, params.subscribers);

		// Select relays among subscribers.
		const relayCount = Math.max(
			0,
			Math.min(subscriberIndices.length, Math.floor(params.relayFraction * subscriberIndices.length)),
		);
		const relaySet = new Set<number>();
		if (relayCount > 0) {
			// Force root-adjacent relays so the tree can actually grow on sparse underlays.
			// (Otherwise, the join can stall at ~root degree if none of the root neighbors can relay.)
			for (const idx of rootNeighbors) {
				if (idx === params.rootIndex) continue;
				relaySet.add(idx);
			}
			for (const idx of subscriberIndices) {
				if (relaySet.size >= relayCount) break;
				relaySet.add(idx);
			}
		}

		// Root opens channel.
		root.openChannel(topic, rootId, {
			role: "root",
			msgRate: params.msgRate,
			msgSize: params.msgSize,
			uploadLimitBps: params.rootUploadLimitBps,
			maxChildren: params.rootMaxChildren,
			allowKick: params.allowKick,
			repair: params.repair,
			neighborRepair: params.neighborRepair,
		});

		const joinedNodes = new Set<number>();

		// Bootstrap the tree by joining relays adjacent to the root first.
		// This makes join success much less sensitive to random relay placement.
		if (relayCount > 0 && rootNeighbors.size > 0) {
			const bootstrapRelays = [...rootNeighbors].filter(
				(i) => i !== params.rootIndex && i < params.nodes,
			);
			await runWithConcurrency(
				bootstrapRelays.map((idx) => async () => {
					const peer = session.peers[idx]!;
					const fanout = (peer as any).services.fanout as any;
					await fanout.joinChannel(
						topic,
						rootId,
						{
							msgRate: params.msgRate,
							msgSize: params.msgSize,
							uploadLimitBps: params.relayUploadLimitBps,
							maxChildren: params.relayMaxChildren,
							allowKick: params.allowKick,
							repair: params.repair,
							neighborRepair: params.neighborRepair,
						},
						{
							bootstrap: bootstrapAddrs,
							bootstrapMaxPeers: params.bootstrapMaxPeers,
							timeoutMs: Math.max(1, Math.min(params.joinTimeoutMs, 10_000)),
							joinReqTimeoutMs: params.joinReqTimeoutMs,
							signal: timeout.signal,
						},
					);
					joinedNodes.add(idx);
				}),
				Math.min(params.concurrency, bootstrapRelays.length),
			);
		}

		const joinStart = Date.now();
		const joined = new Uint8Array(subscriberIndices.length);
		await runWithConcurrency(
			subscriberIndices.map((idx, i) => async () => {
				const peer = session.peers[idx]!;
				const fanout = (peer as any).services.fanout as any;
				const isRelay = relaySet.has(idx);

				try {
					await fanout.joinChannel(
						topic,
						rootId,
						{
							msgRate: params.msgRate,
							msgSize: params.msgSize,
							uploadLimitBps: isRelay ? params.relayUploadLimitBps : 0,
							maxChildren: isRelay ? params.relayMaxChildren : 0,
							allowKick: params.allowKick,
							repair: params.repair,
							neighborRepair: params.neighborRepair,
						},
						{
							bootstrap: bootstrapAddrs,
							bootstrapMaxPeers: params.bootstrapMaxPeers,
							timeoutMs: params.joinTimeoutMs,
							joinReqTimeoutMs: params.joinReqTimeoutMs,
							signal: timeout.signal,
						},
					);
					joined[i] = 1;
					joinedNodes.add(idx);
				} catch {
					joined[i] = 0;
				}
			}),
			params.concurrency,
		);

		const joinedCount = joined.reduce((a, b) => a + b, 0);
		const joinMs = Date.now() - joinStart;

		// Delivery tracking.
		const publishAt: number[] = new Array(params.messages).fill(0);
		const receivedBySubscriber: Uint8Array[] = subscriberIndices.map(
			() => new Uint8Array(params.messages),
		);

		let churnEvents = 0;
		let churnedPeersTotal = 0;

		let delivered = 0;
		let deliveredWithinDeadline = 0;
		let duplicates = 0;
		const latencySamples: number[] = [];
		const maxLatencySamples = 50_000;
		let latencySeen = 0;
		const recordLatency = (ms: number) => {
			latencySeen += 1;
			if (latencySamples.length < maxLatencySamples) {
				latencySamples.push(ms);
				return;
			}
			const j = Math.floor(rng() * latencySeen);
			if (j < maxLatencySamples) latencySamples[j] = ms;
		};

		for (let i = 0; i < subscriberIndices.length; i++) {
			if (!joined[i]) continue;
			const idx = subscriberIndices[i]!;
			const peer = session.peers[idx]!;
			const fanout = (peer as any).services.fanout as any;
			fanout.addEventListener("fanout:data", (ev: any) => {
				const d = ev?.detail;
				if (!d) return;
				if (d.topic !== topic) return;
				if (d.root !== rootId) return;
				const seq = d.seq >>> 0;
				if (seq >= params.messages) return;
				const seenArr = receivedBySubscriber[i]!;
				if (seenArr[seq]) {
					duplicates += 1;
					return;
				}
				seenArr[seq] = 1;
				delivered += 1;

				const t0 = publishAt[seq] ?? 0;
				if (t0 > 0) {
					const ms = Date.now() - t0;
					recordLatency(ms);
					if (params.deadlineMs > 0 && ms <= params.deadlineMs) {
						deliveredWithinDeadline += 1;
					}
				}
			});
		}

		// Publish loop.
		const payload = new Uint8Array(params.msgSize);
		const churnController = new AbortController();
		const churnSignal = anySignal([timeout.signal, churnController.signal]);

		const churnLoop = async () => {
			const everyMs = Math.max(0, Math.floor(params.churnEveryMs));
			const downMs = Math.max(0, Math.floor(params.churnDownMs));
			const fraction = Math.max(0, Math.min(1, Number(params.churnFraction)));
			if (everyMs <= 0 || downMs <= 0 || fraction <= 0) return;

			const network = session.inMemory?.network;
			if (!network) return;

			const joinedSubscriberIndices = subscriberIndices.filter((_, i) => joined[i] === 1);
			if (joinedSubscriberIndices.length === 0) return;

			const excluded = new Set<number>([params.rootIndex, ...bootstrapIndices]);

			for (;;) {
				if (churnSignal.aborted) return;
				await delay(everyMs, { signal: churnSignal });
				if (churnSignal.aborted) return;

				const candidates = joinedSubscriberIndices.filter((idx) => !excluded.has(idx));
				if (candidates.length === 0) continue;

				const target = Math.min(
					candidates.length,
					Math.max(1, Math.floor(candidates.length * fraction)),
				);
				const chosen = new Set<number>();
				const maxAttempts = Math.max(10, target * 20);
				for (let tries = 0; chosen.size < target && tries < maxAttempts; tries++) {
					const idx = candidates[Math.floor(rng() * candidates.length)]!;
					const peer = session.peers[idx] as any;
					const peerId = peer?.libp2p?.peerId;
					if (!peerId) continue;
					if (network.isPeerOffline(peerId)) continue;
					chosen.add(idx);
				}
				if (chosen.size === 0) continue;

				churnEvents += 1;
				churnedPeersTotal += chosen.size;
				const now = Date.now();
				await Promise.all(
					[...chosen].map(async (idx) => {
						const peer = session.peers[idx] as any;
						const peerId = peer?.libp2p?.peerId;
						if (!peerId) return;
						network.setPeerOffline(peerId, downMs, now);
						await network.disconnectPeer(peerId);
					}),
				);
			}
		};

		const publishStart = Date.now();
		const churnPromise = churnLoop().catch(() => {});
		try {
			for (let seq = 0; seq < params.messages; seq++) {
				if (timeout.signal.aborted) break;
				publishAt[seq] = Date.now();
				await root.publishData(topic, rootId, payload);
				if (params.intervalMs > 0) {
					await delay(params.intervalMs, { signal: timeout.signal });
				}
			}
		} finally {
			churnController.abort();
			await churnPromise;
			churnSignal.clear?.();
		}
		const publishMs = Date.now() - publishStart;

		// Signal end-of-stream so subscribers can detect tail gaps and repair.
		if (params.repair && params.messages > 0) {
			await root.publishEnd(topic, rootId, params.messages);
		}

		// Allow repair/tail settling.
		if (params.settleMs > 0) {
			await delay(params.settleMs, { signal: timeout.signal });
		}

		const expected = joinedCount * params.messages;
		const deliveredPct = expected > 0 ? (100 * delivered) / expected : 0;
		const deliveredWithinDeadlinePct =
			expected > 0 ? (100 * deliveredWithinDeadline) / expected : 0;

		latencySamples.sort((a, b) => a - b);
		const latencyP50 = quantile(latencySamples, 0.5);
		const latencyP95 = quantile(latencySamples, 0.95);
		const latencyP99 = quantile(latencySamples, 0.99);
		const latencyMax =
			latencySamples.length > 0 ? latencySamples[latencySamples.length - 1]! : 0;

		// Protocol stats.
		let dataPayloadBytesSent = 0;
		let controlBytesSent = 0;
		let controlBytesSentJoin = 0;
		let controlBytesSentRepair = 0;
		let controlBytesSentTracker = 0;
		let dataWriteDrops = 0;
		let reparent = 0;
		for (const p of session.peers as any[]) {
			const fanout = p.services.fanout;
			const m = fanout.getChannelMetrics(topic, rootId);
			dataPayloadBytesSent += m.dataPayloadBytesSent;
			controlBytesSent += m.controlBytesSent;
			controlBytesSentJoin += m.controlBytesSentJoin;
			controlBytesSentRepair += m.controlBytesSentRepair;
			controlBytesSentTracker += m.controlBytesSentTracker;
			dataWriteDrops += m.dataWriteDrops ?? 0;
			reparent +=
				(m.reparentDisconnect ?? 0) + (m.reparentStale ?? 0) + (m.reparentKicked ?? 0);
		}
		const idealTreePayloadBytes = params.msgSize * params.messages * joinedNodes.size;
		const overheadFactorData =
			idealTreePayloadBytes > 0 ? dataPayloadBytesSent / idealTreePayloadBytes : 0;
		const deliveredPayloadBytes = delivered * params.msgSize;
		const controlBpp = deliveredPayloadBytes > 0 ? controlBytesSent / deliveredPayloadBytes : 0;
		const trackerBpp =
			deliveredPayloadBytes > 0 ? controlBytesSentTracker / deliveredPayloadBytes : 0;
		const repairBpp =
			deliveredPayloadBytes > 0 ? controlBytesSentRepair / deliveredPayloadBytes : 0;

		// Peak upload vs cap (best-effort; counts framed bytes, including overhead).
		const uploadCapByHash = new Map<string, number>();
		if (params.rootUploadLimitBps > 0) {
			uploadCapByHash.set(rootId, params.rootUploadLimitBps);
		}
		for (const idx of joinedNodes) {
			if (idx === params.rootIndex) continue;
			if (!relaySet.has(idx)) continue;
			if (params.relayUploadLimitBps <= 0) continue;
			const h = (session.peers[idx] as any).services.fanout.publicKeyHash as string;
			uploadCapByHash.set(h, params.relayUploadLimitBps);
		}
		let maxUploadFracPct = 0;
		let maxUploadNode: string | undefined;
		let maxUploadBps = 0;
		for (const [hash, pm] of session.inMemory?.network.peerMetricsByHash ?? []) {
			const cap = uploadCapByHash.get(hash);
			if (!cap || cap <= 0) continue;
			const frac = (100 * pm.maxBytesPerSecond) / cap;
			if (frac > maxUploadFracPct) {
				maxUploadFracPct = frac;
				maxUploadNode = hash;
				maxUploadBps = pm.maxBytesPerSecond;
			}
		}

		// Stream queue stats.
		let streamQueuedBytesTotal = 0;
		let streamQueuedBytesMax = 0;
		for (const p of session.peers as any[]) {
			const q = Math.max(0, Math.floor(p.services.fanout.getQueuedBytes()));
			streamQueuedBytesTotal += q;
			if (q > streamQueuedBytesMax) streamQueuedBytesMax = q;
		}

		const lines: string[] = [];
		lines.push("fanout peerbit-sim results");
		lines.push(`- nodes: ${params.nodes}, degree: ${params.degree}, seed: ${params.seed}`);
		lines.push(
			`- bootstraps: count=${bootstrapIndices.length}, addrs=${bootstrapAddrs.length}, maxPeers=${params.bootstrapMaxPeers} (excludes rootIndex=${params.rootIndex})`,
		);
		lines.push(
			`- subscribers: ${params.subscribers} (relays=${relayCount}, relayFraction=${params.relayFraction.toFixed(
				2,
			)})`,
		);
		lines.push(
			`- join: joined=${joinedCount}/${params.subscribers} (${(
				(100 * joinedCount) /
				Math.max(1, params.subscribers)
			).toFixed(2)}%), time=${joinMs}ms`,
		);
		lines.push(`- tree: attachedNodes=${joinedNodes.size + 1} (root+${joinedNodes.size})`);
		lines.push(
			`- publish: messages=${params.messages}, msgRate=${params.msgRate}/s, msgSize=${params.msgSize}B, intervalMs=${params.intervalMs}, time=${publishMs}ms`,
		);
		lines.push(
			`- delivery: expected=${expected}, delivered=${delivered} (${deliveredPct.toFixed(
				2,
			)}%), withinDeadline=${deliveredWithinDeadline} (${deliveredWithinDeadlinePct.toFixed(
				2,
			)}%), duplicates=${duplicates}`,
		);
		lines.push(
			`- latency ms: samples=${latencySamples.length}, p50=${latencyP50.toFixed(
				1,
			)}, p95=${latencyP95.toFixed(1)}, p99=${latencyP99.toFixed(
				1,
			)}, max=${latencyMax.toFixed(1)}`,
		);
		lines.push(
			`- overhead: dataPayloadBytesSent=${dataPayloadBytesSent}, idealTreePayloadBytes=${idealTreePayloadBytes}, overheadFactorData=${overheadFactorData.toFixed(
				3,
			)}`,
		);
		lines.push(
			`- control: bytesSent=${controlBytesSent} (join=${controlBytesSentJoin} tracker=${controlBytesSentTracker} repair=${controlBytesSentRepair}) bpp=${controlBpp.toFixed(
				4,
			)} (tracker=${trackerBpp.toFixed(4)} repair=${repairBpp.toFixed(4)}) dataWriteDrops=${dataWriteDrops} reparent=${reparent}`,
		);
		lines.push(
			`- upload: max=${maxUploadBps} B/s (${maxUploadFracPct.toFixed(1)}% of cap) node=${maxUploadNode ?? "-"}`,
		);
		lines.push(
			`- streamQueuedBytes: total=${streamQueuedBytesTotal}, maxNode=${streamQueuedBytesMax}`,
		);
		lines.push(
			`- churn: everyMs=${params.churnEveryMs} downMs=${params.churnDownMs} fraction=${params.churnFraction} events=${churnEvents} peers=${churnedPeersTotal}`,
		);
		lines.push(
			`- loss: dropDataFrameRate=${params.dropDataFrameRate} dropSeed=${params.dropSeed}`,
		);
		if (session.inMemory?.network) {
			const m = session.inMemory.network.metrics;
			lines.push(
				`- network: dials=${m.dials}, connsOpened=${m.connectionsOpened}, connsClosed=${m.connectionsClosed}, streamsOpened=${m.streamsOpened}, framesSent=${m.framesSent}, bytesSent=${m.bytesSent}, framesDropped=${m.framesDropped}, bytesDropped=${m.bytesDropped}`,
			);
		}

		console.log(lines.join("\n"));

		const joinedPct = params.subscribers > 0 ? (100 * joinedCount) / params.subscribers : 100;
		const asserts: string[] = [];
		if (params.assertMinJoinedPct > 0 && joinedPct < params.assertMinJoinedPct) {
			asserts.push(
				`join% ${joinedPct.toFixed(2)} < assertMinJoinedPct ${params.assertMinJoinedPct}`,
			);
		}
		if (params.assertMinDeliveryPct > 0 && deliveredPct < params.assertMinDeliveryPct) {
			asserts.push(
				`delivery% ${deliveredPct.toFixed(2)} < assertMinDeliveryPct ${params.assertMinDeliveryPct}`,
			);
		}
		if (
			params.assertMinDeadlineDeliveryPct > 0 &&
			deliveredWithinDeadlinePct < params.assertMinDeadlineDeliveryPct
		) {
			asserts.push(
				`deadline% ${deliveredWithinDeadlinePct.toFixed(2)} < assertMinDeadlineDeliveryPct ${params.assertMinDeadlineDeliveryPct}`,
			);
		}
		if (params.assertMaxOverheadFactor > 0 && overheadFactorData > params.assertMaxOverheadFactor) {
			asserts.push(
				`overheadFactorData ${overheadFactorData.toFixed(3)} > assertMaxOverheadFactor ${params.assertMaxOverheadFactor}`,
			);
		}
		if (params.assertMaxUploadFracPct > 0 && maxUploadFracPct > params.assertMaxUploadFracPct) {
			asserts.push(
				`maxUploadFracPct ${maxUploadFracPct.toFixed(1)} > assertMaxUploadFracPct ${params.assertMaxUploadFracPct}`,
			);
		}
		if (params.assertMaxControlBpp > 0 && controlBpp > params.assertMaxControlBpp) {
			asserts.push(
				`controlBpp ${controlBpp.toFixed(4)} > assertMaxControlBpp ${params.assertMaxControlBpp}`,
			);
		}
		if (params.assertMaxTrackerBpp > 0 && trackerBpp > params.assertMaxTrackerBpp) {
			asserts.push(
				`trackerBpp ${trackerBpp.toFixed(4)} > assertMaxTrackerBpp ${params.assertMaxTrackerBpp}`,
			);
		}
		if (params.assertMaxRepairBpp > 0 && repairBpp > params.assertMaxRepairBpp) {
			asserts.push(
				`repairBpp ${repairBpp.toFixed(4)} > assertMaxRepairBpp ${params.assertMaxRepairBpp}`,
			);
		}
		if (asserts.length > 0) {
			throw new Error(`fanout-peerbit-sim assertions failed: ${asserts.join("; ")}`);
		}

		await session.stop();
	} finally {
		clearTimeout(timeoutId);
	}
};

void main().catch((err) => {
	console.error(err);
	process.exitCode = 1;
});
