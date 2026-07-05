// TS adapter for the native DirectStream core (`direct_stream` modules of
// the peerbit_wire crate). Implements the `RustCoreStream` surface consumed
// by `@peerbit/stream` when `rustCore` mode is enabled: the routing table,
// seen-cache, outbound lane scheduler and relay/ack decisions execute in
// wasm; this file only shuttles inputs/outputs and owns the JS-side promise
// and timer machinery (which cannot live in wasm).
import type {
	PushableLanes,
	RelayInfo,
	RouteInfo,
	RoutesLike,
	RustCoreStream,
	RustLanesInit,
	RustRoutesInit,
	RustSeenCache,
	RustStreamDecisions,
} from "@peerbit/stream";
import type { DirectStreamAckRouteHint } from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import pDefer, { type DeferredPromise } from "p-defer";
import type { Uint8ArrayList } from "uint8arraylist";
import {
	type BlockExchangeWasmExports,
	createRustBlockExchange,
} from "./block-exchange.js";
import {
	type FanoutTreeWasmExports,
	createRustFanoutTree,
} from "./fanout-tree.js";
import {
	type TopicControlWasmExports,
	createRustTopicControl,
} from "./topic-control.js";
import { loadWasm } from "./wasm.js";

const ROUTES_ADD_OUTCOMES = ["new", "updated", "restart"] as const;
const ROUTES_ADD_CLEANUP_REQUESTED = 0x100;

type WasmRoutesInstance = {
	add(
		from: string,
		neighbour: string,
		target: string,
		distance: number,
		session: number,
		remoteSession: number,
		nowMs: number,
	): number;
	cleanup_pending(nowMs: number): void;
	get_route_max_retention_period(): number;
	set_route_max_retention_period(ms: number): void;
	remove(target: string): string[];
	remove_neighbour(neighbour: string): void;
	find_neighbor_json(from: string, target: string): string | undefined;
	get_route_hints_json(from: string, target: string, nowMs: number): string;
	is_reachable(from: string, target: string, maxDistance?: number): boolean;
	has_target(target: string): boolean;
	update_session(remote: string, session?: number): boolean;
	get_session(remote: string): number | undefined;
	get_dependent(peer: string): string[];
	count(from: string): number;
	count_all(): number;
	get_fanout_json(
		from: string,
		tos: string[],
		redundancy: number,
	): string | undefined;
	get_prunable(neighbours: string[]): string[];
	clear(): void;
	dump_json(): string;
};

type WasmSeenCacheInstance = {
	modify(frame: Uint8Array, keyKind: number, nowMs: number): number;
	clear(): void;
};

type WasmLanesInstance = {
	push(lane: number, byteLength: number): number;
	shift(): number;
	total_bytes(): number;
	lane_bytes(lane: number): number;
	is_empty(): boolean;
	clear(): void;
};

type DirectStreamWasmExports = {
	DirectStreamRoutes: new (
		me: string,
		routeMaxRetentionPeriodMs?: number,
		maxFromEntries?: number,
		maxTargetsPerFrom?: number,
		maxRelaysPerTarget?: number,
	) => WasmRoutesInstance;
	DirectStreamSeenCache: new (
		max: number,
		ttlMs: number,
	) => WasmSeenCacheInstance;
	DirectStreamLanes: new (
		lanes: number,
		maxBufferedBytes?: number,
	) => WasmLanesInstance;
	ds_should_ignore_data(
		seenBefore: number,
		acknowledgedMode: boolean,
		redundancy: number,
		hops: string[],
		me: string,
		signedBySelf: boolean,
	): boolean;
	ds_should_acknowledge(
		isRecipient: boolean,
		seenBefore: number,
		redundancy: number,
	): boolean;
	ds_ack_next_hop(trace: string[], me: string): string[];
	ds_seek_ack_route_update(
		current: string,
		upstream: string | undefined,
		downstream: string,
	): string[];
	ds_filter_flood_targets(
		candidates: string[],
		from: string,
		signed: string[],
		hops: string[],
	): Uint32Array;
	ds_filter_silent_relay_recipients(
		recipients: string[],
		me: string,
		from: string,
		connected: string[],
		hops: string[],
	): string[];
	ds_select_redundancy_probes(
		peers: string[],
		used: string[],
		redundancy: number,
	): string[];
	decode_and_verify_batch(frames: Uint8Array[], nowMs: number): Uint32Array;
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
};

type RelayInfoJson = {
	hash: string;
	distance: number;
	session: number;
	updatedAt: number;
	expireAt: number | null;
};

type RouteInfoJson = {
	session: number;
	remoteSession: number;
	list: RelayInfoJson[];
};

const parseRelayInfo = (relay: RelayInfoJson): RelayInfo => ({
	hash: relay.hash,
	distance: relay.distance,
	session: relay.session,
	updatedAt: relay.updatedAt,
	expireAt: relay.expireAt ?? undefined,
});

const parseRouteInfo = (info: RouteInfoJson): RouteInfo => ({
	session: info.session,
	remoteSession: info.remoteSession,
	list: info.list.map(parseRelayInfo),
});

/**
 * `RoutesLike` backed by the wasm routing table. Route decisions
 * (add/expiry/fanout/reachability/pruning) run in Rust; this class mirrors
 * the TS `Routes.requestCleanup` coalesced-timer scheduling (a single timer
 * per instance armed for `routeMaxRetentionPeriod + 100` ms).
 */
class RustRoutes implements RoutesLike {
	private readonly wasm: WasmRoutesInstance;
	private readonly me: string;
	private readonly signal?: AbortSignal;
	private cleanupTimer?: ReturnType<typeof setTimeout>;

	constructor(module: DirectStreamWasmExports, init: RustRoutesInit) {
		this.wasm = new module.DirectStreamRoutes(
			init.me,
			init.routeMaxRetentionPeriod,
			init.maxFromEntries,
			init.maxTargetsPerFrom,
			init.maxRelaysPerTarget,
		);
		this.me = init.me;
		this.signal = init.signal;
	}

	get routeMaxRetentionPeriod(): number {
		return this.wasm.get_route_max_retention_period();
	}

	set routeMaxRetentionPeriod(ms: number) {
		this.wasm.set_route_max_retention_period(ms);
	}

	/** Snapshot of the native table in the TS `Routes.routes` map shape. */
	get routes(): Map<string, Map<string, RouteInfo>> {
		const dump = JSON.parse(this.wasm.dump_json()) as [
			string,
			[string, RouteInfoJson][],
		][];
		const out = new Map<string, Map<string, RouteInfo>>();
		for (const [from, targets] of dump) {
			const fromMap = new Map<string, RouteInfo>();
			for (const [target, info] of targets) {
				fromMap.set(target, parseRouteInfo(info));
			}
			out.set(from, fromMap);
		}
		return out;
	}

	private requestCleanup() {
		if (this.signal?.aborted) {
			return;
		}
		if (this.cleanupTimer) {
			return;
		}
		this.cleanupTimer = setTimeout(
			() => {
				this.cleanupTimer = undefined;
				this.wasm.cleanup_pending(Date.now());
			},
			this.routeMaxRetentionPeriod + 100,
		);
	}

	add(
		from: string,
		neighbour: string,
		target: string,
		distance: number,
		session: number,
		remoteSession: number,
	): "new" | "updated" | "restart" {
		const result = this.wasm.add(
			from,
			neighbour,
			target,
			distance,
			session,
			remoteSession,
			Date.now(),
		);
		if (result & ROUTES_ADD_CLEANUP_REQUESTED) {
			this.requestCleanup();
		}
		return ROUTES_ADD_OUTCOMES[result & 0xff];
	}

	clear(): void {
		this.wasm.clear();
		if (this.cleanupTimer) {
			clearTimeout(this.cleanupTimer);
		}
		this.cleanupTimer = undefined;
	}

	remove(target: string): string[] {
		return this.wasm.remove(target);
	}

	removeNeighbour(neighbour: string): void {
		this.wasm.remove_neighbour(neighbour);
	}

	findNeighbor(from: string, target: string): RouteInfo | undefined {
		const json = this.wasm.find_neighbor_json(from, target);
		return json == null
			? undefined
			: parseRouteInfo(JSON.parse(json) as RouteInfoJson);
	}

	getRouteHints(from: string, target: string): DirectStreamAckRouteHint[] {
		const hints = JSON.parse(
			this.wasm.get_route_hints_json(from, target, Date.now()),
		) as {
			from: string;
			target: string;
			nextHop: string;
			distance: number;
			session: number;
			updatedAt: number;
			expiresAt: number | null;
		}[];
		return hints.map((hint) => ({
			kind: "directstream-ack",
			from: hint.from,
			target: hint.target,
			nextHop: hint.nextHop,
			distance: hint.distance,
			session: hint.session,
			updatedAt: hint.updatedAt,
			expiresAt: hint.expiresAt ?? undefined,
		}));
	}

	getBestRouteHint(
		from: string,
		target: string,
	): DirectStreamAckRouteHint | undefined {
		return this.getRouteHints(from, target)[0];
	}

	isReachable(from: string, target: string, maxDistance?: number): boolean {
		return this.wasm.is_reachable(from, target, maxDistance);
	}

	hasTarget(target: string): boolean {
		return this.wasm.has_target(target);
	}

	updateSession(remote: string, session?: number): boolean {
		return this.wasm.update_session(remote, session ?? undefined);
	}

	getSession(remote: string): number | undefined {
		return this.wasm.get_session(remote);
	}

	getDependent(peer: string): string[] {
		return this.wasm.get_dependent(peer);
	}

	count(from: string = this.me): number {
		return this.wasm.count(from);
	}

	countAll(): number {
		return this.wasm.count_all();
	}

	getFanout(
		from: string,
		tos: string[],
		redundancy: number,
	): Map<string, Map<string, { to: string; timestamp: number }>> | undefined {
		const json = this.wasm.get_fanout_json(from, tos, redundancy);
		if (json == null) {
			return undefined;
		}
		const pairs = JSON.parse(json) as [string, [string, number][]][];
		const out = new Map<string, Map<string, { to: string; timestamp: number }>>();
		for (const [neighbour, targets] of pairs) {
			const fanout = new Map<string, { to: string; timestamp: number }>();
			for (const [to, timestamp] of targets) {
				fanout.set(to, { to, timestamp });
			}
			out.set(neighbour, fanout);
		}
		return out;
	}

	getPrunable(neighbours: string[]): string[] {
		return this.wasm.get_prunable(neighbours);
	}
}

class RustSeenCacheAdapter implements RustSeenCache {
	private readonly wasm: WasmSeenCacheInstance;

	constructor(
		module: DirectStreamWasmExports,
		init: { max: number; ttl: number },
	) {
		this.wasm = new module.DirectStreamSeenCache(init.max, init.ttl);
	}

	modify(bytes: Uint8Array, kind: 0 | 1): number {
		return this.wasm.modify(bytes, kind, Date.now());
	}

	clear(): void {
		this.wasm.clear();
	}
}

interface QueuedRecord {
	value?: Uint8Array | Uint8ArrayList;
	done?: boolean;
	error?: Error;
}

const clampLane = (lane: number, lanes: number): number => {
	if (!Number.isFinite(lane)) return 0;
	lane = lane | 0;
	if (lane < 0) return 0;
	if (lane >= lanes) return lanes - 1;
	return lane;
};

/**
 * `PushableLanes`-compatible outbound queue whose ordering, byte accounting
 * and overflow decisions run in the native lane scheduler. Bytes never cross
 * the wasm boundary: the scheduler orders (sequence, byteLength, lane)
 * records and this wrapper maps sequences back to the queued chunks. The
 * promise machinery (drain/onEmpty/onBufferedBelow, end/return/throw
 * semantics incl. the buffer-clearing error path) mirrors
 * `stream/src/pushable-lanes.ts`.
 */
const createRustLanes = (
	module: DirectStreamWasmExports,
	init: RustLanesInit,
): PushableLanes<Uint8Array | Uint8ArrayList> => {
	const laneCount = Math.max(1, init.lanes | 0);
	const scheduler = new module.DirectStreamLanes(
		laneCount,
		init.maxBufferedBytes,
	);
	const records = new Map<number, QueuedRecord>();
	let ended = false;
	let wake: (() => void) | null = null;
	let drain = pDefer<void>();
	const bufferedBelowWaiters = new Set<{
		limitBytes: number;
		deferred: DeferredPromise<void>;
	}>();

	const totalBufferedBytes = () => scheduler.total_bytes();

	const notifyBufferSize = () => {
		init.onBufferSize?.(totalBufferedBytes());
	};

	const notifyBufferedBelowWaiters = () => {
		if (bufferedBelowWaiters.size === 0) return;
		const size = totalBufferedBytes();
		for (const waiter of [...bufferedBelowWaiters]) {
			if (size <= waiter.limitBytes) {
				bufferedBelowWaiters.delete(waiter);
				waiter.deferred.resolve();
			}
		}
	};

	const resolveBufferedBelowWaiters = () => {
		for (const waiter of [...bufferedBelowWaiters]) {
			bufferedBelowWaiters.delete(waiter);
			waiter.deferred.resolve();
		}
	};

	const enqueueRecord = (record: QueuedRecord, lane: number): boolean => {
		const byteLength = record.value?.byteLength ?? 0;
		const sequence = scheduler.push(lane, byteLength);
		if (sequence < 0) {
			return false;
		}
		records.set(sequence, record);
		wake?.();
		return true;
	};

	const clearQueue = () => {
		scheduler.clear();
		records.clear();
	};

	const getNext = (): { done: boolean; value?: Uint8Array | Uint8ArrayList } => {
		const sequence = scheduler.shift();
		if (sequence < 0) {
			return { done: true };
		}
		const record = records.get(sequence);
		records.delete(sequence);
		if (record?.error != null) {
			throw record.error;
		}
		return { done: record?.done === true, value: record?.value };
	};

	let pushable: any;

	const waitNext = async () => {
		try {
			if (!scheduler.is_empty()) {
				return getNext();
			}
			if (ended) {
				return { done: true };
			}
			return await new Promise<{
				done: boolean;
				value?: Uint8Array | Uint8ArrayList;
			}>((resolve, reject) => {
				wake = () => {
					wake = null;
					try {
						resolve(getNext());
					} catch (err: any) {
						reject(err);
					}
				};
			});
		} finally {
			notifyBufferSize();
			notifyBufferedBelowWaiters();
			if (scheduler.is_empty()) {
				queueMicrotask(() => {
					drain.resolve();
					drain = pDefer<void>();
				});
			}
		}
	};

	const push = (value: Uint8Array | Uint8ArrayList, lane: number = 0) => {
		if (ended) {
			return pushable;
		}
		if (!enqueueRecord({ done: false, value }, clampLane(lane, laneCount))) {
			const wouldBe = totalBufferedBytes() + value.byteLength;
			throw new Error(
				`pushableLanes buffer overflow: ${wouldBe} bytes > maxBufferedBytes=${init.maxBufferedBytes}`,
			);
		}
		init.onPush?.(value, clampLane(lane, laneCount));
		notifyBufferSize();
		return pushable;
	};

	const end = (err?: Error) => {
		if (ended) return pushable;
		ended = true;
		queueMicrotask(() => {
			drain.resolve();
			drain = pDefer<void>();
			resolveBufferedBelowWaiters();
		});
		if (err != null) {
			// mirror bufferError: pending values are dropped, the next read throws
			clearQueue();
			notifyBufferSize();
			notifyBufferedBelowWaiters();
			enqueueRecord({ error: err }, 0);
		} else {
			enqueueRecord({ done: true }, 0);
		}
		return pushable;
	};

	const _return = () => {
		clearQueue();
		notifyBufferSize();
		notifyBufferedBelowWaiters();
		end();
		return { done: true };
	};

	const _throw = (err: Error) => {
		end(err);
		return { done: true };
	};

	pushable = {
		[Symbol.asyncIterator]() {
			return this;
		},
		next: waitNext,
		return: _return,
		throw: _throw,
		push,
		end,

		get readableLength(): number {
			return totalBufferedBytes();
		},

		getReadableLength(lane?: number): number {
			if (lane == null) return totalBufferedBytes();
			return scheduler.lane_bytes(clampLane(lane, laneCount));
		},

		onEmpty: async (opts?: { signal?: AbortSignal }) => {
			const signal = opts?.signal;
			signal?.throwIfAborted?.();
			if (scheduler.is_empty() || ended) return;

			let cancel: Promise<void> | undefined;
			let listener: (() => void) | undefined;
			if (signal != null) {
				cancel = new Promise<void>((_resolve, reject) => {
					listener = () => reject(new AbortError());
					signal.addEventListener("abort", listener!);
				});
			}
			try {
				await Promise.race(
					cancel != null ? [drain.promise, cancel] : [drain.promise],
				);
			} finally {
				if (listener != null) {
					signal?.removeEventListener("abort", listener);
				}
			}
		},

		onBufferedBelow: async (
			limitBytes: number,
			opts?: { signal?: AbortSignal },
		) => {
			const signal = opts?.signal;
			signal?.throwIfAborted?.();
			const normalizedLimit = Math.max(0, Math.floor(limitBytes));
			if (totalBufferedBytes() <= normalizedLimit || ended) return;

			const waiter = {
				limitBytes: normalizedLimit,
				deferred: pDefer<void>(),
			};
			bufferedBelowWaiters.add(waiter);

			let cancel: Promise<void> | undefined;
			let listener: (() => void) | undefined;
			if (signal != null) {
				cancel = new Promise<void>((_resolve, reject) => {
					listener = () => {
						bufferedBelowWaiters.delete(waiter);
						reject(new AbortError());
					};
					signal.addEventListener("abort", listener!);
				});
			}
			try {
				await Promise.race(
					cancel != null
						? [waiter.deferred.promise, cancel]
						: [waiter.deferred.promise],
				);
			} finally {
				bufferedBelowWaiters.delete(waiter);
				if (listener != null) {
					signal?.removeEventListener("abort", listener);
				}
			}
		},
	};

	return pushable as PushableLanes<Uint8Array | Uint8ArrayList>;
};

const buildDecisions = (
	module: DirectStreamWasmExports,
): RustStreamDecisions => ({
	shouldIgnoreData: (args) =>
		module.ds_should_ignore_data(
			args.seenBefore,
			args.acknowledgedMode,
			args.redundancy,
			args.hops,
			args.me,
			args.signedBySelf,
		),
	shouldAcknowledge: (args) =>
		module.ds_should_acknowledge(
			args.isRecipient,
			args.seenBefore,
			args.redundancy,
		),
	ackNextHop: (trace, me) => {
		const out = module.ds_ack_next_hop(trace, me);
		return { myIndex: parseInt(out[0], 10), next: out[1] };
	},
	seekAckRouteUpdate: (args) => {
		const out = module.ds_seek_ack_route_update(
			args.current,
			args.upstream,
			args.downstream,
		);
		return { from: out[0], neighbour: out[1] };
	},
	filterFloodTargets: (candidates, from, signed, hops) =>
		module.ds_filter_flood_targets(candidates, from, signed, hops),
	filterSilentRelayRecipients: (recipients, me, from, connected, hops) =>
		module.ds_filter_silent_relay_recipients(
			recipients,
			me,
			from,
			connected,
			hops,
		),
	selectRedundancyProbes: (peers, used, redundancy) =>
		module.ds_select_redundancy_probes(peers, used, redundancy),
});

/**
 * Create the native DirectStream core for
 * `DirectStreamOptions.rustCore`. Includes the batched inbound
 * decode+verify module (nativeWire), the block-exchange components
 * consumed by `@peerbit/blocks` and the topic-control and fanout-tree
 * components consumed by `@peerbit/pubsub`, so enabling rust-core also
 * enables the native inbound wire path and the native protocol codecs.
 */
export const createRustCoreStream = async (): Promise<RustCoreStream> => {
	const wasm = await loadWasm<
		DirectStreamWasmExports &
			BlockExchangeWasmExports &
			TopicControlWasmExports &
			FanoutTreeWasmExports
	>();
	return {
		nativeWire: {
			decodeAndVerifyBatch: (frames, nowMs) =>
				wasm.decode_and_verify_batch(frames, nowMs),
		},
		createRoutes: (init) => new RustRoutes(wasm, init),
		createSeenCache: (init) => new RustSeenCacheAdapter(wasm, init),
		createLanes: (init) => createRustLanes(wasm, init),
		decisions: buildDecisions(wasm),
		blockExchange: createRustBlockExchange(wasm),
		topicControl: createRustTopicControl(wasm),
		fanout: createRustFanoutTree(wasm),
	};
};
