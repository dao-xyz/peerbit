// Interfaces for the optional native (wasm) DirectStream core implemented by
// `@peerbit/network-rust` (the `peerbit_wire` crate's `direct_stream`
// modules). When `DirectStreamOptions.rustCore` is set, the routing table,
// seen-cache dedup, outbound lane scheduling and the seek-routing/relay/ack
// decisions run in Rust; the TS class remains the socket owner and byte
// pump. With the option unset the pure TS implementations are used,
// unchanged.
import type { Uint8ArrayList } from "uint8arraylist";
import type { PushableLanes } from "./pushable-lanes.js";
import type { RoutesLike } from "./routes.js";

/**
 * Native (wasm) batch decode+verify module for inbound frames, implemented by
 * `@peerbit/network-rust`. The result is a flat Uint32Array with
 * NATIVE_WIRE_RECORD_WORDS words per frame; the layout is defined by the
 * `peerbit_wire` crate (see RECORD_* in its lib.rs) and mirrored by the
 * NATIVE_WIRE_* constants in index.ts.
 */
export interface NativeWire {
	decodeAndVerifyBatch(frames: Uint8Array[], nowMs: number): Uint32Array;
}

/** Seen-cache dedup counter backed by the native core. */
export interface RustSeenCache {
	/**
	 * Bump the seen counter for a frame and return how many times it was seen
	 * before. `kind` 0 keys by message id (first 33 frame bytes, the
	 * `getMsgId` rule), 1 by sha256 of the whole frame (the ACK path).
	 */
	modify(bytes: Uint8Array, kind: 0 | 1): number;
	clear(): void;
}

/** Routing/relay/ack decision functions executed by the native core. */
export interface RustStreamDecisions {
	shouldIgnoreData(args: {
		seenBefore: number;
		acknowledgedMode: boolean;
		redundancy: number;
		hops: string[];
		me: string;
		signedBySelf: boolean;
	}): boolean;
	shouldAcknowledge(args: {
		isRecipient: boolean;
		seenBefore: number;
		redundancy: number;
	}): boolean;
	ackNextHop(
		trace: string[],
		me: string,
	): { myIndex: number; next?: string };
	seekAckRouteUpdate(args: {
		current: string;
		upstream?: string;
		downstream: string;
	}): { from: string; neighbour: string };
	filterFloodTargets(
		candidates: string[],
		from: string,
		signed: string[],
		hops: string[],
	): Uint32Array;
	filterSilentRelayRecipients(
		recipients: string[],
		me: string,
		from: string,
		connected: string[],
		hops: string[],
	): string[];
	selectRedundancyProbes(
		peers: string[],
		used: string[],
		redundancy: number,
	): string[];
}

export interface RustRoutesInit {
	me: string;
	routeMaxRetentionPeriod: number;
	signal?: AbortSignal;
	maxFromEntries?: number;
	maxTargetsPerFrom?: number;
	maxRelaysPerTarget?: number;
}

export interface RustLanesInit {
	lanes: number;
	maxBufferedBytes?: number;
	onBufferSize?(bufferedBytes: number): void;
	onPush?(value: { byteLength: number }, lane: number): void;
}

/**
 * The full native DirectStream core: created by
 * `@peerbit/network-rust`'s `createRustCoreStream()` and passed as
 * `DirectStreamOptions.rustCore`.
 */
export interface RustCoreStream {
	/** Batched inbound decode + signature verification (implies nativeWire). */
	nativeWire?: NativeWire;
	createRoutes(init: RustRoutesInit): RoutesLike;
	createSeenCache(init: { max: number; ttl: number }): RustSeenCache;
	createLanes(init: RustLanesInit): PushableLanes<Uint8Array | Uint8ArrayList>;
	decisions: RustStreamDecisions;
}

/**
 * Test-only injection point: when the `PEERBIT_STREAM_RUST_CORE` env flag is
 * set, DirectStream instances constructed without an explicit `rustCore`
 * option pick up the core installed on globalThis under this key. This lets
 * the full @peerbit/stream test-suite run unmodified against the native core
 * (`rustCore: false` opts a specific instance out).
 */
export const RUST_CORE_GLOBAL_KEY = "__peerbitStreamRustCore";

export const resolveInjectedRustCore = (): RustCoreStream | undefined => {
	const processLike = (
		globalThis as {
			process?: { env?: Record<string, string | undefined> };
		}
	).process;
	if (!processLike?.env?.PEERBIT_STREAM_RUST_CORE) {
		return undefined;
	}
	return (globalThis as Record<string, unknown>)[RUST_CORE_GLOBAL_KEY] as
		| RustCoreStream
		| undefined;
};
