import {
	MuxerClosedError,
	StreamResetError,
	TypedEventEmitter,
	UnsupportedProtocolError,
} from "@libp2p/interface";
import type {
	AbortOptions,
	Connection,
	Libp2pEvents,
	PeerId,
	PeerStore,
	PrivateKey,
	Stream,
	TypedEventTarget,
} from "@libp2p/interface";
import type { AddressManager, Registrar } from "@libp2p/interface-internal";
import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";
import { Circuit } from "@multiformats/multiaddr-matcher";
import { Cache } from "@peerbit/cache";
import {
	PublicSignKey,
	getKeypairFromPrivateKey,
	getPublicKeyFromPeerId,
	ready,
	sha256Base64,
	toBase64,
} from "@peerbit/crypto";
import type { SignatureWithKey } from "@peerbit/crypto";
import {
	ACK,
	AcknowledgeAnyWhere,
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	DeliveryError,
	Goodbye,
	InvalidMessageError,
	Message,
	MessageHeader,
	MultiAddrinfo,
	NotStartedError,
	SilentDelivery,
	TracedDelivery,
	coercePeerRefsToHashes,
	deliveryModeHasReceiver,
	getMsgId,
} from "@peerbit/stream-interface";
import type {
	IdOptions,
	PeerRefs,
	PriorityOptions,
	PublicKeyFromHashResolver,
	StreamEvents,
	WaitForAnyOpts,
	WaitForBaseOpts,
	WaitForPeer,
	WaitForPresentOpts,
	WithExtraSigners,
	WithMode,
	WithTo,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError, delay } from "@peerbit/time";
import { abortableSource } from "abortable-iterator";
import { anySignal } from "any-signal";
import * as lp from "it-length-prefixed";
import { pipe } from "it-pipe";
import { type Pushable, pushable } from "it-pushable";
import pDefer, { type DeferredPromise } from "p-defer";
import Queue from "p-queue";
import { Uint8ArrayList } from "uint8arraylist";
import {
	computeSeekAckRouteUpdate,
	shouldAcknowledgeDataMessage,
} from "./core/seek-routing.js";
import { logger } from "./logger.js";
import { type PushableLanes, pushableLanes } from "./pushable-lanes.js";
import { MAX_ROUTE_DISTANCE, Routes } from "./routes.js";
import { BandwidthTracker } from "./stats.js";
import { waitForEvent } from "./wait-for-event.js";

export { logger };
const warn = logger.newScope("warn");

export { BandwidthTracker }; // might be useful for others

export const dontThrowIfDeliveryError = (e: any) => {
	if (
		e instanceof DeliveryError ||
		e instanceof TimeoutError ||
		e instanceof AbortError
	) {
		return;
	}
	throw e;
};

const logError = (e?: any) => {
	if (e?.message === "Cannot push value onto an ended pushable") {
		return; // ignore since we are trying to push to a closed stream
	}
	return logger.error(e?.message);
};

const waitForDrain = async (
	stream: Stream,
	signal?: AbortSignal,
): Promise<void> => {
	if (signal?.aborted) {
		throw signal.reason ?? new AbortError("Drain wait aborted");
	}
	return new Promise<void>((resolve, reject) => {
		let done = false;
		const cleanup = () => {
			if (done) return;
			done = true;
			stream.removeEventListener("drain", onDrain);
			stream.removeEventListener("close", onClose);
			signal?.removeEventListener("abort", onAbort);
		};
		const onDrain = () => {
			cleanup();
			resolve();
		};
		const onAbort = () => {
			cleanup();
			reject(signal?.reason ?? new AbortError("Drain wait aborted"));
		};
		const onClose = (event: Event) => {
			cleanup();
			const detail = (event as any)?.detail;
			const err = detail?.error ?? (event as any)?.error;
			reject(err ?? new Error("Stream closed"));
		};
		stream.addEventListener("drain", onDrain, { once: true });
		stream.addEventListener("close", onClose, { once: true });
		signal?.addEventListener("abort", onAbort, { once: true });
	});
};

export interface PeerStreamsInit {
	peerId: PeerId;
	publicKey: PublicSignKey;
	protocol: string;
	connId: string;
}
const DEFAULT_SEEK_MESSAGE_REDUDANCY = 2;
const DEFAULT_SILENT_MESSAGE_REDUDANCY = 1;

const isWebsocketConnection = (c: Connection) =>
	c.remoteAddr
		.getComponents()
		.some((component) => component.name === "ws" || component.name === "wss");

export interface PeerStreamEvents {
	"stream:inbound": CustomEvent<never>;
	"stream:outbound": CustomEvent<never>;
	close: CustomEvent<never>;
}

const SEEK_DELIVERY_TIMEOUT = 10e3;
const ROUTE_MAX_RETANTION_PERIOD = 5e4;
const MAX_DATA_LENGTH_IN = 15e6 + 1000; // 15 mb and some metadata
const MAX_DATA_LENGTH_OUT = 1e7 + 1000; // 10 mb and some metadata

const MAX_QUEUED_BYTES = MAX_DATA_LENGTH_IN * 50;

const DEFAULT_PRUNE_CONNECTIONS_INTERVAL = 2e4;
const DEFAULT_MIN_CONNECTIONS = 2;
const DEFAULT_MAX_CONNECTIONS = 300;

const DEFAULT_PRUNED_CONNNECTIONS_TIMEOUT = 30 * 1000;

const DEFAULT_CREATE_OUTBOUND_STREAM_TIMEOUT = 30_000;

const PRIORITY_LANES = 4;

const getLaneFromPriority = (priority: number) => {
	// Higher priority numbers should be drained first.
	// Lane 0 is the fastest/highest-priority lane.
	const maxLane = PRIORITY_LANES - 1;
	if (!Number.isFinite(priority)) {
		return maxLane;
	}
	const clampedPriority = Math.max(0, Math.min(maxLane, Math.floor(priority)));
	return maxLane - clampedPriority;
};
interface OutboundCandidate {
	raw: Stream;
	pushable: PushableLanes<Uint8Array>;
	created: number;
	bytesDelivered: number;
	aborted: boolean;
	existing: boolean;
}
export interface InboundStreamRecord {
	raw: Stream;
	iterable: AsyncIterable<Uint8ArrayList>;
	abortController: AbortController;
	created: number;
	lastActivity: number;
	bytesReceived: number;
}
// Hook for tests to override queued length measurement (peerStreams, default impl)
export let measureOutboundQueuedBytes: (
	ps: PeerStreams, // return queued bytes for active outbound (all lanes) or 0 if none
) => number = (ps: PeerStreams) => {
	const active = ps._getActiveOutboundPushable();
	if (!active) return 0;
	// Prefer lane-aware helper if present
	// @ts-ignore - optional test helper
	if (typeof active.getReadableLength === "function") {
		try {
			return active.getReadableLength() || 0;
		} catch {
			// ignore
		}
	}
	// @ts-ignore fallback for vanilla pushable
	return active.readableLength || 0;
};
/**
 * Thin wrapper around a peer's inbound / outbound pubsub streams
 */
export class PeerStreams extends TypedEventEmitter<PeerStreamEvents> {
	public readonly peerId: PeerId;
	public readonly publicKey: PublicSignKey;
	public readonly protocol: string;
	// Removed dedicated outboundStream; first element of outboundStreams[] is active

	/**
	 * Backwards compatible single inbound references (points to first inbound candidate)
	 */
	public inboundStream?: AsyncIterable<Uint8ArrayList>;
	public rawInboundStream?: Stream;

	/**
	 * Multiple inbound stream support (more permissive than outbound)
	 * We retain concurrent inbound streams to avoid races during migration; inactive ones can later be pruned.
	 */
	public inboundStreams: InboundStreamRecord[] = [];

	private _inboundPruneTimer?: ReturnType<typeof setTimeout>;
	public static INBOUND_IDLE_MS = 10_000; // configurable grace for inactivity (made public for tests)
	static MAX_INBOUND_STREAMS = 8; // sensible default to prevent flood

	private outboundAbortController: AbortController;

	private closed: boolean;

	public connId: string;

	public seekedOnce: boolean;

	private usedBandWidthTracker: BandwidthTracker;

	// Unified outbound streams list (during grace may contain >1; after pruning length==1)
	private outboundStreams: OutboundCandidate[] = [];
	// Public debug exposure of current raw outbound streams (during grace may contain >1)
	public get rawOutboundStreams(): Stream[] {
		return this.outboundStreams.map((c) => c.raw);
	}
	public _getActiveOutboundPushable(): PushableLanes<Uint8Array> | undefined {
		return this.outboundStreams[0]?.pushable;
	}
	public getOutboundQueuedBytes(): number {
		return this._getActiveOutboundPushable()?.readableLength ?? 0;
	}

	public getOutboundQueuedBytesByLane(): number[] {
		const p = this._getActiveOutboundPushable();
		if (!p) return Array(PRIORITY_LANES).fill(0);
		const out: number[] = [];
		for (let lane = 0; lane < PRIORITY_LANES; lane++) {
			out.push(p.getReadableLength(lane));
		}
		return out;
	}
	public _getOutboundCount() {
		return this.outboundStreams.length;
	}

	public _getInboundCount() {
		return this.inboundStreams.length;
	}

	public _debugInboundStats(): {
		id: string;
		created: number;
		lastActivity: number;
		bytesReceived: number;
	}[] {
		return this.inboundStreams.map((c) => ({
			id: c.raw.id,
			created: c.created,
			lastActivity: c.lastActivity,
			bytesReceived: c.bytesReceived,
		}));
	}
	private _outboundPruneTimer?: ReturnType<typeof setTimeout>;
	private static readonly OUTBOUND_GRACE_MS = 500; // TODO configurable

	private _addOutboundCandidate(raw: Stream): OutboundCandidate {
		const existing = this.outboundStreams.find((c) => c.raw === raw);
		if (existing) return existing;
		const pushableInst = pushableLanes<Uint8Array>({
			lanes: PRIORITY_LANES,
			onPush: (val: Uint8Array) => {
				candidate.bytesDelivered += val.length || val.byteLength || 0;
			},
		});
		const candidate: OutboundCandidate = {
			raw,
			pushable: pushableInst,
			created: Date.now(),
			bytesDelivered: 0,
			aborted: false,
			existing: false,
		};
		const pump = (async () => {
			try {
				const encodedSource = lp.encode(
					pushableInst as AsyncIterable<Uint8Array | Uint8ArrayList>,
					{ maxDataLength: MAX_DATA_LENGTH_OUT },
				) as AsyncIterable<Uint8Array | Uint8ArrayList>;
				for await (const chunk of encodedSource) {
					if (this.outboundAbortController.signal.aborted) {
						throw (
							this.outboundAbortController.signal.reason ??
							new AbortError("Outbound stream aborted")
						);
					}
					const bytes =
						chunk instanceof Uint8ArrayList ? chunk.subarray() : chunk;
					if (!raw.send(bytes)) {
						await waitForDrain(raw, this.outboundAbortController.signal);
					}
				}
				raw.close?.();
			} catch (err) {
				candidate.aborted = true;
				try {
					pushableInst.end(err as Error);
				} catch {}
				throw err;
			}
		})();
		pump.catch((e: any) => {
			candidate.aborted = true;
			logError(e as { message: string } as any);
		});
		this.outboundStreams.push(candidate);
		const origAbort = raw.abort?.bind(raw);
		raw.abort = (err?: any) => {
			candidate.aborted = true;
			try {
				pushableInst.end(err);
			} catch {}
			return origAbort?.(err);
		};

		const origClose = raw.close?.bind(raw);
		if (origClose) {
			raw.close = (...args: any[]) => {
				candidate.aborted = true;
				try {
					pushableInst.end();
				} catch {}
				return origClose(...args);
			};
		}
		return candidate;
	}

	private _scheduleOutboundPrune(reset = true) {
		if (this.outboundStreams.length <= 1) return;
		if (reset && this._outboundPruneTimer) {
			clearTimeout(this._outboundPruneTimer);
		}
		if (!this._outboundPruneTimer) {
			this._outboundPruneTimer = setTimeout(
				() => this.pruneOutboundCandidates(),
				PeerStreams.OUTBOUND_GRACE_MS,
			);
		}
	}
	constructor(init: PeerStreamsInit) {
		super();

		this.peerId = init.peerId;
		this.publicKey = init.publicKey;
		this.protocol = init.protocol;
		this.outboundAbortController = new AbortController();

		this.closed = false;
		this.connId = init.connId;
		this.usedBandWidthTracker = new BandwidthTracker(10);
		this.usedBandWidthTracker.start();
	}

	/**
	 * Do we have a connection to read from?
	 */
	get isReadable() {
		return this.inboundStreams.length > 0;
	}

	/**
	 * Do we have a connection to write on?
	 */
	get isWritable() {
		return this.outboundStreams.some((c) => !c.aborted);
	}

	get usedBandwidth() {
		return this.usedBandWidthTracker.value;
	}

	/**
	 * Send a message to this peer.
	 * Throws if there is no `stream` to write to available.
	 */
	write(data: Uint8Array | Uint8ArrayList, priority: number) {
		if (data.length > MAX_DATA_LENGTH_OUT) {
			throw new Error(
				`Message too large (${data.length * 1e-6}) mb). Needs to be less than ${
					MAX_DATA_LENGTH_OUT * 1e-6
				} mb`,
			);
		}
		if (!this.isWritable) {
			logger.error("No writable connection to " + this.peerId.toString());
			throw new Error("No writable connection to " + this.peerId.toString());
		}

		this.usedBandWidthTracker.add(data.byteLength);

		// Write to all current outbound streams (normally 1, but >1 during grace)
		const payload = data instanceof Uint8Array ? data : data.subarray();
		let successes = 0;
		let failures: any[] = [];
		const failed: OutboundCandidate[] = [];
		for (const c of this.outboundStreams) {
			if (c.aborted) {
				failures.push(new Error("aborted"));
				failed.push(c);
				continue;
			}

			try {
				c.pushable.push(payload, getLaneFromPriority(priority));
				successes++;
			} catch (e) {
				failures.push(e);
				failed.push(c);
				logError(e);
			}
		}
		if (successes === 0) {
			throw new Error(
				"All outbound writes failed (" +
					failures.map((f) => f?.message).join(", ") +
					")",
			);
		}
		if (failures.length > 0) {
			warn(
				`Partial outbound write failure: ${failures.length} failed, ${successes} succeeded`,
			);
			// Remove failed streams immediately (best-effort)
			if (failed.length) {
				this.outboundStreams = this.outboundStreams.filter(
					(c) => !failed.includes(c),
				);
				for (const f of failed) {
					try {
						f.pushable.end(new AbortError("Failed write" as any));
					} catch {}
					try {
						f.raw.abort?.(new AbortError("Failed write" as any));
					} catch {}
					try {
						f.raw.close?.();
					} catch {}
				}
				// If more than one remains schedule prune; else ensure outbound event raised
				if (this.outboundStreams.length > 1) this._scheduleOutboundPrune(true);
				else this.dispatchEvent(new CustomEvent("stream:outbound"));
			}
		}
	}

	/**
	 * Write to the outbound stream, waiting until it becomes writable.
	 * All listeners are registered with { once:true } *and* removed again
	 * in the shared `cleanup()` so nothing can dangle.
	 */
	async waitForWrite(
		bytes: Uint8Array | Uint8ArrayList,
		priority = 0,
		signal?: AbortSignal,
	) {
		if (this.closed) {
			logger.error(`Failed to send to stream ${this.peerId}: closed`);
			return;
		}

		if (this.isWritable) {
			this.write(bytes, priority);
			return;
		}

		const timeoutMs = 3_000;

		await new Promise<void>((resolve, reject) => {
			const onOutbound = () => {
				cleanup();
				resolve();
			};

			const onAbortOrClose = () => {
				cleanup();
				reject(new AbortError("Closed"));
			};

			const onTimeout = () => {
				cleanup();
				reject(new TimeoutError("Failed to deliver message, never reachable"));
			};

			const timerId = setTimeout(onTimeout, timeoutMs);

			const cleanup = () => {
				clearTimeout(timerId);
				this.removeEventListener("stream:outbound", onOutbound);
				this.removeEventListener("close", onAbortOrClose);
				signal?.removeEventListener("abort", onAbortOrClose);
			};

			this.addEventListener("stream:outbound", onOutbound, { once: true });
			this.addEventListener("close", onAbortOrClose, { once: true });
			if (signal?.aborted) {
				onAbortOrClose();
			} else {
				signal?.addEventListener("abort", onAbortOrClose, { once: true });
			}

			// Catch a race where writability flips after the first check.
			if (this.isWritable) onOutbound();
		});

		this.write(bytes, priority);
	}

	/**
	 * Attach a raw inbound stream and setup a read stream
	 */
	attachInboundStream(stream: Stream): InboundStreamRecord {
		// Support multiple concurrent inbound streams with inactivity pruning.
		// Enforce max inbound streams (drop least recently active)
		if (this.inboundStreams.length >= PeerStreams.MAX_INBOUND_STREAMS) {
			let dropIndex = 0;
			for (let i = 1; i < this.inboundStreams.length; i++) {
				const a = this.inboundStreams[i];
				const b = this.inboundStreams[dropIndex];
				if (
					a.lastActivity < b.lastActivity ||
					(a.lastActivity === b.lastActivity && a.created < b.created)
				) {
					dropIndex = i;
				}
			}
			const [drop] = this.inboundStreams.splice(dropIndex, 1);
			try {
				drop.abortController.abort();
			} catch {
				logger.error("Failed to abort inbound stream");
			}
			try {
				drop.raw.close?.();
			} catch {
				logger.error("Failed to close inbound stream");
			}
		}
		const abortController = new AbortController();
		const decoded = pipe(stream, (source) =>
			lp.decode(source, { maxDataLength: MAX_DATA_LENGTH_IN }),
		);
		const iterable = abortableSource(decoded, abortController.signal, {
			returnOnAbort: true,
			onReturnError: (err) => {
				logger.error("Inbound stream error", err?.message);
			},
		});
		const record: InboundStreamRecord = {
			raw: stream,
			iterable,
			abortController,
			created: Date.now(),
			lastActivity: Date.now(),
			bytesReceived: 0,
		};
		this.inboundStreams.push(record);
		this._scheduleInboundPrune();
		// Backwards compatibility: keep first inbound as public properties
		if (this.inboundStreams.length === 1) {
			this.rawInboundStream = stream;
			this.inboundStream = iterable;
		}
		this.dispatchEvent(new CustomEvent("stream:inbound"));
		return record;
	}

	private _scheduleInboundPrune() {
		if (this._inboundPruneTimer) return; // already scheduled
		this._inboundPruneTimer = setTimeout(() => {
			this._inboundPruneTimer = undefined;
			this._pruneInboundInactive();
			if (this.inboundStreams.length > 1) {
				// schedule again if still multiple
				this._scheduleInboundPrune();
			}
		}, PeerStreams.INBOUND_IDLE_MS);
	}

	private _pruneInboundInactive() {
		if (this.inboundStreams.length <= 1) return;
		const now = Date.now();
		// Keep at least one (the most recently active)
		this.inboundStreams.sort((a, b) => b.lastActivity - a.lastActivity);
		const keep = this.inboundStreams[0];
		const survivors: typeof this.inboundStreams = [keep];
		for (let i = 1; i < this.inboundStreams.length; i++) {
			const candidate = this.inboundStreams[i];
			if (now - candidate.lastActivity <= PeerStreams.INBOUND_IDLE_MS) {
				survivors.push(candidate);
				continue; // still active
			}
			try {
				candidate.abortController.abort();
			} catch {}
			try {
				candidate.raw.close?.();
			} catch {}
		}
		this.inboundStreams = survivors;
		// update legacy references if they were pruned
		if (!this.inboundStreams.includes(keep)) {
			this.rawInboundStream = this.inboundStreams[0]?.raw;
			this.inboundStream = this.inboundStreams[0]?.iterable;
		}
	}

	public forcePruneInbound() {
		if (this._inboundPruneTimer) {
			clearTimeout(this._inboundPruneTimer);
			this._inboundPruneTimer = undefined;
		}
		this._pruneInboundInactive();
	}

	/**
	 * Attach a raw outbound stream and setup a write stream
	 */

	async attachOutboundStream(stream: Stream) {
		if (this.outboundStreams[0] && stream.id === this.outboundStreams[0].raw.id)
			return; // duplicate
		this._addOutboundCandidate(stream);
		if (this.outboundStreams.length === 1) {
			this.dispatchEvent(new CustomEvent("stream:outbound"));
			return;
		}
		this._scheduleOutboundPrune(true);
	}

	private pruneOutboundCandidates() {
		try {
			const candidates = this.outboundStreams;
			if (!candidates.length) return;
			const now = Date.now();
			const healthy = candidates.filter(
				(c: OutboundCandidate) => !c.aborted && c.bytesDelivered > 0,
			);
			let chosen: OutboundCandidate | undefined;
			if (healthy.length === 0) {
				chosen = candidates.reduce((a, b) => (b.created > a.created ? b : a));
			} else {
				let bestScore = -Infinity;
				for (const c of healthy) {
					const age = now - c.created || 1;
					const score = c.bytesDelivered / age;
					if (
						score > bestScore ||
						(score === bestScore && chosen && c.created > chosen.created)
					) {
						bestScore = score;
						chosen = c;
					}
				}
			}
			if (!chosen) return;
			for (const c of candidates) {
				if (c === chosen) continue; // never abort chosen
				try {
					c.raw.abort?.(new AbortError("Replaced outbound stream" as any));
				} catch {
					logger.error("Failed to abort outbound stream");
				}
				try {
					c.pushable.return?.();
				} catch {
					logger.error("Failed to close outbound pushable");
				}
				try {
					c.raw.close?.();
				} catch {
					logger.error("Failed to close outbound stream");
				}
			}
			this.outboundStreams = [chosen];
		} catch (e) {
			logger.error(
				"Error promoting outbound candidate: " + (e as any)?.message,
			);
		} finally {
			this.dispatchEvent(new CustomEvent("stream:outbound"));
		}
	}

	public forcePruneOutbound() {
		if (this._outboundPruneTimer) {
			clearTimeout(this._outboundPruneTimer);
			this._outboundPruneTimer = undefined;
		}
		this.pruneOutboundCandidates();
	}

	/**
	 * Internal helper to perform the actual outbound replacement & piping.
	 */
	// _replaceOutboundStream removed (legacy path)

	// Debug/testing helper: list active outbound raw stream ids
	public _debugActiveOutboundIds(): string[] {
		if (this.outboundStreams.length) {
			return this.outboundStreams.map((c) => c.raw.id);
		}
		return this.outboundStreams.map((c) => c.raw.id);
	}

	public _debugOutboundStats(): {
		id: string;
		bytes: number;
		aborted: boolean;
	}[] {
		if (this.outboundStreams.length) {
			return this.outboundStreams.map((c) => ({
				id: c.raw.id,
				bytes: c.bytesDelivered,
				aborted: !!c.aborted,
			}));
		}
		return this.outboundStreams.map((c) => ({
			id: c.raw.id,
			bytes: c.bytesDelivered,
			aborted: !!c.aborted,
		}));
	}

	/**
	 * Closes the open connection to peer
	 */
	async close() {
		if (this.closed) {
			return;
		}

		this.closed = true;
		if (this._outboundPruneTimer) {
			clearTimeout(this._outboundPruneTimer);
			this._outboundPruneTimer = undefined;
		}

		// End the outbound stream
		if (this.outboundStreams.length) {
			for (const c of this.outboundStreams) {
				try {
					await c.pushable.return?.();
				} catch {}
				try {
					c.raw.abort?.(new AbortError("Closed"));
				} catch {}
			}
			this.outboundAbortController.abort();
		}

		// End inbound streams
		if (this.inboundStreams.length) {
			for (const inbound of this.inboundStreams) {
				try {
					inbound.abortController.abort();
				} catch {
					logger.error("Failed to abort inbound stream");
				}
				try {
					await inbound.raw.close?.();
				} catch {
					logger.error("Failed to close inbound stream");
				}
			}
		}

		this.usedBandWidthTracker.stop();

		this.dispatchEvent(new CustomEvent("close"));

		this.outboundStreams = [];

		this.rawInboundStream = undefined;
		this.inboundStream = undefined;
		this.inboundStreams = [];
	}
}

type DialerOptions = {
	retryDelay: number;
};
type PrunerOptions = {
	interval: number; // how often to check for pruning
	bandwidth?: number; // Mbps, unlimited if unset
	maxBuffer?: number; // max queued bytes until pruning
	connectionTimeout: number; // how long a pruned connection should be treated as non wanted
};
type ConnectionManagerOptions = {
	minConnections: number;
	maxConnections: number;
	dialer?: DialerOptions;
	pruner?: PrunerOptions;
};

export type ConnectionManagerArguments =
	| (Partial<Pick<ConnectionManagerOptions, "minConnections">> &
			Partial<Pick<ConnectionManagerOptions, "maxConnections">> & {
				pruner?: Partial<PrunerOptions> | false;
			} & { dialer?: Partial<DialerOptions> | false })
	| false;

export type DirectStreamOptions = {
	canRelayMessage?: boolean;
	messageProcessingConcurrency?: number;
	maxInboundStreams?: number;
	maxOutboundStreams?: number;
	inboundIdleTimeout?: number; // override PeerStreams.INBOUND_IDLE_MS
	connectionManager?: ConnectionManagerArguments;
	seekTimeout?: number;
	routeMaxRetentionPeriod?: number;
	/**
	 * Best-effort bounds for the per-process route cache. These exist to prevent
	 * unbounded memory growth in large networks/simulations.
	 */
	routeCacheMaxFromEntries?: number;
	routeCacheMaxTargetsPerFrom?: number;
	routeCacheMaxRelaysPerTarget?: number;
	/**
	 * Share node-level routing/session state across DirectStream instances created
	 * from the same libp2p private key.
	 *
	 * This reduces duplicated topology knowledge when multiple protocols run on
	 * the same node (e.g. pubsub + fanout overlays).
	 *
	 * Defaults to `true`.
	 */
	sharedRouting?: boolean;
	seenCacheMax?: number;
	seenCacheTtlMs?: number;
};

type ConnectionManagerLike = {
	getConnections(peerId?: PeerId): Connection[];
	getConnectionsMap(): {
		get(peer: PeerId): Connection[] | undefined;
	};
	getDialQueue(): Array<{ peerId?: PeerId }>;
	isDialable(
		multiaddr: Multiaddr | Multiaddr[],
		options?: unknown,
	): Promise<boolean>;
	openConnection(
		peer: PeerId | Multiaddr | Multiaddr[],
		options?: unknown,
	): Promise<Connection>;
	closeConnections(peer: PeerId, options?: AbortOptions): Promise<void>;
};

export interface DirectStreamComponents {
	peerId: PeerId;
	addressManager: AddressManager;
	registrar: Registrar;
	connectionManager: ConnectionManagerLike;
	peerStore: PeerStore;
	events: TypedEventTarget<Libp2pEvents>;
	privateKey: PrivateKey;
}

type SharedRoutingState = {
	session: number;
	routes: Routes;
	controller: AbortController;
	refs: number;
};

const sharedRoutingByPrivateKey = new WeakMap<PrivateKey, SharedRoutingState>();

export type PublishOptions = (WithMode | WithTo) &
	PriorityOptions &
	WithExtraSigners;

export abstract class DirectStream<
			Events extends { [s: string]: any } = StreamEvents,
		>
		extends TypedEventEmitter<Events>
		implements WaitForPeer, PublicKeyFromHashResolver
	{
	public peerId: PeerId;
	public publicKey: PublicSignKey;
	public publicKeyHash: string;
	public sign: (bytes: Uint8Array) => Promise<SignatureWithKey>;

	public started: boolean;
	public stopping: boolean;
	/**
	 * Map of peer streams
	 */
	public peers: Map<string, PeerStreams>;
	public peerKeyHashToPublicKey: Map<string, PublicSignKey>;
	public routes: Routes;
	/**
	 * If router can relay received messages, even if not subscribed
	 */
	public canRelayMessage: boolean;
	/**
	 * if publish should emit to self, if subscribed
	 */

	public queue: Queue;
	public multicodecs: string[];
	public seenCache: Cache<number>;
	private _registrarTopologyIds: string[] | undefined;
	private readonly maxInboundStreams?: number;
	private readonly maxOutboundStreams?: number;
	connectionManagerOptions: ConnectionManagerOptions;
	private recentDials?: Cache<string>;
	private healthChecks: Map<string, ReturnType<typeof setTimeout>>;
		private pruneConnectionsTimeout: ReturnType<typeof setInterval>;
		private prunedConnectionsCache?: Cache<string>;
		private pruneToLimitsInFlight?: Promise<void>;
		private routeMaxRetentionPeriod: number;
	private routeCacheMaxFromEntries?: number;
	private routeCacheMaxTargetsPerFrom?: number;
	private routeCacheMaxRelaysPerTarget?: number;
	private readonly sharedRouting: boolean;
	private sharedRoutingKey?: PrivateKey;
	private sharedRoutingState?: SharedRoutingState;

	// for sequential creation of outbound streams
		public outboundInflightQueue: Pushable<{
			connection: Connection;
			peerId: PeerId;
		}>;

		seekTimeout: number;
		closeController: AbortController;
		session: number;
		_outboundPump: ReturnType<typeof pipe> | undefined;

	private _ackCallbacks: Map<
		string,
		{
			promise: Promise<void>;
			callback: (
				ack: ACK,
				messageThrough: PeerStreams,
				messageFrom?: PeerStreams,
			) => void;
			clear: () => void;
		}
	>;

	constructor(
		readonly components: DirectStreamComponents,
		multicodecs: string[],
		options?: DirectStreamOptions,
	) {
		super();
		const {
			canRelayMessage = true,
			messageProcessingConcurrency = 10,
				maxInboundStreams,
				maxOutboundStreams,
				connectionManager,
					seekTimeout = SEEK_DELIVERY_TIMEOUT,
					routeMaxRetentionPeriod = ROUTE_MAX_RETANTION_PERIOD,
					routeCacheMaxFromEntries,
					routeCacheMaxTargetsPerFrom,
				routeCacheMaxRelaysPerTarget,
				sharedRouting = true,
				seenCacheMax = 1e6,
				seenCacheTtlMs = 10 * 60 * 1e3,
				inboundIdleTimeout,
			} = options || {};

		const signKey = getKeypairFromPrivateKey(components.privateKey);
		this.seekTimeout = seekTimeout;
		this.sign = signKey.sign.bind(signKey);
		this.peerId = components.peerId;
		this.publicKey = signKey.publicKey;
		if (inboundIdleTimeout != null)
			PeerStreams.INBOUND_IDLE_MS = inboundIdleTimeout;
		if (maxInboundStreams != null)
			PeerStreams.MAX_INBOUND_STREAMS = maxInboundStreams;
		this.publicKeyHash = signKey.publicKey.hashcode();
		this.multicodecs = multicodecs;
		this.started = false;
			this.peers = new Map<string, PeerStreams>();
			this.canRelayMessage = canRelayMessage;
			this.healthChecks = new Map();
			this.queue = new Queue({ concurrency: messageProcessingConcurrency });
			this.maxInboundStreams = maxInboundStreams;
			this.maxOutboundStreams = maxOutboundStreams;
			this.seenCache = new Cache({
				max: Math.max(1, Math.floor(seenCacheMax)),
				ttl: Math.max(1, Math.floor(seenCacheTtlMs)),
			});
			this.routeMaxRetentionPeriod = routeMaxRetentionPeriod;
			this.routeCacheMaxFromEntries = routeCacheMaxFromEntries;
			this.routeCacheMaxTargetsPerFrom = routeCacheMaxTargetsPerFrom;
			this.routeCacheMaxRelaysPerTarget = routeCacheMaxRelaysPerTarget;
			this.sharedRouting = sharedRouting !== false;
			this.peerKeyHashToPublicKey = new Map();
			this._onIncomingStream = this._onIncomingStream.bind(this);
			this.onPeerConnected = this.onPeerConnected.bind(this);
			this.onPeerDisconnected = this.onPeerDisconnected.bind(this);

		this._ackCallbacks = new Map();

		if (connectionManager === false || connectionManager === null) {
			this.connectionManagerOptions = {
				maxConnections: Number.MAX_SAFE_INTEGER,
				minConnections: 0,
				dialer: undefined,
				pruner: undefined,
			};
		} else {
			this.connectionManagerOptions = {
				maxConnections: DEFAULT_MAX_CONNECTIONS,
				minConnections: DEFAULT_MIN_CONNECTIONS,
				...connectionManager,
				dialer:
					connectionManager?.dialer !== false &&
					connectionManager?.dialer !== null
						? { retryDelay: 60 * 1000, ...connectionManager?.dialer }
						: undefined,
				pruner:
					connectionManager?.pruner !== false &&
					connectionManager?.pruner !== null
						? {
								connectionTimeout: DEFAULT_PRUNED_CONNNECTIONS_TIMEOUT,
								interval: DEFAULT_PRUNE_CONNECTIONS_INTERVAL,
								maxBuffer: MAX_QUEUED_BYTES,
								...connectionManager?.pruner,
							}
						: undefined,
			};
		}

		this.recentDials = this.connectionManagerOptions.dialer
			? new Cache({
					ttl: this.connectionManagerOptions.dialer.retryDelay,
					max: 1e3,
				})
			: undefined;

			this.prunedConnectionsCache = this.connectionManagerOptions.pruner
				? new Cache({
						max: 1e6,
						ttl: this.connectionManagerOptions.pruner.connectionTimeout,
					})
				: undefined;
		}

		private pruneConnectionsToLimits(): Promise<void> {
			if (this.pruneToLimitsInFlight) {
				return this.pruneToLimitsInFlight;
			}
			this.pruneToLimitsInFlight = (async () => {
				// Respect minConnections as a hard floor.
				const maxConnections = Math.max(
					this.connectionManagerOptions.minConnections,
					this.connectionManagerOptions.maxConnections,
				);
				if (this.peers.size <= maxConnections) {
					return;
				}

				// Prune in batches so we can quickly recover from join storms without waiting
				// for repeated pruner ticks. Bound work per run to avoid starving the event loop.
				const maxPrunes = Math.min(256, Math.max(1, this.peers.size - maxConnections));
				for (let i = 0; i < maxPrunes; i++) {
					if (this.peers.size <= maxConnections) break;
					const before = this.peers.size;
					await this.pruneConnections();
					if (this.peers.size >= before) break; // nothing prunable
				}
			})().finally(() => {
				this.pruneToLimitsInFlight = undefined;
			});
			return this.pruneToLimitsInFlight;
		}

	async start() {
		if (this.started) {
			return;
		}

		await ready;

		this.closeController = new AbortController();

		this.outboundInflightQueue = pushable({ objectMode: true });

		const drainOutbound = async (
			source: AsyncIterable<{ peerId: PeerId; connection: Connection }>,
		) => {
			for await (const { peerId, connection } of source) {
				if (this.stopping || !this.started) break; // do not 'return' – finish loop cleanly

				// Skip closed/closing connections
				if (connection?.timeline?.close != null) {
					logger.trace(
						"skip outbound stream on closed connection %s",
						connection.remoteAddr?.toString(),
					);
					continue;
				}

				try {
					// Pass an abort + timeout into your stream open so it cannot hang forever
					const attemptSignal = anySignal([
						this.closeController.signal,
						AbortSignal.timeout(DEFAULT_CREATE_OUTBOUND_STREAM_TIMEOUT), // pick a sensible per-attempt cap
					]);
					try {
						await this.createOutboundStream(peerId, connection, {
							signal: attemptSignal,
						});
					} finally {
						attemptSignal.clear?.();
					}
				} catch (e: any) {
					// Treat common shutdowny errors as transient – do NOT crash the pump
					const msg = String(e?.message ?? e);
					if (
						e?.code === "ERR_STREAM_RESET" ||
						/unexpected end of input|ECONNRESET|EPIPE|Muxer closed|Premature close/i.test(
							msg,
						)
					) {
						logger.trace(
							"createOutboundStream transient failure (%s): %s",
							connection?.remoteAddr,
							msg,
						);
					} else {
						warn(
							"createOutboundStream failed (%s): %o",
							connection?.remoteAddr,
							e,
						);
					}
					// continue to next item
				}
			}
		};

		this._outboundPump = pipe(this.outboundInflightQueue, drainOutbound).catch(
			(e) => {
				// Only log if we didn't intentionally abort
				if (!this.closeController.signal.aborted) {
					logger.error("outbound inflight pipeline crashed: %o", e);
					// Optional: restart the pump to self-heal
					this._outboundPump = pipe(
						this.outboundInflightQueue,
						drainOutbound,
					).catch((err) =>
						logger.error("outbound pump crashed again: %o", err),
					);
				}
			},
		);

		this.closeController.signal.addEventListener("abort", () => {
			this.outboundInflightQueue.return();
		});

		if (this.sharedRouting) {
			const key = this.components.privateKey;
			this.sharedRoutingKey = key;
			let state = sharedRoutingByPrivateKey.get(key);
			if (!state) {
				const controller = new AbortController();
				state = {
					session: Date.now(),
					controller,
					routes: new Routes(this.publicKeyHash, {
						routeMaxRetentionPeriod: this.routeMaxRetentionPeriod,
						signal: controller.signal,
						maxFromEntries: this.routeCacheMaxFromEntries,
						maxTargetsPerFrom: this.routeCacheMaxTargetsPerFrom,
						maxRelaysPerTarget: this.routeCacheMaxRelaysPerTarget,
					}),
					refs: 0,
				};
				sharedRoutingByPrivateKey.set(key, state);
			} else {
				// Best-effort: prefer the strictest cleanup policy among co-located protocols.
				state.routes.routeMaxRetentionPeriod = Math.min(
					state.routes.routeMaxRetentionPeriod,
					this.routeMaxRetentionPeriod,
				);
			}

			state.refs += 1;
			this.sharedRoutingState = state;
			this.session = state.session;
			this.routes = state.routes;
		} else {
			this.session = Date.now();
			this.routes = new Routes(this.publicKeyHash, {
				routeMaxRetentionPeriod: this.routeMaxRetentionPeriod,
				signal: this.closeController.signal,
				maxFromEntries: this.routeCacheMaxFromEntries,
				maxTargetsPerFrom: this.routeCacheMaxTargetsPerFrom,
				maxRelaysPerTarget: this.routeCacheMaxRelaysPerTarget,
			});
		}

		this.started = true;
		this.stopping = false;
		logger.trace("starting");

		// Incoming streams
		// Called after a peer dials us
		await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.components.registrar.handle(multicodec, this._onIncomingStream, {
					maxInboundStreams: this.maxInboundStreams,
					maxOutboundStreams: this.maxOutboundStreams,
					runOnLimitedConnection: false,
				}),
			),
		);

		// register protocol with topology
		// Topology callbacks called on connection manager changes
		this._registrarTopologyIds = await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.components.registrar.register(multicodec, {
					onConnect: this.onPeerConnected.bind(this),
					onDisconnect: this.onPeerDisconnected.bind(this),
					notifyOnLimitedConnection: false,
				}),
			),
		);

		// All existing connections are like new ones for us. To deduplication on remotes so we only resuse one connection for this protocol (we could be connected with many connections)
		const peerToConnections: Map<string, Connection[]> = new Map();
		const connections = this.components.connectionManager.getConnections();
		for (const conn of connections) {
			let arr = peerToConnections.get(conn.remotePeer.toString());
			if (!arr) {
				arr = [];
				peerToConnections.set(conn.remotePeer.toString(), arr);
			}
			arr.push(conn);
		}
		for (const [_peer, arr] of peerToConnections) {
			let conn = arr[0]; // TODO choose TCP when both websocket and tcp exist
			for (const c of arr) {
				if (!isWebsocketConnection(c)) {
					// TODO what is correct connection prioritization?
					conn = c; // always favor non websocket address
					break;
				}
			}

			await this.onPeerConnected(conn.remotePeer, conn);
		}
		if (this.connectionManagerOptions.pruner) {
			const pruneConnectionsLoop = () => {
				if (!this.connectionManagerOptions.pruner) {
					return;
				}
				this.pruneConnectionsTimeout = setTimeout(() => {
					this.maybePruneConnections().finally(() => {
						if (!this.started) {
							return;
						}
						pruneConnectionsLoop();
					});
				}, this.connectionManagerOptions.pruner.interval);
			};
			pruneConnectionsLoop();
		}
	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return;
		}

		const sharedState = this.sharedRoutingState;
		const sharedKey = this.sharedRoutingKey;

		clearTimeout(this.pruneConnectionsTimeout);

		await Promise.all(
			this.multicodecs.map((x) => this.components.registrar.unhandle(x)),
		);

		// unregister protocol and handlers
		if (this._registrarTopologyIds != null) {
			await Promise.all(
				this._registrarTopologyIds.map((id) =>
					this.components.registrar.unregister(id),
				),
			);
		}

		// reset and clear up
		this.started = false;
		this.outboundInflightQueue.end();
		this.closeController.abort();

		logger.trace("stopping");
		for (const peerStreams of this.peers.values()) {
			await peerStreams.close();
		}

		for (const [_k, v] of this.healthChecks) {
			clearTimeout(v);
		}
		this.healthChecks.clear();
		this.prunedConnectionsCache?.clear();

		this.queue.clear();
		this.peers.clear();
		this.seenCache.clear();
		// When routing is shared across co-located protocols, only clear once the last
		// instance stops. Otherwise we'd wipe routes still in use by other services.
		if (!sharedState) {
			this.routes.clear();
		}
		this.peerKeyHashToPublicKey.clear();

		for (const [_k, v] of this._ackCallbacks) {
			v.clear();
		}

		this._ackCallbacks.clear();
		this.sharedRoutingState = undefined;
		this.sharedRoutingKey = undefined;
		if (sharedState && sharedKey) {
			sharedState.refs = Math.max(0, sharedState.refs - 1);
			if (sharedState.refs === 0) {
				try {
					sharedState.routes.clear();
				} catch {
					// ignore
				}
				try {
					sharedState.controller.abort();
				} catch {
					// ignore
				}
				sharedRoutingByPrivateKey.delete(sharedKey);
			}
		}
		logger.trace("stopped");
		this.stopping = false;
	}

	isStarted() {
		return this.started;
	}

	/**
	 * On an inbound stream opened
	 */

	protected async _onIncomingStream(stream: Stream, connection: Connection) {
		if (!this.isStarted()) {
			return;
		}
		const peerId = connection.remotePeer;
		if (stream.protocol == null) {
			stream.abort(new Error("Stream was not multiplexed"));
			return;
		}

		const publicKey = getPublicKeyFromPeerId(peerId);

		if (this.prunedConnectionsCache?.has(publicKey.hashcode())) {
			await connection.close();
			await this.components.peerStore.delete(peerId);
			return;
		}

		const peer = this.addPeer(
			peerId,
			publicKey,
			stream.protocol,
			connection.id,
		);

		// handle inbound
		const inboundRecord = peer.attachInboundStream(stream);
		this.processMessages(peer.publicKey, inboundRecord, peer).catch(logError);

		// try to create outbound stream
		await this.outboundInflightQueue.push({ peerId, connection });
	}

	protected async createOutboundStream(
		peerId: PeerId,
		connection: Connection,
		opts?: { signal?: AbortSignal },
	) {
		for (const existingStreams of connection.streams) {
			if (
				existingStreams.protocol &&
				this.multicodecs.includes(existingStreams.protocol) &&
				existingStreams.direction === "outbound"
			) {
				return;
			}
		}

		let stream: Stream = undefined as any; // TODO types
		let tries = 0;
		let peer: PeerStreams = undefined as any;
		const peerKey = getPublicKeyFromPeerId(peerId);
		while (tries <= 3) {
			tries++;
			if (!this.started) {
				return;
			}

			try {
				stream = await connection.newStream(this.multicodecs, {
					// TODO this property seems necessary, together with waitFor isReadable when making sure two peers are conencted before talking.
					// research whether we can do without this so we can push data without beeing able to send
					// more info here https://github.com/libp2p/js-libp2p/issues/2321
					negotiateFully: true,
					signal: anySignal([
						this.closeController.signal,
						...(opts?.signal ? [opts.signal] : []),
					]),
				});

				if (stream.protocol == null) {
					stream.abort(new Error("Stream was not multiplexed"));
					return;
				}

				if (!this.started) {
					// we closed before we could create the stream
					stream.abort(new Error("Closed"));
					return;
				}
				peer = this.addPeer(peerId, peerKey, stream.protocol!, connection.id); // TODO types
				await peer.attachOutboundStream(stream);
			} catch (error: any) {
				if (error.code === "ERR_UNSUPPORTED_PROTOCOL") {
					await delay(100);
					continue; // Retry
				}

				if (error instanceof UnsupportedProtocolError) {
					await delay(100);
					continue; // Retry
				}

				if (
					connection.status !== "open" ||
					error?.message === "Muxer already closed" ||
					error.code === "ERR_STREAM_RESET" ||
					error instanceof StreamResetError ||
					error instanceof MuxerClosedError
				) {
					return; // fail silenty
				}

				throw error;
			}
			break;
		}
		if (!stream) {
			return;
		}
	}

	/**
	 * Registrar notifies an established connection with protocol
	 */
	public async onPeerConnected(peerId: PeerId, connection: Connection) {
		if (
			!this.isStarted() ||
			connection.limits ||
			connection.status !== "open"
		) {
			return;
		}
		const peerKey = getPublicKeyFromPeerId(peerId);

		if (this.prunedConnectionsCache?.has(peerKey.hashcode())) {
			await connection.close();
			await this.components.peerStore.delete(peerId);
			return; // we recently pruned this connect, dont allow it to connect for a while
		}

		this.outboundInflightQueue.push({ peerId, connection });
	}

	/**
	 * Registrar notifies a closing connection with pubsub protocol
	 */
	protected async onPeerDisconnected(peerId: PeerId, conn?: Connection) {
		// PeerId could be me, if so, it means that I am disconnecting
		const peerKey = getPublicKeyFromPeerId(peerId);
		const peerKeyHash = peerKey.hashcode();
		const allConnections =
			this.components.connectionManager.getConnections?.() ?? [];
		const connections = allConnections.filter(
			(connection) => connection.remotePeer.toString() === peerId.toString(),
		);
		if (connections.length > 0) {
			const trackedConnection = conn?.id
				? connections.find((x) => x.id === conn.id)
				: null;
			if (!trackedConnection || connections.length > 1) {
				// Another connection is still alive (or we can't match this disconnect to a tracked connection).
				// Avoid removing the peer entirely, since replication may still be active.
				return;
			}
		}

		if (!this.peers.has(peerKeyHash)) {
			// TODO remove when https://github.com/libp2p/js-libp2p/issues/2369 fixed
			// TODO this code should work even if onPeerDisconnected events are emitted wrongfully. i.e. disconnection should occur and rediscover should be smooth?
			return;
		}
		if (!this.publicKey.equals(peerKey)) {
			await this._removePeer(peerKey);

			// tell dependent peers that there is a node that might have left
			const dependent = this.routes.getDependent(peerKeyHash);

			// make neighbour unreachables
			this.removePeerFromRoutes(peerKeyHash, true);

			if (dependent.length > 0) {
				await this.publishMessage(
					this.publicKey,
					await new Goodbye({
						leaving: [peerKeyHash],
						header: new MessageHeader({
							session: this.session,
							mode: new SilentDelivery({ to: dependent, redundancy: 2 }),
						}),
					}).sign(this.sign),
				).catch(dontThrowIfDeliveryError);
			}

			this.checkIsAlive([peerKeyHash]);
		}

		logger.trace("connection ended:" + peerKey.toString());
	}

	public removePeerFromRoutes(hash: string, deleteIfNeighbour = false) {
		if (this.peers.has(hash) && !deleteIfNeighbour) {
			return;
		}

		const unreachable = this.routes.remove(hash);
		for (const node of unreachable) {
			this.onPeerUnreachable(node); // TODO types
			this.peerKeyHashToPublicKey.delete(node);
		}
	}

	public addRouteConnection(
		from: string,
		neighbour: string,
		target: PublicSignKey,
		distance: number,
		session: number,
		remoteSession: number,
	) {
		const targetHash = typeof target === "string" ? target : target.hashcode();

		const update = this.routes.add(
			from,
			neighbour,
			targetHash,
			distance,
			session,
			remoteSession,
		);

		// second condition is that we don't want to emit 'reachable' events for routes where we act only as a relay
		// in this case, from is != this.publicKeyhash
		if (from === this.publicKeyHash) {
			if (update === "new") {
				this.peerKeyHashToPublicKey.set(target.hashcode(), target);
				this.onPeerReachable(target);
			}
		}
	}

	public onPeerReachable(publicKey: PublicSignKey) {
		// override this fn
		this.dispatchEvent(
			new CustomEvent("peer:reachable", { detail: publicKey }),
		);
	}

	public onPeerUnreachable(hash: string) {
		// override this fns

		this.dispatchEvent(
			// TODO types
			new CustomEvent("peer:unreachable", {
				detail: this.peerKeyHashToPublicKey.get(hash)!,
			}),
		);
	}

	public updateSession(key: PublicSignKey, session?: number) {
		if (this.routes.updateSession(key.hashcode(), session)) {
			return this.onPeerSession(key, session!);
		}
	}
	public invalidateSession(key: string) {
		this.routes.updateSession(key, undefined);
	}

	public onPeerSession(key: PublicSignKey, session: number) {
		this.dispatchEvent(
			// TODO types
			new CustomEvent("peer:session", {
				detail: key,
			}),
		);
	}

	/**
	 * Notifies the router that a peer has been connected
	 */
		addPeer(
			peerId: PeerId,
			publicKey: PublicSignKey,
			protocol: string,
			connId: string,
	): PeerStreams {
		const publicKeyHash = publicKey.hashcode();

		this.clearHealthcheckTimer(publicKeyHash);

		const existing = this.peers.get(publicKeyHash);

		// If peer streams already exists, do nothing
		if (existing != null) {
			existing.connId = connId;
			return existing;
		}

		// else create a new peer streams
		const peerIdStr = peerId.toString();
		logger.trace("new peer" + peerIdStr);

		const peerStreams: PeerStreams = new PeerStreams({
			peerId,
			publicKey,
			protocol,
			connId,
		});

		this.peers.set(publicKeyHash, peerStreams);
		this.updateSession(publicKey, -1);

		// Propagate per-peer stream readiness events to the parent emitter
		const forwardOutbound = () =>
			this.dispatchEvent(new CustomEvent("stream:outbound"));
		const forwardInbound = () =>
			this.dispatchEvent(new CustomEvent("stream:inbound"));
		peerStreams.addEventListener("stream:outbound", forwardOutbound);
		peerStreams.addEventListener("stream:inbound", forwardInbound);

		peerStreams.addEventListener("close", () => this._removePeer(publicKey), {
			once: true,
		});
		peerStreams.addEventListener(
			"close",
			() => {
				peerStreams.removeEventListener("stream:outbound", forwardOutbound);
				peerStreams.removeEventListener("stream:inbound", forwardInbound);
			},
			{ once: true },
		);

			this.addRouteConnection(
				this.publicKeyHash,
				publicKey.hashcode(),
				publicKey,
				-1,
				+new Date(),
				-1,
			);

			// Enforce connection manager limits eagerly when new peers are added. Without this,
			// join storms can create large temporary peer sets and OOM in single-process sims.
			if (this.peers.size > this.connectionManagerOptions.maxConnections) {
				void this.pruneConnectionsToLimits().catch(() => {});
			}

			return peerStreams;
		}

	/**
	 * Notifies the router that a peer has been disconnected
	 */
	protected async _removePeer(publicKey: PublicSignKey) {
		const hash = publicKey.hashcode();
		const peerStreams = this.peers.get(hash);
		this.clearHealthcheckTimer(hash);

		if (peerStreams == null) {
			return;
		}

		// close peer streams
		await peerStreams.close();

		// delete peer streams
		logger.trace("delete peer" + publicKey.toString());
		this.peers.delete(hash);
		return peerStreams;
	}

	// MESSAGE METHODS

	/**
	 * Responsible for processing each RPC message received by other peers.
	 */
	async processMessages(
		peerId: PublicSignKey,
		record: InboundStreamRecord,
		peerStreams: PeerStreams,
	) {
		try {
			for await (const data of record.iterable) {
				const now = Date.now();
				record.lastActivity = now;
				record.bytesReceived += data.length || data.byteLength || 0;

				this.processRpc(peerId, peerStreams, data).catch((e) => logError(e));
			}
		} catch (err: any) {
			if (err?.code === "ERR_STREAM_RESET") {
				// only send stream reset messages to info
				logger(
					"Failed processing messages to id: " +
						peerStreams.peerId.toString() +
						". " +
						err?.message,
				);
			} else {
				warn(
					"Failed processing messages to id: " +
						peerStreams.peerId.toString() +
						". " +
						err?.message,
				);
			}
			this.onPeerDisconnected(peerStreams.peerId);
		}
	}

	/**
	 * Handles an rpc request from a peer
	 */
	async processRpc(
		from: PublicSignKey,
		peerStreams: PeerStreams,
		message: Uint8ArrayList,
	): Promise<boolean> {
		// logger.trace("rpc from " + from + ", " + this.peerIdStr);

		if (message.length > 0) {
			//	logger.trace("messages from " + from);
			await this.queue
				.add(async () => {
					try {
						await this.processMessage(from, peerStreams, message);
					} catch (err: any) {
						logger.error(err);
					}
				})
				.catch(logError);
		}

		return true;
	}

	private async modifySeenCache(
		message: Uint8Array,
		getIdFn: (bytes: Uint8Array) => Promise<string> = getMsgId,
	) {
		const msgId = await getIdFn(message);
		const seen = this.seenCache.get(msgId);
		this.seenCache.add(msgId, seen ? seen + 1 : 1);
		return seen || 0;
	}

	/**
	 * Handles a message from a peer
	 */
	async processMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		msg: Uint8ArrayList,
	) {
		if (!this.started) {
			return;
		}

		// Ensure the message is valid before processing it
		let message: Message | undefined;
		try {
			message = Message.from(msg);
		} catch (error) {
			warn(error, "Failed to decode message frame from", from.hashcode());
			return;
		}
		if (!message) {
			logger.trace("Ignoring empty message frame from", from.hashcode());
			return;
		}
		this.dispatchEvent(
			new CustomEvent("message", {
				detail: message,
			}),
		);

		if (message instanceof DataMessage) {
			// DONT await this since it might introduce a dead-lock
			this._onDataMessage(from, peerStream, msg, message).catch(logError);
		} else {
			if (message instanceof ACK) {
				this.onAck(from, peerStream, msg, message).catch(logError);
			} else if (message instanceof Goodbye) {
				this.onGoodBye(from, peerStream, msg, message).catch(logError);
			} else {
				throw new Error("Unsupported message type");
			}
		}
	}

	public shouldIgnore(message: DataMessage, seenBefore: number) {
		const signedBySelf =
			message.header.signatures?.publicKeys.some((x) =>
				x.equals(this.publicKey),
			) ?? false;

		if (signedBySelf) {
			return true;
		}

		// For acknowledged modes, allow limited duplicate forwarding so that we can
		// discover and maintain multiple candidate routes (distance=seenCounter).
		if (
			message.header.mode instanceof AcknowledgeDelivery ||
			message.header.mode instanceof AcknowledgeAnyWhere
		) {
			return seenBefore >= message.header.mode.redundancy;
		}

		return seenBefore > 0;
	}

	public async onDataMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number,
	) {
		if (this.shouldIgnore(message, seenBefore)) {
			return false;
		}

		let isForMe = false;
		if (
			message.header.mode instanceof AnyWhere ||
			message.header.mode instanceof AcknowledgeAnyWhere
		) {
			isForMe = true;
		} else {
			const isFromSelf = this.publicKey.equals(from);
			if (
				!isFromSelf &&
				(message.header.mode instanceof SilentDelivery ||
					message.header.mode instanceof AcknowledgeDelivery)
			) {
				isForMe = message.header.mode.to.includes(this.publicKeyHash);
			}
		}

		if (isForMe) {
			if (!(await this.verifyAndProcess(message))) {
				// we don't verify messages we don't dispatch because of the performance penalty // TODO add opts for this
				warn("Recieved message with invalid signature or timestamp");
				return false;
			}

			await this.maybeAcknowledgeMessage(peerStream, message, seenBefore);

			if (seenBefore === 0 && message.data) {
				this.dispatchEvent(
					new CustomEvent("data", {
						detail: message,
					}),
				);
			}
		}

		if (
			message.header.mode instanceof SilentDelivery ||
			message.header.mode instanceof AcknowledgeDelivery
		) {
			if (
				message.header.mode.to &&
				message.header.mode.to.length === 1 &&
				message.header.mode.to[0] === this.publicKeyHash
			) {
				// dont forward this message anymore because it was meant ONLY for me
				return true;
			}
		}

			// Forward
			const shouldForward =
				seenBefore === 0 ||
				((message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof AcknowledgeAnyWhere) &&
					seenBefore < message.header.mode.redundancy);

			if (shouldForward) {
				// DONT await this since it might introduce a dead-lock
				this.relayMessage(from, message);
			}
	}

	public async verifyAndProcess(message: Message<any>) {
		const verified = await message.verify(true);
		if (!verified) {
			return false;
		}

		const from = message.header.signatures!.publicKeys[0];
		if (this.peers.has(from.hashcode())) {
			// do nothing
		} else {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}

	async maybeAcknowledgeMessage(
		peerStream: PeerStreams,
		message: DataMessage | Goodbye,
		seenBefore: number,
	) {
		if (
			message.header.mode instanceof AcknowledgeDelivery ||
			message.header.mode instanceof AcknowledgeAnyWhere
		) {
			const isRecipient =
				message.header.mode instanceof AcknowledgeAnyWhere
					? true
					: message.header.mode.to.includes(this.publicKeyHash);
			if (
				!shouldAcknowledgeDataMessage({
					isRecipient,
					seenBefore,
					redundancy: message.header.mode.redundancy,
				})
			) {
				return;
			}
			const signers = message.header.signatures!.publicKeys.map((x) =>
				x.hashcode(),
			);

			await this.publishMessage(
				this.publicKey,
				await new ACK({
					messageIdToAcknowledge: message.id,
					seenCounter: seenBefore,
					// TODO only give origin info to peers we want to connect to us
					header: new MessageHeader({
						mode: new TracedDelivery(signers),
						session: this.session,

							// include our origin for route-learning/dialer hints (best-effort privacy/anti-spam control):
							// only include once (seenBefore=0) and only if we have not recently pruned
							// a connection to any signer in the path
							origin:
								(message.header.mode instanceof AcknowledgeAnyWhere ||
									message.header.mode instanceof AcknowledgeDelivery) &&
								seenBefore === 0 &&
								!message.header.signatures!.publicKeys.find((x) =>
									this.prunedConnectionsCache?.has(x.hashcode()),
								)
									? new MultiAddrinfo(
											this.components.addressManager
											.getAddresses()
											.map((x) => x.toString()),
									)
								: undefined,
					}),
				}).sign(this.sign),
				[peerStream],
			).catch(dontThrowIfDeliveryError);
		}
	}

	private async _onDataMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: DataMessage,
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8ArrayList
				? messageBytes.subarray()
				: messageBytes,
		);

		return this.onDataMessage(from, peerStream, message, seenBefore);
	}

	async onAck(
		publicKey: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: ACK,
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8Array
				? messageBytes
				: messageBytes.subarray(),
			(bytes) => sha256Base64(bytes),
		);

		if (seenBefore > 0) {
			logger.trace(
				"Received message already seen of type: " + message.constructor.name,
			);
			return false;
		}

		if (!(await this.verifyAndProcess(message))) {
			warn(`Recieved ACK message that did not verify`);
			return false;
		}

		const messageIdString = toBase64(message.messageIdToAcknowledge);
		const myIndex = message.header.mode.trace.findIndex(
			(x) => x === this.publicKeyHash,
		);
		const next = message.header.mode.trace[myIndex - 1];
		const nextStream = next ? this.peers.get(next) : undefined;

		this._ackCallbacks
			.get(messageIdString)
			?.callback(message, peerStream, nextStream);

		// relay ACK ?
		// send exactly backwards same route we got this message
		if (nextStream) {
			await this.publishMessage(
				this.publicKey,
				message,
				[nextStream],
				true,
			).catch(dontThrowIfDeliveryError);
		} else {
			if (myIndex !== 0) {
				// TODO should we throw something, or log?
				// we could arrive here if the ACK target does not exist any more...
				return;
			}
			// if origin exist (we can connect to remote peer) && we have autodialer turned on
			if (message.header.origin && this.connectionManagerOptions.dialer) {
				this.maybeConnectDirectly(
					message.header.signatures!.publicKeys[0].hashcode(),
					message.header.origin,
				);
			}
		}
	}

	async onGoodBye(
		publicKey: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: Goodbye,
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8Array
				? messageBytes
				: messageBytes.subarray(),
		);

		if (seenBefore > 0) {
			logger.trace(
				"Received message already seen of type: " + message.constructor.name,
			);
			return;
		}

		if (!(await this.verifyAndProcess(message))) {
			warn(`Recieved ACK message that did not verify`);
			return false;
		}

		await this.maybeAcknowledgeMessage(peerStream, message, seenBefore);

		const filteredLeaving = message.leaving.filter((x) =>
			this.routes.hasTarget(x),
		);

		// Forward to all dependent
		for (const target of message.leaving) {
			// relay message to every one who previously talked to this peer
			const dependent = this.routes.getDependent(target);

			let newTo = [...message.header.mode.to, ...dependent];
			newTo = newTo.filter((x) => x !== this.publicKeyHash);
			message.header.mode = new SilentDelivery({ to: newTo, redundancy: 1 });

			if (message.header.mode.to.length > 0) {
				await this.publishMessage(publicKey, message, undefined, true).catch(
					dontThrowIfDeliveryError,
				);
			}
		}

		await this.maybeDeleteRemoteRoutes(filteredLeaving);
	}

	private maybeDeleteRemoteRoutes(remotes: string[]) {
		// Handle deletion (if message is sign by the peer who left)
		// or invalidation followed up with an attempt to re-establish a connection
		for (const remote of remotes) {
			this.invalidateSession(remote);
		}
		this.checkIsAlive(remotes);
	}
	private async checkIsAlive(remotes: string[]) {
		if (this.peers.size === 0) {
			return false;
		}
		if (remotes.length > 0) {
			return this.publish(undefined, {
				mode: new AcknowledgeDelivery({
					to: remotes,
					redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY,
				}),
			})
				.then(() => true)
				.catch((e) => {
					if (e instanceof DeliveryError) {
						return false;
					} else if (e instanceof NotStartedError) {
						return false;
					} else if (e instanceof TimeoutError) {
						return false;
					} else if (e instanceof AbortError) {
						return false;
					} else {
						throw e;
					}
				}); // this will remove the target if it is still not reable
		}
		return false;
	}

	async createMessage(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: (WithTo | WithMode) &
			PriorityOptions &
			IdOptions & { skipRecipientValidation?: boolean } & WithExtraSigners,
	) {
		// dispatch the event if we are interested

		let mode:
			| SilentDelivery
			| AcknowledgeDelivery
			| AcknowledgeAnyWhere
			| AnyWhere = (
			options as WithMode
		).mode
			? (options as WithMode).mode!
			: new SilentDelivery({
					to: (options as WithTo).to!,
					redundancy: DEFAULT_SILENT_MESSAGE_REDUDANCY,
				});

		if (
			mode instanceof AcknowledgeDelivery ||
			mode instanceof SilentDelivery
		) {
			if (mode.to) {
				let preLength = mode.to.length;
				mode.to = mode.to.filter((x) => x !== this.publicKeyHash);
				if (!options.skipRecipientValidation) {
					if (preLength > 0 && mode.to?.length === 0) {
						throw new InvalidMessageError(
							"Unexpected to create a message with self as the only receiver",
						);
					}

					if (mode.to.length === 0) {
						throw new InvalidMessageError(
							"Unexpected to deliver message with mode: " +
								mode.constructor.name +
								" without recipents",
						);
					}
				}
			}
		}

		const message = new DataMessage({
			data: data instanceof Uint8ArrayList ? data.subarray() : data,
			header: new MessageHeader({
				id: options.id,
				mode,
				session: this.session,
				priority: options.priority,
			}),
		});

		// TODO allow messages to also be sent unsigned (signaturePolicy property)

		await message.sign(this.sign);
		if (options.extraSigners) {
			for (const signer of options.extraSigners) {
				await message.sign(signer);
			}
		}
		return message;
	}
	/**
	 * Publishes messages to all peers
	 */
	async publish(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: PublishOptions = {
			mode: new AcknowledgeAnyWhere({
				redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY,
			}),
		},
	): Promise<Uint8Array | undefined> {
		if (!this.started) {
			throw new NotStartedError();
		}

		const message = await this.createMessage(data, options);
		const withTo = (options as WithTo).to;
		const withMode = (options as WithMode).mode;

		let to: PeerStreams[] | undefined = undefined;
		if (withTo && withMode) {
			// a special case where we want to pick neighbours to send to aswell as delivery recipents
			to = [];
			for (const peer of withTo) {
				const toHash =
					typeof peer === "string"
						? peer
						: peer instanceof PublicSignKey
							? peer.hashcode()
							: getPublicKeyFromPeerId(peer).hashcode();
				const stream = this.peers.get(toHash);
				if (stream) {
					to.push(stream);
				} else {
					warn(`Peer ${peer} not found in peers, skipping neighbor selection`);
					to = undefined;
					break;
				}
			}
		}
		await this.publishMessage(this.publicKey, message, to);
		return message.id;
	}

	public async relayMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | Map<string, PeerStreams>,
	) {
		if (this.canRelayMessage) {
			if (message instanceof DataMessage) {
				if (
					message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof AcknowledgeAnyWhere
				) {
					await message.sign(this.sign);
				}
			}
			if (deliveryModeHasReceiver(message.header.mode)) {
				message.header.mode.to = message.header.mode.to.filter(
					(x) => x !== this.publicKeyHash,
				);
				if (message.header.mode.to.length === 0) {
					return; // non to send to
				}
			}

			return this.publishMessage(from, message, to, true).catch(
				dontThrowIfDeliveryError,
			);
		} else {
			logger.trace("Received a message to relay but canRelayMessage is false");
		}
	}

	private clearHealthcheckTimer(to: string) {
		const timer = this.healthChecks.get(to);
		clearTimeout(timer);
		this.healthChecks.delete(to);
	}

	private async createDeliveryPromise(
		from: PublicSignKey,
		message: DataMessage | Goodbye,
		relayed?: boolean,
		signal?: AbortSignal,
	): Promise<{ promise: Promise<void> }> {
		if (message.header.mode instanceof AnyWhere) {
			return {
				promise: Promise.resolve(),
			};
		}

		const idString = toBase64(message.id);

		const existing = this._ackCallbacks.get(idString);
		if (existing) {
			return {
				promise: existing.promise,
			};
		}

		const fastestNodesReached = new Map<string, number[]>();
		const messageToSet: Set<string> = new Set();
		if (deliveryModeHasReceiver(message.header.mode)) {
			for (const to of message.header.mode.to) {
				if (to === from.hashcode()) continue;
				messageToSet.add(to);

				if (!relayed && !this.healthChecks.has(to)) {
					this.healthChecks.set(
						to,
						setTimeout(() => {
							this.removePeerFromRoutes(to);
						}, this.seekTimeout),
					);
				}
			}
		}
		const haveReceivers = messageToSet.size > 0;

		if (haveReceivers && this.peers.size === 0) {
			return {
				promise: Promise.reject(
					new DeliveryError(
						"Cannnot deliver message to peers because there are no peers to deliver to",
					),
				),
			};
		}

		const deliveryDeferredPromise = pDefer<void>();

		if (!haveReceivers) {
			deliveryDeferredPromise.resolve(); // we dont know how many answer to expect, just resolve immediately
		}

		const willGetAllAcknowledgements = !relayed; // Only the origin will get all acks

		// Expected to receive at least 'filterMessageForSeenCounter' acknowledgements from each peer
		const filterMessageForSeenCounter = relayed ? undefined : 1;

		const uniqueAcks = new Set();
		const session = +new Date();

		const onUnreachable =
			!relayed &&
			((ev: any) => {
				const deletedReceiver = messageToSet.delete(ev.detail.hashcode());
				if (deletedReceiver) {
					// Only reject if we are the sender
					clear();
					deliveryDeferredPromise.reject(
						new DeliveryError(
							`At least one recipent became unreachable while delivering messsage of type ${message.constructor.name}} to ${ev.detail.hashcode()}`,
						),
					);
				}
			});

		onUnreachable && this.addEventListener("peer:unreachable", onUnreachable);

		let onAbort: (() => void) | undefined;
		const clear = () => {
			timeout && clearTimeout(timeout);
			onUnreachable &&
				this.removeEventListener("peer:unreachable", onUnreachable);
			this._ackCallbacks.delete(idString);
			onAbort && signal?.removeEventListener("abort", onAbort);
		};

			const timeout = setTimeout(async () => {
				clear();

			let hasAll = true;

			// peer not reachable (?)!
			for (const to of messageToSet) {
				let foundNode = false;

				if (fastestNodesReached.has(to)) {
					foundNode = true;
					break;
				}

				if (!foundNode && !relayed) {
					hasAll = false;
				}
			}

			if (!hasAll && willGetAllAcknowledgements) {
				deliveryDeferredPromise.reject(
					new DeliveryError(
						`Failed to get message ${idString} ${filterMessageForSeenCounter} ${[
							...messageToSet,
						]} delivery acknowledges from all nodes (${
							fastestNodesReached.size
						}/${messageToSet.size}). Mode: ${
							message.header.mode.constructor.name
						}. Redundancy: ${(message.header.mode as any)["redundancy"]}`,
					),
				);
			} else {
				deliveryDeferredPromise.resolve();
			}
			}, this.seekTimeout);

		if (signal) {
			onAbort = () => {
				clear();
				deliveryDeferredPromise.reject(new AbortError("Aborted"));
			};
			if (signal.aborted) {
				onAbort();
			} else {
				signal.addEventListener("abort", onAbort, { once: true });
			}
		}

			const checkDone = () => {
				// This if clause should never enter for relayed connections, since we don't
				// know how many ACKs we will get
				if (
					filterMessageForSeenCounter != null &&
					uniqueAcks.size >= messageToSet.size * filterMessageForSeenCounter
				) {
					const shouldKeepCallbackForRouteLearning =
						!relayed &&
						message.header.mode instanceof AcknowledgeDelivery &&
						message.header.mode.redundancy > 1;
					if (haveReceivers && !shouldKeepCallbackForRouteLearning) {
						// If we have an explicit recipient list we can clear the ACK callback once we
						// got the expected acknowledgements.
						clear();
					} else if (haveReceivers && shouldKeepCallbackForRouteLearning) {
						// Resolve delivery early, but keep ACK callbacks alive until timeout so we can
						// learn additional redundant routes (seenCounter=1..redundancy-1).
						onUnreachable &&
							this.removeEventListener("peer:unreachable", onUnreachable);
						onAbort && signal?.removeEventListener("abort", onAbort);
						onAbort = undefined;
					}

					deliveryDeferredPromise.resolve();
					return true;
				}
			return false;
		};

		this._ackCallbacks.set(idString, {
			promise: deliveryDeferredPromise.promise,
			callback: (ack: ACK, messageThrough, messageFrom) => {
				const messageTarget = ack.header.signatures!.publicKeys[0];
				const messageTargetHash = messageTarget.hashcode();
				const seenCounter = ack.seenCounter;

				// remove the automatic removal of route timeout since we have observed lifesigns of a peer
				this.clearHealthcheckTimer(messageTargetHash);

				// If the target is not inside the original message `to`, we can still add the target to our routes.
				// This matters because relays may modify `to`, and because "ack-anywhere" style probes intentionally
				// do not provide an explicit recipient list.
				if (
					message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof AcknowledgeAnyWhere
				) {
					const upstreamHash = messageFrom?.publicKey.hashcode();

					const routeUpdate = computeSeekAckRouteUpdate({
						current: this.publicKeyHash,
						upstream: upstreamHash,
						downstream: messageThrough.publicKey.hashcode(),
						target: messageTargetHash,
						// Route "distance" is based on recipient-seen order (0 = fastest). This is relied upon by
						// `Routes.getFanout(...)` which uses `distance < redundancy` to select redundant next-hops.
						distance: seenCounter,
					});

					this.addRouteConnection(
						routeUpdate.from,
						routeUpdate.neighbour,
						messageTarget,
						routeUpdate.distance,
						session,
						Number(ack.header.session),
					);
				}

				if (messageToSet.has(messageTargetHash)) {
					// Only keep track of relevant acks
					if (
						filterMessageForSeenCounter == null ||
						seenCounter < filterMessageForSeenCounter
					) {
						// TODO set limit correctly
						if (seenCounter < MAX_ROUTE_DISTANCE) {
							let arr = fastestNodesReached.get(messageTargetHash);
							if (!arr) {
								arr = [];
								fastestNodesReached.set(messageTargetHash, arr);
							}
							arr.push(seenCounter);

							uniqueAcks.add(messageTargetHash + seenCounter);
						}
					}
				}

				checkDone();
			},
			clear: () => {
				clear();

				deliveryDeferredPromise.resolve();
			},
		});
		return deliveryDeferredPromise;
	}

	public async publishMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | Map<string, PeerStreams>,
		relayed?: boolean,
		signal?: AbortSignal,
	): Promise<void> {
		if (this.stopping || !this.started) {
			throw new NotStartedError();
		}

		const isRelayed = relayed ?? from.hashcode() !== this.publicKeyHash;
		let delivereyPromise: Promise<void> | undefined = undefined as any;

		if (
			(!message.header.signatures ||
				message.header.signatures.publicKeys.length === 0) &&
			message instanceof DataMessage &&
			message.header.mode instanceof SilentDelivery === false
		) {
			throw new Error("Missing signature");
		}

		/**
		 * Logic for handling acknowledge messages when we receive them (later)
		 */

		if (
			(message instanceof DataMessage || message instanceof Goodbye) &&
				(message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof AcknowledgeAnyWhere)
			) {
				const deliveryDeferredPromise = await this.createDeliveryPromise(
					from,
					message,
					isRelayed,
					signal,
				);
				delivereyPromise = deliveryDeferredPromise.promise;
			}

			const bytes = message.bytes();

			if (!isRelayed) {
				const bytesArray = bytes instanceof Uint8Array ? bytes : bytes.subarray();
				await this.modifySeenCache(bytesArray);
			}

		/**
		 * For non SEEKing message delivery modes, use routing
		 */

		if (message instanceof DataMessage) {
			if (
				(message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof SilentDelivery) &&
				!to
			) {
				if (message.header.mode.to.length === 0) {
					return delivereyPromise; // we defintely know that we should not forward the message anywhere
				}

					const fanout = this.routes.getFanout(
						from.hashcode(),
						message.header.mode.to,
						message.header.mode.redundancy,
					);

					// If we have explicit routing information, send only along the chosen next-hops.
					// If `fanout` is empty (no route info yet), fall through to the flooding logic below
					// so acknowledged deliveries can discover/repair routes instead of timing out.
					if (fanout && fanout.size > 0) {
						const promises: Promise<any>[] = [];
						const usedNeighbours = new Set<string>();
						const originalTo = message.header.mode.to;
						for (const [neighbour, _distantPeers] of fanout) {
							const stream = this.peers.get(neighbour);
							if (!stream) continue;
							if (message.header.mode instanceof SilentDelivery) {
								message.header.mode.to = [..._distantPeers.keys()];
								promises.push(
									stream.waitForWrite(message.bytes(), message.header.priority),
								);
							} else {
								promises.push(stream.waitForWrite(bytes, message.header.priority));
							}
							usedNeighbours.add(neighbour);
						}
						if (message.header.mode instanceof SilentDelivery) {
							message.header.mode.to = originalTo;
						}

						// If the sender requested redundancy but we don't yet have enough distinct
						// next-hops for the target(s), opportunistically probe additional neighbours.
						// This replaces the previous "greedy fanout" probing behavior without needing
						// a separate delivery mode.
							if (
								!isRelayed &&
								message.header.mode instanceof AcknowledgeDelivery &&
								usedNeighbours.size < message.header.mode.redundancy
							) {
								for (const [neighbour, stream] of this.peers) {
									if (usedNeighbours.size >= message.header.mode.redundancy) {
									break;
								}
								if (usedNeighbours.has(neighbour)) continue;
								usedNeighbours.add(neighbour);
								promises.push(stream.waitForWrite(bytes, message.header.priority));
							}
						}

						await Promise.all(promises);
						return delivereyPromise; // we are done sending the message in all direction with updates 'to' lists
					}

						// If we don't have routing information:
						// - For acknowledged deliveries, fall through to flooding (route discovery / repair).
						// - For silent deliveries, relays should not flood (prevents unnecessary fanout); origin may still flood.
						//   We still allow direct neighbour delivery to explicit recipients (if connected).
						if (isRelayed && message.header.mode instanceof SilentDelivery) {
							const promises: Promise<any>[] = [];
							const originalTo = message.header.mode.to;
							for (const recipient of originalTo) {
								if (recipient === this.publicKeyHash) continue;
								if (recipient === from.hashcode()) continue; // never send back to previous hop
							const stream = this.peers.get(recipient);
							if (!stream) continue;
							if (
								message.header.signatures?.publicKeys.find(
									(x) => x.hashcode() === recipient,
								)
							) {
								continue; // recipient already signed/seen this message
							}
							message.header.mode.to = [recipient];
							promises.push(
								stream.waitForWrite(message.bytes(), message.header.priority),
							);
						}
						message.header.mode.to = originalTo;
						if (promises.length > 0) {
							await Promise.all(promises);
						}
						return delivereyPromise;
					}
				} // else send to all (fallthrough to code below)
			}

		// We fils to send the message directly, instead fallback to floodsub
		const peers: PeerStreams[] | Map<string, PeerStreams> = to || this.peers;
		if (
			peers == null ||
			(Array.isArray(peers) && peers.length === 0) ||
			(peers instanceof Map && peers.size === 0)
		) {
			logger.trace("No peers to send to");
			return delivereyPromise;
		}

		let sentOnce = false;
		const promises: Promise<any>[] = [];
		for (const stream of peers.values()) {
			const id = stream as PeerStreams;

			// Dont sent back to the sender
			if (id.publicKey.equals(from)) {
				continue;
			}
			// Dont send message back to any of the signers (they have already seen the message)
			if (
				message.header.signatures?.publicKeys.find((x) =>
					x.equals(id.publicKey),
				)
			) {
				continue;
			}

			sentOnce = true;
			promises.push(id.waitForWrite(bytes, message.header.priority));
		}
		await Promise.all(promises);

			if (!sentOnce) {
				// If the caller provided an explicit peer list, treat "no valid receivers" as an error
				// even when forwarding. This catches programming mistakes early and matches test expectations.
				if (!isRelayed || to != null) {
					throw new DeliveryError("Message did not have any valid receivers");
				}
			}
			return delivereyPromise;
		}

	async maybeConnectDirectly(toHash: string, origin: MultiAddrinfo) {
		if (this.peers.has(toHash) || this.prunedConnectionsCache?.has(toHash)) {
			return; // TODO, is this expected, or are we to dial more addresses?
		}

		const addresses = origin.multiaddrs
			.filter((x) => {
				const ret = !this.recentDials!.has(x);
				this.recentDials!.add(x);
				return ret;
			})
			.map((x) => multiaddr(x));
		if (addresses.length > 0) {
			try {
				// sort addresses so circuit addresses are last, because we want to prefer direct connections

				let sortedAddresses = addresses.sort((a, b) => {
					const aIsCircuit = Circuit.matches(a);
					const bIsCircuit = Circuit.matches(b);
					if (aIsCircuit && !bIsCircuit) {
						return 1;
					}
					if (!aIsCircuit && bIsCircuit) {
						return -1;
					}
					return 0;
				});
				for (const address of sortedAddresses) {
					if (await this.components.connectionManager.isDialable(address)) {
						await this.components.connectionManager.openConnection(address);
						break;
					}
				}
			} catch (error: any) {
				logger(
					"Failed to connect directly to: " +
						JSON.stringify(addresses.map((x) => x.toString())) +
						". " +
						error?.message,
				);
			}
		}
	}

	public async waitFor(
		peers: PeerRefs,
		opts: WaitForPresentOpts,
	): Promise<string[]>;
	public async waitFor(
		peers: PeerRefs,
		opts?: WaitForAnyOpts,
	): Promise<string[]>;
	public async waitFor(
		peerOrPeers: PeerRefs,
		opts: WaitForPresentOpts | WaitForAnyOpts = {},
	): Promise<string[]> {
		const {
			settle = "all",
			timeout,
			signal,
			allowSelf = false,
		} = opts as WaitForBaseOpts;

		const seek: "present" | "any" =
			(opts as WaitForPresentOpts).seek === "present" ? "present" : "any";
		type Target = "neighbor" | "reachable";
		const target =
			seek === "present"
				? "neighbor"
				: ((opts as WaitForAnyOpts).target ?? "reachable");

		const isInDialQueue = (h: string) =>
			this.components.connectionManager
				.getDialQueue()
				.some(
					(x) => x.peerId && getPublicKeyFromPeerId(x.peerId).hashcode() === h,
				);

		const hasConnection = (h: string) =>
			this.components.connectionManager
				.getConnections()
				.some((x) => getPublicKeyFromPeerId(x.remotePeer).hashcode() === h);

		const isPresent = (h: string) => isInDialQueue(h) || hasConnection(h);

		const isReachable = (h: string) =>
			this.routes.isReachable(this.publicKeyHash, h, 0);

		const isNeighbor = (h: string) => {
			const s = this.peers.get(h);
			return !!s && s.isReadable && s.isWritable;
		};

		const eventsFor = (t: Target) =>
			t === "neighbor"
				? ["peer:reachable", "stream:outbound", "stream:inbound"]
				: ["peer:reachable"];

		const reached = (h: string, t: Target) =>
			t === "neighbor" ? isNeighbor(h) : isReachable(h);

		let hashes = coercePeerRefsToHashes(peerOrPeers);

		if (!allowSelf) {
			// filter out self
			hashes = hashes.filter((h) => h !== this.publicKeyHash);
		} else {
			throw new Error("Unallowed to wait for self");
		}

		// Admission snapshot
		const admitted: string[] =
			seek === "present" ? hashes.filter(isPresent) : hashes.slice();

		if (admitted.length === 0) return [];

		// Seed successes
		const wins = new Set<string>();
		for (const h of admitted) if (reached(h, target)) wins.add(h);

		// Preserve input order in the returned list (important for deterministic callers/tests).
		const orderedWins = () => admitted.filter((h) => wins.has(h));

		if (settle === "any" && wins.size > 0) return orderedWins();
		if (settle === "all" && wins.size === admitted.length) return orderedWins();

		// Abort/timeout
		const abortSignals = [this.closeController.signal];
		if (signal) {
			abortSignals.push(signal);
		}

		const check = (defer: DeferredPromise<void>) => {
			for (const h of admitted)
				if (!wins.has(h) && reached(h, target)) wins.add(h);
			if (settle === "any" && wins.size > 0) return defer.resolve();
			if (settle === "all" && wins.size === admitted.length)
				return defer.resolve();
		};

			try {
				await waitForEvent(this, eventsFor(target), check, {
					signals: abortSignals,
					timeout,
				});
				return orderedWins();
			} catch (e) {
				const abortSignal = abortSignals.find((s) => s.aborted);
				if (abortSignal) {
					if (abortSignal.reason instanceof Error) {
						throw abortSignal.reason;
					}
					throw new AbortError(
						"Aborted waiting for peers: " + abortSignal.reason,
					);
				}
				if (e instanceof TimeoutError) {
					if (settle === "any") {
						if (wins.size > 0) return orderedWins();
						throw new TimeoutError(
							`Timeout waiting for peers (target=${target}, seek=${seek}, missing=${admitted.length}/${admitted.length})`,
						);
					}
					const missing = admitted.filter((h) => !wins.has(h));
					const preview = missing.slice(0, 5).join(", ");
					throw new TimeoutError(
						`Timeout waiting for peers (target=${target}, seek=${seek}, missing=${missing.length}/${admitted.length}${
							preview ? `, e.g. ${preview}` : ""
						})`,
					);
				}
				if (e instanceof Error) throw e;
				if (settle === "all") throw new TimeoutError("Timeout waiting for peers");
				return orderedWins(); // settle:any: return whatever successes we got
			}
		}

		getPublicKey(hash: string): PublicSignKey | undefined {
			return this.peerKeyHashToPublicKey.get(hash);
		}

	get pending(): boolean {
		return this._ackCallbacks.size > 0;
	}

	// make this into a job? run every few ms
		maybePruneConnections(): Promise<void> {
			// Hard cap on peer streams: treat as a primary pruning signal.
			if (this.peers.size > this.connectionManagerOptions.maxConnections) {
				return this.pruneConnectionsToLimits();
			}

			if (this.connectionManagerOptions.pruner) {
				if (this.connectionManagerOptions.pruner.bandwidth != null) {
					let usedBandwidth = 0;
					for (const [_k, v] of this.peers) {
					usedBandwidth += v.usedBandwidth;
				}
				usedBandwidth /= this.peers.size;

				if (usedBandwidth > this.connectionManagerOptions.pruner.bandwidth) {
					// prune
					return this.pruneConnections();
				}
				} else if (this.connectionManagerOptions.pruner.maxBuffer != null) {
					const queuedBytes = this.getQueuedBytes();
					if (queuedBytes > this.connectionManagerOptions.pruner.maxBuffer) {
						// prune
						return this.pruneConnections();
					}
				}
			}

			return Promise.resolve();
		}

	async pruneConnections(): Promise<void> {
		// TODO sort by bandwidth
		if (this.peers.size <= this.connectionManagerOptions.minConnections) {
			return;
		}

		const sorted = [...this.peers.values()]
			.sort((x, y) => x.usedBandwidth - y.usedBandwidth)
			.map((x) => x.publicKey.hashcode());
			const prunables = this.routes.getPrunable(sorted);
			if (prunables.length === 0) {
				return;
			}

			const stream = this.peers.get(prunables[0])!;
			this.prunedConnectionsCache?.add(stream.publicKey.hashcode());

			await this.onPeerDisconnected(stream.peerId);
			return this.components.connectionManager.closeConnections(stream.peerId);
		}

	getQueuedBytes(): number {
		let sum = 0;
		for (const [_k, ps] of this.peers) {
			sum += measureOutboundQueuedBytes(ps as any); // cast to access hook
		}
		return sum;
	}
}

export const waitForReachable = async (
	...libs: {
		waitFor: (peer: PeerId | PublicSignKey) => Promise<void>;
		peerId: PeerId;
	}[]
) => {
	for (let i = 0; i < libs.length; i++) {
		for (let j = 0; j < libs.length; j++) {
			if (i === j) {
				continue;
			}
			await libs[i].waitFor(libs[j].peerId);
			await libs[j].waitFor(libs[i].peerId);
		}
	}
};

export const waitForNeighbour = async (
	...libs: {
		waitFor: (
			peer: PeerRefs,
			options?: { target?: "neighbor" },
		) => Promise<string[]>;
		peerId: PeerId;
	}[]
) => {
	for (let i = 0; i < libs.length; i++) {
		for (let j = 0; j < libs.length; j++) {
			if (i === j) {
				continue;
			}
			if (libs[i].peerId.equals(libs[j].peerId)) {
				throw new Error("Unexpected waiting for self");
			}
			await libs[i].waitFor(libs[j].peerId, { target: "neighbor" });
			await libs[j].waitFor(libs[i].peerId, { target: "neighbor" });
		}
	}
};
