import { getPublicKeyFromPeerId, ready, sha256Sync, toBase64 } from "@peerbit/crypto";
import type { Connection } from "@libp2p/interface";
import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";
import {
	DirectStream,
	type DirectStreamComponents,
	type DirectStreamOptions,
	type PeerStreams,
	dontThrowIfDeliveryError,
} from "@peerbit/stream";
import {
	AnyWhere,
	DataMessage,
	type FanoutRouteTokenHint,
	type StreamEvents,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError, delay } from "@peerbit/time";
import { anySignal } from "any-signal";
import { Uint8ArrayList } from "uint8arraylist";
import { TopicRootControlPlane } from "./topic-root-control-plane.js";

export type FanoutTreeChannelId = {
	root: string;
	topic: string;
	key: Uint8Array; // 32 bytes
	suffixKey: string; // base64 of key[0..24)
};

export type FanoutTreeChannelRole = "root" | "node";

export type FanoutTreeStreamOptions = DirectStreamOptions & {
	/**
	 * Optional RNG hook used for shuffling/join request ids.
	 *
	 * This is primarily intended for deterministic simulation harnesses.
	 */
	random?: () => number;
	/**
	 * Optional topic-root resolver for consumers (e.g. shared-log) that need to
	 * resolve a root when joining a channel without an explicit root.
	 *
	 * FanoutTree does not currently use this internally, but we expose it as a
	 * shared control-plane hook so applications do not need to hang root
	 * resolution off of a separate pubsub implementation.
	 */
	topicRootControlPlane?: TopicRootControlPlane;
};

export type FanoutTreeChannelOptions = {
	role: FanoutTreeChannelRole;

	// Used to approximate upload capacity in "children per node" terms.
	// For real enforcement we will likely move to token buckets and dynamic sizing,
	// but this gets us a bounded topology that we can reason about.
	msgRate: number; // messages/sec
	msgSize: number; // bytes

	uploadLimitBps: number;
	maxChildren: number;
	/**
	 * Extra per-child overhead (bytes) to include when estimating upload usage.
	 *
	 * This should include stream/protocol overhead and any framing/signature/etc
	 * that is not part of `msgSize`.
	 */
	uploadOverheadBytes?: number;

	/**
	 * Token bucket burst window for upload shaping.
	 *
	 * Capacity = `uploadLimitBps * (uploadBurstMs/1000)`.
	 */
	uploadBurstMs?: number;
	bidPerByte?: number;
	allowKick?: boolean;

	/**
	 * Enable bounded pull-repair (tree-first delivery, parent-based backfill).
	 *
	 * Defaults to `true`. Set to `false` to disable caching/repair logic for the channel.
	 */
	repair?: boolean;
	repairWindowMessages?: number;
	/**
	 * How far back (in messages) we will attempt to repair missing sequences.
	 *
	 * Defaults to `repairWindowMessages` (no extra pruning). For "live" workloads you
	 * may want to set this lower so repair doesn't waste work on stale data.
	 */
	repairMaxBackfillMessages?: number;
	repairIntervalMs?: number;
	repairMaxPerReq?: number;

	/**
	 * If enabled, try to repair missing sequences by querying a small number of
	 * additional peers (not only the current parent). This is a stepping stone
	 * towards Plumtree-style neighbor-assisted repair.
	 */
	neighborRepair?: boolean;
	neighborRepairPeers?: number;

	/**
	 * Target number of "lazy" mesh peers to keep connected for neighbor-assisted repair.
	 * These peers are discovered via trackers and are not part of the parent/child tree.
	 */
	neighborMeshPeers?: number;

	/**
	 * How often to send an IHAVE-style cache summary to mesh peers.
	 * This is throttled and sent only when the node has seen new data.
	 */
	neighborAnnounceIntervalMs?: number;

	/**
	 * How often to refresh mesh peers via tracker queries.
	 */
	neighborMeshRefreshIntervalMs?: number;

	/**
	 * How long to trust an IHAVE summary from a peer when choosing fetch targets.
	 */
	neighborHaveTtlMs?: number;

	/**
	 * Optional budget (token bucket) for neighbor-assisted repair `FETCH_REQ` control traffic.
	 *
	 * If `<= 0`, no budget is applied.
	 */
	neighborRepairBudgetBps?: number;
	neighborRepairBurstMs?: number;

	/**
	 * Optional ingress budget (token bucket) for proxy publishes (`MSG_PUBLISH_PROXY`).
	 *
	 * This is enforced per established child link to cap amplification/DoS at the root
	 * and on intermediate relays. Cost is measured in payload bytes.
	 *
	 * If `<= 0`, no budget is applied.
	 */
	proxyPublishBudgetBps?: number;
	proxyPublishBurstMs?: number;

	/**
	 * Optional ingress budget (token bucket) for relaying unicast control traffic
	 * (`MSG_UNICAST`, `MSG_UNICAST_ACK`).
	 *
	 * This is enforced per established child link. Cost is measured in payload bytes.
	 *
	 * If `<= 0`, no budget is applied.
	 */
	unicastBudgetBps?: number;
	unicastBurstMs?: number;

	/**
	 * If set (>0), do not forward data that is older than this many milliseconds,
	 * based on the message header timestamp (origin publish time, when forwarding
	 * without re-signing).
	 *
	 * This is primarily intended for "live" workloads where late data is worse
	 * than missing data.
	 */
	maxDataAgeMs?: number;

	/**
	 * Max number of cached route tokens kept per channel for targeted unicast.
	 *
	 * `0` disables route caching.
	 */
	routeCacheMaxEntries?: number;

	/**
	 * TTL for cached route tokens (milliseconds).
	 *
	 * `0` disables expiry.
	 */
	routeCacheTtlMs?: number;

	/**
	 * Best-effort "peer hints" cache size bound (per channel).
	 *
	 * This tracks peer hashes observed on the channel control-plane and is used
	 * for route proxy fanout and neighbor-assisted repair. It is intentionally
	 * bounded so it cannot grow toward channel size at large scale.
	 *
	 * `0` disables peer hint tracking.
	 */
	peerHintMaxEntries?: number;

	/**
	 * TTL for peer hint entries (milliseconds).
	 *
	 * `0` disables expiry.
	 */
	peerHintTtlMs?: number;
};

export type FanoutTreeJoinOptions = {
	timeoutMs?: number;
	retryMs?: number;
	signal?: AbortSignal;
	staleAfterMs?: number;

	/**
	 * How long to wait for a JOIN_ACCEPT/JOIN_REJECT after sending a JOIN_REQ.
	 */
	joinReqTimeoutMs?: number;

	/**
	 * Optional bootstrap nodes used as rendezvous/tracker servers for joining.
	 * If omitted, `FanoutTree.setBootstraps()` (if configured) is used.
	 */
	bootstrap?: Array<string | Multiaddr>;

	/**
	 * Max time to wait for a bootstrapped peer to become a `@peerbit/stream`
	 * neighbor (protocol streams established) after dialing.
	 */
	bootstrapDialTimeoutMs?: number;

	/**
	 * Max number of bootstrap peers to dial and keep as tracker candidates while joining.
	 *
	 * Set to `0` to use all provided bootstraps.
	 */
	bootstrapMaxPeers?: number;

	/**
	 * How many candidate parents to request per tracker query.
	 */
	trackerCandidates?: number;

	/**
	 * Shuffle only within the first K ranked candidates to spread load without
	 * destroying the bias towards low-level/high-capacity parents.
	 *
	 * Set to `0` to disable shuffling.
	 */
	candidateShuffleTopK?: number;

	/**
	 * How long to wait for a tracker reply before proceeding.
	 */
	trackerQueryTimeoutMs?: number;

	/**
	 * How often to announce parent capacity to trackers (keep-alive).
	 */
	announceIntervalMs?: number;

	/**
	 * TTL for announcements stored by trackers.
	 * Trackers should treat entries as stale after this.
	 */
	announceTtlMs?: number;

	/**
	 * Min interval between re-dial attempts to bootstrap peers when joining.
	 */
	bootstrapEnsureIntervalMs?: number;

	/**
	 * Min interval between tracker queries while joining.
	 */
	trackerQueryIntervalMs?: number;

	/**
	 * Max number of join candidates to try per retry "round".
	 *
	 * This prevents a long tail of sequential JOIN_REQ timeouts from blocking
	 * the join loop for tens of seconds under overload.
	 */
	joinAttemptsPerRound?: number;

	/**
	 * Cooldown applied to a candidate parent after dial/join failures.
	 *
	 * This reduces hot-spotting (everyone hammering the same few parents) and
	 * keeps control-plane traffic bounded at large scale.
	 */
	candidateCooldownMs?: number;

	/**
	 * Candidate scoring mode for selecting parent join targets.
	 *
	 * - `ranked-shuffle` (default): rank by (level, freeSlots, bid, source) and
	 *   shuffle within `candidateShuffleTopK` to spread load.
	 * - `ranked-strict`: try ranked candidates in order (no shuffle).
	 * - `weighted`: weighted shuffle within `candidateShuffleTopK` using
	 *   `candidateScoringWeights` (defaults bias low level + free slots).
	 */
	candidateScoringMode?: "ranked-shuffle" | "ranked-strict" | "weighted";

	/**
	 * Weights used when `candidateScoringMode="weighted"`.
	 *
	 * Larger values increase the influence of that signal.
	 */
	candidateScoringWeights?: {
		level?: number;
		freeSlots?: number;
		connected?: number;
		bidPerByte?: number;
		source?: number;
	};
};

/**
 * Tracker-backed provider discovery for targeted pulls (blocks/RPC).
 *
 * Providers announce under a bounded namespace key, and consumers query for K candidates.
 * This avoids any search/flood behaviors at large scale.
 */
export type FanoutProviderCandidate = {
	hash: string;
	addrs: Multiaddr[];
};

export type FanoutProviderAnnounceOptions = {
	ttlMs?: number;
	announceIntervalMs?: number;
	bootstrap?: Array<string | Multiaddr>;
	bootstrapDialTimeoutMs?: number;
	bootstrapMaxPeers?: number;
};

export type FanoutProviderQueryOptions = {
	want?: number;
	seed?: number;
	timeoutMs?: number;
	queryTimeoutMs?: number;
	cacheTtlMs?: number;
	signal?: AbortSignal;
	bootstrap?: Array<string | Multiaddr>;
	bootstrapDialTimeoutMs?: number;
	bootstrapMaxPeers?: number;
};

export type FanoutProviderHandle = {
	close: () => void;
};

export type FanoutTreeDataEvent = {
	topic: string;
	root: string;
	seq: number;
	payload: Uint8Array;
	from: string; // immediate sender (edge used for forwarding)
	origin: string; // original sender (signature[0]) if present, else `from`
	timestamp: bigint; // sender-provided timestamp (DataMessage.header.timestamp)
	message: DataMessage; // transport-level message (signed by root for proxy publishes)
};

export type FanoutTreeUnicastEvent = {
	topic: string;
	root: string;
	route: string[];
	payload: Uint8Array;
	from: string; // immediate sender (edge used for forwarding)
	origin: string; // original sender (signature[0])
	to: string; // final destination hash
	timestamp: bigint; // sender-provided timestamp (DataMessage.header.timestamp)
	message: DataMessage; // transport-level message carrying the unicast control frame
};

export type FanoutTreeChannelMetrics = {
	controlSends: number;
	controlBytesSent: number;
	controlReceives: number;
	controlBytesReceived: number;

	/**
	 * Control-plane byte breakdown by purpose.
	 *
	 * These include the full encoded control message bytes and are counted per transmission.
	 */
	controlBytesSentJoin: number;
	controlBytesSentRepair: number;
	controlBytesSentTracker: number;
	controlBytesReceivedJoin: number;
	controlBytesReceivedRepair: number;
	controlBytesReceivedTracker: number;

	dataSends: number;
	dataPayloadBytesSent: number;
	dataReceives: number;
	dataPayloadBytesReceived: number;
	staleForwardsDropped: number;
	dataWriteDrops: number;
	/**
	 * Proxy publish frames dropped due to local rate limiting.
	 *
	 * This is an abuse-resistance knob: it caps how much a single child can
	 * amplify traffic via the root.
	 */
	proxyPublishDrops: number;
	/**
	 * Unicast frames dropped due to local rate limiting.
	 */
	unicastDrops: number;

	joinReqSent: number;
	joinReqReceived: number;
	joinAcceptSent: number;
	joinAcceptReceived: number;
	joinRejectSent: number;
	joinRejectReceived: number;
	kickSent: number;
	kickReceived: number;
	reparentDisconnect: number;
	reparentStale: number;
	reparentKicked: number;
	endSent: number;
	endReceived: number;

	repairReqSent: number;
	repairReqReceived: number;
	fetchReqSent: number;
	fetchReqReceived: number;
	ihaveSent: number;
	ihaveReceived: number;

	trackerAnnounceSent: number;
	trackerAnnounceReceived: number;
	trackerQuerySent: number;
	trackerQueryReceived: number;
	trackerReplySent: number;
	trackerReplyReceived: number;
	trackerFeedbackSent: number;
	trackerFeedbackReceived: number;

	cacheHitsServed: number;
	cacheMissesServed: number;
	holeFillsFromNeighbor: number;
	earnings: number;

	routeCacheHits: number;
	routeCacheMisses: number;
	routeCacheExpirations: number;
	routeCacheEvictions: number;
	routeProxyQueries: number;
	routeProxyTimeouts: number;
	routeProxyFanout: number;
};

export interface FanoutTreeEvents extends StreamEvents {
	"fanout:data": CustomEvent<FanoutTreeDataEvent>;
	"fanout:unicast": CustomEvent<FanoutTreeUnicastEvent>;
	"fanout:joined": CustomEvent<{ topic: string; root: string; parent: string }>;
	"fanout:kicked": CustomEvent<{ topic: string; root: string; from: string }>;
}

const CONTROL_PRIORITY = 10;
const DATA_PRIORITY = 1;
const ROUTE_PROXY_TIMEOUT_MS = 10_000;
const ROUTE_CACHE_MAX_ENTRIES_HARD_CAP = 100_000;
const PEER_HINT_MAX_ENTRIES_HARD_CAP = 100_000;
// Best-effort address cache for join candidates learned via trackers/redirects.
// This must stay small; large values can blow up memory in large-scale sims.
const KNOWN_CANDIDATE_ADDRS_MAX_ENTRIES = 256;
const PROVIDER_DIRECTORY_MAX_ENTRIES = 16_384;
// Best-effort bound on the number of provider namespaces we keep cached locally.
// This prevents unbounded memory growth if callers use extremely high-cardinality namespaces
// (for example, `cid:<cid>` per block).
const PROVIDER_DIRECTORY_MAX_NAMESPACES = 4_096;
// Best-effort bounds for tracker state (join candidate directory).
// Trackers should keep only a bounded set of recent candidates per channel, otherwise
// large networks can cause unbounded memory growth on the tracker nodes.
const TRACKER_DIRECTORY_MAX_ENTRIES = 16_384;
const TRACKER_DIRECTORY_MAX_NAMESPACES = 4_096;

const FANOUT_PROTOCOLS = ["/peerbit/fanout-tree/0.5.0"];

const ID_PREFIX = Uint8Array.from([0x46, 0x4f, 0x55, 0x54]); // "FOUT"

const OVERLOAD_KICK_STREAK_THRESHOLD = 5;
const OVERLOAD_KICK_COOLDOWN_MS = 2_000;
const OVERLOAD_KICK_MAX_PER_EVENT = 4;

// DATA-plane sends are best-effort (repair provides reliability). Never block on
// stream writability; optionally kick persistently failing children.
const DATA_WRITE_FAIL_KICK_STREAK_THRESHOLD = 3;
const DATA_WRITE_FAIL_KICK_COOLDOWN_MS = 2_000;
const DATA_WRITE_FAIL_KICK_MAX_PER_EVENT = 4;

const JOIN_REJECT_REDIRECT_MAX = 4;
const JOIN_REJECT_REDIRECT_ADDR_MAX = 8;
const JOIN_REJECT_REDIRECT_QUEUE_MAX = 64;
// When a relay loses its parent, pause before trying to rejoin so its children can
// attach elsewhere (helps avoid disconnected components stabilizing).
// Rejoin cooldown after losing a parent while acting as a relay (had children).
//
// This gives kicked children a window to reattach elsewhere (helps avoid stable
// disconnected components). Leaf nodes (no children) rejoin immediately.
const RELAY_REJOIN_COOLDOWN_MS = 3_000;

const MSG_JOIN_REQ = 1;
const MSG_JOIN_ACCEPT = 2;
const MSG_JOIN_REJECT = 3;
const MSG_KICK = 4;
const MSG_DATA = 10;
const MSG_END = 11;
const MSG_UNICAST = 12;
const MSG_ROUTE_QUERY = 13;
const MSG_ROUTE_REPLY = 14;
const MSG_PUBLISH_PROXY = 15;
const MSG_LEAVE = 16;
const MSG_UNICAST_ACK = 17;
const MSG_REPAIR_REQ = 20;
const MSG_FETCH_REQ = 21;
const MSG_IHAVE = 22;
const MSG_TRACKER_ANNOUNCE = 30;
const MSG_TRACKER_QUERY = 31;
const MSG_TRACKER_REPLY = 32;
const MSG_TRACKER_FEEDBACK = 33;
const MSG_PROVIDER_ANNOUNCE = 34;
const MSG_PROVIDER_QUERY = 35;
const MSG_PROVIDER_REPLY = 36;

const JOIN_REJECT_NOT_ATTACHED = 1;
const JOIN_REJECT_NO_CAPACITY = 2;
const JOIN_REJECT_LOW_BID = 3;

const TRACKER_FEEDBACK_JOINED = 1;
const TRACKER_FEEDBACK_DIAL_FAILED = 2;
const TRACKER_FEEDBACK_JOIN_TIMEOUT = 3;
const TRACKER_FEEDBACK_JOIN_REJECT = 4;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

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

const writeU64BE = (buf: Uint8Array, offset: number, value: bigint) => {
	let v = value & 0xffffffffffffffffn;
	for (let i = 7; i >= 0; i--) {
		buf[offset + i] = Number(v & 0xffn);
		v >>= 8n;
	}
};

const readU64BE = (buf: Uint8Array, offset: number) => {
	let v = 0n;
	for (let i = 0; i < 8; i++) {
		v = (v << 8n) | BigInt(buf[offset + i]!);
	}
	return v;
};

const writeU16BE = (buf: Uint8Array, offset: number, value: number) => {
	buf[offset + 0] = (value >>> 8) & 0xff;
	buf[offset + 1] = value & 0xff;
};

const readU16BE = (buf: Uint8Array, offset: number) =>
	((buf[offset + 0] << 8) | buf[offset + 1]) >>> 0;

const encodeJoinReq = (channelKey: Uint8Array, reqId: number, bidPerByte: number) => {
	const buf = new Uint8Array(1 + 32 + 4 + 4);
	buf[0] = MSG_JOIN_REQ;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	writeU32BE(buf, 37, bidPerByte >>> 0);
	return buf;
};

const MAX_ROUTE_HOPS = 32;

const UNICAST_FLAG_ACK = 1;
const UNICAST_ACK_DEFAULT_TIMEOUT_MS = 30_000;

const encodeJoinAccept = (
	channelKey: Uint8Array,
	reqId: number,
	level: number,
	parentRouteFromRoot?: string[],
) => {
	const routeBytes: Uint8Array[] = [];
	let bytes = 1 + 32 + 4 + 2 + 1;
	let count = 0;

	for (const hop of parentRouteFromRoot ?? []) {
		if (count >= MAX_ROUTE_HOPS) break;
		if (typeof hop !== "string") continue;
		const hb = textEncoder.encode(hop);
		if (hb.length === 0 || hb.length > 255) continue;
		routeBytes.push(hb);
		bytes += 1 + hb.length;
		count += 1;
	}

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_JOIN_ACCEPT;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	writeU16BE(buf, 37, level & 0xffff);
	buf[39] = count & 0xff;

	let offset = 40;
	for (const hb of routeBytes) {
		buf[offset++] = hb.length & 0xff;
		buf.set(hb, offset);
		offset += hb.length;
	}
	return buf;
};

const encodeJoinReject = (
	channelKey: Uint8Array,
	reqId: number,
	reason: number,
	redirects?: JoinRejectRedirect[],
) => {
	const encoded: Array<{ hashBytes: Uint8Array; addrs: Uint8Array[] }> = [];
	for (const r of redirects ?? []) {
		if (!r?.hash) continue;
		const hashBytes = textEncoder.encode(r.hash);
		if (hashBytes.length === 0 || hashBytes.length > 255) continue;
		const addrs = (r.addrs ?? [])
			.filter((a): a is Uint8Array => a instanceof Uint8Array)
			.filter((a) => a.length > 0 && a.length <= 0xffff)
			.slice(0, JOIN_REJECT_REDIRECT_ADDR_MAX);
		if (addrs.length === 0) continue;
		encoded.push({ hashBytes, addrs });
		if (encoded.length >= JOIN_REJECT_REDIRECT_MAX) break;
	}

	if (encoded.length === 0) {
		const buf = new Uint8Array(1 + 32 + 4 + 1);
		buf[0] = MSG_JOIN_REJECT;
		buf.set(channelKey, 1);
		writeU32BE(buf, 33, reqId >>> 0);
		buf[37] = reason & 0xff;
		return buf;
	}

	let bytes = 1 + 32 + 4 + 1 + 1;
	for (const e of encoded) {
		bytes += 1 + e.hashBytes.length + 1;
		for (const a of e.addrs) bytes += 2 + a.length;
	}

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_JOIN_REJECT;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = reason & 0xff;
	buf[38] = Math.max(0, Math.min(255, encoded.length)) & 0xff;
	let offset = 39;
	for (const e of encoded) {
		buf[offset++] = e.hashBytes.length & 0xff;
		buf.set(e.hashBytes, offset);
		offset += e.hashBytes.length;
		buf[offset++] = Math.max(0, Math.min(255, e.addrs.length)) & 0xff;
		for (const a of e.addrs) {
			writeU16BE(buf, offset, a.length);
			offset += 2;
			buf.set(a, offset);
			offset += a.length;
		}
	}

	return buf;
};

const encodeKick = (channelKey: Uint8Array) => {
	const buf = new Uint8Array(1 + 32);
	buf[0] = MSG_KICK;
	buf.set(channelKey, 1);
	return buf;
};

const encodeEnd = (channelKey: Uint8Array, lastSeqExclusive: number) => {
	const buf = new Uint8Array(1 + 32 + 4);
	buf[0] = MSG_END;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, lastSeqExclusive >>> 0);
	return buf;
};

const encodeRepairReq = (
	channelKey: Uint8Array,
	reqId: number,
	missingSeqs: number[],
) => {
	const count = Math.max(0, Math.min(255, missingSeqs.length));
	const buf = new Uint8Array(1 + 32 + 4 + 1 + count * 4);
	buf[0] = MSG_REPAIR_REQ;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = count & 0xff;
	for (let i = 0; i < count; i++) {
		writeU32BE(buf, 38 + i * 4, missingSeqs[i]! >>> 0);
	}
	return buf;
};

const encodeFetchReq = (
	channelKey: Uint8Array,
	reqId: number,
	missingSeqs: number[],
) => {
	const count = Math.max(0, Math.min(255, missingSeqs.length));
	const buf = new Uint8Array(1 + 32 + 4 + 1 + count * 4);
	buf[0] = MSG_FETCH_REQ;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = count & 0xff;
	for (let i = 0; i < count; i++) {
		writeU32BE(buf, 38 + i * 4, missingSeqs[i]! >>> 0);
	}
	return buf;
};

const encodeIHave = (
	channelKey: Uint8Array,
	haveFrom: number,
	haveToExclusive: number,
) => {
	const buf = new Uint8Array(1 + 32 + 4 + 4);
	buf[0] = MSG_IHAVE;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, haveFrom >>> 0);
	writeU32BE(buf, 37, haveToExclusive >>> 0);
	return buf;
};

const encodeData = (payload: Uint8Array) => {
	const buf = new Uint8Array(1 + payload.length);
	buf[0] = MSG_DATA;
	buf.set(payload, 1);
	return buf;
};

const encodePublishProxy = (channelKey: Uint8Array, payload: Uint8Array) => {
	const buf = new Uint8Array(1 + 32 + payload.length);
	buf[0] = MSG_PUBLISH_PROXY;
	buf.set(channelKey, 1);
	buf.set(payload, 33);
	return buf;
};

const encodeLeave = (channelKey: Uint8Array) => {
	const buf = new Uint8Array(1 + 32);
	buf[0] = MSG_LEAVE;
	buf.set(channelKey, 1);
	return buf;
};

const encodeUnicast = (
	channelKey: Uint8Array,
	route: string[],
	payload: Uint8Array,
	options?: { ackToken?: bigint; replyRoute?: string[] },
) => {
	const wantsAck = options?.ackToken != null;
	const flags = wantsAck ? UNICAST_FLAG_ACK : 0;
	const toRouteBytes: Uint8Array[] = [];
	let bytes = 1 + 32 + 1 + (wantsAck ? 8 : 0) + 1;
	let toCount = 0;

	for (const hop of route ?? []) {
		if (toCount >= MAX_ROUTE_HOPS) break;
		if (typeof hop !== "string") continue;
		const hb = textEncoder.encode(hop);
		if (hb.length === 0 || hb.length > 255) continue;
		toRouteBytes.push(hb);
		bytes += 1 + hb.length;
		toCount += 1;
	}

	const replyRouteBytes: Uint8Array[] = [];
	let replyCount = 0;
	if (wantsAck) {
		bytes += 1; // replyRouteCount
		for (const hop of options?.replyRoute ?? []) {
			if (replyCount >= MAX_ROUTE_HOPS) break;
			if (typeof hop !== "string") continue;
			const hb = textEncoder.encode(hop);
			if (hb.length === 0 || hb.length > 255) continue;
			replyRouteBytes.push(hb);
			bytes += 1 + hb.length;
			replyCount += 1;
		}
	}

	bytes += payload.byteLength;
	const buf = new Uint8Array(bytes);
	buf[0] = MSG_UNICAST;
	buf.set(channelKey, 1);
	buf[33] = flags & 0xff;
	let offset = 34;
	if (wantsAck) {
		writeU64BE(buf, offset, options!.ackToken!);
		offset += 8;
	}
	buf[offset++] = toCount & 0xff;
	for (const hb of toRouteBytes) {
		buf[offset++] = hb.length & 0xff;
		buf.set(hb, offset);
		offset += hb.length;
	}
	if (wantsAck) {
		buf[offset++] = replyCount & 0xff;
		for (const hb of replyRouteBytes) {
			buf[offset++] = hb.length & 0xff;
			buf.set(hb, offset);
			offset += hb.length;
		}
	}
	buf.set(payload, offset);
	return buf;
};

const encodeUnicastAck = (channelKey: Uint8Array, ackToken: bigint, route: string[]) => {
	const routeBytes: Uint8Array[] = [];
	let bytes = 1 + 32 + 8 + 1;
	let count = 0;
	for (const hop of route ?? []) {
		if (count >= MAX_ROUTE_HOPS) break;
		if (typeof hop !== "string") continue;
		const hb = textEncoder.encode(hop);
		if (hb.length === 0 || hb.length > 255) continue;
		routeBytes.push(hb);
		bytes += 1 + hb.length;
		count += 1;
	}
	const buf = new Uint8Array(bytes);
	buf[0] = MSG_UNICAST_ACK;
	buf.set(channelKey, 1);
	writeU64BE(buf, 33, ackToken);
	buf[41] = count & 0xff;
	let offset = 42;
	for (const hb of routeBytes) {
		buf[offset++] = hb.length & 0xff;
		buf.set(hb, offset);
		offset += hb.length;
	}
	return buf;
};

const encodeRouteQuery = (
	channelKey: Uint8Array,
	reqId: number,
	targetHash: string,
) => {
	const targetBytes = textEncoder.encode(targetHash);
	const targetLen = Math.max(0, Math.min(255, targetBytes.length));
	const buf = new Uint8Array(1 + 32 + 4 + 1 + targetLen);
	buf[0] = MSG_ROUTE_QUERY;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = targetLen & 0xff;
	buf.set(targetBytes.subarray(0, targetLen), 38);
	return buf;
};

const encodeRouteReply = (
	channelKey: Uint8Array,
	reqId: number,
	route?: string[],
) => {
	const routeBytes: Uint8Array[] = [];
	let bytes = 1 + 32 + 4 + 1;
	let count = 0;
	for (const hop of route ?? []) {
		if (count >= MAX_ROUTE_HOPS) break;
		if (typeof hop !== "string") continue;
		const hb = textEncoder.encode(hop);
		if (hb.length === 0 || hb.length > 255) continue;
		routeBytes.push(hb);
		bytes += 1 + hb.length;
		count += 1;
	}
	const buf = new Uint8Array(bytes);
	buf[0] = MSG_ROUTE_REPLY;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = count & 0xff;
	let offset = 38;
	for (const hb of routeBytes) {
		buf[offset++] = hb.length & 0xff;
		buf.set(hb, offset);
		offset += hb.length;
	}
	return buf;
};

const decodeRoute = (
	data: Uint8Array,
	offsetStart: number,
	routeCount: number,
): { route: string[]; offset: number } => {
	let offset = offsetStart;
	const route: string[] = [];
	for (let i = 0; i < routeCount; i++) {
		if (offset + 1 > data.length) break;
		const len = data[offset++]!;
		if (len === 0) break;
		if (offset + len > data.length) break;
		if (route.length < MAX_ROUTE_HOPS) {
			route.push(textDecoder.decode(data.subarray(offset, offset + len)));
		}
		offset += len;
	}
	return { route, offset };
};

type RouteCacheEntry = {
	route: string[];
	updatedAt: number;
};

type TrackerCandidate = {
	hash: string;
	level: number;
	freeSlots: number;
	bidPerByte: number;
	addrs: Multiaddr[];
};

type TrackerEntry = {
	hash: string;
	level: number;
	freeSlots: number;
	bidPerByte: number;
	addrs: Uint8Array[];
	expiresAt: number;
};

type JoinRejectRedirect = {
	hash: string;
	addrs: Uint8Array[];
};

const clampU16 = (v: number) => Math.max(0, Math.min(0xffff, v | 0));

const encodeTrackerAnnounce = (
	channelKey: Uint8Array,
	ttlMs: number,
	level: number,
	maxChildren: number,
	freeSlots: number,
	bidPerByte: number,
	addrs: Multiaddr[],
) => {
	const addrCount = Math.max(0, Math.min(255, addrs.length));
	let bytes = 1 + 32 + 4 + 2 + 2 + 2 + 4 + 1;
	const addrBytes = new Array<Uint8Array>(addrCount);
	for (let i = 0; i < addrCount; i++) {
		const b = addrs[i]!.bytes;
		addrBytes[i] = b;
		bytes += 2 + b.length;
	}

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_TRACKER_ANNOUNCE;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, Math.max(0, Math.floor(ttlMs)) >>> 0);
	writeU16BE(buf, 37, clampU16(level));
	writeU16BE(buf, 39, clampU16(maxChildren));
	writeU16BE(buf, 41, clampU16(freeSlots));
	writeU32BE(buf, 43, Math.max(0, Math.floor(bidPerByte)) >>> 0);
	buf[47] = addrCount & 0xff;
	let offset = 48;
	for (let i = 0; i < addrCount; i++) {
		const b = addrBytes[i]!;
		writeU16BE(buf, offset, b.length);
		offset += 2;
		buf.set(b, offset);
		offset += b.length;
	}
	return buf;
};

const encodeTrackerQuery = (channelKey: Uint8Array, reqId: number, want: number) => {
	const buf = new Uint8Array(1 + 32 + 4 + 2);
	buf[0] = MSG_TRACKER_QUERY;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	writeU16BE(buf, 37, clampU16(want));
	return buf;
};

const encodeTrackerReply = (channelKey: Uint8Array, reqId: number, entries: TrackerEntry[]) => {
	const count = Math.max(0, Math.min(255, entries.length));
	let bytes = 1 + 32 + 4 + 1;
	const encoded: Array<{
		hashBytes: Uint8Array;
		level: number;
		freeSlots: number;
		bidPerByte: number;
		addrs: Uint8Array[];
	}> = [];
	for (let i = 0; i < count; i++) {
		const e = entries[i]!;
		const hashBytes = textEncoder.encode(e.hash);
		if (hashBytes.length > 255) continue;
		const addrCount = Math.max(0, Math.min(255, e.addrs.length));
		const addrs = e.addrs.slice(0, addrCount);
		bytes += 1 + hashBytes.length + 2 + 2 + 4 + 1;
		for (const a of addrs) bytes += 2 + a.length;
		encoded.push({
			hashBytes,
			level: e.level,
			freeSlots: e.freeSlots,
			bidPerByte: e.bidPerByte,
			addrs,
		});
	}

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_TRACKER_REPLY;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = Math.max(0, Math.min(255, encoded.length)) & 0xff;
	let offset = 38;
	for (const e of encoded) {
		buf[offset++] = e.hashBytes.length & 0xff;
		buf.set(e.hashBytes, offset);
		offset += e.hashBytes.length;
		writeU16BE(buf, offset, clampU16(e.level));
		offset += 2;
		writeU16BE(buf, offset, clampU16(e.freeSlots));
		offset += 2;
		writeU32BE(buf, offset, Math.max(0, Math.floor(e.bidPerByte)) >>> 0);
		offset += 4;
		buf[offset++] = Math.max(0, Math.min(255, e.addrs.length)) & 0xff;
		for (const a of e.addrs) {
			writeU16BE(buf, offset, a.length);
			offset += 2;
			buf.set(a, offset);
			offset += a.length;
		}
	}
	return buf;
};

const encodeTrackerFeedback = (
	channelKey: Uint8Array,
	candidateHash: string,
	event: number,
	reason: number,
) => {
	const hashBytes = textEncoder.encode(candidateHash);
	const hashLen = Math.max(0, Math.min(255, hashBytes.length));
	const buf = new Uint8Array(1 + 32 + 1 + hashLen + 1 + 1);
	buf[0] = MSG_TRACKER_FEEDBACK;
	buf.set(channelKey, 1);
	buf[33] = hashLen & 0xff;
	buf.set(hashBytes.subarray(0, hashLen), 34);
	buf[34 + hashLen] = event & 0xff;
	buf[34 + hashLen + 1] = reason & 0xff;
	return buf;
};

type ProviderNamespaceId = {
	namespace: string;
	key: Uint8Array;
	suffixKey: string;
};

type ProviderEntry = {
	hash: string;
	addrs: Uint8Array[];
	expiresAt: number;
};

type ProviderAnnounceState = {
	id: ProviderNamespaceId;
	ttlMs: number;
	announceIntervalMs: number;
	bootstrapOverride?: Multiaddr[];
	bootstrapDialTimeoutMs: number;
	bootstrapMaxPeers: number;
	closed: boolean;
	loop?: Promise<void>;
};

const encodeProviderAnnounce = (
	namespaceKey: Uint8Array,
	ttlMs: number,
	addrs: Multiaddr[],
) => {
	const addrCount = Math.max(0, Math.min(255, addrs.length));
	let bytes = 1 + 32 + 4 + 1;
	const addrBytes = new Array<Uint8Array>(addrCount);
	for (let i = 0; i < addrCount; i++) {
		const b = addrs[i]!.bytes;
		addrBytes[i] = b;
		bytes += 2 + b.length;
	}

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_PROVIDER_ANNOUNCE;
	buf.set(namespaceKey, 1);
	writeU32BE(buf, 33, Math.max(0, Math.floor(ttlMs)) >>> 0);
	buf[37] = addrCount & 0xff;
	let offset = 38;
	for (let i = 0; i < addrCount; i++) {
		const b = addrBytes[i]!;
		writeU16BE(buf, offset, b.length);
		offset += 2;
		buf.set(b, offset);
		offset += b.length;
	}
	return buf;
};

const encodeProviderQuery = (
	namespaceKey: Uint8Array,
	reqId: number,
	want: number,
	seed: number,
) => {
	const buf = new Uint8Array(1 + 32 + 4 + 2 + 4);
	buf[0] = MSG_PROVIDER_QUERY;
	buf.set(namespaceKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	writeU16BE(buf, 37, clampU16(want));
	writeU32BE(buf, 39, seed >>> 0);
	return buf;
};

const encodeProviderReply = (
	namespaceKey: Uint8Array,
	reqId: number,
	entries: ProviderEntry[],
) => {
	const count = Math.max(0, Math.min(255, entries.length));
	let bytes = 1 + 32 + 4 + 1;
	const encoded: Array<{ hashBytes: Uint8Array; addrs: Uint8Array[] }> = [];
	for (let i = 0; i < count; i++) {
		const e = entries[i]!;
		const hashBytes = textEncoder.encode(e.hash);
		if (hashBytes.length > 255) continue;
		const addrCount = Math.max(0, Math.min(255, e.addrs.length));
		const addrs = e.addrs.slice(0, addrCount);
		bytes += 1 + hashBytes.length + 1;
		for (const a of addrs) bytes += 2 + a.length;
		encoded.push({ hashBytes, addrs });
	}

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_PROVIDER_REPLY;
	buf.set(namespaceKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	buf[37] = Math.max(0, Math.min(255, encoded.length)) & 0xff;
	let offset = 38;
	for (const e of encoded) {
		buf[offset++] = e.hashBytes.length & 0xff;
		buf.set(e.hashBytes, offset);
		offset += e.hashBytes.length;
		buf[offset++] = Math.max(0, Math.min(255, e.addrs.length)) & 0xff;
		for (const a of e.addrs) {
			writeU16BE(buf, offset, a.length);
			offset += 2;
			buf.set(a, offset);
			offset += a.length;
		}
	}
	return buf;
};

type ChildInfo = { bidPerByte: number };

type JoinAttemptResult = {
	ok: boolean;
	rejectReason?: number;
	timedOut?: boolean;
	redirects?: Array<{ hash: string; addrs: Multiaddr[] }>;
};

type PendingUnicastAck = {
	expectedOrigin: string;
	resolve: () => void;
	reject: (error: unknown) => void;
	timer: ReturnType<typeof setTimeout>;
	signal?: AbortSignal;
	onAbort?: () => void;
};

type ChannelState = {
	id: FanoutTreeChannelId;
	metrics: FanoutTreeChannelMetrics;
	level: number;
	isRoot: boolean;
	closed: boolean;
	parent?: string;
	children: Map<string, ChildInfo>;
	/**
	 * Cooldown applied after a relay loses its parent.
	 *
	 * This gives recently-kicked children a chance to attach elsewhere before the
	 * relay races to reclaim scarce parent capacity, which helps avoid
	 * disconnected components stabilizing under an unrooted relay.
	 */
	rejoinCooldownUntil: number;
	/**
	 * True once this node has successfully joined the channel at least once.
	 *
	 * Join timeouts should only apply to the initial `joinChannel()` await, not to
	 * later re-parenting after disconnects.
	 */
	joinedAtLeastOnce: boolean;
	/**
	 * Source route from the channel root to this node (inclusive).
	 *
	 * This is learned during JOIN via the parent and can be shared out-of-band
	 * to enable economical unicast within the channel (no global membership map).
	 */
	routeFromRoot?: string[];
	/**
	 * Best-effort route cache keyed by target peer hash.
	 *
	 * Entries are learned on-demand via `ROUTE_QUERY/ROUTE_REPLY` and cached locally.
	 */
	routeByPeer: Map<string, RouteCacheEntry>;

	seq: number;

	// options
	bidPerByte: number;
	uploadLimitBps: number;
	maxChildren: number;
	effectiveMaxChildren: number;
	allowKick: boolean;
	maxDataAgeMs: number;

	repairEnabled: boolean;
	repairWindowMessages: number;
	repairMaxBackfillMessages: number;
	repairIntervalMs: number;
	repairMaxPerReq: number;
	neighborRepair: boolean;
	neighborRepairPeers: number;
	neighborMeshPeers: number;
	neighborAnnounceIntervalMs: number;
	neighborMeshRefreshIntervalMs: number;
	neighborHaveTtlMs: number;
	neighborRepairBudgetBps: number;
	neighborRepairTokenCapacity: number;
	neighborRepairTokens: number;
	neighborRepairLastRefillAt: number;
	proxyPublishBudgetBps: number;
	proxyPublishTokenCapacity: number;
	proxyPublishTokensByPeer: Map<string, { tokens: number; lastRefillAt: number }>;
	unicastBudgetBps: number;
	unicastTokenCapacity: number;
	unicastTokensByPeer: Map<string, { tokens: number; lastRefillAt: number }>;

	uploadOverheadBytes: number;
	uploadBurstMs: number;
	uploadTokenCapacity: number;
	uploadTokens: number;
	uploadLastRefillAt: number;
	droppedForwards: number;
	overloadStreak: number;
	lastOverloadKickAt: number;
	dataWriteFailStreakByChild: Map<string, number>;
	lastDataWriteFailKickAt: number;

	cacheSeqs?: Int32Array;
	cachePayloads?: Array<Uint8Array | undefined>;
	nextExpectedSeq: number;
	missingSeqs: Set<number>;
	endSeqExclusive: number;
		lastRepairSentAt: number;
		lastParentDataAt: number;
		receivedAnyParentData: boolean;
		channelPeers: Map<string, number>;
		knownCandidateAddrs: Map<string, Multiaddr[]>;
		lazyPeers: Set<string>;
	haveByPeer: Map<
		string,
		{
			haveFrom: number;
			haveToExclusive: number;
			updatedAt: number;
			requests: number;
			successes: number;
		}
	>;
	maxSeqSeen: number;
	lastIHaveSentAt: number;
	lastIHaveSentMaxSeq: number;

	pendingJoin: Map<number, { resolve(res: JoinAttemptResult): void }>;
	pendingTrackerQuery: Map<number, { resolve(entries: TrackerCandidate[]): void }>;
	pendingRouteQuery: Map<number, { resolve(route?: string[]): void }>;
	pendingUnicastAck: Map<bigint, PendingUnicastAck>;
	pendingRouteProxy: Map<
		number,
		{
			requester: string;
			downstreamReqId: number;
			timer: ReturnType<typeof setTimeout>;
			expectedReplies: Set<string>;
			/**
			 * Optional local completion callback (used when the root resolves a route token
			 * for itself by fanning out queries to children).
			 */
			localResolve?: (route?: string[]) => void;
		}
	>;

	bootstrapOverride?: Multiaddr[];
	bootstrapDialTimeoutMs: number;
	bootstrapMaxPeers: number;
	bootstrapEnsureIntervalMs: number;
	cachedBootstrapPeers: string[];
	lastBootstrapEnsureAt: number;

			announceIntervalMs: number;
			announceTtlMs: number;
			lastAnnouncedAt: number;
			peerHintMaxEntries: number;
			peerHintTtlMs: number;
			routeCacheMaxEntries: number;
			routeCacheTtlMs: number;
		announceLoop?: Promise<void>;
	repairLoop?: Promise<void>;
	meshLoop?: Promise<void>;
	joinLoop?: Promise<void>;
	joinedOnce?: { resolve(): void; reject(err: unknown): void; promise: Promise<void> };

	trackerQueryIntervalMs: number;
	cachedTrackerCandidates: TrackerCandidate[];
	lastTrackerQueryAt: number;
};

const createDeferred = (): {
	resolve(): void;
	reject(err: unknown): void;
	promise: Promise<void>;
} => {
	let resolve!: () => void;
	let reject!: (err: unknown) => void;
	const promise = new Promise<void>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { resolve, reject, promise };
};

const createEmptyMetrics = (): FanoutTreeChannelMetrics => ({
	controlSends: 0,
	controlBytesSent: 0,
	controlReceives: 0,
	controlBytesReceived: 0,
	controlBytesSentJoin: 0,
	controlBytesSentRepair: 0,
	controlBytesSentTracker: 0,
	controlBytesReceivedJoin: 0,
	controlBytesReceivedRepair: 0,
	controlBytesReceivedTracker: 0,
	dataSends: 0,
	dataPayloadBytesSent: 0,
	dataReceives: 0,
	dataPayloadBytesReceived: 0,
	staleForwardsDropped: 0,
	dataWriteDrops: 0,
	proxyPublishDrops: 0,
	unicastDrops: 0,
	joinReqSent: 0,
	joinReqReceived: 0,
	joinAcceptSent: 0,
	joinAcceptReceived: 0,
	joinRejectSent: 0,
	joinRejectReceived: 0,
	kickSent: 0,
	kickReceived: 0,
	reparentDisconnect: 0,
	reparentStale: 0,
	reparentKicked: 0,
	endSent: 0,
	endReceived: 0,
	repairReqSent: 0,
	repairReqReceived: 0,
	fetchReqSent: 0,
	fetchReqReceived: 0,
	ihaveSent: 0,
	ihaveReceived: 0,
	trackerAnnounceSent: 0,
	trackerAnnounceReceived: 0,
	trackerQuerySent: 0,
	trackerQueryReceived: 0,
	trackerReplySent: 0,
	trackerReplyReceived: 0,
	trackerFeedbackSent: 0,
	trackerFeedbackReceived: 0,
	cacheHitsServed: 0,
	cacheMissesServed: 0,
	holeFillsFromNeighbor: 0,
	earnings: 0,
	routeCacheHits: 0,
	routeCacheMisses: 0,
	routeCacheExpirations: 0,
	routeCacheEvictions: 0,
	routeProxyQueries: 0,
	routeProxyTimeouts: 0,
	routeProxyFanout: 0,
});

const isDataId = (id: Uint8Array) =>
	id.length === 32 &&
	id[0] === ID_PREFIX[0] &&
	id[1] === ID_PREFIX[1] &&
	id[2] === ID_PREFIX[2] &&
	id[3] === ID_PREFIX[3];

export class FanoutTree extends DirectStream<FanoutTreeEvents> {
	private channelsBySuffixKey = new Map<string, ChannelState>();
	private readonly cachedSuffixKey = new WeakMap<Uint8Array, string>();
	private readonly metricsBySuffixKey = new Map<string, FanoutTreeChannelMetrics>();
	private bootstraps: Multiaddr[] = [];
	private trackerBySuffixKey = new Map<string, Map<string, TrackerEntry>>();
	private trackerNamespaceLru = new Map<string, number>();
	private providerBySuffixKey = new Map<string, Map<string, ProviderEntry>>();
	private providerNamespaceLru = new Map<string, number>();
	private underlayPeerDisconnectHandler?: (ev: any) => void;
	private pendingProviderQueryBySuffixKey = new Map<
		string,
		Map<number, { resolve: (providers: FanoutProviderCandidate[]) => void }>
	>();
	private providerAnnounceBySuffixKey = new Map<string, ProviderAnnounceState>();
	private readonly defaultUploadOverheadBytes = 128;
	private readonly random: () => number;
	private readonly unicastAckNodeTag32: number;
	private unicastAckSeq = 0;
	public readonly topicRootControlPlane: TopicRootControlPlane;

	constructor(
		components: DirectStreamComponents,
		opts?: FanoutTreeStreamOptions,
	) {
		const { random, topicRootControlPlane, ...rest } = (opts ?? {}) as FanoutTreeStreamOptions;
		super(components, FANOUT_PROTOCOLS, {
			canRelayMessage: false,
			...rest,
		});
		this.random = typeof random === "function" ? random : Math.random;
		this.unicastAckNodeTag32 = readU32BE(
			sha256Sync(textEncoder.encode(this.publicKeyHash)),
			0,
		);
			this.topicRootControlPlane = topicRootControlPlane ?? new TopicRootControlPlane();

			const onPeerDisconnect = (ev: any) => {
				const peerId = ev?.detail;
			if (!peerId) return;
			let peerHash: string;
			try {
				peerHash = getPublicKeyFromPeerId(peerId).hashcode();
			} catch {
				return;
				}
				this.onPeerDisconnectedFromUnderlay(peerHash);
			};
			this.underlayPeerDisconnectHandler = onPeerDisconnect;
			this.components.events.addEventListener("peer:disconnect", onPeerDisconnect as any);
		}

		public override async stop() {
			if (this.underlayPeerDisconnectHandler) {
				this.components.events.removeEventListener(
					"peer:disconnect",
					this.underlayPeerDisconnectHandler as any,
				);
				this.underlayPeerDisconnectHandler = undefined;
			}
			return super.stop();
		}

	public setBootstraps(addrs: Array<string | Multiaddr>) {
		this.bootstraps = addrs
			.map((a) => (typeof a === "string" ? multiaddr(a) : a))
			.filter((a) => Boolean(a));
	}

	private touchTrackerNamespace(suffixKey: string, now = Date.now()) {
		if (!suffixKey) return;
		// LRU touch
		this.trackerNamespaceLru.delete(suffixKey);
		this.trackerNamespaceLru.set(suffixKey, now);
		while (this.trackerNamespaceLru.size > TRACKER_DIRECTORY_MAX_NAMESPACES) {
			const oldest = this.trackerNamespaceLru.keys().next().value as string | undefined;
			if (!oldest) break;
			this.trackerNamespaceLru.delete(oldest);
			this.trackerBySuffixKey.delete(oldest);
		}
	}

	private pruneTrackerNamespaceIfEmpty(suffixKey: string) {
		const byPeer = this.trackerBySuffixKey.get(suffixKey);
		if (byPeer && byPeer.size === 0) {
			this.trackerBySuffixKey.delete(suffixKey);
			this.trackerNamespaceLru.delete(suffixKey);
		}
	}

	private touchProviderNamespace(suffixKey: string, now = Date.now()) {
		if (!suffixKey) return;
		// LRU touch
		this.providerNamespaceLru.delete(suffixKey);
		this.providerNamespaceLru.set(suffixKey, now);
		while (this.providerNamespaceLru.size > PROVIDER_DIRECTORY_MAX_NAMESPACES) {
			const oldest = this.providerNamespaceLru.keys().next().value as string | undefined;
			if (!oldest) break;
			this.providerNamespaceLru.delete(oldest);
			this.providerBySuffixKey.delete(oldest);
			this.pendingProviderQueryBySuffixKey.delete(oldest);
		}
	}

	private pruneProviderNamespaceIfEmpty(suffixKey: string) {
		const byPeer = this.providerBySuffixKey.get(suffixKey);
		if (byPeer && byPeer.size === 0) {
			this.providerBySuffixKey.delete(suffixKey);
			this.providerNamespaceLru.delete(suffixKey);
			this.pendingProviderQueryBySuffixKey.delete(suffixKey);
		}
	}

	private getProviderNamespaceId(namespace: string): ProviderNamespaceId {
		const key = sha256Sync(textEncoder.encode(`provider|${namespace}`));
		const suffixKey = toBase64(key.subarray(0, 24));
		return { namespace, key, suffixKey };
	}

	public provide(
		namespace: string,
		options: FanoutProviderAnnounceOptions = {},
	): FanoutProviderHandle {
		if (!this.started) {
			throw new Error("FanoutTree must be started before providing");
		}

		const id = this.getProviderNamespaceId(namespace);
		const ttlMsRaw = Math.max(0, Math.floor(options.ttlMs ?? 120_000));
		const ttlMs = Math.min(ttlMsRaw, 120_000);
		const announceIntervalMsRaw =
			options.announceIntervalMs ?? Math.max(1_000, Math.floor(ttlMs / 2));
		const announceIntervalMs = Math.max(100, Math.floor(announceIntervalMsRaw));

		const bootstrapOverride =
			options.bootstrap && options.bootstrap.length > 0
				? options.bootstrap
						.map((a) => (typeof a === "string" ? multiaddr(a) : a))
						.filter((a) => Boolean(a))
				: undefined;

		const bootstrapDialTimeoutMs = Math.max(
			0,
			Math.floor(options.bootstrapDialTimeoutMs ?? 2_000),
		);
		const bootstrapMaxPeers = Math.max(
			0,
			Math.floor(options.bootstrapMaxPeers ?? 0),
		);

		let state = this.providerAnnounceBySuffixKey.get(id.suffixKey);
		if (!state) {
			state = {
				id,
				ttlMs,
				announceIntervalMs,
				bootstrapOverride,
				bootstrapDialTimeoutMs,
				bootstrapMaxPeers,
				closed: false,
			};
			this.providerAnnounceBySuffixKey.set(id.suffixKey, state);
			state.loop = this._providerAnnounceLoop(state).catch(() => {});
		} else {
			state.ttlMs = ttlMs;
			state.announceIntervalMs = announceIntervalMs;
			state.bootstrapOverride = bootstrapOverride;
			state.bootstrapDialTimeoutMs = bootstrapDialTimeoutMs;
			state.bootstrapMaxPeers = bootstrapMaxPeers;
		}

		return {
			close: () => {
				const current = this.providerAnnounceBySuffixKey.get(id.suffixKey);
				if (!current) return;
				current.closed = true;
				this.providerAnnounceBySuffixKey.delete(id.suffixKey);
				// Best-effort immediate withdrawal (ttl=0).
				void this.announceProviderOnce(current, this.closeController.signal, 0).catch(
					() => {},
				);
			},
		};
	}

	/**
	 * Announce provider presence once (no background loop).
	 *
	 * This is useful for "on-demand" discovery where the caller wants to publish a
	 * short-lived provider hint (e.g. after putting a block) without keeping a
	 * per-namespace timer alive.
	 */
	public async announceProvider(
		namespace: string,
		options: Omit<FanoutProviderAnnounceOptions, "announceIntervalMs"> = {},
	): Promise<void> {
		if (!this.started) {
			throw new Error("FanoutTree must be started before providing");
		}

		const id = this.getProviderNamespaceId(namespace);
		const ttlMsRaw = Math.max(0, Math.floor(options.ttlMs ?? 120_000));
		const ttlMs = Math.min(ttlMsRaw, 120_000);

		const bootstrapOverride =
			options.bootstrap && options.bootstrap.length > 0
				? options.bootstrap
						.map((a) => (typeof a === "string" ? multiaddr(a) : a))
						.filter((a) => Boolean(a))
				: undefined;

		const bootstrapDialTimeoutMs = Math.max(
			0,
			Math.floor(options.bootstrapDialTimeoutMs ?? 2_000),
		);
		const bootstrapMaxPeers = Math.max(
			0,
			Math.floor(options.bootstrapMaxPeers ?? 0),
		);

		const state: ProviderAnnounceState = {
			id,
			ttlMs,
			// ignored by `announceProviderOnce`
			announceIntervalMs: 0,
			bootstrapOverride,
			bootstrapDialTimeoutMs,
			bootstrapMaxPeers,
			closed: false,
		};

		await this.announceProviderOnce(state, this.closeController.signal, ttlMs);
	}

	private async _providerAnnounceLoop(state: ProviderAnnounceState): Promise<void> {
		const signal = this.closeController.signal;
		for (;;) {
			if (signal.aborted || state.closed) return;
			try {
				await this.announceProviderOnce(state, signal);
			} catch {
				// ignore
			}
			await delay(Math.max(50, state.announceIntervalMs));
		}
	}

	private async announceProviderOnce(
		state: ProviderAnnounceState,
		signal: AbortSignal,
		ttlOverrideMs?: number,
	): Promise<void> {
		const bootstraps = state.bootstrapOverride ?? this.bootstraps;
		if (bootstraps.length === 0) return;

		const peers = await this.ensureBootstrapPeers(
			bootstraps,
			state.bootstrapDialTimeoutMs,
			signal,
			state.bootstrapMaxPeers,
		);
		if (peers.length === 0) return;

		const ttlMs = ttlOverrideMs != null ? ttlOverrideMs : state.ttlMs;
		const addrs = this.getSelfAnnounceAddrs();
		const bytes = encodeProviderAnnounce(state.id.key, ttlMs, addrs);
		await this._sendControlMany(peers, bytes);
	}

	private nextProviderReqId(suffixKey: string): number {
		const pending = this.pendingProviderQueryBySuffixKey.get(suffixKey);
		let reqId = (this.random() * 0xffffffff) >>> 0;
		while (pending?.has(reqId)) {
			reqId = (this.random() * 0xffffffff) >>> 0;
		}
		return reqId;
	}

	public async queryProviderCandidates(
		namespace: string,
		options: FanoutProviderQueryOptions = {},
	): Promise<FanoutProviderCandidate[]> {
		if (!this.started) {
			throw new Error("FanoutTree must be started before querying providers");
		}

		const want = Math.max(0, Math.floor(options.want ?? 8));
		if (want === 0) return [];

		const id = this.getProviderNamespaceId(namespace);
		const seed = options.seed != null ? options.seed >>> 0 : 0;
		const cacheTtlMs = Math.max(0, Math.floor(options.cacheTtlMs ?? 60_000));

		const signal = options.signal
			? anySignal([this.closeController.signal, options.signal])
			: this.closeController.signal;

		try {
			const now = Date.now();
			this.touchProviderNamespace(id.suffixKey, now);

			const shuffleInPlace = (arr: FanoutProviderCandidate[], seed32: number) => {
				if (arr.length <= 1) return;
				if ((seed32 >>> 0) === 0) {
					for (let i = arr.length - 1; i > 0; i--) {
						const j = Math.floor(this.random() * (i + 1));
						const tmp = arr[i]!;
						arr[i] = arr[j]!;
						arr[j] = tmp;
					}
					return;
				}
				let x = seed32 >>> 0;
				for (let i = arr.length - 1; i > 0; i--) {
					// xorshift32
					x ^= x << 13;
					x ^= x >>> 17;
					x ^= x << 5;
					const j = (x >>> 0) % (i + 1);
					const tmp = arr[i]!;
					arr[i] = arr[j]!;
					arr[j] = tmp;
				}
			};

			const orderCandidates = (items: FanoutProviderCandidate[]) => {
				// Prefer already-connected providers, but still spread load within each group.
				const connected: FanoutProviderCandidate[] = [];
				const unconnected: FanoutProviderCandidate[] = [];
				for (const c of items) {
					(this.peers.get(c.hash) ? connected : unconnected).push(c);
				}
				shuffleInPlace(connected, seed);
				shuffleInPlace(unconnected, seed ? (seed ^ 0x9e3779b9) >>> 0 : 0);
				return connected.concat(unconnected).slice(0, want);
			};

			const cached: FanoutProviderCandidate[] = [];
			const byPeer = this.providerBySuffixKey.get(id.suffixKey);
			if (byPeer) {
				for (const [hash, e] of byPeer) {
					if (e.expiresAt <= now) {
						byPeer.delete(hash);
						continue;
					}
					if (hash === this.publicKeyHash) continue;
					const addrs: Multiaddr[] = [];
					for (const a of e.addrs) {
						try {
							addrs.push(multiaddr(a));
						} catch {
							// ignore invalid
						}
					}
					cached.push({ hash, addrs });
				}
				this.pruneProviderNamespaceIfEmpty(id.suffixKey);
			}

			// If the cache is warm, avoid tracker queries on hot paths.
			if (cached.length >= want) {
				return orderCandidates(cached);
			}

			const bootstrapOverride =
				options.bootstrap && options.bootstrap.length > 0
					? options.bootstrap
							.map((a) => (typeof a === "string" ? multiaddr(a) : a))
							.filter((a) => Boolean(a))
					: undefined;
			const bootstraps = bootstrapOverride ?? this.bootstraps;
			const dialTimeoutMs = Math.max(
				0,
				Math.floor(options.bootstrapDialTimeoutMs ?? 2_000),
			);
			const bootstrapMaxPeers = Math.max(
				0,
				Math.floor(options.bootstrapMaxPeers ?? 0),
			);
			const trackerPeers =
				bootstraps.length > 0
					? await this.ensureBootstrapPeers(
							bootstraps,
							dialTimeoutMs,
							signal,
							bootstrapMaxPeers,
						)
					: [];

			const perTrackerTimeout = Math.max(
				0,
				Math.floor(options.queryTimeoutMs ?? 1_000),
			);
			const overallTimeout = Math.max(0, Math.floor(options.timeoutMs ?? 5_000));
			const deadlineAt = overallTimeout > 0 ? Date.now() + overallTimeout : 0;

			const byReq =
				this.pendingProviderQueryBySuffixKey.get(id.suffixKey) ||
				new Map<number, { resolve: (providers: FanoutProviderCandidate[]) => void }>();
			this.pendingProviderQueryBySuffixKey.set(id.suffixKey, byReq);

			const results = await Promise.all(
				trackerPeers.map(async (trackerHash) => {
					const reqId = this.nextProviderReqId(id.suffixKey);
					const p = new Promise<FanoutProviderCandidate[]>((resolve) => {
						byReq.set(reqId, { resolve });
					});
					void this._sendControl(
						trackerHash,
						encodeProviderQuery(id.key, reqId, want, seed),
					);

					const remainingMs =
						deadlineAt > 0 ? Math.max(0, deadlineAt - Date.now()) : perTrackerTimeout;
					const timeoutMs =
						deadlineAt > 0 ? Math.min(perTrackerTimeout, remainingMs) : perTrackerTimeout;

					const res = await Promise.race([
						p,
						delay(timeoutMs, { signal }).then((): null => null),
					]);
					if (res == null) {
						byReq.delete(reqId);
						return [];
					}
					return res;
				}),
			);

			const merged: FanoutProviderCandidate[] = [...cached];
			for (const r of results) merged.push(...r);

			const seen = new Set<string>();
			const deduped = merged.filter((c) => {
				if (!c.hash) return false;
				if (c.hash === this.publicKeyHash) return false;
				if (seen.has(c.hash)) return false;
				seen.add(c.hash);
				return true;
			});

			// Cache (best-effort) to avoid repeated tracker lookups.
			if (cacheTtlMs > 0) {
				const exp = Date.now() + cacheTtlMs;
				let cache = this.providerBySuffixKey.get(id.suffixKey);
				if (!cache) {
					cache = new Map<string, ProviderEntry>();
					this.providerBySuffixKey.set(id.suffixKey, cache);
				}
				for (const c of deduped) {
					cache.delete(c.hash);
					cache.set(c.hash, {
						hash: c.hash,
						addrs: c.addrs.map((a) => a.bytes),
						expiresAt: exp,
					});
				}
				while (cache.size > PROVIDER_DIRECTORY_MAX_ENTRIES) {
					const oldest = cache.keys().next().value as string | undefined;
					if (!oldest) break;
					cache.delete(oldest);
				}
				this.touchProviderNamespace(id.suffixKey);
			}

			return orderCandidates(deduped);
		} finally {
			(signal as any)?.clear?.();
		}
	}

	public async queryProviders(
		namespace: string,
		options: FanoutProviderQueryOptions = {},
	): Promise<string[]> {
		const candidates = await this.queryProviderCandidates(namespace, options);
		return candidates.map((c) => c.hash);
	}

	public getChannelId(topic: string, root: string): FanoutTreeChannelId {
		const key = sha256Sync(textEncoder.encode(`fanout-tree|${root}|${topic}`));
		const suffixKey = toBase64(key.subarray(0, 24));
		return { topic, root, key, suffixKey };
	}

	public openChannel(topic: string, root: string, opts: FanoutTreeChannelOptions) {
		if (!this.started) {
			throw new Error("FanoutTree must be started before opening channels");
		}
		const id = this.getChannelId(topic, root);
		let ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (ch) return ch.id;

		const bidPerByte = Math.max(0, Math.floor(opts.bidPerByte ?? 0)) >>> 0;
		const uploadLimitBps = Math.max(0, Math.floor(opts.uploadLimitBps));
		const maxChildren = Math.max(0, Math.floor(opts.maxChildren));
		const msgRate = Math.max(1, Math.floor(opts.msgRate));
		const msgSize = Math.max(1, Math.floor(opts.msgSize));
		const uploadOverheadBytes = Math.max(
			0,
			Math.floor(opts.uploadOverheadBytes ?? this.defaultUploadOverheadBytes),
		);
		const perChildBytes = Math.max(1, 1 + msgSize + uploadOverheadBytes);
		const perChildBps = Math.max(1, Math.floor(msgRate * perChildBytes));
		const byBps = uploadLimitBps > 0 ? Math.floor(uploadLimitBps / perChildBps) : 0;
		const effectiveMaxChildren = Math.max(0, Math.min(maxChildren, byBps));
		const maxDataAgeMs = Math.max(0, Math.floor(opts.maxDataAgeMs ?? 0));
		const requestedRouteCacheMaxEntries = Math.max(
			0,
			Math.floor(opts.routeCacheMaxEntries ?? (opts.role === "root" ? 2_048 : 512)),
		);
		const routeCacheMaxEntries = Math.min(
			ROUTE_CACHE_MAX_ENTRIES_HARD_CAP,
			requestedRouteCacheMaxEntries,
		);
		const routeCacheTtlMs = Math.max(0, Math.floor(opts.routeCacheTtlMs ?? 10 * 60_000));
		const requestedPeerHintMaxEntries = Math.max(
			0,
			Math.floor(opts.peerHintMaxEntries ?? (opts.role === "root" ? 4_096 : 1_024)),
		);
		const peerHintMaxEntries = Math.min(
			PEER_HINT_MAX_ENTRIES_HARD_CAP,
			requestedPeerHintMaxEntries,
		);
		const peerHintTtlMs = Math.max(0, Math.floor(opts.peerHintTtlMs ?? 10 * 60_000));
		const uploadBurstMsDefault = Math.max(1, Math.ceil(1_000 / msgRate));
		const uploadBurstMs = Math.max(0, Math.floor(opts.uploadBurstMs ?? uploadBurstMsDefault));
		const uploadTokenCapacity =
			uploadLimitBps > 0 && uploadBurstMs > 0
				? Math.max(1, Math.floor((uploadLimitBps * uploadBurstMs) / 1_000))
				: 0;

		const repairEnabled = opts.repair !== false;
		const repairWindowMessages = Math.max(
			0,
			Math.floor(opts.repairWindowMessages ?? 1024),
		);
		const repairMaxBackfillMessagesRaw = Math.max(
			0,
			Math.floor(opts.repairMaxBackfillMessages ?? repairWindowMessages),
		);
		const repairMaxBackfillMessages =
			repairEnabled && repairWindowMessages > 0
				? Math.min(repairMaxBackfillMessagesRaw, repairWindowMessages)
				: 0;
		const repairIntervalMs = Math.max(0, Math.floor(opts.repairIntervalMs ?? 200));
		const repairMaxPerReq = Math.max(0, Math.floor(opts.repairMaxPerReq ?? 64));

		const neighborRepair = opts.neighborRepair === true;
		const neighborRepairPeers = Math.max(
			0,
			Math.floor(opts.neighborRepairPeers ?? 2),
		);
		const neighborMeshPeers = Math.max(
			0,
			Math.floor(opts.neighborMeshPeers ?? Math.max(0, neighborRepairPeers * 2)),
		);
		const neighborAnnounceIntervalMs = Math.max(
			0,
			Math.floor(opts.neighborAnnounceIntervalMs ?? 500),
		);
		const neighborMeshRefreshIntervalMs = Math.max(
			0,
			Math.floor(opts.neighborMeshRefreshIntervalMs ?? 2_000),
		);
		const neighborHaveTtlMs = Math.max(
			0,
			Math.floor(opts.neighborHaveTtlMs ?? 5_000),
		);
		const neighborRepairBudgetBps = Math.max(
			0,
			Math.floor(opts.neighborRepairBudgetBps ?? 0),
		);
		const neighborRepairBurstMs = Math.max(
			0,
			Math.floor(opts.neighborRepairBurstMs ?? 1_000),
		);
		const neighborRepairTokenCapacity =
			neighborRepairBudgetBps > 0 && neighborRepairBurstMs > 0
				? Math.max(
						1,
						Math.floor((neighborRepairBudgetBps * neighborRepairBurstMs) / 1_000),
					)
				: 0;

		const proxyPublishBudgetBps = Math.max(
			0,
			Math.floor(opts.proxyPublishBudgetBps ?? perChildBps),
		);
		const proxyPublishBurstMs = Math.max(
			0,
			Math.floor(opts.proxyPublishBurstMs ?? 1_000),
		);
		const proxyPublishTokenCapacity =
			proxyPublishBudgetBps > 0 && proxyPublishBurstMs > 0
				? Math.max(
						1,
						Math.floor((proxyPublishBudgetBps * proxyPublishBurstMs) / 1_000),
					)
				: 0;

		const unicastBudgetBps = Math.max(
			0,
			Math.floor(opts.unicastBudgetBps ?? perChildBps),
		);
		const unicastBurstMs = Math.max(0, Math.floor(opts.unicastBurstMs ?? 1_000));
		const unicastTokenCapacity =
			unicastBudgetBps > 0 && unicastBurstMs > 0
				? Math.max(1, Math.floor((unicastBudgetBps * unicastBurstMs) / 1_000))
				: 0;

				ch = {
					id,
					metrics: this.getMetricsForSuffixKey(id.suffixKey),
					level: opts.role === "root" ? 0 : Number.POSITIVE_INFINITY,
					isRoot: opts.role === "root",
					closed: false,
					parent: undefined,
					children: new Map(),
					rejoinCooldownUntil: 0,
					joinedAtLeastOnce: opts.role === "root",
					routeFromRoot: opts.role === "root" ? [this.publicKeyHash] : undefined,
					routeByPeer: new Map(),
					seq: 0,
				bidPerByte,
			uploadLimitBps,
			maxChildren,
			effectiveMaxChildren,
			allowKick: opts.allowKick === true,
			maxDataAgeMs,
			repairEnabled,
			repairWindowMessages,
			repairMaxBackfillMessages,
			repairIntervalMs,
			repairMaxPerReq,
			neighborRepair,
			neighborRepairPeers,
			neighborMeshPeers,
			neighborAnnounceIntervalMs,
			neighborMeshRefreshIntervalMs,
			neighborHaveTtlMs,
			neighborRepairBudgetBps,
			neighborRepairTokenCapacity,
			neighborRepairTokens: neighborRepairTokenCapacity,
			neighborRepairLastRefillAt: Date.now(),
			proxyPublishBudgetBps,
			proxyPublishTokenCapacity,
			proxyPublishTokensByPeer: new Map(),
			unicastBudgetBps,
			unicastTokenCapacity,
			unicastTokensByPeer: new Map(),
			uploadOverheadBytes,
			uploadBurstMs,
			uploadTokenCapacity,
			uploadTokens: uploadTokenCapacity,
			uploadLastRefillAt: Date.now(),
			droppedForwards: 0,
			overloadStreak: 0,
			lastOverloadKickAt: 0,
			dataWriteFailStreakByChild: new Map(),
			lastDataWriteFailKickAt: 0,
			cacheSeqs:
				repairEnabled && repairWindowMessages > 0
					? new Int32Array(repairWindowMessages).fill(-1)
					: undefined,
			cachePayloads:
				repairEnabled && repairWindowMessages > 0
					? new Array<Uint8Array | undefined>(repairWindowMessages)
					: undefined,
			nextExpectedSeq: 0,
			missingSeqs: new Set<number>(),
			endSeqExclusive: -1,
			lastRepairSentAt: 0,
			lastParentDataAt: 0,
			receivedAnyParentData: false,
			channelPeers: new Map<string, number>(),
			knownCandidateAddrs: new Map<string, Multiaddr[]>(),
			lazyPeers: new Set<string>(),
			haveByPeer: new Map(),
			maxSeqSeen: -1,
			lastIHaveSentAt: 0,
			lastIHaveSentMaxSeq: -1,
			pendingJoin: new Map(),
			pendingTrackerQuery: new Map(),
				pendingRouteQuery: new Map(),
				pendingUnicastAck: new Map(),
				pendingRouteProxy: new Map(),
				bootstrapOverride: undefined,
				bootstrapDialTimeoutMs: 10_000,
				bootstrapMaxPeers: 0,
			bootstrapEnsureIntervalMs: 5_000,
			cachedBootstrapPeers: [],
			lastBootstrapEnsureAt: 0,
			announceIntervalMs: 2_000,
			announceTtlMs: 10_000,
			lastAnnouncedAt: 0,
			peerHintMaxEntries,
			peerHintTtlMs,
			routeCacheMaxEntries,
			routeCacheTtlMs,
			trackerQueryIntervalMs: 2_000,
			cachedTrackerCandidates: [],
			lastTrackerQueryAt: 0,
			};
		this.channelsBySuffixKey.set(id.suffixKey, ch);
		const needsAnnounceLoop = ch.effectiveMaxChildren > 0;
		if (needsAnnounceLoop) {
			ch.announceLoop = this._announceLoop(ch).catch(() => {});
		}
		if (ch.repairEnabled && !ch.isRoot && ch.repairIntervalMs >= 0) {
			ch.repairLoop = this._repairLoop(ch).catch(() => {});
		}
		if (
			ch.repairEnabled &&
			ch.neighborRepair &&
			!ch.isRoot &&
			ch.neighborMeshPeers > 0
		) {
			ch.meshLoop = this._meshLoop(ch).catch(() => {});
		}
			return id;
		}

	private abortPendingUnicastAcks(ch: ChannelState, error: AbortError) {
		for (const pending of ch.pendingUnicastAck.values()) {
			try {
				clearTimeout(pending.timer);
			} catch {
				// ignore
			}
			try {
				if (pending.signal && pending.onAbort) {
					pending.signal.removeEventListener("abort", pending.onAbort);
				}
			} catch {
				// ignore
			}
			try {
				pending.reject(error);
			} catch {
				// ignore
			}
		}
		ch.pendingUnicastAck.clear();
	}

	private detachFromParent(ch: ChannelState) {
		ch.parent = undefined;
		ch.level = Number.POSITIVE_INFINITY;
		ch.routeFromRoot = undefined;
		ch.routeByPeer.clear();
		ch.lastParentDataAt = 0;
		ch.receivedAnyParentData = false;
		ch.pendingJoin.clear();
		ch.pendingRouteQuery.clear();
		this.abortPendingUnicastAcks(ch, new AbortError("fanout channel detached"));
		for (const pending of ch.pendingRouteProxy.values()) {
			clearTimeout(pending.timer);
		}
		ch.pendingRouteProxy.clear();
	}

	private onPeerDisconnectedFromUnderlay(peerHash: string) {
		if (!peerHash) return;

		// Detach from a disconnected parent immediately, so children can rejoin.
		// This is more reliable than polling `getConnections()` because the underlay
		// can flap/reconnect faster than the join loop cadence.
		const now = Date.now();
		for (const ch of this.channelsBySuffixKey.values()) {
			if (!ch.parent) continue;
			if (ch.parent !== peerHash) continue;
			if (ch.closed) continue;

			ch.metrics.reparentDisconnect += 1;
			const hadChildren = ch.children.size > 0;
			this.detachFromParent(ch);
			void this.kickChildren(ch).catch(() => {});
			if (hadChildren) {
				ch.rejoinCooldownUntil = Math.max(
					ch.rejoinCooldownUntil,
					now + RELAY_REJOIN_COOLDOWN_MS,
				);
			}
		}

		// Also prune disconnected children from rooted nodes to free capacity fast.
		for (const ch of this.channelsBySuffixKey.values()) {
			if (ch.children.delete(peerHash)) {
				ch.dataWriteFailStreakByChild.delete(peerHash);
			}
		}
	}

	public async closeChannel(
		topic: string,
		root: string,
		options?: { notifyParent?: boolean; kickChildren?: boolean },
	): Promise<void> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) return;
		if (ch.closed) return;
		ch.closed = true;

		// If a join is in-flight, surface that it won't complete.
		try {
			ch.joinedOnce?.reject(new AbortError("fanout channel closed"));
		} catch {
			// ignore
		}
		ch.joinedOnce = undefined;

		const notifyParent = options?.notifyParent !== false;
		const kickChildren = options?.kickChildren !== false;

		const pendingSends: Array<Promise<void>> = [];
		if (notifyParent && !ch.isRoot && ch.parent) {
			pendingSends.push(
				this._sendControl(ch.parent, encodeLeave(ch.id.key)).catch(() => {}),
			);
		}
		if (kickChildren && ch.children.size > 0) {
			pendingSends.push(this.kickChildren(ch).catch(() => {}));
		}

		ch.pendingJoin.clear();
		ch.pendingTrackerQuery.clear();
		ch.pendingRouteQuery.clear();
		this.abortPendingUnicastAcks(ch, new AbortError("fanout channel closed"));
		for (const pending of ch.pendingRouteProxy.values()) {
			clearTimeout(pending.timer);
		}
		ch.pendingRouteProxy.clear();

		ch.parent = undefined;
		ch.children.clear();
		ch.lazyPeers.clear();
		ch.haveByPeer.clear();
		ch.knownCandidateAddrs.clear();
		ch.channelPeers.clear();
		ch.routeFromRoot = undefined;
		ch.routeByPeer.clear();

		this.channelsBySuffixKey.delete(id.suffixKey);
		this.trackerBySuffixKey.delete(id.suffixKey);

		await Promise.all(pendingSends);
	}

	public getChannelStats(topic: string, root: string):
		| {
				topic: string;
				root: string;
				parent?: string;
				level: number;
				children: number;
				effectiveMaxChildren: number;
					uploadLimitBps: number;
					droppedForwards: number;
					peerHintEntries: number;
					peerHintMaxEntries: number;
					routeCacheEntries: number;
					routeCacheMaxEntries: number;
			  }
			| undefined {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) return;
		return {
			topic,
			root,
			parent: ch.parent,
			level: ch.level,
			children: ch.children.size,
				effectiveMaxChildren: ch.effectiveMaxChildren,
				uploadLimitBps: ch.uploadLimitBps,
				droppedForwards: ch.droppedForwards,
				peerHintEntries: ch.channelPeers.size,
				peerHintMaxEntries: ch.peerHintMaxEntries,
				routeCacheEntries: ch.routeByPeer.size,
				routeCacheMaxEntries: ch.routeCacheMaxEntries,
			};
		}

	public getChannelMetrics(topic: string, root: string): FanoutTreeChannelMetrics {
		const id = this.getChannelId(topic, root);
		return this.getMetricsForSuffixKey(id.suffixKey);
	}

	public getChannelPeerHashes(
		topic: string,
		root: string,
		options?: { includeSelf?: boolean },
	): string[] {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) return [];

		const includeSelf = options?.includeSelf === true;
		const peers = new Set<string>();

			if (ch.parent) peers.add(ch.parent);
			for (const child of ch.children.keys()) peers.add(child);
			for (const peer of ch.channelPeers.keys()) peers.add(peer);
			if (ch.routeFromRoot) {
				for (const peer of ch.routeFromRoot) peers.add(peer);
			}

		if (includeSelf) {
			peers.add(this.publicKeyHash);
		} else {
			peers.delete(this.publicKeyHash);
		}

		return [...peers];
	}

	public async joinChannel(
		topic: string,
		root: string,
		channelOpts: Omit<FanoutTreeChannelOptions, "role">,
		joinOpts: FanoutTreeJoinOptions = {},
	): Promise<void> {
		await ready;
		const id = this.openChannel(topic, root, { ...channelOpts, role: "node" });
		const ch = this.channelsBySuffixKey.get(id.suffixKey)!;
		if (ch.isRoot) return;

		if (joinOpts.bootstrap) {
			ch.bootstrapOverride = joinOpts.bootstrap.map((a) =>
				typeof a === "string" ? multiaddr(a) : a,
			);
		}
		if (joinOpts.bootstrapDialTimeoutMs != null) {
			ch.bootstrapDialTimeoutMs = Math.max(
				0,
				Math.floor(joinOpts.bootstrapDialTimeoutMs),
			);
		}
		if (joinOpts.bootstrapMaxPeers != null) {
			ch.bootstrapMaxPeers = Math.max(0, Math.floor(joinOpts.bootstrapMaxPeers));
		}
		if (joinOpts.announceIntervalMs != null) {
			ch.announceIntervalMs = Math.max(0, Math.floor(joinOpts.announceIntervalMs));
		}
		if (joinOpts.announceTtlMs != null) {
			ch.announceTtlMs = Math.max(0, Math.floor(joinOpts.announceTtlMs));
		}
		if (joinOpts.bootstrapEnsureIntervalMs != null) {
			ch.bootstrapEnsureIntervalMs = Math.max(
				0,
				Math.floor(joinOpts.bootstrapEnsureIntervalMs),
			);
		}
		if (joinOpts.trackerQueryIntervalMs != null) {
			ch.trackerQueryIntervalMs = Math.max(
				0,
				Math.floor(joinOpts.trackerQueryIntervalMs),
			);
		}

		if (!ch.joinedOnce) ch.joinedOnce = createDeferred();
		if (!ch.joinLoop) {
			ch.joinLoop = this._joinLoop(ch, joinOpts).catch((err) => {
				// Surface join errors to the caller without crashing the process
				// via an unhandled rejection (joinLoop is not generally awaited).
				ch.joinedOnce?.reject(err);
			});
		}
		return ch.joinedOnce.promise;
	}

	/**
	 * Returns this node's current route token for a channel, if attached.
	 *
	 * The token is a source-route path `[root, ..., self]` and can be shared
	 * out-of-band to allow economical unicast within the channel.
	 */
	public getRouteToken(topic: string, root: string): string[] | undefined {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch?.routeFromRoot || ch.routeFromRoot.length === 0) return undefined;
		return [...ch.routeFromRoot];
	}

	public getRouteHint(
		topic: string,
		root: string,
		targetHash: string,
	): FanoutRouteTokenHint | undefined {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) return undefined;

		if (targetHash === this.publicKeyHash && ch.routeFromRoot?.length) {
			return {
				kind: "fanout-token",
				root,
				target: targetHash,
				route: [...ch.routeFromRoot],
				updatedAt: Date.now(),
			};
		}
		if (ch.isRoot && ch.children.has(targetHash)) {
			return {
				kind: "fanout-token",
				root,
				target: targetHash,
				route: [root, targetHash],
				updatedAt: Date.now(),
			};
		}

		const route = this.getCachedRoute(ch, targetHash);
		if (!route) return undefined;
		const entry = ch.routeByPeer.get(targetHash);
		const updatedAt = entry?.updatedAt ?? Date.now();

		return {
			kind: "fanout-token",
			root,
			target: targetHash,
			route,
			updatedAt,
			expiresAt:
				ch.routeCacheTtlMs > 0 ? updatedAt + ch.routeCacheTtlMs : undefined,
		};
	}

	private cacheRoute(ch: ChannelState, route: string[]) {
		if (!route?.length) return;
		if (route[0] !== ch.id.root) return;
		const target = route[route.length - 1];
		if (!target) return;
		if (target === this.publicKeyHash) return;
		if (ch.routeCacheMaxEntries <= 0) return;
		if (ch.isRoot && route.length === 2 && ch.children.has(target)) return;
		const now = Date.now();
		ch.routeByPeer.delete(target);
		ch.routeByPeer.set(target, { route: [...route], updatedAt: now });
		this.pruneRouteCache(ch, now);
	}

	private cacheKnownCandidateAddrs(
		ch: ChannelState,
		hash: string,
		addrs: Multiaddr[],
	) {
		if (!hash) return;
		if (!addrs || addrs.length === 0) return;
		// We only need a handful of addresses to dial a candidate.
		const limited =
			addrs.length > JOIN_REJECT_REDIRECT_ADDR_MAX
				? addrs.slice(0, JOIN_REJECT_REDIRECT_ADDR_MAX)
				: addrs;
		ch.knownCandidateAddrs.delete(hash);
		ch.knownCandidateAddrs.set(hash, limited);
		while (ch.knownCandidateAddrs.size > KNOWN_CANDIDATE_ADDRS_MAX_ENTRIES) {
			const oldest = ch.knownCandidateAddrs.keys().next()
				.value as string | undefined;
			if (!oldest) break;
			ch.knownCandidateAddrs.delete(oldest);
		}
	}

	private touchPeerHint(ch: ChannelState, peerHash: string, now = Date.now()) {
		if (!peerHash) return;
		if (peerHash === this.publicKeyHash) return;
		if (ch.peerHintMaxEntries <= 0) return;
		// LRU touch
		ch.channelPeers.delete(peerHash);
		ch.channelPeers.set(peerHash, now);
		this.prunePeerHints(ch, now);
	}

	private prunePeerHints(ch: ChannelState, now = Date.now()) {
		if (ch.channelPeers.size === 0) return;
		if (ch.peerHintMaxEntries <= 0) {
			ch.channelPeers.clear();
			return;
		}

		// `channelPeers` is maintained as an LRU (touch via delete+set), so the oldest
		// entry is always first. This makes TTL pruning O(expired) rather than O(N).
		if (ch.peerHintTtlMs > 0) {
			for (;;) {
				const oldest = ch.channelPeers.keys().next().value as string | undefined;
				if (!oldest) break;
				const seenAt = ch.channelPeers.get(oldest) ?? 0;
				if (now - seenAt <= ch.peerHintTtlMs) break;
				ch.channelPeers.delete(oldest);
			}
		}

		while (ch.channelPeers.size > ch.peerHintMaxEntries) {
			const oldest = ch.channelPeers.keys().next().value as string | undefined;
			if (!oldest) break;
			ch.channelPeers.delete(oldest);
		}
	}

	private pruneRouteCache(ch: ChannelState, now = Date.now()) {
		if (ch.routeByPeer.size === 0) return;
		if (ch.routeCacheMaxEntries <= 0) {
			const removed = ch.routeByPeer.size;
			ch.routeByPeer.clear();
			if (removed > 0) ch.metrics.routeCacheEvictions += removed;
			return;
		}

		// `routeByPeer` is maintained as an LRU (touch via delete+set), so the oldest
		// entry is always first. This makes TTL pruning O(expired) rather than O(N).
		if (ch.routeCacheTtlMs > 0) {
			for (;;) {
				const oldest = ch.routeByPeer.keys().next().value as string | undefined;
				if (!oldest) break;
				const entry = ch.routeByPeer.get(oldest);
				if (!entry) {
					ch.routeByPeer.delete(oldest);
					continue;
				}
				if (now - entry.updatedAt <= ch.routeCacheTtlMs) break;
				ch.routeByPeer.delete(oldest);
				ch.metrics.routeCacheExpirations += 1;
			}
		}

		while (ch.routeByPeer.size > ch.routeCacheMaxEntries) {
			const oldest = ch.routeByPeer.keys().next().value as string | undefined;
			if (!oldest) break;
			ch.routeByPeer.delete(oldest);
			ch.metrics.routeCacheEvictions += 1;
		}
	}

	private getCachedRoute(ch: ChannelState, targetHash: string): string[] | undefined {
		this.pruneRouteCache(ch);
		const entry = ch.routeByPeer.get(targetHash);
		if (!entry) {
			ch.metrics.routeCacheMisses += 1;
			return undefined;
		}
		const now = Date.now();
		if (ch.routeCacheTtlMs > 0 && now - entry.updatedAt > ch.routeCacheTtlMs) {
			ch.routeByPeer.delete(targetHash);
			ch.metrics.routeCacheMisses += 1;
			ch.metrics.routeCacheExpirations += 1;
			return undefined;
		}
		if (!this.isRouteValidForChannel(ch, entry.route)) {
			ch.routeByPeer.delete(targetHash);
			ch.metrics.routeCacheMisses += 1;
			return undefined;
		}
		// LRU touch
		ch.routeByPeer.delete(targetHash);
		ch.routeByPeer.set(targetHash, { route: entry.route, updatedAt: now });
		ch.metrics.routeCacheHits += 1;
		return [...entry.route];
	}

	private isRouteValidForChannel(ch: ChannelState, route?: string[]) {
		if (!route || route.length === 0) return false;
		if (route[0] !== ch.id.root) return false;
		// Root must be able to forward the first downstream hop immediately.
		if (ch.isRoot && route.length > 1 && !ch.children.has(route[1]!)) return false;
		return true;
	}

	private nextReqId(ch: ChannelState): number {
		let reqId = (this.random() * 0xffffffff) >>> 0;
		while (
			ch.pendingJoin.has(reqId) ||
			ch.pendingTrackerQuery.has(reqId) ||
			ch.pendingRouteQuery.has(reqId) ||
			ch.pendingRouteProxy.has(reqId)
		) {
			reqId = (this.random() * 0xffffffff) >>> 0;
		}
		return reqId;
	}

	private completeRouteProxy(ch: ChannelState, proxyReqId: number, route?: string[]) {
		const proxy = ch.pendingRouteProxy.get(proxyReqId);
		if (!proxy) return;
		ch.pendingRouteProxy.delete(proxyReqId);
		clearTimeout(proxy.timer);
		if (proxy.localResolve) {
			proxy.localResolve(route);
			return;
		}
		void this._sendControl(
			proxy.requester,
			encodeRouteReply(ch.id.key, proxy.downstreamReqId, route),
		).catch(() => {});
	}

	private proxyRouteQuery(
		ch: ChannelState,
		requester: string,
		downstreamReqId: number,
		targetHash: string,
		candidates: string[],
		timeoutMs = ROUTE_PROXY_TIMEOUT_MS,
	) {
		const unique: string[] = [];
		const seen = new Set<string>();
		for (const candidate of candidates) {
			if (!candidate || seen.has(candidate)) continue;
			seen.add(candidate);
			const stream = this.peers.get(candidate);
			if (!stream || !stream.isWritable) continue;
			unique.push(candidate);
		}

		if (unique.length === 0) {
			void this._sendControl(requester, encodeRouteReply(ch.id.key, downstreamReqId)).catch(
				() => {},
			);
			return;
		}
		ch.metrics.routeProxyQueries += 1;
		ch.metrics.routeProxyFanout += unique.length;

		const proxyReqId = this.nextReqId(ch);
		const timer = setTimeout(() => {
			ch.metrics.routeProxyTimeouts += 1;
			this.completeRouteProxy(ch, proxyReqId);
		}, Math.max(1, timeoutMs));
		ch.pendingRouteProxy.set(proxyReqId, {
			requester,
			downstreamReqId,
			timer,
			expectedReplies: new Set(unique),
		});

		void this._sendControlMany(unique, encodeRouteQuery(ch.id.key, proxyReqId, targetHash))
			.catch(() => {
				this.completeRouteProxy(ch, proxyReqId);
			});
	}

	public async resolveRouteToken(
		topic: string,
		root: string,
		targetHash: string,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	): Promise<string[] | undefined> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) throw new Error(`Channel not open: ${topic} (${root})`);
		if (!targetHash) throw new Error("targetHash is required");

		const cached = this.getCachedRoute(ch, targetHash);
		if (cached) {
			return cached;
		}

		if (targetHash === this.publicKeyHash && ch.routeFromRoot) {
			return [...ch.routeFromRoot];
		}

		if (ch.isRoot) {
			if (ch.children.has(targetHash)) {
				const route = [ch.id.root, targetHash];
				this.cacheRoute(ch, route);
				return route;
			}
			const candidates = [...ch.children.keys()];
			if (candidates.length === 0) return undefined;

			const timeoutMs = Math.max(1, Math.floor(options?.timeoutMs ?? 3_000));
			return await new Promise<string[] | undefined>((resolve, reject) => {
				let settled = false;
				const onAbort = () => {
					if (settled) return;
					settled = true;
					ch.pendingRouteProxy.delete(proxyReqId);
					clearTimeout(timer);
					reject(new AbortError());
				};

				const unique: string[] = [];
				const seen = new Set<string>();
				for (const candidate of candidates) {
					if (!candidate || seen.has(candidate)) continue;
					seen.add(candidate);
					const stream = this.peers.get(candidate);
					if (!stream || !stream.isWritable) continue;
					unique.push(candidate);
				}

				if (unique.length === 0) {
					resolve(undefined);
					return;
				}

				ch.metrics.routeProxyQueries += 1;
				ch.metrics.routeProxyFanout += unique.length;

				const proxyReqId = this.nextReqId(ch);
				const timer = setTimeout(() => {
					ch.metrics.routeProxyTimeouts += 1;
					this.completeRouteProxy(ch, proxyReqId);
				}, timeoutMs);

				if (options?.signal) {
					options.signal.addEventListener("abort", onAbort, { once: true });
				}

				ch.pendingRouteProxy.set(proxyReqId, {
					requester: this.publicKeyHash,
					downstreamReqId: 0,
					timer,
					expectedReplies: new Set(unique),
					localResolve: (route?: string[]) => {
						if (settled) return;
						settled = true;
						clearTimeout(timer);
						if (options?.signal) {
							options.signal.removeEventListener("abort", onAbort);
						}
						if (this.isRouteValidForChannel(ch, route)) {
							this.cacheRoute(ch, route!);
							resolve([...route!]);
							return;
						}
						resolve(undefined);
					},
				});

				void this._sendControlMany(unique, encodeRouteQuery(ch.id.key, proxyReqId, targetHash))
					.catch(() => {
						this.completeRouteProxy(ch, proxyReqId);
					});
			});
		}

		if (!ch.parent) {
			throw new Error("Cannot resolve route while not attached to a parent");
		}

		const reqId = this.nextReqId(ch);
		const timeoutMs = Math.max(1, Math.floor(options?.timeoutMs ?? 3_000));
		return await new Promise<string[] | undefined>((resolve, reject) => {
			let settled = false;
			const onAbort = () => {
				if (settled) return;
				settled = true;
				ch.pendingRouteQuery.delete(reqId);
				reject(new AbortError());
			};
			const timer = setTimeout(() => {
				if (settled) return;
				settled = true;
				ch.pendingRouteQuery.delete(reqId);
				resolve(undefined);
			}, timeoutMs);
			if (options?.signal) {
				options.signal.addEventListener("abort", onAbort, { once: true });
			}
			ch.pendingRouteQuery.set(reqId, {
				resolve: (route?: string[]) => {
					if (settled) return;
					settled = true;
					clearTimeout(timer);
					if (options?.signal) {
						options.signal.removeEventListener("abort", onAbort);
					}
					ch.pendingRouteQuery.delete(reqId);
					if (this.isRouteValidForChannel(ch, route)) {
						this.cacheRoute(ch, route!);
						resolve([...route!]);
						return;
					}
					resolve(undefined);
				},
			});
			void this._sendControl(ch.parent!, encodeRouteQuery(ch.id.key, reqId, targetHash))
				.catch((error) => {
					if (settled) return;
					settled = true;
					clearTimeout(timer);
					if (options?.signal) {
						options.signal.removeEventListener("abort", onAbort);
					}
					ch.pendingRouteQuery.delete(reqId);
					reject(error);
				});
		});
	}

	private nextUnicastAckToken(): bigint {
		const seq = this.unicastAckSeq >>> 0;
		this.unicastAckSeq = (this.unicastAckSeq + 1) >>> 0;
		return (BigInt(this.unicastAckNodeTag32) << 32n) | BigInt(seq);
	}

	public async unicastTo(
		topic: string,
		root: string,
		targetHash: string,
		payload: Uint8Array,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	) {
		const route = await this.resolveRouteToken(topic, root, targetHash, options);
		if (!route) {
			throw new Error(`No route token available for target ${targetHash}`);
		}
		return this.unicast(topic, root, route, payload);
	}

	public async unicastToAck(
		topic: string,
		root: string,
		targetHash: string,
		payload: Uint8Array,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	): Promise<void> {
		const route = await this.resolveRouteToken(topic, root, targetHash, options);
		if (!route) {
			throw new Error(`No route token available for target ${targetHash}`);
		}
		return this.unicastAck(topic, root, route, targetHash, payload, options);
	}

	public async unicastAck(
		topic: string,
		root: string,
		toRoute: string[],
		targetHash: string,
		payload: Uint8Array,
		options?: { timeoutMs?: number; signal?: AbortSignal },
	): Promise<void> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) throw new Error(`Channel not open: ${topic} (${root})`);
		if (!Array.isArray(toRoute) || toRoute.length === 0) {
			throw new Error("Invalid unicast route token");
		}
		if (toRoute[0] !== ch.id.root) {
			throw new Error("Unicast route token root mismatch");
		}

		const target = toRoute[toRoute.length - 1]!;
		if (targetHash && targetHash !== target) {
			throw new Error("Unicast route token target mismatch");
		}
		if (target === this.publicKeyHash) {
			const wire = encodeUnicast(ch.id.key, toRoute, payload);
			const message = await this.createMessage(wire, {
				mode: new AnyWhere(),
				priority: CONTROL_PRIORITY,
			} as any);
			this.dispatchEvent(
				new CustomEvent("fanout:unicast", {
					detail: {
						topic: ch.id.topic,
						root: ch.id.root,
						route: [...toRoute],
						payload,
						from: this.publicKeyHash,
						origin: this.publicKeyHash,
						to: target,
						timestamp: message.header.timestamp,
						message,
					},
				}),
			);
			return;
		}

		const timeoutMs = Math.max(
			1,
			Math.floor(options?.timeoutMs ?? UNICAST_ACK_DEFAULT_TIMEOUT_MS),
		);
		const signal = options?.signal
			? anySignal([this.closeController.signal, options.signal])
			: this.closeController.signal;

		const ackToken = this.nextUnicastAckToken();
		const replyRoute = ch.routeFromRoot;
		if (!replyRoute || replyRoute.length === 0) {
			throw new Error("Cannot unicast with ACK without a route token to self");
		}
		if (replyRoute[0] !== ch.id.root) {
			throw new Error("Cannot unicast with ACK: self route token root mismatch");
		}
		const selfHash = this.publicKeyHash;
		if (replyRoute[replyRoute.length - 1] !== selfHash) {
			throw new Error("Cannot unicast with ACK: self route token target mismatch");
		}

		const data = encodeUnicast(ch.id.key, toRoute, payload, {
			ackToken,
			replyRoute,
		});

		return await new Promise<void>((resolve, reject) => {
			let settled = false;
			let timer: ReturnType<typeof setTimeout> | undefined;

			const cleanup = () => {
				if (timer != null) {
					clearTimeout(timer);
					timer = undefined;
				}
				const pending = ch.pendingUnicastAck.get(ackToken);
				if (pending?.signal && pending.onAbort) {
					pending.signal.removeEventListener("abort", pending.onAbort);
				}
				ch.pendingUnicastAck.delete(ackToken);
			};

			const settleOk = () => {
				if (settled) return;
				settled = true;
				cleanup();
				resolve();
			};

			const settleErr = (error: unknown) => {
				if (settled) return;
				settled = true;
				cleanup();
				reject(error);
			};

			const onAbort = () => {
				settleErr(new AbortError());
			};

			if (signal.aborted) {
				onAbort();
				return;
			}
			signal.addEventListener("abort", onAbort);

			timer = setTimeout(() => {
				settleErr(new TimeoutError("Timeout waiting for unicast ACK"));
			}, timeoutMs);

			ch.pendingUnicastAck.set(ackToken, {
				expectedOrigin: targetHash,
				resolve: settleOk,
				reject: settleErr,
				timer,
				signal,
				onAbort,
			});

			void (async () => {
				try {
					if (ch.isRoot) {
						const nextHop = toRoute[1];
						if (!nextHop) {
							throw new Error("Unicast route token missing first hop");
						}
						if (!ch.children.has(nextHop)) {
							throw new Error(
								"Unicast first hop is not a direct child of the root",
							);
						}
						await this._sendControl(nextHop, data);
						return;
					}

					if (!ch.parent) {
						throw new Error("Cannot unicast while not attached to a parent");
					}
					await this._sendControl(ch.parent, data);
				} catch (error) {
					settleErr(error);
				}
			})();
		});
	}

	/**
	 * Economical unicast within an existing fanout channel.
	 *
	 * Any sender can send to `toRoute` by forwarding the message *up* to the root,
	 * and then letting the root forward it *down* the provided route.
	 *
	 * `toRoute` must be a route token `[root, ..., target]`.
	 */
	public async unicast(
		topic: string,
		root: string,
		toRoute: string[],
		payload: Uint8Array,
	): Promise<void> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) throw new Error(`Channel not open: ${topic} (${root})`);
		if (!Array.isArray(toRoute) || toRoute.length === 0) {
			throw new Error("Invalid unicast route token");
		}
		if (toRoute[0] !== ch.id.root) {
			throw new Error("Unicast route token root mismatch");
		}

		const data = encodeUnicast(ch.id.key, toRoute, payload);

		if (ch.isRoot) {
			const target = toRoute[toRoute.length - 1]!;
			if (target === this.publicKeyHash) {
				const message = await this.createMessage(data, {
					mode: new AnyWhere(),
					priority: CONTROL_PRIORITY,
				} as any);
				this.dispatchEvent(
					new CustomEvent("fanout:unicast", {
						detail: {
							topic: ch.id.topic,
							root: ch.id.root,
							route: [...toRoute],
							payload,
							from: this.publicKeyHash,
							origin: this.publicKeyHash,
							to: target,
							timestamp: message.header.timestamp,
							message,
						},
					}),
				);
				return;
			}
			const nextHop = toRoute[1];
			if (!nextHop) {
				throw new Error("Unicast route token missing first hop");
			}
			if (!ch.children.has(nextHop)) {
				throw new Error("Unicast first hop is not a direct child of the root");
			}
			await this._sendControl(nextHop, data);
			return;
		}

		if (!ch.parent) {
			throw new Error("Cannot unicast while not attached to a parent");
		}
		await this._sendControl(ch.parent, data);
	}

	public async publishData(topic: string, root: string, payload: Uint8Array): Promise<void> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) throw new Error(`Channel not open: ${topic} (${root})`);
		if (!ch.isRoot) throw new Error("Only the channel root can publish");
		const seq = ch.seq++;
		const message = await this._sendData(ch, [...ch.children.keys()], seq, payload);
		this.dispatchEvent(
			new CustomEvent("fanout:data", {
				detail: {
					topic: ch.id.topic,
					root: ch.id.root,
					seq,
					payload,
					from: this.publicKeyHash,
					origin: this.publicKeyHash,
					timestamp: message.header.timestamp,
					message,
				},
			}),
		);
	}

	/**
	 * Publishes payload to all channel members.
	 *
	 * Root publishes directly on the data-plane. Non-root members proxy the publish
	 * upstream to the root, which assigns a sequence number and broadcasts.
	 */
	public async publishToChannel(
		topic: string,
		root: string,
		payload: Uint8Array,
	): Promise<void> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) throw new Error(`Channel not open: ${topic} (${root})`);
		if (ch.isRoot) {
			return this.publishData(topic, root, payload);
		}
		if (!ch.parent) {
			// Channels can temporarily detach/re-parent under churn. Proxy publishes should
			// wait briefly for the join loop to attach instead of hard-throwing (which
			// can surface as unhandled rejections in higher layers like pubsub/RPC).
			await this.waitForChannelAttachment(ch, 10_000);
		}
		if (!ch.parent) {
			throw new Error(
				`Cannot proxy publish while not attached to a parent (topic=${topic} root=${root} self=${this.publicKeyHash})`,
			);
		}
		await this._sendControl(ch.parent, encodePublishProxy(ch.id.key, payload));
	}

	private waitForChannelAttachment(ch: ChannelState, timeoutMs: number): Promise<void> {
		if (ch.isRoot || ch.parent) return Promise.resolve();
		const ms = Math.max(0, Math.floor(timeoutMs));
		if (ms === 0) return Promise.resolve();

		const signal = this.closeController.signal;
		return new Promise<void>((resolve, reject) => {
			let done = false;
			let timer: ReturnType<typeof setTimeout> | undefined;
			const cleanup = () => {
				if (done) return;
				done = true;
				try {
					if (timer) clearTimeout(timer);
				} catch {
					// ignore
				}
				try {
					this.removeEventListener("fanout:joined", onJoined as any);
				} catch {
					// ignore
				}
				try {
					signal.removeEventListener("abort", onAbort);
				} catch {
					// ignore
				}
			};
			const onAbort = () => {
				cleanup();
				reject(signal.reason ?? new AbortError("fanout stopped"));
			};
			const onJoined = (ev: any) => {
				const d = ev?.detail as { topic: string; root: string } | undefined;
				if (!d) return;
				if (d.topic !== ch.id.topic) return;
				if (d.root !== ch.id.root) return;
				if (!ch.parent) return;
				cleanup();
				resolve();
			};

			if (signal.aborted) return onAbort();
			this.addEventListener("fanout:joined", onJoined as any);
			signal.addEventListener("abort", onAbort, { once: true });
			timer = setTimeout(() => {
				cleanup();
				reject(
					new Error(
						`fanout proxy publish timed out waiting for attachment (topic=${ch.id.topic} root=${ch.id.root} self=${this.publicKeyHash})`,
					),
				);
			}, ms);
			timer.unref?.();
		});
	}

	public async publishEnd(topic: string, root: string, lastSeqExclusive: number): Promise<void> {
		const id = this.getChannelId(topic, root);
		const ch = this.channelsBySuffixKey.get(id.suffixKey);
		if (!ch) return;
		if (!ch.isRoot) return;
		await this._sendControlMany([...ch.children.keys()], encodeEnd(ch.id.key, lastSeqExclusive));
	}

	private makeDataId(ch: ChannelState, seq: number): Uint8Array {
		const id = new Uint8Array(32);
		id.set(ID_PREFIX, 0);
		writeU32BE(id, 4, seq >>> 0);
		id.set(ch.id.key.subarray(0, 24), 8);
		return id;
	}

	private getSuffixKeyFromId(id: Uint8Array): string {
		const cached = this.cachedSuffixKey.get(id);
		if (cached) return cached;
		const key = toBase64(id.subarray(8, 32));
		this.cachedSuffixKey.set(id, key);
		return key;
	}

	private getMetricsForSuffixKey(suffixKey: string): FanoutTreeChannelMetrics {
		let m = this.metricsBySuffixKey.get(suffixKey);
		if (!m) {
			m = createEmptyMetrics();
			this.metricsBySuffixKey.set(suffixKey, m);
		}
		return m;
	}

	private recordControlSend(bytes: Uint8Array, transmissions: number) {
		if (transmissions <= 0) return;
		if (bytes.length < 1 + 24) return;
		const kind = bytes[0]!;
		const suffixKey = toBase64(bytes.subarray(1, 25));
		const m = this.getMetricsForSuffixKey(suffixKey);
		m.controlSends += transmissions;
		const sentBytes = bytes.byteLength * transmissions;
		m.controlBytesSent += sentBytes;
		switch (kind) {
			case MSG_JOIN_REQ:
				m.joinReqSent += transmissions;
				m.controlBytesSentJoin += sentBytes;
				break;
			case MSG_JOIN_ACCEPT:
				m.joinAcceptSent += transmissions;
				m.controlBytesSentJoin += sentBytes;
				break;
			case MSG_JOIN_REJECT:
				m.joinRejectSent += transmissions;
				m.controlBytesSentJoin += sentBytes;
				break;
			case MSG_KICK:
				m.kickSent += transmissions;
				m.controlBytesSentJoin += sentBytes;
				break;
			case MSG_END:
				m.endSent += transmissions;
				m.controlBytesSentRepair += sentBytes;
				break;
			case MSG_REPAIR_REQ:
				m.repairReqSent += transmissions;
				m.controlBytesSentRepair += sentBytes;
				break;
			case MSG_FETCH_REQ:
				m.fetchReqSent += transmissions;
				m.controlBytesSentRepair += sentBytes;
				break;
			case MSG_IHAVE:
				m.ihaveSent += transmissions;
				m.controlBytesSentRepair += sentBytes;
				break;
			case MSG_TRACKER_ANNOUNCE:
				m.trackerAnnounceSent += transmissions;
				m.controlBytesSentTracker += sentBytes;
				break;
			case MSG_TRACKER_QUERY:
				m.trackerQuerySent += transmissions;
				m.controlBytesSentTracker += sentBytes;
				break;
			case MSG_TRACKER_REPLY:
				m.trackerReplySent += transmissions;
				m.controlBytesSentTracker += sentBytes;
				break;
			case MSG_TRACKER_FEEDBACK:
				m.trackerFeedbackSent += transmissions;
				m.controlBytesSentTracker += sentBytes;
				break;
			case MSG_PROVIDER_ANNOUNCE:
			case MSG_PROVIDER_QUERY:
			case MSG_PROVIDER_REPLY:
				m.controlBytesSentTracker += sentBytes;
				break;
			default:
				break;
		}
	}

	private recordControlReceive(suffixKey: string, kind: number, bytesReceived: number) {
		const m = this.getMetricsForSuffixKey(suffixKey);
		m.controlReceives += 1;
		m.controlBytesReceived += bytesReceived;
		switch (kind) {
			case MSG_JOIN_REQ:
				m.joinReqReceived += 1;
				m.controlBytesReceivedJoin += bytesReceived;
				break;
			case MSG_JOIN_ACCEPT:
				m.joinAcceptReceived += 1;
				m.controlBytesReceivedJoin += bytesReceived;
				break;
			case MSG_JOIN_REJECT:
				m.joinRejectReceived += 1;
				m.controlBytesReceivedJoin += bytesReceived;
				break;
			case MSG_KICK:
				m.kickReceived += 1;
				m.controlBytesReceivedJoin += bytesReceived;
				break;
			case MSG_END:
				m.endReceived += 1;
				m.controlBytesReceivedRepair += bytesReceived;
				break;
			case MSG_REPAIR_REQ:
				m.repairReqReceived += 1;
				m.controlBytesReceivedRepair += bytesReceived;
				break;
			case MSG_FETCH_REQ:
				m.fetchReqReceived += 1;
				m.controlBytesReceivedRepair += bytesReceived;
				break;
			case MSG_IHAVE:
				m.ihaveReceived += 1;
				m.controlBytesReceivedRepair += bytesReceived;
				break;
			case MSG_TRACKER_ANNOUNCE:
				m.trackerAnnounceReceived += 1;
				m.controlBytesReceivedTracker += bytesReceived;
				break;
			case MSG_TRACKER_QUERY:
				m.trackerQueryReceived += 1;
				m.controlBytesReceivedTracker += bytesReceived;
				break;
			case MSG_TRACKER_REPLY:
				m.trackerReplyReceived += 1;
				m.controlBytesReceivedTracker += bytesReceived;
				break;
			case MSG_TRACKER_FEEDBACK:
				m.trackerFeedbackReceived += 1;
				m.controlBytesReceivedTracker += bytesReceived;
				break;
			case MSG_PROVIDER_ANNOUNCE:
			case MSG_PROVIDER_QUERY:
			case MSG_PROVIDER_REPLY:
				m.controlBytesReceivedTracker += bytesReceived;
				break;
			default:
				break;
		}
	}

	private markCached(ch: ChannelState, seq: number, payload: Uint8Array) {
		if (!ch.cacheSeqs || !ch.cachePayloads || ch.cacheSeqs.length === 0) return;
		const idx = seq % ch.cacheSeqs.length;
		ch.cacheSeqs[idx] = seq | 0;
		// Copy to avoid accidental mutation by caller.
		ch.cachePayloads[idx] = payload.slice();
	}

	private getCached(ch: ChannelState, seq: number): Uint8Array | undefined {
		if (!ch.cacheSeqs || !ch.cachePayloads || ch.cacheSeqs.length === 0) return;
		const idx = seq % ch.cacheSeqs.length;
		if (ch.cacheSeqs[idx] !== (seq | 0)) return;
		return ch.cachePayloads[idx];
	}

	private async _sendControl(to: string, bytes: Uint8Array) {
		const stream = this.peers.get(to);
		if (!stream) return;
		this.recordControlSend(bytes, 1);
		const message = await this.createMessage(bytes, {
			mode: new AnyWhere(),
			priority: CONTROL_PRIORITY,
		} as any);
		await this.publishMessage(this.publicKey, message, [stream]).catch(dontThrowIfDeliveryError);
	}

	private async _sendControlMany(to: string[], bytes: Uint8Array) {
		if (to.length === 0) return;
		const streams = to
			.map((t) => this.peers.get(t))
			.filter((s): s is PeerStreams => Boolean(s));
		if (streams.length === 0) return;
		this.recordControlSend(bytes, streams.length);
		const message = await this.createMessage(bytes, {
			mode: new AnyWhere(),
			priority: CONTROL_PRIORITY,
		} as any);
		await this.publishMessage(this.publicKey, message, streams).catch(dontThrowIfDeliveryError);
	}

	private refillUploadTokens(ch: ChannelState, now = Date.now()) {
		if (ch.uploadLimitBps <= 0) return;
		if (ch.uploadTokenCapacity <= 0) return;
		const elapsedMs = now - ch.uploadLastRefillAt;
		if (elapsedMs <= 0) return;
		ch.uploadLastRefillAt = now;
		ch.uploadTokens = Math.min(
			ch.uploadTokenCapacity,
			ch.uploadTokens + (elapsedMs * ch.uploadLimitBps) / 1_000,
		);
	}

	private refillNeighborRepairTokens(ch: ChannelState, now = Date.now()) {
		if (ch.neighborRepairBudgetBps <= 0) return;
		if (ch.neighborRepairTokenCapacity <= 0) return;
		const elapsedMs = now - ch.neighborRepairLastRefillAt;
		if (elapsedMs <= 0) return;
		ch.neighborRepairLastRefillAt = now;
		ch.neighborRepairTokens = Math.min(
			ch.neighborRepairTokenCapacity,
			ch.neighborRepairTokens + (elapsedMs * ch.neighborRepairBudgetBps) / 1_000,
		);
	}

	private takeIngressBudget(
		ch: ChannelState,
		kind: "proxy-publish" | "unicast",
		fromHash: string,
		costBytes: number,
		now = Date.now(),
	): boolean {
		if (!fromHash) return false;
		if (costBytes <= 0) return true;

		const budgetBps =
			kind === "proxy-publish" ? ch.proxyPublishBudgetBps : ch.unicastBudgetBps;
		const capacity =
			kind === "proxy-publish"
				? ch.proxyPublishTokenCapacity
				: ch.unicastTokenCapacity;
		const byPeer =
			kind === "proxy-publish"
				? ch.proxyPublishTokensByPeer
				: ch.unicastTokensByPeer;

		if (budgetBps <= 0 || capacity <= 0) return true;

		const prev = byPeer.get(fromHash);
		const entry =
			prev || ({ tokens: capacity, lastRefillAt: now } as const);
		const elapsedMs = now - entry.lastRefillAt;
		let tokens = entry.tokens;
		if (elapsedMs > 0) {
			tokens = Math.min(capacity, tokens + (elapsedMs * budgetBps) / 1_000);
		}
		if (tokens < costBytes) {
			// LRU-touch on access to keep the bucket map bounded by active children.
			byPeer.delete(fromHash);
			byPeer.set(fromHash, { tokens, lastRefillAt: now });
			return false;
		}
		tokens -= costBytes;
		byPeer.delete(fromHash);
		byPeer.set(fromHash, { tokens, lastRefillAt: now });
		return true;
	}

	private async _sendData(
		ch: ChannelState,
		to: string[],
		seq: number,
		payload: Uint8Array,
	): Promise<DataMessage> {
		this.markCached(ch, seq, payload);
		ch.maxSeqSeen = Math.max(ch.maxSeqSeen, seq);

		const framed = encodeData(payload);
		const message = await this.createMessage(framed, {
			mode: new AnyWhere(),
			priority: DATA_PRIORITY,
			id: this.makeDataId(ch, seq),
		} as any);

		if (to.length === 0) {
			return message;
		}

		this.pruneDisconnectedChildren(ch);

		const candidates = to
			.map((hash) => ({ hash, stream: this.peers.get(hash) }))
			.filter((c): c is { hash: string; stream: PeerStreams } => Boolean(c.stream));
		if (candidates.length === 0) return message;

		let selected = candidates;
		let dropped = 0;
		let costPerChild = 0;

		// Best-effort upload shaping: token bucket enforced at message boundaries.
		if (ch.uploadLimitBps > 0 && ch.uploadTokenCapacity > 0) {
			const now = Date.now();
			this.refillUploadTokens(ch, now);
			costPerChild = Math.max(1, framed.byteLength + ch.uploadOverheadBytes);
			const affordable = Math.floor(ch.uploadTokens / costPerChild);

			if (affordable <= 0) {
				selected = [];
				dropped = candidates.length;
			} else if (affordable < candidates.length) {
				selected = [...candidates]
					.sort((a, b) => {
						const bidA = ch.children.get(a.hash)?.bidPerByte ?? 0;
						const bidB = ch.children.get(b.hash)?.bidPerByte ?? 0;
						return bidB - bidA;
					})
					.slice(0, affordable);
				dropped = candidates.length - selected.length;
			}

				const hasChildCandidate = candidates.some((c) => ch.children.has(c.hash));
				if (hasChildCandidate) {
					if (dropped > 0) {
						ch.droppedForwards += dropped;
						ch.overloadStreak += 1;
					} else {
						ch.overloadStreak = 0;
					}
				}

				// Reserve tokens up front to keep the cap consistent under concurrent sends.
				// Any unused reservation is refunded after attempting writes.
				if (selected.length > 0) {
					ch.uploadTokens -= selected.length * costPerChild;
				}

				if (
					dropped > 0 &&
					hasChildCandidate &&
					ch.allowKick &&
				ch.overloadStreak >= OVERLOAD_KICK_STREAK_THRESHOLD &&
				now - ch.lastOverloadKickAt > OVERLOAD_KICK_COOLDOWN_MS
			) {
				const selectedSet = new Set(selected.map((c) => c.hash));
				const droppedCandidates = candidates.filter(
					(c) => !selectedSet.has(c.hash) && ch.children.has(c.hash),
				);
				droppedCandidates.sort((a, b) => {
					const bidA = ch.children.get(a.hash)?.bidPerByte ?? 0;
					const bidB = ch.children.get(b.hash)?.bidPerByte ?? 0;
					if (bidA !== bidB) return bidA - bidB;
					return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
				});

					const kickCount = Math.min(OVERLOAD_KICK_MAX_PER_EVENT, droppedCandidates.length);
					if (kickCount > 0) {
						const toKick = droppedCandidates.slice(0, kickCount).map((c) => c.hash);
						for (const h of toKick) {
							ch.children.delete(h);
							ch.dataWriteFailStreakByChild.delete(h);
						}
						ch.overloadStreak = 0;
						ch.lastOverloadKickAt = now;
						void this._sendControlMany(toKick, encodeKick(ch.id.key));
						void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
					}
			}
		}

		if (selected.length === 0) return message;

		// Best-effort DATA-plane: never block on writability. Drop if not writable and rely on repair.
		const msgBytes = message.bytes();
		const bytesToSend = msgBytes;
		const priority = Number(message.header.priority ?? DATA_PRIORITY);

		// Keep seen-cache semantics consistent with DirectStream.publishMessage so we
		// ignore any loops/duplicates that come back to us.
		try {
			const msgId = toBase64(bytesToSend.subarray(0, 33)); // discriminator + id
			const seen = this.seenCache.get(msgId);
			this.seenCache.add(msgId, seen ? seen + 1 : 1);
		} catch {
			// ignore
		}

		let successes = 0;
		let writeDrops = 0;
		const now = Date.now();
		const writeFailKickCandidates: string[] = [];

		for (const c of selected) {
			const stream = c.stream;
			if (!stream.isWritable) {
				writeDrops += 1;
				if (ch.children.has(c.hash)) {
					const streak = (ch.dataWriteFailStreakByChild.get(c.hash) ?? 0) + 1;
					ch.dataWriteFailStreakByChild.set(c.hash, streak);
					if (streak >= DATA_WRITE_FAIL_KICK_STREAK_THRESHOLD) {
						writeFailKickCandidates.push(c.hash);
					}
				}
				continue;
			}

			try {
				stream.write(bytesToSend, priority);
				successes += 1;
				ch.dataWriteFailStreakByChild.delete(c.hash);
				const bid = ch.children.get(c.hash)?.bidPerByte ?? 0;
				if (bid > 0) ch.metrics.earnings += payload.byteLength * bid;
			} catch {
				writeDrops += 1;
				if (ch.children.has(c.hash)) {
					const streak = (ch.dataWriteFailStreakByChild.get(c.hash) ?? 0) + 1;
					ch.dataWriteFailStreakByChild.set(c.hash, streak);
					if (streak >= DATA_WRITE_FAIL_KICK_STREAK_THRESHOLD) {
						writeFailKickCandidates.push(c.hash);
					}
				}
			}
		}

		if (successes > 0) {
			ch.metrics.dataSends += successes;
			ch.metrics.dataPayloadBytesSent += payload.byteLength * successes;
		}
		if (costPerChild > 0 && successes < selected.length) {
			const refund = (selected.length - successes) * costPerChild;
			ch.uploadTokens = Math.min(ch.uploadTokenCapacity, ch.uploadTokens + refund);
		}
		if (writeDrops > 0) ch.metrics.dataWriteDrops += writeDrops;

		// Kick persistently failing children (best-effort, bounded per event).
		if (
			ch.allowKick &&
			writeFailKickCandidates.length > 0 &&
			now - ch.lastDataWriteFailKickAt > DATA_WRITE_FAIL_KICK_COOLDOWN_MS
		) {
			const unique = [...new Set(writeFailKickCandidates)].filter((h) => ch.children.has(h));
			unique.sort((a, b) => {
				const bidA = ch.children.get(a)?.bidPerByte ?? 0;
				const bidB = ch.children.get(b)?.bidPerByte ?? 0;
				if (bidA !== bidB) return bidA - bidB;
				return a < b ? -1 : a > b ? 1 : 0;
			});
			const kickCount = Math.min(DATA_WRITE_FAIL_KICK_MAX_PER_EVENT, unique.length);
			if (kickCount > 0) {
				const toKick = unique.slice(0, kickCount);
				for (const h of toKick) {
					ch.children.delete(h);
					ch.dataWriteFailStreakByChild.delete(h);
				}
				ch.lastDataWriteFailKickAt = now;
				void this._sendControlMany(toKick, encodeKick(ch.id.key));
				void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
			}
		}

		return message;
	}

	private async _forwardDataMessage(
		ch: ChannelState,
		to: string[],
		payload: Uint8Array,
		message: DataMessage,
	): Promise<void> {
		if (to.length === 0) return;

		this.pruneDisconnectedChildren(ch);

		const candidates = to
			.map((hash) => ({ hash, stream: this.peers.get(hash) }))
			.filter((c): c is { hash: string; stream: PeerStreams } => Boolean(c.stream));
		if (candidates.length === 0) return;

		if (ch.maxDataAgeMs > 0) {
			const ts = Number(message.header.timestamp);
			if (Number.isFinite(ts)) {
				const ageMs = Date.now() - ts;
				if (ageMs > ch.maxDataAgeMs) {
					ch.metrics.staleForwardsDropped += candidates.length;
					return;
				}
			}
		}

		let selected = candidates;
		let dropped = 0;
		let costPerChild = 0;

		// Best-effort upload shaping: token bucket enforced at message boundaries.
		if (ch.uploadLimitBps > 0 && ch.uploadTokenCapacity > 0) {
			const now = Date.now();
			this.refillUploadTokens(ch, now);
			costPerChild = Math.max(1, payload.byteLength + 1 + ch.uploadOverheadBytes);
			const affordable = Math.floor(ch.uploadTokens / costPerChild);

			if (affordable <= 0) {
				selected = [];
				dropped = candidates.length;
			} else if (affordable < candidates.length) {
				selected = [...candidates]
					.sort((a, b) => {
						const bidA = ch.children.get(a.hash)?.bidPerByte ?? 0;
						const bidB = ch.children.get(b.hash)?.bidPerByte ?? 0;
						return bidB - bidA;
					})
					.slice(0, affordable);
				dropped = candidates.length - selected.length;
			}

				const hasChildCandidate = candidates.some((c) => ch.children.has(c.hash));
				if (hasChildCandidate) {
					if (dropped > 0) {
						ch.droppedForwards += dropped;
						ch.overloadStreak += 1;
					} else {
						ch.overloadStreak = 0;
					}
					}

				// Reserve tokens up front to keep the cap consistent under concurrent sends.
				// Any unused reservation is refunded after attempting writes.
				if (selected.length > 0) {
					ch.uploadTokens -= selected.length * costPerChild;
				}

					if (
						dropped > 0 &&
						hasChildCandidate &&
						ch.allowKick &&
				ch.overloadStreak >= OVERLOAD_KICK_STREAK_THRESHOLD &&
				now - ch.lastOverloadKickAt > OVERLOAD_KICK_COOLDOWN_MS
			) {
				const selectedSet = new Set(selected.map((c) => c.hash));
				const droppedCandidates = candidates.filter(
					(c) => !selectedSet.has(c.hash) && ch.children.has(c.hash),
				);
				droppedCandidates.sort((a, b) => {
					const bidA = ch.children.get(a.hash)?.bidPerByte ?? 0;
					const bidB = ch.children.get(b.hash)?.bidPerByte ?? 0;
					if (bidA !== bidB) return bidA - bidB;
					return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
				});

					const kickCount = Math.min(OVERLOAD_KICK_MAX_PER_EVENT, droppedCandidates.length);
					if (kickCount > 0) {
						const toKick = droppedCandidates.slice(0, kickCount).map((c) => c.hash);
						for (const h of toKick) {
							ch.children.delete(h);
							ch.dataWriteFailStreakByChild.delete(h);
						}
						ch.overloadStreak = 0;
						ch.lastOverloadKickAt = now;
						void this._sendControlMany(toKick, encodeKick(ch.id.key));
						void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
					}
			}
		}

		if (selected.length === 0) return;

		const msgBytes = message.bytes();
		const bytesToSend = msgBytes;
		const priority = Number(message.header.priority ?? DATA_PRIORITY);

		let successes = 0;
		let writeDrops = 0;
		const now = Date.now();
		const writeFailKickCandidates: string[] = [];

		for (const c of selected) {
			const stream = c.stream;
			if (!stream.isWritable) {
				writeDrops += 1;
				const streak = (ch.dataWriteFailStreakByChild.get(c.hash) ?? 0) + 1;
				ch.dataWriteFailStreakByChild.set(c.hash, streak);
				if (streak >= DATA_WRITE_FAIL_KICK_STREAK_THRESHOLD) {
					writeFailKickCandidates.push(c.hash);
				}
				continue;
			}

			try {
				stream.write(bytesToSend, priority);
				successes += 1;
				ch.dataWriteFailStreakByChild.delete(c.hash);
				const bid = ch.children.get(c.hash)?.bidPerByte ?? 0;
				if (bid > 0) ch.metrics.earnings += payload.byteLength * bid;
			} catch {
				writeDrops += 1;
				const streak = (ch.dataWriteFailStreakByChild.get(c.hash) ?? 0) + 1;
				ch.dataWriteFailStreakByChild.set(c.hash, streak);
				if (streak >= DATA_WRITE_FAIL_KICK_STREAK_THRESHOLD) {
					writeFailKickCandidates.push(c.hash);
				}
			}
		}

		if (successes > 0) {
			ch.metrics.dataSends += successes;
			ch.metrics.dataPayloadBytesSent += payload.byteLength * successes;
		}
		if (costPerChild > 0 && successes < selected.length) {
			const refund = (selected.length - successes) * costPerChild;
			ch.uploadTokens = Math.min(ch.uploadTokenCapacity, ch.uploadTokens + refund);
		}
		if (writeDrops > 0) ch.metrics.dataWriteDrops += writeDrops;

		// Kick persistently failing children (best-effort, bounded per event).
		if (
			ch.allowKick &&
			writeFailKickCandidates.length > 0 &&
			now - ch.lastDataWriteFailKickAt > DATA_WRITE_FAIL_KICK_COOLDOWN_MS
		) {
			const unique = [...new Set(writeFailKickCandidates)].filter((h) => ch.children.has(h));
			unique.sort((a, b) => {
				const bidA = ch.children.get(a)?.bidPerByte ?? 0;
				const bidB = ch.children.get(b)?.bidPerByte ?? 0;
				if (bidA !== bidB) return bidA - bidB;
				return a < b ? -1 : a > b ? 1 : 0;
			});
			const kickCount = Math.min(DATA_WRITE_FAIL_KICK_MAX_PER_EVENT, unique.length);
			if (kickCount > 0) {
				const toKick = unique.slice(0, kickCount);
				for (const h of toKick) {
					ch.children.delete(h);
					ch.dataWriteFailStreakByChild.delete(h);
				}
				ch.lastDataWriteFailKickAt = now;
				void this._sendControlMany(toKick, encodeKick(ch.id.key));
				void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
			}
		}
	}

		private noteReceivedSeq(ch: ChannelState, fromHash: string, seq: number) {
			if (!ch.repairEnabled) return;
			if (!ch.parent || fromHash !== ch.parent) {
				// Neighbor-assisted repair may deliver payload from a peer other than the
			// current parent; treat it as a "hole fill" only.
			const wasMissing = ch.missingSeqs.delete(seq);
			if (wasMissing) {
				let have = ch.haveByPeer.get(fromHash);
				if (!have) {
					have = {
						haveFrom: 0,
						haveToExclusive: 0,
						updatedAt: Date.now(),
						requests: 0,
						successes: 0,
					};
					ch.haveByPeer.set(fromHash, have);
				}
				have.successes += 1;
				ch.metrics.holeFillsFromNeighbor += 1;
			}
				return;
			}

			if (seq >= ch.nextExpectedSeq) {
				const maxBackfill = Math.max(0, ch.repairMaxBackfillMessages);
				const start =
				maxBackfill > 0 ? Math.max(ch.nextExpectedSeq, Math.max(0, seq - maxBackfill)) : ch.nextExpectedSeq;
			for (let s = start; s < seq; s++) ch.missingSeqs.add(s);
			ch.nextExpectedSeq = seq + 1;
			if (maxBackfill > 0) {
				const minSeq = Math.max(0, ch.nextExpectedSeq - maxBackfill);
				for (const s of ch.missingSeqs) {
					if (s < minSeq) ch.missingSeqs.delete(s);
				}
			}
		}
		ch.missingSeqs.delete(seq);
	}

	private noteEnd(ch: ChannelState, fromHash: string, lastSeqExclusive: number) {
		if (!ch.repairEnabled) return;
		if (!ch.parent || fromHash !== ch.parent) return;

		if (lastSeqExclusive > ch.nextExpectedSeq) {
			const maxBackfill = Math.max(0, ch.repairMaxBackfillMessages);
			const start =
				maxBackfill > 0
					? Math.max(
							ch.nextExpectedSeq,
							Math.max(0, lastSeqExclusive - maxBackfill),
						)
					: ch.nextExpectedSeq;
			for (let s = start; s < lastSeqExclusive; s++) ch.missingSeqs.add(s);
			ch.nextExpectedSeq = lastSeqExclusive;
			if (maxBackfill > 0) {
				const minSeq = Math.max(0, ch.nextExpectedSeq - maxBackfill);
				for (const s of ch.missingSeqs) {
					if (s < minSeq) ch.missingSeqs.delete(s);
				}
			}
		}
	}

	private async tickRepair(ch: ChannelState, now = Date.now()): Promise<boolean> {
		if (!ch.repairEnabled) return false;

		// Drop missing seqs that are too old to be realistically repaired (bounded window / live mode).
		if (ch.missingSeqs.size > 0 && ch.repairMaxBackfillMessages > 0) {
			const minSeq = Math.max(0, ch.nextExpectedSeq - ch.repairMaxBackfillMessages);
			for (const s of ch.missingSeqs) {
				if (s < minSeq) ch.missingSeqs.delete(s);
			}
		}

		if (ch.missingSeqs.size === 0) return false;
		if (ch.repairIntervalMs > 0 && now - ch.lastRepairSentAt < ch.repairIntervalMs) {
			return false;
		}

		ch.lastRepairSentAt = now;
		const missing = [...ch.missingSeqs].sort((a, b) => a - b);
		const count = Math.min(ch.repairMaxPerReq, missing.length, 255);
		if (count <= 0) return false;
		const reqId = (this.random() * 0xffffffff) >>> 0;
		const slice = missing.slice(0, count);

		let sent = false;

		if (ch.parent) {
			await this._sendControl(ch.parent, encodeRepairReq(ch.id.key, reqId, slice));
			sent = true;
		}

		if (ch.neighborRepair && ch.neighborRepairPeers > 0) {
			const candidates: string[] = [];

			for (const h of ch.knownCandidateAddrs.keys()) candidates.push(h);
			for (const h of ch.channelPeers.keys()) candidates.push(h);
			for (const h of ch.lazyPeers) candidates.push(h);
			for (const h of ch.haveByPeer.keys()) candidates.push(h);

			const unique = [...new Set(candidates)].filter((h) => {
				if (h === this.publicKeyHash) return false;
				if (h === ch.parent) return false;
				if (ch.children.has(h)) return false;
				return Boolean(this.peers.get(h));
			});

			const ttl = Math.max(0, ch.neighborHaveTtlMs);
			const scored = unique.map((peerHash) => {
				const have = ch.haveByPeer.get(peerHash);
				const updatedAt = have?.updatedAt ?? 0;
				const fresh =
					!have || ttl <= 0 ? true : updatedAt > 0 && now - updatedAt <= ttl;

				let coverage = 0;
				if (
					have &&
					fresh &&
					have.haveToExclusive > have.haveFrom &&
					have.haveToExclusive > 0
				) {
					for (const s of slice) {
						if (s < have.haveFrom) continue;
						if (s >= have.haveToExclusive) break;
						coverage += 1;
					}
				}

				const requests = have?.requests ?? 0;
				const successes = have?.successes ?? 0;
				const successRate = (successes + 1) / (requests + 2);

				return {
					peerHash,
					coverage,
					successRate,
					updatedAt,
				};
			});

			scored.sort((a, b) => {
				if (a.coverage !== b.coverage) return b.coverage - a.coverage;
				if (a.coverage > 0) {
					if (a.successRate !== b.successRate) return b.successRate - a.successRate;
					if (a.updatedAt !== b.updatedAt) return b.updatedAt - a.updatedAt;
					const al = ch.lazyPeers.has(a.peerHash) ? 1 : 0;
					const bl = ch.lazyPeers.has(b.peerHash) ? 1 : 0;
					if (al !== bl) return bl - al;
				} else {
					const al = ch.lazyPeers.has(a.peerHash) ? 1 : 0;
					const bl = ch.lazyPeers.has(b.peerHash) ? 1 : 0;
					if (al !== bl) return bl - al;
					if (a.successRate !== b.successRate) return b.successRate - a.successRate;
					if (a.updatedAt !== b.updatedAt) return b.updatedAt - a.updatedAt;
				}
				return a.peerHash < b.peerHash ? -1 : a.peerHash > b.peerHash ? 1 : 0;
			});

			let peers = scored.slice(0, ch.neighborRepairPeers).map((s) => s.peerHash);
			if (peers.length > 0) {
				const bytes = encodeFetchReq(ch.id.key, reqId, slice);

				if (ch.neighborRepairBudgetBps > 0 && ch.neighborRepairTokenCapacity > 0) {
					this.refillNeighborRepairTokens(ch, now);
					const costPerPeer = Math.max(1, bytes.byteLength);
					const affordable = Math.floor(ch.neighborRepairTokens / costPerPeer);
					if (affordable <= 0) {
						peers = [];
					} else if (affordable < peers.length) {
						peers = peers.slice(0, affordable);
					}
					ch.neighborRepairTokens -= peers.length * costPerPeer;
				}

				if (peers.length > 0) {
					for (const p of peers) {
						let have = ch.haveByPeer.get(p);
						if (!have) {
							have = {
								haveFrom: 0,
								haveToExclusive: 0,
								updatedAt: 0,
								requests: 0,
								successes: 0,
							};
							ch.haveByPeer.set(p, have);
						}
						have.requests += 1;
					}
					await this._sendControlMany(peers, bytes);
					sent = true;
				}
			}
		}

		return sent;
	}

	private getBootstrapsForChannel(ch: ChannelState): Multiaddr[] {
		return ch.bootstrapOverride ?? this.bootstraps;
	}

	private getSelfAnnounceAddrs(): Multiaddr[] {
		const peerIdStr = this.components.peerId.toString();
		const addrs = this.components.addressManager.getAddresses();
		if (addrs.length === 0) return [];
		const out: Multiaddr[] = [];
		for (const a of addrs) {
			const s = a.toString();
			if (s.includes("/p2p/")) {
				out.push(a);
				continue;
			}
			try {
				const withPeer = a.encapsulate(`/p2p/${peerIdStr}`);
				// Some test/mocked peer ids may not be valid `/p2p/<peerId>` values.
				// Validate that the multiaddr can be encoded before advertising it.
				void withPeer.bytes;
				out.push(withPeer);
			} catch {
				out.push(a);
			}
		}
		return out;
	}

	private async ensureBootstrapPeers(
		addrs: Multiaddr[],
		timeoutMs: number,
		signal: AbortSignal,
		maxPeers = 0,
	): Promise<string[]> {
		if (addrs.length === 0) return [];
		const max = Math.max(0, Math.floor(maxPeers));
		const shuffled = addrs.slice();
		for (let i = shuffled.length - 1; i > 0; i--) {
			const j = Math.floor(this.random() * (i + 1));
			const tmp = shuffled[i]!;
			shuffled[i] = shuffled[j]!;
			shuffled[j] = tmp;
		}
		const target = max > 0 ? Math.min(max, shuffled.length) : shuffled.length;
		const out: string[] = [];
		for (const a of shuffled) {
			if (signal.aborted) break;
			if (target > 0 && out.length >= target) break;
			try {
				const conn = await this.components.connectionManager.openConnection(a);
				const h = getPublicKeyFromPeerId(conn.remotePeer).hashcode();
				await this.waitFor(h, { seek: "present", timeout: timeoutMs, signal });
				out.push(h);
			} catch {
				// ignore dial failures
			}
		}
		return [...new Set(out)];
	}

	private async announceToTrackers(ch: ChannelState, signal: AbortSignal): Promise<void> {
		const now = Date.now();
		if (now - ch.lastAnnouncedAt < 100) return;
		ch.lastAnnouncedAt = now;

		// Only announce capacity if we're attached (or root).
		if (!ch.isRoot && !ch.parent) return;
		if (ch.effectiveMaxChildren <= 0) return;

		const bootstraps = this.getBootstrapsForChannel(ch);
		if (bootstraps.length === 0) return;

		const peers = await this.ensureBootstrapPeers(
			bootstraps,
			ch.bootstrapDialTimeoutMs,
			signal,
			ch.bootstrapMaxPeers,
		);
		if (peers.length === 0) return;

		this.pruneDisconnectedChildren(ch);
		const freeSlots = Math.max(0, ch.effectiveMaxChildren - ch.children.size);
		const addrs = this.getSelfAnnounceAddrs();
		const bytes = encodeTrackerAnnounce(
			ch.id.key,
			ch.announceTtlMs,
			Number.isFinite(ch.level) ? ch.level : 0xffff,
			ch.effectiveMaxChildren,
			freeSlots,
			ch.bidPerByte,
			addrs,
		);
		await this._sendControlMany(peers, bytes);
	}

	private async _announceLoop(ch: ChannelState): Promise<void> {
		const signal = this.closeController.signal;
		for (;;) {
			if (signal.aborted || ch.closed) return;
			try {
				this.pruneRouteCache(ch);
			} catch {
				// ignore
			}
			try {
				await this.announceToTrackers(ch, signal);
			} catch {
				// ignore
			}
			const sleepMs = ch.announceIntervalMs > 0 ? ch.announceIntervalMs : 1_000;
			await delay(sleepMs);
		}
	}

	private async _repairLoop(ch: ChannelState): Promise<void> {
		const signal = this.closeController.signal;
		for (;;) {
			if (signal.aborted || ch.closed) return;
			try {
				await this.tickRepair(ch);
			} catch {
				// ignore
			}
			const activeMs = ch.repairIntervalMs > 0 ? ch.repairIntervalMs : 200;
			const sleepMs = ch.missingSeqs.size > 0 ? activeMs : Math.max(activeMs, 1_000);
			await delay(sleepMs);
		}
	}

	private pruneLazyPeers(ch: ChannelState) {
		for (const h of ch.lazyPeers) {
			if (h === this.publicKeyHash) {
				ch.lazyPeers.delete(h);
				continue;
			}
			if (h === ch.parent) {
				ch.lazyPeers.delete(h);
				continue;
			}
			if (ch.children.has(h)) {
				ch.lazyPeers.delete(h);
				continue;
			}
			if (!this.peers.get(h)) {
				ch.lazyPeers.delete(h);
				continue;
			}
		}

		if (ch.neighborMeshPeers > 0 && ch.lazyPeers.size > ch.neighborMeshPeers) {
			const peers = [...ch.lazyPeers];
			peers.sort((a, b) => {
				const sa = ch.haveByPeer.get(a);
				const sb = ch.haveByPeer.get(b);
				const ar = sa ? (sa.successes + 1) / (sa.requests + 2) : 0;
				const br = sb ? (sb.successes + 1) / (sb.requests + 2) : 0;
				if (ar !== br) return br - ar;
				const au = sa?.updatedAt ?? 0;
				const bu = sb?.updatedAt ?? 0;
				if (au !== bu) return bu - au;
				return a < b ? -1 : a > b ? 1 : 0;
			});
			for (const h of peers.slice(ch.neighborMeshPeers)) ch.lazyPeers.delete(h);
		}
	}

	private pruneDisconnectedChildren(ch: ChannelState) {
		for (const childHash of ch.children.keys()) {
			const peer = this.peers.get(childHash);
			if (!peer) {
				ch.children.delete(childHash);
				ch.dataWriteFailStreakByChild.delete(childHash);
				continue;
			}

			let connected = true;
			try {
				const conns = this.components.connectionManager.getConnections(peer.peerId);
				connected = conns.length > 0;
			} catch {
				connected = peer.isReadable || peer.isWritable;
			}

			if (!connected) {
				ch.children.delete(childHash);
				ch.dataWriteFailStreakByChild.delete(childHash);
			}
		}
	}

	private pruneHaveByPeer(ch: ChannelState, now = Date.now()) {
		const ttl = Math.max(0, ch.neighborHaveTtlMs);
		for (const [peerHash, have] of ch.haveByPeer) {
			if (!this.peers.get(peerHash) && !ch.lazyPeers.has(peerHash)) {
				ch.haveByPeer.delete(peerHash);
				continue;
			}
			// Keep stats longer than TTL, but prune very stale IHAVE ranges.
			if (ttl > 0 && have.updatedAt > 0 && now - have.updatedAt > ttl * 4) {
				have.haveFrom = 0;
				have.haveToExclusive = 0;
			}
		}
	}

	private getHaveRange(
		ch: ChannelState,
	): { haveFrom: number; haveToExclusive: number } | undefined {
		if (!ch.repairEnabled) return;
		if (!ch.cacheSeqs || !ch.cachePayloads || ch.cacheSeqs.length === 0) return;
		if (ch.maxSeqSeen < 0) return;
		const haveToExclusive = ch.maxSeqSeen + 1;
		const maxScan = Math.min(haveToExclusive, ch.cacheSeqs.length);
		let haveFrom = haveToExclusive;
		for (let i = 0; i < maxScan; i++) {
			const seq = haveToExclusive - 1 - i;
			if (!this.getCached(ch, seq)) break;
			haveFrom = seq;
		}
		return { haveFrom, haveToExclusive };
	}

	private async maybeSendIHave(ch: ChannelState, now = Date.now()): Promise<void> {
		if (!ch.neighborRepair) return;
		if (ch.neighborMeshPeers <= 0) return;
		if (ch.neighborAnnounceIntervalMs <= 0) return;
		if (ch.maxSeqSeen <= ch.lastIHaveSentMaxSeq) return;
		if (now - ch.lastIHaveSentAt < ch.neighborAnnounceIntervalMs) return;

		const range = this.getHaveRange(ch);
		if (!range) return;
		if (range.haveToExclusive <= range.haveFrom) return;

		const peers = [...ch.lazyPeers].filter((h) => Boolean(this.peers.get(h)));
		if (peers.length === 0) return;
		await this._sendControlMany(
			peers,
			encodeIHave(ch.id.key, range.haveFrom, range.haveToExclusive),
		);
		ch.lastIHaveSentAt = now;
		ch.lastIHaveSentMaxSeq = ch.maxSeqSeen;
	}

	private async ensureMeshPeers(ch: ChannelState, signal: AbortSignal): Promise<void> {
		if (ch.neighborMeshPeers <= 0) return;
		if (ch.lazyPeers.size >= ch.neighborMeshPeers) return;

		const maxAttempts = Math.max(16, ch.neighborMeshPeers * 16);
		let attempts = 0;

		const entries = [...ch.knownCandidateAddrs.entries()];
		for (let i = entries.length - 1; i > 0; i--) {
			const j = Math.floor(this.random() * (i + 1));
			const tmp = entries[i]!;
			entries[i] = entries[j]!;
			entries[j] = tmp;
		}

		for (const [hash, addrs] of entries) {
			if (attempts++ > maxAttempts) break;
			if (signal.aborted) break;
			if (ch.lazyPeers.size >= ch.neighborMeshPeers) break;
			if (hash === this.publicKeyHash) continue;
			if (hash === ch.parent) continue;
			if (ch.children.has(hash)) continue;
			if (ch.lazyPeers.has(hash)) continue;
			if (this.peers.get(hash)) {
				ch.lazyPeers.add(hash);
				continue;
			}
			if (!addrs || addrs.length === 0) continue;
			const ok = await this.ensurePeerConnection(
				hash,
				addrs,
				ch.bootstrapDialTimeoutMs,
				signal,
			);
			if (ok) ch.lazyPeers.add(hash);
		}
	}

	private async refreshMeshCandidates(ch: ChannelState, signal: AbortSignal): Promise<void> {
		const bootstraps = this.getBootstrapsForChannel(ch);
		if (bootstraps.length === 0) return;
		const trackerPeers = await this.ensureBootstrapPeers(
			bootstraps,
			ch.bootstrapDialTimeoutMs,
			signal,
			ch.bootstrapMaxPeers,
		);
		if (trackerPeers.length === 0) return;

			const want = Math.max(16, ch.neighborMeshPeers * 4);
			const candidates = await this.queryTrackers(ch, trackerPeers, want, 1_000, signal);
			for (const c of candidates) {
				if (c.hash === this.publicKeyHash) continue;
				if (c.addrs.length === 0) continue;
				this.cacheKnownCandidateAddrs(ch, c.hash, c.addrs);
			}
		}

		private async _meshLoop(ch: ChannelState): Promise<void> {
			const signal = this.closeController.signal;
			let lastRefreshAt = 0;
			for (;;) {
				if (signal.aborted || ch.closed) return;

				const now = Date.now();
				try {
					this.pruneLazyPeers(ch);
					this.pruneHaveByPeer(ch, now);

				const refreshMs = Math.max(0, ch.neighborMeshRefreshIntervalMs);
				if (refreshMs === 0 || now - lastRefreshAt >= refreshMs) {
					lastRefreshAt = now;
					await this.refreshMeshCandidates(ch, signal);
				}

				await this.ensureMeshPeers(ch, signal);
				await this.maybeSendIHave(ch, now);
			} catch {
				// ignore
			}

			const intervals = [ch.neighborAnnounceIntervalMs, ch.neighborMeshRefreshIntervalMs].filter(
				(v) => v > 0,
			);
			const sleepMs = intervals.length > 0 ? Math.min(...intervals) : 500;
			await delay(Math.max(50, sleepMs));
		}
	}

	private async queryTrackers(
		ch: ChannelState,
		trackerPeers: string[],
		want: number,
		timeoutMs: number,
		signal: AbortSignal,
	): Promise<TrackerCandidate[]> {
		if (trackerPeers.length === 0) return [];
		const perTrackerTimeout = Math.max(0, Math.floor(timeoutMs));
		const results = await Promise.all(
			trackerPeers.map(async (trackerHash) => {
				const reqId = (this.random() * 0xffffffff) >>> 0;
				const p = new Promise<TrackerCandidate[]>((resolve) => {
					ch.pendingTrackerQuery.set(reqId, { resolve });
				});
				void this._sendControl(trackerHash, encodeTrackerQuery(ch.id.key, reqId, want));
				const res = await Promise.race([
					p,
					delay(perTrackerTimeout, { signal }).then((): null => null),
				]);
				if (res == null) {
					ch.pendingTrackerQuery.delete(reqId);
					return [];
				}
				return res;
			}),
		);

		const merged: TrackerCandidate[] = [];
		for (const r of results) merged.push(...r);

		const seen = new Set<string>();
		return merged.filter((c) => {
			if (seen.has(c.hash)) return false;
			seen.add(c.hash);
			return true;
		});
	}

	private async ensurePeerConnection(
		hash: string,
		addrs: Multiaddr[],
		timeoutMs: number,
		signal: AbortSignal,
	): Promise<boolean> {
		if (this.peers.get(hash)) return true;
		for (const a of addrs) {
			if (signal.aborted) return false;
			try {
				await this.components.connectionManager.openConnection(a);
				await this.waitFor(hash, { seek: "present", timeout: timeoutMs, signal });
				return true;
			} catch {
				// ignore and try next
			}
		}
		return false;
	}

	private async getPeerAddrsBytes(hash: string): Promise<Uint8Array[]> {
		const stream = this.peers.get(hash);
		if (!stream) return [];
		try {
			const peer: any = await this.components.peerStore.get(stream.peerId);
			const addresses: any[] = Array.isArray(peer?.addresses) ? peer.addresses : [];
			const out: Uint8Array[] = [];
			for (const a of addresses) {
				const ma: any = a?.multiaddr ?? a;
				const bytes: Uint8Array | undefined = ma?.bytes;
				if (!(bytes instanceof Uint8Array)) continue;
				out.push(bytes);
				if (out.length >= JOIN_REJECT_REDIRECT_ADDR_MAX) break;
			}
			return out;
		} catch {
			return [];
		}
	}

	private async pickJoinRejectRedirects(
		ch: ChannelState,
		excludeHash: string,
		limit: number,
	): Promise<JoinRejectRedirect[]> {
		const max = Math.max(0, Math.min(JOIN_REJECT_REDIRECT_MAX, Math.floor(limit)));
		if (max === 0) return [];

		const seen = new Set<string>([excludeHash, this.publicKeyHash]);
		const out: JoinRejectRedirect[] = [];

		const children = [...ch.children.entries()];
		children.sort((a, b) => b[1].bidPerByte - a[1].bidPerByte);
		for (const [hash] of children) {
			if (out.length >= max) break;
			if (seen.has(hash)) continue;
			seen.add(hash);
			const addrs = await this.getPeerAddrsBytes(hash);
			if (addrs.length === 0) continue;
			out.push({ hash, addrs });
		}

		if (out.length >= max) return out;

		// Tracker entries this node has observed (mostly useful when acting as a bootstrap).
		const now = Date.now();
		const byPeer = this.trackerBySuffixKey.get(ch.id.suffixKey);
		if (byPeer) {
			const entries: TrackerEntry[] = [];
			for (const e of byPeer.values()) {
				if (e.expiresAt <= now) continue;
				if (e.freeSlots <= 0) continue;
				if (e.addrs.length === 0) continue;
				if (seen.has(e.hash)) continue;
				entries.push(e);
			}

			entries.sort((a, b) => {
				if (a.level !== b.level) return a.level - b.level;
				if (a.freeSlots !== b.freeSlots) return b.freeSlots - a.freeSlots;
				if (a.bidPerByte !== b.bidPerByte) return b.bidPerByte - a.bidPerByte;
				return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
			});

			for (const e of entries) {
				if (out.length >= max) break;
				seen.add(e.hash);
				out.push({ hash: e.hash, addrs: e.addrs.slice(0, JOIN_REJECT_REDIRECT_ADDR_MAX) });
			}
		}

		if (out.length >= max) return out;

		// Fallback: known candidates this node has learned previously (addresses only).
		for (const [hash, addrs] of ch.knownCandidateAddrs) {
			if (out.length >= max) break;
			if (seen.has(hash)) continue;
			if (!addrs || addrs.length === 0) continue;
			const bytes: Uint8Array[] = [];
			for (const a of addrs.slice(0, JOIN_REJECT_REDIRECT_ADDR_MAX)) {
				try {
					bytes.push(a.bytes);
				} catch {
					// ignore invalid addrs
				}
			}
			if (bytes.length === 0) continue;
			seen.add(hash);
			out.push({ hash, addrs: bytes });
		}

		return out;
	}

	private async sendJoinReject(
		ch: ChannelState,
		toHash: string,
		reqId: number,
		reason: number,
	): Promise<void> {
		const redirects = await this.pickJoinRejectRedirects(ch, toHash, JOIN_REJECT_REDIRECT_MAX);
		await this._sendControl(toHash, encodeJoinReject(ch.id.key, reqId, reason, redirects));
	}

	private async sendTrackerFeedback(
		ch: ChannelState,
		trackerPeers: string[],
		candidateHash: string,
		event: number,
		reason = 0,
	): Promise<void> {
		if (trackerPeers.length === 0) return;
		await this._sendControlMany(
			trackerPeers,
			encodeTrackerFeedback(ch.id.key, candidateHash, event, reason),
		);
	}

	private async _joinLoop(ch: ChannelState, joinOpts: FanoutTreeJoinOptions): Promise<void> {
		const retryMs = Math.max(1, Math.floor(joinOpts.retryMs ?? 200));
		const timeoutMs = Math.max(0, Math.floor(joinOpts.timeoutMs ?? 60_000));
		const staleAfterMs = Math.max(0, Math.floor(joinOpts.staleAfterMs ?? 0));
		const joinReqTimeoutMs = Math.max(0, Math.floor(joinOpts.joinReqTimeoutMs ?? 2_000));
		const bootstrapDialTimeoutMs = Math.max(
			0,
			Math.floor(joinOpts.bootstrapDialTimeoutMs ?? ch.bootstrapDialTimeoutMs),
		);
		const bootstrapMaxPeers = Math.max(
			0,
			Math.floor(joinOpts.bootstrapMaxPeers ?? ch.bootstrapMaxPeers),
		);
		const trackerCandidates = Math.max(0, Math.floor(joinOpts.trackerCandidates ?? 16));
		const candidateShuffleTopK = Math.max(
			0,
			Math.floor(joinOpts.candidateShuffleTopK ?? 8),
		);
		const trackerQueryTimeoutMs = Math.max(
			0,
			Math.floor(joinOpts.trackerQueryTimeoutMs ?? 1_000),
		);
		const bootstrapEnsureIntervalMs = Math.max(
			0,
			Math.floor(joinOpts.bootstrapEnsureIntervalMs ?? ch.bootstrapEnsureIntervalMs),
		);
		const trackerQueryIntervalMs = Math.max(
			0,
			Math.floor(joinOpts.trackerQueryIntervalMs ?? ch.trackerQueryIntervalMs),
		);
		const joinAttemptsPerRound = Math.max(
			1,
			Math.floor(joinOpts.joinAttemptsPerRound ?? 8),
		);
		const candidateCooldownMs = Math.max(
			0,
			Math.floor(joinOpts.candidateCooldownMs ?? 2_000),
		);
		const candidateScoringModeRaw = joinOpts.candidateScoringMode ?? "ranked-shuffle";
		const candidateScoringMode: "ranked-shuffle" | "ranked-strict" | "weighted" =
			candidateScoringModeRaw === "ranked-strict" ||
			candidateScoringModeRaw === "weighted" ||
			candidateScoringModeRaw === "ranked-shuffle"
				? candidateScoringModeRaw
				: "ranked-shuffle";
		const candidateScoringWeights = {
			level: Number(joinOpts.candidateScoringWeights?.level ?? 1),
			freeSlots: Number(joinOpts.candidateScoringWeights?.freeSlots ?? 0.25),
			connected: Number(joinOpts.candidateScoringWeights?.connected ?? 0.5),
			bidPerByte: Number(joinOpts.candidateScoringWeights?.bidPerByte ?? 0),
			source: Number(joinOpts.candidateScoringWeights?.source ?? 0.25),
		};
		const start = Date.now();
		const cooldownUntilByHash = new Map<string, number>();
		const combinedSignal = joinOpts.signal
			? anySignal([this.closeController.signal, joinOpts.signal])
			: this.closeController.signal;
		const signal = combinedSignal as AbortSignal & { clear?: () => void };

			try {
				for (;;) {
					if (ch.closed) {
						throw new AbortError("fanout join aborted: channel closed");
					}
					if (signal.aborted) {
						throw signal.reason ?? new AbortError("fanout join aborted");
					}

						// Parent disappeared? Rejoin.
						if (ch.parent) {
							const parentPeer = this.peers.get(ch.parent);
							let connected = false;
							if (parentPeer) {
								try {
									const conns =
										this.components.connectionManager.getConnections(parentPeer.peerId) as
											| Connection[]
											| undefined;
									connected = (conns?.length ?? 0) > 0;
								} catch {
									connected = parentPeer.isReadable || parentPeer.isWritable;
								}
							}
							if (!connected) {
								ch.metrics.reparentDisconnect += 1;
								const hadChildren = ch.children.size > 0;
								this.detachFromParent(ch);
								// If we lose our parent, we are no longer on the rooted tree; detach children so
								// they can rejoin as well (prevents stable disconnected components).
								void this.kickChildren(ch).catch(() => {});
								if (hadChildren) {
									ch.rejoinCooldownUntil = Math.max(
										ch.rejoinCooldownUntil,
										Date.now() + Math.max(retryMs, RELAY_REJOIN_COOLDOWN_MS),
									);
								}
							}
						}

						if (ch.parent) {
						const endedAndComplete =
							ch.endSeqExclusive > 0 &&
							ch.missingSeqs.size === 0 &&
							ch.nextExpectedSeq >= ch.endSeqExclusive;
						const expectingData =
							ch.missingSeqs.size > 0 || (ch.maxDataAgeMs > 0 && !endedAndComplete);

						if (
							staleAfterMs > 0 &&
							ch.receivedAnyParentData &&
							ch.lastParentDataAt > 0 &&
							Date.now() - ch.lastParentDataAt > staleAfterMs &&
							expectingData
							) {
								// Parent is "alive" at the stream layer but we're not receiving
								// data at the expected rate; detach and try to re-parent.
								ch.metrics.reparentStale += 1;
								ch.parent = undefined;
								ch.level = Number.POSITIVE_INFINITY;
								ch.routeFromRoot = undefined;
								ch.routeByPeer.clear();
								ch.lastParentDataAt = 0;
								ch.receivedAnyParentData = false;
								ch.pendingJoin.clear();
								ch.pendingRouteQuery.clear();
								for (const pending of ch.pendingRouteProxy.values()) {
									clearTimeout(pending.timer);
								}
								ch.pendingRouteProxy.clear();
								void this.kickChildren(ch).catch(() => {});
								await delay(retryMs);
								continue;
							}

						if (!ch.joinedOnce) ch.joinedOnce = createDeferred();
						ch.joinedOnce.resolve();
						ch.joinedAtLeastOnce = true;
						// Once attached, we don't need a fast retry cadence; keep polling coarse to
						// avoid excessive timers when simulating many nodes in one process.
						await delay(Math.max(retryMs, 1_000));
						continue;
					}

							// `timeoutMs` is meant to bound the initial `joinChannel()` await, not to
							// stop re-parenting attempts for long-running nodes.
							if (!ch.joinedAtLeastOnce && timeoutMs > 0 && Date.now() - start > timeoutMs) {
								const bootstrapsCount = this.getBootstrapsForChannel(ch).length;
								const rootPeer = this.peers.get(ch.id.root);
								const rootNeighbor = Boolean(
									rootPeer && rootPeer.isReadable && rootPeer.isWritable,
								);
								throw new Error(
									`fanout join timed out after ${timeoutMs}ms (topic=${ch.id.topic} root=${ch.id.root} self=${this.publicKeyHash} rootNeighbor=${rootNeighbor} peers=${this.peers.size} bootstraps=${bootstrapsCount})`,
								);
							}

						const cooldownMs = ch.rejoinCooldownUntil - Date.now();
						if (cooldownMs > 0) {
							await delay(cooldownMs, { signal });
							continue;
						}

					const bootstraps = this.getBootstrapsForChannel(ch);
					let bootstrapPeers: string[] = [];
					if (bootstraps.length > 0) {
						const now = Date.now();
					const connectedCached = ch.cachedBootstrapPeers.filter((h) =>
						Boolean(this.peers.get(h)),
					);
					const due =
						ch.lastBootstrapEnsureAt === 0 ||
						bootstrapEnsureIntervalMs === 0 ||
						now - ch.lastBootstrapEnsureAt >= bootstrapEnsureIntervalMs;
					const haveEnough =
						bootstrapMaxPeers > 0
							? connectedCached.length >= bootstrapMaxPeers
							: false;
					if (due && !haveEnough) {
						ch.lastBootstrapEnsureAt = now;
						const peers = await this.ensureBootstrapPeers(
							bootstraps,
							bootstrapDialTimeoutMs,
							signal,
							bootstrapMaxPeers,
						);
						if (peers.length > 0) ch.cachedBootstrapPeers = peers;
					}
					bootstrapPeers = ch.cachedBootstrapPeers.filter((h) => Boolean(this.peers.get(h)));
				}

				let tracker: TrackerCandidate[] = [];
				if (bootstrapPeers.length > 0 && trackerCandidates > 0) {
					const now = Date.now();
					const due =
						ch.lastTrackerQueryAt === 0 ||
						trackerQueryIntervalMs === 0 ||
						now - ch.lastTrackerQueryAt >= trackerQueryIntervalMs;
					if (due) {
						ch.lastTrackerQueryAt = now;
						const res = await this.queryTrackers(
							ch,
							bootstrapPeers,
							trackerCandidates,
							trackerQueryTimeoutMs,
							signal,
						);
						if (res.length > 0) ch.cachedTrackerCandidates = res;
					}
					tracker = ch.cachedTrackerCandidates;
				}

				const candidatesByHash = new Map<
					string,
					{
						hash: string;
						addrs: Multiaddr[];
						level: number;
						freeSlots: number;
						bidPerByte: number;
						source: number;
					}
				>();

				const upsertCandidate = (c: {
					hash: string;
					addrs: Multiaddr[];
					level: number;
					freeSlots: number;
					bidPerByte: number;
					source: number;
				}) => {
					const prev = candidatesByHash.get(c.hash);
					if (!prev) {
						candidatesByHash.set(c.hash, { ...c });
						return;
					}
					if (prev.addrs.length === 0 && c.addrs.length > 0) prev.addrs = c.addrs;
					prev.level = Math.min(prev.level, c.level);
					prev.freeSlots = Math.max(prev.freeSlots, c.freeSlots);
					prev.bidPerByte = Math.max(prev.bidPerByte, c.bidPerByte);
					prev.source = Math.min(prev.source, c.source);
				};

				// Fast path: if the designated root is already a direct neighbor, try it first.
				// Without this, large join storms can repeatedly time out on arbitrary peers
				// that don't host the channel yet, starving the real root candidate.
				if (ch.id.root !== this.publicKeyHash && this.peers.has(ch.id.root)) {
					upsertCandidate({
						hash: ch.id.root,
						addrs: [],
						level: 0,
						freeSlots: Number.MAX_SAFE_INTEGER,
						bidPerByte: 0,
						source: -1,
					});
				}

				for (const c of tracker) {
					if (c.hash === this.publicKeyHash) continue;
					if (c.freeSlots <= 0) continue;
					upsertCandidate({
						hash: c.hash,
						addrs: c.addrs,
						level: c.level,
						freeSlots: c.freeSlots,
						bidPerByte: c.bidPerByte,
						source: 0,
					});
				}

				// Secondary fallback: cached tracker candidates we've learned previously.
				for (const [hash, addrs] of ch.knownCandidateAddrs) {
					if (hash === this.publicKeyHash) continue;
					if (!addrs || addrs.length === 0) continue;
					upsertCandidate({
						hash,
						addrs,
						level: 0xffff,
						freeSlots: 0,
						bidPerByte: 0,
						source: 1,
					});
				}

				// Fallback: try a bounded set of already-connected peers.
				let connectedFallbackAdded = 0;
				const connectedFallbackMax = 64;
				for (const h of this.peers.keys()) {
					if (h === this.publicKeyHash) continue;
					upsertCandidate({
						hash: h,
						addrs: [],
						level: 0xffff,
						freeSlots: 0,
						bidPerByte: 0,
						source: 2,
					});
					connectedFallbackAdded += 1;
					if (connectedFallbackAdded >= connectedFallbackMax) break;
				}

				const now = Date.now();
				const allCandidates = [...candidatesByHash.values()]
					.filter((c) => c.hash !== this.publicKeyHash)
					.sort((a, b) => {
						if (a.level !== b.level) return a.level - b.level;
						if (a.freeSlots !== b.freeSlots) return b.freeSlots - a.freeSlots;
						if (a.bidPerByte !== b.bidPerByte) return b.bidPerByte - a.bidPerByte;
						if (a.source !== b.source) return a.source - b.source;
						return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
					});

				const candidates = allCandidates.filter((c) => {
					const until = cooldownUntilByHash.get(c.hash);
					return until == null || until <= now;
				});

				// Everything is in cooldown; sleep briefly (bounded) and retry.
				if (candidates.length === 0) {
					if (allCandidates.length > 0) {
						let nextAt = Number.POSITIVE_INFINITY;
						for (const c of allCandidates) {
							const until = cooldownUntilByHash.get(c.hash);
							if (until != null && until > now) nextAt = Math.min(nextAt, until);
						}
						const waitMs =
							nextAt !== Number.POSITIVE_INFINITY ? Math.max(1, nextAt - now) : retryMs;
						const capMs = Math.max(
							retryMs,
							trackerQueryIntervalMs > 0 ? trackerQueryIntervalMs : retryMs,
						);
							await delay(Math.max(1, Math.min(waitMs, capMs)));
							continue;
						}
						await delay(retryMs);
						continue;
					}

				let ordered = [...candidates];

				if (candidateScoringMode === "ranked-shuffle") {
					// Shuffle to spread load (only among the top ranked candidates).
					if (candidateShuffleTopK > 0 && ordered.length > 1) {
						const k = Math.min(candidateShuffleTopK, ordered.length);
						for (let i = k - 1; i > 0; i--) {
							const j = Math.floor(this.random() * (i + 1));
							const tmp = ordered[i]!;
							ordered[i] = ordered[j]!;
							ordered[j] = tmp;
						}
					}
				} else if (candidateScoringMode === "weighted") {
					const wLevel = Number.isFinite(candidateScoringWeights.level)
						? Math.max(0, candidateScoringWeights.level)
						: 0;
					const wSlots = Number.isFinite(candidateScoringWeights.freeSlots)
						? Math.max(0, candidateScoringWeights.freeSlots)
						: 0;
					const wConnected = Number.isFinite(candidateScoringWeights.connected)
						? Math.max(0, candidateScoringWeights.connected)
						: 0;
					const wBid = Number.isFinite(candidateScoringWeights.bidPerByte)
						? Math.max(0, candidateScoringWeights.bidPerByte)
						: 0;
					const wSource = Number.isFinite(candidateScoringWeights.source)
						? Math.max(0, candidateScoringWeights.source)
						: 0;

					const k =
						candidateShuffleTopK > 0
							? Math.min(candidateShuffleTopK, ordered.length)
							: 0;
					if (k > 1) {
						const head = ordered.slice(0, k);
						const tail = ordered.slice(k);

						const weightOf = (c: (typeof head)[number]) => {
							const level = Math.max(0, Math.floor(c.level));
							const freeSlots = Math.max(0, Math.floor(c.freeSlots));
							const bidPerByte = Math.max(0, Math.floor(c.bidPerByte));
							const source = Math.max(0, Math.floor(c.source));
							const connected = Boolean(this.peers.get(c.hash));

							let weight = 1;
							if (wLevel > 0) weight *= 1 / (1 + wLevel * level);
							if (wSlots > 0) weight *= 1 + wSlots * freeSlots;
							if (wConnected > 0 && connected) weight *= 1 + wConnected;
							if (wBid > 0) weight *= 1 + wBid * bidPerByte;
							if (wSource > 0) weight *= 1 / (1 + wSource * source);
							return weight;
						};

						const out: typeof head = [];
						const remaining = [...head];
						while (remaining.length > 0) {
							let sum = 0;
							const weights: number[] = new Array(remaining.length);
							for (let i = 0; i < remaining.length; i++) {
								const w = weightOf(remaining[i]!);
								const v = Number.isFinite(w) ? Math.max(0, w) : 0;
								weights[i] = v;
								sum += v;
							}
							if (sum <= 0) {
								out.push(...remaining);
								break;
							}

							let r = this.random() * sum;
							let pick = 0;
							for (; pick < weights.length; pick++) {
								r -= weights[pick]!;
								if (r <= 0) break;
							}
							if (pick >= remaining.length) pick = remaining.length - 1;
							out.push(remaining[pick]!);
							remaining.splice(pick, 1);
						}

						ordered = out.concat(tail);
					}
					}

					const queue = ordered;
					const queued = new Set<string>(queue.map((c) => c.hash));
					const dialedNew = new Set<string>();
					let attempts = 0;
					for (
						let i = 0;
						i < queue.length && i < JOIN_REJECT_REDIRECT_QUEUE_MAX;
					i++
				) {
						if (signal.aborted) break;
						if (attempts >= joinAttemptsPerRound) break;
						const c = queue[i]!;
						attempts += 1;
						const wasConnected = Boolean(this.peers.get(c.hash));
						let dialOk = wasConnected;
						if (!dialOk && c.addrs.length > 0) {
							dialOk = await this.ensurePeerConnection(
								c.hash,
								c.addrs,
								bootstrapDialTimeoutMs,
								signal,
							);
						}
						if (!dialOk) {
						try {
							await this.sendTrackerFeedback(
								ch,
								bootstrapPeers,
								c.hash,
								TRACKER_FEEDBACK_DIAL_FAILED,
							);
						} catch {
							// ignore
						}
						if (candidateCooldownMs > 0) {
							cooldownUntilByHash.set(c.hash, Date.now() + candidateCooldownMs * 5);
							}
							continue;
						}
						if (!wasConnected) {
							dialedNew.add(c.hash);
						}
						const reqId = (this.random() * 0xffffffff) >>> 0;
						const res = await this.tryJoinOnce(ch, c.hash, reqId, joinReqTimeoutMs, signal);

						if (res.redirects && res.redirects.length > 0) {
						for (const r of res.redirects) {
							if (queue.length >= JOIN_REJECT_REDIRECT_QUEUE_MAX) break;
							if (!r?.hash) continue;
							if (r.hash === this.publicKeyHash) continue;
							if (!r.addrs || r.addrs.length === 0) continue;
							if (queued.has(r.hash)) continue;
							queued.add(r.hash);
							this.cacheKnownCandidateAddrs(ch, r.hash, r.addrs);
							queue.push({
								hash: r.hash,
								addrs: r.addrs,
								level: 0xffff,
								freeSlots: 0,
								bidPerByte: 0,
								source: 3,
							});
						}
					}

					if (res.ok) {
						try {
							await this.sendTrackerFeedback(
								ch,
								bootstrapPeers,
								c.hash,
								TRACKER_FEEDBACK_JOINED,
							);
						} catch {
							// ignore
						}
							cooldownUntilByHash.delete(c.hash);
							break;
						}

						// Connection hygiene: if we dialed this candidate just for joining and it
						// didn't work out, drop it so large join storms don't accumulate thousands
						// of idle neighbours (important for large sims and long-running clients).
						if (!wasConnected && !bootstrapPeers.includes(c.hash)) {
							const stream = this.peers.get(c.hash);
							if (stream) {
								void this.components.connectionManager
									.closeConnections(stream.peerId)
									.catch(() => {});
							}
						}

						if (res.timedOut) {
							try {
								await this.sendTrackerFeedback(
									ch,
								bootstrapPeers,
								c.hash,
								TRACKER_FEEDBACK_JOIN_TIMEOUT,
							);
						} catch {
							// ignore
						}
						if (candidateCooldownMs > 0) {
							cooldownUntilByHash.set(c.hash, Date.now() + candidateCooldownMs * 2);
						}
						continue;
					}

					const rejectReason = res.rejectReason ?? 0;
					if (candidateCooldownMs > 0) {
						let factor = 2;
						if (rejectReason === JOIN_REJECT_LOW_BID) factor = 30;
						else if (
							rejectReason === JOIN_REJECT_NO_CAPACITY ||
							rejectReason === JOIN_REJECT_NOT_ATTACHED
						) {
							factor = 1;
						}
						cooldownUntilByHash.set(c.hash, Date.now() + candidateCooldownMs * factor);
					}
					try {
						await this.sendTrackerFeedback(
							ch,
							bootstrapPeers,
							c.hash,
							TRACKER_FEEDBACK_JOIN_REJECT,
							rejectReason,
							);
						} catch {
							// ignore
						}
					}

						if (ch.parent) {
							// Keep only the selected parent + bootstraps. Everything else we dialed is
							// best-effort and can be re-dialed if needed later.
							for (const h of dialedNew) {
								if (h === ch.parent) continue;
								if (bootstrapPeers.includes(h)) continue;
								const stream = this.peers.get(h);
								if (!stream) continue;
								void this.components.connectionManager.closeConnections(stream.peerId).catch(() => {});
							}
							continue;
						}
						await delay(retryMs);
					}
				} finally {
					signal.clear?.();
			}
	}

	private async tryJoinOnce(
		ch: ChannelState,
		parentHash: string,
		reqId: number,
		timeoutMs: number,
		signal: AbortSignal,
	): Promise<JoinAttemptResult> {
		if (ch.parent) return { ok: true };
		if (!this.peers.get(parentHash)) return { ok: false, timedOut: true };
		const p = new Promise<JoinAttemptResult>((resolve) => {
			ch.pendingJoin.set(reqId, { resolve });
		});
		await this._sendControl(parentHash, encodeJoinReq(ch.id.key, reqId, ch.bidPerByte));
		const res = await Promise.race([
			p,
			delay(Math.max(1, timeoutMs), { signal }).then((): JoinAttemptResult => ({
				ok: false,
				timedOut: true,
			})),
		]);
		if (res.timedOut) ch.pendingJoin.delete(reqId);
		return res;
	}

	private async kickChildren(ch: ChannelState) {
		const children = [...ch.children.keys()];
		ch.children.clear();
		ch.dataWriteFailStreakByChild.clear();
		if (children.length === 0) return;
		await this._sendControlMany(children, encodeKick(ch.id.key));
	}

	public async onDataMessage(
		from: any,
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number,
	) {
		const ignore = this.shouldIgnore(message, seenBefore);
		const raw = message.data as Uint8ArrayList | Uint8Array | undefined;
		const data = raw instanceof Uint8ArrayList ? raw.subarray() : raw;
		if (!data || data.length === 0) return false;

			const kind = data[0]!;
			const fromHash = from.hashcode();

			// Control-plane + tracker messages carry channelKey (32 bytes) at offset 1.
			if (
				kind === MSG_JOIN_REQ ||
				kind === MSG_JOIN_ACCEPT ||
				kind === MSG_JOIN_REJECT ||
				kind === MSG_KICK ||
				kind === MSG_END ||
				kind === MSG_UNICAST ||
				kind === MSG_UNICAST_ACK ||
				kind === MSG_ROUTE_QUERY ||
				kind === MSG_ROUTE_REPLY ||
				kind === MSG_PUBLISH_PROXY ||
				kind === MSG_LEAVE ||
				kind === MSG_REPAIR_REQ ||
				kind === MSG_FETCH_REQ ||
				kind === MSG_IHAVE ||
				kind === MSG_TRACKER_ANNOUNCE ||
				kind === MSG_TRACKER_QUERY ||
				kind === MSG_TRACKER_REPLY ||
				kind === MSG_TRACKER_FEEDBACK ||
				kind === MSG_PROVIDER_ANNOUNCE ||
				kind === MSG_PROVIDER_QUERY ||
				kind === MSG_PROVIDER_REPLY
			) {
				if (data.length < 1 + 32) return false;
				const channelKey = data.subarray(1, 33);
				const suffixKey = toBase64(channelKey.subarray(0, 24));
				this.recordControlReceive(suffixKey, kind, data.byteLength);
				const ch = this.channelsBySuffixKey.get(suffixKey);
				// DirectStream de-duplicates by message-id, but FanoutTree unicast (and its ACK)
				// legitimately reflect "up to root, then down" through the same top-level branch.
				// Allow a duplicate only when it arrives from the parent (downstream reflection).
				if (
					ignore &&
					!(
						(kind === MSG_UNICAST || kind === MSG_UNICAST_ACK) &&
						ch &&
						fromHash === ch.parent
					)
				) {
					return false;
				}

			if (kind === MSG_TRACKER_ANNOUNCE) {
				if (data.length < 1 + 32 + 4 + 2 + 2 + 2 + 4 + 1) return false;
				const ttlMs = readU32BE(data, 33);
				const level = readU16BE(data, 37);
				// maxChildren is currently unused, but kept in the wire format.
				// const maxChildren = readU16BE(data, 39);
				const freeSlots = readU16BE(data, 41);
				const bidPerByte = readU32BE(data, 43);
				const addrCount = data[47]!;
				let offset = 48;

				const addrs: Uint8Array[] = [];
				const max = Math.min(addrCount, 16);
				for (let i = 0; i < max; i++) {
					if (offset + 2 > data.length) break;
					const len = readU16BE(data, offset);
					offset += 2;
					if (offset + len > data.length) break;
					addrs.push(data.subarray(offset, offset + len));
					offset += len;
				}

				if (addrs.length === 0) {
					try {
						const peer: any = await this.components.peerStore.get(peerStream.peerId);
						const addresses: any[] = Array.isArray(peer?.addresses) ? peer.addresses : [];
						for (const a of addresses) {
							const ma: any = a?.multiaddr ?? a;
							const bytes: Uint8Array | undefined = ma?.bytes;
							if (bytes instanceof Uint8Array) addrs.push(bytes);
							if (addrs.length >= 16) break;
						}
					} catch {
						// ignore
					}
				}

				let byPeer = this.trackerBySuffixKey.get(suffixKey);
				if (!byPeer) {
					byPeer = new Map<string, TrackerEntry>();
					this.trackerBySuffixKey.set(suffixKey, byPeer);
				}

				const now = Date.now();
				this.touchTrackerNamespace(suffixKey, now);
				const ttl = Math.min(ttlMs, 120_000);
				if (ttl === 0) {
					byPeer.delete(fromHash);
					this.pruneTrackerNamespaceIfEmpty(suffixKey);
					return true;
				}
				// LRU by announce freshness, with a hard per-channel cap.
				byPeer.delete(fromHash);
				byPeer.set(fromHash, {
					hash: fromHash,
					level,
					freeSlots,
					bidPerByte,
					addrs,
					expiresAt: now + ttl,
				});
				while (byPeer.size > TRACKER_DIRECTORY_MAX_ENTRIES) {
					const oldest = byPeer.keys().next().value as string | undefined;
					if (!oldest) break;
					byPeer.delete(oldest);
				}
				return true;
			}

			if (kind === MSG_TRACKER_QUERY) {
				if (data.length < 1 + 32 + 4 + 2) return false;
				const reqId = readU32BE(data, 33);
				const want = readU16BE(data, 37);

				const now = Date.now();
				this.touchTrackerNamespace(suffixKey, now);
				const byPeer = this.trackerBySuffixKey.get(suffixKey);
				const entries: TrackerEntry[] = [];
				if (byPeer) {
					for (const [hash, e] of byPeer) {
						if (e.expiresAt <= now) {
							byPeer.delete(hash);
							continue;
						}
						if (hash === fromHash) continue;
						if (e.freeSlots <= 0) continue;
						entries.push(e);
					}
					this.pruneTrackerNamespaceIfEmpty(suffixKey);
				}

				entries.sort((a, b) => {
					if (a.level !== b.level) return a.level - b.level;
					if (a.freeSlots !== b.freeSlots) return b.freeSlots - a.freeSlots;
					if (a.bidPerByte !== b.bidPerByte) return b.bidPerByte - a.bidPerByte;
					return a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0;
				});

				const picked = entries.slice(0, clampU16(want));
				void this._sendControl(fromHash, encodeTrackerReply(channelKey, reqId, picked));
				return true;
			}

			if (kind === MSG_TRACKER_FEEDBACK) {
				if (data.length < 1 + 32 + 1 + 1 + 1) return false;
				const hashLen = data[33]!;
				let offset = 34;
				if (offset + hashLen + 2 > data.length) return false;
				const candidateHash = textDecoder.decode(data.subarray(offset, offset + hashLen));
				offset += hashLen;
				const event = data[offset++]! & 0xff;
				const reason = data[offset++]! & 0xff;

				const byPeer = this.trackerBySuffixKey.get(suffixKey);
				if (!byPeer) return true;
				this.touchTrackerNamespace(suffixKey);
				const entry = byPeer.get(candidateHash);
				if (!entry) return true;

				const now = Date.now();
				if (entry.expiresAt <= now) {
					byPeer.delete(candidateHash);
					this.pruneTrackerNamespaceIfEmpty(suffixKey);
					return true;
				}

				if (event === TRACKER_FEEDBACK_JOINED) {
					entry.freeSlots = clampU16(Math.max(0, entry.freeSlots - 1));
					return true;
				}
				if (event === TRACKER_FEEDBACK_DIAL_FAILED || event === TRACKER_FEEDBACK_JOIN_TIMEOUT) {
					byPeer.delete(candidateHash);
					this.pruneTrackerNamespaceIfEmpty(suffixKey);
					return true;
				}
				if (event === TRACKER_FEEDBACK_JOIN_REJECT) {
					if (reason === JOIN_REJECT_NO_CAPACITY || reason === JOIN_REJECT_NOT_ATTACHED) {
						entry.freeSlots = 0;
						// Expire earlier to reduce time-to-recovery if the entry is stale.
						entry.expiresAt = Math.min(entry.expiresAt, now + 2_000);
					}
					return true;
				}

				return true;
			}

				if (kind === MSG_PROVIDER_ANNOUNCE) {
					if (data.length < 1 + 32 + 4 + 1) return false;
					const ttlMs = readU32BE(data, 33);
					const addrCount = data[37]!;
					let offset = 38;

					const addrs: Uint8Array[] = [];
					const max = Math.min(addrCount, 16);
					for (let i = 0; i < max; i++) {
						if (offset + 2 > data.length) break;
						const len = readU16BE(data, offset);
						offset += 2;
						if (offset + len > data.length) break;
						addrs.push(data.subarray(offset, offset + len));
						offset += len;
					}

					if (addrs.length === 0) {
						try {
							const peer: any = await this.components.peerStore.get(peerStream.peerId);
							const addresses: any[] = Array.isArray(peer?.addresses)
								? peer.addresses
								: [];
							for (const a of addresses) {
								const ma: any = a?.multiaddr ?? a;
								const bytes: Uint8Array | undefined = ma?.bytes;
								if (bytes instanceof Uint8Array) addrs.push(bytes);
								if (addrs.length >= 16) break;
							}
						} catch {
							// ignore
						}
					}

					let byPeer = this.providerBySuffixKey.get(suffixKey);
					if (!byPeer) {
						byPeer = new Map<string, ProviderEntry>();
						this.providerBySuffixKey.set(suffixKey, byPeer);
					}

					const now = Date.now();
					this.touchProviderNamespace(suffixKey, now);
					const ttl = Math.min(ttlMs, 120_000);
					if (ttl === 0) {
						byPeer.delete(fromHash);
						this.pruneProviderNamespaceIfEmpty(suffixKey);
						return true;
					}

					// LRU by announce freshness, with a hard per-namespace cap.
					byPeer.delete(fromHash);
					byPeer.set(fromHash, {
						hash: fromHash,
						addrs,
						expiresAt: now + ttl,
					});
					while (byPeer.size > PROVIDER_DIRECTORY_MAX_ENTRIES) {
						const oldest = byPeer.keys().next().value as string | undefined;
						if (!oldest) break;
						byPeer.delete(oldest);
					}
					this.pruneProviderNamespaceIfEmpty(suffixKey);
					return true;
				}

			if (kind === MSG_PROVIDER_QUERY) {
				if (data.length < 1 + 32 + 4 + 2 + 4) return false;
				const reqId = readU32BE(data, 33);
				const want = readU16BE(data, 37);
				const seed = readU32BE(data, 39);

				const now = Date.now();
				this.touchProviderNamespace(suffixKey, now);
				const byPeer = this.providerBySuffixKey.get(suffixKey);
				const entries: ProviderEntry[] = [];
				if (byPeer) {
					for (const [hash, e] of byPeer) {
						if (e.expiresAt <= now) {
							byPeer.delete(hash);
							continue;
						}
						if (hash === fromHash) continue;
						entries.push(e);
					}
					this.pruneProviderNamespaceIfEmpty(suffixKey);
				}

				// Shuffle to spread load (deterministic if seed is provided).
				if (entries.length > 1) {
					if (seed !== 0) {
						let x = seed >>> 0;
						for (let i = entries.length - 1; i > 0; i--) {
							x ^= x << 13;
							x ^= x >>> 17;
							x ^= x << 5;
							const j = (x >>> 0) % (i + 1);
							const tmp = entries[i]!;
							entries[i] = entries[j]!;
							entries[j] = tmp;
						}
					} else {
						for (let i = entries.length - 1; i > 0; i--) {
							const j = Math.floor(this.random() * (i + 1));
							const tmp = entries[i]!;
							entries[i] = entries[j]!;
							entries[j] = tmp;
						}
					}
				}

				const picked = entries.slice(0, clampU16(want));
				void this._sendControl(fromHash, encodeProviderReply(channelKey, reqId, picked));
				return true;
			}

			if (kind === MSG_PROVIDER_REPLY) {
				if (data.length < 1 + 32 + 4 + 1) return false;
				const reqId = readU32BE(data, 33);

				const pendingByReq = this.pendingProviderQueryBySuffixKey.get(suffixKey);
				const pending = pendingByReq?.get(reqId);
				if (!pending) return true;
				pendingByReq!.delete(reqId);

				const count = data[37]!;
				let offset = 38;
				const candidates: FanoutProviderCandidate[] = [];
				const max = Math.min(count, 255);

				const now = Date.now();
				this.touchProviderNamespace(suffixKey, now);
				let cache = this.providerBySuffixKey.get(suffixKey);
				if (!cache) {
					cache = new Map<string, ProviderEntry>();
					this.providerBySuffixKey.set(suffixKey, cache);
				}

				for (let i = 0; i < max; i++) {
					if (offset + 1 > data.length) break;
					const hashLen = data[offset++]!;
					if (offset + hashLen > data.length) break;
					const hash = textDecoder.decode(data.subarray(offset, offset + hashLen));
					offset += hashLen;
					if (offset + 1 > data.length) break;
					const addrCount = data[offset++]!;
					const addrs: Multiaddr[] = [];
					const addrBytes: Uint8Array[] = [];
					const addrMax = Math.min(addrCount, 16);
					for (let j = 0; j < addrMax; j++) {
						if (offset + 2 > data.length) break;
						const len = readU16BE(data, offset);
						offset += 2;
						if (offset + len > data.length) break;
						const bytes = data.subarray(offset, offset + len);
						offset += len;
						addrBytes.push(bytes);
						try {
							addrs.push(multiaddr(bytes));
						} catch {
							// ignore invalid multiaddrs
						}
					}
					candidates.push({ hash, addrs });
					cache.delete(hash);
					cache.set(hash, {
						hash,
						addrs: addrBytes,
						expiresAt: now + 60_000,
					});
				}
				while (cache.size > PROVIDER_DIRECTORY_MAX_ENTRIES) {
					const oldest = cache.keys().next().value as string | undefined;
					if (!oldest) break;
					cache.delete(oldest);
				}
				this.pruneProviderNamespaceIfEmpty(suffixKey);

				pending.resolve(candidates);
				return true;
			}

			if (kind === MSG_TRACKER_REPLY) {
				if (!ch) return true;
				if (data.length < 1 + 32 + 4 + 1) return false;
				const reqId = readU32BE(data, 33);
				const pending = ch.pendingTrackerQuery.get(reqId);
				if (!pending) return true;
				ch.pendingTrackerQuery.delete(reqId);

				const count = data[37]!;
				let offset = 38;
				const candidates: TrackerCandidate[] = [];
				const max = Math.min(count, 255);
					for (let i = 0; i < max; i++) {
					if (offset + 1 > data.length) break;
					const hashLen = data[offset++]!;
					if (offset + hashLen > data.length) break;
					const hash = textDecoder.decode(data.subarray(offset, offset + hashLen));
					offset += hashLen;
					if (offset + 2 + 2 + 4 + 1 > data.length) break;
					const level = readU16BE(data, offset);
					offset += 2;
					const freeSlots = readU16BE(data, offset);
					offset += 2;
					const bidPerByte = readU32BE(data, offset);
					offset += 4;
					const addrCount = data[offset++]!;
					const addrs: Multiaddr[] = [];
					const addrMax = Math.min(addrCount, 16);
					for (let j = 0; j < addrMax; j++) {
						if (offset + 2 > data.length) break;
						const len = readU16BE(data, offset);
						offset += 2;
						if (offset + len > data.length) break;
						const bytes = data.subarray(offset, offset + len);
						offset += len;
						try {
							addrs.push(multiaddr(bytes));
						} catch {
							// ignore invalid multiaddrs
						}
							}
							candidates.push({ hash, level, freeSlots, bidPerByte, addrs });
							this.cacheKnownCandidateAddrs(ch, hash, addrs);
						}

						pending.resolve(candidates);
					return true;
				}

					if (!ch) return true;
					if (ch.closed) return true;

					if (kind === MSG_LEAVE) {
						// Best-effort: allow a child to explicitly detach to immediately free capacity.
						if (!ch.children.has(fromHash)) return true;
						ch.children.delete(fromHash);
						ch.dataWriteFailStreakByChild.delete(fromHash);
						ch.channelPeers.delete(fromHash);
						ch.lazyPeers.delete(fromHash);
						ch.haveByPeer.delete(fromHash);
						return true;
					}

					if (kind === MSG_ROUTE_QUERY) {
					if (data.length < 1 + 32 + 4 + 1) return false;
					const reqId = readU32BE(data, 33);
					const hashLen = data[37]!;
					if (hashLen === 0 || 38 + hashLen > data.length) {
						void this._sendControl(fromHash, encodeRouteReply(ch.id.key, reqId)).catch(
							() => {},
						);
						return true;
					}
					const targetHash = textDecoder.decode(data.subarray(38, 38 + hashLen));

					const localRoute =
						targetHash === this.publicKeyHash && ch.routeFromRoot
							? ch.routeFromRoot
							: this.getCachedRoute(ch, targetHash);
					if (this.isRouteValidForChannel(ch, localRoute)) {
						void this._sendControl(
							fromHash,
							encodeRouteReply(ch.id.key, reqId, localRoute),
						).catch(() => {});
						return true;
					}

					if (ch.isRoot) {
						const rootRoute = ch.children.has(targetHash)
							? [ch.id.root, targetHash]
							: undefined;
						if (rootRoute) this.cacheRoute(ch, rootRoute);
						if (rootRoute) {
							void this._sendControl(
								fromHash,
								encodeRouteReply(ch.id.key, reqId, rootRoute),
							).catch(() => {});
							return true;
						}
					}

					const fromParent = !ch.isRoot && ch.parent != null && fromHash === ch.parent;

					// Child->parent lookups still go upstream first, which keeps cross-branch
					// route discovery efficient when caches are warm.
					if (!ch.isRoot && !fromParent && ch.parent) {
						this.proxyRouteQuery(ch, fromHash, reqId, targetHash, [ch.parent]);
						return true;
					}

					// Cache miss on a parent->child (or root->child) query:
					// recursively search subtree branches and reply with the first valid route.
					this.proxyRouteQuery(ch, fromHash, reqId, targetHash, [...ch.children.keys()]);
					return true;
				}

				if (kind === MSG_ROUTE_REPLY) {
					if (data.length < 1 + 32 + 4 + 1) return false;
					const reqId = readU32BE(data, 33);
					const routeCount = Math.min(255, data[37]!);
					const { route } = decodeRoute(data, 38, routeCount);
					const parsedRoute = this.isRouteValidForChannel(ch, route) ? route : undefined;
					if (parsedRoute) {
						this.cacheRoute(ch, parsedRoute);
					}

					const pendingLocal = ch.pendingRouteQuery.get(reqId);
					if (pendingLocal) {
						ch.pendingRouteQuery.delete(reqId);
						pendingLocal.resolve(parsedRoute);
						return true;
					}

					const proxy = ch.pendingRouteProxy.get(reqId);
					if (!proxy) return true;

					if (!proxy.expectedReplies.has(fromHash)) return true;
					proxy.expectedReplies.delete(fromHash);

					if (parsedRoute) {
						this.completeRouteProxy(ch, reqId, parsedRoute);
						return true;
					}

					if (proxy.expectedReplies.size === 0) {
						this.completeRouteProxy(ch, reqId);
					}
					return true;
				}

				if (kind === MSG_IHAVE) {
				if (data.length < 1 + 32 + 4 + 4) return false;
				const haveFrom = readU32BE(data, 33);
				const haveToExclusive = readU32BE(data, 37);
				const now = Date.now();
				const prev = ch.haveByPeer.get(fromHash);
				if (prev) {
					prev.haveFrom = haveFrom;
					prev.haveToExclusive = haveToExclusive;
					prev.updatedAt = now;
					} else {
						ch.haveByPeer.set(fromHash, {
							haveFrom,
							haveToExclusive,
							updatedAt: now,
							requests: 0,
							successes: 0,
						});
					}
					this.touchPeerHint(ch, fromHash, now);

					// Opportunistic reciprocity: if we still have room in our lazy mesh, add
					// peers that are actively exchanging IHAVE summaries with us.
					if (
					ch.neighborRepair &&
					ch.neighborMeshPeers > 0 &&
					ch.lazyPeers.size < ch.neighborMeshPeers &&
					fromHash !== ch.parent &&
					!ch.children.has(fromHash) &&
					this.peers.get(fromHash)
				) {
					ch.lazyPeers.add(fromHash);
				}

				return true;
			}

					if (kind === MSG_JOIN_REQ) {
						if (data.length < 1 + 32 + 4 + 4) return false;
						const reqId = readU32BE(data, 33);
						const bidPerByte = readU32BE(data, 37);
						this.pruneDisconnectedChildren(ch);

						// Only accept if we're already attached.
						if (!ch.isRoot && !ch.parent) {
							void this.sendJoinReject(
								ch,
								fromHash,
								reqId,
								JOIN_REJECT_NOT_ATTACHED,
							).catch(() => {});
							return true;
						}

						// Only accept children if we can prove we're on the rooted tree.
						// Otherwise, disconnected components can "stabilize" by joining via unrooted parents.
						if (!ch.isRoot) {
							const route = ch.routeFromRoot;
							const rooted =
								Array.isArray(route) &&
								route.length >= 2 &&
								route[0] === ch.id.root &&
								route[route.length - 1] === this.publicKeyHash;
							if (!rooted) {
								void this.sendJoinReject(
									ch,
									fromHash,
									reqId,
									JOIN_REJECT_NOT_ATTACHED,
								).catch(() => {});
								return true;
							}
						}

						if (ch.effectiveMaxChildren <= 0) {
							void this.sendJoinReject(
								ch,
								fromHash,
								reqId,
							JOIN_REJECT_NO_CAPACITY,
						).catch(() => {});
						return true;
					}

					if (!ch.children.has(fromHash) && ch.children.size >= ch.effectiveMaxChildren) {
						if (!ch.allowKick) {
							void this.sendJoinReject(
								ch,
								fromHash,
								reqId,
								JOIN_REJECT_NO_CAPACITY,
							).catch(() => {});
							void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
							return true;
						}

					let worstChild: string | undefined;
					let worstBid = Number.POSITIVE_INFINITY;
					for (const [childHash, info] of ch.children) {
						if (info.bidPerByte < worstBid) {
							worstBid = info.bidPerByte;
							worstChild = childHash;
						}
					}

					if (worstChild == null || bidPerByte <= worstBid) {
							void this.sendJoinReject(
								ch,
								fromHash,
								reqId,
								JOIN_REJECT_LOW_BID,
							).catch(() => {});
							void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
							return true;
						}

						ch.children.delete(worstChild);
						ch.dataWriteFailStreakByChild.delete(worstChild);
						void this._sendControl(worstChild, encodeKick(ch.id.key));
					}

					ch.children.set(fromHash, { bidPerByte });
					this.touchPeerHint(ch, fromHash);
					void this._sendControl(
						fromHash,
						encodeJoinAccept(ch.id.key, reqId, ch.level, ch.routeFromRoot),
					);
					void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
					return true;
				}

			if (kind === MSG_JOIN_ACCEPT || kind === MSG_JOIN_REJECT) {
				if (data.length < 1 + 32 + 4) return false;
				const reqId = readU32BE(data, 33);
				const pending = ch.pendingJoin.get(reqId);
				if (!pending) return true;
				ch.pendingJoin.delete(reqId);

					if (kind === MSG_JOIN_ACCEPT) {
						if (data.length < 1 + 32 + 4 + 2 + 1) return false;
						const parentLevel = readU16BE(data, 37);
						const routeCount = Math.min(255, data[39]!);
						let offset = 40;
						const parentRouteFromRoot: string[] = [];
						const max = Math.min(routeCount, MAX_ROUTE_HOPS);
							for (let i = 0; i < max; i++) {
								if (offset + 1 > data.length) break;
								const len = data[offset++]!;
								if (len === 0) break;
								if (offset + len > data.length) break;
								parentRouteFromRoot.push(
									textDecoder.decode(data.subarray(offset, offset + len)),
								);
								offset += len;
							}

							const hasValidParentRoute =
								parentRouteFromRoot.length > 0 &&
								parentRouteFromRoot[0] === ch.id.root &&
								parentRouteFromRoot[parentRouteFromRoot.length - 1] === fromHash;

							// Defensive: a JOIN_ACCEPT without a rooted route token (unless the parent is the
							// actual root) can create stable disconnected components. Treat it as a reject.
							if (fromHash !== ch.id.root && !hasValidParentRoute) {
								pending.resolve({ ok: false, rejectReason: JOIN_REJECT_NOT_ATTACHED });
								return true;
							}

								ch.parent = fromHash;
								ch.level = parentLevel + 1;
								ch.joinedAtLeastOnce = true;
								ch.lastParentDataAt = Date.now();
								// Treat JOIN_ACCEPT as parent liveness for stale re-parenting:
								// if callers enable `staleAfterMs`, we should be able to detach even
						// before the first data message arrives (for example, during churn
						// or when a component is partitioned from the root).
							ch.receivedAnyParentData = true;
							// Build/refresh a route token that enables economical unicast.
							if (
								hasValidParentRoute
							) {
								ch.routeFromRoot = [...parentRouteFromRoot, this.publicKeyHash];
								} else if (fromHash === ch.id.root) {
									// Minimal fallback: parent is the root.
									ch.routeFromRoot = [ch.id.root, this.publicKeyHash];
							}
							this.touchPeerHint(ch, fromHash);
							pending.resolve({ ok: true });
							this.dispatchEvent(
								new CustomEvent("fanout:joined", {
									detail: { topic: ch.id.topic, root: ch.id.root, parent: fromHash },
						}),
					);
					void this.announceToTrackers(ch, this.closeController.signal).catch(() => {});
					} else {
						if (data.length < 1 + 32 + 4 + 1) return false;
						const reason = data[37]! & 0xff;
						const redirects: Array<{ hash: string; addrs: Multiaddr[] }> = [];
						if (data.length >= 1 + 32 + 4 + 1 + 1) {
							const count = Math.min(255, data[38]!);
							let offset = 39;
							const max = Math.min(count, JOIN_REJECT_REDIRECT_MAX);
							for (let i = 0; i < max; i++) {
								if (offset + 1 > data.length) break;
								const hashLen = data[offset++]!;
								if (hashLen === 0) break;
								if (offset + hashLen > data.length) break;
								const hash = textDecoder.decode(data.subarray(offset, offset + hashLen));
								offset += hashLen;
								if (offset + 1 > data.length) break;
								const addrCount = Math.min(255, data[offset++]!);
								const addrs: Multiaddr[] = [];
								const addrMax = Math.min(addrCount, JOIN_REJECT_REDIRECT_ADDR_MAX);
								for (let j = 0; j < addrMax; j++) {
									if (offset + 2 > data.length) break;
									const len = readU16BE(data, offset);
									offset += 2;
									if (offset + len > data.length) break;
									const bytes = data.subarray(offset, offset + len);
									offset += len;
									try {
										addrs.push(multiaddr(bytes));
									} catch {
										// ignore invalid multiaddrs
									}
									}
									if (hash && addrs.length > 0) {
										redirects.push({ hash, addrs });
										this.cacheKnownCandidateAddrs(ch, hash, addrs);
									}
								}
							}
						pending.resolve({ ok: false, rejectReason: reason, redirects });
					}
					return true;
					}
	
					if (kind === MSG_PUBLISH_PROXY) {
						// Requires an open channel state
						if (!ch) return true;

						// Only accept/forward within established tree edges to keep the data-plane economical.
						const isFromParent = fromHash === ch.parent;
						const isFromChild = ch.children.has(fromHash);
							if (!isFromParent && !isFromChild) return true;

							const payload = data.subarray(33);
							if (isFromChild) {
								const ok = this.takeIngressBudget(
									ch,
									"proxy-publish",
									fromHash,
									payload.byteLength,
								);
								if (!ok) {
									ch.metrics.proxyPublishDrops += 1;
									return true;
								}
							}
							this.touchPeerHint(ch, fromHash);

							if (ch.isRoot) {
								// Only accept upstream proxy publishes from established children.
								if (!isFromChild) return true;
							const seq = ch.seq++;
							const message = await this._sendData(ch, [...ch.children.keys()], seq, payload);
							this.dispatchEvent(
								new CustomEvent("fanout:data", {
									detail: {
										topic: ch.id.topic,
										root: ch.id.root,
										seq,
										payload,
										from: this.publicKeyHash,
										origin: this.publicKeyHash,
										timestamp: message.header.timestamp,
										message,
									},
								}),
							);
							return true;
						}

						// Upstream forwarding (child -> ... -> root).
						if (!isFromChild) return true;
						if (!ch.parent) return true;
						const up = this.peers.get(ch.parent);
						if (!up) return true;
						void up
							.waitForWrite(
								message.bytes(),
								Number(message.header.priority ?? CONTROL_PRIORITY),
								this.closeController.signal,
							)
							.catch(() => {});
						return true;
					}

					if (kind === MSG_UNICAST_ACK) {
						// Requires an open channel state
						if (!ch) return true;

						// Only accept/forward within established tree edges.
						const isFromParent = fromHash === ch.parent;
						const isFromChild = ch.children.has(fromHash);
						if (!isFromParent && !isFromChild) return true;

						if (data.length < 1 + 32 + 8 + 1) return false;
						const ackToken = readU64BE(data, 33);
						const routeCount = Math.min(255, data[41]!);
						const decoded = decodeRoute(data, 42, routeCount);
						const route = decoded.route;
						const target = route.length > 0 ? route[route.length - 1]! : "";
						const origin =
							message.header.signatures?.publicKeys?.[0]?.hashcode?.() ?? fromHash;

						this.touchPeerHint(ch, fromHash);

						const settleLocal = () => {
							const pending = ch.pendingUnicastAck.get(ackToken);
							if (!pending) return;
							if (pending.expectedOrigin !== origin) return;
							pending.resolve();
						};

						// Root routes downward using the provided token.
						if (ch.isRoot) {
							if (route.length === 0 || route[0] !== ch.id.root) return true;
							if (target === this.publicKeyHash) {
								settleLocal();
								return true;
							}
							const nextHop = route[1];
							if (!nextHop || !ch.children.has(nextHop)) return true;
							const stream = this.peers.get(nextHop);
							if (!stream) return true;
							void stream
								.waitForWrite(
									message.bytes(),
									Number(message.header.priority ?? CONTROL_PRIORITY),
									this.closeController.signal,
								)
								.catch(() => {});
							return true;
						}

						// Downstream routing: parent -> child -> ... -> sender.
						if (isFromParent) {
							const selfHash = this.publicKeyHash;
							const myIndex = route.indexOf(selfHash);
							if (myIndex < 0) return true;
							if (myIndex === route.length - 1) {
								settleLocal();
								return true;
							}
							const nextHop = route[myIndex + 1];
							if (!nextHop || !ch.children.has(nextHop)) return true;
							const stream = this.peers.get(nextHop);
							if (!stream) return true;
							void stream
								.waitForWrite(
									message.bytes(),
									Number(message.header.priority ?? CONTROL_PRIORITY),
									this.closeController.signal,
								)
								.catch(() => {});
							return true;
						}

						// Upstream forwarding (child -> ... -> root). Route token is only used by the root.
						if (!ch.parent) return true;
						const up = this.peers.get(ch.parent);
						if (!up) return true;
						void up
							.waitForWrite(
								message.bytes(),
								Number(message.header.priority ?? CONTROL_PRIORITY),
								this.closeController.signal,
							)
							.catch(() => {});
						return true;
					}

					if (kind === MSG_UNICAST) {
						// Requires an open channel state
						if (!ch) return true;
	
					// Only accept/forward within established tree edges to keep the data-plane economical.
					const isFromParent = fromHash === ch.parent;
					const isFromChild = ch.children.has(fromHash);
					if (!isFromParent && !isFromChild) return true;
					if (isFromChild) {
						const ok = this.takeIngressBudget(
							ch,
							"unicast",
							fromHash,
							data.byteLength,
						);
						if (!ok) {
							ch.metrics.unicastDrops += 1;
							return true;
						}
					}

					if (data.length < 1 + 32 + 1 + 1) return false;
					const flags = data[33]! & 0xff;
					let offset = 34;
					let ackToken: bigint | undefined;
					if (flags & UNICAST_FLAG_ACK) {
						if (data.length < offset + 8 + 1) return false;
						ackToken = readU64BE(data, offset);
						offset += 8;
					}
					const routeCount = Math.min(255, data[offset]!);
					offset += 1;
					const decoded = decodeRoute(data, offset, routeCount);
					const route = decoded.route;
					offset = decoded.offset;
					let replyRoute: string[] | undefined;
					if (ackToken != null) {
						if (data.length < offset + 1) return false;
						const replyCount = Math.min(255, data[offset]!);
						offset += 1;
						const decodedReply = decodeRoute(data, offset, replyCount);
						replyRoute = decodedReply.route;
						offset = decodedReply.offset;
					}
					const payload = data.subarray(offset);
					const target = route.length > 0 ? route[route.length - 1]! : "";
						const origin =
							message.header.signatures?.publicKeys?.[0]?.hashcode?.() ?? fromHash;

							this.touchPeerHint(ch, fromHash);

							// Root routes downward using the provided token.
							if (ch.isRoot) {
							if (route.length === 0 || route[0] !== ch.id.root) return true;
						if (target === this.publicKeyHash) {
							this.dispatchEvent(
								new CustomEvent("fanout:unicast", {
									detail: {
										topic: ch.id.topic,
										root: ch.id.root,
										route,
										payload,
										from: fromHash,
										origin,
										to: target,
										timestamp: message.header.timestamp,
										message,
									},
								}),
							);
							if (ackToken != null) {
								const canAck =
									replyRoute &&
									replyRoute.length > 0 &&
									replyRoute[0] === ch.id.root &&
									replyRoute[replyRoute.length - 1] === origin;
								if (canAck) {
									const nextHop = replyRoute![1];
									if (nextHop && ch.children.has(nextHop)) {
										void this._sendControl(
											nextHop,
											encodeUnicastAck(ch.id.key, ackToken, replyRoute!),
										).catch(() => {});
									}
								}
							}
							return true;
						}
						const nextHop = route[1];
						if (!nextHop || !ch.children.has(nextHop)) return true;
						const stream = this.peers.get(nextHop);
						if (!stream) return true;
						void stream
							.waitForWrite(
								message.bytes(),
								Number(message.header.priority ?? CONTROL_PRIORITY),
								this.closeController.signal,
							)
							.catch(() => {});
						return true;
					}

					// Downstream routing: parent -> child -> ... -> target.
					if (isFromParent) {
						const selfHash = this.publicKeyHash;
						const myIndex = route.indexOf(selfHash);
						if (myIndex < 0) return true;
						if (myIndex === route.length - 1) {
							this.dispatchEvent(
								new CustomEvent("fanout:unicast", {
									detail: {
										topic: ch.id.topic,
										root: ch.id.root,
										route,
										payload,
										from: fromHash,
										origin,
										to: target,
										timestamp: message.header.timestamp,
										message,
									},
								}),
							);
							if (ackToken != null && ch.parent) {
								const canAck =
									replyRoute &&
									replyRoute.length > 0 &&
									replyRoute[0] === ch.id.root &&
									replyRoute[replyRoute.length - 1] === origin;
								if (canAck) {
									void this._sendControl(
										ch.parent,
										encodeUnicastAck(ch.id.key, ackToken, replyRoute!),
									).catch(() => {});
								}
							}
							return true;
						}
						const nextHop = route[myIndex + 1];
						if (!nextHop || !ch.children.has(nextHop)) return true;
						const stream = this.peers.get(nextHop);
						if (!stream) return true;
						void stream
							.waitForWrite(
								message.bytes(),
								Number(message.header.priority ?? CONTROL_PRIORITY),
								this.closeController.signal,
							)
							.catch(() => {});
						return true;
					}

					// Upstream forwarding (child -> ... -> root). Route token is only used by the root.
					if (!ch.parent) return true;
					const up = this.peers.get(ch.parent);
					if (!up) return true;
					void up
						.waitForWrite(
							message.bytes(),
							Number(message.header.priority ?? CONTROL_PRIORITY),
							this.closeController.signal,
						)
						.catch(() => {});
					return true;
				}

					if (kind === MSG_KICK) {
						ch.metrics.reparentKicked += 1;
					ch.parent = undefined;
					ch.level = Number.POSITIVE_INFINITY;
					ch.routeFromRoot = undefined;
					ch.routeByPeer.clear();
					ch.lastParentDataAt = 0;
					ch.receivedAnyParentData = false;
						ch.pendingJoin.clear();
						ch.pendingRouteQuery.clear();
						this.abortPendingUnicastAcks(ch, new AbortError("fanout channel kicked"));
						for (const pending of ch.pendingRouteProxy.values()) {
							clearTimeout(pending.timer);
						}
					ch.pendingRouteProxy.clear();
					void this.kickChildren(ch).catch(() => {});
				this.dispatchEvent(
					new CustomEvent("fanout:kicked", {
						detail: { topic: ch.id.topic, root: ch.id.root, from: fromHash },
					}),
				);
				return true;
			}

					if (kind === MSG_END) {
							if (data.length < 1 + 32 + 4) return false;
							const lastSeqExclusive = readU32BE(data, 33);
							ch.endSeqExclusive = Math.max(ch.endSeqExclusive, lastSeqExclusive);
							this.touchPeerHint(ch, fromHash);
							this.noteEnd(ch, fromHash, lastSeqExclusive);
							if (ch.children.size > 0) {
							void this._sendControlMany(
								[...ch.children.keys()],
						encodeEnd(ch.id.key, lastSeqExclusive),
					);
				}
				void this.tickRepair(ch).catch(() => {});
				return true;
			}

				if (kind === MSG_REPAIR_REQ) {
					if (data.length < 1 + 32 + 4 + 1) return false;
					if (!ch.children.has(fromHash)) return true;
					const count = data[37]!;
				const max = Math.min(count, Math.floor((data.length - 38) / 4));
				for (let i = 0; i < max; i++) {
					const seq = readU32BE(data, 38 + i * 4);
					const cached = this.getCached(ch, seq);
					if (!cached) {
						ch.metrics.cacheMissesServed += 1;
						continue;
					}
					ch.metrics.cacheHitsServed += 1;
					void this._sendData(ch, [fromHash], seq, cached);
				}
					return true;
				}

					if (kind === MSG_FETCH_REQ) {
						if (data.length < 1 + 32 + 4 + 1) return false;
						this.touchPeerHint(ch, fromHash);
						const count = data[37]!;
						const max = Math.min(count, Math.floor((data.length - 38) / 4));
						for (let i = 0; i < max; i++) {
							const seq = readU32BE(data, 38 + i * 4);
						const cached = this.getCached(ch, seq);
						if (!cached) {
							ch.metrics.cacheMissesServed += 1;
							continue;
						}
						ch.metrics.cacheHitsServed += 1;
						void this._sendData(ch, [fromHash], seq, cached);
					}
					return true;
				}

				return true;
			}
	
			// Data-plane (topic identified via id suffix)
				if (kind === MSG_DATA) {
				const id = message.id as Uint8Array;
					if (!(id instanceof Uint8Array) || !isDataId(id)) return false;
				const suffixKey = this.getSuffixKeyFromId(id);
				const ch = this.channelsBySuffixKey.get(suffixKey);
				if (!ch) return false;

						const seq = readU32BE(id, 4);
						if (ch.parent && fromHash === ch.parent) {
							ch.lastParentDataAt = Date.now();
							ch.receivedAnyParentData = true;
						}
						ch.maxSeqSeen = Math.max(ch.maxSeqSeen, seq);
						this.noteReceivedSeq(ch, fromHash, seq);

				const payload = (data as Uint8Array).subarray(1);
				ch.metrics.dataReceives += 1;
				ch.metrics.dataPayloadBytesReceived += payload.byteLength;
				this.markCached(ch, seq, payload);
				this.dispatchEvent(
					new CustomEvent("fanout:data", {
						detail: {
							topic: ch.id.topic,
							root: ch.id.root,
							seq,
							payload,
							from: fromHash,
							origin:
								message.header.signatures?.publicKeys?.[0]?.hashcode?.() ??
								fromHash,
							timestamp: message.header.timestamp,
							message,
						},
					}),
			);

			if (ch.children.size > 0) {
				void this._forwardDataMessage(ch, [...ch.children.keys()], payload, message);
			}
			void this.tickRepair(ch).catch(() => {});
			return true;
		}

		return false;
	}
}
