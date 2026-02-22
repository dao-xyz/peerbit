/**
 * Capacity-aware tree pubsub simulator (Plumtree-inspired).
 *
 * Goals:
 * - Stress the real @peerbit/stream implementation (queues/lanes/backpressure)
 *   without requiring global membership knowledge (`to=[1M subscribers]`).
 * - Model "economic relays" via bids: children offer `bidPerByte` and relays
 *   can prefer higher bids (and optionally kick lower bidders when full).
 * - Respect per-relay upload limits by bounding accepted children based on
 *   `uploadLimitBps` and the configured publish rate/size.
 *
 * NOTE: This is a benchmark/prototype, not a production protocol.
 */

import type { PeerId } from "@libp2p/interface";
import { PreHash, SignatureWithKey } from "@peerbit/crypto";
import { DirectStream, type DirectStreamComponents, type PeerStreams } from "@peerbit/stream";
import { AnyWhere, DataMessage } from "@peerbit/stream-interface";
import { delay } from "@peerbit/time";
import {
	InMemoryConnectionManager,
	InMemoryNetwork,
} from "./sim/inmemory-libp2p.js";
import {
	BENCH_ID_PREFIX,
	buildRandomGraph,
	int,
	isBenchId,
	mulberry32,
	quantile,
	readU32BE,
	runWithConcurrency,
	waitForProtocolStreams,
	writeU32BE,
} from "./sim/bench-utils.js";

type SimParams = {
	nodes: number;
	degree: number;
	rootIndex: number;
	subscribers: number;
	msgRate: number;
	durationMs: number;
	msgSize: number;
	seed: number;
	topic: string;
	timeoutMs: number;
	dialConcurrency: number;
	dialDelayMs: number;
	streamRxDelayMs: number;
	streamHighWaterMarkBytes: number;
	dropDataFrameRate: number;
	relayFraction: number;
	relayUploadLimitBps: number;
	relayMaxChildren: number;
	leafUploadLimitBps: number;
	leafMaxChildren: number;
	bidMin: number;
	bidMax: number;
	allowKick: boolean;
	dropProbability: number;
	repair: boolean;
	repairWindowMessages: number;
	repairIntervalMs: number;
	repairMaxPerReq: number;
	repairSettleMs: number;
	maxLatencySamples: number;
	strict: boolean;
	assertMinConnectedPct: number;
	assertMinDeliveryPct: number;
	assertMinConnectedDeliveryPct: number;
	assertMaxOverheadFactor: number;
	assertMaxUploadFracPct: number;
};

const CONTROL_PRIORITY = 10;
const DATA_PRIORITY = 1;

const shuffleInPlace = <T>(rng: () => number, arr: T[]) => {
	for (let i = arr.length - 1; i > 0; i--) {
		const j = int(rng, i + 1);
		const tmp = arr[i]!;
		arr[i] = arr[j]!;
		arr[j] = tmp;
	}
	return arr;
};

const bfsLevels = (graph: number[][], rootIndex: number): Int32Array => {
	const levels = new Int32Array(graph.length);
	levels.fill(-1);
	const q: number[] = [];
	levels[rootIndex] = 0;
	q.push(rootIndex);
	for (let qi = 0; qi < q.length; qi++) {
		const v = q[qi]!;
		const next = levels[v]! + 1;
		for (const w of graph[v]!) {
			if (levels[w] !== -1) continue;
			levels[w] = next;
			q.push(w);
		}
	}
	return levels;
};

const parseArgs = (argv: string[]): SimParams => {
	const get = (key: string) => {
		const idx = argv.indexOf(key);
		if (idx === -1) return undefined;
		return argv[idx + 1];
	};

	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(
			[
				"pubsub-tree-sim.ts",
				"",
				"Args:",
				"  --nodes N                     total nodes (default: 2000)",
				"  --degree K                    target underlay degree (default: 6)",
				"  --rootIndex I                 root/publisher node index (default: 0)",
				"  --subscribers N               number of subscribers (default: nodes-1)",
				"  --msgRate R                   messages/sec (default: 30)",
				"  --durationMs MS               duration (default: 10000)",
				"  --msgSize BYTES               payload bytes (default: 1024)",
				"  --seed S                      RNG seed (default: 1)",
				"  --topic NAME                  (default: concert)",
				"  --timeoutMs MS                global timeout (default: 300000)",
				"  --dialConcurrency N           dial concurrency (default: 256)",
				"  --dialDelayMs MS              artificial dial delay (default: 0)",
				"  --streamRxDelayMs MS          per-chunk inbound delay in shim (default: 0)",
				"  --streamHighWaterMarkBytes B  backpressure threshold (default: 262144)",
				"  --dropDataFrameRate P         drop rate for stream data frames (default: 0)",
				"  --relayFraction F             fraction of nodes willing to relay (default: 0.2)",
				"  --relayUploadLimitBps BPS      per-relay upload budget (default: 5000000)",
				"  --relayMaxChildren N           hard child cap (default: 8)",
				"  --leafUploadLimitBps BPS       (default: 0)",
				"  --leafMaxChildren N            (default: 0)",
				"  --bidMin N                     min bid per byte (default: 1)",
				"  --bidMax N                     max bid per byte (default: 10)",
				"  --allowKick 0|1                replace low bidders when full (default: 0)",
				"  --dropProbability P            drop forwarded data to child (default: 0)",
				"  --repair 0|1                   enable pull repair (default: 0)",
				"  --repairWindowMessages N       parent cache window (default: 1024)",
				"  --repairIntervalMs MS          repair request interval (default: 200)",
				"  --repairMaxPerReq N            missing seqs per request (default: 64)",
				"  --repairSettleMs MS            repair settle time after END (default: 2000)",
				"  --maxLatencySamples N          reservoir sample size (default: 1000000)",
				"  --strict 0|1                   fail if any subscriber disconnected (default: 0)",
				"  --assertMinConnectedPct PCT    fail if connected < PCT (default: 0)",
				"  --assertMinDeliveryPct PCT     fail if delivered < PCT (default: 0)",
				"  --assertMinConnectedDeliveryPct PCT  fail if delivered among connected < PCT (default: 0)",
				"  --assertMaxOverheadFactor F    fail if payload overhead > F (default: Infinity)",
				"  --assertMaxUploadFracPct PCT   fail if max upload frac > PCT (default: Infinity)",
				"",
				"Example:",
				"  node --loader ts-node/esm ./packages/transport/pubsub/benchmark/pubsub-tree-sim.ts --nodes 2000 --degree 6 --msgRate 30 --durationMs 10000 --msgSize 1024",
			].join("\n"),
		);
		process.exit(0);
	}

	const nodes = Number(get("--nodes") ?? 2000);
	return {
		nodes,
		degree: Number(get("--degree") ?? 6),
		rootIndex: Number(get("--rootIndex") ?? 0),
		subscribers: Number(get("--subscribers") ?? nodes - 1),
		msgRate: Number(get("--msgRate") ?? 30),
		durationMs: Number(get("--durationMs") ?? 10_000),
		msgSize: Number(get("--msgSize") ?? 1024),
		seed: Number(get("--seed") ?? 1),
		topic: String(get("--topic") ?? "concert"),
		timeoutMs: Number(get("--timeoutMs") ?? 300_000),
		dialConcurrency: Number(get("--dialConcurrency") ?? 256),
		dialDelayMs: Number(get("--dialDelayMs") ?? 0),
		streamRxDelayMs: Number(get("--streamRxDelayMs") ?? 0),
		streamHighWaterMarkBytes: Number(get("--streamHighWaterMarkBytes") ?? 256 * 1024),
		dropDataFrameRate: Number(get("--dropDataFrameRate") ?? 0),
		relayFraction: Number(get("--relayFraction") ?? 0.2),
		relayUploadLimitBps: Number(get("--relayUploadLimitBps") ?? 5_000_000),
		relayMaxChildren: Number(get("--relayMaxChildren") ?? 8),
		leafUploadLimitBps: Number(get("--leafUploadLimitBps") ?? 0),
		leafMaxChildren: Number(get("--leafMaxChildren") ?? 0),
		bidMin: Number(get("--bidMin") ?? 1),
		bidMax: Number(get("--bidMax") ?? 10),
		allowKick: String(get("--allowKick") ?? "0") === "1",
		dropProbability: Number(get("--dropProbability") ?? 0),
		repair: String(get("--repair") ?? "0") === "1",
		repairWindowMessages: Number(get("--repairWindowMessages") ?? 1024),
		repairIntervalMs: Number(get("--repairIntervalMs") ?? 200),
		repairMaxPerReq: Number(get("--repairMaxPerReq") ?? 64),
		repairSettleMs: Number(get("--repairSettleMs") ?? 2000),
		maxLatencySamples: Number(get("--maxLatencySamples") ?? 1_000_000),
		strict: String(get("--strict") ?? "0") === "1",
		assertMinConnectedPct: Number(get("--assertMinConnectedPct") ?? 0),
		assertMinDeliveryPct: Number(get("--assertMinDeliveryPct") ?? 0),
		assertMinConnectedDeliveryPct: Number(get("--assertMinConnectedDeliveryPct") ?? 0),
		assertMaxOverheadFactor: Number(get("--assertMaxOverheadFactor") ?? Number.POSITIVE_INFINITY),
		assertMaxUploadFracPct: Number(get("--assertMaxUploadFracPct") ?? Number.POSITIVE_INFINITY),
	};
};

const MSG_JOIN_REQ = 1;
const MSG_JOIN_ACCEPT = 2;
const MSG_JOIN_REJECT = 3;
const MSG_KICK = 4;
const MSG_DATA = 10;
const MSG_END = 11;
const MSG_REPAIR_REQ = 20;

const encodeJoinReq = (reqId: number, bidPerByte: number) => {
	const buf = new Uint8Array(1 + 4 + 4);
	buf[0] = MSG_JOIN_REQ;
	writeU32BE(buf, 1, reqId >>> 0);
	writeU32BE(buf, 5, bidPerByte >>> 0);
	return buf;
};

const encodeJoinAccept = (reqId: number) => {
	const buf = new Uint8Array(1 + 4);
	buf[0] = MSG_JOIN_ACCEPT;
	writeU32BE(buf, 1, reqId >>> 0);
	return buf;
};

const encodeJoinReject = (reqId: number) => {
	const buf = new Uint8Array(1 + 4);
	buf[0] = MSG_JOIN_REJECT;
	writeU32BE(buf, 1, reqId >>> 0);
	return buf;
};

const encodeKick = () => new Uint8Array([MSG_KICK]);

const encodeData = (payload: Uint8Array) => {
	const buf = new Uint8Array(1 + payload.length);
	buf[0] = MSG_DATA;
	buf.set(payload, 1);
	return buf;
};

const encodeEnd = (lastSeqExclusive: number) => {
	const buf = new Uint8Array(1 + 4);
	buf[0] = MSG_END;
	writeU32BE(buf, 1, lastSeqExclusive >>> 0);
	return buf;
};

const encodeRepairReq = (reqId: number, missingSeqs: number[]) => {
	const count = Math.max(0, Math.min(255, missingSeqs.length));
	const buf = new Uint8Array(1 + 4 + 1 + count * 4);
	buf[0] = MSG_REPAIR_REQ;
	writeU32BE(buf, 1, reqId >>> 0);
	buf[5] = count & 0xff;
	for (let i = 0; i < count; i++) {
		writeU32BE(buf, 6 + i * 4, missingSeqs[i]! >>> 0);
	}
	return buf;
};

type ChildInfo = { bidPerByte: number };

class TreeNode extends DirectStream {
	public parent?: string;
	public readonly children = new Map<string, ChildInfo>();
	public readonly bidPerByte: number;
	public readonly uploadLimitBps: number;
	public readonly maxChildren: number;
	public readonly effectiveMaxChildren: number;

	public joinAttempts = 0;
	public joinAccepts = 0;
	public joinRejects = 0;
	public kicksReceived = 0;
	public kicksSent = 0;

	public forwardedPayloadBytes = 0;
	public forwardedMessages = 0;
	public droppedForwards = 0;
	public earned = 0n;

	public repairRequestsSent = 0;
	public repairRequestsReceived = 0;
	public repairMessagesSent = 0;

	public repairMissingCount() {
		return this._missingSeqs.size;
	}

	private pendingJoin = new Map<number, { resolve(ok: boolean): void }>();
	private readonly dataIdPrefix: Uint8Array;
	private readonly benchPayload: Uint8Array;
	private readonly repairEnabled: boolean;
	private readonly repairWindowMessages: number;
	private readonly repairIntervalMs: number;
	private readonly repairMaxPerReq: number;
	private readonly cacheSeqs?: Int32Array;
	private nextExpectedSeq = 0;
	private readonly missingSeqsView: number[] = [];
	private readonly _missingSeqs = new Set<number>();
	private lastRepairSentAt = 0;

	constructor(
		components: DirectStreamComponents,
		opts: {
			msgRate: number;
			msgSize: number;
			uploadLimitBps: number;
			maxChildren: number;
			bidPerByte: number;
			allowKick: boolean;
			dropProbability: number;
			rng: () => number;
			onBenchData?: (seq: number) => void;
			repair: boolean;
			repairWindowMessages: number;
			repairIntervalMs: number;
			repairMaxPerReq: number;
		},
	) {
		super(components, ["/peerbit/treepub/0.0.1"], {
			canRelayMessage: false,
			connectionManager: {
				dialer: false,
				pruner: false,
				maxConnections: Number.MAX_SAFE_INTEGER,
				minConnections: 0,
			},
		});

		this.bidPerByte = opts.bidPerByte >>> 0;
		this.uploadLimitBps = Math.max(0, Math.floor(opts.uploadLimitBps));
		this.maxChildren = Math.max(0, Math.floor(opts.maxChildren));

		const perChildBps = Math.max(1, Math.floor(opts.msgRate * opts.msgSize));
		const byBps = this.uploadLimitBps > 0 ? Math.floor(this.uploadLimitBps / perChildBps) : 0;
		this.effectiveMaxChildren = Math.max(0, Math.min(this.maxChildren, byBps));

		// Fast/mock signing: keep signer identity semantics but skip crypto work.
		this.sign = async () =>
			new SignatureWithKey({
				signature: new Uint8Array([0]),
				publicKey: this.publicKey,
				prehash: PreHash.NONE,
			});

		this.dataIdPrefix = new Uint8Array(32);
		this.dataIdPrefix.set(BENCH_ID_PREFIX, 0);
		this.benchPayload = new Uint8Array(Math.max(0, Math.floor(opts.msgSize)));

		this.repairEnabled = opts.repair;
		this.repairWindowMessages = Math.max(0, Math.floor(opts.repairWindowMessages));
		this.repairIntervalMs = Math.max(0, Math.floor(opts.repairIntervalMs));
		this.repairMaxPerReq = Math.max(0, Math.floor(opts.repairMaxPerReq));
		this.cacheSeqs =
			this.repairEnabled && this.repairWindowMessages > 0
				? new Int32Array(this.repairWindowMessages).fill(-1)
				: undefined;

		this._allowKick = opts.allowKick;
		this._dropProbability = Math.max(0, Math.min(1, opts.dropProbability));
		this._rng = opts.rng;
		this._onBenchData = opts.onBenchData;
	}

	private readonly _allowKick: boolean;
	private readonly _dropProbability: number;
	private readonly _rng: () => number;
	private readonly _onBenchData?: (seq: number) => void;

	public async verifyAndProcess(message: any) {
		// Skip expensive crypto verify, but keep session handling behavior.
		const from = message.header.signatures!.publicKeys[0];
		if (!this.peers.has(from.hashcode())) {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}

	private makeDataId(seq: number): Uint8Array {
		const id = this.dataIdPrefix.slice();
		writeU32BE(id, 4, seq >>> 0);
		return id;
	}

	private async sendControl(to: string, bytes: Uint8Array) {
		const stream = this.peers.get(to);
		if (!stream) return;
		const message = await this.createMessage(bytes, {
			mode: new AnyWhere(),
			priority: CONTROL_PRIORITY,
		} as any);
		await this.publishMessage(this.publicKey, message, [stream]);
	}

	private async sendControlMany(to: string[], bytes: Uint8Array) {
		if (to.length === 0) return;
		const streams = to
			.map((t) => this.peers.get(t))
			.filter((s): s is PeerStreams => Boolean(s));
		if (streams.length === 0) return;
		const message = await this.createMessage(bytes, {
			mode: new AnyWhere(),
			priority: CONTROL_PRIORITY,
		} as any);
		await this.publishMessage(this.publicKey, message, streams);
	}

	private hasCachedSeq(seq: number): boolean {
		if (!this.cacheSeqs || this.cacheSeqs.length === 0) return false;
		return this.cacheSeqs[seq % this.cacheSeqs.length] === (seq | 0);
	}

	private markCachedSeq(seq: number) {
		if (!this.cacheSeqs || this.cacheSeqs.length === 0) return;
		this.cacheSeqs[seq % this.cacheSeqs.length] = seq | 0;
	}

	private noteReceivedSeq(fromHash: string, seq: number) {
		if (!this.repairEnabled) return;
		if (!this.parent || fromHash !== this.parent) return;

		if (seq >= this.nextExpectedSeq) {
			for (let s = this.nextExpectedSeq; s < seq; s++) {
				this._missingSeqs.add(s);
			}
			this.nextExpectedSeq = seq + 1;
		}
		this._missingSeqs.delete(seq);
	}

	private noteEnd(fromHash: string, lastSeqExclusive: number) {
		if (!this.repairEnabled) return;
		if (!this.parent || fromHash !== this.parent) return;

		if (lastSeqExclusive > this.nextExpectedSeq) {
			for (let s = this.nextExpectedSeq; s < lastSeqExclusive; s++) {
				this._missingSeqs.add(s);
			}
			this.nextExpectedSeq = lastSeqExclusive;
		}
	}

	public async tickRepair(now = Date.now()): Promise<boolean> {
		if (!this.repairEnabled) return false;
		if (!this.parent) return false;
		if (this._missingSeqs.size === 0) return false;
		if (this.repairIntervalMs > 0 && now - this.lastRepairSentAt < this.repairIntervalMs) {
			return false;
		}

		this.lastRepairSentAt = now;
		this.missingSeqsView.length = 0;
		for (const s of this._missingSeqs) this.missingSeqsView.push(s);
		this.missingSeqsView.sort((a, b) => a - b);

		const count = Math.min(this.repairMaxPerReq, this.missingSeqsView.length, 255);
		if (count <= 0) return false;
		const reqId = int(this._rng, 0xffffffff) >>> 0;
		this.repairRequestsSent += 1;
		await this.sendControl(this.parent, encodeRepairReq(reqId, this.missingSeqsView.slice(0, count)));
		return true;
	}

	public async sendEnd(to: string[], lastSeqExclusive: number): Promise<void> {
		await this.sendControlMany(to, encodeEnd(lastSeqExclusive));
	}

	public async sendData(to: string[], seq: number, payload: Uint8Array) {
		if (to.length === 0) return;
		this.markCachedSeq(seq);

		const targets: string[] = [];
		if (this._dropProbability > 0) {
			for (const t of to) {
				if (this._rng() < this._dropProbability) {
					this.droppedForwards += 1;
					continue;
				}
				targets.push(t);
			}
		} else {
			targets.push(...to);
		}

		if (targets.length === 0) return;

		const bytes = encodeData(payload);
		const forwardedBytes = payload.byteLength * targets.length;
		this.forwardedPayloadBytes += forwardedBytes;
		this.forwardedMessages += targets.length;

		for (const t of targets) {
			const bid = this.children.get(t)?.bidPerByte ?? 0;
			this.earned += BigInt(bid) * BigInt(payload.byteLength);
		}

		const streams = targets
			.map((t) => this.peers.get(t))
			.filter((s): s is PeerStreams => Boolean(s));
		if (streams.length === 0) return;
		const message = await this.createMessage(bytes, {
			mode: new AnyWhere(),
			priority: DATA_PRIORITY,
			id: this.makeDataId(seq),
		} as any);
		await this.publishMessage(this.publicKey, message, streams);
	}

	public async tryJoinOnce(parent: string, reqId: number): Promise<boolean> {
		if (this.parent) return true;
		if (!this.peers.get(parent)) return false;
		this.joinAttempts += 1;
		const p = new Promise<boolean>((resolve) => {
			this.pendingJoin.set(reqId, { resolve });
		});
		await this.sendControl(parent, encodeJoinReq(reqId, this.bidPerByte));
		return p;
	}

	public async onDataMessage(
		from: any,
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number,
	) {
		if (this.shouldIgnore(message, seenBefore)) {
			return false;
		}

		if (!message.data || message.data.length === 0) {
			return false;
		}

		const kind = (message.data as Uint8Array)[0]!;
		const fromHash = from.hashcode();

		if (kind === MSG_JOIN_REQ) {
			if ((message.data as Uint8Array).length < 1 + 4 + 4) return false;
			const reqId = readU32BE(message.data as Uint8Array, 1);
			const bidPerByte = readU32BE(message.data as Uint8Array, 5);

			// Only accept children if we have relay capacity.
			if (this.effectiveMaxChildren <= 0) {
				this.joinRejects += 1;
				void this.sendControl(fromHash, encodeJoinReject(reqId));
				return true;
			}

			if (!this.children.has(fromHash) && this.children.size >= this.effectiveMaxChildren) {
				if (!this._allowKick) {
					this.joinRejects += 1;
					void this.sendControl(fromHash, encodeJoinReject(reqId));
					return true;
				}

				let worstChild: string | undefined;
				let worstBid = Number.POSITIVE_INFINITY;
				for (const [childHash, info] of this.children) {
					if (info.bidPerByte < worstBid) {
						worstBid = info.bidPerByte;
						worstChild = childHash;
					}
				}

				if (worstChild == null || bidPerByte <= worstBid) {
					this.joinRejects += 1;
					void this.sendControl(fromHash, encodeJoinReject(reqId));
					return true;
				}

				// Replace low bidder.
				this.children.delete(worstChild);
				this.kicksSent += 1;
				void this.sendControl(worstChild, encodeKick());
			}

			this.children.set(fromHash, { bidPerByte });
			this.joinAccepts += 1;
			void this.sendControl(fromHash, encodeJoinAccept(reqId));
			return true;
		}

		if (kind === MSG_JOIN_ACCEPT || kind === MSG_JOIN_REJECT) {
			if ((message.data as Uint8Array).length < 1 + 4) return false;
			const reqId = readU32BE(message.data as Uint8Array, 1);
			const pending = this.pendingJoin.get(reqId);
			if (!pending) return true;
			this.pendingJoin.delete(reqId);
			if (kind === MSG_JOIN_ACCEPT) {
				this.parent = fromHash;
				pending.resolve(true);
			} else {
				pending.resolve(false);
			}
			return true;
		}

		if (kind === MSG_KICK) {
			this.kicksReceived += 1;
			this.parent = undefined;
			return true;
		}

		if (kind === MSG_END) {
			if ((message.data as Uint8Array).length < 1 + 4) return false;
			const lastSeqExclusive = readU32BE(message.data as Uint8Array, 1);
			this.noteEnd(fromHash, lastSeqExclusive);

			if (this.children.size > 0) {
				void this.sendControlMany([...this.children.keys()], encodeEnd(lastSeqExclusive));
			}
			void this.tickRepair().catch(() => {});
			return true;
		}

		if (kind === MSG_REPAIR_REQ) {
			if ((message.data as Uint8Array).length < 1 + 4 + 1) return false;
			if (!this.children.has(fromHash)) return true;
			this.repairRequestsReceived += 1;
			const count = (message.data as Uint8Array)[5]!;
			const max = Math.min(count, Math.floor(((message.data as Uint8Array).length - 6) / 4));
			for (let i = 0; i < max; i++) {
				const seq = readU32BE(message.data as Uint8Array, 6 + i * 4);
				if (!this.hasCachedSeq(seq)) continue;
				this.repairMessagesSent += 1;
				void this.sendData([fromHash], seq, this.benchPayload);
			}
			return true;
		}

		if (kind === MSG_DATA) {
			if (!isBenchId(message.id)) return false;
			const seq = readU32BE(message.id, 4);
			this.noteReceivedSeq(fromHash, seq);
			this._onBenchData?.(seq);

			// Forward down the tree (no global membership required).
			if (this.children.size > 0) {
				const targets = [...this.children.keys()];
				void this.sendData(targets, seq, (message.data as Uint8Array).subarray(1));
			}
			void this.tickRepair().catch(() => {});
			return true;
		}

		// Ignore unknown message types.
		return false;
	}
}

const main = async () => {
	const rawParams = parseArgs(process.argv.slice(2));
	const nodes = Math.max(1, Math.floor(rawParams.nodes));
	const degree = Math.max(0, Math.min(Math.floor(rawParams.degree), nodes - 1));

	if (degree !== rawParams.degree) {
		console.warn(`clamped --degree from ${rawParams.degree} to ${degree} (nodes=${nodes})`);
	}
	if (nodes !== rawParams.nodes) {
		console.warn(`clamped --nodes from ${rawParams.nodes} to ${nodes}`);
	}

	const params = { ...rawParams, nodes, degree };
	const timeoutMs = Math.max(0, params.timeoutMs);
	const timeout =
		timeoutMs > 0
			? setTimeout(() => {
					console.error(
						`pubsub-tree-sim timed out after ${timeoutMs}ms (override with --timeoutMs)`,
					);
					process.exit(124);
				}, timeoutMs)
			: undefined;

	const rng = mulberry32(params.seed);
	const peers: {
		peerId: PeerId;
		node: TreeNode;
		level: number;
		candidates: number[];
		isRelay: boolean;
	}[] = [];
	try {
		const graph = buildRandomGraph(params.nodes, params.degree, rng);
		const rootIndex = Math.max(0, Math.min(params.nodes - 1, params.rootIndex));
		const levels = bfsLevels(graph, rootIndex);

		if (params.strict) {
			for (let i = 0; i < levels.length; i++) {
				if (levels[i] === -1) {
					throw new Error(`graph is disconnected, node ${i} not reachable from root`);
				}
			}
		}

		const network = new InMemoryNetwork({
			streamRxDelayMs: params.streamRxDelayMs,
			streamHighWaterMarkBytes: params.streamHighWaterMarkBytes,
			dialDelayMs: params.dialDelayMs,
			dropDataFrameRate: params.dropDataFrameRate,
			dropSeed: params.seed,
		});

		const subscriberCount = Math.max(
			0,
			Math.min(params.nodes - 1, params.subscribers),
		);
		const allIndices = Array.from({ length: params.nodes }, (_, i) => i);
		allIndices.splice(rootIndex, 1);
		shuffleInPlace(rng, allIndices);
		const subscriberIndices = new Set(allIndices.slice(0, subscriberCount));

		const sendTimes: number[] = [];
		let delivered = 0;
		let deliveredSamplesSeen = 0;
		const samples: number[] = [];
		const maxSamples = Math.max(0, params.maxLatencySamples);
		const recordLatency = (ms: number) => {
			deliveredSamplesSeen += 1;
			if (samples.length < maxSamples) {
				samples.push(ms);
				return;
			}
			if (maxSamples === 0) return;
			const j = int(rng, deliveredSamplesSeen);
			if (j < maxSamples) samples[j] = ms;
		};

		const basePort = 50_000;
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

			const isRelay = i === rootIndex ? true : rng() < params.relayFraction;
			const uploadLimitBps = isRelay ? params.relayUploadLimitBps : params.leafUploadLimitBps;
			const maxChildren = isRelay ? params.relayMaxChildren : params.leafMaxChildren;

			const bidPerByte =
				params.bidMax <= params.bidMin
					? params.bidMin
					: params.bidMin + int(rng, params.bidMax - params.bidMin + 1);

			const onBenchData = subscriberIndices.has(i)
				? (seq: number) => {
						const t0 = sendTimes[seq];
						if (!t0) return;
						delivered += 1;
						recordLatency(Date.now() - t0);
					}
				: undefined;

			const node = new TreeNode(components, {
				msgRate: params.msgRate,
				msgSize: params.msgSize,
				uploadLimitBps,
				maxChildren,
				bidPerByte,
				allowKick: params.allowKick,
				dropProbability: params.dropProbability,
				rng,
				onBenchData,
				repair: params.repair,
				repairWindowMessages: params.repairWindowMessages,
				repairIntervalMs: params.repairIntervalMs,
				repairMaxPerReq: params.repairMaxPerReq,
			});

			peers.push({
				peerId: runtime.peerId,
				node,
				level: levels[i] ?? -1,
				candidates: [],
				isRelay,
			});
		}

		await Promise.all(peers.map((p) => p.node.start()));

		// Establish underlay graph via dials (bounded concurrency).
		const dialTasks: Array<() => Promise<void>> = [];
		for (let a = 0; a < graph.length; a++) {
			for (const b of graph[a]!) {
				if (b <= a) continue;
				const addrB = (peers[b]!.node.components.addressManager as any).getAddresses()[0];
				dialTasks.push(async () => {
					await peers[a]!.node.components.connectionManager.openConnection(addrB);
				});
			}
		}
		await runWithConcurrency(dialTasks, params.dialConcurrency);
		await waitForProtocolStreams(peers.map((p) => p.node as any));

		// Candidate parents: neighbors strictly closer to root (prevents cycles).
		for (let i = 0; i < peers.length; i++) {
			if (i === rootIndex) continue;
			const myLevel = peers[i]!.level;
			if (myLevel < 0) continue;
			peers[i]!.candidates = graph[i]!.filter((n) => {
				const lvl = peers[n]!.level;
				return lvl >= 0 && lvl < myLevel;
			});
		}

		// Join phase: level-ordered, high bidders first inside each level.
		const joinStart = Date.now();
		const byLevel = new Map<number, number[]>();
		for (let i = 0; i < peers.length; i++) {
			if (i === rootIndex) continue;
			const lvl = peers[i]!.level;
			if (lvl < 0) continue;
			const arr = byLevel.get(lvl) ?? [];
			arr.push(i);
			byLevel.set(lvl, arr);
		}

		const levelsSorted = [...byLevel.keys()].sort((a, b) => a - b);
		let joinRejects = 0;
		let joinAccepts = 0;
		let kicksSent = 0;
		let kicksReceived = 0;

		for (const lvl of levelsSorted) {
			const indices = byLevel.get(lvl)!;
			indices.sort((a, b) => peers[b]!.node.bidPerByte - peers[a]!.node.bidPerByte);

			await runWithConcurrency(
				indices.map((idx) => async () => {
					const node = peers[idx]!.node;
					if (node.parent) return;
					const candidates = peers[idx]!.candidates.slice();
					shuffleInPlace(rng, candidates);

					for (const candIdx of candidates) {
						const parentHash = peers[candIdx]!.node.publicKeyHash;
						const reqId = int(rng, 0xffffffff) >>> 0;
						const ok = await node.tryJoinOnce(parentHash, reqId);
						if (ok) break;
					}
				}),
				Math.min(256, params.dialConcurrency),
			);
		}

		// Rejoin loop for kicked nodes (best-effort).
		const maxRejoinRounds = 10;
		for (let round = 0; round < maxRejoinRounds; round++) {
			const need = peers.filter((p, idx) => idx !== rootIndex && p.level >= 0 && !p.node.parent);
			if (need.length === 0) break;
			await runWithConcurrency(
				need.map((p) => async () => {
					const idx = peers.indexOf(p);
					if (idx === -1) return;
					const node = p.node;
					const candidates = p.candidates.slice();
					shuffleInPlace(rng, candidates);
					for (const candIdx of candidates) {
						const parentHash = peers[candIdx]!.node.publicKeyHash;
						const reqId = int(rng, 0xffffffff) >>> 0;
						const ok = await node.tryJoinOnce(parentHash, reqId);
						if (ok) break;
					}
				}),
				Math.min(256, params.dialConcurrency),
			);
		}

		for (const p of peers) {
			joinRejects += p.node.joinRejects;
			joinAccepts += p.node.joinAccepts;
			kicksSent += p.node.kicksSent;
			kicksReceived += p.node.kicksReceived;
		}
		const joinDone = Date.now();

		const root = peers[rootIndex]!.node;
		const rootChildren = [...root.children.keys()];

		// Publish loop (tree fanout only, no global membership).
		const payload = new Uint8Array(params.msgSize);
		const intervalMs = params.msgRate > 0 ? 1000 / params.msgRate : 0;
		const messageCount =
			params.msgRate > 0
				? Math.max(0, Math.floor((params.durationMs / 1000) * params.msgRate))
				: 0;

		const publishStart = Date.now();
		for (let seq = 0; seq < messageCount; seq++) {
			sendTimes[seq] = Date.now();
			// Root forwards to its children. Children forward on receipt.
			await root.sendData(rootChildren, seq, payload);

			if (intervalMs > 0) {
				const next = publishStart + (seq + 1) * intervalMs;
				const wait = Math.max(0, next - Date.now());
				if (wait > 0) await delay(wait);
			}
		}
		const publishDone = Date.now();

		// Allow in-flight to settle a bit.
		await delay(200);

		if (params.repair) {
			// Broadcast END so tail-gaps can be repaired.
			await root.sendEnd(rootChildren, messageCount);

			const settleMs = Math.max(0, Math.floor(params.repairSettleMs));
			const deadline = Date.now() + settleMs;
			const interval = Math.max(1, Math.floor(params.repairIntervalMs || 200));

			while (Date.now() < deadline) {
				const now = Date.now();
				const tasks: Array<() => Promise<boolean>> = [];
				for (const p of peers) {
					if (p.node.repairMissingCount() === 0) continue;
					tasks.push(() => p.node.tickRepair(now));
				}
				if (tasks.length === 0) break;
				await runWithConcurrency(tasks, Math.min(256, params.dialConcurrency));
				const remaining = deadline - Date.now();
				if (remaining <= 0) break;
				await delay(Math.min(interval, remaining));
			}

			// Allow repaired messages to drain.
			await delay(200);
		}

		samples.sort((a, b) => a - b);
		const expected = subscriberCount * messageCount;

		let connectedSubscribers = 0;
		for (let i = 0; i < peers.length; i++) {
			if (!subscriberIndices.has(i)) continue;
			if (i === rootIndex) continue;
			if (peers[i]!.node.parent) connectedSubscribers += 1;
		}

		let maxUploadFrac = 0;
		let maxUploadNode = -1;
		let totalForwardedPayloadBytes = 0;
		let totalDroppedForwards = 0;
		let totalEarned = 0n;
		let relays = 0;
		let totalChildren = 0;
		let totalRepairRequestsSent = 0;
		let totalRepairRequestsReceived = 0;
		let totalRepairMessagesSent = 0;
		let totalRepairMissingRemaining = 0;

		for (let i = 0; i < peers.length; i++) {
			const n = peers[i]!.node;
			totalForwardedPayloadBytes += n.forwardedPayloadBytes;
			totalDroppedForwards += n.droppedForwards;
			totalEarned += n.earned;
			if (n.effectiveMaxChildren > 0) {
				relays += 1;
				totalChildren += n.children.size;
			}
			totalRepairRequestsSent += n.repairRequestsSent;
			totalRepairRequestsReceived += n.repairRequestsReceived;
			totalRepairMessagesSent += n.repairMessagesSent;
			totalRepairMissingRemaining += n.repairMissingCount();

			const durSec = Math.max(0.001, params.durationMs / 1000);
			const uploadBps = n.forwardedPayloadBytes / durSec;
			const frac = n.uploadLimitBps > 0 ? uploadBps / n.uploadLimitBps : 0;
			if (frac > maxUploadFrac) {
				maxUploadFrac = frac;
				maxUploadNode = i;
			}
		}

		const connectedExpected = connectedSubscribers * messageCount;
		const idealForwardedPayloadBytes = totalChildren * messageCount * params.msgSize;
		const overheadFactor =
			idealForwardedPayloadBytes > 0
				? totalForwardedPayloadBytes / idealForwardedPayloadBytes
				: 0;

		const mem = process.memoryUsage();
		const lines: string[] = [];
		lines.push("pubsub pubsub-tree-sim results");
		lines.push(`- nodes: ${params.nodes}, underlay degree: ${params.degree}`);
		lines.push(
			`- rootIndex: ${rootIndex}, subscribers: ${subscriberCount}, topic: ${params.topic}`,
		);
		lines.push(
			`- rate: ${params.msgRate}/s, duration: ${params.durationMs}ms, messages: ${messageCount}, msgSize: ${params.msgSize}B`,
		);
		lines.push(
			`- relays: fraction=${params.relayFraction}, eligible=${relays}, relayUploadLimitBps=${params.relayUploadLimitBps}, relayMaxChildren=${params.relayMaxChildren}`,
		);
		lines.push(
			`- economics: bidPerByte=[${params.bidMin},${params.bidMax}], allowKick=${params.allowKick ? "on" : "off"}`,
		);
		lines.push(
			`- repair: ${params.repair ? "on" : "off"} (window=${params.repairWindowMessages}, intervalMs=${params.repairIntervalMs}, maxPerReq=${params.repairMaxPerReq}, settleMs=${params.repairSettleMs})`,
		);
		lines.push(
			`- transport: dialDelay=${params.dialDelayMs}ms, rxDelay=${params.streamRxDelayMs}ms, hwm=${params.streamHighWaterMarkBytes}B, dropDataFrameRate=${params.dropDataFrameRate}`,
		);
		lines.push(
			`- transport drops: frames=${network.metrics.framesDropped} (data=${network.metrics.dataFramesDropped}), bytes=${network.metrics.bytesDropped}`,
		);
		lines.push(
			`- join: time=${joinDone - joinStart}ms, accepts=${joinAccepts}, rejects=${joinRejects}, kicksSent=${kicksSent}, kicksRecv=${kicksReceived}`,
		);
		lines.push(
			`- join: connectedSubscribers=${connectedSubscribers}/${subscriberCount} (${subscriberCount > 0 ? ((connectedSubscribers / subscriberCount) * 100).toFixed(2) : "0.00"}%)`,
		);
		lines.push(
			`- publish: time=${publishDone - publishStart}ms, expected=${expected}, delivered=${delivered} (${expected > 0 ? ((delivered / expected) * 100).toFixed(2) : "0.00"}%), connectedExpected=${connectedExpected}, connectedDelivery=${connectedExpected > 0 ? ((delivered / connectedExpected) * 100).toFixed(2) : "0.00"}%`,
		);
		if (samples.length > 0) {
			lines.push(
				`- latency ms (sample n=${samples.length}, total=${deliveredSamplesSeen}): p50=${quantile(samples, 0.5).toFixed(1)}, p95=${quantile(samples, 0.95).toFixed(1)}, p99=${quantile(samples, 0.99).toFixed(1)}, max=${samples[samples.length - 1]!.toFixed(1)}`,
			);
		} else {
			lines.push(`- latency ms: (no samples)`);
		}
		lines.push(
			`- tree: rootChildren=${root.children.size}, avgChildren/relay=${relays > 0 ? (totalChildren / relays).toFixed(2) : "0.00"}`,
		);
		lines.push(
			`- uploadCaps: maxObservedFrac=${(maxUploadFrac * 100).toFixed(1)}% (node=${maxUploadNode})`,
		);
		lines.push(
			`- forwarded payload bytes: total=${totalForwardedPayloadBytes} (droppedForwards=${totalDroppedForwards}, dropProbability=${params.dropProbability})`,
		);
		lines.push(
			`- overhead: idealPayloadBytes=${idealForwardedPayloadBytes}, factor=${Number.isFinite(overheadFactor) ? overheadFactor.toFixed(3) : "NaN"}`,
		);
		lines.push(
			`- repair stats: reqSent=${totalRepairRequestsSent}, reqRecv=${totalRepairRequestsReceived}, msgsSent=${totalRepairMessagesSent}, missingRemaining=${totalRepairMissingRemaining}`,
		);
		lines.push(`- earned (bid*bytes): total=${totalEarned.toString()}`);
		lines.push(
			`- memory: rss=${Math.round(mem.rss / 1024 / 1024)}MiB heapUsed=${Math.round(mem.heapUsed / 1024 / 1024)}MiB heapTotal=${Math.round(mem.heapTotal / 1024 / 1024)}MiB`,
		);

		console.log(lines.join("\n"));

		if (params.strict && connectedSubscribers !== subscriberCount) {
			throw new Error("Not all subscribers connected (strict mode)");
		}

		const failures: string[] = [];
		const connectedPct = subscriberCount > 0 ? (connectedSubscribers / subscriberCount) * 100 : 100;
		const deliveryPct = expected > 0 ? (delivered / expected) * 100 : 100;
		const connectedDeliveryPct =
			connectedExpected > 0 ? (delivered / connectedExpected) * 100 : 100;
		const maxUploadFracPct = maxUploadFrac * 100;

		if (connectedPct + 1e-9 < params.assertMinConnectedPct) {
			failures.push(
				`connectedPct ${connectedPct.toFixed(2)} < assertMinConnectedPct ${params.assertMinConnectedPct}`,
			);
		}
		if (deliveryPct + 1e-9 < params.assertMinDeliveryPct) {
			failures.push(
				`deliveryPct ${deliveryPct.toFixed(2)} < assertMinDeliveryPct ${params.assertMinDeliveryPct}`,
			);
		}
		if (connectedDeliveryPct + 1e-9 < params.assertMinConnectedDeliveryPct) {
			failures.push(
				`connectedDeliveryPct ${connectedDeliveryPct.toFixed(2)} < assertMinConnectedDeliveryPct ${params.assertMinConnectedDeliveryPct}`,
			);
		}
		if (
			Number.isFinite(params.assertMaxOverheadFactor) &&
			Number.isFinite(overheadFactor) &&
			overheadFactor - 1e-9 > params.assertMaxOverheadFactor
		) {
			failures.push(
				`overheadFactor ${overheadFactor.toFixed(3)} > assertMaxOverheadFactor ${params.assertMaxOverheadFactor}`,
			);
		}
		if (
			Number.isFinite(params.assertMaxUploadFracPct) &&
			maxUploadFracPct - 1e-9 > params.assertMaxUploadFracPct
		) {
			failures.push(
				`maxUploadFracPct ${maxUploadFracPct.toFixed(2)} > assertMaxUploadFracPct ${params.assertMaxUploadFracPct}`,
			);
		}

		if (failures.length > 0) {
			throw new Error(`Assertions failed:\n- ${failures.join("\n- ")}`);
		}
	} finally {
		await Promise.allSettled(peers.map((p) => p.node.stop()));
		if (timeout) clearTimeout(timeout);
	}
};

await main();
