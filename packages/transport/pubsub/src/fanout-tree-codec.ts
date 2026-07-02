import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";

export const JOIN_REJECT_REDIRECT_MAX = 4;
export const JOIN_REJECT_REDIRECT_ADDR_MAX = 8;

export const MSG_JOIN_REQ = 1;
export const MSG_JOIN_ACCEPT = 2;
export const MSG_JOIN_REJECT = 3;
export const MSG_KICK = 4;
export const MSG_DATA = 10;
export const MSG_END = 11;
export const MSG_UNICAST = 12;
export const MSG_ROUTE_QUERY = 13;
export const MSG_ROUTE_REPLY = 14;
export const MSG_PUBLISH_PROXY = 15;
export const MSG_LEAVE = 16;
export const MSG_UNICAST_ACK = 17;
export const MSG_REPAIR_REQ = 20;
export const MSG_FETCH_REQ = 21;
export const MSG_IHAVE = 22;
export const MSG_TRACKER_ANNOUNCE = 30;
export const MSG_TRACKER_QUERY = 31;
export const MSG_TRACKER_REPLY = 32;
export const MSG_TRACKER_FEEDBACK = 33;
export const MSG_PROVIDER_ANNOUNCE = 34;
export const MSG_PROVIDER_QUERY = 35;
export const MSG_PROVIDER_REPLY = 36;
export const MSG_PROVIDER_SUBSCRIBE = 37;
export const MSG_PROVIDER_UNSUBSCRIBE = 38;
export const MSG_PROVIDER_NOTIFY = 39;
export const MSG_PARENT_PROBE_REQ = 40;
export const MSG_PARENT_PROBE_REPLY = 41;

export const JOIN_REJECT_NOT_ATTACHED = 1;
export const JOIN_REJECT_NO_CAPACITY = 2;
export const JOIN_REJECT_LOW_BID = 3;

export const TRACKER_FEEDBACK_JOINED = 1;
export const TRACKER_FEEDBACK_DIAL_FAILED = 2;
export const TRACKER_FEEDBACK_JOIN_TIMEOUT = 3;
export const TRACKER_FEEDBACK_JOIN_REJECT = 4;

export const PARENT_PROBE_FLAG_ROOTED = 1 << 0;
export const PARENT_PROBE_FLAG_ACCEPTING = 1 << 1;
export const PARENT_PROBE_FLAG_REPAIRING = 1 << 2;
export const PARENT_PROBE_FLAG_OVERLOADED = 1 << 3;

export const UNICAST_FLAG_ACK = 1;
export const UNICAST_ACK_DEFAULT_TIMEOUT_MS = 30_000;

const PARENT_PROBE_REQ_FLAG_RESERVE_ROOT = 1 << 0;
export const MAX_ROUTE_HOPS = 32;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export const clampU16 = (v: number) => Math.max(0, Math.min(0xffff, v | 0));

export const writeU32BE = (
	buf: Uint8Array,
	offset: number,
	value: number,
) => {
	buf[offset + 0] = (value >>> 24) & 0xff;
	buf[offset + 1] = (value >>> 16) & 0xff;
	buf[offset + 2] = (value >>> 8) & 0xff;
	buf[offset + 3] = value & 0xff;
};

export const readU32BE = (buf: Uint8Array, offset: number) =>
	((buf[offset + 0] << 24) |
		(buf[offset + 1] << 16) |
		(buf[offset + 2] << 8) |
		buf[offset + 3]) >>>
	0;

const writeU64BE = (buf: Uint8Array, offset: number, value: bigint) => {
	let v = value & 0xffffffffffffffffn;
	for (let i = 7; i >= 0; i--) {
		buf[offset + i] = Number(v & 0xffn);
		v >>= 8n;
	}
};

export const readU64BE = (buf: Uint8Array, offset: number) => {
	let v = 0n;
	for (let i = 0; i < 8; i++) {
		v = (v << 8n) | BigInt(buf[offset + i]!);
	}
	return v;
};

export const writeU16BE = (
	buf: Uint8Array,
	offset: number,
	value: number,
) => {
	buf[offset + 0] = (value >>> 8) & 0xff;
	buf[offset + 1] = value & 0xff;
};

export const readU16BE = (buf: Uint8Array, offset: number) =>
	((buf[offset + 0] << 8) | buf[offset + 1]) >>> 0;

const CONTROL_FRAME_CHANNEL_KEY_BYTES = 32;
const CONTROL_FRAME_HEADER_BYTES = 1 + CONTROL_FRAME_CHANNEL_KEY_BYTES;

class FrameWriter {
	private readonly buf: Uint8Array;
	private offset = 0;

	constructor(kind: number, channelKey: Uint8Array, payloadBytes: number) {
		this.buf = new Uint8Array(CONTROL_FRAME_HEADER_BYTES + payloadBytes);
		this.u8(kind);
		this.bytes(channelKey, CONTROL_FRAME_CHANNEL_KEY_BYTES, "channel key");
	}

	private ensure(bytes: number) {
		if (this.offset + bytes > this.buf.length) {
			throw new Error("control frame write overflow");
		}
	}

	u8(value: number) {
		this.ensure(1);
		this.buf[this.offset++] = value & 0xff;
		return this;
	}

	u16(value: number) {
		this.ensure(2);
		writeU16BE(this.buf, this.offset, value);
		this.offset += 2;
		return this;
	}

	u32(value: number) {
		this.ensure(4);
		writeU32BE(this.buf, this.offset, value);
		this.offset += 4;
		return this;
	}

	bytes(value: Uint8Array, expectedLength = value.length, label = "bytes") {
		if (value.length !== expectedLength) {
			throw new Error(
				`invalid ${label} length: expected ${expectedLength}, got ${value.length}`,
			);
		}
		this.ensure(value.length);
		this.buf.set(value, this.offset);
		this.offset += value.length;
		return this;
	}

	done() {
		if (this.offset !== this.buf.length) {
			throw new Error("control frame write underflow");
		}
		return this.buf;
	}
}

class FrameReader {
	private constructor(
		private readonly buf: Uint8Array,
		private offset: number,
	) {}

	static control(data: Uint8Array, minPayloadBytes: number) {
		if (data.length < CONTROL_FRAME_HEADER_BYTES + minPayloadBytes) {
			return undefined;
		}
		return new FrameReader(data, CONTROL_FRAME_HEADER_BYTES);
	}

	remaining() {
		return this.buf.length - this.offset;
	}

	has(bytes: number) {
		return this.remaining() >= bytes;
	}

	u8() {
		if (!this.has(1)) return undefined;
		return this.buf[this.offset++]!;
	}

	u16() {
		if (!this.has(2)) return undefined;
		const value = readU16BE(this.buf, this.offset);
		this.offset += 2;
		return value;
	}

	u32() {
		if (!this.has(4)) return undefined;
		const value = readU32BE(this.buf, this.offset);
		this.offset += 4;
		return value;
	}
}

export type JoinRejectRedirect = {
	hash: string;
	addrs: Uint8Array[];
};

export type TrackerEntry = {
	hash: string;
	level: number;
	freeSlots: number;
	bidPerByte: number;
	addrs: Uint8Array[];
	expiresAt: number;
};

export type ParentProbeReply = {
	hash: string;
	rooted: boolean;
	accepting: boolean;
	repairing: boolean;
	overloaded: boolean;
	reservationToken: number;
	level: number;
	maxChildren: number;
	freeSlots: number;
	children: number;
	haveToExclusive: number;
	missingSeqs: number;
	dataWriteDrops: number;
	droppedForwards: number;
};

export type ProviderEntry = {
	hash: string;
	addrs: Uint8Array[];
	expiresAt: number;
};

export type ProviderCandidate = {
	hash: string;
	addrs: Multiaddr[];
};

const JOIN_REQ_PAYLOAD_BYTES = 4 + 4;
const JOIN_REQ_RESERVATION_BYTES = 4;

export const encodeJoinReq = (
	channelKey: Uint8Array,
	reqId: number,
	bidPerByte: number,
	parentUpgradeReservationToken = 0,
) => {
	const hasReservation = parentUpgradeReservationToken > 0;
	const frame = new FrameWriter(
		MSG_JOIN_REQ,
		channelKey,
		JOIN_REQ_PAYLOAD_BYTES + (hasReservation ? JOIN_REQ_RESERVATION_BYTES : 0),
	)
		.u32(reqId >>> 0)
		.u32(bidPerByte >>> 0);
	if (hasReservation) {
		frame.u32(parentUpgradeReservationToken >>> 0);
	}
	return frame.done();
};

export const decodeJoinReq = (data: Uint8Array) => {
	const frame = FrameReader.control(data, JOIN_REQ_PAYLOAD_BYTES);
	if (!frame) return undefined;
	const reqId = frame.u32();
	const bidPerByte = frame.u32();
	if (reqId == null || bidPerByte == null) return undefined;
	return {
		reqId,
		bidPerByte,
		parentUpgradeReservationToken: frame.has(JOIN_REQ_RESERVATION_BYTES)
			? (frame.u32() ?? 0)
			: 0,
	};
};

export const encodeJoinAccept = (
	channelKey: Uint8Array,
	reqId: number,
	level: number,
	parentRouteFromRoot?: string[],
	haveRange?: { haveFrom: number; haveToExclusive: number },
) => {
	const routeBytes: Uint8Array[] = [];
	let bytes = 1 + 32 + 4 + 2 + 1;
	if (haveRange) bytes += 8;
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
	if (haveRange) {
		writeU32BE(buf, offset, haveRange.haveFrom >>> 0);
		offset += 4;
		writeU32BE(buf, offset, haveRange.haveToExclusive >>> 0);
		offset += 4;
	}
	return buf;
};

export const encodeJoinReject = (
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

export const encodeKick = (channelKey: Uint8Array) => {
	const buf = new Uint8Array(1 + 32);
	buf[0] = MSG_KICK;
	buf.set(channelKey, 1);
	return buf;
};

export const encodeEnd = (channelKey: Uint8Array, lastSeqExclusive: number) => {
	const buf = new Uint8Array(1 + 32 + 4);
	buf[0] = MSG_END;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, lastSeqExclusive >>> 0);
	return buf;
};

export const encodeRepairReq = (
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

export const encodeFetchReq = (
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

export const encodeIHave = (
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

export const encodeData = (payload: Uint8Array) => {
	const buf = new Uint8Array(1 + payload.length);
	buf[0] = MSG_DATA;
	buf.set(payload, 1);
	return buf;
};

export const encodePublishProxy = (
	channelKey: Uint8Array,
	payload: Uint8Array,
) => {
	const buf = new Uint8Array(1 + 32 + payload.length);
	buf[0] = MSG_PUBLISH_PROXY;
	buf.set(channelKey, 1);
	buf.set(payload, 33);
	return buf;
};

export const encodeLeave = (channelKey: Uint8Array) => {
	const buf = new Uint8Array(1 + 32);
	buf[0] = MSG_LEAVE;
	buf.set(channelKey, 1);
	return buf;
};

export const encodeUnicast = (
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

export const encodeUnicastAck = (
	channelKey: Uint8Array,
	ackToken: bigint,
	route: string[],
) => {
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

export const encodeRouteQuery = (
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

export const encodeRouteReply = (
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

export const decodeRoute = (
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

export const encodeTrackerAnnounce = (
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

export const encodeTrackerQuery = (
	channelKey: Uint8Array,
	reqId: number,
	want: number,
) => {
	const buf = new Uint8Array(1 + 32 + 4 + 2);
	buf[0] = MSG_TRACKER_QUERY;
	buf.set(channelKey, 1);
	writeU32BE(buf, 33, reqId >>> 0);
	writeU16BE(buf, 37, clampU16(want));
	return buf;
};

export const encodeTrackerReply = (
	channelKey: Uint8Array,
	reqId: number,
	entries: TrackerEntry[],
) => {
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

export const encodeTrackerFeedback = (
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

const PARENT_PROBE_REQ_PAYLOAD_BYTES = 4;
const PARENT_PROBE_REQ_EXTENSION_BYTES = 2 + 1;
const PARENT_PROBE_REPLY_PAYLOAD_BYTES =
	4 + 1 + 2 + 2 + 2 + 2 + 4 + 2 + 4 + 4;
const PARENT_PROBE_REPLY_RESERVATION_BYTES = 4;

export const encodeParentProbeReq = (
	channelKey: Uint8Array,
	reqId: number,
	minFreeSlots = 0,
	reserveRootCapacity = true,
) => {
	const encodedMinFreeSlots = Math.max(0, Math.floor(minFreeSlots));
	const hasExtension = encodedMinFreeSlots > 0 || !reserveRootCapacity;
	const frame = new FrameWriter(
		MSG_PARENT_PROBE_REQ,
		channelKey,
		PARENT_PROBE_REQ_PAYLOAD_BYTES +
			(hasExtension ? PARENT_PROBE_REQ_EXTENSION_BYTES : 0),
	).u32(reqId >>> 0);
	if (hasExtension) {
		frame
			.u16(clampU16(encodedMinFreeSlots))
			.u8(reserveRootCapacity ? PARENT_PROBE_REQ_FLAG_RESERVE_ROOT : 0);
	}
	return frame.done();
};

export const encodeParentProbeReply = (
	channelKey: Uint8Array,
	reqId: number,
	options: {
		flags: number;
		level: number;
		maxChildren: number;
		freeSlots: number;
		children: number;
		haveToExclusive: number;
		missingSeqs: number;
		dataWriteDrops: number;
		droppedForwards: number;
		reservationToken?: number;
	},
) => {
	return new FrameWriter(
		MSG_PARENT_PROBE_REPLY,
		channelKey,
		PARENT_PROBE_REPLY_PAYLOAD_BYTES + PARENT_PROBE_REPLY_RESERVATION_BYTES,
	)
		.u32(reqId >>> 0)
		.u8(options.flags & 0xff)
		.u16(clampU16(options.level))
		.u16(clampU16(options.maxChildren))
		.u16(clampU16(options.freeSlots))
		.u16(clampU16(options.children))
		.u32(Math.max(0, Math.floor(options.haveToExclusive)) >>> 0)
		.u16(clampU16(options.missingSeqs))
		.u32(Math.max(0, Math.floor(options.dataWriteDrops)) >>> 0)
		.u32(Math.max(0, Math.floor(options.droppedForwards)) >>> 0)
		.u32(Math.max(0, Math.floor(options.reservationToken ?? 0)) >>> 0)
		.done();
};

export const decodeParentProbeReq = (data: Uint8Array) => {
	const frame = FrameReader.control(data, PARENT_PROBE_REQ_PAYLOAD_BYTES);
	if (!frame) return undefined;
	const reqId = frame.u32();
	if (reqId == null) return undefined;
	let minFreeSlots = 0;
	let probeFlags = PARENT_PROBE_REQ_FLAG_RESERVE_ROOT;
	if (frame.has(2)) {
		minFreeSlots = frame.u16() ?? 0;
		if (frame.has(1)) {
			probeFlags = frame.u8() ?? PARENT_PROBE_REQ_FLAG_RESERVE_ROOT;
		}
	}
	return {
		reqId,
		minFreeSlots,
		reserveRootCapacity: Boolean(
			probeFlags & PARENT_PROBE_REQ_FLAG_RESERVE_ROOT,
		),
	};
};

export const decodeParentProbeReply = (data: Uint8Array, hash: string) => {
	const frame = FrameReader.control(data, PARENT_PROBE_REPLY_PAYLOAD_BYTES);
	if (!frame) return undefined;
	const reqId = frame.u32();
	const flags = frame.u8();
	const level = frame.u16();
	const maxChildren = frame.u16();
	const freeSlots = frame.u16();
	const children = frame.u16();
	const haveToExclusive = frame.u32();
	const missingSeqs = frame.u16();
	const dataWriteDrops = frame.u32();
	const droppedForwards = frame.u32();
	if (
		reqId == null ||
		flags == null ||
		level == null ||
		maxChildren == null ||
		freeSlots == null ||
		children == null ||
		haveToExclusive == null ||
		missingSeqs == null ||
		dataWriteDrops == null ||
		droppedForwards == null
	) {
		return undefined;
	}
	return {
		reqId,
		hash,
		rooted: Boolean(flags & PARENT_PROBE_FLAG_ROOTED),
		accepting: Boolean(flags & PARENT_PROBE_FLAG_ACCEPTING),
		repairing: Boolean(flags & PARENT_PROBE_FLAG_REPAIRING),
		overloaded: Boolean(flags & PARENT_PROBE_FLAG_OVERLOADED),
		reservationToken: frame.has(PARENT_PROBE_REPLY_RESERVATION_BYTES)
			? (frame.u32() ?? 0)
			: 0,
		level,
		maxChildren,
		freeSlots,
		children,
		haveToExclusive,
		missingSeqs,
		dataWriteDrops,
		droppedForwards,
	};
};

export const encodeProviderAnnounce = (
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

export const encodeProviderQuery = (
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

const encodeProviderEntries = (entries: ProviderEntry[]) => {
	const count = Math.max(0, Math.min(255, entries.length));
	let bytes = 1;
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
	return { bytes, encoded };
};

export const encodeProviderReply = (
	namespaceKey: Uint8Array,
	reqId: number,
	entries: ProviderEntry[],
) => {
	const { bytes: entryBytes, encoded } = encodeProviderEntries(entries);
	const bytes = 1 + 32 + 4 + entryBytes;

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

export const encodeProviderSubscribe = (
	namespaceKey: Uint8Array,
	want: number,
	ttlMs: number,
) => {
	const buf = new Uint8Array(1 + 32 + 2 + 4);
	buf[0] = MSG_PROVIDER_SUBSCRIBE;
	buf.set(namespaceKey, 1);
	writeU16BE(buf, 33, clampU16(want));
	writeU32BE(buf, 35, Math.max(0, Math.floor(ttlMs)) >>> 0);
	return buf;
};

export const encodeProviderUnsubscribe = (namespaceKey: Uint8Array) => {
	const buf = new Uint8Array(1 + 32);
	buf[0] = MSG_PROVIDER_UNSUBSCRIBE;
	buf.set(namespaceKey, 1);
	return buf;
};

export const encodeProviderNotify = (
	namespaceKey: Uint8Array,
	entries: ProviderEntry[],
) => {
	const { bytes: entryBytes, encoded } = encodeProviderEntries(entries);
	const bytes = 1 + 32 + entryBytes;

	const buf = new Uint8Array(bytes);
	buf[0] = MSG_PROVIDER_NOTIFY;
	buf.set(namespaceKey, 1);
	buf[33] = Math.max(0, Math.min(255, encoded.length)) & 0xff;
	let offset = 34;
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

// --- decoders ---------------------------------------------------------------
// Message-level decoders with the exact tolerance of the historical inline
// parsers in fanout-tree.ts: `undefined` means the same minimum-length
// rejects, and truncated lists end at the same element. Multiaddr bytes are
// returned raw; `multiaddr()` construction and validity filtering stay in
// the message handlers so the native codec (which mirrors these functions
// byte-for-byte) drops the same invalid entries.

export type DecodedFanoutJoinAccept = {
	parentLevel: number;
	parentRouteFromRoot: string[];
	haveRange?: { haveFrom: number; haveToExclusive: number };
};

export type DecodedFanoutJoinReject = {
	reason: number;
	redirects: { hash: string; addrs: Uint8Array[] }[];
};

export type DecodedFanoutUnicast = {
	ackToken?: bigint;
	route: string[];
	replyRoute?: string[];
	payloadOffset: number;
};

export type DecodedFanoutTrackerReplyEntry = {
	hash: string;
	level: number;
	freeSlots: number;
	bidPerByte: number;
	addrs: Uint8Array[];
};

export type DecodedFanoutProviderEntry = {
	hash: string;
	addrs: Uint8Array[];
};

/** The shared JOIN_ACCEPT/JOIN_REJECT head (reqId) parsed before the
 * pending-join lookup. */
export const decodeJoinResponseReqId = (
	data: Uint8Array,
): number | undefined => {
	if (data.length < 1 + 32 + 4) return undefined;
	return readU32BE(data, 33);
};

export const decodeJoinAccept = (
	data: Uint8Array,
): DecodedFanoutJoinAccept | undefined => {
	if (data.length < 1 + 32 + 4 + 2 + 1) return undefined;
	const parentLevel = readU16BE(data, 37);
	const routeCount = Math.min(255, data[39]!);
	let offset = 40;
	const parentRouteFromRoot: string[] = [];
	// Unlike `decodeRoute`, this parser stops consuming at MAX_ROUTE_HOPS,
	// which the have-range appendix offset depends on.
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
	const haveRange =
		offset + 8 <= data.length
			? {
					haveFrom: readU32BE(data, offset),
					haveToExclusive: readU32BE(data, offset + 4),
				}
			: undefined;
	return { parentLevel, parentRouteFromRoot, haveRange };
};

export const decodeJoinReject = (
	data: Uint8Array,
): DecodedFanoutJoinReject | undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	const reason = data[37]! & 0xff;
	const redirects: { hash: string; addrs: Uint8Array[] }[] = [];
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
			const addrs: Uint8Array[] = [];
			const addrMax = Math.min(addrCount, JOIN_REJECT_REDIRECT_ADDR_MAX);
			for (let j = 0; j < addrMax; j++) {
				if (offset + 2 > data.length) break;
				const len = readU16BE(data, offset);
				offset += 2;
				if (offset + len > data.length) break;
				addrs.push(data.subarray(offset, offset + len));
				offset += len;
			}
			redirects.push({ hash, addrs });
		}
	}
	return { reason, redirects };
};

export const decodeEnd = (data: Uint8Array): number | undefined => {
	if (data.length < 1 + 32 + 4) return undefined;
	return readU32BE(data, 33);
};

/** MSG_REPAIR_REQ / MSG_FETCH_REQ sequence list: `count` capped by the
 * bytes actually present. */
export const decodeRepairSeqs = (data: Uint8Array): number[] | undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	const count = data[37]!;
	const max = Math.min(count, Math.floor((data.length - 38) / 4));
	const seqs: number[] = [];
	for (let i = 0; i < max; i++) {
		seqs.push(readU32BE(data, 38 + i * 4));
	}
	return seqs;
};

export const decodeIHave = (
	data: Uint8Array,
): { haveFrom: number; haveToExclusive: number } | undefined => {
	if (data.length < 1 + 32 + 4 + 4) return undefined;
	return {
		haveFrom: readU32BE(data, 33),
		haveToExclusive: readU32BE(data, 37),
	};
};

export const decodeUnicast = (
	data: Uint8Array,
): DecodedFanoutUnicast | undefined => {
	if (data.length < 1 + 32 + 1 + 1) return undefined;
	const flags = data[33]! & 0xff;
	let offset = 34;
	let ackToken: bigint | undefined;
	if (flags & UNICAST_FLAG_ACK) {
		if (data.length < offset + 8 + 1) return undefined;
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
		if (data.length < offset + 1) return undefined;
		const replyCount = Math.min(255, data[offset]!);
		offset += 1;
		const decodedReply = decodeRoute(data, offset, replyCount);
		replyRoute = decodedReply.route;
		offset = decodedReply.offset;
	}
	return {
		ackToken,
		route,
		replyRoute,
		payloadOffset: Math.min(offset, data.length),
	};
};

export const decodeUnicastAck = (
	data: Uint8Array,
): { ackToken: bigint; route: string[] } | undefined => {
	if (data.length < 1 + 32 + 8 + 1) return undefined;
	const ackToken = readU64BE(data, 33);
	const routeCount = Math.min(255, data[41]!);
	return { ackToken, route: decodeRoute(data, 42, routeCount).route };
};

export const decodeRouteQuery = (
	data: Uint8Array,
): { reqId: number; targetHash?: string } | undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	const reqId = readU32BE(data, 33);
	const hashLen = data[37]!;
	// A zero/overflowing target is answered with an empty reply, not dropped.
	const targetHash =
		hashLen === 0 || 38 + hashLen > data.length
			? undefined
			: textDecoder.decode(data.subarray(38, 38 + hashLen));
	return { reqId, targetHash };
};

export const decodeRouteReply = (
	data: Uint8Array,
): { reqId: number; route: string[] } | undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	const reqId = readU32BE(data, 33);
	const routeCount = Math.min(255, data[37]!);
	return { reqId, route: decodeRoute(data, 38, routeCount).route };
};

const decodeAddrList = (
	data: Uint8Array,
	offsetStart: number,
	addrCount: number,
): Uint8Array[] => {
	let offset = offsetStart;
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
	return addrs;
};

export const decodeTrackerAnnounce = (
	data: Uint8Array,
):
	| {
			ttlMs: number;
			level: number;
			freeSlots: number;
			bidPerByte: number;
			addrs: Uint8Array[];
	  }
	| undefined => {
	if (data.length < 1 + 32 + 4 + 2 + 2 + 2 + 4 + 1) return undefined;
	return {
		ttlMs: readU32BE(data, 33),
		level: readU16BE(data, 37),
		// maxChildren at offset 39 is kept in the wire format but unused.
		freeSlots: readU16BE(data, 41),
		bidPerByte: readU32BE(data, 43),
		addrs: decodeAddrList(data, 48, data[47]!),
	};
};

export const decodeTrackerQuery = (
	data: Uint8Array,
): { reqId: number; want: number } | undefined => {
	if (data.length < 1 + 32 + 4 + 2) return undefined;
	return { reqId: readU32BE(data, 33), want: readU16BE(data, 37) };
};

export const decodeTrackerReply = (
	data: Uint8Array,
):
	| { reqId: number; entries: DecodedFanoutTrackerReplyEntry[] }
	| undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	const reqId = readU32BE(data, 33);
	const count = data[37]!;
	let offset = 38;
	const entries: DecodedFanoutTrackerReplyEntry[] = [];
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
		const addrs: Uint8Array[] = [];
		const addrMax = Math.min(addrCount, 16);
		for (let j = 0; j < addrMax; j++) {
			if (offset + 2 > data.length) break;
			const len = readU16BE(data, offset);
			offset += 2;
			if (offset + len > data.length) break;
			addrs.push(data.subarray(offset, offset + len));
			offset += len;
		}
		entries.push({ hash, level, freeSlots, bidPerByte, addrs });
	}
	return { reqId, entries };
};

export const decodeTrackerFeedback = (
	data: Uint8Array,
): { candidateHash: string; event: number; reason: number } | undefined => {
	if (data.length < 1 + 32 + 1 + 1 + 1) return undefined;
	const hashLen = data[33]!;
	const offset = 34;
	if (offset + hashLen + 2 > data.length) return undefined;
	return {
		candidateHash: textDecoder.decode(
			data.subarray(offset, offset + hashLen),
		),
		event: data[offset + hashLen]! & 0xff,
		reason: data[offset + hashLen + 1]! & 0xff,
	};
};

export const decodeProviderAnnounce = (
	data: Uint8Array,
): { ttlMs: number; addrs: Uint8Array[] } | undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	return {
		ttlMs: readU32BE(data, 33),
		addrs: decodeAddrList(data, 38, data[37]!),
	};
};

export const decodeProviderQuery = (
	data: Uint8Array,
): { reqId: number; want: number; seed: number } | undefined => {
	if (data.length < 1 + 32 + 4 + 2 + 4) return undefined;
	return {
		reqId: readU32BE(data, 33),
		want: readU16BE(data, 37),
		seed: readU32BE(data, 39),
	};
};

/** `decodeProviderEntries` with the multiaddr construction left to the
 * caller: entries with an empty hash are consumed but skipped. */
const decodeProviderEntriesRaw = (
	data: Uint8Array,
	offsetStart: number,
	maxCount: number,
): DecodedFanoutProviderEntry[] => {
	let offset = offsetStart;
	const providers: DecodedFanoutProviderEntry[] = [];
	const limit = Math.min(maxCount, 255);
	for (let i = 0; i < limit; i++) {
		if (offset + 1 > data.length) break;
		const hashLen = data[offset++]!;
		if (offset + hashLen > data.length) break;
		const hash = textDecoder.decode(data.subarray(offset, offset + hashLen));
		offset += hashLen;
		if (offset + 1 > data.length) break;
		const addrCount = data[offset++]!;
		const addrs: Uint8Array[] = [];
		const addrMax = Math.min(addrCount, 16);
		for (let j = 0; j < addrMax; j++) {
			if (offset + 2 > data.length) break;
			const len = readU16BE(data, offset);
			offset += 2;
			if (offset + len > data.length) break;
			addrs.push(data.subarray(offset, offset + len));
			offset += len;
		}
		if (!hash) continue;
		providers.push({ hash, addrs });
	}
	return providers;
};

export const decodeProviderReply = (
	data: Uint8Array,
): { reqId: number; entries: DecodedFanoutProviderEntry[] } | undefined => {
	if (data.length < 1 + 32 + 4 + 1) return undefined;
	return {
		reqId: readU32BE(data, 33),
		entries: decodeProviderEntriesRaw(data, 38, data[37]!),
	};
};

export const decodeProviderNotify = (
	data: Uint8Array,
): { entries: DecodedFanoutProviderEntry[] } | undefined => {
	if (data.length < 1 + 32 + 1) return undefined;
	return { entries: decodeProviderEntriesRaw(data, 34, data[33]!) };
};

export const decodeProviderSubscribe = (
	data: Uint8Array,
): { want: number; ttlMs: number } | undefined => {
	if (data.length < 1 + 32 + 2 + 4) return undefined;
	return { want: readU16BE(data, 33), ttlMs: readU32BE(data, 35) };
};

export const decodeProviderEntries = (
	data: Uint8Array,
	offsetStart: number,
	maxCount: number,
) => {
	let offset = offsetStart;
	const providers: ProviderCandidate[] = [];
	const limit = Math.min(maxCount, 255);
	for (let i = 0; i < limit; i++) {
		if (offset + 1 > data.length) break;
		const hashLen = data[offset++]!;
		if (offset + hashLen > data.length) break;
		const hash = textDecoder.decode(data.subarray(offset, offset + hashLen));
		offset += hashLen;
		if (offset + 1 > data.length) break;
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
				// ignore invalid
			}
		}
		if (!hash) continue;
		providers.push({ hash, addrs });
	}
	return { providers, offset };
};

/**
 * The complete `/peerbit/fanout-tree/0.5.0` wire codec seam. FanoutTree
 * dispatches every frame encode/decode through this surface: the default
 * implementation is [`tsFanoutWireCodec`] (the functions in this file); in
 * rust-core mode the structurally identical native codec from
 * `@peerbit/network-rust` (`RustFanoutTree`) is used instead, with byte
 * identity covered by the golden-vector parity suite.
 */
/** Widen `Uint8Array<ArrayBuffer>` returns to plain `Uint8Array` so the
 * structurally identical native codec (`RustFanoutTree`) satisfies the
 * same seam type. */
type WireEncoder<F> = F extends (...args: infer A) => Uint8Array
	? (...args: A) => Uint8Array
	: never;

export type FanoutWireCodec = {
	encodeJoinReq: WireEncoder<typeof encodeJoinReq>;
	encodeJoinAccept: WireEncoder<typeof encodeJoinAccept>;
	encodeJoinReject: WireEncoder<typeof encodeJoinReject>;
	encodeKick: WireEncoder<typeof encodeKick>;
	encodeEnd: WireEncoder<typeof encodeEnd>;
	encodeRepairReq: WireEncoder<typeof encodeRepairReq>;
	encodeFetchReq: WireEncoder<typeof encodeFetchReq>;
	encodeIHave: WireEncoder<typeof encodeIHave>;
	encodeData: WireEncoder<typeof encodeData>;
	encodePublishProxy: WireEncoder<typeof encodePublishProxy>;
	encodeLeave: WireEncoder<typeof encodeLeave>;
	encodeUnicast: WireEncoder<typeof encodeUnicast>;
	encodeUnicastAck: WireEncoder<typeof encodeUnicastAck>;
	encodeRouteQuery: WireEncoder<typeof encodeRouteQuery>;
	encodeRouteReply: WireEncoder<typeof encodeRouteReply>;
	encodeTrackerAnnounce: WireEncoder<typeof encodeTrackerAnnounce>;
	encodeTrackerQuery: WireEncoder<typeof encodeTrackerQuery>;
	encodeTrackerReply: WireEncoder<typeof encodeTrackerReply>;
	encodeTrackerFeedback: WireEncoder<typeof encodeTrackerFeedback>;
	encodeParentProbeReq: WireEncoder<typeof encodeParentProbeReq>;
	encodeParentProbeReply: WireEncoder<typeof encodeParentProbeReply>;
	encodeProviderAnnounce: WireEncoder<typeof encodeProviderAnnounce>;
	encodeProviderQuery: WireEncoder<typeof encodeProviderQuery>;
	encodeProviderReply: WireEncoder<typeof encodeProviderReply>;
	encodeProviderSubscribe: WireEncoder<typeof encodeProviderSubscribe>;
	encodeProviderUnsubscribe: WireEncoder<typeof encodeProviderUnsubscribe>;
	encodeProviderNotify: WireEncoder<typeof encodeProviderNotify>;
	decodeJoinReq: typeof decodeJoinReq;
	decodeJoinResponseReqId: typeof decodeJoinResponseReqId;
	decodeJoinAccept: typeof decodeJoinAccept;
	decodeJoinReject: typeof decodeJoinReject;
	decodeEnd: typeof decodeEnd;
	decodeRepairSeqs: typeof decodeRepairSeqs;
	decodeIHave: typeof decodeIHave;
	decodeUnicast: typeof decodeUnicast;
	decodeUnicastAck: typeof decodeUnicastAck;
	decodeRouteQuery: typeof decodeRouteQuery;
	decodeRouteReply: typeof decodeRouteReply;
	decodeTrackerAnnounce: typeof decodeTrackerAnnounce;
	decodeTrackerQuery: typeof decodeTrackerQuery;
	decodeTrackerReply: typeof decodeTrackerReply;
	decodeTrackerFeedback: typeof decodeTrackerFeedback;
	decodeParentProbeReq: typeof decodeParentProbeReq;
	decodeParentProbeReply: typeof decodeParentProbeReply;
	decodeProviderAnnounce: typeof decodeProviderAnnounce;
	decodeProviderQuery: typeof decodeProviderQuery;
	decodeProviderReply: typeof decodeProviderReply;
	decodeProviderNotify: typeof decodeProviderNotify;
	decodeProviderSubscribe: typeof decodeProviderSubscribe;
};

export const tsFanoutWireCodec: FanoutWireCodec = {
	encodeJoinReq,
	encodeJoinAccept,
	encodeJoinReject,
	encodeKick,
	encodeEnd,
	encodeRepairReq,
	encodeFetchReq,
	encodeIHave,
	encodeData,
	encodePublishProxy,
	encodeLeave,
	encodeUnicast,
	encodeUnicastAck,
	encodeRouteQuery,
	encodeRouteReply,
	encodeTrackerAnnounce,
	encodeTrackerQuery,
	encodeTrackerReply,
	encodeTrackerFeedback,
	encodeParentProbeReq,
	encodeParentProbeReply,
	encodeProviderAnnounce,
	encodeProviderQuery,
	encodeProviderReply,
	encodeProviderSubscribe,
	encodeProviderUnsubscribe,
	encodeProviderNotify,
	decodeJoinReq,
	decodeJoinResponseReqId,
	decodeJoinAccept,
	decodeJoinReject,
	decodeEnd,
	decodeRepairSeqs,
	decodeIHave,
	decodeUnicast,
	decodeUnicastAck,
	decodeRouteQuery,
	decodeRouteReply,
	decodeTrackerAnnounce,
	decodeTrackerQuery,
	decodeTrackerReply,
	decodeTrackerFeedback,
	decodeParentProbeReq,
	decodeParentProbeReply,
	decodeProviderAnnounce,
	decodeProviderQuery,
	decodeProviderReply,
	decodeProviderNotify,
	decodeProviderSubscribe,
};
