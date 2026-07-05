// TS adapter for the native fanout-tree components (`fanout_tree` module of
// the peerbit_wire crate). Implements the `RustFanoutTree` surface consumed
// by `@peerbit/pubsub`'s FanoutTree in rust-core mode: the complete
// `/peerbit/fanout-tree/0.5.0` big-endian frame codec (byte-identical to
// fanout-tree-codec.ts) and the parent-upgrade policy/gate decisions merged
// in PR #911 run in wasm. Multiaddr bytes cross the boundary raw; the host
// keeps `multiaddr()` construction and validity filtering as in the TS
// codec, so both modes drop the same invalid entries.
import type {
	RustDecodedFanoutJoinAccept,
	RustDecodedFanoutJoinReject,
	RustDecodedFanoutParentProbeReply,
	RustDecodedFanoutProviderEntry,
	RustDecodedFanoutTrackerReplyEntry,
	RustFanoutTree,
	RustParentUpgradeGateOptions,
	RustParentUpgradeGateState,
	RustParentUpgradeOptions,
	RustParentUpgradePolicy,
} from "@peerbit/stream";

const PARENT_PROBE_FLAG_ROOTED = 1 << 0;
const PARENT_PROBE_FLAG_ACCEPTING = 1 << 1;
const PARENT_PROBE_FLAG_REPAIRING = 1 << 2;
const PARENT_PROBE_FLAG_OVERLOADED = 1 << 3;

const PU_GATE_REASONS = [
	undefined,
	"leaf",
	"repair",
	"data",
	"cooldown",
	"quiet",
	"budget",
] as const;
const PU_GATE_RESET_RETRY_AFTER_SEQ = 0x100;
const PU_MODES = [undefined, "direct", "probe", "shadow"] as const;

type WasmFanoutDecodedFrame = {
	req_id: number;
	bid_per_byte: number;
	reservation_token: number;
	level: number;
	max_children: number;
	free_slots: number;
	children: number;
	have_from: number;
	have_to_exclusive: number;
	has_have_range: boolean;
	missing_seqs: number;
	data_write_drops: number;
	dropped_forwards: number;
	ttl_ms: number;
	want: number;
	seed: number;
	flags: number;
	event: number;
	reason: number;
	ack_token: bigint;
	has_ack: boolean;
	seqs: Uint32Array;
	route: string[];
	reply_route: string[];
	has_reply_route: boolean;
	text: string;
	has_text: boolean;
	payload_offset: number;
	min_free_slots: number;
	reserve_root_capacity: boolean;
	addrs: Uint8Array[];
	entry_hashes: string[];
	entry_levels: Uint32Array;
	entry_free_slots: Uint32Array;
	entry_bids: Uint32Array;
	entry_addr_counts: Uint32Array;
	entry_addrs: Uint8Array[];
	free?: () => void;
};

export type FanoutTreeWasmExports = {
	ft_encode_join_req(
		channelKey: Uint8Array,
		reqId: number,
		bidPerByte: number,
		parentUpgradeReservationToken: number,
	): Uint8Array;
	ft_encode_join_accept(
		channelKey: Uint8Array,
		reqId: number,
		level: number,
		parentRouteFromRoot: string[],
		hasHaveRange: boolean,
		haveFrom: number,
		haveToExclusive: number,
	): Uint8Array;
	ft_encode_join_reject(
		channelKey: Uint8Array,
		reqId: number,
		reason: number,
		redirectHashes: string[],
		redirectAddrCounts: Uint32Array,
		redirectAddrs: Uint8Array[],
	): Uint8Array;
	ft_encode_kick(channelKey: Uint8Array): Uint8Array;
	ft_encode_end(channelKey: Uint8Array, lastSeqExclusive: number): Uint8Array;
	ft_encode_repair_req(
		channelKey: Uint8Array,
		reqId: number,
		missingSeqs: Float64Array,
	): Uint8Array;
	ft_encode_fetch_req(
		channelKey: Uint8Array,
		reqId: number,
		missingSeqs: Float64Array,
	): Uint8Array;
	ft_encode_ihave(
		channelKey: Uint8Array,
		haveFrom: number,
		haveToExclusive: number,
	): Uint8Array;
	ft_encode_data(payload: Uint8Array): Uint8Array;
	ft_encode_publish_proxy(
		channelKey: Uint8Array,
		payload: Uint8Array,
	): Uint8Array;
	ft_encode_leave(channelKey: Uint8Array): Uint8Array;
	ft_encode_unicast(
		channelKey: Uint8Array,
		route: string[],
		payload: Uint8Array,
		hasAck: boolean,
		ackToken: bigint,
		replyRoute: string[],
	): Uint8Array;
	ft_encode_unicast_ack(
		channelKey: Uint8Array,
		ackToken: bigint,
		route: string[],
	): Uint8Array;
	ft_encode_route_query(
		channelKey: Uint8Array,
		reqId: number,
		targetHash: string,
	): Uint8Array;
	ft_encode_route_reply(
		channelKey: Uint8Array,
		reqId: number,
		route: string[],
	): Uint8Array;
	ft_encode_tracker_announce(
		channelKey: Uint8Array,
		ttlMs: number,
		level: number,
		maxChildren: number,
		freeSlots: number,
		bidPerByte: number,
		addrs: Uint8Array[],
	): Uint8Array;
	ft_encode_tracker_query(
		channelKey: Uint8Array,
		reqId: number,
		want: number,
	): Uint8Array;
	ft_encode_tracker_reply(
		channelKey: Uint8Array,
		reqId: number,
		entryHashes: string[],
		entryLevels: Float64Array,
		entryFreeSlots: Float64Array,
		entryBids: Float64Array,
		entryAddrCounts: Uint32Array,
		entryAddrs: Uint8Array[],
	): Uint8Array;
	ft_encode_tracker_feedback(
		channelKey: Uint8Array,
		candidateHash: string,
		event: number,
		reason: number,
	): Uint8Array;
	ft_encode_parent_probe_req(
		channelKey: Uint8Array,
		reqId: number,
		minFreeSlots: number,
		reserveRootCapacity: boolean,
	): Uint8Array;
	ft_encode_parent_probe_reply(
		channelKey: Uint8Array,
		reqId: number,
		flags: number,
		level: number,
		maxChildren: number,
		freeSlots: number,
		children: number,
		haveToExclusive: number,
		missingSeqs: number,
		dataWriteDrops: number,
		droppedForwards: number,
		reservationToken: number,
	): Uint8Array;
	ft_encode_provider_announce(
		namespaceKey: Uint8Array,
		ttlMs: number,
		addrs: Uint8Array[],
	): Uint8Array;
	ft_encode_provider_query(
		namespaceKey: Uint8Array,
		reqId: number,
		want: number,
		seed: number,
	): Uint8Array;
	ft_encode_provider_reply(
		namespaceKey: Uint8Array,
		reqId: number,
		entryHashes: string[],
		entryAddrCounts: Uint32Array,
		entryAddrs: Uint8Array[],
	): Uint8Array;
	ft_encode_provider_subscribe(
		namespaceKey: Uint8Array,
		want: number,
		ttlMs: number,
	): Uint8Array;
	ft_encode_provider_unsubscribe(namespaceKey: Uint8Array): Uint8Array;
	ft_encode_provider_notify(
		namespaceKey: Uint8Array,
		entryHashes: string[],
		entryAddrCounts: Uint32Array,
		entryAddrs: Uint8Array[],
	): Uint8Array;
	ft_decode_join_req(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_join_response_req_id(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_join_accept(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_join_reject(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_end(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_repair_seqs(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_ihave(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_unicast(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_unicast_ack(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_route_query(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_route_reply(data: Uint8Array): WasmFanoutDecodedFrame | undefined;
	ft_decode_tracker_announce(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_tracker_query(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_tracker_reply(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_tracker_feedback(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_parent_probe_req(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_parent_probe_reply(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_provider_announce(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_provider_query(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_provider_reply(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_provider_notify(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_decode_provider_subscribe(
		data: Uint8Array,
	): WasmFanoutDecodedFrame | undefined;
	ft_pu_normalize_policy(options: Float64Array): Float64Array;
	ft_pu_evaluate_gate(
		childrenSize: number,
		missingSeqsSize: number,
		lastRepairSentAt: number,
		endSeqExclusive: number,
		parentUpgradeRetryAfterSeq: number,
		maxSeqSeen: number,
		parentUpgradeCount: number,
		parentUpgradeBackoffUntil: number,
		parentUpgradeLastAt: number,
		lastParentDataAt: number,
		lastParentUpgradeActivityAt: number,
		leafOnly: boolean,
		repairGuard: boolean,
		dataGuard: boolean,
		endedAndComplete: boolean,
		maxPerPeer: number,
		cooldownMs: number,
		quietMs: number,
		repairQuietMs: number,
		now: number,
	): number;
};

const withFrame = <T>(
	frame: WasmFanoutDecodedFrame | undefined,
	map: (frame: WasmFanoutDecodedFrame) => T,
): T | undefined => {
	if (!frame) {
		return undefined;
	}
	try {
		return map(frame);
	} finally {
		frame.free?.();
	}
};

const flattenEntryAddrs = (entries: { addrs: Uint8Array[] }[]) => {
	const counts = new Uint32Array(entries.length);
	const flat: Uint8Array[] = [];
	for (const [index, entry] of entries.entries()) {
		counts[index] = entry.addrs.length;
		flat.push(...entry.addrs);
	}
	return { counts, flat };
};

const groupEntryAddrs = (
	counts: Uint32Array,
	flat: Uint8Array[],
): Uint8Array[][] => {
	const grouped: Uint8Array[][] = [];
	let offset = 0;
	for (const count of counts) {
		grouped.push(flat.slice(offset, offset + count));
		offset += count;
	}
	return grouped;
};

/** Numeric option slot: `Number(value)` verbatim (an explicit NaN is
 * kept); absent options leave a NaN placeholder that the presence mask
 * gates off. */
const num = (value: number | undefined) =>
	value == null ? Number.NaN : Number(value);
/** Tri-state boolean option: -1 unset / 0 false / 1 true. */
const tri = (value: boolean | undefined) =>
	value == null ? -1 : value ? 1 : 0;

const parentUpgradeOptionsVector = (
	options: RustParentUpgradeOptions,
): Float64Array => {
	// Fixed order documented in fanout_tree.rs (indices 0-24).
	const numerics = [
		options.parentUpgradeIntervalMs,
		options.parentUpgradeMinLevelGain,
		options.parentUpgradeRootMinLevelGain,
		options.parentUpgradeRootMinSubtreeGain,
		options.parentUpgradeNonRootMinLevelGain,
		options.parentUpgradeMinFreeSlots,
		options.parentUpgradeRootMinFreeSlots,
		options.parentUpgradeMaxChildLoadRatio,
		options.parentUpgradeRootMaxChildLoadRatio,
		options.parentUpgradeCooldownMs,
		options.parentUpgradeFailedBackoffMinMs,
		options.parentUpgradeFailedBackoffMaxMs,
		options.parentUpgradeQuietMs,
		options.parentUpgradeRepairQuietMs,
		options.parentUpgradeMaxPerPeer,
		options.parentUpgradeStaleRootProbeProbability,
		options.parentProbeTimeoutMs,
		options.parentProbeMaxPerRound,
		options.parentProbeMaxLagMessages,
		options.parentProbeRejectCooldownMs,
		options.parentProbeRejectCooldownMaxMs,
		options.parentShadowObserveMs,
		options.parentShadowMinObservations,
		options.parentShadowDualPathMs,
		options.parentShadowDualPathMinMessages,
	];
	// Bit i marks numeric option i as provided. The TS core's `??` falls
	// back only on null/undefined, so an explicitly-NaN option must stay
	// distinguishable from an absent one: it flows through
	// `Math.max(0, Math.floor(NaN))` as NaN, like the TS implementation.
	let presence = 0;
	for (const [index, value] of numerics.entries()) {
		if (value != null) {
			presence |= 1 << index;
		}
	}
	return Float64Array.from([
		...numerics.map(num),
		tri(options.parentUpgradeLeafOnly),
		tri(options.parentUpgradeRepairGuard),
		tri(options.parentUpgradeDataGuard),
		tri(options.parentUpgradeVerifyStaleRootCapacity),
		options.parentUpgradeMode === "direct"
			? 1
			: options.parentUpgradeMode === "probe"
				? 2
				: options.parentUpgradeMode === "shadow"
					? 3
					: 0,
		presence,
	]);
};

const parentUpgradePolicyFromVector = (
	policy: Float64Array,
): RustParentUpgradePolicy => ({
	intervalMs: policy[0],
	leafOnly: policy[1] !== 0,
	minLevelGain: policy[2],
	rootMinLevelGain: policy[3],
	rootMinSubtreeGain: policy[4],
	nonRootMinLevelGain: policy[5],
	minFreeSlots: policy[6],
	rootMinFreeSlots: policy[7],
	maxChildLoadRatio: policy[8],
	rootMaxChildLoadRatio: policy[9],
	staleRootProbeProbability: policy[10],
	cooldownMs: policy[11],
	quietMs: policy[12],
	repairQuietMs: policy[13],
	maxPerPeer: policy[14],
	repairGuard: policy[15] !== 0,
	dataGuard: policy[16] !== 0,
	mode: PU_MODES[policy[17]] ?? "shadow",
	verifyStaleRootCapacity: policy[18] !== 0,
	failedBackoff: { minMs: policy[19], maxMs: policy[20] },
	probe: {
		timeoutMs: policy[21],
		maxPerRound: policy[22],
		maxLagMessages: policy[23],
		rejectCooldownMs: policy[24],
		rejectCooldownMaxMs: policy[25],
	},
	shadow: {
		observeMs: policy[26],
		minObservations: policy[27],
		dualPathMs: policy[28],
		dualPathMinMessages: policy[29],
	},
});

export const createRustFanoutTree = (
	wasm: FanoutTreeWasmExports,
): RustFanoutTree => ({
	encodeJoinReq: (channelKey, reqId, bidPerByte, reservationToken = 0) =>
		wasm.ft_encode_join_req(channelKey, reqId, bidPerByte, reservationToken),
	encodeJoinAccept: (channelKey, reqId, level, parentRouteFromRoot, haveRange) =>
		wasm.ft_encode_join_accept(
			channelKey,
			reqId,
			level,
			(parentRouteFromRoot ?? []).filter((hop) => typeof hop === "string"),
			haveRange != null,
			haveRange?.haveFrom ?? 0,
			haveRange?.haveToExclusive ?? 0,
		),
	encodeJoinReject: (channelKey, reqId, reason, redirects) => {
		const sanitized = (redirects ?? [])
			.filter((redirect) => Boolean(redirect?.hash))
			.map((redirect) => ({
				hash: redirect.hash,
				addrs: (redirect.addrs ?? []).filter(
					(addr): addr is Uint8Array => addr instanceof Uint8Array,
				),
			}));
		const { counts, flat } = flattenEntryAddrs(sanitized);
		return wasm.ft_encode_join_reject(
			channelKey,
			reqId,
			reason,
			sanitized.map((redirect) => redirect.hash),
			counts,
			flat,
		);
	},
	encodeKick: (channelKey) => wasm.ft_encode_kick(channelKey),
	encodeEnd: (channelKey, lastSeqExclusive) =>
		wasm.ft_encode_end(channelKey, lastSeqExclusive),
	encodeRepairReq: (channelKey, reqId, missingSeqs) =>
		wasm.ft_encode_repair_req(channelKey, reqId, Float64Array.from(missingSeqs)),
	encodeFetchReq: (channelKey, reqId, missingSeqs) =>
		wasm.ft_encode_fetch_req(channelKey, reqId, Float64Array.from(missingSeqs)),
	encodeIHave: (channelKey, haveFrom, haveToExclusive) =>
		wasm.ft_encode_ihave(channelKey, haveFrom, haveToExclusive),
	encodeData: (payload) => wasm.ft_encode_data(payload),
	encodePublishProxy: (channelKey, payload) =>
		wasm.ft_encode_publish_proxy(channelKey, payload),
	encodeLeave: (channelKey) => wasm.ft_encode_leave(channelKey),
	encodeUnicast: (channelKey, route, payload, options) =>
		wasm.ft_encode_unicast(
			channelKey,
			(route ?? []).filter((hop) => typeof hop === "string"),
			payload,
			options?.ackToken != null,
			options?.ackToken ?? 0n,
			(options?.replyRoute ?? []).filter((hop) => typeof hop === "string"),
		),
	encodeUnicastAck: (channelKey, ackToken, route) =>
		wasm.ft_encode_unicast_ack(
			channelKey,
			ackToken,
			(route ?? []).filter((hop) => typeof hop === "string"),
		),
	encodeRouteQuery: (channelKey, reqId, targetHash) =>
		wasm.ft_encode_route_query(channelKey, reqId, targetHash),
	encodeRouteReply: (channelKey, reqId, route) =>
		wasm.ft_encode_route_reply(
			channelKey,
			reqId,
			(route ?? []).filter((hop) => typeof hop === "string"),
		),
	encodeTrackerAnnounce: (
		channelKey,
		ttlMs,
		level,
		maxChildren,
		freeSlots,
		bidPerByte,
		addrs,
	) =>
		wasm.ft_encode_tracker_announce(
			channelKey,
			ttlMs,
			level,
			maxChildren,
			freeSlots,
			bidPerByte,
			addrs.map((addr) => addr.bytes),
		),
	encodeTrackerQuery: (channelKey, reqId, want) =>
		wasm.ft_encode_tracker_query(channelKey, reqId, want),
	encodeTrackerReply: (channelKey, reqId, entries) => {
		const { counts, flat } = flattenEntryAddrs(entries);
		return wasm.ft_encode_tracker_reply(
			channelKey,
			reqId,
			entries.map((entry) => entry.hash),
			Float64Array.from(entries, (entry) => entry.level),
			Float64Array.from(entries, (entry) => entry.freeSlots),
			Float64Array.from(entries, (entry) => entry.bidPerByte),
			counts,
			flat,
		);
	},
	encodeTrackerFeedback: (channelKey, candidateHash, event, reason) =>
		wasm.ft_encode_tracker_feedback(channelKey, candidateHash, event, reason),
	encodeParentProbeReq: (
		channelKey,
		reqId,
		minFreeSlots = 0,
		reserveRootCapacity = true,
	) =>
		wasm.ft_encode_parent_probe_req(
			channelKey,
			reqId,
			minFreeSlots,
			reserveRootCapacity,
		),
	encodeParentProbeReply: (channelKey, reqId, options) =>
		wasm.ft_encode_parent_probe_reply(
			channelKey,
			reqId,
			options.flags,
			options.level,
			options.maxChildren,
			options.freeSlots,
			options.children,
			options.haveToExclusive,
			options.missingSeqs,
			options.dataWriteDrops,
			options.droppedForwards,
			options.reservationToken ?? 0,
		),
	encodeProviderAnnounce: (namespaceKey, ttlMs, addrs) =>
		wasm.ft_encode_provider_announce(
			namespaceKey,
			ttlMs,
			addrs.map((addr) => addr.bytes),
		),
	encodeProviderQuery: (namespaceKey, reqId, want, seed) =>
		wasm.ft_encode_provider_query(namespaceKey, reqId, want, seed),
	encodeProviderReply: (namespaceKey, reqId, entries) => {
		const { counts, flat } = flattenEntryAddrs(entries);
		return wasm.ft_encode_provider_reply(
			namespaceKey,
			reqId,
			entries.map((entry) => entry.hash),
			counts,
			flat,
		);
	},
	encodeProviderSubscribe: (namespaceKey, want, ttlMs) =>
		wasm.ft_encode_provider_subscribe(namespaceKey, want, ttlMs),
	encodeProviderUnsubscribe: (namespaceKey) =>
		wasm.ft_encode_provider_unsubscribe(namespaceKey),
	encodeProviderNotify: (namespaceKey, entries) => {
		const { counts, flat } = flattenEntryAddrs(entries);
		return wasm.ft_encode_provider_notify(
			namespaceKey,
			entries.map((entry) => entry.hash),
			counts,
			flat,
		);
	},

	decodeJoinReq: (data) =>
		withFrame(wasm.ft_decode_join_req(data), (frame) => ({
			reqId: frame.req_id,
			bidPerByte: frame.bid_per_byte,
			parentUpgradeReservationToken: frame.reservation_token,
		})),
	decodeJoinResponseReqId: (data) =>
		withFrame(wasm.ft_decode_join_response_req_id(data), (frame) => frame.req_id),
	decodeJoinAccept: (data) =>
		withFrame(
			wasm.ft_decode_join_accept(data),
			(frame): RustDecodedFanoutJoinAccept => ({
				parentLevel: frame.level,
				parentRouteFromRoot: frame.route,
				haveRange: frame.has_have_range
					? {
							haveFrom: frame.have_from,
							haveToExclusive: frame.have_to_exclusive,
						}
					: undefined,
			}),
		),
	decodeJoinReject: (data) =>
		withFrame(
			wasm.ft_decode_join_reject(data),
			(frame): RustDecodedFanoutJoinReject => {
				const grouped = groupEntryAddrs(
					frame.entry_addr_counts,
					frame.entry_addrs,
				);
				return {
					reason: frame.reason,
					redirects: frame.entry_hashes.map((hash, index) => ({
						hash,
						addrs: grouped[index] ?? [],
					})),
				};
			},
		),
	decodeEnd: (data) =>
		withFrame(wasm.ft_decode_end(data), (frame) => frame.have_to_exclusive),
	decodeRepairSeqs: (data) =>
		withFrame(wasm.ft_decode_repair_seqs(data), (frame) => [...frame.seqs]),
	decodeIHave: (data) =>
		withFrame(wasm.ft_decode_ihave(data), (frame) => ({
			haveFrom: frame.have_from,
			haveToExclusive: frame.have_to_exclusive,
		})),
	decodeUnicast: (data) =>
		withFrame(wasm.ft_decode_unicast(data), (frame) => ({
			ackToken: frame.has_ack ? frame.ack_token : undefined,
			route: frame.route,
			replyRoute: frame.has_reply_route ? frame.reply_route : undefined,
			payloadOffset: frame.payload_offset,
		})),
	decodeUnicastAck: (data) =>
		withFrame(wasm.ft_decode_unicast_ack(data), (frame) => ({
			ackToken: frame.ack_token,
			route: frame.route,
		})),
	decodeRouteQuery: (data) =>
		withFrame(wasm.ft_decode_route_query(data), (frame) => ({
			reqId: frame.req_id,
			targetHash: frame.has_text ? frame.text : undefined,
		})),
	decodeRouteReply: (data) =>
		withFrame(wasm.ft_decode_route_reply(data), (frame) => ({
			reqId: frame.req_id,
			route: frame.route,
		})),
	decodeTrackerAnnounce: (data) =>
		withFrame(wasm.ft_decode_tracker_announce(data), (frame) => ({
			ttlMs: frame.ttl_ms,
			level: frame.level,
			freeSlots: frame.free_slots,
			bidPerByte: frame.bid_per_byte,
			addrs: frame.addrs,
		})),
	decodeTrackerQuery: (data) =>
		withFrame(wasm.ft_decode_tracker_query(data), (frame) => ({
			reqId: frame.req_id,
			want: frame.want,
		})),
	decodeTrackerReply: (data) =>
		withFrame(wasm.ft_decode_tracker_reply(data), (frame) => {
			const grouped = groupEntryAddrs(
				frame.entry_addr_counts,
				frame.entry_addrs,
			);
			const levels = frame.entry_levels;
			const freeSlots = frame.entry_free_slots;
			const bids = frame.entry_bids;
			return {
				reqId: frame.req_id,
				entries: frame.entry_hashes.map(
					(hash, index): RustDecodedFanoutTrackerReplyEntry => ({
						hash,
						level: levels[index],
						freeSlots: freeSlots[index],
						bidPerByte: bids[index],
						addrs: grouped[index] ?? [],
					}),
				),
			};
		}),
	decodeTrackerFeedback: (data) =>
		withFrame(wasm.ft_decode_tracker_feedback(data), (frame) => ({
			candidateHash: frame.text,
			event: frame.event,
			reason: frame.reason,
		})),
	decodeParentProbeReq: (data) =>
		withFrame(wasm.ft_decode_parent_probe_req(data), (frame) => ({
			reqId: frame.req_id,
			minFreeSlots: frame.min_free_slots,
			reserveRootCapacity: frame.reserve_root_capacity,
		})),
	decodeParentProbeReply: (data, hash) =>
		withFrame(
			wasm.ft_decode_parent_probe_reply(data),
			(frame): RustDecodedFanoutParentProbeReply => ({
				reqId: frame.req_id,
				hash,
				rooted: Boolean(frame.flags & PARENT_PROBE_FLAG_ROOTED),
				accepting: Boolean(frame.flags & PARENT_PROBE_FLAG_ACCEPTING),
				repairing: Boolean(frame.flags & PARENT_PROBE_FLAG_REPAIRING),
				overloaded: Boolean(frame.flags & PARENT_PROBE_FLAG_OVERLOADED),
				reservationToken: frame.reservation_token,
				level: frame.level,
				maxChildren: frame.max_children,
				freeSlots: frame.free_slots,
				children: frame.children,
				haveToExclusive: frame.have_to_exclusive,
				missingSeqs: frame.missing_seqs,
				dataWriteDrops: frame.data_write_drops,
				droppedForwards: frame.dropped_forwards,
			}),
		),
	decodeProviderAnnounce: (data) =>
		withFrame(wasm.ft_decode_provider_announce(data), (frame) => ({
			ttlMs: frame.ttl_ms,
			addrs: frame.addrs,
		})),
	decodeProviderQuery: (data) =>
		withFrame(wasm.ft_decode_provider_query(data), (frame) => ({
			reqId: frame.req_id,
			want: frame.want,
			seed: frame.seed,
		})),
	decodeProviderReply: (data) =>
		withFrame(wasm.ft_decode_provider_reply(data), (frame) => ({
			reqId: frame.req_id,
			entries: providerEntriesFromFrame(frame),
		})),
	decodeProviderNotify: (data) =>
		withFrame(wasm.ft_decode_provider_notify(data), (frame) => ({
			entries: providerEntriesFromFrame(frame),
		})),
	decodeProviderSubscribe: (data) =>
		withFrame(wasm.ft_decode_provider_subscribe(data), (frame) => ({
			want: frame.want,
			ttlMs: frame.ttl_ms,
		})),

	normalizeParentUpgradePolicy: (options) =>
		parentUpgradePolicyFromVector(
			wasm.ft_pu_normalize_policy(parentUpgradeOptionsVector(options)),
		),
	evaluateParentUpgradeGate: (
		state: RustParentUpgradeGateState,
		options: RustParentUpgradeGateOptions,
	) => {
		const code = wasm.ft_pu_evaluate_gate(
			state.children.size,
			state.missingSeqs.size,
			state.lastRepairSentAt,
			state.endSeqExclusive,
			state.parentUpgradeRetryAfterSeq,
			state.maxSeqSeen,
			state.parentUpgradeCount,
			state.parentUpgradeBackoffUntil,
			state.parentUpgradeLastAt,
			state.lastParentDataAt,
			state.lastParentUpgradeActivityAt ?? Number.NaN,
			options.leafOnly,
			options.repairGuard,
			options.dataGuard,
			options.endedAndComplete,
			options.maxPerPeer,
			options.cooldownMs,
			options.quietMs,
			options.repairQuietMs,
			options.now,
		);
		if (code & PU_GATE_RESET_RETRY_AFTER_SEQ) {
			state.parentUpgradeRetryAfterSeq = -1;
		}
		const reason = PU_GATE_REASONS[code & 0xff];
		return reason == null ? { run: true } : { run: false, reason };
	},
});

const providerEntriesFromFrame = (
	frame: WasmFanoutDecodedFrame,
): RustDecodedFanoutProviderEntry[] => {
	const grouped = groupEntryAddrs(frame.entry_addr_counts, frame.entry_addrs);
	return frame.entry_hashes.map((hash, index) => ({
		hash,
		addrs: grouped[index] ?? [],
	}));
};
