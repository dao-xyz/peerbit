// Golden parity for the native fanout-tree port: every message kind of the
// /peerbit/fanout-tree/0.5.0 big-endian codec must encode byte-identically
// to the TS implementation (including its JS numeric coercions and its
// skip/truncate rules for oversized hashes, hop lists and address lists),
// and every decoder must agree with the TS parser on every prefix of every
// frame (same minimum-length rejects, same mid-list truncation tolerance).
// The parent-upgrade policy normalization and upgrade gate (PR #911) are
// compared decision-for-decision, including the in-place retry-marker
// reset.
import { fanoutParentUpgrade, fanoutWire } from "@peerbit/pubsub";
import type { RustFanoutTree } from "@peerbit/stream";
import { expect } from "chai";
import { createRustCoreStream } from "../src/index.js";

const channelKey = Uint8Array.from({ length: 32 }, (_, i) => i + 1);

const addr = (...bytes: number[]) => Uint8Array.from(bytes);
/** Multiaddr-shaped input for the announce encoders (only `.bytes` is read). */
const ma = (...bytes: number[]) => ({ bytes: addr(...bytes) }) as any;

// Deterministic PRNG so the fuzz corpus is stable across runs.
const mulberry32 = (seed: number) => () => {
	seed |= 0;
	seed = (seed + 0x6d2b79f5) | 0;
	let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
	t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
	return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};

type EncodeCase = {
	name: string;
	ts: () => Uint8Array;
	native: (fanout: RustFanoutTree) => Uint8Array;
};

const longHash = "h".repeat(300);
const oversizedHop = "x".repeat(256);
const manyHops = Array.from({ length: 40 }, (_, i) => `hop-${i}`);
const manySeqs = Array.from({ length: 300 }, (_, i) => i * 3);
const manyAddrs = Array.from({ length: 20 }, (_, i) => ma(i, 1, 2));

const trackerEntries = [
	{
		hash: "peer-a",
		level: 2,
		freeSlots: 3,
		bidPerByte: 100.9,
		addrs: [addr(1, 2, 3), addr(4)],
		expiresAt: 0,
	},
	{
		hash: longHash, // skipped: hash > 255 bytes
		level: 1,
		freeSlots: 1,
		bidPerByte: 1,
		addrs: [addr(9)],
		expiresAt: 0,
	},
	{
		hash: "", // zero-length hash is kept by the encoder
		level: 70_000, // clamped to 0xffff
		freeSlots: -2, // clamped to 0
		bidPerByte: 5_000_000_000, // >>> 0 wraps
		addrs: [],
		expiresAt: 0,
	},
];

const providerEntries = [
	{ hash: "prov-1", addrs: [addr(1), addr(2, 3)], expiresAt: 0 },
	{ hash: longHash, addrs: [addr(7)], expiresAt: 0 }, // skipped
	{ hash: "prov-2", addrs: [], expiresAt: 0 },
];

const redirects = Array.from({ length: 6 }, (_, i) => ({
	hash: `peer-${i}`,
	addrs: Array.from({ length: 10 }, (_, j) => addr(i, j, 1)),
}));

const encodeCases: EncodeCase[] = [
	{
		name: "join-req",
		ts: () => fanoutWire.encodeJoinReq(channelKey, 7, 9),
		native: (fanout) => fanout.encodeJoinReq(channelKey, 7, 9),
	},
	{
		name: "join-req with reservation",
		ts: () => fanoutWire.encodeJoinReq(channelKey, 0xffffffff, 0, 12345),
		native: (fanout) => fanout.encodeJoinReq(channelKey, 0xffffffff, 0, 12345),
	},
	{
		name: "join-accept",
		ts: () =>
			fanoutWire.encodeJoinAccept(channelKey, 3, 2, ["root", "mid", "me"], {
				haveFrom: 5,
				haveToExclusive: 11,
			}),
		native: (fanout) =>
			fanout.encodeJoinAccept(channelKey, 3, 2, ["root", "mid", "me"], {
				haveFrom: 5,
				haveToExclusive: 11,
			}),
	},
	{
		name: "join-accept hop caps and infinite level",
		ts: () =>
			fanoutWire.encodeJoinAccept(
				channelKey,
				3,
				Number.POSITIVE_INFINITY,
				["", oversizedHop, ...manyHops],
				undefined,
			),
		native: (fanout) =>
			fanout.encodeJoinAccept(
				channelKey,
				3,
				Number.POSITIVE_INFINITY,
				["", oversizedHop, ...manyHops],
				undefined,
			),
	},
	{
		name: "join-reject plain",
		ts: () => fanoutWire.encodeJoinReject(channelKey, 4, 2),
		native: (fanout) => fanout.encodeJoinReject(channelKey, 4, 2),
	},
	{
		name: "join-reject with redirects",
		ts: () => fanoutWire.encodeJoinReject(channelKey, 4, 1, redirects),
		native: (fanout) => fanout.encodeJoinReject(channelKey, 4, 1, redirects),
	},
	{
		name: "join-reject redirects without valid addrs are dropped",
		ts: () =>
			fanoutWire.encodeJoinReject(channelKey, 4, 3, [
				{ hash: "h", addrs: [new Uint8Array(0)] },
			]),
		native: (fanout) =>
			fanout.encodeJoinReject(channelKey, 4, 3, [
				{ hash: "h", addrs: [new Uint8Array(0)] },
			]),
	},
	{
		name: "kick",
		ts: () => fanoutWire.encodeKick(channelKey),
		native: (fanout) => fanout.encodeKick(channelKey),
	},
	{
		name: "leave",
		ts: () => fanoutWire.encodeLeave(channelKey),
		native: (fanout) => fanout.encodeLeave(channelKey),
	},
	{
		name: "end",
		ts: () => fanoutWire.encodeEnd(channelKey, 0xfffffffe),
		native: (fanout) => fanout.encodeEnd(channelKey, 0xfffffffe),
	},
	{
		name: "repair-req",
		ts: () => fanoutWire.encodeRepairReq(channelKey, 5, [1, 2, 3]),
		native: (fanout) => fanout.encodeRepairReq(channelKey, 5, [1, 2, 3]),
	},
	{
		name: "repair-req count cap",
		ts: () => fanoutWire.encodeRepairReq(channelKey, 5, manySeqs),
		native: (fanout) => fanout.encodeRepairReq(channelKey, 5, manySeqs),
	},
	{
		name: "fetch-req",
		ts: () => fanoutWire.encodeFetchReq(channelKey, 6, [9]),
		native: (fanout) => fanout.encodeFetchReq(channelKey, 6, [9]),
	},
	{
		name: "ihave",
		ts: () => fanoutWire.encodeIHave(channelKey, 3, 12),
		native: (fanout) => fanout.encodeIHave(channelKey, 3, 12),
	},
	{
		name: "data",
		ts: () => fanoutWire.encodeData(addr(1, 2, 3, 4)),
		native: (fanout) => fanout.encodeData(addr(1, 2, 3, 4)),
	},
	{
		name: "publish-proxy",
		ts: () => fanoutWire.encodePublishProxy(channelKey, addr(9, 8)),
		native: (fanout) => fanout.encodePublishProxy(channelKey, addr(9, 8)),
	},
	{
		name: "unicast plain",
		ts: () => fanoutWire.encodeUnicast(channelKey, ["a", "b"], addr(1, 2)),
		native: (fanout) => fanout.encodeUnicast(channelKey, ["a", "b"], addr(1, 2)),
	},
	{
		name: "unicast with ack",
		ts: () =>
			fanoutWire.encodeUnicast(channelKey, ["a", "b", "c"], addr(1), {
				ackToken: 0xfffffffffffffff0n,
				replyRoute: ["c", "b", "a"],
			}),
		native: (fanout) =>
			fanout.encodeUnicast(channelKey, ["a", "b", "c"], addr(1), {
				ackToken: 0xfffffffffffffff0n,
				replyRoute: ["c", "b", "a"],
			}),
	},
	{
		name: "unicast hop caps",
		ts: () =>
			fanoutWire.encodeUnicast(channelKey, manyHops, new Uint8Array(0), {
				ackToken: 1n,
				replyRoute: ["", oversizedHop, "ok"],
			}),
		native: (fanout) =>
			fanout.encodeUnicast(channelKey, manyHops, new Uint8Array(0), {
				ackToken: 1n,
				replyRoute: ["", oversizedHop, "ok"],
			}),
	},
	{
		name: "unicast-ack",
		ts: () => fanoutWire.encodeUnicastAck(channelKey, 42n, ["r", "x"]),
		native: (fanout) => fanout.encodeUnicastAck(channelKey, 42n, ["r", "x"]),
	},
	{
		name: "route-query",
		ts: () => fanoutWire.encodeRouteQuery(channelKey, 9, "target"),
		native: (fanout) => fanout.encodeRouteQuery(channelKey, 9, "target"),
	},
	{
		name: "route-query truncated target",
		ts: () => fanoutWire.encodeRouteQuery(channelKey, 9, longHash),
		native: (fanout) => fanout.encodeRouteQuery(channelKey, 9, longHash),
	},
	{
		name: "route-query empty target",
		ts: () => fanoutWire.encodeRouteQuery(channelKey, 9, ""),
		native: (fanout) => fanout.encodeRouteQuery(channelKey, 9, ""),
	},
	{
		name: "route-reply",
		ts: () => fanoutWire.encodeRouteReply(channelKey, 9, ["a", "b", "c"]),
		native: (fanout) => fanout.encodeRouteReply(channelKey, 9, ["a", "b", "c"]),
	},
	{
		name: "route-reply empty",
		ts: () => fanoutWire.encodeRouteReply(channelKey, 9),
		native: (fanout) => fanout.encodeRouteReply(channelKey, 9),
	},
	{
		name: "tracker-announce",
		ts: () =>
			fanoutWire.encodeTrackerAnnounce(
				channelKey,
				60_000.9,
				2,
				8,
				5,
				100.7,
				manyAddrs,
			),
		native: (fanout) =>
			fanout.encodeTrackerAnnounce(
				channelKey,
				60_000.9,
				2,
				8,
				5,
				100.7,
				manyAddrs,
			),
	},
	{
		name: "tracker-announce js number coercions",
		ts: () =>
			fanoutWire.encodeTrackerAnnounce(
				channelKey,
				5_000_000_000,
				70_000,
				-1,
				0xffff,
				-3.5,
				[],
			),
		native: (fanout) =>
			fanout.encodeTrackerAnnounce(
				channelKey,
				5_000_000_000,
				70_000,
				-1,
				0xffff,
				-3.5,
				[],
			),
	},
	{
		name: "tracker-query",
		ts: () => fanoutWire.encodeTrackerQuery(channelKey, 4, 70_000),
		native: (fanout) => fanout.encodeTrackerQuery(channelKey, 4, 70_000),
	},
	{
		name: "tracker-reply",
		ts: () => fanoutWire.encodeTrackerReply(channelKey, 6, trackerEntries),
		native: (fanout) => fanout.encodeTrackerReply(channelKey, 6, trackerEntries),
	},
	{
		name: "tracker-feedback",
		ts: () => fanoutWire.encodeTrackerFeedback(channelKey, "candidate", 1, 2),
		native: (fanout) =>
			fanout.encodeTrackerFeedback(channelKey, "candidate", 1, 2),
	},
	{
		name: "tracker-feedback truncated hash",
		ts: () => fanoutWire.encodeTrackerFeedback(channelKey, longHash, 4, 2),
		native: (fanout) => fanout.encodeTrackerFeedback(channelKey, longHash, 4, 2),
	},
	{
		name: "parent-probe-req default",
		ts: () => fanoutWire.encodeParentProbeReq(channelKey, 3),
		native: (fanout) => fanout.encodeParentProbeReq(channelKey, 3),
	},
	{
		name: "parent-probe-req extended",
		ts: () => fanoutWire.encodeParentProbeReq(channelKey, 3, 4.9, false),
		native: (fanout) => fanout.encodeParentProbeReq(channelKey, 3, 4.9, false),
	},
	{
		name: "parent-probe-reply",
		ts: () =>
			fanoutWire.encodeParentProbeReply(channelKey, 3, {
				flags: 0b1011,
				level: 2,
				maxChildren: 70_000,
				freeSlots: 5,
				children: 3,
				haveToExclusive: 5_000_000_000,
				missingSeqs: 2,
				dataWriteDrops: 7.9,
				droppedForwards: 9,
				reservationToken: 11,
			}),
		native: (fanout) =>
			fanout.encodeParentProbeReply(channelKey, 3, {
				flags: 0b1011,
				level: 2,
				maxChildren: 70_000,
				freeSlots: 5,
				children: 3,
				haveToExclusive: 5_000_000_000,
				missingSeqs: 2,
				dataWriteDrops: 7.9,
				droppedForwards: 9,
				reservationToken: 11,
			}),
	},
	{
		name: "provider-announce",
		ts: () =>
			fanoutWire.encodeProviderAnnounce(channelKey, 30_000, [ma(1, 2, 3)]),
		native: (fanout) =>
			fanout.encodeProviderAnnounce(channelKey, 30_000, [ma(1, 2, 3)]),
	},
	{
		name: "provider-query",
		ts: () => fanoutWire.encodeProviderQuery(channelKey, 2, 3, 0xfffffffe),
		native: (fanout) =>
			fanout.encodeProviderQuery(channelKey, 2, 3, 0xfffffffe),
	},
	{
		name: "provider-reply",
		ts: () => fanoutWire.encodeProviderReply(channelKey, 2, providerEntries),
		native: (fanout) =>
			fanout.encodeProviderReply(channelKey, 2, providerEntries),
	},
	{
		name: "provider-subscribe",
		ts: () => fanoutWire.encodeProviderSubscribe(channelKey, 3, 45_000),
		native: (fanout) => fanout.encodeProviderSubscribe(channelKey, 3, 45_000),
	},
	{
		name: "provider-unsubscribe",
		ts: () => fanoutWire.encodeProviderUnsubscribe(channelKey),
		native: (fanout) => fanout.encodeProviderUnsubscribe(channelKey),
	},
	{
		name: "provider-notify",
		ts: () => fanoutWire.encodeProviderNotify(channelKey, providerEntries),
		native: (fanout) => fanout.encodeProviderNotify(channelKey, providerEntries),
	},
];

describe("fanout-tree parity", () => {
	let fanout: RustFanoutTree;

	before(async () => {
		const core = await createRustCoreStream();
		expect(core.fanout).to.exist;
		fanout = core.fanout!;
	});

	describe("encode byte identity", () => {
		for (const testCase of encodeCases) {
			it(testCase.name, () => {
				expect([...testCase.native(fanout)]).to.deep.equal([
					...testCase.ts(),
				]);
			});
		}
	});

	describe("decode parity on every prefix of every frame", () => {
		// Decoder pairs (TS reference, native) that must agree on arbitrary
		// input, not only on frames of their own kind: the host routes by
		// the kind byte before decoding, so the parsers themselves never
		// inspect it.
		const decoderPairs = (): [
			string,
			(data: Uint8Array) => unknown,
			(data: Uint8Array) => unknown,
		][] => [
			["joinReq", fanoutWire.decodeJoinReq, (d) => fanout.decodeJoinReq(d)],
			[
				"joinResponseReqId",
				fanoutWire.decodeJoinResponseReqId,
				(d) => fanout.decodeJoinResponseReqId(d),
			],
			[
				"joinAccept",
				fanoutWire.decodeJoinAccept,
				(d) => fanout.decodeJoinAccept(d),
			],
			[
				"joinReject",
				fanoutWire.decodeJoinReject,
				(d) => fanout.decodeJoinReject(d),
			],
			["end", fanoutWire.decodeEnd, (d) => fanout.decodeEnd(d)],
			[
				"repairSeqs",
				fanoutWire.decodeRepairSeqs,
				(d) => fanout.decodeRepairSeqs(d),
			],
			["ihave", fanoutWire.decodeIHave, (d) => fanout.decodeIHave(d)],
			["unicast", fanoutWire.decodeUnicast, (d) => fanout.decodeUnicast(d)],
			[
				"unicastAck",
				fanoutWire.decodeUnicastAck,
				(d) => fanout.decodeUnicastAck(d),
			],
			[
				"routeQuery",
				fanoutWire.decodeRouteQuery,
				(d) => fanout.decodeRouteQuery(d),
			],
			[
				"routeReply",
				fanoutWire.decodeRouteReply,
				(d) => fanout.decodeRouteReply(d),
			],
			[
				"trackerAnnounce",
				fanoutWire.decodeTrackerAnnounce,
				(d) => fanout.decodeTrackerAnnounce(d),
			],
			[
				"trackerQuery",
				fanoutWire.decodeTrackerQuery,
				(d) => fanout.decodeTrackerQuery(d),
			],
			[
				"trackerReply",
				fanoutWire.decodeTrackerReply,
				(d) => fanout.decodeTrackerReply(d),
			],
			[
				"trackerFeedback",
				fanoutWire.decodeTrackerFeedback,
				(d) => fanout.decodeTrackerFeedback(d),
			],
			[
				"parentProbeReq",
				fanoutWire.decodeParentProbeReq,
				(d) => fanout.decodeParentProbeReq(d),
			],
			[
				"parentProbeReply",
				(d) => fanoutWire.decodeParentProbeReply(d, "peer"),
				(d) => fanout.decodeParentProbeReply(d, "peer"),
			],
			[
				"providerAnnounce",
				fanoutWire.decodeProviderAnnounce,
				(d) => fanout.decodeProviderAnnounce(d),
			],
			[
				"providerQuery",
				fanoutWire.decodeProviderQuery,
				(d) => fanout.decodeProviderQuery(d),
			],
			[
				"providerReply",
				fanoutWire.decodeProviderReply,
				(d) => fanout.decodeProviderReply(d),
			],
			[
				"providerNotify",
				fanoutWire.decodeProviderNotify,
				(d) => fanout.decodeProviderNotify(d),
			],
			[
				"providerSubscribe",
				fanoutWire.decodeProviderSubscribe,
				(d) => fanout.decodeProviderSubscribe(d),
			],
		];

		it("matches the TS parser on frames and all their prefixes", () => {
			const pairs = decoderPairs();
			for (const testCase of encodeCases) {
				const frame = testCase.ts();
				// Long frames (seq lists) are sampled to keep the run fast;
				// every prefix is exercised for regular control frames.
				const step = frame.length > 200 ? 7 : 1;
				const lengths: number[] = [frame.length];
				for (let length = 0; length < frame.length; length += step) {
					lengths.push(length);
				}
				for (const length of lengths) {
					const prefix = frame.subarray(0, length);
					for (const [name, tsDecode, nativeDecode] of pairs) {
						expect(nativeDecode(prefix)).to.deep.equal(
							tsDecode(prefix),
							`${name} disagrees on ${testCase.name}[0..${length})`,
						);
					}
				}
			}
		});

		it("matches the TS parser on random garbage", () => {
			const rand = mulberry32(0x5eed);
			const pairs = decoderPairs();
			for (let i = 0; i < 64; i++) {
				const length = Math.floor(rand() * 128);
				const bytes = Uint8Array.from({ length }, () =>
					Math.floor(rand() * 256),
				);
				for (const [name, tsDecode, nativeDecode] of pairs) {
					expect(nativeDecode(bytes)).to.deep.equal(
						tsDecode(bytes),
						`${name} disagrees on garbage #${i}`,
					);
				}
			}
		});
	});

	describe("parent-upgrade parity (PR #911)", () => {
		const policyCases: Record<string, any>[] = [
			{},
			{ parentUpgradeMode: "direct" },
			{ parentUpgradeMode: "probe" },
			{ parentUpgradeMode: "shadow" },
			{ parentUpgradeMode: "bogus" },
			{ parentUpgradeLeafOnly: false, parentUpgradeRepairGuard: false },
			{ parentUpgradeDataGuard: false, parentUpgradeVerifyStaleRootCapacity: true },
			{ parentUpgradeVerifyStaleRootCapacity: false },
			{
				parentUpgradeIntervalMs: 2_500.7,
				parentUpgradeMinLevelGain: 5.9,
				parentUpgradeRootMinLevelGain: 2,
				parentUpgradeRootMinSubtreeGain: 1,
				parentUpgradeNonRootMinLevelGain: 0,
			},
			{
				parentUpgradeMinFreeSlots: -3,
				parentUpgradeRootMinFreeSlots: 100.5,
				parentUpgradeMaxChildLoadRatio: 0.3,
			},
			{ parentUpgradeMaxChildLoadRatio: Number.POSITIVE_INFINITY },
			{
				parentUpgradeMaxChildLoadRatio: -1,
				parentUpgradeRootMaxChildLoadRatio: 2.5,
			},
			{ parentUpgradeStaleRootProbeProbability: 2 },
			{ parentUpgradeStaleRootProbeProbability: -0.5 },
			{
				parentUpgradeCooldownMs: 1_000,
				parentUpgradeFailedBackoffMinMs: 90_000,
				parentUpgradeFailedBackoffMaxMs: 10,
			},
			{
				parentUpgradeQuietMs: 250,
				parentUpgradeRepairQuietMs: 0,
				parentUpgradeMaxPerPeer: 0,
			},
			{
				parentProbeTimeoutMs: 0,
				parentProbeMaxPerRound: 0,
				parentProbeMaxLagMessages: 3,
				parentProbeRejectCooldownMs: 90_000,
				parentProbeRejectCooldownMaxMs: 5,
			},
			{
				parentShadowObserveMs: -5,
				parentShadowMinObservations: 0,
				parentShadowDualPathMs: 123,
				parentShadowDualPathMinMessages: 0.4,
				parentUpgradeMode: "probe",
			},
			// Explicitly-NaN numeric options: `??` falls back only on
			// absent options, so both cores must keep the NaN (through
			// `Math.max(0, Math.floor(NaN))`) instead of the defaults.
			{ parentUpgradeNonRootMinLevelGain: Number.NaN },
			{ parentProbeMaxLagMessages: Number.NaN },
			{ parentShadowObserveMs: Number.NaN },
			{
				parentUpgradeIntervalMs: Number.NaN,
				parentUpgradeMinLevelGain: Number.NaN,
				parentUpgradeCooldownMs: Number.NaN,
				parentUpgradeQuietMs: Number.NaN,
				parentUpgradeMaxPerPeer: Number.NaN,
				parentUpgradeFailedBackoffMinMs: Number.NaN,
			},
			{
				parentUpgradeMaxChildLoadRatio: Number.NaN,
				parentUpgradeRootMaxChildLoadRatio: Number.NaN,
				parentUpgradeStaleRootProbeProbability: Number.NaN,
			},
		];

		it("normalizes policies identically", () => {
			for (const options of policyCases) {
				expect(fanout.normalizeParentUpgradePolicy(options)).to.deep.equal(
					fanoutParentUpgrade.normalizeParentUpgradePolicy(options),
					JSON.stringify(options),
				);
			}
		});

		it("keeps explicitly-NaN numeric options exactly like the TS core", () => {
			// `??` falls back only on absent options, so an explicit NaN
			// flows through `Math.max(0, Math.floor(NaN))` and stays NaN
			// (silently disabling the downstream `> 0` guards) instead of
			// taking the documented defaults.
			expect(
				fanout.normalizeParentUpgradePolicy({
					parentUpgradeNonRootMinLevelGain: Number.NaN,
				}).nonRootMinLevelGain,
			).to.be.NaN;
			expect(
				fanout.normalizeParentUpgradePolicy({
					parentProbeMaxLagMessages: Number.NaN,
				}).probe.maxLagMessages,
			).to.be.NaN;
			expect(
				fanout.normalizeParentUpgradePolicy({
					parentShadowObserveMs: Number.NaN,
				}).shadow.observeMs,
			).to.be.NaN;
		});

		it("gates identically when NaN options disable the guards", () => {
			const options = {
				parentUpgradeCooldownMs: Number.NaN,
				parentUpgradeQuietMs: Number.NaN,
				parentUpgradeRepairQuietMs: Number.NaN,
			};
			const nativePolicy = fanout.normalizeParentUpgradePolicy(options);
			const tsPolicy =
				fanoutParentUpgrade.normalizeParentUpgradePolicy(options);
			expect(nativePolicy).to.deep.equal(tsPolicy);

			const now = 100_000;
			const state = () => ({
				children: { size: 0 },
				missingSeqs: { size: 0 },
				lastRepairSentAt: now - 100,
				endSeqExclusive: 1,
				parentUpgradeRetryAfterSeq: -1,
				maxSeqSeen: 0,
				parentUpgradeCount: 0,
				parentUpgradeBackoffUntil: 0,
				parentUpgradeLastAt: now - 100,
				lastParentDataAt: now - 100,
			});
			const gateOptions = (policy: typeof tsPolicy) => ({
				leafOnly: policy.leafOnly,
				repairGuard: policy.repairGuard,
				dataGuard: policy.dataGuard,
				endedAndComplete: true,
				maxPerPeer: policy.maxPerPeer,
				cooldownMs: policy.cooldownMs,
				quietMs: policy.quietMs,
				repairQuietMs: policy.repairQuietMs,
				now,
			});
			const tsResult = fanoutParentUpgrade.evaluateParentUpgradeGate(
				state() as any,
				gateOptions(tsPolicy),
			);
			const nativeResult = fanout.evaluateParentUpgradeGate(
				state(),
				gateOptions(nativePolicy),
			);
			expect(nativeResult).to.deep.equal(tsResult);
			// the NaN cooldown/quiet/repair-quiet guards are silently
			// disabled (`NaN > 0` is false): the gate runs even though the
			// recent activity in `state` would normally trip all three
			expect(nativeResult).to.deep.equal({ run: true });
		});

		it("evaluates the upgrade gate identically (fuzzed)", () => {
			const rand = mulberry32(0xfa17);
			const pick = <T>(values: T[]) =>
				values[Math.floor(rand() * values.length)];
			for (let i = 0; i < 400; i++) {
				const now = 100_000;
				const state = () => ({
					children: { size: pick([0, 1, 3]) },
					missingSeqs: { size: pick([0, 2]) },
					lastRepairSentAt: pick([0, now - 100, now - 10_000]),
					endSeqExclusive: pick([0, 1, 50]),
					parentUpgradeRetryAfterSeq: pick([-1, 5, 50]),
					maxSeqSeen: pick([0, 5, 6, 100]),
					parentUpgradeCount: pick([0, 1, 2, 5]),
					parentUpgradeBackoffUntil: pick([0, now - 1, now + 1]),
					parentUpgradeLastAt: pick([0, now - 100, now - 10_000]),
					lastParentDataAt: pick([0, now - 100, now - 10_000]),
					lastParentUpgradeActivityAt: pick([
						undefined,
						0,
						now - 100,
						now - 10_000,
					]),
				});
				const options = {
					leafOnly: rand() < 0.5,
					repairGuard: rand() < 0.5,
					dataGuard: rand() < 0.5,
					endedAndComplete: rand() < 0.5,
					maxPerPeer: pick([0, 2]),
					cooldownMs: pick([0, 5_000]),
					quietMs: pick([0, 5_000]),
					repairQuietMs: pick([0, 5_000]),
					now,
				};
				const tsState = state();
				// structuredClone keeps `undefined` fields; the gate reads them
				// through `??` so both copies must look identical.
				const nativeState = structuredClone(tsState);
				const tsResult = fanoutParentUpgrade.evaluateParentUpgradeGate(
					tsState as any,
					options,
				);
				const nativeResult = fanout.evaluateParentUpgradeGate(
					nativeState,
					options,
				);
				expect(nativeResult).to.deep.equal(
					tsResult,
					`gate #${i}: ${JSON.stringify({ tsState, options })}`,
				);
				expect(nativeState.parentUpgradeRetryAfterSeq).to.equal(
					tsState.parentUpgradeRetryAfterSeq,
					`retry-marker reset #${i}`,
				);
			}
		});
	});
});
