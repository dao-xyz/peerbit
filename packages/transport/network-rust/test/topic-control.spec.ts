// Golden parity for the native topic-control-plane port: the borsh
// PubSubMessage codec (variants 0-7), the FNV-1a topic hashing behind shard
// mapping and deterministic root selection (including the f64 rounding the
// unchecked JS multiplication introduces), the candidate normalization and
// the subscribe-state convergence rules must match the TS implementations
// byte-for-byte / decision-for-decision.
import { deserialize, serialize } from "@dao-xyz/borsh";
import { TopicRootDirectory } from "@peerbit/pubsub";
import {
	GetSubscribers,
	PeerUnavailable,
	PubSubData,
	PubSubMessage,
	Subscribe,
	TopicRootCandidates,
	TopicRootQuery,
	TopicRootQueryResponse,
	Unsubscribe,
} from "@peerbit/pubsub-interface";
import type { RustTopicControl } from "@peerbit/stream";
import { expect } from "chai";
import { createRustCoreStream } from "../src/index.js";

// The TS reference from pubsub/src/index.ts (plain-number FNV-1a variant).
const topicHash32 = (topic: string) => {
	let hash = 0x811c9dc5; // FNV-1a
	for (let index = 0; index < topic.length; index++) {
		hash ^= topic.charCodeAt(index);
		hash = (hash * 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

// Deterministic PRNG so the fuzz corpus is stable across runs.
const mulberry32 = (seed: number) => () => {
	seed |= 0;
	seed = (seed + 0x6d2b79f5) | 0;
	let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
	t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
	return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};

const randomUtf16String = (rand: () => number, maxUnits: number) => {
	const units: number[] = [];
	const length = Math.floor(rand() * maxUnits);
	for (let i = 0; i < length; i++) {
		// Skip lone surrogates: they cannot round-trip through UTF-8 borsh
		// strings, which both implementations reject/replace differently.
		let unit = Math.floor(rand() * 0xffff);
		if (unit >= 0xd800 && unit <= 0xdfff) {
			unit = unit - 0xd800 + 0x40;
		}
		units.push(unit);
	}
	return String.fromCharCode(...units);
};

describe("topic-control parity", () => {
	let topicControl: RustTopicControl;

	before(async () => {
		const core = await createRustCoreStream();
		expect(core.topicControl).to.exist;
		topicControl = core.topicControl!;
	});

	const corpus = (): PubSubMessage[] => [
		new PubSubData({
			topics: ["a", "b"],
			data: new Uint8Array([1, 2, 3]),
			strict: true,
		}),
		new PubSubData({ topics: [], data: new Uint8Array(0), strict: false }),
		new PubSubData({
			topics: ["日本語", "💜", ""],
			data: new Uint8Array(1024).fill(7),
			strict: false,
		}),
		new Subscribe({ topics: ["t1"], requestSubscribers: true }),
		new Subscribe({ topics: [], requestSubscribers: false }),
		new Unsubscribe({ topics: ["t1", "t2"] }),
		new GetSubscribers({ topics: ["héllo"] }),
		new TopicRootCandidates({ candidates: ["c1", "c2", "c3"] }),
		new PeerUnavailable({
			publicKeyHash: "hash",
			session: 18446744073709551615n,
			timestamp: 0n,
			topics: ["t"],
		}),
		new PeerUnavailable({
			publicKeyHash: "",
			session: 0n,
			timestamp: 1719856000123n,
			topics: [],
		}),
		new TopicRootQuery({ requestId: 0xffffffff, topic: "topic" }),
		new TopicRootQueryResponse({
			requestId: 1,
			topic: "topic",
			root: "root",
		}),
		new TopicRootQueryResponse({ requestId: 2, topic: "topic" }),
	];

	const nativeEncode = (message: PubSubMessage): Uint8Array => {
		if (message instanceof PubSubData) {
			return topicControl.encodePubSubData(
				message.topics,
				message.strict,
				message.data,
			);
		}
		if (message instanceof Subscribe) {
			return topicControl.encodeSubscribe(
				message.topics,
				message.requestSubscribers,
			);
		}
		if (message instanceof Unsubscribe) {
			return topicControl.encodeUnsubscribe(message.topics);
		}
		if (message instanceof GetSubscribers) {
			return topicControl.encodeGetSubscribers(message.topics);
		}
		if (message instanceof TopicRootCandidates) {
			return topicControl.encodeTopicRootCandidates(message.candidates);
		}
		if (message instanceof PeerUnavailable) {
			return topicControl.encodePeerUnavailable(
				message.publicKeyHash,
				message.session,
				message.timestamp,
				message.topics,
			);
		}
		if (message instanceof TopicRootQuery) {
			return topicControl.encodeTopicRootQuery(
				message.requestId,
				message.topic,
			);
		}
		if (message instanceof TopicRootQueryResponse) {
			return topicControl.encodeTopicRootQueryResponse(
				message.requestId,
				message.topic,
				message.root,
			);
		}
		throw new Error("unhandled message class");
	};

	it("encodes every PubSubMessage variant byte-identically to borsh", () => {
		for (const message of corpus()) {
			expect([...nativeEncode(message)]).to.deep.equal(
				[...serialize(message)],
				`encode parity for ${message.constructor.name}`,
			);
		}
	});

	it("decodes TS-serialized frames into equivalent messages", () => {
		for (const message of corpus()) {
			const frame = serialize(message);
			const decoded = topicControl.decodePubSubMessage(frame);
			// Re-encode the decoded shape natively and compare with the TS
			// bytes: any dropped/garbled field would change the frame.
			let reencoded: Uint8Array;
			switch (decoded.type) {
				case "data":
					expect(message).to.be.instanceOf(PubSubData);
					reencoded = topicControl.encodePubSubData(
						decoded.topics,
						decoded.strict,
						decoded.data,
					);
					break;
				case "subscribe":
					reencoded = topicControl.encodeSubscribe(
						decoded.topics,
						decoded.requestSubscribers,
					);
					break;
				case "unsubscribe":
					reencoded = topicControl.encodeUnsubscribe(decoded.topics);
					break;
				case "get-subscribers":
					reencoded = topicControl.encodeGetSubscribers(decoded.topics);
					break;
				case "topic-root-candidates":
					reencoded = topicControl.encodeTopicRootCandidates(
						decoded.candidates,
					);
					break;
				case "peer-unavailable":
					reencoded = topicControl.encodePeerUnavailable(
						decoded.publicKeyHash,
						decoded.session,
						decoded.timestamp,
						decoded.topics,
					);
					break;
				case "topic-root-query":
					reencoded = topicControl.encodeTopicRootQuery(
						decoded.requestId,
						decoded.topic,
					);
					break;
				case "topic-root-query-response":
					reencoded = topicControl.encodeTopicRootQueryResponse(
						decoded.requestId,
						decoded.topic,
						decoded.root,
					);
					break;
			}
			expect([...reencoded]).to.deep.equal(
				[...frame],
				`decode parity for ${message.constructor.name}`,
			);
		}
	});

	it("TS deserializes native frames (reverse direction)", () => {
		for (const message of corpus()) {
			const decoded = deserialize(nativeEncode(message), PubSubMessage);
			expect([...serialize(decoded)]).to.deep.equal([...serialize(message)]);
		}
	});

	it("aliases PubSubData payload bytes without copying", () => {
		const frame = serialize(
			new PubSubData({
				topics: ["t"],
				data: new Uint8Array([9, 9, 9]),
				strict: false,
			}),
		);
		const decoded = topicControl.decodePubSubMessage(frame);
		if (decoded.type !== "data") throw new Error("expected data");
		expect(decoded.data.buffer).to.equal(frame.buffer);
		expect([...decoded.data]).to.deep.equal([9, 9, 9]);
	});

	it("rejects malformed frames like the TS decoder", () => {
		const bad: Uint8Array[] = [
			new Uint8Array(0),
			new Uint8Array([8]), // unknown variant
			new Uint8Array([1, 0, 0, 0, 0, 2]), // non-boolean flag
		];
		const trailing = serialize(new Unsubscribe({ topics: ["a"] }));
		bad.push(new Uint8Array([...trailing, 0]));
		for (const frame of bad) {
			expect(() => topicControl.decodePubSubMessage(frame)).to.throw();
			expect(() => PubSubMessage.from(frame)).to.throw();
		}
	});

	it("matches the JS topicHash32 exactly (incl. f64 rounding overflow)", () => {
		const fixed = [
			"",
			"a",
			"abc",
			"hello world",
			"/peerbit/pubsub-shard/1/0",
			"héllo",
			"日本語",
			"💜",
			"￿￿￿￿",
		];
		const rand = mulberry32(0xbeef);
		for (let i = 0; i < 500; i++) {
			fixed.push(randomUtf16String(rand, 64));
		}
		for (const topic of fixed) {
			const shardCount = 256;
			const prefix = "/peerbit/pubsub-shard/1/";
			expect(
				topicControl.shardTopic(topic, shardCount, prefix),
			).to.equal(
				`${prefix}${topicHash32(topic) % shardCount}`,
				`hash parity for ${JSON.stringify(topic)}`,
			);
		}
	});

	it("normalizes auto topic-root candidates like the TS implementation", () => {
		const tsNormalize = (candidates: string[], me: string) => {
			const unique = new Set<string>();
			for (const c of candidates) {
				if (!c) continue;
				unique.add(c);
			}
			unique.add(me);
			const sorted = [...unique].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
			return sorted.slice(0, 64);
		};
		const cases: string[][] = [
			[],
			["b", "a", "b", ""],
			["z", "y", "x"],
			["\u{1d400}", "Ａ"], // surrogate-pair vs BMP sort order
			Array.from({ length: 100 }, (_, i) => `peer-${(i * 7) % 100}`),
		];
		for (const candidates of cases) {
			expect(
				topicControl.normalizeAutoCandidates(candidates, "me"),
			).to.deep.equal(tsNormalize(candidates, "me"));
		}
	});

	it("applies the subscription watermark rule", () => {
		const isLatest = (
			lasts: bigint[],
			session: bigint,
			timestamp: bigint,
		) =>
			topicControl.subscriptionIsLatest(
				BigUint64Array.from(lasts),
				session,
				timestamp,
			);
		expect(isLatest([], 1n, 1n)).to.equal(true);
		expect(isLatest([1n, 30n], 2n, 20n)).to.equal(true);
		expect(isLatest([2n, 20n], 1n, 30n)).to.equal(false);
		expect(isLatest([2n, 30n], 2n, 20n)).to.equal(false);
		expect(isLatest([2n, 20n], 2n, 30n)).to.equal(true);
		// timestamp 0 sentinel skips the same-session timestamp check
		expect(isLatest([2n, 30n], 2n, 0n)).to.equal(true);
		expect(isLatest([1n, 1n, 3n, 1n], 2n, 5n)).to.equal(false);
	});

	it("applies the subscribe replacement rule", () => {
		expect(topicControl.subscribeShouldReplace(undefined, 1n)).to.equal(true);
		expect(topicControl.subscribeShouldReplace(1n, 2n)).to.equal(true);
		expect(topicControl.subscribeShouldReplace(2n, 2n)).to.equal(false);
		expect(topicControl.subscribeShouldReplace(3n, 2n)).to.equal(false);
	});

	it("backs TopicRootDirectory with native state after adoption", async () => {
		const directory = new TopicRootDirectory();
		directory.setRoot("kept", "root-1");
		directory.setDefaultCandidates(["b", "a", "b"]);

		directory.adoptNativeState(topicControl.createRootDirectoryState());

		expect(directory.getRoot("kept")).to.equal("root-1");
		expect(directory.getDefaultCandidates()).to.deep.equal(["a", "b"]);

		directory.setRoot("t", "explicit");
		expect(await directory.resolveRoot("t")).to.equal("explicit");
		directory.deleteRoot("t");
		expect(directory.getRoot("t")).to.equal(undefined);

		const deterministic = directory.resolveDeterministicCandidate("topic-x");
		expect(deterministic).to.equal(
			["a", "b"][topicHash32("topic-x") % 2],
		);

		// second adoption is a no-op: the first native state owns the data
		directory.adoptNativeState(topicControl.createRootDirectoryState());
		expect(directory.getRoot("kept")).to.equal("root-1");
	});
});
