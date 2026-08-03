import {
	PubSubMessage,
	TOPIC_ROOT_CANDIDATES_MAX,
	TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES,
	TOPIC_ROOT_CANDIDATE_CLAIMS_MAX,
	TOPIC_ROOT_CANDIDATE_CLAIMS_MAX_FRAME_BYTES,
	TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES,
	TopicRootCandidateClaims,
	TopicRootCandidates,
} from "@peerbit/pubsub-interface";
import { expect } from "chai";

describe("topic-root candidate messages", () => {
	const canonicalCandidate = "A".repeat(43) + "=";

	it("rejects an oversized candidate count before deserialization", () => {
		const frame = new Uint8Array([4, TOPIC_ROOT_CANDIDATES_MAX + 1, 0, 0, 0]);

		expect(() => TopicRootCandidates.from(frame)).to.throw(
			`Topic-root candidate count exceeds ${TOPIC_ROOT_CANDIDATES_MAX}`,
		);
	});

	it("rejects an oversized frame before deserialization", () => {
		const frame = new Uint8Array(TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES + 1);
		frame[0] = 4;

		expect(() => TopicRootCandidates.from(frame)).to.throw(
			`Topic-root candidates frame exceeds ${TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES} bytes`,
		);
	});

	it("rejects non-canonical SHA-256 Base64 candidates", () => {
		const invalidCandidates = [
			canonicalCandidate.slice(0, -1),
			"A".repeat(42) + "B=", // non-zero padding bits
			"A".repeat(42) + "-=", // URL-safe/non-standard alphabet
		];

		for (const candidate of invalidCandidates) {
			const frame = new TopicRootCandidates({
				candidates: [candidate],
			}).bytes();
			expect(() => TopicRootCandidates.from(frame)).to.throw(
				"Topic-root candidate is not a canonical SHA-256 hash",
			);
		}
	});

	it("accepts a bounded canonical candidate frame", () => {
		const candidates = Array.from(
			{ length: TOPIC_ROOT_CANDIDATES_MAX },
			() => canonicalCandidate,
		);
		const decoded = TopicRootCandidates.from(
			new TopicRootCandidates({ candidates }).bytes(),
		);

		expect(decoded.candidates).to.deep.equal(candidates);
	});

	it("rejects malformed signed-claim frames before deserialization", () => {
		const oversizedFrame = new Uint8Array(
			TOPIC_ROOT_CANDIDATE_CLAIMS_MAX_FRAME_BYTES + 1,
		);
		oversizedFrame[0] = 8;
		const oversizedClaimLength = TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES + 1;
		const cases: Array<[Uint8Array, string]> = [
			[
				new Uint8Array([8, TOPIC_ROOT_CANDIDATE_CLAIMS_MAX + 1, 0, 0, 0]),
				`Topic-root candidate claim count exceeds ${TOPIC_ROOT_CANDIDATE_CLAIMS_MAX}`,
			],
			[
				oversizedFrame,
				`Topic-root candidate claims frame exceeds ${TOPIC_ROOT_CANDIDATE_CLAIMS_MAX_FRAME_BYTES} bytes`,
			],
			[
				new Uint8Array([
					8,
					1,
					0,
					0,
					0,
					oversizedClaimLength & 0xff,
					(oversizedClaimLength >>> 8) & 0xff,
					0,
					0,
				]),
				`Topic-root candidate claim exceeds ${TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES} bytes`,
			],
			[
				new Uint8Array([8, 1, 0, 0, 0, 3, 0]),
				"Topic-root candidate claims frame is truncated",
			],
			[
				new Uint8Array([8, 1, 0, 0, 0, 3, 0, 0, 0, 1, 2]),
				"Topic-root candidate claims frame is truncated",
			],
			[
				new Uint8Array([8, 0, 0, 0, 0, 7]),
				"Topic-root candidate claims frame has trailing bytes",
			],
		];

		for (const [frame, error] of cases) {
			expect(() => TopicRootCandidateClaims.from(frame)).to.throw(error);
		}
	});

	it("round-trips a maximally bounded opaque signed-claim aggregate", () => {
		const claims = Array.from(
			{ length: TOPIC_ROOT_CANDIDATE_CLAIMS_MAX },
			(_, index) =>
				new Uint8Array(TOPIC_ROOT_CANDIDATE_CLAIM_MAX_BYTES).fill(index),
		);
		const encoded = new TopicRootCandidateClaims({ claims }).bytes();
		const decoded = TopicRootCandidateClaims.from(encoded);

		expect(encoded.byteLength).to.equal(
			TOPIC_ROOT_CANDIDATE_CLAIMS_MAX_FRAME_BYTES,
		);
		expect(decoded.claims).to.deep.equal(claims);
		expect(
			PubSubMessage.from(
				encoded instanceof Uint8Array ? encoded : encoded.subarray(),
			),
		).to.be.instanceOf(TopicRootCandidateClaims);
	});
});
