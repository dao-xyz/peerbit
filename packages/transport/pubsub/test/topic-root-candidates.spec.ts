import {
	TOPIC_ROOT_CANDIDATES_MAX,
	TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES,
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
});
