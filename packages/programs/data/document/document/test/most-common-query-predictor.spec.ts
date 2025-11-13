/* eslint-env mocha */
import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import * as types from "@peerbit/document-interface";
import { Compare, IntegerCompare } from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import MostCommonQueryPredictor from "../src/most-common-query-predictor.ts";

/* ───────────────────── helpers ───────────────────── */

function buildRequest(integerCondition = 0): types.SearchRequest {
	const req = new types.SearchRequest({
		query: [
			new IntegerCompare({
				key: "value",
				compare: Compare.Equal,
				value: integerCondition,
			}),
		],
		sort: [],
		fetch: 1,
	});
	return req; // deterministic ID already set above
}

const assertRequestQueriesInteger = (
	request: types.SearchRequest,
	integerCondition: number,
) => {
	expect(request.query.length).to.equal(1);
	expect(request.query[0]).to.be.instanceOf(IntegerCompare);
	const integerCompare = request.query[0] as IntegerCompare;
	expect(integerCompare.key).to.deep.eq(["value"]);
	expect(integerCompare.compare).to.equal(Compare.Equal);
	expect(integerCompare.value.value).to.equal(integerCondition);
};

/* ───────────────────── tests ───────────────────── */

describe("MostCommonQueryPredictor", () => {
	let clock: sinon.SinonFakeTimers;
	let dummyPeer: PublicSignKey; // good enough for unit tests

	beforeEach(async () => {
		dummyPeer = (await Ed25519Keypair.create()).publicKey;
		clock = sinon.useFakeTimers();
	});

	afterEach(() => {
		clock.restore();
	});

	it("returns undefined until the threshold is met", () => {
		const predictor = new MostCommonQueryPredictor(3 /* threshold */, 10_000);

		const r1 = buildRequest(1);
		predictor.onRequest(r1, { from: dummyPeer });
		predictor.onRequest(r1, { from: dummyPeer });

		const guess = predictor.predictedQuery(dummyPeer);
		expect(guess).to.be.undefined;
	});

	it("predicts the most common query once threshold is met and clones id", async () => {
		const predictor = new MostCommonQueryPredictor(2); // low threshold for test

		const r1 = buildRequest(5);
		const kp = await Ed25519Keypair.create();

		predictor.onRequest(r1, { from: kp.publicKey });
		predictor.onRequest(r1, { from: kp.publicKey }); // threshold reached

		const guess = predictor.predictedQuery(kp.publicKey) as types.SearchRequest;
		expect(guess).to.not.be.undefined;

		// Structurally equal except ID
		expect(guess.query).to.deep.equal(r1.query);
		expect(guess.sort).to.deep.equal(r1.sort);
		expect(guess.fetch).to.equal(r1.fetch);
		expect(Buffer.from(guess.id).equals(Buffer.from(r1.id))).to.be.false;
	});

	it("forgets queries after TTL expires", () => {
		const ttl = 1_000; // 1 second
		const predictor = new MostCommonQueryPredictor(1, ttl);

		const r1 = buildRequest(9);
		predictor.onRequest(r1, { from: dummyPeer });

		// Advance time just before TTL -> still predicted
		clock.tick(ttl - 1);
		expect(predictor.predictedQuery(dummyPeer)).to.not.be.undefined;

		// Advance past TTL -> entry should be cleaned up
		clock.tick(2);
		expect(predictor.predictedQuery(dummyPeer)).to.be.undefined;
	});

	it("always chooses the most frequent among multiple queries", () => {
		const predictor = new MostCommonQueryPredictor(1);

		const r1 = buildRequest(1);
		const r2 = buildRequest(2);

		predictor.onRequest(r1, { from: dummyPeer }); // r1 seen twice
		predictor.onRequest(r1, { from: dummyPeer });
		predictor.onRequest(r2, { from: dummyPeer }); // r2 seen once

		const guess = predictor.predictedQuery(dummyPeer) as types.SearchRequest;
		assertRequestQueriesInteger(guess, 1);
	});
});
