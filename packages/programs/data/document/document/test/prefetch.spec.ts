import { Cache } from "@peerbit/cache";
import {
	IterationRequest,
	PredictedSearchRequest,
	PushUpdatesMode,
	type Result,
	Results,
} from "@peerbit/document-interface";
import type { RPCResponse } from "@peerbit/rpc";
import { expect } from "chai";
import { Prefetch } from "../src/prefetch.js";

type Prediction = RPCResponse<PredictedSearchRequest<Result>>;
type RequestOptions = ConstructorParameters<typeof IterationRequest>[0];

const request = (id: number, options?: RequestOptions) => {
	const value = new IterationRequest({ fetch: 1, ...options });
	value.id = new Uint8Array(32).fill(id);
	return value;
};

const prediction = (id: number, options?: RequestOptions): Prediction => {
	const query = request(id, options);
	return {
		response: new PredictedSearchRequest({
			id: query.id,
			request: query,
			results: new Results({ results: [], kept: 1n }),
		}),
		// The accumulator never reads transport metadata; integration tests cover it.
		message: undefined as unknown as Prediction["message"],
	};
};

describe("prefetch prediction ownership", () => {
	it("returns the exact unconsumed prediction displaced by the same peer", () => {
		const prefetch = new Prefetch();
		const first = prediction(1);
		const second = prediction(2);
		expect(prefetch.add(first, "peer")).to.equal(undefined);
		expect(prefetch.add(second, "peer")).to.equal(first);
		expect(prefetch.size).to.equal(1);
		expect(prefetch.consume(request(10), "peer")).to.equal(second);
	});

	it("does not retire a duplicate of the same remote cursor", () => {
		const prefetch = new Prefetch();
		const first = prediction(1);
		const duplicate = prediction(1);
		prefetch.add(first, "peer");
		expect(prefetch.add(duplicate, "peer")).to.equal(undefined);
		expect(prefetch.consume(request(10), "peer")).to.equal(duplicate);
	});

	it("keeps different peers' cached predictions and active translations separate", () => {
		const prefetch = new Prefetch();
		const active = prediction(1);
		prefetch.add(active, "active-peer");
		expect(prefetch.consume(request(10), "active-peer")).to.equal(active);

		const first = prediction(1);
		const second = prediction(2);
		const other = prediction(3);
		prefetch.add(first, "peer");
		expect(prefetch.add(other, "other-peer")).to.equal(undefined);
		expect(prefetch.add(second, "peer")).to.equal(first);
		expect(prefetch.consume(request(11), "other-peer")).to.equal(other);
		expect(prefetch.consume(request(12), "peer")).to.equal(second);
	});

	it("captures an expired same-key prediction before cache trimming loses it", () => {
		const cache = new Cache<Prediction>({ max: 10, ttl: 1_000 });
		const prefetch = new Prefetch(cache);
		const first = prediction(1);
		const second = prediction(2);
		prefetch.add(first, "peer");
		const cached = [...cache.map.values()][0]!;
		cached.time = 0;
		expect(prefetch.add(second, "peer")).to.equal(first);
		expect(prefetch.consume(request(10), "peer")).to.equal(second);
	});

	it("preserves a consumed cursor when its duplicate is later replaced", () => {
		const prefetch = new Prefetch();
		const first = prediction(1);
		const duplicate = prediction(1);
		const second = prediction(2);
		const local = request(10);
		prefetch.add(first, "peer");
		expect(prefetch.consume(local, "peer")).to.equal(first);
		expect(prefetch.add(duplicate, "peer")).to.equal(undefined);
		expect(prefetch.add(second, "peer")).to.equal(undefined);
		expect(prefetch.getTranslationMap(local)?.get("peer")).to.deep.equal(
			first.response.request.id,
		);

		prefetch.clear(local);
		expect(prefetch.add(duplicate, "peer")).to.equal(second);
		expect(prefetch.add(prediction(3), "peer")).to.equal(duplicate);
	});

	it("allows an add listener to consume the replacement without protecting the old cursor", () => {
		const prefetch = new Prefetch();
		const first = prediction(1);
		const second = prediction(2);
		const local = request(10);
		prefetch.add(first, "peer");
		let consumed: Prediction | undefined;
		prefetch.addEventListener(
			"add",
			() => {
				consumed = prefetch.consume(local, "peer");
			},
			{ once: true },
		);
		expect(prefetch.add(second, "peer")).to.equal(first);
		expect(consumed).to.equal(second);
		expect(prefetch.size).to.equal(0);
		expect(prefetch.getTranslationMap(local)?.get("peer")).to.deep.equal(
			second.response.request.id,
		);
	});

	it("preserves ownership transferred before a listener changes the translation", () => {
		const prefetch = new Prefetch();
		const first = prediction(1);
		const local = request(10);
		prefetch.add(first, "peer");
		prefetch.consume(local, "peer");
		prefetch.add(prediction(1), "peer");
		prefetch.addEventListener("add", () => prefetch.consume(local, "peer"), {
			once: true,
		});
		const second = prediction(2);
		expect(prefetch.add(second, "peer")).to.equal(undefined);
		expect(prefetch.getTranslationMap(local)?.get("peer")).to.deep.equal(
			second.response.request.id,
		);
	});

	for (const [name, options] of [
		["notify-only push", { pushUpdates: PushUpdatesMode.NOTIFY }],
		["streaming push", { pushUpdates: PushUpdatesMode.STREAM }],
		["keepalive", { keepAliveTtl: 1_000n }],
		["zero-valued keepalive", { keepAliveTtl: 0n }],
	] as const) {
		it(`does not retire a ${name} snapshot through ordinary prediction replacement`, () => {
			const prefetch = new Prefetch();
			const first = prediction(1, options);
			const second = prediction(2, options);
			prefetch.add(first, "peer");
			expect(prefetch.add(second, "peer")).to.equal(undefined);
			expect(prefetch.consume(request(10, options), "peer")).to.equal(second);
		});
	}

	it("accepts an existing custom add override that returns void", () => {
		class CustomPrefetch extends Prefetch {
			override add(value: Prediction, peer: string): void {
				super.add(value, peer);
			}
		}
		const prefetch: Prefetch = new CustomPrefetch();
		const second = prediction(2);
		expect(prefetch.add(prediction(1), "peer")).to.equal(undefined);
		expect(prefetch.add(second, "peer")).to.equal(undefined);
		expect(prefetch.consume(request(10), "peer")).to.equal(second);
	});
});
