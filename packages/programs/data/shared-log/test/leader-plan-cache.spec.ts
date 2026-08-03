import { delay } from "@peerbit/time";
import { expect } from "chai";
import { LeaderPlanCache } from "../src/leader-plan-cache.js";

const plan = (...hashes: string[]) =>
	new Map(hashes.map((hash) => [hash, { intersecting: true }]));

describe("isLeader plan cache", () => {
	it("returns stored plans and misses on unknown keys", () => {
		const cache = new LeaderPlanCache({ max: 10, ttl: 1000 });
		cache.put("a|d", plan("x", "y"), cache.capture());
		expect(cache.get("a|d")).to.deep.eq(plan("x", "y"));
		expect(cache.get("b|d")).to.eq(undefined);
	});

	it("isolates callers from mutation of served and stored plans", () => {
		const cache = new LeaderPlanCache({ max: 10, ttl: 1000 });
		const stored = plan("x");
		cache.put("a|d", stored, cache.capture());

		// mutating the map that was stored must not affect the cache
		stored.set("z", { intersecting: true });
		const first = cache.get("a|d")!;
		expect([...first.keys()]).to.deep.eq(["x"]);

		// mutating a served map or its value objects must not affect later hits
		first.set("w", { intersecting: true });
		first.get("x")!.intersecting = false;
		const second = cache.get("a|d")!;
		expect([...second.keys()]).to.deep.eq(["x"]);
		expect(second.get("x")!.intersecting).to.eq(true);
	});

	it("drops stores whose captured version predates an invalidation", () => {
		const cache = new LeaderPlanCache({ max: 10, ttl: 1000 });
		const captured = cache.capture();
		cache.invalidate(); // topology changed while the plan was computing
		cache.put("a|d", plan("x"), captured);
		expect(cache.get("a|d")).to.eq(undefined);

		cache.put("a|d", plan("x"), cache.capture());
		expect(cache.get("a|d")).to.not.eq(undefined);
	});

	it("clears stored plans on invalidation", () => {
		const cache = new LeaderPlanCache({ max: 10, ttl: 1000 });
		cache.put("a|d", plan("x"), cache.capture());
		cache.invalidate();
		expect(cache.get("a|d")).to.eq(undefined);
	});

	it("expires plans after the ttl backstop", async () => {
		const cache = new LeaderPlanCache({ max: 10, ttl: 10 });
		cache.put("a|d", plan("x"), cache.capture());
		await delay(25);
		expect(cache.get("a|d")).to.eq(undefined);
	});

	it("bounds the store by leader-count weighted size", () => {
		const cache = new LeaderPlanCache({ max: 4, ttl: 1000 });
		const version = cache.capture();
		cache.put("a|d", plan("1", "2"), version);
		cache.put("b|d", plan("3", "4"), version);
		cache.put("c|d", plan("5", "6"), version);
		// max 4 size-units with 2-unit entries: the oldest entry is evicted
		expect(cache.get("a|d")).to.eq(undefined);
		expect(cache.get("c|d")).to.not.eq(undefined);
	});
});
