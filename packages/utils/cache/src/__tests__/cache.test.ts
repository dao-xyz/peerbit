import { delay } from "@dao-xyz/peerbit-time";
import { Cache } from "../index.js";

describe("cache", () => {
	it("ttl", async () => {
		const cache = new Cache({ max: 1e3, ttl: 1e3 });
		cache.add("");
		expect(cache.has("")).toBeTrue();
		await delay(3000);
		expect(cache.has("")).toBeFalse();
	});

	it("max", async () => {
		const cache = new Cache({ max: 2, ttl: 1e6 });
		cache.add("1");
		cache.add("2");
		expect(cache.has("1")).toBeTrue();
		cache.add("3");
		expect(cache.has("1")).toBeFalse();
	});

	it("reset", async () => {
		const cache = new Cache<string>({ max: 2, ttl: 1e6 });
		cache.add("1", "a");
		cache.add("1", "b");
		expect(cache.list.length).toEqual(1);
		expect(cache.get("1")).toEqual("b");
	});
	it("empty", async () => {
		const cache = new Cache({ max: 2, ttl: 1e6 });
		cache.has("1");
	});

	it("value", () => {
		const cache = new Cache<string>({ max: 2, ttl: 1e6 });
		cache.add("1");
		cache.add("2", "");
		expect(cache.get("1")).toBeNull();
		expect(cache.get("2")).toEqual("");
	});
});
