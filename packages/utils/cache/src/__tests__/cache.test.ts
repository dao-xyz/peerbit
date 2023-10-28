import { delay } from "@peerbit/time";
import { Cache } from "../index.js";

describe("cache", () => {
	it("ttl", async () => {
		const cache = new Cache({ max: 1e3, ttl: 1e3 });
		cache.add("");
		expect(cache.has("")).toBeTrue();
		await delay(3000);
		expect(cache.has("")).toBeFalse();
	});

	it("trim", async () => {
		const cache = new Cache({ max: 1e3, ttl: 3e3 });
		cache.add("1");
		await delay(1500);
		cache.add("2");
		expect(cache.has("1")).toBeTrue();
		await delay(1500);
		expect(cache.has("1")).toBeFalse();
		expect(cache.has("2")).toBeTrue();
		await delay(1500);
		expect(cache.has("1")).toBeFalse();
		expect(cache.has("2")).toBeFalse();
	});

	it("max", async () => {
		const cache = new Cache({ max: 2, ttl: 1e6 });
		cache.add("1");
		cache.add("2");
		expect(cache.has("1")).toBeTrue();
		cache.add("3");
		expect(cache.has("1")).toBeFalse();
	});

	it("custom size", async () => {
		const cache = new Cache({ max: 21, ttl: 1e6 });
		cache.add("1", undefined, 10);
		cache.add("2", undefined, 10);
		expect(cache.size).toEqual(20);
		expect(cache.has("1")).toBeTrue();
		cache.add("3", undefined, 10);
		expect(cache.has("1")).toBeFalse();
		expect(cache.size).toEqual(20);
		cache.del("2");
		expect(cache.size).toEqual(10);
		cache.del("2");
		expect(cache.size).toEqual(10);
		cache.del("3");
		expect(cache.size).toEqual(0);
	});

	it("no ttl", async () => {
		const cache = new Cache({ max: 2 });
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
		expect(cache.size).toEqual(1);
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

	it("delete", () => {
		const cache = new Cache({ max: 3, ttl: 1e6 });
		cache.add("1");
		cache.add("2");
		cache.add("3");
		expect(cache.has("3")).toBeTrue();
		cache.del("3");
		expect(cache.has("3")).toBeFalse();
		expect(cache.size).toEqual(2);
		cache.add("4");
		cache.add("5");
		expect(cache.size).toEqual(3); // because of trimming, we do this test, to see if there are any unintended sideeffects with trim + delete
		cache.del("4");
		cache.del("5");
		cache.del("2");
		expect(cache.size).toEqual(0);
	});
});
