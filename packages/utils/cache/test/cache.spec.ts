import { delay } from "@peerbit/time";
import { Cache } from "../src/index.js";
import { expect } from "chai";

describe("cache", () => {
	it("ttl", async () => {
		const cache = new Cache({ max: 1e3, ttl: 1e3 });
		cache.add("");
		expect(cache.has("")).to.be.true;
		await delay(3000);
		expect(cache.has("")).to.be.false;
	});

	it("trim", async () => {
		const cache = new Cache({ max: 1e3, ttl: 3e3 });
		cache.add("1");
		await delay(1500);
		cache.add("2");
		expect(cache.has("1")).to.be.true;
		await delay(1600);
		expect(cache.has("1")).to.be.false;
		expect(cache.has("2")).to.be.true;
		await delay(1600);
		expect(cache.has("1")).to.be.false;
		expect(cache.has("2")).to.be.false;
	});

	it("max", async () => {
		const cache = new Cache({ max: 2, ttl: 1e6 });
		cache.add("1");
		cache.add("2");
		expect(cache.has("1")).to.be.true;
		cache.add("3");
		expect(cache.has("1")).to.be.false;
	});

	it("custom size", async () => {
		const cache = new Cache({ max: 21, ttl: 1e6 });
		cache.add("1", undefined, 10);
		cache.add("2", undefined, 10);
		expect(cache.size).equal(20);
		expect(cache.has("1")).to.be.true;
		cache.add("3", undefined, 10);
		expect(cache.has("1")).to.be.false;
		expect(cache.size).equal(20);
		cache.del("2");
		expect(cache.size).equal(10);
		cache.del("2");
		expect(cache.size).equal(10);
		cache.del("3");
		expect(cache.size).equal(0);
	});

	it("no ttl", async () => {
		const cache = new Cache({ max: 2 });
		cache.add("1");
		cache.add("2");
		expect(cache.has("1")).to.be.true;
		cache.add("3");
		expect(cache.has("1")).to.be.false;
	});

	it("reset", async () => {
		const cache = new Cache<string>({ max: 2, ttl: 1e6 });
		cache.add("1", "a");
		cache.add("1", "b");
		expect(cache.size).equal(1);
		expect(cache.get("1")).equal("b");
	});
	it("empty", async () => {
		const cache = new Cache({ max: 2, ttl: 1e6 });
		cache.has("1");
	});

	it("value", () => {
		const cache = new Cache<string>({ max: 2, ttl: 1e6 });
		cache.add("1");
		cache.add("2", "");
		expect(cache.get("1")).to.be.null;
		expect(cache.get("2")).equal("");
	});

	it("delete", () => {
		const cache = new Cache({ max: 3, ttl: 1e6 });
		cache.add("1");
		cache.add("2");
		cache.add("3");
		expect(cache.has("3")).to.be.true;
		cache.del("3");
		expect(cache.has("3")).to.be.false;
		expect(cache.size).equal(2);
		cache.add("4");
		cache.add("5");
		expect(cache.size).equal(3); // because of trimming, we do this test, to see if there are any unintended sideeffects with trim + delete
		cache.del("4");
		cache.del("5");
		cache.del("2");
		expect(cache.size).equal(0);
	});
});
