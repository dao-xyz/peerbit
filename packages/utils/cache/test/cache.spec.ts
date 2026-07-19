/* eslint-disable */
import { expect } from "chai";
import { Cache } from "../src/index.js";

describe("cache", () => {
	const withFakeNow = async (
		fn: (api: { now: (ms: number) => void }) => Promise<void> | void,
	) => {
		const originalNow = Date.now;
		let current = originalNow();
		Date.now = () => current;
		try {
			await fn({ now: (ms) => (current = ms) });
		} finally {
			Date.now = originalNow;
		}
	};

	it("ttl", async () => {
		await withFakeNow(({ now }) => {
			now(0);
			const cache = new Cache({ max: 1e3, ttl: 1e3 });
			cache.add("");
			expect(cache.has("")).to.be.true;
			now(3000);
			expect(cache.has("")).to.be.false;
		});
	});

	it("trim", async () => {
		await withFakeNow(({ now }) => {
			now(0);
			const cache = new Cache({ max: 1e3, ttl: 3e3 });
			cache.add("1");

			now(1500);
			cache.add("2");
			expect(cache.has("1")).to.be.true;

			now(3100);
			expect(cache.has("1")).to.be.false;
			expect(cache.has("2")).to.be.true;

			now(4700);
			expect(cache.has("1")).to.be.false;
			expect(cache.has("2")).to.be.false;
		});
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

	it("delete releases values and delete/re-add accounting stays exact", () => {
		const cache = new Cache<Uint8Array>({ max: 2, ttl: 1e6 });
		const retained = new Uint8Array(1024);
		cache.add("same", retained);
		expect(cache.del("same")?.value).to.equal(retained);
		expect(cache.map.has("same")).to.equal(false);
		expect(cache.size).to.equal(0);

		cache.add("same", new Uint8Array(1));
		cache.del("same");
		expect(cache.size).to.equal(0);
		expect(cache.map.size).to.equal(0);

		cache.add("a");
		cache.add("b");
		cache.add("c");
		expect(cache.size).to.equal(2);
		expect(cache.has("a")).to.equal(false);
		expect(cache.has("b")).to.equal(true);
		expect(cache.has("c")).to.equal(true);
	});

	it("replacement adjusts custom-size accounting", () => {
		const cache = new Cache<string>({ max: 10, ttl: 1e6 });
		cache.add("a", "small", 3);
		cache.add("a", "large", 8);
		expect(cache.size).to.equal(8);
		expect(cache.get("a")).to.equal("large");
		cache.add("b", "value", 3);
		expect(cache.size).to.equal(3);
		expect(cache.has("a")).to.equal(false);
		expect(cache.has("b")).to.equal(true);
	});

	it("replacement refreshes FIFO/TTL order", async () => {
		await withFakeNow(({ now }) => {
			now(0);
			const cache = new Cache<string>({ max: 2, ttl: 1_000 });
			cache.add("a", "a-old");
			now(500);
			cache.add("b", "b");
			now(900);
			cache.add("a", "a-new");

			// Refresh moves `a` behind `b`; a new entry evicts the true oldest.
			cache.add("c", "c");
			expect(cache.has("a")).to.equal(true);
			expect(cache.has("b")).to.equal(false);
			expect(cache.has("c")).to.equal(true);

			const ttlCache = new Cache<string>({ max: 3, ttl: 1_000 });
			now(0);
			ttlCache.add("head", "old");
			now(500);
			ttlCache.add("behind", "expires-first");
			now(900);
			ttlCache.add("head", "refreshed");
			now(1_600);
			expect(ttlCache.has("behind")).to.equal(false);
			expect(ttlCache.has("head")).to.equal(true);
		});
	});
});
