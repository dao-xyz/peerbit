import { delay } from "@dao-xyz/peerbit-time";
import { Cache } from "../cache.js";
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
});
