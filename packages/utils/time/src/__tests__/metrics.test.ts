import { delay } from "..";
import { MovingAverageTracker } from "../metrics.js";

describe("metrics", () => {
	it("moving averiage", async () => {
		const ma = new MovingAverageTracker(1);
		for (let i = 0; i < 200; i++) {
			ma.add(50); // 50 per 50 ms => 1000 per second
			await delay(50);
		}
		expect(ma.value).toBeGreaterThan(900);
		expect(ma.value).toBeLessThan(1050);
	});
});
