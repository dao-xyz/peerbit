import { delay, waitForResolved } from "..";
import { MovingAverageTracker } from "../metrics.js";

describe("metrics", () => {
	it("moving averiage", async () => {
		const ma = new MovingAverageTracker(1);
		let done = false;
		const interval = setInterval(() => {
			ma.add(100); // 100 per 100 ms => 1000 per second
			try {
				expect(ma.value).toBeGreaterThan(850);
				expect(ma.value).toBeLessThan(1100);
				clearInterval(interval);
				done = true;
			} catch (error) {}
		}, 100);

		await waitForResolved(() => expect(done).toBeTrue());
	});
});
