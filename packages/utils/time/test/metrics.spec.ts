import { waitForResolved } from "../src/index.js";
import { MovingAverageTracker } from "../src/metrics.js";
import { expect } from "chai";

describe("metrics", () => {
	it("moving averiage", async () => {
		const ma = new MovingAverageTracker(1);
		let done = false;
		const interval = setInterval(() => {
			ma.add(100); // 100 per 100 ms => 1000 per second
			try {
				expect(ma.value).greaterThan(850);
				expect(ma.value).lessThan(1100);
				clearInterval(interval);
				done = true;
			} catch (error) { }
		}, 100);

		await waitForResolved(() => expect(done).to.be.true);
	});
});
