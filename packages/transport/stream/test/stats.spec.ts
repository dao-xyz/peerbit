import { delay } from "@peerbit/time";
import { expect } from "chai";
import { BandwidthTracker } from "../src/stats.js";

describe("bandwidth-tracker", () => {
	let tracker: BandwidthTracker;
	beforeEach(() => {
		tracker = new BandwidthTracker();
		tracker.start();
	});
	afterEach(() => {
		tracker.stop();
	});
	it("resets after a while", async () => {
		expect(tracker.value).equal(0);
		tracker.add(1e3);
		let v0 = tracker.value;
		expect(v0).greaterThan(0);
		await delay(1000);
		const v1 = tracker.value;
		expect(v1).lessThan(v0);
		expect(v1).greaterThan(5);
		await delay(2000);
		const v2 = tracker.value;
		expect(v2).lessThan(1);
	});
});
