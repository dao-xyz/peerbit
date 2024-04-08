import { CPUUsageIntervalLag } from "../src/cpu.js";
import { expect } from "chai";
import sinon from 'sinon';

describe("CPUUsageIntervalLag", () => {
	let clock: ReturnType<typeof sinon.useFakeTimers>;
	beforeEach(() => {
		clock = sinon.useFakeTimers();
	});
	afterEach(() => {
		clock.restore();
	})

	it("appends dt", () => {
		let windowSize = 10;
		let interval = 100;
		const cpuUsage = new CPUUsageIntervalLag({
			windowSize: windowSize,
			intervalTime: interval,
			upperBoundLag: 1000
		});

		cpuUsage.start();
		// Advance time to simulate intervals
		clock.tick(1e4);
		expect(cpuUsage.dt).to.have.length(10);

		// Replace the assertions with your own based on the expected behavior of your class
		expect(cpuUsage.value()).equal(0);

		// inttroduce lag (TODO do this beter, like real testing somehow with a slow system? (child process with limited cpu (?)))
		cpuUsage.dt.push(1e3);
		cpuUsage.sum += 1e3;
		cpuUsage.sum -= cpuUsage.dt.shift()!;

		expect(cpuUsage.value()).greaterThan(0);
		const c0 = cpuUsage.value();

		// pass some time, make sure lag still affect the "cpu usage"
		clock.tick(interval);
		clock.tick(interval);
		expect(cpuUsage.value()).greaterThan(0);
		expect(cpuUsage.value()).equal(c0); // average window (TODO exponential decay)

		// "flush"
		clock.tick(windowSize * interval);
		expect(cpuUsage.value()).equal(0);

		cpuUsage.stop();
	});
});
