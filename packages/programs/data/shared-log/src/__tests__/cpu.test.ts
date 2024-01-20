import { CPUUsageIntervalLag } from "../cpu.js";
import { jest } from "@jest/globals";

describe("CPUUsageIntervalLag", () => {
	beforeEach(() => {
		jest.useFakeTimers();
	});
	test("appends dt", () => {
		let windowSize = 10;
		let interval = 100;
		const cpuUsage = new CPUUsageIntervalLag({
			windowSize: windowSize,
			intervalTime: interval,
			upperBoundLag: 1000
		});

		cpuUsage.start();
		// Advance time to simulate intervals
		jest.advanceTimersByTime(1e4);
		expect(cpuUsage.dt).toHaveLength(10);

		// Replace the assertions with your own based on the expected behavior of your class
		expect(cpuUsage.value()).toEqual(0);

		// inttroduce lag (TODO do this beter, like real testing somehow with a slow system? (child process with limited cpu (?)))
		cpuUsage.dt.push(1e3);
		cpuUsage.sum += 1e3;
		cpuUsage.sum -= cpuUsage.dt.shift()!;

		expect(cpuUsage.value()).toBeGreaterThan(0);
		const c0 = cpuUsage.value();

		// pass some time, make sure lag still affect the "cpu usage"
		jest.advanceTimersByTime(interval);
		jest.advanceTimersByTime(interval);
		expect(cpuUsage.value()).toBeGreaterThan(0);
		expect(cpuUsage.value()).toEqual(c0); // average window (TODO exponential decay)

		// "flush"
		jest.advanceTimersByTime(windowSize * interval);
		expect(cpuUsage.value()).toEqual(0);

		cpuUsage.stop();
	});
});
