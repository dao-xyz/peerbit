import { PIDReplicationController } from "../pid";

describe("PIDReplicationController", () => {
	describe("cpu", () => {
		it("bounded by cpu available", () => {
			const controller = new PIDReplicationController("", { cpu: { max: 0 } });
			let cpuUsage = 1;
			expect(
				controller.step({
					currentFactor: 0,
					memoryUsage: 0,
					peerCount: 2,
					totalFactor: 1,
					cpuUsage
				})
			).toEqual(0);
			cpuUsage = 0;
			expect(
				controller.step({
					currentFactor: 0.5,
					memoryUsage: 0,
					peerCount: 2,
					totalFactor: 1,
					cpuUsage
				})
			).toBeWithin(0.4, 0.6); // no change
		});

		it("respects peer count of 1", () => {
			const controller = new PIDReplicationController("", { cpu: { max: 0 } });
			let cpuUsage = 1;
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					peerCount: 1,
					totalFactor: 1,
					cpuUsage
				})
			).toEqual(1);
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					peerCount: 1,
					totalFactor: 1,
					cpuUsage
				})
			).toEqual(1);
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					peerCount: 1,
					totalFactor: 1,
					cpuUsage
				})
			).toEqual(1);
		});

		it("coverges to zero of cpu usage is max", () => {
			const controller = new PIDReplicationController("", { cpu: { max: 0 } });
			let cpuUsage = 1;
			let f = 1;
			for (let i = 0; i < 10; i++) {
				f = controller.step({
					currentFactor: f,
					memoryUsage: 0,
					peerCount: 1,
					totalFactor: 2,
					cpuUsage
				});
			}
			expect(f).toEqual(0);
		});

		it("ignores balance if cpu usage is max", () => {
			const controller = new PIDReplicationController("", { cpu: { max: 0 } });
			let cpuUsage = 1;
			let f = 1;
			for (let i = 0; i < 10; i++) {
				f = controller.step({
					currentFactor: f,
					memoryUsage: 0,
					totalFactor: 0.666,
					peerCount: 2,
					cpuUsage
				});
			}
			expect(f).toEqual(0);
		});

		it("respects cpu limit", () => {
			const controller = new PIDReplicationController("", {
				cpu: { max: 0.5 }
			});
			let cpuUsage = 0.4; // < 0.5
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					totalFactor: 1,
					peerCount: 2,
					cpuUsage
				})
			).toEqual(1);
			cpuUsage = 0.6;
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					totalFactor: 1,
					peerCount: 2,
					cpuUsage
				})
			).toBeLessThan(1);
		});
	});
});
