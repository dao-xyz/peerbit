import { expect } from "chai";
import { PIDReplicationController } from "../src/pid.js";

describe("PIDReplicationController", () => {
	describe("balance", () => {
		it("keeps unconstrained peers above an even share filling coverage gaps", () => {
			const controller = new PIDReplicationController("");
			const f = controller.step({
				currentFactor: 0.64,
				memoryUsage: 0,
				totalFactor: 0.8,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0.64);
			expect(f).to.be.at.most(1);
		});

		it("keeps slightly over-even unconstrained peers filling large coverage gaps", () => {
			const controller = new PIDReplicationController("");
			const f = controller.step({
				currentFactor: 0.51,
				memoryUsage: 0,
				totalFactor: 0.74,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0.51);
		});
	});

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
					cpuUsage,
				}),
			).equal(0);
			cpuUsage = 0;
			expect(
				controller.step({
					currentFactor: 0.5,
					memoryUsage: 0,
					peerCount: 2,
					totalFactor: 1,
					cpuUsage,
				}),
			).to.be.within(0.4, 0.6); // no change
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
					cpuUsage,
				}),
			).equal(1);
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					peerCount: 1,
					totalFactor: 1,
					cpuUsage,
				}),
			).equal(1);
			expect(
				controller.step({
					currentFactor: 1,
					memoryUsage: 0,
					peerCount: 1,
					totalFactor: 1,
					cpuUsage,
				}),
			).equal(1);
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
					cpuUsage,
				});
			}
			expect(f).equal(0);
		});

		it("does not deepen coverage gaps when cpu usage is max", () => {
			const controller = new PIDReplicationController("", { cpu: { max: 0 } });
			let cpuUsage = 1;
			let f = 1;
			for (let i = 0; i < 10; i++) {
				f = controller.step({
					currentFactor: f,
					memoryUsage: 0,
					totalFactor: 0.666,
					peerCount: 2,
					cpuUsage,
				});
			}
			expect(f).equal(1);
		});

		it("sheds only surplus replication when cpu usage is max", () => {
			const controller = new PIDReplicationController("", { cpu: { max: 0 } });
			const f = controller.step({
				currentFactor: 0.75,
				memoryUsage: 0,
				totalFactor: 1.25,
				peerCount: 2,
				cpuUsage: 1,
			});

			expect(f).to.be.lessThan(0.75);
			expect(f).to.be.at.least(0.5);
		});

		it("respects cpu limit while preserving coverage", () => {
			const controller = new PIDReplicationController("", {
				cpu: { max: 0.5 },
			});
			let cpuUsage = 0.4; // < 0.5
			// Keep balance/coverage neutral so we isolate the CPU limiter behavior.
			let f = controller.step({
				currentFactor: 0.5,
				memoryUsage: 0,
				totalFactor: 1,
				peerCount: 2,
				cpuUsage,
			});
			expect(f).to.be.within(0.49, 0.51);
			cpuUsage = 0.6;
			f = controller.step({
				currentFactor: 0.75,
				memoryUsage: 0,
				totalFactor: 1.25,
				peerCount: 2,
				cpuUsage,
			});
			expect(f).lessThan(0.75);
			expect(f).to.be.at.least(0.5);
		});
	});

	describe("memory", () => {
		it("uses storage headroom to recover toward an even share during transient surplus", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0.07,
				memoryUsage: 45_540,
				totalFactor: 1.06,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0.1);
			expect(f).to.be.lessThan(0.5);
		});

		it("does not grow past an even share just because storage is available", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0.5,
				memoryUsage: 45_540,
				totalFactor: 1,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.within(0.49, 0.51);
		});

		it("uses storage headroom to fill coverage gaps above an even share", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0.91,
				memoryUsage: 69_828,
				totalFactor: 0.94,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0.91);
			expect(f).to.be.at.most(1);
		});

		it("does not target-clamp an over-even peer for a small coverage gap", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0.59,
				memoryUsage: 60_000,
				totalFactor: 0.95,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0.59);
			expect(f).to.be.lessThan(0.65);
		});

		it("uses storage headroom to repair unequal capacity above an even share", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 200_000 },
			});
			const f = controller.step({
				currentFactor: 0.6,
				memoryUsage: 125_000,
				totalFactor: 0.85,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0.63);
			expect(f).to.be.at.most(1);
		});

		it("uses storage headroom to recover from a zero-width underfill", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0,
				memoryUsage: 21_252,
				totalFactor: 1,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0);
			expect(f).to.be.lessThan(0.5);
		});

		it("uses storage headroom to start an empty zero-width peer", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0,
				memoryUsage: 0,
				totalFactor: 1,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0);
			expect(f).to.be.lessThan(0.5);
		});

		it("keeps a zero-width peer stopped when storage budget is zero", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 0 },
			});
			const f = controller.step({
				currentFactor: 0,
				memoryUsage: 0,
				totalFactor: 1,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.equal(0);
		});

		it("allows coverage repair to restart an over-target zero-width peer", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 100_000 },
			});
			const f = controller.step({
				currentFactor: 0,
				memoryUsage: 200_000,
				totalFactor: 0,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.be.greaterThan(0);
			expect(f).to.be.lessThan(0.5);
		});

		it("keeps a zero-budget peer stopped even when coverage is underfilled", () => {
			const controller = new PIDReplicationController("", {
				storage: { max: 0 },
			});
			const f = controller.step({
				currentFactor: 0,
				memoryUsage: 0,
				totalFactor: 0,
				peerCount: 2,
				cpuUsage: undefined,
			});

			expect(f).to.equal(0);
		});
	});
});
