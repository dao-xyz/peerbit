import { expect } from "chai";
import {
	formatFanoutTreeSimResult,
	runFanoutTreeSim,
} from "../benchmark/fanout-tree-sim-lib.js";

describe("fanout-tree-sim (ci)", () => {
	it("joins and delivers on a small sim", async function () {
		this.timeout(60_000);

		const result = await runFanoutTreeSim({
			nodes: 25,
			bootstraps: 1,
			subscribers: 20,
			relayFraction: 0.3,
			candidateScoringMode: "weighted",
			messages: 20,
			msgRate: 50,
			msgSize: 64,
			settleMs: 500,
			deadlineMs: 500,
			timeoutMs: 20_000,
			seed: 1,
			repair: true,
			rootUploadLimitBps: 100_000_000,
			relayUploadLimitBps: 100_000_000,
			rootMaxChildren: 64,
			relayMaxChildren: 32,
			dropDataFrameRate: 0,
		});

		if (
			result.joinedPct < 99 ||
			result.deliveredPct < 99 ||
			result.deliveredWithinDeadlinePct < 99 ||
			result.overheadFactorData > 1.05 ||
			result.attachP95 > 5_000 ||
			result.treeLevelP95 > 5 ||
			result.formationScore > 20 ||
			result.formationTreeOrphans > 0
		) {
			// Helpful for CI debug
			console.log(formatFanoutTreeSimResult(result));
		}

		expect(result.joinedPct).to.be.greaterThan(99);
		expect(result.deliveredPct).to.be.greaterThan(99);
		expect(result.deliveredWithinDeadlinePct).to.be.greaterThan(99);
		expect(result.overheadFactorData).to.be.lessThan(1.05);
		expect(result.attachSamples).to.equal(result.joinedCount);
		expect(result.attachP95).to.be.lessThan(5_000);
		expect(result.treeLevelP95).to.be.lessThan(5);
		expect(result.formationTreeOrphans).to.equal(0);
		expect(Number.isFinite(result.formationStretchP95)).to.equal(true);
		expect(Number.isFinite(result.formationUnderlayDistP95)).to.equal(true);
		expect(result.formationUnderlayEdges).to.be.greaterThan(0);
		expect(result.formationScore).to.be.lessThan(20);
		expect(result.protocolFetchReqSent).to.equal(0);
		expect(result.protocolIHaveSent).to.equal(0);
		expect(result.protocolControlBytesSent).to.be.lessThan(50_000);
		expect(result.trackerBpp).to.be.lessThan(3);
		expect(result.repairBpp).to.be.lessThan(0.5);
		expect(result.droppedForwardsTotal).to.equal(0);
	});

		it("delivers under mild loss + churn", async function () {
			this.timeout(90_000);

			const result = await runFanoutTreeSim({
				nodes: 40,
				bootstraps: 1,
				subscribers: 30,
				relayFraction: 0.35,
					messages: 40,
					msgRate: 50,
					msgSize: 64,
					// Under monorepo CI load, repairs can be delayed; give the sim a bit more
					// time to converge after publishing.
					settleMs: 5_000,
					// CI can be noisy; use a slightly looser deadline while still enforcing low-latency delivery.
					deadlineMs: 500,
					timeoutMs: 40_000,
					seed: 1,
					repair: true,
			rootUploadLimitBps: 100_000_000,
			relayUploadLimitBps: 100_000_000,
			rootMaxChildren: 64,
			relayMaxChildren: 32,
			neighborRepair: true,
			neighborRepairPeers: 3,
			dropDataFrameRate: 0.1,
			churnEveryMs: 200,
			churnDownMs: 100,
			churnFraction: 0.05,
		});

		if (
			result.joinedPct < 99 ||
			result.deliveredPct < 95 ||
			result.deliveredWithinDeadlinePct < 90 ||
			result.overheadFactorData > 3.5 ||
			result.latencyP95 > 1_500 ||
			result.maintMaxOrphans > 10 ||
			result.maintOrphanArea > 30 ||
			result.maintRecoveryP95Ms > 6_000 ||
			result.maintReparentsPerMin > 500 ||
			result.attachP95 > 10_000 ||
			result.treeLevelP95 > 8 ||
			result.formationScore > 30 ||
			result.formationTreeOrphans > 0
		) {
			// Helpful for CI debug
			console.log(formatFanoutTreeSimResult(result));
		}

		expect(result.joinedPct).to.be.greaterThan(99);
		expect(result.deliveredPct).to.be.greaterThan(95);
		expect(result.deliveredWithinDeadlinePct).to.be.greaterThan(90);
		expect(result.overheadFactorData).to.be.lessThan(3.5);
		expect(result.latencyP95).to.be.lessThan(1_500);
		expect(result.attachSamples).to.equal(result.joinedCount);
		expect(result.attachP95).to.be.lessThan(10_000);
		expect(result.treeLevelP95).to.be.lessThan(8);
		expect(result.formationTreeOrphans).to.equal(0);
		expect(result.maintSamples).to.be.greaterThan(0);
		expect(result.maintMaxOrphans).to.be.lessThan(10);
		expect(result.maintOrphanArea).to.be.lessThan(30);
		expect(result.maintRecoveryCount).to.equal(result.churnEvents);
		expect(result.maintRecoveryP95Ms).to.be.lessThan(6_000);
		expect(result.maintReparentsPerMin).to.be.lessThan(500);
		expect(Number.isFinite(result.formationStretchP95)).to.equal(true);
		expect(Number.isFinite(result.formationUnderlayDistP95)).to.equal(true);
		expect(result.formationUnderlayEdges).to.be.greaterThan(0);
		expect(result.formationScore).to.be.lessThan(30);
		expect(result.protocolFetchReqSent).to.be.lessThan(2_000);
		expect(result.protocolIHaveSent).to.be.lessThan(4_000);
		expect(result.protocolControlBytesSent).to.be.lessThan(300_000);
		expect(result.protocolRepairReqSent).to.be.lessThan(10_000);
		expect(result.trackerBpp).to.be.lessThan(5);
		expect(result.repairBpp).to.be.lessThan(5);
	});
});
