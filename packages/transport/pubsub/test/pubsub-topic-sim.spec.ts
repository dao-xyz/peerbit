import { expect } from "chai";
import {
	formatPubsubTopicSimResult,
	runPubsubTopicSim,
} from "../benchmark/pubsub-topic-sim-lib.js";

describe("pubsub-topic-sim (ci)", () => {
	it("delivers on a small sim", async function () {
		this.timeout(60_000);

			const result = await runPubsubTopicSim({
			nodes: 20,
			degree: 4,
			writerIndex: 0,
			subscribers: 15,
			messages: 10,
			msgSize: 32,
			intervalMs: 0,
			silent: true,
			redundancy: 2,
			seed: 1,
			topic: "concert",
			subscribeModel: "preseed",
				warmupMessages: 2,
				settleMs: 750,
				// CI can be heavily loaded (especially in monorepo runs); give the sim more time
				// to converge before treating it as a failure.
				timeoutMs: 40_000,
			});

		if (result.deliveredPct < 99.9 || result.publishErrors > 0) {
			console.log(formatPubsubTopicSimResult(result));
		}

		expect(result.deliveredPct).to.be.greaterThan(99.9);
		expect(result.deliveredOnlinePct).to.be.greaterThan(99.9);
		expect(result.publishErrors).to.equal(0);
		// Sharded fanout publish does not embed explicit `to=[subscribers]` lists.
		// This keeps per-message overhead independent of subscriber count.
		expect(result.modeToLenMax).to.equal(0);
	});

	it("remains mostly connected under mild churn", async function () {
		this.timeout(90_000);

		const result = await runPubsubTopicSim({
			nodes: 25,
			degree: 6,
			writerIndex: 0,
			subscribers: 18,
			messages: 10,
			msgSize: 64,
			intervalMs: 10,
			silent: true,
			redundancy: 2,
			seed: 1,
			topic: "concert",
			subscribeModel: "preseed",
			warmupMessages: 2,
			settleMs: 1_000,
			timeoutMs: 60_000,
			churnEveryMs: 50,
			churnDownMs: 30,
			churnFraction: 0.05,
		});

		if (result.deliveredOnlinePct < 90 || result.publishErrors > 5) {
			console.log(formatPubsubTopicSimResult(result));
		}

		expect(result.deliveredOnlinePct).to.be.greaterThan(90);
		expect(result.publishErrors).to.be.lessThan(10);
		expect(result.churnEvents).to.be.greaterThan(0);
	});
});
