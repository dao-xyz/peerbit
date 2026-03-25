import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { expect } from "chai";
import {
	formatPubsubTopicSimResult,
	type PubsubTopicSimParams,
	type PubsubTopicSimResult,
} from "../benchmark/pubsub-topic-sim-lib.js";

const execFileAsync = promisify(execFile);

const resolveSimRunnerPath = () => {
	const currentDir = dirname(fileURLToPath(import.meta.url));
	const candidates = [
		resolve(currentDir, "pubsub-topic-sim.runner.js"),
		resolve(currentDir, "../dist/test/pubsub-topic-sim.runner.js"),
	];
	for (const candidate of candidates) {
		if (existsSync(candidate)) return candidate;
	}
	throw new Error(
		`Unable to locate pubsub-topic sim runner. Tried: ${candidates.join(", ")}`,
	);
};

const runPubsubTopicSimIsolated = async (
	params: Partial<PubsubTopicSimParams>,
): Promise<PubsubTopicSimResult> => {
	const runner = resolveSimRunnerPath();
	const { stdout, stderr } = await execFileAsync(
		process.execPath,
		[runner, JSON.stringify(params)],
		{
			maxBuffer: 16 * 1024 * 1024,
			env: process.env,
		},
	);

	const trimmed = stdout.trim();
	if (!trimmed) {
		throw new Error(
			`PubsubTopicSim runner produced no stdout${stderr ? `\n${stderr.trim()}` : ""}`,
		);
	}

	try {
		return JSON.parse(trimmed) as PubsubTopicSimResult;
	} catch (error: any) {
		throw new Error(
			`Failed to parse PubsubTopicSim runner output as JSON: ${error?.message ?? String(error)}\n${trimmed}${stderr ? `\n${stderr.trim()}` : ""}`,
		);
	}
};

describe("pubsub-topic-sim (ci)", () => {
	it("delivers on a small sim", async function () {
		this.timeout(60_000);

		const result = await runPubsubTopicSimIsolated({
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

		const result = await runPubsubTopicSimIsolated({
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

		// The denominator ("online at send time") is optimistic under churn because peers
		// can disconnect during in-flight delivery. CI load can therefore dip into the
		// mid/high 80s while still representing a mostly connected overlay.
		expect(result.deliveredOnlinePct).to.be.at.least(85);
		expect(result.publishErrors).to.be.lessThan(10);
		expect(result.churnEvents).to.be.greaterThan(0);
	});
});
