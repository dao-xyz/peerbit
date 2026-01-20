import { Bench } from "tinybench";
import { PIDReplicationController } from "../src/pid.js";

// Benchmarks PID convergence speed in a simplified simulation model.
//
// Run with:
//   cd packages/programs/data/shared-log
//   PID_WARMUP=2 PID_ITERATIONS=50 PID_RUNS=200 node --loader ts-node/esm ./benchmark/pid-convergence.ts
//
// Notes:
// - This is a *model* benchmark: it does not simulate network, pruning, or real memory measurements.
// - It is intended to catch regressions/improvements in the controller dynamics and step cost.

const parseNumberList = (value: string | undefined, defaults: number[]) => {
	if (!value) return defaults;
	const parsed = value
		.split(",")
		.map((x) => Number.parseInt(x.trim(), 10))
		.filter((x) => Number.isFinite(x) && x > 0);
	return parsed.length > 0 ? parsed : defaults;
};

const warmupIterations = Number.parseInt(process.env.PID_WARMUP || "2", 10);
const iterations = Number.parseInt(process.env.PID_ITERATIONS || "50", 10);
const runsPerTask = Number.parseInt(process.env.PID_RUNS || "200", 10);
const maxSteps = Number.parseInt(process.env.PID_MAX_STEPS || "200", 10);
const peersList = parseNumberList(process.env.PID_PEERS, [2, 3, 5]);

const clamp01 = (value: number) => Math.max(0, Math.min(1, value));

const simulateConvergence = (properties: {
	controllers: PIDReplicationController[];
	initialFactors: number[];
	cpuUsage: (peerIndex: number) => number | undefined;
	memoryUsage: (peerIndex: number, factor: number) => number;
	expectedFactor: (peerIndex: number, peerCount: number) => number;
	epsilon: number;
}) => {
	const {
		controllers,
		initialFactors,
		cpuUsage,
		memoryUsage,
		expectedFactor,
		epsilon,
	} = properties;

	for (const controller of controllers) {
		controller.reset();
	}

	let factors = initialFactors.slice();
	const peerCount = factors.length;

	for (let step = 0; step < maxSteps; step++) {
		const totalFactor = clamp01(factors.reduce((acc, f) => acc + f, 0));
		const next: number[] = [];
		for (let peerIndex = 0; peerIndex < peerCount; peerIndex++) {
			next[peerIndex] = controllers[peerIndex].step({
				memoryUsage: memoryUsage(peerIndex, factors[peerIndex]),
				currentFactor: factors[peerIndex],
				totalFactor,
				peerCount,
				cpuUsage: cpuUsage(peerIndex),
			});
		}
		factors = next;

		let ok = true;
		for (let peerIndex = 0; peerIndex < peerCount; peerIndex++) {
			const target = expectedFactor(peerIndex, peerCount);
			if (Math.abs(factors[peerIndex] - target) > epsilon) {
				ok = false;
				break;
			}
		}
		if (ok && Math.abs(factors.reduce((acc, f) => acc + f, 0) - 1) <= epsilon) {
			return { converged: true, steps: step + 1 };
		}
	}

	return { converged: false, steps: maxSteps };
};

const suite = new Bench({
	name: "pid-convergence",
	warmupIterations: Number.isFinite(warmupIterations) ? warmupIterations : 0,
	iterations: Number.isFinite(iterations) ? iterations : undefined,
});

const makeControllers = (
	peerCount: number,
	options: (peerIndex: number) => ConstructorParameters<typeof PIDReplicationController>[1],
) =>
	Array.from({ length: peerCount }, (_, i) => new PIDReplicationController(`p${i}`, options(i)));

for (const peerCount of peersList) {
	const controllers = makeControllers(peerCount, () => ({}));
	const epsilon = 0.02;
	const target = (_i: number, n: number) => 1 / n;
	const initialFactors = Array.from({ length: peerCount }, () => 0);

	suite.add(`even (peers=${peerCount})`, () => {
		for (let i = 0; i < runsPerTask; i++) {
			const res = simulateConvergence({
				controllers,
				initialFactors,
				cpuUsage: () => 0,
				memoryUsage: () => 0,
				expectedFactor: target,
				epsilon,
			});
			if (!res.converged) {
				throw new Error(`did not converge (peers=${peerCount})`);
			}
		}
	});
}

// A constrained scenario (2 peers): peer 0 has a memory cap (ratio), peer 1 is unconstrained.
{
	const peerCount = 2;
	const totalSize = 1_000_000; // bytes
	const memoryLimit = 250_000; // 25% of total
	const limitRatio = memoryLimit / totalSize;
	const epsilon = 0.03;

	const controllers = makeControllers(peerCount, (peerIndex) =>
		peerIndex === 0 ? { storage: { max: memoryLimit } } : {},
	);

	const initialFactors = [0.5, 0.5];
	const expectedFactor = (peerIndex: number) =>
		peerIndex === 0 ? limitRatio : 1 - limitRatio;

	suite.add("memory-limited (2 peers, 25% cap)", () => {
		for (let i = 0; i < runsPerTask; i++) {
			const res = simulateConvergence({
				controllers,
				initialFactors,
				cpuUsage: () => 0,
				memoryUsage: (peerIndex, factor) =>
					peerIndex === 0 ? totalSize * factor : 0,
				expectedFactor,
				epsilon,
			});
			if (!res.converged) {
				throw new Error("did not converge (memory-limited)");
			}
		}
	});
}

await suite.run();

if (process.env.BENCH_JSON === "1") {
	const tasks = suite.tasks.map((task) => ({
		name: task.name,
		hz: task.result?.hz ?? null,
		mean_ms: task.result?.mean ?? null,
		rme: task.result?.rme ?? null,
		samples: task.result?.samples?.length ?? null,
	}));
	process.stdout.write(
		JSON.stringify(
			{
				name: suite.name,
				tasks,
				meta: {
					peersList,
					warmupIterations,
					iterations,
					runsPerTask,
					maxSteps,
				},
			},
			null,
			2,
		),
	);
} else {
	console.table(suite.table());
}

