import { createWriteStream } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { spawn } from "node:child_process";

type RunSpec = {
	name: string;
	log: string;
	args: string[];
	strict: boolean;
	kind: "single" | "multi" | "frontier";
};

type RunResult = {
	name: string;
	log: string;
	exitCode: number;
};

const usage = () => {
	console.log(
		[
			"fanout-tree-parent-upgrade-prepush.ts",
			"",
			"Runs the bounded local evidence suite for guarded fanout parent upgrades.",
			"",
			"Options:",
			"  --outDir DIR      output directory for logs/summaries (default: sim-results/parent-upgrade-prepush-<timestamp>)",
			"  --frontier 0|1    include non-gating large-idle cap frontier (default: 1)",
			"  --quick 0|1       run seed 1 only and skip frontier (default: 0)",
			"  --help            show this message",
			"",
			"Example:",
			"  pnpm -C packages/transport/pubsub run bench -- fanout-tree-parent-upgrade-prepush",
		].join("\n"),
	);
};

const getArg = (name: string) => {
	const i = process.argv.indexOf(name);
	return i >= 0 ? process.argv[i + 1] : undefined;
};

const boolArg = (name: string, fallback: boolean) => {
	const value = getArg(name);
	if (value == null) return fallback;
	return value !== "0" && value !== "false";
};

const timestamp = () =>
	new Date().toISOString().replace(/[:.]/g, "-").replace("T", "_");

const safeName = (value: string) => value.replace(/[^a-zA-Z0-9_.-]/g, "-");

const baseArgs = [
	"--parentUpgradePreset",
	"default-candidate",
	"--strict",
	"1",
];

const singleEval = (...args: string[]) => [
	"fanout-tree-parent-upgrade-eval",
	...args,
	...baseArgs,
];

const multiEval = (...args: string[]) => [
	"fanout-tree-parent-upgrade-multi-eval",
	...args,
	...baseArgs,
];

const scenarioSeeds = (quick: boolean) => (quick ? "1" : "1,2,3");

const makeRuns = (quick: boolean, includeFrontier: boolean): RunSpec[] => {
	const seeds = scenarioSeeds(quick);
	const runs: RunSpec[] = [
		{
			name: "single-all",
			log: "single-all.txt",
			args: singleEval("--scenario", "all", "--seeds", seeds),
			strict: true,
			kind: "single",
		},
		{
			name: "single-live-stream",
			log: "single-live-stream.txt",
			args: singleEval("--scenario", "ci-live-stream", "--seeds", seeds),
			strict: true,
			kind: "single",
		},
		{
			name: "single-idle-large-pressure",
			log: "single-idle-large-pressure.txt",
			args: singleEval(
				"--scenario",
				"ci-idle-upgrade-large",
				"--seeds",
				seeds,
				"--parentUpgradeRootMaxChildLoadRatio",
				"0.25",
				"--maxRootChildrenDelta",
				"3",
				"--maxCostRatio",
				"1.2",
			),
			strict: true,
			kind: "single",
		},
		{
			name: "multi-all",
			log: "multi-all.txt",
			args: multiEval("--scenario", "all", "--seeds", seeds),
			strict: true,
			kind: "multi",
		},
		{
			name: "multi-scale-live",
			log: "multi-scale-live.txt",
			args: multiEval(
				"--scenario",
				"ci-multi-live",
				"--seeds",
				"1",
				"--nodes",
				"80",
				"--writers",
				"8",
				"--activeWriters",
				"8",
				"--subscribersPerTree",
				"56",
			),
			strict: true,
			kind: "multi",
		},
		{
			name: "multi-scale-idle",
			log: "multi-scale-idle.txt",
			args: multiEval(
				"--scenario",
				"ci-multi-idle",
				"--seeds",
				"1",
				"--nodes",
				"80",
				"--writers",
				"8",
				"--activeWriters",
				"8",
				"--subscribersPerTree",
				"56",
			),
			strict: false,
			kind: "multi",
		},
	];

	if (!quick && includeFrontier) {
		for (const cap of ["0.2", "0.225", "0.25", "0.4"]) {
			runs.push({
				name: `single-idle-large-frontier-${cap}`,
				log: `single-idle-large-frontier-${safeName(cap)}.txt`,
				args: [
					"fanout-tree-parent-upgrade-eval",
					"--scenario",
					"ci-idle-upgrade-large",
					"--seeds",
					"1,2,3",
					"--parentUpgradePreset",
					"default-candidate",
					"--parentUpgradeRootMaxChildLoadRatio",
					cap,
					"--strict",
					"0",
				],
				strict: false,
				kind: "frontier",
			});
		}
	}

	return runs;
};

const runOne = async (spec: RunSpec, outDir: string): Promise<RunResult> => {
	const logPath = resolve(outDir, spec.log);
	const log = createWriteStream(logPath);
	const header = `\n=== ${spec.name} ===\nnode ${[
		"--loader",
		"ts-node/esm",
		"./benchmark/index.ts",
		"--",
		...spec.args,
	].join(" ")}\n\n`;

	process.stdout.write(header);
	log.write(header);

	const child = spawn(
		process.execPath,
		["--loader", "ts-node/esm", "./benchmark/index.ts", "--", ...spec.args],
		{
			cwd: process.cwd(),
			env: process.env,
			stdio: ["ignore", "pipe", "pipe"],
		},
	);

	child.stdout.pipe(process.stdout, { end: false });
	child.stderr.pipe(process.stderr, { end: false });
	child.stdout.pipe(log, { end: false });
	child.stderr.pipe(log, { end: false });

	const exitCode = await new Promise<number>((resolveExit) => {
		child.on("close", (code) => resolveExit(code ?? 1));
		child.on("error", () => resolveExit(1));
	});

	await new Promise<void>((resolveClose) => log.end(resolveClose));
	return { name: spec.name, log: basename(logPath), exitCode };
};

const parseSingleSummary = (text: string) => {
	const lines = text.split(/\r?\n/);
	const rows: string[] = [];
	for (const line of lines) {
		if (!line.startsWith("ci-")) continue;
		const cols = line.trim().split(/\s+/);
		if (cols.length < 19) continue;
		rows.push(
			[
				cols[0],
				cols[1],
				cols[2],
				cols[3],
				cols[4],
				cols[5],
				cols[6],
				cols[7],
				cols[8],
				cols[9],
				cols[15],
				cols[16],
				cols[17],
				cols[18],
			].join("\t"),
		);
	}
	return rows;
};

const parseMultiSummary = (text: string) => {
	const lines = text.split(/\r?\n/);
	const rows: string[] = [];
	for (const line of lines) {
		if (!line.startsWith("ci-multi-")) continue;
		const cols = line.trim().split(/\s+/);
		if (cols.length < 16) continue;
		rows.push(
			[
				cols[0],
				cols[1],
				cols[2],
				cols[3],
				cols[4],
				cols[5],
				cols[6],
				cols[7],
				cols[8],
				cols[10],
				cols[11],
				cols[12],
				cols[13],
				cols[14],
				cols[15],
			].join("\t"),
		);
	}
	return rows;
};

const writeSummaries = async (
	outDir: string,
	runs: RunSpec[],
	results: RunResult[],
) => {
	const fs = await import("node:fs/promises");
	const manifest = results.map((result) => ({
		...result,
		args: runs.find((run) => run.name === result.name)?.args ?? [],
		strict: runs.find((run) => run.name === result.name)?.strict ?? false,
	}));

	await writeFile(
		resolve(outDir, "manifest.json"),
		JSON.stringify({ generatedAt: new Date().toISOString(), manifest }, null, 2),
	);

	const singleRows: string[] = [
		"scenario\tmode\tseeds\tviable\teffects\tupgrades\tprobes\tactiveUpgrades\tactiveProbes\tactiveGuardSkips\trootChildrenDeltaMax\trootUploadPctDeltaMax\tmaxReparents\tfailures",
	];
	const multiRows: string[] = [
		"scenario\tseeds\tviable\tusefulPromotedTrees\tupgrades\tprobes\tactiveUpgrades\tactiveProbes\tactiveGuardSkips\tcontrolBppDeltaPctAvg\trootChildrenDeltaMax\trootChildrenDeltaSumMax\trootUploadPctDeltaMax\tmaxPerPeer\tfailures",
	];
	const frontierRows: string[] = [
		"cap\tscenario\tmode\tseeds\tviable\teffects\tupgrades\tprobes\trootChildrenDeltaMax\trootUploadPctDeltaMax\tfailures",
	];

	for (const run of runs) {
		const text = await fs.readFile(resolve(outDir, run.log), "utf8");
		if (run.kind === "single") {
			singleRows.push(...parseSingleSummary(text));
		} else if (run.kind === "multi") {
			multiRows.push(...parseMultiSummary(text));
		} else {
			const cap = run.args[run.args.indexOf("--parentUpgradeRootMaxChildLoadRatio") + 1];
			for (const row of parseSingleSummary(text)) {
				const cols = row.split("\t");
				frontierRows.push(
					[
						cap,
						cols[0],
						cols[1],
						cols[2],
						cols[3],
						cols[4],
						cols[5],
						cols[6],
						cols[10],
						cols[11],
						cols[13],
					].join("\t"),
				);
			}
		}
	}

	await writeFile(resolve(outDir, "single-summary.tsv"), singleRows.join("\n") + "\n");
	await writeFile(resolve(outDir, "multi-summary.tsv"), multiRows.join("\n") + "\n");
	await writeFile(
		resolve(outDir, "frontier-summary.tsv"),
		frontierRows.join("\n") + "\n",
	);
};

const main = async () => {
	if (process.argv.includes("--help") || process.argv.includes("-h")) {
		usage();
		return;
	}

	const quick = boolArg("--quick", false);
	const includeFrontier = quick ? false : boolArg("--frontier", true);
	const outDir = resolve(
		getArg("--outDir") ?? `sim-results/parent-upgrade-prepush-${timestamp()}`,
	);
	await mkdir(outDir, { recursive: true });

	const runs = makeRuns(quick, includeFrontier);
	const results: RunResult[] = [];
	let failed = false;

	for (const run of runs) {
		const result = await runOne(run, outDir);
		results.push(result);
		if (run.strict && result.exitCode !== 0) {
			failed = true;
		}
	}

	await writeSummaries(outDir, runs, results);

	console.log(`\nparent-upgrade-prepush outDir=${outDir}`);
	for (const result of results) {
		const run = runs.find((candidate) => candidate.name === result.name);
		const status =
			result.exitCode === 0
				? "PASS"
				: run?.strict === false
					? "NON-GATING FAIL"
					: "FAIL";
		console.log(`${status} ${result.name}`);
	}

	if (failed) {
		process.exit(1);
	}
};

await main();
