import {
	resolveFanoutTreeSimParams,
	runFanoutTreeSim,
} from "../benchmark/fanout-tree-sim-lib.js";

const main = async () => {
	const raw = process.argv[2];
	if (!raw) {
		throw new Error("Missing FanoutTreeSim params JSON");
	}

	const params = resolveFanoutTreeSimParams(JSON.parse(raw));
	const result = await runFanoutTreeSim(params);
	process.stdout.write(JSON.stringify(result));
};

try {
	await main();
} catch (error: any) {
	console.error(error?.stack ?? error?.message ?? String(error));
	process.exit(1);
}
