import { readFile } from "node:fs/promises";
import { compareSyncPeerStateResults } from "./sync-peer-state-compare-lib.mjs";

const [baselinePath, candidatePath] = process.argv.slice(2);
if (!baselinePath || !candidatePath) {
	process.stderr.write(
		"Usage: node benchmark/sync-peer-state-compare.mjs <baseline.json> <candidate.json>\n",
	);
	process.exitCode = 2;
} else {
	try {
		const [baseline, candidate] = await Promise.all([
			readFile(baselinePath, "utf8").then(JSON.parse),
			readFile(candidatePath, "utf8").then(JSON.parse),
		]);
		const result = compareSyncPeerStateResults(baseline, candidate);
		process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
		if (result.status === "regression") {
			process.exitCode = 1;
		} else if (result.status === "inconclusive") {
			process.exitCode = 2;
		}
	} catch (error) {
		process.stderr.write(`${error instanceof Error ? error.message : error}\n`);
		process.exitCode = 2;
	}
}
