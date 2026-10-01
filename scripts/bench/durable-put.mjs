import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const configuration = {
	peers: 1,
	replicate: { factor: 1 },
	replicas: "default (min 2)",
	target: "default",
	durability: "default (SQLite FULL / Level sync)",
	canPerform: "JavaScript allow-all callback",
	resolverCache: "default",
	unique: true,
};
const workloads = [
	[4096, 1],
	[524288, 1],
	[262144, 4],
	[524288, 4],
];
const percentile = (values, fraction) =>
	[...values].sort((a, b) => a - b)[Math.ceil(values.length * fraction) - 1];
const sha = (value, length) =>
	assert.match(value, new RegExp(`^[0-9a-f]{${length}}$`));

// File validation cannot prove process exit; invoke only after a successful run.
export function validateRun(raw, provenance, cohort) {
	assert(cohort === "A" || cohort === "B", "cohort must be A or B");
	for (const name of ["A", "B"]) {
		const source = provenance.cohorts?.[name];
		for (const key of ["head", "tree"]) sha(source?.[key], 40);
		for (const key of ["lock", "program", "engine", "schema"])
			sha(source?.[`${key}Sha256`], 64);
	}
	sha(provenance.harnessSha256, 64);
	const lines = raw.replace(/\r?\n$/, "").split(/\r?\n/);
	assert.equal(lines.length, 9, "expected nine events");
	const rows = lines.map((line) => JSON.parse(line));
	const environment = rows[0];
	assert.equal(environment.event, "environment");
	const source = provenance.cohorts[cohort];
	assert.equal(environment.head, source.head, "environment head mismatch");
	assert.equal(environment.programSha256, source.programSha256);
	assert.equal(environment.platform, "linux");
	assert.equal(environment.arch, "x64");
	assert.equal(environment.gc, true);
	assert.deepEqual(environment.configuration, configuration);
	const results = workloads.map(([bytes, concurrency], index) => {
		const start = rows[1 + index * 2];
		const result = rows[2 + index * 2];
		assert.equal(start.event, "start");
		assert.equal(result.event, "result");
		for (const row of [start, result]) {
			assert.equal(row.bytes, bytes, "workload bytes mismatch");
			assert.equal(row.concurrency, concurrency);
		}
		assert.equal(typeof start.directory, "string");
		assert(start.directory.length > 0);
		const samples = concurrency === 1 ? 60 : 30;
		const warmup = concurrency === 1 ? 5 : 2;
		assert.equal(result.samples, samples);
		assert.equal(result.warmup, warmup);
		for (const key of ["wallMs", "cpuMs"]) {
			const values = result[key];
			assert(Array.isArray(values), `${key} must be an array`);
			assert.equal(values.length, samples, `${key} sample count mismatch`);
			assert(values.every((value) => Number.isFinite(value) && value >= 0));
		}
		assert.equal(result.p50Ms, percentile(result.wallMs, 0.5), "p50 mismatch");
		assert.equal(result.p95Ms, percentile(result.wallMs, 0.95), "p95 mismatch");
		assert.equal(
			result.offlineVerifiedEntries,
			(samples + warmup) * concurrency,
		);
		const { p50Ms, p95Ms, offlineVerifiedEntries } = result;
		return {
			bytes,
			concurrency,
			samples,
			warmup,
			p50Ms,
			p95Ms,
			offlineVerifiedEntries,
		};
	});
	return {
		cohort,
		...source,
		harnessSha256: provenance.harnessSha256,
		results,
	};
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	try {
		assert.equal(
			process.argv.length,
			5,
			"expected <raw.jsonl> <provenance.json> <A|B>",
		);
		const [, , rawPath, provenancePath, cohort] = process.argv;
		const raw = readFileSync(rawPath, "utf8");
		const provenance = JSON.parse(readFileSync(provenancePath, "utf8"));
		console.log(JSON.stringify(validateRun(raw, provenance, cohort)));
	} catch (error) {
		console.error(error.message);
		process.exitCode = 1;
	}
}
