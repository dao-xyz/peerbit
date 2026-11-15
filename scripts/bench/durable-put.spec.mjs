import assert from "node:assert/strict";
import test from "node:test";
import { validateRun } from "./durable-put.mjs";

const fixture = () => {
	const source = {
		head: "a".repeat(40),
		tree: "b".repeat(40),
		lockSha256: "c".repeat(64),
		programSha256: "d".repeat(64),
		engineSha256: "e".repeat(64),
		schemaSha256: "f".repeat(64),
	};
	const provenance = {
		cohorts: { A: source, B: { ...source, head: "b".repeat(40) } },
		harnessSha256: "a".repeat(64),
	};
	const rows = [
		{
			event: "environment",
			head: source.head,
			programSha256: source.programSha256,
			platform: "linux",
			arch: "x64",
			gc: true,
			configuration: {
				peers: 1,
				replicate: { factor: 1 },
				replicas: "default (min 2)",
				target: "default",
				durability: "default (SQLite FULL / Level sync)",
				canPerform: "JavaScript allow-all callback",
				resolverCache: "default",
				unique: true,
			},
		},
	];
	for (const [bytes, concurrency] of [
		[4096, 1],
		[524288, 1],
		[262144, 4],
		[524288, 4],
	]) {
		const samples = concurrency === 1 ? 60 : 30;
		const warmup = concurrency === 1 ? 5 : 2;
		const wallMs = Array.from(
			{ length: samples },
			(_, index) => samples - index,
		);
		rows.push({
			event: "start",
			directory: `/tmp/fixture-${bytes}-${concurrency}`,
			bytes,
			concurrency,
		});
		rows.push({
			event: "result",
			bytes,
			concurrency,
			samples,
			warmup,
			wallMs,
			cpuMs: wallMs,
			p50Ms: Math.ceil(samples * 0.5),
			p95Ms: Math.ceil(samples * 0.95),
			offlineVerifiedEntries: (samples + warmup) * concurrency,
		});
	}
	return { rows, provenance };
};
const encode = (rows) =>
	rows.map((row) => JSON.stringify(row)).join("\n") + "\n";

test("validates separate workload results for either cohort without performance claims", () => {
	const { rows, provenance } = fixture();
	const summary = validateRun(encode(rows), provenance, "A");
	assert.equal(summary.results.length, 4);
	assert.deepEqual(
		summary.results.map((row) => row.p50Ms),
		[30, 30, 15, 15],
	);
	assert.equal("passed" in summary, false);
	rows[0].head = provenance.cohorts.B.head;
	assert.equal(
		validateRun(encode(rows), provenance, "B").head,
		provenance.cohorts.B.head,
	);
});

for (const [path, replacement] of [
	["rows.0.head", "b".repeat(40)],
	["rows.0.programSha256", "a".repeat(64)],
	["provenance.cohorts.B.engineSha256", undefined],
	["provenance.harnessSha256", "bad"],
	["rows.0.configuration.target", "none"],
	["rows.0.configuration.native", true],
	["rows.0.platform", "darwin"],
	["rows.0.arch", "arm64"],
	["rows.0.gc", false],
	["rows.1.event", "result"],
	["rows.3.bytes", 4096],
	["rows.4.concurrency", 4],
	["rows.2.wallMs", [1]],
	["rows.2.cpuMs.0", -1],
	["rows.2.wallMs.0", Infinity],
	["rows.2.p50Ms", 1],
	["rows.2.p95Ms", 1],
	["rows.2.samples", 1],
	["rows.2.warmup", 1],
	["rows.2.offlineVerifiedEntries", 1],
]) {
	test(`rejects corrupted ${path}`, () => {
		const value = fixture();
		const keys = path.split(".");
		const last = keys.pop();
		const parent = keys.reduce((object, key) => object[key], value);
		parent[last] = replacement;
		assert.throws(() => validateRun(encode(value.rows), value.provenance, "A"));
	});
}

test("rejects additional output and invalid cohort", () => {
	const { rows, provenance } = fixture();
	for (const raw of [
		"",
		encode(rows) + "log message\n",
		encode(rows) + "{}\n",
		"\n" + encode(rows),
		encode(rows).replace(/^[^\n]+/, "not JSON"),
	]) {
		assert.throws(() => validateRun(raw, provenance, "A"));
	}
	assert.throws(() => validateRun(encode(rows.slice(0, -1)), provenance, "A"));
	assert.throws(() => validateRun(encode(rows), provenance, "C"));
});
