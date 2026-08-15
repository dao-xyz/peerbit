import { parseArgs } from "node:util";

export const SCALE_CENSUS_NAME = "shared-log-resident-scale-census";

export const SCALE_CENSUS_SCENARIOS = Object.freeze([
	"native-graph-chain",
	"native-graph-roots",
	"coordinate-frontier",
]);

const parsePositiveInteger = (value, label) => {
	const normalized = String(value).replaceAll("_", "");
	if (!/^[1-9][0-9]*$/.test(normalized)) {
		throw new Error(`${label} must be a positive integer, got '${value}'`);
	}
	const parsed = Number(normalized);
	if (!Number.isSafeInteger(parsed)) {
		throw new Error(`${label} exceeds JavaScript's safe integer range`);
	}
	return parsed;
};

const parseUniqueList = (value, label, parseValue) => {
	const values = String(value)
		.split(",")
		.map((part) => part.trim())
		.filter(Boolean)
		.map(parseValue);
	if (values.length === 0) {
		throw new Error(`${label} must contain at least one value`);
	}
	if (new Set(values).size !== values.length) {
		throw new Error(`${label} must not contain duplicate values`);
	}
	return values;
};

const parseCounts = (value) =>
	parseUniqueList(value, "counts", (part) =>
		parsePositiveInteger(part, "count"),
	);

const parseScenarios = (value) =>
	parseUniqueList(value, "scenarios", (scenario) => {
		if (!SCALE_CENSUS_SCENARIOS.includes(scenario)) {
			throw new Error(`Unknown scale-census scenario '${scenario}'`);
		}
		return scenario;
	});

export const parseScaleCensusArgs = (args, env = {}) => {
	const normalizedArgs = args[0] === "--" ? args.slice(1) : args;
	const { values } = parseArgs({
		args: normalizedArgs,
		strict: true,
		allowPositionals: false,
		options: {
			counts: { type: "string" },
			scenarios: { type: "string" },
			runs: { type: "string" },
			json: { type: "boolean" },
			help: { type: "boolean" },
			worker: { type: "boolean" },
			scenario: { type: "string" },
			count: { type: "string" },
			run: { type: "string" },
		},
	});

	if (values.help) {
		return { mode: "help" };
	}

	if (values.worker) {
		if (!values.scenario || !values.count || !values.run) {
			throw new Error("worker mode requires --scenario, --count, and --run");
		}
		const [scenario] = parseScenarios(values.scenario);
		if (values.scenario.includes(",")) {
			throw new Error("worker mode accepts exactly one scenario");
		}
		return {
			mode: "worker",
			scenario,
			count: parsePositiveInteger(values.count, "count"),
			run: parsePositiveInteger(values.run, "run"),
		};
	}

	if (values.scenario || values.count || values.run) {
		throw new Error("--scenario, --count, and --run are worker-only options");
	}

	return {
		mode: "parent",
		counts: parseCounts(
			values.counts ?? env.SHARED_LOG_SCALE_COUNTS ?? "100000,1000000",
		),
		scenarios: parseScenarios(
			values.scenarios ??
				env.SHARED_LOG_SCALE_SCENARIOS ??
				SCALE_CENSUS_SCENARIOS.join(","),
		),
		runs: parsePositiveInteger(
			values.runs ?? env.SHARED_LOG_SCALE_RUNS ?? "1",
			"runs",
		),
		json: values.json === true || env.BENCH_JSON === "1",
	};
};

const MEMORY_FIELDS = [
	"rss",
	"heapTotal",
	"heapUsed",
	"external",
	"arrayBuffers",
];

export const memoryDeltas = (before, after, count) => {
	const result = {};
	for (const field of MEMORY_FIELDS) {
		const delta = after[field] - before[field];
		result[`${field}BeforeBytes`] = before[field];
		result[`${field}AfterBytes`] = after[field];
		result[`${field}DeltaBytes`] = delta;
		result[`${field}BytesPerEntry`] = Math.round(delta / count);
	}
	return result;
};

export const buildScaleCensusReport = ({
	counts,
	scenarios,
	runs,
	rows,
	host,
}) => ({
	name: SCALE_CENSUS_NAME,
	schemaVersion: 1,
	meta: {
		counts,
		scenarios,
		runs,
		isolation: "one fresh process per row",
		measurement:
			"resident state added after WASM and an empty index are loaded",
		...host,
	},
	rows,
});
