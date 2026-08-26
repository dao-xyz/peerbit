import { parseArgs } from "node:util";

export const SCALE_CENSUS_NAME = "shared-log-scale-census";

export const SCALE_CENSUS_RESIDENT_SCENARIOS = Object.freeze([
	"native-graph-chain",
	"native-graph-roots",
	"coordinate-frontier",
]);

export const SCALE_CENSUS_PERSISTENT_SCENARIOS = Object.freeze([
	"persistent-head-index-graph-chain",
	"persistent-head-index-graph-roots",
	"persistent-coordinate-index",
]);

export const SCALE_CENSUS_SCENARIOS = Object.freeze([
	...SCALE_CENSUS_RESIDENT_SCENARIOS,
	...SCALE_CENSUS_PERSISTENT_SCENARIOS,
]);

export const isPersistentScaleCensusScenario = (scenario) =>
	SCALE_CENSUS_PERSISTENT_SCENARIOS.includes(scenario);

export const SCALE_CENSUS_HASH_CHARACTERS = 59;
export const SCALE_CENSUS_GID_CHARACTERS = 44;

const HASH_PREFIX = "bafybeigdyrzt";
const HASH_SUFFIX_LENGTH = SCALE_CENSUS_HASH_CHARACTERS - HASH_PREFIX.length;
const GID_PREFIX = "gid-";
const GID_SUFFIX_LENGTH = SCALE_CENSUS_GID_CHARACTERS - GID_PREFIX.length;

export const makeScaleCensusHash = (index) =>
	`${HASH_PREFIX}${index.toString(36).padStart(HASH_SUFFIX_LENGTH, "0")}`;

export const makeScaleCensusGid = (index) =>
	`${GID_PREFIX}${index.toString(36).padStart(GID_SUFFIX_LENGTH, "0")}`;

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
			output: { type: "string" },
			json: { type: "boolean" },
			help: { type: "boolean" },
			worker: { type: "boolean" },
			scenario: { type: "string" },
			count: { type: "string" },
			run: { type: "string" },
			phase: { type: "string" },
			directory: { type: "string" },
		},
	});

	if (values.help) {
		return { mode: "help" };
	}

	if (values.worker) {
		if (values.output) {
			throw new Error("worker mode does not accept --output");
		}
		if (!values.scenario || !values.count || !values.run) {
			throw new Error("worker mode requires --scenario, --count, and --run");
		}
		const [scenario] = parseScenarios(values.scenario);
		if (values.scenario.includes(",")) {
			throw new Error("worker mode accepts exactly one scenario");
		}
		const persistent = isPersistentScaleCensusScenario(scenario);
		const phase = values.phase ?? (persistent ? undefined : "measure");
		if (persistent && phase !== "seed" && phase !== "reopen") {
			throw new Error(
				"persistent worker mode requires --phase seed or --phase reopen",
			);
		}
		if (!persistent && phase !== "measure") {
			throw new Error("resident worker mode only accepts --phase measure");
		}
		if (persistent && !values.directory) {
			throw new Error("persistent worker mode requires --directory");
		}
		if (!persistent && values.directory) {
			throw new Error("resident worker mode does not accept --directory");
		}
		return {
			mode: "worker",
			scenario,
			count: parsePositiveInteger(values.count, "count"),
			run: parsePositiveInteger(values.run, "run"),
			phase,
			...(values.directory ? { directory: values.directory } : {}),
		};
	}

	if (
		values.scenario ||
		values.count ||
		values.run ||
		values.phase ||
		values.directory
	) {
		throw new Error(
			"--scenario, --count, --run, --phase, and --directory are worker-only options",
		);
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
		...((values.output ?? env.SHARED_LOG_SCALE_OUTPUT)
			? { output: values.output ?? env.SHARED_LOG_SCALE_OUTPUT }
			: {}),
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
	activeRow = null,
	failure = null,
}) => ({
	name: SCALE_CENSUS_NAME,
	schemaVersion: 3,
	meta: {
		counts,
		scenarios,
		runs,
		isolation:
			"resident rows use one fresh process; persistent rows use separate seed and reopen processes",
		measurement:
			"resident growth plus persistent index disk, close, and fresh-process reopen costs",
		...host,
	},
	progress: {
		expectedRows: counts.length * scenarios.length * runs,
		completedRows: rows.length,
		complete:
			rows.length === counts.length * scenarios.length * runs &&
			activeRow === null &&
			failure === null,
		activeRow,
		...(failure ? { failure } : {}),
	},
	rows,
});
