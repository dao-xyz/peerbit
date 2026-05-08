import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { type ProgramClient, Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import crypto from "crypto";
import { performance } from "node:perf_hooks";
import { type Args, SharedLog } from "../src/index.js";

type Scenario = "auto-next" | "explicit-root-next";

type BenchRow = {
	scenario: Scenario;
	nativeGraph: boolean;
	entries: number;
	run: number;
	elapsedMs: number;
	opsPerSecond: number;
};

const parsePositiveInteger = (value: string | undefined, fallback: number) => {
	if (!value) {
		return fallback;
	}
	const parsed = Number.parseInt(value, 10);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		throw new Error(`Expected a positive integer, got '${value}'`);
	}
	return parsed;
};

const parseScenarios = (value: string | undefined): Scenario[] => {
	if (!value) {
		return ["auto-next", "explicit-root-next"];
	}
	const scenarios = value
		.split(",")
		.map((x) => x.trim())
		.filter(Boolean);
	for (const scenario of scenarios) {
		if (scenario !== "auto-next" && scenario !== "explicit-root-next") {
			throw new Error(`Unknown scenario '${scenario}'`);
		}
	}
	return scenarios as Scenario[];
};

@variant("shared_log_native_graph_bench_document")
class BenchDocument {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(properties: BenchDocument) {
		this.id = properties.id;
		this.name = properties.name;
		this.bytes = properties.bytes;
	}
}

@variant("shared_log_native_graph_bench_store")
class BenchStore extends Program<Args<BenchDocument, any>> {
	@field({ type: SharedLog })
	logs: SharedLog<BenchDocument, any>;

	constructor(properties?: { logs?: SharedLog<BenchDocument, any> }) {
		super();
		this.logs = properties?.logs ?? new SharedLog<BenchDocument, any>();
	}

	async open(options?: Args<BenchDocument, any>): Promise<void> {
		await this.logs.open({
			...options,
			encoding: {
				decoder: (bytes) => deserialize(bytes, BenchDocument),
				encoder: (data) => serialize(data),
			},
		});
	}
}

const entries = parsePositiveInteger(
	process.env.PEERBIT_SHARED_LOG_NATIVE_GRAPH_ENTRIES,
	1_000,
);
const runs = parsePositiveInteger(
	process.env.PEERBIT_SHARED_LOG_NATIVE_GRAPH_RUNS,
	3,
);
const payloadBytes = parsePositiveInteger(
	process.env.PEERBIT_SHARED_LOG_NATIVE_GRAPH_PAYLOAD_BYTES,
	1_200,
);
const scenarios = parseScenarios(
	process.env.PEERBIT_SHARED_LOG_NATIVE_GRAPH_SCENARIOS,
);

const createDocument = (index: number, bytes: Uint8Array) =>
	new BenchDocument({
		id: `doc-${index}`,
		name: "hello",
		bytes,
	});

const openStore = async (nativeGraph: boolean) => {
	const session = await TestSession.connected(1);
	const store = new BenchStore({
		logs: new SharedLog<BenchDocument, any>({
			id: new Uint8Array(32),
		}),
	});
	const client: ProgramClient = session.peers[0];
	await client.open<BenchStore>(store, {
		args: {
			nativeGraph,
			replicate: { factor: 1 },
			timeUntilRoleMaturity: 0,
			waitForPruneDelay: 0,
			distributionDebounceTime: 50,
		},
	});
	return { session, store };
};

const runScenario = async (
	scenario: Scenario,
	nativeGraph: boolean,
	run: number,
): Promise<BenchRow> => {
	const { session, store } = await openStore(nativeGraph);
	const bytes = crypto.randomBytes(payloadBytes);
	try {
		const started = performance.now();
		for (let i = 0; i < entries; i++) {
			await store.logs.append(createDocument(i, bytes), {
				...(scenario === "explicit-root-next"
					? { meta: { next: [] } }
					: undefined),
			});
		}
		const elapsed = performance.now() - started;
		return {
			scenario,
			nativeGraph,
			entries,
			run,
			elapsedMs: Math.round(elapsed),
			opsPerSecond: Math.round((entries / elapsed) * 1000),
		};
	} finally {
		await store.drop();
		await session.stop();
	}
};

const rows: BenchRow[] = [];
for (const scenario of scenarios) {
	for (const nativeGraph of [false, true]) {
		for (let run = 0; run < runs; run++) {
			rows.push(await runScenario(scenario, nativeGraph, run));
		}
	}
}

const aggregateRows = [...new Set(rows.map((row) => row.scenario))].flatMap(
	(scenario) =>
		[false, true].map((nativeGraph) => {
			const samples = rows.filter(
				(row) => row.scenario === scenario && row.nativeGraph === nativeGraph,
			);
			const meanMs =
				samples.reduce((sum, row) => sum + row.elapsedMs, 0) / samples.length;
			const meanOps =
				samples.reduce((sum, row) => sum + row.opsPerSecond, 0) /
				samples.length;
			return {
				scenario,
				nativeGraph,
				entries,
				runs: samples.length,
				meanMs: Math.round(meanMs),
				meanOpsPerSecond: Math.round(meanOps),
			};
		}),
);

console.table(aggregateRows);
console.table(rows);
process.exit(process.exitCode ?? 0);
