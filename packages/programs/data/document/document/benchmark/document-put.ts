import { field, option, variant } from "@dao-xyz/borsh";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { Bench } from "tinybench";
import { Documents, type SetupOptions } from "../src/program.js";

// Run with:
//   cd packages/programs/data/document/document
//   node --loader ts-node/esm ./benchmark/document-put.ts
//
// Env:
// - DOC_WARMUP=1000
// - DOC_ITERATIONS=200
// - DOC_BYTES=1200
// - BENCH_JSON=1 (emit machine-readable JSON)

const payloadBytes = Math.max(
	1,
	Number.parseInt(process.env.DOC_BYTES || "1200", 10) || 1200,
);

const warmupIterations = Number.parseInt(process.env.DOC_WARMUP || "1000", 10);
const iterations = process.env.DOC_ITERATIONS
	? Number.parseInt(process.env.DOC_ITERATIONS, 10)
	: undefined;

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
			this.bytes = opts.bytes;
		}
	}
}

@variant("test_documents")
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor(properties?: { docs: Documents<Document> }) {
		super();
		this.docs = properties?.docs ?? new Documents<Document>();
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({ ...options, type: Document });
	}
}

const peersCount = 1;
const session = await TestSession.connected(peersCount);

const store = new TestStore({
	docs: new Documents<Document>(),
});

const client: ProgramClient = session.peers[0];
await client.open(store, {
	args: {
		replicate: {
			factor: 1,
		},
		log: {
			trim: { type: "length" as const, to: 100 },
		},
	},
});

const payload = new Uint8Array(payloadBytes);
for (let i = 0; i < payload.length; i++) {
	payload[i] = i % 256;
}

let idCounter = 0;
const suite = new Bench({
	name: "document-put",
	warmupIterations: Number.isFinite(warmupIterations) ? warmupIterations : 0,
	iterations:
		typeof iterations === "number" && Number.isFinite(iterations)
			? iterations
			: undefined,
});

suite.add("put (unique)", async () => {
	const doc = new Document({
		id: String(idCounter++),
		name: "hello",
		number: 1n,
		bytes: payload,
	});
	await store.docs.put(doc, { unique: true });
});

try {
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
						payloadBytes,
						warmupIterations,
						iterations: iterations ?? null,
					},
				},
				null,
				2,
			),
		);
	} else {
		console.table(suite.table());
	}
} finally {
	await store.drop();
	await session.stop();
}
