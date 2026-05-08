import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { createStore as createRustStore } from "@peerbit/any-store-rust";
import { type ProgramClient } from "@peerbit/program";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import crypto from "crypto";
import { performance } from "node:perf_hooks";
import { Bench } from "tinybench";
import { v4 as uuid } from "uuid";
import { type Args, SharedLog } from "../src/index.js";

// Run with:
//   SHARED_LOG_STORE=both pnpm --filter @peerbit/shared-log run benchmark:append-storage
//   SHARED_LOG_BATCH_ENTRIES=1000 SHARED_LOG_STORE=both pnpm --filter @peerbit/shared-log run benchmark:append-storage

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
		this.id = opts.id;
		this.name = opts.name;
		this.number = opts.number;
		this.bytes = opts.bytes;
	}
}

@variant("test_shared_log")
class TestStore extends Program<Args<Document, any>> {
	@field({ type: SharedLog })
	logs: SharedLog<Document, any>;

	constructor(properties?: { logs: SharedLog<Document, any> }) {
		super();
		this.logs = properties?.logs || new SharedLog();
	}

	async open(options?: Args<Document, any>): Promise<void> {
		await this.logs.open({
			...options,
			encoding: {
				decoder: (bytes) => deserialize(bytes, Document),
				encoder: (data) => serialize(data),
			},
		});
	}
}

type StoreMode = "level" | "rust";

const storeMode = (process.env.SHARED_LOG_STORE ?? "level") as StoreMode | "both";
const modes: StoreMode[] =
	storeMode === "both" ? ["level", "rust"] : [storeMode === "rust" ? "rust" : "level"];
const peersCount = 1;
const bytes = crypto.randomBytes(1200);
const rows = [];
const batchEntries = Number(process.env.SHARED_LOG_BATCH_ENTRIES ?? 0);

const createDocument = (index = 0) =>
	new Document({
		id: uuid(),
		name: "hello",
		number: BigInt(index),
		bytes,
	});

const openStore = async (
	mode: StoreMode,
	args: Args<Document, any> = {
		replicate: {
			factor: 1,
		},
		trim: { type: "length" as const, to: 100 },
	},
) => {
	const session = await TestSession.connected(peersCount, {
		storage:
			mode === "rust"
				? {
						storeFactory: createRustStore,
					}
				: undefined,
	});

	const store = new TestStore({
		logs: new SharedLog<Document, any>({
			id: new Uint8Array(32),
		}),
	});

	const client: ProgramClient = session.peers[0];
	await client.open<TestStore>(store, { args });
	return { session, store };
};

for (const mode of modes) {
	const { session, store } = await openStore(mode);

	const suite = new Bench({ name: `${mode} put` });

	suite.add(`${mode} put`, async () => {
		await store.logs.append(createDocument(), { meta: { next: [] } });
	});

	await suite.run();
	rows.push(...suite.table());
	await store.drop();
	await session.stop();
}

if (batchEntries > 0) {
	const batchRows = [];
	for (const mode of modes) {
		{
			const { session, store } = await openStore(mode, { replicate: false });
			const started = performance.now();
			for (let i = 0; i < batchEntries; i++) {
				await store.logs.append(createDocument(i), {
					replicate: false,
					target: "none",
				});
			}
			const elapsed = performance.now() - started;
			batchRows.push({
				mode,
				name: "shared-log append loop auto-next",
				entries: batchEntries,
				elapsedMs: Math.round(elapsed),
				opsPerSecond: Math.round((batchEntries / elapsed) * 1000),
			});
			await store.drop();
			await session.stop();
		}
		{
			const { session, store } = await openStore(mode, { replicate: false });
			const started = performance.now();
			await store.logs.appendMany(
				Array.from({ length: batchEntries }, (_, index) =>
					createDocument(index),
				),
				{
					replicate: false,
					target: "none",
				},
			);
			const elapsed = performance.now() - started;
			batchRows.push({
				mode,
				name: "shared-log appendMany auto-next",
				entries: batchEntries,
				elapsedMs: Math.round(elapsed),
				opsPerSecond: Math.round((batchEntries / elapsed) * 1000),
			});
			await store.drop();
			await session.stop();
		}
	}
	console.table(batchRows);
}

console.table(rows);
