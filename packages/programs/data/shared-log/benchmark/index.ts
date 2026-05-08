import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { createStore as createRustStore } from "@peerbit/any-store-rust";
import { type ProgramClient } from "@peerbit/program";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import crypto from "crypto";
import { Bench } from "tinybench";
import { v4 as uuid } from "uuid";
import { type Args, SharedLog } from "../src/index.js";

// Run with:
//   SHARED_LOG_STORE=both pnpm --filter @peerbit/shared-log run benchmark:append-storage

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

for (const mode of modes) {
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
	await client.open<TestStore>(store, {
		args: {
			replicate: {
				factor: 1,
			},
			trim: { type: "length" as const, to: 100 },
		},
	});

	const suite = new Bench({ name: `${mode} put` });

	suite.add(`${mode} put`, async () => {
		const doc = new Document({
			id: uuid(),
			name: "hello",
			number: 1n,
			bytes,
		});
		await store.logs.append(doc, { meta: { next: [] } });
	});

	await suite.run();
	rows.push(...suite.table());
	await store.drop();
	await session.stop();
}

console.table(rows);
