import { deserialize, field, option, serialize, variant } from "@dao-xyz/borsh";
import { type ProgramClient } from "@peerbit/program";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import crypto from "crypto";
import { Bench } from "tinybench";
import { v4 as uuid } from "uuid";
import { type Args, SharedLog } from "../src/index.js";

// Run with "node --loader ts-node/esm ./benchmark/index.ts"
// put x 5,843 ops/sec ±4.50% (367 runs sampled)

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

const peersCount = 1;
const session = await TestSession.connected(peersCount);

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

const suite = new Bench({ name: "put" });

const bytes = crypto.randomBytes(1200);

suite.add("put", async () => {
	const doc = new Document({
		id: uuid(),
		name: "hello",
		number: 1n,
		bytes,
	});
	await store.logs.append(doc, { meta: { next: [] } });
});

await suite.run();
console.table(suite.table());
await store.drop();
await session.stop();
