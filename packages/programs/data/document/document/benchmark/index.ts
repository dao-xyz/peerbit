import { field, option, variant } from "@dao-xyz/borsh";
import { Program, type ProgramClient } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import crypto from "crypto";
import * as B from "tinybench";
import { v4 as uuid } from "uuid";
import { Documents, type SetupOptions } from "../src/program.js";

// Run with "node --loader ts-node/esm ./benchmark/index.ts"
// put x 9,522 ops/sec ±4.61% (76 runs sampled) (prev merge store with log: put x 11,527 ops/sec ±6.09% (75 runs sampled))

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
		if (properties) {
			this.docs = properties.docs;
		}
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

const suite = new B.Bench({ name: "put", warmupIterations: 1000 });
suite.add("put", async () => {
	const doc = new Document({
		id: uuid(),
		name: "hello",
		number: 1n,
		bytes: crypto.randomBytes(1200),
	});
	await store.docs.put(doc, {
		unique: true,
	});
});

await suite.run();
console.table(suite.table());
await store.drop();
await session.stop();
