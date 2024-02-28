import B from "benchmark";
import { field, option, variant } from "@dao-xyz/borsh";
import { Documents, SetupOptions } from "../program.js";
import { TestSession } from "@peerbit/test-utils";
import { ProgramClient } from "@peerbit/program";
import { v4 as uuid } from "uuid";
import crypto from "crypto";
import { Program } from "@peerbit/program";

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"
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
		await this.docs.open({ ...options, type: Document, index: { key: "id" } });
	}
}

const peersCount = 1;
const session = await TestSession.connected(peersCount);

const store = new TestStore({
	docs: new Documents<Document>()
});

const client: ProgramClient = session.peers[0];
await client.open(store, {
	args: {
		role: {
			type: "replicator",
			factor: 1
		},
		log: {
			trim: { type: "length" as const, to: 100 }
		}
	}
});

const resolver: Map<string, () => void> = new Map();
store.docs.events.addEventListener("change", (change) => {
	change.detail.added.forEach((doc) => {
		resolver.get(doc.id)!();
		resolver.delete(doc.id);
	});
});

const suite = new B.Suite();
suite
	.add("put", {
		fn: async (deferred) => {
			const doc = new Document({
				id: uuid(),
				name: "hello",
				number: 1n,
				bytes: crypto.randomBytes(1200)
			});
			resolver.set(doc.id, () => {
				deferred.resolve();
			});
			await store.docs.put(doc, { unique: true });
		},

		minSamples: 300,
		defer: true
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (err) => {
		throw err;
	})
	.on("complete", async function (this: any, ...args: any[]) {
		await store.drop();
		await session.stop();
	})
	.run();
