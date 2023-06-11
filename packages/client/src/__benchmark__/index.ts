import B from "benchmark";
import { field, option, variant } from "@dao-xyz/borsh";
import { Documents, DocumentIndex } from "@dao-xyz/peerbit-document";
import { Observer, Program, Replicator } from "@dao-xyz/peerbit-program";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../peer.js";
import { createLibp2pExtended } from "@dao-xyz/peerbit-libp2p";
import { tcp } from "@libp2p/tcp";

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"
// put x 1,114 ops/sec ±2.71% (79 runs sampled)

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	@field({ type: option("string") })
	name?: string;

	@field({ type: option("u64") })
	number?: bigint;

	constructor(opts: Document) {
		if (opts) {
			this.id = opts.id;
			this.name = opts.name;
			this.number = opts.number;
		}
	}
}

@variant("test_documents")
class TestStore extends Program {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor() {
		super();
		this.docs = new Documents({ index: new DocumentIndex({ indexBy: "id" }) });
	}

	async setup(): Promise<void> {
		await this.docs.setup({ type: Document });
	}
}

const peers = await Promise.all(
	[
		await createLibp2pExtended({ transports: [tcp()] }),
		await createLibp2pExtended({ transports: [tcp()] }),
		await createLibp2pExtended({ transports: [tcp()] }),
	].map((x) => Peerbit.create({ libp2p: x }))
);

await peers[0].dial(peers[1]);
await peers[1].dial(peers[2]);

const stores: TestStore[] = [];

// Create store
let address: string | undefined = undefined;

const readerResolver: Map<string, () => void> = new Map();

for (const [i, client] of peers.entries()) {
	const onChange =
		i === peers.length - 1
			? (_log, change) => {
					change.added.forEach((e) => {
						readerResolver.get(e.hash)?.();
						readerResolver.delete(e.hash);
					});
			  }
			: undefined;
	const store: TestStore = await client.open(address || new TestStore(), {
		log: {
			onChange,
		},
		role: i === peers.length - 1 ? new Replicator() : new Observer(),
	});

	await store.load();
	stores.push(store);
	address = store.address.toString();
}

const suite = new B.Suite();
suite
	.add("put", {
		fn: async (deferred) => {
			const writeStore = stores[0];
			const doc = new Document({
				id: uuid(),
				name: uuid(),
				number: 2341n,
			});
			const entry = await writeStore.docs.put(doc, { unique: true });

			// wait for reading
			readerResolver.set(entry.entry.hash, deferred.resolve.bind(deferred));
		},
		defer: true,
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (err) => {
		throw err;
	})
	.on("complete", async function (this: any, ...args: any[]) {
		await Promise.all(peers.map((x) => x.disconnect()));
		await Promise.all(peers.map((n) => n.libp2p.stop()));
	})
	.run();
