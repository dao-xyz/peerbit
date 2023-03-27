import B from "benchmark";
import { field, option, variant } from "@dao-xyz/borsh";
import { Documents, DocumentIndex } from "@dao-xyz/peerbit-document";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import {
	ObserverType,
	Program,
	ReplicatorType,
} from "@dao-xyz/peerbit-program";
import { v4 as uuid } from "uuid";
import { Peerbit } from "../peer.js";

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"
// put x 1,257 ops/sec ±2.41% (81 runs sampled)

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

const peersCount = 3;
const session = await LSession.connected(peersCount);

await session.connect([
	[session.peers[0], session.peers[1]],
	[session.peers[1], session.peers[2]],
]);

const stores: TestStore[] = [];

// Create store
const peers: Peerbit[] = [];
let address: string | undefined = undefined;

const readerResolver: Map<string, () => void> = new Map();

for (const [i, peer] of session.peers.entries()) {
	const client = await Peerbit.create({ libp2p: peer });
	peers.push(client);
	const store = await client.open(address || new TestStore(), {
		onUpdate:
			i === session.peers.length - 1
				? (change) => {
					change.added.forEach((e) => {
						readerResolver.get(e.hash)?.();
						readerResolver.delete(e.hash);
					});
				}
				: undefined,
		role:
			i === session.peers.length - 1
				? new ReplicatorType()
				: new ObserverType(),
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
		await session.stop();
	})
	.run();
