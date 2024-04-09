import B from "benchmark";
import { field, option, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { v4 as uuid } from "uuid";
import { Peerbit, createLibp2pExtended } from "peerbit";
import { tcp } from "@libp2p/tcp";
import { Documents, type SetupOptions } from "../src/program.js";
import { DirectSub } from "@peerbit/pubsub";
import { yamux } from "@chainsafe/libp2p-yamux";
import { delay } from "@peerbit/time";

// Run with "node --loader ts-node/esm ./src/__benchmark__/replication.ts"
// put x 862 ops/sec ±4.75% (75 runs sampled)

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
class TestStore extends Program<Partial<SetupOptions<Document>>> {
	@field({ type: Documents })
	docs: Documents<Document>;

	constructor() {
		super();
		this.docs = new Documents();
	}

	async open(options?: Partial<SetupOptions<Document>>): Promise<void> {
		await this.docs.open({ ...options, type: Document });
	}
}

const peers = await Promise.all(
	[
		await createLibp2pExtended({
			transports: [tcp()],
			streamMuxers: [yamux()],
			services: {
				pubsub: (sub) =>
					new DirectSub(sub, {
						canRelayMessage: true
						/* connectionManager: true */
					})
			}
		}),
		await createLibp2pExtended({
			connectionManager: {},
			transports: [tcp()],
			streamMuxers: [yamux()],
			services: {
				pubsub: (sub) =>
					new DirectSub(sub, {
						canRelayMessage: true
						/* connectionManager: true */
					})
			}
		}),
		await createLibp2pExtended({
			transports: [tcp()],
			streamMuxers: [yamux()],
			services: {
				pubsub: (sub) =>
					new DirectSub(sub, {
						canRelayMessage: true
						/* connectionManager: true */
					})
			}
		})
	].map((x) => Peerbit.create({ libp2p: x }))
);

await peers[0].dial(peers[1].getMultiaddrs());
await peers[1].dial(peers[2].getMultiaddrs());

const stores: TestStore[] = [];

// Create store
let address: string | undefined = undefined;

const readerResolver: Map<string, () => void> = new Map();

for (const [i, client] of peers.entries()) {
	let store: TestStore;
	if (address) {
		store = await client.open<TestStore>(address, {
			args: {
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});
	} else {
		store = await client.open(new TestStore(), {
			args: {
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});
		address = store.address;
	}
	if (i === 1) {
		store.docs.events.addEventListener("change", (event) => {
			event.detail.added.forEach((e) => {
				readerResolver.get(e.id)?.();
				readerResolver.delete(e.id);
			});
		});
	}

	stores.push(store);
}

const createDoc = () =>
	new Document({
		id: uuid(),
		name: uuid(),
		number: 2341n
	});

// warmup
for (let i = 0; i < 10; i++) {
	await stores[0].docs.put(createDoc(), { unique: true });
	await delay(1000);
}

await delay(5000);
const suite = new B.Suite();
suite
	.add("put", {
		fn: async (deferred: any) => {
			const writeStore = stores[0];
			const doc = createDoc();
			// wait for reading
			readerResolver.set(doc.id, deferred.resolve.bind(deferred));
			await writeStore.docs.put(doc, { unique: true });
		},
		defer: true
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (err: any) => {
		throw err;
	})
	.on("complete", async function (this: any, ...args: any[]) {
		await Promise.all(peers.map((x) => x.stop()));
		await Promise.all(peers.map((x) => x["libp2p"].stop()));
	})
	.run();
