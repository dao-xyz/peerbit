import B from "benchmark";
import { field, option, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { v4 as uuid } from "uuid";
import { Peerbit, createLibp2pExtended } from "peerbit";
import { tcp } from "@libp2p/tcp";
import { Documents, SetupOptions } from "../document-store.js";
import { Replicator } from "@peerbit/shared-log";
import { DirectSub } from "@peerbit/pubsub";
import { mplex } from "@libp2p/mplex";

// Run with "node --loader ts-node/esm ./src/__benchmark__/replication.ts"
// put x 1,009 ops/sec ±2.57% (80 runs sampled)

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
			streamMuxers: [mplex()],
			services: {
				pubsub: (sub) =>
					new DirectSub(sub, {
						canRelayMessage: true,
						connectionManager: false
					})
			}
		}),
		await createLibp2pExtended({
			connectionManager: {},
			transports: [tcp()],
			streamMuxers: [mplex()],
			services: {
				pubsub: (sub) =>
					new DirectSub(sub, {
						canRelayMessage: true,
						connectionManager: false
					})
			}
		}),
		await createLibp2pExtended({
			transports: [tcp()],
			streamMuxers: [mplex()],
			services: {
				pubsub: (sub) =>
					new DirectSub(sub, {
						canRelayMessage: true,
						connectionManager: false
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
	const settings = {
		//canPerform: (e: Entry<any>) => e.verifySignatures(),
		sync: () => true,
		role: new Replicator() /* i === peers.length - 1 ? new Replicator() : new Observer(), */
	};
	let store: TestStore;
	if (address) {
		store = await client.open<TestStore, any>(address, {
			args: settings
		});
	} else {
		store = await client.open(new TestStore(), {
			args: settings
		});
		address = store.address;
	}
	if (i === 1) {
		store.docs.events.addEventListener("change", (event) => {
			//   console.log(event.detail.added)
			event.detail.added.forEach((e) => {
				readerResolver.get(e.id)!();
				readerResolver.delete(e.id);
			});
		});
	}

	stores.push(store);
}

const suite = new B.Suite();
suite
	.add("put", {
		fn: async (deferred) => {
			const writeStore = stores[0];
			const doc = new Document({
				id: uuid(),
				name: uuid(),
				number: 2341n
			});
			// wait for reading
			readerResolver.set(doc.id, deferred.resolve.bind(deferred));
			await writeStore.docs.put(doc, { unique: true });
		},
		defer: true
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (err) => {
		throw err;
	})
	.on("complete", async function (this: any, ...args: any[]) {
		await Promise.all(peers.map((x) => x.stop()));
		await Promise.all(peers.map((x) => x["libp2p"].stop()));
	})
	.run();
