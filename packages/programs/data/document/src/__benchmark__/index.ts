import B from "benchmark";
import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Documents } from "../document-store.js";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions } from "@dao-xyz/peerbit-store";
import { Identity } from "@dao-xyz/peerbit-log";
import {
	Ed25519Keypair,
	X25519Keypair,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import Cache from "@dao-xyz/lazy-level";
import { AbstractLevel } from "abstract-level";
import { Program } from "@dao-xyz/peerbit-program";
import { DocumentIndex } from "../document-index.js";
import { v4 as uuid } from "uuid";

// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"
// put x 9,477 ops/sec ±2.86% (80 runs sampled)

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

	constructor(properties?: { docs: Documents<Document> }) {
		super();
		if (properties) {
			this.docs = properties.docs;
		}
	}
	async setup(): Promise<void> {
		await this.docs.setup({ type: Document });
	}
}

const cacheStores: AbstractLevel<any, string, Uint8Array>[] = [];
const peersCount = 1;
const session = await LSession.connected(peersCount);

for (let i = 0; i < peersCount; i++) {
	cacheStores.push(await createStore());
}
const stores: TestStore[] = [];
const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		sign: (data) => ed.sign(data),
	} as Identity;
};

// Create store
for (let i = 0; i < peersCount; i++) {
	const store =
		i > 0
			? (await TestStore.load<TestStore>(
					session.peers[i].directblock,
					stores[0].address!
			  ))!
			: new TestStore({
					docs: new Documents<Document>({
						index: new DocumentIndex({
							indexBy: "id",
						}),
					}),
			  });
	const keypair = await X25519Keypair.create();
	await store.init(session.peers[i], await createIdentity(), {
		replicate: i === 0,
		store: {
			...DefaultOptions,
			encryption: {
				getEncryptionKeypair: () => keypair,
				getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
					for (let i = 0; i < publicKeys.length; i++) {
						if (publicKeys[i].equals((keypair as X25519Keypair).publicKey)) {
							return {
								index: i,
								keypair: keypair as Ed25519Keypair | X25519Keypair,
							};
						}
					}
				},
			},
			resolveCache: () =>
				new Cache(cacheStores[i], { batch: { interval: 100 } }),
		},
	});
	stores.push(store);
}

const suite = new B.Suite();
suite
	.add("put", {
		fn: async (deferred) => {
			const writeStore = stores[0];
			const doc = new Document({
				id: uuid(),
				name: "hello",
				number: 1n,
			});
			await writeStore.docs.put(doc, { trim: { type: "length", to: 100 } });
			deferred.resolve();
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
		await Promise.all(stores.map((x) => x.drop()));
		await Promise.all(cacheStores.map((x) => x.close()));
		await session.stop();
	})
	.run();
