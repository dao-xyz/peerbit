import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { create } from "@peerbit/indexer-sqlite3";
import B from "benchmark";
import { Log } from "../src/log.js";

// Run with "node --loader ts-node/esm ./benchmark/append.ts"

let log: Log<Uint8Array>;
let store: AnyBlockStore;
const key = await Ed25519Keypair.create();

const reset = async () => {
	log = new Log<Uint8Array>();
	store = new AnyBlockStore();
	await log.open(store, key, { indexer: await create() });
};
await reset();

const suite = new B.Suite({ delay: 100 });
suite
	.add("chain", {
		fn: async (deferred: any) => {
			await log.append(new Uint8Array([1, 2, 3]));
			deferred.resolve();
		},
		defer: true,
	})
	.add("no-next", {
		fn: async (deferred: any) => {
			await log.append(new Uint8Array([1, 2, 3]), { meta: { next: [] } });
			deferred.resolve();
		},
		defer: true,
	})
	.on("cycle", async (event: any) => {
		console.log(String(event.target));
		await reset();
	})
	.on("error", (err: any) => {
		throw err;
	})
	.run();
