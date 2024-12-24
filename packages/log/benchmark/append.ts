import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { create } from "@peerbit/indexer-sqlite3";
import * as B from "tinybench";
import { Log } from "../src/log.js";

// Run with "node --loader ts-node/esm ./benchmark/append.ts"

let log: Log<Uint8Array>;
let store: AnyBlockStore;
const key = await Ed25519Keypair.create();

const close = () => {
	return log?.close();
};
const reset = async () => {
	await close();
	log = new Log<Uint8Array>();
	store = new AnyBlockStore();
	await log.open(store, key, { indexer: await create() });
};
await reset();

const suite = new B.Bench({ warmupIterations: 1000, setup: reset });
await suite
	.add("chain", async () => {
		await log.append(new Uint8Array([1, 2, 3]));
	})
	.add("no-next", async () => {
		await log.append(new Uint8Array([1, 2, 3]), { meta: { next: [] } });
	})
	.run();

await close();
console.table(suite.table());
