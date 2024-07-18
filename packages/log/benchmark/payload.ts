import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import B from "benchmark";
import crypto from "crypto";
import { v4 as uuid } from "uuid";
import { BORSH_ENCODING } from "../src/encoding.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

// Run with "node --loader ts-node/esm ./benchmark/payload.ts"

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
let log: Log<Document>;
let store: AnyBlockStore;
const key = await Ed25519Keypair.create();

const reset = async () => {
	log = new Log<Document>();
	store = new AnyBlockStore();
	await log.open(store, key, { encoding: BORSH_ENCODING(Document) });
};
await reset();

class NestedEntry {
	@field({ type: Entry })
	entry: Entry<any>;

	constructor(entry: Entry<any>) {
		this.entry = entry;
	}
}
const suite = new B.Suite({ delay: 100 });
suite
	.add("1e3", {
		fn: async (deferred: any) => {
			const doc = new Document({
				id: uuid(),
				name: "hello",
				number: 1n,
				bytes: crypto.randomBytes(1e3),
			});
			await log.append(doc);

			deferred.resolve();
		},
		defer: true,
	})
	.add("1e4", {
		fn: async (deferred: any) => {
			const doc = new Document({
				id: uuid(),
				name: "hello",
				number: 1n,
				bytes: crypto.randomBytes(1e4),
			});
			await log.append(doc);

			deferred.resolve();
		},
		defer: true,
	})
	.add("1e5", {
		fn: async (deferred: any) => {
			const doc = new Document({
				id: uuid(),
				name: "hello",
				number: 1n,
				bytes: crypto.randomBytes(1e5),
			});
			const entry = await log.append(doc);
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));
			serialize(new NestedEntry(entry.entry));

			deferred.resolve();
		},
		defer: true,
	})
	.add("1e6", {
		fn: async (deferred: any) => {
			const doc = new Document({
				id: uuid(),
				name: "hello",
				number: 1n,
				bytes: crypto.randomBytes(1e5),
			});
			await log.append(doc);

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
