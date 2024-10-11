import { Ed25519Keypair } from "@peerbit/crypto";
import type { Index } from "@peerbit/indexer-interface";
import { create as createIndex } from "@peerbit/indexer-sqlite3";
import B from "benchmark";
import {
	ReplicationRangeIndexable,
	getEvenlySpacedU32,
	getSamples,
} from "../src/ranges.js";

// Run with "node --loader ts-node/esm ./benchmark/get-samples.ts"

let create = async (
	...rects: ReplicationRangeIndexable[]
): Promise<[Index<ReplicationRangeIndexable, unknown>, any]> => {
	const indices = await createIndex();
	const index = await indices.init({ schema: ReplicationRangeIndexable });
	await indices.start();
	for (const rect of rects) {
		await index.put(rect);
	}
	return [index, indices];
};

let a = (await Ed25519Keypair.create()).publicKey;
let b = (await Ed25519Keypair.create()).publicKey;
let c = (await Ed25519Keypair.create()).publicKey;

let ranges: ReplicationRangeIndexable[] = [];
let rangeCount = 1000;
for (let i = 0; i < rangeCount; i++) {
	ranges.push(
		...[
			new ReplicationRangeIndexable({
				publicKey: a,
				length: 0.2 / rangeCount,
				offset: (0 + rangeCount / i) % 1,
				timestamp: 0n,
			}),
			new ReplicationRangeIndexable({
				publicKey: b,
				length: 0.4 / rangeCount,
				offset: (0.333 + rangeCount / i) % 1,
				timestamp: 0n,
			}),
			new ReplicationRangeIndexable({
				publicKey: c,
				length: 0.6 / rangeCount,
				offset: (0.666 + rangeCount / i) % 1,
				timestamp: 0n,
			}),
			new ReplicationRangeIndexable({
				publicKey: c,
				length: 0.6 / rangeCount,
				offset: (0.666 + rangeCount / i) % 1,
				timestamp: 0n,
			}),
		],
	);
}

const [index, indices] = await create(...ranges);
const suite = new B.Suite();
suite
	.add("getSamples", {
		fn: async (deferred: any) => {
			await getSamples(getEvenlySpacedU32(Math.random(), 2), index, 0);
			deferred.resolve();
		},
		defer: true,
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (err: any) => {
		throw err;
	})
	.on("complete", async function (this: any) {
		await indices.drop();
	})
	.run();
