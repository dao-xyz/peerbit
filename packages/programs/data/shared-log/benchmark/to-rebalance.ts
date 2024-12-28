// @ts-nocheck
import { Ed25519Keypair, randomBytes } from "@peerbit/crypto";
import type { Index, Indices } from "@peerbit/indexer-interface";
import { create as createIndex } from "@peerbit/indexer-sqlite3";
import * as B from "tinybench";
import {
	type EntryReplicated,
	ReplicationIntent,
	toRebalance,
} from "../src/ranges.js";
import { getEntryAndRangeConstructors } from "./utils.js";

// Run with "node --loader ts-node/esm ./benchmark/to-rebalance.ts"

const resolutions: ("u32" | "u64")[] = ["u32", "u64"];

for (const resolution of resolutions) {
	const { createEntry, createRange, entryClass, numbers } =
		getEntryAndRangeConstructors(resolution);

	let create = async (...rects: EntryReplicated<any>[]) => {
		const indices = await createIndex();
		await indices.start();
		index = await indices.init({ schema: entryClass as any });
		for (const rect of rects) {
			await index!.put(rect);
		}
		return [index, indices] as [Index<EntryReplicated<any>>, Indices];
	};

	let a = (await Ed25519Keypair.create()).publicKey;

	const suite = new B.Bench({ name: resolution });

	let index: Index<any, any>;
	let indices: any = undefined;
	let entryCount = 1e3;
	let rangeboundaryAssigned = 10;
	// this bench tests that the getSamples function can handle overlapping ranges in a more performant way than the sparse ranges

	const consumeAllFromAsyncIterator = async (
		iter: AsyncIterable<EntryReplicated<any>>,
	): Promise<EntryReplicated<any>[]> => {
		const result = [];
		for await (const entry of iter) {
			result.push(entry);
		}
		return result;
	};

	const fullRange = createRange({
		id: randomBytes(32),
		mode: ReplicationIntent.Strict,
		publicKey: a,
		length: 1,
		offset: 0,
	});

	const noRange = createRange({
		id: randomBytes(32),
		mode: ReplicationIntent.Strict,
		publicKey: a,
		length: 0,
		offset: 0,
	});

	const smallRange = createRange({
		id: randomBytes(32),
		mode: ReplicationIntent.Strict,
		publicKey: a,
		length: 0.001,
		offset: 0,
	});

	const anotherSmallRange = createRange({
		id: randomBytes(32),
		mode: ReplicationIntent.Strict,
		publicKey: a,
		length: 0.001,
		offset: 0.5,
	});

	let entries: EntryReplicated<any>[] = [];
	for (let i = 0; i < entryCount; i++) {
		entries.push(
			createEntry({
				coordinate: numbers.denormalize(Math.random()),
				hash: String(i),
				assignedToRangeBoundary: i < rangeboundaryAssigned,
			}),
		);
	}

	const out = await create(...entries);
	index = out[0];
	indices = out[1];

	suite.add("to rebalance all - " + resolution, async () => {
		const samples = await consumeAllFromAsyncIterator(
			toRebalance(
				[
					{
						range: fullRange,
						type: "added",
					},
				],
				index,
			),
		);
		if (samples.length === 0) {
			throw new Error("Expecting samples");
		}
	});

	suite.add("range boundary - " + resolution, async () => {
		const samples = await consumeAllFromAsyncIterator(
			toRebalance(
				[
					{
						range: noRange,
						type: "added",
					},
				],
				index,
			),
		);
		if (samples.length !== rangeboundaryAssigned) {
			throw new Error(
				"Expecting samples: " +
					rangeboundaryAssigned +
					" got " +
					samples.length,
			);
		}
	});

	suite.add("updated - " + resolution, async () => {
		const samples = await consumeAllFromAsyncIterator(
			toRebalance(
				[
					{
						prev: smallRange,
						range: anotherSmallRange,
						type: "updated",
					},
				],
				index,
			),
		);
		if (samples.length === 0) {
			throw new Error(
				"Expecting samples: " +
					rangeboundaryAssigned +
					" got " +
					samples.length,
			);
		}
	});

	await suite.run();
	console.table(suite.table());
	await indices.stop();
}
