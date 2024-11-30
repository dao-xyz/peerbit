import { Ed25519Keypair, PublicSignKey } from "@peerbit/crypto";
import type { Index } from "@peerbit/indexer-interface";
import { create as createIndex } from "@peerbit/indexer-sqlite3";
import * as B from "tinybench";
import { createNumbers, denormalizer } from "../src/integers.js";
import {
	ReplicationIntent,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
	getSamples,
} from "../src/ranges.js";

// Run with "node --loader ts-node/esm ./benchmark/get-samples.ts"

const resolutions: ("u32" | "u64")[] = ["u32", "u64"];
for (const resolution of resolutions) {
	const rangeClass =
		resolution === "u32"
			? ReplicationRangeIndexableU32
			: ReplicationRangeIndexableU64;
	const numbers = createNumbers(resolution);
	const denormalizeFn = denormalizer(resolution);

	const createReplicationRangeFromNormalized = (properties: {
		id?: Uint8Array;
		publicKey: PublicSignKey;
		length: number;
		offset: number;
		timestamp?: bigint;
		mode?: ReplicationIntent;
	}) => {
		return new rangeClass({
			id: properties.id,
			publicKey: properties.publicKey,
			mode: properties.mode,
			// @ts-ignore
			length: denormalizeFn(properties.length),
			// @ts-ignore
			offset: denormalizeFn(properties.offset),
			timestamp: properties.timestamp,
		});
	};

	let create = async (...rects: any[]): Promise<[Index<any, unknown>, any]> => {
		const indices = await createIndex();
		const index = await indices.init({ schema: rangeClass as any });
		await indices.start();
		for (const rect of rects) {
			await index.put(rect);
		}
		return [index, indices];
	};

	let a = (await Ed25519Keypair.create()).publicKey;
	let b = (await Ed25519Keypair.create()).publicKey;
	let c = (await Ed25519Keypair.create()).publicKey;

	const suite = new B.Bench({ name: resolution });

	let index: Index<any, unknown> | undefined;
	let indices: any = undefined;
	/* 
		suite.add("get samples sparse - " + resolution, async () => {
			await getSamples(
				numbers.getGrid(numbers.denormalize(Math.random()), 2),
				index!,
				0,
				numbers,
			);
	
		}, {
			beforeAll: async () => {
				let ranges: any[] = [];
				let rangeCount = 1e4;
				for (let i = 0; i < rangeCount; i++) {
					ranges.push(
						...[
							createReplicationRangeFromNormalized({
								publicKey: a,
								length: 0.2 / rangeCount,
								offset: (0 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								length: 0.4 / rangeCount,
								offset: (0.333 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: (0.666 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: (0.666 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
						],
					);
				}
	
				const out = await create(...ranges);
				index = out[0];
				indices = out[1];
			},
			afterAll: async () => {
				await indices.stop();
			},
		}); */

	// this bench tests that the getSamples function can handle overlapping ranges in a more performant way than the sparse ranges
	suite.add(
		"get samples overlapping - " + resolution,
		async () => {
			await getSamples(
				numbers.getGrid(numbers.denormalize(Math.random()), 2),
				index!,
				0,
				numbers,
			);
		},
		{
			beforeAll: async () => {
				let ranges: any[] = [];
				let rangeCount = 1e4;

				// add 2 overlapping ranges
				ranges.push(
					createReplicationRangeFromNormalized({
						publicKey: a,
						length: 1,
						offset: 0.1 % 1,
						timestamp: 0n,
					}),
				);
				ranges.push(
					createReplicationRangeFromNormalized({
						publicKey: b,
						length: 1,
						offset: 0.7 % 1,
						timestamp: 0n,
					}),
				);

				// add sparse ranges
				for (let i = 0; i < rangeCount; i++) {
					ranges.push(
						...[
							createReplicationRangeFromNormalized({
								publicKey: a,
								length: 0.2 / rangeCount,
								offset: (0 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								length: 0.4 / rangeCount,
								offset: (0.333 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: (0.666 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: (0.666 + i / rangeCount) % 1,
								timestamp: 0n,
							}),
						],
					);
				}

				const out = await create(...ranges);
				index = out[0];
				indices = out[1];
			},
			afterAll: async () => {
				await indices.stop();
			},
		},
	);

	await suite.run();

	console.table(suite.table());
}
