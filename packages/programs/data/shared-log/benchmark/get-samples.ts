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
			width: denormalizeFn(properties.length),
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

	const suite = new B.Bench({ name: resolution, warmupIterations: 1000 });

	let index: Index<any, unknown> | undefined;
	let indices: any = undefined;

	let sampleSize = 2;
	let rangeCount = 1e4;

	// this bench tests that the getSamples function can handle overlapping ranges in a more performant way than the sparse ranges
	suite.add(
		"get samples one range - " + resolution,
		async () => {
			const samples = await getSamples(
				numbers.getGrid(numbers.denormalize(Math.random()), sampleSize),
				index!,
				0,
				numbers,
			);

			if (samples.size !== 1) {
				throw new Error(
					"Expected at least " + 1 + " samples, got " + samples.size,
				);
			}
		},
		{
			beforeAll: async () => {
				const out = await create(
					createReplicationRangeFromNormalized({
						publicKey: a,
						length: 1,
						offset: Math.random(),
						timestamp: 0n,
					}),
				);
				index = out[0];
				indices = out[1];
			},
			afterAll: async () => {
				await indices.stop();
			},
		},
	);

	suite.add(
		"get samples one range unique replicators provided - " + resolution,
		async () => {
			const samples = await getSamples(
				numbers.getGrid(numbers.denormalize(Math.random()), sampleSize),
				index!,
				0,
				numbers,
				{
					uniqueReplicators: new Set([a.hashcode()]),
				},
			);

			if (samples.size !== 1) {
				throw new Error(
					"Expected at least " + 1 + " samples, got " + samples.size,
				);
			}
		},
		{
			beforeAll: async () => {
				const out = await create(
					createReplicationRangeFromNormalized({
						publicKey: a,
						length: 1,
						offset: Math.random(),
						timestamp: 0n,
					}),
				);
				index = out[0];
				indices = out[1];
			},
			afterAll: async () => {
				await indices.stop();
			},
		},
	);

	suite.add(
		"get samples overlapping - " + resolution,
		async () => {
			const point = numbers.denormalize(Math.random());
			const samples = await getSamples(
				numbers.getGrid(point, sampleSize),
				index!,
				0,
				numbers,
				{
					onlyIntersecting: true,
				},
			);
			if (samples.size < sampleSize) {
				throw new Error(
					"Expected at least " + sampleSize + " samples, got " + samples.size,
				);
			}
		},
		{
			beforeAll: async () => {
				let ranges: any[] = [];

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
								offset: Math.random(),
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								length: 0.4 / rangeCount,
								offset: Math.random(),
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: Math.random(),
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: Math.random(),
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

	suite.add(
		"get samples sparse - " + resolution,
		async () => {
			const samples = await getSamples(
				numbers.getGrid(numbers.denormalize(Math.random()), sampleSize),
				index!,
				0,
				numbers,
			);

			if (samples.size < sampleSize) {
				throw new Error(
					"Expected at least " + sampleSize + " samples, got " + samples.size,
				);
			}
		},
		{
			beforeAll: async () => {
				let ranges: any[] = [];

				for (let i = 0; i < rangeCount; i++) {
					ranges.push(
						...[
							createReplicationRangeFromNormalized({
								publicKey: a,
								length: 0.2 / rangeCount,
								offset: Math.random(),
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								length: 0.4 / rangeCount,
								offset: Math.random(),
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: Math.random(),
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								length: 0.6 / rangeCount,
								offset: Math.random(),
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
