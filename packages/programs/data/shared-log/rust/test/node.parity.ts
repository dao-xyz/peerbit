import { BinaryWriter } from "@dao-xyz/borsh";
import { sha256 } from "@peerbit/crypto";
import { create as createIndices } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import {
	type NativeReplicationRange,
	createRangePlanner,
} from "../src/index.js";

type Resolution = "u32" | "u64";
type AnyRange = {
	idString: string;
	hash: string;
	timestamp: bigint;
	start1: number | bigint;
	end1: number | bigint;
	start2: number | bigint;
	end2: number | bigint;
	width: number | bigint;
	mode: number;
};
type SharedLogModules = {
	bytesToNumber: (resolution: Resolution) => (bytes: Uint8Array) => number | bigint;
	createNumbers: (resolution: Resolution) => any;
	denormalizer: (resolution: Resolution) => (value: number) => number | bigint;
	ReplicationIntent: {
		NonStrict: number;
		Strict: number;
	};
	ReplicationRangeIndexableU32: new (properties: any) => AnyRange;
	ReplicationRangeIndexableU64: new (properties: any) => AnyRange;
	getTypeScriptSamples: (
		cursors: Array<number | bigint>,
		index: any,
		roleAge: number,
		numbers: any,
		options?: {
			onlyIntersecting?: boolean;
			uniqueReplicators?: Set<string>;
			peerFilter?: Set<string>;
		},
	) => Promise<Map<string, { intersecting: boolean }>>;
};

let sharedLog: SharedLogModules | undefined;

const loadSharedLog = async (): Promise<SharedLogModules> => {
	if (!sharedLog) {
		const integersPath = "../../../dist/src/integers.js";
		const rangesPath = "../../../dist/src/ranges.js";
		const [{ bytesToNumber, createNumbers, denormalizer }, ranges] = await Promise.all([
			import(integersPath),
			import(rangesPath),
		]);
		sharedLog = {
			bytesToNumber,
			createNumbers,
			denormalizer,
			ReplicationIntent: ranges.ReplicationIntent,
			ReplicationRangeIndexableU32: ranges.ReplicationRangeIndexableU32,
			ReplicationRangeIndexableU64: ranges.ReplicationRangeIndexableU64,
			getTypeScriptSamples: ranges.getSamples,
		};
	}
	return sharedLog;
};

const rangeClass = (resolution: Resolution) => {
	if (!sharedLog) {
		throw new Error("Shared log modules are not loaded");
	}
	return resolution === "u32"
		? sharedLog.ReplicationRangeIndexableU32
		: sharedLog.ReplicationRangeIndexableU64;
};

const deterministicId = (id: number) => {
	const bytes = new Uint8Array(32);
	bytes[31] = id;
	return bytes;
};

const createRange = (
	resolution: Resolution,
	properties: {
		id: number;
		hash: string;
		offset: number;
		width: number;
		timestamp?: bigint;
		mode?: number;
	},
): AnyRange => {
	if (!sharedLog) {
		throw new Error("Shared log modules are not loaded");
	}
	const denormalize = sharedLog.denormalizer(resolution);
	return new (rangeClass(resolution) as any)({
		id: deterministicId(properties.id),
		publicKeyHash: properties.hash,
		offset: denormalize(properties.offset),
		width: denormalize(properties.width),
		timestamp: properties.timestamp,
		mode: properties.mode,
	});
};

const toNativeRange = (range: AnyRange): NativeReplicationRange => ({
	id: range.idString,
	hash: range.hash,
	timestamp: range.timestamp,
	start1: range.start1,
	end1: range.end1,
	start2: range.start2,
	end2: range.end2,
	width: range.width,
	mode: range.mode,
});

const mapEntries = (map: Map<string, { intersecting: boolean }>) =>
	[...map.entries()];

const sortedMapEntries = (map: Map<string, { intersecting: boolean }>) =>
	mapEntries(map).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

const hashGidCursor = async (resolution: Resolution, gid: string) => {
	if (!sharedLog) {
		throw new Error("Shared log modules are not loaded");
	}
	const writer = new BinaryWriter();
	writer.string(gid);
	const digest = await sha256(writer.finalize());
	return sharedLog.bytesToNumber(resolution)(digest);
};

const expectNativeParity = async (
	resolution: Resolution,
	properties: {
		ranges: AnyRange[];
		cursors: Array<number | bigint>;
		roleAge?: number;
		options?: {
			onlyIntersecting?: boolean;
			uniqueReplicators?: Set<string>;
			peerFilter?: Set<string>;
		};
		// Compare entries sorted by hash instead of by map insertion order. The
		// TypeScript getSamples insertion order for intersecting matches follows
		// unordered sqlite query results, which the native planner does not
		// reproduce in every multi-cursor scenario (see the seeded sweep below).
		ignoreOrder?: boolean;
		message?: string;
	},
) => {
	if (!sharedLog) {
		throw new Error("Shared log modules are not loaded");
	}
	const numbers = sharedLog.createNumbers(resolution);
	const indices = await createIndices();
	await indices.start();

	try {
		const index = await indices.init({
			schema: rangeClass(resolution) as any,
		});
		const planner = await createRangePlanner(resolution);

		for (const range of properties.ranges) {
			await index.put(range);
			planner.put(toNativeRange(range));
		}

		const now = Date.now();
		const expected = await sharedLog.getTypeScriptSamples(
			properties.cursors,
			index as any,
			properties.roleAge ?? 0,
			numbers,
			properties.options,
		);
		const actual = planner.getSamples(properties.cursors, {
			now,
			roleAge: properties.roleAge,
			onlyIntersecting: properties.options?.onlyIntersecting,
			uniqueReplicators: properties.options?.uniqueReplicators,
			peerFilter: properties.options?.peerFilter,
		});

		const project = properties.ignoreOrder ? sortedMapEntries : mapEntries;
		expect(project(actual)).to.deep.equal(
			project(expected),
			properties.message,
		);
	} finally {
		await indices.stop();
	}
};

// Margin between range timestamps and the maturity cutoff (now - roleAge).
// The TypeScript implementation samples its own Date.now() internally, so
// timestamps must sit comfortably on one side of the cutoff to stay
// deterministic across the two calls.
const MATURITY_MARGIN_MS = 60_000;

describe("native shared-log range planner parity", () => {
	for (const resolution of ["u32", "u64"] as const) {
		let numbers: any;
		let denormalize: (value: number) => number | bigint;

		before(async () => {
			const modules = await loadSharedLog();
			numbers = modules.createNumbers(resolution);
			denormalize = modules.denormalizer(resolution);
		});

		it(`matches TypeScript hash-domain coordinate creation for ${resolution}`, async () => {
			const planner = await createRangePlanner(resolution);
			const gid = `native-coordinate-${resolution}`;
			const cursor = await hashGidCursor(resolution, gid);

			expect(planner.getGidCoordinates(gid, 3)).to.deep.equal(
				numbers.getGrid(cursor, 3),
			);
		});

		it(`matches TypeScript fallback ordering for ${resolution}`, async () => {
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0,
					width: 0.1,
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-b",
					offset: 0.9,
					width: 0.1,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: numbers.getGrid(denormalize(0.5), 2),
			});
		});

		it(`matches TypeScript intersecting and peer filtering for ${resolution}`, async () => {
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0.1,
					width: 0.4,
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-b",
					offset: 0.1,
					width: 0.4,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.2)],
				options: {
					peerFilter: new Set(["peer-b"]),
				},
			});
		});

		it(`matches TypeScript wrapped range containment for ${resolution}`, async () => {
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0.9,
					width: 0.2,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.05)],
			});
		});

		it(`matches TypeScript strict fallback exclusion for ${resolution}`, async () => {
			if (!sharedLog) {
				throw new Error("Shared log modules are not loaded");
			}
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0.49,
					width: 0.02,
					mode: sharedLog.ReplicationIntent.Strict,
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-b",
					offset: 0.7,
					width: 0.05,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
			});
		});

		it(`matches TypeScript fallback after filtered endpoint candidates for ${resolution}`, async () => {
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0.49,
					width: 0.01,
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-b",
					offset: 0.51,
					width: 0.01,
				}),
				createRange(resolution, {
					id: 3,
					hash: "peer-c",
					offset: 0.8,
					width: 0.01,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
				options: {
					peerFilter: new Set(["peer-c"]),
				},
			});
		});

		it(`matches TypeScript fallback tie-breaking for ${resolution}`, async () => {
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-c",
					offset: 0.49,
					width: 0.01,
					timestamp: 1n,
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-b",
					offset: 0.49,
					width: 0.01,
					timestamp: 0n,
				}),
				createRange(resolution, {
					id: 3,
					hash: "peer-a",
					offset: 0.49,
					width: 0.01,
					timestamp: 0n,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: numbers.getGrid(denormalize(0.5), 2),
			});
		});

		it(`matches TypeScript only-intersecting behavior for ${resolution}`, async () => {
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0.1,
					width: 0.1,
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
				options: {
					onlyIntersecting: true,
				},
			});
		});

		it(`matches TypeScript evenly spaced grid generation for ${resolution}`, async () => {
			const planner = await createRangePlanner(resolution);
			for (const [from, count] of [
				[0, 1],
				[0.13, 2],
				[0.5, 3],
				[0.875, 4],
				[0.999, 7],
			] as const) {
				expect(planner.getGrid(denormalize(from), count)).to.deep.equal(
					numbers.getGrid(denormalize(from), count),
					`getGrid(from=${from}, count=${count})`,
				);
			}
		});

		it(`matches TypeScript maturity fallback past immature intersecting ranges for ${resolution}`, async () => {
			const roleAge = 10 * 60_000;
			const now = Date.now();
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-immature",
					offset: 0.45,
					width: 0.1,
					timestamp: BigInt(now - roleAge + MATURITY_MARGIN_MS),
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-mature",
					offset: 0.7,
					width: 0.05,
					timestamp: BigInt(now - roleAge - MATURITY_MARGIN_MS),
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
				roleAge,
			});
		});

		it(`matches TypeScript boundary maturity classification around the cutoff for ${resolution}`, async () => {
			const roleAge = 10 * 60_000;
			const now = Date.now();
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-just-mature",
					offset: 0.48,
					width: 0.05,
					timestamp: BigInt(now - roleAge - MATURITY_MARGIN_MS),
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-just-immature",
					offset: 0.49,
					width: 0.05,
					timestamp: BigInt(now - roleAge + MATURITY_MARGIN_MS),
				}),
				createRange(resolution, {
					id: 3,
					hash: "peer-distant-mature",
					offset: 0.9,
					width: 0.02,
					timestamp: BigInt(now - roleAge * 2),
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: numbers.getGrid(denormalize(0.5), 2),
				roleAge,
			});
		});

		it(`matches TypeScript all-immature fallback for ${resolution}`, async () => {
			const roleAge = 10 * 60_000;
			const now = Date.now();
			const immatureTimestamp = BigInt(now - roleAge + MATURITY_MARGIN_MS);
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-a",
					offset: 0.4,
					width: 0.2,
					timestamp: immatureTimestamp,
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-b",
					offset: 0.8,
					width: 0.1,
					timestamp: immatureTimestamp,
				}),
			];

			// Cursor covered by an immature range
			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
				roleAge,
			});

			// Cursor not covered by any range, no mature fallback exists
			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.05)],
				roleAge,
			});
		});

		it(`matches TypeScript mature fallback ordering with mixed role ages for ${resolution}`, async () => {
			const roleAge = 10 * 60_000;
			const now = Date.now();
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-close-immature",
					offset: 0.49,
					width: 0.01,
					timestamp: BigInt(now - roleAge + MATURITY_MARGIN_MS),
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-mid-mature",
					offset: 0.6,
					width: 0.01,
					timestamp: BigInt(now - roleAge - MATURITY_MARGIN_MS),
				}),
				createRange(resolution, {
					id: 3,
					hash: "peer-far-mature",
					offset: 0.9,
					width: 0.01,
					timestamp: BigInt(now - roleAge * 3),
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: numbers.getGrid(denormalize(0.5), 2),
				roleAge,
			});
		});

		it(`matches TypeScript maturity fallback with peer filtering for ${resolution}`, async () => {
			const roleAge = 10 * 60_000;
			const now = Date.now();
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-immature",
					offset: 0.45,
					width: 0.1,
					timestamp: BigInt(now - roleAge + MATURITY_MARGIN_MS),
				}),
				createRange(resolution, {
					id: 2,
					hash: "peer-mature",
					offset: 0.7,
					width: 0.05,
					timestamp: BigInt(now - roleAge - MATURITY_MARGIN_MS),
				}),
			];

			// The only mature peer is filtered away, leaving no mature fallback
			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
				roleAge,
				options: {
					peerFilter: new Set(["peer-immature"]),
				},
			});
		});

		it(`matches TypeScript only-intersecting with immature ranges for ${resolution}`, async () => {
			const roleAge = 10 * 60_000;
			const now = Date.now();
			const ranges = [
				createRange(resolution, {
					id: 1,
					hash: "peer-immature",
					offset: 0.45,
					width: 0.1,
					timestamp: BigInt(now - roleAge + MATURITY_MARGIN_MS),
				}),
			];

			await expectNativeParity(resolution, {
				ranges,
				cursors: [denormalize(0.5)],
				roleAge,
				options: {
					onlyIntersecting: true,
				},
			});
		});
	}
});

describe("native shared-log range planner seeded parity sweep", () => {
	// Deterministic PRNG (mulberry32). Same seed -> same scenario every run;
	// the seed is embedded in each test title and assertion message.
	const SWEEP_SEED = 0xc0ffee;
	const SWEEP_ITERATIONS_PER_RESOLUTION = 25;

	const mulberry32 = (seed: number) => {
		let state = seed | 0;
		return () => {
			state = (state + 0x6d2b79f5) | 0;
			let t = Math.imul(state ^ (state >>> 15), 1 | state);
			t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
			return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
		};
	};

	for (const resolution of ["u32", "u64"] as const) {
		describe(resolution, () => {
			let numbers: any;
			let denormalize: (value: number) => number | bigint;

			before(async () => {
				const modules = await loadSharedLog();
				numbers = modules.createNumbers(resolution);
				denormalize = modules.denormalizer(resolution);
			});

			for (
				let iteration = 0;
				iteration < SWEEP_ITERATIONS_PER_RESOLUTION;
				iteration++
			) {
				const seed =
					SWEEP_SEED + (resolution === "u32" ? 0 : 100_000) + iteration;

				it(`matches TypeScript samples for seed ${seed} (${resolution})`, async () => {
					if (!sharedLog) {
						throw new Error("Shared log modules are not loaded");
					}
					const random = mulberry32(seed);
					const pick = <T>(values: readonly T[]): T =>
						values[
							Math.min(values.length - 1, Math.floor(random() * values.length))
						]!;

					const roleAge = pick([0, 60_000, 600_000, 3_600_000]);
					const now = Date.now();
					const timestampFor = (kind: "mature" | "immature" | "old") => {
						if (kind === "immature" && roleAge > 0) {
							// Halfway between the cutoff and now: immature by roleAge / 2
							return BigInt(now - Math.floor(roleAge / 2));
						}
						if (kind === "old") {
							return BigInt(Math.max(0, now - roleAge * 2 - 120_000));
						}
						return BigInt(
							Math.max(
								0,
								now -
									roleAge -
									MATURITY_MARGIN_MS -
									Math.floor(random() * MATURITY_MARGIN_MS),
							),
						);
					};

					const peerCount = 1 + Math.floor(random() * 5);
					const peerHashes: string[] = [];
					const ranges: AnyRange[] = [];
					let nextId = 1;
					for (let peer = 0; peer < peerCount; peer++) {
						const hash = `peer-${peer}`;
						peerHashes.push(hash);
						const rangeCount = random() < 0.3 ? 2 : 1;
						for (let rangeIndex = 0; rangeIndex < rangeCount; rangeIndex++) {
							const maturity =
								roleAge === 0
									? "mature"
									: pick(["mature", "mature", "immature", "old"] as const);
							ranges.push(
								createRange(resolution, {
									id: nextId++,
									hash,
									offset: random(),
									width: 0.01 + random() * 0.39,
									timestamp: timestampFor(maturity),
									mode:
										random() < 0.15
											? sharedLog.ReplicationIntent.Strict
											: sharedLog.ReplicationIntent.NonStrict,
								}),
							);
						}
					}

					const cursorCount = 1 + Math.floor(random() * 3);
					const cursors: Array<number | bigint> =
						random() < 0.5
							? numbers.getGrid(denormalize(random()), cursorCount)
							: Array.from({ length: cursorCount }, () =>
									denormalize(random()),
								);

					const onlyIntersecting = random() < 0.25;
					const peerFilter =
						random() < 0.3
							? new Set(peerHashes.filter(() => random() < 0.6))
							: undefined;
					const uniqueReplicators =
						random() < 0.2 ? new Set(peerHashes) : undefined;

					await expectNativeParity(resolution, {
						ranges,
						cursors,
						roleAge,
						options: {
							onlyIntersecting,
							peerFilter,
							uniqueReplicators,
						},
						// Membership and intersecting flags are compared strictly, but
						// map insertion order is not: TypeScript orders intersecting
						// matches by unordered sqlite query results (observed order-only
						// divergence at seed 12648438).
						ignoreOrder: true,
						message: `seed=${seed} resolution=${resolution} roleAge=${roleAge} peers=${peerCount} cursors=${cursors.map(String).join(",")} onlyIntersecting=${onlyIntersecting} peerFilter=${peerFilter ? [...peerFilter].join("|") : "none"} uniqueReplicators=${uniqueReplicators ? "all" : "none"}`,
					});
				});
			}
		});
	}
});
