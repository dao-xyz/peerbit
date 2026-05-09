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
		const [{ createNumbers, denormalizer }, ranges] = await Promise.all([
			import(integersPath),
			import(rangesPath),
		]);
		sharedLog = {
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

		expect(mapEntries(actual)).to.deep.equal(mapEntries(expected));
	} finally {
		await indices.stop();
	}
};

describe("native shared-log range planner parity", () => {
	for (const resolution of ["u32", "u64"] as const) {
		let numbers: any;
		let denormalize: (value: number) => number | bigint;

		before(async () => {
			const modules = await loadSharedLog();
			numbers = modules.createNumbers(resolution);
			denormalize = modules.denormalizer(resolution);
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
	}
});
