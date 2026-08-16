import { sha256Sync, toHexString } from "@peerbit/crypto";
import deterministicStringify from "json-stringify-deterministic";
import { MAX_U32, MAX_U64, type NumberFromType } from "./integers.js";
import { type RebalanceScanPlan, ReplicationIntent } from "./ranges.js";

type Resolution = "u32" | "u64";

export interface RebalanceWorkPersistence {
	/**
	 * One adapter instance and its two named files must be exclusively owned by
	 * one RebalanceWorkStore for the store's lifetime. Runtime adapters that can
	 * be opened by several processes must provide the equivalent namespace lock.
	 */
	/** Reject an oversized file before allocating/returning its contents. */
	read(name: string, maxBytes: number): Promise<Uint8Array | undefined>;
	/** Replace and truncate the complete named file; never append. */
	write(name: string, bytes: Uint8Array): Promise<void>;
	/**
	 * Persist the exact named-file bytes plus any namespace metadata required to
	 * recover that replacement after a crash.
	 */
	durableBarrier?(name?: string): Promise<void>;
	flush?(name?: string): Promise<void>;
	close?(options?: { flush?: boolean }): Promise<void>;
}

export type RebalanceWorkDurability = "strict" | "memory";

export type RebalanceWorkLimits = Readonly<{
	maxFrameBytes: number;
	maxGeometryRanges: number;
	maxOwnedIntervals: number;
	maxHistoryMutations: number;
	maxCollisionBucket: number;
	maxIdentifierBytes: number;
}>;

export const DEFAULT_REBALANCE_WORK_LIMITS: RebalanceWorkLimits = Object.freeze(
	{
		maxFrameBytes: 4 * 1024 * 1024,
		maxGeometryRanges: 4096,
		maxOwnedIntervals: 8192,
		maxHistoryMutations: 8192,
		maxCollisionBucket: 1024,
		maxIdentifierBytes: 512,
	},
);

export const REBALANCE_WORK_FILES = Object.freeze([
	"rebalance-work.a.json",
	"rebalance-work.b.json",
] as const);

export type RebalanceWorkFence = Readonly<{
	/**
	 * Local immutable-plan binding only. This is not an authenticated placement
	 * epoch, ownership-transfer acknowledgement, or pruning authorization.
	 */
	viewId: string;
	planDigest: string;
	installSequence: bigint;
}>;

export type RebalanceCollisionBucket<R extends Resolution = Resolution> =
	Readonly<{
		hashNumber: NumberFromType<R>;
		hashes: readonly string[];
		nextIndex: number;
	}>;

export type RebalanceWorkCursor<R extends Resolution = Resolution> = Readonly<{
	taskOrdinal: number;
	afterHashNumber?: NumberFromType<R>;
	bucket?: RebalanceCollisionBucket<R>;
}>;

export type RebalanceWorkActive<R extends Resolution = Resolution> = Readonly<{
	resolution: R;
	viewId: string;
	planDigest: string;
	installSequence: bigint;
	plan: RebalanceScanPlan<R>;
	cursor: RebalanceWorkCursor<R>;
}>;

export type RebalanceWorkSnapshot = Readonly<{
	revision: bigint;
	active?: RebalanceWorkActive<"u32"> | RebalanceWorkActive<"u64">;
}>;

declare const durableWorkCommitBrand: unique symbol;

/**
 * Proof that one exact strict-store frame passed its named physical barrier.
 * This is restart metadata, not an ownership-transfer receipt or prune permit.
 */
export type DurableWorkCommit = Readonly<{
	revision: bigint;
	frameChecksum: string;
	fence?: RebalanceWorkFence;
	readonly [durableWorkCommitBrand]: true;
}>;

export type RebalanceWorkMutationResult = Readonly<{
	snapshot: RebalanceWorkSnapshot;
	/** Present only for a strict store after its physical barrier completed. */
	durableCommit?: DurableWorkCommit;
}>;

export type RebalanceWorkPlanInput<R extends Resolution> = Readonly<{
	resolution: R;
	viewId: string;
	plan: RebalanceScanPlan<R>;
}>;

type StoredQueryRange = {
	start1: string;
	end1: string;
	start2: string;
	end2: string;
	mode: "strict";
};

type StoredOwnedInterval = {
	start: string;
	end: string;
	geometryTask: number;
};

type StoredHistoryMutation = {
	rangeHash: string;
	present: boolean;
};

type StoredPlan = {
	resolution: Resolution;
	viewId: string;
	planDigest: string;
	installSequence: string;
	boundary: boolean;
	geometryRanges: StoredQueryRange[];
	ownedIntervals: StoredOwnedInterval[];
	taskCount: number;
	historyMutations: StoredHistoryMutation[];
};

type StoredCursor = {
	taskOrdinal: number;
	afterHashNumber?: string;
	bucket?: {
		hashNumber: string;
		hashes: string[];
		nextIndex: number;
	};
};

type StoredActive = {
	plan: StoredPlan;
	cursor: StoredCursor;
};

type StoredFramePayload = {
	format: typeof REBALANCE_WORK_FORMAT;
	version: 1;
	durability: RebalanceWorkDurability;
	sequence: string;
	state: "active" | "cleared";
	value: StoredActive | null;
};

type LoadedFrame = {
	sequence: bigint;
	slot: 0 | 1;
	durability?: RebalanceWorkDurability;
	active?: StoredActive;
	checksum?: string;
	durabilityConfirmed?: boolean;
	implicit?: boolean;
};

const REBALANCE_WORK_FORMAT = "peerbit-shared-log-rebalance-work" as const;
const REBALANCE_QUERY_RANGES_PER_TASK = 128;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const DECIMAL_PATTERN = /^(0|[1-9][0-9]*)$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const openPersistenceAdapters = new WeakSet<RebalanceWorkPersistence>();

const isRecord = (value: unknown): value is Record<string, unknown> =>
	value != null && typeof value === "object" && !Array.isArray(value);

const hasExactKeys = (
	value: Record<string, unknown>,
	keys: readonly string[],
) => {
	const actual = Object.keys(value).sort();
	const expected = [...keys].sort();
	return (
		actual.length === expected.length &&
		actual.every((key, index) => key === expected[index])
	);
};

const assertSafeCount = (value: unknown, name: string, maximum: number) => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < 0 ||
		value > maximum
	) {
		throw new Error(`Invalid ${name}`);
	}
	return value;
};

const assertBoundedString = (
	value: unknown,
	name: string,
	maximumBytes: number,
) => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > maximumBytes ||
		encoder.encode(value).byteLength > maximumBytes
	) {
		throw new Error(`Invalid ${name}`);
	}
	return value;
};

const assertDigest = (value: unknown, name: string) => {
	if (
		typeof value !== "string" ||
		value.length !== 64 ||
		!DIGEST_PATTERN.test(value)
	) {
		throw new Error(`Invalid ${name}`);
	}
	return value;
};

const parseDecimal = (
	value: unknown,
	name: string,
	maximum: bigint,
	options?: { positive?: boolean },
) => {
	if (
		typeof value !== "string" ||
		value.length > maximum.toString().length ||
		!DECIMAL_PATTERN.test(value)
	) {
		throw new Error(`Invalid ${name}`);
	}
	const parsed = BigInt(value);
	if (parsed > maximum || (options?.positive && parsed === 0n)) {
		throw new Error(`Invalid ${name}`);
	}
	return parsed;
};

const runtimeBigIntToDecimal = (
	value: unknown,
	name: string,
	maximum: bigint,
) => {
	if (typeof value !== "bigint" || value < 0n || value > maximum) {
		throw new Error(`Invalid ${name}`);
	}
	return value.toString();
};

const runtimeNumberToDecimal = <R extends Resolution>(
	value: NumberFromType<R>,
	resolution: R,
	name: string,
) => {
	if (resolution === "u32") {
		if (
			typeof value !== "number" ||
			!Number.isSafeInteger(value) ||
			value < 0 ||
			value > MAX_U32
		) {
			throw new Error(`Invalid ${name}`);
		}
		return String(value);
	}
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error(`Invalid ${name}`);
	}
	return value.toString();
};

const storedNumberToRuntime = <R extends Resolution>(
	value: string,
	resolution: R,
): NumberFromType<R> =>
	(resolution === "u32" ? Number(value) : BigInt(value)) as NumberFromType<R>;

const normalizeLimits = (
	limits?: Partial<RebalanceWorkLimits>,
): RebalanceWorkLimits => {
	const normalized = { ...DEFAULT_REBALANCE_WORK_LIMITS, ...limits };
	for (const [name, value] of Object.entries(normalized)) {
		if (!Number.isSafeInteger(value) || value <= 0) {
			throw new Error(`Invalid rebalance work limit: ${name}`);
		}
	}
	return normalized;
};

const planDigestBody = (plan: StoredPlan) => ({
	resolution: plan.resolution,
	viewId: plan.viewId,
	plan: {
		boundary: plan.boundary,
		geometryRanges: plan.geometryRanges,
		ownedIntervals: plan.ownedIntervals,
		taskCount: plan.taskCount,
		historyMutations: plan.historyMutations,
	},
});

const digestStoredPlan = (plan: StoredPlan) =>
	toHexString(
		sha256Sync(encoder.encode(deterministicStringify(planDigestBody(plan)))),
	);

const deriveOwnedIntervals = (
	geometryRanges: readonly StoredQueryRange[],
	maximum: bigint,
): StoredOwnedInterval[] => {
	const intervals = geometryRanges
		.flatMap((range, rangeIndex) => {
			const start1 = BigInt(range.start1);
			const end1 = BigInt(range.end1);
			const start2 = BigInt(range.start2);
			const end2 = BigInt(range.end2);
			if (start1 >= end1) {
				throw new Error("Rebalance geometry ranges must be non-empty");
			}
			const geometryTask = Math.floor(
				rangeIndex / REBALANCE_QUERY_RANGES_PER_TASK,
			);
			if (start1 === start2 && end1 === end2) {
				return [{ start: start1, end: end1, geometryTask }];
			}
			if (
				end1 !== maximum ||
				start2 !== 0n ||
				start2 >= end2 ||
				end2 > start1
			) {
				throw new Error("Invalid wrapped rebalance geometry range");
			}
			return [
				{ start: start1, end: end1, geometryTask },
				{ start: start2, end: end2, geometryTask },
			];
		})
		.sort((left, right) =>
			left.start < right.start
				? -1
				: left.start > right.start
					? 1
					: left.end < right.end
						? -1
						: left.end > right.end
							? 1
							: 0,
		);
	for (let index = 1; index < intervals.length; index++) {
		if (intervals[index - 1].end > intervals[index].start) {
			throw new Error("Rebalance geometry intervals must not overlap");
		}
	}
	return intervals.map((interval) => ({
		start: interval.start.toString(),
		end: interval.end.toString(),
		geometryTask: interval.geometryTask,
	}));
};

const normalizePlan = <R extends Resolution>(
	input: RebalanceWorkPlanInput<R>,
	installSequence: bigint,
	limits: RebalanceWorkLimits,
): StoredPlan => {
	if (input.resolution !== "u32" && input.resolution !== "u64") {
		throw new Error("Invalid rebalance work resolution");
	}
	const viewId = assertDigest(input.viewId, "rebalance view id");
	const plan = input.plan;
	if (!plan || typeof plan !== "object" || typeof plan.boundary !== "boolean") {
		throw new Error("Invalid rebalance scan plan");
	}
	if (
		!Array.isArray(plan.geometryRanges) ||
		plan.geometryRanges.length > limits.maxGeometryRanges ||
		!Array.isArray(plan.ownedIntervals) ||
		plan.ownedIntervals.length > limits.maxOwnedIntervals ||
		!Array.isArray(plan.historyMutations) ||
		plan.historyMutations.length > limits.maxHistoryMutations
	) {
		throw new Error("Rebalance scan plan exceeds configured bounds");
	}

	const geometryRanges: StoredQueryRange[] = [];
	for (let index = 0; index < plan.geometryRanges.length; index++) {
		const range = plan.geometryRanges[index];
		if (!range || range.mode !== ReplicationIntent.Strict) {
			throw new Error(`Invalid rebalance geometry range ${index}`);
		}
		geometryRanges.push({
			start1: runtimeNumberToDecimal(
				range.start1,
				input.resolution,
				`rebalance geometry range ${index} start1`,
			),
			end1: runtimeNumberToDecimal(
				range.end1,
				input.resolution,
				`rebalance geometry range ${index} end1`,
			),
			start2: runtimeNumberToDecimal(
				range.start2,
				input.resolution,
				`rebalance geometry range ${index} start2`,
			),
			end2: runtimeNumberToDecimal(
				range.end2,
				input.resolution,
				`rebalance geometry range ${index} end2`,
			),
			mode: "strict" as const,
		});
	}

	const maximum = input.resolution === "u32" ? BigInt(MAX_U32) : MAX_U64;
	const geometryTaskCount = Math.ceil(
		geometryRanges.length / REBALANCE_QUERY_RANGES_PER_TASK,
	);
	const ownedIntervals = deriveOwnedIntervals(geometryRanges, maximum);
	if (plan.ownedIntervals.length !== ownedIntervals.length) {
		throw new Error("Rebalance owned intervals do not match query geometry");
	}
	for (let index = 0; index < plan.ownedIntervals.length; index++) {
		const interval = plan.ownedIntervals[index];
		if (!interval || typeof interval !== "object") {
			throw new Error(`Invalid rebalance owned interval ${index}`);
		}
		const start = runtimeBigIntToDecimal(
			interval.start,
			`rebalance owned interval ${index} start`,
			maximum,
		);
		const end = runtimeBigIntToDecimal(
			interval.end,
			`rebalance owned interval ${index} end`,
			maximum,
		);
		const geometryTask = assertSafeCount(
			interval.geometryTask,
			`rebalance owned interval ${index} task`,
			Math.max(0, geometryTaskCount - 1),
		);
		const expected = ownedIntervals[index];
		if (
			start !== expected.start ||
			end !== expected.end ||
			geometryTask !== expected.geometryTask
		) {
			throw new Error("Rebalance owned intervals do not match query geometry");
		}
	}
	if (ownedIntervals.length > 0 && geometryTaskCount === 0) {
		throw new Error("Rebalance owned intervals require geometry tasks");
	}

	const expectedTaskCount = (plan.boundary ? 1 : 0) + geometryTaskCount;
	if (
		!Number.isSafeInteger(plan.taskCount) ||
		plan.taskCount !== expectedTaskCount
	) {
		throw new Error("Invalid rebalance task count");
	}
	const historyMutations: StoredHistoryMutation[] = [];
	for (let index = 0; index < plan.historyMutations.length; index++) {
		const mutation = plan.historyMutations[index];
		if (!mutation || typeof mutation.present !== "boolean") {
			throw new Error(`Invalid rebalance history mutation ${index} state`);
		}
		historyMutations.push({
			rangeHash: assertBoundedString(
				mutation.rangeHash,
				`rebalance history mutation ${index} hash`,
				limits.maxIdentifierBytes,
			),
			present: mutation.present,
		});
	}

	const stored: StoredPlan = {
		resolution: input.resolution,
		viewId,
		planDigest: "",
		installSequence: installSequence.toString(),
		boundary: plan.boundary,
		geometryRanges,
		ownedIntervals,
		taskCount: plan.taskCount,
		historyMutations,
	};
	stored.planDigest = digestStoredPlan(stored);
	return stored;
};

const captureFence = (fence: RebalanceWorkFence): RebalanceWorkFence => {
	const installSequence = fence?.installSequence;
	if (
		typeof installSequence !== "bigint" ||
		installSequence <= 0n ||
		installSequence > MAX_U64
	) {
		throw new Error("Invalid rebalance work fence install sequence");
	}
	return {
		viewId: assertDigest(fence?.viewId, "rebalance work fence view id"),
		planDigest: assertDigest(
			fence?.planDigest,
			"rebalance work fence plan digest",
		),
		installSequence,
	};
};

const captureCursor = <R extends Resolution>(
	cursor: RebalanceWorkCursor<R>,
	maximumBucketSize: number,
): RebalanceWorkCursor<R> => {
	if (!cursor || typeof cursor !== "object") {
		return cursor;
	}
	const bucket = cursor.bucket;
	let hashes = bucket?.hashes;
	if (Array.isArray(hashes)) {
		if (hashes.length > maximumBucketSize) {
			throw new Error("Rebalance collision bucket exceeds configured bounds");
		}
		const copied = new Array<string>(hashes.length);
		for (let index = 0; index < hashes.length; index++) {
			copied[index] = hashes[index];
		}
		hashes = copied;
	}
	return {
		taskOrdinal: cursor.taskOrdinal,
		afterHashNumber: cursor.afterHashNumber,
		bucket:
			bucket && typeof bucket === "object"
				? {
						hashNumber: bucket.hashNumber,
						hashes: hashes!,
						nextIndex: bucket.nextIndex,
					}
				: bucket,
	};
};

const validateStoredPlan = (
	value: unknown,
	frameSequence: bigint,
	limits: RebalanceWorkLimits,
): StoredPlan => {
	if (
		!isRecord(value) ||
		!hasExactKeys(value, [
			"resolution",
			"viewId",
			"planDigest",
			"installSequence",
			"boundary",
			"geometryRanges",
			"ownedIntervals",
			"taskCount",
			"historyMutations",
		])
	) {
		throw new Error("Invalid stored rebalance plan");
	}
	const resolution = value.resolution;
	if (resolution !== "u32" && resolution !== "u64") {
		throw new Error("Invalid stored rebalance resolution");
	}
	const maximum = resolution === "u32" ? BigInt(MAX_U32) : MAX_U64;
	const viewId = assertDigest(value.viewId, "stored rebalance view id");
	const planDigest = assertDigest(
		value.planDigest,
		"stored rebalance plan digest",
	);
	const installSequence = parseDecimal(
		value.installSequence,
		"stored rebalance install sequence",
		MAX_U64,
		{ positive: true },
	);
	if (installSequence > frameSequence) {
		throw new Error("Rebalance install sequence is ahead of its frame");
	}
	if (typeof value.boundary !== "boolean") {
		throw new Error("Invalid stored rebalance boundary state");
	}
	if (
		!Array.isArray(value.geometryRanges) ||
		value.geometryRanges.length > limits.maxGeometryRanges ||
		!Array.isArray(value.ownedIntervals) ||
		value.ownedIntervals.length > limits.maxOwnedIntervals ||
		!Array.isArray(value.historyMutations) ||
		value.historyMutations.length > limits.maxHistoryMutations
	) {
		throw new Error("Stored rebalance plan exceeds configured bounds");
	}
	const geometryRanges: StoredQueryRange[] = value.geometryRanges.map(
		(range, index) => {
			if (
				!isRecord(range) ||
				!hasExactKeys(range, ["start1", "end1", "start2", "end2", "mode"]) ||
				range.mode !== "strict"
			) {
				throw new Error(`Invalid stored rebalance geometry range ${index}`);
			}
			return {
				start1: parseDecimal(
					range.start1,
					`stored rebalance geometry range ${index} start1`,
					maximum,
				).toString(),
				end1: parseDecimal(
					range.end1,
					`stored rebalance geometry range ${index} end1`,
					maximum,
				).toString(),
				start2: parseDecimal(
					range.start2,
					`stored rebalance geometry range ${index} start2`,
					maximum,
				).toString(),
				end2: parseDecimal(
					range.end2,
					`stored rebalance geometry range ${index} end2`,
					maximum,
				).toString(),
				mode: "strict",
			};
		},
	);
	const geometryTaskCount = Math.ceil(
		geometryRanges.length / REBALANCE_QUERY_RANGES_PER_TASK,
	);
	const ownedIntervals = deriveOwnedIntervals(geometryRanges, maximum);
	if (value.ownedIntervals.length !== ownedIntervals.length) {
		throw new Error("Stored owned intervals do not match query geometry");
	}
	value.ownedIntervals.forEach((interval, index) => {
		if (
			!isRecord(interval) ||
			!hasExactKeys(interval, ["start", "end", "geometryTask"])
		) {
			throw new Error(`Invalid stored rebalance owned interval ${index}`);
		}
		const start = parseDecimal(
			interval.start,
			`stored rebalance owned interval ${index} start`,
			maximum,
		);
		const end = parseDecimal(
			interval.end,
			`stored rebalance owned interval ${index} end`,
			maximum,
		);
		const geometryTask = assertSafeCount(
			interval.geometryTask,
			`stored rebalance owned interval ${index} task`,
			Math.max(0, geometryTaskCount - 1),
		);
		const expected = ownedIntervals[index];
		if (
			start.toString() !== expected.start ||
			end.toString() !== expected.end ||
			geometryTask !== expected.geometryTask
		) {
			throw new Error("Stored owned intervals do not match query geometry");
		}
	});
	if (ownedIntervals.length > 0 && geometryTaskCount === 0) {
		throw new Error("Stored rebalance intervals require geometry tasks");
	}
	const expectedTaskCount = (value.boundary ? 1 : 0) + geometryTaskCount;
	if (value.taskCount !== expectedTaskCount) {
		throw new Error("Invalid stored rebalance task count");
	}
	const historyMutations: StoredHistoryMutation[] = value.historyMutations.map(
		(mutation, index) => {
			if (
				!isRecord(mutation) ||
				!hasExactKeys(mutation, ["rangeHash", "present"]) ||
				typeof mutation.present !== "boolean"
			) {
				throw new Error(`Invalid stored rebalance history mutation ${index}`);
			}
			return {
				rangeHash: assertBoundedString(
					mutation.rangeHash,
					`stored rebalance history mutation ${index} hash`,
					limits.maxIdentifierBytes,
				),
				present: mutation.present,
			};
		},
	);
	const stored: StoredPlan = {
		resolution,
		viewId,
		planDigest,
		installSequence: installSequence.toString(),
		boundary: value.boundary,
		geometryRanges,
		ownedIntervals,
		taskCount: expectedTaskCount,
		historyMutations,
	};
	if (digestStoredPlan(stored) !== planDigest) {
		throw new Error("Stored rebalance plan digest mismatch");
	}
	return stored;
};

const normalizeCursor = <R extends Resolution>(
	cursor: RebalanceWorkCursor<R>,
	plan: StoredPlan,
	limits: RebalanceWorkLimits,
): StoredCursor => {
	if (!cursor || typeof cursor !== "object") {
		throw new Error("Invalid rebalance work cursor");
	}
	const taskOrdinal = assertSafeCount(
		cursor.taskOrdinal,
		"rebalance cursor task ordinal",
		plan.taskCount,
	);
	const resolution = plan.resolution as R;
	const normalized: StoredCursor = { taskOrdinal };
	if (cursor.afterHashNumber != null) {
		normalized.afterHashNumber = runtimeNumberToDecimal(
			cursor.afterHashNumber,
			resolution,
			"rebalance cursor hash number",
		);
	}
	if (cursor.bucket) {
		if (taskOrdinal >= plan.taskCount) {
			throw new Error("Completed rebalance cursor cannot retain a bucket");
		}
		const hashNumber = runtimeNumberToDecimal(
			cursor.bucket.hashNumber,
			resolution,
			"rebalance collision bucket hash number",
		);
		if (
			normalized.afterHashNumber != null &&
			BigInt(hashNumber) <= BigInt(normalized.afterHashNumber)
		) {
			throw new Error("Rebalance collision bucket must follow the cursor");
		}
		if (
			!Array.isArray(cursor.bucket.hashes) ||
			cursor.bucket.hashes.length > limits.maxCollisionBucket
		) {
			throw new Error("Rebalance collision bucket exceeds configured bounds");
		}
		const hashes: string[] = [];
		for (let index = 0; index < cursor.bucket.hashes.length; index++) {
			const hash = cursor.bucket.hashes[index];
			hashes.push(
				assertBoundedString(
					hash,
					`rebalance collision bucket hash ${index}`,
					limits.maxIdentifierBytes,
				),
			);
		}
		for (let index = 1; index < hashes.length; index++) {
			if (hashes[index - 1] >= hashes[index]) {
				throw new Error(
					"Rebalance collision bucket hashes must be sorted and unique",
				);
			}
		}
		normalized.bucket = {
			hashNumber,
			hashes,
			nextIndex: assertSafeCount(
				cursor.bucket.nextIndex,
				"rebalance collision bucket next index",
				hashes.length,
			),
		};
	}
	if (taskOrdinal === plan.taskCount && normalized.afterHashNumber != null) {
		throw new Error("Completed rebalance cursor cannot retain scan position");
	}
	return normalized;
};

const sameOptionalDecimal = (left?: string, right?: string) => left === right;

const sameStrings = (left: readonly string[], right: readonly string[]) =>
	left.length === right.length &&
	left.every((value, index) => value === right[index]);

const assertMonotoneCursorTransition = (
	previous: StoredCursor,
	next: StoredCursor,
) => {
	if (next.taskOrdinal === previous.taskOrdinal) {
		if (previous.bucket) {
			if (next.bucket) {
				if (
					!sameOptionalDecimal(
						previous.afterHashNumber,
						next.afterHashNumber,
					) ||
					previous.bucket.hashNumber !== next.bucket.hashNumber ||
					!sameStrings(previous.bucket.hashes, next.bucket.hashes) ||
					next.bucket.nextIndex < previous.bucket.nextIndex
				) {
					throw new Error(
						"Rebalance cursor cannot replace or rewind its bucket",
					);
				}
				return;
			}
			if (
				previous.bucket.nextIndex !== previous.bucket.hashes.length ||
				next.afterHashNumber !== previous.bucket.hashNumber
			) {
				throw new Error(
					"Rebalance cursor can advance only after its bucket is complete",
				);
			}
			return;
		}

		if (!sameOptionalDecimal(previous.afterHashNumber, next.afterHashNumber)) {
			throw new Error(
				"Rebalance cursor can advance only through a frozen bucket",
			);
		}
		if (next.bucket && next.bucket.nextIndex !== 0) {
			throw new Error("A newly frozen rebalance bucket must start at zero");
		}
		return;
	}

	if (
		next.taskOrdinal !== previous.taskOrdinal + 1 ||
		previous.bucket ||
		next.bucket ||
		next.afterHashNumber != null
	) {
		throw new Error("Rebalance cursor task transition is not monotone");
	}
};

const validateStoredCursor = (
	value: unknown,
	plan: StoredPlan,
	limits: RebalanceWorkLimits,
): StoredCursor => {
	if (
		!isRecord(value) ||
		!Object.keys(value).every((key) =>
			["taskOrdinal", "afterHashNumber", "bucket"].includes(key),
		) ||
		!("taskOrdinal" in value)
	) {
		throw new Error("Invalid stored rebalance cursor");
	}
	const maximum = plan.resolution === "u32" ? BigInt(MAX_U32) : MAX_U64;
	const taskOrdinal = assertSafeCount(
		value.taskOrdinal,
		"stored rebalance cursor task ordinal",
		plan.taskCount,
	);
	const cursor: StoredCursor = { taskOrdinal };
	if ("afterHashNumber" in value) {
		cursor.afterHashNumber = parseDecimal(
			value.afterHashNumber,
			"stored rebalance cursor hash number",
			maximum,
		).toString();
	}
	if ("bucket" in value) {
		if (
			taskOrdinal >= plan.taskCount ||
			!isRecord(value.bucket) ||
			!hasExactKeys(value.bucket, ["hashNumber", "hashes", "nextIndex"]) ||
			!Array.isArray(value.bucket.hashes) ||
			value.bucket.hashes.length > limits.maxCollisionBucket
		) {
			throw new Error("Invalid stored rebalance collision bucket");
		}
		const hashNumber = parseDecimal(
			value.bucket.hashNumber,
			"stored rebalance collision bucket hash number",
			maximum,
		).toString();
		if (
			cursor.afterHashNumber != null &&
			BigInt(hashNumber) <= BigInt(cursor.afterHashNumber)
		) {
			throw new Error("Stored collision bucket must follow its cursor");
		}
		const hashes = value.bucket.hashes.map((hash, index) =>
			assertBoundedString(
				hash,
				`stored rebalance collision bucket hash ${index}`,
				limits.maxIdentifierBytes,
			),
		);
		for (let index = 1; index < hashes.length; index++) {
			if (hashes[index - 1] >= hashes[index]) {
				throw new Error(
					"Stored collision bucket hashes must be sorted and unique",
				);
			}
		}
		cursor.bucket = {
			hashNumber,
			hashes,
			nextIndex: assertSafeCount(
				value.bucket.nextIndex,
				"stored rebalance collision bucket next index",
				hashes.length,
			),
		};
	}
	if (taskOrdinal === plan.taskCount && cursor.afterHashNumber != null) {
		throw new Error("Completed stored cursor retains scan position");
	}
	return cursor;
};

const validateStoredActive = (
	value: unknown,
	frameSequence: bigint,
	limits: RebalanceWorkLimits,
): StoredActive => {
	if (!isRecord(value) || !hasExactKeys(value, ["plan", "cursor"])) {
		throw new Error("Invalid stored rebalance work state");
	}
	const plan = validateStoredPlan(value.plan, frameSequence, limits);
	return {
		plan,
		cursor: validateStoredCursor(value.cursor, plan, limits),
	};
};

const storedPlanToRuntime = <R extends Resolution>(
	plan: StoredPlan & { resolution: R },
): RebalanceScanPlan<R> => ({
	boundary: plan.boundary,
	geometryRanges: plan.geometryRanges.map((range) => ({
		start1: storedNumberToRuntime(range.start1, plan.resolution),
		end1: storedNumberToRuntime(range.end1, plan.resolution),
		start2: storedNumberToRuntime(range.start2, plan.resolution),
		end2: storedNumberToRuntime(range.end2, plan.resolution),
		mode: ReplicationIntent.Strict,
	})),
	ownedIntervals: plan.ownedIntervals.map((interval) => ({
		start: BigInt(interval.start),
		end: BigInt(interval.end),
		geometryTask: interval.geometryTask,
	})),
	taskCount: plan.taskCount,
	historyMutations: plan.historyMutations.map((mutation) => ({ ...mutation })),
});

const storedActiveToRuntime = (
	active: StoredActive,
): RebalanceWorkActive<"u32"> | RebalanceWorkActive<"u64"> => {
	const toRuntime = <R extends Resolution>(
		plan: StoredPlan & { resolution: R },
	): RebalanceWorkActive<R> => ({
		resolution: plan.resolution,
		viewId: plan.viewId,
		planDigest: plan.planDigest,
		installSequence: BigInt(plan.installSequence),
		plan: storedPlanToRuntime(plan),
		cursor: {
			taskOrdinal: active.cursor.taskOrdinal,
			afterHashNumber:
				active.cursor.afterHashNumber == null
					? undefined
					: storedNumberToRuntime(
							active.cursor.afterHashNumber,
							plan.resolution,
						),
			bucket: active.cursor.bucket
				? {
						hashNumber: storedNumberToRuntime(
							active.cursor.bucket.hashNumber,
							plan.resolution,
						),
						hashes: [...active.cursor.bucket.hashes],
						nextIndex: active.cursor.bucket.nextIndex,
					}
				: undefined,
		},
	});
	return active.plan.resolution === "u32"
		? toRuntime(active.plan as StoredPlan & { resolution: "u32" })
		: toRuntime(active.plan as StoredPlan & { resolution: "u64" });
};

const storedFence = (active: StoredActive): RebalanceWorkFence => ({
	viewId: active.plan.viewId,
	planDigest: active.plan.planDigest,
	installSequence: BigInt(active.plan.installSequence),
});

const sameFence = (left: RebalanceWorkFence, right: RebalanceWorkFence) =>
	left.viewId === right.viewId &&
	left.planDigest === right.planDigest &&
	left.installSequence === right.installSequence;

const encodeFrame = (
	sequence: bigint,
	active: StoredActive | undefined,
	durability: RebalanceWorkDurability,
	limits: RebalanceWorkLimits,
) => {
	const payloadValue: StoredFramePayload = {
		format: REBALANCE_WORK_FORMAT,
		version: 1,
		durability,
		sequence: sequence.toString(),
		state: active ? "active" : "cleared",
		value: active ?? null,
	};
	const payload = deterministicStringify(payloadValue);
	const checksum = toHexString(sha256Sync(encoder.encode(payload)));
	const bytes = encoder.encode(JSON.stringify({ payload, checksum }));
	if (bytes.byteLength > limits.maxFrameBytes) {
		throw new Error("Rebalance work frame exceeds configured byte bound");
	}
	return { bytes, checksum };
};

const decodeFrame = (
	bytes: Uint8Array,
	slot: 0 | 1,
	limits: RebalanceWorkLimits,
): LoadedFrame => {
	if (bytes.byteLength === 0 || bytes.byteLength > limits.maxFrameBytes) {
		throw new Error("Invalid rebalance work frame size");
	}
	let outer: unknown;
	try {
		outer = JSON.parse(decoder.decode(bytes));
	} catch (error) {
		throw new Error("Invalid rebalance work frame JSON", { cause: error });
	}
	if (
		!isRecord(outer) ||
		!hasExactKeys(outer, ["payload", "checksum"]) ||
		typeof outer.payload !== "string"
	) {
		throw new Error("Invalid rebalance work frame");
	}
	const checksum = assertDigest(
		outer.checksum,
		"rebalance work frame checksum",
	);
	if (toHexString(sha256Sync(encoder.encode(outer.payload))) !== checksum) {
		throw new Error("Rebalance work frame checksum mismatch");
	}
	let payload: unknown;
	try {
		payload = JSON.parse(outer.payload);
	} catch (error) {
		throw new Error("Invalid rebalance work payload JSON", { cause: error });
	}
	if (deterministicStringify(payload) !== outer.payload) {
		throw new Error("Rebalance work payload is not canonical");
	}
	if (
		!isRecord(payload) ||
		!hasExactKeys(payload, [
			"format",
			"version",
			"durability",
			"sequence",
			"state",
			"value",
		]) ||
		payload.format !== REBALANCE_WORK_FORMAT ||
		payload.version !== 1 ||
		(payload.durability !== "strict" && payload.durability !== "memory") ||
		(payload.state !== "active" && payload.state !== "cleared")
	) {
		throw new Error("Invalid rebalance work payload");
	}
	const sequence = parseDecimal(
		payload.sequence,
		"rebalance work frame sequence",
		MAX_U64,
		{ positive: true },
	);
	if (payload.state === "cleared") {
		if (payload.value !== null) {
			throw new Error("Cleared rebalance work frame retains state");
		}
		return { sequence, slot, checksum, durability: payload.durability };
	}
	return {
		sequence,
		slot,
		checksum,
		durability: payload.durability,
		active: validateStoredActive(payload.value, sequence, limits),
	};
};

/**
 * Bounded local restart state for one immutable scan plan. Revisions provide
 * stale-callback protection only inside an exclusively held persistence
 * namespace; the files do not implement cross-instance CAS or placement
 * consensus. This module deliberately carries no transfer or prune authority.
 */
export class RebalanceWorkStore {
	private frame!: LoadedFrame;
	private tail: Promise<void> = Promise.resolve();
	private closing = false;
	private closed = false;
	private closePromise?: Promise<void>;
	private poisoned = false;
	private poisonCause?: unknown;
	private ownsPersistenceLease = false;

	private constructor(
		private readonly persistence: RebalanceWorkPersistence,
		private readonly durability: RebalanceWorkDurability,
		private readonly limits: RebalanceWorkLimits,
	) {}

	static async open(properties: {
		persistence: RebalanceWorkPersistence;
		durability: RebalanceWorkDurability;
		limits?: Partial<RebalanceWorkLimits>;
	}): Promise<RebalanceWorkStore> {
		if (
			!properties.persistence ||
			typeof properties.persistence.read !== "function" ||
			typeof properties.persistence.write !== "function"
		) {
			throw new Error("Invalid rebalance work persistence");
		}
		if (
			properties.durability !== "strict" &&
			properties.durability !== "memory"
		) {
			throw new Error("Invalid rebalance work durability");
		}
		if (
			properties.durability === "strict" &&
			typeof properties.persistence.durableBarrier !== "function"
		) {
			throw new Error(
				"Strict rebalance work persistence requires a physical durability barrier",
			);
		}
		const limits = normalizeLimits(properties.limits);
		if (openPersistenceAdapters.has(properties.persistence)) {
			throw new Error("Rebalance work persistence is already open");
		}
		openPersistenceAdapters.add(properties.persistence);
		const store = new RebalanceWorkStore(
			properties.persistence,
			properties.durability,
			limits,
		);
		store.ownsPersistenceLease = true;
		try {
			store.frame = await store.load();
			await store.confirmLoadedFrame();
			return store;
		} catch (error) {
			const openError =
				error instanceof Error
					? error
					: new Error("Failed to open rebalance work store", {
							cause: error,
						});
			let closeFailed = false;
			let closeError: unknown;
			try {
				await properties.persistence.close?.({ flush: false });
			} catch (cleanupError) {
				closeFailed = true;
				closeError = cleanupError;
			} finally {
				store.closed = true;
				store.releasePersistenceLease();
			}
			if (closeFailed) {
				throw new AggregateError(
					[openError, closeError],
					"Failed to open and close rebalance work store",
				);
			}
			throw openError;
		}
	}

	snapshot(): RebalanceWorkSnapshot {
		this.throwIfUnavailable();
		return this.snapshotUnchecked();
	}

	currentDurableCommit(): DurableWorkCommit | undefined {
		this.throwIfUnavailable();
		return this.commitUnchecked();
	}

	install<R extends Resolution>(
		expectedRevision: bigint,
		input: RebalanceWorkPlanInput<R>,
	): Promise<RebalanceWorkMutationResult> {
		let capturedPlan: StoredPlan;
		try {
			// The digest excludes installSequence, so validating with a placeholder
			// also takes an immutable copy before this operation can wait in the tail.
			capturedPlan = normalizePlan(input, 1n, this.limits);
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueue(async () => {
			this.assertExpectedRevision(expectedRevision);
			const sequence = this.frame.sequence + (this.frame.implicit ? 2n : 1n);
			if (sequence > MAX_U64) {
				throw new Error("Rebalance work sequence exhausted");
			}
			const plan = {
				...capturedPlan,
				installSequence: sequence.toString(),
			};
			const active: StoredActive = {
				plan,
				cursor: { taskOrdinal: 0 },
			};
			// Do not durably advance a fresh store to its cleared baseline if the
			// requested active generation cannot itself fit within the hard bound.
			encodeFrame(sequence, active, this.durability, this.limits);
			await this.materializeBaselineIfNeeded();
			await this.writeNextFrame(active);
			return this.mutationResult();
		});
	}

	checkpoint<R extends Resolution>(
		fence: RebalanceWorkFence,
		expectedRevision: bigint,
		cursor: RebalanceWorkCursor<R>,
	): Promise<RebalanceWorkMutationResult> {
		let capturedFence: RebalanceWorkFence;
		let capturedCursor: RebalanceWorkCursor<R>;
		try {
			capturedFence = captureFence(fence);
			capturedCursor = captureCursor(cursor, this.limits.maxCollisionBucket);
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueue(async () => {
			this.assertExpectedRevision(expectedRevision);
			const active = this.assertFence(capturedFence);
			const normalized = normalizeCursor(
				capturedCursor,
				active.plan,
				this.limits,
			);
			assertMonotoneCursorTransition(active.cursor, normalized);
			await this.writeNextFrame({ plan: active.plan, cursor: normalized });
			return this.mutationResult();
		});
	}

	clear(
		fence: RebalanceWorkFence,
		expectedRevision: bigint,
	): Promise<RebalanceWorkMutationResult> {
		let capturedFence: RebalanceWorkFence;
		try {
			capturedFence = captureFence(fence);
		} catch (error) {
			return Promise.reject(error);
		}
		return this.enqueue(async () => {
			this.assertExpectedRevision(expectedRevision);
			const active = this.assertFence(capturedFence);
			if (active.cursor.taskOrdinal !== active.plan.taskCount) {
				throw new Error("Cannot clear incomplete rebalance work");
			}
			await this.writeNextFrame(undefined);
			return this.mutationResult();
		});
	}

	close(): Promise<void> {
		if (this.closePromise) {
			return this.closePromise;
		}
		this.closing = true;
		this.closePromise = (async () => {
			await this.tail;
			let closeFailed = false;
			let closeError: unknown;
			try {
				await this.persistence.close?.({ flush: false });
			} catch (error) {
				closeFailed = true;
				closeError = error;
			} finally {
				this.closed = true;
				this.releasePersistenceLease();
			}
			const poisonError = this.poisoned
				? new Error("Rebalance work store is poisoned", {
						cause: this.poisonCause,
					})
				: undefined;
			if (closeFailed && poisonError) {
				throw new AggregateError(
					[poisonError, closeError],
					"Failed to close poisoned rebalance work store",
				);
			}
			if (closeFailed) throw closeError;
			if (poisonError) throw poisonError;
		})();
		return this.closePromise;
	}

	private async load(): Promise<LoadedFrame> {
		const reads = await Promise.allSettled(
			REBALANCE_WORK_FILES.map((name) =>
				this.persistence.read(name, this.limits.maxFrameBytes),
			),
		);
		const valid: LoadedFrame[] = [];
		const errors: unknown[] = [];
		for (let slot = 0; slot < reads.length; slot++) {
			const read = reads[slot];
			if (read.status === "rejected") {
				errors.push(read.reason);
				continue;
			}
			if (read.value == null) continue;
			try {
				const frame = decodeFrame(read.value, slot as 0 | 1, this.limits);
				valid.push(frame);
			} catch (error) {
				errors.push(error);
			}
		}
		if (valid.length === 0) {
			if (errors.length > 0) {
				throw new AggregateError(
					errors,
					"No valid rebalance work generation remains",
				);
			}
			return { sequence: 0n, slot: 0, implicit: true };
		}
		valid.sort((left, right) =>
			left.sequence < right.sequence
				? -1
				: left.sequence > right.sequence
					? 1
					: left.slot - right.slot,
		);
		const highest = valid.at(-1)!;
		const tied = valid.filter((frame) => frame.sequence === highest.sequence);
		if (
			tied.length > 1 &&
			tied.some((frame) => frame.checksum !== highest.checksum)
		) {
			throw new Error("Conflicting rebalance work generations");
		}
		if (this.durability === "strict" && highest.durability !== "strict") {
			throw new Error(
				"Strict rebalance work store cannot adopt a memory generation",
			);
		}
		return highest;
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		if (this.closing || this.closed) {
			return Promise.reject(new Error("Rebalance work store is closing"));
		}
		if (this.poisoned) {
			return Promise.reject(
				new Error("Rebalance work store is poisoned", {
					cause: this.poisonCause,
				}),
			);
		}
		const result = this.tail.then(async () => {
			if (this.poisoned) {
				throw new Error("Rebalance work store is poisoned", {
					cause: this.poisonCause,
				});
			}
			return operation();
		});
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	private async materializeBaselineIfNeeded() {
		if (!this.frame.implicit) return;
		const sequence = this.frame.sequence + 1n;
		await this.writeFrame(sequence, 1, undefined);
	}

	private async confirmLoadedFrame() {
		if (
			this.durability !== "strict" ||
			this.frame.implicit ||
			!this.frame.checksum
		) {
			return;
		}
		const file = REBALANCE_WORK_FILES[this.frame.slot];
		try {
			await this.persistence.durableBarrier!(file);
		} catch (error) {
			this.poisoned = true;
			this.poisonCause = error;
			throw new Error("Failed to confirm recovered rebalance work frame", {
				cause: error,
			});
		}
		this.frame.durabilityConfirmed = true;
	}

	private async writeNextFrame(active: StoredActive | undefined) {
		if (this.frame.sequence === MAX_U64) {
			throw new Error("Rebalance work sequence exhausted");
		}
		const sequence = this.frame.sequence + 1n;
		const slot = (this.frame.slot === 0 ? 1 : 0) as 0 | 1;
		await this.writeFrame(sequence, slot, active);
	}

	private async writeFrame(
		sequence: bigint,
		slot: 0 | 1,
		active: StoredActive | undefined,
	) {
		const encoded = encodeFrame(sequence, active, this.durability, this.limits);
		const file = REBALANCE_WORK_FILES[slot];
		try {
			await this.persistence.write(file, encoded.bytes);
			if (this.durability === "strict") {
				await this.persistence.durableBarrier!(file);
			} else if (this.persistence.durableBarrier) {
				await this.persistence.durableBarrier(file);
			} else {
				await this.persistence.flush?.(file);
			}
		} catch (error) {
			this.poisoned = true;
			this.poisonCause = error;
			throw new Error("Failed to persist rebalance work frame", {
				cause: error,
			});
		}
		this.frame = {
			sequence,
			slot,
			durability: this.durability,
			active,
			checksum: encoded.checksum,
			durabilityConfirmed: this.durability === "strict",
		};
	}

	private assertExpectedRevision(expected: bigint) {
		if (typeof expected !== "bigint" || expected !== this.frame.sequence) {
			throw new Error("Stale rebalance work revision");
		}
	}

	private assertFence(fence: RebalanceWorkFence): StoredActive {
		const active = this.frame.active;
		if (!active || !sameFence(fence, storedFence(active))) {
			throw new Error("Stale rebalance work fence");
		}
		return active;
	}

	private snapshotUnchecked(): RebalanceWorkSnapshot {
		return {
			revision: this.frame.sequence,
			active: this.frame.active
				? storedActiveToRuntime(this.frame.active)
				: undefined,
		};
	}

	private commitUnchecked(): DurableWorkCommit | undefined {
		if (
			this.durability !== "strict" ||
			!this.frame.checksum ||
			!this.frame.durabilityConfirmed
		) {
			return undefined;
		}
		return {
			revision: this.frame.sequence,
			frameChecksum: this.frame.checksum,
			fence: this.frame.active ? storedFence(this.frame.active) : undefined,
		} as DurableWorkCommit;
	}

	private mutationResult(): RebalanceWorkMutationResult {
		return {
			snapshot: this.snapshotUnchecked(),
			durableCommit: this.commitUnchecked(),
		};
	}

	private throwIfUnavailable() {
		if (this.poisoned) {
			throw new Error("Rebalance work store is poisoned", {
				cause: this.poisonCause,
			});
		}
		if (this.closing || this.closed) {
			throw new Error("Rebalance work store is closing");
		}
	}

	private releasePersistenceLease() {
		if (!this.ownsPersistenceLease) return;
		this.ownsPersistenceLease = false;
		openPersistenceAdapters.delete(this.persistence);
	}
}
