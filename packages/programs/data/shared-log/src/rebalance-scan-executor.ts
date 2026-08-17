import { MAX_U32, MAX_U64, type NumberFromType } from "./integers.js";
import {
	type RebalanceScanTask,
	findOwningRebalanceTask,
	getRebalanceScanTaskPage,
} from "./ranges.js";
import {
	type DurableWorkCommit,
	type RebalanceWorkActive,
	type RebalanceWorkFence,
	type RebalanceWorkMutationResult,
	type RebalanceWorkSnapshot,
	RebalanceWorkStore,
} from "./rebalance-work-store.js";

export type RebalanceScanResolution = "u32" | "u64";
type Resolution = RebalanceScanResolution;
type ResolutionNumber = NumberFromType<Resolution>;
type MaybePromise<T> = T | Promise<T>;

/**
 * One row from an exact local hash-number collision bucket. The source must
 * return the complete bucket before the executor can persist progress past it.
 */
export type RebalanceScanCandidate<R extends Resolution = Resolution> =
	Readonly<{
		hash: string;
		coordinates: readonly NumberFromType<R>[];
		assignedToRangeBoundary: boolean;
	}>;

export type RebalanceScanSourceMetrics = Readonly<{
	/** Physical/source rows inspected by this atomic operation. */
	visited: number;
	/** Complete rows returned before task ownership filtering. */
	results: number;
	/** Bytes decoded or materialized by the source operation. */
	bytes: number;
}>;

type RebalanceScanSourceResultFor<R extends Resolution> =
	| (RebalanceScanSourceMetrics &
			Readonly<{
				resolution: R;
				eof: true;
				candidates: readonly [];
			}>)
	| (RebalanceScanSourceMetrics &
			Readonly<{
				resolution: R;
				eof: false;
				hashNumber: NumberFromType<R>;
				candidates: readonly RebalanceScanCandidate<R>[];
			}>);

export type RebalanceScanSourceResult =
	| RebalanceScanSourceResultFor<"u32">
	| RebalanceScanSourceResultFor<"u64">;

export type RebalanceScanSourceLimits = Readonly<{
	maxVisited: number;
	maxResults: number;
	maxBytes: number;
	maxIdentifierBytes: number;
	maxCoordinateValues: number;
}>;

/**
 * A source implementation must atomically return either authoritative EOF or
 * a complete task-query result for one physical hash-number bucket strictly
 * after the supplied cursor. An empty non-EOF bucket is durable progress. The
 * source may use a task index to skip empty physical buckets only when it can
 * prove the seek and EOF within the same hard limits. It must reject, without
 * truncation, before any source limit is exceeded; `maxVisited` includes any
 * overflow probe. Generic Index.next() does not satisfy this contract.
 */
type RebalanceScanSourceRequestFor<R extends Resolution> = Readonly<{
	resolution: R;
	fence: RebalanceWorkFence;
	afterHashNumber?: NumberFromType<R>;
	/**
	 * Backends with a task index may use this bounded predicate. Backends that
	 * scan physical buckets may return a complete empty progress bucket.
	 */
	task: RebalanceScanTask<R>;
	excludeBoundary: boolean;
	limits: RebalanceScanSourceLimits;
	/** Cooperative deadline; hard row/byte caps remain authoritative. */
	deadline: number;
	signal?: AbortSignal;
}>;

export type RebalanceScanSourceRequest =
	| RebalanceScanSourceRequestFor<"u32">
	| RebalanceScanSourceRequestFor<"u64">;

export interface BoundedRebalanceScanSource {
	readNextCollisionBucket(
		properties: RebalanceScanSourceRequest,
	): MaybePromise<RebalanceScanSourceResult>;
}

export type RebalanceVisitKey = Readonly<{
	viewId: string;
	planDigest: string;
	installSequence: bigint;
	taskOrdinal: number;
	hashNumber: ResolutionNumber;
	hash: string;
}>;

/** Canonical scalar form suitable for a durable de-duplication key. */
export const createRebalanceVisitIdempotenceKey = (key: RebalanceVisitKey) =>
	JSON.stringify([
		"peerbit-shared-log-rebalance-visit-v1",
		key.viewId,
		key.planDigest,
		key.installSequence.toString(),
		key.taskOrdinal,
		typeof key.hashNumber,
		key.hashNumber.toString(),
		key.hash,
	]);

export type RebalanceScanVisitor = (properties: {
	/**
	 * Stable de-duplication key. A crash before checkpoint can repeat it. The
	 * visitor must point-read and revalidate a still-current candidate, or treat
	 * a missing/stale row as an idempotent no-op, before reporting completion.
	 */
	key: RebalanceVisitKey;
	/** Canonical scalar encoding of key, including decimal bigint fields. */
	idempotenceKey: string;
	/** The visitor must reject before exceeding this per-call byte allowance. */
	maxBytes: number;
	/** Cooperative deadline; one indivisible callback may overshoot it. */
	deadline: number;
	signal?: AbortSignal;
}) => MaybePromise<Readonly<{ bytes: number }>>;

export type RebalanceScanViewGuard = (
	fence: RebalanceWorkFence,
) => MaybePromise<boolean>;

export type RebalanceScanExecutorLimits = Readonly<{
	maxSourceVisited: number;
	maxSourceResults: number;
	maxSourceBytes: number;
	maxIdentifierBytes: number;
	maxCoordinateValues: number;
	maxVisitRows: number;
	maxVisitBytes: number;
	maxTickMs: number;
}>;

/** Frozen ceilings prevent configuration from turning a bounded tick off. */
export const MAX_REBALANCE_SCAN_EXECUTOR_LIMITS: RebalanceScanExecutorLimits =
	Object.freeze({
		maxSourceVisited: 1025,
		maxSourceResults: 1024,
		maxSourceBytes: 4 * 1024 * 1024,
		maxIdentifierBytes: 512,
		maxCoordinateValues: 1024 * 100,
		maxVisitRows: 128,
		maxVisitBytes: 4 * 1024 * 1024,
		maxTickMs: 10_000,
	});

export const DEFAULT_REBALANCE_SCAN_EXECUTOR_LIMITS: RebalanceScanExecutorLimits =
	Object.freeze({
		...MAX_REBALANCE_SCAN_EXECUTOR_LIMITS,
		maxVisitRows: 32,
		maxVisitBytes: 1024 * 1024,
		maxTickMs: 100,
	});

type MutationStatus = Readonly<{
	snapshot: RebalanceWorkSnapshot;
	durableCommit?: DurableWorkCommit;
}>;

export type RebalanceScanTickResult =
	| Readonly<{ status: "idle"; snapshot: RebalanceWorkSnapshot }>
	| Readonly<{ status: "complete"; snapshot: RebalanceWorkSnapshot }>
	| (MutationStatus &
			Readonly<{
				status: "bucket-frozen";
				hashNumber: ResolutionNumber;
				kept: number;
				source: RebalanceScanSourceMetrics;
			}>)
	| (MutationStatus &
			Readonly<{
				status: "task-advanced";
				source: RebalanceScanSourceMetrics;
			}>)
	| (MutationStatus &
			Readonly<{
				status: "bucket-processed";
				processed: number;
				bytes: number;
			}>)
	| (MutationStatus & Readonly<{ status: "bucket-advanced" }>);

export class RebalanceScanStoppedError extends Error {
	constructor(message = "Rebalance scan view is no longer current") {
		super(message);
		this.name = "RebalanceScanStoppedError";
	}
}

export class RebalanceScanDeadlineError extends Error {
	constructor() {
		super("Rebalance scan tick exceeded its deadline");
		this.name = "RebalanceScanDeadlineError";
	}
}

const encoder = new TextEncoder();

const assertPositiveLimit = (value: unknown, maximum: number, name: string) => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value <= 0 ||
		value > maximum
	) {
		throw new Error(`Invalid rebalance scan executor limit: ${name}`);
	}
	return value;
};

const normalizeLimits = (
	limits?: Partial<RebalanceScanExecutorLimits>,
): RebalanceScanExecutorLimits => {
	const value = { ...DEFAULT_REBALANCE_SCAN_EXECUTOR_LIMITS, ...limits };
	return Object.freeze({
		maxSourceVisited: assertPositiveLimit(
			value.maxSourceVisited,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxSourceVisited,
			"maxSourceVisited",
		),
		maxSourceResults: assertPositiveLimit(
			value.maxSourceResults,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxSourceResults,
			"maxSourceResults",
		),
		maxSourceBytes: assertPositiveLimit(
			value.maxSourceBytes,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxSourceBytes,
			"maxSourceBytes",
		),
		maxIdentifierBytes: assertPositiveLimit(
			value.maxIdentifierBytes,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxIdentifierBytes,
			"maxIdentifierBytes",
		),
		maxCoordinateValues: assertPositiveLimit(
			value.maxCoordinateValues,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxCoordinateValues,
			"maxCoordinateValues",
		),
		maxVisitRows: assertPositiveLimit(
			value.maxVisitRows,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxVisitRows,
			"maxVisitRows",
		),
		maxVisitBytes: assertPositiveLimit(
			value.maxVisitBytes,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxVisitBytes,
			"maxVisitBytes",
		),
		maxTickMs: assertPositiveLimit(
			value.maxTickMs,
			MAX_REBALANCE_SCAN_EXECUTOR_LIMITS.maxTickMs,
			"maxTickMs",
		),
	});
};

const assertMetric = (value: unknown, maximum: number, name: string) => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < 0 ||
		value > maximum
	) {
		throw new Error(`Invalid bounded rebalance source ${name}`);
	}
	return value;
};

const assertHashNumber = (
	value: unknown,
	resolution: Resolution,
	name: string,
): ResolutionNumber => {
	if (resolution === "u32") {
		if (
			typeof value !== "number" ||
			!Number.isSafeInteger(value) ||
			value < 0 ||
			value > MAX_U32
		) {
			throw new Error(`Invalid ${name}`);
		}
		return value;
	}
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error(`Invalid ${name}`);
	}
	return value;
};

const greaterThan = (left: ResolutionNumber, right: ResolutionNumber) =>
	typeof left === "bigint"
		? left > (right as bigint)
		: left > (right as number);

const assertBoundedHash = (
	value: unknown,
	maximumBytes: number,
	name: string,
) => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > maximumBytes
	) {
		throw new Error(`Invalid ${name}`);
	}
	const bytes = encoder.encode(value).byteLength;
	if (bytes > maximumBytes) {
		throw new Error(`Invalid ${name}`);
	}
	return { value, bytes };
};

const fenceFor = (active: RebalanceWorkActive): RebalanceWorkFence => ({
	viewId: active.viewId,
	planDigest: active.planDigest,
	installSequence: active.installSequence,
});

const getCurrentTask = <R extends Resolution>(
	active: RebalanceWorkActive<R>,
): RebalanceScanTask<R> => {
	const page = getRebalanceScanTaskPage(active.plan, {
		nextTask: active.cursor.taskOrdinal,
	});
	const task = page.tasks[0];
	if (!task || task.ordinal !== active.cursor.taskOrdinal) {
		throw new Error("Rebalance scan plan is missing its current task");
	}
	return task;
};

const cloneTask = <R extends Resolution>(
	task: RebalanceScanTask<R>,
): RebalanceScanTask<R> =>
	task.kind === "boundary"
		? { ...task }
		: { ...task, ranges: task.ranges.map((range) => ({ ...range })) };

const belongsToTask = <R extends Resolution>(
	candidate: RebalanceScanCandidate,
	task: RebalanceScanTask<R>,
	active: RebalanceWorkActive<R>,
) => {
	if (task.kind === "boundary") {
		return candidate.assignedToRangeBoundary;
	}
	if (active.plan.boundary && candidate.assignedToRangeBoundary) {
		return false;
	}
	return (
		findOwningRebalanceTask(
			candidate.coordinates,
			active.plan.ownedIntervals,
		) === task.geometryTask
	);
};

const mutationStatus = (
	result: RebalanceWorkMutationResult,
): MutationStatus => ({
	snapshot: result.snapshot,
	durableCommit: result.durableCommit,
});

const abortError = () => {
	const error = new Error("Rebalance scan was aborted");
	error.name = "AbortError";
	return error;
};

/**
 * Turns one durable scan cursor into small, replayable candidate visits. It is
 * deliberately not a transfer executor: completion proves only quiescent local
 * enumeration. Concurrent inserts behind the cursor require a future snapshot
 * fence/outbox, and no result here is a transfer receipt or prune authority.
 * A source without a task-aware secondary index can safely walk physical
 * buckets and return empty task results, but will make one local pass per task.
 */
export class RebalanceScanExecutor {
	private tail: Promise<void> = Promise.resolve();
	private readonly limits: RebalanceScanExecutorLimits;
	private readonly store: RebalanceWorkStore;
	private readonly source: BoundedRebalanceScanSource;
	private readonly viewGuard: RebalanceScanViewGuard;
	private readonly visit: RebalanceScanVisitor;
	private readonly now: () => number;

	constructor(properties: {
		store: RebalanceWorkStore;
		source: BoundedRebalanceScanSource;
		viewGuard: RebalanceScanViewGuard;
		visit: RebalanceScanVisitor;
		limits?: Partial<RebalanceScanExecutorLimits>;
		now?: () => number;
	}) {
		if (!properties.store || !properties.source || !properties.viewGuard) {
			throw new Error("Invalid rebalance scan executor properties");
		}
		if (typeof properties.source.readNextCollisionBucket !== "function") {
			throw new Error("Invalid bounded rebalance scan source");
		}
		if (typeof properties.viewGuard !== "function") {
			throw new Error("Invalid rebalance scan view guard");
		}
		if (typeof properties.visit !== "function") {
			throw new Error("Invalid rebalance scan visitor");
		}
		if (properties.now != null && typeof properties.now !== "function") {
			throw new Error("Invalid rebalance scan clock");
		}
		this.store = properties.store;
		this.source = properties.source;
		this.viewGuard = properties.viewGuard;
		this.visit = properties.visit;
		this.now =
			properties.now ?? (() => globalThis.performance?.now() ?? Date.now());
		this.limits = normalizeLimits(properties.limits);
	}

	/**
	 * A rejection after an external operation can represent ambiguous progress
	 * (for example, a deadline observed after a successful checkpoint). Callers
	 * must retry from `store.snapshot()` rather than replaying local assumptions.
	 */
	tick(options?: { signal?: AbortSignal }): Promise<RebalanceScanTickResult> {
		const signal = options?.signal;
		const result = this.tail.then(() => this.runTick(signal));
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	private async assertCurrent(
		fence: RebalanceWorkFence,
		deadline: number,
		signal?: AbortSignal,
	) {
		this.assertCanContinue(deadline, signal);
		const current = await this.viewGuard({ ...fence });
		this.assertCanContinue(deadline, signal);
		if (!current) throw new RebalanceScanStoppedError();
	}

	private assertCanContinue(deadline: number, signal?: AbortSignal) {
		if (signal?.aborted) throw abortError();
		const now = this.now();
		if (!Number.isFinite(now)) throw new Error("Invalid rebalance scan clock");
		if (now > deadline) throw new RebalanceScanDeadlineError();
	}

	private async guardedAwait<T>(
		fence: RebalanceWorkFence,
		deadline: number,
		signal: AbortSignal | undefined,
		operation: () => MaybePromise<T>,
	): Promise<T> {
		await this.assertCurrent(fence, deadline, signal);
		let operationFailed = false;
		let operationError: unknown;
		let value!: T;
		try {
			value = await operation();
		} catch (error) {
			operationFailed = true;
			operationError = error;
		}
		let guardFailed = false;
		let guardError: unknown;
		try {
			await this.assertCurrent(fence, deadline, signal);
		} catch (error) {
			guardFailed = true;
			guardError = error;
		}
		if (operationFailed && guardFailed) {
			throw new AggregateError(
				[operationError, guardError],
				"Rebalance scan operation and its post-await guard failed",
			);
		}
		if (operationFailed) throw operationError;
		if (guardFailed) throw guardError;
		return value;
	}

	private async checkpoint<R extends Resolution>(
		active: RebalanceWorkActive<R>,
		revision: bigint,
		cursor: RebalanceWorkActive<R>["cursor"],
		deadline: number,
		signal?: AbortSignal,
	) {
		const fence = fenceFor(active);
		return this.guardedAwait(fence, deadline, signal, () =>
			this.store.checkpoint(fence, revision, cursor),
		);
	}

	private validateSourceResult<R extends Resolution>(
		result: RebalanceScanSourceResult,
		active: RebalanceWorkActive<R>,
		task: RebalanceScanTask<R>,
	): {
		result: RebalanceScanSourceResult;
		hashes?: string[];
		hashNumber?: NumberFromType<R>;
	} {
		if (!result || typeof result !== "object") {
			throw new Error("Invalid bounded rebalance source result");
		}
		if (result.resolution !== active.resolution) {
			throw new Error("Bounded rebalance source resolution mismatch");
		}
		const visited = assertMetric(
			result.visited,
			this.limits.maxSourceVisited,
			"visited count",
		);
		const results = assertMetric(
			result.results,
			this.limits.maxSourceResults,
			"result count",
		);
		const bytes = assertMetric(
			result.bytes,
			this.limits.maxSourceBytes,
			"byte count",
		);
		if (
			!Array.isArray(result.candidates) ||
			result.candidates.length !== results
		) {
			throw new Error("Bounded rebalance source result count mismatch");
		}
		if (visited < results) {
			throw new Error(
				"Bounded rebalance source visited fewer rows than returned",
			);
		}
		if (result.eof === true) {
			if (results !== 0 || "hashNumber" in result) {
				throw new Error("Invalid bounded rebalance source EOF");
			}
			return { result: { ...result, visited, results, bytes } };
		}
		if (result.eof !== false || !("hashNumber" in result)) {
			throw new Error("Invalid bounded rebalance collision bucket");
		}
		if (visited === 0) {
			throw new Error(
				"Empty rebalance progress requires a visited source bucket",
			);
		}

		const hashNumber = assertHashNumber(
			result.hashNumber,
			active.resolution,
			"rebalance source hash number",
		) as NumberFromType<R>;
		const after = active.cursor.afterHashNumber;
		if (after != null && !greaterThan(hashNumber, after)) {
			throw new Error("Rebalance source did not advance its hash number");
		}

		let coordinateValues = 0;
		let minimumBytes = 0;
		const seen = new Set<string>();
		const matching: string[] = [];
		for (let index = 0; index < result.candidates.length; index++) {
			const candidate = result.candidates[index];
			if (!candidate || typeof candidate !== "object") {
				throw new Error(`Invalid rebalance source candidate ${index}`);
			}
			const hash = assertBoundedHash(
				candidate.hash,
				this.limits.maxIdentifierBytes,
				`rebalance source candidate ${index} hash`,
			);
			minimumBytes += hash.bytes + 1;
			if (seen.has(hash.value)) {
				throw new Error("Bounded rebalance source returned a duplicate hash");
			}
			seen.add(hash.value);
			if (
				typeof candidate.assignedToRangeBoundary !== "boolean" ||
				!Array.isArray(candidate.coordinates)
			) {
				throw new Error(`Invalid rebalance source candidate ${index} fields`);
			}
			coordinateValues += candidate.coordinates.length;
			if (coordinateValues > this.limits.maxCoordinateValues) {
				throw new Error("Bounded rebalance source exceeded coordinate count");
			}
			for (
				let coordinate = 0;
				coordinate < candidate.coordinates.length;
				coordinate++
			) {
				assertHashNumber(
					candidate.coordinates[coordinate],
					active.resolution,
					`rebalance source candidate ${index} coordinate ${coordinate}`,
				);
			}
			minimumBytes +=
				candidate.coordinates.length * (active.resolution === "u32" ? 4 : 8);
			if (belongsToTask(candidate, task, active)) matching.push(hash.value);
		}
		if (bytes < minimumBytes) {
			throw new Error(
				"Bounded rebalance source under-reported materialized bytes",
			);
		}
		matching.sort();
		return { result, hashes: matching, hashNumber };
	}

	private async runTick(
		signal?: AbortSignal,
	): Promise<RebalanceScanTickResult> {
		if (signal?.aborted) throw abortError();
		const started = this.now();
		if (!Number.isFinite(started))
			throw new Error("Invalid rebalance scan clock");
		const deadline = started + this.limits.maxTickMs;
		const snapshot = this.store.snapshot();
		const active = snapshot.active;
		if (!active) return { status: "idle", snapshot };
		const fence = fenceFor(active);
		await this.assertCurrent(fence, deadline, signal);

		if (active.cursor.taskOrdinal === active.plan.taskCount) {
			return { status: "complete", snapshot };
		}

		const bucket = active.cursor.bucket;
		if (bucket && bucket.nextIndex === bucket.hashes.length) {
			const result = await this.checkpoint(
				active,
				snapshot.revision,
				{
					taskOrdinal: active.cursor.taskOrdinal,
					afterHashNumber: bucket.hashNumber,
				},
				deadline,
				signal,
			);
			return { status: "bucket-advanced", ...mutationStatus(result) };
		}

		if (bucket) {
			let processed = 0;
			let bytes = 0;
			const end = Math.min(
				bucket.hashes.length,
				bucket.nextIndex + this.limits.maxVisitRows,
			);
			for (let index = bucket.nextIndex; index < end; index++) {
				const remainingBytes = this.limits.maxVisitBytes - bytes;
				if (remainingBytes <= 0) break;
				const key: RebalanceVisitKey = {
					...fence,
					taskOrdinal: active.cursor.taskOrdinal,
					hashNumber: bucket.hashNumber,
					hash: bucket.hashes[index],
				};
				const visitResult = await this.guardedAwait(
					fence,
					deadline,
					signal,
					() =>
						this.visit({
							key,
							idempotenceKey: createRebalanceVisitIdempotenceKey(key),
							maxBytes: remainingBytes,
							deadline,
							signal,
						}),
				);
				if (
					!visitResult ||
					typeof visitResult.bytes !== "number" ||
					!Number.isSafeInteger(visitResult.bytes) ||
					visitResult.bytes < 0 ||
					visitResult.bytes > remainingBytes
				) {
					throw new Error("Rebalance scan visitor exceeded its byte allowance");
				}
				bytes += visitResult.bytes;
				processed++;
			}
			if (processed === 0) {
				throw new Error("Rebalance scan visitor made no bounded progress");
			}
			const result = await this.checkpoint(
				active,
				snapshot.revision,
				{
					...active.cursor,
					bucket: { ...bucket, nextIndex: bucket.nextIndex + processed },
				},
				deadline,
				signal,
			);
			return {
				status: "bucket-processed",
				processed,
				bytes,
				...mutationStatus(result),
			};
		}

		const sourceRequest: RebalanceScanSourceRequest =
			active.resolution === "u32"
				? (() => {
						const typed = active as RebalanceWorkActive<"u32">;
						const task = getCurrentTask(typed);
						return {
							resolution: "u32" as const,
							fence: { ...fence },
							afterHashNumber: typed.cursor.afterHashNumber,
							task: cloneTask(task),
							excludeBoundary: typed.plan.boundary && task.kind === "geometry",
							limits: {
								maxVisited: this.limits.maxSourceVisited,
								maxResults: this.limits.maxSourceResults,
								maxBytes: this.limits.maxSourceBytes,
								maxIdentifierBytes: this.limits.maxIdentifierBytes,
								maxCoordinateValues: this.limits.maxCoordinateValues,
							},
							deadline,
							signal,
						};
					})()
				: (() => {
						const typed = active as RebalanceWorkActive<"u64">;
						const task = getCurrentTask(typed);
						return {
							resolution: "u64" as const,
							fence: { ...fence },
							afterHashNumber: typed.cursor.afterHashNumber,
							task: cloneTask(task),
							excludeBoundary: typed.plan.boundary && task.kind === "geometry",
							limits: {
								maxVisited: this.limits.maxSourceVisited,
								maxResults: this.limits.maxSourceResults,
								maxBytes: this.limits.maxSourceBytes,
								maxIdentifierBytes: this.limits.maxIdentifierBytes,
								maxCoordinateValues: this.limits.maxCoordinateValues,
							},
							deadline,
							signal,
						};
					})();
		const sourceResult = await this.guardedAwait(fence, deadline, signal, () =>
			this.source.readNextCollisionBucket(sourceRequest),
		);
		const validated =
			active.resolution === "u32"
				? this.validateSourceResult(
						sourceResult,
						active as RebalanceWorkActive<"u32">,
						getCurrentTask(active as RebalanceWorkActive<"u32">),
					)
				: this.validateSourceResult(
						sourceResult,
						active as RebalanceWorkActive<"u64">,
						getCurrentTask(active as RebalanceWorkActive<"u64">),
					);
		const source: RebalanceScanSourceMetrics = {
			visited: sourceResult.visited,
			results: sourceResult.results,
			bytes: sourceResult.bytes,
		};
		if (sourceResult.eof) {
			const result = await this.checkpoint(
				active,
				snapshot.revision,
				{ taskOrdinal: active.cursor.taskOrdinal + 1 },
				deadline,
				signal,
			);
			return { status: "task-advanced", source, ...mutationStatus(result) };
		}
		const result = await this.checkpoint(
			active,
			snapshot.revision,
			{
				...active.cursor,
				bucket: {
					hashNumber: validated.hashNumber!,
					hashes: validated.hashes!,
					nextIndex: 0,
				},
			},
			deadline,
			signal,
		);
		return {
			status: "bucket-frozen",
			hashNumber: validated.hashNumber!,
			kept: validated.hashes!.length,
			source,
			...mutationStatus(result),
		};
	}
}
