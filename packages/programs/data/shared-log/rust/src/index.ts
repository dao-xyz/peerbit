export type RangeResolution = "u32" | "u64";

export type NativeRangeSnapshotLimits = Readonly<{
	maxOwners: number;
	maxRanges: number;
	maxRangesPerOwner: number;
	/** UTF-8 bytes of the native runtime id string (normally padded base64). */
	maxRangeIdBytes: number;
	/** UTF-8 bytes of the owner hash. This is not a public-key byte limit. */
	maxOwnerHashBytes: number;
	/**
	 * Logical binary accounting: two u32 counts, then per range two u32 UTF-8
	 * lengths and strings, one u64 timestamp, five resolution-width coordinates,
	 * and one u8 mode. This is not the decimal-string/array allocation used by
	 * the WASM bridge; row and string caps bound that allocation.
	 */
	maxBytes: number;
}>;

export const MAX_NATIVE_RANGE_SNAPSHOT_LIMITS: NativeRangeSnapshotLimits =
	Object.freeze({
		maxOwners: 4096,
		maxRanges: 16_384,
		maxRangesPerOwner: 4096,
		maxRangeIdBytes: 684,
		maxOwnerHashBytes: 512,
		maxBytes: 4 * 1024 * 1024,
	});

export type NativeRangeSnapshotOverflowCode =
	| "owners"
	| "ranges"
	| "ranges-per-owner"
	| "range-id"
	| "owner-hash"
	| "bytes";

export class NativeRangeSnapshotOverflowError extends Error {
	constructor(readonly code: NativeRangeSnapshotOverflowCode) {
		super(`Native range snapshot exceeded ${code}`);
		this.name = "NativeRangeSnapshotOverflowError";
	}
}

export type NativeRangeSnapshotRangeFor<R extends RangeResolution> = Readonly<{
	idString: string;
	/** Unauthenticated owner hash copied from the local native range mirror. */
	ownerHash: string;
	timestamp: bigint;
	start1: R extends "u64" ? bigint : number;
	end1: R extends "u64" ? bigint : number;
	start2: R extends "u64" ? bigint : number;
	end2: R extends "u64" ? bigint : number;
	width: R extends "u64" ? bigint : number;
	mode: 0 | 1;
}>;

export type NativeRangeSnapshotResultFor<R extends RangeResolution> = Readonly<{
	resolution: R;
	complete: true;
	ownerCount: number;
	rangeCount: number;
	/** Logical binary bytes represented by this complete snapshot (see limits). */
	bytes: number;
	rangeIdBytes: number;
	ownerHashBytes: number;
	ranges: ReadonlyArray<NativeRangeSnapshotRangeFor<R>>;
}>;

export type NativeRangeSnapshotResult =
	| NativeRangeSnapshotResultFor<"u32">
	| NativeRangeSnapshotResultFor<"u64">;

export type NativeRebalanceCollisionBucketLimits = Readonly<{
	maxRows: number;
	maxIdentifierBytes: number;
	maxIdentifierBytesTotal: number;
	maxCoordinateValues: number;
	maxCoordinateBytes: number;
	maxBytes: number;
}>;

export const MAX_NATIVE_REBALANCE_COLLISION_BUCKET_LIMITS: NativeRebalanceCollisionBucketLimits =
	Object.freeze({
		maxRows: 1024,
		maxIdentifierBytes: 512,
		maxIdentifierBytesTotal: 4 * 1024 * 1024,
		maxCoordinateValues: 1024 * 100,
		maxCoordinateBytes: 4 * 1024 * 1024,
		maxBytes: 4 * 1024 * 1024,
	});

export type NativeRebalanceCollisionBucketOverflowCode =
	| "rows"
	| "identifier"
	| "identifier-bytes"
	| "coordinate-values"
	| "coordinate-bytes"
	| "bytes";

export class NativeRebalanceCollisionBucketOverflowError extends Error {
	constructor(readonly code: NativeRebalanceCollisionBucketOverflowCode) {
		super(`Native rebalance collision bucket exceeded ${code}`);
		this.name = "NativeRebalanceCollisionBucketOverflowError";
	}
}

type NativeRebalanceCollisionBucketResultFor<R extends RangeResolution> =
	| Readonly<{
			resolution: R;
			eof: true;
			visited: 0;
			results: 0;
			bytes: 0;
			identifierBytes: 0;
			coordinateValues: 0;
			coordinateBytes: 0;
			candidates: readonly [];
	  }>
	| Readonly<{
			resolution: R;
			eof: false;
			hashNumber: R extends "u64" ? bigint : number;
			visited: number;
			results: number;
			bytes: number;
			identifierBytes: number;
			coordinateValues: number;
			coordinateBytes: number;
			candidates: ReadonlyArray<
				Readonly<{
					hash: string;
					coordinates: ReadonlyArray<R extends "u64" ? bigint : number>;
					assignedToRangeBoundary: boolean;
				}>
			>;
	  }>;

export type NativeRebalanceCollisionBucketResult =
	| NativeRebalanceCollisionBucketResultFor<"u32">
	| NativeRebalanceCollisionBucketResultFor<"u64">;

export type NativeReplicationRange = {
	id: string;
	hash: string;
	timestamp: bigint | number | string;
	start1: bigint | number | string;
	end1: bigint | number | string;
	start2: bigint | number | string;
	end2: bigint | number | string;
	width: bigint | number | string;
	mode: number;
};

type SampleOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	onlyIntersecting?: boolean;
	uniqueReplicators?: Iterable<string>;
	peerFilter?: Iterable<string>;
};

type FullReplicaLeaderOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	includeStrict?: boolean;
	peerFilter?: Iterable<string>;
};

type MaturedPeerOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	selfHash: string;
	selfReplicating: boolean;
};

type FindLeaderOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	peerFilter?: Iterable<string>;
	expandPeerFilter?: boolean;
	selfHash?: string;
	selfReplicating?: boolean;
	fullReplicaFallback?: boolean;
	includeStrictFullReplica?: boolean;
};

type LeaderSample = {
	intersecting: boolean;
};

type LeaderPlan = {
	coordinates: Array<number | bigint>;
	leaders: Map<string, LeaderSample>;
};

type EntryAssignmentPlan = LeaderPlan & {
	assignedToRangeBoundary: boolean;
};

export type AppendDeliveryPlan = {
	hasRemoteRecipients: boolean;
	noPeerError: boolean;
	defaultSendSilent: boolean;
	sendTo: string[];
	ackTo: string[];
	silentTo: string[];
	repairTargets: string[];
	authoritativeRecipients: string[];
};

export type NativeAppendCoordinatePlan = {
	hash: string;
	hashNumber: number | bigint;
	hashNumberString?: string;
	gid: string;
	coordinates: Array<number | bigint>;
	coordinateStrings?: string[];
	assignedToRangeBoundary: boolean;
	requestedReplicas: number;
};

type AppendEntryPlan = EntryAssignmentPlan & {
	isLeader: boolean;
	delivery: AppendDeliveryPlan;
	coordinate: NativeAppendCoordinatePlan;
};

type ReceiveCoordinatePlan = EntryAssignmentPlan & {
	isLeader: boolean;
	coordinate: NativeAppendCoordinatePlan;
};

type AppendEntryBatchInput = {
	entryHash: string;
	gid: string;
	hashNumber?: bigint | number | string;
	nextHashes?: Iterable<string>;
	replicas: number;
};

type LeaderBatchInput = {
	cursors: Iterable<bigint | number | string>;
	replicas: number;
};

type LeaderGidBatchInput = {
	gid: string;
	replicas: number;
};

type LeaderGidHashBatchInput = LeaderGidBatchInput & {
	hash: string;
};

type RepairDispatchBatchEntry = {
	hash: string;
	gid: string;
	requestedReplicas: number;
	currentLeaders: Iterable<string>;
	knownGidPeers?: Iterable<string>;
	knownEntryPeers?: Iterable<string>;
};

type RepairDispatchEntryPlanBatchEntry = {
	hash: string;
	gid: string;
	requestedReplicas: number;
	coordinates: Iterable<bigint | number | string>;
	knownGidPeers?: Iterable<string>;
	knownEntryPeers?: Iterable<string>;
};

type RepairDispatchPlanInput = {
	entries: Iterable<RepairDispatchBatchEntry>;
	pendingModes: Iterable<string>;
	pendingPeersByMode: ReadonlyMap<string, Iterable<string>>;
	optimisticPeersByMode?: ReadonlyMap<
		string,
		ReadonlyMap<string, Iterable<string>>
	>;
	fullReplicaRepairCandidates?: Iterable<string>;
	fullReplicaRepairCandidateCount: number;
	selfHash: string;
};

type RepairDispatchEntryPlanInput = Omit<
	RepairDispatchPlanInput,
	"entries"
> & {
	entries: Iterable<RepairDispatchEntryPlanBatchEntry>;
};
type ResidentRepairDispatchPlanInput = Omit<
	RepairDispatchEntryPlanInput,
	"entries"
>;

type RepairDispatchPlan = Map<string, Map<string, string[]>>;

type NativeRangePlannerHandle = {
	free: () => void;
	len: () => number;
	clear: () => void;
	read_range_snapshot: (
		maxOwners: number,
		maxRanges: number,
		maxRangesPerOwner: number,
		maxRangeIdBytes: number,
		maxOwnerHashBytes: number,
		maxBytes: number,
	) => unknown[];
	put: (
		id: string,
		hash: string,
		timestamp: string,
		start1: string,
		end1: string,
		start2: string,
		end2: string,
		width: string,
		mode: number,
	) => void;
	delete: (id: string) => boolean;
	get_samples: (
		cursors: string[],
		roleAgeMs: number,
		now: string,
		onlyIntersecting: boolean,
		uniqueReplicators?: string[],
		peerFilter?: string[],
	) => unknown[];
	find_leaders: (
		cursors: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	find_leaders_batch: (
		cursorBatches: string[][],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	find_leaders_for_gid: (
		gid: string,
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_leaders_for_gid: (
		gid: string,
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[], unknown[]];
	plan_leaders_for_gids_batch: (
		gids: string[],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_local_leaders_for_gids_batch?: (
		hashes: string[],
		gids: string[],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => string[];
	plan_repair_dispatch: (
		entryHashes: string[],
		entryGids: string[],
		entryRequestedReplicas: number[],
		currentLeaderBatches: string[][],
		knownGidPeerBatches: string[][],
		knownEntryPeerBatches: string[][],
		pendingModes: string[],
		pendingPeersByMode: string[][],
		optimisticPeersByMode: string[][][],
		fullReplicaRepairCandidates: string[],
		fullReplicaRepairCandidateCount: number,
		selfHash: string,
	) => unknown[];
	plan_repair_dispatch_for_entries: (
		entryHashes: string[],
		entryGids: string[],
		entryRequestedReplicas: number[],
		entryCoordinateBatches: string[][],
		knownGidPeerBatches: string[][],
		knownEntryPeerBatches: string[][],
		pendingModes: string[],
		pendingPeersByMode: string[][],
		optimisticPeersByMode: string[][][],
		fullReplicaRepairCandidates: string[],
		fullReplicaRepairCandidateCount: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	get_full_replica_leaders: (
		replicas: number,
		roleAgeMs: number,
		now: string,
		includeStrict: boolean,
		peerFilter?: string[],
	) => unknown[] | undefined;
	include_matured_peers: (
		peerFilter: string[] | undefined,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		includeSelf: boolean,
	) => unknown[] | undefined;
	get_grid: (from: string, count: number) => unknown[];
	get_gid_coordinates: (gid: string, count: number) => unknown[];
};

type NativeSharedLogStateHandle = {
	len: () => number;
	clear: () => void;
	read_range_snapshot: NativeRangePlannerHandle["read_range_snapshot"];
	/** @internal */
	full_replica_candidates_for: (
		minReplicas: number,
		selfHash: string,
	) => string[];
	put: NativeRangePlannerHandle["put"];
	delete: NativeRangePlannerHandle["delete"];
	put_entry_coordinates: (
		hash: string,
		gid: string,
		hashNumber: string,
		coordinates: string[],
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
	) => void;
	delete_entry_coordinates: (hash: string) => boolean;
	get_entry_coordinates: (hash: string) => unknown[] | undefined;
	entry_coordinate_hashes: () => string[];
	entry_hashes_for_hash_numbers: (hashNumbers: string[]) => unknown[];
	entry_hashes_for_hash_numbers_u64?: (
		hashNumbers: BigUint64Array,
	) => unknown[];
	entry_hashes_for_hash_numbers_flat_u64?: (
		hashNumbers: BigUint64Array,
	) => string[];
	entry_hash_numbers_in_range: (
		start1: string,
		end1: string,
		start2: string,
		end2: string,
	) => unknown[];
	entry_hash_numbers_in_range_u64?: (
		start1: string,
		end1: string,
		start2: string,
		end2: string,
	) => BigUint64Array;
	read_next_rebalance_collision_bucket: (
		afterHashNumber: string | undefined,
		maxRows: number,
		maxIdentifierBytes: number,
		maxIdentifierBytesTotal: number,
		maxCoordinateValues: number,
		maxCoordinateBytes: number,
		maxBytes: number,
	) => unknown[];
	commit_entry_coordinates: (
		hash: string,
		gid: string,
		hashNumber: string,
		coordinates: string[],
		nextHashes: string[],
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
	) => void;
	commit_entry_coordinates_batch?: (
		hashes: string[],
		gids: string[],
		hashNumbers: string[],
		coordinateBatches: string[][],
		nextHashBatches: string[][],
		assignedToRangeBoundaries: Uint8Array,
		requestedReplicas: number[],
	) => void;
	count_entry_coordinates_in_ranges: (
		start1: string[],
		end1: string[],
		start2: string[],
		end2: string[],
		includeAssignedToRangeBoundary: boolean,
	) => number;
	delete_entry_coordinates_batch: (hashes: string[]) => void;
	clear_entry_coordinates: () => void;
	add_gid_peers: (gid: string, peers: string[], reset: boolean) => number;
	remove_gid_peer: (peer: string, gid?: string) => void;
	remove_gid_peers?: (peer: string, gids: string[]) => void;
	delete_gid_peers: (gid: string) => boolean;
	clear_gid_peers: () => void;
	mark_entries_known_by_peer: (hashes: string[], peer: string) => void;
	remove_entries_known_by_peer: (hashes: string[], peer: string) => void;
	remove_peer_from_entry_known_peers: (peer: string) => void;
	clear_entry_known_peers: () => void;
	plan_entry_leaders_for_gid: NativeRangePlannerHandle["plan_leaders_for_gid"];
	plan_entry_assignment_for_gid: (
		gid: string,
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[], unknown[], boolean];
	plan_local_append_for_gid: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[], unknown[], boolean, boolean, unknown[]];
	plan_local_append_for_gid_compact?: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[] | undefined, boolean, boolean, unknown[]];
	commit_local_append_for_gid_compact?: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		deleteHashes: string[],
		replicas: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[] | undefined, boolean, boolean, unknown[]];
	plan_append_leaders_for_delivery: (
		leaders: unknown[],
		fullReplicaCandidates: string[],
		minReplicas: number,
	) => unknown[];
	plan_append_delivery: (
		leaders: unknown[],
		fallbackRecipients: string[],
		minReplicas: number,
		selfHash: string,
		isLeader: boolean,
		deliveryEnabled: boolean,
		reliabilityAck: boolean,
		minAcks: number | undefined,
		requireRecipients: boolean,
	) => [boolean, boolean, boolean, string[], string[], string[], string[], string[]];
	plan_append_for_gid: (
		entryHash: string,
		gid: string,
		hashNumber: string,
		nextHashes: string[],
		replicas: number,
		fullReplicaCandidates: string[],
		fallbackRecipients: string[],
		deliverySelfHash: string,
		deliveryEnabled: boolean,
		reliabilityAck: boolean,
		minAcks: number | undefined,
		requireRecipients: boolean,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [
		unknown[],
		unknown[],
		boolean,
		boolean,
		[boolean, boolean, boolean, string[], string[], string[], string[], string[]],
		unknown[],
	];
	plan_append_for_gids_batch: (
		entryHashes: string[],
		gids: string[],
		hashNumbers: string[],
		nextHashBatches: string[][],
		replicaCounts: number[],
		fullReplicaCandidates: string[],
		fallbackRecipients: string[],
		deliverySelfHash: string,
		deliveryEnabled: boolean,
		reliabilityAck: boolean,
		minAcks: number | undefined,
		requireRecipients: boolean,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [
		unknown[],
		unknown[],
		boolean,
		boolean,
		[boolean, boolean, boolean, string[], string[], string[], string[], string[]],
		unknown[],
	][];
	plan_receive_coordinates_for_gids_batch: (
		entryHashes: string[],
		gids: string[],
		hashNumbers: string[],
		nextHashBatches: string[][],
		replicaCounts: number[],
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => [unknown[], unknown[], boolean, boolean, unknown[]][];
	plan_repair_dispatch_for_entries: (
		entryHashes: string[],
		entryGids: string[],
		entryRequestedReplicas: number[],
		entryCoordinateBatches: string[][],
		pendingModes: string[],
		pendingPeersByMode: string[][],
		optimisticPeersByMode: string[][][],
		fullReplicaRepairCandidates: string[],
		fullReplicaRepairCandidateCount: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
	plan_repair_dispatch_for_resident_entries: (
		pendingModes: string[],
		pendingPeersByMode: string[][],
		optimisticGidsByMode: string[][],
		optimisticPeersByGidByMode: string[][][],
		fullReplicaRepairCandidates: string[],
		fullReplicaRepairCandidateCount: number,
		roleAgeMs: number,
		now: string,
		peerFilter: string[] | undefined,
		expandPeerFilter: boolean,
		selfHash: string,
		includeSelf: boolean,
		fullReplicaFallback: boolean,
		includeStrictFullReplica: boolean,
	) => unknown[];
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeRangePlanner: new (resolution: string) => NativeRangePlannerHandle;
	NativeSharedLogState: new (resolution: string) => NativeSharedLogStateHandle;
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;
let wasmInitPromise: Promise<void> | undefined;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		// Keep this import lazy, but leave the relative specifier visible to
		// bundlers so they emit the wasm-bindgen glue as a real browser chunk.
		// Node resolves the same path against dist/src/index.js.
		wasmModulePromise = import(
			"../wasm/shared_log_rust.js"
		) as unknown as Promise<WasmModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		wasmInitPromise ??= (async () => {
			const processLike = (
				globalThis as { process?: { versions?: { node?: string } } }
			).process;
			if (processLike?.versions?.node) {
				const fsPromises = "fs/promises";
				const { readFile } = (await import(
					/* @vite-ignore */ fsPromises
				)) as typeof import("fs/promises");
				const bytes = await readFile(
					new URL("../wasm/shared_log_rust_bg.wasm", import.meta.url),
				);
				wasm.initSync({ module: bytes });
			} else {
				await wasm.default({
					module_or_path: new URL(
						"../wasm/shared_log_rust_bg.wasm",
						import.meta.url,
					),
				});
			}
			wasmInitialized = true;
		})();
	}
	await wasmInitPromise;

	return wasm;
};

const asIntegerString = (value: bigint | number | string) =>
	typeof value === "bigint"
		? value.toString()
		: typeof value === "number"
			? Math.trunc(value).toString()
			: value;

const MAX_NATIVE_REBALANCE_U32 = 0xffff_ffffn;
const MAX_NATIVE_REBALANCE_U64 = 0xffff_ffff_ffff_ffffn;
const canonicalUnsignedInteger = /^(0|[1-9][0-9]*)$/;
const nativeRangeSnapshotEncoder = new TextEncoder();

const rangeSnapshotLimits = (
	limits: NativeRangeSnapshotLimits,
): NativeRangeSnapshotLimits => {
	if (!limits || typeof limits !== "object") {
		throw new Error("Invalid native range snapshot limits");
	}
	const out = {} as Record<keyof NativeRangeSnapshotLimits, number>;
	for (const name of Object.keys(MAX_NATIVE_RANGE_SNAPSHOT_LIMITS) as Array<
		keyof NativeRangeSnapshotLimits
	>) {
		const value = limits[name];
		if (
			!Number.isSafeInteger(value) ||
			value <= 0 ||
			value > MAX_NATIVE_RANGE_SNAPSHOT_LIMITS[name]
		) {
			throw new Error(`Invalid native range snapshot limit: ${name}`);
		}
		out[name] = value;
	}
	return Object.freeze(out) as NativeRangeSnapshotLimits;
};

const rangeSnapshotOverflowCodes = new Set<NativeRangeSnapshotOverflowCode>([
	"owners",
	"ranges",
	"ranges-per-owner",
	"range-id",
	"owner-hash",
	"bytes",
]);

const rangeSnapshotCount = (value: unknown): number => {
	if (!Number.isSafeInteger(value) || (value as number) < 0) {
		throw new Error("Invalid native range snapshot result");
	}
	return value as number;
};

const rangeSnapshotString = (
	value: unknown,
	maximumBytes: number,
): { value: string; encoded: Uint8Array } => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > maximumBytes
	) {
		throw new Error("Invalid native range snapshot result");
	}
	const encoded = nativeRangeSnapshotEncoder.encode(value);
	if (encoded.byteLength > maximumBytes) {
		throw new Error("Invalid native range snapshot result");
	}
	return { value, encoded };
};

const rangeSnapshotUnsigned = (value: unknown): bigint => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > 20 ||
		!canonicalUnsignedInteger.test(value)
	) {
		throw new Error("Invalid native range snapshot result");
	}
	const parsed = BigInt(value);
	if (parsed > MAX_NATIVE_REBALANCE_U64) {
		throw new Error("Invalid native range snapshot result");
	}
	return parsed;
};

type ParsedNativeRangeSnapshotRow = {
	idString: string;
	ownerHash: string;
	timestamp: bigint;
	start1: bigint;
	end1: bigint;
	start2: bigint;
	end2: bigint;
	width: bigint;
	mode: 0 | 1;
	idBytes: number;
	ownerHashBytes: number;
	idOrderBytes: Uint8Array;
	ownerHashOrderBytes: Uint8Array;
};

const compareNativeRangeSnapshotBytes = (
	left: Uint8Array,
	right: Uint8Array,
): number => {
	const length = Math.min(left.byteLength, right.byteLength);
	for (let index = 0; index < length; index++) {
		if (left[index]! < right[index]!) return -1;
		if (left[index]! > right[index]!) return 1;
	}
	return left.byteLength - right.byteLength;
};

const compareNativeRangeSnapshotRows = (
	left: ParsedNativeRangeSnapshotRow,
	right: ParsedNativeRangeSnapshotRow,
): number => {
	const ownerOrder = compareNativeRangeSnapshotBytes(
		left.ownerHashOrderBytes,
		right.ownerHashOrderBytes,
	);
	if (ownerOrder !== 0) return ownerOrder;
	const idOrder = compareNativeRangeSnapshotBytes(
		left.idOrderBytes,
		right.idOrderBytes,
	);
	if (idOrder !== 0) return idOrder;
	for (const [a, b] of [
		[left.timestamp, right.timestamp],
		[left.start1, right.start1],
		[left.end1, right.end1],
		[left.start2, right.start2],
		[left.end2, right.end2],
		[left.width, right.width],
	] as const) {
		if (a < b) return -1;
		if (a > b) return 1;
	}
	return left.mode - right.mode;
};

const nativeRangeSnapshotGeometryIsValid = (
	resolution: RangeResolution,
	range: ParsedNativeRangeSnapshotRow,
): boolean => {
	const maximum =
		resolution === "u32" ? MAX_NATIVE_REBALANCE_U32 : MAX_NATIVE_REBALANCE_U64;
	if (
		[range.start1, range.end1, range.start2, range.end2, range.width].some(
			(value) => value > maximum,
		)
	) {
		return false;
	}
	const unscaledEnd = range.start1 + range.width;
	const expectedEnd1 = unscaledEnd < maximum ? unscaledEnd : maximum;
	const [expectedStart2, expectedEnd2] =
		unscaledEnd > maximum
			? [
					0n,
					range.width === maximum
						? range.start1 % maximum
						: unscaledEnd % maximum,
				]
			: [range.start1, expectedEnd1];
	if (
		range.end1 !== expectedEnd1 ||
		range.start2 !== expectedStart2 ||
		range.end2 !== expectedEnd2
	) {
		return false;
	}
	const reconstructedWidth =
		range.end1 -
		range.start1 +
		(range.end2 < range.end1 ? range.end2 - range.start2 : 0n);
	return reconstructedWidth === range.width;
};

const rangeSnapshotFromRow = (
	resolution: RangeResolution,
	rowValue: unknown,
	limits: NativeRangeSnapshotLimits,
): NativeRangeSnapshotResult => {
	if (!Array.isArray(rowValue)) {
		throw new Error("Invalid native range snapshot result");
	}
	if (rowValue[0] === 2) {
		if (rowValue.length !== 2) {
			throw new Error("Invalid native range snapshot overflow");
		}
		const code = rowValue[1] as NativeRangeSnapshotOverflowCode;
		if (!rangeSnapshotOverflowCodes.has(code)) {
			throw new Error("Invalid native range snapshot overflow");
		}
		throw new NativeRangeSnapshotOverflowError(code);
	}
	if (rowValue.length !== 7 || rowValue[0] !== 1) {
		throw new Error("Invalid native range snapshot result");
	}
	const ownerCount = rangeSnapshotCount(rowValue[1]);
	const rangeCount = rangeSnapshotCount(rowValue[2]);
	const bytes = rangeSnapshotCount(rowValue[3]);
	const expectedRangeIdBytes = rangeSnapshotCount(rowValue[4]);
	const expectedOwnerHashBytes = rangeSnapshotCount(rowValue[5]);
	const rows = rowValue[6];
	if (
		!Array.isArray(rows) ||
		rows.length !== rangeCount ||
		ownerCount > limits.maxOwners ||
		rangeCount > limits.maxRanges ||
		bytes > limits.maxBytes
	) {
		throw new Error("Invalid native range snapshot result");
	}

	const parsed: ParsedNativeRangeSnapshotRow[] = [];
	const ids = new Set<string>();
	const ownerCounts = new Map<string, number>();
	let rangeIdBytes = 0;
	let ownerHashBytes = 0;
	let logicalBytes = 8;
	const coordinateWidth = resolution === "u32" ? 4 : 8;
	const fixedRangeBytes = 4 + 4 + 8 + 5 * coordinateWidth + 1;
	for (let index = 0; index < rangeCount; index++) {
		if (!Object.prototype.hasOwnProperty.call(rows, index)) {
			throw new Error("Invalid native range snapshot result");
		}
		const row = rows[index];
		if (!Array.isArray(row) || row.length !== 9) {
			throw new Error("Invalid native range snapshot result");
		}
		const id = rangeSnapshotString(row[0], limits.maxRangeIdBytes);
		const owner = rangeSnapshotString(row[1], limits.maxOwnerHashBytes);
		if (ids.has(id.value)) {
			throw new Error("Invalid native range snapshot result");
		}
		ids.add(id.value);
		const mode = row[8];
		if (mode !== 0 && mode !== 1) {
			throw new Error("Invalid native range snapshot result");
		}
		const item: ParsedNativeRangeSnapshotRow = {
			idString: id.value,
			ownerHash: owner.value,
			timestamp: rangeSnapshotUnsigned(row[2]),
			start1: rangeSnapshotUnsigned(row[3]),
			end1: rangeSnapshotUnsigned(row[4]),
			start2: rangeSnapshotUnsigned(row[5]),
			end2: rangeSnapshotUnsigned(row[6]),
			width: rangeSnapshotUnsigned(row[7]),
			mode,
			idBytes: id.encoded.byteLength,
			ownerHashBytes: owner.encoded.byteLength,
			idOrderBytes: id.encoded,
			ownerHashOrderBytes: owner.encoded,
		};
		if (!nativeRangeSnapshotGeometryIsValid(resolution, item)) {
			throw new Error("Invalid native range snapshot result");
		}
		const ownerRangeCount = (ownerCounts.get(item.ownerHash) ?? 0) + 1;
		if (ownerRangeCount > limits.maxRangesPerOwner) {
			throw new Error("Invalid native range snapshot result");
		}
		ownerCounts.set(item.ownerHash, ownerRangeCount);
		if (ownerCounts.size > limits.maxOwners) {
			throw new Error("Invalid native range snapshot result");
		}
		rangeIdBytes += item.idBytes;
		ownerHashBytes += item.ownerHashBytes;
		logicalBytes += fixedRangeBytes + item.idBytes + item.ownerHashBytes;
		if (
			!Number.isSafeInteger(rangeIdBytes) ||
			!Number.isSafeInteger(ownerHashBytes) ||
			!Number.isSafeInteger(logicalBytes) ||
			logicalBytes > limits.maxBytes
		) {
			throw new Error("Invalid native range snapshot result");
		}
		if (
			parsed.length > 0 &&
			compareNativeRangeSnapshotRows(parsed[parsed.length - 1]!, item) >= 0
		) {
			throw new Error("Invalid native range snapshot result");
		}
		parsed.push(item);
	}
	if (
		ownerCounts.size !== ownerCount ||
		rangeIdBytes !== expectedRangeIdBytes ||
		ownerHashBytes !== expectedOwnerHashBytes ||
		logicalBytes !== bytes
	) {
		throw new Error("Invalid native range snapshot result");
	}

	const ranges = Object.freeze(
		parsed.map((range) =>
			Object.freeze({
				idString: range.idString,
				ownerHash: range.ownerHash,
				timestamp: range.timestamp,
				start1: resolution === "u32" ? Number(range.start1) : range.start1,
				end1: resolution === "u32" ? Number(range.end1) : range.end1,
				start2: resolution === "u32" ? Number(range.start2) : range.start2,
				end2: resolution === "u32" ? Number(range.end2) : range.end2,
				width: resolution === "u32" ? Number(range.width) : range.width,
				mode: range.mode,
			}),
		),
	);
	return Object.freeze({
		resolution,
		complete: true,
		ownerCount,
		rangeCount,
		bytes,
		rangeIdBytes,
		ownerHashBytes,
		ranges,
	}) as NativeRangeSnapshotResult;
};

const rebalanceCollisionBucketCursor = (
	resolution: RangeResolution,
	value: bigint | number | string | undefined,
): string | undefined => {
	if (value == null) return undefined;
	const maximum =
		resolution === "u32" ? MAX_NATIVE_REBALANCE_U32 : MAX_NATIVE_REBALANCE_U64;
	let parsed: bigint;
	if (typeof value === "string") {
		const maximumDigits = resolution === "u32" ? 10 : 20;
		if (
			value.length === 0 ||
			value.length > maximumDigits ||
			!canonicalUnsignedInteger.test(value)
		) {
			throw new Error("Invalid native rebalance collision bucket cursor");
		}
		parsed = BigInt(value);
	} else if (typeof value === "bigint") {
		parsed = value;
	} else {
		if (!Number.isSafeInteger(value) || value < 0) {
			throw new Error("Invalid native rebalance collision bucket cursor");
		}
		parsed = BigInt(value);
	}
	if (parsed < 0n || parsed > maximum) {
		throw new Error("Invalid native rebalance collision bucket cursor");
	}
	return parsed.toString();
};

const rebalanceCollisionBucketLimits = (
	limits: NativeRebalanceCollisionBucketLimits,
): NativeRebalanceCollisionBucketLimits => {
	if (!limits || typeof limits !== "object") {
		throw new Error("Invalid native rebalance collision bucket limits");
	}
	const out = {} as Record<keyof NativeRebalanceCollisionBucketLimits, number>;
	for (const name of Object.keys(
		MAX_NATIVE_REBALANCE_COLLISION_BUCKET_LIMITS,
	) as Array<keyof NativeRebalanceCollisionBucketLimits>) {
		const value = limits[name];
		if (
			!Number.isSafeInteger(value) ||
			value <= 0 ||
			value > MAX_NATIVE_REBALANCE_COLLISION_BUCKET_LIMITS[name]
		) {
			throw new Error(
				`Invalid native rebalance collision bucket limit: ${name}`,
			);
		}
		out[name] = value;
	}
	return out as NativeRebalanceCollisionBucketLimits;
};

const iterableToArray = <T>(values?: Iterable<T>): T[] => {
	if (!values) {
		return [];
	}
	return Array.isArray(values) ? values : [...values];
};

const optionalIterableToArray = <T>(values?: Iterable<T>): T[] | undefined => {
	if (!values) {
		return undefined;
	}
	return Array.isArray(values) ? values : [...values];
};

const rowsToSamples = (rows: unknown[]): Map<string, LeaderSample> => {
	const out = new Map<string, LeaderSample>();
	for (const row of rows) {
		const [hash, intersecting] = row as [string, boolean];
		out.set(hash, { intersecting });
	}
	return out;
};

const rowsToNumbers = (
	resolution: RangeResolution,
	rows: unknown[],
): Array<number | bigint> =>
	rows.map((row) => {
		const value = row as string;
		return resolution === "u64" ? BigInt(value) : Number(value);
	});

const rowsToHashNumberMap = (rows: unknown[]): Map<bigint, string[]> => {
	const out = new Map<bigint, string[]>();
	for (const row of rows) {
		const [hashNumber, hashes] = row as [string, string[]];
		out.set(BigInt(hashNumber), hashes);
	}
	return out;
};

const rebalanceCollisionBucketOverflowCodes =
	new Set<NativeRebalanceCollisionBucketOverflowCode>([
		"rows",
		"identifier",
		"identifier-bytes",
		"coordinate-values",
		"coordinate-bytes",
		"bytes",
	]);

const rebalanceCollisionBucketFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeRebalanceCollisionBucketResult => {
	const status = row[0];
	if (status === 2) {
		const code = row[1] as NativeRebalanceCollisionBucketOverflowCode;
		if (!rebalanceCollisionBucketOverflowCodes.has(code)) {
			throw new Error("Invalid native rebalance collision bucket overflow");
		}
		throw new NativeRebalanceCollisionBucketOverflowError(code);
	}
	if (status === 0) {
		return {
			resolution,
			eof: true,
			visited: 0,
			results: 0,
			bytes: 0,
			identifierBytes: 0,
			coordinateValues: 0,
			coordinateBytes: 0,
			candidates: [],
		} as NativeRebalanceCollisionBucketResult;
	}
	if (status !== 1 || !Array.isArray(row[6])) {
		throw new Error("Invalid native rebalance collision bucket result");
	}
	const candidates = row[6].map((value) => {
		const [hash, coordinateRows, assignedToRangeBoundary] = value as [
			string,
			unknown[],
			boolean,
		];
		return {
			hash,
			coordinates: rowsToNumbers(resolution, coordinateRows),
			assignedToRangeBoundary,
		};
	});
	return {
		resolution,
		eof: false,
		hashNumber: rowsToNumbers(resolution, [row[1]])[0]!,
		visited: candidates.length,
		results: candidates.length,
		bytes: row[2] as number,
		identifierBytes: row[3] as number,
		coordinateValues: row[4] as number,
		coordinateBytes: row[5] as number,
		candidates,
	} as NativeRebalanceCollisionBucketResult;
};

const appendCoordinatePlanFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeAppendCoordinatePlan => {
	const [
		hash,
		hashNumber,
		gid,
		coordinateRows,
		assignedToRangeBoundary,
		requestedReplicas,
	] = row as [string, unknown, string, unknown[], boolean, number];
	const coordinateStrings = coordinateRows.map((coordinate) =>
		String(coordinate),
	);
	return {
		hash,
		hashNumber: rowsToNumbers(resolution, [hashNumber])[0]!,
		hashNumberString:
			typeof hashNumber === "string" ? hashNumber : String(hashNumber),
		gid,
		coordinates: rowsToNumbers(resolution, coordinateStrings),
		coordinateStrings,
		assignedToRangeBoundary,
		requestedReplicas,
	};
};

const findLeaderArguments = (options?: FindLeaderOptions): [
	number,
	string,
	string[] | undefined,
	boolean,
	string,
	boolean,
	boolean,
	boolean,
] => [
	options?.roleAge ?? 0,
	asIntegerString(options?.now ?? Date.now()),
	optionalIterableToArray(options?.peerFilter),
	options?.expandPeerFilter === true,
	options?.selfHash ?? "",
	options?.selfReplicating === true,
	options?.fullReplicaFallback === true,
	options?.includeStrictFullReplica !== false,
];

const rowsToRepairDispatchPlan = (rows: unknown[]): RepairDispatchPlan => {
	const plan: RepairDispatchPlan = new Map();
	for (const row of rows) {
		const [mode, target, hashes] = row as [string, string, string[]];
		let targets = plan.get(mode);
		if (!targets) {
			targets = new Map();
			plan.set(mode, targets);
		}
		targets.set(target, hashes);
	}
	return plan;
};

const samplesToRows = (leaders: ReadonlyMap<string, LeaderSample>): unknown[] =>
	[...leaders].map(([hash, sample]) => [hash, sample.intersecting]);

const appendDeliveryPlanFromRow = (
	row: [
		boolean,
		boolean,
		boolean,
		string[],
		string[],
		string[],
		string[],
		string[],
	],
): AppendDeliveryPlan => ({
	hasRemoteRecipients: row[0],
	noPeerError: row[1],
	defaultSendSilent: row[2],
	sendTo: row[3],
	ackTo: row[4],
	silentTo: row[5],
	repairTargets: row[6],
	authoritativeRecipients: row[7],
});

export class SharedLogRangePlanner {
	private closed = false;

	private constructor(
		private readonly native: NativeRangePlannerHandle,
		private readonly resolution: RangeResolution,
	) {}

	private handle(): NativeRangePlannerHandle {
		if (this.closed) {
			throw new Error("SharedLogRangePlanner is closed");
		}
		return this.native;
	}

	static async create(
		resolution: RangeResolution,
	): Promise<SharedLogRangePlanner> {
		const wasm = await loadWasm();
		return new SharedLogRangePlanner(
			new wasm.NativeRangePlanner(resolution),
			resolution,
		);
	}

	get length(): number {
		return this.handle().len();
	}

	clear(): void {
		this.handle().clear();
	}

	/**
	 * Copies every resident range only after the complete set passes native and
	 * wrapper caps. Owner hashes are local facts, not authenticated identities.
	 */
	readRangeSnapshot(
		limits: NativeRangeSnapshotLimits,
	): NativeRangeSnapshotResult {
		const native = this.handle();
		const bounded = rangeSnapshotLimits(limits);
		return rangeSnapshotFromRow(
			this.resolution,
			native.read_range_snapshot(
				bounded.maxOwners,
				bounded.maxRanges,
				bounded.maxRangesPerOwner,
				bounded.maxRangeIdBytes,
				bounded.maxOwnerHashBytes,
				bounded.maxBytes,
			),
			bounded,
		);
	}

	put(range: NativeReplicationRange): void {
		this.handle().put(
			range.id,
			range.hash,
			asIntegerString(range.timestamp),
			asIntegerString(range.start1),
			asIntegerString(range.end1),
			asIntegerString(range.start2),
			asIntegerString(range.end2),
			asIntegerString(range.width),
			range.mode,
		);
	}

	delete(id: string): boolean {
		return this.handle().delete(id);
	}

	getSamples(
		cursors: Iterable<bigint | number | string>,
		options?: SampleOptions,
	): Map<string, LeaderSample> {
		const rows = this.handle().get_samples(
			[...cursors].map(asIntegerString),
			options?.roleAge ?? 0,
			asIntegerString(options?.now ?? Date.now()),
			options?.onlyIntersecting === true,
			optionalIterableToArray(options?.uniqueReplicators),
			optionalIterableToArray(options?.peerFilter),
		);
		return rowsToSamples(rows);
	}

	findLeaders(
		cursors: Iterable<bigint | number | string>,
		replicas: number,
		options?: FindLeaderOptions,
	): Map<string, LeaderSample> {
		const rows = this.handle().find_leaders(
			[...cursors].map(asIntegerString),
			replicas,
			...findLeaderArguments(options),
		);
		return rowsToSamples(rows);
	}

	findLeadersBatch(
		items: Iterable<LeaderBatchInput>,
		options?: FindLeaderOptions,
	): Array<Map<string, LeaderSample>> {
		const native = this.handle();
		const cursorBatches: string[][] = [];
		const replicaCounts: number[] = [];
		for (const item of items) {
			cursorBatches.push([...item.cursors].map(asIntegerString));
			replicaCounts.push(item.replicas);
		}

		const rows = native.find_leaders_batch(
			cursorBatches,
			replicaCounts,
			...findLeaderArguments(options),
		);
		return rows.map((row) => rowsToSamples(row as unknown[]));
	}

	findLeadersForGid(
		gid: string,
		replicas: number,
		options?: FindLeaderOptions,
	): Map<string, LeaderSample> {
		const rows = this.handle().find_leaders_for_gid(
			gid,
			replicas,
			...findLeaderArguments(options),
		);
		return rowsToSamples(rows);
	}

	planLeadersForGid(
		gid: string,
		replicas: number,
		options?: FindLeaderOptions,
	): LeaderPlan {
		const [coordinateRows, leaderRows] = this.handle().plan_leaders_for_gid(
			gid,
			replicas,
			...findLeaderArguments(options),
		);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows),
		};
	}

	planLeadersForGidsBatch(
		items: Iterable<LeaderGidBatchInput>,
		options?: FindLeaderOptions,
	): LeaderPlan[] {
		const native = this.handle();
		const gids: string[] = [];
		const replicaCounts: number[] = [];
		for (const item of items) {
			gids.push(item.gid);
			replicaCounts.push(item.replicas);
		}

		const rows = native.plan_leaders_for_gids_batch(
			gids,
			replicaCounts,
			...findLeaderArguments(options),
		);
		return rows.map((row) => {
			const [coordinateRows, leaderRows] = row as [unknown[], unknown[]];
			return {
				coordinates: rowsToNumbers(this.resolution, coordinateRows),
				leaders: rowsToSamples(leaderRows),
			};
		});
	}

	planLocalLeaderHashesForGidsBatch(
		items: Iterable<LeaderGidHashBatchInput>,
		options?: FindLeaderOptions,
	): Set<string> | undefined {
		const native = this.handle();
		if (!native.plan_local_leaders_for_gids_batch) {
			return undefined;
		}
		const hashes: string[] = [];
		const gids: string[] = [];
		const replicaCounts: number[] = [];
		for (const item of items) {
			hashes.push(item.hash);
			gids.push(item.gid);
			replicaCounts.push(item.replicas);
		}
		return new Set(
			native.plan_local_leaders_for_gids_batch(
				hashes,
				gids,
				replicaCounts,
				...findLeaderArguments(options),
			),
		);
	}

	planRepairDispatchBatch(input: RepairDispatchPlanInput): RepairDispatchPlan {
		const native = this.handle();
		const entries = [...input.entries];
		const pendingModes = [...input.pendingModes];
		const rows = native.plan_repair_dispatch(
			entries.map((entry) => entry.hash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => entry.requestedReplicas),
			entries.map((entry) => [...entry.currentLeaders]),
			entries.map((entry) =>
				entry.knownGidPeers ? [...entry.knownGidPeers] : [],
			),
			entries.map((entry) =>
				entry.knownEntryPeers ? [...entry.knownEntryPeers] : [],
			),
			pendingModes,
			pendingModes.map((mode) => [
				...(input.pendingPeersByMode.get(mode) ?? []),
			]),
			pendingModes.map((mode) => {
				const optimisticByGid = input.optimisticPeersByMode?.get(mode);
				return entries.map((entry) => [
					...(optimisticByGid?.get(entry.gid) ?? []),
				]);
			}),
			input.fullReplicaRepairCandidates
				? [...input.fullReplicaRepairCandidates]
				: [],
			input.fullReplicaRepairCandidateCount,
			input.selfHash,
		);

		return rowsToRepairDispatchPlan(rows);
	}

	planRepairDispatchForEntries(
		input: RepairDispatchEntryPlanInput,
		options?: FindLeaderOptions,
	): RepairDispatchPlan {
		const native = this.handle();
		const entries = [...input.entries];
		const pendingModes = [...input.pendingModes];
		const rows = native.plan_repair_dispatch_for_entries(
			entries.map((entry) => entry.hash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => entry.requestedReplicas),
			entries.map((entry) => [...entry.coordinates].map(asIntegerString)),
			entries.map((entry) =>
				entry.knownGidPeers ? [...entry.knownGidPeers] : [],
			),
			entries.map((entry) =>
				entry.knownEntryPeers ? [...entry.knownEntryPeers] : [],
			),
			pendingModes,
			pendingModes.map((mode) => [
				...(input.pendingPeersByMode.get(mode) ?? []),
			]),
			pendingModes.map((mode) => {
				const optimisticByGid = input.optimisticPeersByMode?.get(mode);
				return entries.map((entry) => [
					...(optimisticByGid?.get(entry.gid) ?? []),
				]);
			}),
			input.fullReplicaRepairCandidates
				? [...input.fullReplicaRepairCandidates]
				: [],
			input.fullReplicaRepairCandidateCount,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);

		return rowsToRepairDispatchPlan(rows);
	}

	getFullReplicaLeaders(
		replicas: number,
		options?: FullReplicaLeaderOptions,
	): Map<string, LeaderSample> | undefined {
		const rows = this.handle().get_full_replica_leaders(
			replicas,
			options?.roleAge ?? 0,
			asIntegerString(options?.now ?? Date.now()),
			options?.includeStrict !== false,
			optionalIterableToArray(options?.peerFilter),
		);
		return rows ? rowsToSamples(rows) : undefined;
	}

	includeMaturedPeers(
		peerFilter: Iterable<string> | undefined,
		replicas: number,
		options: MaturedPeerOptions,
	): Set<string> | undefined {
		const peers = this.handle().include_matured_peers(
			peerFilter ? [...peerFilter] : undefined,
			replicas,
			options.roleAge ?? 0,
			asIntegerString(options.now ?? Date.now()),
			options.selfHash,
			options.selfReplicating,
		);
		return peers ? new Set(peers as string[]) : undefined;
	}

	getGrid(
		from: bigint | number | string,
		count: number,
	): Array<number | bigint> {
		return rowsToNumbers(
			this.resolution,
			this.handle().get_grid(asIntegerString(from), count),
		);
	}

	getGidCoordinates(gid: string, count: number): Array<number | bigint> {
		return rowsToNumbers(
			this.resolution,
			this.handle().get_gid_coordinates(gid, count),
		);
	}

	/** Release the owned wasm allocation. Safe to call more than once. */
	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.native.free();
	}

	/** wasm-style alias for callers that own this planner directly. */
	free(): void {
		this.close();
	}
}

export class SharedLogNativeState {
	private constructor(
		private readonly native: NativeSharedLogStateHandle,
		private readonly resolution: RangeResolution,
	) {}

	static async create(resolution: RangeResolution): Promise<SharedLogNativeState> {
		const wasm = await loadWasm();
		return new SharedLogNativeState(
			new wasm.NativeSharedLogState(resolution),
			resolution,
		);
	}

	get length(): number {
		return this.native.len();
	}

	clear(): void {
		this.native.clear();
	}

	/** Complete-or-overflow copy of unauthenticated native range facts. */
	readRangeSnapshot(
		limits: NativeRangeSnapshotLimits,
	): NativeRangeSnapshotResult {
		const bounded = rangeSnapshotLimits(limits);
		return rangeSnapshotFromRow(
			this.resolution,
			this.native.read_range_snapshot(
				bounded.maxOwners,
				bounded.maxRanges,
				bounded.maxRangesPerOwner,
				bounded.maxRangeIdBytes,
				bounded.maxOwnerHashBytes,
				bounded.maxBytes,
			),
			bounded,
		);
	}

	/** @internal */
	fullReplicaCandidatesFor(minReplicas: number, selfHash: string): string[] {
		return this.native.full_replica_candidates_for(minReplicas, selfHash);
	}

	put(range: NativeReplicationRange): void {
		this.native.put(
			range.id,
			range.hash,
			asIntegerString(range.timestamp),
			asIntegerString(range.start1),
			asIntegerString(range.end1),
			asIntegerString(range.start2),
			asIntegerString(range.end2),
			asIntegerString(range.width),
			range.mode,
		);
	}

	delete(id: string): boolean {
		return this.native.delete(id);
	}

	putEntryCoordinates(
		hash: string,
		gid: string,
		coordinates: Iterable<bigint | number | string>,
		assignedToRangeBoundary = false,
		requestedReplicas?: number,
		hashNumber: bigint | number | string = 0,
	): void {
		const coordinateRows = [...coordinates].map(asIntegerString);
		this.native.put_entry_coordinates(
			hash,
			gid,
			asIntegerString(hashNumber),
			coordinateRows,
			assignedToRangeBoundary,
			requestedReplicas ?? coordinateRows.length,
		);
	}

	deleteEntryCoordinates(hash: string): boolean {
		return this.native.delete_entry_coordinates(hash);
	}

	getEntryCoordinates(hash: string): Array<number | bigint> | undefined {
		const coordinates = this.native.get_entry_coordinates(hash);
		return coordinates ? rowsToNumbers(this.resolution, coordinates) : undefined;
	}

	getEntryCoordinateHashes(): string[] {
		return this.native.entry_coordinate_hashes();
	}

	/**
	 * Returns one complete physical collision bucket in hash-number keyset
	 * order. Successful `visited` and `results` are both the exact bucket size;
	 * oversize buckets throw before exposing a partial prefix.
	 */
	readNextRebalanceCollisionBucket(
		afterHashNumber: bigint | number | string | undefined,
		limits: NativeRebalanceCollisionBucketLimits,
	): NativeRebalanceCollisionBucketResult {
		const bounded = rebalanceCollisionBucketLimits(limits);
		return rebalanceCollisionBucketFromRow(
			this.resolution,
			this.native.read_next_rebalance_collision_bucket(
				rebalanceCollisionBucketCursor(this.resolution, afterHashNumber),
				bounded.maxRows,
				bounded.maxIdentifierBytes,
				bounded.maxIdentifierBytesTotal,
				bounded.maxCoordinateValues,
				bounded.maxCoordinateBytes,
				bounded.maxBytes,
			),
		);
	}

	getEntryHashesForHashNumbers(
		hashNumbers: Iterable<bigint | number | string>,
	): Map<bigint, string[]> {
		const rows = this.native.entry_hashes_for_hash_numbers(
			[...hashNumbers].map(asIntegerString),
		);
		return rowsToHashNumberMap(rows);
	}

	getEntryHashesForHashNumbersU64(
		hashNumbers: BigUint64Array,
	): Map<bigint, string[]> | undefined {
		if (
			typeof BigUint64Array === "undefined" ||
			typeof this.native.entry_hashes_for_hash_numbers_u64 !== "function"
		) {
			return undefined;
		}
		return rowsToHashNumberMap(
			this.native.entry_hashes_for_hash_numbers_u64(hashNumbers),
		);
	}

	getEntryHashListForHashNumbersU64(
		hashNumbers: BigUint64Array,
	): string[] | undefined {
		if (
			typeof BigUint64Array === "undefined" ||
			typeof this.native.entry_hashes_for_hash_numbers_flat_u64 !== "function"
		) {
			return undefined;
		}
		return this.native.entry_hashes_for_hash_numbers_flat_u64(hashNumbers);
	}

	getEntryHashNumbersInRange(range: {
		start1: bigint | number | string;
		end1: bigint | number | string;
		start2: bigint | number | string;
		end2: bigint | number | string;
	}): bigint[] {
		return rowsToNumbers(
			"u64",
			this.native.entry_hash_numbers_in_range(
				asIntegerString(range.start1),
				asIntegerString(range.end1),
				asIntegerString(range.start2),
				asIntegerString(range.end2),
			),
		) as bigint[];
	}

	getEntryHashNumbersInRangeU64(range: {
		start1: bigint | number | string;
		end1: bigint | number | string;
		start2: bigint | number | string;
		end2: bigint | number | string;
	}): BigUint64Array | undefined {
		if (
			typeof BigUint64Array === "undefined" ||
			typeof this.native.entry_hash_numbers_in_range_u64 !== "function"
		) {
			return undefined;
		}
		return this.native.entry_hash_numbers_in_range_u64(
			asIntegerString(range.start1),
			asIntegerString(range.end1),
			asIntegerString(range.start2),
			asIntegerString(range.end2),
		);
	}

	commitEntryCoordinates(
		hash: string,
		gid: string,
		coordinates: Iterable<bigint | number | string>,
		nextHashes: Iterable<string>,
		assignedToRangeBoundary = false,
		requestedReplicas?: number,
		hashNumber: bigint | number | string = 0,
	): void {
		const coordinateRows = [...coordinates].map(asIntegerString);
		this.native.commit_entry_coordinates(
			hash,
			gid,
			asIntegerString(hashNumber),
			coordinateRows,
			[...nextHashes],
			assignedToRangeBoundary,
			requestedReplicas ?? coordinateRows.length,
		);
	}

	commitEntryCoordinatesBatch(
		entries: Iterable<{
			hash: string;
			gid: string;
			coordinates: Iterable<bigint | number | string>;
			nextHashes: Iterable<string>;
			assignedToRangeBoundary?: boolean;
			requestedReplicas?: number;
			hashNumber?: bigint | number | string;
		}>,
	): void {
		const rows = [...entries].map((entry) => {
			const coordinates = [...entry.coordinates].map(asIntegerString);
			return {
				hash: entry.hash,
				gid: entry.gid,
				hashNumber: asIntegerString(entry.hashNumber ?? 0),
				coordinates,
				nextHashes: [...entry.nextHashes],
				assignedToRangeBoundary:
					entry.assignedToRangeBoundary === true ? 1 : 0,
				requestedReplicas: entry.requestedReplicas ?? coordinates.length,
			};
		});
		if (rows.length === 0) {
			return;
		}
		const nativeCommitBatch = this.native.commit_entry_coordinates_batch;
		if (!nativeCommitBatch) {
			for (const row of rows) {
				this.native.commit_entry_coordinates(
					row.hash,
					row.gid,
					row.hashNumber,
					row.coordinates,
					row.nextHashes,
					row.assignedToRangeBoundary === 1,
					row.requestedReplicas,
				);
			}
			return;
		}
		nativeCommitBatch.call(
			this.native,
			rows.map((row) => row.hash),
			rows.map((row) => row.gid),
			rows.map((row) => row.hashNumber),
			rows.map((row) => row.coordinates),
			rows.map((row) => row.nextHashes),
			new Uint8Array(rows.map((row) => row.assignedToRangeBoundary)),
			rows.map((row) => row.requestedReplicas),
		);
	}

	countEntryCoordinatesInRanges(
		ranges: Iterable<{
			start1: bigint | number | string;
			end1: bigint | number | string;
			start2: bigint | number | string;
			end2: bigint | number | string;
		}>,
		options?: { includeAssignedToRangeBoundary?: boolean },
	): number {
		const start1: string[] = [];
		const end1: string[] = [];
		const start2: string[] = [];
		const end2: string[] = [];
		for (const range of ranges) {
			start1.push(asIntegerString(range.start1));
			end1.push(asIntegerString(range.end1));
			start2.push(asIntegerString(range.start2));
			end2.push(asIntegerString(range.end2));
		}
		return this.native.count_entry_coordinates_in_ranges(
			start1,
			end1,
			start2,
			end2,
			options?.includeAssignedToRangeBoundary === true,
		);
	}

	deleteEntryCoordinatesBatch(hashes: Iterable<string>): void {
		this.native.delete_entry_coordinates_batch([...hashes]);
	}

	clearEntryCoordinates(): void {
		this.native.clear_entry_coordinates();
	}

	addGidPeers(
		gid: string,
		peers: Iterable<string>,
		reset = false,
	): number {
		return this.native.add_gid_peers(gid, [...peers], reset);
	}

	removeGidPeer(peer: string, gid?: string): void {
		this.native.remove_gid_peer(peer, gid);
	}

	removeGidPeers(peer: string, gids: Iterable<string>): void {
		const gidArray = iterableToArray(gids);
		if (this.native.remove_gid_peers) {
			this.native.remove_gid_peers(peer, gidArray);
			return;
		}
		for (const gid of gidArray) {
			this.native.remove_gid_peer(peer, gid);
		}
	}

	deleteGidPeers(gid: string): boolean {
		return this.native.delete_gid_peers(gid);
	}

	clearGidPeers(): void {
		this.native.clear_gid_peers();
	}

	markEntriesKnownByPeer(hashes: Iterable<string>, peer: string): void {
		this.native.mark_entries_known_by_peer([...hashes], peer);
	}

	removeEntriesKnownByPeer(hashes: Iterable<string>, peer: string): void {
		this.native.remove_entries_known_by_peer([...hashes], peer);
	}

	removePeerFromEntryKnownPeers(peer: string): void {
		this.native.remove_peer_from_entry_known_peers(peer);
	}

	clearEntryKnownPeers(): void {
		this.native.clear_entry_known_peers();
	}

	planLeadersForGid(
		gid: string,
		replicas: number,
		options?: FindLeaderOptions,
	): LeaderPlan {
		const [coordinateRows, leaderRows] =
			this.native.plan_entry_leaders_for_gid(
				gid,
				replicas,
				...findLeaderArguments(options),
			);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows),
		};
	}

	planEntryAssignmentForGid(
		gid: string,
		replicas: number,
		options?: FindLeaderOptions,
	): EntryAssignmentPlan {
		const [coordinateRows, leaderRows, assignedToRangeBoundary] =
			this.native.plan_entry_assignment_for_gid(
				gid,
				replicas,
				...findLeaderArguments(options),
			);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows),
			assignedToRangeBoundary,
		};
	}

	planLocalAppendForGid(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			replicas: number;
			selfHash: string;
		},
		options?: FindLeaderOptions,
	): EntryAssignmentPlan & {
		isLeader: boolean;
		coordinate: NativeAppendCoordinatePlan;
	} {
		const [
			coordinateRows,
			leaderRows,
			isLeader,
			assignedToRangeBoundary,
			coordinatePlanRow,
		] = this.native.plan_local_append_for_gid(
			input.entryHash,
			input.gid,
			asIntegerString(input.hashNumber ?? 0),
			iterableToArray(input.nextHashes),
			input.replicas,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows),
			isLeader,
			assignedToRangeBoundary,
			coordinate: appendCoordinatePlanFromRow(
				this.resolution,
				coordinatePlanRow,
			),
		};
	}

	planLocalAppendForGidCompact(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			replicas: number;
			selfHash: string;
		},
		options?: FindLeaderOptions,
	): {
		coordinates: Array<number | bigint>;
		leaders?: Map<string, LeaderSample>;
		isLeader: boolean;
		assignedToRangeBoundary: boolean;
		coordinate: NativeAppendCoordinatePlan;
	} {
		const compact = this.native.plan_local_append_for_gid_compact;
		if (!compact) {
			return this.planLocalAppendForGid(input, options);
		}
		const [leaderRows, isLeader, assignedToRangeBoundary, coordinatePlanRow] =
			compact.call(
				this.native,
				input.entryHash,
				input.gid,
				asIntegerString(input.hashNumber ?? 0),
				iterableToArray(input.nextHashes),
				input.replicas,
				...findLeaderArguments({
					...options,
					selfHash: input.selfHash,
				}),
			);
		const coordinate = appendCoordinatePlanFromRow(
			this.resolution,
			coordinatePlanRow,
		);
		return {
			coordinates: coordinate.coordinates,
			leaders: leaderRows ? rowsToSamples(leaderRows) : undefined,
			isLeader,
			assignedToRangeBoundary,
			coordinate,
		};
	}

	commitLocalAppendForGidCompact(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			deleteHashes?: Iterable<string>;
			replicas: number;
			selfHash: string;
		},
		options?: FindLeaderOptions,
	): {
		coordinates: Array<number | bigint>;
		leaders?: Map<string, LeaderSample>;
		isLeader: boolean;
		assignedToRangeBoundary: boolean;
		coordinate: NativeAppendCoordinatePlan;
	} {
		const commit = this.native.commit_local_append_for_gid_compact;
		if (!commit) {
			const plan = this.planLocalAppendForGidCompact(input, options);
			if (input.deleteHashes) {
				this.deleteEntryCoordinatesBatch(input.deleteHashes);
			}
			return plan;
		}
		const [leaderRows, isLeader, assignedToRangeBoundary, coordinatePlanRow] =
			commit.call(
				this.native,
				input.entryHash,
				input.gid,
				asIntegerString(input.hashNumber ?? 0),
				iterableToArray(input.nextHashes),
				iterableToArray(input.deleteHashes),
				input.replicas,
				...findLeaderArguments({
					...options,
					selfHash: input.selfHash,
				}),
			);
		const coordinate = appendCoordinatePlanFromRow(
			this.resolution,
			coordinatePlanRow,
		);
		return {
			coordinates: coordinate.coordinates,
			leaders: leaderRows ? rowsToSamples(leaderRows) : undefined,
			isLeader,
			assignedToRangeBoundary,
			coordinate,
		};
	}

	planAppendLeadersForDelivery(
		leaders: ReadonlyMap<string, LeaderSample>,
		fullReplicaCandidates: Iterable<string>,
		minReplicas: number,
	): Map<string, LeaderSample> {
		return rowsToSamples(
			this.native.plan_append_leaders_for_delivery(
				samplesToRows(leaders),
				[...fullReplicaCandidates],
				minReplicas,
			),
		);
	}

	planAppendDelivery(input: {
		leaders: ReadonlyMap<string, LeaderSample>;
		fallbackRecipients?: Iterable<string>;
		minReplicas: number;
		selfHash: string;
		isLeader: boolean;
		deliveryEnabled: boolean;
		reliabilityAck: boolean;
		minAcks?: number;
		requireRecipients: boolean;
	}): AppendDeliveryPlan {
		return appendDeliveryPlanFromRow(
			this.native.plan_append_delivery(
				samplesToRows(input.leaders),
				iterableToArray(input.fallbackRecipients),
				input.minReplicas,
				input.selfHash,
				input.isLeader,
				input.deliveryEnabled,
				input.reliabilityAck,
				input.minAcks,
				input.requireRecipients,
			),
		);
	}

	planAppendForGid(
		input: {
			entryHash: string;
			gid: string;
			hashNumber?: bigint | number | string;
			nextHashes?: Iterable<string>;
			replicas: number;
			fullReplicaCandidates?: Iterable<string>;
			fallbackRecipients?: Iterable<string>;
			selfHash: string;
			deliveryEnabled: boolean;
			reliabilityAck: boolean;
			minAcks?: number;
			requireRecipients: boolean;
		},
		options?: FindLeaderOptions,
	): AppendEntryPlan {
		const [
			coordinateRows,
			leaderRows,
			isLeader,
			assignedToRangeBoundary,
			delivery,
			coordinatePlanRow,
		] = this.native.plan_append_for_gid(
			input.entryHash,
			input.gid,
			asIntegerString(input.hashNumber ?? 0),
			iterableToArray(input.nextHashes),
			input.replicas,
			iterableToArray(input.fullReplicaCandidates),
			iterableToArray(input.fallbackRecipients),
			input.selfHash,
			input.deliveryEnabled,
			input.reliabilityAck,
			input.minAcks,
			input.requireRecipients,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return {
			coordinates: rowsToNumbers(this.resolution, coordinateRows),
			leaders: rowsToSamples(leaderRows),
			isLeader,
			assignedToRangeBoundary,
			delivery: appendDeliveryPlanFromRow(delivery),
			coordinate: appendCoordinatePlanFromRow(
				this.resolution,
				coordinatePlanRow,
			),
		};
	}

	planAppendForGidsBatch(
		input: {
			entries: Iterable<AppendEntryBatchInput>;
			fullReplicaCandidates?: Iterable<string>;
			fallbackRecipients?: Iterable<string>;
			selfHash: string;
			deliveryEnabled: boolean;
			reliabilityAck: boolean;
			minAcks?: number;
			requireRecipients: boolean;
		},
		options?: FindLeaderOptions,
	): AppendEntryPlan[] {
		const entries = [...input.entries];
		const rows = this.native.plan_append_for_gids_batch(
			entries.map((entry) => entry.entryHash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => asIntegerString(entry.hashNumber ?? 0)),
			entries.map((entry) => iterableToArray(entry.nextHashes)),
			entries.map((entry) => entry.replicas),
			iterableToArray(input.fullReplicaCandidates),
			iterableToArray(input.fallbackRecipients),
			input.selfHash,
			input.deliveryEnabled,
			input.reliabilityAck,
			input.minAcks,
			input.requireRecipients,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rows.map(
			([
				coordinateRows,
				leaderRows,
				isLeader,
				assignedToRangeBoundary,
				delivery,
				coordinatePlanRow,
			]) => ({
				coordinates: rowsToNumbers(this.resolution, coordinateRows),
				leaders: rowsToSamples(leaderRows),
				isLeader,
				assignedToRangeBoundary,
				delivery: appendDeliveryPlanFromRow(delivery),
				coordinate: appendCoordinatePlanFromRow(
					this.resolution,
					coordinatePlanRow,
				),
			}),
		);
	}

	planReceiveCoordinatesForGidsBatch(
		input: {
			entries: Iterable<AppendEntryBatchInput>;
			selfHash: string;
		},
		options?: FindLeaderOptions,
	): ReceiveCoordinatePlan[] {
		const entries = [...input.entries];
		const rows = this.native.plan_receive_coordinates_for_gids_batch(
			entries.map((entry) => entry.entryHash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => asIntegerString(entry.hashNumber ?? 0)),
			entries.map((entry) => iterableToArray(entry.nextHashes)),
			entries.map((entry) => entry.replicas),
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rows.map(
			([
				coordinateRows,
				leaderRows,
				isLeader,
				assignedToRangeBoundary,
				coordinatePlanRow,
			]) => ({
				coordinates: rowsToNumbers(this.resolution, coordinateRows),
				leaders: rowsToSamples(leaderRows),
				isLeader,
				assignedToRangeBoundary,
				coordinate: appendCoordinatePlanFromRow(
					this.resolution,
					coordinatePlanRow,
				),
			}),
		);
	}

	planRepairDispatchForEntries(
		input: RepairDispatchEntryPlanInput,
		options?: FindLeaderOptions,
	): RepairDispatchPlan {
		const entries = [...input.entries];
		const pendingModes = [...input.pendingModes];
		const rows = this.native.plan_repair_dispatch_for_entries(
			entries.map((entry) => entry.hash),
			entries.map((entry) => entry.gid),
			entries.map((entry) => entry.requestedReplicas),
			entries.map((entry) => [...entry.coordinates].map(asIntegerString)),
			pendingModes,
			pendingModes.map((mode) => [
				...(input.pendingPeersByMode.get(mode) ?? []),
			]),
			pendingModes.map((mode) => {
				const optimisticByGid = input.optimisticPeersByMode?.get(mode);
				return entries.map((entry) => [
					...(optimisticByGid?.get(entry.gid) ?? []),
				]);
			}),
			input.fullReplicaRepairCandidates
				? [...input.fullReplicaRepairCandidates]
				: [],
			input.fullReplicaRepairCandidateCount,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rowsToRepairDispatchPlan(rows);
	}

	planRepairDispatchForResidentEntries(
		input: ResidentRepairDispatchPlanInput,
		options?: FindLeaderOptions,
	): RepairDispatchPlan {
		const pendingModes = [...input.pendingModes];
		const optimisticGidsByMode: string[][] = [];
		const optimisticPeersByGidByMode: string[][][] = [];
		for (const mode of pendingModes) {
			const optimisticByGid = input.optimisticPeersByMode?.get(mode);
			const gids: string[] = [];
			const peersByGid: string[][] = [];
			if (optimisticByGid) {
				for (const [gid, peers] of optimisticByGid) {
					gids.push(gid);
					peersByGid.push([...peers]);
				}
			}
			optimisticGidsByMode.push(gids);
			optimisticPeersByGidByMode.push(peersByGid);
		}

		const rows = this.native.plan_repair_dispatch_for_resident_entries(
			pendingModes,
			pendingModes.map((mode) => [
				...(input.pendingPeersByMode.get(mode) ?? []),
			]),
			optimisticGidsByMode,
			optimisticPeersByGidByMode,
			input.fullReplicaRepairCandidates
				? [...input.fullReplicaRepairCandidates]
				: [],
			input.fullReplicaRepairCandidateCount,
			...findLeaderArguments({
				...options,
				selfHash: input.selfHash,
			}),
		);
		return rowsToRepairDispatchPlan(rows);
	}
}

export const createRangePlanner = SharedLogRangePlanner.create;
export const createSharedLogState = SharedLogNativeState.create;
