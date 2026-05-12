import * as wasmModuleImport from "../wasm/shared_log_rust.js";

export type RangeResolution = "u32" | "u64";

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

export type SampleOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	onlyIntersecting?: boolean;
	uniqueReplicators?: Iterable<string>;
	peerFilter?: Iterable<string>;
};

export type FullReplicaLeaderOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	includeStrict?: boolean;
	peerFilter?: Iterable<string>;
};

export type MaturedPeerOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	selfHash: string;
	selfReplicating: boolean;
};

export type FindLeaderOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	peerFilter?: Iterable<string>;
	expandPeerFilter?: boolean;
	selfHash?: string;
	selfReplicating?: boolean;
	fullReplicaFallback?: boolean;
	includeStrictFullReplica?: boolean;
};

export type LeaderSample = {
	intersecting: boolean;
};

export type LeaderPlan = {
	coordinates: Array<number | bigint>;
	leaders: Map<string, LeaderSample>;
};

export type EntryAssignmentPlan = LeaderPlan & {
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
	gid: string;
	coordinates: Array<number | bigint>;
	assignedToRangeBoundary: boolean;
	requestedReplicas: number;
};

export type AppendEntryPlan = EntryAssignmentPlan & {
	isLeader: boolean;
	delivery: AppendDeliveryPlan;
	coordinate: NativeAppendCoordinatePlan;
};

export type AppendEntryBatchInput = {
	entryHash: string;
	gid: string;
	hashNumber?: bigint | number | string;
	nextHashes?: Iterable<string>;
	replicas: number;
};

export type LeaderBatchInput = {
	cursors: Iterable<bigint | number | string>;
	replicas: number;
};

export type LeaderGidBatchInput = {
	gid: string;
	replicas: number;
};

export type RepairDispatchBatchEntry = {
	hash: string;
	gid: string;
	requestedReplicas: number;
	currentLeaders: Iterable<string>;
	knownGidPeers?: Iterable<string>;
	knownEntryPeers?: Iterable<string>;
};

export type RepairDispatchEntryPlanBatchEntry = {
	hash: string;
	gid: string;
	requestedReplicas: number;
	coordinates: Iterable<bigint | number | string>;
	knownGidPeers?: Iterable<string>;
	knownEntryPeers?: Iterable<string>;
};

export type RepairDispatchPlanInput = {
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

export type RepairDispatchEntryPlanInput = Omit<
	RepairDispatchPlanInput,
	"entries"
> & {
	entries: Iterable<RepairDispatchEntryPlanBatchEntry>;
};
export type ResidentRepairDispatchPlanInput = Omit<
	RepairDispatchEntryPlanInput,
	"entries"
>;

export type RepairDispatchPlan = Map<string, Map<string, string[]>>;

type NativeRangePlannerHandle = {
	len: () => number;
	clear: () => void;
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
	entry_hash_numbers_in_range: (
		start1: string,
		end1: string,
		start2: string,
		end2: string,
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

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		wasmModulePromise = Promise.resolve(
			wasmModuleImport as unknown as WasmModule,
		);
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
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
	}

	return wasm;
};

const asIntegerString = (value: bigint | number | string) =>
	typeof value === "bigint"
		? value.toString()
		: typeof value === "number"
			? Math.trunc(value).toString()
			: value;

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
	return {
		hash,
		hashNumber: rowsToNumbers(resolution, [hashNumber])[0]!,
		gid,
		coordinates: rowsToNumbers(resolution, coordinateRows),
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
	options?.peerFilter ? [...options.peerFilter] : undefined,
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
	private constructor(
		private readonly native: NativeRangePlannerHandle,
		private readonly resolution: RangeResolution,
	) {}

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
		return this.native.len();
	}

	clear(): void {
		this.native.clear();
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

	getSamples(
		cursors: Iterable<bigint | number | string>,
		options?: SampleOptions,
	): Map<string, LeaderSample> {
		const rows = this.native.get_samples(
			[...cursors].map(asIntegerString),
			options?.roleAge ?? 0,
			asIntegerString(options?.now ?? Date.now()),
			options?.onlyIntersecting === true,
			options?.uniqueReplicators ? [...options.uniqueReplicators] : undefined,
			options?.peerFilter ? [...options.peerFilter] : undefined,
		);
		return rowsToSamples(rows);
	}

	findLeaders(
		cursors: Iterable<bigint | number | string>,
		replicas: number,
		options?: FindLeaderOptions,
	): Map<string, LeaderSample> {
		const rows = this.native.find_leaders(
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
		const cursorBatches: string[][] = [];
		const replicaCounts: number[] = [];
		for (const item of items) {
			cursorBatches.push([...item.cursors].map(asIntegerString));
			replicaCounts.push(item.replicas);
		}

		const rows = this.native.find_leaders_batch(
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
		const rows = this.native.find_leaders_for_gid(
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
		const [coordinateRows, leaderRows] = this.native.plan_leaders_for_gid(
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
		const gids: string[] = [];
		const replicaCounts: number[] = [];
		for (const item of items) {
			gids.push(item.gid);
			replicaCounts.push(item.replicas);
		}

		const rows = this.native.plan_leaders_for_gids_batch(
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

	planRepairDispatchBatch(input: RepairDispatchPlanInput): RepairDispatchPlan {
		const entries = [...input.entries];
		const pendingModes = [...input.pendingModes];
		const rows = this.native.plan_repair_dispatch(
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
		const entries = [...input.entries];
		const pendingModes = [...input.pendingModes];
		const rows = this.native.plan_repair_dispatch_for_entries(
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
		const rows = this.native.get_full_replica_leaders(
			replicas,
			options?.roleAge ?? 0,
			asIntegerString(options?.now ?? Date.now()),
			options?.includeStrict !== false,
			options?.peerFilter ? [...options.peerFilter] : undefined,
		);
		return rows ? rowsToSamples(rows) : undefined;
	}

	includeMaturedPeers(
		peerFilter: Iterable<string> | undefined,
		replicas: number,
		options: MaturedPeerOptions,
	): Set<string> | undefined {
		const peers = this.native.include_matured_peers(
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
			this.native.get_grid(asIntegerString(from), count),
		);
	}

	getGidCoordinates(gid: string, count: number): Array<number | bigint> {
		return rowsToNumbers(
			this.resolution,
			this.native.get_gid_coordinates(gid, count),
		);
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

	getEntryHashesForHashNumbers(
		hashNumbers: Iterable<bigint | number | string>,
	): Map<bigint, string[]> {
		const out = new Map<bigint, string[]>();
		const rows = this.native.entry_hashes_for_hash_numbers(
			[...hashNumbers].map(asIntegerString),
		);
		for (const row of rows) {
			const [hashNumber, hashes] = row as [string, string[]];
			out.set(BigInt(hashNumber), hashes);
		}
		return out;
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
			input.nextHashes ? [...input.nextHashes] : [],
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
				input.nextHashes ? [...input.nextHashes] : [],
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
				input.fallbackRecipients ? [...input.fallbackRecipients] : [],
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
			input.nextHashes ? [...input.nextHashes] : [],
			input.replicas,
			input.fullReplicaCandidates ? [...input.fullReplicaCandidates] : [],
			input.fallbackRecipients ? [...input.fallbackRecipients] : [],
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
			entries.map((entry) => (entry.nextHashes ? [...entry.nextHashes] : [])),
			entries.map((entry) => entry.replicas),
			input.fullReplicaCandidates ? [...input.fullReplicaCandidates] : [],
			input.fallbackRecipients ? [...input.fallbackRecipients] : [],
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
