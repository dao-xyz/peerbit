import { calculateRawCid } from "@peerbit/blocks-interface";

export type RangeResolution = "u32" | "u64";

type NativePeerbitBackboneHandle = {
	log_len: () => number;
	block_len: () => number;
	has_log_entry: (hash: string) => boolean;
	has_block: (hash: string) => boolean;
	entry_coordinate_hashes: () => string[];
	graph_has_many: (hashes: string[]) => string[];
	graph_put: (
		hash: string,
		gid: string,
		next: string[],
		type: number,
		wallTime: bigint,
		logical: number,
		payloadSize: number,
		head: boolean,
		data?: Uint8Array,
	) => void;
	graph_delete: (hash: string) => boolean;
	graph_delete_many: (hashes: string[]) => number;
	graph_oldest_entries: (limit: number) => unknown[];
	graph_heads: (gid?: string) => string[];
	graph_has_head: (gid?: string) => boolean;
	graph_has_any_head: (gids: string[]) => boolean;
	graph_has_any_head_batch: (gidSets: string[][]) => boolean[];
	graph_head_entries: (gid?: string) => unknown[];
	graph_head_data_entries: (gid?: string) => unknown[];
	graph_max_head_data_u32: (gid?: string) => number | undefined;
	graph_max_head_data_u32_batch: (gids: string[]) => Array<number | undefined>;
	graph_join_head_entries: (gid?: string) => unknown[];
	graph_child_join_entries: (hash: string) => unknown[];
	graph_entry_metadata_batch: (hashes: string[]) => unknown[];
	graph_unique_reference_gids: (hash: string) => string[] | undefined;
	graph_unique_reference_gid_rows_batch: (hashes: string[]) => unknown[];
	graph_plan_delete_recursively: (
		hashes: string[],
		skipFirst: boolean,
	) => string[];
	graph_payload_size_sum: () => number;
	graph_oldest_hash: () => string | undefined;
	graph_newest_hash: () => string | undefined;
	graph_count_has_next: (next: string, excludeHash?: string) => number;
	graph_shadowed_gids: (
		gid: string,
		next: string[],
		excludeHash?: string,
	) => string[];
	graph_plan_join: (
		hash: string,
		next: string[],
		type: number,
		reset: boolean,
		gid?: string,
		wallTime?: bigint,
		logical?: number,
	) => [boolean, string[], boolean, boolean];
	block_get: (key: string) => Uint8Array | undefined;
	block_get_many: (keys: string[]) => Array<Uint8Array | undefined>;
	block_has_many: (keys: string[]) => boolean[];
	block_put: (key: string, value: Uint8Array) => void;
	block_put_many: (keys: string[], values: Uint8Array[]) => void;
	block_delete: (key: string) => boolean;
	block_delete_many: (keys: string[]) => number;
	block_entries: () => Array<[string, Uint8Array]>;
	block_size: () => number;
	clear: () => void;
	clear_shared_log: () => void;
	clear_entry_coordinates: () => void;
	put_range: (
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
	delete_range: (id: string) => boolean;
	put_entry_coordinates: (
		hash: string,
		gid: string,
		hashNumber: string,
		coordinates: string[],
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
	) => void;
	delete_entry_coordinates: (hash: string) => boolean;
	delete_entry_coordinates_batch: (hashes: string[]) => void;
	commit_entry_coordinates: (
		hash: string,
		gid: string,
		hashNumber: string,
		coordinates: string[],
		nextHashes: string[],
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
	) => void;
	plan_local_append_for_gid_compact: (
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
	commit_local_append_for_gid_compact: (
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
	append_plain_no_next_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
	) => unknown[];
	append_plain_no_next_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_entry_commit_facts: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number | undefined,
	) => unknown[];
	prepare_plain_entry_storage_facts_and_put: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => unknown[];
	prepare_plain_entry_storage_facts_trim_and_put: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_no_next_storage_append_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
	) => unknown[];
	prepare_plain_no_next_storage_append_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		trimLengthTo: number,
	) => unknown[];
	prepare_plain_storage_append_transaction: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
	) => unknown[];
	prepare_plain_storage_append_transaction_trim: (
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		replicas: number,
		roleAgeMs: number,
		now: string,
		selfHash: string,
		selfReplicating: boolean,
		trimLengthTo: number,
	) => unknown[];
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativePeerbitBackbone: new (
		resolution: string,
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
	) => NativePeerbitBackboneHandle;
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/native_backbone.js";
		wasmModulePromise = import(
			/* @vite-ignore */ wasmModulePath
		) as Promise<WasmModule>;
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
				new URL("../wasm/native_backbone_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL(
					"../wasm/native_backbone_bg.wasm",
					import.meta.url,
				),
			});
		}
		wasmInitialized = true;
	}

	return wasm;
};

export type NativeBackboneLeaderSample = {
	intersecting: boolean;
};

export type NativeBackboneFindLeaderOptions = {
	roleAge?: number;
	now?: bigint | number | string;
	peerFilter?: Iterable<string>;
	expandPeerFilter?: boolean;
	selfHash?: string;
	selfReplicating?: boolean;
	fullReplicaFallback?: boolean;
	includeStrictFullReplica?: boolean;
};

export type NativeBackboneAppendDeliveryPlan = {
	hasRemoteRecipients: boolean;
	noPeerError: boolean;
	defaultSendSilent: boolean;
	sendTo: string[];
	ackTo: string[];
	silentTo: string[];
	repairTargets: string[];
	authoritativeRecipients: string[];
};

export type NativeBackboneCoordinatePlan = {
	hash: string;
	hashNumber: number | bigint;
	hashNumberString: string;
	gid: string;
	coordinates: Array<number | bigint>;
	coordinateStrings: string[];
	assignedToRangeBoundary: boolean;
	requestedReplicas: number;
};

export type NativeBackboneCommittedEntry = {
	cid: string;
	hash: string;
	next: string[];
	bytes?: Uint8Array;
	metaBytes?: Uint8Array;
	byteLength: number;
	signature?: Uint8Array;
	payloadBytes?: Uint8Array;
	signatureBytes?: Uint8Array;
	hashDigestBytes?: Uint8Array;
};

type NativeBackboneStorageBackedEntry = NativeBackboneCommittedEntry & {
	bytes: Uint8Array;
};

export type NativeBackboneLogEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	head?: boolean;
	payloadSize?: number;
	data?: Uint8Array;
	clock: {
		timestamp: {
			wallTime: bigint | number | string;
			logical?: number;
		};
	};
};

export type NativeBackboneTrimmedEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	payloadSize: number;
	data?: Uint8Array;
	clock: {
		timestamp: {
			wallTime: bigint;
			logical: number;
		};
	};
};

export type NativeBackboneAppendInput = {
	wallTime: bigint | number | string;
	logical?: number;
	gid: string;
	type?: number;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
	replicas: number;
	roleAgeMs?: number;
	now?: bigint | number | string;
	selfHash?: string;
	selfReplicating?: boolean;
	trimLengthTo?: number;
};

export type NativeBackboneStorageAppendInput = NativeBackboneAppendInput & {
	next?: Iterable<string>;
};

export type NativeBackboneAppendResult = {
	entry: NativeBackboneCommittedEntry;
	coordinate: NativeBackboneCoordinatePlan;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	trimmed: NativeBackboneTrimmedEntry[];
};

export type NativeBackboneStorageAppendResult = {
	entry: NativeBackboneStorageBackedEntry;
	coordinate: NativeBackboneCoordinatePlan;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	trimmed: NativeBackboneTrimmedEntry[];
};

export type NativeBackboneAppendPlan = {
	coordinates: Array<number | bigint>;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	delivery?: NativeBackboneAppendDeliveryPlan;
	coordinate: NativeBackboneCoordinatePlan;
};

export type NativeBackboneRangeInput = {
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

export type NativeBackboneOptions = {
	resolution?: RangeResolution;
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
};

const rowsToNumbers = (
	resolution: RangeResolution,
	rows: unknown[],
): Array<number | bigint> =>
	rows.map((row) => {
		const value = row as string;
		return resolution === "u64" ? BigInt(value) : Number(value);
	});

const rowsToSamples = (
	rows: unknown[] | undefined,
): Map<string, NativeBackboneLeaderSample> | undefined => {
	if (!rows) {
		return undefined;
	}
	const out = new Map<string, NativeBackboneLeaderSample>();
	for (const row of rows) {
		const [hash, intersecting] = row as [string, boolean];
		out.set(hash, { intersecting });
	}
	return out;
};

const appendCoordinatePlanFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneCoordinatePlan => {
	const [
		hash,
		hashNumber,
		gid,
		coordinateRows,
		assignedToRangeBoundary,
		requestedReplicas,
	] = row as [string, unknown, string, unknown[], boolean, number];
	const coordinateStrings = coordinateRows as string[];
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
): NativeBackboneAppendDeliveryPlan => ({
	hasRemoteRecipients: row[0],
	noPeerError: row[1],
	defaultSendSilent: row[2],
	sendTo: row[3],
	ackTo: row[4],
	silentTo: row[5],
	repairTargets: row[6],
	authoritativeRecipients: row[7],
});

const committedEntryFromRow = (row: unknown[]): NativeBackboneCommittedEntry => {
	const [hash, metaBytes, byteLength, hashDigestBytes] = row as [
		string,
		Uint8Array | undefined,
		number,
		Uint8Array | undefined,
	];
	return {
		cid: hash,
		hash,
		next: [],
		metaBytes,
		byteLength,
		hashDigestBytes,
	};
};

const storageFactsEntryFromRow = (
	row: unknown[],
): NativeBackboneStorageBackedEntry => {
	const [bytes, cid, next, byteLength, metaBytes, hashDigestBytes] = row as [
		Uint8Array,
		string,
		string[],
		number,
		Uint8Array | undefined,
		Uint8Array | undefined,
	];
	return {
		cid,
		hash: cid,
		next,
		bytes,
		byteLength,
		metaBytes,
		hashDigestBytes,
	};
};

const trimmedEntryFromRow = (row: unknown): NativeBackboneTrimmedEntry => {
	const [hash, gid, next, type, wallTime, logical, payloadSize, data] = row as [
		string,
		string,
		string[],
		number,
		string,
		number,
		number,
		Uint8Array | undefined,
	];
	return {
		hash,
		gid,
		next,
		type,
		payloadSize,
		data,
		clock: {
			timestamp: {
				wallTime: BigInt(wallTime),
				logical,
			},
		},
	};
};

const nativeLogEntryFromTrimRow = (row: unknown): NativeBackboneLogEntry => {
	const entry = trimmedEntryFromRow(row);
	return {
		...entry,
		clock: {
			timestamp: {
				wallTime: entry.clock.timestamp.wallTime,
				logical: entry.clock.timestamp.logical,
			},
		},
	};
};

const headEntryFromRow = (row: unknown) => {
	const [hash, gid, wallTime, logical] = row as [
		string,
		string,
		string,
		number,
	];
	return {
		hash,
		meta: {
			gid,
			clock: { timestamp: { wallTime: BigInt(wallTime), logical } },
		},
	};
};

const joinHeadEntryFromRow = (row: unknown) => {
	const [hash, gid, wallTime, logical, type, next] = row as [
		string,
		string,
		string,
		number,
		number,
		string[],
	];
	return {
		hash,
		meta: {
			gid,
			type,
			next,
			clock: { timestamp: { wallTime: BigInt(wallTime), logical } },
		},
	};
};

const headDataEntryFromRow = (row: unknown) => {
	const [hash, data] = row as [string, Uint8Array | undefined];
	return { hash, meta: { data } };
};

const metadataEntryFromRow = (row: unknown) => {
	if (row == null) {
		return undefined;
	}
	const [hash, gid, data] = row as [string, string, Uint8Array | undefined];
	return { hash, gid, data };
};

const appendResultFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneAppendResult => {
	const [
		entryRow,
		leaderRows,
		isLeader,
		assignedToRangeBoundary,
		coordinateRow,
		trimRows,
	] = row as [unknown[], unknown[] | undefined, boolean, boolean, unknown[], unknown[]];
	return {
		entry: committedEntryFromRow(entryRow),
		leaders: rowsToSamples(leaderRows),
		isLeader,
		assignedToRangeBoundary,
		coordinate: appendCoordinatePlanFromRow(resolution, coordinateRow),
		trimmed: trimRows.map(trimmedEntryFromRow),
	};
};

const storageAppendResultFromRow = (
	resolution: RangeResolution,
	row: unknown[],
): NativeBackboneStorageAppendResult => {
	const [
		entryRow,
		leaderRows,
		isLeader,
		assignedToRangeBoundary,
		coordinateRow,
		trimRows,
	] = row as [unknown[], unknown[] | undefined, boolean, boolean, unknown[], unknown[]];
	return {
		entry: storageFactsEntryFromRow(entryRow),
		leaders: rowsToSamples(leaderRows),
		isLeader,
		assignedToRangeBoundary,
		coordinate: appendCoordinatePlanFromRow(resolution, coordinateRow),
		trimmed: trimRows.map(trimmedEntryFromRow),
	};
};

const preparedCommitFactsFromRow = (
	row: unknown[],
): NativeBackboneCommittedEntry & {
	trimmedEntries?: NativeBackboneLogEntry[];
} => {
	const isTrimRow =
		Array.isArray(row) &&
		row.length === 2 &&
		Array.isArray(row[0]) &&
		Array.isArray(row[1]);
	const entryRow = (isTrimRow ? row[0] : row) as unknown[];
	const prepared = committedEntryFromRow(entryRow);
	if (isTrimRow) {
		return {
			...prepared,
			trimmedEntries: (row[1] as unknown[]).map(nativeLogEntryFromTrimRow),
		};
	}
	return prepared;
};

export class NativeBackboneLogGraph {
	constructor(
		private readonly native: NativePeerbitBackboneHandle,
		private readonly options?: { commitBlocks?: boolean },
	) {}

	get length(): number {
		return this.native.log_len();
	}

	has(hash: string): boolean {
		return this.native.has_log_entry(hash);
	}

	hasMany(hashes: Iterable<string>): Set<string> {
		return new Set(this.native.graph_has_many([...hashes]));
	}

	put(entry: NativeBackboneLogEntry): void {
		this.native.graph_put(
			entry.hash,
			entry.gid,
			entry.next,
			entry.type,
			BigInt(entry.clock.timestamp.wallTime),
			entry.clock.timestamp.logical ?? 0,
			entry.payloadSize ?? 0,
			entry.head ?? true,
			entry.data,
		);
	}

	putBatch(entries: NativeBackboneLogEntry[]): void {
		for (const entry of entries) {
			this.put(entry);
		}
	}

	putAppendChain(entries: NativeBackboneLogEntry[]): void {
		for (const entry of entries) {
			this.put(entry);
		}
	}

	prepareEntryV0PlainEntryCommit(
		input: {
			clockId: Uint8Array;
			privateKey: Uint8Array;
			publicKey: Uint8Array;
			wallTime: bigint | number | string;
			logical?: number;
			gid: string;
			next?: string[];
			type?: number;
			metaData?: Uint8Array;
			payloadData: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
			trimLengthTo?: number;
		},
		_blockStore: unknown,
	):
		| (NativeBackboneCommittedEntry & {
				trimmedEntries?: NativeBackboneLogEntry[];
		  })
		| undefined {
		if (this.options?.commitBlocks === false) {
			return undefined;
		}
		if (
			input.includeMaterializationBytes !== false ||
			input.includeAppendFactsBytes !== true
		) {
			return undefined;
		}
		return preparedCommitFactsFromRow(
			this.native.prepare_plain_entry_commit_facts(
				BigInt(input.wallTime),
				input.logical ?? 0,
				input.gid,
				input.next ?? [],
				input.type ?? 0,
				input.metaData,
				input.payloadData,
				input.trimLengthTo,
			),
		);
	}

	prepareEntryV0PlainEntryAndPut(
		input: {
			clockId: Uint8Array;
			privateKey: Uint8Array;
			publicKey: Uint8Array;
			wallTime: bigint | number | string;
			logical?: number;
			gid: string;
			next?: string[];
			type?: number;
			metaData?: Uint8Array;
			payloadData: Uint8Array;
			includeMaterializationBytes?: boolean;
			includeAppendFactsBytes?: boolean;
			trimLengthTo?: number;
		},
		): NativeBackboneStorageBackedEntry & {
			trimmedEntries?: NativeBackboneLogEntry[];
		} {
		const row =
			input.trimLengthTo == null
				? this.native.prepare_plain_entry_storage_facts_and_put(
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.next ?? [],
						input.type ?? 0,
						input.metaData,
						input.payloadData,
					)
				: this.native.prepare_plain_entry_storage_facts_trim_and_put(
						BigInt(input.wallTime),
						input.logical ?? 0,
						input.gid,
						input.next ?? [],
						input.type ?? 0,
						input.metaData,
						input.payloadData,
						input.trimLengthTo,
					);
		const isTrimRow =
			Array.isArray(row) &&
			row.length === 2 &&
			Array.isArray(row[0]) &&
			Array.isArray(row[1]);
		const entry = storageFactsEntryFromRow(
			(isTrimRow ? row[0] : row) as unknown[],
		);
		if (!isTrimRow) {
			return entry;
		}
		return {
			...entry,
			trimmedEntries: (row[1] as unknown[]).map(nativeLogEntryFromTrimRow),
		};
	}

	delete(hash: string): boolean {
		return this.native.graph_delete(hash);
	}

	deleteMany(hashes: Iterable<string>): number {
		return this.native.graph_delete_many([...hashes]);
	}

	oldestEntries(limit: number): NativeBackboneLogEntry[] {
		return this.native.graph_oldest_entries(limit).map(nativeLogEntryFromTrimRow);
	}

	clear(): void {
		this.native.clear();
	}

	heads(gid?: string): string[] {
		return this.native.graph_heads(gid);
	}

	hasHead(gid?: string): boolean {
		return this.native.graph_has_head(gid);
	}

	hasAnyHead(gids: Iterable<string>): boolean {
		return this.native.graph_has_any_head([...gids]);
	}

	hasAnyHeadBatch(gidSets: Iterable<Iterable<string>>): boolean[] {
		return this.native.graph_has_any_head_batch(
			[...gidSets].map((gids) => [...gids]),
		);
	}

	headDataEntries(gid?: string): any[] {
		return this.native.graph_head_data_entries(gid).map(headDataEntryFromRow);
	}

	maxHeadDataU32(gid?: string): number | undefined {
		return this.native.graph_max_head_data_u32(gid);
	}

	maxHeadDataU32Batch(gids: Iterable<string>): Array<number | undefined> {
		return this.native.graph_max_head_data_u32_batch([...gids]);
	}

	headEntries(gid?: string): any[] {
		return this.native.graph_head_entries(gid).map(headEntryFromRow);
	}

	joinHeadEntries(gid?: string): any[] {
		return this.native.graph_join_head_entries(gid).map(joinHeadEntryFromRow);
	}

	childJoinEntries(hash: string): any[] {
		return this.native.graph_child_join_entries(hash).map(joinHeadEntryFromRow);
	}

	entryMetadataBatch(hashes: Iterable<string>): Array<any | undefined> {
		return this.native
			.graph_entry_metadata_batch([...hashes])
			.map(metadataEntryFromRow);
	}

	uniqueReferenceGids(hash: string): string[] | undefined {
		return this.native.graph_unique_reference_gids(hash);
	}

	uniqueReferenceGidRowsBatch(hashes: Iterable<string>): any[] {
		return this.native.graph_unique_reference_gid_rows_batch([...hashes]);
	}

	planDeleteRecursively(hashes: Iterable<string>, skipFirst = false): string[] {
		return this.native.graph_plan_delete_recursively([...hashes], skipFirst);
	}

	payloadSizeSum(): number {
		return this.native.graph_payload_size_sum();
	}

	oldestHash(): string | undefined {
		return this.native.graph_oldest_hash();
	}

	newestHash(): string | undefined {
		return this.native.graph_newest_hash();
	}

	countHasNext(next: string, excludeHash?: string): number {
		return this.native.graph_count_has_next(next, excludeHash);
	}

	shadowedGids(
		gid: string,
		next: string[],
		excludeHash?: string,
	): string[] {
		return this.native.graph_shadowed_gids(gid, next, excludeHash);
	}

	planJoin(
		hash: string,
		next: string[],
		type: number,
		reset = false,
		cutCheck?: {
			gid: string;
			wallTime: bigint | number | string;
			logical?: number;
		},
	): any {
		const [skip, missingParents, cutChecked, coveredByCut] =
			this.native.graph_plan_join(
				hash,
				next,
				type,
				reset,
				cutCheck?.gid,
				cutCheck?.wallTime == null ? undefined : BigInt(cutCheck.wallTime),
				cutCheck?.logical,
			);
		return { skip, missingParents, cutChecked, coveredByCut };
	}
}

export class NativeBackboneBlockStore {
	constructor(private readonly native: NativePeerbitBackboneHandle) {}

	status(): "open" {
		return "open";
	}

	open(): void {}

	close(): void {}

	async put(
		data: Uint8Array | { block: { bytes: Uint8Array }; cid: string },
	): Promise<string> {
		const prepared = data instanceof Uint8Array ? await calculateRawCid(data) : data;
		this.native.block_put(prepared.cid, prepared.block.bytes);
		return prepared.cid;
	}

	async putMany(
		blocks: Array<Uint8Array | { block: { bytes: Uint8Array }; cid: string }>,
	): Promise<string[]> {
		const prepared = await Promise.all(
			blocks.map((block) =>
				block instanceof Uint8Array ? calculateRawCid(block) : block,
			),
		);
		this.native.block_put_many(
			prepared.map((block) => block.cid),
			prepared.map((block) => block.block.bytes),
		);
		return prepared.map((block) => block.cid);
	}

	putKnown(cid: string, bytes: Uint8Array): string {
		this.native.block_put(cid, bytes);
		return cid;
	}

	putKnownMany(blocks: Array<readonly [cid: string, bytes: Uint8Array]>): string[] {
		if (blocks.length === 0) {
			return [];
		}
		this.native.block_put_many(
			blocks.map(([cid]) => cid),
			blocks.map(([, bytes]) => bytes),
		);
		return blocks.map(([cid]) => cid);
	}

	putImmutable(cid: string, bytes: Uint8Array): void {
		this.native.block_put(cid, bytes);
	}

	putManyImmutable(blocks: Array<readonly [cid: string, bytes: Uint8Array]>): void {
		this.putKnownMany(blocks);
	}

	get(cid: string): Uint8Array | undefined {
		return this.native.block_get(cid);
	}

	getMany(cids: string[]): Array<Uint8Array | undefined> {
		return this.native.block_get_many(cids);
	}

	has(cid: string): boolean {
		return this.native.has_block(cid);
	}

	hasMany(cids: string[]): boolean[] {
		return this.native.block_has_many(cids);
	}

	rm(cid: string): void {
		this.native.block_delete(cid);
	}

	del(cid: string): void {
		this.rm(cid);
	}

	rmMany(cids: string[]): number {
		return this.native.block_delete_many(cids);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const [key, value] of this.native.block_entries()) {
			yield [key, value];
		}
	}

	size(): number {
		return this.native.block_size();
	}

	persisted(): boolean {
		return false;
	}

	waitFor(): Promise<string[]> {
		return Promise.resolve([]);
	}
}

const integerString = (value: bigint | number | string): string =>
	typeof value === "string"
		? value
		: typeof value === "number"
			? Math.trunc(value).toString()
			: value.toString();

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

const findLeaderArguments = (
	options?: NativeBackboneFindLeaderOptions,
): [
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
	integerString(options?.now ?? Date.now()),
	optionalIterableToArray(options?.peerFilter),
	options?.expandPeerFilter === true,
	options?.selfHash ?? "",
	options?.selfReplicating === true,
	options?.fullReplicaFallback === true,
	options?.includeStrictFullReplica !== false,
];

export class NativePeerbitBackbone {
	readonly graph: NativeBackboneLogGraph;
	readonly storageBackedGraph: NativeBackboneLogGraph;
	readonly blocks: NativeBackboneBlockStore;

	private constructor(
		private readonly native: NativePeerbitBackboneHandle,
		private readonly resolution: RangeResolution,
	) {
		this.graph = new NativeBackboneLogGraph(native);
		this.storageBackedGraph = new NativeBackboneLogGraph(native, {
			commitBlocks: false,
		});
		this.blocks = new NativeBackboneBlockStore(native);
	}

	static async create(
		options: NativeBackboneOptions,
	): Promise<NativePeerbitBackbone> {
		const wasm = await loadWasm();
		const resolution = options.resolution ?? "u64";
		return new NativePeerbitBackbone(
			new wasm.NativePeerbitBackbone(
				resolution,
				options.clockId,
				options.privateKey,
				options.publicKey,
			),
			resolution,
		);
	}

	get logLength(): number {
		return this.native.log_len();
	}

	get blockLength(): number {
		return this.native.block_len();
	}

	hasLogEntry(hash: string): boolean {
		return this.native.has_log_entry(hash);
	}

	hasBlock(hash: string): boolean {
		return this.native.has_block(hash);
	}

	getEntryCoordinateHashes(): string[] {
		return this.native.entry_coordinate_hashes();
	}

	clear(): void {
		this.native.clear();
	}

	clearSharedLog(): void {
		this.native.clear_shared_log();
	}

	clearEntryCoordinates(): void {
		this.native.clear_entry_coordinates();
	}

	putRange(range: NativeBackboneRangeInput): void {
		this.native.put_range(
			range.id,
			range.hash,
			integerString(range.timestamp),
			integerString(range.start1),
			integerString(range.end1),
			integerString(range.start2),
			integerString(range.end2),
			integerString(range.width),
			range.mode,
		);
	}

	deleteRange(id: string): boolean {
		return this.native.delete_range(id);
	}

	putEntryCoordinates(
		hash: string,
		gid: string,
		coordinates: Iterable<bigint | number | string>,
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
		hashNumber: bigint | number | string,
	): void {
		this.native.put_entry_coordinates(
			hash,
			gid,
			integerString(hashNumber),
			[...coordinates].map(integerString),
			assignedToRangeBoundary,
			requestedReplicas,
		);
	}

	deleteEntryCoordinates(hash: string): boolean {
		return this.native.delete_entry_coordinates(hash);
	}

	deleteEntryCoordinatesBatch(hashes: Iterable<string>): void {
		this.native.delete_entry_coordinates_batch(iterableToArray(hashes));
	}

	commitEntryCoordinates(
		hash: string,
		gid: string,
		coordinates: Iterable<bigint | number | string>,
		nextHashes: Iterable<string>,
		assignedToRangeBoundary: boolean,
		requestedReplicas: number,
		hashNumber: bigint | number | string,
	): void {
		this.native.commit_entry_coordinates(
			hash,
			gid,
			integerString(hashNumber),
			[...coordinates].map(integerString),
			iterableToArray(nextHashes),
			assignedToRangeBoundary,
			requestedReplicas,
		);
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
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan {
		const [leaderRows, isLeader, assignedToRangeBoundary, coordinatePlanRow] =
			this.native.plan_local_append_for_gid_compact(
				input.entryHash,
				input.gid,
				integerString(input.hashNumber ?? 0),
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
			leaders: rowsToSamples(leaderRows),
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
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan {
		const [leaderRows, isLeader, assignedToRangeBoundary, coordinatePlanRow] =
			this.native.commit_local_append_for_gid_compact(
				input.entryHash,
				input.gid,
				integerString(input.hashNumber ?? 0),
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
			leaders: rowsToSamples(leaderRows),
			isLeader,
			assignedToRangeBoundary,
			coordinate,
		};
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
		options?: NativeBackboneFindLeaderOptions,
	): NativeBackboneAppendPlan {
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
			integerString(input.hashNumber ?? 0),
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

	appendPlainNoNextTransaction(
		input: NativeBackboneAppendInput,
	): NativeBackboneAppendResult {
		const baseArgs = [
			BigInt(input.wallTime),
			input.logical ?? 0,
			input.gid,
			input.type ?? 0,
			input.metaData,
			input.payloadData,
			input.replicas,
			input.roleAgeMs ?? 0,
			integerString(input.now ?? Date.now()),
			input.selfHash ?? "",
			input.selfReplicating ?? true,
		] as const;
		const row =
			input.trimLengthTo == null
				? this.native.append_plain_no_next_transaction(...baseArgs)
				: this.native.append_plain_no_next_transaction_trim(
						...baseArgs,
						input.trimLengthTo,
					);
		return appendResultFromRow(this.resolution, row);
	}

	preparePlainNoNextStorageAppendTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneStorageAppendResult {
		return this.preparePlainStorageAppendTransaction({
			...input,
			next: [],
		});
	}

	preparePlainStorageAppendTransaction(
		input: NativeBackboneStorageAppendInput,
	): NativeBackboneStorageAppendResult {
		const baseArgs = [
			BigInt(input.wallTime),
			input.logical ?? 0,
			input.gid,
			iterableToArray(input.next),
			input.type ?? 0,
			input.metaData,
			input.payloadData,
			input.replicas,
			input.roleAgeMs ?? 0,
			integerString(input.now ?? Date.now()),
			input.selfHash ?? "",
			input.selfReplicating ?? true,
		] as const;
		const row =
			input.trimLengthTo == null
				? this.native.prepare_plain_storage_append_transaction(...baseArgs)
				: this.native.prepare_plain_storage_append_transaction_trim(
						...baseArgs,
						input.trimLengthTo,
					);
		return storageAppendResultFromRow(this.resolution, row);
	}
}

export const createNativePeerbitBackbone = NativePeerbitBackbone.create;
