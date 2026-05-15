export type RangeResolution = "u32" | "u64";

type NativePeerbitBackboneHandle = {
	log_len: () => number;
	block_len: () => number;
	has_log_entry: (hash: string) => boolean;
	has_block: (hash: string) => boolean;
	entry_coordinate_hashes: () => string[];
	clear: () => void;
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
	hash: string;
	next: string[];
	metaBytes?: Uint8Array;
	byteLength: number;
	hashDigestBytes?: Uint8Array;
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

export type NativeBackboneAppendResult = {
	entry: NativeBackboneCommittedEntry;
	coordinate: NativeBackboneCoordinatePlan;
	leaders?: Map<string, NativeBackboneLeaderSample>;
	isLeader: boolean;
	assignedToRangeBoundary: boolean;
	trimmed: NativeBackboneTrimmedEntry[];
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

const committedEntryFromRow = (row: unknown[]): NativeBackboneCommittedEntry => {
	const [hash, metaBytes, byteLength, hashDigestBytes] = row as [
		string,
		Uint8Array | undefined,
		number,
		Uint8Array | undefined,
	];
	return {
		hash,
		next: [],
		metaBytes,
		byteLength,
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

const integerString = (value: bigint | number | string): string =>
	typeof value === "string" ? value : value.toString();

export class NativePeerbitBackbone {
	private constructor(
		private readonly native: NativePeerbitBackboneHandle,
		private readonly resolution: RangeResolution,
	) {}

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
}

export const createNativePeerbitBackbone = NativePeerbitBackbone.create;
