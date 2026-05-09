export type NativeLogEntry = {
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

export type NativeLogHeadEntry = {
	hash: string;
	meta: {
		gid: string;
		clock: {
			timestamp: {
				wallTime: bigint;
				logical: number;
			};
		};
	};
};

export type NativeLogJoinEntry = NativeLogHeadEntry & {
	meta: NativeLogHeadEntry["meta"] & {
		type: number;
		next: string[];
	};
};

export type NativeLogHeadDataEntry = {
	hash: string;
	meta: {
		data?: Uint8Array;
	};
};

export type NativeJoinPlan = {
	skip: boolean;
	missingParents: string[];
	cutChecked: boolean;
	coveredByCut: boolean;
};

export type NativeJoinCutCheck = {
	gid: string;
	wallTime: bigint | number | string;
	logical?: number;
};

type NativeLogIndexHandle = {
	clear: () => void;
	len: () => number;
	payload_size_sum: () => number;
	has: (hash: string) => boolean;
	has_many: (hashes: string[]) => string[];
	put: (
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
	put_many: (
		hashes: string[],
		gids: string[],
		nexts: string[][],
		types: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		payloadSizes: Uint32Array,
		heads: Uint8Array,
		datas: Array<Uint8Array | undefined>,
	) => void;
	delete: (hash: string) => boolean;
	heads: (gid?: string) => string[];
	has_head: (gid?: string) => boolean;
	has_any_head: (gids: string[]) => boolean;
	has_any_head_batch: (gidSets: string[][]) => boolean[];
	head_entries: (gid?: string) => unknown[];
	head_data_entries: (gid?: string) => unknown[];
	max_head_data_u32: (gid?: string) => number | undefined;
	head_join_entries: (gid?: string) => unknown[];
	child_join_entries: (hash: string) => unknown[];
	unique_reference_gids: (hash: string) => string[] | undefined;
	plan_delete_recursively: (hashes: string[], skipFirst: boolean) => string[];
	children: (hash: string) => string[];
	count_has_next: (next: string, excludeHash?: string) => number;
	shadowed_gids: (
		gid: string,
		next: string[],
		excludeHash?: string,
	) => string[];
	plan_join: (
		hash: string,
		next: string[],
		type: number,
		reset: boolean,
		gid?: string,
		wallTime?: bigint,
		logical?: number,
	) => [boolean, string[], boolean, boolean];
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeLogIndex: new () => NativeLogIndexHandle;
	encode_entry_v0_signable: (
		clockId: Uint8Array,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => Uint8Array;
	encode_entry_v0_storage: (
		clockId: Uint8Array,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		signature: Uint8Array,
		signaturePublicKey: Uint8Array,
		prehash: number,
	) => Uint8Array;
	encode_entry_v0_storage_with_cid: (
		clockId: Uint8Array,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
		signature: Uint8Array,
		signaturePublicKey: Uint8Array,
		prehash: number,
	) => [Uint8Array, string];
	encode_entry_v0_signable_batch: (
		clockIds: Uint8Array[],
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		nexts: string[][],
		types: Uint8Array,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => Uint8Array[];
	encode_entry_v0_storage_batch_with_cids: (
		clockIds: Uint8Array[],
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		nexts: string[][],
		types: Uint8Array,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
		signatures: Uint8Array[],
		signaturePublicKeys: Uint8Array[],
		prehashes: Uint8Array,
	) => Array<[Uint8Array, string]>;
	calculate_raw_cid_v1: (bytes: Uint8Array) => string;
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/log_rust.js";
		wasmModulePromise = import(wasmModulePath) as Promise<WasmModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		const processLike = (
			globalThis as { process?: { versions?: { node?: string } } }
		).process;
		if (processLike?.versions?.node) {
			const fsPromises = "fs/promises";
			const { readFile } = (await import(
				fsPromises
			)) as typeof import("fs/promises");
			const bytes = await readFile(
				new URL("../wasm/log_rust_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL("../wasm/log_rust_bg.wasm", import.meta.url),
			});
		}
		wasmInitialized = true;
	}

	return wasm;
};

export class LogGraphIndex {
	private constructor(private readonly native: NativeLogIndexHandle) {}

	static async create(): Promise<LogGraphIndex> {
		const wasm = await loadWasm();
		return new LogGraphIndex(new wasm.NativeLogIndex());
	}

	clear(): void {
		this.native.clear();
	}

	get length(): number {
		return this.native.len();
	}

	payloadSizeSum(): number {
		return this.native.payload_size_sum();
	}

	has(hash: string): boolean {
		return this.native.has(hash);
	}

	hasMany(hashes: Iterable<string>): Set<string> {
		return new Set(this.native.has_many([...hashes]));
	}

	put(entry: NativeLogEntry): void {
		this.native.put(
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

	putBatch(entries: NativeLogEntry[]): void {
		if (entries.length === 0) {
			return;
		}
		const hashes = new Array<string>(entries.length);
		const gids = new Array<string>(entries.length);
		const nexts = new Array<string[]>(entries.length);
		const types = new Uint8Array(entries.length);
		const wallTimes = new BigUint64Array(entries.length);
		const logicals = new Uint32Array(entries.length);
		const payloadSizes = new Uint32Array(entries.length);
		const heads = new Uint8Array(entries.length);
		const datas = new Array<Uint8Array | undefined>(entries.length);
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			hashes[i] = entry.hash;
			gids[i] = entry.gid;
			nexts[i] = entry.next;
			types[i] = entry.type;
			wallTimes[i] = BigInt(entry.clock.timestamp.wallTime);
			logicals[i] = entry.clock.timestamp.logical ?? 0;
			payloadSizes[i] = entry.payloadSize ?? 0;
			heads[i] = entry.head === false ? 0 : 1;
			datas[i] = entry.data;
		}
		this.native.put_many(
			hashes,
			gids,
			nexts,
			types,
			wallTimes,
			logicals,
			payloadSizes,
			heads,
			datas,
		);
	}

	delete(hash: string): boolean {
		return this.native.delete(hash);
	}

	heads(gid?: string): string[] {
		return this.native.heads(gid);
	}

	hasHead(gid?: string): boolean {
		return this.native.has_head(gid);
	}

	hasAnyHead(gids: Iterable<string>): boolean {
		return this.native.has_any_head([...gids]);
	}

	hasAnyHeadBatch(gidSets: Iterable<Iterable<string>>): boolean[] {
		return this.native.has_any_head_batch(
			[...gidSets].map((gids) => [...gids]),
		);
	}

	headEntries(gid?: string): NativeLogHeadEntry[] {
		return this.native.head_entries(gid).map((row) => {
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
					clock: {
						timestamp: {
							wallTime: BigInt(wallTime),
							logical,
						},
					},
				},
			};
		});
	}

	headDataEntries(gid?: string): NativeLogHeadDataEntry[] {
		return this.native.head_data_entries(gid).map((row) => {
			const [hash, data] = row as [string, Uint8Array | undefined];
			return {
				hash,
				meta: {
					data,
				},
			};
		});
	}

	maxHeadDataU32(gid?: string): number | undefined {
		return this.native.max_head_data_u32(gid);
	}

	joinHeadEntries(gid?: string): NativeLogJoinEntry[] {
		return this.native.head_join_entries(gid).map((row) => {
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
					clock: {
						timestamp: {
							wallTime: BigInt(wallTime),
							logical,
						},
					},
				},
			};
		});
	}

	childJoinEntries(hash: string): NativeLogJoinEntry[] {
		return this.native.child_join_entries(hash).map((row) => {
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
					clock: {
						timestamp: {
							wallTime: BigInt(wallTime),
							logical,
						},
					},
				},
			};
		});
	}

	uniqueReferenceGids(hash: string): string[] | undefined {
		return this.native.unique_reference_gids(hash);
	}

	planDeleteRecursively(hashes: Iterable<string>, skipFirst = false): string[] {
		return this.native.plan_delete_recursively([...hashes], skipFirst);
	}

	children(hash: string): string[] {
		return this.native.children(hash);
	}

	countHasNext(next: string, excludeHash?: string): number {
		return this.native.count_has_next(next, excludeHash);
	}

	shadowedGids(gid: string, next: string[], excludeHash?: string): string[] {
		return this.native.shadowed_gids(gid, next, excludeHash);
	}

	planJoin(
		hash: string,
		next: string[],
		type: number,
		reset = false,
		cutCheck?: NativeJoinCutCheck,
	): NativeJoinPlan {
		const [skip, missingParents, cutChecked, coveredByCut] =
			this.native.plan_join(
				hash,
				next,
				type,
				reset,
				cutCheck?.gid,
				cutCheck ? BigInt(cutCheck.wallTime) : undefined,
				cutCheck ? (cutCheck.logical ?? 0) : undefined,
			);
		return { skip, missingParents, cutChecked, coveredByCut };
	}
}

export type EntryV0EncodeInput = {
	clockId: Uint8Array;
	wallTime: bigint | number | string;
	logical?: number;
	gid: string;
	next?: string[];
	type?: number;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
};

export type EntryV0StorageEncodeInput = EntryV0EncodeInput & {
	signature: Uint8Array;
	signaturePublicKey: Uint8Array;
	prehash?: number;
};

export type EntryV0EncodedStorage = {
	bytes: Uint8Array;
	cid: string;
};

const entryColumns = (inputs: EntryV0EncodeInput[]) => {
	const clockIds = new Array<Uint8Array>(inputs.length);
	const wallTimes = new BigUint64Array(inputs.length);
	const logicals = new Uint32Array(inputs.length);
	const gids = new Array<string>(inputs.length);
	const nexts = new Array<string[]>(inputs.length);
	const types = new Uint8Array(inputs.length);
	const metaDatas = new Array<Uint8Array | undefined>(inputs.length);
	const payloadDatas = new Array<Uint8Array>(inputs.length);
	for (let i = 0; i < inputs.length; i++) {
		const input = inputs[i]!;
		clockIds[i] = input.clockId;
		wallTimes[i] = BigInt(input.wallTime);
		logicals[i] = input.logical ?? 0;
		gids[i] = input.gid;
		nexts[i] = input.next ?? [];
		types[i] = input.type ?? 0;
		metaDatas[i] = input.metaData;
		payloadDatas[i] = input.payloadData;
	}
	return {
		clockIds,
		wallTimes,
		logicals,
		gids,
		nexts,
		types,
		metaDatas,
		payloadDatas,
	};
};

export const encodeEntryV0Signable = async (
	input: EntryV0EncodeInput,
): Promise<Uint8Array> => {
	const wasm = await loadWasm();
	return wasm.encode_entry_v0_signable(
		input.clockId,
		BigInt(input.wallTime),
		input.logical ?? 0,
		input.gid,
		input.next ?? [],
		input.type ?? 0,
		input.metaData,
		input.payloadData,
	);
};

export const encodeEntryV0SignableBatch = async (
	inputs: EntryV0EncodeInput[],
): Promise<Uint8Array[]> => {
	if (inputs.length === 0) {
		return [];
	}
	const wasm = await loadWasm();
	const columns = entryColumns(inputs);
	return wasm.encode_entry_v0_signable_batch(
		columns.clockIds,
		columns.wallTimes,
		columns.logicals,
		columns.gids,
		columns.nexts,
		columns.types,
		columns.metaDatas,
		columns.payloadDatas,
	);
};

export const encodeEntryV0Storage = async (
	input: EntryV0StorageEncodeInput,
): Promise<Uint8Array> => {
	const wasm = await loadWasm();
	return wasm.encode_entry_v0_storage(
		input.clockId,
		BigInt(input.wallTime),
		input.logical ?? 0,
		input.gid,
		input.next ?? [],
		input.type ?? 0,
		input.metaData,
		input.payloadData,
		input.signature,
		input.signaturePublicKey,
		input.prehash ?? 0,
	);
};

export const encodeEntryV0StorageWithCid = async (
	input: EntryV0StorageEncodeInput,
): Promise<EntryV0EncodedStorage> => {
	const wasm = await loadWasm();
	const [bytes, cid] = wasm.encode_entry_v0_storage_with_cid(
		input.clockId,
		BigInt(input.wallTime),
		input.logical ?? 0,
		input.gid,
		input.next ?? [],
		input.type ?? 0,
		input.metaData,
		input.payloadData,
		input.signature,
		input.signaturePublicKey,
		input.prehash ?? 0,
	);
	return { bytes, cid };
};

export const encodeEntryV0StorageBatchWithCids = async (
	inputs: EntryV0StorageEncodeInput[],
): Promise<EntryV0EncodedStorage[]> => {
	if (inputs.length === 0) {
		return [];
	}
	const wasm = await loadWasm();
	const columns = entryColumns(inputs);
	const signatures = new Array<Uint8Array>(inputs.length);
	const signaturePublicKeys = new Array<Uint8Array>(inputs.length);
	const prehashes = new Uint8Array(inputs.length);
	for (let i = 0; i < inputs.length; i++) {
		const input = inputs[i]!;
		signatures[i] = input.signature;
		signaturePublicKeys[i] = input.signaturePublicKey;
		prehashes[i] = input.prehash ?? 0;
	}
	return wasm
		.encode_entry_v0_storage_batch_with_cids(
			columns.clockIds,
			columns.wallTimes,
			columns.logicals,
			columns.gids,
			columns.nexts,
			columns.types,
			columns.metaDatas,
			columns.payloadDatas,
			signatures,
			signaturePublicKeys,
			prehashes,
		)
		.map(([bytes, cid]) => ({ bytes, cid }));
};

export const calculateRawCidV1 = async (bytes: Uint8Array): Promise<string> => {
	const wasm = await loadWasm();
	return wasm.calculate_raw_cid_v1(bytes);
};

export const createLogGraphIndex = () => LogGraphIndex.create();
