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

export const createLogGraphIndex = () => LogGraphIndex.create();
