export type NativeLogEntry = {
	hash: string;
	gid: string;
	next: string[];
	type: number;
	head?: boolean;
	payloadSize?: number;
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

export type NativeJoinPlan = {
	skip: boolean;
	missingParents: string[];
};

type NativeLogIndexHandle = {
	clear: () => void;
	len: () => number;
	has: (hash: string) => boolean;
	put: (
		hash: string,
		gid: string,
		next: string[],
		type: number,
		wallTime: bigint,
		logical: number,
		payloadSize: number,
		head: boolean,
	) => void;
	delete: (hash: string) => boolean;
	heads: (gid?: string) => string[];
	head_entries: (gid?: string) => unknown[];
	head_join_entries: (gid?: string) => unknown[];
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
	) => [boolean, string[]];
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

	has(hash: string): boolean {
		return this.native.has(hash);
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
		);
	}

	delete(hash: string): boolean {
		return this.native.delete(hash);
	}

	heads(gid?: string): string[] {
		return this.native.heads(gid);
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
	): NativeJoinPlan {
		const [skip, missingParents] = this.native.plan_join(
			hash,
			next,
			type,
			reset,
		);
		return { skip, missingParents };
	}
}

export const createLogGraphIndex = () => LogGraphIndex.create();
