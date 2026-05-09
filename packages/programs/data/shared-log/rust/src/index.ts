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
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeRangePlanner: new (resolution: string) => NativeRangePlannerHandle;
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/shared_log_rust.js";
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

export class SharedLogRangePlanner {
	private constructor(private readonly native: NativeRangePlannerHandle) {}

	static async create(
		resolution: RangeResolution,
	): Promise<SharedLogRangePlanner> {
		const wasm = await loadWasm();
		return new SharedLogRangePlanner(new wasm.NativeRangePlanner(resolution));
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
			options?.roleAge ?? 0,
			asIntegerString(options?.now ?? Date.now()),
			options?.peerFilter ? [...options.peerFilter] : undefined,
			options?.expandPeerFilter === true,
			options?.selfHash ?? "",
			options?.selfReplicating === true,
			options?.fullReplicaFallback === true,
			options?.includeStrictFullReplica !== false,
		);
		return rowsToSamples(rows);
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
}

export const createRangePlanner = SharedLogRangePlanner.create;
