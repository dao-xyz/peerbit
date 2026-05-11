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

export type NativeLogEntryMetadata = {
	hash: string;
	gid: string;
	data?: Uint8Array;
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
	oldest_hash: () => string | undefined;
	newest_hash: () => string | undefined;
	oldest_entries: (limit: number) => unknown[];
	delete_many: (hashes: string[]) => number;
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
	put_append_chain: (
		hashes: string[],
		gid: string,
		initialNext: string[],
		type: number,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		payloadSizes: Uint32Array,
		datas: Array<Uint8Array | undefined>,
	) => void;
	prepare_entry_v0_plain_chain_and_put: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gid: string,
		initialNext: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => EntryV0PreparedPlainEntryRow[];
	prepare_entry_v0_plain_entry_and_put: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => EntryV0PreparedPlainEntryRow;
	prepare_entry_v0_plain_entry_and_put_with_builder: (
		builder: NativeEntryV0PlainBuilderHandle,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => EntryV0PreparedPlainEntryRow;
	prepare_entry_v0_plain_chain_commit_blocks_and_put: (
		blockStore: NativeLogBlockStoreHandle,
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gid: string,
		initialNext: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => EntryV0CommittedPlainEntryRow[];
	prepare_entry_v0_plain_entry_commit_block_and_put: (
		blockStore: NativeLogBlockStoreHandle,
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => EntryV0CommittedPlainEntryRow;
	prepare_entry_v0_plain_entry_commit_block_and_put_with_builder: (
		builder: NativeEntryV0PlainBuilderHandle,
		blockStore: NativeLogBlockStoreHandle,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => EntryV0CommittedPlainEntryRow;
	prepare_entry_v0_plain_entries_commit_blocks_and_put_with_builder: (
		builder: NativeEntryV0PlainBuilderHandle,
		blockStore: NativeLogBlockStoreHandle,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		nexts: string[][],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => EntryV0CommittedPlainEntryRow[];
	prepare_entry_v0_plain_entries_no_next_commit_blocks_and_put_with_builder?: (
		builder: NativeEntryV0PlainBuilderHandle,
		blockStore: NativeLogBlockStoreHandle,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gids: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => EntryV0CommittedPlainEntryRow[];
	delete: (hash: string) => boolean;
	heads: (gid?: string) => string[];
	has_head: (gid?: string) => boolean;
	has_any_head: (gids: string[]) => boolean;
	has_any_head_batch: (gidSets: string[][]) => boolean[];
	head_entries: (gid?: string) => unknown[];
	head_data_entries: (gid?: string) => unknown[];
	max_head_data_u32: (gid?: string) => number | undefined;
	max_head_data_u32_batch: (gids: string[]) => Array<number | undefined>;
	head_join_entries: (gid?: string) => unknown[];
	child_join_entries: (hash: string) => unknown[];
	entry_metadata_batch: (hashes: string[]) => unknown[];
	unique_reference_gids: (hash: string) => string[] | undefined;
	unique_reference_gid_rows_batch: (
		hashes: string[],
	) => Array<Array<[string, string]> | undefined>;
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

type NativeLogBlockStoreHandle = {
	get: (key: string) => Uint8Array | undefined;
	get_many: (keys: string[]) => Array<Uint8Array | undefined>;
	has: (key: string) => boolean;
	has_many: (keys: string[]) => boolean[];
	put: (key: string, value: Uint8Array) => void;
	put_many: (keys: string[], values: Uint8Array[]) => void;
	delete: (key: string) => boolean;
	delete_many: (keys: string[]) => number;
	clear: () => void;
	len: () => number;
	size: () => number;
	entries: () => Array<[string, Uint8Array]>;
};

type NativeEntryV0PlainBuilderHandle = unknown;

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	NativeLogIndex: new () => NativeLogIndexHandle;
	NativeLogBlockStore: new () => NativeLogBlockStoreHandle;
	NativeEntryV0PlainBuilder: new (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
	) => NativeEntryV0PlainBuilderHandle;
	sign_ed25519: (
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		data: Uint8Array,
	) => Uint8Array;
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
	prepare_entry_v0_plain_chain: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		wallTimes: BigUint64Array,
		logicals: Uint32Array,
		gid: string,
		initialNext: string[],
		type: number,
		metaDatas: Array<Uint8Array | undefined>,
		payloadDatas: Uint8Array[],
	) => EntryV0PreparedPlainEntryRow[];
	prepare_entry_v0_plain_entry: (
		clockId: Uint8Array,
		privateKey: Uint8Array,
		publicKey: Uint8Array,
		wallTime: bigint,
		logical: number,
		gid: string,
		next: string[],
		type: number,
		metaData: Uint8Array | undefined,
		payloadData: Uint8Array,
	) => EntryV0PreparedPlainEntryRow;
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

const copyBytes = (bytes: Uint8Array): Uint8Array =>
	new Uint8Array(
		bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength
			? bytes
			: bytes.slice(),
	);

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left === right) {
		return true;
	}
	if (left.byteLength !== right.byteLength) {
		return false;
	}
	for (let i = 0; i < left.byteLength; i++) {
		if (left[i] !== right[i]) {
			return false;
		}
	}
	return true;
};

type BlockInput = Uint8Array | { block: { bytes: Uint8Array }; cid: string };

type NativeLogBlockStoreCarrier = {
	getNativeLogBlockStoreHandle?: () => NativeLogBlockStoreHandle;
};

const nativeLogBlockStoreHandle = (
	store: unknown,
): NativeLogBlockStoreHandle | undefined =>
	(store as NativeLogBlockStoreCarrier | undefined)
		?.getNativeLogBlockStoreHandle?.();

export class LogGraphIndex {
	private plainEntryBuilder:
		| {
				clockId: Uint8Array;
				publicKey: Uint8Array;
				native: NativeEntryV0PlainBuilderHandle;
		  }
		| undefined;

	private constructor(
		private readonly native: NativeLogIndexHandle,
		private readonly wasm: WasmModule,
	) {}

	static async create(): Promise<LogGraphIndex> {
		const wasm = await loadWasm();
		return new LogGraphIndex(new wasm.NativeLogIndex(), wasm);
	}

	private getPlainEntryBuilder(input: {
		clockId: Uint8Array;
		privateKey: Uint8Array;
		publicKey: Uint8Array;
	}): NativeEntryV0PlainBuilderHandle {
		const builder = this.plainEntryBuilder;
		if (
			builder &&
			bytesEqual(builder.clockId, input.clockId) &&
			bytesEqual(builder.publicKey, input.publicKey)
		) {
			return builder.native;
		}
		const native = new this.wasm.NativeEntryV0PlainBuilder(
			input.clockId,
			input.privateKey,
			input.publicKey,
		);
		this.plainEntryBuilder = {
			clockId: copyBytes(input.clockId),
			publicKey: copyBytes(input.publicKey),
			native,
		};
		return native;
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

	oldestHash(): string | undefined {
		return this.native.oldest_hash();
	}

	newestHash(): string | undefined {
		return this.native.newest_hash();
	}

	oldestEntries(limit: number): NativeLogEntry[] {
		return this.native.oldest_entries(limit).map((row) => {
			const [hash, gid, wallTime, logical, type, next, payloadSize, head, data] =
				row as [
					string,
					string,
					string,
					number,
					number,
					string[],
					number,
					boolean,
					Uint8Array | undefined,
				];
			return {
				hash,
				gid,
				next,
				type,
				head,
				payloadSize,
				data,
				clock: {
					timestamp: {
						wallTime: BigInt(wallTime),
						logical,
					},
				},
			};
		});
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

	putAppendChain(entries: NativeLogEntry[]): void {
		if (entries.length === 0) {
			return;
		}
		const first = entries[0]!;
		const hashes = new Array<string>(entries.length);
		const wallTimes = new BigUint64Array(entries.length);
		const logicals = new Uint32Array(entries.length);
		const payloadSizes = new Uint32Array(entries.length);
		const datas = new Array<Uint8Array | undefined>(entries.length);
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i]!;
			hashes[i] = entry.hash;
			wallTimes[i] = BigInt(entry.clock.timestamp.wallTime);
			logicals[i] = entry.clock.timestamp.logical ?? 0;
			payloadSizes[i] = entry.payloadSize ?? 0;
			datas[i] = entry.data;
		}
		this.native.put_append_chain(
			hashes,
			first.gid,
			first.next,
			first.type,
			wallTimes,
			logicals,
			payloadSizes,
			datas,
		);
	}

	prepareEntryV0PlainChainAndPut(
		input: EntryV0PlainChainInput,
	): EntryV0PreparedPlainEntry[] {
		const columns = plainChainInputColumns(input);
		if (!columns) {
			return [];
		}
		return preparedPlainEntryRows(
			this.native.prepare_entry_v0_plain_chain_and_put(
				input.clockId,
				input.privateKey,
				input.publicKey,
				columns.wallTimes,
				columns.logicals,
				input.gid,
				input.initialNext ?? [],
				input.type ?? 0,
				columns.metaDatas,
				input.payloadDatas,
			),
		);
	}

	prepareEntryV0PlainEntryAndPut(
		input: EntryV0PlainEntryInput,
	): EntryV0PreparedPlainEntry {
		const builder = this.getPlainEntryBuilder(input);
		return preparedPlainEntryRow(
			this.native.prepare_entry_v0_plain_entry_and_put_with_builder(
				builder,
				BigInt(input.wallTime),
				input.logical ?? 0,
				input.gid,
				input.next ?? [],
				input.type ?? 0,
				input.metaData,
				input.payloadData,
			),
		);
	}

	prepareEntryV0PlainChainCommit(
		input: EntryV0PlainChainInput,
		blockStore: unknown,
	): EntryV0CommittedPlainEntry[] | undefined {
		const nativeBlockStore = nativeLogBlockStoreHandle(blockStore);
		if (!nativeBlockStore) {
			return undefined;
		}
		const columns = plainChainInputColumns(input);
		if (!columns) {
			return [];
		}
		return committedPlainEntryRows(
			this.native.prepare_entry_v0_plain_chain_commit_blocks_and_put(
				nativeBlockStore,
				input.clockId,
				input.privateKey,
				input.publicKey,
				columns.wallTimes,
				columns.logicals,
				input.gid,
				input.initialNext ?? [],
				input.type ?? 0,
				columns.metaDatas,
				input.payloadDatas,
			),
		);
	}

	prepareEntryV0PlainEntryCommit(
		input: EntryV0PlainEntryInput,
		blockStore: unknown,
	): EntryV0CommittedPlainEntry | undefined {
		const nativeBlockStore = nativeLogBlockStoreHandle(blockStore);
		if (!nativeBlockStore) {
			return undefined;
		}
		const builder = this.getPlainEntryBuilder(input);
		return committedPlainEntryRow(
			this.native.prepare_entry_v0_plain_entry_commit_block_and_put_with_builder(
				builder,
				nativeBlockStore,
				BigInt(input.wallTime),
				input.logical ?? 0,
				input.gid,
				input.next ?? [],
				input.type ?? 0,
				input.metaData,
				input.payloadData,
			),
		);
	}

	prepareEntryV0PlainEntriesCommit(
		input: EntryV0PlainEntriesInput,
		blockStore: unknown,
	): EntryV0CommittedPlainEntry[] | undefined {
		const nativeBlockStore = nativeLogBlockStoreHandle(blockStore);
		if (!nativeBlockStore) {
			return undefined;
		}
		const columns = plainEntriesInputColumns(input);
		if (!columns) {
			return [];
		}
		const builder = this.getPlainEntryBuilder(input);
		if (!input.nexts) {
			const noNextCommit =
				this.native
					.prepare_entry_v0_plain_entries_no_next_commit_blocks_and_put_with_builder;
			if (noNextCommit) {
				return committedPlainEntryRows(
					noNextCommit.call(
						this.native,
						builder,
						nativeBlockStore,
						columns.wallTimes,
						columns.logicals,
						input.gids,
						input.type ?? 0,
						columns.metaDatas,
						input.payloadDatas,
					),
				);
			}
		}
		return committedPlainEntryRows(
			this.native.prepare_entry_v0_plain_entries_commit_blocks_and_put_with_builder(
				builder,
				nativeBlockStore,
				columns.wallTimes,
				columns.logicals,
				input.gids,
				input.nexts ?? input.payloadDatas.map(() => []),
				input.type ?? 0,
				columns.metaDatas,
				input.payloadDatas,
			),
		);
	}

	delete(hash: string): boolean {
		return this.native.delete(hash);
	}

	deleteMany(hashes: Iterable<string>): number {
		return this.native.delete_many([...hashes]);
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

	maxHeadDataU32Batch(gids: Iterable<string>): Array<number | undefined> {
		return this.native.max_head_data_u32_batch([...gids]);
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

	entryMetadataBatch(
		hashes: Iterable<string>,
	): Array<NativeLogEntryMetadata | undefined> {
		return this.native.entry_metadata_batch([...hashes]).map((row) => {
			if (!row) {
				return undefined;
			}
			const [hash, gid, data] = row as [
				string,
				string,
				Uint8Array | undefined,
			];
			return {
				hash,
				gid,
				data,
			};
		});
	}

	uniqueReferenceGids(hash: string): string[] | undefined {
		return this.native.unique_reference_gids(hash);
	}

	uniqueReferenceGidRowsBatch(
		hashes: Iterable<string>,
	): Array<Array<[string, string]> | undefined> {
		return this.native.unique_reference_gid_rows_batch([...hashes]).map((rows) =>
			rows?.map((row) => {
				const [hash, gid] = row;
				return [hash, gid] as [string, string];
			}),
		);
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

export class NativeLogBlockStore {
	private statusValue: "open" | "opening" | "closed" | "closing" = "closed";

	private constructor(private readonly native: NativeLogBlockStoreHandle) {}

	static async create(): Promise<NativeLogBlockStore> {
		const wasm = await loadWasm();
		return new NativeLogBlockStore(new wasm.NativeLogBlockStore());
	}

	getNativeLogBlockStoreHandle(): NativeLogBlockStoreHandle {
		return this.native;
	}

	async start(): Promise<void> {
		this.statusValue = "open";
	}

	async stop(): Promise<void> {
		this.statusValue = "closed";
	}

	status(): "open" | "opening" | "closed" | "closing" {
		return this.statusValue;
	}

	async put(block: BlockInput): Promise<string> {
		const cid =
			block instanceof Uint8Array ? await calculateRawCidV1(block) : block.cid;
		const bytes = block instanceof Uint8Array ? block : block.block.bytes;
		this.native.put(cid, copyBytes(bytes));
		return cid;
	}

	async putMany(blocks: BlockInput[]): Promise<string[]> {
		if (blocks.length === 0) {
			return [];
		}
		const cids = new Array<string>(blocks.length);
		const values = new Array<Uint8Array>(blocks.length);
		await Promise.all(
			blocks.map(async (block, index) => {
				cids[index] =
					block instanceof Uint8Array ? await calculateRawCidV1(block) : block.cid;
				values[index] = copyBytes(
					block instanceof Uint8Array ? block : block.block.bytes,
				);
			}),
		);
		this.native.put_many(cids, values);
		return cids;
	}

	async get(cid: string): Promise<Uint8Array | undefined> {
		const value = this.native.get(cid);
		return value == null ? undefined : copyBytes(value);
	}

	async getMany(cids: string[]): Promise<Array<Uint8Array | undefined>> {
		return this.native
			.get_many(cids)
			.map((value) => (value == null ? undefined : copyBytes(value)));
	}

	async has(cid: string): Promise<boolean> {
		return this.native.has(cid);
	}

	async hasMany(cids: string[]): Promise<boolean[]> {
		return this.native.has_many(cids);
	}

	async rm(cid: string): Promise<void> {
		this.native.delete(cid);
	}

	async rmMany(cids: string[]): Promise<number> {
		return this.native.delete_many(cids);
	}

	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		for (const [key, value] of this.native.entries()) {
			yield [key, copyBytes(value)];
		}
	}

	async size(): Promise<number> {
		return this.native.size();
	}

	persisted(): boolean {
		return false;
	}

	waitFor(): Promise<string[]> {
		return Promise.resolve([]);
	}
}

export const createNativeLogBlockStore =
	async (): Promise<NativeLogBlockStore> => NativeLogBlockStore.create();

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

export type EntryV0PlainChainInput = {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	wallTimes: Array<bigint | number | string>;
	logicals?: number[];
	gid: string;
	initialNext?: string[];
	type?: number;
	metaDatas?: Array<Uint8Array | undefined>;
	payloadDatas: Uint8Array[];
};

export type EntryV0PlainEntryInput = {
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
};

export type EntryV0PlainEntriesInput = {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	wallTimes: Array<bigint | number | string>;
	logicals?: number[];
	gids: string[];
	nexts?: string[][];
	type?: number;
	metaDatas?: Array<Uint8Array | undefined>;
	payloadDatas: Uint8Array[];
};

export type EntryV0PreparedPlainEntry = EntryV0EncodedStorage & {
	byteLength: number;
	signature: Uint8Array;
	next: string[];
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	signatureBytes: Uint8Array;
	hashDigestBytes?: Uint8Array;
};

export type EntryV0CommittedPlainEntry = Omit<
	EntryV0PreparedPlainEntry,
	"bytes"
> & {
	bytes?: undefined;
};

type EntryV0PreparedPlainEntryRow = [
	Uint8Array,
	string,
	Uint8Array,
	string[],
	Uint8Array,
	Uint8Array,
	Uint8Array,
	Uint8Array?,
];

type EntryV0CommittedPlainEntryRow = [
	string,
	Uint8Array,
	string[],
	Uint8Array,
	Uint8Array,
	Uint8Array,
	number,
	Uint8Array?,
];

const plainChainInputColumns = (input: EntryV0PlainChainInput) => {
	if (input.payloadDatas.length === 0) {
		return undefined;
	}
	if (input.wallTimes.length !== input.payloadDatas.length) {
		throw new Error("Expected equal column lengths");
	}
	const wallTimes = new BigUint64Array(input.wallTimes.length);
	const logicals = new Uint32Array(input.payloadDatas.length);
	const metaDatas = new Array<Uint8Array | undefined>(
		input.payloadDatas.length,
	);
	for (let i = 0; i < input.payloadDatas.length; i++) {
		wallTimes[i] = BigInt(input.wallTimes[i]!);
		logicals[i] = input.logicals?.[i] ?? 0;
		metaDatas[i] = input.metaDatas?.[i];
	}
	return { wallTimes, logicals, metaDatas };
};

const plainEntriesInputColumns = (input: EntryV0PlainEntriesInput) => {
	if (input.payloadDatas.length === 0) {
		return undefined;
	}
	if (
		input.wallTimes.length !== input.payloadDatas.length ||
		input.gids.length !== input.payloadDatas.length ||
		(input.nexts && input.nexts.length !== input.payloadDatas.length)
	) {
		throw new Error("Expected equal column lengths");
	}
	const wallTimes = new BigUint64Array(input.wallTimes.length);
	const logicals = new Uint32Array(input.payloadDatas.length);
	const metaDatas = new Array<Uint8Array | undefined>(
		input.payloadDatas.length,
	);
	for (let i = 0; i < input.payloadDatas.length; i++) {
		wallTimes[i] = BigInt(input.wallTimes[i]!);
		logicals[i] = input.logicals?.[i] ?? 0;
		metaDatas[i] = input.metaDatas?.[i];
	}
	return { wallTimes, logicals, metaDatas };
};

const preparedPlainEntryRow = ([
	bytes,
	cid,
	signature,
	next,
	metaBytes,
	payloadBytes,
	signatureBytes,
	hashDigestBytes,
]: EntryV0PreparedPlainEntryRow): EntryV0PreparedPlainEntry => ({
	bytes,
	cid,
	byteLength: bytes.byteLength,
	signature,
	next,
	metaBytes,
	payloadBytes,
	signatureBytes,
	hashDigestBytes,
});

const preparedPlainEntryRows = (
	rows: EntryV0PreparedPlainEntryRow[],
): EntryV0PreparedPlainEntry[] => rows.map(preparedPlainEntryRow);

const committedPlainEntryRow = ([
	cid,
	signature,
	next,
	metaBytes,
	payloadBytes,
	signatureBytes,
	byteLength,
	hashDigestBytes,
]: EntryV0CommittedPlainEntryRow): EntryV0CommittedPlainEntry => ({
	cid,
	byteLength,
	signature,
	next,
	metaBytes,
	payloadBytes,
	signatureBytes,
	hashDigestBytes,
});

const committedPlainEntryRows = (
	rows: EntryV0CommittedPlainEntryRow[],
): EntryV0CommittedPlainEntry[] => rows.map(committedPlainEntryRow);

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

export const signEd25519 = async (input: {
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	data: Uint8Array;
}): Promise<Uint8Array> => {
	const wasm = await loadWasm();
	return wasm.sign_ed25519(input.privateKey, input.publicKey, input.data);
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

export const prepareEntryV0PlainChain = async (
	input: EntryV0PlainChainInput,
): Promise<EntryV0PreparedPlainEntry[]> => {
	const columns = plainChainInputColumns(input);
	if (!columns) {
		return [];
	}
	const wasm = await loadWasm();
	return preparedPlainEntryRows(
		wasm.prepare_entry_v0_plain_chain(
			input.clockId,
			input.privateKey,
			input.publicKey,
			columns.wallTimes,
			columns.logicals,
			input.gid,
			input.initialNext ?? [],
			input.type ?? 0,
			columns.metaDatas,
			input.payloadDatas,
		),
	);
};

export const prepareEntryV0PlainEntry = async (
	input: EntryV0PlainEntryInput,
): Promise<EntryV0PreparedPlainEntry> => {
	const wasm = await loadWasm();
	return preparedPlainEntryRow(
		wasm.prepare_entry_v0_plain_entry(
			input.clockId,
			input.privateKey,
			input.publicKey,
			BigInt(input.wallTime),
			input.logical ?? 0,
			input.gid,
			input.next ?? [],
			input.type ?? 0,
			input.metaData,
			input.payloadData,
		),
	);
};

export const calculateRawCidV1 = async (bytes: Uint8Array): Promise<string> => {
	const wasm = await loadWasm();
	return wasm.calculate_raw_cid_v1(bytes);
};

export const createLogGraphIndex = () => LogGraphIndex.create();
