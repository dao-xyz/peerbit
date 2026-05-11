export type DocumentContextInput = {
	created: bigint | number | string;
	modified: bigint | number | string;
	head: string;
	gid: string;
	size: number;
};

type WasmModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
	encode_context_suffix: (
		created: string,
		modified: string,
		head: string,
		gid: string,
		size: number,
	) => Uint8Array;
	encode_context_suffix_batch: (
		createds: string[],
		modifieds: string[],
		heads: string[],
		gids: string[],
		sizes: Uint32Array,
	) => Uint8Array[];
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmInitialized = false;

const loadWasm = async (): Promise<WasmModule> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/document_rust.js";
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
				new URL("../wasm/document_rust_bg.wasm", import.meta.url),
			);
			wasm.initSync({ module: bytes });
		} else {
			await wasm.default({
				module_or_path: new URL(
					"../wasm/document_rust_bg.wasm",
					import.meta.url,
				),
			});
		}
		wasmInitialized = true;
	}

	return wasm;
};

const asU64String = (value: bigint | number | string): string =>
	typeof value === "bigint" ? value.toString() : String(value);

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes);

export const encodeContextSuffix = async (
	context: DocumentContextInput,
): Promise<Uint8Array> => {
	const wasm = await loadWasm();
	return copyBytes(
		wasm.encode_context_suffix(
			asU64String(context.created),
			asU64String(context.modified),
			context.head,
			context.gid,
			context.size,
		),
	);
};

export const encodeContextSuffixBatch = async (
	contexts: DocumentContextInput[],
): Promise<Uint8Array[]> => {
	if (contexts.length === 0) {
		return [];
	}
	const wasm = await loadWasm();
	return wasm
		.encode_context_suffix_batch(
			contexts.map((context) => asU64String(context.created)),
			contexts.map((context) => asU64String(context.modified)),
			contexts.map((context) => context.head),
			contexts.map((context) => context.gid),
			Uint32Array.from(contexts.map((context) => context.size)),
		)
		.map(copyBytes);
};
