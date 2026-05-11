export type DocumentContextInput = {
	created: bigint | number | string;
	modified: bigint | number | string;
	head: string;
	gid: string;
	size: number;
};

export type DocumentCommitContextInput = {
	existingCreated?: bigint | number | string | null;
	modified: bigint | number | string;
	head: string;
	gid: string;
	size: number;
};

export type DocumentCommitContextPlan = {
	created: bigint;
	modified: bigint;
	head: string;
	gid: string;
	size: number;
	contextBytes: Uint8Array;
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
	plan_document_context: (
		existingCreated: string | undefined,
		modified: string,
		head: string,
		gid: string,
		size: number,
	) => [string, Uint8Array];
	plan_document_context_batch: (
		existingCreateds: Array<string | undefined>,
		modifieds: string[],
		heads: string[],
		gids: string[],
		sizes: Uint32Array,
	) => Array<[string, Uint8Array]>;
};

let wasmModulePromise: Promise<WasmModule> | undefined;
let wasmModule: WasmModule | undefined;
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
	wasmModule = wasm;

	return wasm;
};

const asU64String = (value: bigint | number | string): string =>
	typeof value === "bigint" ? value.toString() : String(value);

const asOptionalU64String = (
	value: bigint | number | string | null | undefined,
): string | undefined =>
	value == null
		? undefined
		: typeof value === "bigint"
			? value.toString()
			: String(value);

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

const toContextPlan = (
	input: DocumentCommitContextInput,
	row: [string, Uint8Array],
): DocumentCommitContextPlan => ({
	created: BigInt(row[0]),
	modified: BigInt(asU64String(input.modified)),
	head: input.head,
	gid: input.gid,
	size: input.size,
	contextBytes: copyBytes(row[1]),
});

const planDocumentContextWithWasm = (
	wasm: WasmModule,
	input: DocumentCommitContextInput,
): DocumentCommitContextPlan => {
	const row = wasm.plan_document_context(
		asOptionalU64String(input.existingCreated),
		asU64String(input.modified),
		input.head,
		input.gid,
		input.size,
	);
	return toContextPlan(input, row);
};

const planDocumentContextBatchWithWasm = (
	wasm: WasmModule,
	inputs: DocumentCommitContextInput[],
): DocumentCommitContextPlan[] => {
	if (inputs.length === 0) {
		return [];
	}
	const rows = wasm.plan_document_context_batch(
		inputs.map((input) => asOptionalU64String(input.existingCreated)),
		inputs.map((input) => asU64String(input.modified)),
		inputs.map((input) => input.head),
		inputs.map((input) => input.gid),
		Uint32Array.from(inputs.map((input) => input.size)),
	);
	return rows.map((row, index) => toContextPlan(inputs[index]!, row));
};

export const tryPlanDocumentContext = (
	input: DocumentCommitContextInput,
): DocumentCommitContextPlan | undefined => {
	return wasmInitialized && wasmModule
		? planDocumentContextWithWasm(wasmModule, input)
		: undefined;
};

export const tryPlanDocumentContextBatch = (
	inputs: DocumentCommitContextInput[],
): DocumentCommitContextPlan[] | undefined => {
	return wasmInitialized && wasmModule
		? planDocumentContextBatchWithWasm(wasmModule, inputs)
		: undefined;
};

export const planDocumentContext = async (
	input: DocumentCommitContextInput,
): Promise<DocumentCommitContextPlan> => {
	const wasm = await loadWasm();
	return planDocumentContextWithWasm(wasm, input);
};

export const planDocumentContextBatch = async (
	inputs: DocumentCommitContextInput[],
): Promise<DocumentCommitContextPlan[]> => {
	if (inputs.length === 0) {
		return [];
	}
	const wasm = await loadWasm();
	return planDocumentContextBatchWithWasm(wasm, inputs);
};
