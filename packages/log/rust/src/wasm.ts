type WasmInitModule = {
	default: (input?: unknown) => Promise<unknown>;
	initSync: (input?: unknown) => unknown;
};

let wasmModulePromise: Promise<WasmInitModule> | undefined;
let wasmInitialized = false;
let wasmInitPromise: Promise<void> | undefined;

export const loadWasm = async <
	T extends WasmInitModule = WasmInitModule,
>(): Promise<T> => {
	if (!wasmModulePromise) {
		const wasmModulePath = "../wasm/log_rust.js";
		wasmModulePromise = import(
			/* @vite-ignore */ wasmModulePath
		) as Promise<WasmInitModule>;
	}

	const wasm = await wasmModulePromise;
	if (!wasmInitialized) {
		wasmInitPromise ??= (async () => {
			const processLike = (
				globalThis as { process?: { versions?: { node?: string } } }
			).process;
			if (processLike?.versions?.node) {
				const fsPromises = "fs/promises";
				const { readFile } = (await import(
					/* @vite-ignore */ fsPromises
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
		})();
	}
	await wasmInitPromise;

	return wasm as T;
};
