import init from "../pkg/riblt.js";

const wasmFetch = async (input: any) =>
	(await (await import("node:fs/promises")).readFile(input)) as any; // TODO fix types.
globalThis.fetch = wasmFetch; // wasm-pack build --target web generated load with 'fetch' but node fetch can not load wasm yet, so we need to do this
await init();
export { DecoderWrapper, EncoderWrapper } from "../pkg/riblt.js";
