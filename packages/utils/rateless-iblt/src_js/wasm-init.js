// Override globalThis.fetch to intercept .wasm requests
import { readFile } from "fs/promises";
import init from "./rateless_iblt.js";

const defaultFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = async (url, options) => {
	// If you have multiple wasm files, you might use some logic to handle them.
	// Here, we assume any request ending in `.wasm` is local on disk at the same path.
	if (url.toString().endsWith(".wasm")) {
		// Return a NodeResponse that looks enough like a fetch Response
		return readFile(url);
	}

	return defaultFetch(url, options);
};

await init();
