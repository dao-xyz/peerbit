// nothing to do since 'fetch' works as expected in the browser
import init from "./rateless_iblt.js";

export const ready = init(
	new URL("/peerbit/riblt/rateless_iblt_bg.wasm", import.meta.url),
);
