// nothing to do since 'fetch' works as expected in the browsere
import init from "./rateless_iblt.js";

await init(new URL("/peerbit/rateless_iblt_bg.wasm", import.meta.url));
