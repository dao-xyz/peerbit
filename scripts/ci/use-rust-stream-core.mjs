// Inject the built native transport into ordinary real-peer regression suites.
import { RUST_CORE_GLOBAL_KEY } from "../../packages/transport/stream/dist/src/index.js";
import { createRustCoreStream } from "../../packages/transport/network-rust/dist/src/index.js";

globalThis[RUST_CORE_GLOBAL_KEY] = await createRustCoreStream();
