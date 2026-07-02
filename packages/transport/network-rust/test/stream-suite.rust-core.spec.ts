// Re-runs the full @peerbit/stream behavioral test-suite with the native
// DirectStream core injected (see RUST_CORE_GLOBAL_KEY in @peerbit/stream):
// every DirectStream the suite constructs picks up the rust-core routing
// table, seen-cache, lane scheduler and relay/ack decisions, so the same
// multi-peer sessions double as behavioral-parity evidence for the port.
//
// Gated behind PEERBIT_STREAM_RUST_CORE=1 (the `test:stream-rust-core`
// script) so the plain `npm test` run keeps the default mode. The
// messages-signing spec is not re-imported: it tests message classes only
// and never constructs a DirectStream, so the core cannot affect it.
import { RUST_CORE_GLOBAL_KEY } from "@peerbit/stream";
import { existsSync } from "fs";
import { fileURLToPath } from "url";
import { createRustCoreStream } from "../src/index.js";

const STREAM_SUITE_SPECS = [
	"stream.spec.js",
	"routes.spec.js",
	"priority-lanes.spec.js",
	"it-pushable.spec.js",
	"errors.spec.js",
	"inmemory-libp2p.spec.js",
	"stats.spec.js",
	"wait-for-event.spec.js",
];

// This file runs from dist/test (aegir) or test (ts-node); the compiled
// stream suite lives a different number of directories up in each case.
const streamTestDir = [
	new URL("../../../stream/dist/test/", import.meta.url),
	new URL("../../stream/dist/test/", import.meta.url),
].find((candidate) => existsSync(fileURLToPath(candidate)));

if (process.env.PEERBIT_STREAM_RUST_CORE) {
	if (!streamTestDir) {
		throw new Error(
			"@peerbit/stream dist/test not found - build @peerbit/stream first",
		);
	}
	(globalThis as Record<string, unknown>)[RUST_CORE_GLOBAL_KEY] =
		await createRustCoreStream();
	for (const spec of STREAM_SUITE_SPECS) {
		await import(/* @vite-ignore */ new URL(spec, streamTestDir).href);
	}
}
