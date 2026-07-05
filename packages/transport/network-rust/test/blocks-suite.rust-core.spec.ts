// Re-runs the @peerbit/blocks test-suite with the native core injected (see
// RUST_CORE_GLOBAL_KEY in @peerbit/stream): every DirectBlock the suite
// constructs picks up the rust-core DirectStream engine plus the native
// block-exchange codec, provider caches and eager-block bookkeeping, so the
// same block-exchange sessions double as behavioral-parity evidence for the
// port.
//
// Gated behind PEERBIT_STREAM_RUST_CORE=1 (the `test:stream-rust-core`
// script) so the plain `npm test` run keeps the default mode.
import { RUST_CORE_GLOBAL_KEY } from "@peerbit/stream";
import { existsSync } from "fs";
import { fileURLToPath } from "url";
import { createRustCoreStream } from "../src/index.js";

const BLOCKS_SUITE_SPECS = [
	"libp2p.spec.js",
	"level.spec.js",
	"shutdown-race.spec.js",
];

// This file runs from dist/test (aegir) or test (ts-node); the compiled
// blocks suite lives a different number of directories up in each case.
const blocksTestDir = [
	new URL("../../../blocks/dist/test/", import.meta.url),
	new URL("../../blocks/dist/test/", import.meta.url),
].find((candidate) => existsSync(fileURLToPath(candidate)));

if (process.env.PEERBIT_STREAM_RUST_CORE) {
	if (!blocksTestDir) {
		throw new Error(
			"@peerbit/blocks dist/test not found - build @peerbit/blocks first",
		);
	}
	(globalThis as Record<string, unknown>)[RUST_CORE_GLOBAL_KEY] ??=
		await createRustCoreStream();
	for (const spec of BLOCKS_SUITE_SPECS) {
		await import(/* @vite-ignore */ new URL(spec, blocksTestDir).href);
	}
}
