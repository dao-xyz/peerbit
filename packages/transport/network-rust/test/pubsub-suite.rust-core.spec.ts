// Re-runs the @peerbit/pubsub test-suite with the native core injected (see
// RUST_CORE_GLOBAL_KEY in @peerbit/stream): every TopicControlPlane the
// suite constructs picks up the rust-core DirectStream engine plus the
// native PubSubMessage codec, topic hashing, root-directory state and
// subscribe-state convergence rules, so the same multi-peer sessions double
// as behavioral-parity evidence for the port. The fanout specs keep running
// on the TS FanoutTree (only its DirectStream substrate is native).
//
// Gated behind PEERBIT_STREAM_RUST_CORE=1 (the `test:stream-rust-core`
// script) so the plain `npm test` run keeps the default mode.
import { RUST_CORE_GLOBAL_KEY } from "@peerbit/stream";
import { existsSync } from "fs";
import { fileURLToPath } from "url";
import { createRustCoreStream } from "../src/index.js";

const PUBSUB_SUITE_SPECS = [
	"index.spec.js",
	"inmemory-libp2p.spec.js",
	"topic-root-directory.spec.js",
	"topic-root-control-plane.spec.js",
	"provider-directory.spec.js",
	"subscribe-races.spec.js",
	"unsubscribe-reason.spec.js",
	"fanout-topics.spec.js",
	"fanout-tree.spec.js",
	"fanout-tree-parent-upgrade.spec.js",
	"fanout-tree-sim.spec.js",
	"pubsub-topic-sim.spec.js",
];

// This file runs from dist/test (aegir) or test (ts-node); the compiled
// pubsub suite lives a different number of directories up in each case.
const pubsubTestDir = [
	new URL("../../../pubsub/dist/test/", import.meta.url),
	new URL("../../pubsub/dist/test/", import.meta.url),
].find((candidate) => existsSync(fileURLToPath(candidate)));

if (process.env.PEERBIT_STREAM_RUST_CORE) {
	if (!pubsubTestDir) {
		throw new Error(
			"@peerbit/pubsub dist/test not found - build @peerbit/pubsub first",
		);
	}
	(globalThis as Record<string, unknown>)[RUST_CORE_GLOBAL_KEY] ??=
		await createRustCoreStream();
	for (const spec of PUBSUB_SUITE_SPECS) {
		await import(/* @vite-ignore */ new URL(spec, pubsubTestDir).href);
	}
}
