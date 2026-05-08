import { benchmarks } from "@peerbit/indexer-tests";
import { create } from "../src/index.js";

// Run with "pnpm --filter @peerbit/indexer-rust benchmark"

await benchmarks(create, "transient");
await benchmarks(create, "persist");
