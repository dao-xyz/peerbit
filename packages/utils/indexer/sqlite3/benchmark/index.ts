import { benchmarks } from '@peerbit/indexer-tests'
import { create } from "../src/index.js";
// Run with "node --loader ts-node/esm ./benchmark/index.ts"

await benchmarks(create, 'transient')
await benchmarks(create, 'persist') 
