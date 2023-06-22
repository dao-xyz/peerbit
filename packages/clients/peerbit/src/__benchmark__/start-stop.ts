import B from "benchmark";
import { createLibp2pExtended } from "../libp2p.js";
// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"

// start and stop x 91.15 ops/sec ±0.95% (87 runs sampled)
const suite = new B.Suite();
suite
	.add("start and stop", {
		fn: async (deferred) => {
			const node = await createLibp2pExtended();
			await node.start();
			await node.stop();
			deferred.resolve();
		},
		defer: true,
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run({ async: true });
