import B from "benchmark";
import { Peerbit } from "../src/peer.js";
// Run with "node --loader ts-node/esm ./benchmark/start-stop.ts"

// start and stop x 91.15 ops/sec ±0.95% (87 runs sampled)
const suite = new B.Suite();
suite
	.add("start and stop", {
		fn: async (deferred: any) => {
			const node = await Peerbit.create();
			await node.start();
			await node.stop();
			deferred.resolve();
		},
		defer: true
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run({ async: true });
