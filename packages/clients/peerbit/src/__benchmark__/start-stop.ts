import B from "benchmark";
import { Peerbit } from "../peer.js";
// Run with "node --loader ts-node/esm ./src/__benchmark__/start-stop.ts"

// start and stop x 91.15 ops/sec ±0.95% (87 runs sampled)
const suite = new B.Suite();
suite
	.add("start and stop", {
		fn: async (deferred) => {
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
