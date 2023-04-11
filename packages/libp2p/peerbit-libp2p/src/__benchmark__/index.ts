import B from "benchmark";
import { createLibp2pExtended } from "../index.js";
import { tcp } from "@libp2p/tcp";
// Run with "node --loader ts-node/esm ./src/__benchmark__/index.ts"

const suite = new B.Suite();
suite
	.add("start and stop", {
		fn: async (deferred) => {
			const node = await createLibp2pExtended({
				libp2p: {},
			});
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
