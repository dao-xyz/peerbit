import { logger as lLogger } from "@libp2p/logger";
import * as B from "tinybench";
import { logger as pLogger } from "../src/index.js";

//node --loader ts-node/esm ./benchmark/index.ts

let libp2pLogger = lLogger("test");
libp2pLogger.error("Error works as expected from libp2p");
let peerbitLogger = pLogger({ module: "test" });
peerbitLogger.level = "error";
peerbitLogger.error("Error works as expected Peerbit!");

const suite = new B.Bench();
await suite
	.add("libp2p-logger", () => {
		libp2pLogger("hello");
	})
	.add("peerbit-logger", () => {
		peerbitLogger.info("hello");
	})
	.run();

console.table(suite.table());
