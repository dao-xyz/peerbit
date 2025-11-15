import { logger as lLogger } from "@libp2p/logger";
import * as B from "tinybench";
import { logger as pLogger } from "../src/index.js";

//node --loader ts-node/esm ./benchmark/index.ts

const libp2pLogger = lLogger("libp2p:test");
const peerbitLogger = pLogger("peerbit:test");

const suite = new B.Bench();
await suite
	.add("libp2p-logger", () => {
		libp2pLogger("hello");
	})
	.add("peerbit-logger", () => {
		peerbitLogger("hello");
	})
	.run();

console.table(suite.table());
