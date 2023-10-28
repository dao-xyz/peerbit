import B from "benchmark";
import crypto from "crypto";
import { Routes } from "../routes.js";

// Run with "node --loader ts-node/esm ./src/__benchmark__/routes.ts"
/* 
const id = () => crypto.randomBytes(16).toString("hex");
let suite = new B.Suite();
const sizes = [5, 10, 20, 40, 80, 160, 320];

for (const size of sizes) {
	const routes = new Routes("_");
	const a = id();
	const b = id();
	const c = id();
	routes.addLink(a, b, 1);
	routes.addLink(b, c, 1);

	for (let i = 0; i < size; i++) {
		routes.addLink(a, id(), Math.random());
	}
	suite = suite.add("peer count: " + size, () => {
		routes.getPath(a, c);
	});
}
suite
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.run();
 */
