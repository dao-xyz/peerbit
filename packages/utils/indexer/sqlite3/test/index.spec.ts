import { tests } from "@peerbit/indexer-tests";
import { create } from "../src/index.js";

describe("all", () => {
	tests(create, "persist", {
		shapingSupported: true,
		u64SumSupported: false,
		iteratorsMutable: true,
	});
	tests(create, "transient", {
		shapingSupported: true,
		u64SumSupported: false,
		iteratorsMutable: true,
	});
});
