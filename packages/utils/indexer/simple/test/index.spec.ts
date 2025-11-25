import { tests } from "@peerbit/indexer-tests";
import { create } from "../src/index.js";

describe("all", () => {
	tests(create, "transient", {
		shapingSupported: false,
		u64SumSupported: true,
		iteratorsMutable: false,
	});
});
