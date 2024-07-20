import { tests } from "@peerbit/indexer-tests";
import { create } from "../src/index.js";

describe("all", () => {
	tests(create, "persist", true);
	tests(create, "transient", true);
});
