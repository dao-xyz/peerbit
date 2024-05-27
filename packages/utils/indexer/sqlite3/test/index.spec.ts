import { tests } from "@peerbit/indexer-tests";
import { create } from "../src/index.js";

describe("all", () => {
    tests(create, 'persist');
    tests(create, 'transient');
})