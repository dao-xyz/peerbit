import { tests } from "@peerbit/indexer-tests";
import { HashmapIndices } from "../src";

describe('all', () => {
    tests(() => new HashmapIndices(), 'transient');
})