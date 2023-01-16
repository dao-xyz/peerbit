import { Key } from "interface-datastore/key";
import { LazyLevelDatastore } from "../../index.js";
import { tempdir } from "../tempdir.js";

async function testLevelIteratorDestroy() {
    const store = new LazyLevelDatastore(tempdir());
    await store.open();
    await store.put(
        new Key(`/test/key${Date.now()}`),
        new TextEncoder().encode(`TESTDATA${Date.now()}`)
    );
    for await (const d of store.query({})) {
        console.log(d); // eslint-disable-line no-console
    }
}

// Will exit with:
// > Assertion failed: (ended_), function ~Iterator, file ../binding.cc, line 546.
// If iterator gets destroyed (in c++ land) and .end() was not called on it.
testLevelIteratorDestroy();
