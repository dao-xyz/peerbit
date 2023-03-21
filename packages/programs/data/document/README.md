# Documents

Distributed document store.

This store is built on top of the base store. This store allows for type safe document store and retrieval accross peers. 

As of know, go through the [tests](./src//__tests__/index.integration.test.ts) for documentation on how to use the module.



Example 
```typescript 
import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";
import {
	Documents,
	DocumentIndex,
	DocumentQueryRequest,
	StringMatchQuery,
	StringMatchMethod,
	Results,
} from "@dao-xyz/peerbit-document";


@variant("document")
class Document {
    @field({ type: "string" })
    id: string;

    @field({ type: option("string") })
    name?: string;

    @field({ type: option("u64") })
    number?: bigint;

    constructor(opts?: Document) {
        if (opts) {
            Object.assign(this, opts);
        }
    }
}

@variant("test_documents")
class TestStore extends Program {
    @field({ type: Documents })
    docs: Documents<Document>;

    constructor(properties?: { docs: Documents<Document> }) {
        super();
        if (properties) {
            this.docs = properties.docs;
        }
    }
    async setup(): Promise<void> {
        await this.docs.setup({ 
			type: Document, 
			index: {
				fields: (obj) => obj // here you can filter and transform what fields you want to index
			}})
    }
}

// later 

const peer = await Peerbit.create ({libp2p: your_libp2p_instance})
const store = peer.open(new TestStore());
console.log(store.address) /// this address can be opened by another peer 


// insert
let doc = new Document({
    id: "1",
    name: "hello world",
    number: 1n,
});
let doc2 = new Document({
    id: "2",
    name: "hello world",
    number: 2n,
});

let doc3 = new Document({
    id: "3",
    name: "foo",
    number: 3n,
});

await store.docs.put(doc);
await store.docs.put(doc2);
await store.docs.put(doc3);


// search for documents from another peer
const peer2 = await Peerbit.create ({libp2: another_libp2p_instance})
const store2 = peer2.open(store.address);

let responses: Results<Document>[] = await store2.docs.index.query(
    new DocumentQueryRequest({
        queries: [
          new StringMatchQuery({
                key: "name",
                value: "ello",
				method: StringMatchMethod.contains
            }),
        ],
    })
);
expect(responses.results[0]).toHaveLength(2);
expect(responses.results[0].map((x) => x.value.id)).toEqual(["1", "2"]);
```
