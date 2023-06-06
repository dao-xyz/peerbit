
## Examples
### [Example library project (contains live demos)](https://github.com/dao-xyz/peerbit-examples)

## Other examples

### Document database
Below is a short example how you can create a database storing documents

```typescript 
import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";
import {
	Documents,
	DocumentIndex,
	SearchRequest,
	StringMatch,
	StringMatchMethod,
	Results,
} from "@dao-xyz/peerbit-document";


@variant(0) // version 0
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

@variant("my_document_store")
class MyDocumentStore extends Program {
    @field({ type: Documents })
    docs: Documents<Document>;

    constructor(properties?: { docs: Documents<Document> }) {
        super();
        if (properties) {
            this.docs = properties.docs;
        }
    }
    async setup(): Promise<void> {
        await this.docs.setup({ type: Document });
    }
}

// later 
const peer = await Peerbit.create()
const store = peer.open(new MyDocumentStore());
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
const peer2 = await Peerbit.creat ()

// Connec to the first peer
await peer2.dial(peer) 

const store2 = peer2.open(store.address);

let responses: Document[] =  await store2.docs.index.query(
    new SearchRequest({
        queries: [
            new StringMatch({
                key: "name",
                value: "ello",
				method: StringMatchMethod.contains
            }),
        ],
    })
);
expect(responses).toHaveLength(2);
expect(responses.map((x) => x.value.id)).toEqual(["1", "2"]);
```



### Collaborative text 
Below is a short example how you can create a collaborative text document: 

```typescript
import { DString, Range } from '@dao-xyz/peerbit-string'
import { Peerbit } from '@dao-xyz/peerbit'
import { Program } from '@dao-xyz/peerbit-program'
import { PublicSignKey } from '@dao-xyz/peerbit-crypto';
import { Range, DString, StringOperation } from '@dao-xyz/peerbit-string';
import { field, variant } from '@dao-xyz/borsh-ts' 

@variant("collaborative_text") // You have to give the program a unique name
class CollaborativeText extends Program {

    @field({ type: DString })
    string: DString // distributed string 

    constructor() {
        this.string = new DString()
    }

    async setup() {
        await this.string.setup({ canAppend: this.canAppend, canRead: this.canRead })
    }

    async canAppend(
        entry: Entry<StringOperation>
    ): Promise<boolean> {
        // .. acl logic writers
    }

    async canRead(identity?: PublicSignKey): Promise<boolean> {
        // .. acl logic for readers
    }

}

// ... 

const peer = await Peerbit.create()
const document = peer.open(new CollaborativeText());
console.log(document.address) /// this address can be opened by another peer 


//  ... 
await document.string.add('hello', new Range({ offset: 0n, length: 6n }));
await document.string.add('world', new Range({ offset: 7n, length: 5n }));

console.log(await document.string.toString()) // 'hello world' from local store
console.log(await document.string.toString({remote: {waitFor: 3000 }})) // 'hello world' from peers

```
