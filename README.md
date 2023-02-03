
<br>
<p align="center">
    <img width="140" src="./logo512.png"  alt="Peerbit icon Icon">
</p>

<h1 align="center" style="font-size: 5vmin;">
    <strong>
        Peerbit
   </strong>
</h1>
<h3 align="center">
    Develop for a distributed web with Peerbit
</h3>

<h3 align="center">🤫 E2EE &nbsp; &nbsp; 👯 P2P &nbsp; &nbsp; ⚖️ Auto-sharding  &nbsp; &nbsp;  🔍 Searchable</h3>
<br>

![tests](https://github.com/dao-xyz/peerbit/actions/workflows/ci.yml/badge.svg)

## P2P databases simplified
Started originally as a fork of OrbitDB: A peer-to-peer database on top of Libp2p (and optionally IPFS) supporting encryption, sharding and discoverability (searching).

Peerbit provides an abstraction layer that lets you program with distributed data types. For example, ```String``` can be replaced with [DString](./packages/programs/data/string) (distributed string). Some datatypes, like [Document store](./packages/programs/data/document) are sharded automatically as long as there are not data dependencies between indiviudal documents.
 
Every peer has an identity which is simply their public key, this key can *currently* either be secp256k1 or a Ed25519 key. To prevent peers from manually sign messages, you can link identities together in a trust graph. This allows you to have a root identity that approves and revokes permissions to keys that can act on your behalf. Hence this allows you to build applications that allows users to act on multiple devices and chains seamlessly.
 
Peers have the possibility to organize themselves into "permissioned" regions. Within a region you can be more confident that peers will respect the sharding distribution algorithm and replicate and index content. Additionally, secret information can be shared freely, this allows peers in the permissioned regions to help each other to decrypt messages in order to be able to index and understand content.
 
Data can be shared and encrypted on a granular level, you can decide exactly what parts of metadata should be public and not. When you create a commit or a query request, you can specify exactly who is going to be able to decrypt the message. If you want an end to end conversation between two identities, you just include the other peers' public key as a receiver and you would be certain that know one in the middle would be able to read your message.

### Goals
The goal of this project is to create a  <span style="color:coral;">cheaper</span> and a more <span style="color:coral;">private</span> way of distributing and accessing data by utilizing consumer hardware and the latest advancements in networking technology. Additionally, we believe that creating a stateful application should and could be made easier if you are approaching it with a P2P database framework like this, since there are no "servers" and "clients", just peers. It should not take longer than a weekend to get started to build your first distributed app!

### Timeline and progress

- ✅ Chain agnostic identities 
- ✅ Permissioned content based sharding 
- ✅ E2EE (no forward secrecy)
- ✅ Physical time with Hybrid Logical Clock
- ✅ Generic search capabilities across networks
- 🚧 Documentation and examples
- 🚧 CLI for non browser nodes (SSL setup and network management)
- 🚧 Benchmarks
- Easy Webtransport/WebRTC setup for device to device networks
- Performant indexation capabilities with WASM search modules
- Improved sharding algorithm that respects device capabilities
- [ZK group access controller](https://vitalik.ca/general/2022/06/15/using_snarks.html)
- E2EE forward secrecy (or alternative security measures)
- ~Trustless hosting 


### Some informational links are found below
[How Peerbit differs from OrbitDB](./documentation/difference.md)

[How Peerbit performs sharding](./documentation/sharding/sharding.md)

[Encryption scheme](./documentation/encryption.md)





### 🚧 Alpha release  🚧
Backwards compatibility for new releases might be lacking. Use with caution and please report bugs and issues you are experiencing when developing with Peerbit. 

### Documentation is lacking at the moment. Be patient! Read module tests for know to know how you can use different modules. Feel free to write an issue to ask any question!  


## Getting started
## [See this guide](https://github.com/dao-xyz/peerbit-getting-started)


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
	DocumentQueryRequest,
	FieldStringMatchQuery,
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
        await this.docs.setup({ type: Document });
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

let response: Results<Document> = undefined as any;
await store2.docs.index.query(
    new DocumentQueryRequest({
        queries: [
            new FieldStringMatchQuery({
                key: "name",
                value: "ello",
            }),
        ],
    }),
    (r: Results<Document>) => {
        response = r;
    },
    { amount: 1 }
);
expect(response.results).toHaveLength(2);
expect(response.results.map((x) => x.value.id)).toEqual(["1", "2"]);
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

const peer = await Peerbit.create(...options...)
const document = peer.open(new CollaborativeText());
console.log(document.address) /// this address can be opened by another peer 


//  ... 
await document.string.add('hello', new Range({ offset: 0n, length: 6n }));
await document.string.add('world', new Range({ offset: 7n, length: 5n }));

console.log(await document.string.toString()) // 'hello world' from local store
console.log(await document.string.toString({remote: {waitFor: 3000 }})) // 'hello world' from peers

```

## [Peerbit](./packages/client)
The peer client
- Open/close programs (databases)

### Installation 
```sh
npm install @dao-xyz/peerbit
```

```typescript
import { Peerbit } from '@dao-xyz/peerbit'

// Create a peer from an libp2p instance
const peer = await Peerbit.create({libp2p: SOME_LIBP2P_INSTANCE })

// Open a program 
const program = await peer.open(PRORGAM ADDRESS or PRORGAM)
program.doThings()
```

## [Programs](./packages/programs)
Contains composable programs you can build your program with. For example distributed [document store](./packages/programs/data/document), [clock service](./packages/programs/clock-service), [chain agnostic access controller](./packages/programs/acl/identity-access-controller) 

A program lets you write control mechanism for Append-Only logs (which are represented as a [Store](./packages/store), example program

```typescript 
import { Peerbit } from '@dao-xyz/peerbit'
import { Store } from '@dao-xyz/peerbit-store'
import { Program } from '@dao-xyz/peerbit-program' 
import { field, variant } from '@dao-xyz/borst-ts' 

@variant("string_store") // Needs to have a variant name so the program is unique
class StringStore extends Program  // Needs to extend Program if you are going to store Store<any> in your class
{
    @field({type: Store}) // decorate it for serialization purposes 
    store: Store<string>

    constructor(properties?:{ store: Store<any>}) {
        if(properties)
        {
            this.store = properties.store
        }
    }

    async setup() 
    {
        // some setup routine that is called before the Program opens
        await store.setup({ encoding: ... , canAppend: ..., canRead: ...})
    }
}



// Later 

const peer = await Peerbit.create({libp2p: ANOTHER_LIBP2P_INSTANCE })

const program = await peer.open(new StringStore({store: new Store()}), ... options ...)
 
console.log(program.address) // "peerbit/123xyz..." 

// Now you can interact the store through 
program.store.addOperation( ... )
```

See the [DString](./packages/programs/data/string) for a complete working example that also includes a string search index


## [Utils](./packages/utils/)
Utility libraries that do not have their own category yet

The most important module here is 
```@dao-xyz/peerbit-crypto``` that is defining all different key types for signing and encrypting messages.


## Running a server node 
See the [CLI](./packages/server-node)
