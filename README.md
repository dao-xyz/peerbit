
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

<p align="center">
<img src="https://github.com/dao-xyz/peerbit/actions/workflows/ci.yml/badge.svg" alt="Tests")
</p>

## P2P database for scalable applications
Peerbit is as easy-to-use as Firebase and provide P2P functionality like OrbitDB or GunJS yet with performance for data-intensive applications like live-streaming and cloud-gaming. It's built on top of Libp2p (and works with IPFS) supporting encryption, sharding and discoverability (searching). 

Your database schema can remain very simple but still utilize P2P networks, auto-scaling, E2E-encryption, discoverability and all other features you'd expect from a database. 

### Comparison with alternatives

||<sub>Peerbit</sub>|<sub>OrbitDB</sub>|<sub>gunJS</sub>|<sub>IPFS</sub>|<sub>Arweave</sub>|
| ------------ | ------------ | ------------ | ------------ | ------------ | ------------ |
|<sub>**Performance**</sub>|<sub>Highly performant. E.g. video-streaming, real-time editing & cloud-gaming</sub>|<sub>Chat-rooms, document store</sub>|<sub>Performant applications</sub>|<sub>File-storage</sub>|<sub>File-storage</sub>|
|<sub>**Search**</sub>|<sub>X</sub>|   |<sub>X</sub>|   |   |
|<sub>**Browser-friendly**</sub>|<sub>X</sub>|   |<sub>X</sub>|<sub>X</sub>|   |
|<sub>**CID**</sub>|<sub>X</sub>|<sub>X</sub>|   |<sub>X</sub>|<sub>X</sub>|
|<sub>**Sharding**</sub>|<sub>X</sub>|   |   |   |   |
|<sub>**Built-in encryption**</sub>|<sub>X</sub>|   |   |   |   |
|<sub>**Supports multiple key-types**</sub>|<sub>X</sub>|   |   |<sub>X</sub>|   |
|<sub>**Discovery algorithm**</sub>|<sub>Automatic based on content</sub>|<sub>KAD DHT</sub>|<sub>Custom solution</sub>|<sub>KAD DHT</sub>|<sub>KAD DHT</sub>|
|<sub>**Client language**</sub>|<sub>Typescript</sub>|<sub>Javascript</sub>|<sub>Javascript</sub>|<sub>Typescript, Go, Rust</sub>|<sub>Typescript/Javascript, PHP</sub>|
|<sub>**Intended usage**</sub>|<sub>High-performance applications & storage|<sub>Databases on IPFS</sub>|<sub>Performant applications</sub>|<sub>Granular control of individual files</sub>|<sub>?Permanent? storage</sub>|


### Performance
Peerbit is performant, so performant in fact you can use it for [streaming video](https://stream.dao.xyz) by having peers subscribing to database updates. In a low latency setting, you can achieve around 1000 replications a second and have a thoughput of 100 MB/s. 

![Dogestream](/videostream.gif)

*Left side is putting video frames in a [document store](https://github.com/dao-xyz/peerbit-examples/blob/master/packages/live-streaming/frontend/src/media/database.ts), every few ms. Right side is subscribed to changes of the document store and renders the changes once they arrive. [Source code](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/live-streaming).*

Peerbit provides an abstraction layer that lets you program with distributed data types. For example, ```String``` can be replaced with [DString](./packages/programs/data/string) (distributed string). Some datatypes, like [Document store](./packages/programs/data/document) are sharded automatically as long as there are not data dependencies between indiviudal documents.

Every peer has an identity which is simply their public key, this key can *currently* either be secp256k1 or a Ed25519 key. To prevent peers from manually sign messages, you can link identities together in a trust graph. This allows you to have a root identity that approves and revokes permissions to keys that can act on your behalf. Hence this allows you to build applications that allows users to act on multiple devices and chains seamlessly.
 
Data can be shared and encrypted on a granular level, you can decide exactly what parts of metadata should be public and not. When you create a commit or a query request, you can specify exactly who is going to be able to decrypt the message. If you want an end to end conversation between two identities, you just include the other peers' public key as a receiver and you would be certain that know one in the middle would be able to read your message.



### Goals
The goal of this project is to create a  <span style="color:coral;">cheaper</span> and a more <span style="color:coral;">private</span> way of distributing and accessing data by utilizing consumer hardware and the latest advancements in networking technology. Additionally, we believe that creating a stateful application should and could be made easier if you are approaching it with a P2P database framework like this, since there are no "servers" and "clients", just peers. It should not take longer than a weekend to get started to build your first distributed app!

### Timeline and progress
- 🚧 Documentation and examples
- 🚧 Easy Webtransport/WebRTC setup for device to device networks
- Performant indexation capabilities with WASM search modules
- Improved sharding algorithm that respects device capabilities
- [ZK group access controller](https://vitalik.ca/general/2022/06/15/using_snarks.html)
- E2EE forward secrecy (or alternative security measures)


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

## [Peerbit](./packages/client)
The peer client
- Open/close programs (databases)

### Installation 
```sh
npm install @dao-xyz/peerbit
```

```typescript
import { Peerbit } from '@dao-xyz/peerbit'

const peer = await Peerbit.create()

// Open a program 
const program = await peer.open(PRORGAM ADDRESS or PRORGAM)
program.doThings()
```

## [Programs](./packages/programs)
Contains composable programs you can build your program with. For example distributed [document store](./packages/programs/data/document), [clock service](./packages/programs/clock-service), [chain agnostic access controller](./packages/programs/acl/identity-access-controller) 

A program lets you write control mechanism for Append-Only logs (which are represented as a [Log](./packages/log), example program

```typescript 
import { Peerbit } from '@dao-xyz/peerbit'
import { Log } from '@dao-xyz/peerbit-log'
import { Program } from '@dao-xyz/peerbit-program' 
import { field, variant } from '@dao-xyz/borst-ts' 

@variant("string_store") // Needs to have a variant name so the program is unique
class StringStore extends Program  // Needs to extend Program if you are going to store Store<any> in your class
{
    @field({type: Log}) // decorate it for serialization purposes 
    log: Log<string>

    constructor(properties: { log?: Log<string>}) {
        this.log = properties.log || new Log()
    }

    async setup() 
    {
        // some setup routine that is called before the Program opens
        await log.setup({ encoding: ... , canAppend: ..., canRead: ...})
    }
}



// Later 

const peer = await Peerbit.create()

const program = await peer.open(new StringStore(), ... options ...)
 
console.log(program.address) // "peerbit/123xyz..." 

// Now you can interact the log through 
program.log.append( ... )
```

See the [DString](./packages/programs/data/string) for a complete working example that also includes a string search index


## [Utils](./packages/utils/)
Utility libraries that do not have their own category yet

The most important module here is 
```@dao-xyz/peerbit-crypto``` that is defining all different key types for signing and encrypting messages.


## Running a relay or replicating server node 
Check out the [CLI](./packages/server-node)
