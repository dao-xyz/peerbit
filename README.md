
<br>
<p align="center">
    <img width="400" src="./peer.png"  alt="Peerbit icon Icon">
</p>

<h1 align="center">
    <strong>
        Peerbit
   </strong>
</h1>

<h3 align="center">
    Develop for a distributed web with Peerbit
</h3>

<h3 align="center">🤫 E2EE &nbsp; &nbsp; 👯 P2P &nbsp; &nbsp; ⚖️ Auto-sharding  &nbsp; &nbsp;  🔍 Searchable</h3>
<br>


## What is this?
Started originally as a fork of OrbitDB: A peer-to-peer database on top of IPFS supporting, encryption, sharding and discoverability (searching). Peers have the possibility to organize themselves into "permissioned" regions. Within a region, secret information can be shared freely, this allows peers to create locally centralized database clusters with efficient replication, sharding, query yet still allowing cross trust region (low trust) activities, like relaying encrypted and signed messages. Data can be shared and encrypted on a granular level, you can decide exactly what parts of metadata should be public and not.

### Some informational links are found below
[How Peerbit differs from OrbitDB](./documentation/difference.md)

[How Peerbit performs sharding](./documentation/sharding/sharding.md)

[Encryption scheme](./documentation/encryption.md)


## 🚧 Alpha release  🚧

Backwards compatibility for new releases might be lacking. Use with caution and please report bugs and issues you are experiencing when developing with Peerbit. 

### Documentation is lacking at the moment. Be patient! Read module tests for know to know how you can use different modules. Feel free to write an issue to ask any question!  


## Example code

### Collaborative text 
Below is a short example how you can create a collaborative text document: 

```typescript
import { DString, Range } from '@dao-xyz/peerbit-string'
import { Peerbit } from '@dao-xyz/peerbit'
import { Program } from '@dao-xyz/peerbit-program'
import { SignKey } from '@dao-xyz/peerbit-crypto';
import { Range, DString, StringOperation } from '@dao-xyz/peerbit-string';
import { field, variant } from '@dao-xyz/borst-ts' 

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

    async canRead(identity?: SignKey): Promise<boolean> {
        // .. acl logic for readers
    }

}

// ... 

const peer = await Peerbit.create (ipfs, options ...)
const document = peer.open(new CollaborativeText());
console.log(document.address) /// this address can be opened by another peer 


//  ... 
await document.string.add('hello', new Range({ offset: 0n, length: 6n }));
await document.string.add('world', new Range({ offset: 7n, length: 5n }));

console.log(await document.string.toString()) // 'hello world' from local store
console.log(await document.string.toString({remote: {maxAggregationTime: 3000 }})) // 'hello world' from peers

```

### Social media app
A (under work) [social media application is developed by dao.xyz](https://github.com/dao-xyz/dao.xyz). Contains a frontend, CLI (for managing nodes) and frontend.


Below are descriptions of some modules

## [Peerbit](./packages/client)
The peer client
- Open/close stores (databases)
- Exchange keys
- Exchange replication info. Healthcheck/Redundancy diagnostics. 
- Leader and leader rotation routines for building functionality around leaders

### Installation 
```sh
npm install @dao-xyz/peerbit
```

```typescript
import { Peerbit } from '@dao-xyz/peerbit'

// Create a peer from an ipfs instance
const peer = await Peerbit.create(IPFS CLIENT, {... options ...})

// Open a program 
const program = await peer.open(PRORGAM ADDRESS or PRORGAM)
program.doThings()
```

## [Programs](./packages/programs)
Contains composable programs you can build your program with. For example distributed [document store](./packages/programs/data/document), [search](./packages/programs/discovery/any-search), [chain agnostic access controller](./packages/programs/acl/identity-access-controller) 

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

const peer = await Peerbit.create(IPFS CLIENT, {... options ...})

const program = await peer.open(new StringStore({store: new Store()}), ... options ...)
 
console.log(program.address) // "peerbit/123xyz..." 

// Now you can interact the store through 
program.store.addOperation( ... )
```

See the [DString](./packages/programs/data/string) for a complete working example that also includes a string search index


### Controlled replication in a with VPC
Distributing content among untrusted peers will be unreliable and not resiliant to malicious parties that takes over the network by participating with multiple nodes. To mitigate this you can launch you program in a "VPC", which is basically a network of nodes that trust each other. 

To do this, you only have to implement the "VPC" interface: 
```typescript
import { Peerbit, VPC } from '@dao-xyz/peerbit'
import { Store } from '@dao-xyz/peerbit-store'
import { Program } from '@dao-xyz/peerbit-program' 
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network' 
import { field, variant } from '@dao-xyz/borst-ts' 

@variant("string_store") // Needs to have a variant name so the program is unique
class StringStore extends Program implements VPC // Needs to extend Program if you are going to store Store<any> in your class
{
    networkType: 'VPC' = 'VPC' // add network type 

    @field({type: Store})
    store: Store<string>

    @field({type: TrustedNetwork}) 
    network: TrustedNetwork // this is a database storing all peers. Peers that are trusted can add new peers

    constructor(properties?:{ store: Store<any>, network: TrustedNetwork }) {
        if(properties)
        {
            this.store = properties.store
        }
    }

    async setup() 
    {
        await store.setup({ encoding: ... , canAppend: ..., canRead: ...})
        await trustedNetwork.setup()
    }
}


// Later 
const peer1 = await Peerbit.create(IPFS CLIENT, {... options ...})
const peer2 = await Peerbit.create(IPFS CLIENT 2, {... options ...})

const programPeer1 = await peer1.open(new StringStore({store: new Store(), network: new TrustedNetwork()}), ... options ...)

// add trust to another peer
await program.network.add(peer2.id) 


// peer2 also has to "join" the network, in practice this that peer2 add a record telling that its Peer ID trusts its IPFS ID

const programPeer2 = await peer2.open(programPeer1.address, ... options ...)
await peer2.join(programPeer2) // This might fail if you do this too quickly after "open", because it has not yet recieved all data from the network changes from peer1
```

See [this test(s)](./packages/client/src/__tests__/network.test.ts) for working examples

## [Utils](./packages/utils/)
Utility libraries that do not have their own category yet

The most important module here is 
```@dao-xyz/peerbit-crypto``` that is defining all different key types for signing and encrypting messages.


## CLI 
Peerbit has a CLI for running a non browser node. Tested with Node with version > 16.x

```sh
npm install -g @dao-xyz/peerbit-node
```

Documentation
```
peerbit --help
```
