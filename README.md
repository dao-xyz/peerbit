
<br>
<p align="center">
    <img width="140" src="./logo512.png"  alt="Peerbit icon Icon">
</p>

<h1 align="center">
    <strong>
        P   e   e   r   b   i   t
   </strong>
</h1>
<h3 align="center">
    Develop for a distributed web with Peerbit
</h3>

<h3 align="center">🤫 E2EE &nbsp; &nbsp; 👯 P2P &nbsp; &nbsp; ⚖️ Auto-sharding  &nbsp; &nbsp;  🔍 Searchable</h3>
<br>

![tests](https://github.com/dao-xyz/peerbit/actions/workflows/ci.yml/badge.svg)

## P2P databases simplified
Started originally as a fork of OrbitDB: A peer-to-peer database on top of IPFS supporting encryption, sharding and discoverability (searching).

Peerbit provides an abstraction layer that lets you program with distributed data types. For example, ```String``` can be replaced with [DString](./packages/programs/data/string) (distributed string). Some datatypes, like [Document store](./packages/programs/data/document) are sharded automatically as long as there are not data dependencies between indiviudal documents.
 
Every peer has an identity which is simply their public key, this key can *currently* either be secp256k1 or a Ed25519 key. To prevent peers from manually sign messages, you can link identities together in a trust graph. This allows you to have a root identity that approves and revokes permissions to keys that can act on your behalf. Hence this allows you to build applications that allows users to act on multiple devices and chains seamlessly.
 
Peers have the possibility to organize themselves into "permissioned" regions. Within a region you can be more confident that peers will respect the sharding distribution algorithm and replicate and index content. Additionally, secret information can be shared freely, this allows peers in the permissioned regions to help each other to decrypt messages in order to be able to index and understand content.
 
Data can be shared and encrypted on a granular level, you can decide exactly what parts of metadata should be public and not. When you create a commit or a query request, you can specify exactly who is going to be able to decrypt the message. If you want an end to end conversation between two identities, you just include the other peers' public key as a receiver and you would be certain that know one in the middle would be able to read your message.

### Goals
The goal of this project is to create a **cheaper** and **more private** way of distributing and accessing data by utilizing consumer hardware and the latest advancements in networking technology. Additionally, we believe that creating a stateful application should and could be made easier if you are approaching it with a P2P database framework like this, since there are no "servers" and "clients", just peers. It should not take longer than a weekend to get started to build your first distributed app!

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


## 🚧 Alpha release  🚧

Backwards compatibility for new releases might be lacking. Use with caution and please report bugs and issues you are experiencing when developing with Peerbit. 

### Documentation is lacking at the moment. Be patient! Read module tests for know to know how you can use different modules. Feel free to write an issue to ask any question!  


## Example code

### Example library project
[A repo containing meaningful examples](https://github.com/dao-xyz/peerbit-examples) is under way. 

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
A (under work) [social media application is developed by dao.xyz](https://github.com/dao-xyz/dao.xyz).


## [Peerbit](./packages/client)
The peer client
- Open/close programs (databases)
- Subscribe to replication topics

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

const peer = await Peerbit.create(IPFS CLIENT, {... options ...})

const program = await peer.open(new StringStore({store: new Store()}), ... options ...)
 
console.log(program.address) // "peerbit/123xyz..." 

// Now you can interact the store through 
program.store.addOperation( ... )
```

See the [DString](./packages/programs/data/string) for a complete working example that also includes a string search index


## Network
Distributing content among untrusted peers will be unreliable and not resilient to malicious parties that starts to participate in the replication process with large amount (>> min replicas) of nodes and shutting them down simultaneously (no way for the original peers recover all lost data). To mitigate this you can launch your program in a "Network", which is basically a list of nodes that trust each other. Symbolically you could thing of this as a VPC.

To do this, you only have to implement the "Network" interface: 
```typescript
import { Peerbit, Network } from '@dao-xyz/peerbit'
import { Store } from '@dao-xyz/peerbit-store'
import { Program } from '@dao-xyz/peerbit-program' 
import { TrustedNetwork } from '@dao-xyz/peerbit-trusted-network' 
import { field, variant } from '@dao-xyz/borst-ts' 

@variant("string_store") 
class StringStore extends Program implements Network 
{
    inNetwork: true = true // This needs to be included for type safety reasons

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
const peer1 = await Peerbit.create(IPFS_CLIENT, {... options ...})
const peer2 = await Peerbit.create(IPFS_CLIENT_2, {... options ...})

const programPeer1 = await peer1.open(new StringStore({store: new Store(), network: new TrustedNetwork()}), {... options ...})

// add trust to another peer
await program.network.add(peer2.identity.publicKey) 


// peer2 also has to "join" the network, in practice this means that peer2 adds a record telling that its Peer ID trusts its IPFS ID
const programPeer2 = await peer2.open(programPeer1.address, {... options ...})
await peer2.join(programPeer2) // This might fail with "AccessError" if you do this too quickly after "open", because it has not yet recieved the full trust graph from peer1 yet
```

See [this test(s)](./packages/client/src/__tests__/network.test.ts) for working examples

## [Utils](./packages/utils/)
Utility libraries that do not have their own category yet

The most important module here is 
```@dao-xyz/peerbit-crypto``` that is defining all different key types for signing and encrypting messages.


## Running a server node 
See the [CLI](./packages/server-node)
