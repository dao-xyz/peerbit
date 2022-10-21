
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
Started originally as a fork of OrbitDB: A peer-to-peer database on top of IPFS supporting, encryption, sharding and discoverability (searching). Peers are a organizing themselves into "permissioned" regions. Within a region, secret information can be shared freely, this allow peers to create locally centralized database clusters with efficient replication, sharding, query yet still allowing cross trust region (low trust) activities, like relying encrypted and signed messages. Data can be shared and encrypted on a granular level, you can decide exactly what parts of metadata should be public and not.

### [How Peerbit differs from OrbitDB](./documentation/DIFFERENCE.md)


## Example code 
Below is a short example how you can create a collaborative text document: 

```typescript
import { DString} from '@dao-xyz/peerbit-dstring'
import { Peerbit } from '@dao-xyz/peerbit'
import { Program, RootProgram } from '@dao-xyz/peerbit-program'

class CollaborativeText extends Program implements RootProgram
    
    @field({type: DString})
    dstring: DString // distributed string 

    constructor()
    {
        this.dstring = new DSstring()
    }

    async setup()
    {
        await this.dstring.setup({canAppend: this.canAppend, canRead: this.canRead})
    }

    async canAppend(payload, identity): Promise<boolean>
    {
       // .. acl logic writers
    }

    async canRead(identity): Promise<boolean>
    {
        // .. acl logic for readers
    }

}

// ... 

const peer = Peerbit.createInstance (ipfs, options ...)
const document = peer.open(new CollaborativeText());
console.log(document.address) /// this address can be opened by another peer 


//  ... 
await document.add('hello', new Range({ offset: 0n, length: 'hello'.length }));
await document.add('world', new Range({ offset: BigInt('hello '.length), length: 'world'.length }));

console.log(await document.dstring.toString()) // 'hello world'
```

</br>
</br>
</br>

## 🚧 WIP 🚧  Use with caution

</br>
</br>
</br>

Below are descriptions of some modules

## [Peerbit](./packages/client)
The peer client
- Open/close stores (databases)
- Exchange keys
- Exchange replication info. Healthcheck/Redundancy diagnostics. 
- Leader and leader rotation routines for building functionality around leaders

## [Programs](./packages/programs)
Contains composable programs you can build your program with. For example distributed [document store](./packages/programs/data/ddoc), [search](./packages/programs/discovery/dsearch), [chain agnostic access controller](./packages/programs/acl/dynamic-access-controller) 

## [Utils](./packages/utils/)
Utilities libraries that do not have their own category yet
