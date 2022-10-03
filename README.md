
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

## 🚧 WIP 🚧  Use with caution
Below are descriptions of some modules

## [Peerbit](./packages/client)
The peer client
- Open/close stores (databases)
- Exchange keys
- Exchange replication info. Healthcheck/Redundancy diagnostics. 
- Leader and leader rotation routines for building functionality around leaders

## [query-protocol](./packages/store/query-protocol)
- Generalized query models/protocol for distributed queries

## [pbayload](./packages/utils/bpayload)
- A generic payload class which you can extend and build binary serialization routines to and from with (@dao-xyz/borsh-ts). For example; Documents that are stored in the [bdocstore](./packages/orbit-db-bdocstore) could/should extend this class so when you are writing or querying data, you can be sure that results with be succesfully be deserialized with BinaryPayload class.


## [bdocstore](./packages/store/orbit-db-bdocstore)
- Document store, but different from the "default" orbit-db implementation in the way serialization/deserialization is performed. In addition, this store supports querying. 

## [bfeedstore](./packages/store/bfeedstore)
- Same as BDocstore but for "feedstore" (though docstore is superior in many aspects)

## [dac](./packages/acl/dynamic-access-controller)
- "Chain agnostic" Access Controller


