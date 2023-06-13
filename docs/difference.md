
# How Peerbit differs in general with other technologies
## Comparison table
The table below briefly outlines the major difference between related frameworks.

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


## How Peerbit differs from OrbitDB
 
Peerbit started as a fork of OrbitDB in order to support some key features that are important for building applications for the modern web. Along the way, many changes have been made improve various aspects of the project.
 
 
### Coding experience
- All modules have been rewritten in TypeScript. There are many benifits of this, one is that we can omit a large amount of type checking code.
- Monorepo. OrbitDB is scattered around many repositories which slows down development speed. Peerbit has collected all modules in one repo with [lerna](https://github.com/lerna/lerna).
- **ESM-only** because we need to be compatible with the latest vestions if `js-ipfs` and `libp2p`
- Tests are now written in `jest` rather than `mocha`
- Type safety. Messages are encoded with [Borsh](https://github.com/near/borsh) serialization format. This format allows one to have a strict type checks during serialization/deserialization and enables message polymorphism which JSON does not (in a easy way) support and provides a much more compact data representation.
- Performance critical modules are written with the mindset that they might be ported to Rust/WASM at some point.
- In OrbitDB it was assumed that a store always has an access controller, here, we don't make that assumption. In fact every "program" that is allowed to update a state needs to define checks ```canAppend```, ```canRead``` or delagate these checks to some "access controller", there are a few predefined ACLs [here](../packages/programs/acl). This way, you have more freedom when creating "programs" as you can pick, choose and combine programs to build the functionality you want to achieve. 
  
### Encryption
- OrbitDB did not support read access control, this feature is much more complicated to achieve in comparison to write access as one also needs to gatekeep data in some way (encrypted storage). In addition to this, one needs to build a framework around managing encryption keys, relaying encrypted messages (1-N encryption) and a query/search framework compatible with encrypted content and metadata. Peerbit supports this, and does so at a granular level.
 
### Performance
- Peerbit tries to solve key problems building distributed databases at scale, for example what happens when a database grows too large? How can sharding create data boundaries that still lets "weak" devices participate in the replication process? Peerbit supports sharding, more or less, seamlessly by building a dependency graph (references) of log entries, so that different nodes can be part of replicating different parts of the log, yet not corrupt any Index built on top of the log.
 
- Not only can the amount of entries in a database grow to a huge number, but the number of database/stores that a node replicate can also grow immensely in real world applications. For example, imagine building a social network where each post has edit capabilities, its own access control, encryption and so on. This could be done by modeling the post as individual databases to contain all the edits. If each post would represent a new database, then we need to really make sure that we can efficiently open a database and transmit to peers that it needs to be replicated. Peerbit is working towards this by allowing replication messages on shared topics, allowing stores to share access controllers and making sure that database manifests are compact in memory.
 
 
### Discoverability
- Storing data is half the purpose of a database. If data is hard to discover, the storage is meaningless. In a distributed setting we need to make sure different participants can get involved based on their capabilities, some peers might be able to answer very complicated query, some peers might just support some basic filtering. Peerbit supports composability for queryable programs. For example, there is a generic RPC program [RPC](../packages/programs/discovery/any-search/) that allows you to build query functionality around peers' data (like an ordinary REST service).
 

 
