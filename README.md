# Sharding + OrbitDB = ShorbitDB

This is a fork of OrbitDB where the codebase is ported to Typescript. Function wise, sharding and query has been introduced into separate modules that can be used to build scalable decentralized databases.

## --> WIP <--  Use with caution

## BPayload [./packages/bpayload](bpayload)
- A generic payload class which you can extend and build binary serialization routines to and from with (@dao-xyz/borsh-ts). For example; Documents that are stored in the  BDocstore [./packages/orbit-db-bdocstore](bdocstore) could/should extend this class so when you are writing or querying data, you can be sure that results with be succesfully be deserialized with BinaryPayload class.


## BQuery  [./packages/bquery](bquery)
- Generalized query models/protocol for distributed queries


## BDocstore [./packages/orbit-db-bdocstore](bdocstore)
- Document store, but different from the "default" orbit-db implementation in the way serialization/deserialization is performed. In addition, this store supports querying. 

## BFeedStore  [./packages/bfeedstore](bfeedstore)
- Same as BDocstore but for "feedstore" (though docstore is superior in many aspects)

## BKVStore  [./packages/bfeedstore](bkvstore)
- Same as BDocstore but for "key value store" (though docstore is superior in many aspects)

## Dynamic Access Controller  [./packages/orbit-db-dynamic-access-controller](dca)
- "Chain agnostic" Access Controller


## Shard [./packages/shard](shard)
- Shard/Container for horizontally scaling databases. 
- Leader and leader rotation routines for building functionality around leaders
- Healthcheck/Redundancy diagnostics for shards.
