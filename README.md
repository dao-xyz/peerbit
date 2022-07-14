# Sharding + OrbitDB = ShorbitDB

This is a fork of OrbitDB where the codebase is ported to Typescript. Function wise, sharding and query has been introduced into separate modules that can be used to built scalable decentralized databases.


--> WIP <-- 
Use with caution

## BPayload [./packages/bpayload](bpayload)
- A generic payload class which you can extend and build binary serialization routines to and from with (@dao-xyz/borsh-ts). For example; Documents that are stored in the  BDocstore [./packages/orbit-db-bdocstore](bdocstore) could/should extend this class so when you are querying data, you can be sure that results with be succesfully be deserialized with BinaryPayload class. 



## BDocstore [./packages/orbit-db-bdocstore](bdocstore)
- Document store, but different from the "default" orbit-db implementaiton in the way serialization/deserialization is done. Support for queries across peers

## Shard [./packages/shard](shard)
- Shard/Container for horizontally scaling databases. 
- Leader and leader rotation routines for building functionality around leaders
- Healthcheck/Redudancy diagnotics. Get info about a shard
