
# pbayload

A generic payload class which you can extend and build binary serialization routines to and from with (@dao-xyz/borsh-ts). For example; Documents that are stored in the [bdocstore](./packages/orbit-db-bdocstore) could/should extend this class so when you are writing or querying data, you can be sure that results with be succesfully be deserialized with BinaryPayload class.