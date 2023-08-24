# Programs
You can think of the `program` component as a database definition with all associated logic: Who can append? Who can read? What happens when an entry gets added? 


In the *future* `program` will more or less behave like a smart contract, with its own runtime, but for now they are simple JavaScript classes that extend the ```Program``` class. 

## Example

[definition](./example.ts ':include :fragment=definition')


A `program` lets you write a control mechanism for Append-Only logs (which are represented as a [Log](./packages/log).


## Encoding
As you might have seen, the `@field` and `@variant` decorators are used in multiple places. They indicate how the client should serialize/deserialize the databases and content you create. Peerbit uses a serialization format by default called Borsh, which provides similar functionality to Protobuf but is simpler to understand - and the Typescript implementation requires less code than a Protobuf equivalent (and is faster). 

Read more about the encoding [here](./../encoding/encoding.md)
