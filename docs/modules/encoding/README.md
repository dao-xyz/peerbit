# Encoding
## How encoding works
Great performance is one of the design goals of Peerbit. To achieve this, Peerbit is built on top of a "no-compromise" encoding scheme.

Generally, it is common to stick with JSON when you are not working with performance-critical applications, or BSON if you intend to store byte arrays as fields. For performance-critical applications, Protobuf is a popular choice. However, all choices have their compromises, as one solution may be easy to debug while the other is performant but opaque.

Peerbit is built on top of the Borsh encoding specification, which has an easy-to-use implementation in many languages, most importantly in [Typescript](https://github.com/dao-xyz/borsh-ts).

The main reasons are outlined below:

- The Borsh specification is very simple. Fields are serialized in a specific order, and sizes are fixed. For example, arrays by default use unsigned 4-byte integers (`u32`) to encode the size. [Read more here](https://borsh.io/). This way of encoding can outperform Protobuf in many ways since it does not rely on variable-length encoded numbers (varint), and the fixed field order eliminates the need to encode field IDs and wire types. Furthermore, this also makes the encoding more friendly towards SIMD, which can greatly accelerate encoding tasks.

- Deterministic behavior is important. With a fixed-order encoding, you can be sure that encoding messages will yield the same output every time. In contrast to JSON, where fields can be serialized in any order.

- Versioning. With Borsh, it is easy to provide data discriminators (version info) that makes migrations easy and reliable.


Below is an example how one would encode a "Post" object with Borsh encoding. 

[borsh](./borsh.ts ':include')


**Read more about all available field types in the Typescript implementation of Borsh [here](https://github.com/dao-xyz/borsh-ts)**



## Using Protobuf or JSON with Peerbit
Even though the first class encoding is done using Borsh encoding scheme, you can still use any encoding layer with your data. see [borsh](./json.ts ':include') in `json.ts`.
