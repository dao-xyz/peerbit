# A building block for the decentralized web
Peerbit is as easy-to-use as Firebase and provide P2P functionality like OrbitDB or GunJS yet with performance for data-intensive applications like live-streaming and cloud-gaming. It's built on top of Libp2p (and works with IPFS) supporting encryption, sharding and discoverability (searching). 

Your database schema can remain very simple but still utilize P2P networks, auto-scaling, E2E-encryption, discoverability and all other features you'd expect from a database. 

Peerbit values simplicitly. Below is an example how to create a document store, that store posts, that can be modified by anyone.

[data](./examples/document-store.ts ':include :type=code :fragment=data')

Later 

[data](./examples/document-store.ts ':include :type=code :fragment=insert')

# Scalability

## Throughput
Peerbit is performant, so performant in fact you can use it for [streaming video](https://stream.dao.xyz) by having peers subscribing to database updates. In a low latency setting, you can achieve around 1000 replications a second and have a thoughput of 100 MB/s. 

![Dogestream](/videostream.gif)

*Left side is putting video frames in a [document store](https://github.com/dao-xyz/peerbit-examples/blob/master/packages/live-streaming/frontend/src/media/database.ts), every few ms. Right side is subscribed to changes of the document store and renders the changes once they arrive. [Source code](https://github.com/dao-xyz/peerbit-examples/tree/master/packages/live-streaming).*

Peerbit provides an abstraction layer that lets you program with distributed data types. For example, ```String``` can be replaced with [DString](./packages/programs/data/string) (distributed string). Some datatypes, like [Document store](./packages/programs/data/document) are sharded automatically as long as there are not data dependencies between indiviudal documents.

Every peer has an identity which is simply their public key, this key can *currently* either be secp256k1 or a Ed25519 key. To prevent peers from manually sign messages, you can link identities together in a trust graph. This allows you to have a root identity that approves and revokes permissions to keys that can act on your behalf. Hence this allows you to build applications that allows users to act on multiple devices and chains seamlessly.
 
Data can be shared and encrypted on a granular level, you can decide exactly what parts of metadata should be public and not. When you create a commit or a query request, you can specify exactly who is going to be able to decrypt the message. If you want an end to end conversation between two identities, you just include the other peers' public key as a receiver and you would be certain that know one in the middle would be able to read your message.

## Scaling networks
Peerbit is built on top of a [pubsub](./../packages/transport/direct-sub/) protocol that automatically optimizes the routing for packages so that the network stays overall healthy. If some path in the network gets congested, packages are routed with alternative routes, potentially over WebRTC, Websocket and TCP connections. 

This is useful when you are building a app that requires streaming large amount of data, with a network consisting of peers with limited bandwidth. An example is a streaming service, where a streamer can write video stream chunks into a database and these chunks can propagage to thousands of peers without having to send to all of them directly.


# Goals
The goal of this project is to create a  <span style="color:coral;">cheaper</span> and a more <span style="color:coral;">private</span> way of distributing and accessing data by utilizing consumer hardware and the latest advancements in networking technology. Additionally, we believe that creating a stateful application should and could be made easier if you are approaching it with a P2P database framework like this, since there are no "servers" and "clients", just peers. It should not take longer than a weekend to get started to build your first distributed app!


# 🚧 Beta release  🚧
Backwards compatibility for new releases might be lacking. Use with caution and please report bugs and issues you are experiencing when developing with Peerbit. 
