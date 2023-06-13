# Client 
The peer client
- Open/close programs (databases)

## Installation 
```sh
npm install @dao-xyz/peerbit
```

## Config

### Memory configuration

The most important configuration is to determine whether you want data to persist between session, or if you want it to persist in memory only
Data is not only things that you generate in your databases, but also keys that are used to encrypt and decrypt them.

[memory](./example.ts ':include :type=code :fragment=memory')


## Connecting nodes

Peerbit nodes are talking to each other through libp2p which is a transport agnostic way of doing communication. It supports a wide range of protocols, like WebRTC, WebSocket, TCP, WebTransport and can in the future support protocol such as Bluetooth.


Connecting nodes is done through dialing anothers peer address, which depends on what transport they support.

Below are a few example how you can dial another node 

[connectivity](./connectivity.ts ':include')




### Running a server node
If you want to deploy a node as server, there is a easy-to-use CLI that allows you to manage it. You can also obtain a free subdomain for testing purposes!

Check out the [CLI](../../../packages/server-node/README.md ':include :relative') 
