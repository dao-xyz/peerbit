# Client 
The peer client
- Open/close programs (databases)

## Installation 
```sh
npm install peerbit
```

## Config

### Memory configuration

The most important configuration is to determine whether you want data to persist between session, or if you want it to persist in memory only.
Data is not only things that you generate in your databases, but also keys that are used to encrypt and decrypt them.

[memory](./example.ts ':include :type=code :fragment=memory')


## Connecting nodes

### Bootstrapping
The easiest way to go online is to "bootstrap" your node. At the moment, this will dial all addresses available in the [public bootstrap list](https://github.com/dao-xyz/peerbit-bootstrap/blob/master/bootstrap.env)

[bootstrap](./bootstrap.ts ':include')


### Directly

Peerbit nodes are talking to each other through libp2p, which is a transport-agnostic way of doing communication. It supports a wide range of protocols, like WebRTC, WebSocket, TCP, WebTransport and can in the future support protocols such as Bluetooth.


Connecting nodes is done through dialing another peer address, which depends on what transports they support.

Below are a few examples of how you can dial another node 

[connectivity](./connectivity-direct.ts ':include')



### Relayed

Browser-to-browser connections can not be established without an intermediate node that facilities a WebRTC direct connection. Or, if this fails, the intermediary can forward packages from one to the other.

You can read more about deploying a relay [here](/modules/deploy/server/)

[connectivity](./connectivity-relay.ts ':include')

