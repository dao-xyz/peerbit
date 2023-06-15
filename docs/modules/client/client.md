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

### Directly

Peerbit nodes are talking to each other through libp2p which is a transport agnostic way of doing communication. It supports a wide range of protocols, like WebRTC, WebSocket, TCP, WebTransport and can in the future support protocol such as Bluetooth.


Connecting nodes is done through dialing anothers peer address, which depends on what transport they support.

Below are a few example how you can dial another node 

[connectivity](./connectivity-direct.ts ':include')



### Relayed

Browser-to-browser connections can not be establish without an intermediate node that facilities that a WebRTC direction connection can be established, or if this fails, the intermediate can forward packages from one to another.

Peerbit support multi

[connectivity](./connectivity-relay.ts ':include')
