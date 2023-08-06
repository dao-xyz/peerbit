# Client 
The peer client
- Open/close programs (databases)

## Installation 
```sh
npm install peerbit
```

## Config

### Memory configuration

The most important configuration is to determine whether you want data to persist between session, or if you want it to persist in memory only
Data is not only things that you generate in your databases, but also keys that are used to encrypt and decrypt them.

[memory](./example.ts ':include :type=code :fragment=memory')


## Connecting nodes

### Bootstrapping
The easiest way to go online is to "bootstrap" your node. At the moment, this will dial all addresses available in the [public bootstrap list](https://github.com/dao-xyz/peerbit-bootstrap/blob/master/bootstrap.env)

[bootstrap](./bootstrap.ts ':include')


### Directly

Peerbit nodes are talking to each other through libp2p which is a transport agnostic way of doing communication. It supports a wide range of protocols, like WebRTC, WebSocket, TCP, WebTransport and can in the future support protocol such as Bluetooth.


Connecting nodes is done through dialing another peer address, which depends on what transport they support.

Below are a few example how you can dial another node 

[connectivity](./connectivity-direct.ts ':include')



### Relayed

Browser-to-browser connections can not be establish without an intermediate node that facilities that a WebRTC direction connection can be established, or if this fails, the intermediate can forward packages from one to another.

You can read more about deploying a relay [here](./deployment/server-node.md)

[connectivity](./connectivity-relay.ts ':include')


## Deployment

### Serverless
Since Peerbit at the current stage is only javascript modules, you can deploy your project to any package manager, like NPM or Github Packages, in order to import them into your app directly, for example a React project or Electron app.

### Server
Sometimes it make sense to deploy a Peerbit on a server that can be accessed through a domain. There are mainly two reasons why you want to do this: 
- Hole punching. Two browser can not connect to each other directly without the aid on an intermediate peer that allows the browser clients to find other
- A replicator that is always online. While a client in the browser can store data themselves, sometimes you need to be sure that there is always one node online. 

To deploy a server node, there is a handy CLI. [See this](https://github.com/dao-xyz/peerbit/tree/master/packages/clients/peerbit-server)