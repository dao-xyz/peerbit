# @peerbit/indexer-proxy

Proxy RPC contract and client for @peerbit/indexer-interface using borsh-rpc.

- Exposes IndicesRPCContract service for binding on the server side.
- Provides IndicesClient, createIndexProxy, and iterator helpers on the client side.

Install:

```sh
npm i @peerbit/indexer-proxy
```

Usage:

```ts
import { LoopbackPair, bindService, createProxyFromService, registerDependencies } from "@dao-xyz/borsh-rpc";
import { IndicesRPCContract, IndicesClient } from "@peerbit/indexer-proxy";

// register schema ctors used across the wire
registerDependencies(IndicesRPCContract as any, { Model });

// server
bindService(IndicesRPCContract as any, transportA, new (IndicesRPCContract as any)(impl));

// client
const rpc = createProxyFromService(IndicesRPCContract as any, transportB) as any;
const indices = new IndicesClient(rpc);
const index = await indices.init({ schema: Model, indexBy: ["id"] });
await index.put(new Model("a"));
```