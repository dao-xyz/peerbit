# Client 
The peer client
- Open/close programs (databases)

### Installation 
```sh
npm install @dao-xyz/peerbit
```

```typescript
import { Peerbit } from '@dao-xyz/peerbit'

const peer = await Peerbit.create()

// Open a program 
const program = await peer.open(PRORGAM ADDRESS or PROGRAM)
program.doThings()
```


# Server client 
[server-node](./server-node.md ':include')