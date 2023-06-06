
<br>
<p align="center">
    <img width="140" src="./docs/peerbit-logo.png"  alt="Peerbit icon Icon">
</p>

<h1 align="center" style="font-size: 5vmin;">
    <strong>
        Peerbit
   </strong>
</h1>

<h3 align="center">
    Develop for a distributed web with Peerbit
</h3>

<h3 align="center">🤫 E2EE &nbsp; &nbsp; 👯 P2P &nbsp; &nbsp; ⚖️ Auto-sharding  &nbsp; &nbsp;  🔍 Searchable</h3>

<p align="center">
<img src="https://github.com/dao-xyz/peerbit/actions/workflows/ci.yml/badge.svg" alt="Tests")
</p>

# A building block for the decentralized web
Peerbit is as easy-to-use as Firebase and provide P2P functionality like OrbitDB or GunJS yet with performance for data-intensive applications like live-streaming and cloud-gaming. It's built on top of Libp2p (and works with IPFS) supporting encryption, sharding and discoverability (searching). 

Your database schema can remain very simple but still utilize P2P networks, auto-scaling, E2E-encryption, discoverability and all other features you'd expect from a database. 

Peerbit values simplicitly. Below is an example how to create a document store, that store posts, that can be modified by anyone.

```typescript 
import { field, option, serialize, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";
import { Documents } from "@dao-xyz/peerbit-document";
import { v4 as uuid } from "uuid";


@variant(0) // version 0
class Post {
    @field({ type: "string" })
    id: string;

    @field({ type: "string" })
    message: string;

    constructor(message: string) {
        this.id = uuid();
		this.message = message;
    }
}

@variant("posts")
class PostsDB extends Program {

    @field({ type: Documents })
    posts: Documents<Post>;

    constructor() {
        super();
        this.posts = new Documents()
    }
    async setup(): Promise<void> {
        await this.posts.setup({ type: Post });
    }
}

// later 
const peer = await Peerbit.create()

// insert
await store.posts.put(new Post("hello world"));

// search for documents from another peer
const peer2 = await Peerbit.create()

// Connec to the first peer
await peer2.dial(peer) 

const store2 = peer2.open(store.address);

let responses: Document[] =  await store2.docs.index.query(
    new SearchRequest({
        queries: [], // query all
    })
);
expect(responses).toHaveLength(1);
expect(responses.map((x) => x.value.message)).toEqual(["hello world"]);
```


## Documentation
[Documentation](https://peerbit.org)

## Contribute
Feel free to contribute!
