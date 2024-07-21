# Indexing and persistance with Peerbit

## Introduction

Indexing and persistance are two important features of Peerbit. Indexing allows you to search for documents in a collection, while persistance allows you to store documents in a collection and retrieve them later. Persistance indexing capabilities allows us to also use less RAM and offload resources to disk which is important for large scale applications.

For both to co-exist we need a efficient indexing backend that allows us to offloading the mem

Consider follow program 

```ts 
import { Documents } from "@peerbit/document";

@variant(0) // version 0
export class Post {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	message: string;

	@field({ type: option("string") })
	parentPostid?: string; // if this value exists, then this post is a comment

	constructor(message: string) {
		this.id = uuid();
		this.message = message;
	}
}


@variant("channel")
export class Channel extends Program<ChannelArgs> {
	// Documents<?> provide document store functionality around posts

	@field({ type: Documents })
	posts: Documents<Post>;

	constructor() {
		super();
		this.posts = new Documents({
			id: sha256Sync(new TextEncoder().encode("posts")),
		});
	}

	// Setup will be called on 'open'
	async open(): Promise<void> {
		
		await this.posts.open({
			type: Post
		});
	}
}
 
const channel = await client.open(new Channel())
await channel.posts.put(new Post("Hello World!"))

// find the post 
const result = await channel.posts.index.search(new SearchRequest({message: 'Hello World!'}))
console.log(result) // [Post]
```

Here it is quite obvious that the `@peerbit/document` needs an efficient way of managing the index given that we want to look for our data in various different ways. Remember that a document store potentially locally could have millions of documents, and globally billions of documents could be of existence.

This naturally leads to the conclusion that it is not only the searching for documents that needs to be efficient, but also the syncing layer and oplog layer too.

The [@peerbit/indexer-interface](https://github.com/dao-xyz/peerbit/tree/master/packages/utils/indexer/interface) together with two implementations [@peerbit/indexer-memory](https://github.com/dao-xyz/peerbit/tree/master/packages/utils/indexer/simple) and [@peerbit/indexer-sqlite](https://github.com/dao-xyz/peerbit/tree/master/packages/utils/indexer/sqlite3) provides a way to manage the index in a efficient way which is the backbone of the [@peerbit/document](https://github.com/dao-xyz/peerbit/tree/master/packages/programs/data/document/document) package. The benifit of using SQLite is that there are implementations that both can run in the browser (with OPFS) and natively.

By providing the `type` field in the open arguments to the documents store, we can specify the type of the document that we are storing. This allows the documents store to create a index for the document type. 

What is interesting with the approach for the SQlite integration is that we can utilize the `borsh` serialization library to generate all necessary information for the index (tables).


## SQLite with Borsh in detail
Consider this somewhat complicated example 

```ts
abstract class Content {}

@variant(0)
class Text extends Content {

    @field({ type: "string" })
    message: string;

    constructor(message: string) {
          super()
        this.message = message;
    }
}

@variant(1)
class Image extends Content {

    @field({ type: Uint8array })
    data: Uint8array;

    constructor(data: Uint8array) {
        super()
        this.data = data;
    }
}


class Post {

    @field({ type: "string" })
    id: string;

    @field({ type: "string" })
    message: string;

    @field({ type: option("string") })
    parentPostid?: string; // if this value exists, then this post is a comment

    @field({ type: vec(Content) })
    content: Content[];

    constructor(message: string,  content: Content,parentPostid?: string) {
        this.id = uuid();
        this.message = message;
        this.content = content
    }
}
```

When this Post type is to be indexed in SQLite, the [@peerbit/indexer-sqlite](https://github.com/dao-xyz/peerbit/tree/master/packages/utils/indexer/sqlite3) package will generate a table for the Post type, and multiple tables for the Content type each with a foreign key to the Post table. This allows us to search for the Post type based on the content type. Additionaly since a post can have many contents, we also need to keep track of the order of the content and make sure the order is preserved when we reconstruct the object if we match against this post in a search query that would looks something like this 

```ts
await posts.index.search(new SearchRequest({content: { message: 'Hello World!' }}))

```

Having many tables can be efficient since for every search we potentially need to join multiple tables. To faciliate better performance the [@peerbit/indexer-sqlite](https://github.com/dao-xyz/peerbit/tree/master/packages/utils/indexer/sqlite3) will inline the content fields into the Post table if it can be done without loss of information. This is done by [identifying]() whether polymorphism is expected to be used in a context. This quickly becomes complicated because we could have endless nestling of polymorphic types and sometimes inlining and we both need to keep track of this when inserting but also when querying and reconstructing the original object. 

Additionaly retrieving results can be inefficient to since pulling the data from an index and reconstruct the original object might us to do multiple joins. To faciliate all indexers can support ["shaped"](https://github.com/dao-xyz/peerbit/blob/9e66213b07920b39e3cae3eb6c59af52a92c70b7/packages/utils/indexer/interface/src/index-engine.ts#L68) queries. This is comes in very handy if we quickly want to know whether a document exists or not. For example if you got a particular commit in an operation log. 

To get a full understanding of what features are supported in both implementations, see the [test suite](https://github.com/dao-xyz/peerbit/blob/master/packages/utils/indexer/tests/src/tests.ts) that the implementations needs to pass.

## Creating a custom implementation

With the peerbit client we can provide a custom indexing implementation on start 

```ts 
import { Peerbit } from 'peerbit'
import { Indicies } from '@peerbit/indexer-interface'

class MyCustomIndexer implements Indicies {
    // implement the interface
}

const client = await Peerbit.create({
    indexer: MyCustomIndexer // if not passed, the default will be SQlite3
})
```

or use the defaults 

```ts 
import { Peerbit } from 'peerbit'

const client = await Peerbit.create() // if not passed, the default will be @peerbit/indexer-sqlite3 for indexer
```



This allows you to create a custom implementation that can be used with the peerbit client. The indexer will be used for all documents stores, all syncing and oplog operations.

This allows for future proofing the client to be able to use different indexing backends. For example, it we could have an implementation with PostgreSQL or MongoDB, or if you want persistance on with a cloud provider that could be possible to!

## Comparison with other solutions

There are multiple different solutions out there  today that tries to approach the problem of doing a local-first, p2p approach of keeping a SQLite database in sync. For example [ElectricSQL](https://electric-sql.com/) and [PowerSync](https://www.powersync.com/). 

The difference with is that Peerbit is not trying to be a SQL database and support all features, but rather focuses on a subset of featuers that are needed for a local-first, p2p application, but still keep the door open for large scale applications to become attractive and do them really well. Mainly the focus for Peerbit is to efficiently handle a scenario where we potentially have thousands of clients that replicate shards of a database and we efficiently want to search for documents, like the "top 10" posts in a channel or get a list of all posts that are comments to a particular post quickly. 

If the support for example "join" like behaviours and aggregation where to be added to early many unforseen challanges would be introduced and performance would be hard to maintain since there would be to exist additional control logic to handle data life cycle and indexing in a sharded context.

In theory it would be possible to wrap the `@peerbit/document` in a SQL look-alike client and you would be able to support a subset of SQL queries. This is not something that is currently supported, but could be a future feature.