# Document store
The document store is program/database you can use within your apps. The purpose of the document store is to act as a key-value storage for any kind of values. Additionally it also supports document querying, such as field search and sorting.

The document store is a separate package and you can install it with:

```
npm install @peerbit/document
```


## Imports
[imports](./document-store.ts ':include :fragment=imports')

## Definition
Below is an example of a definition of two document stores where we store posts, comments, and their reactions. 

[imports](./document-store.ts ':include :fragment=definition')

### Determinism
The `Documents` construction in the Channel constructor leaves some id variables unset. The values for these will be randomly generated. If you want to ensure that the address of the program is the same every time you construct it, you can set these, e.g. 

```typescript

import { sha256Sync } from "@peerbit/crypto";

type ChannelArgs = { role?: Role };
@variant("channel")
export class Channel extends Program<ChannelArgs> {
	// Documents<?> provide document store functionality around posts

	@field({ type: Documents })
	posts: Documents<Post>;

	@field({ type: Documents })
	reactions: Documents<Reaction>;

	constructor() {
		super();
		this.posts = new Documents({
            // id needs to have length 32
            id: sha256Sync(new TextEncoder().encode("posts"))
		});

		this.reactions = new Documents({
            // id needs to have length 32
            id: sha256Sync(new TextEncoder().encode("reactions"))
		});
	}
...

// Now doing

// zb2abc123...
console.log((await peer.open(new Channel())).address); 

// zb2abc123... (the same address will be generated)
console.log((await peer.open(new Channel())).address); 
```

For more applied information about uniqueness see [the tests](https://github.com/dao-xyz/peerbit-getting-started/blob/c19532c658f9cf59988b8f4acc9006b08b6fecbe/src/index.test.ts#L120) in the getting started repo where different setups are explored. For more info about data integrity and uniqueness in general see [this](/topics/integrity.md).



## Put
Inserting documents is done like so.

[put](./document-store.ts ':include :fragment=insert')


## Delete
You can also delete documents.

[delete](./document-store.ts ':include :fragment=delete')


## Searching
Now from another client, let's connect, and try to find our documents.

### Get all

[get-all](./document-store.ts ':include :fragment=search-all')



### Local only
We can also search for only local documents.

[insert](./document-store.ts ':include :fragment=search-locally')

### Query

We can add conditions for our query.

For example, finding all posts created by the given user.

[search-from-one](./document-store.ts ':include :fragment=search-from-one')

And for our reactions, we can find them for a particular post.

[search-reactions-from-one](./document-store.ts ':include :fragment=reactions-one')


#### Query types
There are many different kinds of filter you can apply.

[query-types](./document-store.ts ':include :fragment=query-detailed')


## Sorting 

You can apply one or more sort criteria. This will be evaluated in order, if there is a tie for the first criterion, the next will be evaluated.


[sort-detailed](./document-store.ts ':include :fragment=sort-detailed')


## Iterator

The iterator is the recommended way of collecting and iterating over documents, as this provides the finest amount of control over how and when to fetch more documents from peers. The difference between search and iterate is that the search api creates an iterator under the hood, performs ```iterator.next()```, and tries to collect as many documents as possible, while an iterator allows you to call `next` yourself.

[iterator-detailed](./document-store.ts ':include :fragment=iterator-detailed')

## Syncing 
You can sync documents to your local store while iterating or searching. 

[sync](./document-store.ts ':include :fragment=sync')


## Replication degree 

Below is an additional example of how you granular your control of replication is with Peerbit.
- Replication degree as opening argument.
- Replication degree on put. 
- Filtering of commits, based on allowed replication degree 
- What peers to trust as replicators

[replication-degree](./replication-degree.ts ':include')


## Multi-signed posts

You can override the signers when doing insertions. See example below

[multisig](./multisig.ts ':include')

In real life scenarios, you would not have the `REQUIRED_SIGNER` keypair locally. What you instead can do is to make a remote signer function, where you send away the entry to be signed and get it returned with the required signature. See [this](https://github.com/dao-xyz/peerbit/blob/master/packages/programs/clock-service/src/__tests__/index.test.ts) as an implementation example on how a remote node is signing a document if they approved that the entry has a correctly set timestamp.


It is also possible here to make Web3 wallets such as MetaMask to conform to the signer function type.



## Migrations


Migration is usually something that is achieved by having a central authority iterating over all data in mutating it in some way. In a P2P setup where the ownership can be decentralized makes the approach to this different.


### Modifying the type of new documents
When you want to update how a document should look like, e.g. you have PostV0 and now want to do PostV1 which contains different kinds of information, you can rely on polymorphism to let both types coexist in the same database. See example below


[migration](./migration-types.ts ':include')




### Converting existing documents
If you don't want the old version to linger around you might want to perform two actions.


1. Insert a new document with the new type constructed from the old document
2. Delete the old document once you have successfully inserted the new document.


The challenge with this approach is that if only documents can be deleted or inserted by the owners themselves (let say you can only create a post with your name as author if you signed it), then


- (a) Everyone that has inserted documents in the db needs to participate in the migration process.




Or


- (b) Some authority needs to be trusted by everyone for doing the migration. I.e. This authority can insert documents and when peers evaluate the signatures public key in the `canPerform` callback, they will approve the operations if it is from this trusted authority. This would be conceptually equivalent to a traditional database migration where you have a central authority having power to mutate any document at any time.




(a) is easier than it sounds. You can have a function that is running on `open` that iterates on all documents I have locally (that you have created) and re-insert them. The migration job is basically then just to make sure all peers are running the updated code that does this iteration, and eventually with time, all documents will be migrated.


[migration-owned](./migration-owned.ts ':include')




(b)


For the central authority solution, this is an example how it can look like:


[migration-centralized](./migration-centralized.ts ':include')


Additionally you need to have a good way of managing the information about who is the central authority and how to fetch that information.


