/// [imports]
import { field, option, variant } from "@dao-xyz/borsh";
import { PublicSignKey, sha256Sync } from "@peerbit/crypto";
import { Documents, SearchRequest } from "@peerbit/document";
import {
	And,
	BoolQuery,
	ByteMatchQuery,
	Compare,
	IntegerCompare,
	IsNull,
	Or,
	Sort,
	SortDirection,
	StringMatch,
	StringMatchMethod,
} from "@peerbit/indexer-interface";
import { Program } from "@peerbit/program";
import { type ReplicationOptions } from "@peerbit/shared-log";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { v4 as uuid } from "uuid";

/// [imports]

/// [definition]
const POST_ID_PROPERTY = "id";
const POST_PARENT_POST_ID = "parentPostid";
const POST_FROM_PROPERTY = "from";
const POST_MESSAGE_PROPERTY = "message";
const POST_TIMESTAMP_PROPERTY = "timestamp";

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

enum ReactionType {
	THUMBS_UP = 0,
	THUMBS_DOWN = 1,
	HAHA = 2,
	HEART = 3,
}

const REACTION_ID_PROPERTY = "id";
const REACTION_POST_ID_PROPERTY = "postId";
const REACTION_TYPE_PROPERTY = "type";

@variant(0) // version 0
class Reaction {
	@field({ type: "string" })
	[REACTION_ID_PROPERTY]: string;

	@field({ type: "string" })
	[REACTION_POST_ID_PROPERTY]: string;

	@field({ type: "u8" })
	[REACTION_TYPE_PROPERTY]: ReactionType;

	constructor(postId: string, reaction: ReactionType) {
		this.id = uuid();
		this.postId = postId;
		this[REACTION_TYPE_PROPERTY] = reaction;
	}
}

type ChannelArgs = { replicate?: ReplicationOptions };

// This class will let us to index posts in another format
// this is useful when the original format is not suitable for indexing
// or the indexed format should contain additional information like
// signer, timestamp etc.
class IndexedPost {
	@field({ type: "string" })
	[POST_ID_PROPERTY]: string;

	@field({ type: option("string") })
	[POST_PARENT_POST_ID]?: string;

	@field({ type: "string" })
	[POST_MESSAGE_PROPERTY]: string;

	@field({ type: Uint8Array })
	[POST_FROM_PROPERTY]: Uint8Array;

	@field({ type: "u64" })
	[POST_TIMESTAMP_PROPERTY]: bigint;

	constructor(post: Post, from: Uint8Array, timestamp: bigint) {
		this[POST_ID_PROPERTY] = post.id;
		this[POST_PARENT_POST_ID] = post.parentPostid;
		this[POST_MESSAGE_PROPERTY] = post.message;
		this[POST_FROM_PROPERTY] = from;
		this[POST_TIMESTAMP_PROPERTY] = timestamp;
	}
}

@variant("channel")
export class Channel extends Program<ChannelArgs> {
	// Documents<?> provide document store functionality around posts

	@field({ type: Documents })
	posts: Documents<Post, IndexedPost>;

	@field({ type: Documents })
	reactions: Documents<Reaction>;

	constructor() {
		super();
		this.posts = new Documents({
			id: sha256Sync(new TextEncoder().encode("posts")),
		});

		this.reactions = new Documents({
			id: sha256Sync(new TextEncoder().encode("reactions")),
		});
	}

	// Setup will be called on 'open'
	async open(properties?: ChannelArgs): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.posts.open({
			type: Post,
			replicate: properties?.replicate,
			canPerform: async (properties) => {
				// Determine whether an operation, based on an entry should be allowed

				// You can use the entry to get properties of the operation
				// like signers

				const signers = await properties.entry.getPublicKeys();

				if (properties.type === "put") {
					// do some behaviour
					const value = properties.value; // Post

					// .. do some validation logic here

					return true;
				} else if (properties.type === "delete") {
					// do some other behaviour
					return true;
				}
				return false;
			},

			/// [index]

			index: {
				// Primary key is default 'id', but we can assign it manually here
				idProperty: POST_ID_PROPERTY,

				// The type of the index
				// The constructor for this class needs to take the Post as first argument and context as second
				// or you need to implement the transform function
				type: IndexedPost,

				// transform function is used to construct an instance of the indexable class
				// from the original class. Here you can do async stuff like fetching additional data
				transform: async (post, context) => {
					return new IndexedPost(
						post,
						(
							await this.posts.log.log.get(context.head)
						)?.signatures[0].publicKey.bytes,
						context.modified,
					);
				},

				canRead: (post, publicKey) => {
					// determine whether publicKey can read post
					return true;
				},

				canSearch: (query, publicKey) => {
					// determine whether publicKey can perform query
					return true;
				},
			},

			/// [index]

			replicas: {
				// How many times should posts at least be replicated
				min: 2,
				// How many times at most can a post be replicated?
				max: undefined,
			},
			canReplicate: (publicKey: PublicSignKey) => {
				return true; // Create logic who we trust to be a replicator (and indexer)
			},
		});

		await this.reactions.open({
			type: Reaction,

			// we don't provide an index here, which means we will index all fields of Reaction
		});
	}
}
/// [definition]

/// [insert]

// Start two clients that ought to talk to each other
const peer = await Peerbit.create();
const peer2 = await Peerbit.create();

// Connect to the first peer
await peer2.dial(peer.getMultiaddrs());

const channelFromClient1 = await peer.open<Channel>(new Channel(), {
	args: { replicate: { factor: 1 } },
});
const channelFromClient2 = await peer2.open<Channel>(
	channelFromClient1.address!,
	{
		// Non-replicator will not store anything unless explicitly doing so
		args: { replicate: false },
	},
);

// Wait for peer1 to be reachable for query
await channelFromClient2.waitFor(peer.peerId);

// Lets write some things
const message1 = new Post("hello world");

// Put with no args
await channelFromClient1.posts.put(message1);

// passing { unique: true } will disable the validation check for duplicates, this is useful
// if you know the id is unique in advance (for performance reasons)
await channelFromClient1.posts.put(new Post("The Shoebill is terrifying"), {
	unique: true,
});

const message3 = new Post("No, it just a big duck");
await channelFromClient2.posts.put(message3, {
	replicas: 10, // this is an very important message, so lets notify peers we want a high replication degree on it
});

await waitForResolved(async () =>
	expect(await channelFromClient1.posts.index.getSize()).equal(3),
);

// And to do some reactions

// Client 2 reacts to the first post
await channelFromClient2.reactions.put(
	new Reaction(message1.id, ReactionType.HEART),
);

// Client 1 reacts to the last post
await channelFromClient1.reactions.put(
	new Reaction(message3.id, ReactionType.HAHA),
);
/// [insert]

/// [delete]
const anotherPost = new Post("I will delete this in a moment");
await channelFromClient2.posts.put(anotherPost);
await waitForResolved(async () =>
	expect(await channelFromClient1.posts.index.getSize()).equal(4),
);

// Delete with no arg (will permantly delete)
await channelFromClient2.posts.del(anotherPost.id);

// The delete will eventually propagate to the first client (the replicator)
await waitForResolved(async () =>
	expect(await channelFromClient1.posts.index.getSize()).equal(3),
);

/// [delete]

/// [search-all]
// Imagine if I would want to list all "root" posts (posts with no parent)
// then you can do the following
const posts: Post[] = await channelFromClient2.posts.index.search(
	new SearchRequest({
		query: [new IsNull({ key: POST_PARENT_POST_ID })],
		sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }),
	}),
);
expect(posts).to.have.length(3);
expect(posts.map((x) => x.message)).to.deep.equal([
	"hello world",
	"The Shoebill is terrifying",
	"No, it just a big duck",
]);

/// [search-all]

/// [search-locally]
const postsLocally: Post[] = await channelFromClient2.posts.index.search(
	new SearchRequest({
		query: [new IsNull({ key: POST_PARENT_POST_ID })],
		sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }),
	}),
	{
		remote: false,
		local: true,
	},
);

expect(postsLocally).to.have.length(1);
expect(postsLocally.map((x) => x.message)).to.deep.equal([
	"No, it just a big duck",
]);

/// [search-locally]

/// [search-for-one]
// Get all posts from client1 by filtering by its publicKey
const postsFromClient1: Post[] = await channelFromClient2.posts.index.search(
	new SearchRequest({
		query: [
			new ByteMatchQuery({
				key: POST_FROM_PROPERTY,
				value: peer.identity.publicKey.bytes,
			}),
		],
	}),
);
expect(postsFromClient1).to.have.length(2);
expect(postsFromClient1.map((x) => x.message)).to.deep.equal([
	"hello world",
	"The Shoebill is terrifying",
]);

/// [search-for-one]

/// [reactions-one]

// Get reactions for a particular post
const reactions: Reaction[] = await channelFromClient2.reactions.index.search(
	new SearchRequest({
		query: [
			new StringMatch({ key: [REACTION_POST_ID_PROPERTY], value: posts[2].id }),
		],
	}),
);

expect(reactions).to.have.length(1);
expect(reactions[0][REACTION_TYPE_PROPERTY]).equal(ReactionType.HAHA);
/// [reactions-one]

/// [query-detailed]
new SearchRequest({
	query: [
		// String
		new StringMatch({ key: "stringProperty", value: "hello" }),
		new StringMatch({
			key: "stringProperty",
			value: "hello",
			method: StringMatchMethod.contains,
		}), // string matches somewhere
		new StringMatch({
			key: "stringProperty",
			value: "hello",
			method: StringMatchMethod.exact,
		}), // default
		new StringMatch({
			key: "stringProperty",
			value: "hello",
			method: StringMatchMethod.prefix,
		}), // prefix match
		new StringMatch({
			key: "stringProperty",
			value: "hello",
			caseInsensitive: true,
		}),

		// Integers
		new IntegerCompare({
			key: "integerProperty",
			value: 123,
			compare: Compare.Equal,
		}),
		new IntegerCompare({
			key: "integerProperty",
			value: 123,
			compare: Compare.Greater,
		}),
		new IntegerCompare({
			key: "integerProperty",
			value: 123,
			compare: Compare.GreaterOrEqual,
		}),
		new IntegerCompare({
			key: "integerProperty",
			value: 123,
			compare: Compare.Less,
		}),
		new IntegerCompare({
			key: "integerProperty",
			value: 123,
			compare: Compare.LessOrEqual,
		}),

		// Boolean
		new BoolQuery({ key: "boolProperty", value: true }),

		// Missing values
		new IsNull({ key: "someProperty" }),

		// Nested propety
		// Find values for nested fields, e.g. { a: { b: { c: "hello "}}}
		new StringMatch({ key: ["a", "b", "c"], value: "hello" }),

		// Nested queries
		new Or([
			new StringMatch({ key: "stringProperty", value: "hello" }),
			new And([
				new StringMatch({ key: "anotherProperty", value: "world" }),
				new IntegerCompare({
					key: "integerProperty",
					value: 123,
					compare: Compare.Less,
				}),
			]),
		]),
	],
});
/// [query-detailed]

/// [sort-detailed]
new SearchRequest({
	query: [],
	sort: [
		new Sort({ key: "someProperty" }), // ascending sort direction (default)
		new Sort({ key: "anotherProperty", direction: SortDirection.DESC }),
	],
});

/// [sort-detailed]

/// [iterator-detailed]
const iterator = channelFromClient2.posts.index.iterate(
	new SearchRequest({ sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }) }),
);
const postsFromIterator = await iterator.next(2); // fetch (at most) 2 posts
expect(postsFromIterator).to.have.length(2);
expect(iterator.done()).to.be.false; // There should be 3 posts in total and we only fetched 2

// You can close the iterator once you are done
// This will notify peers that you are doing iterating
await iterator.close();
/// [iterator-detailed]

/// [sync]
const iterateAndSync = await channelFromClient2.posts.index.iterate(
	new SearchRequest({ sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }) }),
	{ local: true, remote: { replicate: true } },
);

const searchAndSync = await channelFromClient2.posts.index.search(
	new SearchRequest({ sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }) }),
	{ local: true, remote: { replicate: true } },
);
/// [sync]

/// [disconnecting]
await peer.stop();
await peer2.stop();
/// [disconnecting]
