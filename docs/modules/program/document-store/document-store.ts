/// [imports]
import { field, variant, option } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "peerbit";
import { v4 as uuid } from "uuid";

import {
	ByteMatchQuery,
	DeleteOperation,
	DocumentIndex,
	Documents,
	MissingField,
	PutOperation,
	SearchRequest,
	Sort,
} from "@peerbit/document";
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
			index: new DocumentIndex(),
		});

		this.reactions = new Documents({
			index: new DocumentIndex(),
		});
	}

	/**
	 * Setup will be called on 'open'
	 */
	async open(properties?: ChannelArgs): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.posts.open({
			type: Post,
			role: properties?.role,
			canPerform: async (operation, { entry }) => {
				// Determine whether an operation, based on an entry should be allowed

				// You can use the entry to get properties of the operation
				// like signers
				const signers = await entry.getPublicKeys();

				if (operation instanceof PutOperation) {
					// do some behaviour
					return true;
				} else if (operation instanceof DeleteOperation) {
					// do some other behaviour
					return true;
				}
				return false;
			},

			index: {
				// Primary key is default 'id', but we can assign it manually here
				key: POST_ID_PROPERTY,

				// You can tailor what fields should be indexed,
				// everything else will be stored on disc (if you use disc storage with the client)
				fields: async (post, context) => {
					return {
						[POST_ID_PROPERTY]: post.message,
						[POST_PARENT_POST_ID]: post.parentPostid,
						[POST_MESSAGE_PROPERTY]: post.message,
						[POST_FROM_PROPERTY]: (await this.posts.log.log.get(context.head))
							?.signatures[0].publicKey.bytes,
						[POST_TIMESTAMP_PROPERTY]: context.modified,
					};
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
			index: {
				// Primary key is default 'id', but we can assign it manually here
				key: REACTION_ID_PROPERTY,
			},
			// we don't provide an index here, which means we will index all fields of Reaction
		});
	}
}
/// [definition]

/// [insert]
import { waitForResolved } from "@peerbit/time";

// Start two clients that ought to talk to each other
const peer = await Peerbit.create();
const peer2 = await Peerbit.create();

// Connect to the first peer
await peer2.dial(peer.getMultiaddrs());

const channelFromClient1 = await peer.open(new Channel());
const channelFromClient2 = await peer2.open<Channel, ChannelArgs>(
	channelFromClient1.address!,
	{
		// Observer will not store anything unless explicitly doing so
		args: { role: new Observer() }, // or new Replicator() (default))
	}
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

// Since the first node is a replicator, it will eventually get all messages
await waitForResolved(() =>
	expect(channelFromClient1.posts.index.size).toEqual(3)
);

// And to do some reactions

// Client 2 reacts to the first post
await channelFromClient2.reactions.put(
	new Reaction(message1.id, ReactionType.HEART)
);

// Client 1 reacts to the last post
await channelFromClient1.reactions.put(
	new Reaction(message3.id, ReactionType.HAHA)
);
/// [insert]

/// [delete]
const anotherPost = new Post("I will delete this in a moment");
await channelFromClient2.posts.put(anotherPost);
await waitForResolved(() =>
	expect(channelFromClient1.posts.index.size).toEqual(4)
);

// Delete with no arg (will permantly delete)
await channelFromClient2.posts.del(anotherPost.id);

// The delete will eventually propagate to the first client (the replicator)
await waitForResolved(() =>
	expect(channelFromClient1.posts.index.size).toEqual(3)
);

/// [delete]

/// [search-all]
// Imagine if I would want to list all "root" posts (posts with no parent)
// then you can do the following
const posts: Post[] = await channelFromClient2.posts.index.search(
	new SearchRequest({
		query: [new MissingField({ key: POST_PARENT_POST_ID })],
		sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }),
	})
);
expect(posts).toHaveLength(3);
expect(posts.map((x) => x.message)).toEqual([
	"hello world",
	"The Shoebill is terrifying",
	"No, it just a big duck",
]);

/// [search-all]

/// [search-locally]
const postsLocally: Post[] = await channelFromClient2.posts.index.search(
	new SearchRequest({
		query: [new MissingField({ key: POST_PARENT_POST_ID })],
		sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }),
	}),
	{
		remote: false,
		local: true,
	}
);
expect(postsLocally).toHaveLength(1);
expect(postsLocally.map((x) => x.message)).toEqual(["No, it just a big duck"]);
/// [search-locally]

/// [search-from-one]
// Get all posts from client1 by filtering by its publicKey
const postsFromClient1: Post[] = await channelFromClient2.posts.index.search(
	new SearchRequest({
		query: [
			new ByteMatchQuery({
				key: POST_FROM_PROPERTY,
				value: peer.identity.publicKey.bytes,
			}),
		],
	})
);
expect(postsFromClient1).toHaveLength(2);
expect(postsFromClient1.map((x) => x.message)).toEqual([
	"hello world",
	"The Shoebill is terrifying",
]);

/// [search-from-one]

/// [reactions-one]
// Get reactions for a particular post
const reactions: Reaction[] = await channelFromClient2.reactions.index.search(
	new SearchRequest({
		query: [
			new StringMatch({ key: [REACTION_POST_ID_PROPERTY], value: posts[2].id }),
		],
	})
);

expect(reactions).toHaveLength(1);
expect(reactions[0][REACTION_TYPE_PROPERTY]).toEqual(ReactionType.HAHA);
/// [reactions-one]

/// [query-detailed]
import {
	StringMatch,
	IntegerCompare,
	BoolQuery,
	StringMatchMethod,
	Compare,
	SortDirection,
	Or,
	And,
	Observer,
	Role,
} from "@peerbit/document";
import { PublicSignKey } from "@peerbit/crypto";

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
		new MissingField({ key: "someProperty" }),

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
	new SearchRequest({ sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }) })
);
const postsFromIterator = await iterator.next(2); // fetch (at most) 2 posts
expect(postsFromIterator).toHaveLength(2);
expect(iterator.done()).toBeFalse(); // There should be 3 posts in total and we only fetched 2

// You can close the iterator once you are done
// This will notify peers that you are doing iterating
await iterator.close();
/// [iterator-detailed]

/// [sync]
const iterateAndSync = await channelFromClient2.posts.index.iterate(
	new SearchRequest({ sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }) }),
	{ local: true, remote: { sync: true } }
);

const searchAndSync = await channelFromClient2.posts.index.search(
	new SearchRequest({ sort: new Sort({ key: POST_TIMESTAMP_PROPERTY }) }),
	{ local: true, remote: { sync: true } }
);
/// [sync]

/// [disconnecting]
await peer.stop();
await peer2.stop();
/// [disconnecting]
