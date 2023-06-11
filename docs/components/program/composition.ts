import { field, variant } from "@dao-xyz/borsh";
import { Observer, Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";
import {
	DocumentIndex,
	Documents,
	SearchRequest,
} from "@dao-xyz/peerbit-document";
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

// This class extends Program which allows it to be replicated amongst peers
@variant("posts")
class PostsDB extends Program {
	@field({ type: Documents })
	posts: Documents<Post>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.posts = new Documents({ index: new DocumentIndex({ indexBy: "id" }) });
	}

	async setup(): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.posts.setup({ type: Post /* canAppend: (entry) => true */ });
	}
}
/// [data]

@variant("channel")
class Channel extends Program {
	// Name of channel
	@field({ type: "string" })
	name: string;

	// Posts within channel
	@field({ type: PostsDB })
	db: PostsDB; // Documents<?> provide document store functionality around your Posts

	constructor(name: string) {
		super();

		this.name = name;
		this.db = new PostsDB();
	}

	async setup(): Promise<void> {
		await this.db.setup();
	}
}

@variant("forum")
class Forum extends Program {
	// Name of channel
	@field({ type: "string" })
	name: string;

	// Posts within channel
	@field({ type: Documents })
	channels: Documents<Channel>;

	constructor(name: string) {
		super();

		this.name = name;
		this.channels = new Documents({
			index: new DocumentIndex({ indexBy: "name" }),
		});
	}

	async setup(): Promise<void> {
		await this.channels.setup({
			type: Channel,
			canAppend: (entry) => true, // who can create a channel?
			canOpen: (channel: Channel) => true, // if someone append a Channel, should I, as a Replicator, start/open it?
		});
	}
}

const client = await Peerbit.create();
const forum = await client.open(new Forum("dforum"));

const channel = new Channel("general");
await forum.channels.put(channel);
await channel.db.posts.put(new Post("Hello world!"));

// Another peer
const client2 = await Peerbit.create();
await client2.dial(client);
const forum2 = await client2.open<Forum>(forum.address, {
	role: new Observer(),
});

// Wait for client 1 to be available (only for testing locally)
await forum2.waitFor(client.libp2p);

// find channels from the forum from client2 perspective
const channels = await forum2.channels.index.query(
	new SearchRequest({ queries: [] })
);
expect(channels).toHaveLength(1);
expect(channels[0].name).toEqual("general");

// open this channel
const channel2 = await client2.open<Channel>(channels[0]);

// Wait for client 1 to be available (only for testing locally)
await channel2.waitFor(client.libp2p);

// find messages
const messages = await channel2.db.posts.index.query(
	new SearchRequest({ queries: [] })
);
expect(messages).toHaveLength(1);
expect(messages[0].message).toEqual("Hello world!");

await client.stop();
await client2.stop();
