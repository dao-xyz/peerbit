import { field, variant } from "@dao-xyz/borsh";
import { Documents, SearchRequest } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { type ReplicationOptions } from "@peerbit/shared-log";
// find channels from the forum from client2 perspective
import { expect } from "chai";
import { Peerbit } from "peerbit";
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
type Args = { replicate: ReplicationOptions };

@variant("posts-with-documents-store")
class PostsDB extends Program<Args> {
	@field({ type: Documents })
	posts: Documents<Post>; // Documents<?> provide document store functionality around your Posts

	constructor() {
		super();
		this.posts = new Documents();
	}

	async open(args?: Args): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.posts.open({
			type: Post,
			replicate: args?.replicate /* canPerform: (entry) => true */,
		});
	}
}

/// [data]
@variant("channel-with-nested-postdb")
class Channel extends Program<Args> {
	// Name of channel
	@field({ type: "string" })
	name: string;

	// Posts within channel
	@field({ type: PostsDB })
	db: PostsDB;

	constructor(name: string) {
		super();

		this.name = name;
		this.db = new PostsDB();
	}

	async open(args?: Args): Promise<void> {
		await this.db.open(args);
	}
}

class IndexableChannel {
	@field({ type: "string" })
	name: string;

	@field({ type: "string" })
	db: string;

	constructor(name: string, address: string) {
		this.name = name;
		this.db = address;
	}
}

const NAME_PROPERTY = "name";

@variant("forum")
class Forum extends Program<Args> {
	// Name of channel
	@field({ type: "string" })
	[NAME_PROPERTY]: string;

	// Posts within channel
	@field({ type: Documents })
	channels: Documents<Channel, IndexableChannel>;

	constructor(name: string) {
		super();

		this[NAME_PROPERTY] = name;
		this.channels = new Documents();
	}

	async open(args?: Args): Promise<void> {
		await this.channels.open({
			type: Channel,
			canPerform: (entry) => true, // who can create a channel?
			canOpen: (channel: Channel) => true, // if someone append a Channel, should I, as a Replicator, start/open it?
			index: {
				idProperty: NAME_PROPERTY,
				type: IndexableChannel,
				transform: async (channel, _context) =>
					new IndexableChannel(channel.name, await channel.calculateAddress()),
			},
			replicate: args?.replicate,
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
await client2.dial(client.getMultiaddrs());

// open the forum as a observer, i.e. not replication duties
const forum2 = await client2.open<Forum>(forum.address, {
	args: { replicate: false },
});

// Wait for client 1 to be available (only needed for testing locally)
await forum2.channels.log.waitForReplicator(client.identity.publicKey);

const channels = await forum2.channels.index.search(new SearchRequest());
expect(channels).to.have.length(1);
expect(channels[0].name).equal("general");

// open this channel (if we would open the forum with replicate: false, this would already be done)
expect(channels[0].closed).to.be.true;
const channel2 = await client2.open<Channel>(channels[0], {
	args: { replicate: false },
});

// Wait for client 1 to be available (only needed for testing locally)
await channel2.db.posts.log.waitForReplicator(client.identity.publicKey);

// find messages
const messages = await channel2.db.posts.index.search(new SearchRequest());
expect(messages).to.have.length(1);
expect(messages[0].message).equal("Hello world!");

await client.stop();
await client2.stop();
