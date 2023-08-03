/**
 * An example where:
 * - A document store is created storing posts
 * - One peer is inserting 1 post
 * - Another peer is dialing the first peer and later tries to find all posts
 *
 *
 * If you get consfused by the "/// [abc]" lines, they are just meant for the documentation
 * website to be able to render parts of the code.
 * If you are to copy code from this example, you can safely remove these
 */

/// [imports]
import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "peerbit";
import { Documents, SearchRequest } from "@peerbit/document";
import { v4 as uuid } from "uuid";
/// [imports]

/// [client]
const peer = await Peerbit.create();
/// [client]

/// [data]
// This class will store post messages
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
		this.posts = new Documents();
	}

	/**
	 * Implement open to control what things are to be done on 'open'
	 */
	async open(): Promise<void> {
		// We need to setup the store in the setup hook
		// we can also modify properties of our store here, for example set access control
		await this.posts.open({
			type: Post,
			index: { key: "id" },
			// You can add more properties here, like
			/* canWrite: (entry) => true */
		});
	}
}
/// [data]

/// [insert]
const store = await peer.open(new PostsDB());
await store.posts.put(new Post("hello world"));
/// [insert]

/// [another-client]
// search for documents from another peer
const peer2 = await Peerbit.create();

// Connect to the first peer
await peer2.dial(peer.getMultiaddrs());

const store2 = await peer2.open<PostsDB>(store.address!);

// Wait for peer1 to be reachable for query
await store.waitFor(peer2.peerId);

const responses: Post[] = await store2.posts.index.search(
	new SearchRequest({
		query: [], // query all
	})
);
expect(responses).toHaveLength(1);
expect(responses.map((x) => x.message)).toEqual(["hello world"]);
/// [another-client]

/// [disconnecting]
await peer.stop();
await peer2.stop();
/// [disconnecting]
