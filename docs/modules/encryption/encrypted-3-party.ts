import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "peerbit";
import {
	DeleteOperation,
	Documents,
	Observer,
	PutOperation,
	SearchRequest,
} from "@peerbit/document";
import { X25519Keypair } from "@peerbit/crypto";

function sleep(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

@variant(0) // version 0
class Post {
	@field({ type: "string" })
	id: string;

	@field({ type: "string" })
	message: string;

	constructor(id: string, message: string) {
		this.id = id;
		this.message = message;
	}
}

@variant("posts")
class PostsDB extends Program {
	@field({ type: Documents })
	posts: Documents<Post>;

	constructor() {
		super();
		this.posts = new Documents();
	}

	async open(): Promise<void> {
		await this.posts.open({
			type: Post,
			index: { key: "id" },
			canAppend: async (entry) => {
				await entry.verifySignatures();
				const payload = await entry.getPayloadValue();
				console.log("GOT PAYLOAD");
				if (payload instanceof PutOperation) {
					const post: Post = payload.getValue(this.posts.index.valueEncoding);
					console.log("PUT POST", post);
					return true;
				} else if (payload instanceof DeleteOperation) {
					return false;
				}
				return true;
			},
			canRead: async (publicKey) => {
				return publicKey?.equals(client3.identity.publicKey) || false;
			},
		});
	}
}

const client1 = await Peerbit.create();
const client2 = await Peerbit.create();
const client3 = await Peerbit.create();

const store = await client1.open(new PostsDB());

const post = new Post("ID1", "hello world");

await store.posts.put(post, {
	encryption: {
		keypair: await X25519Keypair.create(),
		reciever: {
			// Who can read the log entry metadata (e.g. timestamps)
			metadata: [
				// client1.identity.publicKey,
				// client2.identity.publicKey,
				// client3.identity.publicKey
			],

			// Who can read the references of the entry (next pointers)
			next: [
				// client1.identity.publicKey,
				// client2.identity.publicKey,
				// client3.identity.publicKey
			],

			// Who can read the message?
			payload: [
				// client1.identity.publicKey,
				// client2.identity.publicKey,
				client3.identity.publicKey,
			],

			// Who can read the signature ?
			// (In order to validate entries you need to be able to read the signature)
			signatures: [
				// client1.identity.publicKey,
				// client2.identity.publicKey,
				// client3.identity.publicKey
			],
		},
	},
});

async function printPosts(store: any) {
	const responses: Post[] = await store.posts.index.search(
		new SearchRequest({
			query: [], // query all
		})
	);
	console.log(responses);
}

console.log("Dialing client2 with client1");
await client2.dial(client1.getMultiaddrs());

console.log("Dialing client3 with client1");
await client3.dial(client1.getMultiaddrs());

//////////////////////
const store2 = await client2.open<PostsDB>(store.address);
//////////////////////

//////////////////////
const store3 = await client3.open<PostsDB>(store.address);
//////////////////////

await sleep(5000);

console.log("Store1:");
await printPosts(store);
console.log("Store2:");
await printPosts(store2);
console.log("Store3:");
await printPosts(store3);

await sleep(5000);

console.log("END");

await client1.stop();
await client2.stop();
await client3.stop();
