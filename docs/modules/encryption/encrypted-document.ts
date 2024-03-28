import { field, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "peerbit";
import {
	DeleteOperation,
	Documents,
	PutOperation,
	SearchRequest
} from "@peerbit/document";
import { delay } from "@peerbit/time";
import { X25519Keypair } from "@peerbit/crypto";
import { v4 as uuid } from "uuid";

const groupMember1 = await Peerbit.create({
	relay: true
});
const groupMember2 = await Peerbit.create({
	relay: true
});
const nonMember = await Peerbit.create({
	relay: true
});

await groupMember2.dial(groupMember1.getMultiaddrs());
await nonMember.dial(groupMember1.getMultiaddrs());

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

@variant("encrypted-posts-db")
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
			replicas: {
				min: 2
			},
			index: {
				canRead: (post, publicKey) => {
					// can publicKey read this particular post?
					return true;
				},
				canSearch: (request, publicKey) => {
					// can publicKey perform this query request?
					return !!ALL_MEMBERS.find((x) => x.equals(publicKey));
				}
			},
			canReplicate: (publicKey) =>
				!!ALL_MEMBERS.find((x) => x.equals(publicKey))
		});
	}
}

const ALL_MEMBERS = [
	groupMember1.identity.publicKey,
	groupMember2.identity.publicKey
];

const memberStore1 = await groupMember1.open(new PostsDB());

const post = new Post(uuid(), "hello world");

await memberStore1.posts.put(post, {
	encryption: {
		keypair: await X25519Keypair.create(),
		receiver: {
			// Who can read the log entry metadata (e.g. timestamps, next refs)
			meta: [
				// client1.identity.publicKey,
				// client2.identity.publicKey,
				// client3.identity.publicKey
			],

			// Who can read the payload/message?
			payload: ALL_MEMBERS,

			// Who can read the signature ?
			// (In order to validate entries you need to be able to read the signature)
			signatures: [
				// client1.identity.publicKey,
				// client2.identity.publicKey,
				// client3.identity.publicKey
			]
		}
	}
});

const memberStore2 = await groupMember2.open<PostsDB>(memberStore1.address);
const nonMemberStore = await nonMember.open<PostsDB>(memberStore1.address);

// Wait some time as all stores are replicators.
// And there could be a replication progress underway (https://github.com/dao-xyz/peerbit/issues/151)
// If you open the store with an observer role, then you will not need this delay
await delay(3000);
expect(await memberStore1.posts.index.search(new SearchRequest())).toHaveLength(
	1
);

expect(await memberStore2.posts.index.search(new SearchRequest())).toHaveLength(
	1
);

expect(
	await nonMemberStore.posts.index.search(new SearchRequest())
).toHaveLength(0);

await groupMember1.stop();
await groupMember2.stop();
await nonMember.stop();
