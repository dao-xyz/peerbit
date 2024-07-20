import { field, variant } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Documents } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { v4 as uuid } from "uuid";

@variant(0)
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

const REQUIRED_SIGNER = await Ed25519Keypair.create();

@variant("post-store")
class PostStore extends Program {
	@field({ type: Documents })
	posts: Documents<Post>;

	constructor() {
		super();
		this.posts = new Documents();
	}

	async open(args?: any): Promise<void> {
		await this.posts.open({
			type: Post,
			canPerform: (properties) => {
				// This canPerfom will only return true if the post was signed by REQUIRED_SIGNER and another party
				const publicKeys = properties.entry.publicKeys; // Public keys of signers
				if (
					publicKeys.find((publicKey) =>
						publicKey.equals(REQUIRED_SIGNER.publicKey),
					) &&
					publicKeys.find(
						(publicKey) => !publicKey.equals(REQUIRED_SIGNER.publicKey),
					)
				) {
					return true;
				}

				return false;
			},
		});
	}
}

const peer = await Peerbit.create();

const db = await peer.open(new PostStore());

await db.posts.put(new Post("Hello world!"), {
	signers: [
		peer.identity.sign.bind(peer.identity),
		REQUIRED_SIGNER.sign.bind(REQUIRED_SIGNER),
	],
});

expect(await db.posts.index.getSize()).equal(1); // Post was appproved

await peer.stop();
